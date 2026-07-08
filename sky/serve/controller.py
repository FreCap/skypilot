"""SkyServeController: the central controller of SkyServe.

Responsible for autoscaling and replica management.
"""
import contextlib
import hmac
import logging
import os
import threading
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

import colorama
import fastapi
from fastapi import responses
import uvicorn

from sky import serve
from sky import sky_logging
from sky.serve import autoscalers
from sky.serve import lb_rbac_preflight
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import context_utils
from sky.utils import thread_utils
from sky.utils import ux_utils

logger = sky_logging.init_logger(__name__)


def _make_auth_dependency() -> Callable:
    """Build a FastAPI dependency that enforces a shared bearer token.

    The expected token is read fresh from
    `serve_utils.get_controller_auth_token()` on every request, so a token
    rotated after the controller boots is honored without a respawn. When the
    token is None/empty (auth disabled), the
    dependency is a no-op so in-pod / localhost-only deployments are unchanged.
    Otherwise it requires an `Authorization: Bearer <token>` header matching in
    constant time; a missing or wrong token yields 401. Applied to every
    control-plane endpoint -- the destructive ones and the read-only
    sync/status paths -- since the LB now presents the token on every sync.
    """

    async def _verify(authorization: Optional[str] = fastapi.Header(
        None)) -> None:
        expected_token = serve_utils.get_controller_auth_token()
        if not expected_token:
            return
        expected = f'Bearer {expected_token}'
        # isascii() guards hmac.compare_digest, which raises TypeError on a
        # non-ASCII str -- a malformed header must be a clean 401, not a 500.
        if (authorization is None or not authorization.isascii() or
                not hmac.compare_digest(authorization, expected)):
            raise fastapi.HTTPException(status_code=401, detail='Unauthorized.')

    return _verify


class AutoscalerInfoFilter(logging.Filter):

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not ('GET' in message and '200' in message and
                    '/autoscaler/info' in message)


class SkyServeController:
    """SkyServeController: control everything about replica.

    This class is responsible for:
        - Starting and terminating the replica monitor and autoscaler.
        - Providing the HTTP Server API for SkyServe to communicate with.
    """

    def __init__(self, service_name: str, service_spec: serve.SkyServiceSpec,
                 version: int, host: str, port: int) -> None:
        self._service_name = service_name
        self._replica_manager: replica_managers.ReplicaManager = (
            replica_managers.SkyPilotReplicaManager(service_name=service_name,
                                                    spec=service_spec,
                                                    version=version))
        # Pass `version` so a controller rebuilt on restart/respawn starts the
        # autoscaler at the recovered latest version (matching the replica
        # manager above), not INITIAL_VERSION. Otherwise a service updated past
        # v1 would have its autoscaler treat every live replica as outdated and
        # churn replicas forever after any restart.
        self._autoscaler: autoscalers.Autoscaler = (
            autoscalers.Autoscaler.from_spec(service_name, service_spec,
                                             version))
        # [boltz fork] Reserved-capacity fill poller lifecycle: started
        # from run() when the service booted with the flag on, and
        # lazily from update_service when an update enables the flag on
        # a live service (idempotent -- at most one poller thread; a
        # flag toggled OFF leaves the thread alive but dormant, see
        # poller_loop). The poller is the only component allowed to
        # issue the expensive cluster-wide realtime free-GPU query.
        self._reserved_capacity_fill_enabled: bool = bool(
            getattr(service_spec, 'reserved_capacity_fill', False))
        self._reserved_capacity_poller_started: bool = False
        # update_service handlers run in FastAPI's threadpool, so two
        # concurrent fill-enabling updates could both observe the
        # started flag as False; the lock makes start-once atomic.
        self._reserved_capacity_poller_lock = threading.Lock()
        # Seed the zero-cost location set synchronously, before run()
        # starts the autoscaler thread: a respawned controller's
        # autoscaler boots with empty fill state (from_spec above; there
        # is no cross-process dump/load) and its first decision tick can
        # beat the first poll by a lot (per-location cost warm-up + the
        # cluster-wide realtime query). Without the seed, a QPS-family
        # autoscaler's first tick computes target=min_replicas from its
        # empty window and, with zero_cost_count=0, suppression cannot
        # shelter the live fill fleet -- the whole fill surplus would be
        # mass-terminated. Seeding grants NO free slots (snapshot time
        # stays None), so no new fill launches until the first real poll.
        self._seed_fill_zero_cost_locations(self._autoscaler)
        self._host = host
        self._port = port
        # [boltz fork] Cache of replica_id -> (url, gpu_type, gpu_count)
        # for the
        # load_balancer_sync response. Both fields require a cluster handle
        # fetch (and, for the url, an endpoint query) and are fixed for a
        # replica's lifetime once it is READY, so they are resolved at most
        # once per replica. The cache is rebuilt from the currently active
        # replicas on every sync, which prunes replicas that are no longer
        # READY; a replica that recovers with a new endpoint is thus
        # re-resolved.
        self._lb_replica_cache: Dict[int, Tuple[str, str, int]] = {}
        # Superset of _lb_replica_cache for url -> replica_id translation
        # of the LB's in-flight report: keeps entries for replicas that
        # left READY but are still nonterminal, so a probe-blipped
        # replica's running job stays attributed to it (see
        # _get_lb_replica_info / _translate_in_flight).
        self._lb_translation_cache: Dict[int, Tuple[str, str, int]] = {}
        self._app = fastapi.FastAPI(lifespan=self.lifespan)

    @contextlib.asynccontextmanager
    async def lifespan(self, _: fastapi.FastAPI):
        uvicorn_access_logger = logging.getLogger('uvicorn.access')
        for handler in uvicorn_access_logger.handlers:
            handler.setFormatter(sky_logging.FORMATTER)
            handler.addFilter(AutoscalerInfoFilter())
        yield

    def _seed_fill_zero_cost_locations(
            self, autoscaler: autoscalers.Autoscaler) -> None:
        """Seed an autoscaler's zero-cost location set from the placer.

        Spec-derived and cheap by construction: zero_cost_locations()
        computes per-location costs via the placer's cache, which reads
        local catalog files only (never the Kubernetes API), so it is
        safe to call synchronously at boot / inside update_service. The
        seed only sets the location identity set (no free slots, no
        snapshot time), and an already-populated set (e.g. loaded from a
        dump) is never overwritten -- see
        Autoscaler.seed_zero_cost_locations.
        """
        if not autoscaler.reserved_capacity_fill:
            return
        placer = getattr(self._replica_manager, '_spot_placer', None)
        if placer is None:
            return
        autoscaler.seed_zero_cost_locations([
            location.to_pickleable()
            for location in placer.zero_cost_locations()
        ])

    def _get_lb_replica_info(
        self, replica_infos: List['replica_managers.ReplicaInfo']
    ) -> Dict[str, Dict[str, str]]:
        """Build the url -> replica info mapping for load_balancer_sync.

        [boltz fork] Resolving a replica's url and gpu_type is expensive (a
        cluster handle fetch plus, for the url, an endpoint query against a
        database the launch threads contend on), so both are cached per
        replica for the replica's lifetime: only newly-READY replicas are
        resolved on a sync.
        A brand-new replica whose gpu_type cannot be resolved yet is reported
        as 'unknown' until it is.

        `replica_infos` is fetched once by the caller and shared with the
        capacity-hint computation, so the async sync handler issues no
        extra replica-list DB reads.
        """
        record = serve_state.get_service_from_name(self._service_name)
        assert record is not None, ('No service record found for '
                                    f'{self._service_name}')
        active_versions = set(record['active_versions'])
        replica_cache: Dict[int, Tuple[str, str, int]] = {}
        replica_info: Dict[str, Dict[str, str]] = {}
        for info in replica_infos:
            if (info.status != serve_state.ReplicaStatus.READY or
                    info.version not in active_versions):
                continue
            cached = self._lb_replica_cache.get(info.replica_id)
            if cached is None:
                url = info.url
                if url is None:
                    # A replica can be READY while its endpoint is briefly
                    # unresolvable (e.g. the cluster record has no head IP
                    # mid-recovery). Skip it for this sync instead of
                    # crashing the whole load_balancer_sync — it is simply
                    # not routable this round and will be re-resolved on
                    # the next sync.
                    logger.warning(f'Replica {info.replica_id} is READY but '
                                   'its endpoint is not resolvable yet; '
                                   'skipping for this sync.')
                    continue
                # gpu_type/gpu_count are used by instance-aware load
                # balancing policies. They derive from the replica's
                # launched accelerators, which are fixed for the replica's
                # lifetime.
                gpu_type = 'unknown'
                gpu_count = 1
                handle = info.handle()
                if handle is not None:
                    accelerators = handle.launched_resources.accelerators
                    if accelerators:
                        gpu_type = list(accelerators.keys())[0]
                        try:
                            gpu_count = max(1, int(accelerators[gpu_type]))
                        except (TypeError, ValueError):
                            gpu_count = 1
                cached = (url, gpu_type, gpu_count)
            replica_cache[info.replica_id] = cached
            url, gpu_type, gpu_count = cached
            replica_info[url] = {
                'gpu_type': gpu_type,
                'gpu_count': str(gpu_count),
            }
        # The translation cache retains entries for replicas that left
        # READY but are still alive: the LB's in-flight snapshot is taken
        # against ITS last routing view, so a replica probe-blipped out
        # of READY mid-job would otherwise become untranslatable -- its
        # in-flight unit would vanish from the autoscaler's outstanding
        # sum and, worse, the replica would read as idle and become a
        # scale-down victim while an hour-long job still runs on it.
        # Terminal replicas are pruned so the cache stays bounded.
        nonterminal_ids = {
            info.replica_id for info in replica_infos if not info.is_terminal
        }
        translation_cache = {
            replica_id: cached
            for replica_id, cached in self._lb_translation_cache.items()
            if replica_id in nonterminal_ids
        }
        translation_cache.update(replica_cache)
        self._lb_translation_cache = translation_cache
        # Replacing the cache with this sync's active replicas prunes the
        # replicas that are no longer READY.
        self._lb_replica_cache = replica_cache
        return replica_info

    def _translate_in_flight(
            self,
            in_flight_by_url: Optional[Dict[str,
                                            int]]) -> Optional[Dict[int, int]]:
        """Translate the LB's url-keyed in-flight gauge to replica ids.

        [boltz fork] The LB only knows replicas by url; the autoscaler
        only knows them by id. `_lb_translation_cache` (replica_id ->
        (url, gpu_type, gpu_count), rebuilt by `_get_lb_replica_info` on
        every sync, so call that first) provides the inversion. It
        deliberately includes still-alive replicas that left READY, so a
        probe-blipped replica's running job stays attributed instead of
        vanishing (which would both shrink the outstanding sum and make
        the replica read as an idle scale-down victim). A url the cache
        does not know (the replica went terminal) is dropped: there is
        no live replica id to attribute the work to.

        None passes through: it means the LB (old version, or a policy
        that cannot track in-flight) sent no gauge, which the autoscaler
        must distinguish from an empty fleet.
        """
        if in_flight_by_url is None:
            return None
        url_to_replica_id = {
            url: replica_id
            for replica_id, (url, _, _) in self._lb_translation_cache.items()
        }
        in_flight_by_replica_id: Dict[int, int] = {}
        for url, count in in_flight_by_url.items():
            replica_id = url_to_replica_id.get(url)
            if replica_id is not None:
                in_flight_by_replica_id[replica_id] = int(count)
        return in_flight_by_replica_id

    def _get_capacity_hint(
            self, replica_infos: List['replica_managers.ReplicaInfo']
    ) -> Dict[str, int]:
        """Build the capacity_hint block of the sync response.

        [boltz fork] Computed from the replica_infos list the handler
        already fetched for `_get_lb_replica_info` -- no extra DB reads.

        - provisioning_replicas: latest-version, nonterminal, not-yet-
          ready replicas (capacity that will serve soon; lets the data
          plane hold spill decisions for capacity already on the way).
        - target_num_replicas: the autoscaler's current target. While the
          autoscaler's target may still be the rebuilt-blind minimum,
          report max(target, latest nonterminal count) instead: a routine
          controller restart must not tell the platform a live fleet
          wants to shrink. The floor keys on has_recomputed_with_fresh_
          data(), not has_fresh_demand_report(): the sync handler feeds
          the report BEFORE building this hint, so the very first
          post-restart sync is already "fresh" while the target stays
          min_replicas until the autoscaler thread's next decision tick
          consumes the snap.
        """
        latest_version = self._autoscaler.latest_version
        num_provisioning = 0
        num_latest_nonterminal = 0
        for info in replica_infos:
            if info.version != latest_version or info.is_terminal:
                continue
            num_latest_nonterminal += 1
            if not info.is_ready:
                num_provisioning += 1
        target = self._autoscaler.get_final_target_num_replicas()
        if not self._autoscaler.has_recomputed_with_fresh_data():
            target = max(target, num_latest_nonterminal)
        return {
            'provisioning_replicas': num_provisioning,
            'target_num_replicas': target,
        }

    def _get_routing_spec(self) -> Optional[Dict[str, Any]]:
        """Build the routing spec for the load_balancer_sync response.

        [boltz fork] The external load balancer fetches its routing
        configuration -- load-balancing policy, per-replica target QPS, and
        stream timeout -- over the sync channel instead of static launch
        args, so a `sky serve update` that only changes these fields reaches
        a running LB without re-rolling it. Sourced from the latest service
        version's spec (the same version the replica manager/autoscaler are
        advanced to on update). TLS is deliberately NOT synced -- it is
        bound to uvicorn at launch and a private key must not stream over
        this channel -- so it stays a launch/mounted-secret concern.
        Returns None when the spec cannot be loaded yet (mid-init); the LB
        tolerates a missing routing_spec and keeps its default policy until
        the next sync.
        """
        record = serve_state.get_service_from_name(self._service_name)
        if record is None:
            return None
        spec = serve_state.get_spec(self._service_name, record['version'])
        if spec is None:
            return None
        return {
            # `load_balancing_policy` resolves None to the default policy
            # name, so the LB always receives a concrete policy to build.
            'load_balancing_policy_name': spec.load_balancing_policy,
            'target_qps_per_replica': spec.target_qps_per_replica,
            # Lets an instance-aware LB weight replicas per-GPU when the
            # service sizes on concurrency (no QPS dict to weight by) --
            # and clear stale QPS weights after an update switches modes.
            'target_concurrency_per_replica':
                (getattr(spec, 'target_concurrency_per_replica', None)),
            'stream_timeout_seconds': spec.lb_stream_timeout_seconds,
            'retriable_status_codes': spec.lb_retriable_status_codes,
            'max_retries': spec.lb_max_retries,
            'retry_initial_backoff_seconds':
                (spec.lb_retry_initial_backoff_seconds),
        }

    def _start_reserved_capacity_poller_if_needed(self) -> None:
        """Start the reserved-capacity poller (idempotent).

        Called from run() (boot-enabled flag) and from update_service
        (flag enabled on a live service). Getters, not the live objects:
        update_service can replace self._autoscaler and the poller must
        feed the current one. The zero-cost location seeding that
        protects the pre-first-poll window is handled separately by
        _seed_fill_zero_cost_locations (at construction and on update).
        """
        placer = getattr(self._replica_manager, '_spot_placer', None)
        if placer is None:
            # The flag without a spot placer is inert (the placer defines
            # the zero-cost location set fill draws from): say so
            # instead of silently never filling. NOTE: the placer is
            # built once at ReplicaManager construction from the BOOT
            # spec -- an update that INTRODUCES the placer (adds the
            # any_of set / spot_placer field) cannot activate fill until
            # the next controller respawn; this is a pre-existing
            # property of the placer machinery, not of fill.
            logger.warning(
                'reserved_capacity_fill is enabled but the service has no '
                'spot placer (no any_of location set); fill is inactive '
                'until the controller is respawned with a placer-bearing '
                'spec.')
            return
        with self._reserved_capacity_poller_lock:
            if self._reserved_capacity_poller_started:
                return
            self._reserved_capacity_poller_started = True
        thread_utils.start_supervised_thread(
            lambda: reserved_capacity.poller_loop(
                lambda: self._autoscaler, lambda: getattr(
                    self._replica_manager, '_spot_placer', None)),
            'reserved-capacity-poller')

    def _run_autoscaler(self):
        logger.info('Starting autoscaler.')
        while True:
            try:
                replica_infos = serve_state.get_replica_infos(
                    self._service_name)
                # Use the active versions set by replica manager to make
                # sure we only scale down the outdated replicas that are
                # not used by the load balancer.
                record = serve_state.get_service_from_name(self._service_name)
                assert record is not None, ('No service record found for '
                                            f'{self._service_name}')
                active_versions = record['active_versions']
                logger.info(f'All replica info for autoscaler: {replica_infos}')

                # Autoscaler now extracts GPU type info directly from
                # replica_infos in generate_scaling_decisions method
                # for better decoupling.
                scaling_options = self._autoscaler.generate_scaling_decisions(
                    replica_infos, active_versions)
                # Batch consecutive SCALE_UP decisions into ONE
                # replica-manager call: each scale_up acquires the manager
                # lock, which the readiness-probe round holds for tens of
                # seconds per round on large fleets — per-decision calls
                # trickle through the gaps between rounds (measured live: a
                # 1000-target fleet enqueued only ~100 launches per several
                # minutes while the launch budget sat idle). One lock
                # acquisition per tick's upscale restores the launch budget
                # as the intended pacing mechanism. Decision ORDER is
                # preserved: scale-downs still execute at their original
                # position relative to the upscale batches around them.
                pending_scale_up: List[Optional[Dict[str, Any]]] = []

                def _flush_scale_up() -> None:
                    if pending_scale_up:
                        self._replica_manager.scale_up_batch(
                            list(pending_scale_up))
                        pending_scale_up.clear()

                for scaling_option in scaling_options:
                    logger.info(f'Scaling option received: {scaling_option}')
                    if (scaling_option.operator ==
                            autoscalers.AutoscalerDecisionOperator.SCALE_UP):
                        assert (scaling_option.target is None or isinstance(
                            scaling_option.target, dict)), scaling_option
                        pending_scale_up.append(scaling_option.target)
                    else:
                        assert isinstance(scaling_option.target,
                                          int), scaling_option
                        _flush_scale_up()
                        self._replica_manager.scale_down(scaling_option.target)
                _flush_scale_up()
            except Exception as e:  # pylint: disable=broad-except
                # No matter what error happens, we should keep the
                # monitor running.
                logger.error('Error in autoscaler: '
                             f'{common_utils.format_exception(e)}')
                with ux_utils.enable_traceback():
                    logger.error(f'  Traceback: {traceback.format_exc()}')
            time.sleep(self._autoscaler.get_decision_interval())

    def run(self) -> None:

        # Fail fast at boot if the in-cluster ServiceAccount lacks the RBAC
        # needed to manage the external load balancer objects. No-op outside
        # external-LB / in-cluster mode; degrades gracefully if the access
        # review itself cannot be issued.
        lb_rbac_preflight.check_lb_rbac_preflight()

        # Guard every control-plane endpoint with a shared bearer token (no-op
        # when unset). The read-only sync/status paths are gated too: the LB
        # presents the token on every sync (get_controller_auth_token, mounted
        # from the same Secret), so an in-cluster foothold can no longer scrape
        # the replica set. Safe to enforce from the first deploy -- there are no
        # live LBs to strand, and the token is present consistently on the
        # controller and every spawned LB.
        auth_dependency = fastapi.Depends(_make_auth_dependency())

        @self._app.get('/autoscaler/info', dependencies=[auth_dependency])
        async def get_autoscaler_info() -> fastapi.Response:
            return responses.JSONResponse(content=self._autoscaler.info(),
                                          status_code=200)

        @self._app.post('/controller/load_balancer_sync',
                        dependencies=[auth_dependency])
        async def load_balancer_sync(
                request: fastapi.Request) -> fastapi.Response:
            request_data = await request.json()
            # TODO(MaoZiming): Check aggregator type.
            request_aggregator: Dict[str, Any] = request_data.get(
                'request_aggregator', {})
            timestamps: List[int] = request_aggregator.get('timestamps', [])
            logger.info(f'Received {len(timestamps)} inflight requests.')
            # One replica-list fetch shared by the LB replica info and the
            # capacity hint (this handler is async on the event loop --
            # keep its added work O(replicas) with no new blocking DB
            # calls).
            replica_infos = serve_state.get_replica_infos(self._service_name)
            # Resolve the url->id mapping BEFORE translating in_flight:
            # _get_lb_replica_info rebuilds _lb_replica_cache from this
            # sync's READY replicas.
            lb_replica_info = self._get_lb_replica_info(replica_infos)
            # Merge the new demand gauges (all optional; an old LB simply
            # omits them) into the single dict handed to the autoscaler.
            # The QPS/pool autoscalers only .get('timestamps'), so the
            # extra keys are safe for every autoscaler type.
            request_information: Dict[str, Any] = {
                'timestamps': timestamps,
                'in_flight_by_replica_id': self._translate_in_flight(
                    request_data.get('in_flight')),
                'queue_depth': request_data.get('queue_depth'),
                'rejected_in_window': request_data.get('rejected_in_window'),
            }
            self._autoscaler.collect_request_information(request_information)

            return responses.JSONResponse(content={
                'replica_info': lb_replica_info,
                'routing_spec': self._get_routing_spec(),
                'capacity_hint': self._get_capacity_hint(replica_infos),
            },
                                          status_code=200)

        # Deliberately a sync handler: FastAPI runs it in the threadpool, so
        # waiting on the replica-manager lock inside `update_version` (a probe
        # round can hold it for tens of seconds when replicas are unreachable)
        # never stalls the event loop — /controller/load_balancer_sync must
        # keep serving while an update waits its turn.
        @self._app.post('/controller/update_service',
                        dependencies=[auth_dependency])
        def update_service(request_data: Dict[str, Any] = fastapi.Body(
            ...)) -> fastapi.Response:
            try:
                version = request_data.get('version', None)
                if version is None:
                    return responses.JSONResponse(
                        content={'message': 'Error: version is not specified.'},
                        status_code=400)
                update_mode_str = request_data.get(
                    'mode', serve_utils.DEFAULT_UPDATE_MODE.value)
                update_mode = serve_utils.UpdateMode(update_mode_str)
                logger.info(f'Update to new version {version} with '
                            f'update_mode {update_mode}.')
                # The yaml with the name latest_task_yaml will be synced
                # See sky/serve/core.py::update
                latest_task_yaml = serve_utils.generate_task_yaml_file_name(
                    self._service_name, version)
                with open(latest_task_yaml, 'r', encoding='utf-8') as f:
                    yaml_content = f.read()
                service = serve.SkyServiceSpec.from_yaml_str(yaml_content)
                serve_state.add_or_update_version(self._service_name, version,
                                                  service, yaml_content)
                logger.info(
                    f'Update to new version version {version}: {service}')

                self._replica_manager.update_version(version,
                                                     service,
                                                     update_mode=update_mode)
                new_autoscaler = autoscalers.Autoscaler.from_spec(
                    self._service_name, service)
                if not isinstance(self._autoscaler, type(new_autoscaler)):
                    logger.info('Autoscaler type changed to '
                                f'{type(new_autoscaler)}, updating autoscaler.')
                    old_autoscaler = self._autoscaler
                    new_autoscaler.load_dynamic_states(
                        old_autoscaler.dump_dynamic_states())
                    # Initialize the replacement to the update version BEFORE
                    # publishing it, so the autoscaler thread never observes a
                    # transient INITIAL_VERSION autoscaler (which would treat
                    # every live replica as outdated and churn).
                    new_autoscaler.update_version(version,
                                                  service,
                                                  update_mode=update_mode)
                    # Seed BEFORE publishing: if the old autoscaler's dump
                    # carried no fill state (build predating the feature,
                    # or fill just enabled), the replacement would
                    # otherwise take decision ticks with an empty zero-cost
                    # set until the next poll -- one tick with suppression
                    # off can terminate the whole fill fleet. A dump that
                    # did carry locations wins (the seed never overwrites).
                    self._seed_fill_zero_cost_locations(new_autoscaler)
                    self._autoscaler = new_autoscaler
                else:
                    self._autoscaler.update_version(version,
                                                    service,
                                                    update_mode=update_mode)
                if getattr(service, 'reserved_capacity_fill', False):
                    # An update can enable fill on a live service: give
                    # the (retained or replaced) autoscaler the location
                    # set so suppression works immediately (no-op when
                    # already populated), and make sure the poller
                    # exists -- without it fill would sit half-active
                    # (flag on, no free-slot feed) until a respawn.
                    self._seed_fill_zero_cost_locations(self._autoscaler)
                    self._start_reserved_capacity_poller_if_needed()
                return responses.JSONResponse(content={'message': 'Success'},
                                              status_code=200)
            except Exception as e:  # pylint: disable=broad-except
                exception_str = common_utils.format_exception(e)
                logger.error(f'Error in update_service: {exception_str}')
                return responses.JSONResponse(content={
                    'message': 'Error',
                    'exception': exception_str,
                    'traceback': traceback.format_exc()
                },
                                              status_code=500)

        @self._app.post('/controller/terminate_replica',
                        dependencies=[auth_dependency])
        async def terminate_replica(
                request: fastapi.Request) -> fastapi.Response:
            request_data = await request.json()
            replica_id = request_data['replica_id']
            assert isinstance(replica_id,
                              int), 'Error: replica ID must be an integer.'
            purge = request_data['purge']
            assert isinstance(purge, bool), 'Error: purge must be a boolean.'
            replica_info = serve_state.get_replica_info_from_id(
                self._service_name, replica_id)
            assert replica_info is not None, (f'Error: replica '
                                              f'{replica_id} does not exist.')
            replica_status = replica_info.status

            if replica_status == serve_state.ReplicaStatus.SHUTTING_DOWN:
                return responses.JSONResponse(
                    status_code=409,
                    content={
                        'message':
                            f'Replica {replica_id} of service '
                            f'{self._service_name!r} is already in the process '
                            f'of terminating. Skip terminating now.'
                    })

            if (replica_status in serve_state.ReplicaStatus.failed_statuses()
                    and not purge):
                return responses.JSONResponse(
                    status_code=409,
                    content={
                        'message': f'{colorama.Fore.YELLOW}Replica '
                                   f'{replica_id} of service '
                                   f'{self._service_name!r} is in failed '
                                   f'status ({replica_info.status}). '
                                   f'Skipping its termination as it could '
                                   f'lead to a resource leak. '
                                   f'(Use `sky serve down '
                                   f'{self._service_name!r} --replica-id '
                                   f'{replica_id} --purge` to '
                                   'forcefully terminate the replica.)'
                                   f'{colorama.Style.RESET_ALL}'
                    })

            self._replica_manager.scale_down(replica_id, purge=purge)

            action = 'terminated' if not purge else 'purged'
            message = (f'{colorama.Fore.GREEN}Replica {replica_id} of service '
                       f'{self._service_name!r} is scheduled to be '
                       f'{action}.{colorama.Style.RESET_ALL}\n'
                       f'Please use {ux_utils.BOLD}sky serve status '
                       f'{self._service_name}{ux_utils.RESET_BOLD} '
                       f'to check the latest status.')
            return responses.JSONResponse(status_code=200,
                                          content={'message': message})

        @self._app.exception_handler(Exception)
        async def validation_exception_handler(
                request: fastapi.Request, exc: Exception) -> fastapi.Response:
            with ux_utils.enable_traceback():
                logger.error(f'Error in controller: {exc!r}')
            return responses.JSONResponse(
                status_code=500,
                content={
                    'message':
                        (f'Failed method {request.method} at URL {request.url}.'
                         f' Exception message is {exc!r}.')
                },
            )

        # Supervised so a BaseException escaping the autoscaler loop (or the
        # loop returning) does not silently stop all scaling decisions while
        # the controller keeps serving HTTP -- it is restarted instead.
        thread_utils.start_supervised_thread(self._run_autoscaler, 'autoscaler')

        if self._reserved_capacity_fill_enabled:
            self._start_reserved_capacity_poller_if_needed()

        logger.info('SkyServe Controller started on '
                    f'http://{self._host}:{self._port}. PID: {os.getpid()}')

        try:
            uvicorn.run(self._app, host=self._host, port=self._port)
        except BaseException:  # pylint: disable=broad-except
            # The finally below hard-exits, which would otherwise swallow the
            # propagating exception -- log it so a crash-looping controller
            # leaves a post-mortem trace.
            logger.error('SkyServe Controller uvicorn server raised:\n'
                         f'{traceback.format_exc()}')
            raise
        finally:
            # If uvicorn.run() ever returns (a clean shutdown, a child-only
            # SIGINT raising KeyboardInterrupt, or any other exit), the HTTP
            # control plane is dead but the supervised control-loop threads
            # (autoscaler, replica refresher/prober/status-fetcher) are
            # non-daemon and loop forever, so the interpreter cannot exit and
            # the process lingers. The parent `_start` watchdog respawns the
            # controller only when `controller_process.is_alive()` is False,
            # so a lingering process is never respawned and the service is
            # stuck with no working controller. Hard-exit so the parent
            # observes the death and respawns on a fresh port.
            logger.error('SkyServe Controller uvicorn server exited; '
                         'terminating the subprocess so the parent can '
                         'respawn the controller.')
            os._exit(1)  # pylint: disable=protected-access


# TODO(tian): Probably we should support service that will stop the VM in
# specific time period.
def run_controller(service_name: str, service_spec: serve.SkyServiceSpec,
                   version: int, controller_host: str, controller_port: int):
    os.environ[constants.OVERRIDE_CONSOLIDATION_MODE] = 'true'
    # Hijack sys.stdout/stderr to be context aware.
    context_utils.hijack_sys_attrs()
    controller = SkyServeController(service_name, service_spec, version,
                                    controller_host, controller_port)
    controller.run()
