"""SkyServeController: the central controller of SkyServe.

Responsible for autoscaling and replica management.
"""
import asyncio
from collections.abc import Callable
import contextlib
import functools
import hmac
import logging
import os
import threading
import time
import traceback
from typing import Any, NamedTuple

import colorama
import fastapi
from fastapi import responses
import uvicorn

from sky import global_user_state
from sky import serve
from sky import sky_logging
from sky import task as task_lib
from sky.serve import autoscalers
from sky.serve import constants as serve_constants
from sky.serve import lb_ha
from sky.serve import lb_ha_observability as lb_ha_obs
from sky.serve import lb_k8s
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import serve_history
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import context_utils
from sky.utils import thread_utils
from sky.utils import ux_utils

logger = sky_logging.init_logger(__name__)


def _make_auth_dependency(*,
                          sync: bool = False,
                          required: bool = False) -> Callable:
    """Build a dependency for one purpose-specific controller token ring.

    Rings are read fresh for every request, so mounted Secret rotations do not
    require a controller restart. The sync endpoint accepts only the LB-sync
    ring; administrative/status endpoints accept only the admin ring. A broken
    required ring returns 503 (fail closed without claiming the caller merely
    supplied a bad credential); a present but nonmatching credential returns
    401.
    """

    async def _verify(authorization: str | None = fastapi.Header(None)) -> None:
        getter = (serve_utils.get_lb_sync_auth_tokens
                  if sync else serve_utils.get_controller_admin_auth_tokens)
        try:
            expected_tokens = getter(required=required)
        except serve_utils.AuthTokenConfigurationError as e:
            logger.error('Controller authentication is unavailable: %s', e)
            raise fastapi.HTTPException(
                status_code=503,
                detail='Controller authentication is unavailable.') from e
        if not expected_tokens:
            return
        # isascii() guards hmac.compare_digest, which raises TypeError on a
        # non-ASCII str -- a malformed header must be a clean 401, not a 500.
        if authorization is None or not authorization.isascii():
            raise fastapi.HTTPException(status_code=401, detail='Unauthorized.')
        authorized = False
        for expected_token in expected_tokens:
            # Evaluate every ring member instead of short-circuiting on the
            # first match, keeping request timing independent of token order.
            authorized |= hmac.compare_digest(authorization,
                                              f'Bearer {expected_token}')
        if not authorized:
            raise fastapi.HTTPException(status_code=401, detail='Unauthorized.')

    return _verify


def _make_controller_owner_dependency(
        controller_owner_fingerprint: str) -> Callable:
    """Fence every child request to the exact controller owner tuple."""

    async def _verify(requested_owner: str | None = fastapi.Header(
        None, alias=serve_constants.CONTROLLER_OWNER_HEADER)) -> None:
        if requested_owner != controller_owner_fingerprint:
            raise fastapi.HTTPException(
                status_code=409, detail='Controller owner identity mismatch.')

    return _verify


def _read_declared_submitted_yaml(request_data: dict[str, Any],
                                  service_name: str, version: int,
                                  resource_scope: str | None) -> str | None:
    """Read only the submitted YAML declared by this update request."""
    if request_data.get('has_submitted_yaml') is not True:
        return None
    path = serve_utils.generate_submitted_task_yaml_file_name(
        service_name, version, resource_scope=resource_scope)
    try:
        with open(path, encoding='utf-8') as f:
            return f.read()
    except OSError as e:
        logger.warning(
            'Submitted YAML declared for service %r version %s '
            'is unavailable at %s: %s', service_name, version, path, e)
        return None


class AutoscalerInfoFilter(logging.Filter):

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not ('GET' in message and '200' in message and
                    '/autoscaler/info' in message)


class _PendingServiceUpdate(NamedTuple):
    version: int
    service: Any
    update_mode: serve_utils.UpdateMode
    committed_at: float


_UPDATE_RETRY_BACKOFF_SECONDS = 5


class SkyServeController:
    """SkyServeController: control everything about replica.

    This class is responsible for:
        - Starting and terminating the replica monitor and autoscaler.
        - Providing the HTTP Server API for SkyServe to communicate with.
    """

    def __init__(self,
                 service_name: str,
                 service_spec: serve.SkyServiceSpec,
                 version: int,
                 host: str,
                 port: int,
                 controller_owner_fingerprint: str,
                 resource_scope: str | None = None,
                 service_hash: str | None = None,
                 controller_pid: int | None = None,
                 controller_ip: str | None = None,
                 enforce_launch_fence: bool = False) -> None:
        self._service_name = service_name
        self._resource_scope = resource_scope
        self._service_hash = service_hash
        self._controller_owner = ((controller_pid,
                                   controller_ip) if service_hash is not None or
                                  controller_pid is not None or
                                  controller_ip is not None else None)
        if (service_hash is not None and
                serve_state.service_uses_logical_replica_semantics(service_name)
                and getattr(service_spec, 'uses_logical_replicas',
                            False) is not True):
            raise RuntimeError(
                'Refusing to recover a service whose durable logical-replica '
                'activation fence disagrees with its latest specification.')
        # Serialize durable update commits. The live manager/autoscaler
        # transition happens on the reconciler below: a fleet-wide probe can
        # hold the replica-manager lock for minutes, but a second update must
        # still be able to commit while the first transition waits for it.
        self._update_lock = threading.Lock()
        self._update_condition = threading.Condition()
        self._pending_update: _PendingServiceUpdate | None = None
        # A controller child always boots from the latest committed version,
        # so its initial runtime and durable state agree.
        self._committed_version = version
        self._applied_version = version
        self._update_apply_error: str | None = None
        self._update_apply_failures = 0
        # Serialize LB snapshots while resolving a cold replica cache in the
        # threadpool. Concurrent LB Pods can overlap during a rollout; without
        # this lock they would duplicate the fleet-wide endpoint work and race
        # to replace the shared routing/translation caches. Create the asyncio
        # lock lazily inside the running server loop: on Python 3.9 eager lock
        # construction fails if an earlier asyncio.run() closed the thread's
        # current loop.
        self._lb_sync_lock: asyncio.Lock | None = None
        self._lb_role_lock: asyncio.Lock | None = None
        durable_lb_state = (serve_state.get_lb_cutover_state(service_name)
                            if service_hash is not None else None)
        self._lb_ha_enabled = (
            durable_lb_state.enabled if durable_lb_state is not None else
            getattr(service_spec, 'lb_high_availability', False) is True)
        self._lb_session_ledger = (lb_ha.LbSessionLedger(
            serve_constants.LB_ROLE_REPORT_MAX_AGE_SECONDS,
            serve_constants.LB_PROMOTION_OCCUPANCY_MAX_AGE_SECONDS)
                                   if self._lb_ha_enabled else None)
        self._lb_expected_occupancy_urls: set[str] = set()
        # An empty set means "synchronous service" only after one complete
        # routing sync. Before that, it means "contract unknown" and must not
        # make promotion vacuously safe after a controller restart.
        self._lb_occupancy_contract_known = False
        self._lb_last_demand_snapshot = (
            serve_state.get_lb_last_demand_snapshot(service_name)
            if self._lb_ha_enabled else None)
        self._lb_demand_handoff = lb_ha.DemandHandoff(
            serve_constants.LB_DEMAND_HANDOFF_SECONDS)
        self._lb_drain_timeout_seconds = (
            lb_k8s.lb_termination_grace_period_seconds(
                service_spec.lb_stream_timeout_seconds,
                service_spec.graceful_drain_seconds)
            if self._lb_ha_enabled else 0)
        self._controller_owner_fingerprint = controller_owner_fingerprint
        self._is_pool = service_spec.pool
        self._replica_manager: replica_managers.ReplicaManager = (
            replica_managers.SkyPilotReplicaManager(
                service_name=service_name,
                spec=service_spec,
                version=version,
                resource_scope=resource_scope,
                service_hash=service_hash,
                controller_pid=controller_pid,
                controller_ip=controller_ip,
                enforce_launch_fence=enforce_launch_fence))
        # Pass `version` so a controller rebuilt on restart/respawn starts the
        # autoscaler at the recovered latest version (matching the replica
        # manager above), not INITIAL_VERSION. Otherwise a service updated past
        # v1 would have its autoscaler treat every live replica as outdated and
        # churn replicas forever after any restart.
        self._autoscaler: autoscalers.Autoscaler = (
            autoscalers.Autoscaler.from_spec(service_name, service_spec,
                                             version))
        self._configure_instance_aware_accelerators(service_spec)
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
        self._lb_replica_cache: dict[int, tuple[str, str, int]] = {}
        # Superset of _lb_replica_cache for url -> replica_id translation
        # of the LB's in-flight report: keeps entries for replicas that
        # left READY but are still nonterminal, so a probe-blipped
        # replica's running job stays attributed to it (see
        # _get_lb_replica_info / _translate_in_flight).
        self._lb_translation_cache: dict[int, tuple[str, str, int]] = {}
        # Monotonic generation for complete LB demand/capacity reports. It is
        # intentionally process-local: after restart logical scale-down stays
        # disabled until the new controller consumes a fresh report.
        self._reconcile_generation = 0
        # Immutable routing configuration shipped to the external load
        # balancer. Stored in-memory and updated only after the controller's
        # live autoscaler / replica-manager state transitions, so syncs never
        # advertise a newer routing policy than the runtime has actually
        # applied.
        self._routing_spec = self._build_routing_spec(service_spec)
        # Refreshed only by autoscaler/LB-sync paths that already hold a full
        # replica snapshot. Status polling reads this without new DB/API work.
        self._replica_counts_snapshot: dict[str, int | str] | None = None
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
        """Best-effort seed of an autoscaler's zero-cost location set.

        zero_cost_locations() computes per-location costs via the
        placer's cache, but an UNCACHED Kubernetes location's cost
        lookup CAN hit the live Kubernetes API (instance-fit check), so
        a transient API blip in that window can raise. Seeding is
        therefore best-effort: any failure is logged and swallowed,
        degrading to the documented pre-seed behavior (empty location
        set; suppression engages after the first successful poll feeds
        it) instead of killing controller boot / update_service. The
        seed only sets the location identity set (no free slots, no
        snapshot time), and an already-populated set (e.g. loaded from a
        dump) is never overwritten -- see
        Autoscaler.seed_zero_cost_locations.
        """
        if not autoscaler.reserved_capacity_fill:
            return
        placer = self._replica_manager.spot_placer
        if placer is None:
            return
        try:
            autoscaler.seed_zero_cost_locations([
                location.to_pickleable()
                for location in placer.zero_cost_locations()
            ])
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to seed zero-cost locations '
                           '(best-effort; will rely on the first '
                           'successful poll instead): '
                           f'{common_utils.format_exception(e)}')

    def _get_lb_replica_info(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        async_occupancy_by_version: dict[int, bool | None] | None = None,
    ) -> tuple[dict[str, dict[str, str]], int]:
        """Build the url -> replica info mapping for load_balancer_sync.

        [boltz fork] Resolving a replica's url and gpu_type is expensive, so
        cluster records and provider configs for newly-READY replicas are each
        fetched in one batched lookup. The resulting endpoint/accelerator data
        is cached for the replica's lifetime. A warm sync performs neither
        lookup.
        A brand-new replica whose gpu_type cannot be resolved yet is reported
        as 'unknown' until it is.

        `replica_infos` is fetched once by the caller and shared with the
        capacity-hint computation, so the async sync handler issues no
        extra replica-list DB reads.

        Returns the (url -> info) mapping and the number of READY, active
        replicas seen -- which can exceed len(mapping) when a READY replica's
        endpoint is transiently unresolvable this round. The load balancer uses
        that count to tell an authoritative zero (no READY replicas) apart from
        a spurious empty map (READY replicas exist but none resolved), so it
        never blanks a healthy routing set on a transient blip.
        """
        runtime_snapshot = serve_state.get_service_runtime_snapshot(
            self._service_name, require_version=True)
        assert runtime_snapshot is not None, ('No service record found for '
                                              f'{self._service_name}')
        service_hash = getattr(self, '_service_hash', None)
        controller_owner = getattr(self, '_controller_owner', None)
        if (service_hash is not None and
            (runtime_snapshot.get('hash') != service_hash or
             (runtime_snapshot.get('controller_pid'),
              runtime_snapshot.get('controller_ip')) != controller_owner)):
            raise RuntimeError('Controller ownership changed while building '
                               'the load balancer routing snapshot.')
        active_versions = set(runtime_snapshot['active_versions'])
        replica_cache: dict[int, tuple[str, str, int]] = {}
        replica_info: dict[str, dict[str, str]] = {}
        ready_infos = [
            info for info in replica_infos
            if (info.status == serve_state.ReplicaStatus.READY and
                info.version in active_versions)
        ]
        uncached_cluster_names = [
            info.cluster_name
            for info in ready_infos
            if info.replica_id not in self._lb_replica_cache
        ]
        cluster_records: dict[str, dict[str, Any] | None] = {}
        if uncached_cluster_names:
            cluster_records = global_user_state.get_clusters_from_names(
                uncached_cluster_names)

        # get_endpoints normally reads and parses each cluster YAML to obtain
        # its provider config. That is another fleet-sized DB N+1. Reuse the
        # records above to collect the YAML paths, then fetch all YAMLs in one
        # query before resolving endpoints.
        uncached_handles: dict[int, Any] = {}
        yaml_replica_ids: list[int] = []
        yaml_paths: list[str] = []
        for info in ready_infos:
            if info.replica_id in self._lb_replica_cache:
                continue
            cluster_record = cluster_records.get(info.cluster_name)
            if cluster_record is None:
                continue
            handle = info.handle(cluster_record)
            uncached_handles[info.replica_id] = handle
            cluster_yaml = getattr(handle, 'cluster_yaml', None)
            if cluster_yaml is not None:
                yaml_replica_ids.append(info.replica_id)
                yaml_paths.append(cluster_yaml)
        provider_configs: dict[int, dict[str, Any]] = {}
        if yaml_paths:
            yaml_configs = global_user_state.get_cluster_yaml_dict_multiple(
                yaml_paths)
            provider_configs = {
                replica_id: config['provider']
                for replica_id, config in zip(yaml_replica_ids, yaml_configs)
            }

        for info in ready_infos:
            cached = self._lb_replica_cache.get(info.replica_id)
            if cached is None:
                cluster_record = cluster_records.get(info.cluster_name)
                if cluster_record is None:
                    logger.warning(f'Replica {info.replica_id} is READY but '
                                   'its cluster record is not available yet; '
                                   'skipping for this sync.')
                    continue
                handle = uncached_handles.get(info.replica_id)
                url = info._resolve_url(  # pylint: disable=protected-access
                    cluster_record=cluster_record,
                    handle=handle,
                    provider_config=provider_configs.get(info.replica_id))
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
            is_zero_cost = getattr(info, 'is_zero_cost', None)
            if isinstance(is_zero_cost, bool):
                # Placement-cost provenance is independent from the launch
                # reason: an ordinary demand launch may land on free reserved
                # capacity and should receive the same economic tie-break.
                replica_info[url]['is_zero_cost'] = ('true' if is_zero_cost else
                                                     'false')
            async_occupancy = ((async_occupancy_by_version or
                                {}).get(info.version))
            if async_occupancy is not None:
                # Per-replica (not latest-service) declaration: during a
                # rolling update old and new versions may have different
                # fast-ack contracts. Emit explicit false as a tri-state
                # protocol: omission means old controller / preserve prior LB
                # knowledge, while false starts a two-phase disable that keeps
                # old work visible until a generation-valid idle sample.
                replica_info[url]['async_occupancy'] = (
                    'true' if async_occupancy else 'false')
        # The translation cache retains entries for replicas that left
        # READY but are still alive: the LB's in-flight snapshot is taken
        # against ITS last routing view, so a replica probe-blipped out
        # of READY mid-job would otherwise become untranslatable -- its
        # in-flight unit would vanish from the autoscaler's outstanding
        # sum and, worse, the replica would read as idle and become a
        # scale-down victim while an hour-long job still runs on it.
        # Terminal replicas (SHUTTING_DOWN included) are pruned so the
        # cache stays bounded AND so a retiring replica's in-flight stops
        # counting toward the autoscaler's outstanding-work sum: its
        # requests are pinned to it and cannot be re-routed, so counting
        # them as demand would launch phantom replacement capacity for
        # the whole drain window. The retirement drain itself does not
        # need translation -- it matches the LB's raw url-keyed report
        # against the replica's own url (see _ReplicaDrainTracker).
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
        return replica_info, len(ready_infos)

    def _url_to_replica_id_map(self) -> dict[str, int]:
        """Invert the translation cache (url -> replica id)."""
        return {
            url: replica_id
            for replica_id, (url, _, _) in self._lb_translation_cache.items()
        }

    def _translate_in_flight(
            self,
            in_flight_by_url: dict[str, int] | None) -> dict[int, int] | None:
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
        url_to_replica_id = self._url_to_replica_id_map()
        in_flight_by_replica_id: dict[int, int] = {}
        for url, count in in_flight_by_url.items():
            replica_id = url_to_replica_id.get(url)
            if replica_id is not None:
                in_flight_by_replica_id[replica_id] = int(count)
        return in_flight_by_replica_id

    def _translate_observed_slots(
            self, slots_by_url: dict[str, int] | None) -> dict[int, int]:
        """Translate a generation-valid LB slot map to durable replica IDs."""
        if slots_by_url is None:
            return {}
        url_to_replica_id = self._url_to_replica_id_map()
        return {
            url_to_replica_id[url]: max(0, int(slots))
            for url, slots in slots_by_url.items()
            if url in url_to_replica_id
        }

    def _logical_bridge_capacity_candidates(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        logical_versions: set[int],
        observed_slots: dict[int, int],
    ) -> dict[int, int]:
        """Return fresh physical-bridge widths bounded by launched GPUs."""
        if getattr(self._autoscaler, 'replica_unit', None) != 'logical':
            return {}
        candidates: dict[int, int] = {}
        for info in replica_infos:
            if info.version in logical_versions or info.is_terminal:
                continue
            observed = observed_slots.get(info.replica_id)
            if observed is None or observed < 1:
                continue
            current = getattr(info, 'planned_capacity', 1)
            if (isinstance(current, bool) or not isinstance(current, int) or
                    current < 1):
                current = 1
            durable_bound = (current if bool(
                getattr(info, 'logical_bridge_capacity_verified', False)) else
                             1)
            cached = self._lb_translation_cache.get(info.replica_id)
            if cached is None:
                observed_slots[info.replica_id] = min(observed, durable_bound)
                continue
            gpu_type, gpu_count = cached[1], cached[2]
            if (gpu_type == 'unknown' or isinstance(gpu_count, bool) or
                    not isinstance(gpu_count, int) or gpu_count < 1):
                observed_slots[info.replica_id] = min(observed, durable_bound)
                continue
            verified = min(observed, gpu_count)
            # Pass only state transitions to the manager. This keeps the LB
            # sync hot path free of a redundant DB read/write per heartbeat.
            if (not bool(
                    getattr(info, 'logical_bridge_capacity_verified', False)) or
                    verified > current):
                candidates[info.replica_id] = verified
            # Reporter-controlled values must never reach the autoscaler above
            # the independently resolved hardware bound.
            observed_slots[info.replica_id] = verified
        return candidates

    def _unknown_async_replica_ids(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        async_occupancy_by_version: dict[int, bool | None],
        occupancy_sampled_urls: list[str] | None,
        unknown_in_flight_urls: list[str] | None,
        force_all_live_unknown: bool = False,
    ) -> set[int]:
        """Resolve the fail-closed async occupancy set for one LB report.

        An envelope count (including explicit zero) does not prove anything
        about fast-ack work. A declared async replica is known only when the
        LB says its numeric entry includes a validity-filtered occupancy
        sample. Old LBs omit that proof, and a first sync necessarily precedes
        application of the controller's declaration, so both remain unknown.
        """
        url_to_replica_id = self._url_to_replica_id_map()
        sampled_replica_ids: set[int] = set()
        if not force_all_live_unknown:
            sampled_replica_ids = {
                url_to_replica_id[url]
                for url in (occupancy_sampled_urls or [])
                if url in url_to_replica_id
            }
        unknown_replica_ids = {
            url_to_replica_id[url]
            for url in (unknown_in_flight_urls or [])
            if url in url_to_replica_id
        }
        live_infos = [
            info for info in replica_infos
            if info.status in (serve_state.ReplicaStatus.READY,
                               serve_state.ReplicaStatus.NOT_READY)
        ]
        if force_all_live_unknown:
            # Two maxSurge LBs publish last-writer-wins gauges. A sampled zero
            # from either cannot prove that the other accepted no work, so the
            # short overlap/grace fails closed for every live replica,
            # including legacy services without a declaration.
            unknown_replica_ids.update(info.replica_id for info in live_infos)
        else:
            unknown_replica_ids.update(
                info.replica_id
                for info in live_infos
                if async_occupancy_by_version.get(info.version, False) and
                info.replica_id not in sampled_replica_ids)
        return unknown_replica_ids

    def _lb_report_authority(self,
                             session_id: str | None) -> tuple[bool, bool, bool]:
        """Return ``(live member, demand, drain)`` report authority.

        The sole Ready, non-terminating Pod sees all new Service traffic, so it
        may continue feeding demand while an old terminating Pod finishes a
        long stream. Idleness and drain completion are service-wide claims:
        they remain authoritative only when the reporter is the sole live Pod,
        including terminating Pods. Both decisions use one Kubernetes list.
        """
        try:
            pod_authority = lb_k8s.get_lb_pod_authority(self._service_name)
        except Exception as e:  # pylint: disable=broad-except
            # The lifecycle helper already converts Kubernetes API failures to
            # None. Keep this boundary defensive too: report validation must
            # fail closed even if an unexpected adapter error escapes it.
            logger.warning('Failed to validate the live load balancer Pod '
                           f'set: {common_utils.format_exception(e)}')
            return False, False, False
        if session_id is None or pod_authority is None:
            return False, False, False
        reporter_is_live = session_id in pod_authority.live_uids
        if pod_authority.slot_by_uid is not None:
            reporter_slot = pod_authority.slot_by_uid.get(session_id)
            try:
                state = serve_state.get_lb_cutover_state(self._service_name)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Failed to read HA load balancer authority: '
                               f'{common_utils.format_exception(e)}')
                return False, False, False
            coherent = (state is not None and state.enabled and
                        state.active_slot is not None and
                        state.active_slot == pod_authority.selected_slot)
            active_slot = state.active_slot if state is not None else None
            reporter_ready = (reporter_is_live and session_id
                              in pod_authority.ready_nonterminating_uids)
            legacy_selected = (state is not None and state.phase
                               in (lb_ha.LbCutoverPhase.MIGRATING,
                                   lb_ha.LbCutoverPhase.ROLLING_BACK) and
                               pod_authority.selected_slot is None and
                               pod_authority.legacy_uids is not None and
                               session_id in pod_authority.legacy_uids)
            live_slot_uids = (set(pod_authority.slot_by_uid) &
                              pod_authority.live_uids)
            legacy_drain_authoritative = (
                legacy_selected and
                pod_authority.legacy_uids == {session_id} and
                state is not None and
                (state.phase is lb_ha.LbCutoverPhase.MIGRATING or
                 not live_slot_uids))
            demand_authoritative = reporter_ready and (
                (coherent and reporter_slot == active_slot) or legacy_selected)
            # HA drain authority is service-wide and comes from the bounded
            # ACTIVE+DRAINING session ledger, never a single Pod report.
            return (reporter_is_live, demand_authoritative,
                    legacy_drain_authoritative)
        demand_authoritative = (pod_authority.ready_nonterminating_uids == {
            session_id
        })
        drain_authoritative = pod_authority.live_uids == {session_id}
        return reporter_is_live, demand_authoritative, drain_authoritative

    @staticmethod
    def _lb_drain_report_view(
        request_data: dict[str, Any],
        report_is_authoritative: bool,
    ) -> tuple[dict[str, int] | None, list[str] | None]:
        """Return a raw drain view that only the sole live LB can prove."""
        in_flight = request_data.get('in_flight')
        routing_urls = request_data.get('routing_urls')
        if not report_is_authoritative:
            # Never publish any field from a non-authoritative reporter. In
            # particular, replacing a trusted report with a synthetic blocking
            # report would still let a different service's/rollout Pod mutate
            # this service's drain session and freshness timestamp.
            return None, None
        return in_flight, routing_urls

    async def _confirm_logical_bridge_capacities(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        logical_versions: set[int],
        observed_slots: dict[int, int],
    ) -> None:
        """Bound and durably confirm physical-bridge logical capacities."""
        bridge_candidates = self._logical_bridge_capacity_candidates(
            replica_infos, logical_versions, observed_slots)
        if not bridge_candidates:
            return
        confirmer = getattr(self._replica_manager,
                            'confirm_logical_bridge_capacities', None)
        if not callable(confirmer):
            return
        loop = asyncio.get_running_loop()
        try:
            confirmed = await loop.run_in_executor(None, confirmer,
                                                   bridge_candidates)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to persist verified logical bridge '
                           'capacity; retaining conservative width one: '
                           f'{common_utils.format_exception(e)}')
            return
        infos_by_id = {info.replica_id: info for info in replica_infos}
        for replica_id, capacity in confirmed.items():
            info = infos_by_id.get(replica_id)
            if info is not None:
                info.planned_capacity = capacity
                info.logical_bridge_capacity_verified = True

    async def _ingest_load_balancer_report(
        self,
        request_data: dict[str, Any],
        replica_infos: list['replica_managers.ReplicaInfo'],
        async_occupancy_by_version: dict[int, bool | None],
        authority: tuple[bool, bool, bool] | None = None,
        logical_versions: set[int] | None = None,
    ) -> bool:
        """Apply the independently authorized parts of one external LB report.

        A sole Ready reporter keeps scale-up demand live during maxSurge. Until
        every older Pod is gone, its zero gauges cannot prove service-wide
        idleness: all live replicas remain occupancy-unknown and the replica
        manager receives a controller-generated blocking drain view. A wrong
        UID, multiple Ready Pods, or a failed lookup cannot mutate either
        subsystem.
        """
        if authority is None:
            loop = asyncio.get_running_loop()
            authority = await loop.run_in_executor(
                None, self._lb_report_authority,
                request_data.get('lb_session_id'))
        observed_slots: dict[int, int] = {}
        if authority[1]:
            observed_slots = self._translate_observed_slots(
                request_data.get('total_slots_by_url'))
            if logical_versions is not None:
                await self._confirm_logical_bridge_capacities(
                    replica_infos, logical_versions, observed_slots)
        return self._apply_load_balancer_report(
            request_data,
            replica_infos,
            async_occupancy_by_version,
            authority,
            observed_slots,
        )

    def _apply_load_balancer_report(
        self,
        request_data: dict[str, Any],
        replica_infos: list['replica_managers.ReplicaInfo'],
        async_occupancy_by_version: dict[int, bool | None],
        authority: tuple[bool, bool, bool],
        observed_slots: dict[int, int],
    ) -> bool:
        """Synchronously mutate runtime state from one prepared LB report."""
        (reporter_is_live, demand_authoritative,
         drain_authoritative) = authority
        ha_enabled = getattr(self, '_lb_ha_enabled', False)
        if not reporter_is_live:
            logger.warning('Ignoring non-authoritative load balancer demand '
                           'and drain report for service '
                           f'{self._service_name!r}.')
            return False
        if not demand_authoritative and not drain_authoritative:
            # Either genuine Pod may refresh routing during the two-Ready
            # maxSurge window, but neither may mutate last-writer-wins state.
            return True

        if demand_authoritative:
            effective_request_data = request_data
            if ha_enabled:
                state = serve_state.get_lb_cutover_state(self._service_name)
                if (state is not None and
                        state.phase is not lb_ha.LbCutoverPhase.PREPARING):
                    self._restore_lb_demand_handoff(state.generation)
                    sampled_urls = set(
                        request_data.get('occupancy_sampled_urls', []))
                    complete_report = bool(
                        getattr(self, '_lb_occupancy_contract_known',
                                False)) and getattr(
                                    self, '_lb_expected_occupancy_urls',
                                    set()).issubset(sampled_urls)
                    handoff = getattr(self, '_lb_demand_handoff', None)
                    if handoff is not None:
                        if (complete_report and
                                handoff.complete_report_at is None and
                                handoff.generation == state.generation):
                            fence = self._lb_cutover_fence()
                            if fence is not None:
                                service_hash, owner, lifecycle_epoch = fence
                                complete_at = (
                                    serve_state.mark_lb_demand_handoff_complete(
                                        self._service_name, service_hash, owner,
                                        lifecycle_epoch, state.generation))
                                handoff.restore(handoff.generation,
                                                handoff.snapshot, complete_at)
                        handoff_generation = handoff.generation
                        effective_request_data = handoff.apply(
                            state.generation, request_data, complete_report)
                        if (handoff_generation is not None and
                                handoff.generation is None):
                            fence = self._lb_cutover_fence()
                            if fence is not None:
                                service_hash, owner, lifecycle_epoch = fence
                                serve_state.clear_lb_demand_handoff(
                                    self._service_name, service_hash, owner,
                                    lifecycle_epoch, handoff_generation)
                demand_snapshot = lb_ha.DemandSnapshot.from_request(
                    request_data)
                self._lb_last_demand_snapshot = demand_snapshot
                if (state is not None and state.active_slot is not None and
                        state.phase in (lb_ha.LbCutoverPhase.STABLE,
                                        lb_ha.LbCutoverPhase.DRAINING)):
                    fence = self._lb_cutover_fence()
                    if fence is not None:
                        service_hash, owner, lifecycle_epoch = fence
                        serve_state.record_lb_active_demand_snapshot(
                            self._service_name, service_hash, owner,
                            lifecycle_epoch, state.active_slot,
                            state.generation, demand_snapshot)
            # Parse reporter-controlled demand only after its dedicated gate.
            # Besides preventing state mutation, this keeps a stale/wrong Pod
            # from making the controller reject a useful routing response with
            # a malformed demand-only field.
            request_aggregator: dict[str, Any] = effective_request_data.get(
                'request_aggregator', {})
            timestamps: list[int] = request_aggregator.get('timestamps', [])
            compatibility_profiles = request_aggregator.get(
                'compatibility_profiles', [])
            queued_compatibility_profiles = effective_request_data.get(
                'queued_requests_by_compatibility', [])
            logger.info(f'Received {len(timestamps)} inflight requests.')
            translated_in_flight = self._translate_in_flight(
                effective_request_data.get('in_flight'))
            unknown_replica_ids = self._unknown_async_replica_ids(
                replica_infos,
                async_occupancy_by_version,
                effective_request_data.get('occupancy_sampled_urls', []),
                effective_request_data.get('unknown_in_flight_urls', []),
                force_all_live_unknown=(not drain_authoritative and
                                        not ha_enabled))
            self._reconcile_generation = getattr(self, '_reconcile_generation',
                                                 0) + 1
            reconcile_generation = self._reconcile_generation
            self._autoscaler.collect_request_information({
                'timestamps': timestamps,
                'compatibility_profiles': compatibility_profiles,
                'queued_requests_by_compatibility': queued_compatibility_profiles,
                'in_flight_by_replica_id': translated_in_flight,
                'unknown_in_flight_replica_ids': list(unknown_replica_ids),
                'observed_slots_by_replica_id': observed_slots,
                # During maxSurge overlap, no LB can prove service-wide async
                # occupancy. Keep those backends drain-busy, but do not age the
                # degraded-capacity replacement timer: the old Pod may simply
                # be finishing a long stream. Replacement becomes eligible
                # only from a sole-live authoritative reporter's real probe
                # miss.
                'unknown_capacity_replica_ids': list(unknown_replica_ids if (
                    drain_authoritative or ha_enabled) else ()),
                'reconcile_generation': reconcile_generation,
                'queue_depth': effective_request_data.get('queue_depth'),
                'rejected_in_window':
                    effective_request_data.get('rejected_in_window'),
            })
            if (translated_in_flight is not None and getattr(
                    self._autoscaler, 'replica_unit', None) == 'logical'):
                self._replica_manager.update_logical_reconcile_snapshot(
                    version=self._autoscaler.latest_version,
                    generation=reconcile_generation,
                    observed_slots_by_replica_id=observed_slots,
                    in_flight_by_replica_id=translated_in_flight,
                    unknown_replica_ids=unknown_replica_ids)

        if ha_enabled and not drain_authoritative:
            # The fast role channel aggregates ACTIVE and DRAINING sessions.
            # A slot sync must never overwrite that service-wide view. During
            # legacy-selected migration/rollback, the sole legacy Pod remains
            # the stream authority until the stable selector actually moves.
            return True
        if drain_authoritative:
            drain_in_flight, drain_routing_urls = self._lb_drain_report_view(
                request_data, report_is_authoritative=True)
            unknown_urls = request_data.get('unknown_in_flight_urls')
            draining_urls = request_data.get('draining_urls')
        else:
            # This is the legitimate sole Ready Pod, but another live Pod may
            # still own streams. Replace any formerly trusted clean snapshot
            # with a controller-generated blocking view; never copy an
            # overlap reporter's drain fields into the replica manager.
            drain_in_flight, drain_routing_urls = {}, None
            unknown_urls, draining_urls = [], []
        self._replica_manager.update_lb_in_flight(
            drain_in_flight, drain_routing_urls, unknown_urls, draining_urls,
            request_data.get('lb_session_id'))
        return True

    async def _handle_load_balancer_sync(
            self, request_data: dict[str, Any]) -> fastapi.Response:
        """Validate LB membership before disclosing confidential routing."""
        # Every DB read below (ownership fences, replica/spec snapshot) runs
        # in the executor: on a large replica table these are the sync
        # handler's blocking calls, and the FastAPI event loop must stay free
        # so controller liveness and ownership probes remain responsive.
        loop = asyncio.get_running_loop()
        if not await loop.run_in_executor(None, self._owns_current_service):
            return fastapi.Response(status_code=503)
        lb_sync_lock = self._lb_sync_lock
        if lb_sync_lock is None:
            lb_sync_lock = asyncio.Lock()
            self._lb_sync_lock = lb_sync_lock
        async with lb_sync_lock:
            authority = await loop.run_in_executor(
                None, self._lb_report_authority,
                request_data.get('lb_session_id'))
            if not authority[0]:
                # The sync token authenticates the shared LB workload, not
                # membership in this service. Do not reveal replica URLs,
                # capacity, or routing policy to another service's Pod.
                return fastapi.Response(status_code=503)
            (replica_infos, async_occupancy_by_version,
             logical_versions) = (await loop.run_in_executor(
                 None, self._snapshot_replica_occupancy))
            # Cold endpoint resolution is proportional to the READY fleet and
            # may take tens of seconds. Keep it off the FastAPI event loop so
            # controller liveness and ownership probes remain responsive.
            lb_replica_info, num_ready = await loop.run_in_executor(
                None, self._get_lb_replica_info, replica_infos,
                async_occupancy_by_version)
            if isinstance(lb_replica_info, dict):
                self._lb_expected_occupancy_urls = {
                    url for url, info in lb_replica_info.items()
                    if str(info.get('async_occupancy', '')).lower() == 'true'
                }
                self._lb_occupancy_contract_known = True
            # History is incarnation-scoped and never changes runtime state,
            # so it may finish before the final ownership fence even if this
            # controller loses the service mid-write.
            observed_slots: dict[int, int] = {}
            if authority[1]:
                observed_slots = self._translate_observed_slots(
                    request_data.get('total_slots_by_url'))
                if logical_versions is not None:
                    await self._confirm_logical_bridge_capacities(
                        replica_infos, logical_versions, observed_slots)
            request_history_accepted = await self._persist_request_history(
                request_data)
            if not await loop.run_in_executor(None, self._owns_current_service):
                return fastapi.Response(status_code=503)
            # All awaits, including durable bridge confirmation, are above
            # this final ownership fence. Runtime mutation and confidential
            # routing disclosure below are one synchronous critical section.
            self._apply_load_balancer_report(
                request_data,
                replica_infos,
                async_occupancy_by_version,
                authority,
                observed_slots,
            )
            replica_counts = self._get_replica_counts(replica_infos)
            self._replica_counts_snapshot = replica_counts
            response_content = {
                'replica_info': lb_replica_info,
                'num_ready_replicas': num_ready,
                'routing_spec': self._get_routing_spec(),
                'capacity_hint': self._get_capacity_hint(
                    replica_infos,
                    logical_versions,
                    replica_counts=replica_counts),
                'request_history_accepted': request_history_accepted,
                # Additive protocol negotiation for mixed-version rollouts.
                # A new LB only relies exclusively on the replaceable queue
                # gauge after a controller positively advertises support.
                'queued_compatibility_demand_supported': True,
            }
            if getattr(self, '_lb_ha_enabled', False):
                response_content['service_version'] = self._applied_version
            return responses.JSONResponse(content=response_content,
                                          status_code=200)

    def _lb_cutover_fence(
        self,) -> tuple[str, tuple[int | None, str | None], int] | None:
        """Return the current incarnation/owner/epoch fence or fail closed."""
        owner = serve_state.get_service_controller_owner(self._service_name,
                                                         include_lb_state=True)
        if (owner is None or not owner.get('lb_ha_enabled') or
                not owner.get('hash') or owner.get('lifecycle_epoch') is None):
            return None
        expected_owner = self._controller_owner
        actual_owner = (owner.get('controller_pid'), owner.get('controller_ip'))
        if expected_owner is None or actual_owner != expected_owner:
            return None
        return (str(owner['hash']), actual_owner, int(owner['lifecycle_epoch']))

    def _lb_promotion_report_is_current(self, request_data: dict[str,
                                                                 Any]) -> bool:
        if not getattr(self, '_lb_occupancy_contract_known', False):
            return False
        routing_version = request_data.get('routing_version')
        if (not isinstance(routing_version, int) or
                isinstance(routing_version, bool) or
                routing_version != self._applied_version):
            return False
        sample_generations = request_data.get('occupancy_sample_generation', {})
        sample_ages = request_data.get('occupancy_sample_age_seconds', {})
        if not isinstance(sample_generations, dict) or not isinstance(
                sample_ages, dict):
            return False
        return lb_ha.occupancy_samples_are_promotable(
            self._lb_expected_occupancy_urls, sample_generations, sample_ages,
            serve_constants.LB_PROMOTION_OCCUPANCY_MAX_AGE_SECONDS)

    def _restore_lb_demand_handoff(self, generation: int) -> None:
        handoff = self._lb_demand_handoff
        if handoff.generation == generation:
            return
        durable_generation, snapshot, complete_at = (
            serve_state.get_lb_demand_handoff(self._service_name))
        handoff.restore(durable_generation, snapshot, complete_at)

    def _publish_ha_drain_view(self,
                               authority: lb_k8s.LbPodAuthority,
                               state: lb_ha.LbCutoverState,
                               legacy_selected: bool = False) -> None:
        """Publish one service-wide ACTIVE+DRAINING drain snapshot."""
        if (legacy_selected and state.phase is lb_ha.LbCutoverPhase.MIGRATING):
            # The selected legacy Pod publishes the only authoritative drain
            # view through the regular sync path. Idle warm slots must not
            # overwrite it before migration.
            return
        ledger = self._lb_session_ledger
        if ledger is None or authority.slot_by_uid is None:
            return
        ledger.discard_dead(authority.live_uids)
        stream_owner_slots = {state.active_slot}
        if (state.phase is lb_ha.LbCutoverPhase.DRAINING and
                state.pending_slot is not None):
            stream_owner_slots.add(state.pending_slot)
        stream_owner_ids = {
            session_id for session_id, slot in authority.slot_by_uid.items()
            if session_id in authority.live_uids and slot in stream_owner_slots
        }
        legacy_stream_owner_ids = ((authority.legacy_uids or set()) &
                                   authority.live_uids)
        if (state.phase is lb_ha.LbCutoverPhase.ROLLING_BACK and
                not legacy_selected):
            # A terminating migration tail can still own streams when an
            # immediate rollback enters ROLLING_BACK but cannot yet create or
            # select the replacement legacy Deployment. A fresh unselected
            # legacy candidate has never received traffic and is excluded.
            legacy_stream_owner_ids &= authority.terminating_uids or set()
        if legacy_stream_owner_ids:
            # A legacy Pod remains a possible stream owner after the migration
            # selector moves and throughout its termination grace. During
            # rollback, it becomes a possible owner as soon as the selector
            # moves back. Legacy processes do not use the role ledger, so
            # including their UID deliberately makes the aggregate incomplete
            # and blocks backend drain decisions until the topology settles.
            stream_owner_ids.update(legacy_stream_owner_ids)
        report = ledger.aggregate(stream_owner_ids)
        if report.complete:
            in_flight = report.in_flight
            routing_urls = report.routing_urls
            unknown_urls = report.unknown_urls
            draining_urls = report.draining_urls
        else:
            # A missing stream-owner report is not evidence of idleness.
            in_flight, routing_urls, unknown_urls, draining_urls = {}, None, [], []
        self._replica_manager.update_lb_in_flight(
            in_flight, routing_urls, unknown_urls, draining_urls,
            f'ha-generation-{state.generation}')

    def _rollback_active_slot_is_drained(self, authority: lb_k8s.LbPodAuthority,
                                         state: lb_ha.LbCutoverState) -> bool:
        """Return whether rollback may retire the formerly active slot."""
        if (state.phase is not lb_ha.LbCutoverPhase.ROLLING_BACK or
                state.active_slot is None or authority.slot_by_uid is None):
            return False
        active_ids = {
            session_id for session_id, slot in authority.slot_by_uid.items()
            if session_id in authority.live_uids and slot is state.active_slot
        }
        if not active_ids:
            return True
        ledger = self._lb_session_ledger
        if ledger is None:
            return False
        ledger.discard_dead(authority.live_uids)
        report = ledger.aggregate(active_ids,
                                  required_applied_role=lb_ha.LbRole.DRAINING,
                                  required_applied_generation=state.generation)
        # Only process-local work belongs to the retiring LB. Replica-global
        # async occupancy continues to be sampled by the selected legacy LB.
        return report.complete and report.local_in_flight == 0

    def _finish_ha_drain_if_safe(
            self, authority: lb_k8s.LbPodAuthority, state: lb_ha.LbCutoverState,
            fence: tuple[str, tuple[int | None, str | None], int]) -> bool:
        if (state.phase is not lb_ha.LbCutoverPhase.DRAINING or
                state.active_slot is None or state.pending_slot is None or
                authority.slot_by_uid is None):
            return False
        draining_ids = {
            session_id for session_id, slot in authority.slot_by_uid.items()
            if session_id in authority.live_uids and slot == state.pending_slot
        }
        clean = not draining_ids
        if draining_ids and self._lb_session_ledger is not None:
            report = self._lb_session_ledger.aggregate(
                draining_ids,
                required_applied_role=lb_ha.LbRole.DRAINING,
                required_applied_generation=state.generation)
            # Backend async occupancy is replica-global and continues to be
            # sampled by the new active. It is not work owned by the former
            # LB process and must not pin DRAINING forever. The process-local
            # admission count includes unqueued and queued dispatches as well
            # as returned streams. Requiring the LB to acknowledge DRAINING
            # also closes the admission window between the selector move and
            # the first role response applied by the former active.
            clean = report.complete and report.local_in_flight == 0
        drain_started_at = state.drain_started_at
        timed_out = (drain_started_at is not None and
                     time.time() - drain_started_at >= getattr(
                         self, '_lb_drain_timeout_seconds',
                         serve_constants.LB_DRAIN_CLOSE_GRACE_SECONDS))
        if timed_out and not clean:
            logger.warning('Finishing HA LB drain after the bounded Pod '
                           f'termination budget for {self._service_name!r}.')
            clean = True
        if not clean:
            return False
        service_hash, owner, lifecycle_epoch = fence
        return serve_state.finish_lb_cutover_drain(
            self._service_name, service_hash, owner, lifecycle_epoch,
            state.active_slot, state.pending_slot, state.generation)

    @staticmethod
    def _lb_ha_rollout_evidence(
            authority: lb_k8s.LbPodAuthority, state: lb_ha.LbCutoverState,
            desired_runtime_revision: str | None) -> dict[str, Any]:
        """Return role-channel evidence that both slots share one revision."""
        revisions = authority.revision_by_uid or {}
        slot_by_uid = authority.slot_by_uid or {}
        slots: dict[str, dict[str, Any]] = {}
        all_revisions: set[str] = set()
        for slot in lb_ha.LbSlot:
            ready_ids = {
                uid for uid in authority.ready_nonterminating_uids
                if slot_by_uid.get(uid) is slot
            }
            slot_revisions = sorted({
                revision for uid in ready_ids
                if (revision := revisions.get(uid)) is not None
            })
            all_revisions.update(slot_revisions)
            slots[slot.value] = {
                'ready': bool(ready_ids),
                'revisions': slot_revisions,
            }
        converged = (desired_runtime_revision is not None and
                     state.phase is lb_ha.LbCutoverPhase.STABLE and
                     all(slot['ready'] and len(slot['revisions']) == 1
                         for slot in slots.values()) and
                     all_revisions == {desired_runtime_revision})
        return {
            'phase': state.phase.value,
            'selected_slot': (state.active_slot.value
                              if state.active_slot is not None else None),
            'generation': state.generation,
            'desired_revision': desired_runtime_revision,
            'slots': slots,
            'slots_converged': converged,
        }

    async def _handle_load_balancer_role(
            self, request_data: dict[str, Any]) -> fastapi.Response:
        """Ingest a fast HA report and advance the recoverable cutover saga."""
        trace = lb_ha_obs.RoleRequestTrace()

        def role_response(
                outcome: lb_ha_obs.LbRoleOutcome,
                status_code: int,
                content: dict[str, Any] | None = None
        ) -> responses.JSONResponse:
            response_content = dict(content or {})
            response_content['outcome'] = outcome.value
            response_content['observability'] = trace.snapshot()
            return responses.JSONResponse(content=response_content,
                                          status_code=status_code)

        if not self._lb_ha_enabled:
            return role_response(
                lb_ha_obs.LbRoleOutcome.LEGACY_MODE, 200, {
                    'role': lb_ha.LbRole.ACTIVE.value,
                    'generation': 0,
                    'selected_slot': None,
                    'promotable': True,
                })
        session_id = request_data.get('lb_session_id')
        slot = lb_ha.parse_slot(request_data.get('lb_slot'))
        if not isinstance(session_id, str) or slot is None:
            return role_response(lb_ha_obs.LbRoleOutcome.INVALID_REPORT, 503)
        loop = asyncio.get_running_loop()
        if not await trace.run_in_executor(loop, 'postgresql_owner_read',
                                           self._owns_current_service):
            return role_response(lb_ha_obs.LbRoleOutcome.CONTROLLER_NOT_OWNER,
                                 503)
        role_lock = self._lb_role_lock
        if role_lock is None:
            role_lock = asyncio.Lock()
            self._lb_role_lock = role_lock
        lock_wait_started_at = time.monotonic()
        async with role_lock:
            trace.lock_acquired(lock_wait_started_at)
            authority = await trace.run_in_executor(loop,
                                                    'kubernetes_pod_authority',
                                                    lb_k8s.get_lb_pod_authority,
                                                    self._service_name)
            if authority is None or authority.slot_by_uid is None:
                return role_response(
                    lb_ha_obs.LbRoleOutcome.POD_AUTHORITY_UNAVAILABLE, 503)
            if (session_id not in authority.live_uids or
                    authority.slot_by_uid.get(session_id) is not slot):
                return role_response(
                    lb_ha_obs.LbRoleOutcome.POD_NOT_AUTHORITATIVE, 503)
            fence = await trace.run_in_executor(loop, 'postgresql_fence_read',
                                                self._lb_cutover_fence)
            state = await trace.run_in_executor(
                loop, 'postgresql_cutover_state_read',
                serve_state.get_lb_cutover_state, self._service_name)
            if (fence is None or state is None or not state.enabled or
                    state.active_slot is None):
                return role_response(
                    lb_ha_obs.LbRoleOutcome.CUTOVER_STATE_UNAVAILABLE, 503)
            promotable = self._lb_promotion_report_is_current(request_data)
            role = state.role_for(slot)
            ledger = self._lb_session_ledger
            if ledger is None or not ledger.update(
                    session_id, slot, role, state.generation, request_data):
                return role_response(lb_ha_obs.LbRoleOutcome.REPORT_REJECTED,
                                     503)
            try:
                routing: (lb_k8s.LbServiceRouting |
                          lb_k8s.LbServiceTransitionRouting)
                if state.phase in (lb_ha.LbCutoverPhase.MIGRATING,
                                   lb_ha.LbCutoverPhase.ROLLING_BACK):
                    routing = await trace.run_in_executor(
                        loop, 'kubernetes_service_routing_read',
                        lb_k8s.get_lb_service_transition_routing,
                        self._service_name)
                else:
                    routing = await trace.run_in_executor(
                        loop, 'kubernetes_service_routing_read',
                        lb_k8s.get_lb_service_routing, self._service_name)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Cannot reconcile HA LB Service routing: '
                               f'{common_utils.format_exception(e)}')
                return role_response(
                    lb_ha_obs.LbRoleOutcome.ROUTING_UNAVAILABLE, 503)

            service_hash, expected_owner, lifecycle_epoch = fence
            transition_legacy_selected = bool(
                state.phase in (lb_ha.LbCutoverPhase.MIGRATING,
                                lb_ha.LbCutoverPhase.ROLLING_BACK) and
                getattr(routing, 'legacy_selected', False))
            ready_by_slot = {
                candidate_slot: {
                    uid
                    for uid in authority.ready_nonterminating_uids
                    if authority.slot_by_uid.get(uid) is candidate_slot
                } for candidate_slot in lb_ha.LbSlot
            }
            if state.phase is lb_ha.LbCutoverPhase.MIGRATING:
                assert state.active_slot is lb_ha.LbSlot.A
                if transition_legacy_selected:
                    if (slot is lb_ha.LbSlot.A and
                            session_id in ready_by_slot[lb_ha.LbSlot.A] and
                            bool(ready_by_slot[lb_ha.LbSlot.B]) and promotable):
                        patched = await trace.run_in_executor(
                            loop, 'kubernetes_selector_patch',
                            lb_k8s.patch_lb_service_migration_to_slot,
                            self._service_name, service_hash, expected_owner,
                            lifecycle_epoch)
                        if patched:
                            # The old routing snapshot still says legacy, but
                            # the successful resourceVersion-fenced patch is
                            # enough to block drain decisions immediately.
                            transition_legacy_selected = False
                elif (routing.active_slot is lb_ha.LbSlot.A and
                      routing.generation == 1):
                    migrated = await trace.run_in_executor(
                        loop, 'postgresql_cutover_write',
                        serve_state.finish_lb_ha_migration, self._service_name,
                        service_hash, expected_owner, lifecycle_epoch)
                    if migrated:
                        state = await trace.run_in_executor(
                            loop, 'postgresql_cutover_state_read',
                            serve_state.get_lb_cutover_state,
                            self._service_name)
                        assert state is not None
                        # Do not wait for foreground Deployment deletion while
                        # holding the role lock. The terminating legacy Pod may
                        # drain for the full grace period; blocking here makes
                        # both HA slots time out their role heartbeats. Stable
                        # parent-process supervision owns idempotent obsolete-
                        # topology cleanup and retries it on every pass.

            rollback_view_published = False
            if state.phase is lb_ha.LbCutoverPhase.ROLLING_BACK:
                rollback_active_slot = state.active_slot
                assert rollback_active_slot is not None
                legacy_ready = bool((authority.legacy_uids or set()) &
                                    authority.ready_nonterminating_uids)
                if (not transition_legacy_selected and legacy_ready and
                        routing.active_slot is rollback_active_slot and
                        routing.generation == state.generation):
                    patched = await trace.run_in_executor(
                        loop, 'kubernetes_selector_patch',
                        lb_k8s.patch_lb_service_rollback_to_legacy,
                        self._service_name, service_hash, expected_owner,
                        lifecycle_epoch, rollback_active_slot, state.generation)
                    if patched:
                        transition_legacy_selected = True
                if transition_legacy_selected:
                    # Publish a blocking drain view before the database leaves
                    # HA mode. The legacy Pod may already be accepting new
                    # streams, while the former active slot may still be
                    # finishing old ones.
                    self._publish_ha_drain_view(authority, state, True)
                    rollback_view_published = True
                    slot_drained = await trace.run_in_executor(
                        loop, 'drain_evidence_read',
                        self._rollback_active_slot_is_drained, authority, state)
                    rolled_back = False
                    if slot_drained:
                        rolled_back = await trace.run_in_executor(
                            loop, 'postgresql_cutover_write',
                            serve_state.finish_lb_ha_rollback,
                            self._service_name, service_hash, expected_owner,
                            lifecycle_epoch, rollback_active_slot,
                            state.generation)
                    if rolled_back:
                        self._lb_ha_enabled = False
                        self._lb_session_ledger = None
                        self._lb_last_demand_snapshot = None
                        # As above, the parent supervisor deletes obsolete HA
                        # slots outside this role lock. Return the committed
                        # role response immediately so remaining slots keep a
                        # fresh controller heartbeat throughout their drain.
                        return role_response(
                            lb_ha_obs.LbRoleOutcome.SUCCESS, 200, {
                                'role': lb_ha.LbRole.DRAINING.value,
                                'generation': state.generation,
                                'selected_slot': None,
                                'promotable': False,
                                'phase':
                                    lb_ha.LbCutoverPhase.ROLLING_BACK.value,
                            })

            if state.phase is lb_ha.LbCutoverPhase.STABLE:
                stable_active_slot = state.active_slot
                assert stable_active_slot is not None
                if (routing.active_slot is not stable_active_slot or
                        routing.generation != state.generation):
                    return role_response(
                        lb_ha_obs.LbRoleOutcome.ROUTING_NOT_CONVERGED, 503)
                target = stable_active_slot.other
                selected_ready = bool(ready_by_slot[stable_active_slot])
                target_ready = session_id in ready_by_slot[target]
                planned_upgrade = False
                if selected_ready and target_ready:
                    revisions = authority.revision_by_uid or {}
                    target_revision = revisions.get(session_id)
                    desired_revision = routing.desired_runtime_revision
                    active_revisions = {
                        revisions.get(uid)
                        for uid in ready_by_slot[stable_active_slot]
                    }
                    planned_upgrade = (desired_revision is not None and
                                       target_revision == desired_revision and
                                       desired_revision not in active_revisions)
                if (slot is target and target_ready and promotable and
                    (not selected_ready or planned_upgrade)):
                    next_state = await trace.run_in_executor(
                        loop, 'postgresql_cutover_write',
                        serve_state.begin_lb_cutover, self._service_name,
                        service_hash, expected_owner, lifecycle_epoch,
                        stable_active_slot, state.generation, target,
                        self._lb_last_demand_snapshot)
                    if next_state is not None:
                        self._lb_demand_handoff.begin(
                            next_state.generation,
                            self._lb_last_demand_snapshot)
                        state = next_state

            if state.phase is lb_ha.LbCutoverPhase.PREPARING:
                assert state.pending_slot is not None
                assert state.active_slot is not None
                target = state.pending_slot
                preparing_active_slot = state.active_slot
                # Crash recovery: the selector moved but the DB commit did not.
                if (routing.active_slot is target and
                        routing.generation == state.generation):
                    committed = await trace.run_in_executor(
                        loop, 'postgresql_cutover_write',
                        serve_state.commit_lb_cutover, self._service_name,
                        service_hash, expected_owner, lifecycle_epoch,
                        preparing_active_slot, target, state.generation)
                    if committed:
                        state = await trace.run_in_executor(
                            loop, 'postgresql_cutover_state_read',
                            serve_state.get_lb_cutover_state,
                            self._service_name)
                        assert state is not None
                elif (routing.active_slot is preparing_active_slot and
                      routing.generation == state.generation - 1):
                    target_ready = session_id in ready_by_slot[target]
                    armed_generation = request_data.get('armed_generation')
                    if (slot is target and target_ready and promotable and
                            armed_generation == state.generation):
                        patched = await trace.run_in_executor(
                            loop, 'kubernetes_selector_patch',
                            lb_k8s.patch_lb_service_active_slot,
                            self._service_name, service_hash, expected_owner,
                            lifecycle_epoch, preparing_active_slot,
                            state.generation - 1, target, state.generation)
                        if patched:
                            committed = await trace.run_in_executor(
                                loop, 'postgresql_cutover_write',
                                serve_state.commit_lb_cutover,
                                self._service_name, service_hash,
                                expected_owner, lifecycle_epoch,
                                preparing_active_slot, target, state.generation)
                            if committed:
                                state = await trace.run_in_executor(
                                    loop, 'postgresql_cutover_state_read',
                                    serve_state.get_lb_cutover_state,
                                    self._service_name)
                                assert state is not None
                    elif not ready_by_slot[target]:
                        advanced = await trace.run_in_executor(
                            loop, 'kubernetes_selector_patch',
                            lb_k8s.patch_lb_service_aborted_generation,
                            self._service_name, service_hash, expected_owner,
                            lifecycle_epoch, preparing_active_slot, target,
                            state.generation)
                        if advanced:
                            aborted = await trace.run_in_executor(
                                loop, 'postgresql_cutover_write',
                                serve_state.abort_lb_cutover_preparation,
                                self._service_name, service_hash,
                                expected_owner, lifecycle_epoch,
                                preparing_active_slot, target, state.generation)
                            if aborted:
                                self._lb_demand_handoff.restore(
                                    None, None, None)
                                state = await trace.run_in_executor(
                                    loop, 'postgresql_cutover_state_read',
                                    serve_state.get_lb_cutover_state,
                                    self._service_name)
                                assert state is not None
                elif (routing.active_slot is preparing_active_slot and
                      routing.generation == state.generation):
                    # Crash recovery after the Service generation was
                    # advanced but before the database abort committed.
                    aborted = await trace.run_in_executor(
                        loop, 'postgresql_cutover_write',
                        serve_state.abort_lb_cutover_preparation,
                        self._service_name, service_hash, expected_owner,
                        lifecycle_epoch, preparing_active_slot, target,
                        state.generation)
                    if aborted:
                        self._lb_demand_handoff.restore(None, None, None)
                        state = await trace.run_in_executor(
                            loop, 'postgresql_cutover_state_read',
                            serve_state.get_lb_cutover_state,
                            self._service_name)
                        assert state is not None
                else:
                    return role_response(
                        lb_ha_obs.LbRoleOutcome.TRANSITION_INCONSISTENT, 503)

            legacy_selected = bool(
                state.phase in (lb_ha.LbCutoverPhase.MIGRATING,
                                lb_ha.LbCutoverPhase.ROLLING_BACK) and
                transition_legacy_selected)
            if not rollback_view_published:
                self._publish_ha_drain_view(authority, state, legacy_selected)
            if state.phase is lb_ha.LbCutoverPhase.DRAINING:
                finished = await trace.run_in_executor(
                    loop, 'drain_evidence_write', self._finish_ha_drain_if_safe,
                    authority, state, fence)
                if finished:
                    state = await trace.run_in_executor(
                        loop, 'postgresql_cutover_state_read',
                        serve_state.get_lb_cutover_state, self._service_name)
                    assert state is not None
            assert state.active_slot is not None
            role = state.role_for(slot)
            if (state.phase is lb_ha.LbCutoverPhase.ROLLING_BACK and
                    legacy_selected and slot is state.active_slot):
                # Once traffic has moved to legacy, stop admission on the
                # former active before waiting for its process-local count.
                role = lb_ha.LbRole.DRAINING
            return role_response(
                lb_ha_obs.LbRoleOutcome.SUCCESS, 200, {
                    'role': role.value,
                    'generation': state.generation,
                    'selected_slot': state.active_slot.value,
                    'promotable': promotable,
                    'phase': state.phase.value,
                    'ha_rollout': self._lb_ha_rollout_evidence(
                        authority, state, routing.desired_runtime_revision),
                })

    async def _handle_load_balancer_request_history_sync(
            self, request_data: dict[str, Any]) -> fastapi.Response:
        """Persist a draining LB's history without reporting demand."""
        loop = asyncio.get_running_loop()
        if not await loop.run_in_executor(None, self._owns_current_service):
            return fastapi.Response(status_code=503)
        authority = await loop.run_in_executor(
            None, self._lb_report_authority, request_data.get('lb_session_id'))
        if not authority[0]:
            return fastapi.Response(status_code=503)
        accepted = await self._persist_request_history(request_data)
        return responses.JSONResponse(
            content={'request_history_accepted': accepted}, status_code=200)

    async def _persist_request_history(self, request_data: dict[str,
                                                                Any]) -> bool:
        """Persist history without allowing observability to fail sync."""
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None,
                                              self._record_request_history,
                                              request_data)
        except ValueError as e:
            # A malformed snapshot cannot become valid by retrying. Drop it
            # with an acknowledgement so a mixed-version or corrupted LB
            # cannot hammer the controller every sync forever.
            logger.warning('Dropping invalid load balancer request history for '
                           f'{self._service_name!r}: '
                           f'{common_utils.format_exception(e)}')
            return True
        except Exception as e:  # pylint: disable=broad-except
            # Request history is observability, not control-plane state.
            # Keep routing and autoscaling available while asking the LB to
            # retry only its bounded cumulative counters.
            logger.warning(
                'Failed to persist load balancer request history for '
                f'{self._service_name!r}: '
                f'{common_utils.format_exception(e)}')
            return False

    def _record_request_history(self, request_data: dict[str, Any]) -> bool:
        """Persist one live LB process's cumulative minute counters."""
        request_history = request_data.get('request_history')
        if request_history is None:
            return True
        service_hash = getattr(self, '_service_hash', None)
        if service_hash is None:
            # Compatibility for direct/legacy controller construction without
            # an incarnation fence. Do not create history that could leak into
            # a later same-name service.
            return True
        lb_session_id = request_data.get('lb_session_id')
        process_session_id = request_data.get('request_history_session_id')
        if (not isinstance(lb_session_id, str) or not lb_session_id or
                not isinstance(process_session_id, str) or
                len(process_session_id) != 32 or
                any(character not in '0123456789abcdef'
                    for character in process_session_id)):
            raise ValueError('Invalid request history reporter session.')
        reporter_session_id = f'{lb_session_id}:{process_session_id}'
        serve_history.record_request_activity(
            self._service_name,
            service_hash,
            reporter_session_id,
            request_history,
        )
        return True

    def _owns_current_service(self) -> bool:
        """Whether this controller parent still owns the exact DB row."""
        service_hash = getattr(self, '_service_hash', None)
        controller_owner = getattr(self, '_controller_owner', None)
        if service_hash is None:
            # Compatibility for direct/legacy controller construction.
            return True
        owner = serve_state.get_service_controller_owner(self._service_name)
        return (owner is not None and owner.get('hash') == service_hash and
                (owner.get('controller_pid'), owner.get('controller_ip'))
                == controller_owner)

    def _snapshot_replica_occupancy(
        self
    ) -> tuple[list['replica_managers.ReplicaInfo'], dict[int, bool | None],
               set[int]]:
        """Read the replica rows and per-version async-occupancy flags.

        Blocking DB reads; callers on the event loop must run this in an
        executor.
        """
        replica_infos = serve_state.get_replica_infos(self._service_name)
        replica_versions = sorted({info.version for info in replica_infos})
        version_specs = serve_state.get_specs(self._service_name,
                                              replica_versions)
        async_occupancy_by_version: dict[int, bool | None] = {
            replica_version: None if version_specs.get(replica_version)
                             is None else getattr(
                                 version_specs[replica_version],
                                 'graceful_drain_async_occupancy',
                                 None) for replica_version in replica_versions
        }
        logical_versions = {
            replica_version for replica_version in replica_versions
            if getattr(version_specs.get(replica_version),
                       'uses_logical_replicas', False) is True
        }
        return replica_infos, async_occupancy_by_version, logical_versions

    def _get_capacity_hint(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        logical_versions: set[int] | None = None,
        replica_counts: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
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
        - max_replicas: the configured autoscaling ceiling. It changes only
          on service updates, so the external load balancer can retain the
          last synced value while the control plane is temporarily down.
        """
        latest_version = self._autoscaler.latest_version
        num_provisioning = 0
        num_latest_nonterminal = 0
        logical = getattr(self._autoscaler, 'replica_unit', None) == 'logical'
        if logical_versions is None:
            logical_versions = {latest_version} if logical else set()
        for info in replica_infos:
            if info.version != latest_version or info.is_terminal:
                continue
            if (logical and getattr(getattr(info, 'status_property', None),
                                    'is_scale_down', False) is True):
                continue
            width = int(getattr(info, 'planned_capacity', 1)) if logical else 1
            num_latest_nonterminal += width
            if not info.is_ready:
                num_provisioning += width
        target = self._autoscaler.get_final_target_num_replicas()
        if not self._autoscaler.has_recomputed_with_fresh_data():
            target = max(target, num_latest_nonterminal)
        hint: dict[str, Any] = {
            'replica_unit': ('logical_slot' if logical else 'physical_backend'),
            'provisioning_replicas': num_provisioning,
            'target_num_replicas': target,
            'max_replicas': self._autoscaler.max_replicas,
            'configured_max_replicas': self._autoscaler.max_replicas,
        }
        if replica_counts is None:
            replica_counts = self._get_replica_counts(replica_infos)
        hint.update({
            key: value
            for key, value in replica_counts.items()
            if key != 'replica_unit'
        })
        min_by_accelerator = getattr(self._autoscaler,
                                     'min_replicas_by_accelerator', {})
        demand_by_accelerator = getattr(self._autoscaler,
                                        'target_num_replicas_by_accelerator',
                                        {})
        if isinstance(min_by_accelerator, dict) and min_by_accelerator:
            hint['min_replicas_by_accelerator'] = dict(min_by_accelerator)
        if isinstance(demand_by_accelerator, dict) and demand_by_accelerator:
            hint['target_num_replicas_by_accelerator'] = dict(
                demand_by_accelerator)
            hint['demand_target_by_accelerator'] = dict(demand_by_accelerator)
        if logical:
            planned_capacity_by_url = {
                cached[0]: int(getattr(info, 'planned_capacity', 1))
                for info in replica_infos
                if (info.version in logical_versions or bool(
                    getattr(info, 'logical_bridge_capacity_verified', False))
                   ) and not info.is_terminal
                for cached in [self._lb_translation_cache.get(info.replica_id)]
                if cached is not None
            }
            hint['planned_capacity_by_url'] = planned_capacity_by_url
            hint['logical_replica_urls'] = sorted(planned_capacity_by_url)
        return hint

    def _get_replica_counts(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> dict[str, Any]:
        """Return logical capacity and physical backend status aggregates."""
        autoscaler = getattr(self, '_autoscaler', None)
        logical = getattr(autoscaler, 'replica_unit', None) == 'logical'

        ready = total = failed = 0
        physical_ready = physical_total = physical_failed = 0
        ready_by_accelerator: dict[str, int] = {}
        provisioning_by_accelerator: dict[str, int] = {}
        total_by_accelerator: dict[str, int] = {}
        zero_cost_ready_by_accelerator: dict[str, int] = {}
        zero_cost_total_by_accelerator: dict[str, int] = {}
        failed_statuses = serve_state.ReplicaStatus.failed_statuses()
        for info in replica_infos:
            status = info.status
            # Pre-activation bridge rows deserialize with planned_capacity=1;
            # every logical version keeps its selected width. This lets a
            # rolling activation count both generations without spec queries.
            planned_capacity = getattr(info, 'planned_capacity', 1)
            width = (planned_capacity
                     if logical and isinstance(planned_capacity, int) and
                     not isinstance(planned_capacity, bool) and
                     planned_capacity > 0 else 1)
            cached = self._lb_translation_cache.get(info.replica_id)
            accelerator = cached[1] if cached is not None else 'unknown'
            if accelerator == 'unknown':
                resources = getattr(getattr(info, 'handle', None),
                                    'launched_resources', None)
                accelerators = getattr(resources, 'accelerators', None)
                if not accelerators:
                    accelerators = (getattr(info, 'resources_override', None) or
                                    {}).get('accelerators')
                if isinstance(accelerators, dict) and accelerators:
                    accelerator = next(iter(accelerators))
            known_accelerator = accelerator != 'unknown'
            if status == serve_state.ReplicaStatus.READY:
                capacity_getter = getattr(autoscaler,
                                          'get_ready_replica_capacity', None)
                observed_ready = (capacity_getter(info) if logical and
                                  callable(capacity_getter) else width)
                ready += max(0, int(observed_ready))
                physical_ready += 1
                if known_accelerator:
                    ready_by_accelerator[accelerator] = (
                        ready_by_accelerator.get(accelerator, 0) + width)
                if (known_accelerator and
                        bool(getattr(info, 'is_zero_cost', False))):
                    zero_cost_ready_by_accelerator[accelerator] = (
                        zero_cost_ready_by_accelerator.get(accelerator, 0) +
                        width)
            if status in failed_statuses:
                failed += width
                physical_failed += 1
            else:
                total += width
                physical_total += 1
                if known_accelerator:
                    total_by_accelerator[accelerator] = (
                        total_by_accelerator.get(accelerator, 0) + width)
                if (known_accelerator and
                        bool(getattr(info, 'is_zero_cost', False))):
                    zero_cost_total_by_accelerator[accelerator] = (
                        zero_cost_total_by_accelerator.get(accelerator, 0) +
                        width)
                if (known_accelerator and
                        status != serve_state.ReplicaStatus.READY):
                    provisioning_by_accelerator[accelerator] = (
                        provisioning_by_accelerator.get(accelerator, 0) + width)

        counts: dict[str, Any] = {
            'ready_replicas': ready,
            'total_replicas': total,
            'failed_replicas': failed,
            'physical_ready_replicas': physical_ready,
            'physical_total_replicas': physical_total,
            'physical_failed_replicas': physical_failed,
        }
        for key, value in {
                'ready_replicas_by_accelerator': ready_by_accelerator,
                'provisioning_replicas_by_accelerator': provisioning_by_accelerator,
                'total_replicas_by_accelerator': total_by_accelerator,
                'zero_cost_ready_replicas_by_accelerator': zero_cost_ready_by_accelerator,
                'zero_cost_total_replicas_by_accelerator': zero_cost_total_by_accelerator,
        }.items():
            if value:
                counts[key] = value
        free_reserved = self._get_free_reserved_slots_by_accelerator()
        if free_reserved:
            counts['free_reserved_slots_by_accelerator'] = free_reserved
        fill_target = self._get_fill_target_by_accelerator(
            zero_cost_total_by_accelerator, free_reserved)
        if fill_target:
            counts['fill_target_by_accelerator'] = fill_target
        demand_target = getattr(autoscaler,
                                'target_num_replicas_by_accelerator', {})
        if isinstance(demand_target, dict) and demand_target:
            counts['demand_target_by_accelerator'] = dict(demand_target)
        counts['replica_unit'] = ('logical_slot'
                                  if logical else 'physical_backend')
        return counts

    def _get_free_reserved_slots_by_accelerator(self) -> dict[str, int]:
        """Return fresh cached physical zero-cost supply by exact card."""
        placer = getattr(getattr(self, '_replica_manager', None), 'spot_placer',
                         None)
        getter = getattr(placer, 'zero_cost_locations', None)
        if not callable(getter):
            return {}
        locations = getter()
        if not isinstance(locations, list) or not locations:
            return {}
        shapes = reserved_capacity.zero_cost_pool_shapes(locations)
        observations = reserved_capacity.get_cached_free_gpus_by_pool(locations)
        canonical_by_name = {
            str(card).casefold(): str(card) for location in locations
            for card in (location.accelerators or {})
        }
        free_by_accelerator: dict[str, int] = {}
        for (context, normalized_card), per_replica in shapes.items():
            observation = observations.get((context, normalized_card))
            if observation is None or observation.free_gpus is None:
                continue
            card = canonical_by_name.get(normalized_card, normalized_card)
            free_by_accelerator[card] = (
                free_by_accelerator.get(card, 0) +
                max(0, observation.free_gpus) // max(1, per_replica))
        return free_by_accelerator

    def _get_fill_target_by_accelerator(
        self,
        zero_cost_total: dict[str, int],
        free_reserved: dict[str, int],
    ) -> dict[str, int]:
        """Project the aggregate fill overlay onto exact observed cards."""
        if getattr(self._autoscaler, 'reserved_capacity_fill',
                   False) is not True:
            return {}
        raw_aggregate_target = getattr(self._autoscaler, '_fill_target', 0)
        aggregate_target = (max(0, raw_aggregate_target)
                            if isinstance(raw_aggregate_target, int) and
                            not isinstance(raw_aggregate_target, bool) else 0)
        card_order: list[str] = []
        seen: set[str] = set()
        demand_target = getattr(self._autoscaler,
                                'target_num_replicas_by_accelerator', {})
        if not isinstance(demand_target, dict):
            demand_target = {}
        for mapping in (demand_target, zero_cost_total, free_reserved):
            for card in mapping:
                if card.casefold() in seen:
                    continue
                seen.add(card.casefold())
                card_order.append(card)
        result: dict[str, int] = {}
        remaining = aggregate_target
        # Existing reserved replicas are the stable allocation. New fill is
        # then projected only onto exact cards with fresh physical supply.
        for source in (zero_cost_total, free_reserved):
            for card in card_order:
                if remaining <= 0:
                    break
                allocated = min(remaining, max(0, int(source.get(card, 0))))
                if allocated <= 0:
                    continue
                result[card] = result.get(card, 0) + allocated
                remaining -= allocated
        if remaining > 0 and card_order:
            # A broker grant may remain visible for one poll after the exact
            # free observation becomes stale. Preserve aggregate reconciliation
            # without inventing a family match by assigning only to the first
            # exact configured card.
            result[card_order[0]] = result.get(card_order[0], 0) + remaining
        return result

    def _configured_accelerators(self, service_spec: Any) -> list[str]:
        """Return configured exact accelerator IDs in service resource order."""
        yaml_content = getattr(getattr(self, '_replica_manager', None),
                               'yaml_content', None)
        if not isinstance(yaml_content, str):
            # Direct controller unit tests replace ReplicaManager with a loose
            # mock. A real manager always owns the committed YAML string.
            return []
        task = replica_managers.load_task_with_service_spec(
            yaml_content, service_spec)
        configured: list[str] = []
        seen: set[str] = set()
        counts_by_accelerator: dict[str, set[int]] = {}
        for resources in task.resources:
            for accelerator, raw_count in (resources.accelerators or
                                           {}).items():
                normalized = accelerator.casefold()
                try:
                    count = int(raw_count)
                except (TypeError, ValueError):
                    count = 0
                counts_by_accelerator.setdefault(normalized, set()).add(count)
                if normalized in seen:
                    continue
                seen.add(normalized)
                configured.append(accelerator)
        floors = getattr(service_spec, 'min_replicas_by_accelerator', {})
        configured_by_name = {name.casefold(): name for name in configured}
        unknown_floors = [
            name for name in floors if name.casefold() not in configured_by_name
        ]
        if unknown_floors:
            raise ValueError(
                'min_replicas_by_accelerator contains accelerators not '
                f'configured by the service resources: {unknown_floors}.')
        ambiguous = {
            name: sorted(counts)
            for name, counts in counts_by_accelerator.items()
            if len(counts) > 1 or not counts or min(counts) < 1
        }
        # A larger legacy any_of service remains valid but cannot encode its
        # default-all set in the bounded version-1 header. Withhold the
        # capability instead of breaking that existing service. Floors still
        # require an unambiguous shape for every exact card they target.
        if len(configured) > serve_constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS:
            ambiguous_floors = {
                name: counts
                for name, counts in ambiguous.items()
                if name in {floor.casefold() for floor in floors}
            }
            if ambiguous_floors:
                raise ValueError(
                    'SkyServe per-card floors require one positive GPU count '
                    'shape per accelerator; found ambiguous floor shapes '
                    f'{ambiguous_floors}.')
            return []
        if ambiguous:
            raise ValueError(
                'SkyServe exact-card compatibility requires one positive GPU '
                'count shape per accelerator; found ambiguous shapes '
                f'{ambiguous}.')
        return configured

    def _configured_accelerator_shapes(self,
                                       service_spec: Any) -> dict[str, int]:
        """Return canonical exact-card GPU counts from active task resources."""
        configured = self._configured_accelerators(service_spec)
        if not configured:
            return {}
        yaml_content = getattr(getattr(self, '_replica_manager', None),
                               'yaml_content', None)
        if not isinstance(yaml_content, str):
            return {}
        task = replica_managers.load_task_with_service_spec(
            yaml_content, service_spec)
        configured_by_name = {card.casefold(): card for card in configured}
        shapes: dict[str, int] = {}
        for resources in task.resources:
            for accelerator, raw_count in (resources.accelerators or
                                           {}).items():
                card = configured_by_name.get(accelerator.casefold())
                if card is not None:
                    shapes[card] = int(raw_count)
        return shapes

    def _configure_instance_aware_accelerators(self, service_spec: Any) -> None:
        """Feed task-authoritative exact shapes to the compatible autoscaler."""
        if isinstance(self._autoscaler,
                      autoscalers.InstanceAwareRequestRateAutoscaler):
            self._autoscaler.set_configured_accelerator_shapes(
                self._configured_accelerator_shapes(service_spec))

    def _build_routing_spec(self, service_spec: Any) -> dict[str, Any] | None:
        """Build the immutable routing config shipped on LB syncs."""
        if service_spec is None:
            return None
        target_qps = service_spec.target_qps_per_replica
        retriable_status_codes = service_spec.lb_retriable_status_codes
        configured_accelerators = (
            self._configured_accelerators(service_spec) if isinstance(
                getattr(self, '_autoscaler', None),
                autoscalers.InstanceAwareRequestRateAutoscaler) else [])
        routing_spec = {
            # `load_balancing_policy` resolves None to the default policy
            # name, so the LB always receives a concrete policy to build.
            'load_balancing_policy_name': service_spec.load_balancing_policy,
            'target_qps_per_replica':
                (dict(target_qps) if target_qps is not None else None),
            # Lets an instance-aware LB weight replicas per-GPU when the
            # service sizes on concurrency (no QPS dict to weight by) --
            # and clear stale QPS weights after an update switches modes.
            'target_concurrency_per_replica': getattr(
                service_spec, 'target_concurrency_per_replica', None),
            'stream_timeout_seconds': service_spec.lb_stream_timeout_seconds,
            'retriable_status_codes':
                (list(retriable_status_codes)
                 if retriable_status_codes is not None else None),
            'max_retries': service_spec.lb_max_retries,
            'retry_initial_backoff_seconds':
                (service_spec.lb_retry_initial_backoff_seconds),
            'request_queue': getattr(service_spec, 'lb_request_queue', None),
        }
        if configured_accelerators:
            routing_spec.update({
                'request_accelerator_compatibility_version':
                    serve_constants.LB_REQUEST_ACCELERATORS_VERSION,
                'configured_accelerators': configured_accelerators,
            })
        return routing_spec

    def _get_routing_spec(self) -> dict[str, Any] | None:
        """Return the routing spec for the load_balancer_sync response.

        [boltz fork] The external load balancer fetches its routing
        configuration -- load-balancing policy, per-replica target QPS, and
        stream timeout -- over the sync channel instead of static launch
        args, so a `sky serve update` that only changes these fields reaches
        a running LB without re-rolling it. The source of truth is the
        controller's in-memory runtime state, not a per-sync DB reread: the
        update handler commits the new version row before transitioning the
        live autoscaler/replica-manager, so reading the DB here can expose a
        newer routing spec than the runtime has actually applied. Keeping an
        immutable in-memory snapshot removes that mismatch window and avoids
        steady-state DB reads on the hottest serve control-plane path.
        """
        return getattr(self, '_routing_spec', None)

    def _load_service_for_update(self, version: int, yaml_content: str) -> Any:
        """Parse a new update or reuse an exact legacy committed spec.

        An old dynamic_fallback_per_gpu YAML can be invalid under today's
        automatically activated logical contract. A lost-response retry must
        therefore be recognized before parsing. The transactional commit still
        rechecks immutability and lifecycle ownership.
        """
        if serve_state.get_yaml_content(self._service_name,
                                        version) == yaml_content:
            persisted = serve_state.get_spec(self._service_name, version)
            if persisted is None:
                raise RuntimeError(
                    f'Service version {version} has committed YAML but its '
                    'authoritative specification is missing.')
            return persisted
        return serve.SkyServiceSpec.from_yaml_str(yaml_content)

    def _transition_load_balancer_mode(
            self,
            enable_ha: bool,
            target_spec: serve.SkyServiceSpec,
            expected_service_hash: str | None = None,
            expected_lifecycle_epoch: int | None = None) -> None:
        """Run an explicit stable-Service-preserving HA migration/rollback."""
        state = serve_state.get_lb_cutover_state(self._service_name)
        if state is None:
            raise RuntimeError('Service LB cutover state is missing.')
        owner_record = serve_state.get_service_controller_owner(
            self._service_name, include_lb_state=True)
        actual_owner = ((owner_record.get('controller_pid'),
                         owner_record.get('controller_ip'))
                        if owner_record is not None else None)
        if (owner_record is None or not owner_record.get('hash') or
                owner_record.get('lifecycle_epoch') is None or
                actual_owner != self._controller_owner):
            raise RuntimeError('Service ownership changed before LB migration.')
        if (expected_service_hash is not None and
                owner_record['hash'] != expected_service_hash):
            raise RuntimeError(
                'Service incarnation changed before LB migration.')
        if (expected_lifecycle_epoch is not None and
                owner_record.get('lifecycle_epoch')
                != expected_lifecycle_epoch):
            raise RuntimeError('Service lifecycle changed before LB migration.')
        service_hash = str(owner_record['hash'])
        assert actual_owner is not None
        owner = actual_owner
        lifecycle_epoch = int(owner_record['lifecycle_epoch'])
        resuming = False
        if enable_ha and state.enabled:
            if state.phase is lb_ha.LbCutoverPhase.STABLE:
                self._lb_ha_enabled = True
                return
            if state.phase is not lb_ha.LbCutoverPhase.MIGRATING:
                raise RuntimeError(
                    'Load balancer mode cannot change while another cutover '
                    f'is {state.phase.value}.')
            resuming = True
        elif not enable_ha and not state.enabled:
            if state.phase is not lb_ha.LbCutoverPhase.STABLE:
                raise RuntimeError(
                    'Disabled load balancer HA has an invalid non-stable '
                    f'cutover phase {state.phase.value}.')
            self._lb_ha_enabled = False
            return
        elif not enable_ha and state.phase is lb_ha.LbCutoverPhase.ROLLING_BACK:
            resuming = True
        elif state.phase is not lb_ha.LbCutoverPhase.STABLE:
            raise RuntimeError('Load balancer mode cannot change while another '
                               f'cutover is {state.phase.value}.')
        if enable_ha:
            lb_k8s.require_lb_ha_runtime()
        if enable_ha and not resuming:
            started = serve_state.begin_lb_ha_migration(self._service_name,
                                                        service_hash, owner,
                                                        lifecycle_epoch)
            if started:
                self._lb_session_ledger = lb_ha.LbSessionLedger(
                    serve_constants.LB_ROLE_REPORT_MAX_AGE_SECONDS,
                    serve_constants.LB_PROMOTION_OCCUPANCY_MAX_AGE_SECONDS)
                self._lb_occupancy_contract_known = False
                self._lb_last_demand_snapshot = None
        elif not enable_ha and not resuming:
            if state.active_slot is None:
                raise RuntimeError('HA rollback has no committed active slot.')
            started = serve_state.begin_lb_ha_rollback(self._service_name,
                                                       service_hash, owner,
                                                       lifecycle_epoch,
                                                       state.active_slot,
                                                       state.generation)
        else:
            started = True
        if not started:
            raise RuntimeError('LB mode transition lost its durable CAS fence.')
        if enable_ha:
            self._lb_ha_enabled = True
            if self._lb_session_ledger is None:
                self._lb_session_ledger = lb_ha.LbSessionLedger(
                    serve_constants.LB_ROLE_REPORT_MAX_AGE_SECONDS,
                    serve_constants.LB_PROMOTION_OCCUPANCY_MAX_AGE_SECONDS)
                self._lb_occupancy_contract_known = False
                self._lb_last_demand_snapshot = None
        termination_grace = lb_k8s.lb_termination_grace_period_seconds(
            target_spec.lb_stream_timeout_seconds,
            target_spec.graceful_drain_seconds)
        lb_k8s.prepare_lb_mode_transition(
            self._service_name,
            termination_grace,
            service_hash,
            enable_ha,
            continue_guard=self._owns_current_service,
            resource_scope=self._resource_scope)
        deadline = (time.monotonic() +
                    serve_constants.LB_DEPLOYMENT_READY_TIMEOUT_SECONDS)
        while time.monotonic() < deadline:
            current = serve_state.get_lb_cutover_state(self._service_name)
            if (current is not None and current.enabled == enable_ha and
                    current.phase is lb_ha.LbCutoverPhase.STABLE):
                self._lb_ha_enabled = enable_ha
                self._lb_drain_timeout_seconds = termination_grace
                if not enable_ha:
                    self._lb_session_ledger = None
                    self._lb_last_demand_snapshot = None
                return
            if not self._owns_current_service():
                raise RuntimeError('Service ownership changed during LB mode '
                                   'transition.')
            time.sleep(1)
        raise RuntimeError('Timed out waiting for the stable load balancer '
                           'selector mode transition to commit.')

    def _set_load_balancer_high_availability(
            self, enabled: bool, expected_service_hash: str,
            expected_lifecycle_epoch: int) -> None:
        """Apply one fenced LB-only topology update."""
        target_spec = serve_state.get_spec(self._service_name,
                                           self._committed_version)
        if target_spec is None:
            raise RuntimeError('Current service spec is missing.')
        self._transition_load_balancer_mode(
            enabled,
            target_spec,
            expected_service_hash=expected_service_hash,
            expected_lifecycle_epoch=expected_lifecycle_epoch)

    def _commit_service_update(
            self,
            version: int,
            service: Any,
            yaml_content: str,
            update_mode: serve_utils.UpdateMode,
            requested_service_hash: str | None,
            lifecycle_epoch: int | None,
            submitted_yaml_content: str | None = None) -> fastapi.Response:
        """Durably accept one immutable version and schedule its apply."""
        authoritative_retry_service = None
        persisted_yaml = serve_state.get_yaml_content(self._service_name,
                                                      version)
        if persisted_yaml == yaml_content:
            authoritative_retry_service = serve_state.get_spec(
                self._service_name, version)
        validation_service = authoritative_retry_service or service
        if (authoritative_retry_service is None and
                isinstance(validation_service, serve.SkyServiceSpec) and
                not validation_service.lb_high_availability_specified):
            # Default-on applies when a service is created. An existing
            # service whose YAML predates this field inherits its durable mode
            # until a dedicated selector migration is requested explicitly.
            service = service.copy(lb_high_availability=self._lb_ha_enabled)
            validation_service = service
        if (authoritative_retry_service is None and
                isinstance(validation_service, serve.SkyServiceSpec) and
                validation_service.lb_high_availability_specified and
                validation_service.lb_high_availability != self._lb_ha_enabled):
            self._transition_load_balancer_mode(
                validation_service.lb_high_availability,
                validation_service,
                expected_service_hash=requested_service_hash,
                expected_lifecycle_epoch=lifecycle_epoch)
        current_autoscaler = getattr(self, '_autoscaler', None)
        if (authoritative_retry_service is None and getattr(
                current_autoscaler, 'replica_unit', None) == 'logical' and
                getattr(validation_service, 'replica_unit',
                        'physical_backend') != 'logical'):
            return responses.JSONResponse(content={
                'message': 'An existing dynamic_fallback_per_gpu service '
                           'cannot switch in place to physical-backend replica '
                           'semantics. Create a new service for that migration.'
            },
                                          status_code=400)
        if (getattr(validation_service, 'replica_unit',
                    'physical_backend') == 'logical' and
                update_mode == serve_utils.UpdateMode.BLUE_GREEN):
            return responses.JSONResponse(content={
                'message': 'dynamic_fallback_per_gpu services currently '
                           'require rolling updates. Blue-green activation is '
                           'based on physical backend counts and cannot '
                           'preserve the per-GPU capacity target.'
            },
                                          status_code=400)
        if (authoritative_retry_service is None and getattr(
                validation_service, 'uses_logical_replicas', False) is True):
            try:
                update_task = task_lib.Task.from_yaml_str(yaml_content)
                if update_task.num_nodes != 1:
                    raise ValueError(
                        'dynamic_fallback_per_gpu currently supports only '
                        'single-node services. Multi-node replica routing '
                        'does not yet define a safe logical capacity contract.')
            except (ValueError, RuntimeError) as e:
                return responses.JSONResponse(content={'message': str(e)},
                                              status_code=400)
        result = serve_state.add_or_update_version(
            self._service_name,
            version,
            service,
            yaml_content,
            submitted_yaml_content=submitted_yaml_content,
            expected_service_hash=(requested_service_hash or
                                   self._service_hash),
            expected_lifecycle_epoch=lifecycle_epoch,
            expected_controller_owner=self._controller_owner)
        if result is serve_state.VersionCommitResult.REJECTED:
            return responses.JSONResponse(content={
                'message': 'Service lifecycle ownership changed or entered '
                           'terminal status before the update was committed.'
            },
                                          status_code=409)
        if result is serve_state.VersionCommitResult.SEMANTIC_CONFLICT:
            return responses.JSONResponse(content={
                'message': 'An existing dynamic_fallback_per_gpu service '
                           'cannot switch in place to physical-backend replica '
                           'semantics. Create a new service for that migration.'
            },
                                          status_code=400)
        if result is serve_state.VersionCommitResult.LB_HA_CONFLICT:
            return responses.JSONResponse(content={
                'message':
                    'load_balancer.high_availability changed concurrently '
                    'with this update. Re-read service status and retry the '
                    'explicit migration or rollback.'
            },
                                          status_code=400)
        if result is serve_state.VersionCommitResult.CONTENT_CONFLICT:
            return responses.JSONResponse(content={
                'message': f'Service version {version} was already committed '
                           'with different content. Re-run the update to '
                           'allocate a new version.'
            },
                                          status_code=409)
        if result is serve_state.VersionCommitResult.STALE_VERSION:
            return responses.JSONResponse(content={
                'message': f'Service version {version} was superseded by a '
                           'newer committed version before it could commit. '
                           'Re-run the update to allocate a new version.'
            },
                                          status_code=409)
        if result is serve_state.VersionCommitResult.IDEMPOTENT_RETRY:
            # The caller reconstructed this YAML with today's hidden defaults,
            # which may differ from an older stored pickled spec (notably a
            # pre-activation dynamic_fallback_per_gpu version). Apply only the
            # immutable authoritative bytes already committed for this version.
            persisted_service = (authoritative_retry_service or
                                 serve_state.get_spec(self._service_name,
                                                      version))
            if persisted_service is None:
                return responses.JSONResponse(content={
                    'message': f'Service version {version} was committed but '
                               'its authoritative specification is missing.'
                },
                                              status_code=409)
            service = persisted_service

        logger.info(f'Committed update to version {version}: {service}')
        self._record_committed_update(version, service, update_mode)
        content = {'message': 'Success'}
        content.update(self._get_update_status())
        return responses.JSONResponse(content=content, status_code=200)

    def _record_committed_update(self, version: int, service: Any,
                                 update_mode: serve_utils.UpdateMode) -> None:
        """Wake the reconciler after the update's durable commit."""
        update = _PendingServiceUpdate(version, service, update_mode,
                                       time.time())
        scheduled = False
        with self._update_condition:
            self._committed_version = max(self._committed_version, version)
            if version > self._applied_version:
                pending = self._pending_update
                if pending is None or version > pending.version:
                    # Coalesce versions that commit before the worker starts.
                    # This matches controller recovery, which also boots only
                    # the newest committed version.
                    self._pending_update = update
                    scheduled = True
                    self._update_apply_error = None
                    self._update_apply_failures = 0
            self._update_condition.notify()
        if scheduled:
            # Publish this before the reconciler waits on the manager lock.
            # Large stale scale-up batches use the signal to yield to the newer
            # version.
            self._replica_manager.notify_version_pending(version)

    def _get_update_status(self) -> dict[str, Any]:
        """Return committed-versus-applied update visibility."""
        with self._update_condition:
            pending = self._pending_update
            apply_lag = (None if pending is None else max(
                0, int(time.time() - pending.committed_at)))
            return {
                'committed_version': self._committed_version,
                'applied_version': self._applied_version,
                'update_apply_pending': pending is not None,
                'update_apply_lag_seconds': apply_lag,
                'update_apply_error': self._update_apply_error,
                'update_apply_failures': self._update_apply_failures,
            }

    def _update_still_authorized(self) -> bool:
        """Whether this controller still owns a nonterminal service."""
        owner = serve_state.get_service_controller_owner(self._service_name)
        if owner is None or owner['status'] in (
                serve_state.ServiceStatus.terminal_statuses()):
            return False
        if (self._service_hash is not None and
                owner['hash'] != self._service_hash):
            return False
        if self._controller_owner is not None:
            current_owner = (owner['controller_pid'], owner['controller_ip'])
            if current_owner != self._controller_owner:
                return False
        return True

    def _drop_pending_update(self, update: _PendingServiceUpdate) -> None:
        with self._update_condition:
            if self._pending_update is update:
                self._pending_update = None
        self._replica_manager.clear_pending_version(update.version)

    def _reconcile_pending_update_once(self, wait: bool = False) -> bool:
        """Apply one pending update; optionally wait through retry backoff."""
        with self._update_condition:
            while (self._pending_update is None or
                   self._pending_update.version <= self._applied_version):
                if not wait:
                    return True
                self._update_condition.wait()
            update = self._pending_update

        try:
            if not self._update_still_authorized():
                logger.info(
                    f'Dropping committed service version {update.version}: '
                    'the controller no longer owns a live service.')
                self._drop_pending_update(update)
                return True
            self._apply_service_update(update.version, update.service,
                                       update.update_mode)
        except Exception as e:  # pylint: disable=broad-except
            exception_str = common_utils.format_exception(e)
            with self._update_condition:
                retry_same_update = self._pending_update is update
                if retry_same_update:
                    self._update_apply_error = exception_str
                    self._update_apply_failures += 1
            # _apply_service_update clears the pending-version signal in a
            # finally block. Re-publish it while this durable version waits for
            # a retry, unless a newer commit has already replaced the signal.
            self._replica_manager.notify_version_pending(update.version)
            retry_message = ('will retry'
                             if retry_same_update else 'was superseded')
            logger.error(f'Failed to apply committed service version '
                         f'{update.version}; {retry_message}: {exception_str}')
            with ux_utils.enable_traceback():
                logger.error(f'  Traceback: {traceback.format_exc()}')
            if retry_same_update and wait:
                # Release the condition during backoff and wake immediately if
                # a newer commit supersedes this failed update. wait_for()
                # also closes the commit-before-wait lost-wakeup window.
                with self._update_condition:
                    self._update_condition.wait_for(
                        lambda: self._pending_update is not update,
                        timeout=_UPDATE_RETRY_BACKOFF_SECONDS)
            return not retry_same_update

        with self._update_condition:
            self._applied_version = max(self._applied_version, update.version)
            if self._pending_update is update:
                self._pending_update = None
            self._update_apply_error = None
            self._update_apply_failures = 0
        logger.info(f'Applied committed service version {update.version} '
                    f'after {time.time() - update.committed_at:.1f}s.')
        return True

    def _run_update_reconciler(self) -> None:
        """Continuously converge runtime state to the newest committed spec."""
        while True:
            self._reconcile_pending_update_once(wait=True)

    def _apply_service_update(self, version: int, service: Any,
                              update_mode: serve_utils.UpdateMode) -> None:
        """Apply a persisted update to the live controller state."""
        if (getattr(self._autoscaler, 'replica_unit', None) == 'logical' and
                getattr(service, 'uses_logical_replicas', False) is not True):
            raise ValueError(
                'Refusing to apply a physical-backend version after logical '
                'replica semantics were activated.')
        # add_or_update_version commits before this method runs.  Announce the
        # new version without acquiring the replica-manager lock: a large
        # placer-backed scale-up batch may currently hold that lock while
        # enqueueing hundreds of replicas from the superseded version.  The
        # signal lets that batch yield promptly so update_version can acquire
        # the lock and make the durable version live.
        self._replica_manager.notify_version_pending(version)
        try:
            self._replica_manager.update_version(version,
                                                 service,
                                                 update_mode=update_mode)
        finally:
            self._replica_manager.clear_pending_version(version)
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
        self._configure_instance_aware_accelerators(service)
        self._reserved_capacity_fill_enabled = bool(
            getattr(service, 'reserved_capacity_fill', False))
        if self._reserved_capacity_fill_enabled:
            # An update can enable fill on a live service: give
            # the (retained or replaced) autoscaler the location
            # set so suppression works immediately (no-op when
            # already populated), and make sure the poller
            # exists -- without it fill would sit half-active
            # (flag on, no free-slot feed) until a respawn.
            self._seed_fill_zero_cost_locations(self._autoscaler)
            self._start_reserved_capacity_poller_if_needed()
        # Publish the new routing spec only after the live runtime has
        # transitioned, so load_balancer_sync never advertises settings ahead
        # of the controller's own autoscaler / replica-manager state.
        self._routing_spec = self._build_routing_spec(service)

    def _start_reserved_capacity_poller_if_needed(self) -> None:
        """Start the reserved-capacity poller (idempotent).

        Called from run() (boot-enabled flag) and from update_service
        (flag enabled on a live service). Getters, not the live objects:
        update_service can replace self._autoscaler and the poller must
        feed the current one. The zero-cost location seeding that
        protects the pre-first-poll window is handled separately by
        _seed_fill_zero_cost_locations (at construction and on update).
        """
        placer = self._replica_manager.spot_placer
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
                lambda: self._autoscaler, lambda: self._replica_manager.
                spot_placer, self._service_name, self._service_hash, self.
                _controller_owner), 'reserved-capacity-poller')

    def _run_autoscaler(self):
        logger.info('Starting autoscaler.')
        while True:
            try:
                replica_infos = serve_state.get_replica_infos(
                    self._service_name)
                self._replica_counts_snapshot = self._get_replica_counts(
                    replica_infos)
                # Use the active versions set by replica manager to make
                # sure we only scale down the outdated replicas that are
                # not used by the load balancer.
                runtime_snapshot = serve_state.get_service_runtime_snapshot(
                    self._service_name, require_version=True)
                assert runtime_snapshot is not None, (
                    'No service record found for '
                    f'{self._service_name}')
                active_versions = runtime_snapshot['active_versions']
                # Keep the exact autoscaler instance/version that produced
                # this tick. A concurrent update may replace or mutate
                # `self._autoscaler` before actuation; the manager's expected
                # version fence must carry the producer's version, not the
                # newly published one.
                decision_autoscaler = self._autoscaler
                decision_version = decision_autoscaler.latest_version
                decision_autoscaler.set_spot_placer(
                    self._replica_manager.spot_placer)
                if isinstance(decision_autoscaler,
                              autoscalers.InstanceAwareRequestRateAutoscaler):
                    decision_autoscaler.set_free_reserved_slots_by_accelerator(
                        self._get_free_reserved_slots_by_accelerator())

                # Autoscaler now extracts GPU type info directly from
                # replica_infos in generate_scaling_decisions method
                # for better decoupling.
                scaling_options = decision_autoscaler.generate_scaling_decisions(
                    replica_infos, active_versions)
                if (isinstance(decision_autoscaler,
                               autoscalers.ConcurrencyAutoscaler) and
                        decision_autoscaler.replica_unit == 'logical'):
                    target_state = decision_autoscaler.logical_target_state
                    if target_state is not None:
                        self._replica_manager.publish_logical_target(
                            *target_state)
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
                pending_scale_up: list[dict[str, Any] | None] = []

                # The closure is only called within the same outer-loop
                # iteration that (re)binds pending_scale_up, so capturing the
                # loop-scoped list is intentional (B023 false positive).
                def _flush_scale_up(
                        expected_version: int = decision_version) -> None:
                    if pending_scale_up:  # noqa: B023
                        self._replica_manager.scale_up_batch(
                            list(pending_scale_up),  # noqa: B023
                            expected_version=expected_version)
                        pending_scale_up.clear()  # noqa: B023

                for scaling_option in scaling_options:
                    logger.info(f'Scaling option received: {scaling_option}')
                    if (scaling_option.operator ==
                            autoscalers.AutoscalerDecisionOperator.SCALE_UP):
                        if isinstance(scaling_option.target,
                                      autoscalers.LogicalScaleTarget):
                            _flush_scale_up()
                            logical_target = scaling_option.target
                            replacement_kwargs: dict[str, Any] = {}
                            if logical_target.replace_unknown_replica_ids:
                                replacement_kwargs[
                                    'replace_unknown_replica_ids'] = (
                                        logical_target.
                                        replace_unknown_replica_ids)
                            self._replica_manager.scale_up_to_logical_capacity(
                                logical_target.target_capacity,
                                logical_target.version,
                                logical_target.reconcile_generation,
                                **replacement_kwargs)
                        else:
                            assert (scaling_option.target is None or isinstance(
                                scaling_option.target, dict)), scaling_option
                            pending_scale_up.append(scaling_option.target)
                    else:
                        _flush_scale_up()
                        if isinstance(scaling_option.target,
                                      autoscalers.LogicalScaleDownTarget):
                            self._replica_manager.scale_down_logically(
                                scaling_option.target.replica_id,
                                scaling_option.target.target_capacity,
                                scaling_option.target.version,
                                scaling_option.target.reconcile_generation)
                        else:
                            assert isinstance(scaling_option.target,
                                              int), scaling_option
                            self._replica_manager.scale_down(
                                scaling_option.target,
                                wait_for_idle=(
                                    scaling_option.reason == autoscalers.
                                    AutoscalerDecisionReason.COST_REBALANCE),
                                expected_version=decision_version)
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

        # Every non-pool service uses the external LB topology. Refuse to boot
        # an externally reachable controller with either auth boundary absent;
        # pools have no LB and retain optional localhost/admin auth.
        auth_required = not getattr(self, '_is_pool', True)
        if auth_required:
            serve_utils.get_lb_sync_auth_tokens(required=True)
            serve_utils.get_controller_admin_auth_tokens(required=True)

        admin_auth_dependency = fastapi.Depends(
            _make_auth_dependency(required=auth_required))
        sync_auth_dependency = fastapi.Depends(
            _make_auth_dependency(sync=True, required=auth_required))
        controller_owner_dependency = fastapi.Depends(
            _make_controller_owner_dependency(
                self._controller_owner_fingerprint))

        @self._app.get(
            serve_constants.CONTROLLER_HEALTH_ENDPOINT_PATH,
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        async def get_controller_health() -> fastapi.Response:
            # Keep child liveness independent from fleet size.  In particular,
            # autoscaler.info() serializes every replica and can legitimately
            # exceed the supervisor's one-second read budget at fleet scale.
            return responses.JSONResponse(content={'status': 'ok'},
                                          status_code=200)

        @self._app.get(
            serve_constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        async def get_placement_state() -> fastapi.Response:
            placer = self._replica_manager.spot_placer
            if placer is None:
                content = {
                    'available': True,
                    'enabled': False,
                    'locations': [],
                    'truncated': False,
                }
            else:
                content = placer.placement_snapshot()
            return responses.JSONResponse(content=content, status_code=200)

        @self._app.get(
            '/autoscaler/info',
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        async def get_autoscaler_info() -> fastapi.Response:
            info = self._autoscaler.info()
            counts = self._replica_counts_snapshot
            if counts is not None:
                info.update(counts)
            info.update(self._get_update_status())
            return responses.JSONResponse(content=info, status_code=200)

        @self._app.post(
            serve_constants.LB_CONTROLLER_SYNC_PATH,
            dependencies=[sync_auth_dependency, controller_owner_dependency])
        async def load_balancer_sync(
                request: fastapi.Request) -> fastapi.Response:
            request_data = await request.json()
            return await self._handle_load_balancer_sync(request_data)

        @self._app.post(
            serve_constants.LB_CONTROLLER_ROLE_PATH,
            dependencies=[sync_auth_dependency, controller_owner_dependency])
        async def load_balancer_role(
                request: fastapi.Request) -> fastapi.Response:
            request_data = await request.json()
            return await self._handle_load_balancer_role(request_data)

        @self._app.post(
            serve_constants.LB_CONTROLLER_HISTORY_SYNC_PATH,
            dependencies=[sync_auth_dependency, controller_owner_dependency])
        async def load_balancer_request_history_sync(
                request: fastapi.Request) -> fastapi.Response:
            request_data = await request.json()
            return await self._handle_load_balancer_request_history_sync(
                request_data)

        # Deliberately a sync handler: parsing and committing the task YAML can
        # perform blocking file/DB I/O. Runtime application happens on the
        # reconciler, so this lock covers only the short durable commit.
        def _serialize_update(
            handler: Callable[..., fastapi.Response]
        ) -> Callable[..., fastapi.Response]:

            @functools.wraps(handler)
            def _wrapped(*args: Any, **kwargs: Any) -> fastapi.Response:
                with self._update_lock:
                    return handler(*args, **kwargs)

            return _wrapped

        @self._app.post(
            '/controller/update_service',
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        @_serialize_update
        def update_service(request_data: dict[str, Any] = fastapi.Body(
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
                    self._service_name,
                    version,
                    resource_scope=self._resource_scope)
                with open(latest_task_yaml, encoding='utf-8') as f:
                    yaml_content = f.read()
                submitted_yaml_content = _read_declared_submitted_yaml(
                    request_data, self._service_name, version,
                    self._resource_scope)
                service = self._load_service_for_update(version, yaml_content)
                requested_service_hash = request_data.get('service_hash')
                lifecycle_epoch = request_data.get('lifecycle_epoch')
                if (requested_service_hash is not None and
                        requested_service_hash != self._service_hash):
                    return responses.JSONResponse(content={
                        'message': 'Service incarnation changed before '
                                   'the update was committed.'
                    },
                                                  status_code=409)
                return self._commit_service_update(version, service,
                                                   yaml_content, update_mode,
                                                   requested_service_hash,
                                                   lifecycle_epoch,
                                                   submitted_yaml_content)
            except Exception as e:  # pylint: disable=broad-except
                exception_str = common_utils.format_exception(e)
                logger.error(f'Error in update_service: {exception_str}')
                return responses.JSONResponse(content={
                    'message': 'Error',
                    'exception': exception_str,
                    'traceback': traceback.format_exc()
                },
                                              status_code=500)

        @self._app.post(
            '/controller/set_load_balancer_high_availability',
            dependencies=[admin_auth_dependency, controller_owner_dependency])
        @_serialize_update
        def set_load_balancer_high_availability(request_data: dict[
            str, Any] = fastapi.Body(...)) -> fastapi.Response:
            try:
                enabled = request_data.get('enabled')
                if not isinstance(enabled, bool):
                    return responses.JSONResponse(
                        content={'message': 'enabled must be a boolean.'},
                        status_code=400)
                expected_service_hash = request_data.get('service_hash')
                expected_lifecycle_epoch = request_data.get('lifecycle_epoch')
                if (not isinstance(expected_service_hash, str) or
                        not expected_service_hash or
                        not isinstance(expected_lifecycle_epoch, int) or
                        isinstance(expected_lifecycle_epoch, bool)):
                    return responses.JSONResponse(content={
                        'message': 'Service incarnation and lifecycle '
                                   'fences are required.'
                    },
                                                  status_code=400)
                self._set_load_balancer_high_availability(
                    enabled, expected_service_hash, expected_lifecycle_epoch)
                return responses.JSONResponse(
                    content={
                        'message': 'Load balancer high availability is '
                                   f'{"enabled" if enabled else "disabled"}.'
                    })
            except RuntimeError as e:
                return responses.JSONResponse(content={'message': str(e)},
                                              status_code=409)
            except Exception as e:  # pylint: disable=broad-except
                exception_str = common_utils.format_exception(e)
                logger.error('Error changing load balancer high '
                             f'availability: {exception_str}')
                return responses.JSONResponse(content={
                    'message': 'Error',
                    'exception': exception_str,
                    'traceback': traceback.format_exc()
                },
                                              status_code=500)

        @self._app.post(
            '/controller/terminate_replica',
            dependencies=[admin_auth_dependency, controller_owner_dependency])
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

        # A committed update is the API success boundary. Apply it on a
        # controller-owned worker so fleet-wide replica locks cannot make the
        # request time out or prevent a later update from committing.
        thread_utils.start_supervised_thread(self._run_update_reconciler,
                                             'service-update-reconciler')

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
def run_controller(service_name: str,
                   service_spec: serve.SkyServiceSpec,
                   version: int,
                   controller_host: str,
                   controller_port: int,
                   controller_owner_fingerprint: str,
                   resource_scope: str | None = None,
                   service_hash: str | None = None,
                   controller_pid: int | None = None,
                   controller_ip: str | None = None,
                   enforce_launch_fence: bool = False):
    os.environ[constants.OVERRIDE_CONSOLIDATION_MODE] = 'true'
    # Hijack sys.stdout/stderr to be context aware.
    context_utils.hijack_sys_attrs()
    controller = SkyServeController(service_name, service_spec, version,
                                    controller_host, controller_port,
                                    controller_owner_fingerprint,
                                    resource_scope, service_hash,
                                    controller_pid, controller_ip,
                                    enforce_launch_fence)
    controller.run()
