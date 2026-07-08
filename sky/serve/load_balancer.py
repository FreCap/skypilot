"""LoadBalancer: Distribute any incoming request to all ready replicas."""
import argparse
import asyncio
import contextlib
import hmac
import logging
import os
import signal
import threading
import time
import traceback
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import aiohttp
import fastapi
import httpx
from starlette import background
import uvicorn

from sky import sky_logging
from sky.serve import constants
from sky.serve import load_balancing_policies as lb_policies
from sky.serve import serve_utils
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)

# Per-client in-flight request counter attribute. Attached to the
# httpx.AsyncClient OBJECT (not keyed by URL): a URL pruned and re-added
# gets a fresh client while the old one is still draining, and the two
# must not share a counter.
_INFLIGHT_ATTR = '_sky_inflight_requests'


class _RetriableStatusError(Exception):
    """A replica answered with a status the service marked retriable.

    Returned from _proxy_request_to like transport errors so
    _proxy_with_retries re-routes the (idempotent) request to another
    replica. Only statuses listed in the service's
    load_balancer.retriable_status_codes take this path — everything
    else streams to the client verbatim.
    """

    def __init__(self, status_code: int, url: str) -> None:
        super().__init__(
            f'replica {url} answered retriable status {status_code}')
        self.status_code = status_code


def _is_dead_connection_error(exc: Exception) -> bool:
    """Whether a proxy failure indicates a DEAD replica vs a saturated one.

    A healthy replica overloaded at high RPS trips the connect/read timeout
    (httpx.TimeoutException), so timeouts must NOT count toward eviction --
    evicting a merely-saturated replica shrinks capacity under load and
    cascades. Only genuine connection failures (refused/reset: NetworkError,
    ProtocolError) indicate a dead replica worth evicting.
    """
    if isinstance(exc, httpx.TimeoutException):
        return False
    return isinstance(exc, (httpx.NetworkError, httpx.ProtocolError))


class _DrainableServer(uvicorn.Server):
    """A uvicorn Server that drains gracefully on SIGTERM.

    uvicorn installs its own SIGTERM/SIGINT handlers inside
    ``Server.serve()`` via ``capture_signals()`` (there is no
    ``install_signal_handlers`` config knob in modern uvicorn), and its default
    handler sets ``should_exit`` immediately -- which would kill in-flight
    requests and skip the deregister step. We instead install our own
    event-loop signal handlers (asyncio-safe) and suppress uvicorn's, so
    SIGTERM begins draining (fail readiness + stop the controller sync) and the
    server only exits after ``LB_DRAIN_GRACE_SECONDS`` -- long enough for k8s to
    pull the pod from the Service and for in-flight requests to finish. A
    second signal / SIGINT exits promptly.
    """

    def __init__(self, config: 'uvicorn.Config', on_drain: 'Any') -> None:
        super().__init__(config)
        self._on_drain = on_drain
        self._own_signals = False

    @contextlib.contextmanager
    def capture_signals(self):
        # Suppress uvicorn's own signal handlers when we installed ours;
        # otherwise fall back to uvicorn's handling (e.g. platforms without
        # loop.add_signal_handler).
        if self._own_signals:
            yield
        else:
            with super().capture_signals():
                yield

    async def serve_with_drain(self) -> None:
        loop = asyncio.get_running_loop()

        def _on_sigterm() -> None:
            self._on_drain()
            loop.call_later(constants.LB_DRAIN_GRACE_SECONDS,
                            lambda: setattr(self, 'should_exit', True))

        try:
            loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
            loop.add_signal_handler(signal.SIGINT,
                                    lambda: setattr(self, 'should_exit', True))
            self._own_signals = True
        except NotImplementedError:
            # add_signal_handler is unavailable (e.g. Windows); let uvicorn
            # manage signals (no graceful drain, but a correct shutdown).
            self._own_signals = False
        await self.serve()


class _InboundAuthMiddleware:
    """Pure-ASGI bearer gate for inbound inference requests (data-plane auth).

    Implemented as raw ASGI rather than ``BaseHTTPMiddleware`` on purpose: it
    inspects only the request headers and either short-circuits with a 401 or
    delegates to the app, so it NEVER buffers or re-relays the response body.
    Streaming/SSE inference responses and the catch-all proxy's slot-release
    (generator ``finally`` + ``BackgroundTask``) pass through untouched, and the
    hot path takes no per-request task/memory-stream overhead.

    No-op when ``LB_AUTH_TOKEN_ENV_VAR`` is unset (dev / in-pod). Exempts ONLY
    GET/HEAD on the readiness route -- any other method there falls through to
    the (authenticated) catch-all proxy. Constant-time compare, ASCII-guarded so
    a malformed header is a clean 401 rather than a 500.
    """

    def __init__(self, app) -> None:
        self._app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope['type'] == 'http' and not self._authorized(scope):
            await fastapi.responses.JSONResponse(status_code=401,
                                                 content={
                                                     'detail': 'Unauthorized.'
                                                 })(scope, receive, send)
            return
        await self._app(scope, receive, send)

    @staticmethod
    def _authorized(scope) -> bool:
        expected_token = serve_utils.get_lb_auth_token()
        if expected_token is None:
            return True
        if (scope['method'] in ('GET', 'HEAD') and
                scope['path'] == constants.LB_HEALTH_ENDPOINT_PATH):
            return True
        authorization = None
        for name, value in scope.get('headers', []):
            if name == b'authorization':
                authorization = value.decode('latin-1')
                break
        if authorization is None or not authorization.isascii():
            return False
        return hmac.compare_digest(authorization, f'Bearer {expected_token}')


class SkyServeLoadBalancer:
    """SkyServeLoadBalancer: distribute incoming traffic with proxy.

    This class accept any traffic to the controller and proxies it
    to the appropriate endpoint replica according to the load balancing
    policy.
    """

    # Demand-feed state also gets class-level zero-defaults: the demand
    # gauges are touched from every request path, so they must degrade to
    # "no demand" rather than AttributeError on a partially-initialized
    # instance (this fork's tests build the LB via object.__new__ with
    # only the attrs under test). All defaults are immutable -- the
    # reject dict defaults to None and is materialized per-instance by
    # _prune_reject_window (the single funnel every read and write goes
    # through), so instances cannot leak reject state into one another.
    _queue_depth: int = 0
    _reject_last_seen: Optional[Dict[str, float]] = None
    _reject_fallback_seq: int = 0
    _capacity_hint: Optional[Dict[str, Any]] = None
    _draining_clients: Optional[Dict[str, List[httpx.AsyncClient]]] = None

    def __init__(
        self,
        controller_url: str,
        load_balancer_port: int,
        load_balancing_policy_name: Optional[str] = None,
        tls_credential: Optional[serve_utils.TLSCredential] = None,
        target_qps_per_replica: Optional[Union[float, Dict[str, float]]] = None,
        stream_timeout_seconds: Optional[int] = None,
        retriable_status_codes: Optional[List[int]] = None,
        max_retries: Optional[int] = None,
        retry_initial_backoff_seconds: Optional[float] = None,
    ) -> None:
        """Initialize the load balancer.

        The routing spec -- load-balancing policy, per-replica target QPS, and
        stream timeout -- is fetched from the controller over the
        load_balancer_sync channel (see `_apply_routing_spec`), so `sky serve
        update` changes to those fields reach a running LB without re-rolling
        it. The corresponding constructor args are only a bootstrap seed (used
        by the in-pod caller, which already has the spec): until the first sync
        lands, the LB serves with whatever policy is built here, and the
        readiness gate keeps traffic away until that first sync arrives. A
        standalone LB passes None for all three and picks them up from sync.

        Args:
            controller_url: The URL of the controller.
            load_balancer_port: The port where the load balancer listens to.
            load_balancing_policy_name: Seed load balancing policy name.
                Defaults to None (the default policy until the first sync).
            tls_credentials: The TLS credentials for HTTPS endpoint. Defaults
                to None.
            target_qps_per_replica: Seed target QPS per replica for
                instance-aware load balancing. Can be a float or dict mapping
                GPU types to QPS. Defaults to None.
            stream_timeout_seconds: Seed timeout in seconds for proxied
                responses. Defaults to None (the built-in default until synced).
        """
        self._app = fastapi.FastAPI()
        self._controller_url: str = controller_url
        self._load_balancer_port: int = load_balancer_port
        # Use the registry to create the load balancing policy. Track the
        # resolved policy name so a sync only rebuilds the policy object when
        # the name actually changes (a policy swap is rare -- only on an
        # update that changes the policy).
        self._load_balancing_policy_name: str = (
            lb_policies.LoadBalancingPolicy.make_policy_name(
                load_balancing_policy_name))
        self._load_balancing_policy = lb_policies.LoadBalancingPolicy.make(
            self._load_balancing_policy_name)

        # Set accelerator QPS for instance-aware policies
        if (target_qps_per_replica and
                isinstance(target_qps_per_replica, dict) and
                isinstance(self._load_balancing_policy,
                           lb_policies.InstanceAwareLeastLoadPolicy)):
            self._load_balancing_policy.set_target_qps_per_accelerator(
                target_qps_per_replica)

        logger.info('Starting load balancer with policy '
                    f'{self._load_balancing_policy_name}.')
        self._request_aggregator: serve_utils.RequestsAggregator = (
            serve_utils.RequestTimestamp())
        self._tls_credential: Optional[serve_utils.TLSCredential] = (
            tls_credential)
        self._stream_timeout_seconds: int = (
            stream_timeout_seconds if stream_timeout_seconds is not None else
            constants.DEFAULT_LB_STREAM_TIMEOUT)
        # Replica responses with these statuses are re-routed like
        # transport failures (empty = never, the default). Safe only for
        # idempotent workloads and "not now" statuses (503/429): the body
        # is discarded before any byte reaches the client.
        self._retriable_status_codes = frozenset(retriable_status_codes or ())
        # Retry-loop tuning (service YAML load_balancer.max_retries /
        # retry_initial_backoff_seconds). With failed-URL exclusion, more
        # retries = more distinct replicas tried before the client sees an
        # error; the backoff prices how fast we fail over.
        self._max_retries: int = (max_retries if max_retries is not None else
                                  constants.LB_MAX_RETRY)
        self._retry_initial_backoff_seconds: float = (
            retry_initial_backoff_seconds if retry_initial_backoff_seconds
            is not None else constants.LB_RETRY_INITIAL_BACKOFF_SECONDS)
        # TODO(tian): httpx.Client has a resource limit of 100 max connections
        # for each client. We should wait for feedback on the best max
        # connections.
        # Reference: https://www.python-httpx.org/advanced/resource-limits/
        #
        # If more than 100 requests are sent to the same replica, the
        # httpx.Client will queue the requests and send them when a
        # connection is available.
        # Reference: https://github.com/encode/httpcore/blob/a8f80980daaca98d556baea1783c5568775daadc/httpcore/_async/connection_pool.py#L69-L71 # pylint: disable=line-too-long
        self._client_pool: Dict[str, httpx.AsyncClient] = dict()
        # We need this lock to avoid getting from the client pool while
        # updating it from _sync_with_controller.
        self._client_pool_lock: threading.Lock = threading.Lock()
        # Passive replica eviction state, guarded by _client_pool_lock (the
        # same lock that guards the policy's ready set). _replica_dead_failures
        # counts consecutive dead-connection failures per replica;
        # _replica_quarantine_until maps a replica URL to the wall-clock time
        # until which it stays out of routing.
        self._replica_dead_failures: Dict[str, int] = dict()
        self._replica_quarantine_until: Dict[str, float] = dict()
        # Rollout state. `_ready` flips true after the first successful
        # controller sync, so k8s never routes to a cold LB. `_draining` flips
        # true on SIGTERM, which fails readiness and stops the controller sync
        # (deregister-before-drain).
        self._ready: bool = False
        self._draining: bool = False
        # Wall-clock time of the last SUCCESSFUL controller sync; the
        # capacity endpoint reports its age so a data-plane reader can
        # judge freshness during control-plane outages (the whole point
        # of reading capacity from the LB instead of `serve status`).
        # Monotonic clock: the age must not be distorted by wall-clock
        # steps (NTP) — hiding staleness is the exact failure this field
        # exists to expose.
        self._last_sync_time: Optional[float] = None
        # Demand-feed gauges for concurrency-native autoscaling. All three
        # are GAUGES re-read whole on every controller sync -- never
        # cleared on ack -- so a failed or duplicated sync cannot lose or
        # double-count demand (only the timestamp aggregator keeps
        # clear-on-report semantics).
        #
        # Requests currently inside _proxy_with_retries (selecting a
        # replica, dispatching, awaiting headers). Plain int is safe:
        # single uvicorn event loop, and the +=/-= pair brackets the
        # handler without an await between a read and its write.
        self._queue_depth: int = 0
        # Reject window with dedup: job key -> last-seen monotonic time.
        # Keyed by the LB_JOB_ID_HEADER value (stable across retries of
        # the same job) so a held job the platform re-fires every ~30s
        # counts as ONE unit of pressure, not window/retry-period units
        # (see the constant's comment for why raw counting over-provisions
        # ~10x). Entries older than LB_REJECT_WINDOW_SECONDS are pruned on
        # access. Monotonic clock: TTLs must not be distorted by
        # wall-clock steps (NTP). (Typed Optional at class level; always
        # a real dict on instances -- _prune_reject_window materializes.)
        self._reject_last_seen = {}
        # Fallback key sequence for requests without the job-id header:
        # each such reject counts once (raw-count over-estimation,
        # documented -- the platform sends the header).
        self._reject_fallback_seq: int = 0
        # Latest capacity_hint from the controller sync response
        # (provisioning/target replica counts). None until a sync carries
        # one (old controller, or never synced); /_lb/capacity readers
        # judge its freshness via last_sync_age_seconds.
        self._capacity_hint: Optional[Dict[str, Any]] = None
        # [boltz fork] Replica-reported async occupancy, from the probe loop
        # (see _probe_occupancy_loop): url -> running async jobs, and
        # url -> free predict slots (max(0, predict_concurrency - running)).
        # Rebuilt wholesale each probe round from the then-ready set, so a
        # pruned replica ages out on the next round. Absent url == probe
        # failed/never ran == occupancy unknown (never assumed busy). Guarded
        # by _client_pool_lock like the rest of the routing state.
        self._replica_occupancy: Dict[str, int] = {}
        self._replica_free_slots: Dict[str, int] = {}
        # Monotonic time of the last COMPLETED probe round (same clock
        # rationale as _last_sync_time: staleness must not hide behind
        # wall-clock steps).
        self._last_occupancy_probe_time: Optional[float] = None
        # Strong refs to in-progress drain-close tasks (see
        # _drain_and_close_client); a bare create_task result can be GCed.
        self._client_close_tasks: Set[asyncio.Task] = set()

    def _quarantined_replicas(self) -> Set[str]:
        """Replica URLs currently quarantined (TTL not yet expired).

        Must be called while holding `_client_pool_lock`.
        """
        now = time.time()
        return {
            url for url, until in self._replica_quarantine_until.items()
            if until > now
        }

    def _quarantine_replica(self, url: str) -> None:
        """Remove a dead replica from routing for the quarantine TTL.

        Must be called while holding `_client_pool_lock`. Drops the replica
        from the policy's ready set immediately so in-flight routing stops
        selecting it; the sync loop keeps it out (even if the controller still
        lists it as ready) until the TTL expires.
        """
        self._replica_quarantine_until[url] = (
            time.time() + constants.LB_EVICTION_QUARANTINE_SECONDS)
        self._replica_dead_failures.pop(url, None)
        remaining = [
            u for u in self._load_balancing_policy.ready_replicas if u != url
        ]
        self._load_balancing_policy.set_ready_replicas(remaining)
        logger.warning(
            f'Evicted replica {url} after '
            f'{constants.LB_EVICTION_CONSECUTIVE_FAILURES} consecutive '
            f'dead-connection failures; quarantined for '
            f'{constants.LB_EVICTION_QUARANTINE_SECONDS}s.')

    def _record_proxy_outcome(
        self, url: str,
        response_or_exception: Union['fastapi.responses.Response', Exception]
    ) -> None:
        """Update per-replica eviction bookkeeping after a proxy attempt.

        Eviction is deliberately scoped to TCP-dead replicas (connection
        refused/reset before headers): those are the ones whose drained
        in-flight slots read as least-loaded and attract preferential routing.
        A dead-connection failure increments the consecutive count and
        quarantines once the threshold is reached. Everything else -- a
        response of any status (incl. 5xx: the replica is reachable and
        releasing its slot), a saturation timeout, or a mid-stream failure
        after headers -- counts as a completed attempt and clears the streak.
        Exception: statuses configured as retriable arrive here as
        _RetriableStatusError and are INERT for eviction — neither a dead
        failure (the replica answered; it is alive) nor a streak-clearing
        success (a shedding replica should not launder an in-progress
        dead-connection streak).
        Application errors and saturation are intentionally NOT eviction
        signals (evicting a reachable-but-slow replica shrinks capacity).
        """
        with self._client_pool_lock:
            if isinstance(response_or_exception, Exception):
                if _is_dead_connection_error(response_or_exception):
                    count = self._replica_dead_failures.get(url, 0) + 1
                    self._replica_dead_failures[url] = count
                    if count >= constants.LB_EVICTION_CONSECUTIVE_FAILURES:
                        self._quarantine_replica(url)
            else:
                self._replica_dead_failures.pop(url, None)
                self._replica_quarantine_until.pop(url, None)

    def _apply_routing_spec(self, routing_spec: Dict[str, Any]) -> None:
        """Apply a routing spec fetched from the controller.

        Must be called while holding `_client_pool_lock` (it mutates the
        policy object the routing hot path reads under that lock). The three
        routing fields arrive over the sync channel so `sky serve update` can
        change them on a running LB without a re-roll:

        - policy: rebuild + swap the policy object ONLY when the resolved
          name changes (cheap + idempotent otherwise). The fresh policy is
          left empty; the caller's immediately-following `set_ready_replicas`
          re-populates it from this same sync, which is why we do not copy
          the old ready set over (that would short-circuit set_ready_replicas
          and skip load-map initialization).
        - target_qps_per_replica / target_concurrency_per_replica: applied
          to the active policy when it is instance-aware -- a QPS dict
          sets per-accelerator weights; a concurrency knob (no dict) sets
          a uniform per-GPU weight and clears stale dict weights left by
          a previous version.
        - stream_timeout_seconds: stored into the per-request instance var so
          it takes effect on subsequent proxied requests.
        """
        policy_name = routing_spec.get('load_balancing_policy_name')
        if (policy_name is not None and
                policy_name != self._load_balancing_policy_name):
            self._load_balancing_policy = lb_policies.LoadBalancingPolicy.make(
                policy_name)
            self._load_balancing_policy_name = (
                lb_policies.LoadBalancingPolicy.make_policy_name(policy_name))
        target_qps_per_replica = routing_spec.get('target_qps_per_replica')
        target_concurrency = routing_spec.get('target_concurrency_per_replica')
        if isinstance(self._load_balancing_policy,
                      lb_policies.InstanceAwareLeastLoadPolicy):
            if isinstance(target_qps_per_replica, dict):
                self._load_balancing_policy.set_target_qps_per_accelerator(
                    target_qps_per_replica)
            elif target_concurrency is not None:
                # Concurrency-sized service: no QPS dict, uniform per-GPU
                # capacity. This also CLEARS a previous version's QPS
                # weights after an update switches sizing modes --
                # keeping them would normalize routing with obsolete
                # per-accelerator targets indefinitely.
                self._load_balancing_policy.set_default_per_gpu_target(
                    float(target_concurrency))
        stream_timeout_seconds = routing_spec.get('stream_timeout_seconds')
        if stream_timeout_seconds is not None:
            self._stream_timeout_seconds = stream_timeout_seconds
        # Retry tuning rides the same channel so `sky serve update` (and
        # external LBs, which never see the spawn args) picks it up live.
        # `None` means "not set in the spec": fall back to the defaults
        # rather than keeping a stale override from a previous version.
        retriable = routing_spec.get('retriable_status_codes')
        self._retriable_status_codes = frozenset(retriable or ())
        max_retries = routing_spec.get('max_retries')
        self._max_retries = (max_retries if max_retries is not None else
                             constants.LB_MAX_RETRY)
        backoff_seconds = routing_spec.get('retry_initial_backoff_seconds')
        self._retry_initial_backoff_seconds = (
            backoff_seconds if backoff_seconds is not None else
            constants.LB_RETRY_INITIAL_BACKOFF_SECONDS)

    def _is_ready_to_serve(self) -> bool:
        """Readiness: true only once synced at least once and not draining."""
        return self._ready and not self._draining

    def _begin_draining(self) -> None:
        """Start draining (idempotent): fail readiness + stop syncing."""
        if not self._draining:
            logger.info('Draining load balancer: failing readiness and '
                        'deregistering from the controller sync.')
        self._draining = True

    async def _health(self,
                      request: fastapi.Request) -> fastapi.responses.Response:
        del request  # Unused.
        return fastapi.responses.Response(
            status_code=200 if self._is_ready_to_serve() else 503)

    def _in_flight_with_draining(self) -> Optional[Dict[str, int]]:
        """Per-url busyness snapshot: envelopes, occupancy, and draining.

        Three measures of the same running jobs, unioned:
        - The policy's envelope in-flight (load_map) covers synchronous
          requests dispatched through this LB.
        - The replica-reported async occupancy covers fast-ack
          workloads, where the HTTP envelope closes in milliseconds and
          the load_map reads ~0 while the replica crunches for an hour.
          Per-url MAX with the envelope count, never sum: a job awaiting
          its fast-ack (in the envelope count) may already appear in
          occupancy -- the same job must count once.
        - The draining overlay covers pruned-but-draining urls: the
          load_map drops a url the moment it leaves the routable set,
          but requests already running keep running on its draining
          client (see _drain_and_close_client). Without it, a
          probe-blipped replica vanishes from the report one sync after
          the blip, reads as idle to the autoscaler, and becomes a
          preferred scale-down victim mid-job. Draining counts add to
          (not max with) the url's total: a re-added url's NEW client
          tracks its fresh requests in the load_map while the OLD
          draining client only carries the pre-blip streams -- distinct
          requests.
        """
        with self._client_pool_lock:
            in_flight = self._load_balancing_policy.snapshot_in_flight()
            occupancy = dict(self._replica_occupancy)
        if in_flight is None:
            return None
        for url, running in occupancy.items():
            if running > in_flight.get(url, 0):
                in_flight[url] = running
        for url, clients in (self._draining_clients or {}).items():
            draining = sum(
                getattr(client, _INFLIGHT_ATTR, 0) for client in clients)
            if draining > 0:
                in_flight[url] = in_flight.get(url, 0) + draining
        return in_flight

    def _prune_reject_window(self) -> Dict[str, float]:
        """Drop reject entries older than the window; return the live dict.

        Called on every access (record + read) rather than on a timer:
        the dict is bounded by unique keys seen in one window, so an
        O(entries) rebuild on the sync/capacity cadence is cheap, and
        lazy pruning means no extra task to keep alive. Always assigns a
        fresh instance dict (materializing it from the None class
        default on first touch), so the class default stays immutable
        and cannot leak state across instances.
        """
        cutoff = time.monotonic() - constants.LB_REJECT_WINDOW_SECONDS
        current = self._reject_last_seen
        pruned: Dict[str, float] = {}
        if current:
            pruned = {
                key: seen for key, seen in current.items() if seen > cutoff
            }
        self._reject_last_seen = pruned
        return pruned

    def _record_rejection(self, request: fastapi.Request) -> None:
        """Record a terminal-503 exit for the reject-window gauge.

        Keyed by the job-id header when present: the platform re-fires
        the SAME held job every ~30s, so the re-fire must refresh the
        TTL and still count once -- that dedup is the whole point of the
        window (raw counting over-provisions ~10x, see constants).
        Headerless requests get a unique per-request key: one unit each.
        """
        # Starlette header lookup is case-insensitive per the HTTP spec.
        key = request.headers.get(constants.LB_JOB_ID_HEADER)
        if key is None:
            self._reject_fallback_seq += 1
            key = f'_headerless_{self._reject_fallback_seq}'
        self._prune_reject_window()[key] = time.monotonic()

    def _rejected_in_window(self) -> int:
        """Unique jobs terminally 503'd within the reject window (gauge)."""
        return len(self._prune_reject_window())

    async def _capacity(
            self, request: fastapi.Request) -> fastapi.responses.JSONResponse:
        """Data-plane capacity read: the volatile half of admission sizing.

        External admission systems size against `sky serve status`, which
        rides the control plane — every API-server restart or outage
        blinds them and they must decay to a conservative floor. The LB
        IS the data plane: it knows the ready set and (for load-tracking
        policies) the in-flight count, and with an external LB it keeps
        serving straight through control-plane restarts. Aggregates only:
        per-replica URLs are internal addresses and admission needs
        counts, not targets. Slow-changing spec fields (max_replicas,
        target qps) intentionally stay on the `serve status` path — they
        only change on `serve update`.
        """
        del request  # Unused.
        with self._client_pool_lock:
            ready_set = set(self._load_balancing_policy.ready_replicas)
            ready_replicas = len(ready_set)
            # [boltz fork] Occupancy aggregates, over probed AND ready
            # replicas only: a probe entry for a since-pruned replica must
            # not count, and an unprobed replica contributes no free slot
            # (unknown != idle — the admission reader can treat the
            # probed/ready gap as optimistically or conservatively as it
            # likes, but this endpoint never invents capacity).
            probed = {
                url: self._replica_free_slots.get(url, 0)
                for url in self._replica_occupancy
                if url in ready_set
            }
            probed_replicas = len(probed)
            busy_replicas = sum(1 for free in probed.values() if free <= 0)
            free_slots = sum(probed.values())
        # Envelope in-flight unioned with occupancy per url (max, same
        # jobs measured two ways) and including pruned-but-draining work:
        # those requests still occupy replica capacity, which is what an
        # admission reader sizes by.
        in_flight_map = self._in_flight_with_draining()
        in_flight = (sum(in_flight_map.values())
                     if in_flight_map is not None else None)
        last_sync_age: Optional[float] = None
        if self._last_sync_time is not None:
            last_sync_age = max(time.monotonic() - self._last_sync_time, 0.0)
        occupancy_probe_age: Optional[float] = None
        if self._last_occupancy_probe_time is not None:
            occupancy_probe_age = max(
                time.monotonic() - self._last_occupancy_probe_time, 0.0)
        # Capacity hint fields stay null until a controller sync carries
        # one: an admission reader must see "unknown" (and fall back to
        # its conservative floor) rather than zeros it would act on.
        hint = self._capacity_hint or {}
        return fastapi.responses.JSONResponse({
            'ready_replicas': ready_replicas,
            'in_flight': in_flight,
            'draining': self._draining,
            'synced': self._ready,
            'last_sync_age_seconds': last_sync_age,
            'queue_depth': self._queue_depth,
            'rejected_in_window': self._rejected_in_window(),
            'provisioning_replicas': hint.get('provisioning_replicas'),
            'target_replicas': hint.get('target_num_replicas'),
            # [boltz fork] Async-occupancy aggregates (see the probe loop).
            # For fast-ack async fleets envelope-only in_flight reads ~0
            # while replicas crunch, so admission should size on
            # free_slots gated by occupancy_probe_age_seconds, exactly
            # like last_sync_age gates the ready count.
            'probed_replicas': probed_replicas,
            'busy_replicas': busy_replicas,
            'free_slots': free_slots,
            'occupancy_probe_age_seconds': occupancy_probe_age,
        })

    # ------------------------------------------------------------------
    # [boltz fork] Async-occupancy probing.
    #
    # The envelope-scoped in-flight accounting (pre/post_execute_hook) reads
    # ~0 for async fast-ack workloads: the POST is acknowledged in
    # milliseconds while the replica crunches a job for up to hours in a
    # background thread. The replica itself knows its true occupancy — the
    # async wrapper answers an `async_capacity` action (on the same predict
    # route, from the HTTP handler, so it responds while crunching) with its
    # running-job count and predict concurrency. The LB probes that per
    # ready replica and feeds it to (a) the routing policy, which
    # deprioritizes busy replicas instead of discovering them via 429
    # bounces, and (b) /_lb/capacity, so external admission can size on
    # true free slots. A failed probe marks the replica occupancy-UNKNOWN
    # (treated as idle by routing, excluded from free-slot aggregates) —
    # the probe is a hint; the replica's own shedding stays authoritative.
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_replica_occupancy(raw: Any) -> Optional[Tuple[int, int]]:
        """(running_count, free_slots) from an async_capacity payload.

        None on any non-conforming shape (older image without the action,
        or an error body): the caller treats it as occupancy unknown.
        A DRAINING replica reports predict_concurrency 0, so its free
        slots are naturally 0 without a special case.
        """
        if not isinstance(raw, dict):
            return None
        running = raw.get('running_count')
        concurrency = raw.get('predict_concurrency')
        if not isinstance(running, int) or isinstance(running, bool):
            return None
        if not isinstance(concurrency, int) or isinstance(concurrency, bool):
            return None
        if running < 0 or concurrency < 0:
            return None
        return running, max(0, concurrency - running)

    async def _fetch_replica_occupancy(
            self, session: 'aiohttp.ClientSession',
            replica_url: str) -> Optional[Tuple[int, int]]:
        """One occupancy probe; None on any failure (unknown, never busy)."""
        try:
            async with session.post(
                    f'{replica_url}/v1/models/model:predict',
                    json={'action': 'async_capacity'},
                    timeout=aiohttp.ClientTimeout(
                        total=constants.LB_OCCUPANCY_PROBE_TIMEOUT_SECONDS),
            ) as response:
                if response.status != 200:
                    return None
                return self._parse_replica_occupancy(await response.json())
        except Exception:  # pylint: disable=broad-except
            # Dead pod, mid-restart, or a slow/protocol-broken answer — the
            # proxy path's eviction machinery owns dead-replica handling;
            # the probe only reports what it could confirm.
            return None

    async def _probe_replica_occupancy_once(self) -> None:
        """One probe round: rebuild the occupancy maps from the ready set."""
        with self._client_pool_lock:
            ready_urls = list(self._load_balancing_policy.ready_replicas)
        if not ready_urls:
            with self._client_pool_lock:
                self._replica_occupancy = {}
                self._replica_free_slots = {}
                self._last_occupancy_probe_time = time.monotonic()
                self._load_balancing_policy.set_occupancy({})
            return
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *(self._fetch_replica_occupancy(session, url)
                  for url in ready_urls))
        occupancy: Dict[str, int] = {}
        free_slots: Dict[str, int] = {}
        for url, result in zip(ready_urls, results):
            if result is None:
                continue
            occupancy[url], free_slots[url] = result
        with self._client_pool_lock:
            self._replica_occupancy = occupancy
            self._replica_free_slots = free_slots
            self._last_occupancy_probe_time = time.monotonic()
            # Push into the policy under the same lock the sync loop holds
            # for policy swaps; a policy swapped after this round serves
            # without occupancy for at most one probe interval.
            self._load_balancing_policy.set_occupancy(occupancy)

    async def _probe_occupancy_loop(self) -> None:
        """Background occupancy prober, beside the controller-sync loop."""
        interval_str = os.environ.get(
            constants.LB_OCCUPANCY_PROBE_INTERVAL_ENV_VAR,
            str(constants.LB_OCCUPANCY_PROBE_INTERVAL_SECONDS))
        try:
            interval = float(interval_str)
        except ValueError:
            logger.warning('Invalid %s=%r; using default %ss.',
                           constants.LB_OCCUPANCY_PROBE_INTERVAL_ENV_VAR,
                           interval_str,
                           constants.LB_OCCUPANCY_PROBE_INTERVAL_SECONDS)
            interval = float(constants.LB_OCCUPANCY_PROBE_INTERVAL_SECONDS)
        if interval <= 0:
            logger.info('Occupancy probe disabled (interval <= 0).')
            return
        while not self._draining:
            try:
                await self._probe_replica_occupancy_once()
            except Exception:  # pylint: disable=broad-except
                # The prober must never die: without it routing silently
                # degrades to envelope-only accounting for the LB's whole
                # lifetime, which is exactly the blindness it exists to fix.
                logger.error('Occupancy probe round failed: '
                             f'{traceback.format_exc()}')
            await asyncio.sleep(interval)

    async def _sync_with_controller_once(self) -> None:
        ready_replica_urls = []
        replica_info = {}
        routing_spec = None
        capacity_hint = None

        # Present the control-plane bearer token so the (now-authenticated)
        # sync path accepts this LB. No-op header when auth is disabled.
        controller_token = serve_utils.get_controller_auth_token()
        sync_headers = ({
            'Authorization': f'Bearer {controller_token}'
        } if controller_token is not None else None)

        # [boltz fork] Demand gauges ride alongside the timestamp
        # aggregator so the concurrency autoscaler sees outstanding work,
        # not just arrival compression. They are GAUGES -- re-read whole
        # every sync, never cleared on ack -- so a controller hiccup can
        # neither lose nor double-count demand; only the timestamps below
        # keep their existing clear-on-report semantics. The in-flight
        # map may be None (policy without load accounting): sent as-is,
        # the controller treats it as unknown rather than an idle fleet.
        # NOTE: gauges are last-writer-wins per LB (unlike the additive
        # timestamps) -- correct for the pinned single-replica LB
        # deployment; a multi-LB rollout would need per-LB keying.
        in_flight = self._in_flight_with_draining()
        sync_payload = {
            'request_aggregator': self._request_aggregator.to_dict(),
            'in_flight': in_flight,
            'queue_depth': self._queue_depth,
            'rejected_in_window': self._rejected_in_window(),
        }
        async with aiohttp.ClientSession() as session:
            try:
                # Send request information
                async with session.post(
                        self._controller_url + '/controller/load_balancer_sync',
                        json=sync_payload,
                        headers=sync_headers,
                        timeout=aiohttp.ClientTimeout(
                            constants.LB_CONTROLLER_SYNC_TIMEOUT_SECONDS),
                ) as response:
                    response.raise_for_status()
                    # Clean up only after the controller ACCEPTED the report
                    # (2xx). Clearing before raise_for_status would silently
                    # drop the batch on a failed sync (e.g. 401), starving the
                    # autoscaler of load signal it never received. The rare
                    # partial-failure inverse (controller counted it but the LB
                    # saw a non-2xx) re-sends the batch, double-counting a few
                    # timestamps -- tolerated: it biases autoscaling toward
                    # transient over-provisioning, the safe direction.
                    self._request_aggregator.clear()
                    response_json = await response.json()
                    replica_info = response_json.get('replica_info', {})
                    # [boltz fork] The controller ships the routing config
                    # (policy/target-qps/stream-timeout) alongside replica_info
                    # so `sky serve update` changes reach this LB without a
                    # re-roll. Older controllers omit the key -> None -> the LB
                    # keeps its launch-seeded policy.
                    routing_spec = response_json.get('routing_spec')
                    # [boltz fork] Provisioning/target counts for the
                    # /_lb/capacity read; absent on older controllers.
                    capacity_hint = response_json.get('capacity_hint')
                    ready_replica_urls = list(replica_info.keys())
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f'An error occurred when syncing with '
                             f'the controller: {e}'
                             f'\nTraceback: {traceback.format_exc()}')
            else:
                logger.info(f'Available Replica URLs: {ready_replica_urls}')
                with self._client_pool_lock:
                    # Apply the fetched routing spec BEFORE (re)setting the
                    # ready replicas: if the policy object was swapped, the
                    # set_ready_replicas below re-populates the fresh policy
                    # from this same sync.
                    if routing_spec:
                        self._apply_routing_spec(routing_spec)
                    # Keep quarantined (locally-evicted) replicas out of
                    # routing even if the controller still lists them as
                    # ready, until their TTL expires -- otherwise a dead
                    # replica would be re-added on every sync and eviction
                    # would oscillate.
                    quarantined = self._quarantined_replicas()
                    routable = [
                        url for url in ready_replica_urls
                        if url not in quarantined
                    ]
                    self._load_balancing_policy.set_ready_replicas(routable)
                    # Set replica info for instance-aware policies
                    if isinstance(self._load_balancing_policy,
                                  lb_policies.InstanceAwareLeastLoadPolicy):
                        self._load_balancing_policy.set_replica_info(
                            replica_info)
                    # Drop eviction bookkeeping for replicas the controller no
                    # longer lists, and for expired quarantines, so the maps
                    # do not grow unbounded.
                    ready_set = set(ready_replica_urls)
                    now = time.time()
                    self._replica_dead_failures = {
                        url: count
                        for url, count in self._replica_dead_failures.items()
                        if url in ready_set
                    }
                    self._replica_quarantine_until = {
                        url: until
                        for url, until in
                        self._replica_quarantine_until.items()
                        if url in ready_set and until > now
                    }
                    for replica_url in ready_replica_urls:
                        if replica_url not in self._client_pool:
                            self._client_pool[replica_url] = httpx.AsyncClient(
                                base_url=replica_url)
                    urls_to_close = set(
                        self._client_pool.keys()) - set(ready_replica_urls)
                    client_to_close = []
                    for replica_url in urls_to_close:
                        client_to_close.append(
                            (replica_url, self._client_pool.pop(replica_url)))
                for replica_url, client in client_to_close:
                    # Fire-and-forget: a drain can legitimately take as long
                    # as the longest in-flight prediction; the sync loop must
                    # never wait on it. Strong refs held in the task set (a
                    # bare create_task result can be garbage collected).
                    # Registered in _draining_clients first so the demand
                    # feed keeps attributing the still-running work to the
                    # pruned url (see _in_flight_with_draining).
                    if self._draining_clients is None:
                        self._draining_clients = {}
                    self._draining_clients.setdefault(replica_url,
                                                      []).append(client)
                    task = asyncio.create_task(
                        self._drain_and_close_client(replica_url, client))
                    self._client_close_tasks.add(task)
                    task.add_done_callback(self._client_close_tasks.discard)
                # Cache the controller's capacity hint for /_lb/capacity.
                # Absence (older controller) resets to None rather than
                # keeping a stale previous value: readers must see
                # "unknown", not confidently-wrong counts.
                self._capacity_hint = (capacity_hint if isinstance(
                    capacity_hint, dict) else None)
                # First successful sync -> ready to serve (readiness gate).
                self._ready = True
                self._last_sync_time = time.monotonic()

    async def _drain_and_close_client(self, url: str,
                                      client: httpx.AsyncClient) -> None:
        """Close a pruned replica's client once its in-flight work drains.

        aclose() cancels every request still running on the client, so
        closing at prune time turned every graceful replica removal
        (spot drain, rolling update, transient NOT_READY) into aborted
        in-flight predictions. Wait for the per-client in-flight counter
        (maintained by _proxy_request_to) to reach zero; the deadline
        (stream timeout + margin) bounds leaked connections if a counter
        is ever stuck.
        """
        deadline = (asyncio.get_event_loop().time() +
                    self._stream_timeout_seconds +
                    constants.LB_DRAIN_CLOSE_GRACE_SECONDS)
        while (getattr(client, _INFLIGHT_ATTR, 0) > 0 and
               asyncio.get_event_loop().time() < deadline):
            await asyncio.sleep(1)
        inflight = getattr(client, _INFLIGHT_ATTR, 0)
        if inflight > 0:
            logger.warning(f'Closing drained client for {url} with '
                           f'{inflight} request(s) still in flight '
                           '(drain deadline exceeded).')
        try:
            await client.aclose()
        finally:
            # Deregister from the demand feed: the drained client's work
            # is finished (or force-closed) and must stop counting as
            # in-flight for this url.
            clients = (self._draining_clients or {}).get(url)
            if clients is not None:
                with contextlib.suppress(ValueError):
                    clients.remove(client)
                if not clients:
                    assert self._draining_clients is not None
                    del self._draining_clients[url]

    async def _sync_with_controller(self):
        """Sync with controller periodically.

        Every `constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS` seconds, the
        load balancer will sync with the controller to get the latest
        information about available replicas; also, it report the request
        information to the controller, so that the controller can make
        autoscaling decisions.
        """
        # Sleep for a while to wait the controller bootstrap.
        await asyncio.sleep(5)

        while True:
            # Once draining, stop POSTing load_balancer_sync so the controller
            # stops counting this LB's request timestamps -- otherwise it would
            # double-count with the maxSurge replacement during a roll.
            if self._draining:
                logger.info('Draining: stopped syncing with the controller.')
                return
            try:
                await self._sync_with_controller_once()
                await asyncio.sleep(
                    constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'An error occurred when syncing with '
                             f'the controller: {e}'
                             f'\nTraceback: {traceback.format_exc()}')

    async def _proxy_request_to(
        self, url: str, request: fastapi.Request
    ) -> Union[fastapi.responses.Response, Exception]:
        """Proxy the request to the specified URL.

        Returns:
            The response from the endpoint replica. Return the exception
            encountered if anything goes wrong.
        """
        logger.info(f'Proxy request to {url}')
        # The token ties this request's release to the exact accounting
        # generation it incremented (see LoadBalancingPolicy hooks).
        slot_token = self._load_balancing_policy.pre_execute_hook(url, request)
        # Every exit that does NOT hand a streaming response to the client
        # must release the in-flight slot itself, or failed/aborted attempts
        # permanently inflate this replica's load and skew routing away
        # from it (each retry then leaks another slot on another replica).
        released = False
        try:
            # We defer the get of the client here on purpose, for case when the
            # replica is ready in `_proxy_with_retries` but refreshed before
            # entering this function. In that case we will return an error here
            # and retry to find next ready replica. We also need to wait for the
            # update of the client pool to finish before getting the client.
            with self._client_pool_lock:
                client = self._client_pool.get(url, None)
            if client is None:
                return RuntimeError(f'Client for {url} not found.')
            # Counted on the CLIENT object so a pruned client is closed
            # only after its in-flight work drains (a re-added URL gets a
            # fresh client with its own counter). Decremented exactly once
            # per request alongside the slot release below.
            setattr(client, _INFLIGHT_ATTR,
                    getattr(client, _INFLIGHT_ATTR, 0) + 1)
            client_refcount_dropped = False

            def _drop_client_refcount():
                nonlocal client_refcount_dropped
                if client_refcount_dropped:
                    return
                client_refcount_dropped = True
                setattr(client, _INFLIGHT_ATTR,
                        getattr(client, _INFLIGHT_ATTR, 1) - 1)

            worker_url = httpx.URL(path=request.url.path,
                                   query=request.url.query.encode('utf-8'))
            proxy_request = client.build_request(
                request.method,
                worker_url,
                headers=request.headers.raw,
                content=await request.body(),
                # A scalar here would ALSO set the connect timeout: with a
                # long stream timeout (sync model servers send no bytes
                # until compute completes, so read must cover the whole
                # prediction), a dead-but-still-routed replica would hang
                # requests for the full value during the un-route window
                # instead of failing fast into the retry loop.
                timeout=httpx.Timeout(
                    self._stream_timeout_seconds,
                    connect=constants.LB_CONNECT_TIMEOUT_SECONDS))
            proxy_response = await client.send(proxy_request, stream=True)

            if proxy_response.status_code in self._retriable_status_codes:
                # "Not now" from the replica (e.g. 503 while the model
                # warms, 429 shedding): discard and re-route. No byte has
                # reached the client — send() returns at headers with
                # stream=True. Slot + client refcount release via the
                # not-released finally below.
                await proxy_response.aclose()
                return _RetriableStatusError(proxy_response.status_code, url)

            # The slot is owned by the stream now. Starlette runs
            # BackgroundTasks strictly AFTER a successful stream — a
            # mid-stream failure (client disconnect, upstream reset)
            # skips them — so the release lives in the ITERATOR's
            # finally (generator close on any exit runs it) with the
            # background task as a second, idempotent safety net for
            # the stream-never-started edge.
            release_state = {'done': False}

            async def _release_slot():
                if release_state['done']:
                    return
                release_state['done'] = True
                try:
                    await proxy_response.aclose()
                finally:
                    self._load_balancing_policy.post_execute_hook(
                        url, request, slot_token)
                    _drop_client_refcount()

            async def _stream_with_release():
                try:
                    async for chunk in proxy_response.aiter_raw():
                        yield chunk
                finally:
                    await _release_slot()

            response = fastapi.responses.StreamingResponse(
                content=_stream_with_release(),
                status_code=proxy_response.status_code,
                headers=proxy_response.headers,
                background=background.BackgroundTask(_release_slot))
            # Ownership of the slot transfers to the stream/background pair
            # only once the response object exists and will be returned.
            released = True
            return response
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.error(f'Error when proxy request to {url}: '
                         f'{common_utils.format_exception(e)}'
                         f'\nTraceback: {traceback.format_exc()}')
            return e
        finally:
            if not released:
                self._load_balancing_policy.post_execute_hook(
                    url, request, slot_token)
                # Only defined once the client was checked out; exits
                # before that (no client) have nothing to drop.
                if 'client' in locals() and client is not None:
                    _drop_client_refcount()

    async def _proxy_with_retries(
            self, request: fastapi.Request) -> fastapi.responses.Response:
        """Try to proxy the request to the endpoint replica with retries."""
        # Queue-depth gauge: requests currently inside the retry handler
        # but NOT dispatched to a replica (selecting, in retry backoff).
        # The dispatched phase is deliberately excluded: the dispatch
        # site below hands the unit off to the policy's load_map for the
        # duration of the proxy await. For synchronous model servers,
        # response headers arrive only when compute completes, so
        # counting that await here too would double-count every RUNNING
        # job (load_map + queue_depth) for its entire ~1h duration —
        # a sustained ~2x inflation of the autoscaler's outstanding-work
        # sum, not a transient blip.
        # Incremented on entry, decremented on EVERY exit -- return,
        # raise, cancellation -- via the finally; a leaked unit would
        # permanently inflate the autoscaler's demand signal. A returned
        # StreamingResponse leaves the handler while its body still
        # streams: that phase is accounted by the policy's load_map, not
        # here. Plain int is safe: single uvicorn event loop, no await
        # between each read and its paired write.
        self._queue_depth += 1
        try:
            return await self._proxy_with_retries_inner(request)
        finally:
            self._queue_depth -= 1

    async def _proxy_with_retries_inner(
            self, request: fastapi.Request) -> fastapi.responses.Response:
        """Retry loop body, bracketed by the queue-depth gauge above."""
        self._request_aggregator.add(request)
        # TODO(tian): Finetune backoff parameters.
        backoff = common_utils.Backoff(
            initial_backoff=self._retry_initial_backoff_seconds)
        # SkyServe supports serving on Spot Instances. To avoid preemptions
        # during request handling, we add a retry here.
        retry_cnt = 0
        # URLs that already failed THIS request: without exclusion,
        # least-load retries deterministically re-select a
        # dead-but-not-yet-pruned replica on a busy fleet (it sits at
        # load 0 while every healthy replica carries traffic).
        failed_urls: Set[str] = set()

        def _unavailable(detail: str) -> fastapi.HTTPException:
            # Both terminal-503 exits ("no ready replicas", "all ready
            # replicas at capacity") funnel through here: record the
            # rejection so the demand feed keeps pressure on the
            # autoscaler for as long as the job stays unplaced (the QPS
            # window alone decays while the need persists).
            self._record_rejection(request)
            # Retry-After lets a well-behaved client back off instead of
            # hammering; the ready set only changes on the controller
            # sync cadence, so in-LB sleeping is not a useful wait.
            return fastapi.HTTPException(
                status_code=503,
                detail=detail,
                headers={
                    'Retry-After': str(constants.LB_503_RETRY_AFTER_SECONDS)
                })

        while True:
            retry_cnt += 1
            with self._client_pool_lock:
                ready_replica_url = self._load_balancing_policy.select_replica(
                    request, exclude=failed_urls)
            if ready_replica_url is None:
                # Nothing to select at all: burning the remaining attempts
                # asleep only adds latency (and multiplies under the
                # client retry layer) — fail fast with a backoff hint.
                raise _unavailable('No ready replicas. '
                                   'Use "sky serve status [SERVICE_NAME]" '
                                   'to check the replica status.')
            else:
                # Hand the unit off for the dispatch: the proxy await is
                # accounted by the policy's load_map (pre_execute_hook),
                # and for synchronous servers it lasts until compute
                # completes — keeping it in queue_depth as well would
                # double-count the running job. Re-taken in the finally
                # so a failed attempt is queued again while it backs off.
                self._queue_depth -= 1
                try:
                    response_or_exception = await self._proxy_request_to(
                        ready_replica_url, request)
                finally:
                    self._queue_depth += 1
                # Passively evict a replica that keeps failing with dead
                # connections during the controller-pause window.
                self._record_proxy_outcome(ready_replica_url,
                                           response_or_exception)
            if not isinstance(response_or_exception, Exception):
                return response_or_exception
            failed_urls.add(ready_replica_url)
            with self._client_pool_lock:
                all_ready_tried = failed_urls.issuperset(
                    self._load_balancing_policy.ready_replicas)
            # When the user aborts the request during streaming, the request
            # will be disconnected. We do not need to retry for this case.
            if await request.is_disconnected():
                # 499 means a client terminates the connection
                # before the server is able to respond.
                return fastapi.responses.Response(status_code=499)
            if (all_ready_tried and
                    isinstance(response_or_exception, _RetriableStatusError)):
                # Every ready replica already shed THIS request with a
                # retriable status (e.g. all busy at PREDICT_CONCURRENCY
                # capacity, or warming): none of them will free within the
                # 0.5-4s backoff schedule, so remaining attempts are pure
                # added latency. Transport failures deliberately do NOT
                # take this exit: a lone replica's connection blip still
                # recovers transparently via the full-set fallback.
                raise _unavailable('All ready replicas are at capacity. '
                                   f'Last error: {response_or_exception}.')
            # TODO(tian): Fail fast for errors like 404 not found.
            if retry_cnt >= self._max_retries:
                if isinstance(response_or_exception, fastapi.HTTPException):
                    raise response_or_exception
                exception = common_utils.remove_color(
                    common_utils.format_exception(response_or_exception,
                                                  use_bracket=True))
                raise fastapi.HTTPException(
                    # 500 means internal server error.
                    status_code=500,
                    detail=f'Max retries {self._max_retries} exceeded. '
                    f'Last error encountered: {exception}. Please use '
                    '"sky serve logs [SERVICE_NAME] --load-balancer" '
                    'for more information.')
            current_backoff = backoff.current_backoff()
            logger.error(f'Retry in {current_backoff} seconds.')
            await asyncio.sleep(current_backoff)

    def run(self):
        # Gate inbound inference requests on the shared bearer token (no-op when
        # unset). Pure-ASGI so it wraps the catch-all proxy without buffering
        # streaming responses; exempts the readiness probe by method+path.
        self._app.add_middleware(_InboundAuthMiddleware)
        # Register the readiness route BEFORE the catch-all proxy route so it
        # is matched first (Starlette matches in registration order) instead of
        # being proxied to a replica.
        self._app.add_api_route(constants.LB_HEALTH_ENDPOINT_PATH,
                                self._health,
                                methods=['GET'])
        # /_lb/capacity is a data-plane read for external admission systems, so
        # it stays behind the inbound bearer (unlike the readiness probe): the
        # same authenticated client that sends inference reads capacity.
        self._app.add_api_route('/_lb/capacity',
                                self._capacity,
                                methods=['GET'])
        self._app.add_api_route('/{path:path}',
                                self._proxy_with_retries,
                                methods=['GET', 'POST', 'PUT', 'DELETE'])

        @self._app.on_event('startup')
        async def startup():
            # Configure logger
            uvicorn_access_logger = logging.getLogger('uvicorn.access')
            for handler in uvicorn_access_logger.handlers:
                handler.setFormatter(sky_logging.FORMATTER)

            # Register controller synchronization task
            asyncio.create_task(self._sync_with_controller())
            # [boltz fork] Register the async-occupancy prober (no-op task
            # when disabled via env).
            asyncio.create_task(self._probe_occupancy_loop())

        uvicorn_tls_kwargs = ({} if self._tls_credential is None else
                              self._tls_credential.dump_uvicorn_kwargs())

        protocol = 'https' if self._tls_credential is not None else 'http'

        logger.info('SkyServe Load Balancer started on '
                    f'{protocol}://0.0.0.0:{self._load_balancer_port}. '
                    f'PID: {os.getpid()}')

        # Drain gracefully on SIGTERM (rolling update): _DrainableServer
        # deregisters + fails readiness immediately, then exits only after a
        # grace period so in-flight requests finish and k8s has pulled us from
        # the Service.
        config = uvicorn.Config(self._app,
                                host='0.0.0.0',
                                port=self._load_balancer_port,
                                **uvicorn_tls_kwargs)
        server = _DrainableServer(config, on_drain=self._begin_draining)
        asyncio.run(server.serve_with_drain())


def run_load_balancer(
    controller_addr: str,
    load_balancer_port: int,
    load_balancing_policy_name: Optional[str] = None,
    tls_credential: Optional[serve_utils.TLSCredential] = None,
    target_qps_per_replica: Optional[Union[float, Dict[str, float]]] = None,
    stream_timeout_seconds: Optional[int] = None,
    retriable_status_codes: Optional[List[int]] = None,
    max_retries: Optional[int] = None,
    retry_initial_backoff_seconds: Optional[float] = None,
) -> None:
    """Run the load balancer.

    The routing spec (policy / target QPS / stream timeout) is fetched from the
    controller over the sync channel; the corresponding args here are only a
    bootstrap seed for the in-pod caller and default to None for the standalone
    launcher (see `SkyServeLoadBalancer.__init__`).

    Args:
        controller_addr: The address of the controller.
        load_balancer_port: The port where the load balancer listens to.
        load_balancing_policy_name: Seed load balancing policy name.
            Defaults to None.
        tls_credential:
            The TLS credentials for HTTPS endpoint. Defaults to None.
        target_qps_per_replica: Seed target QPS per replica for instance-aware
            load balancing. Can be a float or dict mapping GPU types to QPS.
            Defaults to None.
        stream_timeout_seconds: Seed timeout in seconds for proxied responses.
            Defaults to None.
    """
    load_balancer = SkyServeLoadBalancer(
        controller_url=controller_addr,
        load_balancer_port=load_balancer_port,
        load_balancing_policy_name=load_balancing_policy_name,
        tls_credential=tls_credential,
        target_qps_per_replica=target_qps_per_replica,
        stream_timeout_seconds=stream_timeout_seconds,
        retriable_status_codes=retriable_status_codes,
        max_retries=max_retries,
        retry_initial_backoff_seconds=retry_initial_backoff_seconds)
    load_balancer.run()


def _build_argument_parser() -> argparse.ArgumentParser:
    """Build the standalone (external) load balancer CLI parser.

    The routing spec -- load-balancing policy, per-replica target QPS, and
    stream timeout -- is NOT a launch arg: the LB fetches it from the
    controller over the load_balancer_sync channel (see
    `SkyServeLoadBalancer._apply_routing_spec`), so `sky serve update` changes
    reach a running LB without a re-roll. Only the controller address, the
    listen port, and the TLS material stay CLI args: TLS is bound to uvicorn at
    launch and a private key must never stream over the sync channel, so it
    remains a launch/mounted-secret concern.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--controller-addr',
                        required=True,
                        help='The address of the controller.')
    parser.add_argument('--load-balancer-port',
                        type=int,
                        required=True,
                        help='The port where the load balancer listens to.')
    parser.add_argument(
        '--tls-keyfile',
        type=str,
        default=None,
        help='Path to the TLS private key file for the HTTPS endpoint. Must '
        'be given together with --tls-certfile.')
    parser.add_argument(
        '--tls-certfile',
        type=str,
        default=None,
        help='Path to the TLS certificate file for the HTTPS endpoint. Must '
        'be given together with --tls-keyfile.')
    return parser


def _resolve_launch_kwargs(parser: argparse.ArgumentParser,
                           args: argparse.Namespace) -> Dict[str, Any]:
    """Coerce parsed CLI args into `run_load_balancer` kwargs.

    Invalid combinations exit via `parser.error()`. Factored out of __main__
    so the TLS coercion is unit-testable without starting a server. The
    routing spec (policy / target QPS / stream timeout) is sync-fetched, so it
    is not resolved here -- the standalone launcher passes None and the values
    arrive on the first controller sync.
    """
    if (args.tls_keyfile is None) != (args.tls_certfile is None):
        parser.error('--tls-keyfile and --tls-certfile must be given together.')
    tls_credential: Optional[serve_utils.TLSCredential] = None
    if args.tls_keyfile is not None:
        tls_credential = serve_utils.TLSCredential(keyfile=args.tls_keyfile,
                                                   certfile=args.tls_certfile)

    return dict(
        controller_addr=args.controller_addr,
        load_balancer_port=args.load_balancer_port,
        tls_credential=tls_credential,
    )


if __name__ == '__main__':
    _parser = _build_argument_parser()
    run_load_balancer(**_resolve_launch_kwargs(_parser, _parser.parse_args()))
