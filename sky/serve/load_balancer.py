"""LoadBalancer: Distribute any incoming request to all ready replicas."""
import argparse
import asyncio
import collections
import contextlib
import dataclasses
import hashlib
import heapq
import json
import logging
import math
import os
import ssl
import threading
import time
import traceback
from typing import Any, Union
import uuid

import aiohttp
import fastapi
import httpx
from starlette import background
import uvicorn

from sky import sky_logging
from sky.serve import constants
from sky.serve import lb_ha
from sky.serve import lb_ha_observability as lb_ha_obs
from sky.serve import load_balancer_request_metadata
from sky.serve import load_balancing_policies as lb_policies
from sky.serve import replica_tls
from sky.serve import serve_utils
from sky.serve import system_recovery_route_lease
from sky.serve.load_balancer_http import _DrainableServer
from sky.serve.load_balancer_http import _InboundAuthMiddleware
from sky.serve.load_balancer_http import _ReleasingStreamingResponse
from sky.serve.load_balancer_retry import _can_retry_proxy_failure
from sky.serve.load_balancer_retry import _is_dead_connection_error
from sky.serve.load_balancer_retry import _is_definitely_not_dispatched
from sky.serve.load_balancer_retry import _PreDispatchError
from sky.serve.load_balancer_retry import _RetriableStatusError
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)

# Preserve historical private import and pickle identities while the ASGI
# implementations live behind this module's facade.
_DrainableServer.__module__ = __name__
_InboundAuthMiddleware.__module__ = __name__
_ReleasingStreamingResponse.__module__ = __name__
_RetriableStatusError.__module__ = __name__
_PreDispatchError.__module__ = __name__
_is_dead_connection_error.__module__ = __name__
_is_definitely_not_dispatched.__module__ = __name__
_can_retry_proxy_failure.__module__ = __name__

# Per-client in-flight request counter attribute. Attached to the
# httpx.AsyncClient OBJECT (not keyed by URL): a URL pruned and re-added
# gets a fresh client while the old one is still draining, and the two
# must not share a counter.
_INFLIGHT_ATTR = '_sky_inflight_requests'
# Per-client wakeup for the drain task. The counter above remains
# authoritative; this event only avoids polling after the final release.
_INFLIGHT_ZERO_EVENT_ATTR = '_sky_inflight_zero_event'
# Request-local marker for an occupancy-aware queue admission that has not yet
# been assigned to a concrete replica. It closes the short scheduling gap
# between the fleet-wide admission decision and per-replica slot reservation.
_OCCUPANCY_ADMISSION_ATTR = '_sky_occupancy_admission_unassigned'
# Request-local ownership for bodies cached before queue admission. The byte
# reservation is transferred out of the waiting budget once dispatch admission
# succeeds; the active-concurrency budget covers it from that point onward.
_BOUNDED_REQUEST_BODY_ATTR = '_skyserve_bounded_body'
_WAITING_REQUEST_BODY_BYTES_ATTR = '_skyserve_waiting_body_bytes'
_REQUEST_PRIORITY_ATTR = '_skyserve_request_priority'
_REQUEST_ACCELERATORS_ATTR = '_skyserve_compatible_accelerators'
_REQUEST_GRANTED_ACCELERATOR_ATTR = '_skyserve_granted_accelerator'
_REQUEST_DEMAND_RECORDED_ATTR = '_skyserve_request_demand_recorded'
_REQUEST_CLASSIFICATION_ELIGIBLE_ATTR = (
    '_skyserve_request_classification_eligible')
_REQUEST_CLASSIFICATION_RECORDED_ATTR = (
    '_skyserve_request_classification_recorded')
_REQUEST_ACTION_ATTR = '_skyserve_request_action'
# Exact client/marker generation selected by one retry attempt.  Keeping this
# on the request preserves the historical ``_proxy_request_to(url, request)``
# facade used by tests and extensions while still carrying an object-identity
# checkout across the body-buffering awaits inside that method.
_SELECTED_REPLICA_ATTR = '_skyserve_selected_replica'

_ASYNC_ACTION_PREDICT = 'async_predict'
_ASYNC_ACTION_STATUS = 'async_status'
_ASYNC_ACTIONS = frozenset((_ASYNC_ACTION_PREDICT, _ASYNC_ACTION_STATUS,
                            'async_capacity', 'async_cancel'))
_ASYNC_TERMINAL_OUTCOMES = {
    'SUCCEEDED': 'succeeded',
    'FAILED': 'failed',
    'EXPIRED': 'failed',
    'CANCELED': 'failed',
    'CANCELLED': 'failed',
}

# A queued ASGI request does not receive a task cancellation when its client
# disconnects. Poll the receive channel while waiting so an abandoned request
# cannot later consume a replica slot. Request bodies are bounded and cached
# before admission, so this poll cannot consume an unread body message.
_REQUEST_QUEUE_DISCONNECT_POLL_SECONDS = 1.0


@dataclasses.dataclass
class _RequestQueueWaiter:
    """One queued request and its targeted scheduler notification."""

    request: fastapi.Request
    priority: int
    sequence: int
    future: asyncio.Future
    granted: bool = False
    consumed: bool = False
    abandoned: bool = False
    terminal_error: fastapi.HTTPException | None = None


@dataclasses.dataclass(frozen=True)
class _SelectedReplica:
    """Exact route/client generation captured by one retry selection."""

    url: str
    client: httpx.AsyncClient | None
    client_generation: int | None
    route_marker: system_recovery_route_lease.RouteMarker | None
    route_marker_generation: int | None
    require_current_route: bool


class SkyServeLoadBalancer:
    """SkyServeLoadBalancer: distribute incoming traffic with proxy.

    This class accept any traffic to the controller and proxies it
    to the appropriate endpoint replica according to the load balancing
    policy.
    """

    # Process-local state is declared here for readability and initialized in
    # full by __init__.  Tests use the real constructor as well: supporting
    # partially initialized ``object.__new__`` instances would create a second,
    # implicit state contract and hide missing fields behind fallback values.
    _app: fastapi.FastAPI
    _controller_url: str
    _load_balancer_port: int
    _service_hash: str | None
    _lb_slot: lb_ha.LbSlot | None
    _lb_role: lb_ha.LbRole
    _lb_role_generation: int
    _lb_ha_rollout_evidence: dict[str, Any] | None
    _armed_generation: int | None
    _routing_version: int | None
    _background_tasks: set[asyncio.Task]
    _load_balancing_policy_name: str
    _load_balancing_policy: lb_policies.LoadBalancingPolicy
    _request_aggregator: serve_utils.RequestsAggregator
    _request_history_session_id: str
    _completed_async_prediction_ids: collections.OrderedDict[str, None]
    _stream_timeout_seconds: float
    _retriable_status_codes: frozenset[int]
    _max_retries: int
    _retry_initial_backoff_seconds: float
    _queue_depth: int
    _queue_depth_by_priority: dict[int, int]
    _request_queue_config: dict[str, Any] | None
    _request_queue_condition: asyncio.Condition
    _active_request_count: int
    _waiting_request_count: int
    _waiting_request_body_bytes: int
    _request_queue_waiters: dict[int, dict[int, _RequestQueueWaiter]]
    _request_queue_sequence: int
    _draining: bool
    _reject_last_seen: dict[str, tuple[float, int]]
    _reject_compatibility_by_key: dict[str, tuple[int, tuple[str, ...]]]
    _reject_fallback_seq: int
    _offered_arrivals_by_job: dict[str, float]
    _headerless_offered_arrivals: collections.deque[float]
    _offered_arrival_saturated_until: float | None
    _capacity_hint: dict[str, Any] | None
    _configured_accelerators: tuple[str, ...] | None
    _request_accelerator_compatibility_version: int | None
    # Fail open for mixed-version availability: until a controller explicitly
    # acknowledges the replaceable queue gauge, publish the legacy arrival
    # event before admission so an old controller can cold-start an empty
    # fleet. A successful old-controller response also downgrades this flag.
    _queued_compatibility_demand_supported: bool
    _replica_info_by_url: dict[str, dict[str, Any]]
    _draining_clients: dict[str, list[httpx.AsyncClient]]
    _occupancy_capable: set[str]
    # Subset explicitly declared by the per-version service contract. Unlike
    # inferred capability, declaration is fail-closed: any dispatched request
    # may outlive its envelope, including custom request shapes the LB cannot
    # recognize.
    _occupancy_declared_urls: set[str]
    # Explicit disable is two-phase: a service-only update can flip true ->
    # false while old async work still runs on the same replica URL. Retain
    # capability until a generation-valid zero proves that work drained.
    _occupancy_disable_pending: set[str]
    # Authoritative false persists after pending old work drains. Generic
    # probes cannot re-enable it; a recognized async request may temporarily
    # override false until the next generation-valid zero.
    _occupancy_explicitly_disabled_urls: set[str]
    # url -> monotonic time of the FIRST probe round that observed the
    # capable url off-ready and unanswered (cleared whenever the url is
    # confirmed again). Bounds how long such a url survives consecutive
    # probe misses -- and, because it starts at retirement rather than at
    # the last pre-retirement confirmation, guarantees the retention
    # outlives any allowed graceful_drain_seconds deadline.
    _occupancy_off_ready_since: dict[str, float]
    # urls whose occupancy sample in the LAST completed probe round was
    # taken while the url was off-ready. Only such samples can prove
    # post-retirement idleness: a sample taken while the url was still
    # routed may predate work that arrived just before retirement.
    _occupancy_sampled_off_ready: set[str]
    # Per-url ordering between async dispatches and occupancy probes. A sample
    # is valid only when its captured generation still equals the current
    # generation; this prevents a probe begun before a fast-ack submit from
    # publishing a stale zero after that submit lands.
    _occupancy_dispatch_generation: dict[str, int]
    _occupancy_sample_generation: dict[str, int]
    _occupancy_sample_time: dict[str, float]
    # Current-round, generation-valid samples are the only controller-facing
    # idle proof. The longer-lived maps above may retain a last-good sample for
    # bounded LB-local admission, but must never cross this boundary.
    _occupancy_current_round_sampled_urls: set[str]
    # Local admission epoch under which each sample was taken. It advances on
    # every transition into a changed ARMED/ACTIVE role, including role changes
    # that reuse one controller cutover generation.
    _occupancy_role_epoch: int
    _occupancy_sample_role_epoch: dict[str, int]
    _occupancy_probe_lock: asyncio.Lock
    # Optimistic reservations bridge the interval between a fast async
    # acknowledgement and the first post-dispatch occupancy probe. The active
    # attempt count rejects a probe that overtakes an in-progress POST before
    # the trailing generation fence can invalidate it.
    _occupancy_pending_reservations: dict[str, int]
    _occupancy_active_attempts: dict[str, int]
    # Last generation-valid predict concurrency reported per replica URL.
    # Stored separately from free slots because a draining worker may report
    # running work while advertising zero serving capacity.
    _replica_total_slots: dict[str, int]
    # Fleet slot admitted by the request queue but not yet transferred to a
    # selected URL. Normally lives for one event-loop turn; tracking it makes
    # admission correct even if scheduling changes or a test deliberately
    # pauses between admission and selection.
    _occupancy_unassigned_reservations: int
    _ha_runtime_stats: lb_ha_obs.LbHaRuntimeStats
    _drain_history_flush_task: asyncio.Task | None
    _drain_history_flush_generation: int
    _system_recovery_route_markers: dict[
        str, system_recovery_route_lease.RouteMarker]
    _system_recovery_invalid_route_marker_urls: set[str]
    _system_recovery_route_fenced_urls: set[str]
    _system_recovery_route_marker_generations: dict[str, int]
    _system_recovery_route_lease_deadlines: dict[str, float]
    _system_recovery_route_lease_last_applied_sequences: dict[str, int]
    _system_recovery_route_marker_generation: int
    _system_recovery_route_lease_heartbeat_sequence: int
    _system_recovery_route_lease_heartbeat_lock: asyncio.Lock
    _client_pool_generations: dict[str, tuple[httpx.AsyncClient, int]]
    _client_pool_generation: int
    _client_pool: dict[str, httpx.AsyncClient]
    _replica_ssl_context_cached: ssl.SSLContext | bool | None
    _client_pool_lock: threading.Lock
    _replica_dead_failures: dict[str, int]
    _replica_quarantine_until: dict[str, float]
    _ready: bool
    _last_sync_time: float | None
    _replica_occupancy: dict[str, int]
    _replica_free_slots: dict[str, int]
    _last_occupancy_probe_time: float | None
    _client_close_tasks: set[asyncio.Task]

    def __init__(
        self,
        controller_url: str,
        load_balancer_port: int,
        service_hash: str | None = None,
        lb_slot: str | lb_ha.LbSlot | None = None,
    ) -> None:
        """Initialize the load balancer.

        The routing spec is fetched from the controller over the
        load_balancer_sync channel (see `_apply_routing_spec`). The external
        LB starts with safe defaults and stays behind its readiness gate until
        controller sync succeeds.

        Args:
            controller_url: The URL of the controller.
            load_balancer_port: The port where the load balancer listens to.
            service_hash: Durable incarnation of the service this external LB
                may sync for. Standalone test LBs may omit it when their fake
                controller does not enforce incarnation fencing.
        """
        self._app = fastapi.FastAPI()
        self._controller_url: str = controller_url
        self._load_balancer_port: int = load_balancer_port
        self._service_hash = service_hash
        parsed_slot = (lb_slot if isinstance(lb_slot, lb_ha.LbSlot) else
                       lb_ha.parse_slot(lb_slot))
        if lb_slot is not None and parsed_slot is None:
            raise ValueError(f'Invalid load balancer slot: {lb_slot!r}.')
        self._lb_slot = parsed_slot
        self._lb_role = (lb_ha.LbRole.ACTIVE
                         if parsed_slot is None else lb_ha.LbRole.STANDBY)
        self._lb_role_generation = 0
        self._lb_ha_rollout_evidence: dict[str, Any] | None = None
        self._ha_runtime_stats = lb_ha_obs.LbHaRuntimeStats()
        self._armed_generation: int | None = None
        self._routing_version: int | None = None
        # Strong references to owned background tasks (the event loop only
        # holds weak references to tasks).
        self._background_tasks: set[asyncio.Task] = set()
        # Drain-history sends are coalesced behind one task. The generation
        # closes both races: a terminal classification arriving while a send
        # is in flight forces another pass, while one arriving after task
        # completion starts a fresh task.
        self._drain_history_flush_task = None
        self._drain_history_flush_generation = 0
        # Use the registry to create the load balancing policy. Track the
        # resolved policy name so a sync only rebuilds the policy object when
        # the name actually changes (a policy swap is rare -- only on an
        # update that changes the policy).
        self._load_balancing_policy_name: str = (
            lb_policies.LoadBalancingPolicy.make_policy_name(None))
        self._load_balancing_policy = lb_policies.LoadBalancingPolicy.make(
            self._load_balancing_policy_name)

        logger.info('Starting load balancer with policy '
                    f'{self._load_balancing_policy_name}.')
        self._request_aggregator: serve_utils.RequestsAggregator = (
            serve_utils.RequestTimestamp())
        # A Pod UID authorizes controller syncs, but survives container
        # restarts. Request-history counters restart with this process, so use
        # a process incarnation too; otherwise a restarted LB in the same
        # minute could send a lower cumulative count under the old DB key.
        self._request_history_session_id = uuid.uuid4().hex
        self._completed_async_prediction_ids: collections.OrderedDict[
            str, None] = collections.OrderedDict()
        self._stream_timeout_seconds = constants.DEFAULT_LB_STREAM_TIMEOUT
        # Replica responses with these statuses are re-routed like
        # transport failures (empty = never, the default). Safe only for
        # idempotent workloads and "not now" statuses (503/429): the body
        # is discarded before any byte reaches the client.
        self._retriable_status_codes: frozenset[int] = frozenset()
        # Retry-loop tuning (service YAML load_balancer.max_retries /
        # retry_initial_backoff_seconds). With failed-URL exclusion, more
        # retries = more distinct replicas tried before the client sees an
        # error; the backoff prices how fast we fail over.
        self._max_retries = constants.LB_MAX_RETRY
        self._retry_initial_backoff_seconds = (
            constants.LB_RETRY_INITIAL_BACKOFF_SECONDS)
        # Opt-in bounded admission queue. The config arrives over controller
        # sync so service updates take effect without restarting an external
        # LB. Counts are guarded by the asyncio condition on the single
        # uvicorn event loop.
        self._request_queue_config = None
        self._request_queue_condition = asyncio.Condition()
        self._active_request_count = 0
        self._waiting_request_count = 0
        self._waiting_request_body_bytes = 0
        self._request_queue_waiters = {}
        self._request_queue_sequence = 0
        self._reject_last_seen = {}
        self._reject_compatibility_by_key = {}
        self._reject_fallback_seq = 0
        self._configured_accelerators = None
        self._request_accelerator_compatibility_version = None
        self._queued_compatibility_demand_supported = False
        self._replica_info_by_url = {}
        # TODO(tian): httpx.Client has a resource limit of 100 max connections
        # for each client. We should wait for feedback on the best max
        # connections.
        # Reference: https://www.python-httpx.org/advanced/resource-limits/
        #
        # If more than 100 requests are sent to the same replica, the
        # httpx.Client will queue the requests and send them when a
        # connection is available.
        # Reference: https://github.com/encode/httpcore/blob/a8f80980daaca98d556baea1783c5568775daadc/httpcore/_async/connection_pool.py#L69-L71 # pylint: disable=line-too-long
        self._client_pool: dict[str, httpx.AsyncClient] = dict()
        self._draining_clients = {}
        # Built once: an SSLContext is thread-safe, reusable across clients,
        # and parsing the pinned certificate per replica would be wasted work.
        # None means plaintext, which is what `verify=` is ignored for.
        self._replica_ssl_context_cached = self._build_replica_ssl_context()
        # We need this lock to avoid getting from the client pool while
        # updating it from _sync_with_controller.
        self._client_pool_lock: threading.Lock = threading.Lock()
        self._client_pool_generations = {}
        self._client_pool_generation = 0
        self._system_recovery_route_markers = {}
        self._system_recovery_invalid_route_marker_urls = set()
        self._system_recovery_route_fenced_urls = set()
        self._system_recovery_route_marker_generations = {}
        self._system_recovery_route_lease_deadlines = {}
        self._system_recovery_route_lease_last_applied_sequences = {}
        self._system_recovery_route_marker_generation = 0
        self._system_recovery_route_lease_heartbeat_sequence = 0
        self._system_recovery_route_lease_heartbeat_lock = asyncio.Lock()
        # Passive replica eviction state, guarded by _client_pool_lock (the
        # same lock that guards the policy's ready set). _replica_dead_failures
        # counts consecutive dead-connection failures per replica;
        # _replica_quarantine_until maps a replica URL to the wall-clock time
        # until which it stays out of routing.
        self._replica_dead_failures: dict[str, int] = dict()
        self._replica_quarantine_until: dict[str, float] = dict()
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
        self._last_sync_time: float | None = None
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
        self._queue_depth_by_priority: dict[int, int] = {}
        # Reject window with dedup: job key -> last-seen monotonic time.
        # Keyed by the LB_JOB_ID_HEADER value so repeated attempts for one
        # logical job refresh its TTL while still counting as one unit of
        # pressure. Entries older than LB_REJECT_WINDOW_SECONDS are pruned on
        # access. Monotonic clock: TTLs must not be distorted by
        # wall-clock steps (NTP). (Typed Optional at class level; always
        # a real dict on instances -- _prune_reject_window materializes.)
        self._reject_last_seen = {}
        # Fallback key sequence for requests without the job-id header:
        # each such reject counts once (raw-count over-estimation,
        # documented -- the platform sends the header).
        self._reject_fallback_seq: int = 0
        self._offered_arrivals_by_job: dict[str, float] = {}
        self._headerless_offered_arrivals: collections.deque[float] = (
            collections.deque())
        self._offered_arrival_saturated_until: float | None = None
        # Latest capacity_hint from the controller sync response
        # (provisioning/target replica counts). None until a sync carries
        # one (old controller, or never synced); /_lb/capacity readers
        # judge its freshness via last_sync_age_seconds.
        self._capacity_hint: dict[str, Any] | None = None
        # [boltz fork] Replica-reported async occupancy, from the probe loop
        # (see _probe_occupancy_loop): url -> running async jobs, total predict
        # slots, and free predict slots (max(0, total - running)).
        # Rebuilt wholesale each probe round from the then-ready set, so a
        # pruned replica ages out on the next round. Absent url == probe
        # failed/never ran == occupancy unknown (never assumed busy). Guarded
        # by _client_pool_lock like the rest of the routing state.
        self._replica_occupancy: dict[str, int] = {}
        self._replica_total_slots: dict[str, int] = {}
        self._replica_free_slots: dict[str, int] = {}
        self._occupancy_capable = set()
        self._occupancy_declared_urls = set()
        self._occupancy_disable_pending = set()
        self._occupancy_explicitly_disabled_urls = set()
        self._occupancy_off_ready_since = {}
        self._occupancy_sampled_off_ready = set()
        self._occupancy_dispatch_generation = {}
        self._occupancy_sample_generation = {}
        self._occupancy_sample_time = {}
        self._occupancy_current_round_sampled_urls = set()
        self._occupancy_role_epoch = 0
        self._occupancy_sample_role_epoch = {}
        self._occupancy_probe_lock = asyncio.Lock()
        self._occupancy_pending_reservations = {}
        self._occupancy_active_attempts = {}
        self._occupancy_unassigned_reservations = 0
        # Monotonic time of the last COMPLETED probe round (same clock
        # rationale as _last_sync_time: staleness must not hide behind
        # wall-clock steps).
        self._last_occupancy_probe_time: float | None = None
        # Strong refs to in-progress drain-close tasks (see
        # _drain_and_close_client); a bare create_task result can be GCed.
        self._client_close_tasks: set[asyncio.Task] = set()

    def _ha_stats(self) -> lb_ha_obs.LbHaRuntimeStats:
        """Return the process-local HA stats initialized at construction."""
        return self._ha_runtime_stats

    def _replace_system_recovery_route_markers_locked(
            self, replica_info: dict[str, dict[str, Any]]) -> set[str]:
        """Install one coherent heavyweight-sync marker snapshot.

        Must be called while holding ``_client_pool_lock``.  A URL with any
        marker field but without one complete valid v1 marker is retained in
        the invalid set and can never fall through to ordinary routing.
        Deadlines and response-order fences survive only an exact unchanged
        URL/marker generation.
        """
        old_markers = dict(self._system_recovery_route_markers)
        old_generations = dict(self._system_recovery_route_marker_generations)
        old_deadlines = dict(self._system_recovery_route_lease_deadlines)
        old_sequences = dict(
            self._system_recovery_route_lease_last_applied_sequences)
        markers: dict[str, system_recovery_route_lease.RouteMarker] = {}
        invalid_urls: set[str] = set()
        fenced_urls = set(self._system_recovery_route_fenced_urls)
        for url, info in replica_info.items():
            fields_present, marker = (
                system_recovery_route_lease.parse_route_marker(info))
            if not fields_present:
                # A coherent current unmarked projection is ordinary again.
                fenced_urls.discard(url)
                continue
            fenced_urls.add(url)
            if marker is None:
                invalid_urls.add(url)
                continue
            markers[url] = marker
        # A removed capable URL may remain in the off-ready occupancy/draining
        # overlay.  Retain only those bounded live references so that overlay
        # cannot probe a same-address replacement after marker removal.
        relevant_urls = (set(replica_info) | set(self._client_pool) |
                         set(self._draining_clients) |
                         set(self._occupancy_capable))
        fenced_urls &= relevant_urls

        marker_generation = self._system_recovery_route_marker_generation
        generations: dict[str, int] = {}
        deadlines: dict[str, float] = {}
        sequences: dict[str, int] = {}
        for url, marker in markers.items():
            if old_markers.get(url) == marker and url in old_generations:
                generation = old_generations[url]
                if url in old_deadlines:
                    deadlines[url] = old_deadlines[url]
                if url in old_sequences:
                    sequences[url] = old_sequences[url]
            else:
                marker_generation += 1
                generation = marker_generation
            generations[url] = generation

        self._system_recovery_route_markers = markers
        self._system_recovery_invalid_route_marker_urls = invalid_urls
        self._system_recovery_route_fenced_urls = fenced_urls
        self._system_recovery_route_marker_generations = generations
        self._system_recovery_route_lease_deadlines = deadlines
        self._system_recovery_route_lease_last_applied_sequences = sequences
        self._system_recovery_route_marker_generation = marker_generation
        return invalid_urls

    def _system_recovery_route_is_available_locked(self,
                                                   url: str,
                                                   *,
                                                   now: float | None = None
                                                  ) -> bool:
        """Whether the current exact URL marker permits data-plane use."""
        if url in self._system_recovery_invalid_route_marker_urls:
            return False
        markers = self._system_recovery_route_markers
        if (url in self._system_recovery_route_fenced_urls and
                url not in markers):
            return False
        if url not in markers:
            return True
        deadlines = self._system_recovery_route_lease_deadlines
        deadline = deadlines.get(url)
        if (not isinstance(deadline, (int, float)) or
                isinstance(deadline, bool) or not math.isfinite(deadline)):
            return False
        if now is None:
            now = time.monotonic()
        if deadline <= now:
            # Preserve the per-generation applied sequence: an older delayed
            # positive response must not reinstall this expired lease.
            deadlines.pop(url, None)
            self._system_recovery_route_lease_deadlines = deadlines
            return False
        return True

    def _routable_ready_urls_locked(self) -> set[str]:
        """Ready URLs that also pass the narrow recovery-route fence."""
        return {
            url for url in self._load_balancing_policy.ready_replicas
            if self._system_recovery_route_is_available_locked(url)
        }

    def _client_generation_locked(self, url: str,
                                  client: httpx.AsyncClient) -> int:
        """Return the object-identity generation for one pooled client."""
        generations = self._client_pool_generations
        existing = generations.get(url)
        if existing is not None and existing[0] is client:
            return existing[1]
        generation = self._client_pool_generation + 1
        generations[url] = (client, generation)
        self._client_pool_generations = generations
        self._client_pool_generation = generation
        return generation

    def _capture_selected_replica_locked(
            self, url: str, *, require_current_route: bool) -> _SelectedReplica:
        """Capture the exact client and marker generation chosen under lock."""
        client = self._client_pool.get(url)
        client_generation = (None if client is None else
                             self._client_generation_locked(url, client))
        marker = self._system_recovery_route_markers.get(url)
        marker_generation = self._system_recovery_route_marker_generations.get(
            url)
        return _SelectedReplica(url=url,
                                client=client,
                                client_generation=client_generation,
                                route_marker=marker,
                                route_marker_generation=marker_generation,
                                require_current_route=require_current_route)

    def _checkout_selected_replica_locked(
            self, selected: _SelectedReplica) -> httpx.AsyncClient | None:
        """Atomically revalidate and reference the exact selected client.

        This is the final pre-transport fence after all request-body awaits.
        It never substitutes a newly looked-up client for the selected object.
        """
        if self._draining or not self._accepts_new_requests():
            return None
        if (selected.require_current_route and
                selected.url not in self._load_balancing_policy.ready_replicas):
            return None
        if not self._system_recovery_route_is_available_locked(selected.url):
            return None
        markers = self._system_recovery_route_markers
        generations = self._system_recovery_route_marker_generations
        if (markers.get(selected.url) != selected.route_marker or
                generations.get(
                    selected.url) != selected.route_marker_generation):
            return None
        client = selected.client
        if client is None:
            return None
        current_client = self._client_pool.get(selected.url)
        if current_client is not client:
            return None
        current_generation = self._client_generation_locked(
            selected.url, current_client)
        if current_generation != selected.client_generation:
            return None
        inflight = getattr(client, _INFLIGHT_ATTR, 0)
        if type(inflight) is not int:
            # Tolerate partially initialized clients during rolling upgrades
            # and lightweight test doubles.
            inflight = 0
        setattr(client, _INFLIGHT_ATTR, inflight + 1)
        return client

    def _begin_system_recovery_route_lease_heartbeat_locked(
        self,
    ) -> tuple[int, float, dict[str, tuple[
            system_recovery_route_lease.RouteMarker, int]]]:
        """Capture one ordered heartbeat request against the current snapshot."""
        sequence = self._system_recovery_route_lease_heartbeat_sequence + 1
        self._system_recovery_route_lease_heartbeat_sequence = sequence
        started_at = time.monotonic()
        markers = self._system_recovery_route_markers
        generations = self._system_recovery_route_marker_generations
        snapshot = {
            url: (marker, generations[url])
            for url, marker in markers.items()
            if url in generations
        }
        return sequence, started_at, snapshot

    def _apply_system_recovery_route_lease_heartbeat(
        self,
        payload: object,
        *,
        sequence: int,
        request_started_at: float,
        marker_snapshot: dict[str,
                              tuple[system_recovery_route_lease.RouteMarker,
                                    int]],
    ) -> bool:
        """Atomically apply one newer well-formed heartbeat response."""
        leases = system_recovery_route_lease.validate_heartbeat_payload(payload)
        if (not isinstance(request_started_at, (int, float)) or
                isinstance(request_started_at, bool) or
                not math.isfinite(request_started_at)):
            raise system_recovery_route_lease.RouteLeaseError(
                'heartbeat request start must be finite')
        now = time.monotonic()
        changed = False
        with self._client_pool_lock:
            current_markers = self._system_recovery_route_markers
            current_generations = (
                self._system_recovery_route_marker_generations)
            deadlines = dict(self._system_recovery_route_lease_deadlines)
            applied_sequences = dict(
                self._system_recovery_route_lease_last_applied_sequences)
            for url, (marker, generation) in marker_snapshot.items():
                if (current_markers.get(url) != marker or
                        current_generations.get(url) != generation or
                        sequence <= applied_sequences.get(url, -1)):
                    continue
                applied_sequences[url] = sequence
                remaining = leases.get(marker)
                proposed_deadline = (None if remaining is None else
                                     float(request_started_at) + remaining)
                if proposed_deadline is None or proposed_deadline <= now:
                    if url in deadlines:
                        deadlines.pop(url, None)
                        changed = True
                    continue
                if deadlines.get(url) != proposed_deadline:
                    deadlines[url] = proposed_deadline
                    changed = True
            self._system_recovery_route_lease_deadlines = deadlines
            self._system_recovery_route_lease_last_applied_sequences = (
                applied_sequences)
        return changed

    def _quarantined_replicas(self) -> set[str]:
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
        # Route the quarantined url's client through the same drain path as
        # controller-pruned urls: set_ready_replicas above dropped its
        # load_map entry, so without the draining overlay its still-running
        # requests would vanish from the demand feed -- reading as idle to
        # the autoscaler and, worse, as 'drained' to a retirement waiting on
        # the in-flight gauge. A post-quarantine re-add creates a fresh
        # client; the old one closes once its in-flight work finishes.
        client = self._client_pool.pop(url, None)
        if client is not None:
            self._client_pool_generations.pop(url, None)
            self._draining_clients.setdefault(url, []).append(client)
            task = asyncio.create_task(self._drain_and_close_client(
                url, client))
            self._client_close_tasks.add(task)
            task.add_done_callback(self._client_close_tasks.discard)
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

    def _apply_routing_spec(self, routing_spec: dict[str, Any]) -> None:
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
        request_queue = routing_spec.get('request_queue')
        self._request_queue_config = (dict(request_queue) if isinstance(
            request_queue, dict) else None)
        previous_configured_accelerators = self._configured_accelerators
        compatibility_version = routing_spec.get(
            'request_accelerator_compatibility_version')
        configured_accelerators = routing_spec.get('configured_accelerators')
        if (compatibility_version == constants.LB_REQUEST_ACCELERATORS_VERSION
                and isinstance(configured_accelerators, list) and
                0 < len(configured_accelerators) <=
                constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS and all(
                    isinstance(item, str) and item
                    for item in configured_accelerators) and
                len({item.casefold() for item in configured_accelerators
                    }) == len(configured_accelerators)):
            self._configured_accelerators = tuple(configured_accelerators)
            self._request_accelerator_compatibility_version = (
                compatibility_version)
        else:
            # Never retain a stale catalog across a malformed or downgraded
            # routing spec. Explicit request constraints must fail closed.
            self._configured_accelerators = None
            self._request_accelerator_compatibility_version = None
        self._reconcile_queued_request_accelerators(
            previous_configured_accelerators, self._configured_accelerators)

    def _reconcile_queued_request_accelerators(
        self,
        previous: tuple[str, ...] | None,
        current: tuple[str, ...] | None,
    ) -> None:
        """Re-index queued requests against a changed exact-card catalog.

        Controller sync and request admission run on the same asyncio event
        loop.  This method contains no await, so mutating the waiter registry
        here is atomic with respect to queue admission even though routing
        specs are applied under the separate client-pool lock.
        """
        if previous == current:
            return
        waiters = self._request_queue_waiters_for_instance()
        current_by_name = ({
            card.casefold(): card for card in current
        } if current is not None else {})
        for bucket in list(waiters.values()):
            for waiter in list(bucket.values()):
                requested = getattr(waiter.request, _REQUEST_ACCELERATORS_ATTR,
                                    None)
                if requested is None:
                    requested = previous
                surviving = tuple(current_by_name[card.casefold()]
                                  for card in (requested or ())
                                  if card.casefold() in current_by_name)
                if surviving:
                    setattr(waiter.request, _REQUEST_ACCELERATORS_ATTR,
                            surviving)
                    continue
                self._remove_request_queue_waiter_locked(waiter)
                waiter.terminal_error = self._accelerator_header_error(
                    'has no exact card still configured after a service '
                    'update; retry against the active version.',
                    status_code=503)
                self._resolve_request_queue_waiter_locked(waiter)

    def _queue_uses_async_occupancy(self) -> bool:
        config = self._request_queue_config
        return bool(config is not None and
                    config.get('use_async_occupancy', False))

    def _occupancy_sample_is_locally_usable_locked(self, url: str) -> bool:
        """Whether one sample is valid for bounded LB-local admission."""
        if not self._system_recovery_route_is_available_locked(url):
            return False
        if (url not in self._replica_occupancy or
                url not in self._replica_total_slots or
                url not in self._replica_free_slots):
            return False
        sample_generation = self._occupancy_sample_generation.get(url)
        if sample_generation is None:
            return False
        if (sample_generation != self._occupancy_dispatch_generation.get(
                url, 0) and
                self._occupancy_pending_reservations.get(url, 0) <= 0):
            return False

        sampled_at = self._occupancy_sample_time.get(url)
        if (sampled_at is None or time.monotonic() - sampled_at
                > constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS):
            return False

        if (self._occupancy_sample_role_epoch.get(url)
                != self._occupancy_role_epoch):
            return False
        return True

    def _locally_usable_occupancy_urls_locked(self) -> set[str]:
        """Return generation-, role-, and age-valid local sample URLs.

        A transient probe miss may retain the previous sample for bounded
        admission continuity. This view is deliberately broader than the
        current-round proof exported to the controller, but it never survives
        a dispatch fence, HA role generation change, or the per-URL TTL.
        """
        return {
            url for url in self._replica_free_slots
            if self._occupancy_sample_is_locally_usable_locked(url)
        }

    def _controller_occupancy_proof_urls_locked(self) -> set[str]:
        """Return only current-round samples safe as controller idle proof."""
        proof_urls = self._occupancy_current_round_sampled_urls
        proof_urls = {
            url for url in proof_urls if url in self._replica_occupancy and
            self._occupancy_sample_generation.get(
                url) == self._occupancy_dispatch_generation.get(url, 0)
        }
        proof_urls = {
            url for url in proof_urls if self._occupancy_sample_role_epoch.get(
                url) == self._occupancy_role_epoch
        }
        return proof_urls

    def _invalidate_occupancy_samples_locked(self) -> None:
        """Invalidate pre-role-transition samples without losing real work."""
        self._replica_occupancy = {}
        self._replica_total_slots = {}
        self._replica_free_slots = {}
        self._occupancy_sample_generation = {}
        self._occupancy_sample_time = {}
        self._occupancy_current_round_sampled_urls = set()
        self._occupancy_sample_role_epoch = {}
        self._occupancy_sampled_off_ready = set()
        self._load_balancing_policy.set_occupancy({})

    def _effective_replica_free_slots_locked(self) -> dict[str, int]:
        """Return last observed free slots minus post-sample reservations.

        Must be called while holding `_client_pool_lock`. A raced probe never
        overwrites the last usable baseline; every dispatch since that
        baseline has a reservation, so subtracting reservations preserves the
        still-usable slots without treating stale capacity as fresh.
        """
        usable_urls = self._locally_usable_occupancy_urls_locked()
        return {
            url:
                max(0, slots - self._occupancy_pending_reservations.get(url, 0))
            for url, slots in self._replica_free_slots.items()
            if url in usable_urls
        }

    def _effective_free_slots_for_replica_locked(self, url: str) -> int | None:
        """Return one URL's free slots without scanning the whole fleet."""
        if not self._occupancy_sample_is_locally_usable_locked(url):
            return None
        slots = self._replica_free_slots.get(url)
        if slots is None:
            return None
        pending = self._occupancy_pending_reservations.get(url, 0)
        return max(0, slots - pending)

    def _effective_replica_occupancy_locked(self, url: str) -> int | None:
        """Return routing occupancy including optimistic reservations."""
        pending = self._occupancy_pending_reservations.get(url, 0)
        observed = self._replica_occupancy.get(url)
        dispatch_generation = self._occupancy_dispatch_generation.get(url, 0)
        sample_generation = self._occupancy_sample_generation.get(url)
        locally_usable = self._occupancy_sample_is_locally_usable_locked(url)
        if pending > 0:
            return (observed or 0) + pending
        if (locally_usable and observed is not None and
                sample_generation == dispatch_generation):
            return observed
        return None

    def _effective_occupancy_locked(self) -> dict[str, int]:
        urls = (set(self._replica_occupancy) |
                set(self._occupancy_pending_reservations))
        effective: dict[str, int] = {}
        for url in urls:
            value = self._effective_replica_occupancy_locked(url)
            if value is not None:
                effective[url] = value
        return effective

    def _publish_replica_occupancy_locked(self, url: str) -> None:
        """Publish one reservation-adjusted value to the routing policy."""
        self._load_balancing_policy.set_occupancy_for_replica(
            url, self._effective_replica_occupancy_locked(url))

    @staticmethod
    def _has_unassigned_occupancy_admission(request: fastapi.Request) -> bool:
        # MagicMock manufactures truthy attributes on access, so use identity
        # rather than truthiness for the partially mocked requests in tests.
        return getattr(request, _OCCUPANCY_ADMISSION_ATTR, False) is True

    def _record_unassigned_occupancy_admission(
            self, request: fastapi.Request) -> None:
        """Reserve one fleet slot before a concrete replica is selected."""
        with self._client_pool_lock:
            self._record_unassigned_occupancy_admission_locked(request)

    def _record_unassigned_occupancy_admission_locked(
            self, request: fastapi.Request) -> None:
        """Reserve one fleet slot while already holding the pool lock."""
        if self._has_unassigned_occupancy_admission(request):
            return
        setattr(request, _OCCUPANCY_ADMISSION_ATTR, True)
        self._occupancy_unassigned_reservations += 1

    def _release_unassigned_occupancy_admission_locked(
            self, request: fastapi.Request) -> bool:
        """Release a fleet reservation that never reached replica selection."""
        if not self._has_unassigned_occupancy_admission(request):
            return False
        setattr(request, _OCCUPANCY_ADMISSION_ATTR, False)
        self._occupancy_unassigned_reservations = max(
            0, self._occupancy_unassigned_reservations - 1)
        return True

    def _request_queue_limits(self) -> tuple[int, int]:
        """Return (dispatch concurrency, queue size) for the ready fleet."""
        config = self._request_queue_config
        assert config is not None
        with self._client_pool_lock:
            ready_urls = self._routable_ready_urls_locked()
            ready_replicas = len(ready_urls)
            free_slots = None
            queue_capacity_units = ready_replicas
            dispatch_capacity = (ready_replicas *
                                 config['max_concurrency_per_replica'])
            if config.get('use_async_occupancy', False):
                # Unknown occupancy is not free capacity. A selected async
                # attempt consumes one replica reservation; admissions that
                # have not reached selection consume the fleet-wide remainder.
                effective = self._effective_replica_free_slots_locked()
                total_slots = self._replica_total_slots
                hint = self._capacity_hint or {}
                logical_replicas = hint.get('replica_unit') == 'logical_slot'
                planned_capacity = hint.get('planned_capacity_by_url', {})
                # The configured per-replica value remains a hard safety cap,
                # but a heterogeneous replica contributes its actual probed
                # slots instead of one replica-count unit. Logical mode uses
                # controller-pinned width for stable queue sizing, while the
                # effective free-slot sum below remains observation-gated.
                capacity_by_url = (planned_capacity
                                   if logical_replicas else total_slots)
                queue_capacity_units = sum(
                    min(capacity_by_url.get(url, 0),
                        config['max_concurrency_per_replica'])
                    for url in ready_urls)
                dispatch_capacity = queue_capacity_units
                free_slots = max(
                    0,
                    sum(slots for url, slots in effective.items()
                        if url in ready_urls) -
                    self._occupancy_unassigned_reservations)
        dispatch_limit = min(config['max_concurrency'], dispatch_capacity)
        if free_slots is not None:
            # `_active_request_count` already includes admitted requests whose
            # per-replica or fleet reservation was subtracted above. Add the
            # remaining free slots to the current load to form the dynamic
            # concurrency ceiling without double-debiting active dispatches.
            dispatch_limit = min(dispatch_limit,
                                 self._active_request_count + free_slots)
        queue_size = max(config['min_size'],
                         queue_capacity_units * config['size_per_replica'])
        return dispatch_limit, min(config['max_size'], queue_size)

    def _request_queue_submission_limit(self) -> int:
        """Return the capacity-insensitive controller HTTP concurrency.

        Backend dispatch remains limited by currently usable capacity.  Queue
        submission cannot be: a cold scale-to-zero service needs to accept its
        configured backlog before the first backend exists.  Active requests
        are outside the waiting-depth count, so the useful upper bound includes
        both the maximum waiting backlog and maximum active dispatches.
        """
        config = self._request_queue_config
        assert config is not None
        return config['max_size'] + config['max_concurrency']

    @staticmethod
    def _request_queue_timeout(config: dict[str, Any], priority: int) -> float:
        timeout = float(config['timeout_seconds'])
        for threshold in config.get('timeout_seconds_by_priority', ()):
            if priority < threshold['min_priority']:
                break
            timeout = float(threshold['timeout_seconds'])
        return timeout

    def _change_queue_depth(self, priority: int, delta: int) -> None:
        self._queue_depth = max(0, self._queue_depth + delta)
        depths = self._queue_depth_by_priority
        next_count = depths.get(priority, 0) + delta
        if next_count <= 0:
            depths.pop(priority, None)
        else:
            depths[priority] = next_count

    def _queue_depth_priority_snapshot(self) -> dict[str, int]:
        return {
            str(priority): count
            for priority, count in sorted(self._queue_depth_by_priority.items())
            if count > 0
        }

    def _current_dispatch_load(self) -> int:
        """Conservative live request count, including returned streams."""
        # A streaming response transfers its admission release to its ASGI
        # lifetime below, so this count includes selecting handlers, upstream
        # awaits, and returned streams exactly once.
        return self._active_request_count

    # Preserve the historical private facade without adding wrapper frames.
    # pylint: disable=protected-access
    _priority_header_error = staticmethod(
        load_balancer_request_metadata._priority_header_error)
    _parse_request_priority: Any = classmethod(
        load_balancer_request_metadata._parse_request_priority)
    _accelerator_header_error = staticmethod(
        load_balancer_request_metadata._accelerator_header_error)
    _parse_request_accelerators = (
        load_balancer_request_metadata._parse_request_accelerators)
    _headers_without_request_priority = staticmethod(
        load_balancer_request_metadata._headers_without_request_priority)

    # pylint: enable=protected-access

    def _request_queue_waiters_for_instance(
            self) -> dict[int, dict[int, _RequestQueueWaiter]]:
        return self._request_queue_waiters

    def _remove_request_queue_waiter_locked(
            self, waiter: _RequestQueueWaiter) -> bool:
        waiters = self._request_queue_waiters_for_instance()
        bucket = waiters.get(waiter.priority)
        if bucket is None or bucket.pop(waiter.sequence, None) is None:
            return False
        if not bucket:
            del waiters[waiter.priority]
        self._waiting_request_count = max(0, self._waiting_request_count - 1)
        return True

    def _pop_request_queue_waiter_locked(
        self,
        accelerator_slots: dict[str, int] | None = None,
        zero_cost_slots: dict[str, int] | None = None,
    ) -> _RequestQueueWaiter | None:
        waiters = self._request_queue_waiters_for_instance()
        while waiters:
            selected: _RequestQueueWaiter | None = None
            selected_accelerator: str | None = None
            for priority in sorted(waiters, reverse=True):
                bucket = waiters[priority]
                if accelerator_slots is None:
                    selected = bucket[next(iter(bucket))]
                    break
                candidates: list[tuple[int, int, _RequestQueueWaiter,
                                       list[str]]] = []
                for waiter in bucket.values():
                    if waiter.abandoned:
                        candidates.append((0, waiter.sequence, waiter, []))
                        continue
                    compatible = getattr(waiter.request,
                                         _REQUEST_ACCELERATORS_ATTR, None)
                    ordered_cards = (compatible if compatible is not None else
                                     tuple(accelerator_slots))
                    available_cards = [
                        card for card in ordered_cards
                        if accelerator_slots.get(card, 0) > 0
                    ]
                    if available_cards:
                        candidates.append(
                            (len(available_cards), waiter.sequence, waiter,
                             available_cards))
                if not candidates:
                    continue
                # Most constrained request first within equal numeric
                # priority; FIFO remains the tie-break. Abandoned entries are
                # removed eagerly and never consume capacity.
                _, _, selected, available_cards = min(candidates,
                                                      key=lambda item: item[:2])
                if available_cards:
                    reserved_cards = [
                        card for card in available_cards
                        if (zero_cost_slots or {}).get(card, 0) > 0
                    ]
                    selected_accelerator = (reserved_cards[0] if reserved_cards
                                            else available_cards[0])
                break
            if selected is None:
                return None
            waiter = selected
            self._remove_request_queue_waiter_locked(waiter)
            if waiter.abandoned:
                if not waiter.future.done():
                    waiter.future.set_result(None)
                continue
            if selected_accelerator is not None:
                assert accelerator_slots is not None
                setattr(waiter.request, _REQUEST_GRANTED_ACCELERATOR_ATTR,
                        selected_accelerator)
                accelerator_slots[selected_accelerator] -= 1
                if (zero_cost_slots is not None and
                        zero_cost_slots.get(selected_accelerator, 0) > 0):
                    zero_cost_slots[selected_accelerator] -= 1
            return waiter
        return None

    def _request_queue_fallback_rank_locked(
        self,
        compatible: tuple[str, ...],
        accelerator_slots: dict[str, int],
    ) -> int:
        """Rank the best non-ready fallback; larger means more urgent."""
        hint = self._capacity_hint or {}
        provisioning = hint.get('provisioning_replicas_by_accelerator', {})
        free_reserved = hint.get('free_reserved_slots_by_accelerator', {})
        configured = self._configured_accelerators or compatible
        cost_order = {card: index for index, card in enumerate(configured)}
        ranks: list[int] = []
        for card in compatible:
            if accelerator_slots.get(card, 0) > 0:
                continue
            if int(provisioning.get(card, 0) or 0) > 0:
                ranks.append(10 + cost_order.get(card, len(cost_order)))
            elif int(free_reserved.get(card, 0) or 0) > 0:
                ranks.append(20 + cost_order.get(card, len(cost_order)))
            elif card in cost_order:
                # A later configured card is a more expensive cold fallback.
                ranks.append(30 + cost_order[card])
        # No alternative is worse than every realizable fallback.
        return min(ranks) if ranks else 1000

    # Loop-local closures execute synchronously and never escape their tier.
    # pylint: disable=cell-var-from-loop
    def _build_request_queue_grant_plan_locked(
        self,
        accelerator_slots: dict[str, int],
        zero_cost_slots: dict[str, int],
        max_grants: int,
    ) -> list[tuple[_RequestQueueWaiter, str]]:
        """Build a maximum-cardinality strict-priority matching plan.

        The loop-local closures execute synchronously and never escape their
        tier iteration. Each priority tier is grouped by its bounded exact-card
        compatibility profile and matched against the remaining slots. Within
        a tier, profiles with fewer actual ready slots and worse non-ready
        fallback are processed first; FIFO merges profiles that truly tie. The
        augmenting-path matcher can move an earlier request to another
        compatible card, but never drops it to admit a later peer.
        """
        if max_grants <= 0:
            return []
        waiters = self._request_queue_waiters_for_instance()
        remaining = dict(accelerator_slots)
        plan: list[tuple[_RequestQueueWaiter, str]] = []
        configured = self._configured_accelerators or tuple(remaining)
        card_order = {card: index for index, card in enumerate(configured)}

        for priority in sorted(waiters, reverse=True):
            tier = [
                waiter for waiter in waiters[priority].values()
                if not waiter.abandoned
            ]
            tier_grant_limit = min(max_grants - len(plan),
                                   sum(remaining.values()))
            if tier_grant_limit <= 0:
                break

            def compatible_cards(
                    waiter: _RequestQueueWaiter) -> tuple[str, ...]:
                compatible = getattr(waiter.request, _REQUEST_ACCELERATORS_ATTR,
                                     None)
                if compatible is None:
                    return tuple(configured)
                allowed = set(compatible)
                return tuple(card for card in configured if card in allowed)

            # All waiters in a profile have identical matching edges. Keep
            # FIFO only within the bounded profile set and stop retrying a
            # profile after one waiter cannot augment the current matching.
            # This avoids traversing an arbitrarily large backlog after every
            # ready slot is already assigned.
            profile_waiters: dict[tuple[str, ...],
                                  list[_RequestQueueWaiter]] = {}
            for waiter in tier:
                profile_waiters.setdefault(compatible_cards(waiter),
                                           []).append(waiter)
            profile_heap: list[tuple[int, int, int, tuple[str, ...], int]] = []
            for compatible, queued in profile_waiters.items():
                first = queued[0]
                heapq.heappush(
                    profile_heap,
                    (sum(remaining.get(card, 0) for card in compatible),
                     -self._request_queue_fallback_rank_locked(
                         compatible, remaining), first.sequence, compatible, 0))
            assignments: dict[int, str] = {}
            assigned_by_card: dict[str, list[_RequestQueueWaiter]] = {
                card: [] for card in remaining
            }

            def card_preferences(
                waiter: _RequestQueueWaiter,
                assigned_by_card: dict[str, list[_RequestQueueWaiter]],
            ) -> list[str]:
                cards = [
                    card for card in compatible_cards(waiter)
                    if remaining.get(card, 0) > 0
                ]
                return sorted(cards,
                              key=lambda card: (
                                  0 if len(assigned_by_card[card]) <
                                  zero_cost_slots.get(card, 0) else 1,
                                  card_order.get(card, len(card_order)),
                              ))

            def assign(
                waiter: _RequestQueueWaiter,
                seen_cards: set[str],
                seen_waiters: set[int],
                assigned_by_card: dict[str, list[_RequestQueueWaiter]],
                assignments: dict[int, str],
            ) -> bool:
                if waiter.sequence in seen_waiters:
                    return False
                seen_waiters.add(waiter.sequence)
                for card in card_preferences(waiter, assigned_by_card):
                    if card in seen_cards:
                        continue
                    seen_cards.add(card)
                    occupants = assigned_by_card[card]
                    if len(occupants) < remaining[card]:
                        occupants.append(waiter)
                        assignments[waiter.sequence] = card
                        return True
                    # Move an already-admitted peer to another compatible
                    # card to preserve maximum immediate admissions.
                    for occupant in list(reversed(occupants)):
                        if assign(occupant, seen_cards, seen_waiters,
                                  assigned_by_card, assignments):
                            occupants.remove(occupant)
                            occupants.append(waiter)
                            assignments[waiter.sequence] = card
                            return True
                return False

            accepted: list[_RequestQueueWaiter] = []
            while profile_heap and len(accepted) < tier_grant_limit:
                _, fallback_rank, _, compatible, index = heapq.heappop(
                    profile_heap)
                queued = profile_waiters[compatible]
                waiter = queued[index]
                if not assign(waiter, set(), set(), assigned_by_card,
                              assignments):
                    # A later waiter from this exact profile has the same
                    # edges and cannot augment an unchanged matching either.
                    continue
                accepted.append(waiter)
                next_index = index + 1
                if next_index < len(queued):
                    heapq.heappush(
                        profile_heap,
                        (sum(remaining.get(card, 0) for card in compatible),
                         fallback_rank, queued[next_index].sequence, compatible,
                         next_index))
            for waiter in accepted:
                card = assignments[waiter.sequence]
                plan.append((waiter, card))
                remaining[card] -= 1
                if zero_cost_slots.get(card, 0) > 0:
                    zero_cost_slots[card] -= 1
        return plan

    # pylint: enable=cell-var-from-loop

    def _request_queue_accelerator_slots_locked(
            self) -> tuple[dict[str, int], dict[str, int]] | None:
        """Return currently dispatchable slots by exact card and cost tier."""
        configured = self._configured_accelerators
        if configured is None:
            return None
        # Once the controller advertises the exact-card capability, missing
        # identity is zero compatible capacity, never permission to fall back
        # to aggregate admission. This fails closed during partial syncs.
        replica_info = self._replica_info_by_url
        with self._client_pool_lock:
            ready_urls = self._routable_ready_urls_locked()
            if self._queue_uses_async_occupancy():
                free_by_url = self._effective_replica_free_slots_locked()
            else:
                in_flight = self._load_balancing_policy.snapshot_in_flight()
                config = self._request_queue_config
                assert config is not None
                per_replica_limit = max(
                    1, int(config.get('max_concurrency_per_replica', 1)))
                free_by_url = {
                    url: max(0, per_replica_limit -
                             (in_flight or {}).get(url, 0)) for url in ready_urls
                }
            slots = {accelerator: 0 for accelerator in configured}
            zero_cost_slots = {accelerator: 0 for accelerator in configured}
            for url in ready_urls:
                info = replica_info.get(url, {})
                accelerator = info.get('gpu_type')
                free = max(0, int(free_by_url.get(url, 0)))
                if accelerator not in slots or free <= 0:
                    continue
                slots[accelerator] += free
                if str(info.get('is_zero_cost', '')).lower() == 'true':
                    zero_cost_slots[accelerator] += free
        return slots, zero_cost_slots

    def _reserve_immediate_accelerator_locked(self,
                                              request: fastapi.Request) -> bool:
        """Reserve one compatible ready-card slot for direct admission."""
        snapshot = self._request_queue_accelerator_slots_locked()
        if snapshot is None:
            # A legacy controller/LB pair has no exact-card catalog. Preserve
            # its aggregate admission behavior instead of guessing identity.
            return True
        accelerator_slots, zero_cost_slots = snapshot
        compatible = getattr(request, _REQUEST_ACCELERATORS_ATTR, None)
        configured = self._configured_accelerators or tuple(accelerator_slots)
        allowed = set(compatible if compatible is not None else configured)
        available = [
            card for card in configured
            if card in allowed and accelerator_slots.get(card, 0) > 0
        ]
        if not available:
            return False
        selected = next(
            (card for card in available if zero_cost_slots.get(card, 0) > 0),
            available[0])
        setattr(request, _REQUEST_GRANTED_ACCELERATOR_ATTR, selected)
        return True

    def _request_queue_profiles(self) -> list[dict[str, Any]]:
        """Return bounded queue counts by priority and compatibility set."""
        configured = self._configured_accelerators
        if configured is None:
            return []
        order = {card: index for index, card in enumerate(configured)}
        grouped: dict[tuple[int, frozenset[str]], int] = {}
        for priority, bucket in self._request_queue_waiters_for_instance(
        ).items():
            for waiter in bucket.values():
                if waiter.abandoned:
                    continue
                compatible = getattr(waiter.request, _REQUEST_ACCELERATORS_ATTR,
                                     None)
                cards = frozenset(
                    compatible if compatible is not None else configured)
                grouped[(priority, cards)] = grouped.get(
                    (priority, cards), 0) + 1
        return [{
            'priority': priority,
            'compatible_accelerators': sorted(
                cards, key=lambda card: order.get(card, len(order))),
            'count': count,
        } for (priority, cards), count in sorted(
            grouped.items(),
            key=lambda item:
            (-item[0][0],
             tuple(sorted(order.get(card, len(order)) for card in item[0][1]))))
               ]

    def _record_request_demand_once(self, request: fastapi.Request) -> None:
        """Commit one legacy/accepted demand event for this LB request."""
        if vars(request).get(_REQUEST_DEMAND_RECORDED_ATTR, False):
            return
        self._request_aggregator.add(request)
        setattr(request, _REQUEST_DEMAND_RECORDED_ATTR, True)

    @staticmethod
    def _mark_request_classification_eligible(request: fastapi.Request) -> None:
        """Open the terminal classification fence for one inbound request."""
        setattr(request, _REQUEST_CLASSIFICATION_ELIGIBLE_ATTR, True)

    def _record_request_classification_once(self, request: fastapi.Request, *,
                                            rejected: bool) -> None:
        """Commit exactly one terminal outcome after the eligibility fence."""
        request_state = vars(request)
        if (not request_state.get(_REQUEST_CLASSIFICATION_ELIGIBLE_ATTR, False)
                or request_state.get(_REQUEST_CLASSIFICATION_RECORDED_ATTR,
                                     False)):
            return
        self._request_aggregator.add_request_classification(rejected=rejected)
        setattr(request, _REQUEST_CLASSIFICATION_RECORDED_ATTR, True)
        if self._draining:
            self._schedule_drain_history_flush()

    def _set_queued_compatibility_demand_support(self, supported: bool) -> None:
        """Apply controller queue-gauge capability and rollback fallback."""
        previous = self._queued_compatibility_demand_supported
        self._queued_compatibility_demand_supported = supported
        if not previous or supported:
            return
        # A controller rollback can happen while requests are already waiting.
        # Backfill each waiter once into the legacy arrival feed so the old
        # controller can still cold-start compatible capacity. Request-local
        # idempotence prevents a later admission/timeout from counting twice.
        for bucket in self._request_queue_waiters_for_instance().values():
            for waiter in bucket.values():
                if not waiter.abandoned:
                    self._record_request_demand_once(waiter.request)

    def _in_flight_by_accelerator_locked(self) -> dict[str, int]:
        """Attribute work to exact cards while holding client-pool lock."""
        if self._queue_uses_async_occupancy():
            in_flight_by_url = self._effective_occupancy_locked()
        else:
            in_flight_by_url = (
                self._load_balancing_policy.snapshot_in_flight() or {})
        result: dict[str, int] = {}
        for url, count in in_flight_by_url.items():
            card = self._replica_info_by_url.get(url, {}).get('gpu_type')
            if (not isinstance(card, str) or
                    card not in (self._configured_accelerators or ())):
                continue
            result[card] = result.get(card, 0) + max(0, int(count))
        return result

    @staticmethod
    def _resolve_request_queue_waiter_locked(
            waiter: _RequestQueueWaiter) -> None:
        if not waiter.future.done():
            waiter.future.set_result(None)

    def _grant_request_queue_waiter_locked(self,
                                           waiter: _RequestQueueWaiter) -> None:
        self._active_request_count += 1
        if self._queue_uses_async_occupancy():
            with self._client_pool_lock:
                self._record_unassigned_occupancy_admission_locked(
                    waiter.request)
        waiter.granted = True
        self._resolve_request_queue_waiter_locked(waiter)

    def _reclaim_request_queue_grant_locked(
            self, waiter: _RequestQueueWaiter) -> bool:
        if not waiter.granted or waiter.consumed:
            return False
        waiter.granted = False
        self._active_request_count = max(0, self._active_request_count - 1)
        with self._client_pool_lock:
            self._release_unassigned_occupancy_admission_locked(waiter.request)
        return True

    def _dispatch_request_queue_locked(self) -> None:
        """Grant available slots by strict priority and FIFO within a tie."""
        if self._waiting_request_count <= 0:
            return
        for bucket in list(self._request_queue_waiters_for_instance().values()):
            for waiter in list(bucket.values()):
                if not waiter.abandoned:
                    continue
                self._remove_request_queue_waiter_locked(waiter)
                self._resolve_request_queue_waiter_locked(waiter)
        if self._waiting_request_count <= 0:
            return
        if self._draining or not self._accepts_new_requests():
            while True:
                queued_waiter = self._pop_request_queue_waiter_locked()
                if queued_waiter is None:
                    return
                queued_waiter.terminal_error = (
                    self._draining_request_error()
                    if self._draining else self._inactive_role_request_error())
                self._resolve_request_queue_waiter_locked(queued_waiter)

        if self._request_queue_config is None:
            # A live update disabled queueing. Preserve the existing unbounded
            # semantics by releasing all already-queued requests at once.
            available = self._waiting_request_count
        else:
            dispatch_limit, _ = self._request_queue_limits()
            available = max(0, dispatch_limit - self._current_dispatch_load())
        if available <= 0:
            return
        slot_snapshot = (self._request_queue_accelerator_slots_locked()
                         if self._request_queue_config is not None else None)
        accelerator_slots, zero_cost_slots = (slot_snapshot if slot_snapshot
                                              is not None else (None, None))
        if accelerator_slots is not None and zero_cost_slots is not None:
            grant_plan = self._build_request_queue_grant_plan_locked(
                accelerator_slots, dict(zero_cost_slots), available)
            for waiter, accelerator in grant_plan:
                if not self._remove_request_queue_waiter_locked(waiter):
                    continue
                setattr(waiter.request, _REQUEST_GRANTED_ACCELERATOR_ATTR,
                        accelerator)
                self._grant_request_queue_waiter_locked(waiter)
            return
        for _ in range(available):
            queued_waiter = self._pop_request_queue_waiter_locked(
                accelerator_slots, zero_cost_slots)
            if queued_waiter is None:
                break
            self._grant_request_queue_waiter_locked(queued_waiter)

    async def _notify_request_queue(self) -> None:
        async with self._request_queue_condition:
            self._dispatch_request_queue_locked()

    @staticmethod
    def _draining_request_error() -> fastapi.HTTPException:
        return fastapi.HTTPException(
            status_code=503,
            detail='Load balancer is draining; retry another endpoint.',
            headers={
                'Retry-After': str(constants.LB_503_RETRY_AFTER_SECONDS),
                # A persistent connection may still be pinned to this pod
                # after the Service selector has moved to the standby. Close
                # it after the rejection so the retry reaches the new active
                # slot instead of repeating 503s for the full drain grace.
                'Connection': 'close',
            })

    @staticmethod
    def _queue_timeout_error() -> fastapi.HTTPException:
        return fastapi.HTTPException(
            status_code=503,
            detail='Timed out waiting in the load balancer request queue.',
            headers={'Retry-After': str(constants.LB_503_RETRY_AFTER_SECONDS)})

    @staticmethod
    def _queue_disconnect_error() -> fastapi.HTTPException:
        return fastapi.HTTPException(
            status_code=499,
            detail=('Client disconnected while waiting in the load balancer '
                    'request queue.'))

    def _retain_background_task(self, task: asyncio.Task) -> None:
        """Own a background task until completion and report failures."""
        self._background_tasks.add(task)

        def _forget(done: asyncio.Task) -> None:
            self._background_tasks.discard(done)
            if done.cancelled():
                return
            exception = done.exception()
            if exception is not None:
                done.get_loop().call_exception_handler({
                    'message': 'SkyServe load balancer background task failed',
                    'exception': exception,
                    'task': done,
                })

        task.add_done_callback(_forget)

    def _start_background_loops(self) -> None:
        """Start and own the load balancer's process-lifetime loops."""
        background_loops = (
            self._sync_with_controller,
            self._sync_role_with_controller,
            self._probe_occupancy_loop,
            self._sync_system_recovery_route_lease,
        )
        for background_loop in background_loops:
            self._retain_background_task(asyncio.create_task(background_loop()))

    async def _cleanup_request_queue_waiter(
            self, waiter: _RequestQueueWaiter) -> None:
        async with self._request_queue_condition:
            waiter.abandoned = True
            self._remove_request_queue_waiter_locked(waiter)
            self._reclaim_request_queue_grant_locked(waiter)
            self._resolve_request_queue_waiter_locked(waiter)
            self._dispatch_request_queue_locked()

    async def _acquire_request_slot(self,
                                    request: fastapi.Request,
                                    priority: int | None = None) -> bool:
        """Acquire process-local admission, queueing when configured."""
        if self._draining:
            raise self._draining_request_error()
        if not self._accepts_new_requests():
            raise self._inactive_role_request_error()
        config = self._request_queue_config
        if config is None:
            self._active_request_count += 1
            return True
        if priority is None:
            priority = getattr(request, _REQUEST_PRIORITY_ATTR,
                               constants.LB_REQUEST_PRIORITY_MIN)
        deadline = time.monotonic() + self._request_queue_timeout(
            config, priority)
        waiter: _RequestQueueWaiter | None = None
        try:
            async with self._request_queue_condition:
                if self._draining:
                    raise self._draining_request_error()
                if not self._accepts_new_requests():
                    raise self._inactive_role_request_error()
                # A controller sync may disable the queue while this coroutine
                # was waiting for the scheduler lock.
                if self._request_queue_config is None:
                    self._active_request_count += 1
                    return True
                dispatch_limit, queue_size = self._request_queue_limits()
                if (self._waiting_request_count == 0 and
                        self._current_dispatch_load() < dispatch_limit and
                        self._reserve_immediate_accelerator_locked(request)):
                    self._active_request_count += 1
                    if self._queue_uses_async_occupancy():
                        self._record_unassigned_occupancy_admission(request)
                    return True
                if self._waiting_request_count >= queue_size:
                    # Capacity rejections are real demand even though they
                    # never enter the queue. Unlike HA drain/inactive
                    # rejections, recording them helps the controller grow the
                    # exact compatible fleet instead of hiding overload.
                    self._record_request_demand_once(request)
                    self._mark_request_classification_eligible(request)
                    self._record_rejection(request)
                    raise fastapi.HTTPException(
                        status_code=503,
                        detail=(f'Load balancer request queue is full '
                                f'({queue_size} waiting request(s)).'),
                        headers={
                            'Retry-After': str(
                                constants.LB_503_RETRY_AFTER_SECONDS)
                        })
                sequence = self._request_queue_sequence
                self._request_queue_sequence += 1
                waiter = _RequestQueueWaiter(
                    request=request,
                    priority=priority,
                    sequence=sequence,
                    future=asyncio.get_running_loop().create_future())
                waiters = self._request_queue_waiters_for_instance()
                waiters.setdefault(priority, {})[sequence] = waiter
                self._waiting_request_count += 1
                self._dispatch_request_queue_locked()

            while True:
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(waiter.future),
                            min(remaining,
                                _REQUEST_QUEUE_DISCONNECT_POLL_SECONDS))
                    except asyncio.TimeoutError:
                        pass
                disconnected = await request.is_disconnected()
                async with self._request_queue_condition:
                    if waiter.terminal_error is not None:
                        raise waiter.terminal_error
                    if waiter.granted:
                        if self._draining:
                            self._reclaim_request_queue_grant_locked(waiter)
                            self._dispatch_request_queue_locked()
                            raise self._draining_request_error()
                        if not self._accepts_new_requests():
                            self._reclaim_request_queue_grant_locked(waiter)
                            self._dispatch_request_queue_locked()
                            raise self._inactive_role_request_error()
                        if disconnected:
                            self._reclaim_request_queue_grant_locked(waiter)
                            self._dispatch_request_queue_locked()
                            raise self._queue_disconnect_error()
                        waiter.consumed = True
                        return True
                    if disconnected:
                        self._remove_request_queue_waiter_locked(waiter)
                        self._resolve_request_queue_waiter_locked(waiter)
                        self._dispatch_request_queue_locked()
                        raise self._queue_disconnect_error()
                    if time.monotonic() >= deadline:
                        self._remove_request_queue_waiter_locked(waiter)
                        self._resolve_request_queue_waiter_locked(waiter)
                        self._record_request_demand_once(request)
                        self._mark_request_classification_eligible(request)
                        self._record_rejection(request)
                        self._dispatch_request_queue_locked()
                        raise self._queue_timeout_error()
                    # A missed capacity signal is repaired by the bounded
                    # disconnect poll without broadcasting to other waiters.
                    self._dispatch_request_queue_locked()
                    if waiter.granted:
                        waiter.consumed = True
                        return True
        except asyncio.CancelledError:
            if waiter is not None:
                # Synchronous fencing makes a grant ineligible before cleanup
                # waits for the scheduler lock.
                waiter.abandoned = True
            raise
        finally:
            if waiter is not None and not waiter.consumed:
                cleanup = asyncio.create_task(
                    self._cleanup_request_queue_waiter(waiter))
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    self._retain_background_task(cleanup)
                    raise

    async def _release_request_slot(self,
                                    request: fastapi.Request | None = None
                                   ) -> None:
        # Release state before the first await. A request can be cancelled once
        # for a downstream disconnect and again during server shutdown; if the
        # second cancellation lands while waiting for the condition lock, the
        # admission count must already be returned. These mutations are atomic
        # with acquire's predicate/update on the single event-loop thread
        # because there is no await between their reads and writes.
        if request is not None:
            with self._client_pool_lock:
                self._release_unassigned_occupancy_admission_locked(request)
        self._active_request_count = max(0, self._active_request_count - 1)

        # Keep notification alive independently of this request task. A
        # second cancellation can arrive while it waits for the condition
        # lock; the released state is safe already, but an envelope-mode
        # waiter still needs the wakeup because it has no occupancy probe
        # to provide a later one.
        async def _notify() -> None:
            await self._notify_request_queue()

        try:
            await _notify()
        except asyncio.CancelledError:
            notification = asyncio.create_task(_notify())
            # The event loop only keeps weak task references. Retain this
            # rare cancellation fallback until its condition notification
            # finishes, then consume any shutdown-time exception.
            self._retain_background_task(notification)
            raise

    def _release_waiting_body_budget(self,
                                     request: fastapi.Request,
                                     *,
                                     drop_body: bool = False) -> None:
        """Release one request's pre-admission body-byte reservation."""
        request_state = vars(request)
        reserved = request_state.pop(_WAITING_REQUEST_BODY_BYTES_ATTR, 0)
        if reserved:
            self._waiting_request_body_bytes = max(
                0, self._waiting_request_body_bytes - reserved)
        if drop_body:
            request_state.pop(_BOUNDED_REQUEST_BODY_ATTR, None)

    async def _request_body(self, request: fastapi.Request) -> bytes:
        """Read a request body with the configured hard memory bound."""
        config = self._request_queue_config
        if config is None:
            return await request.body()
        request_state = vars(request)
        cached = request_state.get(_BOUNDED_REQUEST_BODY_ATTR)
        if cached is not None:
            return cached
        limit = config['max_request_body_bytes']
        content_length = request.headers.get('content-length')
        if content_length is not None:
            try:
                if int(content_length) > limit:
                    raise fastapi.HTTPException(
                        status_code=413,
                        detail=f'Request body exceeds the {limit}-byte load '
                        'balancer limit.')
            except ValueError:
                pass
        body = bytearray()
        reserved = 0
        completed = False
        try:
            async for chunk in request.stream():
                if len(body) + len(chunk) > limit:
                    raise fastapi.HTTPException(
                        status_code=413,
                        detail=f'Request body exceeds the {limit}-byte load '
                        'balancer limit.')
                next_total = self._waiting_request_body_bytes + len(chunk)
                waiting_budget = (
                    constants.LB_REQUEST_QUEUE_WAITING_BODY_MEMORY_BUDGET_BYTES)
                if next_total > waiting_budget:
                    self._record_rejection(request)
                    raise fastapi.HTTPException(
                        status_code=503,
                        detail=('Load balancer request-body buffer is full '
                                f'({waiting_budget} bytes).'),
                        headers={
                            'Retry-After': str(
                                constants.LB_503_RETRY_AFTER_SECONDS)
                        })
                self._waiting_request_body_bytes = next_total
                reserved += len(chunk)
                body.extend(chunk)
            result = bytes(body)
            request_state[_BOUNDED_REQUEST_BODY_ATTR] = result
            request_state[_WAITING_REQUEST_BODY_BYTES_ATTR] = reserved
            completed = True
            return result
        finally:
            if not completed and reserved:
                self._waiting_request_body_bytes = max(
                    0, self._waiting_request_body_bytes - reserved)

    def _is_ready_to_serve(self) -> bool:
        """Sync readiness is independent from HA Service traffic selection."""
        return self._ready and not self._draining

    def _accepts_new_requests(self) -> bool:
        """Admit traffic only when this synchronized slot can be selected."""
        # ARMED is still outside the stable Service selector when granted.
        # Letting it admit traffic closes the tiny selector-patch/heartbeat-
        # response window: if Kubernetes starts routing immediately after the
        # patch, the target is already able to serve. Direct Pod-IP access is
        # unsupported, so this does not create a second supported authority.
        return (not self._draining and
                self._lb_role in (lb_ha.LbRole.ARMED, lb_ha.LbRole.ACTIVE))

    def _inactive_role_request_error(self) -> fastapi.HTTPException:
        role = self._lb_role
        headers = {'Retry-After': str(constants.LB_503_RETRY_AFTER_SECONDS)}
        if role is lb_ha.LbRole.DRAINING:
            # Role-driven cutovers can fence the old active slot before its
            # process receives SIGTERM. Release persistent clients in that
            # interval for the same reason as process-local draining.
            headers['Connection'] = 'close'
        return fastapi.HTTPException(
            status_code=503,
            detail=f'Load balancer slot is {role.value.lower()}.',
            headers=headers)

    def _begin_draining(self) -> None:
        """Start draining (idempotent): fail readiness + stop syncing."""
        if self._draining:
            return
        logger.info('Draining load balancer: failing readiness and '
                    'deregistering from the controller sync.')
        self._draining = True
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Direct synchronous tests and unsupported runtimes have no loop.
            # Production SIGTERM is delivered on uvicorn's running loop.
            return
        self._retain_background_task(
            loop.create_task(self._notify_request_queue()))
        self._schedule_drain_history_flush()

    def _schedule_drain_history_flush(self) -> None:
        """Coalesce drain-time history flushes without stranding late data."""
        if not self._draining:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._drain_history_flush_generation += 1
        existing = self._drain_history_flush_task
        if existing is not None and not existing.done():
            return
        task = loop.create_task(self._flush_drain_history_until_quiet())
        self._drain_history_flush_task = task

        def _clear(done: asyncio.Task) -> None:
            # A late classification may have observed ``done()`` and already
            # installed the successor before this callback runs.
            if self._drain_history_flush_task is done:
                self._drain_history_flush_task = None

        task.add_done_callback(_clear)
        self._retain_background_task(task)

    async def _flush_drain_history_until_quiet(self) -> None:
        """Repeat until no classification arrived during the last send."""
        while True:
            generation = self._drain_history_flush_generation
            await self._flush_request_history_on_drain()
            if generation == self._drain_history_flush_generation:
                return

    def _get_lb_session_id(self) -> str:
        """Return the durable external LB identity, failing closed if absent."""
        pod_uid = os.environ.get(constants.LB_POD_UID_ENV_VAR, '').strip()
        if not pod_uid:
            raise RuntimeError(
                'The external load balancer requires the Kubernetes '
                f'Downward API environment variable '
                f'{constants.LB_POD_UID_ENV_VAR}.')
        # Read the Downward API value on every sync. It is immutable for a Pod,
        # but a missing/corrupt runtime environment must fail closed.
        return pod_uid

    async def _health(self,
                      request: fastapi.Request) -> fastapi.responses.Response:
        del request  # Unused.
        return fastapi.responses.Response(
            status_code=200 if self._is_ready_to_serve() else 503,
            headers={'Connection': 'close'} if self._draining else None)

    async def _liveness(self,
                        request: fastapi.Request) -> fastapi.responses.Response:
        del request  # Unused; liveness is independent of controller sync.
        return fastapi.responses.Response(status_code=200)

    def _in_flight_with_draining(
        self,) -> tuple[dict[str, int] | None, list[str], list[str], list[str]]:
        """Per-url busyness snapshot: envelopes, occupancy, and draining.

        Three measures of the same running jobs, unioned:
        - The policy's envelope in-flight (load_map) covers synchronous
          requests dispatched through this LB.
        - The replica-reported async occupancy covers fast-ack
          workloads, where the HTTP envelope closes in milliseconds and
          the load_map reads ~0 while the replica crunches for an hour.
          The occupancy API reports counts, not job ids, so exact overlap
          with still-open envelopes is unknowable. We conservatively sum the
          brief overlap; this can over-count until the fast ack closes, but
          never collapses distinct synchronous/async work into one unit.
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
            proof_urls = self._controller_occupancy_proof_urls_locked()
            occupancy = {
                url: count
                for url, count in self._replica_occupancy.items()
                if url in proof_urls
            }
            sampled_off_ready = set(self._occupancy_sampled_off_ready)
            capable = set(self._occupancy_capable)
            dispatch_generation = dict(self._occupancy_dispatch_generation)
            sample_generation = dict(self._occupancy_sample_generation)
            # Sampled under the same lock as the gauge: the controller's
            # retirement drain uses this to prove the gauge was taken
            # against a routing view that already excluded the retiring
            # replica (the gauge is sampled BEFORE this sync's response
            # re-applies the ready set, so the gauge alone cannot prove
            # it).
            routing_urls = sorted(self._routable_ready_urls_locked())
        # An occupancy sample taken while the url was still routed cannot
        # prove post-retirement idleness (work may have arrived after the
        # sample but before the url left routing): for off-ready urls,
        # only keep samples the prober took AFTER they left the ready
        # set; the rest read as unprobed (-> unknown below).
        routing_set = set(routing_urls)
        occupancy = {
            url: count
            for url, count in occupancy.items()
            if url in routing_set or url in sampled_off_ready
        }
        sampled_urls = sorted(
            url for url in occupancy if url in proof_urls and
            sample_generation.get(url) == dispatch_generation.get(url, 0))
        sampled_set = set(sampled_urls)
        if in_flight is None:
            return None, routing_urls, [], sampled_urls
        # Fold draining refcounts into the envelope totals first: a
        # draining client's streams and the current client's are
        # DISJOINT request sets on the same replica, so they add.
        for url, clients in self._draining_clients.items():
            draining = sum(
                getattr(client, _INFLIGHT_ATTR, 0) for client in clients)
            if draining > 0:
                in_flight[url] = in_flight.get(url, 0) + draining
        # Counts alone cannot identify whether a still-open async submit is
        # already included in running_count. Sum conservatively: synchronous
        # envelopes are certainly disjoint, and any duplicate async unit lasts
        # only until its fast acknowledgement closes. Probe misses ride in a
        # separate unknown set as a full-capacity floor.
        for url, running in occupancy.items():
            # Inserting an explicit 0 for a probed-idle url matters for
            # retiring urls with no envelope entry: absent would read as
            # unknown to the drain, an explicit 0 as drained.
            in_flight[url] = in_flight.get(url, 0) + running
        # An occupancy-CAPABLE url absent from this round's probe is
        # UNKNOWN, not idle: its envelope count is meaningless for
        # fast-ack work, and reporting the explicit envelope zero would
        # bypass the autoscaler's missing-entry-means-busy protection
        # and let a drain kill it mid-job. Omit it so the autoscaler
        # sees no entry.
        # ... unless the url still has live DRAINING streams: those are
        # exact refcounts (not envelope guesses), and dropping them would
        # let a retirement drain read the url as gone and kill the very
        # requests it is waiting for.
        unknown_urls: list[str] = []
        for url in capable:
            if url not in sampled_set:
                if in_flight.get(url, 0) <= 0:
                    in_flight.pop(url, None)
                # Shipped alongside the gauge so the retirement drain can
                # distinguish 'no in-flight work' (absent, trustworthy)
                # from 'occupancy unknown' (capable url with no probe
                # answer this round) and keep waiting on the latter.
                unknown_urls.append(url)
        return in_flight, routing_urls, unknown_urls, sampled_urls

    @staticmethod
    def _reject_entry_seen(entry: Any) -> float:
        """Last-seen stamp of a reject entry, tolerating the legacy float."""
        return entry[0] if isinstance(entry, tuple) else entry

    def _reject_window_for_write(self) -> dict[str, tuple[float, int]]:
        """Expire reject entries for a WRITE, in O(expired) not O(entries).

        _prune_reject_window rebuilds the whole dict. That is the right
        cost on the controller-sync read cadence, but _record_rejection
        runs on the REQUEST cadence: one rebuild per terminal 503 turns a
        rejection storm into O(entries) synchronous work per request. A
        service whose replicas are all unservable rejects every arrival,
        so entries and rejections/second rise together and the LB spends
        seconds of event-loop time per burst -- long enough to starve the
        1s liveness probe and get the Pod killed out of its own Service,
        which is a full data-plane outage rather than a slow gauge.

        Writers append (and re-insert on refresh) under a monotonic
        clock, so live entries stay in non-decreasing last-seen order and
        the oldest is always at the front: evicting from the front is
        O(expired). If that ordering is ever violated -- a dict injected
        directly, say -- front eviction stops early and leaves an expired
        entry resident, which costs a little memory but never a wrong
        gauge: every read goes through _prune_reject_window, which drops
        expired entries regardless of position.
        """
        current = self._reject_last_seen
        cutoff = time.monotonic() - constants.LB_REJECT_WINDOW_SECONDS
        profiles = self._reject_compatibility_by_key
        while current:
            key = next(iter(current))
            if self._reject_entry_seen(current[key]) > cutoff:
                break
            del current[key]
            profiles.pop(key, None)
        return current

    def _prune_reject_window(self) -> dict[str, tuple[float, int]]:
        """Drop reject entries older than the window; return the live dict.

        The READ funnel: every gauge read goes through here, on the
        controller sync/capacity cadence, where an O(entries) rebuild is
        cheap and lazy pruning means no extra task to keep alive. It also
        normalizes legacy float entries, and it drops every expired entry
        regardless of position, so the gauges stay exact even when the
        write path's front-eviction ordering does not hold (the rebuild
        preserves the existing order, it does not re-sort). Writes must
        NOT come through here -- see _reject_window_for_write.
        Always assigns a fresh instance dict so readers cannot mutate the
        writer's ordered snapshot while iterating it.
        """
        cutoff = time.monotonic() - constants.LB_REJECT_WINDOW_SECONDS
        current = self._reject_last_seen
        pruned: dict[str, tuple[float, int]] = {}
        if current:
            for key, entry in current.items():
                if isinstance(entry, tuple):
                    seen, priority = entry
                else:
                    seen = entry
                    priority = constants.LB_REQUEST_PRIORITY_MIN
                if seen > cutoff:
                    pruned[key] = (seen, priority)
        self._reject_last_seen = pruned
        profiles = self._reject_compatibility_by_key
        if profiles is not None:
            self._reject_compatibility_by_key = {
                key: profile
                for key, profile in profiles.items()
                if key in pruned
            }
        return pruned

    def _record_rejection(self, request: fastapi.Request) -> None:
        """Record a terminal-503 exit for the reject-window gauge.

        Keyed by the job-id header when present, so repeated attempts for one
        logical job refresh the TTL while still counting once. This prevents
        retries from multiplying autoscaling pressure (see constants).
        Headerless requests get a unique per-request key: one unit each.
        """
        # Starlette header lookup is case-insensitive per the HTTP spec.
        key = request.headers.get(constants.LB_JOB_ID_HEADER)
        if key is None:
            self._reject_fallback_seq += 1
            key = f'_headerless_{self._reject_fallback_seq}'
        priority = getattr(request, _REQUEST_PRIORITY_ATTR,
                           constants.LB_REQUEST_PRIORITY_MIN)
        if not isinstance(priority, int) or isinstance(priority, bool):
            priority = constants.LB_REQUEST_PRIORITY_MIN
        window = self._reject_window_for_write()
        # Re-insert rather than assign in place: refreshing an existing key
        # keeps its old position in a dict, and the front-eviction in
        # _reject_window_for_write needs the oldest entry to stay at the front.
        window.pop(key, None)
        window[key] = (time.monotonic(), priority)
        configured = self._configured_accelerators
        compatible = getattr(request, _REQUEST_ACCELERATORS_ATTR, None)
        if compatible is None:
            compatible = configured
        if (not isinstance(compatible, (list, tuple)) or not compatible or
                not all(isinstance(card, str) and card for card in compatible)):
            compatible = None
        profiles = self._reject_compatibility_by_key
        if compatible:
            profiles[key] = (priority, tuple(compatible))
        else:
            profiles.pop(key, None)
        self._record_request_classification_once(request, rejected=True)
        self._request_aggregator.add_rejection()

    def _clear_rejection(self, request: fastapi.Request) -> None:
        """Release stale backlog pressure once the same stable job lands."""
        key = request.headers.get(constants.LB_JOB_ID_HEADER)
        if key is not None:
            self._reject_window_for_write().pop(key, None)
            self._reject_compatibility_by_key.pop(key, None)

    def _rejected_compatibility_profiles(self) -> list[dict[str, Any]]:
        """Return the replaceable recent-rejection gauge by exact profile."""
        retained = self._prune_reject_window()
        profiles = self._reject_compatibility_by_key
        cutoff = (time.monotonic() -
                  constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
        grouped: dict[tuple[int, tuple[str, ...]], tuple[int, int]] = {}
        for key, (last_seen, _) in retained.items():
            profile = profiles.get(key)
            if profile is None:
                continue
            count, recent_count = grouped.get(profile, (0, 0))
            grouped[profile] = (count + 1,
                                recent_count + int(last_seen > cutoff))
        return [{
            'priority': priority,
            'compatible_accelerators': list(compatible),
            'count': count,
            'recent_count': recent_count,
        } for (priority, compatible), (count, recent_count) in sorted(
            grouped.items(), key=lambda item: (-item[0][0], item[0][1]))]

    @staticmethod
    def _request_has_stable_job_id(request: fastapi.Request) -> bool:
        stable_job_id = request.headers.get(constants.LB_JOB_ID_HEADER)
        return isinstance(stable_job_id, str) and bool(stable_job_id)

    async def _request_uses_async_occupancy(self,
                                            request: fastapi.Request) -> bool:
        """Infer the deployed fast-ack request contract for compatibility.

        The durable service declaration is what survives a cold LB restart.
        This request-level inference protects existing services before they add
        it: the platform's stable job header identifies held async jobs, while
        direct callers can use the established JSON `action=async_predict`
        contract. JSON parsing is skipped on the platform path and for all
        non-JSON requests.
        """
        if self._request_has_stable_job_id(request):
            return True
        return await self._request_action(request) == _ASYNC_ACTION_PREDICT

    async def _request_action(self,
                              request: fastapi.Request,
                              body: bytes | None = None) -> str | None:
        """Return and cache the established JSON async action, if present."""
        request_state = vars(request)
        if _REQUEST_ACTION_ATTR in request_state:
            return request_state[_REQUEST_ACTION_ATTR]
        action = None
        content_type = request.headers.get('content-type', '')
        if 'application/json' in content_type.lower():
            try:
                content_length = request.headers.get('content-length')
                if (content_length is not None and int(content_length)
                        > constants.LB_ASYNC_ACTION_BODY_MAX_BYTES):
                    request_state[_REQUEST_ACTION_ATTR] = None
                    return None
            except ValueError:
                pass
            try:
                if body is None:
                    bounded_body = request_state.get(_BOUNDED_REQUEST_BODY_ATTR)
                    body = (bounded_body if bounded_body is not None else await
                            self._request_body(request))
                if len(body) <= constants.LB_ASYNC_ACTION_BODY_MAX_BYTES:
                    payload = json.loads(body)
                    if isinstance(payload, dict) and isinstance(
                            payload.get('action'), str):
                        action = payload['action']
            except (UnicodeDecodeError, ValueError, TypeError):
                pass
        request_state[_REQUEST_ACTION_ATTR] = action
        return action

    def _record_prediction_time(self, duration_seconds: float,
                                outcome: str) -> None:
        """Record observability without allowing it to affect inference."""
        try:
            self._request_aggregator.add_prediction_time(
                duration_seconds, outcome)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to record prediction-time history: %s', e)

    def _record_async_prediction_payload(self, payload: Any) -> bool:
        """Validate and record one terminal async completion payload.

        Returns whether the payload is a valid terminal observation. A valid
        duplicate also returns True so at-least-once completion reporters can
        treat the endpoint as idempotent.
        """
        if not isinstance(payload, dict):
            return False
        request_id = payload.get('request_id')
        status = payload.get('status')
        outcome = (_ASYNC_TERMINAL_OUTCOMES.get(status) if isinstance(
            status, str) else None)
        duration_ms = payload.get('processing_time_ms')
        if (not isinstance(request_id, str) or not request_id or len(request_id)
                > constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS or
                outcome is None or not isinstance(duration_ms, (int, float)) or
                isinstance(duration_ms, bool)):
            return False
        try:
            duration_ms_float = float(duration_ms)
        except (OverflowError, TypeError, ValueError):
            return False
        if not math.isfinite(duration_ms_float) or duration_ms_float < 0:
            return False
        completed = self._completed_async_prediction_ids
        if request_id in completed:
            completed.move_to_end(request_id)
            return True
        self._record_prediction_time(duration_ms_float / 1000.0, outcome)
        completed[request_id] = None
        if len(completed) > constants.LB_ASYNC_PREDICTION_DEDUP_CAP:
            completed.popitem(last=False)
        return True

    def _record_async_prediction_status(self, body: bytes,
                                        content_encoding: str) -> bool:
        """Record one terminal async status using its model-reported time."""
        if content_encoding.strip().lower() not in ('', 'identity'):
            return False
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, ValueError, TypeError, RecursionError):
            # This runs on the live proxy path, so a replica returning deeply
            # nested JSON must degrade to "not recorded" instead of escaping
            # into the inference response.
            return False
        return self._record_async_prediction_payload(payload)

    async def _prediction_completed(
            self, request: fastapi.Request) -> fastapi.Response:
        """Accept an out-of-band terminal prediction observation."""
        content_type = request.headers.get('content-type', '')
        media_type = content_type.partition(';')[0].strip().lower()
        if media_type != 'application/json':
            raise fastapi.HTTPException(status_code=415,
                                        detail='Expected application/json.')
        content_encoding = request.headers.get('content-encoding', '')
        if content_encoding.strip().lower() not in ('', 'identity'):
            raise fastapi.HTTPException(
                status_code=415,
                detail='Compressed completion payloads are not supported.')
        try:
            content_length = request.headers.get('content-length')
            if (content_length is not None and int(content_length)
                    > constants.LB_PREDICTION_COMPLETION_BODY_MAX_BYTES):
                raise fastapi.HTTPException(status_code=413,
                                            detail='Payload is too large.')
        except ValueError:
            pass

        body = bytearray()
        async for chunk in request.stream():
            if (len(body) + len(chunk)
                    > constants.LB_PREDICTION_COMPLETION_BODY_MAX_BYTES):
                raise fastapi.HTTPException(status_code=413,
                                            detail='Payload is too large.')
            body.extend(chunk)
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, ValueError, TypeError, RecursionError):
            # Deeply nested JSON raises RecursionError, which is a RuntimeError
            # rather than a ValueError: an under-cap malformed body has to stay
            # a client error instead of surfacing as a server error.
            raise fastapi.HTTPException(
                status_code=422,
                detail='Invalid prediction completion payload.') from None
        if not self._record_async_prediction_payload(payload):
            raise fastapi.HTTPException(
                status_code=422,
                detail='Invalid prediction completion payload.')
        return fastapi.Response(status_code=204)

    def _begin_async_occupancy_attempt_locked(self, url: str,
                                              request: fastapi.Request) -> None:
        """Reserve one replica slot and open the leading probe fence.

        Must be called while holding `_client_pool_lock`, in the same critical
        section as replica selection. No network operation is performed here.
        """
        self._occupancy_capable.add(url)
        if url in self._occupancy_explicitly_disabled_urls:
            self._occupancy_disable_pending.add(url)
        dispatch_generation = self._occupancy_dispatch_generation
        dispatch_generation[url] = dispatch_generation.get(url, 0) + 1

        pending = self._occupancy_pending_reservations
        pending[url] = pending.get(url, 0) + 1
        active = self._occupancy_active_attempts
        active[url] = active.get(url, 0) + 1

        # Transfer the fleet-wide queue reservation to this concrete URL. The
        # total reservation count is unchanged by the transfer.
        self._release_unassigned_occupancy_admission_locked(request)
        self._publish_replica_occupancy_locked(url)

    @staticmethod
    def _async_attempt_rejection(
        outcome: Union['fastapi.responses.Response', BaseException, None],
    ) -> tuple[bool, bool]:
        """Return (definitely rejected, invalidate capacity baseline)."""
        if outcome is None:
            return False, False
        if isinstance(outcome, _RetriableStatusError):
            return True, True
        if isinstance(outcome, fastapi.responses.Response):
            accepted = 200 <= outcome.status_code < 300
            invalidate = outcome.status_code == 429 or outcome.status_code >= 500
            return not accepted, not accepted and invalidate
        if isinstance(outcome, fastapi.HTTPException):
            # Raised by the LB before proxy dispatch (e.g. request-body bound).
            return True, False
        if isinstance(outcome,
                      Exception) and _is_definitely_not_dispatched(outcome):
            # The request was not accepted, but the endpoint itself is missing
            # or unreachable, so its old free-slot sample is not actionable.
            return True, True
        # Read/write/protocol failures and cancellation may follow acceptance.
        return False, False

    def _finish_async_occupancy_attempt(
            self,
            url: str,
            outcome: Union['fastapi.responses.Response', BaseException, None],
            request: fastapi.Request | None = None) -> bool:
        """Close the trailing probe fence and reconcile one reservation.

        Returns whether immediately reusable capacity increased, so the caller
        can wake queue waiters without waking them for every fast ack.
        """
        with self._client_pool_lock:
            before_free = self._effective_free_slots_for_replica_locked(
                url) or 0
            dispatch_generation = self._occupancy_dispatch_generation
            dispatch_generation[url] = dispatch_generation.get(url, 0) + 1

            active = self._occupancy_active_attempts
            active_count = active.get(url, 0)
            if active_count <= 1:
                active.pop(url, None)
            else:
                active[url] = active_count - 1

            rejected, invalidate_capacity = self._async_attempt_rejection(
                outcome)
            pending = self._occupancy_pending_reservations
            if rejected:
                pending_count = pending.get(url, 0)
                if pending_count <= 1:
                    pending.pop(url, None)
                else:
                    pending[url] = pending_count - 1
                returned_to_fleet = request is not None
                if returned_to_fleet:
                    # The request remains admitted while the retry loop decides
                    # whether to select another URL. Transfer, rather than
                    # release, its reservation so a newly admitted request
                    # cannot consume the same fleet slot in that gap. A
                    # terminal result releases it in the request owner's outer
                    # cleanup.
                    self._record_unassigned_occupancy_admission_locked(request)
                if invalidate_capacity:
                    self._replica_free_slots.pop(url, None)
                elif (url not in active and url not in pending and
                      (url in self._replica_occupancy or
                       url in self._replica_free_slots)):
                    # A local/non-capacity rejection cannot have changed
                    # replica occupancy. Revalidate the retained baseline at
                    # the new trailing generation once no other attempt is
                    # crossing it.
                    sample_generation = self._occupancy_sample_generation
                    sample_generation[url] = dispatch_generation[url]

            self._publish_replica_occupancy_locked(url)
            after_free = self._effective_free_slots_for_replica_locked(url) or 0
            return after_free > before_free and not (rejected and
                                                     request is not None)

    def _rejected_in_window(self) -> int:
        """Unique jobs terminally 503'd within the reject window (gauge)."""
        return len(self._prune_reject_window())

    def _rejected_in_recent_window(self) -> int:
        """Unique rejected jobs refreshed in the autoscaler rate window."""
        retained = self._prune_reject_window()
        cutoff = (time.monotonic() -
                  constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
        return sum(last_seen > cutoff for last_seen, _ in retained.values())

    def _rejected_by_priority(self, recent: bool = False) -> dict[str, int]:
        retained = self._prune_reject_window()
        cutoff = (time.monotonic() -
                  constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
        counts: dict[int, int] = {}
        for last_seen, priority in retained.values():
            if recent and last_seen <= cutoff:
                continue
            counts[priority] = counts.get(priority, 0) + 1
        return {
            str(priority): count for priority, count in sorted(counts.items())
        }

    def _offered_arrivals_for_write(
            self,
            now: float) -> tuple[dict[str, float], collections.deque[float]]:
        """Expire arrival entries for a WRITE, in O(expired) not O(entries).

        _record_offered_arrival runs on EVERY request, so rebuilding the
        job dict there costs O(entries) per request. The tracker is sized
        by LB_OFFERED_ARRIVAL_CAP (100k), which bounds memory but turns
        into a CPU cliff: the busier the LB, the more entries are
        resident and the more every single request costs. At the cap that
        is milliseconds of synchronous event-loop time per request, so
        the LB stops answering its own liveness probe and Kubernetes
        evicts it -- load-induced collapse exactly where the cap was
        meant to protect. Front eviction keeps the write path O(expired);
        _prune_offered_arrivals remains the exact reader.
        """
        cutoff = now - constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS
        jobs = self._offered_arrivals_by_job
        while jobs:
            key = next(iter(jobs))
            if jobs[key] > cutoff:
                break
            del jobs[key]
        headerless = self._headerless_offered_arrivals
        while headerless and headerless[0] <= cutoff:
            headerless.popleft()
        saturated_until = self._offered_arrival_saturated_until
        if saturated_until is not None and now >= saturated_until:
            self._offered_arrival_saturated_until = None
        return jobs, headerless

    def _prune_offered_arrivals(
        self,
        now: float | None = None
    ) -> tuple[dict[str, float], collections.deque[float]]:
        current = time.monotonic() if now is None else now
        cutoff = current - constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS
        jobs = self._offered_arrivals_by_job
        self._offered_arrivals_by_job = {
            key: seen for key, seen in jobs.items() if seen > cutoff
        }
        headerless = self._headerless_offered_arrivals
        while headerless and headerless[0] <= cutoff:
            headerless.popleft()
        saturated_until = self._offered_arrival_saturated_until
        if saturated_until is not None and current >= saturated_until:
            self._offered_arrival_saturated_until = None
        return self._offered_arrivals_by_job, headerless

    def _record_offered_arrival(self, request: fastapi.Request) -> None:
        now = time.monotonic()
        jobs, headerless = self._offered_arrivals_for_write(now)
        if self._offered_arrival_saturated_until is not None:
            self._offered_arrival_saturated_until = (
                now + constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS)
            return
        job_id = request.headers.get(constants.LB_JOB_ID_HEADER)
        if not isinstance(job_id, str) or not job_id:
            job_id = None
        key: str | None = None
        if job_id is not None:
            key = hashlib.sha256(job_id.encode('utf-8')).hexdigest()
            if key in jobs:
                # Re-insert so the refreshed entry moves to the back and the
                # oldest arrival stays at the front for O(expired) eviction.
                del jobs[key]
                jobs[key] = now
                return
        if len(jobs) + len(headerless) >= constants.LB_OFFERED_ARRIVAL_CAP:
            self._offered_arrival_saturated_until = (
                now + constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS)
            return
        if job_id is None:
            headerless.append(now)
        else:
            assert key is not None
            jobs[key] = now

    def _offered_arrival_counts(self) -> dict[str, int | bool]:
        now = time.monotonic()
        jobs, headerless = self._prune_offered_arrivals(now)
        recent_cutoff = (now - constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
        saturated = self._offered_arrival_saturated_until is not None
        if saturated:
            # Publish the conservative combined count directly as the stable
            # job bucket. Headerless is zero so consumers that sum the two
            # populations observe exactly the cap rather than twice the cap.
            return {
                'unique_job_arrivals_60s': constants.LB_OFFERED_ARRIVAL_CAP,
                'unique_job_arrivals_300s': constants.LB_OFFERED_ARRIVAL_CAP,
                'headerless_arrivals_60s': 0,
                'headerless_arrivals_300s': 0,
                'offered_arrival_tracking_saturated': True,
            }
        return {
            'unique_job_arrivals_60s': sum(
                seen > recent_cutoff for seen in jobs.values()),
            'unique_job_arrivals_300s': len(jobs),
            'headerless_arrivals_60s': sum(
                seen > recent_cutoff for seen in headerless),
            'headerless_arrivals_300s': len(headerless),
            'offered_arrival_tracking_saturated': saturated,
        }

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
        counts, not targets. The controller sync also carries the configured
        max_replicas ceiling so this endpoint is a complete admission read;
        it remains null until a controller supporting the field has synced.

        CONTRACT for admission readers (deliberate, do not "fix"):
        - materialized_slots is stable inventory for the current ready set.
          ready_replicas / in_flight and the occupancy aggregates remain
          admission-facing and observation-gated in logical mode. With
          reserved-capacity fill enabled, idle reserved (zero-cost) machines
          become admission capacity only by joining the ready set. Fresh free
          reserved slots may be reported separately as control-plane telemetry,
          but are never included in materialized_slots or direct admission.
        - target_replicas is the DEMAND-side autoscaler target, not a
          capacity statement: under fill, ready_replicas legitimately
          exceeds it (opportunistic zero-cost supply). Admission must
          never use it as a floor or clamp capacity down to it — sizing
          on target_replicas would idle exactly the free machines the
          fill feature exists to use.
        - In logical mode max_replicas is the effective admission ceiling,
          max(configured_max_replicas, ready_replicas). This preserves
          indivisible-machine overhang and capacity above a recently reduced
          policy bound. configured_max_replicas remains the demand clamp.
          Legacy physical-backend services retain their historical fields.
        """
        del request  # Unused.
        with self._client_pool_lock:
            ready_set = self._routable_ready_urls_locked()
            ready_replicas = len(ready_set)
            hint = self._capacity_hint or {}
            replica_unit = hint.get('replica_unit', 'physical_backend')
            logical_replicas = replica_unit == 'logical_slot'
            planned_capacity_by_url = hint.get('planned_capacity_by_url', {})
            logical_urls = ready_set & set(
                hint.get('logical_replica_urls', planned_capacity_by_url))
            planned_logical_capacity = {
                url: planned_capacity_by_url[url]
                for url in logical_urls
                if isinstance(planned_capacity_by_url.get(url), int) and
                not isinstance(planned_capacity_by_url[url], bool) and
                planned_capacity_by_url[url] > 0
            }
            materialized_slots = (sum(planned_logical_capacity.values())
                                  if logical_replicas else ready_replicas)
            total_slots_by_url = self._replica_total_slots
            # [boltz fork] Occupancy aggregates, over probed AND ready
            # replicas only: a probe entry for a since-pruned replica must
            # not count, and an unprobed replica contributes no free slot
            # (unknown != idle — the admission reader can treat the
            # probed/ready gap as optimistically or conservatively as it
            # likes, but this endpoint never invents capacity).
            probed = {
                url: slots
                for url, slots in
                self._effective_replica_free_slots_locked().items()
                if url in ready_set and url in total_slots_by_url
            }
            probed_backend_count = len(probed)
            usable_sample_times = {
                url: sampled_at
                for url, sampled_at in self._occupancy_sample_time.items()
                if url in probed
            }
            oldest_usable_sample_age: float | None = None
            if usable_sample_times:
                oldest_usable_sample_age = max(
                    0.0,
                    time.monotonic() - min(usable_sample_times.values()))
            probed_replicas = probed_backend_count
            busy_replicas = sum(1 for free in probed.values() if free <= 0)
            logical_plan_is_usable = (not logical_replicas or all(
                isinstance(planned_capacity_by_url.get(url), int) and
                not isinstance(planned_capacity_by_url[url], bool) and
                planned_capacity_by_url[url] > 0 for url in logical_urls))
            if logical_replicas:
                # The runtime may report a degraded or buggy capacity. Never
                # expose more logical replicas than the immutable width the
                # controller pinned for that backend. A missing plan is an
                # incompatible mixed-version report and fails closed.
                total_slots = sum(
                    min(total_slots_by_url[url], planned_capacity_by_url[url])
                    for url in probed
                    if isinstance(planned_capacity_by_url.get(url), int) and
                    not isinstance(planned_capacity_by_url[url], bool) and
                    planned_capacity_by_url[url] > 0)
            else:
                total_slots = sum(total_slots_by_url[url] for url in probed)
            observed_occupancy = self._replica_occupancy
            pending_reservations = self._occupancy_pending_reservations
            # Reported work is meaningful only for the probed set, while an
            # accepted post-probe reservation remains real work even if the
            # next probe misses and its capacity baseline fails closed.
            running_slots = (
                sum(observed_occupancy.get(url, 0) for url in probed) +
                sum(pending_reservations.get(url, 0) for url in ready_set) +
                self._occupancy_unassigned_reservations)
            if logical_replicas:

                def _bounded_logical_free(url: str) -> int:
                    runtime_total = total_slots_by_url.get(url)
                    if runtime_total is None:
                        # A ready backend can disappear from an occupancy
                        # snapshot after a probe miss. Unknown capacity is not
                        # free capacity, so fail closed instead of indexing a
                        # partial snapshot.
                        return 0
                    runtime_busy = max(0, runtime_total - probed.get(url, 0))
                    planned_free = max(
                        0, planned_capacity_by_url[url] - runtime_busy)
                    return min(probed.get(url, 0), planned_free)

                free_slots = max(
                    0,
                    sum(
                        _bounded_logical_free(url)
                        for url in logical_urls
                        if isinstance(planned_capacity_by_url.get(url), int) and
                        not isinstance(planned_capacity_by_url[url], bool) and
                        planned_capacity_by_url[url] > 0) -
                    self._occupancy_unassigned_reservations)
            else:
                free_slots = max(
                    0,
                    sum(probed.values()) -
                    self._occupancy_unassigned_reservations)
        request_queue_capacity: int | None = None
        request_queue_dispatch_limit: int | None = None
        request_queue_submission_limit: int | None = None
        request_queue_min_size: int | None = None
        request_queue_size_per_replica: int | None = None
        request_queue_max_size: int | None = None
        request_queue_max_concurrency: int | None = None
        request_queue_max_request_body_bytes: int | None = None
        request_queue_timeout_seconds: float | None = None
        request_queue_uses_async_occupancy: bool | None = None
        if self._request_queue_config is not None:
            (request_queue_dispatch_limit,
             request_queue_capacity) = self._request_queue_limits()
            request_queue_submission_limit = (
                self._request_queue_submission_limit())
            request_queue_min_size = self._request_queue_config['min_size']
            request_queue_size_per_replica = self._request_queue_config[
                'size_per_replica']
            request_queue_max_size = self._request_queue_config['max_size']
            request_queue_max_concurrency = self._request_queue_config[
                'max_concurrency']
            request_queue_max_request_body_bytes = self._request_queue_config[
                'max_request_body_bytes']
            request_queue_timeout_seconds = self._request_queue_config[
                'timeout_seconds']
            request_queue_uses_async_occupancy = self._request_queue_config.get(
                'use_async_occupancy', False)
        # Envelope in-flight plus occupancy per url and including
        # pruned-but-draining work. The count API has no job ids, so the brief
        # fast-ack overlap is summed conservatively rather than risking that
        # distinct synchronous and async work collapse into one unit. (Called
        # outside the pool lock -- it acquires the lock itself.)
        in_flight_map, _, _, _ = self._in_flight_with_draining()
        in_flight = (sum(in_flight_map.values())
                     if in_flight_map is not None else None)
        last_sync_age: float | None = None
        if self._last_sync_time is not None:
            last_sync_age = max(time.monotonic() - self._last_sync_time, 0.0)
        occupancy_probe_age: float | None = None
        if self._last_occupancy_probe_time is not None:
            occupancy_probe_age = max(
                time.monotonic() - self._last_occupancy_probe_time, 0.0)
        # Capacity hint fields stay null until a controller sync carries
        # one: an admission reader must see "unknown" (and fall back to
        # its conservative floor) rather than zeros it would act on.
        configured_max_replicas = hint.get('configured_max_replicas',
                                           hint.get('max_replicas'))
        expected_probed_replicas = (len(logical_urls)
                                    if logical_replicas else ready_replicas)
        observed_expected_replicas = (len(set(probed) &
                                          logical_urls) if logical_replicas else
                                      probed_backend_count)
        occupancy_is_usable = (
            observed_expected_replicas == expected_probed_replicas and
            logical_plan_is_usable and occupancy_probe_age is not None and
            occupancy_probe_age <= constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS)
        # Machine-agnostic admission contract. Consumers should not need to
        # know whether capacity comes from one worker per replica or several
        # local workers sharing a multi-GPU machine. Until every ready replica
        # has a fresh occupancy sample, preserve the legacy one-unit-per-
        # replica view instead of exposing a partial slot total.
        current_capacity = (total_slots
                            if occupancy_is_usable else ready_replicas)
        in_flight_capacity = (running_slots
                              if occupancy_is_usable else in_flight)
        if logical_replicas:
            # A physical backend is not a valid fallback unit. Logical
            # readiness therefore fails closed until the complete ready set
            # has a fresh capacity observation.
            ready_replicas = total_slots if occupancy_is_usable else 0
            in_flight = in_flight_capacity
            max_replicas = (max(configured_max_replicas, ready_replicas)
                            if configured_max_replicas is not None else None)
            current_capacity = ready_replicas
            max_capacity = max_replicas
            in_flight_capacity = in_flight
            probed_replicas = total_slots
            busy_replicas = running_slots
        else:
            max_replicas = hint.get('max_replicas')
            # max_replicas is a physical-backend ceiling. It is a valid
            # capacity ceiling only while this response is also expressed in
            # physical backends. A fresh multi-worker snapshot switches
            # current_capacity to slot units, but legacy controller hints do
            # not carry an authoritative future slot width. Report unknown
            # instead of mixing units; logical mode above has the planned
            # slot-unit ceiling needed to publish an exact value.
            max_capacity_is_usable = (not occupancy_is_usable or all(
                total_slots_by_url[url] == 1 for url in probed))
            max_capacity = (max(max_replicas, current_capacity)
                            if max_replicas is not None and
                            max_capacity_is_usable else None)
        role = self._lb_role
        if role not in (lb_ha.LbRole.ARMED, lb_ha.LbRole.ACTIVE):
            # Direct Pod access is unsupported, but fail closed even if a
            # caller bypasses the stable Service selector.
            ready_replicas = 0
            current_capacity = 0
            max_capacity = 0
            max_replicas = 0
            request_queue_capacity = 0
            request_queue_dispatch_limit = 0
            request_queue_submission_limit = 0
        slot = self._lb_slot
        return fastapi.responses.JSONResponse({
            'lb_role': role.value,
            'lb_role_generation': self._lb_role_generation,
            'lb_slot': slot.value if slot is not None else None,
            'lb_pod_uid': os.environ.get(constants.LB_POD_UID_ENV_VAR),
            'lb_image_digest': os.environ.get(constants.LB_IMAGE_DIGEST_ENV_VAR
                                             ),
            'lb_ha_rollout': self._lb_ha_rollout_evidence,
            'replica_unit': replica_unit,
            'ready_replicas': ready_replicas,
            'in_flight': in_flight,
            'draining': self._draining,
            'synced': self._ready,
            # Qualification-only aggregate truth, kept separate from the
            # admission-facing ready_replicas field. A STANDBY deliberately
            # publishes zero admission capacity but must retain a complete
            # routing and occupancy snapshot to remain promotable.
            'routing_backend_count': len(ready_set),
            # Stable inventory is deliberately separate from admission-facing
            # ready/current capacity. It is safe for status and diagnostics,
            # but must never be converted to availability by subtracting work.
            'materialized_slots': materialized_slots,
            'occupancy_probed_backend_count': probed_backend_count,
            'occupancy_fresh_backend_count': observed_expected_replicas,
            'occupancy_unknown_ready_backend_count': max(
                0, expected_probed_replicas - observed_expected_replicas),
            'occupancy_oldest_usable_sample_age_seconds': oldest_usable_sample_age,
            'last_sync_age_seconds': last_sync_age,
            'queue_depth': self._queue_depth,
            'queue_depth_by_priority': self._queue_depth_priority_snapshot(),
            # Process-local admission counters are required to prove that a
            # legacy LB has no body-bearing work before selector migration.
            # They remain behind the data-plane bearer like this entire
            # endpoint and contain neither request identifiers nor payloads.
            'local_in_flight': self._active_request_count,
            'request_queue_depth': self._waiting_request_count,
            'queued_requests_by_compatibility': self._request_queue_profiles(),
            'rejected_requests_by_compatibility':
                self._rejected_compatibility_profiles(),
            'in_flight_by_accelerator': self._in_flight_by_accelerator_locked(),
            'waiting_request_body_bytes': self._waiting_request_body_bytes,
            'request_queue_capacity': request_queue_capacity,
            'request_queue_dispatch_limit': request_queue_dispatch_limit,
            'request_queue_submission_limit': request_queue_submission_limit,
            'request_queue_min_size': request_queue_min_size,
            'request_queue_size_per_replica': request_queue_size_per_replica,
            'request_queue_max_size': request_queue_max_size,
            'request_queue_max_concurrency': request_queue_max_concurrency,
            'request_queue_max_request_body_bytes': request_queue_max_request_body_bytes,
            'request_queue_timeout_seconds': request_queue_timeout_seconds,
            'request_queue_uses_async_occupancy': request_queue_uses_async_occupancy,
            'rejected_in_window': self._rejected_in_window(),
            'rejected_in_recent_window': self._rejected_in_recent_window(),
            'rejected_in_window_by_priority': self._rejected_by_priority(),
            'rejected_in_recent_window_by_priority':
                self._rejected_by_priority(recent=True),
            **self._offered_arrival_counts(),
            'provisioning_replicas': hint.get('provisioning_replicas'),
            'target_replicas': hint.get('target_num_replicas'),
            'min_replicas_by_accelerator': hint.get(
                'min_replicas_by_accelerator', {}),
            'target_replicas_by_accelerator': hint.get(
                'target_num_replicas_by_accelerator', {}),
            'demand_target_by_accelerator': hint.get(
                'demand_target_by_accelerator',
                hint.get('target_num_replicas_by_accelerator', {})),
            'warm_retention_target_by_accelerator': hint.get(
                'warm_retention_target_by_accelerator', {}),
            'cold_launch_authority_by_accelerator': hint.get(
                'cold_launch_authority_by_accelerator', {}),
            'ready_replicas_by_accelerator': hint.get(
                'ready_replicas_by_accelerator', {}),
            'provisioning_replicas_by_accelerator': hint.get(
                'provisioning_replicas_by_accelerator', {}),
            'total_replicas_by_accelerator': hint.get(
                'total_replicas_by_accelerator', {}),
            'zero_cost_ready_replicas_by_accelerator': hint.get(
                'zero_cost_ready_replicas_by_accelerator', {}),
            'fill_target_by_accelerator': hint.get('fill_target_by_accelerator',
                                                   {}),
            'free_reserved_slots_by_accelerator': hint.get(
                'free_reserved_slots_by_accelerator', {}),
            'max_replicas': max_replicas,
            'configured_max_replicas': configured_max_replicas,
            'request_accelerator_compatibility_version':
                (self._request_accelerator_compatibility_version),
            'configured_accelerators': list(self._configured_accelerators or
                                            ()),
            'current_capacity': current_capacity,
            'max_capacity': max_capacity,
            'in_flight_capacity': in_flight_capacity,
            # [boltz fork] Async-occupancy aggregates (see the probe loop).
            # For fast-ack async fleets envelope-only in_flight reads ~0
            # while replicas crunch, so admission should size on
            # free_slots gated by occupancy_probe_age_seconds, exactly
            # like last_sync_age gates the ready count. running_slots is work,
            # not total_slots - free_slots: reservations can outlive a probe
            # baseline, and draining replicas can run with total_slots == 0.
            'probed_replicas': probed_replicas,
            'busy_replicas': busy_replicas,
            'total_slots': total_slots,
            'running_slots': running_slots,
            'free_slots': free_slots,
            'occupancy_probe_age_seconds': occupancy_probe_age,
            # Bounded process-local counters and timings for HA load/chaos
            # qualification. No service, Pod, URL, or request identifiers are
            # retained, and these observations never participate in routing.
            'ha_observability': self._ha_stats().snapshot(),
        })

    def _should_keep_ready_set_on_empty_sync(
            self, ready_replica_urls: list[str],
            num_ready_replicas: int | None) -> bool:
        """Whether to keep the current ready set instead of applying an empty
        sync result.

        [boltz fork] The controller lists a replica only when it is READY *and*
        its endpoint resolves. A cold url cache (controller restart) or the
        endpoint DB being contended under a launch storm can make every READY
        replica transiently unresolvable, so a 2xx sync returns an empty map
        even though replicas are alive -- and blindly applying it would
        ``set_ready_replicas([])`` and 503 all live traffic.

        Keep the existing set only when all three hold: the map is empty, the
        controller still reports >=1 READY replica (num_ready_replicas), and we
        currently have a non-empty set to protect. A genuine scale-to-zero
        (num_ready_replicas == 0) or an older controller that omits the count
        (None) still applies the empty map, preserving prior behavior.

        Convergence while a set is pinned this way relies on the data-plane
        quarantine (``_record_proxy_outcome``): a truly-dead replica is evicted
        once it accrues enough TCP-dead failures, which shrinks the pinned set
        until it empties and the guard stops firing. A partial resolution (any
        non-empty map) always takes the normal path and prunes dead replicas
        directly, so this only ever pins on a fully-empty sync.
        """
        return (not ready_replica_urls and bool(num_ready_replicas) and
                bool(self._load_balancing_policy.ready_replicas))

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
    # true free slots. A failed probe is controller-UNKNOWN immediately. The
    # LB may retain its last generation-valid sample for bounded local
    # routing/admission grace; the replica's own shedding stays authoritative.
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_replica_occupancy(raw: Any) -> tuple[int, int, int] | None:
        """(running_count, free_slots, total_slots) from async_capacity.

        None on any non-conforming shape (older image without the action,
        or an error body): the caller treats it as occupancy unknown.
        A DRAINING replica reports predict_concurrency 0, so its free
        slots are naturally 0 without a special case.
        """
        if not isinstance(raw, dict):
            return None
        status = raw.get('status')
        if status is not None and status not in ('READY', 'DRAINING'):
            # The local router reports UNKNOWN when no child worker returned a
            # trustworthy sample. Its numeric zero is not an idle proof and
            # must not authorize logical scale-down.
            return None
        running = raw.get('running_count')
        concurrency = raw.get('predict_concurrency')
        if not isinstance(running, int) or isinstance(running, bool):
            return None
        if not isinstance(concurrency, int) or isinstance(concurrency, bool):
            return None
        if running < 0 or concurrency < 0:
            return None
        return running, max(0, concurrency - running), concurrency

    async def _fetch_replica_occupancy(
            self, session: 'aiohttp.ClientSession',
            replica_url: str) -> tuple[int, int, int] | None:
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
        """Serialize ordinary and HA-promotion occupancy probe rounds."""
        async with self._occupancy_probe_lock:
            await self._probe_replica_occupancy_once_unlocked()

    async def _probe_replica_occupancy_once_unlocked(self) -> None:
        """Run one probe round and age retained ready-backend samples.

        Occupancy-capable urls that LEFT the ready set (retiring or
        quarantined) are probed too: their async work must stay observable
        until they answer idle, or a retirement drain would see them as
        unknown forever and always wait out its full deadline. They fall
        out of the probe set once torn down (probe fails -> no occupancy
        -> pruned from the capable set below).
        """
        round_started_at = time.monotonic()
        connections_created = 0
        with self._client_pool_lock:
            ready_urls = list(self._routable_ready_urls_locked())
            probe_urls = list(
                set(ready_urls) | {
                    url for url in self._occupancy_capable
                    if self._system_recovery_route_is_available_locked(url)
                })
            probe_generation = {
                url: self._occupancy_dispatch_generation.get(url, 0)
                for url in probe_urls
            }
            probe_role_epoch = self._occupancy_role_epoch
        if not probe_urls:
            with self._client_pool_lock:
                self._replica_occupancy = {}
                self._replica_total_slots = {}
                self._replica_free_slots = {}
                self._occupancy_capable = set()
                self._occupancy_declared_urls = set()
                self._occupancy_disable_pending = set()
                self._occupancy_explicitly_disabled_urls = set()
                self._occupancy_dispatch_generation = {}
                self._occupancy_sample_generation = {}
                self._occupancy_sample_time = {}
                self._occupancy_current_round_sampled_urls = set()
                self._occupancy_sample_role_epoch = {}
                self._occupancy_pending_reservations = {}
                self._occupancy_active_attempts = {}
                self._occupancy_off_ready_since = {}
                self._occupancy_sampled_off_ready = set()
                self._last_occupancy_probe_time = time.monotonic()
                self._load_balancing_policy.set_occupancy({})
            await self._notify_request_queue()
            self._ha_stats().record_probe(total_seconds=time.monotonic() -
                                          round_started_at,
                                          attempted=0,
                                          succeeded=0,
                                          connections_created=0)
            return
        # aiohttp's default connector allows only 100 concurrent sockets. A
        # probe timeout covers the entire request, including time spent queued
        # for a connector slot, so replicas after the first 100 can time out
        # without ever being contacted on a large fleet. Match the connector
        # limit to this round's bounded controller-supplied fleet so every
        # replica gets the same timeout window.
        async def connection_created(*_args: Any) -> None:
            nonlocal connections_created
            connections_created += 1

        trace_config = aiohttp.TraceConfig()
        trace_config.on_connection_create_end.append(connection_created)
        # This probe's failures are swallowed by _fetch_replica_occupancy, so a
        # TLS mismatch here would not error -- occupancy would just go unknown
        # and concurrency-native autoscaling would quietly degrade. Configure it
        # from the same source as the proxy rather than leaving it to default.
        ssl_setting = replica_tls.aiohttp_ssl_setting()
        connector_kwargs: dict[str, Any] = {'limit': len(probe_urls)}
        if ssl_setting is not None:
            connector_kwargs['ssl'] = ssl_setting
        connector = aiohttp.TCPConnector(**connector_kwargs)
        async with aiohttp.ClientSession(connector=connector,
                                         trace_configs=[trace_config
                                                       ]) as session:
            results = await asyncio.gather(
                *(self._fetch_replica_occupancy(session, url)
                  for url in probe_urls))
        occupancy: dict[str, int] = {}
        total_slots: dict[str, int] = {}
        free_slots: dict[str, int] = {}
        for url, result in zip(probe_urls, results):
            if result is None:
                continue
            occupancy[url], free_slots[url], total_slots[url] = result
        # Off-ready urls must not advertise free slots to admission.
        ready_set = set(ready_urls)
        free_slots = {
            url: slots for url, slots in free_slots.items() if url in ready_set
        }
        with self._client_pool_lock:
            current_generation = self._occupancy_dispatch_generation
            active_attempts = self._occupancy_active_attempts
            valid_sample_urls = {
                url for url in occupancy
                if probe_generation.get(url, 0) == current_generation.get(
                    url, 0) and active_attempts.get(url, 0) == 0 and
                probe_role_epoch == self._occupancy_role_epoch
            }
            # Merge only generation-valid results. A probe that races a
            # dispatch must not erase the previous multi-slot baseline: every
            # post-baseline dispatch is already represented by a reservation,
            # so retaining that baseline and subtracting reservations remains
            # conservative while preserving the other slots. A probe miss
            # retains that baseline only through the bounded per-URL TTL.
            merged_occupancy = dict(self._replica_occupancy)
            merged_total_slots = dict(self._replica_total_slots)
            merged_free_slots = dict(self._replica_free_slots)
            merged_sample_generation = dict(self._occupancy_sample_generation)
            merged_sample_time = dict(self._occupancy_sample_time)
            merged_sample_roles = dict(self._occupancy_sample_role_epoch)
            pending = dict(self._occupancy_pending_reservations)
            missed_urls = set(probe_urls) - set(occupancy)
            now = time.monotonic()
            for url in missed_urls:
                sampled_at = merged_sample_time.get(url)
                sample_role = merged_sample_roles.get(url)
                sample_expired = (
                    url not in ready_set or sampled_at is None or
                    now - sampled_at
                    > constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS)
                role_changed = (sample_role is not None and
                                sample_role != self._occupancy_role_epoch)
                if sample_expired or role_changed:
                    merged_occupancy.pop(url, None)
                    merged_total_slots.pop(url, None)
                    merged_free_slots.pop(url, None)
                    merged_sample_generation.pop(url, None)
                    merged_sample_time.pop(url, None)
                    merged_sample_roles.pop(url, None)
            for url in valid_sample_urls:
                merged_occupancy[url] = occupancy[url]
                merged_total_slots[url] = total_slots[url]
                if url in free_slots:
                    merged_free_slots[url] = free_slots[url]
                else:
                    merged_free_slots.pop(url, None)
                merged_sample_generation[url] = probe_generation.get(url, 0)
                merged_sample_time[url] = now
                merged_sample_roles[url] = probe_role_epoch
                # A probe begun after the trailing fence observes every
                # accepted local-router reservation, so it becomes the new
                # authoritative baseline.
                pending.pop(url, None)
            # Off-ready both when the round STARTED and at write time: a
            # url re-added mid-round may have accepted work invisible to
            # its pre-re-add sample, so that sample cannot prove
            # post-retirement idleness if the url is retired again.
            current_ready = self._routable_ready_urls_locked()
            self._occupancy_sampled_off_ready = {
                url for url in valid_sample_urls
                if url not in ready_set and url not in current_ready
            }
            self._replica_occupancy = merged_occupancy
            self._replica_total_slots = {
                url: slots
                for url, slots in merged_total_slots.items()
                if url in ready_set and url in current_ready
            }
            self._replica_free_slots = {
                url: slots
                for url, slots in merged_free_slots.items()
                if url in ready_set and url in current_ready
            }
            self._occupancy_sample_generation = merged_sample_generation
            self._occupancy_sample_time = merged_sample_time
            self._occupancy_current_round_sampled_urls = set(valid_sample_urls)
            self._occupancy_sample_role_epoch = merged_sample_roles
            self._occupancy_pending_reservations = pending
            self._last_occupancy_probe_time = now
            # A url that EVER reported occupancy is occupancy-capable:
            # its envelope in-flight is meaningless for busyness
            # (fast-ack jobs close the envelope in ms), so a later probe
            # MISS for it must read as UNKNOWN -- never as the
            # envelope's explicit zero, which would let the drain paths
            # kill it mid-job (see _in_flight_with_draining). Pruned to
            # urls still relevant so the set stays bounded to the fleet.
            explicitly_disabled = set(self._occupancy_explicitly_disabled_urls)
            positive_disabled = {
                url for url in explicitly_disabled if occupancy.get(url, 0) > 0
            }
            capable = (self._occupancy_capable |
                       (set(occupancy) - explicitly_disabled) |
                       positive_disabled)
            disable_pending = (set(self._occupancy_disable_pending) |
                               positive_disabled)
            disable_ready = {
                url for url in disable_pending
                if url in valid_sample_urls and occupancy.get(url) == 0
            }
            capable -= disable_ready
            disable_pending -= disable_ready
            # An off-ready probe MISS is ambiguous: torn down, or
            # transiently unreachable with async work still running.
            # Retain the url (still probed, still reported as
            # occupancy-unknown) until it answers or the retention TTL
            # expires, so one dropped probe cannot convert 'unknown' into
            # 'absent = drained' for a retiring replica. The retirement
            # drain stays bounded by its own deadline regardless.
            now = time.monotonic()
            confirmed = set(ready_urls) | set(occupancy)
            off_ready_since = dict(self._occupancy_off_ready_since)
            for url in capable:
                if url in confirmed:
                    off_ready_since.pop(url, None)
                else:
                    off_ready_since.setdefault(url, now)
            retained = {
                url for url in capable
                if (now - off_ready_since.get(url, now) <=
                    constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS)
            }
            keep = (confirmed | set(self._draining_clients) | retained)
            self._replica_occupancy = {
                url: count
                for url, count in self._replica_occupancy.items()
                if url in keep
            }
            self._replica_total_slots = {
                url: slots
                for url, slots in self._replica_total_slots.items()
                if url in keep
            }
            self._replica_free_slots = {
                url: slots
                for url, slots in self._replica_free_slots.items()
                if url in keep
            }
            self._occupancy_capable = {url for url in capable if url in keep}
            self._occupancy_declared_urls = {
                url for url in self._occupancy_declared_urls if url in keep
            }
            self._occupancy_disable_pending = {
                url for url in disable_pending if url in keep
            }
            self._occupancy_explicitly_disabled_urls = {
                url for url in explicitly_disabled if url in keep
            }
            self._occupancy_dispatch_generation = {
                url: generation for url, generation in
                self._occupancy_dispatch_generation.items() if url in keep
            }
            self._occupancy_sample_generation = {
                url: generation for url, generation in
                self._occupancy_sample_generation.items() if url in keep
            }
            self._occupancy_sample_time = {
                url: sampled_at
                for url, sampled_at in self._occupancy_sample_time.items()
                if url in keep
            }
            self._occupancy_current_round_sampled_urls = {
                url for url in self._occupancy_current_round_sampled_urls
                if url in keep
            }
            self._occupancy_sample_role_epoch = {
                url: generation for url, generation in
                self._occupancy_sample_role_epoch.items() if url in keep
            }
            self._occupancy_pending_reservations = {
                url: count
                for url, count in self._occupancy_pending_reservations.items()
                if url in keep and count > 0
            }
            self._occupancy_active_attempts = {
                url: count
                for url, count in self._occupancy_active_attempts.items()
                if url in keep and count > 0
            }
            self._occupancy_off_ready_since = {
                url: ts
                for url, ts in off_ready_since.items()
                if url in self._occupancy_capable
            }
            # Push into the policy under the same lock the sync loop holds
            # for policy swaps; a policy swapped after this round serves
            # without occupancy for at most one probe interval.
            self._load_balancing_policy.set_occupancy(
                self._effective_occupancy_locked())
        await self._notify_request_queue()
        self._ha_stats().record_probe(total_seconds=time.monotonic() -
                                      round_started_at,
                                      attempted=len(probe_urls),
                                      succeeded=len(occupancy),
                                      connections_created=connections_created)

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
        num_ready_replicas: int | None = None
        capacity_hint = None
        service_version: int | None = None

        # Read the purpose-specific ring fresh for every sync. The primary is
        # tried first; overlap credentials are replayed only after a 401.
        sync_tokens = serve_utils.get_lb_sync_auth_tokens(required=True)

        # [boltz fork] Demand gauges ride alongside the timestamp
        # aggregator so the concurrency autoscaler sees outstanding work,
        # not just arrival compression. They are GAUGES -- re-read whole
        # every sync, never cleared on ack -- so a controller hiccup can
        # neither lose nor double-count demand; only the timestamps below
        # keep their existing clear-on-report semantics. The in-flight
        # map may be None (policy without load accounting): sent as-is,
        # the controller treats it as unknown rather than an idle fleet.
        # NOTE: gauges remain last-writer-wins per LB (unlike additive
        # timestamps). The controller compares the reporting Pod UID with the
        # complete live LB Pod set on every sync, so maxSurge overlap and
        # Kubernetes-query failure both suppress early drain proofs.
        in_flight, routing_urls, unknown_urls, occupancy_sampled_urls = (
            self._in_flight_with_draining())
        with self._client_pool_lock:
            sampled_set = set(occupancy_sampled_urls)
            total_slots_by_url = {
                url: int(slots)
                for url, slots in self._replica_total_slots.items()
                if url in sampled_set
            }
            occupancy_sample_generation = {
                url: int(generation) for url, generation in
                self._occupancy_sample_generation.items() if url in sampled_set
            }
        session_id = self._get_lb_session_id()
        async with aiohttp.ClientSession() as session:
            # Remove exactly the batch being sent BEFORE awaiting the
            # controller. Requests arriving during the await accumulate in the
            # now-empty aggregator for the next sync. A failed/cancelled send
            # restores this batch ahead of those newer arrivals in `finally`.
            request_batch = self._request_aggregator.drain()
            request_history = (
                self._request_aggregator.request_history_snapshot())
            request_classification_history = (
                self._request_aggregator.
                request_classification_history_snapshot())
            prediction_time_history = (
                self._request_aggregator.prediction_time_history_snapshot())
            request_batch_accepted = False
            sync_payload = {
                # Catalog/version fence for compatibility gauges. This is the
                # version of the routing spec already applied by this LB, not
                # the version the controller may return from this request.
                'routing_version': self._routing_version,
                'request_aggregator': request_batch,
                'request_history': request_history,
                'request_classification_history': request_classification_history,
                'prediction_time_history': prediction_time_history,
                'request_history_session_id': self._request_history_session_id,
                'in_flight': in_flight,
                'routing_urls': routing_urls,
                'unknown_in_flight_urls': unknown_urls,
                # Proof that a declared async replica's numeric entry includes
                # a valid occupancy sample. Old/first-sync LBs omit or send an
                # empty list, so a new controller fails closed instead of
                # mistaking an envelope zero for known-idle async occupancy.
                'occupancy_sampled_urls': occupancy_sampled_urls,
                'total_slots_by_url': total_slots_by_url,
                'occupancy_sample_generation': occupancy_sample_generation,
                'draining_urls': list(self._draining_clients),
                'lb_session_id': session_id,
                'queue_depth': self._queue_depth,
                'queued_requests_by_compatibility':
                    self._request_queue_profiles(),
                'rejected_requests_by_compatibility':
                    self._rejected_compatibility_profiles(),
                'queue_depth_by_priority':
                    self._queue_depth_priority_snapshot(),
                'rejected_in_window': self._rejected_in_window(),
                'rejected_in_recent_window': self._rejected_in_recent_window(),
                'rejected_in_window_by_priority': self._rejected_by_priority(),
                'rejected_in_recent_window_by_priority':
                    self._rejected_by_priority(recent=True),
                **self._offered_arrival_counts(),
            }
            try:
                # Send request information. Drain the aggregator once for the
                # entire credential sequence: a rejected primary must not
                # restore/re-drain the batch before the overlap retry.
                token_attempts: tuple[str | None,
                                      ...] = (sync_tokens if sync_tokens else
                                              (None,))
                for token_index, controller_token in enumerate(token_attempts):
                    sync_headers = {}
                    if controller_token is not None:
                        sync_headers['Authorization'] = (
                            f'Bearer {controller_token}')
                    if self._service_hash is not None:
                        sync_headers[constants.SERVICE_HASH_HEADER] = (
                            self._service_hash)
                    async with session.post(
                            self._controller_url +
                            constants.LB_CONTROLLER_SYNC_PATH,
                            json=sync_payload,
                            headers=sync_headers or None,
                            timeout=aiohttp.ClientTimeout(
                                constants.LB_CONTROLLER_SYNC_TIMEOUT_SECONDS),
                    ) as response:
                        if (getattr(response, 'status', None) == 401 and
                                token_index + 1 < len(token_attempts)):
                            continue
                        response.raise_for_status()
                        # A 2xx acknowledges this exact drained batch. Mark it
                        # accepted before decoding the response: the controller
                        # has already collected the timestamps even if its
                        # response body is malformed. The inverse
                        # partial-failure case (controller counted it but the LB
                        # never receives 2xx) restores and re-sends the batch,
                        # conservatively over-counting.
                        request_batch_accepted = True
                        response_json = await response.json()
                        self._set_queued_compatibility_demand_support(
                            response_json.get(
                                'queued_compatibility_demand_supported') is
                            True)
                        if response_json.get(
                                'request_history_accepted') is True:
                            self._request_aggregator.mark_request_history_accepted(
                                request_history)
                        classification_accepted = response_json.get(
                            'request_classification_history_accepted')
                        if classification_accepted is True:
                            (self._request_aggregator.
                             mark_request_classification_history_accepted(
                                 request_classification_history))
                        if response_json.get(
                                'prediction_time_history_accepted') is True:
                            self._request_aggregator.mark_prediction_time_history_accepted(
                                prediction_time_history)
                        replica_info = response_json.get('replica_info', {})
                        # Count of READY, active replicas the controller has,
                        # which can exceed len(replica_info) when endpoints are
                        # briefly unresolvable. None from an older controller
                        # that omits it.
                        num_ready_replicas = response_json.get(
                            'num_ready_replicas')
                        # [boltz fork] The controller ships the routing config
                        # (policy/target-qps/stream-timeout) alongside
                        # replica_info so `sky serve update` changes reach this
                        # LB without a re-roll. Older controllers omit the key;
                        # a warm LB keeps its last synced policy, while a cold
                        # LB keeps the safe defaults until a complete spec.
                        routing_spec = response_json.get('routing_spec')
                        # [boltz fork] Provisioning/target counts for the
                        # /_lb/capacity read; absent on older controllers.
                        capacity_hint = response_json.get('capacity_hint')
                        response_version = response_json.get('service_version')
                        if (isinstance(response_version, int) and
                                not isinstance(response_version, bool)):
                            service_version = response_version
                        ready_replica_urls = list(replica_info.keys())
                        break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f'An error occurred when syncing with '
                             f'the controller: {e}'
                             f'\nTraceback: {traceback.format_exc()}')
            else:
                if routing_spec is None:
                    # A successful response can temporarily omit the routing
                    # spec while an update's placeholder version is being
                    # committed. The controller has already accepted this
                    # sync's request batch, so do not restore/replay it. A cold
                    # LB, however, must not publish controller-supplied routes
                    # under constructor defaults; and a warm LB must keep its
                    # last coherent spec+route snapshot until a complete one
                    # arrives.
                    logger.warning(
                        'Controller sync omitted the routing spec; retaining '
                        'the last applied routes and readiness state.')
                    return
                if self._should_keep_ready_set_on_empty_sync(
                        ready_replica_urls, num_ready_replicas):
                    # Spurious empty sync: the controller still has READY
                    # replicas but none resolved this round. Keep the existing
                    # ready set rather than blanking it (which would 503 all
                    # live traffic), and try again next sync.
                    logger.warning(
                        f'Controller reported {num_ready_replicas} READY '
                        'replica(s) but no resolvable URLs this sync; keeping '
                        'the existing ready set instead of blanking it.')
                    # The capacity hint is independent of the ready set, so
                    # keep it fresh even while pinning (a spurious-empty sync
                    # is often a controller restart -- exactly when admission
                    # needs current provisioning/target counts).
                    self._capacity_hint = (capacity_hint if isinstance(
                        capacity_hint, dict) else None)
                    self._ready = True
                    self._last_sync_time = time.monotonic()
                    return
                with self._client_pool_lock:
                    # Apply the fetched routing spec BEFORE (re)setting the
                    # ready replicas: if the policy object was swapped, the
                    # set_ready_replicas below re-populates the fresh policy
                    # from this same sync.
                    if routing_spec:
                        self._apply_routing_spec(routing_spec)
                    invalid_marker_urls = (
                        self._replace_system_recovery_route_markers_locked(
                            replica_info))
                    # Declared capability is per replica/version. Seed it before
                    # the first probe so a failed/never-run probe is unknown,
                    # never an explicit idle zero. Retain prior declarations for
                    # off-ready bounded drains; the probe retention logic prunes
                    # them once no longer relevant.
                    declared_async_urls = {
                        url for url, info in replica_info.items()
                        if str(info.get('async_occupancy', '')).lower() ==
                        'true' and url not in invalid_marker_urls
                    }
                    explicitly_sync_urls = {
                        url for url, info in replica_info.items()
                        if str(info.get('async_occupancy', '')).lower() ==
                        'false' and url not in invalid_marker_urls
                    }
                    self._occupancy_declared_urls = (
                        self._occupancy_declared_urls - explicitly_sync_urls |
                        declared_async_urls)
                    previously_disabled = set(
                        self._occupancy_explicitly_disabled_urls)
                    newly_disabled = explicitly_sync_urls - previously_disabled
                    self._occupancy_explicitly_disabled_urls = (
                        previously_disabled |
                        explicitly_sync_urls) - declared_async_urls
                    disable_with_possible_work = {
                        url for url in explicitly_sync_urls
                        if (url in self._occupancy_capable or
                            self._replica_occupancy.get(url, 0) > 0)
                    } | newly_disabled
                    self._occupancy_disable_pending = (
                        self._occupancy_disable_pending |
                        disable_with_possible_work) - declared_async_urls
                    # A cold LB that first learns the replica after a
                    # service-only true->false version bump has no local
                    # evidence of the old async work. Treat the transition as
                    # capable until its first generation-valid zero; a miss is
                    # unknown and remains bounded by the retention/drain TTLs.
                    if newly_disabled:
                        self._occupancy_capable |= newly_disabled
                    if declared_async_urls:
                        self._occupancy_capable |= declared_async_urls
                    # Keep quarantined (locally-evicted) replicas out of
                    # routing even if the controller still lists them as
                    # ready, until their TTL expires -- otherwise a dead
                    # replica would be re-added on every sync and eviction
                    # would oscillate.
                    quarantined = self._quarantined_replicas()
                    routable = [
                        url for url in ready_replica_urls
                        if url not in quarantined and
                        url not in invalid_marker_urls
                    ]
                    self._replica_info_by_url = dict(replica_info)
                    self._load_balancing_policy.set_ready_replicas(routable)
                    # A re-added url voids any off-ready occupancy sample:
                    # work accepted after the re-add would be invisible in
                    # it, so it can no longer prove post-retirement
                    # idleness if the url is retired again.
                    if self._occupancy_sampled_off_ready:
                        self._occupancy_sampled_off_ready = (
                            self._occupancy_sampled_off_ready - set(routable))
                    # Set replica info for instance-aware policies
                    if isinstance(self._load_balancing_policy,
                                  lb_policies.InstanceAwareLeastLoadPolicy):
                        self._load_balancing_policy.set_replica_info(
                            replica_info)
                    # A routing-policy swap must inherit both the latest probe
                    # and any post-probe reservations immediately. Waiting for
                    # the next probe would make a freshly swapped least-load
                    # policy route as if every async worker were idle.
                    self._load_balancing_policy.set_occupancy(
                        self._effective_occupancy_locked())
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
                    client_urls = set(ready_replica_urls) - invalid_marker_urls
                    for replica_url in client_urls:
                        if replica_url not in self._client_pool:
                            self._client_pool[replica_url] = httpx.AsyncClient(
                                base_url=replica_url,
                                verify=self._replica_ssl_context())
                        self._client_generation_locked(
                            replica_url, self._client_pool[replica_url])
                    urls_to_close = set(self._client_pool.keys()) - client_urls
                    client_to_close = []
                    for replica_url in urls_to_close:
                        client_to_close.append(
                            (replica_url, self._client_pool.pop(replica_url)))
                        self._client_pool_generations.pop(replica_url, None)
                for replica_url, client in client_to_close:
                    # Fire-and-forget: a drain can legitimately take as long
                    # as the longest in-flight prediction; the sync loop must
                    # never wait on it. Strong refs held in the task set (a
                    # bare create_task result can be garbage collected).
                    # Registered in _draining_clients first so the demand
                    # feed keeps attributing the still-running work to the
                    # pruned url (see _in_flight_with_draining).
                    self._draining_clients.setdefault(replica_url,
                                                      []).append(client)
                    task = asyncio.create_task(
                        self._drain_and_close_client(replica_url, client))
                    self._client_close_tasks.add(task)
                    task.add_done_callback(self._client_close_tasks.discard)
                # Echo a version only after applying that same response's
                # routing spec and route/catalog snapshot.  In particular, a
                # spurious-empty response retains the previous coherent state
                # and must not advance this fence.
                self._routing_version = service_version
                # Cache the controller's capacity hint for /_lb/capacity.
                # Absence (older controller) resets to None rather than
                # keeping a stale previous value: readers must see
                # "unknown", not confidently-wrong counts.
                self._capacity_hint = (capacity_hint if isinstance(
                    capacity_hint, dict) else None)
                # First successful sync -> ready to serve (readiness gate).
                self._ready = True
                self._last_sync_time = time.monotonic()
                # Ready-replica count changes resize the dynamic queue and
                # dispatch limit. Wake waiters to re-evaluate immediately.
                await self._notify_request_queue()
            finally:
                if not request_batch_accepted:
                    self._request_aggregator.restore(request_batch)

    @staticmethod
    def _build_replica_ssl_context() -> ssl.SSLContext | bool | None:
        """Verification setting for replica connections, or None if plaintext.

        The mode and the certificate arrive in the same Helm-injected
        environment the controller uses when it mints and injects the replica
        key, so the two ends cannot disagree about whether replicas speak TLS.
        """
        mode = serve_utils.replica_tls_mode()
        if mode == constants.REPLICA_TLS_MODE_OFF:
            return None
        certificate_pem = os.environ.get(constants.REPLICA_TLS_CERT_ENV_VAR,
                                         '').strip()
        if mode == constants.REPLICA_TLS_MODE_PINNED and not certificate_pem:
            # Fail closed. Silently degrading to unverified TLS here would
            # present as encrypted while accepting any peer, which is the one
            # outcome an operator asking for pinning must never get.
            raise ValueError(
                f'{constants.REPLICA_TLS_MODE_ENV_VAR}='
                f'{constants.REPLICA_TLS_MODE_PINNED} requires '
                f'{constants.REPLICA_TLS_CERT_ENV_VAR} to carry the service '
                'certificate.')
        if mode == constants.REPLICA_TLS_MODE_UNVERIFIED:
            certificate_pem = ''
        return replica_tls.build_ssl_context(certificate_pem or None)

    def _replica_ssl_context(self) -> Any:
        """``verify=`` for a replica client; httpx ignores it on http URLs."""
        context = self._replica_ssl_context_cached
        # httpx rejects verify=None, and a plaintext base_url ignores the value
        # entirely, so fall back to the library default when TLS is off.
        return True if context is None else context

    @staticmethod
    def _release_client_refcount(client: httpx.AsyncClient) -> None:
        """Release one request and wake an active drain on the last one."""
        remaining = getattr(client, _INFLIGHT_ATTR, 1) - 1
        setattr(client, _INFLIGHT_ATTR, remaining)
        if remaining <= 0:
            zero_event = getattr(client, _INFLIGHT_ZERO_EVENT_ATTR, None)
            if isinstance(zero_event, asyncio.Event):
                zero_event.set()

    @staticmethod
    def _client_refcount_zero_event(client: httpx.AsyncClient) -> asyncio.Event:
        """Return a wakeup whose state reflects the authoritative counter."""
        zero_event = getattr(client, _INFLIGHT_ZERO_EVENT_ATTR, None)
        if not isinstance(zero_event, asyncio.Event):
            zero_event = asyncio.Event()
            setattr(client, _INFLIGHT_ZERO_EVENT_ATTR, zero_event)
        if getattr(client, _INFLIGHT_ATTR, 0) > 0:
            zero_event.clear()
        else:
            zero_event.set()
        return zero_event

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
        loop = asyncio.get_running_loop()
        deadline = (loop.time() + self._stream_timeout_seconds +
                    constants.LB_DRAIN_CLOSE_GRACE_SECONDS)
        while getattr(client, _INFLIGHT_ATTR, 0) > 0:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            zero_event = self._client_refcount_zero_event(client)
            try:
                await asyncio.wait_for(zero_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                break
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
            clients = self._draining_clients.get(url)
            if clients is not None:
                with contextlib.suppress(ValueError):
                    clients.remove(client)
                if not clients:
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
        failure_backoff = common_utils.Backoff(
            initial_backoff=1,
            max_backoff_factor=constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)

        while True:
            # Once draining, stop POSTing load_balancer_sync so the controller
            # stops counting this LB's request timestamps -- otherwise it would
            # double-count with the maxSurge replacement during a roll.
            if self._draining:
                logger.info('Draining: stopped syncing with the controller.')
                return
            try:
                await self._sync_with_controller_once()
                # A successful round ends the failure streak. A later outage
                # should retry promptly instead of inheriting a stale maximum
                # delay from an earlier incident.
                failure_backoff = common_utils.Backoff(
                    initial_backoff=1,
                    max_backoff_factor=(
                        constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS))
                await asyncio.sleep(
                    constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
            except Exception as e:  # pylint: disable=broad-except
                retry_delay = min(failure_backoff.current_backoff(),
                                  constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
                logger.error(f'An error occurred when syncing with '
                             f'the controller: {e}'
                             f'; retrying in {retry_delay:.1f}s'
                             f'\nTraceback: {traceback.format_exc()}')
                # Without a delay, a bad token, unavailable proxy, or failed
                # controller creates a CPU/network/log hot loop.
                await asyncio.sleep(retry_delay)

    async def _sync_system_recovery_route_lease_once(self) -> bool:
        """Fetch and apply one authenticated bounded route-lease heartbeat."""
        async with self._system_recovery_route_lease_heartbeat_lock:
            with self._client_pool_lock:
                sequence, request_started_at, marker_snapshot = (
                    self._begin_system_recovery_route_lease_heartbeat_locked())

            async def _request() -> object:
                sync_tokens = serve_utils.get_lb_sync_auth_tokens(required=True)
                token_attempts: tuple[str | None,
                                      ...] = (sync_tokens if sync_tokens else
                                              (None,))
                absolute_timeout = (
                    request_started_at +
                    constants.LB_SYSTEM_RECOVERY_LEASE_HEARTBEAT_TIMEOUT_SECONDS
                )
                async with aiohttp.ClientSession() as session:
                    for token_index, controller_token in enumerate(
                            token_attempts):
                        remaining = absolute_timeout - time.monotonic()
                        if remaining <= 0:
                            raise asyncio.TimeoutError
                        headers = {}
                        if controller_token is not None:
                            headers['Authorization'] = (
                                f'Bearer {controller_token}')
                        if self._service_hash is not None:
                            headers[constants.SERVICE_HASH_HEADER] = (
                                self._service_hash)
                        request_timeout = min(
                            remaining, constants.
                            LB_SYSTEM_RECOVERY_LEASE_PROXY_TIMEOUT_SECONDS)
                        async with session.post(
                                self._controller_url + constants.
                                LB_CONTROLLER_SYSTEM_RECOVERY_LEASE_PATH,
                                headers=headers or None,
                                timeout=aiohttp.ClientTimeout(
                                    total=request_timeout)) as response:
                            if (getattr(response, 'status', None) == 401 and
                                    token_index + 1 < len(token_attempts)):
                                continue
                            response.raise_for_status()
                            return await response.json()
                raise RuntimeError('No route-lease credential attempt ran.')

            remaining_total = (
                request_started_at +
                constants.LB_SYSTEM_RECOVERY_LEASE_HEARTBEAT_TIMEOUT_SECONDS -
                time.monotonic())
            if remaining_total <= 0:
                raise asyncio.TimeoutError
            payload = await asyncio.wait_for(_request(),
                                             timeout=remaining_total)
            changed = self._apply_system_recovery_route_lease_heartbeat(
                payload,
                sequence=sequence,
                request_started_at=request_started_at,
                marker_snapshot=marker_snapshot)
            # A first positive heartbeat may unblock an occupancy-aware queue.
            # Omission/expiry also needs a fresh dispatch-plan calculation.
            await self._notify_request_queue()
            return changed

    async def _sync_system_recovery_route_lease(self) -> None:
        """Nonoverlapping fixed-start two-second lease-heartbeat loop."""
        while not self._draining:
            round_started_at = time.monotonic()
            try:
                await self._sync_system_recovery_route_lease_once()
            except Exception as e:  # pylint: disable=broad-except
                # The last accepted deadlines remain authoritative and expire
                # locally.  A malformed response, auth failure, or timeout can
                # never refresh them, but must not kill future heartbeat rounds.
                logger.warning(
                    'System-recovery route-lease heartbeat failed: %s',
                    common_utils.format_exception(e))
            next_start = (
                round_started_at +
                constants.LB_SYSTEM_RECOVERY_LEASE_HEARTBEAT_INTERVAL_SECONDS)
            delay = max(0.0, next_start - time.monotonic())
            if self._draining:
                return
            await asyncio.sleep(delay)

    def _ha_role_payload(self) -> dict[str, Any]:
        """Build a non-additive occupancy and additive HTTP role report."""
        _, routing_urls, unknown_urls, sampled_urls = (
            self._in_flight_with_draining())
        with self._client_pool_lock:
            http_in_flight = self._load_balancing_policy.snapshot_in_flight()
            if http_in_flight is None:
                http_in_flight = {}
                unknown_urls = sorted(set(unknown_urls) | set(routing_urls))
            else:
                http_in_flight = dict(http_in_flight)
            for url, clients in self._draining_clients.items():
                count = sum(
                    getattr(client, _INFLIGHT_ATTR, 0) for client in clients)
                if count > 0:
                    http_in_flight[url] = http_in_flight.get(url, 0) + count
            sampled_set = set(sampled_urls)
            async_occupancy = {
                url: int(count)
                for url, count in self._replica_occupancy.items()
                if url in sampled_set
            }
            sample_generations = {
                url: int(generation) for url, generation in
                self._occupancy_sample_generation.items() if url in sampled_set
            }
            now = time.monotonic()
            sample_ages = {
                url: max(0.0, now - sampled_at)
                for url, sampled_at in self._occupancy_sample_time.items()
                if url in sampled_set
            }
        # Fail closed if a live observation has generation evidence but no
        # per-url sample timestamp. The controller rejects unequal key sets.
        reported_urls = (set(async_occupancy) | set(sample_generations) |
                         set(sample_ages))
        common_urls = (set(async_occupancy) & set(sample_generations) &
                       set(sample_ages))
        unknown_urls = sorted(set(unknown_urls) | (reported_urls - common_urls))
        async_occupancy = {url: async_occupancy[url] for url in common_urls}
        sample_generations = {
            url: sample_generations[url] for url in common_urls
        }
        sample_ages = {url: sample_ages[url] for url in common_urls}
        return {
            'lb_session_id': self._get_lb_session_id(),
            'lb_slot': self._lb_slot.value
                       if self._lb_slot is not None else None,
            'routing_version': self._routing_version,
            'armed_generation': self._armed_generation,
            # Echo only the role response already applied locally. The
            # controller uses this acknowledgement to prove that a former
            # active stopped admission before accepting its zero-work report.
            'applied_role': self._lb_role.value,
            'applied_generation': self._lb_role_generation,
            # Process-local admissions include queued, dispatching, and
            # streaming requests. Unlike backend async occupancy, this count
            # belongs to exactly one LB session and is the authoritative
            # bounded-drain signal for a former active slot.
            'local_in_flight': self._active_request_count,
            'http_in_flight': http_in_flight,
            'async_occupancy': async_occupancy,
            'occupancy_sample_generation': sample_generations,
            'occupancy_sample_age_seconds': sample_ages,
            'routing_urls': routing_urls,
            'unknown_in_flight_urls': unknown_urls,
            'draining_urls': list(self._draining_clients),
        }

    async def _sync_role_with_controller_once(self) -> None:
        if self._lb_slot is None:
            return
        payload = self._ha_role_payload()
        payload_bytes = len(json.dumps(payload).encode('utf-8'))
        started_at = time.monotonic()
        status_code: int | None = None
        outcome = lb_ha_obs.LbRoleOutcome.INVALID_RESPONSE.value
        controller_observation: dict[str, Any] | None = None
        sync_tokens = serve_utils.get_lb_sync_auth_tokens(required=True)
        token_attempts: tuple[str | None,
                              ...] = (sync_tokens if sync_tokens else (None,))
        try:
            async with aiohttp.ClientSession() as session:
                for token_index, controller_token in enumerate(token_attempts):
                    headers = {}
                    if controller_token is not None:
                        headers['Authorization'] = f'Bearer {controller_token}'
                    if self._service_hash is not None:
                        headers[constants.SERVICE_HASH_HEADER] = (
                            self._service_hash)
                    async with session.post(
                            self._controller_url +
                            constants.LB_CONTROLLER_ROLE_PATH,
                            json=payload,
                            headers=headers or None,
                            timeout=aiohttp.ClientTimeout(
                                constants.LB_ROLE_HEARTBEAT_TIMEOUT_SECONDS),
                    ) as response:
                        status_code = response.status
                        proxy_observation = None
                        response_headers = getattr(response, 'headers', {})
                        raw_proxy_observation = response_headers.get(
                            constants.LB_ROLE_PROXY_OBSERVABILITY_HEADER)
                        if raw_proxy_observation is not None:
                            try:
                                parsed_proxy_observation = json.loads(
                                    raw_proxy_observation)
                            except (TypeError, ValueError):
                                parsed_proxy_observation = None
                            if isinstance(parsed_proxy_observation, dict):
                                proxy_observation = parsed_proxy_observation
                        try:
                            body = await response.json()
                        except (aiohttp.ContentTypeError, json.JSONDecodeError,
                                ValueError):
                            body = None
                        if (response.status == 401 and
                                token_index + 1 < len(token_attempts)):
                            continue
                        if isinstance(body, dict):
                            body_outcome = body.get('outcome')
                            if isinstance(body_outcome, str):
                                outcome = body_outcome
                            controller_observation = {
                                'controller': body.get('observability'),
                                'proxy': (proxy_observation or
                                          body.get('proxy_observability')),
                            }
                        elif response.status < 400:
                            outcome = lb_ha_obs.LbRoleOutcome.INVALID_RESPONSE.value
                        if response.status >= 400:
                            if outcome == lb_ha_obs.LbRoleOutcome.INVALID_RESPONSE.value:
                                if response.status in (401, 403):
                                    outcome = (lb_ha_obs.LbRoleOutcome.
                                               HTTP_UNAUTHORIZED.value)
                                elif response.status == 409:
                                    outcome = (lb_ha_obs.LbRoleOutcome.
                                               HTTP_CONFLICT.value)
                                else:
                                    outcome = lb_ha_obs.LbRoleOutcome.HTTP_ERROR.value
                            response.raise_for_status()
                        if not isinstance(body, dict):
                            raise ValueError(
                                'Controller returned a non-JSON HA '
                                'role response.')
                        # Mixed-version controllers do not report an outcome;
                        # a validated 2xx role response is still a success.
                        if body.get('outcome') is None:
                            outcome = lb_ha_obs.LbRoleOutcome.SUCCESS.value
                        role: lb_ha.LbRole | None = None
                        try:
                            role = lb_ha.LbRole(body.get('role'))
                        except ValueError:
                            outcome = (
                                lb_ha_obs.LbRoleOutcome.INVALID_RESPONSE.value)
                            raise
                        assert role is not None
                        generation = body.get('generation')
                        if (not isinstance(generation, int) or
                                isinstance(generation, bool) or generation < 1):
                            outcome = (
                                lb_ha_obs.LbRoleOutcome.INVALID_RESPONSE.value)
                            raise ValueError(
                                'Controller returned an invalid HA cutover '
                                'generation.')
                        previous_role = self._lb_role
                        previous_generation = self._lb_role_generation
                        requires_fresh_occupancy = (
                            role in (lb_ha.LbRole.ARMED, lb_ha.LbRole.ACTIVE)
                            and (role is not previous_role or
                                 generation != previous_generation))
                        rollout_evidence = body.get('ha_rollout')
                        with self._client_pool_lock:
                            self._lb_role = role
                            self._lb_role_generation = generation
                            self._lb_ha_rollout_evidence = (
                                rollout_evidence if isinstance(
                                    rollout_evidence, dict) else None)
                            if role is lb_ha.LbRole.ARMED:
                                self._armed_generation = generation
                            else:
                                self._armed_generation = None
                            if requires_fresh_occupancy:
                                # A standby can probe while the old active
                                # accepts fast-ack work. Advancing the epoch and
                                # clearing samples under the admission lock
                                # prevents a crossing probe or request from
                                # reusing pre-transition free capacity.
                                self._occupancy_role_epoch += 1
                                self._invalidate_occupancy_samples_locked()
                        if requires_fresh_occupancy:
                            try:
                                await self._probe_replica_occupancy_once()
                            except Exception as e:  # pylint: disable=broad-except
                                # The cleared state is already fail-closed.
                                # Keep the committed role and let the ordinary
                                # supervised probe loop recover.
                                logger.warning(
                                    'Immediate post-promotion occupancy probe '
                                    'failed: '
                                    f'{common_utils.format_exception(e)}')
                        await self._notify_request_queue()
                        return
        except asyncio.TimeoutError:
            outcome = lb_ha_obs.LbRoleOutcome.CLIENT_TIMEOUT.value
            raise
        except aiohttp.ClientConnectionError:
            outcome = lb_ha_obs.LbRoleOutcome.CLIENT_CONNECTION_ERROR.value
            raise
        except aiohttp.ClientResponseError:
            raise
        except aiohttp.ClientError:
            outcome = lb_ha_obs.LbRoleOutcome.CLIENT_CONNECTION_ERROR.value
            raise
        finally:
            self._ha_stats().record_role(
                payload_bytes=payload_bytes,
                total_seconds=time.monotonic() - started_at,
                outcome=outcome,
                status_code=status_code,
                controller_observation=controller_observation)

    async def _sync_role_with_controller(self) -> None:
        """Keep slot roles current without coupling them to fleet resolution."""
        if self._lb_slot is None:
            return
        while not self._draining:
            try:
                await self._sync_role_with_controller_once()
            except Exception as e:  # pylint: disable=broad-except
                # Preserve the last committed local role across an API-server
                # restart. In particular, heartbeat loss alone must not demote
                # a healthy Service-selected active.
                logger.warning('HA role heartbeat failed; retaining role '
                               f'{self._lb_role.value}: '
                               f'{common_utils.format_exception(e)}')
            await asyncio.sleep(constants.LB_ROLE_HEARTBEAT_INTERVAL_SECONDS)

    async def _flush_request_history_on_drain(self) -> None:
        """Best-effort bounded history flush that cannot report demand."""
        request_history = self._request_aggregator.request_history_snapshot()
        request_classification_history = (
            self._request_aggregator.request_classification_history_snapshot())
        prediction_time_history = (
            self._request_aggregator.prediction_time_history_snapshot())
        if (request_history is None and
                not request_classification_history.get('buckets') and
                prediction_time_history is None):
            return
        try:
            sync_tokens = serve_utils.get_lb_sync_auth_tokens(required=True)
            session_id = self._get_lb_session_id()
            payload = {
                'request_history': request_history,
                'request_classification_history': request_classification_history,
                'prediction_time_history': prediction_time_history,
                'request_history_session_id': self._request_history_session_id,
                'lb_session_id': session_id,
            }
            async with aiohttp.ClientSession() as session:
                token_attempts: tuple[str | None,
                                      ...] = (sync_tokens if sync_tokens else
                                              (None,))
                for token_index, controller_token in enumerate(token_attempts):
                    headers = {}
                    if controller_token is not None:
                        headers['Authorization'] = (
                            f'Bearer {controller_token}')
                    if self._service_hash is not None:
                        headers[constants.SERVICE_HASH_HEADER] = (
                            self._service_hash)
                    async with session.post(
                            self._controller_url +
                            constants.LB_CONTROLLER_HISTORY_SYNC_PATH,
                            json=payload,
                            headers=headers or None,
                            timeout=aiohttp.ClientTimeout(
                                constants.LB_DRAIN_HISTORY_FLUSH_TIMEOUT_SECONDS
                            ),
                    ) as response:
                        if (getattr(response, 'status', None) == 401 and
                                token_index + 1 < len(token_attempts)):
                            continue
                        response.raise_for_status()
                        response_json = await response.json()
                        if response_json.get(
                                'request_history_accepted') is True:
                            self._request_aggregator.mark_request_history_accepted(
                                request_history)
                        classification_accepted = response_json.get(
                            'request_classification_history_accepted')
                        if classification_accepted is True:
                            (self._request_aggregator.
                             mark_request_classification_history_accepted(
                                 request_classification_history))
                        if response_json.get(
                                'prediction_time_history_accepted') is True:
                            self._request_aggregator.mark_prediction_time_history_accepted(
                                prediction_time_history)
                        return
        except Exception as e:  # pylint: disable=broad-except
            # Shutdown must remain bounded even when the controller, token
            # projection, or central database is unavailable.
            logger.warning('Failed to flush request history while draining: '
                           f'{common_utils.format_exception(e)}')

    async def _proxy_request_to(
            self, url: str,
            request: fastapi.Request) -> fastapi.responses.Response | Exception:
        """Proxy the request to the specified URL.

        Returns:
            The response from the endpoint replica. Return the exception
            encountered if anything goes wrong.
        """
        selected = vars(request).pop(_SELECTED_REPLICA_ATTR, None)
        if not isinstance(selected, _SelectedReplica) or selected.url != url:
            # Direct callers retain the historical facade.  Production retry
            # selection installs this snapshot in the same lock section as
            # policy selection; the fallback still captures before any body
            # await, but skips the selection-only ready-membership assertion.
            with self._client_pool_lock:
                selected = self._capture_selected_replica_locked(
                    url, require_current_route=False)
        # The token ties this request's release to the exact accounting
        # generation it incremented (see LoadBalancingPolicy hooks). Keep the
        # policy OBJECT too: a live routing-spec update may replace
        # self._load_balancing_policy while this request is awaiting headers or
        # streaming. Its release must go back to the owner that incremented it.
        slot_policy = self._load_balancing_policy
        slot_token = slot_policy.pre_execute_hook(url, request)
        # Every exit that does NOT hand a streaming response to the client
        # must release the in-flight slot itself, or failed/aborted attempts
        # permanently inflate this replica's load and skew routing away
        # from it (each retry then leaks another slot on another replica).
        released = False
        client = None
        client_refcount_dropped = False

        def _drop_client_refcount():
            nonlocal client_refcount_dropped
            if client_refcount_dropped or client is None:
                return
            client_refcount_dropped = True
            self._release_client_refcount(client)

        try:
            worker_url = httpx.URL(path=request.url.path,
                                   query=request.url.query.encode('utf-8'))
            request_body = await self._request_body(request)
            if self._request_has_stable_job_id(request):
                request_action = None
                is_async_request = True
            else:
                request_action = await self._request_action(
                    request, request_body)
                is_async_request = request_action in _ASYNC_ACTIONS
            # Body buffering/action decoding above may yield long enough for a
            # heavy sync, lease expiry, drain, or URL remove/re-add.  Recheck
            # every identity and increment the exact selected client in one
            # final critical section.  Transport below uses this object
            # directly and never substitutes a later URL lookup.
            with self._client_pool_lock:
                client = self._checkout_selected_replica_locked(selected)
            if client is None:
                return _PreDispatchError(
                    f'Client generation or route lease for {url} is no '
                    'longer available.')
            timeout_kwargs = {
                'connect': constants.LB_CONNECT_TIMEOUT_SECONDS,
            }
            if selected.route_marker is not None:
                timeout_kwargs['pool'] = (
                    constants.LB_SYSTEM_RECOVERY_POOL_TIMEOUT_SECONDS)
            proxy_request = client.build_request(
                request.method,
                worker_url,
                headers=self._headers_without_request_priority(request),
                content=request_body,
                # A scalar here would ALSO set the connect timeout: with a
                # long stream timeout (sync model servers send no bytes
                # until compute completes, so read must cover the whole
                # prediction), a dead-but-still-routed replica would hang
                # requests for the full value during the un-route window
                # instead of failing fast into the retry loop.
                timeout=httpx.Timeout(self._stream_timeout_seconds,
                                      **timeout_kwargs))
            prediction_started_at = (None
                                     if is_async_request else time.monotonic())
            proxy_response = await client.send(proxy_request, stream=True)

            if proxy_response.status_code in self._retriable_status_codes:
                # "Not now" from the replica (e.g. 503 while the model
                # warms, 429 shedding): discard and re-route. No byte has
                # reached the client — send() returns at headers with
                # stream=True. Slot + client refcount release via the
                # not-released finally below.
                await proxy_response.aclose()
                return _RetriableStatusError(proxy_response.status_code, url)

            if prediction_started_at is not None:
                outcome = ('succeeded'
                           if 200 <= proxy_response.status_code < 300 else
                           'failed')
                self._record_prediction_time(
                    time.monotonic() - prediction_started_at, outcome)

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
                    slot_policy.post_execute_hook(url, request, slot_token)
                    _drop_client_refcount()
                    await self._notify_request_queue()

            async def _stream_with_release():
                status_body = (
                    bytearray() if request_action == _ASYNC_ACTION_STATUS and
                    200 <= proxy_response.status_code < 300 else None)
                stream_completed = False
                try:
                    async for chunk in proxy_response.aiter_raw():
                        if status_body is not None:
                            if (len(status_body) + len(chunk) <=
                                    constants.LB_ASYNC_STATUS_BODY_MAX_BYTES):
                                status_body.extend(chunk)
                            else:
                                status_body = None
                        yield chunk
                    stream_completed = True
                finally:
                    try:
                        await _release_slot()
                    finally:
                        if stream_completed and status_body is not None:
                            try:
                                self._record_async_prediction_status(
                                    bytes(status_body),
                                    proxy_response.headers.get(
                                        'content-encoding', ''))
                            except Exception as e:  # pylint: disable=broad-except
                                logger.warning(
                                    'Failed to record async prediction-time '
                                    'history: %s', e)

            response = _ReleasingStreamingResponse(
                content=_stream_with_release(),
                status_code=proxy_response.status_code,
                headers=proxy_response.headers,
                background=background.BackgroundTask(_release_slot),
                release=_release_slot)
            # Starlette's mapping-based header initialization coalesces
            # duplicates. Preserve the upstream wire representation (notably
            # multiple Set-Cookie fields) after constructing the response.
            upstream_raw_headers = getattr(proxy_response.headers, 'raw', None)
            if upstream_raw_headers is not None:
                response.raw_headers = list(upstream_raw_headers)
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
                slot_policy.post_execute_hook(url, request, slot_token)
                # Only defined once the client was checked out; exits
                # before that (no client) have nothing to drop.
                if client is not None:
                    _drop_client_refcount()

    async def _proxy_with_retries(
            self, request: fastapi.Request) -> fastapi.responses.Response:
        """Try to proxy the request to the endpoint replica with retries."""
        if self._draining:
            # The readiness change needs time to propagate through the
            # Kubernetes Service/ingress. Reject requests that arrive in that
            # window instead of starting new work while this Pod terminates.
            raise self._draining_request_error()
        if not self._accepts_new_requests():
            raise self._inactive_role_request_error()
        priority = self._parse_request_priority(request)
        setattr(request, _REQUEST_PRIORITY_ATTR, priority)
        compatible_accelerators = self._parse_request_accelerators(request)
        setattr(request, _REQUEST_ACCELERATORS_ATTR, compatible_accelerators)
        self._record_offered_arrival(request)
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
        self._change_queue_depth(priority, 1)
        acquired_slot = False
        had_admission_slot = False
        body_cleanup_transferred = False
        try:
            # Cache the bounded body before the queue starts polling the ASGI
            # receive channel for disconnects. This also rejects oversized
            # work without occupying a queue slot.
            if self._request_queue_config is not None:
                await self._request_body(request)
            legacy_demand = (not self._queued_compatibility_demand_supported or
                             self._configured_accelerators is None)
            if legacy_demand:
                # Until capability negotiation succeeds, retain the old
                # pre-admission signal. This is required for a new LB rolled
                # out before (or rolled back onto) an old controller: the old
                # controller ignores the replaceable queue gauge. Services
                # without an exact-card catalog also retain aggregate arrival
                # scaling because their queue cannot publish card profiles.
                self._record_request_demand_once(request)
            acquired_slot = await self._acquire_request_slot(request)
            had_admission_slot = acquired_slot
            if acquired_slot:
                # The configured active-concurrency budget owns the body now.
                self._release_waiting_body_budget(request)
            # Draining may begin while admission awaits. Recheck immediately
            # before the inner handler records this arrival; there is no await
            # between this fence and RequestTimestamp.add(), so the shutdown
            # snapshot cannot miss a request that starts after draining.
            if self._draining:
                raise self._draining_request_error()
            if not self._accepts_new_requests():
                raise self._inactive_role_request_error()
            # Commit arrival/history only after admission and its final role
            # fence. Waiting work is reported separately as a live queue gauge,
            # so an empty compatible fleet can still launch without leaving a
            # phantom arrival behind when this LB drains during admission.
            self._record_request_demand_once(request)
            self._mark_request_classification_eligible(request)
            try:
                response = await self._proxy_with_retries_inner(request)
            finally:
                # A terminal rejection classifies itself before raising. Every
                # other return, exception, or cancellation after the final
                # admission fence is part of the non-rejected subset.
                self._record_request_classification_once(request,
                                                         rejected=False)
            if (acquired_slot and
                    isinstance(response, _ReleasingStreamingResponse)):
                response.hold_cleanup_until_complete(
                    lambda: self._release_request_slot(request))
                acquired_slot = False
            if (vars(request).get(_WAITING_REQUEST_BODY_BYTES_ATTR, 0) and
                    isinstance(response, _ReleasingStreamingResponse)):

                async def _release_body() -> None:
                    self._release_waiting_body_budget(request, drop_body=True)

                # A live update may disable queueing between body buffering and
                # admission. Keep that body's bytes charged until its streaming
                # response releases the underlying httpx request owner.
                response.hold_cleanup_until_complete(_release_body)
                body_cleanup_transferred = True
            return response
        finally:
            try:
                if acquired_slot:
                    await self._release_request_slot(request)
                elif not had_admission_slot:
                    # A live config update can enable occupancy-aware queueing
                    # after this request entered without an admission slot. A
                    # later rejected retry then transfers its per-URL
                    # reservation to the request-level marker. It has no
                    # streaming handoff or outer slot release to clear that
                    # marker, so clean it here.
                    with self._client_pool_lock:
                        released_admission = (
                            self._release_unassigned_occupancy_admission_locked(
                                request))
                    if released_admission:
                        await self._notify_request_queue()
            finally:
                try:
                    if not body_cleanup_transferred:
                        self._release_waiting_body_budget(request,
                                                          drop_body=True)
                finally:
                    # Admission notification is itself cancellable. The demand
                    # gauge must still balance on every exit.
                    self._change_queue_depth(priority, -1)

    async def _proxy_with_retries_inner(
            self, request: fastapi.Request) -> fastapi.responses.Response:
        """Retry loop body, bracketed by the queue-depth gauge above."""
        priority = getattr(request, _REQUEST_PRIORITY_ATTR,
                           constants.LB_REQUEST_PRIORITY_MIN)
        # TODO(tian): Finetune backoff parameters.
        backoff = common_utils.Backoff(
            initial_backoff=self._retry_initial_backoff_seconds)
        # SkyServe supports serving on Spot Instances. To avoid preemptions
        # during request handling, we add a retry here.
        retry_cnt = 0
        async_occupancy_request: bool | None = None
        # URLs that already failed THIS request: without exclusion,
        # least-load retries deterministically re-select a
        # dead-but-not-yet-pruned replica on a busy fleet (it sits at
        # load 0 while every healthy replica carries traffic).
        failed_urls: set[str] = set()

        def _unavailable(detail: str) -> fastapi.HTTPException:
            # Every terminal 503 means this job remains unplaced, including a
            # proven pre-dispatch failure. Retain it as demand so the
            # autoscaler keeps pressure on unavailable capacity instead of
            # letting the QPS window decay while the need persists.
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
            track_async_attempt = False
            if async_occupancy_request is None:
                with self._client_pool_lock:
                    if self._draining:
                        raise self._draining_request_error()
                    if not self._accepts_new_requests():
                        raise self._inactive_role_request_error()
                    queue_tracks_occupancy = self._queue_uses_async_occupancy()
                    declared_urls = set(self._occupancy_declared_urls)
                    routable_urls = self._routable_ready_urls_locked()
                    needs_async_inference = bool(
                        not queue_tracks_occupancy and
                        any(url not in declared_urls for url in routable_urls))
                if needs_async_inference:
                    # Parsing can await the request body. Do it before policy
                    # selection so stateful policies such as round-robin
                    # advance exactly once, and selection + reservation can
                    # then share one atomic critical section.
                    async_occupancy_request = (
                        await self._request_uses_async_occupancy(request))
            with self._client_pool_lock:
                # Every attempt owns a fresh role/drain fence.  An admitted
                # handler cannot select a replacement replica after this LB
                # begins draining or loses its serving role during backoff.
                if self._draining:
                    raise self._draining_request_error()
                if not self._accepts_new_requests():
                    raise self._inactive_role_request_error()
                queue_tracks_occupancy = self._queue_uses_async_occupancy()
                all_ready_urls = set(self._load_balancing_policy.ready_replicas)
                routable_urls = self._routable_ready_urls_locked()
                eligible_urls: set[str] | None = (
                    None if routable_urls == all_ready_urls else routable_urls)
                if queue_tracks_occupancy:
                    ready_urls = routable_urls
                    eligible_urls = {
                        url for url, slots in
                        self._effective_replica_free_slots_locked().items()
                        if url in ready_urls and slots > 0
                    }
                compatible_accelerators = getattr(request,
                                                  _REQUEST_ACCELERATORS_ATTR,
                                                  None)
                if compatible_accelerators is not None:
                    ready_urls = routable_urls
                    granted_accelerator = getattr(
                        request, _REQUEST_GRANTED_ACCELERATOR_ATTR, None)
                    requested_cards = ({granted_accelerator}
                                       if granted_accelerator is not None else
                                       set(compatible_accelerators))
                    compatible_urls = {
                        url for url in ready_urls
                        if self._replica_info_by_url.get(url, {}).get(
                            'gpu_type') in requested_cards
                    }
                    # A queue grant is a best-effort reservation against one
                    # current card. If that exact ready set changed before
                    # selection, retain the request's full compatibility set
                    # rather than returning a false 503.
                    if granted_accelerator is not None and not compatible_urls:
                        compatible_urls = {
                            url for url in ready_urls
                            if self._replica_info_by_url.get(url, {}).get(
                                'gpu_type') in set(compatible_accelerators)
                        }
                    eligible_urls = (compatible_urls if eligible_urls is None
                                     else eligible_urls & compatible_urls)
                if eligible_urls is None:
                    ready_replica_url = (
                        self._load_balancing_policy.select_replica(
                            request, exclude=failed_urls))
                else:
                    ready_replica_url = (
                        self._load_balancing_policy.select_replica(
                            request,
                            exclude=failed_urls,
                            eligible=eligible_urls))
                # Only the first attempt consumes the queue's card
                # reservation. Retries may use any compatible exact card.
                if getattr(request, _REQUEST_GRANTED_ACCELERATOR_ATTR,
                           None) is not None:
                    setattr(request, _REQUEST_GRANTED_ACCELERATOR_ATTR, None)
                if ready_replica_url is not None:
                    selected = self._capture_selected_replica_locked(
                        ready_replica_url, require_current_route=True)
                    vars(request)[_SELECTED_REPLICA_ATTR] = selected
                    occupancy_declared = (ready_replica_url
                                          in self._occupancy_declared_urls)
                    track_async_attempt = bool(queue_tracks_occupancy or
                                               occupancy_declared or
                                               async_occupancy_request)
                    if track_async_attempt:
                        # Selection and reservation share one critical section.
                        # No second request can select the same final free slot
                        # before this debit is visible.
                        self._begin_async_occupancy_attempt_locked(
                            ready_replica_url, request)
            if ready_replica_url is None:
                # Nothing to select at all: burning the remaining attempts
                # asleep only adds latency (and multiplies under the
                # client retry layer), so fail fast with a backoff hint.
                detail = ('No replica has confirmed free async capacity. '
                          if queue_tracks_occupancy else 'No ready replicas. ')
                raise _unavailable(detail +
                                   'Use "sky serve status [SERVICE_NAME]" '
                                   'to check the replica status.')
            retry_cnt += 1
            # Hand the unit off for the dispatch: the proxy await is accounted
            # by the policy's load_map (pre_execute_hook), and for synchronous
            # servers it lasts until compute completes. Re-taken in the finally
            # so a failed attempt is queued again while it backs off.
            response_or_exception = None
            attempt_error: BaseException | None = None
            self._change_queue_depth(priority, -1)
            try:
                response_or_exception = await self._proxy_request_to(
                    ready_replica_url, request)
            except BaseException as error:
                attempt_error = error
                raise
            finally:
                # Real proxy calls consume the snapshot at entry; monkeypatched
                # test/extension facades may not, so never let one attempt's
                # selection leak into the next retry.
                vars(request).pop(_SELECTED_REPLICA_ATTR, None)
                self._change_queue_depth(priority, 1)
                if track_async_attempt:
                    capacity_released = self._finish_async_occupancy_attempt(
                        ready_replica_url, response_or_exception
                        if response_or_exception is not None else attempt_error,
                        request if queue_tracks_occupancy else None)
                    if capacity_released:
                        await self._notify_request_queue()
            # Passively evict a replica that keeps failing with dead
            # connections during the controller-pause window.
            self._record_proxy_outcome(ready_replica_url, response_or_exception)
            if not isinstance(response_or_exception, Exception):
                # A prior terminal 503 represented this stable job as queued
                # demand. A 2xx means a replica accepted it, so that demand has
                # transitioned to occupancy/completion and must not remain in
                # the six-minute reject gauge as a second copy. A terminal
                # backend 4xx/5xx does not prove acceptance; clients may retry.
                if 200 <= response_or_exception.status_code < 300:
                    self._clear_rejection(request)
                return response_or_exception
            failed_urls.add(ready_replica_url)
            with self._client_pool_lock:
                all_ready_tried = failed_urls.issuperset(
                    self._routable_ready_urls_locked())
            # When the user aborts the request during streaming, the request
            # will be disconnected. We do not need to retry for this case.
            if await request.is_disconnected():
                # 499 means a client terminates the connection
                # before the server is able to respond.
                return fastapi.responses.Response(status_code=499)
            if not _can_retry_proxy_failure(request.method,
                                            response_or_exception):
                # A POST/PATCH may already have been accepted before a read,
                # write, timeout, or protocol failure became visible. Replaying
                # it can create a duplicate job. Surface the ambiguous outcome
                # immediately; callers that own an idempotency key can make
                # their own retry decision.
                exception = common_utils.remove_color(
                    common_utils.format_exception(response_or_exception,
                                                  use_bracket=True))
                raise fastapi.HTTPException(
                    status_code=502,
                    detail='Upstream outcome is unknown; the non-idempotent '
                    'request was not replayed. '
                    f'Last error encountered: {exception}.')
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
                if _is_definitely_not_dispatched(response_or_exception):
                    raise _unavailable(
                        'Request was not dispatched before the retry budget '
                        f'was exhausted. Last error: {response_or_exception}.')
                if isinstance(response_or_exception, _RetriableStatusError):
                    raise _unavailable(
                        'The retry budget was exhausted after configured '
                        'retriable replica responses. '
                        f'Last error: {response_or_exception}.')
                exception = common_utils.remove_color(
                    common_utils.format_exception(response_or_exception,
                                                  use_bracket=True))
                if isinstance(response_or_exception, httpx.RequestError):
                    raise fastapi.HTTPException(
                        status_code=502,
                        detail='Upstream outcome is unknown after the retry '
                        'budget was exhausted. '
                        f'Last error encountered: {exception}.')
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
        # Refuse to start before every enabled trust boundary is ready.
        # Data-plane authentication is optional; sync authentication is not.
        # Subsequent reads remain live and fail closed if an enabled projected
        # Secret becomes unreadable.
        serve_utils.get_lb_sync_auth_tokens(required=True)
        if serve_utils.is_lb_data_plane_auth_enabled():
            serve_utils.get_lb_auth_tokens(required=True)
        self._get_lb_session_id()
        # Gate inbound inference requests when data-plane auth is enabled.
        # Pure-ASGI so it wraps the catch-all proxy without buffering streaming
        # responses; exempts the readiness probe by method+path.
        self._app.add_middleware(_InboundAuthMiddleware)
        # Register the readiness route BEFORE the catch-all proxy route so it
        # is matched first (Starlette matches in registration order) instead of
        # being proxied to a replica.
        self._app.add_api_route(constants.LB_HEALTH_ENDPOINT_PATH,
                                self._health,
                                methods=['GET'])
        self._app.add_api_route(constants.LB_LIVENESS_ENDPOINT_PATH,
                                self._liveness,
                                methods=['GET'])
        # /_lb/capacity is a data-plane read for external admission systems, so
        # it stays behind the inbound bearer (unlike the readiness probe): the
        # same authenticated client that sends inference reads capacity.
        self._app.add_api_route('/_lb/capacity',
                                self._capacity,
                                methods=['GET'])
        self._app.add_api_route(
            constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
            self._prediction_completed,
            methods=['POST'])
        self._app.add_api_route('/{path:path}',
                                self._proxy_with_retries,
                                methods=['GET', 'POST', 'PUT', 'DELETE'])

        @self._app.on_event('startup')
        async def startup():
            # Configure logger
            uvicorn_access_logger = logging.getLogger('uvicorn.access')
            for handler in uvicorn_access_logger.handlers:
                handler.setFormatter(sky_logging.FORMATTER)

            self._start_background_loops()

        logger.info('SkyServe Load Balancer started on '
                    f'http://0.0.0.0:{self._load_balancer_port}. '
                    f'PID: {os.getpid()}')

        # Drain gracefully on SIGTERM (rolling update): _DrainableServer
        # deregisters + fails readiness immediately, then exits only after a
        # grace period so in-flight requests finish and k8s has pulled us from
        # the Service.
        config = uvicorn.Config(self._app,
                                host='0.0.0.0',
                                port=self._load_balancer_port,
                                **replica_tls.uvicorn_tls_kwargs())
        server = _DrainableServer(config, on_drain=self._begin_draining)
        asyncio.run(server.serve_with_drain())


def run_load_balancer(
    controller_addr: str,
    load_balancer_port: int,
    service_hash: str | None = None,
    lb_slot: str | None = None,
) -> None:
    """Run the load balancer.

    The routing spec is fetched exclusively from the controller sync channel.

    Args:
        controller_addr: The address of the controller.
        load_balancer_port: The port where the load balancer listens to.
        service_hash: Durable incarnation of the service this external LB may
            sync for.
    """
    load_balancer = SkyServeLoadBalancer(controller_url=controller_addr,
                                         load_balancer_port=load_balancer_port,
                                         service_hash=service_hash,
                                         lb_slot=lb_slot)
    load_balancer.run()


def _build_argument_parser() -> argparse.ArgumentParser:
    """Build the standalone (external) load balancer CLI parser.

    Routing configuration is deliberately absent: the LB fetches it from the
    controller sync channel. TLS terminates at the platform ingress, not in the
    per-service LB process.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--controller-addr',
                        required=True,
                        help='The address of the controller.')
    parser.add_argument('--load-balancer-port',
                        type=int,
                        required=True,
                        help='The port where the load balancer listens to.')
    parser.add_argument('--service-hash',
                        required=True,
                        help='The durable service incarnation to sync for.')
    parser.add_argument('--lb-slot',
                        choices=[slot.value for slot in lb_ha.LbSlot],
                        help='Immutable HA traffic slot (a or b).')
    return parser


def _resolve_launch_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    """Translate the external LB CLI's infrastructure arguments."""
    kwargs = dict(
        controller_addr=args.controller_addr,
        load_balancer_port=args.load_balancer_port,
        service_hash=args.service_hash,
    )
    if args.lb_slot is not None:
        kwargs['lb_slot'] = args.lb_slot
    return kwargs


if __name__ == '__main__':
    _parser = _build_argument_parser()
    run_load_balancer(**_resolve_launch_kwargs(_parser.parse_args()))
