"""Reserved-capacity fill poller.

[boltz fork] Opt-in (replica_policy.reserved_capacity_fill): a controller
background thread that measures FREE capacity on the service's zero-cost
locations (reserved/already-paid Kubernetes pools) and feeds the autoscaler
a snapshot via `collect_reserved_capacity`, so the fleet opportunistically
fills idle reserved GPUs. This module owns only the measurement side; the
target composition lives in `Autoscaler._apply_reserved_capacity_fill` and
the zero-cost-only launch pinning in `ReplicaManager._launch_replica`.

With a service_name the poller participates in the reserved-fill BROKER
(multi-service arbitration, sky/serve/reserved_capacity_broker.py): it
upserts a claim each cycle, lets the broker drive/read the shared per-pool
round (one cluster query per interval across ALL services), and feeds the
autoscaler the broker's feed + grant instead of a privately measured free
level. With a single live claim the broker's fast path reproduces the
standalone behavior exactly.
"""
from collections.abc import Callable
import dataclasses
import functools
import hashlib
import json
import math
import os
import re
import threading
import time
import typing
from typing import Any, Optional

from sky import sky_logging
from sky.adaptors import kubernetes
from sky.catalog import kubernetes_catalog
from sky.serve import constants
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import spot_placer as spot_placer_lib
from sky.utils import common_utils
from sky.utils import locks

if typing.TYPE_CHECKING:
    from sky.serve import autoscalers

logger = sky_logging.init_logger(__name__)


@dataclasses.dataclass(frozen=True)
class FreeGpuObservation:
    """One cached raw free-GPU value and the query's start time."""

    free_gpus: int | None
    snapshot_time: float | None


@dataclasses.dataclass(frozen=True)
class FillPoolCandidate:
    """One ordered Kubernetes-context group before physical resolution."""

    position: int
    context: str
    shapes: tuple[tuple[str, int], ...]
    locations: tuple['spot_placer_lib.Location', ...]

    @property
    def accelerator_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.shapes)

    @property
    def gpus_per_replica(self) -> int:
        widths = {count for _, count in self.shapes}
        if len(widths) != 1:
            raise ValueError('A fill pool requires one GPU count per replica; '
                             f'got {self.shapes!r}.')
        return next(iter(widths))


@dataclasses.dataclass(frozen=True)
class FillPoolSpec:
    """One resolved protocol-v2 pool edge in stable task-resource order."""

    position: int
    context: str
    shapes: tuple[tuple[str, int], ...]
    locations: tuple['spot_placer_lib.Location', ...]
    physical_cluster_uid: str
    pool_key: str
    legacy_pool_key: str

    @property
    def accelerator_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.shapes)

    @property
    def gpus_per_replica(self) -> int:
        widths = {count for _, count in self.shapes}
        if len(widths) != 1:
            raise ValueError('A fill pool requires one GPU count per replica; '
                             f'got {self.shapes!r}.')
        return next(iter(widths))


@dataclasses.dataclass(frozen=True)
class FillPoolBudgetInput:
    """One pool's inputs to the service-global budget partition."""

    holdings: int
    capacity_hint: int


@dataclasses.dataclass(frozen=True)
class FillPoolBudget:
    """One pool's partitioned broker cap and floor."""

    edge_cap: int
    edge_floor: int


def allocate_fill_pool_budgets(
    global_budget: int,
    service_floor: int,
    pools: tuple[FillPoolBudgetInput, ...],
) -> tuple[FillPoolBudget, ...]:
    """Partition one service budget over ordered physical pool edges.

    Existing holdings are retained first, clipped by the hard global budget.
    Residual budget is equal-weight water-filled up to each pool's capacity
    hint; integer remainder follows stable input order.  The service floor is
    then assigned in that same order without exceeding an edge cap.
    """
    if (isinstance(global_budget, bool) or not isinstance(global_budget, int) or
            global_budget < 0):
        raise ValueError('global_budget must be a nonnegative integer.')
    if (isinstance(service_floor, bool) or not isinstance(service_floor, int) or
            service_floor < 0):
        raise ValueError('service_floor must be a nonnegative integer.')
    for pool in pools:
        if (isinstance(pool.holdings, bool) or
                not isinstance(pool.holdings, int) or pool.holdings < 0 or
                isinstance(pool.capacity_hint, bool) or
                not isinstance(pool.capacity_hint, int) or
                pool.capacity_hint < 0):
            raise ValueError('Pool holdings and capacity hints must be '
                             'nonnegative integers.')

    caps: list[int] = []
    remaining = global_budget
    for pool in pools:
        retained = min(pool.holdings, remaining)
        caps.append(retained)
        remaining -= retained

    while remaining > 0:
        eligible = [
            index for index, pool in enumerate(pools)
            if caps[index] < max(pool.holdings, pool.capacity_hint)
        ]
        if not eligible:
            break
        share, remainder = divmod(remaining, len(eligible))
        allocated = 0
        for position, index in enumerate(eligible):
            requested = share + int(position < remainder)
            if requested == 0:
                continue
            limit = max(pools[index].holdings, pools[index].capacity_hint)
            give = min(requested, limit - caps[index])
            caps[index] += give
            allocated += give
        if allocated == 0:
            break
        remaining -= allocated

    floor_remaining = min(service_floor, global_budget)
    floors: list[int] = []
    for cap in caps:
        assigned = min(cap, floor_remaining)
        floors.append(assigned)
        floor_remaining -= assigned
    return tuple(
        FillPoolBudget(edge_cap=cap, edge_floor=floor)
        for cap, floor in zip(caps, floors))


_DEMAND_REFRESH_STATE_LOCK = threading.Lock()
_DEMAND_REFRESH_PENDING_CONTEXTS: set[str] = set()
_DEMAND_REFRESH_RUNNING = False

_PHYSICAL_CLUSTER_UID_CACHE_LOCK = threading.Lock()
# context -> (physical uid, expiry on the monotonic clock, lookup generation)
_PHYSICAL_CLUSTER_UID_CACHE: dict[str, tuple[str, float, int]] = {}
# A slow older request must not overwrite the cache result of a newer request.
_PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS: dict[str, int] = {}


def poll_interval_seconds() -> float:
    override = os.environ.get(constants.RESERVED_CAPACITY_POLL_INTERVAL_ENV_VAR)
    if override is not None:
        try:
            return max(1.0, float(override))
        except ValueError:
            logger.warning(
                f'Invalid {constants.RESERVED_CAPACITY_POLL_INTERVAL_ENV_VAR} '
                f'value {override!r}, using default '
                f'{constants.RESERVED_CAPACITY_POLL_INTERVAL_SECONDS}s.')
    return float(constants.RESERVED_CAPACITY_POLL_INTERVAL_SECONDS)


def get_kubernetes_physical_cluster_uid(
    context: str,
    *,
    force_refresh: bool = False,
) -> str | None:
    """Resolve a context to the physical cluster's kube-system UID.

    Successful reads are cached for at most one poll interval.  A forced read
    is used at launch time to fence a context that was retargeted after the
    broker observation.  Failures never fall back to an expired identity.
    """
    now = time.monotonic()
    with _PHYSICAL_CLUSTER_UID_CACHE_LOCK:
        cached = _PHYSICAL_CLUSTER_UID_CACHE.get(context)
        if (not force_refresh and cached is not None and now < cached[1]):
            return cached[0]
        if cached is not None and now >= cached[1]:
            _PHYSICAL_CLUSTER_UID_CACHE.pop(context, None)
        lookup_generation = (
            _PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS.get(context, 0) + 1)
        _PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS[context] = lookup_generation

    try:
        namespace = kubernetes.core_api(context).read_namespace(
            'kube-system', _request_timeout=kubernetes.API_TIMEOUT)
        metadata = getattr(namespace, 'metadata', None)
        raw_uid = getattr(metadata, 'uid', None)
        uid = raw_uid.strip() if isinstance(raw_uid, str) else ''
        if not uid:
            raise ValueError('kube-system namespace has no UID')
    except Exception as e:  # pylint: disable=broad-except
        with _PHYSICAL_CLUSTER_UID_CACHE_LOCK:
            if (_PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS.get(context) ==
                    lookup_generation):
                _PHYSICAL_CLUSTER_UID_CACHE.pop(context, None)
        logger.warning('Reserved-capacity physical-cluster identity lookup '
                       f'failed for context {context!r}: '
                       f'{common_utils.format_exception(e)}')
        return None

    expires_at = time.monotonic() + poll_interval_seconds()
    with _PHYSICAL_CLUSTER_UID_CACHE_LOCK:
        if (_PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS.get(context) ==
                lookup_generation):
            _PHYSICAL_CLUSTER_UID_CACHE[context] = (uid, expires_at,
                                                    lookup_generation)
            return uid

        # A newer lookup completed (or failed) while this request was in
        # flight.  Returning this request's now-stale UID would let a forced
        # launch-time check accept the identity that was current before a
        # context retarget, even though the newer observation already fenced
        # it.  Use only the newer generation's still-live cache entry; if it
        # has none, fail closed.
        current = _PHYSICAL_CLUSTER_UID_CACHE.get(context)
        if current is None:
            return None
        current_uid, current_expires_at, current_generation = current
        if (current_generation <= lookup_generation or
                time.monotonic() >= current_expires_at):
            if current_generation <= lookup_generation:
                return None
            _PHYSICAL_CLUSTER_UID_CACHE.pop(context, None)
            return None
        return current_uid


def group_zero_cost_fill_pools(
    zero_cost_locations: list['spot_placer_lib.Location'],
) -> tuple[FillPoolCandidate, ...]:
    """Group zero-cost Kubernetes locations by context in input order.

    Pool order is the first matching task-resource position. Accelerator names
    are canonicalized case-insensitively, while locations retain their input
    order for deterministic launch selection.  One physical context must use
    one positive whole GPU width; separate contexts may use different widths.
    """
    grouped: dict[str, dict[str, Any]] = {}
    for position, location in enumerate(zero_cost_locations):
        if str(location.cloud).lower() != 'kubernetes':
            continue
        if not location.accelerators:
            continue
        gpu_name, raw_count = next(iter(location.accelerators.items()))
        is_numeric = (not isinstance(raw_count, bool) and
                      isinstance(raw_count, (int, float)))
        is_finite = is_numeric and math.isfinite(float(raw_count))
        if (not is_finite or not float(raw_count).is_integer() or
                float(raw_count) < 1):
            raise ValueError('Reserved-fill capacity requires each '
                             'Kubernetes GPU count to be a positive whole '
                             f'number; got {gpu_name}:{raw_count!r}.')
        context = location.region
        if not isinstance(context, str) or not context:
            raise ValueError('Reserved-fill Kubernetes locations require a '
                             f'nonempty context; got {context!r}.')
        normalized_name = str(gpu_name).lower()
        if not normalized_name:
            raise ValueError('Reserved-fill Kubernetes locations require a '
                             'nonempty accelerator name.')
        exact_count = int(raw_count)
        group = grouped.setdefault(context, {
            'position': position,
            'shapes': {},
            'widths': set(),
            'locations': [],
        })
        group['shapes'][normalized_name] = exact_count
        group['widths'].add(exact_count)
        group['locations'].append(location)

    candidates: list[FillPoolCandidate] = []
    for context, group in grouped.items():
        widths = group['widths']
        if len(widths) != 1:
            raise ValueError('Reserved-fill capacity requires one GPU count '
                             'within each Kubernetes context; context '
                             f'{context!r} has widths {sorted(widths)}.')
        candidates.append(
            FillPoolCandidate(position=group['position'],
                              context=context,
                              shapes=tuple(sorted(group['shapes'].items())),
                              locations=tuple(group['locations'])))
    return tuple(candidates)


def resolve_fill_pool_specs(
    candidates: tuple[FillPoolCandidate, ...],) -> tuple[FillPoolSpec, ...]:
    """Resolve physical identities and reject later alias/overlap edges.

    A failed identity lookup removes only that candidate.  For aliases that
    resolve to the same physical cluster and overlap in accelerator names, the
    first task-resource position survives deterministically.
    """
    resolved: list[FillPoolSpec] = []
    physical_accelerators: dict[str, set[str]] = {}
    for candidate in candidates:
        physical_uid = get_kubernetes_physical_cluster_uid(candidate.context)
        if physical_uid is None:
            logger.error('Reserved-fill pool edge for context '
                         f'{candidate.context!r} is inactive because its '
                         'physical cluster identity could not be resolved.')
            continue
        accelerator_names = candidate.accelerator_names
        prior_names = physical_accelerators.setdefault(physical_uid, set())
        overlap = prior_names.intersection(accelerator_names)
        if overlap:
            logger.error(
                'Reserved-fill pool edge for context '
                f'{candidate.context!r} overlaps an earlier context alias '
                f'on physical cluster {physical_uid!r} for accelerators '
                f'{sorted(overlap)}; keeping the first task-resource edge.')
            continue
        prior_names.update(accelerator_names)
        pool_key = reserved_capacity_broker.make_pool_key(
            candidate.context,
            accelerator_names,
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid=physical_uid)
        legacy_pool_key = reserved_capacity_broker.make_pool_key(
            candidate.context, accelerator_names)
        resolved.append(
            FillPoolSpec(position=candidate.position,
                         context=candidate.context,
                         shapes=candidate.shapes,
                         locations=candidate.locations,
                         physical_cluster_uid=physical_uid,
                         pool_key=pool_key,
                         legacy_pool_key=legacy_pool_key))
    return tuple(resolved)


def discover_fill_pool_specs(
    zero_cost_locations: list['spot_placer_lib.Location'],
) -> tuple[FillPoolSpec, ...]:
    """Build the ordered, physically resolved protocol-v2 pool set."""
    return resolve_fill_pool_specs(
        group_zero_cost_fill_pools(zero_cost_locations))


def zero_cost_pool_shapes(
    zero_cost_locations: list['spot_placer_lib.Location']
) -> dict[tuple[str, str], int]:
    """Per-(context, gpu) pool shapes of the zero-cost location set.

    Pure spec parsing (no cluster query). Rules:
    - Only Kubernetes locations are queryable in v0/v1; other zero-cost
      locations contribute no pool.
    - Same (context, gpu) shape enumerated with different per-replica
      counts (e.g. A100:1 and A100:8 entries over one pool) draws from
      the same free GPUs: count the key once with the LARGEST
      per-replica size -- deterministic and conservative (fewest fill
      launches). A first-seen-wins dedupe would let any_of entry ORDER
      change the fill level.
    - Lowercased gpu name: the realtime query matches
      case-insensitively, so 'A100' and 'a100' entries hit the same
      pool and must dedupe to one key.
    """
    per_key_replica_size: dict[tuple[str, str], int] = {}
    for location in zero_cost_locations:
        if str(location.cloud).lower() != 'kubernetes':
            continue
        if not location.accelerators:
            continue
        gpu_name, per_replica = next(iter(location.accelerators.items()))
        is_numeric = (not isinstance(per_replica, bool) and
                      isinstance(per_replica, (int, float)))
        is_finite = is_numeric and math.isfinite(float(per_replica))
        if (not is_finite or not float(per_replica).is_integer() or
                float(per_replica) < 1):
            logger.error('Reserved-fill capacity has an invalid Kubernetes '
                         f'GPU shape {gpu_name}:{per_replica!r}; each count '
                         'must be a positive whole number. Fill is inactive '
                         'for this service.')
            return {}
        exact_per_replica = int(per_replica)
        key = (location.region, gpu_name.lower())
        per_key_replica_size[key] = max(per_key_replica_size.get(key, 1),
                                        exact_per_replica)
    return per_key_replica_size


def query_pool_observation(
        context: str, gpu_name: str,
        per_replica: int) -> reserved_capacity_broker.PoolObservation:
    """Realtime free-slot measurement of one (context, gpu) pool.

    EXPENSIVE: the realtime availability query lists every pod in the
    cluster and is deliberately uncached -- call it ONLY from the poller
    thread (or the broker round it drives), never from the autoscaler
    decision tick.

    Unknown availability (any negative count, e.g. a swallowed pod-list
    403 surfacing as {'A100': -1}) is a MEASUREMENT BLACKOUT
    (free_slots=None), exactly like a raised query error: converting it
    to an authoritative 0 would let a new claimant or weight change
    redistribute grants and drain existing holdings while availability is
    unknown -- precisely what the broker's blackout semantics prohibit.
    (Single-claimant observable behavior is unchanged: a blackout feeds 0,
    same as a 0 measurement.) A FAILED/unknown query is distinct from a
    successful 0 (full pool). gpu_names carries the canonical accelerator
    names the query saw, the broker's phantom-pool signal (empty = the
    claimed GPU resolves to no labeled nodes).
    """
    try:
        _, _, available = kubernetes_catalog.list_accelerators_realtime(
            gpus_only=True,
            name_filter=f'^{re.escape(gpu_name)}$',
            region_filter=context,
            quantity_filter=None,
            case_sensitive=False,
            require_price=False)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Reserved-capacity poll failed for context '
                       f'{context!r} gpu {gpu_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return reserved_capacity_broker.PoolObservation(free_slots=None)
    if any(count < 0 for count in available.values()):
        logger.warning('Reserved-capacity poll: availability unknown for '
                       f'context {context!r} gpu {gpu_name!r} '
                       f'({available}); treating as a measurement blackout.')
        return reserved_capacity_broker.PoolObservation(free_slots=None,
                                                        gpu_names=tuple(
                                                            available.keys()))
    free_gpus = sum(count for count in available.values() if count > 0)
    free_slots = free_gpus // max(1, per_replica)
    return reserved_capacity_broker.PoolObservation(
        free_slots=free_slots,
        gpu_names=tuple(available.keys()),
        free_slots_by_accelerator=((gpu_name.lower(), free_slots),))


def query_pool_group_observation(
    context: str,
    shapes: dict[str, int],
) -> reserved_capacity_broker.PoolObservation:
    """Measure several accelerator names in one Kubernetes context query."""
    try:
        _, _, available = kubernetes_catalog.list_accelerators_realtime(
            gpus_only=True,
            name_filter=None,
            region_filter=context,
            quantity_filter=None,
            case_sensitive=False,
            require_price=False)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Reserved-capacity group poll failed for context '
                       f'{context!r}: {common_utils.format_exception(e)}')
        return reserved_capacity_broker.PoolObservation(free_slots=None)
    available_lower = {
        str(gpu_name).lower(): count for gpu_name, count in available.items()
    }
    requested_counts = [available_lower.get(name, 0) for name in shapes]
    if any(count < 0 for count in requested_counts):
        logger.warning('Reserved-capacity group availability is unknown for '
                       f'context {context!r} ({available}).')
        return reserved_capacity_broker.PoolObservation(free_slots=None,
                                                        gpu_names=tuple(
                                                            available.keys()))
    free_slots_by_accelerator = tuple(
        (name, max(0, available_lower.get(name, 0)) // per_replica)
        for name, per_replica in shapes.items())
    free_slots = sum(count for _, count in free_slots_by_accelerator)
    matched_names = tuple(
        name for name in available if str(name).lower() in shapes)
    return reserved_capacity_broker.PoolObservation(
        free_slots=free_slots,
        gpu_names=matched_names,
        free_slots_by_accelerator=(free_slots_by_accelerator))


def query_free_slots(
        zero_cost_locations: list['spot_placer_lib.Location']) -> int:
    """Free replica slots across the zero-cost locations, summed per shape.

    Standalone (non-broker) measurement: shapes are assumed to map to
    disjoint node pools (v0; overlapping pools would double-count and are
    explicitly out of scope). A failed context contributes 0 this cycle;
    the autoscaler's staleness decay handles a persistently failing
    poller.
    """
    total = 0
    for (context, gpu_name
        ), per_replica in zero_cost_pool_shapes(zero_cost_locations).items():
        observation = query_pool_observation(context, gpu_name, per_replica)
        if observation.free_slots is not None:
            total += observation.free_slots
    return total


def query_free_slots_by_context(
    zero_cost_locations: list['spot_placer_lib.Location']
) -> dict[str, int | None]:
    """Measure free replica slots with one cluster query per context.

    Demand placement can contain several Kubernetes accelerator shapes in the
    same context.  Calling :func:`query_pool_observation` once per shape would
    repeat the expensive cluster-wide pod listing for every shape.  Fetch all
    accelerator availability in a context once, then project that snapshot
    onto the shapes the placer can actually use.

    ``None`` means the context could not be measured.  A missing accelerator
    key is a successful zero-capacity observation, while a negative value is
    the catalog's explicit unknown-availability sentinel.
    """
    shapes_by_context: dict[str, dict[str, int]] = {}
    for (context, gpu_name
        ), per_replica in zero_cost_pool_shapes(zero_cost_locations).items():
        shapes_by_context.setdefault(context, {})[gpu_name] = per_replica

    result: dict[str, int | None] = {}
    for context, shapes in shapes_by_context.items():
        try:
            _, _, available = kubernetes_catalog.list_accelerators_realtime(
                gpus_only=True,
                name_filter=None,
                region_filter=context,
                quantity_filter=None,
                case_sensitive=False,
                require_price=False)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Zero-cost demand capacity query failed for '
                           f'context {context!r}: '
                           f'{common_utils.format_exception(e)}')
            result[context] = None
            continue

        available_lower = {
            str(gpu_name).lower(): count
            for gpu_name, count in available.items()
        }
        requested_counts = [
            available_lower.get(gpu_name, 0) for gpu_name in shapes
        ]
        if any(count < 0 for count in requested_counts):
            logger.warning('Zero-cost demand capacity is unknown for '
                           f'context {context!r} ({available}).')
            result[context] = None
            continue
        result[context] = sum(
            max(0, available_lower.get(gpu_name, 0)) // per_replica
            for gpu_name, per_replica in shapes.items())
    return result


def _observation_is_fresh(row: dict[str, Any] | None, now: float) -> bool:
    return (row is not None and
            now - float(row['completed_at']) <= poll_interval_seconds())


def _refresh_demand_capacity_contexts(contexts: set[str]) -> None:
    """Refresh stale context rows under one cross-controller query lock."""
    try:
        lock = locks.get_lock(constants.DEMAND_CAPACITY_REFRESH_LOCK_ID)
        with lock.acquire(blocking=False):
            now = time.time()
            rows = serve_state.get_demand_capacity_observations(contexts)
            for context in sorted(contexts):
                if _observation_is_fresh(rows.get(context), now):
                    continue
                # Capture before the expensive query. A replica row created
                # during it is debited from the cached result by the planner.
                snapshot_time = time.time()
                availability: dict[str, int] | None
                try:
                    _, _, available = (
                        kubernetes_catalog.list_accelerators_realtime(
                            gpus_only=True,
                            name_filter=None,
                            region_filter=context,
                            quantity_filter=None,
                            case_sensitive=False,
                            require_price=False))
                    availability = {
                        str(gpu_name).lower(): int(count)
                        for gpu_name, count in available.items()
                    }
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        'Shared demand-capacity query failed for context '
                        f'{context!r}: {common_utils.format_exception(e)}')
                    availability = None
                serve_state.upsert_demand_capacity_observation(
                    context, snapshot_time, time.time(), availability)
    except locks.LockTimeout:
        # Another controller is already producing the shared observation.
        # The next reconciliation tick will consume its durable result.
        return


def _demand_capacity_refresh_worker() -> None:
    global _DEMAND_REFRESH_RUNNING
    while True:
        with _DEMAND_REFRESH_STATE_LOCK:
            contexts = set(_DEMAND_REFRESH_PENDING_CONTEXTS)
            _DEMAND_REFRESH_PENDING_CONTEXTS.clear()
            if not contexts:
                _DEMAND_REFRESH_RUNNING = False
                return
        try:
            _refresh_demand_capacity_contexts(contexts)
        except Exception as e:  # pylint: disable=broad-except
            logger.error('Shared demand-capacity refresh failed: '
                         f'{common_utils.format_exception(e)}')


def _schedule_demand_capacity_refresh(contexts: set[str]) -> None:
    """Coalesce refresh work without issuing provider calls on the caller."""
    global _DEMAND_REFRESH_RUNNING
    if not contexts:
        return
    with _DEMAND_REFRESH_STATE_LOCK:
        _DEMAND_REFRESH_PENDING_CONTEXTS.update(contexts)
        if _DEMAND_REFRESH_RUNNING:
            return
        _DEMAND_REFRESH_RUNNING = True
    worker = threading.Thread(target=_demand_capacity_refresh_worker,
                              name='serve-demand-capacity-refresh',
                              daemon=True)
    try:
        worker.start()
    except RuntimeError as e:
        # Thread.start() can fail under transient process-wide thread
        # exhaustion. No worker exists to release this reservation, so make
        # the pending contexts retryable by the next reconciliation tick.
        with _DEMAND_REFRESH_STATE_LOCK:
            _DEMAND_REFRESH_RUNNING = False
        logger.error('Failed to start shared demand-capacity refresh worker: '
                     f'{common_utils.format_exception(e)}')


def get_cached_free_gpus_by_pool(
    zero_cost_locations: list['spot_placer_lib.Location']
) -> dict[tuple[str, str], FreeGpuObservation]:
    """Read shared raw free GPUs and asynchronously refresh stale contexts.

    This function performs only one batched database read on the reconciliation
    path. Kubernetes/provider calls run in a coalesced daemon worker and are
    serialized across controller processes by a distributed lock.
    """
    pool_keys = set(zero_cost_pool_shapes(zero_cost_locations))
    contexts = {context for context, _ in pool_keys}
    rows = serve_state.get_demand_capacity_observations(contexts)
    now = time.time()
    stale_contexts = {
        context for context in contexts
        if not _observation_is_fresh(rows.get(context), now)
    }
    _schedule_demand_capacity_refresh(stale_contexts)

    observations: dict[tuple[str, str], FreeGpuObservation] = {}
    for context, gpu_name in pool_keys:
        row = rows.get(context)
        if context in stale_contexts or row is None:
            observations[(context, gpu_name)] = FreeGpuObservation(None, None)
            continue
        availability_json = row['availability']
        if availability_json is None:
            free_gpus = None
        else:
            availability = json.loads(availability_json)
            count = int(availability.get(gpu_name, 0))
            free_gpus = None if count < 0 else max(0, count)
        observations[(context, gpu_name)] = FreeGpuObservation(
            free_gpus, float(row['snapshot_time']))
    return observations


def _standalone_cycle(autoscaler: 'autoscalers.Autoscaler',
                      zero_cost: list['spot_placer_lib.Location'],
                      keys: list[dict[str, Any]]) -> None:
    """Pre-broker measurement cycle: private query, no arbitration."""
    # Snapshot time is captured BEFORE the (slow, cluster-wide)
    # availability query: a zero-cost replica row created while the query
    # runs already occupies a slot the query may still have counted free,
    # and the post-snapshot debit (created_at > snapshot_time) only
    # catches it if the snapshot predates the row.
    snapshot_time = time.time()
    free_slots = query_free_slots(zero_cost)
    autoscaler.collect_reserved_capacity(free_slots, keys, snapshot_time)
    logger.info(f'Reserved-capacity poll: {free_slots} free '
                f'slot(s) across {len(keys)} zero-cost '
                'location(s).')


def _placer_can_launch_zero_cost(placer: 'spot_placer_lib.SpotPlacer') -> bool:
    """Whether any zero-cost location is effectively ACTIVE (not benched)."""
    active = placer.active_locations()
    return any(location in active for location in placer.zero_cost_locations())


def _broker_cycle(
    autoscaler: 'autoscalers.Autoscaler',
    placer: 'spot_placer_lib.SpotPlacer',
    service_name: str,
    zero_cost: list['spot_placer_lib.Location'],
    keys: list[dict[str, Any]],
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> None:
    """Broker-arbitrated cycle: claim heartbeat -> round -> feed+grant."""
    fence_kwargs: dict[str, Any] = {}
    if expected_service_hash is not None:
        fence_kwargs['expected_service_hash'] = expected_service_hash
    if expected_controller_owner is not None:
        fence_kwargs['expected_controller_owner'] = expected_controller_owner
    shapes = zero_cost_pool_shapes(zero_cost)
    contexts = {context for context, _ in shapes}
    per_replica_counts = set(shapes.values())
    logical_slot_mismatch = (isinstance(
        placer, spot_placer_lib.CapacityAwareDynamicFallbackSpotPlacer) and
                             per_replica_counts != {1})
    if (len(contexts) != 1 or len(per_replica_counts) != 1 or
            logical_slot_mismatch):
        logger.error(
            'Reserved-fill broker: zero-cost shapes must share one context '
            'and GPU count per backend, and logical services require exact '
            f'one-GPU shapes; got {sorted(shapes.items())}. Fill is inactive '
            'for this service.')
        reserved_capacity_broker.remove_claim(service_name, **fence_kwargs)
        autoscaler.collect_reserved_capacity(0, keys, time.time())
        return
    context = next(iter(contexts))
    per_replica = next(iter(per_replica_counts))
    grouped_shapes = {
        gpu_name: count
        for (shape_context, gpu_name), count in shapes.items()
        if shape_context == context
    }
    pool_key = reserved_capacity_broker.make_pool_key(context,
                                                      tuple(grouped_shapes))
    replica_infos = serve_state.get_replica_infos(service_name)
    # Seed before counting (idempotent no-op when already seeded): after a
    # respawn whose best-effort boot seed failed, an unseeded autoscaler
    # counts zero holdings, and that under-report reaches the broker as a
    # holdings SHRINK -- bypassing the two-round down-damping and cutting
    # peers' grants on a pure reporting artifact.
    autoscaler.seed_zero_cost_locations(keys)
    # Only the FILL count reaches the claim: demand-placed rows are exempt
    # from the ceiling and the broker never reads them.
    holdings_fill, _ = autoscaler.count_zero_cost_holdings(replica_infos)
    floor = autoscaler.reserved_fill_floor_replicas
    # Real capacity cap this claimant can materialize right now: fill rides
    # ABOVE the demand target, so anything past max_replicas - demand_target
    # is phantom capacity. The broker clamps the effective floor, the
    # headroom (share above the floor, derived at allocation time) and the
    # feed need by it -- otherwise an unattainable floor permanently
    # absorbs entitlement and feed the service never launches.
    effective_cap = max(
        0, autoscaler.max_replicas - autoscaler.get_final_target_num_replicas())
    # Utilization signal for the release governor. Sampled HERE rather than
    # inside the decision tick's request-information path, which early
    # returns without a report, does not run when the controller is not
    # demand-authoritative, and stamps the freshness timestamp itself (so a
    # freshness check evaluated beside it would be vacuously true). This
    # cycle runs unconditionally every poll interval and holds a live
    # reference to the very autoscaler the decision tick uses.
    activity: dict[str, Any] | None = None
    if autoscaler.reserved_fill_utilization_gate:
        sample = autoscaler.fill_demand_sample(replica_infos)
        # A current gated writer publishes activity_ts every round. A missing
        # detailed sample carries NULL need so the broker can distinguish
        # armed-but-blind (freeze, then bounded blind-grace decay) from the
        # all-NULL explicit utilization_gate:false opt-out.
        activity = {
            'demonstrated_need':
                (None if sample is None else sample.demonstrated_need()),
            'boot_hold': False if sample is None else sample.boot_hold(),
        }
    claim_persisted = reserved_capacity_broker.upsert_claim(
        service_name,
        pool_key=pool_key,
        weight=autoscaler.reserved_fill_weight,
        floor_replicas=floor,
        gpus_per_replica=per_replica,
        holdings_fill=holdings_fill,
        effective_cap=effective_cap,
        launchable=_placer_can_launch_zero_cost(placer),
        activity=activity,
        **fence_kwargs)
    if claim_persisted is False:
        autoscaler.collect_reserved_capacity(0, keys, time.time())
        logger.info('Reserved-fill broker: claim rejected or controller '
                    f'stale for {service_name!r}; feeding 0 slots.')
        return
    allocation = reserved_capacity_broker.run_round_if_stale(
        service_name, pool_key,
        lambda: query_pool_group_observation(context, grouped_shapes),
        poll_interval_seconds())
    if allocation is None:
        # No allocation this cycle (claim rejected/expired, round lock
        # timeout, or the fresh round predates our claim): feed zero free
        # slots. Existing holdings stay sheltered via zero_cost_count; no
        # new fill until the broker admits us.
        autoscaler.collect_reserved_capacity(0, keys, time.time())
        logger.info('Reserved-fill broker: no allocation for '
                    f'{service_name!r} this cycle; feeding 0 free slots.')
        return
    autoscaler.collect_reserved_capacity(allocation.feed,
                                         keys,
                                         allocation.snapshot_time,
                                         grant=allocation.grant,
                                         grant_epoch=allocation.epoch,
                                         grant_pool_key=pool_key)
    logger.info(f'Reserved-fill broker: {service_name!r} feed='
                f'{allocation.feed} grant={allocation.grant} '
                f'(round {allocation.round_id}, epoch {allocation.epoch}).')


def _pool_capacity_hint(spec: FillPoolSpec, holdings: int, launchable: bool,
                        previous_cap: int, now: float) -> int:
    """Return the bounded discovery/blackout hint for one pool edge."""
    if not launchable:
        return holdings
    round_row = serve_state.get_reserved_fill_round(spec.pool_key)
    if round_row is None or round_row.get('last_observed_free') is None:
        return holdings + 1
    observed_at = round_row.get('last_observed_free_ts')
    if observed_at is not None:
        try:
            fresh = (now - float(observed_at) <= poll_interval_seconds() *
                     constants.RESERVED_CAPACITY_STALE_AFTER_INTERVALS)
        except (TypeError, ValueError):
            fresh = False
        if fresh:
            return holdings + max(0, int(round_row['last_observed_free']))
    return max(holdings, previous_cap)


def _broker_cycle_v2(
    autoscaler: 'autoscalers.Autoscaler',
    placer: 'spot_placer_lib.SpotPlacer',
    service_name: str,
    zero_cost: list['spot_placer_lib.Location'],
    expected_service_hash: str | None,
    expected_controller_owner: tuple[int | None, str | None] | None,
) -> None:
    """Publish and consume one atomic protocol-v2 multi-pool heartbeat."""
    try:
        specs = discover_fill_pool_specs(zero_cost)
    except ValueError as error:
        logger.error('Reserved-fill protocol-v2 pool discovery rejected the '
                     f'service configuration for {service_name!r}: {error}')
        reserved_capacity_broker.remove_claim(
            service_name,
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner)
        autoscaler.collect_reserved_capacity_pools({})
        return
    logical_slot_mismatch = (isinstance(
        placer, spot_placer_lib.CapacityAwareDynamicFallbackSpotPlacer) and
                             any(spec.gpus_per_replica != 1 for spec in specs))
    if not specs or logical_slot_mismatch:
        logger.error('Reserved-fill protocol v2 found no valid physical pool '
                     'set (logical services also require one-GPU shapes); '
                     f'fill is inactive for {service_name!r}.')
        reserved_capacity_broker.remove_claim(
            service_name,
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner)
        autoscaler.collect_reserved_capacity_pools({})
        return

    location_keys = {
        spec.pool_key: [
            location.to_pickleable() for location in spec.locations
        ] for spec in specs
    }
    autoscaler.seed_zero_cost_pools(location_keys)
    previous_set = serve_state.get_reserved_fill_service_claim_set(service_name)
    if previous_set is None or not previous_set.get('integrity_valid'):
        previous_set = None
    previous_edges = ({
        str(edge['pool_key']): edge for edge in previous_set.get('edges', [])
    } if previous_set is not None else {})
    previous_generation = (int(previous_set['generation'])
                           if previous_set is not None else 0)
    pool_authority = {
        pool_key: (str(edge['physical_cluster_uid']), previous_generation)
        for pool_key, edge in previous_edges.items()
        if isinstance(edge.get('physical_cluster_uid'), str) and
        edge['physical_cluster_uid']
    }
    replica_infos = serve_state.get_replica_infos(service_name)
    holdings_by_pool = autoscaler.count_zero_cost_holdings_by_pool(
        replica_infos, location_keys, pool_authority)

    now = time.time()
    global_headroom = max(
        0, autoscaler.max_replicas - autoscaler.get_final_target_num_replicas())
    total_fill_holdings = sum(
        holdings_by_pool.get(spec.pool_key, (0, 0))[0] for spec in specs)
    if (autoscaler.reserved_fill_utilization_gate and
            reserved_capacity_broker.utilization_gate_enabled()):
        sample = autoscaler.fill_demand_sample(replica_infos)
        prior_state = (previous_set.get('utilization_state')
                       if previous_set is not None else None)
        utilization_state = reserved_capacity_broker.advance_release_target(
            prior_state if isinstance(prior_state, dict) else None,
            floor=0,
            holdings=total_fill_holdings,
            need=0 if sample is None else sample.demonstrated_need(),
            boot_hold=False if sample is None else sample.boot_hold(),
            blind=sample is None,
            now=now,
            dwell=constants.RESERVED_FILL_IDLE_DWELL_SECONDS,
            step_seconds=constants.RESERVED_FILL_RELEASE_STEP_SECONDS,
            step_fraction=constants.RESERVED_FILL_RELEASE_STEP_FRACTION,
            min_step=constants.RESERVED_FILL_RELEASE_MIN_STEP,
            headroom=constants.RESERVED_FILL_UTILIZATION_HEADROOM,
            blind_grace=constants.RESERVED_FILL_BLIND_GRACE_SECONDS)
        utilization_ceiling = min(global_headroom,
                                  max(0, int(utilization_state['cap'])))
    else:
        utilization_state = None
        utilization_ceiling = global_headroom
    global_budget = min(global_headroom, utilization_ceiling)

    active_locations = placer.active_locations()
    launchable: dict[str, bool] = {
        spec.pool_key: any(
            any(
                spot_placer_lib.locations_match_placement(location, active)
                for active in active_locations)
            for location in spec.locations) for spec in specs
    }
    budget_inputs: list[FillPoolBudgetInput] = []
    for spec in specs:
        holdings_fill = holdings_by_pool.get(spec.pool_key, (0, 0))[0]
        previous_cap = max(
            0,
            int(
                previous_edges.get(spec.pool_key, {}).get('effective_cap') or
                0))
        budget_inputs.append(
            FillPoolBudgetInput(holdings=holdings_fill,
                                capacity_hint=_pool_capacity_hint(
                                    spec, holdings_fill,
                                    launchable[spec.pool_key], previous_cap,
                                    now)))
    budgets = allocate_fill_pool_budgets(
        global_budget, autoscaler.reserved_fill_floor_replicas,
        tuple(budget_inputs))

    edges: list[dict[str, Any]] = []
    semantic_edges: list[dict[str, Any]] = []
    for spec, budget in zip(specs, budgets):
        holdings_fill = holdings_by_pool.get(spec.pool_key, (0, 0))[0]
        edge = {
            'pool_key': spec.pool_key,
            'legacy_pool_key': spec.legacy_pool_key,
            'pool_position': spec.position,
            'access_context': spec.context,
            'physical_cluster_uid': spec.physical_cluster_uid,
            'accelerator_names': list(spec.accelerator_names),
            'weight': autoscaler.reserved_fill_weight,
            'floor_replicas': budget.edge_floor,
            'gpus_per_replica': spec.gpus_per_replica,
            'holdings_fill': holdings_fill,
            'effective_cap': budget.edge_cap,
            'launchable': launchable[spec.pool_key],
        }
        edges.append(edge)
        semantic_edges.append({
            key: value
            for key, value in edge.items()
            if key not in ('holdings_fill', 'launchable')
        })
    semantic_payload = {
        'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
        'global_headroom': global_headroom,
        'utilization_ceiling': utilization_ceiling,
        'utilization_gate': autoscaler.reserved_fill_utilization_gate,
        'service_floor': autoscaler.reserved_fill_floor_replicas,
        'service_weight': autoscaler.reserved_fill_weight,
        'edges': semantic_edges,
    }
    semantic_hash = hashlib.sha256(
        json.dumps(semantic_payload, sort_keys=True,
                   separators=(',', ':')).encode('utf-8')).hexdigest()
    generation = reserved_capacity_broker.replace_claim_set(
        service_name,
        semantic_hash=semantic_hash,
        global_headroom=global_headroom,
        utilization_ceiling=utilization_ceiling,
        utilization_state=utilization_state,
        edges=edges,
        expected_service_hash=expected_service_hash,
        expected_controller_owner=expected_controller_owner)
    if generation is None:
        autoscaler.collect_reserved_capacity_pools({})
        logger.info('Reserved-fill broker: complete claim-set heartbeat was '
                    f'rejected for {service_name!r}; feeding every pool 0.')
        return

    snapshots: dict[str, dict[str, Any]] = {}
    for spec, budget in zip(specs, budgets):
        try:
            allocation = reserved_capacity_broker.run_round_if_stale(
                service_name,
                spec.pool_key,
                functools.partial(query_pool_group_observation, spec.context,
                                  dict(spec.shapes)),
                poll_interval_seconds(),
                expected_protocol_version=reserved_capacity_broker.PROTOCOL_V2,
                expected_service_generation=generation)
        except Exception as error:  # pylint: disable=broad-except
            # One pool's transient database/lock path must not suppress a
            # healthy peer edge in this same complete-map publication.
            logger.warning('Reserved-fill broker round failed for '
                           f'{service_name!r}/{spec.pool_key}: '
                           f'{common_utils.format_exception(error)}')
            allocation = None
        # A lock timeout or other transient round miss must not cull existing
        # fill from this one pool.  Carry only the last real exact-generation
        # grant as scale-down shelter.  Live launch authority still fails
        # closed: feed and grant are zero and no epoch is replayed.  The exact
        # generation/UID lookup also prevents an old incarnation from
        # sheltering a removed/re-added edge.
        shelter_grant = (autoscaler.get_reserved_capacity_pool_shelter_grant(
            spec.pool_key,
            service_generation=generation,
            physical_cluster_uid=spec.physical_cluster_uid,
            edge_cap=budget.edge_cap)
                         if allocation is None else allocation.grant)
        snapshots[spec.pool_key] = {
            'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
            'pool_key': spec.pool_key,
            'physical_cluster_uid': spec.physical_cluster_uid,
            'service_generation': generation,
            'edge_cap': budget.edge_cap,
            'zero_cost_location_keys': location_keys[spec.pool_key],
            'free_slots': 0 if allocation is None else allocation.feed,
            'free_slots_by_accelerator':
                (None if allocation is None else allocation.feed_by_accelerator
                ),
            'grant': 0 if allocation is None else allocation.grant,
            'shelter_grant': shelter_grant,
            'grant_epoch': None if allocation is None else allocation.epoch,
            'timestamp': now
                         if allocation is None else allocation.snapshot_time,
        }
    autoscaler.collect_reserved_capacity_pools(snapshots)
    logger.info('Reserved-fill broker: published service generation '
                f'{generation} with {len(snapshots)} physical pool(s) for '
                f'{service_name!r}.')


def poller_loop(
    get_autoscaler: Callable[[], 'autoscalers.Autoscaler'],
    get_spot_placer: Callable[[], Optional['spot_placer_lib.SpotPlacer']],
    service_name: str | None = None,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> None:
    """Poll free zero-cost capacity forever, feeding the autoscaler.

    Runs as a supervised thread started by the controller (only when the
    service opted in AND a spot placer exists -- the placer defines the
    zero-cost location set). Takes getters, not the live objects: an
    update_service can replace the controller's autoscaler, and the
    snapshot must reach the current one.

    service_name enables broker arbitration (the controller always passes
    it); None preserves the standalone pre-broker cycle for direct callers
    and tests.
    """
    # Whether a broker claim of ours may exist. Starts True: a previous
    # incarnation of this controller may have left one behind (respawn),
    # so the first disabled observation still clears it. Reset to True
    # BEFORE every broker cycle (which upserts the claim).
    claim_may_exist = service_name is not None
    fence_kwargs: dict[str, Any] = {}
    if expected_service_hash is not None:
        fence_kwargs['expected_service_hash'] = expected_service_hash
    if expected_controller_owner is not None:
        fence_kwargs['expected_controller_owner'] = expected_controller_owner
    while True:
        try:
            if service_name is not None and expected_service_hash is not None:
                owner = serve_state.get_service_controller_owner(service_name)
                current_owner = (owner.get('controller_pid'),
                                 owner.get('controller_ip')) if owner else None
                if (owner is None or
                        owner.get('hash') != expected_service_hash or
                    (expected_controller_owner is not None and
                     current_owner != expected_controller_owner)):
                    logger.info(
                        f'Reserved-capacity poller for stale service owner '
                        f'{service_name!r}/{expected_service_hash!r}/'
                        f'{expected_controller_owner!r} is exiting.')
                    return
            placer = get_spot_placer()
            # An update can turn the flag off on the live autoscaler; the
            # thread stays alive (a later update can re-enable it) but
            # must not keep issuing the expensive cluster-wide pod-listing
            # query for a snapshot nobody consumes.
            autoscaler = get_autoscaler()
            fill_enabled = autoscaler.reserved_capacity_fill
            if placer is not None and fill_enabled:
                zero_cost = placer.zero_cost_locations()
                keys: list[dict[str, Any]] = [
                    location.to_pickleable() for location in zero_cost
                ]
                if service_name is None:
                    _standalone_cycle(autoscaler, zero_cost, keys)
                else:
                    # Set BEFORE the cycle: it upserts the claim partway
                    # through, and an exception after that upsert (e.g.
                    # the round query) must still leave the flag true --
                    # otherwise a subsequent disable would skip
                    # remove_claim and leave a ghost claim absorbing
                    # entitlement for the whole claim TTL.
                    claim_may_exist = True
                    protocol_version = (
                        reserved_capacity_broker.get_protocol_version())
                    if protocol_version == reserved_capacity_broker.PROTOCOL_V2:
                        _broker_cycle_v2(autoscaler, placer, service_name,
                                         zero_cost, expected_service_hash,
                                         expected_controller_owner)
                    else:
                        _broker_cycle(autoscaler, placer, service_name,
                                      zero_cost, keys, expected_service_hash,
                                      expected_controller_owner)
            elif service_name is not None and claim_may_exist:
                # Fill turned off (or the placer is gone): withdraw the
                # claim NOW instead of leaving peers arbitrating around a
                # ghost for the whole claim TTL. Once per disable
                # transition (idempotent; also drops our cached
                # allocation), not re-spammed every cycle.
                reserved_capacity_broker.remove_claim(service_name,
                                                      **fence_kwargs)
                claim_may_exist = False
        except Exception as e:  # pylint: disable=broad-except
            logger.error('Error in reserved-capacity poller: '
                         f'{common_utils.format_exception(e)}')
        time.sleep(poll_interval_seconds())
