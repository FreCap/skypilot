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
import json
import math
import os
import re
import threading
import time
import typing
from typing import Any, Optional

from sky import sky_logging
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


_DEMAND_REFRESH_STATE_LOCK = threading.Lock()
_DEMAND_REFRESH_PENDING_CONTEXTS: set[str] = set()
_DEMAND_REFRESH_RUNNING = False


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
    return reserved_capacity_broker.PoolObservation(
        free_slots=free_gpus // max(1, per_replica),
        gpu_names=tuple(available.keys()))


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
    free_slots = sum(
        max(0, available_lower.get(name, 0)) // per_replica
        for name, per_replica in shapes.items())
    matched_names = tuple(
        name for name in available if str(name).lower() in shapes)
    return reserved_capacity_broker.PoolObservation(free_slots=free_slots,
                                                    gpu_names=matched_names)


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
                    _broker_cycle(autoscaler, placer, service_name, zero_cost,
                                  keys, expected_service_hash,
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
