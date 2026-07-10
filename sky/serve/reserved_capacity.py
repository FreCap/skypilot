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
import os
import re
import time
import typing
from typing import Any, Callable, Dict, List, Optional, Tuple

from sky import sky_logging
from sky.catalog import kubernetes_catalog
from sky.serve import constants
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.utils import common_utils

if typing.TYPE_CHECKING:
    from sky.serve import autoscalers
    from sky.serve import spot_placer as spot_placer_lib

logger = sky_logging.init_logger(__name__)


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
    zero_cost_locations: List['spot_placer_lib.Location']
) -> Dict[Tuple[str, str], int]:
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
    per_key_replica_size: Dict[Tuple[str, str], int] = {}
    for location in zero_cost_locations:
        if str(location.cloud).lower() != 'kubernetes':
            continue
        if not location.accelerators:
            continue
        gpu_name, per_replica = next(iter(location.accelerators.items()))
        try:
            per_replica = max(1, int(per_replica))
        except (TypeError, ValueError):
            per_replica = 1
        key = (location.region, gpu_name.lower())
        per_key_replica_size[key] = max(per_key_replica_size.get(key, 1),
                                        per_replica)
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


def query_free_slots(
        zero_cost_locations: List['spot_placer_lib.Location']) -> int:
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


def _standalone_cycle(autoscaler: 'autoscalers.Autoscaler',
                      zero_cost: List['spot_placer_lib.Location'],
                      keys: List[Dict[str, Any]]) -> None:
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


def _broker_cycle(autoscaler: 'autoscalers.Autoscaler',
                  placer: 'spot_placer_lib.SpotPlacer', service_name: str,
                  zero_cost: List['spot_placer_lib.Location'],
                  keys: List[Dict[str, Any]]) -> None:
    """Broker-arbitrated cycle: claim heartbeat -> round -> feed+grant."""
    shapes = zero_cost_pool_shapes(zero_cost)
    if len(shapes) != 1:
        # v1 restriction: all zero-cost shapes must resolve into ONE pool
        # group (multi-pool claims/feeds/pinned launches are v2). Withdraw
        # the claim (peers must not arbitrate around a ghost) and feed
        # zero: existing holdings keep their shelter via zero_cost_count,
        # but no new fill happens on an un-arbitrated pool set. Re-logged
        # every interval by design -- this is a misconfiguration.
        logger.error(
            'Reserved-fill broker: service zero-cost shapes resolve to '
            f'{len(shapes)} pools ({sorted(shapes)}); v1 supports exactly '
            'one. Fill is inactive for this service.')
        reserved_capacity_broker.remove_claim(service_name)
        autoscaler.collect_reserved_capacity(0, keys, time.time())
        return
    (context, gpu_name), per_replica = next(iter(shapes.items()))
    pool_key = reserved_capacity_broker.make_pool_key(context, gpu_name)
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
    reserved_capacity_broker.upsert_claim(
        service_name,
        pool_key=pool_key,
        weight=autoscaler.reserved_fill_weight,
        floor_replicas=floor,
        gpus_per_replica=per_replica,
        holdings_fill=holdings_fill,
        effective_cap=effective_cap,
        launchable=_placer_can_launch_zero_cost(placer))
    allocation = reserved_capacity_broker.run_round_if_stale(
        service_name, pool_key,
        lambda: query_pool_observation(context, gpu_name, per_replica),
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


def poller_loop(get_autoscaler: Callable[[], 'autoscalers.Autoscaler'],
                get_spot_placer: Callable[
                    [], Optional['spot_placer_lib.SpotPlacer']],
                service_name: Optional[str] = None) -> None:
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
    while True:
        try:
            placer = get_spot_placer()
            # An update can turn the flag off on the live autoscaler; the
            # thread stays alive (a later update can re-enable it) but
            # must not keep issuing the expensive cluster-wide pod-listing
            # query for a snapshot nobody consumes.
            autoscaler = get_autoscaler()
            fill_enabled = autoscaler.reserved_capacity_fill
            if placer is not None and fill_enabled:
                zero_cost = placer.zero_cost_locations()
                keys: List[Dict[str, Any]] = [
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
                                  keys)
            elif service_name is not None and claim_may_exist:
                # Fill turned off (or the placer is gone): withdraw the
                # claim NOW instead of leaving peers arbitrating around a
                # ghost for the whole claim TTL. Once per disable
                # transition (idempotent; also drops our cached
                # allocation), not re-spammed every cycle.
                reserved_capacity_broker.remove_claim(service_name)
                claim_may_exist = False
        except Exception as e:  # pylint: disable=broad-except
            logger.error('Error in reserved-capacity poller: '
                         f'{common_utils.format_exception(e)}')
        time.sleep(poll_interval_seconds())
