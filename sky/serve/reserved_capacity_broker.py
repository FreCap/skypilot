"""Reserved-fill broker: multi-service arbitration of zero-cost pools.

[boltz fork] With multiple fill-enabled services (#108) on one reserved
pool, independent pollers race for the same free GPUs: every autoscaler
targets them, the k8s scheduler picks winners, losers bench their zero-cost
tier for the retry TTL, and the cluster-wide realtime query runs N times per
interval. This module arbitrates: each service's poller upserts a CLAIM
(weight / floor / holdings / heartbeat), one poller per interval drives a
ROUND (single cluster query under a cross-process lock), and the round
publishes per-service GRANTS (entitlement ceilings) and FEEDS (launchable-
now free slots, sum <= observed free by construction).

Design invariants (see the 2026-07-08 design doc):
- Entitlements are floors-first (largest-remainder proportional scale-down
  if oversubscribed) then weighted water-filling of the remainder with
  headroom caps and redistribution; all arithmetic in integer replica slots
  (v1 requires uniform gpus_per_replica per pool).
- Feeds are a separate water-fill of OBSERVED FREE among under-holders: a
  peer's slow graceful drain must not make Sum(feeds) exceed physical free
  capacity (entitlement-as-feed overshoots; feed-split cannot).
- Grants only ever gate NEW launches, so stale readers are safe; the pool's
  ROUND epoch is the fencing token that keeps a respawned/stalled controller
  from ACTING on a superseded allocation (per-pool, so one pool's grant
  churn never fences another's launches); the global lease epoch exists
  only for the publish CAS.
- Exactly one live claim => the fast path: grant None (no ceiling), feed =
  raw observed free -- byte-identical #108 behavior, pinned by the existing
  test suite.

This module owns allocation logic only; all SQL lives in serve_state (the
shared serve DB every controller in the api-server pod already uses).
"""
import dataclasses
import json
import math
import os
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from sky import sky_logging
from sky.serve import constants
from sky.serve import serve_state
from sky.utils import common_utils
from sky.utils import locks

logger = sky_logging.init_logger(__name__)

# Round age below which a poller reads the published round instead of
# driving a new one, as a fraction of the poll interval. Slightly below 1 so
# scheduling jitter cannot leave a pool permanently one-poller short of
# driving (with N pollers on the same interval, roughly one round is driven
# per interval; the rest read).
_ROUND_FRESH_FRACTION = 0.9


def claim_ttl_seconds() -> float:
    override = os.environ.get(constants.RESERVED_FILL_CLAIM_TTL_ENV_VAR)
    if override is not None:
        try:
            return max(1.0, float(override))
        except ValueError:
            logger.warning(
                f'Invalid {constants.RESERVED_FILL_CLAIM_TTL_ENV_VAR} value '
                f'{override!r}, using default '
                f'{constants.RESERVED_FILL_CLAIM_TTL_SECONDS}s.')
    return float(constants.RESERVED_FILL_CLAIM_TTL_SECONDS)


def make_pool_key(context: str, gpu_name: str) -> str:
    """Canonical pool identity: (kubernetes context, lowercased GPU name).

    JSON list, not a joined string: context names are user-controlled and a
    separator collision must not merge two pools.
    """
    return json.dumps([context, gpu_name.lower()])


def parse_pool_key(pool_key: str) -> Tuple[str, str]:
    context, gpu_name = json.loads(pool_key)
    return context, gpu_name


@dataclasses.dataclass(frozen=True)
class PoolObservation:
    """One realtime free-capacity measurement of a pool.

    free_slots None = the query FAILED (measurement blackout) -- distinct
    from 0 free, which is a successful measurement of a full pool.
    gpu_names are the canonical accelerator names the realtime query
    reported for the pool's context; empty on a successful query means the
    claimed GPU resolves to no labeled nodes (phantom pool).
    """
    free_slots: Optional[int]
    gpu_names: Tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class ClaimInput:
    """Pure-math view of one live claim."""
    floor: int
    weight: float
    holdings_fill: int
    launchable: bool
    # Real capacity cap the claimant can materialize right now
    # (max(0, max_replicas - demand_target)); None = unbounded (legacy
    # claim rows). Clamps the effective floor, the headroom (weighted
    # share above the floor, derived in compute_entitlements) and the
    # feed need: a floor the service cannot actually launch
    # (floor > cap) must not absorb entitlement or feed -- the excess
    # joins the burst remainder (work conservation).
    effective_cap: Optional[int] = None

    def attainable_floor(self) -> int:
        """The floor, clamped to what the claimant can materialize."""
        floor = max(0, self.floor)
        if self.effective_cap is not None:
            floor = min(floor, max(0, self.effective_cap))
        return floor


@dataclasses.dataclass(frozen=True)
class Allocation:
    """One service's slice of the latest round."""
    # None = single-claimant fast path: no ceiling (#108 identity).
    grant: Optional[int]
    feed: int
    round_id: int
    epoch: int
    snapshot_time: float


# In-process cache of the last GRANT each service observed, refreshed by
# its poller every poll interval. The demand-placement gate in the launch
# path reads ONLY this cache (never the DB): the gate is advisory and a
# poll-interval-stale read is safe (grants only gate NEW launches), while a
# DB read per demand launch would be a hot-path regression. A None grant
# (single-claimant fast path) and a missing/stale entry read the same --
# both leave the gate inert.
_GRANT_CACHE: Dict[str, Tuple[Optional[int], float]] = {}


def clear_caches() -> None:
    """Test hook: drop in-process state."""
    _GRANT_CACHE.clear()


def get_cached_grant(service_name: str,
                     max_age_seconds: float) -> Optional[int]:
    entry = _GRANT_CACHE.get(service_name)
    if entry is None:
        return None
    grant, cached_at = entry
    if time.time() - cached_at > max_age_seconds:
        return None
    return grant


# Sentinel returned by current_epoch while a pool's fence_pending marker
# is set: published epochs start at 1, so no launch ever carries it and
# the launch-path comparison fails closed (skip) without a special case.
_FENCE_PENDING_EPOCH = -1


def current_epoch(pool_key: str) -> Optional[int]:
    """The POOL's current fencing epoch (cheap single-row DB read).

    Per-pool by design: rounds and grants are per-pool, so the launch
    fence must compare a carried epoch against ITS pool's round epoch.
    Fencing on the global lease epoch would let pool A's grant churn
    fence pool B's unrelated fill launches for up to two poll intervals.
    None (no round published yet) fails open at the fence: there is no
    newer allocation to defer to.

    A set fence_pending marker fails CLOSED: every grant issued before a
    lease-dead gap is suspect until an epoch-bumping publish clears the
    marker, so the sentinel returned here mismatches any carried epoch
    and the launch skips -- even for a pool that will never publish again
    (claims gone). add_replica_if_round_epoch enforces the same predicate
    atomically at persist time.
    """
    round_row = serve_state.get_reserved_fill_round(pool_key)
    if round_row is None:
        return None
    if bool(round_row['fence_pending']):
        return _FENCE_PENDING_EPOCH
    return int(round_row['epoch'])


def persist_fill_replica(service_name: str, replica_id: int, replica_info: Any,
                         *, pool_key: str, expected_epoch: int) -> bool:
    """Atomically persists a fill replica row, excluded from broker rounds.

    Ordering invariant (the other half lives on run_round_if_stale): a
    fill row must never become durable INSIDE a round's scan->publish
    window. The round's debit scan cannot see a row persisted after it
    ran, and the epoch fence cannot see a round that has not published
    yet -- a persist landing between the two is counted by neither, and
    the round re-feeds the just-taken slot to a peer. The round holds the
    cross-process broker lock for its whole body (scan through publish),
    so taking the same lock here leaves exactly two outcomes: the persist
    lands BEFORE the round's scan (the row is counted by the debit) or
    AFTER its publish (a superseded decision is fenced by the bumped
    epoch / fence_pending inside add_replica_if_round_epoch).

    Non-blocking on purpose: a round in flight holds the lock across its
    whole cluster query, and blocking a scale-up batch that long is worse
    than skipping -- contention degrades into a fence-skip (False) and
    the autoscaler re-emits the launch on its next tick. The persist
    itself is one quick DB write, so a round waiting behind it is never
    delayed noticeably.
    """
    try:
        lock = locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID)
        with lock.acquire(blocking=False):
            return serve_state.add_replica_if_round_epoch(
                service_name,
                replica_id,
                replica_info,
                pool_key=pool_key,
                expected_epoch=expected_epoch)
    except locks.LockTimeout:
        return False


# =========================== Pure allocation math ===========================
# No DB access below this line until the round driver section: every
# function here is deterministic on its inputs (ties broken by sorted
# service name) so rounds are reproducible and unit-testable in isolation.


def _largest_remainder_round(amounts: Mapping[str, float],
                             total: int) -> Dict[str, int]:
    """Rounds fractional shares to integers summing exactly to `total`.

    Floor everything, then hand the leftover units to the largest
    fractional remainders (ties by service name for determinism).
    """
    base = {name: int(amount) for name, amount in amounts.items()}
    leftover = total - sum(base.values())
    order = sorted(amounts,
                   key=lambda name: (-(amounts[name] - base[name]), name))
    for name in order[:max(0, leftover)]:
        base[name] += 1
    return base


def scale_floors(total: int, floors: Mapping[str, int]) -> Dict[str, int]:
    """Floors, proportionally scaled down when oversubscribed.

    Sum(floors) <= total returns floors unchanged; otherwise each floor is
    scaled by total/Sum(floors) with largest-remainder rounding so the
    scaled floors sum exactly to total (nobody gets a unit another service
    was proportionally closer to).
    """
    total = max(0, total)
    floor_sum = sum(floors.values())
    if floor_sum <= total:
        return dict(floors)
    # Reaching here implies floor_sum > total >= 0: no division by zero.
    scaled = {name: total * floor / floor_sum for name, floor in floors.items()}
    return _largest_remainder_round(scaled, total)


def water_fill(amount: int, weights: Mapping[str, float],
               caps: Mapping[str, Optional[int]]) -> Dict[str, int]:
    """Weighted integer water-fill of `amount` with per-service caps.

    Iteratively hands out weight-proportional shares (largest-remainder
    integer rounding); a service hitting its cap is fixed there and its
    unused share is redistributed among the remaining ones. Terminates
    because every iteration either exhausts the amount or removes at least
    one capped service. Sum(result) <= amount always; < only when every
    service is capped below its share.
    """
    result = {name: 0 for name in weights}
    remaining = max(0, amount)
    active = [
        name for name in sorted(weights) if weights[name] > 0 and
        (caps.get(name) is None or (caps[name] or 0) > 0)
    ]
    while remaining > 0 and active:
        # Normalize by the largest active weight before summing or
        # multiplying: finite-but-huge weights (isfinite passes 1e308)
        # would overflow remaining*weight or sum(weights) into inf, and
        # inf/inf -> NaN crashes the integer rounding -- every round, for
        # the whole pool. After normalization each term is <= remaining;
        # fsum keeps the sum exact across wide magnitude spreads.
        max_weight = max(weights[name] for name in active)
        normalized = {name: weights[name] / max_weight for name in active}
        weight_sum = math.fsum(normalized.values())
        shares = {
            name: remaining * normalized[name] / weight_sum for name in active
        }
        rounded = _largest_remainder_round(shares, remaining)
        capped_any = False
        for name in list(active):
            give = rounded.get(name, 0)
            cap = caps.get(name)
            if cap is not None:
                room = cap - result[name]
                if give >= room:
                    give = max(0, room)
                    capped_any = True
            result[name] += give
            remaining -= give
        if capped_any:
            active = [
                name for name in active
                if caps.get(name) is None or result[name] < (caps[name] or 0)
            ]
        else:
            # Uncapped iteration distributes everything by construction.
            break
    return result


def compute_entitlements(total: int,
                         claims: Mapping[str, ClaimInput]) -> Dict[str, int]:
    """Per-service entitlements: floors first, then weighted water-fill.

    total is the whole-pool fill capacity this round (debited observed free
    + Sum of fill holdings) -- whole-pool allocation, no grandfathering.
    Sum(entitlements) <= total in all reachable states (floors are scaled
    into total; the water-fill never exceeds its amount).

    Floors are clamped to each claimant's effective_cap first: an
    unattainable floor (floor > what the service can launch under its
    demand pressure) must not absorb entitlement it can never
    materialize; the clamped excess joins the weighted remainder and
    flows to peers. The water-fill share is capped by the HEADROOM
    (effective_cap minus the attainable floor, derived here rather than
    stored) so the whole entitlement never exceeds effective_cap either.
    """
    floors = scale_floors(
        total,
        {name: claim.attainable_floor() for name, claim in claims.items()})
    remainder = max(0, total) - sum(floors.values())
    caps: Dict[str, Optional[int]] = {}
    for name, claim in claims.items():
        if claim.effective_cap is None:
            caps[name] = None
        else:
            caps[name] = max(0, claim.effective_cap - claim.attainable_floor())
    shares = water_fill(remainder,
                        {name: claim.weight for name, claim in claims.items()},
                        caps)
    return {name: floors[name] + shares[name] for name in claims}


def damp_grants(raw: Mapping[str, int], prev_grants: Optional[Mapping[str,
                                                                      int]],
                prev_raw: Optional[Mapping[str, int]],
                holdings_shrank: bool) -> Dict[str, int]:
    """Two-round persistence for grant moves (mirrors #108's poll damping).

    Up-moves and observed-free-driven down-moves apply only when two
    consecutive rounds propose them, acting on the level that persisted
    across both (min for ups, max for downs) -- grants only gate launches,
    so damping costs nothing and kills oscillation. Holdings-driven downs
    (pool-wide fill holdings shrank: pods are physically gone) apply
    immediately: capacity that vanished must stop being granted now.

    A service with no published-integer baseline (first multi-claimant
    round, or transitioning off the single-claimant None grant) takes its
    raw entitlement immediately -- there is no previous level to persist
    against.
    """
    if prev_grants is None:
        return dict(raw)
    damped: Dict[str, int] = {}
    prev_raw = prev_raw or {}
    for name, proposed in raw.items():
        prev = prev_grants.get(name)
        if prev is None:
            damped[name] = proposed
        elif proposed > prev:
            last_proposed = prev_raw.get(name)
            if last_proposed is not None and last_proposed > prev:
                damped[name] = min(proposed, last_proposed)
            else:
                damped[name] = prev
        elif proposed < prev:
            if holdings_shrank:
                damped[name] = proposed
            else:
                last_proposed = prev_raw.get(name)
                if last_proposed is not None and last_proposed < prev:
                    damped[name] = max(proposed, last_proposed)
                else:
                    damped[name] = prev
        else:
            damped[name] = proposed
    return damped


def compute_feeds(
    observed_free: int,
    grants: Mapping[str, int],
    claims: Mapping[str, ClaimInput],
    sticky_state: Mapping[str, Dict[str, Any]],
    now: float,
    sticky_window_seconds: float,
    raw_grants: Optional[Mapping[str, int]] = None,
) -> Tuple[Dict[str, int], Dict[str, Dict[str, Any]]]:
    """Splits OBSERVED FREE among launchable under-holders.

    Sum(feeds) <= observed_free by construction, so a peer's slow graceful
    drain (holdings above its shrunken grant) can never make the collective
    launch beyond physical free capacity. Share un-feedable this round
    (claimant benched, or already at its grant) is redistributed to
    claimants that can launch; entitlements are untouched so stickiness of
    the GRANT snaps holdings back later.

    Feed stickiness: a service holding a still-fresh positive assignment
    (within sticky_window_seconds of when its streak began) is served first
    at up to its previous amount -- without this, a single free GPU
    fairness-alternated between two services never survives the local
    two-poll increase damping and idles forever. `since` is preserved
    across consecutive positive rounds, so the window measures the streak
    start, not the last assignment.

    raw_grants (this round's UNDAMPED entitlements) additionally clamps the
    feed need to min(damped, raw): during a down-move's two-round damping
    window the published grant sits ABOVE the raw entitlement, and feeding
    that gap launches a replica the damped grant is about to catch down to
    and cull (the ceiling strips its shelter as soon as it boots). Damping
    exists to protect launches, never to originate doomed ones; up-damping
    is unaffected (there min(damped, raw) == damped).
    """
    free = max(0, observed_free)
    # Need is clamped by effective_cap, not just the grant: damping (and
    # the blind-round holdings floor) can keep a published grant above
    # what the claimant can currently materialize, and feed handed to a
    # service that cannot launch it idles for the whole round.
    need = {}
    for name, claim in claims.items():
        grant = grants.get(name, 0)
        if raw_grants is not None and name in raw_grants:
            grant = min(grant, raw_grants[name])
        if claim.effective_cap is not None:
            grant = min(grant, max(0, claim.effective_cap))
        need[name] = max(0, grant - claim.holdings_fill)
    eligible = {
        name for name, claim in claims.items()
        if claim.launchable and need[name] > 0
    }
    feeds = {name: 0 for name in claims}
    # Pass 1: honor fresh sticky assignments (deterministic name order).
    for name in sorted(sticky_state):
        if name not in eligible or free <= 0:
            continue
        entry = sticky_state[name]
        try:
            amount = int(entry['amount'])
            since = float(entry['since'])
        except (KeyError, TypeError, ValueError):
            continue
        if now - since > sticky_window_seconds:
            continue
        give = min(amount, need[name], free)
        if give <= 0:
            continue
        feeds[name] = give
        free -= give
    # Pass 2: water-fill the remaining free among eligible under-holders.
    if free > 0 and eligible:
        extra = water_fill(
            free, {name: claims[name].weight for name in eligible},
            {name: need[name] - feeds[name] for name in eligible})
        for name, give in extra.items():
            feeds[name] += give
    # New sticky state: only positive feeds carry a streak; `since` is kept
    # from an unexpired previous entry (streak continues) and reset
    # otherwise (fresh assignment starts a new window).
    new_sticky: Dict[str, Dict[str, Any]] = {}
    for name, feed in feeds.items():
        if feed <= 0:
            continue
        since = now
        prev = sticky_state.get(name)
        if prev is not None:
            try:
                prev_since = float(prev['since'])
                if now - prev_since <= sticky_window_seconds:
                    since = prev_since
            except (KeyError, TypeError, ValueError):
                pass
        new_sticky[name] = {'amount': feed, 'since': since}
    return feeds, new_sticky


# ============================== Round driver ================================


def upsert_claim(service_name: str,
                 *,
                 pool_key: str,
                 weight: float,
                 floor_replicas: int,
                 gpus_per_replica: int,
                 holdings_fill: int,
                 launchable: bool,
                 effective_cap: Optional[int] = None) -> None:
    """Upserts this service's claim (the per-poll heartbeat)."""
    serve_state.upsert_reserved_fill_claim(service_name,
                                           pool_key=pool_key,
                                           weight=weight,
                                           floor_replicas=floor_replicas,
                                           gpus_per_replica=gpus_per_replica,
                                           holdings_fill=holdings_fill,
                                           effective_cap=effective_cap,
                                           launchable=launchable,
                                           heartbeat_ts=time.time())


def remove_claim(service_name: str) -> None:
    serve_state.remove_reserved_fill_claim(service_name)
    _GRANT_CACHE.pop(service_name, None)


def _claim_input(row: Dict[str, Any]) -> ClaimInput:
    effective_cap = row.get('effective_cap')
    weight = float(row['weight'] or 1.0)
    if not math.isfinite(weight):
        # Defensive: SkyServiceSpec rejects non-finite weights, but a
        # poisoned DB row (older writer, manual surgery) must not crash
        # water-filling (inf/inf -> NaN in rounding) EVERY round for the
        # pool while the claim stays live. Clamp loudly to the default.
        logger.warning(
            f'Reserved-fill broker: claim of {row["service_name"]!r} '
            f'carries a non-finite weight {weight!r}; clamping to 1.0.')
        weight = 1.0
    elif weight > constants.RESERVED_FILL_MAX_WEIGHT:
        # Same defense for finite-but-out-of-bound weights (the spec
        # rejects them at construction): clamp to the documented bound so
        # a poisoned row degrades to an extreme-but-finite share instead
        # of crashing rounds (the water-fill normalization above is the
        # second layer).
        logger.warning(
            f'Reserved-fill broker: claim of {row["service_name"]!r} '
            f'carries weight {weight!r} above the supported maximum; '
            f'clamping to {constants.RESERVED_FILL_MAX_WEIGHT}.')
        weight = float(constants.RESERVED_FILL_MAX_WEIGHT)
    return ClaimInput(floor=int(row['floor_replicas'] or 0),
                      weight=weight,
                      holdings_fill=int(row['holdings_fill'] or 0),
                      launchable=bool(row['launchable']),
                      effective_cap=(int(effective_cap)
                                     if effective_cap is not None else None))


def _reject_mixed_gpus_per_replica(
        pool_key: str, rows: Dict[str, Dict[str,
                                            Any]]) -> Dict[str, Dict[str, Any]]:
    """Rejects claims disagreeing on gpus_per_replica (v1 uniform pools).

    Integer replica-slot bookkeeping is only sound when every claimant of a
    pool converts GPUs to slots the same way. Deterministic survivor rule:
    the gpus_per_replica value shared by the most claimants wins, ties by
    the smaller value; the losers' claims are DELETED (loud, visible) so
    their pollers re-log every interval instead of silently free-riding.
    """
    sizes = sorted({int(row['gpus_per_replica'] or 1) for row in rows.values()})
    if len(sizes) <= 1:
        return rows
    counts = {
        size: sum(1 for row in rows.values()
                  if int(row['gpus_per_replica'] or 1) == size)
        for size in sizes
    }
    winner = max(sizes, key=lambda size: (counts[size], -size))
    losers = [
        name for name, row in rows.items()
        if int(row['gpus_per_replica'] or 1) != winner
    ]
    logger.error(
        f'Reserved-fill broker: pool {pool_key} has claims with mixed '
        f'gpus_per_replica {sizes}; v1 requires a uniform pool. Rejecting '
        f'claims of {losers} (keeping gpus_per_replica={winner}).')
    for name in losers:
        serve_state.remove_reserved_fill_claim(name)
        rows.pop(name)
    return rows


def _replica_row_on_pool(info: Any, context: str, gpu_name: str) -> bool:
    """Whether a replica row's persisted location sits on the pool.

    Relaxed placement identity (mirrors the #108 fill matcher's spirit):
    Kubernetes + same context; a shape-carrying row must name the pool's
    GPU (case-insensitive), a legacy shape-less row matches on context
    alone (its bound pod still occupies the pool).
    """
    location = getattr(info, 'location', None)
    if not location:
        return False
    if str(location.get('cloud', '')).lower() != 'kubernetes':
        return False
    if location.get('region') != context:
        return False
    accelerators = location.get('accelerators') or {}
    if not accelerators:
        return True
    return any(name.lower() == gpu_name for name in accelerators)


def _row_was_launched(info: Any) -> bool:
    """Whether the row's sky.launch completed (a cluster was provisioned).

    SHUTTING_DOWN is broader than "bound graceful drainer": a
    launch-cancelled row (sky.launch INTERRUPTED mid-run) maps to
    SHUTTING_DOWN too, yet may never have bound a pod -- the measured
    free still counts its slot. Only sky_launch_status == SUCCEEDED
    means a pod was actually provisioned and keeps occupying the pool
    through the drain. Rows failing this signal are counted nowhere,
    matching physical reality (no pod); an interrupted launch whose pod
    DID partially bind reads as free-side undercount for its short
    cleanup window -- the conservative direction (never over-grant).
    """
    status_property = getattr(info, 'status_property', None)
    return (getattr(status_property, 'sky_launch_status',
                    None) == common_utils.ProcessStatus.SUCCEEDED)


def _occupying_debit(
        claim_names: List[str], pool_key: str,
        snapshot_time: float) -> Tuple[int, int, Dict[str, int], int]:
    """Row-consistent scan of every service's replica rows on the pool.

    Mirrors the #108 occupied-slot subtraction at broker level. The scan
    covers ALL services with replica rows, not just current claimants: a
    FORMER claimant (disabled, pruned, or moved to another pool) can
    leave nonterminal fill rows behind -- a queued launch not yet bound
    (invisible to the cluster query: its slot still reads free) or a live
    pod riding out its lifetime. Scanning only claimants would feed those
    slots to a peer while the orphaned launch can still start. Rows are a
    local DB read, so the wider scan costs no cluster traffic. Returns
    (feed_debit, entitlement_debit, live_fill, unclaimed_fill):

    - feed_debit (rows not READY, or created after the snapshot): applied
      to the observed free the FEED split spends. A launching pod may be
      unbound and invisible to the query, and a demand launch binding
      mid-query holds a slot the snapshot counted free; either way the
      slot must not be fed again -- never over-launch. Each claimant's
      local overlay additionally debits its OWN occupying rows from its
      feed, so this under-fills a launching service by its in-flight
      count for its whole bind->READY window; feeds only add NEW
      launches, so the cost is a delayed launch, never a cull.
    - entitlement_debit (ONLY rows created after the snapshot): applied to
      the ENTITLEMENT total. Entitlements launch nothing, and a bound
      not-READY pod is ALREADY excluded from the measured free (its node
      capacity is taken); subtracting it here again would shrink the
      whole-pool total for the entire bind->READY window, driving grants
      below the owner's holdings and culling exactly the pods that are
      booting (a broker-generated churn wave). Only the mid-query bind
      race (created_at > snapshot) still needs the debit.
    - live_fill (per-CLAIMANT CURRENT count of nonterminal pool-matched
      rows with reserved_fill=True; an entry for EVERY claimant whose
      rows were readable, 0 included): the row-consistent replacement for
      the owner's claimed holdings_fill. A claim's holdings are only as
      fresh as its owner's last heartbeat, while unclaimed_fill below is
      a live row scan; mixing the two views double-counts every replica
      that turned SHUTTING_DOWN after its owner's last poll. This is the
      same quantity the owner itself reports (nonterminal fill rows on
      its zero-cost location), just read from the rows NOW. It inherently
      includes post-snapshot fill binds, so a mid-query FILL bind stays
      attributed to its owner (the entitlement debit subtracts it from
      free; counting it here keeps the whole-pool total conserved and
      the owner's grant covering the replica the previous round's feed
      just launched). Post-snapshot DEMAND rows keep the plain debit:
      they are an external mid-query race, not arbitrated capacity.
    - unclaimed_fill (pool-wide count of fill rows occupying the pool
      that belong to NO current claimant's holdings): added to the
      ENTITLEMENT total. Two populations, same conservation reasoning:

      * Graceful drainers -- SHUTTING_DOWN rows with reserved_fill=True
        (any service) whose sky.launch SUCCEEDED (see _row_was_launched).
        A culled fill replica leaves its owner's holdings the moment it
        turns terminal, but its pod stays bound for the whole graceful
        drain (multiple broker rounds), so the measured free does not
        see the slot either; without this term the round total
        undercounts by every drainer and the shrunken Sum(holdings)
        reads as "pods physically gone", triggering immediate down-moves
        that cull warm replicas below the allocation fixpoint.
      * FORMER claimants' nonterminal pool-matched fill rows. Their
        service holds no claim, so live_fill cannot attribute them, yet
        the rows occupy (or are about to occupy) the pool exactly like a
        drainer mid-drain: conserved in the total, granted to nobody's
        holdings, and their unbound window feed-debited below so the
        slot is never fed to a peer while the orphaned launch can still
        start (the atomic persist additionally refuses new orphan rows
        -- see add_replica_if_round_epoch's live-claim predicate).

      Counted pool-wide (never re-attributed to any claimant's
      holdings): these slots back no live claim, so they must not lower
      anyone's feed need or raise a blind-round holdings floor. Draining
      DEMAND rows are deliberately NOT counted: a demand row was never
      in holdings and its bound pod was already excluded from the
      measured free while it was LIVE, so the total's view of it is
      unchanged by the drain -- demand capacity is not fill-arbitrable,
      before or during its drain (the pre-existing steady-state
      undercount by live demand pods is by design; non-claimants'
      nonterminal DEMAND rows stay invisible for the same reason).
      FAILED_CLEANUP rows are also left out on purpose: they persist
      indefinitely, and counting them forever would over-count the pool
      once the pod eventually dies (accepted: rare and launch-gated).
    """
    context, gpu_name = parse_pool_key(pool_key)
    feed_debit = 0
    entitlement_debit = 0
    live_fill: Dict[str, int] = {}
    unclaimed_fill = 0
    scan_names = set(claim_names)
    try:
        scan_names.update(serve_state.get_replica_service_names())
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(
            'Reserved-fill broker: could not enumerate replica-owning '
            'services for the round debit (scanning claimants only): '
            f'{common_utils.format_exception(e)}')
    claimants = set(claim_names)
    for name in sorted(scan_names):
        is_claimant = name in claimants
        try:
            infos = serve_state.get_replica_infos(name)
        except Exception as e:  # pylint: disable=broad-except
            # Failing to read one service's rows must not sink the round;
            # skipping its debit (and, for a claimant, falling back to its
            # possibly-stale claim holdings: no live_fill entry) is the
            # OPTIMISTIC direction, but bounded (one service, one round)
            # and self-healing.
            logger.warning(
                f'Reserved-fill broker: could not read replicas of {name!r} '
                f'for the round debit: {common_utils.format_exception(e)}')
            continue
        if is_claimant:
            live_fill[name] = 0
        for info in infos:
            if info.is_terminal:
                # Draining FILL rows still occupy their pool slot for the
                # whole graceful drain; count them into the entitlement
                # total (any service, SHUTTING_DOWN only, and only when
                # the launch actually provisioned a pod -- see the
                # unclaimed_fill docstring above for the demand-drain,
                # FAILED_CLEANUP and unbound-launch reasoning).
                if (getattr(info, 'status',
                            None) == serve_state.ReplicaStatus.SHUTTING_DOWN and
                        bool(getattr(info, 'reserved_fill', False)) and
                        _replica_row_on_pool(info, context, gpu_name) and
                        _row_was_launched(info)):
                    unclaimed_fill += 1
                continue
            if not _replica_row_on_pool(info, context, gpu_name):
                continue
            is_fill = bool(getattr(info, 'reserved_fill', False))
            if not is_claimant and not is_fill:
                # Non-claimants' demand rows stay invisible by design
                # (demand capacity is not fill-arbitrable); only their
                # fill rows are the broker's business.
                continue
            if is_fill:
                if is_claimant:
                    live_fill[name] += 1
                else:
                    # Former claimant's fill row: unclaimed occupancy,
                    # conserved like a drainer (see docstring).
                    unclaimed_fill += 1
            created_at = getattr(info, 'created_at', None)
            post_snapshot = (created_at is not None and
                             created_at > snapshot_time)
            if (not info.is_ready) or post_snapshot:
                feed_debit += 1
            if post_snapshot:
                entitlement_debit += 1
    return feed_debit, entitlement_debit, live_fill, unclaimed_fill


def _allocation_from_round(service_name: str,
                           round_row: Dict[str, Any]) -> Optional[Allocation]:
    grants = json.loads(round_row['grants'] or '{}')
    if service_name not in grants:
        # Claimed after this round was published: no allocation until the
        # next round (at most one poll interval away).
        return None
    feeds = json.loads(round_row['feeds'] or '{}')
    allocation = Allocation(grant=grants[service_name],
                            feed=int(feeds.get(service_name, 0)),
                            round_id=int(round_row['round_id']),
                            epoch=int(round_row['epoch']),
                            snapshot_time=float(round_row['snapshot_time']))
    _GRANT_CACHE[service_name] = (allocation.grant, time.time())
    return allocation


def get_my_allocation(service_name: str) -> Optional[Allocation]:
    """This service's slice of the latest published round, or None.

    None when the service has no live claim (expired/rejected) or the
    latest round predates its claim.
    """
    claims = serve_state.get_reserved_fill_claims()
    row = next((row for row in claims if row['service_name'] == service_name),
               None)
    if row is None:
        return None
    if time.time() - float(row['heartbeat_ts'] or 0) > claim_ttl_seconds():
        return None
    round_row = serve_state.get_reserved_fill_round(row['pool_key'])
    if round_row is None:
        return None
    return _allocation_from_round(service_name, round_row)


def run_round_if_stale(service_name: str, pool_key: str,
                       query_fn: Callable[[], Optional[PoolObservation]],
                       poll_interval_seconds: float) -> Optional[Allocation]:
    """Reads the pool's round, driving a fresh one if it went stale.

    The caller (a service's capacity poller) must have upserted its claim
    first. Under the cross-process broker lock: if the published round is
    younger than ~one poll interval, return the caller's slice of it (no
    cluster query -- this is what collapses N per-interval queries to one);
    otherwise drive a new round: CAS-advance the global lease FIRST to
    take an ownership token (the round's entry point), then read all live
    claims and the previous round (reads-after-token), snapshot time
    BEFORE the slow query, validate, debit, allocate, publish atomically
    conditional on the lease still holding that exact token.

    The broker lock also excludes fill-row persists (see
    persist_fill_replica): the round holds it from its debit scan through
    its publish, so a launch's row lands either before the scan (counted)
    or after the publish (fenced by the bumped epoch) -- never inside the
    scan->publish window where it would be counted by neither.

    Returns None when the caller holds no live claim (expired, or rejected
    by a validation) or the round could not be driven; the caller then
    feeds its autoscaler zero free slots (existing holdings keep their
    shelter via zero_cost_count, no new fill).
    """
    try:
        with locks.get_lock(
                constants.RESERVED_FILL_BROKER_LOCK_ID,
                timeout=constants.RESERVED_FILL_BROKER_LOCK_TIMEOUT_SECONDS):
            return _run_round_locked(service_name, pool_key, query_fn,
                                     poll_interval_seconds)
    except locks.LockTimeout:
        logger.warning(
            'Reserved-fill broker: timed out waiting for the round lock '
            f'(service {service_name!r}, pool {pool_key}); skipping this '
            'cycle.')
        return None


def _run_round_locked(service_name: str, pool_key: str,
                      query_fn: Callable[[], Optional[PoolObservation]],
                      poll_interval_seconds: float) -> Optional[Allocation]:
    now = time.time()
    pruned = serve_state.prune_reserved_fill_claims(now - claim_ttl_seconds())
    if pruned:
        logger.warning('Reserved-fill broker: pruned expired claim(s) of '
                       f'{pruned}.')
    claim_rows = {
        row['service_name']: row
        for row in serve_state.get_reserved_fill_claims(pool_key=pool_key)
    }
    claim_rows = _reject_mixed_gpus_per_replica(pool_key, claim_rows)
    if service_name not in claim_rows:
        # Our own claim was pruned or rejected; the poller will re-upsert
        # (and re-trip any validation, loudly) next interval.
        return None
    round_row = serve_state.get_reserved_fill_round(pool_key)
    if (round_row is not None and now - float(round_row['snapshot_time']) <
            _ROUND_FRESH_FRACTION * poll_interval_seconds):
        return _allocation_from_round(service_name, round_row)

    # ---- Drive a new round: ownership token FIRST. ----
    # TOKEN-FIRST ordering invariant (the other half lives in
    # serve_state.acquire_reserved_fill_lease_token): the token is the
    # round's entry point, CAS-advanced and committed before ANY state
    # that feeds the publish is read -- the claims, the previous round row
    # and the slow cluster query all come after it. The advisory round
    # lock can die mid-round (e.g. a PostgreSQL advisory-lock session
    # drop), letting a replacement writer drive and publish a newer round
    # while this writer is suspended anywhere below; because the publish
    # CASes on this exact token and the replacement's own advance
    # invalidates it, a writer resuming with pre-replacement state can
    # never publish it (rowcount 0 -> rollback -> observation discarded)
    # -- no per-pool epoch regress, no clearing of a peer's fence_pending
    # marker. The claims/round reads ABOVE this line serve only the read
    # path (freshness gate) and are re-read below.
    lease_ttl_seconds = (constants.RESERVED_FILL_LEASE_TTL_INTERVALS *
                         poll_interval_seconds)
    # A post-expiry acquisition (dead gap: no rounds at all for a lease
    # TTL) also stamps the persistent per-pool fence_pending marker in the
    # same transaction; see acquire_reserved_fill_lease_token for the
    # crash-window reasoning and the epoch computation below for the bump
    # it forces.
    acquired = serve_state.acquire_reserved_fill_lease_token(now=now,
                                                             expires_at=now +
                                                             lease_ttl_seconds)
    if acquired is None:
        logger.error(
            'Reserved-fill broker: lost the lease-token race before the '
            f'round query (pool {pool_key}); a writer bypassed the round '
            'lock. Skipping this cycle.')
        return None
    lease_token, lease_expired = acquired
    # Reads-after-token: the claim set and the previous round feeding the
    # publish below.
    claim_rows = {
        row['service_name']: row
        for row in serve_state.get_reserved_fill_claims(pool_key=pool_key)
    }
    claim_rows = _reject_mixed_gpus_per_replica(pool_key, claim_rows)
    if service_name not in claim_rows:
        # Our claim vanished between the pre-token check and here (only
        # possible when the round lock was bypassed); same reaction as
        # the pre-token miss.
        return None
    round_row = serve_state.get_reserved_fill_round(pool_key)
    # Snapshot time BEFORE the slow cluster query: a zero-cost row created
    # while the query runs already occupies a slot the query may still have
    # counted free, and the created_at > snapshot_time debit only catches it
    # if the snapshot predates the row.
    snapshot_time = time.time()
    observation: Optional[PoolObservation] = None
    try:
        observation = query_fn()
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Reserved-fill broker: pool query failed for '
                       f'{pool_key}: {common_utils.format_exception(e)}')
    query_ok = observation is not None and observation.free_slots is not None
    prev_phantom_streak = (int(round_row['phantom_streak'] or 0)
                           if round_row is not None else 0)
    # Carried unchanged through a measurement blackout: a failed query is
    # not an observation, so it neither confirms nor clears a phantom
    # suspicion.
    phantom_streak = prev_phantom_streak
    if query_ok:
        assert observation is not None
        if observation.gpu_names:
            phantom_streak = 0
        else:
            # Phantom pool: the claimed GPU resolves to no labeled nodes.
            # kubernetes_catalog reports empty dicts WITHOUT raising on
            # credential/cache/label-formatter failures, so one phantom
            # reading can be a transient kube-apiserver blip disguised as
            # a successful observation. Require N consecutive phantom
            # observations before rejecting every claim on the pool
            # (their pollers re-log per interval); until confirmed, treat
            # the round as a measurement blackout: feed 0 (conservative),
            # release nothing, keep the claims.
            phantom_streak = prev_phantom_streak + 1
            if (phantom_streak >=
                    constants.RESERVED_FILL_PHANTOM_CONFIRM_ROUNDS):
                logger.error(
                    f'Reserved-fill broker: pool {pool_key} is phantom (the '
                    'realtime query reports no such accelerator in the '
                    f'context, {phantom_streak} consecutive rounds). '
                    'Rejecting all claims on it.')
                serve_state.remove_reserved_fill_claims_for_pool(pool_key)
                # Fall through and PUBLISH an empty (blackout) round
                # instead of returning here: without a published round
                # the freshness gate never engages, so every claimant's
                # poller re-drives the full cluster query each interval
                # forever (N x duplication) with the pinned streak
                # re-confirming each time. With the claim rows emptied
                # the round below computes empty grants/feeds; readers
                # then get no allocation (feed 0) WITHOUT driving a query
                # for the rest of the interval. Grants going from
                # something to nothing is an allocation change, so the
                # transition bumps the fencing epoch once
                # (re-confirmations republish identical empty grants and
                # keep it stable); a later healthy observation still
                # resets the streak, and the re-upserted claims resume
                # normal rounds.
                claim_rows = {}
            else:
                logger.warning(
                    f'Reserved-fill broker: pool {pool_key} looks phantom '
                    f'({phantom_streak} consecutive observation(s), need '
                    f'{constants.RESERVED_FILL_PHANTOM_CONFIRM_ROUNDS} to '
                    'reject claims); treating the round as a measurement '
                    'blackout.')
            query_ok = False

    claims = {name: _claim_input(row) for name, row in claim_rows.items()}
    names = sorted(claims)
    prev_grants_json: Dict[str, Any] = (json.loads(round_row['grants'] or '{}')
                                        if round_row is not None else {})
    prev_raw: Dict[str, int] = (json.loads(round_row['raw_grants'] or '{}')
                                if round_row is not None else {})
    sticky: Dict[str, Dict[str, Any]] = (json.loads(
        round_row['feed_state'] or '{}') if round_row is not None else {})
    last_free: Optional[int] = (round_row['last_observed_free']
                                if round_row is not None else None)
    last_free_ts: Optional[float] = (round_row['last_observed_free_ts']
                                     if round_row is not None else None)
    sum_holdings = sum(claim.holdings_fill for claim in claims.values())

    grants: Dict[str, Optional[int]]
    if len(claims) == 1:
        # SINGLE-CLAIMANT FAST PATH: #108 identity. No ceiling, feed = raw
        # measured free (a failed query reads 0 free, exactly like the
        # pre-broker poller), no debit (the local overlay already debits its
        # own rows), no damping (the local two-poll damping is untouched),
        # no stickiness.
        assert names == [service_name], (names, service_name)
        free = 0
        if query_ok:
            assert observation is not None and observation.free_slots is not None
            free = max(0, int(observation.free_slots))
            last_free, last_free_ts = free, snapshot_time
        grants = {service_name: None}
        feeds = {service_name: free}
        raw_grants: Dict[str, int] = {}
        new_sticky: Dict[str, Dict[str, Any]] = {}
        # No debit scan on the fast path (#108 identity), so no draining
        # term either; harmless -- a single-claimant round's stored sum is
        # never a damping baseline (its None grant carries no integer
        # baseline into the next multi-claimant round). Any pending shrink
        # candidate is dropped for the same reason: with the peers gone
        # there is no damping bypass left to confirm.
        published_sum_holdings = sum_holdings
        new_shrink_baseline: Optional[int] = None
    else:
        # The debit scan runs on blind rounds too (replica rows are DB
        # reads, not cluster queries): draining rows keep occupying the
        # pool regardless of whether this round's measurement succeeded,
        # the live-holdings correction below must apply while blind, and
        # the conservation bookkeeping must not flip on a blackout.
        (feed_debit, entitlement_debit, live_fill,
         unclaimed_fill) = _occupying_debit(names, pool_key, snapshot_time)
        # One row-consistent view: a claim's holdings_fill is only as
        # fresh as its owner's last heartbeat, while unclaimed_fill comes
        # from the live row scan above -- summing the two double-counts
        # every replica that turned SHUTTING_DOWN after its owner's last
        # poll (the stale claim still holds it AND the scan counts it
        # draining), inflating the pool total (over-grants, too-permissive
        # demand gate) until the owner re-heartbeats. For every claimant
        # whose rows were readable the scan-derived CURRENT nonterminal
        # fill count REPLACES the claim's holdings for all round math
        # (grants, feeds, the holdings-shrank bypass, the blind-round
        # floor); the claim value is only the fallback when the scan
        # could not cover that service (see _occupying_debit).
        if live_fill:
            claims = {
                name: (dataclasses.replace(claim, holdings_fill=live_fill[name])
                       if name in live_fill else claim)
                for name, claim in claims.items()
            }
            sum_holdings = sum(claim.holdings_fill for claim in claims.values())
        # Conservation invariant: the whole-pool total is observed free +
        # live fill holdings + unclaimed fill rows (drainers and former
        # claimants' orphaned rows). A drainer has left its owner's
        # holdings but its pod still occupies the pool (excluded from the
        # measured free), so without the unclaimed term every in-flight
        # cull shrinks the total below the pool's real capacity and the
        # round reclaims slots that are not actually gone; an orphaned
        # fill row occupies its slot the same way, just with no claim
        # left to ever re-adopt it.
        conserved_holdings = sum_holdings + unclaimed_fill
        # Previous single-claimant None grants carry no integer baseline:
        # drop them so damping treats the service as newly-baselined.
        prev_published: Optional[Dict[str, int]] = None
        if round_row is not None:
            prev_published = {
                name: value
                for name, value in prev_grants_json.items()
                if isinstance(value, int)
            }
        prev_sum_holdings = (round_row['sum_holdings']
                             if round_row is not None else None)
        prev_shrink_baseline = (round_row['shrink_baseline']
                                if round_row is not None else None)
        if query_ok:
            assert observation is not None and observation.free_slots is not None
            measured = max(0, int(observation.free_slots))
            last_free, last_free_ts = measured, snapshot_time
            observed_free = max(0, measured - feed_debit)
            # The entitlement total only debits the mid-query bind race:
            # bound not-READY pods are already excluded from the measured
            # free AND counted in their owner's fill holdings, so the full
            # feed debit here would double-subtract them for the whole
            # bind->READY window and cull the booting pods (see
            # _occupying_debit). A mid-query FILL bind stays attributed to
            # its owner through the live_fill holdings above, keeping the
            # total conserved.
            entitlement_free = max(0, measured - entitlement_debit)
            total = entitlement_free + conserved_holdings
            raw_grants = compute_entitlements(total, claims)
            # The immediate-down bypass keys on (holdings + draining): a
            # holdings drop whose slots merely moved into a graceful drain
            # is NOT capacity that physically vanished -- the drainers'
            # pods are still bound. And a one-round conserved shrink can
            # be a pure observation artifact: a drain completing between
            # the cluster query and the row scan leaves the slot counted
            # occupied by the query (not free) yet already deleted from
            # the rows (not held, not draining), so BOTH terms omit it for
            # exactly this round; firing the bypass on that phantom culls
            # a warm replica the next query would have vindicated. The
            # bypass therefore requires CONFIRMATION: a shrink below the
            # previous round's conserved sum only records that sum as a
            # pending baseline (this round takes the normal two-round
            # damped path), and only a next round still below the baseline
            # treats the capacity as physically gone. A legitimate fast
            # reclaim (pods really deleted) loses at most one round of
            # down-speed to this -- acceptable, and the ordinary two-round
            # damped down usually lands the same round anyway.
            new_shrink_baseline = None
            if (prev_shrink_baseline is not None and
                    conserved_holdings < int(prev_shrink_baseline)):
                # Confirmed: the shrink persisted across two consecutive
                # row-consistent scans -- pods are physically gone.
                holdings_shrank = True
            elif (prev_sum_holdings is not None and
                  conserved_holdings < int(prev_sum_holdings)):
                # First observation of this shrink: could be the
                # query-then-scan gap; damp normally and remember the
                # pre-shrink baseline for next round's confirmation.
                holdings_shrank = False
                new_shrink_baseline = int(prev_sum_holdings)
            else:
                holdings_shrank = False
            damped = damp_grants(raw_grants, prev_published, prev_raw,
                                 holdings_shrank)
            # raw_grants clamps each feed need to min(damped, raw): a
            # service inside a down-move's damping window must not be fed
            # above its raw entitlement -- the damped grant catches down
            # next round and the just-launched replica would be culled.
            feeds, new_sticky = compute_feeds(
                observed_free,
                damped,
                claims,
                sticky,
                now,
                constants.RESERVED_FILL_STICKY_FEED_INTERVALS *
                poll_interval_seconds,
                raw_grants=raw_grants)
            published_sum_holdings = conserved_holdings
        else:
            # Measurement blackout: a failed query is not an observation,
            # so it must not CHANGE the allocation -- the previous round's
            # grants are carried forward as-is (floored at each claimant's
            # CURRENT holdings so a blackout never strips a live replica's
            # shelter), never recomputed. Recomputing from a synthesized
            # total (stale last-known free + current holdings) double-
            # counts every slot consumed since the last good measurement:
            # 10 free observed -> 10 launched -> blackout would read
            # 10 + 10 = 20 and the inflated grants would reopen the
            # demand-placement gate on a ten-slot pool. Feeds are 0 (never
            # launch blind), sticky state is carried unchanged (its window
            # is wall-clock, so a short blackout does not break an
            # in-progress streak), and the raw-grant damping baselines,
            # sum_holdings and any pending shrink baseline are carried too
            # -- the blackout is fully transparent to the shrink
            # confirmation, which then compares the last measured round
            # directly against the next one (no bypass evaluation happens
            # on a carried round: grants are not recomputed at all). A
            # claimant with no previous grant (joined during the blackout)
            # gets its holdings floor: nothing new, nothing stripped.
            raw_grants = {name: int(value) for name, value in prev_raw.items()}
            damped = {}
            for name, claim in claims.items():
                base = (prev_published.get(name)
                        if prev_published is not None else None)
                damped[name] = max(base if base is not None else 0,
                                   claim.holdings_fill)
            feeds = {name: 0 for name in claims}
            new_sticky = dict(sticky)
            published_sum_holdings = (int(prev_sum_holdings)
                                      if prev_sum_holdings is not None else
                                      conserved_holdings)
            new_shrink_baseline = (int(prev_shrink_baseline) if
                                   prev_shrink_baseline is not None else None)
        grants = dict(damped)

    grants_changed = round_row is None or prev_grants_json != grants
    # Feeds are part of the allocation the fence protects: a feed-only
    # redistribution (grants damped in place while the launchable-now
    # split moved to a peer) or a positive-feed round giving way to a
    # blackout must fence launch batches queued under the previous round
    # -- their slots may now be fed to someone else, or unmeasurable.
    # Multi-claimant rounds only: the single-claimant fast-path feed is
    # the raw measured free (fluctuates every round, redistributes to
    # nobody), and bumping on it would fence steady-state fill launches
    # the pre-broker #108 path never fenced.
    feeds_changed = (len(claims) != 1 and round_row is not None and
                     json.loads(round_row['feeds'] or '{}') != feeds)
    # The ROUND epoch is per-pool (the fencing token the launch path
    # compares against -- pool A's grant churn must not fence pool B's
    # launches). It bumps only when THIS pool's allocation (grants OR
    # feeds) changes, or after a lease-dead gap where every outstanding
    # grant is suspect -- not on every round: per-round bumps would fence
    # out nearly every fill launch in steady state (each service's carried
    # epoch is refreshed only on its own poll), while the fencing intent
    # is precisely "never actuate a superseded allocation". The LEASE
    # epoch is a separate global stream advanced unconditionally per
    # driven round (the pre-query ownership token above).
    prev_round_epoch = (int(round_row['epoch'])
                        if round_row is not None else None)
    new_epoch = prev_round_epoch if prev_round_epoch is not None else 0
    # lease_expired covers the writer that OBSERVED the dead gap;
    # fence_pending covers its crash window: a post-expiry writer's token
    # acquisition already refreshed expires_at, so if it died before
    # publishing, the next writer reads an unexpired lease and only the
    # persisted per-pool marker still demands the bump. The successful
    # publish below clears the marker in the same transaction (safe: any
    # concurrent marker-setter advanced the lease, so this publish would
    # CAS-fail instead of clearing).
    fence_pending = (bool(round_row['fence_pending'])
                     if round_row is not None else False)
    if grants_changed or feeds_changed or lease_expired or fence_pending:
        new_epoch += 1
    round_id = int(round_row['round_id']) + 1 if round_row is not None else 1
    published = serve_state.publish_reserved_fill_round(
        pool_key,
        round_id=round_id,
        snapshot_time=snapshot_time,
        epoch=new_epoch,
        grants=json.dumps(grants, sort_keys=True),
        feeds=json.dumps(feeds, sort_keys=True),
        raw_grants=json.dumps(raw_grants, sort_keys=True),
        feed_state=json.dumps(new_sticky, sort_keys=True),
        sum_holdings=published_sum_holdings,
        last_observed_free=last_free,
        last_observed_free_ts=last_free_ts,
        phantom_streak=phantom_streak,
        shrink_baseline=new_shrink_baseline,
        lease_token=lease_token,
        lease_expires_at=now + lease_ttl_seconds)
    if not published:
        logger.error(
            'Reserved-fill broker: lease token superseded while publishing '
            f'round {round_id} for pool {pool_key} (token {lease_token}); a '
            'replacement writer took over mid-query. Discarding this '
            'observation.')
        return None
    logger.info(
        f'Reserved-fill broker: round {round_id} (epoch {new_epoch}) for '
        f'pool {pool_key}: grants={grants} feeds={feeds} '
        f'claimants={names}.')
    if service_name not in grants:
        # Confirmed-phantom blackout round: our claim was just rejected
        # along with everyone else's, so there is no allocation to hand
        # back (the caller feeds its autoscaler 0 free slots). Every
        # normal round grants exactly its claimants.
        return None
    allocation = Allocation(grant=grants.get(service_name),
                            feed=int(feeds.get(service_name, 0)),
                            round_id=round_id,
                            epoch=new_epoch,
                            snapshot_time=snapshot_time)
    _GRANT_CACHE[service_name] = (allocation.grant, time.time())
    return allocation
