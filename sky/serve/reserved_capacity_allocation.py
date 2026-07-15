"""Deterministic allocation policy for reserved-capacity pools."""

from collections.abc import Mapping
import dataclasses
import math
from typing import Any


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
    effective_cap: int | None = None

    def attainable_floor(self) -> int:
        """The floor, clamped to what the claimant can materialize."""
        floor = max(0, self.floor)
        if self.effective_cap is not None:
            floor = min(floor, max(0, self.effective_cap))
        return floor


def _largest_remainder_round(amounts: Mapping[str, float],
                             total: int) -> dict[str, int]:
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


def scale_floors(total: int, floors: Mapping[str, int]) -> dict[str, int]:
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
               caps: Mapping[str, int | None]) -> dict[str, int]:
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
                         claims: Mapping[str, ClaimInput]) -> dict[str, int]:
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
    floors = scale_floors(total, {
        name: claim.attainable_floor() for name, claim in claims.items()
    })
    remainder = max(0, total) - sum(floors.values())
    caps: dict[str, int | None] = {}
    for name, claim in claims.items():
        if claim.effective_cap is None:
            caps[name] = None
        else:
            caps[name] = max(0, claim.effective_cap - claim.attainable_floor())
    shares = water_fill(remainder, {
        name: claim.weight for name, claim in claims.items()
    }, caps)
    return {name: floors[name] + shares[name] for name in claims}


def damp_grants(raw: Mapping[str, int], prev_grants: Mapping[str, int] | None,
                prev_raw: Mapping[str, int] | None,
                holdings_shrank: bool) -> dict[str, int]:
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
    damped: dict[str, int] = {}
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
    sticky_state: Mapping[str, dict[str, Any]],
    now: float,
    sticky_window_seconds: float,
    raw_grants: Mapping[str, int] | None = None,
) -> tuple[dict[str, int], dict[str, dict[str, Any]]]:
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
        extra = water_fill(free,
                           {name: claims[name].weight for name in eligible},
                           {name: need[name] - feeds[name] for name in eligible})
        for name, give in extra.items():
            feeds[name] += give
    # New sticky state: only positive feeds carry a streak; `since` is kept
    # from an unexpired previous entry (streak continues) and reset
    # otherwise (fresh assignment starts a new window).
    new_sticky: dict[str, dict[str, Any]] = {}
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
