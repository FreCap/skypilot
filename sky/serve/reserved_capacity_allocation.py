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
    # Utilization gate release target: the ceiling the claimant has walked
    # itself down to after demonstrating no work. None = ungated (gate
    # explicitly disabled, or a claim written by a binary that predates the
    # gate), which is the exact static-reservation behavior. Unlike
    # effective_cap, this ceiling also clamps the reserved floor: an idle
    # gated claimant can release its whole fill reservation.
    utilization_cap: int | None = None

    def attainable_floor(self) -> int:
        """The floor, clamped to what the claimant can materialize."""
        floor = max(0, self.floor)
        if self.effective_cap is not None:
            floor = min(floor, max(0, self.effective_cap))
        return floor

    def allocation_floor(self) -> int:
        """The floor retained in this round after utilization gating."""
        floor = self.attainable_floor()
        if self.utilization_cap is not None:
            floor = min(floor, max(0, self.utilization_cap))
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

    Floors are clamped to each claimant's effective_cap and utilization_cap
    first. An unattainable or activity-unbacked floor must not absorb
    entitlement; the clamped excess joins the weighted remainder and flows
    to peers. The water-fill share is capped by each total ceiling minus the
    retained floor, so the whole entitlement never exceeds either ceiling.
    """
    floors = scale_floors(total, {
        name: claim.allocation_floor() for name, claim in claims.items()
    })
    remainder = max(0, total) - sum(floors.values())
    caps: dict[str, int | None] = {}
    for name, claim in claims.items():
        # Both caps bound the same quantity (the weighted share ABOVE the
        # retained floor), so the binding one is their min, with None meaning
        # unbounded on either side. A utilization cap of 0 therefore removes
        # both the floor and headroom for an idle gated claimant.
        bounds = []
        if claim.effective_cap is not None:
            bounds.append(max(0, claim.effective_cap - floors[name]))
        if claim.utilization_cap is not None:
            bounds.append(max(0, claim.utilization_cap - floors[name]))
        caps[name] = min(bounds) if bounds else None
    shares = water_fill(remainder, {
        name: claim.weight for name, claim in claims.items()
    }, caps)
    return {name: floors[name] + shares[name] for name in claims}


def _release_entry(cap: int, hot_until: float, stepped_at: float,
                   blind_since: float | None) -> dict[str, Any]:
    return {
        'cap': int(cap),
        'hot_until': float(hot_until),
        'stepped_at': float(stepped_at),
        'blind_since': blind_since,
    }


def advance_release_target(prev: Mapping[str, Any] | None, *, floor: int,
                           holdings: int, need: int, boot_hold: bool,
                           blind: bool, now: float, dwell: float,
                           step_seconds: float, step_fraction: float,
                           min_step: int, headroom: float,
                           blind_grace: float) -> dict[str, Any]:
    """One claimant's release target, advanced by one round.

    Pure: all state is in `prev` and the return value, both JSON-shaped so
    the caller can persist them on the round row. The cap is a CEILING on
    the claimant's entitlement, never a target the fleet is driven to; the
    ordinary autoscaler and effective_cap stay the binding constraints
    below it.

    Rising is instantaneous and one-sided. Releasing is a dwell followed by a
    bounded step schedule, and every step is gated on the previous one being
    physically actuated. That asymmetry is the whole safety argument: active
    demand reclaims utilization-proportional entitlement within one round,
    while giving it up takes many rounds and can be interrupted at any point
    by a single non-zero sample.
    """
    cap = int(prev['cap']) if prev else max(floor, holdings)
    hot_until = float(prev['hot_until']) if prev else now + dwell
    stepped_at = float(prev['stepped_at']) if prev else now
    blind_since = prev.get('blind_since') if prev else None

    # The blind_since value to persist on the fall-through (non-freeze)
    # returns. None outside the blind path (today's exact behavior); the
    # PRESERVED stamp on the wedged-past-grace path. Resetting it to None
    # there re-arms the grace timer next round (blind_since is None ->
    # re-stamped to `now` -> back inside the grace window -> FREEZE), and
    # because the freeze branch keeps pushing hot_until to now + dwell every
    # round, the single round that reaches past-grace always lands in the
    # `now < hot_until` dwell branch and re-freezes before it can step. The
    # net effect of resetting to None is that a permanently blind claimant
    # freezes forever and NEVER resumes the decay -- the exact pin the grace
    # period exists to prevent. Preserving the stamp keeps `now - blind_since
    # > blind_grace` true on subsequent rounds so the decay actually resumes.
    resumed_blind_since: float | None = None
    if blind:
        blind_since = now if blind_since is None else float(blind_since)
        if now - blind_since <= blind_grace:
            # FREEZE. Deliberately neither raises nor lowers. Raising to
            # max(cap, holdings) would undo a decay in progress on every
            # blind round, and since every serve controller lives in the
            # api-server pod, a routine deploy makes every claimant blind
            # at once: the pool would reset its decay on each deploy and
            # never complete a release. Lowering is equally wrong, because
            # a blind claimant may be fully busy. The step clock is paused
            # so the grace period cannot bank steps.
            return _release_entry(cap, max(hot_until, now + dwell), now,
                                  blind_since)
        # Wedged past the grace period: a permanently broken telemetry path
        # must not pin the pool forever, so resume the decay as if idle.
        # Keep the original blind_since stamp so the grace window stays
        # expired and the decay continues round over round instead of
        # re-freezing (see resumed_blind_since above). recovery to a
        # non-blind round clears it via the else branch.
        need, boot_hold = 0, False
        resumed_blind_since = blind_since
    else:
        blind_since = None

    target = max(floor, math.ceil(need * (1.0 + headroom)))
    if target > cap:
        # RISE: one round, no dwell and no step schedule. hot_until is
        # pushed out so the release schedule restarts from scratch.
        return _release_entry(target, now + dwell, now, None)
    if boot_hold:
        # Fill replicas this claimant already authorized are still coming
        # up. Stepping here would cull them mid-boot: pre-ready rows are
        # the FIRST scale-down victims, so the gate would repeatedly order
        # capacity and destroy it before it ever served a request.
        return _release_entry(cap, max(hot_until, now + dwell), now,
                              resumed_blind_since)
    if now < hot_until:
        return _release_entry(cap, hot_until, stepped_at, resumed_blind_since)
    if holdings > cap:
        # ACTUATION GATE: the previous step has not drained yet. Holding
        # here keeps the cap at most one step ahead of physical reality,
        # and turns a stuck drain into a visibly stalled cap rather than a
        # cap parked at the floor with the fleet still running.
        return _release_entry(cap, hot_until, now, resumed_blind_since)
    if now - stepped_at < step_seconds:
        return _release_entry(cap, hot_until, stepped_at, resumed_blind_since)
    # Step from what the claimant actually holds, not from the cap: a cap
    # left high by a rise the fleet never materialized must not authorize a
    # correspondingly huge step.
    anchor = max(floor, min(cap, holdings))
    step = max(min_step, math.ceil(step_fraction * (anchor - floor)))
    return _release_entry(max(floor, anchor - step), hot_until, now,
                          resumed_blind_since)


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
