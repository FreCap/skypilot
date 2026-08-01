"""Pure exact-card compatibility policy for Serve autoscalers."""
import typing

if typing.TYPE_CHECKING:
    from sky.serve import replica_managers


def _allocate_compatibility_target(
    *,
    configured_cards: list[str],
    capacities: dict[str, float],
    floors: dict[str, int],
    min_replicas: int,
    max_replicas: int,
    demand_profiles: list[tuple[int, tuple[str, ...], float]],
    fixed_work_by_accelerator: dict[str, float],
    ready_zero_cost: dict[str, int],
    ready: dict[str, int],
    provisioning: dict[str, int],
    free_reserved: dict[str, int],
    cold_order: list[str],
    use_existing_supply: bool,
) -> dict[str, int]:
    """Allocate exact-card work into one bounded per-card target.

    `fixed_work_by_accelerator` is already-running or conservatively unknown
    work. It cannot move without preemption, so it consumes capacity only on
    its current card before flexible queued/rejected profiles are considered.
    `demand_profiles` contains work units, not request counts, which lets the
    same scarcity/supply allocator serve both QPS and concurrency policies.
    """
    demand_epsilon = 1e-9
    # A logical scale-up wave can deliberately place a ceiling below the
    # eventual hard floors. Admit those floors incrementally instead of
    # returning a map whose sum exceeds the actuation fence. The configured
    # order is the stable tie-break until later waves complete every floor.
    target: dict[str, int] = {}
    remaining_floor_budget = max(0, max_replicas)
    for card in configured_cards:
        floor = min(max(0, int(floors.get(card.casefold(), 0))),
                    remaining_floor_budget)
        target[card] = floor
        remaining_floor_budget -= floor
    unused_capacity = {
        card: target[card] * max(0.0, capacities.get(card, 0.0))
        for card in configured_cards
    }

    # Running work is non-preemptive. Pin its target to the card already
    # serving it before assigning any flexible backlog.
    for card in configured_cards:
        remaining = max(0.0, fixed_work_by_accelerator.get(card, 0.0))
        consumed = min(remaining, unused_capacity.get(card, 0.0))
        remaining -= consumed
        unused_capacity[card] = max(0.0,
                                    unused_capacity.get(card, 0.0) - consumed)
        capacity = capacities.get(card, 0.0)
        if capacity <= 0:
            continue
        while (remaining > demand_epsilon and
               sum(target.values()) < max_replicas):
            target[card] = target.get(card, 0) + 1
            if capacity > remaining:
                unused_capacity[card] = (unused_capacity.get(card, 0.0) +
                                         capacity - remaining)
                remaining = 0.0
            else:
                remaining -= capacity

    cold_rank = {card: index for index, card in enumerate(cold_order)}
    canonical_by_name = {card.casefold(): card for card in configured_cards}
    grouped: dict[tuple[int, tuple[str, ...]], float] = {}
    for priority, raw_compatible, work in demand_profiles:
        requested = {
            canonical_by_name[card.casefold()]
            for card in raw_compatible
            if card.casefold() in canonical_by_name
        }
        # Compatibility is a set. Canonicalize by live paid-card order so
        # caller list order never becomes a hardware preference.
        compatible = tuple(card for card in cold_order
                           if card in requested and card in capacities)
        if not compatible or work <= demand_epsilon:
            continue
        key = (priority, compatible)
        grouped[key] = grouped.get(key, 0.0) + float(work)

    # Demand attribution and actuation use the same compatibility allocator
    # with different supply semantics. The durable/displayed demand map skips
    # these tiers and assigns flexible work to the cheapest compatible cold
    # card. A second actuation pass enables the tiers so compatible warm or
    # committed supply suppresses duplicate launches without reattributing the
    # traffic target.
    planned_by_tier: list[dict[str, int]] = []
    if use_existing_supply:
        planned_by_tier = [dict(ready_zero_cost), dict(ready)]
        planned_by_tier.append({
            card: ready.get(card, 0) + provisioning.get(card, 0)
            for card in configured_cards
        })
        planned_by_tier.append({
            card: (ready.get(card, 0) + provisioning.get(card, 0) +
                   free_reserved.get(card, 0)) for card in configured_cards
        })

    def fallback_after_next_assignment(
            compatible: tuple[str, ...]) -> tuple[int, int]:
        """Return the second-best marginal supply tier for one profile."""
        options: list[tuple[int, int]] = []
        for card in compatible:
            if unused_capacity.get(card, 0.0) > 0:
                options.append((0, cold_rank[card]))
            previous_count = target.get(card, 0)
            for tier_index, tier in enumerate(planned_by_tier, start=1):
                tier_count = max(previous_count, tier.get(card, 0))
                # Two copies are sufficient: only the best option is consumed
                # before the profile's fallback is compared.
                options.extend([(tier_index, cold_rank[card])] *
                               min(2, max(0, tier_count - previous_count)))
                previous_count = tier_count
            options.append((len(planned_by_tier) + 1, cold_rank[card]))
        options.sort()
        if len(options) > 1:
            return options[1]
        if options:
            return options[0]
        return len(planned_by_tier) + 2, len(cold_order)

    groups_by_priority: dict[int, list[tuple[tuple[str, ...], float]]] = {}
    for (priority, compatible), work in grouped.items():
        groups_by_priority.setdefault(priority, []).append((compatible, work))
    for priority in sorted(groups_by_priority, reverse=True):
        pending = groups_by_priority[priority]
        while pending:
            # Protect the profile whose best non-selected fallback is worse.
            # Stable list order preserves report/FIFO order on a true tie.
            fallback_keys = [
                tuple(-value
                      for value in fallback_after_next_assignment(compatible))
                for compatible, _ in pending
            ]
            selected_index = min(range(len(pending)),
                                 key=fallback_keys.__getitem__)
            compatible, remaining = pending.pop(selected_index)
            if sum(target.values()) >= max_replicas:
                continue
            for card in compatible:
                consumed = min(remaining, unused_capacity.get(card, 0.0))
                remaining -= consumed
                unused_capacity[card] = max(
                    0.0,
                    unused_capacity.get(card, 0.0) - consumed)
                if remaining <= demand_epsilon:
                    break
            while (remaining > demand_epsilon and
                   sum(target.values()) < max_replicas):
                selected: str | None = None
                for tier in planned_by_tier:
                    selected = next(
                        (card for card in compatible
                         if tier.get(card, 0) > target.get(card, 0)), None)
                    if selected is not None:
                        break
                if selected is None:
                    selected = next(
                        card for card in cold_order if card in compatible)
                capacity = capacities.get(selected, 0.0)
                if capacity <= 0:
                    break
                target[selected] = target.get(selected, 0) + 1
                if capacity > remaining:
                    unused_capacity[selected] = (
                        unused_capacity.get(selected, 0.0) + capacity -
                        remaining)
                    remaining = 0.0
                else:
                    remaining -= capacity

    # The aggregate floor is independent from per-card floors. The demand pass
    # attributes its remainder to the cheapest card. The actuation pass may
    # instead reuse already materialized compatible supply.
    while sum(target.values()) < min_replicas and configured_cards:
        selected = None
        for tier in planned_by_tier:
            selected = next((card for card in configured_cards
                             if tier.get(card, 0) > target.get(card, 0)), None)
            if selected is not None:
                break
        if selected is None:
            selected = cold_order[0]
        target[selected] = target.get(selected, 0) + 1
    return {card: count for card, count in target.items() if count > 0}


def _replica_is_retiring_card_supply(
        replica_info: 'replica_managers.ReplicaInfo') -> bool:
    """Whether a row must not authorize replacement on its current card."""
    status = replica_info.status_property
    return (getattr(status, 'is_scale_down', False) is True or
            getattr(status, 'preempted', False) is True)


def _merge_fresh_target_into_downscale_hold(
    *,
    adopted_target: dict[str, int],
    fresh_target: dict[str, int],
    configured_cards: list[str],
    replacement_order: list[str],
    target_total: int,
) -> dict[str, int]:
    """Replace only the held slots required by current exact-card demand."""
    fresh_total = sum(fresh_target.values())
    if fresh_total > target_total:
        return {}
    if (set(adopted_target) - set(configured_cards) or
            set(fresh_target) - set(configured_cards)):
        return {}
    target = {
        card: max(0, int(adopted_target.get(card, 0)))
        for card in configured_cards
    }
    for card in configured_cards:
        target[card] = max(target[card], int(fresh_target.get(card, 0)))
    excess = sum(target.values()) - target_total
    removal_order = list(dict.fromkeys(replacement_order + configured_cards))
    for card in removal_order:
        removable = max(0, target.get(card, 0) - fresh_target.get(card, 0))
        removed = min(excess, removable)
        target[card] -= removed
        excess -= removed
        if excess == 0:
            break
    if excess != 0:
        return {}
    result = {card: count for card, count in target.items() if count > 0}
    if (sum(result.values()) != target_total or any(
            result.get(card, 0) < count
            for card, count in fresh_target.items())):
        return {}
    return result


def _revalidate_actuation_target(
    *,
    adopted_target: dict[str, int],
    desired_target: dict[str, int],
    nonretiring_supply: dict[str, int],
    configured_cards: list[str],
    final_target: int,
    allow_adopted_reassignment: bool = True,
    allow_unbacked_adopted_reassignment: bool = True,
) -> dict[str, int]:
    """Build a supply-aware actuator without bypassing target adoption.

    The adopted map owns compatibility changes, hysteresis, and logical-card
    wave limits. Actuation may immediately move an adopted unit when its card
    is no longer backed, or when the fresh placement can move that unit onto
    compatible supply that already exists. It must not cold-launch additional
    units for a not-yet-adopted compatibility migration. During an aggregate
    downscale hold, unbacked adopted units remain on their exact cards, while
    materialized compatible supply may still replace them.
    """
    if sum(desired_target.values()) != final_target:
        return {}
    target = {
        card: max(0, int(adopted_target.get(card, 0)))
        for card in configured_cards
    }

    def fill_toward_desired(count: int, *, require_backing: bool) -> int:
        for card in configured_cards:
            if count <= 0:
                break
            deficit = max(0, desired_target.get(card, 0) - target[card])
            if require_backing:
                deficit = min(
                    deficit,
                    max(0,
                        int(nonretiring_supply.get(card, 0)) - target[card]))
            added = min(count, deficit)
            target[card] += added
            count -= added
        return count

    # Generic overprovision is already part of final_target and can follow the
    # fresh supply-aware placement without changing adopted demand.
    remaining = final_target - sum(target.values())
    if remaining < 0 or fill_toward_desired(remaining,
                                            require_backing=False) != 0:
        return {}
    if not allow_adopted_reassignment:
        # A mixed-version rollout must preserve the compatibility-owned card
        # map. Old-version supply cannot prove that a cross-card replacement is
        # compatible, and moving an adopted unit here can turn a warm
        # zero-cost card into paid same-card launch authority. Generic
        # overprovision was assigned above without changing adopted demand.
        return {card: count for card, count in target.items() if count > 0}

    # First replace adopted capacity that is disappearing. This is allowed to
    # create a cold shortage, because retaining the old card would otherwise
    # turn a retirement or preemption into an accidental same-card relaunch.
    if allow_unbacked_adopted_reassignment:
        reassigned = 0
        for card in configured_cards:
            unbacked = max(
                0, target[card] - max(0, int(nonretiring_supply.get(card, 0))))
            removable = min(unbacked,
                            max(0, target[card] - desired_target.get(card, 0)))
            target[card] -= removable
            reassigned += removable
        if fill_toward_desired(reassigned, require_backing=False) != 0:
            return {}

    # Then let already-existing compatible supply replace backed capacity.
    # This lets reserved A100s serve flexible L4-attributed demand and retire
    # redundant paid L4s, without permitting a new A100 cold launch.
    movable = 0
    for card in configured_cards:
        removable = max(0, target[card] - desired_target.get(card, 0))
        target[card] -= removable
        movable += removable
    unplaced = fill_toward_desired(movable, require_backing=True)
    if unplaced > 0:
        # Restore units with no already-existing destination. Preserve service
        # order for a stable actuator across identical snapshots.
        for card in configured_cards:
            deficit = max(0, adopted_target.get(card, 0) - target.get(card, 0))
            restored = min(unplaced, deficit)
            target[card] += restored
            unplaced -= restored
            if unplaced == 0:
                break
    if unplaced != 0:
        return {}
    return {card: count for card, count in target.items() if count > 0}
