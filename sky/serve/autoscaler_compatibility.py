"""Pure exact-card compatibility policy for Serve autoscalers."""
import dataclasses
import enum
import math
import typing

from sky.serve import compatibility_matching
from sky.serve import constants

if typing.TYPE_CHECKING:
    from sky.serve import replica_managers


class SupplyPreference(enum.Enum):
    """Economic ordering for compatible already-committed capacity."""

    WARM_FIRST = 'warm_first'
    ZERO_COST_FIRST = 'zero_cost_first'


@dataclasses.dataclass(frozen=True, kw_only=True)
class CompatibilityTargetPlan:
    """One exact-card target and its optional demand-owned reduction."""

    target_entries: tuple[tuple[str, int], ...]
    reservation_acquisition_classes: (
        tuple[compatibility_matching.CompatibilityDemand, ...] | None)

    def __post_init__(self) -> None:
        if (not isinstance(self.target_entries, tuple) or
                any(not isinstance(entry, tuple) or len(entry) != 2 or
                    not isinstance(entry[0], str) or not entry[0] or
                    type(entry[1]) is not int or entry[1] <= 0
                    for entry in self.target_entries)):
            raise ValueError('Compatibility target is malformed.')
        if len({card.casefold() for card, _ in self.target_entries
               }) != len(self.target_entries):
            raise ValueError('Compatibility target repeats an accelerator.')
        classes = self.reservation_acquisition_classes
        if (classes is not None and (not isinstance(classes, tuple) or any(
                not isinstance(item, compatibility_matching.CompatibilityDemand)
                for item in classes))):
            raise ValueError('Compatibility demand ownership is malformed.')
        if (classes is not None and any(card != card.casefold()
                                        for item in classes
                                        for card in item.compatible_cards)):
            raise ValueError('Compatibility demand ownership is not canonical.')
        if (classes is not None and sum(item.count for item in classes) != sum(
                count for _, count in self.target_entries)):
            raise ValueError('Compatibility demand ownership is incomplete.')

    def as_dict(self) -> dict[str, int]:
        return dict(self.target_entries)


@dataclasses.dataclass
class _DemandOwnedSlot:
    """Mutable fractional-work bin used only during one pure reduction."""

    ordinal: int
    card: str
    unused_work: float
    priority: int | None = None
    compatible_cards: frozenset[str] | None = None


@dataclasses.dataclass(frozen=True, kw_only=True)
class DeadlineDemand:
    """One queue bucket with one dispatch deadline and routing class."""

    sequence: int
    priority: int
    compatible_cards: tuple[str, ...]
    count: int
    remaining_seconds: float

    def __post_init__(self) -> None:
        if (type(self.sequence) is not int or self.sequence < 0 or
                type(self.priority) is not int or type(self.count) is not int or
                self.count < 0 or not isinstance(self.remaining_seconds,
                                                 (int, float)) or
                isinstance(self.remaining_seconds, bool) or
                not math.isfinite(float(self.remaining_seconds))):
            raise ValueError('Deadline demand is malformed.')
        compatible = tuple(
            sorted({
                card.casefold(): card
                for card in self.compatible_cards
                if isinstance(card, str) and card
            }.values(),
                   key=str.casefold))
        if self.count > 0 and not compatible:
            raise ValueError('Positive deadline demand has no accelerator.')
        object.__setattr__(self, 'compatible_cards', compatible)
        object.__setattr__(self, 'remaining_seconds',
                           float(self.remaining_seconds))


@dataclasses.dataclass(frozen=True, kw_only=True)
class DeadlineSupply:
    """One finite or prospective logical GPU slot in economic tier order."""

    card: str
    available_after_seconds: float
    tier: int

    def __post_init__(self) -> None:
        if (not isinstance(self.card, str) or not self.card or
                not isinstance(self.available_after_seconds, (int, float)) or
                isinstance(self.available_after_seconds, bool) or
                not math.isfinite(float(self.available_after_seconds)) or
                self.available_after_seconds < 0 or
                type(self.tier) is not int or self.tier < 0):
            raise ValueError('Deadline supply is malformed.')
        object.__setattr__(self, 'available_after_seconds',
                           float(self.available_after_seconds))


@dataclasses.dataclass(frozen=True, kw_only=True)
class DeadlineCapacityPlan:
    """Integer card target and queue work that no timely slot can rescue."""

    target_by_card: dict[str, int]
    infeasible_requests_by_priority: dict[int, float]
    reservation_acquisition_classes: tuple[
        compatibility_matching.CompatibilityDemand, ...] = ()


@dataclasses.dataclass
class _ActiveDeadlineSlot:
    card: str
    available_after_seconds: float
    priority: int | None = None


def _allocate_deadline_capacity_target(
    *,
    configured_cards: list[str],
    demand: list[DeadlineDemand],
    finite_supply: list[DeadlineSupply],
    paid_cold_order: list[str],
    service_seconds_by_card: dict[str, float],
    utilization: float,
    paid_cold_lead_seconds: float,
    max_slots: int,
) -> DeadlineCapacityPlan:
    """Allocate cumulative capacity-time without reusing one GPU-second.

    Supply is activated lazily.  This is important when a service already has
    more ready slots than its deadline needs: the target is the smallest
    compatible subset, rather than the entire sunk fleet.  Finite supply is
    ordered by its caller-assigned economic tier; prospective paid capacity is
    considered only after every timely finite alternative.
    """
    epsilon = 1e-9
    canonical = {card.casefold(): card for card in configured_cards}
    prospective_cold_order = []
    for raw_card in paid_cold_order:
        card = canonical.get(raw_card.casefold())
        if card is not None and card not in prospective_cold_order:
            prospective_cold_order.append(card)
    card_order = prospective_cold_order + [
        card for card in configured_cards if card not in prospective_cold_order
    ]
    cold_rank = {card: index for index, card in enumerate(card_order)}
    duration = {
        canonical[raw_card.casefold()]: float(seconds)
        for raw_card, seconds in service_seconds_by_card.items()
        if raw_card.casefold() in canonical and
        isinstance(seconds, (int, float)) and not isinstance(seconds, bool) and
        math.isfinite(seconds) and seconds > 0
    }
    if (not 0 < utilization <= 1 or max_slots <= 0 or not configured_cards or
            not duration):
        invalid_infeasible: dict[int, float] = {}
        for item in demand:
            if item.count > 0:
                invalid_infeasible[item.priority] = (
                    invalid_infeasible.get(item.priority, 0.0) + item.count)
        return DeadlineCapacityPlan(
            target_by_card={},
            infeasible_requests_by_priority=invalid_infeasible)

    def _valid_supply(item: DeadlineSupply) -> bool:
        available = item.available_after_seconds
        return (item.card.casefold() in canonical and
                canonical[item.card.casefold()] in duration and
                isinstance(available,
                           (int, float)) and not isinstance(available, bool) and
                math.isfinite(available) and available >= 0)

    pending_supply = [item for item in finite_supply if _valid_supply(item)]
    pending_supply.sort(key=lambda item: (
        item.tier, item.available_after_seconds,
        cold_rank.get(canonical[item.card.casefold()], len(cold_rank))))
    active: list[_ActiveDeadlineSlot] = []
    target = {card: 0 for card in configured_cards}
    infeasible: dict[int, float] = {}

    grouped: dict[tuple[int, float, tuple[str, ...]], int] = {}
    for item in demand:
        if (not isinstance(item.count, int) or isinstance(item.count, bool) or
                item.count <= 0 or not isinstance(item.remaining_seconds,
                                                  (int, float)) or
                isinstance(item.remaining_seconds, bool) or
                not math.isfinite(item.remaining_seconds)):
            continue
        requested = {
            canonical[card.casefold()]
            for card in item.compatible_cards
            if card.casefold() in canonical
        }
        compatible = tuple(card for card in card_order
                           if card in requested and card in duration)
        if not compatible:
            infeasible[item.priority] = (infeasible.get(item.priority, 0.0) +
                                         item.count)
            continue
        key = (int(item.priority), max(0.0, float(item.remaining_seconds)),
               compatible)
        grouped[key] = grouped.get(key, 0) + item.count

    def _slot_capacity(slot: _ActiveDeadlineSlot, deadline: float) -> float:
        return max(0.0, deadline - slot.available_after_seconds) * (
            utilization / duration[slot.card])

    def _consume(slot: _ActiveDeadlineSlot, deadline: float, remaining: float,
                 *, priority: int) -> float:
        capacity = _slot_capacity(slot, deadline)
        used = min(remaining, capacity)
        if used > epsilon:
            slot.priority = (priority if slot.priority is None else max(
                slot.priority, priority))
        slot.available_after_seconds += used * duration[slot.card] / utilization
        return remaining - used

    def _finite_fallback_key(compatible: tuple[str, ...],
                             deadline: float) -> tuple[int, int]:
        options = sorted(
            (item.tier,
             cold_rank.get(canonical[item.card.casefold()], len(cold_rank)))
            for item in pending_supply
            if canonical[item.card.casefold()] in compatible and
            item.available_after_seconds < deadline)
        options.extend((1 << 20, cold_rank[card]) for card in compatible)
        if len(options) > 1:
            return options[1]
        if options:
            return options[0]
        return 1 << 21, len(cold_rank)

    def _profile_scarcity_key(compatible: tuple[str, ...],
                              deadline: float) -> tuple[int, tuple[int, int]]:
        return -len(compatible), _finite_fallback_key(compatible, deadline)

    priorities = sorted({key[0] for key in grouped}, reverse=True)
    for priority in priorities:
        deadlines = sorted({key[1] for key in grouped if key[0] == priority})
        for deadline in deadlines:
            pending = [
                (compatible, float(count))
                for (item_priority, item_deadline,
                     compatible), count in grouped.items()
                if item_priority == priority and item_deadline == deadline
            ]
            while pending:
                # Fewer compatible card types is the primary scarcity proof;
                # an exact A100 bucket must own A100 before an A100-or-L4
                # bucket even when several A100 slots make their immediate
                # supply keys tie.  The worse second-best supply option then
                # breaks equal-width profiles.  Exact ties retain report
                # order.
                selected_index = max(range(len(pending)),
                                     key=lambda index: _profile_scarcity_key(
                                         pending[index][0], deadline))
                compatible, remaining = pending.pop(selected_index)

                # Reuse activated capacity before materializing another slot.
                # Larger remaining budgets go first so a partially used slot
                # does not force an otherwise avoidable new target unit.
                compatible_active = [
                    slot for slot in active if slot.card in compatible
                ]
                for slot in sorted(
                        compatible_active,
                        key=lambda slot: _slot_capacity(slot, deadline),
                        reverse=True):
                    remaining = _consume(slot,
                                         deadline,
                                         remaining,
                                         priority=priority)
                    if remaining <= epsilon:
                        break

                while remaining > epsilon and sum(target.values()) < max_slots:
                    selected_supply_index = next(
                        (index
                         for index, supply_item in enumerate(pending_supply)
                         if canonical[supply_item.card.casefold()] in compatible
                         and supply_item.available_after_seconds < deadline),
                        None)
                    if selected_supply_index is not None:
                        supply_item = pending_supply.pop(selected_supply_index)
                        card = canonical[supply_item.card.casefold()]
                        slot = _ActiveDeadlineSlot(
                            card, float(supply_item.available_after_seconds))
                    else:
                        card = next((card for card in prospective_cold_order
                                     if card in compatible), None)
                        if (card is None or
                                paid_cold_lead_seconds >= deadline or
                                not math.isfinite(paid_cold_lead_seconds)):
                            break
                        slot = _ActiveDeadlineSlot(
                            card, max(0.0, paid_cold_lead_seconds))
                    if _slot_capacity(slot, deadline) <= epsilon:
                        continue
                    active.append(slot)
                    target[slot.card] += 1
                    remaining = _consume(slot,
                                         deadline,
                                         remaining,
                                         priority=priority)

                if remaining > epsilon:
                    infeasible[priority] = (infeasible.get(priority, 0.0) +
                                            remaining)
                    # A deadline that cold capacity can no longer meet is
                    # still real queued work.  Recover it through the same
                    # bounded target rather than making mathematical
                    # infeasibility suppress every launch.  One slot per
                    # residual request matches the existing raw concurrency
                    # ceiling; max_slots and the caller's paid/provider fences
                    # remain authoritative.  Debit the assigned request so a
                    # later bucket cannot reuse its GPU-time.
                    while (remaining > epsilon and
                           sum(target.values()) < max_slots):
                        selected_supply_index = next(
                            (index
                             for index, supply_item in enumerate(pending_supply)
                             if canonical[supply_item.card.casefold()] in
                             compatible), None)
                        if selected_supply_index is not None:
                            supply_item = pending_supply.pop(
                                selected_supply_index)
                            card = canonical[supply_item.card.casefold()]
                            slot = _ActiveDeadlineSlot(
                                card,
                                float(supply_item.available_after_seconds))
                        else:
                            card = next((card for card in prospective_cold_order
                                         if card in compatible), None)
                            if card is None:
                                break
                            slot = _ActiveDeadlineSlot(
                                card, (max(0.0, paid_cold_lead_seconds)
                                       if math.isfinite(paid_cold_lead_seconds)
                                       else float('inf')))
                        active.append(slot)
                        target[slot.card] += 1
                        assigned = min(1.0, remaining)
                        slot.priority = priority
                        slot.available_after_seconds += (assigned *
                                                         duration[slot.card] /
                                                         utilization)
                        remaining -= assigned

    acquisition: dict[tuple[int, tuple[str, ...]], int] = {}
    for slot in active:
        if slot.priority is None:
            continue
        # The deadline planner selected this exact card from a capacity-time
        # curve.  It has not proved that relocating the slot preserves the
        # service-time/SLA schedule, so reservation acquisition must pin it.
        acquisition_key = (slot.priority, (slot.card.casefold(),))
        acquisition[acquisition_key] = acquisition.get(acquisition_key, 0) + 1
    classes = tuple(
        compatibility_matching.CompatibilityDemand(
            priority=priority, compatible_cards=compatible, count=count)
        for (priority, compatible), count in sorted(
            acquisition.items(), key=lambda item: (-item[0][0], item[0][1])))
    return DeadlineCapacityPlan(target_by_card={
        card: count for card, count in target.items() if count > 0
    },
                                infeasible_requests_by_priority={
                                    priority: count
                                    for priority, count in infeasible.items()
                                    if count > epsilon
                                },
                                reservation_acquisition_classes=classes)


def _plan_compatibility_target(
    *,
    configured_cards: list[str],
    capacities: dict[str, float],
    floors: dict[str, int],
    min_replicas: int,
    max_replicas: int,
    demand_profiles: list[tuple[int, tuple[str, ...], float]],
    fixed_work_by_accelerator: dict[str, float],
    ready_zero_cost: dict[str, int],
    committed_zero_cost: dict[str, int],
    free_reserved: dict[str, int],
    ready_paid: dict[str, int],
    committed_paid: dict[str, int],
    supply_preference: SupplyPreference,
    cold_order: list[str],
    use_existing_supply: bool,
) -> CompatibilityTargetPlan:
    """Allocate exact-card work and reduce its demand-owned capacity classes.

    `fixed_work_by_accelerator` is already-running or conservatively unknown
    work. It cannot move without preemption, so it consumes capacity only on
    its current card before flexible queued/rejected profiles are considered.
    `demand_profiles` contains work units, not request counts, which lets the
    same scarcity/supply allocator serve both QPS and concurrency policies.

    The supply maps are disjoint economic facts except for the explicit subset
    relationships ``ready_zero_cost <= committed_zero_cost`` and
    ``ready_paid <= committed_paid``. The promoted durable planner uses
    ``ZERO_COST_FIRST`` so flexible work consumes every compatible zero-cost
    tier before paid supply. The retained generic adapter uses ``WARM_FIRST``
    until that path is promoted; its reservation launches remain an
    independent padding projection. Fixed work above never moves between cards
    under either policy.
    """
    demand_epsilon = 1e-9
    canonical_by_name = {card.casefold(): card for card in configured_cards}
    capacity_by_card = {
        card: max(0.0, float(capacities.get(card, 0.0)))
        for card in configured_cards
    }
    ownership_supported = True
    # Fixed work has lost its originating request priority. It must not inherit
    # the maximum priority of an unrelated queued/rejected class: that would
    # let existing work consume another request's strict-priority reservation
    # entitlement. Copacked typed work may still raise the resulting slot's
    # priority through consume_owned_slot().
    fixed_priority = constants.LB_REQUEST_PRIORITY_MIN
    owned_slots: list[_DemandOwnedSlot] = []

    def add_slot(card: str) -> _DemandOwnedSlot:
        slot = _DemandOwnedSlot(ordinal=len(owned_slots),
                                card=card,
                                unused_work=max(0.0, capacities.get(card, 0.0)))
        owned_slots.append(slot)
        return slot

    def consume_owned_slot(slot: _DemandOwnedSlot, *, priority: int,
                           compatible: frozenset[str], work: float) -> float:
        consumed = min(max(0.0, work), max(0.0, slot.unused_work))
        if consumed <= demand_epsilon:
            return 0.0
        intersection = (compatible if slot.compatible_cards is None else
                        slot.compatible_cards.intersection(compatible))
        occupied = capacities.get(slot.card, 0.0) - slot.unused_work + consumed
        intersection = frozenset(card for card in intersection
                                 if capacity_by_card.get(card, 0.0) +
                                 demand_epsilon >= occupied)
        if not intersection:
            return 0.0
        slot.priority = (priority if slot.priority is None else max(
            slot.priority, priority))
        slot.compatible_cards = intersection
        slot.unused_work = max(0.0, slot.unused_work - consumed)
        return consumed

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
        for _ in range(floor):
            add_slot(card)
        remaining_floor_budget -= floor
    unused_capacity = {
        card: target[card] * max(0.0, capacities.get(card, 0.0))
        for card in configured_cards
    }

    # Running work is non-preemptive. Pin its target to the card already
    # serving it before assigning any flexible backlog.
    for card in configured_cards:
        remaining = max(0.0, fixed_work_by_accelerator.get(card, 0.0))
        capacity = capacities.get(card, 0.0)
        if capacity <= 0:
            continue
        fixed_compatible = frozenset((card,))
        for slot in owned_slots:
            if slot.card != card:
                continue
            consumed = consume_owned_slot(slot,
                                          priority=fixed_priority,
                                          compatible=fixed_compatible,
                                          work=remaining)
            remaining -= consumed
            unused_capacity[card] = max(
                0.0,
                unused_capacity.get(card, 0.0) - consumed)
            if remaining <= demand_epsilon:
                break
        while (remaining > demand_epsilon and
               sum(target.values()) < max_replicas):
            target[card] = target.get(card, 0) + 1
            slot = add_slot(card)
            consumed = consume_owned_slot(slot,
                                          priority=fixed_priority,
                                          compatible=fixed_compatible,
                                          work=remaining)
            remaining -= consumed
            # `unused_capacity` starts with floor capacity only; a newly
            # added slot contributes exactly its unused tail.
            unused_capacity[card] = (unused_capacity.get(card, 0.0) +
                                     slot.unused_work)

    cold_rank = {card: index for index, card in enumerate(cold_order)}
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
        if not isinstance(supply_preference, SupplyPreference):
            raise ValueError('Supply preference is malformed.')
        for card in configured_cards:
            if (ready_zero_cost.get(card, 0) > committed_zero_cost.get(card, 0)
                    or ready_paid.get(card, 0) > committed_paid.get(card, 0)):
                raise ValueError('Ready capacity exceeds committed capacity.')
        if supply_preference is SupplyPreference.ZERO_COST_FIRST:
            zero_with_reservation = {
                card: (committed_zero_cost.get(card, 0) +
                       free_reserved.get(card, 0)) for card in configured_cards
            }
            planned_by_tier = [
                dict(ready_zero_cost),
                dict(committed_zero_cost),
                zero_with_reservation,
                {
                    card: (zero_with_reservation.get(card, 0) +
                           ready_paid.get(card, 0)) for card in configured_cards
                },
                {
                    card: (zero_with_reservation.get(card, 0) +
                           committed_paid.get(card, 0)
                          ) for card in configured_cards
                },
            ]
        else:
            ready_all = {
                card: (ready_zero_cost.get(card, 0) + ready_paid.get(card, 0))
                for card in configured_cards
            }
            committed_all = {
                card: (committed_zero_cost.get(card, 0) +
                       committed_paid.get(card, 0)) for card in configured_cards
            }
            planned_by_tier = [
                dict(ready_zero_cost), ready_all, committed_all, {
                    card: (committed_all.get(card, 0) +
                           free_reserved.get(card, 0)
                          ) for card in configured_cards
                }
            ]

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

    def project_ownership(*, priority: int, compatible: tuple[str, ...],
                          accepted_work: float,
                          new_slots: list[_DemandOwnedSlot],
                          slack_by_card: dict[str, float]) -> bool:
        """Bind accepted fractional work to the exact bins it consumed."""
        if not ownership_supported:
            return False
        remaining = accepted_work
        compatible_set = frozenset(compatible)
        new_ordinals = {slot.ordinal for slot in new_slots}
        # Mirror the allocator: consume earlier slack before newly added bins.
        # This keeps per-bin ownership aligned with the aggregate slack map.
        for card in compatible:
            card_remaining = min(remaining, slack_by_card.get(card, 0.0))
            while card_remaining > demand_epsilon:
                candidates = []
                for slot in owned_slots:
                    if (slot.ordinal in new_ordinals or slot.card != card or
                            slot.unused_work <= demand_epsilon):
                        continue
                    intersection = (
                        compatible_set if slot.compatible_cards is None else
                        slot.compatible_cards.intersection(compatible_set))
                    if intersection:
                        candidates.append(
                            (-len(intersection), slot.ordinal, slot))
                if not candidates:
                    return False
                slot = min(candidates, key=lambda item: item[:-1])[-1]
                consumed = consume_owned_slot(slot,
                                              priority=priority,
                                              compatible=compatible_set,
                                              work=card_remaining)
                if consumed <= demand_epsilon:
                    return False
                card_remaining -= consumed
                remaining -= consumed
            if remaining <= demand_epsilon:
                return True
        for slot in new_slots:
            remaining -= consume_owned_slot(slot,
                                            priority=priority,
                                            compatible=compatible_set,
                                            work=remaining)
            if remaining <= demand_epsilon:
                return True
        return remaining <= demand_epsilon

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
            original_work = remaining
            slack_by_card: dict[str, float] = {}
            for card in compatible:
                consumed = min(remaining, unused_capacity.get(card, 0.0))
                remaining -= consumed
                if consumed > demand_epsilon:
                    slack_by_card[card] = consumed
                unused_capacity[card] = max(
                    0.0,
                    unused_capacity.get(card, 0.0) - consumed)
                if remaining <= demand_epsilon:
                    break
            new_slots: list[_DemandOwnedSlot] = []
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
                new_slots.append(add_slot(selected))
                if capacity > remaining:
                    unused_capacity[selected] = (
                        unused_capacity.get(selected, 0.0) + capacity -
                        remaining)
                    remaining = 0.0
                else:
                    remaining -= capacity
            if not project_ownership(priority=priority,
                                     compatible=compatible,
                                     accepted_work=(original_work - remaining),
                                     new_slots=new_slots,
                                     slack_by_card=slack_by_card):
                ownership_supported = False

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
        add_slot(selected)

    target_entries = tuple(
        (card, count) for card, count in target.items() if count > 0)
    target_total = sum(count for _, count in target_entries)
    classes: tuple[compatibility_matching.CompatibilityDemand, ...] | None
    if target_total == 0:
        classes = ()
    elif (not ownership_supported or len(owned_slots) != target_total or
          any(slot.priority is None or slot.compatible_cards is None
              for slot in owned_slots)):
        classes = None
    else:
        grouped_classes: dict[tuple[int, tuple[str, ...]], int] = {}
        for slot in owned_slots:
            assert slot.priority is not None
            assert slot.compatible_cards is not None
            key = (slot.priority,
                   tuple(
                       sorted(
                           card.casefold() for card in slot.compatible_cards)))
            grouped_classes[key] = grouped_classes.get(key, 0) + 1
        classes = tuple(
            compatibility_matching.CompatibilityDemand(
                priority=priority, compatible_cards=compatible, count=count)
            for (priority, compatible
                ), count in sorted(grouped_classes.items(),
                                   key=lambda item: (-item[0][0], item[0][1])))
    return CompatibilityTargetPlan(target_entries=target_entries,
                                   reservation_acquisition_classes=classes)


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
    committed_zero_cost: dict[str, int],
    free_reserved: dict[str, int],
    ready_paid: dict[str, int],
    committed_paid: dict[str, int],
    supply_preference: SupplyPreference,
    cold_order: list[str],
    use_existing_supply: bool,
) -> dict[str, int]:
    """Return the historical map view of the canonical typed allocation."""
    return _plan_compatibility_target(
        configured_cards=configured_cards,
        capacities=capacities,
        floors=floors,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        demand_profiles=demand_profiles,
        fixed_work_by_accelerator=fixed_work_by_accelerator,
        ready_zero_cost=ready_zero_cost,
        committed_zero_cost=committed_zero_cost,
        free_reserved=free_reserved,
        ready_paid=ready_paid,
        committed_paid=committed_paid,
        supply_preference=supply_preference,
        cold_order=cold_order,
        use_existing_supply=use_existing_supply).as_dict()


def _replica_is_retiring_card_supply(
        replica_info: 'replica_managers.ReplicaInfo') -> bool:
    """Whether a row must not authorize replacement on its current card."""
    status = replica_info.status_property
    return status.is_scale_down is True or status.preempted is True


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


def _bound_materialized_reassignment_target(
    *,
    adopted_target: dict[str, int],
    desired_target: dict[str, int],
    reassignment_source_by_accelerator: dict[str, int],
    reassignment_destination_by_accelerator: dict[str, int],
    configured_cards: list[str],
    final_target: int,
) -> dict[str, int]:
    """Build a full target whose only deficits come from proven sources.

    An aggregate downscale hold can combine old exact-card slots with a fresh
    compatibility-owned subset.  A backed reassignment of that fresh subset
    must not consume an unrelated held slot merely because it appears first
    in configured-card order.  This helper starts from the complete adopted
    map, adds ordinary overprovision padding, then creates destination
    deficits by removing only the explicitly identified fresh source units.
    The actuation revalidator still decides whether materialized destination
    supply is sufficient; this helper grants no cold-launch authority.
    """
    maps = (adopted_target, desired_target, reassignment_source_by_accelerator,
            reassignment_destination_by_accelerator)
    configured = set(configured_cards)
    if (isinstance(final_target, bool) or not isinstance(final_target, int) or
            final_target < 0 or
            any(set(values) - configured for values in maps) or any(
                isinstance(count, bool) or not isinstance(count, int) or
                count < 0 for values in maps for count in values.values()) or
            sum(desired_target.values()) != final_target or
            sum(adopted_target.values()) > final_target or
            any(count > adopted_target.get(card, 0)
                for card, count in reassignment_source_by_accelerator.items())
            or any(count > desired_target.get(card, 0) for card, count in
                   reassignment_destination_by_accelerator.items())):
        return {}

    target = {
        card: int(adopted_target.get(card, 0)) for card in configured_cards
    }
    remaining = final_target - sum(target.values())
    for card in configured_cards:
        if remaining <= 0:
            break
        added = min(remaining, max(0,
                                   desired_target.get(card, 0) - target[card]))
        target[card] += added
        remaining -= added
    if remaining != 0:
        return {}

    destination_deficit = {
        card: max(
            0,
            reassignment_destination_by_accelerator.get(card, 0) -
            target[card]) for card in configured_cards
    }
    movable = sum(destination_deficit.values())
    removed = 0
    for card in configured_cards:
        if removed >= movable:
            break
        source_available = min(
            reassignment_source_by_accelerator.get(card, 0),
            max(
                0, target[card] -
                reassignment_destination_by_accelerator.get(card, 0)))
        count = min(source_available, movable - removed)
        target[card] -= count
        removed += count

    to_place = removed
    for card in configured_cards:
        if to_place <= 0:
            break
        count = min(to_place, destination_deficit[card])
        target[card] += count
        to_place -= count
    if to_place != 0 or sum(target.values()) != final_target:
        return {}
    return {card: count for card, count in target.items() if count > 0}


def _revalidate_actuation_target(
    *,
    adopted_target: dict[str, int],
    desired_target: dict[str, int],
    nonretiring_supply: dict[str, int],
    configured_cards: list[str],
    final_target: int,
    allow_adopted_reassignment: bool = True,
    allow_unbacked_adopted_reassignment: bool = True,
    allow_mixed_version_backed_reassignment: bool = False,
    old_version_supply: dict[str, int] | None = None,
    reassignment_target_by_accelerator: dict[str, int] | None = None,
) -> dict[str, int]:
    """Build a supply-aware actuator without bypassing target adoption.

    The adopted map owns compatibility changes, hysteresis, and logical-card
    wave limits. Actuation may immediately move an adopted unit when its card
    is no longer backed, or when the fresh placement can move that unit onto
    compatible supply that already exists. It must not cold-launch additional
    units for a not-yet-adopted compatibility migration. During an aggregate
    downscale hold, unbacked adopted units remain on their exact cards, while
    materialized compatible supply may still replace them.

    ``old_version_supply`` is per-card nonterminal, non-retiring supply on
    versions OTHER than the latest. An explicit map is a complete snapshot;
    missing keys, including every key in an empty map, mean known zero supply.
    ``None`` means that old-version provenance is unavailable, so a
    mixed-version rollout fails closed by preserving the adopted card map. It
    exists to split the two meanings that "unbacked" used to conflate during a
    rolling update, when ``nonretiring_supply`` (latest-version only) says
    nothing about whether a card is still serving:

    - A card with no latest-version supply but live old-version supply is
      mid-replacement. Releasing it retires replicas that are still serving
      demand which may accept only that card, so it is preserved.
    - A card with supply on NEITHER generation is genuinely gone (preempted
      reserved capacity is the measured case). Preserving it turned the loss
      into paid same-card launch authority at roughly 6.8x the price the
      same card-agnostic requests accept, so it is released and re-priced
      even while a rollout is in flight.

    Outside a rollout the original latest-version unbacked-release path still
    applies, so callers that omit the provenance parameter retain their legacy
    behaviour.

    ``reassignment_target_by_accelerator`` bounds every card movement to an
    explicitly owned subset of ``desired_target``. It lets the logical caller
    distinguish compatibility-proven work from aggregate/default padding. The
    omitted legacy shape treats the complete desired target as owned.

    ``allow_mixed_version_backed_reassignment`` lets a rollout replace backed
    adopted units toward that owned subset instead of first purchasing the old
    physical card. The default remains fail-closed for legacy, incomplete,
    stale, and helper callers.
    """
    if sum(desired_target.values()) != final_target:
        return {}
    if reassignment_target_by_accelerator is None:
        reassignment_target = {
            card: max(0, int(desired_target.get(card, 0)))
            for card in configured_cards
        }
    else:
        if (set(reassignment_target_by_accelerator) - set(configured_cards) or
                any(
                    isinstance(count, bool) or not isinstance(count, int) or
                    count < 0 or count > desired_target.get(card, 0) for card,
                    count in reassignment_target_by_accelerator.items())):
            return {}
        reassignment_target = {
            card: int(reassignment_target_by_accelerator.get(card, 0))
            for card in configured_cards
        }
    target = {
        card: max(0, int(adopted_target.get(card, 0)))
        for card in configured_cards
    }

    def fill_toward(
        placement: dict[str, int],
        count: int,
        *,
        require_backing: bool,
    ) -> int:
        for card in configured_cards:
            if count <= 0:
                break
            deficit = max(0, placement.get(card, 0) - target[card])
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
    if remaining < 0 or fill_toward(
            desired_target, remaining, require_backing=False) != 0:
        return {}
    # Release adopted capacity that no generation still backs, BEFORE the
    # mixed-version guard: capacity that exists on neither the latest nor any
    # old version is genuinely gone, and preserving its card turns a
    # preemption into paid same-card launch authority. A card that old-version
    # rows still serve is only mid-replacement and is preserved here. Require
    # an explicit complete snapshot for this pre-guard release: None means the
    # old-version provenance is unknown, so a mixed-version rollout must fail
    # closed. The rollout-scoped release below (gated on adopted reassignment)
    # retains the original single-version behaviour.
    if (allow_unbacked_adopted_reassignment and old_version_supply is not None):
        vanished = 0
        needed = sum(
            max(0,
                reassignment_target.get(card, 0) - target[card])
            for card in configured_cards)
        for card in configured_cards:
            if vanished >= needed:
                break
            backing = (max(0, int(nonretiring_supply.get(card, 0))) +
                       max(0, int(old_version_supply.get(card, 0))))
            unbacked = max(0, target[card] - backing)
            removable = min(
                unbacked, max(0,
                              target[card] - reassignment_target.get(card, 0)),
                needed - vanished)
            target[card] -= removable
            vanished += removable
        if fill_toward(reassignment_target, vanished,
                       require_backing=False) != 0:
            return {}

        # A vanished, unowned placeholder may also be replaced by already
        # materialized latest-version supply from the full desired placement.
        # This recognizes capacity that exists; it never creates a cold
        # shortage or moves a unit still backed on any generation.
        reusable = 0
        reusable_removed_by_card: dict[str, int] = {}
        needed = sum(
            max(0,
                desired_target.get(card, 0) - target[card])
            for card in configured_cards)
        for card in configured_cards:
            if reusable >= needed:
                break
            backing = (max(0, int(nonretiring_supply.get(card, 0))) +
                       max(0, int(old_version_supply.get(card, 0))))
            unbacked = max(0, target[card] - backing)
            removable = min(unbacked,
                            max(0, target[card] - desired_target.get(card, 0)),
                            needed - reusable)
            if removable > 0:
                reusable_removed_by_card[card] = removable
                target[card] -= removable
                reusable += removable
        unplaced = fill_toward(desired_target, reusable, require_backing=True)
        if unplaced > 0:
            for card, removed in reusable_removed_by_card.items():
                restored = min(unplaced, removed)
                target[card] += restored
                unplaced -= restored
                if unplaced == 0:
                    break
        if unplaced != 0:
            return {}

    if (not allow_adopted_reassignment and
            not allow_mixed_version_backed_reassignment):
        # A mixed-version rollout preserves every still-backed adopted unit
        # unless the caller explicitly enables movement toward the bounded
        # reassignment subset. Generic padding was assigned above without
        # changing adopted demand; vanished units moved only when that subset
        # contained a proven destination.
        return {card: count for card, count in target.items() if count > 0}

    # First replace adopted capacity that is absent from the latest version.
    # In the ordinary path that means disappearing capacity.  With the
    # explicit mixed-version proof above it also means old-version backing
    # whose fresh compatible replacement belongs on another card.  This is
    # allowed to create a cold shortage: the rollout and victim selectors
    # still require materialized compatible coverage and preserve busy work.
    if allow_unbacked_adopted_reassignment:
        reassigned = 0
        needed = sum(
            max(0,
                reassignment_target.get(card, 0) - target[card])
            for card in configured_cards)
        for card in configured_cards:
            if reassigned >= needed:
                break
            unbacked = max(
                0, target[card] - max(0, int(nonretiring_supply.get(card, 0))))
            removable = min(
                unbacked, max(0,
                              target[card] - reassignment_target.get(card, 0)),
                needed - reassigned)
            target[card] -= removable
            reassigned += removable
        if fill_toward(reassignment_target, reassigned,
                       require_backing=False) != 0:
            return {}

    # Then let already-existing compatible supply replace backed capacity.
    # This lets reserved A100s serve flexible L4-attributed demand and retire
    # redundant paid L4s, without permitting a new A100 cold launch.
    movable = 0
    needed = sum(
        max(0,
            reassignment_target.get(card, 0) - target[card])
        for card in configured_cards)
    removed_by_card: dict[str, int] = {}
    for card in configured_cards:
        if movable >= needed:
            break
        removable = min(max(0, target[card] - reassignment_target.get(card, 0)),
                        needed - movable)
        if removable > 0:
            removed_by_card[card] = removable
            target[card] -= removable
            movable += removable
    unplaced = fill_toward(reassignment_target, movable, require_backing=True)
    if unplaced > 0:
        # Restore the exact sources for units with no materialized destination.
        for card, removed in removed_by_card.items():
            restored = min(unplaced, removed)
            target[card] += restored
            unplaced -= restored
            if unplaced == 0:
                break
    if unplaced != 0:
        return {}
    return {card: count for card, count in target.items() if count > 0}
