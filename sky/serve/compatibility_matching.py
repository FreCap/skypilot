"""Pure counted matching of compatible demand to exact-card supply."""

from collections.abc import Iterable
import dataclasses

from sky.serve import constants


@dataclasses.dataclass(frozen=True, kw_only=True)
class CompatibilityDemand:
    """One integer demand class at one strict priority."""

    priority: int
    compatible_cards: tuple[str, ...]
    count: int

    def __post_init__(self) -> None:
        if type(self.priority) is not int or type(self.count) is not int:
            raise ValueError('Compatibility demand counts must be integers.')
        if self.count <= 0:
            raise ValueError('Compatibility demand count must be positive.')
        if not isinstance(self.compatible_cards, tuple):
            raise ValueError('Compatible cards must be a tuple.')
        if not self.compatible_cards:
            raise ValueError('Compatibility demand must name a card.')
        if any(not isinstance(card, str) or not card or card.strip() != card
               for card in self.compatible_cards):
            raise ValueError('Compatibility demand has an invalid card.')
        normalized_cards = tuple(
            card.casefold() for card in self.compatible_cards)
        if len(set(normalized_cards)) != len(normalized_cards):
            raise ValueError('Compatibility demand has duplicate cards.')
        object.__setattr__(self, 'compatible_cards',
                           tuple(sorted(normalized_cards)))


@dataclasses.dataclass(frozen=True, kw_only=True)
class CompatibilitySupply:
    """One exact-card supply atom with a binary economic preference.

    ``preferred_capacity`` is the prefix used before any compatible
    nonpreferred unit.  A broker uses it for authenticated holdings; a planner
    uses full preferred capacity for zero-cost atoms and zero for paid atoms.
    """

    supply_id: str
    card: str
    capacity: int
    preferred_capacity: int
    stable_rank: int

    def __post_init__(self) -> None:
        if (not isinstance(self.supply_id, str) or not self.supply_id or
                self.supply_id.strip() != self.supply_id):
            raise ValueError('Compatibility supply has an invalid identity.')
        if (not isinstance(self.card, str) or not self.card or
                self.card.strip() != self.card):
            raise ValueError('Compatibility supply has an invalid card.')
        if (type(self.capacity) is not int or self.capacity <= 0 or
                type(self.preferred_capacity) is not int or
                self.preferred_capacity < 0 or
                self.preferred_capacity > self.capacity or
                type(self.stable_rank) is not int or self.stable_rank < 0):
            raise ValueError('Compatibility supply capacity is malformed.')
        object.__setattr__(self, 'card', self.card.casefold())


@dataclasses.dataclass(frozen=True, kw_only=True)
class CompatibilityMatch:
    """Deeply immutable counted assignment and unmatched-demand projection."""

    assigned_by_supply: tuple[tuple[str, int], ...]
    assigned_by_card: tuple[tuple[str, int], ...]
    unmatched_by_priority: tuple[tuple[int, int], ...]


@dataclasses.dataclass(frozen=True)
class _SupplyBin:
    supply_id: str
    card: str
    capacity: int
    preferred: bool
    preference_rank: int


def _capacity_by_subset(capacity_by_card: tuple[int, ...]) -> list[int]:
    """Return summed capacity for every subset of the card universe."""
    subset_capacity = [0] * (1 << len(capacity_by_card))
    for subset in range(1, len(subset_capacity)):
        lowest_bit = subset & -subset
        card_index = lowest_bit.bit_length() - 1
        subset_capacity[subset] = (subset_capacity[subset ^ lowest_bit] +
                                   capacity_by_card[card_index])
    return subset_capacity


def _add_to_supersets(values: list[int], subset: int, count: int) -> None:
    """Add ``count`` to every entry whose mask contains ``subset``."""
    full_set = len(values) - 1
    available_bits = full_set ^ subset
    extension = available_bits
    while True:
        values[subset | extension] += count
        if extension == 0:
            return
        extension = (extension - 1) & available_bits


def match_compatible_capacity(
    *,
    demand: Iterable[CompatibilityDemand],
    supply: Iterable[CompatibilitySupply],
    maximum_assigned: int | None = None,
) -> CompatibilityMatch:
    """Return the deterministic optimal matching for counted capacity.

    A larger integer is a higher priority.  Matched cardinality is maximized
    lexicographically by descending priority.  Among matchings with the same
    priority counts, preferred capacity is used before nonpreferred capacity
    and then lower stable ranks are preferred.  ``maximum_assigned`` applies
    one global cap without creating a second allocation path.  Inputs are never
    mutated.
    """
    if (maximum_assigned is not None and
        (type(maximum_assigned) is not int or maximum_assigned < 0)):
        raise ValueError('Maximum assigned capacity must be nonnegative.')
    demands = tuple(demand)
    supplies = tuple(supply)
    if any(not isinstance(item, CompatibilityDemand) for item in demands):
        raise ValueError('Demand contains an invalid compatibility class.')
    if any(not isinstance(item, CompatibilitySupply) for item in supplies):
        raise ValueError('Supply contains an invalid physical pool.')
    supply_ids = [item.supply_id for item in supplies]
    if len(set(supply_ids)) != len(supply_ids):
        raise ValueError('Compatibility supply identities must be unique.')

    grouped: dict[tuple[int, tuple[str, ...]], int] = {}
    for item in demands:
        key = item.priority, item.compatible_cards
        grouped[key] = grouped.get(key, 0) + item.count
    demand_groups = tuple(
        CompatibilityDemand(
            priority=priority, compatible_cards=cards, count=count)
        for (priority, cards), count in sorted(
            grouped.items(), key=lambda item: (-item[0][0], item[0][1])))
    stable_supply = tuple(
        sorted(supplies,
               key=lambda item: (item.stable_rank, item.card, item.supply_id)))
    total_demand = sum(item.count for item in demand_groups)
    demand_cards = {
        card for item in demand_groups for card in item.compatible_cards
    }
    supply_cards = {item.card for item in stable_supply}
    relevant_cards = demand_cards & supply_cards
    if len(relevant_cards) > constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS:
        raise ValueError(
            'Compatibility matching has more than '
            f'{constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS} accelerator '
            'cards.')

    supply_preference = {
        item.supply_id: rank for rank, item in enumerate(stable_supply)
    }
    bins: list[_SupplyBin] = []
    for item in stable_supply:
        rank = supply_preference[item.supply_id]
        if item.preferred_capacity:
            bins.append(
                _SupplyBin(item.supply_id, item.card, item.preferred_capacity,
                           True, rank))
        nonpreferred_capacity = item.capacity - item.preferred_capacity
        if nonpreferred_capacity:
            bins.append(
                _SupplyBin(item.supply_id, item.card, nonpreferred_capacity,
                           False, rank))
    bins.sort(key=lambda capacity_bin:
              (not capacity_bin.preferred, capacity_bin.preference_rank,
               capacity_bin.card, capacity_bin.supply_id))

    # Accelerator-card cardinality is deliberately small even when demand
    # classes and physical pools number in the thousands.  Use that bounded
    # dimension directly: the maximum matching rank is the minimum cut over
    # card subsets.  This avoids expanding counted capacity or materializing a
    # demand-by-pool graph.
    priorities = sorted({item.priority for item in demand_groups}, reverse=True)
    cards = tuple(sorted(relevant_cards))
    card_index = {card: index for index, card in enumerate(cards)}
    demand_masks = tuple(
        sum(1 << card_index[card]
            for card in item.compatible_cards
            if card in card_index)
        for item in demand_groups)
    physical_capacity_by_card = [0] * len(cards)
    for supply_item in stable_supply:
        if supply_item.card in card_index:
            physical_capacity_by_card[card_index[
                supply_item.card]] += supply_item.capacity
    physical_capacity_by_subset = _capacity_by_subset(
        tuple(physical_capacity_by_card))
    subset_count = len(physical_capacity_by_subset)
    assignment_limit = (total_demand if maximum_assigned is None else min(
        total_demand, maximum_assigned))
    contained_demand_by_subset = [0] * subset_count
    matched_by_priority: dict[int, int] = {}
    demand_by_priority: dict[int, int] = {}
    cumulative_demand = 0
    cumulative_rank = 0
    demand_index = 0
    while demand_index < len(demand_groups):
        priority = demand_groups[demand_index].priority
        priority_demand = 0
        while (demand_index < len(demand_groups) and
               demand_groups[demand_index].priority == priority):
            demand_group = demand_groups[demand_index]
            priority_demand += demand_group.count
            cumulative_demand += demand_group.count
            _add_to_supersets(contained_demand_by_subset,
                              demand_masks[demand_index], demand_group.count)
            demand_index += 1
        next_rank = min(
            assignment_limit,
            min(physical_capacity_by_subset[subset] + cumulative_demand -
                contained_demand_by_subset[subset]
                for subset in range(subset_count)))
        assert next_rank >= cumulative_rank
        matched_by_priority[priority] = next_rank - cumulative_rank
        demand_by_priority[priority] = priority_demand
        cumulative_rank = next_rank

    # A basis of each demand-priority prefix extends to a basis of the next
    # prefix, so the rank increments above are the exact lexicographic match
    # counts.  Fix those counts and view physical supply units as the ground
    # set of the reverse matching gammoid.  Standard matroid greedy then gives
    # the minimum-cost supply basis: every preferred bin first, followed by
    # nonpreferred bins, with stable supply rank breaking ties.  A whole
    # counted bin is admitted with one more subset-rank calculation.
    matched_total = cumulative_rank
    groups_by_priority: dict[int, list[tuple[int, int]]] = {
        priority: [] for priority in priorities
    }
    for demand_group, demand_mask in zip(demand_groups, demand_masks):
        groups_by_priority[demand_group.priority].append(
            (demand_mask, demand_group.count))
    reachable_demand_by_subset = [0] * subset_count
    for subset in range(subset_count):
        reachable_demand_by_subset[subset] = sum(
            min(
                matched_by_priority[priority],
                sum(count
                    for demand_mask, count in groups_by_priority[priority]
                    if demand_mask & subset))
            for priority in priorities)

    assigned_by_supply = {item.supply_id: 0 for item in stable_supply}
    selected_capacity_by_subset = [0] * subset_count
    selected_total = 0
    for capacity_bin in bins:
        if selected_total == matched_total:
            break
        if capacity_bin.card not in card_index:
            continue
        card_bit = 1 << card_index[capacity_bin.card]
        candidate_total = selected_total + capacity_bin.capacity
        candidate_rank = min(
            matched_total,
            min(candidate_total - selected_capacity_by_subset[subset] -
                (capacity_bin.capacity if subset & card_bit else 0) +
                reachable_demand_by_subset[subset]
                for subset in range(subset_count)))
        selected_from_bin = candidate_rank - selected_total
        assert 0 <= selected_from_bin <= capacity_bin.capacity
        if selected_from_bin == 0:
            continue
        assigned_by_supply[capacity_bin.supply_id] += selected_from_bin
        selected_total += selected_from_bin
        _add_to_supersets(selected_capacity_by_subset, card_bit,
                          selected_from_bin)
    assert selected_total == matched_total

    assigned_by_card = {item.card: 0 for item in stable_supply}
    for item in stable_supply:
        assigned_by_card[item.card] += assigned_by_supply[item.supply_id]
    unmatched_by_priority = {
        priority: demand_by_priority[priority] - matched_by_priority[priority]
        for priority in priorities
    }

    return CompatibilityMatch(
        assigned_by_supply=tuple(
            (item.supply_id, assigned_by_supply[item.supply_id])
            for item in stable_supply),
        assigned_by_card=tuple(sorted(assigned_by_card.items())),
        unmatched_by_priority=tuple(
            (priority, unmatched_by_priority[priority])
            for priority in sorted(priorities, reverse=True)),
    )
