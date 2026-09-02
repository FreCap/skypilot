"""Tests for the one counted compatibility-matching kernel."""

import dataclasses
import itertools
import random
import time

import pytest

from sky.serve import compatibility_matching
from sky.serve import constants


def _demand(cards: str,
            *,
            priority: int = 0,
            count: int = 1) -> compatibility_matching.CompatibilityDemand:
    return compatibility_matching.CompatibilityDemand(
        priority=priority, compatible_cards=tuple(cards), count=count)


def _supply(supply_id: str,
            card: str,
            *,
            capacity: int = 1,
            preferred: int = 0,
            rank: int = 0) -> compatibility_matching.CompatibilitySupply:
    return compatibility_matching.CompatibilitySupply(
        supply_id=supply_id,
        card=card,
        capacity=capacity,
        preferred_capacity=preferred,
        stable_rank=rank)


def _match(
    demand: list[compatibility_matching.CompatibilityDemand],
    supply: list[compatibility_matching.CompatibilitySupply],
    *,
    maximum_assigned: int | None = None,
) -> compatibility_matching.CompatibilityMatch:
    return compatibility_matching.match_compatible_capacity(
        demand=demand, supply=supply, maximum_assigned=maximum_assigned)


def test_global_match_fills_abc_repro() -> None:
    result = _match([_demand('AB'), _demand('AB'),
                     _demand('AC')], [
                         _supply('pool-a', 'A', rank=0),
                         _supply('pool-b', 'B', rank=1),
                         _supply('pool-c', 'C', rank=2),
                     ])

    assert dict(result.assigned_by_card) == {'a': 1, 'b': 1, 'c': 1}
    assert result.unmatched_by_priority == ((0, 0),)


def test_accelerator_names_are_case_insensitive() -> None:
    result = _match([
        compatibility_matching.CompatibilityDemand(
            priority=0, compatible_cards=('a100',), count=1)
    ], [
        _supply('east-a100', 'A100', preferred=1),
    ])

    assert result.assigned_by_supply == (('east-a100', 1),)
    assert result.assigned_by_card == (('a100', 1),)
    assert result.unmatched_by_priority == ((0, 0),)


def test_exact_and_flexible_demand_use_global_match() -> None:
    result = _match([
        _demand('AB', count=2),
        _demand('A'),
    ], [
        _supply('pool-a', 'A'),
        _supply('pool-b', 'B', capacity=2, rank=1),
    ])

    assert dict(result.assigned_by_card) == {'a': 1, 'b': 2}
    assert result.unmatched_by_priority == ((0, 0),)


def test_priority_is_lexicographic_before_cardinality() -> None:
    result = _match([
        _demand('AB', priority=10),
        _demand('A', priority=0, count=2),
    ], [
        _supply('pool-a', 'A'),
        _supply('pool-b', 'B', rank=1),
    ])

    assert sum(dict(result.assigned_by_card).values()) == 2
    assert result.unmatched_by_priority == ((10, 0), (0, 1))


def test_preference_does_not_reduce_priority_or_cardinality() -> None:
    # The preferred A pool is not compatible with the high-priority B-only
    # class.  Preference therefore cannot steal B or reduce the optimal match.
    result = _match([
        _demand('B', priority=10),
        _demand('AB', priority=0),
    ], [
        _supply('fresh-b', 'B', rank=0),
        _supply('preferred-a', 'A', preferred=1, rank=10),
    ])

    assert dict(result.assigned_by_supply) == {
        'fresh-b': 1,
        'preferred-a': 1,
    }
    assert result.unmatched_by_priority == ((10, 0), (0, 0))


def test_preferred_capacity_precedes_stable_rank() -> None:
    result = _match([_demand('AB')], [
        _supply('rank-zero-fresh', 'A', rank=0),
        _supply('rank-ten-preferred', 'B', preferred=1, rank=10),
    ])

    assert dict(result.assigned_by_supply) == {
        'rank-zero-fresh': 0,
        'rank-ten-preferred': 1,
    }


def test_zero_cost_caller_encoding_beats_every_paid_rank() -> None:
    result = _match([_demand('AB', count=4)], [
        _supply('paid-rank-zero', 'A', capacity=4, rank=0),
        _supply('zero-cost-rank-ten', 'B', capacity=4, preferred=4, rank=10),
    ])

    assert dict(result.assigned_by_supply) == {
        'paid-rank-zero': 0,
        'zero-cost-rank-ten': 4,
    }


def test_global_assignment_cap_selects_one_best_partial_wave() -> None:
    result = _match([_demand('AB', priority=2, count=100)], [
        _supply('preferred-a', 'A', capacity=7, preferred=7, rank=0),
        _supply('preferred-b', 'B', capacity=5, preferred=5, rank=1),
        _supply('nonpreferred-a', 'A', capacity=100, rank=2),
    ],
                    maximum_assigned=10)

    assert dict(result.assigned_by_supply) == {
        'preferred-a': 7,
        'preferred-b': 3,
        'nonpreferred-a': 0,
    }
    assert result.unmatched_by_priority == ((2, 90),)


def test_global_assignment_cap_preserves_priority() -> None:
    result = _match([
        _demand('AB', priority=10, count=2),
        _demand('A', priority=0, count=2),
    ], [
        _supply('a', 'A', capacity=2),
        _supply('b', 'B', capacity=2, rank=1),
    ],
                    maximum_assigned=2)

    assert sum(dict(result.assigned_by_supply).values()) == 2
    assert result.unmatched_by_priority == ((10, 0), (0, 2))


def test_duplicate_same_card_pools_use_stable_rank() -> None:
    result = _match([_demand('A', count=3)], [
        _supply('later', 'A', capacity=2, rank=20),
        _supply('first', 'A', capacity=2, rank=10),
    ])

    assert dict(result.assigned_by_supply) == {'first': 2, 'later': 1}


def test_input_permutations_do_not_change_result() -> None:
    demand = [
        _demand('BA', priority=2, count=2),
        _demand('CA', priority=1),
        _demand('C', priority=-1),
    ]
    supply = [
        _supply('a', 'A', preferred=1, rank=1),
        _supply('b', 'B', capacity=2, rank=0),
        _supply('c', 'C', rank=2),
    ]
    expected = _match(demand, supply)

    for demand_order in itertools.permutations(demand):
        for supply_order in itertools.permutations(supply):
            assert _match(list(demand_order), list(supply_order)) == expected


def test_adding_compatible_supply_cannot_reduce_any_priority_match() -> None:
    demand = [
        _demand('AB', priority=4, count=2),
        _demand('AC', priority=2, count=2),
        _demand('C', priority=0, count=2),
    ]
    initial_supply = [_supply('a', 'A'), _supply('c', 'C', rank=1)]
    expanded_supply = initial_supply + [_supply('b', 'B', rank=2)]
    before = dict(_match(demand, initial_supply).unmatched_by_priority)
    after = dict(_match(demand, expanded_supply).unmatched_by_priority)

    assert all(after[priority] <= before[priority] for priority in before)


def test_sparse_large_counts_remain_counted() -> None:
    result = _match([
        _demand('AB', priority=1, count=1_000_000),
        _demand('B', priority=0, count=1_000_000),
    ], [
        _supply('a', 'A', capacity=750_000, preferred=500_000),
        _supply('b', 'B', capacity=500_000, rank=1),
    ])

    assert sum(dict(result.assigned_by_supply).values()) == 1_250_000
    assert result.unmatched_by_priority == ((1, 0), (0, 750_000))


def test_lower_priority_demand_reroutes_higher_priority_assignment() -> None:
    result = _match([
        _demand('AB', priority=10),
        _demand('A', priority=0),
    ], [
        _supply('a', 'A'),
        _supply('b', 'B', rank=1),
    ])

    assert dict(result.assigned_by_card) == {'a': 1, 'b': 1}
    assert result.unmatched_by_priority == ((10, 0), (0, 0))


def test_many_distinct_classes_have_bounded_matching_cost() -> None:
    cards = tuple('ABCDEFGH')
    compatibility_sets = tuple(
        subset for size in range(1,
                                 len(cards) + 1)
        for subset in itertools.combinations(cards, size))
    demand = [
        compatibility_matching.CompatibilityDemand(priority=priority,
                                                   compatible_cards=subset,
                                                   count=1)
        for priority in range(8)
        for subset in compatibility_sets
    ]
    # Physical-pool count must not create a demand-by-pool graph.  Each card
    # has 64 distinct pools here, and every pool contributes two preference
    # bins.
    supply = [
        _supply(f'pool-{index:04d}',
                cards[index % len(cards)],
                capacity=5,
                preferred=2,
                rank=index) for index in range(512)
    ]

    started = time.monotonic()
    result = _match(demand, supply, maximum_assigned=2_000)
    elapsed = time.monotonic() - started

    assert sum(dict(result.assigned_by_supply).values()) == 2_000
    assert result.unmatched_by_priority == (
        (7, 0),
        (6, 0),
        (5, 0),
        (4, 0),
        (3, 0),
        (2, 0),
        (1, 0),
        (0, 40),
    )
    assert elapsed < 10
    assert _match(list(reversed(demand)),
                  list(reversed(supply)),
                  maximum_assigned=2_000) == result


def test_supply_only_cards_do_not_expand_matching_universe() -> None:
    supply = [_supply('matching', 'A')]
    supply.extend(
        _supply(f'unused-{index}', f'UNUSED-{index}', rank=index + 1)
        for index in range(100))

    result = _match([_demand('A')], supply)

    assert dict(result.assigned_by_supply)['matching'] == 1
    assert sum(dict(result.assigned_by_supply).values()) == 1


def test_demand_card_universe_uses_request_contract_limit() -> None:
    cards = tuple(
        f'card-{index}'
        for index in range(constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS + 1))
    demand = compatibility_matching.CompatibilityDemand(priority=0,
                                                        compatible_cards=cards,
                                                        count=1)

    # Unavailable demand-only cards do not expand the subset universe.
    assert _match([demand],
                  [_supply('first', cards[0])]).assigned_by_supply == (('first',
                                                                        1),)

    with pytest.raises(ValueError, match='more than 8 accelerator cards'):
        _match([demand], [
            _supply(f'supply-{index}', card, rank=index)
            for index, card in enumerate(cards)
        ])


def _full_objective(
    result: compatibility_matching.CompatibilityMatch,
    demand: list[compatibility_matching.CompatibilityDemand],
    supply: list[compatibility_matching.CompatibilitySupply],
) -> tuple[int, ...]:
    demand_by_priority: dict[int, int] = {}
    for item in demand:
        demand_by_priority[item.priority] = (
            demand_by_priority.get(item.priority, 0) + item.count)
    unmatched = dict(result.unmatched_by_priority)
    priority_objective = tuple(
        -(demand_by_priority[priority] - unmatched[priority])
        for priority in sorted(demand_by_priority, reverse=True))
    stable_supply = sorted(supply,
                           key=lambda item:
                           (item.stable_rank, item.card, item.supply_id))
    assigned = dict(result.assigned_by_supply)
    nonpreferred = sum(
        max(0, assigned[item.supply_id] - item.preferred_capacity)
        for item in stable_supply)
    stable_rank = sum(assigned[item.supply_id] * rank
                      for rank, item in enumerate(stable_supply))
    return priority_objective + (nonpreferred, stable_rank)


def _brute_force_full_objective(
    demand: list[compatibility_matching.CompatibilityDemand],
    supply: list[compatibility_matching.CompatibilitySupply],
    maximum_assigned: int | None,
) -> tuple[int, ...]:
    demand_units = tuple((item.priority, item.compatible_cards)
                         for item in demand
                         for _ in range(item.count))
    stable_supply = sorted(supply,
                           key=lambda item:
                           (item.stable_rank, item.card, item.supply_id))
    supply_units = tuple((item.card, copy < item.preferred_capacity, rank)
                         for rank, item in enumerate(stable_supply)
                         for copy in range(item.capacity))
    priorities = sorted({priority for priority, _ in demand_units},
                        reverse=True)
    matched_by_priority = {priority: 0 for priority in priorities}
    best: tuple[int, ...] | None = None

    def visit(demand_index: int, used_supply: int, nonpreferred: int,
              stable_rank: int) -> None:
        nonlocal best
        if demand_index == len(demand_units):
            candidate = tuple(
                -matched_by_priority[priority]
                for priority in priorities) + (nonpreferred, stable_rank)
            if best is None or candidate < best:
                best = candidate
            return
        visit(demand_index + 1, used_supply, nonpreferred, stable_rank)
        if (maximum_assigned is not None and
                used_supply.bit_count() >= maximum_assigned):
            return
        priority, compatible_cards = demand_units[demand_index]
        for supply_index, (card, preferred, rank) in enumerate(supply_units):
            supply_bit = 1 << supply_index
            if used_supply & supply_bit or card not in compatible_cards:
                continue
            matched_by_priority[priority] += 1
            visit(demand_index + 1, used_supply | supply_bit,
                  nonpreferred + (not preferred), stable_rank + rank)
            matched_by_priority[priority] -= 1

    visit(0, 0, 0, 0)
    assert best is not None
    return best


def test_small_graphs_match_brute_force_full_objective() -> None:
    compatibility_sets = (('A',), ('B',), ('A', 'B'))
    for priorities in ((0, 0), (1, 0)):
        for demand_cards in itertools.product(compatibility_sets, repeat=2):
            demand = [
                compatibility_matching.CompatibilityDemand(
                    priority=priority, compatible_cards=cards, count=1)
                for priority, cards in zip(priorities, demand_cards)
            ]
            for supply_cards in itertools.product(('A', 'B'), repeat=2):
                for preferred in itertools.product((0, 1), repeat=2):
                    supply = [
                        _supply(f'supply-{index}',
                                card,
                                preferred=preferred[index],
                                rank=index)
                        for index, card in enumerate(supply_cards)
                    ]
                    for maximum_assigned in (None, 1):
                        result = _match(demand,
                                        supply,
                                        maximum_assigned=maximum_assigned)
                        assert _full_objective(
                            result, demand,
                            supply) == _brute_force_full_objective(
                                demand, supply, maximum_assigned)


def _random_counted_graph(
    rng: random.Random,
) -> tuple[list[compatibility_matching.CompatibilityDemand],
           list[compatibility_matching.CompatibilitySupply], int | None]:
    """Draw counted classes, split preferred bins, and an optional cap."""
    cards = ('A', 'B', 'C', 'D')[:rng.randint(1, 4)]
    demand: list[compatibility_matching.CompatibilityDemand] = []
    demand_units = 0
    demand_budget = rng.randint(1, 6)
    while demand_units < demand_budget:
        count = min(rng.randint(1, 3), demand_budget - demand_units)
        demand.append(
            compatibility_matching.CompatibilityDemand(
                priority=rng.randint(0, 3),
                compatible_cards=tuple(
                    rng.sample(cards, rng.randint(1, len(cards)))),
                count=count))
        demand_units += count
    supply: list[compatibility_matching.CompatibilitySupply] = []
    supply_units = 0
    supply_budget = rng.randint(1, 6)
    while supply_units < supply_budget:
        capacity = min(rng.randint(1, 3), supply_budget - supply_units)
        supply.append(
            _supply(f'supply-{len(supply)}',
                    rng.choice(cards),
                    capacity=capacity,
                    preferred=rng.randint(0, capacity),
                    rank=rng.randint(0, 2)))
        supply_units += capacity
    maximum_assigned = rng.choice((None, rng.randint(0, supply_units)))
    return demand, supply, maximum_assigned


def test_counted_random_graphs_match_brute_force_full_objective() -> None:
    """Counted classes, split bins, and global caps stay exactly optimal.

    The exhaustive oracle above only draws unit demand and unit supply.  This
    seeded draw exercises the counted subset-rank and bin-splitting paths that
    production actually takes (multi-unit classes, pools holding several
    units, partial preferred prefixes, and partial waves).
    """
    rng = random.Random(20260902)
    for _ in range(1500):
        demand, supply, maximum_assigned = _random_counted_graph(rng)
        result = _match(demand, supply, maximum_assigned=maximum_assigned)
        assigned = dict(result.assigned_by_supply)
        assert all(
            0 <= assigned[item.supply_id] <= item.capacity for item in supply)
        assert sum(assigned.values()) == sum(
            dict(result.assigned_by_card).values())
        if maximum_assigned is not None:
            assert sum(assigned.values()) <= maximum_assigned
        assert _full_objective(result, demand,
                               supply) == _brute_force_full_objective(
                                   demand, supply, maximum_assigned)


def _brute_force_maximum(cards_by_demand: tuple[tuple[str, ...], ...],
                         cards: tuple[str, ...]) -> int:

    def _visit(index: int, used: frozenset[str]) -> int:
        if index == len(cards_by_demand):
            return 0
        best = _visit(index + 1, used)
        for card in cards_by_demand[index]:
            if card not in used:
                best = max(best, 1 + _visit(index + 1, used | {card}))
        return best

    assert set(itertools.chain.from_iterable(cards_by_demand)) <= set(cards)
    return _visit(0, frozenset())


def test_small_graphs_match_brute_force_maximum() -> None:
    cards = ('A', 'B', 'C')
    compatibility_sets = tuple(
        subset for size in range(1,
                                 len(cards) + 1)
        for subset in itertools.combinations(cards, size))
    supply = [
        _supply(card.lower(), card, rank=index)
        for index, card in enumerate(cards)
    ]

    for cards_by_demand in itertools.product(compatibility_sets, repeat=3):
        result = _match([
            compatibility_matching.CompatibilityDemand(
                priority=0, compatible_cards=compatible, count=1)
            for compatible in cards_by_demand
        ], supply)
        assert sum(dict(
            result.assigned_by_card).values()) == (_brute_force_maximum(
                cards_by_demand, cards))


def test_result_and_inputs_are_immutable() -> None:
    demand = _demand('BA')
    supply = _supply('pool-a', 'A')
    result = _match([demand], [supply])

    assert demand.compatible_cards == ('a', 'b')
    with pytest.raises(dataclasses.FrozenInstanceError):
        demand.count = 2  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.assigned_by_card = ()  # type: ignore[misc]


@pytest.mark.parametrize('factory', [
    lambda: compatibility_matching.CompatibilityDemand(
        priority=True, compatible_cards=('A',), count=1),
    lambda: compatibility_matching.CompatibilityDemand(
        priority=0, compatible_cards=['A'], count=1),
    lambda: compatibility_matching.CompatibilityDemand(
        priority=0, compatible_cards=('A', 'A'), count=1),
    lambda: compatibility_matching.CompatibilityDemand(
        priority=0, compatible_cards=('A100', 'a100'), count=1),
    lambda: compatibility_matching.CompatibilityDemand(
        priority=0, compatible_cards=('A',), count=0),
    lambda: compatibility_matching.CompatibilitySupply(supply_id='a',
                                                       card='A',
                                                       capacity=1,
                                                       preferred_capacity=2,
                                                       stable_rank=0),
    lambda: compatibility_matching.CompatibilitySupply(supply_id='a',
                                                       card='A',
                                                       capacity=True,
                                                       preferred_capacity=0,
                                                       stable_rank=0),
])
def test_malformed_inputs_fail_exactly(factory: object) -> None:
    with pytest.raises(ValueError):
        factory()  # type: ignore[operator]


def test_duplicate_supply_identity_is_rejected() -> None:
    with pytest.raises(ValueError, match='identities must be unique'):
        _match([_demand('A')], [
            _supply('same', 'A'),
            _supply('same', 'A', rank=1),
        ])


@pytest.mark.parametrize('maximum_assigned', [-1, True, 1.5])
def test_malformed_global_assignment_cap_is_rejected(
        maximum_assigned: object) -> None:
    with pytest.raises(ValueError, match='must be nonnegative'):
        compatibility_matching.match_compatible_capacity(
            demand=[_demand('A')],
            supply=[_supply('a', 'A')],
            maximum_assigned=maximum_assigned)  # type: ignore[arg-type]
