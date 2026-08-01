"""Characterization tests for autoscaler compatibility policy helpers."""
# pylint: disable=protected-access
import inspect
import pickle
import types

from sky.serve import autoscaler_compatibility
from sky.serve import autoscalers

_HELPER_NAMES = (
    '_allocate_compatibility_target',
    '_replica_is_retiring_card_supply',
    '_merge_fresh_target_into_downscale_hold',
    '_revalidate_actuation_target',
)


def _allocate(**overrides):
    kwargs = {
        'configured_cards': ['L4', 'A100'],
        'capacities': {
            'L4': 1.0,
            'A100': 2.0,
        },
        'floors': {},
        'min_replicas': 0,
        'max_replicas': 10,
        'demand_profiles': [],
        'fixed_work_by_accelerator': {},
        'ready_zero_cost': {},
        'ready': {},
        'provisioning': {},
        'free_reserved': {},
        'cold_order': ['A100', 'L4'],
        'use_existing_supply': False,
    }
    kwargs.update(overrides)
    return autoscalers._allocate_compatibility_target(**kwargs)


def test_historical_helper_identity_and_signatures():
    expected_signatures = {
        '_allocate_compatibility_target':
            ('(*, configured_cards: list[str], capacities: dict[str, float], '
             'floors: dict[str, int], min_replicas: int, max_replicas: int, '
             'demand_profiles: list[tuple[int, tuple[str, ...], float]], '
             'fixed_work_by_accelerator: dict[str, float], '
             'ready_zero_cost: dict[str, int], ready: dict[str, int], '
             'provisioning: dict[str, int], free_reserved: dict[str, int], '
             'cold_order: list[str], use_existing_supply: bool) -> '
             'dict[str, int]'),
        '_replica_is_retiring_card_supply':
            ("(replica_info: 'replica_managers.ReplicaInfo') -> bool"),
        '_merge_fresh_target_into_downscale_hold': (
            '(*, adopted_target: dict[str, int], fresh_target: dict[str, int], '
            'configured_cards: list[str], replacement_order: list[str], '
            'target_total: int) -> dict[str, int]'),
        '_revalidate_actuation_target':
            ('(*, adopted_target: dict[str, int], desired_target: '
             'dict[str, int], nonretiring_supply: dict[str, int], '
             'configured_cards: list[str], final_target: int, '
             'allow_adopted_reassignment: bool = True, '
             'allow_unbacked_adopted_reassignment: bool = True) -> '
             'dict[str, int]'),
    }
    for name in _HELPER_NAMES:
        helper = getattr(autoscalers, name)
        assert helper is getattr(autoscaler_compatibility, name)
        assert helper.__name__ == name
        assert helper.__qualname__ == name
        assert helper.__module__ == 'sky.serve.autoscalers'
        assert str(inspect.signature(helper)) == expected_signatures[name]
        assert pickle.loads(pickle.dumps(helper)) is helper


def test_allocate_respects_bounded_per_card_floors():
    assert _allocate(floors={
        'l4': 2,
        'a100': 2
    }, max_replicas=3) == {
        'L4': 2,
        'A100': 1,
    }


def test_allocate_pins_fixed_work_to_its_current_card():
    assert _allocate(fixed_work_by_accelerator={'L4': 1.5}) == {'L4': 2}


def test_allocate_uses_cold_order_for_flexible_work():
    demand = [(10, ('L4', 'A100'), 3.0)]
    assert _allocate(demand_profiles=demand) == {'A100': 2}


def test_allocate_reuses_materialized_supply_before_cold_order():
    demand = [(10, ('L4', 'A100'), 1.0)]
    assert _allocate(demand_profiles=demand,
                     ready={'L4': 1},
                     use_existing_supply=True) == {
                         'L4': 1
                     }
    assert _allocate(demand_profiles=demand) == {'A100': 1}


def test_allocate_prioritizes_more_constrained_equal_priority_demand():
    demand = [
        (10, ('L4', 'A100'), 1.0),
        (10, ('L4',), 1.0),
    ]
    assert _allocate(demand_profiles=demand, max_replicas=2) == {
        'L4': 1,
        'A100': 1,
    }


def test_retiring_supply_classification():

    def replica(*, is_scale_down=False, preempted=False):
        status = types.SimpleNamespace(is_scale_down=is_scale_down,
                                       preempted=preempted)
        return types.SimpleNamespace(status_property=status)

    assert not autoscalers._replica_is_retiring_card_supply(replica())
    assert autoscalers._replica_is_retiring_card_supply(
        replica(is_scale_down=True))
    assert autoscalers._replica_is_retiring_card_supply(replica(preempted=True))


def test_merge_fresh_target_replaces_only_unrequired_held_slots():
    merge = autoscalers._merge_fresh_target_into_downscale_hold
    assert merge(adopted_target={'L4': 2},
                 fresh_target={'A100': 1},
                 configured_cards=['L4', 'A100'],
                 replacement_order=['L4'],
                 target_total=2) == {
                     'L4': 1,
                     'A100': 1,
                 }
    assert merge(adopted_target={'L4': 1},
                 fresh_target={'A100': 2},
                 configured_cards=['L4', 'A100'],
                 replacement_order=['L4'],
                 target_total=1) == {}
    assert merge(adopted_target={'unknown': 1},
                 fresh_target={},
                 configured_cards=['L4', 'A100'],
                 replacement_order=[],
                 target_total=1) == {}


def test_revalidate_rejects_inconsistent_desired_total():
    revalidate = autoscalers._revalidate_actuation_target
    assert revalidate(adopted_target={'L4': 1},
                      desired_target={'A100': 2},
                      nonretiring_supply={},
                      configured_cards=['L4', 'A100'],
                      final_target=1) == {}


def test_revalidate_preserves_adopted_map_when_reassignment_is_disabled():
    revalidate = autoscalers._revalidate_actuation_target
    assert revalidate(adopted_target={'L4': 1},
                      desired_target={'A100': 1},
                      nonretiring_supply={'A100': 1},
                      configured_cards=['L4', 'A100'],
                      final_target=1,
                      allow_adopted_reassignment=False) == {
                          'L4': 1
                      }


def test_revalidate_moves_unbacked_or_materialized_compatible_supply():
    revalidate = autoscalers._revalidate_actuation_target
    common = {
        'adopted_target': {
            'L4': 1,
        },
        'desired_target': {
            'A100': 1,
        },
        'configured_cards': ['L4', 'A100'],
        'final_target': 1,
    }
    assert revalidate(nonretiring_supply={}, **common) == {'A100': 1}
    assert revalidate(nonretiring_supply={
        'L4': 1,
        'A100': 1,
    }, **common) == {
        'A100': 1
    }
