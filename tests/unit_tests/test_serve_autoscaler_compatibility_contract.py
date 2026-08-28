"""Characterization tests for autoscaler compatibility policy helpers."""
# pylint: disable=protected-access
import inspect
import os
import pickle
import subprocess
import sys
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
        'committed_zero_cost': {},
        'free_reserved': {},
        'ready_paid': {},
        'committed_paid': {},
        'supply_preference':
            (autoscaler_compatibility.SupplyPreference.WARM_FIRST),
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
             'ready_zero_cost: dict[str, int], committed_zero_cost: '
             'dict[str, int], free_reserved: dict[str, int], ready_paid: '
             'dict[str, int], committed_paid: dict[str, int], '
             'supply_preference: sky.serve.autoscaler_compatibility.'
             'SupplyPreference, '
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
             'allow_unbacked_adopted_reassignment: bool = True, '
             'allow_mixed_version_backed_reassignment: bool = False, '
             'old_version_supply: dict[str, int] | None = None, '
             'reassignment_target_by_accelerator: dict[str, int] | None = '
             'None) -> '
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


def test_fresh_process_import_orders_preserve_identity(tmp_path):
    import_orders = (
        ('from sky.serve import autoscaler_compatibility as compatibility\n'
         'from sky.serve import autoscalers as facade'),
        ('from sky.serve import autoscalers as facade\n'
         'from sky.serve import autoscaler_compatibility as compatibility'),
    )
    for index, imports in enumerate(import_orders):
        code = f'''{imports}
import pickle

names = {_HELPER_NAMES!r}
for name in names:
    helper = getattr(facade, name)
    assert helper is getattr(compatibility, name)
    assert helper.__module__ == 'sky.serve.autoscalers'
    assert pickle.loads(pickle.dumps(helper)) is helper
'''
        env = os.environ.copy()
        env['SKY_RUNTIME_DIR'] = str(tmp_path / f'order-{index}')
        result = subprocess.run([sys.executable, '-c', code],
                                env=env,
                                text=True,
                                capture_output=True,
                                check=False)
        assert result.returncode == 0, result.stderr


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
                     ready_paid={'L4': 1},
                     committed_paid={'L4': 1},
                     use_existing_supply=True) == {
                         'L4': 1
                     }
    assert _allocate(demand_profiles=demand) == {'A100': 1}


def test_allocate_prefers_free_reservation_over_ready_paid_supply():
    demand = [(10, ('L4', 'A100'), 1.0)]
    assert _allocate(
        demand_profiles=demand,
        free_reserved={'A100': 1},
        ready_paid={'L4': 1},
        committed_paid={'L4': 1},
        supply_preference=(
            autoscaler_compatibility.SupplyPreference.ZERO_COST_FIRST),
        use_existing_supply=True) == {
            'A100': 1
        }


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
    """A card old-version rows still serve is mid-replacement, not gone.

    This is the invariant the pre-provenance version of this test pinned by
    accident: it passed no old-version supply, so it also froze cards whose
    capacity had genuinely vanished, which turned a preemption during a
    rolling update into paid same-card launch authority (#1301). The
    protected scenario is stated explicitly now.
    """
    revalidate = autoscalers._revalidate_actuation_target
    assert revalidate(adopted_target={'L4': 1},
                      desired_target={'A100': 1},
                      nonretiring_supply={'A100': 1},
                      configured_cards=['L4', 'A100'],
                      final_target=1,
                      allow_adopted_reassignment=False,
                      old_version_supply={'L4': 1}) == {
                          'L4': 1
                      }


def test_revalidate_requires_explicit_proof_to_move_old_backing():
    revalidate = autoscalers._revalidate_actuation_target
    common = {
        'adopted_target': {
            'A100': 3,
        },
        'desired_target': {
            'L4': 3,
        },
        'nonretiring_supply': {},
        'configured_cards': ['L4', 'A100'],
        'final_target': 3,
        'allow_adopted_reassignment': False,
        'old_version_supply': {
            'A100': 3,
        },
    }

    assert revalidate(**common) == {'A100': 3}
    assert revalidate(**common,
                      allow_mixed_version_backed_reassignment=True) == {
                          'L4': 3
                      }


def test_revalidate_bounds_movement_to_explicit_owned_subset():
    revalidate = autoscalers._revalidate_actuation_target
    assert revalidate(adopted_target={'A100': 3},
                      desired_target={'L4': 3},
                      nonretiring_supply={},
                      configured_cards=['L4', 'A100'],
                      final_target=3,
                      allow_adopted_reassignment=False,
                      allow_unbacked_adopted_reassignment=True,
                      allow_mixed_version_backed_reassignment=True,
                      old_version_supply={'A100': 1},
                      reassignment_target_by_accelerator={'L4': 1}) == {
                          'L4': 1,
                          'A100': 2,
                      }


def test_revalidate_mixed_version_proof_keeps_exact_compatible_card():
    revalidate = autoscalers._revalidate_actuation_target
    assert revalidate(adopted_target={'L4': 40},
                      desired_target={'L4': 40},
                      nonretiring_supply={'A100': 40},
                      configured_cards=['L4', 'A100'],
                      final_target=40,
                      allow_adopted_reassignment=False,
                      allow_mixed_version_backed_reassignment=True,
                      old_version_supply={'L4': 40}) == {
                          'L4': 40
                      }


def test_revalidate_releases_capacity_gone_from_every_generation():
    """No latest-version supply AND no old-version supply means gone.

    Preserving the card here is what bought paid A100 at roughly 6.8x while
    the same card-agnostic requests accept L4 (#1301). The unit follows the
    fresh placement instead, even mid-rollout.
    """
    revalidate = autoscalers._revalidate_actuation_target
    assert revalidate(adopted_target={'L4': 1},
                      desired_target={'A100': 1},
                      nonretiring_supply={'A100': 1},
                      configured_cards=['L4', 'A100'],
                      final_target=1,
                      allow_adopted_reassignment=False,
                      old_version_supply={}) == {
                          'A100': 1
                      }


def test_revalidate_without_old_version_provenance_fails_closed_mid_rollout():
    """Unknown old-version supply cannot prove an adopted card vanished."""
    revalidate = autoscalers._revalidate_actuation_target
    assert revalidate(adopted_target={'L4': 1},
                      desired_target={'A100': 1},
                      nonretiring_supply={'A100': 1},
                      configured_cards=['L4', 'A100'],
                      final_target=1,
                      allow_adopted_reassignment=False,
                      old_version_supply=None) == {
                          'L4': 1
                      }


def test_revalidate_complete_provenance_releases_only_vanished_capacity():
    """An explicit map distinguishes backed and vanished adopted units."""
    revalidate = autoscalers._revalidate_actuation_target
    assert revalidate(adopted_target={'L4': 3},
                      desired_target={'A100': 3},
                      nonretiring_supply={'A100': 3},
                      configured_cards=['L4', 'A100'],
                      final_target=3,
                      allow_adopted_reassignment=False,
                      old_version_supply={'L4': 1}) == {
                          'L4': 1,
                          'A100': 2,
                      }


def test_revalidate_without_provenance_matches_the_old_behaviour():
    """Omitting old_version_supply must not change legacy callers.

    The physical-path caller does not pass it. Outside a mixed-version rollout,
    the original release block still handles unbacked capacity.
    """
    revalidate = autoscalers._revalidate_actuation_target
    assert revalidate(adopted_target={'L4': 1},
                      desired_target={'A100': 1},
                      nonretiring_supply={'A100': 1},
                      configured_cards=['L4', 'A100'],
                      final_target=1) == {
                          'A100': 1
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
