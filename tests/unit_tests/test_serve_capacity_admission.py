"""Pure contract tests for ordered SkyServe capacity admission."""

import pytest

from sky.serve import capacity_admission


def _input(**overrides) -> capacity_admission.CapacityPlanInput:
    values = {
        'service_name': 'svc',
        'service_hash': 'service-hash',
        'service_lifecycle_epoch': 3,
        'service_version': 7,
        'demand_source_epoch': 2,
        'demand_feed_generation': 11,
        'receipt_watermark': [{
            'reporter_session_id': 'reporter-a',
            'sequence': 4,
            'payload_sha256': 'a' * 64,
        }],
        'route_generation': 5,
        'route_sha256': 'b' * 64,
        'route_source_epoch': 1,
        'normalized_demand': {
            'recent_request_count': 5,
        },
        'capacity_target_by_accelerator': {
            'L4': 5,
        },
    }
    values.update(overrides)
    return capacity_admission.CapacityPlanInput(**values)


def test_capacity_plan_requires_exact_post_zero_cost_residual():
    payload = _input().payload(
        existing_zero_cost_capacity_by_accelerator={'l4': 2},
        existing_paid_capacity_by_accelerator={'L4': 1},
        paid_residual_by_accelerator={'l4': 2})

    assert payload['service'] == {
        'name': 'svc',
        'hash': 'service-hash',
        'lifecycle_epoch': 3,
        'version': 7,
    }
    assert payload['existing_zero_cost_capacity_by_accelerator'] == {'l4': 2}
    assert payload['existing_paid_capacity_by_accelerator'] == {'l4': 1}
    assert payload['paid_residual_by_accelerator'] == {'l4': 2}

    with pytest.raises(ValueError, match='exact post-zero-cost'):
        _input().payload(existing_zero_cost_capacity_by_accelerator={'L4': 2},
                         existing_paid_capacity_by_accelerator={'L4': 1},
                         paid_residual_by_accelerator={'L4': 3})


def test_capacity_plan_uses_supply_aware_target_not_cold_demand_card():
    payload = _input(capacity_target_by_accelerator={
        'L4': 0,
        'A100': 5,
    },
                     normalized_demand={
                         'demand_target_by_accelerator': {
                             'L4': 5,
                         }
                     }).payload(existing_zero_cost_capacity_by_accelerator={
                         'L4': 0,
                         'A100': 4,
                     },
                                existing_paid_capacity_by_accelerator={
                                    'L4': 0,
                                    'A100': 0,
                                },
                                paid_residual_by_accelerator={'A100': 1})

    assert payload['normalized_demand']['demand_target_by_accelerator'] == {
        'L4': 5,
    }
    assert payload['capacity_target_by_accelerator'] == {
        'a100': 5,
        'l4': 0,
    }
    assert payload['paid_residual_by_accelerator'] == {'a100': 1}


def test_capacity_plan_rejects_mixed_aggregate_and_exact_cards():
    with pytest.raises(ValueError, match='cannot mix aggregate'):
        _input(capacity_target_by_accelerator={
            '*': 5,
            'L4': 1,
        }).payload(existing_zero_cost_capacity_by_accelerator={
            '*': 0,
            'L4': 0,
        },
                   existing_paid_capacity_by_accelerator={
                       '*': 0,
                       'L4': 0,
                   },
                   paid_residual_by_accelerator={
                       '*': 5,
                       'L4': 1,
                   })


def test_paid_launch_authority_debits_exact_or_aggregate_units():
    exact = capacity_admission.PaidLaunchAuthority(
        service_name='svc',
        service_hash='hash',
        generation=3,
        content_sha256='c' * 64,
        demand_feed_generation=9,
        demand_source_epoch=2,
        paid_residual_by_accelerator=(('l4', 4),))
    claim = exact.claim_values('L4', units=4)
    assert claim['capacity_plan_accelerator'] == 'l4'
    assert claim['capacity_plan_units'] == 4
    with pytest.raises(capacity_admission.CapacityAdmissionConflict):
        exact.claim_values('L4', units=5)

    aggregate = capacity_admission.PaidLaunchAuthority(
        service_name='svc',
        service_hash='hash',
        generation=4,
        content_sha256='d' * 64,
        demand_feed_generation=10,
        demand_source_epoch=2,
        paid_residual_by_accelerator=(('*', 2),))
    assert aggregate.claim_values('A100',
                                  units=1)['capacity_plan_accelerator'] == '*'
