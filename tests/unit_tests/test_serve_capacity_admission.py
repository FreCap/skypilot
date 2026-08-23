"""Pure contract tests for ordered SkyServe capacity admission."""

import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky.serve import capacity_admission
from sky.serve import reserved_fill_planner
from sky.serve import serve_state_schema


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
        'reserved_fill_authority':
            (capacity_admission.ReservedFillPlanAuthority.not_applicable()),
    }
    values.update(overrides)
    return capacity_admission.CapacityPlanInput(**values)


def _allocation_identity(
) -> reserved_fill_planner.ReservedFillAllocationIdentity:
    return reserved_fill_planner.ReservedFillAllocationIdentity(
        allocation_generation=7,
        allocation_input_sha256='1' * 64,
        allocation_claim_generation=11,
        service_version=7,
        ordinary_zero_cost_admission_sequence_high_water=13,
        reconciliation_gate_generation=5,
        reclaim_fleet_bundle_sha256='2' * 64,
        reclaim_policy_revision='policy-v1',
        reclaim_provider_inventory_sha256='3' * 64)


def test_paid_claim_constraints_are_postgresql_only():
    table = serve_state_schema.paid_capacity_claims_table
    sqlite_ddl = str(
        sqlalchemy.schema.CreateTable(table).compile(dialect=sqlite.dialect()))
    postgres_ddl = str(
        sqlalchemy.schema.CreateTable(table).compile(
            dialect=postgresql.dialect()))

    for constraint_name in ('serve050_paid_claim_plan_complete_ck',
                            'serve050_paid_claim_plan_values_ck'):
        assert constraint_name not in sqlite_ddl
        assert constraint_name in postgres_ddl


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


def test_capacity_plan_subtracts_allocation_tail_before_paid_residual():
    plan = _input(capacity_target_by_accelerator={
        'L4': 1,
        'H200': 2,
    },
                  allocation_reserved_capacity_by_accelerator={
                      'L4': 0,
                      'H200': 1,
                  },
                  expected_pending_zero_cost_capacity_by_accelerator={
                      'L4': 0,
                      'H200': 1,
                  })

    payload = plan.payload(existing_zero_cost_capacity_by_accelerator={
        'L4': 0,
        'H200': 0,
    },
                           pending_zero_cost_capacity_by_accelerator={
                               'L4': 0,
                               'H200': 1,
                           },
                           allocation_reserved_capacity_by_accelerator={
                               'L4': 0,
                               'H200': 1,
                           },
                           existing_paid_capacity_by_accelerator={
                               'L4': 0,
                               'H200': 0,
                           },
                           paid_residual_by_accelerator={'L4': 1})

    assert payload['allocation_reserved_capacity_by_accelerator'] == {
        'h200': 1,
        'l4': 0,
    }
    assert payload['pending_zero_cost_capacity_by_accelerator'] == {
        'h200': 1,
        'l4': 0,
    }
    assert payload['paid_residual_by_accelerator'] == {'l4': 1}
    with pytest.raises(ValueError, match='exact post-zero-cost'):
        plan.payload(existing_zero_cost_capacity_by_accelerator={
            'L4': 0,
            'H200': 0,
        },
                     pending_zero_cost_capacity_by_accelerator={
                         'L4': 0,
                         'H200': 1,
                     },
                     allocation_reserved_capacity_by_accelerator={
                         'L4': 0,
                         'H200': 1,
                     },
                     existing_paid_capacity_by_accelerator={
                         'L4': 0,
                         'H200': 0,
                     },
                     paid_residual_by_accelerator={
                         'L4': 1,
                         'H200': 1,
                     })


def test_reserved_fill_plan_authority_round_trips_canonical_identity():
    identity = _allocation_identity()
    authority = capacity_admission.ReservedFillPlanAuthority.bound(identity)

    encoded = authority.to_mapping()

    assert (capacity_admission.ReservedFillPlanAuthority.from_mapping(encoded)
            == authority)
    assert encoded == {
        'mode': 'ALLOCATION_BOUND',
        'allocation': identity.to_mapping(),
    }
    with pytest.raises(ValueError, match='malformed'):
        reserved_fill_planner.ReservedFillAllocationIdentity.from_mapping({
            **identity.to_mapping(), 'future_field': 1
        })


def test_zero_revocation_is_explicit_unbound_and_all_zero():
    authority = capacity_admission.ReservedFillPlanAuthority.zero_revocation()
    zero_input = _input(capacity_target_by_accelerator={'L4': 0},
                        reserved_fill_authority=authority)

    payload = zero_input.payload(
        existing_zero_cost_capacity_by_accelerator={'L4': 0},
        existing_paid_capacity_by_accelerator={'L4': 0},
        paid_residual_by_accelerator={})

    assert payload['reserved_fill_authority'] == {
        'mode': 'UNBOUND_ZERO_REVOCATION'
    }
    with pytest.raises(ValueError, match='all-zero'):
        _input(reserved_fill_authority=authority).payload(
            existing_zero_cost_capacity_by_accelerator={'L4': 0},
            existing_paid_capacity_by_accelerator={'L4': 0},
            paid_residual_by_accelerator={'L4': 5})


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
        paid_residual_by_accelerator=(('l4', 4),),
        reserved_fill_authority=(
            capacity_admission.ReservedFillPlanAuthority.not_applicable()))
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
        paid_residual_by_accelerator=(('*', 2),),
        reserved_fill_authority=(
            capacity_admission.ReservedFillPlanAuthority.not_applicable()))
    assert aggregate.claim_values('A100',
                                  units=1)['capacity_plan_accelerator'] == '*'
