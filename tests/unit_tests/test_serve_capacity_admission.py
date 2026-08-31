"""Pure contract tests for ordered SkyServe capacity admission."""

import copy
import dataclasses

import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky.serve import capacity_admission
from sky.serve import capacity_planning
from sky.serve import kueue_lane_capacity
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
        'paid_residual': capacity_planning.AcceleratorCapacity.from_mapping(
            {'L4': 2}),
        'paid_launch_target':
            (capacity_planning.AcceleratorCapacity.from_mapping({'L4': 2})),
    }
    values.update(overrides)
    if 'paid_launch_target' not in overrides:
        values['paid_launch_target'] = values['paid_residual']
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


def _fill_config(
    *,
    binding_required: bool = True,
    utilization_gate: bool = True,
) -> capacity_admission._ReservedFillServiceConfig:
    return capacity_admission._ReservedFillServiceConfig(
        binding_required=binding_required,
        max_capacity=20,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL,
        reserved_accelerators=('l4',),
        worker_projection_sha256='4' * 64,
        configured_utilization_gate=utilization_gate,
        fill_policy_sha256='5' * 64)


def _gate_allocation(
    *,
    armed: bool,
    demonstrated_need: int | None = None,
    settled: bool = True,
) -> reserved_fill_planner.AuthenticatedAllocationMap:
    return reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=7,
        allocation_claim_generation=11,
        service_version=7,
        ordinary_zero_cost_admission_sequence_high_water=13,
        reconciliation_gate_generation=5,
        reclaim_fleet_bundle_sha256='2' * 64,
        reclaim_policy_revision='policy-v1',
        reclaim_provider_inventory_sha256='3' * 64,
        utilization_gate_armed=armed,
        utilization_demonstrated_need=demonstrated_need,
        utilization_ceiling=(0 if demonstrated_need is None else
                             demonstrated_need),
        upward_grants_settled=settled,
        pool_snapshots=())


def _capacity(**values: int) -> capacity_planning.AcceleratorCapacity:
    return capacity_planning.AcceleratorCapacity.from_mapping(values)


def _work(**values: float) -> capacity_planning.AcceleratorWork:
    return capacity_planning.AcceleratorWork.from_mapping(values)


def _demand_planner_envelope(
    *,
    source_fingerprint: str = 'f' * 64,
) -> tuple[dict[str, object], capacity_planning.CapacityPlanningSnapshot,
           capacity_planning.CapacityPlanCandidate]:
    reservation = capacity_planning.ReservationPlanningInput(
        gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
        evidence_state=(
            capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED),
        authenticated_capacity=_capacity(L4=2),
        eligible_capacity=_capacity(L4=2),
        pending_zero_cost_capacity=_capacity(L4=1),
        existing_zero_cost_capacity=_capacity(L4=0),
        existing_paid_capacity=_capacity(L4=0),
        charged_paid_gpu_units=0,
        evidence_fingerprint='e' * 64,
        allocation_demand_witness_sha256='0' * 64,
        allocation_demonstrated_need=5,
        allocation_ceiling=5)
    demand = capacity_planning.CompatibilityDemand(
        sequence=0, priority=20, compatible_accelerators=('L4',), work=5)
    snapshot = capacity_planning.CapacityPlanningSnapshot(
        source_generation=11,
        service_version=7,
        configured_accelerators=('L4',),
        capacity_unit=capacity_planning.CapacityUnit.LOGICAL_GPU,
        physical_gpu_width_by_accelerator=_capacity(L4=1),
        capacity_per_accelerator=_work(L4=1),
        floors=_capacity(),
        minimum_capacity=0,
        paid_minimum_capacity=0,
        actuation_minimum_capacity=0,
        maximum_capacity=20,
        demand_profiles=(demand,),
        explicit_demand_profiles=(demand,),
        paid_demand_profiles=(demand,),
        fixed_work=_work(),
        explicit_fixed_work=_work(),
        paid_fixed_work=_work(),
        retention_work=_work(),
        ready_zero_cost=_capacity(),
        ready=_capacity(),
        provisioning=_capacity(),
        reservation=reservation,
        cold_accelerator_order=('L4',),
        prospective_paid_accelerator_order=('L4',),
        planning_purpose=(
            capacity_planning.CapacityPlanningPurpose.ECONOMIC_ADMISSION),
        actuation_supply_policy=(
            capacity_planning.ActuationSupplyPolicy.REUSE_CURRENT_SUPPLY),
        attribution_complete=True,
        planning_time=1000.0,
        max_live_paid_gpu_units=None,
        retirement_shelter_target=_capacity(),
        source_fingerprint=source_fingerprint,
        configured_reservation_accelerators=('L4',),
        demand_witness_scope_sha256='a' * 64)
    acquisition = capacity_planning.plan_capacity(snapshot)
    assert acquisition.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    assert acquisition.demand_witness_sha256 is not None
    snapshot = dataclasses.replace(snapshot,
                                   reservation=dataclasses.replace(
                                       reservation,
                                       allocation_demand_witness_sha256=(
                                           acquisition.demand_witness_sha256)))
    candidate = capacity_planning.plan_capacity(snapshot)
    return (capacity_planning.planner_envelope(snapshot,
                                               candidate), snapshot, candidate)


def _validate_prospective_planner(
    envelope: dict[str, object],
    *,
    demand_feed_generation: int = 11,
    paid_residual: int = 2,
) -> tuple[capacity_planning.CapacityPlanningSnapshot,
           capacity_planning.CapacityPlanCandidate]:
    return capacity_admission._validate_prospective_planner_candidate(
        {'planner': envelope},
        service_version=7,
        demand_feed_generation=demand_feed_generation,
        accounting_cards={'l4'},
        capacity_target={'l4': 5},
        existing_zero_cost={'l4': 0},
        pending_zero_cost={'l4': 1},
        allocation_reserved={'l4': 2},
        existing_paid={'l4': 0},
        paid_residual={'l4': paid_residual},
        paid_launch_target={'l4': paid_residual})


def _planner_decision(
    *,
    source_fingerprint: str = 'f' * 64,
) -> capacity_admission.CapacityPlanDecision:
    envelope, _, candidate = _demand_planner_envelope(
        source_fingerprint=source_fingerprint)
    return capacity_admission.CapacityPlanDecision(
        capacity_target_by_accelerator={'L4': 5},
        normalized_demand_extensions={
            'autoscaler_target': 5,
            'demand_target_by_accelerator': {
                'L4': 5,
            },
            'replica_unit': 'logical',
        },
        reserved_capacity_commitment_by_accelerator={'L4': 2},
        expected_paid_residual_by_accelerator={'L4': 2},
        expected_paid_launch_target_by_accelerator={'L4': 2},
        static_reserved_fill_target_by_accelerator={},
        paid_launch_priority_by_accelerator={'l4': 50},
        planner_payload=envelope)


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


def test_capacity_plan_persists_typed_planner_residual_without_recomputing():
    payload = _input().payload(
        existing_zero_cost_capacity_by_accelerator={'l4': 2},
        existing_paid_capacity_by_accelerator={'L4': 1})

    assert payload['service'] == {
        'name': 'svc',
        'hash': 'service-hash',
        'lifecycle_epoch': 3,
        'version': 7,
    }
    assert payload['existing_zero_cost_capacity_by_accelerator'] == {'l4': 2}
    assert payload['existing_paid_capacity_by_accelerator'] == {'l4': 1}
    assert payload['paid_residual_by_accelerator'] == {'l4': 2}

    with pytest.raises(ValueError, match='not typed accelerator capacity'):
        dataclasses.replace(
            _input(),
            paid_residual={  # type: ignore[arg-type]
                'L4': 3
            }).payload(existing_zero_cost_capacity_by_accelerator={'L4': 2},
                       existing_paid_capacity_by_accelerator={'L4': 1})


def test_capacity_plan_persists_allocation_tail_and_typed_paid_residual():
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
                  },
                  paid_residual=_capacity(L4=1))

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
                           })

    assert payload['allocation_reserved_capacity_by_accelerator'] == {
        'h200': 1,
        'l4': 0,
    }
    assert payload['pending_zero_cost_capacity_by_accelerator'] == {
        'h200': 1,
        'l4': 0,
    }
    assert payload['paid_residual_by_accelerator'] == {'l4': 1}


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


def test_static_incompatibility_authority_round_trips_exact_cards():
    authority = (
        capacity_admission.ReservedFillPlanAuthority.statically_incompatible(
            ('L4',), '4' * 64))

    encoded = authority.to_mapping()

    assert encoded == {
        'mode': 'STATICALLY_INCOMPATIBLE',
        'incompatible_accelerators': ['l4'],
        'worker_projection_sha256': '4' * 64,
    }
    assert (capacity_admission.ReservedFillPlanAuthority.from_mapping(encoded)
            == authority)
    with pytest.raises(ValueError, match='not canonical and complete'):
        capacity_admission.ReservedFillPlanAuthority.statically_incompatible(
            ('*',), '4' * 64)


def test_reservation_ineligible_authority_is_not_gate_specific():
    authority = (
        capacity_admission.ReservedFillPlanAuthority.reservation_ineligible(
            '6' * 64))

    assert authority.to_mapping() == {
        'mode': 'GATE_INELIGIBLE',
        'reservation_evidence_sha256': '6' * 64,
    }
    assert (capacity_admission.ReservedFillPlanAuthority.from_mapping(
        authority.to_mapping()) == authority)
    assert (capacity_admission.ReservedFillPlanAuthority.gate_ineligible(
        '6' * 64) == authority)


def test_static_incompatibility_authority_must_match_positive_target():
    authority = (
        capacity_admission.ReservedFillPlanAuthority.statically_incompatible(
            ('L4',), '4' * 64))
    plan = _input(capacity_target_by_accelerator={'H200': 1},
                  reserved_fill_authority=authority,
                  paid_residual=_capacity(H200=1))

    with pytest.raises(ValueError, match='positive target cards'):
        plan.payload(existing_zero_cost_capacity_by_accelerator={'H200': 0},
                     existing_paid_capacity_by_accelerator={'H200': 0})


def test_zero_revocation_is_explicit_unbound_and_all_zero():
    authority = capacity_admission.ReservedFillPlanAuthority.zero_revocation()
    zero_input = _input(capacity_target_by_accelerator={'L4': 0},
                        reserved_fill_authority=authority,
                        paid_residual=_capacity())

    payload = zero_input.payload(
        existing_zero_cost_capacity_by_accelerator={'L4': 0},
        existing_paid_capacity_by_accelerator={'L4': 0})

    assert payload['reserved_fill_authority'] == {
        'mode': 'UNBOUND_ZERO_REVOCATION'
    }
    with pytest.raises(ValueError, match='all-zero'):
        _input(reserved_fill_authority=authority).payload(
            existing_zero_cost_capacity_by_accelerator={'L4': 0},
            existing_paid_capacity_by_accelerator={'L4': 0})


def test_capacity_plan_uses_supply_aware_target_not_cold_demand_card():
    payload = _input(capacity_target_by_accelerator={
        'L4': 0,
        'A100': 5,
    },
                     normalized_demand={
                         'demand_target_by_accelerator': {
                             'L4': 5,
                         }
                     },
                     paid_residual=_capacity(A100=1)).payload(
                         existing_zero_cost_capacity_by_accelerator={
                             'L4': 0,
                             'A100': 4,
                         },
                         existing_paid_capacity_by_accelerator={
                             'L4': 0,
                             'A100': 0,
                         })

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
        },
               paid_residual=capacity_planning.AcceleratorCapacity.from_mapping(
                   {
                       '*': 5,
                       'L4': 1,
                   })).payload(existing_zero_cost_capacity_by_accelerator={
                       '*': 0,
                       'L4': 0,
                   },
                               existing_paid_capacity_by_accelerator={
                                   '*': 0,
                                   'L4': 0,
                               })


def test_capacity_plan_accepts_only_strict_planner_envelope():
    envelope, _, _ = _demand_planner_envelope()
    plan = _input(planner_payload=envelope,
                  allocation_reserved_capacity_by_accelerator={'L4': 2})

    payload = plan.payload(
        existing_zero_cost_capacity_by_accelerator={'L4': 0},
        pending_zero_cost_capacity_by_accelerator={'L4': 1},
        allocation_reserved_capacity_by_accelerator={'L4': 2},
        existing_paid_capacity_by_accelerator={'L4': 0})

    assert payload['planner'] == envelope
    malformed = copy.deepcopy(envelope)
    malformed['candidate']['paid_residual']['entries'][0][1] = 3
    with pytest.raises(ValueError, match='planner payload is malformed'):
        _input(planner_payload=malformed,
               allocation_reserved_capacity_by_accelerator={
                   'L4': 2
               }).payload(existing_zero_cost_capacity_by_accelerator={'L4': 0},
                          pending_zero_cost_capacity_by_accelerator={'L4': 1},
                          allocation_reserved_capacity_by_accelerator={'L4': 2},
                          existing_paid_capacity_by_accelerator={'L4': 0})

    with pytest.raises(ValueError, match='immutable planner candidate'):
        dataclasses.replace(plan, paid_residual=_capacity(L4=1)).payload(
            existing_zero_cost_capacity_by_accelerator={'L4': 0},
            pending_zero_cost_capacity_by_accelerator={'L4': 1},
            allocation_reserved_capacity_by_accelerator={'L4': 2},
            existing_paid_capacity_by_accelerator={'L4': 0})


def test_prospective_paid_claim_authenticates_exact_planner_candidate():
    envelope, expected_snapshot, expected_candidate = (
        _demand_planner_envelope())

    snapshot, candidate = _validate_prospective_planner(envelope)

    assert snapshot == expected_snapshot
    assert candidate == expected_candidate


@pytest.mark.parametrize('mutation, message', [
    ('absent', 'no valid immutable planner candidate'),
    ('fingerprint', 'no valid immutable planner candidate'),
    ('generation', 'generation or fingerprint is stale'),
    ('source-fingerprint', 'generation or fingerprint is stale'),
    ('incomplete', 'attribution is incomplete'),
    ('paid-residual', 'accounting differs'),
])
def test_prospective_paid_claim_fails_closed_without_exact_candidate(
        mutation, message):
    envelope, snapshot, _ = _demand_planner_envelope()
    demand_generation = 11
    paid_residual = 2
    if mutation == 'absent':
        envelope = {}
    elif mutation == 'fingerprint':
        envelope = copy.deepcopy(envelope)
        envelope['candidate']['snapshot_fingerprint'] = '0' * 64
    elif mutation == 'generation':
        demand_generation = 12
    elif mutation == 'source-fingerprint':
        snapshot = dataclasses.replace(snapshot, source_fingerprint='')
        envelope = capacity_planning.planner_envelope(
            snapshot, capacity_planning.plan_capacity(snapshot))
    elif mutation == 'incomplete':
        snapshot = dataclasses.replace(snapshot, attribution_complete=False)
        candidate = capacity_planning.incomplete_capacity_plan(
            source_generation=snapshot.source_generation)
        candidate = dataclasses.replace(
            candidate, snapshot_fingerprint=snapshot.fingerprint)
        envelope = capacity_planning.planner_envelope(snapshot, candidate)
    elif mutation == 'paid-residual':
        paid_residual = 3
    else:
        raise AssertionError(f'Unhandled mutation {mutation!r}.')

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match=message):
        _validate_prospective_planner(envelope,
                                      demand_feed_generation=demand_generation,
                                      paid_residual=paid_residual)


@pytest.mark.parametrize(
    'gate_policy, admission_policy',
    ((capacity_planning.ReservationGatePolicy.DEMAND_GATED,
      capacity_admission.ReservedSupplyPolicy.DEMAND_GATED),
     (capacity_planning.ReservationGatePolicy.UNGATED,
      capacity_admission.ReservedSupplyPolicy.STATIC_PREFILL)))
def test_prospective_paid_candidate_accepts_reservations_with_or_without_gate(
        gate_policy, admission_policy):
    _, snapshot, _ = _demand_planner_envelope()
    reservation = dataclasses.replace(
        snapshot.reservation,
        gate_policy=gate_policy,
        allocation_demand_witness_sha256=(
            snapshot.reservation.allocation_demand_witness_sha256 if gate_policy
            is capacity_planning.ReservationGatePolicy.DEMAND_GATED else None),
        allocation_demonstrated_need=(
            snapshot.reservation.allocation_demonstrated_need if gate_policy
            is capacity_planning.ReservationGatePolicy.DEMAND_GATED else None),
        allocation_ceiling=(
            snapshot.reservation.allocation_ceiling if gate_policy
            is capacity_planning.ReservationGatePolicy.DEMAND_GATED else 0))
    snapshot = dataclasses.replace(
        snapshot,
        reservation=reservation,
        demand_witness_scope_sha256=(
            snapshot.demand_witness_scope_sha256 if gate_policy
            is capacity_planning.ReservationGatePolicy.DEMAND_GATED else ''))
    candidate = capacity_planning.plan_capacity(snapshot)
    envelope = capacity_planning.planner_envelope(snapshot, candidate)
    planner_snapshot, planner_candidate = _validate_prospective_planner(
        envelope)

    capacity_admission._validate_prospective_reservation_evidence(
        planner_snapshot,
        planner_candidate,
        accounting_cards={'l4'},
        policy=admission_policy,
        evidence_state=(capacity_admission.ReservedSupplyEvidenceState.
                        AUTHENTICATED_SETTLED),
        authenticated_capacity={'l4': 2},
        eligible_capacity={'l4': 2},
        reservation_evidence_sha256='e' * 64)


def test_prospective_paid_candidate_rejects_changed_usage_gate_evidence():
    envelope, _, _ = _demand_planner_envelope()
    planner_snapshot, planner_candidate = _validate_prospective_planner(
        envelope)

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='usage-gate evidence changed'):
        capacity_admission._validate_prospective_reservation_evidence(
            planner_snapshot,
            planner_candidate,
            accounting_cards={'l4'},
            policy=capacity_admission.ReservedSupplyPolicy.DEMAND_GATED,
            evidence_state=(capacity_admission.ReservedSupplyEvidenceState.
                            AUTHENTICATED_UNSETTLED),
            authenticated_capacity={'l4': 2},
            eligible_capacity={},
            reservation_evidence_sha256='e' * 64)


@pytest.mark.parametrize('field, replacement, message', [
    ('capacity_target_by_accelerator', {
        'L4': 4
    }, 'traffic target'),
    ('reserved_capacity_commitment_by_accelerator', {
        'L4': 1
    }, 'reservation commitment'),
    ('expected_paid_residual_by_accelerator', {
        'L4': 3
    }, 'paid residual'),
    ('expected_paid_launch_target_by_accelerator', {
        'L4': 3
    }, 'paid launch target'),
    ('static_reserved_fill_target_by_accelerator', {
        'L4': 1
    }, 'static prefill'),
])
def test_capacity_decision_cannot_diverge_from_planner_envelope(
        field, replacement, message):
    decision = _planner_decision()
    values = {
        'capacity_target_by_accelerator':
            decision.capacity_target_by_accelerator,
        'normalized_demand_extensions': decision.normalized_demand_extensions,
        'reserved_capacity_commitment_by_accelerator':
            decision.reserved_capacity_commitment_by_accelerator,
        'expected_paid_residual_by_accelerator':
            decision.expected_paid_residual_by_accelerator,
        'expected_paid_launch_target_by_accelerator':
            decision.expected_paid_launch_target_by_accelerator,
        'static_reserved_fill_target_by_accelerator':
            decision.static_reserved_fill_target_by_accelerator,
        'paid_launch_priority_by_accelerator':
            decision.paid_launch_priority_by_accelerator,
        'planner_payload': decision.planner_payload,
    }
    values[field] = replacement
    divergent = capacity_admission.CapacityPlanDecision(**values)

    with pytest.raises(ValueError, match=message):
        divergent.canonical_target({'l4'})


def test_planner_reservation_evidence_must_match_locked_supply():
    _, witness_snapshot, _ = _demand_planner_envelope()
    supply = capacity_admission.ReservedSupplyProjection(
        pending_zero_cost_capacity_by_accelerator={'L4': 1},
        allocation_reserved_capacity_by_accelerator={'L4': 2},
        economic_replica_infos=(),
        economic_kueue_capacity=(
            kueue_lane_capacity.KueueReplicaCapacitySnapshot({})),
        economic_capacity_graph_sha256='d' * 64,
        existing_zero_cost_capacity_by_accelerator={'L4': 0},
        existing_paid_capacity_by_accelerator={'L4': 0},
        authenticated_capacity_by_accelerator={'L4': 2},
        eligible_capacity_by_accelerator={'L4': 2},
        policy=capacity_admission.ReservedSupplyPolicy.DEMAND_GATED,
        evidence_state=(capacity_admission.ReservedSupplyEvidenceState.
                        AUTHENTICATED_SETTLED),
        fill_policy_sha256='a' * 64,
        reservation_evidence_sha256='e' * 64,
        demand_witness_scope_sha256='a' * 64,
        allocation_demand_witness_sha256=(
            witness_snapshot.reservation.allocation_demand_witness_sha256),
        allocation_demonstrated_need=5,
        allocation_ceiling=5,
        reserved_accelerators=('l4',),
        allocation_map=object(),
        allocation_bound=True)
    source_fingerprint = (capacity_admission.locked_planning_source_fingerprint(
        'f' * 64, supply.economic_capacity_graph_sha256))
    decision = _planner_decision(source_fingerprint=source_fingerprint)
    snapshot, candidate = decision.decode_planner()

    capacity_admission._validate_planner_against_locked_supply(
        planner_snapshot=snapshot,
        candidate=candidate,
        service_version=7,
        accounting_cards={'l4'},
        capacity_target={'l4': 5},
        reservation_commitment={'l4': 2},
        static_fill_target={'l4': 0},
        supply_projection=supply,
        expected_planning_state_fingerprint='f' * 64)
    changed = capacity_admission.ReservedSupplyProjection(**{
        **supply.__dict__,
        'reservation_evidence_sha256': '9' * 64,
    })
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='policy or evidence'):
        capacity_admission._validate_planner_against_locked_supply(
            planner_snapshot=snapshot,
            candidate=candidate,
            service_version=7,
            accounting_cards={'l4'},
            capacity_target={'l4': 5},
            reservation_commitment={'l4': 2},
            static_fill_target={'l4': 0},
            supply_projection=changed,
            expected_planning_state_fingerprint='f' * 64)


@pytest.mark.parametrize(
    'allocation, evidence_state',
    ((None, capacity_admission.ReservedSupplyEvidenceState.UNAVAILABLE),
     (_gate_allocation(armed=True),
      capacity_admission.ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED),
     (_gate_allocation(armed=False),
      capacity_admission.ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED),
     (_gate_allocation(armed=True, demonstrated_need=2, settled=False),
      capacity_admission.ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED)))
def test_configured_usage_gate_requires_current_armed_evidence(
        allocation, evidence_state):
    policy, evidence, authenticated, eligible = (
        capacity_admission._reserved_supply_policy_and_evidence(
            _fill_config(utilization_gate=True),
            allocation, {'l4': 2},
            existing_zero_cost={},
            pending_zero_cost={}))

    assert policy is capacity_admission.ReservedSupplyPolicy.DEMAND_GATED
    assert evidence is evidence_state
    assert eligible == {}
    assert authenticated == ({} if allocation is None else {'l4': 2})


def test_ungated_observer_blackout_grants_no_fill_supply():
    policy, evidence, authenticated, eligible = (
        capacity_admission._reserved_supply_policy_and_evidence(
            _fill_config(utilization_gate=False),
            None, {'l4': 2},
            existing_zero_cost={},
            pending_zero_cost={}))

    assert policy is capacity_admission.ReservedSupplyPolicy.STATIC_PREFILL
    assert evidence is capacity_admission.ReservedSupplyEvidenceState.UNAVAILABLE
    assert authenticated == {}
    assert eligible == {}


def test_ungated_unsettled_grant_grants_no_fill_supply():
    policy, evidence, authenticated, eligible = (
        capacity_admission._reserved_supply_policy_and_evidence(
            _fill_config(utilization_gate=False),
            _gate_allocation(armed=False, settled=False), {'l4': 2},
            existing_zero_cost={},
            pending_zero_cost={}))

    assert policy is capacity_admission.ReservedSupplyPolicy.STATIC_PREFILL
    assert evidence is (
        capacity_admission.ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED)
    assert authenticated == {'l4': 2}
    assert eligible == {}


def test_disabled_reservation_projection_keeps_committed_inventory():
    config = _fill_config(binding_required=False, utilization_gate=True)
    policy, evidence, authenticated, eligible = (
        capacity_admission._reserved_supply_policy_and_evidence(
            config,
            None, {'l4': 0},
            existing_zero_cost={},
            pending_zero_cost={}))
    projection = capacity_admission.ReservedSupplyProjection(
        pending_zero_cost_capacity_by_accelerator={'l4': 3},
        allocation_reserved_capacity_by_accelerator={},
        economic_replica_infos=(),
        economic_kueue_capacity=(
            kueue_lane_capacity.KueueReplicaCapacitySnapshot({})),
        economic_capacity_graph_sha256='6' * 64,
        existing_zero_cost_capacity_by_accelerator={'l4': 1},
        existing_paid_capacity_by_accelerator={'l4': 2},
        charged_paid_gpu_units=2,
        authenticated_capacity_by_accelerator=authenticated,
        eligible_capacity_by_accelerator=eligible,
        policy=policy,
        evidence_state=evidence,
        fill_policy_sha256=config.fill_policy_sha256,
        reservation_evidence_sha256=(
            capacity_admission._reservation_evidence_sha256(config, None)),
        allocation_map=None,
        allocation_bound=False)

    assert projection.reservation_evidence_sha256 == ''
    assert projection.pending_zero_cost_capacity_by_accelerator == {'l4': 3}
    assert projection.existing_zero_cost_capacity_by_accelerator == {'l4': 1}
    assert projection.existing_paid_capacity_by_accelerator == {'l4': 2}
    assert projection.additional_capacity_by_accelerator() == {'l4': 3}
    with pytest.raises(ValueError, match='Disabled reserved supply'):
        capacity_admission.ReservedSupplyProjection(
            **{
                **projection.__dict__,
                'allocation_reserved_capacity_by_accelerator': {
                    'l4': 1,
                },
            })


def test_persisted_plan_is_redecoded_before_authority_is_returned():
    envelope, snapshot, candidate = _demand_planner_envelope()
    plan = _input(planner_payload=envelope,
                  allocation_reserved_capacity_by_accelerator={'L4': 2})
    payload = plan.payload(
        existing_zero_cost_capacity_by_accelerator={'L4': 0},
        pending_zero_cost_capacity_by_accelerator={'L4': 1},
        allocation_reserved_capacity_by_accelerator={'L4': 2},
        existing_paid_capacity_by_accelerator={'L4': 0})
    row = {
        'service_name': 'svc',
        'service_hash': 'service-hash',
        'generation': 4,
        'content_sha256':
            capacity_admission.capacity_plan_content_sha256(payload),
        'demand_feed_generation': 11,
        'demand_source_epoch': 2,
        'payload': payload,
    }

    authority = capacity_admission._validate_committed_plan_row(
        row,
        expected_snapshot=snapshot,
        expected_candidate=candidate,
        accounting_cards={'l4'},
        demand_feed_generation=11)

    assert authority.generation == 4
    assert authority.economic_residual() == {'l4': 2}
    assert authority.remaining_launch_capacity() == {'l4': 2}
    assert authority.capacity_unit is candidate.capacity_unit
    assert dict(authority.physical_gpu_width_by_accelerator) == (
        candidate.physical_gpu_width_by_accelerator.as_dict())
    corrupt = copy.deepcopy(row)
    corrupt['payload']['planner']['candidate']['source_generation'] = 10
    corrupt['content_sha256'] = (
        capacity_admission.capacity_plan_content_sha256(corrupt['payload']))
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='planner envelope is invalid'):
        capacity_admission._validate_committed_plan_row(
            corrupt,
            expected_snapshot=snapshot,
            expected_candidate=candidate,
            accounting_cards={'l4'},
            demand_feed_generation=11)


def test_paid_launch_authority_debits_exact_or_aggregate_units():
    exact = capacity_admission.PaidLaunchAuthority(
        service_name='svc',
        service_hash='hash',
        generation=3,
        content_sha256='c' * 64,
        demand_feed_generation=9,
        demand_source_epoch=2,
        paid_residual_by_accelerator=(('l4', 4),),
        paid_launch_target_by_accelerator=(('l4', 4),),
        capacity_unit=capacity_planning.CapacityUnit.LOGICAL_GPU,
        physical_gpu_width_by_accelerator=(('l4', 4),),
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
        paid_launch_target_by_accelerator=(('*', 2),),
        capacity_unit=capacity_planning.CapacityUnit.PHYSICAL_BACKEND,
        physical_gpu_width_by_accelerator=(('*', 1),),
        reserved_fill_authority=(
            capacity_admission.ReservedFillPlanAuthority.not_applicable()))
    assert aggregate.claim_values('A100',
                                  units=1)['capacity_plan_accelerator'] == '*'
