"""Tests for the canonical side-effect-free Serve capacity planner."""

# pylint: disable=protected-access

import copy
import dataclasses
import inspect
import itertools
import json

import pytest

from sky.serve import autoscaler_compatibility
from sky.serve import autoscalers
from sky.serve import capacity_planning
from sky.serve import compatibility_matching

_AUTO_GATE_WITNESS = '0' * 64


def _capacity(**values: int) -> capacity_planning.AcceleratorCapacity:
    return capacity_planning.AcceleratorCapacity.from_mapping(values)


def _work(**values: float) -> capacity_planning.AcceleratorWork:
    return capacity_planning.AcceleratorWork.from_mapping(values)


def _demand(priority: int,
            cards: tuple[str, ...],
            work: float,
            sequence: int = 0) -> capacity_planning.CompatibilityDemand:
    return capacity_planning.CompatibilityDemand(sequence=sequence,
                                                 priority=priority,
                                                 compatible_accelerators=cards,
                                                 work=work)


def _profile_work(
    result: capacity_planning.DemandObservationReconciliation,
) -> dict[tuple[int, tuple[str, ...]], float]:
    return {
        (item.priority, item.compatible_accelerators): item.work
        for item in result.reconciled_profiles
    }


def test_demand_observation_union_keeps_different_typed_classes_distinct(
) -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(_demand(50, ('L4', 'A100'), 1),),
        fixed_work=_work(),
        arrival_profiles=(_demand(50, ('L4',), 2),))

    assert result.incremental_arrival_work == pytest.approx(2)
    assert result.withheld_arrival_work == pytest.approx(0)
    assert result.ambiguous_fixed_arrival_work == pytest.approx(0)
    assert result.ambiguous_fixed_shelter_accelerators == ()
    assert _profile_work(result) == {
        (50, ('A100', 'L4')): 1,
        (50, ('L4',)): 2,
    }


def test_demand_observation_union_emits_identical_class_excess() -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(_demand(50, ('L4',), 1),),
        fixed_work=_work(),
        arrival_profiles=(_demand(50, ('L4',), 2),))

    assert result.incremental_arrival_work == pytest.approx(1)
    assert result.withheld_arrival_work == pytest.approx(1)
    assert result.ambiguous_fixed_arrival_work == pytest.approx(0)
    assert _profile_work(result) == {(50, ('L4',)): 2}


def test_demand_observation_union_keeps_incompatible_and_priority_distinct(
) -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(_demand(50, ('A100',), 1),),
        fixed_work=_work(),
        arrival_profiles=(
            _demand(20, ('A100',), 1),
            _demand(50, ('L4',), 1, sequence=1),
        ))

    assert result.incremental_arrival_work == pytest.approx(2)
    assert result.withheld_arrival_work == pytest.approx(0)
    assert _profile_work(result) == {
        (50, ('A100',)): 1,
        (50, ('L4',)): 1,
        (20, ('A100',)): 1,
    }


def test_demand_observation_reconciliation_withholds_lossy_fixed_priority(
) -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(),
        fixed_work=_work(L4=0.75),
        arrival_profiles=(
            _demand(20, ('L4',), 0.5),
            _demand(50, ('L4',), 0.5, sequence=1),
        ))

    # Fixed work has no request priority. Choosing either arrival class as its
    # overlap would invent priority, so neither can authorize a provider
    # launch. Current queue/rejection evidence will supply that authority.
    assert result.incremental_arrival_work == pytest.approx(0)
    assert result.withheld_arrival_work == pytest.approx(1)
    assert result.ambiguous_fixed_arrival_work == pytest.approx(1)
    assert result.ambiguous_fixed_shelter_accelerators == ('L4',)
    assert _profile_work(result) == {}


def test_demand_observation_reconciliation_withholds_fixed_intersection(
) -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(),
        fixed_work=_work(L4=1),
        arrival_profiles=(_demand(50, ('L4',), 2),))

    assert result.incremental_arrival_work == pytest.approx(0)
    assert result.withheld_arrival_work == pytest.approx(2)
    assert result.ambiguous_fixed_arrival_work == pytest.approx(2)
    assert result.ambiguous_fixed_shelter_accelerators == ('L4',)
    assert _profile_work(result) == {}


def test_demand_observation_reconciliation_does_not_assign_fixed_priority(
) -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(),
        fixed_work=_work(L4=1),
        arrival_profiles=(
            _demand(20, ('L4',), 1),
            _demand(50, ('L4',), 1, sequence=1),
        ))

    assert result.incremental_arrival_work == pytest.approx(0)
    assert result.withheld_arrival_work == pytest.approx(2)
    assert result.ambiguous_fixed_arrival_work == pytest.approx(2)
    assert result.ambiguous_fixed_shelter_accelerators == ('L4',)
    assert _profile_work(result) == {}


def test_demand_observation_reconciliation_keeps_immutable_classes_distinct(
) -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(
            _demand(50, ('A100', 'L4'), 0.5),
            _demand(50, ('L4', 'H200'), 0.5, sequence=1),
        ),
        fixed_work=_work(),
        arrival_profiles=(
            _demand(50, ('A100', 'H200'), 0.5),
            _demand(50, ('A100', 'H100'), 0.5, sequence=1),
        ))

    assert result.incremental_arrival_work == pytest.approx(1)
    assert result.withheld_arrival_work == pytest.approx(0)
    assert result.ambiguous_fixed_shelter_accelerators == ()
    assert result.ambiguous_fixed_arrival_work == pytest.approx(0)
    assert _profile_work(result) == {
        (50, ('A100', 'L4')): 0.5,
        (50, ('A100', 'H100')): 0.5,
        (50, ('A100', 'H200')): 0.5,
        (50, ('H200', 'L4')): 0.5,
    }


def test_demand_observation_reconciliation_keeps_disjoint_arrival() -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(),
        fixed_work=_work(A100=1),
        arrival_profiles=(_demand(50, ('L4',), 1),))

    assert result.incremental_arrival_work == pytest.approx(1)
    assert result.withheld_arrival_work == pytest.approx(0)
    assert _profile_work(result) == {(50, ('L4',)): 1}


def test_demand_observation_reconciliation_shelters_only_ambiguous_cards(
) -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(),
        fixed_work=_work(L4=1),
        arrival_profiles=(
            _demand(50, ('L4',), 1),
            _demand(50, ('A100',), 1, sequence=1),
        ))

    assert result.incremental_arrival_work == pytest.approx(1)
    assert result.ambiguous_fixed_arrival_work == pytest.approx(1)
    assert result.ambiguous_fixed_shelter_accelerators == ('L4',)
    assert _profile_work(result) == {(50, ('A100',)): 1}


def test_demand_observation_reconciliation_does_not_guess_flexible_identity(
) -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(),
        fixed_work=_work(A100=1),
        arrival_profiles=(
            _demand(50, ('A100', 'L4'), 1),
            _demand(50, ('A100', 'H200'), 1, sequence=1),
        ))

    # Choosing either original class would invent identity and paid economics.
    # Both remain provider-ineligible until queue/rejection evidence arrives.
    assert result.incremental_arrival_work == pytest.approx(0)
    assert result.withheld_arrival_work == pytest.approx(2)
    assert result.ambiguous_fixed_arrival_work == pytest.approx(2)
    assert result.ambiguous_fixed_shelter_accelerators == (
        'A100',
        'H200',
        'L4',
    )
    assert _profile_work(result) == {}


def test_demand_observation_reconciliation_withholds_tail_without_safe_card(
) -> None:
    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=(),
        fixed_work=_work(A=1, B=1),
        arrival_profiles=(
            _demand(50, ('A', 'C'), 1),
            _demand(50, ('B', 'D'), 1, sequence=1),
            _demand(50, ('A', 'B'), 1, sequence=2),
        ))

    # The component has one guaranteed new request, but no accelerator common
    # to every possible owning class. It may shelter compatible existing cards
    # but cannot authorize a provider choice.
    assert result.incremental_arrival_work == pytest.approx(0)
    assert result.withheld_arrival_work == pytest.approx(3)
    assert result.ambiguous_fixed_arrival_work == pytest.approx(3)
    assert result.ambiguous_fixed_shelter_accelerators == ('A', 'B', 'C', 'D')
    assert _profile_work(result) == {}


def test_demand_observation_union_is_aggregated_and_order_invariant() -> None:
    primary = (
        _demand(50, ('L4',), 500_000, sequence=4),
        _demand(50, ('L4',), 500_000, sequence=3),
    )
    arrivals = (
        _demand(50, ('L4',), 250_000, sequence=9),
        _demand(50, ('L4',), 750_000, sequence=8),
    )

    first = capacity_planning.reconcile_demand_observations(
        primary_profiles=primary, fixed_work=_work(), arrival_profiles=arrivals)
    second = capacity_planning.reconcile_demand_observations(
        primary_profiles=tuple(reversed(primary)),
        fixed_work=_work(),
        arrival_profiles=tuple(reversed(arrivals)))

    assert first == second
    assert first.incremental_arrival_work == pytest.approx(0)
    assert first.withheld_arrival_work == pytest.approx(1_000_000)
    assert len(first.reconciled_profiles) == 1


def test_demand_observation_union_has_canonical_float_aggregation() -> None:
    primary = (
        _demand(50, ('L4',), 1e16, sequence=0),
        _demand(50, ('L4',), 1, sequence=1),
        _demand(50, ('L4',), 1, sequence=2),
    )

    first = capacity_planning.reconcile_demand_observations(
        primary_profiles=primary, fixed_work=_work(), arrival_profiles=())
    second = capacity_planning.reconcile_demand_observations(
        primary_profiles=tuple(reversed(primary)),
        fixed_work=_work(),
        arrival_profiles=())

    assert first == second
    assert first.reconciled_profiles[0].work == 1.0000000000000002e16


def test_demand_observation_union_scales_with_profiles_not_pair_edges() -> None:
    cards = tuple(f'GPU-{index}' for index in range(8))
    subsets = tuple(
        tuple(card
              for index, card in enumerate(cards)
              if mask & (1 << index))
        for mask in range(1, 1 << len(cards)))
    primary = tuple(
        _demand(priority, subset, 1, sequence=sequence)
        for sequence, (priority, subset) in enumerate(
            (priority, subset) for priority in range(4) for subset in subsets))
    arrivals = tuple(
        _demand(priority, subset, 1, sequence=sequence)
        for sequence, (priority, subset) in enumerate(
            (priority, subset) for priority in range(4) for subset in subsets))

    result = capacity_planning.reconcile_demand_observations(
        primary_profiles=primary, fixed_work=_work(), arrival_profiles=arrivals)

    assert result.incremental_arrival_work == pytest.approx(0)
    assert result.withheld_arrival_work == pytest.approx(len(primary))
    assert len(result.reconciled_profiles) == len(primary)


def test_demand_observation_union_enforces_bounded_card_catalog() -> None:
    cards = tuple(f'GPU-{index}' for index in range(9))

    with pytest.raises(ValueError, match='eight-card'):
        capacity_planning.reconcile_demand_observations(
            primary_profiles=(_demand(50, cards, 1),),
            fixed_work=_work(),
            arrival_profiles=())


def _acquisition(
    priority: int,
    cards: tuple[str, ...],
    count: int,
) -> compatibility_matching.CompatibilityDemand:
    return compatibility_matching.CompatibilityDemand(
        priority=priority,
        compatible_cards=tuple(card.casefold() for card in cards),
        count=count)


def _deadline(*,
              remaining_seconds: float,
              priority: int = 50,
              cards: tuple[str, ...] = ('A100',),
              count: int = 1) -> capacity_planning.DeadlinePlanningInput:
    return capacity_planning.DeadlinePlanningInput(
        demand=(autoscaler_compatibility.DeadlineDemand(
            sequence=0,
            priority=priority,
            compatible_cards=cards,
            count=count,
            remaining_seconds=remaining_seconds),),
        finite_supply=(),
        service_seconds_by_accelerator=_work(L4=30, A100=30),
        service_time_sources=(('L4', 'seed'), ('A100', 'seed')),
        utilization=0.95,
        paid_cold_lead_seconds=600)


def _reservation(
    *,
    gate_policy: capacity_planning.ReservationGatePolicy = (
        capacity_planning.ReservationGatePolicy.NOT_CONFIGURED),
    evidence_state: capacity_planning.ReservationEvidenceState = (
        capacity_planning.ReservationEvidenceState.NOT_APPLICABLE),
    authenticated: capacity_planning.AcceleratorCapacity | None = None,
    eligible: capacity_planning.AcceleratorCapacity | None = None,
    pending: capacity_planning.AcceleratorCapacity | None = None,
    existing_zero_cost: capacity_planning.AcceleratorCapacity | None = None,
    existing_paid: capacity_planning.AcceleratorCapacity | None = None,
    allocation_witness: str | None = None,
    demonstrated_need: int | None = None,
    allocation_ceiling: int | None = None,
) -> capacity_planning.ReservationPlanningInput:
    applicable = gate_policy is not (
        capacity_planning.ReservationGatePolicy.NOT_CONFIGURED)
    paid = existing_paid or _capacity()
    gated_settled = (
        gate_policy is capacity_planning.ReservationGatePolicy.DEMAND_GATED and
        evidence_state
        is capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED)
    if gated_settled and allocation_witness is None:
        allocation_witness = _AUTO_GATE_WITNESS
    if gated_settled and demonstrated_need is None:
        demonstrated_need = 1_000_000
    if gated_settled and allocation_ceiling is None:
        allocation_ceiling = 1_000_000
    return capacity_planning.ReservationPlanningInput(
        gate_policy=gate_policy,
        evidence_state=evidence_state,
        authenticated_capacity=authenticated or _capacity(),
        eligible_capacity=eligible or _capacity(),
        pending_zero_cost_capacity=pending or _capacity(),
        existing_zero_cost_capacity=existing_zero_cost or _capacity(),
        existing_paid_capacity=paid,
        charged_paid_gpu_units=paid.total(),
        evidence_fingerprint=('e' * 64 if applicable else ''),
        allocation_demand_witness_sha256=allocation_witness,
        allocation_demonstrated_need=demonstrated_need,
        allocation_ceiling=allocation_ceiling or 0)


def _snapshot(
        **overrides: object) -> capacity_planning.CapacityPlanningSnapshot:
    values = {
        'source_generation': 7,
        'service_version': 3,
        'configured_accelerators': ('L4', 'A100'),
        'capacity_unit': capacity_planning.CapacityUnit.LOGICAL_GPU,
        'planning_capacity_quantum_by_accelerator': _capacity(L4=1, A100=1),
        'capacity_per_accelerator': _work(L4=1, A100=1),
        'floors': _capacity(),
        'minimum_capacity': 0,
        'paid_minimum_capacity': 0,
        'actuation_minimum_capacity': 0,
        'maximum_capacity': 20,
        'demand_profiles': (_demand(20, ('L4', 'A100'), 2),),
        'explicit_demand_profiles': (_demand(20, ('L4', 'A100'), 2),),
        'paid_demand_profiles': (_demand(20, ('L4', 'A100'), 2),),
        'fixed_work': _work(),
        'explicit_fixed_work': _work(),
        'paid_fixed_work': _work(),
        'retention_work': _work(),
        'ready_zero_cost': _capacity(),
        'ready': _capacity(),
        'provisioning': _capacity(),
        'reservation': _reservation(),
        'cold_accelerator_order': ('L4', 'A100'),
        'prospective_paid_accelerator_order': ('L4', 'A100'),
        'planning_purpose':
            capacity_planning.CapacityPlanningPurpose.LOCAL_ACTUATION,
        'actuation_supply_policy':
            (capacity_planning.ActuationSupplyPolicy.REUSE_CURRENT_SUPPLY),
        'attribution_complete': True,
        'planning_time': 1000.0,
        'max_live_paid_gpu_units': None,
        'retirement_shelter_target': _capacity(),
        'source_fingerprint': 'f' * 64,
    }
    values.update(overrides)
    reservation = values['reservation']
    assert isinstance(reservation, capacity_planning.ReservationPlanningInput)
    if ('configured_reservation_accelerators' not in overrides and
            reservation.gate_policy
            is not capacity_planning.ReservationGatePolicy.NOT_CONFIGURED):
        reservation_cards = {
            card.casefold(): card
            for capacity in (reservation.authenticated_capacity,
                             reservation.eligible_capacity,
                             reservation.pending_zero_cost_capacity)
            for card, _ in capacity.entries
        }
        if not reservation_cards:
            reservation_cards = {
                card.casefold(): card
                for card in values['configured_accelerators']
            }
        values['configured_reservation_accelerators'] = tuple(
            sorted(reservation_cards.values(), key=str.casefold))
    if ('demand_witness_scope_sha256' not in overrides and
            reservation.gate_policy
            is capacity_planning.ReservationGatePolicy.DEMAND_GATED):
        values['demand_witness_scope_sha256'] = 'a' * 64
    snapshot = capacity_planning.CapacityPlanningSnapshot(**values)
    if (reservation.gate_policy
            is capacity_planning.ReservationGatePolicy.DEMAND_GATED and
            reservation.evidence_state
            is capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED
            and
            reservation.allocation_demand_witness_sha256 == _AUTO_GATE_WITNESS):
        acquisition = capacity_planning.plan_capacity(snapshot)
        if acquisition.kind is (
                capacity_planning.CapacityPlanKind.GATE_ACQUISITION):
            assert acquisition.demand_witness_sha256 is not None
            snapshot = dataclasses.replace(
                snapshot,
                reservation=dataclasses.replace(
                    reservation,
                    allocation_demand_witness_sha256=(
                        acquisition.demand_witness_sha256)))
    return snapshot


def _policy_state(**overrides: object) -> capacity_planning.CapacityPolicyState:
    values = {
        'service_name': 'svc',
        'service_version': 3,
        'last_reduced_demand_generation': 6,
        'capacity_unit': capacity_planning.CapacityUnit.LOGICAL_GPU,
        'maximum_capacity': 20,
        'upscale_observations': 0,
        'downscale_started_db_epoch': None,
        'downscale_veto_streak': 0,
        'snap_target_on_next_recompute': False,
        'adopt_total_capacity_on_next_recompute': False,
        'pending_retention_floor': None,
        'pending_capacity_at_adoption': 0,
        'pending_budget_spent': 0,
        'paid_window_started_db_epoch': None,
        'paid_window_ceiling_by_accelerator': _capacity(),
    }
    values.update(overrides)
    return capacity_planning.CapacityPolicyState(**values)


def _policy_input(**overrides: object) -> capacity_planning.CapacityPolicyInput:
    values = {
        'planning_db_epoch': 100.0,
        'fresh_demand': True,
        'pressure_latched': False,
        'pressure_reasons': (),
        'ready_demand_owned_capacity': 0,
        'latest_committed_capacity': 0,
        'nonterminal_committed_capacity': 0,
        'provisioning_demand_owned_capacity': 0,
        'latest_committed_by_accelerator': _capacity(),
        'upscale_delay_observations': 1,
        'downscale_delay_seconds': 30.0,
        'decision_interval_seconds': 30.0,
        'max_downscale_pressure_vetoes': 2,
        'scale_up_rate_percentage': None,
        'scale_up_rate_min_capacity': 0,
        'scale_up_rate_period_seconds': None,
        'max_scale_down_rate_percentage': 100,
        'overprovision_capacity': 0,
    }
    values.update(overrides)
    return capacity_planning.CapacityPolicyInput(**values)


def _prior_candidate(
    state: capacity_planning.CapacityPolicyState,
    *,
    target: int = 0,
    card: str = 'L4',
) -> capacity_planning.CapacityPlanCandidate:
    if target == 0:
        genesis_state, candidate = capacity_planning.genesis_capacity_policy(
            service_name=state.service_name,
            service_version=state.service_version,
            last_reduced_demand_generation=(
                state.last_reduced_demand_generation),
            capacity_unit=state.capacity_unit,
            maximum_capacity=state.maximum_capacity,
            planning_capacity_quantum_by_accelerator=_capacity(L4=1, A100=1))
        assert genesis_state.service_name == state.service_name
        return dataclasses.replace(candidate, next_policy_state=state)
    demand = (() if target == 0 else (_demand(50, (card,), target),))
    candidate = capacity_planning.plan_capacity(
        _snapshot(source_generation=state.last_reduced_demand_generation,
                  maximum_capacity=state.maximum_capacity,
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand))
    return dataclasses.replace(candidate, next_policy_state=state)


def test_identical_semantic_snapshots_have_identical_plans() -> None:
    first = _snapshot(configured_accelerators=('L4', 'A100'),
                      demand_profiles=(
                          _demand(20, ('L4', 'A100'), 1, 0),
                          _demand(50, ('A100',), 1, 1),
                      ),
                      explicit_demand_profiles=(
                          _demand(20, ('L4', 'A100'), 1, 0),
                          _demand(50, ('A100',), 1, 1),
                      ),
                      paid_demand_profiles=(
                          _demand(20, ('L4', 'A100'), 1, 0),
                          _demand(50, ('A100',), 1, 1),
                      ))
    second = _snapshot(demand_profiles=(
        _demand(50, ('A100',), 1, 1),
        _demand(20, ('A100', 'L4'), 1, 0),
    ),
                       explicit_demand_profiles=(
                           _demand(50, ('A100',), 1, 1),
                           _demand(20, ('A100', 'L4'), 1, 0),
                       ),
                       paid_demand_profiles=(
                           _demand(50, ('A100',), 1, 1),
                           _demand(20, ('A100', 'L4'), 1, 0),
                       ))

    assert first.fingerprint == second.fingerprint
    assert capacity_planning.plan_capacity(first) == (
        capacity_planning.plan_capacity(second))


def test_policy_generation_advances_once_and_round_trips_with_envelope(
) -> None:
    state = _policy_state()
    snapshot = _snapshot(prior_policy_state=state,
                         prior_candidate=_prior_candidate(state),
                         policy_input=_policy_input())

    candidate = capacity_planning.plan_capacity(snapshot)

    assert snapshot.prior_policy_state is not None
    assert snapshot.prior_policy_state.last_reduced_demand_generation == 6
    assert snapshot.source_generation == 7
    assert candidate.next_policy_state is not None
    assert candidate.next_policy_state.last_reduced_demand_generation == 7
    payload = capacity_planning.planner_envelope(snapshot, candidate)
    decoded_snapshot, decoded_candidate = (
        capacity_planning.decode_planner_envelope(payload))
    assert decoded_snapshot == snapshot
    assert decoded_candidate == candidate


def test_format_six_policy_state_contains_only_reducer_memory() -> None:
    state = _policy_state()

    assert {field.name for field in dataclasses.fields(state)} == {
        'service_name', 'service_version', 'last_reduced_demand_generation',
        'capacity_unit', 'maximum_capacity', 'upscale_observations',
        'downscale_started_db_epoch', 'downscale_veto_streak',
        'snap_target_on_next_recompute',
        'adopt_total_capacity_on_next_recompute', 'pending_retention_floor',
        'pending_capacity_at_adoption', 'pending_budget_spent',
        'paid_window_started_db_epoch', 'paid_window_ceiling_by_accelerator'
    }
    payload = state.canonical_payload()
    assert not ({
        'target_capacity', 'cold_launch_authority_target', 'pressure_latched',
        'pressure_reasons', 'last_scale_up_wave_monotonic'
    } & payload.keys())


def test_genesis_policy_builds_one_valid_zero_prior_pair() -> None:
    state, candidate = capacity_planning.genesis_capacity_policy(
        service_name='svc',
        service_version=3,
        last_reduced_demand_generation=6,
        capacity_unit=capacity_planning.CapacityUnit.LOGICAL_GPU,
        maximum_capacity=20,
        planning_capacity_quantum_by_accelerator=_capacity(L4=1, A100=8))

    assert candidate.next_policy_state == state
    assert candidate.source_generation == 6
    assert candidate.aggregate_demand_target == 0
    assert candidate.wave_limited_actuation_target.total() == 0
    assert candidate.paid_launch_target.total() == 0


@pytest.mark.parametrize('configured_accelerators', [
    ('L4',),
    ('L4', 'A100', 'H200'),
    ('L4', 'l4', 'A100'),
])
def test_policy_history_reprojection_rejects_card_domain_changes(
        configured_accelerators: tuple[str, ...]) -> None:
    state, candidate = capacity_planning.genesis_capacity_policy(
        service_name='svc',
        service_version=3,
        last_reduced_demand_generation=6,
        capacity_unit=capacity_planning.CapacityUnit.LOGICAL_GPU,
        maximum_capacity=20,
        planning_capacity_quantum_by_accelerator=_capacity(l4=1, a100=8))

    with pytest.raises(ValueError, match='identity|accelerator domain'):
        capacity_planning.reproject_capacity_policy_history(
            state, candidate, configured_accelerators=configured_accelerators)


def _folded_genesis_history() -> tuple[capacity_planning.CapacityPolicyState,
                                       capacity_planning.CapacityPlanCandidate]:
    """Repository genesis in the folded PostgreSQL accounting-card domain."""
    return capacity_planning.genesis_capacity_policy(
        service_name='svc',
        service_version=3,
        last_reduced_demand_generation=6,
        capacity_unit=capacity_planning.CapacityUnit.LOGICAL_GPU,
        maximum_capacity=20,
        planning_capacity_quantum_by_accelerator=_capacity(l4=1, a100=8))


def test_policy_history_reprojection_rejects_same_size_card_swap() -> None:
    state, candidate = _folded_genesis_history()

    # Same cardinality as the history, one card replaced: only the folded
    # set comparison distinguishes this from a case-only respelling.
    with pytest.raises(ValueError, match='accelerator domain'):
        capacity_planning.reproject_capacity_policy_history(
            state, candidate, configured_accelerators=('L4', 'H200'))


def test_policy_history_reprojection_rejects_detached_policy_state() -> None:
    state, candidate = _folded_genesis_history()
    detached = dataclasses.replace(state, upscale_observations=1)

    with pytest.raises(ValueError, match='identity'):
        capacity_planning.reproject_capacity_policy_history(
            detached, candidate, configured_accelerators=('L4', 'A100'))


def test_policy_history_reprojection_rejects_unknown_ceiling_card() -> None:
    state, candidate = _folded_genesis_history()
    ceiling_state = dataclasses.replace(
        state,
        paid_window_started_db_epoch=90.0,
        paid_window_ceiling_by_accelerator=_capacity(h200=2))
    ceiling_candidate = dataclasses.replace(candidate,
                                            next_policy_state=ceiling_state)

    # The physical domain matches; the paid-window ceiling names a card the
    # current catalog does not configure.
    with pytest.raises(ValueError, match='unknown accelerator'):
        capacity_planning.reproject_capacity_policy_history(
            ceiling_state,
            ceiling_candidate,
            configured_accelerators=('L4', 'A100'))


def test_policy_history_reprojection_preserves_counts_across_domains() -> None:
    state, candidate = _folded_genesis_history()
    ceiling_state = dataclasses.replace(
        state,
        paid_window_started_db_epoch=90.0,
        paid_window_ceiling_by_accelerator=_capacity(l4=3, a100=2))
    ceiling_candidate = dataclasses.replace(candidate,
                                            next_policy_state=ceiling_state)
    configured = ('L4', 'A100')

    new_state, new_candidate = (
        capacity_planning.reproject_capacity_policy_history(
            ceiling_state,
            ceiling_candidate,
            configured_accelerators=configured))

    assert new_state.paid_window_ceiling_by_accelerator.as_dict() == {
        'A100': 2,
        'L4': 3,
    }
    assert new_candidate.planning_capacity_quantum_by_accelerator.as_dict() == {
        'A100': 8,
        'L4': 1,
    }
    assert new_candidate.next_policy_state == new_state
    for field in capacity_planning._CANDIDATE_CAPACITY_FIELDS:  # pylint: disable=protected-access
        before = getattr(ceiling_candidate, field).as_dict()
        after = getattr(new_candidate, field).as_dict()
        assert after == {card.upper(): count for card, count in before.items()}
    assert dataclasses.replace(
        new_state,
        paid_window_ceiling_by_accelerator=(
            ceiling_state.paid_window_ceiling_by_accelerator)) == ceiling_state
    # Reprojecting display-domain history into the same catalog is a fixpoint.
    assert capacity_planning.reproject_capacity_policy_history(
        new_state, new_candidate,
        configured_accelerators=configured) == (new_state, new_candidate)


def test_fixed_paid_wave_uses_db_epoch_and_prior_committed_candidate() -> None:
    demand = (_demand(50, ('L4',), 5),)
    state = _policy_state(paid_window_started_db_epoch=95.0,
                          paid_window_ceiling_by_accelerator=_capacity(L4=2))
    prior_candidate = _prior_candidate(state, target=1)
    policy = _policy_input(planning_db_epoch=100.0,
                           latest_committed_capacity=1,
                           nonterminal_committed_capacity=1,
                           latest_committed_by_accelerator=_capacity(L4=1),
                           scale_up_rate_percentage=100,
                           scale_up_rate_min_capacity=1,
                           scale_up_rate_period_seconds=60.0)
    common = {
        'demand_profiles': demand,
        'explicit_demand_profiles': demand,
        'paid_demand_profiles': demand,
        'prior_policy_state': state,
        'prior_candidate': prior_candidate,
        'policy_input': policy,
        'reservation': _reservation(existing_paid=_capacity(L4=1)),
    }

    snapshot = _snapshot(**common)
    candidate = capacity_planning.plan_capacity(snapshot)

    assert candidate.aggregate_demand_target == 5
    assert candidate.paid_launch_target.as_dict() == {'L4': 1}
    finalized = capacity_planning.finalize_capacity_plan(
        snapshot,
        candidate,
        accepted_paid_plan_units=_capacity(L4=1),
        accepted_paid_gpu_units=1,
        decision_db_epoch=101.0)
    assert finalized.next_policy_state is not None
    assert finalized.next_policy_state.paid_window_started_db_epoch == 95.0
    assert (finalized.next_policy_state.paid_window_ceiling_by_accelerator ==
            _capacity(L4=2))

    later_snapshot = _snapshot(
        **{
            **common,
            'policy_input': dataclasses.replace(policy, planning_db_epoch=156.0
                                               ),
        })
    later = capacity_planning.plan_capacity(later_snapshot)
    later_finalized = capacity_planning.finalize_capacity_plan(
        later_snapshot,
        later,
        accepted_paid_plan_units=_capacity(L4=1),
        accepted_paid_gpu_units=1,
        decision_db_epoch=157.0)
    assert later_finalized.next_policy_state is not None
    assert (
        later_finalized.next_policy_state.paid_window_started_db_epoch == 157.0)


def test_all_rejected_paid_wave_does_not_start_cooldown() -> None:
    demand = (_demand(50, ('L4',), 5),)
    state = _policy_state()
    policy = _policy_input(planning_db_epoch=100.0,
                           scale_up_rate_percentage=100,
                           scale_up_rate_min_capacity=2,
                           scale_up_rate_period_seconds=60.0)
    snapshot = _snapshot(demand_profiles=demand,
                         explicit_demand_profiles=demand,
                         paid_demand_profiles=demand,
                         prior_policy_state=state,
                         prior_candidate=_prior_candidate(state),
                         policy_input=policy)
    candidate = capacity_planning.plan_capacity(snapshot)

    assert candidate.paid_launch_target.as_dict() == {'L4': 2}
    rejected = capacity_planning.finalize_capacity_plan(
        snapshot,
        candidate,
        accepted_paid_plan_units=_capacity(),
        accepted_paid_gpu_units=0,
        decision_db_epoch=101.0)
    assert rejected.next_policy_state is not None
    assert rejected.next_policy_state.paid_window_started_db_epoch is None
    assert rejected.next_policy_state.paid_window_ceiling_by_accelerator.total(
    ) == 0


def test_partial_acceptance_starts_full_fixed_per_card_wave() -> None:
    demand = (
        _demand(50, ('L4',), 3),
        _demand(50, ('A100',), 3, sequence=1),
    )
    state = _policy_state()
    snapshot = _snapshot(demand_profiles=demand,
                         explicit_demand_profiles=demand,
                         paid_demand_profiles=demand,
                         prior_policy_state=state,
                         prior_candidate=_prior_candidate(state),
                         policy_input=_policy_input(
                             scale_up_rate_percentage=100,
                             scale_up_rate_min_capacity=4,
                             scale_up_rate_period_seconds=60.0))
    candidate = capacity_planning.plan_capacity(snapshot)

    assert candidate.paid_launch_target.as_dict() == {'A100': 1, 'L4': 3}
    finalized = capacity_planning.finalize_capacity_plan(
        snapshot,
        candidate,
        accepted_paid_plan_units=_capacity(L4=1),
        accepted_paid_gpu_units=1,
        decision_db_epoch=101.0)

    assert finalized.next_policy_state is not None
    assert finalized.next_policy_state.paid_window_started_db_epoch == 101.0
    assert (finalized.next_policy_state.paid_window_ceiling_by_accelerator ==
            candidate.paid_launch_target)


def test_same_demand_generation_does_not_advance_hysteresis_twice() -> None:
    demand = (_demand(50, ('L4',), 5),)
    state = _policy_state(last_reduced_demand_generation=7,
                          upscale_observations=1)
    snapshot = _snapshot(
        demand_profiles=demand,
        explicit_demand_profiles=demand,
        paid_demand_profiles=demand,
        prior_policy_state=state,
        prior_candidate=_prior_candidate(state, target=1),
        policy_input=_policy_input(upscale_delay_observations=2))

    candidate = capacity_planning.plan_capacity(snapshot)

    assert candidate.aggregate_demand_target == 1
    assert candidate.next_policy_state is not None
    assert candidate.next_policy_state.upscale_observations == 1


def test_gate_acquisition_copies_minimal_state_without_consuming_generation(
) -> None:
    demand = (_demand(50, ('A100',), 3),)
    state = _policy_state()
    snapshot = _snapshot(
        demand_profiles=demand,
        explicit_demand_profiles=demand,
        paid_demand_profiles=demand,
        prior_policy_state=state,
        prior_candidate=_prior_candidate(state),
        policy_input=_policy_input(),
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_UNSETTLED),
            authenticated=_capacity(A100=3)))

    candidate = capacity_planning.plan_capacity(snapshot)

    assert candidate.kind is capacity_planning.CapacityPlanKind.GATE_ACQUISITION
    assert candidate.next_policy_state == state


def test_policy_state_gets_prior_effects_only_from_committed_candidate(
) -> None:
    demand = (_demand(50, ('L4',), 1),)
    state = _policy_state()
    prior_candidate = _prior_candidate(state, target=5)
    snapshot = _snapshot(demand_profiles=demand,
                         explicit_demand_profiles=demand,
                         paid_demand_profiles=demand,
                         prior_policy_state=state,
                         prior_candidate=prior_candidate,
                         policy_input=_policy_input(
                             latest_committed_capacity=5,
                             nonterminal_committed_capacity=5,
                             latest_committed_by_accelerator=_capacity(L4=5),
                             downscale_delay_seconds=600.0))

    candidate = capacity_planning.plan_capacity(snapshot)

    assert candidate.aggregate_demand_target == 5
    assert 'target_capacity' not in {
        field.name for field in dataclasses.fields(state)
    }


def test_policy_rejects_future_db_epoch_instead_of_rebasing_it() -> None:
    state = _policy_state(downscale_started_db_epoch=10_000.0)

    with pytest.raises(ValueError, match='PostgreSQL clock'):
        capacity_planning.plan_capacity(
            _snapshot(prior_policy_state=state,
                      prior_candidate=_prior_candidate(state),
                      policy_input=_policy_input(planning_db_epoch=100.0)))


def test_snapshot_fingerprint_binds_physical_gpu_packing() -> None:
    one_gpu = _snapshot(
        planning_capacity_quantum_by_accelerator=_capacity(L4=1, A100=1))
    eight_gpu = _snapshot(
        planning_capacity_quantum_by_accelerator=_capacity(L4=1, A100=8))

    assert one_gpu.fingerprint != eight_gpu.fingerprint
    assert (capacity_planning.plan_capacity(eight_gpu).
            planning_capacity_quantum_by_accelerator.as_dict()['A100'] == 8)


def test_reserved_supply_changes_actuation_not_demand_attribution() -> None:
    without_reserved = capacity_planning.plan_capacity(_snapshot())
    with_reserved = capacity_planning.plan_capacity(
        _snapshot(reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_SETTLED),
            authenticated=_capacity(A100=2),
            eligible=_capacity(A100=2))))

    assert without_reserved.demand_attribution.as_dict() == {'L4': 2}
    assert with_reserved.demand_attribution == without_reserved.demand_attribution
    assert with_reserved.supply_aware_actuation_target.as_dict() == {'A100': 2}


def test_supply_rematch_preserves_explicit_cold_paid_order() -> None:
    demand = (_demand(20, ('A100', 'L4'), 1),)
    plan = capacity_planning.plan_capacity(
        _snapshot(configured_accelerators=('A100', 'L4'),
                  planning_capacity_quantum_by_accelerator=_capacity(A100=1,
                                                                     L4=1),
                  capacity_per_accelerator=_work(A100=1, L4=1),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  cold_accelerator_order=('L4', 'A100'),
                  prospective_paid_accelerator_order=('L4', 'A100')))

    assert plan.demand_attribution.as_dict() == {'L4': 1}
    assert plan.supply_aware_demand_target.as_dict() == {'L4': 1}
    assert plan.paid_residual.as_dict() == {'L4': 1}
    assert plan.paid_launch_target.as_dict() == {'L4': 1}


@pytest.mark.parametrize('gate_policy',
                         (capacity_planning.ReservationGatePolicy.DEMAND_GATED,
                          capacity_planning.ReservationGatePolicy.UNGATED))
def test_flexible_demand_uses_reserved_a100_then_only_l4_paid_authority(
        gate_policy: capacity_planning.ReservationGatePolicy) -> None:
    profiles = (
        _demand(20, ('A100', 'L4'), 1, 0),
        _demand(20, ('L4', 'A100'), 1, 1),
    )
    reservation = _reservation(
        gate_policy=gate_policy,
        evidence_state=(
            capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED),
        authenticated=_capacity(A100=1),
        eligible=_capacity(A100=1))

    plan = capacity_planning.plan_capacity(
        _snapshot(demand_profiles=profiles,
                  explicit_demand_profiles=profiles,
                  paid_demand_profiles=profiles,
                  prospective_paid_accelerator_order=('L4',),
                  reservation=reservation))
    reordered_plan = capacity_planning.plan_capacity(
        _snapshot(demand_profiles=tuple(reversed(profiles)),
                  explicit_demand_profiles=tuple(reversed(profiles)),
                  paid_demand_profiles=tuple(reversed(profiles)),
                  prospective_paid_accelerator_order=('L4',),
                  reservation=reservation))

    assert reordered_plan == plan
    assert plan.demand_attribution.as_dict() == {'L4': 2}
    assert plan.supply_aware_demand_target.as_dict() == {'A100': 1, 'L4': 1}
    assert plan.new_reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.paid_residual.as_dict() == {'L4': 1}
    assert plan.paid_launch_target.as_dict() == {'L4': 1}


@pytest.mark.parametrize('gate_policy',
                         (capacity_planning.ReservationGatePolicy.DEMAND_GATED,
                          capacity_planning.ReservationGatePolicy.UNGATED))
def test_high_priority_a100_only_demand_preserves_a100_for_constrained_work(
        gate_policy: capacity_planning.ReservationGatePolicy) -> None:
    profiles = (
        _demand(50, ('A100',), 1, 1),
        _demand(20, ('L4', 'A100'), 1, 0),
    )
    reservation = _reservation(
        gate_policy=gate_policy,
        evidence_state=(
            capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED),
        authenticated=_capacity(A100=1),
        eligible=_capacity(A100=1))

    plan = capacity_planning.plan_capacity(
        _snapshot(demand_profiles=profiles,
                  explicit_demand_profiles=profiles,
                  paid_demand_profiles=profiles,
                  prospective_paid_accelerator_order=('L4',),
                  reservation=reservation))
    reordered_plan = capacity_planning.plan_capacity(
        _snapshot(demand_profiles=tuple(reversed(profiles)),
                  explicit_demand_profiles=tuple(reversed(profiles)),
                  paid_demand_profiles=tuple(reversed(profiles)),
                  prospective_paid_accelerator_order=('L4',),
                  reservation=reservation))

    assert reordered_plan == plan
    assert plan.demand_attribution.as_dict() == {'A100': 1, 'L4': 1}
    assert plan.supply_aware_demand_target.as_dict() == {'A100': 1, 'L4': 1}
    assert plan.new_reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.paid_residual.as_dict() == {'L4': 1}
    assert plan.paid_launch_target.as_dict() == {'L4': 1}


def test_live_twelve_plus_three_padding_is_distinct_from_paid_demand() -> None:
    profiles = (_demand(20, ('L4', 'A100'), 12),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            minimum_capacity=12,
            actuation_minimum_capacity=15,
            demand_profiles=profiles,
            explicit_demand_profiles=profiles,
            paid_demand_profiles=profiles,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=15),
                eligible=_capacity(A100=15))))

    assert plan.kind is capacity_planning.CapacityPlanKind.DEMAND
    assert plan.aggregate_demand_target == 12
    assert plan.supply_aware_demand_target.as_dict() == {'A100': 12}
    assert plan.zero_cost_padding_target.as_dict() == {'A100': 3}
    assert plan.supply_aware_actuation_target.as_dict() == {'A100': 15}
    assert plan.paid_demand_attribution.total() == 12


def test_wave_limited_actuation_can_retain_a_different_card() -> None:
    profiles = (_demand(20, ('A100',), 12),)
    desired = capacity_planning.plan_capacity(
        _snapshot(
            minimum_capacity=12,
            actuation_minimum_capacity=15,
            demand_profiles=profiles,
            explicit_demand_profiles=profiles,
            paid_demand_profiles=profiles,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=15),
                eligible=_capacity(A100=15))))

    wave = dataclasses.replace(desired,
                               transition_retention_target=_capacity(L4=4),
                               wave_limited_actuation_target=_capacity(A100=11,
                                                                       L4=4),
                               retirement_floor_target=_capacity(A100=11, L4=4))

    assert wave.supply_aware_demand_target.as_dict() == {'A100': 12}
    assert wave.zero_cost_padding_target.as_dict() == {'A100': 3}
    assert wave.supply_aware_actuation_target.as_dict() == {'A100': 15}
    assert wave.target_by_accelerator == {'A100': 11, 'L4': 4}


def test_incompatible_reserved_supply_does_not_suppress_paid_demand() -> None:
    l4_only = (_demand(50, ('L4',), 1),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=l4_only,
            explicit_demand_profiles=l4_only,
            paid_demand_profiles=l4_only,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=10),
                eligible=_capacity(A100=10))))

    assert plan.demand_attribution.as_dict() == {'L4': 1}
    assert plan.supply_aware_actuation_target.as_dict() == {'L4': 1}
    assert plan.paid_demand_attribution.as_dict() == {'L4': 1}
    assert plan.new_reserved_capacity_committed.total() == 0
    assert plan.paid_residual.as_dict() == {'L4': 1}


def test_reservation_only_card_never_receives_paid_launch_authority() -> None:
    a100_only = (_demand(50, ('A100',), 1),)

    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=a100_only,
            explicit_demand_profiles=a100_only,
            paid_demand_profiles=a100_only,
            prospective_paid_accelerator_order=('L4',),
        ))

    # The demand is retained for observability and can be satisfied by a
    # later reservation observation.  This generation has no proven paid
    # A100 location, so it grants no cold provider authority.
    assert plan.supply_aware_demand_target.as_dict() == {'A100': 1}
    assert plan.paid_residual.total() == 0


def test_usage_gate_commits_only_eligible_tail_then_keeps_spot_residual(
) -> None:
    demand = (_demand(50, ('A100',), 3),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=3),
                eligible=_capacity(A100=1))))

    assert plan.supply_aware_demand_target.as_dict() == {'A100': 3}
    assert plan.new_reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.paid_residual.as_dict() == {'A100': 2}


def test_usage_gate_off_commits_reservation_before_uncovered_spot_residual(
) -> None:
    demand = (_demand(50, ('A100',), 3),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=capacity_planning.ReservationGatePolicy.UNGATED,
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=1),
                eligible=_capacity(A100=1))))

    assert plan.kind is capacity_planning.CapacityPlanKind.DEMAND
    assert plan.new_reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.reserved_launch_target.as_dict() == {'A100': 1}
    assert plan.paid_residual.as_dict() == {'A100': 2}
    assert plan.paid_launch_target.as_dict() == {'A100': 2}


def test_logical_reservation_debit_launches_one_complete_eight_gpu_backend(
) -> None:
    demand = (_demand(50, ('A100',), 1),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            planning_capacity_quantum_by_accelerator=_capacity(L4=1, A100=8),
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=8),
                eligible=_capacity(A100=8))))

    assert plan.supply_aware_demand_target.as_dict() == {'A100': 1}
    assert plan.new_reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.reserved_launch_target.as_dict() == {'A100': 8}
    assert plan.reserved_packing_padding_target.as_dict() == {'A100': 7}
    assert plan.paid_residual.total() == 0


def test_partial_multi_gpu_reservation_stays_spot_demand() -> None:
    demand = (_demand(50, ('A100',), 1),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            planning_capacity_quantum_by_accelerator=_capacity(L4=1, A100=8),
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=4),
                eligible=_capacity(A100=4))))

    assert plan.new_reserved_capacity_committed.total() == 0
    assert plan.reserved_launch_target.total() == 0
    assert plan.reserved_packing_padding_target.total() == 0
    assert plan.paid_residual.as_dict() == {'A100': 1}
    assert plan.paid_launch_target.as_dict() == {'A100': 8}
    assert plan.paid_packing_padding_target.as_dict() == {'A100': 7}


def test_multi_gpu_paid_launch_is_minimal_whole_backend_cover() -> None:
    demand = (_demand(50, ('A100',), 9),)

    plan = capacity_planning.plan_capacity(
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand))

    assert plan.paid_residual.as_dict() == {'A100': 9}
    assert plan.paid_launch_target.as_dict() == {'A100': 16}
    assert plan.paid_packing_padding_target.as_dict() == {'A100': 7}


def test_paid_cap_preserves_residual_while_authorizing_one_whole_backend(
) -> None:
    demand = (_demand(50, ('A100',), 9),)

    plan = capacity_planning.plan_capacity(
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  max_live_paid_gpu_units=8))

    assert plan.paid_residual.as_dict() == {'A100': 9}
    assert plan.paid_launch_target.as_dict() == {'A100': 8}
    assert plan.paid_packing_padding_target.total() == 0
    assert plan.paid_cap == capacity_planning.PaidCapProjection(
        max_live_paid_gpu_units=8,
        charged_paid_gpu_units=0,
        remaining_paid_gpu_units=8)


@pytest.mark.parametrize(('cap', 'existing', 'expected_launch'),
                         ((16, 0, 1), (32, 0, 2), (32, 1, 1)))
def test_physical_paid_cap_debits_every_backend_node(
        cap: int, existing: int, expected_launch: int) -> None:
    demand = (_demand(50, ('A100',), 2),)
    reservation = _reservation(existing_paid=_capacity(A100=existing))
    reservation = dataclasses.replace(reservation,
                                      charged_paid_gpu_units=existing * 16)

    plan = capacity_planning.plan_capacity(
        _snapshot(capacity_unit=capacity_planning.CapacityUnit.PHYSICAL_BACKEND,
                  backend_num_nodes=2,
                  planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  reservation=reservation,
                  max_live_paid_gpu_units=cap))

    assert plan.paid_residual.as_dict() == {'A100': 2 - existing}
    assert plan.paid_launch_target.as_dict() == ({
        'A100': expected_launch
    } if expected_launch else {})
    assert plan.backend_num_nodes == 2


def test_physical_backend_shape_product_overflow_is_rejected() -> None:
    with pytest.raises(ValueError, match='exact accounting range'):
        _snapshot(capacity_unit=capacity_planning.CapacityUnit.PHYSICAL_BACKEND,
                  backend_num_nodes=2,
                  planning_capacity_quantum_by_accelerator=_capacity(
                      L4=(1 << 63) - 1, A100=8))


def test_existing_paid_capacity_is_charged_before_new_authority() -> None:
    demand = (_demand(50, ('A100',), 1),)

    plan = capacity_planning.plan_capacity(
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  max_live_paid_gpu_units=8,
                  reservation=_reservation(existing_paid=_capacity(L4=8))))

    assert plan.paid_residual.as_dict() == {'A100': 1}
    assert plan.paid_launch_target.total() == 0
    assert plan.paid_cap.charged_paid_gpu_units == 8
    assert plan.paid_cap.remaining_paid_gpu_units == 0


def test_cleanup_unproven_old_paid_capacity_charges_without_covering_demand(
) -> None:
    demand = (_demand(50, ('A100',), 1),)
    reservation = dataclasses.replace(_reservation(existing_paid=_capacity()),
                                      charged_paid_gpu_units=8)

    plan = capacity_planning.plan_capacity(
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  max_live_paid_gpu_units=8,
                  reservation=reservation))

    assert plan.paid_residual.as_dict() == {'A100': 1}
    assert plan.paid_launch_target.total() == 0
    assert plan.paid_cap.charged_paid_gpu_units == 8
    assert plan.paid_cap.remaining_paid_gpu_units == 0


def test_paid_cap_skips_nonfitting_wide_card_for_later_narrow_card() -> None:
    demand = (
        _demand(50, ('A100',), 1, 0),
        _demand(50, ('L4',), 3, 1),
    )

    plan = capacity_planning.plan_capacity(
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  prospective_paid_accelerator_order=('A100', 'L4'),
                  max_live_paid_gpu_units=4))

    assert plan.paid_residual.as_dict() == {'A100': 1, 'L4': 3}
    assert plan.paid_launch_target.as_dict() == {'L4': 3}
    assert plan.paid_packing_padding_target.total() == 0


def test_paid_cap_smaller_than_backend_grants_no_fractional_authority() -> None:
    demand = (_demand(50, ('A100',), 1),)

    plan = capacity_planning.plan_capacity(
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  max_live_paid_gpu_units=7))

    assert plan.paid_residual.as_dict() == {'A100': 1}
    assert plan.paid_launch_target.total() == 0
    assert plan.paid_packing_padding_target.total() == 0


@pytest.mark.parametrize(('launch', 'padding'), ((7, 6), (16, 15), (8, 6)))
def test_candidate_rejects_nonminimal_or_inconsistent_paid_backend_cover(
        launch: int, padding: int) -> None:
    demand = (_demand(50, ('A100',), 1),)
    plan = capacity_planning.plan_capacity(
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand))

    with pytest.raises(ValueError, match='minimal whole physical cover'):
        dataclasses.replace(plan,
                            paid_launch_target=_capacity(A100=launch),
                            paid_packing_padding_target=_capacity(A100=padding))


def test_unsettled_usage_gate_commits_effect_free_acquisition() -> None:
    demand = (_demand(50, ('A100',), 3),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_UNSETTLED),
                authenticated=_capacity(A100=3))))

    assert plan.kind is capacity_planning.CapacityPlanKind.GATE_ACQUISITION
    assert plan.aggregate_demand_target == 3
    assert plan.demand_attribution.as_dict() == {'A100': 3}
    assert plan.new_reserved_capacity_committed.total() == 0
    assert plan.paid_residual.total() == 0
    assert plan.paid_launch_target.total() == 0
    assert plan.wave_limited_actuation_target.total() == 0
    assert plan.retirement_floor_target.total() == 0


def test_downscale_hold_above_raw_demand_has_no_acquisition_classes() -> None:
    demand = (_demand(50, ('L4',), 1),)
    held = _capacity(L4=5)
    prior = _policy_state()
    snapshot = _snapshot(
        demand_profiles=demand,
        explicit_demand_profiles=demand,
        paid_demand_profiles=demand,
        prior_policy_state=prior,
        prior_candidate=_prior_candidate(prior, target=5),
        policy_input=_policy_input(latest_committed_capacity=5,
                                   nonterminal_committed_capacity=5,
                                   latest_committed_by_accelerator=held,
                                   downscale_delay_seconds=600.0),
        configured_reservation_accelerators=('A100',),
        demand_witness_scope_sha256='a' * 64,
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(
                capacity_planning.ReservationEvidenceState.UNAVAILABLE)))

    candidate = capacity_planning.plan_capacity(snapshot)

    assert candidate.kind is capacity_planning.CapacityPlanKind.INCOMPLETE
    assert candidate.reservation_acquisition_classes is None


def test_matching_gate_witness_commits_reservation_before_spot_residual(
) -> None:
    demand = (_demand(50, ('A100',), 3),)
    acquisition_snapshot = _snapshot(
        demand_profiles=demand,
        explicit_demand_profiles=demand,
        paid_demand_profiles=demand,
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_UNSETTLED),
            authenticated=_capacity(A100=1)))
    acquisition = capacity_planning.plan_capacity(acquisition_snapshot)
    assert acquisition.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    assert acquisition.demand_witness_sha256 is not None

    settled = dataclasses.replace(
        acquisition_snapshot,
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_SETTLED),
            authenticated=_capacity(A100=1),
            eligible=_capacity(A100=1),
            allocation_witness=acquisition.demand_witness_sha256,
            demonstrated_need=3,
            allocation_ceiling=3))
    plan = capacity_planning.plan_capacity(settled)

    assert plan.kind is capacity_planning.CapacityPlanKind.DEMAND
    assert plan.new_reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.paid_residual.as_dict() == {'A100': 2}
    assert plan.paid_launch_target.as_dict() == {'A100': 2}


def test_gate_witness_survives_equivalent_deadline_heartbeat() -> None:
    initial_demand = (_demand(50, ('A100',), 0.49),)
    acquisition_snapshot = _snapshot(
        demand_profiles=initial_demand,
        explicit_demand_profiles=initial_demand,
        paid_demand_profiles=initial_demand,
        deadline=_deadline(remaining_seconds=3600),
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_UNSETTLED),
            authenticated=_capacity(A100=1)))
    acquisition = capacity_planning.plan_capacity(acquisition_snapshot)
    assert acquisition.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    witness = acquisition.demand_witness_sha256
    assert witness is not None

    refreshed_demand = (_demand(50, ('A100',), 0.105),)
    settled_snapshot = dataclasses.replace(
        acquisition_snapshot,
        demand_profiles=refreshed_demand,
        explicit_demand_profiles=refreshed_demand,
        paid_demand_profiles=refreshed_demand,
        deadline=_deadline(remaining_seconds=3595),
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_SETTLED),
            authenticated=_capacity(A100=1),
            eligible=_capacity(A100=1),
            allocation_witness=witness,
            demonstrated_need=1,
            allocation_ceiling=1))
    settled = capacity_planning.plan_capacity(settled_snapshot)

    assert settled.demand_witness_sha256 == witness
    assert settled.kind is capacity_planning.CapacityPlanKind.DEMAND
    assert settled.new_reserved_capacity_committed.as_dict() == {'A100': 1}
    assert settled.reserved_launch_target.as_dict() == {'A100': 1}


def test_gate_witness_survives_equivalent_deadline_bucket_merge() -> None:
    split_deadline = capacity_planning.DeadlinePlanningInput(
        demand=(
            autoscaler_compatibility.DeadlineDemand(sequence=0,
                                                    priority=50,
                                                    compatible_cards=('A100',),
                                                    count=1,
                                                    remaining_seconds=3600),
            autoscaler_compatibility.DeadlineDemand(sequence=1,
                                                    priority=50,
                                                    compatible_cards=('A100',),
                                                    count=1,
                                                    remaining_seconds=3595),
        ),
        finite_supply=(),
        service_seconds_by_accelerator=_work(L4=30, A100=30),
        service_time_sources=(('L4', 'seed'), ('A100', 'seed')),
        utilization=0.95,
        paid_cold_lead_seconds=600)
    merged_deadline = _deadline(remaining_seconds=3590, count=2)
    demand = (_demand(50, ('A100',), 1),)
    split = _snapshot(
        demand_profiles=demand,
        explicit_demand_profiles=demand,
        paid_demand_profiles=demand,
        deadline=split_deadline,
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_UNSETTLED),
            authenticated=_capacity(A100=2)))
    merged = dataclasses.replace(split, deadline=merged_deadline)

    assert capacity_planning.demand_witness_semantic_sha256(
        split,
        aggregate_demand_target=2,
        demand_attribution=_capacity(A100=2),
        reservation_acquisition_classes=(_acquisition(
            50, ('A100',),
            2),)) == (capacity_planning.demand_witness_semantic_sha256(
                merged,
                aggregate_demand_target=2,
                demand_attribution=_capacity(A100=2),
                reservation_acquisition_classes=(_acquisition(50, ('A100',),
                                                              2),)))


def test_gate_witness_changes_with_request_class_target_or_card() -> None:
    demand = (_demand(50, ('A100',), 1),)
    snapshot = _snapshot(
        demand_profiles=demand,
        explicit_demand_profiles=demand,
        paid_demand_profiles=demand,
        deadline=_deadline(remaining_seconds=3600),
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_UNSETTLED),
            authenticated=_capacity(A100=2)))
    baseline = capacity_planning.demand_witness_semantic_sha256(
        snapshot,
        aggregate_demand_target=1,
        demand_attribution=_capacity(A100=1),
        reservation_acquisition_classes=(_acquisition(50, ('A100',), 1),))

    priority_demand = (_demand(20, ('A100',), 1),)
    priority_snapshot = dataclasses.replace(
        snapshot,
        demand_profiles=priority_demand,
        explicit_demand_profiles=priority_demand,
        paid_demand_profiles=priority_demand,
        deadline=_deadline(remaining_seconds=3600, priority=20))
    flexible_demand = (_demand(50, ('L4', 'A100'), 1),)
    compatibility_snapshot = dataclasses.replace(
        snapshot,
        demand_profiles=flexible_demand,
        explicit_demand_profiles=flexible_demand,
        paid_demand_profiles=flexible_demand,
        deadline=_deadline(remaining_seconds=3600, cards=('L4', 'A100')))

    assert capacity_planning.demand_witness_semantic_sha256(
        priority_snapshot,
        aggregate_demand_target=1,
        demand_attribution=_capacity(A100=1),
        reservation_acquisition_classes=(_acquisition(20, ('A100',),
                                                      1),)) != baseline
    assert capacity_planning.demand_witness_semantic_sha256(
        compatibility_snapshot,
        aggregate_demand_target=1,
        demand_attribution=_capacity(A100=1),
        reservation_acquisition_classes=(_acquisition(50, ('L4', 'A100'),
                                                      1),)) != baseline
    assert capacity_planning.demand_witness_semantic_sha256(
        snapshot,
        aggregate_demand_target=2,
        demand_attribution=_capacity(A100=2),
        reservation_acquisition_classes=(_acquisition(50, ('A100',),
                                                      2),)) != baseline
    assert capacity_planning.demand_witness_semantic_sha256(
        snapshot,
        aggregate_demand_target=1,
        demand_attribution=_capacity(L4=1),
        reservation_acquisition_classes=(_acquisition(50, ('L4',),
                                                      1),)) != baseline


def test_gate_witness_binds_scope_and_ignores_volatile_inputs() -> None:
    demand = (_demand(50, ('A100',), 1),)
    snapshot = _snapshot(
        demand_profiles=demand,
        explicit_demand_profiles=demand,
        paid_demand_profiles=demand,
        deadline=_deadline(remaining_seconds=3600),
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_UNSETTLED),
            authenticated=_capacity(A100=2)))
    classes = (_acquisition(50, ('A100',), 1),)

    def witness(candidate: capacity_planning.CapacityPlanningSnapshot) -> str:
        return capacity_planning.demand_witness_semantic_sha256(
            candidate,
            aggregate_demand_target=1,
            demand_attribution=_capacity(A100=1),
            reservation_acquisition_classes=classes)

    baseline = witness(snapshot)

    # Inputs whose capacity consequence is already carried by the reduced
    # target, attribution, and acquisition classes must not move the witness:
    # an equivalent heartbeat would otherwise revoke the grant it authorized.
    refreshed_demand = (_demand(50, ('A100',), 0.25),)
    volatile = dataclasses.replace(snapshot,
                                   service_version=snapshot.service_version + 1,
                                   maximum_capacity=5,
                                   minimum_capacity=1,
                                   actuation_minimum_capacity=1,
                                   floors=_capacity(A100=1),
                                   capacity_per_accelerator=_work(L4=2, A100=2),
                                   demand_profiles=refreshed_demand,
                                   explicit_demand_profiles=refreshed_demand,
                                   paid_demand_profiles=refreshed_demand,
                                   deadline=dataclasses.replace(
                                       _deadline(remaining_seconds=1),
                                       utilization=0.5,
                                       paid_cold_lead_seconds=0.0),
                                   cold_accelerator_order=('A100', 'L4'))
    assert witness(volatile) == baseline

    # Every configured-scope field the reservation grant was sized against
    # must move it.
    assert witness(
        dataclasses.replace(
            snapshot,
            configured_reservation_accelerators=('L4', 'A100'))) != baseline
    assert witness(
        dataclasses.replace(snapshot,
                            planning_capacity_quantum_by_accelerator=_capacity(
                                L4=1, A100=8))) != baseline
    physical = dataclasses.replace(
        snapshot, capacity_unit=capacity_planning.CapacityUnit.PHYSICAL_BACKEND)
    multi_node = dataclasses.replace(physical, backend_num_nodes=2)
    assert witness(physical) != baseline
    assert witness(multi_node) != witness(physical)
    assert witness(
        dataclasses.replace(snapshot,
                            demand_witness_scope_sha256='b' * 64)) != baseline


def test_gate_witness_binds_reduced_acquisition_classes() -> None:
    demand = (_demand(50, ('A100',), 2),)
    snapshot = _snapshot(
        configured_accelerators=('A100',),
        configured_reservation_accelerators=('A100',),
        planning_capacity_quantum_by_accelerator=_capacity(A100=1),
        capacity_per_accelerator=_work(A100=1),
        demand_profiles=demand,
        explicit_demand_profiles=(),
        paid_demand_profiles=(),
        reservation=_reservation(
            gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
            evidence_state=(capacity_planning.ReservationEvidenceState.
                            AUTHENTICATED_UNSETTLED)),
        cold_accelerator_order=('A100',),
        prospective_paid_accelerator_order=())

    exact = capacity_planning.demand_witness_semantic_sha256(
        snapshot,
        aggregate_demand_target=2,
        demand_attribution=_capacity(A100=2),
        reservation_acquisition_classes=(_acquisition(50, ('A100',), 2),))
    flexible = capacity_planning.demand_witness_semantic_sha256(
        snapshot,
        aggregate_demand_target=2,
        demand_attribution=_capacity(A100=2),
        reservation_acquisition_classes=(_acquisition(50, ('A100', 'A100-80GB'),
                                                      2),))

    assert exact != flexible


def test_fractional_copack_intersects_acquisition_compatibility() -> None:
    configured = ('A', 'B', 'C')
    demand = (
        _demand(20, ('A', 'B'), 0.5),
        _demand(20, ('B', 'C'), 0.5, sequence=1),
    )

    plan = capacity_planning.plan_capacity(
        _snapshot(configured_accelerators=configured,
                  planning_capacity_quantum_by_accelerator=_capacity(A=1,
                                                                     B=1,
                                                                     C=1),
                  capacity_per_accelerator=_work(A=1, B=1, C=1),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  cold_accelerator_order=('B', 'A', 'C'),
                  prospective_paid_accelerator_order=configured))

    assert plan.aggregate_demand_target == 1
    assert plan.reservation_acquisition_classes == (_acquisition(20, ('B',),
                                                                 1),)


def test_acquisition_class_excludes_card_that_cannot_carry_copacked_work(
) -> None:
    demand = (_demand(20, ('FAST', 'SLOW'), 1.5),)

    plan = capacity_planning.plan_capacity(
        _snapshot(configured_accelerators=('FAST', 'SLOW'),
                  planning_capacity_quantum_by_accelerator=_capacity(FAST=1,
                                                                     SLOW=1),
                  capacity_per_accelerator=_work(FAST=2, SLOW=1),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  cold_accelerator_order=('FAST', 'SLOW'),
                  prospective_paid_accelerator_order=('FAST', 'SLOW')))

    assert plan.demand_attribution.as_dict() == {'FAST': 1}
    assert plan.reservation_acquisition_classes == (_acquisition(
        20, ('FAST',), 1),)


def test_fixed_work_exact_pins_a_shared_fractional_slot() -> None:
    demand = (_demand(50, ('A100', 'H200'), 0.5),)

    plan = capacity_planning.plan_capacity(
        _snapshot(configured_accelerators=('A100', 'H200'),
                  planning_capacity_quantum_by_accelerator=_capacity(A100=1,
                                                                     H200=1),
                  capacity_per_accelerator=_work(A100=1, H200=1),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  fixed_work=_work(A100=0.5),
                  cold_accelerator_order=('A100', 'H200'),
                  prospective_paid_accelerator_order=('A100', 'H200')))

    assert plan.demand_attribution.as_dict() == {'A100': 1}
    assert plan.reservation_acquisition_classes == (_acquisition(
        50, ('A100',), 1),)


def test_disjoint_high_priority_demand_does_not_upgrade_fixed_work() -> None:
    demand = (_demand(50, ('L4',), 1),)

    plan = capacity_planning.plan_capacity(
        _snapshot(configured_accelerators=('A100', 'L4'),
                  planning_capacity_quantum_by_accelerator=_capacity(A100=1,
                                                                     L4=1),
                  capacity_per_accelerator=_work(A100=1, L4=1),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  fixed_work=_work(A100=1),
                  explicit_fixed_work=_work(A100=1),
                  paid_fixed_work=_work(A100=1),
                  cold_accelerator_order=('A100', 'L4'),
                  prospective_paid_accelerator_order=('A100', 'L4')))

    assert plan.reservation_acquisition_classes == (
        _acquisition(50, ('L4',), 1),
        _acquisition(0, ('A100',), 1),
    )


def test_logical_multi_gpu_target_carries_one_class_per_logical_slot() -> None:
    demand = (_demand(20, ('A100',), 8),)

    plan = capacity_planning.plan_capacity(
        _snapshot(configured_accelerators=('A100',),
                  planning_capacity_quantum_by_accelerator=_capacity(A100=8),
                  capacity_per_accelerator=_work(A100=1),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  cold_accelerator_order=('A100',),
                  prospective_paid_accelerator_order=('A100',)))

    assert plan.aggregate_demand_target == 8
    assert plan.paid_launch_target.as_dict() == {'A100': 8}
    assert plan.reservation_acquisition_classes == (_acquisition(
        20, ('A100',), 8),)


def test_joint_matcher_consumes_all_compatible_free_cards_before_paid() -> None:
    configured = ('A', 'B', 'C')
    demand = (
        _demand(50, ('A', 'C'), 1),
        _demand(50, ('A', 'B'), 2, sequence=1),
    )
    free = _capacity(A=1, B=1, C=1)

    plan = capacity_planning.plan_capacity(
        _snapshot(
            configured_accelerators=configured,
            planning_capacity_quantum_by_accelerator=free,
            capacity_per_accelerator=_work(A=1, B=1, C=1),
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=capacity_planning.ReservationGatePolicy.
                DEMAND_GATED,
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=free,
                eligible=free),
            cold_accelerator_order=configured,
            prospective_paid_accelerator_order=configured))

    assert plan.supply_aware_demand_target.as_dict() == {'A': 1, 'B': 1, 'C': 1}
    assert plan.new_reserved_capacity_committed.as_dict() == {
        'A': 1,
        'B': 1,
        'C': 1,
    }
    assert plan.paid_residual.total() == 0
    assert plan.paid_launch_target.total() == 0


def test_zero_cost_preference_beats_lower_rank_paid_counterexample() -> None:
    configured = ('A', 'B', 'C')
    demand = (
        _demand(50, ('A', 'C'), 1),
        _demand(50, ('A', 'B'), 5, sequence=1),
    )

    plan = capacity_planning.plan_capacity(
        _snapshot(
            configured_accelerators=configured,
            planning_capacity_quantum_by_accelerator=_capacity(A=1, B=1, C=1),
            capacity_per_accelerator=_work(A=1, B=1, C=1),
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=capacity_planning.ReservationGatePolicy.
                DEMAND_GATED,
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A=5, B=5),
                eligible=_capacity(A=5, B=5),
                existing_paid=_capacity(C=1)),
            cold_accelerator_order=configured,
            prospective_paid_accelerator_order=configured))

    assert plan.supply_aware_demand_target.total() == 6
    assert 'C' not in plan.supply_aware_demand_target.as_dict()
    assert plan.new_reserved_capacity_committed.total() == 6
    assert plan.paid_residual.total() == 0


def test_deadline_only_target_uses_exact_selected_acquisition_class() -> None:
    deadline = capacity_planning.DeadlinePlanningInput(
        demand=(autoscaler_compatibility.DeadlineDemand(
            sequence=0,
            priority=50,
            compatible_cards=('A100',),
            count=1,
            remaining_seconds=3600),),
        finite_supply=(),
        service_seconds_by_accelerator=_work(A100=30),
        service_time_sources=(('A100', 'seed'),),
        utilization=0.95,
        paid_cold_lead_seconds=600)

    plan = capacity_planning.plan_capacity(
        _snapshot(
            configured_accelerators=('A100',),
            planning_capacity_quantum_by_accelerator=_capacity(A100=1),
            capacity_per_accelerator=_work(A100=1),
            demand_profiles=(),
            explicit_demand_profiles=(),
            paid_demand_profiles=(),
            deadline=deadline,
            reservation=_reservation(
                gate_policy=capacity_planning.ReservationGatePolicy.
                DEMAND_GATED,
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=1),
                eligible=_capacity(A100=1)),
            cold_accelerator_order=('A100',),
            prospective_paid_accelerator_order=('A100',)))

    assert plan.kind is capacity_planning.CapacityPlanKind.DEMAND
    assert plan.deadline_target.as_dict() == {'A100': 1}
    assert plan.reservation_acquisition_classes == (_acquisition(
        50, ('A100',), 1),)
    assert plan.new_reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.paid_residual.total() == 0


def test_mixed_case_cards_share_one_matcher_domain() -> None:
    demand = (_demand(50, ('a100', 'H200'), 2),)
    free = _capacity(A100=1, h200=1)

    plan = capacity_planning.plan_capacity(
        _snapshot(
            configured_accelerators=('A100', 'h200'),
            planning_capacity_quantum_by_accelerator=free,
            capacity_per_accelerator=_work(A100=1, h200=1),
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=capacity_planning.ReservationGatePolicy.
                DEMAND_GATED,
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=free,
                eligible=free),
            cold_accelerator_order=('A100', 'h200'),
            prospective_paid_accelerator_order=('A100', 'h200')))

    assert plan.reservation_acquisition_classes == (_acquisition(
        50, ('A100', 'h200'), 2),)
    assert plan.supply_aware_demand_target.as_dict() == {'A100': 1, 'h200': 1}
    assert plan.paid_residual.total() == 0


def test_stale_gate_witness_with_large_numeric_ceiling_has_no_effect() -> None:
    demand = (_demand(50, ('A100',), 3),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=3),
                eligible=_capacity(A100=3),
                allocation_witness='1' * 64,
                demonstrated_need=1_000_000,
                allocation_ceiling=1_000_000)))

    assert plan.kind is capacity_planning.CapacityPlanKind.GATE_ACQUISITION
    assert plan.paid_launch_target.total() == 0
    assert plan.reserved_launch_target.total() == 0


@pytest.mark.parametrize('gate_policy',
                         (capacity_planning.ReservationGatePolicy.DEMAND_GATED,
                          capacity_planning.ReservationGatePolicy.UNGATED))
def test_exact_disjoint_spot_demand_bypasses_reservation_observer_blackout(
        gate_policy) -> None:
    demand = (_demand(50, ('L4',), 2),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            configured_reservation_accelerators=('A100',),
            demand_witness_scope_sha256=(
                'a' * 64 if gate_policy
                is capacity_planning.ReservationGatePolicy.DEMAND_GATED else
                ''),
            reservation=_reservation(
                gate_policy=gate_policy,
                evidence_state=(
                    capacity_planning.ReservationEvidenceState.UNAVAILABLE))))

    assert plan.kind is capacity_planning.CapacityPlanKind.DEMAND
    assert plan.reservation_demand_relation is (
        capacity_planning.ReservationDemandRelation.STATICALLY_DISJOINT)
    assert plan.statically_disjoint_demand_accelerators == ('L4',)
    assert plan.paid_launch_target.as_dict() == {'L4': 2}


def test_unowned_generic_minimum_fails_closed() -> None:
    demand = (_demand(50, ('L4',), 1),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            minimum_capacity=5,
            paid_minimum_capacity=5,
            actuation_minimum_capacity=5,
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            configured_reservation_accelerators=('A100',),
            demand_witness_scope_sha256='a' * 64,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(
                    capacity_planning.ReservationEvidenceState.UNAVAILABLE))))

    assert plan.kind is capacity_planning.CapacityPlanKind.INCOMPLETE
    assert plan.reservation_acquisition_classes is None


def test_demand_derived_minimum_keeps_exact_disjoint_spot_path() -> None:
    demand = (_demand(50, ('L4',), 2),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            minimum_capacity=2,
            paid_minimum_capacity=0,
            actuation_minimum_capacity=2,
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            configured_reservation_accelerators=('A100',),
            demand_witness_scope_sha256='a' * 64,
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(
                    capacity_planning.ReservationEvidenceState.UNAVAILABLE))))

    assert plan.kind is capacity_planning.CapacityPlanKind.DEMAND
    assert plan.reservation_demand_relation is (
        capacity_planning.ReservationDemandRelation.STATICALLY_DISJOINT)
    assert plan.paid_launch_target.as_dict() == {'L4': 2}


@pytest.mark.parametrize('gate_policy',
                         (capacity_planning.ReservationGatePolicy.DEMAND_GATED,
                          capacity_planning.ReservationGatePolicy.UNGATED))
def test_unavailable_compatible_reservation_evidence_fails_closed(
        gate_policy) -> None:
    demand = (_demand(50, ('A100',), 3),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=gate_policy,
                evidence_state=(
                    capacity_planning.ReservationEvidenceState.UNAVAILABLE))))

    assert plan.new_reserved_capacity_committed.total() == 0
    assert plan.reserved_launch_target.total() == 0
    assert plan.paid_residual.total() == 0
    if gate_policy is capacity_planning.ReservationGatePolicy.DEMAND_GATED:
        assert plan.kind is capacity_planning.CapacityPlanKind.GATE_ACQUISITION
        assert plan.aggregate_demand_target == 3
    else:
        assert plan.kind is capacity_planning.CapacityPlanKind.INCOMPLETE


def test_unsettled_gate_preserves_existing_capacity_by_no_effect() -> None:
    demand = (_demand(50, ('A100',), 3),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            ready_zero_cost=_capacity(A100=1),
            ready=_capacity(A100=1),
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_UNSETTLED),
                authenticated=_capacity(A100=3),
                existing_zero_cost=_capacity(A100=1))))

    assert plan.kind is capacity_planning.CapacityPlanKind.GATE_ACQUISITION
    assert plan.reserved_capacity_committed.total() == 0
    assert plan.new_reserved_capacity_committed.total() == 0
    assert plan.paid_residual.total() == 0
    assert plan.retirement_floor_target.total() == 0


def test_disabled_fill_still_debits_surviving_pending_reserved_capacity(
) -> None:
    demand = (_demand(50, ('A100',), 3),)
    plan = capacity_planning.plan_capacity(
        _snapshot(demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  reservation=_reservation(pending=_capacity(A100=2))))

    assert plan.reserved_capacity_committed.as_dict() == {'A100': 2}
    assert plan.new_reserved_capacity_committed.total() == 0
    assert plan.reserved_launch_target.total() == 0
    assert plan.paid_residual.as_dict() == {'A100': 1}


def test_spot_only_inventory_is_visible_without_reservation_authority() -> None:
    demand = (_demand(50, ('A100',), 5),)
    plan = capacity_planning.plan_capacity(
        _snapshot(demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  ready_zero_cost=_capacity(A100=1),
                  ready=_capacity(A100=3),
                  reservation=_reservation(existing_zero_cost=_capacity(A100=1),
                                           existing_paid=_capacity(A100=2))))

    assert plan.reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.new_reserved_capacity_committed.total() == 0
    assert plan.paid_residual.as_dict() == {'A100': 2}


@pytest.mark.parametrize('gate_policy',
                         (capacity_planning.ReservationGatePolicy.UNGATED,
                          capacity_planning.ReservationGatePolicy.DEMAND_GATED))
def test_free_compatible_reservation_replaces_ready_paid_cross_card(
        gate_policy) -> None:
    demand = (_demand(50, ('L4', 'A100'), 1),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            ready=_capacity(L4=1),
            reservation=_reservation(
                gate_policy=gate_policy,
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=1),
                eligible=_capacity(A100=1),
                existing_paid=_capacity(L4=1))))

    # Demand attribution remains on the cheapest compatible cold card for
    # explanation. Actuation prefers the compatible free reservation, which
    # makes the paid L4 surplus eligible for normal retirement.
    assert plan.demand_attribution.as_dict() == {'L4': 1}
    assert plan.supply_aware_demand_target.as_dict() == {'A100': 1}
    assert plan.reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.new_reserved_capacity_committed.as_dict() == {'A100': 1}
    assert plan.reserved_launch_target.as_dict() == {'A100': 1}
    assert plan.paid_residual.total() == 0
    assert plan.paid_launch_target.total() == 0
    assert plan.wave_limited_actuation_target.as_dict() == {'A100': 1}


def test_small_reservation_gate_matrix_is_monotonic_and_order_invariant(
) -> None:
    """Exhaust the small production-planner reservation/gate state space."""
    compatibility_sets = (('L4',), ('A100',), ('L4', 'A100'))
    for (compatible, demand_count, pending_count, existing_zero_count,
         existing_paid_count) in itertools.product(compatibility_sets,
                                                   range(1, 4), range(3),
                                                   range(2), range(2)):
        demand = (_demand(50, compatible, demand_count),)
        inventory_card = compatible[-1]
        pending = _capacity(**{inventory_card: pending_count})
        existing_zero = _capacity(**{inventory_card: existing_zero_count})
        existing_paid = _capacity(**{inventory_card: existing_paid_count})
        ready_count = existing_zero_count + existing_paid_count
        common = {
            'demand_profiles': demand,
            'explicit_demand_profiles': demand,
            'paid_demand_profiles': demand,
            'ready_zero_cost': existing_zero,
            'ready': _capacity(**{inventory_card: ready_count}),
            'provisioning': pending,
        }

        disabled = capacity_planning.plan_capacity(
            _snapshot(**common,
                      reservation=_reservation(pending=pending,
                                               existing_zero_cost=existing_zero,
                                               existing_paid=existing_paid)))
        unsettled = capacity_planning.plan_capacity(
            _snapshot(
                **common,
                reservation=_reservation(
                    gate_policy=(
                        capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                    evidence_state=(capacity_planning.ReservationEvidenceState.
                                    AUTHENTICATED_UNSETTLED),
                    authenticated=_capacity(**{inventory_card: 2}),
                    pending=pending,
                    existing_zero_cost=existing_zero,
                    existing_paid=existing_paid)))
        assert unsettled.kind is (
            capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
        assert unsettled.demand_attribution == disabled.demand_attribution
        assert unsettled.paid_residual.total() == 0
        assert unsettled.wave_limited_actuation_target.total() == 0

        for supply_card in ('L4', 'A100'):
            paid_totals: list[int] = []
            for eligible_count in range(3):
                supply = _capacity(**{supply_card: eligible_count})
                gated_reservation = _reservation(
                    gate_policy=(
                        capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                    evidence_state=(capacity_planning.ReservationEvidenceState.
                                    AUTHENTICATED_SETTLED),
                    authenticated=supply,
                    eligible=supply,
                    pending=pending,
                    existing_zero_cost=existing_zero,
                    existing_paid=existing_paid)
                gated = capacity_planning.plan_capacity(
                    _snapshot(**common, reservation=gated_reservation))
                ungated = capacity_planning.plan_capacity(
                    _snapshot(**common,
                              reservation=_reservation(
                                  gate_policy=(capacity_planning.
                                               ReservationGatePolicy.UNGATED),
                                  evidence_state=(
                                      capacity_planning.ReservationEvidenceState
                                      .AUTHENTICATED_SETTLED),
                                  authenticated=supply,
                                  eligible=supply,
                                  pending=pending,
                                  existing_zero_cost=existing_zero,
                                  existing_paid=existing_paid)))
                # Gate-off may add static zero-cost fill, but it cannot change
                # the traffic debit or paid demand for the same eligible
                # reservation envelope.
                assert gated.supply_aware_demand_target == (
                    ungated.supply_aware_demand_target)
                assert gated.paid_residual == ungated.paid_residual
                paid_totals.append(gated.paid_residual.total())

                reversed_reservation = dataclasses.replace(
                    gated_reservation,
                    authenticated_capacity=capacity_planning.
                    AcceleratorCapacity(entries=tuple(
                        reversed(
                            gated_reservation.authenticated_capacity.entries))),
                    eligible_capacity=capacity_planning.AcceleratorCapacity(
                        entries=tuple(
                            reversed(
                                gated_reservation.eligible_capacity.entries))))
                reordered = capacity_planning.plan_capacity(
                    _snapshot(**common, reservation=reversed_reservation))
                assert reordered == gated

            if supply_card in compatible:
                assert paid_totals == sorted(paid_totals, reverse=True)
            else:
                assert len(set(paid_totals)) == 1


@pytest.mark.parametrize('field',
                         ('authenticated_capacity', 'eligible_capacity'))
def test_unconfigured_reservation_rejects_new_capacity_authority(field) -> None:
    values = {
        'gate_policy': capacity_planning.ReservationGatePolicy.NOT_CONFIGURED,
        'evidence_state':
            (capacity_planning.ReservationEvidenceState.NOT_APPLICABLE),
        'authenticated_capacity': _capacity(),
        'eligible_capacity': _capacity(),
        'pending_zero_cost_capacity': _capacity(),
        'existing_zero_cost_capacity': _capacity(A100=1),
        'existing_paid_capacity': _capacity(A100=2),
        'charged_paid_gpu_units': 2,
        'evidence_fingerprint': '',
    }
    values[field] = _capacity(A100=1)

    with pytest.raises(ValueError, match='reservation'):
        capacity_planning.ReservationPlanningInput(**values)


def test_gate_off_fresh_zero_has_explicit_static_prefill_projection() -> None:
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=(),
            explicit_demand_profiles=(),
            paid_demand_profiles=(),
            reservation=_reservation(
                gate_policy=(capacity_planning.ReservationGatePolicy.UNGATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=4),
                eligible=_capacity(A100=4))))

    assert plan.kind is capacity_planning.CapacityPlanKind.STATIC_PREFILL
    assert plan.aggregate_demand_target == 0
    assert plan.static_prefill_target.as_dict() == {'A100': 4}
    assert plan.reserved_launch_target.as_dict() == {'A100': 4}
    assert plan.reserved_packing_padding_target.as_dict() == {'A100': 4}
    assert plan.paid_residual.total() == 0


def test_gate_off_zero_demand_observer_blackout_preserves_proven_holdings(
) -> None:
    plan = capacity_planning.plan_capacity(
        _snapshot(
            planning_capacity_quantum_by_accelerator=_capacity(L4=1, A100=8),
            demand_profiles=(),
            explicit_demand_profiles=(),
            paid_demand_profiles=(),
            retirement_shelter_target=_capacity(A100=8),
            reservation=_reservation(
                gate_policy=capacity_planning.ReservationGatePolicy.UNGATED,
                evidence_state=(
                    capacity_planning.ReservationEvidenceState.UNAVAILABLE),
                existing_zero_cost=_capacity(A100=8))))

    assert plan.aggregate_demand_target == 0
    assert plan.reserved_launch_target.total() == 0
    assert plan.paid_launch_target.total() == 0
    assert plan.retirement_floor_target.as_dict() == {'A100': 8}


def test_gate_off_prefill_preserves_ceiling_for_disjoint_spot_demand() -> None:
    demand = (_demand(50, ('L4',), 2),)
    plan = capacity_planning.plan_capacity(
        _snapshot(
            maximum_capacity=4,
            demand_profiles=demand,
            explicit_demand_profiles=demand,
            paid_demand_profiles=demand,
            reservation=_reservation(
                gate_policy=capacity_planning.ReservationGatePolicy.UNGATED,
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=4),
                eligible=_capacity(A100=4))))

    assert plan.paid_launch_target.as_dict() == {'L4': 2}
    assert plan.static_prefill_target.as_dict() == {'A100': 2}
    assert (plan.paid_launch_target.total() +
            plan.static_prefill_target.total()) == 4


def test_fresh_zero_retention_and_ungated_prefill_are_orthogonal() -> None:
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=(),
            explicit_demand_profiles=(),
            paid_demand_profiles=(),
            actuation_minimum_capacity=2,
            ready_zero_cost=_capacity(A100=2),
            ready=_capacity(A100=2),
            planning_purpose=(
                capacity_planning.CapacityPlanningPurpose.FRESH_ZERO_RETENTION),
            reservation=_reservation(
                gate_policy=capacity_planning.ReservationGatePolicy.UNGATED,
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=8),
                eligible=_capacity(A100=8),
                existing_zero_cost=_capacity(A100=2))))

    assert plan.kind is capacity_planning.CapacityPlanKind.STATIC_PREFILL
    assert plan.retained_existing_target.as_dict() == {'A100': 2}
    assert plan.static_prefill_target.as_dict() == {'A100': 8}
    assert plan.reserved_launch_target.as_dict() == {'A100': 8}
    assert plan.reserved_packing_padding_target.as_dict() == {'A100': 8}


def test_static_prefill_preserves_independent_zero_retention_cooldown() -> None:
    state = _policy_state()
    prior = _prior_candidate(state, target=5)
    for generation, epoch in ((7, 100.0), (8, 130.0)):
        snapshot = _snapshot(
            source_generation=generation,
            maximum_capacity=20,
            demand_profiles=(),
            explicit_demand_profiles=(),
            paid_demand_profiles=(),
            minimum_capacity=0,
            paid_minimum_capacity=0,
            ready=_capacity(L4=5),
            planning_purpose=(
                capacity_planning.CapacityPlanningPurpose.FRESH_ZERO_RETENTION),
            prior_policy_state=state,
            prior_candidate=prior,
            policy_input=_policy_input(
                planning_db_epoch=epoch,
                downscale_delay_seconds=300,
                latest_committed_capacity=5,
                latest_committed_by_accelerator=_capacity(L4=5)),
            reservation=_reservation(
                gate_policy=capacity_planning.ReservationGatePolicy.UNGATED,
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=3),
                eligible=_capacity(A100=3),
                existing_paid=_capacity(L4=5)))
        plan = capacity_planning.plan_capacity(snapshot)
        assert plan.kind is capacity_planning.CapacityPlanKind.STATIC_PREFILL
        # Unmaterialized static prefill cannot substitute for the warm
        # backends protected by the independently configured cooldown.
        assert plan.retained_existing_target.as_dict() == {'L4': 5}
        assert plan.static_prefill_target.as_dict() == {'A100': 3}
        assert plan.paid_launch_target.total() == 0
        assert plan.next_policy_state is not None
        assert plan.next_policy_state.downscale_started_db_epoch == 70
        finalized = capacity_planning.finalize_capacity_plan(
            snapshot,
            plan,
            accepted_paid_plan_units=_capacity(),
            accepted_paid_gpu_units=0,
            decision_db_epoch=epoch)
        _, prior = capacity_planning.decode_planner_envelope(
            capacity_planning.planner_envelope(snapshot, finalized))
        assert prior.next_policy_state is not None
        state = prior.next_policy_state


def test_gate_on_fresh_zero_never_prefills_uncommitted_headroom() -> None:
    plan = capacity_planning.plan_capacity(
        _snapshot(
            demand_profiles=(),
            explicit_demand_profiles=(),
            paid_demand_profiles=(),
            reservation=_reservation(
                gate_policy=(
                    capacity_planning.ReservationGatePolicy.DEMAND_GATED),
                evidence_state=(capacity_planning.ReservationEvidenceState.
                                AUTHENTICATED_SETTLED),
                authenticated=_capacity(A100=4))))

    assert plan.kind is capacity_planning.CapacityPlanKind.DEMAND
    assert plan.static_prefill_target.total() == 0
    assert plan.paid_residual.total() == 0


def test_snapshot_canonicalizes_card_casing_to_configured_names() -> None:
    l4_demand = (_demand(20, ('l4',), 1),)
    plan = capacity_planning.plan_capacity(
        _snapshot(configured_accelerators=('L4',),
                  planning_capacity_quantum_by_accelerator=_capacity(l4=1),
                  capacity_per_accelerator=_work(l4=1),
                  demand_profiles=l4_demand,
                  explicit_demand_profiles=l4_demand,
                  paid_demand_profiles=l4_demand,
                  cold_accelerator_order=('l4',),
                  prospective_paid_accelerator_order=('l4',)))

    assert plan.aggregate_demand_target == 1
    assert plan.demand_attribution.as_dict() == {'L4': 1}


@pytest.mark.parametrize('invalid_cap', (True, -1, 1.5))
def test_snapshot_rejects_invalid_paid_gpu_cap(invalid_cap: object) -> None:
    with pytest.raises(ValueError, match='identity or bounds'):
        _snapshot(max_live_paid_gpu_units=invalid_cap)


def test_snapshot_rejects_fractional_backend_retirement_shelter() -> None:
    with pytest.raises(ValueError, match='whole-backend exact'):
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  retirement_shelter_target=_capacity(A100=7))


def test_incomplete_snapshot_returns_unique_fail_closed_plan() -> None:
    plan = capacity_planning.plan_capacity(
        _snapshot(attribution_complete=False))

    assert plan == capacity_planning.incomplete_capacity_plan(
        source_generation=7)


def test_deadline_uses_only_prospective_paid_accelerators() -> None:
    deadline = capacity_planning.DeadlinePlanningInput(
        demand=(autoscaler_compatibility.DeadlineDemand(
            sequence=0,
            priority=50,
            compatible_cards=('L4', 'A100'),
            count=1,
            remaining_seconds=60),),
        finite_supply=(),
        service_seconds_by_accelerator=_work(L4=10, A100=10),
        service_time_sources=(('L4', 'seed'), ('A100', 'seed')),
        utilization=1.0,
        paid_cold_lead_seconds=1.0)

    plan = capacity_planning.plan_capacity(
        _snapshot(demand_profiles=(),
                  explicit_demand_profiles=(),
                  paid_demand_profiles=(),
                  cold_accelerator_order=('A100', 'L4'),
                  prospective_paid_accelerator_order=('L4',),
                  deadline=deadline))

    assert plan.deadline_target.as_dict() == {'L4': 1}


def test_deadline_target_does_not_double_count_queued_demand() -> None:
    demand = (_demand(50, ('L4',), 1),)
    deadline = capacity_planning.DeadlinePlanningInput(
        demand=(autoscaler_compatibility.DeadlineDemand(
            sequence=0,
            priority=50,
            compatible_cards=('L4',),
            count=1,
            remaining_seconds=60),),
        finite_supply=(),
        service_seconds_by_accelerator=_work(L4=10, A100=10),
        service_time_sources=(('L4', 'seed'), ('A100', 'seed')),
        utilization=1.0,
        paid_cold_lead_seconds=1.0)

    plan = capacity_planning.plan_capacity(
        _snapshot(demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  deadline=deadline))

    assert plan.deadline_target.as_dict() == {'L4': 1}
    assert plan.demand_attribution.as_dict() == {'L4': 1}
    assert plan.supply_aware_demand_target.as_dict() == {'L4': 1}
    assert plan.paid_residual.as_dict() == {'L4': 1}


def test_fresh_zero_retention_carries_no_demand_or_padding() -> None:
    plan = capacity_planning.plan_capacity(
        _snapshot(
            planning_purpose=(
                capacity_planning.CapacityPlanningPurpose.FRESH_ZERO_RETENTION),
            minimum_capacity=0,
            paid_minimum_capacity=0,
            actuation_minimum_capacity=3,
            demand_profiles=(),
            explicit_demand_profiles=(),
            paid_demand_profiles=(),
            ready_zero_cost=_capacity(A100=3),
            ready=_capacity(A100=3),
            reservation=_reservation(existing_zero_cost=_capacity(A100=3))))

    assert plan.kind is capacity_planning.CapacityPlanKind.FRESH_ZERO_RETENTION
    assert plan.aggregate_demand_target == 0
    assert plan.demand_attribution.total() == 0
    assert plan.paid_demand_attribution.total() == 0
    assert plan.zero_cost_padding_target.total() == 0
    assert plan.retained_existing_target.as_dict() == {'A100': 3}
    assert plan.supply_aware_actuation_target.as_dict() == {'A100': 3}


def test_retirement_shelter_composes_same_card_without_double_counting(
) -> None:
    demand = (_demand(50, ('A100',), 1),)

    plan = capacity_planning.plan_capacity(
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  retirement_shelter_target=_capacity(A100=8)))

    assert plan.wave_limited_actuation_target.as_dict() == {'A100': 1}
    assert plan.retirement_floor_target.as_dict() == {'A100': 8}
    assert plan.paid_residual.as_dict() == {'A100': 1}


def test_retirement_shelter_composes_disjoint_card_without_creating_demand(
) -> None:
    demand = (_demand(50, ('L4',), 1),)

    plan = capacity_planning.plan_capacity(
        _snapshot(planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     A100=8),
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  retirement_shelter_target=_capacity(A100=8)))

    assert plan.wave_limited_actuation_target.as_dict() == {'L4': 1}
    assert plan.retirement_floor_target.as_dict() == {'A100': 8, 'L4': 1}
    assert plan.paid_residual.as_dict() == {'L4': 1}
    assert plan.paid_launch_target.as_dict() == {'L4': 1}


def test_disjoint_demand_evicts_unfittable_whole_backend_shelter() -> None:
    demand = (_demand(50, ('L4',), 3),)

    plan = capacity_planning.plan_capacity(
        _snapshot(configured_accelerators=('L4', 'H200'),
                  planning_capacity_quantum_by_accelerator=_capacity(L4=1,
                                                                     H200=8),
                  capacity_per_accelerator=_work(L4=1, H200=1),
                  maximum_capacity=10,
                  demand_profiles=demand,
                  explicit_demand_profiles=demand,
                  paid_demand_profiles=demand,
                  cold_accelerator_order=('L4', 'H200'),
                  prospective_paid_accelerator_order=('L4',),
                  retirement_shelter_target=_capacity(H200=8)))

    assert plan.wave_limited_actuation_target.as_dict() == {'L4': 3}
    assert plan.retirement_floor_target.as_dict() == {'L4': 3}
    assert plan.paid_launch_target.as_dict() == {'L4': 3}


def test_fresh_zero_retirement_floor_combines_retention_and_shelter() -> None:
    plan = capacity_planning.plan_capacity(
        _snapshot(
            planning_capacity_quantum_by_accelerator=_capacity(L4=1, A100=8),
            planning_purpose=(
                capacity_planning.CapacityPlanningPurpose.FRESH_ZERO_RETENTION),
            minimum_capacity=0,
            paid_minimum_capacity=0,
            actuation_minimum_capacity=2,
            demand_profiles=(),
            explicit_demand_profiles=(),
            paid_demand_profiles=(),
            ready_zero_cost=_capacity(L4=2),
            ready=_capacity(L4=2),
            reservation=_reservation(existing_zero_cost=_capacity(L4=2)),
            retirement_shelter_target=_capacity(A100=8)))

    assert plan.retained_existing_target.as_dict() == {'L4': 2}
    assert plan.retirement_floor_target.as_dict() == {'A100': 8, 'L4': 2}
    assert plan.paid_residual.total() == 0
    assert plan.paid_launch_target.total() == 0


def test_planner_does_not_mutate_snapshot() -> None:
    snapshot = _snapshot(reservation=_reservation(
        gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
        evidence_state=(
            capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED),
        authenticated=_capacity(A100=2),
        eligible=_capacity(A100=2)),
                         retention_work=_work(A100=1))
    before = dataclasses.asdict(snapshot)

    capacity_planning.plan_capacity(snapshot)

    assert dataclasses.asdict(snapshot) == before
    with pytest.raises(dataclasses.FrozenInstanceError):
        snapshot.minimum_capacity = 4  # type: ignore[misc]


def test_capacity_planning_has_no_positional_schema_dispatch() -> None:
    source = inspect.getsource(capacity_planning)
    forbidden = ('len(logical_target)', 'logical_target[',
                 'isinstance(logical_target, tuple)')
    assert all(marker not in source for marker in forbidden)


def test_autoscaler_has_no_mutable_economic_planning_wrapper() -> None:
    source = inspect.getsource(autoscalers.ConcurrencyAutoscaler)
    assert 'economic_capacity_plan' not in source
    assert 'existing_capacity_retention_target_by_accelerator' not in source
    assert '_capacity_planning_snapshot' not in source
    assert '_economic_capacity_planning_snapshot' not in source
    parameters = inspect.signature(
        autoscalers.ConcurrencyAutoscaler.
        _calculate_concurrency_target_by_accelerator).parameters
    assert set(parameters) == {
        'self', 'replica_infos', 'target_ceiling', 'min_replicas_override',
        'purpose'
    }


def test_capacity_planning_records_are_keyword_only_and_deeply_immutable(
) -> None:
    records = (
        capacity_planning.AcceleratorCapacity,
        capacity_planning.AcceleratorWork,
        capacity_planning.CompatibilityDemand,
        capacity_planning.DemandObservationReconciliation,
        capacity_planning.DeadlinePlanningInput,
        capacity_planning.ReservationPlanningInput,
        capacity_planning.PaidCapProjection,
        capacity_planning.CapacityPolicyState,
        capacity_planning.CapacityPolicyInput,
        capacity_planning.CapacityPlanningSnapshot,
        capacity_planning.CapacityPlanCandidate,
        capacity_planning.CapacityPlanningEnvelope,
    )
    for record in records:
        parameters = inspect.signature(record).parameters.values()
        assert all(parameter.kind is inspect.Parameter.KEYWORD_ONLY
                   for parameter in parameters)
        assert record.__dataclass_params__.frozen  # type: ignore[attr-defined]


def _planner_payload() -> dict[str, object]:
    snapshot = _snapshot(reservation=_reservation(
        gate_policy=capacity_planning.ReservationGatePolicy.DEMAND_GATED,
        evidence_state=(
            capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED),
        authenticated=_capacity(A100=2),
        eligible=_capacity(A100=2)))
    candidate = capacity_planning.plan_capacity(snapshot)
    return json.loads(
        json.dumps(capacity_planning.planner_envelope(snapshot, candidate)))


def test_planner_envelope_round_trips_frozen_typed_records() -> None:
    payload = _planner_payload()

    snapshot, candidate = capacity_planning.decode_planner_envelope(payload)

    assert payload == capacity_planning.planner_envelope(snapshot, candidate)
    assert candidate.snapshot_fingerprint == snapshot.fingerprint
    assert snapshot.reservation.eligible_capacity.as_dict() == {'A100': 2}
    assert candidate.reservation_acquisition_classes == (_acquisition(
        20, ('L4', 'A100'), 2),)
    with pytest.raises(dataclasses.FrozenInstanceError):
        snapshot.service_version = 4  # type: ignore[misc]


def test_planner_envelope_round_trips_paid_cap_and_retirement_floor() -> None:
    demand = (_demand(50, ('L4',), 2),)
    snapshot = _snapshot(demand_profiles=demand,
                         explicit_demand_profiles=demand,
                         paid_demand_profiles=demand,
                         max_live_paid_gpu_units=3,
                         retirement_shelter_target=_capacity(A100=2))
    candidate = capacity_planning.plan_capacity(snapshot)

    decoded_snapshot, decoded_candidate = (
        capacity_planning.decode_planner_envelope(
            capacity_planning.planner_envelope(snapshot, candidate)))

    assert decoded_snapshot.max_live_paid_gpu_units == 3
    assert decoded_candidate.paid_cap.remaining_paid_gpu_units == 3
    assert decoded_candidate.retirement_floor_target.as_dict() == {
        'A100': 2,
        'L4': 2,
    }


@pytest.mark.parametrize('missing_field',
                         ('schema_version', 'snapshot', 'candidate',
                          'snapshot_fingerprint', 'candidate_fingerprint'))
def test_planner_envelope_rejects_missing_top_level_field(
        missing_field: str) -> None:
    payload = _planner_payload()
    del payload[missing_field]

    with pytest.raises(ValueError, match='missing or unexpected'):
        capacity_planning.decode_planner_envelope(payload)


def test_planner_envelope_rejects_extra_nested_field() -> None:
    payload = _planner_payload()
    snapshot = payload['snapshot']
    assert isinstance(snapshot, dict)
    reservation = snapshot['reservation']
    assert isinstance(reservation, dict)
    reservation['legacy_gate'] = True

    with pytest.raises(ValueError, match='missing or unexpected'):
        capacity_planning.decode_planner_envelope(payload)


def test_planner_envelope_rejects_missing_nested_field() -> None:
    payload = _planner_payload()
    snapshot = payload['snapshot']
    assert isinstance(snapshot, dict)
    reservation = snapshot['reservation']
    assert isinstance(reservation, dict)
    del reservation['existing_paid_capacity']

    with pytest.raises(ValueError, match='missing or unexpected'):
        capacity_planning.decode_planner_envelope(payload)


@pytest.mark.parametrize(('field', 'invalid_value'), (
    ('schema_version', True),
    ('snapshot_fingerprint', 'F' * 64),
    ('candidate_fingerprint', '0' * 63),
))
def test_planner_envelope_rejects_invalid_schema_and_digests(
        field: str, invalid_value: object) -> None:
    payload = _planner_payload()
    payload[field] = invalid_value

    with pytest.raises(ValueError):
        capacity_planning.decode_planner_envelope(payload)


@pytest.mark.parametrize('old_schema', (1, 2, 3, 4, 5))
def test_format_six_strictly_rejects_old_envelopes(old_schema: int) -> None:
    payload = _planner_payload()
    payload['schema_version'] = old_schema
    snapshot = payload['snapshot']
    candidate = payload['candidate']
    assert isinstance(snapshot, dict)
    assert isinstance(candidate, dict)
    del snapshot['backend_num_nodes']
    del candidate['backend_num_nodes']

    with pytest.raises(ValueError, match='Unsupported.*schema'):
        capacity_planning.decode_planner_envelope(payload)


@pytest.mark.parametrize('record', ('snapshot', 'candidate'))
def test_format_six_requires_backend_node_count(record: str) -> None:
    payload = _planner_payload()
    nested = payload[record]
    assert isinstance(nested, dict)
    del nested['backend_num_nodes']

    with pytest.raises(ValueError, match='missing or unexpected'):
        capacity_planning.decode_planner_envelope(payload)


def test_format_six_requires_acquisition_classes() -> None:
    payload = _planner_payload()
    candidate = payload['candidate']
    assert isinstance(candidate, dict)
    del candidate['reservation_acquisition_classes']

    with pytest.raises(ValueError, match='missing or unexpected'):
        capacity_planning.decode_planner_envelope(payload)


def test_planner_envelope_rejects_bool_as_integer_and_unknown_enum() -> None:
    bool_payload = _planner_payload()
    bool_snapshot = bool_payload['snapshot']
    assert isinstance(bool_snapshot, dict)
    bool_snapshot['maximum_capacity'] = True

    with pytest.raises(ValueError, match='must be an integer'):
        capacity_planning.decode_planner_envelope(bool_payload)

    enum_payload = _planner_payload()
    enum_snapshot = enum_payload['snapshot']
    assert isinstance(enum_snapshot, dict)
    enum_snapshot['planning_purpose'] = 'legacy-economic-mode'

    with pytest.raises(ValueError, match='supported enum'):
        capacity_planning.decode_planner_envelope(enum_payload)


def test_planner_envelope_rejects_invalid_card_map_value() -> None:
    payload = _planner_payload()
    snapshot = payload['snapshot']
    assert isinstance(snapshot, dict)
    widths = snapshot['planning_capacity_quantum_by_accelerator']
    assert isinstance(widths, dict)
    entries = widths['entries']
    assert isinstance(entries, list)
    entries[0][1] = True

    with pytest.raises(ValueError, match='must be an integer'):
        capacity_planning.decode_planner_envelope(payload)


def test_planner_envelope_rejects_corrupt_paid_cap_metadata() -> None:
    payload = _planner_payload()
    candidate = payload['candidate']
    assert isinstance(candidate, dict)
    paid_cap = candidate['paid_cap']
    assert isinstance(paid_cap, dict)
    paid_cap['remaining_paid_gpu_units'] = 3

    with pytest.raises(ValueError, match='Paid-cap remaining'):
        capacity_planning.decode_planner_envelope(payload)


def test_planner_envelope_rejects_missing_retirement_projection() -> None:
    payload = _planner_payload()
    candidate = payload['candidate']
    assert isinstance(candidate, dict)
    del candidate['retirement_floor_target']

    with pytest.raises(ValueError, match='missing or unexpected'):
        capacity_planning.decode_planner_envelope(payload)


def test_planner_envelope_rejects_snapshot_and_candidate_corruption() -> None:
    snapshot_payload = _planner_payload()
    snapshot = snapshot_payload['snapshot']
    assert isinstance(snapshot, dict)
    snapshot['planning_time'] = 1001.0

    with pytest.raises(ValueError,
                       match='different snapshot|fingerprint disagrees'):
        capacity_planning.decode_planner_envelope(snapshot_payload)

    candidate_payload = _planner_payload()
    candidate = candidate_payload['candidate']
    assert isinstance(candidate, dict)
    candidate['source_generation'] = 8

    with pytest.raises(ValueError, match='different generation'):
        capacity_planning.decode_planner_envelope(candidate_payload)


def test_planner_envelope_rejects_noncanonical_card_order() -> None:
    payload = copy.deepcopy(_planner_payload())
    snapshot = payload['snapshot']
    assert isinstance(snapshot, dict)
    widths = snapshot['planning_capacity_quantum_by_accelerator']
    assert isinstance(widths, dict)
    entries = widths['entries']
    assert isinstance(entries, list)
    entries.reverse()

    with pytest.raises(ValueError, match='not canonical'):
        capacity_planning.decode_planner_envelope(payload)


def test_planner_encoder_rejects_candidate_from_another_snapshot() -> None:
    snapshot = _snapshot()
    candidate = capacity_planning.plan_capacity(snapshot)
    other_snapshot = dataclasses.replace(snapshot, planning_time=1001.0)

    with pytest.raises(ValueError, match='different snapshot'):
        capacity_planning.planner_envelope(other_snapshot, candidate)
