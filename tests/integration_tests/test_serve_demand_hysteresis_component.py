"""Component integration of configured demand hysteresis and durable planning.

The entry point is ``ConcurrencyAutoscaler.plan_durable_capacity_reconcile``,
the production scheduling boundary used by the logical paid controller. Real
service-YAML decoding, replica records, locked-input binding, allocation, policy
reduction, decision generation, and planner-envelope serialization remain
intact. Only the adjacent repository's already-locked observations and clock
are supplied as input data. No autoscaler/planner method is mocked.

This is not an unpaid provider E2E: it does not run PostgreSQL, the controller
process, provider provisioning, load-balancer dispatch, or physical teardown.
"""

import dataclasses

import pytest

from sky.serve import autoscalers
from sky.serve import capacity_planning
from sky.serve import constants
from sky.serve import kueue_lane_capacity
from sky.serve import replica_info
from sky.serve import service_spec
from sky.utils import common_utils

pytestmark = pytest.mark.component

_SERVICE_NAME = 'demand-hysteresis-component'
_GPU_SLOTS = 1000
_DB_EPOCH = 1_000_000_000.0


def _autoscaler(delay_seconds: int,
                downscale_rate: int,
                overprovision: int = 0) -> autoscalers.ConcurrencyAutoscaler:
    spec = service_spec.SkyServiceSpec.from_yaml_config({
        'readiness_probe': '/',
        'load_balancing_policy': 'instance_aware_least_load',
        'graceful_drain_async_occupancy': True,
        'replica_policy': {
            'min_replicas': 0,
            'max_replicas': _GPU_SLOTS,
            'spot_placer': 'dynamic_fallback_per_gpu',
            'target_concurrency_per_replica': 1,
            'upscale_delay_seconds': 0,
            'downscale_delay_seconds': delay_seconds,
            'max_scale_down_rate_percentage': downscale_rate,
            'adaptive_demand_estimation': False,
            'num_overprovision': overprovision,
        },
    })
    scaler = autoscalers.ConcurrencyAutoscaler(_SERVICE_NAME, spec, version=1)
    scaler.set_configured_accelerator_shapes({'L4': 1})
    assert scaler.replica_unit == 'logical'
    assert scaler.downscale_delay_seconds == delay_seconds
    return scaler


def _ready_replicas(gpus_per_machine: int) -> list[replica_info.ReplicaInfo]:
    replicas = []
    for replica_id in range(1, _GPU_SLOTS // gpus_per_machine + 1):
        replica = replica_info.ReplicaInfo(
            replica_id=replica_id,
            cluster_name=f'hysteresis-{replica_id}',
            replica_port='8080',
            is_spot=True,
            location=None,
            version=1,
            resources_override={'accelerators': {
                'L4': gpus_per_machine
            }},
            planned_capacity=gpus_per_machine)
        replica.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        replica.status_property.service_ready_now = True
        replica.status_property.first_ready_time = _DB_EPOCH - 1
        replicas.append(replica)
    return replicas


@dataclasses.dataclass
class _PlanningSequence:
    """Repository input/output seam, without a surrogate capacity allocator."""

    scaler: autoscalers.ConcurrencyAutoscaler
    prior_state: capacity_planning.CapacityPolicyState
    prior_candidate: capacity_planning.CapacityPlanCandidate
    delay_seconds: int
    downscale_rate: int
    overprovision: int = 0
    generation: int = 0

    @classmethod
    def create(cls,
               delay_seconds: int,
               downscale_rate: int = 100,
               overprovision: int = 0) -> '_PlanningSequence':
        state, candidate = capacity_planning.genesis_capacity_policy(
            service_name=_SERVICE_NAME,
            service_version=1,
            last_reduced_demand_generation=0,
            capacity_unit=capacity_planning.CapacityUnit.LOGICAL_GPU,
            maximum_capacity=_GPU_SLOTS,
            planning_capacity_quantum_by_accelerator=(
                capacity_planning.AcceleratorCapacity.from_mapping({'L4': 1})))
        return cls(_autoscaler(delay_seconds, downscale_rate,
                               overprovision), state, candidate, delay_seconds,
                   downscale_rate, overprovision)

    def tick(
        self,
        seconds: float,
        queue_depth: int,
        replicas: list[replica_info.ReplicaInfo],
        *,
        fresh_zero: bool = False,
    ) -> autoscalers.DurableCapacityReconcilePlan:
        self.generation += 1
        report = {
            'timestamps': [],
            'in_flight_by_replica_id': {
                r.replica_id: 0 for r in replicas
            },
            'queue_depth': queue_depth,
            'rejected_in_window': 0,
            'unknown_in_flight_replica_ids': [],
            'observed_slots_by_replica_id': {
                r.replica_id: r.planned_capacity for r in replicas
            },
            'unknown_capacity_replica_ids': [],
            'reconcile_generation': self.generation,
            'compatibility_profiles': [],
            'queued_requests_by_compatibility': [{
                'priority': 0,
                'compatible_accelerators': ['L4'],
                'count': queue_depth,
            }] if queue_depth else [],
            'rejected_requests_by_compatibility': [],
            'compatibility_demand_complete': True,
        }
        inputs = autoscalers.bind_locked_capacity_planning_inputs(
            autoscalers.ScalingDecisionInputs(
                gpu_shape_handles={},
                historical_scaling_values={},
                cold_paid_accelerator_order=('L4',),
                prospective_paid_accelerator_order=('L4',)), replicas,
            kueue_lane_capacity.KueueReplicaCapacitySnapshot({}))
        empty = capacity_planning.AcceleratorCapacity()
        committed = sum(r.planned_capacity for r in replicas)
        reservation = capacity_planning.ReservationPlanningInput(
            gate_policy=capacity_planning.ReservationGatePolicy.NOT_CONFIGURED,
            evidence_state=(
                capacity_planning.ReservationEvidenceState.NOT_APPLICABLE),
            authenticated_capacity=empty,
            eligible_capacity=empty,
            pending_zero_cost_capacity=empty,
            existing_zero_cost_capacity=empty,
            existing_paid_capacity=(
                capacity_planning.AcceleratorCapacity.from_mapping(
                    {'L4': committed} if committed else {})),
            charged_paid_gpu_units=committed,
            evidence_fingerprint='')
        result = self.scaler.plan_durable_capacity_reconcile(
            replicas,
            report,
            reservation,
            source_fingerprint='f' * 64,
            decision_inputs=inputs,
            retirement_shelter=None,
            max_live_paid_gpu_units=_GPU_SLOTS,
            prior_policy_state=self.prior_state,
            prior_candidate=self.prior_candidate,
            planning_db_epoch=_DB_EPOCH + seconds,
            fresh_zero=fresh_zero)
        assert result is not None
        if fresh_zero:
            assert result.envelope.candidate.aggregate_demand_target == 0
            assert result.envelope.candidate.demand_attribution.total() == 0
            assert result.envelope.candidate.paid_residual.total() == 0
            assert result.envelope.candidate.paid_launch_target.total() == 0
            assert all(decision.operator is not (
                autoscalers.AutoscalerDecisionOperator.SCALE_UP)
                       for decision in result.scaling_decisions)
        candidate = capacity_planning.finalize_capacity_plan(
            result.envelope.snapshot,
            result.envelope.candidate,
            accepted_paid_plan_units=empty,
            accepted_paid_gpu_units=0,
            decision_db_epoch=_DB_EPOCH + seconds)
        # The successor consumes only the serialized prior committed result,
        # never mutable state retained by the autoscaler instance.
        _, decoded = capacity_planning.decode_planner_envelope(
            capacity_planning.planner_envelope(result.envelope.snapshot,
                                               candidate))
        assert decoded.next_policy_state is not None
        self.prior_candidate = decoded
        self.prior_state = decoded.next_policy_state
        # Simulate process-local autoscaler loss after every committed tick.
        # A retained mutable timer would make the test pass without proving
        # that the actual durable policy handoff carries the cooldown.
        self.scaler = _autoscaler(self.delay_seconds, self.downscale_rate,
                                  self.overprovision)
        return result


@pytest.mark.parametrize('delay_seconds', [60, 300])
@pytest.mark.parametrize('gpus_per_machine', [1, 8])
def test_demand_rebound_restarts_configured_downscale_window(
        delay_seconds: int, gpus_per_machine: int) -> None:
    sequence = _PlanningSequence.create(delay_seconds)
    initial = sequence.tick(0, _GPU_SLOTS, [])
    assert initial.logical_target.target_capacity == _GPU_SLOTS
    replicas = _ready_replicas(gpus_per_machine)
    assert len(replicas) * gpus_per_machine == _GPU_SLOTS
    interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
    quiet_start = float(interval)
    quiet_end = quiet_start + delay_seconds - interval
    first_zero = sequence.tick(quiet_start, 0, replicas, fresh_zero=True)
    assert first_zero.logical_retirement_floor.target_capacity == _GPU_SLOTS
    before_rebound = sequence.tick(quiet_end - 1, 0, replicas, fresh_zero=True)
    assert before_rebound.logical_retirement_floor.target_capacity == _GPU_SLOTS
    rebound = sequence.tick(quiet_end, _GPU_SLOTS, replicas)
    assert rebound.logical_retirement_floor.target_capacity == _GPU_SLOTS
    second_start = quiet_end + interval
    second_end = second_start + delay_seconds - interval
    again = sequence.tick(second_start, 0, replicas, fresh_zero=True)
    assert again.logical_retirement_floor.target_capacity == _GPU_SLOTS
    held = sequence.tick(second_end - 1, 0, replicas, fresh_zero=True)
    assert held.logical_retirement_floor.target_capacity == _GPU_SLOTS
    drained = sequence.tick(second_end, 0, replicas, fresh_zero=True)
    assert drained.logical_retirement_floor.target_capacity == 0
    victims = [
        d.target.replica_id
        for d in drained.scaling_decisions
        if d.operator is autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
    ]
    assert set(victims) == {r.replica_id for r in replicas}


def test_sustained_zero_retires_half_capacity_per_full_configured_window(
) -> None:
    delay_seconds = 60
    interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
    sequence = _PlanningSequence.create(delay_seconds, downscale_rate=50)
    sequence.tick(0, _GPU_SLOTS, [])
    replicas = _ready_replicas(1)
    seconds = 0.0
    targets = []
    while replicas:
        previous_capacity = len(replicas)
        seconds += interval
        first_zero = sequence.tick(seconds, 0, replicas, fresh_zero=True)
        assert first_zero.logical_retirement_floor.target_capacity == (
            previous_capacity)
        seconds += delay_seconds - interval
        reduced = sequence.tick(seconds, 0, replicas, fresh_zero=True)
        target = reduced.logical_retirement_floor.target_capacity
        assert target == previous_capacity // 2
        targets.append(target)
        victims = {
            decision.target.replica_id
            for decision in reduced.scaling_decisions
            if decision.operator is (
                autoscalers.AutoscalerDecisionOperator.SCALE_DOWN)
        }
        assert len(victims) == previous_capacity - target
        # The provider/retirement owner is outside this component boundary.
        # Supply the next locked replica projection after it settles the wave.
        replicas = [r for r in replicas if r.replica_id not in victims]
    assert targets == [500, 250, 125, 62, 31, 15, 7, 3, 1, 0]


def test_zero_delay_is_a_negative_control_for_retention() -> None:
    sequence = _PlanningSequence.create(delay_seconds=0)
    initial = sequence.tick(0, _GPU_SLOTS, [])
    assert initial.logical_target.target_capacity == _GPU_SLOTS
    zero = sequence.tick(20, 0, _ready_replicas(8), fresh_zero=True)
    assert zero.logical_retirement_floor.target_capacity == 0


def test_zero_retains_only_existing_supply_after_partial_rebound() -> None:
    sequence = _PlanningSequence.create(delay_seconds=60, downscale_rate=50)
    sequence.tick(0, _GPU_SLOTS, [])
    replicas = _ready_replicas(1)
    sequence.tick(20, 0, replicas, fresh_zero=True)
    reduced = sequence.tick(60, 0, replicas, fresh_zero=True)
    victims = {
        d.target.replica_id
        for d in reduced.scaling_decisions
        if d.operator is autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
    }
    replicas = [r for r in replicas if r.replica_id not in victims]
    assert len(replicas) == 500
    rebound = sequence.tick(80, 750, replicas)
    assert rebound.logical_target.target_capacity == 750
    assert rebound.envelope.candidate.paid_residual.total() == 250
    # No replacement has materialized yet. Zero revokes its launch authority
    # immediately, while retaining only the 500 existing slots under cooldown.
    first_zero = sequence.tick(100, 0, replicas, fresh_zero=True)
    assert first_zero.logical_retirement_floor.target_capacity == 500
    still_held = sequence.tick(139, 0, replicas, fresh_zero=True)
    assert still_held.logical_retirement_floor.target_capacity == 500
    next_wave = sequence.tick(140, 0, replicas, fresh_zero=True)
    assert next_wave.logical_retirement_floor.target_capacity == 250


def test_retention_does_not_readd_overprovision_padding() -> None:
    sequence = _PlanningSequence.create(delay_seconds=300, overprovision=10)
    initial = sequence.tick(0, 100, [])
    assert initial.logical_target.target_capacity == 110
    # Existing supply exceeds the retained target so an incorrect repeated
    # padding addition cannot hide behind the existing-supply clamp.
    replicas = _ready_replicas(1)[:150]
    for seconds in (20, 40, 60, 299):
        held = sequence.tick(seconds, 0, replicas, fresh_zero=True)
        assert held.logical_retirement_floor.target_capacity == 110
