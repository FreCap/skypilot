"""Autoscalers: perform autoscaling by monitoring metrics."""
import bisect
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
import contextlib
import copy
import dataclasses
import hashlib
import json
import math
import threading
import time
import typing
from typing import Any
import uuid

from sky import global_user_state
from sky import sky_logging
from sky.jobs import state as managed_job_state
from sky.serve import async_request_ledger
from sky.serve import autoscaler_compatibility
from sky.serve import autoscaler_decisions
from sky.serve import capacity_planning
from sky.serve import constants
from sky.serve import kueue_lane_capacity
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.utils import common_utils
from sky.utils import operator_notifications

if typing.TYPE_CHECKING:
    from sky.serve import replica_managers
    from sky.serve import service_spec

logger = sky_logging.init_logger(__name__)

AutoscalerDecisionOperator: typing.TypeAlias = (
    autoscaler_decisions.AutoscalerDecisionOperator)
AutoscalerDecisionReason: typing.TypeAlias = (
    autoscaler_decisions.AutoscalerDecisionReason)
LogicalCapacityTarget: typing.TypeAlias = (
    autoscaler_decisions.LogicalCapacityTarget)
LogicalScaleTarget: typing.TypeAlias = autoscaler_decisions.LogicalScaleTarget
LogicalScaleDownTarget: typing.TypeAlias = (
    autoscaler_decisions.LogicalScaleDownTarget)
UnrecoverableRolloutFailure: typing.TypeAlias = (
    autoscaler_decisions.UnrecoverableRolloutFailure)
FillDemandSample: typing.TypeAlias = autoscaler_decisions.FillDemandSample
AutoscalerDecision: typing.TypeAlias = autoscaler_decisions.AutoscalerDecision


class PreparedReplicaSnapshotChanged(ValueError):
    """A blocking planning preload no longer names the locked replica rows."""


@dataclasses.dataclass(frozen=True, kw_only=True)
class PreparedReplicaPlanningBinding:
    """Immutable identity and exact shape for one prepared replica input."""

    replica_id: int
    replica_record_id: uuid.UUID
    service_version: int
    cluster_name: str
    exact_gpu_shape: tuple[str, int] | None
    durable_gpu_shape: tuple[str, int] | None
    planned_capacity: int


@dataclasses.dataclass(frozen=True)
class ScalingDecisionInputs:
    """Blocking inputs prepared for one exact autoscaler replica snapshot.

    Controller routing publication must never wait on provider or database
    I/O. Shape-aware autoscalers therefore resolve durable cluster handles and
    historical capacity metadata before the controller enters its
    routing-epoch lock, then consume this token without another state-store
    read inside that lock.
    """

    replica_bindings: tuple[PreparedReplicaPlanningBinding, ...] = ()
    gpu_shape_handles: dict[int, Any] | None = None
    gpu_shapes_by_replica_id: dict[int, tuple[str, int]] = (dataclasses.field(
        default_factory=dict))
    historical_scaling_values: dict[int, Any] | None = None
    kueue_capacity_by_replica_id: dict[
        int,
        kueue_lane_capacity.KueueReplicaCapacityClass] = (dataclasses.field(
            default_factory=dict))
    kueue_blocked_retirement_shapes: frozenset[tuple[str, int]] = frozenset()
    kueue_transition_replica_ids: frozenset[int] = frozenset()
    kueue_ready_paid_replacement_replica_ids: frozenset[int] = frozenset()
    service_time_estimates_by_accelerator: dict[str,
                                                dict[str, float |
                                                     int]] = dataclasses.field(
                                                         default_factory=dict)
    cold_paid_accelerator_order: tuple[str, ...] = ()
    prospective_paid_accelerator_order: tuple[str, ...] = ()

    @property
    def replica_ids(self) -> tuple[int, ...]:
        """Return the one canonical identity tuple's numeric projections."""
        return tuple(binding.replica_id for binding in self.replica_bindings)


def _canonical_exact_gpu_shape(raw_shape: Any) -> tuple[str, int] | None:
    if raw_shape is None:
        return None
    if (not isinstance(raw_shape, tuple) or len(raw_shape) != 2 or
            not isinstance(raw_shape[0], str) or not raw_shape[0] or
            type(raw_shape[1]) is not int or raw_shape[1] < 1):
        raise ValueError('Replica planning shape is malformed.')
    return raw_shape[0].casefold(), raw_shape[1]


def _durable_exact_gpu_shape(
    info: 'replica_managers.ReplicaInfo',) -> tuple[str, int] | None:
    return spot_placer.durable_exact_accelerator_shape(
        getattr(info, 'location', None),
        getattr(info, 'resources_override', None))


def build_replica_planning_bindings(
    replica_infos: Sequence['replica_managers.ReplicaInfo'],
    gpu_shapes_by_replica_id: Mapping[int, tuple[str, int]],
) -> tuple[PreparedReplicaPlanningBinding, ...]:
    """Build the canonical ABA-safe identity for one prepared row set."""
    bindings = []
    for info in replica_infos:
        replica_id = getattr(info, 'replica_id', None)
        raw_record_id = getattr(info, 'replica_record_id', None)
        service_version = getattr(info, 'version', None)
        cluster_name = getattr(info, 'cluster_name', None)
        if (type(replica_id) is not int or replica_id < 1 or
                type(service_version) is not int or service_version < 0 or
                not isinstance(cluster_name, str) or not cluster_name):
            raise ValueError('Replica planning identity is malformed.')
        try:
            record_id = uuid.UUID(str(raw_record_id))
        except (TypeError, ValueError, AttributeError) as error:
            raise ValueError(
                'Replica planning record identity is malformed.') from error
        planned_capacity = getattr(info, 'planned_capacity', None)
        if type(planned_capacity) is not int or planned_capacity < 1:
            raise ValueError('Replica planned capacity is malformed.')
        shape = _canonical_exact_gpu_shape(
            gpu_shapes_by_replica_id.get(replica_id))
        durable_shape = _durable_exact_gpu_shape(info)
        if durable_shape is not None and durable_shape != shape:
            raise ValueError('Replica durable and prepared shapes differ.')
        bindings.append(
            PreparedReplicaPlanningBinding(replica_id=replica_id,
                                           replica_record_id=record_id,
                                           service_version=service_version,
                                           cluster_name=cluster_name,
                                           exact_gpu_shape=shape,
                                           durable_gpu_shape=durable_shape,
                                           planned_capacity=planned_capacity))
    if len({binding.replica_id for binding in bindings}) != len(bindings):
        raise ValueError('Replica planning bindings contain duplicate ids.')
    return tuple(sorted(bindings, key=lambda binding: binding.replica_id))


def replica_planning_binding_fingerprint(
        decision_inputs: ScalingDecisionInputs) -> str:
    """Hash only immutable prepared facts consumed after blocking preload."""
    bindings = decision_inputs.replica_bindings
    if not isinstance(bindings, tuple):
        raise ValueError('Replica planning bindings are malformed.')
    material = [{
        'replica_id': binding.replica_id,
        'replica_record_id': str(binding.replica_record_id),
        'service_version': binding.service_version,
        'cluster_name': binding.cluster_name,
        'exact_gpu_shape': binding.exact_gpu_shape,
        'durable_gpu_shape': binding.durable_gpu_shape,
        'planned_capacity': binding.planned_capacity,
    } for binding in bindings]
    encoded = json.dumps(material, sort_keys=True,
                         separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _exact_gpu_shape_from_decision_inputs(
    info: 'replica_managers.ReplicaInfo',
    decision_inputs: ScalingDecisionInputs,
) -> tuple[str, int] | None:
    """Resolve an exact shape without I/O from one prepared input token."""
    raw_shape = _durable_exact_gpu_shape(info)
    if raw_shape is None:
        raw_shape = decision_inputs.gpu_shapes_by_replica_id.get(
            info.replica_id)
    if raw_shape is None:
        handles = decision_inputs.gpu_shape_handles
        handle = None if handles is None else handles.get(info.replica_id)
        accelerators = getattr(getattr(handle, 'launched_resources', None),
                               'accelerators', None)
        if isinstance(accelerators, Mapping) and len(accelerators) == 1:
            raw_card, raw_count = next(iter(accelerators.items()))
            raw_shape = (raw_card, raw_count)
    if (not isinstance(raw_shape, tuple) or len(raw_shape) != 2 or
            not isinstance(raw_shape[0], str) or not raw_shape[0]):
        return None
    try:
        count = int(raw_shape[1])
    except (TypeError, ValueError):
        return None
    if isinstance(raw_shape[1], bool) or count < 1:
        return None
    return raw_shape[0].casefold(), count


def bind_locked_kueue_capacity_snapshot(
    decision_inputs: ScalingDecisionInputs,
    replica_infos: list['replica_managers.ReplicaInfo'],
    snapshot: kueue_lane_capacity.KueueReplicaCapacitySnapshot,
) -> ScalingDecisionInputs:
    """Replace pre-lock scheduler observations with one locked typed source.

    The returned token is the only Kueue authority consumed by durable local
    planning.  Exact UNKNOWN scopes remain committed and retirement-blocked;
    only an unbounded scope adds the service-wide ``('*', 0)`` barrier.
    Replacement-surge victim protection is derived from this same snapshot.
    """
    if (not isinstance(decision_inputs, ScalingDecisionInputs) or
            not isinstance(snapshot,
                           kueue_lane_capacity.KueueReplicaCapacitySnapshot)):
        raise TypeError('Locked Kueue capacity binding is malformed.')
    replica_ids = tuple(info.replica_id for info in replica_infos)
    prepared_replica_ids = decision_inputs.replica_ids
    if (not isinstance(prepared_replica_ids, tuple) or any(
            type(replica_id) is not int or replica_id < 1
            for replica_id in (*prepared_replica_ids, *replica_ids)) or
            len(set(prepared_replica_ids)) != len(prepared_replica_ids) or
            len(set(replica_ids)) != len(replica_ids) or
            set(prepared_replica_ids) != set(replica_ids)):
        raise PreparedReplicaSnapshotChanged(
            'Locked capacity names a different replica snapshot.')
    prepared_bindings = decision_inputs.replica_bindings
    if (not isinstance(prepared_bindings, tuple) or
            len(prepared_bindings) != len(replica_infos) or
            any(not isinstance(binding, PreparedReplicaPlanningBinding)
                for binding in prepared_bindings)):
        raise ValueError('Prepared replica planning bindings are malformed.')
    prepared_by_id = {
        binding.replica_id: binding for binding in prepared_bindings
    }
    if len(prepared_by_id) != len(prepared_bindings):
        raise ValueError('Prepared replica planning bindings are malformed.')
    locked_shapes: dict[int, tuple[str, int] | None] = {}
    for info in replica_infos:
        prepared = prepared_by_id.get(info.replica_id)
        if prepared is None:
            raise PreparedReplicaSnapshotChanged(
                'Locked capacity names a different replica snapshot.')
        try:
            locked_record_id = uuid.UUID(str(info.replica_record_id))
        except (TypeError, ValueError, AttributeError) as error:
            raise ValueError(
                'Locked replica record identity is malformed.') from error
        locked_shape = _durable_exact_gpu_shape(info)
        prepared_shape = _canonical_exact_gpu_shape(prepared.exact_gpu_shape)
        prepared_durable_shape = _canonical_exact_gpu_shape(
            prepared.durable_gpu_shape)
        if (prepared.replica_record_id != locked_record_id or
                prepared.service_version != info.version or
                prepared.cluster_name != info.cluster_name or
                prepared.planned_capacity != info.planned_capacity or
                prepared_durable_shape != locked_shape or
            (locked_shape is not None and locked_shape != prepared_shape)):
            raise PreparedReplicaSnapshotChanged(
                'Locked capacity changed a prepared replica identity or '
                'shape.')
        locked_shapes[info.replica_id] = (locked_shape if locked_shape
                                          is not None else prepared_shape)
    replica_id_set = set(replica_ids)
    classes = dict(snapshot.by_replica_id)
    ordinary_replica_ids = set(snapshot.ordinary_scheduler_replica_ids)
    if (set(classes) - replica_id_set or
            any(not isinstance(replica_id, int) or isinstance(replica_id, bool)
                for replica_id in classes) or
            any(not isinstance(value,
                               kueue_lane_capacity.KueueReplicaCapacityClass)
                for value in classes.values())):
        raise ValueError('Locked Kueue capacity classes are malformed.')
    if (ordinary_replica_ids - replica_id_set or
            ordinary_replica_ids & set(classes) or
            any(not isinstance(replica_id, int) or isinstance(replica_id, bool)
                for replica_id in ordinary_replica_ids)):
        raise ValueError('Locked ordinary-scheduler capacity is malformed.')

    shapes_by_replica_id = dict(locked_shapes)
    blocked_shapes = set(snapshot.unknown_shapes)
    for info in replica_infos:
        has_reserved_intent = isinstance(
            getattr(info, 'reserved_fill_intent_idempotency_key', None),
            str) and bool(info.reserved_fill_intent_idempotency_key)
        if (info.is_zero_cost is True and
            (getattr(info, 'reserved_fill', False) or has_reserved_intent) and
                info.replica_id not in classes and
                info.replica_id not in ordinary_replica_ids):
            # The complete locked projection must positively classify every
            # reserved row.  Absence alone is never East authority.
            classes[info.replica_id] = (
                kueue_lane_capacity.KueueReplicaCapacityClass.UNKNOWN)
    for replica_id, capacity_class in classes.items():
        if capacity_class is not (
                kueue_lane_capacity.KueueReplicaCapacityClass.UNKNOWN):
            continue
        shape = shapes_by_replica_id.get(replica_id)
        if shape is None:
            blocked_shapes.add(('*', 0))
        else:
            blocked_shapes.add(shape)
    if snapshot.unbounded_unknown:
        blocked_shapes.add(('*', 0))

    transition_ids: set[int] = set()
    ready_paid_ids: set[int] = set()
    surge_shapes = {(card.casefold(), count)
                    for card, count in snapshot.replacement_surge_shapes}
    if surge_shapes:
        zero_cost_infos = [
            info for info in replica_infos if info.is_zero_cost is True
        ]
        paid_infos = [
            info for info in replica_infos if info.is_zero_cost is not True
        ]
        transition_ids.update(info.replica_id for info in zero_cost_infos)
        compatible_paid = []
        for info in paid_infos:
            shape = shapes_by_replica_id.get(info.replica_id)
            if shape is None:
                # This row may be the exact paid replacement.  Without its
                # shape the planner cannot safely select a transition victim.
                blocked_shapes.add(('*', 0))
            elif shape in surge_shapes:
                compatible_paid.append(info)
        transition_ids.update(info.replica_id for info in compatible_paid)
        surge_ready = any(
            info.replica_id in snapshot.replacement_surge_replica_ids and
            classes.get(info.replica_id) is kueue_lane_capacity.
            KueueReplicaCapacityClass.POLICY_ADMITTED and info.is_ready
            for info in zero_cost_infos)
        if surge_ready:
            ready_paid_ids.update(
                info.replica_id
                for info in compatible_paid
                if (not info.is_terminal and
                    info.status_property.is_scale_down is not True))

    rebound_bindings = build_replica_planning_bindings(
        replica_infos, {
            replica_id: shape
            for replica_id, shape in locked_shapes.items()
            if shape is not None
        })
    return dataclasses.replace(
        decision_inputs,
        replica_bindings=rebound_bindings,
        gpu_shapes_by_replica_id={
            replica_id: shape
            for replica_id, shape in locked_shapes.items()
            if shape is not None
        },
        kueue_capacity_by_replica_id=classes,
        kueue_blocked_retirement_shapes=frozenset(blocked_shapes),
        kueue_transition_replica_ids=frozenset(transition_ids),
        kueue_ready_paid_replacement_replica_ids=frozenset(ready_paid_ids))


@dataclasses.dataclass(frozen=True, kw_only=True)
class DurableCapacityReconcilePlan:
    """One uncommitted pure plan and its controller effects."""

    envelope: capacity_planning.CapacityPlanningEnvelope
    logical_target: LogicalCapacityTarget | None
    logical_retirement_floor: LogicalCapacityTarget | None
    retirement_shelter: (reserved_fill_planner.SequencedRetirementShelter |
                         None)
    scaling_decisions: tuple[AutoscalerDecision, ...]
    rollout_failure: UnrecoverableRolloutFailure | None

    def __post_init__(self) -> None:
        if not isinstance(self.envelope,
                          capacity_planning.CapacityPlanningEnvelope):
            raise ValueError('Durable capacity reconcile plan is malformed.')
        candidate = self.envelope.candidate
        prior = self.envelope.snapshot.prior_policy_state
        if ((self.retirement_shelter is not None and
             not isinstance(self.retirement_shelter,
                            reserved_fill_planner.SequencedRetirementShelter))
                or prior is None or
                not isinstance(self.scaling_decisions, tuple) or not all(
                    isinstance(item, AutoscalerDecision)
                    for item in self.scaling_decisions) or
            (self.rollout_failure is not None and not isinstance(
                self.rollout_failure, UnrecoverableRolloutFailure))):
            raise ValueError('Durable capacity reconcile plan is malformed.')
        if candidate.kind is capacity_planning.CapacityPlanKind.GATE_ACQUISITION:
            if (candidate.next_policy_state != prior or
                    self.logical_target is not None or
                    self.logical_retirement_floor is not None or
                    self.retirement_shelter is not None or
                    self.scaling_decisions or self.rollout_failure is not None):
                raise ValueError(
                    'Gate acquisition carries a controller effect.')
            return
        if (candidate.next_policy_state is None or
                not isinstance(self.logical_target, LogicalCapacityTarget) or
                not isinstance(self.logical_retirement_floor,
                               LogicalCapacityTarget) or
                self.logical_target.generation != candidate.source_generation or
                self.logical_target.target_capacity
                != candidate.wave_limited_actuation_target.total() or
                self.logical_retirement_floor.version
                != self.logical_target.version or
                self.logical_retirement_floor.generation
                != self.logical_target.generation or
                self.logical_retirement_floor.target_capacity
                != candidate.retirement_floor_target.total() or dict(
                    self.logical_retirement_floor.target_capacity_by_accelerator
                ) != candidate.retirement_floor_target.as_dict() or
                self.logical_retirement_floor.accelerator_shapes
                != self.logical_target.accelerator_shapes):
            raise ValueError('Durable capacity reconcile plan is malformed.')
        shelter_target = self.envelope.snapshot.retirement_shelter_target
        if self.retirement_shelter is None:
            if shelter_target.total() != 0:
                raise ValueError('Durable capacity plan drops its retirement '
                                 'shelter.')
        else:
            shelter = self.retirement_shelter
            shelter_by_card = {
                card.casefold(): count
                for card, count in shelter.target_capacity_by_accelerator
            }
            snapshot_by_card = {
                card.casefold(): count for card, count in shelter_target.entries
            }
            plan_shapes = {
                card.casefold(): width
                for card, width in self.logical_target.accelerator_shapes
            }
            shelter_shapes = {
                card.casefold(): width
                for card, width in shelter.accelerator_shapes
            }
            if (shelter.service_version != self.logical_target.version or
                    shelter_by_card != snapshot_by_card or any(
                        plan_shapes.get(card) != width
                        for card, width in shelter_shapes.items())):
                raise ValueError('Durable capacity plan changes its retirement '
                                 'shelter.')


def _without_ambiguous_prior_authority(
    prior_policy_state: capacity_planning.CapacityPolicyState,
    prior_candidate: capacity_planning.CapacityPlanCandidate,
) -> tuple[capacity_planning.CapacityPolicyState,
           capacity_planning.CapacityPlanCandidate]:
    """Clear stale demand/effects while preserving durable policy identity."""
    sanitized_state = dataclasses.replace(
        prior_policy_state,
        upscale_observations=0,
        snap_target_on_next_recompute=False,
        adopt_total_capacity_on_next_recompute=False)
    empty = capacity_planning.AcceleratorCapacity()
    sanitized_candidate = dataclasses.replace(
        prior_candidate,
        kind=capacity_planning.CapacityPlanKind.DEMAND,
        aggregate_demand_target=0,
        raw_demand_target=0,
        demand_attribution=empty,
        supply_aware_demand_target=empty,
        reserved_capacity_committed=empty,
        new_reserved_capacity_committed=empty,
        reserved_launch_target=empty,
        reserved_packing_padding_target=empty,
        paid_residual=empty,
        paid_launch_target=empty,
        paid_packing_padding_target=empty,
        zero_cost_padding_target=empty,
        static_prefill_target=empty,
        retained_existing_target=empty,
        transition_retention_target=empty,
        wave_limited_actuation_target=empty,
        supply_aware_actuation_target=empty,
        explicit_demand_attribution=empty,
        paid_demand_attribution=empty,
        warm_retention_target=empty,
        deadline_target=empty,
        infeasible_demand_by_priority=(),
        service_time_sources=(),
        reservation_demand_relation=(
            capacity_planning.ReservationDemandRelation.NOT_APPLICABLE),
        statically_disjoint_demand_accelerators=(),
        retirement_floor_target=empty,
        reservation_acquisition_classes=None,
        next_policy_state=sanitized_state)
    return sanitized_state, sanitized_candidate


# Preserve historical private import and pickle identities while the pure
# compatibility policy lives behind this module's facade. Internal call sites
# intentionally continue resolving these globals so facade monkeypatches keep
# controlling every strategy.
_allocate_compatibility_target = (
    autoscaler_compatibility._allocate_compatibility_target)  # pylint: disable=protected-access
_allocate_deadline_capacity_target = (
    autoscaler_compatibility._allocate_deadline_capacity_target)  # pylint: disable=protected-access
DeadlineDemand: typing.TypeAlias = autoscaler_compatibility.DeadlineDemand
DeadlineSupply: typing.TypeAlias = autoscaler_compatibility.DeadlineSupply
DeadlineCapacityPlan: typing.TypeAlias = (
    autoscaler_compatibility.DeadlineCapacityPlan)
_replica_is_retiring_card_supply = (
    autoscaler_compatibility._replica_is_retiring_card_supply)  # pylint: disable=protected-access
_merge_fresh_target_into_downscale_hold = (
    autoscaler_compatibility._merge_fresh_target_into_downscale_hold)  # pylint: disable=protected-access
_bound_materialized_reassignment_target = (
    autoscaler_compatibility._bound_materialized_reassignment_target)  # pylint: disable=protected-access
_revalidate_actuation_target = (
    autoscaler_compatibility._revalidate_actuation_target)  # pylint: disable=protected-access
for _compatibility_helper in (
        _allocate_compatibility_target,
        _allocate_deadline_capacity_target,
        _replica_is_retiring_card_supply,
        _merge_fresh_target_into_downscale_hold,
        _bound_materialized_reassignment_target,
        _revalidate_actuation_target,
):
    _compatibility_helper.__module__ = __name__
del _compatibility_helper

_LOGICAL_ROLLING_UPDATE_MAX_RETIREMENTS_PER_TICK = 20
# Maximum consecutive downscale pressure vetoes per downscale episode.
# Genuine rising pressure raises the raw target and takes the upscale
# branch, which ends the episode on its own; the veto only needs to
# protect against downscaling at the exact moment pressure begins.
# Bounding it at 2 consecutive decision ticks preserves that protection
# while restoring downscale liveness under trickle traffic. The veto does
# not restart the already elapsed downscale delay.
_MAX_CONSECUTIVE_DOWNSCALE_VETOES = 2
_COST_REBALANCE_STATE_VERSION = 2
_COST_REBALANCE_STATE_MAX_ENTRIES = 256
# Converting a modeled work floor back into whole slots divides one float by
# another, and both sides carry binary-float tails. A retention floor built
# from n identical utilization-adjusted capacities is the exact case: three
# 0.7-work floors sum to 2.1, and 2.1 / 0.7 evaluates to 3.0000000000000004,
# so a bare ceil manufactures a fourth slot out of arithmetic noise. Real
# demand moves in whole-capacity quanta, never by 1e-9, so tolerate a
# sub-epsilon remainder here exactly as the compatibility allocator's
# demand_epsilon already does.
_SLOT_CONVERSION_EPSILON = 1e-9
_RESERVED_CAPACITY_MAX_FUTURE_SKEW_SECONDS = (
    constants.RESERVED_CAPACITY_POLL_INTERVAL_SECONDS *
    constants.RESERVED_CAPACITY_STALE_AFTER_INTERVALS)
_CompatibilityWorkProfile = tuple[int, tuple[str, ...], float]
_AnnotatedCompatibilityWorkProfile = tuple[int, tuple[str, ...], float, bool]


def _canonical_additional_supply(
    configured_cards: Iterable[str],
    supply: Mapping[str, int],
) -> dict[str, int]:
    """Validate capacity-unit supply against the configured exact cards."""
    canonical_by_name = {card.casefold(): card for card in configured_cards}
    result: dict[str, int] = {}
    for raw_card, count in supply.items():
        if not isinstance(raw_card, str):
            raise ValueError('Additional zero-cost supply is malformed.')
        canonical = canonical_by_name.get(raw_card.casefold())
        if (canonical is None or canonical in result or
                not isinstance(count, int) or isinstance(count, bool) or
                count < 0):
            raise ValueError('Additional zero-cost supply is malformed.')
        result[canonical] = count
    return result


def _validate_reserved_fill_pool_topology(
    edges: Iterable[tuple[str, str, Iterable[str]]],) -> None:
    """Validate one complete v2 map using physical UID/card identity."""
    physical_uid_by_context: dict[str, str] = {}
    cards_by_physical_uid: dict[str, set[str]] = {}
    for context, physical_uid, gpu_names in edges:
        context_physical_uid = physical_uid_by_context.setdefault(
            context, physical_uid)
        if context_physical_uid != physical_uid:
            raise ValueError('One protocol-v2 Kubernetes context cannot '
                             'identify multiple physical clusters: '
                             f'{context!r}.')
        physical_cards = cards_by_physical_uid.setdefault(physical_uid, set())
        cards = set(gpu_names)
        overlap = physical_cards.intersection(cards)
        if overlap:
            raise ValueError('Protocol-v2 pool snapshots overlap on one '
                             'physical cluster for cards '
                             f'{sorted(overlap)}.')
        physical_cards.update(cards)


@dataclasses.dataclass
class _PoolFillState:
    """One protocol-v2 reserved-fill pool's independently mutable gauges."""

    protocol_version: int
    pool_key: str
    physical_cluster_uid: str
    service_generation: int
    edge_cap: int
    free_slots: int = 0
    last_raw_free_slots: int | None = None
    # None means the broker round had no exact-card measurement. A present map
    # is this service's already-arbitrated portion of the aggregate pool feed.
    free_slots_by_accelerator: dict[str, int] | None = None
    zero_cost_locations: list[spot_placer.Location] = dataclasses.field(
        default_factory=list)
    snapshot_time: float | None = None
    # Scale-down protection is deliberately separate from live launch
    # authority.  A transient broker-round failure may carry the last real
    # grant from the same physical pool here while clearing grant/feed/epoch,
    # so a service-generation transition cannot cull existing pool-local fill
    # and no new launch can replay stale authority.
    shelter_grant: int = 0
    grant: int = 0
    grant_epoch: int | None = None
    fill_target: int = 0

    def detached_copy(self) -> '_PoolFillState':
        return dataclasses.replace(
            self,
            zero_cost_locations=list(self.zero_cost_locations),
            free_slots_by_accelerator=(None if self.free_slots_by_accelerator
                                       is None else dict(
                                           self.free_slots_by_accelerator)))


_CompatibilityTargetResult: typing.TypeAlias = (
    capacity_planning.CapacityPlanCandidate)


def _work_to_slots(work: float, capacity: float) -> int:
    """Whole slots needed for `work`, ignoring sub-epsilon float remainders."""
    if capacity <= 0:
        return 0
    return math.ceil(work / capacity - _SLOT_CONVERSION_EPSILON)


def _scale_down_replica_id(target: int | LogicalScaleDownTarget) -> int:
    return target if isinstance(target, int) else target.replica_id


def _prediction_bucket_representative(index: int,
                                      bounds: Sequence[float]) -> float:
    """One duration standing in for every request in a histogram bucket.

    The buckets are log-scale and wide (the 10s-30s bucket spans 3x), so the
    choice of representative moves the estimate far more than it looks. The
    geometric midpoint is the unbiased summary of a log-scale bucket; taking
    the upper bound instead inflates the estimate by the square root of the
    bucket's width, measured at 1.70x against a real production
    distribution where 97% of requests landed in that one bucket.

    That inflation matters because it is invisible. Conservatism in fleet
    sizing belongs in the knobs an operator can see and tune
    (target_utilization_percentage, the provisioning lead, SLA weighting),
    not hidden inside a histogram summary where it silently compounds with
    them.
    """
    upper = bounds[min(index, len(bounds) - 1)]
    if index >= len(bounds):
        # The final bucket is unbounded above; its lower bound is the only
        # honest floor available.
        return upper
    lower = bounds[index - 1] if index > 0 else 0.0
    if lower <= 0:
        # The first bucket starts at zero, whose geometric mean is
        # degenerate; use the arithmetic midpoint.
        return upper / 2.0
    return math.sqrt(lower * upper)


def _generate_scale_up_decisions(
        num: int, target: dict[str, Any] | None) -> list[AutoscalerDecision]:
    return [
        AutoscalerDecision(AutoscalerDecisionOperator.SCALE_UP,
                           copy.copy(target)) for _ in range(num)
    ]


def _order_cold_paid_cards(
    configured_cards: list[str],
    placer: spot_placer.SpotPlacer | None,
    configured_gpu_count: typing.Callable[[str], int],
    location_gpu_shape: typing.Callable[[spot_placer.Location], tuple[str,
                                                                      int]],
    known_location_costs: Mapping[spot_placer.Location, float] | None = None,
) -> list[str]:
    """Order paid-capable cold cards from the centralized catalog."""
    if placer is None:
        return list(configured_cards)
    canonical_by_name = {card.casefold(): card for card in configured_cards}
    paid_costs: dict[str, float] = {}
    zero_cost_cards: set[str] = set()
    unpriced_cards: set[str] = set()
    if known_location_costs is None:
        try:
            known_location_costs = placer.known_location_costs()
        except Exception:  # pylint: disable=broad-except
            return list(configured_cards)
    for location, raw_cost in known_location_costs.items():
        raw_card, gpu_count = location_gpu_shape(location)
        card = canonical_by_name.get(raw_card.casefold())
        if card is None or gpu_count != configured_gpu_count(card):
            continue
        try:
            hourly_cost = float(raw_cost)
        except Exception:  # pylint: disable=broad-except
            unpriced_cards.add(card)
            continue
        if not math.isfinite(hourly_cost) or hourly_cost < 0:
            unpriced_cards.add(card)
        elif hourly_cost == 0:
            zero_cost_cards.add(card)
        else:
            paid_costs[card] = min(hourly_cost,
                                   paid_costs.get(card, float('inf')))

    # A card is reserved-only only when every inspected location is free and
    # no lookup was inconclusive. Exact-card demand and reserved fill still
    # retain the card; this order governs flexible cold-paid attribution only.
    reserved_only_cards = {
        card for card in configured_cards if card in zero_cost_cards and
        card not in paid_costs and card not in unpriced_cards
    }
    paid_or_unpriced_cards = [
        card for card in configured_cards if card not in reserved_only_cards
    ]
    # An unavailable nominal price keeps service order deterministic instead
    # of letting incomplete provider pricing promote a different card.
    if (not unpriced_cards and
            all(card in paid_costs for card in paid_or_unpriced_cards)):
        service_order = {
            card: index for index, card in enumerate(configured_cards)
        }
        paid_or_unpriced_cards.sort(key=lambda card: (paid_costs.get(
            card, float('inf')), service_order[card]))
    return paid_or_unpriced_cards + [
        card for card in configured_cards if card in reserved_only_cards
    ]


def _prospective_paid_cards(
    configured_cards: list[str],
    placer: spot_placer.SpotPlacer | None,
    configured_gpu_count: typing.Callable[[str], int],
    location_gpu_shape: typing.Callable[[spot_placer.Location], tuple[str,
                                                                      int]],
    known_location_costs: Mapping[spot_placer.Location, float] | None = None,
) -> list[str]:
    """Return only cards with a paid or conservatively unpriced location."""
    if placer is None:
        # Static services have no placement catalog.  Their configured task
        # remains the single prospective launch choice.
        return list(configured_cards)
    if known_location_costs is None:
        try:
            known_location_costs = placer.known_location_costs()
        except Exception:  # pylint: disable=broad-except
            # A failed catalog snapshot cannot promise a deadline-rescuing
            # paid launch.  Provider admission remains independently fenced.
            return []
    canonical_by_name = {card.casefold(): card for card in configured_cards}
    paid_capable: set[str] = set()
    for location, raw_cost in known_location_costs.items():
        # A placer-backed prospective card is closed Spot evidence.  An
        # on-demand or malformed catalog entry must not mint authority that a
        # later provider guard can only reject after publication.
        if getattr(location, 'use_spot', None) is not True:
            continue
        raw_card, gpu_count = location_gpu_shape(location)
        card = canonical_by_name.get(raw_card.casefold())
        if card is None or gpu_count != configured_gpu_count(card):
            continue
        try:
            hourly_cost = float(raw_cost)
        except Exception:  # pylint: disable=broad-except
            paid_capable.add(card)
            continue
        if not math.isfinite(hourly_cost) or hourly_cost < 0 or hourly_cost > 0:
            paid_capable.add(card)
    ordered = _order_cold_paid_cards(configured_cards, placer,
                                     configured_gpu_count, location_gpu_shape,
                                     known_location_costs)
    return [card for card in ordered if card in paid_capable]


def _generate_scale_down_decisions(
    replica_ids: list[int],
    reason: AutoscalerDecisionReason | None = None,
) -> list[AutoscalerDecision]:
    return [
        AutoscalerDecision(AutoscalerDecisionOperator.SCALE_DOWN,
                           replica_id,
                           reason=reason) for replica_id in replica_ids
    ]


def _select_nonterminal_replicas_to_scale_down(
    num_replica_to_scale_down: int,
    replica_infos: Iterable['replica_managers.ReplicaInfo'],
    service_name: str | None = None,
    cluster_job_counts: dict[str, int] | None = None,
) -> list[int]:
    """Select nonterminal replicas to scale down.

    We sort the replicas based on the following order:
        1. Based on the `scale_down_decision_order` of the status. We terminate
            the replicas that is in earlier stage first, as the replicas in
            later stage may become ready soon.
        2. Based on the version in ascending order, so we scale down the older
            versions first.
        3. For pools, based on the number of running jobs in ascending order,
            so we scale down idle workers first. For SkyServe services, job
            counts will be zero so this criterion has no effect.
        4. Based on the replica_id in descending order, which is also the order
            of the replicas being launched. We scale down the replicas that are
            launched earlier first, as the replicas that are launched later may
            become ready soon.

    Args:
        num_replica_to_scale_down: The number of replicas to scale down.
        replica_infos: The list of replica informations to select from.
        service_name: The name of the pool to query job counts for. When
            provided, replicas with fewer running jobs are scaled down first.
        cluster_job_counts: Optional pre-fetched pool job counts keyed by
            cluster name. When provided, avoids re-querying the same pool
            counts inside a caller that already fetched them.

    Returns:
        The list of replica ids to scale down.
    """
    replicas = list(replica_infos)
    status_order = serve_state.ReplicaStatus.scale_down_decision_order()
    assert all(info.status in status_order for info in replicas), (
        'All replicas to scale down should be in provisioning or launched '
        'status.', replicas)

    # Get the number of running jobs for each replica. For pools this
    # prioritizes scaling down idle workers; when service_name is not
    # provided all counts default to 0 and the sort falls through.
    if service_name is not None:
        if cluster_job_counts is None:
            cluster_job_counts = (
                managed_job_state.get_nonterminal_job_counts_by_pool(
                    service_name))
    if cluster_job_counts is None:
        cluster_job_counts = {}
    replica_job_counts: dict[int, int] = {}
    for info in replicas:
        replica_job_counts[info.replica_id] = (cluster_job_counts.get(
            info.cluster_name, 0))

    replicas = sorted(
        replicas,
        key=lambda info: (
            status_order.index(info.status),
            # version in ascending order
            info.version,
            # number of running jobs in ascending order
            replica_job_counts[info.replica_id],
            # replica_id in descending order, i.e. launched order
            -info.replica_id))
    assert len(replicas) >= num_replica_to_scale_down, (
        'Not enough replicas to scale down. Available replicas: ',
        f'{replicas}, num_replica_to_scale_down: {num_replica_to_scale_down}.')
    return [info.replica_id for info in replicas][:num_replica_to_scale_down]


class Autoscaler:
    """Abstract class for autoscalers."""

    # --------------- APIs to implement for custom autoscaler ---------------

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the autoscaler.

        Variables:
            min_replicas: Minimum number of replicas.
            max_replicas: Maximum number of replicas. Default to fixed
                number of replicas, i.e. min_replicas == max_replicas.
            target_num_replicas: Target number of replicas output by autoscaler.
            latest_version: latest version of the service.
            latest_version_ever_ready: The latest version that is ever ready.
            update_mode: Update mode for the service.
        """
        self._service_name: str = service_name
        self.min_replicas: int = spec.min_replicas
        self.min_replicas_by_accelerator: dict[str, int] = dict(
            spec.min_replicas_by_accelerator)
        self.max_replicas: int = (spec.max_replicas if spec.max_replicas
                                  is not None else spec.min_replicas)
        self.num_overprovision: int | None = spec.num_overprovision
        # All autoscaler implementations expose the service's replica unit so
        # controller consumers can rely on one explicit base-class interface.
        self.replica_unit: str = spec.replica_unit
        # Target number of replicas is initialized to min replicas
        self.target_num_replicas: int = max(
            spec.min_replicas, sum(self.min_replicas_by_accelerator.values()))
        self.target_num_replicas_by_accelerator: dict[str, int] = dict(
            self.min_replicas_by_accelerator)
        # Supply-aware exact-card target selected by the latest complete
        # actuation pass.  This is deliberately distinct from
        # target_num_replicas_by_accelerator, which attributes flexible demand
        # to the cheapest compatible cold card for explanation.  Ordered paid
        # admission must debit the target that already reused compatible
        # materialized supply, not that explanatory demand attribution.
        self.capacity_target_by_accelerator: dict[str, int] = {}
        self.capacity_target_complete: bool = False
        # Zero-cost-only local capacity beyond traffic demand.  It is carried
        # separately so neither PostgreSQL paid residual nor request demand
        # attribution can accidentally purchase it.
        self.zero_cost_padding_target_by_accelerator: dict[str, int] = {}
        # Independent explanatory floor for running or occupancy-unknown work
        # on its already-materialized exact card. It is not additive with the
        # cheapest-compatible demand attribution above and need not be its
        # subset.
        self.warm_retention_target_by_accelerator: dict[str, int] = {}
        # Positive incremental exact-card shortage that can authorize a cold
        # launch in the most recent reconciliation tick. Unlike the serving
        # target, this never treats satisfied warm retention as scale-up
        # demand.
        self.cold_launch_authority_by_accelerator: dict[str, int] = {}
        # Optional demand surfaces have explicit neutral defaults in the base
        # interface. Shape-aware and request-rate autoscalers replace these
        # values with their live state; generic autoscalers do not need
        # capability probes to participate in shared status/fill logic.
        self.request_timestamps: list[float] | None = None
        self.qps_window_size: int | None = None
        self._queue_depth_by_priority: dict[int, int] | None = None
        self.compatibility_profiles: list[dict[str, Any]] = []
        self.queued_compatibility_profiles: list[dict[str, Any]] = []
        self.queued_deadline_profiles: list[dict[str, Any]] | None = None
        self.rejected_compatibility_profiles: list[dict[str, Any]] = []
        self._compatibility_demand_complete: bool = False
        self.configured_accelerator_shapes: dict[str, int] = {}
        # Exact task node count for one backend. Logical services remain 1;
        # physical paid-cap accounting multiplies each per-node GPU shape by it.
        self.backend_num_nodes: int = 1
        self.free_reserved_slots_by_accelerator: dict[str, int] = {}
        # Shape-aware autoscalers publish a per-tick handle snapshot before
        # entering their state lock. The neutral value is part of the shared
        # resolver interface and means no snapshot is active.
        self._gpu_shape_handles_for_tick: dict[int, Any] | None = None
        # Exact Kueue admission classification is also prepared outside the
        # routing lock. None means no prepared decision is active. Unknown is
        # conservative capacity and is never ordinary retirement authority.
        self._kueue_capacity_by_replica_id_for_tick: dict[
            int, kueue_lane_capacity.KueueReplicaCapacityClass] | None = None
        self._kueue_blocked_retirement_shapes_for_tick: frozenset[tuple[
            str, int]] = frozenset()
        self._kueue_transition_replica_ids_for_tick: frozenset[int] = (
            frozenset())
        self._kueue_ready_paid_replacement_replica_ids_for_tick: frozenset[
            int] = frozenset()
        # Seed from the constructed service version (not always
        # INITIAL_VERSION). On a controller restart/respawn the autoscaler is
        # rebuilt; if it reset to version 1 while live replicas are at version
        # >= 2 (any service updated at least once), the version filters below
        # would treat every running replica as outdated and drive permanent
        # replica churn. The caller (`from_spec`) passes the recovered latest
        # version so the autoscaler agrees with the replica manager.
        self.latest_version: int = version
        # The latest_version_ever_ready should be smaller than the
        # latest_version, so we can fail early if the initial version got
        # unrecoverable failure.
        self.latest_version_ever_ready: int = self.latest_version - 1
        # Set only for a never-ready candidate whose persisted replica state
        # satisfies ReplicaStatusProperty.unrecoverable_failure(). The
        # controller durably quarantines this exact version before respawning
        # onto the proven active runtime. Generic provisioning/capacity
        # failures deliberately never populate this signal.
        self._unrecoverable_rollout_failure: (UnrecoverableRolloutFailure |
                                              None) = None
        self.update_mode = serve_utils.DEFAULT_UPDATE_MODE
        # [boltz fork] Reserved-capacity fill (opt-in): snapshot state fed
        # by the controller's poller thread via collect_reserved_capacity.
        # Lives in the base class so fill composes with every autoscaler type
        # without touching their demand math. SkyServiceSpec.__setstate__
        # materializes the field for specs unpickled from old DB rows.
        self.reserved_capacity_fill: bool = bool(spec.reserved_capacity_fill)
        # Broker claim parameters, snapshotted from the spec so the poller
        # can read them off the live autoscaler (update_version refreshes them).
        self.reserved_fill_floor_replicas: int = int(
            spec.reserved_fill_floor_replicas or 0)
        self.reserved_fill_weight: float = float(spec.reserved_fill_weight or
                                                 1.0)
        # Whether this service releases its whole fill entitlement while it
        # demonstrates no work (see reserved_capacity_broker).
        self.reserved_fill_utilization_gate: bool = bool(
            spec.reserved_fill_utilization_gate)
        # Damped free-slot value the fill target acts on (see
        # collect_reserved_capacity for the two-poll increase damping).
        self._fill_free_slots: int = 0
        self._fill_last_raw_free_slots: int | None = None
        self._fill_zero_cost_locations: list[spot_placer.Location] = []
        self._fill_snapshot_time: float | None = None
        # Last computed fill target, surfaced via info() only.
        self._fill_target: int = 0
        # Broker grant ceiling + the epoch it was issued under + the pool
        # key the epoch belongs to (epochs are per-pool round counters, so
        # the launch fence needs both). None grant = no ceiling
        # (single-service #108 identity; also the pre-broker default so
        # every existing call path is unchanged). DELIBERATELY not
        # persisted in dump_dynamic_states: grants are DB-authoritative
        # and the poller re-feeds them within one interval -- a swapped-in
        # autoscaler briefly without a ceiling is safe (ceilings only gate
        # NEW launches).
        self._fill_grant: int | None = None
        self._fill_grant_epoch: int | None = None
        self._fill_grant_pool_key: str | None = None
        self._fill_protocol_version: int = 1
        self._fill_service_generation: int = 0
        self._fill_physical_cluster_uid: str | None = None
        # Protocol-v2 state is a complete map published atomically by one
        # service poll cycle. The legacy scalar fields above remain the exact
        # protocol-v1 implementation and compatibility/status projection.
        self._fill_pool_state_lock = threading.RLock()
        self._fill_pool_states: dict[str, _PoolFillState] = {}
        # Stable identity of the pool that actually emitted the prior wave's
        # first decision. The replica manager deliberately stops a v2 batch
        # when provider admission is busy, so the next wave starts after this
        # pool in configured order. Identity (rather than an actionable-list
        # index) keeps rotation stable as other pools become actionable.
        self._fill_pool_last_started_key: str | None = None
        # Fences a detached decision wave from debiting replacement feed or
        # restoring a rotation anchor after membership, ordering, or an
        # explicit lifecycle reset. Same-order feed refreshes deliberately
        # retain the revision.
        self._fill_pool_order_revision = 0
        # The sequenced planner is invoked by the controller on the same
        # thread as ``generate_scaling_decisions``.  A thread-local scope
        # keeps that one decision tick from speculatively spending the legacy
        # protocol-v2 feed while leaving concurrent status/poller readers and
        # legacy ticks untouched.
        self._sequenced_reserved_fill_planning_state = threading.local()
        # Opt-in economic replacement.  The placer reference is injected by
        # the controller each tick because ReplicaManager owns placement state.
        self.cost_rebalance: bool = bool(spec.cost_rebalance)
        self.cost_rebalance_min_savings_fraction: float = float(
            spec.cost_rebalance_min_savings_fraction)
        self.cost_rebalance_max_parallel_replacements: int = int(
            spec.cost_rebalance_max_parallel_replacements)
        self.cost_rebalance_stabilization_seconds: float = float(
            spec.cost_rebalance_stabilization_seconds)
        self._cost_rebalance_spot_placer: spot_placer.SpotPlacer | None = None
        # One immutable bulk catalog view is shared by every ordering and
        # rebalance pass in a decision tick. None in an active scope means the
        # snapshot failed and every economic decision must fail closed.
        self._cold_paid_costs_tick_active = False
        self._cold_paid_location_costs_for_tick: (Mapping[spot_placer.Location,
                                                          float] | None) = None
        self._cost_rebalance_candidate_since: dict[tuple[str,
                                                         spot_placer.Location],
                                                   float] = {}
        self._cost_rebalance_state_dirty = False
        self._cost_rebalance_replica_cost_cache: dict[tuple[int, str],
                                                      float] = {}
        # Freshness fence for priority-only gauges. A stale LB report may keep
        # a conservative scale-up target, but it must not keep refreshing a
        # high-priority paid-capacity waiter indefinitely.
        self._launch_priority_report_received_at: float | None = None

    def get_final_target_num_replicas(self) -> int:
        """Get the final target number of replicas."""
        if self.num_overprovision is None:
            return self.target_num_replicas
        return self.target_num_replicas + self.num_overprovision

    def current_launch_priority(self) -> int:
        """Highest recent demand priority that may require fresh capacity."""
        if not self._launch_priority_evidence_is_fresh():
            return constants.LB_REQUEST_PRIORITY_MIN
        priorities = [constants.LB_REQUEST_PRIORITY_MIN]
        by_priority = self._queue_depth_by_priority
        if isinstance(by_priority, dict):
            priorities.extend(
                int(priority)
                for priority, count in by_priority.items()
                if isinstance(priority, int) and
                not isinstance(priority, bool) and isinstance(count, int) and
                not isinstance(count, bool) and count > 0)
        for profiles in (self.queued_compatibility_profiles,
                         self.rejected_compatibility_profiles,
                         self.compatibility_profiles):
            if not isinstance(profiles, list):
                continue
            for profile in profiles:
                if not isinstance(profile, dict):
                    continue
                priority = profile.get('priority')
                count = profile.get('recent_count', profile.get('count', 0))
                if (isinstance(priority, int) and
                        not isinstance(priority, bool) and
                        isinstance(count, (int, float)) and
                        not isinstance(count, bool) and count > 0):
                    priorities.append(priority)
        return max(constants.LB_REQUEST_PRIORITY_MIN,
                   min(constants.LB_REQUEST_PRIORITY_MAX, max(priorities)))

    def _launch_priority_evidence_is_fresh(self) -> bool:
        received_at = self._launch_priority_report_received_at
        if received_at is None:
            return False
        threshold = 3.0 * constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS
        return time.time() - received_at <= threshold

    def current_launch_priorities_by_accelerator(
            self, accelerators: Iterable[str]) -> dict[str, int]:
        """Highest active priority whose compatibility includes each card."""
        canonical = {
            str(accelerator).casefold(): str(accelerator)
            for accelerator in accelerators
        }
        priorities = {
            accelerator: constants.LB_REQUEST_PRIORITY_MIN
            for accelerator in canonical.values()
        }
        if not self._launch_priority_evidence_is_fresh():
            return priorities
        saw_valid_profile = False
        for profiles in (self.queued_compatibility_profiles,
                         self.rejected_compatibility_profiles,
                         self.compatibility_profiles):
            if not isinstance(profiles, list):
                continue
            for profile in profiles:
                if not isinstance(profile, dict):
                    continue
                priority = profile.get('priority')
                count = profile.get('recent_count', profile.get('count', 0))
                if (not isinstance(priority, int) or
                        isinstance(priority, bool) or
                        not isinstance(count, (int, float)) or
                        isinstance(count, bool) or count <= 0):
                    continue
                saw_valid_profile = True
                compatible = profile.get('compatible_accelerators')
                if not isinstance(compatible, (list, tuple)) or not compatible:
                    matching = list(priorities)
                else:
                    matching = [
                        canonical[str(card).casefold()]
                        for card in compatible
                        if str(card).casefold() in canonical
                    ]
                if not matching:
                    continue
                clamped = max(constants.LB_REQUEST_PRIORITY_MIN,
                              min(constants.LB_REQUEST_PRIORITY_MAX, priority))
                for card in matching:
                    priorities[card] = max(priorities[card], clamped)
        if not saw_valid_profile:
            fallback = self.current_launch_priority()
            return {card: fallback for card in priorities}
        return priorities

    @property
    def unrecoverable_rollout_failure(
            self) -> UnrecoverableRolloutFailure | None:
        """Return this tick's typed never-ready rollout failure, if any."""
        return self._unrecoverable_rollout_failure

    @property
    def fill_target(self) -> int:
        """Return the latest aggregate reserved-capacity fill target."""
        return self._fill_target

    @contextlib.contextmanager
    def sequenced_reserved_fill_planning(self) -> typing.Iterator[None]:
        """Bypass the legacy fill overlay for one typed planning tick.

        The sequenced path derives its immutable retirement shelter directly
        from PostgreSQL allocation authority.  It must never project that map
        into the legacy process-local feed or let the legacy overlay launch,
        shelter, or mutate fairness state.
        """
        state = self._sequenced_reserved_fill_planning_state
        previously_enabled = bool(getattr(state, 'enabled', False))
        state.enabled = True
        try:
            yield
        finally:
            state.enabled = previously_enabled

    def _sequenced_reserved_fill_planning_enabled(self) -> bool:
        return bool(
            getattr(self._sequenced_reserved_fill_planning_state, 'enabled',
                    False))

    def reserved_fill_rotation_anchor(self) -> str | None:
        """Return the pool whose last wave began with durable acceptance."""
        with self._fill_pool_state_lock:
            anchor = self._fill_pool_last_started_key
            return anchor if anchor in self._fill_pool_states else None

    def commit_reserved_fill_rotation_anchor(self, pool_key: str) -> bool:
        """Commit a receipt-proven rotation anchor if the pool is still live.

        Returns false across a concurrent pool-set replacement.  The caller
        can simply wake reconciliation: the next authenticated map will use
        its own ordered membership and no stale anchor is restored.
        """
        if not isinstance(pool_key, str) or not pool_key:
            raise ValueError('Reserved-fill rotation anchor must be a '
                             'nonempty pool key.')
        with self._fill_pool_state_lock:
            if pool_key not in self._fill_pool_states:
                return False
            self._fill_pool_last_started_key = pool_key
            return True

    def reserved_fill_materialized_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Return provider-free capacity already represented by replica rows.

        This deliberately excludes the autoscaler's demand target.  Sequenced
        reserved fill can therefore commit free-capacity intents before the
        independent load-balancer demand feed is usable, while the durable
        admission transaction still enforces the service-wide maximum.
        """
        return sum(
            self._service_ceiling_capacity_units(info)
            for info in replica_infos
            if self._reserved_fill_row_is_materialized(info))

    def _calculate_target_num_replicas(self) -> int:
        """Calculate target number of replicas."""
        raise NotImplementedError

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            logger.error(f'Invalid version: {version}, '
                         f'latest version: {self.latest_version}')
            return
        self.latest_version = version
        self.min_replicas = spec.min_replicas
        self.min_replicas_by_accelerator = dict(
            spec.min_replicas_by_accelerator)
        self.max_replicas = (spec.max_replicas if spec.max_replicas is not None
                             else spec.min_replicas)
        self.replica_unit = spec.replica_unit
        # Re-clip self.target_num_replicas with new min and max replicas.
        self.target_num_replicas = self._clip_target_num_replicas(
            self.target_num_replicas)
        self.update_mode = update_mode
        # An update can toggle the fill flag; consumption follows the new
        # spec immediately. (The controller's update_service handler
        # seeds the zero-cost location set and starts the poller when an
        # update enables the flag -- no respawn needed, provided the spot
        # placer already exists.)
        self.reserved_capacity_fill = bool(spec.reserved_capacity_fill)
        # Broker claim knobs follow the update too: the poller reads them
        # off the live autoscaler on its next heartbeat.
        self.reserved_fill_floor_replicas = int(
            spec.reserved_fill_floor_replicas or 0)
        self.reserved_fill_weight = float(spec.reserved_fill_weight or 1.0)
        self.reserved_fill_utilization_gate = bool(
            spec.reserved_fill_utilization_gate)
        with self._fill_pool_state_lock:
            if not self.reserved_capacity_fill:
                # Disabling fill deliberately withdraws every edge. Do not
                # leave a process-local shelter that a later re-enable could
                # relay across a failed first round for a newly created claim.
                self._fill_pool_states = {}
                self._fill_pool_last_started_key = None
            else:
                # A service update may add/remove/reorder pool edges and
                # therefore advance the authoritative service generation.
                # Preserve location identity for scale-down shelter, but
                # invalidate all old feed until the poller publishes the new
                # complete generation.
                for pool_state in self._fill_pool_states.values():
                    pool_state.free_slots = 0
                    pool_state.last_raw_free_slots = None
                    # Shelter-only until the next exact-generation heartbeat:
                    # preserve only the last real broker entitlement. Zero
                    # feed cannot authorize a launch under it, while widening
                    # the grant to edge_cap would let an update shelter
                    # holdings that a peer had already been granted.
                    pool_state.shelter_grant = min(pool_state.shelter_grant,
                                                   pool_state.edge_cap)
                    pool_state.grant = 0
                    pool_state.grant_epoch = None
            self._fill_pool_order_revision += 1
            self._refresh_legacy_fill_projection_locked()
        self.cost_rebalance = bool(spec.cost_rebalance)
        self.cost_rebalance_min_savings_fraction = float(
            spec.cost_rebalance_min_savings_fraction)
        self.cost_rebalance_max_parallel_replacements = int(
            spec.cost_rebalance_max_parallel_replacements)
        self.cost_rebalance_stabilization_seconds = float(
            spec.cost_rebalance_stabilization_seconds)
        self._clear_cost_rebalance_candidates()
        self.warm_retention_target_by_accelerator = {}
        self.cold_launch_authority_by_accelerator = {}

    def set_spot_placer(self, placer: spot_placer.SpotPlacer | None) -> None:
        """Publish ReplicaManager's live placement/bench state for this tick."""
        self._cost_rebalance_spot_placer = placer

    @contextlib.contextmanager
    def _cold_paid_cost_snapshot_for_tick(self) -> typing.Iterator[None]:
        """Share one workspace-policy/cost view across a decision tick.

        This is planning state only. SpotPlacer launch admission still
        revalidates the live workspace policy before provisioning.
        """
        previous_active = self._cold_paid_costs_tick_active
        previous_costs = self._cold_paid_location_costs_for_tick
        self._cold_paid_costs_tick_active = True
        placer = self._cost_rebalance_spot_placer
        try:
            if placer is None:
                self._cold_paid_location_costs_for_tick = None
            else:
                try:
                    self._cold_paid_location_costs_for_tick = (
                        placer.known_location_costs())
                except Exception:  # pylint: disable=broad-except
                    self._cold_paid_location_costs_for_tick = None
            yield
        finally:
            self._cold_paid_costs_tick_active = previous_active
            self._cold_paid_location_costs_for_tick = previous_costs

    def _known_location_costs_for_current_tick(
            self) -> Mapping[spot_placer.Location, float] | None:
        """Return the tick snapshot, or acquire one for a non-shape scaler."""
        if self._cold_paid_costs_tick_active:
            return self._cold_paid_location_costs_for_tick
        placer = self._cost_rebalance_spot_placer
        if placer is None:
            return None
        try:
            return placer.known_location_costs()
        except Exception:  # pylint: disable=broad-except
            return None

    def _order_cold_paid_cards_for_tick(
        self,
        configured_cards: list[str],
        configured_gpu_count: typing.Callable[[str], int],
        location_gpu_shape: typing.Callable[[spot_placer.Location], tuple[str,
                                                                          int]],
    ) -> list[str]:
        """Order cards from the decision tick's immutable bulk cost view."""
        if self._cold_paid_costs_tick_active:
            known_costs = self._cold_paid_location_costs_for_tick
            if known_costs is None:
                return list(configured_cards)
            return _order_cold_paid_cards(configured_cards,
                                          self._cost_rebalance_spot_placer,
                                          configured_gpu_count,
                                          location_gpu_shape, known_costs)
        return _order_cold_paid_cards(configured_cards,
                                      self._cost_rebalance_spot_placer,
                                      configured_gpu_count, location_gpu_shape)

    def _prospective_paid_cards_for_tick(
        self,
        configured_cards: list[str],
        configured_gpu_count: typing.Callable[[str], int],
        location_gpu_shape: typing.Callable[[spot_placer.Location], tuple[str,
                                                                          int]],
    ) -> list[str]:
        """Return paid-capable cards from the same immutable tick snapshot."""
        if self._cold_paid_costs_tick_active:
            if (self._cost_rebalance_spot_placer is not None and
                    self._cold_paid_location_costs_for_tick is None):
                return []
            return _prospective_paid_cards(
                configured_cards, self._cost_rebalance_spot_placer,
                configured_gpu_count, location_gpu_shape,
                self._cold_paid_location_costs_for_tick)
        return _prospective_paid_cards(configured_cards,
                                       self._cost_rebalance_spot_placer,
                                       configured_gpu_count, location_gpu_shape)

    def _clear_cost_rebalance_candidates(self) -> None:
        if self._cost_rebalance_candidate_since:
            self._cost_rebalance_candidate_since.clear()
            self._cost_rebalance_state_dirty = True

    def dump_cost_rebalance_state(self) -> dict[str, Any]:
        """Return bounded JSON-safe continuous-eligibility evidence."""
        limit = min(_COST_REBALANCE_STATE_MAX_ENTRIES,
                    max(16, 4 * self.cost_rebalance_max_parallel_replacements))
        candidates = []
        for (replica_record_id, location), first_seen_at in list(
                self._cost_rebalance_candidate_since.items())[:limit]:
            if not math.isfinite(first_seen_at):
                continue
            candidates.append({
                'replica_record_id': replica_record_id,
                'location': location.to_pickleable(),
                'first_seen_at': first_seen_at,
            })
        return {
            'version': _COST_REBALANCE_STATE_VERSION,
            'service_version': self.latest_version,
            'candidates': candidates,
        }

    def load_cost_rebalance_state(self, state: dict[str, Any] | None) -> None:
        """Restore candidate timers without extending them across a restart."""
        if (not isinstance(state, dict) or
                state.get('version') != _COST_REBALANCE_STATE_VERSION or
                state.get('service_version') != self.latest_version):
            return
        candidates = state.get('candidates')
        if not isinstance(candidates, list):
            return
        limit = min(_COST_REBALANCE_STATE_MAX_ENTRIES,
                    max(16, 4 * self.cost_rebalance_max_parallel_replacements))
        now = time.time()
        restored = {}
        for raw in candidates[:limit]:
            if not isinstance(raw, dict):
                continue
            raw_replica_record_id = raw.get('replica_record_id')
            first_seen_at = raw.get('first_seen_at')
            if (not isinstance(raw_replica_record_id, str) or
                    not isinstance(first_seen_at, (int, float)) or
                    isinstance(first_seen_at, bool) or
                    not math.isfinite(first_seen_at)):
                continue
            try:
                replica_record_id = str(uuid.UUID(raw_replica_record_id))
            except ValueError:
                continue
            raw_location = raw.get('location')
            if not isinstance(raw_location, dict):
                continue
            try:
                location = spot_placer.Location.from_pickleable(raw_location)
            except (AssertionError, KeyError, TypeError, ValueError):
                continue
            if location is None:
                continue
            restored[(replica_record_id,
                      location)] = min(float(first_seen_at), now)
        self._cost_rebalance_candidate_since = restored
        self._cost_rebalance_state_dirty = False

    @property
    def cost_rebalance_state_dirty(self) -> bool:
        return self._cost_rebalance_state_dirty

    def mark_cost_rebalance_state_persisted(self) -> None:
        self._cost_rebalance_state_dirty = False

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        """Collect request information from aggregator for autoscaling."""
        raise NotImplementedError

    def clear_paid_launch_authority_for_fresh_zero(self) -> None:
        """Withdraw every process-local cold-launch allowance.

        The durable zero capacity plan is the launch fence.  Clearing this
        explanatory/process-local map at ingestion keeps status and later
        planning from carrying a positive allowance across the exact zero
        decision.
        """
        self.cold_launch_authority_by_accelerator = {}

    def collect_reserved_capacity(
            self,
            free_slots: int,
            zero_cost_location_keys: list[dict[str, Any]],
            timestamp: float,
            grant: int | None = None,
            grant_epoch: int | None = None,
            grant_pool_key: str | None = None,
            protocol_version: int = 1,
            service_generation: int = 0,
            physical_cluster_uid: str | None = None) -> None:
        """Ingest a free-capacity snapshot from the reserved-capacity poller.

        `zero_cost_location_keys` are Location.to_pickleable() dicts of
        the placer's zero-cost location set (benched ones included: they
        still identify which existing replicas are fill).

        Damping: an INCREASE in free slots is acted on only when two
        consecutive snapshots both exceed the previously-acted-on value
        (acting on the min of the two -- the level that persisted across
        both polls), so an eviction storm's transient free spike cannot
        cause launch/evict churn. A DECREASE applies immediately:
        capacity that vanished must stop being filled now.

        grant/grant_epoch/grant_pool_key come from the reserved-fill
        broker: grant is the entitlement ceiling on the FILL fleet (None =
        no ceiling, the single-service identity), grant_epoch the fencing
        token stamped onto fill scale-up decisions so a launch outliving
        its allocation round is skipped at actuation time, and
        grant_pool_key the pool the epoch belongs to (epochs are per-pool
        round counters; the fence compares against that pool's round).
        """
        if int(protocol_version) == 1:
            # Explicit protocol demotion: scalar state becomes authoritative.
            # A retained v2 map would otherwise make the overlay ignore every
            # subsequent v1 heartbeat forever.
            with self._fill_pool_state_lock:
                self._fill_pool_states = {}
                self._fill_pool_last_started_key = None
                self._fill_pool_order_revision += 1
        free_slots = max(0, int(free_slots))
        prev_raw = self._fill_last_raw_free_slots
        self._fill_last_raw_free_slots = free_slots
        if free_slots <= self._fill_free_slots:
            self._fill_free_slots = free_slots
        elif prev_raw is not None and prev_raw > self._fill_free_slots:
            self._fill_free_slots = min(prev_raw, free_slots)
        self._fill_zero_cost_locations = [
            location for location in (spot_placer.Location.from_pickleable(key)
                                      for key in zero_cost_location_keys)
            if location is not None
        ]
        self._fill_snapshot_time = timestamp
        self._fill_grant = grant
        self._fill_grant_epoch = grant_epoch
        self._fill_grant_pool_key = grant_pool_key
        self._fill_protocol_version = int(protocol_version)
        self._fill_service_generation = int(service_generation)
        self._fill_physical_cluster_uid = physical_cluster_uid

    @staticmethod
    def _parse_reserved_fill_pool_locations(
        identity: reserved_capacity_broker.PoolIdentity,
        raw_location_keys: Any,
    ) -> list[spot_placer.Location]:
        """Parse an exact v2 pool location set or reject all authority."""
        if not isinstance(raw_location_keys, list) or not raw_location_keys:
            raise ValueError('Protocol-v2 pool snapshots require a nonempty '
                             'location-key list.')
        locations: list[spot_placer.Location] = []
        cards: set[str] = set()
        contexts: set[str] = set()
        widths: set[int] = set()
        for key in raw_location_keys:
            try:
                location = spot_placer.Location.from_pickleable(key)
            except (AssertionError, KeyError, TypeError, ValueError) as error:
                raise ValueError('Protocol-v2 pool snapshot contains a '
                                 'malformed location key.') from error
            if location is None or str(location.cloud).lower() != 'kubernetes':
                raise ValueError('Protocol-v2 pool locations must resolve to '
                                 'Kubernetes.')
            if not isinstance(location.region, str) or not location.region:
                raise ValueError('Protocol-v2 pool locations require a '
                                 'nonempty Kubernetes context.')
            accelerators = location.accelerators
            if not isinstance(accelerators, dict) or len(accelerators) != 1:
                raise ValueError('Protocol-v2 pool locations require one '
                                 'exact accelerator shape.')
            raw_card, raw_count = next(iter(accelerators.items()))
            if (not isinstance(raw_card, str) or not raw_card or
                    isinstance(raw_count, bool) or
                    not isinstance(raw_count, (int, float)) or
                    not math.isfinite(float(raw_count)) or
                    not float(raw_count).is_integer() or raw_count <= 0):
                raise ValueError('Protocol-v2 pool locations require a '
                                 'positive whole accelerator count.')
            card = raw_card.casefold()
            if card not in identity.gpu_names:
                raise ValueError('Protocol-v2 pool location accelerator does '
                                 'not match its composite pool key.')
            cards.add(card)
            contexts.add(location.region)
            widths.add(int(raw_count))
            locations.append(location)
        if (cards != set(identity.gpu_names) or len(contexts) != 1 or
                len(widths) != 1):
            raise ValueError('Protocol-v2 pool locations must exactly cover '
                             'their composite cards in one context and at one '
                             'GPU width.')
        return locations

    def collect_reserved_capacity_pools(
        self,
        pool_snapshots: dict[str, dict[str, Any]],
    ) -> None:
        """Atomically ingest one complete protocol-v2 pool snapshot map.

        Every entry must describe the same authoritative service generation.
        A pool without an exact-generation round is still present, but carries
        ``free_slots=0`` and ``grant=0``. A generation change starts damping
        from zero, so feed from the old cross-pool budget cannot survive the
        atomic map swap.
        """
        parsed: dict[str, _PoolFillState] = {}
        generations: set[int] = set()
        topology_edges: list[tuple[str, str, Iterable[str]]] = []
        for map_key, snapshot in pool_snapshots.items():
            pool_key = str(snapshot.get('pool_key', map_key))
            if pool_key != map_key:
                raise ValueError('Reserved-fill pool snapshot key mismatch: '
                                 f'{map_key!r} != {pool_key!r}.')
            try:
                identity = reserved_capacity_broker.parse_pool_identity(
                    pool_key)
            except (TypeError, ValueError) as error:
                raise ValueError('Reserved-fill pool snapshot has a malformed '
                                 f'pool key {pool_key!r}.') from error
            if identity.protocol_version != 2:
                raise ValueError('Multi-pool snapshots require a protocol-v2 '
                                 f'pool key, got {pool_key!r}.')
            canonical_pool_key = reserved_capacity_broker.make_pool_key(
                '',
                identity.gpu_names,
                protocol_version=reserved_capacity_broker.PROTOCOL_V2,
                physical_cluster_uid=identity.physical_cluster_uid)
            if pool_key != canonical_pool_key:
                raise ValueError('Protocol-v2 pool snapshot key must be '
                                 f'canonical, got {pool_key!r}.')
            protocol_version = snapshot.get('protocol_version', 0)
            if (isinstance(protocol_version, bool) or
                    not isinstance(protocol_version, int) or
                    protocol_version != 2):
                raise ValueError('Multi-pool snapshots require reserved-fill '
                                 f'protocol 2, got {protocol_version!r}.')
            generation = snapshot['service_generation']
            if (isinstance(generation, bool) or
                    not isinstance(generation, int) or generation < 1):
                raise ValueError('Reserved-fill service generation must be '
                                 'positive under protocol 2.')
            generations.add(generation)
            edge_cap = snapshot['edge_cap']
            if (isinstance(edge_cap, bool) or not isinstance(edge_cap, int) or
                    edge_cap < 0):
                raise ValueError('Reserved-fill edge cap must be a '
                                 'non-negative integer under protocol 2.')
            raw_free = snapshot.get('free_slots', 0)
            if (isinstance(raw_free, bool) or not isinstance(raw_free, int) or
                    raw_free < 0):
                raise ValueError('Protocol-v2 free-slot feed must be a '
                                 'non-negative integer.')
            raw_free_by_accelerator = snapshot.get('free_slots_by_accelerator')
            free_by_accelerator: dict[str, int] | None = None
            if raw_free_by_accelerator is not None:
                if not isinstance(raw_free_by_accelerator, dict):
                    raise ValueError('Protocol-v2 exact-card feed must be a '
                                     'mapping when present.')
                free_by_accelerator = {}
                for raw_card, raw_count in raw_free_by_accelerator.items():
                    if (not isinstance(raw_card, str) or not raw_card or
                            isinstance(raw_count, bool) or
                            not isinstance(raw_count, int) or raw_count < 0):
                        raise ValueError('Protocol-v2 exact-card feed contains '
                                         'an invalid card/count entry.')
                    card = raw_card.casefold()
                    if card in free_by_accelerator:
                        raise ValueError('Protocol-v2 exact-card feed contains '
                                         'duplicate card identities.')
                    if raw_count > 0:
                        free_by_accelerator[card] = raw_count
                if sum(free_by_accelerator.values()) != raw_free:
                    raise ValueError('Protocol-v2 exact-card feed must sum to '
                                     'its aggregate free-slot feed.')
                if any(card not in identity.gpu_names
                       for card in free_by_accelerator):
                    raise ValueError('Protocol-v2 exact-card feed contains a '
                                     'card outside its composite pool key.')
            raw_grant = snapshot.get('grant', 0)
            raw_shelter_grant = snapshot.get('shelter_grant', raw_grant)
            if (isinstance(raw_grant, bool) or not isinstance(raw_grant, int) or
                    raw_grant < 0 or isinstance(raw_shelter_grant, bool) or
                    not isinstance(raw_shelter_grant, int) or
                    raw_shelter_grant < 0):
                raise ValueError('Protocol-v2 grant and shelter must be '
                                 'non-negative integers.')
            raw_grant_epoch = snapshot.get('grant_epoch')
            if (raw_grant_epoch is not None and
                (isinstance(raw_grant_epoch, bool) or
                 not isinstance(raw_grant_epoch, int) or raw_grant_epoch < 1)):
                raise ValueError('Protocol-v2 grant epoch must be a '
                                 'positive integer when present.')
            if raw_grant_epoch is None and (raw_grant > 0 or raw_free > 0):
                raise ValueError('Protocol-v2 live launch authority requires '
                                 'a positive grant epoch.')
            grant = min(edge_cap, raw_grant)
            shelter_grant = min(edge_cap, raw_shelter_grant)
            locations = self._parse_reserved_fill_pool_locations(
                identity, snapshot.get('zero_cost_location_keys'))
            physical_uid = snapshot.get('physical_cluster_uid')
            if (not isinstance(physical_uid, str) or not physical_uid or
                    physical_uid != identity.physical_cluster_uid):
                raise ValueError('Protocol-v2 pool snapshot requires a '
                                 'physical Kubernetes cluster UID matching '
                                 'its composite pool key.')
            context = locations[0].region
            topology_edges.append((context, physical_uid, identity.gpu_names))
            raw_timestamp = snapshot['timestamp']
            if (isinstance(raw_timestamp, bool) or
                    not isinstance(raw_timestamp, (int, float)) or
                    not math.isfinite(raw_timestamp) or raw_timestamp < 0):
                raise ValueError('Protocol-v2 pool snapshot timestamp must be '
                                 'a finite non-negative number.')
            if (raw_timestamp
                    > time.time() + _RESERVED_CAPACITY_MAX_FUTURE_SKEW_SECONDS):
                raise ValueError('Protocol-v2 pool snapshot timestamp is too '
                                 'far in the future.')
            parsed[pool_key] = _PoolFillState(
                protocol_version=protocol_version,
                pool_key=pool_key,
                physical_cluster_uid=physical_uid,
                service_generation=generation,
                edge_cap=edge_cap,
                free_slots_by_accelerator=free_by_accelerator,
                zero_cost_locations=locations,
                snapshot_time=float(raw_timestamp),
                shelter_grant=shelter_grant,
                grant=grant,
                grant_epoch=raw_grant_epoch,
            )
            # Damping is filled under the lock from the prior exact-generation
            # state; raw_free remains local so no half-updated map is visible.
            parsed[pool_key].last_raw_free_slots = raw_free

        _validate_reserved_fill_pool_topology(topology_edges)
        if len(generations) > 1:
            raise ValueError('A complete reserved-fill pool map must carry '
                             f'one service generation, got {generations}.')

        with self._fill_pool_state_lock:
            previous = self._fill_pool_states
            for pool_key, state in parsed.items():
                raw_free = state.last_raw_free_slots or 0
                prior = previous.get(pool_key)
                same_pool_lineage = (
                    prior is not None and
                    prior.physical_cluster_uid == state.physical_cluster_uid and
                    prior.service_generation <= state.service_generation)
                if not same_pool_lineage:
                    # A newly discovered/replaced pool gets no feed on its
                    # first sample. The next observation confirms the
                    # increase, mirroring protocol-v1 two-poll damping.
                    state.free_slots = 0
                    state.last_raw_free_slots = raw_free
                else:
                    assert prior is not None
                    # Service generations fence launch authority, not the
                    # physical capacity observation. Demand/headroom changes
                    # can advance the generation every poll; restarting the
                    # two-poll damping on each advance would therefore keep a
                    # continuously free pool at zero forever. Carry only the
                    # pool-local damping memory across a forward generation.
                    # The new state still carries the new generation, grant,
                    # epoch, cap, and allowed locations, so no old launch
                    # authority is reused. A removed edge has no `prior` on
                    # re-add, and a replacement UID starts from zero above.
                    state.free_slots = prior.free_slots
                    previous_raw = prior.last_raw_free_slots
                    state.last_raw_free_slots = raw_free
                    if raw_free <= state.free_slots:
                        state.free_slots = raw_free
                    elif (previous_raw is not None and
                          previous_raw > state.free_slots):
                        state.free_slots = min(previous_raw, raw_free)
                state.free_slots = min(state.free_slots, state.edge_cap)
            if tuple(previous) != tuple(parsed):
                self._fill_pool_order_revision += 1
            self._fill_pool_states = parsed
            if self._fill_pool_last_started_key not in parsed:
                self._fill_pool_last_started_key = None
            self._refresh_legacy_fill_projection_locked()

    def seed_zero_cost_pools(
        self,
        pool_location_keys: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Seed protocol-v2 location identity without authorizing feed."""
        with self._fill_pool_state_lock:
            if self._fill_pool_states:
                return
            # A seed intentionally lacks protocol/generation/UID authority.
            # Keep it only in the aggregate legacy location projection for
            # restart-time scale-down protection; it cannot launch.
            self._fill_zero_cost_locations = [
                location for keys in pool_location_keys.values()
                for location in (
                    spot_placer.Location.from_pickleable(key) for key in keys)
                if location is not None
            ]

    def _refresh_legacy_fill_projection_locked(self) -> None:
        """Refresh scalar compatibility/status fields from the v2 map."""
        states = list(self._fill_pool_states.values())
        self._fill_free_slots = sum(state.free_slots for state in states)
        raw_values = [
            state.last_raw_free_slots
            for state in states
            if state.last_raw_free_slots is not None
        ]
        self._fill_last_raw_free_slots = (sum(raw_values)
                                          if raw_values else None)
        self._fill_zero_cost_locations = [
            location for state in states
            for location in state.zero_cost_locations
        ]
        timestamps = [
            state.snapshot_time
            for state in states
            if state.snapshot_time is not None
        ]
        # The oldest component controls aggregate freshness.
        self._fill_snapshot_time = min(timestamps) if timestamps else None
        self._fill_grant = sum(state.grant for state in states)
        self._fill_grant_epoch = None
        self._fill_grant_pool_key = None

    def _pool_fill_states_snapshot(self) -> dict[str, _PoolFillState]:
        states, _ = self._pool_fill_states_snapshot_with_order_revision()
        return states

    def _pool_fill_states_snapshot_with_order_revision(
            self) -> tuple[dict[str, _PoolFillState], int]:
        with self._fill_pool_state_lock:
            return ({
                key: state.detached_copy()
                for key, state in self._fill_pool_states.items()
            }, self._fill_pool_order_revision)

    def get_reserved_capacity_pool_shelter_grant(self, pool_key: str, *,
                                                 service_generation: int,
                                                 physical_cluster_uid: str,
                                                 edge_cap: int) -> int:
        """Return clipped, non-launching shelter from an exact prior edge.

        The broker poller uses this only after a protocol-v2 round failed to
        return an allocation. A generation advance invalidates every launch
        grant and feed, but it must not cull healthy holdings in an unchanged
        physical pool just because that generation's first provider poll
        failed. Pool identity and physical UID fence the carry, and a future
        prior generation is rejected.
        """
        with self._fill_pool_state_lock:
            prior = self._fill_pool_states.get(pool_key)
            if (isinstance(service_generation, bool) or
                    not isinstance(service_generation, int) or
                    service_generation < 1 or
                    not isinstance(physical_cluster_uid, str) or
                    not physical_cluster_uid or isinstance(edge_cap, bool) or
                    not isinstance(edge_cap, int) or edge_cap < 0 or
                    prior is None or prior.protocol_version != 2 or
                    isinstance(prior.service_generation, bool) or
                    not isinstance(prior.service_generation, int) or
                    prior.service_generation < 1 or
                    prior.service_generation > service_generation or
                    prior.physical_cluster_uid != physical_cluster_uid):
                return 0
            return max(0, min(edge_cap, prior.shelter_grant))

    @staticmethod
    def _location_in_pool(location: spot_placer.Location,
                          state: _PoolFillState) -> bool:
        return any(
            spot_placer.locations_match_placement(location, candidate)
            for candidate in state.zero_cost_locations)

    def _fill_pool_key_for_replica(
        self,
        info: 'replica_managers.ReplicaInfo',
        states: dict[str, _PoolFillState],
    ) -> str | None:
        # Read persisted fields without triggering unittest.mock.Mock's dynamic
        # attribute synthesis: only actual row state is provenance authority.
        try:
            persisted = vars(info)
        except TypeError:
            persisted = {}
        persisted_key = persisted.get('reserved_fill_pool_key')
        persisted_generation = persisted.get('reserved_fill_service_generation')
        persisted_uid = persisted.get('reserved_fill_physical_cluster_uid')
        provenance = (persisted_key, persisted_generation, persisted_uid)
        if any(value is not None for value in provenance):
            # Once any v2 origin field exists, the trio is authoritative.  A
            # partial, malformed, retargeted, or future-generation row must not
            # be re-attributed by a coincidentally matching context/location.
            if (not isinstance(persisted_key, str) or not persisted_key or
                    isinstance(persisted_generation, bool) or
                    not isinstance(persisted_generation, int) or
                    persisted_generation < 1 or
                    not isinstance(persisted_uid, str) or not persisted_uid):
                return None
            state = states.get(persisted_key)
            if (state is None or persisted_uid != state.physical_cluster_uid or
                    persisted_generation > state.service_generation):
                return None
            location = info.get_spot_location()
            if (location is None or
                    not self._location_in_pool(location, state)):
                # Explicit origin and persisted placement are one authority
                # tuple.  A retargeted/corrupt row must not consume shelter
                # from either its claimed pool or a coincidentally matching
                # replacement pool.
                return None
            # Older positive generations remain valid for existing holdings:
            # the generation is the immutable launch fence and is expected to
            # trail the service set after later cap/policy heartbeats.
            return persisted_key

        # Only genuinely legacy rows (and ordinary demand rows), for which all
        # three origin fields are absent, may use exact location attribution.
        location = info.get_spot_location()
        if location is None:
            return None
        matches = [
            pool_key for pool_key, state in states.items()
            if self._location_in_pool(location, state)
        ]
        return matches[0] if len(matches) == 1 else None

    @staticmethod
    def _exact_launch_shapes_for_pool(
        state: _PoolFillState,) -> dict[str, tuple[str, int]] | None:
        """Return normalized card -> exact launch shape in location order."""
        try:
            identity = reserved_capacity_broker.parse_pool_identity(
                state.pool_key)
        except (TypeError, ValueError):
            return None
        if identity.protocol_version != 2:
            return None
        shapes: dict[str, tuple[str, int]] = {}
        for location in state.zero_cost_locations:
            accelerators = location.accelerators
            if not isinstance(accelerators, dict) or len(accelerators) != 1:
                return None
            raw_card, raw_count = next(iter(accelerators.items()))
            if (not isinstance(raw_card, str) or not raw_card or
                    isinstance(raw_count, bool) or
                    not isinstance(raw_count, (int, float)) or
                    not float(raw_count).is_integer() or raw_count <= 0):
                return None
            card = raw_card.casefold()
            if card not in identity.gpu_names:
                return None
            shape = (raw_card, int(raw_count))
            prior = shapes.get(card)
            if prior is not None and prior[1] != shape[1]:
                return None
            shapes.setdefault(card, shape)
        return shapes or None

    def fill_demand_sample(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> 'FillDemandSample | None':
        """Work this service can demonstrate on its zero-cost tier.

        Read-only projection for the reserved-fill poller thread, which
        calls it once per poll from _broker_cycle. None means "no usable
        telemetry". For an armed utilization gate, the poller publishes that
        as fresh NULL need: the broker freezes for its 900s blind grace and
        then resumes bounded decay if blindness persists.

        The base class has no per-replica occupancy signal, so it returns
        None. A service that needs static reservation behavior must explicitly
        set utilization_gate: false.
        """
        del replica_infos  # Unused: no occupancy telemetry on the base.
        return None

    def count_zero_cost_holdings(
            self, replica_infos: list['replica_managers.ReplicaInfo']
    ) -> tuple[int, int]:
        """(fill, demand) split of nonterminal zero-cost replicas.

        The broker claim heartbeat reports this split: fill holdings are
        broker property (arbitrated by grants), demand-placed rows are
        demand-protected and exempt from the ceiling. ReplicaInfo's storage
        decoder materializes rows predating reserved_fill as demand -- the
        conservative direction: they keep their shelter and inflate nobody's
        fill count.
        """
        fill = 0
        demand = 0
        for info in replica_infos:
            if info.is_terminal:
                continue
            if not self._replica_on_zero_cost_location(info):
                continue
            if info.reserved_fill:
                fill += 1
            else:
                demand += 1
        return fill, demand

    def count_zero_cost_holdings_by_pool(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        pool_location_keys: dict[str, list[dict[str, Any]]] | None = None,
        pool_authority: dict[str, tuple[str, int]] | None = None,
    ) -> dict[str, tuple[int, int]]:
        """Return the fill/demand holdings split for every v2 pool.

        ``pool_authority`` supplies the durable UID/current generation during
        controller restart, before an in-memory pool snapshot exists. It is
        required to validate explicit v2 provenance; location-only fallback is
        reserved for rows whose entire provenance trio predates protocol v2.
        """
        states = self._pool_fill_states_snapshot()
        if pool_location_keys is not None:
            for pool_key, keys in pool_location_keys.items():
                if pool_key in states:
                    continue
                locations = [
                    location
                    for location in (spot_placer.Location.from_pickleable(key)
                                     for key in keys)
                    if location is not None
                ]
                authority = (pool_authority or {}).get(pool_key)
                physical_uid, generation = (authority if authority is not None
                                            else ('', 0))
                states[pool_key] = _PoolFillState(
                    protocol_version=2,
                    pool_key=pool_key,
                    physical_cluster_uid=(physical_uid),
                    service_generation=generation,
                    edge_cap=0,
                    zero_cost_locations=locations)
        counts = {pool_key: [0, 0] for pool_key in states}
        for info in replica_infos:
            if info.is_terminal:
                continue
            replica_pool_key = self._fill_pool_key_for_replica(info, states)
            if replica_pool_key is None:
                continue
            index = 0 if info.reserved_fill else 1
            counts[replica_pool_key][index] += self._fill_capacity_units(info)
        return {
            pool_key: (values[0], values[1])
            for pool_key, values in counts.items()
        }

    def seed_zero_cost_locations(
            self, zero_cost_location_keys: list[dict[str, Any]]) -> None:
        """Seed the zero-cost location set WITHOUT granting free slots.

        Called synchronously by the controller (at boot, and on the
        autoscaler swap in update_service) with the placer's spec-derived
        location set, BEFORE the seeded instance takes decision ticks.
        After a controller respawn the fill state is empty (boot builds
        the autoscaler via from_spec; there is no dump/load across
        processes) and the first poll can lag the first decision tick by
        a lot (per-location cost warm-up + the cluster-wide realtime
        query). A QPS-family autoscaler's first tick then computes
        target=min_replicas from its empty window and, with
        zero_cost_count=0, suppression cannot shelter the live fill
        fleet from the resulting mass scale-down. Seeding only the
        location set makes zero_cost_count-based suppression work from
        tick zero, while _fill_snapshot_time stays None and free slots
        stay 0 so no new fill is launched until the first real poll.

        A loaded dump wins: never overwrite an existing location set
        (it may carry a fresher view than the spec-derived one).
        """
        if self._fill_zero_cost_locations:
            return
        self._fill_zero_cost_locations = [
            location for location in (spot_placer.Location.from_pickleable(key)
                                      for key in zero_cost_location_keys)
            if location is not None
        ]

    def _fresh_fill_free_slots(self) -> int:
        """Damped free slots, decayed to 0 when the snapshot is stale."""
        if self._fill_snapshot_time is None:
            return 0
        max_age = (reserved_capacity.poll_interval_seconds() *
                   constants.RESERVED_CAPACITY_STALE_AFTER_INTERVALS)
        if time.time() - self._fill_snapshot_time > max_age:
            return 0
        return self._fill_free_slots

    # Kept as a staticmethod alias: the matcher moved to spot_placer so the
    # launch path's demand-placement gate can share it without importing
    # autoscalers (see spot_placer.locations_match_placement for the full
    # relaxed-identity rationale).
    _fill_location_matches = staticmethod(spot_placer.locations_match_placement)

    def _fill_row_occupies_free_slot(
            self, info: 'replica_managers.ReplicaInfo') -> bool:
        """Whether a zero-cost row occupies a slot the snapshot counted free.

        Subtract rows that are (not READY) OR (created after the
        snapshot). Each row is evaluated once against this single
        predicate, so the two clauses can never double-subtract the same
        row:
        - not READY: launched-but-unbound pods are invisible to the
          poller, so their slots still read free. A not-READY row OLDER
          than the snapshot (long provisioning) may in fact have a bound
          pod the poll already excluded; still subtracting it is the
          conservative direction -- never over-launch, at worst
          under-fill until it turns READY (layer 3 re-syncs).
        - created after the snapshot: a DEMAND launch placed on the
          zero-cost tier that binds AND turns READY within one
          inter-poll gap escapes the not-READY clause, yet the slot it
          sits on was counted free when the snapshot was taken. Any
          zero-cost row newer than the snapshot occupies such a slot
          regardless of readiness.
        Rows without a creation timestamp (pickles from builds predating
        ReplicaInfo.created_at) are treated as older than the snapshot:
        they predate this build entirely, their bound pods are already
        excluded by every fresh poll, and always-subtracting them would
        under-fill for their whole lifetime.

        Known sampling window (accepted): a row created BEFORE the
        snapshot whose pod binds after it escapes both clauses once
        READY -- up to one poll interval of over-launch; the extra fill
        fails fast on the full tier and at worst benches it for one
        retry TTL. Inherent to sampling free capacity at an instant.
        """
        if not info.is_ready:
            return True
        if self._fill_snapshot_time is None:
            # No snapshot: spendable free slots are 0 regardless.
            return False
        created_at = info.created_at
        return created_at is not None and created_at > self._fill_snapshot_time

    def _replica_on_zero_cost_location(
            self, info: 'replica_managers.ReplicaInfo') -> bool:
        if not self._fill_zero_cost_locations:
            return False
        location = info.get_spot_location()
        if location is None:
            return False
        return any(
            self._fill_location_matches(location, zero_cost)
            for zero_cost in self._fill_zero_cost_locations)

    def is_replica_on_zero_cost_location(
            self, info: 'replica_managers.ReplicaInfo') -> bool:
        """Whether a replica occupies a configured zero-cost location.

        The controller uses this same classifier for exact-card history.  In
        particular, legacy ReplicaInfo rows predate persisted is_zero_cost
        provenance but still retain enough placement identity to match the
        autoscaler's active reserved locations.
        """
        return self._replica_on_zero_cost_location(info)

    def _fill_capacity_units(self, info: 'replica_managers.ReplicaInfo') -> int:
        """Autoscaling units represented by one row for fill accounting."""
        del info
        return 1

    def _service_ceiling_capacity_units(
            self, info: 'replica_managers.ReplicaInfo') -> int:
        """Project any cleanup-unproven row into the current service unit."""
        capacity_unit = (reserved_fill_planner.FillCapacityUnit.LOGICAL
                         if self.replica_unit == 'logical' else
                         reserved_fill_planner.FillCapacityUnit.PHYSICAL)
        if capacity_unit is reserved_fill_planner.FillCapacityUnit.PHYSICAL:
            return 1
        shapes: set[tuple[str, int]] = set()
        resources_override = getattr(info, 'resources_override', None)
        if resources_override is not None:
            if not isinstance(resources_override, Mapping):
                raise ValueError('Materialized replica resources override is '
                                 'malformed.')
            accelerators = resources_override.get('accelerators')
            if accelerators is not None:
                shapes.add(
                    reserved_fill_planner.exact_accelerator_shape(accelerators))
        try:
            location = info.get_spot_location()
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError('Materialized replica location is malformed.') \
                from error
        if location is not None:
            accelerators = getattr(location, 'accelerators', None)
            if accelerators is not None:
                shapes.add(
                    reserved_fill_planner.exact_accelerator_shape(accelerators))
        if len(shapes) != 1:
            raise ValueError('Materialized replica has missing or conflicting '
                             'accelerator shapes.')
        _, accelerator_count = next(iter(shapes))
        return capacity_unit.intent_cost(accelerator_count)

    def _reserved_fill_row_is_materialized(
            self, info: 'replica_managers.ReplicaInfo') -> bool:
        """Whether the durable row may still own provider capacity."""
        status = getattr(info, 'status_property', None)
        return (status is None or getattr(status, 'sky_down_status', None)
                != common_utils.ProcessStatus.SUCCEEDED)

    def sequenced_reserved_fill_holdings(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> tuple[reserved_fill_planner.MaterializedFillHolding, ...]:
        """Return immutable provider-free facts for typed shelter planning."""
        holdings: list[reserved_fill_planner.MaterializedFillHolding] = []
        for info in replica_infos:
            if (info.reserved_fill is not True or
                    info.is_zero_cost is not True or
                    not self._reserved_fill_row_is_materialized(info)):
                continue
            pool_key = getattr(info, 'reserved_fill_pool_key', None)
            location = info.get_spot_location()
            accelerators = None if location is None else location.accelerators
            if not isinstance(accelerators, dict) or len(accelerators) != 1:
                accelerators = (info.resources_override or
                                {}).get('accelerators')
            card: str | None = None
            accelerator_count: int | None = None
            if isinstance(accelerators, dict) and len(accelerators) == 1:
                raw_card, raw_count = next(iter(accelerators.items()))
                if isinstance(raw_card, str) and raw_card:
                    try:
                        count = int(raw_count)
                    except (TypeError, ValueError):
                        count = 0
                    if count > 0:
                        card = raw_card
                        accelerator_count = count
            holdings.append(
                reserved_fill_planner.MaterializedFillHolding(
                    replica_id=info.replica_id,
                    service_version=info.version,
                    capacity=self._fill_capacity_units(info),
                    pool_key=(pool_key if isinstance(pool_key, str) and pool_key
                              else None),
                    physical_cluster_uid=(getattr(
                        info, 'reserved_fill_physical_cluster_uid', None)),
                    service_generation=getattr(
                        info, 'reserved_fill_service_generation', None),
                    accelerator=card,
                    accelerator_count=accelerator_count,
                ))
        return tuple(holdings)

    def _reserved_fill_committed_capacity(
            self, info: 'replica_managers.ReplicaInfo') -> int:
        """Capacity an ordinary absolute target can already rely on."""
        return self._fill_capacity_units(info)

    def _supports_exact_fill_shape_resolution(self) -> bool:
        """Whether this autoscaler can resolve exact replica GPU shapes."""
        return False

    def _kueue_counts_as_assigned(self,
                                  info: 'replica_managers.ReplicaInfo') -> bool:
        """Neutral Kueue accounting for autoscalers without exact shapes."""
        del info
        return True

    def _resolve_fill_gpu_shape(
            self, info: 'replica_managers.ReplicaInfo') -> tuple[str, int]:
        """Resolve one replica's exact GPU shape for fill accounting."""
        del info
        raise NotImplementedError

    def _exact_card_fill_shelter(
        self,
        zero_cost_infos: list['replica_managers.ReplicaInfo'],
        fill_target: int,
    ) -> tuple[dict[str, int], dict[int, str]] | None:
        """Return per-card scale-down shelter and replica attribution.

        The aggregate fill target overlaps only demand assigned to the same
        exact reserved card. Existing zero-cost holdings receive the target
        first, in configured card order, so a demand-only downscale cannot
        drain one reserved card and immediately refill another. Autoscalers
        without a complete exact-card view retain the legacy aggregate path.
        """
        demand_target = self.target_num_replicas_by_accelerator
        configured_shapes = self.configured_accelerator_shapes
        if (not isinstance(demand_target, dict) or
                not isinstance(configured_shapes, dict) or
                not configured_shapes or
                not self._compatibility_demand_complete):
            return None

        canonical_by_name = {
            str(card).casefold(): str(card) for card in configured_shapes
        }
        demand_by_card: dict[str, int] = {}
        for raw_card, raw_target in demand_target.items():
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is None:
                return None
            demand_by_card[card] = max(0, int(raw_target))
        if sum(demand_by_card.values()) != self.get_final_target_num_replicas():
            # Generic overprovision and stale/partial maps do not have a safe
            # exact-card attribution. The aggregate path still enforces the
            # fill ceiling without guessing where that demand belongs.
            return None

        current_by_card: dict[str, int] = {}
        replica_cards: dict[int, str] = {}
        for info in zero_cost_infos:
            location = info.get_spot_location()
            accelerators = (location.accelerators
                            if location is not None else None)
            if not accelerators or len(accelerators) != 1:
                return None
            raw_card = next(iter(accelerators))
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is None:
                # Old-version or partially launched rows may not have a
                # trustworthy exact shape. Aggregate shelter is conservative
                # and avoids inventing an accelerator identity for them.
                return None
            replica_cards[info.replica_id] = card
            current_by_card[card] = (current_by_card.get(card, 0) +
                                     self._fill_capacity_units(info))

        remaining = max(0, fill_target)
        fill_by_card: dict[str, int] = {}
        for card in configured_shapes:
            canonical = canonical_by_name[str(card).casefold()]
            allocated = min(remaining, current_by_card.get(canonical, 0))
            if allocated > 0:
                fill_by_card[canonical] = allocated
                remaining -= allocated
            if remaining <= 0:
                break
        shelter = {
            card: max(0, fill - demand_by_card.get(card, 0))
            for card, fill in fill_by_card.items()
        }
        return shelter, replica_cards

    def _reserved_slots_claimed_by_demand(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        decisions: list[AutoscalerDecision],
    ) -> tuple[int, dict[str, int] | None, dict[str, int] | None]:
        """Count free exact-card slots already claimed by demand decisions.

        Reserved fill is overlaid after ordinary demand scaling. A shaped
        demand launch can consume one of the same freshly reported reserved
        slots, so emitting the full fill delta as well would create two rows
        for one physical slot. Count only claims that match a currently free
        exact card. Unknown or aggregate decisions retain the legacy fill
        behavior because they cannot be reconciled safely by card here.  The
        third return value preserves the exact-card split so protocol v2 can
        debit only compatible physical pools instead of assigning an H200
        demand claim to (for example) an unrelated L4 pool.
        """
        raw_free = self.free_reserved_slots_by_accelerator
        configured_shapes = self.configured_accelerator_shapes
        if (not isinstance(raw_free, dict) or not raw_free or
                not isinstance(configured_shapes, dict) or
                not configured_shapes or
                not self._supports_exact_fill_shape_resolution()):
            return 0, None, None
        canonical_by_name = {
            str(card).casefold(): str(card) for card in configured_shapes
        }
        remaining_free: dict[str, int] = {}
        for raw_card, raw_count in raw_free.items():
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is None:
                continue
            remaining_free[card] = max(0, int(raw_count))

        current_capacity_by_card = {
            card: 0 for card in canonical_by_name.values()
        }
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info) or
                    not self._kueue_counts_as_assigned(info)):
                continue
            raw_card, _ = self._resolve_fill_gpu_shape(info)
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is not None:
                current_capacity_by_card[card] += self._fill_capacity_units(
                    info)

        claimed = 0
        claimed_by_card: dict[str, int] = {}

        def claim(card: str, count: int) -> None:
            nonlocal claimed
            available = remaining_free.get(card, 0)
            consumed = min(available, max(0, count))
            remaining_free[card] = available - consumed
            claimed += consumed
            if consumed > 0:
                claimed_by_card[card] = (claimed_by_card.get(card, 0) +
                                         consumed)

        for decision in decisions:
            if decision.operator != AutoscalerDecisionOperator.SCALE_UP:
                continue
            target = decision.target
            if isinstance(target, dict):
                if target.get(constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY):
                    continue
                accelerators = target.get('accelerators')
                if not isinstance(accelerators, dict) or len(accelerators) != 1:
                    continue
                raw_card = next(iter(accelerators))
                card = canonical_by_name.get(str(raw_card).casefold())
                if card is not None:
                    claim(card, 1)
                continue
            if not isinstance(target, LogicalScaleTarget):
                continue
            target_by_card = dict(target.target_capacity_by_accelerator)
            shapes = dict(target.accelerator_shapes)
            for raw_card, raw_target in target_by_card.items():
                card = canonical_by_name.get(str(raw_card).casefold())
                if card is None:
                    continue
                raw_gpu_count = shapes.get(raw_card)
                if raw_gpu_count is None:
                    raw_gpu_count = configured_shapes.get(card)
                if (not isinstance(raw_gpu_count, int) or
                        isinstance(raw_gpu_count, bool) or raw_gpu_count <= 0):
                    continue
                gpu_count = raw_gpu_count
                shortfall = max(
                    0,
                    int(raw_target) - current_capacity_by_card.get(card, 0))
                claim(card, math.ceil(shortfall / gpu_count))
        return claimed, remaining_free, claimed_by_card

    def _fresh_pool_fill_free_slots(self, state: _PoolFillState) -> int:
        if state.snapshot_time is None:
            return 0
        max_age = (reserved_capacity.poll_interval_seconds() *
                   constants.RESERVED_CAPACITY_STALE_AFTER_INTERVALS)
        if time.time() - state.snapshot_time > max_age:
            return 0
        return min(state.edge_cap, state.free_slots)

    def _exact_card_pool_shelter(
        self,
        data: dict[str, dict[str, Any]],
        targets: dict[str, int],
        ordered_keys: list[str],
    ) -> tuple[dict[str, dict[str, int]], dict[int, str]] | None:
        """Partition exact-card demand coverage independently by pool."""
        demand_target = self.target_num_replicas_by_accelerator
        configured_shapes = self.configured_accelerator_shapes
        if (not isinstance(demand_target, dict) or
                not isinstance(configured_shapes, dict) or
                not configured_shapes or
                not self._compatibility_demand_complete):
            return None
        canonical_by_name = {
            str(card).casefold(): str(card) for card in configured_shapes
        }
        demand_by_card: dict[str, int] = {}
        for raw_card, raw_target in demand_target.items():
            card = canonical_by_name.get(str(raw_card).casefold())
            if card is None:
                return None
            demand_by_card[card] = max(0, int(raw_target))
        if sum(demand_by_card.values()) != self.get_final_target_num_replicas():
            return None

        replica_cards: dict[int, str] = {}
        targets_by_pool_card: dict[str, dict[str, int]] = {}
        for pool_key in ordered_keys:
            current_by_card: dict[str, int] = {}
            for info in data[pool_key]['infos']:
                location = info.get_spot_location()
                accelerators = (location.accelerators
                                if location is not None else None)
                if not accelerators or len(accelerators) != 1:
                    return None
                raw_card = next(iter(accelerators))
                card = canonical_by_name.get(str(raw_card).casefold())
                if card is None:
                    return None
                replica_cards[info.replica_id] = card
                current_by_card[card] = (current_by_card.get(card, 0) +
                                         self._fill_capacity_units(info))
            remaining = targets[pool_key]
            pool_targets: dict[str, int] = {}
            for configured_card in configured_shapes:
                card = canonical_by_name[str(configured_card).casefold()]
                assigned = min(remaining, current_by_card.get(card, 0))
                if assigned > 0:
                    pool_targets[card] = assigned
                    remaining -= assigned
                if remaining <= 0:
                    break
            targets_by_pool_card[pool_key] = pool_targets

        shelter: dict[str, dict[str, int]] = {
            pool_key: {} for pool_key in ordered_keys
        }
        for configured_card in configured_shapes:
            card = canonical_by_name[str(configured_card).casefold()]
            remaining_demand = demand_by_card.get(card, 0)
            for pool_key in ordered_keys:
                target = targets_by_pool_card[pool_key].get(card, 0)
                covered = min(target, remaining_demand)
                remaining_demand -= covered
                quota = target - covered
                if quota > 0:
                    shelter[pool_key][card] = quota
        return shelter, replica_cards

    def _apply_reserved_capacity_fill_v2(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        decisions: list[AutoscalerDecision],
        states: dict[str, _PoolFillState],
        pool_order_revision: int,
        *,
        emit_legacy_launches: bool = True,
    ) -> list[AutoscalerDecision]:
        """Apply independently fenced pool feeds under one service ceiling.

        ``emit_legacy_launches=False`` retains the complete target/shelter
        calculation while leaving feed and rotation state unchanged.  The
        typed sequenced planner uses that mode and commits only from a durable
        manager receipt.
        """
        if not states:
            return decisions
        ordered_keys = list(states)
        generations = {state.service_generation for state in states.values()}
        if len(generations) != 1:
            # A complete poll publication may never mix budgets. Fail closed
            # if corrupted in-memory state reaches a decision tick.
            logger.error('Reserved-fill protocol-v2 state mixes service '
                         f'generations {generations}; feeding zero.')
            return decisions

        data: dict[str, dict[str, Any]] = {
            key: {
                'count': 0,
                'latest': 0,
                'occupying': 0,
                'demand': 0,
                'demand_latest': 0,
                'infos': [],
            } for key in ordered_keys
        }
        num_nonterminal = 0
        num_latest_nonterminal = 0
        for info in replica_infos:
            if info.is_terminal:
                continue
            units = self._fill_capacity_units(info)
            num_nonterminal += units
            is_latest = info.version == self.latest_version
            if is_latest:
                num_latest_nonterminal += units
            pool_key = self._fill_pool_key_for_replica(info, states)
            if pool_key is None:
                continue
            entry = data[pool_key]
            entry['infos'].append(info)
            entry['count'] += units
            if is_latest:
                entry['latest'] += units
            state = states[pool_key]
            created_at = info.created_at
            if (not info.is_ready or
                (state.snapshot_time is not None and created_at is not None and
                 created_at > state.snapshot_time)):
                entry['occupying'] += units
            if not info.reserved_fill:
                entry['demand'] += units
                if is_latest:
                    entry['demand_latest'] += units

        spendable: dict[str, int] = {
            key: max(
                0,
                self._fresh_pool_fill_free_slots(states[key]) -
                int(data[key]['occupying'])) for key in ordered_keys
        }
        # Ordinary decisions are emitted before this overlay and may consume
        # a just-observed reserved slot. Debit each exact-card claim only from
        # pools that can serve that card. If several contexts expose the same
        # card, the ordinary decision does not yet carry its eventual context;
        # debit the claim from every compatible pool. This intentionally
        # withholds some fill while placement is ambiguous, but guarantees that
        # whichever context demand selects cannot receive both the demand launch
        # and fill for the same physical slot.
        (_, remaining_global_free_by_card, demand_reserved_claims_by_card) = (
            self._reserved_slots_claimed_by_demand(replica_infos, decisions))
        pool_cards: dict[str, frozenset[str]] = {}
        pool_shapes: dict[str, dict[str, tuple[str, int]] | None] = {}
        pool_exact_slots: dict[str, dict[str, int] | None] = {}
        for key in ordered_keys:
            try:
                identity = reserved_capacity_broker.parse_pool_identity(key)
            except (TypeError, ValueError):
                logger.error('Reserved-fill protocol-v2 state has a malformed '
                             f'pool key {key!r}; feeding it zero.')
                spendable[key] = 0
                pool_shapes[key] = None
                pool_exact_slots[key] = {}
                continue
            if identity.protocol_version != 2:
                logger.error('Reserved-fill protocol-v2 state has a non-v2 '
                             f'pool key {key!r}; feeding it zero.')
                spendable[key] = 0
                pool_shapes[key] = None
                pool_exact_slots[key] = {}
                continue
            pool_cards[key] = frozenset(identity.gpu_names)
            shapes = self._exact_launch_shapes_for_pool(states[key])
            pool_shapes[key] = shapes
            exact_slots = states[key].free_slots_by_accelerator
            if exact_slots is None:
                pool_exact_slots[key] = None
            elif (shapes is None or
                  any(card not in shapes for card in exact_slots)):
                # A present per-card feed is authoritative. If it cannot be
                # translated back to an exact task shape, never degrade it to
                # an aggregate launch.
                logger.error('Reserved-fill protocol-v2 exact-card feed does '
                             f'not match pool locations for {key!r}; '
                             'withholding its launches.')
                pool_exact_slots[key] = {}
                spendable[key] = 0
            else:
                pool_exact_slots[key] = dict(exact_slots)
        if demand_reserved_claims_by_card:
            for card, claimed_slots in demand_reserved_claims_by_card.items():
                canonical_card = str(card).casefold()
                for key in ordered_keys:
                    if canonical_card not in pool_cards.get(key, frozenset()):
                        continue
                    spendable[key] = max(0, spendable[key] - claimed_slots)
                    exact_slots = pool_exact_slots[key]
                    if exact_slots is not None:
                        exact_slots[canonical_card] = max(
                            0,
                            exact_slots.get(canonical_card, 0) - claimed_slots)

        targets: dict[str, int] = {}
        launch_targets: dict[str, int] = {}
        for key in ordered_keys:
            state = states[key]
            entry = data[key]
            ceiling = state.shelter_grant + int(entry['demand'])
            launch_ceiling = state.grant + int(entry['demand_latest'])
            targets[key] = min(
                int(entry['count']) + spendable[key], ceiling,
                self.max_replicas)
            launch_targets[key] = min(
                int(entry['latest']) + spendable[key], launch_ceiling,
                self.max_replicas)
            state.fill_target = targets[key]

        remaining_target_budget = self.max_replicas
        for key in ordered_keys:
            targets[key] = min(targets[key], remaining_target_budget)
            remaining_target_budget -= targets[key]

        result = list(decisions)

        # Partition the exact v1 shelter equation over pools. When complete
        # exact-card demand telemetry exists, run the same coverage equation
        # independently per card before the stable pool pass.
        exact_shelter = self._exact_card_pool_shelter(data, targets,
                                                      ordered_keys)
        shelter_quota: dict[str, int] = {}
        if exact_shelter is None:
            remaining_demand = min(self.get_final_target_num_replicas(),
                                   sum(targets.values()))
            for key in ordered_keys:
                covered = min(targets[key], remaining_demand)
                remaining_demand -= covered
                shelter_quota[key] = max(0, targets[key] - covered)

        id_to_info = {info.replica_id: info for info in replica_infos}
        victims_by_pool: dict[str, list[int]] = {key: [] for key in ordered_keys}
        for index, decision in enumerate(decisions):
            if decision.operator != AutoscalerDecisionOperator.SCALE_DOWN:
                continue
            assert isinstance(decision.target, (int, LogicalScaleDownTarget))
            victim = id_to_info.get(_scale_down_replica_id(decision.target))
            if victim is None:
                continue
            pool_key = self._fill_pool_key_for_replica(victim, states)
            if pool_key is not None:
                victims_by_pool[pool_key].append(index)
        suppressed: set[int] = set()
        for key in ordered_keys:
            if exact_shelter is not None:
                quotas_by_card, replica_cards = exact_shelter
                remaining_by_card = dict(quotas_by_card[key])
                for index in reversed(victims_by_pool[key]):
                    victim_target = decisions[index].target
                    assert isinstance(victim_target,
                                      (int, LogicalScaleDownTarget))
                    victim = id_to_info[_scale_down_replica_id(victim_target)]
                    card = replica_cards[victim.replica_id]
                    if remaining_by_card.get(card, 0) <= 0:
                        continue
                    suppressed.add(index)
                    remaining_by_card[card] = max(
                        0, remaining_by_card[card] -
                        self._fill_capacity_units(victim))
            else:
                remaining = shelter_quota[key]
                for index in reversed(victims_by_pool[key]):
                    if remaining <= 0:
                        break
                    victim_target = decisions[index].target
                    assert isinstance(victim_target,
                                      (int, LogicalScaleDownTarget))
                    victim = id_to_info[_scale_down_replica_id(victim_target)]
                    suppressed.add(index)
                    remaining -= self._fill_capacity_units(victim)
        if suppressed:
            result = [
                decision for index, decision in enumerate(decisions)
                if index not in suppressed
            ]

        num_old_nonterminal = num_nonterminal - num_latest_nonterminal
        demand_target = self.get_final_target_num_replicas()
        planned_total = (num_old_nonterminal +
                         max(num_latest_nonterminal, demand_target))
        hard_headroom = (max(0, self.max_replicas -
                             planned_total) if emit_legacy_launches else 0)
        emitted_by_pool: dict[str, int] = {key: 0 for key in ordered_keys}
        emitted_by_pool_card: dict[str, dict[str, int]] = {
            key: {} for key in ordered_keys
        }
        # Additive round compatibility: a round written before the broker
        # persisted its exact-card split still carries valid aggregate
        # authority.  If this autoscaler independently has exact-card
        # telemetry, use one shared, debit-aware budget across every legacy
        # pool instead of multiplying it once per pool.  With no exact
        # telemetry at all, retain the old unshaped launch behavior.
        global_exact_slots: dict[str, int] | None = None
        if remaining_global_free_by_card is not None:
            global_exact_slots = {}
            for raw_card, raw_count in remaining_global_free_by_card.items():
                if (isinstance(raw_card, str) and raw_card and
                        not isinstance(raw_count, bool) and
                        isinstance(raw_count, int) and raw_count >= 0):
                    card = raw_card.casefold()
                    global_exact_slots[card] = (
                        global_exact_slots.get(card, 0) + raw_count)
        launch_remaining: dict[str, int] = {}
        launch_overrides: dict[str, dict[str, Any]] = {}
        launch_exact_slots: dict[str, dict[str, int] | None] = {}
        for key in ordered_keys:
            entry = data[key]
            # The stable target partition is also the durable scale-down
            # authority for each pool. Never interleave a launch beyond that
            # pool's partition: the next tick would immediately select the
            # out-of-target replica as a victim and churn provider capacity.
            pool_launch_target = min(launch_targets[key], targets[key])
            desired = max(0, pool_launch_target - int(entry['latest']))
            if desired <= 0:
                continue
            state = states[key]
            override: dict[str, Any] = {
                constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True,
                constants.RESERVED_FILL_PROTOCOL_VERSION_OVERRIDE_KEY:
                    state.protocol_version,
                constants.RESERVED_FILL_POOL_KEY_OVERRIDE_KEY: key,
                constants.RESERVED_FILL_SERVICE_GENERATION_OVERRIDE_KEY:
                    state.service_generation,
                constants.RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY:
                    state.physical_cluster_uid,
                constants.RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY: [
                    location.to_pickleable()
                    for location in state.zero_cost_locations
                ],
            }
            if state.grant_epoch is not None:
                override[constants.RESERVED_FILL_GRANT_EPOCH_OVERRIDE_KEY] = (
                    state.grant_epoch)
            exact_slots = pool_exact_slots[key]
            if exact_slots is None and global_exact_slots is not None:
                exact_slots = global_exact_slots
            if exact_slots is not None and pool_shapes[key] is None:
                # A present exact-card budget is authoritative.  If it cannot
                # be expressed as one of this pool's exact location shapes,
                # never silently fall back to an aggregate launch.
                continue
            launch_remaining[key] = desired
            launch_overrides[key] = override
            launch_exact_slots[key] = exact_slots

        # Interleave independent physical pools one launch at a time. Provider
        # admission and durable reservation happen serially in the replica
        # manager, and a large/slow/broken first pool can otherwise consume the
        # whole validity window before a later pool is attempted. Rotate the
        # first actionable pool across waves too: provider-phase contention
        # intentionally stops a batch, so a fixed first pool would still be
        # able to starve every later pool before the within-wave round robin
        # gets its first turn.
        with self._fill_pool_state_lock:
            last_started_key = self._fill_pool_last_started_key
        rotated_keys = ordered_keys
        if last_started_key in ordered_keys:
            start = ordered_keys.index(last_started_key) + 1
            rotated_keys = ordered_keys[start:] + ordered_keys[:start]
        launch_order = [
            key for key in rotated_keys if launch_remaining.get(key, 0) > 0
        ]
        first_emitted_key: str | None = None
        while hard_headroom > 0:
            made_progress = False
            for key in launch_order:
                if hard_headroom <= 0:
                    break
                remaining = launch_remaining.get(key, 0)
                if remaining <= 0:
                    continue
                override = launch_overrides[key]
                exact_slots = launch_exact_slots[key]
                if exact_slots is None:
                    # No exact-card measurement exists in either authority
                    # path. This is the compatibility behavior for an old v2
                    # round.
                    result.extend(_generate_scale_up_decisions(1, override))
                    if first_emitted_key is None:
                        first_emitted_key = key
                    emitted_by_pool[key] += 1
                    launch_remaining[key] = remaining - 1
                    hard_headroom -= 1
                    made_progress = True
                    continue

                shapes = pool_shapes[key]
                assert shapes is not None
                for card, (display_card, gpu_count) in shapes.items():
                    available = max(0, int(exact_slots.get(card, 0)))
                    if available <= 0:
                        continue
                    shaped_override = dict(override)
                    shaped_override['accelerators'] = {display_card: gpu_count}
                    result.extend(
                        _generate_scale_up_decisions(1, shaped_override))
                    if first_emitted_key is None:
                        first_emitted_key = key
                    exact_slots[card] = available - 1
                    emitted_by_pool_card[key][card] = (
                        emitted_by_pool_card[key].get(card, 0) + 1)
                    emitted_by_pool[key] += 1
                    launch_remaining[key] = remaining - 1
                    hard_headroom -= 1
                    made_progress = True
                    break
            if not made_progress:
                break

        fill: list[AutoscalerDecision] = []
        if first_emitted_key is not None:
            num_fill_decisions = sum(emitted_by_pool.values())
            fill = result[-num_fill_decisions:]

        pool_authority_is_current = False
        with self._fill_pool_state_lock:
            pool_order_is_current = (
                self._fill_pool_order_revision == pool_order_revision)
            if pool_order_is_current:
                pool_authority_is_current = True
                for key, source in states.items():
                    live = self._fill_pool_states.get(key)
                    if (live is None or live.service_generation
                            != source.service_generation or
                            live.physical_cluster_uid
                            != source.physical_cluster_uid or
                            live.grant_epoch != source.grant_epoch):
                        pool_authority_is_current = False
                        break
            if pool_order_is_current and pool_authority_is_current:
                # Target/headroom and shelter are partitioned across the full
                # ordered map. Commit only if every pool still has the exact
                # authority used by that coupled calculation.
                for key, target in targets.items():
                    self._fill_pool_states[key].fill_target = target
                self._fill_target = sum(
                    state.fill_target
                    for state in self._fill_pool_states.values())
                if first_emitted_key is not None:
                    for key, emitted in emitted_by_pool.items():
                        if emitted <= 0:
                            continue
                        live = self._fill_pool_states[key]
                        live.free_slots = max(0, live.free_slots - emitted)
                        if live.last_raw_free_slots is not None:
                            live.last_raw_free_slots = max(
                                0, live.last_raw_free_slots - emitted)
                        if live.free_slots_by_accelerator is not None:
                            for card, card_emitted in emitted_by_pool_card[
                                    key].items():
                                live.free_slots_by_accelerator[card] = max(
                                    0,
                                    live.free_slots_by_accelerator.get(card, 0)
                                    - card_emitted)
                    self._fill_pool_last_started_key = first_emitted_key
                    self._refresh_legacy_fill_projection_locked()
            else:
                # Publication installs replacement live states with their own
                # per-pool targets (normally zero). Keep the aggregate status
                # projection aligned even though this detached overlay rolls
                # back without mutating feed or rotation state.
                self._fill_target = sum(
                    state.fill_target
                    for state in self._fill_pool_states.values())

        if not pool_order_is_current or not pool_authority_is_current:
            # A lifecycle or authority boundary superseded part of this
            # globally coupled calculation. Preserve the caller's exact
            # ordinary work; a fresh tick retries the complete live map.
            return decisions

        ordinary = [
            decision for index, decision in enumerate(decisions)
            if index not in suppressed
        ]
        result = ordinary
        if first_emitted_key is not None:
            # Reserved fill is computed after ordinary decisions so the
            # conservative exact-card debits above can account for every
            # demand launch. Execution order need not match computation
            # order, though. A LogicalScaleTarget is a blocking manager call
            # that may reconcile a large paid launch budget before the next
            # decision is handled. If it precedes protocol-v2 fill, the
            # broker epoch can expire without any zero-cost launch reaching
            # admission. Put this tick's already-debited fill decisions before
            # the first ordinary scale-up while retaining any leading
            # scale-down decisions and their existing ordering contract.
            first_ordinary_up = next(
                (index for index, decision in enumerate(ordinary)
                 if decision.operator == AutoscalerDecisionOperator.SCALE_UP),
                len(ordinary))
            result = (ordinary[:first_ordinary_up] + fill +
                      ordinary[first_ordinary_up:])
        return result

    def _apply_reserved_capacity_fill(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        decisions: list[AutoscalerDecision],
    ) -> list[AutoscalerDecision]:
        """Overlay zero-cost capacity fill onto the demand decisions.

        fill_target = (nonterminal replicas already on a zero-cost
        location) + (spendable free slots: fresh damped free slots minus
        launched-but-not-READY latest zero-cost replicas), clamped to
        max_replicas but deliberately NOT floored by min_replicas -- an
        empty free tier must not assert a floor. target_num_replicas and
        thus the controller's capacity hint stay DEMAND-ONLY: fill
        replicas are opportunistic supply, and the platform's spill
        logic must not read them as demand.

        - Every spendable free slot that fits below the hard aggregate
          max_replicas ceiling carries the sentinel override so the launch
          path pins it to zero-cost ACTIVE locations only (and skips entirely
          when none is). Demand and rolling-update launches reserve their
          planned ceiling headroom first, but do not otherwise suppress fill.
        - Scale-downs covered by the fill surplus are suppressed, taking
          the shelter quota from the TAIL of the zero-cost victims: the
          subclass ordered its victims most-preferred-first (initializing
          replicas before READY ones), so when the surplus only covers
          part of them the ones sheltered must be the LEAST preferred --
          a prefix keep would shelter a warming PROVISIONING replica
          while killing the READY one serving traffic. Output order (and
          the cost-aware / drain-aware selection itself) is untouched.
        - With a stale snapshot, fill_target degrades to exactly the
          zero-cost replica count: existing fill replicas are protected
          from victimization by staleness, but no new fill is launched.
        """
        if not self.reserved_capacity_fill:
            return decisions
        if self._sequenced_reserved_fill_planning_enabled():
            # A typed PostgreSQL shelter is composed by the controller and
            # enforced by ReplicaManager.  Mutating or consulting the legacy
            # feed here would recreate the restart race this boundary removes.
            self._fill_target = 0
            return decisions
        (pool_states, pool_order_revision) = (
            self._pool_fill_states_snapshot_with_order_revision())
        if pool_states:
            return self._apply_reserved_capacity_fill_v2(
                replica_infos,
                decisions,
                pool_states,
                pool_order_revision,
                emit_legacy_launches=True)
        # Zero-cost accounting is version-asymmetric by design; the
        # four roles use different version scopes:
        # - LAUNCH TARGET: latest-version zero-cost rows only. Old-version
        #   zero-cost replicas (a rolling update draining its previous fleet)
        #   would otherwise inflate the target and compound fill launches.
        #   The HARD CEILING below is deliberately all-version: old rows still
        #   occupy physical capacity and must reduce aggregate headroom.
        # - OCCUPANCY DEBIT: all versions. ANY nonterminal zero-cost row
        #   whose pod may be unbound (not READY, or created after the
        #   snapshot) holds a claim on a slot the snapshot counted free
        #   regardless of version -- an old-version PROVISIONING row
        #   left out of the debit would let a fill launch collide with
        #   its claim, fail on capacity, and bench the zero-cost tier.
        # - SUPPRESSION: all versions. Every existing zero-cost replica
        #   occupies free-tier capacity regardless of version and
        #   deserves shelter from DEMAND scale-downs; sheltering is
        #   bounded by the victims actually present (demand victims are
        #   latest-version, and the outdated-version drain bypasses this
        #   overlay entirely).
        # - CEILING: split by side, mirroring the asymmetry above. The
        #   LAUNCH-side ceiling (grant + demand-placed rows riding on top
        #   of it) counts latest-version demand-placed rows only: it caps
        #   fill_target_launch, which is latest-only, and an old-version
        #   demand row draining through a rolling update would otherwise
        #   inflate the ceiling and let fill overshoot the grant by its
        #   count (bench churn against peers). The TARGET/SHELTER-side
        #   ceiling keeps the all-version count, consistent with
        #   all-version suppression: every existing demand-placed row
        #   deserves its exemption regardless of version.
        zero_cost_count = 0
        zero_cost_latest = 0
        zero_cost_occupying = 0
        zero_cost_demand_placed = 0
        zero_cost_demand_placed_latest = 0
        num_nonterminal = 0
        num_latest_nonterminal = 0
        zero_cost_infos: list[replica_managers.ReplicaInfo] = []
        for info in replica_infos:
            if info.is_terminal:
                continue
            capacity_units = self._fill_capacity_units(info)
            num_nonterminal += capacity_units
            is_latest = info.version == self.latest_version
            if is_latest:
                num_latest_nonterminal += capacity_units
            if self._replica_on_zero_cost_location(info):
                zero_cost_infos.append(info)
                zero_cost_count += capacity_units
                if is_latest:
                    zero_cost_latest += capacity_units
                if self._fill_row_occupies_free_slot(info):
                    zero_cost_occupying += capacity_units
                # reserved_fill is the persisted launch-origin flag: only
                # sentinel (fill) launches carry it. Demand-placed
                # zero-cost rows are demand-protected, not broker
                # property, so the grant ceiling below exempts them. Rows
                # pickled before the flag existed read as demand
                # (__setstate__ default False, same as the claim-heartbeat
                # split in count_zero_cost_holdings): they keep their
                # shelter but stay ceiling-exempt until natural churn
                # replaces them with flagged rows.
                if not info.reserved_fill:
                    zero_cost_demand_placed += capacity_units
                    if is_latest:
                        zero_cost_demand_placed_latest += capacity_units
        # Three defense layers keep fill launches within physical free
        # capacity:
        # 1. Emission-time spend (below): free-slot memory is deducted
        #    the moment launch decisions are emitted, covering the
        #    intra-poll window.
        # 2. Occupied-slot subtraction (here): zero-cost replicas of ANY
        #    version that are not READY (pods invisible to the poller --
        #    launch threads can queue for multiple poll intervals) or
        #    that were created after the snapshot (e.g. a demand launch
        #    landing on the zero-cost tier and turning READY within one
        #    inter-poll gap) occupy slots the snapshot counted free, so
        #    they are subtracted from the spendable free level (see
        #    _fill_row_occupies_free_slot). This may overlap with slots
        #    the poller already excluded once pods bind; subtracting is
        #    the conservative direction -- never over-launch, worst case
        #    under-fill for one poll.
        # 3. Poll re-sync: subsequent snapshots restore the true level
        #    (immediately on decrease, two-poll damped on increase).
        spendable_free_slots = max(
            0,
            self._fresh_fill_free_slots() - zero_cost_occupying)
        # Broker grant ceiling: the one new actuator arbitration needs.
        # The #108 fill target is structurally >= current holdings, so
        # lowering the FEED alone can never shrink a fleet; capping the
        # target at grant + demand-placed rows makes holdings above the
        # ceiling lose their scale-down shelter, and the normal graceful,
        # drain-aware scale-down returns the machines. None = no ceiling
        # (single-service identity). Demand-placed zero-cost rows ride on
        # top of the grant: they are demand-protected, and the broker
        # already excludes them from the fill capacity it arbitrates.
        # Version scope per side per the CEILING note above: launch-side
        # counts latest-version demand-placed rows only.
        fill_ceiling: int | None = None
        fill_ceiling_launch: int | None = None
        if self._fill_grant is not None:
            fill_ceiling = self._fill_grant + zero_cost_demand_placed
            fill_ceiling_launch = (self._fill_grant +
                                   zero_cost_demand_placed_latest)
        fill_target = min(zero_cost_count + spendable_free_slots,
                          self.max_replicas)
        if fill_ceiling is not None:
            fill_target = min(fill_target, fill_ceiling)
        self._fill_target = fill_target
        demand_target = self.get_final_target_num_replicas()
        surplus_covered = fill_target - demand_target
        # Keep this overlay side-effect free for callers that retain the
        # ordinary decision list for later policy checks.
        result = list(decisions)
        exact_shelter = self._exact_card_fill_shelter(zero_cost_infos,
                                                      fill_target)
        if surplus_covered > 0 or exact_shelter is not None:
            # Victim-aware suppression: shelter ONLY scale-downs whose victim
            # replica sits on a zero-cost location, up to the fill surplus.
            # Downs targeting paid replicas always pass through -- fill
            # surplus must never keep a PAID replica alive (the subclass
            # orders victims newest-first, so a victim-blind prefix keep
            # could shelter a paid replica indefinitely while repeatedly
            # killing and relaunching zero-cost ones).
            id_to_info = {info.replica_id: info for info in replica_infos}
            # Take the shelter quota from the TAIL of the zero-cost victims:
            # the subclass emits victims most-preferred-first, so a partial
            # surplus must shelter the LEAST-preferred ones (e.g. keep the
            # READY replica serving traffic, not the PROVISIONING one ahead
            # of it in the list). Two passes so output order is preserved.
            zero_cost_decisions = []
            for idx, decision in enumerate(decisions):
                if decision.operator == AutoscalerDecisionOperator.SCALE_DOWN:
                    assert isinstance(decision.target,
                                      (int, LogicalScaleDownTarget))
                    victim = id_to_info.get(
                        _scale_down_replica_id(decision.target))
                    if (victim is not None and
                            self._replica_on_zero_cost_location(victim)):
                        zero_cost_decisions.append((idx, victim))
            suppressed_ids: set[int] = set()
            if exact_shelter is not None:
                shelter_by_card, replica_cards = exact_shelter
                remaining_by_card = dict(shelter_by_card)
                for idx, victim in reversed(zero_cost_decisions):
                    card = replica_cards[victim.replica_id]
                    if remaining_by_card.get(card, 0) <= 0:
                        continue
                    suppressed_ids.add(idx)
                    remaining_by_card[card] = max(
                        0, remaining_by_card[card] -
                        self._fill_capacity_units(victim))
            else:
                suppressed_ids = {
                    idx for idx, _ in zero_cost_decisions[-surplus_covered:]
                }
            result = [
                decision for idx, decision in enumerate(decisions)
                if idx not in suppressed_ids
            ]
        # Launch target: latest-version zero-cost replicas only (see the
        # version-asymmetry note above). Fill intent is independent of demand;
        # the hard aggregate headroom calculation below separately reserves
        # latest demand and counts every old-version nonterminal row.
        fill_target_launch = min(zero_cost_latest + spendable_free_slots,
                                 self.max_replicas)
        if fill_ceiling_launch is not None:
            # Launch-side ceiling: a feed above the remaining grant
            # headroom (e.g. a stale feed raced by a peer's launch) must
            # not push the fleet past its entitlement. Latest-only
            # demand-placed exemption here (see the CEILING note above):
            # old-version demand rows must not inflate launches during a
            # rolling update.
            fill_target_launch = min(fill_target_launch, fill_ceiling_launch)
        desired_fill_up = max(0, fill_target_launch - zero_cost_latest)
        (demand_reserved_claims, remaining_free_by_card,
         _) = self._reserved_slots_claimed_by_demand(replica_infos, decisions)
        desired_fill_up = max(0, desired_fill_up - demand_reserved_claims)
        if remaining_free_by_card is not None:
            desired_fill_up = min(desired_fill_up,
                                  sum(remaining_free_by_card.values()))
        num_old_nonterminal = num_nonterminal - num_latest_nonterminal
        planned_total = (num_old_nonterminal +
                         max(num_latest_nonterminal, demand_target))
        hard_ceiling_headroom = max(0, self.max_replicas - planned_total)
        num_fill_up = min(desired_fill_up, hard_ceiling_headroom)
        if num_fill_up <= 0 and self._fill_grant:
            # A pool that is granted capacity but launches nothing is
            # indistinguishable from a pool with nothing to launch, because
            # the success line below is the only one emitted. That ambiguity
            # cost a full debugging session against a fleet holding one
            # replica while the broker fed it thirty. Name the term that is
            # actually zero.
            snapshot_age = (None if self._fill_snapshot_time is None else round(
                time.time() - self._fill_snapshot_time, 1))
            logger.info(
                f'Reserved-capacity fill: no launch. spendable free slots '
                f'{spendable_free_slots} (raw feed {self._fill_free_slots}, '
                f'fresh {self._fresh_fill_free_slots()}, snapshot age '
                f'{snapshot_age}s, zero-cost occupying '
                f'{zero_cost_occupying}), desired {desired_fill_up}, '
                f'launch target {fill_target_launch}, latest zero-cost '
                f'{zero_cost_latest}, grant {self._fill_grant}, ceiling '
                f'{fill_ceiling_launch}, demand target {demand_target}, '
                f'hard-ceiling headroom {hard_ceiling_headroom}.')
        if num_fill_up > 0:
            logger.info(f'Reserved-capacity fill: launch target '
                        f'{fill_target_launch} (latest zero-cost replicas '
                        f'{zero_cost_latest} + spendable free slots '
                        f'{spendable_free_slots}), demand target '
                        f'{demand_target}, planned total {planned_total}, '
                        f'demand-reserved claims {demand_reserved_claims}, '
                        f'hard-ceiling headroom {hard_ceiling_headroom}; '
                        f'scaling up {num_fill_up} '
                        'zero-cost-only replica(s).')
            fill_override: dict[str, Any] = {
                constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True
            }
            if self._fill_grant_epoch is not None:
                # Epoch fencing: the launch path re-checks this against
                # the POOL's current round epoch right before committing
                # (epochs are per-pool, so the pool key rides along).
                # Attached only when a broker round supplied one, so the
                # pre-broker decision shape (and every existing test) is
                # unchanged.
                fill_override[
                    constants.RESERVED_FILL_GRANT_EPOCH_OVERRIDE_KEY] = (
                        self._fill_grant_epoch)
                if self._fill_grant_pool_key is not None:
                    fill_override[
                        constants.RESERVED_FILL_POOL_KEY_OVERRIDE_KEY] = (
                            self._fill_grant_pool_key)
                    fill_override[
                        constants.
                        RESERVED_FILL_PROTOCOL_VERSION_OVERRIDE_KEY] = (
                            self._fill_protocol_version)
                    fill_override[
                        constants.
                        RESERVED_FILL_SERVICE_GENERATION_OVERRIDE_KEY] = (
                            self._fill_service_generation)
                    if self._fill_physical_cluster_uid is not None:
                        fill_override[
                            constants.
                            RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY] = (
                                self._fill_physical_cluster_uid)
            if remaining_free_by_card is None:
                result.extend(
                    _generate_scale_up_decisions(num_fill_up, fill_override))
            else:
                configured_shapes = self.configured_accelerator_shapes
                remaining = num_fill_up
                for card, raw_gpu_count in configured_shapes.items():
                    if remaining <= 0:
                        break
                    if (not isinstance(raw_gpu_count, int) or
                            isinstance(raw_gpu_count, bool) or
                            raw_gpu_count <= 0):
                        continue
                    launches = min(remaining,
                                   remaining_free_by_card.get(card, 0))
                    exact_fill_override = {
                        **fill_override,
                        'accelerators': {
                            card: raw_gpu_count
                        },
                    }
                    result.extend(
                        _generate_scale_up_decisions(launches,
                                                     exact_fill_override))
                    remaining -= launches
                if remaining > 0:
                    # Exact free-slot telemetry was present, so never guess a
                    # card for the unaccounted remainder. A later poll can
                    # restore the conservatively withheld fill.
                    num_fill_up -= remaining
            # Invariant: a free slot is SPENT the moment a launch decision
            # is emitted, not when the poller next observes the pod. Fill
            # launches persist replica rows immediately, so
            # zero_cost_count already grows on the next tick while the
            # snapshot only refreshes on the poll interval -- without this
            # deduction the same static snapshot would be re-consumed
            # every tick, compounding the fill fleet. Deduct from BOTH the
            # damped value and the last raw poll value:
            # collect_reserved_capacity re-raises the damped value from
            # min(prev_raw, new) on the next poll, so an undeducted stale
            # prev_raw would re-grant the spent slots after a single poll,
            # defeating the two-poll damping. The next polls re-sync the
            # true level (immediate on decrease, damped on increase).
            # This read-modify-write races the poller thread's
            # collect_reserved_capacity (no lock, same as the other
            # cross-thread gauges here): worst case one poll's decrease
            # is overwritten for a single interval, and the resulting
            # over-launch fails fast on the benched location and is
            # re-synced by the next poll.
            self._fill_free_slots = max(0, self._fill_free_slots - num_fill_up)
            if self._fill_last_raw_free_slots is not None:
                self._fill_last_raw_free_slots = max(
                    0, self._fill_last_raw_free_slots - num_fill_up)
        return result

    def has_recomputed_with_fresh_data(self) -> bool:
        """Whether target_num_replicas reflects a fresh-data recompute.

        QPS/queue autoscalers recompute from always-available signals on
        every tick, so their target is never the rebuilt-blind minimum.
        The concurrency autoscaler overrides this: after a controller
        restart its target stays at min_replicas until the first
        decision tick that consumed a fresh demand report, and the
        capacity hint must keep flooring until then.
        """
        return True

    def info(self) -> dict[str, Any]:
        """Get information about the autoscaler."""
        info: dict[str, Any] = {
            'target_num_replicas': self.target_num_replicas,
            'min_replicas': self.min_replicas,
            'max_replicas': self.max_replicas,
            'min_replicas_by_accelerator': dict(self.min_replicas_by_accelerator
                                               ),
            'target_num_replicas_by_accelerator': dict(
                self.target_num_replicas_by_accelerator),
            'demand_target_by_accelerator': dict(
                self.target_num_replicas_by_accelerator),
            'capacity_target_by_accelerator': dict(
                getattr(self, 'capacity_target_by_accelerator', {})),
            'capacity_target_complete': getattr(self,
                                                'capacity_target_complete',
                                                False),
            'zero_cost_padding_target_by_accelerator': dict(
                getattr(self, 'zero_cost_padding_target_by_accelerator', {})),
            'warm_retention_target_by_accelerator': dict(
                self.warm_retention_target_by_accelerator),
            'cold_launch_authority_by_accelerator': dict(
                self.cold_launch_authority_by_accelerator),
        }
        request_timestamps = self.request_timestamps
        request_window_seconds = self.qps_window_size
        if (isinstance(request_timestamps, list) and
                isinstance(request_window_seconds, int) and
                request_window_seconds > 0):
            cutoff = time.time() - request_window_seconds
            recent_request_count = sum(
                timestamp >= cutoff for timestamp in request_timestamps)
            info.update({
                'recent_request_count': recent_request_count,
                'request_window_seconds': request_window_seconds,
                'requests_per_second': recent_request_count /
                                       request_window_seconds,
            })
        if self.reserved_capacity_fill:
            # target_num_replicas above stays demand-only; the fill
            # overlay is observable through these keys instead.
            snapshot_age = (time.time() - self._fill_snapshot_time
                            if self._fill_snapshot_time is not None else None)
            info.update({
                'fill_free_slots': self._fill_free_slots,
                'fill_snapshot_age': snapshot_age,
                'fill_target': self._fill_target,
            })
            pool_states = self._pool_fill_states_snapshot()
            if pool_states:
                now = time.time()
                info['fill_by_pool'] = {
                    pool_key: {
                        'free_slots': state.free_slots,
                        'snapshot_age':
                            (None if state.snapshot_time is None else now -
                             state.snapshot_time),
                        'fill_target': state.fill_target,
                        'edge_cap': state.edge_cap,
                        'grant': state.grant,
                        'shelter_grant': state.shelter_grant,
                        'service_generation': state.service_generation,
                        'physical_cluster_uid': state.physical_cluster_uid,
                    } for pool_key, state in pool_states.items()
                }
        return info

    def get_ready_replica_capacity(self,
                                   info: 'replica_managers.ReplicaInfo') -> int:
        """Return the public replica units currently ready on one backend."""
        return 1 if info.is_ready else 0

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on replica information."""
        raise NotImplementedError

    def _dump_dynamic_states(self) -> dict[str, Any]:
        """Dump dynamic states from autoscaler."""
        raise NotImplementedError

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        """Load dynamic states to autoscaler."""
        raise NotImplementedError

    # --------------- Utility Functions ---------------

    def _clip_target_num_replicas(self, target_num_replicas: int) -> int:
        """Clip target number of replicas with current minimal and maximum
        number of replicas.
        """
        return max(self.min_replicas, min(self.max_replicas,
                                          target_num_replicas))

    @classmethod
    def from_spec(cls,
                  service_name: str,
                  spec: 'service_spec.SkyServiceSpec',
                  version: int = constants.INITIAL_VERSION) -> 'Autoscaler':
        # TODO(MaoZiming): use NAME to get the class.
        if spec.pool:
            return QueueLengthAutoscaler(service_name, spec, version)
        # SkyServiceSpec.__setstate__ materializes the concurrency knob for
        # specs unpickled from old DB rows.
        elif spec.target_concurrency_per_replica is not None:
            # Checked before the qps branches: the knob is mutually
            # exclusive with target_qps_per_replica (validated at spec
            # load), so a set knob unambiguously selects concurrency-based
            # autoscaling.
            return ConcurrencyAutoscaler(service_name, spec, version)
        elif spec.use_ondemand_fallback:
            return FallbackRequestRateAutoscaler(service_name, spec, version)
        elif isinstance(spec.target_qps_per_replica, dict):
            # Use instance-aware autoscaler
            # when target_qps_per_replica is a dict
            return InstanceAwareRequestRateAutoscaler(service_name, spec,
                                                      version)
        else:
            return RequestRateAutoscaler(service_name, spec, version)

    def get_decision_interval(self) -> int:
        """Get the decision interval for the autoscaler.

        We reduce the decision interval when the desired number of replicas is
        0, to make the service scale faster when the service is not running.
        This will happen when min_replicas = 0 and no traffic.
        """
        if self.get_final_target_num_replicas() == 0:
            return constants.AUTOSCALER_NO_REPLICA_DECISION_INTERVAL_SECONDS
        else:
            return constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS

    def _select_outdated_replicas_to_scale_down(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[int]:
        """Select outdated replicas to scale down."""

        if self.update_mode == serve_utils.UpdateMode.ROLLING:
            latest_ready_replicas: list[replica_managers.ReplicaInfo] = []
            old_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []
            for info in replica_infos:
                if info.version == self.latest_version:
                    if info.is_ready:
                        latest_ready_replicas.append(info)
                elif not info.is_terminal:
                    old_nonterminal_replicas.append(info)

            num_latest_ready_replicas = len(latest_ready_replicas)

            # We compare to target_num_replicas instead of min_replicas, to
            # guarantee better service quality. Since mixing traffic across
            # old and latest versions are allowed in rolling update, this will
            # not affect the time it takes for the service to updated to the
            # latest version.
            if (num_latest_ready_replicas
                    >= self.get_final_target_num_replicas()):
                # Once the number of ready new replicas is greater than or equal
                # to the target, we can scale down all old replicas.
                return [info.replica_id for info in old_nonterminal_replicas]
            # If rolling update is in progress, we scale down old replicas
            # based on the number of ready new replicas.
            num_old_replicas_to_keep = (self.get_final_target_num_replicas() -
                                        num_latest_ready_replicas)
            # Remove old replicas (especially old launching replicas) and only
            # keep the required number of replicas, as we want to let the new
            # replicas to take over the provisioning old replicas faster.
            # `_select_replicas_to_scale_down` will make sure we scale the
            # replicas in initializing statuses first before scaling down the
            # READY old replicas.
            return _select_nonterminal_replicas_to_scale_down(
                max(0,
                    len(old_nonterminal_replicas) - num_old_replicas_to_keep),
                old_nonterminal_replicas,
            )

        if not active_versions:
            # active_versions can be empty when none of the replicas are ready
            # when the load balancer sync with the controller.
            return []
        # The active_versions should supposedly only having one version, but
        # we use min() here to make sure this works when rolling update and
        # blue-green update are mixed. min is used as we will scale down all old
        # replicas with version smaller than `latest_version_with_min_replicas`.
        latest_version_with_min_replicas = min(active_versions)
        # When it is blue green update, we scale down old replicas when the
        # number of ready new replicas is greater than or equal to the min
        # replicas instead of the target, to ensure the service being updated
        # to the latest version faster.
        return [
            info.replica_id
            for info in replica_infos
            if info.version < latest_version_with_min_replicas
        ]

    def _cost_rebalance_replica_capacity(
            self, info: 'replica_managers.ReplicaInfo') -> float:
        """Serving-capacity units represented by an existing replica."""
        del info
        return 1.0

    def _cost_rebalance_location_capacity(
            self, location: spot_placer.Location) -> float:
        """Serving-capacity units represented by a candidate location."""
        del location
        return 1.0

    def _cost_rebalance_location_is_compatible(
        self,
        incumbent: 'replica_managers.ReplicaInfo',
        location: spot_placer.Location,
    ) -> bool:
        """Whether an economic replacement preserves autoscaler policy."""
        del incumbent, location
        return True

    def _get_hourly_cost_from_replica_info(
            self, replica_info: 'replica_managers.ReplicaInfo') -> float:
        """Resolve whole-replica hourly cost conservatively."""
        cache_key = self._cost_rebalance_replica_cache_key(replica_info)
        if cache_key is not None:
            cached = self._cost_rebalance_replica_cost_cache.get(cache_key)
            if cached is not None:
                return cached
        cost = 0.0
        resolved = False
        try:
            handle = replica_info.handle()
            if handle is not None:
                cost = float(handle.launched_resources.get_cost(seconds=3600))
                resolved = True
        except Exception:  # pylint: disable=broad-except
            cost = 0.0
        if (cache_key is not None and resolved and
                replica_info.status_property.sky_launch_status
                == common_utils.ProcessStatus.SUCCEEDED):
            self._cost_rebalance_replica_cost_cache[cache_key] = cost
        return cost

    @staticmethod
    def _cost_rebalance_replica_cache_key(
        replica_info: 'replica_managers.ReplicaInfo',
    ) -> tuple[int, str] | None:
        """Return an ABA-safe memo key, or disable caching without identity."""
        replica_id = getattr(replica_info, 'replica_id', None)
        record_id = getattr(replica_info, 'replica_record_id', None)
        if (type(replica_id) is not int or replica_id < 1 or
                not isinstance(record_id, str) or not record_id):
            return None
        return replica_id, record_id

    def _cost_rebalance_pairs(
        self, replica_infos: list['replica_managers.ReplicaInfo']
    ) -> dict[int, 'replica_managers.ReplicaInfo']:
        """Return one live replacement row per live incumbent."""
        by_id = {info.replica_id: info for info in replica_infos}
        pairs: dict[int, replica_managers.ReplicaInfo] = {}
        for replacement in replica_infos:
            victim_id = replacement.cost_rebalance_for_replica_id
            if victim_id is None or replacement.is_terminal:
                continue
            victim = by_id.get(victim_id)
            if victim is None or victim.is_terminal:
                continue
            prior = pairs.get(victim_id)
            if prior is None or replacement.replica_id < prior.replica_id:
                pairs[victim_id] = replacement
        return pairs

    def _protect_cost_rebalance_overlap(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        decisions: list[AutoscalerDecision],
    ) -> list[AutoscalerDecision]:
        """Keep ordinary autoscaling from consuming replacement overlap."""
        pairs = self._cost_rebalance_pairs(replica_infos)
        if not pairs:
            return decisions
        protected_ids = set(pairs)
        protected_ids.update(
            replacement.replica_id for replacement in pairs.values())
        overlap_to_ignore = len(pairs)
        kept: list[AutoscalerDecision] = []
        for decision in decisions:
            if decision.operator != AutoscalerDecisionOperator.SCALE_DOWN:
                kept.append(decision)
                continue
            assert isinstance(decision.target, (int, LogicalScaleDownTarget))
            replica_id = _scale_down_replica_id(decision.target)
            if replica_id in protected_ids:
                logger.info('Suppressing ordinary scale-down of cost-rebalance '
                            f'pair member {replica_id}.')
                if overlap_to_ignore > 0:
                    overlap_to_ignore -= 1
                continue
            if overlap_to_ignore > 0:
                logger.info('Suppressing one ordinary scale-down for temporary '
                            'cost-rebalance replacement headroom.')
                overlap_to_ignore -= 1
                continue
            kept.append(decision)
        return kept

    @staticmethod
    def _location_gpu_shape(location: spot_placer.Location) -> tuple[str, int]:
        accelerators = location.accelerators or {}
        if not accelerators:
            return 'unknown', 1
        gpu_type, gpu_count = next(iter(accelerators.items()))
        return gpu_type, max(1, int(gpu_count))

    def _best_cost_rebalance_candidate(
        self,
        incumbent: 'replica_managers.ReplicaInfo',
        active_locations: list[spot_placer.Location],
        location_load: dict[spot_placer.Location, int],
        known_location_costs: Mapping[spot_placer.Location, float],
    ) -> spot_placer.Location | None:
        placer = self._cost_rebalance_spot_placer
        if placer is None:
            return None
        if (self.reserved_capacity_fill and
            (incumbent.reserved_fill or incumbent.is_zero_cost is True)):
            # The reserved-fill controller exclusively owns convergence to
            # free capacity. Generic rebalance handles paid-to-paid movement.
            return None
        incumbent_location = incumbent.get_spot_location()
        if incumbent_location is None:
            return None
        incumbent_capacity = self._cost_rebalance_replica_capacity(incumbent)
        incumbent_cost = self._get_hourly_cost_from_replica_info(incumbent)
        if incumbent_capacity <= 0 or incumbent_cost <= 0:
            # Unknown cost is deliberately conservative in the existing cost
            # resolver.  Never replace an unknown/zero-cost incumbent.
            return None
        incumbent_unit_cost = incumbent_cost / incumbent_capacity
        maximum_unit_cost = incumbent_unit_cost * (
            1.0 - self.cost_rebalance_min_savings_fraction)

        eligible: list[tuple[float, int, str, spot_placer.Location]] = []
        for location in active_locations:
            if spot_placer.locations_match_placement(incumbent_location,
                                                     location):
                continue
            if not self._cost_rebalance_location_is_compatible(
                    incumbent, location):
                continue
            candidate_capacity = self._cost_rebalance_location_capacity(
                location)
            if candidate_capacity + 1e-9 < incumbent_capacity:
                continue
            candidate_cost = known_location_costs.get(location, float('inf'))
            if not math.isfinite(candidate_cost) or candidate_cost < 0:
                continue
            if self.reserved_capacity_fill and candidate_cost == 0:
                continue
            candidate_unit_cost = candidate_cost / candidate_capacity
            if candidate_unit_cost > maximum_unit_cost + 1e-12:
                continue
            eligible.append((candidate_unit_cost, location_load[location],
                             repr(location.to_pickleable()), location))
        if not eligible:
            return None
        return min(eligible, key=lambda item: item[:3])[-1]

    def _generate_cost_rebalance_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        ordinary_decisions: list[AutoscalerDecision],
    ) -> list[AutoscalerDecision]:
        """Progress durable pairs and, when stable, start cheaper replacements."""
        live_replica_records: set[tuple[int, str]] = set()
        for info in replica_infos:
            if info.is_terminal:
                continue
            cache_key = self._cost_rebalance_replica_cache_key(info)
            if cache_key is not None:
                live_replica_records.add(cache_key)
        for cache_key in list(self._cost_rebalance_replica_cost_cache):
            if cache_key not in live_replica_records:
                del self._cost_rebalance_replica_cost_cache[cache_key]
        pairs = self._cost_rebalance_pairs(replica_infos)
        by_id = {info.replica_id: info for info in replica_infos}
        decisions: list[AutoscalerDecision] = []

        # Existing pairs are completed even when a later update disables the
        # policy or changes the placement contract. In either case keep the
        # incumbent and drain the replacement; otherwise wait for replacement
        # readiness, then strictly drain the incumbent. COST_REBALANCE means
        # off-route now, terminate only after the LB proves zero occupancy.
        for victim_id, replacement in sorted(pairs.items()):
            victim = by_id[victim_id]
            replacement_location = replacement.get_spot_location()
            replacement_preserves_policy = (
                replacement_location is not None and
                self._cost_rebalance_location_is_compatible(
                    victim, replacement_location))
            if self.cost_rebalance and replacement_preserves_policy:
                if (replacement.is_ready and
                        not _replica_is_retiring_card_supply(replacement) and
                        victim.status_property.sky_down_status is None):
                    decisions.extend(
                        _generate_scale_down_decisions(
                            [victim.replica_id],
                            reason=AutoscalerDecisionReason.COST_REBALANCE))
            elif (replacement.is_ready and
                  replacement.status_property.sky_down_status is None):
                decisions.extend(
                    _generate_scale_down_decisions(
                        [replacement.replica_id],
                        reason=AutoscalerDecisionReason.COST_REBALANCE))
            elif replacement.status_property.sky_down_status is None:
                decisions.extend(
                    _generate_scale_down_decisions([replacement.replica_id]))

        if (not self.cost_rebalance or
                self._cost_rebalance_spot_placer is None):
            self._clear_cost_rebalance_candidates()
            return decisions
        if ordinary_decisions:
            self._clear_cost_rebalance_candidates()
            return decisions
        if any(not info.is_terminal and info.version != self.latest_version
               for info in replica_infos):
            self._clear_cost_rebalance_candidates()
            return decisions

        slots = self.cost_rebalance_max_parallel_replacements - len(pairs)
        paired_ids = set(pairs)
        candidates = [
            info for info in replica_infos
            if (info.version == self.latest_version and info.is_ready and
                not _replica_is_retiring_card_supply(info) and
                info.replica_id not in paired_ids)
        ]
        candidates.sort(
            key=lambda info: -self._get_hourly_cost_from_replica_info(info))
        planned_locations = [
            location for location in (info.get_spot_location()
                                      for info in replica_infos
                                      if not info.is_terminal)
            if location is not None
        ]
        known_location_costs = self._known_location_costs_for_current_tick()
        if known_location_costs is None:
            self._clear_cost_rebalance_candidates()
            return decisions
        active_locations = self._cost_rebalance_spot_placer.active_locations(
            known_location_costs)
        # This load is shared by every incumbent evaluated in the tick.  On a
        # large fleet, rebuilding it inside `_best_cost_rebalance_candidate`
        # turns one placement scan into a redundant scan per replica.
        location_load = {
            location: sum(
                spot_placer.locations_match_placement(current, location)
                for current in planned_locations
            ) for location in active_locations
        }
        now = time.time()
        current_candidate_keys: set[tuple[str, spot_placer.Location]] = set()
        for incumbent in candidates:
            location = self._best_cost_rebalance_candidate(
                incumbent, active_locations, location_load,
                known_location_costs)
            if location is None:
                continue
            try:
                replica_record_id = str(
                    uuid.UUID(str(incumbent.replica_record_id)))
            except (AttributeError, TypeError, ValueError):
                continue
            key = (replica_record_id, location)
            current_candidate_keys.add(key)
            first_seen = self._cost_rebalance_candidate_since.get(key)
            if first_seen is None:
                self._cost_rebalance_candidate_since[key] = now
                self._cost_rebalance_state_dirty = True
                first_seen = now
            if (now - first_seen < self.cost_rebalance_stabilization_seconds):
                continue
            if slots <= 0:
                # Keep validating continuous eligibility while another pair
                # occupies the replacement slot, but do not launch overlap.
                continue
            override = location.to_dict()
            override[constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY] = (
                incumbent.replica_id)
            decisions.append(
                AutoscalerDecision(
                    AutoscalerDecisionOperator.SCALE_UP,
                    override,
                    reason=AutoscalerDecisionReason.COST_REBALANCE))
            planned_locations.append(location)
            for active_location in active_locations:
                if spot_placer.locations_match_placement(
                        location, active_location):
                    location_load[active_location] += 1
            slots -= 1

        for key in list(self._cost_rebalance_candidate_since):
            if key not in current_candidate_keys:
                del self._cost_rebalance_candidate_since[key]
                self._cost_rebalance_state_dirty = True
        return decisions

    def _notify_rollout_blocked(self, previous_version: int) -> None:
        operator_notifications.record_notification(
            operator_notifications.OperatorNotificationCategory.
            SERVE_ROLLOUT_BLOCKED,
            f'SkyServe rollout blocked for service {self._service_name!r}: '
            f'version {self.latest_version} failed before any replica became '
            f'ready. Version {previous_version} remains active. Inspect the '
            'new replica provisioning and setup logs.',
            dedupe_window_seconds=operator_notifications.
            SERVE_ROLLOUT_BLOCKED_DEDUPE_WINDOW_SECONDS)

    def generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on replica information.
        If the number of launched replicas is less than the target, trigger a
        scale up. Else, trigger a scale down. This function also handles the
        version control of the replicas.

        For future compatibility, we return a list of AutoscalerDecision.
        Scale-up could include both spot and on-demand, each with a resource
        override dict. Active migration could require returning both SCALE_UP
        and SCALE_DOWN.
        """

        # Handle latest version unrecoverable failure first.
        self._unrecoverable_rollout_failure = None
        latest_replicas: list[replica_managers.ReplicaInfo] = []
        for info in replica_infos:
            if info.version == self.latest_version:
                latest_replicas.append(info)
                if info.is_ready:
                    self.latest_version_ever_ready = self.latest_version
        previous_versions = [
            version for version in active_versions
            if version < self.latest_version
        ]
        if self.latest_version_ever_ready < self.latest_version:
            unrecoverable = [
                info for info in latest_replicas
                if info.status_property.unrecoverable_failure()
            ]
            if unrecoverable:
                if previous_versions:
                    self._notify_rollout_blocked(max(previous_versions))
                    evidence = ', '.join(
                        f'{info.replica_id}:{info.status.value}' for info in
                        sorted(unrecoverable,
                               key=lambda replica: replica.replica_id)[:20])
                    if len(unrecoverable) > 20:
                        evidence += f', and {len(unrecoverable) - 20} more'
                    self._unrecoverable_rollout_failure = (
                        UnrecoverableRolloutFailure(
                            version=self.latest_version,
                            reason=(
                                f'Version {self.latest_version} never became '
                                'ready and has unrecoverable replica evidence: '
                                f'{evidence}.')))
                # Stop scaling if one replica of the latest version has a
                # typed never-ready failure. With a previous active version,
                # the controller consumes the signal above by quarantining
                # this exact candidate and respawning onto the proven runtime.
                # Without a fallback, preserve the historical fail-closed
                # behavior rather than retrying a broken initial version.
                return []
            if (previous_versions and latest_replicas and
                    all(info.is_terminal for info in latest_replicas) and
                    any(info.status in
                        serve_state.ReplicaStatus.failed_statuses() and
                        not info.status_property.is_scale_down
                        for info in latest_replicas)):
                self._notify_rollout_blocked(max(previous_versions))

        scaling_decisions = []

        # If rolling update is in progress, we scale down old replicas based on
        # the number of ready new replicas and the traffic is directed to both
        # old and new replicas. Or, for blue_green update, once there is
        # min_replicas number of ready new replicas, we will direct all traffic
        # to them, we can scale down all old replicas.
        # TODO(MaoZiming,zhwu): corner case: We should make sure the fallback
        # replicas are ready before scaling down the old replicas to avoid the
        # situation that all the ready new replicas are preempted together.
        scaling_decisions.extend(
            _generate_scale_down_decisions(
                self._select_outdated_replicas_to_scale_down(
                    replica_infos, active_versions)))

        # If the latest version is ever ready, we can proceed to generate
        # decisions from the implementations in subclasses. The
        # reserved-capacity fill overlay wraps only the subclass's demand
        # decisions -- the outdated-replica drain above is version
        # control, not demand, and must never be suppressed by fill.
        ordinary_decisions = self._apply_reserved_capacity_fill(
            replica_infos, self._generate_scaling_decisions(replica_infos))
        ordinary_decisions = self._protect_cost_rebalance_overlap(
            replica_infos, ordinary_decisions)
        scaling_decisions.extend(ordinary_decisions)
        scaling_decisions.extend(
            self._generate_cost_rebalance_decisions(replica_infos,
                                                    ordinary_decisions))

        if not scaling_decisions:
            logger.info('No scaling needed.')

        return scaling_decisions

    def dump_dynamic_states(self) -> dict[str, Any]:
        """Dump dynamic states from autoscaler."""
        states: dict[str, Any] = {
            'latest_version_ever_ready': self.latest_version_ever_ready
        }
        # Reserved-capacity fill snapshot: carried across the in-process
        # autoscaler swap in update_service. Without it a fresh autoscaler
        # instance has no zero-cost location set until the next poll, so
        # one decision tick with suppression off could terminate the whole
        # fill fleet. Nested under a single key (in pickleable form) so
        # subclass _load_dynamic_states leftover-logging never sees it.
        with self._fill_pool_state_lock:
            fill_state_version = 2 if self._fill_pool_states else 1
            fill_pool_last_started_key = self._fill_pool_last_started_key
            if fill_pool_last_started_key not in self._fill_pool_states:
                fill_pool_last_started_key = None
            # Capture the version discriminator and complete v2 pool map from
            # one critical section. A poller map swap between two independent
            # reads must not produce a v1 discriminator with v2 contents (or
            # vice versa).
            dumped_pools = {
                key: {
                    'protocol_version': pool.protocol_version,
                    'physical_cluster_uid': pool.physical_cluster_uid,
                    'service_generation': pool.service_generation,
                    'edge_cap': pool.edge_cap,
                    # This is non-launching restart shelter only. Feed is never
                    # restored and the epoch remains DB-authoritative.
                    'shelter_grant': max(0,
                                         min(pool.edge_cap,
                                             pool.shelter_grant)),
                    'zero_cost_location_keys': [
                        location.to_pickleable()
                        for location in pool.zero_cost_locations
                    ],
                    'snapshot_time': pool.snapshot_time,
                } for key, pool in self._fill_pool_states.items()
            }
        states['reserved_capacity_fill_state'] = {
            'version': fill_state_version,
            # A brokered feed is round authority, not durable autoscaler
            # state. Preserve standalone pre-broker behavior, but make every
            # brokered v1 swap fail closed until its poller republishes.
            'broker_authority': (self._fill_grant_pool_key is not None or
                                 self._fill_grant_epoch is not None),
            'fill_free_slots': self._fill_free_slots,
            'fill_last_raw_free_slots': self._fill_last_raw_free_slots,
            'fill_zero_cost_location_keys': [
                location.to_pickleable()
                for location in self._fill_zero_cost_locations
            ],
            'fill_snapshot_time': self._fill_snapshot_time,
            'fill_pool_last_started_key': fill_pool_last_started_key,
            # Grants/epochs remain DB-authoritative and are deliberately not
            # restored. Locations and identity are enough to protect existing
            # replicas during an in-process autoscaler swap; feed resumes only
            # after the poller publishes an exact-generation round.
            'pools': dumped_pools,
        }
        states.update(self._dump_dynamic_states())
        return states

    def load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        """Load dynamic states to autoscaler."""
        self.latest_version_ever_ready = dynamic_states.pop(
            'latest_version_ever_ready', constants.INITIAL_VERSION)
        # Absent in dumps from builds predating the fill feature: keep
        # the constructor defaults (empty snapshot). A disabled destination
        # is also a lifecycle boundary: do not restore authority that a later
        # re-enable could mistake for its newly created claim.
        fill_state = dynamic_states.pop('reserved_capacity_fill_state', None)
        with self._fill_pool_state_lock:
            self._fill_pool_last_started_key = None
            self._fill_pool_order_revision += 1
        if fill_state is not None and self.reserved_capacity_fill:
            broker_authority = bool(fill_state.get('broker_authority', True))
            self._fill_free_slots = (0 if broker_authority else max(
                0, int(fill_state.get('fill_free_slots', 0))))
            self._fill_last_raw_free_slots = (
                None if broker_authority else
                fill_state.get('fill_last_raw_free_slots'))
            self._fill_zero_cost_locations = [
                location for location in
                (spot_placer.Location.from_pickleable(key)
                 for key in fill_state.get('fill_zero_cost_location_keys', []))
                if location is not None
            ]
            self._fill_snapshot_time = fill_state.get('fill_snapshot_time')
            if fill_state.get('version') == 2:
                restored: dict[str, _PoolFillState] = {}
                restored_topology: list[tuple[str, str, Iterable[str]]] = []
                restored_generations: set[int] = set()
                for pool_key, raw_pool in fill_state.get('pools', {}).items():
                    try:
                        if (not isinstance(pool_key, str) or
                                not isinstance(raw_pool, dict)):
                            continue
                        identity = reserved_capacity_broker.parse_pool_identity(
                            pool_key)
                        if identity.protocol_version != 2:
                            continue
                        canonical_pool_key = (
                            reserved_capacity_broker.make_pool_key(
                                '',
                                identity.gpu_names,
                                protocol_version=(
                                    reserved_capacity_broker.PROTOCOL_V2),
                                physical_cluster_uid=(
                                    identity.physical_cluster_uid)))
                        raw_protocol_version = raw_pool['protocol_version']
                        raw_physical_uid = raw_pool['physical_cluster_uid']
                        raw_generation = raw_pool['service_generation']
                        raw_edge_cap = raw_pool['edge_cap']
                        if (pool_key != canonical_pool_key or
                                isinstance(raw_protocol_version, bool) or
                                not isinstance(raw_protocol_version, int) or
                                raw_protocol_version != 2 or
                                not isinstance(raw_physical_uid, str) or
                                not raw_physical_uid or raw_physical_uid
                                != identity.physical_cluster_uid or
                                isinstance(raw_generation, bool) or
                                not isinstance(raw_generation, int) or
                                raw_generation < 1 or
                                isinstance(raw_edge_cap, bool) or
                                not isinstance(raw_edge_cap, int) or
                                raw_edge_cap < 0):
                            continue
                        locations = self._parse_reserved_fill_pool_locations(
                            identity, raw_pool.get('zero_cost_location_keys'))
                        restored_edge_cap = raw_edge_cap
                        raw_snapshot_time = raw_pool.get('snapshot_time')
                        if (isinstance(raw_snapshot_time, bool) or
                                not isinstance(raw_snapshot_time,
                                               (int, float)) or
                                not math.isfinite(raw_snapshot_time) or
                                raw_snapshot_time < 0 or
                                raw_snapshot_time > time.time() +
                                _RESERVED_CAPACITY_MAX_FUTURE_SKEW_SECONDS):
                            continue
                        raw_shelter_grant = raw_pool.get('shelter_grant')
                        if (isinstance(raw_shelter_grant, bool) or
                                not isinstance(raw_shelter_grant, int) or
                                raw_shelter_grant < 0):
                            # Protocol v2 did not exist before this dump field.
                            # Missing/malformed authority is corruption, not a
                            # compatibility shape: retain location identity but
                            # fail closed to zero shelter.
                            restored_shelter_grant = 0
                        else:
                            restored_shelter_grant = min(
                                restored_edge_cap, raw_shelter_grant)
                        restored_topology.append(
                            (locations[0].region, raw_physical_uid,
                             identity.gpu_names))
                        restored_generations.add(raw_generation)
                        restored[str(pool_key)] = _PoolFillState(
                            protocol_version=raw_protocol_version,
                            pool_key=pool_key,
                            physical_cluster_uid=raw_physical_uid,
                            service_generation=raw_generation,
                            edge_cap=restored_edge_cap,
                            # Feed and epoch fail closed across the swap. The
                            # prior real grant remains a shelter-only ceiling;
                            # with zero feed it authorizes no launch.
                            free_slots=0,
                            last_raw_free_slots=None,
                            zero_cost_locations=locations,
                            snapshot_time=float(raw_snapshot_time),
                            shelter_grant=restored_shelter_grant,
                            grant=0,
                            grant_epoch=None)
                    except (KeyError, TypeError, ValueError):
                        continue
                try:
                    _validate_reserved_fill_pool_topology(restored_topology)
                except ValueError:
                    # A restored complete-map conflict has no deterministic
                    # authoritative subset. Drop the whole map rather than
                    # making shelter depend on serialized edge order.
                    restored = {}
                if len(restored_generations) > 1:
                    restored = {}
                raw_last_started_key = fill_state.get(
                    'fill_pool_last_started_key')
                restored_last_started_key = (
                    raw_last_started_key
                    if isinstance(raw_last_started_key, str) and
                    raw_last_started_key in restored else None)
                with self._fill_pool_state_lock:
                    self._fill_pool_states = restored
                    self._fill_pool_last_started_key = (
                        restored_last_started_key)
                    self._refresh_legacy_fill_projection_locked()
        self._load_dynamic_states(dynamic_states)


class _AutoscalerWithHysteresis(Autoscaler):
    """_AutoscalerWithHysteresis: Autoscale with hysteresis.

    This is an internal class for developing autoscalers with hysteresis. It
    only scales when the number of replicas is above or below the target number
    of replicas for a certain number of consecutive periods.
    """

    def _setup_thresholds(self, spec: 'service_spec.SkyServiceSpec') -> None:
        upscale_delay_seconds = (
            spec.upscale_delay_seconds if spec.upscale_delay_seconds is not None
            else constants.AUTOSCALER_DEFAULT_UPSCALE_DELAY_SECONDS)
        self.scale_up_threshold: int = int(
            upscale_delay_seconds /
            constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS)
        downscale_delay_seconds = (
            spec.downscale_delay_seconds
            if spec.downscale_delay_seconds is not None else
            constants.AUTOSCALER_DEFAULT_DOWNSCALE_DELAY_SECONDS)
        self.downscale_delay_seconds: float = float(downscale_delay_seconds)
        self.scale_down_threshold: int = int(
            self.downscale_delay_seconds /
            constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS)

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the hysteresis autoscaler.

        Variables:
            upscale_counter: Counter for upscale decisions of replicas.
            downscale_counter: Counter for downscale decisions of replicas.
            scale_up_threshold: The threshold to trigger a scale up.
            scale_down_threshold: The threshold to trigger a scale down.
        """
        super().__init__(service_name, spec, version)
        self.upscale_counter: int = 0
        self.downscale_counter: int = 0
        self._setup_thresholds(spec)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions but returns normally;
            # without this guard we would still reset the hysteresis
            # counters and thresholds from the stale spec below.
            super().update_version(version, spec, update_mode)
            return
        super().update_version(version, spec, update_mode)
        # We directly set the target_num_replicas here instead of calling
        # `_set_target_num_replicas_with_hysteresis` to have the replicas
        # quickly scale after each update.
        self.target_num_replicas = self._calculate_target_num_replicas()
        logger.debug(f'Target number of replicas: {self.target_num_replicas}'
                     'after update_version.')
        # Cleanup hysteresis counters.
        self.upscale_counter = 0
        self.downscale_counter = 0
        self._setup_thresholds(spec)

    def _set_target_num_replicas_with_hysteresis(self) -> None:
        """Set target_num_replicas based on request rate with hysteresis."""
        target_num_replicas = self._calculate_target_num_replicas()
        old_target_num_replicas = self.target_num_replicas

        # Faster scale up when there is no replica.
        if self.target_num_replicas == 0:
            self.target_num_replicas = target_num_replicas
        elif target_num_replicas > self.target_num_replicas:
            self.upscale_counter += 1
            self.downscale_counter = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                self.target_num_replicas = target_num_replicas
        elif target_num_replicas < self.target_num_replicas:
            self.downscale_counter += 1
            self.upscale_counter = 0
            if self.downscale_counter >= self.scale_down_threshold:
                self.downscale_counter = 0
                self.target_num_replicas = target_num_replicas
        else:
            self.upscale_counter = self.downscale_counter = 0

        logger.info(
            f'Old target number of replicas: {old_target_num_replicas}. '
            f'Current target number of replicas: {target_num_replicas}. '
            f'Final target number of replicas: {self.target_num_replicas}. '
            f'Num overprovision: {self.num_overprovision}. '
            f'Upscale counter: {self.upscale_counter}/'
            f'{self.scale_up_threshold}. '
            f'Downscale counter: {self.downscale_counter}/'
            f'{self.scale_down_threshold}. ')


class RequestRateAutoscaler(_AutoscalerWithHysteresis):
    """RequestRateAutoscaler: Autoscale according to request rate.

    Scales when the number of requests per replica in the given interval
    is above or below the target qps per replica. The instance can be
    either spot or on-demand, but not both.
    """

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the request rate autoscaler.

        Variables:
            target_qps_per_replica: Target qps per replica for autoscaling.
            qps_window_size: Window size for qps calculating.
            request_timestamps: All request timestamps within the window.
        """
        super().__init__(service_name, spec, version)
        self.target_qps_per_replica: float | dict[
            str, float] | None = spec.target_qps_per_replica
        self.qps_window_size: int = constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS
        self.request_timestamps: list[float] = []

    def _calculate_target_num_replicas(self) -> int:
        if self.target_qps_per_replica is None:
            return self.min_replicas

        # RequestRateAutoscaler should only handle float values
        if isinstance(self.target_qps_per_replica, dict):
            raise ValueError('RequestRateAutoscaler does not support dict '
                             'target_qps_per_replica. Should use '
                             'InstanceAwareRequestRateAutoscaler instead.')

        num_requests_per_second = len(
            self.request_timestamps) / self.qps_window_size
        target_num_replicas = \
            math.ceil(num_requests_per_second / self.target_qps_per_replica)
        logger.info(f'Requests per second: {num_requests_per_second}. '
                    f'Target number of replicas: {target_num_replicas}.')

        return self._clip_target_num_replicas(target_num_replicas)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions; don't overwrite the
            # live qps target from a stale spec either.
            super().update_version(version, spec, update_mode)
            return
        super().update_version(version, spec, update_mode)
        self.target_qps_per_replica = spec.target_qps_per_replica

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        """Collect request information from aggregator for autoscaling.

        request_aggregator_info should be a dict with the following format:

        {
            'timestamps': [timestamp1 (float), timestamp2 (float), ...]
        }
        """
        replace_request_window = request_aggregator_info.get(
            'replace_request_window') is True
        incoming_timestamps = request_aggregator_info.get('timestamps', [])
        if replace_request_window:
            self.request_timestamps = list(incoming_timestamps)
        else:
            self.request_timestamps.extend(incoming_timestamps)
        current_time = time.time()
        index = bisect.bisect_left(self.request_timestamps,
                                   current_time - self.qps_window_size)
        self.request_timestamps = self.request_timestamps[index:]
        logger.info(f'Num of requests in the last {self.qps_window_size} '
                    f'seconds: {len(self.request_timestamps)}')

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on request rate."""

        # Use standard hysteresis-based logic (non-instance-aware)
        self._set_target_num_replicas_with_hysteresis()

        latest_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []

        for info in replica_infos:
            if info.version == self.latest_version:
                if not info.is_terminal:
                    latest_nonterminal_replicas.append(info)

        scaling_decisions: list[AutoscalerDecision] = []

        # Case 1. when latest_nonterminal_replicas is less
        # than num_to_provision, we always scale up new replicas.
        target_num_replicas = self.get_final_target_num_replicas()
        if len(latest_nonterminal_replicas) < target_num_replicas:
            num_replicas_to_scale_up = (target_num_replicas -
                                        len(latest_nonterminal_replicas))
            logger.info('Number of replicas to scale up: '
                        f'{num_replicas_to_scale_up}')
            scaling_decisions.extend(
                _generate_scale_up_decisions(num_replicas_to_scale_up, None))

        # Case 2: when latest_nonterminal_replicas is more
        # than target_num_replicas, we scale down new replicas.
        replicas_to_scale_down = []
        if len(latest_nonterminal_replicas) > target_num_replicas:
            num_replicas_to_scale_down = (len(latest_nonterminal_replicas) -
                                          target_num_replicas)
            # Use standard downscaling logic
            replicas_to_scale_down = (
                _select_nonterminal_replicas_to_scale_down(
                    num_replicas_to_scale_down, latest_nonterminal_replicas))
            logger.info(
                'Number of replicas to scale down: '
                f'{num_replicas_to_scale_down} {replicas_to_scale_down}')

        scaling_decisions.extend(
            _generate_scale_down_decisions(replicas_to_scale_down))

        return scaling_decisions

    def _dump_dynamic_states(self) -> dict[str, Any]:
        return {
            'request_timestamps': self.request_timestamps,
        }

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        if 'request_timestamps' in dynamic_states:
            self.request_timestamps = dynamic_states.pop('request_timestamps')
        if dynamic_states:
            logger.info(f'Remaining dynamic states: {dynamic_states}')


# Distinguishes "caller did not resolve a handle" from a resolved None
# (cluster row or handle genuinely absent) in the mixin helpers below.
_UNRESOLVED_HANDLE = object()


class _GpuShapeResolverMixin:
    """Shared GPU-shape resolution with a post-launch-only memo.

    Used by the shape-aware autoscalers (instance-aware QPS and
    concurrency): both need a replica's (gpu_type, gpu_count) to size its
    capacity, and both must avoid repeating the blocking handle() DB read
    + unpickle for the same replica across the 2-3 passes per decision
    tick. Subclasses must initialize `_gpu_shape_cache` in __init__ and
    prune it to the live replica set each tick via
    `_prune_gpu_shape_cache` so the memo stays bounded.
    """
    # replica_id -> (gpu_type, gpu_count). A shape is cached only once the
    # replica's launch has finished: while it is still provisioning, the
    # cluster record is rewritten for every failover attempt and its
    # accelerators can change, so a mid-launch resolution must be
    # re-resolved on later ticks. After launch the shape is fixed for the
    # replica's lifetime.
    _gpu_shape_cache: dict[int, tuple[str, int]]
    # replica_id -> hourly cost of launched resources (same lifecycle
    # rules as the shape cache). Backs cost-aware victim ordering in both
    # shape-aware autoscalers.
    _replica_cost_cache: dict[int, float]
    # Numeric replica ids are reusable after exact cleanup.  This map binds
    # both memos above to the immutable database-record identity so a newly
    # created row can never inherit its predecessor's shape or cost.
    _replica_cache_record_ids: dict[int, str]
    configured_accelerator_shapes: dict[str, int]
    latest_version: int
    _service_name: str
    # Immutable per-decision legacy handle snapshot, populated before the
    # autoscaler state lock is acquired.
    _gpu_shape_handles_for_tick: dict[int, Any] | None
    _kueue_capacity_by_replica_id_for_tick: dict[
        int, kueue_lane_capacity.KueueReplicaCapacityClass] | None
    _kueue_blocked_retirement_shapes_for_tick: frozenset[tuple[str, int]]
    _kueue_transition_replica_ids_for_tick: frozenset[int]
    _kueue_ready_paid_replacement_replica_ids_for_tick: frozenset[int]

    def _supports_exact_fill_shape_resolution(self) -> bool:
        return True

    def _prepare_scaling_decision_inputs(
        self, replica_infos: list['replica_managers.ReplicaInfo']
    ) -> ScalingDecisionInputs:
        """Resolve every durable input before routing serialization."""
        for info in replica_infos:
            self._bind_replica_cache_identity(info)
        historical_versions = {
            info.version
            for info in replica_infos
            if not info.is_terminal and info.version != self.latest_version
        }
        if historical_versions:
            historical_versions.difference_update(
                self._cached_historical_scaling_versions())
        sorted_historical_versions = sorted(historical_versions)
        historical_values: dict[int, Any] = {}
        if sorted_historical_versions:
            load_failed = False
            try:
                historical_specs = serve_state.get_specs(
                    self._service_name, sorted_historical_versions)
            except Exception as e:  # pylint: disable=broad-except
                load_failed = True
                logger.warning(
                    'Failed to batch-load historical service specs for '
                    f'versions {sorted_historical_versions}: '
                    f'{common_utils.format_exception(e)}')
                historical_specs = {}
            for version in sorted_historical_versions:
                value = self._normalize_historical_scaling_spec(
                    historical_specs.get(version))
                if value is None and not load_failed:
                    logger.warning(
                        'No usable scaling capacity metadata for historical '
                        'version %s; using the latest-version fallback for '
                        'this decision tick.', version)
                historical_values[version] = value
        try:
            kueue_snapshot = (
                kueue_lane_capacity.snapshot_replica_capacity_classes(
                    self._service_name, replica_infos))
        except kueue_lane_capacity.KueueAdmissionCapacityError as error:
            # A read failure cannot make a waiting/admitted lane disappear.
            # Conservatively protect all reserved-fill rows from retirement;
            # final PostgreSQL paid admission independently fails closed.
            unknown = {
                info.replica_id:
                    kueue_lane_capacity.KueueReplicaCapacityClass.UNKNOWN
                for info in replica_infos
                if isinstance(
                    getattr(info, 'reserved_fill_intent_idempotency_key', None),
                    str) and bool(info.reserved_fill_intent_idempotency_key)
            }
            logger.warning(
                'Failed to prepare Kueue admission capacity for '
                'service %s: %s', self._service_name,
                common_utils.format_exception(error))
            kueue_snapshot = kueue_lane_capacity.KueueReplicaCapacitySnapshot(
                unknown)
        service_time_estimates = self._prepare_service_time_estimates()
        cold_paid_order: tuple[str, ...] = ()
        prospective_paid_order: tuple[str, ...] = ()
        if isinstance(self, ConcurrencyAutoscaler):
            configured_cards = self._configured_cards_from_profiles()
            with self._cold_paid_cost_snapshot_for_tick():
                cold_paid_order = tuple(
                    self._cold_paid_card_order(configured_cards))
                prospective_paid_order = tuple(
                    self._prospective_paid_card_order(configured_cards))
        gpu_shape_handles = self._resolve_gpu_shape_handles(replica_infos)
        base_inputs = ScalingDecisionInputs(
            gpu_shape_handles=gpu_shape_handles,
            historical_scaling_values=historical_values,
            service_time_estimates_by_accelerator=service_time_estimates,
            cold_paid_accelerator_order=cold_paid_order,
            prospective_paid_accelerator_order=prospective_paid_order)
        exact_shapes: dict[int, tuple[str, int]] = {}
        for info in replica_infos:
            cached = self._gpu_shape_cache.get(info.replica_id)
            shape = ((cached[0].casefold(),
                      int(cached[1])) if cached is not None else
                     _exact_gpu_shape_from_decision_inputs(info, base_inputs))
            if shape is not None:
                exact_shapes[info.replica_id] = shape
        base_inputs = dataclasses.replace(
            base_inputs,
            replica_bindings=build_replica_planning_bindings(
                replica_infos, exact_shapes),
            gpu_shapes_by_replica_id=exact_shapes)
        return bind_locked_kueue_capacity_snapshot(base_inputs, replica_infos,
                                                   kueue_snapshot)

    def _prepare_service_time_estimates(
            self) -> dict[str, dict[str, float | int]]:
        """Return optional PostgreSQL exact-card timing evidence."""
        return {}

    def _cached_historical_scaling_versions(self) -> set[int]:
        """Return versions whose capacity metadata needs no durable read."""
        raise NotImplementedError

    def _normalize_historical_scaling_spec(self, spec: Any) -> Any:
        """Extract immutable capacity metadata from one historical spec."""
        raise NotImplementedError

    @staticmethod
    def _validate_scaling_decision_inputs(
        decision_inputs: ScalingDecisionInputs,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> None:
        if not isinstance(decision_inputs, ScalingDecisionInputs):
            raise TypeError('Invalid scaling decision input token.')
        replica_ids = tuple(info.replica_id for info in replica_infos)
        if (any(
                type(replica_id) is not int or replica_id < 1
                for replica_id in replica_ids) or
                len(set(replica_ids)) != len(replica_ids) or
                decision_inputs.replica_ids != tuple(sorted(replica_ids))):
            raise ValueError('Scaling decision inputs do not match the exact '
                             'replica snapshot.')
        expected_bindings = build_replica_planning_bindings(
            replica_infos, decision_inputs.gpu_shapes_by_replica_id)
        if decision_inputs.replica_bindings != expected_bindings:
            raise ValueError('Scaling decision inputs do not match the exact '
                             'replica identities.')
        gpu_shapes = decision_inputs.gpu_shapes_by_replica_id
        if (not isinstance(gpu_shapes, dict) or
                set(gpu_shapes) - set(replica_ids) or
                any(not isinstance(replica_id, int) or isinstance(
                    replica_id, bool) or not isinstance(shape, tuple) or
                    len(shape) != 2 or not isinstance(shape[0], str) or
                    not shape[0] or type(shape[1]) is not int or shape[1] < 1
                    for replica_id, shape in gpu_shapes.items())):
            raise ValueError('Scaling decision inputs have invalid exact GPU '
                             'shapes.')
        kueue_classes = decision_inputs.kueue_capacity_by_replica_id
        if (set(kueue_classes) - set(replica_ids) or not all(
                isinstance(value, kueue_lane_capacity.KueueReplicaCapacityClass)
                for value in kueue_classes.values())):
            raise ValueError('Scaling decision inputs have an invalid Kueue '
                             'admission snapshot.')
        for shapes in (decision_inputs.kueue_blocked_retirement_shapes,):
            if (not isinstance(shapes, frozenset) or not all(
                    isinstance(shape, tuple) and len(shape) == 2 and
                    isinstance(shape[0], str) and shape[0] and
                    isinstance(shape[1], int) and
                    not isinstance(shape[1], bool) and shape[1] >= 0
                    for shape in shapes)):
                raise ValueError('Scaling decision inputs have invalid Kueue '
                                 'capacity scopes.')
        transition_ids = decision_inputs.kueue_transition_replica_ids
        ready_paid_ids = (
            decision_inputs.kueue_ready_paid_replacement_replica_ids)
        if (not isinstance(transition_ids, frozenset) or
                not isinstance(ready_paid_ids, frozenset) or
                not ready_paid_ids <= transition_ids or
                not transition_ids <= set(replica_ids) or
                any(not isinstance(value, int) or isinstance(value, bool)
                    for value in transition_ids)):
            raise ValueError('Scaling decision inputs have invalid Kueue '
                             'replacement replicas.')
        for card, estimate in (
                decision_inputs.service_time_estimates_by_accelerator.items()):
            if (not isinstance(card, str) or not card or
                    not isinstance(estimate, dict) or
                    not isinstance(estimate.get('duration_seconds'),
                                   (int, float)) or
                    isinstance(estimate.get('duration_seconds'), bool) or
                    float(estimate['duration_seconds']) <= 0 or
                    not isinstance(estimate.get('samples'), int) or
                    isinstance(estimate.get('samples'), bool) or
                    int(estimate['samples'])
                    < constants.AUTOSCALER_ADAPTIVE_DURATION_MIN_SAMPLES or
                    not isinstance(estimate.get('observed_at'), (int, float)) or
                    isinstance(estimate.get('observed_at'), bool)):
                raise ValueError('Scaling decision inputs have invalid '
                                 'service-time estimates.')
        for order in (decision_inputs.cold_paid_accelerator_order,
                      decision_inputs.prospective_paid_accelerator_order):
            if (not isinstance(order, tuple) or any(
                    not isinstance(card, str) or not card for card in order) or
                    len({card.casefold() for card in order}) != len(order)):
                raise ValueError('Scaling decision inputs have an invalid '
                                 'paid accelerator order.')

    def _resolve_fill_gpu_shape(
            self, info: 'replica_managers.ReplicaInfo') -> tuple[str, int]:
        """Expose exact GPU shapes to the shared reserved-fill overlay."""
        return self._get_gpu_shape_from_replica_info(info)

    def _kueue_capacity_class(
        self, info: 'replica_managers.ReplicaInfo'
    ) -> kueue_lane_capacity.KueueReplicaCapacityClass | None:
        # Legacy pickles, direct helper tests, and third-party shape-aware
        # autoscalers can predate the prepared Kueue tick fields.  No active
        # snapshot is the neutral historical behavior; it must not turn a
        # read-only capacity classification into an AttributeError.
        snapshot = getattr(self, '_kueue_capacity_by_replica_id_for_tick', None)
        if snapshot is None:
            return None
        return snapshot.get(info.replica_id)

    def _kueue_counts_as_assigned(self,
                                  info: 'replica_managers.ReplicaInfo') -> bool:
        """Fresh waiting Pods are the sole zero-width assigned state."""
        return (
            self._kueue_capacity_class(info)
            is not kueue_lane_capacity.KueueReplicaCapacityClass.FRESH_WAITING)

    def _kueue_ordinary_victim_eligible(
            self,
            info: 'replica_managers.ReplicaInfo',
            resolved_shape: tuple[str, int] | None = None) -> bool:
        """Keep replacement safety exact-shape scoped and paid-first."""
        try:
            if resolved_shape is None:
                resolved_shape = self._resolve_fill_gpu_shape(info)
            raw_card, count = resolved_shape
            shape = (raw_card.casefold(), int(count))
        except (AttributeError, TypeError, ValueError):
            shape = ('*', 0)
        blocked: frozenset[tuple[str, int]] = getattr(
            self, '_kueue_blocked_retirement_shapes_for_tick', frozenset())
        if ('*', 0) in blocked or shape in blocked:
            return False
        admission = self._kueue_capacity_class(info)
        if admission in (
                kueue_lane_capacity.KueueReplicaCapacityClass.FRESH_WAITING,
                kueue_lane_capacity.KueueReplicaCapacityClass.UNKNOWN):
            return False
        transition_replica_ids: frozenset[int] = getattr(
            self, '_kueue_transition_replica_ids_for_tick', frozenset())
        if info.replica_id in transition_replica_ids:
            # While compatible paid capacity covers the transition, admitted
            # reserved supply is not a victim.  Only a compatible paid row may
            # retire, and only once at least one replacement is READY.
            ready_paid_replacement_ids: frozenset[int] = getattr(
                self, '_kueue_ready_paid_replacement_replica_ids_for_tick',
                frozenset())
            return info.replica_id in ready_paid_replacement_ids
        if (admission is
                kueue_lane_capacity.KueueReplicaCapacityClass.POLICY_ADMITTED):
            return info.is_ready
        return True

    @staticmethod
    def _gpu_shape_from_resources_override(
            replica_info: 'replica_managers.ReplicaInfo'
    ) -> tuple[str, int] | None:
        """Return the exact shape carried by a replica launch override."""
        resources_override = replica_info.resources_override
        if not isinstance(resources_override, dict):
            return None
        accelerators = resources_override.get('accelerators')
        if not isinstance(accelerators, dict) or not accelerators:
            return None
        gpu_type = next(iter(accelerators))
        if not isinstance(gpu_type, str) or not gpu_type:
            return None
        try:
            gpu_count = max(1, int(accelerators[gpu_type]))
        except (TypeError, ValueError):
            gpu_count = 1
        return gpu_type, gpu_count

    def _prune_gpu_shape_cache(self, live_replica_ids: set[int]) -> None:
        """Drop cached shapes/costs for replicas that no longer exist."""
        for replica_id in list(self._gpu_shape_cache):
            if replica_id not in live_replica_ids:
                del self._gpu_shape_cache[replica_id]
        for replica_id in list(self._replica_cost_cache):
            if replica_id not in live_replica_ids:
                del self._replica_cost_cache[replica_id]
        record_ids = getattr(self, '_replica_cache_record_ids', None)
        if record_ids is not None:
            for replica_id in list(record_ids):
                if replica_id not in live_replica_ids:
                    del record_ids[replica_id]

    def _bind_replica_cache_identity(
            self, replica_info: 'replica_managers.ReplicaInfo') -> None:
        """Invalidate both memos when a numeric id names a new DB row."""
        record_id = getattr(replica_info, 'replica_record_id', None)
        if not isinstance(record_id, str) or not record_id:
            # An identity-less row can be read, but it can never safely own a
            # memo across calls.
            self._gpu_shape_cache.pop(replica_info.replica_id, None)
            self._replica_cost_cache.pop(replica_info.replica_id, None)
            return
        record_ids = getattr(self, '_replica_cache_record_ids', None)
        if record_ids is None:
            record_ids = {}
            self._replica_cache_record_ids = record_ids
        previous = record_ids.get(replica_info.replica_id)
        if previous is not None and previous != record_id:
            self._gpu_shape_cache.pop(replica_info.replica_id, None)
            self._replica_cost_cache.pop(replica_info.replica_id, None)
        record_ids[replica_info.replica_id] = record_id

    def _resolve_replica_handles(
            self, replica_infos: list['replica_managers.ReplicaInfo']
    ) -> dict[int, Any]:
        """Batch-resolve cluster handles for replicas missing a cached memo.

        `ReplicaInfo.handle()` with no record hits the cluster table once per
        call, and while a replica is provisioning neither the shape nor the
        cost memo may cache (the record is rewritten per failover attempt), so
        a selection pass that scores each replica twice would pay 2 reads per
        provisioning replica. One batched read replaces all of them, and also
        scores shape and cost from the same record snapshot instead of two
        reads at different times mid-sort.
        """
        for info in replica_infos:
            self._bind_replica_cache_identity(info)
        uncached = [
            info for info in replica_infos
            if info.replica_id not in self._gpu_shape_cache or
            info.replica_id not in self._replica_cost_cache
        ]
        if not uncached:
            return {}
        tick_handles = self._gpu_shape_handles_for_tick
        if tick_handles is not None:
            return {
                info.replica_id: tick_handles.get(info.replica_id)
                for info in uncached
            }
        records = global_user_state.get_clusters_from_names(
            [info.cluster_name for info in uncached])
        handles: dict[int, Any] = {}
        for info in uncached:
            record = records.get(info.cluster_name)
            # A missing record means the cluster row is gone; a bare
            # info.handle() would resolve to None too, just via another read.
            handles[info.replica_id] = (info.handle(record)
                                        if record is not None else None)
        return handles

    def _resolve_gpu_shape_handles(
            self, replica_infos: list['replica_managers.ReplicaInfo']
    ) -> dict[int, Any]:
        """Batch-resolve legacy shapes before entering an autoscaler lock.

        Exact-card launch overrides are hard resource constraints and can be
        read directly. They are deliberately not memoized until launch
        succeeds, so an override rewritten by failover is observed next tick.
        Cost-aware victim selection still needs launched-resource handles, so
        every missing shape or cost memo is included in this one outside-lock
        batch instead of falling back to per-replica reads under the lock.
        """
        for info in replica_infos:
            self._bind_replica_cache_identity(info)
        unresolved = [
            info for info in replica_infos if (not info.is_terminal and (
                (info.replica_id not in self._gpu_shape_cache and
                 self._gpu_shape_from_resources_override(info) is None) or
                info.replica_id not in self._replica_cost_cache))
        ]
        if not unresolved:
            return {}
        records = global_user_state.get_clusters_from_names(
            [info.cluster_name for info in unresolved])
        return {
            info.replica_id: (info.handle(records[info.cluster_name])
                              if info.cluster_name in records else None
                             ) for info in unresolved
        }

    def _get_hourly_cost_from_replica_info(
            self,
            replica_info: 'replica_managers.ReplicaInfo',
            handle: Any = _UNRESOLVED_HANDLE) -> float:
        """Hourly cost of a replica's launched resources (0.0 = reserved).

        Used to prefer scaling down PAID replicas before zero-cost ones
        (e.g. cloud spot before a reserved Kubernetes pool) -- without
        this, shedding the expensive replica first is luck, not policy.
        Unknown costs resolve to 0.0 (treated like reserved capacity, so
        they are shed last -- the conservative direction for cost).
        """
        self._bind_replica_cache_identity(replica_info)
        cached = self._replica_cost_cache.get(replica_info.replica_id)
        if cached is not None:
            return cached
        cost = 0.0
        resolved = False
        try:
            if handle is _UNRESOLVED_HANDLE:
                tick_handles = self._gpu_shape_handles_for_tick
                if tick_handles is not None:
                    handle = tick_handles.get(replica_info.replica_id)
                else:
                    handle = replica_info.handle()
            if handle is not None:
                # Coerce: anything non-numeric degrades to 0.0 (shed last).
                cost = float(handle.launched_resources.get_cost(seconds=3600))
                resolved = True
        except Exception:  # pylint: disable=broad-except
            cost = 0.0
        # Same post-launch-only cache rule as the shape memo: while the
        # replica is provisioning the record may be rewritten by failover.
        if (resolved and replica_info.status_property.sky_launch_status
                == common_utils.ProcessStatus.SUCCEEDED):
            self._replica_cost_cache[replica_info.replica_id] = cost
        return cost

    def _get_gpu_shape_from_replica_info(
            self,
            replica_info: 'replica_managers.ReplicaInfo',
            handle: Any = _UNRESOLVED_HANDLE) -> tuple[str, int]:
        """Extract (GPU type, GPU count) from ReplicaInfo object."""
        self._bind_replica_cache_identity(replica_info)
        cached = self._gpu_shape_cache.get(replica_info.replica_id)
        if cached is not None:
            return cached
        override_shape = self._gpu_shape_from_resources_override(replica_info)
        if override_shape is not None:
            gpu_type, gpu_count = override_shape
        else:
            gpu_type = 'unknown'
            gpu_count = 1
            if handle is _UNRESOLVED_HANDLE:
                tick_handles = self._gpu_shape_handles_for_tick
                if tick_handles is not None:
                    handle = tick_handles.get(replica_info.replica_id,
                                              _UNRESOLVED_HANDLE)
            if handle is _UNRESOLVED_HANDLE:
                handle = replica_info.handle()
            if handle is not None:
                accelerators = handle.launched_resources.accelerators
                if accelerators and len(accelerators) > 0:
                    # Get the first accelerator entry.
                    gpu_type = list(accelerators.keys())[0]
                    try:
                        gpu_count = max(1, int(accelerators[gpu_type]))
                    except (TypeError, ValueError):
                        gpu_count = 1
        # Cache only a resolved shape of a replica whose launch has finished.
        # While the replica is still provisioning, the cluster record (and
        # thus launched_resources) is rewritten for every failover attempt, so
        # the accelerator resolved mid-launch may not be the one the launch
        # finally lands on and must be re-resolved on later ticks.
        if (gpu_type != 'unknown' and
                replica_info.status_property.sky_launch_status
                == common_utils.ProcessStatus.SUCCEEDED):
            self._gpu_shape_cache[replica_info.replica_id] = (gpu_type,
                                                              gpu_count)
        return gpu_type, gpu_count

    def _known_gpu_shape_from_replica_info(
            self, replica_info: 'replica_managers.ReplicaInfo'
    ) -> tuple[str, int] | None:
        """Return only already-materialized exact-card facts, without I/O."""
        self._bind_replica_cache_identity(replica_info)
        cached = self._gpu_shape_cache.get(replica_info.replica_id)
        if cached is not None:
            return cached
        return self._gpu_shape_from_resources_override(replica_info)

    def _cost_rebalance_location_is_compatible(
        self,
        incumbent: 'replica_managers.ReplicaInfo',
        location: spot_placer.Location,
    ) -> bool:
        """Keep authoritative exact-card targets stable during rebalancing."""
        configured_shapes = self.configured_accelerator_shapes
        if not configured_shapes:
            return True
        canonical_by_name = {
            card.casefold(): (card, count)
            for card, count in configured_shapes.items()
        }
        incumbent_card, incumbent_count = (
            self._get_gpu_shape_from_replica_info(incumbent))
        candidate_card, candidate_count = Autoscaler._location_gpu_shape(  # pylint: disable=protected-access
            location)
        configured = canonical_by_name.get(candidate_card.casefold())
        if configured is None:
            return False
        canonical_card, configured_count = configured
        return (incumbent_card.casefold() == canonical_card.casefold() and
                incumbent_count == configured_count and
                candidate_count == configured_count)


class InstanceAwareRequestRateAutoscaler(_GpuShapeResolverMixin,
                                         RequestRateAutoscaler):
    """Instance-aware RequestRateAutoscaler:
    Autoscale based on each replica's GPU-specific QPS.

    This autoscaler considers different QPS targets for different GPU types
    when target_qps_per_replica is provided as a dictionary mapping GPU types
    to their respective QPS targets.
    """

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        super().__init__(service_name, spec, version)
        # Serializes version/catalog publication, demand ingestion, reserved
        # supply, and decision generation. The controller publishes a retained
        # QPS autoscaler through update_version_and_accelerator_shapes(), so a
        # decision can never combine the new QPS dict with the old card catalog.
        self._instance_state_lock = threading.RLock()
        # Ensure target_qps_per_replica is a dict for instance-aware logic
        assert isinstance(spec.target_qps_per_replica, dict), \
            'InstanceAware Autoscaler requires dict type target_qps_per_replica'
        # Re-assign with correct type using setattr to avoid typing issues
        self.target_qps_per_replica = spec.target_qps_per_replica
        # Memoizes a replica's resolved GPU shape (replica_id ->
        # (gpu_type, gpu_count)) so the blocking handle() DB read + unpickle
        # is not repeated for the same replica across the 2-3 passes per
        # decision tick. A shape is cached only once the replica's launch has
        # finished: while it is still provisioning, the cluster record is
        # rewritten for every failover attempt and its accelerators can
        # change, so a mid-launch resolution must be re-resolved on later
        # ticks. After launch the shape is fixed for the replica's lifetime.
        # Pruned to the live replica set each tick.
        self._gpu_shape_cache: dict[int, tuple[str, int]] = {}
        # replica_id -> hourly cost of launched resources (same lifecycle
        # rules as the shape cache).
        self._replica_cost_cache: dict[int, float] = {}
        self._replica_cache_record_ids: dict[int, str] = {}
        # Shapes already warned about bare-key per-GPU scaling.
        self._bare_key_warned: set[tuple[str, int]] = set()
        # One-shot hysteresis bypass, armed by update_version AND at
        # construction: the base class snaps target_num_replicas directly
        # after an update so the service scales quickly; the instance-
        # aware equivalent must wait for the next tick's shape-aware
        # recompute, which must then apply its result immediately instead
        # of being gated behind the upscale/downscale delay counters.
        # Armed at construction because a rebuilt autoscaler (controller
        # restart) starts at target=min_replicas with no hysteresis
        # history worth protecting: mid-rolling-update, letting that
        # stale minimum stand for the upscale delay would satisfy the
        # drain's 'ready latest >= target' cutoff and retire all old
        # capacity while the real target is still counters away.
        self._snap_target_on_next_recompute: bool = True
        # version -> that version's qps dict. A live replica's capacity is
        # a property of the spec it was launched under: after a
        # shape-changing update (e.g. {'L4': 0.1} -> {'A100': 10.0}) the
        # old shape is missing from the new dict and would resolve via the
        # min-value fallback — overestimating 100 old L4s by 100x, which
        # collapses the computed target and lets the rolling drain kill
        # them before the new capacity exists. Pruned each tick to the
        # live replica versions (+ latest).
        self._qps_dict_by_version: dict[int, dict[str, float]] = {
            version: spec.target_qps_per_replica
        }
        # Missing or failed historical-spec reads fall back for one decision
        # tick. Keep that fallback out of the durable live-version cache so a
        # later tick retries and can heal.
        self._qps_dict_unavailable_versions_for_tick: set[int] | None = None
        self.compatibility_profiles: list[dict[str, Any]] = []
        # Outstanding queue demand is a last-writer-wins gauge. Unlike arrival
        # profiles, it must be replaced on every authoritative LB report rather
        # than accumulated across the QPS window.
        self.queued_compatibility_profiles: list[dict[str, Any]] = []
        # Recent rejections are a replaceable gauge used for launch priority.
        # They do not change the QPS magnitude, which remains derived from the
        # accepted-arrival window.
        self.rejected_compatibility_profiles: list[dict[str, Any]] = []
        # False after a catalog transition until a version-matched LB report
        # replaces every exact-card gauge. Incomplete/old reports may still
        # refresh aggregate QPS timestamps but cannot re-arm cleared profiles.
        self._compatibility_demand_complete: bool = False
        # Controller-owned exact task shapes. target_qps_per_replica keys are
        # performance profiles, not an authoritative resource shape: a bare
        # A100 profile can still describe an A100:8 task resource.
        self.configured_accelerator_shapes: dict[str, int] = {}
        # Fresh cached physical reserved supply, fed once per controller tick.
        # This is marginal supply only; ready/provisioning replicas are counted
        # independently below and must not be double-counted.
        self.free_reserved_slots_by_accelerator: dict[str, int] = {}
        configured_cards = self._configured_cards_from_profiles()
        while (sum(self.target_num_replicas_by_accelerator.values())
               < self.target_num_replicas and configured_cards):
            card = configured_cards[0]
            self.target_num_replicas_by_accelerator[card] = (
                self.target_num_replicas_by_accelerator.get(card, 0) + 1)

    def set_configured_accelerator_shapes(self,
                                          shapes: dict[str, int],
                                          *,
                                          backend_num_nodes: int = 1) -> None:
        """Set canonical exact-card GPU counts from active task resources."""
        with self._instance_state_lock:
            self._set_configured_accelerator_shapes_locked(
                shapes, backend_num_nodes=backend_num_nodes)

    def _set_configured_accelerator_shapes_locked(
            self,
            shapes: dict[str, int],
            *,
            backend_num_nodes: int = 1) -> None:
        """Set exact-card shapes while holding the instance-state lock."""
        if (not isinstance(backend_num_nodes, int) or
                isinstance(backend_num_nodes, bool) or backend_num_nodes < 1):
            raise ValueError('Backend node count must be a positive integer.')
        previous_shapes = self.configured_accelerator_shapes
        previous_num_nodes = self.backend_num_nodes
        configured_shapes = {
            str(card): int(count)
            for card, count in shapes.items()
            if isinstance(card, str) and card and isinstance(count, int) and
            not isinstance(count, bool) and count > 0
        }
        catalog_changed = (bool(previous_shapes) and
                           (configured_shapes != previous_shapes or
                            backend_num_nodes != previous_num_nodes))
        self.configured_accelerator_shapes = configured_shapes
        self.backend_num_nodes = backend_num_nodes
        if catalog_changed or not configured_shapes:
            self.compatibility_profiles = []
            self.queued_compatibility_profiles = []
            self.queued_deadline_profiles = None
            self.rejected_compatibility_profiles = []
            self.target_num_replicas_by_accelerator = {}
            self.warm_retention_target_by_accelerator = {}
            self.cold_launch_authority_by_accelerator = {}
            self.free_reserved_slots_by_accelerator = {}
            self._compatibility_demand_complete = False

    def set_free_reserved_slots_by_accelerator(self, slots: dict[str,
                                                                 int]) -> None:
        """Set fresh unmaterialized reserved supply by exact card."""
        with self._instance_state_lock:
            self._set_free_reserved_slots_by_accelerator_locked(slots)

    def _set_free_reserved_slots_by_accelerator_locked(
            self, slots: dict[str, int]) -> None:
        configured_by_name = {
            card.casefold(): card
            for card in self._configured_cards_from_profiles()
        }
        normalized: dict[str, int] = {}
        for raw_card, raw_count in slots.items():
            card = configured_by_name.get(str(raw_card).casefold())
            if card is None or isinstance(raw_count, bool):
                continue
            try:
                count = int(raw_count)
            except (TypeError, ValueError):
                continue
            if count > 0:
                normalized[card] = normalized.get(card, 0) + count
        self.free_reserved_slots_by_accelerator = normalized

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        with self._instance_state_lock:
            self._collect_request_information_locked(request_aggregator_info)

    def _collect_request_information_locked(
            self, request_aggregator_info: dict[str, Any]) -> None:
        super().collect_request_information(request_aggregator_info)
        compatibility_complete = request_aggregator_info.get(
            'compatibility_demand_complete')
        if compatibility_complete is not True:
            # Direct/legacy construction without an authoritative catalog
            # retains the pre-fence test and compatibility behavior. Once the
            # controller supplies a catalog, only a version-matched complete
            # report may replace exact-card state.
            compatibility_complete = ('compatibility_demand_complete'
                                      not in request_aggregator_info and
                                      not self.configured_accelerator_shapes)
        if not compatibility_complete:
            self._compatibility_demand_complete = False
            return
        for profile in request_aggregator_info.get('compatibility_profiles',
                                                   []):
            if not isinstance(profile, dict):
                continue
            timestamp = profile.get('timestamp')
            priority = profile.get('priority')
            accelerators = profile.get('compatible_accelerators')
            count = profile.get('count', 1)
            if (not isinstance(timestamp,
                               (int, float)) or isinstance(timestamp, bool) or
                    not isinstance(priority, int) or
                    isinstance(priority, bool) or accelerators is None or
                    not isinstance(accelerators, list) or not accelerators or
                    not isinstance(count, int) or isinstance(count, bool) or
                    count < 1 or not all(
                        isinstance(item, str) and item
                        for item in accelerators)):
                continue
            self.compatibility_profiles.append({
                'timestamp': float(timestamp),
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'count': count,
            })
        queued_profiles: list[dict[str, Any]] = []
        for profile in request_aggregator_info.get(
                'queued_requests_by_compatibility', []):
            if not isinstance(profile, dict):
                continue
            priority = profile.get('priority')
            accelerators = profile.get('compatible_accelerators')
            count = profile.get('count', 1)
            if (not isinstance(priority, int) or isinstance(priority, bool) or
                    not isinstance(accelerators, list) or not accelerators or
                    not isinstance(count, int) or isinstance(count, bool) or
                    count < 1 or not all(
                        isinstance(item, str) and item
                        for item in accelerators)):
                continue
            queued_profiles.append({
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'count': count,
            })
        self.queued_compatibility_profiles = queued_profiles
        rejected_profiles: list[dict[str, Any]] = []
        for profile in request_aggregator_info.get(
                'rejected_requests_by_compatibility', []):
            if not isinstance(profile, dict):
                continue
            priority = profile.get('priority')
            accelerators = profile.get('compatible_accelerators')
            count = profile.get('count', 1)
            recent_count = profile.get('recent_count', count)
            if (not isinstance(priority, int) or isinstance(priority, bool) or
                    not isinstance(accelerators, list) or not accelerators or
                    not isinstance(count, int) or isinstance(count, bool) or
                    count < 1 or not isinstance(recent_count, int) or
                    isinstance(recent_count, bool) or recent_count < 0 or
                    recent_count > count or not all(
                        isinstance(item, str) and item
                        for item in accelerators)):
                continue
            rejected_profiles.append({
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'count': count,
                'recent_count': recent_count,
            })
        self.rejected_compatibility_profiles = rejected_profiles
        self._compatibility_demand_complete = True
        self._launch_priority_report_received_at = time.time()
        cutoff = time.time() - self.qps_window_size
        self.compatibility_profiles = [
            profile for profile in self.compatibility_profiles
            if profile['timestamp'] >= cutoff
        ]

    def _dump_dynamic_states(self) -> dict[str, Any]:
        """Preserve exact-card demand across an autoscaler replacement."""
        with self._instance_state_lock:
            return self._dump_dynamic_states_locked()

    def _dump_dynamic_states_locked(self) -> dict[str, Any]:
        states = super()._dump_dynamic_states()
        states['compatibility_profiles'] = [{
            **profile,
            'compatible_accelerators': list(profile['compatible_accelerators']),
        } for profile in self.compatibility_profiles]
        states['queued_compatibility_profiles'] = [{
            **profile,
            'compatible_accelerators': list(profile['compatible_accelerators']),
        } for profile in self.queued_compatibility_profiles]
        states['rejected_compatibility_profiles'] = [{
            **profile,
            'compatible_accelerators': list(profile['compatible_accelerators']),
        } for profile in self.rejected_compatibility_profiles]
        states['compatibility_demand_complete'] = (
            self._compatibility_demand_complete)
        states['configured_accelerator_shapes'] = dict(
            self.configured_accelerator_shapes)
        states['backend_num_nodes'] = self.backend_num_nodes
        states['launch_priority_report_received_at'] = (
            self._launch_priority_report_received_at)
        return states

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        """Restore exact-card arrivals and the replaceable queue gauge."""
        compatibility_arrivals_present = ('compatibility_profiles'
                                          in dynamic_states)
        profiles = dynamic_states.pop('compatibility_profiles', [])
        queued_profiles = dynamic_states.pop('queued_compatibility_profiles',
                                             [])
        rejected_profiles = dynamic_states.pop(
            'rejected_compatibility_profiles', [])
        compatibility_complete = bool(
            dynamic_states.pop('compatibility_demand_complete', False))
        source_shapes = dynamic_states.pop('configured_accelerator_shapes', {})
        source_num_nodes = dynamic_states.pop('backend_num_nodes', 1)
        priority_report_received_at = dynamic_states.pop(
            'launch_priority_report_received_at', None)
        super()._load_dynamic_states(dynamic_states)
        self.compatibility_profiles = []
        self.queued_compatibility_profiles = []
        self.rejected_compatibility_profiles = []
        self.configured_accelerator_shapes = {
            str(card): int(count)
            for card, count in source_shapes.items()
            if isinstance(card, str) and card and isinstance(count, int) and
            not isinstance(count, bool) and count > 0
        } if isinstance(source_shapes, dict) else {}
        self.backend_num_nodes = (source_num_nodes
                                  if isinstance(source_num_nodes, int) and
                                  not isinstance(source_num_nodes, bool) and
                                  source_num_nodes > 0 else 1)
        # Cross-type dumps from older binaries do not identify the catalog
        # that admitted their profiles. Preserve aggregate timestamps but fail
        # closed on exact-card transfer until a fresh report arrives.
        compatibility_complete = (compatibility_complete and
                                  compatibility_arrivals_present and
                                  bool(self.configured_accelerator_shapes))
        self.collect_request_information({
            'timestamps': [],
            'compatibility_profiles': profiles,
            'queued_requests_by_compatibility': queued_profiles,
            'rejected_requests_by_compatibility': rejected_profiles,
            'compatibility_demand_complete': compatibility_complete,
        })
        self._launch_priority_report_received_at = (
            float(priority_report_received_at)
            if isinstance(priority_report_received_at, (int, float)) and
            not isinstance(priority_report_received_at, bool) else None)

    def generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        decision_inputs = self._prepare_scaling_decision_inputs(replica_infos)
        return self._generate_scaling_decisions_with_inputs(
            replica_infos, active_versions, decision_inputs)

    def _cached_historical_scaling_versions(self) -> set[int]:
        with self._instance_state_lock:
            return set(self._qps_dict_by_version)

    def _normalize_historical_scaling_spec(
            self, spec: Any) -> dict[str, float] | None:
        if spec is None or not isinstance(spec.target_qps_per_replica, dict):
            return None
        return dict(spec.target_qps_per_replica)

    def _generate_scaling_decisions_with_inputs(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
        decision_inputs: ScalingDecisionInputs,
    ) -> list[AutoscalerDecision]:
        """Consume controller-prepared handles without public API changes."""
        self._validate_scaling_decision_inputs(decision_inputs, replica_infos)
        shape_handles = decision_inputs.gpu_shape_handles
        if shape_handles is None:
            raise ValueError('Shape-aware scaling inputs have no handle '
                             'snapshot.')
        historical_values = decision_inputs.historical_scaling_values
        if historical_values is None:
            raise ValueError('Shape-aware scaling inputs have no historical '
                             'capacity snapshot.')
        for version, value in historical_values.items():
            if value is not None and not isinstance(value, dict):
                raise TypeError('Invalid prepared historical QPS '
                                f'value for version {version}.')
        with self._instance_state_lock:
            self._gpu_shape_handles_for_tick = shape_handles
            self._kueue_capacity_by_replica_id_for_tick = dict(
                decision_inputs.kueue_capacity_by_replica_id)
            self._kueue_blocked_retirement_shapes_for_tick = (
                decision_inputs.kueue_blocked_retirement_shapes)
            self._kueue_transition_replica_ids_for_tick = (
                decision_inputs.kueue_transition_replica_ids)
            self._kueue_ready_paid_replacement_replica_ids_for_tick = (
                decision_inputs.kueue_ready_paid_replacement_replica_ids)
            self._qps_dict_unavailable_versions_for_tick = {
                version for version, value in historical_values.items()
                if value is None
            }
            for version, value in historical_values.items():
                if value is not None:
                    assert isinstance(value, dict)
                    self._qps_dict_by_version[version] = value
            with self._cold_paid_cost_snapshot_for_tick():
                try:
                    return self._generate_scaling_decisions_locked(
                        replica_infos, active_versions)
                finally:
                    self._qps_dict_unavailable_versions_for_tick = None
                    self._gpu_shape_handles_for_tick = None
                    self._kueue_capacity_by_replica_id_for_tick = None
                    self._kueue_blocked_retirement_shapes_for_tick = frozenset()
                    self._kueue_transition_replica_ids_for_tick = frozenset()
                    self._kueue_ready_paid_replacement_replica_ids_for_tick = (
                        frozenset())

    def _generate_scaling_decisions_locked(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        # Recompute the shape-aware target BEFORE the base class runs the
        # outdated-replica drain: the drain compares ready new-version
        # replicas against target_num_replicas, and a stale target (e.g.
        # right after an update that lowered per-replica capacity) would
        # scale down every old replica while only a fraction of the
        # required new capacity exists. This is the single recompute for
        # the tick; _generate_scaling_decisions must not recompute again
        # or the hysteresis counters would double-increment.
        # Drop cached GPU types for replicas that no longer exist so the
        # cache stays bounded to the live replica set.
        live_replica_ids = {info.replica_id for info in replica_infos}
        self._prune_gpu_shape_cache(live_replica_ids)
        keep_versions = {info.version for info in replica_infos}
        keep_versions.add(self.latest_version)
        for version in list(self._qps_dict_by_version):
            if version not in keep_versions:
                del self._qps_dict_by_version[version]
        self._set_target_num_replicas_with_instance_aware_logic(replica_infos)
        return super().generate_scaling_decisions(replica_infos,
                                                  active_versions)

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate autoscaling decisions with instance-aware logic.

        The shape-aware target was already recomputed for this tick in
        generate_scaling_decisions (before the outdated-replica drain).
        """
        latest_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []

        for info in replica_infos:
            if (not info.is_terminal and info.version == self.latest_version and
                    self._kueue_counts_as_assigned(info)):
                latest_nonterminal_replicas.append(info)

        target_num_replicas = self.get_final_target_num_replicas()
        current_num_replicas = len(latest_nonterminal_replicas)

        scaling_decisions: list[AutoscalerDecision] = []

        target_by_card, use_card_targets = (
            self._actuation_target_by_accelerator(replica_infos))
        self.capacity_target_by_accelerator = dict(target_by_card)
        self.capacity_target_complete = use_card_targets
        if use_card_targets:
            replicas_by_card: dict[str, list[replica_managers.ReplicaInfo]] = {}
            ready_by_card: dict[str, int] = {}
            for info in latest_nonterminal_replicas:
                if _replica_is_retiring_card_supply(info):
                    continue
                card, _ = self._get_gpu_shape_from_replica_info(info)
                replicas_by_card.setdefault(card, []).append(info)
                if info.is_ready:
                    ready_by_card[card] = ready_by_card.get(card, 0) + 1
            shortages = {
                card: max(0, target - len(replicas_by_card.get(card, [])))
                for card, target in target_by_card.items()
            }
            if any(shortages.values()):
                for card, shortage in shortages.items():
                    for _ in range(shortage):
                        scaling_decisions.append(
                            AutoscalerDecision(
                                AutoscalerDecisionOperator.SCALE_UP,
                                target={
                                    'accelerators': {
                                        card: self._configured_gpu_count(card)
                                    }
                                }))
                # Graceful non-preemptive transition: provisioning rows count
                # against duplicate launches, but excess old-card capacity is
                # retained until every target card is actually READY.
                return scaling_decisions
            all_targets_ready = all(
                ready_by_card.get(card, 0) >= target
                for card, target in target_by_card.items())
            if all_targets_ready:
                for card, replicas in replicas_by_card.items():
                    excess = max(0, len(replicas) - target_by_card.get(card, 0))
                    if excess <= 0:
                        continue
                    eligible = [
                        info for info in replicas
                        if self._kueue_ordinary_victim_eligible(info)
                    ]
                    ordered_ids = self._select_replicas_to_scale_down_by_qps(
                        len(eligible), eligible)
                    by_id = {info.replica_id: info for info in eligible}
                    remaining_ready = sum(info.is_ready for info in replicas)
                    selected: list[int] = []
                    for replica_id in ordered_ids:
                        info = by_id[replica_id]
                        if (info.is_ready and
                                remaining_ready - 1 < target_by_card.get(
                                    card, 0)):
                            continue
                        selected.append(replica_id)
                        if info.is_ready:
                            remaining_ready -= 1
                        if len(selected) >= excess:
                            break
                    for replica_id in selected:
                        scaling_decisions.append(
                            AutoscalerDecision(
                                AutoscalerDecisionOperator.SCALE_DOWN,
                                target=replica_id))
            return scaling_decisions

        # Decide if to scale up or down.
        if target_num_replicas > current_num_replicas:
            for _ in range(target_num_replicas - current_num_replicas):
                # No resources_override to use when scaling up
                scaling_decisions.append(
                    AutoscalerDecision(AutoscalerDecisionOperator.SCALE_UP,
                                       target=None))
        elif target_num_replicas < current_num_replicas:
            num_replicas_to_scale_down = \
                current_num_replicas - target_num_replicas

            # Use instance-aware scale down logic
            eligible = [
                info for info in latest_nonterminal_replicas
                if self._kueue_ordinary_victim_eligible(info)
            ]
            ordered_ids = self._select_replicas_to_scale_down_by_qps(
                len(eligible), eligible)
            replicas_to_scale_down = []
            for replica_id in ordered_ids:
                replicas_to_scale_down.append(replica_id)
                if len(replicas_to_scale_down) >= num_replicas_to_scale_down:
                    break
            for replica_id in replicas_to_scale_down:
                scaling_decisions.append(
                    AutoscalerDecision(AutoscalerDecisionOperator.SCALE_DOWN,
                                       target=replica_id))

        # Outdated replicas are handled by base class generate_scaling_decisions
        # No need to handle them here

        upscale_decisions = [
            d for d in scaling_decisions
            if d.operator == AutoscalerDecisionOperator.SCALE_UP
        ]
        downscale_decisions = [
            d for d in scaling_decisions
            if d.operator == AutoscalerDecisionOperator.SCALE_DOWN
        ]
        logger.info(f'Scaling decisions: '
                    f'{len(upscale_decisions)} scale up, '
                    f'{len(downscale_decisions)} scale down '
                    f'(latest nonterminal: {current_num_replicas}, '
                    f'target: {target_num_replicas})')

        return scaling_decisions

    def _configured_cards_from_profiles(self) -> list[str]:
        # A controller-provided task catalog is authoritative. In particular,
        # recent arrivals from the previous service version must not revive a
        # card that the active version removed. Direct/unit-test construction
        # has no task catalog and retains the additive fallbacks below.
        if self.configured_accelerator_shapes:
            return list(self.configured_accelerator_shapes)
        cards: list[str] = []
        seen: set[str] = set()
        if isinstance(self.target_qps_per_replica, dict):
            for key in self.target_qps_per_replica:
                card = key.partition(':')[0]
                if card.casefold() not in seen:
                    cards.append(card)
                    seen.add(card.casefold())
        for profile in (self.compatibility_profiles +
                        self.queued_compatibility_profiles):
            for card in profile['compatible_accelerators']:
                if card.casefold() not in seen:
                    cards.append(card)
                    seen.add(card.casefold())
        for card in self.min_replicas_by_accelerator:
            if card.casefold() not in seen:
                cards.append(card)
                seen.add(card.casefold())
        return cards

    def _actuation_target_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> tuple[dict[str, int], bool]:
        """Revalidate exact QPS cold launches at the adopted total target."""
        demand_target = self.target_num_replicas_by_accelerator
        compatibility_complete = (self._compatibility_demand_complete or
                                  not self.configured_accelerator_shapes)
        if (not compatibility_complete or
                sum(demand_target.values()) != self.target_num_replicas):
            return {}, False
        has_exact_profiles = bool(self.compatibility_profiles or
                                  self.queued_compatibility_profiles)
        exact_profiles_available = (has_exact_profiles and
                                    (self._compatibility_demand_complete or
                                     not self.configured_accelerator_shapes))
        exact_arrival_qps = 0.0
        if exact_profiles_available:
            exact_arrival_qps = (sum(
                float(profile.get('count', 1))
                for profile in self.compatibility_profiles) /
                                 self.qps_window_size)
        aggregate_qps = len(self.request_timestamps) / self.qps_window_size
        aggregate_fallback_qps = max(0.0, aggregate_qps - exact_arrival_qps)
        final_target = self.get_final_target_num_replicas()
        desired_target = self._calculate_target_by_accelerator(
            replica_infos,
            include_exact_profiles=exact_profiles_available,
            fallback_aggregate_qps=aggregate_fallback_qps,
            min_replicas_override=final_target,
            max_replicas_override=final_target,
            use_existing_supply=True)
        cards = self._configured_cards_from_profiles()
        canonical_by_name = {card.casefold(): card for card in cards}
        nonretiring_supply = {card: 0 for card in cards}
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info) or
                    not self._kueue_counts_as_assigned(info)):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = canonical_by_name.get(raw_card.casefold())
            if card is not None:
                nonretiring_supply[card] += 1
        for raw_card, count in self.free_reserved_slots_by_accelerator.items():
            card = canonical_by_name.get(raw_card.casefold())
            if card is not None:
                nonretiring_supply[card] += max(0, int(count))
        target = _revalidate_actuation_target(
            adopted_target=demand_target,
            desired_target=desired_target,
            nonretiring_supply=nonretiring_supply,
            configured_cards=cards,
            final_target=final_target)
        return target, sum(target.values()) == final_target

    def _cold_paid_card_order(self, configured_cards: list[str]) -> list[str]:
        """Order cold cards by nominal paid cost, independent of availability."""
        return self._order_cold_paid_cards_for_tick(configured_cards,
                                                    self._configured_gpu_count,
                                                    self._location_gpu_shape)

    def _configured_gpu_count(self, card: str) -> int:
        """Return the service's unique configured GPU count for a card."""
        for configured, count in self.configured_accelerator_shapes.items():
            if configured.casefold() == card.casefold():
                return count
        if isinstance(self.target_qps_per_replica, dict):
            prefix = f'{card.casefold()}:'
            for key in self.target_qps_per_replica:
                normalized = key.casefold()
                if normalized == card.casefold():
                    return 1
                if normalized.startswith(prefix):
                    try:
                        count = int(normalized[len(prefix):])
                    except ValueError:
                        continue
                    if count > 0:
                        return count
        return 1

    def _calculate_target_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        *,
        include_exact_profiles: bool = True,
        fallback_aggregate_qps: float | None = None,
        min_replicas_override: int | None = None,
        max_replicas_override: int | None = None,
        use_existing_supply: bool = False,
        additional_zero_cost_supply_by_accelerator: (Mapping[str, int] |
                                                     None) = None,
    ) -> dict[str, int]:
        """Allocate recent demand to exact cards, priority first."""
        configured_cards = self._configured_cards_from_profiles()
        floors_by_name = {
            card.casefold(): floor
            for card, floor in self.min_replicas_by_accelerator.items()
        }
        capacities = {
            card: self._get_target_qps_for_gpu_shape(
                card,
                self._configured_gpu_count(card),
                version=self.latest_version) for card in configured_cards
        }
        ready_zero_cost: dict[str, int] = {card: 0 for card in configured_cards}
        committed_zero_cost: dict[str, int] = {
            card: 0 for card in configured_cards
        }
        ready_paid: dict[str, int] = {card: 0 for card in configured_cards}
        committed_paid: dict[str, int] = {card: 0 for card in configured_cards}
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info) or
                    not self._kueue_counts_as_assigned(info)):
                continue
            card, _ = self._get_gpu_shape_from_replica_info(info)
            if card not in ready_zero_cost:
                continue
            if info.is_zero_cost is True:
                committed_zero_cost[card] += 1
                if info.is_ready:
                    ready_zero_cost[card] += 1
            else:
                # Unknown attribution is conservatively paid. It may suppress
                # a duplicate cold launch but can never be promoted into the
                # preferred zero-cost tiers.
                committed_paid[card] += 1
                if info.is_ready:
                    ready_paid[card] += 1

        cold_order = self._cold_paid_card_order(configured_cards)
        profiles = ([(int(profile['priority']),
                      tuple(profile['compatible_accelerators']),
                      float(profile.get('count', 1)) / self.qps_window_size)
                     for profile in (self.compatibility_profiles +
                                     self.queued_compatibility_profiles)]
                    if include_exact_profiles else [])
        if fallback_aggregate_qps is not None and fallback_aggregate_qps > 0:
            # Missing/incomplete exact telemetry means every configured card
            # is compatible. Preserve aggregate demand and let the shared
            # supply-aware allocator compose it with hard per-card floors.
            profiles.append(
                (0, tuple(configured_cards), fallback_aggregate_qps))
        free_reserved = self.free_reserved_slots_by_accelerator
        if additional_zero_cost_supply_by_accelerator is not None:
            free_reserved = _canonical_additional_supply(
                configured_cards, additional_zero_cost_supply_by_accelerator)
        return _allocate_compatibility_target(
            configured_cards=configured_cards,
            capacities=capacities,
            floors=floors_by_name,
            min_replicas=(self.min_replicas if min_replicas_override is None
                          else min_replicas_override),
            max_replicas=(self.max_replicas if max_replicas_override is None
                          else max_replicas_override),
            demand_profiles=profiles,
            fixed_work_by_accelerator={},
            ready_zero_cost=ready_zero_cost,
            committed_zero_cost=committed_zero_cost,
            free_reserved=free_reserved,
            ready_paid=ready_paid,
            committed_paid=committed_paid,
            supply_preference=(
                autoscaler_compatibility.SupplyPreference.WARM_FIRST),
            cold_order=cold_order,
            use_existing_supply=use_existing_supply)

    def _set_target_num_replicas_with_instance_aware_logic(
            self, replica_infos: list['replica_managers.ReplicaInfo']) -> None:
        """Set target_num_replicas using instance-aware logic."""
        assert isinstance(self.target_qps_per_replica,
                          dict), 'Expected dict for instance-aware logic'
        num_requests_per_second = len(
            self.request_timestamps) / self.qps_window_size
        candidate_target_by_accelerator: dict[str, int] | None = None
        latest_capacities: list[float] = []
        configured_accelerator_shapes = self.configured_accelerator_shapes
        has_exact_profiles = bool(self.compatibility_profiles or
                                  self.queued_compatibility_profiles)
        exact_profiles_available = (has_exact_profiles and
                                    (self._compatibility_demand_complete or
                                     not configured_accelerator_shapes))
        exact_arrival_qps = 0.0
        if exact_profiles_available:
            exact_arrival_qps = (sum(
                float(profile.get('count', 1))
                for profile in self.compatibility_profiles) /
                                 self.qps_window_size)
        # Completeness describes the current report and its replaceable
        # gauges. It cannot retroactively attribute aggregate arrivals from an
        # earlier incomplete report that are still inside the QPS window.
        # Preserve that unmatched remainder as all-configured-card demand.
        aggregate_fallback_qps = max(
            0.0, num_requests_per_second - exact_arrival_qps)
        if (configured_accelerator_shapes or exact_profiles_available or
                self.min_replicas_by_accelerator):
            candidate_target_by_accelerator = (
                self._calculate_target_by_accelerator(
                    replica_infos,
                    include_exact_profiles=exact_profiles_available,
                    fallback_aggregate_qps=aggregate_fallback_qps))
            target_num_replicas = self._clip_target_num_replicas(
                sum(candidate_target_by_accelerator.values()))
        else:
            # Compatibility telemetry is additive and versioned. Preserve the
            # pre-feature aggregate algorithm for an old LB rather than
            # inventing card assignments from missing data.
            target_qps_dict = self.target_qps_per_replica
            for info in replica_infos:
                if info.is_terminal or info.version != self.latest_version:
                    continue
                capacity = self._get_target_qps_for_gpu_shape(
                    *self._get_gpu_shape_from_replica_info(info),
                    version=info.version)
                if capacity > 0:
                    latest_capacities.append(capacity)
            latest_capacities.sort(reverse=True)
            raw_target_num = 0
            covered_qps = 0.0
            for capacity in latest_capacities:
                raw_target_num += 1
                covered_qps += capacity
                if covered_qps > num_requests_per_second:
                    break
            if covered_qps <= num_requests_per_second:
                remaining_qps = num_requests_per_second - covered_qps
                estimated_qps = (latest_capacities[0]
                                 if latest_capacities else 0.0)
                if estimated_qps <= 0:
                    estimated_qps = max(target_qps_dict.values())
                if estimated_qps > 0 and remaining_qps > 0:
                    raw_target_num += math.ceil(remaining_qps / estimated_qps)
            raw_target_num = max(raw_target_num,
                                 sum(self.min_replicas_by_accelerator.values()))
            target_num_replicas = self._clip_target_num_replicas(raw_target_num)
        logger.info(f'Instance-aware autoscaling: '
                    f'requests/s: {num_requests_per_second}, '
                    f'latest-version capacities: {latest_capacities}, '
                    'target by accelerator: '
                    f'{candidate_target_by_accelerator}, '
                    f'target replicas (latest version): '
                    f'{target_num_replicas}')

        # Apply hysteresis logic
        old_target_num_replicas = self.target_num_replicas

        target_map_changed = (candidate_target_by_accelerator is not None and
                              candidate_target_by_accelerator
                              != self.target_num_replicas_by_accelerator)
        candidate_target_map = candidate_target_by_accelerator or {}
        apply_target = False
        if self._snap_target_on_next_recompute:
            # First recompute after an update: apply directly (the base
            # class's post-update snap semantics, but shape-aware).
            self._snap_target_on_next_recompute = False
            self.upscale_counter = 0
            self.downscale_counter = 0
            apply_target = True
        # Faster scale up when there is no replica.
        elif self.target_num_replicas == 0:
            apply_target = True
        elif target_num_replicas > self.target_num_replicas:
            self.upscale_counter += 1
            self.downscale_counter = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                apply_target = True
        elif target_num_replicas < self.target_num_replicas:
            self.downscale_counter += 1
            self.upscale_counter = 0
            if self.downscale_counter >= self.scale_down_threshold:
                self.downscale_counter = 0
                apply_target = True
        elif (target_map_changed and any(
                candidate_target_map.get(card, 0) >
                self.target_num_replicas_by_accelerator.get(card, 0)
                for card in candidate_target_map)):
            # A same-size exact-card migration is an upscale for hysteresis
            # purposes. A LOWER aggregate target is handled by the branch
            # above even when its card mix contains a positive delta: card
            # churn must not restart aggregate downscale proof indefinitely.
            self.upscale_counter += 1
            self.downscale_counter = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                apply_target = True
        elif target_map_changed:
            self.downscale_counter += 1
            self.upscale_counter = 0
            if self.downscale_counter >= self.scale_down_threshold:
                self.downscale_counter = 0
                apply_target = True
        else:
            self.upscale_counter = self.downscale_counter = 0
        if apply_target:
            self.target_num_replicas = target_num_replicas
            # Aggregate fallback deliberately has no exact-card assignment.
            # Clear an older compatibility map so status and the decision path
            # cannot reuse a stale shape target after the gauge becomes empty
            # or an old LB stops publishing compatibility telemetry.
            self.target_num_replicas_by_accelerator = dict(
                candidate_target_by_accelerator or {})

        logger.info(
            f'Instance-aware: Old target number of replicas: '
            f'{old_target_num_replicas}. '
            f'Current target number of replicas: {target_num_replicas}. '
            f'Final target number of replicas: {self.target_num_replicas}. '
            f'Num overprovision: {self.num_overprovision}. '
            f'Upscale counter: {self.upscale_counter}/'
            f'{self.scale_up_threshold}. '
            f'Downscale counter: {self.downscale_counter}/'
            f'{self.scale_down_threshold}. ')

    def _select_outdated_replicas_to_scale_down(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[int]:
        """Capacity-aware rolling drain of old-version replicas.

        The base class keeps (target - ready_new) OLD replicas — a count
        that treats every replica as interchangeable. With per-shape
        capacities that can retire 99% of the serving capacity while one
        big new replica is still alone. Keep old replicas by CAPACITY:
        enough READY old ones to cover the demand the ready latest
        replicas cannot yet serve, never fewer than the base class would
        have kept.
        """
        if self.update_mode != serve_utils.UpdateMode.ROLLING:
            candidates = super()._select_outdated_replicas_to_scale_down(
                replica_infos, active_versions)
            by_id = {info.replica_id: info for info in replica_infos}
            return [
                replica_id for replica_id in candidates
                if self._kueue_ordinary_victim_eligible(by_id[replica_id])
            ]
        old_nonterminal = [
            info for info in replica_infos
            if info.version < self.latest_version and not info.is_terminal
        ]
        if not old_nonterminal:
            return []
        actuation_target, exact_target_complete = (
            self._actuation_target_by_accelerator(replica_infos))
        if exact_target_complete:
            canonical_by_name = {
                card.casefold(): card for card in actuation_target
            }
            ready_latest_by_card = {card: 0 for card in actuation_target}
            for info in replica_infos:
                if (info.version != self.latest_version or not info.is_ready or
                        _replica_is_retiring_card_supply(info)):
                    continue
                raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                card = canonical_by_name.get(raw_card.casefold())
                if card is not None:
                    ready_latest_by_card[card] += 1
            if any(
                    ready_latest_by_card.get(card, 0) < target
                    for card, target in actuation_target.items()):
                # The latest fleet may satisfy the aggregate count entirely
                # with the wrong card. Launch the exact replacement first;
                # retaining all old replicas for one more tick is the only
                # non-preemptive rollout choice.
                return []
        num_ready_latest = 0
        ready_latest_capacity = 0.0
        for info in replica_infos:
            if (info.version == self.latest_version and info.is_ready and
                    not _replica_is_retiring_card_supply(info)):
                num_ready_latest += 1
                ready_latest_capacity += self._get_target_qps_for_gpu_shape(
                    *self._get_gpu_shape_from_replica_info(info),
                    version=info.version)
        if num_ready_latest >= self.get_final_target_num_replicas():
            # Enough latest-version replicas: retire all old ones (same
            # terminal condition as the base class).
            return [
                info.replica_id
                for info in old_nonterminal
                if self._kueue_ordinary_victim_eligible(info)
            ]

        demand = len(self.request_timestamps) / self.qps_window_size
        shortfall = demand - ready_latest_capacity
        # Never keep fewer old replicas than the base class's count rule
        # (target - ready_new): capacity packing with a few big old
        # replicas could otherwise drain the standby pool a low-traffic
        # service relies on for its next request.
        keep_count_floor = min(
            len(old_nonterminal),
            max(0,
                self.get_final_target_num_replicas() - num_ready_latest))

        ready_old = []
        nonready_old = []
        for info in old_nonterminal:
            capacity = self._get_target_qps_for_gpu_shape(
                *self._get_gpu_shape_from_replica_info(info),
                version=info.version)
            if info.is_ready:
                ready_old.append((capacity, info))
            else:
                nonready_old.append((capacity, info))
        unavailable_versions = self._qps_dict_unavailable_versions_for_tick
        if unavailable_versions:
            logger.info(
                'Instance-aware rolling drain waiting for historical '
                'capacity for versions: %s.', sorted(unavailable_versions))
            return []
        # Largest capacity first: fewest old replicas kept, fastest
        # rollout. Replica id tie-break keeps the selection stable
        # across ticks.
        ready_old.sort(key=lambda pair: (-pair[0], pair[1].replica_id))

        keep_ids: set[int] = set()
        covered_qps = 0.0
        for capacity, info in ready_old:
            if covered_qps >= shortfall and len(keep_ids) >= keep_count_floor:
                break
            keep_ids.add(info.replica_id)
            if capacity > 0:
                covered_qps += capacity
        # Not-yet-ready old replicas add no serving capacity; they only
        # count toward the base-class floor (the base helper likewise
        # prefers draining initializing replicas first).
        for _, info in nonready_old:
            if len(keep_ids) >= keep_count_floor:
                break
            keep_ids.add(info.replica_id)

        return [
            info.replica_id
            for info in old_nonterminal
            if (info.replica_id not in keep_ids and
                self._kueue_ordinary_victim_eligible(info))
        ]

    def _get_qps_dict_for_version(self, version: int) -> dict[str, float]:
        """The qps dict a given service version was launched under.

        Unknown versions (the autoscaler was rebuilt after the update
        that created them, e.g. a controller restart mid-rolling-update)
        rehydrate from the durable per-version spec so old-version
        replicas keep their real capacity. Falls back to the latest dict
        when the version's spec is unavailable; misses are not memoized
        across ticks so a transient DB error can heal on the next tick.
        """
        cached = self._qps_dict_by_version.get(version)
        if cached is not None:
            return cached
        unavailable_versions = self._qps_dict_unavailable_versions_for_tick
        if (unavailable_versions is not None and
                version in unavailable_versions):
            assert isinstance(self.target_qps_per_replica, dict), \
                'Expected dict for instance-aware logic'
            return self.target_qps_per_replica
        # Historical metadata has one canonical I/O path: the prepared token.
        # An unexpected miss must fail closed rather than re-enter PostgreSQL
        # from decision generation under the controller routing lock.
        if unavailable_versions is not None:
            unavailable_versions.add(version)
        else:
            logger.warning(
                'Historical QPS version %s was used outside a prepared '
                'decision; using the latest-version fallback.', version)
        assert isinstance(self.target_qps_per_replica, dict), \
            'Expected dict for instance-aware logic'
        return self.target_qps_per_replica

    def _get_target_qps_for_gpu_shape(self,
                                      gpu_type: str,
                                      gpu_count: int,
                                      version: int | None = None) -> float:
        """Per-replica target QPS for a `gpu_count` x `gpu_type` replica.

        Resolution (see serve_utils.resolve_target_qps_for_gpu_shape):
        exact shape key is a per-replica value; a bare type key is
        per-GPU and is multiplied by the replica's GPU count.

        `version` selects the qps dict the replica was launched under, so
        old-version replicas keep their real capacity across a
        shape-changing update (falls back to the latest dict when the
        version's dict is unknown, e.g. after a controller restart).
        """
        assert isinstance(self.target_qps_per_replica,
                          dict), 'Expected dict for instance-aware logic'
        target_qps_dict = self.target_qps_per_replica
        if version is not None and version != self.latest_version:
            target_qps_dict = self._get_qps_dict_for_version(version)

        resolved = serve_utils.resolve_target_qps_for_gpu_shape(
            gpu_type, gpu_count, target_qps_dict)
        if resolved is not None:
            if (gpu_count > 1 and
                    f'{gpu_type}:{gpu_count}' not in target_qps_dict and
                (gpu_type, gpu_count) not in self._bare_key_warned):
                # Per-GPU scaling of a bare type key assumes ONE model
                # instance per GPU. A replica serving K-GPU model
                # instances is over-counted by K unless an exact shape
                # key pins its per-replica capacity. Warn once per shape.
                self._bare_key_warned.add((gpu_type, gpu_count))
                logger.warning(
                    f'Multi-GPU replica shape {gpu_type}:{gpu_count} is '
                    'scaled from a bare per-GPU QPS key. This is correct '
                    'ONLY if each GPU hosts one model instance; for '
                    'k-GPU-per-instance models declare an exact shape '
                    f'key (e.g. "{gpu_type}:{gpu_count}": '
                    '<instances_per_replica * qps_per_instance>).')
            return resolved

        # Fallback to minimum QPS
        unavailable_versions = self._qps_dict_unavailable_versions_for_tick
        using_historical_fallback = (version is not None and
                                     version != self.latest_version and
                                     unavailable_versions is not None and
                                     version in unavailable_versions)
        if not using_historical_fallback:
            logger.warning(f'No matching QPS found for GPU shape: '
                           f'{gpu_type}:{gpu_count}. '
                           f'Available types: {list(target_qps_dict.keys())}. '
                           f'Using minimum QPS as fallback.')
        return min(target_qps_dict.values())

    def _cost_rebalance_replica_capacity(
            self, info: 'replica_managers.ReplicaInfo') -> float:
        return self._get_target_qps_for_gpu_shape(
            *self._get_gpu_shape_from_replica_info(info), version=info.version)

    def _cost_rebalance_location_capacity(
            self, location: spot_placer.Location) -> float:
        return self._get_target_qps_for_gpu_shape(
            *self._location_gpu_shape(location), version=self.latest_version)

    def _select_replicas_to_scale_down_by_qps(
            self, num_replicas_to_scale_down: int,
            replica_infos: list['replica_managers.ReplicaInfo']) -> list[int]:
        """Select replicas to scale down (lowest QPS first)."""
        # Create a list of (replica_info, target_qps) tuples
        replica_qps_pairs: list[tuple[replica_managers.ReplicaInfo, float]] = []

        # One batched cluster-table read for every replica the memos cannot
        # serve; the sort below scores each replica twice (shape + cost).
        handles = self._resolve_replica_handles(replica_infos)

        for info in replica_infos:
            # Include old-version replicas as well so they also get a target_qps
            # assigned. Skip terminal replicas only.
            if info.is_terminal:
                continue

            # Get GPU shape directly from replica info
            gpu_type, gpu_count = self._get_gpu_shape_from_replica_info(
                info, handles.get(info.replica_id, _UNRESOLVED_HANDLE))
            if not self._kueue_ordinary_victim_eligible(info,
                                                        (gpu_type, gpu_count)):
                continue

            # Use flexible matching logic, weighted by GPU count so
            # smaller-capacity replicas are preferred for scale-down.
            target_qps = self._get_target_qps_for_gpu_shape(
                gpu_type, gpu_count, version=info.version)

            replica_qps_pairs.append((info, float(target_qps)))
            logger.info(f'Replica {info.replica_id} '
                        f'with GPU {gpu_type}:{gpu_count}: {target_qps} QPS')

        # Create a mapping from replica_id to target_qps for sorting
        replica_qps_map = {
            info.replica_id: target_qps
            for info, target_qps in replica_qps_pairs
        }

        # Sort replicas by: 1. status order, 2. target_qps (asc),
        # 3. version (asc), 4. replica_id (desc).
        # scale_down_decision_order() is a classmethod returning the
        # static ordering list; the sort key needs this replica's INDEX
        # in it (the list itself is identical for every replica and
        # would let weighted QPS outrank status).
        status_order = serve_state.ReplicaStatus.scale_down_decision_order()

        def _status_rank(info: 'replica_managers.ReplicaInfo') -> int:
            try:
                return status_order.index(info.status)
            except ValueError:
                return len(status_order)

        # Cost breaks ties AFTER capacity (qps): among replicas of equal
        # serving capacity, shed the most expensive first (cloud spot
        # before a zero-cost reserved pool). Cost must NOT outrank qps —
        # the downscale target is computed assuming the highest-capacity
        # replicas are kept, so shedding a high-capacity paid replica
        # ahead of low-capacity free ones could leave less capacity than
        # the target assumed. Uniform-capacity fleets (all per-type qps
        # equal) get full cost-priority within each status tier.
        #
        # PER-MACHINE vs PER-GPU: the cost used here is the replica's
        # whole-machine hourly cost, and that is deliberate. Because cost
        # only compares replicas of equal RESOLVED qps (the configured
        # per-type targets, count-weighted — for unresolved shapes the
        # min-qps fallback applies, so this is not a guarantee about TRUE
        # capacity), machine cost ranks identically to cost-per-unit-of-
        # serving-capacity (same denominator) — the economically correct
        # metric. It is strictly better than per-GPU price, which would
        # misrank GPU types with different throughput (an A100:1 at
        # \$2/hr serving 0.4 qps beats an L4:4 at \$2.40/hr serving the
        # same 0.4 qps, despite the L4s' lower per-GPU price). The qps
        # key is quantized so float noise (3 * 0.1 != 0.3) cannot split
        # mathematically equal capacities away from the cost tie-break.
        sorted_replicas = sorted(
            replica_infos,
            key=lambda info: (
                _status_rank(info),
                round(replica_qps_map.get(info.replica_id, float('inf')), 9),
                -self._get_hourly_cost_from_replica_info(
                    info, handles.get(info.replica_id, _UNRESOLVED_HANDLE)),
                info.version,
                -info.replica_id,
            ))

        selected_replica_ids = []
        for info in sorted_replicas:
            if info.is_terminal:
                continue
            selected_replica_ids.append(info.replica_id)
            if len(selected_replica_ids) >= num_replicas_to_scale_down:
                break

        logger.info(
            f'Selected {len(selected_replica_ids)} replicas to scale down: '
            f'{selected_replica_ids}')
        return selected_replica_ids

    def _calculate_target_num_replicas(self) -> int:
        # Shape-aware sizing needs replica_infos, which this hook (invoked
        # by the base update_version to snap the target after an update)
        # does not receive. Keep the current target instead of snapping to
        # a shape-blind estimate: the outdated-replica drain in
        # generate_scaling_decisions consumes the target BEFORE the
        # instance-aware recompute runs, so an underestimate here could
        # scale down all old replicas mid-rolling-update with only a
        # fraction of the new capacity ready. The next decision tick
        # recomputes from live replica shapes via
        # _set_target_num_replicas_with_instance_aware_logic.
        return self._clip_target_num_replicas(self.target_num_replicas)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        with self._instance_state_lock:
            self._update_version_locked(version, spec, update_mode)

    def update_version_and_accelerator_shapes(
            self,
            version: int,
            spec: 'service_spec.SkyServiceSpec',
            update_mode: serve_utils.UpdateMode,
            accelerator_shapes: dict[str, int],
            *,
            backend_num_nodes: int = 1) -> None:
        """Atomically publish a QPS version and its exact-card catalog."""
        with self._instance_state_lock:
            self._update_version_locked(version, spec, update_mode)
            self._set_configured_accelerator_shapes_locked(
                accelerator_shapes, backend_num_nodes=backend_num_nodes)

    def _update_version_locked(self, version: int,
                               spec: 'service_spec.SkyServiceSpec',
                               update_mode: serve_utils.UpdateMode) -> None:
        # Ensure it's a dict and re-assign using setattr to avoid typing.
        # Must happen BEFORE super().update_version: the base class
        # recomputes target_num_replicas via _calculate_target_num_replicas,
        # which must see the new version's dict.
        if version <= self.latest_version:
            # The base class rejects stale versions; don't mutate the qps
            # dict or arm the post-update snap for a rejected call either.
            super(RequestRateAutoscaler,
                  self).update_version(version, spec, update_mode)
            return
        assert isinstance(spec.target_qps_per_replica, dict), \
            'InstanceAware Autoscaler requires dict type target_qps_per_replica'
        # Assign BEFORE the base update runs so any recompute it triggers
        # sees the new version's dict.
        self.target_qps_per_replica = spec.target_qps_per_replica
        self._qps_dict_by_version[version] = spec.target_qps_per_replica
        super(RequestRateAutoscaler,
              self).update_version(version, spec, update_mode)
        self._snap_target_on_next_recompute = True


class ConcurrencyAutoscaler(_GpuShapeResolverMixin, _AutoscalerWithHysteresis):
    """ConcurrencyAutoscaler: size the fleet by outstanding work.

    For long synchronous jobs (~1 h, one per GPU) request RATE measures
    arrival compression, not load: 100 hour-long jobs arriving over 2 min
    vs over 10 min are the same 100 concurrent jobs but produce 3x
    different QPS targets. This autoscaler instead targets
    `ceil(outstanding / per_replica_concurrency)` where outstanding =
    in-flight + queued + recently-rejected jobs, all reported by the LB as
    GAUGES over the sync channel (no clear-on-ack batches to lose or
    double-count on controller hiccups).

    The knob `target_concurrency_per_replica` is PER GPU. Physical-backend
    services pack outstanding work onto knob x gpu_count capacities. Logical
    services publish GPU-slot targets and divide outstanding work by the knob;
    backend packing happens later from those whole-slot targets.

    SIGNAL-GAP RULE: the demand gauges only exist in LB reports. A report
    is fresh iff it carried a non-None in-flight map and is younger than
    3x the LB sync interval. While no fresh report exists -- including a
    freshly (re)built autoscaler, which starts stale -- this autoscaler
    emits NO scale-down decisions and NO rolling-drain retirements at all:
    a rebuilt controller starts at target=min_replicas with no data, and
    acting on that would mass-retire a live fleet before the first sync.
    Scale-UP stays available while stale via the arrival floor
    ceil(arrivals_in_window / best_capacity) from request timestamps
    (which ride every sync), so a blind controller can still grow, never
    shrink.
    """

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        super().__init__(service_name, spec, version)
        target_concurrency = spec.target_concurrency_per_replica
        assert target_concurrency is not None, (
            'ConcurrencyAutoscaler requires target_concurrency_per_replica')
        # Per-GPU target concurrency; a replica's capacity in concurrency
        # units is this knob x its gpu_count.
        self.target_concurrency_per_replica: float = float(target_concurrency)
        self.target_utilization_percentage: int = int(
            spec.target_utilization_percentage)
        self.expected_request_duration_seconds: float | None = (
            spec.expected_request_duration_seconds)
        self.initial_provision_lead_time_seconds: float | str | None = (
            spec.initial_provision_lead_time_seconds)
        self.adaptive_demand_estimation: bool = (spec.adaptive_demand_estimation
                                                 is not False)
        # Live demand-estimation state. Both estimators supersede their
        # configured counterpart only while they hold enough fresh evidence;
        # configuration remains the fallback and the cold-start value.
        self._measured_duration_seconds: float | None = None
        self._measured_duration_samples: int = 0
        self._measured_duration_at: float | None = None
        # Cumulative per-bucket counts already folded into the estimate, so
        # a repeated (unacknowledged) histogram report is not double counted.
        self._prediction_counts_seen: dict[int, list[int]] = {}
        self._provision_lead_samples: list[float] = []
        self._provision_lead_at: float | None = None
        # Replica rows whose launch-to-ready has already been sampled.
        self._provision_lead_seen_replica_ids: set[int] = set()
        self.max_scale_up_rate_percentage: int | None = (
            spec.max_scale_up_rate_percentage)
        self.scale_up_rate_min_replicas: int | None = (
            spec.scale_up_rate_min_replicas)
        self.scale_up_rate_period_seconds: int | None = (
            spec.scale_up_rate_period_seconds)
        adaptive_scale_up = spec.adaptive_scale_up
        self.adaptive_scale_up: dict[str, Any] | None = (
            dict(adaptive_scale_up)
            if isinstance(adaptive_scale_up, dict) else None)
        queue_config = spec.lb_request_queue or {}
        self._queue_timeout_seconds: float | None = queue_config.get(
            'timeout_seconds')
        self._queue_timeout_thresholds: tuple[tuple[int, float], ...] = tuple(
            (int(entry['min_priority']), float(entry['timeout_seconds']))
            for entry in queue_config.get('timeout_seconds_by_priority', ()))
        # SkyServiceSpec exposes 50 for new specs and restores 100 for old
        # pickles through __setstate__.
        self.max_scale_down_rate_percentage: int = int(
            spec.max_scale_down_rate_percentage)
        self._last_scale_up_wave_at: float | None = None
        # The timestamp opens a rollout window; this ceiling retains the
        # unspent part of that window when placement cannot make progress on
        # its first reconciliation tick. It is latest-version committed
        # logical capacity plus the authorized wave width.
        self._logical_scale_up_wave_ceiling: int | None = None
        # Logical downscale hysteresis is elapsed-time based. A nominal
        # decision tick can stretch substantially while probing a large fleet,
        # so a tick counter cannot implement a duration contract. This state is
        # deliberately controller-local and resets conservatively on rebuilds
        # and service updates.
        self._downscale_started_at: float | None = None
        self._raw_target_num_replicas: int = self.target_num_replicas
        self._latest_committed_capacity: int = 0
        self._latest_provisioning_capacity: int = 0
        self._rejected_concurrency: float = 0.0
        self._weighted_queue_work: float = 0.0
        self._deadline_capacity_plan: DeadlineCapacityPlan | None = None
        self._deadline_target_by_accelerator: dict[str, int] = {}
        self._deadline_infeasible_by_priority: dict[int, float] = {}
        self._service_time_estimates_by_accelerator: dict[str, dict[str, float |
                                                                    int]] = {}
        self._service_time_source_by_accelerator: dict[str, str] = {}
        self._effective_service_time_by_accelerator: dict[str, float] = {}
        self._arrival_floor_target: int = 0
        # Request timestamps back the arrival floor (the only up-signal
        # available while the demand report is stale), windowed exactly
        # like RequestRateAutoscaler's QPS window.
        self.qps_window_size: int = constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS
        self.request_timestamps: list[float] = []
        # Latest demand report from the LB. `None` in-flight means no
        # usable report has ever been received (or the loaded one carried
        # none): the signal-gap rule keys on this plus the report's age.
        # The gauges are stored verbatim; freshness is derived, never
        # stored, so a report ages out automatically (also after a
        # _load_dynamic_states round-trip, since the received-at time is
        # absolute).
        self._in_flight_by_replica_id: dict[int, int] | None = None
        self._queue_depth: int = 0
        self._queue_depth_by_priority: dict[int, int] | None = None
        self._rejected_in_window: int = 0
        self._rejected_in_recent_window: int | None = None
        self._rejected_in_window_by_priority: dict[int, int] | None = None
        self._rejected_in_recent_window_by_priority: dict[int,
                                                          int] | None = None
        self._unique_job_arrivals_60s: int | None = None
        self._unique_job_arrivals_300s: int | None = None
        self._headerless_arrivals_60s: int | None = None
        self._headerless_arrivals_300s: int | None = None
        self._offered_arrival_tracking_saturated: bool = False
        self._pressure_baseline: tuple[int, int, int] | None = None
        self._pressure_latched: bool = False
        self._pressure_reasons: tuple[str, ...] = ()
        self._pressure_streak: int = 0
        self._adaptive_until: float | None = None
        self._downscale_veto_reason: str | None = None
        # Consecutive pressure vetoes within the current downscale episode
        # (a run of recomputes whose raw target stays below the adopted
        # target). Bounded by _MAX_CONSECUTIVE_DOWNSCALE_VETOES: under
        # trickle traffic a tiny positive delta re-latches pressure nearly
        # every decision tick, and an unbounded veto would defer downscale
        # forever even after the hysteresis timer elapsed.
        self._downscale_veto_streak: int = 0
        self._pending_retention_floor: int | None = None
        self._pending_capacity_at_adoption: int = 0
        self._pending_budget_spent: int = 0
        self._last_scale_down_allowance: int = 0
        self._last_pending_allowance: int = 0
        # Replica ids whose declared async occupancy could not be sampled.
        # They contribute a retention floor to outstanding work: raw capacity
        # in physical mode and utilization-adjusted capacity in logical mode.
        # Unknown is a potentially-full replica, never an idle zero, but it is
        # not measured saturation that authorizes extra utilization headroom.
        self._unknown_in_flight_replica_ids: set[int] = set()
        self._report_received_at: float | None = None
        self._reconcile_generation: int = 0
        self._observed_slots_by_replica_id: dict[int, int] = {}
        self._unknown_capacity_replica_ids: set[int] = set()
        # Unknown capacity and an authoritative zero-slot report both mean a
        # ready backend cannot currently serve work. They share one bounded,
        # one-wave replacement incident state.
        self._degraded_capacity_since_by_replica_id: dict[int, float] = {}
        self._logical_state_lock = threading.RLock()
        self._last_logical_target_state: LogicalCapacityTarget | None = None
        self._gpu_shape_cache: dict[int, tuple[str, int]] = {}
        # Backs the cost-descending victim tiebreak (shed paid spot
        # before zero-cost reserved capacity); pruned with the shape
        # cache each tick.
        self._replica_cost_cache: dict[int, float] = {}
        self._replica_cache_record_ids: dict[int, str] = {}
        # Replaceable exact-card gauges shipped with the authoritative
        # concurrency report. Running work is attributed separately from the
        # per-replica in-flight map at decision time, so it remains pinned to
        # the card already serving it.
        # Windowed accepted-arrival profiles shape the deduplicated offered-
        # arrival floor without controlling its magnitude. They also survive
        # a later switch to QPS autoscaling.
        self.compatibility_profiles: list[dict[str, Any]] = []
        self.queued_compatibility_profiles: list[dict[str, Any]] = []
        self.rejected_compatibility_profiles: list[dict[str, Any]] = []
        self.configured_accelerator_shapes: dict[str, int] = {}
        self.free_reserved_slots_by_accelerator: dict[str, int] = {}
        self._compatibility_demand_complete: bool = False
        # version -> that version's per-GPU knob. A live replica's
        # capacity is a property of the spec it was launched under: after
        # an update that raises the knob (1 -> 2), sizing old-version
        # replicas with the NEW knob overstates their coverage 2x, so
        # the rolling drain would retire old replicas the kept set cannot
        # actually replace (same hazard the instance-aware autoscaler
        # guards with _qps_dict_by_version). Pruned each tick to the live
        # replica versions (+ latest).
        self._knob_by_version: dict[int, float] = {
            version: float(target_concurrency)
        }
        # See the request-rate autoscaler's matching tick-local memo. A failed
        # historical knob read is shared only within one decision tick.
        self._knob_unavailable_versions_for_tick: set[int] | None = None
        # One-shot hysteresis bypass, armed by update_version AND at
        # construction, same as the instance-aware autoscaler: the target
        # can only be recomputed on a tick (it needs replica shapes), and
        # that first recompute must apply immediately instead of being
        # gated behind the delay counters -- a rebuilt autoscaler
        # (controller restart) starts at target=min_replicas with no
        # hysteresis history worth protecting. Unlike the instance-aware
        # class the snap is consumed only once a FRESH demand report
        # exists: snapping on stale data would just re-assert the blind
        # minimum.
        self._snap_target_on_next_recompute: bool = True
        # Construction means controller restart: the first fresh report may
        # recover the demand-owned target from every surviving version. An
        # in-process version update also arms the snap above, but explicitly
        # clears this flag so its cold replacement still enters through the
        # configured rollout wave.
        self._adopt_total_capacity_on_next_recompute: bool = True
        # Per-tick freshness snapshot (see _fresh_for_tick). None outside
        # a tick.
        self._tick_fresh: bool | None = None
        # True only while an increase in the demand-derived target is waiting
        # for upscale hysteresis.  The live fleet must not be shrunk toward
        # the old target during that wait: doing so makes the autoscaler issue
        # scale-down and scale-up intents for opposite demand snapshots.
        self._upscale_pending: bool = False
        # Snapshotted before each decision tick mutates the aggregate wave
        # timestamp. Exact-card actuation uses this budget to limit cold card
        # migrations without retaining the physical supply mix in the public
        # demand target.
        self._logical_actuation_wave_budget: int | None = None
        self._logical_actuation_wave_started: bool = False
        self._logical_actuation_wave_is_new: bool = False
        self._logical_card_transition_pending: bool = False
        self._logical_actuation_target_by_accelerator: dict[str, int] = {}
        self._logical_actuation_desired_by_accelerator: dict[str, int] = {}
        self._logical_transition_retention_target_by_accelerator: dict[
            str, int] = {}
        # Explicit compatibility/floor ownership carried with the adopted
        # demand map. A later empty history can retry that exact owned card,
        # while synthesized aggregate padding remains distinguishable.
        self._logical_adopted_explicit_target_by_accelerator: dict[str,
                                                                   int] = {}
        # Paid-launch ownership is distinct from compatibility proof. An
        # aggregate minimum or headerless queued request may buy the cheapest
        # compatible card without proving that old-version work can move to
        # it during a rollout.
        self._logical_adopted_paid_target_by_accelerator: dict[str, int] = {}
        # Absolute paid capacity ceiling for the current actuation map. It is
        # derived from the separately allocated/adopted ownership map; during
        # rollout it also includes live same-card old-version backing. The
        # decision generator subtracts latest committed supply to obtain the
        # incremental launch authority.
        self._logical_paid_launch_target_by_accelerator: dict[str, int] = {}
        if (self.replica_unit == 'logical' and
                self.max_scale_up_rate_percentage is not None):
            # A cold logical service must enter through the configured slot
            # wave even when its aggregate or per-card floor is larger than
            # one wave. A rebuilt controller remains fail-closed at zero until
            # the first complete fresh report, then reconstructs live
            # committed capacity before applying this same limiter.
            self.target_num_replicas = 0
            self.target_num_replicas_by_accelerator = {}

    def install_committed_capacity_projection(
        self,
        *,
        committed_candidate: capacity_planning.CapacityPlanCandidate,
    ) -> None:
        """Project one committed candidate into disposable local state.

        PostgreSQL owns durable policy history.  This projection supports
        controller-local observability and legacy consumers after the commit;
        it is never read as an input to durable planning.
        """
        if (not isinstance(committed_candidate,
                           capacity_planning.CapacityPlanCandidate) or
                committed_candidate.next_policy_state is None):
            raise ValueError('Committed durable capacity plan is malformed.')
        next_policy_state = committed_candidate.next_policy_state
        expected_widths = capacity_planning.AcceleratorCapacity.from_mapping(
            self.configured_accelerator_shapes)
        if (next_policy_state.service_name != self._service_name or
                next_policy_state.service_version != self.latest_version or
                next_policy_state.capacity_unit
                is not capacity_planning.CapacityUnit.LOGICAL_GPU or
                next_policy_state.maximum_capacity != self.max_replicas or
                committed_candidate.capacity_unit
                is not capacity_planning.CapacityUnit.LOGICAL_GPU or
                committed_candidate.physical_gpu_width_by_accelerator
                != expected_widths or
                committed_candidate.backend_num_nodes != 1):
            raise ValueError('Committed durable capacity plan has a different '
                             'policy identity.')

        target_by_card = committed_candidate.demand_attribution.as_dict()
        explicit_by_card = (
            committed_candidate.explicit_demand_attribution.as_dict())
        paid_by_card = committed_candidate.paid_demand_attribution.as_dict()
        warm_by_card = committed_candidate.warm_retention_target.as_dict()
        cold_by_card = committed_candidate.paid_launch_target.as_dict()
        padding_by_card = (
            committed_candidate.zero_cost_padding_target.as_dict())
        desired_by_card = (
            committed_candidate.supply_aware_actuation_target.as_dict())
        wave_by_card = (
            committed_candidate.wave_limited_actuation_target.as_dict())
        transition_by_card = (
            committed_candidate.transition_retention_target.as_dict())
        upscale_pending = (committed_candidate.raw_demand_target
                           > committed_candidate.aggregate_demand_target)
        card_transition_pending = (
            committed_candidate.wave_limited_actuation_target
            != committed_candidate.supply_aware_actuation_target)
        with self._logical_state_lock:
            self.target_num_replicas = (
                committed_candidate.aggregate_demand_target)
            self._raw_target_num_replicas = (
                committed_candidate.raw_demand_target)
            self.target_num_replicas_by_accelerator = target_by_card
            self._logical_adopted_explicit_target_by_accelerator = (
                explicit_by_card)
            self._logical_adopted_paid_target_by_accelerator = paid_by_card
            self.warm_retention_target_by_accelerator = warm_by_card
            self.cold_launch_authority_by_accelerator = cold_by_card
            self._logical_paid_launch_target_by_accelerator = cold_by_card
            self.zero_cost_padding_target_by_accelerator = padding_by_card
            self._logical_actuation_desired_by_accelerator = desired_by_card
            self._logical_actuation_target_by_accelerator = wave_by_card
            self._logical_transition_retention_target_by_accelerator = (
                transition_by_card)
            self.upscale_counter = next_policy_state.upscale_observations
            # Never copy a PostgreSQL epoch into the legacy monotonic clock
            # domain. Durable planning reads this value only from PostgreSQL.
            self._downscale_started_at = None
            self._downscale_veto_streak = (
                next_policy_state.downscale_veto_streak)
            self._snap_target_on_next_recompute = (
                next_policy_state.snap_target_on_next_recompute)
            self._adopt_total_capacity_on_next_recompute = (
                next_policy_state.adopt_total_capacity_on_next_recompute)
            self._upscale_pending = upscale_pending
            self._logical_card_transition_pending = card_transition_pending
            self._pending_retention_floor = (
                next_policy_state.pending_retention_floor)
            self._pending_capacity_at_adoption = (
                next_policy_state.pending_capacity_at_adoption)
            self._pending_budget_spent = next_policy_state.pending_budget_spent
            self._reconcile_generation = committed_candidate.source_generation
            self.capacity_target_by_accelerator = dict(wave_by_card)
            self.capacity_target_complete = True
            self._last_logical_target_state = LogicalCapacityTarget(
                version=self.latest_version,
                generation=committed_candidate.source_generation,
                target_capacity=sum(wave_by_card.values()),
                target_capacity_by_accelerator=tuple(wave_by_card.items()),
                accelerator_shapes=tuple(
                    self.configured_accelerator_shapes.items()))

    def plan_durable_capacity_reconcile(
        self,
        replica_infos: Sequence['replica_managers.ReplicaInfo'],
        request_information: Mapping[str, Any],
        reservation_input: capacity_planning.ReservationPlanningInput,
        *,
        source_fingerprint: str,
        decision_inputs: ScalingDecisionInputs,
        retirement_shelter: (reserved_fill_planner.SequencedRetirementShelter |
                             None),
        max_live_paid_gpu_units: int | None,
        prior_policy_state: capacity_planning.CapacityPolicyState,
        prior_candidate: capacity_planning.CapacityPlanCandidate,
        planning_db_epoch: float,
        fresh_zero: bool = False,
        configured_reservation_accelerators: tuple[str, ...] = (),
        demand_witness_scope_sha256: str = '',
    ) -> DurableCapacityReconcilePlan | None:
        """Build and run the one durable logical planner without mutation.

        Demand comes from the PostgreSQL-locked report, reservation and paid
        inventory comes from the repository-locked projection, and replica
        readiness comes from the exact controller-prepared census.  In
        particular, this method never reconstructs economic inventory by
        filtering replica rows: cleanup-unproven retiring paid rows remain in
        ``reservation_input.existing_paid_capacity`` until the repository
        proves provider teardown.
        """
        infos = list(replica_infos)
        try:
            self._validate_scaling_decision_inputs(decision_inputs, infos)
        except (TypeError, ValueError):
            return None
        if ('*', 0) in decision_inputs.kueue_blocked_retirement_shapes:
            # Only an unbounded scheduler ambiguity freezes the whole plan.
            # Exact-shape UNKNOWN remains a committed debit for that card and
            # is independently protected from retirement below.
            return None
        if (self.replica_unit != 'logical' or
                self.adaptive_scale_up is not None or
                not isinstance(request_information, Mapping) or
                not isinstance(reservation_input,
                               capacity_planning.ReservationPlanningInput) or
                not isinstance(source_fingerprint, str) or
                not isinstance(prior_policy_state,
                               capacity_planning.CapacityPolicyState) or
                not isinstance(prior_candidate,
                               capacity_planning.CapacityPlanCandidate) or
                not isinstance(planning_db_epoch, (int, float)) or
                isinstance(planning_db_epoch, bool) or
                not math.isfinite(planning_db_epoch) or planning_db_epoch < 0 or
            (retirement_shelter is not None and
             not isinstance(retirement_shelter,
                            reserved_fill_planner.SequencedRetirementShelter))
                or (max_live_paid_gpu_units is not None and
                    (type(max_live_paid_gpu_units) is not int or
                     max_live_paid_gpu_units < 0)) or
                type(fresh_zero) is not bool):
            return None

        with self._logical_state_lock:
            generation = request_information.get('reconcile_generation')
            if (type(generation) is not int or generation < max(
                    prior_policy_state.last_reduced_demand_generation,
                    prior_candidate.source_generation) or
                    prior_policy_state.service_name != self._service_name or
                    prior_policy_state.service_version != self.latest_version or
                    prior_policy_state.capacity_unit
                    is not capacity_planning.CapacityUnit.LOGICAL_GPU or
                    prior_policy_state.maximum_capacity != self.max_replicas or
                    prior_candidate.next_policy_state != prior_policy_state or
                    prior_candidate.capacity_unit
                    is not capacity_planning.CapacityUnit.LOGICAL_GPU or
                    not prior_candidate.attribution_complete or
                    any(timestamp is not None and timestamp > planning_db_epoch
                        for timestamp in (
                            prior_policy_state.downscale_started_db_epoch,
                            prior_policy_state.paid_window_started_db_epoch))):
                return None
            configured_cards = tuple(self._configured_cards_from_profiles())
            if (not configured_cards or
                    len({card.casefold() for card in configured_cards
                        }) != len(configured_cards)):
                return None
            canonical = {card.casefold(): card for card in configured_cards}
            configured_shapes = {
                card.casefold(): width
                for card, width in self.configured_accelerator_shapes.items()
            }
            prior_shapes = {
                card.casefold(): width for card, width in
                prior_candidate.physical_gpu_width_by_accelerator.entries
            }
            if (set(canonical) != {
                    card.casefold()
                    for card in self.configured_accelerator_shapes
            } or prior_shapes != configured_shapes or
                    prior_candidate.backend_num_nodes != 1):
                return None
            retirement_shelter_target = (
                capacity_planning.AcceleratorCapacity())
            if retirement_shelter is not None:
                # A typed zero shelter with no allocation identity is the
                # characteristic product of missing sequenced evidence.  It is
                # never equivalent to the adapter's explicit no-fill ``None``.
                if (retirement_shelter.service_version != self.latest_version or
                    (retirement_shelter.target_capacity == 0 and
                     not retirement_shelter.authority_current)):
                    return None
                shelter_shapes = dict(retirement_shelter.accelerator_shapes)
                target_by_card = dict(
                    retirement_shelter.target_capacity_by_accelerator)
                if (set(shelter_shapes) - set(canonical) or
                        set(target_by_card) - set(canonical) or
                        any(self.configured_accelerator_shapes[canonical[card]]
                            != width
                            for card, width in shelter_shapes.items())):
                    return None
                retirement_shelter_target = (
                    capacity_planning.AcceleratorCapacity.from_mapping({
                        canonical[card]: count
                        for card, count in target_by_card.items()
                    }))

            def _canonical_cards(raw_cards: object) -> tuple[str, ...] | None:
                if (not isinstance(raw_cards, (list, tuple)) or not raw_cards):
                    return None
                result: list[str] = []
                seen: set[str] = set()
                for raw_card in raw_cards:
                    if not isinstance(raw_card, str):
                        return None
                    card = canonical.get(raw_card.casefold())
                    if card is None or card.casefold() in seen:
                        return None
                    seen.add(card.casefold())
                    result.append(card)
                return tuple(result)

            raw_in_flight = request_information.get('in_flight_by_replica_id')
            if not isinstance(raw_in_flight, Mapping):
                return None
            in_flight: dict[int, int] = {}
            for raw_replica_id, raw_count in raw_in_flight.items():
                try:
                    replica_id = int(raw_replica_id)
                except (TypeError, ValueError):
                    return None
                if (type(raw_count) is not int or raw_count < 0 or
                        replica_id in in_flight):
                    return None
                in_flight[replica_id] = raw_count

            def _nonnegative_count(field: str,
                                   *,
                                   optional: bool = False) -> int | None:
                value = request_information.get(field)
                if value is None and optional:
                    return None
                if type(value) is not int or value < 0:
                    return None
                return value

            def _priority_counts(field: str) -> dict[int, int] | None:
                value = request_information.get(field)
                if not isinstance(value, Mapping):
                    return None
                result: dict[int, int] = {}
                for raw_priority, count in value.items():
                    try:
                        priority = int(raw_priority)
                    except (TypeError, ValueError):
                        return None
                    if (not 0 <= priority <= 100 or type(count) is not int or
                            count < 0 or priority in result):
                        return None
                    result[priority] = count
                return result

            queue_depth = _nonnegative_count('queue_depth')
            rejected_count = _nonnegative_count('rejected_in_window')
            if queue_depth is None or rejected_count is None:
                return None
            recent_rejected = _nonnegative_count('rejected_in_recent_window',
                                                 optional=True)
            queue_by_priority = _priority_counts('queue_depth_by_priority')

            raw_timestamps = request_information.get('timestamps', ())
            if not isinstance(raw_timestamps, (list, tuple)):
                return None
            timestamps: list[float] = []
            cutoff = planning_db_epoch - self.qps_window_size
            for timestamp in raw_timestamps:
                if (not isinstance(timestamp, (int, float)) or
                        isinstance(timestamp, bool) or
                        not math.isfinite(timestamp)):
                    return None
                if float(timestamp) >= cutoff:
                    timestamps.append(float(timestamp))

            raw_unknown_in_flight = request_information.get(
                'unknown_in_flight_replica_ids', ()) or ()
            raw_unknown_capacity = request_information.get(
                'unknown_capacity_replica_ids', ()) or ()
            if (not isinstance(raw_unknown_in_flight, (list, tuple, set)) or
                    not isinstance(raw_unknown_capacity, (list, tuple, set))):
                return None
            try:
                unknown_in_flight = {
                    int(value) for value in raw_unknown_in_flight
                }
                unknown_capacity = {
                    int(value) for value in raw_unknown_capacity
                }
            except (TypeError, ValueError):
                return None
            raw_observed = request_information.get(
                'observed_slots_by_replica_id', {})
            if not isinstance(raw_observed, Mapping):
                return None
            observed_slots: dict[int, int] = {}
            for raw_replica_id, raw_slots in raw_observed.items():
                try:
                    replica_id = int(raw_replica_id)
                except (TypeError, ValueError):
                    return None
                if (type(raw_slots) is not int or raw_slots < 0 or
                        replica_id in observed_slots):
                    return None
                observed_slots[replica_id] = raw_slots

            def _parse_profiles(
                field: str,
                *,
                arrivals: bool = False,
                rejected: bool = False,
            ) -> list[dict[str, Any]] | None:
                raw = request_information.get(field, [])
                if not isinstance(raw, list):
                    return None
                if arrivals:
                    parsed = self._parse_compatibility_arrivals(raw)
                else:
                    parsed = self._parse_compatibility_gauge(
                        raw, include_recent_count=rejected)
                if len(parsed) != len(raw):
                    return None
                normalized: list[dict[str, Any]] = []
                for profile in parsed:
                    cards = _canonical_cards(profile['compatible_accelerators'])
                    if cards is None:
                        return None
                    item = dict(profile)
                    item['compatible_accelerators'] = cards
                    normalized.append(item)
                return normalized

            arrivals = _parse_profiles('compatibility_profiles', arrivals=True)
            queued_profiles = _parse_profiles(
                'queued_requests_by_compatibility')
            rejected_profiles = _parse_profiles(
                'rejected_requests_by_compatibility', rejected=True)
            if (arrivals is None or queued_profiles is None or
                    rejected_profiles is None or
                    request_information.get('compatibility_demand_complete')
                    is not True):
                return None
            arrivals = [
                profile for profile in arrivals
                if profile['timestamp'] >= planning_db_epoch -
                constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS
            ]

            def _adaptive_sample_fresh(observed_at: float | None) -> bool:
                if observed_at is None:
                    return False
                age = planning_db_epoch - observed_at
                return (-60.0 <= age <=
                        constants.AUTOSCALER_ADAPTIVE_SAMPLE_MAX_AGE_SECONDS)

            duration = self.expected_request_duration_seconds
            if (self.adaptive_demand_estimation and
                    self._measured_duration_seconds is not None and
                    self._measured_duration_samples
                    >= constants.AUTOSCALER_ADAPTIVE_DURATION_MIN_SAMPLES and
                    _adaptive_sample_fresh(self._measured_duration_at)):
                duration = self._measured_duration_seconds
            configured_lead = self.initial_provision_lead_time_seconds
            if (not isinstance(configured_lead, (int, float)) or
                    isinstance(configured_lead, bool)):
                configured_lead = (
                    constants.AUTOSCALER_DEFAULT_PROVISION_LEAD_SECONDS)
            lead = float(configured_lead)
            if (self.adaptive_demand_estimation and
                    len(self._provision_lead_samples)
                    >= constants.AUTOSCALER_ADAPTIVE_LEAD_MIN_SAMPLES and
                    _adaptive_sample_fresh(self._provision_lead_at)):
                ordered_leads = sorted(self._provision_lead_samples)
                lead_index = min(
                    len(ordered_leads) - 1,
                    int(constants.AUTOSCALER_ADAPTIVE_LEAD_QUANTILE *
                        len(ordered_leads)))
                lead = float(ordered_leads[lead_index])
            effective_capacity = (self.target_concurrency_per_replica *
                                  self.target_utilization_percentage / 100.0)
            if effective_capacity <= 0:
                return None

            def _priority_timeout(priority: int) -> float | None:
                timeout = self._queue_timeout_seconds
                for minimum_priority, threshold in (
                        self._queue_timeout_thresholds):
                    if priority < minimum_priority:
                        break
                    timeout = threshold
                return timeout

            weighted_queue_available = (
                duration is not None and
                self._queue_timeout_seconds is not None and
                queue_by_priority is not None and
                sum(queue_by_priority.values()) >= queue_depth)

            def _queued_work_per_request(priority: int) -> float:
                if not weighted_queue_available:
                    return 1.0
                assert duration is not None
                timeout = _priority_timeout(priority)
                if timeout is None:
                    return 1.0
                return min(1.0, duration / max(duration, timeout - lead))

            queue_work = float(queue_depth)
            if weighted_queue_available:
                assert queue_by_priority is not None
                queue_work = sum(
                    count * _queued_work_per_request(priority)
                    for priority, count in queue_by_priority.items())
            rejected_work = float(rejected_count)
            if duration is not None:
                retained_rejected = (rejected_count * duration /
                                     constants.LB_REJECT_WINDOW_SECONDS)
                recent_rejected_work = (retained_rejected if recent_rejected
                                        is None else recent_rejected *
                                        duration / self.qps_window_size)
                rejected_work = max(retained_rejected, recent_rejected_work)

            optional_arrival_fields = ('unique_job_arrivals_60s',
                                       'unique_job_arrivals_300s',
                                       'headerless_arrivals_60s',
                                       'headerless_arrivals_300s')
            arrival_counts = {
                field: _nonnegative_count(field, optional=True)
                for field in optional_arrival_fields
            }
            offered_complete = all(
                value is not None for value in arrival_counts.values())
            saturated = request_information.get(
                'offered_arrival_tracking_saturated') is True

            shape_handles = decision_inputs.gpu_shape_handles
            if shape_handles is None:
                return None

            def _shape(
                info: 'replica_managers.ReplicaInfo',
            ) -> tuple[str, int] | None:
                raw_shape = decision_inputs.gpu_shapes_by_replica_id.get(
                    info.replica_id)
                if raw_shape is None:
                    raw_shape = self._gpu_shape_from_resources_override(info)
                if raw_shape is None:
                    handle = shape_handles.get(info.replica_id)
                    if handle is None:
                        return None
                    try:
                        accelerators = handle.launched_resources.accelerators
                        if not accelerators:
                            return None
                        raw_card = next(iter(accelerators))
                        raw_shape = (str(raw_card),
                                     max(1, int(accelerators[raw_card])))
                    except (AttributeError, TypeError, ValueError):
                        return None
                if (not isinstance(raw_shape, tuple) or len(raw_shape) != 2 or
                        not isinstance(raw_shape[0], str) or not raw_shape[0]):
                    return None
                try:
                    width = int(raw_shape[1])
                except (TypeError, ValueError):
                    return None
                if width <= 0:
                    return None
                card = canonical.get(raw_shape[0].casefold())
                if card is None:
                    return None
                return card, width

            kueue_classes = decision_inputs.kueue_capacity_by_replica_id
            degraded_since = dict(self._degraded_capacity_since_by_replica_id)
            infos_by_id = {info.replica_id: info for info in infos}
            if len(infos_by_id) != len(infos):
                return None

            def _kueue_assigned(info: 'replica_managers.ReplicaInfo',) -> bool:
                capacity_class = kueue_classes.get(info.replica_id)
                return capacity_class is not (
                    kueue_lane_capacity.KueueReplicaCapacityClass.FRESH_WAITING)

            def _committed(info: 'replica_managers.ReplicaInfo',) -> int:
                if (info.is_terminal or _replica_is_retiring_card_supply(info)):
                    return 0
                assigned = _kueue_assigned(info)
                if not assigned:
                    return 0
                planned = max(0, int(info.planned_capacity))
                observed = observed_slots.get(info.replica_id)
                degraded = (info.replica_id in unknown_capacity or
                            (info.is_ready and observed == 0))
                if degraded:
                    since = degraded_since.get(info.replica_id,
                                               planning_db_epoch)
                    if (planning_db_epoch - since >= constants.
                            LOGICAL_UNKNOWN_CAPACITY_REPLACEMENT_SECONDS and
                            info.unknown_capacity_replacement is not True):
                        return 0
                    return planned
                if info.is_ready and observed is not None:
                    return min(planned, observed)
                return planned

            def _ready(info: 'replica_managers.ReplicaInfo') -> int:
                if not info.is_ready or info.replica_id in unknown_capacity:
                    return 0
                observed = observed_slots.get(info.replica_id)
                if observed is None:
                    return 0
                return min(max(0, int(info.planned_capacity)), observed)

            ready: dict[str, int] = {}
            ready_zero_cost: dict[str, int] = {}
            provisioning: dict[str, int] = {}
            latest_committed: dict[str, int] = {}
            old_committed: dict[str, int] = {}
            committed_by_id: dict[int, int] = {}
            card_by_id: dict[int, str] = {}
            latest_capacity = 0
            nonterminal_capacity = 0
            ready_demand_owned_capacity = 0
            provisioning_demand_owned_capacity = 0
            provisioning_statuses = {
                serve_state.ReplicaStatus.PENDING,
                serve_state.ReplicaStatus.PROVISIONING,
                serve_state.ReplicaStatus.STARTING,
            }

            for info in infos:
                if (info.is_terminal or _replica_is_retiring_card_supply(info)):
                    continue
                shape = _shape(info)
                committed = _committed(info)
                if shape is None:
                    return None
                card, _ = shape
                card_by_id[info.replica_id] = card
                committed_by_id[info.replica_id] = committed
                if committed <= 0:
                    continue
                nonterminal_capacity += committed
                destination = (latest_committed if info.version
                               == self.latest_version else old_committed)
                destination[card] = destination.get(card, 0) + committed
                if info.version == self.latest_version:
                    latest_capacity += committed
                if info.is_ready:
                    ready[card] = ready.get(card, 0) + committed
                    if info.is_zero_cost is True:
                        ready_zero_cost[card] = (ready_zero_cost.get(card, 0) +
                                                 committed)
                    if not info.reserved_fill:
                        ready_demand_owned_capacity += committed
                else:
                    provisioning[card] = (provisioning.get(card, 0) + committed)
                if (info.version == self.latest_version and
                        info.status in provisioning_statuses and
                        not info.reserved_fill):
                    provisioning_demand_owned_capacity += committed

            retention_fixed: dict[str, float] = {}
            flexible_fixed = 0.0
            has_unattributed_fixed_work = False

            def _add_fixed(replica_id: int, work: float,
                           destination: dict[str, float]) -> bool:
                nonlocal flexible_fixed
                info = infos_by_id.get(replica_id)
                if info is None:
                    return False
                if _replica_is_retiring_card_supply(info):
                    flexible_fixed += max(0.0, work)
                    return True
                card = card_by_id.get(replica_id)
                if card is None:
                    return False
                destination[card] = (destination.get(card, 0.0) +
                                     max(0.0, work))
                return True

            for replica_id, count in in_flight.items():
                if saturated and (replica_id in unknown_in_flight or
                                  replica_id in unknown_capacity):
                    # The replica reported work but not a trustworthy current
                    # in-flight classification.  Saturation prevents a partial
                    # arrival sample from supplying the missing card proof.
                    if replica_id not in infos_by_id:
                        return None
                    has_unattributed_fixed_work = (has_unattributed_fixed_work
                                                   or count > 0)
                    continue
                if not _add_fixed(replica_id, float(count), retention_fixed):
                    return None
            original_unknown: dict[str, float] = {}
            replacement_unknown: dict[str, float] = {}
            for replica_id in unknown_in_flight:
                unknown_info = infos_by_id.get(replica_id)
                if unknown_info is None:
                    return None
                unknown_work = (max(0, int(unknown_info.planned_capacity)) *
                                effective_capacity)
                unknown_destination: dict[str, float] = (
                    replacement_unknown
                    if unknown_info.unknown_capacity_replacement is True else
                    original_unknown)
                if not _add_fixed(replica_id, unknown_work,
                                  unknown_destination):
                    return None
            unknown_fixed = (replacement_unknown if sum(
                replacement_unknown.values()) > sum(original_unknown.values())
                             else original_unknown)
            if saturated:
                # Unknown-capacity work has no current exact-card service
                # proof. It may be sheltered by committed supply below, but
                # must not enter economic demand or provider authority.
                has_unattributed_fixed_work = (has_unattributed_fixed_work or
                                               sum(unknown_fixed.values())
                                               > _SLOT_CONVERSION_EPSILON)
            else:
                for card, work in unknown_fixed.items():
                    retention_fixed[card] = (retention_fixed.get(card, 0.0) +
                                             work)

            materialized_work = {card: 0.0 for card in configured_cards}
            for info in infos:
                if (info.is_terminal or
                        _replica_is_retiring_card_supply(info) or info.status
                        not in (serve_state.ReplicaStatus.READY,
                                serve_state.ReplicaStatus.NOT_READY)):
                    continue
                materialized_card = card_by_id.get(info.replica_id)
                if materialized_card is not None:
                    materialized_work[materialized_card] += (
                        max(0, int(info.planned_capacity)) * effective_capacity)
            capped_retention: dict[str, float] = {}
            for card, work in retention_fixed.items():
                retained = min(work, materialized_work.get(card, 0.0))
                if retained > 0:
                    capped_retention[card] = retained
                flexible_fixed += max(0.0, work - retained)

            work_profiles: list[tuple[int, tuple[str, ...], float]] = []
            explicit_profiles: list[tuple[int, tuple[str, ...], float]] = []
            paid_profiles: list[tuple[int, tuple[str, ...], float]] = []
            default_compatible = tuple(configured_cards)

            queue_entries: list[tuple[int, tuple[str, ...], float, bool]] = [
                (int(profile['priority']),
                 tuple(profile['compatible_accelerators']),
                 float(profile['count']) *
                 _queued_work_per_request(int(profile['priority'])), True)
                for profile in queued_profiles
            ]
            if weighted_queue_available:
                assert queue_by_priority is not None
                profiled_by_priority: dict[int, int] = {}
                for profile in queued_profiles:
                    priority = int(profile['priority'])
                    profiled_by_priority[priority] = (
                        profiled_by_priority.get(priority, 0) +
                        int(profile['count']))
                for priority, count in queue_by_priority.items():
                    missing = max(0,
                                  count - profiled_by_priority.get(priority, 0))
                    if missing:
                        queue_entries.append(
                            (priority, default_compatible,
                             missing * _queued_work_per_request(priority),
                             False))
            else:
                represented = sum(
                    int(profile['count']) for profile in queued_profiles)
                if queue_depth > represented:
                    queue_entries.append(
                        (constants.LB_REQUEST_PRIORITY_MIN, default_compatible,
                         float(queue_depth - represented), False))
            represented_queue_work = sum(item[2] for item in queue_entries)
            if represented_queue_work > queue_work + _SLOT_CONVERSION_EPSILON:
                bounded: list[tuple[int, tuple[str, ...], float, bool]] = []
                remaining = queue_work
                for priority in sorted({item[0] for item in queue_entries},
                                       reverse=True):
                    group = [
                        item for item in queue_entries if item[0] == priority
                    ]
                    group_work = sum(item[2] for item in group)
                    accepted = min(remaining, group_work)
                    if accepted <= _SLOT_CONVERSION_EPSILON:
                        break
                    scale = accepted / group_work
                    bounded.extend(
                        (item_priority, compatible, work * scale, is_explicit)
                        for item_priority, compatible, work, is_explicit in
                        group)
                    remaining -= accepted
                queue_entries = bounded
            elif represented_queue_work < queue_work - _SLOT_CONVERSION_EPSILON:
                queue_entries.append(
                    (constants.LB_REQUEST_PRIORITY_MIN, default_compatible,
                     queue_work - represented_queue_work, False))
            queue_public = [(priority, compatible, work)
                            for priority, compatible, work, _ in queue_entries
                            if work > 0]
            queue_explicit = [
                (priority, compatible, work)
                for priority, compatible, work, is_explicit in queue_entries
                if is_explicit and work > 0
            ]
            work_profiles.extend(queue_public)
            explicit_profiles.extend(queue_explicit)
            paid_profiles.extend(queue_public)

            raw_rejected_work: list[tuple[int, tuple[str, ...], float]] = []
            for profile in rejected_profiles:
                count = int(profile['count'])
                profile_work = float(count)
                if duration is not None:
                    retained = count * duration / constants.LB_REJECT_WINDOW_SECONDS
                    recent = (int(profile['recent_count']) * duration /
                              self.qps_window_size)
                    profile_work = max(retained, recent)
                raw_rejected_work.append(
                    (int(profile['priority']),
                     tuple(profile['compatible_accelerators']), profile_work))
            represented_rejected = sum(item[2] for item in raw_rejected_work)
            if represented_rejected > 0:
                rejected_scale = min(1.0, rejected_work / represented_rejected)
                normalized_rejected = [
                    (priority, compatible, work * rejected_scale)
                    for priority, compatible, work in raw_rejected_work
                ]
            else:
                normalized_rejected = []
            work_profiles.extend(normalized_rejected)
            explicit_profiles.extend(normalized_rejected)
            paid_profiles.extend(normalized_rejected)
            represented_rejected = sum(item[2] for item in normalized_rejected)
            if rejected_work > represented_rejected + _SLOT_CONVERSION_EPSILON:
                fallback_rejected = (constants.LB_REQUEST_PRIORITY_MIN,
                                     default_compatible,
                                     rejected_work - represented_rejected)
                work_profiles.append(fallback_rejected)
                paid_profiles.append(fallback_rejected)

            exact_fixed_work = capped_retention if saturated else {}
            if not saturated:
                fixed_work = sum(capped_retention.values()) + flexible_fixed
                fixed_evidence = [(int(profile['priority']),
                                   tuple(profile['compatible_accelerators']),
                                   float(profile['count']))
                                  for profile in arrivals
                                  if float(profile['count']) > 0]
                fixed_evidence_total = sum(item[2] for item in fixed_evidence)
                if fixed_work > 0 and fixed_evidence_total > 0:
                    fixed_scale = fixed_work / fixed_evidence_total
                    shaped_fixed = [
                        (priority, compatible, work * fixed_scale)
                        for priority, compatible, work in fixed_evidence
                    ]
                    work_profiles.extend(shaped_fixed)
                    explicit_profiles.extend(shaped_fixed)
                    paid_profiles.extend(shaped_fixed)
                elif fixed_work > 0:
                    work_profiles.append((constants.LB_REQUEST_PRIORITY_MIN,
                                          default_compatible, fixed_work))

            def _offered(window: int) -> int:
                if saturated:
                    return constants.LB_OFFERED_ARRIVAL_CAP
                suffix = '60s' if window == 60 else '300s'
                unique = arrival_counts[f'unique_job_arrivals_{suffix}']
                headerless = arrival_counts[f'headerless_arrivals_{suffix}']
                return int(unique or 0) + int(headerless or 0)

            arrival_work = 0.0
            arrival_evidence_window = self.qps_window_size
            if duration is not None:
                if offered_complete:
                    recent_work = (_offered(60) * duration /
                                   constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
                    retained_work = (
                        1.15 * _offered(300) * duration /
                        constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS)
                    arrival_work = max(recent_work, retained_work)
                    if retained_work > recent_work:
                        arrival_evidence_window = (
                            constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS)
                else:
                    arrival_work = (len(timestamps) * duration /
                                    self.qps_window_size)
            # Retention is already represented either by shaped profiles or
            # saturated exact-card fixed work. Adding it again would suppress
            # valid arrival-floor demand.
            attributed_work = (sum(item[2] for item in work_profiles) +
                               sum(exact_fixed_work.values()))
            arrival_gap = max(0.0, arrival_work - attributed_work)
            unattributed_saturated_work = False
            saturated_shelter_cards: set[str] | None = set()
            if saturated:
                # Queue, rejection, in-flight, and arrival telemetry are
                # projections of one request stream. Reduce them to one
                # typed reconciliation before capacity allocation. Classes
                # disjoint from lossy fixed work remain incremental; classes
                # that intersect it and the unclassified aggregate saturation
                # gap remain shelter-only.
                if (work_profiles != explicit_profiles or
                        work_profiles != paid_profiles):
                    return None
                primary = tuple(
                    capacity_planning.CompatibilityDemand(
                        sequence=sequence,
                        priority=priority,
                        compatible_accelerators=compatible,
                        work=work)
                    for sequence, (priority, compatible,
                                   work) in enumerate(work_profiles)
                    if work > _SLOT_CONVERSION_EPSILON)
                measured_arrivals: tuple[capacity_planning.CompatibilityDemand,
                                         ...] = ()
                if duration is not None:
                    measured_arrivals = tuple(
                        capacity_planning.CompatibilityDemand(
                            sequence=sequence,
                            priority=int(profile['priority']),
                            compatible_accelerators=tuple(
                                profile['compatible_accelerators']),
                            work=(float(profile['count']) * duration /
                                  arrival_evidence_window))
                        for sequence, profile in enumerate(arrivals)
                        if profile['timestamp'] >= planning_db_epoch -
                        arrival_evidence_window and float(profile['count']) > 0)
                observation_reconciliation = (
                    capacity_planning.reconcile_demand_observations(
                        primary_profiles=primary,
                        fixed_work=(capacity_planning.AcceleratorWork.
                                    from_mapping(exact_fixed_work)),
                        arrival_profiles=measured_arrivals))
                work_profiles = [(profile.priority,
                                  profile.compatible_accelerators, profile.work)
                                 for profile in
                                 observation_reconciliation.reconciled_profiles]
                explicit_profiles = list(work_profiles)
                paid_profiles = list(work_profiles)
                attributed_work = (sum(item[2] for item in work_profiles) +
                                   sum(exact_fixed_work.values()))
                arrival_gap = max(0.0, arrival_work - attributed_work)
                unclassified_arrival_gap = max(
                    0.0, arrival_gap -
                    observation_reconciliation.ambiguous_fixed_arrival_work)
                global_saturated_uncertainty = (
                    duration is None or
                    unclassified_arrival_gap > _SLOT_CONVERSION_EPSILON or
                    flexible_fixed > _SLOT_CONVERSION_EPSILON or
                    has_unattributed_fixed_work)
                fixed_overlap_uncertainty = (
                    observation_reconciliation.ambiguous_fixed_arrival_work
                    > _SLOT_CONVERSION_EPSILON)
                unattributed_saturated_work = (global_saturated_uncertainty or
                                               fixed_overlap_uncertainty)
                saturated_shelter_cards = (
                    None if global_saturated_uncertainty else set(
                        observation_reconciliation.
                        ambiguous_fixed_shelter_accelerators))
                if attributed_work <= _SLOT_CONVERSION_EPSILON:
                    return None
                outstanding_work = attributed_work
            else:
                if arrival_gap > _SLOT_CONVERSION_EPSILON:
                    arrival_evidence = [
                        (int(profile['priority']),
                         tuple(profile['compatible_accelerators']),
                         float(profile['count']))
                        for profile in arrivals
                        if profile['timestamp'] >= planning_db_epoch -
                        arrival_evidence_window and float(profile['count']) > 0
                    ]
                    arrival_evidence.extend(
                        (int(profile['priority']),
                         tuple(profile['compatible_accelerators']),
                         float(profile['count']))
                        for profile in queued_profiles
                        if float(profile['count']) > 0)
                    evidence_total = sum(item[2] for item in arrival_evidence)
                    if evidence_total <= 0:
                        return None
                    scale = arrival_gap / evidence_total
                    shaped_arrival = [
                        (priority, compatible, work * scale)
                        for priority, compatible, work in arrival_evidence
                    ]
                    work_profiles.extend(shaped_arrival)
                    explicit_profiles.extend(shaped_arrival)
                    paid_profiles.extend(shaped_arrival)
                outstanding_work = (sum(in_flight.values()) + queue_work +
                                    rejected_work + sum(unknown_fixed.values()))
            target_work = (outstanding_work if saturated else max(
                outstanding_work, arrival_work))
            raw_target = math.ceil(target_work / effective_capacity -
                                   _SLOT_CONVERSION_EPSILON)
            minimum_capacity = min(
                self.max_replicas,
                max(self.min_replicas, 0 if fresh_zero else raw_target))

            deadline_input = None
            raw_deadlines = request_information.get(
                'queued_request_deadline_buckets')
            parsed_deadlines = self._parse_deadline_gauge(raw_deadlines)
            if (duration is not None and isinstance(raw_deadlines, list) and
                    len(parsed_deadlines) == len(raw_deadlines) and
                    queue_by_priority is not None):
                normalized_deadlines: list[dict[str, Any]] = []
                deadline_counts: dict[int, int] = {}
                for profile in parsed_deadlines:
                    cards = _canonical_cards(profile['compatible_accelerators'])
                    if cards is None:
                        return None
                    item = dict(profile)
                    item['compatible_accelerators'] = cards
                    normalized_deadlines.append(item)
                    priority = int(item['priority'])
                    deadline_counts[priority] = (
                        deadline_counts.get(priority, 0) + int(item['count']))
                if (sum(deadline_counts.values()) == queue_depth and
                        deadline_counts == queue_by_priority):
                    service_seconds = {
                        card: float(duration) for card in configured_cards
                    }
                    sources = {
                        card: 'aggregate_or_configured_seed'
                        for card in configured_cards
                    }
                    for raw_card, estimate in (
                            decision_inputs.
                            service_time_estimates_by_accelerator.items()):
                        estimated_card = canonical.get(raw_card.casefold())
                        if estimated_card is None:
                            continue
                        observed_at = float(estimate['observed_at'])
                        if (-60.0 <= planning_db_epoch - observed_at <=
                                constants.
                                AUTOSCALER_ADAPTIVE_SAMPLE_MAX_AGE_SECONDS):
                            service_seconds[estimated_card] = float(
                                estimate['duration_seconds'])
                            sources[
                                estimated_card] = 'postgresql_async_ledger_p75'
                    finite_supply: list[DeadlineSupply] = []
                    for info in infos:
                        committed = committed_by_id.get(info.replica_id, 0)
                        supply_card = card_by_id.get(info.replica_id)
                        if committed <= 0 or supply_card is None:
                            continue
                        if info.is_ready:
                            if info.replica_id in unknown_in_flight:
                                continue
                            running = in_flight.get(info.replica_id, 0)
                            base, extra = divmod(running, committed)
                            tier = 0 if info.is_zero_cost is True else 3
                            for index in range(committed):
                                jobs = base + (1 if index < extra else 0)
                                finite_supply.append(
                                    DeadlineSupply(
                                        card=supply_card,
                                        available_after_seconds=(
                                            jobs *
                                            service_seconds[supply_card] /
                                            (self.target_utilization_percentage
                                             / 100.0)),
                                        tier=tier))
                        else:
                            created_at = info.created_at
                            age = (max(0.0, planning_db_epoch -
                                       float(created_at))
                                   if isinstance(created_at, (int, float)) and
                                   not isinstance(created_at, bool) else 0.0)
                            available = max(0.0, lead - age)
                            tier = 1 if info.is_zero_cost is True else 4
                            finite_supply.extend(
                                DeadlineSupply(
                                    card=supply_card,
                                    available_after_seconds=available,
                                    tier=tier) for _ in range(committed))
                    free_reservation = {
                        card: (reservation_input.pending_zero_cost_capacity.get(
                            card, 0) +
                               reservation_input.eligible_capacity.get(card, 0)
                              ) for card in configured_cards
                    }
                    for card, count in free_reservation.items():
                        finite_supply.extend(
                            DeadlineSupply(
                                card=card, available_after_seconds=lead, tier=2)
                            for _ in range(max(0, count)))
                    deadline_input = capacity_planning.DeadlinePlanningInput(
                        demand=tuple(
                            DeadlineDemand(
                                sequence=sequence,
                                priority=int(profile['priority']),
                                compatible_cards=tuple(
                                    profile['compatible_accelerators']),
                                count=int(profile['count']),
                                remaining_seconds=float(
                                    profile['remaining_seconds'])) for sequence,
                            profile in enumerate(normalized_deadlines)),
                        finite_supply=tuple(finite_supply),
                        service_seconds_by_accelerator=(
                            capacity_planning.AcceleratorWork.from_mapping(
                                service_seconds)),
                        service_time_sources=tuple(sources.items()),
                        utilization=(self.target_utilization_percentage /
                                     100.0),
                        paid_cold_lead_seconds=lead)

            pressure_reasons: list[str] = []
            if request_information.get(
                    'pressure_report_is_floored') is not True:
                if queue_depth > 0:
                    pressure_reasons.append('queue_depth')
                if (recent_rejected or 0) > 0:
                    pressure_reasons.append('recent_rejections')
                if _offered(60) > 0:
                    pressure_reasons.append('offered_arrivals_60s')
            policy_input = capacity_planning.CapacityPolicyInput(
                planning_db_epoch=planning_db_epoch,
                fresh_demand=True,
                pressure_latched=bool(pressure_reasons),
                pressure_reasons=tuple(pressure_reasons),
                ready_demand_owned_capacity=ready_demand_owned_capacity,
                latest_committed_capacity=latest_capacity,
                nonterminal_committed_capacity=nonterminal_capacity,
                provisioning_demand_owned_capacity=(
                    provisioning_demand_owned_capacity),
                latest_committed_by_accelerator=(
                    capacity_planning.AcceleratorCapacity.from_mapping(
                        latest_committed)),
                upscale_delay_observations=max(1, self.scale_up_threshold),
                downscale_delay_seconds=float(self.downscale_delay_seconds),
                decision_interval_seconds=float(
                    constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS),
                max_downscale_pressure_vetoes=(
                    _MAX_CONSECUTIVE_DOWNSCALE_VETOES),
                scale_up_rate_percentage=self.max_scale_up_rate_percentage,
                scale_up_rate_min_capacity=(self.scale_up_rate_min_replicas or
                                            0),
                scale_up_rate_period_seconds=(
                    self.scale_up_rate_period_seconds),
                max_scale_down_rate_percentage=(
                    self.max_scale_down_rate_percentage),
                overprovision_capacity=max(0, int(self.num_overprovision or 0)))

            def _typed_profiles(
                profiles: list[tuple[int, tuple[str, ...], float]],
            ) -> tuple[capacity_planning.CompatibilityDemand, ...]:
                return tuple(
                    capacity_planning.CompatibilityDemand(
                        sequence=sequence,
                        priority=priority,
                        compatible_accelerators=compatible,
                        work=work)
                    for sequence, (priority, compatible,
                                   work) in enumerate(profiles)
                    if work > 0)

            if unattributed_saturated_work and not fresh_zero:
                shelter = {
                    card: count
                    for card, count in retirement_shelter_target.entries
                    if count > 0
                }
                cards_to_shelter = (configured_cards if saturated_shelter_cards
                                    is None else tuple(
                                        card for card in configured_cards
                                        if card in saturated_shelter_cards))
                for card in cards_to_shelter:
                    committed = (latest_committed.get(card, 0) +
                                 old_committed.get(card, 0))
                    if committed > 0:
                        shelter[card] = max(shelter.get(card, 0), committed)
                if sum(shelter.values()) > self.max_replicas:
                    return None
                retirement_shelter_target = (
                    capacity_planning.AcceleratorCapacity.from_mapping(shelter))
                if retirement_shelter_target.total() > 0:
                    sheltered_cards = retirement_shelter_target.as_dict()
                    retirement_shelter = (
                        reserved_fill_planner.SequencedRetirementShelter(
                            service_version=self.latest_version,
                            target_capacity=(retirement_shelter_target.total()),
                            target_capacity_by_accelerator=tuple(
                                sheltered_cards.items()),
                            accelerator_shapes=tuple(
                                (card, self.configured_accelerator_shapes[card])
                                for card in sheltered_cards),
                            allocation_identity=None))
            if fresh_zero:
                minimum_capacity = 0
                work_profiles = []
                explicit_profiles = []
                paid_profiles = []
                capped_retention = {}
                exact_fixed_work = {}
            planning_prior_state = prior_policy_state
            planning_prior_candidate = prior_candidate
            if unattributed_saturated_work:
                (planning_prior_state, planning_prior_candidate) = (
                    _without_ambiguous_prior_authority(prior_policy_state,
                                                       prior_candidate))
            cold_order = (decision_inputs.cold_paid_accelerator_order or
                          configured_cards)
            # An exact empty tuple is a closed catalog result (including
            # provider failure or on-demand-only candidates), not permission
            # to reopen every configured accelerator.
            prospective_order = (
                decision_inputs.prospective_paid_accelerator_order)
            snapshot = capacity_planning.CapacityPlanningSnapshot(
                source_generation=generation,
                service_version=self.latest_version,
                configured_accelerators=configured_cards,
                capacity_unit=capacity_planning.CapacityUnit.LOGICAL_GPU,
                backend_num_nodes=1,
                physical_gpu_width_by_accelerator=(
                    capacity_planning.AcceleratorCapacity.from_mapping(
                        self.configured_accelerator_shapes)),
                capacity_per_accelerator=(
                    capacity_planning.AcceleratorWork.from_mapping({
                        card: effective_capacity for card in configured_cards
                    })),
                floors=capacity_planning.AcceleratorCapacity.from_mapping({
                    canonical[card.casefold()]: int(floor)
                    for card, floor in self.min_replicas_by_accelerator.items()
                    if card.casefold() in canonical
                }),
                minimum_capacity=minimum_capacity,
                paid_minimum_capacity=(0 if fresh_zero else min(
                    self.min_replicas, minimum_capacity)),
                actuation_minimum_capacity=minimum_capacity,
                maximum_capacity=self.max_replicas,
                demand_profiles=_typed_profiles(work_profiles),
                explicit_demand_profiles=_typed_profiles(explicit_profiles),
                paid_demand_profiles=_typed_profiles(paid_profiles),
                fixed_work=(capacity_planning.AcceleratorWork.from_mapping(
                    exact_fixed_work)),
                explicit_fixed_work=(capacity_planning.AcceleratorWork.
                                     from_mapping(exact_fixed_work)),
                paid_fixed_work=(capacity_planning.AcceleratorWork.from_mapping(
                    exact_fixed_work)),
                retention_work=(capacity_planning.AcceleratorWork.from_mapping(
                    capped_retention)),
                ready_zero_cost=(capacity_planning.AcceleratorCapacity.
                                 from_mapping(ready_zero_cost)),
                ready=capacity_planning.AcceleratorCapacity.from_mapping(ready),
                provisioning=(capacity_planning.AcceleratorCapacity.
                              from_mapping(provisioning)),
                reservation=reservation_input,
                cold_accelerator_order=tuple(cold_order),
                prospective_paid_accelerator_order=tuple(prospective_order),
                planning_purpose=(capacity_planning.CapacityPlanningPurpose.
                                  FRESH_ZERO_RETENTION
                                  if fresh_zero else capacity_planning.
                                  CapacityPlanningPurpose.ECONOMIC_ADMISSION),
                actuation_supply_policy=(
                    capacity_planning.ActuationSupplyPolicy.REUSE_CURRENT_SUPPLY
                ),
                attribution_complete=True,
                planning_time=planning_db_epoch,
                max_live_paid_gpu_units=max_live_paid_gpu_units,
                retirement_shelter_target=retirement_shelter_target,
                deadline=None if fresh_zero else deadline_input,
                source_fingerprint=source_fingerprint,
                configured_reservation_accelerators=(
                    configured_reservation_accelerators),
                demand_witness_scope_sha256=demand_witness_scope_sha256,
                prior_policy_state=planning_prior_state,
                prior_candidate=planning_prior_candidate,
                policy_input=policy_input)

            # This is the sole production planner invocation for the durable
            # logical reconciliation.  Every projection below consumes this
            # candidate and never re-runs allocation against mutable state.
            candidate = capacity_planning.plan_capacity(snapshot)
            if not candidate.attribution_complete:
                return None
            envelope = capacity_planning.CapacityPlanningEnvelope(
                schema_version=(capacity_planning.
                                CAPACITY_PLANNING_ENVELOPE_SCHEMA_VERSION),
                snapshot=snapshot,
                candidate=candidate)
            if candidate.kind is (
                    capacity_planning.CapacityPlanKind.GATE_ACQUISITION):
                return DurableCapacityReconcilePlan(
                    envelope=envelope,
                    logical_target=None,
                    logical_retirement_floor=None,
                    retirement_shelter=None,
                    scaling_decisions=(),
                    rollout_failure=None)
            if candidate.next_policy_state is None:
                return None

            def _logical_target(
                target: capacity_planning.AcceleratorCapacity,
            ) -> LogicalCapacityTarget:
                target_map = target.as_dict()
                return LogicalCapacityTarget(
                    version=self.latest_version,
                    generation=generation,
                    target_capacity=sum(target_map.values()),
                    target_capacity_by_accelerator=tuple(target_map.items()),
                    accelerator_shapes=tuple(
                        self.configured_accelerator_shapes.items()))

            logical_target = _logical_target(
                candidate.wave_limited_actuation_target)
            logical_retirement_floor = _logical_target(
                candidate.retirement_floor_target)
            target_map = candidate.wave_limited_actuation_target.as_dict()
            retirement_floor_map = candidate.retirement_floor_target.as_dict()

            rollout_failure = None
            latest_replicas = [
                info for info in infos if info.version == self.latest_version
            ]
            previous_versions = {
                info.version
                for info in infos
                if info.version < self.latest_version and not info.is_terminal
            }
            if (self.latest_version_ever_ready < self.latest_version and
                    previous_versions):
                unrecoverable = [
                    info for info in latest_replicas
                    if info.status_property.unrecoverable_failure()
                ]
                if unrecoverable:
                    evidence = ', '.join(
                        f'{info.replica_id}:{info.status.value}' for info in
                        sorted(unrecoverable,
                               key=lambda replica: replica.replica_id)[:20])
                    rollout_failure = UnrecoverableRolloutFailure(
                        version=self.latest_version,
                        reason=(f'Version {self.latest_version} never became '
                                'ready and has unrecoverable replica evidence: '
                                f'{evidence}.'))

            decisions: list[AutoscalerDecision] = []
            committed_by_card = {
                card: latest_committed.get(card, 0) +
                      old_committed.get(card, 0) for card in configured_cards
            }
            shortages = {
                card: max(
                    0,
                    target_map.get(card, 0) -
                    committed_by_card.get(card, 0)) for card in configured_cards
            }
            if rollout_failure is None and any(shortages.values()):
                priorities = {
                    card: constants.LB_REQUEST_PRIORITY_MIN
                    for card in configured_cards
                }
                for priority, compatible, work in work_profiles:
                    if work <= 0:
                        continue
                    clamped = max(
                        constants.LB_REQUEST_PRIORITY_MIN,
                        min(constants.LB_REQUEST_PRIORITY_MAX, priority))
                    for card in compatible:
                        priorities[card] = max(priorities[card], clamped)
                replacement_ids = tuple(
                    sorted(
                        info.replica_id
                        for info in infos
                        if (info.version == self.latest_version and
                            not info.is_terminal and
                            info.status_property.is_scale_down is not True and
                            committed_by_id.get(info.replica_id, 0) == 0)))
                scale_target = LogicalScaleTarget(
                    version=self.latest_version,
                    reconcile_generation=generation,
                    target_capacity=sum(target_map.values()),
                    target_capacity_by_accelerator=tuple(target_map.items()),
                    accelerator_shapes=tuple(
                        self.configured_accelerator_shapes.items()),
                    replace_unknown_replica_ids=replacement_ids,
                    launch_budget=sum(shortages.values()),
                    launch_priority=max(priorities.values()),
                    launch_priority_by_accelerator=tuple(priorities.items()),
                    cold_launch_authority_by_accelerator=tuple(
                        candidate.paid_launch_target.entries))
                decisions.append(
                    AutoscalerDecision(AutoscalerDecisionOperator.SCALE_UP,
                                       scale_target))

            next_state = candidate.next_policy_state
            assert next_state is not None
            upscale_pending = (candidate.raw_demand_target
                               > candidate.aggregate_demand_target)
            card_transition_pending = (
                candidate.wave_limited_actuation_target
                != candidate.supply_aware_actuation_target)
            if (rollout_failure is None and not any(shortages.values()) and
                    not upscale_pending and not card_transition_pending):
                status_order = serve_state.ReplicaStatus.scale_down_decision_order(
                )

                def _status_rank(info: 'replica_managers.ReplicaInfo') -> int:
                    try:
                        return status_order.index(info.status)
                    except ValueError:
                        return len(status_order)

                def _idle(info: 'replica_managers.ReplicaInfo') -> bool:
                    if info.replica_id in unknown_in_flight:
                        return False
                    if info.status in (serve_state.ReplicaStatus.READY,
                                       serve_state.ReplicaStatus.NOT_READY):
                        return in_flight.get(info.replica_id) == 0
                    return in_flight.get(info.replica_id, 0) == 0

                def _victim_eligible(
                        info: 'replica_managers.ReplicaInfo') -> bool:
                    shape = _shape(info)
                    if shape is None:
                        return False
                    normalized_shape = (shape[0].casefold(), shape[1])
                    blocked = decision_inputs.kueue_blocked_retirement_shapes
                    if ('*', 0) in blocked or normalized_shape in blocked:
                        return False
                    admission = kueue_classes.get(info.replica_id)
                    if admission in (kueue_lane_capacity.
                                     KueueReplicaCapacityClass.FRESH_WAITING,
                                     kueue_lane_capacity.
                                     KueueReplicaCapacityClass.UNKNOWN):
                        return False
                    if info.replica_id in (
                            decision_inputs.kueue_transition_replica_ids):
                        return info.replica_id in (
                            decision_inputs.
                            kueue_ready_paid_replacement_replica_ids)
                    if admission is (kueue_lane_capacity.
                                     KueueReplicaCapacityClass.POLICY_ADMITTED):
                        return info.is_ready
                    return True

                candidates = [
                    info for info in infos
                    if (not info.is_terminal and
                        info.status_property.is_scale_down is not True and
                        _idle(info) and _victim_eligible(info) and
                        committed_by_id.get(info.replica_id, 0) > 0)
                ]
                candidates.sort(key=lambda info: (
                    _status_rank(info),
                    committed_by_id.get(info.replica_id, 0),
                    info.is_zero_cost is True,
                    -info.replica_id,
                ))
                remaining_committed = sum(committed_by_card.values())
                remaining_by_card = dict(committed_by_card)
                remaining_ready = sum(_ready(info) for info in infos)
                remaining_ready_by_card = {card: 0 for card in configured_cards}
                for info in infos:
                    ready_card = card_by_id.get(info.replica_id)
                    if ready_card is not None:
                        remaining_ready_by_card[ready_card] += _ready(info)
                remaining_demand_pending = (provisioning_demand_owned_capacity)
                for info in candidates:
                    card = card_by_id[info.replica_id]
                    committed_width = committed_by_id[info.replica_id]
                    ready_width = _ready(info)
                    card_retirement_floor = retirement_floor_map.get(card, 0)
                    if (info.status in provisioning_statuses and
                            not info.reserved_fill and
                            next_state.pending_retention_floor is not None and
                            remaining_demand_pending - committed_width
                            < next_state.pending_retention_floor):
                        continue
                    if info.is_ready:
                        if (remaining_ready - ready_width
                                < logical_retirement_floor.target_capacity or
                                remaining_ready_by_card[card] - ready_width
                                < card_retirement_floor):
                            continue
                    elif (remaining_committed - committed_width
                          < logical_retirement_floor.target_capacity or
                          remaining_by_card[card] - committed_width
                          < card_retirement_floor):
                        continue
                    remaining_committed -= committed_width
                    remaining_by_card[card] -= committed_width
                    if info.is_ready:
                        remaining_ready -= ready_width
                        remaining_ready_by_card[card] -= ready_width
                    if (info.status in provisioning_statuses and
                            not info.reserved_fill):
                        remaining_demand_pending -= committed_width
                    decisions.append(
                        AutoscalerDecision(
                            AutoscalerDecisionOperator.SCALE_DOWN,
                            LogicalScaleDownTarget(
                                version=self.latest_version,
                                reconcile_generation=generation,
                                target_capacity=(
                                    logical_retirement_floor.target_capacity),
                                replica_id=info.replica_id,
                                target_capacity_by_accelerator=tuple(
                                    retirement_floor_map.items()),
                                accelerator_shapes=tuple(
                                    self.configured_accelerator_shapes.items()))
                        ))

            return DurableCapacityReconcilePlan(
                envelope=envelope,
                logical_target=logical_target,
                logical_retirement_floor=logical_retirement_floor,
                retirement_shelter=retirement_shelter,
                scaling_decisions=tuple(decisions),
                rollout_failure=rollout_failure)

    def set_configured_accelerator_shapes(self,
                                          shapes: dict[str, int],
                                          *,
                                          backend_num_nodes: int = 1) -> None:
        """Set the active version's authoritative exact-card shapes."""
        with self._logical_state_lock:
            self._set_configured_accelerator_shapes_locked(
                shapes, backend_num_nodes=backend_num_nodes)

    def _set_configured_accelerator_shapes_locked(
            self,
            shapes: dict[str, int],
            *,
            backend_num_nodes: int = 1) -> None:
        """Set exact-card shapes while holding the decision-state lock."""
        if (not isinstance(backend_num_nodes, int) or
                isinstance(backend_num_nodes, bool) or backend_num_nodes < 1):
            raise ValueError('Backend node count must be a positive integer.')
        previous_shapes = self.configured_accelerator_shapes
        previous_num_nodes = self.backend_num_nodes
        configured_shapes = {
            str(card): int(count)
            for card, count in shapes.items()
            if isinstance(card, str) and card and isinstance(count, int) and
            not isinstance(count, bool) and count > 0
        }
        catalog_changed = (bool(previous_shapes) and
                           (configured_shapes != previous_shapes or
                            backend_num_nodes != previous_num_nodes))
        self.configured_accelerator_shapes = configured_shapes
        self.backend_num_nodes = backend_num_nodes
        if catalog_changed:
            # Compatibility gauges describe the catalog under which the LB
            # admitted them. Never reinterpret an A100-only waiter as H100
            # demand, or an A100:1 target as A100:8 capacity, across an atomic
            # task-catalog update. The next complete report re-establishes all
            # replaceable gauges under the new routing version.
            self.compatibility_profiles = []
            self.queued_compatibility_profiles = []
            self.queued_deadline_profiles = None
            self.rejected_compatibility_profiles = []
            self.warm_retention_target_by_accelerator = {}
            self.cold_launch_authority_by_accelerator = {}
            self._logical_card_transition_pending = False
            self._logical_actuation_target_by_accelerator = {}
            self._logical_actuation_desired_by_accelerator = {}
            self._logical_adopted_explicit_target_by_accelerator = {}
            self._logical_adopted_paid_target_by_accelerator = {}
            self._logical_paid_launch_target_by_accelerator = {}
            self._compatibility_demand_complete = False
        if not self.configured_accelerator_shapes:
            self.target_num_replicas_by_accelerator = {}
            self.warm_retention_target_by_accelerator = {}
            self.cold_launch_authority_by_accelerator = {}
            self.compatibility_profiles = []
            self.queued_compatibility_profiles = []
            self.rejected_compatibility_profiles = []
            self.free_reserved_slots_by_accelerator = {}
            self._logical_card_transition_pending = False
            self._logical_actuation_target_by_accelerator = {}
            self._logical_actuation_desired_by_accelerator = {}
            self._logical_adopted_explicit_target_by_accelerator = {}
            self._logical_adopted_paid_target_by_accelerator = {}
            self._logical_paid_launch_target_by_accelerator = {}
            self._compatibility_demand_complete = False
            return
        floors = {
            card.casefold(): floor
            for card, floor in self.min_replicas_by_accelerator.items()
        }
        canonical_target: dict[str, int] = {}
        remaining = self.target_num_replicas
        for card in self.configured_accelerator_shapes:
            floor = min(remaining, int(floors.get(card.casefold(), 0)))
            if floor > 0:
                canonical_target[card] = floor
                remaining -= floor
        first = next(iter(self.configured_accelerator_shapes))
        while sum(canonical_target.values()) < self.target_num_replicas:
            canonical_target[first] = canonical_target.get(first, 0) + 1
        self.target_num_replicas_by_accelerator = canonical_target
        self._logical_adopted_explicit_target_by_accelerator = {
            card: min(canonical_target.get(card, 0),
                      max(0, int(floors.get(card.casefold(), 0))))
            for card in canonical_target
            if floors.get(card.casefold(), 0) > 0
        }
        self._logical_adopted_paid_target_by_accelerator = dict(
            canonical_target)

    def set_free_reserved_slots_by_accelerator(self, slots: dict[str,
                                                                 int]) -> None:
        """Set fresh unmaterialized reserved supply by exact card."""
        configured_by_name = {
            card.casefold(): card
            for card in self._configured_cards_from_profiles()
        }
        normalized: dict[str, int] = {}
        for raw_card, raw_count in slots.items():
            card = configured_by_name.get(str(raw_card).casefold())
            if card is None or isinstance(raw_count, bool):
                continue
            try:
                count = int(raw_count)
            except (TypeError, ValueError):
                continue
            if count > 0:
                normalized[card] = normalized.get(card, 0) + count
        self.free_reserved_slots_by_accelerator = normalized

    def _configured_cards_from_profiles(self) -> list[str]:
        if self.configured_accelerator_shapes:
            return list(self.configured_accelerator_shapes)
        cards: list[str] = []
        seen: set[str] = set()
        for profile in (self.queued_compatibility_profiles +
                        self.rejected_compatibility_profiles):
            for card in profile['compatible_accelerators']:
                if card.casefold() not in seen:
                    cards.append(card)
                    seen.add(card.casefold())
        for card in self.min_replicas_by_accelerator:
            if card.casefold() not in seen:
                cards.append(card)
                seen.add(card.casefold())
        return cards

    def _configured_gpu_count(self, card: str) -> int:
        for configured, count in self.configured_accelerator_shapes.items():
            if configured.casefold() == card.casefold():
                return count
        return 1

    def _cold_paid_card_order(self, configured_cards: list[str]) -> list[str]:
        """Order cold cards by nominal paid cost, independent of availability."""
        return self._order_cold_paid_cards_for_tick(configured_cards,
                                                    self._configured_gpu_count,
                                                    self._location_gpu_shape)

    def _prospective_paid_card_order(self,
                                     configured_cards: list[str]) -> list[str]:
        """Return cards on which a new paid replica can actually launch."""
        return self._prospective_paid_cards_for_tick(configured_cards,
                                                     self._configured_gpu_count,
                                                     self._location_gpu_shape)

    def _staleness_threshold_seconds(self) -> float:
        """Age beyond which a demand report no longer counts as fresh.

        Three sync intervals: one for the in-flight sync, one for jitter,
        one for a single dropped sync -- beyond that the LB is gone or
        wedged and the gauges describe a fleet state that may no longer
        exist.
        """
        return 3.0 * constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS

    def has_fresh_demand_report(self) -> bool:
        if (self._in_flight_by_replica_id is None or
                self._report_received_at is None):
            return False
        return (
            time.time() -
            self._report_received_at) <= self._staleness_threshold_seconds()

    def has_recomputed_with_fresh_data(self) -> bool:
        """Whether the target reflects at least one fresh-data recompute.

        The first LB report flips has_fresh_demand_report() on the SYNC
        thread, but target_num_replicas stays at the rebuilt-blind
        min_replicas until the autoscaler thread's next decision tick
        consumes the one-shot snap. Consumers that would act on a blind
        target (the controller's capacity hint) must keep their
        stale-mode floor until this is True, or a routine controller
        restart reports target=min_replicas to the platform's spill
        logic for a tick.
        """
        return not self._snap_target_on_next_recompute

    @property
    def reconcile_generation(self) -> int:
        return self._reconcile_generation

    @property
    def logical_target_state(self,) -> LogicalCapacityTarget | None:
        """Version, report generation, and demand actuation target."""
        with self._logical_state_lock:
            return self._last_logical_target_state

    def _fresh_for_tick(self) -> bool:
        """Freshness as snapshotted once at the top of the current tick.

        collect_request_information runs concurrently on the sync
        thread; if the first fresh report landed mid-tick,
        re-evaluating freshness at each consumer would let the
        recompute take the stale path (target still the rebuilt-blind
        minimum) while the later drain/scale-down guards saw fresh and
        proceeded -- marrying a blind target to fresh-mode kills. Falls
        back to a live evaluation when no tick snapshot is active (a
        direct call outside generate_scaling_decisions).
        """
        if self._tick_fresh is not None:
            return self._tick_fresh
        return self.has_fresh_demand_report()

    def fill_demand_sample(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> 'FillDemandSample | None':
        """Demonstrated work for the reserved-fill utilization gate.

        Called from the poller thread, never from the decision tick, so it
        must not mutate decision-owned state: it uses the pure
        _outstanding_work_parts rather than _outstanding_work.

        Returns None whenever the demand report is not fresh. The poller
        publishes this as armed-but-blind (fresh activity_ts, NULL need), so
        the broker freezes for the blind grace before it resumes bounded
        decay; it does not mistake telemetry loss for confirmed idle.
        """
        with self._logical_state_lock:
            if not self.has_fresh_demand_report():
                return None
            if self._in_flight_by_replica_id is None:
                return None
            queue_work, rejected, unknown_floor = (
                self._outstanding_work_parts(replica_infos))
            outstanding = float(
                sum(self._in_flight_by_replica_id.values()) + queue_work +
                rejected + unknown_floor)
            busy = 0
            pre_ready = 0
            pre_ready_statuses = (
                serve_state.ReplicaStatus.PENDING,
                serve_state.ReplicaStatus.PROVISIONING,
                serve_state.ReplicaStatus.STARTING,
            )
            for info in replica_infos:
                if info.is_terminal:
                    continue
                if not self._replica_on_zero_cost_location(info):
                    continue
                if not info.reserved_fill:
                    # Demand-placed zero-cost rows are demand-protected and
                    # already exempt from the grant ceiling, so counting
                    # them here would inflate the need by capacity the gate
                    # can never reclaim anyway.
                    continue
                if info.status in pre_ready_statuses:
                    pre_ready += 1
                elif self._replica_is_busy(info):
                    busy += 1
            work_per_replica = float(self.target_concurrency_per_replica)
            if self.replica_unit == 'logical':
                work_per_replica = self._effective_logical_capacity_per_gpu()
            return FillDemandSample(
                outstanding_work=outstanding,
                busy_fill_holdings=busy,
                pre_ready_fill_holdings=pre_ready,
                upscale_pending=self.upscale_counter > 0,
                work_per_replica=work_per_replica,
                planned_replicas=max(0,
                                     int(self.get_final_target_num_replicas())),
            )

    def _replica_is_busy(self, info: 'replica_managers.ReplicaInfo') -> bool:
        """Whether the latest report shows in-flight work on a replica.

        READY and NOT_READY replicas missing from the report count as
        BUSY: for READY the LB may simply not have picked them up yet;
        for NOT_READY the replica WAS serving and blipped a probe -- for
        async fast-ack work the LB's occupancy probe only covers the
        routable set, so a blipped replica's running jobs may be
        unreported, and guessing idle kills them. Both also count busy
        with reported work > 0, which the controller keeps attributable
        (sticky url translation) while the replica is nonterminal.
        Never-served statuses (PENDING/PROVISIONING/STARTING) missing
        from the report count as idle: they cannot carry jobs, and
        treating them busy would starve scale-down of its preferred
        kill-first victims.
        """
        if info.replica_id in self._unknown_in_flight_replica_ids:
            return True
        in_flight = self._in_flight_by_replica_id or {}
        if info.status in (serve_state.ReplicaStatus.READY,
                           serve_state.ReplicaStatus.NOT_READY):
            return in_flight.get(info.replica_id) != 0
        return in_flight.get(info.replica_id, 0) > 0

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        with self._logical_state_lock:
            self._collect_request_information_locked(request_aggregator_info)

    def _collect_request_information_locked(
            self, request_aggregator_info: dict[str, Any]) -> None:
        """Collect timestamps and the latest LB demand report.

        Expected dict (extra keys ignored; all demand keys optional so an
        old LB that only ships timestamps degrades to the signal-gap
        rules):

        {
            'timestamps': [...],
            'in_flight_by_replica_id': {replica_id: int} | None,
            'queue_depth': int | None,
            'rejected_in_window': int | None,
            'rejected_in_recent_window': int | None,
            'unknown_in_flight_replica_ids': [replica_id, ...],
            'observed_slots_by_replica_id': {replica_id: int},
            'unknown_capacity_replica_ids': [replica_id, ...],
            'reconcile_generation': int,
        }
        """
        replace_request_window = request_aggregator_info.get(
            'replace_request_window') is True
        incoming_timestamps = request_aggregator_info.get('timestamps', [])
        if replace_request_window:
            self.request_timestamps = list(incoming_timestamps)
        else:
            self.request_timestamps.extend(incoming_timestamps)
        current_time = time.time()
        index = bisect.bisect_left(self.request_timestamps,
                                   current_time - self.qps_window_size)
        self.request_timestamps = self.request_timestamps[index:]
        self.compatibility_profiles = [
            profile for profile in self.compatibility_profiles
            if profile['timestamp'] >= current_time -
            constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS
        ]

        in_flight = request_aggregator_info.get('in_flight_by_replica_id')
        if in_flight is None:
            # No usable demand report in this sync (old LB, or a policy
            # that cannot track in-flight). Keep the previous report: it
            # ages out on its own; overwriting it with nothing would
            # discard a still-fresh signal.
            return
        compatibility_complete = (
            bool(self.configured_accelerator_shapes) and
            request_aggregator_info.get('compatibility_demand_complete')
            is True)
        if compatibility_complete:
            incoming_profiles = self._parse_compatibility_arrivals(
                request_aggregator_info.get('compatibility_profiles', []))
            if replace_request_window:
                self.compatibility_profiles = incoming_profiles
            else:
                self.compatibility_profiles.extend(incoming_profiles)
            self.queued_compatibility_profiles = (
                self._parse_compatibility_gauge(
                    request_aggregator_info.get(
                        'queued_requests_by_compatibility', [])))
            self.rejected_compatibility_profiles = (
                self._parse_compatibility_gauge(request_aggregator_info.get(
                    'rejected_requests_by_compatibility', []),
                                                include_recent_count=True))
        raw_deadlines = request_aggregator_info.get(
            'queued_request_deadline_buckets')
        parsed_deadlines = self._parse_deadline_gauge(raw_deadlines)
        self.queued_deadline_profiles = None
        if (compatibility_complete and isinstance(raw_deadlines, list) and
                len(parsed_deadlines) == len(raw_deadlines)):
            deadline_by_priority: dict[int, int] = {}
            for profile in parsed_deadlines:
                priority = int(profile['priority'])
                deadline_by_priority[priority] = (
                    deadline_by_priority.get(priority, 0) +
                    int(profile['count']))
            raw_queue_by_priority = request_aggregator_info.get(
                'queue_depth_by_priority')
            normalized_queue_by_priority: dict[int, int] | None = None
            if isinstance(raw_queue_by_priority, dict):
                normalized_queue_by_priority = {}
                for priority, count in raw_queue_by_priority.items():
                    if (not str(priority).isdigit() or
                            not 0 <= int(priority) <= 100 or
                            not isinstance(count, int) or
                            isinstance(count, bool) or count < 0):
                        normalized_queue_by_priority = None
                        break
                    normalized_queue_by_priority[int(priority)] = count
            queue_depth = request_aggregator_info.get('queue_depth')
            if (normalized_queue_by_priority is not None and
                    isinstance(queue_depth, int) and
                    not isinstance(queue_depth, bool) and queue_depth >= 0 and
                    sum(deadline_by_priority.values()) == queue_depth and
                    deadline_by_priority == normalized_queue_by_priority):
                self.queued_deadline_profiles = parsed_deadlines
        self._compatibility_demand_complete = compatibility_complete
        # Normalize keys/values: the controller builds this dict
        # in-process today, but a defensive int() keeps us safe if it is
        # ever rebuilt from a JSON round-trip (string keys).
        self._in_flight_by_replica_id = {
            int(replica_id): int(count)
            for replica_id, count in in_flight.items()
        }
        queue_depth = request_aggregator_info.get('queue_depth')
        self._queue_depth = int(queue_depth) if queue_depth is not None else 0

        def _priority_counts(value: Any) -> dict[int, int] | None:
            if not isinstance(value, dict):
                return None
            return {
                int(priority): int(count)
                for priority, count in value.items()
                if (str(priority).isdigit() and 0 <= int(priority) <= 100 and
                    isinstance(count, int) and not isinstance(count, bool) and
                    count >= 0)
            }

        self._queue_depth_by_priority = _priority_counts(
            request_aggregator_info.get('queue_depth_by_priority'))
        rejected = request_aggregator_info.get('rejected_in_window')
        self._rejected_in_window = int(rejected) if rejected is not None else 0
        recent_rejected = request_aggregator_info.get(
            'rejected_in_recent_window')
        self._rejected_in_recent_window = (
            int(recent_rejected) if recent_rejected is not None else None)
        self._rejected_in_window_by_priority = _priority_counts(
            request_aggregator_info.get('rejected_in_window_by_priority'))
        self._rejected_in_recent_window_by_priority = _priority_counts(
            request_aggregator_info.get(
                'rejected_in_recent_window_by_priority'))

        def _optional_count(field: str) -> int | None:
            value = request_aggregator_info.get(field)
            if (not isinstance(value, int) or isinstance(value, bool) or
                    value < 0):
                return None
            return value

        self._unique_job_arrivals_60s = _optional_count(
            'unique_job_arrivals_60s')
        self._unique_job_arrivals_300s = _optional_count(
            'unique_job_arrivals_300s')
        self._headerless_arrivals_60s = _optional_count(
            'headerless_arrivals_60s')
        self._headerless_arrivals_300s = _optional_count(
            'headerless_arrivals_300s')
        self._offered_arrival_tracking_saturated = (
            request_aggregator_info.get('offered_arrival_tracking_saturated')
            is True)
        self._ingest_prediction_time_history(
            request_aggregator_info.get('prediction_time_history'))
        report_is_floored = request_aggregator_info.get(
            'pressure_report_is_floored') is True
        arrival_60 = self._offered_arrival_count(60)
        pressure_sample = (self._queue_depth, self._rejected_in_recent_window or
                           0, arrival_60)
        if not report_is_floored:
            if self._pressure_baseline is None:
                self._pressure_streak = 0
            else:
                labels = ('queue_depth', 'recent_rejections',
                          'offered_arrivals_60s')
                reasons = tuple(label for label, current, previous in zip(
                    labels, pressure_sample, self._pressure_baseline)
                                if current > previous)
                if not reasons:
                    # A queue pinned flat at its cap is saturation, not
                    # relief; requiring strictly increasing samples disarms
                    # adaptive scale-up exactly when overload plateaus.
                    # Only a draining queue resets the streak. The plateau
                    # floor keeps a benign flat trickle queue from latching
                    # pressure indefinitely, and stable rejection
                    # populations deliberately stay non-latching (bounded
                    # downscale vetoes) -- cap and timeout rejections always
                    # ride on a deep queue, which this clause covers.
                    plateau_floor = max(1, self.scale_up_rate_min_replicas or 1)
                    if (pressure_sample[0] >= plateau_floor and
                            pressure_sample[0] >= self._pressure_baseline[0]):
                        reasons = ('queue_plateau',)
                if reasons:
                    self._pressure_latched = True
                    self._pressure_reasons = reasons
                    self._pressure_streak += 1
                    if (self.adaptive_scale_up is not None and
                            self._pressure_streak
                            >= self.adaptive_scale_up['pressure_observations']):
                        self._adaptive_until = (
                            time.monotonic() +
                            self.adaptive_scale_up['hold_seconds'])
                else:
                    self._pressure_streak = 0
            self._pressure_baseline = pressure_sample
        else:
            # A maximum-merged handoff gauge is not an authoritative
            # observation of new offered demand. It also breaks a run of
            # consecutive pressure observations, while leaving an already
            # active adaptive hold untouched until its normal expiry.
            self._pressure_streak = 0
        self._unknown_in_flight_replica_ids = {
            int(replica_id) for replica_id in (request_aggregator_info.get(
                'unknown_in_flight_replica_ids', []) or [])
        }
        self._observed_slots_by_replica_id = {
            int(replica_id): max(0, int(slots))
            for replica_id, slots in request_aggregator_info.get(
                'observed_slots_by_replica_id', {}).items()
        }
        self._unknown_capacity_replica_ids = {
            int(replica_id) for replica_id in request_aggregator_info.get(
                'unknown_capacity_replica_ids', [])
        }
        degraded_capacity_ids = self._unknown_capacity_replica_ids | {
            replica_id
            for replica_id, slots in self._observed_slots_by_replica_id.items()
            if slots == 0
        }
        for replica_id in degraded_capacity_ids:
            self._degraded_capacity_since_by_replica_id.setdefault(
                replica_id, current_time)
        self._degraded_capacity_since_by_replica_id = {
            replica_id: since
            for replica_id, since in
            self._degraded_capacity_since_by_replica_id.items()
            if replica_id in degraded_capacity_ids
        }
        self._reconcile_generation = int(
            request_aggregator_info.get('reconcile_generation',
                                        self._reconcile_generation + 1))
        self._report_received_at = current_time
        # A plan is tied to the exact queue/supply snapshot consumed by one
        # decision tick.  Never reuse it after a newer report arrives.
        self._deadline_capacity_plan = None
        self._launch_priority_report_received_at = current_time
        logger.info(f'Concurrency report: in_flight_total='
                    f'{sum(self._in_flight_by_replica_id.values())}, '
                    f'queue_depth={self._queue_depth}, '
                    f'rejected_in_window={self._rejected_in_window}, '
                    f'rejected_in_recent_window='
                    f'{self._rejected_in_recent_window}, '
                    f'unknown_replicas='
                    f'{len(self._unknown_in_flight_replica_ids)}, '
                    f'requests in the last {self.qps_window_size}s: '
                    f'{len(self.request_timestamps)}')

    @staticmethod
    def _parse_compatibility_arrivals(
            raw_profiles: Any) -> list[dict[str, Any]]:
        profiles: list[dict[str, Any]] = []
        if not isinstance(raw_profiles, list):
            return profiles
        for raw in raw_profiles:
            if not isinstance(raw, dict):
                continue
            timestamp = raw.get('timestamp')
            priority = raw.get('priority')
            accelerators = raw.get('compatible_accelerators')
            count = raw.get('count', 1)
            if (not isinstance(timestamp,
                               (int, float)) or isinstance(timestamp, bool) or
                    not math.isfinite(timestamp) or timestamp < 0 or
                    not isinstance(priority, int) or
                    isinstance(priority, bool) or
                    not isinstance(accelerators, list) or not accelerators or
                    not all(
                        isinstance(card, str) and card for card in accelerators)
                    or not isinstance(count, int) or isinstance(count, bool) or
                    count < 1):
                continue
            profiles.append({
                'timestamp': float(timestamp),
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'count': count,
            })
        return profiles

    @staticmethod
    def _parse_compatibility_gauge(
        raw_profiles: Any,
        *,
        include_recent_count: bool = False,
    ) -> list[dict[str, Any]]:
        profiles: list[dict[str, Any]] = []
        if not isinstance(raw_profiles, list):
            return profiles
        for raw in raw_profiles:
            if not isinstance(raw, dict):
                continue
            priority = raw.get('priority')
            accelerators = raw.get('compatible_accelerators')
            count = raw.get('count', 1)
            recent_count = raw.get('recent_count')
            if (not isinstance(priority, int) or isinstance(priority, bool) or
                    not isinstance(accelerators, list) or not accelerators or
                    not all(
                        isinstance(card, str) and card for card in accelerators)
                    or not isinstance(count, int) or isinstance(count, bool) or
                    count < 1):
                continue
            profile: dict[str, Any] = {
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'count': count,
            }
            if include_recent_count:
                if (not isinstance(recent_count, int) or
                        isinstance(recent_count, bool) or recent_count < 0 or
                        recent_count > count):
                    continue
                profile['recent_count'] = recent_count
            profiles.append(profile)
        return profiles

    @staticmethod
    def _parse_deadline_gauge(raw_profiles: Any) -> list[dict[str, Any]]:
        profiles: list[dict[str, Any]] = []
        if not isinstance(raw_profiles, list):
            return profiles
        for raw in raw_profiles:
            if not isinstance(raw, dict):
                continue
            priority = raw.get('priority')
            accelerators = raw.get('compatible_accelerators')
            remaining = raw.get('remaining_seconds')
            count = raw.get('count')
            if (not isinstance(priority, int) or isinstance(priority, bool) or
                    not 0 <= priority <= 100 or
                    not isinstance(accelerators, list) or not accelerators or
                    not all(
                        isinstance(card, str) and card for card in accelerators)
                    or not isinstance(remaining, (int, float)) or
                    isinstance(remaining, bool) or
                    not math.isfinite(remaining) or remaining < 0 or
                    remaining > constants.LB_REQUEST_DEADLINE_MAX_SECONDS or
                    not isinstance(count, int) or isinstance(count, bool) or
                    count < 1):
                continue
            profiles.append({
                'priority': priority,
                'compatible_accelerators': tuple(accelerators),
                'remaining_seconds': float(remaining),
                'count': count,
            })
        return profiles

    def _get_knob_for_version(self, version: int) -> float:
        """The per-GPU knob a given service version was launched under.

        Unknown versions (the autoscaler was rebuilt after the update
        that created them, e.g. a controller restart mid-rolling-update)
        rehydrate from the durable per-version spec so old-version
        replicas keep their real capacity. Falls back to the latest knob
        when the version's spec is unavailable; misses are not memoized
        across ticks so a transient DB error can heal on the next tick.
        """
        cached = self._knob_by_version.get(version)
        if cached is not None:
            return cached
        unavailable_versions = self._knob_unavailable_versions_for_tick
        if (unavailable_versions is not None and
                version in unavailable_versions):
            return self.target_concurrency_per_replica
        # Historical metadata has one canonical I/O path: the prepared token.
        # An unexpected miss must fail closed rather than re-enter PostgreSQL
        # from decision generation under the controller routing lock.
        if unavailable_versions is not None:
            unavailable_versions.add(version)
        else:
            logger.warning(
                'Historical concurrency version %s was used outside a '
                'prepared decision; using the latest-version fallback.',
                version)
        return self.target_concurrency_per_replica

    def _replica_capacity(self, info: 'replica_managers.ReplicaInfo') -> float:
        """A replica's capacity in the autoscaler's target units.

        Logical targets are GPU slots, so a physical backend contributes its
        immutable planned slot width. Physical-backend targets are replica
        counts, so each replica contributes knob x gpu_count concurrency.
        The knob is resolved for the replica's OWN version after updates.
        """
        if self.replica_unit == 'logical':
            return float(info.planned_capacity)
        _, gpu_count = self._get_gpu_shape_from_replica_info(info)
        return self._get_knob_for_version(info.version) * gpu_count

    def _fill_capacity_units(self, info: 'replica_managers.ReplicaInfo') -> int:
        if self.replica_unit == 'logical':
            return max(1, int(self._replica_capacity(info)))
        return super()._fill_capacity_units(info)

    def _ready_capacity(self, info: 'replica_managers.ReplicaInfo') -> int:
        """Observed ready logical slots, or zero when not proven fresh."""
        if not info.is_ready:
            return 0
        observed = self._observed_slots_by_replica_id.get(info.replica_id)
        if (observed is None or
                info.replica_id in self._unknown_capacity_replica_ids):
            return 0
        return min(int(self._replica_capacity(info)), observed)

    def _committed_capacity(self, info: 'replica_managers.ReplicaInfo') -> int:
        """Pinned capacity used to suppress duplicate logical launches."""
        if _replica_is_retiring_card_supply(info):
            return 0
        if not self._kueue_counts_as_assigned(info):
            return 0
        planned = int(self._replica_capacity(info))
        observed = self._observed_slots_by_replica_id.get(info.replica_id)
        degraded = (info.replica_id in self._unknown_capacity_replica_ids or
                    (info.is_ready and observed == 0))
        if degraded:
            degraded_since = self._degraded_capacity_since_by_replica_id.get(
                info.replica_id)
            replacement_age = (time.time() - degraded_since
                               if degraded_since is not None else 0)
            replacement_timeout = (
                constants.LOGICAL_UNKNOWN_CAPACITY_REPLACEMENT_SECONDS)
            if (self.replica_unit == 'logical' and
                    degraded_since is not None and
                    replacement_age >= replacement_timeout and
                    info.unknown_capacity_replacement is not True):
                return 0
            return planned
        if info.is_ready and observed is not None:
            return min(planned, observed)
        return planned

    def _reserved_fill_committed_capacity(
            self, info: 'replica_managers.ReplicaInfo') -> int:
        if self.replica_unit == 'logical':
            return max(0, self._committed_capacity(info))
        return super()._reserved_fill_committed_capacity(info)

    def get_ready_replica_capacity(self,
                                   info: 'replica_managers.ReplicaInfo') -> int:
        if self.replica_unit == 'logical':
            # Public status reports materialized GPU inventory. Occupancy
            # freshness remains a separate safety signal: internal scale-down,
            # replacement, and retirement paths call `_ready_capacity()`
            # directly and continue to fail closed on unknown observations.
            return (max(1, int(self._replica_capacity(info)))
                    if info.is_ready else 0)
        return super().get_ready_replica_capacity(info)

    def _cost_rebalance_replica_capacity(
            self, info: 'replica_managers.ReplicaInfo') -> float:
        return self._replica_capacity(info)

    def _cost_rebalance_location_capacity(
            self, location: spot_placer.Location) -> float:
        _, gpu_count = self._location_gpu_shape(location)
        if self.replica_unit == 'logical':
            return float(gpu_count)
        return self.target_concurrency_per_replica * gpu_count

    def _latest_capacities(
            self,
            replica_infos: list['replica_managers.ReplicaInfo']) -> list[float]:
        """Capacities of live latest-version replicas, largest first."""
        capacities = []
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    not self._kueue_counts_as_assigned(info)):
                continue
            capacity = self._replica_capacity(info)
            if capacity > 0:
                capacities.append(capacity)
        capacities.sort(reverse=True)
        return capacities

    def _effective_logical_capacity_per_gpu(self) -> float:
        return (self.target_concurrency_per_replica *
                self.target_utilization_percentage / 100.0)

    def _clip_concurrency_demand_target(self, target: int) -> int:
        """Clip demand, allowing a logical wave to approach floors."""
        if (self.replica_unit == 'logical' and
                self.max_scale_up_rate_percentage is not None):
            return max(0, min(self.max_replicas, target))
        return self._clip_target_num_replicas(target)

    def _priority_timeout(self, priority: int) -> float | None:
        timeout = self._queue_timeout_seconds
        for min_priority, threshold_timeout in self._queue_timeout_thresholds:
            if priority < min_priority:
                break
            timeout = threshold_timeout
        return timeout

    def _queue_deadline_weighting_available(self) -> bool:
        """Whether the current queue gauges support deadline weighting."""
        return (self.replica_unit == 'logical' and
                self.effective_request_duration_seconds is not None and
                self._queue_timeout_seconds is not None and
                self._queue_depth_by_priority is not None and
                sum(self._queue_depth_by_priority.values())
                >= self._queue_depth)

    def _deadline_capacity_planning_available(self) -> bool:
        """Whether one complete current queue can use capacity-time sizing."""
        profiles = self.queued_deadline_profiles
        return (self.replica_unit == 'logical' and profiles is not None and
                self._compatibility_demand_complete and
                self.effective_request_duration_seconds is not None and
                sum(int(profile['count']) for profile in profiles)
                == self._queue_depth)

    def _deadline_planning_input_for_supply(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        configured_cards: list[str],
        free_reserved: Mapping[str, int],
        *,
        planning_time: float,
        kueue_capacity_by_replica_id: Mapping[
            int, kueue_lane_capacity.KueueReplicaCapacityClass] | None,
    ) -> capacity_planning.DeadlinePlanningInput | None:
        """Prepare immutable deadline facts without running an allocator."""
        if not self._deadline_capacity_planning_available():
            return None
        duration = self.effective_request_duration_seconds
        assert duration is not None
        canonical = {card.casefold(): card for card in configured_cards}
        service_seconds = {card: duration for card in configured_cards}
        sources = {
            card: 'aggregate_or_configured_seed' for card in configured_cards
        }
        for raw_card, estimate in (
                self._service_time_estimates_by_accelerator.items()):
            card = canonical.get(raw_card.casefold())
            observed_at = estimate.get('observed_at')
            measured = estimate.get('duration_seconds')
            samples = estimate.get('samples')
            if (card is None or not isinstance(measured, (int, float)) or
                    isinstance(measured, bool) or float(measured) <= 0 or
                    not isinstance(samples, int) or isinstance(samples, bool) or
                    samples < constants.AUTOSCALER_ADAPTIVE_DURATION_MIN_SAMPLES
                    or not isinstance(observed_at, (int, float)) or
                    isinstance(observed_at, bool) or
                    not -60.0 <= planning_time - float(observed_at) <=
                    constants.AUTOSCALER_ADAPTIVE_SAMPLE_MAX_AGE_SECONDS):
                continue
            service_seconds[card] = float(measured)
            sources[card] = 'postgresql_async_ledger_p75'
        finite_supply: list[DeadlineSupply] = []
        for info in replica_infos:
            capacity_class = (None if kueue_capacity_by_replica_id is None else
                              kueue_capacity_by_replica_id.get(info.replica_id))
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info) or
                    capacity_class is kueue_lane_capacity.
                    KueueReplicaCapacityClass.FRESH_WAITING):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = canonical.get(raw_card.casefold())
            if card is None:
                continue
            width = max(0, self._committed_capacity(info))
            if width <= 0:
                continue
            zero_cost = info.is_zero_cost is True
            if info.is_ready:
                if info.replica_id in self._unknown_in_flight_replica_ids:
                    # Unknown occupancy remains protected by fixed work, but
                    # it cannot promise queue capacity before a deadline.
                    continue
                in_flight = max(
                    0,
                    int((self._in_flight_by_replica_id or
                         {}).get(info.replica_id, 0)))
                base, extra = divmod(in_flight, width)
                tier = 0 if zero_cost else 3
                for index in range(width):
                    jobs = base + (1 if index < extra else 0)
                    available = (jobs * service_seconds[card] /
                                 (self.target_utilization_percentage / 100.0))
                    finite_supply.append(
                        DeadlineSupply(card=card,
                                       available_after_seconds=available,
                                       tier=tier))
                continue
            created_at = info.created_at
            age = (max(0.0, planning_time -
                       float(created_at)) if isinstance(created_at,
                                                        (int, float)) and
                   not isinstance(created_at, bool) else 0.0)
            available = max(0.0, self.effective_provision_lead_seconds - age)
            tier = 1 if zero_cost else 4
            finite_supply.extend(
                DeadlineSupply(
                    card=card, available_after_seconds=available, tier=tier)
                for _ in range(width))
        for raw_card, count in free_reserved.items():
            card = canonical.get(str(raw_card).casefold())
            if card is None:
                continue
            finite_supply.extend(
                DeadlineSupply(card=card,
                               available_after_seconds=(
                                   self.effective_provision_lead_seconds),
                               tier=2) for _ in range(max(0, int(count))))
        demand = [
            DeadlineDemand(
                sequence=sequence,
                priority=int(profile['priority']),
                compatible_cards=tuple(profile['compatible_accelerators']),
                count=int(profile['count']),
                remaining_seconds=float(profile['remaining_seconds']))
            for sequence, profile in enumerate(self.queued_deadline_profiles or
                                               ())
        ]
        return capacity_planning.DeadlinePlanningInput(
            demand=tuple(demand),
            finite_supply=tuple(finite_supply),
            service_seconds_by_accelerator=(capacity_planning.AcceleratorWork.
                                            from_mapping(service_seconds)),
            service_time_sources=tuple(sources.items()),
            utilization=self.target_utilization_percentage / 100.0,
            paid_cold_lead_seconds=self.effective_provision_lead_seconds)

    def _queued_request_work(self, priority: int) -> float:
        """Concurrent work represented by one queued request."""
        if not self._queue_deadline_weighting_available():
            return 1.0
        duration = self.effective_request_duration_seconds
        assert duration is not None
        timeout = self._priority_timeout(priority)
        if timeout is None:
            return 1.0
        lead = self.effective_provision_lead_seconds
        return min(1.0, duration / max(duration, timeout - lead))

    def _queue_work(self) -> float:
        if self._deadline_capacity_plan is not None:
            planned_work = (
                sum(self._deadline_capacity_plan.target_by_card.values()) *
                self._effective_logical_capacity_per_gpu())
            running_work = float(
                sum((self._in_flight_by_replica_id or {}).values()))
            return max(0.0, planned_work - running_work)
        if not self._queue_deadline_weighting_available():
            # A mixed-version HA floor can carry aggregate demand from an old
            # active beside an empty or partial priority map from the new
            # active. Never let the optional map erase that proven queue.
            return float(self._queue_depth)
        assert self._queue_depth_by_priority is not None
        return sum(count * self._queued_request_work(priority)
                   for priority, count in self._queue_depth_by_priority.items())

    def _queued_compatibility_work(
        self,
        configured_cards: list[str],
    ) -> tuple[list[_CompatibilityWorkProfile],
               list[_CompatibilityWorkProfile]]:
        """Return the queue's canonical priority/exact-card work profiles.

        Aggregate target sizing and exact-card allocation must consume the
        same queue-work representation.  The former historically applied
        priority timeout weights while the latter consumed raw request counts,
        so a complete compatibility report could silently erase the timeout
        discount by raising the aggregate target back to one slot per request.

        A current complete report carries matching aggregate, priority, and
        compatibility gauges.  HA handoff and adjacent-version reports can be
        conservatively floored or partial, so the result is bounded to the
        aggregate queue work in strict-priority order and any unattributed
        remainder stays flexible across all configured cards.  When deadline
        gauges are incomplete, every request remains one raw work unit.
        """
        default_compatible = tuple(configured_cards)

        def public_profiles(
            entries: list[_AnnotatedCompatibilityWorkProfile],
            *,
            explicit_only: bool = False,
        ) -> list[_CompatibilityWorkProfile]:
            return [(priority, compatible, work)
                    for priority, compatible, work, is_explicit in entries
                    if not explicit_only or is_explicit]

        profile_entries = [(int(profile['priority']),
                            tuple(profile['compatible_accelerators']),
                            float(profile['count']) *
                            self._queued_request_work(int(profile['priority'])),
                            True)
                           for profile in self.queued_compatibility_profiles]
        raw_profile_count = sum(
            int(profile['count'])
            for profile in self.queued_compatibility_profiles)
        if not self._queue_deadline_weighting_available():
            if self._queue_depth > raw_profile_count:
                profile_entries.append(
                    (constants.LB_REQUEST_PRIORITY_MIN, default_compatible,
                     float(self._queue_depth - raw_profile_count), False))
            return (public_profiles(profile_entries),
                    public_profiles(profile_entries, explicit_only=True))

        assert self._queue_depth_by_priority is not None
        profiled_by_priority: dict[int, int] = {}
        for profile in self.queued_compatibility_profiles:
            priority = int(profile['priority'])
            profiled_by_priority[priority] = (
                profiled_by_priority.get(priority, 0) + int(profile['count']))
        for priority, count in self._queue_depth_by_priority.items():
            missing = max(0, count - profiled_by_priority.get(priority, 0))
            if missing:
                profile_entries.append(
                    (priority, default_compatible,
                     missing * self._queued_request_work(priority), False))

        aggregate_work = self._queue_work()
        represented_work = sum(entry[2] for entry in profile_entries)
        if represented_work < aggregate_work - _SLOT_CONVERSION_EPSILON:
            highest_priority = max(
                (priority
                 for priority, count in self._queue_depth_by_priority.items()
                 if count > 0),
                default=constants.LB_REQUEST_PRIORITY_MIN)
            profile_entries.append((highest_priority, default_compatible,
                                    aggregate_work - represented_work, False))
            return (public_profiles(profile_entries),
                    public_profiles(profile_entries, explicit_only=True))
        if represented_work <= aggregate_work + _SLOT_CONVERSION_EPSILON:
            return (public_profiles(profile_entries),
                    public_profiles(profile_entries, explicit_only=True))

        # Maximum-merged HA profiles can describe more simultaneous work than
        # their aggregate gauge.  Keep the aggregate magnitude authoritative,
        # retaining higher priorities first and preserving the distribution of
        # equal-priority compatibility sets.
        bounded_entries: list[_AnnotatedCompatibilityWorkProfile] = []
        remaining = aggregate_work
        for priority in sorted({item[0] for item in profile_entries},
                               reverse=True):
            group = [item for item in profile_entries if item[0] == priority]
            group_work = sum(item[2] for item in group)
            accepted = min(remaining, group_work)
            if accepted <= _SLOT_CONVERSION_EPSILON:
                break
            scale = accepted / group_work
            bounded_entries.extend(
                (profile_priority, compatible, work * scale, is_explicit)
                for profile_priority, compatible, work, is_explicit in group)
            remaining -= accepted
        return (public_profiles(bounded_entries),
                public_profiles(bounded_entries, explicit_only=True))

    def _offered_arrival_count(self, window_seconds: int) -> int:
        if self._offered_arrival_tracking_saturated:
            return constants.LB_OFFERED_ARRIVAL_CAP
        if window_seconds == constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS:
            values = (self._unique_job_arrivals_60s,
                      self._headerless_arrivals_60s)
        else:
            values = (self._unique_job_arrivals_300s,
                      self._headerless_arrivals_300s)
        if any(value is None for value in values):
            return 0
        return sum(typing.cast(int, value) for value in values)

    def _arrival_work(self) -> float:
        duration = self.effective_request_duration_seconds
        if duration is None:
            return 0.0
        if (self._unique_job_arrivals_60s is None or
                self._unique_job_arrivals_300s is None or
                self._headerless_arrivals_60s is None or
                self._headerless_arrivals_300s is None):
            return (len(self.request_timestamps) * duration /
                    self.qps_window_size)
        recent = (self._offered_arrival_count(60) * duration /
                  constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
        retained = (1.15 * self._offered_arrival_count(300) * duration /
                    constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS)
        return max(recent, retained)

    def _arrival_compatibility_work(
        self,
        arrival_work: float,
        allocator_attributed_work: float,
    ) -> list[tuple[int, tuple[str, ...], float]]:
        """Shape only the offered-arrival work not already attributed.

        Offered-arrival counters are the deduplicated magnitude authority.
        Accepted-arrival profiles and the current queued gauge are used only
        as compatibility/priority distribution evidence, so retries cannot
        inflate total work here. The queued gauge covers requests that cannot
        be admitted until a compatible card exists. Both sources stay in
        request-count units because they shape the same offered-arrival counter:
        every request is recorded there before admission.
        """
        arrival_gap = max(0.0, arrival_work - allocator_attributed_work)
        if arrival_gap <= 0:
            return []

        duration = self.effective_request_duration_seconds
        offered_counts_complete = (duration is not None and
                                   self._unique_job_arrivals_60s is not None and
                                   self._unique_job_arrivals_300s is not None
                                   and
                                   self._headerless_arrivals_60s is not None and
                                   self._headerless_arrivals_300s is not None)
        window_seconds = self.qps_window_size
        if offered_counts_complete:
            assert duration is not None
            recent_work = (self._offered_arrival_count(60) * duration /
                           constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
            retained_work = (1.15 * self._offered_arrival_count(300) *
                             duration /
                             constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS)
            if retained_work > recent_work:
                window_seconds = constants.LB_OFFERED_ARRIVAL_WINDOW_SECONDS

        cutoff = time.time() - window_seconds
        evidence = [
            (int(profile['priority']),
             tuple(profile['compatible_accelerators']), float(profile['count']))
            for profile in self.compatibility_profiles
            if profile['timestamp'] >= cutoff and float(profile['count']) > 0
        ]
        evidence.extend(
            (int(profile['priority']),
             tuple(profile['compatible_accelerators']), float(profile['count']))
            for profile in self.queued_compatibility_profiles
            if float(profile['count']) > 0)
        evidence_total = sum(work for _, _, work in evidence)
        if evidence_total <= 0:
            # Compatibility-unknown work may hold the aggregate target, but
            # it must never authorize a guessed exact-card launch.
            return []
        scale = arrival_gap / evidence_total
        return [(priority, compatible, work * scale)
                for priority, compatible, work in evidence]

    def _adaptive_sample_is_fresh(self, observed_at: float | None) -> bool:
        if observed_at is None:
            return False
        age = time.time() - observed_at
        # Tolerate a small negative age from clock adjustment rather than
        # discarding an otherwise usable estimate.
        return -60.0 <= age <= constants.AUTOSCALER_ADAPTIVE_SAMPLE_MAX_AGE_SECONDS

    @property
    def effective_request_duration_seconds(self) -> float | None:
        """Measured request duration, falling back to configuration.

        Configuration is a hand-set estimate that silently mis-sizes every
        target it feeds once the workload drifts. A measured duration backed
        by enough fresh completions is strictly better evidence, so it wins
        while it holds; otherwise the configured value stands.
        """
        if (self.adaptive_demand_estimation and
                self._measured_duration_seconds is not None and
                self._measured_duration_samples
                >= constants.AUTOSCALER_ADAPTIVE_DURATION_MIN_SAMPLES and
                self._adaptive_sample_is_fresh(self._measured_duration_at)):
            return self._measured_duration_seconds
        return self.expected_request_duration_seconds

    @property
    def configured_provision_lead_seconds(self) -> float:
        """Resolve the configured seed, including the 'auto' sentinel.

        'auto' (the default) means the service has not declared a lead and
        wants one measured. Until it has, assume the order of magnitude
        every supported cloud actually takes to provision a GPU replica:
        assuming zero would size a young service's first bursts as if
        capacity were instant.
        """
        configured = self.initial_provision_lead_time_seconds
        if isinstance(configured,
                      (int, float)) and not isinstance(configured, bool):
            return float(configured)
        return constants.AUTOSCALER_DEFAULT_PROVISION_LEAD_SECONDS

    @property
    def effective_provision_lead_seconds(self) -> float:
        """Observed launch-to-ready quantile, falling back to the seed."""
        if (self.adaptive_demand_estimation and
                len(self._provision_lead_samples)
                >= constants.AUTOSCALER_ADAPTIVE_LEAD_MIN_SAMPLES and
                self._adaptive_sample_is_fresh(self._provision_lead_at)):
            ordered = sorted(self._provision_lead_samples)
            index = min(
                len(ordered) - 1,
                int(constants.AUTOSCALER_ADAPTIVE_LEAD_QUANTILE * len(ordered)))
            return ordered[index]
        return self.configured_provision_lead_seconds

    def _ingest_prediction_time_history(self,
                                        prediction_time_history: Any) -> None:
        """Fold newly completed request durations into the EMA.

        The load balancer reports per-minute cumulative histograms and keeps
        re-reporting a bucket until the controller durably accepts it, so
        only the positive delta against what this estimator already folded
        in may contribute.
        """
        if not isinstance(prediction_time_history, dict):
            return
        if (prediction_time_history.get('histogram_version')
                != constants.LB_PREDICTION_TIME_HISTOGRAM_VERSION):
            # Bucket arrays are interpreted by index; a different version
            # is not comparable and is dropped rather than guessed.
            return
        buckets = prediction_time_history.get('buckets')
        if not isinstance(buckets, list):
            return
        bounds = constants.LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS
        total_new = 0
        weighted_new = 0.0
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            bucket_start = bucket.get('bucket_start')
            outcome_counts = bucket.get('outcome_counts')
            if (not isinstance(bucket_start, int) or
                    isinstance(bucket_start, bool) or
                    not isinstance(outcome_counts, dict)):
                continue
            # Only successful requests describe how long serving a request
            # occupies a slot. A fast failure would drag the estimate down
            # and undersize the fleet.
            counts = outcome_counts.get('succeeded')
            if not isinstance(counts, list):
                continue
            seen = self._prediction_counts_seen.setdefault(
                bucket_start, [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT)
            for index, count in enumerate(counts):
                if index >= len(seen):
                    break
                if (not isinstance(count, int) or isinstance(count, bool) or
                        count <= seen[index]):
                    continue
                delta = count - seen[index]
                seen[index] = count
                representative = _prediction_bucket_representative(
                    index, bounds)
                total_new += delta
                weighted_new += delta * representative
        if total_new <= 0:
            return
        self._prune_prediction_counts_seen()
        sample = weighted_new / total_new
        alpha = constants.AUTOSCALER_ADAPTIVE_DURATION_EMA_ALPHA
        if self._measured_duration_seconds is None:
            self._measured_duration_seconds = sample
        else:
            self._measured_duration_seconds = (
                (1.0 - alpha) * self._measured_duration_seconds +
                alpha * sample)
        self._measured_duration_samples += total_new
        self._measured_duration_at = time.time()

    def _prune_prediction_counts_seen(self) -> None:
        """Bound the per-bucket dedup ledger to the freshness window."""
        cutoff = (time.time() -
                  constants.AUTOSCALER_ADAPTIVE_SAMPLE_MAX_AGE_SECONDS)
        for bucket_start in list(self._prediction_counts_seen):
            if bucket_start < cutoff:
                del self._prediction_counts_seen[bucket_start]

    def _observe_provision_leads(
            self, replica_infos: list['replica_managers.ReplicaInfo']) -> None:
        """Sample launch-to-ready for replicas that just became ready."""
        live_ids = set()
        for info in replica_infos:
            replica_id = info.replica_id
            live_ids.add(replica_id)
            if replica_id in self._provision_lead_seen_replica_ids:
                continue
            created_at = info.created_at
            ready_at = info.status_property.first_ready_time
            if (not isinstance(created_at,
                               (int, float)) or isinstance(created_at, bool) or
                    not isinstance(ready_at,
                                   (int, float)) or isinstance(ready_at, bool)):
                continue
            lead = ready_at - created_at
            if lead <= 0:
                # -1 is the never-ready sentinel; a non-positive span is
                # not a launch measurement.
                continue
            self._provision_lead_seen_replica_ids.add(replica_id)
            self._provision_lead_samples.append(float(lead))
            del self._provision_lead_samples[:-constants.
                                             AUTOSCALER_ADAPTIVE_LEAD_SAMPLE_CAP]
            self._provision_lead_at = time.time()
        # Terminated rows can never be sampled again, so the ledger tracks
        # the live fleet rather than growing for the service's lifetime.
        self._provision_lead_seen_replica_ids &= live_ids

    def _adaptive_scale_up_active(self) -> bool:
        return (self.adaptive_scale_up is not None and
                self._adaptive_until is not None and
                time.monotonic() < self._adaptive_until)

    def _rejected_work(self) -> float:
        """Convert the retained rejection population to concurrent work."""
        duration = self.effective_request_duration_seconds
        if self.replica_unit != 'logical' or duration is None:
            return float(self._rejected_in_window)
        retained_work = (self._rejected_in_window * duration /
                         constants.LB_REJECT_WINDOW_SECONDS)
        if self._rejected_in_recent_window is None:
            return retained_work
        recent_work = (self._rejected_in_recent_window * duration /
                       self.qps_window_size)
        return max(retained_work, recent_work)

    def _latest_committed_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Latest-version planned slots, from every launch origin."""
        return sum(
            max(0, self._committed_capacity(info))
            for info in replica_infos
            if (not info.is_terminal and info.version == self.latest_version and
                info.status_property.is_scale_down is not True))

    def _latest_demand_owned_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Latest-version planned slots whose launch origin was demand.

        ``reserved_fill`` is launch-origin attribution, not placement-cost
        provenance. A demand launch remains demand-owned when it lands on a
        zero-cost location. Legacy rows missing the additive flag default to
        demand-owned, which is the conservative compatibility direction.
        """
        return sum(
            max(0, int(self._replica_capacity(info)))
            for info in replica_infos
            if (not info.is_terminal and info.version == self.latest_version and
                info.status_property.is_scale_down is not True and
                not info.reserved_fill))

    def _total_ready_demand_owned_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Ready demand-owned slots across every active rollout version."""
        return sum(
            max(0, int(self._replica_capacity(info)))
            for info in replica_infos
            if (info.is_ready and not info.is_terminal and info.status_property.
                is_scale_down is not True and not info.reserved_fill))

    def _nonterminal_committed_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Planned slots across every non-retiring version and launch origin.

        This is the base for the aggregate target CEILING, not for the wave
        rate. During a rolling update the serving fleet can be entirely
        old-version, and a latest-only ceiling pins the adopted target
        below the fleet that is already saturated: growing to meet demand
        becomes gated behind version replacement progress (observed live at
        raw target 1000, adopted 50, fleet 156). Replacement pacing itself
        stays on the latest-version rate base.
        """
        return sum(
            max(0, self._committed_capacity(info))
            for info in replica_infos
            if (not info.is_terminal and
                info.status_property.is_scale_down is not True))

    def _limit_logical_scale_up(
        self,
        raw_target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Bound one demand-driven target increase to a configured wave."""
        budget = self._logical_scale_up_budget(replica_infos)
        if budget is None:
            return raw_target
        if budget == 0:
            return self.target_num_replicas
        committed = self._nonterminal_committed_logical_capacity(replica_infos)
        return max(self.target_num_replicas, min(raw_target,
                                                 committed + budget))

    def _logical_scale_up_budget(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int | None:
        """Return new or retained slot authority for this reconciliation."""
        self._logical_actuation_wave_is_new = False
        if (self.replica_unit != 'logical' or
                self.max_scale_up_rate_percentage is None):
            return None
        assert self.scale_up_rate_min_replicas is not None
        assert self.scale_up_rate_period_seconds is not None
        now = time.time()
        if (self._last_scale_up_wave_at is not None and
                now - self._last_scale_up_wave_at
                < self.scale_up_rate_period_seconds):
            if self._logical_scale_up_wave_ceiling is None:
                # Dynamic handoff deliberately carries the timer but not its
                # version-specific ceiling. Preserve a fail-closed cooldown
                # for the remainder of that window.
                return 0
            committed = self._nonterminal_committed_logical_capacity(
                replica_infos)
            return max(0, self._logical_scale_up_wave_ceiling - committed)
        # The wave RATE stays on latest-version capacity: it also paces
        # rollout replacement launches, and ramping a new version from its
        # own committed capacity is a deliberate contract. Only the target
        # ceiling below counts the whole fleet.
        committed = self._latest_committed_logical_capacity(replica_infos)
        rate_percentage = self.max_scale_up_rate_percentage
        min_replicas = self.scale_up_rate_min_replicas
        if self._adaptive_scale_up_active():
            assert self.adaptive_scale_up is not None
            rate_percentage = self.adaptive_scale_up[
                'max_scale_up_rate_percentage']
            min_replicas = self.adaptive_scale_up['scale_up_rate_min_replicas']
        assert rate_percentage is not None
        assert min_replicas is not None
        self._logical_actuation_wave_is_new = True
        return max(min_replicas, math.ceil(committed * rate_percentage / 100.0))

    def _record_logical_scale_up_wave(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        launch_budget: int | None,
    ) -> None:
        """Open a new wave without burning retained cooldown authority.

        The ceiling base is derived here rather than accepted from callers:
        the retained-cooldown branch of _logical_scale_up_budget spends this
        ceiling against the same all-version base, and a caller passing a
        latest-version base would leave the ceiling below that subtrahend,
        silently zeroing retained authority for the rest of the cooldown.
        """
        if (launch_budget is None or launch_budget <= 0 or
                self._logical_actuation_wave_started):
            return
        if self._logical_actuation_wave_is_new:
            committed = self._nonterminal_committed_logical_capacity(
                replica_infos)
            self._last_scale_up_wave_at = time.time()
            self._logical_scale_up_wave_ceiling = committed + launch_budget
        self._logical_actuation_wave_started = True

    def _adopt_scale_up_target(
        self,
        raw_target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> None:
        old_target = self.target_num_replicas
        committed = (self._nonterminal_committed_logical_capacity(replica_infos)
                     if self.replica_unit == 'logical' else 0)
        self.target_num_replicas = self._limit_logical_scale_up(
            raw_target, replica_infos)
        # Only an increase that requires capacity beyond what is already
        # committed consumes the wave timer. Raising a recovered target inside
        # an already-live fleet does not delay the next real launch wave.
        if (self.max_scale_up_rate_percentage is not None and
                self.target_num_replicas > old_target and
                self.target_num_replicas > committed):
            if self.replica_unit == 'logical':
                launch_budget = self._logical_actuation_wave_budget
                if launch_budget is None:
                    launch_budget = self.target_num_replicas - committed
                self._record_logical_scale_up_wave(replica_infos, launch_budget)
            else:
                self._last_scale_up_wave_at = time.time()
        if self.target_num_replicas > old_target:
            self._pending_retention_floor = None
            self._pending_capacity_at_adoption = 0
            self._pending_budget_spent = 0

    def _limit_logical_scale_down(
        self,
        raw_target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        if self.replica_unit != 'logical':
            return raw_target
        committed = self._latest_demand_owned_logical_capacity(replica_infos)
        allowance = max(
            1,
            math.ceil(committed * self.max_scale_down_rate_percentage / 100.0))
        self._last_scale_down_allowance = allowance
        return max(raw_target, committed - allowance)

    def _provisioning_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        provisioning_statuses = {
            serve_state.ReplicaStatus.PENDING,
            serve_state.ReplicaStatus.PROVISIONING,
            serve_state.ReplicaStatus.STARTING,
        }
        return sum(
            self._committed_capacity(info)
            for info in replica_infos
            if (not info.is_terminal and info.version == self.latest_version and
                info.status in provisioning_statuses and
                info.status_property.is_scale_down is not True))

    def _provisioning_demand_owned_logical_capacity(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> int:
        """Demand-owned subset of provisioning logical capacity."""
        provisioning_statuses = {
            serve_state.ReplicaStatus.PENDING,
            serve_state.ReplicaStatus.PROVISIONING,
            serve_state.ReplicaStatus.STARTING,
        }
        return sum(
            self._committed_capacity(info)
            for info in replica_infos
            if (not info.is_terminal and info.version == self.latest_version and
                info.status in provisioning_statuses and info.status_property.
                is_scale_down is not True and not info.reserved_fill))

    def _adopt_scale_down_target(
        self,
        raw_target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> None:
        if self.replica_unit != 'logical':
            self.target_num_replicas = raw_target
            return
        self.target_num_replicas = self._limit_logical_scale_down(
            raw_target, replica_infos)
        provisioning = self._provisioning_demand_owned_logical_capacity(
            replica_infos)
        allowance = (max(
            1,
            math.ceil(provisioning * self.max_scale_down_rate_percentage /
                      100.0)) if provisioning > 0 else 0)
        self._last_pending_allowance = allowance
        self._pending_capacity_at_adoption = provisioning
        self._pending_retention_floor = max(0, provisioning - allowance)
        self._pending_budget_spent = 0

    def _reset_downscale_hysteresis(self) -> None:
        self.downscale_counter = 0
        self._downscale_started_at = None

    def _downscale_hysteresis_elapsed(self) -> bool:
        """Whether this lower-target observation completes its delay.

        Logical concurrency policies use elapsed monotonic time. Other
        concurrency modes retain the legacy decision-count behavior.
        """
        self.downscale_counter += 1
        if self.replica_unit != 'logical':
            return self.downscale_counter >= self.scale_down_threshold
        now = time.monotonic()
        if self._downscale_started_at is None:
            # Preserve the established one-tick default: the first lower
            # observation represents the nominal decision interval that just
            # elapsed. Further progress is real monotonic time, never loop
            # counts, so slow large-fleet ticks cannot stretch the duration.
            initial_credit = min(
                self.downscale_delay_seconds,
                float(constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS))
            self._downscale_started_at = now - initial_credit
            # The current raw target already incorporates every report seen
            # before this quiet interval. Only later positive deltas may veto
            # its acceptance.
            self._pressure_latched = False
            self._pressure_reasons = ()
        return (now - self._downscale_started_at
                >= self.downscale_delay_seconds)

    def _downscale_elapsed_seconds(self) -> float:
        if self._downscale_started_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self._downscale_started_at)

    def _consume_downscale_pressure_veto(self) -> bool:
        if not self._pressure_latched:
            self._downscale_veto_reason = None
            self._downscale_veto_streak = 0
            return False
        if self._downscale_veto_streak >= _MAX_CONSECUTIVE_DOWNSCALE_VETOES:
            # The latch is magnitude-blind: under trickle traffic a tiny
            # positive delta re-arms it nearly every decision tick, and an
            # unbounded veto would defer downscale forever.
            # After the cap, let the downscale proceed; a genuine burst
            # raises the raw target and exits the downscale episode via
            # the upscale branch anyway.
            self._downscale_veto_reason = None
            self._pressure_latched = False
            self._pressure_reasons = ()
            self._downscale_veto_streak = 0
            return False
        self._downscale_veto_streak += 1
        self._downscale_veto_reason = ','.join(self._pressure_reasons)[:128]
        self._pressure_latched = False
        self._pressure_reasons = ()
        return True

    def _outstanding_work(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'] | None = None,
    ) -> float:
        """Outstanding jobs per the latest report (gauges, one snapshot).

        A job can transiently appear in both queue_depth (one sync) and
        rejected_in_window (a later sync) -- at most a 2x count per job,
        absorbed by hysteresis (accepted in the plan).
        """
        queue_work, rejected, unknown_floor = self._outstanding_work_parts(
            replica_infos)
        # These two are observability fields owned by the decision tick (see
        # info()). The pure variant assigns nothing, so the reserved-fill
        # poller thread can sample outstanding work without clobbering them.
        self._weighted_queue_work = queue_work
        self._rejected_concurrency = rejected
        assert self._in_flight_by_replica_id is not None
        return float(
            sum(self._in_flight_by_replica_id.values()) + queue_work +
            rejected + unknown_floor)

    def _outstanding_work_parts(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'] | None = None,
    ) -> tuple[float, float, float]:
        """(queue work, rejected work, unknown-occupancy floor). Pure."""
        assert self._in_flight_by_replica_id is not None
        unknown_floor = 0.0
        if self._unknown_in_flight_replica_ids:
            infos_by_id = {
                info.replica_id: info
                for info in (replica_infos or [])
                if not info.is_terminal
            }
            default_capacity = self.target_concurrency_per_replica
            if self.replica_unit == 'logical':
                default_capacity = self._effective_logical_capacity_per_gpu()
            fallback_capacity = max((self._unknown_occupancy_work(info)
                                     for info in infos_by_id.values()),
                                    default=default_capacity)
            original_unknown_floor = 0.0
            replacement_unknown_floor = 0.0
            for replica_id in self._unknown_in_flight_replica_ids:
                info = infos_by_id.get(replica_id)
                if info is None:
                    # Defensive fallback for transient list/cache skew: use
                    # the best live capacity rather than silently shrinking a
                    # potentially multi-GPU unknown replica to one GPU.
                    original_unknown_floor += fallback_capacity
                else:
                    capacity = self._unknown_occupancy_work(info)
                    if info.unknown_capacity_replacement is True:
                        replacement_unknown_floor += capacity
                    else:
                        original_unknown_floor += capacity
            # A degraded replacement wave overlaps uncertain originals. If
            # both sides are unobservable, counting their floors additively
            # creates recursive phantom demand. The larger side is the safe
            # possible-work floor; when either side recovers, the other still
            # protects its own capacity.
            # Repeated fractional utilization capacities (for example ten
            # 0.9-slot floors) can accumulate a positive binary-float tail.
            # Normalize only this modeled floor so ceil(work / capacity) does
            # not manufacture a slot from arithmetic noise.
            unknown_floor = round(
                max(original_unknown_floor, replacement_unknown_floor), 12)
        return (self._queue_work(), self._rejected_work(), unknown_floor)

    def _unknown_occupancy_work(self,
                                info: 'replica_managers.ReplicaInfo') -> float:
        """Work floor that preserves an occupancy-unknown replica.

        Unknown occupancy is a retention signal, not observed demand. In
        logical mode, express it at the configured utilization-adjusted work
        capacity so dividing by that same capacity preserves exactly the
        materialized slots. Charging the raw saturation capacity here would
        apply utilization headroom a second time and turn a controller/LB
        handoff that marks the whole fleet unknown into a phantom scale-up.
        Physical-backend mode retains its existing raw-capacity semantics.
        """
        capacity = self._replica_capacity(info)
        if self.replica_unit == 'logical':
            capacity *= self._effective_logical_capacity_per_gpu()
        return capacity

    def _fixed_concurrency_work_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> tuple[dict[str, float], float, bool]:
        """Split fixed work into card-retention work and flexible overflow.

        Running and occupancy-unknown work cannot be moved off the replica
        that already owns it, so it protects materialized capacity on that
        replica's exact card. Work above the card's materialized serving
        capacity is already being served through temporary oversubscription;
        treating that excess as an exact-card capacity deficit would cold
        start the same card even when a cheaper compatible card can absorb new
        work. Return that excess separately so the allocator can preserve the
        aggregate work as a flexible compatibility profile.
        """
        assert self._in_flight_by_replica_id is not None
        infos_by_id = {
            info.replica_id: info
            for info in replica_infos
            if not info.is_terminal
        }
        configured_by_name = {
            card.casefold(): card
            for card in self._configured_cards_from_profiles()
        }
        fixed: dict[str, float] = {}
        complete = True

        def add(replica_id: int, work: float, destination: dict[str,
                                                                float]) -> None:
            nonlocal complete
            info = infos_by_id.get(replica_id)
            if info is None:
                complete = False
                return
            if _replica_is_retiring_card_supply(info):
                # The row remains in aggregate outstanding work until its
                # bounded drain completes, but replacing that work on the
                # retiring row's exact card would turn a graceful retirement
                # into a cold same-card relaunch.
                return
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = configured_by_name.get(raw_card.casefold())
            if card is None:
                complete = False
                return
            destination[card] = destination.get(card, 0.0) + max(0.0, work)

        for replica_id, count in self._in_flight_by_replica_id.items():
            add(replica_id, float(count), fixed)

        original_unknown: dict[str, float] = {}
        replacement_unknown: dict[str, float] = {}
        for replica_id in self._unknown_in_flight_replica_ids:
            info = infos_by_id.get(replica_id)
            if info is None:
                complete = False
                continue
            destination = (replacement_unknown
                           if info.unknown_capacity_replacement is True else
                           original_unknown)
            add(replica_id, self._unknown_occupancy_work(info), destination)
        # Mirror _outstanding_work(): an uncertain bounded replacement wave
        # overlaps its original, so only the larger side contributes.
        unknown = (replacement_unknown if sum(replacement_unknown.values())
                   > sum(original_unknown.values()) else original_unknown)
        for card, work in unknown.items():
            fixed[card] = fixed.get(card, 0.0) + work

        materialized_work_capacity = {
            card: 0.0 for card in configured_by_name.values()
        }
        materialized_statuses = {
            serve_state.ReplicaStatus.READY,
            serve_state.ReplicaStatus.NOT_READY,
        }
        for info in replica_infos:
            if (info.is_terminal or info.status not in materialized_statuses or
                    _replica_is_retiring_card_supply(info)):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            materialized_card = configured_by_name.get(raw_card.casefold())
            if materialized_card is None:
                continue
            capacity = self._replica_capacity(info)
            if self.replica_unit == 'logical':
                capacity *= self._effective_logical_capacity_per_gpu()
            materialized_work_capacity[materialized_card] += max(
                0.0, float(capacity))

        flexible_overflow = 0.0
        capped_fixed: dict[str, float] = {}
        for card, work in fixed.items():
            retained = min(max(0.0, work),
                           materialized_work_capacity.get(card, 0.0))
            if retained > 0:
                capped_fixed[card] = retained
            flexible_overflow += max(0.0, work - retained)
        return capped_fixed, flexible_overflow, complete

    def _rejected_compatibility_work(
            self) -> list[tuple[int, tuple[str, ...], float]]:
        """Distribute aggregate rejection work without changing its total."""
        raw: list[tuple[int, tuple[str, ...], float]] = []
        for profile in self.rejected_compatibility_profiles:
            count = int(profile['count'])
            duration = self.effective_request_duration_seconds
            if self.replica_unit != 'logical' or duration is None:
                work = float(count)
            else:
                retained = (count * duration /
                            constants.LB_REJECT_WINDOW_SECONDS)
                recent = (int(profile.get('recent_count', 0)) * duration /
                          self.qps_window_size)
                work = max(retained, recent)
            raw.append((int(profile['priority']),
                        tuple(profile['compatible_accelerators']), work))
        aggregate = self._rejected_work()
        raw_total = sum(work for _, _, work in raw)
        if raw_total <= 0:
            return []
        scale = aggregate / raw_total
        return [(priority, compatible, work * scale)
                for priority, compatible, work in raw]

    def _calculate_concurrency_target_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        *,
        target_ceiling: int | None = None,
        min_replicas_override: int | None = None,
        purpose: capacity_planning.CapacityPlanningPurpose = (
            capacity_planning.CapacityPlanningPurpose.DEMAND_ATTRIBUTION),
    ) -> _CompatibilityTargetResult:
        """Run the pure planner for the ordinary process-local path."""
        if purpose not in (
                capacity_planning.CapacityPlanningPurpose.DEMAND_ATTRIBUTION,
                capacity_planning.CapacityPlanningPurpose.LOCAL_ACTUATION):
            raise ValueError('Ordinary capacity planning accepts only demand '
                             'attribution or local actuation.')
        reuse_existing_supply = (
            purpose
            is capacity_planning.CapacityPlanningPurpose.LOCAL_ACTUATION)
        use_free_reserved = (
            purpose
            is capacity_planning.CapacityPlanningPurpose.DEMAND_ATTRIBUTION)
        kueue_snapshot = self._kueue_capacity_by_replica_id_for_tick
        if kueue_snapshot is not None and (
                set(kueue_snapshot) -
            {info.replica_id for info in replica_infos} or not all(
                isinstance(value, kueue_lane_capacity.KueueReplicaCapacityClass)
                for value in kueue_snapshot.values())):
            return capacity_planning.incomplete_capacity_plan(
                source_generation=self._reconcile_generation)
        configured_cards = self._configured_cards_from_profiles()
        if not configured_cards:
            if purpose is (capacity_planning.CapacityPlanningPurpose.
                           DEMAND_ATTRIBUTION):
                self.warm_retention_target_by_accelerator = {}
            return capacity_planning.incomplete_capacity_plan(
                source_generation=self._reconcile_generation)
        if self.replica_unit == 'logical':
            capacity_per_card = {
                card: self._effective_logical_capacity_per_gpu()
                for card in configured_cards
            }
        else:
            capacity_per_card = {
                card: (self.target_concurrency_per_replica *
                       self._configured_gpu_count(card)
                      ) for card in configured_cards
            }
        ready_zero_cost = {card: 0 for card in configured_cards}
        ready = {card: 0 for card in configured_cards}
        provisioning = {card: 0 for card in configured_cards}
        existing_zero_cost = {card: 0 for card in configured_cards}
        existing_paid = {card: 0 for card in configured_cards}
        charged_paid_gpu_units = sum(
            max(1, int(info.planned_capacity))
            for info in replica_infos
            if info.is_zero_cost is not True and info.status_property.
            sky_down_status is not common_utils.ProcessStatus.SUCCEEDED)
        canonical_by_name = {card.casefold(): card for card in configured_cards}
        for info in replica_infos:
            if (info.is_terminal or _replica_is_retiring_card_supply(info) or
                    info.version != self.latest_version):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = canonical_by_name.get(raw_card.casefold())
            if card is None:
                continue
            width = (max(0, self._committed_capacity(info))
                     if self.replica_unit == 'logical' else 1)
            if width == 0:
                continue
            if info.is_zero_cost is True:
                existing_zero_cost[card] += width
            else:
                existing_paid[card] += width
            if info.is_ready:
                ready[card] += width
                if info.is_zero_cost is True:
                    ready_zero_cost[card] += width
            else:
                provisioning[card] += width

        floors = {
            card.casefold(): int(floor)
            for card, floor in self.min_replicas_by_accelerator.items()
        }
        free_reserved = (dict(self.free_reserved_slots_by_accelerator)
                         if use_free_reserved else {})
        if self.replica_unit == 'logical':
            free_reserved = {
                card: count * self._configured_gpu_count(card)
                for card, count in free_reserved.items()
            }
        reservation_configured = bool(free_reserved)
        reservation = capacity_planning.ReservationPlanningInput(
            gate_policy=(
                capacity_planning.ReservationGatePolicy.UNGATED
                if reservation_configured else
                capacity_planning.ReservationGatePolicy.NOT_CONFIGURED),
            evidence_state=(
                capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED
                if reservation_configured else
                capacity_planning.ReservationEvidenceState.NOT_APPLICABLE),
            authenticated_capacity=(capacity_planning.AcceleratorCapacity.
                                    from_mapping(free_reserved)),
            eligible_capacity=(capacity_planning.AcceleratorCapacity.
                               from_mapping(free_reserved)),
            pending_zero_cost_capacity=capacity_planning.AcceleratorCapacity(),
            existing_zero_cost_capacity=(capacity_planning.AcceleratorCapacity.
                                         from_mapping(existing_zero_cost)),
            existing_paid_capacity=(capacity_planning.AcceleratorCapacity.
                                    from_mapping(existing_paid)),
            charged_paid_gpu_units=charged_paid_gpu_units,
            evidence_fingerprint=('0' * 64 if reservation_configured else ''))
        ceiling = (self.max_replicas if target_ceiling is None else min(
            self.max_replicas, target_ceiling))
        cold_order = self._cold_paid_card_order(configured_cards)
        prospective_paid_order = self._prospective_paid_card_order(
            configured_cards)
        planning_time = time.time()
        deadline_input = self._deadline_planning_input_for_supply(
            replica_infos,
            configured_cards,
            free_reserved,
            planning_time=planning_time,
            kueue_capacity_by_replica_id=kueue_snapshot)

        profiles: list[_CompatibilityWorkProfile] = []
        explicit_profiles: list[_CompatibilityWorkProfile] = []
        if deadline_input is None:
            profiles, explicit_profiles = self._queued_compatibility_work(
                configured_cards)
        paid_profiles = list(profiles)
        default_compatible = tuple(configured_cards)
        rejected_profiles = self._rejected_compatibility_work()
        profiles.extend(rejected_profiles)
        explicit_profiles.extend(rejected_profiles)
        paid_profiles.extend(rejected_profiles)
        rejected_profile_total = sum(work for _, _, work in rejected_profiles)
        rejected_total = self._rejected_work()
        if rejected_total > rejected_profile_total:
            default_rejected_profile = (constants.LB_REQUEST_PRIORITY_MIN,
                                        default_compatible,
                                        rejected_total - rejected_profile_total)
            profiles.append(default_rejected_profile)
            paid_profiles.append(default_rejected_profile)
        retention_fixed, flexible_fixed_overflow, attribution_complete = (
            self._fixed_concurrency_work_by_accelerator(replica_infos))
        allocation_fixed = retention_fixed
        explicit_fixed = retention_fixed
        if (self.replica_unit == 'logical' and
                self._compatibility_demand_complete):
            # Running work is physically non-preemptive but does not make its
            # serving card the owner of flexible demand. Reuse the bounded
            # accepted-arrival histogram as compatibility evidence for the
            # current in-flight population. When that history has aged out,
            # the protocol default remains all configured cards; warm
            # retention and the supply-aware actuation pass still keep the
            # actual serving cards until their work drains. The allocation
            # result marks that fallback as insufficient proof for a
            # mixed-version cross-card replacement.
            fixed_work = (sum(retention_fixed.values()) +
                          flexible_fixed_overflow)
            allocation_fixed = {}
            explicit_fixed = {}
            evidence = [(int(profile['priority']),
                         tuple(profile['compatible_accelerators']),
                         float(profile['count']))
                        for profile in self.compatibility_profiles
                        if float(profile['count']) > 0]
            evidence_total = sum(work for _, _, work in evidence)
            if fixed_work > 0 and evidence_total > 0:
                scale = fixed_work / evidence_total
                scaled_evidence = [(priority, compatible, work * scale)
                                   for priority, compatible, work in evidence]
                profiles.extend(scaled_evidence)
                explicit_profiles.extend(scaled_evidence)
                paid_profiles.extend(scaled_evidence)
            elif fixed_work > 0:
                profiles.append((constants.LB_REQUEST_PRIORITY_MIN,
                                 default_compatible, fixed_work))
        elif flexible_fixed_overflow > 0:
            profiles.append((constants.LB_REQUEST_PRIORITY_MIN,
                             default_compatible, flexible_fixed_overflow))
        if self.replica_unit == 'logical' and self._fresh_for_tick():
            allocator_attributed_work = (sum(allocation_fixed.values()) +
                                         sum(work for _, _, work in profiles))
            arrival_work = self._arrival_work()
            arrival_profiles = self._arrival_compatibility_work(
                arrival_work, allocator_attributed_work)
            profiles.extend(arrival_profiles)
            explicit_profiles.extend(arrival_profiles)
            paid_profiles.extend(arrival_profiles)
        requested_minimum = min(
            self.min_replicas if min_replicas_override is None else
            min_replicas_override, ceiling)
        demand_minimum = (
            min(self.target_num_replicas, ceiling) if purpose
            is capacity_planning.CapacityPlanningPurpose.LOCAL_ACTUATION else
            requested_minimum)

        def _typed_profiles(
            values: list[tuple[int, tuple[str, ...], float]]
        ) -> tuple[capacity_planning.CompatibilityDemand, ...]:
            return tuple(
                capacity_planning.CompatibilityDemand(
                    sequence=sequence,
                    priority=priority,
                    compatible_accelerators=compatible,
                    work=work)
                for sequence, (priority, compatible, work) in enumerate(values)
                if work > 0)

        snapshot = capacity_planning.CapacityPlanningSnapshot(
            source_generation=self._reconcile_generation,
            service_version=self.latest_version,
            configured_accelerators=tuple(configured_cards),
            capacity_unit=(capacity_planning.CapacityUnit.LOGICAL_GPU
                           if self.replica_unit == 'logical' else
                           capacity_planning.CapacityUnit.PHYSICAL_BACKEND),
            backend_num_nodes=(1 if self.replica_unit == 'logical' else
                               self.backend_num_nodes),
            physical_gpu_width_by_accelerator=(
                capacity_planning.AcceleratorCapacity.from_mapping({
                    card: self._configured_gpu_count(card)
                    for card in configured_cards
                })),
            capacity_per_accelerator=(capacity_planning.AcceleratorWork.
                                      from_mapping(capacity_per_card)),
            floors=capacity_planning.AcceleratorCapacity.from_mapping({
                card: int(floors.get(card.casefold(), 0))
                for card in configured_cards
            }),
            minimum_capacity=demand_minimum,
            paid_minimum_capacity=min(self.min_replicas, ceiling),
            actuation_minimum_capacity=requested_minimum,
            maximum_capacity=ceiling,
            demand_profiles=_typed_profiles(profiles),
            explicit_demand_profiles=_typed_profiles(explicit_profiles),
            paid_demand_profiles=_typed_profiles(paid_profiles),
            fixed_work=capacity_planning.AcceleratorWork.from_mapping(
                allocation_fixed),
            explicit_fixed_work=capacity_planning.AcceleratorWork.from_mapping(
                explicit_fixed),
            paid_fixed_work=capacity_planning.AcceleratorWork(),
            retention_work=capacity_planning.AcceleratorWork.from_mapping(
                retention_fixed),
            ready_zero_cost=(capacity_planning.AcceleratorCapacity.from_mapping(
                ready_zero_cost)),
            ready=capacity_planning.AcceleratorCapacity.from_mapping(ready),
            provisioning=(capacity_planning.AcceleratorCapacity.from_mapping(
                provisioning)),
            reservation=reservation,
            cold_accelerator_order=tuple(cold_order),
            prospective_paid_accelerator_order=tuple(prospective_paid_order),
            planning_purpose=purpose,
            actuation_supply_policy=(
                capacity_planning.ActuationSupplyPolicy.REUSE_CURRENT_SUPPLY
                if reuse_existing_supply else
                capacity_planning.ActuationSupplyPolicy.COLD_ATTRIBUTION),
            attribution_complete=attribution_complete,
            planning_time=planning_time,
            max_live_paid_gpu_units=None,
            retirement_shelter_target=(capacity_planning.AcceleratorCapacity()),
            deadline=deadline_input)
        plan = capacity_planning.plan_capacity(snapshot)
        if purpose in (
                capacity_planning.CapacityPlanningPurpose.DEMAND_ATTRIBUTION,
                capacity_planning.CapacityPlanningPurpose.LOCAL_ACTUATION):
            self.warm_retention_target_by_accelerator = (
                plan.warm_retention_target.as_dict()
                if attribution_complete else {})
        if purpose is (
                capacity_planning.CapacityPlanningPurpose.DEMAND_ATTRIBUTION):
            self._deadline_target_by_accelerator = (
                plan.deadline_target.as_dict())
            self._deadline_infeasible_by_priority = dict(
                plan.infeasible_demand_by_priority)
            self._service_time_source_by_accelerator = dict(
                plan.service_time_sources)
            self._effective_service_time_by_accelerator = (
                {} if snapshot.deadline is None else
                snapshot.deadline.service_seconds_by_accelerator.as_dict())
            self._deadline_capacity_plan = (
                None if snapshot.deadline is None else DeadlineCapacityPlan(
                    target_by_card=plan.deadline_target.as_dict(),
                    infeasible_requests_by_priority=dict(
                        plan.infeasible_demand_by_priority)))
        return plan

    def _logical_committed_capacity_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> dict[str, int]:
        """Return latest-version committed logical slots by exact card."""
        configured_by_name = {
            card.casefold(): card
            for card in self._configured_cards_from_profiles()
        }
        committed: dict[str, int] = {}
        for info in replica_infos:
            if (info.is_terminal or info.version != self.latest_version or
                    _replica_is_retiring_card_supply(info)):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = configured_by_name.get(raw_card.casefold())
            if card is None:
                continue
            committed[card] = (committed.get(card, 0) +
                               self._committed_capacity(info))
        return committed

    def _limit_logical_actuation_transition(
        self,
        desired: dict[str, int],
        target: int,
        replica_infos: list['replica_managers.ReplicaInfo'],
        wave_budget: int | None,
    ) -> tuple[dict[str, int], int]:
        """Limit cold card migration without changing demand attribution.

        Reconstruct the transition baseline from committed supply, preferring
        cards already wanted by the fresh supply-aware actuator. Positive
        deficits then consume the exact-card wave budget. Old-card capacity is
        retained only in this private actuation map until each replacement
        wave commits; it never appears in the public cheapest-compatible
        demand map.
        """
        if sum(desired.values()) != target:
            return {}, 0
        cards = self._configured_cards_from_profiles()
        committed = self._logical_committed_capacity_by_accelerator(
            replica_infos)
        previous = self._logical_actuation_target_by_accelerator
        same_desired = (
            desired == self._logical_actuation_desired_by_accelerator)
        if same_desired and sum(previous.values()) == target:
            # Preserve a previously authorized cold wave until its pending
            # rows become committed, so a transiently dropped manager decision
            # is retried during the cooldown instead of being forgotten.
            current = {
                card: max(0, int(previous.get(card, 0))) for card in cards
            }
        else:
            current = {card: 0 for card in cards}
            remaining = max(0, target)
            # Existing capacity on a desired card is a supply reuse, not a
            # cold migration, so it does not consume a launch wave.
            for card in cards:
                kept = min(remaining, committed.get(card, 0),
                           max(0, int(desired.get(card, 0))))
                current[card] = kept
                remaining -= kept
            # Retain other committed cards as transition placeholders. They
            # are removed only as authorized replacement capacity enters the
            # map.
            for card in cards:
                available = max(0, committed.get(card, 0) - current[card])
                kept = min(remaining, available)
                current[card] += kept
                remaining -= kept

        # Supply that appeared after the prior authorization can immediately
        # replace a transition placeholder. This is reuse, not a new cold
        # wave, even when the desired profile itself is unchanged.
        reusable = 0
        for card in cards:
            moved = min(max(0,
                            desired.get(card, 0) - current.get(card, 0)),
                        max(0,
                            committed.get(card, 0) - current.get(card, 0)))
            current[card] += moved
            reusable += moved
        for card in reversed(cards):
            if reusable <= 0:
                break
            removable = max(0, current.get(card, 0) - desired.get(card, 0))
            removed = min(reusable, removable)
            current[card] -= removed
            reusable -= removed

        desired_additions = sum(
            max(0,
                desired.get(card, 0) - current.get(card, 0)) for card in cards)
        # The aggregate target may have been recovered from healthy old
        # versions or adopted by an earlier demand wave. Its exact-card map
        # must be complete even when latest-version committed supply plus this
        # tick's card budget is smaller. Completing that held target does not
        # create demand; target and max_replicas remain the hard ceilings.
        required_to_complete = max(0, target - sum(current.values()))
        additions_left = (desired_additions if wave_budget is None else max(
            0, wave_budget, required_to_complete))
        added = 0
        for card in cards:
            increase = max(0, desired.get(card, 0) - current.get(card, 0))
            accepted = min(increase, additions_left)
            current[card] = current.get(card, 0) + accepted
            additions_left -= accepted
            added += accepted

        # A target-map reduction is only an intent. The decision generator
        # still proves replacement readiness and per-card coverage before it
        # emits any idle victim, so balancing the map here is non-preemptive.
        excess = max(0, sum(current.values()) - target)
        for card in reversed(cards):
            removable = max(0, current.get(card, 0) - desired.get(card, 0))
            removed = min(excess, removable)
            current[card] -= removed
            excess -= removed
            if excess == 0:
                break

        limited = {card: count for card, count in current.items() if count > 0}
        if sum(limited.values()) != target:
            # A valid aggregate wave always leaves enough budget to cover any
            # target units not backed by committed supply. Fail closed rather
            # than publish an incomplete exact-card actuator.
            return {}, 0
        return limited, added

    def _actuation_target_by_accelerator(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> tuple[dict[str, int], bool]:
        """Revalidate logical cold launches at the adopted total target."""
        demand_target = self.target_num_replicas_by_accelerator
        if (not self._compatibility_demand_complete or
                sum(demand_target.values()) != self.target_num_replicas):
            self.warm_retention_target_by_accelerator = {}
            self.cold_launch_authority_by_accelerator = {}
            self._logical_paid_launch_target_by_accelerator = {}
            self.zero_cost_padding_target_by_accelerator = {}
            self._logical_card_transition_pending = False
            return {}, False
        final_target = self.get_final_target_num_replicas()
        cards = self._configured_cards_from_profiles()
        allocation = self._calculate_concurrency_target_by_accelerator(
            replica_infos,
            target_ceiling=final_target,
            min_replicas_override=final_target,
            purpose=(capacity_planning.CapacityPlanningPurpose.LOCAL_ACTUATION))
        desired_target = allocation.target_by_accelerator
        explicit_target = allocation.explicit_target_by_accelerator
        paid_target = allocation.paid_target_by_accelerator
        attribution_complete = allocation.card_attribution_complete
        if (not attribution_complete or
                sum(desired_target.values()) != final_target):
            self._logical_paid_launch_target_by_accelerator = {}
            self.zero_cost_padding_target_by_accelerator = {}
            self._logical_card_transition_pending = False
            return {}, False
        fresh_complete_attribution = (self._fresh_for_tick() and
                                      self._compatibility_demand_complete and
                                      attribution_complete and
                                      sum(desired_target.values())
                                      == final_target)
        canonical_by_name = {card.casefold(): card for card in cards}
        nonretiring_supply = {card: 0 for card in cards}
        # Old-version rows are provenance for the reconciler: they cannot
        # authorize a launch, but a card they still serve is mid-replacement
        # rather than gone, and must not be released as vanished capacity
        # while the rollout drains. Preempted and scale-down rows are excluded
        # on both versions for the same reason they are excluded from latest
        # supply: they must not preserve, let alone replace, their card.
        old_version_supply = {card: 0 for card in cards}
        has_active_old_version = any(
            not info.is_terminal and info.version != self.latest_version
            for info in replica_infos)
        for info in replica_infos:
            if info.is_terminal or _replica_is_retiring_card_supply(info):
                continue
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            card = canonical_by_name.get(raw_card.casefold())
            if card is None:
                continue
            width = (max(0, self._committed_capacity(info))
                     if self.replica_unit == 'logical' else 1)
            if width == 0:
                continue
            if info.version == self.latest_version:
                nonretiring_supply[card] += width
            else:
                old_version_supply[card] += width
        # Broker-reported free slots are opportunities, not materialized
        # supply. Treating them as backing here can move flexible L4 demand
        # onto A100 during a rollout. If the research slot then disappears,
        # the exact-card shortage retries on a paid A100 location. Reserved
        # fill owns those opportunities independently and carries the
        # zero-cost-only launch fence; demand actuation may reuse the card only
        # after a latest-version replica row materializes it.
        downscale_hold = (self._raw_target_num_replicas
                          < self.target_num_replicas)
        if self.replica_unit != 'logical':
            # Physical-backend scaling retains the legacy actuation contract:
            # its exact-card target is itself the launch decision, so there is
            # no separate paid-ownership channel to reconcile here.
            target = _revalidate_actuation_target(
                adopted_target=demand_target,
                desired_target=desired_target,
                nonretiring_supply=nonretiring_supply,
                configured_cards=cards,
                final_target=final_target,
                allow_adopted_reassignment=not has_active_old_version,
                allow_unbacked_adopted_reassignment=not downscale_hold,
                old_version_supply=old_version_supply)
            return target, (attribution_complete and
                            sum(target.values()) == final_target)
        # Rollout movement requires explicit compatibility evidence. In the
        # latest-only case, paid-owned headerless/minimum demand can also move
        # to its freshly allocated card. Inferred in-flight overflow and
        # generic overprovision padding remain reconciliation-only.
        allow_mixed_version_backed_reassignment = (has_active_old_version and
                                                   not downscale_hold and
                                                   fresh_complete_attribution
                                                   and bool(explicit_target))
        revalidation_desired_target = desired_target
        if fresh_complete_attribution:
            if has_active_old_version:
                reassignment_target = ({}
                                       if downscale_hold else explicit_target)
            else:
                # A downscale hold protects the adopted aggregate and its
                # unbacked exact-card retry fence.  It must not prevent fresh,
                # compatibility-owned demand from reusing a compatible
                # latest-version backend that is already materialized.  The
                # revalidator's backed-only pass makes this reassignment
                # non-launching; cold cross-card movement remains disabled
                # below for the duration of the hold.
                reassignment_target = paid_target
                if downscale_hold:
                    # The public held map has already merged the fresh
                    # no-supply placement with older exact-card slots. Keep
                    # that fresh placement as the only eligible source for a
                    # backed move; otherwise configured-card iteration could
                    # replace an unrelated held unit that lacks compatibility
                    # proof for the materialized destination.
                    fresh_source_allocation = (
                        self._calculate_concurrency_target_by_accelerator(
                            replica_infos,
                            target_ceiling=self._raw_target_num_replicas,
                            min_replicas_override=(
                                self._raw_target_num_replicas)))
                    if (fresh_source_allocation.card_attribution_complete and
                            sum(fresh_source_allocation.target_by_accelerator.
                                values()) == self._raw_target_num_replicas):
                        backed_reassignment_source = {
                            card: min(count, demand_target.get(card, 0))
                            for card, count in fresh_source_allocation.
                            paid_target_by_accelerator.items()
                            if count > 0 and demand_target.get(card, 0) > 0
                        }
                        bounded_target = (
                            _bound_materialized_reassignment_target(
                                adopted_target=demand_target,
                                desired_target=desired_target,
                                reassignment_source_by_accelerator=(
                                    backed_reassignment_source),
                                reassignment_destination_by_accelerator=(
                                    paid_target),
                                configured_cards=cards,
                                final_target=final_target))
                        if bounded_target or final_target == 0:
                            revalidation_desired_target = bounded_target
                            reassignment_target = bounded_target
                        else:
                            reassignment_target = {}
                    else:
                        reassignment_target = {}
        else:
            reassignment_target = {}
        target = _revalidate_actuation_target(
            adopted_target=demand_target,
            desired_target=revalidation_desired_target,
            nonretiring_supply=nonretiring_supply,
            configured_cards=cards,
            final_target=final_target,
            allow_adopted_reassignment=(not has_active_old_version and
                                        fresh_complete_attribution),
            allow_unbacked_adopted_reassignment=(fresh_complete_attribution and
                                                 not downscale_hold),
            allow_mixed_version_backed_reassignment=(
                allow_mixed_version_backed_reassignment),
            old_version_supply=old_version_supply,
            reassignment_target_by_accelerator=reassignment_target)
        if not target and final_target > 0:
            self._logical_paid_launch_target_by_accelerator = {}
            self.zero_cost_padding_target_by_accelerator = {}
            self._logical_card_transition_pending = False
            return {}, False
        # Stale reports may preserve a prior exact-card reconciliation fence,
        # but never authorize paid acquisition.  A downscale hold keeps its
        # adopted exact-card retry contract; otherwise the fresh supply-aware
        # placement is the sole economic authority for a new backend.
        if not fresh_complete_attribution:
            paid_launch_target: dict[str, int] = {}
        else:
            current_ownership = (explicit_target
                                 if has_active_old_version else paid_target)
            adopted_ownership = (
                self._logical_adopted_explicit_target_by_accelerator
                if has_active_old_version else
                self._logical_adopted_paid_target_by_accelerator)
            # Ownership is adopted with the aggregate target and survives a
            # transiently empty histogram until a later target adoption
            # replaces it. Current evidence may add ownership immediately;
            # neither source can exceed the retained exact-card demand map.
            paid_ownership = {
                card: min(
                    int(target.get(card, 0)),
                    max(
                        int(current_ownership.get(card, 0)),
                        min(int(adopted_ownership.get(card, 0)),
                            int(demand_target.get(card, 0)))))
                for card in cards
                if target.get(card, 0) > 0 and (current_ownership.get(
                    card, 0) > 0 or adopted_ownership.get(card, 0) > 0)
            }
            # The retained exact-card map is a reconciliation fence, not an
            # economic entitlement.  During a downscale hold it may keep an
            # older paid card until fresh compatibility evidence permits a
            # non-preemptive move.  If compatible materialized supply already
            # covers the supply-aware target, replacing a retiring instance on
            # that older card would purchase capacity the service does not
            # need.  Bound the combined current/adopted ownership by the
            # residual shortage in the fresh supply-aware allocation.  Apply
            # that bound to shortages rather than absolute card targets so an
            # unbacked retained same-card target can still retry when the
            # service is genuinely below its held aggregate capacity.
            economic_shortage = {
                card: max(
                    0,
                    int(desired_target.get(card, 0)) -
                    int(nonretiring_supply.get(card, 0))) for card in cards
            }
            residual_left = sum(economic_shortage.values())
            authorized_shortage = {card: 0 for card in cards}
            # Fresh ownership already intersects the supply-aware target, so
            # spend the residual on its exact cards first.  If a transition
            # fence has not admitted that card into `target`, fail closed
            # instead of transferring its compatibility proof to an older
            # adopted card.
            for card in cards:
                committed_on_card = int(nonretiring_supply.get(card, 0))
                current_target = min(int(target.get(card, 0)),
                                     int(current_ownership.get(card, 0)))
                current_shortage = max(0, current_target - committed_on_card)
                accepted = min(current_shortage, economic_shortage[card],
                               residual_left)
                authorized_shortage[card] += accepted
                residual_left -= accepted
            # With no fresh paid owner, the held target may still be the only
            # compatibility evidence for a genuine capacity deficit. Preserve
            # its same-card retry, bounded by the remaining economic residual.
            if not current_ownership:
                for card in cards:
                    committed_on_card = int(nonretiring_supply.get(card, 0))
                    owned_shortage = max(
                        0,
                        int(paid_ownership.get(card, 0)) - committed_on_card)
                    accepted = min(owned_shortage, residual_left)
                    authorized_shortage[card] += accepted
                    residual_left -= accepted
            bounded_paid_ownership: dict[str, int] = {}
            for card in cards:
                committed_on_card = int(nonretiring_supply.get(card, 0))
                owned_target = int(paid_ownership.get(card, 0))
                bounded_paid_target_on_card = min(
                    owned_target, committed_on_card + authorized_shortage[card])
                if bounded_paid_target_on_card > 0:
                    bounded_paid_ownership[card] = bounded_paid_target_on_card
            paid_ownership = bounded_paid_ownership
            # Paid ownership is separate from compatibility ownership. A
            # latest-only minimum or headerless queue can buy its allocator-
            # selected card; a mixed-version rollout requires explicit proof.
            # Vanished adopted units and inferred in-flight/overprovision
            # padding own no paid placement.
            paid_launch_target = {
                card: count
                for card, count in paid_ownership.items()
                if count > 0
            }
            if has_active_old_version:
                for card in cards:
                    # This is an absolute latest-version ceiling. The decision
                    # generator subtracts latest committed supply later,
                    # leaving exactly the live old-version backing as
                    # same-card retry authority. Using old supply as an
                    # incremental ceiling would stall a partially completed
                    # rollout (latest=1, old=1, target=2) at zero authority.
                    same_card_ceiling = min(
                        int(target.get(card,
                                       0)), int(demand_target.get(card, 0)),
                        int(nonretiring_supply.get(card, 0)) +
                        int(old_version_supply.get(card, 0)))
                    if same_card_ceiling > paid_launch_target.get(card, 0):
                        paid_launch_target[card] = same_card_ceiling
        self._logical_paid_launch_target_by_accelerator = {
            card: max(0, int(paid_launch_target.get(card, 0)))
            for card in cards
            if paid_launch_target.get(card, 0) > 0
        }
        wave_budget = self._logical_actuation_wave_budget
        if wave_budget is not None:
            if self._logical_actuation_wave_started:
                # Several consumers ask for the actuation map in one
                # controller tick. The shared snapshot, including the
                # overprovision allowance, is one budget rather than one
                # budget per caller.
                wave_budget = 0
            else:
                # num_overprovision is deliberately outside the traffic target
                # and historically was not charged to its demand scale-up
                # wave.
                wave_budget += max(0, final_target - self.target_num_replicas)
        limited_target, added_card_slots = (
            self._limit_logical_actuation_transition(target, final_target,
                                                     replica_infos,
                                                     wave_budget))
        if not limited_target and final_target > 0:
            self._logical_paid_launch_target_by_accelerator = {}
            self.zero_cost_padding_target_by_accelerator = {}
            self._logical_card_transition_pending = False
            return {}, False
        self._logical_actuation_target_by_accelerator = dict(limited_target)
        self._logical_actuation_desired_by_accelerator = dict(target)
        raw_padding = allocation.zero_cost_padding_target.as_dict()
        self.zero_cost_padding_target_by_accelerator = {
            card: min(count, limited_target.get(card, 0))
            for card, count in raw_padding.items()
            if count > 0 and limited_target.get(card, 0) > 0
        }
        self._logical_card_transition_pending = limited_target != target
        if (added_card_slots > 0 and
                self.max_scale_up_rate_percentage is not None and
                not self._logical_actuation_wave_started):
            self._record_logical_scale_up_wave(
                replica_infos, self._logical_actuation_wave_budget)
        return (limited_target, attribution_complete and
                sum(limited_target.values()) == final_target)

    def _set_target_num_replicas_with_concurrency_logic(
            self, replica_infos: list['replica_managers.ReplicaInfo']) -> None:
        """Recompute target_num_replicas for this tick.

        Mirrors _set_target_num_replicas_with_instance_aware_logic's
        structure: pack demand onto the existing latest replicas (largest
        first), size the remainder with the best live capacity (falling
        back to knob x 1 for an empty fleet so scale-from-zero is not
        stuck), then apply the snap/zero/hysteresis ladder.
        """
        latest_capacities = self._latest_capacities(replica_infos)
        if self.replica_unit == 'logical':
            # Public targets count GPU slots. Each slot absorbs the configured
            # amount of outstanding work; physical backend packing happens
            # later, after the manager selects exact 1/4/8-GPU placements.
            best_capacity = self._effective_logical_capacity_per_gpu()
            self._latest_committed_capacity = (
                self._latest_committed_logical_capacity(replica_infos))
            self._latest_provisioning_capacity = (
                self._provisioning_logical_capacity(replica_infos))
        else:
            best_capacity = (latest_capacities[0] if latest_capacities else
                             self.target_concurrency_per_replica)
        self._upscale_pending = False

        if not self._fresh_for_tick():
            if self.replica_unit == 'logical':
                # A signal gap cannot prove continuous low demand. Require a
                # complete fresh elapsed window after reports recover.
                self._reset_downscale_hysteresis()
                self._pressure_baseline = None
                self._pressure_latched = False
                self._pressure_reasons = ()
                self._pressure_streak = 0
                self._downscale_veto_streak = 0
            # SIGNAL GAP: the only trustworthy signal is arrivals (they
            # ride every sync). Raise-only floor, applied without
            # hysteresis -- while blind we must not delay growth, and we
            # never shrink. The one-shot snap is deliberately NOT
            # consumed here: it waits for the first recompute with fresh
            # data.
            # Prune the window here, not just in
            # collect_request_information: once syncs stop entirely,
            # collect is never called again, and unpruned timestamps
            # would keep asserting an arrival floor for arrivals long
            # outside the window.
            index = bisect.bisect_left(self.request_timestamps,
                                       time.time() - self.qps_window_size)
            self.request_timestamps = self.request_timestamps[index:]
            arrivals = len(self.request_timestamps)
            if arrivals > 0 and best_capacity > 0:
                arrival_work = float(arrivals)
                duration = self.effective_request_duration_seconds
                if duration is not None:
                    arrival_work *= (duration / self.qps_window_size)
                arrival_floor = self._clip_target_num_replicas(
                    math.ceil(arrival_work / best_capacity))
                if arrival_floor > self.target_num_replicas:
                    logger.info(
                        'Concurrency autoscaler signal-stale: raising '
                        f'target to arrival floor {arrival_floor} '
                        f'({arrivals} arrivals / capacity {best_capacity}).')
                    self._raw_target_num_replicas = arrival_floor
                    self._adopt_scale_up_target(arrival_floor, replica_infos)
            else:
                logger.info('Concurrency autoscaler signal-stale: holding '
                            f'target at {self.target_num_replicas}.')
            return

        if (self.configured_accelerator_shapes and
                not self._compatibility_demand_complete):
            # Mixed controller/LB rollout: aggregate gauges are fresh, but a
            # card assignment is not. Keep the prior target and leave the
            # one-shot restart fence armed. This prevents both an unshaped
            # launch and a card-blind downscale until the new active LB has
            # reported every replaceable compatibility gauge.
            logger.info(
                'Concurrency compatibility gauges incomplete: '
                'holding exact-card target at %s.',
                self.target_num_replicas_by_accelerator)
            return

        # The canonical compatibility planner owns deadline allocation.  Run
        # it before aggregate sizing so _outstanding_work() can consume this
        # generation's deadline target instead of treating every queued
        # request as one immediately required slot.
        self._deadline_capacity_plan = None
        self._deadline_target_by_accelerator = {}
        self._deadline_infeasible_by_priority = {}
        candidate_allocation: _CompatibilityTargetResult | None = None
        candidate_target_by_accelerator: dict[str, int] | None = None
        if self._compatibility_demand_complete:
            candidate_allocation = (
                self._calculate_concurrency_target_by_accelerator(replica_infos)
            )
            if candidate_allocation.card_attribution_complete:
                candidate_target_by_accelerator = (
                    candidate_allocation.target_by_accelerator)
        outstanding = self._outstanding_work(replica_infos)
        if self.replica_unit == 'logical':
            raw_target_num = _work_to_slots(outstanding, best_capacity)
            arrival_work = self._arrival_work()
            self._arrival_floor_target = self._clip_target_num_replicas(
                math.ceil(arrival_work / best_capacity))
            raw_target_num = max(raw_target_num, self._arrival_floor_target)
        else:
            self._arrival_floor_target = 0
            raw_target_num = 0
            covered = 0.0
            for capacity in latest_capacities:
                if covered >= outstanding:
                    break
                raw_target_num += 1
                covered += capacity
            if covered < outstanding:
                remaining = outstanding - covered
                if best_capacity > 0:
                    raw_target_num += math.ceil(remaining / best_capacity)

        if candidate_target_by_accelerator is not None:
            # Compatibility constraints can require a different physical
            # packing than the aggregate best-capacity estimate. The aggregate
            # offered-arrival floor remains independently authoritative when
            # compatibility evidence is unavailable.
            raw_target_num = max(raw_target_num,
                                 sum(candidate_target_by_accelerator.values()))

        target_num_replicas = self._clip_concurrency_demand_target(
            raw_target_num)
        self._raw_target_num_replicas = target_num_replicas
        candidate_covers_raw_target = (
            candidate_target_by_accelerator is not None and
            sum(candidate_target_by_accelerator.values())
            >= target_num_replicas)
        if (self.replica_unit == 'logical' and
                self._snap_target_on_next_recompute and
                self._adopt_total_capacity_on_next_recompute):
            # The adopted target is controller-local and rebuilds at
            # min_replicas, while the latest-version demand-owned fleet may
            # already be much larger. Re-establish that traffic fleet as the
            # actuation baseline once, before applying hysteresis and the
            # downscale limit. Fill-origin rows remain independently protected
            # by the reserved-capacity overlay; including them here would turn
            # opportunistic supply into paid replacement demand.
            # Otherwise the first fresh report after a restart can publish a
            # tiny target and retire the whole live fleet in one tick. Do not
            # repeat this after the one-shot snap: an adopted downscale target
            # must remain below committed capacity while retirement catches up.
            committed = self._total_ready_demand_owned_logical_capacity(
                replica_infos)
            self.target_num_replicas = max(
                self.target_num_replicas,
                self._clip_concurrency_demand_target(committed))
        old_target_num_replicas = self.target_num_replicas
        old_target_by_accelerator = dict(
            self.target_num_replicas_by_accelerator)
        old_explicit_target_by_accelerator = dict(
            self._logical_adopted_explicit_target_by_accelerator)
        old_paid_target_by_accelerator = dict(
            self._logical_adopted_paid_target_by_accelerator)
        if (self.replica_unit == 'logical' and candidate_covers_raw_target and
                sum(old_target_by_accelerator.values())
                != old_target_num_replicas):
            # A rebuilt controller reconstructs the aggregate safety target
            # from committed demand-owned capacity, while its process-local
            # exact-card demand map starts empty. Attribute the entire held
            # aggregate through the fresh compatibility allocator. Committed
            # A100 supply belongs to the separate actuation map and must not
            # become A100 demand merely because it survived the restart.
            recovered_allocation = (
                self._calculate_concurrency_target_by_accelerator(
                    replica_infos,
                    target_ceiling=old_target_num_replicas,
                    min_replicas_override=old_target_num_replicas))
            recovered_map = recovered_allocation.target_by_accelerator
            if (recovered_allocation.card_attribution_complete and
                    sum(recovered_map.values()) == old_target_num_replicas):
                self.target_num_replicas_by_accelerator = recovered_map
                self._logical_adopted_explicit_target_by_accelerator = {
                    card: min(count, recovered_map.get(card, 0))
                    for card, count in
                    recovered_allocation.explicit_target_by_accelerator.items()
                    if count > 0 and recovered_map.get(card, 0) > 0
                }
                self._logical_adopted_paid_target_by_accelerator = {
                    card: min(count, recovered_map.get(card, 0))
                    for card, count in
                    recovered_allocation.paid_target_by_accelerator.items()
                    if count > 0 and recovered_map.get(card, 0) > 0
                }
                old_target_by_accelerator = dict(recovered_map)
                old_explicit_target_by_accelerator = dict(
                    self._logical_adopted_explicit_target_by_accelerator)
                old_paid_target_by_accelerator = dict(
                    self._logical_adopted_paid_target_by_accelerator)
        target_map_changed = (candidate_target_by_accelerator is not None and
                              candidate_target_by_accelerator
                              != old_target_by_accelerator)
        target_map_increases = False
        if target_map_changed and candidate_target_by_accelerator is not None:
            target_map_increases = any(
                candidate_target_by_accelerator.get(card, 0) >
                old_target_by_accelerator.get(card, 0)
                for card in candidate_target_by_accelerator)
        apply_target = False
        apply_card_transition = False

        if self._snap_target_on_next_recompute:
            # First recompute with fresh data after construction or an update:
            # snap upward immediately, but never bypass downscale hysteresis.
            # A policy-only update can land during a brief idle interval; an
            # immediate downward snap would tear down the live fleet before
            # the configured downscale delay has proved sustained idleness.
            self._snap_target_on_next_recompute = False
            self._adopt_total_capacity_on_next_recompute = False
            self.upscale_counter = 0
            self._reset_downscale_hysteresis()
            self._downscale_veto_streak = 0
            if target_num_replicas >= self.target_num_replicas:
                self._adopt_scale_up_target(target_num_replicas, replica_infos)
                apply_target = True
            else:
                if self._downscale_hysteresis_elapsed():
                    if not self._consume_downscale_pressure_veto():
                        self._reset_downscale_hysteresis()
                        self._adopt_scale_down_target(target_num_replicas,
                                                      replica_infos)
                        apply_target = True
        # Faster scale up when there is no replica.
        elif self.target_num_replicas == 0:
            self._reset_downscale_hysteresis()
            self._downscale_veto_streak = 0
            self._adopt_scale_up_target(target_num_replicas, replica_infos)
            apply_target = True
        elif target_num_replicas > self.target_num_replicas:
            self.upscale_counter += 1
            self._reset_downscale_hysteresis()
            # A rising raw target ends the downscale episode: the next
            # episode gets a fresh veto budget.
            self._downscale_veto_streak = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                self._adopt_scale_up_target(target_num_replicas, replica_infos)
                apply_target = True
        elif target_num_replicas < self.target_num_replicas:
            # Aggregate and exact-card directions are independent. A lower
            # aggregate target must continue its elapsed proof even if the
            # compatibility mix asks for more of one card. The card migration
            # may still advance under the normal upscale observation and wave
            # bounds while the aggregate target remains held.
            if target_map_increases:
                self.upscale_counter += 1
                if self.upscale_counter >= self.scale_up_threshold:
                    self.upscale_counter = 0
                    apply_card_transition = True
            else:
                self.upscale_counter = 0
            if self._downscale_hysteresis_elapsed():
                if not self._consume_downscale_pressure_veto():
                    self._reset_downscale_hysteresis()
                    self._adopt_scale_down_target(target_num_replicas,
                                                  replica_infos)
                    apply_target = True
        elif target_map_increases:
            # A same-size migration is still an upscale. It ends any prior
            # lower-demand episode, but never changes the aggregate target.
            self.upscale_counter += 1
            self._reset_downscale_hysteresis()
            self._downscale_veto_streak = 0
            if self.upscale_counter >= self.scale_up_threshold:
                self.upscale_counter = 0
                self._adopt_scale_up_target(target_num_replicas, replica_infos)
                apply_target = True
        elif target_map_changed:
            self.upscale_counter = 0
            if self._downscale_hysteresis_elapsed():
                if not self._consume_downscale_pressure_veto():
                    self._reset_downscale_hysteresis()
                    self._adopt_scale_down_target(target_num_replicas,
                                                  replica_infos)
                    apply_target = True
        else:
            self.upscale_counter = 0
            self._reset_downscale_hysteresis()
            self._downscale_veto_streak = 0

        if ((apply_target or apply_card_transition) and
                candidate_covers_raw_target):
            if (self._raw_target_num_replicas < self.target_num_replicas):
                fresh_allocation = (
                    self._calculate_concurrency_target_by_accelerator(
                        replica_infos,
                        target_ceiling=self._raw_target_num_replicas,
                        min_replicas_override=self._raw_target_num_replicas))
                fresh_map = fresh_allocation.target_by_accelerator
                adopted_map = _merge_fresh_target_into_downscale_hold(
                    adopted_target=old_target_by_accelerator,
                    fresh_target=fresh_map,
                    configured_cards=self._configured_cards_from_profiles(),
                    replacement_order=self._cold_paid_card_order(
                        self._configured_cards_from_profiles()),
                    target_total=self.target_num_replicas)
            else:
                adopted_allocation = (
                    self._calculate_concurrency_target_by_accelerator(
                        replica_infos,
                        target_ceiling=self.target_num_replicas,
                        min_replicas_override=self.target_num_replicas))
                adopted_map = adopted_allocation.target_by_accelerator
                fresh_allocation = adopted_allocation
            if (fresh_allocation.card_attribution_complete and
                    sum(adopted_map.values()) == self.target_num_replicas):
                self.target_num_replicas_by_accelerator = adopted_map
                fresh_explicit = {
                    card: min(count, adopted_map.get(card, 0))
                    for card, count in
                    fresh_allocation.explicit_target_by_accelerator.items()
                    if count > 0 and adopted_map.get(card, 0) > 0
                }
                fresh_paid = {
                    card: min(count, adopted_map.get(card, 0))
                    for card, count in
                    fresh_allocation.paid_target_by_accelerator.items()
                    if count > 0 and adopted_map.get(card, 0) > 0
                }
                if self._raw_target_num_replicas < self.target_num_replicas:
                    self._logical_adopted_explicit_target_by_accelerator = {
                        card: max(
                            fresh_explicit.get(card, 0),
                            min(old_explicit_target_by_accelerator.get(card, 0),
                                adopted_map.get(card, 0)))
                        for card in adopted_map
                        if (fresh_explicit.get(card, 0) > 0 or
                            old_explicit_target_by_accelerator.get(card, 0) > 0)
                    }
                    self._logical_adopted_paid_target_by_accelerator = {
                        card: max(
                            fresh_paid.get(card, 0),
                            min(old_paid_target_by_accelerator.get(card, 0),
                                adopted_map.get(card, 0)))
                        for card in adopted_map
                        if (fresh_paid.get(card, 0) > 0 or
                            old_paid_target_by_accelerator.get(card, 0) > 0)
                    }
                else:
                    self._logical_adopted_explicit_target_by_accelerator = (
                        fresh_explicit)
                    self._logical_adopted_paid_target_by_accelerator = (
                        fresh_paid)

        self._upscale_pending = (
            target_num_replicas > self.target_num_replicas or
            (candidate_target_by_accelerator is not None and any(
                candidate_target_by_accelerator.get(card, 0) >
                self.target_num_replicas_by_accelerator.get(card, 0)
                for card in candidate_target_by_accelerator)))

        if self.replica_unit == 'logical':
            downscale_status = (
                f'Downscale observations: {self.downscale_counter}. '
                f'Downscale elapsed: {self._downscale_elapsed_seconds():.1f}/'
                f'{self.downscale_delay_seconds:.1f}s. ')
        else:
            downscale_status = (f'Downscale counter: {self.downscale_counter}/'
                                f'{self.scale_down_threshold}. ')
        logger.info(
            f'Concurrency: outstanding work: {outstanding}. '
            f'Latest-version capacities: {latest_capacities}. '
            f'Old target number of replicas: {old_target_num_replicas}. '
            f'Current target number of replicas: {target_num_replicas}. '
            f'Final target number of replicas: {self.target_num_replicas}. '
            f'Target by accelerator: '
            f'{self.target_num_replicas_by_accelerator}. '
            f'Upscale counter: {self.upscale_counter}/'
            f'{self.scale_up_threshold}. '
            f'{downscale_status}')

    def generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        decision_inputs = self._prepare_scaling_decision_inputs(replica_infos)
        return self._generate_scaling_decisions_with_inputs(
            replica_infos, active_versions, decision_inputs)

    def _cached_historical_scaling_versions(self) -> set[int]:
        with self._logical_state_lock:
            return set(self._knob_by_version)

    def _prepare_service_time_estimates(
            self) -> dict[str, dict[str, float | int]]:
        with self._logical_state_lock:
            if (self._queue_depth <= 0 or
                    not self._deadline_capacity_planning_available()):
                return {}
        try:
            service_hash = serve_state.get_service_hash(self._service_name)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Failed to resolve service incarnation for exact '
                'card timing: %s', common_utils.format_exception(error))
            return {}
        if not isinstance(service_hash, str) or not service_hash:
            return {}
        return async_request_ledger.get_service_time_estimates(
            self._service_name, service_hash, self.latest_version)

    def _normalize_historical_scaling_spec(self, spec: Any) -> float | None:
        if spec is None or spec.target_concurrency_per_replica is None:
            return None
        try:
            return float(spec.target_concurrency_per_replica)
        except (TypeError, ValueError):
            return None

    def _generate_scaling_decisions_with_inputs(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
        decision_inputs: ScalingDecisionInputs,
    ) -> list[AutoscalerDecision]:
        """Consume controller-prepared handles without public API changes."""
        self._validate_scaling_decision_inputs(decision_inputs, replica_infos)
        shape_handles = decision_inputs.gpu_shape_handles
        if shape_handles is None:
            raise ValueError('Shape-aware scaling inputs have no handle '
                             'snapshot.')
        historical_values = decision_inputs.historical_scaling_values
        if historical_values is None:
            raise ValueError('Shape-aware scaling inputs have no historical '
                             'capacity snapshot.')
        for version, value in historical_values.items():
            if (value is not None and
                (not isinstance(value,
                                (int, float)) or isinstance(value, bool))):
                raise TypeError('Invalid prepared historical concurrency '
                                f'value for version {version}.')
        with self._logical_state_lock:
            self._service_time_estimates_by_accelerator = {
                str(card): dict(estimate) for card, estimate in
                decision_inputs.service_time_estimates_by_accelerator.items()
            }
            self._gpu_shape_handles_for_tick = shape_handles
            self._kueue_capacity_by_replica_id_for_tick = dict(
                decision_inputs.kueue_capacity_by_replica_id)
            self._kueue_blocked_retirement_shapes_for_tick = (
                decision_inputs.kueue_blocked_retirement_shapes)
            self._kueue_transition_replica_ids_for_tick = (
                decision_inputs.kueue_transition_replica_ids)
            self._kueue_ready_paid_replacement_replica_ids_for_tick = (
                decision_inputs.kueue_ready_paid_replacement_replica_ids)
            self._knob_unavailable_versions_for_tick = {
                version for version, value in historical_values.items()
                if value is None
            }
            for version, value in historical_values.items():
                if value is not None:
                    assert isinstance(value, (int, float))
                    self._knob_by_version[version] = float(value)
            with self._cold_paid_cost_snapshot_for_tick():
                try:
                    return self._generate_scaling_decisions_locked(
                        replica_infos, active_versions)
                finally:
                    self._knob_unavailable_versions_for_tick = None
                    self._gpu_shape_handles_for_tick = None
                    self._kueue_capacity_by_replica_id_for_tick = None
                    self._kueue_blocked_retirement_shapes_for_tick = frozenset()
                    self._kueue_transition_replica_ids_for_tick = frozenset()
                    self._kueue_ready_paid_replacement_replica_ids_for_tick = (
                        frozenset())

    def _generate_scaling_decisions_locked(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[AutoscalerDecision]:
        # Recompute the target BEFORE the base class runs the
        # outdated-replica drain, for the same reason as the
        # instance-aware autoscaler: the drain compares ready new-version
        # replicas against target_num_replicas, and a stale target would
        # let it retire old capacity that is still needed. Single
        # recompute per tick.
        # Freshness is snapshotted ONCE per tick: collect_request_
        # information runs concurrently on the sync thread, and if the
        # first fresh report landed mid-tick the recompute would take
        # the stale path (target still the rebuilt-blind minimum) while
        # the later drain/scale-down guards saw "fresh" and proceeded --
        # marrying a blind target to fresh-mode kills. All three
        # consumers read this snapshot instead of re-evaluating.
        self._tick_fresh = self.has_fresh_demand_report()
        # Sample launch-to-ready before sizing, so a wave that just landed
        # informs this tick's lead estimate.
        self._observe_provision_leads(replica_infos)
        self._logical_actuation_wave_is_new = False
        self._logical_actuation_wave_budget = self._logical_scale_up_budget(
            replica_infos)
        self._logical_actuation_wave_started = False
        self._logical_card_transition_pending = False
        try:
            self._prune_gpu_shape_cache(
                {info.replica_id for info in replica_infos})
            keep_versions = {info.version for info in replica_infos}
            keep_versions.add(self.latest_version)
            for version in list(self._knob_by_version):
                if version not in keep_versions:
                    del self._knob_by_version[version]
            self._set_target_num_replicas_with_concurrency_logic(replica_infos)
            decisions = super().generate_scaling_decisions(
                replica_infos, active_versions)
            if self.replica_unit != 'logical':
                return decisions
            fenced: list[AutoscalerDecision] = []
            target = self.get_final_target_num_replicas()
            target_by_card, use_card_targets = (
                self._actuation_target_by_accelerator(replica_infos))
            self.capacity_target_by_accelerator = dict(target_by_card)
            self.capacity_target_complete = use_card_targets
            target_by_card_state = (tuple(target_by_card.items())
                                    if use_card_targets else ())
            shape_state = (tuple(self.configured_accelerator_shapes.items())
                           if use_card_targets else ())
            if self.configured_accelerator_shapes and not use_card_targets:
                # An authoritative exact-card catalog without a complete
                # compatibility report cannot safely authorize aggregate-only
                # retirement. None tells the controller to revoke any target
                # published by an earlier complete generation.
                self._last_logical_target_state = None
            elif use_card_targets:
                self._last_logical_target_state = LogicalCapacityTarget(
                    version=self.latest_version,
                    generation=self._reconcile_generation,
                    target_capacity=target,
                    target_capacity_by_accelerator=target_by_card_state,
                    accelerator_shapes=shape_state)
            else:
                self._last_logical_target_state = LogicalCapacityTarget(
                    version=self.latest_version,
                    generation=self._reconcile_generation,
                    target_capacity=target)
            for decision in decisions:
                if (decision.operator == AutoscalerDecisionOperator.SCALE_DOWN
                        and isinstance(decision.target, int)):
                    if not self._fresh_for_tick():
                        continue
                    decision = AutoscalerDecision(
                        AutoscalerDecisionOperator.SCALE_DOWN,
                        LogicalScaleDownTarget(
                            version=self.latest_version,
                            reconcile_generation=self._reconcile_generation,
                            target_capacity=target,
                            replica_id=decision.target,
                            target_capacity_by_accelerator=(
                                target_by_card_state),
                            accelerator_shapes=shape_state),
                        reason=decision.reason)
                fenced.append(decision)
            return fenced
        finally:
            self._tick_fresh = None

    def _calculate_target_num_replicas(self) -> int:
        # Demand-aware sizing needs replica_infos, which this hook
        # (invoked by the base update_version to snap the target after an
        # update) does not receive. Keep the current target (re-clipped to
        # the new bounds); the next decision tick recomputes from live
        # replica shapes and the fresh demand report.
        return self._clip_target_num_replicas(self.target_num_replicas)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        with self._logical_state_lock:
            self._update_version_locked(version, spec, update_mode)

    def update_version_and_accelerator_shapes(
            self,
            version: int,
            spec: 'service_spec.SkyServiceSpec',
            update_mode: serve_utils.UpdateMode,
            accelerator_shapes: dict[str, int],
            *,
            backend_num_nodes: int = 1) -> None:
        """Atomically publish a version and its exact-card policy state."""
        with self._logical_state_lock:
            self._update_version_locked(version, spec, update_mode)
            self._set_configured_accelerator_shapes_locked(
                accelerator_shapes, backend_num_nodes=backend_num_nodes)

    def _update_version_locked(self, version: int,
                               spec: 'service_spec.SkyServiceSpec',
                               update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions; don't overwrite the
            # live concurrency knob or arm the post-update snap for a
            # rejected call either.
            super().update_version(version, spec, update_mode)
            return
        target_concurrency = spec.target_concurrency_per_replica
        if target_concurrency is not None:
            # Assign BEFORE the base update runs so any recompute it
            # triggers sees the new knob.
            self.target_concurrency_per_replica = float(target_concurrency)
            self._knob_by_version[version] = float(target_concurrency)
        self.target_utilization_percentage = int(
            spec.target_utilization_percentage)
        self.expected_request_duration_seconds = (
            spec.expected_request_duration_seconds)
        self.initial_provision_lead_time_seconds = (
            spec.initial_provision_lead_time_seconds)
        # Measurements describe the workload, not the spec revision, so an
        # update keeps them. Disabling the feature must take effect at once.
        self.adaptive_demand_estimation = (spec.adaptive_demand_estimation
                                           is not False)
        self.max_scale_up_rate_percentage = spec.max_scale_up_rate_percentage
        self.scale_up_rate_min_replicas = spec.scale_up_rate_min_replicas
        self.scale_up_rate_period_seconds = spec.scale_up_rate_period_seconds
        adaptive_scale_up = spec.adaptive_scale_up
        self.adaptive_scale_up = (dict(adaptive_scale_up) if isinstance(
            adaptive_scale_up, dict) else None)
        queue_config = spec.lb_request_queue or {}
        self._queue_timeout_seconds = queue_config.get('timeout_seconds')
        self._queue_timeout_thresholds = tuple(
            (int(entry['min_priority']), float(entry['timeout_seconds']))
            for entry in queue_config.get('timeout_seconds_by_priority', ()))
        self.max_scale_down_rate_percentage = int(
            spec.max_scale_down_rate_percentage)
        super().update_version(version, spec, update_mode)
        self._reset_downscale_hysteresis()
        self._downscale_veto_streak = 0
        self._pending_retention_floor = None
        self._pending_capacity_at_adoption = 0
        self._pending_budget_spent = 0
        if (self.replica_unit == 'logical' and
                self.max_scale_up_rate_percentage is not None):
            # target_num_replicas described the previous version's launch
            # intent.  The new version has no committed capacity yet, so
            # carrying that target across the update would let its first
            # reconciliation bypass the scale-up wave and launch the whole
            # inherited target from zero. Start from a cold zero baseline; the
            # next fresh or stale recompute authorizes at most one configured
            # wave, including any aggregate or per-card floor.
            self.target_num_replicas = 0
            self.target_num_replicas_by_accelerator = {}
            self._logical_adopted_explicit_target_by_accelerator = {}
            self._logical_adopted_paid_target_by_accelerator = {}
            # A retained ceiling belongs to the previous version's committed
            # capacity. Keep the shared timer, but fail closed for the rest of
            # that cooldown instead of granting the new version the old
            # version's unspent authority.
            self._logical_scale_up_wave_ceiling = None
        self._snap_target_on_next_recompute = True
        self._adopt_total_capacity_on_next_recompute = False
        self._last_logical_target_state = None

    def _select_outdated_replicas_to_scale_down(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        active_versions: list[int],
    ) -> list[int]:
        """Capacity-aware rolling drain in concurrency units.

        Mirrors the instance-aware implementation with demand =
        outstanding work: keep enough READY old replicas to cover the
        demand the ready latest replicas cannot yet serve (never fewer
        than the base class's count rule), and retire the rest. Two
        concurrency-specific twists:
        - SIGNAL GAP: no retirements at all while the demand report is
          stale (a rebuilt controller at target=min_replicas would
          otherwise mass-retire a live fleet before the first sync).
        - Idle-only victims: among READY old replicas, busy ones (fresh
          in-flight > 0 or unknown) are kept as coverage so in-progress jobs
          are never aborted. They become eligible on a later idle tick.
        """
        if not self._fresh_for_tick():
            return []
        target_by_card, use_card_targets = (
            self._actuation_target_by_accelerator(replica_infos))
        canonical_by_name: dict[str, str] = {}
        ready_latest_by_card: dict[str, int] = {}
        if use_card_targets:
            canonical_by_name = {
                card.casefold(): card
                for card in self._configured_cards_from_profiles()
            }
            ready_latest_by_card = {card: 0 for card in target_by_card}
            for info in replica_infos:
                if (info.version != self.latest_version or not info.is_ready or
                        _replica_is_retiring_card_supply(info)):
                    continue
                raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                card = canonical_by_name.get(raw_card.casefold())
                if card is None:
                    continue
                width = (self._ready_capacity(info)
                         if self.replica_unit == 'logical' else 1)
                ready_latest_by_card[card] = (
                    ready_latest_by_card.get(card, 0) + width)
            exact_card_shortfall = any(
                ready_latest_by_card.get(card, 0) < target
                for card, target in target_by_card.items())
            incremental_logical_rollout = (self.replica_unit == 'logical' and
                                           self.update_mode
                                           == serve_utils.UpdateMode.ROLLING)
            if exact_card_shortfall and not incremental_logical_rollout:
                logger.info(
                    'Concurrency rolling drain waiting for '
                    'latest-version exact-card coverage: ready=%s, '
                    'target=%s.', ready_latest_by_card, target_by_card)
                return []
        if (self.replica_unit == 'logical' and
                self.update_mode == serve_utils.UpdateMode.ROLLING):
            old_nonterminal = [
                info for info in replica_infos
                if (info.version < self.latest_version and not info.is_terminal
                    and info.status_property.is_scale_down is not True)
            ]
            if not old_nonterminal:
                return []
            latest_ready_capacity = sum(
                self._ready_capacity(info)
                for info in replica_infos
                if (info.version == self.latest_version and
                    not _replica_is_retiring_card_supply(info)))

            # Old physical rows predate authoritative logical-width reports.
            # Every READY backend nevertheless represents at least one serving
            # slot, so counting one slot per old backend is a conservative
            # coverage floor. Keep enough old READY backends to cover both raw
            # demand and the adopted target while latest-version observed
            # logical capacity comes online. This permits incremental rollout
            # progress even when the complete latest target cannot be placed.
            coverage_target = max(self.get_final_target_num_replicas(),
                                  self._raw_target_num_replicas)
            old_ready = [info for info in old_nonterminal if info.is_ready]
            required_ready_old = max(0, coverage_target - latest_ready_capacity)
            excess_ready_old = max(0, len(old_ready) - required_ready_old)

            # Never-served old replicas add no live coverage and can be
            # retired first. Probe-blipped or occupancy-unknown backends still
            # count as busy through _replica_is_busy and remain protected.
            idle_nonready_old = [
                info for info in old_nonterminal
                if (not info.is_ready and not self._replica_is_busy(info) and
                    self._kueue_ordinary_victim_eligible(info))
            ]
            idle_ready_old = [
                info for info in old_ready
                if (not self._replica_is_busy(info) and
                    self._kueue_ordinary_victim_eligible(info))
            ]
            batch_limit = _LOGICAL_ROLLING_UPDATE_MAX_RETIREMENTS_PER_TICK
            selected_nonready = _select_nonterminal_replicas_to_scale_down(
                min(batch_limit, len(idle_nonready_old)), idle_nonready_old)
            remaining_limit = batch_limit - len(selected_nonready)
            ready_limit = min(remaining_limit, excess_ready_old,
                              len(idle_ready_old))
            if use_card_targets and ready_limit > 0:
                old_ready_by_card: dict[str, int] = {}
                for info in old_ready:
                    raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                    card = canonical_by_name.get(raw_card.casefold())
                    if card is not None:
                        old_ready_by_card[card] = (
                            old_ready_by_card.get(card, 0) + 1)
                old_or_target_cards = (set(old_ready_by_card) |
                                       set(target_by_card))
                excess_old_by_card = {
                    card: max(
                        0,
                        old_ready_by_card.get(card, 0) - max(
                            0,
                            target_by_card.get(card, 0) -
                            ready_latest_by_card.get(card, 0),
                        ),
                    ) for card in old_or_target_cards
                }
                ordered_idle_ids = (_select_nonterminal_replicas_to_scale_down(
                    len(idle_ready_old), idle_ready_old))
                idle_by_id = {info.replica_id: info for info in idle_ready_old}
                selected_ready = []
                for replica_id in ordered_idle_ids:
                    info = idle_by_id[replica_id]
                    raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                    card = canonical_by_name.get(raw_card.casefold())
                    if card is None or excess_old_by_card.get(card, 0) <= 0:
                        continue
                    selected_ready.append(replica_id)
                    excess_old_by_card[card] -= 1
                    if len(selected_ready) >= ready_limit:
                        break
            else:
                selected_ready = _select_nonterminal_replicas_to_scale_down(
                    ready_limit, idle_ready_old)
            selected = selected_nonready + selected_ready
            logger.info(
                'Logical rolling drain: coverage_target=%s, '
                'latest_ready_capacity=%s, ready_old=%s, '
                'required_ready_old=%s, idle_nonready_old=%s, '
                'selected=%s.', coverage_target, latest_ready_capacity,
                len(old_ready), required_ready_old, len(idle_nonready_old),
                len(selected))
            return selected
        if self._upscale_pending:
            logger.info('Concurrency autoscaler suppressing outdated-replica '
                        'drain while an upscale is pending hysteresis.')
            return []
        if self.update_mode != serve_utils.UpdateMode.ROLLING:
            candidates = super()._select_outdated_replicas_to_scale_down(
                replica_infos, active_versions)
            by_id = {info.replica_id: info for info in replica_infos}
            return [
                replica_id for replica_id in candidates
                if self._kueue_ordinary_victim_eligible(by_id[replica_id])
            ]
        old_nonterminal = [
            info for info in replica_infos
            if info.version < self.latest_version and not info.is_terminal
        ]
        if not old_nonterminal:
            return []
        num_ready_latest = 0
        ready_latest_capacity = 0.0
        for info in replica_infos:
            if (info.version == self.latest_version and info.is_ready and
                    not _replica_is_retiring_card_supply(info)):
                num_ready_latest += 1
                ready_latest_capacity += self._replica_capacity(info)
        if num_ready_latest >= self.get_final_target_num_replicas():
            # Enough latest-version replicas: retire the old ones -- but
            # only those not visibly mid-job. The base class retires all
            # of them unconditionally, which for hour-long jobs would
            # abort every in-progress prediction the moment the new
            # fleet is ready; a busy old replica is instead retired on a
            # later tick, once its job finishes and it reports idle.
            return [
                info.replica_id
                for info in old_nonterminal
                if (not self._replica_is_busy(info) and
                    self._kueue_ordinary_victim_eligible(info))
            ]

        shortfall = (self._outstanding_work(replica_infos) -
                     ready_latest_capacity)
        # Never keep fewer old replicas than the base class's count rule
        # (target - ready_new): capacity packing with a few big old
        # replicas could otherwise drain the standby pool a low-traffic
        # service relies on for its next request.
        keep_count_floor = min(
            len(old_nonterminal),
            max(0,
                self.get_final_target_num_replicas() - num_ready_latest))

        ready_old = []
        nonready_old = []
        for info in old_nonterminal:
            capacity = self._replica_capacity(info)
            if info.is_ready:
                ready_old.append((capacity, info))
            else:
                nonready_old.append((capacity, info))
        unavailable_versions = self._knob_unavailable_versions_for_tick
        if unavailable_versions:
            logger.info(
                'Concurrency rolling drain waiting for historical capacity '
                'for versions: %s.', sorted(unavailable_versions))
            return []
        # Keep-preference order: busy replicas first (retiring them kills
        # jobs; keeping them retains capacity that is provably serving),
        # then largest capacity (fewest old replicas kept, fastest
        # rollout), replica id as a stable tie-break across ticks. A
        # READY replica missing from the fresh in-flight map counts as
        # busy: the LB may simply not have reported it yet.
        ready_old.sort(key=lambda pair: (not self._replica_is_busy(pair[1]),
                                         -pair[0], pair[1].replica_id))

        keep_ids: set[int] = set()
        covered = 0.0
        for capacity, info in ready_old:
            if covered >= shortfall and len(keep_ids) >= keep_count_floor:
                break
            keep_ids.add(info.replica_id)
            if capacity > 0:
                covered += capacity
        # Not-yet-ready old replicas add no serving capacity; they only
        # count toward the base-class floor (the base helper likewise
        # prefers draining initializing replicas first).
        for _, info in nonready_old:
            if len(keep_ids) >= keep_count_floor:
                break
            keep_ids.add(info.replica_id)
        # Never retire a visibly-busy old replica, regardless of the
        # coverage math: killing it aborts an hour-long job that will
        # re-run from scratch. The busy-first keep-preference above
        # usually keeps them anyway; this makes it a guarantee (they
        # are retired on a later tick, once idle).
        for info in old_nonterminal:
            if self._replica_is_busy(info):
                keep_ids.add(info.replica_id)

        return [
            info.replica_id
            for info in old_nonterminal
            if (info.replica_id not in keep_ids and
                self._kueue_ordinary_victim_eligible(info))
        ]

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate scale-up/down decisions with drain-aware victims.

        The target was already recomputed for this tick in
        generate_scaling_decisions (before the outdated-replica drain).
        """
        latest_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []
        for info in replica_infos:
            if (not info.is_terminal and info.version == self.latest_version and
                    self._kueue_counts_as_assigned(info)):
                latest_nonterminal_replicas.append(info)

        if self.replica_unit == 'logical':
            return self._generate_logical_scaling_decisions(
                replica_infos, latest_nonterminal_replicas)

        scaling_decisions: list[AutoscalerDecision] = []
        self.cold_launch_authority_by_accelerator = {}
        target_num_replicas = self.get_final_target_num_replicas()
        current_num_replicas = len(latest_nonterminal_replicas)
        target_by_card, use_card_targets = (
            self._actuation_target_by_accelerator(replica_infos))
        if use_card_targets:
            replicas_by_card: dict[str, list[replica_managers.ReplicaInfo]] = {}
            ready_by_card: dict[str, int] = {}
            canonical_by_name = {
                card.casefold(): card
                for card in self._configured_cards_from_profiles()
            }
            for info in latest_nonterminal_replicas:
                if _replica_is_retiring_card_supply(info):
                    continue
                raw_card, _ = self._get_gpu_shape_from_replica_info(info)
                card = canonical_by_name.get(raw_card.casefold())
                if card is None:
                    continue
                replicas_by_card.setdefault(card, []).append(info)
                if info.is_ready:
                    ready_by_card[card] = ready_by_card.get(card, 0) + 1
            shortages = {
                card: max(0, target - len(replicas_by_card.get(card, [])))
                for card, target in target_by_card.items()
            }
            self.cold_launch_authority_by_accelerator = {
                card: shortage
                for card, shortage in shortages.items()
                if shortage > 0
            }
            if any(shortages.values()):
                for card, shortage in shortages.items():
                    for _ in range(shortage):
                        scaling_decisions.append(
                            AutoscalerDecision(
                                AutoscalerDecisionOperator.SCALE_UP,
                                target={
                                    'accelerators': {
                                        card: self._configured_gpu_count(card)
                                    }
                                }))
                # Do not retire excess old-card capacity until every target
                # card is actually ready. This is a non-preemptive migration.
                return scaling_decisions
            if not self._compatibility_demand_complete:
                return scaling_decisions
            if not self._fresh_for_tick():
                logger.info('Concurrency autoscaler signal-stale: '
                            'suppressing exact-card scale-down decisions.')
                return scaling_decisions
            if self._upscale_pending:
                logger.info(
                    'Concurrency autoscaler suppressing exact-card '
                    'scale-down while an upscale is pending hysteresis.')
                return scaling_decisions
            all_targets_ready = all(
                ready_by_card.get(card, 0) >= target
                for card, target in target_by_card.items())
            if all_targets_ready:
                for card, replicas in replicas_by_card.items():
                    excess = max(0, len(replicas) - target_by_card.get(card, 0))
                    if excess <= 0:
                        continue
                    eligible = [
                        info for info in replicas
                        if (not self._replica_is_busy(info) and
                            self._kueue_ordinary_victim_eligible(info))
                    ]
                    ordered_ids = (self._select_victims_capacity_and_cost_aware(
                        len(eligible), eligible))
                    by_id = {info.replica_id: info for info in eligible}
                    remaining_ready = sum(info.is_ready for info in replicas)
                    selected: list[int] = []
                    for replica_id in ordered_ids:
                        info = by_id[replica_id]
                        if (info.is_ready and
                                remaining_ready - 1 < target_by_card.get(
                                    card, 0)):
                            continue
                        selected.append(replica_id)
                        if info.is_ready:
                            remaining_ready -= 1
                        if len(selected) >= excess:
                            break
                    scaling_decisions.extend(
                        _generate_scale_down_decisions(selected))
            return scaling_decisions

        if self.configured_accelerator_shapes:
            # A compatibility-capable service must never fall back to an
            # unshaped launch or card-blind retirement while a mixed-version
            # report leaves the per-card target incomplete.
            logger.info('Concurrency exact-card target is incomplete; '
                        'suppressing card-blind scaling decisions.')
            return scaling_decisions

        if current_num_replicas < target_num_replicas:
            scaling_decisions.extend(
                _generate_scale_up_decisions(
                    target_num_replicas - current_num_replicas, None))
        elif current_num_replicas > target_num_replicas:
            if not self._fresh_for_tick():
                # SIGNAL GAP: never shrink while blind. (The stale-path
                # recompute also never lowers the target, but the target
                # can sit below the live fleet right after a controller
                # rebuild -- this is the guard that actually prevents the
                # kills.)
                logger.info('Concurrency autoscaler signal-stale: suppressing '
                            f'{current_num_replicas - target_num_replicas} '
                            'scale-down decision(s).')
                return scaling_decisions
            if self._upscale_pending:
                logger.info(
                    'Concurrency autoscaler suppressing scale-down while an '
                    'upscale is pending hysteresis.')
                return scaling_decisions
            num_to_scale_down = current_num_replicas - target_num_replicas
            # Drain-aware victim eligibility (see _replica_is_busy): a
            # READY replica may be killed ONLY if the fresh report shows
            # zero in-flight work on it (missing entry counts as busy);
            # non-READY replicas are eligible unless the report shows
            # work on them (probe-blipped mid-job).
            eligible_victims = [
                info for info in latest_nonterminal_replicas
                if (not self._replica_is_busy(info) and
                    self._kueue_ordinary_victim_eligible(info))
            ]
            # Clip to the eligible victims and wait otherwise (same
            # pattern as QueueLengthAutoscaler's idle clip): a busy
            # replica finishing its ~1 h job frees up on a later tick.
            ordered_ids = self._select_victims_capacity_and_cost_aware(
                len(eligible_victims), eligible_victims)
            selected_ids = ordered_ids[:num_to_scale_down]
            actual_num_to_scale_down = len(selected_ids)
            if actual_num_to_scale_down < num_to_scale_down:
                logger.info(
                    'Concurrency autoscaler clipping scale-down: requested '
                    f'{num_to_scale_down}, but only '
                    f'{len(eligible_victims)} idle/non-ready replicas are '
                    'eligible.')
            if actual_num_to_scale_down > 0:
                scaling_decisions.extend(
                    _generate_scale_down_decisions(selected_ids))

        return scaling_decisions

    def _generate_logical_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        latest_nonterminal_replicas: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate one shaped scale target or capacity-safe retirements.

        Exact-card revalidation needs the complete active fleet so running
        work on an old version remains attributable during a rolling update.
        Committed and ready capacity below stays latest-version-only: old
        replicas prove the transition shape but never satisfy its launch
        target.
        """
        target = self.get_final_target_num_replicas()
        self.cold_launch_authority_by_accelerator = {}
        target_by_card, use_card_targets = (
            self._actuation_target_by_accelerator(replica_infos))
        self.capacity_target_by_accelerator = dict(target_by_card)
        self.capacity_target_complete = use_card_targets
        if self.configured_accelerator_shapes and not use_card_targets:
            logger.info('Logical concurrency exact-card target is incomplete; '
                        'suppressing card-blind scaling decisions.')
            return []
        canonical_by_name = {
            card.casefold(): card
            for card in (self.configured_accelerator_shapes or target_by_card)
        }

        def _card(info: 'replica_managers.ReplicaInfo') -> str | None:
            raw_card, _ = self._get_gpu_shape_from_replica_info(info)
            return canonical_by_name.get(raw_card.casefold())

        committed = sum(
            self._committed_capacity(info)
            for info in latest_nonterminal_replicas)
        committed_by_card: dict[str, int] = {}
        for info in latest_nonterminal_replicas:
            card = _card(info)
            if card is not None:
                committed_by_card[card] = (committed_by_card.get(card, 0) +
                                           self._committed_capacity(info))
        launch_budget = self._logical_actuation_wave_budget
        launch_authority_left = launch_budget
        if use_card_targets:
            paid_launch_target = (
                self._logical_paid_launch_target_by_accelerator)
            for card, card_target in target_by_card.items():
                committed_on_card = committed_by_card.get(card, 0)
                target_shortage = max(0, card_target - committed_on_card)
                ownership_shortage = max(
                    0,
                    paid_launch_target.get(card, 0) - committed_on_card)
                # The retained actuation map remains the reconciliation and
                # retirement fence. Paid acquisition is the intersection of
                # that shortage with the fresh supply-aware replacement
                # placement: a warm/old-version card may remain in the former
                # without becoming purchase authority in the latter.
                shortage = min(target_shortage, ownership_shortage)
                if launch_authority_left is not None:
                    shortage = min(shortage, launch_authority_left)
                    launch_authority_left -= shortage
                if shortage > 0:
                    self.cold_launch_authority_by_accelerator[card] = shortage
        paid_card_shortage = bool(use_card_targets and
                                  self.cold_launch_authority_by_accelerator)
        # A proof-free card mismatch with sufficient aggregate committed
        # capacity is only a zero-cost placement preference. Do not emit a
        # no-op reconciliation request for it; an aggregate shortage still
        # emits the exact fence so eligible zero-cost supply can fill it.
        if committed < target or paid_card_shortage:
            if launch_budget is not None and launch_budget > 0:
                # Completing the full exact-card fence can consume the map's
                # transition delta in the first restart tick. Later launch
                # waves still need to advance the cooldown when they are
                # authorized, even though that complete map no longer changes.
                self._record_logical_scale_up_wave(replica_infos, launch_budget)
            replace_unknown_replica_ids = tuple(
                sorted(
                    info.replica_id
                    for info in latest_nonterminal_replicas
                    if info.status_property.is_scale_down is not True and info.
                    replica_id in self._degraded_capacity_since_by_replica_id
                    and self._committed_capacity(info) == 0))
            launch_priorities_by_accelerator: tuple[tuple[str, int], ...] = ()
            if use_card_targets:
                current_priorities = (
                    self.current_launch_priorities_by_accelerator(
                        target_by_card))
                launch_priorities_by_accelerator = tuple(
                    current_priorities.items())
            return [
                AutoscalerDecision(
                    AutoscalerDecisionOperator.SCALE_UP,
                    LogicalScaleTarget(
                        version=self.latest_version,
                        reconcile_generation=self._reconcile_generation,
                        target_capacity=target,
                        launch_budget=launch_budget,
                        target_capacity_by_accelerator=tuple(
                            target_by_card.items()) if use_card_targets else (),
                        accelerator_shapes=tuple(
                            self.configured_accelerator_shapes.items())
                        if use_card_targets else (),
                        replace_unknown_replica_ids=replace_unknown_replica_ids,
                        launch_priority=self.current_launch_priority(),
                        launch_priority_by_accelerator=(
                            launch_priorities_by_accelerator),
                        cold_launch_authority_by_accelerator=(tuple(
                            self.cold_launch_authority_by_accelerator.items())
                                                              if
                                                              use_card_targets
                                                              else None)))
            ]
        if (not self._fresh_for_tick() or self._upscale_pending or
                self._logical_card_transition_pending or
            (use_card_targets and not self._compatibility_demand_complete)):
            return []

        status_order = serve_state.ReplicaStatus.scale_down_decision_order()

        def _status_rank(info: 'replica_managers.ReplicaInfo') -> int:
            try:
                return status_order.index(info.status)
            except ValueError:
                return len(status_order)

        candidates = [
            info for info in latest_nonterminal_replicas
            if (info.status_property.is_scale_down is not True and
                not self._replica_is_busy(info) and
                self._kueue_ordinary_victim_eligible(info))
        ]
        candidates.sort(key=lambda info: (
            _status_rank(info),
            self._ready_capacity(info)
            if info.is_ready else self._committed_capacity(info),
            -self._get_hourly_cost_from_replica_info(info),
            -info.replica_id,
        ))
        remaining_committed = committed
        remaining_ready = sum(
            self._ready_capacity(info) for info in latest_nonterminal_replicas)
        remaining_ready_by_card: dict[str, int] = {}
        for info in latest_nonterminal_replicas:
            card = _card(info)
            if card is not None:
                remaining_ready_by_card[card] = (
                    remaining_ready_by_card.get(card, 0) +
                    self._ready_capacity(info))
        remaining_committed_by_card = dict(committed_by_card)
        provisioning_statuses = {
            serve_state.ReplicaStatus.PENDING,
            serve_state.ReplicaStatus.PROVISIONING,
            serve_state.ReplicaStatus.STARTING,
        }
        remaining_demand_pending = sum(
            self._committed_capacity(info)
            for info in latest_nonterminal_replicas
            if (info.status in provisioning_statuses and not info.reserved_fill
               ))
        decisions: list[AutoscalerDecision] = []
        for info in candidates:
            card = _card(info)
            if use_card_targets and card is None:
                continue
            card_target = target_by_card.get(card, 0) if card is not None else 0
            remaining_card_committed = (remaining_committed_by_card.get(
                card, 0) if card is not None else 0)
            remaining_card_ready = (remaining_ready_by_card.get(card, 0)
                                    if card is not None else 0)
            committed_width = self._committed_capacity(info)
            demand_owned = not info.reserved_fill
            if (info.status in provisioning_statuses and demand_owned and
                    self._pending_retention_floor is not None and
                    remaining_demand_pending - committed_width
                    < self._pending_retention_floor):
                # The frozen episode budget is measured in logical slots. A
                # multi-slot victim that would overspend is conservatively
                # skipped rather than rounded through the percentage cap.
                continue
            if info.is_ready:
                ready_width = self._ready_capacity(info)
                if ready_width <= 0:
                    # A fresh, idle backend that serves no logical slots can be
                    # retired once the OTHER positive ready capacity and the
                    # remaining committed capacity cover the target. This
                    # cleans up both a recovered original's redundant zero-slot
                    # replacement and a timed-out zero-slot original after its
                    # replacement becomes healthy.
                    if (remaining_ready < target or
                            remaining_committed - committed_width < target):
                        continue
                    if (use_card_targets and
                            remaining_card_committed - committed_width
                            < card_target):
                        continue
                else:
                    if (remaining_ready - ready_width < target or
                        (use_card_targets and
                         remaining_card_ready - ready_width < card_target)):
                        continue
                    remaining_ready -= ready_width
                    if card is not None:
                        remaining_ready_by_card[card] = (
                            remaining_ready_by_card.get(card, 0) - ready_width)
            elif remaining_committed - committed_width < target:
                continue
            elif (use_card_targets and
                  remaining_card_committed - committed_width < card_target):
                continue
            remaining_committed -= committed_width
            if card is not None:
                remaining_committed_by_card[card] = (
                    remaining_committed_by_card.get(card, 0) - committed_width)
            if info.status in provisioning_statuses and demand_owned:
                remaining_demand_pending -= committed_width
            decisions.append(
                AutoscalerDecision(
                    AutoscalerDecisionOperator.SCALE_DOWN,
                    LogicalScaleDownTarget(
                        version=self.latest_version,
                        reconcile_generation=self._reconcile_generation,
                        target_capacity=target,
                        replica_id=info.replica_id,
                        target_capacity_by_accelerator=(tuple(
                            target_by_card.items()) if use_card_targets else
                                                        ()),
                        accelerator_shapes=(tuple(
                            self.configured_accelerator_shapes.items())
                                            if use_card_targets else ()))))
        self._pending_budget_spent = max(
            0, self._pending_capacity_at_adoption - remaining_demand_pending)
        return decisions

    def _select_victims_capacity_and_cost_aware(
            self, num_to_scale_down: int,
            eligible_victims: list['replica_managers.ReplicaInfo']
    ) -> list[int]:
        """Order victims: status, capacity (asc), then COST (desc).

        Mirrors the instance-aware autoscaler's rationale: among equal
        status, shed the lowest-capacity replicas first (the packing
        target assumes the largest capacities are kept), and among equal
        capacity shed the most EXPENSIVE first -- cloud spot before a
        zero-cost reserved pool. Without the cost key, the routine
        reclaim cycle (research jobs evict the zero-cost fill fleet,
        demand relaunches land on paid spot, jobs finish, fill relaunches
        zero-cost with the newest ids, demand drops) picks the newest --
        zero-cost -- replicas as victims and settles into a stable state
        that pays for spot while free reserved slots sit unfilled.
        Cost must not outrank capacity, same as the instance-aware
        ordering (the target math assumed the biggest replicas survive);
        the capacity key is quantized so float noise cannot split
        mathematically equal capacities away from the cost tiebreak.
        """
        status_order = serve_state.ReplicaStatus.scale_down_decision_order()

        def _status_rank(info: 'replica_managers.ReplicaInfo') -> int:
            try:
                return status_order.index(info.status)
            except ValueError:
                return len(status_order)

        ordered = sorted(eligible_victims,
                         key=lambda info: (
                             _status_rank(info),
                             round(self._replica_capacity(info), 9),
                             -self._get_hourly_cost_from_replica_info(info),
                             info.version,
                             -info.replica_id,
                         ))
        return [info.replica_id for info in ordered[:num_to_scale_down]]

    def info(self) -> dict[str, Any]:
        info = super().info()
        if not self.has_recomputed_with_fresh_data():
            info['target_num_replicas_by_accelerator'] = {}
            info['demand_target_by_accelerator'] = {}
            info['capacity_target_by_accelerator'] = {}
            info['capacity_target_complete'] = False
            info['warm_retention_target_by_accelerator'] = {}
            info['cold_launch_authority_by_accelerator'] = {}
        in_flight_total = (sum(self._in_flight_by_replica_id.values()) if
                           self._in_flight_by_replica_id is not None else None)
        report_age = (time.time() - self._report_received_at
                      if self._report_received_at is not None else None)
        adaptive_remaining = 0.0
        if self._adaptive_until is not None:
            adaptive_remaining = max(0.0,
                                     self._adaptive_until - time.monotonic())
        info.update({
            'replica_unit': self.replica_unit,
            'adaptive_demand_estimation': self.adaptive_demand_estimation,
            'effective_request_duration_seconds':
                self.effective_request_duration_seconds,
            'effective_provision_lead_seconds':
                self.effective_provision_lead_seconds,
            'measured_duration_seconds': self._measured_duration_seconds,
            'measured_duration_samples': self._measured_duration_samples,
            'provision_lead_samples': len(self._provision_lead_samples),
            'in_flight_total': in_flight_total,
            'queue_depth': self._queue_depth,
            'queue_depth_by_priority': self._queue_depth_by_priority,
            'weighted_queue_work': self._weighted_queue_work,
            'queue_capacity_time_planner_active': self._deadline_capacity_plan
                                                  is not None,
            'queue_capacity_time_target_by_accelerator':
                self._deadline_target_by_accelerator,
            'queue_sla_infeasible_by_priority':
                self._deadline_infeasible_by_priority,
            'queue_deadline_profiles': self.queued_deadline_profiles,
            'service_time_source_by_accelerator':
                self._service_time_source_by_accelerator,
            'service_time_seconds_by_accelerator':
                self._effective_service_time_by_accelerator,
            'rejected_in_window': self._rejected_in_window,
            'rejected_in_recent_window': self._rejected_in_recent_window,
            'rejected_in_window_by_priority':
                self._rejected_in_window_by_priority,
            'rejected_in_recent_window_by_priority':
                self._rejected_in_recent_window_by_priority,
            'rejected_concurrency': self._rejected_concurrency,
            'unique_job_arrivals_60s': self._unique_job_arrivals_60s,
            'unique_job_arrivals_300s': self._unique_job_arrivals_300s,
            'headerless_arrivals_60s': self._headerless_arrivals_60s,
            'headerless_arrivals_300s': self._headerless_arrivals_300s,
            'offered_arrival_tracking_saturated':
                self._offered_arrival_tracking_saturated,
            'arrival_floor_target': self._arrival_floor_target,
            'raw_target_num_replicas': self._raw_target_num_replicas,
            'committed_capacity': self._latest_committed_capacity,
            'provisioning_capacity': self._latest_provisioning_capacity,
            'target_utilization_percentage': self.target_utilization_percentage,
            'latest_scale_up_wave_at': self._last_scale_up_wave_at,
            'pressure_streak': self._pressure_streak,
            'pressure_latched': self._pressure_latched,
            'pressure_reasons': list(self._pressure_reasons),
            'adaptive_scale_up_active': self._adaptive_scale_up_active(),
            'adaptive_hold_remaining_seconds': adaptive_remaining,
            'downscale_elapsed_seconds': self._downscale_elapsed_seconds(),
            'downscale_delay_seconds': self.downscale_delay_seconds,
            'downscale_veto_reason': self._downscale_veto_reason,
            'downscale_veto_streak': self._downscale_veto_streak,
            'downscale_veto_budget': _MAX_CONSECUTIVE_DOWNSCALE_VETOES,
            'scale_down_allowance': self._last_scale_down_allowance,
            'pending_scale_down_allowance': self._last_pending_allowance,
            'pending_retention_floor': self._pending_retention_floor,
            'pending_budget_spent': self._pending_budget_spent,
            'unknown_in_flight_replicas': len(
                self._unknown_in_flight_replica_ids),
            'report_age_seconds': report_age,
            'compatibility_demand_complete':
                self._compatibility_demand_complete,
        })
        return info

    def _dump_dynamic_states(self) -> dict[str, Any]:
        # Only consumed by the in-process autoscaler swap during
        # update_service (NOT on controller restart). The received-at
        # time is absolute, so a report that crosses the swap simply
        # reads as stale once it exceeds the staleness threshold.
        with self._logical_state_lock:
            return self._dump_dynamic_states_locked()

    def _dump_dynamic_states_locked(self) -> dict[str, Any]:
        return {
            'request_timestamps': self.request_timestamps,
            'in_flight_by_replica_id': self._in_flight_by_replica_id,
            'queue_depth': self._queue_depth,
            'queue_depth_by_priority': self._queue_depth_by_priority,
            'rejected_in_window': self._rejected_in_window,
            'rejected_in_recent_window': self._rejected_in_recent_window,
            'rejected_in_window_by_priority':
                self._rejected_in_window_by_priority,
            'rejected_in_recent_window_by_priority':
                self._rejected_in_recent_window_by_priority,
            'unique_job_arrivals_60s': self._unique_job_arrivals_60s,
            'unique_job_arrivals_300s': self._unique_job_arrivals_300s,
            'headerless_arrivals_60s': self._headerless_arrivals_60s,
            'headerless_arrivals_300s': self._headerless_arrivals_300s,
            'offered_arrival_tracking_saturated':
                self._offered_arrival_tracking_saturated,
            'measured_duration_seconds': self._measured_duration_seconds,
            'measured_duration_samples': self._measured_duration_samples,
            'measured_duration_at': self._measured_duration_at,
            'provision_lead_samples': list(self._provision_lead_samples),
            'provision_lead_at': self._provision_lead_at,
            'pressure_baseline': self._pressure_baseline,
            'pressure_latched': self._pressure_latched,
            'pressure_reasons': self._pressure_reasons,
            'pressure_streak': self._pressure_streak,
            'downscale_veto_streak': self._downscale_veto_streak,
            'adaptive_until': self._adaptive_until,
            'unknown_in_flight_replica_ids': sorted(
                self._unknown_in_flight_replica_ids),
            'report_received_at': self._report_received_at,
            'launch_priority_report_received_at':
                self._launch_priority_report_received_at,
            'reconcile_generation': self._reconcile_generation,
            'observed_slots_by_replica_id': self._observed_slots_by_replica_id,
            'unknown_capacity_replica_ids': sorted(
                self._unknown_capacity_replica_ids),
            'degraded_capacity_since_by_replica_id':
                self._degraded_capacity_since_by_replica_id,
            'last_scale_up_wave_at': self._last_scale_up_wave_at,
            # Always present, including when empty. A QPS replacement uses
            # presence as proof that the source binary could preserve exact
            # arrival constraints; older dumps fail closed on this field.
            'compatibility_profiles': [{
                **profile,
                'compatible_accelerators': list(
                    profile['compatible_accelerators']),
            } for profile in self.compatibility_profiles],
            'queued_compatibility_profiles': [{
                **profile,
                'compatible_accelerators': list(
                    profile['compatible_accelerators']),
            } for profile in self.queued_compatibility_profiles],
            'queued_deadline_profiles':
                ([{
                    **profile,
                    'compatible_accelerators': list(
                        profile['compatible_accelerators']),
                } for profile in self.queued_deadline_profiles]
                 if self.queued_deadline_profiles is not None else None),
            'rejected_compatibility_profiles': [{
                **profile,
                'compatible_accelerators': list(
                    profile['compatible_accelerators']),
            } for profile in self.rejected_compatibility_profiles],
            'compatibility_demand_complete':
                self._compatibility_demand_complete,
            'configured_accelerator_shapes': dict(
                self.configured_accelerator_shapes),
            'backend_num_nodes': self.backend_num_nodes,
        }

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        # Tolerate dumps from other autoscaler types (an update can
        # change the autoscaler class; e.g. RequestRateAutoscaler only
        # dumps request_timestamps): missing keys keep the stale-start
        # defaults.
        compatibility_arrivals_present = ('compatibility_profiles'
                                          in dynamic_states)
        if compatibility_arrivals_present:
            self.compatibility_profiles = self._parse_compatibility_arrivals(
                dynamic_states.pop('compatibility_profiles'))
        if 'request_timestamps' in dynamic_states:
            self.request_timestamps = dynamic_states.pop('request_timestamps')
        # Estimator state survives a controller restart: re-learning a
        # duration from zero would silently fall back to the configured
        # value for the whole warm-up, which is exactly when a restart
        # under load can least afford an undersized target.
        measured_duration = dynamic_states.pop('measured_duration_seconds',
                                               None)
        if (isinstance(measured_duration, (int, float)) and
                not isinstance(measured_duration, bool) and
                math.isfinite(measured_duration) and measured_duration > 0):
            self._measured_duration_seconds = float(measured_duration)
            samples = dynamic_states.pop('measured_duration_samples', 0)
            self._measured_duration_samples = (
                int(samples) if isinstance(samples, int) and
                not isinstance(samples, bool) and samples >= 0 else 0)
            observed_at = dynamic_states.pop('measured_duration_at', None)
            self._measured_duration_at = (float(observed_at) if isinstance(
                observed_at,
                (int, float)) and not isinstance(observed_at, bool) else None)
        else:
            dynamic_states.pop('measured_duration_samples', None)
            dynamic_states.pop('measured_duration_at', None)
        lead_samples = dynamic_states.pop('provision_lead_samples', None)
        if isinstance(lead_samples, list):
            self._provision_lead_samples = [
                float(sample)
                for sample in lead_samples
                if (isinstance(sample, (int, float)) and not isinstance(
                    sample, bool) and math.isfinite(sample) and sample > 0)
            ][-constants.AUTOSCALER_ADAPTIVE_LEAD_SAMPLE_CAP:]
        lead_at = dynamic_states.pop('provision_lead_at', None)
        if (isinstance(lead_at, (int, float)) and
                not isinstance(lead_at, bool)):
            self._provision_lead_at = float(lead_at)
        if 'in_flight_by_replica_id' in dynamic_states:
            self._in_flight_by_replica_id = dynamic_states.pop(
                'in_flight_by_replica_id')
        if 'queue_depth' in dynamic_states:
            self._queue_depth = dynamic_states.pop('queue_depth')
        if 'queue_depth_by_priority' in dynamic_states:
            self._queue_depth_by_priority = dynamic_states.pop(
                'queue_depth_by_priority')
        if 'rejected_in_window' in dynamic_states:
            self._rejected_in_window = dynamic_states.pop('rejected_in_window')
        if 'rejected_in_recent_window' in dynamic_states:
            self._rejected_in_recent_window = dynamic_states.pop(
                'rejected_in_recent_window')
        if 'queued_compatibility_profiles' in dynamic_states:
            self.queued_compatibility_profiles = (
                self._parse_compatibility_gauge(
                    dynamic_states.pop('queued_compatibility_profiles')))
        if 'queued_deadline_profiles' in dynamic_states:
            raw_deadlines = dynamic_states.pop('queued_deadline_profiles')
            self.queued_deadline_profiles = (
                None if raw_deadlines is None else
                self._parse_deadline_gauge(raw_deadlines))
        if 'rejected_compatibility_profiles' in dynamic_states:
            self.rejected_compatibility_profiles = (
                self._parse_compatibility_gauge(
                    dynamic_states.pop('rejected_compatibility_profiles'),
                    include_recent_count=True))
        if 'compatibility_demand_complete' in dynamic_states:
            self._compatibility_demand_complete = bool(
                dynamic_states.pop('compatibility_demand_complete'))
        if 'configured_accelerator_shapes' in dynamic_states:
            source_shapes = dynamic_states.pop('configured_accelerator_shapes')
            if isinstance(source_shapes, dict):
                self.configured_accelerator_shapes = {
                    str(card): int(count)
                    for card, count in source_shapes.items()
                    if isinstance(card, str) and card and
                    isinstance(count, int) and not isinstance(count, bool) and
                    count > 0
                }
        source_num_nodes = dynamic_states.pop('backend_num_nodes', 1)
        self.backend_num_nodes = (source_num_nodes
                                  if isinstance(source_num_nodes, int) and
                                  not isinstance(source_num_nodes, bool) and
                                  source_num_nodes > 0 else 1)
        if (not self.configured_accelerator_shapes or
                not compatibility_arrivals_present):
            self.compatibility_profiles = []
            self.queued_compatibility_profiles = []
            self.queued_deadline_profiles = None
            self.rejected_compatibility_profiles = []
            self._compatibility_demand_complete = False
        for field in ('rejected_in_window_by_priority',
                      'rejected_in_recent_window_by_priority',
                      'unique_job_arrivals_60s', 'unique_job_arrivals_300s',
                      'headerless_arrivals_60s', 'headerless_arrivals_300s',
                      'offered_arrival_tracking_saturated', 'pressure_baseline',
                      'pressure_latched', 'pressure_reasons', 'pressure_streak',
                      'downscale_veto_streak', 'adaptive_until'):
            key = field
            if key in dynamic_states:
                setattr(self, f'_{field}', dynamic_states.pop(key))
        if 'unknown_in_flight_replica_ids' in dynamic_states:
            self._unknown_in_flight_replica_ids = {
                int(replica_id) for replica_id in dynamic_states.pop(
                    'unknown_in_flight_replica_ids')
            }
        if 'report_received_at' in dynamic_states:
            self._report_received_at = dynamic_states.pop('report_received_at')
        if 'launch_priority_report_received_at' in dynamic_states:
            priority_report_received_at = dynamic_states.pop(
                'launch_priority_report_received_at')
            self._launch_priority_report_received_at = (
                float(priority_report_received_at)
                if isinstance(priority_report_received_at, (int, float)) and
                not isinstance(priority_report_received_at, bool) else None)
        if 'last_scale_up_wave_at' in dynamic_states:
            self._last_scale_up_wave_at = dynamic_states.pop(
                'last_scale_up_wave_at')
        if 'reconcile_generation' in dynamic_states:
            self._reconcile_generation = int(
                dynamic_states.pop('reconcile_generation'))
        if 'observed_slots_by_replica_id' in dynamic_states:
            self._observed_slots_by_replica_id = {
                int(replica_id): int(slots) for replica_id, slots in
                dynamic_states.pop('observed_slots_by_replica_id').items()
            }
        if 'unknown_capacity_replica_ids' in dynamic_states:
            self._unknown_capacity_replica_ids = {
                int(replica_id) for replica_id in dynamic_states.pop(
                    'unknown_capacity_replica_ids')
            }
        degraded_state = dynamic_states.pop(
            'degraded_capacity_since_by_replica_id',
            dynamic_states.pop('unknown_capacity_since_by_replica_id', None))
        if degraded_state is not None:
            self._degraded_capacity_since_by_replica_id = {
                int(replica_id): float(since)
                for replica_id, since in degraded_state.items()
            }
        if dynamic_states:
            logger.info(f'Remaining dynamic states: {dynamic_states}')


class FallbackRequestRateAutoscaler(RequestRateAutoscaler):
    """FallbackRequestRateAutoscaler

    Autoscale based on request rate. It adds additional ability to
    RequestRateAutoscaler for having spot with on-demand fallback.

    When spec.base_ondemand_fallback_replicas is set, we make sure
    there are at least spec.base_ondemand_fallback_replicas on-demands
    to be always there to provide basic guarantee for the availability.

    When spec.dynamic_ondemand_fallback is set, on-demand instances
    will be scheduled to provision for any preempted spot instance, i.e.,
    on-demand instance are used as dynamic fallback of spot.
    """

    # job_recovery field is checked earlier in core
    SPOT_OVERRIDE = {'use_spot': True}
    ONDEMAND_OVERRIDE = {'use_spot': False}

    def _setup_fallback_options(self,
                                spec: 'service_spec.SkyServiceSpec') -> None:
        self.base_ondemand_fallback_replicas: int = (
            spec.base_ondemand_fallback_replicas
            if spec.base_ondemand_fallback_replicas is not None else 0)
        # Assert: Either dynamic_ondemand_fallback is set
        # or base_ondemand_fallback_replicas is greater than 0.
        assert spec.use_ondemand_fallback
        self.dynamic_ondemand_fallback: bool = (
            spec.dynamic_ondemand_fallback
            if spec.dynamic_ondemand_fallback is not None else False)

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the fallback request rate autoscaler.

        Variables:
            base_ondemand_fallback_replicas: Minimum number of on-demand
                replicas to be always there.
            dynamic_ondemand_fallback: Whether to dynamically provision
                on-demand instances for preempted spot instances.
        """
        super().__init__(service_name, spec, version)
        self._setup_fallback_options(spec)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions; don't reset fallback
            # options from a stale spec either.
            super().update_version(version, spec, update_mode=update_mode)
            return
        super().update_version(version, spec, update_mode=update_mode)
        self._setup_fallback_options(spec)

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on request rate, with on-demand
        fallback.

        The autoscaler will make sure there are at least
        `base_ondemand_fallback_replicas` on-demand replicas to be always there,
        so the service can provide basic guarantee for the availability.
        """

        self._set_target_num_replicas_with_hysteresis()

        latest_nonterminal_replicas = list(
            filter(
                lambda info: not info.is_terminal and info.version == self.
                latest_version, replica_infos))
        num_nonterminal_spot, num_ready_spot = 0, 0
        num_nonterminal_ondemand, num_ready_ondemand = 0, 0

        for info in latest_nonterminal_replicas:
            if info.is_spot:
                if info.status == serve_state.ReplicaStatus.READY:
                    num_ready_spot += 1
                num_nonterminal_spot += 1
            else:
                if info.status == serve_state.ReplicaStatus.READY:
                    num_ready_ondemand += 1
                num_nonterminal_ondemand += 1

        logger.info(
            f'Number of alive spot instances: {num_nonterminal_spot}, '
            f'Number of ready spot instances: {num_ready_spot}, '
            f'Number of alive on-demand instances: {num_nonterminal_ondemand}, '
            f'Number of ready on-demand instances: {num_ready_ondemand}')

        scaling_decisions: list[AutoscalerDecision] = []
        all_replica_ids_to_scale_down: list[int] = []

        # Decide how many spot instances to launch.
        num_spot_to_provision = (self.get_final_target_num_replicas() -
                                 self.base_ondemand_fallback_replicas)
        if num_nonterminal_spot < num_spot_to_provision:
            # Not enough spot instances, scale up.
            num_spot_to_scale_up = (num_spot_to_provision -
                                    num_nonterminal_spot)
            logger.info('Number of spot instances to scale up: '
                        f'{num_spot_to_scale_up}')
            scaling_decisions.extend(
                _generate_scale_up_decisions(num_spot_to_scale_up,
                                             self.SPOT_OVERRIDE))
        elif num_nonterminal_spot > num_spot_to_provision:
            # Too many spot instances, scale down.
            # Get the replica to scale down with _select_replicas_to_scale_down
            num_spot_to_scale_down = (num_nonterminal_spot -
                                      num_spot_to_provision)
            replicas_to_scale_down = (
                _select_nonterminal_replicas_to_scale_down(
                    num_spot_to_scale_down,
                    filter(lambda info: info.is_spot,
                           latest_nonterminal_replicas)))
            logger.info('Number of spot instances to scale down: '
                        f'{num_spot_to_scale_down} {replicas_to_scale_down}')
            all_replica_ids_to_scale_down.extend(replicas_to_scale_down)

        # Decide how many on-demand instances to launch.
        num_ondemand_to_provision = self.base_ondemand_fallback_replicas
        if self.dynamic_ondemand_fallback:
            # `num_ready_spot` instead of `num_nonterminal_spot`
            # because the provisioning spot can fail to UP due to the capacity
            # issue, and on-demand should fill the gap between the required
            # number of spot and ready spot.
            # When scaling down spot instances, it is possible that the number
            # of ready spot is more than the number of spot to provision, thus
            # generate a negative number. In this case, we don't need to
            # provision on-demand instances.
            num_ondemand_to_provision += max(
                0, num_spot_to_provision - num_ready_spot)

        # Make sure we don't launch on-demand fallback for
        # overprovisioned replicas.
        num_ondemand_to_provision = min(num_ondemand_to_provision,
                                        self.target_num_replicas)
        if num_ondemand_to_provision > num_nonterminal_ondemand:
            num_ondemand_to_scale_up = (num_ondemand_to_provision -
                                        num_nonterminal_ondemand)
            logger.info('Number of on-demand instances to scale up: '
                        f'{num_ondemand_to_scale_up}')
            scaling_decisions.extend(
                _generate_scale_up_decisions(num_ondemand_to_scale_up,
                                             self.ONDEMAND_OVERRIDE))
        else:
            num_ondemand_to_scale_down = (num_nonterminal_ondemand -
                                          num_ondemand_to_provision)
            replicas_to_scale_down = (
                _select_nonterminal_replicas_to_scale_down(
                    num_ondemand_to_scale_down,
                    filter(lambda info: not info.is_spot,
                           latest_nonterminal_replicas)))
            logger.info(
                'Number of on-demand instances to scale down: '
                f'{num_ondemand_to_scale_down} {replicas_to_scale_down}')

            all_replica_ids_to_scale_down.extend(replicas_to_scale_down)

        scaling_decisions.extend(
            _generate_scale_down_decisions(all_replica_ids_to_scale_down))

        return scaling_decisions


class QueueLengthAutoscaler(_AutoscalerWithHysteresis):
    """QueueLengthAutoscaler: Autoscale pools based on queue length.

    Scales pool workers based on the number of pending jobs in the queue.
    When queue length exceeds the threshold, scales up by 1 worker.
    When queue length is below the threshold, scales down by 1 worker.
    Uses hysteresis to prevent rapid scaling decisions.
    """

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int = constants.INITIAL_VERSION) -> None:
        """Initialize the queue length autoscaler.

        Variables:
            queue_length_threshold: Threshold for queue length to trigger
            scaling up or down.
            service_name: The pool name (used to query pending jobs).
        """
        super().__init__(service_name, spec, version)
        # Use default threshold if not specified
        self.queue_length_threshold = (
            spec.queue_length_threshold
            if spec.queue_length_threshold is not None else
            constants.AUTOSCALER_DEFAULT_QUEUE_LENGTH_THRESHOLD)
        self._service_name: str = service_name
        logger.info(f'QueueLengthAutoscaler for pool "{service_name}": '
                    f'min_replicas={self.min_replicas}, '
                    f'max_replicas={self.max_replicas}, '
                    f'queue_length_threshold={self.queue_length_threshold}')

    def _calculate_target_num_replicas(self) -> int:
        """Calculate target number of replicas based on queue length."""
        queue_length = managed_job_state.get_pending_jobs_count_by_pool(
            self._service_name)
        current_num_replicas = self.target_num_replicas

        logger.info(f'[QueueLengthAutoscaler] Pool "{self._service_name}": '
                    f'queue_length={queue_length}, '
                    f'threshold={self.queue_length_threshold}, '
                    f'current_target_replicas={current_num_replicas}, '
                    f'min_replicas={self.min_replicas}, '
                    f'max_replicas={self.max_replicas}')

        # Determine target based on queue length vs threshold
        if queue_length == 0:
            # There are no pending jobs, we should quickly scale down to 0.
            target_num_replicas = 0
            decision = 'SCALE_DOWN_TO_ZERO'
        elif queue_length > self.queue_length_threshold:
            # Scale up by 1
            # TODO(lloyd): we probably want support for scaling up by more than
            # 1 in the future. We are punting on this currently because without
            # an understanding of the workload the right number of replicas to
            # scale up by is not clear and the user can just tweak the upscale
            # delay to control the rate of scaling up.
            target_num_replicas = current_num_replicas + 1
            decision = 'SCALE_UP'
        elif queue_length < self.queue_length_threshold:
            # Scale down by 1
            target_num_replicas = current_num_replicas - 1
            decision = 'SCALE_DOWN'
        else:
            # Queue length equals threshold, keep current
            target_num_replicas = current_num_replicas
            decision = 'NO_CHANGE'
        logger.info(f'[QueueLengthAutoscaler] Decision: {decision} '
                    f'{current_num_replicas} -> {target_num_replicas}')

        # Special case: if target_num_replicas is 0 and queue_length is greater
        # than 0, we should not scale down to 0. This is to prevent the service
        # from scaling to zero when there are jobs in the queue.
        if target_num_replicas == 0 and queue_length > 0:
            target_num_replicas = 1
            logger.info('Preventing scale to zero since there are jobs in the'
                        f'queue: {queue_length}')

        clipped_target = self._clip_target_num_replicas(target_num_replicas)
        if clipped_target != target_num_replicas:
            logger.info(f'[QueueLengthAutoscaler] Clipped target: '
                        f'{target_num_replicas} -> {clipped_target} '
                        f'(bounds: [{self.min_replicas}, {self.max_replicas}])')

        return clipped_target

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        if version <= self.latest_version:
            # The base class rejects stale versions; don't update the
            # queue threshold from a stale spec either.
            super().update_version(version, spec, update_mode)
            return
        super().update_version(version, spec, update_mode)
        # Update threshold.
        if isinstance(spec.queue_length_threshold, int):
            self.queue_length_threshold = spec.queue_length_threshold

    def collect_request_information(
            self, request_aggregator_info: dict[str, Any]) -> None:
        """Collect request information from aggregator for autoscaling.

        Not needed for queue-based autoscaling, we query the job queue directly.
        """
        pass

    def _get_idle_replicas(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
        cluster_job_counts: dict[str, int] | None = None,
    ) -> list['replica_managers.ReplicaInfo']:
        """Get replicas that have no active jobs (idle replicas).

        Args:
            replica_infos: List of replica information to check.

        Returns:
            List of replicas that have no active jobs running on them.
        """
        if cluster_job_counts is None:
            cluster_job_counts = (
                managed_job_state.get_nonterminal_job_counts_by_pool(
                    self._service_name))
        idle_replicas = []
        for info in replica_infos:
            active_job_count = cluster_job_counts.get(info.cluster_name, 0)
            if active_job_count == 0:
                idle_replicas.append(info)
                logger.debug(
                    f'[QueueLengthAutoscaler] Replica {info.replica_id} '
                    f'({info.cluster_name}) is idle (no active jobs)')
            else:
                logger.debug(
                    f'[QueueLengthAutoscaler] Replica {info.replica_id} '
                    f'({info.cluster_name}) has {active_job_count} active '
                    'jobs,'
                    ' skipping for scale-down')
        return idle_replicas

    def _generate_scaling_decisions(
        self,
        replica_infos: list['replica_managers.ReplicaInfo'],
    ) -> list[AutoscalerDecision]:
        """Generate Autoscaling decisions based on queue length.

        Overrides parent to ensure we only scale down replicas that are idle
        (not running any jobs).
        """
        # Use standard hysteresis-based logic
        self._set_target_num_replicas_with_hysteresis()

        latest_nonterminal_replicas: list[replica_managers.ReplicaInfo] = []

        for info in replica_infos:
            if info.version == self.latest_version:
                if not info.is_terminal:
                    latest_nonterminal_replicas.append(info)

        scaling_decisions: list[AutoscalerDecision] = []

        # Case 1. when latest_nonterminal_replicas is less
        # than num_to_provision, we always scale up new replicas.
        target_num_replicas = self.get_final_target_num_replicas()
        if len(latest_nonterminal_replicas) < target_num_replicas:
            num_replicas_to_scale_up = (target_num_replicas -
                                        len(latest_nonterminal_replicas))
            logger.info('[QueueLengthAutoscaler] Number of replicas to scale up'
                        f': {num_replicas_to_scale_up}')
            scaling_decisions.extend(
                _generate_scale_up_decisions(num_replicas_to_scale_up, None))

        # Case 2: when latest_nonterminal_replicas is more
        # than target_num_replicas, we scale down new replicas.
        # IMPORTANT: Only scale down replicas that are idle (no active jobs).
        replicas_to_scale_down = []
        if len(latest_nonterminal_replicas) > target_num_replicas:
            num_replicas_to_scale_down = (len(latest_nonterminal_replicas) -
                                          target_num_replicas)
            cluster_job_counts = (
                managed_job_state.get_nonterminal_job_counts_by_pool(
                    self._service_name))

            # Get idle replicas (replicas with no active jobs)
            idle_replicas = self._get_idle_replicas(latest_nonterminal_replicas,
                                                    cluster_job_counts)
            num_idle_replicas = len(idle_replicas)

            # Clip the number of replicas to scale down to the number of idle
            # replicas.
            actual_num_to_scale_down = min(num_replicas_to_scale_down,
                                           num_idle_replicas)

            if actual_num_to_scale_down < num_replicas_to_scale_down:
                logger.info(
                    f'[QueueLengthAutoscaler] Clipping scale-down: requested '
                    f'{num_replicas_to_scale_down} replicas, but only '
                    f'{num_idle_replicas} idle replicas available. Scaling down'
                    f' {actual_num_to_scale_down} replicas.')

            if actual_num_to_scale_down > 0:
                # Select replicas to scale down from idle replicas only
                replicas_to_scale_down = (
                    _select_nonterminal_replicas_to_scale_down(
                        actual_num_to_scale_down, idle_replicas,
                        self._service_name, cluster_job_counts))
                logger.info(
                    f'[QueueLengthAutoscaler] Number of replicas to scale down:'
                    f' {actual_num_to_scale_down} {replicas_to_scale_down}')
            elif num_replicas_to_scale_down > 0:
                logger.info(
                    f'[QueueLengthAutoscaler] Cannot scale down: requested '
                    f'{num_replicas_to_scale_down} replicas, but all replicas '
                    'have active jobs. Skipping scale-down.')

        scaling_decisions.extend(
            _generate_scale_down_decisions(replicas_to_scale_down))

        return scaling_decisions

    def _dump_dynamic_states(self) -> dict[str, Any]:
        """Dump dynamic states from autoscaler.

        Hysteresis state is handled by base class, no additional state needed.
        """
        return {}

    def _load_dynamic_states(self, dynamic_states: dict[str, Any]) -> None:
        """Load dynamic states to autoscaler.

        Hysteresis state is handled by base class, no additional state needed.
        """
        pass


def _prepared_scaling_implementation(autoscaler: Any) -> str | None:
    """Identify an unmodified built-in shape-aware public implementation.

    The controller adapter must not assume that every Autoscaler subclass (or
    duck-typed implementation) accepts new methods or keyword arguments. A
    bound method's ``__func__`` proves the public implementation is one of the
    two built-ins whose private prepared-input path we own. Instance-level
    replacements and subclasses that override the public method therefore
    retain the historical two-positional-argument call.
    """
    public_method = getattr(autoscaler, 'generate_scaling_decisions', None)
    public_function = getattr(public_method, '__func__', None)
    if (isinstance(autoscaler, InstanceAwareRequestRateAutoscaler) and
            public_function
            is InstanceAwareRequestRateAutoscaler.generate_scaling_decisions):
        return 'instance-aware-request-rate'
    if (isinstance(autoscaler, ConcurrencyAutoscaler) and public_function
            is ConcurrencyAutoscaler.generate_scaling_decisions):
        return 'concurrency'
    return None


def prepare_controller_scaling_decision_inputs(
    autoscaler: Any,
    replica_infos: list['replica_managers.ReplicaInfo'],
) -> ScalingDecisionInputs | None:
    """Resolve blocking built-in inputs before controller serialization.

    ``None`` means the implementation owns only the historical public method
    and the controller must call that method unchanged.
    """
    if _prepared_scaling_implementation(autoscaler) is None:
        return None
    return autoscaler._prepare_scaling_decision_inputs(replica_infos)  # pylint: disable=protected-access


def controller_prepares_scaling_decision_inputs(autoscaler: Any) -> bool:
    """Whether the canonical controller adapter owns a blocking preload."""
    return _prepared_scaling_implementation(autoscaler) is not None


def generate_controller_scaling_decisions(
    autoscaler: Any,
    replica_infos: list['replica_managers.ReplicaInfo'],
    active_versions: list[int],
    decision_inputs: ScalingDecisionInputs | None,
) -> list[AutoscalerDecision]:
    """Consume prepared inputs or preserve the custom autoscaler contract."""
    if decision_inputs is None:
        return autoscaler.generate_scaling_decisions(replica_infos,
                                                     active_versions)
    if _prepared_scaling_implementation(autoscaler) is None:
        raise RuntimeError('Autoscaler scaling implementation changed while '
                           'its blocking inputs were prepared.')
    return autoscaler._generate_scaling_decisions_with_inputs(  # pylint: disable=protected-access
        replica_infos, active_versions, decision_inputs)
