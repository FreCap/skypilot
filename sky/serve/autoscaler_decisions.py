"""Data contracts emitted by SkyServe autoscaling policies."""

import dataclasses
import enum
import math
from typing import Any

from sky.serve import constants


class AutoscalerDecisionOperator(enum.Enum):
    SCALE_UP = 'scale_up'
    SCALE_DOWN = 'scale_down'


class AutoscalerDecisionReason(enum.Enum):
    COST_REBALANCE = 'cost_rebalance'


@dataclasses.dataclass(frozen=True)
class LogicalScaleTarget:
    """One capacity target derived from an immutable LB generation."""

    version: int
    reconcile_generation: int
    target_capacity: int
    target_capacity_by_accelerator: tuple[tuple[str, int], ...] = ()
    accelerator_shapes: tuple[tuple[str, int], ...] = ()
    replace_unknown_replica_ids: tuple[int, ...] = ()
    launch_budget: int | None = None
    launch_priority: int = constants.LB_REQUEST_PRIORITY_MIN
    launch_priority_by_accelerator: tuple[tuple[str, int], ...] = ()


@dataclasses.dataclass(frozen=True)
class LogicalScaleDownTarget:
    """One backend retirement selected against a logical target."""

    version: int
    reconcile_generation: int
    target_capacity: int
    replica_id: int
    target_capacity_by_accelerator: tuple[tuple[str, int], ...] = ()
    accelerator_shapes: tuple[tuple[str, int], ...] = ()


@dataclasses.dataclass(frozen=True)
class UnrecoverableRolloutFailure:
    """Typed evidence that a candidate version never became serviceable."""

    version: int
    reason: str


@dataclasses.dataclass(frozen=True)
class FillDemandSample:
    """Work a service can demonstrate, for the reserved-fill gate.

    Sampled by the poller thread once per poll interval, published on the
    broker claim, and consumed by the release governor. Every term is a
    RETAIN signal: any of them being non-zero keeps the claimant's
    entitlement, and only the conjunction of all of them being zero starts
    a release. That asymmetry is deliberate, because the cost of a false
    "idle" (culling a fleet that is working) is far higher than the cost of
    a false "busy" (holding capacity one dwell longer).
    """

    outstanding_work: float
    # Fill replicas individually reporting work. Per-replica on purpose: a
    # service-level "any unknown occupancy" boolean would let three flapping
    # replicas out of seventy-seven pin the whole fleet as busy forever, and
    # the gate would be silently inert on exactly the service it is for.
    busy_fill_holdings: int
    # Fill replicas in PENDING / PROVISIONING / STARTING. Boot protection:
    # these are the FIRST scale-down victims, so without this term the gate
    # would order a fleet, hold it through a 20-minute readiness delay, and
    # cull it before it served a request.
    pre_ready_fill_holdings: int
    upscale_pending: bool
    work_per_replica: float

    def demonstrated_need(self) -> int:
        """Replicas this claimant can prove it is using right now."""
        per_replica = max(1e-9, float(self.work_per_replica))
        return max(
            self.busy_fill_holdings + self.pre_ready_fill_holdings,
            math.ceil(max(0.0, self.outstanding_work) / per_replica),
        )

    def boot_hold(self) -> bool:
        """Whether a step must be deferred: authorized fleet still booting."""
        return self.pre_ready_fill_holdings > 0 or self.upscale_pending


@dataclasses.dataclass
class AutoscalerDecision:
    """Autoscaling decisions.

    |------------------------------------------------------------------------|
    | Operator   | TargetType                | Meaning                       |
    |------------|---------------------------|-------------------------------|
    | SCALE_UP   | Optional[Dict[str, Any]   | Resource override to add      |
    |------------|---------------------------|-------------------------------|
    | SCALE_DOWN | int                       | Replica id to remove          |
    |------------------------------------------------------------------------|
    """
    operator: AutoscalerDecisionOperator
    target: (dict[str, Any] | None | int | LogicalScaleTarget |
             LogicalScaleDownTarget)
    reason: AutoscalerDecisionReason | None

    # TODO(MaoZiming): Add a doc to elaborate on autoscaling policies.
    def __init__(self,
                 operator: AutoscalerDecisionOperator,
                 target: (dict[str, Any] | None | int | LogicalScaleTarget |
                          LogicalScaleDownTarget),
                 reason: AutoscalerDecisionReason | None = None):
        if operator == AutoscalerDecisionOperator.SCALE_UP:
            assert (target is None or isinstance(target,
                                                 (dict, LogicalScaleTarget)))
        else:
            assert isinstance(target, (int, LogicalScaleDownTarget))
        self.operator = operator
        self.target = target
        self.reason = reason

    def __repr__(self) -> str:
        return (f'AutoscalerDecision({self.operator}, {self.target}, '
                f'reason={self.reason})')


# The historical facade remains the serialized and introspection identity.
for _contract_type in (
        AutoscalerDecisionOperator,
        AutoscalerDecisionReason,
        LogicalScaleTarget,
        LogicalScaleDownTarget,
        UnrecoverableRolloutFailure,
        FillDemandSample,
        AutoscalerDecision,
):
    _contract_type.__module__ = 'sky.serve.autoscalers'
