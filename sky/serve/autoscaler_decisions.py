"""Data contracts emitted by SkyServe autoscaling policies."""

import dataclasses
import enum
import math
from typing import Any

from sky.serve import constants

LogicalAcceleratorState = tuple[tuple[str, int], ...]


@dataclasses.dataclass(frozen=True)
class LogicalCapacityTarget:
    """One canonical logical-capacity intent and exact accelerator catalog."""

    version: int
    generation: int
    target_capacity: int
    target_capacity_by_accelerator: LogicalAcceleratorState = ()
    accelerator_shapes: LogicalAcceleratorState = ()

    def __post_init__(self) -> None:
        if (type(self.version) is not int or self.version < 1 or
                type(self.generation) is not int or self.generation < 0 or
                type(self.target_capacity) is not int or
                self.target_capacity < 0):
            raise ValueError('Logical capacity target identity is malformed.')

        def _normalize(
            raw: LogicalAcceleratorState,
            *,
            allow_zero: bool,
        ) -> LogicalAcceleratorState:
            if type(raw) is not tuple:
                raise ValueError('Logical accelerator state must be immutable.')
            normalized: list[tuple[str, int]] = []
            seen: set[str] = set()
            for item in raw:
                if (type(item) is not tuple or len(item) != 2 or
                        not isinstance(item[0], str) or not item[0] or
                        type(item[1]) is not int or
                        item[1] < (0 if allow_zero else 1)):
                    raise ValueError('Logical accelerator state is malformed.')
                card = item[0].casefold()
                if card in seen:
                    raise ValueError('Logical accelerator state repeats a '
                                     'card.')
                seen.add(card)
                if item[1] > 0 or not allow_zero:
                    normalized.append(item)
            return tuple(normalized)

        target = _normalize(self.target_capacity_by_accelerator,
                            allow_zero=True)
        shapes = _normalize(self.accelerator_shapes, allow_zero=False)
        target_cards = {card.casefold() for card, _ in target}
        shape_cards = {card.casefold() for card, _ in shapes}
        if ((target or shapes) and
            (sum(value for _, value in target) != self.target_capacity or
             target_cards - shape_cards or
             (self.target_capacity > 0 and not target))):
            raise ValueError('Logical exact-card target does not match its '
                             'aggregate capacity and shapes.')
        object.__setattr__(self, 'target_capacity_by_accelerator', target)
        object.__setattr__(self, 'accelerator_shapes', shapes)

    @property
    def is_exact(self) -> bool:
        return bool(self.target_capacity_by_accelerator or
                    self.accelerator_shapes)

    def intent_is_preserved_by(self, current: 'LogicalCapacityTarget') -> bool:
        """Whether ``current`` is a freshness renewal of this same intent."""
        return (
            isinstance(current, LogicalCapacityTarget) and
            current.generation >= self.generation and
            (current.version, current.target_capacity,
             current.target_capacity_by_accelerator, current.accelerator_shapes)
            == (self.version, self.target_capacity,
                self.target_capacity_by_accelerator, self.accelerator_shapes))


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
    # None is the legacy call shape, where the exact-card target itself is
    # interpreted as launch authority.  New compatibility-aware decisions
    # always carry an explicit map, including an empty tuple when no paid
    # cold launch is authorized for this reconciliation generation.
    cold_launch_authority_by_accelerator: (tuple[tuple[str, int], ...] |
                                           None) = None


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
    # Logical capacity selected by the current SLA plan.  The utilization
    # gate and paid planner must share this witness: concurrency work alone
    # can be much smaller than the fleet required to meet a deadline.
    planned_replicas: int = 0

    def demonstrated_need(self) -> int:
        """Replicas this claimant can prove it is using right now."""
        per_replica = max(1e-9, float(self.work_per_replica))
        return max(
            self.busy_fill_holdings + self.pre_ready_fill_holdings,
            math.ceil(max(0.0, self.outstanding_work) / per_replica),
            0,
            self.planned_replicas,
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
        LogicalCapacityTarget,
        AutoscalerDecisionOperator,
        AutoscalerDecisionReason,
        LogicalScaleTarget,
        LogicalScaleDownTarget,
        UnrecoverableRolloutFailure,
        FillDemandSample,
        AutoscalerDecision,
):
    _contract_type.__module__ = 'sky.serve.autoscalers'
