"""Deterministic, side-effect-free exact-card capacity planning contracts."""

import dataclasses
import enum
import hashlib
import json
import math
import typing
from typing import Mapping, TypeVar

from sky.serve import autoscaler_compatibility
from sky.serve import compatibility_matching

CAPACITY_PLANNING_ENVELOPE_SCHEMA_VERSION = 6
_MAX_EXACT_ACCOUNTING_INTEGER = (1 << 63) - 1

_EnumT = TypeVar('_EnumT', bound=enum.Enum)


def _validate_physical_shape_accounting(
    physical_widths: 'AcceleratorCapacity',
    backend_num_nodes: int,
) -> None:
    """Keep every derived physical-backend GPU debit in BIGINT range."""
    if (type(backend_num_nodes) is not int or  # pylint: disable=unidiomatic-typecheck
            not 1 <= backend_num_nodes <= _MAX_EXACT_ACCOUNTING_INTEGER
            or any(width > _MAX_EXACT_ACCOUNTING_INTEGER // backend_num_nodes
                   for _, width in physical_widths.entries)):
        raise ValueError(
            'Physical backend shape exceeds exact accounting range.')


def _require_exact_keys(value: object, expected: frozenset[str],
                        field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(f'{field} has missing or unexpected fields.')
    if any(type(key) is not str for key in value):
        raise ValueError(f'{field} has a non-string field name.')
    return value


def _require_sequence(value: object, field: str) -> tuple[object, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f'{field} must be an array.')
    return tuple(value)


def _require_pair(value: object, field: str) -> tuple[object, object]:
    items = _require_sequence(value, field)
    if len(items) != 2:
        raise ValueError(f'{field} must contain exactly two values.')
    return items[0], items[1]


def _require_string(value: object, field: str, *, nonempty: bool = True) -> str:
    if type(value) is not str or (nonempty and not value):
        raise ValueError(f'{field} must be a string.')
    return value


def _require_int(value: object,
                 field: str,
                 *,
                 minimum: int | None = None) -> int:
    if type(value) is not int or (minimum is not None and value < minimum):
        raise ValueError(f'{field} must be an integer.')
    return value


def _require_float(value: object, field: str) -> float:
    # All floating-point domain values are normalized to JSON floats by the
    # dataclasses before they can be encoded.  Requiring the same type here
    # prevents Python's bool/int/float equality from weakening the codec.
    if type(value) is not float or not math.isfinite(value):
        raise ValueError(f'{field} must be a finite float.')
    return value


def _require_bool(value: object, field: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f'{field} must be a boolean.')
    return value


def _require_optional_int(value: object,
                          field: str,
                          *,
                          minimum: int | None = None) -> int | None:
    if value is None:
        return None
    return _require_int(value, field, minimum=minimum)


def _require_optional_float(value: object,
                            field: str,
                            *,
                            minimum: float | None = None) -> float | None:
    if value is None:
        return None
    result = _require_float(value, field)
    if minimum is not None and result < minimum:
        raise ValueError(f'{field} is below its minimum.')
    return result


def _require_sha256(value: object, field: str) -> str:
    digest = _require_string(value, field)
    if (len(digest) != 64 or
            any(character not in '0123456789abcdef' for character in digest)):
        raise ValueError(f'{field} must be a lowercase SHA-256 digest.')
    return digest


def _require_enum(value: object, enum_type: type[_EnumT], field: str) -> _EnumT:
    raw_value = _require_string(value, field)
    try:
        return enum_type(raw_value)
    except ValueError as error:
        raise ValueError(f'{field} is not a supported enum value.') from error


def _require_strings(value: object, field: str) -> tuple[str, ...]:
    return tuple(
        _require_string(item, f'{field}[]')
        for item in _require_sequence(value, field))


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(value,
                          sort_keys=True,
                          separators=(',', ':'),
                          allow_nan=False).encode()
    except (TypeError, ValueError) as error:
        raise ValueError(
            'Capacity planning payload is not canonical JSON.') from error


def build_demand_witness_scope_sha256(
    *,
    service_name: str,
    service_hash: str,
    service_lifecycle_epoch: int,
    service_version: int,
    demand_source_epoch: int,
    fill_policy_sha256: str,
) -> str:
    """Bind stable demand semantics to one service/fill-policy lifecycle."""
    _require_string(service_name, 'service_name')
    _require_string(service_hash, 'service_hash')
    _require_int(service_lifecycle_epoch, 'service_lifecycle_epoch', minimum=1)
    _require_int(service_version, 'service_version', minimum=1)
    _require_int(demand_source_epoch, 'demand_source_epoch', minimum=1)
    _require_sha256(fill_policy_sha256, 'fill_policy_sha256')
    payload = {
        'protocol': 'serve-fill-demand-witness-scope-v1',
        'service_name': service_name,
        'service_hash': service_hash,
        'service_lifecycle_epoch': service_lifecycle_epoch,
        'service_version': service_version,
        'demand_source_epoch': demand_source_epoch,
        'fill_policy_sha256': fill_policy_sha256,
    }
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


@dataclasses.dataclass(frozen=True, kw_only=True)
class AcceleratorCapacity:
    """Canonical immutable accelerator-to-capacity mapping.

    The tuple is an implementation detail of this named value object, not a
    positional domain schema.  Callers construct it from a mapping and consume
    it through named methods.
    """

    entries: tuple[tuple[str, int], ...] = ()

    def __post_init__(self) -> None:
        canonical: list[tuple[str, int]] = []
        seen: set[str] = set()
        for raw_card, raw_capacity in self.entries:
            if (not isinstance(raw_card, str) or not raw_card or
                    type(raw_capacity) is not int or raw_capacity < 0):
                raise ValueError('Accelerator capacity is malformed.')
            card = raw_card.casefold()
            if card in seen:
                raise ValueError('Accelerator capacity repeats a card.')
            seen.add(card)
            canonical.append((raw_card, raw_capacity))
        canonical.sort(key=lambda item: item[0].casefold())
        object.__setattr__(self, 'entries', tuple(canonical))

    @classmethod
    def from_mapping(cls, values: Mapping[str, int]) -> 'AcceleratorCapacity':
        return cls(entries=tuple(values.items()))

    def as_dict(self) -> dict[str, int]:
        return dict(self.entries)

    def total(self) -> int:
        return sum(capacity for _, capacity in self.entries)

    def get(self, card: str, default: int = 0) -> int:
        folded = card.casefold()
        return next((capacity for item, capacity in self.entries
                     if item.casefold() == folded), default)


@dataclasses.dataclass(frozen=True, kw_only=True)
class AcceleratorWork:
    """Canonical immutable accelerator-to-work mapping."""

    entries: tuple[tuple[str, float], ...] = ()

    def __post_init__(self) -> None:
        canonical: list[tuple[str, float]] = []
        seen: set[str] = set()
        for raw_card, raw_work in self.entries:
            if (not isinstance(raw_card, str) or not raw_card or
                    not isinstance(raw_work, (int, float)) or
                    isinstance(raw_work, bool) or
                    not math.isfinite(float(raw_work)) or raw_work < 0):
                raise ValueError('Accelerator work is malformed.')
            card = raw_card.casefold()
            if card in seen:
                raise ValueError('Accelerator work repeats a card.')
            seen.add(card)
            canonical.append((raw_card, float(raw_work)))
        canonical.sort(key=lambda item: item[0].casefold())
        object.__setattr__(self, 'entries', tuple(canonical))

    @classmethod
    def from_mapping(cls, values: Mapping[str, float]) -> 'AcceleratorWork':
        return cls(entries=tuple(values.items()))

    def as_dict(self) -> dict[str, float]:
        return dict(self.entries)


@dataclasses.dataclass(frozen=True, kw_only=True)
class CompatibilityDemand:
    """One priority bucket expressed in allocator work units."""

    sequence: int
    priority: int
    compatible_accelerators: tuple[str, ...]
    work: float

    def __post_init__(self) -> None:
        if (type(self.sequence) is not int or self.sequence < 0 or
                type(self.priority) is not int or
                not isinstance(self.work,
                               (int, float)) or isinstance(self.work, bool) or
                not math.isfinite(float(self.work)) or self.work < 0):
            raise ValueError('Compatibility demand is malformed.')
        compatible = tuple(
            sorted({
                card.casefold(): card for card in self.compatible_accelerators
            }.values(),
                   key=str.casefold))
        if self.work > 0 and not compatible:
            raise ValueError('Positive demand has no compatible accelerator.')
        object.__setattr__(self, 'compatible_accelerators', compatible)
        object.__setattr__(self, 'work', float(self.work))


@dataclasses.dataclass(frozen=True, kw_only=True)
class DemandObservationReconciliation:
    """Conservative reconciliation of primary and arrival observations."""

    reconciled_profiles: tuple[CompatibilityDemand, ...]
    incremental_arrival_work: float
    withheld_arrival_work: float
    ambiguous_fixed_arrival_work: float
    ambiguous_fixed_shelter_accelerators: tuple[str, ...]

    def __post_init__(self) -> None:
        if (not isinstance(self.reconciled_profiles, tuple) or
                any(not isinstance(item, CompatibilityDemand)
                    for item in self.reconciled_profiles) or
                len({item.sequence for item in self.reconciled_profiles
                    }) != len(self.reconciled_profiles)):
            raise ValueError('Reconciled demand profiles are malformed.')
        for field in ('incremental_arrival_work', 'withheld_arrival_work',
                      'ambiguous_fixed_arrival_work'):
            value = getattr(self, field)
            if (not isinstance(value,
                               (int, float)) or isinstance(value, bool) or
                    not math.isfinite(float(value)) or value < 0):
                raise ValueError('Demand observation reconciliation is '
                                 'malformed.')
            object.__setattr__(self, field, float(value))
        raw_shelter = self.ambiguous_fixed_shelter_accelerators
        if (not isinstance(raw_shelter, tuple) or any(
                not isinstance(card, str) or not card for card in raw_shelter)):
            raise ValueError('Demand observation shelter is malformed.')
        shelter = tuple(
            sorted({card.casefold(): card for card in raw_shelter}.values(),
                   key=str.casefold))
        object.__setattr__(self, 'ambiguous_fixed_shelter_accelerators',
                           shelter)


def reconcile_demand_observations(
    *,
    primary_profiles: tuple[CompatibilityDemand, ...],
    fixed_work: AcceleratorWork,
    arrival_profiles: tuple[CompatibilityDemand, ...],
) -> DemandObservationReconciliation:
    """Reconcile measured arrivals without inventing compatibility authority.

    The primary pressure and arrival sample are two observations of one stream.
    A request's priority and compatibility attributes are immutable across its
    arrival, queue, and rejection projections, so only an identical typed class
    can overlap. Fixed in-flight work has lost those attributes. A residual
    arrival that could describe fixed work is withheld from provider authority
    instead of being assigned an invented identity. Its compatible cards may
    shelter existing capacity until queue or rejection telemetry supplies a
    current authoritative class.

    Runtime is linear in the classified profiles and bounded accelerator
    catalog. Requests and profile pairs are never expanded.
    """
    if (not isinstance(primary_profiles, tuple) or
            any(not isinstance(item, CompatibilityDemand)
                for item in primary_profiles) or
            not isinstance(arrival_profiles, tuple) or
            any(not isinstance(item, CompatibilityDemand)
                for item in arrival_profiles) or
            not isinstance(fixed_work, AcceleratorWork)):
        raise ValueError('Demand observations are malformed.')

    names_by_folded: dict[str, set[str]] = {}
    for profile in primary_profiles + arrival_profiles:
        for card in profile.compatible_accelerators:
            names_by_folded.setdefault(card.casefold(), set()).add(card)
    for card, _ in fixed_work.entries:
        names_by_folded.setdefault(card.casefold(), set()).add(card)
    if len(names_by_folded) > 8:
        raise ValueError('Demand observations exceed the eight-card catalog.')
    canonical = {folded: min(names) for folded, names in names_by_folded.items()}

    def compatibility(profile: CompatibilityDemand) -> tuple[str, ...]:
        return tuple(canonical[card.casefold()]
                     for card in sorted(profile.compatible_accelerators,
                                        key=str.casefold))

    def aggregate(
        profiles: tuple[CompatibilityDemand, ...]
    ) -> dict[tuple[int, tuple[str, ...]], float]:
        values_by_class: dict[tuple[int, tuple[str, ...]], list[float]] = {}
        for profile in profiles:
            if profile.work <= 0:
                continue
            profile_cards = compatibility(profile)
            key = profile.priority, profile_cards
            values_by_class.setdefault(key, []).append(profile.work)
        return {
            key: math.fsum(sorted(values))
            for key, values in values_by_class.items()
        }

    primary = aggregate(primary_profiles)
    arrivals = aggregate(arrival_profiles)

    def class_key(
        item: tuple[tuple[int, tuple[str, ...]], float]
    ) -> tuple[int, int, tuple[str, ...]]:
        (priority, compatible), _ = item
        return -priority, len(compatible), tuple(map(str.casefold, compatible))

    fixed_cards = {
        canonical[card.casefold()]
        for card, work in fixed_work.entries
        if work > 1e-12
    }
    reconciled = dict(primary)
    incremental_arrival_work = 0.0
    ambiguous_fixed_arrival_work = 0.0
    ambiguous_shelter_cards: set[str] = set()
    for key, work in sorted(arrivals.items(), key=class_key):
        exact_overlap = min(work, primary.get(key, 0.0))
        residual = work - exact_overlap
        if residual <= 1e-12:
            continue
        _, compatible = key
        if fixed_cards.intersection(compatible):
            ambiguous_fixed_arrival_work += residual
            ambiguous_shelter_cards.update(compatible)
            continue
        reconciled[key] = math.fsum((reconciled.get(key, 0.0), residual))
        incremental_arrival_work += residual

    total_arrival_work = math.fsum(arrivals.values())
    withheld_arrival_work = max(0.0,
                                total_arrival_work - incremental_arrival_work)

    ordered_profiles = sorted(reconciled.items(), key=class_key)
    profiles = tuple(
        CompatibilityDemand(sequence=sequence,
                            priority=priority,
                            compatible_accelerators=compatible,
                            work=work)
        for sequence, ((priority, compatible),
                       work) in enumerate(ordered_profiles))
    return DemandObservationReconciliation(
        reconciled_profiles=profiles,
        incremental_arrival_work=max(0.0, incremental_arrival_work),
        withheld_arrival_work=max(0.0, withheld_arrival_work),
        ambiguous_fixed_arrival_work=max(0.0, ambiguous_fixed_arrival_work),
        ambiguous_fixed_shelter_accelerators=tuple(
            sorted(ambiguous_shelter_cards, key=str.casefold)))


class ActuationSupplyPolicy(enum.Enum):
    """Which finite supply the local actuation projection may reuse."""

    COLD_ATTRIBUTION = 'cold-attribution'
    REUSE_CURRENT_SUPPLY = 'reuse-current-supply'


class CapacityPlanningPurpose(enum.Enum):
    """Named adapter policy; prevents invalid boolean-mode combinations."""

    DEMAND_ATTRIBUTION = 'demand-attribution'
    LOCAL_ACTUATION = 'local-actuation'
    ECONOMIC_ADMISSION = 'economic-admission'
    FRESH_ZERO_RETENTION = 'fresh-zero-retention'


class ReservationGatePolicy(enum.Enum):
    """How authenticated reservation headroom becomes spendable."""

    NOT_CONFIGURED = 'not-configured'
    UNGATED = 'ungated'
    DEMAND_GATED = 'demand-gated'


class ReservationEvidenceState(enum.Enum):
    """Whether this snapshot has current reservation allocation evidence."""

    NOT_APPLICABLE = 'not-applicable'
    AUTHENTICATED_SETTLED = 'authenticated-settled'
    AUTHENTICATED_UNSETTLED = 'authenticated-unsettled'
    UNAVAILABLE = 'unavailable'


class ReservationDemandRelation(enum.Enum):
    """Pure classification of demand against the reservation card catalog."""

    NOT_APPLICABLE = 'not-applicable'
    COMPATIBLE = 'compatible'
    STATICALLY_DISJOINT = 'statically-disjoint'


class CapacityPlanKind(enum.Enum):
    """Closed plan variants with different authority invariants."""

    DEMAND = 'demand'
    GATE_ACQUISITION = 'gate-acquisition'
    STATIC_PREFILL = 'static-prefill'
    FRESH_ZERO_RETENTION = 'fresh-zero-retention'
    INCOMPLETE = 'incomplete'


class CapacityUnit(enum.Enum):
    """Unit conserved by one capacity target."""

    UNKNOWN = 'unknown'
    PHYSICAL_BACKEND = 'physical-backend'
    LOGICAL_GPU = 'logical-gpu'


@dataclasses.dataclass(frozen=True, kw_only=True)
class CapacityPolicyState:
    """Minimal planner-owned memory for one committed generation.

    This is deliberately a named state object rather than a tuple or a frozen
    ``dict``. Prior targets and every actuation/economic projection live only
    on the prior committed :class:`CapacityPlanCandidate`; duplicating those
    effects here would create two durable authorities. Every timestamp is a
    PostgreSQL epoch and is therefore portable across controller processes.
    """

    service_name: str
    service_version: int
    last_reduced_demand_generation: int
    capacity_unit: CapacityUnit
    maximum_capacity: int
    upscale_observations: int
    downscale_started_db_epoch: float | None
    downscale_veto_streak: int
    snap_target_on_next_recompute: bool
    adopt_total_capacity_on_next_recompute: bool
    pending_retention_floor: int | None
    pending_capacity_at_adoption: int
    pending_budget_spent: int
    paid_window_started_db_epoch: float | None
    paid_window_ceiling_by_accelerator: AcceleratorCapacity

    def __post_init__(self) -> None:
        nonnegative_values = (
            self.last_reduced_demand_generation,
            self.maximum_capacity,
            self.upscale_observations,
            self.downscale_veto_streak,
            self.pending_capacity_at_adoption,
            self.pending_budget_spent,
        )
        optional_nonnegative_values = (self.pending_retention_floor,)
        optional_times = (self.downscale_started_db_epoch,
                          self.paid_window_started_db_epoch)
        if (not isinstance(self.service_name, str) or not self.service_name or
                type(self.service_version) is not int or
                self.service_version < 1 or
                not isinstance(self.capacity_unit, CapacityUnit) or
                self.capacity_unit is CapacityUnit.UNKNOWN or any(
                    type(value) is not int or value < 0
                    for value in nonnegative_values) or
                any(value is not None and (type(value) is not int or value < 0)
                    for value in optional_nonnegative_values) or
                any(value is not None and
                    (not isinstance(value, (int, float)) or isinstance(
                        value, bool) or not math.isfinite(value) or value < 0)
                    for value in optional_times) or
                not isinstance(self.paid_window_ceiling_by_accelerator,
                               AcceleratorCapacity) or
                any(
                    type(value) is not bool for value in (
                        self.snap_target_on_next_recompute,
                        self.adopt_total_capacity_on_next_recompute,
                    ))):
            raise ValueError('Capacity policy state is malformed.')
        if ((self.paid_window_started_db_epoch is None)
                != (self.paid_window_ceiling_by_accelerator.total() == 0)):
            raise ValueError('Paid window time and ceiling must be paired.')
        object.__setattr__(self, 'downscale_started_db_epoch',
                           (None if self.downscale_started_db_epoch is None else
                            float(self.downscale_started_db_epoch)))
        object.__setattr__(self, 'paid_window_started_db_epoch',
                           (None if self.paid_window_started_db_epoch is None
                            else float(self.paid_window_started_db_epoch)))

    def canonical_payload(self) -> dict[str, object]:
        payload = dataclasses.asdict(self)
        payload['capacity_unit'] = self.capacity_unit.value
        return payload

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(
            self.canonical_payload())).hexdigest()


@dataclasses.dataclass(frozen=True, kw_only=True)
class CapacityPolicyInput:
    """Immutable fleet/config facts used to reduce one policy generation."""

    planning_db_epoch: float
    fresh_demand: bool
    pressure_latched: bool
    pressure_reasons: tuple[str, ...]
    ready_demand_owned_capacity: int
    latest_committed_capacity: int
    nonterminal_committed_capacity: int
    provisioning_demand_owned_capacity: int
    latest_committed_by_accelerator: AcceleratorCapacity
    upscale_delay_observations: int
    downscale_delay_seconds: float
    decision_interval_seconds: float
    max_downscale_pressure_vetoes: int
    scale_up_rate_percentage: int | None
    scale_up_rate_min_capacity: int
    scale_up_rate_period_seconds: float | None
    max_scale_down_rate_percentage: int
    overprovision_capacity: int

    def __post_init__(self) -> None:
        nonnegative = (
            self.ready_demand_owned_capacity,
            self.latest_committed_capacity,
            self.nonterminal_committed_capacity,
            self.provisioning_demand_owned_capacity,
            self.upscale_delay_observations,
            self.max_downscale_pressure_vetoes,
            self.scale_up_rate_min_capacity,
            self.max_scale_down_rate_percentage,
            self.overprovision_capacity,
        )
        if (not isinstance(self.planning_db_epoch, (int, float)) or
                isinstance(self.planning_db_epoch, bool) or
                not math.isfinite(self.planning_db_epoch) or
                self.planning_db_epoch < 0 or
                type(self.fresh_demand) is not bool or
                type(self.pressure_latched) is not bool or
                not isinstance(self.pressure_reasons, tuple) or not all(
                    isinstance(reason, str) and reason
                    for reason in self.pressure_reasons) or any(
                        type(value) is not int or value < 0
                        for value in nonnegative) or
                not isinstance(self.downscale_delay_seconds, (int, float)) or
                isinstance(self.downscale_delay_seconds, bool) or
                not math.isfinite(self.downscale_delay_seconds) or
                self.downscale_delay_seconds < 0 or
                not isinstance(self.decision_interval_seconds, (int, float)) or
                isinstance(self.decision_interval_seconds, bool) or
                not math.isfinite(self.decision_interval_seconds) or
                self.decision_interval_seconds <= 0 or
            (self.scale_up_rate_percentage is not None and
             (type(self.scale_up_rate_percentage) is not int or
              self.scale_up_rate_percentage < 0)) or
            (self.scale_up_rate_period_seconds is not None and
             (not isinstance(self.scale_up_rate_period_seconds, (int, float)) or
              isinstance(self.scale_up_rate_period_seconds, bool) or
              not math.isfinite(self.scale_up_rate_period_seconds) or
              self.scale_up_rate_period_seconds < 0)) or not isinstance(
                  self.latest_committed_by_accelerator, AcceleratorCapacity)):
            raise ValueError('Capacity policy input is malformed.')
        if ((self.scale_up_rate_percentage is None)
                != (self.scale_up_rate_period_seconds is None)):
            raise ValueError('Paid wave rate and period must be paired.')
        object.__setattr__(self, 'planning_db_epoch',
                           float(self.planning_db_epoch))
        object.__setattr__(self, 'downscale_delay_seconds',
                           float(self.downscale_delay_seconds))
        object.__setattr__(self, 'decision_interval_seconds',
                           float(self.decision_interval_seconds))
        object.__setattr__(self, 'scale_up_rate_period_seconds',
                           (None if self.scale_up_rate_period_seconds is None
                            else float(self.scale_up_rate_period_seconds)))


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReservationPlanningInput:
    """Locked exact-card reservation and paid-inventory evidence.

    ``authenticated_capacity`` is the complete unmaterialized allocation
    envelope. ``eligible_capacity`` is the subset this generation may commit:
    it equals the envelope when the gate is disabled, is demand-gated when the
    gate is enabled, and is empty when gate evidence is blind or unsettled.
    Pending and existing capacity are already committed inventory.  They
    remain spendable under every gate state, including ``NOT_CONFIGURED``, so
    disabling fill or losing telemetry cannot manufacture a paid replacement.
    """

    gate_policy: ReservationGatePolicy
    evidence_state: ReservationEvidenceState
    authenticated_capacity: AcceleratorCapacity
    eligible_capacity: AcceleratorCapacity
    pending_zero_cost_capacity: AcceleratorCapacity
    existing_zero_cost_capacity: AcceleratorCapacity
    existing_paid_capacity: AcceleratorCapacity
    charged_paid_gpu_units: int
    evidence_fingerprint: str
    allocation_demand_witness_sha256: str | None = None
    allocation_demonstrated_need: int | None = None
    allocation_ceiling: int = 0

    def __post_init__(self) -> None:
        capacities = (
            self.authenticated_capacity,
            self.eligible_capacity,
            self.pending_zero_cost_capacity,
            self.existing_zero_cost_capacity,
            self.existing_paid_capacity,
        )
        if (not isinstance(self.gate_policy, ReservationGatePolicy) or
                not isinstance(self.evidence_state, ReservationEvidenceState) or
                not all(
                    isinstance(value, AcceleratorCapacity)
                    for value in capacities) or
                type(self.charged_paid_gpu_units) is not int or
                self.charged_paid_gpu_units < 0 or
                not isinstance(self.evidence_fingerprint, str) or
            (self.allocation_demonstrated_need is not None and
             (type(self.allocation_demonstrated_need) is not int or
              self.allocation_demonstrated_need < 0)) or
                type(self.allocation_ceiling) is not int or
                self.allocation_ceiling < 0):
            raise ValueError('Reservation planning input is malformed.')
        if (self.allocation_demand_witness_sha256 is not None and
            (type(self.allocation_demand_witness_sha256) is not str or
             len(self.allocation_demand_witness_sha256) != 64 or
             any(character not in '0123456789abcdef'
                 for character in self.allocation_demand_witness_sha256))):
            raise ValueError('Reservation allocation witness is malformed.')
        authenticated = {
            card.casefold(): count
            for card, count in self.authenticated_capacity.entries
        }
        if any(count > authenticated.get(card.casefold(), 0)
               for card, count in self.eligible_capacity.entries):
            raise ValueError(
                'Eligible reservation capacity exceeds its envelope.')
        applicable = (self.gate_policy
                      is not ReservationGatePolicy.NOT_CONFIGURED)
        if applicable != (self.evidence_state
                          is not ReservationEvidenceState.NOT_APPLICABLE):
            raise ValueError('Reservation policy and evidence disagree.')
        if not applicable:
            # Reservation policy controls only authority for *new* zero-cost
            # capacity.  Existing zero-cost and paid replicas are ordinary
            # locked inventory and must remain visible even for a Spot-only
            # service.  Otherwise the planner can purchase replacements for
            # capacity it already owns merely because reservation fill is not
            # configured.
            if (self.authenticated_capacity.total() or
                    self.eligible_capacity.total() or
                    self.evidence_fingerprint or
                    self.allocation_demand_witness_sha256 is not None or
                    self.allocation_demonstrated_need is not None or
                    self.allocation_ceiling != 0):
                raise ValueError(
                    'An unconfigured reservation grants new capacity.')
            return
        if (not self.evidence_fingerprint or
                len(self.evidence_fingerprint) != 64 or
                any(character not in '0123456789abcdef'
                    for character in self.evidence_fingerprint)):
            raise ValueError(
                'Reservation evidence needs a canonical SHA-256 digest.')
        if (self.evidence_state is ReservationEvidenceState.UNAVAILABLE and
            (self.authenticated_capacity.total() or
             self.eligible_capacity.total())):
            raise ValueError(
                'Unavailable reservation evidence grants new capacity.')
        if (self.evidence_state
                is ReservationEvidenceState.AUTHENTICATED_UNSETTLED and
                self.eligible_capacity.total()):
            raise ValueError(
                'Unsettled reservation evidence grants new capacity.')
        if (self.gate_policy is ReservationGatePolicy.UNGATED and
                self.evidence_state
                is ReservationEvidenceState.AUTHENTICATED_SETTLED and
                self.eligible_capacity != self.authenticated_capacity):
            raise ValueError(
                'Ungated settled reservations must expose their full envelope.')
        if self.gate_policy is ReservationGatePolicy.UNGATED:
            if (self.allocation_demand_witness_sha256 is not None or
                    self.allocation_demonstrated_need is not None or
                    self.allocation_ceiling != 0):
                raise ValueError(
                    'Ungated reservation evidence carries gate authority.')
        elif self.evidence_state is ReservationEvidenceState.UNAVAILABLE:
            if (self.allocation_demand_witness_sha256 is not None or
                    self.allocation_demonstrated_need is not None or
                    self.allocation_ceiling != 0):
                raise ValueError(
                    'Unavailable reservation evidence carries gate authority.')
        elif self.evidence_state is ReservationEvidenceState.AUTHENTICATED_SETTLED:
            if (self.allocation_demand_witness_sha256 is None or
                    self.allocation_demonstrated_need is None):
                raise ValueError(
                    'Settled gated reservation lacks its demand witness.')


@dataclasses.dataclass(frozen=True, kw_only=True)
class DeadlinePlanningInput:
    """Immutable deadline demand, finite supply, and timing policy."""

    demand: tuple[autoscaler_compatibility.DeadlineDemand, ...]
    finite_supply: tuple[autoscaler_compatibility.DeadlineSupply, ...]
    service_seconds_by_accelerator: AcceleratorWork
    service_time_sources: tuple[tuple[str, str], ...]
    utilization: float
    paid_cold_lead_seconds: float

    def __post_init__(self) -> None:
        if (not isinstance(self.demand, tuple) or not all(
                isinstance(item, autoscaler_compatibility.DeadlineDemand)
                for item in self.demand) or
                not isinstance(self.finite_supply, tuple) or not all(
                    isinstance(item, autoscaler_compatibility.DeadlineSupply)
                    for item in self.finite_supply) or not isinstance(
                        self.service_seconds_by_accelerator, AcceleratorWork) or
                not isinstance(self.service_time_sources, tuple) or
                not isinstance(self.utilization, (int, float)) or
                isinstance(self.utilization, bool) or
                not 0 < self.utilization <= 1 or
                not isinstance(self.paid_cold_lead_seconds, (int, float)) or
                isinstance(self.paid_cold_lead_seconds, bool) or
                self.paid_cold_lead_seconds < 0):
            raise ValueError('Deadline planning timing policy is malformed.')
        if len({item.sequence for item in self.demand}) != len(self.demand):
            raise ValueError('Deadline demand repeats a FIFO sequence.')
        demand = tuple(sorted(self.demand, key=lambda item: item.sequence))
        supply = tuple(
            sorted(self.finite_supply,
                   key=lambda item: (item.tier, item.available_after_seconds,
                                     item.card.casefold())))
        sources_by_card: dict[str, tuple[str, str]] = {}
        for item in self.service_time_sources:
            if (not isinstance(item, tuple) or len(item) != 2 or
                    not isinstance(item[0], str) or not item[0] or
                    not isinstance(item[1], str) or not item[1] or
                    item[0].casefold() in sources_by_card):
                raise ValueError('Deadline service-time source is malformed.')
            sources_by_card[item[0].casefold()] = item
        sources = tuple(
            sorted(sources_by_card.values(),
                   key=lambda item: item[0].casefold()))
        object.__setattr__(self, 'demand', demand)
        object.__setattr__(self, 'finite_supply', supply)
        object.__setattr__(self, 'service_time_sources', sources)
        object.__setattr__(self, 'utilization', float(self.utilization))
        object.__setattr__(self, 'paid_cold_lead_seconds',
                           float(self.paid_cold_lead_seconds))


@dataclasses.dataclass(frozen=True, kw_only=True)
class CapacityPlanningSnapshot:
    """Deeply immutable inputs for one exact-card planning generation."""

    source_generation: int
    service_version: int
    configured_accelerators: tuple[str, ...]
    capacity_unit: CapacityUnit
    # Exact GPUs on each node in one launched backend.
    physical_gpu_width_by_accelerator: AcceleratorCapacity
    # Task-authoritative node count for one launched backend. Logical replica
    # semantics require one node; physical paid GPU debit is width * node count.
    backend_num_nodes: int = 1
    capacity_per_accelerator: AcceleratorWork
    floors: AcceleratorCapacity
    minimum_capacity: int
    paid_minimum_capacity: int
    actuation_minimum_capacity: int
    maximum_capacity: int
    demand_profiles: tuple[CompatibilityDemand, ...]
    explicit_demand_profiles: tuple[CompatibilityDemand, ...]
    paid_demand_profiles: tuple[CompatibilityDemand, ...]
    fixed_work: AcceleratorWork
    explicit_fixed_work: AcceleratorWork
    paid_fixed_work: AcceleratorWork
    retention_work: AcceleratorWork
    ready_zero_cost: AcceleratorCapacity
    ready: AcceleratorCapacity
    provisioning: AcceleratorCapacity
    reservation: ReservationPlanningInput
    cold_accelerator_order: tuple[str, ...]
    prospective_paid_accelerator_order: tuple[str, ...]
    planning_purpose: CapacityPlanningPurpose
    actuation_supply_policy: ActuationSupplyPolicy
    attribution_complete: bool
    planning_time: float
    max_live_paid_gpu_units: int | None
    retirement_shelter_target: AcceleratorCapacity
    deadline: DeadlinePlanningInput | None = None
    source_fingerprint: str = ''
    prior_policy_state: CapacityPolicyState | None = None
    prior_candidate: 'CapacityPlanCandidate | None' = None
    policy_input: CapacityPolicyInput | None = None
    configured_reservation_accelerators: tuple[str, ...] = ()
    demand_witness_scope_sha256: str = ''

    def __post_init__(self) -> None:
        if (type(self.source_generation) is not int or
                self.source_generation < 0 or
                type(self.service_version) is not int or
                self.service_version < 1 or
                type(self.minimum_capacity) is not int or
                self.minimum_capacity < 0 or
                type(self.paid_minimum_capacity) is not int or
                self.paid_minimum_capacity < 0 or
                self.paid_minimum_capacity > self.minimum_capacity or
                type(self.actuation_minimum_capacity) is not int or
                self.actuation_minimum_capacity < self.minimum_capacity or
                type(self.maximum_capacity) is not int or
                self.maximum_capacity < self.actuation_minimum_capacity or
                not isinstance(self.capacity_unit, CapacityUnit) or
                self.capacity_unit is CapacityUnit.UNKNOWN or
                not isinstance(self.physical_gpu_width_by_accelerator,
                               AcceleratorCapacity) or
                type(self.backend_num_nodes) is not int or
                self.backend_num_nodes < 1 or
                not isinstance(self.capacity_per_accelerator, AcceleratorWork)
                or not isinstance(self.floors, AcceleratorCapacity) or
                not isinstance(self.fixed_work, AcceleratorWork) or
                not isinstance(self.explicit_fixed_work, AcceleratorWork) or
                not isinstance(self.paid_fixed_work, AcceleratorWork) or
                not isinstance(self.retention_work, AcceleratorWork) or
                not isinstance(self.ready_zero_cost, AcceleratorCapacity) or
                not isinstance(self.ready, AcceleratorCapacity) or
                not isinstance(self.provisioning, AcceleratorCapacity) or
                not isinstance(self.reservation, ReservationPlanningInput) or
                not isinstance(self.actuation_supply_policy,
                               ActuationSupplyPolicy) or
                not isinstance(self.planning_purpose, CapacityPlanningPurpose)
                or type(self.attribution_complete) is not bool or
            (self.max_live_paid_gpu_units is not None and
             (type(self.max_live_paid_gpu_units) is not int or
              self.max_live_paid_gpu_units < 0)) or not isinstance(
                  self.retirement_shelter_target, AcceleratorCapacity) or
                not isinstance(self.source_fingerprint, str) or
                not isinstance(self.demand_witness_scope_sha256, str) or
            (self.deadline is not None and
             not isinstance(self.deadline, DeadlinePlanningInput)) or
            (self.prior_policy_state is not None and
             not isinstance(self.prior_policy_state, CapacityPolicyState)) or
            (self.prior_candidate is not None and
             not isinstance(self.prior_candidate, CapacityPlanCandidate)) or
            (self.policy_input is not None and
             not isinstance(self.policy_input, CapacityPolicyInput)) or
                not isinstance(self.planning_time, (int, float)) or isinstance(
                    self.planning_time,
                    bool) or not math.isfinite(float(self.planning_time))):
            raise ValueError(
                'Capacity planning identity or bounds are invalid.')
        _validate_physical_shape_accounting(
            self.physical_gpu_width_by_accelerator, self.backend_num_nodes)
        policy_items = (self.prior_policy_state, self.prior_candidate,
                        self.policy_input)
        if any(item is None for item in policy_items) and any(
                item is not None for item in policy_items):
            raise ValueError('Capacity policy state, candidate, and input must '
                             'be paired.')
        if self.prior_policy_state is not None:
            policy_state = self.prior_policy_state
            prior_candidate = self.prior_candidate
            assert prior_candidate is not None
            assert self.policy_input is not None
            if (policy_state.service_version != self.service_version or
                    policy_state.last_reduced_demand_generation
                    > self.source_generation or
                    policy_state.capacity_unit is not self.capacity_unit or
                    policy_state.maximum_capacity != self.maximum_capacity or
                    prior_candidate.source_generation > self.source_generation
                    or
                    prior_candidate.capacity_unit is not self.capacity_unit or
                    prior_candidate.next_policy_state != policy_state or
                    not prior_candidate.attribution_complete):
                raise ValueError('Capacity policy identity does not match its '
                                 'planning snapshot.')
        canonical = {
            card.casefold(): card
            for card in self.configured_accelerators
            if isinstance(card, str) and card
        }
        if len(canonical) != len(self.configured_accelerators):
            raise ValueError('Configured accelerators are malformed.')
        # Configured accelerator order is an explicit policy tiebreak for
        # equal-cost floors and already-materialized supply.  Preserve it;
        # only map-like inputs are canonicalized independently of order.
        configured = tuple(canonical.values())
        if not configured:
            raise ValueError('Capacity planning has no configured accelerator.')
        object.__setattr__(self, 'configured_accelerators', configured)
        if (not isinstance(self.configured_reservation_accelerators, tuple) or
                any(not isinstance(card, str) or not card or
                    card.casefold() not in canonical
                    for card in self.configured_reservation_accelerators)):
            raise ValueError(
                'Configured reservation accelerators are malformed.')
        reservation_cards = tuple(
            sorted(
                {
                    canonical[card.casefold()]
                    for card in self.configured_reservation_accelerators
                },
                key=str.casefold))
        if len(reservation_cards) != len(
                self.configured_reservation_accelerators):
            raise ValueError(
                'Configured reservation accelerators repeat a card.')
        object.__setattr__(self, 'configured_reservation_accelerators',
                           reservation_cards)
        if self.demand_witness_scope_sha256:
            _require_sha256(self.demand_witness_scope_sha256,
                            'demand_witness_scope_sha256')

        def canonical_capacity(
                value: AcceleratorCapacity) -> AcceleratorCapacity:
            return AcceleratorCapacity(entries=tuple(
                (canonical[card.casefold()], count)
                for card, count in value.entries))

        def canonical_work(value: AcceleratorWork) -> AcceleratorWork:
            return AcceleratorWork(entries=tuple(
                (canonical[card.casefold()], work)
                for card, work in value.entries))

        for field in ('physical_gpu_width_by_accelerator',
                      'capacity_per_accelerator', 'floors', 'fixed_work',
                      'explicit_fixed_work', 'paid_fixed_work',
                      'retention_work', 'ready_zero_cost', 'ready',
                      'provisioning', 'retirement_shelter_target'):
            values = getattr(self, field)
            if any(card.casefold() not in canonical
                   for card, _ in values.entries):
                raise ValueError(
                    'Capacity planning input names an unknown accelerator.')
            normalized = (canonical_work(values) if isinstance(
                values, AcceleratorWork) else canonical_capacity(values))
            object.__setattr__(self, field, normalized)

        if self.prior_policy_state is not None:
            policy_state = self.prior_policy_state
            paid_ceiling = policy_state.paid_window_ceiling_by_accelerator
            if any(card.casefold() not in canonical
                   for card, _ in paid_ceiling.entries):
                raise ValueError(
                    'Capacity policy state names an unknown accelerator.')
            normalized_policy_state = dataclasses.replace(
                policy_state,
                paid_window_ceiling_by_accelerator=canonical_capacity(
                    paid_ceiling))
            object.__setattr__(self, 'prior_policy_state',
                               normalized_policy_state)
            prior_candidate = self.prior_candidate
            assert prior_candidate is not None
            for field in _CANDIDATE_CAPACITY_FIELDS:
                values = getattr(prior_candidate, field)
                if any(card.casefold() not in canonical or
                       canonical[card.casefold()] != card
                       for card, _ in values.entries):
                    raise ValueError(
                        'Prior capacity candidate names a noncanonical card.')
            object.__setattr__(
                self, 'prior_candidate',
                dataclasses.replace(prior_candidate,
                                    next_policy_state=normalized_policy_state))
            assert self.policy_input is not None
            policy_input = self.policy_input
            latest_committed = policy_input.latest_committed_by_accelerator
            if any(card.casefold() not in canonical
                   for card, _ in latest_committed.entries):
                raise ValueError(
                    'Capacity policy input names an unknown accelerator.')
            object.__setattr__(
                self, 'policy_input',
                dataclasses.replace(policy_input,
                                    latest_committed_by_accelerator=(
                                        canonical_capacity(latest_committed))))
        reservation = self.reservation
        reservation_capacities: dict[str, AcceleratorCapacity] = {}
        for field in ('authenticated_capacity', 'eligible_capacity',
                      'pending_zero_cost_capacity',
                      'existing_zero_cost_capacity', 'existing_paid_capacity'):
            values = getattr(reservation, field)
            if any(card.casefold() not in canonical
                   for card, _ in values.entries):
                raise ValueError(
                    'Reservation planning names an unknown accelerator.')
            reservation_capacities[field] = canonical_capacity(values)
        object.__setattr__(
            self, 'reservation',
            ReservationPlanningInput(
                gate_policy=reservation.gate_policy,
                evidence_state=reservation.evidence_state,
                authenticated_capacity=reservation_capacities[
                    'authenticated_capacity'],
                eligible_capacity=reservation_capacities['eligible_capacity'],
                pending_zero_cost_capacity=reservation_capacities[
                    'pending_zero_cost_capacity'],
                existing_zero_cost_capacity=reservation_capacities[
                    'existing_zero_cost_capacity'],
                existing_paid_capacity=reservation_capacities[
                    'existing_paid_capacity'],
                charged_paid_gpu_units=reservation.charged_paid_gpu_units,
                evidence_fingerprint=reservation.evidence_fingerprint,
                allocation_demand_witness_sha256=(
                    reservation.allocation_demand_witness_sha256),
                allocation_demonstrated_need=(
                    reservation.allocation_demonstrated_need),
                allocation_ceiling=reservation.allocation_ceiling))
        if (self.reservation.gate_policy is ReservationGatePolicy.DEMAND_GATED
                and (not self.configured_reservation_accelerators or
                     not self.demand_witness_scope_sha256)):
            raise ValueError(
                'Demand-gated planning lacks its immutable witness scope.')
        current_paid_gpu_units = _capacity_gpu_units(
            capacity_unit=self.capacity_unit,
            physical_widths=(self.physical_gpu_width_by_accelerator.as_dict()),
            backend_num_nodes=self.backend_num_nodes,
            capacity=self.reservation.existing_paid_capacity)
        if (self.max_live_paid_gpu_units is not None and
                self.reservation.charged_paid_gpu_units
                < current_paid_gpu_units):
            raise ValueError(
                'Charged paid capacity omits usable paid inventory.')
        if (not isinstance(self.cold_accelerator_order, tuple) or
                any(not isinstance(card, str) or not card or
                    card.casefold() not in canonical
                    for card in self.cold_accelerator_order)):
            raise ValueError('Cold accelerator order is malformed.')
        cold = tuple(
            canonical[card.casefold()] for card in self.cold_accelerator_order)
        if len(set(map(str.casefold, cold))) != len(cold):
            raise ValueError('Cold accelerator order repeats a card.')
        cold += tuple(card for card in configured if card not in cold)
        object.__setattr__(self, 'cold_accelerator_order', cold)
        if (not isinstance(self.prospective_paid_accelerator_order, tuple) or
                any(not isinstance(card, str) or not card or
                    card.casefold() not in canonical
                    for card in self.prospective_paid_accelerator_order)):
            raise ValueError('Prospective paid accelerator order is malformed.')
        prospective = tuple(canonical[card.casefold()]
                            for card in self.prospective_paid_accelerator_order)
        if len(set(map(str.casefold, prospective))) != len(prospective):
            raise ValueError('Prospective paid accelerator order repeats a '
                             'card.')
        object.__setattr__(self, 'prospective_paid_accelerator_order',
                           prospective)
        configured_names = set(canonical)
        if ({
                card.casefold()
                for card, _ in self.physical_gpu_width_by_accelerator.entries
        } != configured_names or
                any(width <= 0 for _, width in
                    self.physical_gpu_width_by_accelerator.entries)):
            raise ValueError('Every accelerator needs a positive GPU width.')
        if (self.capacity_unit is CapacityUnit.LOGICAL_GPU and
                self.backend_num_nodes != 1):
            raise ValueError('Logical capacity requires single-node backends.')
        if ({
                card.casefold()
                for card, _ in self.capacity_per_accelerator.entries
        } != configured_names or
                any(work <= 0
                    for _, work in self.capacity_per_accelerator.entries)):
            raise ValueError(
                'Every configured accelerator needs positive capacity.')
        if self.retirement_shelter_target.total() > self.maximum_capacity:
            raise ValueError('Retirement shelter exceeds service capacity.')
        widths = self.physical_gpu_width_by_accelerator.as_dict()
        if any(count % (widths[card] if self.capacity_unit is
                        CapacityUnit.LOGICAL_GPU else 1) != 0
               for card, count in self.retirement_shelter_target.entries):
            raise ValueError('Retirement shelter is not whole-backend exact.')
        for field in ('demand_profiles', 'explicit_demand_profiles',
                      'paid_demand_profiles'):
            profiles = getattr(self, field)
            if (not isinstance(profiles, tuple) or not all(
                    isinstance(item, CompatibilityDemand) for item in profiles)
                    or any(
                        set(map(str.casefold, item.compatible_accelerators)) -
                        configured_names for item in profiles) or
                    len({item.sequence for item in profiles}) != len(profiles)):
                raise ValueError('Capacity demand profiles are malformed.')
            ordered = tuple(
                CompatibilityDemand(
                    sequence=item.sequence,
                    priority=item.priority,
                    compatible_accelerators=tuple(
                        canonical[card.casefold()]
                        for card in item.compatible_accelerators),
                    work=item.work)
                for item in sorted(profiles, key=lambda item: item.sequence))
            object.__setattr__(self, field, ordered)
        if self.deadline is not None:
            deadline_cards = {
                card.casefold() for card, _ in
                self.deadline.service_seconds_by_accelerator.entries
            }
            source_cards = {
                card.casefold()
                for card, _ in self.deadline.service_time_sources
            }
            demand_cards = {
                card.casefold()
                for item in self.deadline.demand
                for card in item.compatible_cards
            }
            supply_cards = {
                item.card.casefold() for item in self.deadline.finite_supply
            }
            if (deadline_cards != configured_names or
                    source_cards != configured_names or
                    demand_cards - configured_names or
                    supply_cards - configured_names):
                raise ValueError('Deadline input has incomplete card facts.')
            deadline = self.deadline
            object.__setattr__(
                self, 'deadline',
                DeadlinePlanningInput(
                    demand=tuple(
                        autoscaler_compatibility.DeadlineDemand(
                            sequence=item.sequence,
                            priority=item.priority,
                            compatible_cards=tuple(
                                canonical[card.casefold()]
                                for card in item.compatible_cards),
                            count=item.count,
                            remaining_seconds=item.remaining_seconds)
                        for item in deadline.demand),
                    finite_supply=tuple(
                        autoscaler_compatibility.DeadlineSupply(
                            card=canonical[item.card.casefold()],
                            available_after_seconds=(
                                item.available_after_seconds),
                            tier=item.tier) for item in deadline.finite_supply),
                    service_seconds_by_accelerator=canonical_work(
                        deadline.service_seconds_by_accelerator),
                    service_time_sources=tuple(
                        (canonical[card.casefold()], source)
                        for card, source in deadline.service_time_sources),
                    utilization=deadline.utilization,
                    paid_cold_lead_seconds=(deadline.paid_cold_lead_seconds)))
        object.__setattr__(self, 'planning_time', float(self.planning_time))

    def canonical_payload(self) -> dict[str, object]:
        """Return the closed JSON-compatible snapshot representation."""
        payload = dataclasses.asdict(self)
        payload['actuation_supply_policy'] = self.actuation_supply_policy.value
        payload['planning_purpose'] = self.planning_purpose.value
        payload['capacity_unit'] = self.capacity_unit.value
        if self.prior_policy_state is not None:
            payload['prior_policy_state']['capacity_unit'] = (
                self.prior_policy_state.capacity_unit.value)
            assert self.prior_candidate is not None
            payload['prior_candidate'] = self.prior_candidate.canonical_payload(
            )
        payload['reservation']['gate_policy'] = (
            self.reservation.gate_policy.value)
        payload['reservation']['evidence_state'] = (
            self.reservation.evidence_state.value)
        return payload

    @property
    def fingerprint(self) -> str:
        """Canonical fingerprint preserving explicit policy/FIFO ordering."""
        payload = self.canonical_payload()
        encoded = json.dumps(payload,
                             sort_keys=True,
                             separators=(',', ':'),
                             allow_nan=False).encode()
        return hashlib.sha256(encoded).hexdigest()

    @property
    def demand_fingerprint(self) -> str:
        """Fingerprint inputs shared by attribution and supply actuation."""
        payload = {
            'source_generation': self.source_generation,
            'service_version': self.service_version,
            'configured_accelerators': self.configured_accelerators,
            'configured_reservation_accelerators':
                self.configured_reservation_accelerators,
            'demand_witness_scope_sha256': self.demand_witness_scope_sha256,
            'capacity_unit': self.capacity_unit.value,
            'backend_num_nodes': self.backend_num_nodes,
            'physical_gpu_width_by_accelerator': dataclasses.asdict(
                self.physical_gpu_width_by_accelerator),
            'capacity_per_accelerator': dataclasses.asdict(
                self.capacity_per_accelerator),
            'floors': dataclasses.asdict(self.floors),
            'minimum_capacity': self.minimum_capacity,
            'paid_minimum_capacity': self.paid_minimum_capacity,
            'demand_profiles': tuple(
                dataclasses.asdict(item) for item in self.demand_profiles),
            'explicit_demand_profiles': tuple(
                dataclasses.asdict(item)
                for item in self.explicit_demand_profiles),
            'paid_demand_profiles': tuple(
                dataclasses.asdict(item) for item in self.paid_demand_profiles),
            'fixed_work': dataclasses.asdict(self.fixed_work),
            'explicit_fixed_work': dataclasses.asdict(self.explicit_fixed_work),
            'paid_fixed_work': dataclasses.asdict(self.paid_fixed_work),
            'retention_work': dataclasses.asdict(self.retention_work),
            'cold_accelerator_order': self.cold_accelerator_order,
            'prospective_paid_accelerator_order':
                self.prospective_paid_accelerator_order,
            'attribution_complete': self.attribution_complete,
            'deadline_demand': (None if self.deadline is None else tuple(
                dataclasses.asdict(item) for item in self.deadline.demand)),
            'deadline_service_seconds':
                (None if self.deadline is None else dataclasses.asdict(
                    self.deadline.service_seconds_by_accelerator)),
            'deadline_utilization':
                (None if self.deadline is None else self.deadline.utilization),
            'deadline_paid_cold_lead_seconds':
                (None if self.deadline is None else
                 self.deadline.paid_cold_lead_seconds),
        }
        encoded = json.dumps(payload,
                             sort_keys=True,
                             separators=(',', ':'),
                             allow_nan=False).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclasses.dataclass(frozen=True, kw_only=True)
class _DemandWitnessSemantics:
    """Decision-equivalent reservation-acquisition identity.

    Raw work estimates and deadline countdowns are intentionally absent.  The
    reduced acquisition classes and attribution already capture their capacity
    consequence; hashing moving inputs would make equivalent load-balancer
    heartbeats continuously revoke a reservation grant.
    """

    scope_sha256: str
    configured_accelerators: tuple[str, ...]
    configured_reservation_accelerators: tuple[str, ...]
    capacity_unit: str
    backend_num_nodes: int
    physical_gpu_width_by_accelerator: AcceleratorCapacity
    reservation_acquisition_classes: (
        tuple[compatibility_matching.CompatibilityDemand, ...] | None)
    aggregate_demand_target: int
    demand_attribution: AcceleratorCapacity

    @property
    def sha256(self) -> str:
        payload = dataclasses.asdict(self)
        payload['protocol'] = 'serve-fill-demand-witness-v5'
        return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _apply_deadline_acquisition_pins(
    classes: tuple[compatibility_matching.CompatibilityDemand, ...] | None,
    deadline_classes: tuple[compatibility_matching.CompatibilityDemand, ...],
    *,
    aggregate_demand_target: int,
) -> tuple[compatibility_matching.CompatibilityDemand, ...] | None:
    """Exact-pin only the demand units selected by the SLA planner."""
    if classes is None:
        # Deadline-only planning expresses the SLA result as exact-card floors,
        # so the ordinary reducer intentionally has no separate owner.  The
        # deadline planner's selected slots are the complete ownership proof.
        return (deadline_classes if sum(item.count for item in deadline_classes)
                == aggregate_demand_target else None)
    if not deadline_classes:
        return classes
    remaining = {
        (item.priority, item.compatible_cards): item.count for item in classes
    }
    pinned: dict[tuple[int, tuple[str, ...]], int] = {}
    for deadline in deadline_classes:
        if len(deadline.compatible_cards) != 1:
            return None
        card = deadline.compatible_cards[0]
        needed = deadline.count
        candidates = sorted(
            (key for key, count in remaining.items()
             if count > 0 and key[0] == deadline.priority and card in key[1]),
            key=lambda key: (len(key[1]), key[1]))
        for key in candidates:
            consumed = min(needed, remaining[key])
            remaining[key] -= consumed
            needed -= consumed
            if needed == 0:
                break
        if needed:
            # The deadline planner selected an exact slot but the ordinary
            # reduction cannot prove which same-priority demand unit owns it.
            return None
        pin = (deadline.priority, (card,))
        pinned[pin] = pinned.get(pin, 0) + deadline.count
    combined = dict(pinned)
    for key, count in remaining.items():
        if count > 0:
            combined[key] = combined.get(key, 0) + count
    result = tuple(
        compatibility_matching.CompatibilityDemand(
            priority=priority, compatible_cards=compatible, count=count)
        for (priority, compatible), count in sorted(
            combined.items(), key=lambda item: (-item[0][0], item[0][1])))
    if sum(item.count for item in result) != aggregate_demand_target:
        return None
    return result


def _match_supply_aware_demand(
    snapshot: CapacityPlanningSnapshot,
    classes: tuple[compatibility_matching.CompatibilityDemand, ...],
    *,
    pending_reserved: dict[str, int],
    eligible_reserved: dict[str, int],
) -> AcceleratorCapacity | None:
    """Match post-policy classes through reservation-first finite supply."""
    demand_units = sum(item.count for item in classes)
    if demand_units == 0:
        return AcceleratorCapacity()
    canonical = {
        card.casefold(): card for card in snapshot.configured_accelerators
    }
    reservation = snapshot.reservation
    supply: list[compatibility_matching.CompatibilitySupply] = []
    stable_rank = 0

    def add_tier(
        name: str,
        values: Mapping[str, int],
        *,
        preferred: bool,
        card_order: tuple[str, ...] | None = None,
    ) -> None:
        nonlocal stable_rank
        if card_order is None:
            card_order = snapshot.configured_accelerators
        for card in card_order:
            card = canonical[card.casefold()]
            count = max(0, int(values.get(card, 0)))
            if count > 0:
                supply.append(
                    compatibility_matching.CompatibilitySupply(
                        supply_id=f'{name}:{card.casefold()}',
                        card=card.casefold(),
                        capacity=count,
                        preferred_capacity=(count if preferred else 0),
                        stable_rank=stable_rank))
            stable_rank += 1

    # Existing and pending reservation holdings are retained first. Eligible
    # zero-cost capacity still precedes already-paid supply so compatible free
    # GPUs can drain surplus Spot. Existing paid then precedes cold paid.
    add_tier('existing-zero',
             reservation.existing_zero_cost_capacity.as_dict(),
             preferred=True)
    add_tier('pending-zero', pending_reserved, preferred=True)
    # `preferred_capacity` is the strict zero-cost preference: every compatible
    # free unit dominates every paid unit, including across reassignment.
    add_tier('eligible-zero', eligible_reserved, preferred=True)
    add_tier('existing-paid',
             reservation.existing_paid_capacity.as_dict(),
             preferred=False)
    prospective = {
        card.casefold() for card in snapshot.prospective_paid_accelerator_order
    }
    add_tier('cold-paid', {
        canonical[card.casefold()]: demand_units
        for card in snapshot.prospective_paid_accelerator_order
        if card.casefold() in canonical
    },
             preferred=False,
             card_order=snapshot.prospective_paid_accelerator_order)
    # A reservation-only card may retain unmatched economic demand but cannot
    # mint paid authority. This final virtual tier conserves the target; paid
    # projection remains intersected with the immutable prospective set.
    unfunded_order = tuple(
        dict.fromkeys((*snapshot.cold_accelerator_order,
                       *snapshot.configured_accelerators)))
    add_tier('unfunded', {
        canonical[card.casefold()]: demand_units
        for card in unfunded_order
        if card.casefold() in canonical and card.casefold() not in prospective
    },
             preferred=False,
             card_order=unfunded_order)
    matched = compatibility_matching.match_compatible_capacity(
        demand=classes, supply=tuple(supply))
    if any(count > 0 for _, count in matched.unmatched_by_priority):
        return None
    assigned = {
        canonical[card.casefold()]: count
        for card, count in matched.assigned_by_card
        if count > 0 and card.casefold() in canonical
    }
    result = AcceleratorCapacity.from_mapping(assigned)
    if result.total() != demand_units:
        return None
    return result


def _rebase_actuation_on_matched_demand(
    *,
    original_demand: AcceleratorCapacity,
    matched_demand: AcceleratorCapacity,
    actuation: AcceleratorCapacity,
    configured_accelerators: tuple[str, ...],
) -> AcceleratorCapacity | None:
    """Preserve zero-cost-only padding around the matched demand target."""
    if (original_demand.total() != matched_demand.total() or
            actuation.total() < original_demand.total()):
        return None
    original = original_demand.as_dict()
    desired = matched_demand.as_dict()
    old_actuation = actuation.as_dict()
    padding = actuation.total() - original_demand.total()
    for card in configured_accelerators:
        count = min(padding,
                    max(0,
                        old_actuation.get(card, 0) - original.get(card, 0)))
        if count > 0:
            desired[card] = desired.get(card, 0) + count
            padding -= count
    if padding:
        return None
    result = AcceleratorCapacity.from_mapping(desired)
    return result if result.total() == actuation.total() else None


def _acquisition_classes_cover(
    classes: tuple[compatibility_matching.CompatibilityDemand, ...],
    target: AcceleratorCapacity,
) -> bool:
    """Whether the shared matcher can realize one exact-card projection."""
    supply = tuple(
        compatibility_matching.CompatibilitySupply(
            supply_id=f'target:{index}:{card.casefold()}',
            card=card.casefold(),
            capacity=count,
            preferred_capacity=0,
            stable_rank=index)
        for index, (card, count) in enumerate(target.entries))
    matched = compatibility_matching.match_compatible_capacity(demand=classes,
                                                               supply=supply)
    return (not any(count for _, count in matched.unmatched_by_priority) and
            sum(count for _, count in matched.assigned_by_supply)
            == target.total())


def demand_witness_semantic_sha256(
    snapshot: CapacityPlanningSnapshot,
    *,
    aggregate_demand_target: int,
    demand_attribution: AcceleratorCapacity,
    reservation_acquisition_classes: (
        tuple[compatibility_matching.CompatibilityDemand, ...] | None),
) -> str:
    """Hash only stable demand/config semantics used by gate acquisition.

    Replica, reservation, Kueue, route, clock, generation, and provider-order
    observations are intentionally excluded.  Materializing capacity must not
    invalidate the demand witness that authorized it.
    """
    if (not isinstance(snapshot, CapacityPlanningSnapshot) or
            type(aggregate_demand_target) is not int or
            aggregate_demand_target < 0 or
            not isinstance(demand_attribution, AcceleratorCapacity) or
            demand_attribution.total() != aggregate_demand_target):
        raise ValueError('Demand witness semantics are malformed.')
    if (reservation_acquisition_classes is not None and
        (not isinstance(reservation_acquisition_classes, tuple) or
         any(not isinstance(item, compatibility_matching.CompatibilityDemand)
             for item in reservation_acquisition_classes) or
         sum(item.count for item in reservation_acquisition_classes)
         != aggregate_demand_target)):
        raise ValueError('Demand witness acquisition classes are malformed.')
    semantics = _DemandWitnessSemantics(
        scope_sha256=snapshot.demand_witness_scope_sha256,
        configured_accelerators=snapshot.configured_accelerators,
        configured_reservation_accelerators=(
            snapshot.configured_reservation_accelerators),
        capacity_unit=snapshot.capacity_unit.value,
        backend_num_nodes=snapshot.backend_num_nodes,
        physical_gpu_width_by_accelerator=(
            snapshot.physical_gpu_width_by_accelerator),
        reservation_acquisition_classes=reservation_acquisition_classes,
        aggregate_demand_target=aggregate_demand_target,
        demand_attribution=demand_attribution)
    return semantics.sha256


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidCapProjection:
    """Auditable service paid-cap facts for one candidate generation."""

    max_live_paid_gpu_units: int | None
    charged_paid_gpu_units: int
    remaining_paid_gpu_units: int | None

    def __post_init__(self) -> None:
        if (self.max_live_paid_gpu_units is not None and
            (type(self.max_live_paid_gpu_units) is not int or
             self.max_live_paid_gpu_units < 0)):
            raise ValueError('Paid-cap maximum is malformed.')
        if (type(self.charged_paid_gpu_units) is not int or
                self.charged_paid_gpu_units < 0 or
            (self.remaining_paid_gpu_units is not None and
             (type(self.remaining_paid_gpu_units) is not int or
              self.remaining_paid_gpu_units < 0))):
            raise ValueError('Paid-cap accounting is malformed.')
        expected_remaining = (None
                              if self.max_live_paid_gpu_units is None else max(
                                  0, self.max_live_paid_gpu_units -
                                  self.charged_paid_gpu_units))
        if self.remaining_paid_gpu_units != expected_remaining:
            raise ValueError('Paid-cap remaining capacity is inconsistent.')


@dataclasses.dataclass(frozen=True, kw_only=True)
class CapacityPlanCandidate:
    """All exact-card projections derived from one uncommitted snapshot."""

    kind: CapacityPlanKind
    capacity_unit: CapacityUnit
    physical_gpu_width_by_accelerator: AcceleratorCapacity
    backend_num_nodes: int = 1
    aggregate_demand_target: int
    raw_demand_target: int
    demand_attribution: AcceleratorCapacity
    supply_aware_demand_target: AcceleratorCapacity
    reserved_capacity_committed: AcceleratorCapacity
    new_reserved_capacity_committed: AcceleratorCapacity
    reserved_launch_target: AcceleratorCapacity
    reserved_packing_padding_target: AcceleratorCapacity
    paid_residual: AcceleratorCapacity
    paid_launch_target: AcceleratorCapacity
    paid_packing_padding_target: AcceleratorCapacity
    zero_cost_padding_target: AcceleratorCapacity
    static_prefill_target: AcceleratorCapacity
    retained_existing_target: AcceleratorCapacity
    transition_retention_target: AcceleratorCapacity
    wave_limited_actuation_target: AcceleratorCapacity
    supply_aware_actuation_target: AcceleratorCapacity
    explicit_demand_attribution: AcceleratorCapacity
    paid_demand_attribution: AcceleratorCapacity
    warm_retention_target: AcceleratorCapacity
    deadline_target: AcceleratorCapacity
    infeasible_demand_by_priority: tuple[tuple[int, float], ...]
    service_time_sources: tuple[tuple[str, str], ...]
    attribution_complete: bool
    source_generation: int
    snapshot_fingerprint: str
    demand_witness_sha256: str | None
    reservation_demand_relation: ReservationDemandRelation
    statically_disjoint_demand_accelerators: tuple[str, ...]
    paid_cap: PaidCapProjection
    retirement_floor_target: AcceleratorCapacity
    next_policy_state: CapacityPolicyState | None = None
    reservation_acquisition_classes: (
        tuple[compatibility_matching.CompatibilityDemand, ...] | None) = None

    def __post_init__(self) -> None:
        capacities = (
            self.physical_gpu_width_by_accelerator,
            self.demand_attribution,
            self.supply_aware_demand_target,
            self.reserved_capacity_committed,
            self.new_reserved_capacity_committed,
            self.reserved_launch_target,
            self.reserved_packing_padding_target,
            self.paid_residual,
            self.paid_launch_target,
            self.paid_packing_padding_target,
            self.zero_cost_padding_target,
            self.static_prefill_target,
            self.retained_existing_target,
            self.transition_retention_target,
            self.wave_limited_actuation_target,
            self.supply_aware_actuation_target,
            self.explicit_demand_attribution,
            self.paid_demand_attribution,
            self.warm_retention_target,
            self.deadline_target,
            self.retirement_floor_target,
        )
        if (not isinstance(self.kind, CapacityPlanKind) or
                not isinstance(self.capacity_unit, CapacityUnit) or
                type(self.backend_num_nodes) is not int or
                self.backend_num_nodes < 1 or
                type(self.aggregate_demand_target) is not int or
                self.aggregate_demand_target < 0 or
                type(self.raw_demand_target) is not int or
                self.raw_demand_target < 0 or not all(
                    isinstance(value, AcceleratorCapacity)
                    for value in capacities) or
                type(self.attribution_complete) is not bool or
                type(self.source_generation) is not int or
                self.source_generation < 0 or
                not isinstance(self.paid_cap, PaidCapProjection) or
                not isinstance(self.reservation_demand_relation,
                               ReservationDemandRelation) or
                not isinstance(self.snapshot_fingerprint, str) or
            (self.reservation_acquisition_classes is not None and
             (not isinstance(self.reservation_acquisition_classes, tuple) or
              any(not isinstance(item,
                                 compatibility_matching.CompatibilityDemand)
                  for item in self.reservation_acquisition_classes))) or
            (self.next_policy_state is not None and
             not isinstance(self.next_policy_state, CapacityPolicyState)) or
                not isinstance(self.infeasible_demand_by_priority, tuple) or
                not isinstance(self.service_time_sources, tuple)):
            raise ValueError('Capacity plan is malformed.')
        _validate_physical_shape_accounting(
            self.physical_gpu_width_by_accelerator, self.backend_num_nodes)
        if (self.demand_witness_sha256 is not None and
            (type(self.demand_witness_sha256) is not str or
             len(self.demand_witness_sha256) != 64 or
             any(character not in '0123456789abcdef'
                 for character in self.demand_witness_sha256))):
            raise ValueError('Capacity plan demand witness is malformed.')
        acquisition_total = (
            None if self.reservation_acquisition_classes is None else sum(
                item.count for item in self.reservation_acquisition_classes))
        if (acquisition_total is not None and
                acquisition_total != self.aggregate_demand_target):
            raise ValueError(
                'Reservation acquisition classes do not conserve demand.')
        classes = self.reservation_acquisition_classes
        if (classes is not None and any(card != card.casefold()
                                        for item in classes
                                        for card in item.compatible_cards)):
            raise ValueError('Reservation acquisition cards are not canonical.')
        disjoint_cards = tuple(
            sorted({
                card.casefold(): card
                for card in self.statically_disjoint_demand_accelerators
                if isinstance(card, str) and card
            }.values(),
                   key=str.casefold))
        if (not isinstance(self.statically_disjoint_demand_accelerators, tuple)
                or
                disjoint_cards != self.statically_disjoint_demand_accelerators):
            raise ValueError('Capacity plan disjoint demand is malformed.')
        if ((self.reservation_demand_relation
             is ReservationDemandRelation.STATICALLY_DISJOINT)
                != bool(disjoint_cards)):
            raise ValueError(
                'Capacity plan reservation-demand relation is inconsistent.')
        if (self.reservation_demand_relation
                is ReservationDemandRelation.STATICALLY_DISJOINT and
            (self.kind is not CapacityPlanKind.DEMAND or
             self.aggregate_demand_target <= 0)):
            raise ValueError('Static reservation disjointness has no demand.')
        infeasible_priorities: set[int] = set()
        for priority, work in self.infeasible_demand_by_priority:
            if (type(priority) is not int or
                    priority in infeasible_priorities or
                    not isinstance(work,
                                   (int, float)) or isinstance(work, bool) or
                    not math.isfinite(float(work)) or work < 0):
                raise ValueError('Capacity plan infeasible demand is invalid.')
            infeasible_priorities.add(priority)
        source_cards: set[str] = set()
        for card, source in self.service_time_sources:
            if (not isinstance(card, str) or not card or
                    card.casefold() in source_cards or
                    not isinstance(source, str) or not source):
                raise ValueError('Capacity plan service source is invalid.')
            source_cards.add(card.casefold())
        if not self.attribution_complete:
            if (self.kind is not CapacityPlanKind.INCOMPLETE or
                    self.capacity_unit is not CapacityUnit.UNKNOWN or
                    self.demand_witness_sha256 is not None or
                    self.reservation_acquisition_classes is not None or
                    self.reservation_demand_relation
                    is not ReservationDemandRelation.NOT_APPLICABLE or
                    self.statically_disjoint_demand_accelerators or
                    any(value.total() for value in capacities)):
                raise ValueError('Incomplete capacity plan grants authority.')
            return
        if (self.kind is CapacityPlanKind.INCOMPLETE or
                self.capacity_unit is CapacityUnit.UNKNOWN or
                self.physical_gpu_width_by_accelerator.total() <= 0):
            raise ValueError('Complete capacity plan is marked incomplete.')
        if self.demand_witness_sha256 is None:
            raise ValueError('Complete capacity plan has no demand witness.')
        if (classes is not None and not _acquisition_classes_cover(
                classes, self.demand_attribution)):
            raise ValueError(
                'Reservation acquisition classes cannot realize demand.')
        if (self.next_policy_state is not None and
            (self.next_policy_state.last_reduced_demand_generation
             > self.source_generation or
             self.next_policy_state.capacity_unit is not self.capacity_unit)):
            raise ValueError('Capacity plan next policy state has a different '
                             'identity or future demand generation.')
        if self.kind is CapacityPlanKind.GATE_ACQUISITION:
            effect_capacities = (
                self.supply_aware_demand_target,
                self.reserved_capacity_committed,
                self.new_reserved_capacity_committed,
                self.reserved_launch_target,
                self.reserved_packing_padding_target,
                self.paid_residual,
                self.paid_launch_target,
                self.paid_packing_padding_target,
                self.zero_cost_padding_target,
                self.static_prefill_target,
                self.retained_existing_target,
                self.transition_retention_target,
                self.wave_limited_actuation_target,
                self.supply_aware_actuation_target,
                self.explicit_demand_attribution,
                self.paid_demand_attribution,
                self.warm_retention_target,
                self.deadline_target,
                self.retirement_floor_target,
            )
            if (self.aggregate_demand_target <= 0 or
                    self.reservation_demand_relation
                    is not ReservationDemandRelation.COMPATIBLE or
                    self.reservation_acquisition_classes is None or
                    self.demand_attribution.total()
                    != self.aggregate_demand_target or
                    any(value.total() for value in effect_capacities)):
                raise ValueError(
                    'Gate-acquisition plan carries an actuation effect.')
            return
        if (self.demand_attribution.total() != self.aggregate_demand_target or
                self.supply_aware_demand_target.total()
                != self.aggregate_demand_target):
            raise ValueError('Capacity plan does not conserve demand.')
        if (classes is not None and not _acquisition_classes_cover(
                classes, self.supply_aware_demand_target)):
            raise ValueError('Reservation acquisition classes cannot realize '
                             'the supply-aware demand target.')
        demand = self.supply_aware_demand_target.as_dict()
        reserved = self.reserved_capacity_committed.as_dict()
        new_reserved = self.new_reserved_capacity_committed.as_dict()
        reserved_launch = self.reserved_launch_target.as_dict()
        reserved_packing_padding = (
            self.reserved_packing_padding_target.as_dict())
        paid = self.paid_residual.as_dict()
        paid_launch = self.paid_launch_target.as_dict()
        paid_packing_padding = self.paid_packing_padding_target.as_dict()
        padding = self.zero_cost_padding_target.as_dict()
        retained = self.retained_existing_target.as_dict()
        desired_actuation = self.supply_aware_actuation_target.as_dict()
        wave_actuation = self.wave_limited_actuation_target.as_dict()
        transition = self.transition_retention_target.as_dict()
        retirement_floor = self.retirement_floor_target.as_dict()
        cards = (set(demand) | set(padding) | set(retained) |
                 set(desired_actuation))
        if (self.supply_aware_actuation_target.total()
                != self.aggregate_demand_target +
                self.zero_cost_padding_target.total() +
                self.retained_existing_target.total() or any(
                    desired_actuation.get(card, 0) != demand.get(card, 0) +
                    padding.get(card, 0) + retained.get(card, 0)
                    for card in cards)):
            raise ValueError('Capacity plan does not conserve desired '
                             'actuation.')
        if (self.wave_limited_actuation_target.total()
                != self.supply_aware_actuation_target.total() or
                any(count > wave_actuation.get(card, 0)
                    for card, count in transition.items())):
            raise ValueError('Capacity plan does not conserve its actuation '
                             'wave.')
        if (self.retirement_floor_target.total()
                < self.wave_limited_actuation_target.total() or
                any(count > retirement_floor.get(card, 0)
                    for card, count in wave_actuation.items())):
            raise ValueError('Capacity plan retirement floor drops demand.')
        if self.kind is CapacityPlanKind.FRESH_ZERO_RETENTION:
            if (self.aggregate_demand_target != 0 or
                    self.zero_cost_padding_target.total() != 0 or
                    self.static_prefill_target.total() != 0 or
                    self.explicit_demand_attribution.total() != 0 or
                    self.paid_demand_attribution.total() != 0 or
                    self.reserved_capacity_committed.total() != 0 or
                    self.paid_residual.total() != 0 or
                    self.paid_launch_target.total() != 0 or
                    self.paid_packing_padding_target.total() != 0):
                raise ValueError('Fresh-zero plan carries demand authority.')
        elif self.kind is CapacityPlanKind.STATIC_PREFILL:
            if (self.aggregate_demand_target != 0 or
                    self.static_prefill_target.total() == 0 or
                    self.zero_cost_padding_target.total() != 0 or
                    self.paid_residual.total() != 0 or
                    self.paid_launch_target.total() != 0 or
                    self.paid_packing_padding_target.total() != 0):
                raise ValueError('Static prefill carries demand authority.')
        elif self.retained_existing_target.total() != 0:
            raise ValueError('Demand plan carries fresh-zero retention.')
        if self.kind is CapacityPlanKind.DEMAND:
            if (self.reservation_demand_relation
                    is ReservationDemandRelation.COMPATIBLE and
                    self.reservation_acquisition_classes is None):
                raise ValueError(
                    'Compatible demand has no reservation acquisition class.')
            if any(
                    reserved.get(card, 0) +
                    paid.get(card, 0) > demand.get(card, 0)
                    for card in set(reserved) | set(paid) | set(demand)):
                raise ValueError('Capacity funding exceeds traffic demand.')
            if any(count > reserved.get(card, 0)
                   for card, count in new_reserved.items()):
                raise ValueError('New reservation commitment is not reserved.')
            if any(count > reserved_launch.get(card, 0)
                   for card, count in new_reserved.items()):
                raise ValueError('New reservation commitment has no complete '
                                 'physical launch target.')
        elif (self.reserved_capacity_committed.total() != 0 or
              self.new_reserved_capacity_committed.total() != 0 or
              self.paid_residual.total() != 0 or
              self.paid_launch_target.total() != 0 or
              self.paid_packing_padding_target.total() != 0 or
              (self.kind is not CapacityPlanKind.STATIC_PREFILL and
               self.reserved_launch_target.total() != 0)):
            raise ValueError('A non-demand plan carries demand funding.')
        widths = self.physical_gpu_width_by_accelerator.as_dict()
        if (self.capacity_unit is CapacityUnit.LOGICAL_GPU and
                self.backend_num_nodes != 1):
            raise ValueError('Logical capacity requires single-node backends.')
        launch_cards = (set(reserved_launch) | set(new_reserved) |
                        set(reserved_packing_padding))
        if any(
                reserved_launch.get(card, 0) != new_reserved.get(card, 0) +
                reserved_packing_padding.get(card, 0) for card in launch_cards):
            raise ValueError('Reserved packing padding does not conserve its '
                             'physical launch target.')
        for card, count in reserved_launch.items():
            width = (widths.get(card, 0)
                     if self.capacity_unit is CapacityUnit.LOGICAL_GPU else 1)
            if width <= 0 or count % width != 0:
                raise ValueError('Reserved launch target is not a whole '
                                 'physical backend.')
        paid_launch_cards = (set(paid) | set(paid_launch) |
                             set(paid_packing_padding))
        paid_launch_gpu_units = 0
        for card in paid_launch_cards:
            width = (widths.get(card, 0)
                     if self.capacity_unit is CapacityUnit.LOGICAL_GPU else 1)
            residual = paid.get(card, 0)
            if width <= 0:
                raise ValueError('Paid launch target has no physical width.')
            launch = paid_launch.get(card, 0)
            packing_padding = paid_packing_padding.get(card, 0)
            authorized_residual = launch - packing_padding
            expected_launch = (0 if authorized_residual == 0 else
                               math.ceil(authorized_residual / width) * width)
            if (authorized_residual < 0 or authorized_residual > residual or
                    launch != expected_launch or
                    packing_padding != expected_launch - authorized_residual):
                raise ValueError('Paid launch target is not the minimal whole '
                                 'physical cover of its authorized residual.')
            paid_launch_gpu_units += (launch if self.capacity_unit
                                      is CapacityUnit.LOGICAL_GPU else launch *
                                      widths[card] * self.backend_num_nodes)
        if (self.paid_cap.remaining_paid_gpu_units is not None and
                paid_launch_gpu_units > self.paid_cap.remaining_paid_gpu_units):
            raise ValueError('Paid launch target exceeds paid-cap headroom.')
        for owned in (self.explicit_demand_attribution,
                      self.paid_demand_attribution):
            if any(count > demand.get(card, 0)
                   for card, count in owned.entries):
                raise ValueError('Capacity plan ownership exceeds demand.')

    @property
    def target_by_accelerator(self) -> dict[str, int]:
        return self.wave_limited_actuation_target.as_dict()

    @property
    def explicit_target_by_accelerator(self) -> dict[str, int]:
        return self.explicit_demand_attribution.as_dict()

    @property
    def paid_target_by_accelerator(self) -> dict[str, int]:
        return self.paid_demand_attribution.as_dict()

    @property
    def card_attribution_complete(self) -> bool:
        return self.attribution_complete

    def canonical_payload(self) -> dict[str, object]:
        """Return the closed JSON-compatible candidate representation."""
        payload = dataclasses.asdict(self)
        payload['kind'] = self.kind.value
        payload['capacity_unit'] = self.capacity_unit.value
        payload['reservation_demand_relation'] = (
            self.reservation_demand_relation.value)
        if self.next_policy_state is not None:
            payload['next_policy_state']['capacity_unit'] = (
                self.next_policy_state.capacity_unit.value)
        return payload

    @property
    def fingerprint(self) -> str:
        encoded = json.dumps(self.canonical_payload(),
                             sort_keys=True,
                             separators=(',', ':'),
                             allow_nan=False).encode()
        return hashlib.sha256(encoded).hexdigest()


_CAPACITY_FIELDS = frozenset({'entries'})
_COMPATIBILITY_DEMAND_FIELDS = frozenset(
    {'sequence', 'priority', 'compatible_accelerators', 'work'})
_RESERVATION_ACQUISITION_CLASS_FIELDS = frozenset(
    {'priority', 'compatible_cards', 'count'})
_DEADLINE_DEMAND_FIELDS = frozenset(
    {'sequence', 'priority', 'compatible_cards', 'count', 'remaining_seconds'})
_DEADLINE_SUPPLY_FIELDS = frozenset({'card', 'available_after_seconds', 'tier'})
_DEADLINE_FIELDS = frozenset({
    'demand', 'finite_supply', 'service_seconds_by_accelerator',
    'service_time_sources', 'utilization', 'paid_cold_lead_seconds'
})
_RESERVATION_FIELDS = frozenset({
    'gate_policy', 'evidence_state', 'authenticated_capacity',
    'eligible_capacity', 'pending_zero_cost_capacity',
    'existing_zero_cost_capacity', 'existing_paid_capacity',
    'charged_paid_gpu_units', 'evidence_fingerprint',
    'allocation_demand_witness_sha256', 'allocation_demonstrated_need',
    'allocation_ceiling'
})
_PAID_CAP_FIELDS = frozenset({
    'max_live_paid_gpu_units', 'charged_paid_gpu_units',
    'remaining_paid_gpu_units'
})
_POLICY_STATE_CAPACITY_FIELDS = ('paid_window_ceiling_by_accelerator',)
_POLICY_STATE_FIELDS = frozenset({
    'service_name',
    'service_version',
    'last_reduced_demand_generation',
    'capacity_unit',
    'maximum_capacity',
    'upscale_observations',
    'downscale_started_db_epoch',
    'downscale_veto_streak',
    'snap_target_on_next_recompute',
    'adopt_total_capacity_on_next_recompute',
    'pending_retention_floor',
    'pending_capacity_at_adoption',
    'pending_budget_spent',
    'paid_window_started_db_epoch',
    *_POLICY_STATE_CAPACITY_FIELDS,
})
_POLICY_INPUT_FIELDS = frozenset({
    'planning_db_epoch',
    'fresh_demand',
    'pressure_latched',
    'pressure_reasons',
    'ready_demand_owned_capacity',
    'latest_committed_capacity',
    'nonterminal_committed_capacity',
    'provisioning_demand_owned_capacity',
    'latest_committed_by_accelerator',
    'upscale_delay_observations',
    'downscale_delay_seconds',
    'decision_interval_seconds',
    'max_downscale_pressure_vetoes',
    'scale_up_rate_percentage',
    'scale_up_rate_min_capacity',
    'scale_up_rate_period_seconds',
    'max_scale_down_rate_percentage',
    'overprovision_capacity',
})
_SNAPSHOT_FIELDS = frozenset({
    'source_generation', 'service_version', 'configured_accelerators',
    'capacity_unit', 'physical_gpu_width_by_accelerator', 'backend_num_nodes',
    'capacity_per_accelerator', 'floors', 'minimum_capacity',
    'paid_minimum_capacity', 'actuation_minimum_capacity', 'maximum_capacity',
    'demand_profiles', 'explicit_demand_profiles', 'paid_demand_profiles',
    'fixed_work', 'explicit_fixed_work', 'paid_fixed_work', 'retention_work',
    'ready_zero_cost', 'ready', 'provisioning', 'reservation',
    'cold_accelerator_order', 'prospective_paid_accelerator_order',
    'planning_purpose', 'actuation_supply_policy', 'attribution_complete',
    'planning_time', 'max_live_paid_gpu_units', 'retirement_shelter_target',
    'deadline', 'source_fingerprint', 'prior_policy_state', 'prior_candidate',
    'policy_input', 'configured_reservation_accelerators',
    'demand_witness_scope_sha256'
})
_CANDIDATE_CAPACITY_FIELDS = (
    'physical_gpu_width_by_accelerator',
    'demand_attribution',
    'supply_aware_demand_target',
    'reserved_capacity_committed',
    'new_reserved_capacity_committed',
    'reserved_launch_target',
    'reserved_packing_padding_target',
    'paid_residual',
    'paid_launch_target',
    'paid_packing_padding_target',
    'zero_cost_padding_target',
    'static_prefill_target',
    'retained_existing_target',
    'transition_retention_target',
    'wave_limited_actuation_target',
    'supply_aware_actuation_target',
    'explicit_demand_attribution',
    'paid_demand_attribution',
    'warm_retention_target',
    'deadline_target',
    'retirement_floor_target',
)
_CANDIDATE_FIELDS = frozenset({
    'kind', 'capacity_unit', 'aggregate_demand_target', 'raw_demand_target',
    'backend_num_nodes', 'infeasible_demand_by_priority',
    'service_time_sources', 'attribution_complete', 'source_generation',
    'snapshot_fingerprint', 'paid_cap', 'next_policy_state',
    'demand_witness_sha256', 'reservation_acquisition_classes',
    'reservation_demand_relation', 'statically_disjoint_demand_accelerators',
    *_CANDIDATE_CAPACITY_FIELDS
})
_ENVELOPE_FIELDS = frozenset({
    'schema_version', 'snapshot', 'candidate', 'snapshot_fingerprint',
    'candidate_fingerprint'
})


def _decode_capacity(value: object, field: str) -> AcceleratorCapacity:
    payload = _require_exact_keys(value, _CAPACITY_FIELDS, field)
    entries: list[tuple[str, int]] = []
    for index, raw_entry in enumerate(
            _require_sequence(payload['entries'], f'{field}.entries')):
        raw_card, raw_capacity = _require_pair(raw_entry,
                                               f'{field}.entries[{index}]')
        entries.append((_require_string(raw_card,
                                        f'{field}.entries[{index}].card'),
                        _require_int(raw_capacity,
                                     f'{field}.entries[{index}].capacity',
                                     minimum=0)))
    return AcceleratorCapacity(entries=tuple(entries))


def _decode_work(value: object, field: str) -> AcceleratorWork:
    payload = _require_exact_keys(value, _CAPACITY_FIELDS, field)
    entries: list[tuple[str, float]] = []
    for index, raw_entry in enumerate(
            _require_sequence(payload['entries'], f'{field}.entries')):
        raw_card, raw_work = _require_pair(raw_entry,
                                           f'{field}.entries[{index}]')
        work = _require_float(raw_work, f'{field}.entries[{index}].work')
        if work < 0:
            raise ValueError(f'{field}.entries[{index}].work is negative.')
        entries.append(
            (_require_string(raw_card, f'{field}.entries[{index}].card'), work))
    return AcceleratorWork(entries=tuple(entries))


def _decode_compatibility_demand(value: object,
                                 field: str) -> CompatibilityDemand:
    payload = _require_exact_keys(value, _COMPATIBILITY_DEMAND_FIELDS, field)
    return CompatibilityDemand(sequence=_require_int(payload['sequence'],
                                                     f'{field}.sequence',
                                                     minimum=0),
                               priority=_require_int(payload['priority'],
                                                     f'{field}.priority'),
                               compatible_accelerators=_require_strings(
                                   payload['compatible_accelerators'],
                                   f'{field}.compatible_accelerators'),
                               work=_require_float(payload['work'],
                                                   f'{field}.work'))


def _decode_reservation(value: object, field: str) -> ReservationPlanningInput:
    payload = _require_exact_keys(value, _RESERVATION_FIELDS, field)
    return ReservationPlanningInput(
        gate_policy=_require_enum(payload['gate_policy'], ReservationGatePolicy,
                                  f'{field}.gate_policy'),
        evidence_state=_require_enum(payload['evidence_state'],
                                     ReservationEvidenceState,
                                     f'{field}.evidence_state'),
        authenticated_capacity=_decode_capacity(
            payload['authenticated_capacity'],
            f'{field}.authenticated_capacity'),
        eligible_capacity=_decode_capacity(payload['eligible_capacity'],
                                           f'{field}.eligible_capacity'),
        pending_zero_cost_capacity=_decode_capacity(
            payload['pending_zero_cost_capacity'],
            f'{field}.pending_zero_cost_capacity'),
        existing_zero_cost_capacity=_decode_capacity(
            payload['existing_zero_cost_capacity'],
            f'{field}.existing_zero_cost_capacity'),
        existing_paid_capacity=_decode_capacity(
            payload['existing_paid_capacity'],
            f'{field}.existing_paid_capacity'),
        charged_paid_gpu_units=_require_int(payload['charged_paid_gpu_units'],
                                            f'{field}.charged_paid_gpu_units',
                                            minimum=0),
        evidence_fingerprint=_require_string(payload['evidence_fingerprint'],
                                             f'{field}.evidence_fingerprint',
                                             nonempty=False),
        allocation_demand_witness_sha256=(
            None if payload['allocation_demand_witness_sha256'] is None else
            _require_sha256(payload['allocation_demand_witness_sha256'],
                            f'{field}.allocation_demand_witness_sha256')),
        allocation_demonstrated_need=_require_optional_int(
            payload['allocation_demonstrated_need'],
            f'{field}.allocation_demonstrated_need',
            minimum=0),
        allocation_ceiling=_require_int(payload['allocation_ceiling'],
                                        f'{field}.allocation_ceiling',
                                        minimum=0))


def _decode_paid_cap(value: object, field: str) -> PaidCapProjection:
    payload = _require_exact_keys(value, _PAID_CAP_FIELDS, field)
    return PaidCapProjection(max_live_paid_gpu_units=_require_optional_int(
        payload['max_live_paid_gpu_units'],
        f'{field}.max_live_paid_gpu_units',
        minimum=0),
                             charged_paid_gpu_units=_require_int(
                                 payload['charged_paid_gpu_units'],
                                 f'{field}.charged_paid_gpu_units',
                                 minimum=0),
                             remaining_paid_gpu_units=_require_optional_int(
                                 payload['remaining_paid_gpu_units'],
                                 f'{field}.remaining_paid_gpu_units',
                                 minimum=0))


def _decode_policy_state(value: object, field: str) -> CapacityPolicyState:
    payload = _require_exact_keys(value, _POLICY_STATE_FIELDS, field)
    capacities = {
        name: _decode_capacity(payload[name], f'{field}.{name}')
        for name in _POLICY_STATE_CAPACITY_FIELDS
    }
    return CapacityPolicyState(
        service_name=_require_string(payload['service_name'],
                                     f'{field}.service_name'),
        service_version=_require_int(payload['service_version'],
                                     f'{field}.service_version',
                                     minimum=1),
        last_reduced_demand_generation=_require_int(
            payload['last_reduced_demand_generation'],
            f'{field}.last_reduced_demand_generation',
            minimum=0),
        capacity_unit=_require_enum(payload['capacity_unit'], CapacityUnit,
                                    f'{field}.capacity_unit'),
        maximum_capacity=_require_int(payload['maximum_capacity'],
                                      f'{field}.maximum_capacity',
                                      minimum=0),
        upscale_observations=_require_int(payload['upscale_observations'],
                                          f'{field}.upscale_observations',
                                          minimum=0),
        downscale_started_db_epoch=_require_optional_float(
            payload['downscale_started_db_epoch'],
            f'{field}.downscale_started_db_epoch',
            minimum=0),
        downscale_veto_streak=_require_int(payload['downscale_veto_streak'],
                                           f'{field}.downscale_veto_streak',
                                           minimum=0),
        snap_target_on_next_recompute=_require_bool(
            payload['snap_target_on_next_recompute'],
            f'{field}.snap_target_on_next_recompute'),
        adopt_total_capacity_on_next_recompute=_require_bool(
            payload['adopt_total_capacity_on_next_recompute'],
            f'{field}.adopt_total_capacity_on_next_recompute'),
        pending_retention_floor=_require_optional_int(
            payload['pending_retention_floor'],
            f'{field}.pending_retention_floor',
            minimum=0),
        pending_capacity_at_adoption=_require_int(
            payload['pending_capacity_at_adoption'],
            f'{field}.pending_capacity_at_adoption',
            minimum=0),
        pending_budget_spent=_require_int(payload['pending_budget_spent'],
                                          f'{field}.pending_budget_spent',
                                          minimum=0),
        paid_window_started_db_epoch=_require_optional_float(
            payload['paid_window_started_db_epoch'],
            f'{field}.paid_window_started_db_epoch',
            minimum=0),
        **capacities)


def _decode_policy_input(value: object, field: str) -> CapacityPolicyInput:
    payload = _require_exact_keys(value, _POLICY_INPUT_FIELDS, field)
    return CapacityPolicyInput(
        planning_db_epoch=_require_float(payload['planning_db_epoch'],
                                         f'{field}.planning_db_epoch'),
        fresh_demand=_require_bool(payload['fresh_demand'],
                                   f'{field}.fresh_demand'),
        pressure_latched=_require_bool(payload['pressure_latched'],
                                       f'{field}.pressure_latched'),
        pressure_reasons=_require_strings(payload['pressure_reasons'],
                                          f'{field}.pressure_reasons'),
        ready_demand_owned_capacity=_require_int(
            payload['ready_demand_owned_capacity'],
            f'{field}.ready_demand_owned_capacity',
            minimum=0),
        latest_committed_capacity=_require_int(
            payload['latest_committed_capacity'],
            f'{field}.latest_committed_capacity',
            minimum=0),
        nonterminal_committed_capacity=_require_int(
            payload['nonterminal_committed_capacity'],
            f'{field}.nonterminal_committed_capacity',
            minimum=0),
        provisioning_demand_owned_capacity=_require_int(
            payload['provisioning_demand_owned_capacity'],
            f'{field}.provisioning_demand_owned_capacity',
            minimum=0),
        latest_committed_by_accelerator=_decode_capacity(
            payload['latest_committed_by_accelerator'],
            f'{field}.latest_committed_by_accelerator'),
        upscale_delay_observations=_require_int(
            payload['upscale_delay_observations'],
            f'{field}.upscale_delay_observations',
            minimum=0),
        downscale_delay_seconds=_require_float(
            payload['downscale_delay_seconds'],
            f'{field}.downscale_delay_seconds'),
        decision_interval_seconds=_require_float(
            payload['decision_interval_seconds'],
            f'{field}.decision_interval_seconds'),
        max_downscale_pressure_vetoes=_require_int(
            payload['max_downscale_pressure_vetoes'],
            f'{field}.max_downscale_pressure_vetoes',
            minimum=0),
        scale_up_rate_percentage=_require_optional_int(
            payload['scale_up_rate_percentage'],
            f'{field}.scale_up_rate_percentage',
            minimum=0),
        scale_up_rate_min_capacity=_require_int(
            payload['scale_up_rate_min_capacity'],
            f'{field}.scale_up_rate_min_capacity',
            minimum=0),
        scale_up_rate_period_seconds=_require_optional_float(
            payload['scale_up_rate_period_seconds'],
            f'{field}.scale_up_rate_period_seconds',
            minimum=0),
        max_scale_down_rate_percentage=_require_int(
            payload['max_scale_down_rate_percentage'],
            f'{field}.max_scale_down_rate_percentage',
            minimum=0),
        overprovision_capacity=_require_int(payload['overprovision_capacity'],
                                            f'{field}.overprovision_capacity',
                                            minimum=0))


def _decode_deadline_demand(
        value: object, field: str) -> autoscaler_compatibility.DeadlineDemand:
    payload = _require_exact_keys(value, _DEADLINE_DEMAND_FIELDS, field)
    return autoscaler_compatibility.DeadlineDemand(
        sequence=_require_int(payload['sequence'],
                              f'{field}.sequence',
                              minimum=0),
        priority=_require_int(payload['priority'], f'{field}.priority'),
        compatible_cards=_require_strings(payload['compatible_cards'],
                                          f'{field}.compatible_cards'),
        count=_require_int(payload['count'], f'{field}.count', minimum=0),
        remaining_seconds=_require_float(payload['remaining_seconds'],
                                         f'{field}.remaining_seconds'))


def _decode_deadline_supply(
        value: object, field: str) -> autoscaler_compatibility.DeadlineSupply:
    payload = _require_exact_keys(value, _DEADLINE_SUPPLY_FIELDS, field)
    return autoscaler_compatibility.DeadlineSupply(
        card=_require_string(payload['card'], f'{field}.card'),
        available_after_seconds=_require_float(
            payload['available_after_seconds'],
            f'{field}.available_after_seconds'),
        tier=_require_int(payload['tier'], f'{field}.tier', minimum=0))


def _decode_string_pairs(value: object,
                         field: str) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = []
    for index, raw_pair in enumerate(_require_sequence(value, field)):
        raw_first, raw_second = _require_pair(raw_pair, f'{field}[{index}]')
        pairs.append((_require_string(raw_first, f'{field}[{index}][0]'),
                      _require_string(raw_second, f'{field}[{index}][1]')))
    return tuple(pairs)


def _decode_deadline(value: object, field: str) -> DeadlinePlanningInput:
    payload = _require_exact_keys(value, _DEADLINE_FIELDS, field)
    demand = tuple(
        _decode_deadline_demand(item, f'{field}.demand[{index}]')
        for index, item in enumerate(
            _require_sequence(payload['demand'], f'{field}.demand')))
    finite_supply = tuple(
        _decode_deadline_supply(item, f'{field}.finite_supply[{index}]')
        for index, item in enumerate(
            _require_sequence(payload['finite_supply'],
                              f'{field}.finite_supply')))
    utilization = _require_float(payload['utilization'], f'{field}.utilization')
    paid_cold_lead_seconds = _require_float(payload['paid_cold_lead_seconds'],
                                            f'{field}.paid_cold_lead_seconds')
    return DeadlinePlanningInput(demand=demand,
                                 finite_supply=finite_supply,
                                 service_seconds_by_accelerator=_decode_work(
                                     payload['service_seconds_by_accelerator'],
                                     f'{field}.service_seconds_by_accelerator'),
                                 service_time_sources=_decode_string_pairs(
                                     payload['service_time_sources'],
                                     f'{field}.service_time_sources'),
                                 utilization=utilization,
                                 paid_cold_lead_seconds=paid_cold_lead_seconds)


def _decode_profiles(value: object,
                     field: str) -> tuple[CompatibilityDemand, ...]:
    return tuple(
        _decode_compatibility_demand(item, f'{field}[{index}]')
        for index, item in enumerate(_require_sequence(value, field)))


def _decode_snapshot(value: object) -> CapacityPlanningSnapshot:
    payload = _require_exact_keys(value, _SNAPSHOT_FIELDS, 'snapshot')
    raw_deadline = payload['deadline']
    if raw_deadline is not None and not isinstance(raw_deadline, Mapping):
        raise ValueError('snapshot.deadline must be null or an object.')
    raw_prior_policy_state = payload['prior_policy_state']
    if (raw_prior_policy_state is not None and
            not isinstance(raw_prior_policy_state, Mapping)):
        raise ValueError(
            'snapshot.prior_policy_state must be null or an object.')
    raw_prior_candidate = payload['prior_candidate']
    if (raw_prior_candidate is not None and
            not isinstance(raw_prior_candidate, Mapping)):
        raise ValueError('snapshot.prior_candidate must be null or an object.')
    raw_policy_input = payload['policy_input']
    if raw_policy_input is not None and not isinstance(raw_policy_input,
                                                       Mapping):
        raise ValueError('snapshot.policy_input must be null or an object.')
    source_fingerprint = _require_string(payload['source_fingerprint'],
                                         'snapshot.source_fingerprint',
                                         nonempty=False)
    if source_fingerprint:
        _require_sha256(source_fingerprint, 'snapshot.source_fingerprint')
    return CapacityPlanningSnapshot(
        source_generation=_require_int(payload['source_generation'],
                                       'snapshot.source_generation',
                                       minimum=0),
        service_version=_require_int(payload['service_version'],
                                     'snapshot.service_version',
                                     minimum=1),
        configured_accelerators=_require_strings(
            payload['configured_accelerators'],
            'snapshot.configured_accelerators'),
        capacity_unit=_require_enum(payload['capacity_unit'], CapacityUnit,
                                    'snapshot.capacity_unit'),
        backend_num_nodes=_require_int(payload['backend_num_nodes'],
                                       'snapshot.backend_num_nodes',
                                       minimum=1),
        physical_gpu_width_by_accelerator=_decode_capacity(
            payload['physical_gpu_width_by_accelerator'],
            'snapshot.physical_gpu_width_by_accelerator'),
        capacity_per_accelerator=_decode_work(
            payload['capacity_per_accelerator'],
            'snapshot.capacity_per_accelerator'),
        floors=_decode_capacity(payload['floors'], 'snapshot.floors'),
        minimum_capacity=_require_int(payload['minimum_capacity'],
                                      'snapshot.minimum_capacity',
                                      minimum=0),
        paid_minimum_capacity=_require_int(payload['paid_minimum_capacity'],
                                           'snapshot.paid_minimum_capacity',
                                           minimum=0),
        actuation_minimum_capacity=_require_int(
            payload['actuation_minimum_capacity'],
            'snapshot.actuation_minimum_capacity',
            minimum=0),
        maximum_capacity=_require_int(payload['maximum_capacity'],
                                      'snapshot.maximum_capacity',
                                      minimum=0),
        demand_profiles=_decode_profiles(payload['demand_profiles'],
                                         'snapshot.demand_profiles'),
        explicit_demand_profiles=_decode_profiles(
            payload['explicit_demand_profiles'],
            'snapshot.explicit_demand_profiles'),
        paid_demand_profiles=_decode_profiles(payload['paid_demand_profiles'],
                                              'snapshot.paid_demand_profiles'),
        fixed_work=_decode_work(payload['fixed_work'], 'snapshot.fixed_work'),
        explicit_fixed_work=_decode_work(payload['explicit_fixed_work'],
                                         'snapshot.explicit_fixed_work'),
        paid_fixed_work=_decode_work(payload['paid_fixed_work'],
                                     'snapshot.paid_fixed_work'),
        retention_work=_decode_work(payload['retention_work'],
                                    'snapshot.retention_work'),
        ready_zero_cost=_decode_capacity(payload['ready_zero_cost'],
                                         'snapshot.ready_zero_cost'),
        ready=_decode_capacity(payload['ready'], 'snapshot.ready'),
        provisioning=_decode_capacity(payload['provisioning'],
                                      'snapshot.provisioning'),
        reservation=_decode_reservation(payload['reservation'],
                                        'snapshot.reservation'),
        cold_accelerator_order=_require_strings(
            payload['cold_accelerator_order'],
            'snapshot.cold_accelerator_order'),
        prospective_paid_accelerator_order=_require_strings(
            payload['prospective_paid_accelerator_order'],
            'snapshot.prospective_paid_accelerator_order'),
        planning_purpose=_require_enum(payload['planning_purpose'],
                                       CapacityPlanningPurpose,
                                       'snapshot.planning_purpose'),
        actuation_supply_policy=_require_enum(
            payload['actuation_supply_policy'], ActuationSupplyPolicy,
            'snapshot.actuation_supply_policy'),
        attribution_complete=_require_bool(payload['attribution_complete'],
                                           'snapshot.attribution_complete'),
        planning_time=_require_float(payload['planning_time'],
                                     'snapshot.planning_time'),
        max_live_paid_gpu_units=_require_optional_int(
            payload['max_live_paid_gpu_units'],
            'snapshot.max_live_paid_gpu_units',
            minimum=0),
        retirement_shelter_target=_decode_capacity(
            payload['retirement_shelter_target'],
            'snapshot.retirement_shelter_target'),
        deadline=(None if raw_deadline is None else _decode_deadline(
            raw_deadline, 'snapshot.deadline')),
        source_fingerprint=source_fingerprint,
        prior_policy_state=(
            None if raw_prior_policy_state is None else _decode_policy_state(
                raw_prior_policy_state, 'snapshot.prior_policy_state')),
        prior_candidate=(None if raw_prior_candidate is None else
                         _decode_candidate(raw_prior_candidate)),
        policy_input=(None
                      if raw_policy_input is None else _decode_policy_input(
                          raw_policy_input, 'snapshot.policy_input')),
        configured_reservation_accelerators=_require_strings(
            payload['configured_reservation_accelerators'],
            'snapshot.configured_reservation_accelerators'),
        demand_witness_scope_sha256=_require_string(
            payload['demand_witness_scope_sha256'],
            'snapshot.demand_witness_scope_sha256',
            nonempty=False))


def _decode_infeasible_demand(value: object) -> tuple[tuple[int, float], ...]:
    result: list[tuple[int, float]] = []
    for index, raw_pair in enumerate(
            _require_sequence(value,
                              'candidate.infeasible_demand_by_priority')):
        raw_priority, raw_work = _require_pair(
            raw_pair, f'candidate.infeasible_demand_by_priority[{index}]')
        work = _require_float(
            raw_work, f'candidate.infeasible_demand_by_priority[{index}].work')
        if work < 0:
            raise ValueError('Candidate infeasible demand is negative.')
        result.append((_require_int(
            raw_priority,
            f'candidate.infeasible_demand_by_priority[{index}].priority'),
                       work))
    return tuple(result)


def _decode_reservation_acquisition_classes(
    value: object,
) -> tuple[compatibility_matching.CompatibilityDemand, ...] | None:
    if value is None:
        return None
    result = []
    for index, raw_item in enumerate(
            _require_sequence(value,
                              'candidate.reservation_acquisition_classes')):
        field = f'candidate.reservation_acquisition_classes[{index}]'
        payload = _require_exact_keys(raw_item,
                                      _RESERVATION_ACQUISITION_CLASS_FIELDS,
                                      field)
        result.append(
            compatibility_matching.CompatibilityDemand(
                priority=_require_int(payload['priority'], f'{field}.priority'),
                compatible_cards=_require_strings(payload['compatible_cards'],
                                                  f'{field}.compatible_cards'),
                count=_require_int(payload['count'],
                                   f'{field}.count',
                                   minimum=1)))
    return tuple(result)


def _decode_candidate(value: object) -> CapacityPlanCandidate:
    payload = _require_exact_keys(value, _CANDIDATE_FIELDS, 'candidate')
    raw_next_policy_state = payload['next_policy_state']
    if (raw_next_policy_state is not None and
            not isinstance(raw_next_policy_state, Mapping)):
        raise ValueError(
            'candidate.next_policy_state must be null or an object.')
    capacities = {
        field: _decode_capacity(payload[field], f'candidate.{field}')
        for field in _CANDIDATE_CAPACITY_FIELDS
    }
    return CapacityPlanCandidate(
        kind=_require_enum(payload['kind'], CapacityPlanKind, 'candidate.kind'),
        capacity_unit=_require_enum(payload['capacity_unit'], CapacityUnit,
                                    'candidate.capacity_unit'),
        backend_num_nodes=_require_int(payload['backend_num_nodes'],
                                       'candidate.backend_num_nodes',
                                       minimum=1),
        aggregate_demand_target=_require_int(
            payload['aggregate_demand_target'],
            'candidate.aggregate_demand_target',
            minimum=0),
        raw_demand_target=_require_int(payload['raw_demand_target'],
                                       'candidate.raw_demand_target',
                                       minimum=0),
        infeasible_demand_by_priority=_decode_infeasible_demand(
            payload['infeasible_demand_by_priority']),
        service_time_sources=_decode_string_pairs(
            payload['service_time_sources'], 'candidate.service_time_sources'),
        attribution_complete=_require_bool(payload['attribution_complete'],
                                           'candidate.attribution_complete'),
        source_generation=_require_int(payload['source_generation'],
                                       'candidate.source_generation',
                                       minimum=0),
        snapshot_fingerprint=_require_sha256(payload['snapshot_fingerprint'],
                                             'candidate.snapshot_fingerprint'),
        demand_witness_sha256=(None if payload['demand_witness_sha256'] is None
                               else _require_sha256(
                                   payload['demand_witness_sha256'],
                                   'candidate.demand_witness_sha256')),
        reservation_acquisition_classes=(
            _decode_reservation_acquisition_classes(
                payload['reservation_acquisition_classes'])),
        reservation_demand_relation=_require_enum(
            payload['reservation_demand_relation'], ReservationDemandRelation,
            'candidate.reservation_demand_relation'),
        statically_disjoint_demand_accelerators=_require_strings(
            payload['statically_disjoint_demand_accelerators'],
            'candidate.statically_disjoint_demand_accelerators'),
        paid_cap=_decode_paid_cap(payload['paid_cap'], 'candidate.paid_cap'),
        next_policy_state=(None if raw_next_policy_state is None else
                           _decode_policy_state(raw_next_policy_state,
                                                'candidate.next_policy_state')),
        **capacities)


@dataclasses.dataclass(frozen=True, kw_only=True)
class CapacityPlanningEnvelope:
    """A typed planner decision and the exact immutable input that produced it."""

    schema_version: int
    snapshot: CapacityPlanningSnapshot
    candidate: CapacityPlanCandidate

    def __post_init__(self) -> None:
        if (type(self.schema_version) is not int or self.schema_version
                != CAPACITY_PLANNING_ENVELOPE_SCHEMA_VERSION or
                not isinstance(self.snapshot, CapacityPlanningSnapshot) or
                not isinstance(self.candidate, CapacityPlanCandidate)):
            raise ValueError('Capacity planning envelope is malformed.')
        snapshot_fingerprint = self.snapshot.fingerprint
        if self.candidate.snapshot_fingerprint != snapshot_fingerprint:
            raise ValueError('Capacity candidate names a different snapshot.')
        if self.candidate.source_generation != self.snapshot.source_generation:
            raise ValueError('Capacity candidate names a different generation.')
        prior_policy_state = self.snapshot.prior_policy_state
        next_policy_state = self.candidate.next_policy_state
        acquiring_gate = (self.candidate.kind
                          is CapacityPlanKind.GATE_ACQUISITION)
        if ((prior_policy_state is None) != (next_policy_state is None)):
            raise ValueError('Capacity candidate changes policy-state mode.')
        if (acquiring_gate and prior_policy_state is not None and
                next_policy_state != prior_policy_state):
            raise ValueError('Gate acquisition mutates capacity policy state.')
        if prior_policy_state is not None:
            assert next_policy_state is not None
            if (next_policy_state.service_name
                    != prior_policy_state.service_name or
                    next_policy_state.service_version
                    != self.snapshot.service_version or
                    next_policy_state.last_reduced_demand_generation
                    > self.snapshot.source_generation or
                    next_policy_state.capacity_unit
                    is not self.snapshot.capacity_unit or
                    next_policy_state.maximum_capacity
                    != self.snapshot.maximum_capacity):
                raise ValueError('Capacity candidate changes policy identity.')
        if (self.candidate.attribution_complete and
            (self.candidate.capacity_unit is not self.snapshot.capacity_unit or
             self.candidate.backend_num_nodes != self.snapshot.backend_num_nodes
             or self.candidate.physical_gpu_width_by_accelerator
             != self.snapshot.physical_gpu_width_by_accelerator)):
            raise ValueError('Capacity candidate changes the snapshot units.')
        if self.candidate.attribution_complete:
            reservation = self.snapshot.reservation
            expected_witness = demand_witness_semantic_sha256(
                self.snapshot,
                aggregate_demand_target=(
                    self.candidate.aggregate_demand_target),
                demand_attribution=self.candidate.demand_attribution,
                reservation_acquisition_classes=(
                    self.candidate.reservation_acquisition_classes))
            if self.candidate.demand_witness_sha256 != expected_witness:
                raise ValueError('Capacity candidate changes demand witness.')
            expected_relation, expected_disjoint = (
                _classify_reservation_demand(
                    self.snapshot,
                    self.candidate.demand_attribution,
                    self.candidate.supply_aware_demand_target,
                    raw_demand_target=self.candidate.raw_demand_target))
            if (self.candidate.reservation_demand_relation
                    is not expected_relation or
                    self.candidate.statically_disjoint_demand_accelerators
                    != expected_disjoint):
                raise ValueError(
                    'Capacity candidate changes reservation compatibility.')
            if (self.candidate.paid_cap.max_live_paid_gpu_units
                    != self.snapshot.max_live_paid_gpu_units or
                    self.candidate.paid_cap.charged_paid_gpu_units
                    != reservation.charged_paid_gpu_units or
                    self.candidate.paid_cap.remaining_paid_gpu_units
                    != _remaining_paid_gpu_units(self.snapshot)):
                raise ValueError('Capacity candidate changes paid-cap facts.')
            expected_paid_launch, expected_paid_padding = (
                _project_paced_paid_launch_authority(
                    self.snapshot, self.candidate.paid_residual))
            if (self.candidate.paid_launch_target != expected_paid_launch or
                    self.candidate.paid_packing_padding_target
                    != expected_paid_padding):
                raise ValueError('Capacity candidate changes the cap-bounded '
                                 'paid launch prefix.')
            if (not acquiring_gate and self.candidate.retirement_floor_target
                    != (_compose_retirement_floor(
                        self.snapshot,
                        self.candidate.wave_limited_actuation_target))):
                raise ValueError('Capacity candidate changes the retirement '
                                 'floor projection.')
            launch = self.candidate.reserved_launch_target.as_dict()
            eligible = reservation.eligible_capacity.as_dict()
            prospective_paid = {
                card.casefold()
                for card in self.snapshot.prospective_paid_accelerator_order
            }
            paid_cards = {
                card.casefold()
                for field in ('paid_residual', 'paid_launch_target',
                              'paid_packing_padding_target')
                for card, _ in getattr(self.candidate, field).entries
            }
            if paid_cards - prospective_paid:
                raise ValueError('Capacity candidate grants paid authority on '
                                 'a non-prospective accelerator.')
            if any(count > eligible.get(card, 0)
                   for card, count in launch.items()):
                raise ValueError('Capacity candidate launches outside its '
                                 'eligible reservation envelope.')
            if reservation.gate_policy is ReservationGatePolicy.NOT_CONFIGURED:
                if launch:
                    raise ValueError('Unconfigured reservations cannot launch.')
            elif reservation.gate_policy is ReservationGatePolicy.UNGATED:
                if (self.candidate.reserved_launch_target
                        != self.candidate.static_prefill_target):
                    raise ValueError('Ungated reservation launch and static '
                                     'prefill targets disagree.')
            elif self.candidate.static_prefill_target.total() != 0:
                raise ValueError('Demand-gated reservations cannot prefill.')
        configured = {
            card.casefold(): card
            for card in self.snapshot.configured_accelerators
        }
        classes = self.candidate.reservation_acquisition_classes
        if classes is not None and any(card.casefold() not in configured
                                       for item in classes
                                       for card in item.compatible_cards):
            raise ValueError(
                'Reservation acquisition class names an unknown card.')
        for field in _CANDIDATE_CAPACITY_FIELDS:
            capacity = getattr(self.candidate, field)
            if any(card.casefold() not in configured or
                   configured[card.casefold()] != card
                   for card, _ in capacity.entries):
                raise ValueError(
                    'Capacity candidate names a noncanonical card.')

    def canonical_payload(self) -> dict[str, object]:
        return {
            'schema_version': self.schema_version,
            'snapshot': self.snapshot.canonical_payload(),
            'candidate': self.candidate.canonical_payload(),
            'snapshot_fingerprint': self.snapshot.fingerprint,
            'candidate_fingerprint': self.candidate.fingerprint,
        }


def planner_envelope(snapshot: CapacityPlanningSnapshot,
                     candidate: CapacityPlanCandidate) -> dict[str, object]:
    """Encode and self-verify one immutable planner snapshot and candidate."""
    envelope = CapacityPlanningEnvelope(
        schema_version=CAPACITY_PLANNING_ENVELOPE_SCHEMA_VERSION,
        snapshot=snapshot,
        candidate=candidate)
    payload = json.loads(
        _canonical_json_bytes(envelope.canonical_payload()).decode('utf-8'))
    # Decode our own output so a manually constructed, noncanonical record can
    # never cross the persistence boundary merely because its dataclass accepts
    # a semantically equivalent representation.
    decode_planner_envelope(payload)
    return payload


def decode_planner_envelope(
        value: object
) -> tuple[CapacityPlanningSnapshot, CapacityPlanCandidate]:
    """Strictly decode and authenticate one canonical planner envelope."""
    payload = _require_exact_keys(value, _ENVELOPE_FIELDS,
                                  'capacity planning envelope')
    schema_version = _require_int(payload['schema_version'],
                                  'capacity planning schema_version')
    if schema_version != CAPACITY_PLANNING_ENVELOPE_SCHEMA_VERSION:
        raise ValueError('Unsupported capacity planning envelope schema.')
    declared_snapshot_fingerprint = _require_sha256(
        payload['snapshot_fingerprint'], 'snapshot_fingerprint')
    declared_candidate_fingerprint = _require_sha256(
        payload['candidate_fingerprint'], 'candidate_fingerprint')
    snapshot = _decode_snapshot(payload['snapshot'])
    candidate = _decode_candidate(payload['candidate'])
    envelope = CapacityPlanningEnvelope(schema_version=schema_version,
                                        snapshot=snapshot,
                                        candidate=candidate)
    if snapshot.fingerprint != declared_snapshot_fingerprint:
        raise ValueError('Capacity planning snapshot fingerprint disagrees.')
    if candidate.fingerprint != declared_candidate_fingerprint:
        raise ValueError('Capacity planning candidate fingerprint disagrees.')
    if (_canonical_json_bytes(payload)
            != _canonical_json_bytes(envelope.canonical_payload())):
        raise ValueError('Capacity planning envelope is not canonical.')
    return snapshot, candidate


def genesis_capacity_policy(
    *,
    service_name: str,
    service_version: int,
    last_reduced_demand_generation: int,
    capacity_unit: CapacityUnit,
    maximum_capacity: int,
    physical_gpu_width_by_accelerator: AcceleratorCapacity,
    backend_num_nodes: int = 1,
) -> tuple[CapacityPolicyState, CapacityPlanCandidate]:
    """Build the unique zero-effect prior pair for a proven-clean service.

    The repository remains responsible for proving that no head, live replica,
    claim, intent, provider operation, or other capacity-authority graph exists
    before using this constructor. Keeping the mechanical zero candidate here
    prevents callers from hand-building a second genesis representation.
    """
    state = CapacityPolicyState(
        service_name=service_name,
        service_version=service_version,
        last_reduced_demand_generation=last_reduced_demand_generation,
        capacity_unit=capacity_unit,
        maximum_capacity=maximum_capacity,
        upscale_observations=0,
        downscale_started_db_epoch=None,
        downscale_veto_streak=0,
        snap_target_on_next_recompute=False,
        adopt_total_capacity_on_next_recompute=False,
        pending_retention_floor=None,
        pending_capacity_at_adoption=0,
        pending_budget_spent=0,
        paid_window_started_db_epoch=None,
        paid_window_ceiling_by_accelerator=AcceleratorCapacity())
    identity = {
        'protocol': 'serve-capacity-policy-genesis-v1',
        'service_name': service_name,
        'service_version': service_version,
        'last_reduced_demand_generation': last_reduced_demand_generation,
        'capacity_unit': capacity_unit.value,
        'maximum_capacity': maximum_capacity,
        'physical_gpu_width_by_accelerator':
            dataclasses.asdict(physical_gpu_width_by_accelerator),
        'backend_num_nodes': backend_num_nodes,
    }
    genesis_fingerprint = hashlib.sha256(
        _canonical_json_bytes(identity)).hexdigest()
    empty = AcceleratorCapacity()
    candidate = CapacityPlanCandidate(
        kind=CapacityPlanKind.DEMAND,
        capacity_unit=capacity_unit,
        physical_gpu_width_by_accelerator=(physical_gpu_width_by_accelerator),
        backend_num_nodes=backend_num_nodes,
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
        attribution_complete=True,
        source_generation=last_reduced_demand_generation,
        snapshot_fingerprint=genesis_fingerprint,
        demand_witness_sha256=genesis_fingerprint,
        reservation_demand_relation=(ReservationDemandRelation.NOT_APPLICABLE),
        statically_disjoint_demand_accelerators=(),
        paid_cap=PaidCapProjection(max_live_paid_gpu_units=None,
                                   charged_paid_gpu_units=0,
                                   remaining_paid_gpu_units=None),
        retirement_floor_target=empty,
        next_policy_state=state)
    return state, candidate


def incomplete_capacity_plan(*,
                             source_generation: int) -> CapacityPlanCandidate:
    """Return the unique fail-closed plan for unavailable exact-card input."""
    empty = AcceleratorCapacity()
    return CapacityPlanCandidate(
        kind=CapacityPlanKind.INCOMPLETE,
        capacity_unit=CapacityUnit.UNKNOWN,
        backend_num_nodes=1,
        physical_gpu_width_by_accelerator=empty,
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
        attribution_complete=False,
        source_generation=source_generation,
        snapshot_fingerprint='',
        demand_witness_sha256=None,
        reservation_acquisition_classes=None,
        reservation_demand_relation=(ReservationDemandRelation.NOT_APPLICABLE),
        statically_disjoint_demand_accelerators=(),
        paid_cap=PaidCapProjection(max_live_paid_gpu_units=None,
                                   charged_paid_gpu_units=0,
                                   remaining_paid_gpu_units=None),
        retirement_floor_target=empty)


def _profiles(
    values: tuple[CompatibilityDemand, ...]
) -> list[tuple[int, tuple[str, ...], float]]:
    return [(item.priority, item.compatible_accelerators, item.work)
            for item in values]


@dataclasses.dataclass(frozen=True, kw_only=True)
class _PolicyReduction:
    """Pure policy-state transition before exact-card actuation limiting."""

    target_capacity: int
    target_by_accelerator: AcceleratorCapacity
    explicit_target_by_accelerator: AcceleratorCapacity
    paid_target_by_accelerator: AcceleratorCapacity
    upscale_observations: int
    downscale_started_db_epoch: float | None
    downscale_veto_streak: int
    snap_target_on_next_recompute: bool
    adopt_total_capacity_on_next_recompute: bool
    pending_retention_floor: int | None
    pending_capacity_at_adoption: int
    pending_budget_spent: int


@dataclasses.dataclass(frozen=True, kw_only=True)
class _ActuationTransition:
    """Wave-limited actuation and the prior-card capacity it retains."""

    target: AcceleratorCapacity
    retention: AcceleratorCapacity


def _capacity_gpu_units(*, capacity_unit: CapacityUnit,
                        physical_widths: Mapping[str,
                                                 int], backend_num_nodes: int,
                        capacity: AcceleratorCapacity) -> int:
    """Charge a capacity projection in physical GPU units."""
    if capacity_unit is CapacityUnit.LOGICAL_GPU:
        return capacity.total()
    return sum(count * physical_widths[card] * backend_num_nodes
               for card, count in capacity.entries)


def _remaining_paid_gpu_units(snapshot: CapacityPlanningSnapshot) -> int | None:
    if snapshot.max_live_paid_gpu_units is None:
        return None
    return max(
        0, snapshot.max_live_paid_gpu_units -
        snapshot.reservation.charged_paid_gpu_units)


def _project_paid_launch_authority(
    snapshot: CapacityPlanningSnapshot,
    paid_residual: AcceleratorCapacity,
) -> tuple[AcceleratorCapacity, AcceleratorCapacity]:
    """Project the cap-bounded deterministic whole-backend paid prefix."""
    residual_by_card = paid_residual.as_dict()
    physical_widths = (snapshot.physical_gpu_width_by_accelerator.as_dict())
    remaining_gpu_units = _remaining_paid_gpu_units(snapshot)
    launch_target: dict[str, int] = {}
    packing_padding: dict[str, int] = {}
    for card in snapshot.prospective_paid_accelerator_order:
        residual = residual_by_card.get(card, 0)
        if residual <= 0:
            continue
        physical_width = physical_widths[card]
        launch_width = (physical_width if snapshot.capacity_unit
                        is CapacityUnit.LOGICAL_GPU else 1)
        backend_gpu_units = (physical_width if snapshot.capacity_unit
                             is CapacityUnit.LOGICAL_GPU else physical_width *
                             snapshot.backend_num_nodes)
        required_backends = math.ceil(residual / launch_width)
        authorized_backends = required_backends
        if remaining_gpu_units is not None and backend_gpu_units > 0:
            authorized_backends = min(required_backends,
                                      remaining_gpu_units // backend_gpu_units)
        if authorized_backends <= 0:
            # A wide exact-card backend may not fit while a later narrower
            # backend does.  Keep the ordered traversal deterministic without
            # letting one incompatible demand strand unrelated cap headroom.
            continue
        launch_capacity = authorized_backends * launch_width
        authorized_residual = min(residual, launch_capacity)
        launch_target[card] = launch_capacity
        if launch_capacity > authorized_residual:
            packing_padding[card] = launch_capacity - authorized_residual
        if remaining_gpu_units is not None:
            remaining_gpu_units -= authorized_backends * backend_gpu_units
    return (AcceleratorCapacity.from_mapping(launch_target),
            AcceleratorCapacity.from_mapping(packing_padding))


def _paid_window_is_active(
    state: CapacityPolicyState,
    policy: CapacityPolicyInput,
    *,
    db_epoch: float,
) -> bool:
    """Return whether the committed fixed paid window is still active."""
    started = state.paid_window_started_db_epoch
    period = policy.scale_up_rate_period_seconds
    if started is None or period is None:
        return False
    if started > db_epoch:
        raise ValueError('Paid window is ahead of the PostgreSQL clock.')
    return db_epoch - started < period


def _project_paced_paid_launch_authority(
    snapshot: CapacityPlanningSnapshot,
    paid_residual: AcceleratorCapacity,
) -> tuple[AcceleratorCapacity, AcceleratorCapacity]:
    """Apply one fixed, per-card paid-residual wave to cold authority.

    Reserved admission and logical target adoption deliberately do not consume
    this budget. The committed window is an absolute per-card paid ceiling;
    when it has expired, this function proposes a new wave but does not start
    it. Only :func:`finalize_capacity_plan` may start that window after at least
    one exact paid unit survives arbitration.
    """
    state = snapshot.prior_policy_state
    policy = snapshot.policy_input
    if state is None or policy is None or policy.scale_up_rate_percentage is None:
        return _project_paid_launch_authority(snapshot, paid_residual)

    residual = paid_residual.as_dict()
    widths = snapshot.physical_gpu_width_by_accelerator.as_dict()
    existing_paid = snapshot.reservation.existing_paid_capacity.as_dict()
    authorized: dict[str, int] = {}
    if _paid_window_is_active(state, policy, db_epoch=policy.planning_db_epoch):
        ceiling = state.paid_window_ceiling_by_accelerator.as_dict()
        for card in snapshot.prospective_paid_accelerator_order:
            width = (widths[card] if snapshot.capacity_unit
                     is CapacityUnit.LOGICAL_GPU else 1)
            remaining_window = max(
                0,
                ceiling.get(card, 0) - existing_paid.get(card, 0))
            launchable = remaining_window // width * width
            count = min(residual.get(card, 0), launchable)
            if count > 0:
                authorized[card] = count
    else:
        rate = policy.scale_up_rate_percentage
        assert rate is not None
        remaining_wave = max(
            policy.scale_up_rate_min_capacity,
            math.ceil(policy.latest_committed_capacity * rate / 100.0))
        selected_launch = 0
        for card in snapshot.prospective_paid_accelerator_order:
            count = residual.get(card, 0)
            if count <= 0 or remaining_wave <= 0:
                continue
            width = (widths[card] if snapshot.capacity_unit
                     is CapacityUnit.LOGICAL_GPU else 1)
            launchable = remaining_wave // width * width
            if launchable == 0 and selected_launch == 0:
                # A positive wave must be able to make progress on one wide
                # backend; its complete width becomes the typed wave ceiling.
                launchable = width
            selected = min(count, launchable)
            if selected <= 0:
                continue
            launch = math.ceil(selected / width) * width
            authorized[card] = selected
            selected_launch += launch
            remaining_wave = max(0, remaining_wave - launch)
    return _project_paid_launch_authority(
        snapshot, AcceleratorCapacity.from_mapping(authorized))


def _compose_retirement_floor(
    snapshot: CapacityPlanningSnapshot,
    demand_target: AcceleratorCapacity,
) -> AcceleratorCapacity:
    """Compose demand and the non-overlapping exact-card shelter."""
    target = demand_target.as_dict()
    shelter = snapshot.retirement_shelter_target.as_dict()
    widths = snapshot.physical_gpu_width_by_accelerator.as_dict()
    remaining = max(0, snapshot.maximum_capacity - demand_target.total())
    for card in snapshot.configured_accelerators:
        target_count = target.get(card, 0)
        width = (widths[card]
                 if snapshot.capacity_unit is CapacityUnit.LOGICAL_GPU else 1)
        shelter_prefix = min(shelter.get(card, 0),
                             (target_count + remaining) // width * width)
        additional = max(0, shelter_prefix - target_count)
        # A shelter represents provider materialization, not fungible logical
        # slots. Keep only the largest complete-backend prefix that fits after
        # demand; truncating an eight-GPU H200 shelter to seven strands both the
        # H200 victim and an incompatible paid launch behind the ceiling.
        if additional > 0:
            target[card] = target_count + additional
            remaining -= additional
    return AcceleratorCapacity.from_mapping(target)


def _merge_capacity_ownership(
    *,
    fresh: AcceleratorCapacity,
    prior: AcceleratorCapacity,
    target: AcceleratorCapacity,
) -> AcceleratorCapacity:
    target_map = target.as_dict()
    fresh_map = fresh.as_dict()
    prior_map = prior.as_dict()
    return AcceleratorCapacity.from_mapping({
        card: max(fresh_map.get(card, 0), min(prior_map.get(card, 0), count))
        for card, count in target_map.items()
        if fresh_map.get(card, 0) > 0 or prior_map.get(card, 0) > 0
    })


def _reduce_capacity_policy(
    snapshot: CapacityPlanningSnapshot,
    *,
    raw_target: AcceleratorCapacity,
    capped_target: typing.Callable[[int], AcceleratorCapacity],
    explicit_target: typing.Callable[[int], AcceleratorCapacity],
    paid_target: typing.Callable[[int], AcceleratorCapacity],
) -> _PolicyReduction:
    """Purely apply hysteresis and exact-card adoption once per demand gen."""
    prior = snapshot.prior_policy_state
    prior_candidate = snapshot.prior_candidate
    policy = snapshot.policy_input
    assert prior is not None
    assert prior_candidate is not None
    assert policy is not None
    raw_total = raw_target.total()
    if (prior_candidate.kind is CapacityPlanKind.GATE_ACQUISITION and
            prior.last_reduced_demand_generation
            < prior_candidate.source_generation):
        # Gate acquisition is permitted only over clean genesis and carries
        # diagnostic demand, not an adopted target.
        old_total = 0
        old_map = AcceleratorCapacity()
        old_explicit = AcceleratorCapacity()
        old_paid = AcceleratorCapacity()
    else:
        old_total = prior_candidate.aggregate_demand_target
        old_map = prior_candidate.demand_attribution
        old_explicit = prior_candidate.explicit_demand_attribution
        old_paid = prior_candidate.paid_demand_attribution
    if (prior.snap_target_on_next_recompute and
            prior.adopt_total_capacity_on_next_recompute):
        old_total = max(
            old_total,
            min(snapshot.maximum_capacity, policy.ready_demand_owned_capacity))
        if old_map.total() != old_total:
            old_map = capped_target(old_total)

    target = old_total
    upscale_observations = prior.upscale_observations
    downscale_started = prior.downscale_started_db_epoch
    downscale_veto_streak = prior.downscale_veto_streak
    snap = prior.snap_target_on_next_recompute
    adopt_total = prior.adopt_total_capacity_on_next_recompute
    pending_floor = prior.pending_retention_floor
    pending_capacity = prior.pending_capacity_at_adoption
    pending_spent = prior.pending_budget_spent
    if (downscale_started is not None and
            downscale_started > policy.planning_db_epoch):
        raise ValueError('Downscale state is ahead of the PostgreSQL clock.')

    def reset_downscale() -> None:
        nonlocal downscale_started
        downscale_started = None

    def downscale_elapsed() -> bool:
        nonlocal downscale_started
        if downscale_started is None:
            initial_credit = min(policy.downscale_delay_seconds,
                                 policy.decision_interval_seconds)
            downscale_started = policy.planning_db_epoch - initial_credit
        return (policy.planning_db_epoch - downscale_started
                >= policy.downscale_delay_seconds)

    def pressure_vetoes() -> bool:
        nonlocal downscale_veto_streak
        if not policy.pressure_latched:
            downscale_veto_streak = 0
            return False
        if (downscale_veto_streak >= policy.max_downscale_pressure_vetoes):
            downscale_veto_streak = 0
            return False
        downscale_veto_streak += 1
        return True

    def adopt_upscale(requested: int) -> None:
        nonlocal target, pending_floor, pending_capacity, pending_spent
        old_target = target
        target = requested
        if target > old_target:
            pending_floor = None
            pending_capacity = 0
            pending_spent = 0

    def adopt_downscale(requested: int) -> None:
        nonlocal target, pending_floor, pending_capacity, pending_spent
        committed = policy.latest_committed_capacity
        allowance = max(
            1,
            math.ceil(committed * policy.max_scale_down_rate_percentage /
                      100.0))
        target = max(requested, committed - allowance)
        provisioning = policy.provisioning_demand_owned_capacity
        pending_allowance = (max(
            1,
            math.ceil(provisioning * policy.max_scale_down_rate_percentage /
                      100.0)) if provisioning > 0 else 0)
        pending_capacity = provisioning
        pending_floor = max(0, provisioning - pending_allowance)
        pending_spent = 0

    consumes_generation = (policy.fresh_demand and snapshot.source_generation
                           > prior.last_reduced_demand_generation)
    if not consumes_generation:
        # A durable planner never converts stale telemetry into downscale or
        # advances hysteresis twice for a same-generation supply-only replan.
        pass
    elif snap:
        snap = False
        adopt_total = False
        upscale_observations = 0
        reset_downscale()
        downscale_veto_streak = 0
        if raw_total >= target:
            adopt_upscale(raw_total)
        elif downscale_elapsed() and not pressure_vetoes():
            reset_downscale()
            adopt_downscale(raw_total)
    elif target == 0:
        reset_downscale()
        downscale_veto_streak = 0
        adopt_upscale(raw_total)
    elif raw_total > target:
        upscale_observations += 1
        reset_downscale()
        downscale_veto_streak = 0
        if upscale_observations >= max(1, policy.upscale_delay_observations):
            upscale_observations = 0
            adopt_upscale(raw_total)
    elif raw_total < target:
        upscale_observations = 0
        if downscale_elapsed() and not pressure_vetoes():
            reset_downscale()
            adopt_downscale(raw_total)
    else:
        upscale_observations = 0
        reset_downscale()
        downscale_veto_streak = 0

    target = max(0, min(snapshot.maximum_capacity, target))
    fresh_map = capped_target(min(raw_total, target))
    if target > raw_total:
        merged = autoscaler_compatibility._merge_fresh_target_into_downscale_hold(  # pylint: disable=protected-access
            adopted_target=old_map.as_dict(),
            fresh_target=fresh_map.as_dict(),
            configured_cards=list(snapshot.configured_accelerators),
            replacement_order=list(snapshot.cold_accelerator_order),
            target_total=target)
        adopted_map = AcceleratorCapacity.from_mapping(merged)
    else:
        adopted_map = capped_target(target)
    if adopted_map.total() != target:
        adopted_map = AcceleratorCapacity()

    fresh_explicit = explicit_target(min(raw_total, target))
    fresh_paid = paid_target(min(raw_total, target))
    if target > raw_total and adopted_map.total() == target:
        adopted_explicit = _merge_capacity_ownership(fresh=fresh_explicit,
                                                     prior=old_explicit,
                                                     target=adopted_map)
        adopted_paid = _merge_capacity_ownership(fresh=fresh_paid,
                                                 prior=old_paid,
                                                 target=adopted_map)
    else:
        adopted_explicit = fresh_explicit
        adopted_paid = fresh_paid
    return _PolicyReduction(target_capacity=target,
                            target_by_accelerator=adopted_map,
                            explicit_target_by_accelerator=adopted_explicit,
                            paid_target_by_accelerator=adopted_paid,
                            upscale_observations=upscale_observations,
                            downscale_started_db_epoch=downscale_started,
                            downscale_veto_streak=downscale_veto_streak,
                            snap_target_on_next_recompute=snap,
                            adopt_total_capacity_on_next_recompute=adopt_total,
                            pending_retention_floor=pending_floor,
                            pending_capacity_at_adoption=pending_capacity,
                            pending_budget_spent=pending_spent)


def _limit_actuation_transition(
    snapshot: CapacityPlanningSnapshot,
    desired: AcceleratorCapacity,
) -> _ActuationTransition:
    """Purely retain reusable capacity during an exact-card transition."""
    prior_candidate = snapshot.prior_candidate
    policy = snapshot.policy_input
    assert prior_candidate is not None
    assert policy is not None
    desired_map = desired.as_dict()
    target = desired.total()
    cards = list(snapshot.configured_accelerators)
    if target == 0:
        empty = AcceleratorCapacity()
        return _ActuationTransition(target=empty, retention=empty)
    committed = policy.latest_committed_by_accelerator.as_dict()
    previous = prior_candidate.wave_limited_actuation_target.as_dict()
    if (desired == prior_candidate.supply_aware_actuation_target and
            sum(previous.values()) == target):
        current = {card: max(0, previous.get(card, 0)) for card in cards}
    else:
        current = {card: 0 for card in cards}
        remaining = target
        for card in cards:
            kept = min(remaining, committed.get(card, 0),
                       desired_map.get(card, 0))
            current[card] = kept
            remaining -= kept
        for card in cards:
            available = max(0, committed.get(card, 0) - current[card])
            kept = min(remaining, available)
            current[card] += kept
            remaining -= kept

    reusable = 0
    for card in cards:
        moved = min(max(0,
                        desired_map.get(card, 0) - current[card]),
                    max(0,
                        committed.get(card, 0) - current[card]))
        current[card] += moved
        reusable += moved
    for card in reversed(cards):
        if reusable <= 0:
            break
        removable = max(0, current[card] - desired_map.get(card, 0))
        removed = min(reusable, removable)
        current[card] -= removed
        reusable -= removed

    desired_additions = sum(
        max(0,
            desired_map.get(card, 0) - current[card]) for card in cards)
    required_to_complete = max(0, target - sum(current.values()))
    additions_left = max(desired_additions, required_to_complete)
    for card in cards:
        increase = max(0, desired_map.get(card, 0) - current[card])
        accepted = min(increase, additions_left)
        current[card] += accepted
        additions_left -= accepted
    excess = max(0, sum(current.values()) - target)
    for card in reversed(cards):
        removable = max(0, current[card] - desired_map.get(card, 0))
        removed = min(excess, removable)
        current[card] -= removed
        excess -= removed
        if excess == 0:
            break
    limited = AcceleratorCapacity.from_mapping({
        card: count for card, count in current.items() if count > 0
    })
    if limited.total() != target:
        limited = AcceleratorCapacity()
    retention = AcceleratorCapacity.from_mapping({
        card: max(0, count - desired_map.get(card, 0))
        for card, count in limited.entries
        if count > desired_map.get(card, 0)
    })
    return _ActuationTransition(target=limited, retention=retention)


def _classify_reservation_demand(
    snapshot: CapacityPlanningSnapshot,
    demand_attribution: AcceleratorCapacity,
    supply_aware_demand: AcceleratorCapacity,
    *,
    raw_demand_target: int,
) -> tuple[ReservationDemandRelation, tuple[str, ...]]:
    """Classify reservation compatibility from immutable demand semantics."""
    if (demand_attribution.total() <= 0 or snapshot.reservation.gate_policy
            is ReservationGatePolicy.NOT_CONFIGURED):
        return ReservationDemandRelation.NOT_APPLICABLE, ()
    reserved = {
        card.casefold() for card in snapshot.configured_reservation_accelerators
    }
    # Demand-gated snapshots require a nonempty immutable catalog.  Keep this
    # fallback fail-closed for manually constructed snapshots as well.
    if not reserved:
        return ReservationDemandRelation.COMPATIBLE, ()
    if any(card.casefold() in reserved and count > 0
           for capacity in (demand_attribution, supply_aware_demand)
           for card, count in capacity.entries):
        return ReservationDemandRelation.COMPATIBLE, ()
    # Generic minimums and policy-retained capacity have no exact
    # compatibility source.  They could legally consume a reserved card, so
    # they may not mint a static-disjoint paid escape.
    if (snapshot.paid_minimum_capacity > 0 or
            demand_attribution.total() > raw_demand_target):
        return ReservationDemandRelation.COMPATIBLE, ()

    saw_source = False
    for profiles in (snapshot.demand_profiles,
                     snapshot.explicit_demand_profiles,
                     snapshot.paid_demand_profiles):
        for profile in profiles:
            if profile.work <= 0:
                continue
            saw_source = True
            if (len(profile.compatible_accelerators) != 1 or
                    reserved.intersection(
                        map(str.casefold, profile.compatible_accelerators))):
                return ReservationDemandRelation.COMPATIBLE, ()
    for fixed in (snapshot.fixed_work, snapshot.explicit_fixed_work,
                  snapshot.paid_fixed_work):
        for card, work in fixed.entries:
            if work <= 0:
                continue
            saw_source = True
            if card.casefold() in reserved:
                return ReservationDemandRelation.COMPATIBLE, ()
    for card, count in snapshot.floors.entries:
        if count <= 0:
            continue
        saw_source = True
        if card.casefold() in reserved:
            return ReservationDemandRelation.COMPATIBLE, ()
    if snapshot.deadline is not None:
        for demand in snapshot.deadline.demand:
            if demand.count <= 0:
                continue
            saw_source = True
            if (len(demand.compatible_cards) != 1 or reserved.intersection(
                    map(str.casefold, demand.compatible_cards))):
                return ReservationDemandRelation.COMPATIBLE, ()
    # A positive target retained only by minimum/hysteresis has no complete
    # static incompatibility proof, so it must acquire a bounded witness.
    if not saw_source:
        return ReservationDemandRelation.COMPATIBLE, ()
    disjoint = tuple(
        sorted(
            (card for card, count in supply_aware_demand.entries if count > 0),
            key=str.casefold))
    if not disjoint:
        return ReservationDemandRelation.COMPATIBLE, ()
    return ReservationDemandRelation.STATICALLY_DISJOINT, disjoint


def _gate_allocation_covers_demand(
    reservation: ReservationPlanningInput,
    *,
    demand_witness_sha256: str,
    aggregate_demand_target: int,
) -> bool:
    """Return whether one settled allocation causally covers this demand."""
    return bool(
        reservation.evidence_state
        is ReservationEvidenceState.AUTHENTICATED_SETTLED and
        reservation.allocation_demand_witness_sha256 == demand_witness_sha256
        and reservation.allocation_demonstrated_need is not None and
        reservation.allocation_demonstrated_need >= aggregate_demand_target and
        reservation.allocation_ceiling >= aggregate_demand_target)


def plan_capacity(snapshot: CapacityPlanningSnapshot) -> CapacityPlanCandidate:
    """Return a deterministic plan without I/O or mutation."""
    if not snapshot.attribution_complete:
        return incomplete_capacity_plan(
            source_generation=snapshot.source_generation)
    configured = list(snapshot.configured_accelerators)
    capacities = snapshot.capacity_per_accelerator.as_dict()
    floors = {
        card.casefold(): capacity for card, capacity in snapshot.floors.entries
    }
    ready_zero_cost = snapshot.ready_zero_cost.as_dict()
    ready = snapshot.ready.as_dict()
    provisioning = snapshot.provisioning.as_dict()
    reservation = snapshot.reservation
    physical_widths = snapshot.physical_gpu_width_by_accelerator.as_dict()
    prospective_paid = {
        card.casefold() for card in snapshot.prospective_paid_accelerator_order
    }
    fresh_zero = (snapshot.planning_purpose
                  is CapacityPlanningPurpose.FRESH_ZERO_RETENTION)

    def launch_width(card: str) -> int:
        if snapshot.capacity_unit is CapacityUnit.PHYSICAL_BACKEND:
            return 1
        return physical_widths[card]

    def whole_backend_supply(capacity: AcceleratorCapacity) -> dict[str, int]:
        result: dict[str, int] = {}
        for card, count in capacity.entries:
            width = launch_width(card)
            whole_capacity = count // width * width
            if whole_capacity > 0:
                result[card] = whole_capacity
        return result

    # Only complete physical backends are prospective supply.  A demand debit
    # may consume fewer logical slots, but it cannot make a fractional machine
    # launchable.
    pending_reserved = whole_backend_supply(
        reservation.pending_zero_cost_capacity)
    eligible_reserved = whole_backend_supply(reservation.eligible_capacity)
    free_reserved = {
        card: pending_reserved.get(card, 0) + eligible_reserved.get(card, 0)
        for card in configured
        if pending_reserved.get(card, 0) > 0 or
        eligible_reserved.get(card, 0) > 0
    }
    deadline_plan: autoscaler_compatibility.DeadlineCapacityPlan | None = None
    deadline_target = AcceleratorCapacity()
    infeasible_by_priority: tuple[tuple[int, float], ...] = ()
    service_time_sources: tuple[tuple[str, str], ...] = ()
    if snapshot.deadline is not None:
        deadline = snapshot.deadline
        deadline_plan = (
            autoscaler_compatibility._allocate_deadline_capacity_target(  # pylint: disable=protected-access
                configured_cards=configured,
                demand=list(deadline.demand),
                finite_supply=list(deadline.finite_supply),
                paid_cold_order=list(
                    snapshot.prospective_paid_accelerator_order),
                service_seconds_by_card=(
                    deadline.service_seconds_by_accelerator.as_dict()),
                utilization=deadline.utilization,
                paid_cold_lead_seconds=deadline.paid_cold_lead_seconds,
                max_slots=snapshot.maximum_capacity))
        deadline_target = AcceleratorCapacity.from_mapping(
            deadline_plan.target_by_card)
        infeasible_by_priority = tuple(
            sorted(deadline_plan.infeasible_requests_by_priority.items()))
        service_time_sources = deadline.service_time_sources

    # A deadline target is capacity selected *for the same queued demand* that
    # appears in the compatibility profiles.  Express it as an exact-card
    # floor so that demand consumes the SLA-selected slots.  Treating those
    # slots as fixed work would consume their capacity first and then allocate
    # the queued profiles a second time, doubling both the target and paid
    # residual.
    allocation_floors = dict(floors)
    for card, count in deadline_target.entries:
        folded = card.casefold()
        allocation_floors[folded] = max(allocation_floors.get(folded, 0), count)

    def allocate_plan(
        minimum: int,
        profiles: tuple[CompatibilityDemand, ...],
        fixed: AcceleratorWork,
        *,
        use_existing_supply: bool,
        maximum: int | None = None,
    ) -> autoscaler_compatibility.CompatibilityTargetPlan:
        allocation_maximum = (snapshot.maximum_capacity
                              if maximum is None else maximum)
        minimum = min(minimum, allocation_maximum)
        existing_zero_cost = (reservation.existing_zero_cost_capacity.as_dict())
        ready_paid: dict[str, int] = {}
        committed_paid: dict[str, int] = {}
        for card in configured:
            current_ready = ready.get(card, 0)
            current_ready_zero = ready_zero_cost.get(card, 0)
            current_committed = current_ready + provisioning.get(card, 0)
            current_committed_zero = existing_zero_cost.get(card, 0)
            if current_ready_zero > current_ready:
                raise ValueError('Ready zero-cost supply exceeds ready supply.')
            ready_paid[card] = current_ready - current_ready_zero
            # Locked PostgreSQL zero-cost inventory remains authoritative
            # during an observer blackout and may exceed the controller's
            # current usable census. Attribute every overlapping committed
            # slot to zero cost first, while retaining independently observed
            # ready paid slots when that census is incomplete.
            committed_paid[card] = max(
                ready_paid[card], current_committed -
                min(current_committed, current_committed_zero))
        return autoscaler_compatibility._plan_compatibility_target(  # pylint: disable=protected-access
            configured_cards=configured,
            capacities=capacities,
            floors=allocation_floors,
            min_replicas=minimum,
            max_replicas=allocation_maximum,
            demand_profiles=_profiles(profiles),
            fixed_work_by_accelerator=fixed.as_dict(),
            ready_zero_cost=ready_zero_cost,
            committed_zero_cost=existing_zero_cost,
            free_reserved=free_reserved,
            ready_paid=ready_paid,
            committed_paid=committed_paid,
            supply_preference=(
                autoscaler_compatibility.SupplyPreference.ZERO_COST_FIRST),
            cold_order=list(snapshot.cold_accelerator_order),
            use_existing_supply=use_existing_supply)

    def allocate(
        minimum: int,
        profiles: tuple[CompatibilityDemand, ...],
        fixed: AcceleratorWork,
        *,
        use_existing_supply: bool,
        maximum: int | None = None,
    ) -> AcceleratorCapacity:
        return AcceleratorCapacity.from_mapping(
            allocate_plan(minimum,
                          profiles,
                          fixed,
                          use_existing_supply=use_existing_supply,
                          maximum=maximum).as_dict())

    raw_demand_plan = allocate_plan(snapshot.minimum_capacity,
                                    snapshot.demand_profiles,
                                    snapshot.fixed_work,
                                    use_existing_supply=False)
    raw_demand = AcceleratorCapacity.from_mapping(raw_demand_plan.as_dict())
    acquisition_plan = raw_demand_plan
    use_supply = (snapshot.actuation_supply_policy
                  is ActuationSupplyPolicy.REUSE_CURRENT_SUPPLY)
    reduction: _PolicyReduction | None = None
    if snapshot.prior_policy_state is not None:

        def capped_target(target: int) -> AcceleratorCapacity:
            return allocate(target,
                            snapshot.demand_profiles,
                            snapshot.fixed_work,
                            use_existing_supply=False,
                            maximum=target)

        def capped_explicit(target: int) -> AcceleratorCapacity:
            return allocate(0,
                            snapshot.explicit_demand_profiles,
                            snapshot.explicit_fixed_work,
                            use_existing_supply=False,
                            maximum=target)

        def capped_paid(target: int) -> AcceleratorCapacity:
            return allocate(min(snapshot.paid_minimum_capacity, target),
                            snapshot.paid_demand_profiles,
                            snapshot.paid_fixed_work,
                            use_existing_supply=False,
                            maximum=target)

        reduction = _reduce_capacity_policy(snapshot,
                                            raw_target=raw_demand,
                                            capped_target=capped_target,
                                            explicit_target=capped_explicit,
                                            paid_target=capped_paid)
        if (reduction.target_capacity > 0 and
                reduction.target_by_accelerator.total()
                != reduction.target_capacity):
            return incomplete_capacity_plan(
                source_generation=snapshot.source_generation)
        adopted_target = reduction.target_capacity
        policy = snapshot.policy_input
        assert policy is not None
        actuation_target = min(snapshot.maximum_capacity,
                               adopted_target + policy.overprovision_capacity)
        actuation = allocate(actuation_target,
                             snapshot.demand_profiles,
                             snapshot.fixed_work,
                             use_existing_supply=use_supply,
                             maximum=actuation_target)
        if fresh_zero:
            demand = AcceleratorCapacity()
            supply_aware_demand = AcceleratorCapacity()
            raw_explicit = AcceleratorCapacity()
            raw_paid = AcceleratorCapacity()
        else:
            demand = reduction.target_by_accelerator
            if adopted_target != raw_demand.total():
                acquisition_plan = allocate_plan(adopted_target,
                                                 snapshot.demand_profiles,
                                                 snapshot.fixed_work,
                                                 use_existing_supply=False,
                                                 maximum=adopted_target)
            supply_aware_demand = allocate(adopted_target,
                                           snapshot.demand_profiles,
                                           snapshot.fixed_work,
                                           use_existing_supply=use_supply,
                                           maximum=adopted_target)
            raw_explicit = reduction.explicit_target_by_accelerator
            raw_paid = reduction.paid_target_by_accelerator
    else:
        demand = raw_demand
        supply_aware_demand = allocate(snapshot.minimum_capacity,
                                       snapshot.demand_profiles,
                                       snapshot.fixed_work,
                                       use_existing_supply=use_supply)
        actuation = allocate(snapshot.actuation_minimum_capacity,
                             snapshot.demand_profiles,
                             snapshot.fixed_work,
                             use_existing_supply=use_supply)
        raw_explicit = allocate(0,
                                snapshot.explicit_demand_profiles,
                                snapshot.explicit_fixed_work,
                                use_existing_supply=use_supply)
        raw_paid = allocate(snapshot.paid_minimum_capacity,
                            snapshot.paid_demand_profiles,
                            snapshot.paid_fixed_work,
                            use_existing_supply=use_supply)

    acquisition_classes = (() if fresh_zero else
                           acquisition_plan.reservation_acquisition_classes)
    if deadline_plan is not None and not fresh_zero:
        acquisition_classes = _apply_deadline_acquisition_pins(
            acquisition_classes,
            deadline_plan.reservation_acquisition_classes,
            aggregate_demand_target=demand.total())
    if (acquisition_classes is not None and
            sum(item.count for item in acquisition_classes) != demand.total()):
        acquisition_classes = None

    # Keep cold attribution diagnostic and rematch only the actuation/economic
    # projection through the shared subset-rank matcher. This is the canonical
    # A/B/C correction: a constrained class can revise an earlier flexible
    # assignment so all compatible reservation supply is consumed before paid.
    if use_supply and acquisition_classes is not None and not fresh_zero:
        matched_demand = _match_supply_aware_demand(
            snapshot,
            acquisition_classes,
            pending_reserved=pending_reserved,
            eligible_reserved=eligible_reserved)
        if matched_demand is None:
            return incomplete_capacity_plan(
                source_generation=snapshot.source_generation)
        rebased_actuation = _rebase_actuation_on_matched_demand(
            original_demand=supply_aware_demand,
            matched_demand=matched_demand,
            actuation=actuation,
            configured_accelerators=snapshot.configured_accelerators)
        if rebased_actuation is None:
            return incomplete_capacity_plan(
                source_generation=snapshot.source_generation)
        supply_aware_demand = matched_demand
        actuation = rebased_actuation

    economic_map = supply_aware_demand.as_dict()

    def intersect(raw: AcceleratorCapacity) -> AcceleratorCapacity:
        return AcceleratorCapacity.from_mapping({
            card: min(count, economic_map.get(card, 0))
            for card, count in raw.entries
            if count > 0 and economic_map.get(card, 0) > 0
        })

    demand_witness_sha256 = demand_witness_semantic_sha256(
        snapshot,
        aggregate_demand_target=demand.total(),
        demand_attribution=demand,
        reservation_acquisition_classes=acquisition_classes)
    reservation_demand_relation, statically_disjoint_cards = (
        _classify_reservation_demand(snapshot,
                                     demand,
                                     supply_aware_demand,
                                     raw_demand_target=raw_demand.total()))
    if (not fresh_zero and reservation_demand_relation
            is ReservationDemandRelation.COMPATIBLE and
            acquisition_classes is None):
        return incomplete_capacity_plan(
            source_generation=snapshot.source_generation)
    if (not fresh_zero and
            reservation.gate_policy is ReservationGatePolicy.UNGATED and
            reservation_demand_relation is ReservationDemandRelation.COMPATIBLE
            and reservation.evidence_state
            is not ReservationEvidenceState.AUTHENTICATED_SETTLED):
        # An ungated service still needs the current authenticated allocation
        # before compatible paid residual can be computed.  Static disjointness
        # is the only proof that may bypass that reservation snapshot.
        return incomplete_capacity_plan(
            source_generation=snapshot.source_generation)
    if (not fresh_zero and
            reservation.gate_policy is ReservationGatePolicy.DEMAND_GATED and
            reservation_demand_relation is ReservationDemandRelation.COMPATIBLE
            and not _gate_allocation_covers_demand(
                reservation,
                demand_witness_sha256=demand_witness_sha256,
                aggregate_demand_target=demand.total())):
        # This committed result is the effect-free first phase of gate
        # acquisition.  Only aggregate demand and its cold attribution remain
        # diagnostic; every field that a controller, provider, retirement
        # lane, or paid ledger could consume as authority is exact zero.
        empty = AcceleratorCapacity()
        return CapacityPlanCandidate(
            kind=CapacityPlanKind.GATE_ACQUISITION,
            capacity_unit=snapshot.capacity_unit,
            backend_num_nodes=snapshot.backend_num_nodes,
            physical_gpu_width_by_accelerator=(
                snapshot.physical_gpu_width_by_accelerator),
            aggregate_demand_target=demand.total(),
            raw_demand_target=raw_demand.total(),
            demand_attribution=demand,
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
            retirement_floor_target=empty,
            infeasible_demand_by_priority=infeasible_by_priority,
            service_time_sources=service_time_sources,
            attribution_complete=True,
            source_generation=snapshot.source_generation,
            snapshot_fingerprint=snapshot.fingerprint,
            demand_witness_sha256=demand_witness_sha256,
            reservation_acquisition_classes=acquisition_classes,
            reservation_demand_relation=reservation_demand_relation,
            statically_disjoint_demand_accelerators=(),
            paid_cap=PaidCapProjection(
                max_live_paid_gpu_units=snapshot.max_live_paid_gpu_units,
                charged_paid_gpu_units=reservation.charged_paid_gpu_units,
                remaining_paid_gpu_units=_remaining_paid_gpu_units(snapshot)),
            next_policy_state=snapshot.prior_policy_state)

    warm = {
        card: math.ceil(work / capacities[card] - 1e-9)
        for card, work in snapshot.retention_work.entries
        if work > 0 and capacities.get(card, 0) > 0
    }
    static_prefill: dict[str, int] = {}
    if fresh_zero:
        padding_target = AcceleratorCapacity()
        retained_target = actuation
    else:
        padding_target = AcceleratorCapacity.from_mapping({
            card: max(0, count - economic_map.get(card, 0))
            for card, count in actuation.entries
            if count > economic_map.get(card, 0)
        })
        retained_target = AcceleratorCapacity()

    economic = supply_aware_demand.as_dict()
    existing_zero_cost = reservation.existing_zero_cost_capacity.as_dict()
    existing_paid = reservation.existing_paid_capacity.as_dict()
    reserved_committed: dict[str, int] = {}
    new_reserved_committed: dict[str, int] = {}
    paid_residual: dict[str, int] = {}
    if not fresh_zero:
        for card in configured:
            target = economic.get(card, 0)
            if target <= 0:
                continue
            after_existing_zero = max(0,
                                      target - existing_zero_cost.get(card, 0))
            pending_commit = min(after_existing_zero,
                                 pending_reserved.get(card, 0))
            after_pending = after_existing_zero - pending_commit
            new_commit = min(after_pending, eligible_reserved.get(card, 0))
            reserved_total = min(
                target,
                existing_zero_cost.get(card, 0) + pending_commit + new_commit)
            if reserved_total > 0:
                reserved_committed[card] = reserved_total
            if new_commit > 0:
                new_reserved_committed[card] = new_commit
            residual = max(
                0, after_pending - new_commit - existing_paid.get(card, 0))
            # Demand on a reservation-only card remains visible in the plan,
            # but it cannot mint provider authority.  The paid location set is
            # frozen in this snapshot; an absent or stale catalog therefore
            # fails closed until a later generation proves a prospective
            # launch path.
            if residual > 0 and card.casefold() in prospective_paid:
                paid_residual[card] = residual
    demand_reserved_launch: dict[str, int] = {}
    for card, commitment in new_reserved_committed.items():
        width = launch_width(card)
        launch_capacity = math.ceil(commitment / width) * width
        if launch_capacity > eligible_reserved.get(card, 0):
            raise ValueError('Eligible reservation supply cannot launch a '
                             'complete physical backend.')
        demand_reserved_launch[card] = launch_capacity

    paid_residual_target = AcceleratorCapacity.from_mapping(paid_residual)
    paid_launch, paid_packing_padding = _project_paced_paid_launch_authority(
        snapshot, paid_residual_target)
    reserved_launch = dict(demand_reserved_launch)
    if (reservation.gate_policy is ReservationGatePolicy.UNGATED and
            reservation.evidence_state
            is ReservationEvidenceState.AUTHENTICATED_SETTLED):
        # Ungated fill includes demand-selected backends first, then fills the
        # remaining service ceiling in deterministic card order.  Both the
        # launch target and static projection are expressed as whole physical
        # backends even when service capacity is counted in logical GPU slots.
        remaining = max(
            0, snapshot.maximum_capacity -
            reservation.existing_zero_cost_capacity.total() -
            reservation.existing_paid_capacity.total() -
            reservation.pending_zero_cost_capacity.total() -
            sum(reserved_launch.values()) - paid_launch.total())
        for card in configured:
            width = launch_width(card)
            available = max(
                0,
                eligible_reserved.get(card, 0) - reserved_launch.get(card, 0))
            admitted = min(available, remaining // width * width)
            if admitted > 0:
                reserved_launch[card] = (reserved_launch.get(card, 0) +
                                         admitted)
                remaining -= admitted
            if remaining == 0:
                break
        static_prefill = dict(reserved_launch)
    reserved_packing_padding = {
        card: count - new_reserved_committed.get(card, 0)
        for card, count in reserved_launch.items()
        if count > new_reserved_committed.get(card, 0)
    }
    plan_kind = CapacityPlanKind.DEMAND
    if demand.total() == 0 and static_prefill:
        plan_kind = CapacityPlanKind.STATIC_PREFILL
    elif fresh_zero:
        plan_kind = CapacityPlanKind.FRESH_ZERO_RETENTION
    wave_limited_actuation = actuation
    transition_retention = AcceleratorCapacity()
    next_policy_state: CapacityPolicyState | None = None
    if reduction is not None:
        transition = _limit_actuation_transition(snapshot, actuation)
        if actuation.total() > 0 and transition.target.total() == 0:
            return incomplete_capacity_plan(
                source_generation=snapshot.source_generation)
        wave_limited_actuation = transition.target
        transition_retention = transition.retention
        prior = snapshot.prior_policy_state
        policy = snapshot.policy_input
        assert prior is not None
        assert policy is not None
        last_reduced_generation = prior.last_reduced_demand_generation
        if policy.fresh_demand:
            last_reduced_generation = snapshot.source_generation
        if fresh_zero:
            next_policy_state = dataclasses.replace(
                prior,
                service_version=snapshot.service_version,
                last_reduced_demand_generation=last_reduced_generation,
                upscale_observations=0,
                downscale_started_db_epoch=None,
                downscale_veto_streak=0,
                snap_target_on_next_recompute=False,
                adopt_total_capacity_on_next_recompute=False,
                pending_retention_floor=None,
                pending_capacity_at_adoption=0,
                pending_budget_spent=0,
                paid_window_started_db_epoch=None,
                paid_window_ceiling_by_accelerator=AcceleratorCapacity())
        else:
            next_policy_state = dataclasses.replace(
                prior,
                service_version=snapshot.service_version,
                last_reduced_demand_generation=last_reduced_generation,
                upscale_observations=reduction.upscale_observations,
                downscale_started_db_epoch=(
                    reduction.downscale_started_db_epoch),
                downscale_veto_streak=reduction.downscale_veto_streak,
                snap_target_on_next_recompute=(
                    reduction.snap_target_on_next_recompute),
                adopt_total_capacity_on_next_recompute=(
                    reduction.adopt_total_capacity_on_next_recompute),
                pending_retention_floor=reduction.pending_retention_floor,
                pending_capacity_at_adoption=(
                    reduction.pending_capacity_at_adoption),
                pending_budget_spent=reduction.pending_budget_spent)
    retirement_floor = _compose_retirement_floor(snapshot,
                                                 wave_limited_actuation)
    return CapacityPlanCandidate(
        kind=plan_kind,
        capacity_unit=snapshot.capacity_unit,
        backend_num_nodes=snapshot.backend_num_nodes,
        physical_gpu_width_by_accelerator=(
            snapshot.physical_gpu_width_by_accelerator),
        aggregate_demand_target=demand.total(),
        raw_demand_target=raw_demand.total(),
        demand_attribution=demand,
        supply_aware_demand_target=supply_aware_demand,
        reserved_capacity_committed=(
            AcceleratorCapacity.from_mapping(reserved_committed)),
        new_reserved_capacity_committed=(
            AcceleratorCapacity.from_mapping(new_reserved_committed)),
        reserved_launch_target=AcceleratorCapacity.from_mapping(
            reserved_launch),
        reserved_packing_padding_target=AcceleratorCapacity.from_mapping(
            reserved_packing_padding),
        paid_residual=paid_residual_target,
        paid_launch_target=paid_launch,
        paid_packing_padding_target=paid_packing_padding,
        zero_cost_padding_target=padding_target,
        static_prefill_target=AcceleratorCapacity.from_mapping(static_prefill),
        retained_existing_target=retained_target,
        transition_retention_target=transition_retention,
        wave_limited_actuation_target=wave_limited_actuation,
        supply_aware_actuation_target=actuation,
        explicit_demand_attribution=intersect(raw_explicit),
        paid_demand_attribution=intersect(raw_paid),
        warm_retention_target=AcceleratorCapacity.from_mapping(warm),
        deadline_target=deadline_target,
        retirement_floor_target=retirement_floor,
        infeasible_demand_by_priority=infeasible_by_priority,
        service_time_sources=service_time_sources,
        attribution_complete=snapshot.attribution_complete,
        source_generation=snapshot.source_generation,
        snapshot_fingerprint=snapshot.fingerprint,
        demand_witness_sha256=demand_witness_sha256,
        reservation_acquisition_classes=acquisition_classes,
        reservation_demand_relation=reservation_demand_relation,
        statically_disjoint_demand_accelerators=statically_disjoint_cards,
        paid_cap=PaidCapProjection(
            max_live_paid_gpu_units=snapshot.max_live_paid_gpu_units,
            charged_paid_gpu_units=reservation.charged_paid_gpu_units,
            remaining_paid_gpu_units=_remaining_paid_gpu_units(snapshot)),
        next_policy_state=next_policy_state)


def finalize_capacity_plan(
    snapshot: CapacityPlanningSnapshot,
    candidate: CapacityPlanCandidate,
    *,
    accepted_paid_plan_units: AcceleratorCapacity,
    accepted_paid_gpu_units: int,
    decision_db_epoch: float,
) -> CapacityPlanCandidate:
    """Finalize DB-clock policy memory after paid-member arbitration.

    This is deliberately not a second planner. It validates the exact accepted
    subset of the already planned paid authority, derives decision-time epochs,
    and returns the same candidate with finalized minimal policy state. An
    all-rejected subset never starts or advances a paid window.
    """
    state = snapshot.prior_policy_state
    policy = snapshot.policy_input
    next_state = candidate.next_policy_state
    if (state is None or policy is None or next_state is None or
            candidate.snapshot_fingerprint != snapshot.fingerprint or
            candidate.source_generation != snapshot.source_generation):
        raise ValueError('Capacity plan cannot be finalized for this snapshot.')
    if (not isinstance(accepted_paid_plan_units, AcceleratorCapacity) or
            type(accepted_paid_gpu_units) is not int or
            accepted_paid_gpu_units < 0 or
            not isinstance(decision_db_epoch, (int, float)) or
            isinstance(decision_db_epoch, bool) or
            not math.isfinite(float(decision_db_epoch)) or
            decision_db_epoch < policy.planning_db_epoch):
        raise ValueError('Accepted paid capacity or decision epoch is invalid.')

    configured = {
        card.casefold(): card for card in snapshot.configured_accelerators
    }
    proposed = candidate.paid_launch_target.as_dict()
    widths = snapshot.physical_gpu_width_by_accelerator.as_dict()
    for card, count in accepted_paid_plan_units.entries:
        if (card.casefold() not in configured or
                configured[card.casefold()] != card or
                count > proposed.get(card, 0)):
            raise ValueError(
                'Accepted paid capacity exceeds planned authority.')
        width = (widths[card]
                 if snapshot.capacity_unit is CapacityUnit.LOGICAL_GPU else 1)
        if count % width != 0:
            raise ValueError('Accepted paid capacity is not whole-backend.')
    expected_gpu_units = _capacity_gpu_units(
        capacity_unit=snapshot.capacity_unit,
        physical_widths=widths,
        backend_num_nodes=snapshot.backend_num_nodes,
        capacity=accepted_paid_plan_units)
    if expected_gpu_units != accepted_paid_gpu_units:
        raise ValueError('Accepted paid plan and physical GPU units disagree.')
    maximum_paid = candidate.paid_cap.max_live_paid_gpu_units
    if (maximum_paid is not None and
            candidate.paid_cap.charged_paid_gpu_units + accepted_paid_gpu_units
            > maximum_paid):
        raise ValueError('Accepted paid capacity exceeds service cap.')

    decision_db_epoch = float(decision_db_epoch)
    downscale_started = next_state.downscale_started_db_epoch
    if (state.downscale_started_db_epoch is None and
            downscale_started is not None):
        initial_credit = policy.planning_db_epoch - downscale_started
        if initial_credit < 0:
            raise ValueError('Proposed downscale epoch is in the future.')
        downscale_started = decision_db_epoch - initial_credit

    paid_window_started = state.paid_window_started_db_epoch
    paid_window_ceiling = state.paid_window_ceiling_by_accelerator
    if candidate.kind is CapacityPlanKind.FRESH_ZERO_RETENTION:
        paid_window_started = None
        paid_window_ceiling = AcceleratorCapacity()
    elif (accepted_paid_plan_units.total() > 0 and
          policy.scale_up_rate_percentage is not None and
          not _paid_window_is_active(state, policy,
                                     db_epoch=decision_db_epoch)):
        existing = snapshot.reservation.existing_paid_capacity.as_dict()
        proposed_wave = candidate.paid_launch_target.as_dict()
        paid_window_started = decision_db_epoch
        paid_window_ceiling = AcceleratorCapacity.from_mapping({
            card: existing.get(card, 0) + count
            for card, count in proposed_wave.items()
            if count > 0
        })

    finalized_state = dataclasses.replace(
        next_state,
        downscale_started_db_epoch=downscale_started,
        paid_window_started_db_epoch=paid_window_started,
        paid_window_ceiling_by_accelerator=paid_window_ceiling)
    return dataclasses.replace(candidate, next_policy_state=finalized_state)
