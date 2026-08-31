"""Durable demand ownership and ordered paid-capacity admission.

This module does not choose providers.  It records the autoscaler's normalized
capacity decision after zero-cost acceptance and gives the existing paid
capacity ledger one immutable authority tuple to bind into each claim.
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
import dataclasses
import datetime
import enum
import hashlib
import json
import math
import pickle
import re
import threading
import time
from typing import Any, TYPE_CHECKING
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.adaptors import common as adaptors_common
from sky.events import api_models as event_api_models
from sky.serve import autoscaler_compatibility
from sky.serve import capacity_admission_schema
from sky.serve import capacity_planning
from sky.serve import compatibility_matching
from sky.serve import constants
from sky.serve import demand_state
from sky.serve import demand_state_schema
from sky.serve import kueue_lane_capacity
from sky.serve import lb_ha
from sky.serve import paid_capacity as serve_paid_capacity
from sky.serve import placement_policy
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_state_schema
from sky.serve import serve_statuses
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.serve import zero_cost_actuation_schema
from sky.utils.db import db_utils

if TYPE_CHECKING:
    from sky.serve.reserved_fill_planner import AuthenticatedAllocationMap
    from sky.serve.reserved_fill_planner import FillCapacityUnit
    from sky.serve.reserved_fill_planner import ReservedFillAllocationIdentity

reserved_fill_allocation = adaptors_common.LazyImport(
    'sky.serve.reserved_fill_allocation')
reserved_fill_planner = adaptors_common.LazyImport(
    'sky.serve.reserved_fill_planner')
serve_state = adaptors_common.LazyImport('sky.serve.serve_state')
service_spec = adaptors_common.LazyImport('sky.serve.service_spec')
kubernetes_identity = adaptors_common.LazyImport(
    'sky.serve.kubernetes_identity')
zero_cost_actuation = adaptors_common.LazyImport(
    'sky.serve.zero_cost_actuation')
ordinary_launch_binding = adaptors_common.LazyImport(
    'sky.serve.ordinary_launch_binding')
request_postgres_schema = adaptors_common.LazyImport(
    'sky.server.requests.postgres_schema')

PROTOCOL_VERSION = 1
CAPABILITY_COHORT_EPOCH = 1
AGGREGATE_ACCELERATOR = '*'
_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_SERVICES = serve_state_schema.services_table
_VERSION_SPECS = serve_state_schema.version_specs_table
_CLAIMS = serve_state_schema.paid_capacity_claims_table
_PAID_WAITERS = serve_state_schema.paid_capacity_waiters_table
_PLANS = capacity_admission_schema.serve_capacity_plans_table
_HEADS = capacity_admission_schema.serve_capacity_plan_heads_table
_DEMAND_GENERATIONS = (demand_state_schema.serve_demand_feed_generations_table)
_DEMAND_REPORTS = demand_state_schema.serve_lb_demand_reports_table
_ROUTE_HEADS = route_projection_schema.serve_route_heads_table
_ROUTE_SNAPSHOTS = route_projection_schema.serve_route_snapshots_table
_REPLICAS = serve_state_schema.replicas_table
_ZERO_COST_INTENTS = (
    zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table)
_PENDING_ZERO_COST_INTENT_STATES = ('GRANTED', 'ACTUATING', 'RETRYABLE')
_BOUND_NON_POOL_LAUNCH_HANDLER = ('sky.server.requests.non_pool_launch:launch')
_BOUND_REQUEST_PROFILE_FIELDS = (
    'binding_protocol_version',
    'profile_kind',
    'profile_version',
    'profile_digest',
    'capability_cohort_epoch',
    'capability_profile_set_digest',
    'receipt_protocol_version',
)
_TERMINAL_REQUEST_STATUSES = frozenset(('SUCCEEDED', 'FAILED', 'CANCELLED'))
_TERMINAL_REPLICA_STATUSES = frozenset({
    'SHUTTING_DOWN',
    'FAILED',
    'FAILED_INITIAL_DELAY',
    'FAILED_PROBING',
    'FAILED_PROVISION',
    'FAILED_CLEANUP',
    'PREEMPTED',
    'UNKNOWN',
})

_FILL_DEMAND_WITNESS_WAKE = threading.Condition()
_FILL_DEMAND_WITNESS_WAKE_SEQUENCE: dict[str, int] = {}
_FILL_DEMAND_WITNESS_WAKE_DIGEST: dict[str, str] = {}


def replica_state_semantic_sha256_expression(
    replica_state_column: Any,) -> sqlalchemy.ColumnElement[Any]:
    """Return PostgreSQL's SHA-256 of one canonical JSONB document."""
    canonical_json = sqlalchemy.cast(replica_state_column,
                                     postgresql.JSONB).cast(sqlalchemy.Text)
    return sqlalchemy.func.encode(
        sqlalchemy.func.sha256(
            sqlalchemy.func.convert_to(canonical_json, 'UTF8')), 'hex')


class DemandSourceMode(str, enum.Enum):
    LEGACY_CONTROLLER = 'LEGACY_CONTROLLER'
    DURABLE_FEED = 'DURABLE_FEED'


class ReservedFillPlanAuthorityMode(str, enum.Enum):
    """How one immutable capacity plan relates to reserved-fill authority."""

    NOT_APPLICABLE = 'NOT_APPLICABLE'
    ALLOCATION_BOUND = 'ALLOCATION_BOUND'
    STATICALLY_INCOMPATIBLE = 'STATICALLY_INCOMPATIBLE'
    GATE_INELIGIBLE = 'GATE_INELIGIBLE'
    UNBOUND_ZERO_REVOCATION = 'UNBOUND_ZERO_REVOCATION'


class ReservedSupplyPolicy(str, enum.Enum):
    """Effective immutable reservation policy for one locked plan."""

    DISABLED = 'DISABLED'
    STATIC_PREFILL = 'STATIC_PREFILL'
    DEMAND_GATED = 'DEMAND_GATED'


class ReservedSupplyEvidenceState(str, enum.Enum):
    """Availability of the locked authenticated reservation envelope."""

    NOT_APPLICABLE = 'NOT_APPLICABLE'
    AUTHENTICATED_SETTLED = 'AUTHENTICATED_SETTLED'
    AUTHENTICATED_UNSETTLED = 'AUTHENTICATED_UNSETTLED'
    UNAVAILABLE = 'UNAVAILABLE'


class _RetainedRequestRootState(enum.Enum):
    """Pure classification of one locked historical request root."""

    CLOSED_QUIESCED = enum.auto()
    BLOCKING = enum.auto()
    MALFORMED = enum.auto()


class CapacityAdmissionError(RuntimeError):
    """Base error for ordered capacity authority."""


class CapacityAdmissionConflict(CapacityAdmissionError):
    """Planner inputs no longer match locked durable state."""


class CapacityAdmissionUnavailable(CapacityAdmissionError):
    """Ordered admission cannot be proven on this installation."""


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(value,
                          sort_keys=True,
                          separators=(',', ':'),
                          ensure_ascii=False,
                          allow_nan=False).encode('utf-8')
    except (TypeError, ValueError) as error:
        raise ValueError(
            'Capacity plan must contain canonical JSON.') from error


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def capacity_plan_content_sha256(payload: Any) -> str:
    """Return the canonical digest used by persisted capacity plans."""
    return _sha256(payload)


def locked_planning_source_fingerprint(
    planning_state_fingerprint: str | None,
    economic_capacity_graph_sha256: str,
) -> str:
    """Bind a planner envelope to replica and scheduler-capacity state."""
    if (planning_state_fingerprint is not None and
        (not isinstance(planning_state_fingerprint, str) or
         _SHA256_RE.fullmatch(planning_state_fingerprint) is None)):
        raise ValueError('Planning-state fingerprint is malformed.')
    if (not isinstance(economic_capacity_graph_sha256, str) or
            _SHA256_RE.fullmatch(economic_capacity_graph_sha256) is None):
        raise ValueError('Economic capacity graph fingerprint is malformed.')
    return _sha256({
        'protocol': 'locked-capacity-planning-source-v1',
        'planning_state_sha256': planning_state_fingerprint,
        'economic_capacity_graph_sha256': economic_capacity_graph_sha256,
    })


def _positive_int(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f'{field} must be a positive integer.')
    return value


def _canonical_counts(value: Mapping[str, int], field: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise ValueError(f'{field} must be a mapping.')
    result: dict[str, int] = {}
    for raw_card, raw_count in value.items():
        if not isinstance(raw_card, str) or not raw_card:
            raise ValueError(f'{field} has an invalid accelerator.')
        card = (AGGREGATE_ACCELERATOR
                if raw_card == AGGREGATE_ACCELERATOR else raw_card.casefold())
        if card in result or isinstance(
                raw_count,
                bool) or not isinstance(raw_count, int) or raw_count < 0:
            raise ValueError(f'{field} has invalid or duplicate capacity.')
        result[card] = raw_count
    return dict(sorted(result.items()))


def _capacity_for_accounting_cards(
    value: capacity_planning.AcceleratorCapacity,
    accounting_cards: set[str],
    field: str,
) -> dict[str, int]:
    """Return one typed planner map in the repository's exact-card domain."""
    if not isinstance(value, capacity_planning.AcceleratorCapacity):
        raise ValueError(f'{field} is not typed accelerator capacity.')
    counts = _canonical_counts(value.as_dict(), field)
    if set(counts) - accounting_cards:
        raise ValueError(f'{field} names an unknown accelerator.')
    return {card: counts.get(card, 0) for card in sorted(accounting_cards)}


def _positive_counts(value: Mapping[str, int]) -> dict[str, int]:
    return {card: count for card, count in value.items() if count > 0}


def _decode_planner_payload(
    value: Any,
) -> tuple[capacity_planning.CapacityPlanningSnapshot,
           capacity_planning.CapacityPlanCandidate]:
    """Decode the one closed planner envelope accepted by admission."""
    if not isinstance(value, Mapping) or not value:
        raise ValueError('Capacity plan has no immutable planner envelope.')
    try:
        return capacity_planning.decode_planner_envelope(value)
    except ValueError as error:
        raise ValueError(
            'Capacity plan planner payload is malformed.') from error


def _validate_prospective_planner_candidate(
    payload: Mapping[str, Any],
    *,
    service_version: int,
    demand_feed_generation: int,
    accounting_cards: set[str],
    capacity_target: Mapping[str, int],
    existing_zero_cost: Mapping[str, int],
    pending_zero_cost: Mapping[str, int],
    allocation_reserved: Mapping[str, int],
    existing_paid: Mapping[str, int],
    paid_residual: Mapping[str, int],
    paid_launch_target: Mapping[str, int],
) -> tuple[capacity_planning.CapacityPlanningSnapshot,
           capacity_planning.CapacityPlanCandidate]:
    """Authenticate the exact persisted planner authority for paid I/O.

    The JSON accounting maps remain useful independent database constraints,
    but they are not a second launch authority.  A prospective paid claim must
    name the complete typed candidate that produced those maps, at the exact
    durable-demand generation, before any provider-side effect is possible.
    """
    try:
        planner_snapshot, candidate = _decode_planner_payload(
            payload.get('planner'))
        configured_cards = {
            card.casefold() for card in planner_snapshot.configured_accelerators
        }
        candidate_target = _capacity_for_accounting_cards(
            candidate.supply_aware_demand_target, accounting_cards,
            'planner candidate traffic target')
        candidate_commitment = _capacity_for_accounting_cards(
            candidate.new_reserved_capacity_committed, accounting_cards,
            'planner candidate reservation commitment')
        candidate_paid = _positive_counts(
            _capacity_for_accounting_cards(candidate.paid_residual,
                                           accounting_cards,
                                           'planner candidate paid residual'))
        candidate_paid_launch = _positive_counts(
            _capacity_for_accounting_cards(
                candidate.paid_launch_target, accounting_cards,
                'planner candidate paid launch target'))
        reservation_inventory = {
            'existing_zero_cost': _capacity_for_accounting_cards(
                planner_snapshot.reservation.existing_zero_cost_capacity,
                accounting_cards,
                'planner reservation existing zero-cost capacity'),
            'pending_zero_cost': _capacity_for_accounting_cards(
                planner_snapshot.reservation.pending_zero_cost_capacity,
                accounting_cards,
                'planner reservation pending zero-cost capacity'),
            'existing_paid': _capacity_for_accounting_cards(
                planner_snapshot.reservation.existing_paid_capacity,
                accounting_cards, 'planner reservation existing paid capacity'),
        }
    except (AttributeError, TypeError, ValueError) as error:
        raise CapacityAdmissionConflict(
            'Paid claim has no valid immutable planner candidate.') from error

    if (planner_snapshot.service_version != service_version or
            planner_snapshot.source_generation != demand_feed_generation or
            candidate.source_generation != demand_feed_generation or
            candidate.snapshot_fingerprint != planner_snapshot.fingerprint or
            _SHA256_RE.fullmatch(planner_snapshot.source_fingerprint) is None or
            configured_cards != accounting_cards):
        raise CapacityAdmissionConflict(
            'Paid claim planner generation or fingerprint is stale.')
    if (not planner_snapshot.attribution_complete or
            not candidate.attribution_complete or
            candidate.kind is not capacity_planning.CapacityPlanKind.DEMAND or
            planner_snapshot.planning_purpose
            is not capacity_planning.CapacityPlanningPurpose.ECONOMIC_ADMISSION
            or planner_snapshot.actuation_supply_policy
            is not capacity_planning.ActuationSupplyPolicy.REUSE_CURRENT_SUPPLY
       ):
        raise CapacityAdmissionConflict(
            'Paid claim planner attribution is incomplete or ineligible.')

    expected_target = {
        card: capacity_target.get(card, 0) for card in sorted(accounting_cards)
    }
    expected_commitment = {
        card: allocation_reserved.get(card, 0)
        for card in sorted(accounting_cards)
    }
    expected_inventory = {
        'existing_zero_cost': {
            card: existing_zero_cost.get(card, 0)
            for card in sorted(accounting_cards)
        },
        'pending_zero_cost': {
            card: pending_zero_cost.get(card, 0)
            for card in sorted(accounting_cards)
        },
        'existing_paid': {
            card: existing_paid.get(card, 0) for card in sorted(accounting_cards)
        },
    }
    if (candidate_target != expected_target or
            candidate_commitment != expected_commitment or
            candidate_paid != _positive_counts(paid_residual) or
            candidate_paid_launch != _positive_counts(paid_launch_target) or
            reservation_inventory != expected_inventory):
        raise CapacityAdmissionConflict(
            'Paid claim accounting differs from its immutable planner '
            'candidate.')
    return planner_snapshot, candidate


def _planner_reservation_gate_policy(
    policy: ReservedSupplyPolicy,) -> capacity_planning.ReservationGatePolicy:
    """Return the planner spelling of one immutable reservation policy."""
    return {
        ReservedSupplyPolicy.DISABLED:
            capacity_planning.ReservationGatePolicy.NOT_CONFIGURED,
        ReservedSupplyPolicy.STATIC_PREFILL:
            capacity_planning.ReservationGatePolicy.UNGATED,
        ReservedSupplyPolicy.DEMAND_GATED:
            capacity_planning.ReservationGatePolicy.DEMAND_GATED,
    }[policy]


def _validate_prospective_reservation_policy(
    planner_snapshot: capacity_planning.CapacityPlanningSnapshot,
    policy: ReservedSupplyPolicy,
) -> None:
    """Keep immutable fill policy changes fail-closed for every authority."""
    if (planner_snapshot.reservation.gate_policy
            is not _planner_reservation_gate_policy(policy)):
        raise CapacityAdmissionConflict(
            'Paid claim reservation policy changed.')


def _validate_prospective_reservation_evidence(
    planner_snapshot: capacity_planning.CapacityPlanningSnapshot,
    candidate: capacity_planning.CapacityPlanCandidate,
    *,
    accounting_cards: set[str],
    policy: ReservedSupplyPolicy,
    evidence_state: ReservedSupplyEvidenceState,
    authenticated_capacity: Mapping[str, int],
    eligible_capacity: Mapping[str, int],
    reservation_evidence_sha256: str,
) -> None:
    """Bind the persisted candidate to current reservation/gate evidence."""
    _validate_prospective_reservation_policy(planner_snapshot, policy)
    expected_evidence_state = {
        ReservedSupplyEvidenceState.NOT_APPLICABLE:
            capacity_planning.ReservationEvidenceState.NOT_APPLICABLE,
        ReservedSupplyEvidenceState.AUTHENTICATED_SETTLED:
            capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED,
        ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED:
            capacity_planning.ReservationEvidenceState.AUTHENTICATED_UNSETTLED,
        ReservedSupplyEvidenceState.UNAVAILABLE:
            capacity_planning.ReservationEvidenceState.UNAVAILABLE,
    }[evidence_state]
    reservation = planner_snapshot.reservation
    try:
        authenticated = _capacity_for_accounting_cards(
            reservation.authenticated_capacity, accounting_cards,
            'planner authenticated reservation capacity')
        eligible = _capacity_for_accounting_cards(
            reservation.eligible_capacity, accounting_cards,
            'planner eligible reservation capacity')
        launch_target = _capacity_for_accounting_cards(
            candidate.reserved_launch_target, accounting_cards,
            'planner reserved launch target')
        expected_authenticated = _canonical_counts(authenticated_capacity,
                                                   'authenticated_capacity')
        expected_eligible = _canonical_counts(eligible_capacity,
                                              'eligible_capacity')
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Paid claim reservation planner evidence is malformed.') from error
    expected_authenticated = {
        card: expected_authenticated.get(card, 0)
        for card in sorted(accounting_cards)
    }
    expected_eligible = {
        card: expected_eligible.get(card, 0) for card in sorted(accounting_cards)
    }
    if (reservation.evidence_state is not expected_evidence_state or
            reservation.evidence_fingerprint != reservation_evidence_sha256 or
            authenticated != expected_authenticated or
            eligible != expected_eligible or any(
                launch_target.get(card, 0) > expected_eligible.get(card, 0)
                for card in accounting_cards)):
        raise CapacityAdmissionConflict(
            'Paid claim reservation or usage-gate evidence changed.')


def _validate_static_disjoint_prospective_authority(
    planner_snapshot: capacity_planning.CapacityPlanningSnapshot,
    candidate: capacity_planning.CapacityPlanCandidate,
    authority: ReservedFillPlanAuthority,
    *,
    accounting_cards: set[str],
    capacity_target: Mapping[str, int],
    fill_config: _ReservedFillServiceConfig,
    policy: ReservedSupplyPolicy,
) -> None:
    """Validate allocation-independent authority for exact disjoint demand."""
    positive_target_cards = tuple(
        sorted(card for card, count in capacity_target.items() if count > 0))
    try:
        candidate_disjoint_cards = tuple(
            sorted(
                card.casefold()
                for card in candidate.statically_disjoint_demand_accelerators))
        candidate_commitment = _capacity_for_accounting_cards(
            candidate.new_reserved_capacity_committed, accounting_cards,
            'planner candidate reservation commitment')
        candidate_reserved_launch = _capacity_for_accounting_cards(
            candidate.reserved_launch_target, accounting_cards,
            'planner candidate reserved launch target')
        candidate_static_fill = _capacity_for_accounting_cards(
            candidate.static_prefill_target, accounting_cards,
            'planner candidate static fill target')
        planner_reserved_cards = {
            card.casefold()
            for card in planner_snapshot.configured_reservation_accelerators
        }
    except (AttributeError, TypeError, ValueError) as error:
        raise CapacityAdmissionConflict(
            'Paid claim static incompatibility evidence is malformed.'
        ) from error
    expected_capacity_unit = (
        capacity_planning.CapacityUnit.LOGICAL_GPU if fill_config.capacity_unit
        is reserved_fill_planner.FillCapacityUnit.LOGICAL else
        capacity_planning.CapacityUnit.PHYSICAL_BACKEND)
    current_reserved_cards = set(fill_config.reserved_accelerators or ())
    if (authority.mode
            is not ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE or
            candidate.reservation_demand_relation is not capacity_planning.
            ReservationDemandRelation.STATICALLY_DISJOINT or
            authority.incompatible_accelerators != positive_target_cards or
            candidate_disjoint_cards != positive_target_cards or
            any(candidate_commitment.values()) or
            any(candidate_reserved_launch.values()) or
            any(candidate_static_fill.values()) or
            planner_snapshot.maximum_capacity != fill_config.max_capacity or
            planner_snapshot.capacity_unit is not expected_capacity_unit or
            planner_reserved_cards != current_reserved_cards):
        raise CapacityAdmissionConflict(
            'Paid claim no longer proves exact static reservation '
            'incompatibility.')
    _validate_prospective_reservation_policy(planner_snapshot, policy)


def _canonical_watermark(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError('receipt_watermark must be a nonempty list.')
    result = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {
                'reporter_session_id', 'sequence', 'payload_sha256'
        }:
            raise ValueError('receipt_watermark has an invalid entry.')
        reporter = item['reporter_session_id']
        sequence = item['sequence']
        digest = item['payload_sha256']
        if (not isinstance(reporter, str) or not reporter or reporter in seen or
                not isinstance(sequence, int) or isinstance(sequence, bool) or
                sequence <= 0 or not isinstance(digest, str) or
                _SHA256_RE.fullmatch(digest) is None):
            raise ValueError('receipt_watermark has an invalid entry.')
        seen.add(reporter)
        result.append({
            'reporter_session_id': reporter,
            'sequence': sequence,
            'payload_sha256': digest,
        })
    if [item['reporter_session_id'] for item in result] != sorted(seen):
        raise ValueError('receipt_watermark must be canonically ordered.')
    return result


@dataclasses.dataclass(frozen=True)
class ReservedFillPlanAuthority:
    """Explicit allocation binding or explicit unbound plan disposition."""

    mode: ReservedFillPlanAuthorityMode
    allocation: ReservedFillAllocationIdentity | None = None
    incompatible_accelerators: tuple[str, ...] = ()
    worker_projection_sha256: str | None = None
    reservation_evidence_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mode, ReservedFillPlanAuthorityMode):
            raise ValueError('Reserved-fill plan authority mode is invalid.')
        allocation_bound = (self.mode
                            is ReservedFillPlanAuthorityMode.ALLOCATION_BOUND)
        statically_incompatible = (
            self.mode is ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE)
        reservation_ineligible = (
            self.mode is ReservedFillPlanAuthorityMode.GATE_INELIGIBLE)
        if allocation_bound != (self.allocation is not None):
            raise ValueError('Only an allocation-bound plan may carry a '
                             'reserved-fill allocation identity.')
        if statically_incompatible:
            canonical = tuple(
                sorted({
                    card.casefold()
                    for card in self.incompatible_accelerators
                    if isinstance(card, str) and card
                }))
            if (not canonical or canonical != self.incompatible_accelerators or
                    AGGREGATE_ACCELERATOR in canonical or
                    not isinstance(self.worker_projection_sha256, str) or
                    _SHA256_RE.fullmatch(
                        self.worker_projection_sha256) is None):
                raise ValueError('Statically incompatible authority is not '
                                 'canonical and complete.')
        elif (self.incompatible_accelerators or
              self.worker_projection_sha256 is not None):
            raise ValueError('Only statically incompatible authority may carry '
                             'accelerator projection evidence.')
        if reservation_ineligible:
            if (not isinstance(self.reservation_evidence_sha256, str) or
                    _SHA256_RE.fullmatch(
                        self.reservation_evidence_sha256) is None):
                raise ValueError('Reservation-ineligible authority has no '
                                 'immutable reservation evidence.')
        elif self.reservation_evidence_sha256 is not None:
            raise ValueError('Only reservation-ineligible authority may carry '
                             'fill reservation evidence.')

    @classmethod
    def not_applicable(cls) -> 'ReservedFillPlanAuthority':
        return cls(ReservedFillPlanAuthorityMode.NOT_APPLICABLE)

    @classmethod
    def zero_revocation(cls) -> 'ReservedFillPlanAuthority':
        return cls(ReservedFillPlanAuthorityMode.UNBOUND_ZERO_REVOCATION)

    @classmethod
    def bound(cls, identity: Any) -> 'ReservedFillPlanAuthority':
        if not isinstance(identity,
                          reserved_fill_planner.ReservedFillAllocationIdentity):
            raise ValueError(
                'Reserved-fill allocation identity has the wrong type.')
        return cls(ReservedFillPlanAuthorityMode.ALLOCATION_BOUND, identity)

    @classmethod
    def gate_ineligible(
        cls,
        reservation_evidence_sha256: str,
    ) -> 'ReservedFillPlanAuthority':
        """Compatibility spelling for unavailable reservation evidence."""
        return cls.reservation_ineligible(reservation_evidence_sha256)

    @classmethod
    def reservation_ineligible(
        cls,
        reservation_evidence_sha256: str,
    ) -> 'ReservedFillPlanAuthority':
        """Bind paid authority to exact unavailable reservation evidence."""
        return cls(ReservedFillPlanAuthorityMode.GATE_INELIGIBLE,
                   reservation_evidence_sha256=(reservation_evidence_sha256))

    @classmethod
    def statically_incompatible(
        cls,
        accelerators: Sequence[str],
        worker_projection_sha256: str,
    ) -> 'ReservedFillPlanAuthority':
        canonical = tuple(sorted({card.casefold() for card in accelerators}))
        return cls(ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE,
                   incompatible_accelerators=canonical,
                   worker_projection_sha256=worker_projection_sha256)

    @classmethod
    def from_mapping(cls, value: Any) -> 'ReservedFillPlanAuthority':
        allowed = {
            'mode', 'allocation', 'incompatible_accelerators',
            'worker_projection_sha256', 'reservation_evidence_sha256'
        }
        if not isinstance(value, Mapping) or set(value) - allowed:
            raise ValueError('Reserved-fill plan authority is malformed.')
        try:
            mode = ReservedFillPlanAuthorityMode(value.get('mode'))
        except (TypeError, ValueError) as error:
            raise ValueError(
                'Reserved-fill plan authority mode is invalid.') from error
        reservation_evidence_digest = None
        if mode is ReservedFillPlanAuthorityMode.ALLOCATION_BOUND:
            if set(value) != {'mode', 'allocation'}:
                raise ValueError(
                    'Allocation-bound plan authority is incomplete.')
            allocation = (reserved_fill_planner.ReservedFillAllocationIdentity.
                          from_mapping(value['allocation']))
            accelerators: tuple[str, ...] = ()
            projection_digest = None
        elif mode is ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE:
            if set(value) != {
                    'mode', 'incompatible_accelerators',
                    'worker_projection_sha256'
            } or not isinstance(value['incompatible_accelerators'], list):
                raise ValueError(
                    'Statically incompatible plan authority is incomplete.')
            allocation = None
            accelerators = tuple(value['incompatible_accelerators'])
            projection_digest = value['worker_projection_sha256']
        elif mode is ReservedFillPlanAuthorityMode.GATE_INELIGIBLE:
            if set(value) != {'mode', 'reservation_evidence_sha256'}:
                raise ValueError(
                    'Gate-ineligible plan authority is incomplete.')
            allocation = None
            accelerators = ()
            projection_digest = None
            reservation_evidence_digest = value['reservation_evidence_sha256']
        else:
            if set(value) != {'mode'}:
                raise ValueError(
                    'Unbound plan authority must not carry an allocation.')
            allocation = None
            accelerators = ()
            projection_digest = None
        return cls(mode, allocation, accelerators, projection_digest,
                   reservation_evidence_digest)

    def to_mapping(self) -> dict[str, Any]:
        result: dict[str, Any] = {'mode': self.mode.value}
        if self.allocation is not None:
            result['allocation'] = self.allocation.to_mapping()
        if self.mode is ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE:
            result['incompatible_accelerators'] = list(
                self.incompatible_accelerators)
            result['worker_projection_sha256'] = self.worker_projection_sha256
        if self.mode is ReservedFillPlanAuthorityMode.GATE_INELIGIBLE:
            result['reservation_evidence_sha256'] = (
                self.reservation_evidence_sha256)
        return result


@dataclasses.dataclass(frozen=True)
class CapacityPlanInput:
    """Demand-side planner input after the zero-cost commit boundary.

    Committed inventory is deliberately absent.  The repository derives it
    from locked replica rows in the publication transaction; accepting a
    controller-supplied inventory would recreate the zero-cost/paid TOCTOU
    this protocol exists to remove.
    """

    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    service_version: int
    demand_source_epoch: int
    demand_feed_generation: int
    receipt_watermark: list[dict[str, Any]]
    route_generation: int
    route_sha256: str
    route_source_epoch: int
    normalized_demand: Mapping[str, Any]
    capacity_target_by_accelerator: Mapping[str, int]
    reserved_fill_authority: ReservedFillPlanAuthority
    paid_residual: capacity_planning.AcceleratorCapacity
    paid_launch_target: capacity_planning.AcceleratorCapacity
    allocation_reserved_capacity_by_accelerator: Mapping[str, int] = (
        dataclasses.field(default_factory=dict))
    expected_pending_zero_cost_capacity_by_accelerator: Mapping[str, int] = (
        dataclasses.field(default_factory=dict))
    expected_economic_capacity_graph_sha256: str | None = None
    planner_payload: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def payload(
        self,
        *,
        existing_zero_cost_capacity_by_accelerator: Mapping[str, int],
        pending_zero_cost_capacity_by_accelerator: Mapping[str, int] |
        None = None,
        allocation_reserved_capacity_by_accelerator: Mapping[str, int] |
        None = None,
        existing_paid_capacity_by_accelerator: Mapping[str, int],
    ) -> dict[str, Any]:
        if not isinstance(self.service_name, str) or not self.service_name:
            raise ValueError('service_name must be nonempty.')
        if not isinstance(self.service_hash, str) or not self.service_hash:
            raise ValueError('service_hash must be nonempty.')
        for field in ('service_lifecycle_epoch', 'service_version',
                      'demand_source_epoch', 'demand_feed_generation',
                      'route_generation', 'route_source_epoch'):
            _positive_int(getattr(self, field), field)
        if (not isinstance(self.route_sha256, str) or
                _SHA256_RE.fullmatch(self.route_sha256) is None):
            raise ValueError('route_sha256 must be lowercase SHA-256.')
        if not isinstance(self.normalized_demand, Mapping):
            raise ValueError('normalized_demand must be a mapping.')
        capacity_target = _canonical_counts(self.capacity_target_by_accelerator,
                                            'capacity_target_by_accelerator')
        existing_zero_cost = _canonical_counts(
            existing_zero_cost_capacity_by_accelerator,
            'existing_zero_cost_capacity_by_accelerator')
        pending_zero_cost = _canonical_counts(
            ({
                card: 0 for card in capacity_target
            } if pending_zero_cost_capacity_by_accelerator is None else
             pending_zero_cost_capacity_by_accelerator),
            'pending_zero_cost_capacity_by_accelerator')
        allocation_reserved = _canonical_counts(
            ({
                card: 0 for card in capacity_target
            } if allocation_reserved_capacity_by_accelerator is None else
             allocation_reserved_capacity_by_accelerator),
            'allocation_reserved_capacity_by_accelerator')
        existing_paid = _canonical_counts(
            existing_paid_capacity_by_accelerator,
            'existing_paid_capacity_by_accelerator')
        paid_capacity = _capacity_for_accounting_cards(
            self.paid_residual, set(capacity_target),
            'paid_residual_by_accelerator')
        paid = _positive_counts(paid_capacity)
        paid_launch_capacity = _capacity_for_accounting_cards(
            self.paid_launch_target, set(capacity_target),
            'paid_launch_target_by_accelerator')
        paid_launch = _positive_counts(paid_launch_capacity)
        planner_payload = json.loads(
            _canonical_json(dict(self.planner_payload)).decode('utf-8'))
        if planner_payload:
            _, candidate = _decode_planner_payload(planner_payload)
            candidate_paid = _capacity_for_accounting_cards(
                candidate.paid_residual, set(capacity_target),
                'planner candidate paid residual')
            candidate_paid_launch = _capacity_for_accounting_cards(
                candidate.paid_launch_target, set(capacity_target),
                'planner candidate paid launch target')
            if (candidate_paid != paid_capacity or
                    candidate_paid_launch != paid_launch_capacity):
                raise ValueError('Capacity plan paid projections differ from '
                                 'its immutable planner candidate.')
        cards = (set(capacity_target) | set(existing_zero_cost) |
                 set(pending_zero_cost) | set(allocation_reserved) |
                 set(existing_paid) | set(paid) | set(paid_launch))
        if AGGREGATE_ACCELERATOR in cards and len(cards) != 1:
            raise ValueError('A capacity plan cannot mix aggregate and '
                             'exact-card accounting.')
        authority = self.reserved_fill_authority
        if not isinstance(authority, ReservedFillPlanAuthority):
            raise ValueError('Capacity plan has no typed reserved-fill '
                             'authority.')
        economic_graph_sha256 = self.expected_economic_capacity_graph_sha256
        if (authority.mode is ReservedFillPlanAuthorityMode.ALLOCATION_BOUND and
                economic_graph_sha256 is None):
            raise ValueError('Allocation-bound capacity plan has no exact '
                             'economic capacity graph digest.')
        if (economic_graph_sha256 is not None and
            (not isinstance(economic_graph_sha256, str) or
             _SHA256_RE.fullmatch(economic_graph_sha256) is None)):
            raise ValueError('Capacity plan has a malformed economic capacity '
                             'graph digest.')
        positive_target_cards = tuple(
            sorted(
                card for card, count in capacity_target.items() if count > 0))
        if (authority.mode
                is ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE and
                authority.incompatible_accelerators != positive_target_cards):
            raise ValueError('Statically incompatible authority does not name '
                             'the exact positive target cards.')
        if (authority.mode
                is ReservedFillPlanAuthorityMode.UNBOUND_ZERO_REVOCATION and
                any(capacity_target.values())):
            raise ValueError('An unbound revocation plan must have an all-zero '
                             'capacity target.')
        if (authority.allocation is not None and
                authority.allocation.service_version != self.service_version):
            raise ValueError('Capacity plan and reserved-fill allocation '
                             'versions disagree.')
        _canonical_watermark(self.receipt_watermark)
        normalized_demand = json.loads(
            _canonical_json(dict(self.normalized_demand)).decode('utf-8'))
        payload = {
            'protocol_version': PROTOCOL_VERSION,
            'service': {
                'name': self.service_name,
                'hash': self.service_hash,
                'lifecycle_epoch': self.service_lifecycle_epoch,
                'version': self.service_version,
            },
            'source': {
                'demand_source_epoch': self.demand_source_epoch,
                'route_generation': self.route_generation,
                'route_sha256': self.route_sha256,
                'route_source_epoch': self.route_source_epoch,
            },
            'normalized_demand': normalized_demand,
            'capacity_target_by_accelerator': capacity_target,
            'existing_zero_cost_capacity_by_accelerator': existing_zero_cost,
            'pending_zero_cost_capacity_by_accelerator': pending_zero_cost,
            'allocation_reserved_capacity_by_accelerator':
                (allocation_reserved),
            'existing_paid_capacity_by_accelerator': existing_paid,
            'paid_residual_by_accelerator': paid,
            'paid_launch_target_by_accelerator': paid_launch,
            'reserved_fill_authority': authority.to_mapping(),
        }
        if planner_payload:
            _decode_planner_payload(planner_payload)
            payload['planner'] = planner_payload
        if economic_graph_sha256 is not None:
            payload['economic_capacity_graph_sha256'] = economic_graph_sha256
        return payload


@dataclasses.dataclass(frozen=True)
class CapacityPlanDecision:
    """Pure planner output for one locked current demand/supply snapshot.

    The repository owns every durable identity and inventory field.  A planner
    may return only the exact-card target plus the three derived autoscaler
    fields that are intentionally embedded in ``normalized_demand`` for later
    claim verification.
    """

    capacity_target_by_accelerator: Mapping[str, int]
    normalized_demand_extensions: Mapping[str, Any]
    reserved_capacity_commitment_by_accelerator: Mapping[str, int] = (
        dataclasses.field(default_factory=dict))
    expected_paid_residual_by_accelerator: Mapping[str, int] | None = None
    expected_paid_launch_target_by_accelerator: Mapping[str, int] | None = None
    static_reserved_fill_target_by_accelerator: Mapping[str, int] = (
        dataclasses.field(default_factory=dict))
    paid_launch_priority_by_accelerator: Mapping[str, int] = (dataclasses.field(
        default_factory=dict))
    planner_payload: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def paid_launch_priority(self, accelerator: str) -> int:
        """Return the exact planner-derived priority for one paid card."""
        card = accelerator.casefold()
        if card not in self.paid_launch_priority_by_accelerator:
            raise ValueError('Capacity planner paid priority is missing.')
        return self.paid_launch_priority_by_accelerator[card]

    def decode_planner(
        self,
    ) -> tuple[capacity_planning.CapacityPlanningSnapshot,
               capacity_planning.CapacityPlanCandidate]:
        """Return the only planner snapshot/candidate this decision may name."""
        return _decode_planner_payload(self.planner_payload)

    def canonical_target(self, accounting_cards: set[str]) -> dict[str, int]:
        target = _canonical_counts(self.capacity_target_by_accelerator,
                                   'capacity_target_by_accelerator')
        if set(target) != accounting_cards:
            raise ValueError('Capacity planner target does not cover the exact '
                             'accounting cards.')
        if (not isinstance(self.normalized_demand_extensions, Mapping) or
                set(self.normalized_demand_extensions)
                != _PLAN_DERIVED_DEMAND_FIELDS):
            raise ValueError('Capacity planner demand extensions are '
                             'incomplete or contain authority fields.')
        commitment = _canonical_counts(
            self.reserved_capacity_commitment_by_accelerator,
            'reserved_capacity_commitment_by_accelerator')
        if set(commitment) - accounting_cards:
            raise ValueError('Capacity planner reservation commitment names '
                             'an unknown accelerator.')
        static_fill = _canonical_counts(
            self.static_reserved_fill_target_by_accelerator,
            'static_reserved_fill_target_by_accelerator')
        if set(static_fill) - accounting_cards:
            raise ValueError('Capacity planner static fill names an unknown '
                             'accelerator.')
        if (not isinstance(self.paid_launch_priority_by_accelerator, Mapping) or
                any(not isinstance(card, str) or not card or
                    card != card.casefold() or type(priority) is not int or
                    not constants.LB_REQUEST_PRIORITY_MIN <= priority <=
                    constants.LB_REQUEST_PRIORITY_MAX for card, priority in
                    self.paid_launch_priority_by_accelerator.items()) or
                set(self.paid_launch_priority_by_accelerator) -
                accounting_cards):
            raise ValueError('Capacity planner paid priorities are malformed.')
        if self.expected_paid_residual_by_accelerator is None:
            raise ValueError('Capacity planner has no exact paid residual.')
        paid = _canonical_counts(self.expected_paid_residual_by_accelerator,
                                 'expected_paid_residual_by_accelerator')
        if set(paid) - accounting_cards:
            raise ValueError('Capacity planner paid residual names an '
                             'unknown accelerator.')
        if self.expected_paid_launch_target_by_accelerator is None:
            raise ValueError('Capacity planner has no exact paid launch '
                             'target.')
        paid_launch = _canonical_counts(
            self.expected_paid_launch_target_by_accelerator,
            'expected_paid_launch_target_by_accelerator')
        if set(paid_launch) - accounting_cards:
            raise ValueError('Capacity planner paid launch target names an '
                             'unknown accelerator.')
        planner_snapshot, candidate = self.decode_planner()
        if (not candidate.attribution_complete or candidate.kind
                is capacity_planning.CapacityPlanKind.INCOMPLETE or
                candidate.source_generation
                != planner_snapshot.source_generation):
            raise ValueError('Capacity planner returned incomplete or stale '
                             'authority.')
        candidate_target = _capacity_for_accounting_cards(
            candidate.supply_aware_demand_target, accounting_cards,
            'planner candidate supply-aware demand target')
        candidate_commitment = _capacity_for_accounting_cards(
            candidate.new_reserved_capacity_committed, accounting_cards,
            'planner candidate new reservation commitment')
        candidate_paid = _capacity_for_accounting_cards(
            candidate.paid_residual, accounting_cards,
            'planner candidate paid residual')
        candidate_paid_launch = _capacity_for_accounting_cards(
            candidate.paid_launch_target, accounting_cards,
            'planner candidate paid launch target')
        positive_paid_cards = {
            card for card, count in candidate_paid_launch.items() if count > 0
        }
        if set(self.paid_launch_priority_by_accelerator) != positive_paid_cards:
            raise ValueError('Capacity planner paid priorities do not exactly '
                             'cover its positive paid launch cards.')
        candidate_static_fill = _capacity_for_accounting_cards(
            candidate.static_prefill_target, accounting_cards,
            'planner candidate static prefill target')
        complete_commitment = {
            card: commitment.get(card, 0) for card in sorted(accounting_cards)
        }
        complete_paid = {
            card: paid.get(card, 0) for card in sorted(accounting_cards)
        }
        complete_paid_launch = {
            card: paid_launch.get(card, 0) for card in sorted(accounting_cards)
        }
        complete_static_fill = {
            card: static_fill.get(card, 0) for card in sorted(accounting_cards)
        }
        if candidate_target != target:
            raise ValueError('Capacity planner envelope changes its traffic '
                             'target.')
        if candidate_commitment != complete_commitment:
            raise ValueError('Capacity planner envelope changes its new '
                             'reservation commitment.')
        if candidate_paid != complete_paid:
            raise ValueError('Capacity planner envelope changes its paid '
                             'residual.')
        if candidate_paid_launch != complete_paid_launch:
            raise ValueError('Capacity planner envelope changes its paid '
                             'launch target.')
        if candidate_static_fill != complete_static_fill:
            raise ValueError('Capacity planner envelope changes its static '
                             'prefill target.')
        return target


@dataclasses.dataclass(frozen=True)
class PaidLaunchAuthority:
    """Immutable planner tuple copied into one or more paid claims."""

    service_name: str
    service_hash: str
    generation: int
    content_sha256: str
    demand_feed_generation: int
    demand_source_epoch: int
    paid_residual_by_accelerator: tuple[tuple[str, int], ...]
    paid_launch_target_by_accelerator: tuple[tuple[str, int], ...]
    reserved_fill_authority: ReservedFillPlanAuthority
    capacity_unit: capacity_planning.CapacityUnit
    physical_gpu_width_by_accelerator: tuple[tuple[str, int], ...]
    backend_num_nodes: int = 1

    def economic_residual(self) -> dict[str, int]:
        return dict(self.paid_residual_by_accelerator)

    def remaining_launch_capacity(self) -> dict[str, int]:
        return dict(self.paid_launch_target_by_accelerator)

    def backend_shape(
            self, accelerator: str) -> serve_paid_capacity.PhysicalBackendShape:
        """Return the immutable physical shape authorized for one backend."""
        card = accelerator.casefold()
        widths = {
            raw_card.casefold(): width
            for raw_card, width in self.physical_gpu_width_by_accelerator
        }
        physical_width = widths.get(card)
        if (type(physical_width) is not int or physical_width < 1 or  # pylint: disable=unidiomatic-typecheck
                type(self.backend_num_nodes) is not int or  # pylint: disable=unidiomatic-typecheck
                self.backend_num_nodes < 1 or self.capacity_unit
                not in (capacity_planning.CapacityUnit.PHYSICAL_BACKEND,
                        capacity_planning.CapacityUnit.LOGICAL_GPU) or
            (self.capacity_unit is capacity_planning.CapacityUnit.LOGICAL_GPU
             and self.backend_num_nodes != 1)):
            raise CapacityAdmissionConflict(
                f'Capacity plan has no exact backend claim shape for {card!r}.')
        try:
            return serve_paid_capacity.PhysicalBackendShape(
                accelerator=card,
                gpu_units_per_node=physical_width,
                num_nodes=self.backend_num_nodes)
        except serve_paid_capacity.PaidGPUAttributionError as error:
            raise CapacityAdmissionConflict(
                f'Capacity plan has no exact backend claim shape for {card!r}.'
            ) from error

    def claim_units_per_backend(self, accelerator: str) -> int:
        """Return immutable plan units debited by one backend claim."""
        shape = self.backend_shape(accelerator)
        if self.capacity_unit is capacity_planning.CapacityUnit.PHYSICAL_BACKEND:
            return 1
        return shape.gpu_units_per_node

    def claim_values(self, accelerator: str, units: int = 1) -> dict[str, Any]:
        card = accelerator.casefold()
        _positive_int(units, 'units')
        remaining = self.remaining_launch_capacity()
        debit_card = (card if remaining.get(card, 0) >= units else
                      AGGREGATE_ACCELERATOR)
        if remaining.get(debit_card, 0) < units:
            raise CapacityAdmissionConflict(
                f'Capacity plan has no whole-backend paid launch authority '
                f'for {card!r}.')
        return {
            'capacity_plan_generation': self.generation,
            'capacity_plan_sha256': self.content_sha256,
            'demand_feed_generation': self.demand_feed_generation,
            'demand_source_epoch': self.demand_source_epoch,
            'capacity_plan_accelerator': debit_card,
            'capacity_plan_units': units,
        }


@dataclasses.dataclass(frozen=True, kw_only=True)
class CommittedFillDemandWitness:
    """PostgreSQL-committed causal demand lease for the free-capacity gate.

    This witness is deliberately longer-lived than provider-effect authority.
    ``serve_capacity_plan_heads.valid_until`` continues to fence launches at
    the short capacity-plan TTL; the fill poller uses ``refreshed_at`` with a
    separate bounded horizon only to acquire reservation entitlement.  The
    semantic digest excludes live replica, intent, reservation, and scheduler
    inventory so materializing the plan cannot invalidate the demand that
    caused it. ``demand_feed_generation`` is always the generation that the
    production planner consumed. ``observed_demand_feed_generation`` may be
    newer only for an explicitly retained monotonic deadline lower bound; it
    never upgrades the old lease into current paid/provider authority.
    """

    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    service_version: int
    demand_source_epoch: int
    demand_feed_generation: int
    observed_demand_feed_generation: int
    route_generation: int
    route_sha256: str
    route_source_epoch: int
    capacity_plan_generation: int
    capacity_plan_sha256: str
    target_capacity: int
    reservation_acquisition_classes: (
        tuple[compatibility_matching.CompatibilityDemand, ...] | None)
    semantic_sha256: str
    refreshed_at: datetime.datetime

    def __post_init__(self) -> None:
        positive = (
            self.service_lifecycle_epoch,
            self.service_version,
            self.demand_source_epoch,
            self.demand_feed_generation,
            self.observed_demand_feed_generation,
            self.route_generation,
            self.route_source_epoch,
            self.capacity_plan_generation,
        )
        if (not isinstance(self.service_name, str) or not self.service_name or
                not isinstance(self.service_hash, str) or
                not self.service_hash or
                any(type(value) is not int or value < 1 for value in positive)
                or self.observed_demand_feed_generation
                < self.demand_feed_generation or
                type(self.target_capacity) is not int or
                self.target_capacity < 0 or
            (self.reservation_acquisition_classes is not None and
             (not isinstance(self.reservation_acquisition_classes, tuple) or
              any(not isinstance(item,
                                 compatibility_matching.CompatibilityDemand)
                  for item in self.reservation_acquisition_classes) or
              sum(item.count for item in self.reservation_acquisition_classes)
              != self.target_capacity)) or
                _SHA256_RE.fullmatch(self.route_sha256) is None or
                _SHA256_RE.fullmatch(self.capacity_plan_sha256) is None or
                _SHA256_RE.fullmatch(self.semantic_sha256) is None or
                not isinstance(self.refreshed_at, datetime.datetime)):
            raise ValueError('Committed fill-demand witness is malformed.')


def _notify_fill_demand_witness(service_name: str,
                                semantic_sha256: str) -> None:
    """Wake this process's poller after a committed semantic plan."""
    if _SHA256_RE.fullmatch(semantic_sha256) is None:
        raise ValueError('Fill-demand witness wake digest is malformed.')
    with _FILL_DEMAND_WITNESS_WAKE:
        if (_FILL_DEMAND_WITNESS_WAKE_DIGEST.get(service_name) ==
                semantic_sha256):
            return
        _FILL_DEMAND_WITNESS_WAKE_DIGEST[service_name] = semantic_sha256
        _FILL_DEMAND_WITNESS_WAKE_SEQUENCE[service_name] = (
            _FILL_DEMAND_WITNESS_WAKE_SEQUENCE.get(service_name, 0) + 1)
        _FILL_DEMAND_WITNESS_WAKE.notify_all()


def fill_demand_witness_wake_sequence(service_name: str) -> int:
    """Return the process-local lost-wakeup token for one service."""
    with _FILL_DEMAND_WITNESS_WAKE:
        return _FILL_DEMAND_WITNESS_WAKE_SEQUENCE.get(service_name, 0)


def wait_for_fill_demand_witness(
    service_name: str,
    after_sequence: int,
    timeout_seconds: float,
    stop_event: threading.Event | None = None,
) -> tuple[int, bool]:
    """Wait for plan publication without making the wakeup authoritative."""
    if (not isinstance(service_name, str) or not service_name or
            type(after_sequence) is not int or after_sequence < 0 or
            not isinstance(timeout_seconds, (int, float)) or
            isinstance(timeout_seconds, bool) or timeout_seconds < 0):
        raise ValueError('Fill-demand witness wait is malformed.')
    deadline = time.monotonic() + float(timeout_seconds)
    with _FILL_DEMAND_WITNESS_WAKE:
        while (_FILL_DEMAND_WITNESS_WAKE_SEQUENCE.get(service_name, 0)
               <= after_sequence):
            if stop_event is not None and stop_event.is_set():
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            # The stop event does not share this condition. Bound its shutdown
            # latency without a second event/condition ownership protocol.
            _FILL_DEMAND_WITNESS_WAKE.wait(timeout=min(remaining, 1.0))
        current = _FILL_DEMAND_WITNESS_WAKE_SEQUENCE.get(service_name, 0)
    return current, current > after_sequence


@dataclasses.dataclass(frozen=True)
class CommittedCapacityPlan:
    """One planner candidate proven committed under the PostgreSQL fence."""

    authority: PaidLaunchAuthority
    demand_snapshot: demand_state.DurableAutoscalingSnapshot
    planner_snapshot: capacity_planning.CapacityPlanningSnapshot
    candidate: capacity_planning.CapacityPlanCandidate
    allocation_map: AuthenticatedAllocationMap | None
    paid_launch_receipt: serve_paid_capacity.PaidLaunchReceipt

    def __post_init__(self) -> None:
        if (not isinstance(self.authority, PaidLaunchAuthority) or
                not isinstance(self.demand_snapshot,
                               demand_state.DurableAutoscalingSnapshot) or
                not isinstance(self.planner_snapshot,
                               capacity_planning.CapacityPlanningSnapshot) or
                not isinstance(self.candidate,
                               capacity_planning.CapacityPlanCandidate) or
                not isinstance(self.paid_launch_receipt,
                               serve_paid_capacity.PaidLaunchReceipt) or
                self.candidate.snapshot_fingerprint
                != self.planner_snapshot.fingerprint):
            raise ValueError('Committed capacity plan is malformed.')
        receipt = self.paid_launch_receipt
        if ((receipt.service_name, receipt.service_hash,
             receipt.service_lifecycle_epoch, receipt.service_version) !=
            (self.authority.service_name, self.authority.service_hash,
             self.demand_snapshot.reconcile_authority.service_lifecycle_epoch,
             self.planner_snapshot.service_version) or
                receipt.capacity_plan_generation != self.authority.generation or
                receipt.capacity_plan_sha256 != self.authority.content_sha256 or
                receipt.capacity_unit != self.authority.capacity_unit.value):
            raise ValueError('Committed paid launch receipt is malformed.')
        if self.allocation_map is not None and not isinstance(
                self.allocation_map,
                reserved_fill_planner.AuthenticatedAllocationMap):
            raise ValueError('Committed capacity plan allocation is malformed.')


@dataclasses.dataclass(frozen=True)
class ReservedSupplyProjection:
    """One PostgreSQL-locked supply and scheduler-capacity projection.

    ``economic_kueue_capacity`` is the complete typed Kueue view produced by
    the same transaction as ``economic_replica_infos`` and the economic
    inventory.  Durable local actuation must consume this object directly;
    a pre-lock controller observation is not launch or retirement authority.
    """

    pending_zero_cost_capacity_by_accelerator: Mapping[str, int]
    allocation_reserved_capacity_by_accelerator: Mapping[str, int]
    economic_replica_infos: tuple[Any, ...]
    economic_kueue_capacity: (kueue_lane_capacity.KueueReplicaCapacitySnapshot)
    economic_capacity_graph_sha256: str
    existing_zero_cost_capacity_by_accelerator: Mapping[str, int] = (
        dataclasses.field(default_factory=dict))
    existing_paid_capacity_by_accelerator: Mapping[str,
                                                   int] = (dataclasses.field(
                                                       default_factory=dict))
    charged_paid_gpu_units: int = 0
    authenticated_capacity_by_accelerator: Mapping[str,
                                                   int] = (dataclasses.field(
                                                       default_factory=dict))
    eligible_capacity_by_accelerator: Mapping[str, int] = (dataclasses.field(
        default_factory=dict))
    policy: ReservedSupplyPolicy = ReservedSupplyPolicy.DISABLED
    evidence_state: ReservedSupplyEvidenceState = (
        ReservedSupplyEvidenceState.NOT_APPLICABLE)
    fill_policy_sha256: str = ''
    reservation_evidence_sha256: str = ''
    demand_witness_scope_sha256: str = ''
    allocation_demand_witness_sha256: str | None = None
    allocation_demonstrated_need: int | None = None
    allocation_ceiling: int = 0
    allocation_map: Any | None = None
    reserved_accelerators: tuple[str, ...] = ()
    allocation_bound: bool = True
    prior_policy_state: capacity_planning.CapacityPolicyState | None = None
    prior_candidate: capacity_planning.CapacityPlanCandidate | None = None
    planning_db_epoch: float | None = None
    max_live_paid_gpu_units: int | None = None

    def __post_init__(self) -> None:
        fields = (
            'pending_zero_cost_capacity_by_accelerator',
            'allocation_reserved_capacity_by_accelerator',
            'existing_zero_cost_capacity_by_accelerator',
            'existing_paid_capacity_by_accelerator',
            'authenticated_capacity_by_accelerator',
            'eligible_capacity_by_accelerator',
        )
        canonical: dict[str, dict[str, int]] = {}
        for field in fields:
            canonical[field] = _canonical_counts(getattr(self, field), field)
            object.__setattr__(self, field, canonical[field])
        if (not isinstance(self.policy, ReservedSupplyPolicy) or
                not isinstance(self.evidence_state, ReservedSupplyEvidenceState)
                or type(self.allocation_bound) is not bool or
                not isinstance(self.economic_replica_infos, tuple) or
                not isinstance(self.economic_kueue_capacity,
                               kueue_lane_capacity.KueueReplicaCapacitySnapshot)
                or not isinstance(self.economic_capacity_graph_sha256, str) or
                _SHA256_RE.fullmatch(self.economic_capacity_graph_sha256)
                is None or not isinstance(self.fill_policy_sha256, str) or
                _SHA256_RE.fullmatch(self.fill_policy_sha256) is None or
                not isinstance(self.reservation_evidence_sha256, str) or
                not isinstance(self.demand_witness_scope_sha256, str) or
            (self.allocation_demand_witness_sha256 is not None and
             (not isinstance(self.allocation_demand_witness_sha256, str) or
              _SHA256_RE.fullmatch(
                  self.allocation_demand_witness_sha256) is None)) or
            (self.allocation_demonstrated_need is not None and
             (type(self.allocation_demonstrated_need) is not int or
              self.allocation_demonstrated_need < 0)) or
                type(self.allocation_ceiling) is not int or
                self.allocation_ceiling < 0 or
                type(self.charged_paid_gpu_units) is not int or
                self.charged_paid_gpu_units < 0 or
            (self.planning_db_epoch is not None and
             (not isinstance(self.planning_db_epoch, (int, float)) or
              isinstance(self.planning_db_epoch, bool) or
              not math.isfinite(float(self.planning_db_epoch)) or
              self.planning_db_epoch < 0)) or
            (self.max_live_paid_gpu_units is not None and
             (type(self.max_live_paid_gpu_units) is not int or
              self.max_live_paid_gpu_units < 0))):
            raise ValueError('Reserved supply projection is malformed.')
        history = (self.prior_policy_state, self.prior_candidate,
                   self.planning_db_epoch)
        if any(value is None for value in history) and any(
                value is not None for value in history):
            raise ValueError('Reserved supply policy history is incomplete.')
        if (self.prior_policy_state is not None and
            (not isinstance(self.prior_policy_state,
                            capacity_planning.CapacityPolicyState) or
             not isinstance(self.prior_candidate,
                            capacity_planning.CapacityPlanCandidate))):
            raise ValueError('Reserved supply policy history is malformed.')
        if self.planning_db_epoch is not None:
            object.__setattr__(self, 'planning_db_epoch',
                               float(self.planning_db_epoch))
        if (not isinstance(self.reserved_accelerators, tuple) or
                any(not isinstance(card, str) or not card
                    for card in self.reserved_accelerators) or
                tuple(sorted(set(self.reserved_accelerators)))
                != self.reserved_accelerators):
            raise ValueError('Reserved supply accelerator catalog is invalid.')
        if self.allocation_bound != (self.allocation_map is not None):
            raise ValueError('Reserved supply allocation binding is malformed.')
        authenticated = canonical['authenticated_capacity_by_accelerator']
        eligible = canonical['eligible_capacity_by_accelerator']
        allocation_reserved = canonical[
            'allocation_reserved_capacity_by_accelerator']
        if any(count > authenticated.get(card, 0)
               for card, count in eligible.items()):
            raise ValueError('Eligible reserved supply exceeds its envelope.')
        if self.policy is ReservedSupplyPolicy.DISABLED:
            if (self.evidence_state
                    is not ReservedSupplyEvidenceState.NOT_APPLICABLE or
                    self.reservation_evidence_sha256 or self.allocation_bound or
                    self.demand_witness_scope_sha256 or
                    self.allocation_demand_witness_sha256 is not None or
                    self.allocation_demonstrated_need is not None or
                    self.allocation_ceiling != 0 or
                    any(authenticated.values()) or any(eligible.values()) or
                    any(allocation_reserved.values())):
                raise ValueError('Disabled reserved supply grants authority.')
        else:
            if (_SHA256_RE.fullmatch(self.reservation_evidence_sha256) is None):
                raise ValueError('Enabled reserved supply has malformed '
                                 'reservation evidence.')
            if self.evidence_state is (
                    ReservedSupplyEvidenceState.NOT_APPLICABLE):
                raise ValueError('Enabled reserved supply has no evidence '
                                 'state.')
        if self.policy is ReservedSupplyPolicy.DEMAND_GATED:
            if (_SHA256_RE.fullmatch(self.demand_witness_scope_sha256) is None
                    or not self.reserved_accelerators):
                raise ValueError('Gated reserved supply has no witness scope.')
            if (self.evidence_state
                    is ReservedSupplyEvidenceState.AUTHENTICATED_SETTLED and
                (self.allocation_demand_witness_sha256 is None or
                 self.allocation_demonstrated_need is None)):
                raise ValueError(
                    'Settled gated reserved supply has no demand witness.')
        elif (self.demand_witness_scope_sha256 or
              self.allocation_demand_witness_sha256 is not None or
              self.allocation_demonstrated_need is not None or
              self.allocation_ceiling != 0):
            raise ValueError('Ungated reserved supply carries gate evidence.')
        if self.evidence_state in (
                ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED,
                ReservedSupplyEvidenceState.UNAVAILABLE) and eligible:
            raise ValueError('Ineligible reservation evidence grants supply.')
        if (self.evidence_state is ReservedSupplyEvidenceState.UNAVAILABLE and
            (self.allocation_bound or authenticated)):
            raise ValueError('Unavailable reservation evidence is bound.')

    def additional_capacity_by_accelerator(self) -> dict[str, int]:
        cards = (set(self.pending_zero_cost_capacity_by_accelerator) |
                 set(self.eligible_capacity_by_accelerator))
        return {
            card: (self.pending_zero_cost_capacity_by_accelerator.get(card, 0) +
                   self.eligible_capacity_by_accelerator.get(card, 0)
                  ) for card in sorted(cards)
        }


def _validate_planner_against_locked_supply(
    *,
    planner_snapshot: capacity_planning.CapacityPlanningSnapshot,
    candidate: capacity_planning.CapacityPlanCandidate,
    service_version: int,
    accounting_cards: set[str],
    capacity_target: Mapping[str, int],
    reservation_commitment: Mapping[str, int],
    static_fill_target: Mapping[str, int],
    supply_projection: ReservedSupplyProjection | None,
    expected_planning_state_fingerprint: str | None,
) -> None:
    """Bind a pure planner envelope to the PostgreSQL-locked supply facts."""
    configured_cards = {
        card.casefold() for card in planner_snapshot.configured_accelerators
    }
    if (planner_snapshot.service_version != service_version or
            configured_cards != accounting_cards):
        raise CapacityAdmissionConflict(
            'Planner snapshot names a different service version or card set.')
    expected_source_fingerprint = expected_planning_state_fingerprint
    if supply_projection is not None:
        expected_source_fingerprint = locked_planning_source_fingerprint(
            expected_planning_state_fingerprint,
            supply_projection.economic_capacity_graph_sha256)
    if (expected_source_fingerprint is not None and
            planner_snapshot.source_fingerprint != expected_source_fingerprint):
        raise CapacityAdmissionConflict(
            'Planner snapshot names a different locked planning or scheduler '
            'capacity state.')
    if (candidate.source_generation != planner_snapshot.source_generation or
            candidate.snapshot_fingerprint != planner_snapshot.fingerprint):
        raise CapacityAdmissionConflict(
            'Planner candidate names a different snapshot generation.')
    if (supply_projection is not None and
        (supply_projection.prior_policy_state is not None or
         supply_projection.prior_candidate is not None or
         supply_projection.planning_db_epoch is not None)):
        policy_input = planner_snapshot.policy_input
        if (supply_projection.prior_policy_state is None or
                supply_projection.prior_candidate is None or
                supply_projection.planning_db_epoch is None or
                planner_snapshot.prior_policy_state
                != supply_projection.prior_policy_state or
                planner_snapshot.prior_candidate
                != supply_projection.prior_candidate or policy_input is None or
                policy_input.planning_db_epoch
                != supply_projection.planning_db_epoch or
                planner_snapshot.max_live_paid_gpu_units
                != supply_projection.max_live_paid_gpu_units):
            raise CapacityAdmissionConflict(
                'Planner policy history or PostgreSQL epoch changed under lock.'
            )
    configured_names = {
        card.casefold(): card
        for card in planner_snapshot.configured_accelerators
    }
    try:
        expected_reservation_catalog = (
            () if supply_projection is None else tuple(
                sorted((configured_names[card.casefold()]
                        for card in supply_projection.reserved_accelerators),
                       key=str.casefold)))
    except KeyError as error:
        raise CapacityAdmissionConflict(
            'Locked reservation catalog names an unconfigured card.') from error
    expected_witness_scope = ('' if supply_projection is None else
                              supply_projection.demand_witness_scope_sha256)
    if (planner_snapshot.configured_reservation_accelerators
            != expected_reservation_catalog or
            planner_snapshot.demand_witness_scope_sha256
            != expected_witness_scope):
        raise CapacityAdmissionConflict(
            'Planner reservation catalog or witness scope changed under lock.')

    expected_target = {
        card: capacity_target.get(card, 0) for card in sorted(accounting_cards)
    }
    expected_commitment = {
        card: reservation_commitment.get(card, 0)
        for card in sorted(accounting_cards)
    }
    expected_static = {
        card: static_fill_target.get(card, 0)
        for card in sorted(accounting_cards)
    }
    comparisons = (
        (candidate.supply_aware_demand_target, expected_target,
         'traffic target'),
        (candidate.new_reserved_capacity_committed, expected_commitment,
         'new reservation commitment'),
        (candidate.static_prefill_target, expected_static,
         'static prefill target'),
    )
    for capacity, expected, subject in comparisons:
        try:
            actual = _capacity_for_accounting_cards(
                capacity, accounting_cards, f'planner candidate {subject}')
        except ValueError as error:
            raise CapacityAdmissionConflict(
                f'Planner candidate {subject} is outside locked accounting.'
            ) from error
        if actual != expected:
            raise CapacityAdmissionConflict(
                f'Planner candidate {subject} changed before publication.')
    for field, subject in (
        ('paid_residual', 'paid residual'),
        ('paid_launch_target', 'paid launch target'),
        ('paid_packing_padding_target', 'paid packing padding'),
    ):
        try:
            _capacity_for_accounting_cards(getattr(candidate,
                                                   field), accounting_cards,
                                           f'planner candidate {subject}')
        except ValueError as error:
            raise CapacityAdmissionConflict(
                f'Planner candidate {subject} is outside locked accounting.'
            ) from error

    reservation = planner_snapshot.reservation
    expected_reservation_capacities: Mapping[str, Mapping[str, int]]
    if supply_projection is None:
        expected_policy = (
            capacity_planning.ReservationGatePolicy.NOT_CONFIGURED)
        expected_evidence = (
            capacity_planning.ReservationEvidenceState.NOT_APPLICABLE)
        expected_fingerprint = ''
        expected_reservation_capacities = {
            field: {
                card: 0 for card in sorted(accounting_cards)
            } for field in ('authenticated_capacity', 'eligible_capacity',
                           'pending_zero_cost_capacity',
                           'existing_zero_cost_capacity',
                           'existing_paid_capacity')
        }
    else:
        expected_policy = _planner_reservation_gate_policy(
            supply_projection.policy)
        expected_evidence = {
            ReservedSupplyEvidenceState.NOT_APPLICABLE:
                capacity_planning.ReservationEvidenceState.NOT_APPLICABLE,
            ReservedSupplyEvidenceState.AUTHENTICATED_SETTLED:
                capacity_planning.ReservationEvidenceState.
                AUTHENTICATED_SETTLED,
            ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED:
                capacity_planning.ReservationEvidenceState.
                AUTHENTICATED_UNSETTLED,
            ReservedSupplyEvidenceState.UNAVAILABLE:
                capacity_planning.ReservationEvidenceState.UNAVAILABLE,
        }[supply_projection.evidence_state]
        expected_fingerprint = supply_projection.reservation_evidence_sha256
        expected_reservation_capacities = {
            'authenticated_capacity':
                supply_projection.authenticated_capacity_by_accelerator,
            'eligible_capacity':
                supply_projection.eligible_capacity_by_accelerator,
            'pending_zero_cost_capacity':
                supply_projection.pending_zero_cost_capacity_by_accelerator,
            'existing_zero_cost_capacity':
                supply_projection.existing_zero_cost_capacity_by_accelerator,
            'existing_paid_capacity':
                supply_projection.existing_paid_capacity_by_accelerator,
        }
    expected_charged_paid_gpu_units = (0 if supply_projection is None else
                                       supply_projection.charged_paid_gpu_units)
    expected_allocation_witness = (
        None if supply_projection is None else
        supply_projection.allocation_demand_witness_sha256)
    expected_demonstrated_need = (
        None if supply_projection is None else
        supply_projection.allocation_demonstrated_need)
    expected_allocation_ceiling = (0 if supply_projection is None else
                                   supply_projection.allocation_ceiling)
    if (reservation.gate_policy is not expected_policy or
            reservation.evidence_state is not expected_evidence or
            reservation.evidence_fingerprint != expected_fingerprint or
            reservation.charged_paid_gpu_units
            != expected_charged_paid_gpu_units or
            reservation.allocation_demand_witness_sha256
            != expected_allocation_witness or
            reservation.allocation_demonstrated_need
            != expected_demonstrated_need or
            reservation.allocation_ceiling != expected_allocation_ceiling):
        raise CapacityAdmissionConflict(
            'Planner reservation policy or evidence changed under lock.')
    for field, expected_counts in expected_reservation_capacities.items():
        try:
            actual = _capacity_for_accounting_cards(
                getattr(reservation, field), accounting_cards,
                f'planner reservation {field}')
            expected = _canonical_counts(expected_counts, field)
        except ValueError as error:
            raise CapacityAdmissionConflict(
                'Planner reservation evidence is outside locked accounting.'
            ) from error
        expected = {
            card: expected.get(card, 0) for card in sorted(accounting_cards)
        }
        if actual != expected:
            raise CapacityAdmissionConflict(
                f'Planner reservation {field} changed under lock.')
    try:
        launch_target = _capacity_for_accounting_cards(
            candidate.reserved_launch_target, accounting_cards,
            'planner candidate reserved launch target')
        locked_eligible = _canonical_counts(
            expected_reservation_capacities['eligible_capacity'],
            'eligible_capacity')
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Planner reserved launch target is outside locked accounting.'
        ) from error
    locked_eligible = {
        card: locked_eligible.get(card, 0) for card in sorted(accounting_cards)
    }
    if any(
            launch_target.get(card, 0) > locked_eligible.get(card, 0)
            for card in accounting_cards):
        raise CapacityAdmissionConflict(
            'Planner reserved launch target exceeds locked eligible supply.')
    if any(
            expected_commitment.get(card, 0) > launch_target.get(card, 0)
            for card in accounting_cards):
        raise CapacityAdmissionConflict(
            'Planner reservation debit exceeds its physical launch target.')


def _validate_committed_plan_row(
    row: Mapping[str, Any],
    *,
    expected_snapshot: capacity_planning.CapacityPlanningSnapshot,
    expected_candidate: capacity_planning.CapacityPlanCandidate,
    accounting_cards: set[str],
    demand_feed_generation: int,
) -> PaidLaunchAuthority:
    """Authenticate the SELECT-after-write row before exposing authority."""
    payload = row.get('payload')
    digest = row.get('content_sha256')
    if (not isinstance(payload, Mapping) or not isinstance(digest, str) or
            _SHA256_RE.fullmatch(digest) is None or
            capacity_plan_content_sha256(payload) != digest):
        raise CapacityAdmissionConflict(
            'Persisted capacity plan content hash is invalid.')
    required_fields = {
        'protocol_version', 'service', 'source', 'normalized_demand',
        'capacity_target_by_accelerator',
        'existing_zero_cost_capacity_by_accelerator',
        'pending_zero_cost_capacity_by_accelerator',
        'allocation_reserved_capacity_by_accelerator',
        'existing_paid_capacity_by_accelerator', 'paid_residual_by_accelerator',
        'paid_launch_target_by_accelerator', 'reserved_fill_authority',
        'planner'
    }
    allowed_fields = required_fields | {'economic_capacity_graph_sha256'}
    if not required_fields.issubset(payload) or set(payload) - allowed_fields:
        raise CapacityAdmissionConflict(
            'Persisted capacity plan has an open or incomplete schema.')
    try:
        planner_snapshot, candidate = _decode_planner_payload(
            payload['planner'])
        target = _canonical_counts(payload['capacity_target_by_accelerator'],
                                   'capacity_target_by_accelerator')
        commitment = _canonical_counts(
            payload['allocation_reserved_capacity_by_accelerator'],
            'allocation_reserved_capacity_by_accelerator')
        paid = _canonical_counts(payload['paid_residual_by_accelerator'],
                                 'paid_residual_by_accelerator')
        paid_launch = _canonical_counts(
            payload['paid_launch_target_by_accelerator'],
            'paid_launch_target_by_accelerator')
        expected_target = _capacity_for_accounting_cards(
            candidate.supply_aware_demand_target, accounting_cards,
            'persisted planner traffic target')
        expected_commitment = _capacity_for_accounting_cards(
            candidate.new_reserved_capacity_committed, accounting_cards,
            'persisted planner reservation commitment')
        expected_paid = _positive_counts(
            _capacity_for_accounting_cards(candidate.paid_residual,
                                           accounting_cards,
                                           'persisted planner paid residual'))
        expected_paid_launch = _positive_counts(
            _capacity_for_accounting_cards(
                candidate.paid_launch_target, accounting_cards,
                'persisted planner paid launch target'))
    except (KeyError, TypeError, ValueError) as error:
        raise CapacityAdmissionConflict(
            'Persisted capacity plan planner envelope is invalid.') from error
    if (planner_snapshot != expected_snapshot or
            candidate != expected_candidate or target != expected_target or
            commitment != expected_commitment or paid != expected_paid or
            paid_launch != expected_paid_launch):
        raise CapacityAdmissionConflict(
            'Persisted capacity plan differs from its committed candidate.')
    return _authority(row, demand_feed_generation=demand_feed_generation)


def _authority(
    row: Mapping[str, Any],
    *,
    demand_feed_generation: int | None = None,
) -> PaidLaunchAuthority:
    payload = row['payload']
    if not isinstance(payload, Mapping):
        raise CapacityAdmissionConflict('Capacity plan payload is malformed.')
    paid = _canonical_counts(payload.get('paid_residual_by_accelerator', {}),
                             'paid_residual_by_accelerator')
    paid_launch = _canonical_counts(
        payload.get('paid_launch_target_by_accelerator', {}),
        'paid_launch_target_by_accelerator')
    try:
        reserved_fill_authority = ReservedFillPlanAuthority.from_mapping(
            payload.get('reserved_fill_authority'))
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Capacity plan has no valid reserved-fill authority.') from error
    try:
        _, candidate = _decode_planner_payload(payload.get('planner'))
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Capacity plan has no immutable backend claim shape.') from error
    return PaidLaunchAuthority(
        service_name=str(row['service_name']),
        service_hash=str(row['service_hash']),
        generation=int(row['generation']),
        content_sha256=str(row['content_sha256']),
        demand_feed_generation=int(
            row['demand_feed_generation'] if demand_feed_generation is
            None else demand_feed_generation),
        demand_source_epoch=int(row['demand_source_epoch']),
        paid_residual_by_accelerator=tuple(paid.items()),
        paid_launch_target_by_accelerator=tuple(paid_launch.items()),
        reserved_fill_authority=reserved_fill_authority,
        capacity_unit=candidate.capacity_unit,
        backend_num_nodes=candidate.backend_num_nodes,
        physical_gpu_width_by_accelerator=tuple(
            candidate.physical_gpu_width_by_accelerator.entries))


def _validate_reserved_fill_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    authority: ReservedFillPlanAuthority,
    *,
    reserved_fill_binding_required: bool,
    protocol_and_service_prelocked: bool,
) -> AuthenticatedAllocationMap | None:
    """Validate one positive plan's exact reserved-fill allocation identity."""
    if authority.mode is ReservedFillPlanAuthorityMode.NOT_APPLICABLE:
        if reserved_fill_binding_required:
            raise CapacityAdmissionConflict(
                'Durable-intent paid authority must name its exact current '
                'reserved-fill allocation.')
        return None
    if authority.mode is ReservedFillPlanAuthorityMode.UNBOUND_ZERO_REVOCATION:
        raise CapacityAdmissionConflict(
            'An unbound zero revocation cannot authorize a paid claim.')
    if authority.mode is ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE:
        if not reserved_fill_binding_required:
            raise CapacityAdmissionConflict(
                'Static reserved incompatibility requires enabled durable '
                'intent actuation.')
        config = _reserved_fill_service_config_in_connection(
            connection, service)
        if (config.reserved_accelerators is None or
                config.worker_projection_sha256 is None or
                authority.worker_projection_sha256
                != config.worker_projection_sha256 or
                set(authority.incompatible_accelerators) &
                set(config.reserved_accelerators)):
            raise CapacityAdmissionConflict(
                'Paid plan no longer proves static incompatibility with the '
                'current reserved worker projection.')
        return None
    if authority.mode is ReservedFillPlanAuthorityMode.GATE_INELIGIBLE:
        # Decode retained rows so recovery can inspect them, but never let the
        # former no-allocation escape authorize a new provider effect.  A
        # current writer replaces it through the causal allocation path.
        raise CapacityAdmissionConflict(
            'Legacy reservation-ineligible paid authority is fail-closed.')
    binding = authority.allocation
    assert binding is not None
    if not reserved_fill_binding_required:
        raise CapacityAdmissionConflict(
            'An allocation-bound paid plan requires enabled durable intent '
            'actuation.')
    if not protocol_and_service_prelocked:
        # Every production call path takes the shared protocol mutex before
        # lifecycle/service locks.  Acquiring it here after the service row
        # would invert the sequencer's canonical lock order.
        raise CapacityAdmissionConflict(
            'Allocation-bound paid validation lacks its protocol-first lock.')
    try:
        current = reserved_fill_allocation.ReservedFillAllocationRepository(
            connection.engine).read_current_in_connection(
                connection,
                str(service['name']),
                str(service['hash']),
                (service.get('controller_pid'), service.get('controller_ip')),
                protocol_and_service_prelocked=True)
    except (TypeError, ValueError,
            reserved_fill_allocation.ReservedFillAllocationError) as error:
        raise CapacityAdmissionConflict(
            'Reserved-fill allocation validation failed closed.') from error
    if current is None or binding != current.identity:
        raise CapacityAdmissionConflict(
            'Paid plan no longer names the exact current reserved-fill '
            'allocation.')
    return current


@dataclasses.dataclass(frozen=True)
class _LockedPaidLaunchVersionAuthority:
    """Raw immutable launch facts held by the elected-version row lock."""

    service_spec: bytes
    launch_yaml_content: str
    placement_catalog: Mapping[str, Any] | None
    placement_contract: placement_policy.PlacementContract
    controller_config: bytes | None
    controller_config_digest: str | None
    controller_config_snapshot_id: str | None


@dataclasses.dataclass(frozen=True)
class _ReservedFillServiceConfig:
    binding_required: bool
    max_capacity: int
    capacity_unit: FillCapacityUnit
    reserved_accelerators: tuple[str, ...] | None
    worker_projection_sha256: str | None
    configured_utilization_gate: bool
    fill_policy_sha256: str
    max_live_paid_gpu_units: int | None = None
    paid_launch_version: _LockedPaidLaunchVersionAuthority | None = None


def _reserved_fill_service_config_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
) -> _ReservedFillServiceConfig:
    """Read the immutable fill discriminator and ceiling under lock."""
    version_row = connection.execute(
        sqlalchemy.select(
            _VERSION_SPECS.c.spec,
            _VERSION_SPECS.c.worker_placement_projections,
            _VERSION_SPECS.c.yaml_content, _VERSION_SPECS.c.placement_catalog,
            _VERSION_SPECS.c.controller_config,
            _VERSION_SPECS.c.controller_config_digest,
            _VERSION_SPECS.c.controller_config_snapshot_id).where(
                _VERSION_SPECS.c.service_name == service['name'],
                _VERSION_SPECS.c.version == service['current_version'],
                _VERSION_SPECS.c.yaml_content.isnot(None),
                _VERSION_SPECS.c.quarantined_at.is_(None),
                _VERSION_SPECS.c.retired_at.is_(None)).with_for_update(
                    read=True)).one_or_none()
    if version_row is None:
        raise CapacityAdmissionConflict(
            'Current service version has no immutable reserved-fill spec.')
    serialized_spec = version_row[0]
    if isinstance(serialized_spec, memoryview):
        serialized_spec = serialized_spec.tobytes()
    if not isinstance(serialized_spec, bytes):
        raise CapacityAdmissionConflict(
            'Current service version has no immutable reserved-fill spec.')
    try:
        spec = pickle.loads(serialized_spec)
        if type(spec) is not service_spec.SkyServiceSpec:
            raise TypeError('Unexpected persisted service-spec type.')
        fill_enabled = spec.reserved_capacity_fill
        maximum = (spec.max_replicas
                   if spec.max_replicas is not None else spec.min_replicas)
        replica_unit = spec.replica_unit
        configured_utilization_gate = spec.reserved_fill_utilization_gate
        max_live_paid_gpu_units = spec.max_live_paid_gpu_units
        placement_contract = spec.placement_contract
    except Exception as error:  # pylint: disable=broad-except
        raise CapacityAdmissionConflict(
            'Current service version reserved-fill spec is malformed.'
        ) from error
    if type(fill_enabled) is not bool:
        raise CapacityAdmissionConflict(
            'Current service version reserved-fill selector is malformed.')
    if type(configured_utilization_gate) is not bool:
        raise CapacityAdmissionConflict(
            'Current service version utilization gate is malformed.')
    if (max_live_paid_gpu_units is not None and
        (type(max_live_paid_gpu_units) is not int or
         max_live_paid_gpu_units < 0)):
        raise CapacityAdmissionConflict(
            'Current service version paid GPU cap is malformed.')
    mode = service.get('reserved_fill_actuation_mode')
    if mode not in ('DIRECT_REPLICA', 'DURABLE_INTENT'):
        raise CapacityAdmissionConflict(
            'Service reserved-fill actuation mode is malformed.')
    if (not isinstance(maximum, int) or isinstance(maximum, bool) or
            maximum < 0 or replica_unit not in ('physical_backend', 'logical')):
        raise CapacityAdmissionConflict(
            'Current service reserved-fill ceiling is malformed.')
    capacity_unit = (reserved_fill_planner.FillCapacityUnit.LOGICAL
                     if replica_unit == 'logical' else
                     reserved_fill_planner.FillCapacityUnit.PHYSICAL)
    reserved_accelerators = None
    worker_projection_sha256 = None
    raw_worker_projections = version_row[1]
    if raw_worker_projections is not None:
        try:
            worker_projections = (
                kubernetes_identity.validate_worker_placement_projections(
                    raw_worker_projections, allow_none=False))
        except (TypeError, ValueError) as error:
            raise CapacityAdmissionConflict(
                'Current service worker projection is malformed.') from error
        assert worker_projections is not None
        reserved_accelerators = tuple(
            sorted({
                str(projection['accelerator_name']).casefold()
                for projection in worker_projections
            }))
        worker_projection_sha256 = _sha256(worker_projections)
    fill_policy_sha256 = _sha256({
        'binding_required': fill_enabled and mode == 'DURABLE_INTENT',
        'max_capacity': maximum,
        'capacity_unit': capacity_unit.value,
        'configured_utilization_gate': configured_utilization_gate,
        'reserved_accelerators': reserved_accelerators,
        'worker_projection_sha256': worker_projection_sha256,
    })
    controller_config = version_row[4]
    if isinstance(controller_config, memoryview):
        controller_config = controller_config.tobytes()
    paid_launch_version = _LockedPaidLaunchVersionAuthority(
        service_spec=serialized_spec,
        launch_yaml_content=version_row[2],
        placement_catalog=version_row[3],
        placement_contract=placement_contract,
        controller_config=controller_config,
        controller_config_digest=version_row[5],
        controller_config_snapshot_id=version_row[6])
    return _ReservedFillServiceConfig(
        binding_required=fill_enabled and mode == 'DURABLE_INTENT',
        max_capacity=maximum,
        capacity_unit=capacity_unit,
        reserved_accelerators=(reserved_accelerators),
        worker_projection_sha256=(worker_projection_sha256),
        configured_utilization_gate=configured_utilization_gate,
        fill_policy_sha256=fill_policy_sha256,
        max_live_paid_gpu_units=max_live_paid_gpu_units,
        paid_launch_version=paid_launch_version)


def _reserved_fill_binding_required_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
) -> bool:
    """Read the elected immutable service-spec discriminator under lock."""
    return _reserved_fill_service_config_in_connection(connection,
                                                       service).binding_required


def _reservation_evidence_sha256(
    config: _ReservedFillServiceConfig,
    allocation: AuthenticatedAllocationMap | None,
) -> str:
    """Fingerprint the exact locked allocation presence and fill policy."""
    if not config.binding_required:
        # A disabled reservation protocol has no reservation evidence.  Its
        # locked existing zero-cost and paid inventory remains planner supply,
        # but it must not be confused with authority for a new fill launch.
        return ''
    return _sha256({
        'schema_version': 1,
        'fill_policy_sha256': config.fill_policy_sha256,
        'allocation_identity':
            (None if allocation is None else allocation.identity.to_mapping()),
    })


def _allocation_grants_are_represented(
    config: _ReservedFillServiceConfig,
    allocation: AuthenticatedAllocationMap,
    *,
    existing_zero_cost: Mapping[str, int],
    pending_zero_cost: Mapping[str, int],
    allocation_reserved: Mapping[str, int],
) -> bool:
    """Whether locked inventory can realize every authenticated pool grant.

    A broker grant is entitlement, not supply.  Claim heartbeats may lag a
    provider or scheduler transition, so they cannot prove that a service
    still owns the holdings used to satisfy a grant.  The final capacity-plan
    lock instead requires each granted unit to be represented by a usable
    zero-cost replica, a live pending zero-cost intent, or currently feedable
    allocation tail before compatible paid capacity may be admitted.
    """
    if not isinstance(allocation,
                      reserved_fill_planner.AuthenticatedAllocationMap):
        return False
    granted: dict[str, int] = {}
    for snapshot in allocation.pool_snapshots:
        if snapshot.grant == 0:
            continue
        shapes = {(location.accelerator.casefold(), location.accelerator_count)
                  for location in snapshot.locations}
        if len(shapes) != 1:
            # A scalar grant over multiple cards has no exact-card meaning.
            return False
        card, accelerator_count = next(iter(shapes))
        granted[card] = (granted.get(card, 0) + snapshot.grant *
                         config.capacity_unit.intent_cost(accelerator_count))

    existing = _canonical_counts(existing_zero_cost,
                                 'existing_zero_cost_capacity')
    pending = _canonical_counts(pending_zero_cost, 'pending_zero_cost_capacity')
    feedable = _canonical_counts(allocation_reserved,
                                 'allocation_reserved_capacity')
    accounting_cards = set(existing) | set(pending) | set(feedable)
    if accounting_cards == {AGGREGATE_ACCELERATOR}:
        return sum(
            granted.values()) <= (existing.get(AGGREGATE_ACCELERATOR, 0) +
                                  pending.get(AGGREGATE_ACCELERATOR, 0) +
                                  feedable.get(AGGREGATE_ACCELERATOR, 0))
    if AGGREGATE_ACCELERATOR in accounting_cards:
        return False
    return all(units <= (existing.get(card, 0) + pending.get(card, 0) +
                         feedable.get(card, 0))
               for card, units in granted.items())


def _reserved_supply_policy_and_evidence(
    config: _ReservedFillServiceConfig,
    allocation: AuthenticatedAllocationMap | None,
    allocation_reserved: Mapping[str, int],
    *,
    existing_zero_cost: Mapping[str, int],
    pending_zero_cost: Mapping[str, int],
) -> tuple[ReservedSupplyPolicy, ReservedSupplyEvidenceState, dict[str, int],
           dict[str, int]]:
    """Classify locked reservation evidence without erasing paid demand."""
    if not config.binding_required:
        return (ReservedSupplyPolicy.DISABLED,
                ReservedSupplyEvidenceState.NOT_APPLICABLE, {}, {})
    policy = (ReservedSupplyPolicy.DEMAND_GATED
              if config.configured_utilization_gate else
              ReservedSupplyPolicy.STATIC_PREFILL)
    if allocation is None:
        return (policy, ReservedSupplyEvidenceState.UNAVAILABLE, {}, {})
    authenticated = _canonical_counts(allocation_reserved,
                                      'authenticated_capacity_by_accelerator')
    grants_settled = bool(allocation.upward_grants_settled and
                          _allocation_grants_are_represented(
                              config,
                              allocation,
                              existing_zero_cost=existing_zero_cost,
                              pending_zero_cost=pending_zero_cost,
                              allocation_reserved=allocation_reserved))
    gate_settled = bool(
        grants_settled and
        (policy is ReservedSupplyPolicy.STATIC_PREFILL or
         (allocation.utilization_gate_armed and
          allocation.utilization_demonstrated_need is not None and
          allocation.utilization_demand_witness_sha256 is not None)))
    if not gate_settled:
        return (policy, ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED,
                authenticated, {})
    return (policy, ReservedSupplyEvidenceState.AUTHENTICATED_SETTLED,
            authenticated, dict(authenticated))


def _require_demand_causal_allocation(
    config: _ReservedFillServiceConfig,
    allocation: AuthenticatedAllocationMap | None,
    candidate: capacity_planning.CapacityPlanCandidate,
    *,
    positive_target: bool,
) -> None:
    """Fence compatible paid planning behind the exact utilization witness."""
    if candidate.kind is capacity_planning.CapacityPlanKind.GATE_ACQUISITION:
        if positive_target or not config.configured_utilization_gate:
            raise CapacityAdmissionConflict(
                'Gate acquisition carries capacity or has no configured gate.')
        return
    if not positive_target:
        return
    if (candidate.reservation_demand_relation
            is capacity_planning.ReservationDemandRelation.STATICALLY_DISJOINT):
        return
    if (candidate.reservation_demand_relation
            is not capacity_planning.ReservationDemandRelation.COMPATIBLE):
        raise CapacityAdmissionConflict(
            'Positive reservation demand has no typed compatibility proof.')
    if allocation is None:
        raise CapacityAdmissionConflict(
            'Positive reservation-compatible demand has no current reserved '
            'allocation.')
    if not config.configured_utilization_gate:
        return
    current_target = candidate.aggregate_demand_target
    if (not allocation.utilization_gate_armed or
            allocation.utilization_demand_witness_sha256
            != candidate.demand_witness_sha256 or
            allocation.utilization_demonstrated_need is None or
            not allocation.upward_grants_settled or
            allocation.utilization_demonstrated_need < current_target or
            allocation.utilization_ceiling < current_target):
        raise CapacityAdmissionConflict(
            'Current utilization-gated allocation does not causally cover '
            'the locked SLA target.')


def _require_postgres(connection: sqlalchemy.engine.Connection) -> None:
    if connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise CapacityAdmissionUnavailable(
            'Ordered capacity admission requires PostgreSQL.')


def _replica_card(state: Mapping[str, Any]) -> str | None:
    for field in ('location', 'resources_override'):
        resource = state.get(field)
        accelerators = (resource.get('accelerators') if isinstance(
            resource, Mapping) else None)
        if isinstance(accelerators, Mapping) and len(accelerators) == 1:
            raw_card = next(iter(accelerators))
            if isinstance(raw_card, str) and raw_card:
                return raw_card.casefold()
    return None


def _validated_replica_attribution(
    row: Mapping[str, Any],) -> tuple[Mapping[str, Any], int, bool, bool]:
    """Require the normalized ReplicaInfo v18 capacity-attribution core."""
    state = row['replica_state']
    if (row['replica_state_version'] != 1 or not isinstance(state, Mapping) or
            state.get('replica_info_version') != 18):
        raise CapacityAdmissionConflict(
            'Committed replica is not normalized ReplicaInfo v18 state.')
    status = state.get('status_property')
    planned_capacity = state.get('planned_capacity')
    is_zero_cost = state.get('is_zero_cost')
    if (not isinstance(status, Mapping) or
            type(status.get('is_scale_down')) is not bool or
            not isinstance(planned_capacity, int) or
            isinstance(planned_capacity, bool) or planned_capacity < 1 or
            type(is_zero_cost) is not bool):
        raise CapacityAdmissionConflict(
            'Committed replica capacity attribution is malformed.')
    return (state, planned_capacity, is_zero_cost,
            bool(status['is_scale_down']))


def _cleanup_proven_from_locked_row(row: Mapping[str, Any],
                                    state: Mapping[str, Any]) -> bool:
    """Apply the shared relational/JSON cleanup proof to one locked row."""
    try:
        return serve_paid_capacity.paid_replica_cleanup_proven(
            state, sky_down_status_value=row['sky_down_status'])
    except serve_paid_capacity.PaidGPUAttributionError as error:
        raise CapacityAdmissionConflict(
            'Committed replica cleanup attribution is malformed.') from error


@dataclasses.dataclass(frozen=True)
class _LockedCapacityRows:
    """Intent/replica graph locked before its Kueue admission rows."""

    replica_rows: tuple[Mapping[str, Any], ...]
    intent_rows: tuple[Mapping[str, Any], ...]
    live_replica_record_ids: frozenset[tuple[int, uuid.UUID]]
    provider_present_replica_record_ids: frozenset[tuple[int, uuid.UUID]]
    live_intent_keys: frozenset[str]
    planned_capacity_by_intent_key: Mapping[str, int]
    capacity_unit_by_intent_key: Mapping[str, str]
    # Complete same-name nonterminal intent census across service hashes.  The
    # economic projection consumes only ``intent_rows`` for the current
    # incarnation, while recreate fencing must also see a late handler retained
    # by an old incarnation.  DB-constrained terminal history is inert.
    all_service_nonterminal_intent_rows: tuple[Mapping[str, Any], ...] = ()


def _lock_capacity_rows(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_hash: str,
    now: datetime.datetime,
) -> _LockedCapacityRows:
    """Lock the live capacity graph before sorted Kueue admission rows."""
    all_service_nonterminal_intent_rows = connection.execute(
        sqlalchemy.select(_ZERO_COST_INTENTS).where(
            _ZERO_COST_INTENTS.c.service_name == service_name,
            _ZERO_COST_INTENTS.c.state.in_(
                zero_cost_actuation_schema.NONTERMINAL_INTENT_STATES)).order_by(
                    _ZERO_COST_INTENTS.c.intent_idempotency_key).
        with_for_update()).mappings().all()
    for row in all_service_nonterminal_intent_rows:
        row_hash = row.get('service_hash')
        if not isinstance(row_hash, str) or not row_hash:
            raise CapacityAdmissionConflict(
                'Capacity plan retains an unattributed nonterminal intent.')
        if row_hash != service_hash:
            raise CapacityAdmissionConflict(
                'Capacity plan retains a nonterminal intent from a prior '
                'lifecycle.')
    intent_rows = tuple(row for row in all_service_nonterminal_intent_rows
                        if row['service_hash'] == service_hash)
    replica_state_sha256 = replica_state_semantic_sha256_expression(
        _REPLICAS.c.replica_state).label('_replica_state_sha256')
    replica_rows = connection.execute(
        sqlalchemy.select(
            _REPLICAS.c.replica_id, _REPLICAS.c.status, _REPLICAS.c.version,
            _REPLICAS.c.reserved_fill_intent_idempotency_key,
            _REPLICAS.c.ordinary_launch_association_id,
            _REPLICAS.c.paid_capacity_pool_key, _REPLICAS.c.sky_down_status,
            _REPLICAS.c.replica_state_version, _REPLICAS.c.replica_state,
            replica_state_sha256).where(
                _REPLICAS.c.service_name == service_name).order_by(
                    _REPLICAS.c.replica_id).with_for_update()).mappings().all()

    live_replica_record_ids: set[tuple[int, uuid.UUID]] = set()
    provider_present_replica_record_ids: set[tuple[int, uuid.UUID]] = set()
    live_intent_keys: set[str] = set()
    for row in replica_rows:
        state = row['replica_state']
        if row['replica_state_version'] != 1 or not isinstance(state, Mapping):
            continue
        try:
            record_id = uuid.UUID(str(state.get('replica_record_id')))
        except (TypeError, ValueError, AttributeError):
            continue
        record = (int(row['replica_id']), record_id)
        provider_present_replica_record_ids.add(record)
        intent_key = row['reserved_fill_intent_idempotency_key']
        if isinstance(intent_key, str) and intent_key:
            # A retained provider-owned replica can still affect final paid
            # and retirement accounting even after its status turns terminal.
            live_intent_keys.add(intent_key)
        status = state.get('status_property')
        if (row['status'] in _TERMINAL_REPLICA_STATUSES or
                not isinstance(status, Mapping) or
                status.get('is_scale_down') is True):
            continue
        live_replica_record_ids.add(record)

    planned_capacity_by_intent_key: dict[str, int] = {}
    capacity_unit_by_intent_key: dict[str, str] = {}
    for row in intent_rows:
        key = row['intent_idempotency_key']
        planned_capacity = row['planned_capacity']
        capacity_unit = row['capacity_unit']
        if (not isinstance(key, str) or not key or
                not isinstance(planned_capacity, int) or
                isinstance(planned_capacity, bool) or planned_capacity < 1 or
                capacity_unit not in ('physical', 'logical')):
            raise CapacityAdmissionConflict(
                'Zero-cost intent capacity attribution is malformed.')
        planned_capacity_by_intent_key[key] = planned_capacity
        capacity_unit_by_intent_key[key] = capacity_unit
        if (row['state'] == 'COMMITTED' or
            (row['state'] in _PENDING_ZERO_COST_INTENT_STATES and
             row['valid_until'] > now)):
            live_intent_keys.add(key)
    return _LockedCapacityRows(
        replica_rows=tuple(replica_rows),
        intent_rows=tuple(intent_rows),
        live_replica_record_ids=frozenset(live_replica_record_ids),
        provider_present_replica_record_ids=frozenset(
            provider_present_replica_record_ids),
        live_intent_keys=frozenset(live_intent_keys),
        planned_capacity_by_intent_key=planned_capacity_by_intent_key,
        capacity_unit_by_intent_key=capacity_unit_by_intent_key,
        all_service_nonterminal_intent_rows=tuple(
            all_service_nonterminal_intent_rows))


def _locked_planning_state_fingerprint(service: Mapping[str, Any],
                                       locked: _LockedCapacityRows) -> str:
    """Match the controller's semantic fingerprint from locked rows."""
    active_versions = service['active_versions']
    if isinstance(active_versions, str):
        active_versions = json.loads(active_versions) if active_versions else []
    material = {
        'runtime': {
            'hash': service['hash'],
            'status': service['status'],
            'current_version': service['current_version'],
            'controller_pid': service['controller_pid'],
            'controller_ip': service['controller_ip'],
            'controller_incarnation':
                (None if service['controller_incarnation'] is None else str(
                    service['controller_incarnation'])),
            'controller_owner_epoch': service['controller_owner_epoch'],
            'active_versions': active_versions or [],
        },
        'replicas': [(int(row['replica_id']), row['replica_state_version'],
                      row['_replica_state_sha256'])
                     for row in locked.replica_rows],
    }
    encoded = json.dumps(material, sort_keys=True,
                         separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _lock_kueue_projection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_hash: str,
    service_lifecycle_epoch: int,
    service_version: int,
    accounting_cards: set[str],
    locked: _LockedCapacityRows,
) -> kueue_lane_capacity.KueueAdmissionCapacityProjection:
    """Lock and validate the admission rows associated with the graph."""
    try:
        projection = (
            kueue_lane_capacity.lock_capacity_projection_in_connection(
                connection,
                service_name=service_name,
                service_hash=service_hash,
                service_lifecycle_epoch=service_lifecycle_epoch,
                service_version=service_version,
                accounting_cards=accounting_cards,
                locked_intent_rows=locked.intent_rows,
                planned_capacity_by_intent_key=(
                    locked.planned_capacity_by_intent_key),
                capacity_unit_by_intent_key=(
                    locked.capacity_unit_by_intent_key),
                live_replica_record_ids=set(locked.live_replica_record_ids),
                provider_present_replica_record_ids=set(
                    locked.provider_present_replica_record_ids),
                live_intent_keys=set(locked.live_intent_keys)))
    except kueue_lane_capacity.KueueAdmissionCapacityError as error:
        raise CapacityAdmissionConflict(
            'Kueue admission capacity cannot be proven.') from error
    if projection.unbounded_unknown:
        raise CapacityAdmissionConflict(
            'Kueue admission capacity has an unbounded unknown scope.')
    return projection


def _require_recreated_logical_version_is_clean(
    service: Mapping[str, Any],
    config: _ReservedFillServiceConfig,
    locked: _LockedCapacityRows,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
) -> None:
    """Fence a recreated logical service from an older provider graph.

    Test-only logical services intentionally use delete/recreate instead of a
    mixed-version migration.  A same-version controller restart must retain
    its rows, but a new version cannot plan while an older provider-possible
    replica, nonterminal zero-cost intent, or unresolved Kueue admission still
    exists.  This check runs after the complete live/provider-possible graph is
    locked and before the planner callback or any capacity-plan write.
    """
    if (service.get('demand_source_mode') != DemandSourceMode.DURABLE_FEED.value
            or config.capacity_unit
            is not reserved_fill_planner.FillCapacityUnit.LOGICAL):
        return
    current_version = service.get('current_version')
    if type(current_version) is not int or current_version < 1:
        raise CapacityAdmissionConflict(
            'Recreated logical service has no current version.')

    for row in locked.replica_rows:
        version = row.get('version')
        if type(version) is not int or version < 1:
            raise CapacityAdmissionConflict(
                'Recreated logical service has an unattributed replica.')
        if version == current_version:
            continue
        if version > current_version:
            raise CapacityAdmissionConflict(
                'Recreated logical service has a future-version replica.')
        if _replica_service_ceiling_capacity(row, config.capacity_unit) > 0:
            raise CapacityAdmissionConflict(
                'Recreated logical service retains provider-possible '
                'capacity from a prior version.')

    current_hash = service.get('hash')
    current_lifecycle_epoch = service.get('lifecycle_epoch')
    if (not isinstance(current_hash, str) or not current_hash or
            type(current_lifecycle_epoch) is not int or
            current_lifecycle_epoch < 1):
        raise CapacityAdmissionConflict(
            'Recreated logical service has no exact lifecycle identity.')

    for row in locked.all_service_nonterminal_intent_rows:
        row_hash = row.get('service_hash')
        row_lifecycle_epoch = row.get('service_lifecycle_epoch')
        version = row.get('service_version')
        if (not isinstance(row_hash, str) or not row_hash or
                type(row_lifecycle_epoch) is not int or
                row_lifecycle_epoch < 1 or type(version) is not int or
                version < 1):
            raise CapacityAdmissionConflict(
                'Recreated logical service has an unattributed intent.')
        same_incarnation = (row_hash == current_hash and
                            row_lifecycle_epoch == current_lifecycle_epoch)
        if not same_incarnation:
            raise CapacityAdmissionConflict(
                'Recreated logical service retains a nonterminal intent '
                'from a prior lifecycle.')
        if version == current_version:
            continue
        if version > current_version:
            raise CapacityAdmissionConflict(
                'Recreated logical service has a future-version intent.')
        raise CapacityAdmissionConflict(
            'Recreated logical service retains a nonterminal intent from a '
            'prior version.')

    for row in lane_projection.rows:
        version = getattr(row, 'service_version', None)
        if type(version) is not int or version < 1:
            raise CapacityAdmissionConflict(
                'Recreated logical service has an unattributed Kueue '
                'admission.')
        if version == current_version:
            continue
        if version > current_version:
            raise CapacityAdmissionConflict(
                'Recreated logical service has a future-version Kueue '
                'admission.')
        # The admission table has no terminal state. Provider-free terminal
        # lineage is deleted transactionally; a retained row is unresolved.
        raise CapacityAdmissionConflict(
            'Recreated logical service retains an unresolved Kueue '
            'admission from a prior version.')


def _project_capacity_inventory(
    locked: _LockedCapacityRows,
    *,
    service_version: int,
    capacity_unit: FillCapacityUnit,
    accounting_cards: set[str],
    now: datetime.datetime,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
) -> tuple[dict[str, int], dict[str, int], dict[str, int], int]:
    """Project demand supply from one locked replica/admission snapshot."""
    if not isinstance(capacity_unit, reserved_fill_planner.FillCapacityUnit):
        raise CapacityAdmissionConflict(
            'Capacity inventory has no exact accounting unit.')
    if not accounting_cards:
        raise CapacityAdmissionConflict(
            'Capacity plan has no accounting class.')
    aggregate = accounting_cards == {AGGREGATE_ACCELERATOR}
    if AGGREGATE_ACCELERATOR in accounting_cards and not aggregate:
        raise CapacityAdmissionConflict(
            'Capacity plan mixes aggregate and exact-card accounting.')
    zero_cost = {card: 0 for card in accounting_cards}
    paid = {card: 0 for card in accounting_cards}
    charged_paid_gpu_units = 0
    counted_zero_cost_intents: set[str] = set()
    for row in locked.replica_rows:
        terminal = row['status'] in _TERMINAL_REPLICA_STATUSES
        raw_state = row['replica_state']
        if (terminal and isinstance(raw_state, Mapping) and
                raw_state.get('is_zero_cost') is True):
            cleanup_proven = _cleanup_proven_from_locked_row(row, raw_state)
            if cleanup_proven:
                continue
            try:
                serve_paid_capacity.validate_paid_replica_relational_copies(
                    raw_state, pool_key_value=row['paid_capacity_pool_key'])
            except serve_paid_capacity.PaidGPUAttributionError as error:
                raise CapacityAdmissionConflict(
                    'Committed replica pool contradicts zero-cost attribution.'
                ) from error
            # Terminal reserved rows are neither usable nor billable. Keep
            # the prior tolerance for retained legacy reserved tombstones;
            # their provider cleanup is owned by the separate exact fence.
            continue
        state, planned_capacity, is_zero_cost, is_scale_down = (
            _validated_replica_attribution(row))
        cleanup_proven = _cleanup_proven_from_locked_row(row, state)
        if cleanup_proven:
            continue
        try:
            relationally_paid = (
                serve_paid_capacity.validate_paid_replica_relational_copies(
                    state, pool_key_value=row['paid_capacity_pool_key']))
        except serve_paid_capacity.PaidGPUAttributionError as error:
            raise CapacityAdmissionConflict(
                'Committed replica pool contradicts zero-cost attribution.'
            ) from error
        if relationally_paid == is_zero_cost:
            raise CapacityAdmissionConflict(
                'Committed replica capacity class is contradictory.')
        if not is_zero_cost:
            # planned_capacity remains the service's logical/physical target
            # unit. The paid provider pool separately persists the exact
            # per-node shape and num_nodes needed for billing. Charge every
            # service version: an old worker may be unusable for current
            # demand while its provider allocation still exists or bills.
            try:
                charged_paid_gpu_units += (
                    serve_paid_capacity.paid_replica_gpu_units(
                        state, pool_key_value=row['paid_capacity_pool_key']))
            except serve_paid_capacity.PaidGPUAttributionError as error:
                raise CapacityAdmissionConflict(
                    'Committed paid replica physical GPU attribution is '
                    'malformed.') from error
            if row['version'] != service_version:
                continue
        # A controller lifecycle label is not provider-cleanup evidence.  A
        # paid replica can already be SHUTTING_DOWN (or otherwise terminal)
        # while its VM still exists or bills, so retain it in the purchased
        # baseline until the durable down operation succeeds.  Reserved rows
        # stop contributing once terminal because they are no longer usable
        # demand supply; their provider ownership remains protected by the
        # separate cleanup/quiescence protocol.
        if terminal and is_zero_cost:
            continue
        # A reserved row stops contributing once its scheduler capacity is
        # being yielded.  A paid row is different: until its exact claim and
        # cleanup-proven provider allocation disappear it remains both a cost
        # debit and capacity already purchased for residual accounting.  Do
        # not launch a paid replacement merely because retirement began.
        if is_scale_down and is_zero_cost:
            continue
        card = (AGGREGATE_ACCELERATOR if aggregate else _replica_card(state))
        if (is_zero_cost and row['version'] != service_version and
                card not in accounting_cards):
            # A retained old-version reservation on a card removed by the
            # current service catalog is retirement-only inventory. It still
            # participates in the exact cleanup/service-ceiling graph, but it
            # cannot cover current demand and must not make a catalog update
            # impossible.
            continue
        if card not in accounting_cards:
            intent_key = row['reserved_fill_intent_idempotency_key']
            if (is_zero_cost and
                    intent_key in lane_projection.unknown_intent_keys and
                    not lane_projection.unbounded_unknown):
                # A bounded exact-shape UNKNOWN cannot suppress an unrelated
                # paid accounting class.
                continue
            raise CapacityAdmissionConflict(
                'Committed replica is outside the exact-card accounting set.')
        if is_zero_cost:
            intent_key = row['reserved_fill_intent_idempotency_key']
            lane_assigned = lane_projection.assigned_gpu_for_intent(intent_key)
            ordinary_assigned = lane_projection.uses_ordinary_scheduler(
                intent_key)
            if lane_assigned is False:
                continue
            if (row['version'] != service_version and
                    lane_assigned is not True and not ordinary_assigned):
                continue
            zero_cost[card] += planned_capacity
            if ((lane_assigned is True or ordinary_assigned) and
                    isinstance(intent_key, str)):
                counted_zero_cost_intents.add(intent_key)
        else:
            paid[card] += planned_capacity

    pending_zero_cost = {card: 0 for card in accounting_cards}
    accounted_intent_keys = set(counted_zero_cost_intents)
    for row in locked.intent_rows:
        intent_key = row['intent_idempotency_key']
        lane_assigned = lane_projection.assigned_gpu_for_intent(intent_key)
        if lane_assigned is True:
            if intent_key in counted_zero_cost_intents:
                continue
        elif lane_projection.uses_ordinary_scheduler(intent_key):
            if intent_key in counted_zero_cost_intents:
                continue
        elif lane_assigned is False:
            continue
        elif (row['state'] not in _PENDING_ZERO_COST_INTENT_STATES or
              row['valid_until'] <= now or
              row['service_version'] != service_version):
            continue
        card = (AGGREGATE_ACCELERATOR
                if aggregate else str(row['accelerator']).casefold())
        planned_capacity = row['planned_capacity']
        if (card not in pending_zero_cost or
                not isinstance(planned_capacity, int) or
                isinstance(planned_capacity, bool) or planned_capacity < 1):
            if (card not in pending_zero_cost and
                    intent_key in lane_projection.unknown_intent_keys and
                    not lane_projection.unbounded_unknown):
                continue
            raise CapacityAdmissionConflict(
                'Pending zero-cost intent accounting is malformed.')
        pending_zero_cost[card] += planned_capacity
        accounted_intent_keys.add(intent_key)

    # An intent absent from the current live lock (normally TERMINAL) has no
    # usable intent authority. Its retained Kueue row is nevertheless unresolved
    # scheduler authority. That row immutably copied the exact capacity shape at
    # grant time, so use it only as a conservative debit when the lane projection
    # has already classified the missing intent as UNKNOWN. It never becomes
    # demand supply or admission authority, and no Kueue -> intent lock inversion
    # is needed.
    for lane_row in lane_projection.rows:
        intent_key = lane_row.intent_idempotency_key
        if (intent_key in accounted_intent_keys or
                intent_key not in lane_projection.unknown_intent_keys or
                intent_key not in lane_projection.assigned_gpu_intent_keys):
            continue
        raw_accelerator = lane_row.accelerator
        planned_capacity = lane_row.planned_capacity
        accelerator_count = lane_row.accelerator_count
        row_capacity_unit = lane_row.capacity_unit
        if (not isinstance(raw_accelerator, str) or not raw_accelerator or
                row_capacity_unit not in ('physical', 'logical') or
                type(accelerator_count) is not int or accelerator_count < 1 or
                type(planned_capacity) is not int or planned_capacity < 1):
            raise CapacityAdmissionConflict(
                'Retained Kueue capacity debit is malformed.')
        expected_capacity = (1 if row_capacity_unit == 'physical' else
                             accelerator_count)
        if (row_capacity_unit != capacity_unit.value or
                planned_capacity != expected_capacity):
            raise CapacityAdmissionConflict(
                'Retained Kueue capacity debit is malformed.')
        accelerator = raw_accelerator.casefold()
        card = AGGREGATE_ACCELERATOR if aggregate else accelerator
        if card not in pending_zero_cost:
            if not lane_projection.unbounded_unknown:
                # A bounded exact-shape UNKNOWN cannot suppress unrelated paid
                # demand.
                continue
            raise CapacityAdmissionConflict(
                'Retained Kueue capacity debit is outside the accounting set.')
        pending_zero_cost[card] += planned_capacity
        accounted_intent_keys.add(intent_key)
    return zero_cost, paid, pending_zero_cost, charged_paid_gpu_units


def _economic_capacity_graph_snapshot(
    locked: _LockedCapacityRows,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
    *,
    service_version: int,
) -> tuple[tuple[Any, ...], kueue_lane_capacity.KueueReplicaCapacitySnapshot,
           str]:
    """Decode supply and fingerprint only reserved-side economic semantics.

    Paid replicas are deliberately absent from the fingerprint.  Publishing a
    positive plan is followed by inserting its own paid replica row, so a
    digest of the complete replica table would revoke the authority it just
    minted.  Paid baseline plus exact claim delta is checked separately at
    provider start.  This digest instead freezes every durable zero-cost fact
    consumed by compatible-card placement: immutable replica identity and
    width, current/historical contribution class, intent edges, and Kueue
    assignment semantics.  Volatile JSON fields and timestamps are omitted.
    """
    replica_infos: list[Any] = []
    replica_rows_by_id: dict[int, Mapping[str, Any]] = {}
    for row in locked.replica_rows:
        state = row['replica_state']
        if not isinstance(state, dict):
            raise CapacityAdmissionConflict(
                'Economic capacity graph contains malformed replica state.')
        try:
            info = serve_state.decode_replica_state_for_authority(
                int(row['replica_state_version']), state)
        except (RuntimeError, TypeError, ValueError) as error:
            raise CapacityAdmissionConflict(
                'Economic capacity graph cannot decode a replica row.') from (
                    error)
        if (info.replica_id != row['replica_id'] or
                info.version != row['version'] or
                info.status.value != row['status']):
            raise CapacityAdmissionConflict(
                'Economic capacity graph replica columns disagree.')
        replica_infos.append(info)
        replica_rows_by_id[int(row['replica_id'])] = row
    capacity_snapshot = (
        kueue_lane_capacity.replica_capacity_snapshot_from_projection(
            tuple(replica_infos), lane_projection))
    classes = dict(capacity_snapshot.by_replica_id)

    reserved_replicas: list[dict[str, Any]] = []
    for info in replica_infos:
        row = replica_rows_by_id[int(info.replica_id)]
        state, planned_capacity, is_zero_cost, is_scale_down = (
            _validated_replica_attribution(row))
        if not is_zero_cost:
            continue
        try:
            record_id = str(uuid.UUID(str(info.replica_record_id)))
        except (TypeError, ValueError, AttributeError) as error:
            raise CapacityAdmissionConflict(
                'Reserved economic replica has no exact record identity.'
            ) from error
        intent_key = row['reserved_fill_intent_idempotency_key']
        if intent_key is not None and (not isinstance(intent_key, str) or
                                       not intent_key):
            raise CapacityAdmissionConflict(
                'Reserved economic replica has a malformed intent edge.')
        admission_class = classes.get(int(info.replica_id))
        ordinary_scheduler = lane_projection.uses_ordinary_scheduler(intent_key)
        kueue_assigned = (
            admission_class
            is not kueue_lane_capacity.KueueReplicaCapacityClass.FRESH_WAITING)
        terminal = row['status'] in _TERMINAL_REPLICA_STATUSES
        historical = int(row['version']) != service_version
        contributes = (not terminal and not is_scale_down and kueue_assigned and
                       (not historical or admission_class is not None or
                        ordinary_scheduler))
        reserved_replicas.append({
            'replica_id': int(row['replica_id']),
            'replica_record_id': record_id,
            'service_version': int(row['version']),
            'historical': historical,
            'intent_idempotency_key': intent_key,
            'accelerator': _replica_card(state),
            'planned_capacity': planned_capacity,
            'lifecycle':
                ('terminal' if terminal else 'retiring' if is_scale_down else
                 'ready' if info.is_ready else 'provisioning'),
            'kueue_capacity_class':
                (None if admission_class is None else admission_class.value),
            'ordinary_scheduler': ordinary_scheduler,
            'economic_contribution': (planned_capacity if contributes else 0),
        })

    intents: list[dict[str, Any]] = []
    for row in locked.intent_rows:
        state = str(row['state'])
        live = (state == 'COMMITTED' or
                (state in _PENDING_ZERO_COST_INTENT_STATES and
                 row['valid_until'] > lane_projection.now))
        intents.append({
            'intent_idempotency_key': str(row['intent_idempotency_key']),
            'service_version': int(row['service_version']),
            'state': state,
            'live': live,
            'pool_key': str(row['pool_key']),
            'accelerator': str(row['accelerator']).casefold(),
            'accelerator_count': int(row['accelerator_count']),
            'capacity_unit': str(row['capacity_unit']),
            'planned_capacity': int(row['planned_capacity']),
            'allocation_generation': int(row['allocation_generation']),
            'allocation_input_sha256': str(row['allocation_input_sha256']),
            'allocation_claim_generation': int(
                row['allocation_claim_generation']),
            'reconciliation_gate_generation': int(
                row['reconciliation_gate_generation']),
            'reclaim_fleet_bundle_sha256': str(
                row['reclaim_fleet_bundle_sha256']),
            'reclaim_policy_revision': str(row['reclaim_policy_revision']),
            'reclaim_provider_inventory_sha256': str(
                row['reclaim_provider_inventory_sha256']),
            'replica_id':
                (None if row['replica_id'] is None else int(row['replica_id'])),
            'replica_record_id': (None if row['replica_record_id'] is None else
                                  str(row['replica_record_id'])),
        })

    admissions: list[dict[str, Any]] = []
    for row in lane_projection.rows:
        intent_key = str(row.intent_idempotency_key)
        if intent_key in lane_projection.fresh_waiting_intent_keys:
            capacity_class = 'FRESH_WAITING'
        elif intent_key in lane_projection.admitted_intent_keys:
            capacity_class = 'POLICY_ADMITTED'
        elif intent_key in lane_projection.unknown_intent_keys:
            capacity_class = 'UNKNOWN'
        else:
            capacity_class = 'INTENT_PENDING'
        admissions.append({
            'intent_idempotency_key': intent_key,
            'service_version': int(row.service_version),
            'pool_key': str(row.pool_key),
            'accelerator': str(row.accelerator).casefold(),
            'accelerator_count': int(row.accelerator_count),
            'capacity_unit': str(row.capacity_unit),
            'planned_capacity': int(row.planned_capacity),
            'capacity_class': capacity_class,
            'assigned_gpu': (intent_key
                             in lane_projection.assigned_gpu_intent_keys),
            'demand_supply': (intent_key
                              in lane_projection.demand_supply_intent_keys),
            'replica_id':
                (None if row.replica_id is None else int(row.replica_id)),
            'replica_record_id': (None if row.replica_record_id is None else
                                  str(row.replica_record_id)),
            'replacement_surge_units': int(row.replacement_surge_units),
        })
    digest = _sha256({
        'protocol': 'reserved-economic-supply-v1',
        'service_version': service_version,
        'reserved_replicas': reserved_replicas,
        'zero_cost_intents': intents,
        'kueue_admissions': admissions,
        'ordinary_scheduler_intent_keys': sorted(
            lane_projection.ordinary_scheduler_intent_keys),
        'unknown_shapes': sorted(
            [list(shape) for shape in lane_projection.unknown_shapes]),
        'unbounded_unknown': lane_projection.unbounded_unknown,
    })
    return tuple(replica_infos), capacity_snapshot, digest


def _replica_service_ceiling_capacity(
    row: Mapping[str, Any],
    capacity_unit: FillCapacityUnit,
) -> int:
    """Project one cleanup-unproven row into the service's configured unit."""
    state = row['replica_state']
    if (isinstance(state, Mapping) and
            _cleanup_proven_from_locked_row(row, state)):
        return 0
    try:
        return zero_cost_actuation.replica_capacity_for_unit(
            row['replica_state_version'], state, capacity_unit)
    except zero_cost_actuation.ZeroCostActuationConflict as error:
        raise CapacityAdmissionConflict(
            'Service ceiling contains malformed replica capacity.') from error


def _project_allocation_reserved_capacity(
    allocation: AuthenticatedAllocationMap | None,
    locked: _LockedCapacityRows,
    *,
    service_hash: str,
    service_version: int,
    accounting_cards: set[str],
    now: datetime.datetime,
    config: _ReservedFillServiceConfig,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
) -> dict[str, int]:
    """Project the locked current-allocation tail without double counting."""
    zero = {card: 0 for card in accounting_cards}
    if allocation is None or not allocation.pool_snapshots:
        return zero
    if not isinstance(allocation,
                      reserved_fill_planner.AuthenticatedAllocationMap):
        raise CapacityAdmissionConflict(
            'Current reserved-fill allocation has an invalid type.')
    if allocation.service_version != service_version:
        raise CapacityAdmissionConflict(
            'Current reserved-fill allocation names another service version.')

    provider_replica_keys: set[str] = set()
    materialized_capacity = 0
    for row in locked.replica_rows:
        capacity = _replica_service_ceiling_capacity(row, config.capacity_unit)
        materialized_capacity += capacity
        key = row['reserved_fill_intent_idempotency_key']
        if capacity > 0 and isinstance(key, str) and key:
            provider_replica_keys.add(key)

    pending_capacity = 0
    debit_counts: dict[tuple[str, str], int] = {}
    admission_keys = {
        str(row.intent_idempotency_key) for row in lane_projection.rows
    }
    represented_replica_keys = {
        str(row['reserved_fill_intent_idempotency_key'])
        for row in locked.replica_rows
        if isinstance(row['reserved_fill_intent_idempotency_key'], str) and
        row['reserved_fill_intent_idempotency_key']
    }
    allocation_identity = (
        allocation.allocation_generation,
        allocation.allocation_input_sha256,
        allocation.allocation_claim_generation,
        allocation.reconciliation_gate_generation,
        allocation.reclaim_fleet_bundle_sha256,
        allocation.reclaim_policy_revision,
        allocation.reclaim_provider_inventory_sha256,
    )
    for row in locked.intent_rows:
        state = row['state']
        live_pending = (state in _PENDING_ZERO_COST_INTENT_STATES and
                        row['valid_until'] > now)
        if (live_pending and
                row['intent_idempotency_key'] not in admission_keys):
            try:
                pending_capacity += zero_cost_actuation.intent_capacity_for_unit(
                    row, config.capacity_unit)
            except zero_cost_actuation.ZeroCostActuationConflict as error:
                raise CapacityAdmissionConflict(
                    'Service ceiling contains malformed intent capacity.'
                ) from error
        row_identity = (row['allocation_generation'],
                        row['allocation_input_sha256'],
                        row['allocation_claim_generation'],
                        row['reconciliation_gate_generation'],
                        row['reclaim_fleet_bundle_sha256'],
                        row['reclaim_policy_revision'],
                        row['reclaim_provider_inventory_sha256'])
        current_materialized = (state == 'COMMITTED' and
                                row['intent_idempotency_key']
                                in provider_replica_keys)
        current_unrepresented_admission = (
            row['intent_idempotency_key'] in admission_keys and
            row['intent_idempotency_key'] not in represented_replica_keys)
        if (row['service_hash'] != service_hash or
                row['service_version'] != service_version or
                row_identity != allocation_identity or
                not (live_pending or current_materialized or
                     current_unrepresented_admission)):
            continue
        key = (str(row['pool_key']), str(row['accelerator']).casefold())
        debit_counts[key] = debit_counts.get(key, 0) + 1

    try:
        unresolved_admission_capacity = (
            zero_cost_actuation.unrepresented_kueue_admission_capacity(
                lane_projection.rows,
                capacity_unit=config.capacity_unit,
                represented_intent_keys=frozenset(represented_replica_keys)))
    except zero_cost_actuation.ZeroCostActuationConflict as error:
        raise CapacityAdmissionConflict(
            'Kueue admission service ceiling debit is malformed.') from error

    debits = tuple(
        reserved_fill_planner.CommittedFillDebit(
            allocation_generation=allocation.allocation_generation,
            allocation_input_sha256=allocation.allocation_input_sha256,
            allocation_claim_generation=(
                allocation.allocation_claim_generation),
            pool_key=pool_key,
            accelerator=accelerator,
            replica_slots=count)
        for (pool_key, accelerator), count in sorted(debit_counts.items()))
    planned_capacity = (materialized_capacity + pending_capacity +
                        unresolved_admission_capacity)
    try:
        exact = (
            reserved_fill_planner.ReservedFillPlanner.
            project_remaining_capacity_by_accelerator(
                allocation_map=allocation,
                # Use the real locked service ceiling here.  The
                # canonical planner admits only whole backends that fit
                # the remaining headroom and skips a wider card without
                # globally suppressing independent cards.
                max_replicas=config.max_capacity,
                planned_replicas=planned_capacity,
                capacity_unit=config.capacity_unit,
                committed_fill_debits=debits))
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Current allocation tail cannot be projected exactly.') from error
    if accounting_cards == {AGGREGATE_ACCELERATOR}:
        return {AGGREGATE_ACCELERATOR: sum(exact.values())}
    if set(exact) - accounting_cards:
        raise CapacityAdmissionConflict(
            'Current allocation tail is outside the plan accounting classes.')
    return {card: exact.get(card, 0) for card in sorted(accounting_cards)}


@dataclasses.dataclass(frozen=True)
class _PlanClaimProjection:
    units_by_accelerator: Mapping[str, int]
    physical_gpu_units: int


def _planner_bound_pool_shape(
    pool_key: Any,
    candidate: capacity_planning.CapacityPlanCandidate,
    debit_accelerator: str,
) -> serve_paid_capacity.PhysicalBackendShape:
    """Decode and bind one relational pool to immutable planner shape."""
    try:
        pool_shape = serve_paid_capacity.paid_pool_gpu_shape(pool_key)
        pool_card = pool_shape.accelerator
        if pool_card is None:
            raise serve_paid_capacity.PaidGPUAttributionError(
                'Planner-bound claims require an exact GPU accelerator.')
        expected_card = (pool_card if debit_accelerator == AGGREGATE_ACCELERATOR
                         else debit_accelerator.casefold())
        expected_shape = serve_paid_capacity.PhysicalBackendShape(
            accelerator=expected_card,
            gpu_units_per_node=(
                candidate.physical_gpu_width_by_accelerator.get(expected_card)),
            num_nodes=candidate.backend_num_nodes)
    except serve_paid_capacity.PaidGPUAttributionError as error:
        raise CapacityAdmissionConflict(
            'Planner-bound paid claim provider pool is malformed.') from error
    if pool_shape != expected_shape:
        raise CapacityAdmissionConflict(
            'Paid claim provider pool contradicts its immutable backend shape.')
    return pool_shape


def _plan_claim_projection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_hash: str,
    generation: int,
    accounting_cards: set[str],
    capacity_unit: capacity_planning.CapacityUnit | None,
    candidate: capacity_planning.CapacityPlanCandidate | None = None,
) -> _PlanClaimProjection:
    """Read one locked typed claim projection for units and physical debit."""
    units = {card: 0 for card in accounting_cards}
    rows = connection.execute(
        sqlalchemy.select(_CLAIMS.c.capacity_plan_accelerator,
                          _CLAIMS.c.capacity_plan_units,
                          _CLAIMS.c.pool_key).where(
                              _CLAIMS.c.service_name == service_name,
                              _CLAIMS.c.service_hash == service_hash,
                              _CLAIMS.c.capacity_plan_generation ==
                              generation).with_for_update()).mappings().all()
    physical_gpu_units = 0
    for row in rows:
        card = row['capacity_plan_accelerator']
        count = row['capacity_plan_units']
        if (card not in units or not isinstance(count, int) or
                isinstance(count, bool) or count < 1):
            raise CapacityAdmissionConflict(
                'Planner-bound claim accounting is malformed.')
        units[card] += count
        if capacity_unit in (capacity_planning.CapacityUnit.PHYSICAL_BACKEND,
                             capacity_planning.CapacityUnit.LOGICAL_GPU):
            try:
                if candidate is None:
                    pool_shape = serve_paid_capacity.paid_pool_gpu_shape(
                        row['pool_key'])
                    exact_card_matches = (card == AGGREGATE_ACCELERATOR or
                                          pool_shape.accelerator
                                          == card.casefold())
                else:
                    pool_shape = _planner_bound_pool_shape(
                        row['pool_key'], candidate, card)
                    exact_card_matches = True
            except (serve_paid_capacity.PaidGPUAttributionError,
                    CapacityAdmissionConflict) as error:
                raise CapacityAdmissionConflict(
                    'Planner-bound claim attribution is malformed.') from error
            if not exact_card_matches:
                raise CapacityAdmissionConflict(
                    'Planner-bound claim attribution is malformed.')
            if (capacity_unit is capacity_planning.CapacityUnit.PHYSICAL_BACKEND
                    and count != 1):
                raise CapacityAdmissionConflict(
                    'Physical-backend claim attribution is malformed.')
            if (capacity_unit is capacity_planning.CapacityUnit.LOGICAL_GPU and
                (pool_shape.num_nodes != 1 or
                 count != pool_shape.gpu_units_per_node)):
                raise CapacityAdmissionConflict(
                    'Logical-GPU claim attribution is malformed.')
            physical_gpu_units += pool_shape.total_gpu_units
    return _PlanClaimProjection(units_by_accelerator=units,
                                physical_gpu_units=physical_gpu_units)


def _claim_units_for_plan(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_hash: str,
    generation: int,
    accounting_cards: set[str],
) -> dict[str, int]:
    return dict(
        _plan_claim_projection(connection,
                               service_name=service_name,
                               service_hash=service_hash,
                               generation=generation,
                               accounting_cards=accounting_cards,
                               capacity_unit=None).units_by_accelerator)


def _subtract_counts(total: Mapping[str, int],
                     debit: Mapping[str, int]) -> dict[str, int]:
    result = {}
    for card in set(total) | set(debit):
        remaining = total.get(card, 0) - debit.get(card, 0)
        if remaining < 0:
            raise CapacityAdmissionConflict(
                'Planner claims exceed committed paid capacity.')
        result[card] = remaining
    return dict(sorted(result.items()))


@dataclasses.dataclass(frozen=True)
class _LockedPlanHistory:
    """Capacity head and referenced plan locked before paid pool rows."""

    head: Mapping[str, Any] | None
    previous: Mapping[str, Any] | None
    maximum_generation: int | None


@dataclasses.dataclass(frozen=True)
class _WrittenCapacityPlan:
    row: Mapping[str, Any]
    valid_until: datetime.datetime


@dataclasses.dataclass(frozen=True)
class _ValidatedDemandSources:
    """One locked demand graph and its exact positive-authority lease."""

    plan: CapacityPlanInput
    valid_until: datetime.datetime


def _lock_plan_history_in_connection(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> _LockedPlanHistory:
    """Lock the head and its one referenced immutable plan exactly once."""
    head = connection.execute(
        sqlalchemy.select(_HEADS).where(_HEADS.c.service_name == service_name).
        with_for_update()).mappings().one_or_none()
    previous = None
    if head is not None:
        previous = connection.execute(
            sqlalchemy.select(_PLANS).where(
                _PLANS.c.service_name == service_name, _PLANS.c.generation ==
                head['generation']).with_for_update()).mappings().one_or_none()
        if previous is None:
            raise CapacityAdmissionConflict(
                'Capacity-plan head has no referenced immutable plan.')
    maximum = connection.execute(
        sqlalchemy.select(sqlalchemy.func.max(_PLANS.c.generation)).where(
            _PLANS.c.service_name == service_name)).scalar_one()
    maximum_generation = None if maximum is None else int(maximum)
    if (head is not None and maximum_generation is not None and
            int(head['generation']) > maximum_generation):
        raise CapacityAdmissionConflict('Capacity-plan history is malformed.')
    return _LockedPlanHistory(head=head,
                              previous=previous,
                              maximum_generation=maximum_generation)


def _validate_optimistic_capacity_projection(
    raw_expected: Mapping[str, int],
    current: Mapping[str, int],
    accounting_cards: set[str],
    *,
    field: str,
    changed: str,
) -> None:
    """Compare one controller read with the locked publication snapshot."""
    if not raw_expected:
        raw_expected = {card: 0 for card in accounting_cards}
    try:
        expected = _canonical_counts(raw_expected, field)
    except ValueError as error:
        raise CapacityAdmissionConflict(
            f'Controller {field} projection is malformed.') from error
    if set(expected) != accounting_cards or expected != current:
        raise CapacityAdmissionConflict(changed)


_PLAN_DERIVED_DEMAND_FIELDS = frozenset({
    'autoscaler_target',
    'demand_target_by_accelerator',
    'replica_unit',
})


def _deadline_demand_from_snapshot(
    snapshot: demand_state.DurableAutoscalingSnapshot,
) -> tuple[autoscaler_compatibility.DeadlineDemand, ...] | None:
    """Decode the complete current deadline gauge without planning it."""
    normalized = snapshot.normalized_demand
    raw = snapshot.request_information.get('queued_request_deadline_buckets')
    queue_depth = normalized.get('queue_depth')
    if (type(queue_depth) is not int or queue_depth < 0 or
            normalized.get('queue_deadline_profiles_complete') is not True or
            not isinstance(raw, list)):
        return None
    try:
        demand = tuple(
            autoscaler_compatibility.DeadlineDemand(
                sequence=sequence,
                priority=int(profile['priority']),
                compatible_cards=tuple(profile['compatible_accelerators']),
                count=int(profile['count']),
                remaining_seconds=float(profile['remaining_seconds']))
            for sequence, profile in enumerate(raw))
    except (KeyError, TypeError, ValueError):
        return None
    if sum(item.count for item in demand) != queue_depth:
        return None
    return demand


def _deadline_demand_is_monotonic_tightening(
    prior: tuple[autoscaler_compatibility.DeadlineDemand, ...],
    current: tuple[autoscaler_compatibility.DeadlineDemand, ...],
) -> bool:
    """Return whether every current deadline cohort only became tighter.

    This is a relation between observations, not a capacity calculation. It
    permits an older production plan to remain a free-capacity lower bound;
    only the production planner may derive a new exact target.
    """

    def _group(
        demand: tuple[autoscaler_compatibility.DeadlineDemand, ...],
    ) -> dict[tuple[int, tuple[str, ...]], list[tuple[float, int]]]:
        grouped: dict[tuple[int, tuple[str, ...]], dict[float, int]] = {}
        for item in demand:
            if item.count <= 0:
                continue
            key = item.priority, tuple(
                card.casefold() for card in item.compatible_cards)
            by_deadline = grouped.setdefault(key, {})
            by_deadline[item.remaining_seconds] = (
                by_deadline.get(item.remaining_seconds, 0) + item.count)
        return {
            key: sorted(by_deadline.items())
            for key, by_deadline in grouped.items()
        }

    prior_groups = _group(prior)
    current_groups = _group(current)
    if set(prior_groups) != set(current_groups):
        return False
    for key, prior_deadlines in prior_groups.items():
        current_deadlines = current_groups[key]
        if (sum(count for _, count in prior_deadlines)
                != sum(count for _, count in current_deadlines)):
            return False
        prior_index = current_index = 0
        prior_remaining = prior_deadlines[0][1]
        current_remaining = current_deadlines[0][1]
        while prior_index < len(prior_deadlines):
            prior_deadline = prior_deadlines[prior_index][0]
            current_deadline = current_deadlines[current_index][0]
            if current_deadline > prior_deadline:
                return False
            consumed = min(prior_remaining, current_remaining)
            prior_remaining -= consumed
            current_remaining -= consumed
            if prior_remaining == 0:
                prior_index += 1
                if prior_index < len(prior_deadlines):
                    prior_remaining = prior_deadlines[prior_index][1]
            if current_remaining == 0:
                current_index += 1
                if current_index < len(current_deadlines):
                    current_remaining = current_deadlines[current_index][1]
        if current_index != len(current_deadlines):
            return False
    return True


def _changed_demand_semantics(expected: Any,
                              current: Mapping[str, Any]) -> list[str]:
    """Return reporter-owned demand fields that differ from a new snapshot.

    Capacity plans append autoscaler-derived explanation fields to the
    reporter snapshot. Those fields are not heartbeat semantics; every other
    field is compared with exact key presence as well as exact value.
    """
    if not isinstance(expected, Mapping):
        return ['unavailable']
    expected = {
        key: value
        for key, value in expected.items()
        if key not in _PLAN_DERIVED_DEMAND_FIELDS
    }
    current = {
        key: value
        for key, value in current.items()
        if key not in _PLAN_DERIVED_DEMAND_FIELDS
    }
    keys = set(expected) | set(current)
    return [
        str(key) for key in sorted(
            keys, key=repr) if key not in expected or key not in current or
        _sha256({'value': expected[key]}) != _sha256({'value': current[key]})
    ]


def get_service_source_mode(
        service_name: str) -> tuple[DemandSourceMode, int] | None:
    """Read the current per-service demand owner without provider access."""
    engine = serve_state_schema.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return None
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(
                    _SERVICES.c.demand_source_mode,
                    _SERVICES.c.demand_source_epoch).where(
                        _SERVICES.c.name == service_name)).one_or_none()
    except sqlalchemy.exc.SQLAlchemyError:
        return None
    if row is None:
        return None
    try:
        return DemandSourceMode(str(row[0])), int(row[1])
    except (TypeError, ValueError):
        return None


@dataclasses.dataclass(frozen=True)
class _PreparedPaidAdmission:
    launch_spec: serve_paid_capacity.PaidLaunchSpec
    persistence_spec: serve_paid_capacity.PaidClaimPersistenceSpec
    plan_units: int
    physical_gpu_units: int


def _retained_request_root_state(
    request: Mapping[str, Any],
    association: Mapping[str, Any],
) -> _RetainedRequestRootState:
    """Classify one request/association root without reading mutable state.

    A retained terminal API request is audit evidence, not live launch
    authority, only when it copies the association's immutable identity and
    terminal receipt exactly and proves completion of the same execution
    generation.  This is deliberately stricter than request-store retention
    safety: a linked resource action remains blocking even if its attempt is
    settled.  Queue, pin, Kueue, and replica references are deliberately
    outside this pure classifier and remain independent blockers.
    """
    if not isinstance(request, Mapping) or not isinstance(association, Mapping):
        return _RetainedRequestRootState.MALFORMED
    try:
        request_id = request['request_id']
        association_request_id = association['request_id']
        request_association_id = request['ordinary_launch_association_id']
        association_id = association['association_id']
    except KeyError:
        return _RetainedRequestRootState.MALFORMED
    if (not isinstance(request_id, str) or not request_id or
            not isinstance(association_request_id, str) or
            not association_request_id or
            not isinstance(request_association_id, uuid.UUID) or
            not isinstance(association_id, uuid.UUID) or
            request_id != association_request_id or
            request_association_id != association_id):
        return _RetainedRequestRootState.MALFORMED

    association_profile = tuple(
        association.get(field) for field in _BOUND_REQUEST_PROFILE_FIELDS)
    request_profile = tuple(
        request.get(field) for field in _BOUND_REQUEST_PROFILE_FIELDS)
    if all(value is None for value in association_profile):
        # Protocol-v1 history has no immutable generic profile to compare.
        return _RetainedRequestRootState.BLOCKING
    if not all(value is not None for value in association_profile):
        return _RetainedRequestRootState.MALFORMED
    if (request.get('handler_name') != _BOUND_NON_POOL_LAUNCH_HANDLER or
            request_profile != association_profile):
        return _RetainedRequestRootState.MALFORMED

    status = request.get('status')
    if not isinstance(status, str):
        return _RetainedRequestRootState.MALFORMED
    if status not in _TERMINAL_REQUEST_STATUSES:
        return _RetainedRequestRootState.BLOCKING
    terminal_status = association.get('terminal_status')
    terminal_cause = request.get('terminal_cause')
    association_terminal_cause = association.get('terminal_cause')
    execution_generation = request.get('execution_generation')
    association_generation = association.get('terminal_execution_generation')
    try:
        canonical_cause = event_api_models.EventCause(terminal_cause)
        canonical_association_cause = event_api_models.EventCause(
            association_terminal_cause)
    except (TypeError, ValueError):
        return _RetainedRequestRootState.MALFORMED
    if (terminal_status != status or
            canonical_cause is not canonical_association_cause or
            type(execution_generation) is not int or execution_generation < 0 or
            type(association_generation) is not int or
            association_generation != execution_generation):
        return _RetainedRequestRootState.MALFORMED

    finished_at = request.get('finished_at')
    if finished_at is None:
        return _RetainedRequestRootState.BLOCKING
    if not isinstance(finished_at, datetime.datetime):
        return _RetainedRequestRootState.MALFORMED
    if (request.get('resource_action_id') is not None or
            request.get('resource_action_attempt') is not None):
        return _RetainedRequestRootState.BLOCKING
    if (request.get('execution_quiescence_required') is not True or
            association.get('execution_quiescence_required') is not True):
        return _RetainedRequestRootState.BLOCKING
    quiesced_generation = request.get('execution_quiesced_generation')
    quiesced_at = request.get('execution_quiesced_at')
    association_quiesced_generation = association.get(
        'execution_quiesced_generation')
    association_quiesced_at = association.get('execution_quiesced_at')
    if quiesced_generation is None or quiesced_at is None:
        return _RetainedRequestRootState.BLOCKING
    if (type(quiesced_generation) is not int or
            quiesced_generation != execution_generation or
            association_quiesced_generation != quiesced_generation or
            not isinstance(quiesced_at, datetime.datetime) or
            not isinstance(association_quiesced_at, datetime.datetime) or
            association_quiesced_at != quiesced_at):
        return _RetainedRequestRootState.MALFORMED
    return _RetainedRequestRootState.CLOSED_QUIESCED


def _inert_recreated_service_association(
    association: Mapping[str, Any],
    *,
    replica_records: set[tuple[int, str]],
    retained_association_ids: set[uuid.UUID],
    retained_request_ids: set[str],
) -> bool:
    """Return whether settled old-incarnation history is fully detached."""
    try:
        association_id = association['association_id']
        replica_record = (int(association['replica_id']),
                          str(association['replica_record_id']))
        request_id = str(association['request_id'])
    except (KeyError, TypeError, ValueError):
        return False
    return bool(
        isinstance(association_id, uuid.UUID) and
        replica_record not in replica_records and
        association_id not in retained_association_ids and
        request_id not in retained_request_ids and
        ordinary_launch_binding.settled_association_proves_execution_quiescence(
            association))


@dataclasses.dataclass(frozen=True)
class _LockedAssociationAuthorityGraph:
    """Bounded association authority locked for one planning transaction."""

    association_rows: tuple[Mapping[str, Any], ...]
    active_association_rows: tuple[Mapping[str, Any], ...]
    request_rows: tuple[Mapping[str, Any], ...]
    queue_rows: tuple[Mapping[str, Any], ...]
    pin_rows: tuple[Mapping[str, Any], ...]
    blocking_request_count: int


def _lock_association_authority_graph(
    connection: sqlalchemy.engine.Connection,
    *,
    ordinary_associations: sqlalchemy.Table,
    request_rows_table: sqlalchemy.Table,
    request_queue_table: sqlalchemy.Table,
    request_pins_table: sqlalchemy.Table,
    service_name: str,
    service_hash: str,
    exhaustive_history_census: bool,
    prepared_specs: Sequence[serve_paid_capacity.PaidLaunchSpec],
    locked_capacity: _LockedCapacityRows,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
) -> _LockedAssociationAuthorityGraph:
    """Lock either genesis history or the bounded live authority frontier.

    Format-6 genesis performs an exhaustive service-wide census.  Each strict-
    valid current head is then an inductive receipt: every successor validated
    its predecessor under the service lock back to that genesis census.
    Association resolution is monotonic and supported writers serialize on the
    already-held service row, so later ticks need only lock unresolved rows,
    current attachment pointers, and exact prepared-candidate collisions.
    """
    replica_records = {(replica_id, str(record_id)) for replica_id, record_id in
                       locked_capacity.provider_present_replica_record_ids}
    live_record_ids = {
        record_id
        for _, record_id in locked_capacity.provider_present_replica_record_ids
    }
    pointed_association_identities: dict[uuid.UUID, tuple[int, uuid.UUID]] = {}

    def _remember_pointer(association_id: uuid.UUID, replica_id: int,
                          replica_record_id: uuid.UUID) -> None:
        expected = (replica_id, replica_record_id)
        previous = pointed_association_identities.setdefault(
            association_id, expected)
        if previous != expected:
            raise CapacityAdmissionConflict(
                'Locked association pointers carry conflicting replica '
                'identities.')

    for replica_row in locked_capacity.replica_rows:
        pointer = replica_row.get('ordinary_launch_association_id')
        if isinstance(pointer, uuid.UUID):
            state = replica_row.get('replica_state')
            if not isinstance(state, Mapping):
                raise CapacityAdmissionConflict(
                    'Locked replica association pointer has no record identity.'
                )
            try:
                record_id = uuid.UUID(str(state['replica_record_id']))
            except (KeyError, TypeError, ValueError, AttributeError) as error:
                raise CapacityAdmissionConflict(
                    'Locked replica association pointer has no record identity.'
                ) from error
            _remember_pointer(pointer, int(replica_row['replica_id']),
                              record_id)
    for lane_row in lane_projection.rows:
        pointer = lane_row.association_id
        if pointer is None:
            continue
        if (not isinstance(pointer, uuid.UUID) or
                type(lane_row.replica_id) is not int or
                not isinstance(lane_row.replica_record_id, uuid.UUID)):
            raise CapacityAdmissionConflict(
                'Locked Kueue association pointer has no replica identity.')
        _remember_pointer(pointer, lane_row.replica_id,
                          lane_row.replica_record_id)
    pointed_association_ids = set(pointed_association_identities)

    if exhaustive_history_census:
        association_statement = sqlalchemy.select(ordinary_associations).where(
            ordinary_associations.c.service_name == service_name)
    else:
        # Keep every selector independently indexable.  In particular, the
        # unresolved selector uses uq_serve_ordinary_binding_unsettled instead
        # of forcing PostgreSQL through retained settled tombstones.
        scope_selects = [
            sqlalchemy.select(ordinary_associations.c.association_id).where(
                ordinary_associations.c.service_name == service_name,
                ordinary_associations.c.resolution.in_(
                    tuple(value.value for value in
                          ordinary_launch_binding.UNSETTLED_RESOLUTIONS)))
        ]
        if pointed_association_ids:
            scope_selects.append(
                sqlalchemy.select(ordinary_associations.c.association_id).where(
                    ordinary_associations.c.service_name == service_name,
                    ordinary_associations.c.association_id.in_(
                        tuple(sorted(pointed_association_ids, key=str)))))
        # ``replica_record_id`` is PostgreSQL Uuid(as_uuid=True).  Keep every
        # bind typed as ``uuid.UUID`` instead of relying on driver-specific
        # coercion of canonical strings.  PaidLaunchSpec already rejects a
        # noncanonical spelling at its immutable input boundary.
        candidate_record_ids = tuple(
            sorted(
                {uuid.UUID(spec.replica_record_id) for spec in prepared_specs} |
                live_record_ids,
                key=str))
        if candidate_record_ids:
            scope_selects.append(
                sqlalchemy.select(ordinary_associations.c.association_id).where(
                    ordinary_associations.c.service_name == service_name,
                    ordinary_associations.c.replica_record_id.in_(
                        candidate_record_ids)))
        candidate_replica_ids = tuple(
            sorted({spec.replica_id for spec in prepared_specs}))
        if candidate_replica_ids:
            # Replica numbers are lifecycle-local.  Old inert incarnations may
            # legitimately reuse them, so numeric collision scope is current-
            # hash only.  UUID record identities remain cross-incarnation.
            scope_selects.append(
                sqlalchemy.select(ordinary_associations.c.association_id).where(
                    ordinary_associations.c.service_name == service_name,
                    ordinary_associations.c.service_hash == service_hash,
                    ordinary_associations.c.replica_id.in_(
                        candidate_replica_ids)))
        scope_query = (scope_selects[0] if len(scope_selects) == 1 else
                       sqlalchemy.union(*scope_selects))
        scope = scope_query.subquery('bounded_association_authority_scope')
        association_statement = sqlalchemy.select(ordinary_associations).join(
            scope,
            scope.c.association_id == ordinary_associations.c.association_id)
    association_rows = tuple(
        connection.execute(
            association_statement.order_by(
                ordinary_associations.c.association_id).with_for_update(
                    of=ordinary_associations)).mappings())
    association_ids = tuple(row['association_id'] for row in association_rows)
    association_request_ids = tuple(
        sorted({str(row['request_id']) for row in association_rows}))

    request_rows: tuple[Mapping[str, Any], ...] = ()
    queue_rows: tuple[Mapping[str, Any], ...] = ()
    pin_rows: tuple[Mapping[str, Any], ...] = ()
    if association_ids:
        # Both sides are bounded/indexed identity probes: request_id is the
        # primary key and API009 installs the unique partial
        # uq_api_requests_ordinary_launch_association index.  Retain the latter
        # on steady ticks too so a malformed divergent root still fails closed
        # without scanning historical requests.
        request_predicate = sqlalchemy.or_(
            request_rows_table.c.request_id.in_(association_request_ids),
            request_rows_table.c.ordinary_launch_association_id.in_(
                association_ids))
        request_rows = tuple(
            connection.execute(
                sqlalchemy.select(
                    request_rows_table.c.request_id,
                    request_rows_table.c.ordinary_launch_association_id,
                    request_rows_table.c.handler_name,
                    request_rows_table.c.status,
                    request_rows_table.c.terminal_cause,
                    request_rows_table.c.finished_at,
                    request_rows_table.c.execution_generation,
                    request_rows_table.c.execution_quiescence_required,
                    request_rows_table.c.execution_quiesced_generation,
                    request_rows_table.c.execution_quiesced_at,
                    request_rows_table.c.resource_action_id,
                    request_rows_table.c.resource_action_attempt,
                    request_rows_table.c.binding_protocol_version,
                    request_rows_table.c.profile_kind,
                    request_rows_table.c.profile_version,
                    request_rows_table.c.profile_digest,
                    request_rows_table.c.capability_cohort_epoch,
                    request_rows_table.c.capability_profile_set_digest,
                    request_rows_table.c.receipt_protocol_version).where(
                        request_predicate).order_by(
                            request_rows_table.c.request_id).with_for_update()).
            mappings())
        queue_rows = tuple(
            connection.execute(
                sqlalchemy.select(request_queue_table.c.request_id).where(
                    request_queue_table.c.request_id.in_(
                        association_request_ids)).order_by(
                            request_queue_table.c.request_id).with_for_update()
            ).mappings())
        pin_rows = tuple(
            connection.execute(
                sqlalchemy.select(
                    request_pins_table.c.pin_kind, request_pins_table.c.pin_id,
                    request_pins_table.c.request_id).where(
                        sqlalchemy.or_(
                            request_pins_table.c.request_id.in_(
                                association_request_ids),
                            request_pins_table.c.pin_id.in_(association_ids))).
                order_by(
                    request_pins_table.c.pin_kind,
                    request_pins_table.c.pin_id).with_for_update()).mappings())

    association_by_id = {row['association_id']: row for row in association_rows}
    for association_id, expected in pointed_association_identities.items():
        association = association_by_id.get(association_id)
        if (association is None or
                association.get('service_name') != service_name or
                association.get('service_hash') != service_hash or
                association.get('replica_id') != expected[0] or
                association.get('replica_record_id') != expected[1]):
            raise CapacityAdmissionConflict(
                'Locked association pointer is missing or has a mismatched '
                'service/replica identity.')
    blocking_request_association_ids: set[uuid.UUID] = set()
    blocking_request_ids: set[str] = set()
    for request_row in request_rows:
        association_id = request_row['ordinary_launch_association_id']
        association = association_by_id.get(association_id)
        if association is None:
            raise CapacityAdmissionConflict(
                'Retained ordinary launch request root is malformed.')
        root_state = _retained_request_root_state(request_row, association)
        if root_state is _RetainedRequestRootState.MALFORMED:
            raise CapacityAdmissionConflict(
                'Retained ordinary launch request root is malformed.')
        if root_state is _RetainedRequestRootState.BLOCKING:
            blocking_request_association_ids.add(association_id)
            blocking_request_ids.add(str(request_row['request_id']))

    if exhaustive_history_census:
        retained_association_ids = set(blocking_request_association_ids)
        retained_association_ids.update(pointed_association_ids)
        retained_association_ids.update({
            row['pin_id']
            for row in pin_rows
            if isinstance(row['pin_id'], uuid.UUID)
        })
        retained_request_ids = set(blocking_request_ids)
        retained_request_ids.update(
            {str(row['request_id']) for row in queue_rows})
        retained_request_ids.update({
            str(row['request_id'])
            for row in pin_rows
            if row['request_id'] is not None
        })
        active_rows = tuple(
            row for row in association_rows
            if not (str(row['service_hash']) != service_hash and
                    _inert_recreated_service_association(
                        row,
                        replica_records=replica_records,
                        retained_association_ids=retained_association_ids,
                        retained_request_ids=retained_request_ids)))
    else:
        active_rows = association_rows
    return _LockedAssociationAuthorityGraph(
        association_rows=association_rows,
        active_association_rows=active_rows,
        request_rows=request_rows,
        queue_rows=queue_rows,
        pin_rows=pin_rows,
        blocking_request_count=len(blocking_request_ids))


def _canonical_prepared_paid_launch_specs(
    value: Sequence[serve_paid_capacity.PaidLaunchSpec],
    *,
    service: Mapping[str, Any],
    service_name: str,
    service_hash: str,
    service_lifecycle_epoch: int,
    service_version: int,
    accounting_cards: Mapping[str, int],
    backend_num_nodes: int,
    locked_version: _LockedPaidLaunchVersionAuthority | None,
) -> tuple[tuple[serve_paid_capacity.PaidLaunchSpec, ...], str | None]:
    """Validate the provider-free candidate lock superset."""
    if (not isinstance(value, Sequence) or isinstance(value,
                                                      (str, bytes, bytearray))):
        raise ValueError('Prepared paid launch specs must be a sequence.')
    specs = tuple(value)
    if len(specs) > serve_paid_capacity.MAX_PREPARED_LAUNCH_SPECS:
        raise ValueError('Prepared paid launch cohort is too large.')
    if any(not isinstance(spec, serve_paid_capacity.PaidLaunchSpec)
           for spec in specs):
        raise ValueError('Prepared paid launch spec is malformed.')
    ordinals = tuple(spec.ordinal for spec in specs)
    if ordinals != tuple(range(len(specs))):
        raise ValueError('Prepared paid launch order is noncanonical.')
    if not specs:
        return specs, None
    if locked_version is None:
        raise CapacityAdmissionConflict(
            'Elected version has no immutable paid launch authority.')
    try:
        controller_config = locked_version.controller_config
        controller_config_digest = locked_version.controller_config_digest
        controller_config_snapshot_id = (
            locked_version.controller_config_snapshot_id)
        if (not isinstance(controller_config, bytes) or
                not isinstance(controller_config_digest, str) or
                not isinstance(controller_config_snapshot_id, str)):
            raise ValueError('Elected version controller config is absent.')
        expected_version_authority = (
            serve_paid_capacity.PaidLaunchVersionAuthority(
                service_spec=locked_version.service_spec,
                service_spec_sha256=hashlib.sha256(
                    locked_version.service_spec).hexdigest(),
                controller_config=controller_config,
                controller_config_digest=controller_config_digest,
                controller_config_snapshot_id=controller_config_snapshot_id))
        if (not isinstance(locked_version.launch_yaml_content, str) or
                not locked_version.launch_yaml_content or
                not isinstance(locked_version.placement_catalog, Mapping)):
            raise ValueError('Elected version launch inputs are absent.')
        catalog_payload = dict(locked_version.placement_catalog)
        catalog_sha256 = serve_paid_capacity.paid_launch_payload_sha256(
            catalog_payload)
        catalog = spot_placer.PlacementCatalog.from_dict(catalog_payload)
        if catalog.num_nodes != backend_num_nodes:
            raise ValueError('Placement catalog has a different node count.')
        launch_spec = pickle.loads(expected_version_authority.service_spec)
        launch_task = serve_utils.load_task_with_service_spec(
            locked_version.launch_yaml_content, launch_spec)
        if launch_task.service is None:
            raise ValueError('Elected version has no service policy.')
        replica_port = serve_utils.resolve_replica_ingress_port(
            launch_task, pool=launch_task.service.pool)
        ranked_catalog = catalog.ranked_entries(
            locked_version.placement_contract)
        catalog_by_location = {
            entry.location: entry for entry in ranked_catalog
        }
    except (TypeError, ValueError) as error:
        raise CapacityAdmissionConflict(
            'Elected version paid launch authority is malformed.') from error
    workspace = service.get('workspace')
    resource_scope = service.get('resource_scope')
    if (not isinstance(resource_scope, str) or not resource_scope or
            resource_scope != service_hash):
        raise CapacityAdmissionConflict(
            'Paid launch requires the current incarnation resource scope.')
    expected = (service_name, service_hash, service_lifecycle_epoch,
                service_version)
    identities = set()
    record_ids = set()
    occurrences_by_rank: dict[int, int] = {}
    previous_order: tuple[int, int, int] | None = None
    pool_window = serve_paid_capacity.base_limit()
    expected_worker_fields = {
        'schema_version', 'launch_yaml_content', 'cluster_name',
        'log_file_name', 'resources_override', 'retry_until_up',
        'frozen_controller_config_path'
    }
    for spec in specs:
        if ((spec.service_name, spec.service_hash, spec.service_lifecycle_epoch,
             spec.service_version) != expected or
                not isinstance(workspace, str) or not workspace or
                spec.workspace != workspace or
                spec.accelerator not in accounting_cards or
                spec.gpu_units_per_node != accounting_cards[spec.accelerator] or
                spec.num_nodes != backend_num_nodes):
            raise CapacityAdmissionConflict(
                'Prepared paid launch identity changed before admission.')
        evidence = spec.catalog_evidence
        try:
            worker = serve_paid_capacity.thaw_paid_launch_payload(
                spec.worker_construction)
            stored_override = serve_paid_capacity.thaw_paid_launch_payload(
                spec.resources_override)
            decoded_override = spot_placer.decode_resources_override(
                stored_override)
            location = spot_placer.Location.from_resources_override(
                decoded_override)
            catalog_entry = (None if location is None else
                             catalog_by_location.get(location))
            expected_pool_key = (None if location is None else
                                 serve_paid_capacity.pool_key(
                                     location,
                                     workspace=spec.workspace,
                                     num_nodes=spec.num_nodes,
                                     aws_account_id=spec.provider_account))
            expected_frontier_key = (None if location is None else
                                     serve_paid_capacity.frontier_key(location))
            expected_cluster_name = serve_utils.generate_replica_cluster_name(
                service_name, spec.replica_id, resource_scope)
            expected_log_file_name = (
                serve_utils.generate_replica_launch_log_file_name(
                    service_name, spec.replica_id, resource_scope))
            expected_config_path = (
                serve_utils.generate_versioned_config_yaml_file_name(
                    service_name, service_version, resource_scope))
        except (TypeError, ValueError) as error:
            raise CapacityAdmissionConflict(
                'Prepared paid launch cannot be derived from its immutable '
                'version.') from error
        if (evidence.version_authority != expected_version_authority or
                evidence.placement_catalog_sha256 != catalog_sha256 or
                worker.get('launch_yaml_content')
                != locked_version.launch_yaml_content or
                set(worker) != expected_worker_fields or
                worker.get('schema_version') != 1 or
                worker.get('cluster_name') != expected_cluster_name or
                worker.get('log_file_name') != expected_log_file_name or
                worker.get('frozen_controller_config_path')
                != expected_config_path or
                worker.get('retry_until_up') is not False or
                spec.cluster_name_seed != expected_cluster_name or
                catalog_entry is None or
                catalog_entry.rank != evidence.catalog_rank or
                catalog_entry.location.use_spot is not True or
                str(catalog_entry.location.cloud).casefold() not in ('aws',
                                                                     'gcp') or
                not math.isfinite(catalog_entry.hourly_cost) or
                catalog_entry.hourly_cost <= 0 or
                not math.isfinite(catalog_entry.normalized_hourly_cost) or
                catalog_entry.normalized_hourly_cost <= 0 or
                evidence.slot_within_pool_window >= pool_window or
                expected_pool_key != spec.pool_key or
                expected_frontier_key != spec.frontier_key or
                worker.get('resources_override') != stored_override or
                not isinstance(replica_port, str) or not replica_port):
            raise CapacityAdmissionConflict(
                'Prepared paid launch disagrees with the elected catalog.')
        occurrence = occurrences_by_rank.get(evidence.catalog_rank, 0)
        expected_round, expected_slot = divmod(occurrence, pool_window)
        order = (evidence.exploration_round, evidence.catalog_rank,
                 evidence.slot_within_pool_window)
        if ((evidence.exploration_round, evidence.slot_within_pool_window)
                != (expected_round, expected_slot) or
            (previous_order is not None and order <= previous_order)):
            raise CapacityAdmissionConflict(
                'Prepared paid launch catalog traversal is noncanonical.')
        occurrences_by_rank[evidence.catalog_rank] = occurrence + 1
        previous_order = order
        identity = (spec.replica_id, spec.replica_record_id)
        if spec.replica_id in identities or spec.replica_record_id in record_ids:
            raise ValueError('Prepared paid launch identities are duplicated.')
        identities.add(spec.replica_id)
        record_ids.add(spec.replica_record_id)
    return specs, replica_port


def _resolve_locked_policy_history(
    *,
    history: _LockedPlanHistory,
    config: _ReservedFillServiceConfig,
    snapshot: demand_state.DurableAutoscalingSnapshot,
    service_name: str,
    service_hash: str,
    service_lifecycle_epoch: int,
    service_version: int,
    accounting_cards: Mapping[str, int],
    backend_num_nodes: int,
    locked_capacity: _LockedCapacityRows,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
    allocation_reserved: Mapping[str, int],
    raw_claim_count: int,
    raw_waiter_count: int,
    dependent_effect_count: int,
) -> tuple[capacity_planning.CapacityPolicyState,
           capacity_planning.CapacityPlanCandidate]:
    """Strict-decode format 6, or construct its unique clean genesis."""
    capacity_unit = (capacity_planning.CapacityUnit.LOGICAL_GPU
                     if config.capacity_unit
                     is reserved_fill_planner.FillCapacityUnit.LOGICAL else
                     capacity_planning.CapacityUnit.PHYSICAL_BACKEND)
    if history.previous is not None:
        previous = history.previous
        head = history.head
        assert head is not None
        payload = previous['payload']
        digest = previous['content_sha256']
        if (previous['service_hash'] != service_hash or
                previous['service_lifecycle_epoch'] != service_lifecycle_epoch
                or previous['service_version'] != service_version or
                previous['protocol_version'] != PROTOCOL_VERSION or
                previous['generation'] != head['generation'] or
                previous['demand_feed_generation']
                != head['demand_feed_generation'] or
                previous['demand_source_epoch'] != snapshot.demand_source_epoch
                or not isinstance(previous['demand_feed_generation'], int) or
                previous['demand_feed_generation'] < 1 or
                previous['demand_feed_generation']
                > snapshot.demand_feed_generation or
                not isinstance(payload, Mapping) or
                not isinstance(digest, str) or
                _SHA256_RE.fullmatch(digest) is None or
                _sha256(payload) != digest):
            raise CapacityAdmissionConflict(
                'Current capacity policy belongs to another service identity.')
        try:
            prior_snapshot, candidate = capacity_planning.decode_planner_envelope(
                payload.get('planner'))
        except ValueError as error:
            raise CapacityAdmissionConflict(
                'Current capacity policy is not strict format 6.') from error
        payload_service = payload.get('service')
        payload_source = payload.get('source')
        state = candidate.next_policy_state
        if (not isinstance(payload_service, Mapping) or
                not isinstance(payload_source, Mapping) or
                payload_service.get('name') != service_name or
                payload_service.get('hash') != service_hash or
                payload_service.get('lifecycle_epoch')
                != service_lifecycle_epoch or
                payload_service.get('version') != service_version or
                payload_source.get('demand_source_epoch')
                != previous['demand_source_epoch'] or
                payload_source.get('route_generation')
                != previous['route_generation'] or
                payload_source.get('route_sha256') != previous['route_sha256']
                or payload_source.get('route_source_epoch')
                != previous['route_source_epoch'] or
                prior_snapshot.source_generation
                != previous['demand_feed_generation'] or
                prior_snapshot.service_version != service_version or {
                    card.casefold()
                    for card in prior_snapshot.configured_accelerators
                } != set(accounting_cards) or
                prior_snapshot.capacity_unit is not capacity_unit or
                prior_snapshot.maximum_capacity != config.max_capacity or
                prior_snapshot.physical_gpu_width_by_accelerator.as_dict()
                != dict(accounting_cards) or
                prior_snapshot.backend_num_nodes != backend_num_nodes or
                set(candidate.physical_gpu_width_by_accelerator.as_dict())
                != set(accounting_cards) or
                candidate.physical_gpu_width_by_accelerator
                != prior_snapshot.physical_gpu_width_by_accelerator or
                candidate.backend_num_nodes != prior_snapshot.backend_num_nodes
                or state is None or state.service_name != service_name or
                state.service_version != service_version or
                state.capacity_unit is not capacity_unit or
                state.maximum_capacity != config.max_capacity or
                candidate.capacity_unit is not capacity_unit or
                not candidate.attribution_complete):
            raise CapacityAdmissionConflict(
                'Current capacity policy identity is inconsistent.')
        return state, candidate

    # A current, validated reserved allocation is an input to the first policy
    # decision, not evidence that an older policy already committed effects.
    # Genesis must therefore be able to consume it.  Replica, intent, lane,
    # claim, waiter, and dependent-effect rows remain forbidden so a retained
    # authority graph can never be relabelled as a clean recreation.
    clean = bool(history.head is None and history.maximum_generation is None and
                 not locked_capacity.replica_rows and
                 not locked_capacity.all_service_nonterminal_intent_rows and
                 not lane_projection.rows and raw_claim_count == 0 and
                 raw_waiter_count == 0 and dependent_effect_count == 0)
    if not clean:
        raise CapacityAdmissionConflict(
            'Missing capacity policy beside a retained authority graph.')
    try:
        return capacity_planning.genesis_capacity_policy(
            service_name=service_name,
            service_version=service_version,
            last_reduced_demand_generation=0,
            capacity_unit=capacity_unit,
            maximum_capacity=config.max_capacity,
            physical_gpu_width_by_accelerator=(
                capacity_planning.AcceleratorCapacity.from_mapping(
                    accounting_cards)),
            backend_num_nodes=backend_num_nodes)
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Clean capacity-policy genesis is malformed.') from error


def _resolve_validated_format_6_head(
    *,
    history: _LockedPlanHistory,
    config: _ReservedFillServiceConfig,
    snapshot: demand_state.DurableAutoscalingSnapshot,
    service_name: str,
    service_hash: str,
    service_lifecycle_epoch: int,
    service_version: int,
    accounting_cards: Mapping[str, int],
    backend_num_nodes: int,
    locked_capacity: _LockedCapacityRows,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
    allocation_reserved: Mapping[str, int],
) -> tuple[capacity_planning.CapacityPolicyState,
           capacity_planning.CapacityPlanCandidate] | None:
    """Strict-decode the trust anchor before using a bounded history scope.

    Authority counts are inputs only to headless genesis.  This wrapper enters
    the prior-head branch exclusively, making it impossible for a placeholder
    zero count to authorize genesis or bypass its exhaustive census.
    """
    previous = history.previous
    if previous is None:
        return None
    payload = previous.get('payload')
    planner = payload.get('planner') if isinstance(payload, Mapping) else None
    # Schema 5 existed before the exhaustive census was part of genesis.  It
    # therefore cannot be an inductive receipt, even when it is otherwise a
    # valid planner envelope.  There is deliberately no transition decoder:
    # any non-current head first takes the exhaustive authority scope and then
    # fails the strict current-only decode below, requiring the authorized
    # exact-zero service reset.
    if (not isinstance(planner, Mapping) or
            type(planner.get('schema_version')) is not int or
            planner['schema_version']
            != capacity_planning.CAPACITY_PLANNING_ENVELOPE_SCHEMA_VERSION):
        return None
    return _resolve_locked_policy_history(
        history=history,
        config=config,
        snapshot=snapshot,
        service_name=service_name,
        service_hash=service_hash,
        service_lifecycle_epoch=service_lifecycle_epoch,
        service_version=service_version,
        accounting_cards=accounting_cards,
        backend_num_nodes=backend_num_nodes,
        locked_capacity=locked_capacity,
        lane_projection=lane_projection,
        allocation_reserved=allocation_reserved,
        raw_claim_count=0,
        raw_waiter_count=0,
        dependent_effect_count=0)


def _clip_prepared_paid_admission(
    prepared_specs: tuple[serve_paid_capacity.PaidLaunchSpec, ...],
    *,
    candidate: capacity_planning.CapacityPlanCandidate,
    decision: CapacityPlanDecision,
    frontier_limit: int,
    replica_port: str,
    created_at: float,
) -> tuple[_PreparedPaidAdmission, ...]:
    """Clip immutable cheapest-first specs to one planner paid target."""
    if candidate.reserved_launch_target.total() > 0:
        return ()
    remaining = candidate.paid_launch_target.as_dict()
    physical_widths = candidate.physical_gpu_width_by_accelerator.as_dict()
    clipped = []
    for launch_spec in prepared_specs:
        card = launch_spec.accelerator
        if (physical_widths.get(card) != launch_spec.gpu_units_per_node or
                candidate.backend_num_nodes != launch_spec.num_nodes):
            raise CapacityAdmissionConflict(
                'Prepared paid launch contradicts the planned backend shape.')
        plan_units = (1 if candidate.capacity_unit
                      is capacity_planning.CapacityUnit.PHYSICAL_BACKEND else
                      launch_spec.gpu_units_per_node)
        if remaining.get(card, 0) < plan_units:
            continue
        persistence_spec = launch_spec.persistence_spec(
            priority=decision.paid_launch_priority(card),
            frontier_limit=frontier_limit,
            replica_port=replica_port,
            planned_capacity=plan_units,
            created_at=created_at)
        if persistence_spec.candidate.replica_info.planned_capacity != plan_units:
            raise CapacityAdmissionConflict(
                'Prepared paid launch has the wrong planner debit width.')
        clipped.append(
            _PreparedPaidAdmission(
                launch_spec=launch_spec,
                persistence_spec=persistence_spec,
                plan_units=plan_units,
                physical_gpu_units=launch_spec.physical_gpu_units))
        remaining[card] -= plan_units
    return tuple(clipped)


def _read_paid_launch_receipt(
    connection: sqlalchemy.engine.Connection,
    *,
    authority: PaidLaunchAuthority,
    service_lifecycle_epoch: int,
    service_version: int,
    accepted: tuple[_PreparedPaidAdmission, ...],
) -> serve_paid_capacity.PaidLaunchReceipt:
    """Read the exact sparse accepted graph back from PostgreSQL."""
    members: list[serve_paid_capacity.PaidLaunchReceiptMember] = []
    if accepted:
        replica_ids = [item.launch_spec.replica_id for item in accepted]
        rows = connection.execute(
            sqlalchemy.select(
                _REPLICAS.c.replica_id, _REPLICAS.c.replica_state,
                _REPLICAS.c.paid_capacity_pool_key.label('replica_pool_key'),
                _CLAIMS.c.pool_key.label('claim_pool_key'), _CLAIMS.c.priority,
                _CLAIMS.c.capacity_plan_generation,
                _CLAIMS.c.capacity_plan_sha256,
                _CLAIMS.c.capacity_plan_accelerator,
                _CLAIMS.c.capacity_plan_units).select_from(
                    _REPLICAS.join(
                        _CLAIMS,
                        sqlalchemy.and_(
                            _CLAIMS.c.service_name == _REPLICAS.c.service_name,
                            _CLAIMS.c.replica_id == _REPLICAS.c.replica_id,
                            _CLAIMS.c.service_hash == authority.service_hash))).
            where(_REPLICAS.c.service_name == authority.service_name,
                  _REPLICAS.c.replica_id.in_(replica_ids)).order_by(
                      _REPLICAS.c.replica_id)).mappings().all()
        by_replica_id = {int(row['replica_id']): row for row in rows}
        if set(by_replica_id) != set(replica_ids):
            raise CapacityAdmissionConflict(
                'Committed paid admission graph is incomplete.')
        for item in accepted:
            spec = item.launch_spec
            row = by_replica_id[spec.replica_id]
            state = row['replica_state']
            claim_pool_key = row['claim_pool_key']
            try:
                shape = serve_paid_capacity.paid_pool_gpu_shape(claim_pool_key)
            except serve_paid_capacity.PaidGPUAttributionError as error:
                raise CapacityAdmissionConflict(
                    'Committed paid admission pool has no exact GPU shape.') \
                    from error
            if (not isinstance(state, Mapping) or dict(state) != item.
                    persistence_spec.candidate.replica_info.to_storage_dict() or
                    state.get('replica_record_id') != spec.replica_record_id or
                    row['replica_pool_key'] != claim_pool_key or
                    claim_pool_key != spec.pool_key or
                    row['capacity_plan_generation'] != authority.generation or
                    row['capacity_plan_sha256'] != authority.content_sha256 or
                    row['capacity_plan_accelerator'] != shape.accelerator or
                    shape.accelerator != spec.accelerator or
                    row['capacity_plan_units'] != item.plan_units or
                    shape.gpu_units_per_node != spec.gpu_units_per_node or
                    shape.num_nodes != spec.num_nodes):
                raise CapacityAdmissionConflict(
                    'Committed paid admission graph changed during readback.')
            members.append(
                serve_paid_capacity.PaidLaunchReceiptMember(
                    replica_id=spec.replica_id,
                    replica_record_id=spec.replica_record_id,
                    pool_key=claim_pool_key,
                    priority=int(row['priority']),
                    accelerator=shape.accelerator,
                    plan_units=int(row['capacity_plan_units']),
                    physical_gpu_units=shape.total_gpu_units))
    return serve_paid_capacity.PaidLaunchReceipt(
        service_name=authority.service_name,
        service_hash=authority.service_hash,
        service_lifecycle_epoch=service_lifecycle_epoch,
        service_version=service_version,
        capacity_plan_generation=authority.generation,
        capacity_plan_sha256=authority.content_sha256,
        capacity_unit=authority.capacity_unit.value,
        members=tuple(members))


def _locked_supply_authority_valid_until(
    *,
    allocation: AuthenticatedAllocationMap | None,
    allocation_authorizing: bool,
    locked_capacity: _LockedCapacityRows,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
) -> datetime.datetime | None:
    """Return the earliest TTL that affected the locked supply projection.

    Admitted Kueue rows and committed intents are durable positive ownership,
    so they carry no freshness lease. Fresh waiting rows, pending intents, and
    allocation observations are time-sensitive classifications and are fenced.
    A statically disjoint paid plan deliberately does not consume allocation
    evidence, so unrelated pool expiry is excluded in that one case.
    """
    horizons: list[datetime.datetime] = []
    if allocation_authorizing and allocation is not None:
        horizons.extend(
            datetime.datetime.fromtimestamp(pool.valid_until,
                                            datetime.timezone.utc)
            for pool in allocation.pool_snapshots)
    for row in locked_capacity.intent_rows:
        if row.get('state') in _PENDING_ZERO_COST_INTENT_STATES:
            valid_until = row.get('valid_until')
            if (isinstance(valid_until, datetime.datetime) and
                    valid_until > lane_projection.now):
                horizons.append(valid_until)
    for intent_key in lane_projection.fresh_waiting_intent_keys:
        lane_row = lane_projection.row_by_intent_key.get(intent_key)
        valid_until = getattr(lane_row, 'valid_until', None)
        if isinstance(valid_until, datetime.datetime):
            horizons.append(valid_until)
    return min(horizons) if horizons else None


def _postwrite_revalidate_current_admission(
    connection: sqlalchemy.engine.Connection,
    *,
    service_before: Mapping[str, Any],
    snapshot: demand_state.DurableAutoscalingSnapshot,
    plan: CapacityPlanInput,
    valid_until: datetime.datetime,
) -> None:
    """Apply the final DB-clock, source, version, and owner fence."""
    postwrite_now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    if postwrite_now >= valid_until:
        raise CapacityAdmissionConflict(
            'Capacity-plan authority expired during atomic admission.')
    service = connection.execute(
        sqlalchemy.select(_SERVICES).where(
            _SERVICES.c.name == plan.service_name)).mappings().one_or_none()
    identity_fields = ('hash', 'lifecycle_epoch', 'current_version',
                       'controller_pid', 'controller_ip',
                       'controller_incarnation', 'controller_owner_epoch',
                       'demand_source_epoch', 'route_source_epoch')
    if (service is None or any(
            service.get(field) != service_before.get(field)
            for field in identity_fields)):
        raise CapacityAdmissionConflict(
            'Service owner or version changed during atomic admission.')
    demand_generation = connection.execute(
        sqlalchemy.select(_DEMAND_GENERATIONS.c.generation).where(
            _DEMAND_GENERATIONS.c.service_name == plan.service_name,
            _DEMAND_GENERATIONS.c.service_hash ==
            plan.service_hash)).scalar_one_or_none()
    route_head = connection.execute(
        sqlalchemy.select(_ROUTE_HEADS).where(
            _ROUTE_HEADS.c.service_name ==
            plan.service_name)).mappings().one_or_none()
    reports = connection.execute(
        sqlalchemy.select(_DEMAND_REPORTS).where(
            _DEMAND_REPORTS.c.service_name == plan.service_name,
            _DEMAND_REPORTS.c.service_hash == plan.service_hash).order_by(
                _DEMAND_REPORTS.c.reporter_session_id)).mappings().all()
    selected_reports = demand_state.current_demand_report_rows(reports, service)
    watermark = ([] if selected_reports is None else [{
        'reporter_session_id': row['reporter_session_id'],
        'sequence': int(row['sequence']),
        'payload_sha256': row['payload_sha256'],
    } for row in selected_reports])
    if (demand_generation != snapshot.demand_feed_generation or
            route_head is None or
            route_head['generation'] != snapshot.route_generation or
            route_head['valid_until'] <= postwrite_now or
            selected_reports is None or
            watermark != _canonical_watermark(snapshot.receipt_watermark) or
            any(row['valid_until'] <= postwrite_now
                for row in selected_reports)):
        raise CapacityAdmissionConflict(
            'Demand or route authority expired during atomic admission.')


class CapacityAdmissionRepository:
    """Transactional owner of semantic plans and their freshness head."""

    def __init__(self, engine: sqlalchemy.engine.Engine | None = None) -> None:
        self._engine = engine

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        engine = self._engine or serve_state_schema.get_database_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise CapacityAdmissionUnavailable(
                'Ordered capacity admission requires PostgreSQL.')
        return engine

    def read_current_fill_demand_witness(
        self,
        service_name: str,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
        *,
        max_age_seconds: float,
    ) -> CommittedFillDemandWitness | None:
        """Read a current semantic demand witness without effect authority.

        The caller supplies a freshness horizon suitable for the reserved
        poller (normally several poll intervals).  The short plan
        ``valid_until`` is intentionally not consulted here: it remains the
        independent provider-effect fence.  Every current service, demand,
        route, plan-head, content-hash, and immutable fill-policy identity is
        nevertheless revalidated through one connection-bounded read.  A
        newer heartbeat generation may retain the plan only when the
        reconstructed normalized demand and exact route context are unchanged
        and any deadline multiset is a monotonic tightening. The witness keeps
        the plan's original causal generation and is only a free-capacity
        lower bound; it carries no provider-effect authority.
        """
        if (not isinstance(service_name, str) or not service_name or
                not isinstance(expected_service_hash, str) or
                not expected_service_hash or
                not isinstance(expected_controller_owner, tuple) or
                len(expected_controller_owner) != 2 or
                not isinstance(max_age_seconds, (int, float)) or
                isinstance(max_age_seconds, bool) or
                not math.isfinite(float(max_age_seconds)) or
                max_age_seconds <= 0):
            raise ValueError('Fill-demand witness read is malformed.')
        with self.engine.connect() as connection:
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            service = connection.execute(
                sqlalchemy.select(_SERVICES).where(
                    _SERVICES.c.name == service_name)).mappings().one_or_none()
            if (service is None or service['hash'] != expected_service_hash or
                (service.get('controller_pid'), service.get('controller_ip'))
                    != expected_controller_owner or service.get('pool') != 0 or
                    service.get('demand_source_mode')
                    != DemandSourceMode.DURABLE_FEED.value or
                    service.get('demand_authority_capable') is not True or
                    service.get('demand_authority_controller_incarnation')
                    != service.get('controller_incarnation') or
                    service.get('demand_authority_protocol_version')
                    != PROTOCOL_VERSION or
                    service.get('route_source_mode') != 'DURABLE_PROJECTED' or
                    service.get('route_projection_capable') is not True or
                    service.get('route_projection_controller_incarnation')
                    != service.get('controller_incarnation') or
                    service.get('route_projection_protocol_version')
                    not in (1, 2)):
                return None
            head = connection.execute(
                sqlalchemy.select(_HEADS).where(
                    _HEADS.c.service_name ==
                    service_name)).mappings().one_or_none()
            if head is None:
                return None
            plan = connection.execute(
                sqlalchemy.select(_PLANS).where(
                    _PLANS.c.service_name == service_name, _PLANS.c.generation
                    == head['generation'])).mappings().one_or_none()
            current_demand_generation = connection.execute(
                sqlalchemy.select(_DEMAND_GENERATIONS.c.generation).where(
                    _DEMAND_GENERATIONS.c.service_name == service_name,
                    _DEMAND_GENERATIONS.c.service_hash ==
                    expected_service_hash)).scalar_one_or_none()
            route_head = connection.execute(
                sqlalchemy.select(_ROUTE_HEADS).where(
                    _ROUTE_HEADS.c.service_name ==
                    service_name)).mappings().one_or_none()
            route = (None if route_head is None else connection.execute(
                sqlalchemy.select(_ROUTE_SNAPSHOTS).where(
                    _ROUTE_SNAPSHOTS.c.service_name == service_name,
                    _ROUTE_SNAPSHOTS.c.generation
                    == route_head['generation'])).mappings().one_or_none())
            refreshed_at = head['refreshed_at']
            plan_demand_generation = (None if plan is None else
                                      plan['demand_feed_generation'])
            if (plan is None or route_head is None or route is None or
                    not isinstance(refreshed_at, datetime.datetime) or
                    refreshed_at > now or now - refreshed_at
                    > datetime.timedelta(seconds=float(max_age_seconds)) or
                    type(current_demand_generation) is not int or
                    type(plan_demand_generation) is not int or
                    head['demand_feed_generation'] != plan_demand_generation or
                    plan_demand_generation > current_demand_generation or
                    plan['service_hash'] != service['hash'] or
                    plan['service_lifecycle_epoch']
                    != service['lifecycle_epoch'] or
                    plan['service_version'] != service['current_version'] or
                    plan['demand_source_epoch']
                    != service['demand_source_epoch'] or
                    plan['route_generation'] != route_head['generation'] or
                    plan['route_sha256'] != route['content_sha256'] or
                    plan['route_source_epoch'] != service['route_source_epoch']
                    or plan['protocol_version'] != PROTOCOL_VERSION or
                    route['service_hash'] != service['hash'] or
                    route['service_lifecycle_epoch']
                    != service['lifecycle_epoch'] or
                    route['service_version'] != service['current_version'] or
                    route['controller_incarnation']
                    != service['controller_incarnation'] or
                    route['protocol_version'] != PROTOCOL_VERSION or
                    route['producer_protocol_version']
                    != service['route_projection_protocol_version']):
                return None
            payload = plan['payload']
            content_sha256 = plan['content_sha256']
            if (not isinstance(payload, Mapping) or
                    _SHA256_RE.fullmatch(content_sha256) is None or
                    _sha256(payload) != content_sha256):
                return None
            try:
                (route_projection.RouteProjectionRepository.
                 validate_snapshot_row(route))
                if not route_projection.snapshot_owner_matches(route, service):
                    return None
                planner_snapshot, candidate = _decode_planner_payload(
                    payload.get('planner'))
                if plan_demand_generation < current_demand_generation:
                    current_snapshot = demand_state.get_autoscaling_snapshot(
                        service_name,
                        expected_service_hash,
                        connection=connection)
                    if (current_snapshot is None or
                            current_snapshot.demand_feed_generation
                            != current_demand_generation or
                            current_snapshot.demand_source_epoch
                            != service['demand_source_epoch'] or
                            current_snapshot.route_generation
                            != plan['route_generation'] or
                            current_snapshot.route_sha256
                            != plan['route_sha256'] or
                            current_snapshot.route_source_epoch
                            != plan['route_source_epoch'] or
                            _changed_demand_semantics(
                                payload.get('normalized_demand'),
                                current_snapshot.normalized_demand)):
                        return None
                    if planner_snapshot.deadline is not None:
                        current_deadline = _deadline_demand_from_snapshot(
                            current_snapshot)
                        if (current_deadline is None or
                                not _deadline_demand_is_monotonic_tightening(
                                    planner_snapshot.deadline.demand,
                                    current_deadline)):
                            return None
                if (planner_snapshot.service_version
                        != service['current_version'] or
                        planner_snapshot.source_generation
                        != plan_demand_generation or
                        candidate.source_generation != plan_demand_generation or
                        not candidate.attribution_complete or
                        planner_snapshot.reservation.gate_policy
                        is not capacity_planning.ReservationGatePolicy.
                        DEMAND_GATED):
                    return None
                semantic_sha256 = candidate.demand_witness_sha256
                if semantic_sha256 is None:
                    return None
                reservation_compatible = (
                    candidate.reservation_demand_relation
                    is capacity_planning.ReservationDemandRelation.COMPATIBLE)
                if reservation_compatible:
                    reservation_acquisition_classes = (
                        candidate.reservation_acquisition_classes)
                    target_capacity = candidate.aggregate_demand_target
                else:
                    reservation_acquisition_classes = None
                    target_capacity = 0
                return CommittedFillDemandWitness(
                    service_name=service_name,
                    service_hash=str(service['hash']),
                    service_lifecycle_epoch=int(service['lifecycle_epoch']),
                    service_version=int(service['current_version']),
                    demand_source_epoch=int(service['demand_source_epoch']),
                    demand_feed_generation=int(plan_demand_generation),
                    observed_demand_feed_generation=int(
                        current_demand_generation),
                    route_generation=int(route_head['generation']),
                    route_sha256=str(route['content_sha256']),
                    route_source_epoch=int(service['route_source_epoch']),
                    capacity_plan_generation=int(plan['generation']),
                    capacity_plan_sha256=str(content_sha256),
                    target_capacity=target_capacity,
                    reservation_acquisition_classes=(
                        reservation_acquisition_classes),
                    semantic_sha256=semantic_sha256,
                    refreshed_at=refreshed_at)
            except (KeyError, TypeError, ValueError,
                    route_projection.RouteProjectionError,
                    CapacityAdmissionError):
                return None

    @staticmethod
    def _validate_sources(
            connection: sqlalchemy.engine.Connection, plan: CapacityPlanInput,
            service: Mapping[str, Any]) -> _ValidatedDemandSources:
        if (service['hash'] != plan.service_hash or
                service['lifecycle_epoch'] != plan.service_lifecycle_epoch or
                service['current_version'] != plan.service_version or
                service['demand_source_mode']
                != DemandSourceMode.DURABLE_FEED.value or
                service['demand_source_epoch'] != plan.demand_source_epoch or
                service['demand_authority_capable'] is not True or
                service['demand_authority_protocol_version'] != PROTOCOL_VERSION
                or service['demand_authority_controller_incarnation']
                != service['controller_incarnation']):
            raise CapacityAdmissionConflict(
                'Service demand authority changed before plan publication.')
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        demand_generation = connection.execute(
            sqlalchemy.select(_DEMAND_GENERATIONS.c.generation).where(
                _DEMAND_GENERATIONS.c.service_name == plan.service_name,
                _DEMAND_GENERATIONS.c.service_hash ==
                plan.service_hash).with_for_update()).scalar_one_or_none()
        if demand_generation != plan.demand_feed_generation:
            current_snapshot = demand_state.get_autoscaling_snapshot(
                plan.service_name, plan.service_hash, connection=connection)
            if (not isinstance(demand_generation, int) or
                    demand_generation <= plan.demand_feed_generation or
                    current_snapshot is None or
                    current_snapshot.demand_feed_generation != demand_generation
                    or current_snapshot.demand_source_epoch
                    != plan.demand_source_epoch or
                    current_snapshot.route_generation != plan.route_generation
                    or current_snapshot.route_sha256 != plan.route_sha256 or
                    current_snapshot.route_source_epoch
                    != plan.route_source_epoch or _changed_demand_semantics(
                        plan.normalized_demand,
                        current_snapshot.normalized_demand)):
                raise CapacityAdmissionConflict(
                    'Demand feed advanced with changed or unavailable '
                    'semantics before plan publication.')
            if plan.planner_payload:
                try:
                    planner_snapshot, _ = _decode_planner_payload(
                        plan.planner_payload)
                except ValueError as error:
                    raise CapacityAdmissionConflict(
                        'Stale plan has no decodable production planner '
                        'snapshot.') from error
                if planner_snapshot.deadline is not None:
                    raise CapacityAdmissionConflict(
                        'Deadline demand advanced before production plan '
                        'publication; a fresh locked planner run is required.')
            # Heartbeats advance the durable sequence even when the canonical
            # non-deadline demand decision is unchanged. Rebind to the exact
            # locked receipt so legacy publication does not have to race the
            # heartbeat interval. Deadline planning must use
            # plan_and_admit_current() and never reaches this shortcut.
            plan = dataclasses.replace(
                plan,
                demand_feed_generation=current_snapshot.demand_feed_generation,
                receipt_watermark=current_snapshot.receipt_watermark)
        reports = connection.execute(
            sqlalchemy.select(_DEMAND_REPORTS).where(
                _DEMAND_REPORTS.c.service_name == plan.service_name,
                _DEMAND_REPORTS.c.service_hash == plan.service_hash,
                _DEMAND_REPORTS.c.valid_until
                > now).order_by(_DEMAND_REPORTS.c.reporter_session_id).
            with_for_update()).mappings().all()
        selected_reports = demand_state.current_demand_report_rows(
            reports, service)
        if selected_reports is None:
            raise CapacityAdmissionConflict(
                'Fresh demand has no current load balancer.')
        reports = selected_reports
        watermark = [{
            'reporter_session_id': row['reporter_session_id'],
            'sequence': int(row['sequence']),
            'payload_sha256': row['payload_sha256'],
        } for row in reports]
        zero_plan = bool(
            plan.capacity_target_by_accelerator and
            plan.normalized_demand.get('fresh_aggregate_zero') is True and
            all(count == 0
                for count in plan.capacity_target_by_accelerator.values()))
        reports_complete = all(
            row['complete'] is True and row['protocol_version'] == 2
            for row in reports)
        reports_allow_zero = (
            zero_plan and service['route_projection_protocol_version'] == 2 and
            demand_state.reports_prove_fresh_aggregate_zero(reports))
        if (watermark != _canonical_watermark(plan.receipt_watermark) or
                not (reports_complete or reports_allow_zero)):
            raise CapacityAdmissionConflict(
                'Fresh demand receipts changed before plan publication.')
        route_head = connection.execute(
            sqlalchemy.select(_ROUTE_HEADS).where(
                _ROUTE_HEADS.c.service_name ==
                plan.service_name).with_for_update()).mappings().one_or_none()
        route = connection.execute(
            sqlalchemy.select(_ROUTE_SNAPSHOTS).where(
                _ROUTE_SNAPSHOTS.c.service_name == plan.service_name,
                _ROUTE_SNAPSHOTS.c.generation ==
                plan.route_generation)).mappings().one_or_none()
        if (route_head is None or route is None or
                route_head['generation'] != plan.route_generation or
                route_head['valid_until'] <= now or
                route['content_sha256'] != plan.route_sha256 or
                route['service_hash'] != plan.service_hash or
                route['service_lifecycle_epoch'] != plan.service_lifecycle_epoch
                or route['service_version'] != plan.service_version or
                route['controller_incarnation']
                != service['controller_incarnation'] or
                route['protocol_version'] != PROTOCOL_VERSION or
                route['producer_protocol_version']
                != service['route_projection_protocol_version'] or
                service['route_source_mode'] != 'DURABLE_PROJECTED' or
                service['route_source_epoch'] != plan.route_source_epoch or
                service['route_projection_capable'] is not True or
                service['route_projection_controller_incarnation']
                != service['controller_incarnation'] or
                service['route_projection_protocol_version'] not in (1, 2)):
            raise CapacityAdmissionConflict(
                'Fresh projected route changed before plan publication.')
        try:
            route_projection.RouteProjectionRepository.validate_snapshot_row(
                route)
        except route_projection.RouteProjectionError as error:
            raise CapacityAdmissionConflict(
                'Fresh projected route is corrupt.') from error
        if not route_projection.snapshot_owner_matches(route, service):
            raise CapacityAdmissionConflict(
                'Fresh projected route belongs to a different owner.')
        route_context = demand_state.validate_report_route_contexts(
            connection, service, reports, route_head, route, now)
        if (route_context is None or
                route_context.generation != plan.route_generation or
                route_context.content_sha256 != plan.route_sha256 or
                route_context.source_epoch != plan.route_source_epoch):
            raise CapacityAdmissionConflict(
                'Demand reports do not match the fresh projected route '
                'context.')
        valid_until = min([route_head['valid_until']] +
                          [row['valid_until'] for row in reports])
        if valid_until <= now:
            raise CapacityAdmissionConflict(
                'Demand authority expired during source validation.')
        return _ValidatedDemandSources(plan=plan, valid_until=valid_until)

    def project_reserved_supply(
        self,
        *,
        service_name: str,
        service_hash: str,
        service_lifecycle_epoch: int,
        service_version: int,
        accounting_cards: Mapping[str, int],
        authority: ReservedFillPlanAuthority,
    ) -> ReservedSupplyProjection:
        """Read the optimistic supply input later compared by publication."""
        cards = set(
            _canonical_counts(accounting_cards,
                              'capacity_target_by_accelerator'))
        if (not cards or authority.mode
                is not ReservedFillPlanAuthorityMode.ALLOCATION_BOUND):
            raise ValueError(
                'Reserved supply projection requires allocation-bound cards.')
        with self.engine.begin() as connection:
            serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
                connection)
            service = connection.execute(
                sqlalchemy.select(_SERVICES).where(
                    _SERVICES.c.name ==
                    service_name).with_for_update()).mappings().one_or_none()
            if (service is None or service['hash'] != service_hash or
                    service['lifecycle_epoch'] != service_lifecycle_epoch or
                    service['current_version'] != service_version):
                raise CapacityAdmissionConflict(
                    'Service changed before reserved-supply projection.')
            config = _reserved_fill_service_config_in_connection(
                connection, service)
            allocation = _validate_reserved_fill_authority_in_connection(
                connection,
                service,
                authority,
                reserved_fill_binding_required=config.binding_required,
                protocol_and_service_prelocked=True)
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            locked = _lock_capacity_rows(connection,
                                         service_name=service_name,
                                         service_hash=service_hash,
                                         now=now)
            lane_projection = _lock_kueue_projection(
                connection,
                service_name=service_name,
                service_hash=service_hash,
                service_lifecycle_epoch=service_lifecycle_epoch,
                service_version=service_version,
                accounting_cards=cards,
                locked=locked)
            full_zero_cost, full_paid, pending, charged_paid = (
                _project_capacity_inventory(locked,
                                            service_version=service_version,
                                            capacity_unit=config.capacity_unit,
                                            accounting_cards=cards,
                                            now=now,
                                            lane_projection=lane_projection))
            tail = _project_allocation_reserved_capacity(
                allocation,
                locked,
                service_hash=service_hash,
                service_version=service_version,
                accounting_cards=cards,
                now=now,
                config=config,
                lane_projection=lane_projection)
            economic_infos, economic_kueue, economic_digest = (
                _economic_capacity_graph_snapshot(
                    locked, lane_projection, service_version=service_version))
            policy, evidence_state, authenticated, eligible = (
                _reserved_supply_policy_and_evidence(
                    config,
                    allocation,
                    tail,
                    existing_zero_cost=full_zero_cost,
                    pending_zero_cost=pending))
            reservation_enabled = policy is not ReservedSupplyPolicy.DISABLED
            reservation_catalog = (() if not reservation_enabled else tuple(
                sorted(config.reserved_accelerators or cards)))
            gated = policy is ReservedSupplyPolicy.DEMAND_GATED
            demand_witness_scope_sha256 = (
                capacity_planning.build_demand_witness_scope_sha256(
                    service_name=service_name,
                    service_hash=service_hash,
                    service_lifecycle_epoch=service_lifecycle_epoch,
                    service_version=service_version,
                    demand_source_epoch=service['demand_source_epoch'],
                    fill_policy_sha256=config.fill_policy_sha256)
                if gated else '')
        return ReservedSupplyProjection(
            pending_zero_cost_capacity_by_accelerator=pending,
            allocation_reserved_capacity_by_accelerator=tail,
            economic_replica_infos=economic_infos,
            economic_kueue_capacity=economic_kueue,
            economic_capacity_graph_sha256=economic_digest,
            existing_zero_cost_capacity_by_accelerator=full_zero_cost,
            existing_paid_capacity_by_accelerator=full_paid,
            charged_paid_gpu_units=charged_paid,
            authenticated_capacity_by_accelerator=authenticated,
            eligible_capacity_by_accelerator=eligible,
            policy=policy,
            evidence_state=evidence_state,
            fill_policy_sha256=config.fill_policy_sha256,
            reservation_evidence_sha256=_reservation_evidence_sha256(
                config, allocation),
            demand_witness_scope_sha256=demand_witness_scope_sha256,
            allocation_demand_witness_sha256=(
                None if not gated or allocation is None else
                allocation.utilization_demand_witness_sha256),
            allocation_demonstrated_need=(
                None if not gated or allocation is None else
                allocation.utilization_demonstrated_need),
            allocation_ceiling=(0 if not gated or allocation is None else
                                allocation.utilization_ceiling),
            allocation_map=allocation,
            reserved_accelerators=reservation_catalog,
            allocation_bound=allocation is not None)

    @staticmethod
    def _write_plan_in_connection(
        connection: sqlalchemy.engine.Connection,
        plan: CapacityPlanInput,
        *,
        locked_history: _LockedPlanHistory,
        prior_claim_units: Mapping[str, int],
        full_zero_cost: Mapping[str, int],
        full_paid: Mapping[str, int],
        pending_zero_cost: Mapping[str, int],
        allocation_reserved: Mapping[str, int],
        decision_now: datetime.datetime,
        ttl_seconds: int,
        authority_valid_until: datetime.datetime,
    ) -> _WrittenCapacityPlan:
        """Persist one plan without reacquiring its prelocked history."""
        capacity_target = _canonical_counts(plan.capacity_target_by_accelerator,
                                            'capacity_target_by_accelerator')
        accounting_cards = set(capacity_target)
        watermark_sha256 = _sha256(_canonical_watermark(plan.receipt_watermark))
        previous = locked_history.previous
        duplicate_payload = None
        duplicate_digest = None
        if (previous is not None and
                previous['service_hash'] == plan.service_hash and
                previous['service_lifecycle_epoch']
                == plan.service_lifecycle_epoch and
                previous['service_version'] == plan.service_version and
                previous['demand_source_epoch'] == plan.demand_source_epoch):
            prior_paid_baseline = _subtract_counts(full_paid, prior_claim_units)
            duplicate_payload = plan.payload(
                existing_zero_cost_capacity_by_accelerator=full_zero_cost,
                pending_zero_cost_capacity_by_accelerator=pending_zero_cost,
                allocation_reserved_capacity_by_accelerator=(
                    allocation_reserved),
                existing_paid_capacity_by_accelerator=prior_paid_baseline)
            duplicate_digest = _sha256(duplicate_payload)
        duplicate = bool(previous is not None and
                         duplicate_digest == previous['content_sha256'])
        if duplicate:
            assert previous is not None
            generation = int(previous['generation'])
            payload = duplicate_payload
            digest = duplicate_digest
        else:
            payload = plan.payload(
                existing_zero_cost_capacity_by_accelerator=full_zero_cost,
                pending_zero_cost_capacity_by_accelerator=pending_zero_cost,
                allocation_reserved_capacity_by_accelerator=(
                    allocation_reserved),
                existing_paid_capacity_by_accelerator=full_paid)
            digest = _sha256(payload)
            maximum = locked_history.maximum_generation
            generation = 1 if maximum is None else maximum + 1
            connection.execute(
                sqlalchemy.insert(_PLANS).values(
                    service_name=plan.service_name,
                    generation=generation,
                    service_hash=plan.service_hash,
                    service_lifecycle_epoch=plan.service_lifecycle_epoch,
                    service_version=plan.service_version,
                    demand_source_epoch=plan.demand_source_epoch,
                    demand_feed_generation=plan.demand_feed_generation,
                    route_generation=plan.route_generation,
                    route_sha256=plan.route_sha256,
                    route_source_epoch=plan.route_source_epoch,
                    protocol_version=PROTOCOL_VERSION,
                    content_sha256=digest,
                    payload=payload,
                    created_at=decision_now))
        assert payload is not None and digest is not None
        valid_until = min(
            decision_now + datetime.timedelta(seconds=ttl_seconds),
            authority_valid_until)
        if valid_until <= decision_now:
            raise CapacityAdmissionConflict(
                'Capacity-plan source authority expired before publication.')
        head_insert = postgresql.insert(_HEADS).values(
            service_name=plan.service_name,
            generation=generation,
            demand_feed_generation=plan.demand_feed_generation,
            receipt_watermark_sha256=watermark_sha256,
            refreshed_at=decision_now,
            valid_until=valid_until)
        connection.execute(
            head_insert.on_conflict_do_update(
                index_elements=[_HEADS.c.service_name],
                set_={
                    'generation': generation,
                    'demand_feed_generation': plan.demand_feed_generation,
                    'receipt_watermark_sha256': watermark_sha256,
                    'refreshed_at': decision_now,
                    'valid_until': valid_until,
                }))
        # Capacity plans are operational fences, not an unbounded history
        # store.  The current head and the composite claim FK retain every
        # generation that can still authorize work; all other generations
        # are superseded and may be removed in this same transaction.
        connection.execute(
            sqlalchemy.delete(_PLANS).where(
                _PLANS.c.service_name == plan.service_name, _PLANS.c.generation
                != generation, ~sqlalchemy.exists().where(
                    _CLAIMS.c.service_name == _PLANS.c.service_name,
                    _CLAIMS.c.capacity_plan_generation == _PLANS.c.generation)))
        row = connection.execute(
            sqlalchemy.select(_PLANS).where(
                _PLANS.c.service_name == plan.service_name,
                _PLANS.c.generation == generation)).mappings().one()
        return _WrittenCapacityPlan(row=row, valid_until=valid_until)

    def plan_and_admit_current(
        self,
        *,
        service_name: str,
        service_hash: str,
        service_lifecycle_epoch: int,
        service_version: int,
        expected_controller_incarnation: uuid.UUID,
        expected_controller_owner_epoch: int,
        accounting_cards: Mapping[str, int],
        backend_num_nodes: int,
        sequenced_reserved_fill: bool,
        planner: Callable[[
            demand_state.DurableAutoscalingSnapshot, ReservedSupplyProjection |
            None
        ], CapacityPlanDecision],
        prepared_paid_launch_specs: Sequence[
            serve_paid_capacity.PaidLaunchSpec] = (),
        expected_planning_state_fingerprint: str | None = None,
        ttl_seconds: int = constants.CAPACITY_PLAN_TTL_SECONDS,
    ) -> CommittedCapacityPlan:
        """Plan and atomically admit from one PostgreSQL-linearized graph.

        Every potentially conflicting durable input is locked before
        ``planner`` runs.  The callback must therefore be bounded, in-memory,
        and free of database, provider, manager, network, or filesystem I/O.
        Demand reporters lock the service row before replacing a report, so a
        report is either part of this snapshot or the next generation.  The
        current reserved-fill allocation is likewise read only after its
        protocol and service locks are held; callers cannot nominate or fence
        the transaction against an optimistic allocation identity.
        """
        canonical_cards = _canonical_counts(accounting_cards,
                                            'accounting_cards')
        card_set = set(canonical_cards)
        if not card_set:
            raise ValueError('accounting_cards must not be empty.')
        if (any(width < 1 for width in canonical_cards.values()) or
                type(backend_num_nodes) is not int or  # pylint: disable=unidiomatic-typecheck
                backend_num_nodes < 1):
            raise ValueError('Exact paid backend shape must be positive.')
        if not isinstance(sequenced_reserved_fill, bool):
            raise ValueError('sequenced_reserved_fill must be boolean.')
        if (not isinstance(expected_controller_incarnation, uuid.UUID) or
                type(expected_controller_owner_epoch) is not int or  # pylint: disable=unidiomatic-typecheck
                expected_controller_owner_epoch < 1):
            raise ValueError('Expected controller authority is malformed.')
        if not callable(planner):
            raise ValueError('planner must be callable.')
        if (expected_planning_state_fingerprint is not None and
                not _SHA256_RE.fullmatch(expected_planning_state_fingerprint)):
            raise ValueError('expected_planning_state_fingerprint must be a '
                             'lowercase SHA-256 digest.')
        if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
            raise ValueError('ttl_seconds must be positive.')

        # Resolve lazy table modules before the transaction. Importing a
        # module while correctness locks are held can perform arbitrary I/O
        # and makes the lock duration depend on interpreter state.
        ordinary_associations = (
            ordinary_launch_binding.ordinary_launch_associations_table)
        request_rows_table = request_postgres_schema.REQUESTS
        request_queue_table = request_postgres_schema.QUEUE
        request_pins_table = request_postgres_schema.REQUEST_RETENTION_PINS

        with self.engine.begin() as connection:
            if sequenced_reserved_fill:
                # Allocation writers use this protocol mutex before any
                # service-local row.  Taking it even for a possible zero plan
                # lets current demand choose bound-positive versus unbound-zero
                # without changing lock order after the snapshot is known.
                serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
                    connection)
            service = connection.execute(
                sqlalchemy.select(_SERVICES).where(
                    _SERVICES.c.name ==
                    service_name).with_for_update()).mappings().one_or_none()
            if (service is None or service['hash'] != service_hash or
                    service['lifecycle_epoch'] != service_lifecycle_epoch or
                    service['current_version'] != service_version or
                    service['controller_incarnation']
                    != expected_controller_incarnation or
                    service['controller_owner_epoch']
                    != expected_controller_owner_epoch):
                raise CapacityAdmissionConflict(
                    'Service changed before current capacity planning.')
            try:
                current_service_status = serve_statuses.ServiceStatus(
                    str(service['status']))
            except ValueError as error:
                raise CapacityAdmissionConflict(
                    'Service status is malformed during capacity planning.'
                ) from error
            if current_service_status in (serve_statuses.ServiceStatus.
                                          replica_launch_blocking_statuses()):
                raise CapacityAdmissionConflict(
                    'Service no longer authorizes capacity admission.')
            fill_config = _reserved_fill_service_config_in_connection(
                connection, service)
            if fill_config.binding_required is not sequenced_reserved_fill:
                raise CapacityAdmissionConflict(
                    'Current capacity planner disagrees with the service '
                    'reserved-fill authority mode.')
            if (fill_config.capacity_unit
                    is reserved_fill_planner.FillCapacityUnit.LOGICAL and
                    backend_num_nodes != 1):
                raise CapacityAdmissionConflict(
                    'Logical capacity requires one-node paid backends.')
            prepared_specs, paid_replica_port = (
                _canonical_prepared_paid_launch_specs(
                    prepared_paid_launch_specs,
                    service=service,
                    service_name=service_name,
                    service_hash=service_hash,
                    service_lifecycle_epoch=service_lifecycle_epoch,
                    service_version=service_version,
                    accounting_cards=canonical_cards,
                    backend_num_nodes=backend_num_nodes,
                    locked_version=fill_config.paid_launch_version))

            locked_generation = connection.execute(
                sqlalchemy.select(_DEMAND_GENERATIONS.c.generation).where(
                    _DEMAND_GENERATIONS.c.service_name == service_name,
                    _DEMAND_GENERATIONS.c.service_hash ==
                    service_hash).with_for_update()).scalar_one_or_none()
            snapshot = demand_state.get_autoscaling_snapshot(
                service_name, service_hash, connection=connection)
            if (snapshot is None or
                    snapshot.demand_feed_generation != locked_generation):
                raise CapacityAdmissionConflict(
                    'Current durable demand is unavailable or inconsistent.')

            provisional_authority = (
                ReservedFillPlanAuthority.zero_revocation()
                if sequenced_reserved_fill else
                ReservedFillPlanAuthority.not_applicable())
            provisional = CapacityPlanInput(
                service_name=service_name,
                service_hash=service_hash,
                service_lifecycle_epoch=service_lifecycle_epoch,
                service_version=service_version,
                demand_source_epoch=snapshot.demand_source_epoch,
                demand_feed_generation=snapshot.demand_feed_generation,
                receipt_watermark=snapshot.receipt_watermark,
                route_generation=snapshot.route_generation,
                route_sha256=snapshot.route_sha256,
                route_source_epoch=snapshot.route_source_epoch,
                normalized_demand=snapshot.normalized_demand,
                capacity_target_by_accelerator={card: 0 for card in card_set},
                reserved_fill_authority=provisional_authority,
                paid_residual=capacity_planning.AcceleratorCapacity(),
                paid_launch_target=capacity_planning.AcceleratorCapacity())
            # This locks and validates the exact current reporter and route
            # rows.  The service lock already prevents their writers from
            # advancing, but keeping validation centralized avoids a second
            # interpretation of the promoted-demand contract.
            demand_sources = self._validate_sources(connection, provisional,
                                                    service)
            provisional = demand_sources.plan

            validated_allocation = None
            allocation_authority = None
            if sequenced_reserved_fill:
                try:
                    validated_allocation = (
                        reserved_fill_allocation.
                        ReservedFillAllocationRepository(
                            connection.engine).read_current_in_connection(
                                connection,
                                service_name,
                                service_hash, (service.get('controller_pid'),
                                               service.get('controller_ip')),
                                protocol_and_service_prelocked=True))
                except (TypeError, ValueError,
                        reserved_fill_allocation.ReservedFillAllocationError
                       ) as error:
                    raise CapacityAdmissionConflict(
                        'Current reserved-fill allocation could not be read '
                        'under the capacity-plan lock.') from error
                if validated_allocation is not None:
                    allocation_authority = ReservedFillPlanAuthority.bound(
                        validated_allocation.identity)

            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            locked_capacity = _lock_capacity_rows(connection,
                                                  service_name=service_name,
                                                  service_hash=service_hash,
                                                  now=now)
            if (expected_planning_state_fingerprint is not None and
                    _locked_planning_state_fingerprint(service, locked_capacity)
                    != expected_planning_state_fingerprint):
                raise CapacityAdmissionConflict(
                    'Prepared planning state changed before its rows were '
                    'locked.')
            lane_projection = _lock_kueue_projection(
                connection,
                service_name=service_name,
                service_hash=service_hash,
                service_lifecycle_epoch=service_lifecycle_epoch,
                service_version=service_version,
                accounting_cards=card_set,
                locked=locked_capacity)
            _require_recreated_logical_version_is_clean(service, fill_config,
                                                        locked_capacity,
                                                        lane_projection)
            full_zero_cost, full_paid, pending_zero_cost, charged_paid = (
                _project_capacity_inventory(
                    locked_capacity,
                    service_version=service_version,
                    capacity_unit=(fill_config.capacity_unit),
                    accounting_cards=card_set,
                    now=now,
                    lane_projection=lane_projection))
            allocation_reserved = _project_allocation_reserved_capacity(
                validated_allocation,
                locked_capacity,
                service_hash=service_hash,
                service_version=service_version,
                accounting_cards=card_set,
                now=now,
                config=fill_config,
                lane_projection=lane_projection)
            # The pure planner always receives the complete locked inventory.
            # Reservation fill being disabled removes only new reservation
            # authority.  It must not hide existing or already-committed
            # zero-cost capacity (including pending launches), or paid
            # replicas, and thereby manufacture a paid residual.
            economic_infos, economic_kueue, economic_digest = (
                _economic_capacity_graph_snapshot(
                    locked_capacity,
                    lane_projection,
                    service_version=service_version))
            policy, evidence_state, authenticated, eligible = (
                _reserved_supply_policy_and_evidence(
                    fill_config,
                    validated_allocation,
                    allocation_reserved,
                    existing_zero_cost=(full_zero_cost),
                    pending_zero_cost=(pending_zero_cost)))
            reservation_enabled = policy is not ReservedSupplyPolicy.DISABLED
            reservation_catalog = (() if not reservation_enabled else tuple(
                sorted(fill_config.reserved_accelerators or card_set)))
            gated = policy is ReservedSupplyPolicy.DEMAND_GATED
            demand_witness_scope_sha256 = (
                capacity_planning.build_demand_witness_scope_sha256(
                    service_name=service_name,
                    service_hash=service_hash,
                    service_lifecycle_epoch=service_lifecycle_epoch,
                    service_version=service_version,
                    demand_source_epoch=snapshot.demand_source_epoch,
                    fill_policy_sha256=fill_config.fill_policy_sha256)
                if gated else '')
            supply_projection = ReservedSupplyProjection(
                pending_zero_cost_capacity_by_accelerator=pending_zero_cost,
                allocation_reserved_capacity_by_accelerator=(
                    allocation_reserved if reservation_enabled else {}),
                economic_replica_infos=economic_infos,
                economic_kueue_capacity=economic_kueue,
                economic_capacity_graph_sha256=economic_digest,
                existing_zero_cost_capacity_by_accelerator=full_zero_cost,
                existing_paid_capacity_by_accelerator=full_paid,
                charged_paid_gpu_units=charged_paid,
                authenticated_capacity_by_accelerator=authenticated,
                eligible_capacity_by_accelerator=eligible,
                policy=policy,
                evidence_state=evidence_state,
                fill_policy_sha256=fill_config.fill_policy_sha256,
                reservation_evidence_sha256=(_reservation_evidence_sha256(
                    fill_config, validated_allocation)),
                demand_witness_scope_sha256=(demand_witness_scope_sha256),
                allocation_demand_witness_sha256=(
                    None if not gated or validated_allocation is None else
                    validated_allocation.utilization_demand_witness_sha256),
                allocation_demonstrated_need=(
                    None if not gated or validated_allocation is None else
                    validated_allocation.utilization_demonstrated_need),
                allocation_ceiling=(0 if not gated or
                                    validated_allocation is None else
                                    validated_allocation.utilization_ceiling),
                allocation_map=validated_allocation,
                reserved_accelerators=reservation_catalog,
                allocation_bound=validated_allocation is not None)

            existing_replica_ids = {
                int(row['replica_id']) for row in locked_capacity.replica_rows
            }
            existing_record_ids = {
                str(row['replica_state'].get('replica_record_id'))
                for row in locked_capacity.replica_rows
                if isinstance(row['replica_state'], Mapping)
            }
            if any(spec.replica_id in existing_replica_ids or
                   spec.replica_record_id in existing_record_ids
                   for spec in prepared_specs):
                raise CapacityAdmissionConflict(
                    'Prepared paid launch collides with a locked replica.')

            # The current head/plan precede every paid-pool row in the single
            # repository-wide lock order.  Never reacquire them after this.
            locked_history = _lock_plan_history_in_connection(
                connection, service_name)
            frontier_limit = (
                serve_paid_capacity.max_service_exploration_frontier(
                    workspace=service.get('workspace'),
                    service_name=service_name,
                    service_hash=service_hash))
            if prepared_specs and paid_replica_port is None:
                raise CapacityAdmissionConflict(
                    'Prepared paid launch has no locked replica port.')
            canonical_paid_replica_port = paid_replica_port or ''
            temporary_persistence_specs = [
                spec.persistence_spec(
                    priority=constants.LB_REQUEST_PRIORITY_MIN,
                    frontier_limit=frontier_limit,
                    replica_port=canonical_paid_replica_port,
                    planned_capacity=(
                        spec.gpu_units_per_node if fill_config.capacity_unit
                        is reserved_fill_planner.FillCapacityUnit.LOGICAL else
                        1),
                    created_at=None) for spec in prepared_specs
            ]
            serve_state._validate_paid_capacity_admission_inputs(  # pylint: disable=protected-access
                temporary_persistence_specs,
                service_limit=None,
                max_live_paid_gpu_units=fill_config.max_live_paid_gpu_units,
                frontier_default_limit=None,
                frontier_limits_by_key=None)
            upstream = serve_state._PaidCapacityAdmissionUpstreamContext(  # pylint: disable=protected-access
                service_hash=service_hash,
                service_version=service_version,
                max_live_paid_gpu_units=fill_config.max_live_paid_gpu_units)
            paid_census = serve_state._paid_capacity_admission_census_in_session(  # pylint: disable=protected-access
                connection,
                service_name,
                service_hash,
                temporary_persistence_specs,
                max_live_paid_gpu_units=fill_config.max_live_paid_gpu_units)
            if paid_census is None:
                raise CapacityAdmissionConflict(
                    'Locked paid capacity has no exact physical GPU census.')
            if paid_census.live_paid_gpu_units != charged_paid:
                raise CapacityAdmissionConflict(
                    'Planner and paid arbitration GPU censuses disagree.')
            paid_context = serve_state._lock_paid_capacity_admission_context_in_session(  # pylint: disable=protected-access
                connection,
                self.engine,
                service_name,
                temporary_persistence_specs,
                upstream=upstream,
                census=paid_census,
                base_limit=serve_paid_capacity.base_limit(),
                now=None)

            raw_claim_rows = connection.execute(
                sqlalchemy.select(
                    _CLAIMS.c.replica_id, _CLAIMS.c.service_hash).where(
                        _CLAIMS.c.service_name == service_name)).all()
            raw_waiter_rows = connection.execute(
                sqlalchemy.select(_PAID_WAITERS.c.service_hash).where(
                    _PAID_WAITERS.c.service_name == service_name)).all()
            validated_policy_head = _resolve_validated_format_6_head(
                history=locked_history,
                config=fill_config,
                snapshot=snapshot,
                service_name=service_name,
                service_hash=service_hash,
                service_lifecycle_epoch=service_lifecycle_epoch,
                service_version=service_version,
                accounting_cards=canonical_cards,
                backend_num_nodes=backend_num_nodes,
                locked_capacity=locked_capacity,
                lane_projection=lane_projection,
                allocation_reserved=allocation_reserved)
            has_validated_format_6_head = validated_policy_head is not None
            if validated_policy_head is None:
                prior_policy_state = None
                prior_candidate = None
            else:
                prior_policy_state, prior_candidate = validated_policy_head

            dependent_graph = _lock_association_authority_graph(
                connection,
                ordinary_associations=ordinary_associations,
                request_rows_table=request_rows_table,
                request_queue_table=request_queue_table,
                request_pins_table=request_pins_table,
                service_name=service_name,
                service_hash=service_hash,
                exhaustive_history_census=not has_validated_format_6_head,
                prepared_specs=prepared_specs,
                locked_capacity=locked_capacity,
                lane_projection=lane_projection)
            active_effect_rows = dependent_graph.active_association_rows
            if (any(str(row[1]) != service_hash for row in raw_claim_rows) or
                    any(str(row[0]) != service_hash for row in raw_waiter_rows)
                    or any(
                        str(row['service_hash']) != service_hash
                        for row in active_effect_rows)):
                raise CapacityAdmissionConflict(
                    'A retained authority graph belongs to another service '
                    'incarnation.')
            raw_claim_ids = {int(row[0]) for row in raw_claim_rows}
            association_replica_ids = {
                int(row['replica_id']) for row in active_effect_rows
            }
            association_record_ids = {
                str(row['replica_record_id'])
                for row in dependent_graph.association_rows
            }
            if any(spec.replica_id in raw_claim_ids or
                   spec.replica_id in association_replica_ids or
                   spec.replica_record_id in association_record_ids
                   for spec in prepared_specs):
                raise CapacityAdmissionConflict(
                    'Prepared paid launch collides with a retained authority '
                    'graph.')

            if not has_validated_format_6_head:
                prior_policy_state, prior_candidate = (
                    _resolve_locked_policy_history(
                        history=locked_history,
                        config=fill_config,
                        snapshot=snapshot,
                        service_name=service_name,
                        service_hash=service_hash,
                        service_lifecycle_epoch=service_lifecycle_epoch,
                        service_version=service_version,
                        accounting_cards=canonical_cards,
                        backend_num_nodes=backend_num_nodes,
                        locked_capacity=locked_capacity,
                        lane_projection=lane_projection,
                        allocation_reserved=allocation_reserved,
                        raw_claim_count=len(raw_claim_rows),
                        raw_waiter_count=len(raw_waiter_rows),
                        dependent_effect_count=(
                            len(active_effect_rows) +
                            dependent_graph.blocking_request_count +
                            len(dependent_graph.queue_rows) +
                            len(dependent_graph.pin_rows))))
            assert prior_policy_state is not None
            assert prior_candidate is not None
            planning_db_epoch = paid_context.transaction_now
            supply_projection = dataclasses.replace(
                supply_projection,
                prior_policy_state=prior_policy_state,
                prior_candidate=prior_candidate,
                planning_db_epoch=planning_db_epoch,
                max_live_paid_gpu_units=fill_config.max_live_paid_gpu_units)
            decision = planner(snapshot, supply_projection)
            if not isinstance(decision, CapacityPlanDecision):
                raise ValueError('planner returned no typed capacity decision.')
            capacity_target = decision.canonical_target(card_set)
            planner_snapshot, candidate = decision.decode_planner()
            if snapshot.fresh_aggregate_zero and any(capacity_target.values()):
                raise CapacityAdmissionConflict(
                    'Fresh aggregate zero produced a positive capacity target.')
            positive_target = any(capacity_target.values())
            reservation_commitment = _canonical_counts(
                decision.reserved_capacity_commitment_by_accelerator,
                'reserved_capacity_commitment_by_accelerator')
            reservation_commitment = {
                card: reservation_commitment.get(card, 0) for card in card_set
            }
            static_fill_target = _canonical_counts(
                decision.static_reserved_fill_target_by_accelerator,
                'static_reserved_fill_target_by_accelerator')
            static_fill_target = {
                card: static_fill_target.get(card, 0) for card in card_set
            }
            if sequenced_reserved_fill:
                assert supply_projection is not None
                locked_eligible = (
                    supply_projection.eligible_capacity_by_accelerator)
                if any(
                        reservation_commitment.get(card, 0) >
                        locked_eligible.get(card, 0) for card in card_set):
                    raise CapacityAdmissionConflict(
                        'Planner committed reservation capacity outside the '
                        'locked eligible envelope.')
                if any(static_fill_target.values()):
                    if (supply_projection.policy
                            is not ReservedSupplyPolicy.STATIC_PREFILL or
                            not supply_projection.allocation_bound or any(
                                static_fill_target.get(card, 0) >
                                locked_eligible.get(card, 0)
                                for card in card_set)):
                        raise CapacityAdmissionConflict(
                            'Planner static fill exceeds its locked ungated '
                            'reservation envelope.')
            elif (any(reservation_commitment.values()) or
                  any(static_fill_target.values())):
                raise ValueError('A non-fill planner committed reservations.')
            _validate_planner_against_locked_supply(
                planner_snapshot=planner_snapshot,
                candidate=candidate,
                service_version=service_version,
                accounting_cards=card_set,
                capacity_target=capacity_target,
                reservation_commitment=reservation_commitment,
                static_fill_target=static_fill_target,
                supply_projection=supply_projection,
                expected_planning_state_fingerprint=(
                    expected_planning_state_fingerprint))
            statically_incompatible_cards = None
            if (sequenced_reserved_fill and positive_target and
                    candidate.reservation_demand_relation is capacity_planning.
                    ReservationDemandRelation.STATICALLY_DISJOINT and
                    not any(reservation_commitment.values()) and
                    not any(static_fill_target.values())):
                positive_cards = tuple(
                    sorted(card for card, count in capacity_target.items()
                           if count > 0))
                candidate_disjoint_cards = tuple(
                    sorted(card.casefold() for card in
                           candidate.statically_disjoint_demand_accelerators))
                if (candidate_disjoint_cards != positive_cards or
                        fill_config.worker_projection_sha256 is None):
                    raise CapacityAdmissionConflict(
                        'Pure planner static incompatibility no longer matches '
                        'the locked worker projection or exact target cards.')
                statically_incompatible_cards = positive_cards
            if sequenced_reserved_fill:
                _require_demand_causal_allocation(
                    fill_config,
                    validated_allocation,
                    candidate,
                    positive_target=positive_target)
            if sequenced_reserved_fill and (positive_target or
                                            any(static_fill_target.values())):
                if statically_incompatible_cards is not None:
                    assert fill_config.worker_projection_sha256 is not None
                    final_authority = (
                        ReservedFillPlanAuthority.statically_incompatible(
                            statically_incompatible_cards,
                            fill_config.worker_projection_sha256))
                elif allocation_authority is not None:
                    final_authority = allocation_authority
                elif positive_target:
                    raise CapacityAdmissionConflict(
                        'Positive reservation-compatible demand has no exact '
                        'reserved allocation authority.')
                else:
                    raise CapacityAdmissionConflict(
                        'Positive capacity target has no exact reserved supply '
                        'authority.')
            elif sequenced_reserved_fill:
                final_authority = ReservedFillPlanAuthority.zero_revocation()
            else:
                final_authority = ReservedFillPlanAuthority.not_applicable()

            clipped = _clip_prepared_paid_admission(
                prepared_specs,
                candidate=candidate,
                decision=decision,
                frontier_limit=frontier_limit,
                replica_port=canonical_paid_replica_port,
                created_at=paid_context.transaction_now)
            temporary_index = {
                spec.candidate.replica_id: index
                for index, spec in enumerate(temporary_persistence_specs)
            }
            clipped_indices = tuple(temporary_index[item.launch_spec.replica_id]
                                    for item in clipped)
            clipped_census = serve_state._PaidCapacityAdmissionCensus(  # pylint: disable=protected-access
                service_claims=paid_census.service_claims,
                paid_gpu_units_by_index=tuple(
                    paid_census.paid_gpu_units_by_index[index]
                    for index in clipped_indices),
                live_paid_gpu_units=paid_census.live_paid_gpu_units)
            clipped_context = serve_state._LockedPaidCapacityAdmissionContext(  # pylint: disable=protected-access
                upstream=paid_context.upstream,
                census=clipped_census,
                pool_rows=paid_context.pool_rows,
                transaction_now=paid_context.transaction_now)
            clipped_persistence_specs = [
                item.persistence_spec for item in clipped
            ]
            paid_decision = serve_state._admit_replicas_with_paid_capacity_claims_in_session(  # pylint: disable=protected-access
                connection,
                self.engine,
                service_name,
                clipped_persistence_specs,
                locked_context=clipped_context,
                base_limit=serve_paid_capacity.base_limit(),
                max_limit=serve_paid_capacity.max_limit(),
                service_limit=None,
                success_ttl_seconds=(serve_paid_capacity.success_ttl_seconds()),
                failure_cooldown_seconds=(
                    serve_paid_capacity.failure_cooldown_seconds()),
                waiter_ttl_seconds=serve_paid_capacity.waiter_ttl_seconds(),
                frontier_default_limit=frontier_limit)
            if paid_decision.existing_indices:
                raise CapacityAdmissionConflict(
                    'Fresh prepared paid launch resolved to an existing claim.')
            accepted = tuple(
                clipped[index] for index in paid_decision.accepted_indices)
            accepted_plan_units: dict[str, int] = {}
            accepted_paid_gpu_units = 0
            for item in accepted:
                card = item.launch_spec.accelerator
                accepted_plan_units[card] = (accepted_plan_units.get(card, 0) +
                                             item.plan_units)
                accepted_paid_gpu_units += item.physical_gpu_units
            decision_now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            supply_valid_until = _locked_supply_authority_valid_until(
                allocation=validated_allocation,
                allocation_authorizing=(
                    final_authority.mode
                    is ReservedFillPlanAuthorityMode.ALLOCATION_BOUND),
                locked_capacity=locked_capacity,
                lane_projection=lane_projection)
            paid_valid_until = (None
                                if paid_decision.authority_valid_until_epoch
                                is None else datetime.datetime.fromtimestamp(
                                    paid_decision.authority_valid_until_epoch,
                                    datetime.timezone.utc))
            authority_horizons = [demand_sources.valid_until]
            authority_horizons.extend(
                horizon for horizon in (supply_valid_until, paid_valid_until)
                if horizon is not None)
            authority_valid_until = min(authority_horizons)
            if decision_now >= authority_valid_until:
                raise CapacityAdmissionConflict(
                    'Capacity source authority expired during arbitration.')
            candidate = capacity_planning.finalize_capacity_plan(
                planner_snapshot,
                candidate,
                accepted_paid_plan_units=(capacity_planning.AcceleratorCapacity.
                                          from_mapping(accepted_plan_units)),
                accepted_paid_gpu_units=accepted_paid_gpu_units,
                decision_db_epoch=decision_now.timestamp())

            normalized_demand = dict(snapshot.normalized_demand)
            if set(normalized_demand) & set(
                    decision.normalized_demand_extensions):
                raise ValueError('Planner extensions overwrite demand '
                                 'authority fields.')
            normalized_demand.update(decision.normalized_demand_extensions)
            bound_projection = (
                supply_projection if final_authority.mode
                is ReservedFillPlanAuthorityMode.ALLOCATION_BOUND else None)
            plan = CapacityPlanInput(
                service_name=service_name,
                service_hash=service_hash,
                service_lifecycle_epoch=service_lifecycle_epoch,
                service_version=service_version,
                demand_source_epoch=snapshot.demand_source_epoch,
                demand_feed_generation=snapshot.demand_feed_generation,
                receipt_watermark=snapshot.receipt_watermark,
                route_generation=snapshot.route_generation,
                route_sha256=snapshot.route_sha256,
                route_source_epoch=snapshot.route_source_epoch,
                normalized_demand=normalized_demand,
                capacity_target_by_accelerator=capacity_target,
                reserved_fill_authority=final_authority,
                paid_residual=candidate.paid_residual,
                paid_launch_target=candidate.paid_launch_target,
                allocation_reserved_capacity_by_accelerator=(
                    reservation_commitment),
                expected_pending_zero_cost_capacity_by_accelerator=(
                    {} if bound_projection is None else
                    bound_projection.pending_zero_cost_capacity_by_accelerator),
                expected_economic_capacity_graph_sha256=(
                    None if supply_projection is None else
                    supply_projection.economic_capacity_graph_sha256),
                planner_payload=capacity_planning.planner_envelope(
                    planner_snapshot, candidate))
            # Arbitration has now removed every stale retained claim while
            # preserving the complete prelocked pool/claim set. Compute the
            # duplicate-plan baseline only from claims that still exist.
            prior_claim_units = {card: 0 for card in card_set}
            if locked_history.previous is not None:
                prior_claim_units = _claim_units_for_plan(
                    connection,
                    service_name=service_name,
                    service_hash=service_hash,
                    generation=int(locked_history.previous['generation']),
                    accounting_cards=card_set)
            written = self._write_plan_in_connection(
                connection,
                plan,
                locked_history=locked_history,
                prior_claim_units=prior_claim_units,
                full_zero_cost=full_zero_cost,
                full_paid=full_paid,
                pending_zero_cost=pending_zero_cost,
                allocation_reserved=reservation_commitment,
                decision_now=decision_now,
                ttl_seconds=ttl_seconds,
                authority_valid_until=authority_valid_until)
            committed_authority = _validate_committed_plan_row(
                written.row,
                expected_snapshot=planner_snapshot,
                expected_candidate=candidate,
                accounting_cards=card_set,
                demand_feed_generation=snapshot.demand_feed_generation)
            supplied_claims = {
                item.launch_spec.replica_id: committed_authority.claim_values(
                    item.launch_spec.accelerator,
                    item.plan_units) for item in accepted
            }
            claims = serve_state._capacity_plan_claims_for_paid_admission(  # pylint: disable=protected-access
                clipped_persistence_specs, paid_decision, supplied_claims)
            if not serve_state._persist_paid_capacity_admission_in_session(  # pylint: disable=protected-access
                    connection,
                    self.engine,
                    service_name,
                    clipped_persistence_specs,
                    paid_decision,
                    locked_context=clipped_context,
                    capacity_plan_claims_by_replica_id=claims):
                raise CapacityAdmissionConflict(
                    'Prepared paid launch identity changed during admission.')
            paid_launch_receipt = _read_paid_launch_receipt(
                connection,
                authority=committed_authority,
                service_lifecycle_epoch=service_lifecycle_epoch,
                service_version=service_version,
                accepted=accepted)
            _postwrite_revalidate_current_admission(
                connection,
                service_before=service,
                snapshot=snapshot,
                plan=plan,
                valid_until=written.valid_until)
            committed = CommittedCapacityPlan(
                authority=committed_authority,
                demand_snapshot=snapshot,
                planner_snapshot=planner_snapshot,
                candidate=candidate,
                allocation_map=validated_allocation,
                paid_launch_receipt=paid_launch_receipt)
            witness_semantic_sha256 = candidate.demand_witness_sha256
            assert witness_semantic_sha256 is not None
        _notify_fill_demand_witness(service_name, witness_semantic_sha256)
        return committed


def _validate_committed_paid_claim_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    claim_plan: Mapping[str, Any],
    *,
    claim_demand_generation: int,
    claim_source_epoch: int,
    claim_accelerator: str,
    claim_units: int,
    planner_candidate: capacity_planning.CapacityPlanCandidate,
    now: datetime.datetime,
) -> datetime.datetime:
    """Validate immutable post-admission authority against current routing.

    Replica, claim, paid-pool debit, association, and execution-claim identity
    are locked by the provider-effect caller.  Demand reports, load-balancer
    role heartbeats, and economic planning are intentionally absent here: they
    authorized that atomic commit and may change or become temporarily
    unavailable afterwards, but cannot revoke its one first provider effect.
    """
    current_demand_generation = connection.execute(
        sqlalchemy.select(_DEMAND_GENERATIONS.c.generation).where(
            _DEMAND_GENERATIONS.c.service_name == service['name'],
            _DEMAND_GENERATIONS.c.service_hash ==
            service['hash']).with_for_update()).scalar_one_or_none()
    route_head = connection.execute(
        sqlalchemy.select(_ROUTE_HEADS).where(
            _ROUTE_HEADS.c.service_name ==
            service['name']).with_for_update()).mappings().one_or_none()
    route = (None if route_head is None else connection.execute(
        sqlalchemy.select(_ROUTE_SNAPSHOTS).where(
            _ROUTE_SNAPSHOTS.c.service_name == service['name'],
            _ROUTE_SNAPSHOTS.c.generation
            == route_head['generation'])).mappings().one_or_none())
    if (claim_plan['service_hash'] != service['hash'] or
            claim_plan['service_lifecycle_epoch'] != service['lifecycle_epoch']
            or claim_plan['service_version'] != service['current_version'] or
            claim_plan['demand_source_epoch'] != claim_source_epoch or
            current_demand_generation is None or
            current_demand_generation < claim_demand_generation or
            service.get('demand_source_mode')
            != DemandSourceMode.DURABLE_FEED.value or
            service.get('demand_source_epoch') != claim_source_epoch or
            service.get('demand_authority_capable') is not True or
            service.get('demand_authority_controller_incarnation')
            != service.get('controller_incarnation') or
            service.get('demand_authority_protocol_version') != PROTOCOL_VERSION
            or service.get('route_source_mode') != 'DURABLE_PROJECTED' or
            service.get('route_projection_capable') is not True or
            service.get('route_projection_controller_incarnation')
            != service.get('controller_incarnation') or
            service.get('route_projection_protocol_version') not in (1, 2) or
            service.get('route_source_epoch')
            != claim_plan['route_source_epoch'] or route_head is None or
            route is None or route_head['valid_until'] <= now or
            route_head['generation'] < claim_plan['route_generation'] or
            route['service_hash'] != service['hash'] or
            route['service_lifecycle_epoch'] != service['lifecycle_epoch'] or
            route['service_version'] != service['current_version'] or
            route['controller_incarnation'] != service['controller_incarnation']
            or route['protocol_version'] != PROTOCOL_VERSION or
            route['producer_protocol_version']
            != service.get('route_projection_protocol_version')):
        raise CapacityAdmissionConflict(
            'Committed paid claim lost lifecycle or route authority.')
    try:
        route_projection.RouteProjectionRepository.validate_snapshot_row(route)
    except route_projection.RouteProjectionError as error:
        raise CapacityAdmissionConflict(
            'Paid claim route projection is corrupt.') from error
    if not route_projection.snapshot_owner_matches(route, service):
        raise CapacityAdmissionConflict(
            'Paid claim route projection belongs to a different owner.')

    claim_plan_payload = claim_plan['payload']
    plan_source = claim_plan_payload.get('source')
    if (not isinstance(plan_source, Mapping) or
            plan_source.get('route_generation')
            != claim_plan.get('route_generation') or
            plan_source.get('route_sha256') != claim_plan.get('route_sha256') or
            plan_source.get('route_source_epoch')
            != claim_plan.get('route_source_epoch')):
        raise CapacityAdmissionConflict(
            'Committed paid claim has an inconsistent admitted route.')
    try:
        authorized_paid = _canonical_counts(
            claim_plan_payload.get('paid_launch_target_by_accelerator', {}),
            'paid_launch_target_by_accelerator')
    except (AttributeError, ValueError) as error:
        raise CapacityAdmissionConflict(
            'Committed paid claim debit ledger is malformed.') from error
    accounting_cards = set(authorized_paid)
    if (claim_accelerator not in accounting_cards or
            authorized_paid[claim_accelerator] < claim_units):
        raise CapacityAdmissionConflict(
            'Committed paid claim exceeds its immutable plan debit.')
    claim_projection = _plan_claim_projection(
        connection,
        service_name=str(service['name']),
        service_hash=str(service['hash']),
        generation=int(claim_plan['generation']),
        accounting_cards=accounting_cards,
        capacity_unit=planner_candidate.capacity_unit,
        candidate=planner_candidate)
    claimed_units = claim_projection.units_by_accelerator
    if (claimed_units.get(claim_accelerator, 0) < claim_units or
            any(claimed_units[card] > authorized_paid[card]
                for card in accounting_cards)):
        raise CapacityAdmissionConflict(
            'Committed paid claims exceed their immutable plan debit.')

    final_now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    paid_fresh_until = route_head['valid_until']
    if paid_fresh_until <= final_now:
        raise CapacityAdmissionConflict(
            'Paid claim freshness expired while validation waited.')
    return paid_fresh_until


def _validate_planner_claim_pool_shape(
    claim: Mapping[str, Any],
    candidate: capacity_planning.CapacityPlanCandidate,
    accelerator: str,
) -> None:
    """Bind one claim's relational pool identity to immutable task shape."""
    relational_pool_key = claim.get('paid_capacity_pool_key')
    claim_pool_key = claim.get('pool_key')
    if (relational_pool_key is not None and claim_pool_key is not None and
            relational_pool_key != claim_pool_key):
        raise CapacityAdmissionConflict(
            'Paid claim row and replica name different provider pools.')
    pool_key = (relational_pool_key
                if relational_pool_key is not None else claim_pool_key)
    if not isinstance(pool_key, str) or not pool_key:
        raise CapacityAdmissionConflict(
            'Planner-bound paid claim has no exact provider pool identity.')
    _planner_bound_pool_shape(pool_key, candidate, accelerator)


def validate_paid_claim_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    claim: Mapping[str, Any],
    *,
    prospective: bool = False,
    require_planner: bool = True,
    protocol_and_service_prelocked: bool = False,
    _batch_member_units: tuple[int, ...] | None = None,
    _batch_member_pool_keys: tuple[str, ...] | None = None,
) -> datetime.datetime | None:
    """Revalidate one planner-bound claim before provider I/O."""
    fields = ('capacity_plan_generation', 'capacity_plan_sha256',
              'demand_feed_generation', 'demand_source_epoch',
              'capacity_plan_accelerator', 'capacity_plan_units')
    if any(claim.get(field) is None for field in fields):
        if (require_planner and service.get('demand_source_mode')
                == DemandSourceMode.DURABLE_FEED.value):
            raise CapacityAdmissionConflict(
                'Durable-demand service retained an unbound paid claim.')
        return None
    # Legacy controller-sourced admission remains supported by the local
    # controller SQLite catalog, whose migration head intentionally predates
    # Serve050.  Only a complete planner tuple crosses the PostgreSQL-only
    # ordered-admission boundary.
    _require_postgres(connection)
    try:
        generation = _positive_int(claim['capacity_plan_generation'],
                                   'capacity_plan_generation')
        claim_demand_generation = _positive_int(claim['demand_feed_generation'],
                                                'demand_feed_generation')
        claim_source_epoch = _positive_int(claim['demand_source_epoch'],
                                           'demand_source_epoch')
        claim_units = _positive_int(claim['capacity_plan_units'],
                                    'capacity_plan_units')
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Paid claim planner tuple is malformed.') from error
    claim_sha256 = claim['capacity_plan_sha256']
    accelerator = claim['capacity_plan_accelerator']
    if (not isinstance(claim_sha256, str) or
            _SHA256_RE.fullmatch(claim_sha256) is None or
            not isinstance(accelerator, str) or not accelerator):
        raise CapacityAdmissionConflict(
            'Paid claim planner tuple is malformed.')
    now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    claim_plan = connection.execute(
        sqlalchemy.select(_PLANS).where(
            _PLANS.c.service_name == service['name'],
            _PLANS.c.generation == generation)).mappings().one_or_none()
    if (claim_plan is None or claim_plan['service_hash'] != service['hash'] or
            claim_plan['content_sha256'] != claim_sha256 or
            claim_plan['service_lifecycle_epoch'] != service['lifecycle_epoch']
            or claim_plan['service_version'] != service['current_version'] or
            claim_plan['demand_source_epoch'] != claim_source_epoch or
            claim_plan['protocol_version'] != PROTOCOL_VERSION):
        raise CapacityAdmissionConflict(
            'Paid claim lost its current fresh capacity-plan authority.')
    claim_plan_payload = claim_plan['payload']
    if (not isinstance(claim_plan_payload, Mapping) or
            _sha256(claim_plan_payload) != claim_sha256):
        raise CapacityAdmissionConflict(
            'Capacity plan digest no longer matches its payload.')
    try:
        planner_snapshot, planner_candidate = _decode_planner_payload(
            claim_plan_payload.get('planner'))
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Capacity plan has no immutable backend claim shape.') from error
    if (prospective and service.get('demand_source_mode')
            == DemandSourceMode.DURABLE_FEED.value):
        # Format-6 policy memory and its accepted effect rows are one atomic
        # graph. A later prospective validator cannot safely spend that
        # committed candidate again; only plan_and_admit_current may insert a
        # fresh claim. Committed recovery/provider checks remain valid below.
        raise CapacityAdmissionConflict(
            'Format-6 paid claims require fused plan admission.')
    if _batch_member_pool_keys is None:
        _validate_planner_claim_pool_shape(claim, planner_candidate,
                                           accelerator)
    else:
        for member_pool_key in _batch_member_pool_keys:
            _planner_bound_pool_shape(member_pool_key, planner_candidate,
                                      accelerator)
    if not prospective:
        return _validate_committed_paid_claim_in_connection(
            connection,
            service,
            claim_plan,
            claim_demand_generation=claim_demand_generation,
            claim_source_epoch=claim_source_epoch,
            claim_accelerator=accelerator,
            claim_units=claim_units,
            planner_candidate=planner_candidate,
            now=now)
    head = connection.execute(
        sqlalchemy.select(_HEADS).where(
            _HEADS.c.service_name ==
            service['name']).with_for_update()).mappings().one_or_none()
    if head is None:
        raise CapacityAdmissionConflict(
            'Paid claim lost its current fresh capacity-plan authority.')
    plan = claim_plan
    validation_generation = generation
    payload = plan['payload']
    if not isinstance(payload, Mapping):
        raise CapacityAdmissionConflict('Capacity plan payload is malformed.')
    if _sha256(payload) != plan['content_sha256']:
        raise CapacityAdmissionConflict(
            'Capacity plan digest no longer matches its payload.')
    reserved_fill_authority_present = 'reserved_fill_authority' in payload
    raw_reserved_fill_authority = payload.get('reserved_fill_authority')
    fill_config = _reserved_fill_service_config_in_connection(
        connection, service)
    try:
        plan_reserved_fill_authority = (
            ReservedFillPlanAuthority.from_mapping(raw_reserved_fill_authority))
    except ValueError as error:
        reserved_fill_binding_required = fill_config.binding_required
        if (reserved_fill_binding_required or reserved_fill_authority_present):
            raise CapacityAdmissionConflict(
                'Paid claim has no valid reserved-fill plan authority.'
            ) from error
        # Plans published before the allocation-binding contract may finish
        # only while this exact elected service version does not require a
        # binding. Enabling fill under DURABLE_INTENT makes the same bytes fail
        # closed above; a fill-disabled durable birth remains compatible.
        plan_reserved_fill_authority = (
            ReservedFillPlanAuthority.not_applicable())
    else:
        reserved_fill_binding_required = fill_config.binding_required
    try:
        plan_capacity_target = _canonical_counts(
            payload.get('capacity_target_by_accelerator', {}),
            'capacity_target_by_accelerator')
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Paid claim capacity target is malformed.') from error
    positive_target_cards = tuple(
        sorted(
            card for card, count in plan_capacity_target.items() if count > 0))
    if (plan_reserved_fill_authority.mode
            is ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE and
            plan_reserved_fill_authority.incompatible_accelerators
            != positive_target_cards):
        raise CapacityAdmissionConflict(
            'Paid claim static incompatibility changed target cards.')
    validated_allocation = _validate_reserved_fill_authority_in_connection(
        connection,
        service,
        plan_reserved_fill_authority,
        reserved_fill_binding_required=reserved_fill_binding_required,
        protocol_and_service_prelocked=protocol_and_service_prelocked)
    current_demand_generation = connection.execute(
        sqlalchemy.select(_DEMAND_GENERATIONS.c.generation).where(
            _DEMAND_GENERATIONS.c.service_name == service['name'],
            _DEMAND_GENERATIONS.c.service_hash ==
            service['hash'])).scalar_one_or_none()
    route_head = connection.execute(
        sqlalchemy.select(_ROUTE_HEADS).where(
            _ROUTE_HEADS.c.service_name ==
            service['name']).with_for_update()).mappings().one_or_none()
    route = (None if route_head is None else connection.execute(
        sqlalchemy.select(_ROUTE_SNAPSHOTS).where(
            _ROUTE_SNAPSHOTS.c.service_name == service['name'],
            _ROUTE_SNAPSHOTS.c.generation
            == route_head['generation'])).mappings().one_or_none())
    if (head['generation'] != validation_generation or
            head['valid_until'] <= now or
            plan['service_hash'] != service['hash'] or
            plan['content_sha256'] != claim_sha256 or
            plan['protocol_version'] != PROTOCOL_VERSION or
            current_demand_generation is None or
            plan['demand_feed_generation'] > claim_demand_generation or
            head['demand_feed_generation'] > current_demand_generation or
            head['demand_feed_generation'] < claim_demand_generation or
            plan['demand_source_epoch'] != claim_source_epoch or
            plan['service_lifecycle_epoch'] != service['lifecycle_epoch'] or
            plan['service_version'] != service['current_version'] or
            route_head is None or route is None or
            route_head['valid_until'] <= now or
            route_head['generation'] != plan['route_generation'] or
            route['content_sha256'] != plan['route_sha256'] or
            route['service_hash'] != service['hash'] or
            route['service_lifecycle_epoch'] != service['lifecycle_epoch'] or
            route['service_version'] != service['current_version'] or
            route['controller_incarnation'] != service['controller_incarnation']
            or route['protocol_version'] != PROTOCOL_VERSION or
            route['producer_protocol_version']
            != service.get('route_projection_protocol_version') or
            service.get('demand_source_mode')
            != DemandSourceMode.DURABLE_FEED.value or
            service.get('demand_source_epoch') != claim_source_epoch or
            service.get('demand_authority_capable') is not True or
            service.get('demand_authority_controller_incarnation')
            != service.get('controller_incarnation') or
            service.get('demand_authority_protocol_version') != PROTOCOL_VERSION
            or service.get('route_source_mode') != 'DURABLE_PROJECTED' or
            service.get('route_source_epoch') != plan['route_source_epoch'] or
            service.get('route_projection_capable') is not True or
            service.get('route_projection_controller_incarnation')
            != service.get('controller_incarnation') or
            service.get('route_projection_protocol_version') not in (1, 2)):
        raise CapacityAdmissionConflict(
            'Paid claim lost its current fresh capacity-plan authority.')
    try:
        route_projection.RouteProjectionRepository.validate_snapshot_row(route)
    except route_projection.RouteProjectionError as error:
        raise CapacityAdmissionConflict(
            'Paid claim route projection is corrupt.') from error
    if not route_projection.snapshot_owner_matches(route, service):
        raise CapacityAdmissionConflict(
            'Paid claim route projection belongs to a different owner.')
    fresh_reports = connection.execute(
        sqlalchemy.select(_DEMAND_REPORTS).where(
            _DEMAND_REPORTS.c.service_name == service['name'],
            _DEMAND_REPORTS.c.service_hash == service['hash'],
            _DEMAND_REPORTS.c.valid_until
            > now).order_by(_DEMAND_REPORTS.c.reporter_session_id).
        with_for_update()).mappings().all()
    selected_reports = demand_state.current_demand_report_rows(
        fresh_reports, service)
    if selected_reports is None:
        raise CapacityAdmissionConflict(
            'Paid claim lost its current load balancer.')
    fresh_reports = selected_reports
    current_watermark = [{
        'reporter_session_id': row['reporter_session_id'],
        'sequence': int(row['sequence']),
        'payload_sha256': row['payload_sha256'],
    } for row in fresh_reports]
    current_watermark_sha256 = (_sha256(current_watermark)
                                if current_watermark else None)
    if (not current_watermark or
        (head['demand_feed_generation'] == current_demand_generation and
         current_watermark_sha256 != head['receipt_watermark_sha256']) or
            any(row['complete'] is not True or row['protocol_version'] != 2
                for row in fresh_reports)):
        raise CapacityAdmissionConflict(
            'Paid claim lost its fresh demand receipt watermark.')
    if (planner_snapshot.deadline is not None and
        (head['demand_feed_generation'] != current_demand_generation or
         current_watermark_sha256 != head['receipt_watermark_sha256'])):
        # Countdown-only heartbeats may retain an older fill witness as a
        # monotonic free-capacity lower bound. They never extend that plan's
        # prospective paid/provider lease: every new debit must come from a
        # fresh planner run under the current PostgreSQL demand lock.
        raise CapacityAdmissionConflict(
            'Deadline demand advanced before paid claim admission; a fresh '
            'locked planner run is required.')
    route_context = demand_state.validate_report_route_contexts(
        connection, service, fresh_reports, route_head, route, now)
    if (route_context is None or
            route_context.generation != plan['route_generation'] or
            route_context.content_sha256 != plan['route_sha256'] or
            route_context.source_epoch != plan['route_source_epoch']):
        raise CapacityAdmissionConflict(
            'Paid claim demand receipts no longer match its fresh route '
            'context.')
    # This is the prospective boundary: the current plan head is the bounded
    # spend lease. Ingest serializes on the already-locked service row, so this
    # reconstruction must still prove structurally coherent current reports and
    # the exact route. Mutable positive demand counts do not revoke an unexpired
    # head while a provider-free candidate wave is prepared. A complete fresh
    # zero report does revoke it immediately.
    current_snapshot = demand_state.get_autoscaling_snapshot(
        str(service['name']), str(service['hash']), connection=connection)
    snapshot_inconsistent = bool(
        current_snapshot is None or
        current_snapshot.service_name != service['name'] or
        current_snapshot.service_hash != service['hash'] or
        current_snapshot.demand_source_epoch != claim_source_epoch or
        current_snapshot.demand_feed_generation != current_demand_generation or
        current_snapshot.receipt_watermark != current_watermark or
        current_snapshot.route_generation != plan['route_generation'] or
        current_snapshot.route_sha256 != plan['route_sha256'] or
        current_snapshot.route_source_epoch != plan['route_source_epoch'])
    if snapshot_inconsistent:
        raise CapacityAdmissionConflict(
            'Paid claim demand evidence is unavailable before admission.')
    if demand_state.reports_prove_fresh_aggregate_zero(fresh_reports):
        raise CapacityAdmissionConflict(
            'Fresh aggregate zero revoked paid claim admission.')
    try:
        capacity_target = _canonical_counts(
            payload.get('capacity_target_by_accelerator', {}),
            'capacity_target_by_accelerator')
        baseline_zero = _canonical_counts(
            payload.get('existing_zero_cost_capacity_by_accelerator', {}),
            'existing_zero_cost_capacity_by_accelerator')
        raw_pending_zero = payload.get(
            'pending_zero_cost_capacity_by_accelerator')
        if raw_pending_zero is None:
            # Serve050 plans predate grant-before-row admission. They carry no
            # pending intents, so their additive Serve052 interpretation is an
            # explicit all-zero map over the existing accounting classes.
            raw_pending_zero = {card: 0 for card in capacity_target}
        baseline_pending_zero = _canonical_counts(
            raw_pending_zero, 'pending_zero_cost_capacity_by_accelerator')
        raw_allocation_reserved = payload.get(
            'allocation_reserved_capacity_by_accelerator')
        if raw_allocation_reserved is None:
            if validated_allocation is not None:
                raise ValueError(
                    'Allocation-bound plan predates allocation-tail '
                    'accounting.')
            raw_allocation_reserved = {card: 0 for card in capacity_target}
        baseline_allocation_reserved = _canonical_counts(
            raw_allocation_reserved,
            'allocation_reserved_capacity_by_accelerator')
        baseline_paid = _canonical_counts(
            payload.get('existing_paid_capacity_by_accelerator', {}),
            'existing_paid_capacity_by_accelerator')
        paid = _canonical_counts(
            payload.get('paid_residual_by_accelerator', {}),
            'paid_residual_by_accelerator')
        paid_launch = _canonical_counts(
            payload.get('paid_launch_target_by_accelerator', {}),
            'paid_launch_target_by_accelerator')
        economic_graph_sha256 = payload.get('economic_capacity_graph_sha256')
        if validated_allocation is not None and (
                not isinstance(economic_graph_sha256, str) or
                _SHA256_RE.fullmatch(economic_graph_sha256) is None):
            raise ValueError(
                'Allocation-bound plan lacks its economic capacity graph.')
        if (economic_graph_sha256 is not None and
            (not isinstance(economic_graph_sha256, str) or
             _SHA256_RE.fullmatch(economic_graph_sha256) is None)):
            raise ValueError('Capacity plan has a malformed economic capacity '
                             'graph.')
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Capacity plan accounting is malformed.') from error
    accounting_cards = set(capacity_target)
    if (not accounting_cards or set(baseline_zero) != accounting_cards or
            set(baseline_pending_zero) != accounting_cards or
            set(baseline_allocation_reserved) != accounting_cards or
            set(baseline_paid) != accounting_cards or
            set(paid) - accounting_cards or
            set(paid_launch) - accounting_cards):
        raise CapacityAdmissionConflict(
            'Capacity plan accounting classes are inconsistent.')
    planner_snapshot, planner_candidate = (
        _validate_prospective_planner_candidate(
            payload,
            service_version=int(service['current_version']),
            demand_feed_generation=int(plan['demand_feed_generation']),
            accounting_cards=accounting_cards,
            capacity_target=capacity_target,
            existing_zero_cost=baseline_zero,
            pending_zero_cost=baseline_pending_zero,
            allocation_reserved=baseline_allocation_reserved,
            existing_paid=baseline_paid,
            paid_residual=paid,
            paid_launch_target=paid_launch))
    launch_width = (
        1 if planner_candidate.capacity_unit
        is capacity_planning.CapacityUnit.PHYSICAL_BACKEND else
        planner_candidate.physical_gpu_width_by_accelerator.get(accelerator))
    member_units = ((claim_units,)
                    if _batch_member_units is None else _batch_member_units)
    if (launch_width <= 0 or not member_units or
            sum(member_units) != claim_units or any(
                type(units) is not int or units != launch_width
                for units in member_units)):
        raise CapacityAdmissionConflict(
            'Paid claim does not debit exact whole-backend capacity.')
    locked_capacity = _lock_capacity_rows(connection,
                                          service_name=service['name'],
                                          service_hash=service['hash'],
                                          now=now)
    lane_projection = _lock_kueue_projection(
        connection,
        service_name=service['name'],
        service_hash=service['hash'],
        service_lifecycle_epoch=int(service['lifecycle_epoch']),
        service_version=int(service['current_version']),
        accounting_cards=accounting_cards,
        locked=locked_capacity)
    if economic_graph_sha256 is not None:
        _, _, current_economic_digest = _economic_capacity_graph_snapshot(
            locked_capacity,
            lane_projection,
            service_version=int(service['current_version']))
        if current_economic_digest != economic_graph_sha256:
            raise CapacityAdmissionConflict(
                'Economic supply or scheduler capacity changed after the '
                'ordered plan snapshot.')
    current_zero, current_paid, current_pending_zero, current_charged_paid = (
        _project_capacity_inventory(locked_capacity,
                                    service_version=int(
                                        service['current_version']),
                                    capacity_unit=fill_config.capacity_unit,
                                    accounting_cards=accounting_cards,
                                    now=now,
                                    lane_projection=lane_projection))
    current_allocation_reserved = _project_allocation_reserved_capacity(
        validated_allocation,
        locked_capacity,
        service_hash=str(service['hash']),
        service_version=int(service['current_version']),
        accounting_cards=accounting_cards,
        now=now,
        config=fill_config,
        lane_projection=lane_projection)
    (current_reservation_policy, current_reservation_evidence,
     current_authenticated_reserved,
     current_eligible_reserved) = _reserved_supply_policy_and_evidence(
         fill_config,
         validated_allocation,
         current_allocation_reserved,
         existing_zero_cost=current_zero,
         pending_zero_cost=current_pending_zero)
    if (plan_reserved_fill_authority.mode
            is ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE):
        # Exact disjoint demand is bound to the immutable worker projection,
        # not to unrelated broker allocation generations or usage-gate
        # evidence for other cards.  The helper still validates every stable
        # planner/policy dimension before this allocation-dependent check is
        # omitted.
        _validate_static_disjoint_prospective_authority(
            planner_snapshot,
            planner_candidate,
            plan_reserved_fill_authority,
            accounting_cards=accounting_cards,
            capacity_target=capacity_target,
            fill_config=fill_config,
            policy=current_reservation_policy)
    else:
        _validate_prospective_reservation_evidence(
            planner_snapshot,
            planner_candidate,
            accounting_cards=accounting_cards,
            policy=current_reservation_policy,
            evidence_state=current_reservation_evidence,
            authenticated_capacity=current_authenticated_reserved,
            eligible_capacity=current_eligible_reserved,
            reservation_evidence_sha256=_reservation_evidence_sha256(
                fill_config, validated_allocation))
    if any(
            baseline_allocation_reserved.get(card, 0) >
            current_eligible_reserved.get(card, 0)
            for card in accounting_cards):
        raise CapacityAdmissionConflict(
            'Committed reservation debit exceeds the current locked eligible '
            'envelope.')
    claim_projection = _plan_claim_projection(
        connection,
        service_name=service['name'],
        service_hash=service['hash'],
        generation=validation_generation,
        accounting_cards=accounting_cards,
        capacity_unit=planner_candidate.capacity_unit,
        candidate=planner_candidate)
    claim_units_by_card = claim_projection.units_by_accelerator
    claimed_paid_gpu_units = claim_projection.physical_gpu_units
    expected_paid = {
        card: baseline_paid.get(card, 0) + claim_units_by_card.get(card, 0)
        for card in accounting_cards
    }
    if (current_zero != baseline_zero or
            current_pending_zero != baseline_pending_zero or
            current_paid != expected_paid or current_charged_paid
            != planner_snapshot.reservation.charged_paid_gpu_units +
            claimed_paid_gpu_units):
        raise CapacityAdmissionConflict(
            'Committed capacity changed after the ordered plan snapshot.')
    authorized = paid_launch.get(accelerator, 0)
    claimed = claim_units_by_card.get(accelerator, 0) + claim_units
    if authorized <= 0 or claimed > authorized:
        raise CapacityAdmissionConflict(
            'Paid claims exceed the exact whole-backend launch target.')
    # The first clock sample selects rows conservatively, but allocation,
    # route, capacity, and claim locks may all wait.  Provider-start authority
    # therefore ends on a fresh database-clock sample taken after every lock.
    final_now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    freshness_horizons = [head['valid_until'], route_head['valid_until']]
    freshness_horizons.extend(row['valid_until'] for row in fresh_reports)
    if validated_allocation is not None:
        freshness_horizons.extend(
            datetime.datetime.fromtimestamp(snapshot.valid_until,
                                            datetime.timezone.utc)
            for snapshot in validated_allocation.pool_snapshots)
    paid_fresh_until = min(freshness_horizons)
    if paid_fresh_until <= final_now:
        raise CapacityAdmissionConflict(
            'Paid claim freshness expired while validation waited.')
    return paid_fresh_until


def validate_prospective_paid_claim_batch_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    claims: Sequence[Mapping[str, Any]],
    *,
    protocol_and_service_prelocked: bool = False,
) -> datetime.datetime:
    """Validate one bounded planner-authority batch before any writes.

    Every member must carry the same immutable plan authority.  Units are
    summed by the exact debit card before the existing prospective validator
    observes the database, so multiple members cannot each independently pass
    against the same residual.  The caller retains the transaction and locks
    while persisting the accepted batch graph.
    """
    if (not isinstance(claims, Sequence) or
            isinstance(claims, (str, bytes, bytearray)) or not claims):
        raise CapacityAdmissionConflict(
            'Paid claim batch must be a nonempty ordered sequence.')
    authority_fields = ('capacity_plan_generation', 'capacity_plan_sha256',
                        'demand_feed_generation', 'demand_source_epoch')
    first = claims[0]
    if not isinstance(first, Mapping):
        raise CapacityAdmissionConflict(
            'Paid claim batch contains a malformed claim.')
    authority = tuple(first.get(field) for field in authority_fields)
    if any(value is None for value in authority):
        raise CapacityAdmissionConflict(
            'Paid claim batch has no exact planner authority.')

    units_by_card: dict[str, int] = {}
    member_units_by_card: dict[str, list[int]] = {}
    representative_by_card: dict[str, Mapping[str, Any]] = {}
    pool_keys_by_card: dict[str, list[str]] = {}
    for claim in claims:
        if not isinstance(claim, Mapping):
            raise CapacityAdmissionConflict(
                'Paid claim batch contains a malformed claim.')
        if tuple(claim.get(field) for field in authority_fields) != authority:
            raise CapacityAdmissionConflict(
                'Paid claim batch spans multiple planner authorities.')
        card = claim.get('capacity_plan_accelerator')
        units = claim.get('capacity_plan_units')
        if (not isinstance(card, str) or not card or
                not isinstance(units, int) or isinstance(units, bool) or
                units <= 0):
            raise CapacityAdmissionConflict(
                'Paid claim batch contains a malformed planner debit.')
        relational_pool_key = claim.get('paid_capacity_pool_key')
        persisted_pool_key = claim.get('pool_key')
        if (relational_pool_key is not None and
                persisted_pool_key is not None and
                relational_pool_key != persisted_pool_key):
            raise CapacityAdmissionConflict(
                'Paid claim batch contains contradictory provider pools.')
        pool_key = (relational_pool_key
                    if relational_pool_key is not None else persisted_pool_key)
        try:
            serve_paid_capacity.paid_pool_gpu_shape(pool_key)
        except serve_paid_capacity.PaidGPUAttributionError as error:
            raise CapacityAdmissionConflict(
                'Paid claim batch contains a malformed provider pool.'
            ) from error
        assert isinstance(pool_key, str)
        pool_keys_by_card.setdefault(card, []).append(pool_key)
        representative_by_card.setdefault(card, claim)
        units_by_card[card] = units_by_card.get(card, 0) + units
        member_units_by_card.setdefault(card, []).append(units)

    freshness_horizons: list[datetime.datetime] = []
    for card in sorted(units_by_card):
        aggregate_claim = dict(representative_by_card[card])
        aggregate_claim['capacity_plan_units'] = units_by_card[card]
        paid_fresh_until = validate_paid_claim_in_connection(
            connection,
            service,
            aggregate_claim,
            prospective=True,
            require_planner=True,
            protocol_and_service_prelocked=protocol_and_service_prelocked,
            _batch_member_units=tuple(member_units_by_card[card]),
            _batch_member_pool_keys=tuple(pool_keys_by_card[card]))
        if paid_fresh_until is None:
            raise CapacityAdmissionConflict(
                'Paid claim batch has no exact planner authority.')
        freshness_horizons.append(paid_fresh_until)
    return min(freshness_horizons)


def promote_service_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    participant_barrier_passed: bool |
    Callable[[sqlalchemy.engine.Connection], bool],
) -> int:
    """Promote one service after the caller proves the API012 fleet barrier."""
    _require_postgres(connection)
    service = connection.execute(
        sqlalchemy.select(_SERVICES).where(_SERVICES.c.name == service_name).
        with_for_update()).mappings().one_or_none()
    if service is None:
        raise CapacityAdmissionConflict('Service no longer exists.')
    try:
        service_status = serve_statuses.ServiceStatus(str(service['status']))
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Demand promotion encountered an unknown service status.'
        ) from error
    if (service['pool'] != 0 or
            service['controller_incarnation'] != controller_incarnation or
            service['ordinary_launch_binding_mode'] != 'bound' or
            service['ordinary_launch_binding_capable'] is not True or
            service['non_pool_launch_binding_capable'] is not True or
            service['non_pool_launch_controller_incarnation']
            != controller_incarnation or
            service['non_pool_launch_binding_protocol_version'] != 2 or
            not isinstance(
                service['non_pool_launch_capability_profile_set_digest'], str)
            or _SHA256_RE.fullmatch(
                service['non_pool_launch_capability_profile_set_digest'])
            is None or service['non_pool_launch_capability_cohort_epoch']
            != constants.NON_POOL_CAPABILITY_COHORT_EPOCH or
            service['non_pool_launch_receipt_protocol_version'] != 1 or
            service['route_source_mode'] != 'DURABLE_PROJECTED' or
            service['route_source_epoch'] < 1 or
            service['route_projection_capable'] is not True or
            service['route_projection_controller_incarnation']
            != controller_incarnation or
            service['route_projection_protocol_version'] not in (1, 2) or
            service_status
            in serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        raise CapacityAdmissionConflict(
            'Service is not ready for durable demand authority.')
    current_mode = DemandSourceMode(service['demand_source_mode'])
    if current_mode is DemandSourceMode.DURABLE_FEED:
        return int(service['demand_source_epoch'])
    if (service['lb_cutover_phase'] != lb_ha.LbCutoverPhase.STABLE.value):
        raise CapacityAdmissionUnavailable(
            'Promotion requires a stable load balancer cutover.')
    if participant_barrier_passed is True:
        raise CapacityAdmissionUnavailable(
            'A precomputed fleet barrier cannot authorize promotion.')
    barrier = (participant_barrier_passed(connection)
               if callable(participant_barrier_passed) else False)
    if barrier is not True:
        raise CapacityAdmissionUnavailable(
            'Promotion requires the exact API012 fleet capability.')
    pending_claim = connection.execute(
        sqlalchemy.select(sqlalchemy.literal(True)).where(
            sqlalchemy.exists().where(
                _CLAIMS.c.service_name == service_name))).scalar_one_or_none()
    if pending_claim:
        raise CapacityAdmissionConflict(
            'Legacy paid claims must settle before demand promotion.')
    replica_rows = connection.execute(
        sqlalchemy.select(
            _REPLICAS.c.status, _REPLICAS.c.replica_state_version,
            _REPLICAS.c.replica_state).where(
                _REPLICAS.c.service_name == service_name).order_by(
                    _REPLICAS.c.replica_id).with_for_update()).mappings().all()
    for row in replica_rows:
        if row['status'] not in _TERMINAL_REPLICA_STATUSES:
            _validated_replica_attribution(row)
    now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    route_head = connection.execute(
        sqlalchemy.select(_ROUTE_HEADS).where(
            _ROUTE_HEADS.c.service_name ==
            service_name).with_for_update()).mappings().one_or_none()
    route = (None if route_head is None else connection.execute(
        sqlalchemy.select(_ROUTE_SNAPSHOTS).where(
            _ROUTE_SNAPSHOTS.c.service_name == service_name,
            _ROUTE_SNAPSHOTS.c.generation
            == route_head['generation'])).mappings().one_or_none())
    generation = connection.execute(
        sqlalchemy.select(_DEMAND_GENERATIONS.c.generation).where(
            _DEMAND_GENERATIONS.c.service_name == service_name,
            _DEMAND_GENERATIONS.c.service_hash ==
            service['hash']).with_for_update()).scalar_one_or_none()
    reports = connection.execute(
        sqlalchemy.select(_DEMAND_REPORTS).where(
            _DEMAND_REPORTS.c.service_name == service_name,
            _DEMAND_REPORTS.c.service_hash == service['hash'],
            _DEMAND_REPORTS.c.valid_until
            > now).with_for_update()).mappings().all()
    selected_reports = demand_state.current_demand_report_rows(reports, service)
    if selected_reports is None:
        raise CapacityAdmissionUnavailable(
            'Promotion requires a current load balancer.')
    reports = selected_reports
    if (route_head is None or route is None or
            route_head['valid_until'] <= now or
            route['service_hash'] != service['hash'] or
            route['service_lifecycle_epoch'] != service['lifecycle_epoch'] or
            route['controller_incarnation'] != controller_incarnation or
            route['service_version'] != service['current_version'] or
            route['protocol_version'] != PROTOCOL_VERSION or
            route['producer_protocol_version']
            != service['route_projection_protocol_version'] or
            generation is None or not reports or
            any(row['complete'] is not True or row['protocol_version'] != 2
                for row in reports)):
        raise CapacityAdmissionUnavailable(
            'Promotion requires fresh complete demand and route evidence.')
    route_context = demand_state.validate_report_route_contexts(
        connection, service, reports, route_head, route, now)
    if (route_context is None or route_context.relation
            is not route_projection.DemandReportRouteRelation.EXACT):
        raise CapacityAdmissionUnavailable(
            'Fresh demand does not match the current projected route '
            'context.')
    next_epoch = int(service['demand_source_epoch']) + 1
    connection.execute(
        sqlalchemy.update(_SERVICES).where(
            _SERVICES.c.name == service_name).values(
                demand_source_mode=DemandSourceMode.DURABLE_FEED.value,
                demand_source_epoch=next_epoch,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=(
                    controller_incarnation),
                demand_authority_protocol_version=PROTOCOL_VERSION))
    return next_epoch


def demote_service_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    expected_source_epoch: int,
) -> int:
    """Return demand ownership to the legacy path after claims settle."""
    _require_postgres(connection)
    service = connection.execute(
        sqlalchemy.select(_SERVICES).where(_SERVICES.c.name == service_name).
        with_for_update()).mappings().one_or_none()
    if (service is None or
            service['controller_incarnation'] != controller_incarnation or
            service['demand_source_epoch'] != expected_source_epoch):
        raise CapacityAdmissionConflict(
            'Demand demotion lost its service or source epoch.')
    if service[
            'demand_source_mode'] == DemandSourceMode.LEGACY_CONTROLLER.value:
        return int(service['demand_source_epoch'])
    planner_claim = connection.execute(
        sqlalchemy.select(sqlalchemy.literal(True)).where(
            sqlalchemy.exists().where(
                _CLAIMS.c.service_name == service_name,
                _CLAIMS.c.capacity_plan_generation.is_not(
                    None)))).scalar_one_or_none()
    if planner_claim:
        raise CapacityAdmissionUnavailable(
            'Planner-bound paid claims must settle before demand demotion.')
    next_epoch = int(service['demand_source_epoch']) + 1
    connection.execute(
        sqlalchemy.update(_SERVICES).where(
            _SERVICES.c.name == service_name).values(
                demand_source_mode=DemandSourceMode.LEGACY_CONTROLLER.value,
                demand_source_epoch=next_epoch,
                demand_authority_capable=False,
                demand_authority_controller_incarnation=None,
                demand_authority_protocol_version=None))
    connection.execute(
        sqlalchemy.delete(_HEADS).where(_HEADS.c.service_name == service_name))
    connection.execute(
        sqlalchemy.delete(_PLANS).where(_PLANS.c.service_name == service_name))
    return next_epoch
