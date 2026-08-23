"""Durable demand ownership and ordered paid-capacity admission.

This module does not choose providers.  It records the autoscaler's normalized
capacity decision after zero-cost acceptance and gives the existing paid
capacity ledger one immutable authority tuple to bind into each claim.
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
import dataclasses
import datetime
import enum
import hashlib
import json
import pickle
import re
from typing import Any, TYPE_CHECKING
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.adaptors import common as adaptors_common
from sky.serve import capacity_admission_schema
from sky.serve import constants
from sky.serve import demand_state
from sky.serve import demand_state_schema
from sky.serve import kueue_lane_capacity
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_state_schema
from sky.serve import serve_statuses
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
zero_cost_actuation = adaptors_common.LazyImport(
    'sky.serve.zero_cost_actuation')

PROTOCOL_VERSION = 1
CAPABILITY_COHORT_EPOCH = 1
AGGREGATE_ACCELERATOR = '*'
_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_SERVICES = serve_state_schema.services_table
_VERSION_SPECS = serve_state_schema.version_specs_table
_CLAIMS = serve_state_schema.paid_capacity_claims_table
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


class DemandSourceMode(str, enum.Enum):
    LEGACY_CONTROLLER = 'LEGACY_CONTROLLER'
    DURABLE_FEED = 'DURABLE_FEED'


class ReservedFillPlanAuthorityMode(str, enum.Enum):
    """How one immutable capacity plan relates to reserved-fill authority."""

    NOT_APPLICABLE = 'NOT_APPLICABLE'
    ALLOCATION_BOUND = 'ALLOCATION_BOUND'
    UNBOUND_ZERO_REVOCATION = 'UNBOUND_ZERO_REVOCATION'


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

    def __post_init__(self) -> None:
        if not isinstance(self.mode, ReservedFillPlanAuthorityMode):
            raise ValueError('Reserved-fill plan authority mode is invalid.')
        if ((self.mode is ReservedFillPlanAuthorityMode.ALLOCATION_BOUND)
                != (self.allocation is not None)):
            raise ValueError('Only an allocation-bound plan may carry a '
                             'reserved-fill allocation identity.')

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
    def from_mapping(cls, value: Any) -> 'ReservedFillPlanAuthority':
        if not isinstance(value,
                          Mapping) or set(value) - {'mode', 'allocation'}:
            raise ValueError('Reserved-fill plan authority is malformed.')
        try:
            mode = ReservedFillPlanAuthorityMode(value.get('mode'))
        except (TypeError, ValueError) as error:
            raise ValueError(
                'Reserved-fill plan authority mode is invalid.') from error
        if mode is ReservedFillPlanAuthorityMode.ALLOCATION_BOUND:
            if set(value) != {'mode', 'allocation'}:
                raise ValueError(
                    'Allocation-bound plan authority is incomplete.')
            allocation = (reserved_fill_planner.ReservedFillAllocationIdentity.
                          from_mapping(value['allocation']))
        else:
            if set(value) != {'mode'}:
                raise ValueError(
                    'Unbound plan authority must not carry an allocation.')
            allocation = None
        return cls(mode, allocation)

    def to_mapping(self) -> dict[str, Any]:
        result: dict[str, Any] = {'mode': self.mode.value}
        if self.allocation is not None:
            result['allocation'] = self.allocation.to_mapping()
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
    allocation_reserved_capacity_by_accelerator: Mapping[str, int] = (
        dataclasses.field(default_factory=dict))
    expected_pending_zero_cost_capacity_by_accelerator: Mapping[str, int] = (
        dataclasses.field(default_factory=dict))
    expected_economic_capacity_graph_sha256: str | None = None

    def payload(
        self,
        *,
        existing_zero_cost_capacity_by_accelerator: Mapping[str, int],
        pending_zero_cost_capacity_by_accelerator: Mapping[str, int] |
        None = None,
        allocation_reserved_capacity_by_accelerator: Mapping[str, int] |
        None = None,
        existing_paid_capacity_by_accelerator: Mapping[str, int],
        paid_residual_by_accelerator: Mapping[str, int],
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
        paid = _canonical_counts(paid_residual_by_accelerator,
                                 'paid_residual_by_accelerator')
        cards = (set(capacity_target) | set(existing_zero_cost) |
                 set(pending_zero_cost) | set(allocation_reserved) |
                 set(existing_paid) | set(paid))
        if AGGREGATE_ACCELERATOR in cards and len(cards) != 1:
            raise ValueError('A capacity plan cannot mix aggregate and '
                             'exact-card accounting.')
        expected_paid = {
            card: max(
                0,
                capacity_target.get(card, 0) - existing_zero_cost.get(card, 0) -
                pending_zero_cost.get(card, 0) -
                allocation_reserved.get(card, 0) -
                existing_paid.get(card, 0)) for card in cards
        }
        expected_paid = {
            card: count for card, count in expected_paid.items() if count > 0
        }
        paid = {card: count for card, count in paid.items() if count > 0}
        if paid != expected_paid:
            raise ValueError('Paid residual is not the exact post-zero-cost '
                             'capacity deficit.')
        authority = self.reserved_fill_authority
        if not isinstance(authority, ReservedFillPlanAuthority):
            raise ValueError('Capacity plan has no typed reserved-fill '
                             'authority.')
        economic_graph_sha256 = self.expected_economic_capacity_graph_sha256
        if authority.mode is ReservedFillPlanAuthorityMode.ALLOCATION_BOUND:
            if (not isinstance(economic_graph_sha256, str) or
                    _SHA256_RE.fullmatch(economic_graph_sha256) is None):
                raise ValueError('Allocation-bound capacity plan has no exact '
                                 'economic capacity graph digest.')
        elif economic_graph_sha256 is not None:
            raise ValueError('An unbound capacity plan must not carry an '
                             'economic capacity graph digest.')
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
            'reserved_fill_authority': authority.to_mapping(),
        }
        if economic_graph_sha256 is not None:
            payload['economic_capacity_graph_sha256'] = economic_graph_sha256
        return payload


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
    reserved_fill_authority: ReservedFillPlanAuthority

    def remaining(self) -> dict[str, int]:
        return dict(self.paid_residual_by_accelerator)

    def claim_values(self, accelerator: str, units: int = 1) -> dict[str, Any]:
        card = accelerator.casefold()
        _positive_int(units, 'units')
        remaining = self.remaining()
        debit_card = (card if remaining.get(card, 0) >= units else
                      AGGREGATE_ACCELERATOR)
        if remaining.get(debit_card, 0) < units:
            raise CapacityAdmissionConflict(
                f'Capacity plan has no paid residual for {card!r}.')
        return {
            'capacity_plan_generation': self.generation,
            'capacity_plan_sha256': self.content_sha256,
            'demand_feed_generation': self.demand_feed_generation,
            'demand_source_epoch': self.demand_source_epoch,
            'capacity_plan_accelerator': debit_card,
            'capacity_plan_units': units,
        }


@dataclasses.dataclass(frozen=True)
class ReservedSupplyProjection:
    """Exact additional zero-cost supply used by economic demand placement."""

    pending_zero_cost_capacity_by_accelerator: Mapping[str, int]
    allocation_reserved_capacity_by_accelerator: Mapping[str, int]
    economic_replica_infos: tuple[Any, ...]
    economic_kueue_capacity_by_replica_id: Mapping[
        int, kueue_lane_capacity.KueueReplicaCapacityClass]
    economic_capacity_graph_sha256: str

    def additional_capacity_by_accelerator(self) -> dict[str, int]:
        cards = (set(self.pending_zero_cost_capacity_by_accelerator) |
                 set(self.allocation_reserved_capacity_by_accelerator))
        return {
            card: (
                self.pending_zero_cost_capacity_by_accelerator.get(card, 0) +
                self.allocation_reserved_capacity_by_accelerator.get(card, 0)
            ) for card in sorted(cards)
        }


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
    try:
        reserved_fill_authority = ReservedFillPlanAuthority.from_mapping(
            payload.get('reserved_fill_authority'))
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Capacity plan has no valid reserved-fill authority.') from error
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
        reserved_fill_authority=reserved_fill_authority)


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
class _ReservedFillServiceConfig:
    binding_required: bool
    max_capacity: int
    capacity_unit: FillCapacityUnit


def _reserved_fill_service_config_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
) -> _ReservedFillServiceConfig:
    """Read the immutable fill discriminator and ceiling under lock."""
    version_row = connection.execute(
        sqlalchemy.select(_VERSION_SPECS.c.spec).where(
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
    except Exception as error:  # pylint: disable=broad-except
        raise CapacityAdmissionConflict(
            'Current service version reserved-fill spec is malformed.'
        ) from error
    if type(fill_enabled) is not bool:
        raise CapacityAdmissionConflict(
            'Current service version reserved-fill selector is malformed.')
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
    return _ReservedFillServiceConfig(binding_required=fill_enabled and
                                      mode == 'DURABLE_INTENT',
                                      max_capacity=maximum,
                                      capacity_unit=capacity_unit)


def _reserved_fill_binding_required_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
) -> bool:
    """Read the elected immutable service-spec discriminator under lock."""
    return _reserved_fill_service_config_in_connection(connection,
                                                       service).binding_required


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


def _lock_capacity_rows(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_hash: str,
    now: datetime.datetime,
) -> _LockedCapacityRows:
    """Lock the complete capacity graph before sorted Kueue admission rows."""
    intent_rows = connection.execute(
        sqlalchemy.select(_ZERO_COST_INTENTS).where(
            _ZERO_COST_INTENTS.c.service_name == service_name,
            _ZERO_COST_INTENTS.c.service_hash == service_hash).order_by(
                _ZERO_COST_INTENTS.c.intent_idempotency_key).with_for_update()
    ).mappings().all()
    replica_rows = connection.execute(
        sqlalchemy.select(
            _REPLICAS.c.replica_id, _REPLICAS.c.status, _REPLICAS.c.version,
            _REPLICAS.c.reserved_fill_intent_idempotency_key,
            _REPLICAS.c.replica_state_version, _REPLICAS.c.replica_state).where(
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
        capacity_unit_by_intent_key=capacity_unit_by_intent_key)


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


def _project_capacity_inventory(
    locked: _LockedCapacityRows,
    *,
    service_version: int,
    accounting_cards: set[str],
    now: datetime.datetime,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    """Project demand supply from one locked replica/admission snapshot."""
    if not accounting_cards:
        raise CapacityAdmissionConflict(
            'Capacity plan has no accounting class.')
    aggregate = accounting_cards == {AGGREGATE_ACCELERATOR}
    if AGGREGATE_ACCELERATOR in accounting_cards and not aggregate:
        raise CapacityAdmissionConflict(
            'Capacity plan mixes aggregate and exact-card accounting.')
    zero_cost = {card: 0 for card in accounting_cards}
    paid = {card: 0 for card in accounting_cards}
    counted_kueue_intents: set[str] = set()
    for row in locked.replica_rows:
        if row['status'] in _TERMINAL_REPLICA_STATUSES:
            continue
        state, planned_capacity, is_zero_cost, is_scale_down = (
            _validated_replica_attribution(row))
        if is_scale_down:
            continue
        card = (AGGREGATE_ACCELERATOR if aggregate else _replica_card(state))
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
            if lane_assigned is False:
                continue
            if row['version'] != service_version and lane_assigned is not True:
                continue
            zero_cost[card] += planned_capacity
            if lane_assigned is True and isinstance(intent_key, str):
                counted_kueue_intents.add(intent_key)
        elif row['version'] == service_version:
            paid[card] += planned_capacity

    pending_zero_cost = {card: 0 for card in accounting_cards}
    for row in locked.intent_rows:
        intent_key = row['intent_idempotency_key']
        lane_assigned = lane_projection.assigned_gpu_for_intent(intent_key)
        if lane_assigned is True:
            if intent_key in counted_kueue_intents:
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
    return zero_cost, paid, pending_zero_cost


def _economic_capacity_graph_snapshot(
    locked: _LockedCapacityRows,
    lane_projection: kueue_lane_capacity.KueueAdmissionCapacityProjection,
    *,
    service_version: int,
) -> tuple[tuple[Any, ...], dict[
        int, kueue_lane_capacity.KueueReplicaCapacityClass], str]:
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
        kueue_assigned = (
            admission_class
            is not kueue_lane_capacity.KueueReplicaCapacityClass.FRESH_WAITING)
        terminal = row['status'] in _TERMINAL_REPLICA_STATUSES
        historical = int(row['version']) != service_version
        contributes = (not terminal and not is_scale_down and kueue_assigned and
                       (not historical or admission_class is not None))
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
        'unknown_shapes': sorted(
            [list(shape) for shape in lane_projection.unknown_shapes]),
        'unbounded_unknown': lane_projection.unbounded_unknown,
    })
    return tuple(replica_infos), classes, digest


def _replica_service_ceiling_capacity(
    row: Mapping[str, Any],
    capacity_unit: FillCapacityUnit,
) -> int:
    """Project one cleanup-unproven row into the service's configured unit."""
    state = row['replica_state']
    status = (state.get('status_property')
              if isinstance(state, Mapping) else None)
    if (isinstance(status, Mapping) and
            status.get('sky_down_status') == 'SUCCEEDED'):
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
    allocation_upper_bound = planned_capacity
    for snapshot in allocation.pool_snapshots:
        slot_cost = max(
            config.capacity_unit.intent_cost(location.accelerator_count)
            for location in snapshot.locations)
        allocation_upper_bound += (
            min(snapshot.free_slots, snapshot.grant, snapshot.edge_cap) *
            slot_cost)
    try:
        exact = (reserved_fill_planner.ReservedFillPlanner.
                 project_remaining_capacity_by_accelerator(
                     allocation_map=allocation,
                     max_replicas=allocation_upper_bound,
                     planned_replicas=planned_capacity,
                     capacity_unit=config.capacity_unit,
                     committed_fill_debits=debits))
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Current allocation tail cannot be projected exactly.') from error
    if sum(exact.values()) > max(0, config.max_capacity - planned_capacity):
        # The fairness rotation anchor is process-local.  Under a binding
        # mixed-card ceiling it can change the exact-card tail selected by the
        # planner, so positive paid authority must wait instead of guessing.
        raise CapacityAdmissionConflict(
            'Current allocation tail exceeds rotation-independent service '
            'headroom.')
    if accounting_cards == {AGGREGATE_ACCELERATOR}:
        return {AGGREGATE_ACCELERATOR: sum(exact.values())}
    if set(exact) - accounting_cards:
        raise CapacityAdmissionConflict(
            'Current allocation tail is outside the plan accounting classes.')
    return {card: exact.get(card, 0) for card in sorted(accounting_cards)}


def _claim_units_for_plan(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_hash: str,
    generation: int,
    accounting_cards: set[str],
) -> dict[str, int]:
    units = {card: 0 for card in accounting_cards}
    rows = connection.execute(
        sqlalchemy.select(_CLAIMS.c.capacity_plan_accelerator,
                          _CLAIMS.c.capacity_plan_units).where(
                              _CLAIMS.c.service_name == service_name,
                              _CLAIMS.c.service_hash == service_hash,
                              _CLAIMS.c.capacity_plan_generation ==
                              generation).with_for_update()).mappings().all()
    for row in rows:
        card = row['capacity_plan_accelerator']
        count = row['capacity_plan_units']
        if (card not in units or not isinstance(count, int) or
                isinstance(count, bool) or count < 1):
            raise CapacityAdmissionConflict(
                'Planner-bound claim accounting is malformed.')
        units[card] += count
    return units


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


def _paid_residual(
    demand: Mapping[str, int],
    existing_zero_cost: Mapping[str, int],
    pending_zero_cost: Mapping[str, int],
    existing_paid: Mapping[str, int],
    allocation_reserved: Mapping[str, int] | None = None,
) -> dict[str, int]:
    if allocation_reserved is None:
        allocation_reserved = {}
    cards = (set(demand) | set(existing_zero_cost) | set(pending_zero_cost) |
             set(allocation_reserved) | set(existing_paid))
    return {
        card: residual for card in sorted(cards) if (residual := max(
            0,
            demand.get(card, 0) - existing_zero_cost.get(card, 0) -
            pending_zero_cost.get(card, 0) - allocation_reserved.get(card, 0) -
            existing_paid.get(card, 0))) > 0
    }


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

    @staticmethod
    def _validate_sources(connection: sqlalchemy.engine.Connection,
                          plan: CapacityPlanInput,
                          service: Mapping[str, Any]) -> None:
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
            raise CapacityAdmissionConflict(
                'Demand feed advanced before plan publication.')
        reports = connection.execute(
            sqlalchemy.select(_DEMAND_REPORTS).where(
                _DEMAND_REPORTS.c.service_name == plan.service_name,
                _DEMAND_REPORTS.c.service_hash == plan.service_hash,
                _DEMAND_REPORTS.c.valid_until
                > now).order_by(_DEMAND_REPORTS.c.reporter_session_id).
            with_for_update()).mappings().all()
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
                not (reports_complete or reports_allow_zero) or
                not demand_state.reports_match_current_lb_authority(
                    reports, service)):
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
        for row in reports:
            payload = row['payload']
            if (not isinstance(payload, Mapping) or
                    payload.get('route_projection_generation')
                    != plan.route_generation or
                    payload.get('route_projection_sha256') != plan.route_sha256
                    or payload.get('route_source_epoch')
                    != plan.route_source_epoch):
                raise CapacityAdmissionConflict(
                    'Demand report does not name the exact fresh route.')

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
            _, _, pending = _project_capacity_inventory(
                locked,
                service_version=service_version,
                accounting_cards=cards,
                now=now,
                lane_projection=lane_projection)
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
        return ReservedSupplyProjection(
            pending_zero_cost_capacity_by_accelerator=pending,
            allocation_reserved_capacity_by_accelerator=tail,
            economic_replica_infos=economic_infos,
            economic_kueue_capacity_by_replica_id=economic_kueue,
            economic_capacity_graph_sha256=economic_digest)

    def publish(
        self,
        plan: CapacityPlanInput,
        *,
        ttl_seconds: int = constants.CAPACITY_PLAN_TTL_SECONDS
    ) -> PaidLaunchAuthority:
        """Publish or refresh one post-zero-cost semantic plan."""
        capacity_target = _canonical_counts(plan.capacity_target_by_accelerator,
                                            'capacity_target_by_accelerator')
        accounting_cards = set(capacity_target)
        if not accounting_cards:
            raise ValueError(
                'capacity_target_by_accelerator must not be empty.')
        watermark_sha256 = _sha256(_canonical_watermark(plan.receipt_watermark))
        if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
            raise ValueError('ttl_seconds must be positive.')
        with self.engine.begin() as connection:
            allocation_bound = (
                plan.reserved_fill_authority.mode
                is ReservedFillPlanAuthorityMode.ALLOCATION_BOUND)
            if allocation_bound:
                # Allocation writers take the exclusive protocol mutex before
                # service-local rows.  Publication shares that exact prefix so
                # a positive plan and the allocation it names linearize in one
                # PostgreSQL transaction.
                serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
                    connection)
            service = connection.execute(
                sqlalchemy.select(_SERVICES).where(
                    _SERVICES.c.name == plan.service_name).with_for_update()
            ).mappings().one_or_none()
            if service is None:
                raise CapacityAdmissionConflict('Service no longer exists.')
            fill_config = _reserved_fill_service_config_in_connection(
                connection, service)
            reserved_fill_binding_required = fill_config.binding_required
            positive_target = any(capacity_target.values())
            authority_mode = plan.reserved_fill_authority.mode
            expected_authority_mode = (
                ReservedFillPlanAuthorityMode.ALLOCATION_BOUND
                if reserved_fill_binding_required and positive_target else
                ReservedFillPlanAuthorityMode.UNBOUND_ZERO_REVOCATION
                if reserved_fill_binding_required else
                ReservedFillPlanAuthorityMode.NOT_APPLICABLE)
            if authority_mode is not expected_authority_mode:
                raise CapacityAdmissionConflict(
                    'Capacity plan reserved-fill authority does not match its '
                    'service actuation mode and target.')
            self._validate_sources(connection, plan, service)
            validated_allocation = None
            if allocation_bound:
                validated_allocation = (
                    _validate_reserved_fill_authority_in_connection(
                        connection,
                        service,
                        plan.reserved_fill_authority,
                        reserved_fill_binding_required=(
                            reserved_fill_binding_required),
                        protocol_and_service_prelocked=True))
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            locked_capacity = _lock_capacity_rows(
                connection,
                service_name=plan.service_name,
                service_hash=plan.service_hash,
                now=now)
            lane_projection = _lock_kueue_projection(
                connection,
                service_name=plan.service_name,
                service_hash=plan.service_hash,
                service_lifecycle_epoch=plan.service_lifecycle_epoch,
                service_version=plan.service_version,
                accounting_cards=accounting_cards,
                locked=locked_capacity)
            full_zero_cost, full_paid, pending_zero_cost = (
                _project_capacity_inventory(
                    locked_capacity,
                    service_version=plan.service_version,
                    accounting_cards=accounting_cards,
                    now=now,
                    lane_projection=lane_projection))
            if allocation_bound:
                _, _, economic_digest = _economic_capacity_graph_snapshot(
                    locked_capacity,
                    lane_projection,
                    service_version=plan.service_version)
                if (plan.expected_economic_capacity_graph_sha256
                        != economic_digest):
                    raise CapacityAdmissionConflict(
                        'Economic capacity graph changed before plan '
                        'publication.')
                _validate_optimistic_capacity_projection(
                    plan.expected_pending_zero_cost_capacity_by_accelerator,
                    pending_zero_cost,
                    accounting_cards,
                    field=(
                        'expected_pending_zero_cost_capacity_by_accelerator'),
                    changed=('Pending zero-cost supply changed before plan '
                             'publication.'))
            allocation_reserved = _project_allocation_reserved_capacity(
                validated_allocation,
                locked_capacity,
                service_hash=plan.service_hash,
                service_version=plan.service_version,
                accounting_cards=accounting_cards,
                now=now,
                config=fill_config,
                lane_projection=lane_projection)
            _validate_optimistic_capacity_projection(
                plan.allocation_reserved_capacity_by_accelerator,
                allocation_reserved,
                accounting_cards,
                field='allocation_reserved_capacity_by_accelerator',
                changed=('Reserved-fill allocation tail changed before plan '
                         'publication.'))
            head = connection.execute(
                sqlalchemy.select(_HEADS).where(
                    _HEADS.c.service_name == plan.service_name).with_for_update(
                    )).mappings().one_or_none()
            previous = None
            if head is not None:
                previous = connection.execute(
                    sqlalchemy.select(_PLANS).where(
                        _PLANS.c.service_name == plan.service_name,
                        _PLANS.c.generation ==
                        head['generation'])).mappings().one_or_none()
            duplicate_payload = None
            duplicate_digest = None
            if (previous is not None and
                    previous['service_hash'] == plan.service_hash and
                    previous['service_lifecycle_epoch']
                    == plan.service_lifecycle_epoch and
                    previous['service_version'] == plan.service_version and
                    previous['demand_source_epoch']
                    == plan.demand_source_epoch):
                prior_claim_units = _claim_units_for_plan(
                    connection,
                    service_name=plan.service_name,
                    service_hash=plan.service_hash,
                    generation=int(previous['generation']),
                    accounting_cards=accounting_cards)
                prior_paid_baseline = _subtract_counts(full_paid,
                                                       prior_claim_units)
                duplicate_payload = plan.payload(
                    existing_zero_cost_capacity_by_accelerator=full_zero_cost,
                    pending_zero_cost_capacity_by_accelerator=(
                        pending_zero_cost),
                    allocation_reserved_capacity_by_accelerator=(
                        allocation_reserved),
                    existing_paid_capacity_by_accelerator=prior_paid_baseline,
                    paid_residual_by_accelerator=_paid_residual(
                        capacity_target, full_zero_cost, pending_zero_cost,
                        prior_paid_baseline, allocation_reserved))
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
                    pending_zero_cost_capacity_by_accelerator=(
                        pending_zero_cost),
                    allocation_reserved_capacity_by_accelerator=(
                        allocation_reserved),
                    existing_paid_capacity_by_accelerator=full_paid,
                    paid_residual_by_accelerator=_paid_residual(
                        capacity_target, full_zero_cost, pending_zero_cost,
                        full_paid, allocation_reserved))
                digest = _sha256(payload)
                maximum = connection.execute(
                    sqlalchemy.select(sqlalchemy.func.max(
                        _PLANS.c.generation)).where(
                            _PLANS.c.service_name ==
                            plan.service_name)).scalar_one()
                generation = 1 if maximum is None else int(maximum) + 1
                now = connection.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
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
                        created_at=now))
            assert payload is not None and digest is not None
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            head_insert = postgresql.insert(_HEADS).values(
                service_name=plan.service_name,
                generation=generation,
                demand_feed_generation=plan.demand_feed_generation,
                receipt_watermark_sha256=watermark_sha256,
                refreshed_at=now,
                valid_until=now + datetime.timedelta(seconds=ttl_seconds))
            connection.execute(
                head_insert.on_conflict_do_update(
                    index_elements=[_HEADS.c.service_name],
                    set_={
                        'generation': generation,
                        'demand_feed_generation': plan.demand_feed_generation,
                        'receipt_watermark_sha256': watermark_sha256,
                        'refreshed_at': now,
                        'valid_until': now +
                                       datetime.timedelta(seconds=ttl_seconds),
                    }))
            # Capacity plans are operational fences, not an unbounded history
            # store.  The current head and the composite claim FK retain every
            # generation that can still authorize work; all other generations
            # are superseded and may be removed in this same transaction.
            connection.execute(
                sqlalchemy.delete(_PLANS).where(
                    _PLANS.c.service_name == plan.service_name,
                    _PLANS.c.generation != generation,
                    ~sqlalchemy.exists().where(
                        _CLAIMS.c.service_name == _PLANS.c.service_name,
                        _CLAIMS.c.capacity_plan_generation
                        == _PLANS.c.generation)))
            row = connection.execute(
                sqlalchemy.select(_PLANS).where(
                    _PLANS.c.service_name == plan.service_name,
                    _PLANS.c.generation == generation)).mappings().one()
        return _authority(row,
                          demand_feed_generation=plan.demand_feed_generation)


def validate_paid_claim_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    claim: Mapping[str, Any],
    *,
    prospective: bool = False,
    require_planner: bool = True,
    protocol_and_service_prelocked: bool = False,
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
    plan = connection.execute(
        sqlalchemy.select(_PLANS).where(
            _PLANS.c.service_name == service['name'],
            _PLANS.c.generation == generation)).mappings().one_or_none()
    if (plan is None or plan['service_hash'] != service['hash'] or
            plan['content_sha256'] != claim_sha256):
        raise CapacityAdmissionConflict(
            'Paid claim lost its current fresh capacity-plan authority.')
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
    validated_allocation = _validate_reserved_fill_authority_in_connection(
        connection,
        service,
        plan_reserved_fill_authority,
        reserved_fill_binding_required=reserved_fill_binding_required,
        protocol_and_service_prelocked=protocol_and_service_prelocked)
    head = connection.execute(
        sqlalchemy.select(_HEADS).where(
            _HEADS.c.service_name ==
            service['name']).with_for_update()).mappings().one_or_none()
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
    if (head is None or head['generation'] != generation or
            head['valid_until'] <= now or
            plan['service_hash'] != service['hash'] or
            plan['content_sha256'] != claim_sha256 or
            head['demand_feed_generation'] != current_demand_generation or
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
    current_watermark = [{
        'reporter_session_id': row['reporter_session_id'],
        'sequence': int(row['sequence']),
        'payload_sha256': row['payload_sha256'],
    } for row in fresh_reports]
    if (not current_watermark or
            _sha256(current_watermark) != head['receipt_watermark_sha256'] or
            any(row['complete'] is not True or row['protocol_version'] != 2
                for row in fresh_reports) or
            not demand_state.reports_match_current_lb_authority(
                fresh_reports, service)):
        raise CapacityAdmissionConflict(
            'Paid claim lost its fresh demand receipt watermark.')
    for row in fresh_reports:
        report = row['payload']
        if (not isinstance(report, Mapping) or
                report.get('route_projection_generation')
                != plan['route_generation'] or
                report.get('route_projection_sha256') != plan['route_sha256'] or
                report.get('route_source_epoch') != plan['route_source_epoch']):
            raise CapacityAdmissionConflict(
                'Paid claim demand receipts no longer name its exact route.')
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
        economic_graph_sha256 = payload.get('economic_capacity_graph_sha256')
        if validated_allocation is not None and (
                not isinstance(economic_graph_sha256, str) or
                _SHA256_RE.fullmatch(economic_graph_sha256) is None):
            raise ValueError(
                'Allocation-bound plan lacks its economic capacity graph.')
        if validated_allocation is None and economic_graph_sha256 is not None:
            raise ValueError('Unbound plan carries an economic capacity graph.')
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Capacity plan accounting is malformed.') from error
    accounting_cards = set(capacity_target)
    if (not accounting_cards or set(baseline_zero) != accounting_cards or
            set(baseline_pending_zero) != accounting_cards or
            set(baseline_allocation_reserved) != accounting_cards or
            set(baseline_paid) != accounting_cards or
            set(paid) - accounting_cards):
        raise CapacityAdmissionConflict(
            'Capacity plan accounting classes are inconsistent.')
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
    if validated_allocation is not None:
        _, _, current_economic_digest = _economic_capacity_graph_snapshot(
            locked_capacity,
            lane_projection,
            service_version=int(service['current_version']))
        if current_economic_digest != economic_graph_sha256:
            raise CapacityAdmissionConflict(
                'Reserved economic supply changed after the ordered plan '
                'snapshot.')
    current_zero, current_paid, current_pending_zero = (
        _project_capacity_inventory(locked_capacity,
                                    service_version=int(
                                        service['current_version']),
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
    claim_units_by_card = _claim_units_for_plan(
        connection,
        service_name=service['name'],
        service_hash=service['hash'],
        generation=generation,
        accounting_cards=accounting_cards)
    expected_paid = {
        card: baseline_paid.get(card, 0) + claim_units_by_card.get(card, 0)
        for card in accounting_cards
    }
    if (current_zero != baseline_zero or
            current_pending_zero != baseline_pending_zero or
            current_allocation_reserved != baseline_allocation_reserved or
            current_paid != expected_paid):
        raise CapacityAdmissionConflict(
            'Committed capacity changed after the ordered plan snapshot.')
    if paid != _paid_residual(capacity_target, baseline_zero,
                              baseline_pending_zero, baseline_paid,
                              baseline_allocation_reserved):
        raise CapacityAdmissionConflict(
            'Paid residual is not the exact post-reserved deficit.')
    authorized = paid.get(accelerator, 0)
    claimed = claim_units_by_card.get(accelerator, 0)
    if prospective:
        claimed += claim_units
    if authorized <= 0 or claimed > authorized:
        raise CapacityAdmissionConflict(
            'Paid claims exceed the exact post-zero-cost residual.')
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
                for row in reports) or
            not demand_state.reports_match_current_lb_authority(
                reports, service)):
        raise CapacityAdmissionUnavailable(
            'Promotion requires fresh complete demand and route evidence.')
    try:
        route_projection.RouteProjectionRepository.validate_snapshot_row(route)
    except route_projection.RouteProjectionError as error:
        raise CapacityAdmissionUnavailable(
            'Promotion route evidence is corrupt.') from error
    if not route_projection.snapshot_owner_matches(route, service):
        raise CapacityAdmissionUnavailable(
            'Promotion route evidence belongs to a different owner.')
    for report in reports:
        payload = report['payload']
        if (report['protocol_version'] != 2 or
                not isinstance(payload, Mapping) or
                payload.get('route_projection_generation')
                != route_head['generation'] or
                payload.get('route_projection_sha256')
                != route['content_sha256'] or payload.get('route_source_epoch')
                != service['route_source_epoch']):
            raise CapacityAdmissionUnavailable(
                'Fresh demand does not name the current projected route.')
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
