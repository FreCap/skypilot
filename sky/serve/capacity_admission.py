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
import re
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.serve import capacity_admission_schema
from sky.serve import constants
from sky.serve import demand_state
from sky.serve import demand_state_schema
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_state_schema
from sky.serve import serve_statuses
from sky.utils.db import db_utils

PROTOCOL_VERSION = 1
CAPABILITY_COHORT_EPOCH = 1
AGGREGATE_ACCELERATOR = '*'
_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_SERVICES = serve_state_schema.services_table
_CLAIMS = serve_state_schema.paid_capacity_claims_table
_PLANS = capacity_admission_schema.serve_capacity_plans_table
_HEADS = capacity_admission_schema.serve_capacity_plan_heads_table
_DEMAND_GENERATIONS = (demand_state_schema.serve_demand_feed_generations_table)
_DEMAND_REPORTS = demand_state_schema.serve_lb_demand_reports_table
_ROUTE_HEADS = route_projection_schema.serve_route_heads_table
_ROUTE_SNAPSHOTS = route_projection_schema.serve_route_snapshots_table
_REPLICAS = serve_state_schema.replicas_table
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

    def payload(
        self,
        *,
        existing_zero_cost_capacity_by_accelerator: Mapping[str, int],
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
        existing_paid = _canonical_counts(
            existing_paid_capacity_by_accelerator,
            'existing_paid_capacity_by_accelerator')
        paid = _canonical_counts(paid_residual_by_accelerator,
                                 'paid_residual_by_accelerator')
        cards = (set(capacity_target) | set(existing_zero_cost) |
                 set(existing_paid) | set(paid))
        if AGGREGATE_ACCELERATOR in cards and len(cards) != 1:
            raise ValueError('A capacity plan cannot mix aggregate and '
                             'exact-card accounting.')
        expected_paid = {
            card: max(
                0,
                capacity_target.get(card, 0) - existing_zero_cost.get(card, 0) -
                existing_paid.get(card, 0)) for card in cards
        }
        expected_paid = {
            card: count for card, count in expected_paid.items() if count > 0
        }
        paid = {card: count for card, count in paid.items() if count > 0}
        if paid != expected_paid:
            raise ValueError('Paid residual is not the exact post-zero-cost '
                             'capacity deficit.')
        _canonical_watermark(self.receipt_watermark)
        normalized_demand = json.loads(
            _canonical_json(dict(self.normalized_demand)).decode('utf-8'))
        return {
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
            'existing_paid_capacity_by_accelerator': existing_paid,
            'paid_residual_by_accelerator': paid,
        }


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
    return PaidLaunchAuthority(
        service_name=str(row['service_name']),
        service_hash=str(row['service_hash']),
        generation=int(row['generation']),
        content_sha256=str(row['content_sha256']),
        demand_feed_generation=int(
            row['demand_feed_generation'] if demand_feed_generation is
            None else demand_feed_generation),
        demand_source_epoch=int(row['demand_source_epoch']),
        paid_residual_by_accelerator=tuple(paid.items()))


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


def _locked_capacity_inventory(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_version: int,
    accounting_cards: set[str],
) -> tuple[dict[str, int], dict[str, int]]:
    """Project compatible committed rows under the locked service mutex."""
    if not accounting_cards:
        raise CapacityAdmissionConflict(
            'Capacity plan has no accounting class.')
    aggregate = accounting_cards == {AGGREGATE_ACCELERATOR}
    if AGGREGATE_ACCELERATOR in accounting_cards and not aggregate:
        raise CapacityAdmissionConflict(
            'Capacity plan mixes aggregate and exact-card accounting.')
    zero_cost = {card: 0 for card in accounting_cards}
    paid = {card: 0 for card in accounting_cards}
    rows = connection.execute(
        sqlalchemy.select(
            _REPLICAS.c.status, _REPLICAS.c.version,
            _REPLICAS.c.replica_state_version, _REPLICAS.c.replica_state).where(
                _REPLICAS.c.service_name == service_name).order_by(
                    _REPLICAS.c.replica_id).with_for_update()).mappings().all()
    for row in rows:
        if (row['version'] != service_version or
                row['status'] in _TERMINAL_REPLICA_STATUSES):
            continue
        state, planned_capacity, is_zero_cost, is_scale_down = (
            _validated_replica_attribution(row))
        if is_scale_down:
            continue
        card = (AGGREGATE_ACCELERATOR if aggregate else _replica_card(state))
        if card not in accounting_cards:
            raise CapacityAdmissionConflict(
                'Committed replica is outside the exact-card accounting set.')
        target = zero_cost if is_zero_cost else paid
        target[card] += planned_capacity
    return zero_cost, paid


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
    existing_paid: Mapping[str, int],
) -> dict[str, int]:
    cards = set(demand) | set(existing_zero_cost) | set(existing_paid)
    return {
        card: residual for card in sorted(cards) if (residual := max(
            0,
            demand.get(card, 0) - existing_zero_cost.get(card, 0) -
            existing_paid.get(card, 0))) > 0
    }


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
        if (watermark != _canonical_watermark(plan.receipt_watermark) or
                any(row['complete'] is not True or row['protocol_version'] != 2
                    for row in reports) or
                not demand_state.reports_match_current_lb_authority(
                    reports, service)):
            raise CapacityAdmissionConflict(
                'Fresh complete demand receipts changed before publication.')
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
                service['route_source_mode'] != 'DURABLE_PROJECTED' or
                service['route_source_epoch'] != plan.route_source_epoch or
                service['route_projection_capable'] is not True or
                service['route_projection_controller_incarnation']
                != service['controller_incarnation'] or
                service['route_projection_protocol_version']
                != PROTOCOL_VERSION):
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
            service = connection.execute(
                sqlalchemy.select(_SERVICES).where(
                    _SERVICES.c.name == plan.service_name).with_for_update()
            ).mappings().one_or_none()
            if service is None:
                raise CapacityAdmissionConflict('Service no longer exists.')
            self._validate_sources(connection, plan, service)
            full_zero_cost, full_paid = _locked_capacity_inventory(
                connection,
                service_name=plan.service_name,
                service_version=plan.service_version,
                accounting_cards=accounting_cards)
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
                    existing_paid_capacity_by_accelerator=prior_paid_baseline,
                    paid_residual_by_accelerator=_paid_residual(
                        capacity_target, full_zero_cost, prior_paid_baseline))
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
                    existing_paid_capacity_by_accelerator=full_paid,
                    paid_residual_by_accelerator=_paid_residual(
                        capacity_target, full_zero_cost, full_paid))
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
) -> None:
    """Revalidate one planner-bound claim before provider I/O."""
    fields = ('capacity_plan_generation', 'capacity_plan_sha256',
              'demand_feed_generation', 'demand_source_epoch',
              'capacity_plan_accelerator', 'capacity_plan_units')
    if any(claim.get(field) is None for field in fields):
        if (require_planner and service.get('demand_source_mode')
                == DemandSourceMode.DURABLE_FEED.value):
            raise CapacityAdmissionConflict(
                'Durable-demand service retained an unbound paid claim.')
        return
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
    head = connection.execute(
        sqlalchemy.select(_HEADS).where(
            _HEADS.c.service_name ==
            service['name']).with_for_update()).mappings().one_or_none()
    plan = connection.execute(
        sqlalchemy.select(_PLANS).where(
            _PLANS.c.service_name == service['name'],
            _PLANS.c.generation == generation)).mappings().one_or_none()
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
    if (head is None or plan is None or head['generation'] != generation or
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
            service.get('route_projection_protocol_version')
            != PROTOCOL_VERSION):
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
    payload = plan['payload']
    if not isinstance(payload, Mapping):
        raise CapacityAdmissionConflict('Capacity plan payload is malformed.')
    if _sha256(payload) != plan['content_sha256']:
        raise CapacityAdmissionConflict(
            'Capacity plan digest no longer matches its payload.')
    try:
        capacity_target = _canonical_counts(
            payload.get('capacity_target_by_accelerator', {}),
            'capacity_target_by_accelerator')
        baseline_zero = _canonical_counts(
            payload.get('existing_zero_cost_capacity_by_accelerator', {}),
            'existing_zero_cost_capacity_by_accelerator')
        baseline_paid = _canonical_counts(
            payload.get('existing_paid_capacity_by_accelerator', {}),
            'existing_paid_capacity_by_accelerator')
        paid = _canonical_counts(
            payload.get('paid_residual_by_accelerator', {}),
            'paid_residual_by_accelerator')
    except ValueError as error:
        raise CapacityAdmissionConflict(
            'Capacity plan accounting is malformed.') from error
    accounting_cards = set(capacity_target)
    if (not accounting_cards or set(baseline_zero) != accounting_cards or
            set(baseline_paid) != accounting_cards or
            set(paid) - accounting_cards):
        raise CapacityAdmissionConflict(
            'Capacity plan accounting classes are inconsistent.')
    claim_units_by_card = _claim_units_for_plan(
        connection,
        service_name=service['name'],
        service_hash=service['hash'],
        generation=generation,
        accounting_cards=accounting_cards)
    current_zero, current_paid = _locked_capacity_inventory(
        connection,
        service_name=service['name'],
        service_version=int(service['current_version']),
        accounting_cards=accounting_cards)
    expected_paid = {
        card: baseline_paid.get(card, 0) + claim_units_by_card.get(card, 0)
        for card in accounting_cards
    }
    if current_zero != baseline_zero or current_paid != expected_paid:
        raise CapacityAdmissionConflict(
            'Committed capacity changed after the ordered plan snapshot.')
    authorized = paid.get(accelerator, 0)
    claimed = claim_units_by_card.get(accelerator, 0)
    if prospective:
        claimed += claim_units
    if authorized <= 0 or claimed > authorized:
        raise CapacityAdmissionConflict(
            'Paid claims exceed the exact post-zero-cost residual.')


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
            is None or
            service['non_pool_launch_capability_cohort_epoch'] != 1 or
            service['non_pool_launch_receipt_protocol_version'] != 1 or
            service['route_source_mode'] != 'DURABLE_PROJECTED' or
            service['route_source_epoch'] < 1 or
            service['route_projection_capable'] is not True or
            service['route_projection_controller_incarnation']
            != controller_incarnation or
            service['route_projection_protocol_version'] != PROTOCOL_VERSION or
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
