"""Durable grant-before-row ownership for zero-cost Serve capacity."""
from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import enum
from typing import Any
import uuid

import sqlalchemy

from sky.serve import controller_transport
from sky.serve import reserved_fill_planner
from sky.serve import serve_state_schema
from sky.serve import serve_statuses
from sky.serve import zero_cost_actuation_schema
from sky.utils.db import db_utils

PROTOCOL_VERSION = 1

_SERVICES = serve_state_schema.services_table
_REPLICAS = serve_state_schema.replicas_table
_INTENTS = (zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table)
_PENDING_STATES = frozenset({'GRANTED', 'ACTUATING', 'RETRYABLE'})
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


class ActuationMode(str, enum.Enum):
    DIRECT_REPLICA = 'DIRECT_REPLICA'
    DURABLE_INTENT = 'DURABLE_INTENT'


class IntentState(str, enum.Enum):
    GRANTED = 'GRANTED'
    ACTUATING = 'ACTUATING'
    COMMITTED = 'COMMITTED'
    RETRYABLE = 'RETRYABLE'
    TERMINAL = 'TERMINAL'


class ZeroCostActuationError(RuntimeError):
    """Base error for durable zero-cost actuation."""


class ZeroCostActuationConflict(ZeroCostActuationError):
    """The durable service or intent authority changed."""


class ZeroCostActuationUnavailable(ZeroCostActuationError):
    """The PostgreSQL-only repository is unavailable."""


@dataclasses.dataclass(frozen=True)
class IntentLease:
    """Exact lease token for one per-pool executor attempt."""

    intent: reserved_fill_planner.FillIntent
    service_lifecycle_epoch: int
    owner: uuid.UUID
    generation: int
    expires_at: datetime.datetime


def _require_postgres(connection: sqlalchemy.engine.Connection) -> None:
    if connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ZeroCostActuationUnavailable(
            'Zero-cost actuation intents require PostgreSQL.')


def _utc_from_epoch(value: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)


def _intent_values(
    intent: reserved_fill_planner.FillIntent,
    *,
    service_name: str,
    service_lifecycle_epoch: int,
) -> dict[str, Any]:
    intent.__post_init__()
    planned_capacity = intent.capacity_unit.intent_cost(
        intent.accelerator_count)
    return {
        'intent_idempotency_key': intent.idempotency_key,
        'service_name': service_name,
        'service_hash': intent.service_incarnation,
        'service_lifecycle_epoch': service_lifecycle_epoch,
        'service_version': intent.service_version,
        'controller_owner': intent.controller_owner,
        'ordinal': intent.ordinal,
        'protocol_version': intent.protocol_version,
        'policy_revision': intent.policy_revision,
        'reconcile_generation': intent.reconcile_generation,
        'allocation_generation': intent.allocation_generation,
        'allocation_input_sha256': intent.allocation_input_sha256,
        'allocation_claim_generation': intent.allocation_claim_generation,
        'reconciliation_gate_generation': intent.reconciliation_gate_generation,
        'reclaim_fleet_bundle_sha256': intent.reclaim_fleet_bundle_sha256,
        'reclaim_policy_revision': intent.reclaim_policy_revision,
        'reclaim_provider_inventory_sha256':
            intent.reclaim_provider_inventory_sha256,
        'service_generation': intent.service_generation,
        'pool_key': intent.pool_key,
        'pool_epoch': intent.pool_epoch,
        'physical_cluster_uid': intent.physical_cluster_uid,
        'kubernetes_context': intent.allowed_locations[0].region,
        'worker_projection_sha256': intent.worker_projection_sha256,
        'observation_generation': intent.observation_generation,
        'observation_sequence': intent.observation_sequence,
        'ordinary_zero_cost_admission_sequence':
            intent.ordinary_zero_cost_admission_sequence,
        'valid_until_epoch': intent.valid_until,
        'valid_until': _utc_from_epoch(intent.valid_until),
        'accelerator': intent.accelerator,
        'accelerator_count': intent.accelerator_count,
        'capacity_unit': intent.capacity_unit.value,
        'planned_capacity': planned_capacity,
        'allowed_locations': [
            location.to_pickleable() for location in intent.allowed_locations
        ],
    }


def _intent_from_row(
        row: Mapping[str, Any]) -> reserved_fill_planner.FillIntent:
    raw_locations = row['allowed_locations']
    if not isinstance(raw_locations, list):
        raise ZeroCostActuationConflict(
            'Zero-cost intent locations are malformed.')
    try:
        locations = tuple(
            reserved_fill_planner.LocationSnapshot.from_pickleable(location)
            for location in raw_locations)
        valid_until_epoch = row['valid_until_epoch']
        if (not isinstance(valid_until_epoch, float) or valid_until_epoch <= 0):
            raise ValueError('valid_until_epoch is not a positive float')
        intent = reserved_fill_planner.FillIntent(
            ordinal=int(row['ordinal']),
            idempotency_key=str(row['intent_idempotency_key']),
            protocol_version=int(row['protocol_version']),
            policy_revision=int(row['policy_revision']),
            reconcile_generation=int(row['reconcile_generation']),
            allocation_generation=int(row['allocation_generation']),
            allocation_input_sha256=str(row['allocation_input_sha256']),
            allocation_claim_generation=int(row['allocation_claim_generation']),
            reconciliation_gate_generation=int(
                row['reconciliation_gate_generation']),
            reclaim_fleet_bundle_sha256=str(row['reclaim_fleet_bundle_sha256']),
            reclaim_policy_revision=str(row['reclaim_policy_revision']),
            reclaim_provider_inventory_sha256=str(
                row['reclaim_provider_inventory_sha256']),
            service_incarnation=str(row['service_hash']),
            service_version=int(row['service_version']),
            controller_owner=str(row['controller_owner']),
            service_generation=int(row['service_generation']),
            pool_key=str(row['pool_key']),
            pool_epoch=int(row['pool_epoch']),
            physical_cluster_uid=str(row['physical_cluster_uid']),
            worker_projection_sha256=str(row['worker_projection_sha256']),
            observation_generation=int(row['observation_generation']),
            observation_sequence=int(row['observation_sequence']),
            ordinary_zero_cost_admission_sequence=int(
                row['ordinary_zero_cost_admission_sequence']),
            valid_until=valid_until_epoch,
            accelerator=str(row['accelerator']),
            accelerator_count=int(row['accelerator_count']),
            capacity_unit=reserved_fill_planner.FillCapacityUnit(
                row['capacity_unit']),
            allowed_locations=locations)
    except (KeyError, TypeError, ValueError) as error:
        raise ZeroCostActuationConflict(
            'Zero-cost intent authority is malformed.') from error
    return intent


def _row_matches_values(row: Mapping[str, Any], values: Mapping[str,
                                                                Any]) -> bool:
    immutable = set(values)
    return all(
        row.get(field) == value
        for field, value in values.items()
        if field in immutable)


def _locked_replica_capacity(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_version: int,
    capacity_unit: reserved_fill_planner.FillCapacityUnit,
) -> int:
    rows = connection.execute(
        sqlalchemy.select(
            _REPLICAS.c.status, _REPLICAS.c.version,
            _REPLICAS.c.replica_state_version, _REPLICAS.c.replica_state).where(
                _REPLICAS.c.service_name == service_name).order_by(
                    _REPLICAS.c.replica_id).with_for_update()).mappings().all()
    total = 0
    for row in rows:
        if (row['version'] != service_version or
                row['status'] in _TERMINAL_REPLICA_STATUSES):
            continue
        if capacity_unit is reserved_fill_planner.FillCapacityUnit.PHYSICAL:
            total += 1
            continue
        state = row['replica_state']
        planned_capacity = (state.get('planned_capacity') if isinstance(
            state, Mapping) else None)
        if (row['replica_state_version'] != 1 or
                not isinstance(planned_capacity, int) or
                isinstance(planned_capacity, bool) or planned_capacity < 1):
            raise ZeroCostActuationConflict(
                'Current logical replica capacity is malformed.')
        total += planned_capacity
    return total


def _retire_expired_locked(connection: sqlalchemy.engine.Connection,
                           rows: list[Mapping[str, Any]],
                           now: datetime.datetime) -> None:
    expired_keys = [
        row['intent_idempotency_key']
        for row in rows
        if row['state'] in _PENDING_STATES and row['valid_until'] <= now
    ]
    if expired_keys:
        connection.execute(
            sqlalchemy.update(_INTENTS).where(
                _INTENTS.c.intent_idempotency_key.in_(expired_keys),
                _INTENTS.c.state.in_(tuple(_PENDING_STATES))).values(
                    state=IntentState.TERMINAL.value,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_error='grant_expired',
                    updated_at=now,
                    terminal_at=now))


def pending_capacity_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    service_hash: str,
    service_version: int,
    accounting_cards: set[str],
    now: datetime.datetime,
) -> dict[str, int]:
    """Lock and project unmaterialized zero-cost capacity by accounting card."""
    _require_postgres(connection)
    if not accounting_cards:
        raise ZeroCostActuationConflict(
            'Pending zero-cost accounting has no classes.')
    aggregate = accounting_cards == {'*'}
    if '*' in accounting_cards and not aggregate:
        raise ZeroCostActuationConflict(
            'Pending zero-cost accounting mixes aggregate and exact cards.')
    rows = connection.execute(
        sqlalchemy.select(_INTENTS).where(
            _INTENTS.c.service_name == service_name,
            _INTENTS.c.service_hash == service_hash,
            _INTENTS.c.service_version == service_version).order_by(
                _INTENTS.c.intent_idempotency_key).with_for_update()).mappings(
                ).all()
    _retire_expired_locked(connection, rows, now)
    result = {card: 0 for card in accounting_cards}
    for row in rows:
        if row['state'] not in _PENDING_STATES or row['valid_until'] <= now:
            continue
        card = '*' if aggregate else str(row['accelerator']).casefold()
        if card not in result:
            raise ZeroCostActuationConflict(
                'Pending zero-cost intent is outside the plan accounting set.')
        planned_capacity = row['planned_capacity']
        if (not isinstance(planned_capacity, int) or
                isinstance(planned_capacity, bool) or planned_capacity < 1):
            raise ZeroCostActuationConflict(
                'Pending zero-cost intent capacity is malformed.')
        result[card] += planned_capacity
    return result


class ZeroCostActuationRepository:
    """Transactional owner of grants and per-physical-pool leases."""

    def __init__(self, engine: sqlalchemy.engine.Engine | None = None) -> None:
        self._engine = engine

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        engine = self._engine or serve_state_schema.get_database_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise ZeroCostActuationUnavailable(
                'Zero-cost actuation intents require PostgreSQL.')
        return engine

    @staticmethod
    def _validate_service(service: Mapping[str, Any], *, service_name: str,
                          plan: reserved_fill_planner.FillPlan) -> None:
        if not plan.intents:
            return
        first = plan.intents[0]
        try:
            status = serve_statuses.ServiceStatus(service['status'])
            owner = controller_transport.make_controller_owner_fingerprint(
                service['hash'], service['controller_pid'],
                service['controller_ip'], service['controller_port'])
        except (KeyError, TypeError, ValueError,
                controller_transport.ControllerOwnerError) as error:
            raise ZeroCostActuationConflict(
                'Durable controller ownership is malformed.') from error
        if (service['name'] != service_name or service['pool'] != 0 or
                service['hash'] != first.service_incarnation or
                service['current_version'] != first.service_version or
                status in serve_statuses.ServiceStatus.
                replica_launch_blocking_statuses() or
                owner != first.controller_owner or
                service['reserved_fill_actuation_mode']
                != ActuationMode.DURABLE_INTENT.value or
                service['reserved_fill_actuation_epoch'] <= 0 or
                service['reserved_fill_actuation_capable'] is not True or
                service['reserved_fill_actuation_controller_incarnation']
                != service['controller_incarnation'] or
                service['reserved_fill_actuation_protocol_version']
                != PROTOCOL_VERSION):
            raise ZeroCostActuationConflict(
                'Service durable-intent authority changed before grant.')

    def grant_plan(
        self,
        service_name: str,
        plan: reserved_fill_planner.FillPlan,
        *,
        max_capacity: int,
    ) -> reserved_fill_planner.FillCommitResult:
        """Persist plan intents before any replica ID or request exists."""
        if not isinstance(plan, reserved_fill_planner.FillPlan):
            raise ValueError('Grant admission requires a FillPlan.')
        plan.__post_init__()
        if (not isinstance(service_name, str) or not service_name or
                not isinstance(max_capacity, int) or
                isinstance(max_capacity, bool) or max_capacity < 0):
            raise ValueError('Grant admission requires a valid service limit.')
        if not plan.intents:
            return reserved_fill_planner.FillCommitResult(
                accepted=(), deferred=(), authority_current=True)
        accepted: list[reserved_fill_planner.AcceptedFillIntent] = []
        deferred: list[reserved_fill_planner.DeferredFillIntent] = []
        authority_current = True
        with self.engine.begin() as connection:
            service = connection.execute(
                sqlalchemy.select(_SERVICES).where(
                    _SERVICES.c.name ==
                    service_name).with_for_update()).mappings().one_or_none()
            if service is None:
                raise ZeroCostActuationConflict('Service no longer exists.')
            self._validate_service(service,
                                   service_name=service_name,
                                   plan=plan)
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            rows = connection.execute(
                sqlalchemy.select(_INTENTS).where(
                    _INTENTS.c.service_name == service_name).order_by(
                        _INTENTS.c.intent_idempotency_key).with_for_update()
            ).mappings().all()
            _retire_expired_locked(connection, rows, now)
            rows_by_key = {row['intent_idempotency_key']: row for row in rows}
            pending_capacity = sum(
                int(row['planned_capacity'])
                for row in rows
                if row['state'] in _PENDING_STATES and row['valid_until'] > now)
            current_capacity = _locked_replica_capacity(
                connection,
                service_name=service_name,
                service_version=plan.intents[0].service_version,
                capacity_unit=plan.capacity_unit)
            remaining = max(0,
                            max_capacity - current_capacity - pending_capacity)
            lifecycle_epoch = service['lifecycle_epoch']
            if (not isinstance(lifecycle_epoch, int) or
                    isinstance(lifecycle_epoch, bool) or lifecycle_epoch < 1):
                raise ZeroCostActuationConflict(
                    'Service lifecycle authority is malformed.')
            for intent in plan.intents:
                values = _intent_values(intent,
                                        service_name=service_name,
                                        service_lifecycle_epoch=lifecycle_epoch)
                existing = rows_by_key.get(intent.idempotency_key)
                if existing is not None:
                    if not _row_matches_values(existing, values):
                        raise ZeroCostActuationConflict(
                            'Intent idempotency key maps to different authority.'
                        )
                    state = IntentState(existing['state'])
                    if state is IntentState.COMMITTED:
                        accepted.append(
                            reserved_fill_planner.AcceptedFillIntent(
                                intent.idempotency_key,
                                int(existing['replica_id'])))
                    elif (state.value in _PENDING_STATES and
                          existing['valid_until'] > now):
                        accepted.append(
                            reserved_fill_planner.AcceptedFillIntent(
                                intent.idempotency_key, None))
                    else:
                        deferred.append(
                            reserved_fill_planner.DeferredFillIntent(
                                intent, reserved_fill_planner.
                                DeferredFillReason.STALE_OBSERVATION,
                                'the durable grant is terminal or expired'))
                        authority_current = False
                    continue
                if values['valid_until'] <= now:
                    deferred.append(
                        reserved_fill_planner.DeferredFillIntent(
                            intent, reserved_fill_planner.DeferredFillReason.
                            STALE_OBSERVATION,
                            'the carried observation expired before grant'))
                    authority_current = False
                    continue
                cost = int(values['planned_capacity'])
                if cost > remaining:
                    deferred.append(
                        reserved_fill_planner.DeferredFillIntent(
                            intent, reserved_fill_planner.DeferredFillReason.
                            MAX_REPLICAS_EXHAUSTED,
                            'durable rows and pending grants consumed the '
                            'service capacity ceiling'))
                    continue
                connection.execute(
                    sqlalchemy.insert(_INTENTS).values(
                        **values,
                        state=IntentState.GRANTED.value,
                        lease_generation=0,
                        created_at=now,
                        updated_at=now))
                accepted.append(
                    reserved_fill_planner.AcceptedFillIntent(
                        intent.idempotency_key, None))
                remaining -= cost
        receipt = reserved_fill_planner.FillCommitResult(
            accepted=tuple(accepted),
            deferred=tuple(deferred),
            authority_current=authority_current)
        receipt.validate_for_plan(plan)
        return receipt

    def lease_next(
        self,
        *,
        service_name: str,
        pool_key: str,
        owner: uuid.UUID,
        lease_seconds: int,
    ) -> IntentLease | None:
        """Lease one intent without serializing an unrelated physical pool."""
        if (not isinstance(owner, uuid.UUID) or
                not isinstance(lease_seconds, int) or lease_seconds <= 0):
            raise ValueError('Intent lease requires a UUID owner and TTL.')
        with self.engine.begin() as connection:
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            connection.execute(
                sqlalchemy.update(_INTENTS).where(
                    _INTENTS.c.service_name == service_name,
                    _INTENTS.c.pool_key == pool_key,
                    _INTENTS.c.state == IntentState.ACTUATING.value,
                    _INTENTS.c.lease_expires_at <= now, _INTENTS.c.valid_until
                    > now).values(state=IntentState.RETRYABLE.value,
                                  lease_owner=None,
                                  lease_expires_at=None,
                                  last_error='lease_expired',
                                  updated_at=now))
            connection.execute(
                sqlalchemy.update(_INTENTS).where(
                    _INTENTS.c.service_name == service_name,
                    _INTENTS.c.pool_key == pool_key,
                    _INTENTS.c.state.in_(tuple(_PENDING_STATES)),
                    _INTENTS.c.valid_until
                    <= now).values(state=IntentState.TERMINAL.value,
                                   lease_owner=None,
                                   lease_expires_at=None,
                                   last_error='grant_expired',
                                   updated_at=now,
                                   terminal_at=now))
            row = connection.execute(
                sqlalchemy.select(_INTENTS).where(
                    _INTENTS.c.service_name == service_name,
                    _INTENTS.c.pool_key == pool_key,
                    _INTENTS.c.state.in_(
                        (IntentState.GRANTED.value,
                         IntentState.RETRYABLE.value)), _INTENTS.c.valid_until
                    > now).order_by(_INTENTS.c.created_at,
                                    _INTENTS.c.intent_idempotency_key).limit(1).
                with_for_update(skip_locked=True)).mappings().one_or_none()
            if row is None:
                return None
            generation = int(row['lease_generation']) + 1
            expires_at = now + datetime.timedelta(seconds=lease_seconds)
            connection.execute(
                sqlalchemy.update(_INTENTS).where(
                    _INTENTS.c.intent_idempotency_key ==
                    row['intent_idempotency_key'],
                    _INTENTS.c.state.in_((IntentState.GRANTED.value,
                                          IntentState.RETRYABLE.value))).values(
                                              state=IntentState.ACTUATING.value,
                                              lease_owner=owner,
                                              lease_generation=generation,
                                              lease_expires_at=expires_at,
                                              updated_at=now))
            intent = _intent_from_row(row)
            return IntentLease(intent, int(row['service_lifecycle_epoch']),
                               owner, generation, expires_at)

    def release_retryable(self, lease: IntentLease, error: str) -> bool:
        """Release a pre-row failure without inventing a provider effect."""
        if not isinstance(error, str) or not error:
            raise ValueError('Retryable release requires a nonempty reason.')
        with self.engine.begin() as connection:
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            state = (IntentState.RETRYABLE.value if _utc_from_epoch(
                lease.intent.valid_until) > now else IntentState.TERMINAL.value)
            result = connection.execute(
                sqlalchemy.update(_INTENTS).where(
                    _INTENTS.c.intent_idempotency_key ==
                    lease.intent.idempotency_key,
                    _INTENTS.c.state == IntentState.ACTUATING.value,
                    _INTENTS.c.lease_owner == lease.owner,
                    _INTENTS.c.lease_generation == lease.generation).values(
                        state=state,
                        lease_owner=None,
                        lease_expires_at=None,
                        last_error=error,
                        updated_at=now,
                        terminal_at=(now if state == IntentState.TERMINAL.value
                                     else None)))
            return result.rowcount == 1

    def commit(self, lease: IntentLease, *, replica_id: int,
               replica_record_id: uuid.UUID) -> bool:
        """Record an already-atomic replica/action commit under the lease."""
        if (not isinstance(replica_id, int) or isinstance(replica_id, bool) or
                replica_id < 1 or not isinstance(replica_record_id, uuid.UUID)):
            raise ValueError('Intent commit requires exact replica identity.')
        with self.engine.begin() as connection:
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            replica_exists = connection.execute(
                sqlalchemy.select(_REPLICAS.c.replica_id).where(
                    _REPLICAS.c.service_name == connection.execute(
                        sqlalchemy.select(_INTENTS.c.service_name).where(
                            _INTENTS.c.intent_idempotency_key ==
                            lease.intent.idempotency_key)).scalar_one_or_none(),
                    _REPLICAS.c.replica_id ==
                    replica_id).with_for_update()).scalar_one_or_none()
            if replica_exists is None:
                raise ZeroCostActuationConflict(
                    'Intent commit has no durable replica row.')
            result = connection.execute(
                sqlalchemy.update(_INTENTS).where(
                    _INTENTS.c.intent_idempotency_key ==
                    lease.intent.idempotency_key,
                    _INTENTS.c.state == IntentState.ACTUATING.value,
                    _INTENTS.c.lease_owner == lease.owner,
                    _INTENTS.c.lease_generation == lease.generation,
                    _INTENTS.c.valid_until
                    > now).values(state=IntentState.COMMITTED.value,
                                  lease_owner=None,
                                  lease_expires_at=None,
                                  replica_id=replica_id,
                                  replica_record_id=replica_record_id,
                                  last_error=None,
                                  updated_at=now,
                                  committed_at=now))
            return result.rowcount == 1

    def pending_debits(
        self,
        *,
        service_name: str,
        service_hash: str,
        allocation_generation: int,
        allocation_input_sha256: str,
        allocation_claim_generation: int,
    ) -> tuple[reserved_fill_planner.CommittedFillDebit, ...]:
        """Return live grants as planner debits for the same allocation."""
        with self.engine.begin() as connection:
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            rows = connection.execute(
                sqlalchemy.select(_INTENTS).where(
                    _INTENTS.c.service_name == service_name,
                    _INTENTS.c.service_hash == service_hash,
                    _INTENTS.c.allocation_generation == allocation_generation,
                    _INTENTS.c.allocation_input_sha256 ==
                    allocation_input_sha256,
                    _INTENTS.c.allocation_claim_generation ==
                    allocation_claim_generation).order_by(
                        _INTENTS.c.intent_idempotency_key).with_for_update()
            ).mappings().all()
            _retire_expired_locked(connection, rows, now)
            counts: dict[tuple[str, str], int] = {}
            for row in rows:
                if (row['state'] not in _PENDING_STATES or
                        row['valid_until'] <= now):
                    continue
                key = (str(row['pool_key']), str(row['accelerator']).casefold())
                counts[key] = counts.get(key, 0) + 1
        return tuple(
            reserved_fill_planner.CommittedFillDebit(
                allocation_generation=allocation_generation,
                allocation_input_sha256=allocation_input_sha256,
                allocation_claim_generation=allocation_claim_generation,
                pool_key=pool_key,
                accelerator=accelerator,
                replica_slots=count)
            for (pool_key, accelerator), count in sorted(counts.items()))


def get_service_mode(service_name: str) -> ActuationMode | None:
    """Read the durable mode; unavailable is distinct from direct."""
    try:
        engine = serve_state_schema.get_database_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            return None
        with engine.connect() as connection:
            raw = connection.execute(
                sqlalchemy.select(
                    _SERVICES.c.reserved_fill_actuation_mode).where(
                        _SERVICES.c.name == service_name)).scalar_one_or_none()
    except sqlalchemy.exc.SQLAlchemyError:
        return None
    try:
        return ActuationMode(raw) if raw is not None else None
    except ValueError:
        return None
