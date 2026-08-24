"""PostgreSQL authority for exact asynchronous dispatch and safe replay.

This ledger stores operational dispatch receipts only. Durable S3 results and
completion markers remain the result authority; PostgreSQL fences dispatch and
indexes authenticated completion observations.
"""
from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import enum
import hashlib
import json
import os
import re
import threading
from typing import Any
import uuid
import weakref

import sqlalchemy

from sky.serve import async_request_ledger_schema
from sky.serve import constants
from sky.serve import kubernetes_identity
from sky.serve import kueue_lane_lineage_schema
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_state_schema
from sky.serve import serve_statuses
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

PROTOCOL_VERSION = 1
SCHEMA_MIN_VERSION = '058'
MAX_LEDGER_PAYLOAD_BYTES = 16 * 1024
_MAX_TEXT_BYTES = 2048
_MAX_COUNTER = (1 << 63) - 1
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_TERMINAL_STATES = frozenset(('SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED'))

_REQUESTS_LEDGER = async_request_ledger_schema.serve_async_requests_table
_ATTEMPTS = async_request_ledger_schema.serve_async_request_attempts_table
_SERVICES = serve_state_schema.services_table
_REPLICAS = serve_state_schema.replicas_table
_VERSION_SPECS = serve_state_schema.version_specs_table
_HEADS = route_projection_schema.serve_route_heads_table
_SNAPSHOTS = route_projection_schema.serve_route_snapshots_table
_KUEUE_ADMISSIONS = kueue_lane_lineage_schema.serve_kueue_admissions_table

_SCHEMA_AVAILABLE_ENGINES: weakref.WeakKeyDictionary[
    sqlalchemy.engine.Engine, int] = weakref.WeakKeyDictionary()
_SCHEMA_AVAILABLE_LOCK = threading.Lock()


def schema_available(engine: sqlalchemy.engine.Engine | None = None) -> bool:
    """Read the Alembic head until this process confirms Serve058 or newer.

    Serve migrations are forward-only, so a positive result is safe to cache
    for this engine and process.  Negative and failed reads remain uncached so
    a completed migration becomes visible without restarting the API server.
    The PID value prevents a forked child from inheriting its parent's proof.
    """
    try:
        postgres_engine = _postgres_engine(engine)
    except (RuntimeError, sqlalchemy.exc.SQLAlchemyError, ValueError):
        return False
    process_id = os.getpid()
    with _SCHEMA_AVAILABLE_LOCK:
        if _SCHEMA_AVAILABLE_ENGINES.get(postgres_engine) == process_id:
            return True
        try:
            revision = migration_utils.get_current_alembic_revision(
                postgres_engine, migration_utils.SERVE_DB_NAME)
            available = (revision is not None and revision.isdecimal() and
                         int(revision) >= int(SCHEMA_MIN_VERSION))
        except (RuntimeError, sqlalchemy.exc.SQLAlchemyError, ValueError):
            return False
        if available:
            _SCHEMA_AVAILABLE_ENGINES[postgres_engine] = process_id
        return available


class AsyncRequestState(enum.Enum):
    """Closed current-attempt operational state machine."""

    REJECTED_PRE_DISPATCH = 'REJECTED_PRE_DISPATCH'
    DISPATCH_MAY_HAVE_OCCURRED = 'DISPATCH_MAY_HAVE_OCCURRED'
    ACCEPTED = 'ACCEPTED'
    AMBIGUOUS = 'AMBIGUOUS'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'
    EXPIRED = 'EXPIRED'

    @property
    def is_terminal(self) -> bool:
        return self.value in _TERMINAL_STATES


class AsyncRequestLedgerError(ValueError):
    """Base class for a rejected ledger operation."""


class AsyncRequestLedgerConflict(AsyncRequestLedgerError):
    """The operation conflicts with a durable receipt or incarnation."""


class AsyncRequestLedgerRouteAuthorityConflict(AsyncRequestLedgerConflict):
    """A new bind lost route authority before creating a durable attempt.

    This exported type is raised only from ``bind()`` after the exact request
    lock proved that no current attempt exists.  It is therefore safe for the
    load balancer to discard the selected route and run route selection again;
    no provider send or durable request/attempt row can precede it.
    """

    error_code = constants.LB_ASYNC_LEDGER_ROUTE_AUTHORITY_CONFLICT_CODE


class _RouteAuthorityConflict(AsyncRequestLedgerConflict):
    """Internal selected-projection conflict awaiting request-row context."""


class AsyncRequestLedgerNotFound(AsyncRequestLedgerError):
    """No current receipt exists for a read-only bind lookup."""


class AsyncRequestLedgerUnavailable(RuntimeError):
    """The PostgreSQL correctness boundary could not be reached."""


@dataclasses.dataclass(frozen=True)
class AsyncRequestReceipt:
    """One durable state-machine acknowledgement."""

    request_key_sha256: str
    attempt_id: str
    attempt_no: int
    state: str
    revision: int
    duplicate: bool
    dispatch_authorized: bool = False

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def _postgres_engine(
        engine: sqlalchemy.engine.Engine | None = None
) -> sqlalchemy.engine.Engine:
    resolved = engine or serve_state_schema.get_database_engine()
    if resolved.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise AsyncRequestLedgerUnavailable(
            'The asynchronous request ledger requires PostgreSQL.')
    return resolved


def _bounded_text(value: Any,
                  field: str,
                  *,
                  max_bytes: int = _MAX_TEXT_BYTES) -> str:
    if not isinstance(value, str) or not value:
        raise AsyncRequestLedgerError(f'{field} must be a non-empty string.')
    try:
        encoded = value.encode('utf-8')
    except UnicodeEncodeError as error:
        raise AsyncRequestLedgerError(
            f'{field} must be valid UTF-8.') from error
    if len(encoded) > max_bytes:
        raise AsyncRequestLedgerError(f'{field} is too large.')
    return value


def _digest(value: Any, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise AsyncRequestLedgerError(
            f'{field} must be a lowercase SHA-256 digest.')
    return value


def _positive_int(value: Any, field: str) -> int:
    if (not isinstance(value, int) or isinstance(value, bool) or value <= 0 or
            value > _MAX_COUNTER):
        raise AsyncRequestLedgerError(
            f'{field} must be a bounded positive integer.')
    return value


def _nonnegative_int(value: Any, field: str) -> int:
    if (not isinstance(value, int) or isinstance(value, bool) or value < 0 or
            value > _MAX_COUNTER):
        raise AsyncRequestLedgerError(
            f'{field} must be a bounded nonnegative integer.')
    return value


def _canonical_uuid(value: Any, field: str) -> uuid.UUID:
    if not isinstance(value, str):
        raise AsyncRequestLedgerError(
            f'{field} must be a canonical UUID string.')
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise AsyncRequestLedgerError(
            f'{field} must be a canonical UUID string.') from error
    if str(parsed) != value:
        raise AsyncRequestLedgerError(
            f'{field} must be a canonical UUID string.')
    return parsed


def _canonical_json(value: Any, field: str) -> tuple[dict[str, Any], bytes]:
    if not isinstance(value, dict):
        raise AsyncRequestLedgerError(f'{field} must be an object.')
    try:
        encoded = json.dumps(value,
                             sort_keys=True,
                             separators=(',', ':'),
                             allow_nan=False).encode('utf-8')
    except (TypeError, ValueError) as error:
        raise AsyncRequestLedgerError(
            f'{field} must contain canonical JSON values.') from error
    if len(encoded) > MAX_LEDGER_PAYLOAD_BYTES:
        raise AsyncRequestLedgerError(f'{field} is too large.')
    return dict(value), encoded


def _request_identity(request_id: Any) -> tuple[str, str]:
    raw = _bounded_text(
        request_id,
        'request_id',
        max_bytes=constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS * 4)
    if len(raw) > constants.LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS:
        raise AsyncRequestLedgerError('request_id is too large.')
    return raw, hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _dispatch_payload(value: Any) -> dict[str, Any]:
    payload, _ = _canonical_json(value, 'ledger bind payload')
    expected = {
        'protocol_version', 'request_id', 'intent_sha256',
        'route_contract_service_version', 'route_projection_generation',
        'route_projection_sha256', 'route_source_epoch', 'selected_route_url',
        'allow_new_attempt'
    }
    if set(payload) != expected or payload.get(
            'protocol_version') != PROTOCOL_VERSION or payload.get(
                'allow_new_attempt') is not True:
        raise AsyncRequestLedgerError(
            'Ledger bind payload has an unsupported shape.')
    _request_identity(payload['request_id'])
    _digest(payload['intent_sha256'], 'intent_sha256')
    _positive_int(payload['route_contract_service_version'],
                  'route_contract_service_version')
    _positive_int(payload['route_projection_generation'],
                  'route_projection_generation')
    _digest(payload['route_projection_sha256'], 'route_projection_sha256')
    _positive_int(payload['route_source_epoch'], 'route_source_epoch')
    _bounded_text(payload['selected_route_url'],
                  'selected_route_url',
                  max_bytes=4096)
    return payload


def _lookup_payload(value: Any) -> dict[str, Any]:
    payload, _ = _canonical_json(value, 'ledger read-only bind payload')
    if (set(payload) != {
            'protocol_version', 'request_id', 'intent_sha256',
            'allow_new_attempt'
    } or payload.get('protocol_version') != PROTOCOL_VERSION or
            payload.get('allow_new_attempt') is not False):
        raise AsyncRequestLedgerError(
            'Read-only ledger bind payload has an unsupported shape.')
    _request_identity(payload['request_id'])
    _digest(payload['intent_sha256'], 'intent_sha256')
    return payload


def _wire_bool(value: Any, field: str) -> bool:
    if value == 'true':
        return True
    if value == 'false':
        return False
    raise AsyncRequestLedgerConflict(f'Route {field} is not exact.')


def _canonical_location(replica_state: Mapping[str, Any]) -> dict[str, Any]:
    context = replica_state.get('reserved_fill_kubernetes_context')
    physical_uid = replica_state.get('reserved_fill_physical_cluster_uid')
    pool_key = replica_state.get('reserved_fill_pool_key')
    reserved = (context, physical_uid, pool_key)
    if all(isinstance(item, str) and item for item in reserved):
        return {
            'kind': 'kubernetes',
            'kubernetes_context': _bounded_text(context, 'kubernetes_context'),
            'physical_cluster_uid': _bounded_text(physical_uid,
                                                  'physical_cluster_uid'),
            'reserved_pool_key': _bounded_text(pool_key, 'reserved_pool_key'),
        }
    if any(item is not None for item in reserved):
        raise AsyncRequestLedgerConflict(
            'Replica has a partial reserved location identity.')
    raw_location = replica_state.get('location')
    if not isinstance(raw_location, Mapping):
        raise AsyncRequestLedgerConflict(
            'Replica has no canonical dispatch location.')
    cloud = _bounded_text(raw_location.get('cloud'), 'cloud', max_bytes=128)
    region = _bounded_text(raw_location.get('region'), 'region', max_bytes=256)
    zone = raw_location.get('zone')
    if zone is not None:
        zone = _bounded_text(zone, 'zone', max_bytes=256)
    return {
        'kind': 'cloud',
        'cloud': cloud,
        'region': region,
        'zone': zone,
    }


def _worker_admission(connection: sqlalchemy.engine.Connection,
                      service_name: str, service_hash: str,
                      service_version: int, replica_id: int,
                      record_id: uuid.UUID, projected_accelerator: str,
                      projected_count: int, replica_state: Mapping[str, Any],
                      location: Mapping[str, Any]) -> dict[str, Any] | None:
    """Bind Kubernetes worker lineage without inventing device evidence."""
    if location.get('kind') == 'cloud':
        return None
    if location.get('kind') != 'kubernetes':
        raise AsyncRequestLedgerConflict(
            'Replica has an unsupported dispatch location.')
    projection = _digest(
        replica_state.get('reserved_fill_worker_projection_sha256'),
        'reserved_fill_worker_projection_sha256')
    raw_projections = connection.execute(
        sqlalchemy.select(_VERSION_SPECS.c.worker_placement_projections).where(
            _VERSION_SPECS.c.service_name == service_name,
            _VERSION_SPECS.c.version == service_version).with_for_update(
                read=True)).scalar_one_or_none()
    try:
        projections = kubernetes_identity.validate_worker_placement_projections(
            raw_projections, allow_none=False)
        assert projections is not None
        matching_projections = [
            candidate for candidate in projections
            if kubernetes_identity.worker_projection_sha256(candidate) ==
            projection
        ]
    except (AssertionError, TypeError, ValueError) as error:
        raise AsyncRequestLedgerConflict(
            'The bound Kubernetes worker projection is unavailable.') from error
    if len(matching_projections) != 1:
        raise AsyncRequestLedgerConflict(
            'The bound Kubernetes worker projection is not unique.')
    matching_projection = matching_projections[0]
    if (matching_projection['kubernetes_context']
            != location.get('kubernetes_context') or
            str(matching_projection['accelerator_name']).casefold()
            != projected_accelerator.casefold() or
            matching_projection['accelerator_count'] != projected_count):
        raise AsyncRequestLedgerConflict(
            'The bound Kubernetes worker projection changed.')
    expects_kueue = matching_projection.get('kueue_admission') is not None
    admission = connection.execute(
        sqlalchemy.select(_KUEUE_ADMISSIONS).where(
            _KUEUE_ADMISSIONS.c.service_name == service_name,
            _KUEUE_ADMISSIONS.c.replica_id == replica_id).with_for_update(
                read=True)).mappings().one_or_none()
    if admission is None and not expects_kueue:
        # An explicit null Kueue contract (currently East) is the only
        # projection-only path.  Absence of an expected Kueue row fails closed.
        return {
            'kind': 'projection_only',
            'worker_projection_sha256': projection,
            'pod_uid': None,
            'pod_receipt_sha256': None,
            'intent_idempotency_key': None,
        }
    if admission is None:
        raise AsyncRequestLedgerConflict(
            'The selected Kueue worker has no admitted lineage.')
    if not expects_kueue:
        raise AsyncRequestLedgerConflict(
            'A projection-only worker has unexpected Kueue lineage.')
    try:
        admission_record_id = str(admission['replica_record_id'])
        admission_accelerator_count = admission['accelerator_count']
    except (KeyError, TypeError, ValueError) as error:
        raise AsyncRequestLedgerConflict(
            'Kueue admission has malformed replica identity.') from error
    if (type(admission_accelerator_count) is not int or
            admission_accelerator_count <= 0 or
            admission['service_hash'] != service_hash or
            admission['service_version'] != service_version or
            admission_record_id != str(record_id) or
            admission['state'] != 'POLICY_ADMITTED' or
            admission['pool_key'] != location.get('reserved_pool_key') or
            admission['physical_cluster_uid']
            != location.get('physical_cluster_uid') or
            admission['kubernetes_context']
            != location.get('kubernetes_context') or
            str(admission['accelerator']).casefold()
            != projected_accelerator.casefold() or
            admission_accelerator_count != projected_count or
            admission['worker_projection_sha256'] != projection or
            not isinstance(admission['pod_uid'], str) or
            not admission['pod_uid'] or
            not isinstance(admission['pod_receipt_sha256'], str)):
        raise AsyncRequestLedgerConflict(
            'Kueue admission does not match the selected route.')
    return {
        'kind': 'kueue',
        'worker_projection_sha256': projection,
        'pod_uid': _bounded_text(admission['pod_uid'],
                                 'admitted_pod_uid',
                                 max_bytes=512),
        'pod_receipt_sha256': _digest(admission['pod_receipt_sha256'],
                                      'pod_receipt_sha256'),
        'intent_idempotency_key': _digest(admission['intent_idempotency_key'],
                                          'intent_idempotency_key'),
    }


def _advisory_lock(connection: sqlalchemy.engine.Connection, service_name: str,
                   service_hash: str, request_key: str) -> None:
    lock_key = hashlib.sha256(
        f'{service_name}\0{service_hash}\0{request_key}'.encode(
            'utf-8')).hexdigest()
    connection.execute(
        sqlalchemy.text(
            'SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))'),
        {'lock_key': lock_key})


def _receipt(row: Mapping[str, Any],
             *,
             duplicate: bool,
             dispatch_authorized: bool = False) -> AsyncRequestReceipt:
    return AsyncRequestReceipt(request_key_sha256=str(
        row['request_key_sha256']),
                               attempt_id=str(row['attempt_id']),
                               attempt_no=int(row['attempt_no']),
                               state=str(row['state']),
                               revision=int(row['revision']),
                               duplicate=duplicate,
                               dispatch_authorized=dispatch_authorized)


class AsyncRequestLedgerRepository:
    """Incarnation- and attempt-fenced asynchronous request repository."""

    def __init__(self, engine: sqlalchemy.engine.Engine | None = None) -> None:
        self._engine = engine

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        return _postgres_engine(self._engine)

    @staticmethod
    def _locked_request(connection: sqlalchemy.engine.Connection,
                        service_name: str, service_hash: str,
                        request_key: str) -> Mapping[str, Any] | None:
        return connection.execute(
            sqlalchemy.select(_REQUESTS_LEDGER).where(
                _REQUESTS_LEDGER.c.service_name == service_name,
                _REQUESTS_LEDGER.c.service_hash == service_hash,
                _REQUESTS_LEDGER.c.request_key_sha256 ==
                request_key).with_for_update()).mappings().one_or_none()

    @staticmethod
    def _locked_attempt(connection: sqlalchemy.engine.Connection,
                        service_name: str, service_hash: str, request_key: str,
                        attempt_id: uuid.UUID) -> Mapping[str, Any] | None:
        return connection.execute(
            sqlalchemy.select(_ATTEMPTS).where(
                _ATTEMPTS.c.service_name == service_name,
                _ATTEMPTS.c.service_hash == service_hash,
                _ATTEMPTS.c.request_key_sha256 == request_key,
                _ATTEMPTS.c.attempt_id ==
                attempt_id).with_for_update()).mappings().one_or_none()

    @classmethod
    def _locked_current(
        cls, connection: sqlalchemy.engine.Connection, service_name: str,
        service_hash: str, request_key: str
    ) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]:
        request_row = cls._locked_request(connection, service_name,
                                          service_hash, request_key)
        if request_row is None:
            return None, None
        attempt_row = cls._locked_attempt(connection, service_name,
                                          service_hash, request_key,
                                          request_row['current_attempt_id'])
        if (attempt_row is None or
                attempt_row['attempt_no'] != request_row['current_attempt_no']):
            raise AsyncRequestLedgerUnavailable(
                'The current asynchronous attempt pointer is corrupt.')
        return request_row, attempt_row

    @staticmethod
    def _current_owner(connection: sqlalchemy.engine.Connection,
                       service_name: str,
                       service_hash: str) -> Mapping[str, Any]:
        owner = connection.execute(
            sqlalchemy.select(_SERVICES).where(
                _SERVICES.c.name == service_name).with_for_update(
                    read=True)).mappings().one_or_none()
        if owner is None or owner['hash'] != service_hash:
            raise AsyncRequestLedgerConflict('Service incarnation mismatch.')
        if owner['pool'] not in (0, False):
            raise AsyncRequestLedgerConflict(
                'Pools have no asynchronous request ledger.')
        return owner

    @staticmethod
    def _binding(connection: sqlalchemy.engine.Connection,
                 owner: Mapping[str, Any], payload: Mapping[str, Any]) -> dict:
        service_name = str(owner['name'])
        if (owner['route_source_mode'] != 'DURABLE_PROJECTED' or
                owner['status'] in {
                    status.value for status in serve_statuses.ServiceStatus.
                    replica_launch_blocking_statuses()
                }):
            raise AsyncRequestLedgerConflict(
                'The selected route is not current durable authority.')
        if (owner['route_projection_capable'] is not True or
                owner['route_projection_protocol_version'] not in (
                    route_projection.PROTOCOL_VERSION,
                    route_projection.INCREMENTAL_PRODUCER_PROTOCOL_VERSION)):
            raise AsyncRequestLedgerUnavailable(
                'Current controller route capability is unavailable.')
        selected_generation = int(payload['route_projection_generation'])
        snapshot = connection.execute(
            sqlalchemy.select(_SNAPSHOTS).where(
                _SNAPSHOTS.c.service_name == service_name,
                _SNAPSHOTS.c.generation ==
                selected_generation)).mappings().one_or_none()
        if snapshot is None:
            # Fast capacity-only publication can retire G from bounded history
            # before its selected request reaches bind. No row exists yet, so
            # a fresh selection is safe; a digest mismatch on a retained G is
            # not equivalent and remains a generic integrity conflict below.
            raise _RouteAuthorityConflict(
                'The selected route projection is missing, stale, or moved.')
        if snapshot['content_sha256'] != payload['route_projection_sha256']:
            raise AsyncRequestLedgerConflict(
                'The selected route projection fence does not match.')
        if (snapshot['service_hash'] != owner['hash'] or
                snapshot['service_lifecycle_epoch']
                != owner['lifecycle_epoch']):
            raise AsyncRequestLedgerConflict(
                'The selected route belongs to another service incarnation.')
        if snapshot['producer_protocol_version'] not in (
                route_projection.PROTOCOL_VERSION,
                route_projection.INCREMENTAL_PRODUCER_PROTOCOL_VERSION):
            raise AsyncRequestLedgerUnavailable(
                'The selected route projection producer is unsupported.')
        try:
            response, identities = (route_projection.RouteProjectionRepository.
                                    validate_snapshot_row(snapshot))
        except route_projection.RouteProjectionError as error:
            raise AsyncRequestLedgerUnavailable(
                'The selected route projection is corrupt.') from error
        selected_url = str(payload['selected_route_url'])
        selected_identity = identities.get(selected_url)
        selected_wire = response.get('replica_info', {}).get(selected_url)
        if (not isinstance(selected_identity, dict) or
                selected_identity.get('advertised') is not True or
                selected_identity.get('alias_expires_at') is not None or
                not isinstance(selected_wire, dict)):
            raise AsyncRequestLedgerConflict(
                'The selected route has no advertised identity.')
        # Validate a retained immutable G before classifying mutable-head or
        # owner movement as safely retryable.  A stale H must not turn a forged
        # digest, corrupt retained G, or arbitrary URL into fresh-selection
        # authority.
        if (owner['current_version']
                != payload['route_contract_service_version'] or
                owner['route_source_epoch'] != payload['route_source_epoch'] or
                owner['route_projection_controller_incarnation']
                != owner['controller_incarnation']):
            raise _RouteAuthorityConflict(
                'The selected route projection is missing, stale, or moved.')
        if (not route_projection.snapshot_owner_matches(snapshot, owner) or
                snapshot['producer_protocol_version']
                != owner['route_projection_protocol_version']):
            raise _RouteAuthorityConflict(
                'The selected route projection is missing, stale, or moved.')
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        head = connection.execute(
            sqlalchemy.select(_HEADS).where(
                _HEADS.c.service_name == service_name).with_for_update(
                    read=True)).mappings().one_or_none()
        if head is None or head['valid_until'] <= now:
            raise _RouteAuthorityConflict(
                'The selected route projection is missing, stale, or moved.')
        head_generation = int(head['generation'])
        if head_generation < selected_generation:
            # Generations are monotonic within one validated owner lineage.
            # Never reinterpret an older H as commit-time authority, even if
            # this route's projected context happens to be identical.
            raise AsyncRequestLedgerUnavailable(
                'The current route projection head regressed.')
        if head_generation != selected_generation:
            # Full projection generations include volatile capacity telemetry
            # and unrelated fleet churn. A selected immutable snapshot remains
            # valid when the service/routing contract and this exact selected
            # route's public + private identity are unchanged in the fresh
            # head. Live replica, record, cost, and worker-admission checks
            # below still run against current PostgreSQL rows.
            head_snapshot = connection.execute(
                sqlalchemy.select(_SNAPSHOTS).where(
                    _SNAPSHOTS.c.service_name == service_name,
                    _SNAPSHOTS.c.generation ==
                    head_generation)).mappings().one_or_none()
            if (head_snapshot is None or
                    not route_projection.snapshot_owner_matches(
                        head_snapshot, owner) or
                    head_snapshot['producer_protocol_version']
                    != owner['route_projection_protocol_version']):
                raise AsyncRequestLedgerUnavailable(
                    'The current route projection is corrupt or unowned.')
            try:
                head_response, head_identities = (
                    route_projection.RouteProjectionRepository.
                    validate_snapshot_row(head_snapshot))
            except route_projection.RouteProjectionError as error:
                raise AsyncRequestLedgerUnavailable(
                    'The current route projection is corrupt.') from error
            if (response.get('service_version')
                    == head_response.get('service_version') and
                    response.get('routing_spec')
                    != head_response.get('routing_spec')):
                # A routing contract is immutable within a service version.
                # Treat same-version drift as corruption, not normal authority
                # movement that a fresh LB selection could safely cure.
                raise AsyncRequestLedgerUnavailable(
                    'The current route projection routing contract diverged.')
            selected_context = (route_projection.selected_route_context_sha256(
                response, identities, selected_url))
            head_context = route_projection.selected_route_context_sha256(
                head_response, head_identities, selected_url)
            if selected_context != head_context:
                raise _RouteAuthorityConflict(
                    'The selected route projection is missing, stale, or moved.'
                )
            # H, not the retained selection G, is the commit-time authority.
            # Context equality proves that H still contains the same stable
            # route identities; all identity/wire derivation and the durable
            # audit fence therefore use H below.
            snapshot = head_snapshot
            response = head_response
            identities = head_identities
        identity = identities.get(selected_url)
        wire = response.get('replica_info', {}).get(selected_url)
        if (not isinstance(identity, dict) or
                identity.get('advertised') is not True or
                identity.get('alias_expires_at') is not None or
                not isinstance(wire, dict)):
            raise AsyncRequestLedgerConflict(
                'The selected route has no current advertised identity.')
        replica_id = _positive_int(identity.get('replica_id'), 'replica_id')
        record_id = _canonical_uuid(identity.get('replica_record_id'),
                                    'replica_record_id')
        selected_worker_service_version = _positive_int(
            identity.get('service_version'), 'selected_worker_service_version')
        projected_accelerator = _bounded_text(identity.get('gpu_type'),
                                              'projected_accelerator',
                                              max_bytes=128)
        projected_count = _positive_int(identity.get('gpu_count'),
                                        'projected_accelerator_count')
        if (wire.get('gpu_type') != projected_accelerator or
                wire.get('gpu_count') != str(projected_count)):
            raise AsyncRequestLedgerConflict(
                'The public route shape disagrees with its private identity.')
        is_zero_cost = _wire_bool(wire.get('is_zero_cost'), 'is_zero_cost')
        replica = connection.execute(
            sqlalchemy.select(
                _REPLICAS.c.version, _REPLICAS.c.status,
                _REPLICAS.c.replica_state).where(
                    _REPLICAS.c.service_name == service_name,
                    _REPLICAS.c.replica_id == replica_id).with_for_update(
                        read=True)).mappings().one_or_none()
        try:
            raw_active_versions = owner['active_versions']
            active_versions = set(
                json.loads(raw_active_versions) if raw_active_versions else [])
        except (KeyError, TypeError, ValueError) as error:
            raise AsyncRequestLedgerUnavailable(
                'The service active-version authority is corrupt.') from error
        if (any(
                type(version) is not int or version < 1
                for version in active_versions) or
                selected_worker_service_version not in active_versions):
            raise AsyncRequestLedgerConflict(
                'The selected worker version is no longer active.')
        if (replica is None or
                replica['version'] != selected_worker_service_version or
                replica['status'] != serve_statuses.ReplicaStatus.READY.value or
                not isinstance(replica['replica_state'], dict)):
            raise AsyncRequestLedgerConflict(
                'The selected replica is no longer current and ready.')
        replica_state = replica['replica_state']
        if (_canonical_uuid(replica_state.get('replica_record_id'),
                            'replica_record_id') != record_id or
                replica_state.get('is_zero_cost') is not is_zero_cost):
            raise AsyncRequestLedgerConflict(
                'The selected replica record or cost provenance changed.')
        location = _canonical_location(replica_state)
        worker_admission = _worker_admission(connection, service_name,
                                             str(owner['hash']),
                                             selected_worker_service_version,
                                             replica_id, record_id,
                                             projected_accelerator,
                                             projected_count, replica_state,
                                             location)
        binding = {
            'schema_version': 1,
            'route_contract_service_version': int(
                payload['route_contract_service_version']),
            'selected_worker_service_version':
                (selected_worker_service_version),
            'route_projection_generation': int(head_generation),
            'route_projection_sha256': str(snapshot['content_sha256']),
            'route_source_epoch': int(payload['route_source_epoch']),
            'replica_id': replica_id,
            'replica_record_id': str(record_id),
            'projected_accelerator': projected_accelerator,
            'projected_accelerator_count': projected_count,
            'is_zero_cost': is_zero_cost,
            'location': location,
            'worker_admission': worker_admission,
        }
        _canonical_json(binding, 'dispatch_binding')
        return binding

    def lookup_current(self, service_name: str, service_hash: str,
                       raw_payload: Any) -> AsyncRequestReceipt:
        """Return a current receipt without creating or advancing an attempt."""
        service_name = _bounded_text(service_name,
                                     'service_name',
                                     max_bytes=512)
        service_hash = _bounded_text(service_hash,
                                     'service_hash',
                                     max_bytes=512)
        payload = _lookup_payload(raw_payload)
        _, request_key = _request_identity(payload['request_id'])
        intent_sha256 = _digest(payload['intent_sha256'], 'intent_sha256')
        try:
            with self.engine.begin() as connection:
                self._current_owner(connection, service_name, service_hash)
                _advisory_lock(connection, service_name, service_hash,
                               request_key)
                request_row, current = self._locked_current(
                    connection, service_name, service_hash, request_key)
                if request_row is None or current is None:
                    raise AsyncRequestLedgerNotFound(
                        'No durable request attempt exists.')
                if request_row['intent_sha256'] != intent_sha256:
                    raise AsyncRequestLedgerConflict(
                        'Stable request ID was reused for a different intent.')
                return _receipt(current, duplicate=True)
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise AsyncRequestLedgerUnavailable(
                'PostgreSQL request lookup failed.') from error

    def bind(self, service_name: str, service_hash: str,
             raw_payload: Any) -> AsyncRequestReceipt:
        """Commit the exact selected route before any provider HTTP send."""
        service_name = _bounded_text(service_name,
                                     'service_name',
                                     max_bytes=512)
        service_hash = _bounded_text(service_hash,
                                     'service_hash',
                                     max_bytes=512)
        payload = _dispatch_payload(raw_payload)
        _, request_key = _request_identity(payload['request_id'])
        intent_sha256 = _digest(payload['intent_sha256'], 'intent_sha256')
        try:
            with self.engine.begin() as connection:
                owner = self._current_owner(connection, service_name,
                                            service_hash)
                _advisory_lock(connection, service_name, service_hash,
                               request_key)
                request_row, current = self._locked_current(
                    connection, service_name, service_hash, request_key)
                if current is not None:
                    assert request_row is not None
                    if request_row['intent_sha256'] != intent_sha256:
                        raise AsyncRequestLedgerConflict(
                            'Stable request ID was reused for a different '
                            'intent.')
                    state = AsyncRequestState(current['state'])
                    if state is not AsyncRequestState.REJECTED_PRE_DISPATCH:
                        # Existing-attempt recovery must not depend on a live
                        # route or current projection. A lost acknowledgement
                        # can therefore be recovered even with zero ready URLs.
                        return _receipt(current, duplicate=True)
                else:
                    assert request_row is None
                # Only a genuinely new attempt validates and captures a route.
                try:
                    binding = self._binding(connection, owner, payload)
                except _RouteAuthorityConflict as error:
                    if current is None:
                        # No request/attempt row existed under the exact
                        # advisory + row-lock scope, and _binding() runs before
                        # either insert below.  Expose the only conflict whose
                        # machine-readable response can authorize fresh route
                        # selection without risking a second provider send.
                        raise AsyncRequestLedgerRouteAuthorityConflict(
                            str(error)) from error
                    # A rejected predecessor is still an existing attempt.  It
                    # may permit a caller-directed new attempt, but never an
                    # implicit in-LB replay based on a route conflict.
                    raise AsyncRequestLedgerConflict(str(error)) from error
                now = connection.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                if current is None:
                    attempt_id = uuid.uuid4()
                    connection.execute(
                        sqlalchemy.insert(_REQUESTS_LEDGER).values(
                            service_name=service_name,
                            service_hash=service_hash,
                            request_key_sha256=request_key,
                            intent_sha256=intent_sha256,
                            current_attempt_id=attempt_id,
                            current_attempt_no=1,
                            created_at=now,
                            updated_at=now))
                    row = connection.execute(
                        sqlalchemy.insert(_ATTEMPTS).values(
                            service_name=service_name,
                            service_hash=service_hash,
                            request_key_sha256=request_key,
                            attempt_id=attempt_id,
                            attempt_no=1,
                            state=AsyncRequestState.DISPATCH_MAY_HAVE_OCCURRED.
                            value,
                            revision=1,
                            dispatch_binding=binding,
                            created_at=now,
                            updated_at=now).returning(
                                _ATTEMPTS)).mappings().one()
                    return _receipt(row,
                                    duplicate=False,
                                    dispatch_authorized=True)
                assert request_row is not None
                attempt_id = uuid.uuid4()
                attempt_no = int(current['attempt_no']) + 1
                row = connection.execute(
                    sqlalchemy.insert(_ATTEMPTS).values(
                        service_name=service_name,
                        service_hash=service_hash,
                        request_key_sha256=request_key,
                        attempt_id=attempt_id,
                        attempt_no=attempt_no,
                        state=AsyncRequestState.DISPATCH_MAY_HAVE_OCCURRED.
                        value,
                        revision=1,
                        dispatch_binding=binding,
                        created_at=now,
                        updated_at=now).returning(_ATTEMPTS)).mappings().one()
                connection.execute(
                    sqlalchemy.update(_REQUESTS_LEDGER).where(
                        _REQUESTS_LEDGER.c.service_name == service_name,
                        _REQUESTS_LEDGER.c.service_hash == service_hash,
                        _REQUESTS_LEDGER.c.request_key_sha256 ==
                        request_key).values(current_attempt_id=attempt_id,
                                            current_attempt_no=attempt_no,
                                            updated_at=now))
                return _receipt(row, duplicate=False, dispatch_authorized=True)
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise AsyncRequestLedgerUnavailable(
                'PostgreSQL request bind failed.') from error

    def reject_before_dispatch(self, service_name: str, service_hash: str,
                               request_id: Any,
                               intent_sha256: Any) -> AsyncRequestReceipt:
        """Record a request that provably never selected a provider route."""
        service_name = _bounded_text(service_name,
                                     'service_name',
                                     max_bytes=512)
        service_hash = _bounded_text(service_hash,
                                     'service_hash',
                                     max_bytes=512)
        _, request_key = _request_identity(request_id)
        intent = _digest(intent_sha256, 'intent_sha256')
        try:
            with self.engine.begin() as connection:
                self._current_owner(connection, service_name, service_hash)
                _advisory_lock(connection, service_name, service_hash,
                               request_key)
                request_row, current = self._locked_current(
                    connection, service_name, service_hash, request_key)
                if current is not None:
                    assert request_row is not None
                    if request_row['intent_sha256'] != intent:
                        raise AsyncRequestLedgerConflict(
                            'Stable request ID was reused for a different '
                            'intent.')
                    if current['state'] != (
                            AsyncRequestState.REJECTED_PRE_DISPATCH.value):
                        raise AsyncRequestLedgerConflict(
                            'A durable attempt already exists for this '
                            'request.')
                    return _receipt(current, duplicate=True)
                now = connection.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                attempt_id = uuid.uuid4()
                connection.execute(
                    sqlalchemy.insert(_REQUESTS_LEDGER).values(
                        service_name=service_name,
                        service_hash=service_hash,
                        request_key_sha256=request_key,
                        intent_sha256=intent,
                        current_attempt_id=attempt_id,
                        current_attempt_no=1,
                        created_at=now,
                        updated_at=now))
                row = connection.execute(
                    sqlalchemy.insert(_ATTEMPTS).values(
                        service_name=service_name,
                        service_hash=service_hash,
                        request_key_sha256=request_key,
                        attempt_id=attempt_id,
                        attempt_no=1,
                        state=AsyncRequestState.REJECTED_PRE_DISPATCH.value,
                        revision=1,
                        created_at=now,
                        updated_at=now).returning(_ATTEMPTS)).mappings().one()
                return _receipt(row, duplicate=False)
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise AsyncRequestLedgerUnavailable(
                'PostgreSQL pre-dispatch rejection write failed.') from error

    def _transition(self,
                    service_name: str,
                    service_hash: str,
                    request_id: Any,
                    intent_sha256: Any,
                    attempt_id: Any,
                    attempt_no: Any,
                    expected_revision: Any,
                    operation: str,
                    *,
                    processing_time_us: Any = None,
                    terminal_status: Any = None) -> AsyncRequestReceipt:
        service_name = _bounded_text(service_name,
                                     'service_name',
                                     max_bytes=512)
        service_hash = _bounded_text(service_hash,
                                     'service_hash',
                                     max_bytes=512)
        _, request_key = _request_identity(request_id)
        intent = _digest(intent_sha256, 'intent_sha256')
        attempt = _canonical_uuid(attempt_id, 'attempt_id')
        attempt_number = _positive_int(attempt_no, 'attempt_no')
        revision = _positive_int(expected_revision, 'expected_revision')
        parsed_terminal_status: AsyncRequestState | None = None
        parsed_processing_time: int | None = None
        if operation == 'terminal':
            try:
                parsed_terminal_status = AsyncRequestState(terminal_status)
            except (TypeError, ValueError) as error:
                raise AsyncRequestLedgerError(
                    'terminal_status is unsupported.') from error
            if not parsed_terminal_status.is_terminal:
                raise AsyncRequestLedgerError(
                    'terminal_status is not terminal.')
            parsed_processing_time = _nonnegative_int(processing_time_us,
                                                      'processing_time_us')
        try:
            with self.engine.begin() as connection:
                self._current_owner(connection, service_name, service_hash)
                _advisory_lock(connection, service_name, service_hash,
                               request_key)
                request_row, current = self._locked_current(
                    connection, service_name, service_hash, request_key)
                if request_row is None or current is None:
                    raise AsyncRequestLedgerConflict(
                        'No durable request attempt exists.')
                if request_row['intent_sha256'] != intent or current[
                        'attempt_id'] != attempt or int(
                            current['attempt_no']) != attempt_number:
                    raise AsyncRequestLedgerConflict(
                        'Request intent or attempt fence does not match.')
                state = AsyncRequestState(current['state'])
                current_revision = int(current['revision'])
                if operation == 'terminal' and current_revision < revision:
                    raise AsyncRequestLedgerConflict(
                        'Request revision fence does not match.')
                if operation == 'accepted' and (state in (
                        AsyncRequestState.ACCEPTED, AsyncRequestState.AMBIGUOUS)
                                                or state.is_terminal):
                    return _receipt(current, duplicate=True)
                if operation == 'ambiguous' and (
                        state is AsyncRequestState.AMBIGUOUS or
                        state.is_terminal):
                    return _receipt(current, duplicate=True)
                if (operation == 'rejected' and
                        state is AsyncRequestState.REJECTED_PRE_DISPATCH):
                    return _receipt(current, duplicate=True)
                if operation == 'terminal' and state.is_terminal:
                    if (state is parsed_terminal_status and
                            int(current['processing_time_us'])
                            == parsed_processing_time):
                        return _receipt(current, duplicate=True)
                    raise AsyncRequestLedgerConflict(
                        'A different terminal receipt already exists.')
                if operation != 'terminal' and current_revision != revision:
                    raise AsyncRequestLedgerConflict(
                        'Request revision fence does not match.')
                now = connection.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                values: dict[str, Any] = {
                    'revision': current_revision + 1,
                    'updated_at': now,
                }
                if operation == 'accepted':
                    if state is not AsyncRequestState.DISPATCH_MAY_HAVE_OCCURRED:
                        raise AsyncRequestLedgerConflict(
                            'Only a dispatched attempt can be accepted.')
                    values.update(state=AsyncRequestState.ACCEPTED.value,
                                  accepted_at=now)
                elif operation == 'ambiguous':
                    if state not in (
                            AsyncRequestState.DISPATCH_MAY_HAVE_OCCURRED,
                            AsyncRequestState.ACCEPTED):
                        raise AsyncRequestLedgerConflict(
                            'This attempt cannot become ambiguous.')
                    values['state'] = AsyncRequestState.AMBIGUOUS.value
                elif operation == 'rejected':
                    if state is not AsyncRequestState.DISPATCH_MAY_HAVE_OCCURRED:
                        raise AsyncRequestLedgerConflict(
                            'Only an unsent bound attempt can be rejected.')
                    values['state'] = (
                        AsyncRequestState.REJECTED_PRE_DISPATCH.value)
                elif operation == 'terminal':
                    if state not in (
                            AsyncRequestState.DISPATCH_MAY_HAVE_OCCURRED,
                            AsyncRequestState.ACCEPTED,
                            AsyncRequestState.AMBIGUOUS):
                        raise AsyncRequestLedgerConflict(
                            'This attempt cannot accept a terminal receipt.')
                    assert parsed_terminal_status is not None
                    values.update(state=parsed_terminal_status.value,
                                  accepted_at=(current['accepted_at'] or now),
                                  terminal_at=now,
                                  terminal_status=parsed_terminal_status.value,
                                  processing_time_us=parsed_processing_time)
                else:
                    raise AsyncRequestLedgerError(
                        'Ledger transition operation is unsupported.')
                row = connection.execute(
                    sqlalchemy.update(_ATTEMPTS).where(
                        _ATTEMPTS.c.service_name == service_name,
                        _ATTEMPTS.c.service_hash == service_hash,
                        _ATTEMPTS.c.request_key_sha256 == request_key,
                        _ATTEMPTS.c.attempt_id == attempt).values(
                            **values).returning(_ATTEMPTS)).mappings().one()
                return _receipt(row, duplicate=False)
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise AsyncRequestLedgerUnavailable(
                'PostgreSQL request transition failed.') from error

    def transition(self, service_name: str, service_hash: str,
                   raw_payload: Any) -> AsyncRequestReceipt:
        """Apply one LB-authorized transition from a strict JSON envelope."""
        payload, _ = _canonical_json(raw_payload, 'ledger transition payload')
        required = {
            'protocol_version', 'operation', 'request_id', 'intent_sha256',
            'attempt_id', 'attempt_no', 'expected_revision'
        }
        operation = payload.get('operation')
        terminal_fields = {'processing_time_us', 'terminal_status'}
        expected = (required |
                    terminal_fields if operation == 'terminal' else required)
        if (set(payload) != expected or
                payload.get('protocol_version') != PROTOCOL_VERSION or operation
                not in ('accepted', 'ambiguous', 'rejected', 'terminal')):
            raise AsyncRequestLedgerError(
                'Ledger transition payload has an unsupported shape.')
        return self._transition(
            service_name,
            service_hash,
            payload['request_id'],
            payload['intent_sha256'],
            payload['attempt_id'],
            payload['attempt_no'],
            payload['expected_revision'],
            operation,
            processing_time_us=payload.get('processing_time_us'),
            terminal_status=payload.get('terminal_status'))

    def summary(self, service_name: str, service_hash: str) -> dict[str, Any]:
        """Return exact unique operational counts for one service incarnation."""
        service_name = _bounded_text(service_name,
                                     'service_name',
                                     max_bytes=512)
        service_hash = _bounded_text(service_hash,
                                     'service_hash',
                                     max_bytes=512)
        try:
            with (self.engine.connect().execution_options(
                    isolation_level='REPEATABLE READ') as
                  connection, connection.begin()):
                now = connection.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                current_owner = connection.execute(
                    sqlalchemy.select(_SERVICES.c.hash, _SERVICES.c.pool).where(
                        _SERVICES.c.name == service_name,
                        _SERVICES.c.pool == 0)).mappings().one_or_none()
                if (current_owner is None or
                        current_owner['hash'] != service_hash):
                    raise AsyncRequestLedgerConflict(
                        'Service incarnation mismatch.')
                current_attempts = _REQUESTS_LEDGER.join(
                    _ATTEMPTS,
                    sqlalchemy.and_(
                        _ATTEMPTS.c.service_name ==
                        _REQUESTS_LEDGER.c.service_name,
                        _ATTEMPTS.c.service_hash ==
                        _REQUESTS_LEDGER.c.service_hash,
                        _ATTEMPTS.c.request_key_sha256 ==
                        _REQUESTS_LEDGER.c.request_key_sha256,
                        _ATTEMPTS.c.attempt_id ==
                        _REQUESTS_LEDGER.c.current_attempt_id))
                state_rows = connection.execute(
                    sqlalchemy.select(
                        _ATTEMPTS.c.state,
                        sqlalchemy.func.count().label(  # pylint: disable=not-callable
                            'count')).select_from(current_attempts).where(
                                _REQUESTS_LEDGER.c.service_name == service_name,
                                _REQUESTS_LEDGER.c.service_hash ==
                                service_hash).group_by(
                                    _ATTEMPTS.c.state)).all()
                terminal_rows = connection.execute(
                    sqlalchemy.select(
                        _ATTEMPTS.c.terminal_status,
                        sqlalchemy.func.count().label(  # pylint: disable=not-callable
                            'count')).select_from(current_attempts).where(
                                _REQUESTS_LEDGER.c.service_name == service_name,
                                _REQUESTS_LEDGER.c.service_hash == service_hash,
                                _ATTEMPTS.c.terminal_at.is_not(None)).group_by(
                                    _ATTEMPTS.c.terminal_status)).all()
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise AsyncRequestLedgerUnavailable(
                'PostgreSQL request summary read failed.') from error
        states = {state.value: 0 for state in AsyncRequestState}
        states.update({str(state): int(count) for state, count in state_rows})
        terminal_by_status = {state: 0 for state in sorted(_TERMINAL_STATES)}
        terminal_by_status.update({
            str(state): int(count) for state, count in terminal_rows
        })
        return {
            'available': True,
            'source': 'postgresql_async_request_ledger',
            # Protocol 1 is caller-opt-in. These counts are exact for opted-in
            # work, but cannot replace legacy telemetry until every producer
            # is separately proven to use the protocol.
            'coverage': 'partial',
            'protocol_version': PROTOCOL_VERSION,
            'observed_at': now.timestamp(),
            'service_hash': service_hash,
            'state_counts': states,
            'operational_terminal_receipt_total': sum(
                terminal_by_status.values()),
            'operational_terminal_receipts_by_status': terminal_by_status,
        }


def get_summary(
        service_name: str,
        service_hash: str,
        engine: sqlalchemy.engine.Engine | None = None) -> dict[str, Any]:
    """Read one fail-closed, incarnation-scoped operational summary.

    This is the canonical adapter for API projections.  It keeps schema and
    database failures in the observability-only response contract while the
    repository retains strict exceptions for correctness-sensitive callers.
    """
    if engine is None:
        try:
            engine = _postgres_engine()
        except (AsyncRequestLedgerUnavailable, RuntimeError,
                sqlalchemy.exc.SQLAlchemyError, ValueError):
            return unavailable_summary('postgresql_required')
    if not schema_available(engine):
        return unavailable_summary('schema_unavailable')
    try:
        return AsyncRequestLedgerRepository(engine).summary(
            service_name, service_hash)
    except AsyncRequestLedgerConflict:
        return unavailable_summary('service_incarnation_mismatch')
    except AsyncRequestLedgerUnavailable:
        return unavailable_summary('database_read_failed')


def unavailable_summary(reason: str,
                        *,
                        coverage: str = 'none') -> dict[str, Any]:
    """Return a stable fail-closed shape for status projection."""
    return {
        'available': False,
        'source': 'postgresql_async_request_ledger',
        'reason': reason,
        'coverage': coverage,
        'protocol_version': PROTOCOL_VERSION,
        'observed_at': None,
        'service_hash': None,
        'state_counts': {
            state.value: 0 for state in AsyncRequestState
        },
        'operational_terminal_receipt_total': 0,
        'operational_terminal_receipts_by_status': {
            state: 0 for state in sorted(_TERMINAL_STATES)
        },
    }
