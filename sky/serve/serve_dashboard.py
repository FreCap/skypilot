"""Bounded PostgreSQL reads for the SkyServe dashboard."""

import base64
from collections.abc import Collection
from collections.abc import Mapping
import json
import math
import time
import typing
from typing import Any

import sqlalchemy

from sky.serve import serve_state
from sky.serve.serve_statuses import ReplicaStatus
from sky.utils.db import db_utils

PAST_ATTEMPT_STATUSES = frozenset({
    ReplicaStatus.FAILED.value,
    ReplicaStatus.FAILED_INITIAL_DELAY.value,
    ReplicaStatus.FAILED_PROBING.value,
    ReplicaStatus.FAILED_PROVISION.value,
})
CURRENT_OR_UNCERTAIN_SCOPE = 'current_or_uncertain'
PAST_ATTEMPTS_SCOPE = 'past_attempts'
REPLICA_SCOPES = frozenset({CURRENT_OR_UNCERTAIN_SCOPE, PAST_ATTEMPTS_SCOPE})

_CURSOR_VERSION = 1
_CURSOR_PREFIX = f'v{_CURSOR_VERSION}.'
_MAX_CURSOR_LENGTH = 4096


class ServiceNotFoundError(RuntimeError):
    """The requested service is absent or is a worker pool."""


class ServiceHashMismatchError(RuntimeError):
    """The requested service name now identifies another incarnation."""


class InvalidReplicaCursorError(ValueError):
    """A replica cursor is malformed or unsupported."""


class ReplicaCursorMismatchError(ValueError):
    """A cursor belongs to another service incarnation or scope."""


def _postgres_engine() -> sqlalchemy.engine.Engine:
    """Return the authoritative central PostgreSQL engine."""
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('Direct SkyServe dashboard reads require the '
                           'central PostgreSQL database.')
    return engine


def _repeatable_read_connection(
    engine: sqlalchemy.engine.Engine,) -> sqlalchemy.engine.Connection:
    """Open a connection whose transaction observes one stable snapshot."""
    return engine.connect().execution_options(isolation_level='REPEATABLE READ')


def _normalized_status(status: Any) -> str:
    return status if isinstance(status, str) and status else (
        ReplicaStatus.UNKNOWN.value)


def _planned_capacity(replica_state: Any) -> int:
    if not isinstance(replica_state, Mapping):
        return 1
    value = replica_state.get('planned_capacity', 1)
    if (isinstance(value, bool) or not isinstance(value, int) or value < 1):
        return 1
    return value


def _scope_predicate(scope: str) -> sqlalchemy.ColumnElement[bool]:
    """Return the exhaustive physical-row predicate for ``scope``."""
    if scope == PAST_ATTEMPTS_SCOPE:
        return serve_state.replicas_table.c.status.in_(PAST_ATTEMPT_STATUSES)
    if scope == CURRENT_OR_UNCERTAIN_SCOPE:
        # SQL NOT IN does not include NULL. Legacy/null and future states are
        # deliberately current-or-uncertain so they cannot disappear from an
        # operator's view.
        return sqlalchemy.or_(
            serve_state.replicas_table.c.status.is_(None),
            serve_state.replicas_table.c.status.not_in(PAST_ATTEMPT_STATUSES))
    raise ValueError(f'Unknown replica scope: {scope!r}')


def _replica_summary_query(
    service_names: Collection[str] | None = None,) -> sqlalchemy.Select:
    """Build one grouped scan over compact replica state."""
    services = serve_state.services_table
    replicas = serve_state.replicas_table
    has_replica = replicas.c.replica_id.is_not(None)
    raw_planned_capacity = replicas.c.replica_state[
        'planned_capacity'].as_integer()
    valid_planned_capacity = sqlalchemy.case(
        (raw_planned_capacity > 0, raw_planned_capacity), else_=1)
    capacity = sqlalchemy.case(
        (sqlalchemy.and_(services.c.logical_replica_semantics == 1,
                         has_replica), valid_planned_capacity),
        (has_replica, 1),
        else_=0)
    query = (
        sqlalchemy.select(
            services.c.name,
            services.c.hash,
            services.c.logical_replica_semantics,
            replicas.c.status,
            sqlalchemy.func.count(  # pylint: disable=not-callable
                replicas.c.replica_id).label('physical_count'),
            sqlalchemy.func.coalesce(  # pylint: disable=not-callable
                sqlalchemy.func.sum(capacity),  # pylint: disable=not-callable
                0).label('capacity_count'),
        ).select_from(
            services.outerjoin(
                replicas, replicas.c.service_name == services.c.name)).where(
                    services.c.pool == 0))
    if service_names is not None:
        names = list(dict.fromkeys(service_names))
        if not names:
            query = query.where(sqlalchemy.false())
        else:
            query = query.where(services.c.name.in_(names))
    return query.group_by(services.c.name, services.c.hash,
                          services.c.logical_replica_semantics,
                          replicas.c.status).order_by(services.c.name)


def _build_replica_summaries(rows: Collection[Any]) -> list[dict[str, Any]]:
    """Collapse grouped SQL rows into one summary per service."""
    summaries: dict[str, dict[str, Any]] = {}
    for row in rows:
        mapping = getattr(row, '_mapping', row)
        service_name = str(mapping['name'])
        summary = summaries.get(service_name)
        if summary is None:
            logical = bool(mapping['logical_replica_semantics'])
            summary = {
                'service_name': service_name,
                'service_hash': mapping['hash'],
                'replica_unit':
                    ('logical_slot' if logical else 'physical_backend'),
                'replica_status_counts': {},
                'replica_capacity_counts': {},
                'current_or_uncertain_count': 0,
                'past_attempt_count': 0,
            }
            summaries[service_name] = summary
        physical_count = int(mapping['physical_count'])
        if physical_count == 0:
            continue
        status = _normalized_status(mapping['status'])
        capacity_count = int(mapping['capacity_count'])
        summary['replica_status_counts'][status] = physical_count
        summary['replica_capacity_counts'][status] = capacity_count
        if status in PAST_ATTEMPT_STATUSES:
            summary['past_attempt_count'] += physical_count
        else:
            summary['current_or_uncertain_count'] += physical_count
    return list(summaries.values())


def get_replica_summaries(
    service_names: Collection[str] | None = None,) -> dict[str, Any]:
    """Return one persisted summary for each selected non-pool service."""
    engine = _postgres_engine()
    with _repeatable_read_connection(engine) as connection:
        with connection.begin():
            observed_at = time.time()
            rows = connection.execute(
                _replica_summary_query(service_names)).fetchall()
    return {
        'available': True,
        'observed_at': observed_at,
        'summaries': _build_replica_summaries(rows),
    }


def unavailable_replica_summaries(reason: str) -> dict[str, Any]:
    return {
        'available': False,
        'reason': reason,
        'observed_at': None,
        'summaries': [],
    }


def _encode_cursor(service_hash: str, scope: str, first_max_replica_id: int,
                   last_replica_id: int) -> str:
    payload = {
        'hash': service_hash,
        'last': last_replica_id,
        'max': first_max_replica_id,
        'scope': scope,
        'version': _CURSOR_VERSION,
    }
    raw = json.dumps(payload, separators=(',', ':'), sort_keys=True).encode()
    encoded = base64.urlsafe_b64encode(raw).decode().rstrip('=')
    return _CURSOR_PREFIX + encoded


def _decode_cursor(cursor: str, expected_service_hash: str,
                   scope: str) -> tuple[int, int]:
    """Decode and bind a cursor to an exact incarnation and scope."""
    if (not isinstance(cursor, str) or not cursor.startswith(_CURSOR_PREFIX) or
            len(cursor) > _MAX_CURSOR_LENGTH):
        raise InvalidReplicaCursorError('Malformed replica cursor.')
    encoded = cursor[len(_CURSOR_PREFIX):]
    if not encoded:
        raise InvalidReplicaCursorError('Malformed replica cursor.')
    try:
        padding = '=' * (-len(encoded) % 4)
        raw = base64.b64decode(encoded + padding, altchars=b'-_', validate=True)
        payload = json.loads(raw)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError,
            RecursionError) as exc:
        raise InvalidReplicaCursorError('Malformed replica cursor.') from exc
    if (not isinstance(payload, dict) or
            set(payload) != {'hash', 'last', 'max', 'scope', 'version'} or
            payload['version'] != _CURSOR_VERSION or
            not isinstance(payload['hash'], str) or not payload['hash'] or
            not isinstance(payload['scope'], str) or
            payload['scope'] not in REPLICA_SCOPES or
            isinstance(payload['max'], bool) or
            not isinstance(payload['max'], int) or payload['max'] < 1 or
            isinstance(payload['last'], bool) or
            not isinstance(payload['last'], int) or payload['last'] < 1 or
            payload['last'] > payload['max']):
        raise InvalidReplicaCursorError('Malformed replica cursor.')
    if (payload['hash'] != expected_service_hash or payload['scope'] != scope):
        raise ReplicaCursorMismatchError(
            'Replica cursor belongs to another service or scope.')
    return payload['max'], payload['last']


def _service_identity_query(service_name: str) -> sqlalchemy.Select:
    services = serve_state.services_table
    return sqlalchemy.select(
        services.c.hash,
        services.c.logical_replica_semantics,
    ).where(services.c.name == service_name, services.c.pool == 0)


def _bounded_scope_predicate(
    scope: str,
    first_max_replica_id: int | None = None,
    last_replica_id: int | None = None,
) -> sqlalchemy.ColumnElement[bool]:
    replicas = serve_state.replicas_table
    predicates = [_scope_predicate(scope)]
    if first_max_replica_id is not None:
        predicates.append(replicas.c.replica_id <= first_max_replica_id)
    if last_replica_id is not None:
        predicates.append(replicas.c.replica_id < last_replica_id)
    return sqlalchemy.and_(*predicates)


def _first_max_replica_id_query(service_name: str,
                                scope: str) -> sqlalchemy.Select:
    replicas = serve_state.replicas_table
    return sqlalchemy.select(sqlalchemy.func.max(replicas.c.replica_id)).where(
        replicas.c.service_name == service_name, _scope_predicate(scope))


def _replica_total_query(service_name: str, scope: str,
                         first_max_replica_id: int) -> sqlalchemy.Select:
    replicas = serve_state.replicas_table
    return sqlalchemy.select(
        sqlalchemy.func.count()  # pylint: disable=not-callable
    ).where(replicas.c.service_name == service_name,
            _bounded_scope_predicate(scope, first_max_replica_id))


def _replica_page_query(service_name: str, scope: str, limit: int,
                        first_max_replica_id: int,
                        last_replica_id: int | None) -> sqlalchemy.Select:
    """Select at most ``limit + 1`` compact rows after all page filters."""
    replicas = serve_state.replicas_table
    return sqlalchemy.select(
        replicas.c.replica_id,
        replicas.c.status,
        replicas.c.version,
        replicas.c.created_at,
        replicas.c.is_spot,
        replicas.c.replica_state_version,
        replicas.c.replica_state,
    ).where(
        replicas.c.service_name == service_name,
        _bounded_scope_predicate(scope, first_max_replica_id, last_replica_id),
    ).order_by(replicas.c.replica_id.desc()).limit(limit + 1)


def _finite_timestamp(value: Any) -> float | None:
    if (isinstance(value, bool) or not isinstance(value, (int, float)) or
            not math.isfinite(value)):
        return None
    return float(value)


def _serialize_replica_row(row: Any) -> dict[str, Any]:
    """Serialize a compact row without handles, endpoints, or pricing."""
    mapping = getattr(row, '_mapping', row)
    replica_state = mapping['replica_state']
    state = replica_state if isinstance(replica_state, Mapping) else {}
    status_state = state.get('status_property')
    if not isinstance(status_state, Mapping):
        status_state = {}
    created_at = _finite_timestamp(mapping['created_at'])
    ready_at = _finite_timestamp(status_state.get('first_ready_time'))
    if ready_at is not None and ready_at < 0:
        ready_at = None
    time_to_ready = None
    if (created_at is not None and ready_at is not None and
            ready_at >= created_at):
        time_to_ready = ready_at - created_at
    location = state.get('location')
    if not isinstance(location, Mapping):
        location = state.get('resources_override')
    if not isinstance(location, Mapping):
        location = {}
    cloud = location.get('cloud')
    region = location.get('region')
    zone = location.get('zone')
    instance_type = location.get('instance_type')
    accelerators = location.get('accelerators')
    safe_cloud = cloud if isinstance(cloud, str) else None
    safe_region = region if isinstance(region, str) else None
    infra = safe_cloud
    if safe_cloud is not None and safe_region is not None:
        infra = f'{safe_cloud} ({safe_region})'
    safe_accelerators = None
    if isinstance(accelerators, Mapping):
        filtered_accelerators = {
            name: count
            for name, count in accelerators.items()
            if (isinstance(name, str) and name and
                not isinstance(count, bool) and isinstance(count, (
                    int, float)) and math.isfinite(count) and count > 0)
        }
        if filtered_accelerators:
            safe_accelerators = filtered_accelerators
    resource_parts = []
    if isinstance(instance_type, str):
        resource_parts.append(instance_type)
    if safe_accelerators:
        resource_parts.append(', '.join(
            f'{name}:{count}'
            for name, count in sorted(safe_accelerators.items())))
    resources_str = '; '.join(resource_parts) or None
    return {
        'replica_id': int(mapping['replica_id']),
        'status': _normalized_status(mapping['status']),
        'version': mapping['version'],
        'planned_capacity': _planned_capacity(state),
        'is_spot': bool(mapping['is_spot']),
        'created_at': created_at,
        # True provider launch time lives in the cluster record. Direct reads
        # intentionally do not resolve that record; do not mislabel replica
        # row creation time as a cloud launch timestamp.
        'launched_at': None,
        'ready_at': ready_at,
        'time_to_ready_seconds': time_to_ready,
        'cloud': safe_cloud,
        'region': safe_region,
        'zone': zone if isinstance(zone, str) else None,
        'infra': infra,
        'resources_str': resources_str,
        'resources_str_full': resources_str,
        'instance_type':
            (instance_type if isinstance(instance_type, str) else None),
        'accelerators': safe_accelerators,
    }


def get_replica_page(service_name: str, expected_service_hash: str, scope: str,
                     limit: int, cursor: str | None) -> dict[str, Any]:
    """Return one stable descending page of lightweight replica rows."""
    if scope not in REPLICA_SCOPES:
        raise ValueError(f'Unknown replica scope: {scope!r}')
    cursor_max = None
    cursor_last = None
    if cursor is not None:
        cursor_max, cursor_last = _decode_cursor(cursor, expected_service_hash,
                                                 scope)

    engine = _postgres_engine()
    with _repeatable_read_connection(engine) as connection:
        with connection.begin():
            identity = connection.execute(
                _service_identity_query(service_name)).fetchone()
            if identity is None:
                raise ServiceNotFoundError(service_name)
            service_hash, logical_replica_semantics = identity
            if service_hash != expected_service_hash:
                raise ServiceHashMismatchError(service_name)
            first_max_replica_id = cursor_max
            if first_max_replica_id is None:
                first_max_replica_id = connection.execute(
                    _first_max_replica_id_query(service_name,
                                                scope)).scalar_one()
            observed_at = time.time()
            if first_max_replica_id is None:
                total = 0
                rows = []
            else:
                total = int(
                    connection.execute(
                        _replica_total_query(
                            service_name, scope,
                            first_max_replica_id)).scalar_one())
                rows = connection.execute(
                    _replica_page_query(service_name, scope, limit,
                                        first_max_replica_id,
                                        cursor_last)).fetchall()

    has_more = len(rows) > limit
    visible_rows = rows[:limit]
    next_cursor = None
    if has_more:
        last_mapping = typing.cast(
            Mapping[str, Any],
            getattr(visible_rows[-1], '_mapping', visible_rows[-1]))
        next_cursor = _encode_cursor(expected_service_hash, scope,
                                     first_max_replica_id,
                                     int(last_mapping['replica_id']))
    return {
        'available': True,
        'service_name': service_name,
        'service_hash': expected_service_hash,
        'scope': scope,
        'replica_unit':
            ('logical_slot' if logical_replica_semantics else 'physical_backend'
            ),
        'observed_at': observed_at,
        'total': total,
        'replicas': [_serialize_replica_row(row) for row in visible_rows],
        'next_cursor': next_cursor,
    }


def unavailable_replica_page(service_name: str, service_hash: str, scope: str,
                             reason: str) -> dict[str, Any]:
    return {
        'available': False,
        'reason': reason,
        'service_name': service_name,
        'service_hash': service_hash,
        'scope': scope,
        'replica_unit': None,
        'observed_at': None,
        'total': 0,
        'replicas': [],
        'next_cursor': None,
    }
