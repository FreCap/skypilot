"""Bounded PostgreSQL reads for the SkyServe dashboard."""

import base64
from collections.abc import Collection
from collections.abc import Mapping
import dataclasses
import hashlib
import json
import math
import time
import typing
from typing import Any
import uuid

import sqlalchemy

from sky.serve import serve_state
from sky.serve import spot_placer
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

PRICE_BASIS = 'version_catalog'
MAX_PRICING_REPLICA_IDS = 100
MAX_PRICING_REPLICA_ID = 2_147_483_647

_COST_UNTRACKED_STATUSES = frozenset({
    ReplicaStatus.PENDING.value,
    ReplicaStatus.FAILED.value,
    ReplicaStatus.FAILED_INITIAL_DELAY.value,
    ReplicaStatus.FAILED_PROBING.value,
    ReplicaStatus.FAILED_PROVISION.value,
    ReplicaStatus.PREEMPTED.value,
})
_MAX_PRICING_TRACKED_ROWS = 10_000
_MAX_PRICING_GROUPS = 4_096
_MAX_PRICING_VERSIONS = 128
_MAX_PRICING_CATALOG_ENTRIES = 10_000
_MAX_PRICING_CATALOG_ENTRIES_PER_CATALOG = 10_000
_MAX_PRICING_CATALOG_BYTES = 8 * 1024 * 1024
_MAX_PRICING_TOTAL_CATALOG_BYTES = 16 * 1024 * 1024
_MAX_PRICING_IDENTITY_BYTES = 64 * 1024

_PRICING_EXCLUSION_REASONS = frozenset({
    'missing_version_catalog',
    'unsupported_version_catalog',
    'invalid_version_catalog',
    'catalog_too_large',
    'missing_location',
    'invalid_location',
    'location_not_in_version_catalog',
    'ambiguous_legacy_location',
    'catalog_price_unavailable',
    'purchase_option_mismatch',
    'unknown_node_count',
    'pricing_identity_too_large',
})

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
        replicas.c.cluster_name,
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


def _canonical_json_bytes(value: Any) -> bytes | None:
    try:
        return json.dumps(value,
                          allow_nan=False,
                          separators=(',', ':'),
                          sort_keys=True).encode()
    except (OverflowError, TypeError, ValueError):
        return None


def _pricing_identity_is_too_large(state: Mapping[str, Any]) -> bool:
    """Bound the two persisted resource-identity documents together."""
    size = 0
    for key in ('location', 'resources_override'):
        if key not in state:
            continue
        try:
            # PostgreSQL JSONB::text uses literal UTF-8 and a space after
            # separators.  Matching that representation keeps direct-page
            # and SQL-projected fingerprint decisions identical at the cap.
            encoded = json.dumps(state[key],
                                 allow_nan=False,
                                 ensure_ascii=False,
                                 separators=(', ', ': '),
                                 sort_keys=True).encode()
        except (OverflowError, TypeError, ValueError):
            return True
        size += len(encoded)
        if size > _MAX_PRICING_IDENTITY_BYTES:
            return True
    return False


def _pricing_fingerprint(
    mapping: Mapping[str, Any],
    *,
    identity_size_validated: bool = False,
) -> str | None:
    """Hash physical record identity and every persisted pricing input."""
    replica_state = mapping.get('replica_state')
    state = replica_state if isinstance(replica_state, Mapping) else {}
    is_zero_cost = state.get('is_zero_cost') is True
    if (not is_zero_cost and not identity_size_validated and
            _pricing_identity_is_too_large(state)):
        return None
    record_id = state.get('replica_record_id')
    canonical_record_id = None
    if (isinstance(record_id, str) and
            len(record_id.encode()) <= _MAX_PRICING_IDENTITY_BYTES):
        try:
            parsed_record_id = uuid.UUID(record_id)
        except (AttributeError, TypeError, ValueError):
            pass
        else:
            if str(parsed_record_id) == record_id:
                canonical_record_id = record_id
    if canonical_record_id is not None:
        record_identity: Any = {'replica_record_id': record_id}
    else:
        # Replica IDs are reusable.  Creation time and cluster name together
        # are the durable fields available on every legacy physical row.
        record_identity = {
            'legacy_replica_id': mapping.get('replica_id'),
            'legacy_cluster_name': mapping.get('cluster_name'),
            'legacy_created_at': mapping.get('created_at'),
        }
    material = {
        'record': record_identity,
        'version': mapping.get('version'),
        'is_spot': mapping.get('is_spot'),
        'is_zero_cost': is_zero_cost,
    }
    if not is_zero_cost:
        raw_location = state.get('location')
        if raw_location is not None:
            material['pricing_location'] = {'location': raw_location}
        else:
            material['pricing_location'] = {
                'resources_override': state.get('resources_override')
            }
    encoded = _canonical_json_bytes(material)
    if encoded is None:
        return None
    return 'v1.' + hashlib.sha256(encoded).hexdigest()


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
        'pricing_fingerprint': _pricing_fingerprint(mapping),
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


def _cost_tracked_predicate() -> sqlalchemy.ColumnElement[bool]:
    replicas = serve_state.replicas_table
    return sqlalchemy.or_(
        replicas.c.status.is_(None),
        replicas.c.status.not_in(_COST_UNTRACKED_STATUSES),
    )


def _exact_zero_cost_expression() -> sqlalchemy.ColumnElement[bool]:
    raw_zero = serve_state.replicas_table.c.replica_state['is_zero_cost']
    return sqlalchemy.func.coalesce(
        sqlalchemy.and_(
            sqlalchemy.func.jsonb_typeof(raw_zero) == 'boolean',
            sqlalchemy.cast(raw_zero, sqlalchemy.Text) == 'true',
        ), sqlalchemy.false())


def _json_octet_length(
        value: sqlalchemy.ColumnElement[Any]) -> sqlalchemy.ColumnElement[int]:
    return sqlalchemy.func.coalesce(
        sqlalchemy.func.octet_length(sqlalchemy.cast(value, sqlalchemy.Text)),
        0)


def _pricing_identity_size_expression() -> sqlalchemy.ColumnElement[int]:
    state = serve_state.replicas_table.c.replica_state
    return (_json_octet_length(state['location']) +
            _json_octet_length(state['resources_override']))


def _pricing_row_probe_query(service_name: str) -> sqlalchemy.Select:
    """Probe physical cardinality before any JSON projection or grouping."""
    replicas = serve_state.replicas_table
    return sqlalchemy.select(replicas.c.replica_id).where(
        replicas.c.service_name == service_name,
        _cost_tracked_predicate(),
    ).limit(_MAX_PRICING_TRACKED_ROWS + 1)


def _pricing_group_query(service_name: str) -> sqlalchemy.Select:
    """Build the bounded aggregate grouping over safe persisted identity."""
    replicas = serve_state.replicas_table
    state = replicas.c.replica_state
    is_zero_cost = _exact_zero_cost_expression()
    identity_too_large = sqlalchemy.case(
        (is_zero_cost, sqlalchemy.false()),
        else_=_pricing_identity_size_expression() > _MAX_PRICING_IDENTITY_BYTES,
    )
    omit_identity = sqlalchemy.or_(is_zero_cost, identity_too_large)
    location = sqlalchemy.case(
        (omit_identity, sqlalchemy.null()),
        else_=state['location'],
    )
    resources_override = sqlalchemy.case(
        (omit_identity, sqlalchemy.null()),
        else_=state['resources_override'],
    )
    group_expressions = (
        replicas.c.version,
        replicas.c.status,
        replicas.c.is_spot,
        is_zero_cost,
        identity_too_large,
        location,
        resources_override,
    )
    return sqlalchemy.select(
        replicas.c.version,
        replicas.c.status,
        replicas.c.is_spot,
        is_zero_cost.label('is_zero_cost'),
        identity_too_large.label('pricing_identity_too_large'),
        location.label('location'),
        resources_override.label('resources_override'),
        sqlalchemy.func.count().label(  # pylint: disable=not-callable
            'physical_count'),
    ).where(
        replicas.c.service_name == service_name,
        _cost_tracked_predicate(),
    ).group_by(*group_expressions).limit(_MAX_PRICING_GROUPS + 1)


def _pricing_replica_query(service_name: str,
                           replica_ids: Collection[int]) -> sqlalchemy.Select:
    """Select only bounded price identity for requested current rows."""
    replicas = serve_state.replicas_table
    state = replicas.c.replica_state
    is_zero_cost = _exact_zero_cost_expression()
    identity_too_large = sqlalchemy.case(
        (is_zero_cost, sqlalchemy.false()),
        else_=_pricing_identity_size_expression() > _MAX_PRICING_IDENTITY_BYTES,
    )
    omit_identity = sqlalchemy.or_(is_zero_cost, identity_too_large)
    location = sqlalchemy.case((omit_identity, sqlalchemy.null()),
                               else_=state['location'])
    resources_override = sqlalchemy.case((omit_identity, sqlalchemy.null()),
                                         else_=state['resources_override'])
    record_id_json = state['replica_record_id']
    record_id = sqlalchemy.case(
        (sqlalchemy.and_(
            sqlalchemy.func.jsonb_typeof(record_id_json) == 'string',
            _json_octet_length(record_id_json)
            <= _MAX_PRICING_IDENTITY_BYTES), record_id_json.as_string()),
        else_=sqlalchemy.null(),
    )
    ids = list(replica_ids)
    return sqlalchemy.select(
        replicas.c.replica_id,
        replicas.c.status,
        replicas.c.version,
        replicas.c.cluster_name,
        replicas.c.created_at,
        replicas.c.is_spot,
        is_zero_cost.label('is_zero_cost'),
        identity_too_large.label('pricing_identity_too_large'),
        location.label('location'),
        resources_override.label('resources_override'),
        record_id.label('replica_record_id'),
    ).where(
        replicas.c.service_name == service_name,
        replicas.c.replica_id.in_(ids),
        _scope_predicate(CURRENT_OR_UNCERTAIN_SCOPE),
    ).order_by(replicas.c.replica_id).limit(MAX_PRICING_REPLICA_IDS)


def _catalog_metadata_query(service_name: str,
                            versions: Collection[int]) -> sqlalchemy.Select:
    """Inspect bounded JSON metadata before selecting any catalog body."""
    version_specs = serve_state.version_specs_table
    catalog = version_specs.c.placement_catalog
    schema_version = catalog['schema_version']
    entries = catalog['entries']
    entries_type = sqlalchemy.func.jsonb_typeof(entries)
    entry_count = sqlalchemy.case(
        (entries_type == 'array', sqlalchemy.func.jsonb_array_length(entries)),
        else_=sqlalchemy.null(),
    )
    schema_text = sqlalchemy.case(
        (sqlalchemy.func.jsonb_typeof(schema_version)
         == 'number', sqlalchemy.cast(schema_version, sqlalchemy.Text)),
        else_=sqlalchemy.null(),
    )
    return sqlalchemy.select(
        version_specs.c.version,
        sqlalchemy.func.jsonb_typeof(catalog).label('catalog_type'),
        sqlalchemy.func.jsonb_typeof(schema_version).label('schema_type'),
        schema_text.label('schema_text'),
        entries_type.label('entries_type'),
        entry_count.label('entry_count'),
        sqlalchemy.func.octet_length(sqlalchemy.cast(
            catalog, sqlalchemy.Text)).label('catalog_bytes'),
    ).where(
        version_specs.c.service_name == service_name,
        version_specs.c.version.in_(list(versions)),
    ).order_by(version_specs.c.version)


def _catalog_body_query(service_name: str,
                        versions: Collection[int]) -> sqlalchemy.Select:
    version_specs = serve_state.version_specs_table
    return sqlalchemy.select(
        version_specs.c.version,
        sqlalchemy.cast(version_specs.c.placement_catalog,
                        sqlalchemy.Text).label('placement_catalog_text'),
    ).where(
        version_specs.c.service_name == service_name,
        version_specs.c.version.in_(list(versions)),
    ).order_by(version_specs.c.version)


def _row_mapping(row: Any) -> Mapping[str, Any]:
    return typing.cast(Mapping[str, Any], getattr(row, '_mapping', row))


@dataclasses.dataclass(frozen=True)
class _PricingCatalogResolver:
    """One request-local, pre-indexed immutable version catalog."""

    catalog: spot_placer.PlacementCatalog
    locations: spot_placer.CatalogLocationIndex
    costs: Mapping[spot_placer.Location, float]

    @classmethod
    def from_catalog(
            cls,
            catalog: spot_placer.PlacementCatalog) -> '_PricingCatalogResolver':
        costs = catalog.costs()
        return cls(catalog,
                   spot_placer.CatalogLocationIndex.from_locations(costs),
                   costs)


def _load_version_catalogs(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
    versions: Collection[Any],
    *,
    aggregate_mode: bool,
) -> tuple[dict[Any, _PricingCatalogResolver | str], bool]:
    """Load only bounded, structurally supported catalog bodies.

    The boolean result reports an aggregate-wide projection bound.  In ID
    mode, bounds are instead materialized as ``catalog_too_large`` for every
    affected catalog-dependent row so every requested ID settles.
    """
    distinct_versions = list(dict.fromkeys(versions))
    catalogs: dict[Any, _PricingCatalogResolver | str] = {
        version: 'missing_version_catalog' for version in distinct_versions
    }
    query_versions = [
        version for version in distinct_versions
        if (not isinstance(version, bool) and isinstance(version, int))
    ]
    if not query_versions:
        return catalogs, False

    metadata_rows = connection.execute(
        _catalog_metadata_query(service_name, query_versions)).fetchall()
    metadata = {
        _row_mapping(row)['version']: _row_mapping(row) for row in metadata_rows
    }
    total_entries = 0
    total_bytes = 0
    oversized_versions: set[int] = set()
    for version, row in metadata.items():
        entry_count = row.get('entry_count')
        catalog_bytes = row.get('catalog_bytes')
        if isinstance(entry_count, int) and not isinstance(entry_count, bool):
            total_entries += max(0, entry_count)
            if entry_count > _MAX_PRICING_CATALOG_ENTRIES_PER_CATALOG:
                oversized_versions.add(version)
        if isinstance(catalog_bytes,
                      int) and not isinstance(catalog_bytes, bool):
            total_bytes += max(0, catalog_bytes)
            if catalog_bytes > _MAX_PRICING_CATALOG_BYTES:
                oversized_versions.add(version)
    total_too_large = (total_entries > _MAX_PRICING_CATALOG_ENTRIES or
                       total_bytes > _MAX_PRICING_TOTAL_CATALOG_BYTES)
    if aggregate_mode and (oversized_versions or total_too_large):
        return catalogs, True
    if total_too_large:
        for version in query_versions:
            catalogs[version] = 'catalog_too_large'
        return catalogs, False
    for version in oversized_versions:
        catalogs[version] = 'catalog_too_large'

    body_versions = []
    expected_schema = spot_placer.PLACEMENT_CATALOG_SCHEMA_VERSION
    for version in query_versions:
        if version in oversized_versions:
            continue
        metadata_row = metadata.get(version)
        if metadata_row is None or metadata_row.get('catalog_type') is None:
            catalogs[version] = 'missing_version_catalog'
            continue
        if metadata_row.get('catalog_type') != 'object':
            catalogs[version] = 'invalid_version_catalog'
            continue
        schema_text = metadata_row.get('schema_text')
        if metadata_row.get('schema_type') != 'number' or not isinstance(
                schema_text, str):
            catalogs[version] = 'invalid_version_catalog'
            continue
        try:
            schema_version = json.loads(schema_text)
        except (ValueError, RecursionError):
            catalogs[version] = 'invalid_version_catalog'
            continue
        if (isinstance(schema_version, bool) or
                not isinstance(schema_version, int)):
            catalogs[version] = 'invalid_version_catalog'
            continue
        if schema_version != expected_schema:
            catalogs[version] = 'unsupported_version_catalog'
            continue
        if metadata_row.get('entries_type') != 'array' or not isinstance(
                metadata_row.get('entry_count'), int):
            catalogs[version] = 'invalid_version_catalog'
            continue
        body_versions.append(version)

    if body_versions:
        body_rows = connection.execute(
            _catalog_body_query(service_name, body_versions)).fetchall()
        bodies = {
            _row_mapping(row)['version']:
                _row_mapping(row)['placement_catalog_text'] for row in body_rows
        }
        for version in body_versions:
            raw_catalog_text = bodies.get(version)
            if not isinstance(raw_catalog_text, str):
                catalogs[version] = 'invalid_version_catalog'
                continue
            try:
                raw_catalog = json.loads(raw_catalog_text)
                if not isinstance(raw_catalog, dict):
                    raise ValueError('Placement catalog must be an object.')
                catalog = spot_placer.PlacementCatalog.from_dict(raw_catalog)
                catalogs[version] = _PricingCatalogResolver.from_catalog(
                    catalog)
            except (AssertionError, AttributeError, KeyError, OverflowError,
                    RecursionError, TypeError, ValueError):
                catalogs[version] = 'invalid_version_catalog'
    return catalogs, False


def _decode_replica_resource_identity(
        value: Mapping[str, Any]) -> dict[str, Any]:
    """Restore the lossless JSON form used by ReplicaInfo storage."""
    decoded = dict(value)
    image_id = decoded.get('image_id')
    if isinstance(image_id, list):
        restored: dict[str | None, str] = {}
        for item in image_id:
            if (not isinstance(item, list) or len(item) != 2 or
                    item[0] is not None and not isinstance(item[0], str) or
                    not isinstance(item[1], str) or item[0] in restored):
                raise ValueError('Invalid persisted image identity.')
            restored[item[0]] = item[1]
        decoded['image_id'] = restored
    elif isinstance(image_id, Mapping) and 'null' in image_id:
        decoded['image_id'] = {
            None if region == 'null' else region: image
            for region, image in image_id.items()
        }
    return decoded


def _persisted_location(
    raw_location: Any,
    raw_override: Any,
) -> tuple[spot_placer.Location | None, str | None]:
    """Decode exact location, or the one permitted pinned-override fallback."""
    if raw_location is not None:
        if not isinstance(raw_location, Mapping):
            return None, 'invalid_location'
        try:
            location = spot_placer.Location.from_pickleable(
                _decode_replica_resource_identity(raw_location))
        except (AssertionError, AttributeError, KeyError, RecursionError,
                TypeError, ValueError):
            return None, 'invalid_location'
        if location is None:
            return None, 'invalid_location'
        return location, None
    if raw_override is None:
        return None, 'missing_location'
    if not isinstance(raw_override, Mapping):
        return None, 'invalid_location'
    try:
        decoded_override = _decode_replica_resource_identity(raw_override)
        location = spot_placer.Location.from_resources_override(
            decoded_override)
    except (AssertionError, AttributeError, KeyError, RecursionError, TypeError,
            ValueError):
        return None, 'invalid_location'
    if location is None:
        return None, 'missing_location'
    return location, None


def _resolve_persisted_price(
    row: Mapping[str, Any],
    catalogs: Mapping[Any, _PricingCatalogResolver | str],
) -> tuple[float | None, str | None, str | None]:
    """Resolve one row/group using only immutable persisted inputs."""
    if row.get('is_zero_cost') is True:
        return 0.0, 'zero_cost_provenance', None
    if row.get('pricing_identity_too_large') is True:
        return None, None, 'pricing_identity_too_large'
    catalog_or_reason = catalogs.get(row.get('version'),
                                     'missing_version_catalog')
    if isinstance(catalog_or_reason, str):
        assert catalog_or_reason in _PRICING_EXCLUSION_REASONS
        return None, None, catalog_or_reason

    location, location_error = _persisted_location(
        row.get('location'), row.get('resources_override'))
    if location_error is not None:
        return None, None, location_error
    assert location is not None
    try:
        matched, ambiguous = spot_placer.match_catalog_location_strict(
            location, catalog_or_reason.locations)
    except (AttributeError, TypeError, ValueError):
        return None, None, 'invalid_location'
    if matched is None:
        reason = ('ambiguous_legacy_location'
                  if ambiguous else 'location_not_in_version_catalog')
        return None, None, reason
    purchase_option = row.get('is_spot')
    if not isinstance(purchase_option,
                      bool) or purchase_option != matched.use_spot:
        return None, None, 'purchase_option_mismatch'
    hourly_cost = catalog_or_reason.costs.get(matched)
    if hourly_cost is None or not math.isfinite(hourly_cost):
        return None, None, 'catalog_price_unavailable'
    if hourly_cost == 0:
        return 0.0, PRICE_BASIS, None
    num_nodes = catalog_or_reason.catalog.num_nodes
    if num_nodes is None:
        return None, None, 'unknown_node_count'
    # PlacementCatalog validates positive prices and a positive strict integer
    # node count.  Retain a defensive finite check before placing it on wire.
    try:
        total_cost = hourly_cost * num_nodes
    except OverflowError:
        return None, None, 'catalog_price_unavailable'
    if not math.isfinite(total_cost):
        return None, None, 'catalog_price_unavailable'
    return total_cost, PRICE_BASIS, None


def _unavailable_aggregate() -> dict[str, Any]:
    return {
        'available': False,
        'unavailable_reason': 'projection_too_large',
        'coverage': None,
        'known_hourly_cost': None,
        'spot_hourly_cost': None,
        'non_spot_hourly_cost': None,
        'tracked_replica_count': None,
        'priced_replica_count': None,
        'excluded_replica_count': None,
        'exclusion_reasons': None,
    }


def _empty_aggregate() -> dict[str, Any]:
    return {
        'available': True,
        'unavailable_reason': None,
        'coverage': 'empty',
        'known_hourly_cost': 0.0,
        'spot_hourly_cost': 0.0,
        'non_spot_hourly_cost': 0.0,
        'tracked_replica_count': 0,
        'priced_replica_count': 0,
        'excluded_replica_count': 0,
        'exclusion_reasons': {},
    }


def _aggregate_pricing(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> dict[str, Any]:
    probe_rows = connection.execute(
        _pricing_row_probe_query(service_name)).fetchall()
    if len(probe_rows) > _MAX_PRICING_TRACKED_ROWS:
        return _unavailable_aggregate()
    if not probe_rows:
        return _empty_aggregate()

    group_rows = connection.execute(
        _pricing_group_query(service_name)).fetchall()
    if len(group_rows) > _MAX_PRICING_GROUPS:
        return _unavailable_aggregate()
    groups = [_row_mapping(row) for row in group_rows]
    tracked_count = sum(int(row['physical_count']) for row in groups)
    if tracked_count > _MAX_PRICING_TRACKED_ROWS:
        return _unavailable_aggregate()
    live_versions = list(dict.fromkeys(row.get('version') for row in groups))
    if len(live_versions) > _MAX_PRICING_VERSIONS:
        return _unavailable_aggregate()
    catalog_groups = [
        row for row in groups if row.get('is_zero_cost') is not True and
        row.get('pricing_identity_too_large') is not True
    ]
    versions = list(dict.fromkeys(row.get('version') for row in catalog_groups))
    catalogs, projection_too_large = _load_version_catalogs(
        connection,
        service_name,
        versions,
        aggregate_mode=True,
    )
    if projection_too_large:
        return _unavailable_aggregate()

    known_hourly_cost = 0.0
    spot_hourly_cost = 0.0
    non_spot_hourly_cost = 0.0
    priced_count = 0
    exclusion_reasons: dict[str, int] = {}
    for row in groups:
        physical_count = int(row['physical_count'])
        cost, _, reason = _resolve_persisted_price(row, catalogs)
        if reason is not None:
            exclusion_reasons[reason] = (exclusion_reasons.get(reason, 0) +
                                         physical_count)
            continue
        assert cost is not None
        group_cost = cost * physical_count
        next_known_cost = known_hourly_cost + group_cost
        if row.get('is_spot') is True:
            next_spot_cost = spot_hourly_cost + group_cost
            next_non_spot_cost = non_spot_hourly_cost
        else:
            next_spot_cost = spot_hourly_cost
            next_non_spot_cost = non_spot_hourly_cost + group_cost
        if not all(
                math.isfinite(value)
                for value in (group_cost, next_known_cost, next_spot_cost,
                              next_non_spot_cost)):
            exclusion_reasons['catalog_price_unavailable'] = (
                exclusion_reasons.get('catalog_price_unavailable', 0) +
                physical_count)
            continue
        known_hourly_cost = next_known_cost
        spot_hourly_cost = next_spot_cost
        non_spot_hourly_cost = next_non_spot_cost
        priced_count += physical_count

    excluded_count = tracked_count - priced_count
    wire_known_hourly_cost: float | None = known_hourly_cost
    wire_spot_hourly_cost: float | None = spot_hourly_cost
    wire_non_spot_hourly_cost: float | None = non_spot_hourly_cost
    if excluded_count == 0:
        coverage = 'complete'
    elif priced_count == 0:
        coverage = 'none'
        wire_known_hourly_cost = None
        wire_spot_hourly_cost = None
        wire_non_spot_hourly_cost = None
    else:
        coverage = 'partial'
    return {
        'available': True,
        'unavailable_reason': None,
        'coverage': coverage,
        'known_hourly_cost': wire_known_hourly_cost,
        'spot_hourly_cost': wire_spot_hourly_cost,
        'non_spot_hourly_cost': wire_non_spot_hourly_cost,
        'tracked_replica_count': tracked_count,
        'priced_replica_count': priced_count,
        'excluded_replica_count': excluded_count,
        'exclusion_reasons': dict(sorted(exclusion_reasons.items())),
    }


def _projected_pricing_fingerprint(row: Mapping[str, Any]) -> str | None:
    if (row.get('pricing_identity_too_large') is True and
            row.get('is_zero_cost') is not True):
        return None
    synthetic_state = {
        'location': row.get('location'),
        'resources_override': row.get('resources_override'),
        'is_zero_cost': row.get('is_zero_cost') is True,
        'replica_record_id': row.get('replica_record_id'),
    }
    synthetic_row = {
        'replica_id': row.get('replica_id'),
        'cluster_name': row.get('cluster_name'),
        'created_at': row.get('created_at'),
        'version': row.get('version'),
        'is_spot': row.get('is_spot'),
        'replica_state': synthetic_state,
    }
    return _pricing_fingerprint(synthetic_row, identity_size_validated=True)


def _replica_pricing(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
    replica_ids: list[int],
) -> list[dict[str, Any]]:
    rows = connection.execute(_pricing_replica_query(service_name,
                                                     replica_ids)).fetchall()
    by_id = {
        int(_row_mapping(row)['replica_id']): _row_mapping(row) for row in rows
    }
    catalog_rows = [
        row for row in by_id.values() if row.get('is_zero_cost') is not True and
        row.get('pricing_identity_too_large') is not True
    ]
    catalogs, _ = _load_version_catalogs(
        connection,
        service_name,
        [row.get('version') for row in catalog_rows],
        aggregate_mode=False,
    )
    results: list[dict[str, Any]] = []
    for replica_id in replica_ids:
        row = by_id.get(replica_id)
        if row is None:
            results.append({
                'replica_id': replica_id,
                'pricing_fingerprint': None,
                'hourly_cost': None,
                'price_source': None,
                'hourly_cost_exclusion_reason': 'not_current_or_uncertain',
            })
            continue
        cost, source, reason = _resolve_persisted_price(row, catalogs)
        results.append({
            'replica_id': replica_id,
            'pricing_fingerprint': _projected_pricing_fingerprint(row),
            'hourly_cost': cost,
            'price_source': source,
            'hourly_cost_exclusion_reason': reason,
        })
    return results


def get_service_pricing(
    service_name: str,
    expected_service_hash: str,
    replica_ids: Collection[int] | None = None,
) -> dict[str, Any]:
    """Return bounded persisted pricing for a service incarnation."""
    if (not isinstance(expected_service_hash, str) or
            not expected_service_hash):
        raise ValueError('Expected service hash must be a non-empty string.')
    requested_ids: list[int] | None = None
    if replica_ids is not None:
        raw_ids = list(replica_ids)
        if len(raw_ids) > MAX_PRICING_REPLICA_IDS:
            raise ValueError('At most 100 raw replica IDs may be requested.')
        for replica_id in raw_ids:
            if (isinstance(replica_id, bool) or
                    not isinstance(replica_id, int) or replica_id < 1 or
                    replica_id > MAX_PRICING_REPLICA_ID):
                raise ValueError('Replica IDs must be positive PostgreSQL '
                                 'INTEGER values.')
        requested_ids = list(dict.fromkeys(raw_ids))

    engine = _postgres_engine()
    with _repeatable_read_connection(engine) as connection:
        with connection.begin():
            identity = connection.execute(
                _service_identity_query(service_name)).fetchone()
            if identity is None:
                raise ServiceNotFoundError(service_name)
            service_hash = identity[0]
            if service_hash != expected_service_hash:
                raise ServiceHashMismatchError(service_name)
            observed_at = time.time()
            if requested_ids is None:
                aggregate = _aggregate_pricing(connection, service_name)
                replicas = []
            else:
                aggregate = None
                replicas = _replica_pricing(connection, service_name,
                                            requested_ids)
    return {
        'available': True,
        'service_name': service_name,
        'service_hash': expected_service_hash,
        'observed_at': observed_at,
        'price_basis': PRICE_BASIS,
        'aggregate': aggregate,
        'replicas': replicas,
    }


def unavailable_service_pricing(service_name: str, service_hash: str,
                                reason: str) -> dict[str, Any]:
    return {
        'available': False,
        'reason': reason,
        'service_name': service_name,
        'service_hash': service_hash,
        'observed_at': None,
        'price_basis': PRICE_BASIS,
        'aggregate': None,
        'replicas': [],
    }
