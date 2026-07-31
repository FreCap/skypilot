"""PostgreSQL repository for bounded physical-capacity evidence scans.

Only ``capacity_projection_scans`` is writable.  Source adapters receive a
caller-owned, read-only repeatable-read connection; their immutable result is
published later in a short controller-fenced transaction.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
import dataclasses
import datetime
import re
import threading
import time
from typing import Any, TypeVar
import uuid

import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc

from sky.physical_capacity import contracts
from sky.physical_capacity import hashing
from sky.physical_capacity import models
from sky.physical_capacity import schema
from sky.physical_capacity import source_queries
from sky.server.requests import postgres as request_postgres
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_ENGINE_NAMESPACE = 'physical-capacity-evidence'
_APPLICATION_NAME = 'skypilot-physical-capacity-evidence'
_POOL_SIZE = 1
_MAX_OVERFLOW = 0
_POOL_PRE_PING = False

_SCAN_DEADLINE_SECONDS = 30.0
_STALE_SCAN_SECONDS = 10 * 60
_ACTIVATION_ROW_LIMIT = 53_776
_ACTIVATION_BATCH_SIZE = 256
_ACTIVATION_CURSOR_BYTES = 4 * 1024
_ACTIVATION_TOTAL_BYTES = 64 * 1024 * 1024
_REPORT_ROW_LIMIT = 4_000
_PILOT_MAX_DAYS = 35
_SLOT_SECONDS = 900
_SERIALIZATION_DELAYS_SECONDS = (0.0, 0.05, 0.1)
_STALE_FINALIZED = object()
_TIMESTAMP_PATTERN = re.compile(
    r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
_SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')

_FAILURE_CODES = frozenset({
    'row_limit_exceeded',
    'byte_limit_exceeded',
    'scan_timeout',
    'source_decode_failed',
    'source_conflict',
    'selector_mismatch',
    'source_index_missing',
    'non_colocated_source_store',
    'controller_fenced',
    'serialization_exhausted',
    'database_unavailable',
    'database_statement_failed',
    'stale_scan',
})

_REQUIRED_REVISIONS = {
    'alembic_version_state_db': migration_utils.GLOBAL_USER_STATE_VERSION,
    'alembic_version_serve_state_db': migration_utils.SERVE_VERSION,
    'alembic_version_spot_jobs_db': migration_utils.SPOT_JOBS_VERSION,
    'alembic_version_api_requests_db': migration_utils.API_REQUESTS_VERSION,
    'alembic_version_capacity_state_db': migration_utils.CAPACITY_STATE_VERSION,
}


@dataclasses.dataclass(frozen=True)
class _CatalogRequirement:
    relation: str
    columns: tuple[tuple[str, str], ...]
    index_leading_columns: tuple[tuple[str, ...], ...]


_REPOSITORY_SCHEMA_REQUIREMENTS = (
    _CatalogRequirement(
        'capacity_projection_scans',
        (('scan_id', 'uuid'), ('workspace', 'text'), ('source_kind', 'text'),
         ('source_partition_hash', 'character(64)'),
         ('cursor_schema_version', 'integer'), ('cursor', 'jsonb'),
         ('state', 'text'), ('controller_instance_id', 'uuid'),
         ('controller_generation', 'bigint'), ('rows_seen', 'bigint'),
         ('finding_counts', 'jsonb'), ('error_code', 'text'),
         ('started_at', 'timestamp with time zone'),
         ('updated_at', 'timestamp with time zone'),
         ('completed_at', 'timestamp with time zone')),
        (('scan_id',), ('workspace', 'source_kind', 'source_partition_hash'),
         ('workspace', 'source_kind', 'completed_at'),
         ('state', 'updated_at'))),
    _CatalogRequirement('api_controller_leadership',
                        (('leadership_key', 'text'), ('generation', 'bigint'),
                         ('instance_id', 'uuid'),
                         ('lock_backend_pid', 'integer'),
                         ('generation_lock_key', 'bigint'),
                         ('released_at', 'timestamp with time zone')),
                        (('leadership_key',),)),
)

_T = TypeVar('_T')


class ScanRepositoryError(RuntimeError):
    """Base class for closed evidence-scan repository failures."""


class ControllerFencedError(ScanRepositoryError):
    """The caller no longer owns the declared controller generation."""


class ScanFailure(ScanRepositoryError):
    """A scan failed with one closed, persistence-safe code."""

    def __init__(self, code: str, *, rows_seen: int = 0):
        if code not in _FAILURE_CODES:
            raise ValueError(f'Unknown scan failure code: {code!r}.')
        super().__init__(code)
        self.code = code
        self.rows_seen = max(0, rows_seen)


@dataclasses.dataclass(frozen=True)
class ControllerIdentity:
    """Canonical controller generation used by every repository fence."""

    instance_id: str
    generation: int

    def __post_init__(self) -> None:
        try:
            canonical_id = str(uuid.UUID(self.instance_id))
        except (TypeError, ValueError, AttributeError) as e:
            raise ValueError('controller instance_id must be a UUID.') from e
        if canonical_id != self.instance_id:
            raise ValueError('controller instance_id must be canonical.')
        if (not isinstance(self.generation, int) or
                isinstance(self.generation, bool) or self.generation <= 0):
            raise ValueError('controller generation must be positive.')


@dataclasses.dataclass(frozen=True)
class ActivationSnapshot:
    """Immutable pilot anchor established before the daemon can start."""

    pilot_end_utc: str
    durable_partitions: frozenset[contracts.SourcePartition]
    activated_at: datetime.datetime


@dataclasses.dataclass(frozen=True)
class ScanHandle:
    """Identity and deadline shared by one logical partition operation."""

    scan_id: uuid.UUID
    partition: contracts.SourcePartition
    source_partition_hash: str
    projection_scope_hash: str
    pilot_end_utc: str
    scheduled_slot_utc: str
    controller: ControllerIdentity
    started_monotonic: float

    def running_cursor(self) -> dict[str, object]:
        return _cursor(self, inventory_digest=None)


@dataclasses.dataclass(frozen=True)
class PublishedScan:
    scan_id: uuid.UUID
    digest_changed: bool
    completed_at: datetime.datetime


def _parse_timestamp(value: str) -> datetime.datetime:
    if (not isinstance(value, str) or
            _TIMESTAMP_PATTERN.fullmatch(value) is None or
            len(value.encode('ascii', errors='ignore')) != 20):
        raise ValueError('Timestamp must use YYYY-MM-DDTHH:MM:SSZ.')
    try:
        parsed = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError as e:
        raise ValueError('Timestamp must be a valid UTC instant.') from e
    return parsed.replace(tzinfo=datetime.timezone.utc)


def format_timestamp(value: datetime.datetime) -> str:
    """Format an aware instant in the cursor's exact UTC representation."""
    if value.tzinfo is None:
        raise ValueError('Timestamp must be timezone-aware.')
    value = value.astimezone(datetime.timezone.utc).replace(microsecond=0)
    return value.strftime('%Y-%m-%dT%H:%M:%SZ')


def _cursor(handle: ScanHandle, *,
            inventory_digest: str | None) -> dict[str, object]:
    if (inventory_digest is not None and
            _SHA256_PATTERN.fullmatch(inventory_digest) is None):
        raise ValueError('inventory_digest must be a lowercase SHA-256.')
    return {
        'mapping_version': 1,
        'phase': 'full_snapshot',
        'projection_scope_hash': handle.projection_scope_hash,
        'pilot_end_utc': handle.pilot_end_utc,
        'scheduled_slot_utc': handle.scheduled_slot_utc,
        'inventory_digest': inventory_digest,
    }


def _sqlstate(error: BaseException) -> str | None:
    current: Any = error
    for _ in range(3):
        value = (getattr(current, 'sqlstate', None) or
                 getattr(current, 'pgcode', None))
        if value is not None:
            return str(value)
        current = getattr(current, 'orig', None)
        if current is None:
            break
    return None


def _set_long_timeouts(connection: sqlalchemy.engine.Connection,
                       remaining_seconds: float) -> None:
    milliseconds = max(1, min(30_000, int(remaining_seconds * 1000)))
    connection.exec_driver_sql(
        f"SET LOCAL statement_timeout = '{milliseconds}ms'")
    connection.exec_driver_sql("SET LOCAL lock_timeout = '250ms'")
    connection.exec_driver_sql(
        "SET LOCAL idle_in_transaction_session_timeout = '35000ms'")


def _set_short_timeouts(connection: sqlalchemy.engine.Connection) -> None:
    connection.exec_driver_sql("SET LOCAL statement_timeout = '1000ms'")
    connection.exec_driver_sql("SET LOCAL lock_timeout = '250ms'")
    connection.exec_driver_sql(
        "SET LOCAL idle_in_transaction_session_timeout = '2000ms'")


def _database_failure(error: BaseException,
                      *,
                      outer_deadline: float | None = None,
                      long_operation: bool = False) -> ScanFailure:
    if outer_deadline is not None and time.monotonic() >= outer_deadline:
        return ScanFailure('scan_timeout')
    state = _sqlstate(error)
    # PostgreSQL reports ``statement_timeout`` and an explicit libpq cancel
    # with query-canceled (57014).  Long activation/source transactions use
    # cancellation only as their deadline watchdog, so a cancellation there
    # is the closed scan-timeout outcome even if the integer millisecond
    # server timeout fires just before the process-local deadline comparison.
    if long_operation and state == '57014':
        return ScanFailure('scan_timeout')
    invalidated = bool(getattr(error, 'connection_invalidated', False))
    if (invalidated or isinstance(error, sqlalchemy_exc.TimeoutError) or
            isinstance(error, sqlalchemy_exc.InterfaceError) or
        (isinstance(error, sqlalchemy_exc.OperationalError) and
         state is None) or (state is not None and state.startswith('08'))):
        return ScanFailure('database_unavailable')
    return ScanFailure('database_statement_failed')


def _cancel_connection(connection: sqlalchemy.engine.Connection) -> None:
    """Cancel exactly one DBAPI connection without consulting global state."""
    try:
        connection.connection.driver_connection.cancel()
    except Exception:  # pylint: disable=broad-except
        pass


class ScanRepository:
    """One-controller repository backed by one explicit PostgreSQL pool."""

    def __init__(self, base_engine: sqlalchemy.engine.Engine,
                 controller: ControllerIdentity):
        if base_engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise RuntimeError(
                'Physical-capacity evidence requires PostgreSQL.')
        self._base_engine = base_engine
        self._controller = controller
        self._engine = db_utils.get_isolated_postgres_engine(
            base_engine,
            namespace=_ENGINE_NAMESPACE,
            pool_size=_POOL_SIZE,
            max_overflow=_MAX_OVERFLOW,
            pool_pre_ping=_POOL_PRE_PING,
            application_name=_APPLICATION_NAME)
        self._cancelled = threading.Event()
        self._active_lock = threading.Lock()
        self._active_connection: sqlalchemy.engine.Connection | None = None
        self._activation_snapshot: ActivationSnapshot | None = None
        self._closed = False

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        return self._engine

    def _prove_controller(self, connection: sqlalchemy.engine.Connection, *,
                          lock: bool) -> None:
        generation = connection.execute(
            request_postgres.current_controller_leadership_statement(
                self._controller.instance_id,
                self._controller.generation,
                lock=lock)).scalar_one_or_none()
        if generation is None:
            raise ControllerFencedError('Controller generation is not current.')

    def _run_short_transaction(
        self,
        operation: Callable[[sqlalchemy.engine.Connection], _T],
        *,
        outer_deadline: float | None = None,
    ) -> _T:
        """Run a checked-out short write/proof under its two-second watchdog.

        Queue checkout/physical connection establishment is governed by the
        isolated pool's exact one-second checkout and five-second connect
        budgets.  The client watchdog begins as soon as checkout returns and
        covers transaction setup, the operation, commit, and rollback.
        """
        self._raise_if_cancelled()
        timeout = 2.0
        if outer_deadline is not None:
            timeout = min(timeout, outer_deadline - time.monotonic())
        if timeout <= 0:
            raise ScanFailure('scan_timeout')
        started = time.monotonic()
        with self._engine.connect() as connection:
            self._raise_if_cancelled()
            remaining = timeout - (time.monotonic() - started)
            if remaining <= 0:
                if outer_deadline is not None:
                    raise ScanFailure('scan_timeout')
                raise ScanFailure('database_unavailable')
            self._set_active(connection)
            timer = threading.Timer(remaining,
                                    _cancel_connection,
                                    args=(connection,))
            timer.start()
            transaction = connection.begin()
            try:
                _set_short_timeouts(connection)
                result = operation(connection)
                transaction.commit()
                return result
            except BaseException:
                transaction.rollback()
                raise
            finally:
                timer.cancel()
                timer.join()
                self._set_active(None)

    def validate_schema(self, requirements: Iterable[Any] = ()) -> str:
        """Validate revision minima and fixed source/index catalog contracts."""
        with self._engine.connect() as connection:
            transaction = connection.begin()
            try:
                _set_long_timeouts(connection, _SCAN_DEADLINE_SECONDS)
                schema_name = connection.execute(
                    sqlalchemy.text('SELECT current_schema()')).scalar_one()
                if not isinstance(schema_name, str) or not schema_name:
                    raise ScanFailure('non_colocated_source_store')
                for table_name, expected in _REQUIRED_REVISIONS.items():
                    actual = connection.execute(
                        sqlalchemy.text(f'SELECT version_num FROM {table_name}')
                    ).scalar_one_or_none()
                    if actual is None:
                        raise ScanFailure('source_index_missing')
                    if table_name == 'alembic_version_capacity_state_db':
                        valid = actual == expected
                    else:
                        try:
                            valid = int(actual) >= int(expected)
                        except (TypeError, ValueError):
                            valid = False
                    if not valid:
                        raise ScanFailure('source_index_missing')
                self._validate_catalog(connection, schema_name, requirements)
                transaction.commit()
                return schema_name
            except BaseException:
                transaction.rollback()
                raise

    def _validate_catalog(self, connection: sqlalchemy.engine.Connection,
                          schema_name: str,
                          requirements: Iterable[Any]) -> None:
        requirements = (*_REPOSITORY_SCHEMA_REQUIREMENTS, *tuple(requirements))
        if not requirements:
            return
        relations = sorted(
            {str(requirement.relation) for requirement in requirements})
        statement = sqlalchemy.text("""
            SELECT c.relname, a.attname,
                   pg_catalog.format_type(a.atttypid, a.atttypmod),
                   a.attnotnull
            FROM pg_catalog.pg_attribute AS a
            JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema_name
              AND c.relname IN :relations
              AND c.relkind = 'r'
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY c.relname, a.attnum
        """).bindparams(sqlalchemy.bindparam('relations', expanding=True))
        rows = connection.execute(statement, {
            'schema_name': schema_name,
            'relations': relations,
        })
        columns = {
            (row.relname, row.attname): (row[2], bool(row.attnotnull))
            for row in rows
        }

        index_statement = sqlalchemy.text("""
            SELECT table_class.relname AS table_name,
                   index_class.relname AS index_name,
                   pg_catalog.pg_get_indexdef(index_class.oid) AS index_def
            FROM pg_catalog.pg_index AS idx
            JOIN pg_catalog.pg_class AS table_class
              ON table_class.oid = idx.indrelid
            JOIN pg_catalog.pg_class AS index_class
              ON index_class.oid = idx.indexrelid
            JOIN pg_catalog.pg_namespace AS n
              ON n.oid = table_class.relnamespace
            WHERE n.nspname = :schema_name
              AND table_class.relname IN :relations
            ORDER BY table_class.relname, index_class.relname
        """).bindparams(sqlalchemy.bindparam('relations', expanding=True))
        indexes: dict[str, list[str]] = {}
        for row in connection.execute(index_statement, {
                'schema_name': schema_name,
                'relations': relations,
        }):
            indexes.setdefault(row.table_name, []).append(row.index_def)

        for requirement in requirements:
            relation = str(requirement.relation)
            for column, expected_type in requirement.columns:
                actual = columns.get((relation, column))
                if actual is None or actual[0] != expected_type:
                    raise ScanFailure('source_index_missing')
            for required_leading in requirement.index_leading_columns:
                needle = '(' + ', '.join(required_leading)
                if not any(needle in definition
                           for definition in indexes.get(relation, ())):
                    raise ScanFailure('source_index_missing')

    def load_activation_snapshot(
        self,
        configured_partitions: Iterable[contracts.SourcePartition],
        pilot_end_utc: str,
    ) -> ActivationSnapshot:
        """Reconstruct the bounded global mapping-v1 activation anchor."""
        configured = frozenset(configured_partitions)
        if len(configured) > 16:
            raise ValueError('At most 16 source partitions are supported.')
        configured_end = _parse_timestamp(pilot_end_utc)
        self._raise_if_cancelled()
        durable: set[contracts.SourcePartition] = set()
        durable_end: str | None = None
        deadline = time.monotonic() + _SCAN_DEADLINE_SECONDS
        total_bytes = 0
        phase_two_seen = 0
        tuple_keys: set[tuple[str, str, str]] = set()
        partition_counts: dict[contracts.SourcePartition, int] = {}
        try:
            with self._engine.connect().execution_options(
                    isolation_level='REPEATABLE READ') as connection:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ScanFailure('scan_timeout')
                self._set_active(connection)
                timer = threading.Timer(remaining,
                                        _cancel_connection,
                                        args=(connection,))
                timer.start()
                transaction = connection.begin()
                try:
                    connection.exec_driver_sql('SET TRANSACTION READ ONLY')
                    _set_long_timeouts(connection, deadline - time.monotonic())
                    activated_at = connection.execute(
                        sqlalchemy.select(sqlalchemy.func.transaction_timestamp(
                        ))).scalar_one()
                    phase_one = connection.execution_options(
                        stream_results=True,
                        yield_per=_ACTIVATION_BATCH_SIZE).execute(
                            sqlalchemy.text("""
                            SELECT scan_id,
                                   octet_length(workspace) AS workspace_bytes,
                                   octet_length(source_kind) AS source_kind_bytes,
                                   source_kind IN (
                                     'serve_service', 'serve_pool',
                                     'managed_job_task') AS source_kind_valid,
                                   cursor_schema_version,
                                   pg_column_size(cursor) AS cursor_bytes,
                                   (controller_instance_id IS NOT NULL AND
                                    controller_generation > 0) AS owner_valid
                            FROM capacity_projection_scans
                            ORDER BY scan_id
                            LIMIT :limit
                        """), {'limit': _ACTIVATION_ROW_LIMIT + 1})
                    scan_ids: list[uuid.UUID] = []
                    for batch in phase_one.partitions(_ACTIVATION_BATCH_SIZE):
                        self._raise_if_cancelled()
                        if time.monotonic() >= deadline:
                            raise ScanFailure('scan_timeout')
                        for row in batch:
                            scan_ids.append(row.scan_id)
                            if len(scan_ids) > _ACTIVATION_ROW_LIMIT:
                                raise ScanFailure('row_limit_exceeded')
                            if (row.workspace_bytes is None or
                                    row.workspace_bytes > 256 or
                                    row.source_kind_bytes is None or
                                    not row.source_kind_valid or
                                    row.cursor_schema_version != 1 or
                                    row.cursor_bytes is None or row.cursor_bytes
                                    > _ACTIVATION_CURSOR_BYTES or
                                    not row.owner_valid):
                                raise ScanFailure('source_decode_failed')
                            total_bytes += (16 + row.workspace_bytes +
                                            row.source_kind_bytes +
                                            row.cursor_bytes)
                            if total_bytes > _ACTIVATION_TOTAL_BYTES:
                                raise ScanFailure('byte_limit_exceeded')

                    phase_two_statement = sqlalchemy.text("""
                        SELECT scan_id, workspace, source_kind,
                               source_partition_hash,
                               cursor ->> 'projection_scope_hash' AS scope_hash,
                               cursor ->> 'pilot_end_utc' AS pilot_end_utc,
                               cursor ->> 'scheduled_slot_utc'
                                 AS scheduled_slot_utc,
                               (
                                 state IN ('running', 'completed', 'failed')
                                 AND cursor ?& ARRAY[
                                   'mapping_version', 'phase',
                                   'projection_scope_hash', 'pilot_end_utc',
                                   'scheduled_slot_utc', 'inventory_digest']
                                 AND cursor - ARRAY[
                                   'mapping_version', 'phase',
                                   'projection_scope_hash', 'pilot_end_utc',
                                   'scheduled_slot_utc',
                                   'inventory_digest']::text[] = '{}'::jsonb
                                 AND jsonb_typeof(cursor) = 'object'
                                 AND cursor -> 'mapping_version' = '1'::jsonb
                                 AND cursor -> 'phase' =
                                     '"full_snapshot"'::jsonb
                                 AND jsonb_typeof(cursor ->
                                     'projection_scope_hash') = 'string'
                                 AND octet_length(cursor ->>
                                     'projection_scope_hash') = 64
                                 AND (cursor ->> 'projection_scope_hash') ~
                                     '^[0-9a-f]{64}$'
                                 AND jsonb_typeof(cursor ->
                                     'pilot_end_utc') = 'string'
                                 AND octet_length(cursor ->>
                                     'pilot_end_utc') = 20
                                 AND (cursor ->> 'pilot_end_utc') ~
                                     '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                                 AND jsonb_typeof(cursor ->
                                     'scheduled_slot_utc') = 'string'
                                 AND octet_length(cursor ->>
                                     'scheduled_slot_utc') = 20
                                 AND (cursor ->> 'scheduled_slot_utc') ~
                                     '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                                 AND (
                                   (state IN ('running', 'failed') AND
                                    cursor -> 'inventory_digest' = 'null'::jsonb)
                                   OR
                                   (state = 'completed' AND
                                    jsonb_typeof(cursor ->
                                      'inventory_digest') = 'string' AND
                                    octet_length(cursor ->>
                                      'inventory_digest') = 64 AND
                                    (cursor ->> 'inventory_digest') ~
                                      '^[0-9a-f]{64}$'))
                               ) AS cursor_valid
                        FROM capacity_projection_scans
                        WHERE scan_id IN :scan_ids
                        ORDER BY scan_id
                    """).bindparams(
                        sqlalchemy.bindparam('scan_ids', expanding=True))
                    for offset in range(0, len(scan_ids),
                                        _ACTIVATION_BATCH_SIZE):
                        self._raise_if_cancelled()
                        ids = scan_ids[offset:offset + _ACTIVATION_BATCH_SIZE]
                        rows = connection.execution_options(
                            stream_results=True,
                            yield_per=_ACTIVATION_BATCH_SIZE).execute(
                                phase_two_statement, {'scan_ids': ids})
                        for row in rows:
                            phase_two_seen += 1
                            if not row.cursor_valid:
                                raise ScanFailure('source_decode_failed')
                            row_end = _parse_timestamp(row.pilot_end_utc)
                            slot = _parse_timestamp(row.scheduled_slot_utc)
                            partition = contracts.SourcePartition(
                                workspace=row.workspace,
                                source_kind=models.ProjectionSourceKind(
                                    row.source_kind))
                            expected_partition_hash = (
                                hashing.source_partition_hash(partition))
                            if expected_partition_hash != str(
                                    row.source_partition_hash):
                                raise ScanFailure('source_decode_failed')
                            jitter = hashing.slot_jitter_seconds(
                                expected_partition_hash)
                            if ((int(slot.timestamp()) - jitter) % _SLOT_SECONDS
                                    != 0):
                                raise ScanFailure('source_decode_failed')
                            if not (row_end - datetime.timedelta(
                                    days=_PILOT_MAX_DAYS, seconds=_SLOT_SECONDS)
                                    <= slot < row_end):
                                raise ScanFailure('source_decode_failed')
                            tuple_key = (partition.workspace,
                                         partition.source_kind.value,
                                         row.scheduled_slot_utc)
                            if tuple_key in tuple_keys:
                                raise ScanFailure('source_conflict')
                            tuple_keys.add(tuple_key)
                            partition_counts[partition] = (
                                partition_counts.get(partition, 0) + 1)
                            if partition_counts[partition] > 3_361:
                                raise ScanFailure('row_limit_exceeded')
                            phase_two_bytes = sum(
                                len(value.encode('utf-8'))
                                for value in (row.workspace, row.source_kind,
                                              str(row.source_partition_hash),
                                              row.scope_hash, row.pilot_end_utc,
                                              row.scheduled_slot_utc))
                            total_bytes += 16 + phase_two_bytes
                            if total_bytes > _ACTIVATION_TOTAL_BYTES:
                                raise ScanFailure('byte_limit_exceeded')
                            if durable_end is None:
                                durable_end = row.pilot_end_utc
                            elif durable_end != row.pilot_end_utc:
                                raise ScanFailure('source_conflict')
                            durable.add(partition)
                    if phase_two_seen != len(scan_ids):
                        raise ScanFailure('source_decode_failed')
                    transaction.commit()
                except BaseException:
                    transaction.rollback()
                    raise
                finally:
                    timer.cancel()
                    timer.join()
                    self._set_active(None)
        except sqlalchemy_exc.SQLAlchemyError as e:
            if time.monotonic() >= deadline:
                raise ScanFailure('scan_timeout') from e
            raise _database_failure(e, long_operation=True) from e

        if durable_end is None:
            if not activated_at < configured_end <= (
                    activated_at + datetime.timedelta(days=_PILOT_MAX_DAYS)):
                raise ValueError('A new pilot end must be after activation and '
                                 'at most 35 days later.')
        elif durable_end != pilot_end_utc:
            raise ValueError('The configured pilot end differs from the '
                             'durable activation anchor.')
        durable.update(configured)
        if len(durable) > 16:
            raise ValueError('The durable pilot union exceeds 16 partitions.')

        def prove(connection: sqlalchemy.engine.Connection) -> None:
            self._prove_controller(connection, lock=True)

        try:
            self._run_short_transaction(prove, outer_deadline=deadline)
        except sqlalchemy_exc.SQLAlchemyError as e:
            raise _database_failure(e, outer_deadline=deadline) from e
        snapshot = ActivationSnapshot(pilot_end_utc, frozenset(durable),
                                      activated_at)
        self._activation_snapshot = snapshot
        return snapshot

    def begin_scan(
            self,
            partition: contracts.SourcePartition,
            source_partition_hash: str,
            projection_scope_hash: str,
            pilot_end_utc: str,
            *,
            _started_monotonic: float | None = None) -> ScanHandle | None:
        """Fence, de-duplicate one UTC slot, and insert its running row."""
        activation = self._activation_snapshot
        if (activation is None or activation.pilot_end_utc != pilot_end_utc or
                partition not in activation.durable_partitions):
            raise RuntimeError('Partition is outside the fenced activation '
                               'snapshot.')
        if hashing.source_partition_hash(partition) != source_partition_hash:
            raise ValueError('source_partition_hash does not match partition.')
        if _SHA256_PATTERN.fullmatch(projection_scope_hash) is None:
            raise ValueError('projection_scope_hash must be a SHA-256.')
        pilot_end = _parse_timestamp(pilot_end_utc)
        started_monotonic = (time.monotonic() if _started_monotonic is None else
                             _started_monotonic)
        deadline = started_monotonic + _SCAN_DEADLINE_SECONDS

        def insert_if_eligible(
            connection: sqlalchemy.engine.Connection
        ) -> ScanHandle | None | object:
            # Eligibility and DB-clock slot selection intentionally precede
            # the leadership-row lock.  Only the final stale CAS or insert
            # holds that shared lock.
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.transaction_timestamp())).scalar_one()
            if now >= pilot_end:
                return None
            jitter = hashing.slot_jitter_seconds(source_partition_hash)
            slot_number = (int(now.timestamp()) - jitter) // _SLOT_SECONDS
            slot = datetime.datetime.fromtimestamp(slot_number * _SLOT_SECONDS +
                                                   jitter,
                                                   tz=datetime.timezone.utc)
            scheduled_slot_utc = format_timestamp(slot)
            handle = ScanHandle(uuid.uuid4(), partition, source_partition_hash,
                                projection_scope_hash, pilot_end_utc,
                                scheduled_slot_utc, self._controller,
                                started_monotonic)
            running = connection.execute(
                sqlalchemy.select(schema.PROJECTION_SCANS).where(
                    schema.PROJECTION_SCANS.c.workspace == partition.workspace,
                    schema.PROJECTION_SCANS.c.source_kind ==
                    partition.source_kind.value,
                    schema.PROJECTION_SCANS.c.source_partition_hash ==
                    source_partition_hash,
                    schema.PROJECTION_SCANS.c.state ==
                    models.ProjectionScanState.RUNNING.value,
                )).mappings().one_or_none()
            if running is not None:
                age = (now - running['started_at']).total_seconds()
                if age <= _STALE_SCAN_SECONDS:
                    return None
                self._prove_controller(connection, lock=True)
                connection.execute(
                    sqlalchemy.update(schema.PROJECTION_SCANS).where(
                        schema.PROJECTION_SCANS.c.scan_id == running['scan_id'],
                        schema.PROJECTION_SCANS.c.state ==
                        models.ProjectionScanState.RUNNING.value,
                    ).values(state=models.ProjectionScanState.FAILED.value,
                             completed_at=now,
                             updated_at=now,
                             finding_counts={},
                             error_code='stale_scan'))
                # Commit the short stale finalization before re-evaluating the
                # current slot, so its leadership lock never spans report
                # reads or a later current-slot insert.
                return _STALE_FINALIZED

            terminal_slots = connection.execute(
                sqlalchemy.select(schema.PROJECTION_SCANS.c.
                                  cursor['scheduled_slot_utc'].as_string()).
                where(
                    schema.PROJECTION_SCANS.c.workspace == partition.workspace,
                    schema.PROJECTION_SCANS.c.source_kind ==
                    partition.source_kind.value,
                    schema.PROJECTION_SCANS.c.state.in_((
                        models.ProjectionScanState.COMPLETED.value,
                        models.ProjectionScanState.FAILED.value,
                    )),
                    schema.PROJECTION_SCANS.c.completed_at
                    >= now - datetime.timedelta(days=_PILOT_MAX_DAYS),
                ).order_by(schema.PROJECTION_SCANS.c.completed_at.desc()).limit(
                    _REPORT_ROW_LIMIT)).scalars()
            if scheduled_slot_utc in set(terminal_slots):
                return None

            self._prove_controller(connection, lock=True)
            connection.execute(
                sqlalchemy.insert(schema.PROJECTION_SCANS).values(
                    scan_id=handle.scan_id,
                    workspace=partition.workspace,
                    source_kind=partition.source_kind.value,
                    source_partition_hash=source_partition_hash,
                    cursor_schema_version=1,
                    cursor=handle.running_cursor(),
                    state=models.ProjectionScanState.RUNNING.value,
                    controller_instance_id=uuid.UUID(
                        self._controller.instance_id),
                    controller_generation=self._controller.generation,
                    rows_seen=0,
                    finding_counts={},
                    error_code=None,
                    started_at=now,
                    updated_at=now,
                    completed_at=None))
            return handle

        try:
            result = self._run_short_transaction(insert_if_eligible,
                                                 outer_deadline=deadline)
            if result is _STALE_FINALIZED:
                return self.begin_scan(partition,
                                       source_partition_hash,
                                       projection_scope_hash,
                                       pilot_end_utc,
                                       _started_monotonic=started_monotonic)
            assert result is None or isinstance(result, ScanHandle)
            return result
        except sqlalchemy_exc.IntegrityError as e:
            constraint_name = getattr(getattr(e, 'orig', None), 'diag', None)
            constraint_name = getattr(constraint_name, 'constraint_name', None)
            if constraint_name == 'uq_capacity_projection_scans_running_partition':
                return None
            raise _database_failure(e, outer_deadline=deadline) from e
        except sqlalchemy_exc.SQLAlchemyError as e:
            raise _database_failure(e, outer_deadline=deadline) from e

    def finalize_stale(self, partition: contracts.SourcePartition) -> int:
        """Fence and fail old running rows without starting an expired scan."""
        partition_hash = hashing.source_partition_hash(partition)

        def finalize(connection: sqlalchemy.engine.Connection) -> int:
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.transaction_timestamp())).scalar_one()
            stale_exists = connection.execute(
                sqlalchemy.select(schema.PROJECTION_SCANS.c.scan_id).where(
                    schema.PROJECTION_SCANS.c.workspace == partition.workspace,
                    schema.PROJECTION_SCANS.c.source_kind ==
                    partition.source_kind.value,
                    schema.PROJECTION_SCANS.c.source_partition_hash ==
                    partition_hash,
                    schema.PROJECTION_SCANS.c.state ==
                    models.ProjectionScanState.RUNNING.value,
                    schema.PROJECTION_SCANS.c.started_at
                    < now - datetime.timedelta(seconds=_STALE_SCAN_SECONDS),
                ).limit(1)).scalar_one_or_none()
            if stale_exists is None:
                return 0
            self._prove_controller(connection, lock=True)
            result = connection.execute(
                sqlalchemy.update(schema.PROJECTION_SCANS).where(
                    schema.PROJECTION_SCANS.c.workspace == partition.workspace,
                    schema.PROJECTION_SCANS.c.source_kind ==
                    partition.source_kind.value,
                    schema.PROJECTION_SCANS.c.source_partition_hash ==
                    partition_hash,
                    schema.PROJECTION_SCANS.c.state ==
                    models.ProjectionScanState.RUNNING.value,
                    schema.PROJECTION_SCANS.c.started_at
                    < now - datetime.timedelta(seconds=_STALE_SCAN_SECONDS),
                ).values(state=models.ProjectionScanState.FAILED.value,
                         completed_at=now,
                         updated_at=now,
                         finding_counts={},
                         error_code='stale_scan'))
            return result.rowcount

        try:
            return self._run_short_transaction(finalize)
        except sqlalchemy_exc.SQLAlchemyError as e:
            raise _database_failure(e) from e

    def current_database_time(self) -> datetime.datetime:
        """Return a bounded database clock sample for scheduling/expiry."""

        def read_time(
                connection: sqlalchemy.engine.Connection) -> datetime.datetime:
            return connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.transaction_timestamp())).scalar_one()

        try:
            return self._run_short_transaction(read_time)
        except sqlalchemy_exc.SQLAlchemyError as e:
            raise _database_failure(e) from e

    def _set_active(self,
                    connection: sqlalchemy.engine.Connection | None) -> None:
        with self._active_lock:
            if connection is not None and self._cancelled.is_set():
                raise ScanFailure('scan_timeout')
            self._active_connection = connection

    def _raise_if_cancelled(self) -> None:
        if self._cancelled.is_set():
            raise ScanFailure('scan_timeout')

    def cancel_active(self) -> None:
        """Request libpq cancellation for the current isolated transaction."""
        # Publish cancellation before inspecting the active pointer.  A
        # concurrent checkout either registered first and is canceled below,
        # or observes the event while registering and closes without starting
        # a transaction.  Retry/backoff paths also test this event before
        # opening their next snapshot.
        self._cancelled.set()
        with self._active_lock:
            connection = self._active_connection
        if connection is None:
            return
        try:
            driver = connection.connection.driver_connection
            driver.cancel()
        except Exception:  # pylint: disable=broad-except
            # The worker may have completed between the pointer read and cancel.
            pass

    def _read_snapshot(
        self,
        handle: ScanHandle,
        reader: Callable[[sqlalchemy.engine.Connection], _T],
    ) -> _T:
        """Run a bounded callback in a fresh read-only RR snapshot."""
        deadline = handle.started_monotonic + _SCAN_DEADLINE_SECONDS
        last_error: BaseException | None = None
        for delay in _SERIALIZATION_DELAYS_SECONDS:
            self._raise_if_cancelled()
            if delay:
                remaining = deadline - time.monotonic()
                if remaining <= delay:
                    raise ScanFailure('scan_timeout')
                time.sleep(delay)
                self._raise_if_cancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ScanFailure('scan_timeout')
            timer: threading.Timer | None = None
            try:
                with self._engine.connect().execution_options(
                        isolation_level='REPEATABLE READ') as connection:
                    self._raise_if_cancelled()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ScanFailure('scan_timeout')
                    self._set_active(connection)
                    timer = threading.Timer(remaining,
                                            _cancel_connection,
                                            args=(connection,))
                    timer.start()
                    transaction = connection.begin()
                    try:
                        connection.exec_driver_sql('SET TRANSACTION READ ONLY')
                        _set_long_timeouts(connection, remaining)
                        self._prove_controller(connection, lock=False)
                        result = reader(connection)
                        transaction.commit()
                        return result
                    except BaseException:
                        transaction.rollback()
                        raise
            except ControllerFencedError:
                raise
            except ScanFailure:
                raise
            except sqlalchemy_exc.SQLAlchemyError as e:
                last_error = e
                if _sqlstate(e) == '40001':
                    continue
                if time.monotonic() >= deadline:
                    raise ScanFailure('scan_timeout') from e
                raise _database_failure(e,
                                        outer_deadline=deadline,
                                        long_operation=True) from e
            except BaseException as e:
                last_error = e
                raise
            finally:
                if timer is not None:
                    timer.cancel()
                    timer.join()
                self._set_active(None)
        raise ScanFailure('serialization_exhausted') from last_error

    def read_evidence(
        self,
        handle: ScanHandle,
        adapter: Callable[[source_queries.SourceReader], _T],
    ) -> _T:
        """Run an adapter with only the fixed, bounded SourceReader API."""

        def read(connection: sqlalchemy.engine.Connection) -> _T:
            source_reader = source_queries.PartitionSourceCache(
                connection,
                deadline_monotonic=(handle.started_monotonic +
                                    _SCAN_DEADLINE_SECONDS))
            return adapter(source_reader)

        return self._read_snapshot(handle, read)

    def publish_completed(
        self,
        handle: ScanHandle,
        *,
        rows_seen: int,
        finding_counts: Mapping[str, int],
        inventory_digest: str,
    ) -> PublishedScan:
        """Publish an immutable in-memory result with a short fenced CAS."""
        if rows_seen < 0:
            raise ValueError('rows_seen must be non-negative.')
        if _SHA256_PATTERN.fullmatch(inventory_digest) is None:
            raise ValueError('inventory_digest must be a lowercase SHA-256.')
        deadline = handle.started_monotonic + _SCAN_DEADLINE_SECONDS

        def read_previous(
                connection: sqlalchemy.engine.Connection) -> str | None:
            sampled_at = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.transaction_timestamp())).scalar_one()
            completed_at_floor = sampled_at - datetime.timedelta(
                days=_PILOT_MAX_DAYS)
            rows = connection.execute(
                sqlalchemy.select(
                    schema.PROJECTION_SCANS.c.cursor['projection_scope_hash'].
                    as_string(), schema.PROJECTION_SCANS.c.
                    cursor['inventory_digest'].as_string()).where(
                        schema.PROJECTION_SCANS.c.workspace ==
                        handle.partition.workspace,
                        schema.PROJECTION_SCANS.c.source_kind ==
                        handle.partition.source_kind.value,
                        schema.PROJECTION_SCANS.c.state ==
                        models.ProjectionScanState.COMPLETED.value,
                        schema.PROJECTION_SCANS.c.completed_at
                        >= completed_at_floor,
                        schema.PROJECTION_SCANS.c.completed_at <= sampled_at,
                        schema.PROJECTION_SCANS.c.scan_id != handle.scan_id,
                    ).order_by(
                        schema.PROJECTION_SCANS.c.completed_at.desc()).limit(
                            _REPORT_ROW_LIMIT))
            for scope_hash, digest in rows:
                if scope_hash == handle.projection_scope_hash:
                    return digest
            return None

        try:
            previous_digest = self._run_short_transaction(
                read_previous, outer_deadline=deadline)
        except sqlalchemy_exc.SQLAlchemyError as e:
            raise _database_failure(e, outer_deadline=deadline) from e

        last_error: BaseException | None = None
        for delay in _SERIALIZATION_DELAYS_SECONDS:
            self._raise_if_cancelled()
            if delay:
                remaining = deadline - time.monotonic()
                if remaining <= delay:
                    raise ScanFailure('scan_timeout')
                time.sleep(delay)
                self._raise_if_cancelled()

            def publish(
                    connection: sqlalchemy.engine.Connection
            ) -> datetime.datetime:
                self._prove_controller(connection, lock=True)
                completed_at = connection.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.transaction_timestamp())).scalar_one()
                result = connection.execute(
                    sqlalchemy.update(schema.PROJECTION_SCANS).where(
                        schema.PROJECTION_SCANS.c.scan_id == handle.scan_id,
                        schema.PROJECTION_SCANS.c.state ==
                        models.ProjectionScanState.RUNNING.value,
                        schema.PROJECTION_SCANS.c.controller_instance_id ==
                        uuid.UUID(handle.controller.instance_id),
                        schema.PROJECTION_SCANS.c.controller_generation ==
                        handle.controller.generation,
                    ).values(cursor=_cursor(handle,
                                            inventory_digest=inventory_digest),
                             state=models.ProjectionScanState.COMPLETED.value,
                             rows_seen=rows_seen,
                             finding_counts=dict(finding_counts),
                             error_code=None,
                             updated_at=completed_at,
                             completed_at=completed_at))
                if result.rowcount != 1:
                    raise ControllerFencedError(
                        'Running scan is no longer publishable.')
                return completed_at

            try:
                completed_at = self._run_short_transaction(
                    publish, outer_deadline=deadline)
                return PublishedScan(
                    handle.scan_id, previous_digest is not None and
                    previous_digest != inventory_digest, completed_at)
            except ControllerFencedError:
                raise
            except ScanFailure:
                raise
            except BaseException as e:
                last_error = e
                if _sqlstate(e) == '40001':
                    continue
                if isinstance(e, sqlalchemy_exc.SQLAlchemyError):
                    raise _database_failure(e, outer_deadline=deadline) from e
                raise
        raise ScanFailure('serialization_exhausted') from last_error

    def publish_failed(self, handle: ScanHandle, failure: ScanFailure) -> bool:
        """Best-effort short failure CAS for the same controller generation.

        Controller fencing is not best effort and propagates to stop the
        projector before it can dequeue another partition.
        """
        deadline = handle.started_monotonic + _SCAN_DEADLINE_SECONDS

        def publish(connection: sqlalchemy.engine.Connection) -> bool:
            self._prove_controller(connection, lock=True)
            completed_at = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.transaction_timestamp())).scalar_one()
            result = connection.execute(
                sqlalchemy.update(schema.PROJECTION_SCANS).where(
                    schema.PROJECTION_SCANS.c.scan_id == handle.scan_id,
                    schema.PROJECTION_SCANS.c.state ==
                    models.ProjectionScanState.RUNNING.value,
                    schema.PROJECTION_SCANS.c.controller_instance_id ==
                    uuid.UUID(handle.controller.instance_id),
                    schema.PROJECTION_SCANS.c.controller_generation ==
                    handle.controller.generation,
                ).values(state=models.ProjectionScanState.FAILED.value,
                         rows_seen=failure.rows_seen,
                         finding_counts={},
                         error_code=failure.code,
                         updated_at=completed_at,
                         completed_at=completed_at))
            return result.rowcount == 1

        try:
            return self._run_short_transaction(publish, outer_deadline=deadline)
        except (ScanFailure, sqlalchemy_exc.SQLAlchemyError):
            return False

    def close(self) -> None:
        """Cancel, prove the isolated pool idle, dispose it, and forbid reuse."""
        if self._closed:
            return
        self.cancel_active()
        if db_utils.isolated_postgres_engine_checked_out(self._engine):
            raise RuntimeError('Evidence repository still has a live checkout.')
        db_utils.dispose_isolated_postgres_engine(
            self._base_engine,
            namespace=_ENGINE_NAMESPACE,
            pool_size=_POOL_SIZE,
            max_overflow=_MAX_OVERFLOW,
            pool_pre_ping=_POOL_PRE_PING,
            application_name=_APPLICATION_NAME)
        self._closed = True
