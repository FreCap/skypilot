"""Utils for sky databases."""
import asyncio
from collections.abc import Callable
from collections.abc import Iterable
import contextlib
import enum
import functools
import logging
import os
import pathlib
import sqlite3
import threading
import time
import typing
from typing import Any, Literal

import aiosqlite
import aiosqlite.context
import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy.ext import asyncio as sqlalchemy_async

from sky.adaptors import common as adaptors_common

constants = adaptors_common.LazyImport('sky.skylet.constants')
runtime_utils = adaptors_common.LazyImport('sky.skylet.runtime_utils')

logger = logging.getLogger(__name__)
metrics_utils = adaptors_common.LazyImport('sky.metrics.utils')
if typing.TYPE_CHECKING:
    from sqlalchemy.orm import Session

# This parameter (passed to sqlite3.connect) controls how long we will wait to
# obtains a database lock (not necessarily during connection, but whenever it is
# needed). It is not a connection timeout.
# Even in WAL mode, only a single writer is allowed at a time. Other writers
# will block until the write lock can be obtained. This behavior is described in
# the SQLite documentation for WAL: https://www.sqlite.org/wal.html
# Python's default timeout is 5s. In normal usage, lock contention is very low,
# and this is more than sufficient. However, in some highly concurrent cases,
# such as a jobs controller suddenly recovering thousands of jobs at once, we
# can see a small number of processes that take much longer to obtain the lock.
# In contrived highly contentious cases, around 0.1% of transactions will take
# >30s to take the lock. We have not seen cases that take >60s. For cases up to
# 1000x parallelism, this is thus thought to be a conservative setting.
# For more info, see the PR description for #4552.
_DB_TIMEOUT_S = 60

# SQLite's busy handler (and therefore _DB_TIMEOUT_S) is NOT invoked for every
# contended write. In WAL mode a deferred transaction that took its read
# snapshot before another connection committed fails immediately with
# SQLITE_BUSY_SNAPSHOT rather than waiting, so the write must be retried from
# the beginning to observe the newer snapshot. These retries are therefore
# short: they exist for the fail-immediately case, while genuine lock waits are
# still absorbed by the connection timeout.
_SQLITE_BUSY_MAX_ATTEMPTS = 5
_SQLITE_BUSY_INITIAL_BACKOFF_S = 0.02
_SQLITE_BUSY_BACKOFF_MULTIPLIER = 3
# Keep control-plane operations bounded when PostgreSQL or its network path is
# unavailable. libpq otherwise has no connection deadline, so one transient
# routing failure can indefinitely stall a Serve controller's reconciliation.
_POSTGRES_CONNECT_TIMEOUT_SECONDS = 15
_POSTGRES_LOCK_APPLICATION_NAME = 'skypilot-advisory-lock'
# Bound QueuePool checkout separately from connection establishment. A leaked
# or unexpectedly long transaction must not leave a worker waiting forever for
# another connection from its process-local budget.
_POSTGRES_POOL_TIMEOUT_SECONDS = 15
_API_SERVER_ROLE_ENV_VAR = 'SKYPILOT_API_SERVER_ROLE'
_POSTGRES_METRICS_ENABLED_ENV_VAR = 'SKY_API_SERVER_METRICS_ENABLED'
_POSTGRES_CONNECTION_METRIC_PROCESS_ROLES = frozenset({
    'all',
    'api',
    'executor',
    'controller',
    'managed-job-controller',
    'serve-controller',
    'unknown',
})
_POSTGRES_CONNECTION_METRIC_BASE_PROCESS_ROLES = frozenset({
    'all',
    'api',
    'executor',
    'controller',
})
_POSTGRES_CONNECTION_METRIC_ENGINE_NAMESPACES = frozenset({
    'shared',
    'api-requests-control',
    'advisory-lock',
    'reserved-fill-reclaim-proof',
    'other',
})
_POSTGRES_CONNECTION_METRIC_MODES = frozenset({'sync', 'async'})

_postgres_connection_metrics_process_role_override: str | None = None
_postgres_connection_metrics_warning_emitted = False
_postgres_connection_metrics_lock = threading.Lock()


def is_sqlite_busy_error(e: BaseException) -> bool:
    """Whether an exception is SQLite refusing a write due to contention."""
    if not isinstance(e, sqlite3.OperationalError):
        return False
    message = str(e).lower()
    return 'database is locked' in message or 'database is busy' in message


def retry_on_sqlite_busy(func):
    """Retry a SQLite write that failed immediately due to contention.

    Only for operations that are safe to repeat: a busy error means the
    transaction was not applied, so the callable must not have committed
    partial state before raising.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        backoff = _SQLITE_BUSY_INITIAL_BACKOFF_S
        for attempt in range(_SQLITE_BUSY_MAX_ATTEMPTS):
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if (not is_sqlite_busy_error(e) or
                        attempt == _SQLITE_BUSY_MAX_ATTEMPTS - 1):
                    raise
                time.sleep(backoff)
                backoff *= _SQLITE_BUSY_BACKOFF_MULTIPLIER
        raise AssertionError('unreachable')

    return wrapper


def retry_on_sqlite_busy_async(func):
    """Async counterpart of retry_on_sqlite_busy."""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        backoff = _SQLITE_BUSY_INITIAL_BACKOFF_S
        for attempt in range(_SQLITE_BUSY_MAX_ATTEMPTS):
            try:
                return await func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if (not is_sqlite_busy_error(e) or
                        attempt == _SQLITE_BUSY_MAX_ATTEMPTS - 1):
                    raise
                await asyncio.sleep(backoff)
                backoff *= _SQLITE_BUSY_BACKOFF_MULTIPLIER
        raise AssertionError('unreachable')

    return wrapper


class UniqueConstraintViolationError(Exception):
    """Exception raised for unique constraint violation.
    Attributes:
        value -- the input value that caused the error
        message -- explanation of the error
    """

    def __init__(self, value, message='Unique constraint violation'):
        self.value = value
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return (f'UniqueConstraintViolationError: {self.message} '
                f'(Value: {self.value})')


class SQLAlchemyDialect(enum.Enum):
    SQLITE = 'sqlite'
    POSTGRESQL = 'postgresql'


@contextlib.contextmanager
def safe_cursor(db_path: str):
    """A newly created, auto-committing, auto-closing cursor."""
    conn = sqlite3.connect(db_path, timeout=_DB_TIMEOUT_S)
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        conn.commit()
        conn.close()


@contextlib.contextmanager
def safe_cursor_on_connection(conn: 'sqlite3.Connection'):
    """A auto-committing, auto-closing cursor on an existing connection."""
    # Ensure commit() is called when the context is exited.
    with conn:
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()


def add_column_to_table(
    cursor: 'sqlite3.Cursor',
    conn: 'sqlite3.Connection',
    table_name: str,
    column_name: str,
    column_type: str,
    copy_from: str | None = None,
    value_to_replace_existing_entries: Any | None = None,
):
    """Add a column to a table."""
    for row in cursor.execute(f'PRAGMA table_info({table_name})'):
        if row[1] == column_name:
            break
    else:
        try:
            add_column_cmd = (f'ALTER TABLE {table_name} '
                              f'ADD COLUMN {column_name} {column_type}')
            cursor.execute(add_column_cmd)
            if copy_from is not None:
                cursor.execute(f'UPDATE {table_name} '
                               f'SET {column_name} = {copy_from}')
            if value_to_replace_existing_entries is not None:
                cursor.execute(
                    f'UPDATE {table_name} '
                    f'SET {column_name} = (?) '
                    f'WHERE {column_name} IS NULL',
                    (value_to_replace_existing_entries,))
        except sqlite3.OperationalError as e:
            if 'duplicate column name' in str(e):
                # We may be trying to add the same column twice, when
                # running multiple threads. This is fine.
                pass
            else:
                raise
    conn.commit()


def add_all_tables_to_db_sqlalchemy(
        metadata: sqlalchemy.MetaData,
        engine: sqlalchemy.Engine | sqlalchemy.engine.Connection,
        *,
        reconcile_indexes_for: Iterable[str] = (),
):
    """Add tables to the database and repair selected tables' indexes.

    Index reconciliation is opt-in because migration modules import the
    current SQLAlchemy metadata.  An older migration may run against a table
    that intentionally lacks columns introduced by later migrations, so
    blindly creating every current index would make historical upgrades fail.
    """
    reconcile_indexes = frozenset(reconcile_indexes_for)
    # Historical bootstrap revisions import the current metadata graph and
    # create its tables one at a time so index reconciliation can remain
    # selective.  Follow the graph's foreign-key topology: declaration order
    # is not a dependency contract, and a newly added parent may be declared
    # after an older child table.
    for table in metadata.sorted_tables:
        try:
            table.create(bind=engine, checkfirst=True)
        except (sqlalchemy_exc.OperationalError,
                sqlalchemy_exc.ProgrammingError) as e:
            if 'already exists' not in str(e):
                raise
        if table.name not in reconcile_indexes:
            continue
        # `Table.create(checkfirst=True)` skips the complete table object when
        # a prior autocommit attempt created the table but crashed between
        # index statements. Reconcile selected indexes independently. In a
        # multi-host race, one duplicate must not prevent later missing
        # indexes from being attempted before the migration is stamped.
        for index in table.indexes:
            try:
                index.create(bind=engine, checkfirst=True)
            except (sqlalchemy_exc.OperationalError,
                    sqlalchemy_exc.ProgrammingError) as e:
                if 'already exists' not in str(e):
                    raise


def add_table_to_db_sqlalchemy(
    metadata: sqlalchemy.MetaData,
    engine: sqlalchemy.Engine | sqlalchemy.engine.Connection,
    table_name: str,
):
    """Add a specific table to the database."""
    try:
        table = metadata.tables[table_name]
    except KeyError as e:
        raise e

    try:
        table.create(bind=engine, checkfirst=True)
    except (sqlalchemy_exc.OperationalError,
            sqlalchemy_exc.ProgrammingError) as e:
        if 'already exists' in str(e):
            pass
        else:
            raise


def add_column_to_table_sqlalchemy(
    session: 'Session',
    table_name: str,
    column_name: str,
    column_type: sqlalchemy.types.TypeEngine,
    default_statement: str | None = None,
    copy_from: str | None = None,
    value_to_replace_existing_entries: Any | None = None,
):
    """Add a column to a table."""
    # column type may be different for different dialects.
    # for example, sqlite uses BLOB for LargeBinary
    # while postgres uses BYTEA.
    column_type_str = column_type.compile(dialect=session.bind.dialect)
    default_statement_str = (f' {default_statement}'
                             if default_statement is not None else '')
    try:
        session.execute(
            sqlalchemy.text(f'ALTER TABLE {table_name} '
                            f'ADD COLUMN {column_name} {column_type_str}'
                            f'{default_statement_str}'))
        if copy_from is not None:
            session.execute(
                sqlalchemy.text(f'UPDATE {table_name} '
                                f'SET {column_name} = {copy_from}'))
        if value_to_replace_existing_entries is not None:
            session.execute(
                sqlalchemy.text(f'UPDATE {table_name} '
                                f'SET {column_name} = :replacement_value '
                                f'WHERE {column_name} IS NULL'),
                {'replacement_value': value_to_replace_existing_entries})
    #sqlite
    except sqlalchemy_exc.OperationalError as e:
        if 'duplicate column name' in str(e):
            pass
        else:
            raise
    #postgresql
    except sqlalchemy_exc.ProgrammingError as e:
        if 'already exists' in str(e):
            pass
        else:
            raise
    session.commit()


def add_column_to_table_alembic(
    table_name: str,
    column_name: str,
    column_type: sqlalchemy.types.TypeEngine,
    server_default: str | None = None,
    copy_from: str | None = None,
    value_to_replace_existing_entries: Any | None = None,
    index: bool | None = None,
):
    """Add a column to a table using Alembic operations.

    This provides the same interface as add_column_to_table_sqlalchemy but
    uses Alembic's connection context for proper migration support.

    Args:
        table_name: Name of the table to add column to
        column_name: Name of the new column
        column_type: SQLAlchemy column type
        server_default: Server-side default value for the column
        copy_from: Column name to copy values from (for existing rows)
        value_to_replace_existing_entries: Default value for existing NULL
            entries
        index: If True, create an index on this column. If None, no index
            is created.
    """
    from alembic import op  # pylint: disable=import-outside-toplevel

    bind = op.get_bind()
    existing_columns = {
        column['name']
        for column in sqlalchemy.inspect(bind).get_columns(table_name)
    }
    if column_name in existing_columns:
        return

    # Check before issuing DDL instead of catching a duplicate-column error.
    # PostgreSQL aborts the entire transaction on that error, so swallowing the
    # exception would make every later statement in the migration fail.
    column = sqlalchemy.Column(column_name,
                               column_type,
                               server_default=server_default,
                               index=index)
    op.add_column(table_name, column)

    # Handle data migration
    if copy_from is not None:
        op.execute(
            sqlalchemy.text(
                f'UPDATE {table_name} SET {column_name} = {copy_from}'))

    if value_to_replace_existing_entries is not None:
        # Use parameterized query for safety
        bind.execute(
            sqlalchemy.text(f'UPDATE {table_name} '
                            f'SET {column_name} = :replacement_value '
                            f'WHERE {column_name} IS NULL'),
            {'replacement_value': value_to_replace_existing_entries})


def drop_column_from_table_alembic(
    table_name: str,
    column_name: str,
):
    """Drop a column from a table using Alembic operations.

    Args:
        table_name: Name of the table to drop column from.
        column_name: Name of the column to drop.
    """
    from alembic import op  # pylint: disable=import-outside-toplevel

    # Check if column exists before trying to drop it
    bind = op.get_bind()
    inspector = sqlalchemy.inspect(bind)
    columns = [col['name'] for col in inspector.get_columns(table_name)]

    if column_name not in columns:
        # Column doesn't exist; nothing to do
        return

    try:
        op.drop_column(table_name, column_name)
    except (sqlalchemy_exc.ProgrammingError,
            sqlalchemy_exc.OperationalError) as e:
        if 'does not exist' in str(e).lower():
            pass  # Already dropped
        else:
            raise


def fault_point():
    """For test fault injection."""
    pass


# Escape character for LIKE patterns built by ``glob_to_like_pattern``. Pass
# it as the ``escape`` argument of ``ColumnOperators.like``.
LIKE_ESCAPE_CHAR = '\\'


def glob_to_like_pattern(glob_pattern: str) -> str:
    """Converts a glob pattern to a SQL LIKE pattern.

    LIKE metacharacters (``%``, ``_``) and the escape character itself are
    escaped so they match literally, then glob wildcards are mapped
    (``*`` -> ``%``, ``?`` -> ``_``). The result must be used with
    ``column.like(pattern, escape=LIKE_ESCAPE_CHAR)``.
    """
    escaped = (glob_pattern.replace(LIKE_ESCAPE_CHAR,
                                    LIKE_ESCAPE_CHAR * 2).replace(
                                        '%', LIKE_ESCAPE_CHAR + '%').replace(
                                            '_', LIKE_ESCAPE_CHAR + '_'))
    return escaped.replace('*', '%').replace('?', '_')


class SQLiteConn(threading.local):
    """Thread-local connection to the sqlite3 database."""

    def __init__(self, db_path: str, create_table: Callable):
        super().__init__()
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, timeout=_DB_TIMEOUT_S)
        self.cursor = self.conn.cursor()
        create_table(self.cursor, self.conn)
        self._async_conn: aiosqlite.Connection | None = None
        self._async_conn_lock: asyncio.Lock | None = None

    async def _get_async_conn(self) -> aiosqlite.Connection:
        """Get the shared aiosqlite connection for current thread.

        Typically, external caller should not get the connection directly,
        instead, SQLiteConn.{operation}_async methods should be used. This
        is to avoid txn interleaving on the shared aiosqlite connection.
        E.g.
        coroutine 1:
            A: await write(row1)
            B: cursor = await conn.execute(read_row1)
            C: await cursor.fetchall()
        coroutine 2:
            D: await write(row2)
            E: cursor = await conn.execute(read_row2)
            F: await cursor.fetchall()
        The A -> B -> D -> E -> C time sequence will cause B and D read at the
        same snapshot point when B started, thus cause coroutine2 lost the
        read-after-write consistency. When you are adding new async operations
        to SQLiteConn, make sure the txn pattern does not cause this issue.
        """
        # Python 3.8 binds current event loop to asyncio.Lock(), which requires
        # a loop available in current thread. Lazy-init the lock to avoid this
        # dependency. The correctness is guranteed since SQLiteConn is
        # thread-local so there is no race condition between check and init.
        if self._async_conn_lock is None:
            self._async_conn_lock = asyncio.Lock()
        if self._async_conn is None:
            async with self._async_conn_lock:
                if self._async_conn is None:
                    # Init logic like requests.init_db_within_lock will handle
                    # initialization like setting the WAL mode, so we do not
                    # duplicate that logic here.
                    # aiosqlite otherwise falls back to sqlite3's five-second
                    # default, which is shorter than the contention policy
                    # used by this class's synchronous connection.
                    self._async_conn = await aiosqlite.connect(
                        self.db_path, timeout=_DB_TIMEOUT_S)
        return self._async_conn

    async def execute_and_commit_async(self,
                                       sql: str,
                                       parameters: Iterable[Any] | None = None
                                      ) -> None:
        """Execute the sql and commit the transaction in a sync block."""
        conn = await self._get_async_conn()

        if parameters is None:
            parameters = []

        def exec_and_commit(sql: str, parameters: Iterable[Any] | None):
            # pylint: disable=protected-access
            with safe_cursor_on_connection(conn._conn) as cursor:
                cursor.execute(sql, parameters)

        # pylint: disable=protected-access
        await conn._execute(exec_and_commit, sql, parameters)

    @aiosqlite.context.contextmanager
    async def execute_fetchall_async(
            self,
            sql: str,
            parameters: Iterable[Any] | None = None) -> Iterable[sqlite3.Row]:
        conn = await self._get_async_conn()
        if parameters is None:
            parameters = []

        def exec_fetch_all(sql: str, parameters: Iterable[Any] | None):
            # pylint: disable=protected-access
            with safe_cursor_on_connection(conn._conn) as cursor:
                cursor.execute(sql, parameters)
                # Note(dev): sqlite3.Connection cannot be patched, keep
                # fault_point here to test the integrity of exec_fetch_all()
                fault_point()
                return cursor.fetchall()

        # pylint: disable=protected-access
        return await conn._execute(exec_fetch_all, sql, parameters)

    async def execute_get_returning_value_async(
            self,
            sql: str,
            parameters: Iterable[Any] | None = None) -> sqlite3.Row | None:
        conn = await self._get_async_conn()

        if parameters is None:
            parameters = []

        def exec_and_get_returning_value(sql: str,
                                         parameters: Iterable[Any] | None):
            # pylint: disable=protected-access
            with safe_cursor_on_connection(conn._conn) as cursor:
                cursor.execute(sql, parameters)
                return cursor.fetchone()

        # pylint: disable=protected-access
        return await conn._execute(exec_and_get_returning_value, sql,
                                   parameters)

    async def close(self):
        if self._async_conn is not None:
            await self._async_conn.close()
        self.conn.close()


class DatabaseManager:
    """Encapsulates lazy engine initialization with double-checked locking.

    Replaces the common pattern of module-level globals (_SQLALCHEMY_ENGINE,
    _SQLALCHEMY_ENGINE_LOCK) and per-module initialize_and_get_db() functions.

    Usage:
        _db_manager = DatabaseManager('my_db', create_table_fn)
    """

    def __init__(
        self,
        db_name: str,
        create_table_fn: Callable[[sqlalchemy.engine.Engine], Any],
        post_init_fn: Callable[[sqlalchemy.engine.Engine], Any] | None = None,
        engine_namespace: str | None = None,
    ):
        self._db_name = db_name
        self._create_table_fn = create_table_fn
        self._post_init_fn = post_init_fn
        self._engine_namespace = engine_namespace
        self._lock = threading.Lock()
        self._engine: sqlalchemy.engine.Engine | None = None
        self._engine_async: sqlalchemy_async.AsyncEngine | None = None

    def get_engine(self) -> sqlalchemy.engine.Engine:
        """Lazy sync engine init with double-checked locking."""
        if self._engine is not None:
            return self._engine
        with self._lock:
            if self._engine is not None:
                return self._engine
            engine = get_engine(self._db_name,
                                engine_namespace=self._engine_namespace)
            self._create_table_fn(engine)
            # Set _engine before post_init_fn so that post_init_fn
            # can access self.engine (e.g. _sqlite_supports_returning).
            self._engine = engine
            if self._post_init_fn is not None:
                self._post_init_fn(engine)
            return self._engine

    async def get_async_engine(self) -> sqlalchemy_async.AsyncEngine:
        """Lazy async engine init; delegates table creation to get_engine."""
        if self._engine_async is not None:
            return self._engine_async

        def init_db():
            with self._lock:
                if self._engine_async is not None:
                    return
                self._engine_async = get_engine(
                    self._db_name,
                    async_engine=True,
                    engine_namespace=self._engine_namespace)
            # Ensure tables are created via the sync path.
            self.get_engine()

        # Use asyncio.to_thread to avoid blocking the event loop, matching the
        # original _init_db_async pattern.
        await asyncio.to_thread(init_db)
        engine = self._engine_async
        if engine is None:
            raise RuntimeError('Async database engine initialization '
                               'completed without an engine.')
        return engine


_max_connections = 0
_postgres_engine_cache: dict[tuple[str, bool, str], sqlalchemy.engine.Engine |
                             sqlalchemy_async.AsyncEngine] = {}
# Session-level advisory locks must keep their PostgreSQL connection for the
# entire lock lifetime.  Reusing the ordinary QueuePool for those connections
# can therefore deadlock a process: a lock checks out the last pooled
# connection, then the protected operation waits for an ORM connection from
# that same pool.  A cached NullPool engine keeps lock sessions on a separate
# connection path and physically closes them when the lock is released.
_postgres_lock_engine_cache: dict[str, sqlalchemy.engine.Engine] = {}
_sqlite_engine_cache: dict[str, sqlalchemy.engine.Engine] = {}

_db_creation_lock = threading.Lock()


def set_postgres_connection_metrics_process_role(process_role: str) -> None:
    """Set the immutable connection-metric role for this process.

    Specialized child entrypoints call this before plugins or database state
    initialize. Repeating the same value is harmless, while changing roles in
    one process would make the counter ambiguous and is rejected.
    """
    if process_role not in _POSTGRES_CONNECTION_METRIC_PROCESS_ROLES:
        raise ValueError(f'Invalid PostgreSQL connection metric process role: '
                         f'{process_role!r}')
    global _postgres_connection_metrics_process_role_override
    with _postgres_connection_metrics_lock:
        current = _postgres_connection_metrics_process_role_override
        if current is None:
            _postgres_connection_metrics_process_role_override = process_role
        elif current != process_role:
            raise RuntimeError('PostgreSQL connection metric process role is '
                               f'already set to {current!r}; cannot change it '
                               f'to {process_role!r}.')


def _postgres_connection_metrics_process_role() -> str:
    override = _postgres_connection_metrics_process_role_override
    if override is not None:
        return override
    process_role = os.environ.get(_API_SERVER_ROLE_ENV_VAR)
    if process_role in _POSTGRES_CONNECTION_METRIC_BASE_PROCESS_ROLES:
        return process_role
    return 'unknown'


def _postgres_connection_metrics_engine_namespace(
        engine_namespace: str | None) -> str:
    if not engine_namespace:
        return 'shared'
    if engine_namespace in _POSTGRES_CONNECTION_METRIC_ENGINE_NAMESPACES:
        return engine_namespace
    return 'other'


def _postgres_connection_metrics_enabled() -> bool:
    return os.environ.get(_POSTGRES_METRICS_ENABLED_ENV_VAR,
                          'false').lower() == 'true'


def _warn_postgres_connection_metrics_failure(error: Exception) -> None:
    global _postgres_connection_metrics_warning_emitted
    with _postgres_connection_metrics_lock:
        if _postgres_connection_metrics_warning_emitted:
            return
        _postgres_connection_metrics_warning_emitted = True
    try:
        logger.warning(
            'Failed to record a PostgreSQL physical-connection metric '
            f'({type(error).__name__}); database connection will continue.')
    except Exception:  # pylint: disable=broad-except
        # Observability must never make a physical database connection fail,
        # even if a custom logging handler is broken.
        pass


def _record_postgres_connection_opened(
    _dbapi_connection: Any,
    _connection_record: Any,
    *,
    engine_namespace: str,
    mode: Literal['sync', 'async'],
) -> None:
    try:
        if not metrics_utils.METRICS_ENABLED:
            return
        metrics_utils.SKY_POSTGRES_CONNECTIONS_OPENED_TOTAL.labels(
            process_role=_postgres_connection_metrics_process_role(),
            engine_namespace=engine_namespace,
            mode=mode).inc()
    except Exception as e:  # pylint: disable=broad-except
        _warn_postgres_connection_metrics_failure(e)


def _install_postgres_connection_metrics_listener(
    engine: sqlalchemy.engine.Engine | sqlalchemy_async.AsyncEngine,
    *,
    engine_namespace: str | None,
    mode: Literal['sync', 'async'],
) -> None:
    if not _postgres_connection_metrics_enabled():
        return
    if mode not in _POSTGRES_CONNECTION_METRIC_MODES:
        raise ValueError(f'Invalid PostgreSQL connection metric mode: {mode}')
    normalized_namespace = _postgres_connection_metrics_engine_namespace(
        engine_namespace)
    target = engine.sync_engine if mode == 'async' else engine
    try:
        sqlalchemy.event.listen(
            target, 'connect',
            functools.partial(_record_postgres_connection_opened,
                              engine_namespace=normalized_namespace,
                              mode=mode))
    except Exception as e:  # pylint: disable=broad-except
        _warn_postgres_connection_metrics_failure(e)


def set_max_connections(max_connections: int):
    """Set the strict process-local synchronous PostgreSQL connection limit.

    Zero disables connection reuse with ``NullPool``; it does not cap concurrent
    unpooled operations. A positive value is enforced by ``QueuePool`` without
    overflow. Configure this before creating a synchronous PostgreSQL engine so
    the cached engine cannot silently retain a stale pool policy.
    """
    global _max_connections
    if max_connections < 0:
        raise ValueError('max_connections must be non-negative')
    _max_connections = max_connections


def get_max_connections():
    return _max_connections


def get_postgres_lock_engine(
        engine: sqlalchemy.engine.Engine) -> sqlalchemy.engine.Engine:
    """Return the dedicated NullPool engine for session advisory locks.

    PostgreSQL session advisory locks are owned by one backend session and
    survive transaction commits.  They cannot safely share the process-local
    QueuePool used by ORM operations: protected code often needs another
    connection, and some cluster operations deliberately nest two advisory
    locks.  ``NullPool`` gives each held lock its required backend session
    without consuming an ordinary pooled checkout.  Closing a connection from
    this engine also closes the physical session, providing a final guarantee
    that PostgreSQL releases every lock owned by it.
    """
    if engine.dialect.name != SQLAlchemyDialect.POSTGRESQL.value:
        raise ValueError('Postgres lock connections require PostgreSQL. '
                         f'Current dialect: {engine.dialect.name}')

    # Preserve credentials when deriving the dedicated engine.  ``str(url)``
    # redacts the password and cannot be used to reconnect.
    connection_url = engine.url.render_as_string(hide_password=False)
    with _db_creation_lock:
        if connection_url not in _postgres_lock_engine_cache:
            lock_engine = sqlalchemy.create_engine(
                engine.url,
                poolclass=sqlalchemy.NullPool,
                connect_args={
                    'connect_timeout': _POSTGRES_CONNECT_TIMEOUT_SECONDS,
                    'application_name': _POSTGRES_LOCK_APPLICATION_NAME,
                })
            _install_postgres_connection_metrics_listener(
                lock_engine, engine_namespace='advisory-lock', mode='sync')
            _postgres_lock_engine_cache[connection_url] = lock_engine
        return _postgres_lock_engine_cache[connection_url]


def create_postgres_nullpool_engine(
    engine: sqlalchemy.engine.Engine,
    *,
    connect_args: dict[str, Any],
    engine_namespace: str,
    pool_reset_on_return: str | None = 'rollback',
) -> sqlalchemy.engine.Engine:
    """Derive one instrumented, physically non-reusing PostgreSQL engine."""
    if engine.dialect.name != SQLAlchemyDialect.POSTGRESQL.value:
        raise ValueError('Postgres NullPool connections require PostgreSQL. '
                         f'Current dialect: {engine.dialect.name}')
    derived = sqlalchemy.create_engine(
        engine.url,
        poolclass=sqlalchemy.NullPool,
        connect_args=dict(connect_args),
        pool_reset_on_return=(pool_reset_on_return))
    _install_postgres_connection_metrics_listener(
        derived, engine_namespace=engine_namespace, mode='sync')
    return derived


def get_postgres_lock_connection(
    engine: sqlalchemy.engine.Engine,) -> sqlalchemy.pool.PoolProxiedConnection:
    """Open a dedicated, non-reused connection for a session advisory lock."""
    return get_postgres_lock_engine(engine).raw_connection()


def _make_asyncpg_creator(dsn: str) -> Callable[[], Any]:
    """Build a SQLAlchemy ``async_creator`` that hands asyncpg a libpq DSN.

    SQLAlchemy's asyncpg dialect normally parses the URL into kwargs and calls
    ``asyncpg.connect(**kwargs)``. asyncpg's kwarg path accepts ``ssl=`` only;
    the libpq query-param names (``sslmode``, ``sslcert``, ``sslkey``,
    ``sslrootcert``, ``sslcrl``) are recognized only when asyncpg parses them
    out of a DSN string itself (see ``asyncpg.connect_utils.
    _parse_connect_dsn_and_args``). Leaving e.g. ``?sslmode=require`` in the
    URL therefore raises ``connect() got an unexpected keyword argument
    'sslmode'``.

    Bypassing SQLAlchemy's URL disassembly via ``async_creator`` lets asyncpg
    parse the DSN itself, so every libpq URI param is handled natively without
    us having to translate. The creator takes no arguments per SQLAlchemy's
    contract — connection params come from the captured DSN, not the URL
    passed to ``create_async_engine`` (which is used only for dialect
    selection).

    Refs:
      https://github.com/sqlalchemy/sqlalchemy/issues/6275
      https://github.com/MagicStack/asyncpg/issues/737
    """
    # pylint: disable=import-outside-toplevel
    import asyncpg

    async def _connect() -> Any:
        return await asyncpg.connect(dsn,
                                     timeout=_POSTGRES_CONNECT_TIMEOUT_SECONDS)

    return _connect


@typing.overload
def get_engine(db_name: str | None,
               async_engine: Literal[False] = False,
               *,
               engine_namespace: str | None = None) -> sqlalchemy.engine.Engine:
    ...


@typing.overload
def get_engine(
        db_name: str | None,
        async_engine: Literal[True],
        *,
        engine_namespace: str | None = None) -> sqlalchemy_async.AsyncEngine:
    ...


def get_engine(
    db_name: str | None,
    async_engine: bool = False,
    *,
    engine_namespace: str | None = None,
) -> sqlalchemy.engine.Engine | sqlalchemy_async.AsyncEngine:
    """Get the engine for the given database name.

    Args:
        db_name: The name of the database. ONLY used for SQLite. On Postgres,
        we use a single database, which we get from the connection string.
        async_engine: Whether to return an async engine.
        engine_namespace: Optional PostgreSQL engine-cache namespace. Callers
            that require an isolated connection pool can use a stable name
            while still sharing the same database. SQLite already keys engines
            by ``db_name`` and ignores this value.

    PostgreSQL synchronous engines use the process-local policy configured by
    ``set_max_connections``. Positive limits are strict: pool overflow is
    disabled and checkout waits are bounded. Async engines deliberately use
    ``NullPool`` because the cached engine can be used from multiple event
    loops; their concurrency is governed by the calling server/executor path,
    not by this synchronous pool setting.
    """
    conn_string = None
    if os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER) is not None:
        conn_string = os.environ.get(constants.ENV_VAR_DB_CONNECTION_URI)
    if conn_string:
        # A namespace deliberately creates a distinct process-local pool for
        # the same PostgreSQL database. Keep sync and async engines separate as
        # well, without embedding credentials in the namespace itself.
        cache_key = (engine_namespace or '', async_engine, conn_string)
        with _db_creation_lock:
            if cache_key not in _postgres_engine_cache:
                engine_type = 'sync' if not async_engine else 'async'
                logger.debug(
                    f'Creating a new postgres {engine_type} engine with '
                    f'maximum {_max_connections} connections')
                created_engine: (sqlalchemy.engine.Engine |
                                 sqlalchemy_async.AsyncEngine)
                if async_engine:
                    # Use NullPool for async engines to avoid event loop binding
                    # issues. asyncpg connection pools bind to the event loop on
                    # first use, which causes "Future attached to a different
                    # loop" errors if the engine is created in a different
                    # context (e.g., a thread). NullPool creates a fresh
                    # connection per operation, avoiding this issue.
                    # Refer to https://docs.sqlalchemy.org/en/21/orm/extensions/asyncio.html#using-multiple-asyncio-event-loops for more details. # pylint: disable=line-too-long
                    created_engine = sqlalchemy_async.create_async_engine(
                        # The URL is used only for dialect selection;
                        # all connection params come from async_creator.
                        'postgresql+asyncpg://',
                        poolclass=sqlalchemy.NullPool,
                        async_creator=_make_asyncpg_creator(conn_string))
                    _install_postgres_connection_metrics_listener(
                        created_engine,
                        engine_namespace=engine_namespace,
                        mode='async')
                elif _max_connections == 0:
                    created_engine = sqlalchemy.create_engine(
                        conn_string,
                        poolclass=sqlalchemy.NullPool,
                        connect_args={
                            'connect_timeout': _POSTGRES_CONNECT_TIMEOUT_SECONDS
                        })
                    _install_postgres_connection_metrics_listener(
                        created_engine,
                        engine_namespace=engine_namespace,
                        mode='sync')
                else:
                    # A positive value is a strict process-local limit, not a
                    # target idle size. In particular, do not restore the
                    # historical "at least five" overflow behavior here: the
                    # server distributes PostgreSQL's usable connection
                    # capacity across its processes.
                    created_engine = sqlalchemy.create_engine(
                        conn_string,
                        poolclass=sqlalchemy.pool.QueuePool,
                        pool_size=_max_connections,
                        max_overflow=0,
                        pool_timeout=_POSTGRES_POOL_TIMEOUT_SECONDS,
                        pool_pre_ping=True,
                        pool_recycle=1800,
                        connect_args={
                            'connect_timeout': _POSTGRES_CONNECT_TIMEOUT_SECONDS
                        })
                    _install_postgres_connection_metrics_listener(
                        created_engine,
                        engine_namespace=engine_namespace,
                        mode='sync')
                _postgres_engine_cache[cache_key] = created_engine
            engine = _postgres_engine_cache[cache_key]
    else:
        if db_name is None:
            raise ValueError('db_name must be provided for SQLite')
        db_path = runtime_utils.get_runtime_dir_path(f'.sky/{db_name}.db')
        pathlib.Path(db_path).parents[0].mkdir(parents=True, exist_ok=True)
        if async_engine:
            # This is an AsyncEngine, instead of a (normal, synchronous) Engine,
            # so we should not put it in the cache. Instead, just return.
            return sqlalchemy_async.create_async_engine(
                'sqlite+aiosqlite:///' + db_path, connect_args={'timeout': 30})
        if db_path not in _sqlite_engine_cache:
            _sqlite_engine_cache[db_path] = sqlalchemy.create_engine(
                'sqlite:///' + db_path)
        engine = _sqlite_engine_cache[db_path]
    return engine
