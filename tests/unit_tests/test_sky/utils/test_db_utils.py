"""Unit tests for database utilities with SKY_RUNTIME_DIR environment variable."""
# pylint: disable=protected-access,redefined-outer-name
import os
import sqlite3
import subprocess
import sys
from unittest import mock

from prometheus_client import CollectorRegistry
from prometheus_client import multiprocess
import pytest
import pytest_asyncio
import sqlalchemy

from sky.utils.db import db_utils


class TestSkyRuntimeDirEnvVar:
    """Test that db_utils correctly uses SKY_RUNTIME_DIR for database paths."""

    @pytest.mark.parametrize('use_custom_dir', [False, True])
    def test_get_engine_runtime_dir(self, tmp_path, monkeypatch,
                                    use_custom_dir):
        """Test get_engine respects SKY_RUNTIME_DIR when set."""
        monkeypatch.delenv('SKY_RUNTIME_DIR', raising=False)
        if use_custom_dir:
            monkeypatch.setenv('SKY_RUNTIME_DIR', str(tmp_path))

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            db_utils.get_engine(db_name='test')

            call_args = mock_create.call_args
            db_path = call_args[0][0]
            if use_custom_dir:
                expected_path = str(tmp_path / '.sky/test.db')
            else:
                expected_path = os.path.expanduser('~/.sky/test.db')
            assert expected_path in db_path

    def test_get_engine_async_custom_runtime_dir(self, tmp_path, monkeypatch):
        """Test async engine creation uses custom SKY_RUNTIME_DIR."""
        monkeypatch.setenv('SKY_RUNTIME_DIR', str(tmp_path))

        with mock.patch(
                'sqlalchemy.ext.asyncio.create_async_engine') as mock_create:
            db_utils.get_engine(db_name='test', async_engine=True)

            call_args = mock_create.call_args
            db_path = call_args[0][0]
            expected_path = str(tmp_path / '.sky/test.db')
            assert expected_path in db_path


@pytest_asyncio.fixture
async def isolated_database(tmp_path):
    """Create an isolated SQLiteConn backed by a real SQLite file."""
    db_path = tmp_path / 'db_utils_async.db'

    def create_table(cursor, conn):
        cursor.execute('CREATE TABLE IF NOT EXISTS items ('
                       'id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)')
        conn.commit()

    conn = db_utils.SQLiteConn(str(db_path), create_table)
    try:
        yield conn, str(db_path)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_execute_fetchall_async_error_does_not_stall_read_txn(
        isolated_database):
    conn, db_path = isolated_database

    await conn.execute_and_commit_async('INSERT INTO items (value) VALUES (?)',
                                        ('initial',))
    with pytest.raises(sqlite3.OperationalError):
        with mock.patch.object(db_utils,
                               'fault_point',
                               side_effect=sqlite3.OperationalError('BOOM')):
            async with conn.execute_fetchall_async('SELECT * FROM items') as _:
                pass

    # Another connection writes to the database while the failed read is cleaned
    # up to ensure there is no lingering read transaction.
    with sqlite3.connect(db_path) as external_conn:
        external_conn.execute('INSERT INTO items (value) VALUES (?)',
                              ('external',))

    # The original connection should be able to promote to a write txn without
    # hitting "database is locked".
    await conn.execute_and_commit_async('INSERT INTO items (value) VALUES (?)',
                                        ('after_error',))

    async with conn.execute_fetchall_async(
            'SELECT value FROM items ORDER BY id') as rows:
        values = [row[0] for row in rows]

    assert values == ['initial', 'external', 'after_error']


@pytest.mark.asyncio
async def test_sqlite_async_connection_uses_standard_lock_timeout(
        isolated_database):
    conn, _ = isolated_database

    async with conn.execute_fetchall_async('PRAGMA busy_timeout') as rows:
        busy_timeout_ms = rows[0][0]

    assert busy_timeout_ms == 60_000


class TestGetEngine:
    """Tests for get_engine function."""

    @pytest.fixture(autouse=True)
    def clear_caches(self, monkeypatch):
        """Clear engine caches before each test."""
        # Clear the module-level caches
        db_utils._postgres_engine_cache.clear()
        db_utils._postgres_lock_engine_cache.clear()
        db_utils._sqlite_engine_cache.clear()
        monkeypatch.setattr(
            db_utils, '_postgres_connection_metrics_process_role_override',
            None)
        monkeypatch.setattr(db_utils,
                            '_postgres_connection_metrics_warning_emitted',
                            False)
        # Reset max_connections to default
        db_utils.set_max_connections(0)
        # Ensure we're not in server mode by default
        monkeypatch.delenv('IS_SKYPILOT_SERVER', raising=False)
        monkeypatch.delenv('SKYPILOT_DB_CONNECTION_URI', raising=False)
        monkeypatch.delenv('SKYPILOT_API_SERVER_ROLE', raising=False)
        monkeypatch.delenv('SKY_API_SERVER_METRICS_ENABLED', raising=False)

    def test_sqlite_sync_engine_creation(self, tmp_path, monkeypatch):
        """Test SQLite sync engine is created correctly."""
        monkeypatch.setenv('SKY_RUNTIME_DIR', str(tmp_path))

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            engine = db_utils.get_engine(db_name='test_db')

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert 'sqlite:///' in call_args[0][0]
            assert 'test_db.db' in call_args[0][0]
            assert engine == mock_engine

    def test_sqlite_sync_engine_caching(self, tmp_path, monkeypatch):
        """Test SQLite sync engine is cached and reused."""
        monkeypatch.setenv('SKY_RUNTIME_DIR', str(tmp_path))

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            engine1 = db_utils.get_engine(db_name='cached_db')
            engine2 = db_utils.get_engine(db_name='cached_db')

            # Should only create once
            assert mock_create.call_count == 1
            assert engine1 is engine2

    def test_sqlite_async_engine_creation(self, tmp_path, monkeypatch):
        """Test SQLite async engine is created correctly."""
        monkeypatch.setenv('SKY_RUNTIME_DIR', str(tmp_path))

        with mock.patch(
                'sqlalchemy.ext.asyncio.create_async_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            engine = db_utils.get_engine(db_name='async_db', async_engine=True)

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert 'sqlite+aiosqlite:///' in call_args[0][0]
            assert 'async_db.db' in call_args[0][0]
            assert call_args[1]['connect_args'] == {'timeout': 30}
            assert engine == mock_engine

    def test_sqlite_async_engine_not_cached(self, tmp_path, monkeypatch):
        """Test SQLite async engines are NOT cached (unlike sync engines)."""
        monkeypatch.setenv('SKY_RUNTIME_DIR', str(tmp_path))

        with mock.patch(
                'sqlalchemy.ext.asyncio.create_async_engine') as mock_create:
            mock_engine1 = mock.MagicMock()
            mock_engine2 = mock.MagicMock()
            mock_create.side_effect = [mock_engine1, mock_engine2]

            engine1 = db_utils.get_engine(db_name='async_db', async_engine=True)
            engine2 = db_utils.get_engine(db_name='async_db', async_engine=True)

            # Async SQLite engines are NOT cached, so create should be called twice
            assert mock_create.call_count == 2
            assert engine1 is not engine2

    def test_sqlite_db_name_required(self, monkeypatch):
        """Test that db_name is required for SQLite."""
        monkeypatch.delenv('IS_SKYPILOT_SERVER', raising=False)

        with pytest.raises(ValueError,
                           match='db_name must be provided for SQLite'):
            db_utils.get_engine(db_name=None)

    def test_postgres_sync_engine_creation_with_nullpool(self, monkeypatch):
        """Test Postgres sync engine with NullPool when max_connections=0."""
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')
        db_utils.set_max_connections(0)

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            engine = db_utils.get_engine(db_name='ignored')

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert call_args[0][0] == 'postgresql://user:pass@localhost/db'
            assert call_args[1]['poolclass'] == sqlalchemy.NullPool
            assert call_args[1]['connect_args'] == {'connect_timeout': 15}
            assert engine == mock_engine

    def test_postgres_sync_engine_creation_with_queuepool(self, monkeypatch):
        """Test Postgres sync engine with QueuePool when max_connections>0."""
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')
        db_utils.set_max_connections(10)

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            engine = db_utils.get_engine(db_name='ignored')

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert call_args[0][0] == 'postgresql://user:pass@localhost/db'
            assert call_args[1]['poolclass'] == sqlalchemy.pool.QueuePool
            assert call_args[1]['pool_size'] == 10
            assert call_args[1]['max_overflow'] == 0
            assert call_args[1]['pool_timeout'] == 15
            assert call_args[1]['pool_pre_ping'] is True
            assert call_args[1]['pool_recycle'] == 1800
            assert call_args[1]['connect_args'] == {'connect_timeout': 15}
            assert engine == mock_engine

    def test_postgres_sync_engine_queuepool_limit_has_no_overflow(
            self, monkeypatch):
        """Test small configured limits remain strict QueuePool limits."""
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')

        db_utils.set_max_connections(1)

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            db_utils.get_engine(db_name='ignored')

            call_args = mock_create.call_args
            assert call_args[1]['pool_size'] == 1
            assert call_args[1]['max_overflow'] == 0
            assert call_args[1]['pool_timeout'] == 15

    def test_postgres_engine_namespaces_isolate_queuepools(self, monkeypatch):
        """Named users get distinct strict pools for the same PostgreSQL DB."""
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')
        db_utils.set_max_connections(1)
        ordinary_engine = mock.MagicMock()
        request_control_engine = mock.MagicMock()

        with mock.patch('sqlalchemy.create_engine',
                        side_effect=[ordinary_engine,
                                     request_control_engine]) as mock_create:
            ordinary = db_utils.get_engine(db_name='state')
            control = db_utils.get_engine(
                db_name='api_requests', engine_namespace='api-requests-control')
            ordinary_again = db_utils.get_engine(db_name='other_state')
            control_again = db_utils.get_engine(
                db_name='ignored', engine_namespace='api-requests-control')

        assert ordinary is ordinary_engine
        assert control is request_control_engine
        assert ordinary_again is ordinary
        assert control_again is control
        assert mock_create.call_count == 2
        for call in mock_create.call_args_list:
            assert call.kwargs['poolclass'] == sqlalchemy.pool.QueuePool
            assert call.kwargs['pool_size'] == 1
            assert call.kwargs['max_overflow'] == 0

    def test_postgres_lock_connections_use_separate_nullpool(self):
        """Session locks must not consume an ordinary QueuePool checkout."""
        ordinary_engine = mock.MagicMock()
        ordinary_engine.dialect.name = (
            db_utils.SQLAlchemyDialect.POSTGRESQL.value)
        ordinary_engine.url = sqlalchemy.engine.make_url(
            'postgresql://user:pass@localhost/db')
        lock_engine = mock.MagicMock()
        lock_connection = mock.MagicMock()
        lock_engine.raw_connection.return_value = lock_connection

        with mock.patch('sqlalchemy.create_engine',
                        return_value=lock_engine) as mock_create:
            connection_one = db_utils.get_postgres_lock_connection(
                ordinary_engine)
            connection_two = db_utils.get_postgres_lock_connection(
                ordinary_engine)

        assert connection_one is lock_connection
        assert connection_two is lock_connection
        mock_create.assert_called_once_with(
            ordinary_engine.url,
            poolclass=sqlalchemy.NullPool,
            connect_args={
                'connect_timeout': 15,
                'application_name': 'skypilot-advisory-lock',
            })
        assert lock_engine.raw_connection.call_count == 2
        ordinary_engine.raw_connection.assert_not_called()

    def test_postgres_lock_connection_rejects_sqlite(self):
        engine = mock.MagicMock()
        engine.dialect.name = db_utils.SQLAlchemyDialect.SQLITE.value

        with pytest.raises(ValueError,
                           match='lock connections require PostgreSQL'):
            db_utils.get_postgres_lock_connection(engine)

    def test_max_connections_must_be_non_negative(self):
        with pytest.raises(ValueError,
                           match='max_connections must be non-negative'):
            db_utils.set_max_connections(-1)

    def test_postgres_async_engine_creation(self, monkeypatch):
        """Test Postgres async engine uses asyncpg and NullPool."""
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')

        with mock.patch(
                'sqlalchemy.ext.asyncio.create_async_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            engine = db_utils.get_engine(db_name='ignored', async_engine=True)

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            # URL is just the dialect placeholder; all connection params
            # are supplied via async_creator (see _make_asyncpg_creator).
            assert call_args[0][0] == 'postgresql+asyncpg://'
            assert call_args[1]['poolclass'] == sqlalchemy.NullPool
            assert callable(call_args[1].get('async_creator'))
            assert engine == mock_engine

    @pytest.mark.asyncio
    async def test_postgres_async_engine_does_not_leak_libpq_kwargs_to_asyncpg(
            self, monkeypatch):
        """End-to-end check: with a sslmode-bearing URI, real SQLAlchemy
        must not forward libpq query params as kwargs to asyncpg.connect.

        Without the fix in ``get_engine``, SQLAlchemy's asyncpg dialect
        parses the URL into kwargs and calls
        ``asyncpg.connect(host=..., port=..., ..., sslmode='require')``,
        which asyncpg rejects with
        ``unexpected keyword argument 'sslmode'``. We capture the creator
        passed to SQLAlchemy, invoke it with ``asyncpg.connect`` mocked at the
        boundary, and inspect how it was actually called.

        See https://github.com/sqlalchemy/sqlalchemy/issues/6275.
        """
        libpq_uri = 'postgresql://user:pass@localhost/db?sslmode=require'
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI', libpq_uri)

        # Capture and invoke the async creator that get_engine gives SQLAlchemy.
        # A generic AsyncMock is not a valid asyncpg connection and driving it
        # through SQLAlchemy leaks awaitables from the adapter internals.
        with mock.patch('asyncpg.connect',
                        new_callable=mock.AsyncMock) as mock_connect, \
             mock.patch('sqlalchemy.ext.asyncio.create_async_engine') as mock_create:
            connection = object()
            mock_connect.return_value = connection

            engine = db_utils.get_engine(db_name='ignored', async_engine=True)
            assert engine is mock_create.return_value

            async_creator = mock_create.call_args.kwargs['async_creator']
            assert await async_creator() is connection

        mock_connect.assert_awaited_once_with(
            libpq_uri, timeout=db_utils._POSTGRES_CONNECT_TIMEOUT_SECONDS)

    def test_postgres_engine_caching(self, monkeypatch):
        """Test Postgres sync engines are cached and reused."""
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')
        db_utils.set_max_connections(0)

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            engine1 = db_utils.get_engine(db_name='ignored')
            engine2 = db_utils.get_engine(db_name='any_name')

            # Should only create once regardless of db_name
            assert mock_create.call_count == 1
            assert engine1 is engine2

    def test_postgres_async_engine_caching(self, monkeypatch):
        """Test Postgres async engines are cached and reused."""
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')

        with mock.patch(
                'sqlalchemy.ext.asyncio.create_async_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            engine1 = db_utils.get_engine(db_name='ignored', async_engine=True)
            engine2 = db_utils.get_engine(db_name='any_name', async_engine=True)

            # Should only create once regardless of db_name
            assert mock_create.call_count == 1
            assert engine1 is engine2

    def test_postgres_sync_and_async_engines_cached_separately(
            self, monkeypatch):
        """Test sync and async Postgres engines are cached separately."""
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')
        db_utils.set_max_connections(0)

        with mock.patch('sqlalchemy.create_engine') as mock_sync_create, \
             mock.patch('sqlalchemy.ext.asyncio.create_async_engine') as mock_async_create:
            mock_sync_engine = mock.MagicMock()
            mock_async_engine = mock.MagicMock()
            mock_sync_create.return_value = mock_sync_engine
            mock_async_create.return_value = mock_async_engine

            sync_engine = db_utils.get_engine(db_name='ignored')
            async_engine = db_utils.get_engine(db_name='ignored',
                                               async_engine=True)

            assert mock_sync_create.call_count == 1
            assert mock_async_create.call_count == 1
            assert sync_engine is not async_engine

    def test_postgres_db_name_ignored(self, monkeypatch):
        """Test that db_name is ignored when using Postgres."""
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')
        db_utils.set_max_connections(0)

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            # db_name can be None or any value for Postgres
            engine1 = db_utils.get_engine(db_name=None)
            engine2 = db_utils.get_engine(db_name='some_db')
            engine3 = db_utils.get_engine(db_name='other_db')

            # All should return the same cached engine
            assert engine1 is engine2 is engine3
            assert mock_create.call_count == 1

    def test_env_var_is_skypilot_server_required_for_postgres(
            self, monkeypatch):
        """Test IS_SKYPILOT_SERVER env var is required for Postgres mode."""
        # Only set DB_CONNECTION_URI, not IS_SKYPILOT_SERVER
        monkeypatch.delenv('IS_SKYPILOT_SERVER', raising=False)
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            # Should fall back to SQLite mode since IS_SKYPILOT_SERVER is not set
            with pytest.raises(ValueError, match='db_name must be provided'):
                db_utils.get_engine(db_name=None)

    def test_directory_created_for_sqlite(self, tmp_path, monkeypatch):
        """Test that parent directory is created for SQLite database."""
        runtime_dir = tmp_path / 'nonexistent' / 'path'
        monkeypatch.setenv('SKY_RUNTIME_DIR', str(runtime_dir))

        with mock.patch('sqlalchemy.create_engine') as mock_create:
            mock_engine = mock.MagicMock()
            mock_create.return_value = mock_engine

            db_utils.get_engine(db_name='test_db')

            # Parent directory should be created
            expected_dir = runtime_dir / '.sky'
            assert expected_dir.exists()

    @pytest.mark.parametrize(('namespace', 'expected'),
                             [(None, 'shared'), ('', 'shared'),
                              ('api-requests-control', 'api-requests-control'),
                              ('advisory-lock', 'advisory-lock'),
                              ('physical-capacity-evidence', 'other'),
                              ('caller-controlled-value', 'other')])
    def test_postgres_connection_metric_namespace_is_bounded(
            self, namespace, expected):
        assert (db_utils._postgres_connection_metrics_engine_namespace(
            namespace) == expected)

    def test_postgres_connection_metric_label_sets_are_closed(self):
        assert db_utils._POSTGRES_CONNECTION_METRIC_PROCESS_ROLES == frozenset({
            'all',
            'api',
            'executor',
            'controller',
            'authority-worker',
            'managed-job-controller',
            'serve-controller',
            'unknown',
        })
        assert db_utils._POSTGRES_CONNECTION_METRIC_BASE_PROCESS_ROLES == (
            frozenset({
                'all',
                'api',
                'executor',
                'controller',
                'authority-worker',
            }))
        assert db_utils._POSTGRES_CONNECTION_METRIC_ENGINE_NAMESPACES == (
            frozenset({
                'shared',
                'api-requests-control',
                'advisory-lock',
                'other',
            }))
        assert db_utils._POSTGRES_CONNECTION_METRIC_MODES == frozenset(
            {'sync', 'async'})
        assert (len(db_utils._POSTGRES_CONNECTION_METRIC_PROCESS_ROLES) *
                len(db_utils._POSTGRES_CONNECTION_METRIC_ENGINE_NAMESPACES) *
                len(db_utils._POSTGRES_CONNECTION_METRIC_MODES) == 64)

    def test_postgres_connection_metric_process_role_is_write_once(
            self, monkeypatch):
        monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'api')
        assert db_utils._postgres_connection_metrics_process_role() == 'api'

        db_utils.set_postgres_connection_metrics_process_role(
            'serve-controller')
        db_utils.set_postgres_connection_metrics_process_role(
            'serve-controller')
        assert (db_utils._postgres_connection_metrics_process_role() ==
                'serve-controller')

        with pytest.raises(RuntimeError, match='already set'):
            db_utils.set_postgres_connection_metrics_process_role(
                'managed-job-controller')
        with pytest.raises(ValueError, match='Invalid PostgreSQL'):
            db_utils.set_postgres_connection_metrics_process_role(
                'service-name-from-user-input')

    def test_postgres_connection_metric_unknown_base_role_is_bounded(
            self, monkeypatch):
        monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'unexpected-role')
        assert (
            db_utils._postgres_connection_metrics_process_role() == 'unknown')

    def test_postgres_connections_opened_counter_has_only_bounded_labels(self):
        labels = {
            'process_role': 'managed-job-controller',
            'engine_namespace': 'shared',
            'mode': 'async',
        }
        counter = db_utils.metrics_utils.SKY_POSTGRES_CONNECTIONS_OPENED_TOTAL
        counter.labels(**labels).inc()

        samples = [
            sample for family in counter.collect() for sample in family.samples
            if sample.name == 'sky_postgres_connections_opened_total'
        ]

        assert any(sample.labels == labels for sample in samples)
        assert all('pid' not in sample.labels for sample in samples)

    def test_postgres_connections_opened_counter_is_collected_multiprocess(
            self, tmp_path):
        script = """
from sky.metrics import utils as metrics_utils
metrics_utils.SKY_POSTGRES_CONNECTIONS_OPENED_TOTAL.labels(
    process_role='serve-controller',
    engine_namespace='shared',
    mode='async',
).inc()
"""
        env = os.environ.copy()
        env['PROMETHEUS_MULTIPROC_DIR'] = str(tmp_path)
        env['SKY_API_SERVER_METRICS_ENABLED'] = 'true'
        subprocess.run([sys.executable, '-c', script],
                       env=env,
                       capture_output=True,
                       text=True,
                       check=True,
                       timeout=60)

        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry, path=str(tmp_path))
        samples = [
            sample for family in registry.collect() for sample in family.samples
            if sample.name == 'sky_postgres_connections_opened_total'
        ]

        assert len(samples) == 1
        assert samples[0].labels == {
            'process_role': 'serve-controller',
            'engine_namespace': 'shared',
            'mode': 'async',
        }
        assert samples[0].value == 1
        assert 'pid' not in samples[0].labels

    def test_postgres_metrics_disabled_does_not_resolve_or_listen(
            self, monkeypatch):
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')
        lazy_metrics = mock.MagicMock()
        monkeypatch.setattr(db_utils, 'metrics_utils', lazy_metrics)

        with mock.patch('sqlalchemy.create_engine') as create_engine, \
             mock.patch('sqlalchemy.event.listen') as listen:
            db_utils.get_engine(db_name='ignored')

        create_engine.assert_called_once()
        listen.assert_not_called()
        assert not lazy_metrics.mock_calls

    def test_postgres_sync_listener_attaches_once_on_cache_miss(
            self, monkeypatch):
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')
        monkeypatch.setenv('SKY_API_SERVER_METRICS_ENABLED', 'true')
        engine = mock.MagicMock()

        with mock.patch('sqlalchemy.create_engine', return_value=engine), \
             mock.patch('sqlalchemy.event.listen') as listen:
            first = db_utils.get_engine(db_name='state')
            second = db_utils.get_engine(db_name='other')

        assert first is second is engine
        listen.assert_called_once()
        assert listen.call_args.args[:2] == (engine, 'connect')
        callback = listen.call_args.args[2]
        assert callback.keywords == {
            'engine_namespace': 'shared',
            'mode': 'sync',
        }

    def test_postgres_async_listener_uses_sync_engine(self, monkeypatch):
        monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
        monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI',
                           'postgresql://user:pass@localhost/db')
        monkeypatch.setenv('SKY_API_SERVER_METRICS_ENABLED', 'true')
        engine = mock.MagicMock()

        with mock.patch('sqlalchemy.ext.asyncio.create_async_engine',
                        return_value=engine), \
             mock.patch('sqlalchemy.event.listen') as listen:
            db_utils.get_engine(db_name='state', async_engine=True)
            db_utils.get_engine(db_name='other', async_engine=True)

        listen.assert_called_once()
        assert listen.call_args.args[:2] == (engine.sync_engine, 'connect')
        assert listen.call_args.args[2].keywords == {
            'engine_namespace': 'shared',
            'mode': 'async',
        }

    def test_postgres_lock_engine_attaches_once_with_bounded_namespace(
            self, monkeypatch):
        monkeypatch.setenv('SKY_API_SERVER_METRICS_ENABLED', 'true')
        base_engine = mock.MagicMock()
        base_engine.dialect.name = (db_utils.SQLAlchemyDialect.POSTGRESQL.value)
        base_engine.url = sqlalchemy.engine.make_url(
            'postgresql://user:pass@localhost/db')
        lock_engine = mock.MagicMock()

        with mock.patch('sqlalchemy.create_engine', return_value=lock_engine), \
             mock.patch('sqlalchemy.event.listen') as listen:
            first_lock = db_utils.get_postgres_lock_engine(base_engine)
            second_lock = db_utils.get_postgres_lock_engine(base_engine)

        assert first_lock is second_lock is lock_engine
        listen.assert_called_once()
        assert listen.call_args.args[:2] == (lock_engine, 'connect')
        assert listen.call_args.args[2].keywords == {
            'engine_namespace': 'advisory-lock',
            'mode': 'sync',
        }

    def test_connection_metric_counts_connects_not_queuepool_checkouts(
            self, monkeypatch):
        monkeypatch.setenv('SKY_API_SERVER_METRICS_ENABLED', 'true')
        monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'api')
        counter = mock.MagicMock()
        metrics = mock.MagicMock(METRICS_ENABLED=True)
        metrics.SKY_POSTGRES_CONNECTIONS_OPENED_TOTAL = counter
        monkeypatch.setattr(db_utils, 'metrics_utils', metrics)

        null_engine = sqlalchemy.create_engine('sqlite://',
                                               poolclass=sqlalchemy.NullPool)
        db_utils._install_postgres_connection_metrics_listener(
            null_engine, engine_namespace='advisory-lock', mode='sync')
        try:
            with null_engine.connect():
                pass
            with null_engine.connect():
                pass
        finally:
            null_engine.dispose()

        assert counter.labels.call_count == 2
        counter.labels.assert_called_with(process_role='api',
                                          engine_namespace='advisory-lock',
                                          mode='sync')
        assert counter.labels.return_value.inc.call_count == 2

        counter.reset_mock()
        queue_engine = sqlalchemy.create_engine(
            'sqlite://',
            poolclass=sqlalchemy.pool.QueuePool,
            pool_size=1,
            max_overflow=0)
        db_utils._install_postgres_connection_metrics_listener(
            queue_engine, engine_namespace=None, mode='sync')
        try:
            with queue_engine.connect():
                pass
            with queue_engine.connect():
                pass
        finally:
            queue_engine.dispose()

        counter.labels.assert_called_once_with(process_role='api',
                                               engine_namespace='shared',
                                               mode='sync')
        counter.labels.return_value.inc.assert_called_once_with()

    def test_connection_metric_resolves_role_when_connection_opens(
            self, monkeypatch):
        monkeypatch.setenv('SKY_API_SERVER_METRICS_ENABLED', 'true')
        counter = mock.MagicMock()
        metrics = mock.MagicMock(METRICS_ENABLED=True)
        metrics.SKY_POSTGRES_CONNECTIONS_OPENED_TOTAL = counter
        monkeypatch.setattr(db_utils, 'metrics_utils', metrics)
        engine = sqlalchemy.create_engine('sqlite://',
                                          poolclass=sqlalchemy.NullPool)
        db_utils._install_postgres_connection_metrics_listener(
            engine, engine_namespace=None, mode='sync')
        monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'controller')

        try:
            with engine.connect():
                pass
        finally:
            engine.dispose()

        counter.labels.assert_called_once_with(process_role='controller',
                                               engine_namespace='shared',
                                               mode='sync')

    def test_failed_physical_connection_does_not_increment(self, monkeypatch):
        monkeypatch.setenv('SKY_API_SERVER_METRICS_ENABLED', 'true')
        counter = mock.MagicMock()
        metrics = mock.MagicMock(METRICS_ENABLED=True)
        metrics.SKY_POSTGRES_CONNECTIONS_OPENED_TOTAL = counter
        monkeypatch.setattr(db_utils, 'metrics_utils', metrics)

        def fail_to_connect():
            raise sqlite3.OperationalError('expected connection failure')

        engine = sqlalchemy.create_engine('sqlite://',
                                          poolclass=sqlalchemy.NullPool,
                                          creator=fail_to_connect)
        db_utils._install_postgres_connection_metrics_listener(
            engine, engine_namespace=None, mode='sync')
        try:
            with pytest.raises(sqlalchemy.exc.OperationalError):
                engine.connect()
        finally:
            engine.dispose()

        counter.labels.assert_not_called()

    def test_metric_failure_is_fail_open_and_warns_once(self, monkeypatch,
                                                        caplog):
        monkeypatch.setenv('SKY_API_SERVER_METRICS_ENABLED', 'true')
        metrics = mock.MagicMock(METRICS_ENABLED=True)
        metrics.SKY_POSTGRES_CONNECTIONS_OPENED_TOTAL.labels.side_effect = (
            OSError('expected metrics failure'))
        monkeypatch.setattr(db_utils, 'metrics_utils', metrics)
        engine = sqlalchemy.create_engine('sqlite://',
                                          poolclass=sqlalchemy.NullPool)
        db_utils._install_postgres_connection_metrics_listener(
            engine, engine_namespace=None, mode='sync')

        try:
            with engine.connect():
                pass
            with engine.connect():
                pass
        finally:
            engine.dispose()

        assert caplog.text.count('database connection will continue') == 1

    def test_metric_and_warning_failures_are_fail_open(self, monkeypatch):
        monkeypatch.setenv('SKY_API_SERVER_METRICS_ENABLED', 'true')
        metrics = mock.MagicMock(METRICS_ENABLED=True)
        metrics.SKY_POSTGRES_CONNECTIONS_OPENED_TOTAL.labels.side_effect = (
            OSError('expected metrics failure'))
        monkeypatch.setattr(db_utils, 'metrics_utils', metrics)
        monkeypatch.setattr(
            db_utils.logger, 'warning',
            mock.Mock(side_effect=RuntimeError('broken logger')))

        db_utils._record_postgres_connection_opened(None,
                                                    None,
                                                    engine_namespace='shared',
                                                    mode='sync')

        db_utils.logger.warning.assert_called_once()

    def test_listener_registration_failure_is_fail_open(self, monkeypatch,
                                                        caplog):
        monkeypatch.setenv('SKY_API_SERVER_METRICS_ENABLED', 'true')
        engine = mock.MagicMock()

        with mock.patch('sqlalchemy.event.listen',
                        side_effect=TypeError('expected listener failure')):
            db_utils._install_postgres_connection_metrics_listener(
                engine, engine_namespace=None, mode='sync')

        assert caplog.text.count('database connection will continue') == 1
