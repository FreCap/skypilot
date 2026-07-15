"""Real-PostgreSQL parity tests for the shared KV cache."""
# pylint: disable=protected-access,redefined-outer-name,unused-argument
import concurrent.futures
import contextlib
import time
from unittest import mock

import pytest
import sqlalchemy
from testcontainers import postgres as testcontainers_postgres

from sky.utils.db import kv_cache


@pytest.fixture(scope='module')
def postgres_engine():
    container = testcontainers_postgres.PostgresContainer('postgres:16')
    container.start()
    engine = sqlalchemy.create_engine(container.get_connection_url())
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


@pytest.fixture
def postgres_database(postgres_engine, monkeypatch):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    monkeypatch.setattr(kv_cache._db_manager, '_engine', postgres_engine)
    kv_cache.create_table(postgres_engine)
    yield postgres_engine


@contextlib.contextmanager
def _count_sql_statements(engine):
    count = {'value': 0}

    def _before_cursor_execute(*_args, **_kwargs):
        count['value'] += 1

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _before_cursor_execute)
    try:
        yield count
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _before_cursor_execute)


def test_postgres_all_operations_and_observability(postgres_database,
                                                   monkeypatch):
    now = time.time()
    warning = mock.Mock()
    record = mock.Mock()
    monkeypatch.setattr(kv_cache.logger, 'warning', warning)
    monkeypatch.setattr(kv_cache.metrics_lib, 'METRICS_ENABLED', True)
    monkeypatch.setattr(kv_cache.metrics_lib, 'record_persistence_operation',
                        record)

    kv_cache.add_or_update_cache_entry('perm:ws:a:user-1', '1', now + 100)
    kv_cache.add_or_update_cache_entry('perm:ws:b:user-1', '1', now + 100)
    kv_cache.add_or_update_cache_entry('perm:ws:a:user-2', '0', now + 100)
    assert kv_cache.get_cache_entry('perm:ws:a:user-1') == '1'
    kv_cache.delete_cache_entry('perm:ws:a:user-2')
    kv_cache.delete_cache_entries_by_prefix_suffix('perm:ws:', ':user-1')
    assert kv_cache.get_cache_entry('perm:ws:a:user-1') is None
    assert kv_cache.get_cache_entry('perm:ws:b:user-1') is None

    kv_cache.add_or_update_cache_entry('50%off:item', '1', now + 100)
    kv_cache.add_or_update_cache_entry('50Xoff:item', '1', now + 100)
    kv_cache.delete_cache_entries_by_prefix('50%off:')
    assert kv_cache.get_cache_entry('50%off:item') is None
    assert kv_cache.get_cache_entry('50Xoff:item') == '1'

    warning.assert_not_called()
    assert record.call_count == 13
    assert all(call.args[-1] == 'postgresql' for call in record.call_args_list)


def test_postgres_extend_is_monotonic_under_concurrent_writers(
        postgres_database, monkeypatch):
    monkeypatch.setattr(kv_cache.time, 'time', lambda: 1000.0)

    def _write(value, expires_at):
        kv_cache.add_or_extend_cache_entry('capacity:key', value, expires_at)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(_write, f'value-{expiry}', float(expiry))
            for expiry in range(1100, 1180, 10)
        ]
        for future in futures:
            future.result(timeout=10)

    assert kv_cache.get_cache_entry('capacity:key') == 'value-1170'


def test_postgres_migration_is_idempotent_and_lookup_uses_primary_key(
        postgres_database):
    kv_cache.create_table(postgres_database)
    kv_cache.create_table(postgres_database)
    kv_cache.add_or_update_cache_entry('lookup-key', 'value', time.time() + 60)

    with postgres_database.connect() as connection:
        connection.exec_driver_sql('SET enable_seqscan = off')
        plan = '\n'.join(row[0] for row in connection.exec_driver_sql(
            "EXPLAIN SELECT value FROM kv_cache "
            "WHERE key = 'lookup-key' AND expires_at > 0"))
    assert 'kv_cache_pkey' in plan


def test_postgres_get_is_one_statement(postgres_database):
    kv_cache.add_or_update_cache_entry('lookup-key', 'value', time.time() + 60)

    with _count_sql_statements(postgres_database) as count:
        assert kv_cache.get_cache_entry('lookup-key') == 'value'

    assert count['value'] == 1


def test_postgres_cache_survives_engine_restart(postgres_database, monkeypatch):
    kv_cache.add_or_update_cache_entry('restart-key', 'value', time.time() + 60)
    restarted_engine = sqlalchemy.create_engine(postgres_database.url)
    monkeypatch.setattr(kv_cache._db_manager, '_engine', restarted_engine)
    try:
        assert kv_cache.get_cache_entry('restart-key') == 'value'
    finally:
        restarted_engine.dispose()
