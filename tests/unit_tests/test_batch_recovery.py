"""Focused tests for Sky Batch recovery and bounded-memory reduction."""

# pylint: disable=protected-access,redefined-outer-name

import asyncio
import contextlib
import importlib
import os
import shutil
import subprocess
import threading
import time
import types
from unittest import mock

from alembic import migration
from alembic import operations
import pytest
import sqlalchemy

from sky.batch import coordinator
from sky.batch import io_formats
from sky.batch import utils
from sky.batch import worker
from sky.jobs import batch_state as batch_state_lib
from sky.jobs import controller as jobs_controller
from sky.jobs import state
from sky.utils.db import migration_utils


@pytest.fixture
def batch_state_db(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "batch-state.db"}')
    monkeypatch.setattr(state._db_manager, '_engine', engine)
    state.Base.metadata.create_all(engine)
    yield
    engine.dispose()


@contextlib.contextmanager
def _count_sql_statements(engine):
    counts = {'n': 0}

    def _count(*args, **kwargs):
        del args, kwargs
        counts['n'] += 1

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _count)
    try:
        yield counts
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', _count)


def _create_batch_job(job_id: int, owner_token: str) -> None:
    engine = state._db_manager.get_engine()
    with engine.begin() as connection:
        connection.execute(state.job_info_table.insert().values(
            spot_job_id=job_id, is_batch=True))
    assert state.acquire_batch_coordinator(job_id, owner_token) is None


def _create_running_batch_task(job_id: int) -> None:
    engine = state._db_manager.get_engine()
    with engine.begin() as connection:
        connection.execute(state.spot_table.insert().values(
            spot_job_id=job_id,
            task_id=0,
            status=state.ManagedJobStatus.RUNNING.value,
            end_at=None))


def test_batch_attempt_fences_stale_transitions_and_persists_backoff(
        batch_state_db):
    del batch_state_db
    _create_batch_job(7, 'owner-a')
    assert state.save_batch_states(7, [[0, 9]], 'owner-a')

    claim_1 = state.claim_batch(7,
                                0,
                                'owner-a',
                                'worker-a',
                                lease_duration=10,
                                now=100)
    assert claim_1 == (1, 0)
    assert state.claim_batch(7,
                             0,
                             'owner-a',
                             'worker-b',
                             lease_duration=10,
                             now=100) is None
    assert not state.set_batch_attempt_status(
        7, 0, 2, 'owner-a', 'COMPLETED', now=101)
    assert state.renew_batch_lease(7,
                                   0,
                                   1,
                                   'owner-a',
                                   lease_duration=10,
                                   now=105)

    assert state.set_batch_attempt_status(7,
                                          0,
                                          1,
                                          'owner-a',
                                          'PENDING',
                                          retry_count=1,
                                          next_retry_at=120,
                                          now=106)
    assert state.claim_batch(7,
                             0,
                             'owner-a',
                             'worker-b',
                             lease_duration=10,
                             now=119) is None
    claim_2 = state.claim_batch(7,
                                0,
                                'owner-a',
                                'worker-b',
                                lease_duration=10,
                                now=120)
    assert claim_2 == (2, 1)
    assert not state.set_batch_attempt_status(
        7, 0, 1, 'owner-a', 'COMPLETED', now=121)
    assert state.set_batch_attempt_status(7,
                                          0,
                                          2,
                                          'owner-a',
                                          'COMPLETED',
                                          now=121)

    record = state.get_batch_states(7)[0]
    assert record['status'] == 'COMPLETED'
    assert record['attempt_id'] == 2
    assert record['attempt_owner_token'] == 'owner-a'
    assert record['retry_count'] == 1
    assert record['lease_expires_at'] is None
    assert record['next_retry_at'] is None


def test_expired_batch_attempt_is_reclaimed_once(batch_state_db):
    del batch_state_db
    _create_batch_job(8, 'owner-a')
    assert state.save_batch_states(8, [[0, 4]], 'owner-a')
    assert state.claim_batch(8,
                             0,
                             'owner-a',
                             'worker-a',
                             lease_duration=10,
                             now=100) == (1, 0)

    assert not state.requeue_expired_batch_attempts(8, 'owner-a', now=109)
    assert state.requeue_expired_batch_attempts(8, 'owner-a', now=110) == [0]
    assert not state.requeue_expired_batch_attempts(8, 'owner-a', now=110)
    assert state.claim_batch(8,
                             0,
                             'owner-a',
                             'worker-b',
                             lease_duration=10,
                             now=110) == (2, 0)


def test_batch_returning_fallback_preserves_claim_and_requeue(
        batch_state_db, monkeypatch):
    del batch_state_db
    monkeypatch.setattr(batch_state_lib, '_supports_update_returning',
                        lambda engine: False)
    _create_batch_job(80, 'owner-a')
    assert state.save_batch_states(80, [[0, 4]], 'owner-a')
    assert state.claim_batch(80,
                             0,
                             'owner-a',
                             'worker-a',
                             lease_duration=10,
                             now=100) == (1, 0)
    assert state.requeue_expired_batch_attempts(80, 'owner-a', now=110) == [0]
    assert state.claim_batch(80,
                             0,
                             'owner-a',
                             'worker-b',
                             lease_duration=10,
                             now=110) == (2, 0)


def test_claim_batch_uses_returning_when_supported(batch_state_db):
    del batch_state_db
    engine = state._db_manager.get_engine()
    if not state._supports_update_returning(engine):
        pytest.skip('dialect does not support UPDATE RETURNING')
    _create_batch_job(81, 'owner-a')
    assert state.save_batch_states(81, [[0, 4]], 'owner-a')

    with _count_sql_statements(engine) as counts:
        claim = state.claim_batch(81,
                                  0,
                                  'owner-a',
                                  'worker-a',
                                  lease_duration=10,
                                  now=100)

    assert claim == (1, 0)
    assert counts['n'] <= 3, counts


def test_requeue_expired_batches_uses_single_returning_update(batch_state_db):
    del batch_state_db
    engine = state._db_manager.get_engine()
    if not state._supports_update_returning(engine):
        pytest.skip('dialect does not support UPDATE RETURNING')
    _create_batch_job(82, 'owner-a')
    assert state.save_batch_states(82,
                                   [[i * 10, i * 10 + 9] for i in range(50)],
                                   'owner-a')
    for i in range(50):
        assert state.claim_batch(82,
                                 i,
                                 'owner-a',
                                 f'worker-{i}',
                                 lease_duration=10,
                                 now=100) == (1, 0)

    with _count_sql_statements(engine) as counts:
        reclaimed = state.requeue_expired_batch_attempts(82, 'owner-a', now=111)

    assert reclaimed == list(range(50))
    assert counts['n'] <= 3, counts


def test_coordinator_takeover_fences_all_old_owner_mutations(batch_state_db):
    del batch_state_db
    _create_batch_job(9, 'old-owner')
    assert state.save_batch_states(9, [[0, 4], [5, 9]], 'old-owner')
    assert state.claim_batch(9, 0, 'old-owner', 'worker-a', 10,
                             now=100) == (1, 0)

    assert state.acquire_batch_coordinator(9, 'new-owner') == 'old-owner'
    assert not state.is_batch_coordinator_owner(9, 'old-owner')
    assert state.is_batch_coordinator_owner(9, 'new-owner')

    # Takeover is a job-wide fence: the old owner cannot claim untouched work
    # or mutate the attempt it claimed before takeover.
    assert state.claim_batch(9, 1, 'old-owner', 'worker-b', 10, now=101) is None
    assert not state.renew_batch_lease(9, 0, 1, 'old-owner', 10, now=101)
    assert not state.set_batch_attempt_status(
        9, 0, 1, 'old-owner', 'COMPLETED', now=101)
    assert not state.requeue_expired_batch_attempts(9, 'old-owner', now=110)

    # The new owner honors the old lease, then reclaims and owns the next
    # attempt with a new attempt token.
    assert not state.requeue_expired_batch_attempts(9, 'new-owner', now=109)
    assert state.requeue_expired_batch_attempts(9, 'new-owner', now=110) == [0]
    assert state.claim_batch(9, 0, 'new-owner', 'worker-b', 10,
                             now=110) == (2, 0)


def test_new_launch_waits_for_paused_old_owner_transaction(
        batch_state_db, monkeypatch):
    del batch_state_db
    _create_batch_job(10, 'old-owner')
    assert state.save_batch_states(10, [[0, 4]], 'old-owner')
    owner_locked = threading.Event()
    release_old = threading.Event()
    takeover_started = threading.Event()
    takeover_done = threading.Event()
    new_launch = mock.Mock()
    errors = []
    original_lock = batch_state_lib._lock_batch_coordinator_owner

    def _pause_old_owner(session, job_id, owner_token):
        owned = original_lock(session, job_id, owner_token)
        if threading.current_thread().name == 'old-owner-claim':
            owner_locked.set()
            if not release_old.wait(timeout=5):
                raise RuntimeError('test timed out releasing old owner')
        return owned

    monkeypatch.setattr(batch_state_lib, '_lock_batch_coordinator_owner',
                        _pause_old_owner)

    def _old_claim():
        try:
            assert state.claim_batch(10,
                                     0,
                                     'old-owner',
                                     'worker-a',
                                     10,
                                     now=100) == (1, 0)
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)

    def _new_takeover():
        try:
            takeover_started.set()
            assert state.acquire_batch_coordinator(10,
                                                   'new-owner') == 'old-owner'
            new_launch()
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            takeover_done.set()

    old_thread = threading.Thread(target=_old_claim, name='old-owner-claim')
    old_thread.start()
    assert owner_locked.wait(timeout=5)
    takeover_thread = threading.Thread(target=_new_takeover)
    takeover_thread.start()

    assert takeover_started.wait(timeout=5)
    assert not takeover_done.wait(timeout=0.2)
    new_launch.assert_not_called()
    release_old.set()
    old_thread.join(timeout=5)
    takeover_thread.join(timeout=5)

    assert not old_thread.is_alive()
    assert not takeover_thread.is_alive()
    assert not errors
    new_launch.assert_called_once_with()
    assert state.is_batch_coordinator_owner(10, 'new-owner')


def test_triple_takeover_retains_only_durable_stale_generations(batch_state_db):
    del batch_state_db
    _create_batch_job(11, 'owner-a')
    assert state.save_batch_states(11, [[0, 4]], 'owner-a')
    assert state.claim_batch(11, 0, 'owner-a', 'worker-a', 10,
                             now=100) == (1, 0)
    assert state.acquire_batch_coordinator(11, 'owner-b') == 'owner-a'
    assert state.acquire_batch_coordinator(11, 'owner-c') == 'owner-b'

    batch_coordinator = _make_coordinator(job_id=11)
    batch_coordinator._worker_token = 'owner-c'
    batch_coordinator._stale_worker_tokens.add('owner-b')
    batch_coordinator._resume_from_db()

    assert batch_coordinator._stale_worker_tokens == {'owner-a', 'owner-b'}
    assert not state.renew_batch_lease(11, 0, 1, 'owner-a', 10, now=101)
    assert state.requeue_expired_batch_attempts(11, 'owner-c', now=110) == [0]


def test_batch_lifecycle_transitions_are_owner_fenced(batch_state_db):
    del batch_state_db
    _create_batch_job(12, 'old-owner')
    _create_running_batch_task(12)
    assert state.acquire_batch_coordinator(12, 'new-owner') == 'old-owner'

    assert state.set_batch_winding_down(
        12, 0, 'old-owner') == state.BatchLifecycleTransition.OWNER_LOST
    assert state.set_batch_failed(
        12, 0, 'old-owner',
        'stale failure') == state.BatchLifecycleTransition.OWNER_LOST
    assert state.set_batch_winding_down(
        12, 0, 'new-owner') == state.BatchLifecycleTransition.APPLIED
    assert state.set_batch_succeeded(
        12, 0, 'new-owner',
        end_time=123) == state.BatchLifecycleTransition.APPLIED

    engine = state._db_manager.get_engine()
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(state.spot_table.c.status,
                              state.spot_table.c.failure_reason,
                              state.spot_table.c.end_at).where(
                                  state.spot_table.c.spot_job_id == 12)).one()
    assert row.status == state.ManagedJobStatus.SUCCEEDED.value
    assert row.failure_reason is None
    assert row.end_at == 123


def test_batch_failure_covers_starting_and_reports_invalid_state(
        batch_state_db):
    del batch_state_db
    _create_batch_job(13, 'owner-a')
    engine = state._db_manager.get_engine()
    with engine.begin() as connection:
        connection.execute(state.spot_table.insert().values(
            spot_job_id=13,
            task_id=0,
            status=state.ManagedJobStatus.STARTING.value,
            end_at=None))

    assert state.set_batch_failed(
        13, 0, 'owner-a',
        'startup failed') == state.BatchLifecycleTransition.APPLIED
    assert state.set_batch_failed(
        13, 0, 'owner-a',
        'same result') == state.BatchLifecycleTransition.ALREADY_TARGET
    assert state.set_batch_succeeded(
        13, 0, 'owner-a',
        end_time=123) == state.BatchLifecycleTransition.INVALID_STATE


def test_schema_022_upgrades_existing_batch_state_table(tmp_path):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "old.db"}')
    old_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'batch_state', old_metadata,
        sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('batch_idx', sqlalchemy.Integer, primary_key=True))
    old_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                'INSERT INTO batch_state (job_id, batch_idx) VALUES (1, 0)'))

    schema_022 = importlib.import_module(
        'sky.schemas.db.spot_jobs.022_add_batch_attempt_leases')
    with engine.connect() as connection:
        context = migration.MigrationContext.configure(connection)
        with operations.Operations.context(context):
            schema_022.upgrade()

    columns = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('batch_state')
    }
    assert {'attempt_id', 'lease_expires_at', 'next_retry_at'} <= columns
    with engine.connect() as connection:
        attempt_id = connection.execute(
            sqlalchemy.text('SELECT attempt_id FROM batch_state')).scalar_one()
    assert attempt_id == 0


def test_schema_023_adds_batch_coordinator_ownership_tokens(tmp_path):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "owner.db"}')
    old_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'job_info', old_metadata,
        sqlalchemy.Column('spot_job_id', sqlalchemy.Integer, primary_key=True))
    sqlalchemy.Table(
        'batch_state', old_metadata,
        sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('batch_idx', sqlalchemy.Integer, primary_key=True))
    old_metadata.create_all(engine)

    schema_023 = importlib.import_module(
        'sky.schemas.db.spot_jobs.023_add_batch_coordinator_fence')
    with engine.connect() as connection:
        context = migration.MigrationContext.configure(connection)
        with operations.Operations.context(context):
            schema_023.upgrade()

    inspector = sqlalchemy.inspect(engine)
    job_columns = {
        column['name'] for column in inspector.get_columns('job_info')
    }
    batch_columns = {
        column['name'] for column in inspector.get_columns('batch_state')
    }
    assert 'batch_coordinator_token' in job_columns
    assert 'attempt_owner_token' in batch_columns
    assert inspector.has_table('batch_worker')
    worker_columns = {
        column['name'] for column in inspector.get_columns('batch_worker')
    }
    assert {
        'coordinator_token', 'worker_cluster', 'worker_job_name',
        'launch_request_id', 'worker_job_id'
    } <= worker_columns
    assert inspector.get_pk_constraint(
        'batch_worker')['constrained_columns'] == [
            'job_id', 'coordinator_token', 'worker_cluster'
        ]


def test_schema_024_indexes_shared_api_tokens(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "tokens.db"}')
    old_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'api_access_tokens', old_metadata,
        sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('token_id', sqlalchemy.Text, nullable=False))
    sqlalchemy.Table(
        'alembic_version_spot_jobs_db', old_metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True))
    old_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                'INSERT INTO alembic_version_spot_jobs_db (version_num) '
                "VALUES ('023')"))

    @contextlib.contextmanager
    def unlocked(_section):
        yield

    monkeypatch.setattr(migration_utils, 'db_lock', unlocked)
    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.SPOT_JOBS_DB_NAME,
                                         '024')

    indexes = {
        index['name']: index['column_names']
        for index in sqlalchemy.inspect(engine).get_indexes('api_access_tokens')
    }
    assert indexes['ix_api_access_tokens_token_id'] == ['token_id']


def test_schema_024_builds_postgres_index_concurrently(monkeypatch):
    migration = importlib.import_module(
        'sky.schemas.db.spot_jobs.024_add_api_access_token_index')
    bind = mock.Mock()
    bind.dialect.name = 'postgresql'
    inspector = mock.Mock()
    inspector.get_indexes.return_value = []
    create_index = mock.Mock()
    monkeypatch.setattr(migration.op, 'get_bind', lambda: bind)
    monkeypatch.setattr(migration.sa, 'inspect', lambda _: inspector)
    monkeypatch.setattr(migration.op, 'create_index', create_index)

    @contextlib.contextmanager
    def autocommit_block():
        yield

    context = mock.Mock()
    context.autocommit_block = autocommit_block
    monkeypatch.setattr(migration.op, 'get_context', lambda: context)

    migration.upgrade()

    create_index.assert_called_once_with('ix_api_access_tokens_token_id',
                                         'api_access_tokens', ['token_id'],
                                         postgresql_concurrently=True)


def test_schema_025_indexes_exact_job_task_identity(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "tasks.db"}')
    old_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'spot', old_metadata,
        sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spot_job_id', sqlalchemy.Integer),
        sqlalchemy.Column('task_id', sqlalchemy.Integer))
    sqlalchemy.Table(
        'alembic_version_spot_jobs_db', old_metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True))
    old_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                'INSERT INTO alembic_version_spot_jobs_db (version_num) '
                "VALUES ('024')"))

    @contextlib.contextmanager
    def unlocked(_section):
        yield

    monkeypatch.setattr(migration_utils, 'db_lock', unlocked)
    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.SPOT_JOBS_DB_NAME,
                                         '025')

    indexes = {
        index['name']: index['column_names']
        for index in sqlalchemy.inspect(engine).get_indexes('spot')
    }
    assert indexes['ix_spot_job_task'] == ['spot_job_id', 'task_id']


def test_schema_025_rejects_same_name_with_wrong_columns(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "malformed-tasks.db"}')
    metadata = sqlalchemy.MetaData()
    spot = sqlalchemy.Table(
        'spot', metadata,
        sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spot_job_id', sqlalchemy.Integer),
        sqlalchemy.Column('task_id', sqlalchemy.Integer),
        sqlalchemy.Column('status', sqlalchemy.Text))
    sqlalchemy.Index('ix_spot_job_task', spot.c.status)
    version = sqlalchemy.Table(
        'alembic_version_spot_jobs_db', metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True))
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(version.insert().values(version_num='024'))

    @contextlib.contextmanager
    def unlocked(_section):
        yield

    monkeypatch.setattr(migration_utils, 'db_lock', unlocked)
    with pytest.raises(RuntimeError, match='unexpected shape'):
        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SPOT_JOBS_DB_NAME,
                                             '025')

    with engine.connect() as connection:
        assert connection.execute(sqlalchemy.select(
            version.c.version_num)).scalar_one() == '024'


def test_schema_026_adds_managed_job_controller_ownership(
        tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "controller-owner.db"}')

    @contextlib.contextmanager
    def unlocked(_section):
        yield

    monkeypatch.setattr(migration_utils, 'db_lock', unlocked)
    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.SPOT_JOBS_DB_NAME,
                                         '025')
    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.SPOT_JOBS_DB_NAME,
                                         '026')

    columns = {
        column['name']: column
        for column in sqlalchemy.inspect(engine).get_columns('job_info')
    }
    assert columns['controller_instance_id'][
        'type'].__class__.__name__ == 'TEXT'
    assert (
        columns['controller_generation']['type'].__class__.__name__ == 'BIGINT')
    with engine.connect() as connection:
        revision = connection.execute(
            sqlalchemy.text('SELECT version_num FROM '
                            'alembic_version_spot_jobs_db')).scalar_one()
    assert revision == '026'
    engine.dispose()


def test_schema_027_indexes_waiting_jobs_in_scheduler_order(
        tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "scheduler-index.db"}')

    @contextlib.contextmanager
    def unlocked(_section):
        yield

    monkeypatch.setattr(migration_utils, 'db_lock', unlocked)
    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.SPOT_JOBS_DB_NAME,
                                         '026')
    # Revision 001 creates indexes from current metadata for fresh databases.
    # Remove it to exercise the real 026-to-027 upgrade path.
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'DROP INDEX IF EXISTS ix_job_info_schedule_priority')

    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.SPOT_JOBS_DB_NAME,
                                         '027')

    with engine.connect() as connection:
        summary = next(row for row in connection.exec_driver_sql(
            "PRAGMA index_list('job_info')").mappings()
                       if row['name'] == 'ix_job_info_schedule_priority')
        index_rows = connection.exec_driver_sql(
            "PRAGMA index_xinfo('ix_job_info_schedule_priority')").mappings()
        keys = [row for row in index_rows if bool(row['key'])]
        revision = connection.execute(
            sqlalchemy.text('SELECT version_num FROM '
                            'alembic_version_spot_jobs_db')).scalar_one()

    assert not bool(summary['unique'])
    assert not bool(summary['partial'])
    assert [row['name'] for row in keys
           ] == ['schedule_state', 'priority', 'spot_job_id']
    assert [bool(row['desc']) for row in keys] == [False, True, False]
    assert revision == '027'
    engine.dispose()


def test_schema_027_fresh_bootstrap_matches_migrated_shape(
        tmp_path, monkeypatch):
    migration = importlib.import_module(
        'sky.schemas.db.spot_jobs.027_add_waiting_job_priority_index')
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "fresh-scheduler-index.db"}')

    @contextlib.contextmanager
    def unlocked(_section):
        yield

    monkeypatch.setattr(migration_utils, 'db_lock', unlocked)
    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.SPOT_JOBS_DB_NAME,
                                         '027')

    with engine.connect() as connection:
        index = migration._sqlite_index_state(connection)
        revision = connection.execute(
            sqlalchemy.text('SELECT version_num FROM '
                            'alembic_version_spot_jobs_db')).scalar_one()

    assert index is not None
    assert migration._sqlite_shape_matches(index)
    assert revision == '027'
    engine.dispose()


def test_schema_027_rejects_same_name_with_wrong_order(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "malformed-scheduler-index.db"}')

    @contextlib.contextmanager
    def unlocked(_section):
        yield

    monkeypatch.setattr(migration_utils, 'db_lock', unlocked)
    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.SPOT_JOBS_DB_NAME,
                                         '026')
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'DROP INDEX IF EXISTS ix_job_info_schedule_priority')
        connection.exec_driver_sql(
            'CREATE INDEX ix_job_info_schedule_priority ON job_info '
            '(schedule_state, priority ASC, spot_job_id ASC)')

    with pytest.raises(RuntimeError, match='unexpected shape'):
        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SPOT_JOBS_DB_NAME,
                                             '027')

    with engine.connect() as connection:
        revision = connection.execute(
            sqlalchemy.text('SELECT version_num FROM '
                            'alembic_version_spot_jobs_db')).scalar_one()
    assert revision == '026'
    engine.dispose()


def test_schema_027_builds_postgres_index_concurrently(monkeypatch):
    migration = importlib.import_module(
        'sky.schemas.db.spot_jobs.027_add_waiting_job_priority_index')
    bind = mock.Mock()
    bind.dialect.name = 'postgresql'
    create_index = mock.Mock()
    monkeypatch.setattr(migration, '_postgres_index_state', lambda _: None)
    monkeypatch.setattr(migration.op, 'get_bind', lambda: bind)
    monkeypatch.setattr(migration.op, 'create_index', create_index)

    @contextlib.contextmanager
    def autocommit_block():
        yield

    context = mock.Mock()
    context.autocommit_block = autocommit_block
    monkeypatch.setattr(migration.op, 'get_context', lambda: context)

    migration.upgrade()

    args = create_index.call_args.args
    assert args[:2] == ('ix_job_info_schedule_priority', 'job_info')
    assert [str(column) for column in args[2]
           ] == ['schedule_state', 'priority DESC', 'spot_job_id ASC']
    assert create_index.call_args.kwargs == {'postgresql_concurrently': True}


def test_schema_027_repairs_invalid_postgres_index(monkeypatch):
    migration = importlib.import_module(
        'sky.schemas.db.spot_jobs.027_add_waiting_job_priority_index')
    bind = mock.Mock()
    bind.dialect.name = 'postgresql'
    bind.dialect.identifier_preparer.quote_schema.side_effect = (
        lambda value: f'"{value}"')
    bind.dialect.identifier_preparer.quote.side_effect = (
        lambda value: f'"{value}"')
    create_index = mock.Mock()
    monkeypatch.setattr(
        migration, '_postgres_index_state', lambda _: {
            'table_schema': 'public',
            'table_name': 'job_info',
            'index_schema': 'public',
            'is_valid': False,
            'is_ready': False,
        })
    monkeypatch.setattr(migration.op, 'create_index', create_index)

    migration._ensure_index(bind)

    bind.exec_driver_sql.assert_called_once_with(
        'DROP INDEX CONCURRENTLY IF EXISTS '
        '"public"."ix_job_info_schedule_priority"')
    assert create_index.call_args.kwargs == {'postgresql_concurrently': True}


def test_schema_027_rejects_valid_postgres_index_with_wrong_order(monkeypatch):
    migration = importlib.import_module(
        'sky.schemas.db.spot_jobs.027_add_waiting_job_priority_index')
    bind = mock.Mock()
    bind.dialect.name = 'postgresql'
    index = {
        'table_schema': 'public',
        'table_name': 'job_info',
        'index_schema': 'public',
        'is_valid': True,
        'is_ready': True,
        'is_unique': False,
        'is_primary': False,
        'is_exclusion': False,
        'is_unfiltered': True,
        'is_expression_free': True,
        'access_method': 'btree',
        'key_count': 3,
        'attribute_count': 3,
        'key_columns': ['schedule_state', 'priority', 'spot_job_id'],
        'key_options': [0, 0, 0],
    }
    monkeypatch.setattr(migration, '_postgres_index_state', lambda _: index)
    create_index = mock.Mock()
    monkeypatch.setattr(migration.op, 'create_index', create_index)

    with pytest.raises(RuntimeError, match='unexpected shape'):
        migration._ensure_index(bind)

    bind.exec_driver_sql.assert_not_called()
    create_index.assert_not_called()


def test_spot_jobs_database_targets_latest_migration(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "target.db"}')
    upgrade = mock.Mock()
    monkeypatch.setattr(migration_utils, 'safe_alembic_upgrade', upgrade)

    state.create_table(engine)

    upgrade.assert_called_once_with(engine,
                                    migration_utils.SPOT_JOBS_DB_NAME,
                                    '027',
                                    mode='auto')
    assert migration_utils.SPOT_JOBS_VERSION == '027'
    engine.dispose()


def _make_coordinator(job_id=1):
    return coordinator.BatchCoordinator(dataset_path='s3://bucket/input.jsonl',
                                        output_path='s3://bucket/output.jsonl',
                                        batch_size=4,
                                        pool_name='pool',
                                        serialized_fn='serialized',
                                        input_format_dict={
                                            'format': 'json',
                                            'path': 's3://bucket/input.jsonl',
                                        },
                                        output_formats_dict=[{
                                            'format': 'json',
                                            'path': 's3://bucket/output.jsonl',
                                        }],
                                        job_id=job_id)


def test_inline_coordinator_does_not_replace_process_signal_handler():
    with mock.patch.object(coordinator.signal, 'signal') as install_handler:
        _make_coordinator()
    install_handler.assert_not_called()


def test_custom_writer_without_attempt_hooks_fails_before_dispatch():

    class _LegacyWriter(io_formats.OutputWriter):
        """Writer implementing only the pre-fencing contract."""

        def upload_batch(self, results, start_idx, end_idx, job_id):
            del results, start_idx, end_idx, job_id
            return self.path

        def reduce_results(self, job_id):
            del job_id

        def cleanup(self, job_id):
            del job_id

    writer = _LegacyWriter('s3://bucket/output')
    with pytest.raises(ValueError, match='upload_batch_attempt'):
        writer.validate_attempt_fencing()


def test_coordinator_rejects_pool_with_old_worker_runtime(monkeypatch):
    batch_coordinator = _make_coordinator()
    monkeypatch.setattr(
        batch_coordinator, '_fetch_pool_status', lambda: {
            'replica_info': [{
                'name': 'old-worker',
                'status': 'READY',
                'replica_info_version': 5,
            }]
        })

    with pytest.raises(RuntimeError, match='Recreate pool'):
        batch_coordinator._get_ready_workers()


def test_coordinator_uses_only_workers_with_valid_available_usage(monkeypatch):
    batch_coordinator = _make_coordinator(job_id=7)
    monkeypatch.setattr(
        batch_coordinator, '_fetch_pool_status', lambda: {
            'replica_info': [{
                'name': 'available-worker-int-id',
                'status': 'READY',
                'replica_info_version': 6,
                'used_by': [7],
            }, {
                'name': 'available-worker-string-id',
                'status': 'READY',
                'replica_info_version': 6,
                'used_by': ['7'],
            }, {
                'name': 'unrelated-worker',
                'status': 'READY',
                'replica_info_version': 6,
                'used_by': [42],
            }, {
                'name': 'mixed-use-worker',
                'status': 'READY',
                'replica_info_version': 6,
                'used_by': [7, '42'],
            }]
        })

    assert batch_coordinator._get_ready_workers() == [
        'available-worker-int-id', 'available-worker-string-id'
    ]


@pytest.mark.parametrize('replica_info', [
    pytest.param({}, id='missing-used-by'),
    pytest.param({'used_by': None}, id='null-used-by'),
    pytest.param({'used_by': {
        'job_id': 7
    }}, id='object-used-by'),
    pytest.param({'used_by': '7'}, id='string-used-by'),
    pytest.param({'used_by': 7}, id='own-scalar-used-by'),
])
def test_coordinator_fails_closed_for_invalid_worker_usage(
        monkeypatch, replica_info):
    batch_coordinator = _make_coordinator(job_id=7)
    info = {
        'name': 'worker',
        'status': 'READY',
        'replica_info_version': 6,
    }
    info.update(replica_info)
    monkeypatch.setattr(batch_coordinator, '_fetch_pool_status',
                        lambda: {'replica_info': [info]})

    assert not batch_coordinator._get_ready_workers()


def test_pending_queue_honors_retry_time(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator._enqueue_batch(3, ready_at=120)

    monkeypatch.setattr(coordinator.time, 'time', lambda: 119)
    assert batch_coordinator._pop_ready_batch() == (None, 1)
    monkeypatch.setattr(coordinator.time, 'time', lambda: 120)
    assert batch_coordinator._pop_ready_batch() == (3, 0)


def test_resume_rejects_pre_fence_attempt_state(monkeypatch):
    batch_coordinator = _make_coordinator()
    monkeypatch.setattr(
        coordinator.managed_job_state, 'get_batch_states',
        mock.Mock(return_value=[{
            'batch_idx': 0,
            'start_idx': 0,
            'end_idx': 3,
            'status': 'DISPATCHED',
            'attempt_id': 0,
            'attempt_owner_token': None,
            'worker_cluster': 'worker-a',
            'retry_count': 0,
        }]))

    with pytest.raises(RuntimeError, match='pre-fence attempt state'):
        batch_coordinator._resume_from_db()


def test_dispatch_waits_for_live_lease_and_uses_db_completion(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator.batches = [[0, 3]]
    batch_coordinator._workers = ['worker-a']
    batch_coordinator._enqueue_batch(0)
    monkeypatch.setattr(batch_coordinator, '_reclaim_expired_batches',
                        mock.Mock(return_value=0))
    monkeypatch.setattr(batch_coordinator, '_assert_coordinator_owner',
                        mock.Mock())
    progress = mock.Mock(side_effect=[
        (0, {'worker-a'}, []),
        (1, set(), []),
        (1, set(), []),
    ])
    monkeypatch.setattr(batch_coordinator, '_sync_batch_progress_from_db',
                        progress)
    monkeypatch.setattr(batch_coordinator, '_get_ready_workers',
                        mock.Mock(return_value=['worker-a']))
    dispatch = mock.Mock()
    monkeypatch.setattr(batch_coordinator, '_worker_dispatch_loop', dispatch)
    monkeypatch.setattr(coordinator.time, 'sleep', mock.Mock())

    batch_coordinator._dispatch_all()

    dispatch.assert_not_called()
    assert progress.call_count == 3


def test_dispatch_preserves_worker_discovery_failure(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator.batches = [[0, 3]]
    batch_coordinator._workers = ['worker-a']
    batch_coordinator._enqueue_batch(0)
    monkeypatch.setattr(batch_coordinator, '_reclaim_expired_batches',
                        mock.Mock(return_value=0))
    monkeypatch.setattr(batch_coordinator, '_assert_coordinator_owner',
                        mock.Mock())
    monkeypatch.setattr(batch_coordinator, '_cleanup_stale_worker_services',
                        mock.Mock())
    monkeypatch.setattr(batch_coordinator, '_sync_batch_progress_from_db',
                        mock.Mock(return_value=(0, set(), [])))
    monkeypatch.setattr(
        batch_coordinator, '_get_ready_workers',
        mock.Mock(side_effect=RuntimeError('Recreate pool before retrying')))
    dispatch = mock.Mock()
    monkeypatch.setattr(batch_coordinator, '_worker_dispatch_loop', dispatch)
    monkeypatch.setattr(coordinator.time, 'sleep', mock.Mock())

    with pytest.raises(RuntimeError, match='Recreate pool before retrying'):
        batch_coordinator._dispatch_all()

    dispatch.assert_not_called()


def test_worker_commands_are_scoped_to_coordinator_token():
    batch_coordinator = _make_coordinator()
    batch_coordinator.batches = [[0, 3]]

    notify_code = batch_coordinator._generate_notify_code(0, attempt_id=4)
    shutdown_code = batch_coordinator._generate_shutdown_code()
    token_header = ('X-Sky-Batch-Worker-Token: '
                    f'{batch_coordinator._worker_token}')
    assert token_header in notify_code
    assert token_header in shutdown_code
    assert '"attempt_id": 4' in notify_code
    assert '/health' in shutdown_code
    assert '--connect-timeout 2' in shutdown_code
    assert '--max-time 5' in shutdown_code


def test_completed_resume_cleans_workers_before_reduction(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator._is_resume = True
    events = []
    monkeypatch.setattr(batch_coordinator, '_resolve_formats', mock.Mock())
    monkeypatch.setattr(coordinator.managed_job_state,
                        'acquire_batch_coordinator',
                        mock.Mock(return_value='old-token'))

    def _resume():
        batch_coordinator.batches = [[0, 3]]
        return 1

    monkeypatch.setattr(batch_coordinator, '_resume_from_db', _resume)
    monkeypatch.setattr(batch_coordinator, '_cleanup_stale_worker_services',
                        lambda: events.append('cleanup'))
    monkeypatch.setattr(batch_coordinator, '_set_winding_down',
                        lambda: events.append('winding_down'))
    monkeypatch.setattr(batch_coordinator, '_reduce_results',
                        lambda: events.append('reduce'))

    batch_coordinator.run()

    assert events == ['cleanup', 'winding_down', 'reduce']


def test_worker_rejects_control_from_stale_coordinator(monkeypatch):
    monkeypatch.setattr(worker, '_worker_token', 'current-token')
    handler = object.__new__(worker._WorkerHandler)
    handler.headers = {'X-Sky-Batch-Worker-Token': 'stale-token'}
    handler._send_json = mock.Mock()

    assert not handler._is_authorized()
    handler._send_json.assert_called_once_with(
        409, {'error': 'stale batch coordinator'})


def test_worker_shutdown_cancels_only_owned_job(monkeypatch):
    batch_coordinator = _make_coordinator()
    monkeypatch.setattr(coordinator.sdk, 'exec', mock.Mock(return_value='exec'))
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock())
    cancel = mock.Mock(return_value='cancel')
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    sleep = mock.Mock()
    monkeypatch.setattr(coordinator.time, 'sleep', sleep)
    remove = mock.Mock(return_value=True)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove)

    batch_coordinator._shutdown_worker('worker-a', worker_job_id=17)

    cancel.assert_called_once_with('worker-a', job_ids=[17])
    remove.assert_called_once_with(1,
                                   batch_coordinator._worker_token,
                                   'worker-a',
                                   worker_job_id=17)
    sleep.assert_not_called()


def test_worker_shutdown_code_waits_for_health_to_disappear(
        tmp_path, monkeypatch):
    fake_bin = tmp_path / 'fake-bin'
    fake_bin.mkdir()
    health_calls = tmp_path / 'health-calls'
    fake_curl = fake_bin / 'curl'
    fake_curl.write_text(f"""#!/bin/bash
set -e
case "$*" in
  *"/shutdown"*)
    exit 0
    ;;
  *"/health"*)
    count=0
    if [ -f "{health_calls}" ]; then
      count=$(cat "{health_calls}")
    fi
    count=$((count + 1))
    printf '%s' "$count" > "{health_calls}"
    if [ "$count" -lt 3 ]; then
      printf '200'
    else
      printf '000'
    fi
    exit 0
    ;;
esac
echo "unexpected curl invocation: $*" >&2
exit 1
""",
                         encoding='utf-8')
    fake_curl.chmod(0o755)
    fake_sleep = fake_bin / 'sleep'
    fake_sleep.write_text('#!/bin/bash\nexit 0\n', encoding='utf-8')
    fake_sleep.chmod(0o755)
    monkeypatch.setenv('PATH', f'{fake_bin}:{os.environ["PATH"]}')
    batch_coordinator = _make_coordinator()

    proc = subprocess.run(
        ['/bin/bash', '-c',
         batch_coordinator._generate_shutdown_code()],
        check=False,
        capture_output=True,
        text=True)

    assert proc.returncode == 0
    assert health_calls.read_text(encoding='utf-8') == '3'


def test_worker_shutdown_code_has_bounded_health_wait(tmp_path, monkeypatch):
    fake_bin = tmp_path / 'fake-bin'
    fake_bin.mkdir()
    health_calls = tmp_path / 'health-calls'
    fake_curl = fake_bin / 'curl'
    fake_curl.write_text(f"""#!/bin/bash
set -e
case "$*" in
  *"/shutdown"*)
    exit 0
    ;;
  *"/health"*)
    count=0
    if [ -f "{health_calls}" ]; then
      count=$(cat "{health_calls}")
    fi
    count=$((count + 1))
    printf '%s' "$count" > "{health_calls}"
    printf '200'
    exit 0
    ;;
esac
echo "unexpected curl invocation: $*" >&2
exit 1
""",
                         encoding='utf-8')
    fake_curl.chmod(0o755)
    fake_sleep = fake_bin / 'sleep'
    fake_sleep.write_text('#!/bin/bash\nexit 0\n', encoding='utf-8')
    fake_sleep.chmod(0o755)
    monkeypatch.setenv('PATH', f'{fake_bin}:{os.environ["PATH"]}')
    batch_coordinator = _make_coordinator()

    proc = subprocess.run(
        ['/bin/bash', '-c',
         batch_coordinator._generate_shutdown_code()],
        check=False,
        capture_output=True,
        text=True)

    expected_polls = int(
        coordinator.constants.WORKER_SHUTDOWN_HEALTH_WAIT_SECONDS /
        coordinator.constants.WORKER_SHUTDOWN_POLL_INTERVAL_SECONDS)
    assert proc.returncode == 0
    assert int(health_calls.read_text(encoding='utf-8')) == expected_polls


def test_worker_shutdown_code_skips_health_wait_when_worker_already_gone(
        tmp_path, monkeypatch):
    fake_bin = tmp_path / 'fake-bin'
    fake_bin.mkdir()
    health_calls = tmp_path / 'health-calls'
    fake_curl = fake_bin / 'curl'
    fake_curl.write_text(f"""#!/bin/bash
set -e
case "$*" in
  *"/shutdown"*)
    exit 7
    ;;
  *"/health"*)
    printf 'unexpected' > "{health_calls}"
    exit 1
    ;;
esac
echo "unexpected curl invocation: $*" >&2
exit 1
""",
                         encoding='utf-8')
    fake_curl.chmod(0o755)
    fake_sleep = fake_bin / 'sleep'
    fake_sleep.write_text('#!/bin/bash\nexit 0\n', encoding='utf-8')
    fake_sleep.chmod(0o755)
    monkeypatch.setenv('PATH', f'{fake_bin}:{os.environ["PATH"]}')
    batch_coordinator = _make_coordinator()

    proc = subprocess.run(
        ['/bin/bash', '-c',
         batch_coordinator._generate_shutdown_code()],
        check=False,
        capture_output=True,
        text=True)

    assert proc.returncode == 0
    assert not health_calls.exists()


def test_cancel_claims_worker_cleanup_once(monkeypatch):
    batch_coordinator = _make_coordinator()
    entered = threading.Event()
    release = threading.Event()
    monkeypatch.setattr(batch_coordinator, '_launch_worker_service',
                        mock.Mock(return_value=17))

    def _pop_ready_batch():
        entered.set()
        release.wait(timeout=5)
        return None, None

    monkeypatch.setattr(batch_coordinator, '_pop_ready_batch', _pop_ready_batch)
    shutdown = mock.Mock()
    monkeypatch.setattr(batch_coordinator, '_shutdown_worker', shutdown)
    dispatch = threading.Thread(target=batch_coordinator._worker_dispatch_loop,
                                args=('worker-a',))
    dispatch.start()
    assert entered.wait(timeout=5)

    batch_coordinator.cancel()
    batch_coordinator.cancel()
    release.set()
    dispatch.join(timeout=5)

    assert not dispatch.is_alive()
    shutdown.assert_called_once_with('worker-a', 17)
    assert not batch_coordinator._active_workers


def test_cancel_fans_out_worker_shutdowns(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator._active_workers.update({
        'worker-a': 17,
        'worker-b': 18,
    })
    events = []

    def _shutdown_worker(cluster_name, worker_job_id=None):
        events.append(f'shutdown:{cluster_name}:{worker_job_id}')

    monkeypatch.setattr(batch_coordinator, '_shutdown_worker', _shutdown_worker)

    class _ContextStub:
        """Runs the provided target inline for deterministic testing."""

        def run(self, fn, *args):
            fn(*args)

    class _FakeThread:
        """Records start/join ordering before running the target on join."""

        def __init__(self, target, args=(), kwargs=None):
            self._target = target
            self._args = args
            self._kwargs = kwargs or {}

        def start(self):
            events.append(f'start:{self._args[1]}:{self._args[2]}')

        def join(self):
            events.append(f'join:{self._args[1]}:{self._args[2]}')
            self._target(*self._args, **self._kwargs)

    def _copy_context():
        return _ContextStub()

    monkeypatch.setattr(coordinator.contextvars, 'copy_context', _copy_context)
    monkeypatch.setattr(coordinator.threading, 'Thread', _FakeThread)

    batch_coordinator.cancel()

    assert events == [
        'start:worker-a:17',
        'start:worker-b:18',
        'join:worker-a:17',
        'shutdown:worker-a:17',
        'join:worker-b:18',
        'shutdown:worker-b:18',
    ]
    assert not batch_coordinator._active_workers


def test_cancel_contains_worker_shutdown_failure(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator._active_workers.update({
        'worker-a': 17,
        'worker-b': 18,
    })
    second_started = threading.Event()

    def _shutdown_worker(cluster_name, worker_job_id=None):
        if cluster_name == 'worker-a':
            assert worker_job_id == 17
            raise RuntimeError('shutdown unavailable')
        if cluster_name == 'worker-b':
            assert worker_job_id == 18
            second_started.set()
            return
        raise AssertionError(f'unexpected worker {cluster_name!r}')

    monkeypatch.setattr(batch_coordinator, '_shutdown_worker', _shutdown_worker)
    with mock.patch.object(coordinator.logger, 'warning') as log_warning:
        batch_coordinator.cancel()

    assert second_started.is_set()
    log_warning.assert_called_once_with('Failed to shutdown worker on worker-a')
    assert not batch_coordinator._active_workers


def test_cancel_retries_unresolved_worker_cleanup_once(monkeypatch):
    batch_coordinator = _make_coordinator()
    entered = threading.Event()
    release = threading.Event()
    monkeypatch.setattr(batch_coordinator, '_launch_worker_service',
                        mock.Mock(return_value=17))

    def _pop_ready_batch():
        entered.set()
        release.wait(timeout=5)
        return None, None

    monkeypatch.setattr(batch_coordinator, '_pop_ready_batch', _pop_ready_batch)
    shutdown_request = mock.Mock(
        side_effect=RuntimeError('transient exec failure'))
    cancel_request = mock.Mock(
        side_effect=[RuntimeError('transient cancel failure'), 'cancel'])
    remove_record = mock.Mock(return_value=True)
    monkeypatch.setattr(coordinator.sdk, 'exec', shutdown_request)
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock())
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel_request)
    monkeypatch.setattr(coordinator.time, 'sleep', mock.Mock())
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove_record)
    dispatch = threading.Thread(target=batch_coordinator._worker_dispatch_loop,
                                args=('worker-a',))
    dispatch.start()
    assert entered.wait(timeout=5)

    batch_coordinator.cancel()
    release.set()
    dispatch.join(timeout=5)

    assert not dispatch.is_alive()
    shutdown_request.assert_called_once()
    assert cancel_request.call_count == 2
    remove_record.assert_called_once_with(1,
                                          batch_coordinator._worker_token,
                                          'worker-a',
                                          worker_job_id=17)


def test_worker_cancel_reuses_request_after_transient_wait_failure(monkeypatch):
    batch_coordinator = _make_coordinator()
    cancel_request = mock.Mock(return_value='cancel')
    wait = mock.Mock(side_effect=[RuntimeError('transient wait failure'), None])
    remove_record = mock.Mock(return_value=True)
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel_request)
    monkeypatch.setattr(coordinator.sdk, 'get', wait)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove_record)

    batch_coordinator._cancel_worker_job_by_id('worker-a', 17, 'token')

    cancel_request.assert_called_once_with('worker-a', job_ids=[17])
    assert wait.call_args_list == [mock.call('cancel'), mock.call('cancel')]
    remove_record.assert_called_once_with(1,
                                          'token',
                                          'worker-a',
                                          worker_job_id=17)


def test_worker_finalization_claims_cleanup_before_late_cancel(monkeypatch):
    batch_coordinator = _make_coordinator()
    monkeypatch.setattr(batch_coordinator, '_launch_worker_service',
                        mock.Mock(return_value=17))
    monkeypatch.setattr(batch_coordinator, '_pop_ready_batch',
                        mock.Mock(return_value=(None, None)))
    shutdown = mock.Mock()
    monkeypatch.setattr(batch_coordinator, '_shutdown_worker', shutdown)

    batch_coordinator._worker_dispatch_loop('worker-a')
    batch_coordinator.cancel()

    shutdown.assert_called_once_with('worker-a', worker_job_id=17)
    assert not batch_coordinator._active_workers
    assert not batch_coordinator._launching_workers
    assert not batch_coordinator._cleaning_workers


def test_worker_launched_after_cancel_cleans_itself(monkeypatch):
    batch_coordinator = _make_coordinator()
    entered = threading.Event()
    release = threading.Event()

    def _launch_worker_service(cluster_name):
        assert cluster_name == 'worker-a'
        entered.set()
        release.wait(timeout=5)
        return 17

    monkeypatch.setattr(batch_coordinator, '_launch_worker_service',
                        _launch_worker_service)
    shutdown = mock.Mock()
    monkeypatch.setattr(batch_coordinator, '_shutdown_worker', shutdown)
    dispatch = threading.Thread(target=batch_coordinator._worker_dispatch_loop,
                                args=('worker-a',))
    dispatch.start()
    assert entered.wait(timeout=5)

    batch_coordinator.cancel()
    shutdown.assert_not_called()
    release.set()
    dispatch.join(timeout=5)

    assert not dispatch.is_alive()
    shutdown.assert_called_once_with('worker-a', worker_job_id=17)
    assert not batch_coordinator._cleaning_workers
    assert not batch_coordinator._active_workers


@pytest.mark.asyncio
async def test_superseded_cleanup_waits_for_late_launched_worker(monkeypatch):
    batch_coordinator = _make_coordinator()
    entered = threading.Event()
    release_launch = threading.Event()
    shutdown_entered = threading.Event()
    release_shutdown = threading.Event()

    def _launch_worker_service(cluster_name):
        assert cluster_name == 'worker-a'
        entered.set()
        release_launch.wait(timeout=5)
        return 17

    def _shutdown_worker(cluster_name, worker_job_id=None):
        assert cluster_name == 'worker-a'
        assert worker_job_id == 17
        shutdown_entered.set()
        release_shutdown.wait(timeout=5)

    monkeypatch.setattr(batch_coordinator, '_launch_worker_service',
                        _launch_worker_service)
    monkeypatch.setattr(batch_coordinator, '_pop_ready_batch',
                        mock.Mock(return_value=(None, None)))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records', mock.Mock(return_value=[]))
    shutdown = mock.Mock(side_effect=_shutdown_worker)
    monkeypatch.setattr(batch_coordinator, '_shutdown_worker', shutdown)

    dispatch = threading.Thread(target=batch_coordinator._worker_dispatch_loop,
                                args=('worker-a',))
    dispatch.start()
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        cleanup = asyncio.create_task(
            batch_coordinator.handle_superseded(timeout=1))
        await asyncio.sleep(0.05)
        assert not cleanup.done()

        release_launch.set()
        assert await asyncio.to_thread(shutdown_entered.wait, 1)
        await asyncio.sleep(0.05)
        assert not cleanup.done()

        release_time = time.monotonic()
        release_shutdown.set()
        await asyncio.wait_for(cleanup, timeout=1)
        assert time.monotonic() - release_time < 0.15
    finally:
        release_launch.set()
        release_shutdown.set()
        dispatch.join(timeout=5)

    assert not dispatch.is_alive()
    shutdown.assert_called_once_with('worker-a', worker_job_id=17)
    assert not batch_coordinator._active_workers
    assert not batch_coordinator._launching_workers
    assert not batch_coordinator._cleaning_workers


@pytest.mark.asyncio
async def test_superseded_cleanup_waits_for_started_worker_finalizer(
        monkeypatch):
    batch_coordinator = _make_coordinator()
    shutdown_entered = threading.Event()
    release_shutdown = threading.Event()

    monkeypatch.setattr(batch_coordinator, '_launch_worker_service',
                        mock.Mock(return_value=17))
    monkeypatch.setattr(batch_coordinator, '_pop_ready_batch',
                        mock.Mock(return_value=(None, None)))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records', mock.Mock(return_value=[]))

    def _shutdown_worker(cluster_name, worker_job_id=None):
        assert cluster_name == 'worker-a'
        assert worker_job_id == 17
        shutdown_entered.set()
        release_shutdown.wait(timeout=5)

    shutdown = mock.Mock(side_effect=_shutdown_worker)
    monkeypatch.setattr(batch_coordinator, '_shutdown_worker', shutdown)
    dispatch = threading.Thread(target=batch_coordinator._worker_dispatch_loop,
                                args=('worker-a',))
    dispatch.start()
    try:
        assert await asyncio.to_thread(shutdown_entered.wait, 1)
        cleanup = asyncio.create_task(
            batch_coordinator.handle_superseded(timeout=1))
        await asyncio.sleep(0.05)
        assert not cleanup.done()

        release_shutdown.set()
        await asyncio.wait_for(cleanup, timeout=1)
    finally:
        release_shutdown.set()
        dispatch.join(timeout=5)

    assert not dispatch.is_alive()
    shutdown.assert_called_once_with('worker-a', worker_job_id=17)
    assert not batch_coordinator._cleaning_workers


@pytest.mark.asyncio
async def test_superseded_cleanup_waits_for_failed_late_launch(monkeypatch):
    batch_coordinator = _make_coordinator()
    entered = threading.Event()
    release = threading.Event()
    errors = []

    def _launch_worker_service(cluster_name):
        assert cluster_name == 'worker-a'
        entered.set()
        release.wait(timeout=5)
        raise RuntimeError('launch failed')

    monkeypatch.setattr(batch_coordinator, '_launch_worker_service',
                        _launch_worker_service)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records', mock.Mock(return_value=[]))
    shutdown = mock.Mock()
    monkeypatch.setattr(batch_coordinator, '_shutdown_worker', shutdown)

    def _dispatch():
        try:
            batch_coordinator._worker_dispatch_loop('worker-a')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)

    dispatch = threading.Thread(target=_dispatch)
    dispatch.start()
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        cleanup = asyncio.create_task(
            batch_coordinator.handle_superseded(timeout=1))
        await asyncio.sleep(0.05)
        assert not cleanup.done()
        release.set()
        await asyncio.wait_for(cleanup, timeout=1)
    finally:
        release.set()
        dispatch.join(timeout=5)

    assert not dispatch.is_alive()
    assert len(errors) == 1
    assert str(errors[0]) == 'launch failed'
    shutdown.assert_not_called()
    assert not batch_coordinator._active_workers
    assert not batch_coordinator._launching_workers
    assert not batch_coordinator._cleaning_workers


def test_superseded_cleanup_claim_blocks_worker_finalizer(monkeypatch):
    batch_coordinator = _make_coordinator()
    entered = threading.Event()
    release = threading.Event()
    monkeypatch.setattr(batch_coordinator, '_launch_worker_service',
                        mock.Mock(return_value=17))

    def _pop_ready_batch():
        entered.set()
        release.wait(timeout=5)
        return None, None

    monkeypatch.setattr(batch_coordinator, '_pop_ready_batch', _pop_ready_batch)
    shutdown = mock.Mock()
    monkeypatch.setattr(batch_coordinator, '_shutdown_worker', shutdown)
    dispatch = threading.Thread(target=batch_coordinator._worker_dispatch_loop,
                                args=('worker-a',))
    dispatch.start()
    assert entered.wait(timeout=5)

    claimed = batch_coordinator._begin_cleanup(superseded=True)
    release.set()
    dispatch.join(timeout=5)

    assert claimed == [('worker-a', 17)]
    assert not dispatch.is_alive()
    shutdown.assert_not_called()
    assert not batch_coordinator._active_workers


def test_durable_worker_id_ignores_duplicate_or_spoofed_names(
        batch_state_db, monkeypatch):
    del batch_state_db
    _create_batch_job(1, 'old-token')
    assert state.register_batch_worker_launch(1, 'old-token', 'worker-a',
                                              'batch-worker-1-old-token')
    assert state.record_batch_worker_job_id(1, 'old-token', 'worker-a', 17)
    assert state.acquire_batch_coordinator(1, 'new-token') == 'old-token'
    batch_coordinator = _make_coordinator(job_id=1)
    batch_coordinator._worker_token = 'new-token'
    batch_coordinator._stale_worker_tokens.add('old-token')
    batch_coordinator._stale_attempt_leases_drained = True
    batch_coordinator._resolve_formats()
    queue = mock.Mock()
    monkeypatch.setattr(coordinator.sdk, 'queue', queue)
    get = mock.Mock(return_value=None)
    monkeypatch.setattr(coordinator.sdk, 'get', get)
    cancel = mock.Mock(return_value='cancel-request')
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)

    batch_coordinator._cancel_stale_worker_jobs('worker-a', 'old-token')

    cancel.assert_called_once_with('worker-a', job_ids=[17])
    queue.assert_not_called()
    with pytest.raises(ValueError, match='durably captured'):
        batch_coordinator._cancel_stale_worker_jobs('worker-a', 'spoof-token')
    startup_code = batch_coordinator._generate_worker_startup_code()
    assert '/proc/net/tcp' not in startup_code
    assert 'os.kill' not in startup_code


def test_unresolved_duplicate_worker_names_are_never_bulk_cancelled(
        batch_state_db, monkeypatch):
    del batch_state_db
    _create_batch_job(2, 'old-token')
    assert state.register_batch_worker_launch(2, 'old-token', 'worker-a',
                                              'batch-worker-2-old-token')
    assert state.acquire_batch_coordinator(2, 'new-token') == 'old-token'
    batch_coordinator = _make_coordinator(job_id=2)
    batch_coordinator._worker_token = 'new-token'
    batch_coordinator._stale_worker_tokens.add('old-token')
    batch_coordinator._stale_attempt_leases_drained = True
    queued = [
        types.SimpleNamespace(job_id=17, job_name='batch-worker-2-old-token'),
        types.SimpleNamespace(job_id=18, job_name='batch-worker-2-old-token'),
    ]
    monkeypatch.setattr(coordinator.sdk, 'queue',
                        mock.Mock(return_value='queue-request'))
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(return_value=queued))
    cancel = mock.Mock()
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)

    batch_coordinator._cancel_stale_worker_jobs('worker-a', 'old-token')

    cancel.assert_not_called()
    assert state.get_batch_worker_records(2)[0]['worker_job_id'] is None


def test_stale_cleanup_reuses_one_queue_snapshot_per_cluster(
        batch_state_db, monkeypatch):
    del batch_state_db
    _create_batch_job(3, 'owner-a')
    assert state.register_batch_worker_launch(3, 'owner-a', 'worker-a',
                                              'batch-worker-3-owner-a')
    assert state.register_batch_worker_launch(3, 'owner-a', 'worker-b',
                                              'batch-worker-3-owner-a')
    assert state.acquire_batch_coordinator(3, 'owner-b') == 'owner-a'
    assert state.register_batch_worker_launch(3, 'owner-b', 'worker-a',
                                              'batch-worker-3-owner-b')
    assert state.acquire_batch_coordinator(3, 'owner-c') == 'owner-b'

    batch_coordinator = _make_coordinator(job_id=3)
    batch_coordinator._worker_token = 'owner-c'
    batch_coordinator._stale_attempt_leases_drained = True

    def _queue(cluster_name, **_):
        return f'queue-{cluster_name}'

    queue = mock.Mock(side_effect=_queue)
    queued = {
        'queue-worker-a': [
            types.SimpleNamespace(job_id=17, job_name='batch-worker-3-owner-a'),
            types.SimpleNamespace(job_id=18, job_name='batch-worker-3-owner-b'),
        ],
        'queue-worker-b': [
            types.SimpleNamespace(job_id=19, job_name='batch-worker-3-owner-a'),
        ],
    }
    get = mock.Mock(side_effect=queued.get)
    cancel = mock.Mock(side_effect=[
        'cancel-owner-a-worker-a',
        'cancel-owner-a-worker-b',
        'cancel-owner-b-worker-a',
    ])
    monkeypatch.setattr(coordinator.sdk, 'queue', queue)
    monkeypatch.setattr(coordinator.sdk, 'get', get)
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)

    batch_coordinator._cleanup_stale_worker_services(strict=True)

    assert queue.call_args_list == [
        mock.call('worker-a', skip_finished=True),
        mock.call('worker-b', skip_finished=True),
    ]
    assert get.call_args_list == [
        mock.call('queue-worker-a'),
        mock.call('cancel-owner-a-worker-a'),
        mock.call('queue-worker-b'),
        mock.call('cancel-owner-a-worker-b'),
        mock.call('cancel-owner-b-worker-a'),
    ]
    assert cancel.call_args_list == [
        mock.call('worker-a', job_ids=[17]),
        mock.call('worker-b', job_ids=[19]),
        mock.call('worker-a', job_ids=[18]),
    ]
    assert state.get_batch_worker_records(3) == []


def test_stale_cleanup_reads_worker_records_once_per_pass(
        batch_state_db, monkeypatch):
    del batch_state_db
    _create_batch_job(5, 'owner-a')
    assert state.register_batch_worker_launch(5, 'owner-a', 'worker-a',
                                              'batch-worker-5-owner-a')
    assert state.register_batch_worker_launch(5, 'owner-a', 'worker-b',
                                              'batch-worker-5-owner-a')
    assert state.acquire_batch_coordinator(5, 'owner-b') == 'owner-a'
    assert state.register_batch_worker_launch(5, 'owner-b', 'worker-a',
                                              'batch-worker-5-owner-b')
    assert state.acquire_batch_coordinator(5, 'owner-c') == 'owner-b'

    batch_coordinator = _make_coordinator(job_id=5)
    batch_coordinator._worker_token = 'owner-c'
    batch_coordinator._stale_attempt_leases_drained = True

    original_get_batch_worker_records = state.get_batch_worker_records

    def _get_batch_worker_records(job_id):
        return original_get_batch_worker_records(job_id)

    get_batch_worker_records = mock.Mock(side_effect=_get_batch_worker_records)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records', get_batch_worker_records)

    queue = mock.Mock(
        side_effect=lambda cluster_name, **_: f'queue-{cluster_name}')
    queued = {
        'queue-worker-a': [
            types.SimpleNamespace(job_id=17, job_name='batch-worker-5-owner-a'),
            types.SimpleNamespace(job_id=18, job_name='batch-worker-5-owner-b'),
        ],
        'queue-worker-b': [
            types.SimpleNamespace(job_id=19, job_name='batch-worker-5-owner-a'),
        ],
    }
    get = mock.Mock(side_effect=queued.get)
    cancel = mock.Mock(side_effect=[
        'cancel-owner-a-worker-a',
        'cancel-owner-a-worker-b',
        'cancel-owner-b-worker-a',
    ])
    monkeypatch.setattr(coordinator.sdk, 'queue', queue)
    monkeypatch.setattr(coordinator.sdk, 'get', get)
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)

    batch_coordinator._cleanup_stale_worker_services(strict=True)

    assert get_batch_worker_records.call_count == 1
    assert original_get_batch_worker_records(5) == []


@pytest.mark.parametrize(
    ('invalid_snapshot', 'error_type', 'error_match'),
    [(None, TypeError, 'Queue snapshot for worker-a returned None'),
     ([object()], AttributeError, "object.*has no attribute 'job_name'")])
def test_stale_cleanup_retries_invalid_queue_snapshot(batch_state_db,
                                                      monkeypatch,
                                                      invalid_snapshot,
                                                      error_type, error_match):
    del batch_state_db
    _create_batch_job(4, 'owner-a')
    assert state.register_batch_worker_launch(4, 'owner-a', 'worker-a',
                                              'batch-worker-4-owner-a')
    assert state.acquire_batch_coordinator(4, 'owner-b') == 'owner-a'
    assert state.register_batch_worker_launch(4, 'owner-b', 'worker-a',
                                              'batch-worker-4-owner-b')
    assert state.acquire_batch_coordinator(4, 'owner-c') == 'owner-b'

    batch_coordinator = _make_coordinator(job_id=4)
    batch_coordinator._worker_token = 'owner-c'
    batch_coordinator._stale_attempt_leases_drained = True
    queue = mock.Mock(
        side_effect=['queue-strict', 'queue-owner-a', 'queue-owner-b'])
    get = mock.Mock(side_effect=[
        invalid_snapshot,
        invalid_snapshot,
        [types.SimpleNamespace(job_id=18, job_name='batch-worker-4-owner-b')],
        None,
    ])
    cancel = mock.Mock(return_value='cancel-owner-b')
    monkeypatch.setattr(coordinator.sdk, 'queue', queue)
    monkeypatch.setattr(coordinator.sdk, 'get', get)
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)

    with pytest.raises(error_type, match=error_match):
        batch_coordinator._cleanup_stale_worker_services(strict=True)
    batch_coordinator._cleanup_stale_worker_services()

    assert queue.call_count == 3
    cancel.assert_called_once_with('worker-a', job_ids=[18])
    records = state.get_batch_worker_records(4)
    assert [(record['coordinator_token'], record['worker_job_id'])
            for record in records] == [('owner-a', None)]


def test_takeover_waits_for_old_lease_before_exact_cleanup(monkeypatch):
    batch_coordinator = _make_coordinator(job_id=1)
    batch_coordinator._worker_token = 'new-token'
    batch_coordinator._workers = ['worker-a']
    events = []
    monkeypatch.setattr(batch_coordinator, '_assert_coordinator_owner',
                        mock.Mock())
    monkeypatch.setattr(batch_coordinator, '_reclaim_expired_batches',
                        mock.Mock(return_value=0))
    get_states = mock.Mock(side_effect=[
        [{
            'status': 'DISPATCHED',
            'attempt_owner_token': 'old-token',
            'lease_expires_at': 105,
        }],
        [{
            'status': 'PENDING',
            'attempt_owner_token': 'old-token',
            'lease_expires_at': None,
        }],
    ])
    monkeypatch.setattr(coordinator.managed_job_state, 'get_batch_states',
                        get_states)
    monkeypatch.setattr(coordinator.time, 'time', lambda: 100)
    monkeypatch.setattr(coordinator.time, 'sleep',
                        lambda seconds: events.append(('sleep', seconds)))
    monkeypatch.setattr(batch_coordinator,
                        '_cleanup_worker_services_for_token',
                        lambda token, workers=None, queue_jobs_by_cluster=None,
                        records=None: events.append(('cancel', workers, token)))

    batch_coordinator._wait_for_stale_attempt_leases()

    assert events == [('sleep', 5.0), ('cancel', None, 'old-token')]


def test_triple_takeover_recovers_worker_launched_before_first_claim(
        batch_state_db, monkeypatch):
    del batch_state_db
    _create_batch_job(15, 'owner-a')
    # A persists and launches a service but dies before claiming any batch.
    assert state.register_batch_worker_launch(15, 'owner-a', 'worker-a',
                                              'batch-worker-15-owner-a')
    assert state.record_batch_worker_launch_request(15, 'owner-a', 'worker-a',
                                                    'request-a')
    assert state.record_batch_worker_job_id(15, 'owner-a', 'worker-a', 17)
    # B takes ownership and dies without launching or claiming.  C must still
    # discover A through durable launch history, not just the predecessor token.
    assert state.acquire_batch_coordinator(15, 'owner-b') == 'owner-a'
    assert state.acquire_batch_coordinator(15, 'owner-c') == 'owner-b'

    batch_coordinator = _make_coordinator(job_id=15)
    batch_coordinator._worker_token = 'owner-c'
    batch_coordinator._stale_attempt_leases_drained = True
    cancel = mock.Mock(return_value='cancel-request')
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(return_value=None))

    batch_coordinator._cleanup_stale_worker_services(strict=True)

    assert batch_coordinator._stale_worker_tokens == {'owner-a'}
    cancel.assert_called_once_with('worker-a', job_ids=[17])
    assert state.get_batch_worker_records(15) == []


def test_worker_launch_intent_commits_before_external_exec(monkeypatch):
    batch_coordinator = _make_coordinator(job_id=1)
    batch_coordinator._worker_token = 'owner-token'
    batch_coordinator._stale_attempt_leases_drained = True
    batch_coordinator._resolve_formats()
    monkeypatch.setattr(batch_coordinator, '_assert_coordinator_owner',
                        mock.Mock())
    monkeypatch.setattr(batch_coordinator, '_get_pool_resources',
                        mock.Mock(return_value=None))
    monkeypatch.setattr(batch_coordinator, '_cleanup_stale_worker_services',
                        mock.Mock())
    execute = mock.Mock()
    monkeypatch.setattr(coordinator.sdk, 'exec', execute)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'register_batch_worker_launch',
                        mock.Mock(return_value=False))

    with pytest.raises(coordinator.SupersededCoordinator,
                       match='before worker launch'):
        batch_coordinator._launch_worker_service('worker-a')

    execute.assert_not_called()


def test_worker_launch_crossing_takeover_cancels_exact_job_id(monkeypatch):
    batch_coordinator = _make_coordinator(job_id=1)
    batch_coordinator._worker_token = 'old-token'
    batch_coordinator._resolve_formats()
    monkeypatch.setattr(batch_coordinator, '_assert_coordinator_owner',
                        mock.Mock())
    monkeypatch.setattr(batch_coordinator, '_get_pool_resources',
                        mock.Mock(return_value=None))
    register = mock.Mock(return_value=True)
    record_request = mock.Mock(return_value=True)
    record_job_id = mock.Mock(return_value=True)
    remove_record = mock.Mock(return_value=True)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'register_batch_worker_launch', register)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_launch_request', record_request)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_job_id', record_job_id)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove_record)
    execute = mock.Mock(return_value='launch-request')
    monkeypatch.setattr(coordinator.sdk, 'exec', execute)
    get = mock.Mock(side_effect=[(17, None), None])
    monkeypatch.setattr(coordinator.sdk, 'get', get)
    cancel = mock.Mock(return_value='cancel-request')
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'is_batch_coordinator_owner',
                        mock.Mock(return_value=False))

    with pytest.raises(RuntimeError, match='lost ownership while launching'):
        batch_coordinator._launch_worker_service('worker-a')

    launched_task = execute.call_args.args[0]
    assert launched_task.name == 'batch-worker-1-old-token'
    register.assert_called_once_with(1, 'old-token', 'worker-a',
                                     'batch-worker-1-old-token')
    record_request.assert_called_once_with(1, 'old-token', 'worker-a',
                                           'launch-request')
    record_job_id.assert_called_once_with(1, 'old-token', 'worker-a', 17)
    cancel.assert_called_once_with('worker-a', job_ids=[17])


def test_worker_health_failure_cancels_exact_launched_job_id(monkeypatch):
    batch_coordinator = _make_coordinator(job_id=1)
    batch_coordinator._worker_token = 'owner-token'
    batch_coordinator._resolve_formats()
    monkeypatch.setattr(batch_coordinator, '_assert_coordinator_owner',
                        mock.Mock())
    monkeypatch.setattr(batch_coordinator, '_get_pool_resources',
                        mock.Mock(return_value=None))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'register_batch_worker_launch',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_launch_request',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_job_id',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record',
                        mock.Mock(return_value=True))
    execute = mock.Mock(side_effect=['launch-request', 'health-request'])
    monkeypatch.setattr(coordinator.sdk, 'exec', execute)
    get = mock.Mock(side_effect=[(17, None), RuntimeError('unhealthy'), None])
    monkeypatch.setattr(coordinator.sdk, 'get', get)
    cancel = mock.Mock(return_value='cancel-request')
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'is_batch_coordinator_owner',
                        mock.Mock(return_value=True))

    with pytest.raises(RuntimeError, match='failed to start'):
        batch_coordinator._launch_worker_service('worker-a')

    cancel.assert_called_once_with('worker-a', job_ids=[17])


def test_worker_uses_threaded_http_server(monkeypatch):
    server = mock.Mock()
    server.serve_forever = mock.Mock()
    server.shutdown = mock.Mock()
    server_factory = mock.Mock(return_value=server)
    monkeypatch.setattr(worker.http_server, 'ThreadingHTTPServer',
                        server_factory)
    monkeypatch.setattr(worker, '_resolve_input_format', mock.Mock())
    monkeypatch.setattr(worker, '_resolve_output_formats',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(worker.utils, 'deserialize_function',
                        mock.Mock(return_value=lambda: None))

    worker.start_worker('serialized', 's3://bucket/output', 'job-1', 'token')

    server_factory.assert_called_once_with(
        ('127.0.0.1', worker.constants.WORKER_SERVICE_PORT),
        worker._WorkerHandler)
    server.shutdown.assert_called_once_with()


def test_worker_start_raises_on_mapper_crash(monkeypatch):
    server = mock.Mock()
    server.serve_forever = mock.Mock()
    server.shutdown = mock.Mock()
    server_factory = mock.Mock(return_value=server)
    monkeypatch.setattr(worker.http_server, 'ThreadingHTTPServer',
                        server_factory)
    monkeypatch.setattr(worker, '_resolve_input_format', mock.Mock())
    monkeypatch.setattr(worker, '_resolve_output_formats',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(
        worker.utils, 'deserialize_function',
        mock.Mock(
            return_value=lambda: (_ for _ in ()).throw(RuntimeError('boom'))))

    with pytest.raises(RuntimeError, match='boom'):
        worker.start_worker('serialized', 's3://bucket/output', 'job-1',
                            'token')

    assert 'RuntimeError: boom' in worker._get_mapper_failure()
    server.shutdown.assert_called_once_with()


def test_worker_health_reports_mapper_failure(monkeypatch):
    monkeypatch.setattr(worker, '_mapper_failure', None)
    assert worker._get_health_response() == (200, {'status': 'healthy'})

    worker._record_mapper_failure('mapper crashed')

    assert worker._get_health_response() == (503, {
        'status': 'failed',
        'error': 'mapper crashed',
    })


def test_wait_for_batch_completion_fails_fast_when_mapper_failed(monkeypatch):
    item = worker._BatchItem([{'value': 1}], 0, 0, 0)
    monkeypatch.setattr(worker, '_mapper_failure', None)
    worker._record_mapper_failure('mapper crashed')

    start = time.monotonic()
    completed = worker._wait_for_batch_completion(item,
                                                  timeout=3600,
                                                  poll_interval=0.01)
    elapsed = time.monotonic() - start

    assert completed
    assert elapsed < 0.5
    assert item.done_event.is_set()
    assert item.error == 'mapper crashed'


def test_wait_for_batch_completion_notices_late_mapper_failure(monkeypatch):
    item = worker._BatchItem([{'value': 1}], 0, 0, 0)
    monkeypatch.setattr(worker, '_mapper_failure', None)

    def _fail_mapper():
        time.sleep(0.05)
        worker._record_mapper_failure('late crash')

    failure_thread = threading.Thread(target=_fail_mapper, daemon=True)
    failure_thread.start()
    try:
        start = time.monotonic()
        completed = worker._wait_for_batch_completion(item,
                                                      timeout=3600,
                                                      poll_interval=0.01)
        elapsed = time.monotonic() - start
    finally:
        failure_thread.join(timeout=1)

    assert completed
    assert elapsed < 0.5
    assert item.done_event.is_set()
    assert item.error == 'late crash'


def test_worker_startup_code_preserves_python_exit_status():
    batch_coordinator = _make_coordinator()
    batch_coordinator._resolve_formats()

    startup_code = batch_coordinator._generate_worker_startup_code()

    assert 'set -eo pipefail' in startup_code
    assert 'tee /tmp/sky_batch_worker.log' in startup_code
    assert (f'rm -f {coordinator.constants.WORKER_FAILURE_MARKER_PATH}'
            in startup_code)


def test_worker_health_check_code_fails_fast_on_failure_marker(
        tmp_path, monkeypatch):
    failure_marker = tmp_path / 'worker-failure.txt'
    fake_bin = tmp_path / 'fake-bin'
    curl_marker = tmp_path / 'curl-invoked'
    monkeypatch.setattr(coordinator.constants, 'WORKER_FAILURE_MARKER_PATH',
                        str(failure_marker))
    fake_bin.mkdir()
    fake_curl = fake_bin / 'curl'
    fake_curl.write_text(f"""#!/bin/bash
touch '{curl_marker}'
printf '200'
""",
                         encoding='utf-8')
    fake_curl.chmod(0o755)
    monkeypatch.setenv('PATH', f'{fake_bin}:{os.environ["PATH"]}')
    batch_coordinator = _make_coordinator()

    health_code = batch_coordinator._generate_worker_health_check_code()

    # Prove the fake curl is reachable before relying on its marker to verify
    # that the failure-marker branch short-circuits the health request.
    proc = subprocess.run(['/bin/bash', '-c', health_code],
                          check=False,
                          capture_output=True,
                          text=True)
    assert proc.returncode == 0
    assert curl_marker.exists()

    curl_marker.unlink()
    failure_marker.write_text('mapper crashed', encoding='utf-8')
    proc = subprocess.run(['/bin/bash', '-c', health_code],
                          check=False,
                          capture_output=True,
                          text=True)

    assert proc.returncode == 1
    assert not curl_marker.exists()
    assert 'mapper crashed' in proc.stdout


def test_worker_health_check_ignores_stale_marker_from_previous_launch(
        tmp_path, monkeypatch):
    """A marker left by a crashed prior launch must not fail a fresh one."""
    stale_marker = tmp_path / 'worker-failure.txt'
    fake_bin = tmp_path / 'fake-bin'
    monkeypatch.setattr(coordinator.constants, 'WORKER_FAILURE_MARKER_PATH',
                        str(stale_marker))
    fake_bin.mkdir()
    fake_curl = fake_bin / 'curl'
    # Healthy worker: report HTTP 200 and write the -o body file.
    fake_curl.write_text("""#!/bin/bash
while [ $# -gt 0 ]; do
    if [ "$1" = "-o" ]; then echo healthy > "$2"; shift; fi
    shift
done
printf '200'
""",
                         encoding='utf-8')
    fake_curl.chmod(0o755)
    monkeypatch.setenv('PATH', f'{fake_bin}:{os.environ["PATH"]}')
    batch_coordinator = _make_coordinator()

    # Simulate a crashed previous worker launch on the same node.
    stale_marker.write_text('mapper crashed', encoding='utf-8')

    launch_marker = batch_coordinator._new_failure_marker_path()
    health_code = batch_coordinator._generate_worker_health_check_code(
        launch_marker)

    # Plain (non-login) shell so the monkeypatched PATH is preserved.
    proc = subprocess.run(['/bin/bash', '-c', health_code],
                          check=False,
                          capture_output=True,
                          text=True)

    assert proc.returncode == 0
    assert 'ready' in proc.stdout


def test_worker_launch_failure_marker_is_launch_unique():
    batch_coordinator = _make_coordinator()
    batch_coordinator._resolve_formats()

    first = batch_coordinator._new_failure_marker_path()
    second = batch_coordinator._new_failure_marker_path()

    assert first != second
    base, _ = os.path.splitext(coordinator.constants.WORKER_FAILURE_MARKER_PATH)
    assert first.startswith(base)

    startup_code = batch_coordinator._generate_worker_startup_code(first)
    health_code = batch_coordinator._generate_worker_health_check_code(first)
    env_var = coordinator.constants.WORKER_FAILURE_MARKER_ENV_VAR
    assert f'rm -f {first}' in startup_code
    assert f'export {env_var}={first}' in startup_code
    assert f'failure_marker={first}' in health_code


def test_record_mapper_failure_honors_marker_env_override(
        tmp_path, monkeypatch):
    launch_marker = tmp_path / 'launch-scoped-failure.txt'
    default_marker = tmp_path / 'default-failure.txt'
    monkeypatch.setattr(worker.constants, 'WORKER_FAILURE_MARKER_PATH',
                        str(default_marker))
    monkeypatch.setenv(worker.constants.WORKER_FAILURE_MARKER_ENV_VAR,
                       str(launch_marker))

    worker._reset_worker_runtime_state()
    worker._record_mapper_failure('mapper crashed')

    assert launch_marker.read_text(encoding='utf-8') == 'mapper crashed'
    assert not default_marker.exists()


def test_expired_worker_batch_rejects_late_save(monkeypatch):
    item = worker._BatchItem([{'value': 1}], 0, 0, 0)
    monkeypatch.setattr(worker, '_current_batch', item)
    worker._expire_batch(item, 'timed out')

    assert item.done_event.is_set()
    assert item.error == 'timed out'
    assert worker._current_batch is None
    with pytest.raises(RuntimeError, match='without a current batch'):
        worker.save_results([{'value': 2}])


def test_record_mapper_failure_persists_failure_marker(tmp_path, monkeypatch):
    failure_marker = tmp_path / 'worker-failure.txt'
    monkeypatch.setattr(worker.constants, 'WORKER_FAILURE_MARKER_PATH',
                        str(failure_marker))

    worker._reset_worker_runtime_state()
    worker._record_mapper_failure('mapper crashed')
    worker._record_mapper_failure(
        'later crash should not replace the first one')

    assert failure_marker.read_text(encoding='utf-8') == 'mapper crashed'


def test_worker_uploads_to_attempt_scoped_writer(monkeypatch):
    item = worker._BatchItem([{'value': 1}], 0, 0, 0, attempt_id=7)
    output_writer = mock.Mock()
    output_writer.upload_batch_attempt.return_value = 'attempt-path'
    monkeypatch.setattr(worker, '_current_batch', item)
    monkeypatch.setattr(worker, '_output_formats', [output_writer])
    monkeypatch.setattr(worker, '_job_id', 'job-1')

    worker.save_results([{'value': 2}])

    output_writer.upload_batch_attempt.assert_called_once_with([{
        'value': 2
    }], 0, 0, 'job-1', 7)


def test_json_writer_reduces_only_completed_attempts(monkeypatch):
    writer = io_formats.JsonWriter('s3://bucket/output.jsonl')
    save = mock.Mock()
    monkeypatch.setattr(utils, 'save_jsonl_to_cloud', save)

    attempt_path = writer.upload_batch_attempt([{'value': 1}], 0, 3, 'job-1', 4)
    assert attempt_path == utils.get_attempt_batch_path(writer.path, 0, 3,
                                                        'job-1', 4)
    assert '/outputs/' in attempt_path
    save.assert_called_once_with([{'value': 1}], attempt_path)

    concatenate = mock.Mock()
    monkeypatch.setattr(utils, 'concatenate_batch_files_to_output', concatenate)
    writer.reduce_attempt_results('job-1', [(0, 3, 4), (4, 5, 2)])
    concatenate.assert_called_once_with('s3://bucket/output.jsonl', [
        utils.get_attempt_batch_path(writer.path, 0, 3, 'job-1', 4),
        utils.get_attempt_batch_path(writer.path, 4, 5, 'job-1', 2),
    ])


def test_json_writers_in_same_directory_use_distinct_attempt_paths(monkeypatch):
    first = io_formats.JsonWriter('s3://bucket/first.jsonl', column='first')
    second = io_formats.JsonWriter('s3://bucket/second.jsonl', column='second')
    save = mock.Mock()
    monkeypatch.setattr(utils, 'save_jsonl_to_cloud', save)

    first_path = first.upload_batch_attempt([{'first': 1}], 0, 0, 'job-1', 1)
    second_path = second.upload_batch_attempt([{'second': 2}], 0, 0, 'job-1', 1)

    assert first_path != second_path
    assert first_path == utils.get_attempt_batch_path(first.path, 0, 0, 'job-1',
                                                      1)
    assert second_path == utils.get_attempt_batch_path(second.path, 0, 0,
                                                       'job-1', 1)


def test_image_writer_promotes_only_winning_attempt(monkeypatch):

    class _Image:

        def save(self, buffer, format):  # pylint: disable=redefined-builtin
            assert format == 'PNG'
            buffer.write(b'png')

    writer = io_formats.ImageWriter('s3://bucket/images/')
    upload = mock.Mock()
    copy = mock.Mock()
    delete = mock.Mock()
    monkeypatch.setattr(utils, 'upload_bytes_to_cloud', upload)
    monkeypatch.setattr(utils, 'copy_cloud_file', copy)
    monkeypatch.setattr(utils, 'delete_cloud_prefix', delete)

    writer.upload_batch_attempt([{'image': _Image()}], 3, 3, 'job-1', 5)
    attempt_path = ('s3://bucket/images/.sky_batch_tmp/job-1/attempts/5/images/'
                    '00000003.png')
    upload.assert_called_once_with(b'png', attempt_path)

    writer.reduce_attempt_results('job-1', [(3, 3, 5)])
    copy.assert_called_once_with(attempt_path,
                                 's3://bucket/images/00000003.png')
    writer.cleanup('job-1')
    delete.assert_called_once_with('s3://bucket/images/.sky_batch_tmp/job-1/')


def test_image_cleanup_prefix_is_rooted_inside_output_directory():
    assert utils.get_directory_job_temp_prefix(
        's3://bucket/nested/images/',
        'job-1') == ('s3://bucket/nested/images/.sky_batch_tmp/job-1/')
    assert utils.get_directory_job_temp_prefix(
        'gs://bucket/images/',
        'job-2') == ('gs://bucket/images/.sky_batch_tmp/job-2/')


def test_coordinator_reduces_winners_before_separate_cleanup(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator.batches = [[0, 3], [4, 5]]
    output_writer = mock.Mock()
    output_writer.path = 's3://bucket/output.jsonl'
    batch_coordinator._output_formats = [output_writer]
    monkeypatch.setattr(batch_coordinator, '_assert_coordinator_owner',
                        mock.Mock())
    monkeypatch.setattr(
        coordinator.managed_job_state, 'get_batch_states',
        mock.Mock(return_value=[{
            'batch_idx': 0,
            'start_idx': 0,
            'end_idx': 3,
            'status': 'COMPLETED',
            'attempt_id': 4,
            'attempt_owner_token': 'owner-a',
        }, {
            'batch_idx': 1,
            'start_idx': 4,
            'end_idx': 5,
            'status': 'COMPLETED',
            'attempt_id': 2,
            'attempt_owner_token': 'owner-a',
        }]))

    batch_coordinator._reduce_results()

    output_writer.reduce_attempt_results.assert_called_once_with(
        '1', [(0, 3, 4), (4, 5, 2)])
    output_writer.cleanup.assert_not_called()

    batch_coordinator.cleanup()
    output_writer.cleanup.assert_called_once_with('1')


@pytest.mark.asyncio
async def test_batch_cleanup_runs_after_durable_success(monkeypatch):
    events = []
    batch_coordinator = mock.Mock()
    batch_coordinator.run.side_effect = lambda: events.append('run')
    batch_coordinator.mark_succeeded.side_effect = (
        lambda end_time: events.append('succeeded'))
    batch_coordinator.cleanup.side_effect = lambda: events.append('cleanup')
    monkeypatch.setattr(jobs_controller.batch_coordinator, 'BatchCoordinator',
                        mock.Mock(return_value=batch_coordinator))
    monkeypatch.setattr(
        jobs_controller.managed_job_state, 'get_latest_task_id_status_async',
        mock.AsyncMock(return_value=(0, state.ManagedJobStatus.RUNNING)))

    task = mock.Mock()
    task.metadata = {
        'batch_dataset_path': 's3://bucket/input.jsonl',
        'batch_output_path': 's3://bucket/output.jsonl',
        'batch_size': 4,
        'batch_pool_name': 'pool',
        'batch_serialized_fn': 'serialized',
        'batch_input_format': {},
        'batch_output_formats': [],
    }
    controller_instance = types.SimpleNamespace(
        _job_id=1, _release_initial_launch_slot=mock.AsyncMock())

    succeeded = await jobs_controller.JobController._run_batch_coordinator_task(
        controller_instance,
        task_id=0,
        task=task,
        callback_func=mock.AsyncMock(),
        is_resume=True)

    assert succeeded
    assert events == ['run', 'succeeded', 'cleanup']


@pytest.mark.asyncio
@pytest.mark.parametrize('is_resume', [False, True])
async def test_batch_releases_launch_slot_before_coordinator_loop(
        is_resume, monkeypatch):
    run_started = threading.Event()
    finish_run = threading.Event()
    slot_present_during_run = []
    batch_coordinator = mock.Mock()

    controller_instance = jobs_controller.JobController.__new__(
        jobs_controller.JobController)
    controller_instance._job_id = 1
    controller_instance._backend = mock.Mock(
        run_timestamp='run-2026-07-27-00-00-00-000000')
    controller_instance.starting = {1}
    controller_instance.starting_lock = asyncio.Lock()
    controller_instance.starting_signal = asyncio.Condition(
        controller_instance.starting_lock)

    def run():
        slot_present_during_run.append(1 in controller_instance.starting)
        run_started.set()
        assert finish_run.wait(timeout=2)

    batch_coordinator.run.side_effect = run
    monkeypatch.setattr(jobs_controller.batch_coordinator, 'BatchCoordinator',
                        mock.Mock(return_value=batch_coordinator))
    monkeypatch.setattr(
        jobs_controller.managed_job_state, 'get_latest_task_id_status_async',
        mock.AsyncMock(return_value=(0, state.ManagedJobStatus.RUNNING)))
    set_starting = mock.AsyncMock()
    set_started = mock.AsyncMock()
    monkeypatch.setattr(jobs_controller.managed_job_state, 'set_starting_async',
                        set_starting)
    monkeypatch.setattr(jobs_controller.managed_job_state, 'set_started_async',
                        set_started)

    task = mock.Mock()
    task.metadata = {
        'batch_dataset_path': 's3://bucket/input.jsonl',
        'batch_output_path': 's3://bucket/output.jsonl',
        'batch_size': 4,
        'batch_pool_name': 'pool',
        'batch_serialized_fn': 'serialized',
        'batch_input_format': {},
        'batch_output_formats': [],
    }

    async def wait_for_slot():
        async with controller_instance.starting_signal:
            await controller_instance.starting_signal.wait_for(
                lambda: 1 not in controller_instance.starting)

    waiter = asyncio.create_task(wait_for_slot())
    await asyncio.sleep(0)
    run_task = asyncio.create_task(
        controller_instance._run_batch_coordinator_task(
            task_id=0,
            task=task,
            callback_func=mock.AsyncMock(),
            is_resume=is_resume))
    try:
        assert await asyncio.to_thread(run_started.wait, 1)
        await asyncio.wait_for(asyncio.shield(waiter), timeout=1)
        assert not run_task.done()
        assert slot_present_during_run == [False]
    finally:
        finish_run.set()
        await asyncio.gather(run_task, return_exceptions=True)
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)

    if is_resume:
        set_starting.assert_not_awaited()
        set_started.assert_not_awaited()
    else:
        set_starting.assert_awaited_once()
        set_started.assert_awaited_once()


@pytest.mark.asyncio
async def test_terminal_batch_releases_launch_slot_without_coordinator(
        monkeypatch):
    controller_instance = jobs_controller.JobController.__new__(
        jobs_controller.JobController)
    controller_instance._job_id = 1
    controller_instance.starting = {1}
    controller_instance.starting_lock = asyncio.Lock()
    controller_instance.starting_signal = asyncio.Condition(
        controller_instance.starting_lock)

    monkeypatch.setattr(
        jobs_controller.managed_job_state, 'get_latest_task_id_status_async',
        mock.AsyncMock(return_value=(0, state.ManagedJobStatus.SUCCEEDED)))
    coordinator_factory = mock.Mock(
        side_effect=AssertionError('terminal batch built a coordinator'))
    monkeypatch.setattr(jobs_controller.batch_coordinator, 'BatchCoordinator',
                        coordinator_factory)

    result = await controller_instance._run_batch_coordinator_task(
        task_id=0,
        task=mock.Mock(),
        callback_func=mock.AsyncMock(),
        is_resume=True)

    assert result
    assert controller_instance.starting == set()
    coordinator_factory.assert_not_called()


@pytest.mark.asyncio
async def test_superseded_coordinator_never_marks_batch_failed(monkeypatch):
    batch_coordinator = mock.Mock()
    batch_coordinator.run.side_effect = coordinator.SupersededCoordinator(
        'new owner')
    batch_coordinator.handle_superseded = mock.AsyncMock()
    monkeypatch.setattr(jobs_controller.batch_coordinator, 'BatchCoordinator',
                        mock.Mock(return_value=batch_coordinator))
    monkeypatch.setattr(
        jobs_controller.managed_job_state, 'get_latest_task_id_status_async',
        mock.AsyncMock(return_value=(0, state.ManagedJobStatus.RUNNING)))
    task = mock.Mock()
    task.metadata = {
        'batch_dataset_path': 's3://bucket/input.jsonl',
        'batch_output_path': 's3://bucket/output.jsonl',
        'batch_size': 4,
        'batch_pool_name': 'pool',
        'batch_serialized_fn': 'serialized',
        'batch_input_format': {},
        'batch_output_formats': [],
    }
    controller_instance = types.SimpleNamespace(
        _job_id=1, _release_initial_launch_slot=mock.AsyncMock())

    with pytest.raises(coordinator.SupersededCoordinator):
        await jobs_controller.JobController._run_batch_coordinator_task(
            controller_instance,
            task_id=0,
            task=task,
            callback_func=mock.AsyncMock(),
            is_resume=True)

    batch_coordinator.handle_superseded.assert_awaited_once_with()
    batch_coordinator.mark_failed.assert_not_called()


@pytest.mark.parametrize('supersession_source', ['run', 'mark_failed'])
@pytest.mark.asyncio
async def test_supersession_cleanup_preserves_owner_signal_under_cancellation(
        supersession_source, monkeypatch):
    batch_coordinator = mock.Mock()
    if supersession_source == 'run':
        batch_coordinator.run.side_effect = coordinator.SupersededCoordinator(
            'new owner')
    else:
        batch_coordinator.run.side_effect = RuntimeError('batch failed')
        batch_coordinator.mark_failed.side_effect = (
            coordinator.SupersededCoordinator('new owner'))

    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_completed = asyncio.Event()

    async def blocked_superseded_cleanup():
        cleanup_started.set()
        await allow_cleanup.wait()
        cleanup_completed.set()

    batch_coordinator.handle_superseded = mock.AsyncMock(
        side_effect=blocked_superseded_cleanup)
    monkeypatch.setattr(jobs_controller.batch_coordinator, 'BatchCoordinator',
                        mock.Mock(return_value=batch_coordinator))
    monkeypatch.setattr(
        jobs_controller.managed_job_state, 'get_latest_task_id_status_async',
        mock.AsyncMock(return_value=(0, state.ManagedJobStatus.RUNNING)))
    task = mock.Mock()
    task.metadata = {
        'batch_dataset_path': 's3://bucket/input.jsonl',
        'batch_output_path': 's3://bucket/output.jsonl',
        'batch_size': 4,
        'batch_pool_name': 'pool',
        'batch_serialized_fn': 'serialized',
        'batch_input_format': {},
        'batch_output_formats': [],
    }
    controller_instance = types.SimpleNamespace(
        _job_id=1, _release_initial_launch_slot=mock.AsyncMock())

    run_task = asyncio.create_task(
        jobs_controller.JobController._run_batch_coordinator_task(
            controller_instance,
            task_id=0,
            task=task,
            callback_func=mock.AsyncMock(),
            is_resume=True))
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    run_task.cancel()
    await asyncio.sleep(0)
    run_task.cancel()
    await asyncio.sleep(0)

    try:
        assert not run_task.done()
        assert not cleanup_completed.is_set()
    finally:
        # Release retained cleanup even after an assertion failure so the test
        # cannot leak a cancellation-resistant task.
        allow_cleanup.set()
        task_result = await asyncio.gather(run_task, return_exceptions=True)

    assert isinstance(task_result[0], coordinator.SupersededCoordinator)
    assert cleanup_completed.is_set()
    batch_coordinator.handle_superseded.assert_awaited_once_with()
    if supersession_source == 'run':
        batch_coordinator.mark_failed.assert_not_called()
    else:
        batch_coordinator.mark_failed.assert_called_once_with('batch failed')


@pytest.mark.asyncio
async def test_superseded_cleanup_has_one_global_deadline(monkeypatch):
    batch_coordinator = _make_coordinator()
    with batch_coordinator._active_workers_lock:
        batch_coordinator._active_workers['worker-a'] = 17
    release = threading.Event()
    entered = threading.Event()

    def _slow_exec(task, cluster_name):
        del task, cluster_name
        entered.set()
        release.wait(timeout=5)
        return 'shutdown-request'

    monkeypatch.setattr(coordinator.sdk, 'exec', _slow_exec)
    get = mock.Mock()
    cancel = mock.Mock()
    monkeypatch.setattr(coordinator.sdk, 'get', get)
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    started = time.monotonic()
    try:
        await batch_coordinator.handle_superseded(timeout=0.05)
    finally:
        release.set()

    assert await asyncio.to_thread(entered.wait, 1)
    assert time.monotonic() - started < 0.5
    # The timed-out exec may finish, but no subsequent get/cancel is started.
    get.assert_not_called()
    cancel.assert_not_called()


@pytest.mark.asyncio
async def test_superseded_cleanup_fans_out_active_workers(monkeypatch):
    batch_coordinator = _make_coordinator()
    with batch_coordinator._active_workers_lock:
        batch_coordinator._active_workers.update({
            'worker-a': 17,
            'worker-b': 18,
        })

    blocked_worker_entered = threading.Event()
    release_blocked_worker = threading.Event()
    sibling_cancelled = threading.Event()
    events = []

    def _exec(task, cluster_name):
        del task
        events.append(('exec', cluster_name))
        if cluster_name == 'worker-a':
            blocked_worker_entered.set()
            release_blocked_worker.wait(timeout=5)
        return f'shutdown-{cluster_name}'

    def _get(request_id):
        events.append(('get', request_id))
        return None

    def _cancel(cluster_name, job_ids):
        events.append(('cancel', cluster_name, job_ids))
        if cluster_name == 'worker-b':
            sibling_cancelled.set()
        return f'cancel-{cluster_name}'

    def _remove_record(job_id, worker_token, cluster_name, *, worker_job_id):
        del job_id, worker_token
        events.append(('remove', cluster_name, worker_job_id))
        with batch_coordinator._active_workers_lock:
            batch_coordinator._active_workers.pop(cluster_name, None)
        return True

    monkeypatch.setattr(coordinator.sdk, 'exec', _exec)
    monkeypatch.setattr(coordinator.sdk, 'get', _get)
    monkeypatch.setattr(coordinator.sdk, 'cancel', _cancel)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', _remove_record)
    cleanup = asyncio.create_task(
        batch_coordinator.handle_superseded(timeout=2))
    try:
        assert await asyncio.to_thread(blocked_worker_entered.wait, 1)
        assert await asyncio.to_thread(sibling_cancelled.wait, 1)
        release_blocked_worker.set()
        await cleanup
    finally:
        release_blocked_worker.set()
        await asyncio.gather(cleanup, return_exceptions=True)

    sibling_events = [
        event for event in events
        if any(value == 'worker-b' or value == 'shutdown-worker-b' or
               value == 'cancel-worker-b' for value in event)
    ]
    assert sibling_events == [
        ('exec', 'worker-b'),
        ('get', 'shutdown-worker-b'),
        ('cancel', 'worker-b', [18]),
        ('get', 'cancel-worker-b'),
        ('remove', 'worker-b', 18),
    ]


@pytest.mark.asyncio
async def test_superseded_cleanup_bounds_active_worker_fanout(monkeypatch):
    batch_coordinator = _make_coordinator()
    with batch_coordinator._active_workers_lock:
        batch_coordinator._active_workers.update({
            'worker-a': 17,
            'worker-b': 18,
            'worker-c': 19,
            'worker-d': 20,
        })

    monkeypatch.setattr(coordinator, '_SUPERSEDED_CLEANUP_MAX_CONCURRENCY', 2)
    release_exec = threading.Event()
    two_execs_started = threading.Event()
    third_exec_started = threading.Event()
    state_lock = threading.Lock()
    active_execs = 0
    peak_execs = 0

    def _exec(task, cluster_name):
        del task, cluster_name
        nonlocal active_execs, peak_execs
        with state_lock:
            active_execs += 1
            peak_execs = max(peak_execs, active_execs)
            if active_execs >= 2:
                two_execs_started.set()
            if active_execs >= 3:
                third_exec_started.set()
        release_exec.wait(timeout=5)
        with state_lock:
            active_execs -= 1
        return 'shutdown-request'

    monkeypatch.setattr(coordinator.sdk, 'exec', _exec)
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(return_value=None))
    monkeypatch.setattr(coordinator.sdk, 'cancel',
                        mock.Mock(return_value='cancel-request'))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records', mock.Mock(return_value=[]))

    cleanup = asyncio.create_task(
        batch_coordinator.handle_superseded(timeout=2))
    try:
        assert await asyncio.to_thread(two_execs_started.wait, 1)
        assert not await asyncio.to_thread(third_exec_started.wait, 0.1)
    finally:
        release_exec.set()
        await asyncio.gather(cleanup, return_exceptions=True)

    assert peak_execs == 2


@pytest.mark.asyncio
async def test_bounded_cleanup_propagates_worker_failure_without_deadlock(
        monkeypatch):
    monkeypatch.setattr(coordinator, '_SUPERSEDED_CLEANUP_MAX_CONCURRENCY', 1)
    started = []

    async def _fail_first(item):
        started.append(item)
        raise RuntimeError('unexpected cleanup failure')

    cleanup = asyncio.create_task(
        coordinator._run_bounded_async(['first', 'second'], func=_fail_first))
    done, _ = await asyncio.wait({cleanup}, timeout=0.2)
    try:
        assert cleanup in done
    finally:
        if not cleanup.done():
            cleanup.cancel()
            await asyncio.gather(cleanup, return_exceptions=True)

    with pytest.raises(RuntimeError, match='unexpected cleanup failure'):
        cleanup.result()
    assert started == ['first']


@pytest.mark.asyncio
async def test_superseded_cleanup_retires_cleaned_active_workers_immediately(
        monkeypatch):
    batch_coordinator = _make_coordinator()
    with batch_coordinator._active_workers_lock:
        batch_coordinator._active_workers['worker-a'] = 17

    monkeypatch.setattr(coordinator.sdk, 'exec',
                        mock.Mock(return_value='shutdown-request'))
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(return_value=None))
    monkeypatch.setattr(coordinator.sdk, 'cancel',
                        mock.Mock(return_value='cancel-request'))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records', mock.Mock(return_value=[]))

    await asyncio.wait_for(batch_coordinator.handle_superseded(timeout=10),
                           timeout=1)

    with batch_coordinator._active_workers_lock:
        assert not batch_coordinator._active_workers


def test_cancel_worker_job_by_id_keeps_newer_active_worker_registration(
        monkeypatch):
    batch_coordinator = _make_coordinator()
    with batch_coordinator._active_workers_lock:
        batch_coordinator._active_workers['worker-a'] = 18

    monkeypatch.setattr(coordinator.sdk, 'cancel',
                        mock.Mock(return_value='cancel-request'))
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(return_value=None))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record',
                        mock.Mock(return_value=True))

    batch_coordinator._cancel_worker_job_by_id('worker-a', 17,
                                               batch_coordinator._worker_token)

    with batch_coordinator._active_workers_lock:
        assert batch_coordinator._active_workers == {'worker-a': 18}


@pytest.mark.asyncio
async def test_superseded_cleanup_queries_each_owned_cluster_once(monkeypatch):
    batch_coordinator = _make_coordinator()
    records = [{
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': 'worker-a',
        'worker_job_name': 'batch-worker-1-owner-a',
        'worker_job_id': None,
        'launch_request_id': None,
    }, {
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': 'worker-b',
        'worker_job_name': 'batch-worker-1-owner-b',
        'worker_job_id': None,
        'launch_request_id': None,
    }, {
        'coordinator_token': 'replacement-owner',
        'worker_cluster': 'worker-a',
        'worker_job_name': 'batch-worker-1-replacement-owner',
        'worker_job_id': None,
        'launch_request_id': None,
    }]
    queued_jobs = {
        'queue-worker-a': [
            types.SimpleNamespace(job_id=17, job_name='batch-worker-1-owner-a')
        ],
        'queue-worker-b': [
            types.SimpleNamespace(job_id=18, job_name='batch-worker-1-owner-b')
        ],
    }
    queue = mock.Mock(
        side_effect=lambda cluster_name, **_: f'queue-{cluster_name}')
    persist_job_id = mock.Mock(return_value=True)
    remove_record = mock.Mock(return_value=True)

    def _get(request_id):
        if request_id in queued_jobs:
            return queued_jobs[request_id]
        if request_id in ('cancel-17', 'cancel-18'):
            return None
        raise AssertionError(f'unexpected sdk.get request {request_id!r}')

    def _cancel(cluster_name, job_ids):
        assert cluster_name in ('worker-a', 'worker-b')
        assert len(job_ids) == 1
        return f'cancel-{job_ids[0]}'

    monkeypatch.setattr(coordinator.sdk, 'queue', queue)
    get = mock.Mock(side_effect=_get)
    cancel = mock.Mock(side_effect=_cancel)
    monkeypatch.setattr(coordinator.sdk, 'get', get)
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records',
                        mock.Mock(return_value=records))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_job_id', persist_job_id)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove_record)

    await batch_coordinator.handle_superseded(timeout=1)

    assert sorted(call.args[0] for call in queue.call_args_list) == [
        'worker-a', 'worker-b'
    ]
    assert sorted(call.args[0] for call in get.call_args_list) == [
        'cancel-17', 'cancel-18', 'queue-worker-a', 'queue-worker-b'
    ]
    assert sorted((call.args[0], tuple(call.kwargs['job_ids']))
                  for call in cancel.call_args_list) == [('worker-a', (17,)),
                                                         ('worker-b', (18,))]
    persist_job_id.assert_has_calls([
        mock.call(1, batch_coordinator._worker_token, 'worker-a', 17),
        mock.call(1, batch_coordinator._worker_token, 'worker-b', 18),
    ],
                                    any_order=True)
    remove_record.assert_has_calls([
        mock.call(
            1, batch_coordinator._worker_token, 'worker-a', worker_job_id=17),
        mock.call(
            1, batch_coordinator._worker_token, 'worker-b', worker_job_id=18),
    ],
                                   any_order=True)


@pytest.mark.asyncio
async def test_superseded_cleanup_reuses_queue_snapshot_on_same_cluster(
        monkeypatch):
    batch_coordinator = _make_coordinator()
    records = [{
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': 'worker-a',
        'worker_job_name': 'batch-worker-1-owner-a',
        'worker_job_id': None,
        'launch_request_id': None,
    }, {
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': 'worker-a',
        'worker_job_name': 'batch-worker-1-owner-b',
        'worker_job_id': None,
        'launch_request_id': None,
    }]
    queue_calls = []
    persist_job_id = mock.Mock(return_value=True)
    remove_record = mock.Mock(return_value=True)

    def _queue(cluster_name, **_):
        queue_calls.append(cluster_name)
        if len(queue_calls) > 1:
            raise RuntimeError('duplicate queue snapshot')
        return 'queue-worker-a'

    def _get(request_id):
        if request_id == 'queue-worker-a':
            return [
                types.SimpleNamespace(job_id=17,
                                      job_name='batch-worker-1-owner-a'),
                types.SimpleNamespace(job_id=18,
                                      job_name='batch-worker-1-owner-b'),
            ]
        if request_id in ('cancel-17', 'cancel-18'):
            return None
        raise AssertionError(f'unexpected sdk.get request {request_id!r}')

    def _cancel(cluster_name, job_ids):
        assert cluster_name == 'worker-a'
        assert len(job_ids) == 1
        return f'cancel-{job_ids[0]}'

    monkeypatch.setattr(coordinator.sdk, 'queue', mock.Mock(side_effect=_queue))
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(side_effect=_get))
    monkeypatch.setattr(coordinator.sdk, 'cancel',
                        mock.Mock(side_effect=_cancel))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records',
                        mock.Mock(return_value=records))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_job_id', persist_job_id)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove_record)

    await batch_coordinator.handle_superseded(timeout=1)

    assert queue_calls == ['worker-a']
    persist_job_id.assert_has_calls([
        mock.call(1, batch_coordinator._worker_token, 'worker-a', 17),
        mock.call(1, batch_coordinator._worker_token, 'worker-a', 18),
    ],
                                    any_order=True)
    remove_record.assert_has_calls([
        mock.call(
            1, batch_coordinator._worker_token, 'worker-a', worker_job_id=17),
        mock.call(
            1, batch_coordinator._worker_token, 'worker-a', worker_job_id=18),
    ],
                                   any_order=True)


@pytest.mark.asyncio
async def test_superseded_cleanup_durable_timeout_starts_no_later_call(
        monkeypatch):
    batch_coordinator = _make_coordinator()
    records = [{
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': 'worker-a',
        'worker_job_name': 'batch-worker-1-owner-a',
        'worker_job_id': None,
        'launch_request_id': 'launch-17',
    }]
    launch_recovery_entered = threading.Event()
    release_launch_recovery = threading.Event()
    launch_recovery_returned = threading.Event()
    queue_started = threading.Event()

    def _get(request_id):
        assert request_id == 'launch-17'
        launch_recovery_entered.set()
        release_launch_recovery.wait(timeout=5)
        launch_recovery_returned.set()
        return None

    def _queue(*args, **kwargs):
        del args, kwargs
        queue_started.set()
        return 'queue-17'

    persist_job_id = mock.Mock(return_value=True)
    cancel = mock.Mock()
    remove_record = mock.Mock(return_value=True)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records',
                        mock.Mock(return_value=records))
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(side_effect=_get))
    monkeypatch.setattr(coordinator.sdk, 'queue', mock.Mock(side_effect=_queue))
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_job_id', persist_job_id)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove_record)

    try:
        await batch_coordinator.handle_superseded(timeout=0.2)
        assert await asyncio.to_thread(launch_recovery_entered.wait, 1)
    finally:
        release_launch_recovery.set()

    assert await asyncio.to_thread(launch_recovery_returned.wait, 1)
    assert not await asyncio.to_thread(queue_started.wait, 0.2)
    persist_job_id.assert_not_called()
    cancel.assert_not_called()
    remove_record.assert_not_called()


@pytest.mark.asyncio
async def test_superseded_cleanup_fans_out_durable_workers(monkeypatch):
    batch_coordinator = _make_coordinator()
    records = [{
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': 'worker-a',
        'worker_job_name': 'batch-worker-1-owner-a',
        'worker_job_id': None,
        'launch_request_id': 'launch-a',
    }, {
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': 'worker-b',
        'worker_job_name': 'batch-worker-1-owner-b',
        'worker_job_id': None,
        'launch_request_id': None,
    }]
    launch_recovery_entered = threading.Event()
    release_launch_recovery = threading.Event()
    sibling_cancelled = threading.Event()
    events = []

    def _get(request_id):
        events.append(('get', request_id))
        if request_id == 'launch-a':
            launch_recovery_entered.set()
            release_launch_recovery.wait(timeout=5)
            return 17
        if request_id == 'queue-worker-b':
            return [
                types.SimpleNamespace(job_id=18,
                                      job_name='batch-worker-1-owner-b')
            ]
        if request_id == 'cancel-17':
            return None
        if request_id == 'cancel-18':
            return None
        raise AssertionError(f'unexpected sdk.get request {request_id!r}')

    def _queue(cluster_name, **_):
        events.append(('queue', cluster_name))
        assert cluster_name == 'worker-b'
        return 'queue-worker-b'

    def _cancel(cluster_name, job_ids):
        events.append(('cancel', cluster_name, job_ids))
        if cluster_name == 'worker-a':
            assert job_ids == [17]
            return 'cancel-17'
        assert cluster_name == 'worker-b'
        assert job_ids == [18]
        sibling_cancelled.set()
        return 'cancel-18'

    persist_job_id = mock.Mock(return_value=True)
    remove_record = mock.Mock(return_value=True)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records',
                        mock.Mock(return_value=records))
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(side_effect=_get))
    monkeypatch.setattr(coordinator.sdk, 'queue', mock.Mock(side_effect=_queue))
    monkeypatch.setattr(coordinator.sdk, 'cancel',
                        mock.Mock(side_effect=_cancel))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_job_id', persist_job_id)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove_record)

    cleanup = asyncio.create_task(
        batch_coordinator.handle_superseded(timeout=2))
    try:
        assert await asyncio.to_thread(launch_recovery_entered.wait, 1)
        assert await asyncio.to_thread(sibling_cancelled.wait, 1)
    finally:
        release_launch_recovery.set()
        await asyncio.gather(cleanup, return_exceptions=True)

    sibling_events = [
        event for event in events
        if any(value == 'worker-b' or value == 'queue-worker-b' or
               value == 'cancel-18' for value in event)
    ]
    assert sibling_events == [
        ('queue', 'worker-b'),
        ('get', 'queue-worker-b'),
        ('cancel', 'worker-b', [18]),
        ('get', 'cancel-18'),
    ]
    persist_job_id.assert_has_calls(
        [mock.call(1, batch_coordinator._worker_token, 'worker-b', 18)])
    remove_record.assert_has_calls([
        mock.call(1,
                  batch_coordinator._worker_token,
                  'worker-b',
                  worker_job_id=18)
    ])


@pytest.mark.asyncio
async def test_superseded_cleanup_bounds_durable_worker_fanout(monkeypatch):
    batch_coordinator = _make_coordinator()
    records = [{
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': f'worker-{index}',
        'worker_job_name': f'batch-worker-1-owner-{index}',
        'worker_job_id': None,
        'launch_request_id': f'launch-{index}',
    } for index in range(4)]

    monkeypatch.setattr(coordinator, '_SUPERSEDED_CLEANUP_MAX_CONCURRENCY', 2)
    release_launch_recovery = threading.Event()
    two_launches_started = threading.Event()
    third_launch_started = threading.Event()
    state_lock = threading.Lock()
    active_launches = 0
    peak_launches = 0

    def _get(request_id):
        nonlocal active_launches, peak_launches
        if request_id.startswith('launch-'):
            with state_lock:
                active_launches += 1
                peak_launches = max(peak_launches, active_launches)
                if active_launches >= 2:
                    two_launches_started.set()
                if active_launches >= 3:
                    third_launch_started.set()
            release_launch_recovery.wait(timeout=5)
            with state_lock:
                active_launches -= 1
            return int(request_id.split('-')[-1]) + 17
        if request_id.startswith('cancel-'):
            return None
        raise AssertionError(f'unexpected sdk.get request {request_id!r}')

    def _cancel(cluster_name, job_ids):
        del cluster_name
        return f'cancel-{job_ids[0]}'

    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records',
                        mock.Mock(return_value=records))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_job_id',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record',
                        mock.Mock(return_value=True))
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(side_effect=_get))
    monkeypatch.setattr(coordinator.sdk, 'cancel',
                        mock.Mock(side_effect=_cancel))
    monkeypatch.setattr(coordinator.sdk, 'queue', mock.Mock())

    cleanup = asyncio.create_task(
        batch_coordinator.handle_superseded(timeout=2))
    try:
        assert await asyncio.to_thread(two_launches_started.wait, 1)
        assert not await asyncio.to_thread(third_launch_started.wait, 0.1)
    finally:
        release_launch_recovery.set()
        await asyncio.gather(cleanup, return_exceptions=True)

    assert peak_launches == 2


@pytest.mark.asyncio
async def test_superseded_cleanup_refuses_ambiguous_queue_ids(monkeypatch):
    batch_coordinator = _make_coordinator()
    worker_job_name = 'batch-worker-1-owner-a'
    records = [{
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': 'worker-a',
        'worker_job_name': worker_job_name,
        'worker_job_id': None,
        'launch_request_id': None,
    }]
    queued_jobs = [
        types.SimpleNamespace(job_id=17, job_name=worker_job_name),
        types.SimpleNamespace(job_id=18, job_name=worker_job_name),
    ]
    persist_job_id = mock.Mock(return_value=True)
    cancel = mock.Mock()
    remove_record = mock.Mock(return_value=True)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records',
                        mock.Mock(return_value=records))
    monkeypatch.setattr(coordinator.sdk, 'queue',
                        mock.Mock(return_value='queue-1'))
    monkeypatch.setattr(coordinator.sdk, 'get',
                        mock.Mock(return_value=queued_jobs))
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_job_id', persist_job_id)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove_record)

    await batch_coordinator.handle_superseded(timeout=1)

    persist_job_id.assert_not_called()
    cancel.assert_not_called()
    remove_record.assert_not_called()


@pytest.mark.asyncio
async def test_superseded_cleanup_contains_failed_cluster_queue(monkeypatch):
    batch_coordinator = _make_coordinator()
    records = [{
        'coordinator_token': batch_coordinator._worker_token,
        'worker_cluster': 'worker-a',
        'worker_job_name': 'batch-worker-1-owner-a',
        'worker_job_id': None,
        'launch_request_id': None,
    }]
    queue = mock.Mock(side_effect=RuntimeError('queue unavailable'))
    persist_job_id = mock.Mock(return_value=True)
    cancel = mock.Mock()
    remove_record = mock.Mock(return_value=True)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records',
                        mock.Mock(return_value=records))
    monkeypatch.setattr(coordinator.sdk, 'queue', queue)
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock())
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'record_batch_worker_job_id', persist_job_id)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', remove_record)

    await batch_coordinator.handle_superseded(timeout=1)

    queue.assert_called_once_with('worker-a', skip_finished=True)
    persist_job_id.assert_not_called()
    cancel.assert_not_called()
    remove_record.assert_not_called()


@pytest.mark.asyncio
async def test_superseded_cleanup_contains_active_worker_failure(monkeypatch):
    batch_coordinator = _make_coordinator()
    with batch_coordinator._active_workers_lock:
        batch_coordinator._active_workers.update({
            'worker-a': 17,
            'worker-b': 18,
        })
    cancel_calls = []

    def _exec(task, cluster_name):
        del task
        if cluster_name == 'worker-a':
            raise RuntimeError('shutdown unavailable')
        return 'shutdown-worker-b'

    def _cancel(cluster_name, job_ids):
        cancel_calls.append((cluster_name, job_ids))
        if cluster_name == 'worker-a':
            with batch_coordinator._active_workers_lock:
                batch_coordinator._active_workers.pop(cluster_name)
            raise RuntimeError('cancel unavailable')
        assert job_ids == [18]
        return 'cancel-worker-b'

    remove_record = mock.Mock(return_value=True)

    def _remove_and_finalize(*args, **kwargs):
        cluster_name = args[2]
        with batch_coordinator._active_workers_lock:
            batch_coordinator._active_workers.pop(cluster_name)
        return remove_record(*args, **kwargs)

    monkeypatch.setattr(coordinator.sdk, 'exec', _exec)
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock(return_value=None))
    monkeypatch.setattr(coordinator.sdk, 'cancel', _cancel)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'remove_batch_worker_record', _remove_and_finalize)
    monkeypatch.setattr(coordinator.managed_job_state,
                        'get_batch_worker_records', mock.Mock(return_value=[]))

    await batch_coordinator.handle_superseded(timeout=1)

    assert sorted(cancel_calls) == [('worker-a', [17]), ('worker-b', [18])]
    remove_record.assert_called_once_with(1,
                                          batch_coordinator._worker_token,
                                          'worker-b',
                                          worker_job_id=18)


@pytest.mark.asyncio
async def test_superseded_jobs_controller_skips_terminal_finalizers(
        monkeypatch):
    task = mock.Mock()
    task.name = 'batch-task'
    dag = mock.Mock()
    dag.is_job_group.return_value = False
    dag.tasks = [task]
    controller_instance = object.__new__(jobs_controller.JobController)
    controller_instance._job_id = 1
    controller_instance._dag = dag
    controller_instance._run_one_task = mock.AsyncMock(
        side_effect=coordinator.SupersededCoordinator('new owner'))
    cancelling = mock.AsyncMock()
    cancelled = mock.AsyncMock()
    monkeypatch.setattr(jobs_controller.managed_job_state,
                        'set_cancelling_async', cancelling)
    monkeypatch.setattr(jobs_controller.managed_job_state,
                        'set_cancelled_async', cancelled)

    with pytest.raises(coordinator.SupersededCoordinator):
        await jobs_controller.JobController.run(controller_instance)

    cancelling.assert_not_awaited()
    cancelled.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_job_loop_superseded_skips_all_durable_finalizers(
        tmp_path, monkeypatch):
    manager = jobs_controller.ControllerManager('old-controller')
    manager.starting.add(1)
    old_controller = mock.Mock()
    old_controller.run = mock.AsyncMock(
        side_effect=coordinator.SupersededCoordinator('replacement owns job'))
    monkeypatch.setattr(jobs_controller, 'JobController',
                        mock.Mock(return_value=old_controller))
    monkeypatch.setattr(jobs_controller.file_content_utils,
                        'get_job_env_content', mock.Mock(return_value=None))
    monkeypatch.setattr(jobs_controller.usage_lib,
                        'install_fresh_messages_for_current_context',
                        mock.Mock())
    cleanup = mock.AsyncMock()
    monkeypatch.setattr(manager, '_cleanup', cleanup)
    get_status = mock.AsyncMock()
    set_failed = mock.AsyncMock()
    job_done = mock.AsyncMock()
    monkeypatch.setattr(jobs_controller.managed_job_state, 'get_status_async',
                        get_status)
    monkeypatch.setattr(jobs_controller.managed_job_state, 'set_failed_async',
                        set_failed)
    monkeypatch.setattr(jobs_controller.scheduler, 'job_done_async', job_done)

    with pytest.raises(coordinator.SupersededCoordinator):
        await manager.run_job_loop(1, str(tmp_path / 'controller.log'))

    # External/durable finalization belongs only to the replacement process.
    cleanup.assert_not_awaited()
    get_status.assert_not_awaited()
    set_failed.assert_not_awaited()
    job_done.assert_not_awaited()
    # Old-process-local scheduler bookkeeping is still released.
    assert 1 not in manager.starting
    assert 1 not in manager.job_tasks


def test_json_reduction_streams_files_without_loading_rows(
        tmp_path, monkeypatch):
    first = tmp_path / 'first.jsonl'
    second = tmp_path / 'second.jsonl'
    first.write_bytes(b'{"idx": 0}\n{"idx": 1}\n')
    second.write_bytes(b'{"idx": 2}\n')
    cloud_files = {
        's3://bucket/first.jsonl': first,
        's3://bucket/second.jsonl': second,
    }
    uploaded = tmp_path / 'uploaded.jsonl'

    monkeypatch.setattr(utils, 'list_batch_files', lambda *_: list(cloud_files))

    def _download(cloud_path, local_path):
        shutil.copyfile(cloud_files[cloud_path], local_path)

    def _upload(local_path, cloud_path):
        del cloud_path
        shutil.copyfile(local_path, uploaded)

    monkeypatch.setattr(utils, 'download_file_from_cloud', _download)
    monkeypatch.setattr(utils, 'upload_file_to_cloud', _upload)
    monkeypatch.setattr(
        utils, 'load_jsonl_from_cloud',
        mock.Mock(side_effect=AssertionError('must not load batches into RAM')))

    utils.concatenate_batches_to_output('s3://bucket/output.jsonl', 'job-1')

    assert uploaded.read_bytes() == first.read_bytes() + second.read_bytes()


def test_s3_attempt_cleanup_deletes_one_page_at_a_time(monkeypatch):
    paginator = mock.Mock()
    paginator.paginate.return_value = [{
        'Contents': [{
            'Key': 'tmp/a'
        }, {
            'Key': 'tmp/b'
        }]
    }, {
        'Contents': [{
            'Key': 'tmp/c'
        }]
    }]
    s3 = mock.Mock()
    s3.get_paginator.return_value = paginator
    monkeypatch.setattr(utils.aws, 'client', mock.Mock(return_value=s3))

    utils.delete_cloud_prefix('s3://bucket/tmp/')

    assert s3.delete_objects.call_count == 2
    s3.delete_objects.assert_any_call(Bucket='bucket',
                                      Delete={
                                          'Objects': [{
                                              'Key': 'tmp/a'
                                          }, {
                                              'Key': 'tmp/b'
                                          }],
                                          'Quiet': True,
                                      })


def test_dispatch_propagates_worker_thread_start_failure(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator.batches = [[0, 3]]
    batch_coordinator._workers = ['worker-a']
    batch_coordinator._enqueue_batch(0)
    monkeypatch.setattr(batch_coordinator, '_reclaim_expired_batches',
                        mock.Mock(return_value=0))
    monkeypatch.setattr(batch_coordinator, '_assert_coordinator_owner',
                        mock.Mock())
    monkeypatch.setattr(batch_coordinator, '_cleanup_stale_worker_services',
                        mock.Mock())
    monkeypatch.setattr(batch_coordinator, '_sync_batch_progress_from_db',
                        mock.Mock(return_value=(0, set(), [])))
    monkeypatch.setattr(batch_coordinator, '_get_ready_workers',
                        mock.Mock(return_value=['worker-a']))
    broken_thread = mock.Mock()
    broken_thread.start.side_effect = RuntimeError("can't start new thread")
    monkeypatch.setattr(coordinator.threading, 'Thread',
                        mock.Mock(return_value=broken_thread))
    monkeypatch.setattr(coordinator.time, 'sleep', mock.Mock())

    with pytest.raises(RuntimeError, match="can't start new thread"):
        batch_coordinator._dispatch_all()

    # The failure must surface on the first attempt instead of being
    # swallowed into a silent retry loop.
    assert broken_thread.start.call_count == 1


def test_dispatch_discovery_failure_waits_for_live_lease(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator.batches = [[0, 3]]
    batch_coordinator._workers = ['worker-a']
    batch_coordinator._enqueue_batch(0)
    monkeypatch.setattr(batch_coordinator, '_reclaim_expired_batches',
                        mock.Mock(return_value=0))
    monkeypatch.setattr(batch_coordinator, '_assert_coordinator_owner',
                        mock.Mock())
    monkeypatch.setattr(batch_coordinator, '_cleanup_stale_worker_services',
                        mock.Mock())
    progress = mock.Mock(side_effect=[
        (0, {'worker-a'}, []),
        (1, set(), []),
        (1, set(), []),
    ])
    monkeypatch.setattr(batch_coordinator, '_sync_batch_progress_from_db',
                        progress)
    discovery = mock.Mock(side_effect=RuntimeError('transient pool error'))
    monkeypatch.setattr(batch_coordinator, '_get_ready_workers', discovery)
    dispatch = mock.Mock()
    monkeypatch.setattr(batch_coordinator, '_worker_dispatch_loop', dispatch)
    sleep = mock.Mock()
    monkeypatch.setattr(coordinator.time, 'sleep', sleep)

    # A live durable lease means another incarnation may still finish the
    # work: the discovery failure must not abort the pass.
    batch_coordinator._dispatch_all()

    dispatch.assert_not_called()
    assert sleep.called
    assert progress.call_count >= 2
