"""Characterization tests for managed-job initial-row registration."""
# pylint: disable=protected-access,redefined-outer-name

import contextlib
import inspect
import pickle
import types

import filelock
import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky.jobs import state
from sky.jobs import state_job_registration


@pytest.fixture
def managed_jobs_db(tmp_path, monkeypatch):
    """Provide an isolated SQLite database for registration."""
    db_path = tmp_path / 'managed_jobs.db'
    engine = create_engine(f'sqlite:///{db_path}')

    @contextlib.contextmanager
    def _tmp_db_lock(section: str):
        lock_path = tmp_path / f'.{section}.lock'
        with filelock.FileLock(str(lock_path), timeout=10):
            yield

    monkeypatch.setattr(state.migration_utils, 'db_lock', _tmp_db_lock)
    monkeypatch.setattr(state._db_manager, '_engine', engine)
    state.create_table(engine)
    try:
        yield engine
    finally:
        engine.dispose()


@contextlib.contextmanager
def _count_registration_work(engine):
    counts = {'statements': 0, 'commits': 0}

    def _count_statement(*_args, **_kwargs):
        counts['statements'] += 1

    def _count_commit(*_args, **_kwargs):
        counts['commits'] += 1

    event.listen(engine, 'before_cursor_execute', _count_statement)
    event.listen(orm.Session, 'after_commit', _count_commit)
    try:
        yield counts
    finally:
        event.remove(engine, 'before_cursor_execute', _count_statement)
        event.remove(orm.Session, 'after_commit', _count_commit)


def _row(engine, job_id: int):
    with orm.Session(engine) as session:
        return session.execute(
            sqlalchemy.select(state.job_info_table).where(
                state.job_info_table.c.spot_job_id == job_id)).one()


def test_job_registration_public_contract():
    assert state.sqlite is sqlite
    assert state.postgresql is postgresql
    expected_signatures = {
        'set_job_info_without_job_id':
            '(name: str, workspace: str, entrypoint: str, pool: str | None, '
            'pool_hash: str | None, user_hash: str | None, execution: str | '
            'None = None, is_batch: bool = False, file_mounts_blob_id: str | '
            'None = None) -> int',
        'set_job_info':
            '(job_id: int, name: str, workspace: str, entrypoint: str, pool: '
            'str | None, pool_hash: str | None, user_hash: str | None = None, '
            'execution: str | None = None, is_batch: bool = False)',
    }
    for name, expected_signature in expected_signatures.items():
        function = getattr(state, name)
        assert function is getattr(state_job_registration, name)
        assert function.__name__ == name
        assert function.__module__ == 'sky.jobs.state'
        assert str(inspect.signature(function)) == expected_signature
        assert pickle.loads(pickle.dumps(function)) is function


def test_database_assigned_registration_row_and_budgets(managed_jobs_db):
    engine = managed_jobs_db
    with _count_registration_work(engine) as counts:
        job_id = state.set_job_info_without_job_id(
            name='database-assigned',
            workspace='workspace-a',
            entrypoint='echo assigned',
            pool='pool-a',
            pool_hash='pool-hash-a',
            user_hash='user-hash-a',
            execution='parallel',
            is_batch=True,
            file_mounts_blob_id='blob-a',
        )
    assert counts == {'statements': 1, 'commits': 1}
    assert isinstance(job_id, int)

    row = _row(engine, job_id)
    assert row.spot_job_id == job_id
    assert row.name == 'database-assigned'
    assert row.schedule_state == state.ManagedJobScheduleState.INACTIVE.value
    assert row.workspace == 'workspace-a'
    assert row.entrypoint == 'echo assigned'
    assert row.pool == 'pool-a'
    assert row.pool_hash == 'pool-hash-a'
    assert row.user_hash == 'user-hash-a'
    assert row.execution == 'parallel'
    assert row.is_batch is True
    assert row.file_mounts_blob_id == 'blob-a'


def test_preallocated_registration_row_budgets_and_duplicate_failure(
        managed_jobs_db):
    engine = managed_jobs_db
    with _count_registration_work(engine) as counts:
        result = state.set_job_info(
            4242,
            name='preallocated',
            workspace='workspace-b',
            entrypoint='echo preallocated',
            pool='pool-b',
            pool_hash='pool-hash-b',
            user_hash='user-hash-b',
            execution='serial',
            is_batch=False,
        )
    assert result is None
    assert counts == {'statements': 1, 'commits': 1}

    row = _row(engine, 4242)
    assert row.name == 'preallocated'
    assert row.schedule_state == state.ManagedJobScheduleState.INACTIVE.value
    assert row.workspace == 'workspace-b'
    assert row.entrypoint == 'echo preallocated'
    assert row.pool == 'pool-b'
    assert row.pool_hash == 'pool-hash-b'
    assert row.user_hash == 'user-hash-b'
    assert row.execution == 'serial'
    assert row.is_batch is False
    assert row.file_mounts_blob_id is None

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        state.set_job_info(
            4242,
            name='duplicate',
            workspace='workspace-c',
            entrypoint='echo duplicate',
            pool=None,
            pool_hash=None,
        )
    assert _row(engine, 4242).name == 'preallocated'


class _Result:
    """Minimal SQLAlchemy result used by dialect characterization."""

    lastrowid = 7001

    def scalar(self):
        return 7002


class _RecordingSession:
    """Record statements and commits without opening a database."""

    def __init__(self, _engine, statements, commits):
        self._statements = statements
        self._commits = commits

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        return False

    def execute(self, statement):
        self._statements.append(statement)
        return _Result()

    def commit(self):
        self._commits.append(None)


def test_postgresql_registration_statement_shapes(monkeypatch):
    engine = types.SimpleNamespace(dialect=postgresql.dialect())
    statements = []
    commits = []
    monkeypatch.setattr(state._db_manager, 'get_engine', lambda: engine)
    monkeypatch.setattr(
        state.orm, 'Session', lambda bound_engine: _RecordingSession(
            bound_engine, statements, commits))

    assert state.set_job_info_without_job_id(
        name='database-assigned',
        workspace='workspace',
        entrypoint='echo assigned',
        pool=None,
        pool_hash=None,
        user_hash=None,
    ) == 7002
    assigned_sql = str(statements.pop().compile(dialect=engine.dialect))
    assert 'RETURNING job_info.spot_job_id' in assigned_sql

    assert state.set_job_info(
        7003,
        name='preallocated',
        workspace='workspace',
        entrypoint='echo preallocated',
        pool=None,
        pool_hash=None,
    ) is None
    preallocated_sql = str(statements.pop().compile(dialect=engine.dialect))
    assert 'RETURNING' not in preallocated_sql
    assert commits == [None, None]


def test_unsupported_registration_dialect(monkeypatch):
    engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name='other'))
    statements = []
    commits = []
    monkeypatch.setattr(state._db_manager, 'get_engine', lambda: engine)
    monkeypatch.setattr(
        state.orm, 'Session', lambda bound_engine: _RecordingSession(
            bound_engine, statements, commits))

    with pytest.raises(ValueError, match='Unsupported database dialect'):
        state.set_job_info_without_job_id(
            name='unsupported',
            workspace='workspace',
            entrypoint='echo unsupported',
            pool=None,
            pool_hash=None,
            user_hash=None,
        )
    with pytest.raises(ValueError, match='Unsupported database dialect'):
        state.set_job_info(
            7004,
            name='unsupported',
            workspace='workspace',
            entrypoint='echo unsupported',
            pool=None,
            pool_hash=None,
        )
    assert not statements
    assert not commits
