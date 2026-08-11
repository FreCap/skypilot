"""Characterization tests for managed-job file-mount blob references."""
# pylint: disable=protected-access,redefined-outer-name

import asyncio
import contextlib
import inspect
import pickle

import filelock
import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import create_async_engine

from sky.jobs import state
from sky.jobs import state_file_mount_blobs
from sky.jobs.status_types import ManagedJobStatus


@pytest.fixture
def managed_jobs_db(tmp_path, monkeypatch):
    """Provide one isolated database to sync and async state functions."""
    db_path = tmp_path / 'managed_jobs.db'
    engine = create_engine(f'sqlite:///{db_path}')
    async_engine = create_async_engine(f'sqlite+aiosqlite:///{db_path}',
                                       connect_args={'timeout': 30})

    @contextlib.contextmanager
    def _tmp_db_lock(section: str):
        lock_path = tmp_path / f'.{section}.lock'
        with filelock.FileLock(str(lock_path), timeout=10):
            yield

    monkeypatch.setattr(state.migration_utils, 'db_lock', _tmp_db_lock)
    monkeypatch.setattr(state._db_manager, '_engine', engine)
    monkeypatch.setattr(state._db_manager, '_engine_async', async_engine)
    state.create_table(engine)
    try:
        yield engine, async_engine
    finally:
        asyncio.run(async_engine.dispose())
        engine.dispose()


@contextlib.contextmanager
def _count_sql_statements(engine):
    counts = {'n': 0}

    def _count(*_args, **_kwargs):
        counts['n'] += 1

    event.listen(engine, 'before_cursor_execute', _count)
    try:
        yield counts
    finally:
        event.remove(engine, 'before_cursor_execute', _count)


def _new_job(engine, blob_id: str | None, *statuses:
             ManagedJobStatus | None) -> int:
    job_id = state.set_job_info_without_job_id(
        name='job',
        workspace='workspace',
        entrypoint='echo hi',
        pool=None,
        pool_hash=None,
        user_hash='user-hash',
        file_mounts_blob_id=blob_id,
    )
    with orm.Session(engine) as session:
        for task_id, status in enumerate(statuses):
            session.execute(
                sqlalchemy.insert(state.spot_table).values(
                    spot_job_id=job_id,
                    task_id=task_id,
                    task_name=f'task-{task_id}',
                    status=None if status is None else status.value,
                ))
        session.commit()
    return job_id


def test_file_mount_blob_public_contract():
    expected_signatures = {
        'get_active_file_mounts_blob_ids': '() -> set[str]',
        'get_file_mounts_blob_id_async': '(job_id: int) -> str | None',
    }
    for name, expected_signature in expected_signatures.items():
        function = getattr(state, name)
        assert function is getattr(state_file_mount_blobs, name)
        assert function.__name__ == name
        assert function.__module__ == 'sky.jobs.state'
        assert str(inspect.signature(function)) == expected_signature
        assert pickle.loads(pickle.dumps(function)) is function

    async_reader = state.get_file_mounts_blob_id_async
    assert async_reader.__wrapped__.__name__ == async_reader.__name__
    assert async_reader.__wrapped__.__module__ == 'sky.jobs.state'


@pytest.mark.asyncio
async def test_file_mount_blob_reads_and_query_budgets(managed_jobs_db):
    engine, async_engine = managed_jobs_db
    active_job = _new_job(engine, 'blob-active', ManagedJobStatus.SUCCEEDED,
                          ManagedJobStatus.RECOVERING)
    pending_job = _new_job(engine, 'blob-pending', ManagedJobStatus.PENDING,
                           ManagedJobStatus.PENDING)
    _new_job(engine, 'blob-null-status', None)
    _new_job(engine, 'blob-terminal', ManagedJobStatus.SUCCEEDED,
             ManagedJobStatus.CANCELLED)
    _new_job(engine, None, ManagedJobStatus.RUNNING)
    _new_job(engine, 'blob-without-task')

    with _count_sql_statements(engine) as counts:
        assert state.get_active_file_mounts_blob_ids() == {
            'blob-active',
            'blob-pending',
            'blob-null-status',
        }
    assert counts['n'] == 1

    with _count_sql_statements(async_engine.sync_engine) as counts:
        assert await state.get_file_mounts_blob_id_async(active_job
                                                        ) == 'blob-active'
    assert counts['n'] == 1

    with _count_sql_statements(async_engine.sync_engine) as counts:
        assert await state.get_file_mounts_blob_id_async(pending_job) == (
            'blob-pending')
    assert counts['n'] == 1

    with _count_sql_statements(async_engine.sync_engine) as counts:
        assert await state.get_file_mounts_blob_id_async(999_999) is None
    assert counts['n'] == 1
