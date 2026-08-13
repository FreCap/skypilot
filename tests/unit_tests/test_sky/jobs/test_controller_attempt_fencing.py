"""Exact-attempt fencing tests for managed-job task mutations."""

import asyncio
import contextlib
import os
import threading
import time

import filelock
import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import create_async_engine

from sky.jobs import constants as managed_job_constants
from sky.jobs import controller_fencing
from sky.jobs import state

_OWNER = ('12345678-1234-4234-8234-123456789abc', 17)
_SLOT_ID = 3
_CURRENT_ATTEMPT = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
_STALE_ATTEMPT = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'


@pytest.mark.parametrize(('values', 'expected'), [
    ({
        'controller_instance_id': None,
        'controller_generation': None,
        'controller_slot_id': None,
        'controller_slot_attempt': None,
    }, None),
    ({
        'controller_instance_id': _OWNER[0],
        'controller_generation': _OWNER[1] - 1,
        'controller_slot_id': None,
        'controller_slot_attempt': None,
    }, None),
    ({
        'controller_instance_id': _OWNER[0],
        'controller_generation': _OWNER[1] - 1,
        'controller_slot_id': _SLOT_ID,
        'controller_slot_attempt': _CURRENT_ATTEMPT,
    }, (_OWNER[0], _OWNER[1] - 1, _SLOT_ID, _CURRENT_ATTEMPT)),
])
def test_persisted_job_attempt_identity_recognizes_exact_and_legacy(
        values, expected):
    assert controller_fencing.persisted_job_attempt_identity(values,
                                                             _OWNER) == expected


@pytest.mark.parametrize('values', [
    {
        'controller_instance_id': _OWNER[0],
        'controller_generation': _OWNER[1],
        'controller_slot_id': None,
        'controller_slot_attempt': None,
    },
    {
        'controller_instance_id': _OWNER[0],
        'controller_generation': _OWNER[1] - 1,
        'controller_slot_id': _SLOT_ID,
        'controller_slot_attempt': None,
    },
    {
        'controller_instance_id': _OWNER[0],
        'controller_generation': None,
        'controller_slot_id': None,
        'controller_slot_attempt': None,
    },
])
def test_persisted_job_attempt_identity_rejects_unsafe_partial_or_current(
        values):
    with pytest.raises(ValueError):
        controller_fencing.persisted_job_attempt_identity(values, _OWNER)


@pytest.fixture
def managed_jobs_db(tmp_path, monkeypatch):
    """Create an isolated managed-jobs database with sync/async engines."""
    db_path = tmp_path / 'managed_jobs_attempt_fencing.db'
    engine = create_engine(f'sqlite:///{db_path}', connect_args={'timeout': 5})
    async_engine = create_async_engine(f'sqlite+aiosqlite:///{db_path}',
                                       connect_args={'timeout': 5})

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
        yield engine
    finally:
        asyncio.run(async_engine.dispose())
        engine.dispose()


def _publish_local_slot(monkeypatch, attempt: str) -> None:
    pid = os.getpid()
    monkeypatch.setenv(managed_job_constants.CONTROLLER_OWNER_MODE_ENV_VAR,
                       controller_fencing.LOCAL_OWNER_MODE)
    monkeypatch.setenv(
        managed_job_constants.CONTROLLER_OWNER_INSTANCE_ID_ENV_VAR, _OWNER[0])
    monkeypatch.setenv(
        managed_job_constants.CONTROLLER_OWNER_GENERATION_ENV_VAR,
        str(_OWNER[1]))
    monkeypatch.setenv(managed_job_constants.CONTROLLER_OWNER_PID_ENV_VAR,
                       str(pid))
    monkeypatch.setenv(
        managed_job_constants.CONTROLLER_OWNER_START_TICKS_ENV_VAR,
        str(controller_fencing._read_process_start_time_ticks(pid)))
    monkeypatch.setenv(managed_job_constants.CONTROLLER_SLOT_ID_ENV_VAR,
                       str(_SLOT_ID))
    monkeypatch.setenv(managed_job_constants.CONTROLLER_SLOT_ATTEMPT_ENV_VAR,
                       attempt)


def _seed_starting_task(engine, *, attempt: str = _CURRENT_ATTEMPT) -> int:
    job_id = 41
    with orm.Session(engine) as session:
        session.execute(state.job_info_table.insert().values(
            spot_job_id=job_id,
            name='fenced-job',
            schedule_state=state.ManagedJobScheduleState.ALIVE.value,
            controller_instance_id=_OWNER[0],
            controller_generation=_OWNER[1],
            controller_slot_id=_SLOT_ID,
            controller_slot_attempt=attempt))
        session.execute(state.spot_table.insert().values(
            spot_job_id=job_id,
            task_id=0,
            task_name='task-0',
            status=state.ManagedJobStatus.STARTING.value))
        session.commit()
    return job_id


def _task_status_and_events(engine, job_id: int):
    with orm.Session(engine) as session:
        status = session.execute(
            sqlalchemy.select(state.spot_table.c.status).where(
                sqlalchemy.and_(state.spot_table.c.spot_job_id == job_id,
                                state.spot_table.c.task_id == 0))).scalar_one()
        events = session.execute(
            sqlalchemy.select(
                state.job_events_table.c.new_status,
                state.job_events_table.c.reason).where(
                    state.job_events_table.c.spot_job_id == job_id)).all()
    return status, events


def test_stale_attempt_cannot_mutate_task_or_emit_event(managed_jobs_db,
                                                        monkeypatch):
    job_id = _seed_starting_task(managed_jobs_db)
    _publish_local_slot(monkeypatch, _STALE_ATTEMPT)
    callbacks = []

    async def callback(status: str) -> None:
        callbacks.append(status)

    with pytest.raises(controller_fencing.ControllerLeadershipLostError,
                       match='no longer owned'):
        asyncio.run(
            state.set_started_async(job_id,
                                    task_id=0,
                                    start_time=123.0,
                                    callback_func=callback))

    status, events = _task_status_and_events(managed_jobs_db, job_id)
    assert status == state.ManagedJobStatus.STARTING.value
    assert events == []
    assert callbacks == []


def test_current_attempt_commits_status_and_event_together(
        managed_jobs_db, monkeypatch):
    job_id = _seed_starting_task(managed_jobs_db)
    _publish_local_slot(monkeypatch, _CURRENT_ATTEMPT)
    callbacks = []

    async def callback(status: str) -> None:
        callbacks.append(status)

    asyncio.run(
        state.set_started_async(job_id,
                                task_id=0,
                                start_time=123.0,
                                callback_func=callback))

    status, events = _task_status_and_events(managed_jobs_db, job_id)
    assert status == state.ManagedJobStatus.RUNNING.value
    assert events == [(state.ManagedJobStatus.RUNNING.value, 'Job has started')]
    assert callbacks == ['STARTED']


def test_event_insert_failure_rolls_back_current_attempt_status(
        managed_jobs_db, monkeypatch):
    job_id = _seed_starting_task(managed_jobs_db)
    _publish_local_slot(monkeypatch, _CURRENT_ATTEMPT)
    callbacks = []

    async def callback(status: str) -> None:
        callbacks.append(status)

    def fail_event_insert(*_args, **_kwargs):
        raise RuntimeError('event insert failed')

    monkeypatch.setattr(state.state_events, 'job_event_insert_statement',
                        fail_event_insert)
    with pytest.raises(RuntimeError, match='event insert failed'):
        asyncio.run(
            state.set_started_async(job_id,
                                    task_id=0,
                                    start_time=123.0,
                                    callback_func=callback))

    status, events = _task_status_and_events(managed_jobs_db, job_id)
    assert status == state.ManagedJobStatus.STARTING.value
    assert events == []
    assert callbacks == []


def test_local_sqlite_exact_attempt_check_serializes_writers(
        managed_jobs_db, monkeypatch):
    """The SQLite ownership check must hold a write lock until commit."""
    job_id = _seed_starting_task(managed_jobs_db)
    _publish_local_slot(monkeypatch, _CURRENT_ATTEMPT)
    rotate_started = threading.Event()
    rotate_finished = threading.Event()
    rotate_errors = []

    def rotate_attempt() -> None:
        try:
            with orm.Session(managed_jobs_db) as session:
                rotate_started.set()
                session.execute(
                    sqlalchemy.update(state.job_info_table).where(
                        state.job_info_table.c.spot_job_id == job_id).values(
                            controller_slot_attempt=_STALE_ATTEMPT))
                session.commit()
        except BaseException as error:  # pylint: disable=broad-except
            rotate_errors.append(error)
        finally:
            rotate_finished.set()

    with orm.Session(managed_jobs_db) as session:
        controller_fencing.lock_current_job_attempt(
            session, job_id, (*_OWNER, _SLOT_ID, _CURRENT_ATTEMPT))
        thread = threading.Thread(target=rotate_attempt)
        thread.start()
        assert rotate_started.wait(timeout=5)
        # The exact-attempt check is a no-op UPDATE on SQLite. Holding this
        # transaction open must nevertheless prevent an ownership rotation
        # from committing between the check and the caller's fenced write.
        time.sleep(0.2)
        assert not rotate_finished.is_set()
        session.commit()

    thread.join(timeout=5)
    assert not thread.is_alive()
    assert rotate_errors == []
    assert rotate_finished.is_set()
    with orm.Session(managed_jobs_db) as session:
        attempt = session.execute(
            sqlalchemy.select(
                state.job_info_table.c.controller_slot_attempt).where(
                    state.job_info_table.c.spot_job_id == job_id)).scalar_one()
    assert attempt == _STALE_ATTEMPT
