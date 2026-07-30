"""Managed-job scheduler ownership tests for split controller roles."""
# pylint: disable=protected-access

import asyncio
import contextlib

import filelock
import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from sky.jobs import state


@pytest.fixture
def _mock_managed_jobs_db_conn(tmp_path, monkeypatch):
    """Create an isolated managed-jobs database with sync and async engines."""
    db_path = tmp_path / 'managed_jobs_controller_ownership.db'
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
        yield engine
    finally:
        asyncio.run(async_engine.dispose())
        engine.dispose()


class TestManagedJobControllerOwnership:
    """Outer controller generation must be part of a scheduler claim."""

    @staticmethod
    def _seed_waiting_job():
        job_id = state.set_job_info_without_job_id(name='generation-fenced',
                                                   workspace='ws1',
                                                   entrypoint='ep',
                                                   pool=None,
                                                   pool_hash=None,
                                                   user_hash='u1')
        state.set_pending(job_id,
                          task_id=0,
                          task_name='t0',
                          resources_str='{}',
                          metadata='{}')
        state.scheduler_set_waiting([job_id], '/tmp/d.yaml', '/tmp/u.yaml',
                                    '/tmp/e', None, 100)
        return job_id

    def test_claim_persists_outer_instance_and_generation(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        job_id = self._seed_waiting_job()
        owner = ('2bc9ae9e-3871-4cb1-a15d-c4557b1daaa1', 41)
        observed_owners = []

        async def _record_lock(_session, observed_owner):
            observed_owners.append(observed_owner)

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: owner)
        monkeypatch.setattr(state, '_lock_current_controller_owner_async',
                            _record_lock)

        claimed = asyncio.run(
            state.get_waiting_job_async(pid=4242, pid_started_at=1.5))

        assert claimed == {'job_id': job_id, 'pool': None}
        assert observed_owners == [owner]
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            row = session.execute(
                sqlalchemy.select(
                    state.job_info_table.c.controller_pid,
                    state.job_info_table.c.controller_pid_started_at,
                    state.job_info_table.c.controller_instance_id,
                    state.job_info_table.c.controller_generation,
                ).where(state.job_info_table.c.spot_job_id == job_id)).one()
        assert tuple(row) == (4242, 1.5, owner[0], owner[1])

    def test_lost_outer_generation_cannot_claim_waiting_job(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        job_id = self._seed_waiting_job()
        owner = ('843270f6-3c89-4af1-b176-f63992b025d3', 7)

        async def _reject_stale_lock(_session, _owner):
            raise state.ControllerLeadershipLostError('stale generation')

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: owner)
        monkeypatch.setattr(state, '_lock_current_controller_owner_async',
                            _reject_stale_lock)

        with pytest.raises(state.ControllerLeadershipLostError,
                           match='stale generation'):
            asyncio.run(
                state.get_waiting_job_async(pid=4242, pid_started_at=1.5))

        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            row = session.execute(
                sqlalchemy.select(
                    state.job_info_table.c.schedule_state,
                    state.job_info_table.c.controller_pid,
                    state.job_info_table.c.controller_instance_id,
                    state.job_info_table.c.controller_generation,
                ).where(state.job_info_table.c.spot_job_id == job_id)).one()
        assert tuple(row) == (
            state.ManagedJobScheduleState.WAITING.value,
            None,
            None,
            None,
        )

    def test_recovery_resets_only_stale_outer_owner(self,
                                                    _mock_managed_jobs_db_conn,
                                                    monkeypatch):
        stale_job_id = self._seed_waiting_job()
        current_job_id = self._seed_waiting_job()
        current_owner = ('96d9d1f6-8ba4-402b-85f5-27db321fd504', 22)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == stale_job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value,
                    controller_pid=111,
                    controller_pid_started_at=1.0,
                    controller_instance_id='old-instance',
                    controller_generation=21))
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == current_job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value,
                    controller_pid=222,
                    controller_pid_started_at=2.0,
                    controller_instance_id=current_owner[0],
                    controller_generation=current_owner[1]))
            session.commit()

        observed_owners = []
        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: current_owner)
        monkeypatch.setattr(
            state, '_lock_current_controller_owner',
            lambda _session, owner: observed_owners.append(owner))

        assert state.reset_stale_jobs_for_current_controller() == 1

        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            rows = session.execute(
                sqlalchemy.select(
                    state.job_info_table.c.spot_job_id,
                    state.job_info_table.c.schedule_state,
                    state.job_info_table.c.controller_pid,
                    state.job_info_table.c.controller_instance_id,
                    state.job_info_table.c.controller_generation,
                ).where(
                    state.job_info_table.c.spot_job_id.in_([
                        stale_job_id, current_job_id
                    ])).order_by(state.job_info_table.c.spot_job_id)).all()
        assert observed_owners == [current_owner]
        assert tuple(rows[0]) == (
            stale_job_id,
            state.ManagedJobScheduleState.WAITING.value,
            None,
            None,
            None,
        )
        assert tuple(rows[1]) == (
            current_job_id,
            state.ManagedJobScheduleState.ALIVE.value,
            222,
            current_owner[0],
            current_owner[1],
        )

    def test_recovery_preserves_terminal_stale_owner_for_cleanup(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        job_id = self._seed_waiting_job()
        current_owner = ('96d9d1f6-8ba4-402b-85f5-27db321fd504', 22)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value,
                    controller_pid=111,
                    controller_pid_started_at=1.0,
                    controller_instance_id='old-instance',
                    controller_generation=21))
            session.execute(state.spot_table.update().where(
                state.spot_table.c.spot_job_id == job_id).values(
                    status=state.ManagedJobStatus.SUCCEEDED.value, end_at=10.0))
            session.commit()

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: current_owner)
        monkeypatch.setattr(state, '_lock_current_controller_owner',
                            lambda _session, _owner: None)

        assert state.reset_stale_jobs_for_current_controller() == 0
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            row = session.execute(
                sqlalchemy.select(
                    state.job_info_table.c.schedule_state,
                    state.job_info_table.c.controller_instance_id,
                    state.job_info_table.c.controller_generation,
                ).where(state.job_info_table.c.spot_job_id == job_id)).one()
        assert tuple(row) == (
            state.ManagedJobScheduleState.ALIVE.value,
            'old-instance',
            21,
        )

    def test_failure_terminalizes_before_cleanup_and_then_finishes(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        job_id = self._seed_waiting_job()
        owner = ('96d9d1f6-8ba4-402b-85f5-27db321fd504', 22)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value,
                    controller_pid=111,
                    controller_pid_started_at=1.0,
                    controller_instance_id=owner[0],
                    controller_generation=owner[1]))
            session.execute(state.spot_table.update().where(
                state.spot_table.c.spot_job_id == job_id).values(
                    status=state.ManagedJobStatus.RUNNING.value))
            session.commit()

        observed_owners = []
        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: owner)
        monkeypatch.setattr(
            state, '_lock_current_controller_owner',
            lambda _session, observed: observed_owners.append(observed))
        snapshot = {
            'schedule_state': state.ManagedJobScheduleState.ALIVE,
            'controller_pid': 111,
            'controller_pid_started_at': 1.0,
            'controller_instance_id': owner[0],
            'controller_generation': owner[1],
        }

        assert state.set_failed_controller_if_current_snapshot(
            job_id, **snapshot, failure_reason='controller died')
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            job_row = session.execute(
                sqlalchemy.select(state.job_info_table.c.schedule_state).where(
                    state.job_info_table.c.spot_job_id == job_id)).one()
            task_row = session.execute(
                sqlalchemy.select(state.spot_table.c.status).where(
                    state.spot_table.c.spot_job_id == job_id)).one()
        assert job_row.schedule_state == (
            state.ManagedJobScheduleState.ALIVE.value)
        assert task_row.status == (
            state.ManagedJobStatus.FAILED_CONTROLLER.value)

        assert state.finish_controller_cleanup_if_current_snapshot(
            job_id, **snapshot)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            job_row = session.execute(
                sqlalchemy.select(state.job_info_table.c.schedule_state).where(
                    state.job_info_table.c.spot_job_id == job_id)).one()
        assert job_row.schedule_state == (
            state.ManagedJobScheduleState.DONE.value)
        assert observed_owners == [owner, owner]

    def test_replacement_finishes_terminal_cleanup_from_stale_owner(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        job_id = self._seed_waiting_job()
        old_owner = ('96d9d1f6-8ba4-402b-85f5-27db321fd501', 21)
        current_owner = ('96d9d1f6-8ba4-402b-85f5-27db321fd504', 22)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value,
                    controller_pid=111,
                    controller_pid_started_at=1.0,
                    controller_instance_id=old_owner[0],
                    controller_generation=old_owner[1]))
            session.execute(state.spot_table.update().where(
                state.spot_table.c.spot_job_id == job_id).values(
                    status=state.ManagedJobStatus.SUCCEEDED.value, end_at=10.0))
            session.commit()

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: current_owner)
        monkeypatch.setattr(state, '_lock_current_controller_owner',
                            lambda _session, _owner: None)

        assert state.finish_controller_cleanup_if_current_snapshot(
            job_id,
            schedule_state=state.ManagedJobScheduleState.ALIVE,
            controller_pid=111,
            controller_pid_started_at=1.0,
            controller_instance_id=old_owner[0],
            controller_generation=old_owner[1])
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            row = session.execute(
                sqlalchemy.select(
                    state.job_info_table.c.schedule_state,
                    state.job_info_table.c.controller_instance_id,
                    state.job_info_table.c.controller_generation,
                ).where(state.job_info_table.c.spot_job_id == job_id)).one()
        assert tuple(row) == (
            state.ManagedJobScheduleState.DONE.value,
            current_owner[0],
            current_owner[1],
        )
