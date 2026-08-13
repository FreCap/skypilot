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

_SLOT_ID = 3
_SLOT_ATTEMPT = '12345678-1234-4234-8234-123456789abc'
_OLD_OWNER_INSTANCE_ID = '12345678-1234-4234-8234-123456789abd'


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


def _forbid_task_row_materialization(monkeypatch):
    """Fail if an exact recheck materializes task rows instead of aggregates."""
    original_execute = state.orm.Session.execute

    class _ScalarResultProxy:
        """Reject scalar result materialization for exact task rechecks."""

        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def all(self):
            raise AssertionError(
                'exact task rechecks must not materialize every task row')

    class _ResultProxy:
        """Reject row materialization for exact task rechecks."""

        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def all(self):
            raise AssertionError(
                'exact task rechecks must not materialize every task row')

        def scalars(self):
            return _ScalarResultProxy(self._inner.scalars())

    def _wrap_if_task_select(self, statement, *args, **kwargs):
        result = original_execute(self, statement, *args, **kwargs)
        if isinstance(statement, sqlalchemy.sql.Select):
            sql_text = str(statement)
            if 'FROM spot' in sql_text and 'JOIN' not in sql_text:
                return _ResultProxy(result)
        return result

    monkeypatch.setattr(state.orm.Session, 'execute', _wrap_if_task_select)


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
        monkeypatch.setattr(state, 'get_current_controller_slot_identity',
                            lambda: (*owner, _SLOT_ID, _SLOT_ATTEMPT))
        monkeypatch.setattr(state, '_lock_current_controller_owner_async',
                            _record_lock)

        claimed = asyncio.run(
            state.get_waiting_job_async(pid=4242,
                                        pid_started_at=1.5,
                                        controller_slot_id=_SLOT_ID,
                                        controller_slot_attempt=_SLOT_ATTEMPT))

        assert claimed == {
            'job_id': job_id,
            'pool': None,
            'cleanup_only': False,
        }
        assert observed_owners == [owner]
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            row = session.execute(
                sqlalchemy.select(
                    state.job_info_table.c.controller_pid,
                    state.job_info_table.c.controller_pid_started_at,
                    state.job_info_table.c.controller_instance_id,
                    state.job_info_table.c.controller_generation,
                    state.job_info_table.c.controller_slot_id,
                    state.job_info_table.c.controller_slot_attempt,
                ).where(state.job_info_table.c.spot_job_id == job_id)).one()
        assert tuple(row) == (4242, 1.5, owner[0], owner[1], _SLOT_ID,
                              _SLOT_ATTEMPT)

    def test_lost_outer_generation_cannot_claim_waiting_job(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        job_id = self._seed_waiting_job()
        owner = ('843270f6-3c89-4af1-b176-f63992b025d3', 7)

        async def _reject_stale_lock(_session, _owner):
            raise state.ControllerLeadershipLostError('stale generation')

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: owner)
        monkeypatch.setattr(state, 'get_current_controller_slot_identity',
                            lambda: (*owner, _SLOT_ID, _SLOT_ATTEMPT))
        monkeypatch.setattr(state, '_lock_current_controller_owner_async',
                            _reject_stale_lock)

        with pytest.raises(state.ControllerLeadershipLostError,
                           match='stale generation'):
            asyncio.run(
                state.get_waiting_job_async(
                    pid=4242,
                    pid_started_at=1.5,
                    controller_slot_id=_SLOT_ID,
                    controller_slot_attempt=_SLOT_ATTEMPT))

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

    def test_terminal_waiting_job_is_claimed_only_for_exact_cleanup(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        job_id = self._seed_waiting_job()
        owner = ('2bc9ae9e-3871-4cb1-a15d-c4557b1daaa1', 41)
        identity = (*owner, _SLOT_ID, _SLOT_ATTEMPT)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.spot_table.update().where(
                state.spot_table.c.spot_job_id == job_id).values(
                    status=state.ManagedJobStatus.SUCCEEDED.value, end_at=10.0))
            session.commit()

        async def _lock_owner(_session, _owner):
            return None

        async def _lock_attempt(_session, _job_id, observed_identity):
            assert observed_identity == identity
            return observed_identity

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: owner)
        monkeypatch.setattr(state, 'get_current_controller_slot_identity',
                            lambda: identity)
        monkeypatch.setattr(state, '_lock_current_controller_owner_async',
                            _lock_owner)
        monkeypatch.setattr(state.controller_fencing,
                            'lock_current_job_attempt_async', _lock_attempt)

        claimed = asyncio.run(
            state.get_waiting_job_async(pid=4242,
                                        pid_started_at=1.5,
                                        controller_slot_id=_SLOT_ID,
                                        controller_slot_attempt=_SLOT_ATTEMPT))

        assert claimed == {
            'job_id': job_id,
            'pool': None,
            'cleanup_only': True,
        }
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    controller_slot_quiescing=True))
            session.commit()
        with pytest.raises(state.ControllerLeadershipLostError,
                           match='no longer an exact cleanup-only claim'):
            asyncio.run(state.scheduler_set_cleanup_done_async(job_id))
        assert (state.get_job_schedule_state(job_id) ==
                state.ManagedJobScheduleState.LAUNCHING)

        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    controller_slot_quiescing=False))
            session.commit()
        asyncio.run(state.scheduler_set_cleanup_done_async(job_id))
        assert (state.get_job_schedule_state(job_id) ==
                state.ManagedJobScheduleState.DONE)
        assert state.get_status(job_id) == state.ManagedJobStatus.SUCCEEDED

    def test_cleanup_done_rejects_nonterminal_claim(self,
                                                    _mock_managed_jobs_db_conn,
                                                    monkeypatch):
        job_id = self._seed_waiting_job()
        owner = ('2bc9ae9e-3871-4cb1-a15d-c4557b1daaa1', 41)
        identity = (*owner, _SLOT_ID, _SLOT_ATTEMPT)

        async def _lock_owner(_session, _owner):
            return None

        async def _lock_attempt(_session, _job_id, observed_identity):
            return observed_identity

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: owner)
        monkeypatch.setattr(state, 'get_current_controller_slot_identity',
                            lambda: identity)
        monkeypatch.setattr(state, '_lock_current_controller_owner_async',
                            _lock_owner)
        monkeypatch.setattr(state.controller_fencing,
                            'lock_current_job_attempt_async', _lock_attempt)

        claimed = asyncio.run(
            state.get_waiting_job_async(pid=4242,
                                        pid_started_at=1.5,
                                        controller_slot_id=_SLOT_ID,
                                        controller_slot_attempt=_SLOT_ATTEMPT))
        assert claimed is not None
        assert claimed['cleanup_only'] is False

        with pytest.raises(state.ControllerLeadershipLostError,
                           match='no longer an exact cleanup-only claim'):
            asyncio.run(state.scheduler_set_cleanup_done_async(job_id))
        assert (state.get_job_schedule_state(job_id) ==
                state.ManagedJobScheduleState.LAUNCHING)

    def test_dead_cleanup_manager_is_re_adopted_without_workload_relaunch(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        job_id = self._seed_waiting_job()
        owner = ('2bc9ae9e-3871-4cb1-a15d-c4557b1daaa1', 41)
        old_identity = (*owner, _SLOT_ID, _SLOT_ATTEMPT)
        replacement_attempt = '12345678-1234-4234-8234-123456789abe'
        replacement_identity = (*owner, _SLOT_ID, replacement_attempt)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.spot_table.update().where(
                state.spot_table.c.spot_job_id == job_id).values(
                    status=state.ManagedJobStatus.SUCCEEDED.value, end_at=10.0))
            session.commit()

        active_identity = [old_identity]

        async def _lock_owner(_session, _owner):
            return None

        async def _lock_attempt(_session, _job_id, observed_identity):
            assert observed_identity == active_identity[0]
            return observed_identity

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: owner)
        monkeypatch.setattr(state, 'get_current_controller_slot_identity',
                            lambda: active_identity[0])
        monkeypatch.setattr(state, '_lock_current_controller_owner_async',
                            _lock_owner)
        monkeypatch.setattr(state, '_lock_current_controller_owner',
                            lambda _session, _owner: None)
        monkeypatch.setattr(state.controller_fencing,
                            'lock_current_job_attempt_async', _lock_attempt)

        first_claim = asyncio.run(
            state.get_waiting_job_async(pid=4242,
                                        pid_started_at=1.5,
                                        controller_slot_id=_SLOT_ID,
                                        controller_slot_attempt=_SLOT_ATTEMPT))
        assert first_claim is not None
        assert first_claim['cleanup_only'] is True

        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    controller_slot_quiescing=True))
            session.commit()
        assert state.reset_jobs_for_controller_slot(old_identity) == 1

        active_identity[0] = replacement_identity
        replacement_claim = asyncio.run(
            state.get_waiting_job_async(
                pid=4343,
                pid_started_at=2.5,
                controller_slot_id=_SLOT_ID,
                controller_slot_attempt=replacement_attempt))
        assert replacement_claim is not None
        assert replacement_claim['cleanup_only'] is True
        assert state.get_status(job_id) == state.ManagedJobStatus.SUCCEEDED

        asyncio.run(state.scheduler_set_cleanup_done_async(job_id))
        assert (state.get_job_schedule_state(job_id) ==
                state.ManagedJobScheduleState.DONE)
        assert state.get_status(job_id) == state.ManagedJobStatus.SUCCEEDED

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
                    controller_instance_id=_OLD_OWNER_INSTANCE_ID,
                    controller_generation=21,
                    controller_slot_id=_SLOT_ID,
                    controller_slot_attempt=_SLOT_ATTEMPT,
                    controller_slot_quiescing=True))
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == current_job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value,
                    controller_pid=222,
                    controller_pid_started_at=2.0,
                    controller_instance_id=current_owner[0],
                    controller_generation=current_owner[1],
                    controller_slot_id=_SLOT_ID,
                    controller_slot_attempt=_SLOT_ATTEMPT))
            session.commit()

        observed_owners = []
        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: current_owner)
        monkeypatch.setattr(
            state, '_lock_current_controller_owner',
            lambda _session, owner: observed_owners.append(owner))
        monkeypatch.setattr(state.api_requests,
                            'quiesce_stale_managed_job_requests',
                            lambda owner: observed_owners.append(owner))

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
        assert observed_owners == [current_owner, current_owner]
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

    def test_recovery_queues_terminal_stale_owner_for_cleanup(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        job_id = self._seed_waiting_job()
        current_owner = ('96d9d1f6-8ba4-402b-85f5-27db321fd504', 22)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value,
                    controller_pid=111,
                    controller_pid_started_at=1.0,
                    controller_instance_id=_OLD_OWNER_INSTANCE_ID,
                    controller_generation=21,
                    controller_slot_id=_SLOT_ID,
                    controller_slot_attempt=_SLOT_ATTEMPT,
                    controller_slot_quiescing=True))
            session.execute(state.spot_table.update().where(
                state.spot_table.c.spot_job_id == job_id).values(
                    status=state.ManagedJobStatus.SUCCEEDED.value, end_at=10.0))
            session.commit()

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: current_owner)
        monkeypatch.setattr(state, '_lock_current_controller_owner',
                            lambda _session, _owner: None)
        monkeypatch.setattr(state.api_requests,
                            'quiesce_stale_managed_job_requests',
                            lambda _owner: None)

        assert state.reset_stale_jobs_for_current_controller() == 1
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            row = session.execute(
                sqlalchemy.select(
                    state.job_info_table.c.schedule_state,
                    state.job_info_table.c.controller_instance_id,
                    state.job_info_table.c.controller_generation,
                    state.job_info_table.c.controller_slot_quiescing,
                ).where(state.job_info_table.c.spot_job_id == job_id)).one()
        assert tuple(row) == (
            state.ManagedJobScheduleState.WAITING.value,
            None,
            None,
            False,
        )

    def test_first_slot_rollout_adopts_only_non_done_legacy_jobs(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        nonterminal_job_id = self._seed_waiting_job()
        terminal_job_id = self._seed_waiting_job()
        done_job_id = self._seed_waiting_job()
        current_owner = ('96d9d1f6-8ba4-402b-85f5-27db321fd504', 22)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id ==
                nonterminal_job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value,
                    controller_pid=111,
                    controller_pid_started_at=1.0,
                    controller_instance_id=_OLD_OWNER_INSTANCE_ID,
                    controller_generation=21,
                    controller_slot_id=None,
                    controller_slot_attempt=None))
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == terminal_job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value,
                    controller_pid=112,
                    controller_pid_started_at=1.1,
                    controller_instance_id=None,
                    controller_generation=None,
                    controller_slot_id=None,
                    controller_slot_attempt=None))
            session.execute(state.spot_table.update().where(
                state.spot_table.c.spot_job_id == terminal_job_id).values(
                    status=state.ManagedJobStatus.SUCCEEDED.value, end_at=10.0))
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == done_job_id).values(
                    schedule_state=state.ManagedJobScheduleState.DONE.value,
                    controller_instance_id=None,
                    controller_generation=None,
                    controller_slot_id=None,
                    controller_slot_attempt=None))
            session.commit()

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: current_owner)
        monkeypatch.setattr(state, '_lock_current_controller_owner',
                            lambda _session, _owner: None)

        observed_plans = []

        def _quiesce(owner):
            plan = state.begin_stale_controller_request_quiescence(owner)
            observed_plans.append(plan)

        monkeypatch.setattr(state.api_requests,
                            'quiesce_stale_managed_job_requests', _quiesce)

        assert state.reset_stale_jobs_for_current_controller() == 2
        assert observed_plans == [
            state.StaleControllerRequestQuiescencePlan(
                exact_identities=(),
                legacy_job_ids=tuple(
                    sorted((nonterminal_job_id, terminal_job_id))))
        ]
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            rows = session.execute(
                sqlalchemy.select(
                    state.job_info_table.c.spot_job_id,
                    state.job_info_table.c.schedule_state,
                    state.job_info_table.c.controller_instance_id,
                    state.job_info_table.c.controller_slot_quiescing,
                ).where(
                    state.job_info_table.c.spot_job_id.in_([
                        nonterminal_job_id, terminal_job_id, done_job_id
                    ])).order_by(state.job_info_table.c.spot_job_id)).all()
            terminal_task_status = session.execute(
                sqlalchemy.select(state.spot_table.c.status).where(
                    state.spot_table.c.spot_job_id ==
                    terminal_job_id)).scalar_one()
        assert tuple(rows[0]) == (
            nonterminal_job_id,
            state.ManagedJobScheduleState.WAITING.value,
            None,
            False,
        )
        assert tuple(rows[1]) == (
            terminal_job_id,
            state.ManagedJobScheduleState.WAITING.value,
            None,
            False,
        )
        assert terminal_task_status == state.ManagedJobStatus.SUCCEEDED.value
        assert tuple(rows[2]) == (
            done_job_id,
            state.ManagedJobScheduleState.DONE.value,
            None,
            False,
        )

    def test_dead_slot_queues_terminal_work_but_never_resets_done(
            self, _mock_managed_jobs_db_conn, monkeypatch):
        cleanup_job_id = self._seed_waiting_job()
        done_job_id = self._seed_waiting_job()
        owner = ('96d9d1f6-8ba4-402b-85f5-27db321fd504', 22)
        identity = (*owner, _SLOT_ID, _SLOT_ATTEMPT)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            for job_id, schedule_state in (
                (cleanup_job_id, state.ManagedJobScheduleState.ALIVE),
                (done_job_id, state.ManagedJobScheduleState.DONE),
            ):
                session.execute(state.job_info_table.update().where(
                    state.job_info_table.c.spot_job_id == job_id).values(
                        schedule_state=schedule_state.value,
                        controller_pid=111,
                        controller_pid_started_at=1.0,
                        controller_instance_id=owner[0],
                        controller_generation=owner[1],
                        controller_slot_id=_SLOT_ID,
                        controller_slot_attempt=_SLOT_ATTEMPT,
                        controller_slot_quiescing=True))
                session.execute(state.spot_table.update().where(
                    state.spot_table.c.spot_job_id == job_id).values(
                        status=state.ManagedJobStatus.SUCCEEDED.value,
                        end_at=10.0))
            session.commit()

        monkeypatch.setattr(state, '_lock_current_controller_owner',
                            lambda _session, observed: None)

        assert state.reset_jobs_for_controller_slot(identity) == 1
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            rows = session.execute(
                sqlalchemy.select(
                    state.job_info_table.c.spot_job_id,
                    state.job_info_table.c.schedule_state,
                    state.job_info_table.c.controller_instance_id,
                    state.job_info_table.c.controller_slot_attempt,
                    state.job_info_table.c.controller_slot_quiescing,
                ).where(
                    state.job_info_table.c.spot_job_id.in_([
                        cleanup_job_id, done_job_id
                    ])).order_by(state.job_info_table.c.spot_job_id)).all()
        assert tuple(rows[0]) == (
            cleanup_job_id,
            state.ManagedJobScheduleState.WAITING.value,
            None,
            None,
            False,
        )
        assert tuple(rows[1]) == (
            done_job_id,
            state.ManagedJobScheduleState.DONE.value,
            owner[0],
            _SLOT_ATTEMPT,
            True,
        )

    def test_failure_terminalizes_for_fixed_slot_cleanup_adoption(
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
                    controller_generation=owner[1],
                    controller_slot_id=_SLOT_ID,
                    controller_slot_attempt=_SLOT_ATTEMPT))
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
            'controller_slot_id': _SLOT_ID,
            'controller_slot_attempt': _SLOT_ATTEMPT,
        }

        assert (state.set_failed_controller_if_current_snapshot(
            job_id, **snapshot, failure_reason='controller died') ==
                state.ControllerFailureDecision.TERMINALIZED)
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

        assert observed_owners == [owner]

    def test_failure_exact_recheck_stays_aggregate_and_preserves_reason(
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
                    controller_generation=owner[1],
                    controller_slot_id=_SLOT_ID,
                    controller_slot_attempt=_SLOT_ATTEMPT))
            session.execute(state.spot_table.delete().where(
                state.spot_table.c.spot_job_id == job_id))
            session.execute(state.spot_table.insert(), [{
                'spot_job_id': job_id,
                'task_id': 0,
                'task_name': 'active-task',
                'status': state.ManagedJobStatus.RUNNING.value,
            }, {
                'spot_job_id': job_id,
                'task_id': 1,
                'task_name': 'succeeded-task',
                'status': state.ManagedJobStatus.SUCCEEDED.value,
            }] + [{
                'spot_job_id': job_id,
                'task_id': task_id,
                'task_name': f'succeeded-task-{task_id}',
                'status': state.ManagedJobStatus.SUCCEEDED.value,
            } for task_id in range(2, 200)])
            session.execute(state.spot_table.update().where(
                state.spot_table.c.spot_job_id == job_id,
                state.spot_table.c.task_id == 1).values(
                    failure_reason='older failure'))
            session.commit()

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: owner)
        monkeypatch.setattr(state, '_lock_current_controller_owner',
                            lambda _session, _owner: None)
        _forbid_task_row_materialization(monkeypatch)
        snapshot = {
            'schedule_state': state.ManagedJobScheduleState.ALIVE,
            'controller_pid': 111,
            'controller_pid_started_at': 1.0,
            'controller_instance_id': owner[0],
            'controller_generation': owner[1],
            'controller_slot_id': _SLOT_ID,
            'controller_slot_attempt': _SLOT_ATTEMPT,
        }

        assert (state.set_failed_controller_if_current_snapshot(
            job_id, **snapshot, failure_reason='controller died') ==
                state.ControllerFailureDecision.TERMINALIZED)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            row = session.execute(
                sqlalchemy.select(
                    state.spot_table.c.status,
                    state.spot_table.c.failure_reason).where(
                        state.spot_table.c.spot_job_id == job_id).order_by(
                            state.spot_table.c.task_id.asc())).first()
        assert row.status == state.ManagedJobStatus.FAILED_CONTROLLER.value
        assert row.failure_reason == (
            'controller died. Previously: older failure')

    def test_failure_recheck_reports_already_terminal_without_rewrite(
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
                    controller_generation=owner[1],
                    controller_slot_id=_SLOT_ID,
                    controller_slot_attempt=_SLOT_ATTEMPT))
            session.execute(state.spot_table.update().where(
                state.spot_table.c.spot_job_id == job_id).values(
                    status=state.ManagedJobStatus.SUCCEEDED.value,
                    failure_reason=None,
                    end_at=10.0))
            session.commit()

        monkeypatch.setattr(state, 'get_current_controller_owner',
                            lambda: owner)
        monkeypatch.setattr(state, '_lock_current_controller_owner',
                            lambda _session, _owner: None)
        snapshot = {
            'schedule_state': state.ManagedJobScheduleState.ALIVE,
            'controller_pid': 111,
            'controller_pid_started_at': 1.0,
            'controller_instance_id': owner[0],
            'controller_generation': owner[1],
            'controller_slot_id': _SLOT_ID,
            'controller_slot_attempt': _SLOT_ATTEMPT,
        }

        assert (state.set_failed_controller_if_current_snapshot(
            job_id, **snapshot, failure_reason='controller died') ==
                state.ControllerFailureDecision.ALREADY_TERMINAL)
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            row = session.execute(
                sqlalchemy.select(
                    state.spot_table.c.status,
                    state.spot_table.c.failure_reason).where(
                        state.spot_table.c.spot_job_id == job_id)).one()
        assert row.status == state.ManagedJobStatus.SUCCEEDED.value
        assert row.failure_reason is None
