"""Real-PostgreSQL proofs for managed jobs and Batch state."""
# pylint: disable=protected-access,redefined-outer-name
import asyncio
import contextlib
import datetime
import importlib
import os
import shutil
import threading
import time
from unittest import mock
import uuid

import pytest
import sqlalchemy
from sqlalchemy.ext import asyncio as sqlalchemy_async

from sky.jobs import batch_state as batch_state_lib
from sky.jobs import state
from sky.server.requests import postgres as request_postgres
from sky.utils import locks

testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    shutil.which('docker') is None,
    reason='docker unavailable; skipping real-Postgres Batch fence test')


@pytest.fixture(scope='module')
def postgres_engine():
    container = testcontainers_postgres.PostgresContainer('postgres:16')
    try:
        container.start()
    except Exception as e:  # pylint: disable=broad-except
        pytest.skip(f'could not start postgres container: {e}')
    engine = sqlalchemy.create_engine(container.get_connection_url())
    state.Base.metadata.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


def test_postgres_waiting_job_index_shape_and_idle_plan(postgres_engine):
    migration_027 = importlib.import_module(
        'sky.schemas.db.spot_jobs.027_add_waiting_job_priority_index')
    first_job_id = 9_000_000
    job_count = 5_000

    with postgres_engine.begin() as connection:
        index = migration_027._postgres_index_state(connection)
        assert index is not None
        assert migration_027._postgres_shape_matches(index)
        connection.execute(state.job_info_table.insert(), [{
            'spot_job_id': first_job_id + offset,
            'schedule_state': state.ManagedJobScheduleState.DONE.value,
            'priority': 0,
            'is_batch': False,
        } for offset in range(job_count)])
        connection.exec_driver_sql('ANALYZE job_info')

        active_batch_states = [
            state.ManagedJobScheduleState.LAUNCHING.value,
            state.ManagedJobScheduleState.ALIVE.value,
            state.ManagedJobScheduleState.ALIVE_WAITING.value,
            state.ManagedJobScheduleState.ALIVE_BACKOFF.value,
        ]
        busy_batch_pools = sqlalchemy.select(state.job_info_table.c.pool).where(
            sqlalchemy.and_(
                state.job_info_table.c.pool.isnot(None),
                state.job_info_table.c.is_batch.is_(True),
                state.job_info_table.c.schedule_state.in_(active_batch_states),
            )).correlate(None).scalar_subquery()
        candidate = sqlalchemy.select(
            state.job_info_table.c.spot_job_id,
            state.job_info_table.c.schedule_state,
            state.job_info_table.c.pool,
        ).where(
            sqlalchemy.and_(
                state.job_info_table.c.schedule_state.in_([
                    state.ManagedJobScheduleState.WAITING.value,
                ]),
                sqlalchemy.or_(
                    state.job_info_table.c.is_batch.isnot(True),
                    ~state.job_info_table.c.pool.in_(busy_batch_pools),
                ),
            )).order_by(
                state.job_info_table.c.priority.desc(),
                state.job_info_table.c.spot_job_id.asc(),
            ).limit(1).with_for_update()
        sql = candidate.compile(dialect=postgres_engine.dialect,
                                compile_kwargs={'literal_binds': True})
        plan_document = connection.exec_driver_sql(
            f'EXPLAIN (FORMAT JSON) {sql}').scalar_one()

        connection.execute(state.job_info_table.delete().where(
            state.job_info_table.c.spot_job_id.between(
                first_job_id, first_job_id + job_count - 1)))
        connection.exec_driver_sql('ANALYZE job_info')

    plan = plan_document[0]['Plan']
    assert plan['Node Type'] == 'Limit'
    lock_rows = plan['Plans'][0]
    assert lock_rows['Node Type'] == 'LockRows'
    index_scan = lock_rows['Plans'][0]
    assert index_scan['Node Type'] == 'Index Scan'
    assert index_scan['Relation Name'] == 'job_info'
    assert index_scan['Index Name'] == 'ix_job_info_schedule_priority'


def test_postgres_job_event_writers_preserve_utc_instants(
        postgres_engine, monkeypatch):
    """A non-UTC process must not shift timestamptz event writes."""

    def _set_session_utc(dbapi_connection, *_args):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SET TIME ZONE 'UTC'")
        finally:
            cursor.close()

    sqlalchemy.event.listen(postgres_engine, 'checkout', _set_session_utc)
    monkeypatch.setattr(state._db_manager, '_engine', postgres_engine)
    async_url = postgres_engine.url.render_as_string(
        hide_password=False).replace('postgresql+psycopg2',
                                     'postgresql+asyncpg')
    async_engine = sqlalchemy_async.create_async_engine(
        async_url, connect_args={'server_settings': {
            'timezone': 'UTC'
        }})
    monkeypatch.setattr(state._db_manager, '_engine_async', async_engine)

    original_tz = os.environ.get('TZ')
    os.environ['TZ'] = 'Asia/Kolkata'
    time.tzset()
    try:
        before = datetime.datetime.now(datetime.timezone.utc)
        state.add_job_event(90_001, 0, state.ManagedJobStatus.PENDING,
                            'sync event')
        state.add_job_event(90_004,
                            0,
                            state.ManagedJobStatus.PENDING,
                            'recent explicit event',
                            timestamp=before)
        state.add_job_event(90_005,
                            0,
                            state.ManagedJobStatus.PENDING,
                            'stale explicit event',
                            timestamp=before - datetime.timedelta(hours=2))

        async def _write_async_event_and_apply_retention():
            try:
                await state.add_job_event_async(90_002, 0,
                                                state.ManagedJobStatus.STARTING,
                                                'async event')
                await state.cleanup_job_events_with_retention_async(1)
            finally:
                await async_engine.dispose()

        asyncio.run(_write_async_event_and_apply_retention())

        with postgres_engine.begin() as connection:
            connection.execute(state.job_info_table.insert().values(
                spot_job_id=90_003, is_batch=True))
            connection.execute(state.spot_table.insert().values(
                spot_job_id=90_003,
                task_id=0,
                status=state.ManagedJobStatus.RUNNING.value,
                end_at=None))
        assert state.acquire_batch_coordinator(90_003, 'owner') is None
        assert state.set_batch_succeeded(
            90_003, 0, 'owner',
            end_time=123) is (state.BatchLifecycleTransition.APPLIED)
        after = datetime.datetime.now(datetime.timezone.utc)

        with postgres_engine.connect() as connection:
            rows = connection.execute(
                sqlalchemy.select(state.job_events_table.c.spot_job_id,
                                  state.job_events_table.c.timestamp).where(
                                      state.job_events_table.c.spot_job_id.in_([
                                          90_001, 90_002, 90_003, 90_004, 90_005
                                      ]))).all()

        assert {row.spot_job_id for row in rows
               } == {90_001, 90_002, 90_003, 90_004}
        for row in rows:
            assert row.timestamp.tzinfo is not None
            assert before <= row.timestamp <= after
    finally:
        sqlalchemy.event.remove(postgres_engine, 'checkout', _set_session_utc)
        if original_tz is None:
            os.environ.pop('TZ', None)
        else:
            os.environ['TZ'] = original_tz
        time.tzset()


def test_postgres_takeover_waits_for_old_owner_commit(postgres_engine,
                                                      monkeypatch):
    monkeypatch.setattr(state._db_manager, '_engine', postgres_engine)
    with postgres_engine.begin() as connection:
        connection.execute(state.job_info_table.insert().values(spot_job_id=1,
                                                                is_batch=True))
    assert state.acquire_batch_coordinator(1, 'old-owner') is None
    assert state.save_batch_states(1, [[0, 4]], 'old-owner')

    owner_locked = threading.Event()
    release_old = threading.Event()
    takeover_started = threading.Event()
    takeover_done = threading.Event()
    errors = []
    new_launch = mock.Mock()
    original_lock = batch_state_lib._lock_batch_coordinator_owner

    def _pause_old_owner(session, job_id, owner_token):
        owned = original_lock(session, job_id, owner_token)
        if threading.current_thread().name == 'old-owner-claim':
            owner_locked.set()
            if not release_old.wait(timeout=10):
                raise RuntimeError('test timed out releasing old owner')
        return owned

    monkeypatch.setattr(batch_state_lib, '_lock_batch_coordinator_owner',
                        _pause_old_owner)

    def _old_claim():
        try:
            assert state.claim_batch(1, 0, 'old-owner', 'worker-a', 10,
                                     now=100) == (1, 0)
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)

    def _takeover_and_launch():
        try:
            takeover_started.set()
            assert state.acquire_batch_coordinator(1,
                                                   'new-owner') == 'old-owner'
            new_launch()
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            takeover_done.set()

    old_thread = threading.Thread(target=_old_claim, name='old-owner-claim')
    old_thread.start()
    assert owner_locked.wait(timeout=10)
    takeover_thread = threading.Thread(target=_takeover_and_launch)
    takeover_thread.start()

    assert takeover_started.wait(timeout=10)
    assert not takeover_done.wait(timeout=0.2)
    new_launch.assert_not_called()
    release_old.set()
    old_thread.join(timeout=10)
    takeover_thread.join(timeout=10)

    assert not old_thread.is_alive()
    assert not takeover_thread.is_alive()
    assert not errors
    new_launch.assert_called_once_with()


def _seed_waiting_pending_job(engine, job_id):
    """Seed a submitted-but-never-started job: PENDING task, WAITING state."""
    with engine.begin() as connection:
        connection.execute(state.job_info_table.insert().values(
            spot_job_id=job_id,
            schedule_state=state.ManagedJobScheduleState.WAITING.value))
        connection.execute(state.spot_table.insert().values(
            spot_job_id=job_id,
            task_id=0,
            job_name='cancel-me',
            status=state.ManagedJobStatus.PENDING.value))


@contextlib.contextmanager
def _live_controller_generation(engine, instance_id: str, generation: int):
    """Publish one leadership row backed by both exact advisory locks."""
    request_postgres.CONTROLLER_LEADERSHIP.create(engine, checkfirst=True)
    election_key = locks.postgres_lock_key('skypilot:api-controller-leader:v1')
    generation_key = locks.postgres_lock_key(
        f'skypilot:api-controller-generation:v1:{generation}')
    lock_connection = engine.raw_connection()
    cursor = lock_connection.cursor()
    try:
        cursor.execute('SELECT pg_advisory_lock(%s)', (election_key,))
        cursor.execute('SELECT pg_advisory_lock(%s)', (generation_key,))
        cursor.execute('SELECT pg_backend_pid()')
        backend_pid = int(cursor.fetchone()[0])
        lock_connection.commit()
        with engine.begin() as connection:
            connection.execute(request_postgres.CONTROLLER_LEADERSHIP.delete())
            connection.execute(
                request_postgres.CONTROLLER_LEADERSHIP.insert().values(
                    leadership_key='api-controller',
                    generation=generation,
                    instance_id=uuid.UUID(instance_id),
                    lock_backend_pid=backend_pid,
                    generation_lock_key=generation_key,
                    acquired_at=sqlalchemy.func.clock_timestamp(),
                    heartbeat_at=sqlalchemy.func.clock_timestamp(),
                    released_at=None))
        yield
    finally:
        with engine.begin() as connection:
            connection.execute(request_postgres.CONTROLLER_LEADERSHIP.delete())
        cursor.execute('SELECT pg_advisory_unlock(%s)', (generation_key,))
        cursor.execute('SELECT pg_advisory_unlock(%s)', (election_key,))
        lock_connection.commit()
        cursor.close()
        lock_connection.close()


def test_managed_job_claim_serializes_with_controller_generation_handoff(
        postgres_engine, monkeypatch):
    """The leadership row is the claim versus handoff serialization point."""
    monkeypatch.setattr(state._db_manager, '_engine', postgres_engine)
    async_url = postgres_engine.url.render_as_string(
        hide_password=False).replace('postgresql+psycopg2',
                                     'postgresql+asyncpg')
    # Claims run on different event loops below. NullPool keeps an asyncpg
    # connection from being reused by a loop other than the one that opened it.
    async_engine = sqlalchemy_async.create_async_engine(
        async_url, poolclass=sqlalchemy.pool.NullPool)
    monkeypatch.setattr(state._db_manager, '_engine_async', async_engine)

    instance_id = '3c97d5af-31f9-4a63-a84a-ec1493842297'
    next_instance_id = '6d6a2f13-b3bb-4a2f-a7d6-025751412918'
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'controller')
    monkeypatch.setenv(request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR,
                       instance_id)
    monkeypatch.setenv(request_postgres.CONTROLLER_GENERATION_ENV_VAR, '31')
    _seed_waiting_pending_job(postgres_engine, 4103)
    _seed_waiting_pending_job(postgres_engine, 4104)

    claim_locked = threading.Event()
    release_claim = threading.Event()
    handoff_started = threading.Event()
    handoff_done = threading.Event()
    errors = []
    claim_result = {}
    original_lock = state._lock_current_controller_owner_async

    async def _pause_after_leadership_lock(session, owner):
        await original_lock(session, owner)
        claim_locked.set()
        if not release_claim.wait(timeout=10):
            raise RuntimeError('test timed out releasing managed-job claim')

    monkeypatch.setattr(state, '_lock_current_controller_owner_async',
                        _pause_after_leadership_lock)

    def _claim():
        try:
            claim_result['value'] = asyncio.run(
                state.get_waiting_job_async(pid=7777, pid_started_at=1.0))
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)

    def _handoff():
        try:
            handoff_started.set()
            with postgres_engine.begin() as connection:
                connection.execute(
                    request_postgres.CONTROLLER_LEADERSHIP.update().where(
                        request_postgres.CONTROLLER_LEADERSHIP.c.leadership_key
                        == 'api-controller').values(
                            generation=32,
                            instance_id=uuid.UUID(next_instance_id)))
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            handoff_done.set()

    try:
        with _live_controller_generation(postgres_engine, instance_id, 31):
            claim_thread = threading.Thread(target=_claim)
            claim_thread.start()
            assert claim_locked.wait(timeout=10)

            handoff_thread = threading.Thread(target=_handoff)
            handoff_thread.start()
            assert handoff_started.wait(timeout=10)
            assert not handoff_done.wait(timeout=0.2)

            release_claim.set()
            claim_thread.join(timeout=10)
            handoff_thread.join(timeout=10)
            assert not claim_thread.is_alive()
            assert not handoff_thread.is_alive()
            assert not errors
            assert claim_result['value'] == {'job_id': 4103, 'pool': None}

            with postgres_engine.connect() as connection:
                owner = connection.execute(
                    sqlalchemy.select(
                        state.job_info_table.c.controller_instance_id,
                        state.job_info_table.c.controller_generation,
                    ).where(state.job_info_table.c.spot_job_id == 4103)).one()
            assert tuple(owner) == (instance_id, 31)

            # The environment still says generation 31, but the durable row
            # has advanced. No subsequent waiting job may be claimed.
            with pytest.raises(state.ControllerLeadershipLostError):
                asyncio.run(
                    state.get_waiting_job_async(pid=8888, pid_started_at=2.0))
            assert (state.get_job_schedule_state(4104) ==
                    state.ManagedJobScheduleState.WAITING)
    finally:
        asyncio.run(async_engine.dispose())
        with postgres_engine.begin() as connection:
            job_ids = [4103, 4104]
            connection.execute(state.spot_table.delete().where(
                state.spot_table.c.spot_job_id.in_(job_ids)))
            connection.execute(state.job_info_table.delete().where(
                state.job_info_table.c.spot_job_id.in_(job_ids)))


def test_postgres_pending_cancel_finalizes_in_one_transaction(
        postgres_engine, monkeypatch):
    """set_pending_cancelled must finalize completely on real PostgreSQL.

    SQLite serializes writers, so it cannot show that the UPDATE ... WHERE
    EXISTS guard and the two-statement transaction behave under PostgreSQL's
    READ COMMITTED isolation.
    """
    monkeypatch.setattr(state._db_manager, '_engine', postgres_engine)
    _seed_waiting_pending_job(postgres_engine, 4101)

    assert state.set_pending_cancelled(4101) is True

    assert state.get_status(4101) == state.ManagedJobStatus.CANCELLED
    assert (state.get_job_schedule_state(4101) ==
            state.ManagedJobScheduleState.DONE)
    with postgres_engine.connect() as connection:
        end_at = connection.execute(
            sqlalchemy.select(state.spot_table.c.end_at).where(
                state.spot_table.c.spot_job_id == 4101)).scalar()
    assert end_at is not None
    # The finalized job is no longer alive for autostop accounting.
    assert state.get_num_alive_jobs() == 0


def test_postgres_log_gc_filters_compare_schedule_state_values(
        postgres_engine, monkeypatch):
    """Log-GC DONE predicates must compile and execute on PostgreSQL."""
    monkeypatch.setattr(state._db_manager, '_engine', postgres_engine)
    job_id = 4105
    with postgres_engine.begin() as connection:
        connection.execute(state.job_info_table.insert().values(
            spot_job_id=job_id,
            schedule_state=state.ManagedJobScheduleState.DONE.value,
            controller_logs_cleaned_at=None))
        connection.execute(state.spot_table.insert().values(
            spot_job_id=job_id,
            task_id=0,
            job_name='log-gc-postgres',
            status=state.ManagedJobStatus.SUCCEEDED.value,
            end_at=time.time() - 120,
            local_log_file='/tmp/log-gc-postgres.log',
            logs_cleaned_at=None))
    try:
        task_rows = state.get_task_logs_to_clean(60, batch_size=10)
        controller_rows = state.get_controller_logs_to_clean(60, batch_size=10)
        assert any(row['job_id'] == job_id for row in task_rows)
        assert any(row['job_id'] == job_id for row in controller_rows)
    finally:
        with postgres_engine.begin() as connection:
            connection.execute(state.spot_table.delete().where(
                state.spot_table.c.spot_job_id == job_id))
            connection.execute(state.job_info_table.delete().where(
                state.job_info_table.c.spot_job_id == job_id))


def test_postgres_cancel_and_claim_cannot_both_win(postgres_engine,
                                                   monkeypatch):
    """Exactly one of "cancel" and "claim" may win the schedule-state CAS.

    Both writers compare-and-swap job_info.schedule_state, so on real
    PostgreSQL the second one must observe the first one's committed row and
    fail its own guard. If both could win, a cancelled job would still be
    handed to a controller and run.
    """
    monkeypatch.setattr(state._db_manager, '_engine', postgres_engine)
    _seed_waiting_pending_job(postgres_engine, 4102)

    async_url = postgres_engine.url.render_as_string(
        hide_password=False).replace('postgresql+psycopg2',
                                     'postgresql+asyncpg')
    async_engine = sqlalchemy_async.create_async_engine(
        async_url, poolclass=sqlalchemy.pool.NullPool)
    monkeypatch.setattr(state._db_manager, '_engine_async', async_engine)

    cancel_result = {}
    claim_result = {}
    barrier = threading.Barrier(2, timeout=10)

    def _cancel():
        barrier.wait()
        cancel_result['won'] = state.set_pending_cancelled(4102)

    def _claim():
        barrier.wait()
        claimed = asyncio.run(
            state.get_waiting_job_async(pid=7777, pid_started_at=1.0))
        claim_result['won'] = claimed is not None

    threads = [
        threading.Thread(target=_cancel),
        threading.Thread(target=_claim),
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
        assert not any(thread.is_alive() for thread in threads)

        # Never both: the losing writer's guard must have matched zero rows.
        assert not (cancel_result['won'] and claim_result['won'])

        schedule_state = state.get_job_schedule_state(4102)
        if cancel_result['won']:
            assert schedule_state == state.ManagedJobScheduleState.DONE
            assert state.get_status(4102) == (state.ManagedJobStatus.CANCELLED)
        else:
            # The controller claimed it; the job stays runnable and PENDING.
            assert schedule_state == (state.ManagedJobScheduleState.LAUNCHING)
            assert state.get_status(4102) == state.ManagedJobStatus.PENDING
    finally:
        asyncio.run(async_engine.dispose())


def test_postgres_controller_failure_decision_uses_live_generation(
        postgres_engine, monkeypatch):
    """Terminalization and DONE compose with the real dual-lock fence."""
    monkeypatch.setattr(state._db_manager, '_engine', postgres_engine)
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    generation = 72
    job_id = 4172
    with postgres_engine.begin() as connection:
        connection.execute(state.job_info_table.insert().values(
            spot_job_id=job_id,
            schedule_state=state.ManagedJobScheduleState.ALIVE.value,
            controller_pid=7777,
            controller_pid_started_at=1.0,
            controller_instance_id=instance_id,
            controller_generation=generation))
        connection.execute(state.spot_table.insert().values(
            spot_job_id=job_id,
            task_id=0,
            job_name='generation-fenced-cleanup',
            status=state.ManagedJobStatus.RUNNING.value))

    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'controller')
    monkeypatch.setenv(request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR,
                       instance_id)
    monkeypatch.setenv(request_postgres.CONTROLLER_GENERATION_ENV_VAR,
                       str(generation))
    snapshot = {
        'schedule_state': state.ManagedJobScheduleState.ALIVE,
        'controller_pid': 7777,
        'controller_pid_started_at': 1.0,
        'controller_instance_id': instance_id,
        'controller_generation': generation,
    }
    try:
        with _live_controller_generation(postgres_engine, instance_id,
                                         generation):
            assert (state.set_failed_controller_if_current_snapshot(
                job_id, **snapshot, failure_reason='controller process died') ==
                    state.ControllerFailureDecision.TERMINALIZED)
            assert state.get_status(job_id) == (
                state.ManagedJobStatus.FAILED_CONTROLLER)
            assert state.get_job_schedule_state(job_id) == (
                state.ManagedJobScheduleState.ALIVE)
            assert state.finish_controller_cleanup_if_current_snapshot(
                job_id, **snapshot)
            assert state.get_job_schedule_state(job_id) == (
                state.ManagedJobScheduleState.DONE)
    finally:
        with postgres_engine.begin() as connection:
            connection.execute(state.spot_table.delete().where(
                state.spot_table.c.spot_job_id == job_id))
            connection.execute(state.job_info_table.delete().where(
                state.job_info_table.c.spot_job_id == job_id))
