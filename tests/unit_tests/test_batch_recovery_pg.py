"""Real-PostgreSQL proofs for managed jobs and Batch state."""
# pylint: disable=protected-access,redefined-outer-name
import asyncio
import datetime
import os
import shutil
import threading
import time
from unittest import mock

import pytest
import sqlalchemy
from sqlalchemy.ext import asyncio as sqlalchemy_async

from sky.jobs import batch_state as batch_state_lib
from sky.jobs import state

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
