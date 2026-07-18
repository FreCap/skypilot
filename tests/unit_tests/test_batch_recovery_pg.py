"""Real-PostgreSQL ordering proof for the Batch coordinator owner fence."""
# pylint: disable=protected-access,redefined-outer-name
import shutil
import threading
from unittest import mock

import pytest
import sqlalchemy

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
    original_lock = state._lock_batch_coordinator_owner

    def _pause_old_owner(session, job_id, owner_token):
        owned = original_lock(session, job_id, owner_token)
        if threading.current_thread().name == 'old-owner-claim':
            owner_locked.set()
            if not release_old.wait(timeout=10):
                raise RuntimeError('test timed out releasing old owner')
        return owned

    monkeypatch.setattr(state, '_lock_batch_coordinator_owner',
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
