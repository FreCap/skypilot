"""Real-PostgreSQL auth-session visibility and consumption tests."""
# pylint: disable=redefined-outer-name,too-many-function-args
import concurrent.futures
import shutil
import threading

import pytest
import sqlalchemy

from sky import global_user_state
from sky.server.auth import sessions
from sky.utils import common_utils

testcontainers_postgres = pytest.importorskip('testcontainers.postgres')

pytestmark = pytest.mark.skipif(
    shutil.which('docker') is None,
    reason='docker unavailable; skipping real-PostgreSQL auth-session tests')


@pytest.fixture(scope='module')
def postgres_url():
    """Start one throwaway PostgreSQL server for this module."""
    container = testcontainers_postgres.PostgresContainer('postgres:16')
    try:
        container.start()
    except Exception as e:  # pylint: disable=broad-except
        pytest.skip(f'could not start postgres container: {e}')
    try:
        yield container.get_connection_url()
    finally:
        container.stop()


def test_sessions_are_shared_and_consumed_once_on_postgres(postgres_url):
    """Independent API-pod engines share and atomically consume sessions."""
    engine_a = sqlalchemy.create_engine(postgres_url)
    engine_b = sqlalchemy.create_engine(postgres_url)
    try:
        global_user_state.Base.metadata.drop_all(engine_a)
        global_user_state.Base.metadata.create_all(engine_a)
        store_a = sessions.AuthSessionStore(lambda: engine_a)
        store_b = sessions.AuthSessionStore(lambda: engine_b)
        verifier = 'postgres-cross-replica-verifier'
        challenge = common_utils.compute_code_challenge(verifier)
        store_a.create_session(challenge, 'shared-token')
        barrier = threading.Barrier(2)

        def poll(store):
            barrier.wait()
            return store.poll_session(verifier)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(poll, (store_a, store_b)))

        assert sorted(
            results, key=lambda token: token is None) == ['shared-token', None]
    finally:
        engine_a.dispose()
        engine_b.dispose()
