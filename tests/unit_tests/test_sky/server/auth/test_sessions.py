"""Tests for the auth sessions module."""
import concurrent.futures
import hashlib
import threading
import time
import types

import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky import global_user_state
from sky.server import constants as server_constants
from sky.server.auth import sessions
from sky.utils import common_utils
from sky.utils.db import migration_utils


class TestComputeCodeChallenge:
    """Tests for the compute_code_challenge function."""

    def test_compute_challenge(self):
        code_verifier = 'test_verifier_123456'
        verifier_hash = hashlib.sha256(code_verifier.encode('utf-8')).digest()
        expected = common_utils.base64_url_encode(verifier_hash)
        assert common_utils.compute_code_challenge(code_verifier) == expected

    def test_different_verifiers_different_challenges(self):
        challenge1 = common_utils.compute_code_challenge('verifier1')
        challenge2 = common_utils.compute_code_challenge('verifier2')
        assert challenge1 != challenge2


class TestAuthSessionStore:
    """Tests for the AuthSessionStore class."""

    @pytest.fixture
    def engine(self, tmp_path):
        """Create a temporary auth-session database."""
        db_path = str(tmp_path / 'test_sessions.db')
        engine = sqlalchemy.create_engine(
            f'sqlite:///{db_path}',
            connect_args={'check_same_thread': False},
        )
        global_user_state.Base.metadata.create_all(engine)
        yield engine
        engine.dispose()

    @pytest.fixture
    def store(self, engine):
        """Create a store over the temporary database."""
        return sessions.AuthSessionStore(lambda: engine)

    def test_create_session(self, store):
        code_verifier = 'test_verifier'
        code_challenge = common_utils.compute_code_challenge(code_verifier)

        store.create_session(code_challenge, 'my_token')

        token = store.poll_session(code_verifier)
        assert token == 'my_token'

    def test_create_session_overwrites(self, store):
        # Duplicate authorize clicks should just overwrite
        code_verifier = 'test_verifier'
        code_challenge = common_utils.compute_code_challenge(code_verifier)

        store.create_session(code_challenge, 'token1')
        store.create_session(code_challenge, 'token2')

        token = store.poll_session(code_verifier)
        assert token == 'token2'

    def test_poll_session_consumes(self, store):
        code_verifier = 'test_verifier'
        code_challenge = common_utils.compute_code_challenge(code_verifier)

        store.create_session(code_challenge, 'my_token')

        # First poll returns the token
        token = store.poll_session(code_verifier)
        assert token == 'my_token'

        # Session should be consumed (deleted)
        token = store.poll_session(code_verifier)
        assert token is None

    def test_sessions_are_shared_between_store_instances(self, store, engine):
        code_verifier = 'test_verifier'
        code_challenge = common_utils.compute_code_challenge(code_verifier)
        other_store = sessions.AuthSessionStore(lambda: engine)

        store.create_session(code_challenge, 'my_token')

        assert other_store.poll_session(code_verifier) == 'my_token'

    def test_concurrent_polls_consume_once(self, store, engine):
        code_verifier = 'test_verifier'
        code_challenge = common_utils.compute_code_challenge(code_verifier)
        other_store = sessions.AuthSessionStore(lambda: engine)
        store.create_session(code_challenge, 'my_token')
        barrier = threading.Barrier(2)

        def poll(auth_store):
            barrier.wait()
            return auth_store.poll_session(code_verifier)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(poll, (store, other_store)))

        assert sorted(results,
                      key=lambda result: result is None) == ['my_token', None]

    def test_restore_session_preserves_newer_authorization(self, store):
        code_verifier = 'test_verifier'
        code_challenge = common_utils.compute_code_challenge(code_verifier)
        store.create_session(code_challenge, 'token1')
        assert store.poll_session(code_verifier) == 'token1'

        store.restore_session(code_challenge, 'token1')
        assert store.poll_session(code_verifier) == 'token1'

        store.create_session(code_challenge, 'token2')
        store.restore_session(code_challenge, 'token1')
        assert store.poll_session(code_verifier) == 'token2'

    def test_poll_session_not_found(self, store):
        # Poll with a verifier that has no corresponding session
        token = store.poll_session('nonexistent_verifier')
        assert token is None

    def test_poll_expired_session(self, store, monkeypatch):
        code_verifier = 'test_verifier'
        code_challenge = common_utils.compute_code_challenge(code_verifier)

        store.create_session(code_challenge, 'my_token')

        # Fast-forward time past expiration
        future_time = time.time(
        ) + server_constants.AUTH_SESSION_TIMEOUT_SECONDS + 10
        monkeypatch.setattr(time, 'time', lambda: future_time)

        # Should return None due to expiration
        token = store.poll_session(code_verifier)
        assert token is None

    def test_expired_sessions_cleanup(self, store, engine, monkeypatch):
        code_verifier1 = 'verifier1'
        code_verifier2 = 'verifier2'
        code_challenge1 = common_utils.compute_code_challenge(code_verifier1)
        code_challenge2 = common_utils.compute_code_challenge(code_verifier2)

        store.create_session(code_challenge1, 'token1')
        store.create_session(code_challenge2, 'token2')

        # Fast-forward time past expiration
        future_time = time.time(
        ) + server_constants.AUTH_SESSION_TIMEOUT_SECONDS + 10
        monkeypatch.setattr(time, 'time', lambda: future_time)

        # Both should be expired now
        assert store.poll_session(code_verifier1) is None
        assert store.poll_session(code_verifier2) is None

        # A later authorization opportunistically removes abandoned rows.
        new_verifier = 'new_verifier'
        store.create_session(common_utils.compute_code_challenge(new_verifier),
                             'new_token')
        table = global_user_state.auth_session_table
        with engine.connect() as connection:
            rows = connection.execute(sqlalchemy.select(table)).all()
        assert len(rows) == 1
        assert rows[0].token == 'new_token'

    def test_postgres_statements_use_upsert_consume_and_restore(self):
        executed_statements = []

        class FakeResult:

            def first(self):
                return None

        class FakeConnection:

            def execute(self, statement):
                executed_statements.append(statement)
                return FakeResult()

        class FakeTransaction:

            def __enter__(self):
                return FakeConnection()

            def __exit__(self, exc_type, exc_value, traceback):
                return False

        fake_engine = types.SimpleNamespace(
            dialect=types.SimpleNamespace(name='postgresql'),
            begin=FakeTransaction,
        )
        store = sessions.AuthSessionStore(lambda: fake_engine)
        code_verifier = 'test_verifier'
        code_challenge = common_utils.compute_code_challenge(code_verifier)

        store.create_session(code_challenge, 'my_token')
        upsert_sql = str(
            executed_statements[1].compile(dialect=postgresql.dialect()))
        assert 'ON CONFLICT (code_challenge) DO UPDATE' in upsert_sql

        executed_statements.clear()
        assert store.poll_session(code_verifier) is None
        consume_sql = str(
            executed_statements[0].compile(dialect=postgresql.dialect()))
        assert consume_sql.startswith('DELETE FROM auth_sessions')
        assert 'RETURNING auth_sessions.token' in consume_sql

        executed_statements.clear()
        store.restore_session(code_challenge, 'my_token')
        restore_sql = str(
            executed_statements[0].compile(dialect=postgresql.dialect()))
        assert 'ON CONFLICT (code_challenge) DO NOTHING' in restore_sql


class TestGlobalStore:

    def test_global_store_exists(self):
        assert isinstance(sessions.auth_session_store,
                          sessions.AuthSessionStore)


def test_global_state_migration_creates_auth_sessions(tmp_path):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "state.db"}')
    for _ in range(2):
        migration_utils.safe_alembic_upgrade(
            engine,
            migration_utils.GLOBAL_USER_STATE_DB_NAME,
            migration_utils.GLOBAL_USER_STATE_VERSION,
        )

    columns = {
        column['name']: column
        for column in sqlalchemy.inspect(engine).get_columns('auth_sessions')
    }
    assert set(columns) == {'code_challenge', 'token', 'created_at'}
    assert columns['code_challenge']['primary_key'] == 1
    assert columns['code_challenge']['nullable'] is False
    assert columns['token']['nullable'] is False
    assert columns['created_at']['nullable'] is False
