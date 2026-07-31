"""Database migration ownership and verify-mode tests."""

import os
from unittest import mock

import pytest
import sqlalchemy

from sky import global_user_state
from sky import skypilot_config
from sky.jobs import state_storage
from sky.physical_capacity import config as capacity_config
from sky.physical_capacity import state as capacity_state
from sky.serve import serve_state
from sky.server import database_migrations
from sky.server.requests import postgres as request_postgres
from sky.skylet import constants
from sky.utils.db import migration_utils


def test_safe_alembic_verify_mode_never_enters_upgrade_path() -> None:
    engine = mock.Mock()
    verify = mock.Mock()
    with mock.patch.object(migration_utils, 'verify_alembic_revision', verify), \
         mock.patch.object(
             migration_utils,
             'needs_upgrade',
             side_effect=AssertionError('verify mode must not inspect upgrade')):
        migration_utils.safe_alembic_upgrade(engine,
                                             'state_db',
                                             '024',
                                             mode='verify')

    verify.assert_called_once_with(engine, 'state_db', '024', None)


def test_safe_alembic_rejects_unknown_migration_mode() -> None:
    with pytest.raises(ValueError, match='Invalid database migration mode'):
        migration_utils.safe_alembic_upgrade(mock.Mock(),
                                             'state_db',
                                             '024',
                                             mode='unsafe')


def test_older_rollback_target_accepts_newer_additive_revision(
        monkeypatch: pytest.MonkeyPatch) -> None:
    engine = mock.Mock()
    monkeypatch.setattr(migration_utils, 'get_current_alembic_revision',
                        lambda *_args, **_kwargs: '027')

    assert not migration_utils.needs_upgrade(engine, 'serve_db', '026')
    migration_utils.verify_alembic_revision(engine, 'serve_db', '026')


def test_global_state_create_table_uses_configured_migration_mode(
        monkeypatch: pytest.MonkeyPatch) -> None:
    engine = sqlalchemy.create_engine('sqlite://')
    upgrade = mock.Mock()
    monkeypatch.setattr(migration_utils, 'safe_alembic_upgrade', upgrade)
    monkeypatch.setenv(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'verify')

    global_user_state.create_table(engine)

    upgrade.assert_called_once_with(engine,
                                    migration_utils.GLOBAL_USER_STATE_DB_NAME,
                                    migration_utils.GLOBAL_USER_STATE_VERSION,
                                    mode='verify')
    engine.dispose()


@pytest.mark.parametrize(('create_table', 'database_name', 'version'), [
    (serve_state.create_table, migration_utils.SERVE_DB_NAME,
     migration_utils.SERVE_VERSION),
    (state_storage.create_table, migration_utils.SPOT_JOBS_DB_NAME,
     migration_utils.SPOT_JOBS_VERSION),
])
def test_companion_create_table_uses_configured_migration_mode(
        monkeypatch: pytest.MonkeyPatch, create_table, database_name: str,
        version: str) -> None:
    engine = sqlalchemy.create_engine('sqlite://')
    upgrade = mock.Mock()
    monkeypatch.setattr(migration_utils, 'safe_alembic_upgrade', upgrade)
    monkeypatch.setenv(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'verify')

    create_table(engine)

    upgrade.assert_called_once_with(engine,
                                    database_name,
                                    version,
                                    mode='verify')
    engine.dispose()


def test_database_migration_entrypoint_forces_upgrade_mode(
        monkeypatch: pytest.MonkeyPatch) -> None:
    initialize = mock.Mock()
    initialize_config = mock.Mock()
    initialize_serve = mock.Mock()
    initialize_jobs = mock.Mock()
    monkeypatch.setattr(global_user_state, 'initialize_and_get_db', initialize)
    monkeypatch.setattr(skypilot_config, 'initialize_and_get_db',
                        initialize_config)
    monkeypatch.setattr(serve_state, 'get_database_engine', initialize_serve)
    monkeypatch.setattr(state_storage, 'initialize_and_get_db', initialize_jobs)
    monkeypatch.setenv(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'verify')
    monkeypatch.delenv(constants.ENV_VAR_IS_SKYPILOT_SERVER, raising=False)
    monkeypatch.delenv('SKYPILOT_API_REQUEST_BACKEND', raising=False)

    database_migrations.main()

    assert os.environ[constants.ENV_VAR_IS_SKYPILOT_SERVER] == 'true'
    assert os.environ[constants.ENV_VAR_STATE_DB_MIGRATION_MODE] == 'upgrade'
    initialize.assert_called_once_with()
    initialize_config.assert_called_once_with()
    initialize_serve.assert_called_once_with()
    initialize_jobs.assert_called_once_with()


def test_database_migration_entrypoint_preserves_explicit_bootstrap_mode(
        monkeypatch: pytest.MonkeyPatch) -> None:
    initialize = mock.Mock()
    initialize_config = mock.Mock()
    initialize_serve = mock.Mock()
    initialize_jobs = mock.Mock()
    monkeypatch.setattr(global_user_state, 'initialize_and_get_db', initialize)
    monkeypatch.setattr(skypilot_config, 'initialize_and_get_db',
                        initialize_config)
    monkeypatch.setattr(serve_state, 'get_database_engine', initialize_serve)
    monkeypatch.setattr(state_storage, 'initialize_and_get_db', initialize_jobs)
    monkeypatch.setenv(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'bootstrap')
    monkeypatch.delenv('SKYPILOT_API_REQUEST_BACKEND', raising=False)

    database_migrations.main()

    assert os.environ[constants.ENV_VAR_STATE_DB_MIGRATION_MODE] == 'bootstrap'
    initialize.assert_called_once_with()
    initialize_config.assert_called_once_with()
    initialize_serve.assert_called_once_with()
    initialize_jobs.assert_called_once_with()


def test_database_migration_initializes_selected_request_store(
        monkeypatch: pytest.MonkeyPatch) -> None:
    initialize = mock.Mock()
    initialize_config = mock.Mock()
    initialize_serve = mock.Mock()
    initialize_jobs = mock.Mock()
    initialize_requests = mock.Mock()
    monkeypatch.setattr(global_user_state, 'initialize_and_get_db', initialize)
    monkeypatch.setattr(skypilot_config, 'initialize_and_get_db',
                        initialize_config)
    monkeypatch.setattr(serve_state, 'get_database_engine', initialize_serve)
    monkeypatch.setattr(state_storage, 'initialize_and_get_db', initialize_jobs)
    monkeypatch.setattr(request_postgres, 'initialize_and_get_db',
                        initialize_requests)
    monkeypatch.setenv('SKYPILOT_API_REQUEST_BACKEND', 'postgres')

    database_migrations.initialize_central_databases()

    initialize.assert_called_once_with()
    initialize_config.assert_called_once_with()
    initialize_serve.assert_called_once_with()
    initialize_jobs.assert_called_once_with()
    initialize_requests.assert_called_once_with()


def test_database_migration_initializes_capacity_last_on_postgres(
        monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    postgres_engine = mock.Mock()
    postgres_engine.dialect.name = 'postgresql'
    configuration = mock.Mock(mode=capacity_config.CapacityMode.DISABLED)

    def initialize_global():
        order.append('global')
        return postgres_engine

    def load_capacity_config():
        order.append('capacity_config')
        return configuration

    monkeypatch.setattr(global_user_state, 'initialize_and_get_db',
                        initialize_global)
    monkeypatch.setattr(skypilot_config, 'initialize_and_get_db',
                        lambda: order.append('config'))
    monkeypatch.setattr(serve_state, 'get_database_engine',
                        lambda: order.append('serve'))
    monkeypatch.setattr(state_storage, 'initialize_and_get_db',
                        lambda: order.append('jobs'))
    monkeypatch.setattr(capacity_config, 'load_config', load_capacity_config)
    validate = mock.Mock(
        side_effect=lambda *_args, **_kwargs: order.append('capacity_validate'))
    monkeypatch.setattr(capacity_config, 'validate_runtime_capability',
                        validate)
    monkeypatch.setattr(capacity_state, 'initialize_and_get_db',
                        lambda: order.append('capacity'))
    monkeypatch.delenv('SKYPILOT_API_REQUEST_BACKEND', raising=False)

    database_migrations.initialize_central_databases()

    assert order == [
        'capacity_config', 'global', 'config', 'serve', 'jobs',
        'capacity_validate', 'capacity'
    ]
    validate.assert_called_once_with(configuration, revision='001')


def test_database_migration_skips_disabled_capacity_on_sqlite(
        monkeypatch: pytest.MonkeyPatch) -> None:
    sqlite_engine = mock.Mock()
    sqlite_engine.dialect.name = 'sqlite'
    configuration = mock.Mock(mode=capacity_config.CapacityMode.DISABLED)
    initialize_capacity = mock.Mock()

    monkeypatch.setattr(global_user_state, 'initialize_and_get_db',
                        lambda: sqlite_engine)
    monkeypatch.setattr(skypilot_config, 'initialize_and_get_db', mock.Mock())
    monkeypatch.setattr(serve_state, 'get_database_engine', mock.Mock())
    monkeypatch.setattr(state_storage, 'initialize_and_get_db', mock.Mock())
    monkeypatch.setattr(capacity_config, 'load_config', lambda: configuration)
    monkeypatch.setattr(capacity_state, 'initialize_and_get_db',
                        initialize_capacity)
    monkeypatch.delenv('SKYPILOT_API_REQUEST_BACKEND', raising=False)

    database_migrations.initialize_central_databases()

    initialize_capacity.assert_not_called()


def test_database_migration_rejects_capacity_mode_on_sqlite(
        monkeypatch: pytest.MonkeyPatch) -> None:
    sqlite_engine = mock.Mock()
    sqlite_engine.dialect.name = 'sqlite'
    configuration = mock.Mock(mode=capacity_config.CapacityMode.SHADOW)

    monkeypatch.setattr(global_user_state, 'initialize_and_get_db',
                        lambda: sqlite_engine)
    monkeypatch.setattr(skypilot_config, 'initialize_and_get_db', mock.Mock())
    monkeypatch.setattr(serve_state, 'get_database_engine', mock.Mock())
    monkeypatch.setattr(state_storage, 'initialize_and_get_db', mock.Mock())
    monkeypatch.setattr(capacity_config, 'load_config', lambda: configuration)
    monkeypatch.delenv('SKYPILOT_API_REQUEST_BACKEND', raising=False)

    with pytest.raises(RuntimeError, match='PostgreSQL'):
        database_migrations.initialize_central_databases()
