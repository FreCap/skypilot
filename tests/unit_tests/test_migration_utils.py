"""Database migration ownership and verify-mode tests."""

import os
from unittest import mock

import pytest
import sqlalchemy

from sky import global_user_state
from sky.server import database_migrations
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
        migration_utils.safe_alembic_upgrade(
            mock.Mock(), 'state_db', '024',
            mode='unsafe')  # type: ignore[arg-type]


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


def test_database_migration_entrypoint_forces_upgrade_mode(
        monkeypatch: pytest.MonkeyPatch) -> None:
    initialize = mock.Mock()
    monkeypatch.setattr(global_user_state, 'initialize_and_get_db', initialize)
    monkeypatch.setenv(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'verify')

    database_migrations.main()

    assert os.environ[constants.ENV_VAR_STATE_DB_MIGRATION_MODE] == 'upgrade'
    initialize.assert_called_once_with()
