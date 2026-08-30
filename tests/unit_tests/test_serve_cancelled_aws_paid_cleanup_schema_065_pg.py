"""PostgreSQL DDL contracts for cancelled AWS paid cleanup."""
# pylint: disable=protected-access,redefined-outer-name,unused-import

import importlib

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import placement_normalization_authority
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_cancelled_aws_paid_cleanup_schema_065_pg')

_MIGRATION = importlib.import_module(
    'sky.schemas.db.serve_state.065_cancelled_aws_paid_cleanup')


def _function_definition(engine: sqlalchemy.engine.Engine,
                         function_name: str) -> str:
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.text('SELECT pg_get_functiondef('
                            'CAST(:signature AS regprocedure))'), {
                                'signature': f'{function_name}()'
                            }).scalar_one()


def test_serve065_lineage_and_runtime_metadata() -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ['065']
    assert scripts.get_revision('065').down_revision == '064'
    assert migration_utils.SERVE_VERSION == '065'
    assert '065' in (
        placement_normalization_authority.RECOGNIZED_ADDITIVE_REVISIONS)


def test_serve065_widens_only_cancelled_aws_terminal_guards(
        empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '064')
    association_guard = _function_definition(
        empty_postgres, _MIGRATION._ASSOCIATION_GUARD_FUNCTION)
    replica_guard = _function_definition(empty_postgres,
                                         _MIGRATION._REPLICA_GUARD_FUNCTION)
    assert _MIGRATION._ASSOCIATION_OLD_TERMINAL_SOURCE in association_guard
    assert _MIGRATION._ASSOCIATION_NEW_TERMINAL_SOURCE in association_guard
    assert _MIGRATION._REPLICA_TERMINAL_SOURCE in replica_guard

    alembic_command.upgrade(config, '065')

    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '065'
    association_guard = _function_definition(
        empty_postgres, _MIGRATION._ASSOCIATION_GUARD_FUNCTION)
    replica_guard = _function_definition(empty_postgres,
                                         _MIGRATION._REPLICA_GUARD_FUNCTION)
    assert (_MIGRATION._ASSOCIATION_OLD_TERMINAL_REPLACEMENT
            in association_guard)
    assert (_MIGRATION._ASSOCIATION_NEW_TERMINAL_REPLACEMENT
            in association_guard)
    assert _MIGRATION._REPLICA_TERMINAL_REPLACEMENT in replica_guard
    with pytest.raises(RuntimeError, match='Serve065 is forward-only'):
        alembic_command.downgrade(config, '064')
