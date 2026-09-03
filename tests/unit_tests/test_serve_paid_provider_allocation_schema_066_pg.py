"""PostgreSQL DDL contracts for paid provider-allocation feedback."""
# pylint: disable=protected-access,redefined-outer-name,unused-import

import datetime
import importlib

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import placement_normalization_authority
from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_paid_provider_allocation_schema_066_pg')

_MIGRATION = importlib.import_module(
    'sky.schemas.db.serve_state.066_paid_provider_allocation_feedback')


def _set_receipt(engine: sqlalchemy.engine.Engine, *, recorded_at,
                 receipt_sha256: str | None) -> None:
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('''
                UPDATE paid_capacity_claims
                SET provider_allocation_recorded_at = :recorded_at,
                    provider_allocation_receipt_sha256 = :receipt_sha256
                WHERE service_name = 'service-a'
                  AND service_hash = 'service-hash-a'
                  AND replica_id = 1
            '''), {
                'recorded_at': recorded_at,
                'receipt_sha256': receipt_sha256,
            })


def test_serve066_lineage_and_runtime_metadata() -> None:
    sqlite_engine = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite_engine,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ['068']
    assert scripts.get_revision('066').down_revision == '065'
    assert migration_utils.SERVE_VERSION == '068'
    assert '066' in (
        placement_normalization_authority.RECOGNIZED_ADDITIVE_REVISIONS)

    table = serve_state_schema.paid_capacity_claims_table
    sqlite_ddl = str(
        sqlalchemy.schema.CreateTable(table).compile(dialect=sqlite.dialect()))
    postgres_ddl = str(
        sqlalchemy.schema.CreateTable(table).compile(
            dialect=postgresql.dialect()))
    for constraint_name in (_MIGRATION._COMPLETE_CONSTRAINT,
                            _MIGRATION._DIGEST_CONSTRAINT):
        assert constraint_name not in sqlite_ddl
        assert constraint_name in postgres_ddl


def test_serve066_adds_complete_validated_receipt_pair(empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '065')

    columns = {
        column['name'] for column in sqlalchemy.inspect(
            empty_postgres).get_columns(_MIGRATION._CLAIMS)
    }
    assert _MIGRATION._RECORDED_AT not in columns
    assert _MIGRATION._RECEIPT_SHA256 not in columns
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.text('''
                INSERT INTO paid_capacity_pools (
                    pool_key, current_limit, successes_since_resize,
                    updated_at)
                VALUES ('pool-a', 1, 0, 1.0)
            '''))
        connection.execute(
            sqlalchemy.text('''
                INSERT INTO paid_capacity_claims (
                    service_name, service_hash, replica_id, pool_key,
                    priority, claimed_at)
                VALUES (
                    'service-a', 'service-hash-a', 1, 'pool-a', 1, 1.0)
            '''))

    alembic_command.upgrade(config, '066')

    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '066'
    columns = {
        column['name']: column for column in sqlalchemy.inspect(
            empty_postgres).get_columns(_MIGRATION._CLAIMS)
    }
    assert columns[_MIGRATION._RECORDED_AT]['nullable'] is True
    assert columns[_MIGRATION._RECEIPT_SHA256]['nullable'] is True
    constraints = {
        constraint['name'] for constraint in sqlalchemy.inspect(
            empty_postgres).get_check_constraints(_MIGRATION._CLAIMS)
    }
    assert _MIGRATION._COMPLETE_CONSTRAINT in constraints
    assert _MIGRATION._DIGEST_CONSTRAINT in constraints

    with empty_postgres.connect() as connection:
        retained = connection.execute(
            sqlalchemy.text('''
                SELECT provider_allocation_recorded_at,
                       provider_allocation_receipt_sha256
                FROM paid_capacity_claims
                WHERE service_name = 'service-a'
                  AND service_hash = 'service-hash-a'
                  AND replica_id = 1
            ''')).one()
    assert retained == (None, None)

    recorded_at = datetime.datetime(2026,
                                    9,
                                    1,
                                    12,
                                    0,
                                    tzinfo=datetime.timezone.utc)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        _set_receipt(empty_postgres,
                     recorded_at=recorded_at,
                     receipt_sha256=None)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        _set_receipt(empty_postgres, recorded_at=None, receipt_sha256='a' * 64)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        _set_receipt(empty_postgres,
                     recorded_at=recorded_at,
                     receipt_sha256='not-a-sha256')

    _set_receipt(empty_postgres,
                 recorded_at=recorded_at,
                 receipt_sha256='a' * 64)
    with empty_postgres.connect() as connection:
        retained = connection.execute(
            sqlalchemy.text('''
                SELECT provider_allocation_recorded_at,
                       provider_allocation_receipt_sha256
                FROM paid_capacity_claims
                WHERE service_name = 'service-a'
                  AND service_hash = 'service-hash-a'
                  AND replica_id = 1
            ''')).one()
    assert retained == (recorded_at, 'a' * 64)

    with pytest.raises(RuntimeError, match='Serve066 is forward-only'):
        alembic_command.downgrade(config, '065')
