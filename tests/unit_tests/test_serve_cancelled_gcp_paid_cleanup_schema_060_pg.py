"""PostgreSQL DDL contracts for cancelled GCP paid cleanup."""
# pylint: disable=protected-access,redefined-outer-name,unused-import

import datetime
import importlib
import json

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import ordinary_launch_binding
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_cancelled_gcp_paid_cleanup_schema_060_pg')

_MIGRATION = importlib.import_module(
    'sky.schemas.db.serve_state.060_cancelled_gcp_paid_cleanup')


def _function_definition(engine: sqlalchemy.engine.Engine,
                         function_name: str) -> str:
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.text('SELECT pg_get_functiondef('
                            'CAST(:signature AS regprocedure))'), {
                                'signature': f'{function_name}()'
                            }).scalar_one()


def _constraint_definition(engine: sqlalchemy.engine.Engine) -> str:
    constraints = sqlalchemy.inspect(engine).get_check_constraints(
        ordinary_launch_binding.ordinary_launch_associations_table.name)
    return next(item['sqltext']
                for item in constraints
                if item['name'] == _MIGRATION._PROJECTION_CONSTRAINT)


def _compact(expression: str) -> str:
    return ''.join(expression.split())


def _pool_key(*,
              cloud: str = 'gcp',
              use_spot: bool = True,
              version: int = 1) -> str:
    return json.dumps(
        {
            'accelerators': [['l4', 1]],
            'cloud': cloud,
            'instance_type': 'g2-standard-4',
            'num_nodes': 1,
            'region': 'us-east4',
            'use_spot': use_spot,
            'version': version,
            'workspace': 'workspace-a',
            'zone': 'us-east4-a',
        },
        sort_keys=True,
        separators=(',', ':'))


def _projection_shape_is_accepted(connection: sqlalchemy.engine.Connection,
                                  **overrides) -> bool:
    quiesced_at = datetime.datetime(2026,
                                    8,
                                    26,
                                    9,
                                    0,
                                    tzinfo=datetime.timezone.utc)
    values = {
        'resolution': 'PROJECTED',
        'effect_phase': 'PROVIDER_IO',
        'binding_protocol_version': 2,
        'profile_kind': 'ORDINARY_PAID',
        'reconciliation_outcome': 'PROJECTED',
        'provider_evidence': 'ABSENT',
        'provider_evidence_observed_at': quiesced_at +
                                         datetime.timedelta(seconds=1),
        'execution_quiesced_at': quiesced_at,
        'paid_capacity_pool_key': _pool_key(),
        'service_job_id': None,
        'terminal_status': 'CANCELLED',
        'terminal_cause': 'explicit_cancel',
    }
    values.update(overrides)
    return bool(
        connection.execute(
            sqlalchemy.text(f'''
                WITH candidate AS (
                    SELECT
                        CAST(:resolution AS TEXT) AS resolution,
                        CAST(:effect_phase AS TEXT) AS effect_phase,
                        CAST(:binding_protocol_version AS INTEGER)
                            AS binding_protocol_version,
                        CAST(:profile_kind AS TEXT) AS profile_kind,
                        CAST(:reconciliation_outcome AS TEXT)
                            AS reconciliation_outcome,
                        CAST(:provider_evidence AS TEXT) AS provider_evidence,
                        CAST(:provider_evidence_observed_at AS TIMESTAMPTZ)
                            AS provider_evidence_observed_at,
                        CAST(:execution_quiesced_at AS TIMESTAMPTZ)
                            AS execution_quiesced_at,
                        CAST(:paid_capacity_pool_key AS TEXT)
                            AS paid_capacity_pool_key,
                        CAST(:service_job_id AS BIGINT) AS service_job_id,
                        CAST(:terminal_status AS TEXT) AS terminal_status,
                        CAST(:terminal_cause AS TEXT) AS terminal_cause
                )
                SELECT ({_MIGRATION._PROJECTION_CHECK}) IS TRUE FROM candidate
            '''), values).scalar_one())


def test_serve060_lineage_and_runtime_metadata() -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ['066']
    assert scripts.get_revision('060').down_revision == '059'
    assert migration_utils.SERVE_VERSION == '066'
    constraint = next(
        item for item in
        ordinary_launch_binding.ordinary_launch_associations_table.constraints
        if item.name == _MIGRATION._PROJECTION_CONSTRAINT)
    assert _compact(str(constraint.sqltext)) == _compact(
        _MIGRATION._PROJECTION_CHECK)


def test_serve060_migrates_the_live_059_constraint_and_guards(
        empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '059')
    association_guard = _function_definition(
        empty_postgres, _MIGRATION._ASSOCIATION_GUARD_FUNCTION)
    replica_guard = _function_definition(empty_postgres,
                                         _MIGRATION._REPLICA_GUARD_FUNCTION)
    assert _MIGRATION._ASSOCIATION_OLD_TERMINAL_SOURCE in association_guard
    assert _MIGRATION._ASSOCIATION_NEW_TERMINAL_SOURCE in association_guard
    assert _MIGRATION._REPLICA_TERMINAL_SOURCE in replica_guard

    alembic_command.upgrade(config, '060')

    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '060'
    constraint = _constraint_definition(empty_postgres)
    assert "terminal_status = 'CANCELLED'::text" in constraint
    assert "terminal_cause = 'explicit_cancel'::text" in constraint
    assert "->> 'cloud'::text) = 'gcp'::text" in constraint
    assert "->> 'version'::text) = '1'::text" in constraint
    assert "->> 'use_spot'::text) = 'true'::text" in constraint
    association_guard = _function_definition(
        empty_postgres, _MIGRATION._ASSOCIATION_GUARD_FUNCTION)
    replica_guard = _function_definition(empty_postgres,
                                         _MIGRATION._REPLICA_GUARD_FUNCTION)
    assert _MIGRATION._ASSOCIATION_OLD_TERMINAL_REPLACEMENT in association_guard
    assert _MIGRATION._ASSOCIATION_NEW_TERMINAL_REPLACEMENT in association_guard
    assert _MIGRATION._REPLICA_TERMINAL_REPLACEMENT in replica_guard
    with pytest.raises(RuntimeError, match='Serve060 is forward-only'):
        alembic_command.downgrade(config, '059')


def test_serve060_projection_shape_is_exact_gcp_v1_spot(empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '060')
    with empty_postgres.connect() as connection:
        assert _projection_shape_is_accepted(connection)
        assert not _projection_shape_is_accepted(
            connection, paid_capacity_pool_key=_pool_key(use_spot=False))
        assert not _projection_shape_is_accepted(
            connection, paid_capacity_pool_key=_pool_key(cloud='aws'))
        assert not _projection_shape_is_accepted(
            connection, paid_capacity_pool_key=_pool_key(version=2))
        assert not _projection_shape_is_accepted(
            connection, terminal_cause='handler_failed')
        assert _projection_shape_is_accepted(
            connection,
            paid_capacity_pool_key=_pool_key(cloud='aws'),
            terminal_status='FAILED',
            terminal_cause='handler_failed')
