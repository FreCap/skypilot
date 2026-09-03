"""PostgreSQL DDL contracts for infrastructure-terminal paid cleanup."""
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

from sky.events import api_models as event_api_models
from sky.serve import ordinary_launch_binding
from sky.serve import placement_normalization_authority
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_infrastructure_terminal_cleanup_schema_068_pg')

_MIGRATION = importlib.import_module(
    'sky.schemas.db.serve_state.068_infrastructure_terminal_paid_cleanup')


def _compact(expression: str) -> str:
    return ''.join(expression.split())


def _constraint_definition(engine: sqlalchemy.engine.Engine,
                           constraint_name: str) -> str:
    constraints = sqlalchemy.inspect(engine).get_check_constraints(
        ordinary_launch_binding.ordinary_launch_associations_table.name)
    return next(item['sqltext']
                for item in constraints
                if item['name'] == constraint_name)


def _function_definition(engine: sqlalchemy.engine.Engine,
                         function_name: str) -> str:
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.text('SELECT pg_get_functiondef('
                            'CAST(:signature AS regprocedure))'), {
                                'signature': f'{function_name}()'
                            }).scalar_one()


def _pool_key(cloud: str, *, version: int = 2, use_spot: bool = True) -> str:
    payload = {
        'accelerators': [['l4', 1]],
        'cloud': cloud,
        'instance_type': ('g6.2xlarge' if cloud == 'aws' else 'g2-standard-4'),
        'num_nodes': 1,
        'provider_identity': ({
            'aws_account_id': '123456789012'
        } if cloud == 'aws' else {
            'gcp_project_id': 'boltz-498512'
        }),
        'region': 'us-east-2' if cloud == 'aws' else 'us-east4',
        'use_spot': use_spot,
        'version': version,
        'workspace': 'workspace-a',
        'zone': 'us-east-2a' if cloud == 'aws' else 'us-east4-a',
    }
    if cloud == 'gcp' and version == 1:
        payload.pop('provider_identity')
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))


def _projection_is_accepted(connection: sqlalchemy.engine.Connection, *,
                            pool_key: str, status: str, cause: str) -> bool:
    quiesced_at = datetime.datetime(2026,
                                    9,
                                    3,
                                    1,
                                    0,
                                    tzinfo=datetime.timezone.utc)
    return bool(
        connection.execute(
            sqlalchemy.text(f'''
                WITH candidate AS (
                    SELECT 'PROJECTED'::TEXT AS resolution,
                           'PROVIDER_IO'::TEXT AS effect_phase,
                           2::INTEGER AS binding_protocol_version,
                           'ORDINARY_PAID'::TEXT AS profile_kind,
                           'PROJECTED'::TEXT AS reconciliation_outcome,
                           'ABSENT'::TEXT AS provider_evidence,
                           CAST(:observed_at AS TIMESTAMPTZ)
                               AS provider_evidence_observed_at,
                           CAST(:quiesced_at AS TIMESTAMPTZ)
                               AS execution_quiesced_at,
                           CAST(:pool_key AS TEXT) AS paid_capacity_pool_key,
                           NULL::BIGINT AS service_job_id,
                           CAST(:status AS TEXT) AS terminal_status,
                           CAST(:cause AS TEXT) AS terminal_cause
                )
                SELECT ({_MIGRATION._PROJECTION_CHECK}) IS TRUE
                FROM candidate
            '''), {
                'cause': cause,
                'observed_at': quiesced_at + datetime.timedelta(seconds=1),
                'pool_key': pool_key,
                'quiesced_at': quiesced_at,
                'status': status,
            }).scalar_one())


def test_serve068_upgrades_exact_067_guards(empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    assert scripts.get_heads() == ['068']
    assert scripts.get_revision('068').down_revision == '067'
    assert migration_utils.SERVE_VERSION == '068'
    assert '068' in placement_normalization_authority.RECOGNIZED_ADDITIVE_REVISIONS

    alembic_command.upgrade(config, '067')
    association_before = _function_definition(
        empty_postgres, _MIGRATION._ASSOCIATION_GUARD_FUNCTION)
    replica_before = _function_definition(empty_postgres,
                                          _MIGRATION._REPLICA_GUARD_FUNCTION)
    assert association_before.count(
        _MIGRATION._ASSOCIATION_OLD_TERMINAL_SOURCE) == 1
    assert association_before.count(
        _MIGRATION._ASSOCIATION_NEW_TERMINAL_SOURCE) == 1
    assert replica_before.count(_MIGRATION._REPLICA_TERMINAL_SOURCE) == 1

    alembic_command.upgrade(config, '068')

    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '068'
    association_after = _function_definition(
        empty_postgres, _MIGRATION._ASSOCIATION_GUARD_FUNCTION)
    replica_after = _function_definition(empty_postgres,
                                         _MIGRATION._REPLICA_GUARD_FUNCTION)
    assert association_after.count(
        _MIGRATION._ASSOCIATION_OLD_TERMINAL_REPLACEMENT) == 1
    assert association_after.count(
        _MIGRATION._ASSOCIATION_NEW_TERMINAL_REPLACEMENT) == 1
    assert replica_after.count(_MIGRATION._REPLICA_TERMINAL_REPLACEMENT) == 1
    runtime_projection = next(
        constraint for constraint in
        ordinary_launch_binding.ordinary_launch_associations_table.constraints
        if constraint.name == _MIGRATION._PROJECTION_CONSTRAINT)
    assert _compact(str(runtime_projection.sqltext)) == _compact(
        _MIGRATION._PROJECTION_CHECK)
    installed_projection = _constraint_definition(
        empty_postgres, _MIGRATION._PROJECTION_CONSTRAINT)
    assert 'SUCCEEDED' in installed_projection
    assert 'terminal_cause' in installed_projection
    assert 'dispatcher_submit_failed' not in installed_projection
    assert 'execution_lease_expired' not in installed_projection
    assert _compact(ordinary_launch_binding._ORDINARY_PAID_PROVIDER_TERMINAL_SQL
                   ) == _compact(_MIGRATION._TERMINAL_CHECK)

    with pytest.raises(RuntimeError, match='Serve068 is forward-only'):
        alembic_command.downgrade(config, '067')


def test_serve068_exact_v2_cleanup_ignores_diagnostic_terminal_cause(
        empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '068')

    with empty_postgres.connect() as connection:
        for status in ordinary_launch_binding.TerminalStatus:
            for cause in event_api_models.EventCause:
                assert _projection_is_accepted(connection,
                                               pool_key=_pool_key('aws'),
                                               status=status.value,
                                               cause=cause.value)
                assert _projection_is_accepted(connection,
                                               pool_key=_pool_key('gcp'),
                                               status=status.value,
                                               cause=cause.value)
                expected_legacy = bool(
                    (status is ordinary_launch_binding.TerminalStatus.FAILED and
                     cause is event_api_models.EventCause.HANDLER_FAILED) or
                    (status is ordinary_launch_binding.TerminalStatus.CANCELLED
                     and cause is event_api_models.EventCause.EXPLICIT_CANCEL))
                assert _projection_is_accepted(
                    connection,
                    pool_key=_pool_key('gcp', version=1),
                    status=status.value,
                    cause=cause.value) is expected_legacy


def test_serve068_structural_terminal_gate_is_future_safe(
        empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '068')

    with empty_postgres.connect() as connection:
        assert _projection_is_accepted(connection,
                                       pool_key=_pool_key('aws'),
                                       status='FAILED',
                                       cause='future_terminal_cause')
        assert not _projection_is_accepted(
            connection, pool_key=_pool_key('aws'), status='FAILED', cause='')
        assert not _projection_is_accepted(connection,
                                           pool_key=_pool_key('aws'),
                                           status='PENDING',
                                           cause='future_terminal_cause')
        assert not _projection_is_accepted(connection,
                                           pool_key=_pool_key('aws',
                                                              use_spot=False),
                                           status='FAILED',
                                           cause='future_terminal_cause')
