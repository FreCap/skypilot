"""PostgreSQL DDL contracts for project-scoped GCP paid launches."""
# pylint: disable=protected-access,redefined-outer-name,unused-import

import datetime
import importlib
import json
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import ordinary_launch_binding
from sky.serve import placement_normalization_authority
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_project_scoped_gcp_paid_schema_067_pg')

_MIGRATION = importlib.import_module(
    'sky.schemas.db.serve_state.067_project_scoped_gcp_paid_admission')


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


def _pool_key(*,
              version: int,
              use_spot: bool = True,
              provider_identity: object = None) -> str:
    payload = {
        'accelerators': [['l4', 1]],
        'cloud': 'gcp',
        'instance_type': 'g2-standard-4',
        'num_nodes': 1,
        'region': 'us-east4',
        'use_spot': use_spot,
        'version': version,
        'workspace': 'workspace-a',
        'zone': 'us-east4-a',
    }
    if provider_identity is not None:
        payload['provider_identity'] = provider_identity
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))


def _scope_is_accepted(connection: sqlalchemy.engine.Connection,
                       *,
                       pool_key: str,
                       cohort: int,
                       profile_kind: str = 'ORDINARY_PAID') -> bool:
    return bool(
        connection.execute(
            sqlalchemy.text(f'''
                WITH candidate AS (
                    SELECT CAST(:profile_kind AS TEXT) AS profile_kind,
                           CAST(:cohort AS BIGINT) AS capability_cohort_epoch,
                           CAST(:pool_key AS TEXT) AS paid_capacity_pool_key
                )
                SELECT ({_MIGRATION._PAID_POOL_SCOPE_CHECK}) IS TRUE
                FROM candidate
            '''), {
                'profile_kind': profile_kind,
                'cohort': cohort,
                'pool_key': pool_key,
            }).scalar_one())


def _projection_is_accepted(connection: sqlalchemy.engine.Connection,
                            *,
                            pool_key: str,
                            terminal_status: str = 'CANCELLED') -> bool:
    quiesced_at = datetime.datetime(2026,
                                    9,
                                    2,
                                    12,
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
                           CAST(:terminal_status AS TEXT) AS terminal_status,
                           'explicit_cancel'::TEXT AS terminal_cause
                )
                SELECT ({_MIGRATION._PROJECTION_CHECK}) IS TRUE
                FROM candidate
            '''), {
                'observed_at': quiesced_at + datetime.timedelta(seconds=1),
                'quiesced_at': quiesced_at,
                'pool_key': pool_key,
                'terminal_status': terminal_status,
            }).scalar_one())


def _insert_retained_cohort10_row(engine: sqlalchemy.engine.Engine) -> None:
    """Seed one opaque row accepted before provider-scoped paid pools."""
    table = ordinary_launch_binding.ordinary_launch_associations_table
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {table.name} DISABLE TRIGGER USER')
        try:
            connection.execute(
                sqlalchemy.insert(table).values(
                    association_id=uuid.UUID(
                        '67000000-0000-4000-8000-000000000001'),
                    submission_id=uuid.UUID(
                        '67000000-0000-4000-8000-000000000002'),
                    tenant_scope='legacy-tenant',
                    service_name='legacy-service',
                    service_hash='legacy-service-hash',
                    service_workspace='legacy-workspace',
                    service_lifecycle_epoch=1,
                    service_binding_epoch=1,
                    service_version=1,
                    replica_id=1,
                    replica_record_id=uuid.UUID(
                        '67000000-0000-4000-8000-000000000003'),
                    paid_capacity_pool_key='legacy|opaque|paid-pool',
                    launch_generation=1,
                    cluster_name='legacy-service-1',
                    request_id='legacy-request',
                    input_digest='a' * 64,
                    owner_controller_incarnation=uuid.UUID(
                        '67000000-0000-4000-8000-000000000004'),
                    owner_controller_epoch=1,
                    binding_protocol_version=2,
                    profile_kind='ORDINARY_PAID',
                    profile_version=1,
                    profile_digest='b' * 64,
                    capability_cohort_epoch=10,
                    capability_profile_set_digest='c' * 64,
                    receipt_protocol_version=1,
                    authorization_kind='PAID_CAPACITY_CLAIM',
                    authorization_reference='paid-capacity:legacy',
                    authorization_generation=0,
                    authorization_digest='d' * 64,
                    reconciliation_outcome='ACTIVE_ADOPT',
                    provider_evidence='NOT_QUERIED'))
        finally:
            connection.exec_driver_sql(
                f'ALTER TABLE {table.name} ENABLE TRIGGER USER')


def test_serve067_upgrades_exact_constraints_and_guards_without_rewrite(
        empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    assert scripts.get_heads() == ['068']
    assert scripts.get_revision('067').down_revision == '066'
    assert migration_utils.SERVE_VERSION == '068'
    assert '067' in placement_normalization_authority.RECOGNIZED_ADDITIVE_REVISIONS

    alembic_command.upgrade(config, '066')
    _insert_retained_cohort10_row(empty_postgres)
    old_association_guard = _function_definition(
        empty_postgres, _MIGRATION._ASSOCIATION_GUARD_FUNCTION)
    old_replica_guard = _function_definition(empty_postgres,
                                             _MIGRATION._REPLICA_GUARD_FUNCTION)
    assert old_association_guard.count(
        _MIGRATION._ASSOCIATION_OLD_GCP_SOURCE) == 2
    assert old_association_guard.count(
        _MIGRATION._ASSOCIATION_NEW_GCP_SOURCE) == 1
    assert old_replica_guard.count(_MIGRATION._REPLICA_GCP_SOURCE) == 2
    for constraint_name in (_MIGRATION._PAID_POOL_SCOPE_CONSTRAINT,
                            _MIGRATION._PROJECTION_CONSTRAINT):
        assert 'gcp_project_id' not in _constraint_definition(
            empty_postgres, constraint_name)

    alembic_command.upgrade(config, '067')

    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '067'
    runtime_constraints = {
        constraint.name: _compact(str(constraint.sqltext))
        for constraint in
        ordinary_launch_binding.ordinary_launch_associations_table.constraints
        if constraint.name in (_MIGRATION._PAID_POOL_SCOPE_CONSTRAINT,
                               _MIGRATION._PROJECTION_CONSTRAINT)
    }
    assert runtime_constraints[_MIGRATION._PAID_POOL_SCOPE_CONSTRAINT] == (
        _compact(_MIGRATION._PAID_POOL_SCOPE_CHECK))
    assert runtime_constraints[_MIGRATION._PROJECTION_CONSTRAINT] == _compact(
        _MIGRATION._PROJECTION_CHECK)
    for constraint_name in (_MIGRATION._PAID_POOL_SCOPE_CONSTRAINT,
                            _MIGRATION._PROJECTION_CONSTRAINT):
        installed = _constraint_definition(empty_postgres, constraint_name)
        assert 'gcp_project_id' in installed
        assert 'use_spot' in installed

    association_guard = _function_definition(
        empty_postgres, _MIGRATION._ASSOCIATION_GUARD_FUNCTION)
    replica_guard = _function_definition(empty_postgres,
                                         _MIGRATION._REPLICA_GUARD_FUNCTION)
    assert association_guard.count(
        _MIGRATION._ASSOCIATION_OLD_GCP_REPLACEMENT) == 2
    assert association_guard.count(
        _MIGRATION._ASSOCIATION_NEW_GCP_REPLACEMENT) == 1
    assert replica_guard.count(_MIGRATION._REPLICA_GCP_REPLACEMENT) == 2
    with empty_postgres.connect() as connection:
        retained = connection.execute(
            sqlalchemy.text('''
                SELECT paid_capacity_pool_key, capability_cohort_epoch
                FROM serve_ordinary_launch_associations
                WHERE association_id =
                    '67000000-0000-4000-8000-000000000001'
            ''')).one()
    assert retained == ('legacy|opaque|paid-pool', 10)

    with pytest.raises(RuntimeError, match='Serve067 is forward-only'):
        alembic_command.downgrade(config, '066')


def test_serve067_accepts_current_gcp_v2_and_retains_v1_cleanup_semantics(
        empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '067')
    current = _pool_key(version=2,
                        provider_identity={'gcp_project_id': 'boltz-498512'})
    missing_project = _pool_key(version=2, provider_identity={})
    invalid_project = _pool_key(
        version=2, provider_identity={'gcp_project_id': 'INVALID_PROJECT'})
    on_demand = _pool_key(version=2,
                          use_spot=False,
                          provider_identity={'gcp_project_id': 'boltz-498512'})
    retained_v1 = _pool_key(version=1)
    aws_spot_payload = json.loads(current)
    aws_spot_payload.update({
        'cloud': 'aws',
        'instance_type': 'g6.xlarge',
        'provider_identity': {
            'aws_account_id': '123456789012'
        },
    })
    aws_spot = json.dumps(aws_spot_payload,
                          sort_keys=True,
                          separators=(',', ':'))
    aws_spot_payload['use_spot'] = False
    aws_on_demand = json.dumps(aws_spot_payload,
                               sort_keys=True,
                               separators=(',', ':'))

    with empty_postgres.connect() as connection:
        assert _scope_is_accepted(connection, pool_key=current, cohort=15)
        assert _projection_is_accepted(connection, pool_key=current)
        assert _scope_is_accepted(connection, pool_key=aws_spot, cohort=15)
        assert _projection_is_accepted(connection, pool_key=aws_spot)
        assert not _scope_is_accepted(
            connection, pool_key=aws_on_demand, cohort=15)
        assert not _projection_is_accepted(connection, pool_key=aws_on_demand)
        for near_miss in (missing_project, invalid_project, on_demand):
            assert not _scope_is_accepted(
                connection, pool_key=near_miss, cohort=15)
            assert not _projection_is_accepted(connection, pool_key=near_miss)
        assert not _scope_is_accepted(
            connection, pool_key=retained_v1, cohort=15)
        assert _scope_is_accepted(connection, pool_key=retained_v1, cohort=14)
        assert _scope_is_accepted(connection,
                                  pool_key=retained_v1,
                                  cohort=15,
                                  profile_kind='UNKNOWN_CAPACITY_REPLACEMENT')
        assert _projection_is_accepted(connection, pool_key=retained_v1)
