"""PostgreSQL DDL contracts for exact paid provider absence."""
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
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_paid_provider_absence_schema_059_pg')

_MIGRATION = importlib.import_module(
    'sky.schemas.db.serve_state.059_ordinary_paid_provider_absence')
_CURRENT_MIGRATION = importlib.import_module(
    'sky.schemas.db.serve_state.060_cancelled_gcp_paid_cleanup')


def _function_definition(engine: sqlalchemy.engine.Engine,
                         function_name: str) -> str:
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.text('SELECT pg_get_functiondef('
                            'CAST(:signature AS regprocedure))'), {
                                'signature': f'{function_name}()'
                            }).scalar_one()


def _constraint_definition(engine: sqlalchemy.engine.Engine,
                           constraint_name: str) -> str:
    constraints = sqlalchemy.inspect(engine).get_check_constraints(
        ordinary_launch_binding.ordinary_launch_associations_table.name)
    return next(item['sqltext']
                for item in constraints
                if item['name'] == constraint_name)


def _compact(expression: str) -> str:
    return ''.join(expression.split())


def _paid_shape_is_accepted(connection: sqlalchemy.engine.Connection,
                            **overrides) -> bool:
    quiesced_at = datetime.datetime(2026,
                                    8,
                                    25,
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
        'paid_capacity_pool_key': 'aws|eu-south-2a|g6.2xlarge|spot',
        'service_job_id': None,
        'terminal_status': 'FAILED',
        'terminal_cause': 'handler_failed',
    }
    values.update(overrides)
    expression = _MIGRATION._PROJECTION_CHECK
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
                SELECT ({expression}) IS TRUE FROM candidate
            '''), values).scalar_one())


def _paid_pool_scope_is_accepted(
    connection: sqlalchemy.engine.Connection,
    *,
    profile_kind: str = 'ORDINARY_PAID',
    capability_cohort_epoch: int = 11,
    paid_capacity_pool_key: str | None,
) -> bool:
    expression = _MIGRATION._PAID_POOL_SCOPE_CHECK
    return bool(
        connection.execute(
            sqlalchemy.text(f'''
                WITH candidate AS (
                    SELECT
                        CAST(:profile_kind AS TEXT) AS profile_kind,
                        CAST(:capability_cohort_epoch AS BIGINT)
                            AS capability_cohort_epoch,
                        CAST(:paid_capacity_pool_key AS TEXT)
                            AS paid_capacity_pool_key
                )
                SELECT ({expression}) IS TRUE FROM candidate
            '''), {
                'profile_kind': profile_kind,
                'capability_cohort_epoch': capability_cohort_epoch,
                'paid_capacity_pool_key': paid_capacity_pool_key,
            }).scalar_one())


def _paid_receipt_scope_is_accepted(
    connection: sqlalchemy.engine.Connection,
    *,
    profile_kind: str = 'ORDINARY_PAID',
    capability_cohort_epoch: int = 11,
    provider_evidence: str = 'ABSENT',
    paid_capacity_pool_key: str,
    provider_evidence_payload,
) -> bool:
    expression = _MIGRATION._PAID_RECEIPT_SCOPE_CHECK
    return bool(
        connection.execute(
            sqlalchemy.text(f'''
                WITH candidate AS (
                    SELECT
                        CAST(:profile_kind AS TEXT) AS profile_kind,
                        CAST(:capability_cohort_epoch AS BIGINT)
                            AS capability_cohort_epoch,
                        CAST(:provider_evidence AS TEXT) AS provider_evidence,
                        CAST(:paid_capacity_pool_key AS TEXT)
                            AS paid_capacity_pool_key,
                        CAST(:provider_evidence_payload AS JSONB)
                            AS provider_evidence_payload
                )
                SELECT ({expression}) IS TRUE FROM candidate
            '''), {
                'profile_kind': profile_kind,
                'capability_cohort_epoch': capability_cohort_epoch,
                'provider_evidence': provider_evidence,
                'paid_capacity_pool_key': paid_capacity_pool_key,
                'provider_evidence_payload':
                    json.dumps(provider_evidence_payload),
            }).scalar_one())


def _paid_pool_key(*, cloud: str, provider_identity, version: int = 2) -> str:
    payload = {
        'version': version,
        'workspace': 'default',
        'cloud': cloud,
        'region': 'region-a',
        'zone': 'zone-a',
        'instance_type': 'gpu.large',
        'accelerators': [['l4', 1]],
        'use_spot': True,
        'num_nodes': 1,
    }
    if version == 2:
        payload['provider_identity'] = provider_identity
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))


def _insert_legacy_non_json_paid_association(
        engine: sqlalchemy.engine.Engine) -> uuid.UUID:
    """Seed one retained cohort-10 row without invoking live graph guards."""
    association_id = uuid.UUID('59100000-0000-4000-8000-000000000001')
    table = ordinary_launch_binding.ordinary_launch_associations_table
    with engine.begin() as connection:
        # This migration fixture deliberately has no live service/replica graph.
        # Disable only this table's user triggers while seeding the otherwise
        # constraint-valid retained row, then restore them before migration.
        connection.exec_driver_sql(
            f'ALTER TABLE {table.name} DISABLE TRIGGER USER')
        try:
            connection.execute(
                sqlalchemy.insert(table).values(
                    association_id=association_id,
                    submission_id=uuid.UUID(
                        '59100000-0000-4000-8000-000000000002'),
                    tenant_scope='legacy-tenant',
                    service_name='legacy-service',
                    service_hash='legacy-service-hash',
                    service_workspace='legacy-workspace',
                    service_lifecycle_epoch=1,
                    service_binding_epoch=1,
                    service_version=1,
                    replica_id=1,
                    replica_record_id=uuid.UUID(
                        '59100000-0000-4000-8000-000000000003'),
                    paid_capacity_pool_key='legacy|opaque|paid-pool',
                    launch_generation=1,
                    cluster_name='legacy-service-1',
                    request_id='legacy-request',
                    input_digest='a' * 64,
                    owner_controller_incarnation=uuid.UUID(
                        '59100000-0000-4000-8000-000000000004'),
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
    return association_id


def test_serve059_lineage_and_runtime_metadata() -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ['060']
    assert scripts.get_revision('060').down_revision == '059'
    assert scripts.get_revision('059').down_revision == '058'
    assert migration_utils.SERVE_VERSION == '060'
    assert migration_utils.serve_target_version(sqlite) == '037'

    constraint = next(
        item for item in
        ordinary_launch_binding.ordinary_launch_associations_table.constraints
        if item.name == _MIGRATION._PROJECTION_CONSTRAINT)
    assert _compact(str(constraint.sqltext)) == _compact(
        _CURRENT_MIGRATION._PROJECTION_CHECK)
    paid_pool_constraint = next(
        item for item in
        ordinary_launch_binding.ordinary_launch_associations_table.constraints
        if item.name == _MIGRATION._PAID_POOL_SCOPE_CONSTRAINT)
    assert _compact(str(paid_pool_constraint.sqltext)) == _compact(
        _MIGRATION._PAID_POOL_SCOPE_CHECK)
    paid_receipt_constraint = next(
        item for item in
        ordinary_launch_binding.ordinary_launch_associations_table.constraints
        if item.name == _MIGRATION._PAID_RECEIPT_SCOPE_CONSTRAINT)
    assert _compact(str(paid_receipt_constraint.sqltext)) == _compact(
        _MIGRATION._PAID_RECEIPT_SCOPE_CHECK)


def test_serve059_migrates_all_three_database_gates(empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '058')

    old_constraint = _constraint_definition(empty_postgres,
                                            _MIGRATION._PROJECTION_CONSTRAINT)
    old_association_guard = _function_definition(
        empty_postgres, _MIGRATION._ASSOCIATION_GUARD_FUNCTION)
    old_replica_guard = _function_definition(empty_postgres,
                                             _MIGRATION._REPLICA_GUARD_FUNCTION)
    assert "profile_kind = 'ORDINARY_PAID'" not in old_constraint
    assert _MIGRATION._ASSOCIATION_TRANSITION_SOURCE in old_association_guard
    assert (_MIGRATION._ASSOCIATION_TRANSITION_REPLACEMENT
            not in old_association_guard)
    assert _MIGRATION._REPLICA_POINTER_SOURCE in old_replica_guard
    assert _MIGRATION._REPLICA_POINTER_REPLACEMENT not in old_replica_guard
    legacy_association_id = _insert_legacy_non_json_paid_association(
        empty_postgres)

    alembic_command.upgrade(config, '059')

    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '059'
    new_constraint = _constraint_definition(empty_postgres,
                                            _MIGRATION._PROJECTION_CONSTRAINT)
    paid_pool_constraint = _constraint_definition(
        empty_postgres, _MIGRATION._PAID_POOL_SCOPE_CONSTRAINT)
    assert "profile_kind = 'RESERVED_FILL'" in new_constraint
    assert "profile_kind = 'ORDINARY_PAID'" in new_constraint
    assert 'paid_capacity_pool_key IS NOT NULL' in new_constraint
    assert 'service_job_id IS NULL' in new_constraint
    assert "terminal_status = 'FAILED'" in new_constraint
    assert "terminal_cause = 'handler_failed'" in new_constraint
    assert "->> 'version'" in paid_pool_constraint
    assert "->> 'cloud'" in paid_pool_constraint
    assert "'aws_account_id'" in paid_pool_constraint
    assert "'null'::jsonb" not in paid_pool_constraint
    with empty_postgres.connect() as connection:
        retained_pool_key = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                paid_capacity_pool_key).where(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.association_id == legacy_association_id)).scalar_one()
    assert retained_pool_key == 'legacy|opaque|paid-pool'
    assert _MIGRATION._ASSOCIATION_TRANSITION_REPLACEMENT in (
        _function_definition(empty_postgres,
                             _MIGRATION._ASSOCIATION_GUARD_FUNCTION))
    assert _MIGRATION._REPLICA_POINTER_REPLACEMENT in _function_definition(
        empty_postgres, _MIGRATION._REPLICA_GUARD_FUNCTION)

    with pytest.raises(RuntimeError, match='Serve059 is forward-only'):
        alembic_command.downgrade(config, '058')
    assert migration_utils.get_current_alembic_revision(
        empty_postgres, migration_utils.SERVE_DB_NAME) == '059'


def test_serve059_paid_projection_shape_is_closed(empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '059')
    quiesced_at = datetime.datetime(2026,
                                    8,
                                    25,
                                    9,
                                    0,
                                    tzinfo=datetime.timezone.utc)
    with empty_postgres.connect() as connection:
        assert _paid_shape_is_accepted(connection)
        assert not _paid_shape_is_accepted(connection,
                                           binding_protocol_version=1)
        # Serve059 must retain the existing reserved-fill projection arm.
        assert _paid_shape_is_accepted(connection,
                                       profile_kind='RESERVED_FILL',
                                       effect_phase='NOT_STARTED',
                                       paid_capacity_pool_key=None)
        assert not _paid_shape_is_accepted(connection,
                                           profile_kind='ORDINARY_ZERO_COST')
        assert not _paid_shape_is_accepted(
            connection, reconciliation_outcome='POST_EFFECT_AMBIGUOUS')
        assert not _paid_shape_is_accepted(connection,
                                           provider_evidence='UNKNOWN')
        assert not _paid_shape_is_accepted(
            connection,
            provider_evidence_observed_at=(quiesced_at -
                                           datetime.timedelta(seconds=1)))
        assert not _paid_shape_is_accepted(connection,
                                           execution_quiesced_at=None)
        assert not _paid_shape_is_accepted(connection,
                                           effect_phase='SERVICE_JOB_IO')
        assert not _paid_shape_is_accepted(connection,
                                           paid_capacity_pool_key=None)
        assert not _paid_shape_is_accepted(connection, service_job_id=41)
        assert not _paid_shape_is_accepted(connection,
                                           terminal_status='CANCELLED')
        assert not _paid_shape_is_accepted(connection,
                                           terminal_cause='cancelled')


def test_serve059_paid_pool_scope_keeps_non_aws_paid_admission(
        empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '059')
    aws_key = _paid_pool_key(
        cloud='aws', provider_identity={'aws_account_id': '123456789012'})
    stale_gcp_v2_key = _paid_pool_key(cloud='gcp', provider_identity=None)
    gcp_v1_key = _paid_pool_key(cloud='gcp', provider_identity=None, version=1)
    legacy_key = _paid_pool_key(cloud='aws', provider_identity=None, version=1)

    with empty_postgres.connect() as connection:
        assert _paid_pool_scope_is_accepted(connection,
                                            paid_capacity_pool_key=aws_key)
        assert _paid_pool_scope_is_accepted(connection,
                                            paid_capacity_pool_key=gcp_v1_key)
        assert not _paid_pool_scope_is_accepted(
            connection, paid_capacity_pool_key=stale_gcp_v2_key)
        assert not _paid_pool_scope_is_accepted(
            connection, paid_capacity_pool_key=legacy_key)
        assert _paid_pool_scope_is_accepted(connection,
                                            capability_cohort_epoch=10,
                                            paid_capacity_pool_key=legacy_key)
        assert _paid_pool_scope_is_accepted(
            connection,
            capability_cohort_epoch=10,
            paid_capacity_pool_key='legacy|opaque|paid-pool')
        assert not _paid_pool_scope_is_accepted(connection,
                                                paid_capacity_pool_key=None)
        assert not _paid_pool_scope_is_accepted(
            connection,
            paid_capacity_pool_key=_paid_pool_key(cloud='aws',
                                                  provider_identity=None))
        assert not _paid_pool_scope_is_accepted(
            connection,
            paid_capacity_pool_key=_paid_pool_key(
                cloud='aws',
                provider_identity={'aws_account_id': 'not-an-account'}))
        assert not _paid_pool_scope_is_accepted(
            connection,
            paid_capacity_pool_key=_paid_pool_key(
                cloud='gcp',
                provider_identity={'aws_account_id': '123456789012'}))


def test_serve059_paid_receipt_scope_rejects_null_authority(
        empty_postgres) -> None:
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '059')
    pool_key = _paid_pool_key(
        cloud='aws', provider_identity={'aws_account_id': '123456789012'})
    gcp_pool_key = _paid_pool_key(cloud='gcp',
                                  provider_identity=None,
                                  version=1)
    receipt = {
        'receipt': {
            'aws_account_id': '123456789012',
            'client_token': 'a' * 64,
        }
    }
    with empty_postgres.connect() as connection:
        assert _paid_receipt_scope_is_accepted(
            connection,
            paid_capacity_pool_key=pool_key,
            provider_evidence_payload=receipt)
        assert not _paid_receipt_scope_is_accepted(
            connection,
            paid_capacity_pool_key=pool_key,
            provider_evidence_payload={'receipt': {
                'client_token': 'a' * 64,
            }})
        assert not _paid_receipt_scope_is_accepted(
            connection,
            paid_capacity_pool_key=pool_key,
            provider_evidence_payload={
                'receipt': {
                    'aws_account_id': '123456789012',
                }
            })
        assert _paid_receipt_scope_is_accepted(
            connection,
            profile_kind='RESERVED_FILL',
            paid_capacity_pool_key='legacy|opaque|paid-pool',
            provider_evidence_payload={})
        assert _paid_receipt_scope_is_accepted(
            connection,
            paid_capacity_pool_key=gcp_pool_key,
            provider_evidence_payload={'probe_contract': 'gcp-read-v1'})
        assert _paid_receipt_scope_is_accepted(
            connection,
            capability_cohort_epoch=10,
            paid_capacity_pool_key='legacy|opaque|paid-pool',
            provider_evidence_payload={})
