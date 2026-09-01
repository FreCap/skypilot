"""Schema and migration contracts for PostgreSQL-only SkyServe revision 039."""
# pylint: disable=not-callable,protected-access,redefined-outer-name

import datetime
import importlib
import os
from pathlib import Path
import shutil
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy

from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_action_state_schema as action_schema
from sky.utils.db import migration_utils

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
_VERSION_TABLE = 'alembic_version_serve_state_db'
_NOW = datetime.datetime(2026, 8, 4, 12, 0, tzinfo=datetime.timezone.utc)
_SERVICE_HASH = '11111111-1111-4111-8111-111111111111'
_SHA_A = 'a' * 64

_NEW_TABLES = {
    'serve_resource_action_execution_authority_lineage',
    'serve_resource_action_attempt_terminal_authority',
    'serve_resource_action_shadow_request_terminal_history',
    'serve_resource_action_shadow_admission_fallback_history',
    'serve_resource_action_shadow_admission_fallback_progress_log',
    'serve_resource_action_shadow_settlement_history',
    'serve_resource_action_shadow_execution_history',
    'serve_resource_action_worker_process_supersessions',
    'serve_resource_action_api_instance_gc_cursors',
}


@pytest.fixture(scope='module')
def postgres_engine():
    pytest.importorskip('psycopg2')
    container = None
    admin_engine = None
    temporary_database = None
    if _POSTGRES_URL is None:
        if shutil.which('docker') is None:
            pytest.skip('docker unavailable; skipping PostgreSQL migration')
        testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
        try:
            container = testcontainers_postgres.PostgresContainer('postgres:16')
            container.start()
        except Exception as e:  # pylint: disable=broad-except
            pytest.skip(f'could not start postgres container: {e}')
        assert container is not None
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_serve_039_{uuid.uuid4().hex}'
        admin_engine = sqlalchemy.create_engine(_POSTGRES_URL,
                                                isolation_level='AUTOCOMMIT')
        quoted = admin_engine.dialect.identifier_preparer.quote(
            temporary_database)
        try:
            with admin_engine.connect() as connection:
                connection.exec_driver_sql(f'CREATE DATABASE {quoted}')
        except Exception as e:  # pylint: disable=broad-except
            admin_engine.dispose()
            pytest.skip(f'could not create temporary postgres database: {e}')
        postgres_url = sqlalchemy.engine.make_url(_POSTGRES_URL).set(
            database=temporary_database).render_as_string(hide_password=False)
    engine = sqlalchemy.create_engine(postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()
        if temporary_database is not None:
            assert admin_engine is not None
            quoted = admin_engine.dialect.identifier_preparer.quote(
                temporary_database)
            with admin_engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text('SELECT pg_terminate_backend(pid) '
                                    'FROM pg_stat_activity '
                                    'WHERE datname = :database AND '
                                    'pid <> pg_backend_pid()'),
                    {'database': temporary_database})
                connection.exec_driver_sql(f'DROP DATABASE {quoted}')
            admin_engine.dispose()
        elif container is not None:
            container.stop()


def _reset(engine: sqlalchemy.engine.Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')


def _upgrade(engine: sqlalchemy.engine.Engine, revision: str) -> None:
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         revision)


def _revision(engine: sqlalchemy.engine.Engine) -> str | None:
    return migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)


def _migration_module():
    return importlib.import_module(
        'sky.schemas.db.serve_state.039_serve_resource_action_execution_history'
    )


def _insert_038_shadow_parent(connection: sqlalchemy.engine.Connection,
                              decision_id: uuid.UUID) -> None:
    connection.execute(m4_schema.SHADOW_COVERAGE_V2.insert().values(
        decision_id=decision_id,
        service_name='svc',
        service_hash=_SERVICE_HASH,
        service_incarnation=uuid.UUID(_SERVICE_HASH),
        replica_id=0,
        replica_incarnation=uuid.uuid4(),
        desired_generation=1,
        action_type='launch',
        normalizer_contract_version=1,
        normalization_outcome='REPRESENTABLE',
        not_representable_reason=None,
        worker_cohort_ref_id=None,
        admitted_at=_NOW,
        candidate_epoch=uuid.uuid4(),
        qualification_policy_sha256=_SHA_A,
        qualification_binding_sha256=_SHA_A))
    connection.execute(action_schema.SHADOW_SAMPLES.insert().values(
        would_be_action_id=decision_id,
        service_name='svc',
        service_hash=_SERVICE_HASH,
        service_incarnation=uuid.UUID(_SERVICE_HASH),
        replica_id=0,
        replica_incarnation=uuid.uuid4(),
        desired_generation=1,
        action_type='launch',
        resource_identity='resource',
        immutable_spec={'version': 1},
        immutable_spec_sha256=_SHA_A,
        provider_plan={'version': 1},
        provider_plan_sha256=_SHA_A,
        profile_eligibility='ELIGIBLE',
        phase='PENDING',
        parity_class='PENDING',
        revision=1,
        created_at=_NOW,
        updated_at=_NOW))


def test_serve039_lineage_metadata_and_dialect_target(monkeypatch) -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    revision = scripts.get_revision('039')
    assert scripts.get_heads() == ['066']
    assert Path(revision.path).name == (
        '039_serve_resource_action_execution_history.py')
    assert revision.down_revision == '038'
    assert migration_utils.SERVE_VERSION == '066'
    assert migration_utils.serve_target_version(sqlite) == '037'
    assert set(m4_schema.SERVE039_METADATA.tables) == _NEW_TABLES
    assert not _NEW_TABLES.intersection(m4_schema.SERVE038_METADATA.tables)
    assert {table.name for table in m4_schema.SERVE039_ALTERED_RELATION_TABLES
           } == {
               action_schema.SHADOW_SAMPLES.name,
               action_schema.SHADOW_ATTEMPTS.name,
               m4_schema.WORKER_REGISTRATION_LEASES.name,
           }
    monkeypatch.setenv(migration_utils.SERVE_MIGRATION_CEILING_ENV_VAR, '037')
    with pytest.raises(RuntimeError, match='applies only to the PostgreSQL'):
        migration_utils.serve_target_version(sqlite)


def test_serve039_closed_metadata_contract() -> None:
    lineage_fks = {
        str(foreign_key.name): foreign_key for foreign_key in
        m4_schema.EXECUTION_AUTHORITY_LINEAGE.foreign_key_constraints
    }
    assert set(lineage_fks) == {
        'fk_serve_ra_execution_authority_lineage_reference',
        'fk_serve_ra_execution_authority_lineage_lease',
        'fk_serve_ra_execution_authority_lineage_policy',
    }
    assert tuple(
        column.name for column in
        lineage_fks['fk_serve_ra_execution_authority_lineage_reference'].columns
    ) == ('action_id',)
    assert tuple(column.name for column in
                 lineage_fks['fk_serve_ra_execution_authority_lineage_lease'].
                 columns) == ('cohort_id', 'authority_worker_instance_id')
    assert tuple(column.name for column in
                 lineage_fks['fk_serve_ra_execution_authority_lineage_policy'].
                 columns) == ('service_hash', 'policy_epoch', 'policy_sha256',
                              'authority_binding_sha256')

    process_fks = m4_schema.WORKER_PROCESS_SUPERSESSIONS.foreign_key_constraints
    assert len(process_fks) == 1
    process_fk = next(iter(process_fks))
    assert tuple(column.name for column in process_fk.columns) == ('cohort_id',)
    assert process_fk.referred_table.name == action_schema.WORKER_COHORTS.name
    assert {
        str(index.name)
        for index in m4_schema.WORKER_PROCESS_SUPERSESSIONS.indexes
    } == {'ix_serve_ra_worker_process_supersessions_authority'}

    check_sql = {
        str(constraint.name): ''.join(str(constraint.sqltext).split())
        for table in (
            m4_schema.EXECUTION_AUTHORITY_LINEAGE,
            m4_schema.ATTEMPT_TERMINAL_AUTHORITY,
            m4_schema.SHADOW_REQUEST_TERMINAL_HISTORY,
            m4_schema.SHADOW_EXECUTION_HISTORY,
            m4_schema.WORKER_PROCESS_SUPERSESSIONS,
        )
        for constraint in table.constraints
        if isinstance(constraint, sqlalchemy.CheckConstraint)
    }
    assert 'request_execution_generation=1' in check_sql[
        'ck_serve_ra_execution_authority_lineage_shape']
    assert 'registration_set_revision=cohort_revision' in check_sql[
        'ck_serve_ra_execution_authority_lineage_shape']
    selector_sql = check_sql['ck_serve_ra_attempt_terminal_authority_shape']
    assert 'request_execution_generationIN(0,1)' in selector_sql
    assert "terminal_cause='CLAIM_START_NOT_REPRESENTABLE'" in selector_sql
    assert "terminal_cause='CLAIM_REAUTHORIZATION_FAILED'" in selector_sql
    shadow_terminal_sql = check_sql[
        'ck_serve_ra_shadow_request_terminal_history_shape']
    assert "request_role='PRIMARY_LAUNCH'" in shadow_terminal_sql
    assert "request_return_sha256ISNOTNULL" in shadow_terminal_sql
    assert 'ck_serve_ra_shadow_execution_history_phase' in check_sql
    assert 'ck_serve_ra_shadow_execution_history_progress' in check_sql
    process_sql = check_sql['serve039_worker_process_supersession_ck']
    assert 'source_lease_generation=source_lease_revision' in process_sql
    assert "prior_execution_owner->>'api_instance_id'" in process_sql

    settlement_indexes = {
        str(index.name): index
        for index in m4_schema.SHADOW_SETTLEMENT_HISTORY.indexes
    }
    assert settlement_indexes[
        'uq_serve_ra_shadow_settlement_partial_source'].unique
    assert settlement_indexes[
        'uq_serve_ra_shadow_settlement_partial_target'].unique
    assert not settlement_indexes[
        'ix_serve_ra_shadow_settlement_partial_target'].unique
    assert tuple(_migration_module()._COMPLETE_LOCK_ORDER) == (
        m4_schema.AUTHORITY_POLICY_EPOCHS.name,
        action_schema.WORKER_COHORTS.name,
        m4_schema.WORKER_REGISTRATION_HANDOFFS.name,
        m4_schema.WORKER_PROCESS_SUPERSESSIONS.name,
        m4_schema.WORKER_REGISTRATION_LEASES.name,
        action_schema.WORKER_COHORT_REFS.name,
        action_schema.SHADOW_SAMPLES.name,
        action_schema.SHADOW_ATTEMPTS.name,
        m4_schema.SHADOW_EXECUTION_HISTORY.name,
        m4_schema.API_INSTANCE_GC_CURSORS.name,
        m4_schema.EXECUTION_AUTHORITY_LINEAGE.name,
        m4_schema.ATTEMPT_TERMINAL_AUTHORITY.name,
        m4_schema.SHADOW_REQUEST_TERMINAL_HISTORY.name,
        m4_schema.SHADOW_ADMISSION_FALLBACK_HISTORY.name,
        m4_schema.SHADOW_ADMISSION_FALLBACK_PROGRESS_HISTORY.name,
        m4_schema.SHADOW_SETTLEMENT_HISTORY.name,
    )


def test_serve039_rejects_direct_sqlite_upgrade() -> None:
    engine = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    with pytest.raises(RuntimeError, match='PostgreSQL-only'):
        alembic_command.upgrade(config, '039')


def test_serve039_upgrade_backfill_and_catalog(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '038')
    decision_id = uuid.uuid4()
    with postgres_engine.begin() as connection:
        _insert_038_shadow_parent(connection, decision_id)
    _upgrade(postgres_engine, '039')
    assert _revision(postgres_engine) == '039'

    inspector = sqlalchemy.inspect(postgres_engine)
    assert _NEW_TABLES.issubset(set(inspector.get_table_names()))
    lease_columns = {
        str(column['name']) for column in inspector.get_columns(
            m4_schema.WORKER_REGISTRATION_LEASES.name)
    }
    assert {
        'execution_owner', 'execution_owner_sha256',
        'execution_owner_api_instance_id'
    } <= lease_columns
    lease_checks = {
        str(check['name']) for check in inspector.get_check_constraints(
            m4_schema.WORKER_REGISTRATION_LEASES.name)
    }
    assert 'serve038_worker_lease_closed_shape_ck' not in lease_checks
    assert 'serve039_worker_lease_execution_owner_ck' in lease_checks

    parent_columns = {
        str(column['name']): column
        for column in inspector.get_columns(action_schema.SHADOW_SAMPLES.name)
    }
    assert parent_columns['execution_route']['nullable'] is False
    assert 'LEGACY_CONTROLLER' in str(
        parent_columns['execution_route']['default'])
    child_checks = {
        str(check['name']) for check in inspector.get_check_constraints(
            action_schema.SHADOW_ATTEMPTS.name)
    }
    assert 'ck_serve_ra_shadow_attempts_execution' not in child_checks
    assert 'serve039_shadow_child_execution_kind_ck' in child_checks
    with postgres_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                m4_schema.SHADOW_SAMPLES_V2.c.execution_route,
                m4_schema.SHADOW_SAMPLES_V2.c.private_fallback_reason,
                m4_schema.SHADOW_SAMPLES_V2.c.private_fallback_evidence,
                m4_schema.SHADOW_SAMPLES_V2.c.private_fallback_evidence_sha256).
            where(m4_schema.SHADOW_SAMPLES_V2.c.would_be_action_id ==
                  decision_id)).one()
    assert tuple(row) == ('LEGACY_CONTROLLER', None, None, None)


def test_serve039_complete_old_stamp_adoption(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '039')
    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(f'UPDATE {_VERSION_TABLE} SET version_num = '
                            "'038'"))
    _upgrade(postgres_engine, '039')
    assert _revision(postgres_engine) == '039'


def test_serve039_nonempty_old_stamp_adoption_refuses(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '039')
    with postgres_engine.begin() as connection:
        connection.execute(m4_schema.API_INSTANCE_GC_CURSORS.insert().values(
            cursor_name='authority-worker-v2',
            sweep_epoch=0,
            sweep_upper_bound_instance_id=None,
            after_instance_id=None,
            revision=1,
            last_operation_id=uuid.uuid4(),
            updated_at=_NOW))
        connection.execute(
            sqlalchemy.text(f'UPDATE {_VERSION_TABLE} SET version_num = '
                            "'038'"))
    with pytest.raises(RuntimeError, match='cannot adopt nonempty'):
        _upgrade(postgres_engine, '039')
    assert _revision(postgres_engine) == '038'


def test_serve039_partial_catalog_refuses(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '038')
    with postgres_engine.begin() as connection:
        m4_schema.API_INSTANCE_GC_CURSORS.create(connection)
    with pytest.raises(RuntimeError, match='partially installed'):
        _upgrade(postgres_engine, '039')
    assert _revision(postgres_engine) == '038'


def test_serve039_downgrade_refuses(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '039')
    config = migration_utils.get_alembic_config(postgres_engine,
                                                migration_utils.SERVE_DB_NAME)
    with pytest.raises(RuntimeError, match='cannot be downgraded'):
        alembic_command.downgrade(config, '038')
    assert _revision(postgres_engine) == '039'
