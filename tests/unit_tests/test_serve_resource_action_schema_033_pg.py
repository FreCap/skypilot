"""Schema-contract tests for additive SkyServe revision 033."""
# pylint: disable=not-callable,redefined-outer-name

import datetime
import os
import shutil
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy

from sky.serve import resource_action_state_schema as action_schema
from sky.utils.db import migration_utils

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
_UTC = datetime.timezone.utc
_SERVICE_UUID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_REPLICA_UUID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_CLUSTER_UUID = uuid.UUID('33333333-3333-4333-8333-333333333333')


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
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_serve_033_{uuid.uuid4().hex}'
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


def _reset_to_revision_031(engine: sqlalchemy.engine.Engine) -> None:
    if engine.dialect.name == 'postgresql':
        with engine.begin() as connection:
            connection.exec_driver_sql('DROP SCHEMA public CASCADE')
            connection.exec_driver_sql('CREATE SCHEMA public')
    metadata = sqlalchemy.MetaData()
    services = sqlalchemy.Table(
        'services', metadata,
        sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('status', sqlalchemy.Text))
    replicas = sqlalchemy.Table(
        'replicas', metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('status', sqlalchemy.Text))
    version = sqlalchemy.Table(
        'alembic_version_serve_state_db', metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True))
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(services.insert().values(name='svc', status='READY'))
        connection.execute(replicas.insert().values(service_name='svc',
                                                    replica_id=7,
                                                    status='PENDING'))
        connection.execute(version.insert().values(version_num='031'))


def _upgrade(engine: sqlalchemy.engine.Engine, revision: str) -> None:
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         revision)


def _revision(engine: sqlalchemy.engine.Engine) -> str:
    revision = migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)
    assert revision is not None
    return revision


def _foreign_keys(inspector: sqlalchemy.Inspector,
                  table: str) -> dict[str, tuple[str, tuple[str, ...], str]]:
    return {
        foreign_key['name']: (
            foreign_key['referred_table'],
            tuple(foreign_key['constrained_columns']),
            foreign_key['options'].get('ondelete')
        ) for foreign_key in inspector.get_foreign_keys(table)
    }


def _pending_shadow_sample(action_id: uuid.UUID) -> dict[str, object]:
    return {
        'would_be_action_id': action_id,
        'service_name': 'svc',
        'service_hash': str(_SERVICE_UUID),
        'service_incarnation': _SERVICE_UUID,
        'replica_id': 7,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': 1,
        'action_type': 'launch',
        'resource_identity': 'serve-action:test',
        'immutable_spec': {},
        'immutable_spec_sha256': 'a' * 64,
        'provider_plan': {},
        'provider_plan_sha256': 'b' * 64,
        'profile_eligibility': 'UNSUPPORTED',
        'phase': 'PENDING',
        'parity_class': 'PENDING',
    }


def test_revision_032_catalog_remains_frozen_and_head_catalog_is_complete():
    assert set(action_schema.REVISION_032_METADATA.tables) == {
        'serve_resource_action_shadow_samples',
        'serve_resource_action_shadow_attempts',
    }
    assert 'legacy_effect_trace' not in action_schema.SHADOW_ATTEMPTS_032.c
    assert set(action_schema.RESOURCE_ACTION_STATE_METADATA.tables) == {
        'serve_resource_action_shadow_samples',
        'serve_resource_action_shadow_attempts',
        'serve_resource_action_worker_cohorts',
        'serve_resource_action_worker_cohort_refs',
        'serve_resource_action_shadow_coverage',
        'serve_resource_action_shadow_coverage_attempts',
    }
    assert {'legacy_effect_trace', 'legacy_effect_trace_sha256'
           } <= set(action_schema.SHADOW_ATTEMPTS.c.keys())
    parent_foreign_keys = {
        foreign_key.name: foreign_key
        for foreign_key in action_schema.SHADOW_SAMPLES.foreign_key_constraints
    }
    coverage_foreign_key = parent_foreign_keys[
        'fk_serve_ra_shadow_samples_coverage']
    assert coverage_foreign_key.ondelete == 'RESTRICT'
    assert next(iter(coverage_foreign_key.elements)).target_fullname == (
        'serve_resource_action_shadow_coverage.decision_id')


def test_revision_033_sqlite_adds_only_portable_inert_links(tmp_path):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "serve-033.sqlite"}')
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    inspector = sqlalchemy.inspect(engine)
    columns_032 = {
        column['name'] for column in inspector.get_columns('replicas')
    }
    assert 'launch_shadow_coverage_id' not in columns_032
    assert 'down_shadow_coverage_id' not in columns_032

    _upgrade(engine, '033')
    inspector = sqlalchemy.inspect(engine)
    columns_033 = {
        column['name'] for column in inspector.get_columns('replicas')
    }
    assert {'launch_shadow_coverage_id',
            'down_shadow_coverage_id'} <= columns_033
    assert not ({
        'serve_resource_action_worker_cohorts',
        'serve_resource_action_worker_cohort_refs',
        'serve_resource_action_shadow_coverage',
        'serve_resource_action_shadow_coverage_attempts',
        'serve_resource_action_shadow_samples',
        'serve_resource_action_shadow_attempts',
    } & set(inspector.get_table_names()))
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.text(
                'SELECT status, launch_shadow_coverage_id, '
                'down_shadow_coverage_id FROM replicas')).mappings().one()
    assert row == {
        'status': 'PENDING',
        'launch_shadow_coverage_id': None,
        'down_shadow_coverage_id': None,
    }
    assert _revision(engine) == '033'

    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    with pytest.raises(RuntimeError, match='cannot be downgraded'):
        alembic_command.downgrade(config, '032')
    assert _revision(engine) == '033'


def test_revision_033_postgres_upgrade_catalog_and_constraints(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    inspector = sqlalchemy.inspect(engine)
    assert _revision(engine) == '032'
    assert 'legacy_effect_trace' not in {
        column['name'] for column in inspector.get_columns(
            'serve_resource_action_shadow_attempts')
    }
    assert 'serve_resource_action_shadow_coverage' not in (
        inspector.get_table_names())

    _upgrade(engine, '033')
    inspector = sqlalchemy.inspect(engine)
    assert _revision(engine) == '033'
    assert {
        'serve_resource_action_worker_cohorts',
        'serve_resource_action_worker_cohort_refs',
        'serve_resource_action_shadow_coverage',
        'serve_resource_action_shadow_coverage_attempts',
    } <= set(inspector.get_table_names())
    assert {'launch_shadow_coverage_id', 'down_shadow_coverage_id'} <= {
        column['name'] for column in inspector.get_columns('replicas')
    }
    assert {'legacy_effect_trace', 'legacy_effect_trace_sha256'} <= {
        column['name'] for column in inspector.get_columns(
            'serve_resource_action_shadow_attempts')
    }
    assert _foreign_keys(inspector, 'serve_resource_action_shadow_samples'
                        )['fk_serve_ra_shadow_samples_coverage'] == (
                            'serve_resource_action_shadow_coverage',
                            ('would_be_action_id',), 'RESTRICT')
    assert _foreign_keys(inspector,
                         'serve_resource_action_worker_cohort_refs') == {
                             'fk_serve_ra_worker_cohort_refs_cohort':
                                 ('serve_resource_action_worker_cohorts',
                                  ('cohort_id',), 'RESTRICT')
                         }
    assert _foreign_keys(inspector,
                         'serve_resource_action_shadow_coverage_attempts') == {
                             'fk_serve_ra_shadow_coverage_attempts_coverage':
                                 ('serve_resource_action_shadow_coverage',
                                  ('decision_id',), 'CASCADE')
                         }
    replica_indexes = {
        index['name']: index for index in inspector.get_indexes('replicas')
    }
    for name, column in {
            'uq_replicas_ra_launch_shadow_coverage': 'launch_shadow_coverage_id',
            'uq_replicas_ra_down_shadow_coverage': 'down_shadow_coverage_id',
    }.items():
        assert replica_indexes[name]['unique']
        assert replica_indexes[name]['column_names'] == [column]
    replica_checks = {
        constraint['name']
        for constraint in inspector.get_check_constraints('replicas')
    }
    assert {
        'ck_replicas_resource_action_links',
        'ck_replicas_resource_action_launch_exclusive',
        'ck_replicas_resource_action_down_exclusive',
        'ck_replicas_resource_action_shadow_links',
    } <= replica_checks
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.text(
                'SELECT status, launch_shadow_coverage_id, '
                'down_shadow_coverage_id FROM replicas')).mappings().one()
    assert row == {
        'status': 'PENDING',
        'launch_shadow_coverage_id': None,
        'down_shadow_coverage_id': None,
    }


def test_revision_033_postgres_checks_restrict_and_cascade(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '033')
    decision_id = uuid.uuid4()
    now = datetime.datetime.now(_UTC)
    with engine.begin() as connection:
        connection.execute(action_schema.WORKER_COHORTS.insert().values(
            cohort_id='cohort-v1',
            deployment_uid='deployment-uid-v1',
            cohort_identity={},
            cohort_identity_sha256='a' * 64,
            registration_attestations={},
            registration_attestations_sha256='b' * 64,
            lifecycle_state='REGISTERING'))
        connection.execute(action_schema.WORKER_COHORT_REFS.insert().values(
            decision_id=decision_id,
            cohort_id='cohort-v1',
            service_hash=str(_SERVICE_UUID),
            replica_incarnation=_REPLICA_UUID,
            desired_generation=1,
            action_type='launch',
            controller_owner_fence='123:10.0.0.1',
            lifecycle_epoch=1,
            reference_state='PREPARING'))
        connection.execute(action_schema.SHADOW_COVERAGE.insert().values(
            decision_id=decision_id,
            service_name='svc',
            service_hash=str(_SERVICE_UUID),
            service_incarnation=_SERVICE_UUID,
            replica_id=7,
            replica_incarnation=_REPLICA_UUID,
            desired_generation=1,
            action_type='launch',
            normalizer_contract_version=1,
            normalization_outcome='REPRESENTABLE',
            worker_cohort_ref_id=decision_id,
            admitted_at=now))
        connection.execute(action_schema.SHADOW_SAMPLES.insert().values(
            **_pending_shadow_sample(decision_id)))
        connection.execute(action_schema.SHADOW_ATTEMPTS.insert().values(
            would_be_action_id=decision_id,
            request_sequence=1,
            logical_attempt=1,
            request_role='PRIMARY_LAUNCH',
            planned_execution_kind='api_request',
            phase='PRE_SUBMIT',
            invocation={},
            invocation_sha256='c' * 64))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(action_schema.SHADOW_ATTEMPTS.update().where(
                action_schema.SHADOW_ATTEMPTS.c.would_be_action_id ==
                decision_id).values(legacy_effect_trace={}))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(action_schema.SHADOW_COVERAGE.delete().where(
                action_schema.SHADOW_COVERAGE.c.decision_id == decision_id))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(action_schema.WORKER_COHORTS.delete().where(
                action_schema.WORKER_COHORTS.c.cohort_id == 'cohort-v1'))

    invalid_reason_id = uuid.uuid4()
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(action_schema.SHADOW_COVERAGE.insert().values(
                decision_id=invalid_reason_id,
                service_name='svc',
                service_hash=str(_SERVICE_UUID),
                service_incarnation=_SERVICE_UUID,
                replica_id=8,
                replica_incarnation=uuid.uuid4(),
                desired_generation=2,
                action_type='down',
                normalizer_contract_version=1,
                normalization_outcome='NOT_REPRESENTABLE',
                not_representable_reason='multi_node'))

    coverage_only_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(action_schema.SHADOW_COVERAGE.insert().values(
            decision_id=coverage_only_id,
            service_name='svc',
            service_hash=str(_SERVICE_UUID),
            service_incarnation=_SERVICE_UUID,
            replica_id=8,
            replica_incarnation=uuid.uuid4(),
            desired_generation=2,
            action_type='down',
            normalizer_contract_version=1,
            normalization_outcome='NOT_REPRESENTABLE',
            not_representable_reason='prior_launch_basis'))
        connection.execute(
            action_schema.SHADOW_COVERAGE_ATTEMPTS.insert().values(
                decision_id=coverage_only_id,
                request_sequence=1,
                logical_attempt=1,
                request_role='PRIMARY_DOWN',
                phase='PRE_SUBMIT'))
        connection.execute(action_schema.SHADOW_COVERAGE.delete().where(
            action_schema.SHADOW_COVERAGE.c.decision_id == coverage_only_id))
        remaining = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                action_schema.SHADOW_COVERAGE_ATTEMPTS).where(
                    action_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id ==
                    coverage_only_id)).scalar_one()
    assert remaining == 0


def test_revision_033_postgres_nonempty_shadow_fails_before_catalog_change(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    action_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(action_schema.SHADOW_SAMPLES_032.insert().values(
            **_pending_shadow_sample(action_id)))

    with pytest.raises(RuntimeError, match='reviewed evidence backfill'):
        _upgrade(engine, '033')
    assert _revision(engine) == '032'
    inspector = sqlalchemy.inspect(engine)
    assert 'serve_resource_action_shadow_coverage' not in (
        inspector.get_table_names())
    assert 'launch_shadow_coverage_id' not in {
        column['name'] for column in inspector.get_columns('replicas')
    }


def test_revision_033_postgres_refuses_downgrade(postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '033')
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    with pytest.raises(RuntimeError, match='cannot be downgraded'):
        alembic_command.downgrade(config, '032')
    assert _revision(engine) == '033'
