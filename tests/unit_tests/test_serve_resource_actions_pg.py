"""Migration and catalog tests for SkyServe resource-action shadow state."""
# pylint: disable=protected-access,redefined-outer-name

import os
import pathlib
import shutil
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.serve import replica_managers
from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_action_state_schema as action_schema
from sky.serve import serve_state
from sky.utils.db import migration_utils

pytest.importorskip('psycopg2')
_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')


@pytest.fixture(scope='module')
def postgres_engine():
    container = None
    admin_engine = None
    temporary_database = None
    if _POSTGRES_URL is None:
        if shutil.which('docker') is None:
            pytest.skip('docker unavailable; skipping Serve PostgreSQL tests')
        testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
        try:
            container = testcontainers_postgres.PostgresContainer('postgres:16')
            container.start()
        except Exception as e:  # pylint: disable=broad-except
            pytest.skip(f'could not start postgres container: {e}')
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_serve_actions_{uuid.uuid4().hex}'
        admin_engine = sqlalchemy.create_engine(_POSTGRES_URL,
                                                isolation_level='AUTOCOMMIT')
        quoted_database = admin_engine.dialect.identifier_preparer.quote(
            temporary_database)
        try:
            with admin_engine.connect() as connection:
                connection.exec_driver_sql(f'CREATE DATABASE {quoted_database}')
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
            quoted_database = admin_engine.dialect.identifier_preparer.quote(
                temporary_database)
            with admin_engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text(
                        'SELECT pg_terminate_backend(pid) '
                        'FROM pg_stat_activity '
                        'WHERE datname = :database AND pid <> pg_backend_pid()'
                    ), {'database': temporary_database})
                connection.exec_driver_sql(f'DROP DATABASE {quoted_database}')
            admin_engine.dispose()
        elif container is not None:
            container.stop()


@pytest.fixture
def empty_postgres(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    return postgres_engine


def _sample_values() -> dict:
    service_incarnation = uuid.UUID('11111111-1111-4111-8111-111111111111')
    return {
        'would_be_action_id': uuid.uuid4(),
        'service_name': 'service-a',
        'service_hash': str(service_incarnation),
        'service_incarnation': service_incarnation,
        'replica_id': 1,
        'replica_incarnation': uuid.uuid4(),
        'desired_generation': 1,
        'action_type': 'launch',
        'resource_identity': '{"version":1}',
        'immutable_spec': {
            'version': 1
        },
        'immutable_spec_sha256': 'a' * 64,
        'provider_plan': {
            'version': 1
        },
        'provider_plan_sha256': 'b' * 64,
        'profile_eligibility': 'ELIGIBLE',
        'phase': 'PENDING',
        'parity_class': 'PENDING',
    }


def _attempt_values(action_id: uuid.UUID) -> dict:
    return {
        'would_be_action_id': action_id,
        'request_sequence': 1,
        'logical_attempt': 1,
        'request_role': 'PRIMARY_LAUNCH',
        'planned_execution_kind': 'api_request',
        'phase': 'PRE_SUBMIT',
        'invocation': {
            'version': 1
        },
        'invocation_sha256': 'c' * 64,
    }


def _coverage_values(sample: dict) -> dict:
    return {
        'decision_id': sample['would_be_action_id'],
        'service_name': sample['service_name'],
        'service_hash': sample['service_hash'],
        'service_incarnation': sample['service_incarnation'],
        'replica_id': sample['replica_id'],
        'replica_incarnation': sample['replica_incarnation'],
        'desired_generation': sample['desired_generation'],
        'action_type': sample['action_type'],
        'normalizer_contract_version': 1,
        'normalization_outcome': 'REPRESENTABLE',
        'candidate_epoch': uuid.uuid4(),
        'qualification_policy_sha256': 'd' * 64,
        'qualification_binding_sha256': 'e' * 64,
    }


def _drop_pre_033_resource_action_columns(engine) -> None:
    """Materialize the shipped revision-032 pre-resource-action catalog."""
    replica_columns = (
        'down_shadow_coverage_id',
        'launch_shadow_coverage_id',
        'down_shadow_sample_id',
        'launch_shadow_sample_id',
        'down_action_id',
        'launch_action_id',
        'sky_cluster_record_uuid',
        'desired_generation',
        'replica_incarnation',
    )
    with engine.begin() as connection:
        for column in replica_columns:
            connection.exec_driver_sql(
                f'ALTER TABLE replicas DROP COLUMN {column}')
        connection.exec_driver_sql('ALTER TABLE services DROP COLUMN '
                                   'resource_action_mode_changed_at')
        connection.exec_driver_sql(
            'ALTER TABLE services DROP COLUMN resource_action_mode')


def _assert_upstream_request_classification_catalog(engine) -> None:
    """Verify revision-032 request classification survives unchanged."""
    inspector = sqlalchemy.inspect(engine)
    raw_columns = {
        column['name']: column
        for column in inspector.get_columns('serve_request_activity_history')
    }
    for name in ('classified_request_count', 'counted_rejected_count'):
        column = raw_columns[name]
        assert isinstance(column['type'], sqlalchemy.Integer)
        assert column['nullable'] is True
        assert column['default'] is None

    daily_columns = {
        column['name']: column
        for column in inspector.get_columns('serve_request_activity_daily')
    }
    for name in ('classified_request_count', 'counted_rejected_count'):
        column = daily_columns[name]
        assert isinstance(column['type'], sqlalchemy.BigInteger)
        assert column['nullable'] is True
        assert column['default'] is None
    for name in ('classified_first_bucket_start',
                 'classified_last_bucket_start'):
        column = daily_columns[name]
        assert isinstance(column['type'], sqlalchemy.DateTime)
        assert column['type'].timezone is True
        assert column['nullable'] is True
        assert column['default'] is None
    incomplete = daily_columns['classification_incomplete']
    assert isinstance(incomplete['type'], sqlalchemy.Boolean)
    assert incomplete['nullable'] is False
    assert 'false' in str(incomplete['default']).lower()

    raw_checks = {
        constraint['name']: ''.join(str(constraint['sqltext']).lower().split())
        for constraint in inspector.get_check_constraints(
            'serve_request_activity_history')
    }
    assert ('counted_rejected_count<=classified_request_count'
            in raw_checks['serve_request_activity_history_classified_pair'])
    daily_checks = {
        constraint['name']: ''.join(str(constraint['sqltext']).lower().split())
        for constraint in inspector.get_check_constraints(
            'serve_request_activity_daily')
    }
    assert ('classified_first_bucket_start<=classified_last_bucket_start'
            in daily_checks['serve_request_activity_daily_classified_pair'])


def _assert_classification_rows_retained(engine) -> None:
    with engine.connect() as connection:
        raw = connection.execute(
            sqlalchemy.text(
                'SELECT classified_request_count, counted_rejected_count '
                'FROM serve_request_activity_history WHERE '
                "service_name = 'legacy' AND reporter_session_id = 'session'")
        ).one()
        daily = connection.execute(
            sqlalchemy.text(
                'SELECT classified_request_count, counted_rejected_count, '
                'classification_incomplete '
                'FROM serve_request_activity_daily WHERE '
                "service_name = 'legacy'")).one()
    assert raw == (7, 2)
    assert daily == (7, 2, False)


def _replica(replica_id: int, version: int = 1) -> replica_managers.ReplicaInfo:
    return replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'svc-{replica_id}',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=version,
                                        resources_override=None)


def test_pg_upgrade_from_032_and_catalog_are_exact(empty_postgres):
    engine = empty_postgres
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         '032')
    _drop_pre_033_resource_action_columns(engine)
    _assert_upstream_request_classification_catalog(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO services (name, hash) VALUES ('legacy', 'hash')"))
        connection.execute(
            sqlalchemy.text("INSERT INTO replicas (service_name, replica_id) "
                            "VALUES ('legacy', 1)"))
        connection.execute(
            sqlalchemy.text(
                'INSERT INTO serve_request_activity_history '
                '(service_name, service_hash, reporter_session_id, '
                'bucket_start, observed_at, request_count, rejected_count, '
                'rejection_count_available, classified_request_count, '
                'counted_rejected_count) VALUES '
                "('legacy', 'hash', 'session', "
                "TIMESTAMPTZ '2026-08-01 00:00:00+00', "
                "TIMESTAMPTZ '2026-08-01 00:01:00+00', 7, 2, true, 7, 2)"))
        connection.execute(
            sqlalchemy.text(
                'INSERT INTO serve_request_activity_daily '
                '(day_start, service_name, service_hash, first_bucket_start, '
                'last_bucket_start, request_count, classified_request_count, '
                'counted_rejected_count, classified_first_bucket_start, '
                'classified_last_bucket_start, classification_incomplete, '
                'observed_at) VALUES '
                "(TIMESTAMPTZ '2026-08-01 00:00:00+00', 'legacy', 'hash', "
                "TIMESTAMPTZ '2026-08-01 00:00:00+00', "
                "TIMESTAMPTZ '2026-08-01 00:00:00+00', 7, 7, 2, "
                "TIMESTAMPTZ '2026-08-01 00:00:00+00', "
                "TIMESTAMPTZ '2026-08-01 00:00:00+00', false, "
                "TIMESTAMPTZ '2026-08-01 00:01:00+00')"))
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         '033')
    _assert_upstream_request_classification_catalog(engine)
    _assert_classification_rows_retained(engine)
    # Simulate a lost revision-033 acknowledgement before revision 034 could
    # have run: retain its DDL/data, move only Alembic's marker back, and prove
    # 032/033 convergence without making the historical migration aware of a
    # future table inventory.
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.stamp(config, '031')
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         '033')
    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME) == '033'
    _assert_upstream_request_classification_catalog(engine)
    _assert_classification_rows_retained(engine)

    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         migration_utils.SERVE_VERSION)
    alembic_command.stamp(config, '033')
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         migration_utils.SERVE_VERSION)
    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME) == migration_utils.SERVE_VERSION

    inspector = sqlalchemy.inspect(engine)
    assert {
        action_schema.SHADOW_SAMPLES.name,
        action_schema.SHADOW_ATTEMPTS.name,
    } <= set(inspector.get_table_names())
    service_columns = {
        column['name']: column for column in inspector.get_columns('services')
    }
    assert service_columns['resource_action_mode']['nullable'] is False
    assert service_columns['resource_action_mode']['default'] is not None
    replica_columns = {
        column['name']: column for column in inspector.get_columns('replicas')
    }
    action_columns = {
        'replica_incarnation',
        'desired_generation',
        'sky_cluster_record_uuid',
        'launch_action_id',
        'down_action_id',
        'launch_shadow_coverage_id',
        'down_shadow_coverage_id',
        'launch_shadow_sample_id',
        'down_shadow_sample_id',
    }
    assert action_columns <= set(replica_columns)
    for name in action_columns - {'desired_generation'}:
        assert isinstance(replica_columns[name]['type'], postgresql.UUID)

    sample_indexes = {
        index['name']: index
        for index in inspector.get_indexes(action_schema.SHADOW_SAMPLES.name)
    }
    assert {
        'ix_serve_ra_shadow_samples_promotion',
        'ix_serve_ra_shadow_samples_blockers',
        'ix_serve_ra_shadow_samples_retention',
    } <= set(sample_indexes)
    attempt_indexes = {
        index['name']: index
        for index in inspector.get_indexes(action_schema.SHADOW_ATTEMPTS.name)
    }
    assert attempt_indexes['uq_serve_ra_shadow_attempts_request']['unique']
    foreign_keys = inspector.get_foreign_keys(
        action_schema.SHADOW_ATTEMPTS.name)
    assert len(foreign_keys) == 1
    assert foreign_keys[0][
        'referred_table'] == action_schema.SHADOW_SAMPLES.name
    assert foreign_keys[0]['options']['ondelete'] == 'CASCADE'
    parent_foreign_keys = inspector.get_foreign_keys(
        action_schema.SHADOW_SAMPLES.name)
    assert len(parent_foreign_keys) == 1
    assert parent_foreign_keys[0][
        'referred_table'] == action_schema.SHADOW_COVERAGE.name
    assert parent_foreign_keys[0]['options']['ondelete'] == 'RESTRICT'
    reference_columns = {
        column['name']: column for column in inspector.get_columns(
            action_schema.WORKER_COHORT_REFS.name)
    }
    capability_column = reference_columns['preparation_capability_sha256']
    assert capability_column['nullable'] is False
    assert capability_column['default'] is None
    reference_checks = {
        constraint['name'] for constraint in inspector.get_check_constraints(
            action_schema.WORKER_COHORT_REFS.name)
    }
    assert 'ck_serve_ra_worker_cohort_refs_capability' in reference_checks

    with engine.connect() as connection:
        legacy = connection.execute(
            sqlalchemy.text('SELECT resource_action_mode, '
                            'resource_action_mode_changed_at FROM services '
                            "WHERE name = 'legacy'")).one()
        replica = connection.execute(
            sqlalchemy.text(
                'SELECT replica_incarnation, desired_generation, '
                'sky_cluster_record_uuid, launch_action_id, down_action_id, '
                'launch_shadow_coverage_id, down_shadow_coverage_id, '
                'launch_shadow_sample_id, down_shadow_sample_id FROM replicas '
                "WHERE service_name = 'legacy' AND replica_id = 1")).one()
    assert legacy == ('legacy', None)
    assert all(value is None for value in replica)


def test_pg_constraints_cascade_and_schema_down_refusal(empty_postgres):
    engine = empty_postgres
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         migration_utils.SERVE_VERSION)
    sample = _sample_values()
    with engine.begin() as connection:
        connection.execute(sqlalchemy.insert(m4_schema.SHADOW_COVERAGE_V2),
                           _coverage_values(sample))
        connection.execute(sqlalchemy.insert(action_schema.SHADOW_SAMPLES),
                           sample)
        connection.execute(sqlalchemy.insert(action_schema.SHADOW_ATTEMPTS),
                           _attempt_values(sample['would_be_action_id']))
        connection.execute(
            sqlalchemy.delete(action_schema.SHADOW_SAMPLES).where(
                action_schema.SHADOW_SAMPLES.c.would_be_action_id ==
                sample['would_be_action_id']))
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(action_schema.SHADOW_ATTEMPTS)).scalar_one() == 0

    invalid_parent = _sample_values()
    invalid_parent['immutable_spec'] = []
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(sqlalchemy.insert(m4_schema.SHADOW_COVERAGE_V2),
                               _coverage_values(invalid_parent))
            connection.execute(sqlalchemy.insert(action_schema.SHADOW_SAMPLES),
                               invalid_parent)

    sample = _sample_values()
    with engine.begin() as connection:
        connection.execute(sqlalchemy.insert(m4_schema.SHADOW_COVERAGE_V2),
                           _coverage_values(sample))
        connection.execute(sqlalchemy.insert(action_schema.SHADOW_SAMPLES),
                           sample)
    invalid_attempt = _attempt_values(sample['would_be_action_id'])
    invalid_attempt['phase'] = 'REQUEST_BOUND'
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(sqlalchemy.insert(action_schema.SHADOW_ATTEMPTS),
                               invalid_attempt)

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text(
                    "INSERT INTO services (name, resource_action_mode) "
                    "VALUES ('bad-mode', 'invalid')"))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text(
                    'INSERT INTO replicas '
                    '(service_name, replica_id, replica_incarnation, '
                    'sky_cluster_record_uuid) '
                    "VALUES ('partial', 1, CAST(:incarnation AS UUID), "
                    'CAST(:cluster_uuid AS UUID))'), {
                        'incarnation': str(uuid.uuid4()),
                        'cluster_uuid': str(uuid.uuid4()),
                    })

    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    with pytest.raises(RuntimeError, match='additive and cannot be downgraded'):
        alembic_command.downgrade(config, '031')
    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME) == migration_utils.SERVE_VERSION


def test_sqlite_gets_only_inert_common_columns_and_refuses_down(tmp_path):
    database_path = pathlib.Path(tmp_path) / 'serve.db'
    engine = sqlalchemy.create_engine(f'sqlite:///{database_path}')
    try:
        migration_utils.safe_alembic_upgrade(
            engine, migration_utils.SERVE_DB_NAME,
            migration_utils.SERVE_NON_POSTGRES_VERSION)
        inspector = sqlalchemy.inspect(engine)
        assert 'resource_action_mode' in {
            column['name'] for column in inspector.get_columns('services')
        }
        assert {
            'replica_incarnation',
            'desired_generation',
            'sky_cluster_record_uuid',
            'launch_action_id',
            'down_action_id',
            'launch_shadow_coverage_id',
            'down_shadow_coverage_id',
            'launch_shadow_sample_id',
            'down_shadow_sample_id',
        } <= {column['name'] for column in inspector.get_columns('replicas')}
        assert action_schema.SHADOW_SAMPLES.name not in inspector.get_table_names(
        )
        assert action_schema.SHADOW_ATTEMPTS.name not in inspector.get_table_names(
        )

        config = migration_utils.get_alembic_config(
            engine, migration_utils.SERVE_DB_NAME)
        with pytest.raises(RuntimeError,
                           match='additive and cannot be downgraded'):
            alembic_command.downgrade(config, '031')
        assert migration_utils.get_current_alembic_revision(
            engine, migration_utils.SERVE_DB_NAME) == (
                migration_utils.SERVE_NON_POSTGRES_VERSION)
    finally:
        engine.dispose()


def test_pg_replica_updates_preserve_actions_and_admissions_reject_duplicates(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         migration_utils.SERVE_VERSION)
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    service_hash = '11111111-1111-4111-8111-111111111111'
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc', hash=service_hash, status='READY'))

    expected_by_replica = {}
    for replica_id in range(1, 5):
        assert serve_state.add_or_update_replica('svc', replica_id,
                                                 _replica(replica_id))
        action_values = {
            'replica_incarnation': uuid.UUID(int=replica_id * 100 + 1),
            'desired_generation': replica_id,
            'sky_cluster_record_uuid': uuid.UUID(int=replica_id * 100 + 2),
            'launch_action_id':
                (uuid.UUID(int=replica_id * 100 + 3) if replica_id % 2 else None
                ),
            'down_action_id':
                (uuid.UUID(int=replica_id * 100 + 4) if replica_id % 2 else None
                ),
            'launch_shadow_coverage_id':
                (None if replica_id % 2 else uuid.UUID(int=replica_id * 100 + 5)
                ),
            'down_shadow_coverage_id':
                (None if replica_id % 2 else uuid.UUID(int=replica_id * 100 + 6)
                ),
            'launch_shadow_sample_id':
                (None if replica_id % 2 else uuid.UUID(int=replica_id * 100 + 5)
                ),
            'down_shadow_sample_id':
                (None if replica_id % 2 else uuid.UUID(int=replica_id * 100 + 6)
                ),
            'resource_action_spec_identity_sha256': 'f' * 64,
        }
        expected_by_replica[replica_id] = action_values
        with orm.Session(engine) as session:
            session.execute(
                sqlalchemy.update(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id ==
                    replica_id).values(**action_values))
            session.commit()

    first = serve_state.get_replica_info_from_id('svc', 1)
    second = serve_state.get_replica_info_from_id('svc', 2)
    assert first is not None and second is not None
    first.version = 2
    second.version = 2
    assert serve_state.add_or_update_replica('svc',
                                             1,
                                             first,
                                             expected_replica_exists=True)
    assert serve_state.add_or_update_replicas('svc', [(2, second)],
                                              expected_replica_exists=True)

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            service_hash,
            3,
            _replica(3, version=2),
            pool_key='test-paid-pool',
            priority=1,
            base_limit=1,
            max_limit=2,
            now=100.0,
            success_ttl_seconds=60.0,
            waiter_ttl_seconds=60.0,
            expected_controller_owner=None)

    fill_pool_key = '["test-context","a100"]'
    with orm.Session(engine) as session:
        session.execute(serve_state.reserved_fill_claims_table.insert().values(
            service_name='svc',
            pool_key=fill_pool_key,
            weight=1,
            floor_replicas=1,
            gpus_per_replica=1,
            holdings_fill=1,
            heartbeat_ts=100.0))
        session.execute(serve_state.reserved_fill_lease_table.insert().values(
            id=1, epoch=1))
        session.commit()
    duplicate_fill_replica = _replica(4, version=2)
    duplicate_fill_replica.reserved_fill = True
    duplicate_fill_replica.reserved_fill_pool_key = fill_pool_key
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.add_replica_if_round_epoch('svc',
                                               4,
                                               duplicate_fill_replica,
                                               pool_key=fill_pool_key,
                                               expected_epoch=1,
                                               expected_lease_token=1)

    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name ==
                'svc')).mappings().all()
    actual_by_replica = {row['replica_id']: row for row in rows}
    assert {
        replica_id: row['version']
        for replica_id, row in actual_by_replica.items()
    } == {
        1: 2,
        2: 2,
        3: 1,
        4: 1,
    }
    for replica_id, expected in expected_by_replica.items():
        assert {
            name: actual_by_replica[replica_id][name]
            for name in serve_state._ACTION_OWNED_REPLICA_COLUMNS
        } == expected
