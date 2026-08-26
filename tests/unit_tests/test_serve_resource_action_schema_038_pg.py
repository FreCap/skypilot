"""Schema and migration contracts for PostgreSQL-only SkyServe revision 038."""
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
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
_VERSION_TABLE = 'alembic_version_serve_state_db'
_UTC = datetime.timezone.utc
_NOW = datetime.datetime(2026, 8, 3, 12, 0, tzinfo=_UTC)
_SERVICE_HASH = '11111111-1111-4111-8111-111111111111'
_SHA_A = 'a' * 64
_SHA_B = 'b' * 64

_NEW_TABLES = {
    'serve_resource_action_authority_policy_epochs',
    'serve_resource_action_worker_registration_leases',
    'serve_resource_action_worker_registration_handoffs',
    'serve_resource_action_worker_registration_cold_recoveries',
    'serve_resource_action_crash_canary_runs',
    'serve_resource_action_attempt_exhaustions',
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
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_serve_038_{uuid.uuid4().hex}'
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


def _reset(postgres_engine: sqlalchemy.engine.Engine) -> None:
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')


def _upgrade(engine: sqlalchemy.engine.Engine, revision: str) -> None:
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         revision)


def _upgrade_api_requests(engine: sqlalchemy.engine.Engine) -> None:
    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         migration_utils.API_REQUESTS_VERSION)


def _revision(engine: sqlalchemy.engine.Engine) -> str | None:
    return migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)


def _cohort_values(cohort_id: str, state: str,
                   version: int) -> dict[str, object]:
    values: dict[str, object] = {
        'cohort_id': cohort_id,
        'deployment_uid': f'deployment-{cohort_id}',
        'cohort_identity': {
            'version': version
        },
        'cohort_identity_sha256': _SHA_A,
        'registration_attestations': {
            'version': version
        },
        'registration_attestations_sha256': _SHA_A,
        'lifecycle_state': state,
        'revision': 1,
        'created_at': _NOW,
        'state_changed_at': _NOW,
        'retired_at': _NOW if state == 'RETIRED' else None,
    }
    return values


def _insert_policy(connection: sqlalchemy.engine.Connection,
                   epoch: uuid.UUID,
                   *,
                   policy_hash: str = _SHA_A) -> None:
    connection.execute(m4_schema.AUTHORITY_POLICY_EPOCHS.insert().values(
        service_hash=_SERVICE_HASH,
        policy_epoch=epoch,
        predecessor_policy_epoch=None,
        policy={'version': 1},
        policy_sha256=policy_hash,
        authority_binding_sha256=_SHA_B,
        rotation_proof={'version': 1},
        rotation_proof_sha256=_SHA_A,
        nonterminal_inventory={'version': 1},
        nonterminal_inventory_sha256=_SHA_A,
        reason='INITIAL_PROMOTION',
        policy_state='ACTIVE',
        admission_state='OPEN',
        admission_revision=1,
        last_operation_id=uuid.uuid4(),
        last_operation_kind='ACTIVATE',
        created_at=_NOW,
        admission_changed_at=_NOW,
        activated_at=_NOW,
        superseded_at=None))


def _open_handoff_values(cohort_id: str, *,
                         embedded_source_revision: int) -> dict[str, object]:
    stale = uuid.uuid4()
    survivor = uuid.uuid4()
    candidate = uuid.uuid4()
    return {
        'cohort_id': cohort_id,
        'handoff_id': uuid.uuid4(),
        'predecessor_handoff_id': None,
        'chain_sequence': 1,
        'stale_fence_disposition': 'NEWLY_REVOKED',
        'source_cohort_revision': 1,
        'source_cohort_state': 'ACCEPTING',
        'source_registration_set_revision': 1,
        'source_registration_set': {
            'revision': embedded_source_revision,
            'workers': [{
                'worker_instance_id': str(stale)
            }, {
                'worker_instance_id': str(survivor)
            }],
        },
        'source_registration_set_sha256': _SHA_A,
        'stale_worker_instance_id': stale,
        'stale_pod_name': 'stale-pod',
        'stale_pod_uid': stale,
        'survivor_worker_instance_id': survivor,
        'survivor_pod_uid': survivor,
        'candidate_worker_instance_id': candidate,
        'candidate_pod_name': 'candidate-pod',
        'candidate_pod_uid': candidate,
        'stale_authority_fence': {
            'version': 1
        },
        'stale_authority_fence_sha256': _SHA_A,
        'stale_uid_absence_proof': {
            'version': 1
        },
        'stale_uid_absence_proof_sha256': _SHA_A,
        'candidate_registration': {
            'version': 2
        },
        'candidate_registration_sha256': _SHA_A,
        'handoff_state': 'OPEN',
        'revision': 1,
        'opened_at': _NOW,
        'fenced_at': _NOW,
    }


def test_serve038_lineage_and_dialect_target() -> None:
    engine = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    revision = scripts.get_revision('038')
    assert scripts.get_heads() == ['061']
    assert Path(
        revision.path).name == ('038_serve_resource_action_authority.py')
    assert revision.down_revision == '037'
    assert migration_utils.SERVE_VERSION == '061'
    assert migration_utils.serve_target_version(engine) == '037'


def test_serve038_metadata_is_separate_and_complete() -> None:
    assert set(m4_schema.SERVE038_METADATA.tables) == _NEW_TABLES
    assert not _NEW_TABLES.intersection(
        action_schema.RESOURCE_ACTION_STATE_METADATA.tables)
    assert all(column.table is None
               for column in m4_schema.service_candidate_columns())
    assert {
        'resource_action_candidate_epoch',
        'resource_action_candidate_policy_sha256',
        'resource_action_candidate_binding_sha256',
    } <= set(serve_state_schema.services_table.c.keys())
    assert {
        'resource_action_spec_identity',
        'resource_action_spec_identity_sha256',
    } <= set(serve_state_schema.version_specs_table.c.keys())
    assert 'resource_action_spec_identity_sha256' in (
        serve_state_schema.replicas_table.c)
    assert serve_state.services_table is serve_state_schema.services_table
    assert serve_state.version_specs_table is serve_state_schema.version_specs_table
    assert serve_state.replicas_table is serve_state_schema.replicas_table
    assert set(m4_schema.SERVE038_ALTERED_RELATION_METADATA.tables) == {
        action_schema.WORKER_COHORTS.name,
        action_schema.WORKER_COHORT_REFS.name,
        action_schema.SHADOW_COVERAGE.name,
    }
    assert 'removal_authorized_at' in m4_schema.WORKER_COHORTS_V2.c
    assert 'authority_policy_epoch' in m4_schema.WORKER_COHORT_REFS_V2.c
    assert 'candidate_epoch' in m4_schema.SHADOW_COVERAGE_V2.c
    checks = m4_schema.serve038_worker_state_check_constraints()
    assert {
        str(constraint.name)
        for constraints in checks.values()
        for constraint in constraints
    } == {
        'serve038_worker_lease_closed_shape_ck',
        'serve038_worker_handoff_scalar_lineage_ck',
        'serve038_worker_handoff_pairing_state_ck',
        'serve038_worker_handoff_terminal_revision_ck',
        'serve038_worker_cold_required_json_ck',
        'serve038_worker_cold_revision_shape_ck',
    }
    assert tuple(
        importlib.import_module(
            'sky.schemas.db.serve_state.038_serve_resource_action_authority').
        _ALTERED_RELATIONS) == ('services', 'version_specs', 'replicas',
                                'serve_resource_action_worker_cohorts',
                                'serve_resource_action_worker_cohort_refs',
                                'serve_resource_action_shadow_coverage')


def test_policy_and_reference_physical_contracts() -> None:
    policy = m4_schema.AUTHORITY_POLICY_EPOCHS
    check_names = {
        str(constraint.name)
        for constraint in policy.constraints
        if isinstance(constraint, sqlalchemy.CheckConstraint)
    }
    assert {
        'ck_serve_ra_authority_policy_epochs_admission',
        'ck_serve_ra_authority_policy_epochs_timestamps',
        'ck_serve_ra_authority_policy_epochs_reason',
        'ck_serve_ra_authority_policy_epochs_predecessor',
    } <= check_names
    assert {str(index.name) for index in policy.indexes} == {
        'uq_serve_ra_authority_policy_epochs_predecessor',
        'uq_serve_ra_authority_policy_epochs_root',
        'uq_serve_ra_authority_policy_epochs_active',
    }
    foreign_key = m4_schema.cohort_ref_authority_foreign_key()
    assert tuple(foreign_key.column_keys) == ('service_hash',
                                              'authority_policy_epoch',
                                              'authority_policy_sha256',
                                              'authority_binding_sha256')
    assert foreign_key.ondelete == 'RESTRICT'
    assert tuple(
        element.target_fullname for element in foreign_key.elements
    ) == (
        'serve_resource_action_authority_policy_epochs.service_hash',
        'serve_resource_action_authority_policy_epochs.policy_epoch',
        'serve_resource_action_authority_policy_epochs.policy_sha256',
        'serve_resource_action_authority_policy_epochs.authority_binding_sha256',
    )


def test_sqlite_never_stamps_serve038(tmp_path) -> None:
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "serve-038.sqlite"}')
    _upgrade(engine, migration_utils.serve_target_version(engine))
    assert _revision(engine) == '037'
    inspector = sqlalchemy.inspect(engine)
    assert 'resource_action_candidate_epoch' not in {
        str(column['name']) for column in inspector.get_columns('services')
    }
    assert 'resource_action_spec_identity' not in {
        str(column['name']) for column in inspector.get_columns('version_specs')
    }
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    with pytest.raises(RuntimeError, match='PostgreSQL-only'):
        alembic_command.upgrade(config, '038')
    assert _revision(engine) == '037'


def test_postgres_fresh_038_catalog_and_retired_v1_grandfather(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    with postgres_engine.begin() as connection:
        connection.execute(action_schema.WORKER_COHORTS.insert().values(
            **_cohort_values('v1-retired', 'RETIRED', 1)))

    _upgrade(postgres_engine, '038')

    assert _revision(postgres_engine) == '038'
    inspector = sqlalchemy.inspect(postgres_engine)
    assert _NEW_TABLES <= set(inspector.get_table_names())
    with postgres_engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.text(
                'SELECT cohort_id, removal_authorized_at FROM '
                'serve_resource_action_worker_cohorts ORDER BY cohort_id')).all(
                )
    assert rows == [('v1-retired', None)]

    reflected = sqlalchemy.Table(action_schema.WORKER_COHORTS.name,
                                 sqlalchemy.MetaData(),
                                 autoload_with=postgres_engine)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(reflected.insert().values(
                **_cohort_values('v1-live-rejected', 'ACCEPTING', 1)))
    with postgres_engine.begin() as connection:
        connection.execute(reflected.insert().values(
            **_cohort_values('v2-live-accepted', 'ACCEPTING', 2)))


def test_postgres_adopts_exact_empty_policy_table(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    m4_schema.AUTHORITY_POLICY_EPOCHS.create(postgres_engine)

    _upgrade(postgres_engine, '038')

    assert _revision(postgres_engine) == '038'
    assert _NEW_TABLES <= set(
        sqlalchemy.inspect(postgres_engine).get_table_names())


def test_postgres_rejects_hostile_preexisting_038_table(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    m4_schema.AUTHORITY_POLICY_EPOCHS.create(postgres_engine)
    check = 'ck_serve_ra_authority_policy_epochs_admission'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {m4_schema.AUTHORITY_POLICY_EPOCHS.name} '
            f'DROP CONSTRAINT {check}')
        connection.exec_driver_sql(
            f'ALTER TABLE {m4_schema.AUTHORITY_POLICY_EPOCHS.name} '
            f'ADD CONSTRAINT {check} CHECK (admission_revision > 0)')

    with pytest.raises(RuntimeError, match='incompatible check constraints'):
        _upgrade(postgres_engine, '038')
    assert _revision(postgres_engine) == '037'
    service_columns = {
        str(column['name']) for column in sqlalchemy.inspect(
            postgres_engine).get_columns('services')
    }
    assert {
        'resource_action_candidate_epoch',
        'resource_action_candidate_policy_sha256',
        'resource_action_candidate_binding_sha256',
    } <= service_columns
    assert not (_NEW_TABLES -
                {m4_schema.AUTHORITY_POLICY_EPOCHS.name}).intersection(
                    sqlalchemy.inspect(postgres_engine).get_table_names())


def test_postgres_rejects_partial_bootstrap_column_set(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('ALTER TABLE services DROP COLUMN '
                                   'resource_action_candidate_policy_sha256')
        connection.exec_driver_sql('ALTER TABLE services DROP COLUMN '
                                   'resource_action_candidate_binding_sha256')

    with pytest.raises(RuntimeError, match='partial candidate-column set'):
        _upgrade(postgres_engine, '038')
    assert _revision(postgres_engine) == '037'


def test_postgres_rejects_weakened_serve033_check(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    table = action_schema.WORKER_COHORTS.name
    check = 'ck_serve_ra_worker_cohorts_state'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {table} DROP CONSTRAINT {check}')
        connection.exec_driver_sql(
            f'ALTER TABLE {table} ADD CONSTRAINT {check} '
            'CHECK (revision > 0)')

    with pytest.raises(RuntimeError, match='incompatible check constraints'):
        _upgrade(postgres_engine, '038')
    assert _revision(postgres_engine) == '037'


@pytest.mark.parametrize(
    'lifecycle_state',
    ['REGISTERING', 'ACCEPTING', 'DRAINING', 'REMOVAL_AUTHORIZED'])
def test_postgres_rejects_every_nonretired_v1_cohort(
        postgres_engine, lifecycle_state: str) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    with postgres_engine.begin() as connection:
        connection.execute(
            action_schema.WORKER_COHORTS.insert().values(**_cohort_values(
                f'v1-{lifecycle_state.lower()}', lifecycle_state, 1)))

    with pytest.raises(RuntimeError, match='exact retired V1 null-time'):
        _upgrade(postgres_engine, '038')
    assert _revision(postgres_engine) == '037'


def test_postgres_rejects_malformed_retired_v1_version_tokens(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    malformed = ({}, {'version': None}, {'version': '1'}, {'version': 1.0})
    with postgres_engine.begin() as connection:
        for index, registration in enumerate(malformed):
            values = _cohort_values(f'malformed-v1-{index}', 'RETIRED', 1)
            values['registration_attestations'] = registration
            connection.execute(
                action_schema.WORKER_COHORTS.insert().values(**values))
        accepted = connection.execute(
            sqlalchemy.text(
                "SELECT COUNT(*) FROM serve_resource_action_worker_cohorts "
                "WHERE ((registration_attestations -> 'version')::text = '1') "
                'IS TRUE')).scalar_one()
    assert accepted == 0

    with pytest.raises(RuntimeError, match='exact retired V1 null-time'):
        _upgrade(postgres_engine, '038')
    assert _revision(postgres_engine) == '037'


def test_postgres_rejects_stale_and_fresh_authority_instances(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    _upgrade_api_requests(postgres_engine)
    instances = sqlalchemy.Table('api_server_instances',
                                 sqlalchemy.MetaData(),
                                 autoload_with=postgres_engine)
    with postgres_engine.begin() as connection:
        for index, heartbeat_at in enumerate(
            (_NOW - datetime.timedelta(days=7), _NOW)):
            connection.execute(instances.insert().values(
                instance_id=uuid.uuid4(),
                role='authority-worker',
                pod_name=f'authority-{index}',
                pod_uid=str(uuid.uuid4()),
                pod_ip=f'10.0.0.{index + 1}',
                version='p2a-v1',
                started_at=heartbeat_at,
                heartbeat_at=heartbeat_at,
                draining_at=None,
                ready=index == 1,
                health_detail={},
                supported_handlers=[],
                supported_payload_versions={}))

    with pytest.raises(RuntimeError, match='zero stale or fresh'):
        _upgrade(postgres_engine, '038')
    assert _revision(postgres_engine) == '037'


def test_postgres_rejects_extra_common_table_check(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('ALTER TABLE services ADD CONSTRAINT '
                                   'ck_hostile_serve038_legacy_only '
                                   "CHECK (resource_action_mode = 'legacy')")

    with pytest.raises(RuntimeError, match='incompatible check constraints'):
        _upgrade(postgres_engine, '038')
    assert _revision(postgres_engine) == '037'
    assert 'removal_authorized_at' not in {
        str(column['name']) for column in sqlalchemy.inspect(
            postgres_engine).get_columns(action_schema.WORKER_COHORTS.name)
    }


def test_postgres_policy_lineage_and_reference_tuple_are_physical(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '038')
    policy_epoch = uuid.uuid4()
    with postgres_engine.begin() as connection:
        _insert_policy(connection, policy_epoch)
        connection.execute(
            sqlalchemy.text(
                'INSERT INTO serve_resource_action_worker_cohorts '
                '(cohort_id, deployment_uid, cohort_identity, '
                'cohort_identity_sha256, registration_attestations, '
                'registration_attestations_sha256, lifecycle_state, revision, '
                'created_at, state_changed_at) VALUES '
                '(:cohort, :deployment, CAST(:identity AS JSONB), :hash, '
                'CAST(:registration AS JSONB), :hash, :state, 1, :now, :now)'),
            {
                'cohort': 'v2-cohort',
                'deployment': 'deployment-v2',
                'identity': '{"version":2}',
                'registration': '{"version":2}',
                'hash': _SHA_A,
                'state': 'ACCEPTING',
                'now': _NOW,
            })

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            _insert_policy(connection, uuid.uuid4())

    reference = sqlalchemy.Table(action_schema.WORKER_COHORT_REFS.name,
                                 sqlalchemy.MetaData(),
                                 autoload_with=postgres_engine)
    common = {
        'cohort_id': 'v2-cohort',
        'service_hash': _SERVICE_HASH,
        'replica_incarnation': uuid.uuid4(),
        'desired_generation': 1,
        'action_type': 'launch',
        'controller_owner_fence': 'owner:1',
        'lifecycle_epoch': 1,
        'preparation_capability_sha256': _SHA_A,
        'reference_state': 'ACTION_ACTIVE',
        'revision': 1,
        'created_at': _NOW,
        'bound_at': _NOW,
        'released_at': None,
        'authority_policy_epoch': policy_epoch,
        'authority_policy_sha256': _SHA_A,
        'authority_binding_sha256': _SHA_B,
    }
    with postgres_engine.begin() as connection:
        connection.execute(reference.insert().values(decision_id=uuid.uuid4(),
                                                     **common))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(reference.insert().values(
                decision_id=uuid.uuid4(),
                **{
                    **common, 'authority_policy_sha256': _SHA_B
                }))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(reference.insert().values(
                decision_id=uuid.uuid4(),
                **{
                    **common,
                    'authority_policy_epoch': None,
                    'authority_policy_sha256': None,
                    'authority_binding_sha256': None,
                }))


def test_postgres_replica_links_require_version_identity_hash(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '038')
    replicas = sqlalchemy.Table('replicas',
                                sqlalchemy.MetaData(),
                                autoload_with=postgres_engine)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(replicas.insert().values(
                service_name='identity-bound-service',
                replica_id=1,
                replica_incarnation=uuid.uuid4(),
                desired_generation=1,
                sky_cluster_record_uuid=uuid.uuid4(),
                launch_action_id=uuid.uuid4(),
                resource_action_spec_identity_sha256=None))
    with postgres_engine.begin() as connection:
        connection.execute(replicas.insert().values(
            service_name='identity-bound-service',
            replica_id=2,
            replica_incarnation=uuid.uuid4(),
            desired_generation=1,
            sky_cluster_record_uuid=uuid.uuid4(),
            launch_action_id=uuid.uuid4(),
            resource_action_spec_identity_sha256=_SHA_A))


def test_postgres_handoff_requires_source_embedded_revision(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '038')
    cohorts = sqlalchemy.Table(action_schema.WORKER_COHORTS.name,
                               sqlalchemy.MetaData(),
                               autoload_with=postgres_engine)
    with postgres_engine.begin() as connection:
        connection.execute(cohorts.insert().values(
            **_cohort_values('handoff-source-revision', 'ACCEPTING', 2)))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(
                m4_schema.WORKER_REGISTRATION_HANDOFFS.insert().values(
                    **_open_handoff_values('handoff-source-revision',
                                           embedded_source_revision=999)))
    with postgres_engine.begin() as connection:
        connection.execute(
            m4_schema.WORKER_REGISTRATION_HANDOFFS.insert().values(
                **_open_handoff_values('handoff-source-revision',
                                       embedded_source_revision=1)))


def test_postgres_writer_lock_timeout_is_atomic(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '037')
    # Alembic reconnects from the rendered URL, so the timeout must be part of
    # that URL rather than an engine-only ``connect_args`` value.
    timed_url = postgres_engine.url.update_query_dict(
        {'options': '-c lock_timeout=200ms'})
    timed_engine = sqlalchemy.create_engine(timed_url)
    try:
        with postgres_engine.connect() as writer:
            transaction = writer.begin()
            try:
                writer.execute(
                    serve_state_schema.services_table.insert().values(
                        name='serve038-lock-writer',
                        resource_action_mode='shadow',
                        resource_action_mode_changed_at=_NOW))
                with pytest.raises(sqlalchemy.exc.OperationalError):
                    _upgrade(timed_engine, '038')
                assert _revision(postgres_engine) == '037'
                assert 'removal_authorized_at' not in {
                    str(column['name'])
                    for column in sqlalchemy.inspect(postgres_engine).
                    get_columns(action_schema.WORKER_COHORTS.name)
                }
            finally:
                transaction.rollback()
    finally:
        timed_engine.dispose()

    _upgrade(postgres_engine, '038')
    assert _revision(postgres_engine) == '038'


def test_postgres_closed_checks_reject_null_discriminated_shapes(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '038')
    services = sqlalchemy.Table('services',
                                sqlalchemy.MetaData(),
                                autoload_with=postgres_engine)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(services.insert().values(
                name='null-shadow-hashes',
                resource_action_mode='shadow',
                resource_action_mode_changed_at=_NOW,
                resource_action_candidate_epoch=uuid.uuid4(),
                resource_action_candidate_policy_sha256=None,
                resource_action_candidate_binding_sha256=None))

    crash_runs = m4_schema.CRASH_CANARY_RUNS
    base_run = {
        'service_name': 'svc',
        'service_hash': _SERVICE_HASH,
        'service_incarnation': uuid.UUID(_SERVICE_HASH),
        'candidate_epoch': uuid.uuid4(),
        'boundary_id': 'request-boundary',
        'run_id': uuid.uuid4(),
        'subject_kind': 'request',
        'action_kind': 'launch',
        'action_id': uuid.uuid4(),
        'attempt': None,
        'request_id': str(uuid.uuid4()),
        'qualification_policy_sha256': _SHA_A,
        'qualification_binding_sha256': _SHA_A,
        'injection_nonce_sha256': _SHA_A,
        'run_state': 'STARTED',
        'revision': 1,
        'started_at': _NOW,
    }
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(crash_runs.insert().values(**base_run))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(crash_runs.insert().values(
                **{
                    **base_run,
                    'candidate_epoch': uuid.uuid4(),
                    'boundary_id': 'service-boundary',
                    'run_id': uuid.uuid4(),
                    'subject_kind': 'service',
                    'action_id': None,
                    'request_id': None,
                    'run_state': 'COMPLETED',
                    'revision': 2,
                    'verification_evidence': {
                        'version': 1
                    },
                    'verification_evidence_sha256': _SHA_A,
                    'outcome': None,
                    'completed_at': _NOW,
                }))


def test_postgres_serve038_refuses_downgrade(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '038')
    config = migration_utils.get_alembic_config(postgres_engine,
                                                migration_utils.SERVE_DB_NAME)
    with pytest.raises(RuntimeError, match='cannot be downgraded'):
        alembic_command.downgrade(config, '034')
    assert _revision(postgres_engine) == '038'
