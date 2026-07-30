"""Real-PostgreSQL tests for physical-capacity revision 001."""
# pylint: disable=redefined-outer-name

import concurrent.futures
import importlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import uuid

from alembic import migration
from alembic import operations
import pytest
import sqlalchemy

from sky.physical_capacity import schema
from sky.physical_capacity import state
from sky.skylet import constants
from sky.utils.db import migration_utils

_POSTGRES_URI = os.environ.get('SKYPILOT_TEST_POSTGRES_URI')
testcontainers_postgres = (None if _POSTGRES_URI is not None else
                           pytest.importorskip('testcontainers.postgres'))
pytest.importorskip('psycopg2')

pytestmark = [
    pytest.mark.skipif(
        _POSTGRES_URI is None and shutil.which('docker') is None,
        reason=('docker unavailable; skipping physical-capacity PostgreSQL '
                'tests')),
    pytest.mark.xdist_group(name='physical_capacity_schema_pg'),
]

_MIGRATION = importlib.import_module(
    'sky.schemas.db.capacity_state.001_initial_schema')
_TABLES = {
    'capacity_projection_scans',
    'capacity_groups',
    'capacity_group_intents',
    'capacity_allocations',
    'capacity_allocation_desires',
}


@pytest.fixture(scope='module')
def postgres_engine():
    if _POSTGRES_URI is not None:
        engine = sqlalchemy.create_engine(_POSTGRES_URI)
        try:
            yield engine
        finally:
            engine.dispose()
        return

    assert testcontainers_postgres is not None
    container = None
    try:
        container = testcontainers_postgres.PostgresContainer('postgres:16')
        container.start()
    except Exception as e:  # pylint: disable=broad-except
        pytest.skip(f'could not start postgres container: {e}')
    assert container is not None
    engine = sqlalchemy.create_engine(container.get_connection_url())
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


def _reset_schema(engine: sqlalchemy.engine.Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')


def _migration_call(engine: sqlalchemy.engine.Engine, function) -> None:
    with engine.begin() as connection:
        context = migration.MigrationContext.configure(connection)
        with operations.Operations.context(context):
            function()


def _catalog_shape(engine: sqlalchemy.engine.Engine) -> dict[str, tuple]:
    with engine.connect() as connection:
        tables = tuple(
            connection.execute(
                sqlalchemy.text("""
                    SELECT c.relname
                    FROM pg_catalog.pg_class AS c
                    JOIN pg_catalog.pg_namespace AS n
                      ON n.oid = c.relnamespace
                    WHERE n.nspname = current_schema()
                      AND c.relkind = 'r'
                      AND c.relname LIKE 'capacity_%'
                    ORDER BY c.relname
                """)).scalars())
        columns = tuple(
            tuple(row) for row in connection.execute(
                sqlalchemy.text("""
                    SELECT c.relname, a.attnum, a.attname,
                           pg_catalog.format_type(a.atttypid, a.atttypmod),
                           a.attnotnull,
                           pg_catalog.pg_get_expr(
                               d.adbin, d.adrelid, false)
                    FROM pg_catalog.pg_attribute AS a
                    JOIN pg_catalog.pg_class AS c
                      ON c.oid = a.attrelid
                    JOIN pg_catalog.pg_namespace AS n
                      ON n.oid = c.relnamespace
                    LEFT JOIN pg_catalog.pg_attrdef AS d
                      ON d.adrelid = a.attrelid
                     AND d.adnum = a.attnum
                    WHERE n.nspname = current_schema()
                      AND c.relkind = 'r'
                      AND c.relname LIKE 'capacity_%'
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY c.relname, a.attnum
                """)))
        constraints = tuple(
            tuple(row) for row in connection.execute(
                sqlalchemy.text("""
                    SELECT c.relname, k.conname, k.contype,
                           k.condeferrable, k.condeferred, k.confdeltype,
                           pg_catalog.pg_get_constraintdef(k.oid, false)
                    FROM pg_catalog.pg_constraint AS k
                    JOIN pg_catalog.pg_class AS c
                      ON c.oid = k.conrelid
                    JOIN pg_catalog.pg_namespace AS n
                      ON n.oid = c.relnamespace
                    WHERE n.nspname = current_schema()
                      AND c.relname LIKE 'capacity_%'
                    ORDER BY c.relname, k.conname
                """)))
        indexes = tuple(
            tuple(row) for row in connection.execute(
                sqlalchemy.text("""
                    SELECT tablename, indexname, indexdef
                    FROM pg_catalog.pg_indexes
                    WHERE schemaname = current_schema()
                      AND tablename LIKE 'capacity_%'
                    ORDER BY tablename, indexname
                """)))
    return {
        'tables': tables,
        'columns': columns,
        'constraints': constraints,
        'indexes': indexes,
    }


def test_migration_matches_runtime_metadata(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    _migration_call(postgres_engine, _MIGRATION.upgrade)
    migration_shape = _catalog_shape(postgres_engine)

    assert set(migration_shape['tables']) == _TABLES
    constraints = {row[1]: row for row in migration_shape['constraints']}
    for name in ('fk_capacity_groups_current_intent',
                 'fk_capacity_group_intents_group'):
        constraint = constraints[name]
        assert constraint[2] == 'f'
        assert constraint[3] is True
        assert constraint[4] is True
        assert constraint[5] == 'a'  # ON DELETE NO ACTION.
    for name in ('fk_capacity_groups_last_seen_scan',
                 'fk_capacity_allocations_last_seen_scan'):
        assert constraints[name][5] == 'n'  # ON DELETE SET NULL.
    for name in ('fk_capacity_allocations_group',
                 'fk_capacity_allocations_birth_intent',
                 'fk_capacity_allocation_desires_intent',
                 'fk_capacity_allocation_desires_allocation'):
        assert constraints[name][5] == 'r'  # ON DELETE RESTRICT.

    indexes = {row[1]: row[2] for row in migration_shape['indexes']}
    cluster_index = indexes['uq_capacity_allocations_active_cluster_hash']
    assert 'CREATE UNIQUE INDEX' in cluster_index
    assert '(cluster_hash)' in cluster_index
    assert 'cluster_hash IS NOT NULL' in cluster_index
    assert 'lifecycle_state' in cluster_index
    intent_index = indexes['ix_capacity_group_intents_intent_hash']
    assert 'CREATE UNIQUE INDEX' not in intent_index
    assert '(group_id, intent_hash)' in intent_index

    _reset_schema(postgres_engine)
    with postgres_engine.begin() as connection:
        schema.METADATA.create_all(connection)
    assert _catalog_shape(postgres_engine) == migration_shape


@pytest.mark.parametrize('mode', ['upgrade', 'bootstrap'])
def test_alembic_lineage_upgrade_verify_and_later_additive_revision(
        postgres_engine: sqlalchemy.engine.Engine,
        mode: migration_utils.MigrationMode) -> None:
    _reset_schema(postgres_engine)

    state.initialize_schema(postgres_engine, mode=mode)

    assert migration_utils.get_current_alembic_revision(
        postgres_engine, migration_utils.CAPACITY_STATE_DB_NAME) == '001'
    assert set(_catalog_shape(postgres_engine)['tables']) == _TABLES
    state.initialize_schema(postgres_engine, mode='verify')

    # Revision checks deliberately accept a newer additive numeric schema so
    # an older C1 binary remains usable during a later expand-first rollout.
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql("UPDATE alembic_version_capacity_state_db "
                                   "SET version_num = '002'")
    state.initialize_schema(postgres_engine, mode='verify')


def test_alembic_verify_rejects_missing_capacity_lineage(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)

    with pytest.raises(RuntimeError, match='revision uninitialized'):
        state.initialize_schema(postgres_engine, mode='verify')


def test_concurrent_first_alembic_upgrade_converges_once(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    start = threading.Barrier(2)

    def upgrade(_index: int) -> None:
        start.wait()
        state.initialize_schema(postgres_engine, mode='upgrade')

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(upgrade, range(2)))

    assert migration_utils.get_current_alembic_revision(
        postgres_engine, migration_utils.CAPACITY_STATE_DB_NAME) == '001'
    assert set(_catalog_shape(postgres_engine)['tables']) == _TABLES


def test_fresh_process_bootstraps_all_lineages_with_capacity(
        postgres_engine: sqlalchemy.engine.Engine, tmp_path: Path) -> None:
    fresh_schema = f'capacity_bootstrap_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {fresh_schema}')
    fresh_url = postgres_engine.url.update_query_dict(
        {'options': f'-csearch_path={fresh_schema}'})
    empty_config = tmp_path / 'config.yaml'
    empty_config.write_text('', encoding='utf-8')
    environment = os.environ.copy()
    environment.update({
        constants.ENV_VAR_IS_SKYPILOT_SERVER: 'true',
        constants.ENV_VAR_DB_CONNECTION_URI:
            fresh_url.render_as_string(hide_password=False),
        constants.ENV_VAR_STATE_DB_MIGRATION_MODE: 'bootstrap',
        'SKYPILOT_API_REQUEST_BACKEND': 'postgres',
        'SKYPILOT_GLOBAL_CONFIG': str(empty_config),
        'SKYPILOT_PHYSICAL_CAPACITY_MODE': 'disabled',
    })
    repository_root = Path(__file__).resolve().parents[2]
    existing_pythonpath = environment.get('PYTHONPATH')
    environment['PYTHONPATH'] = (
        str(repository_root) if not existing_pythonpath else
        f'{repository_root}{os.pathsep}{existing_pythonpath}')
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'sky.server.database_migrations'],
            cwd=repository_root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=120)
        assert result.returncode == 0, (f'migration stderr:\n{result.stderr}\n'
                                        f'migration stdout:\n{result.stdout}')

        with sqlalchemy.create_engine(fresh_url).connect() as connection:
            revisions = {
                table: connection.execute(
                    sqlalchemy.text(f'SELECT version_num FROM {table}')
                ).scalar_one() for table in (
                    'alembic_version_state_db',
                    'alembic_version_sky_config_db',
                    'alembic_version_serve_state_db',
                    'alembic_version_spot_jobs_db',
                    'alembic_version_api_requests_db',
                    'alembic_version_capacity_state_db',
                )
            }
        assert revisions == {
            'alembic_version_state_db':
                migration_utils.GLOBAL_USER_STATE_VERSION,
            'alembic_version_sky_config_db':
                migration_utils.SKYPILOT_CONFIG_VERSION,
            'alembic_version_serve_state_db': migration_utils.SERVE_VERSION,
            'alembic_version_spot_jobs_db': migration_utils.SPOT_JOBS_VERSION,
            'alembic_version_api_requests_db':
                migration_utils.API_REQUESTS_VERSION,
            'alembic_version_capacity_state_db':
                migration_utils.CAPACITY_STATE_VERSION,
        }
    finally:
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {fresh_schema} CASCADE')


def test_deferred_cycle_and_hash_uniqueness(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    _migration_call(postgres_engine, _MIGRATION.upgrade)
    group_id = uuid.uuid4()
    digest = 'a' * 64
    with postgres_engine.begin() as connection:
        # The group and first intent point at each other and must be publishable
        # atomically despite either insert order.
        connection.execute(schema.GROUPS.insert().values(
            group_id=group_id,
            workspace='workspace-a',
            owner_kind='managed_job_task',
            writer_fence_kind='legacy',
            source_kind='managed_job_task',
            source_key='job-1/task-0',
            source_incarnation_hash=digest,
            projection_confidence='legacy',
            current_intent_generation=1,
            created_by_actor_id='projector',
            updated_by_actor_id='projector',
            created_by_actor_type='system',
            updated_by_actor_type='system'))
        intent = {
            'group_id': group_id,
            'workspace': 'workspace-a',
            'schema_version': 1,
            'placement_contract': {},
            'placement_contract_hash': digest,
            'desired_count': 2,
            'topology': {},
            'intent_hash': digest,
            'source_fingerprint': digest,
            'created_by_actor_id': 'projector',
            'created_by_actor_type': 'system',
        }
        connection.execute(schema.GROUP_INTENTS.insert().values(
            intent_generation=1, **intent))
        # An A-to-B-to-A history is legal: intent_hash is lookup-only.
        connection.execute(schema.GROUP_INTENTS.insert().values(
            intent_generation=2, **intent))
        connection.execute(schema.ALLOCATIONS.insert().values(
            allocation_id=uuid.uuid4(),
            group_id=group_id,
            workspace='workspace-a',
            created_by_intent_generation=1,
            source_kind='managed_job_cluster',
            source_key='cluster-1',
            source_incarnation_hash=digest,
            identity_confidence='legacy',
            cluster_hash='global-cluster-generation'))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(schema.ALLOCATIONS.insert().values(
                allocation_id=uuid.uuid4(),
                group_id=group_id,
                workspace='workspace-a',
                created_by_intent_generation=1,
                source_kind='managed_job_cluster',
                source_key='cluster-2',
                source_incarnation_hash='b' * 64,
                identity_confidence='legacy',
                cluster_hash='global-cluster-generation'))


def test_same_workspace_foreign_keys_reject_cross_tenant_rows(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    _migration_call(postgres_engine, _MIGRATION.upgrade)
    group_id = uuid.uuid4()
    allocation_id = uuid.uuid4()
    digest = 'a' * 64
    intent_values = {
        'group_id': group_id,
        'intent_generation': 1,
        'placement_contract': {},
        'placement_contract_hash': digest,
        'desired_count': 1,
        'topology': {},
        'intent_hash': digest,
        'source_fingerprint': digest,
        'created_by_actor_id': 'projector',
        'created_by_actor_type': 'system',
    }
    with postgres_engine.begin() as connection:
        connection.execute(schema.GROUPS.insert().values(
            group_id=group_id,
            workspace='workspace-a',
            owner_kind='managed_job_task',
            writer_fence_kind='legacy',
            source_kind='managed_job_task',
            source_key='job-1/task-0',
            source_incarnation_hash=digest,
            projection_confidence='legacy',
            current_intent_generation=1,
            created_by_actor_id='projector',
            updated_by_actor_id='projector',
            created_by_actor_type='system',
            updated_by_actor_type='system'))
        connection.execute(schema.GROUP_INTENTS.insert().values(
            workspace='workspace-a', **intent_values))
        connection.execute(schema.ALLOCATIONS.insert().values(
            allocation_id=allocation_id,
            group_id=group_id,
            workspace='workspace-a',
            created_by_intent_generation=1,
            source_kind='managed_job_cluster',
            source_key='cluster-1',
            source_incarnation_hash=digest,
            identity_confidence='legacy'))

    with pytest.raises(sqlalchemy.exc.IntegrityError) as intent_error:
        with postgres_engine.begin() as connection:
            cross_workspace_intent = {
                **intent_values,
                'intent_generation': 2,
            }
            connection.execute(schema.GROUP_INTENTS.insert().values(
                workspace='workspace-b', **cross_workspace_intent))
    assert 'fk_capacity_group_intents_group' in str(intent_error.value)

    with pytest.raises(sqlalchemy.exc.IntegrityError) as allocation_error:
        with postgres_engine.begin() as connection:
            connection.execute(schema.ALLOCATIONS.insert().values(
                allocation_id=uuid.uuid4(),
                group_id=group_id,
                workspace='workspace-b',
                created_by_intent_generation=1,
                source_kind='managed_job_cluster',
                source_key='cluster-cross-tenant',
                source_incarnation_hash='b' * 64,
                identity_confidence='legacy'))
    assert any(constraint in str(allocation_error.value) for constraint in (
        'fk_capacity_allocations_group',
        'fk_capacity_allocations_birth_intent',
    ))

    with pytest.raises(sqlalchemy.exc.IntegrityError) as desire_error:
        with postgres_engine.begin() as connection:
            connection.execute(schema.ALLOCATION_DESIRES.insert().values(
                group_id=group_id,
                workspace='workspace-b',
                intent_generation=1,
                allocation_id=allocation_id,
                ordinal=0,
                desired_state='present',
                reason_code='projection'))
    assert any(constraint in str(desire_error.value) for constraint in (
        'fk_capacity_allocation_desires_intent',
        'fk_capacity_allocation_desires_allocation',
    ))


def test_downgrade_is_empty_only(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    _migration_call(postgres_engine, _MIGRATION.upgrade)
    scan_id = uuid.uuid4()
    group_id = uuid.uuid4()
    allocation_id = uuid.uuid4()
    digest = 'a' * 64
    with postgres_engine.begin() as connection:
        connection.execute(schema.PROJECTION_SCANS.insert().values(
            scan_id=scan_id,
            workspace='workspace-a',
            source_kind='managed_job_task',
            source_partition_hash=digest,
            cursor={},
            state='running'))
        connection.execute(schema.GROUPS.insert().values(
            group_id=group_id,
            workspace='workspace-a',
            owner_kind='managed_job_task',
            writer_fence_kind='legacy',
            source_kind='managed_job_task',
            source_key='job-1/task-0',
            source_incarnation_hash=digest,
            projection_confidence='legacy',
            current_intent_generation=1,
            last_seen_scan_id=scan_id,
            created_by_actor_id='projector',
            updated_by_actor_id='projector',
            created_by_actor_type='system',
            updated_by_actor_type='system'))
        connection.execute(schema.GROUP_INTENTS.insert().values(
            group_id=group_id,
            workspace='workspace-a',
            intent_generation=1,
            placement_contract={},
            placement_contract_hash=digest,
            desired_count=1,
            topology={},
            intent_hash=digest,
            source_fingerprint=digest,
            created_by_actor_id='projector',
            created_by_actor_type='system'))
        connection.execute(schema.ALLOCATIONS.insert().values(
            allocation_id=allocation_id,
            group_id=group_id,
            workspace='workspace-a',
            created_by_intent_generation=1,
            source_kind='managed_job_cluster',
            source_key='cluster-1',
            source_incarnation_hash=digest,
            identity_confidence='legacy',
            last_seen_scan_id=scan_id))
        connection.execute(schema.ALLOCATION_DESIRES.insert().values(
            group_id=group_id,
            workspace='workspace-a',
            intent_generation=1,
            allocation_id=allocation_id,
            ordinal=0,
            desired_state='present',
            reason_code='projection'))

    with pytest.raises(RuntimeError) as exc_info:
        _migration_call(postgres_engine, _MIGRATION.downgrade)
    for table in _TABLES:
        assert table in str(exc_info.value)
    assert set(_catalog_shape(postgres_engine)['tables']) == _TABLES

    with postgres_engine.begin() as connection:
        connection.execute(schema.ALLOCATION_DESIRES.delete())
        connection.execute(schema.ALLOCATIONS.delete())
        connection.execute(schema.GROUP_INTENTS.delete())
        connection.execute(schema.GROUPS.delete())
        connection.execute(schema.PROJECTION_SCANS.delete())
    _migration_call(postgres_engine, _MIGRATION.downgrade)
    assert not _catalog_shape(postgres_engine)['tables']


@pytest.mark.parametrize('locked_table', sorted(_TABLES))
def test_downgrade_locks_every_capacity_table(
        postgres_engine: sqlalchemy.engine.Engine, locked_table: str) -> None:
    _reset_schema(postgres_engine)
    _migration_call(postgres_engine, _MIGRATION.upgrade)
    with postgres_engine.connect() as holder:
        holder_transaction = holder.begin()
        try:
            holder.exec_driver_sql(
                f'LOCK TABLE {locked_table} IN ROW EXCLUSIVE MODE')
            with pytest.raises(sqlalchemy.exc.OperationalError):
                with postgres_engine.begin() as connection:
                    connection.exec_driver_sql(
                        "SET LOCAL lock_timeout = '100ms'")
                    context = migration.MigrationContext.configure(connection)
                    with operations.Operations.context(context):
                        _MIGRATION.downgrade()
        finally:
            holder_transaction.rollback()
