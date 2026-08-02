"""Real-PostgreSQL proofs for cluster-record identity revision 028."""
# pylint: disable=protected-access,redefined-outer-name

import concurrent.futures
import importlib
import os
import shutil
import threading
import uuid

from alembic import migration
from alembic import operations
import pytest
import sqlalchemy
from sqlalchemy import orm

from sky import global_user_state
from sky.utils.db import migration_utils

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
testcontainers_postgres = None
if _POSTGRES_URL is None:
    testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    _POSTGRES_URL is None and shutil.which('docker') is None,
    reason='docker unavailable; skipping cluster identity PostgreSQL tests')

_RECORD_UUID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_OTHER_UUID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_INDEX = 'uq_clusters_cluster_record_uuid_nonnull'


class _EngineManager:

    def __init__(self, engine: sqlalchemy.engine.Engine) -> None:
        self._engine = engine

    def get_engine(self) -> sqlalchemy.engine.Engine:
        return self._engine


class _MinimalHandle:
    launched_resources = None

    def __init__(self, marker: str) -> None:
        self.marker = marker


def _migration_call(engine: sqlalchemy.engine.Engine, function) -> None:
    with engine.begin() as connection:
        context = migration.MigrationContext.configure(connection)
        with operations.Operations.context(context):
            function()


@pytest.fixture(scope='module')
def postgres_engine():
    container = None
    admin_engine = None
    temporary_database = None
    if _POSTGRES_URL is None:
        assert testcontainers_postgres is not None
        try:
            container = testcontainers_postgres.PostgresContainer('postgres:16')
            container.start()
        except Exception as e:  # pylint: disable=broad-except
            pytest.skip(f'could not start postgres container: {e}')
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_cluster_identity_{uuid.uuid4().hex}'
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
                    sqlalchemy.text(
                        'SELECT pg_terminate_backend(pid) '
                        'FROM pg_stat_activity '
                        'WHERE datname = :database AND pid <> pg_backend_pid()'
                    ), {'database': temporary_database})
                connection.exec_driver_sql(f'DROP DATABASE {quoted}')
            admin_engine.dispose()
        elif container is not None:
            container.stop()


@pytest.fixture
def identity_database(postgres_engine, monkeypatch):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
        connection.exec_driver_sql(
            'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
    migration_028 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '028_resource_action_cluster_identity')
    _migration_call(postgres_engine, migration_028.upgrade)
    monkeypatch.setattr(global_user_state, '_db_manager',
                        _EngineManager(postgres_engine))
    return postgres_engine


@pytest.fixture
def full_state_database(postgres_engine, monkeypatch):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    global_user_state.Base.metadata.create_all(postgres_engine)
    monkeypatch.setattr(global_user_state, '_db_manager',
                        _EngineManager(postgres_engine))
    return postgres_engine


def _identity_rows(engine: sqlalchemy.engine.Engine):
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.select(
                global_user_state.cluster_table.c.name,
                global_user_state.cluster_table.c.cluster_record_uuid).order_by(
                    global_user_state.cluster_table.c.name)).all()


def _commit_identity(
    engine: sqlalchemy.engine.Engine,
    name: str,
    record_uuid: uuid.UUID | str,
) -> global_user_state.ClusterRecordIdentityWriteOutcome:
    with orm.Session(engine) as session:
        outcome = global_user_state._commit_cluster_record_identity_in_session(
            session, name, record_uuid)
        session.commit()
        return outcome


def test_action_aware_identity_insert_adopt_and_borrowed_transaction(
        identity_database) -> None:
    with orm.Session(identity_database) as session:
        outcome = global_user_state._commit_cluster_record_identity_in_session(
            session, 'borrowed', _RECORD_UUID)
        assert outcome is (
            global_user_state.ClusterRecordIdentityWriteOutcome.INSERTED)
        session.rollback()
    assert _identity_rows(identity_database) == []

    inserted = _commit_identity(identity_database, 'cluster-a',
                                str(_RECORD_UUID))
    assert inserted is (
        global_user_state.ClusterRecordIdentityWriteOutcome.INSERTED)
    adopted = _commit_identity(identity_database, 'cluster-a', _RECORD_UUID)
    assert adopted is (
        global_user_state.ClusterRecordIdentityWriteOutcome.ADOPTED)
    assert _identity_rows(identity_database) == [('cluster-a', _RECORD_UUID)]


def test_action_aware_identity_rejects_every_name_and_uuid_collision(
        identity_database) -> None:
    table = global_user_state.cluster_table
    with identity_database.begin() as connection:
        connection.execute(table.insert().values(name='legacy-null'))
        connection.execute(table.insert().values(
            name='committed', cluster_record_uuid=_RECORD_UUID))

    with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                       match='null'):
        _commit_identity(identity_database, 'legacy-null', _OTHER_UUID)
    with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                       match='expected'):
        _commit_identity(identity_database, 'committed', _OTHER_UUID)
    with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                       match='already committed'):
        _commit_identity(identity_database, 'different-name', _RECORD_UUID)

    assert _identity_rows(identity_database) == [('committed', _RECORD_UUID),
                                                 ('legacy-null', None)]


@pytest.mark.parametrize('value', [
    '11111111111141118111111111111111',
    '11111111-1111-4111-8111-11111111111A',
    '{11111111-1111-4111-8111-111111111111}',
    'not-a-uuid',
])
def test_action_aware_identity_rejects_noncanonical_uuid_text(
        identity_database, value: str) -> None:
    with pytest.raises(ValueError, match='canonical UUID text'):
        _commit_identity(identity_database, 'invalid', value)
    assert _identity_rows(identity_database) == []


def test_action_aware_cluster_upsert_is_atomic_and_ordinary_updates_preserve(
        full_state_database) -> None:
    cluster_hash = global_user_state.add_or_update_cluster(
        'complete-row',
        _MinimalHandle('initial'),
        requested_resources=set(),
        ready=False,
        is_managed=True,
        cluster_record_uuid=_RECORD_UUID,
    )
    with full_state_database.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(global_user_state.cluster_table).where(
                global_user_state.cluster_table.c.name ==
                'complete-row')).mappings().one()
    assert row['cluster_record_uuid'] == _RECORD_UUID
    assert row['cluster_hash'] == cluster_hash
    assert row['handle'] is not None
    assert row['status'] == 'INIT'
    assert row['is_managed'] == 1

    global_user_state.add_or_update_cluster(
        'complete-row',
        _MinimalHandle('ordinary-update'),
        requested_resources=set(),
        ready=True,
    )
    with full_state_database.connect() as connection:
        retained = connection.execute(
            sqlalchemy.select(
                global_user_state.cluster_table.c.cluster_record_uuid).where(
                    global_user_state.cluster_table.c.name ==
                    'complete-row')).scalar_one()
    assert retained == _RECORD_UUID

    global_user_state.add_or_update_cluster(
        'legacy-null',
        _MinimalHandle('legacy'),
        requested_resources=set(),
        ready=False,
    )
    with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                       match='null'):
        global_user_state.add_or_update_cluster(
            'legacy-null',
            _MinimalHandle('action-aware'),
            requested_resources=set(),
            ready=False,
            cluster_record_uuid=_OTHER_UUID,
        )


def test_action_aware_cluster_snapshot_is_exact_and_transaction_owned(
        full_state_database) -> None:
    handle = _MinimalHandle('snapshot')
    global_user_state.add_or_update_cluster(
        'snapshot-row',
        handle,
        requested_resources=set(),
        ready=False,
        cluster_record_uuid=_RECORD_UUID,
    )
    with orm.Session(full_state_database) as session:
        snapshot = (global_user_state._read_cluster_record_identity_in_session(
            session, 'snapshot-row', _RECORD_UUID))
        assert snapshot is not None
        assert snapshot.cluster_name == 'snapshot-row'
        assert snapshot.cluster_record_uuid == _RECORD_UUID
        assert snapshot.serialized_handle
        assert snapshot.handle.marker == 'snapshot'
        session.rollback()

    with orm.Session(full_state_database) as session:
        assert (global_user_state._read_cluster_record_identity_in_session(
            session, 'missing-row', _OTHER_UUID) is None)


def test_action_aware_cluster_snapshot_rejects_incompatible_rows(
        full_state_database) -> None:
    global_user_state.add_or_update_cluster(
        'committed-row',
        _MinimalHandle('committed'),
        requested_resources=set(),
        ready=False,
        cluster_record_uuid=_RECORD_UUID,
    )
    global_user_state.add_or_update_cluster(
        'legacy-row',
        _MinimalHandle('legacy'),
        requested_resources=set(),
        ready=False,
    )
    with orm.Session(full_state_database) as session:
        with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                           match='incompatible'):
            global_user_state._read_cluster_record_identity_in_session(
                session, 'committed-row', _OTHER_UUID)
        session.rollback()
    with orm.Session(full_state_database) as session:
        with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                           match='null'):
            global_user_state._read_cluster_record_identity_in_session(
                session, 'legacy-row', _OTHER_UUID)


def test_expected_identity_removal_exactly_deletes_and_adopts_absence(
        full_state_database) -> None:
    assert full_state_database is not None
    handle = _MinimalHandle('remove-exact')
    global_user_state.add_or_update_cluster(
        'remove-row',
        handle,
        requested_resources=set(),
        ready=True,
        cluster_record_uuid=_RECORD_UUID,
    )

    outcome = global_user_state.remove_cluster(
        'remove-row',
        terminate=True,
        expected_cluster_record_uuid=_RECORD_UUID,
        expected_cluster_handle=handle,
    )
    assert outcome is global_user_state.ClusterRecordRemovalOutcome.REMOVED_EXACT
    assert global_user_state.get_cluster_from_name('remove-row') is None

    replay = global_user_state.remove_cluster(
        'remove-row',
        terminate=True,
        expected_cluster_record_uuid=_RECORD_UUID,
        expected_cluster_handle=handle,
    )
    assert replay is global_user_state.ClusterRecordRemovalOutcome.ALREADY_ABSENT


def test_expected_identity_removal_rejects_handle_or_identity_replacement(
        full_state_database) -> None:
    assert full_state_database is not None
    handle = _MinimalHandle('original')
    global_user_state.add_or_update_cluster(
        'protected-row',
        handle,
        requested_resources=set(),
        ready=True,
        cluster_record_uuid=_RECORD_UUID,
    )

    with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                       match='different persisted handle'):
        global_user_state.remove_cluster(
            'protected-row',
            terminate=True,
            expected_cluster_record_uuid=_RECORD_UUID,
            expected_cluster_handle=_MinimalHandle('replacement'),
        )
    with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                       match='incompatible'):
        global_user_state.remove_cluster(
            'protected-row',
            terminate=True,
            expected_cluster_record_uuid=_OTHER_UUID,
            expected_cluster_handle=handle,
        )
    with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                       match='unexpectedly has a row'):
        global_user_state.remove_cluster(
            'protected-row',
            terminate=True,
            expected_cluster_record_uuid=_RECORD_UUID,
            expected_cluster_handle=None,
        )
    retained = global_user_state.get_cluster_from_name('protected-row')
    assert retained is not None
    assert retained['handle'].marker == 'original'


def test_expected_identity_removal_requires_closed_action_fence(
        full_state_database) -> None:
    assert full_state_database is not None
    with pytest.raises(ValueError, match='explicit expected handle'):
        global_user_state.remove_cluster(
            'missing-row',
            terminate=True,
            expected_cluster_record_uuid=_RECORD_UUID,
        )
    with pytest.raises(ValueError, match='requires terminate=True'):
        global_user_state.remove_cluster(
            'missing-row',
            terminate=False,
            expected_cluster_record_uuid=_RECORD_UUID,
            expected_cluster_handle=None,
        )
    with pytest.raises(ValueError, match='mutually exclusive'):
        global_user_state.remove_cluster(
            'missing-row',
            terminate=True,
            existing_cluster_hash='legacy-hash',
            expected_cluster_record_uuid=_RECORD_UUID,
            expected_cluster_handle=None,
        )


def test_action_aware_upsert_and_inverse_uuid_claim_do_not_deadlock(
        full_state_database, monkeypatch) -> None:
    global_user_state.add_or_update_cluster(
        'committed',
        _MinimalHandle('initial'),
        requested_resources=set(),
        ready=False,
        cluster_record_uuid=_RECORD_UUID,
    )
    adopter_at_uuid_lock = threading.Event()
    collider_has_uuid_lock = threading.Event()
    original_lock = global_user_state._lock_cluster_record_uuid_in_session

    def orchestrated_lock(session: orm.Session, record_uuid: uuid.UUID) -> None:
        if threading.current_thread().name == 'identity-adopter':
            adopter_at_uuid_lock.set()
            assert collider_has_uuid_lock.wait(timeout=5)
        original_lock(session, record_uuid)
        if threading.current_thread().name == 'identity-collider':
            collider_has_uuid_lock.set()

    monkeypatch.setattr(global_user_state,
                        '_lock_cluster_record_uuid_in_session',
                        orchestrated_lock)

    def adopt() -> str:
        threading.current_thread().name = 'identity-adopter'
        return global_user_state.add_or_update_cluster(
            'committed',
            _MinimalHandle('adopted'),
            requested_resources=set(),
            ready=True,
            cluster_record_uuid=_RECORD_UUID,
        )

    def collide() -> global_user_state.ClusterRecordIdentityWriteOutcome:
        threading.current_thread().name = 'identity-collider'
        assert adopter_at_uuid_lock.wait(timeout=5)
        return _commit_identity(full_state_database, 'different-name',
                                _RECORD_UUID)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        adopter = executor.submit(adopt)
        assert adopter_at_uuid_lock.wait(timeout=5)
        collider = executor.submit(collide)
        with pytest.raises(global_user_state.ClusterRecordIdentityConflictError,
                           match='already committed'):
            collider.result(timeout=10)
        assert adopter.result(timeout=10)
    assert _identity_rows(full_state_database) == [('committed', _RECORD_UUID)]


def test_migration_028_postgres_is_native_partial_unique_and_no_backfill(
        postgres_engine) -> None:
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
        connection.exec_driver_sql(
            'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
        connection.exec_driver_sql(
            "INSERT INTO clusters (name) VALUES ('legacy')")
    migration_028 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '028_resource_action_cluster_identity')
    _migration_call(postgres_engine, migration_028.upgrade)
    _migration_call(postgres_engine, migration_028.upgrade)

    columns = {
        column['name']: column for column in sqlalchemy.inspect(
            postgres_engine).get_columns('clusters')
    }
    assert str(columns['cluster_record_uuid']['type']) == 'UUID'
    assert columns['cluster_record_uuid']['nullable']
    with postgres_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.text('SELECT cluster_record_uuid FROM clusters '
                            "WHERE name = 'legacy'")).scalar_one() is None
        index_definition = connection.execute(
            sqlalchemy.text("""
                SELECT pg_get_indexdef(indexrelid)
                FROM pg_index
                WHERE indexrelid = CAST(:index_name AS regclass)
            """), {
                'index_name': _INDEX
            }).scalar_one()
    assert 'CREATE UNIQUE INDEX' in index_definition
    assert '(cluster_record_uuid)' in index_definition
    assert '(cluster_record_uuid IS NOT NULL)' in index_definition

    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('UPDATE clusters SET cluster_record_uuid = '
                            ':record_uuid WHERE name = :name'), {
                                'record_uuid': _RECORD_UUID,
                                'name': 'legacy',
                            })
    with pytest.raises(RuntimeError, match='every cluster-record UUID'):
        _migration_call(postgres_engine, migration_028.downgrade)
    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('UPDATE clusters SET cluster_record_uuid = NULL'))
    _migration_call(postgres_engine, migration_028.downgrade)
    assert {
        column['name'] for column in sqlalchemy.inspect(
            postgres_engine).get_columns('clusters')
    } == {'name'}


def test_migration_028_rejects_malformed_reserved_postgres_index(
        postgres_engine) -> None:
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
        connection.exec_driver_sql(
            'CREATE TABLE clusters (name TEXT PRIMARY KEY, '
            'cluster_record_uuid UUID)')
        connection.exec_driver_sql(
            f'CREATE INDEX {_INDEX} ON clusters (cluster_record_uuid)')
    migration_028 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '028_resource_action_cluster_identity')
    with pytest.raises(RuntimeError, match='unexpected shape'):
        _migration_call(postgres_engine, migration_028.upgrade)


def test_migration_028_rejects_generated_uuid_column(postgres_engine) -> None:
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
        connection.exec_driver_sql(
            'CREATE TABLE clusters ('
            'name TEXT PRIMARY KEY, '
            'cluster_record_uuid UUID GENERATED ALWAYS AS '
            f"('{_RECORD_UUID}'::uuid) STORED)")
    migration_028 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '028_resource_action_cluster_identity')
    with pytest.raises(RuntimeError, match='incompatible'):
        _migration_call(postgres_engine, migration_028.upgrade)


def test_migration_028_downgrade_blocks_raced_identity_write(
        postgres_engine, monkeypatch) -> None:
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
        connection.exec_driver_sql(
            'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
        connection.exec_driver_sql(
            "INSERT INTO clusters (name) VALUES ('raced')")
    migration_028 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '028_resource_action_cluster_identity')
    _migration_call(postgres_engine, migration_028.upgrade)
    reached_drop = threading.Event()
    allow_drop = threading.Event()
    original_drop = migration_028.db_utils.drop_column_from_table_alembic

    def paused_drop(*args, **kwargs) -> None:
        reached_drop.set()
        assert allow_drop.wait(timeout=5)
        original_drop(*args, **kwargs)

    monkeypatch.setattr(migration_028.db_utils,
                        'drop_column_from_table_alembic', paused_drop)

    def write_identity() -> None:
        with postgres_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text(
                    'UPDATE clusters SET cluster_record_uuid = :record_uuid '
                    'WHERE name = :name'), {
                        'record_uuid': _RECORD_UUID,
                        'name': 'raced',
                    })

    writer_was_blocked = False
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        downgrade = executor.submit(_migration_call, postgres_engine,
                                    migration_028.downgrade)
        assert reached_drop.wait(timeout=5)
        writer = executor.submit(write_identity)
        try:
            writer.result(timeout=0.5)
        except concurrent.futures.TimeoutError:
            writer_was_blocked = True
        finally:
            allow_drop.set()
        downgrade.result(timeout=10)
        if writer_was_blocked:
            with pytest.raises(sqlalchemy.exc.DBAPIError):
                writer.result(timeout=10)
    assert writer_was_blocked


def test_global_state_bootstrap_chain_converges_to_revision_028(
        postgres_engine) -> None:
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(
        postgres_engine,
        migration_utils.GLOBAL_USER_STATE_DB_NAME,
        migration_utils.GLOBAL_USER_STATE_VERSION,
        mode='bootstrap',
    )
    assert migration_utils.get_current_alembic_revision(
        postgres_engine, migration_utils.GLOBAL_USER_STATE_DB_NAME) == '028'
    columns = {
        column['name']: column for column in sqlalchemy.inspect(
            postgres_engine).get_columns('clusters')
    }
    assert str(columns['cluster_record_uuid']['type']) == 'UUID'
    assert columns['cluster_record_uuid']['nullable']
    indexes = {
        index['name']: index
        for index in sqlalchemy.inspect(postgres_engine).get_indexes('clusters')
    }
    assert indexes[_INDEX]['unique']
