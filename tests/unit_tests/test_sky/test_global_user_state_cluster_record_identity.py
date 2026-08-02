"""SQLite and metadata contracts for cluster-record identity revision 028."""
# pylint: disable=protected-access,redefined-outer-name

import importlib
import uuid

from alembic import migration
from alembic import operations
import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky import global_user_state
from sky.utils.db import migration_utils

_RECORD_UUID = uuid.UUID('11111111-1111-4111-8111-111111111111')


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


@pytest.fixture
def sqlite_state(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "state.db"}')
    global_user_state.create_table(engine)
    monkeypatch.setattr(global_user_state, '_db_manager',
                        _EngineManager(engine))
    try:
        yield engine
    finally:
        engine.dispose()


def test_cluster_record_uuid_metadata_is_portable_and_partial_unique() -> None:
    column_type = global_user_state.cluster_table.c.cluster_record_uuid.type
    assert isinstance(column_type, sqlalchemy.Uuid)
    assert column_type.compile(dialect=postgresql.dialect()) == 'UUID'
    assert column_type.compile(dialect=sqlite.dialect()) == 'CHAR(32)'
    index = next(index for index in global_user_state.cluster_table.indexes
                 if index.name == 'uq_clusters_cluster_record_uuid_nonnull')
    assert index.unique
    assert list(index.columns) == [
        global_user_state.cluster_table.c.cluster_record_uuid
    ]
    assert str(index.dialect_options['postgresql']['where']) == (
        'clusters.cluster_record_uuid IS NOT NULL')
    assert str(index.dialect_options['sqlite']['where']) == (
        'clusters.cluster_record_uuid IS NOT NULL')
    assert migration_utils.GLOBAL_USER_STATE_VERSION == '028'


def test_sqlite_catalog_is_inert_and_ordinary_updates_preserve_identity(
        sqlite_state) -> None:
    name = 'identity-preserved'
    global_user_state.add_or_update_cluster(
        name,
        _MinimalHandle('initial'),
        requested_resources=set(),
        ready=False,
    )
    with sqlite_state.begin() as connection:
        initial = connection.execute(
            sqlalchemy.select(
                global_user_state.cluster_table.c.cluster_record_uuid).
            where(global_user_state.cluster_table.c.name == name)).scalar_one()
        assert initial is None
        connection.execute(
            sqlalchemy.update(global_user_state.cluster_table).where(
                global_user_state.cluster_table.c.name == name).values(
                    cluster_record_uuid=_RECORD_UUID))

    global_user_state.add_or_update_cluster(
        name,
        _MinimalHandle('updated'),
        requested_resources=set(),
        ready=True,
    )
    with sqlite_state.connect() as connection:
        retained = connection.execute(
            sqlalchemy.select(
                global_user_state.cluster_table.c.cluster_record_uuid).
            where(global_user_state.cluster_table.c.name == name)).scalar_one()
    assert retained == _RECORD_UUID

    with pytest.raises(RuntimeError, match='requires.*PostgreSQL'):
        global_user_state.add_or_update_cluster(
            'sqlite-action-aware',
            _MinimalHandle('action-aware'),
            requested_resources=set(),
            ready=False,
            cluster_record_uuid=_RECORD_UUID,
        )
    with sqlite_state.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(global_user_state.cluster_table.c.name).where(
                global_user_state.cluster_table.c.name ==
                'sqlite-action-aware')).first() is None

    with pytest.raises(RuntimeError, match='requires.*PostgreSQL'):
        global_user_state.remove_cluster(
            name,
            terminate=True,
            expected_cluster_record_uuid=_RECORD_UUID,
            expected_cluster_handle=_MinimalHandle('updated'),
        )


def test_sqlite_partial_unique_index_rejects_duplicate_nonnull_identity(
        sqlite_state) -> None:
    table = global_user_state.cluster_table
    with sqlite_state.begin() as connection:
        connection.execute(table.insert().values(name='first'))
        connection.execute(table.insert().values(name='second'))
        connection.execute(table.insert().values(name='third'))
        connection.execute(table.update().where(table.c.name == 'first').values(
            cluster_record_uuid=_RECORD_UUID))
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            connection.execute(
                table.update().where(table.c.name == 'second').values(
                    cluster_record_uuid=_RECORD_UUID))


def test_migration_028_sqlite_reruns_without_backfill_and_downgrades_empty_only(
) -> None:
    engine = sqlalchemy.create_engine('sqlite://')
    migration_028 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '028_resource_action_cluster_identity')
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
            connection.exec_driver_sql(
                "INSERT INTO clusters (name) VALUES ('legacy')")
        _migration_call(engine, migration_028.upgrade)
        _migration_call(engine, migration_028.upgrade)
        columns = {
            column['name']: column
            for column in sqlalchemy.inspect(engine).get_columns('clusters')
        }
        assert str(columns['cluster_record_uuid']['type']) == 'CHAR(32)'
        assert columns['cluster_record_uuid']['nullable']
        with engine.connect() as connection:
            assert connection.execute(
                sqlalchemy.text('SELECT cluster_record_uuid FROM clusters '
                                "WHERE name = 'legacy'")).scalar_one() is None
        indexes = {
            index['name']: index
            for index in sqlalchemy.inspect(engine).get_indexes('clusters')
        }
        index = indexes['uq_clusters_cluster_record_uuid_nonnull']
        assert index['unique']
        assert index['column_names'] == ['cluster_record_uuid']

        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('UPDATE clusters SET cluster_record_uuid = '
                                ':record_uuid WHERE name = :name'), {
                                    'record_uuid': _RECORD_UUID.hex,
                                    'name': 'legacy',
                                })
        with pytest.raises(RuntimeError, match='every cluster-record UUID'):
            _migration_call(engine, migration_028.downgrade)
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('UPDATE clusters SET cluster_record_uuid = '
                                'NULL'))
        _migration_call(engine, migration_028.downgrade)
        assert {
            column['name']
            for column in sqlalchemy.inspect(engine).get_columns('clusters')
        } == {'name'}
    finally:
        engine.dispose()


def test_migration_028_rejects_incompatible_adopted_column() -> None:
    engine = sqlalchemy.create_engine('sqlite://')
    migration_028 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '028_resource_action_cluster_identity')
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TABLE clusters (name TEXT PRIMARY KEY, '
                'cluster_record_uuid TEXT)')
        with pytest.raises(RuntimeError, match='incompatible'):
            _migration_call(engine, migration_028.upgrade)
    finally:
        engine.dispose()
