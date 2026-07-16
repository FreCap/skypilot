"""Real-PostgreSQL parity tests for the Recipes store."""
# pylint: disable=protected-access,redefined-outer-name,unused-argument
import concurrent.futures
import contextlib
import threading
from unittest import mock

import pytest
import sqlalchemy
from testcontainers import postgres as testcontainers_postgres

from sky import exceptions
from sky.recipes import db as recipes_db
from sky.recipes.utils import RecipeType


@pytest.fixture(scope='module')
def postgres_engine():
    container = testcontainers_postgres.PostgresContainer('postgres:16')
    container.start()
    engine = sqlalchemy.create_engine(container.get_connection_url())
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


def _reset_database(engine):
    with engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')


@pytest.fixture
def postgres_database(postgres_engine, monkeypatch):
    _reset_database(postgres_engine)
    monkeypatch.setattr(recipes_db._db_manager, '_engine', postgres_engine)
    recipes_db._create_table(postgres_engine)
    recipes_db._insert_default_templates(postgres_engine)
    yield postgres_engine


@contextlib.contextmanager
def _count_sql_statements(engine):
    count = {'value': 0}

    def _before_cursor_execute(*_args, **_kwargs):
        count['value'] += 1

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _before_cursor_execute)
    try:
        yield count
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _before_cursor_execute)


def test_postgres_all_operations(postgres_database):
    defaults = recipes_db.list_recipes(pinned_only=True)
    assert {recipe.name for recipe in defaults} == {
        metadata['name'] for metadata in recipes_db.DEFAULT_TEMPLATES.values()
    }

    created = recipes_db.create_recipe(
        name='alice-cluster',
        content='resources:\n  cpus: 2',
        recipe_type=RecipeType.CLUSTER,
        user_id='alice',
        user_name='Alice',
        description='initial',
    )
    assert created.user_id == 'alice'
    assert recipes_db.get_recipe('alice-cluster').description == 'initial'

    with pytest.raises(exceptions.RecipeAlreadyExistsError):
        recipes_db.create_recipe(
            name='alice-cluster',
            content='resources:\n  cpus: 4',
            recipe_type=RecipeType.CLUSTER,
            user_id='alice',
        )

    updated = recipes_db.update_recipe(
        recipe_name='alice-cluster',
        user_id='bob',
        user_name='Bob',
        description='updated',
    )
    assert updated.description == 'updated'
    assert updated.updated_by_id == 'bob'

    mine = recipes_db.list_recipes(user_id='alice', my_recipes_only=True)
    assert [recipe.name for recipe in mine] == ['alice-cluster']
    typed = recipes_db.list_recipes(recipe_type=RecipeType.CLUSTER)
    assert 'alice-cluster' in {recipe.name for recipe in typed}

    pinned = recipes_db.toggle_pin('alice-cluster', True)
    assert pinned.pinned
    assert 'alice-cluster' in {
        recipe.name for recipe in recipes_db.list_recipes(pinned_only=True)
    }

    assert not recipes_db.delete_recipe('alice-cluster', user_id='bob')
    assert recipes_db.delete_recipe('alice-cluster', user_id='alice')
    assert recipes_db.get_recipe('alice-cluster') is None

    with pytest.raises(ValueError, match='not editable'):
        recipes_db.update_recipe('basic-cluster',
                                 user_id='alice',
                                 description='changed')
    with pytest.raises(ValueError, match='cannot be deleted'):
        recipes_db.delete_recipe('basic-cluster', user_id='system')


def test_postgres_operations_are_counted_without_legacy_marker(
        postgres_database, monkeypatch):
    record = mock.Mock()
    marker = mock.Mock()
    monkeypatch.setattr(recipes_db.metrics_lib, 'METRICS_ENABLED', True)
    monkeypatch.setattr(recipes_db.metrics_lib, 'record_persistence_operation',
                        record)
    monkeypatch.setattr(recipes_db, '_emit_legacy_sqlite_marker', marker)

    assert recipes_db.get_recipe('basic-cluster') is not None
    recipes_db.list_recipes(pinned_only=True)

    assert record.call_args_list == [
        mock.call('recipes', 'get', 'read', 'postgresql'),
        mock.call('recipes', 'list', 'read', 'postgresql'),
    ]
    marker.assert_not_called()


def test_postgres_duplicate_create_is_atomic(postgres_database):
    barrier = threading.Barrier(8)

    def _create():
        barrier.wait(timeout=10)
        try:
            recipes_db.create_recipe(
                name='concurrent-create',
                content='resources:\n  cpus: 1',
                recipe_type=RecipeType.CLUSTER,
                user_id='alice',
            )
            return 'created'
        except exceptions.RecipeAlreadyExistsError:
            return 'duplicate'

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: _create(), range(8)))

    assert results.count('created') == 1
    assert results.count('duplicate') == 7
    assert recipes_db.get_recipe('concurrent-create') is not None


def test_postgres_default_seeding_survives_concurrent_startup(
        postgres_engine, monkeypatch):
    _reset_database(postgres_engine)
    recipes_db._create_table(postgres_engine)
    barrier = threading.Barrier(4)
    thread_state = threading.local()
    original_load = recipes_db._load_example_content

    def _load_after_all_replicas_count_empty(filename):
        if not getattr(thread_state, 'synchronized', False):
            thread_state.synchronized = True
            barrier.wait(timeout=10)
        return original_load(filename)

    monkeypatch.setattr(recipes_db, '_load_example_content',
                        _load_after_all_replicas_count_empty)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(recipes_db._insert_default_templates,
                            postgres_engine) for _ in range(4)
        ]
        for future in futures:
            future.result(timeout=20)

    with postgres_engine.connect() as connection:
        names = set(
            connection.execute(
                sqlalchemy.select(recipes_db.recipes_table.c.name)).scalars())
    assert names == {
        metadata['name'] for metadata in recipes_db.DEFAULT_TEMPLATES.values()
    }


def test_postgres_default_seeding_does_not_hide_other_integrity_errors(
        postgres_engine, monkeypatch):
    _reset_database(postgres_engine)
    recipes_db._create_table(postgres_engine)
    monkeypatch.setattr(recipes_db, '_load_example_content', lambda _: None)

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        recipes_db._insert_default_templates(postgres_engine)


def test_postgres_default_seeding_rejects_non_system_name_collision(
        postgres_engine, monkeypatch):
    _reset_database(postgres_engine)
    recipes_db._create_table(postgres_engine)
    original_load = recipes_db._load_example_content
    collision_inserted = False

    def _load_after_user_collision(filename):
        nonlocal collision_inserted
        if not collision_inserted:
            collision_inserted = True
            with postgres_engine.begin() as connection:
                connection.execute(recipes_db.recipes_table.insert().values(
                    name='basic-cluster',
                    content='resources:\n  cpus: 1',
                    recipe_type='cluster',
                    pinned=0,
                    user_id='user',
                    is_editable=1,
                    is_pinnable=1,
                ))
        return original_load(filename)

    monkeypatch.setattr(recipes_db, '_load_example_content',
                        _load_after_user_collision)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        recipes_db._insert_default_templates(postgres_engine)


def test_postgres_migration_is_idempotent_and_indexes_are_used(
        postgres_database):
    recipes_db._create_table(postgres_database)
    recipes_db._create_table(postgres_database)

    inspector = sqlalchemy.inspect(postgres_database)
    indexes = {index['name'] for index in inspector.get_indexes('recipes')}
    assert indexes == {
        'idx_recipe_pinned', 'idx_recipe_type', 'idx_recipe_user_id'
    }

    with postgres_database.connect() as connection:
        connection.exec_driver_sql('SET enable_seqscan = off')
        plan = '\n'.join(row[0] for row in connection.exec_driver_sql(
            "EXPLAIN SELECT * FROM recipes WHERE name = 'basic-cluster'"))
    assert 'recipes_pkey' in plan


def test_postgres_get_is_one_statement(postgres_database):
    with _count_sql_statements(postgres_database) as count:
        recipe = recipes_db.get_recipe('basic-cluster')

    assert recipe is not None
    assert count['value'] == 1


def test_postgres_recipes_survive_engine_restart(postgres_database,
                                                 monkeypatch):
    recipes_db.create_recipe(
        name='restart-recipe',
        content='resources:\n  cpus: 2',
        recipe_type=RecipeType.CLUSTER,
        user_id='alice',
    )
    restarted_engine = sqlalchemy.create_engine(postgres_database.url)
    monkeypatch.setattr(recipes_db._db_manager, '_engine', restarted_engine)
    try:
        assert recipes_db.get_recipe('restart-recipe') is not None
    finally:
        restarted_engine.dispose()
