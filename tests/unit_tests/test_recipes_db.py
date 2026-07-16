"""Persistence observability tests for the Recipes store."""
# pylint: disable=protected-access,redefined-outer-name,unused-argument
from unittest import mock

import pytest
import sqlalchemy

from sky.recipes import db as recipes_db


@pytest.fixture()
def isolated_recipes_database(tmp_path, monkeypatch):
    """Create an isolated SQLite Recipes database."""
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "recipes.db"}')
    monkeypatch.setattr(recipes_db._db_manager, '_engine', engine)
    recipes_db._create_table(engine)
    recipes_db._insert_default_templates(engine)
    recipes_db._legacy_sqlite_marker_emitted = False
    try:
        yield
    finally:
        recipes_db._legacy_sqlite_marker_emitted = False
        engine.dispose()


def test_sqlite_use_emits_one_structured_marker_and_counts_operations(
        isolated_recipes_database, monkeypatch):
    warning = mock.Mock()
    record = mock.Mock()
    monkeypatch.setattr(recipes_db.logger, 'warning', warning)
    monkeypatch.setattr(recipes_db.metrics_lib, 'METRICS_ENABLED', True)
    monkeypatch.setattr(recipes_db.metrics_lib, 'record_persistence_operation',
                        record)

    assert recipes_db.get_recipe('non-sensitive-recipe-name') is None
    recipes_db.list_recipes(pinned_only=True)

    assert record.call_args_list == [
        mock.call('recipes', 'get', 'read', 'sqlite'),
        mock.call('recipes', 'list', 'read', 'sqlite'),
    ]
    warning.assert_called_once()
    marker_format, *marker_args = warning.call_args.args
    assert marker_format.startswith('event_name=%s component=%s')
    assert marker_args[:5] == [
        recipes_db._LEGACY_BACKEND_EVENT, 'recipes', 'get', 'read', 'sqlite'
    ]
    assert 'non-sensitive-recipe-name' not in warning.call_args.args
