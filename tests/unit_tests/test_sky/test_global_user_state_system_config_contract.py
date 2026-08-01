"""Characterize the global-user-state system-configuration facade."""

import inspect
from unittest import mock

from sqlalchemy import event
from sqlalchemy import text

from sky import global_user_state
from sky.skylet import constants
from sky.utils.db import db_utils

_PUBLIC_SIGNATURES = {
    'get_system_config': '(config_key: str) -> str | None',
    'get_or_set_system_config': '(config_key: str, default_value: str) -> str',
    'set_system_config': '(config_key: str, config_value: str) -> None',
}


class _TrackingManager:

    def __init__(self, manager):
        self._manager = manager
        self.get_engine_calls = 0

    def get_engine(self):
        self.get_engine_calls += 1
        return self._manager.get_engine()


def _wrapper_depth(function):
    depth = 0
    while hasattr(function, '__wrapped__'):
        depth += 1
        function = function.__wrapped__
    return depth


def _fresh_tracking_db(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    manager = db_utils.DatabaseManager(
        'state',
        global_user_state.create_table,
        # pylint: disable=protected-access
        post_init_fn=lambda _: global_user_state._sqlite_supports_returning(),
    )
    engine = manager.get_engine()
    tracking_manager = _TrackingManager(manager)
    monkeypatch.setattr(global_user_state, '_db_manager', tracking_manager)
    return engine, tracking_manager


def test_public_surface_and_decorator_contract():
    for name, expected_signature in _PUBLIC_SIGNATURES.items():
        function = getattr(global_user_state, name)
        assert str(inspect.signature(function)) == expected_signature
        assert function.__module__ == 'sky.global_user_state'
        assert function.__qualname__ == name
        assert _wrapper_depth(function) == 1


def test_facade_preserves_statements_timestamps_and_dependency_paths(
        tmp_path, monkeypatch):
    engine, manager = _fresh_tracking_db(tmp_path, monkeypatch)
    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _record_statement(*args):
        statements.append(args[2])

    insert_calls = 0
    real_insert = global_user_state.sqlite.insert

    def _tracking_insert(*args, **kwargs):
        nonlocal insert_calls
        insert_calls += 1
        return real_insert(*args, **kwargs)

    monkeypatch.setattr(global_user_state.sqlite, 'insert', _tracking_insert)
    timestamps = iter((101, 102, 103))
    monkeypatch.setattr(global_user_state.time, 'time',
                        lambda: next(timestamps))

    assert global_user_state.get_system_config('identity') is None
    assert global_user_state.get_or_set_system_config('identity',
                                                      'first') == 'first'
    assert global_user_state.get_or_set_system_config('identity',
                                                      'second') == 'first'
    global_user_state.set_system_config('identity', 'updated')
    assert global_user_state.get_system_config('identity') == 'updated'

    assert manager.get_engine_calls == 5
    assert insert_calls == 3
    assert len(statements) == 7
    assert sum(statement.lstrip().upper().startswith('SELECT')
               for statement in statements) == 4
    assert sum(statement.lstrip().upper().startswith('INSERT')
               for statement in statements) == 3

    event.remove(engine, 'before_cursor_execute', _record_statement)
    with engine.connect() as connection:
        row = connection.execute(
            text('SELECT config_value, created_at, updated_at '
                 'FROM system_config WHERE config_key = :key'), {
                     'key': 'identity'
                 }).one()
    assert tuple(row) == ('updated', 101, 103)


def test_postgresql_upsert_preserves_facade_dependency_paths(monkeypatch):
    engine = mock.Mock()
    engine.dialect.name = db_utils.SQLAlchemyDialect.POSTGRESQL.value
    manager = mock.Mock()
    manager.get_engine.return_value = engine
    monkeypatch.setattr(global_user_state, '_db_manager', manager)

    sqlite_dialect = mock.Mock()
    postgresql_dialect = mock.Mock()
    monkeypatch.setattr(global_user_state, 'sqlite', sqlite_dialect)
    monkeypatch.setattr(global_user_state, 'postgresql', postgresql_dialect)
    monkeypatch.setattr(global_user_state.time, 'time', lambda: 123)

    session_factory = mock.MagicMock()
    monkeypatch.setattr(global_user_state.orm, 'Session', session_factory)
    session = session_factory.return_value.__enter__.return_value
    insert_stmnt = postgresql_dialect.insert.return_value.values.return_value
    upsert_stmnt = insert_stmnt.on_conflict_do_update.return_value

    global_user_state.set_system_config('identity', 'value')

    manager.get_engine.assert_called_once_with()
    session_factory.assert_called_once_with(engine)
    sqlite_dialect.insert.assert_not_called()
    postgresql_dialect.insert.assert_called_once_with(
        global_user_state.system_config_table)
    postgresql_dialect.insert.return_value.values.assert_called_once_with(
        config_key='identity',
        config_value='value',
        created_at=123,
        updated_at=123)
    insert_stmnt.on_conflict_do_update.assert_called_once_with(
        index_elements=[global_user_state.system_config_table.c.config_key],
        set_={
            global_user_state.system_config_table.c.config_value: 'value',
            global_user_state.system_config_table.c.updated_at: 123,
        })
    session.execute.assert_called_once_with(upsert_stmnt)
    session.commit.assert_called_once_with()
