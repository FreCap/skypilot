"""Characterize the global-user-state user repository facade."""

import contextlib
import inspect
import types
from unittest import mock

import pytest
from sqlalchemy import event

from sky import global_user_state
from sky import models
from sky.skylet import constants
from sky.utils.db import db_utils

_PUBLIC_PARAMETERS = {
    'add_or_update_user': ['user', 'allow_duplicate_name', 'return_user'],
    'get_user': ['user_id', 'session'],
    'get_users': ['user_ids'],
    'get_user_by_name': ['username'],
    'get_user_by_name_match': ['username_match'],
    'delete_user': ['user_id'],
    'get_all_users': [],
    'set_user_preferred_workspace': ['user_id', 'workspace'],
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
    for name, expected_parameters in _PUBLIC_PARAMETERS.items():
        function = getattr(global_user_state, name)
        assert list(
            inspect.signature(function).parameters) == expected_parameters
        assert function.__module__ == 'sky.global_user_state'
        assert function.__qualname__ == name
        expected_depth = 2 if name == 'set_user_preferred_workspace' else 1
        assert _wrapper_depth(function) == expected_depth

    add_signature = inspect.signature(global_user_state.add_or_update_user)
    assert add_signature.parameters['allow_duplicate_name'].default is True
    assert add_signature.parameters['return_user'].default is False
    assert inspect.signature(
        global_user_state.get_user).parameters['session'].default is None
    assert inspect.signature(
        global_user_state.set_user_preferred_workspace
    ).parameters['workspace'].default is inspect.Signature.empty


def test_sqlite_lifecycle_preserves_projections_sessions_and_counts(
        tmp_path, monkeypatch):
    engine, manager = _fresh_tracking_db(tmp_path, monkeypatch)
    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _record_statement(*args):
        statements.append(args[2])

    monkeypatch.setattr(global_user_state.time, 'time', lambda: 101)
    returning = mock.Mock(return_value=False)
    monkeypatch.setattr(global_user_state, '_sqlite_supports_returning',
                        returning)

    unnamed = models.User(id='unnamed')
    assert global_user_state.add_or_update_user(unnamed) is False
    assert global_user_state.add_or_update_user(unnamed,
                                                return_user=True) == (False,
                                                                      unnamed)
    assert manager.get_engine_calls == 0

    user = models.User(id='User-A',
                       name='Alice',
                       password='secret',
                       user_type=models.UserType.BASIC.value)
    was_inserted, stored = global_user_state.add_or_update_user(
        user, return_user=True)
    assert was_inserted is True
    assert stored == models.User(id='user-a',
                                 name='Alice',
                                 password='secret',
                                 created_at=101,
                                 user_type=models.UserType.BASIC.value)
    returning.assert_called_once_with()

    point = global_user_state.get_user('user-a')
    batch = global_user_state.get_users({'user-a', 'missing'})
    exact = global_user_state.get_user_by_name('Alice')
    partial = global_user_state.get_user_by_name_match('lic')
    all_users = global_user_state.get_all_users()
    assert point == stored
    assert batch == {'user-a': stored}
    assert exact == [stored]
    assert all_users == [stored]
    assert partial == [
        models.User(id='user-a',
                    name='Alice',
                    created_at=101,
                    user_type=models.UserType.BASIC.value)
    ]
    assert partial[0].password is None

    assert global_user_state.set_user_preferred_workspace('user-a',
                                                          'team-a') is True
    manager_calls_before_reuse = manager.get_engine_calls
    with global_user_state.orm.Session(engine) as session:
        reused = global_user_state.get_user('user-a', session=session)
    assert reused is not None
    assert reused.preferred_workspace == 'team-a'
    assert manager.get_engine_calls == manager_calls_before_reuse

    global_user_state.delete_user('user-a')
    assert global_user_state.get_user('user-a') is None
    assert global_user_state.set_user_preferred_workspace('missing',
                                                          None) is False

    assert manager.get_engine_calls == 10
    assert sum(statement.lstrip().upper().startswith('INSERT')
               for statement in statements) == 1
    assert sum(statement.lstrip().upper().startswith('SELECT')
               for statement in statements) == 8
    assert sum(statement.lstrip().upper().startswith('UPDATE')
               for statement in statements) == 2
    assert sum(statement.lstrip().upper().startswith('DELETE')
               for statement in statements) == 1


def test_update_duplicate_name_and_return_shapes(tmp_path, monkeypatch):
    _, manager = _fresh_tracking_db(tmp_path, monkeypatch)
    timestamps = iter((101, 202, 303))
    monkeypatch.setattr(global_user_state.time, 'time',
                        lambda: next(timestamps))
    returning = mock.Mock(return_value=False)
    monkeypatch.setattr(global_user_state, '_sqlite_supports_returning',
                        returning)

    original = models.User(id='user-a',
                           name='Alice',
                           password='secret',
                           user_type=models.UserType.BASIC.value)
    assert global_user_state.add_or_update_user(original) is True
    was_inserted, updated = global_user_state.add_or_update_user(
        models.User(id='user-a', name='Alicia'), return_user=True)
    assert was_inserted is False
    assert updated == models.User(id='user-a',
                                  name='Alicia',
                                  password='secret',
                                  created_at=101,
                                  user_type=models.UserType.BASIC.value)

    duplicate = models.User(id='user-b', name='Alicia')
    assert global_user_state.add_or_update_user(duplicate,
                                                allow_duplicate_name=False,
                                                return_user=True) == (False,
                                                                      duplicate)
    assert global_user_state.get_user('user-b') is None
    assert returning.call_count == 1
    assert manager.get_engine_calls == 4


def test_get_user_projects_after_shared_session_scope_exits(monkeypatch):
    events = []
    session = mock.Mock()
    session.query.return_value.filter_by.return_value.first.return_value = (
        types.SimpleNamespace(id='user-a',
                              name='Alice',
                              password='secret',
                              created_at=101,
                              type=models.UserType.BASIC.value,
                              preferred_workspace='team-a'))

    @contextlib.contextmanager
    def _tracking_scope(passed_session=None):
        assert passed_session is session
        events.append('enter')
        yield session
        events.append('exit')

    real_user = models.User

    def _tracking_user(**kwargs):
        assert events == ['enter', 'exit']
        return real_user(**kwargs)

    monkeypatch.setattr(global_user_state, '_session_scope', _tracking_scope)
    monkeypatch.setattr(global_user_state.models, 'User', _tracking_user)

    user = global_user_state.get_user('user-a', session=session)
    assert user == real_user(id='user-a',
                             name='Alice',
                             password='secret',
                             created_at=101,
                             user_type=models.UserType.BASIC.value,
                             preferred_workspace='team-a')
    assert events == ['enter', 'exit']


def test_postgresql_dependencies_remain_late_bound(monkeypatch):
    engine = mock.Mock()
    engine.dialect.name = db_utils.SQLAlchemyDialect.POSTGRESQL.value
    manager = mock.Mock()
    manager.get_engine.return_value = engine
    monkeypatch.setattr(global_user_state, '_db_manager', manager)
    monkeypatch.setattr(global_user_state.time, 'time', lambda: 101)

    sqlite_dialect = mock.Mock()
    postgresql_dialect = mock.Mock()
    monkeypatch.setattr(global_user_state, 'sqlite', sqlite_dialect)
    monkeypatch.setattr(global_user_state, 'postgresql', postgresql_dialect)
    session_factory = mock.MagicMock()
    monkeypatch.setattr(global_user_state.orm, 'Session', session_factory)
    session = session_factory.return_value.__enter__.return_value

    insert_statement = postgresql_dialect.insert.return_value.values.return_value
    update_statement = insert_statement.on_conflict_do_update.return_value
    returning_statement = update_statement.returning.return_value
    row = types.SimpleNamespace(id='user-a',
                                name='Alice',
                                password='secret',
                                created_at=101,
                                type=models.UserType.BASIC.value,
                                preferred_workspace='team-a',
                                was_inserted=True)
    session.execute.return_value.fetchone.return_value = row
    user = models.User(id='user-a',
                       name='Alice',
                       password='secret',
                       user_type=models.UserType.BASIC.value)

    assert global_user_state.add_or_update_user(
        user,
        return_user=True) == (True,
                              models.User(id='user-a',
                                          name='Alice',
                                          password='secret',
                                          created_at=101,
                                          user_type=models.UserType.BASIC.value,
                                          preferred_workspace='team-a'))

    manager.get_engine.assert_called_once_with()
    session_factory.assert_called_once_with(engine)
    sqlite_dialect.insert.assert_not_called()
    postgresql_dialect.insert.assert_called_once_with(
        global_user_state.user_table)
    values = postgresql_dialect.insert.return_value.values.call_args.kwargs
    assert values == {
        'id': 'user-a',
        'name': 'Alice',
        'password': 'secret',
        'created_at': 101,
        'type': models.UserType.BASIC.value,
    }
    insert_statement.on_conflict_do_update.assert_called_once()
    session.execute.assert_called_once_with(returning_statement)
    session.commit.assert_called_once_with()


def test_unsupported_dialect_contract(monkeypatch):
    engine = mock.Mock()
    engine.dialect.name = 'unsupported'
    manager = mock.Mock()
    manager.get_engine.return_value = engine
    monkeypatch.setattr(global_user_state, '_db_manager', manager)
    monkeypatch.setattr(global_user_state.time, 'time', lambda: 101)

    with mock.patch.object(global_user_state.orm, 'Session'):
        with pytest.raises(ValueError, match='Unsupported database dialect'):
            global_user_state.add_or_update_user(
                models.User(id='user-a', name='Alice'))
