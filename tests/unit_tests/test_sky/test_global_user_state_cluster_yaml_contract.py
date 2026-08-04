"""Characterize the global-user-state cluster YAML facade."""

import inspect
from unittest import mock

from sqlalchemy import event
from sqlalchemy import insert

from sky import global_user_state
from sky.skylet import constants
from sky.utils.db import db_utils

_PUBLIC_DECORATOR_DEPTHS = {
    'get_cluster_yaml_str': 2,
    'get_cluster_yaml_str_multiple': 0,
    'get_cluster_yaml_dict': 0,
    'get_cluster_yaml_dict_multiple': 0,
    'set_cluster_yaml': 1,
    'remove_cluster_yaml': 1,
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
    expected_parameters = {
        'get_cluster_yaml_str': ['cluster_yaml_path'],
        'get_cluster_yaml_str_multiple': ['cluster_yaml_paths'],
        'get_cluster_yaml_dict': ['cluster_yaml_path'],
        'get_cluster_yaml_dict_multiple': ['cluster_yaml_paths'],
        'set_cluster_yaml': ['cluster_name', 'yaml_str'],
        'remove_cluster_yaml': ['cluster_name'],
    }
    for name, expected_depth in _PUBLIC_DECORATOR_DEPTHS.items():
        function = getattr(global_user_state, name)
        assert list(inspect.signature(function).parameters) == (
            expected_parameters[name])
        assert function.__module__ == 'sky.global_user_state'
        assert function.__qualname__ == name
        assert _wrapper_depth(function) == expected_depth


def test_facade_preserves_database_operations_and_batch_cardinality(
        tmp_path, monkeypatch):
    engine, manager = _fresh_tracking_db(tmp_path, monkeypatch)
    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _record_statement(*args):
        statements.append(args[2])

    assert not global_user_state.get_cluster_yaml_str_multiple([])
    assert manager.get_engine_calls == 0
    assert not statements

    global_user_state.set_cluster_yaml('alpha', 'value: alpha\n')
    global_user_state.set_cluster_yaml('beta', 'value: beta\n')
    assert manager.get_engine_calls == 4
    assert len(statements) == 2
    assert all(statement.lstrip().upper().startswith('INSERT')
               for statement in statements)

    statements.clear()
    assert global_user_state.get_cluster_yaml_str_multiple([
        '/one/alpha.yml', '/two/beta.yml', '/three/alpha.yml'
    ]) == ['value: alpha\n', 'value: beta\n', 'value: alpha\n']
    assert manager.get_engine_calls == 5
    assert len(statements) == 1
    assert statements[0].lstrip().upper().startswith('SELECT')

    statements.clear()
    global_user_state.remove_cluster_yaml('beta')
    assert manager.get_engine_calls == 6
    assert len(statements) == 1
    assert statements[0].lstrip().upper().startswith('DELETE')


def test_file_fallback_preserves_debug_precedence_and_facade_setter(
        tmp_path, monkeypatch):
    exact_path = tmp_path / 'legacy.yml'
    debug_path = tmp_path / 'legacy.yml.debug'
    exact_path.write_text('source: exact\n', encoding='utf-8')
    debug_path.write_text('source: debug\n', encoding='utf-8')
    setter = mock.Mock()
    monkeypatch.setattr(global_user_state, 'set_cluster_yaml', setter)

    # The historical implementation checks both paths, so `.debug` wins when
    # both files exist.
    result = global_user_state._set_cluster_yaml_from_file(  # pylint: disable=protected-access
        str(exact_path), 'legacy')

    assert result == 'source: debug\n'
    setter.assert_called_once_with('legacy', 'source: debug\n')


def test_missing_row_uses_late_bound_file_fallback(tmp_path, monkeypatch):
    _, _ = _fresh_tracking_db(tmp_path, monkeypatch)
    fallback = mock.Mock(return_value='source: fallback\n')
    monkeypatch.setattr(global_user_state, '_set_cluster_yaml_from_file',
                        fallback)

    assert global_user_state.get_cluster_yaml_str(
        '/tmp/missing.yml') == 'source: fallback\n'
    fallback.assert_called_once_with('/tmp/missing.yml', 'missing')


def test_present_null_row_does_not_trigger_file_fallback(tmp_path, monkeypatch):
    engine, _ = _fresh_tracking_db(tmp_path, monkeypatch)
    with engine.begin() as connection:
        connection.execute(
            insert(global_user_state.cluster_yaml_table).values(
                cluster_name='present-null', yaml=None))
    fallback = mock.Mock(return_value='fallback')
    monkeypatch.setattr(global_user_state, '_set_cluster_yaml_from_file',
                        fallback)

    assert global_user_state.get_cluster_yaml_str(
        '/tmp/present-null.yml') is None
    fallback.assert_not_called()


def test_yaml_projection_preserves_late_bound_facade_calls(monkeypatch):
    single_loader = mock.Mock(return_value='provider:\n  type: kubernetes\n')
    batch_loader = mock.Mock(return_value=['name: first\n', 'name: second\n'])
    monkeypatch.setattr(global_user_state, 'get_cluster_yaml_str',
                        single_loader)
    monkeypatch.setattr(global_user_state, 'get_cluster_yaml_str_multiple',
                        batch_loader)

    assert global_user_state.get_cluster_yaml_dict('/tmp/cluster.yml') == {
        'provider': {
            'type': 'kubernetes'
        }
    }
    assert global_user_state.get_cluster_yaml_dict_multiple(
        ['/tmp/first.yml', '/tmp/second.yml']) == [{
            'name': 'first'
        }, {
            'name': 'second'
        }]
    single_loader.assert_called_once_with('/tmp/cluster.yml')
    batch_loader.assert_called_once_with(['/tmp/first.yml', '/tmp/second.yml'])


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

    session_factory = mock.MagicMock()
    monkeypatch.setattr(global_user_state.orm, 'Session', session_factory)
    session = session_factory.return_value.__enter__.return_value
    insert_stmnt = postgresql_dialect.insert.return_value.values.return_value
    upsert_stmnt = insert_stmnt.on_conflict_do_update.return_value

    global_user_state.set_cluster_yaml('cluster', 'provider: {}\n')

    assert manager.get_engine.call_count == 2
    session_factory.assert_called_once_with(engine)
    sqlite_dialect.insert.assert_not_called()
    postgresql_dialect.insert.assert_called_once_with(
        global_user_state.cluster_yaml_table)
    insert_stmnt.on_conflict_do_update.assert_called_once_with(
        index_elements=[global_user_state.cluster_yaml_table.c.cluster_name],
        set_={global_user_state.cluster_yaml_table.c.yaml: 'provider: {}\n'})
    session.execute.assert_called_once_with(upsert_stmnt)
    session.commit.assert_called_once_with()
