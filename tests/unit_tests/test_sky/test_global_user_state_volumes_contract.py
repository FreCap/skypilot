"""Characterize the global-user-state volume repository facade."""

import inspect
from unittest import mock

import pytest
from sqlalchemy import event

from sky import global_user_state
from sky import models
from sky.skylet import constants
from sky.utils import status_lib
from sky.utils.db import db_utils

_PUBLIC_PARAMETERS = {
    'get_volume_names_start_with': ['starts_with'],
    'get_volumes': ['is_ephemeral', 'name'],
    'get_volume_configs_by_names': ['names'],
    'get_volume_by_name': ['name'],
    'add_volume': ['name', 'config', 'status', 'is_ephemeral', 'creation_yaml'],
    'update_volume_config': ['name', 'config'],
    'update_volume': ['name', 'last_attached_at', 'status'],
    'update_volume_status': [
        'name', 'status', 'error_message', 'usedby_pods', 'usedby_clusters'
    ],
    'delete_volume': ['name'],
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


def _volume_config(name, name_on_cloud):
    return models.VolumeConfig(
        name=name,
        cloud='kubernetes',
        type='k8s-pvc',
        region='context',
        zone=None,
        size='100Gi',
        config={'namespace': 'default'},
        name_on_cloud=name_on_cloud,
    )


def test_public_surface_and_decorator_contract():
    for name, expected_parameters in _PUBLIC_PARAMETERS.items():
        function = getattr(global_user_state, name)
        assert list(
            inspect.signature(function).parameters) == expected_parameters
        assert function.__module__ == 'sky.global_user_state'
        assert function.__qualname__ == name
        assert _wrapper_depth(function) == 1

    add_signature = inspect.signature(global_user_state.add_volume)
    assert add_signature.parameters['is_ephemeral'].default is False
    assert add_signature.parameters['creation_yaml'].default is None
    get_signature = inspect.signature(global_user_state.get_volumes)
    assert get_signature.parameters['is_ephemeral'].default is None
    assert get_signature.parameters['name'].default is None
    update_signature = inspect.signature(global_user_state.update_volume_status)
    assert all(update_signature.parameters[name].default is None
               for name in ('error_message', 'usedby_pods', 'usedby_clusters'))


def test_sqlite_lifecycle_preserves_payloads_projection_and_operation_counts(
        tmp_path, monkeypatch):
    engine, manager = _fresh_tracking_db(tmp_path, monkeypatch)
    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _record_statement(*args):
        statements.append(args[2])

    monkeypatch.setattr(global_user_state.time, 'time', lambda: 101)
    monkeypatch.setattr(global_user_state.common_utils, 'get_current_command',
                        lambda: 'sky volumes apply alpha')
    monkeypatch.setattr(global_user_state.common_utils, 'get_current_user',
                        lambda: mock.Mock(id='user-1'))
    monkeypatch.setattr(global_user_state.skypilot_config,
                        'get_active_workspace', lambda: 'workspace-1')

    original = _volume_config('alpha', 'pvc-alpha')
    global_user_state.add_volume('alpha',
                                 original,
                                 status_lib.VolumeStatus.READY,
                                 creation_yaml='name: alpha')
    point_record = global_user_state.get_volume_by_name('alpha')
    assert point_record is not None
    assert set(point_record) == {
        'name', 'launched_at', 'handle', 'user_hash', 'workspace',
        'last_attached_at', 'last_use', 'status', 'error_message',
        'usedby_pods', 'usedby_clusters', 'creation_yaml'
    }
    assert type(point_record['handle']) is models.VolumeConfig
    assert point_record['handle'] == original
    assert point_record['status'] is status_lib.VolumeStatus.READY
    assert point_record['user_hash'] == 'user-1'
    assert point_record['workspace'] == 'workspace-1'
    assert point_record['last_use'] == 'sky volumes apply alpha'
    assert point_record['creation_yaml'] == 'name: alpha'
    assert point_record['usedby_pods'] == []
    assert point_record['usedby_clusters'] == []

    list_record = global_user_state.get_volumes(is_ephemeral=False,
                                                name='alpha')[0]
    assert set(list_record) == set(point_record) | {'is_ephemeral'}
    assert list_record['is_ephemeral'] is False
    assert type(list_record['handle']) is models.VolumeConfig
    assert global_user_state.get_volume_names_start_with('alp') == ['alpha']
    assert global_user_state.get_volume_configs_by_names(
        ['alpha', 'missing', 'alpha']) == {
            'alpha': original
        }

    updated = _volume_config('alpha', 'pvc-updated')
    global_user_state.update_volume_config('alpha', updated)
    global_user_state.update_volume('alpha', 202,
                                    status_lib.VolumeStatus.IN_USE)
    global_user_state.update_volume_status('alpha',
                                           status_lib.VolumeStatus.NOT_READY,
                                           error_message='detached',
                                           usedby_pods=['pod-a'],
                                           usedby_clusters=['cluster-a'])
    updated_record = global_user_state.get_volume_by_name('alpha')
    assert updated_record is not None
    assert updated_record['handle'] == updated
    assert updated_record['last_attached_at'] == 202
    assert updated_record['status'] is status_lib.VolumeStatus.NOT_READY
    assert updated_record['error_message'] == 'detached'
    assert updated_record['usedby_pods'] == ['pod-a']
    assert updated_record['usedby_clusters'] == ['cluster-a']

    global_user_state.delete_volume('alpha')
    assert global_user_state.get_volume_by_name('alpha') is None
    assert manager.get_engine_calls == 11
    assert sum(statement.lstrip().upper().startswith('INSERT')
               for statement in statements) == 1
    assert sum(statement.lstrip().upper().startswith('SELECT')
               for statement in statements) == 6
    assert sum(statement.lstrip().upper().startswith('UPDATE')
               for statement in statements) == 3
    assert sum(statement.lstrip().upper().startswith('DELETE')
               for statement in statements) == 1


def test_empty_batch_is_engine_and_query_free(monkeypatch):
    manager = mock.Mock()
    monkeypatch.setattr(global_user_state, '_db_manager', manager)

    assert global_user_state.get_volume_configs_by_names([]) == {}
    manager.get_engine.assert_not_called()


def test_ephemeral_insert_and_postgresql_dependencies_remain_late_bound(
        monkeypatch):
    engine = mock.Mock()
    engine.dialect.name = db_utils.SQLAlchemyDialect.POSTGRESQL.value
    manager = mock.Mock()
    manager.get_engine.return_value = engine
    monkeypatch.setattr(global_user_state, '_db_manager', manager)

    sqlite_dialect = mock.Mock()
    postgresql_dialect = mock.Mock()
    monkeypatch.setattr(global_user_state, 'sqlite', sqlite_dialect)
    monkeypatch.setattr(global_user_state, 'postgresql', postgresql_dialect)
    timestamps = iter((101, 202))
    monkeypatch.setattr(global_user_state.time, 'time',
                        lambda: next(timestamps))
    monkeypatch.setattr(global_user_state.common_utils, 'get_current_command',
                        lambda: 'sky volumes apply ephemeral')
    monkeypatch.setattr(global_user_state.common_utils, 'get_current_user',
                        lambda: mock.Mock(id='user-2'))
    monkeypatch.setattr(global_user_state.skypilot_config,
                        'get_active_workspace', lambda: 'workspace-2')

    session_factory = mock.MagicMock()
    monkeypatch.setattr(global_user_state.orm, 'Session', session_factory)
    session = session_factory.return_value.__enter__.return_value
    insert_statement = postgresql_dialect.insert.return_value.values.return_value
    conflict_statement = insert_statement.on_conflict_do_nothing.return_value
    config = _volume_config('ephemeral', 'pvc-ephemeral')

    global_user_state.add_volume('ephemeral',
                                 config,
                                 status_lib.VolumeStatus.READY,
                                 is_ephemeral=True,
                                 creation_yaml='name: ephemeral')

    manager.get_engine.assert_called_once_with()
    session_factory.assert_called_once_with(engine)
    sqlite_dialect.insert.assert_not_called()
    postgresql_dialect.insert.assert_called_once_with(
        global_user_state.volume_table)
    values = postgresql_dialect.insert.return_value.values.call_args.kwargs
    assert values['name'] == 'ephemeral'
    assert values['launched_at'] == 101
    assert values['last_attached_at'] == 202
    assert values['status'] == status_lib.VolumeStatus.IN_USE.value
    assert type(values['is_ephemeral']) is int
    assert values['is_ephemeral'] == 1
    assert values['user_hash'] == 'user-2'
    assert values['workspace'] == 'workspace-2'
    assert values['last_use'] == 'sky volumes apply ephemeral'
    assert values['creation_yaml'] == 'name: ephemeral'
    assert global_user_state.pickle.loads(values['handle']) == config
    insert_statement.on_conflict_do_nothing.assert_called_once_with()
    session.execute.assert_called_once_with(conflict_statement)
    session.commit.assert_called_once_with()


def test_unsupported_dialect_contract(monkeypatch):
    engine = mock.Mock()
    engine.dialect.name = 'unsupported'
    manager = mock.Mock()
    manager.get_engine.return_value = engine
    monkeypatch.setattr(global_user_state, '_db_manager', manager)
    monkeypatch.setattr(global_user_state.time, 'time', lambda: 101)
    monkeypatch.setattr(global_user_state.common_utils, 'get_current_command',
                        lambda: 'sky volumes apply')
    monkeypatch.setattr(global_user_state.common_utils, 'get_current_user',
                        lambda: mock.Mock(id='user-3'))
    monkeypatch.setattr(global_user_state.skypilot_config,
                        'get_active_workspace', lambda: 'workspace-3')

    with mock.patch.object(global_user_state.orm, 'Session'):
        with pytest.raises(ValueError, match='Unsupported database dialect'):
            global_user_state.add_volume('alpha',
                                         _volume_config('alpha', 'pvc-alpha'),
                                         status_lib.VolumeStatus.READY)
