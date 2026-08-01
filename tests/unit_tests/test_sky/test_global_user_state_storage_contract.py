"""Characterize the global-user-state storage repository facade."""

import inspect
from unittest import mock

import pytest
from sqlalchemy import event
from sqlalchemy import text

from sky import global_user_state
from sky.data.storage import Storage
from sky.skylet import constants
from sky.utils import status_lib
from sky.utils.db import db_utils

_PUBLIC_SIGNATURES = {
    'add_or_update_storage':
        ("(storage_name: str, storage_handle: 'Storage.StorageMetadata', "
         'storage_status: sky.utils.status_lib.StorageStatus)'),
    'remove_storage': '(storage_name: str)',
    'set_storage_status':
        ('(storage_name: str, status: sky.utils.status_lib.StorageStatus) '
         '-> None'),
    'get_storage_status':
        ('(storage_name: str) -> sky.utils.status_lib.StorageStatus | None'),
    'set_storage_handle':
        ("(storage_name: str, handle: 'Storage.StorageMetadata') -> None"),
    'get_handle_from_storage_name':
        ("(storage_name: str | None) -> Optional[ForwardRef('Storage."
         "StorageMetadata')]"),
    'get_glob_storage_name': '(storage_name: str) -> list[str]',
    'get_storage_names_start_with': '(starts_with: str) -> list[str]',
    'get_storage': '() -> list[dict[str, typing.Any]]',
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


def _metadata(name, source):
    return Storage.StorageMetadata(storage_name=name, source=source)


def test_public_surface_and_decorator_contract():
    for name, expected_signature in _PUBLIC_SIGNATURES.items():
        function = getattr(global_user_state, name)
        assert str(inspect.signature(function)) == expected_signature
        assert function.__module__ == 'sky.global_user_state'
        assert function.__qualname__ == name
        assert _wrapper_depth(function) == 1


def test_sqlite_lifecycle_preserves_payloads_projection_and_operation_counts(
        tmp_path, monkeypatch):
    engine, manager = _fresh_tracking_db(tmp_path, monkeypatch)
    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _record_statement(*args):
        statements.append(args[2])

    timestamps = iter((101, 202))
    commands = iter(('sky storage create alpha', 'sky storage update alpha'))
    monkeypatch.setattr(global_user_state.time, 'time',
                        lambda: next(timestamps))
    monkeypatch.setattr(global_user_state.common_utils, 'get_current_command',
                        lambda: next(commands))

    original = _metadata('alpha', 's3://original')
    global_user_state.add_or_update_storage('alpha', original,
                                            status_lib.StorageStatus.INIT)
    assert global_user_state.get_storage_status(
        'alpha') is status_lib.StorageStatus.INIT
    restored = global_user_state.get_handle_from_storage_name('alpha')
    assert type(restored) is Storage.StorageMetadata
    assert restored.storage_name == 'alpha'
    assert restored.source == 's3://original'
    assert global_user_state.get_glob_storage_name('a*') == ['alpha']
    assert global_user_state.get_storage_names_start_with('alp') == ['alpha']

    records = global_user_state.get_storage()
    assert len(records) == 1
    assert set(
        records[0]) == {'name', 'launched_at', 'handle', 'last_use', 'status'}
    assert records[0]['name'] == 'alpha'
    assert records[0]['launched_at'] == 101
    assert records[0]['last_use'] == 'sky storage create alpha'
    assert records[0]['status'] is status_lib.StorageStatus.INIT
    assert type(records[0]['handle']) is Storage.StorageMetadata
    assert records[0]['handle'].source == 's3://original'

    updated = _metadata('alpha', 'gs://updated')
    global_user_state.set_storage_handle('alpha', updated)
    global_user_state.set_storage_status('alpha',
                                         status_lib.StorageStatus.READY)
    assert global_user_state.get_handle_from_storage_name(
        'alpha').source == 'gs://updated'
    assert global_user_state.get_storage_status(
        'alpha') is status_lib.StorageStatus.READY

    global_user_state.add_or_update_storage(
        'alpha', updated, status_lib.StorageStatus.UPLOAD_FAILED)
    with engine.connect() as connection:
        row = connection.execute(
            text('SELECT launched_at, last_use, status FROM storage '
                 'WHERE name = :name'), {
                     'name': 'alpha'
                 }).one()
    assert tuple(row) == (202, 'sky storage update alpha', 'UPLOAD_FAILED')

    global_user_state.remove_storage('alpha')
    assert manager.get_engine_calls == 12
    assert sum(statement.lstrip().upper().startswith('INSERT')
               for statement in statements) == 2
    # Includes the direct verification query above in addition to seven facade
    # reads.
    assert sum(statement.lstrip().upper().startswith('SELECT')
               for statement in statements) == 8
    assert sum(statement.lstrip().upper().startswith('UPDATE')
               for statement in statements) == 2
    assert sum(statement.lstrip().upper().startswith('DELETE')
               for statement in statements) == 1


def test_none_lookup_and_missing_update_contract(tmp_path, monkeypatch):
    _, manager = _fresh_tracking_db(tmp_path, monkeypatch)

    assert global_user_state.get_handle_from_storage_name(None) is None
    assert manager.get_engine_calls == 1

    with pytest.raises(ValueError, match=r'^Storage missing not found\.$'):
        global_user_state.set_storage_status('missing',
                                             status_lib.StorageStatus.READY)
    with pytest.raises(ValueError, match=r'^Storagemissing not found\.$'):
        global_user_state.set_storage_handle(
            'missing', _metadata('missing', 's3://missing'))
    assert manager.get_engine_calls == 3


def test_add_validation_and_unsupported_dialect_contract(monkeypatch):
    engine = mock.Mock()
    engine.dialect.name = 'unsupported'
    manager = mock.Mock()
    manager.get_engine.return_value = engine
    monkeypatch.setattr(global_user_state, '_db_manager', manager)

    with pytest.raises(ValueError,
                       match='Storage Status .* is passed in incorrectly'):
        global_user_state.add_or_update_storage(
            'alpha', _metadata('alpha', 's3://alpha'),
            status_lib.ClusterStatus.INIT)
    manager.get_engine.assert_called_once_with()

    with mock.patch.object(global_user_state.orm, 'Session'):
        with pytest.raises(ValueError, match='Unsupported database dialect'):
            global_user_state.add_or_update_storage(
                'alpha', _metadata('alpha', 's3://alpha'),
                status_lib.StorageStatus.INIT)


def test_postgresql_upsert_and_glob_preserve_facade_dependency_paths(
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
    monkeypatch.setattr(global_user_state.time, 'time', lambda: 123)
    monkeypatch.setattr(global_user_state.common_utils, 'get_current_command',
                        lambda: 'sky storage create')

    session_factory = mock.MagicMock()
    monkeypatch.setattr(global_user_state.orm, 'Session', session_factory)
    session = session_factory.return_value.__enter__.return_value
    insert_stmnt = postgresql_dialect.insert.return_value.values.return_value
    upsert_stmnt = insert_stmnt.on_conflict_do_update.return_value
    handle = _metadata('alpha', 's3://alpha')

    global_user_state.add_or_update_storage('alpha', handle,
                                            status_lib.StorageStatus.INIT)

    manager.get_engine.assert_called_once_with()
    session_factory.assert_called_once_with(engine)
    sqlite_dialect.insert.assert_not_called()
    postgresql_dialect.insert.assert_called_once_with(
        global_user_state.storage_table)
    values = postgresql_dialect.insert.return_value.values.call_args.kwargs
    assert values['name'] == 'alpha'
    assert values['last_use'] == 'sky storage create'
    assert values['launched_at'] == 123
    assert values['status'] == 'INIT'
    assert type(global_user_state.pickle.loads(
        values['handle'])) is (Storage.StorageMetadata)
    insert_stmnt.on_conflict_do_update.assert_called_once()
    assert insert_stmnt.on_conflict_do_update.call_args.kwargs[
        'index_elements'] == [global_user_state.storage_table.c.name]
    session.execute.assert_called_once_with(upsert_stmnt)
    session.commit.assert_called_once_with()

    manager.reset_mock()
    session_factory.reset_mock()
    converter = mock.Mock(return_value='translated')
    monkeypatch.setattr(global_user_state, '_glob_to_similar', converter)
    session = session_factory.return_value.__enter__.return_value
    session.query.return_value.filter.return_value.all.return_value = []

    assert global_user_state.get_glob_storage_name('alpha*') == []
    manager.get_engine.assert_called_once_with()
    converter.assert_called_once_with('alpha*')
    session_factory.assert_called_once_with(engine)
