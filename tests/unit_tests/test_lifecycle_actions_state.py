"""Tests for the inert lifecycle-action foundation runtime surface."""
# pylint: disable=protected-access

import dataclasses
import datetime
import importlib.util
from unittest import mock
import uuid

import pytest
import sqlalchemy

from sky import lifecycle_actions
from sky.lifecycle_actions import state
from sky.utils.db import migration_utils

_STORE_UUID = uuid.UUID('00000000-0000-4000-8000-000000000001')
_TIMESTAMP = datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc)


def _store_row(**overrides):
    row = {
        'store_key': 'global',
        'store_uuid': _STORE_UUID,
        'schema_version': 1,
        'writer_authority_digest': None,
        'created_at': _TIMESTAMP,
        'created_at_finite': True,
    }
    row.update(overrides)
    return row


def _scope_row(**overrides):
    row = {
        'domain': 'VOLUME',
        'operation_subset': 'KUBERNETES_PVC_OWNED_LIFECYCLE_V1',
        'store_mode': 'CENTRAL_POSTGRESQL',
        'routing_mode': 'DARK',
        'minimum_lifecycle_version': 0,
        'ownership_epoch': 1,
        'authority_generation': 0,
        'writer_implementation_digest': None,
        'reconciler_implementation_digest': None,
        'updated_at': _TIMESTAMP,
        'updated_at_finite': True,
    }
    row.update(overrides)
    return row


def _mapping_result(rows):
    result = mock.Mock()
    result.mappings.return_value.all.return_value = rows
    return result


def _postgres_engine_with_rows(store_rows, scope_rows):
    """Builds a connection that permits SELECT only after read-only mode."""
    engine = mock.MagicMock()
    engine.dialect.name = 'postgresql'
    connection = mock.MagicMock()
    connection_context = (
        engine.connect.return_value.execution_options.return_value)
    connection_context.__enter__.return_value = connection
    transaction = connection.begin.return_value
    read_only = False

    def exec_driver_sql(statement: str):
        nonlocal read_only
        if statement != 'SET TRANSACTION READ ONLY':
            raise AssertionError(f'unexpected driver SQL: {statement}')
        read_only = True

    def execute(statement, *_args, **_kwargs):
        sql = str(statement).strip()
        if not read_only:
            raise AssertionError('query executed before read-only transaction')
        if not sql.upper().startswith('SELECT'):
            raise AssertionError(f'DML is forbidden: {sql}')
        if 'FROM lifecycle_store_identity' in sql:
            return _mapping_result(store_rows)
        if 'FROM lifecycle_ownership_scopes' in sql:
            return _mapping_result(scope_rows)
        raise AssertionError(f'unexpected query: {sql}')

    connection.exec_driver_sql.side_effect = exec_driver_sql
    connection.execute.side_effect = execute
    return engine, connection, transaction


def test_repository_is_lazy_and_shares_default_engine_namespace() -> None:
    assert state._db_manager._engine_namespace is None
    sentinel = mock.Mock(spec=sqlalchemy.engine.Engine)
    with mock.patch.object(state._db_manager,
                           'get_engine',
                           return_value=sentinel) as get_engine:
        assert lifecycle_actions.initialize_and_verify() is None
    get_engine.assert_called_once_with()


def test_private_initializer_rejects_sqlite_before_alembic() -> None:
    sqlite_engine = sqlalchemy.create_engine('sqlite://')
    with mock.patch.object(
            migration_utils,
            'safe_alembic_upgrade') as upgrade, mock.patch.object(
                state, '_read_foundation_from_engine') as read:
        with pytest.raises(RuntimeError, match='requires PostgreSQL'):
            state._initialize_schema(sqlite_engine, mode='verify')

    upgrade.assert_not_called()
    read.assert_not_called()
    assert sqlalchemy.inspect(sqlite_engine).get_table_names() == []
    sqlite_engine.dispose()


@pytest.mark.parametrize('mode', ['auto', 'upgrade', 'bootstrap', 'verify'])
def test_private_initializer_forwards_explicit_mode(mode: str) -> None:
    engine = mock.Mock()
    engine.dialect.name = 'postgresql'
    with mock.patch.object(
            migration_utils,
            'configured_migration_mode',
            side_effect=AssertionError('explicit mode must win')), \
         mock.patch.object(migration_utils,
                           'safe_alembic_upgrade') as upgrade, \
         mock.patch.object(state,
                           '_read_foundation_from_engine') as read:
        state._initialize_schema(engine, mode=mode)

    upgrade.assert_called_once_with(
        engine,
        migration_utils.LIFECYCLE_ACTIONS_DB_NAME,
        migration_utils.LIFECYCLE_ACTIONS_VERSION,
        mode=mode,
    )
    read.assert_called_once_with(engine)


def test_private_initializer_forwards_configured_mode() -> None:
    engine = mock.Mock()
    engine.dialect.name = 'postgresql'
    with mock.patch.object(
            migration_utils, 'configured_migration_mode',
            return_value='bootstrap') as configured, mock.patch.object(
                migration_utils,
                'safe_alembic_upgrade') as upgrade, mock.patch.object(
                    state, '_read_foundation_from_engine') as read:
        state._initialize_schema(engine)

    configured.assert_called_once_with()
    upgrade.assert_called_once_with(
        engine,
        migration_utils.LIFECYCLE_ACTIONS_DB_NAME,
        migration_utils.LIFECYCLE_ACTIONS_VERSION,
        mode='bootstrap',
    )
    read.assert_called_once_with(engine)


def test_public_reader_is_read_only_known_column_and_frozen() -> None:
    engine, connection, transaction = _postgres_engine_with_rows([_store_row()],
                                                                 [_scope_row()])

    with mock.patch.object(
            state._db_manager, 'get_engine',
            return_value=engine) as get_engine, mock.patch.object(
                state.uuid,
                'uuid4',
                side_effect=AssertionError(
                    'runtime must not generate or repair UUIDs')):
        snapshot = lifecycle_actions.read_foundation()

    get_engine.assert_called_once_with()
    engine.connect.return_value.execution_options.assert_called_once_with(
        isolation_level='REPEATABLE READ')
    connection.exec_driver_sql.assert_called_once_with(
        'SET TRANSACTION READ ONLY')
    transaction.commit.assert_called_once_with()
    transaction.rollback.assert_not_called()

    queries = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert len(queries) == 2
    assert all('SELECT *' not in query.upper() for query in queries)
    assert 'store_uuid' in queries[0]
    assert 'writer_authority_digest' in queries[0]
    assert 'minimum_lifecycle_version' in queries[1]
    assert 'reconciler_implementation_digest' in queries[1]
    assert 'WHERE domain = :domain' in queries[1]
    assert connection.execute.call_args_list[1].args[1] == {
        'domain': 'VOLUME',
        'operation_subset': 'KUBERNETES_PVC_OWNED_LIFECYCLE_V1',
        'store_mode': 'CENTRAL_POSTGRESQL',
    }

    assert snapshot == lifecycle_actions.FoundationSnapshot(
        store_identity=lifecycle_actions.StoreIdentitySnapshot(
            store_key='global',
            store_uuid=_STORE_UUID,
            schema_version=1,
            writer_authority_digest=None,
            created_at=_TIMESTAMP,
        ),
        ownership_scope=lifecycle_actions.OwnershipScopeSnapshot(
            domain='VOLUME',
            operation_subset='KUBERNETES_PVC_OWNED_LIFECYCLE_V1',
            store_mode='CENTRAL_POSTGRESQL',
            routing_mode='DARK',
            minimum_lifecycle_version=0,
            ownership_epoch=1,
            authority_generation=0,
            writer_implementation_digest=None,
            reconciler_implementation_digest=None,
            updated_at=_TIMESTAMP,
        ),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(snapshot.store_identity, 'schema_version', 2)
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(snapshot.ownership_scope, 'routing_mode', 'ACTION_OPEN')


def test_public_surface_exposes_no_database_or_mutation_primitives() -> None:
    expected = {
        'FoundationSnapshot',
        'OwnershipScopeSnapshot',
        'StoreIdentitySnapshot',
        'initialize_and_verify',
        'read_foundation',
    }
    assert set(lifecycle_actions.__all__) == expected
    assert set(state.__all__) == expected
    assert importlib.util.find_spec('sky.lifecycle_actions.schema') is None
    for snapshot_type in (
            lifecycle_actions.StoreIdentitySnapshot,
            lifecycle_actions.OwnershipScopeSnapshot,
            lifecycle_actions.FoundationSnapshot,
    ):
        assert dataclasses.is_dataclass(snapshot_type)
        assert snapshot_type.__dataclass_params__.frozen

    for forbidden_name in (
            'engine',
            'get_engine',
            'initialize_and_get_db',
            'connection',
            'session',
            'transaction',
            'insert',
            'update',
            'delete',
            'transition',
            'seal',
            'grant',
            'producer',
            'reconciler',
            'worker',
    ):
        assert not hasattr(lifecycle_actions, forbidden_name)


@pytest.mark.parametrize(('store_count', 'scope_count', 'message'), [
    (0, 1, 'exactly one store identity; found 0'),
    (2, 1, 'exactly one store identity; found 2'),
    (1, 0, 'exactly one volume pilot scope; found 0'),
    (1, 2, 'exactly one volume pilot scope; found 2'),
])
def test_reader_rejects_missing_or_extra_required_rows(store_count: int,
                                                       scope_count: int,
                                                       message: str) -> None:
    engine, _, _ = _postgres_engine_with_rows(
        [_store_row() for _ in range(store_count)],
        [_scope_row() for _ in range(scope_count)],
    )
    with pytest.raises(RuntimeError, match=message):
        state._read_foundation_from_engine(engine)


@pytest.mark.parametrize(('field', 'value'), [
    ('store_key', 'other'),
    ('store_uuid', '00000000-0000-4000-8000-000000000001'),
    ('store_uuid', uuid.UUID('00000000-0000-1000-8000-000000000001')),
    ('schema_version', 2),
    ('writer_authority_digest', 'a' * 64),
])
def test_reader_rejects_malformed_or_incompatible_store_identity(
        field: str, value: object) -> None:
    engine, _, _ = _postgres_engine_with_rows([_store_row(**{field: value})],
                                              [_scope_row()])
    with pytest.raises(RuntimeError, match='store identity is malformed'):
        state._read_foundation_from_engine(engine)


@pytest.mark.parametrize(('field', 'value'), [
    ('domain', 'OTHER'),
    ('operation_subset', 'OTHER'),
    ('store_mode', 'OTHER'),
    ('routing_mode', 'LEGACY_OPEN'),
    ('minimum_lifecycle_version', 1),
    ('ownership_epoch', 2),
    ('authority_generation', 1),
    ('writer_implementation_digest', 'a' * 64),
    ('reconciler_implementation_digest', 'b' * 64),
])
def test_reader_rejects_changed_pilot_scope(field: str, value: object) -> None:
    engine, _, _ = _postgres_engine_with_rows([_store_row()],
                                              [_scope_row(**{field: value})])
    with pytest.raises(RuntimeError, match='exact inert M3-S2 pilot'):
        state._read_foundation_from_engine(engine)


@pytest.mark.parametrize(('store_overrides', 'scope_overrides', 'field'), [
    ({
        'created_at': _TIMESTAMP.replace(tzinfo=None)
    }, {}, 'created_at'),
    ({
        'created_at_finite': False
    }, {}, 'created_at'),
    ({}, {
        'updated_at': _TIMESTAMP.replace(tzinfo=None)
    }, 'updated_at'),
    ({}, {
        'updated_at_finite': False
    }, 'updated_at'),
])
def test_reader_rejects_invalid_database_timestamps(store_overrides,
                                                    scope_overrides,
                                                    field: str) -> None:
    engine, _, _ = _postgres_engine_with_rows([_store_row(**store_overrides)],
                                              [_scope_row(**scope_overrides)])
    with pytest.raises(RuntimeError, match=f'invalid {field} timestamp'):
        state._read_foundation_from_engine(engine)
