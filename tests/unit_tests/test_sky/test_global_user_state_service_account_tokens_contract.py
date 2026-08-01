"""Characterize the global-user-state service-account-token facade."""

import inspect

from sqlalchemy import event

from sky import global_user_state
from sky.skylet import constants
from sky.utils.db import db_utils

_PUBLIC_SIGNATURES = {
    'add_service_account_token':
        '(token_id: str, token_name: str, token_hash: str, '
        'creator_user_hash: str, service_account_user_id: str, '
        'expires_at: int | None = None) -> None',
    'get_service_account_token': '(token_id: str) -> dict[str, Any] | None',
    'get_service_account_token_by_hash': '(token_hash: str) -> dict[str, Any] | None',
    'get_user_service_account_tokens': '(user_hash: str) -> list[dict[str, Any]]',
    'update_service_account_token_last_used': '(token_id: str) -> None',
    'delete_service_account_token': '(token_id: str) -> bool',
    'rotate_service_account_token':
        '(token_id: str, new_token_hash: str, '
        'new_expires_at: int | None = None) -> None',
    'get_expired_service_account_tokens_by_name_prefix': '(name_prefix: str, now: int) -> list[dict[str, Any]]',
    'get_all_service_account_tokens': '() -> list[dict[str, Any]]',
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
        signature = str(inspect.signature(function)).replace(
            'typing.Any', 'Any')
        assert signature == expected_signature
        assert function.__module__ == 'sky.global_user_state'
        assert function.__qualname__ == name
        expected_depth = 2 if name == 'delete_service_account_token' else 1
        assert _wrapper_depth(function) == expected_depth


def test_facade_preserves_engine_statements_and_projection(
        tmp_path, monkeypatch):
    engine, manager = _fresh_tracking_db(tmp_path, monkeypatch)
    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _record_statement(*args):
        statements.append(args[2])

    global_user_state.add_service_account_token(
        token_id='managed-job-token',
        token_name='managed-job-owner-12345678',
        token_hash='old-hash',
        creator_user_hash='owner',
        service_account_user_id='sa-owner',
        expires_at=50,
    )
    by_id = global_user_state.get_service_account_token('managed-job-token')
    by_hash = global_user_state.get_service_account_token_by_hash('old-hash')
    by_creator = global_user_state.get_user_service_account_tokens('owner')
    expired = (
        global_user_state.get_expired_service_account_tokens_by_name_prefix(
            'managed-job-', 100))
    global_user_state.rotate_service_account_token('managed-job-token',
                                                   'new-hash', 200)
    assert global_user_state.get_service_account_token_by_hash(
        'old-hash') is None
    rotated = global_user_state.get_service_account_token_by_hash('new-hash')
    global_user_state.update_service_account_token_last_used(
        'managed-job-token')
    all_tokens = global_user_state.get_all_service_account_tokens()
    assert global_user_state.delete_service_account_token('managed-job-token')

    assert by_id == by_hash == by_creator[0] == expired[0]
    assert by_id == {
        'token_id': 'managed-job-token',
        'token_name': 'managed-job-owner-12345678',
        'token_hash': 'old-hash',
        'created_at': by_id['created_at'],
        'last_used_at': None,
        'expires_at': 50,
        'creator_user_hash': 'owner',
        'service_account_user_id': 'sa-owner',
    }
    assert rotated is not None
    assert rotated['token_hash'] == 'new-hash'
    assert rotated['expires_at'] == 200
    assert rotated['last_used_at'] is None
    assert all_tokens[0]['last_used_at'] is not None
    assert manager.get_engine_calls == 11
    assert len(statements) == 11
    assert sum(statement.lstrip().upper().startswith('SELECT')
               for statement in statements) == 7
    assert sum(statement.lstrip().upper().startswith('INSERT')
               for statement in statements) == 1
    assert sum(statement.lstrip().upper().startswith('UPDATE')
               for statement in statements) == 2
    assert sum(statement.lstrip().upper().startswith('DELETE')
               for statement in statements) == 1
