"""Characterize the global-user-state cloud-check cache facade."""

# pylint: disable=protected-access

import inspect
import logging
from unittest import mock

from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from sky import global_user_state
from sky import global_user_state_cloud_checks
from sky.clouds.cloud import CloudCapability
from sky.skylet import constants
from sky.utils.db import db_utils

_PUBLIC_SIGNATURES = {
    'get_cached_enabled_clouds': "(cloud_capability: 'cloud.CloudCapability', workspace: str) -> list['clouds.Cloud']",
    'set_enabled_clouds': "(enabled_clouds: list[str], cloud_capability: 'cloud.CloudCapability', workspace: str) -> None",
    'get_cached_check_results': "(workspace: str) -> dict[str, dict[str, dict[str, typing.Any]]]",
    'set_check_results': "(results: dict[str, dict[str, dict[str, typing.Any]]], workspace: str, *, is_full_workspace_run: bool) -> None",
    'get_allowed_clouds': "(workspace: str) -> list[str]",
    'set_allowed_clouds': "(allowed_clouds: list[str], workspace: str) -> None",
}


class _TrackingManager:

    def __init__(self, manager):
        self._manager = manager
        self.get_engine_calls = 0

    def get_engine(self):
        self.get_engine_calls += 1
        return self._manager.get_engine()


def _fresh_tracking_db(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    manager = db_utils.DatabaseManager(
        'state',
        global_user_state.create_table,
        post_init_fn=lambda _: global_user_state._sqlite_supports_returning(),
    )
    # Initialize schema before statement accounting starts.
    engine = manager.get_engine()
    tracking_manager = _TrackingManager(manager)
    monkeypatch.setattr(global_user_state, '_db_manager', tracking_manager)
    return engine, tracking_manager


def test_public_surface_and_key_formats():
    for name, expected_signature in _PUBLIC_SIGNATURES.items():
        function = getattr(global_user_state, name)
        assert function.__module__ == 'sky.global_user_state'
        assert str(inspect.signature(function)) == expected_signature

    assert global_user_state._get_enabled_clouds_key(
        CloudCapability.COMPUTE, 'team') == 'enabled_clouds_team_compute'
    assert global_user_state._get_check_results_key(
        'team') == 'check_results_team'
    assert global_user_state._get_allowed_clouds_key(
        'team') == 'allowed_clouds_team'
    for name in ('_get_enabled_clouds_key', '_get_check_results_key',
                 '_get_allowed_clouds_key'):
        facade_helper = getattr(global_user_state, name)
        assert facade_helper is getattr(global_user_state_cloud_checks, name)
        assert facade_helper.__module__ == 'sky.global_user_state'


def test_facade_preserves_engine_and_statement_counts(tmp_path, monkeypatch):
    engine, manager = _fresh_tracking_db(tmp_path, monkeypatch)
    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _record_statement(*args):
        statements.append(args[2])

    global_user_state.set_enabled_clouds(['AWS'], CloudCapability.COMPUTE,
                                         'team')
    enabled = global_user_state.get_cached_enabled_clouds(
        CloudCapability.COMPUTE, 'team')
    global_user_state.set_allowed_clouds(['AWS', 'GCP'], 'team')
    allowed = global_user_state.get_allowed_clouds('team')
    results = {'AWS': {'': {'enabled': True, 'reason': 'enabled.'}}}
    global_user_state.set_check_results(results,
                                        'team',
                                        is_full_workspace_run=True)
    cached_results = global_user_state.get_cached_check_results('team')

    assert [str(cloud) for cloud in enabled] == ['AWS']
    assert allowed == ['AWS', 'GCP']
    assert cached_results == results
    assert manager.get_engine_calls == 6
    assert len(statements) == 6
    assert sum(statement.lstrip().upper().startswith('SELECT')
               for statement in statements) == 3
    assert sum(statement.lstrip().upper().startswith('INSERT')
               for statement in statements) == 3


def test_postgres_upsert_statements(monkeypatch):
    engine = mock.MagicMock()
    engine.dialect.name = db_utils.SQLAlchemyDialect.POSTGRESQL.value
    session = mock.MagicMock()
    session_context = mock.MagicMock()
    session_context.__enter__.return_value = session
    monkeypatch.setattr(global_user_state_cloud_checks.orm, 'Session',
                        lambda _: session_context)

    global_user_state_cloud_checks.set_enabled_clouds(engine, ['AWS'],
                                                      CloudCapability.COMPUTE,
                                                      'team')
    global_user_state_cloud_checks.set_allowed_clouds(engine, ['AWS', 'GCP'],
                                                      'team')
    global_user_state_cloud_checks.set_check_results(
        engine, {'AWS': {
            '': {
                'enabled': True
            }
        }},
        'team',
        mock.MagicMock(),
        is_full_workspace_run=True)

    expected_rows = [
        ('enabled_clouds_team_compute', '["AWS"]'),
        ('allowed_clouds_team', '["AWS", "GCP"]'),
        ('check_results_team', '{"AWS": {"": {"enabled": true}}}'),
    ]
    assert session.execute.call_count == len(expected_rows)
    assert session.commit.call_count == len(expected_rows)
    assert session_context.__exit__.call_count == len(expected_rows)
    for execute_call, (expected_key,
                       expected_value) in zip(session.execute.call_args_list,
                                              expected_rows):
        statement = execute_call.args[0]
        compiled = statement.compile(dialect=postgresql.dialect())
        assert ('ON CONFLICT (key) DO UPDATE SET value = ' in str(compiled))
        assert compiled.params == {
            'key': expected_key,
            'value': expected_value,
            'param_1': expected_value,
        }


def test_corrupt_results_keep_historical_logger(tmp_path, monkeypatch, caplog):
    engine, manager = _fresh_tracking_db(tmp_path, monkeypatch)
    with engine.begin() as connection:
        connection.execute(
            text('INSERT INTO config (key, value) VALUES (:key, :value)'), {
                'key': global_user_state._get_check_results_key('team'),
                'value': '{invalid json',
            })

    with caplog.at_level(logging.WARNING, logger='sky.global_user_state'):
        assert global_user_state.get_cached_check_results('team') == {}

    assert manager.get_engine_calls == 1
    assert [
        (record.name, record.getMessage()) for record in caplog.records
    ] == [(
        'sky.global_user_state',
        "Corrupt check_results row for workspace 'team'; returning empty dict.")
         ]
