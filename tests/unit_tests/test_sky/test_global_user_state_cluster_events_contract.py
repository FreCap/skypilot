"""Characterize the global-user-state cluster-event repository facade."""

import asyncio
import inspect
import types
from unittest import mock

import pytest

from sky import global_user_state
from sky.utils import status_lib
from sky.utils.db import db_utils

_PUBLIC_PARAMETERS = {
    'add_cluster_event': [
        'cluster_name', 'new_status', 'reason', 'event_type',
        'nop_if_duplicate', 'duplicate_regex', 'expose_duplicate_error',
        'transitioned_at', 'existing_cluster_hash'
    ],
    'get_last_cluster_event': ['cluster_hash', 'event_type', 'session'],
    'get_terminal_or_last_status_change_event': ['cluster_hash'],
    '_get_last_or_terminal_cluster_event_multiple': ['cluster_hashes'],
    'get_last_cluster_event_of_type_multiple': ['cluster_hashes', 'event_type'],
    'get_last_status_change_times': ['cluster_hashes', 'ending_status'],
    'get_first_status_change_time_since': [
        'cluster_hash', 'ending_status', 'since'
    ],
    'cleanup_cluster_events_with_retention': ['retention_hours', 'event_type'],
    'get_cluster_events': [
        'cluster_name', 'cluster_hash', 'event_type', 'include_timestamps',
        'limit'
    ],
    'get_cluster_events_by_names': ['cluster_names', 'event_types', 'limit'],
}


def _wrapper_depth(function):
    depth = 0
    while hasattr(function, '__wrapped__'):
        depth += 1
        function = function.__wrapped__
    return depth


def test_public_surface_and_decorator_contract():
    for name, expected_parameters in _PUBLIC_PARAMETERS.items():
        function = getattr(global_user_state, name)
        assert list(
            inspect.signature(function).parameters) == expected_parameters
        assert function.__module__ == 'sky.global_user_state'
        assert function.__qualname__ == name

    assert _wrapper_depth(global_user_state.add_cluster_event) == 2
    assert _wrapper_depth(global_user_state.get_cluster_events) == 1
    assert _wrapper_depth(global_user_state.get_cluster_events_by_names) == 1
    for name in set(_PUBLIC_PARAMETERS) - {
            'add_cluster_event', 'get_cluster_events',
            'get_cluster_events_by_names'
    }:
        assert _wrapper_depth(getattr(global_user_state, name)) == 0

    assert global_user_state.ClusterEventType.__module__ == (
        'sky.global_user_state')


def test_add_event_reuses_facade_duplicate_helper_and_generation_fence(
        monkeypatch):
    engine = mock.Mock()
    engine.dialect.name = db_utils.SQLAlchemyDialect.SQLITE.value
    manager = mock.Mock()
    manager.get_engine.return_value = engine
    monkeypatch.setattr(global_user_state, '_db_manager', manager)

    session_factory = mock.MagicMock()
    monkeypatch.setattr(global_user_state.orm, 'Session', session_factory)
    session = session_factory.return_value.__enter__.return_value
    query = session.query.return_value.filter_by.return_value
    fenced_query = query.filter_by.return_value
    fenced_query.first.return_value = types.SimpleNamespace(cluster_hash='hash',
                                                            status='UP')
    duplicate_lookup = mock.Mock(return_value='same reason')
    monkeypatch.setattr(global_user_state, 'get_last_cluster_event',
                        duplicate_lookup)

    global_user_state.add_cluster_event(
        'cluster',
        status_lib.ClusterStatus.UP,
        'same reason',
        global_user_state.ClusterEventType.STATUS_CHANGE,
        nop_if_duplicate=True,
        existing_cluster_hash='hash')

    manager.get_engine.assert_called_once_with()
    session_factory.assert_called_once_with(engine)
    query.filter_by.assert_called_once_with(cluster_hash='hash')
    duplicate_lookup.assert_called_once_with(
        'hash',
        event_type=global_user_state.ClusterEventType.STATUS_CHANGE,
        session=session)
    session.execute.assert_not_called()
    session.commit.assert_not_called()


def test_event_listing_resolves_through_facade_before_repository_query(
        monkeypatch):
    engine = mock.Mock()
    manager = mock.Mock()
    manager.get_engine.return_value = engine
    monkeypatch.setattr(global_user_state, '_db_manager', manager)
    resolver = mock.Mock(return_value=None)
    monkeypatch.setattr(global_user_state, '_resolve_cluster_hash', resolver)

    with pytest.raises(ValueError, match='Hash for cluster missing not found'):
        global_user_state.get_cluster_events(
            cluster_name='missing',
            cluster_hash=None,
            event_type=global_user_state.ClusterEventType.DEBUG)

    manager.get_engine.assert_called_once_with()
    resolver.assert_called_once_with(None, 'missing')


def test_retention_daemon_calls_historical_cleanup_seam(monkeypatch):
    cleanup = mock.Mock(side_effect=RuntimeError('stop after first cleanup'))
    monkeypatch.setattr(global_user_state,
                        'cleanup_cluster_events_with_retention', cleanup)
    monkeypatch.setattr(global_user_state.skypilot_config, 'reload_config',
                        mock.Mock())
    monkeypatch.setattr(global_user_state.skypilot_config, 'get_nested',
                        mock.Mock(return_value=2))

    sleep = mock.AsyncMock(side_effect=RuntimeError('stop loop'))
    monkeypatch.setattr(global_user_state.asyncio, 'sleep', sleep)

    with pytest.raises(RuntimeError, match='stop loop'):
        asyncio.run(global_user_state.cluster_event_retention_daemon())

    assert cleanup.call_args_list[0] == mock.call(
        2, global_user_state.ClusterEventType.STATUS_CHANGE)
    sleep.assert_awaited_once_with(7200)
