"""Characterization tests for the managed-jobs event state facade."""
# pylint: disable=protected-access

import ast
import asyncio
import datetime
import inspect

import pytest

from sky.jobs import state
from sky.jobs import state_events
from sky.jobs import state_schema
from sky.jobs import state_storage

_EVENT_SIGNATURES = {
    'add_job_event': ('job_id', 'task_id', 'new_status', 'reason', 'timestamp'),
    '_get_all_task_ids_async': ('job_id',),
    'add_job_event_async':
        ('job_id', 'task_id', 'new_status', 'reason', 'code', 'timestamp'),
    'get_job_events': ('job_id', 'task_id', 'limit'),
    '_get_latest_event_reasons': ('job_ids_by_status',),
    'get_latest_recovery_and_pending_reasons':
        ('recovering_job_ids', 'pending_job_ids'),
    'get_latest_recovery_reasons': ('job_ids',),
    'cleanup_job_events_with_retention_async': ('retention_hours',),
    'job_event_retention_daemon': (),
}


def test_event_facade_contract():
    for name, expected_parameters in _EVENT_SIGNATURES.items():
        signature = inspect.signature(getattr(state, name))
        assert tuple(signature.parameters) == expected_parameters

    assert state.DEFAULT_JOB_EVENT_RETENTION_HOURS == 30 * 24.0
    assert state.JOB_EVENT_DAEMON_INTERVAL_SECONDS == 3600
    assert state.job_events_table is state_schema.job_events_table
    assert getattr(state, '_db_manager') is state_storage.db_manager
    assert state.logger.name == 'sky.jobs.state'


def test_event_facade_uses_direct_repository_aliases():
    for name in _EVENT_SIGNATURES:
        assert getattr(state, name) is getattr(state_events, name)

    assert (state.DEFAULT_JOB_EVENT_RETENTION_HOURS ==
            state_events.DEFAULT_JOB_EVENT_RETENTION_HOURS)
    assert (state.JOB_EVENT_DAEMON_INTERVAL_SECONDS ==
            state_events.JOB_EVENT_DAEMON_INTERVAL_SECONDS)
    assert state_events.job_events_table is state_schema.job_events_table
    assert getattr(state_events, '_db_manager') is state_storage.db_manager
    assert state_events.logger.name == 'sky.jobs.state'


def test_job_event_timestamp_normalization_is_utc_aware():
    naive = datetime.datetime(2026, 7, 21, 12, 0)
    assert state_events._normalize_timestamp(naive) == datetime.datetime(
        2026, 7, 21, 12, 0, tzinfo=datetime.timezone.utc)

    offset = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    aware = datetime.datetime(2026, 7, 21, 12, 0, tzinfo=offset)
    assert state_events._normalize_timestamp(aware) == datetime.datetime(
        2026, 7, 21, 6, 30, tzinfo=datetime.timezone.utc)

    before = datetime.datetime.now(datetime.timezone.utc)
    generated = state_events._normalize_timestamp()
    after = datetime.datetime.now(datetime.timezone.utc)
    assert generated.tzinfo is datetime.timezone.utc
    assert before <= generated <= after


def test_pending_transition_uses_canonical_event_statement_atomically():
    tree = ast.parse(inspect.getsource(state.set_pending))
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert 'job_event_insert_statement' in called_attributes


def test_event_retention_daemon_propagates_cancellation(monkeypatch):
    cleanup_calls = []

    async def _run():
        cleanup_started = asyncio.Event()
        cleanup_blocked = asyncio.Event()

        async def _blocked_cleanup(retention_hours):
            cleanup_calls.append(retention_hours)
            cleanup_started.set()
            await cleanup_blocked.wait()

        monkeypatch.setattr(state_events,
                            'cleanup_job_events_with_retention_async',
                            _blocked_cleanup)
        task = asyncio.create_task(state.job_event_retention_daemon())
        await cleanup_started.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()

    asyncio.run(_run())

    assert cleanup_calls == [state.DEFAULT_JOB_EVENT_RETENTION_HOURS]
