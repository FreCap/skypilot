"""Tests for sky/server/daemons.py.

Covers `_ensure_leader_lock`: the serve/pool consolidation leader lock must
be re-acquired on a fresh session when the underlying Postgres advisory-lock
session silently dies (RDS failover, idle timeout, pg_terminate_backend).
`PostgresLock.is_locked()` only reflects the local flag, so without the
session probe two pods can both believe they are the leader (split-brain HA
recovery), mirroring `_lock_still_held` in the managed-jobs refresh thread.
"""
# pylint: disable=protected-access
from unittest import mock

from sky.server import daemons
from sky.skylet import events
from sky.utils import locks


def _pg_lock(is_locked=True, session_alive=True):
    lock = mock.Mock(spec=locks.PostgresLock)
    lock.is_locked.return_value = is_locked
    lock.is_session_alive.return_value = session_alive
    return lock


def test_none_lock_is_created_and_acquired():
    fresh = mock.Mock(spec=locks.PostgresLock)
    fresh.is_locked.return_value = False
    with mock.patch.object(daemons.locks, 'get_lock',
                           return_value=fresh) as get_lock:
        result = daemons._ensure_leader_lock(None, 'lock-id', 'test lock')
    get_lock.assert_called_once_with('lock-id')
    fresh.acquire.assert_called_once()
    assert result is fresh


def test_held_lock_with_live_session_is_kept():
    lock = _pg_lock(is_locked=True, session_alive=True)
    with mock.patch.object(daemons.locks, 'get_lock') as get_lock:
        result = daemons._ensure_leader_lock(lock, 'lock-id', 'test lock')
    get_lock.assert_not_called()
    lock.acquire.assert_not_called()
    assert result is lock


def test_dead_session_is_discarded_and_reacquired():
    stale = _pg_lock(is_locked=True, session_alive=False)
    fresh = _pg_lock(is_locked=False)
    with mock.patch.object(daemons.locks, 'get_lock',
                           return_value=fresh) as get_lock:
        result = daemons._ensure_leader_lock(stale, 'lock-id', 'test lock')
    stale.release.assert_called_once()
    get_lock.assert_called_once_with('lock-id')
    fresh.acquire.assert_called_once()
    assert result is fresh


def test_dead_session_release_failure_still_reacquires():
    stale = _pg_lock(is_locked=True, session_alive=False)
    stale.release.side_effect = RuntimeError('connection closed')
    fresh = _pg_lock(is_locked=False)
    with mock.patch.object(daemons.locks, 'get_lock', return_value=fresh):
        result = daemons._ensure_leader_lock(stale, 'lock-id', 'test lock')
    fresh.acquire.assert_called_once()
    assert result is fresh


def test_non_postgres_lock_skips_session_probe():
    # FileLock has no session concept; the probe must not be attempted
    # (a Mock(spec=FileLock) would raise AttributeError if it were).
    lock = mock.Mock(spec=locks.FileLock)
    lock.is_locked.return_value = True
    with mock.patch.object(daemons.locks, 'get_lock') as get_lock:
        result = daemons._ensure_leader_lock(lock, 'lock-id', 'test lock')
    get_lock.assert_not_called()
    lock.acquire.assert_not_called()
    assert result is lock


def test_serve_history_event_uses_wall_clock_minutes_and_retries(monkeypatch):
    record = mock.Mock(return_value=2)
    rollup = mock.Mock(return_value=1)
    monkeypatch.setattr(events.serve_history, 'record_status_snapshot', record)
    monkeypatch.setattr(events.serve_history, 'rollup_request_activity_daily',
                        rollup)
    timestamps = iter([120.1, 140.1, 180.1, 240.1, 240.2])
    event = events.ServiceStatusHistoryEvent(time_fn=lambda: next(timestamps))

    event.run()
    event.run()
    event.run()
    assert record.call_args_list == [
        mock.call(timestamp=120.1),
        mock.call(timestamp=180.1),
    ]
    assert rollup.call_args_list == [
        mock.call(timestamp=120.1),
        mock.call(timestamp=180.1),
    ]

    record.side_effect = RuntimeError('transient database failure')
    event.run()
    record.side_effect = None
    event.run()
    assert record.call_args_list[-1] == mock.call(timestamp=240.2)


def test_serve_history_rollup_failure_does_not_block_snapshot(monkeypatch):
    rollup = mock.Mock(side_effect=RuntimeError('rollup failed'))
    record = mock.Mock(return_value=2)
    monkeypatch.setattr(events.serve_history, 'rollup_request_activity_daily',
                        rollup)
    monkeypatch.setattr(events.serve_history, 'record_status_snapshot', record)
    event = events.ServiceStatusHistoryEvent(time_fn=lambda: 120.1)

    event.run()

    rollup.assert_called_once_with(timestamp=120.1)
    record.assert_called_once_with(timestamp=120.1)
