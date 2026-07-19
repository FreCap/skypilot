"""Tests for the requests GC: cadence, finished_at index, and file cleanup.

The requests GC daemon must run hourly regardless of the retention period
(retention only controls the age cutoff), its batch query must be served by a
partial index on finished_at for terminal rows, and each collected request
must also drop its per-request '.<request_id>.lock' file, which otherwise
accumulates for the whole server uptime.
"""
# pylint: disable=protected-access,redefined-outer-name
import asyncio
import pathlib
import sqlite3
import time
import unittest.mock as mock

import pytest

from sky.server.requests import payloads
from sky.server.requests import requests as requests_lib
from sky.server.requests.requests import RequestStatus


def _dummy():
    return None


@pytest.fixture()
def isolated_database(tmp_path, monkeypatch):
    temp_db_path = tmp_path / 'requests.db'
    temp_log_path = tmp_path / 'logs'
    temp_log_path.mkdir()
    debug_log_dir = tmp_path / 'debug_logs'
    debug_log_dir.mkdir()
    monkeypatch.setattr(requests_lib, 'LEGACY_REQUEST_LOG_PATH_PREFIX',
                        str(tmp_path / 'legacy_logs'))
    monkeypatch.setattr(requests_lib.sky_logging, 'DEBUG_LOG_DIR',
                        str(debug_log_dir))
    with mock.patch('sky.server.constants.API_SERVER_REQUEST_DB_PATH',
                    str(temp_db_path)):
        with mock.patch('sky.server.constants.REQUEST_LOG_PATH_PREFIX',
                        str(temp_log_path)):
            requests_lib._DB = None
            yield temp_db_path
            requests_lib._DB = None


def _make_request(request_id: str,
                  status: RequestStatus,
                  finished_at,
                  name: str = 'sky.launch') -> requests_lib.Request:
    return requests_lib.Request(request_id=request_id,
                                name=name,
                                entrypoint=_dummy,
                                request_body=payloads.RequestBody(),
                                status=status,
                                created_at=0.0,
                                user_id='test-user',
                                finished_at=finished_at)


async def _seed_request(request_id: str,
                        status: RequestStatus,
                        finished_at,
                        name: str = 'sky.launch') -> dict:
    """Create a request row plus its log, debug-log and lock files."""
    request = _make_request(request_id, status, finished_at, name=name)
    assert await requests_lib.create_if_not_exists_async(request)
    files = {
        'log': request.log_path,
        'debug': (pathlib.Path(requests_lib.sky_logging.DEBUG_LOG_DIR) /
                  request_id).with_suffix('.log'),
        'lock': pathlib.Path(requests_lib.request_lock_path(request_id)),
    }
    for path in files.values():
        path.touch()
    return files


@pytest.mark.asyncio
async def test_gc_removes_rows_logs_and_lock_files(isolated_database):
    del isolated_database
    old_files = await _seed_request('req-old', RequestStatus.SUCCEEDED,
                                    time.time() - 10)
    live_files = await _seed_request('req-live', RequestStatus.RUNNING, None)

    await requests_lib.clean_finished_requests_with_retention(0)

    # Check the files before any get_request call: locked accessors recreate
    # the lock file as a side effect of taking the per-request file lock.
    for path in old_files.values():
        assert not path.exists()
    assert requests_lib.get_request('req-old') is None
    # A still-active request keeps its row and all of its files.
    record = requests_lib.get_request('req-live')
    assert record is not None
    assert record.status == RequestStatus.RUNNING
    for path in live_files.values():
        assert path.exists()


@pytest.mark.asyncio
async def test_gc_keeps_requests_younger_than_retention(isolated_database):
    del isolated_database
    files = await _seed_request('req-recent', RequestStatus.FAILED, time.time())

    await requests_lib.clean_finished_requests_with_retention(3600)

    record = requests_lib.get_request('req-recent')
    assert record is not None
    assert record.status == RequestStatus.FAILED
    for path in files.values():
        assert path.exists()


@pytest.mark.asyncio
async def test_gc_removes_terminal_row_without_finished_at(isolated_database):
    del isolated_database
    files = await _seed_request('req-legacy-cancelled', RequestStatus.CANCELLED,
                                None)

    await requests_lib.clean_finished_requests_with_retention(0)

    # Check before get_request(), which recreates the lock file while looking
    # up the now-deleted row.
    for path in files.values():
        assert not path.exists()
    assert requests_lib.get_request('req-legacy-cancelled') is None


@pytest.mark.asyncio
async def test_gc_can_target_only_streaming_request_names(isolated_database):
    del isolated_database
    streaming_name = requests_lib.STREAMING_REQUEST_NAMES[0]
    streaming_files = await _seed_request('req-streaming',
                                          RequestStatus.SUCCEEDED,
                                          time.time() - 10,
                                          name=streaming_name)
    ordinary_files = await _seed_request('req-ordinary',
                                         RequestStatus.SUCCEEDED,
                                         time.time() - 10)

    await requests_lib.clean_finished_requests_with_retention(
        0, include_request_names=list(requests_lib.STREAMING_REQUEST_NAMES))

    # Check before get_request(), which recreates the lock file while looking
    # up the now-deleted row.
    for path in streaming_files.values():
        assert not path.exists()
    assert requests_lib.get_request('req-streaming') is None
    assert requests_lib.get_request('req-ordinary') is not None
    for path in ordinary_files.values():
        assert path.exists()


@pytest.mark.parametrize(
    ('total_gib', 'expected_soft_gib', 'expected_hard_gib'), [(10, 2, 1),
                                                              (200, 20, 10),
                                                              (1000, 20, 10)])
def test_request_log_storage_thresholds(monkeypatch, tmp_path, total_gib,
                                        expected_soft_gib, expected_hard_gib):
    gib = 1024 * 1024 * 1024
    monkeypatch.setattr(requests_lib.server_constants,
                        'REQUEST_LOG_PATH_PREFIX', str(tmp_path))
    monkeypatch.setattr(
        requests_lib.shutil, 'disk_usage', lambda _: mock.Mock(
            total=total_gib * gib, used=0, free=total_gib * gib))

    usage = requests_lib.get_request_log_storage_usage()

    assert usage.free_bytes == total_gib * gib
    assert usage.soft_free_bytes == expected_soft_gib * gib
    assert usage.hard_free_bytes == expected_hard_gib * gib


@pytest.mark.asyncio
async def test_pressure_cleanup_is_idle_when_filesystem_is_healthy(monkeypatch):
    monkeypatch.setattr(
        requests_lib, 'get_request_log_storage_usage',
        lambda: requests_lib.RequestLogStorageUsage(
            free_bytes=11, soft_free_bytes=10, hard_free_bytes=5))
    cleanup = mock.AsyncMock()
    monkeypatch.setattr(requests_lib, 'clean_finished_requests_with_retention',
                        cleanup)

    assert not await requests_lib.cleanup_streaming_requests_under_pressure()
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_pressure_cleanup_targets_terminal_streaming_requests(
        monkeypatch):
    usages = iter([
        requests_lib.RequestLogStorageUsage(free_bytes=9,
                                            soft_free_bytes=10,
                                            hard_free_bytes=5),
        requests_lib.RequestLogStorageUsage(free_bytes=12,
                                            soft_free_bytes=10,
                                            hard_free_bytes=5),
    ])
    monkeypatch.setattr(requests_lib, 'get_request_log_storage_usage',
                        lambda: next(usages))
    cleanup = mock.AsyncMock()
    monkeypatch.setattr(requests_lib, 'clean_finished_requests_with_retention',
                        cleanup)
    cancel = mock.AsyncMock()
    monkeypatch.setattr(requests_lib, 'kill_request_async', cancel)

    assert await requests_lib.cleanup_streaming_requests_under_pressure()
    cleanup.assert_awaited_once_with(
        requests_lib._REQUEST_LOG_PRESSURE_CLEANUP_GRACE_SECONDS,
        include_request_names=list(requests_lib.STREAMING_REQUEST_NAMES))
    cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_pressure_cleanup_cancels_active_streams_below_hard_reserve(
        monkeypatch):
    usages = iter([
        requests_lib.RequestLogStorageUsage(free_bytes=4,
                                            soft_free_bytes=10,
                                            hard_free_bytes=5),
        requests_lib.RequestLogStorageUsage(free_bytes=12,
                                            soft_free_bytes=10,
                                            hard_free_bytes=5),
    ])
    monkeypatch.setattr(requests_lib, 'get_request_log_storage_usage',
                        lambda: next(usages))
    active = [
        mock.Mock(request_id='active-1'),
        mock.Mock(request_id='active-2')
    ]
    query = mock.AsyncMock(return_value=active)
    monkeypatch.setattr(requests_lib, 'get_request_tasks_async', query)
    cancel = mock.AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(requests_lib, 'kill_request_async', cancel)
    cleanup = mock.AsyncMock()
    monkeypatch.setattr(requests_lib, 'clean_finished_requests_with_retention',
                        cleanup)

    assert await requests_lib.cleanup_streaming_requests_under_pressure()

    req_filter = query.await_args.kwargs['req_filter']
    assert req_filter.status == RequestStatus.active_statuses()
    assert req_filter.include_request_names == list(
        requests_lib.STREAMING_REQUEST_NAMES)
    assert cancel.await_count == 2
    cleanup.assert_awaited_once_with(
        requests_lib._REQUEST_LOG_PRESSURE_CLEANUP_GRACE_SECONDS,
        include_request_names=list(requests_lib.STREAMING_REQUEST_NAMES))


@pytest.mark.asyncio
async def test_requests_gc_daemon_does_not_spin_on_retention_failure(
        isolated_database):
    """A failed hourly retention pass is not retried every pressure tick."""
    del isolated_database
    with mock.patch(
            'sky.server.requests.requests.skypilot_config') as mock_config:
        with mock.patch(
                'sky.server.requests.requests.clean_finished_requests_with_retention',
                new_callable=mock.AsyncMock,
                side_effect=RuntimeError('database unavailable')) as mock_clean:
            with mock.patch(
                    'sky.server.requests.requests.get_request_log_storage_usage',
                    return_value=requests_lib.RequestLogStorageUsage(
                        free_bytes=11, soft_free_bytes=10,
                        hard_free_bytes=5)) as mock_pressure_check:
                with mock.patch('asyncio.sleep') as mock_sleep:
                    mock_config.get_nested.return_value = 24
                    mock_sleep.side_effect = [None, asyncio.CancelledError()]

                    with pytest.raises(asyncio.CancelledError):
                        await requests_lib.requests_gc_daemon()

                    mock_clean.assert_awaited_once()
                    assert mock_pressure_check.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('free_bytes', [9, 4])
async def test_requests_gc_daemon_throttles_pressure_cleanup(
        isolated_database, free_bytes):
    """Persistent pressure does not query the database on every probe."""
    del isolated_database
    usage = requests_lib.RequestLogStorageUsage(free_bytes=free_bytes,
                                                soft_free_bytes=10,
                                                hard_free_bytes=5)
    with mock.patch(
            'sky.server.requests.requests.skypilot_config') as mock_config:
        with mock.patch(
                'sky.server.requests.requests.get_request_log_storage_usage',
                return_value=usage):
            with mock.patch(
                    'sky.server.requests.requests.cleanup_streaming_requests_under_pressure',
                    new_callable=mock.AsyncMock) as mock_pressure_clean:
                with mock.patch('asyncio.sleep') as mock_sleep:
                    mock_config.get_nested.return_value = -1
                    mock_sleep.side_effect = [None, asyncio.CancelledError()]

                    with pytest.raises(asyncio.CancelledError):
                        await requests_lib.requests_gc_daemon()

                    mock_pressure_clean.assert_awaited_once_with(usage)


@pytest.mark.asyncio
async def test_finished_at_index_created(isolated_database):
    temp_db_path = isolated_database
    await _seed_request('req-any', RequestStatus.SUCCEEDED, time.time())

    conn = sqlite3.connect(str(temp_db_path))
    try:
        rows = conn.execute('SELECT name FROM sqlite_master WHERE type = ?',
                            ('index',)).fetchall()
    finally:
        conn.close()
    assert ('finished_at_idx',) in rows


def test_gc_daemon_runs_hourly():
    # The GC cadence must be fixed (hourly), not tied to the retention
    # period: with the default 24h retention a retention-based sleep would
    # run the GC once per day and let the table grow between runs.
    assert requests_lib._GC_INTERVAL_SECONDS == 3600
