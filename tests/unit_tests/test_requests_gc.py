"""Tests for the requests GC: cadence, finished_at index, and file cleanup.

The requests GC daemon must run hourly regardless of the retention period
(retention only controls the age cutoff), its batch query must be served by a
partial index on finished_at for terminal rows, and each collected request
must also drop its per-request '.<request_id>.lock' file, which otherwise
accumulates for the whole server uptime.
"""
# pylint: disable=protected-access,redefined-outer-name
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


def _make_request(request_id: str, status: RequestStatus,
                  finished_at) -> requests_lib.Request:
    return requests_lib.Request(request_id=request_id,
                                name='sky.launch',
                                entrypoint=_dummy,
                                request_body=payloads.RequestBody(),
                                status=status,
                                created_at=0.0,
                                user_id='test-user',
                                finished_at=finished_at)


async def _seed_request(request_id: str, status: RequestStatus,
                        finished_at) -> dict:
    """Create a request row plus its log, debug-log and lock files."""
    request = _make_request(request_id, status, finished_at)
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
