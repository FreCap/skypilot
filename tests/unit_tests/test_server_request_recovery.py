"""Tests for request-state recovery across API server restarts.

The API server used to wipe the whole request DB and logs on every startup
(``reset_db_and_logs``), so any restart -- hard crashes included -- destroyed
queued PENDING/WAITING requests and left clients polling in-flight requests
with a 404. ``recover_db_and_logs`` replaces the wipe: it deletes stale
internal-daemon rows, marks interrupted rows CANCELLED + should_retry (the
client retry signal), preserves queued rows for re-enqueue, and falls back to
the legacy wipe when recovery fails or is explicitly disabled.
"""
# pylint: disable=protected-access
# pylint: disable=redefined-outer-name,unused-argument
import unittest.mock as mock

import pytest

from sky.server import daemons
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import requests as requests_lib
from sky.server.requests.requests import RequestStatus


def _dummy():
    return None


@pytest.fixture()
def isolated_database(tmp_path):
    temp_db_path = tmp_path / 'requests.db'
    temp_log_path = tmp_path / 'logs'
    temp_log_path.mkdir()
    with mock.patch('sky.server.constants.API_SERVER_REQUEST_DB_PATH',
                    str(temp_db_path)):
        with mock.patch('sky.server.constants.REQUEST_LOG_PATH_PREFIX',
                        str(temp_log_path)):
            requests_lib._DB = None
            yield temp_db_path
            requests_lib._DB = None


@pytest.fixture()
def isolated_legacy_logs(tmp_path, monkeypatch):
    monkeypatch.setattr(requests_lib, 'LEGACY_REQUEST_LOG_PATH_PREFIX',
                        str(tmp_path / 'legacy_logs'))


def _make_request(request_id: str,
                  status: RequestStatus,
                  created_at: float = 0.0,
                  schedule_type=requests_lib.ScheduleType.LONG,
                  ignore_return_value: bool = False,
                  retryable: bool = False) -> requests_lib.Request:
    return requests_lib.Request(request_id=request_id,
                                name='sky.launch',
                                entrypoint=_dummy,
                                request_body=payloads.RequestBody(),
                                status=status,
                                created_at=created_at,
                                user_id='test-user',
                                schedule_type=schedule_type,
                                ignore_return_value=ignore_return_value,
                                retryable=retryable)


@pytest.mark.asyncio
async def test_enqueue_flags_survive_insert_read_roundtrip(isolated_database):
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-flags',
                      RequestStatus.PENDING,
                      ignore_return_value=True,
                      retryable=True))
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-defaults', RequestStatus.PENDING))

    record = requests_lib.get_request('req-flags')
    assert record.ignore_return_value is True
    assert record.retryable is True
    record = requests_lib.get_request('req-defaults')
    assert record.ignore_return_value is False
    assert record.retryable is False


@pytest.mark.asyncio
async def test_recovery_reconciles_each_status(isolated_database,
                                               isolated_legacy_logs):
    daemon_id = daemons.INTERNAL_REQUEST_DAEMONS[0].id
    seed = [
        _make_request('req-pending', RequestStatus.PENDING),
        _make_request('req-waiting-retryable',
                      RequestStatus.WAITING,
                      retryable=True),
        _make_request('req-waiting-not-retryable', RequestStatus.WAITING),
        _make_request('req-waiting-legacy-null', RequestStatus.WAITING),
        _make_request('req-running', RequestStatus.RUNNING),
        _make_request('req-succeeded', RequestStatus.SUCCEEDED),
        _make_request('req-failed', RequestStatus.FAILED),
        _make_request('req-cancelled', RequestStatus.CANCELLED),
        _make_request(daemon_id, RequestStatus.RUNNING, retryable=True),
    ]
    for request in seed:
        assert await requests_lib.create_if_not_exists_async(request)
    # Simulate a row written by an older server without the retryable
    # column (NULL instead of 0).
    with requests_lib._DB.conn:
        requests_lib._DB.conn.execute(
            f'UPDATE {requests_lib.REQUEST_TABLE} SET retryable = NULL '
            'WHERE request_id = ?', ('req-waiting-legacy-null',))

    requests_lib.recover_db_and_logs()

    # Stale daemon rows are deleted so the daemon is re-created and
    # re-enqueued on this boot.
    assert requests_lib.get_request(daemon_id) is None
    # Interrupted rows get the client retry signal.
    for request_id in ('req-running', 'req-waiting-not-retryable',
                       'req-waiting-legacy-null'):
        record = requests_lib.get_request(request_id)
        assert record.status == RequestStatus.CANCELLED, request_id
        assert record.should_retry is True, request_id
        assert record.finished_at is not None, request_id
    # Queued rows are preserved for re-enqueue.
    record = requests_lib.get_request('req-pending')
    assert record.status == RequestStatus.PENDING
    assert record.should_retry is False
    record = requests_lib.get_request('req-waiting-retryable')
    assert record.status == RequestStatus.WAITING
    assert record.should_retry is False
    # Terminal rows are untouched.
    record = requests_lib.get_request('req-succeeded')
    assert record.status == RequestStatus.SUCCEEDED
    assert record.should_retry is False
    record = requests_lib.get_request('req-failed')
    assert record.status == RequestStatus.FAILED
    record = requests_lib.get_request('req-cancelled')
    assert record.status == RequestStatus.CANCELLED


@pytest.mark.asyncio
async def test_reenqueue_recovered_requests_in_created_at_order(
        isolated_database, monkeypatch):
    daemon_id = daemons.INTERNAL_REQUEST_DAEMONS[0].id
    seed = [
        _make_request('req-newer',
                      RequestStatus.PENDING,
                      created_at=2.0,
                      schedule_type=requests_lib.ScheduleType.LONG,
                      ignore_return_value=True),
        _make_request('req-older',
                      RequestStatus.WAITING,
                      created_at=1.0,
                      schedule_type=requests_lib.ScheduleType.SHORT,
                      retryable=True),
        _make_request('req-done', RequestStatus.SUCCEEDED, created_at=0.5),
        # Not retryable: never replayed, even if recovery somehow left it
        # in WAITING instead of flipping it to CANCELLED.
        _make_request('req-waiting-not-retryable',
                      RequestStatus.WAITING,
                      created_at=0.3),
        _make_request(daemon_id, RequestStatus.PENDING, created_at=0.1),
    ]
    for request in seed:
        assert await requests_lib.create_if_not_exists_async(request)

    puts = []

    class _StubQueue:

        def __init__(self, schedule_type):
            self._schedule_type = schedule_type

        def put(self, item):
            puts.append((self._schedule_type, item))

    monkeypatch.setattr(executor, '_get_queue', _StubQueue)

    executor.reenqueue_recovered_requests()

    assert puts == [
        (requests_lib.ScheduleType.SHORT, ('req-older', False, True)),
        (requests_lib.ScheduleType.LONG, ('req-newer', True, False)),
    ]


@pytest.mark.asyncio
async def test_reset_env_var_forces_full_wipe(isolated_database,
                                              isolated_legacy_logs,
                                              monkeypatch):
    monkeypatch.setattr(requests_lib.bs, 'get_blob_storage', mock.Mock)
    monkeypatch.setenv(requests_lib.RESET_REQUESTS_ON_STARTUP_ENV_VAR, '1')
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-healthy', RequestStatus.PENDING))

    requests_lib.recover_db_and_logs()

    assert requests_lib.get_request_tasks(
        requests_lib.RequestTaskFilter()) == []


def test_plugin_request_backend_falls_back_to_wipe(monkeypatch):
    # A plugin RequestBackend owns its own restart semantics via
    # reset_on_startup(); the sqlite-level recovery would not see its rows,
    # so the legacy reset path must be taken.
    monkeypatch.setattr(
        requests_lib.request_storage, '_storage_backend',
        mock.Mock(spec=requests_lib.request_storage.RequestBackend))
    wipe = mock.Mock()
    monkeypatch.setattr(requests_lib, 'reset_db_and_logs', wipe)

    requests_lib.recover_db_and_logs()

    wipe.assert_called_once()


@pytest.mark.asyncio
async def test_corrupted_db_falls_back_to_wipe(isolated_database,
                                               isolated_legacy_logs,
                                               monkeypatch):
    monkeypatch.setattr(requests_lib.bs, 'get_blob_storage', mock.Mock)
    isolated_database.write_bytes(b'this is not a sqlite database at all')

    # Must not raise: a corrupted DB may never block startup.
    requests_lib.recover_db_and_logs()

    # The corrupted file was wiped and replaced with a fresh, empty DB that
    # accepts new writes.
    assert requests_lib.get_request_tasks(
        requests_lib.RequestTaskFilter()) == []
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-new', RequestStatus.PENDING))
