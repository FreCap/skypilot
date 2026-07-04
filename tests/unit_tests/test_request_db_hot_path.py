"""Tests for the per-request DB hot-path operations.

Covers the targeted status transitions (``try_mark_running`` and the
terminal setters) and the exact-match single-request lookups:
  - ``try_mark_running`` flips PENDING/WAITING to RUNNING atomically and
    refuses any other status.
  - The terminal setters persist SUCCEEDED/FAILED without rewriting
    entrypoint/request_body and never overwrite an already-terminal row
    (notably a CANCELLED + should_retry marker from the shutdown sweep).
  - Single-request getters match on the exact request id; prefix matching
    stays confined to the *_with_prefix APIs.
  - The guarded UPDATE paths serialize with ``update_request``'s
    FileLock-protected full-row read-modify-write, so a stale full-row
    REPLACE can never clobber a terminal result (and vice versa).
"""
# pylint: disable=protected-access
# Pytest fixtures are injected by argument name.
# pylint: disable=redefined-outer-name,unused-argument
import asyncio
import threading
import time
import unittest.mock as mock

import pytest

from sky.server.requests import payloads
from sky.server.requests import requests
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
            requests._DB = None
            yield
            requests._DB = None


def _make_request(request_id: str,
                  status: RequestStatus,
                  pid=None,
                  status_msg=None,
                  should_retry=False) -> requests.Request:
    return requests.Request(request_id=request_id,
                            name='test-request',
                            entrypoint=_dummy,
                            request_body=payloads.RequestBody(),
                            status=status,
                            created_at=0.0,
                            user_id='test-user',
                            pid=pid,
                            status_msg=status_msg,
                            should_retry=should_retry)


# --- try_mark_running ---


@pytest.mark.asyncio
@pytest.mark.parametrize('status',
                         [RequestStatus.PENDING, RequestStatus.WAITING])
async def test_try_mark_running_from_executable_status(isolated_database,
                                                       status):
    assert await requests.create_if_not_exists_async(
        _make_request('req-exec', status, status_msg='retry backoff'))

    assert requests.try_mark_running('req-exec', pid=4242) is True

    record = requests.get_request('req-exec')
    assert record is not None
    assert record.status == RequestStatus.RUNNING
    assert record.pid == 4242
    assert record.status_msg is None


@pytest.mark.asyncio
@pytest.mark.parametrize('status', [
    RequestStatus.RUNNING, RequestStatus.SUCCEEDED, RequestStatus.FAILED,
    RequestStatus.CANCELLED
])
async def test_try_mark_running_refuses_non_executable_status(
        isolated_database, status):
    assert await requests.create_if_not_exists_async(
        _make_request('req-noexec', status, pid=99, status_msg='original'))

    assert requests.try_mark_running('req-noexec', pid=4242) is False

    # The row is left untouched.
    record = requests.get_request('req-noexec')
    assert record is not None
    assert record.status == status
    assert record.pid == 99
    assert record.status_msg == 'original'


def test_try_mark_running_missing_request(isolated_database):
    assert requests.try_mark_running('no-such-request', pid=4242) is False


# --- Terminal setters ---


@pytest.mark.asyncio
async def test_set_request_succeeded_from_running(isolated_database):
    assert await requests.create_if_not_exists_async(
        _make_request('req-ok', RequestStatus.RUNNING, pid=4242))

    requests.set_request_succeeded('req-ok', {'answer': 42})

    record = requests.get_request('req-ok')
    assert record is not None
    assert record.status == RequestStatus.SUCCEEDED
    assert record.finished_at is not None
    assert record.get_return_value() == {'answer': 42}


@pytest.mark.asyncio
async def test_set_request_failed_from_running(isolated_database):
    assert await requests.create_if_not_exists_async(
        _make_request('req-bad', RequestStatus.RUNNING, pid=4242))

    requests.set_request_failed('req-bad', ValueError('boom'))

    record = requests.get_request('req-bad')
    assert record is not None
    assert record.status == RequestStatus.FAILED
    assert record.finished_at is not None
    error = record.get_error()
    assert error is not None
    assert error['type'] == 'ValueError'
    assert isinstance(error['object'], ValueError)


@pytest.mark.asyncio
async def test_terminal_setters_async_variants(isolated_database):
    assert await requests.create_if_not_exists_async(
        _make_request('req-ok-async', RequestStatus.RUNNING, pid=4242))
    assert await requests.create_if_not_exists_async(
        _make_request('req-bad-async', RequestStatus.RUNNING, pid=4242))

    await requests.set_request_succeeded_async('req-ok-async', [1, 2, 3])
    await requests.set_request_failed_async('req-bad-async',
                                            RuntimeError('boom'))

    ok = requests.get_request('req-ok-async')
    assert ok.status == RequestStatus.SUCCEEDED
    assert ok.get_return_value() == [1, 2, 3]
    bad = requests.get_request('req-bad-async')
    assert bad.status == RequestStatus.FAILED
    assert bad.get_error()['type'] == 'RuntimeError'


@pytest.mark.asyncio
async def test_succeeded_does_not_overwrite_cancelled(isolated_database):
    # A CANCELLED + should_retry marker written by the graceful-shutdown
    # sweep must survive a late terminal write from the worker.
    assert await requests.create_if_not_exists_async(
        _make_request('req-interrupted',
                      RequestStatus.CANCELLED,
                      pid=4242,
                      should_retry=True))

    requests.set_request_succeeded('req-interrupted', 'late-result')
    await requests.set_request_succeeded_async('req-interrupted', 'late-result')

    record = requests.get_request('req-interrupted')
    assert record is not None
    assert record.status == RequestStatus.CANCELLED
    assert record.should_retry is True
    assert record.return_value is None


@pytest.mark.asyncio
async def test_failed_does_not_overwrite_succeeded(isolated_database):
    assert await requests.create_if_not_exists_async(
        _make_request('req-done', RequestStatus.SUCCEEDED, pid=4242))

    requests.set_request_failed('req-done', ValueError('too late'))
    await requests.set_request_failed_async('req-done', ValueError('too late'))

    record = requests.get_request('req-done')
    assert record is not None
    assert record.status == RequestStatus.SUCCEEDED
    assert record.get_error() is None


# --- Exact-match lookups ---


@pytest.mark.asyncio
async def test_single_request_getters_require_full_id(isolated_database):
    assert await requests.create_if_not_exists_async(
        _make_request('req-exact-0123456789', RequestStatus.PENDING))

    # Full id matches.
    assert requests.get_request('req-exact-0123456789') is not None
    assert await requests.get_request_async('req-exact-0123456789') is not None
    status = await requests.get_request_status_async('req-exact-0123456789')
    assert status is not None
    assert status.status == RequestStatus.PENDING

    # A bare prefix does not.
    assert requests.get_request('req-exact') is None
    assert await requests.get_request_async('req-exact') is None
    assert await requests.get_request_status_async('req-exact') is None


@pytest.mark.asyncio
async def test_prefix_apis_still_match_prefixes(isolated_database):
    assert await requests.create_if_not_exists_async(
        _make_request('req-prefix-0123456789', RequestStatus.PENDING))

    matches = requests.get_requests_with_prefix('req-prefix')
    assert matches is not None
    assert [m.request_id for m in matches] == ['req-prefix-0123456789']

    matches = await requests.get_requests_async_with_prefix('req-prefix')
    assert matches is not None
    assert [m.request_id for m in matches] == ['req-prefix-0123456789']


# --- Round-trip integrity ---


@pytest.mark.asyncio
async def test_transitions_preserve_entrypoint_and_body(isolated_database):
    # The targeted UPDATEs must never rewrite entrypoint/request_body:
    # after the RUNNING flip and the terminal write, decoding the row
    # still yields the originally-persisted values.
    original = _make_request('req-roundtrip', RequestStatus.PENDING)
    assert await requests.create_if_not_exists_async(original)

    assert requests.try_mark_running('req-roundtrip', pid=4242) is True
    requests.set_request_succeeded('req-roundtrip', 'ok')

    record = requests.get_request('req-roundtrip')
    assert record is not None
    assert record.status == RequestStatus.SUCCEEDED
    assert record.entrypoint is _dummy
    assert record.request_body == original.request_body
    assert record.get_return_value() == 'ok'


# --- Composition with update_request()'s full-row writers ---
#
# update_request() (used by the kill paths and by
# interrupt_request_for_retry) does a FileLock-protected full-row
# SELECT + INSERT OR REPLACE. The guarded UPDATE paths must hold the
# same per-request lock: the SQL status guard alone cannot protect
# against a writer that read the row before the guarded UPDATE landed
# and later REPLACEs its full (stale) row back, clobbering the result.
# These tests hold the lock via update_request() and assert the guarded
# writers block until the context exits, and that no update is lost.

_WAIT_TIMEOUT = 30.0
# Long enough for a non-blocking (buggy) writer to finish many times
# over; the assertion is on the event staying unset, so a correctly
# blocking writer passes deterministically regardless of load.
_BLOCKED_CHECK_TIMEOUT = 1.0


@pytest.mark.asyncio
async def test_set_request_finished_serializes_with_update_request(
        isolated_database):
    # Simulates interrupt_request_for_retry's read-modify-write racing
    # with the worker's terminal write.
    assert await requests.create_if_not_exists_async(
        _make_request('req-compose-sync', RequestStatus.RUNNING, pid=4242))

    started = threading.Event()
    finished = threading.Event()

    def _late_terminal_write():
        started.set()
        requests.set_request_succeeded('req-compose-sync', 'late-result')
        finished.set()

    writer = threading.Thread(target=_late_terminal_write, daemon=True)
    with requests.update_request('req-compose-sync') as record:
        assert record is not None
        assert record.status == RequestStatus.RUNNING
        writer.start()
        assert started.wait(_WAIT_TIMEOUT)
        # While the full-row writer holds the per-request lock, the
        # guarded terminal UPDATE must not land.
        assert not finished.wait(_BLOCKED_CHECK_TIMEOUT)
        record.status = RequestStatus.CANCELLED
        record.should_retry = True
        record.finished_at = time.time()
    # Lock released: the guarded write completes, and its terminal-status
    # guard refuses to overwrite the CANCELLED marker written first.
    assert finished.wait(_WAIT_TIMEOUT)
    writer.join(_WAIT_TIMEOUT)
    assert not writer.is_alive()

    final = requests.get_request('req-compose-sync')
    assert final is not None
    assert final.status == RequestStatus.CANCELLED
    assert final.should_retry is True
    assert final.return_value is None


@pytest.mark.asyncio
async def test_try_mark_running_serializes_with_update_request(
        isolated_database):
    # Inverse race: a kill-path full-row writer vs the RUNNING flip.
    assert await requests.create_if_not_exists_async(
        _make_request('req-compose-flip', RequestStatus.PENDING))

    started = threading.Event()
    finished = threading.Event()
    flip_result = {}

    def _flip_to_running():
        started.set()
        flip_result['value'] = requests.try_mark_running('req-compose-flip',
                                                         pid=777)
        finished.set()

    flipper = threading.Thread(target=_flip_to_running, daemon=True)
    with requests.update_request('req-compose-flip') as record:
        assert record is not None
        flipper.start()
        assert started.wait(_WAIT_TIMEOUT)
        assert not finished.wait(_BLOCKED_CHECK_TIMEOUT)
        # Simulate the kill path cancelling the request under the lock.
        record.status = RequestStatus.CANCELLED
        record.finished_at = time.time()
    assert finished.wait(_WAIT_TIMEOUT)
    flipper.join(_WAIT_TIMEOUT)
    assert not flipper.is_alive()

    # The flip observed the CANCELLED row and refused.
    assert flip_result['value'] is False
    final = requests.get_request('req-compose-flip')
    assert final is not None
    assert final.status == RequestStatus.CANCELLED
    assert final.pid is None


@pytest.mark.asyncio
async def test_set_request_finished_async_serializes_with_update_request(
        isolated_database):
    assert await requests.create_if_not_exists_async(
        _make_request('req-compose-async', RequestStatus.RUNNING, pid=4242))

    lock_held = threading.Event()
    release = threading.Event()

    def _hold_lock_and_cancel():
        with requests.update_request('req-compose-async') as record:
            assert record is not None
            record.status = RequestStatus.CANCELLED
            record.should_retry = True
            record.finished_at = time.time()
            lock_held.set()
            release.wait(_WAIT_TIMEOUT)

    holder = threading.Thread(target=_hold_lock_and_cancel, daemon=True)
    holder.start()
    assert lock_held.wait(_WAIT_TIMEOUT)

    task = asyncio.create_task(
        requests.set_request_succeeded_async('req-compose-async',
                                             'late-result'))
    done, _ = await asyncio.wait({task}, timeout=_BLOCKED_CHECK_TIMEOUT)
    # The async guarded write must stay blocked while the lock is held.
    assert not done
    release.set()
    await asyncio.wait_for(task, timeout=_WAIT_TIMEOUT)
    holder.join(_WAIT_TIMEOUT)
    assert not holder.is_alive()

    final = requests.get_request('req-compose-async')
    assert final is not None
    assert final.status == RequestStatus.CANCELLED
    assert final.should_retry is True
    assert final.return_value is None
