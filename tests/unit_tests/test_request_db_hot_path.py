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
"""
# pylint: disable=protected-access
# Pytest fixtures are injected by argument name.
# pylint: disable=redefined-outer-name,unused-argument
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
