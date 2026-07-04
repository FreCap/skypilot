"""Regression test: requests dying between dequeue and execution must fail.

``RequestWorker.process_request`` pops an element from the queue, reads the
request row, and submits it to the executor pool. If anything in that window
raises (a transient DB error, a submit failure), the popped element is gone
from the queue while the row stays PENDING; the ``/api/get`` long-poll only
exits on a terminal status, so the client blocks forever. These tests pin
that such failures terminalize the row (unblocking clients) without crashing
the dispatcher thread, and that an already-terminal row (e.g. cancelled by a
concurrent kill) is left untouched.
"""
# pylint: disable=protected-access,redefined-outer-name,unused-argument
import unittest.mock as mock

import pytest

from sky.server import config as server_config
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
            yield
            requests_lib._DB = None


def _make_request(request_id: str,
                  status: RequestStatus) -> requests_lib.Request:
    return requests_lib.Request(request_id=request_id,
                                name='sky.launch',
                                entrypoint=_dummy,
                                request_body=payloads.RequestBody(),
                                status=status,
                                created_at=0.0,
                                user_id='test-user')


def _worker() -> executor.RequestWorker:
    return executor.RequestWorker(
        requests_lib.ScheduleType.LONG,
        server_config.WorkerConfig(garanteed_parallelism=1,
                                   burstable_parallelism=0,
                                   num_db_connections_per_worker=1))


class _FakeQueue:
    """Yields the given elements once, then behaves as empty."""

    def __init__(self, elements):
        self._elements = list(elements)

    def get(self):
        if self._elements:
            return self._elements.pop(0)
        return None


class _FailingExecutor:
    """Executor whose submit fails, optionally after a side effect."""

    def __init__(self, side_effect=None):
        self.side_effect = side_effect
        self.submit_calls = 0

    def submit_until_success(self, *args, **kwargs):
        del args, kwargs
        self.submit_calls += 1
        if self.side_effect is not None:
            self.side_effect()
        raise RuntimeError('worker pool broken')


@pytest.mark.asyncio
async def test_submit_failure_fails_request(isolated_database, monkeypatch):
    monkeypatch.setattr(executor.time, 'sleep', lambda _: None)
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-strand', RequestStatus.PENDING))
    worker = _worker()
    fake_executor = _FailingExecutor()
    queue = _FakeQueue([('req-strand', True, False)])

    # Must not raise despite the submit failure.
    worker.process_request(fake_executor, queue)

    # The row is terminalized so clients polling /api/get unblock.
    record = requests_lib.get_request('req-strand')
    assert record.status == RequestStatus.FAILED
    # The dispatcher survives: a subsequent poll on an empty queue works.
    worker.process_request(fake_executor, queue)
    assert fake_executor.submit_calls == 1


@pytest.mark.asyncio
async def test_concurrently_cancelled_request_left_untouched(
        isolated_database, monkeypatch):
    monkeypatch.setattr(executor.time, 'sleep', lambda _: None)
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-cancelled', RequestStatus.PENDING))

    def _cancel_then_fail():
        # Simulate a concurrent kill landing between the dequeue status
        # check and the submit failure.
        with requests_lib.update_request('req-cancelled') as record:
            record.status = RequestStatus.CANCELLED

    worker = _worker()
    fake_executor = _FailingExecutor(side_effect=_cancel_then_fail)
    queue = _FakeQueue([('req-cancelled', True, False)])

    worker.process_request(fake_executor, queue)

    # The terminal status set by the concurrent kill is not overwritten.
    record = requests_lib.get_request('req-cancelled')
    assert record.status == RequestStatus.CANCELLED


def test_missing_row_is_dropped(isolated_database, monkeypatch):
    monkeypatch.setattr(executor.time, 'sleep', lambda _: None)
    worker = _worker()
    fake_executor = _FailingExecutor()
    queue = _FakeQueue([('req-ghost', True, False)])

    # Must not raise; the element is dropped without submitting.
    worker.process_request(fake_executor, queue)

    assert fake_executor.submit_calls == 0
    # The dispatcher continues to serve subsequent polls.
    worker.process_request(fake_executor, queue)
    assert fake_executor.submit_calls == 0
