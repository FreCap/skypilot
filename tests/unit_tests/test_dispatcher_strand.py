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
import asyncio
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
            asyncio.run(requests_lib.close_db_async())
            yield
            asyncio.run(requests_lib.close_db_async())


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
        self._reservation = None

    def try_reserve_idle_worker(self):
        assert self._reservation is None
        self._reservation = object()
        return self._reservation

    def release_idle_worker_reservation(self, reservation):
        if reservation is not self._reservation:
            raise ValueError('reservation already consumed')
        self._reservation = None

    def submit_reserved(self, reservation, *args, **kwargs):
        del args, kwargs
        assert reservation is self._reservation
        self._reservation = None
        self.submit_calls += 1
        if self.side_effect is not None:
            self.side_effect()
        raise RuntimeError('worker pool broken')


class _SucceedingExecutor:
    """Executor whose submit succeeds, returning a fake future."""

    def __init__(self):
        self.submit_calls = 0
        self._reservation = None

    def try_reserve_idle_worker(self):
        assert self._reservation is None
        self._reservation = object()
        return self._reservation

    def release_idle_worker_reservation(self, reservation):
        if reservation is not self._reservation:
            raise ValueError('reservation already consumed')
        self._reservation = None

    def submit_reserved(self, reservation, *args, **kwargs):
        del args, kwargs
        assert reservation is self._reservation
        self._reservation = None
        self.submit_calls += 1
        return mock.MagicMock()


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


@pytest.mark.asyncio
async def test_post_submit_failure_leaves_request_untouched(
        isolated_database, monkeypatch):
    """A failure AFTER a successful submit must not terminalize the row.

    Once a future is obtained the request is executing (or about to); its
    lifecycle belongs to ``handle_task_result``. Marking it FAILED from the
    dispatcher would clobber a request that is running normally.
    """
    monkeypatch.setattr(executor.time, 'sleep', lambda _: None)
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-post-submit', RequestStatus.PENDING))

    def _raise_on_thread(*args, **kwargs):
        raise RuntimeError('post-submit bookkeeping broken')

    # Inject a failure in the post-submit bookkeeping (the monitor thread
    # creation), i.e. after submit_reserved returned a future.
    monkeypatch.setattr(executor.threading, 'Thread', _raise_on_thread)

    worker = _worker()
    fake_executor = _SucceedingExecutor()
    queue = _FakeQueue([('req-post-submit', True, False)])

    # Must not raise despite the post-submit failure.
    worker.process_request(fake_executor, queue)

    assert fake_executor.submit_calls == 1
    # The row is NOT terminalized: the request was successfully submitted.
    record = requests_lib.get_request('req-post-submit')
    assert record.status == RequestStatus.PENDING


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
