"""Unit tests for sky/server/requests/process.py."""
from concurrent.futures import Future
import concurrent.futures.process
import multiprocessing
import os
import threading
import time
import unittest.mock

import pytest

from sky import exceptions
from sky.server.requests.process import BurstableExecutor
from sky.server.requests.process import DisposableExecutor
from sky.server.requests.process import PoolExecutor


def dummy_task(sleep_time=0.1):
    """A dummy task that sleeps for a given time."""
    time.sleep(sleep_time)
    return True


def failing_task():
    """A task that raises an exception."""
    raise ValueError('Task failed')


def exception_result_task():
    """Return an exception object as ordinary task data."""
    return ValueError('task data')


def abruptly_exiting_task():
    """A task that exits its disposable worker before returning a result."""
    os._exit(7)


def terminating_task():
    """A task that raises a process-terminating exception."""
    raise SystemExit(3)


class UnserializableResult:
    """A result whose serialization fails synchronously."""

    def __reduce__(self):
        raise TypeError('cannot serialize disposable result')


def unserializable_result_task():
    """Return a result that cannot cross the process boundary."""
    return UnserializableResult()


def large_result_task():
    """Return a result larger than a typical OS pipe buffer."""
    return b'x' * 1024 * 1024


def verify_workers_cleanup(executor):
    """Verify workers to be cleaned up.

    Args:
        executor: The DisposableExecutor instance

    Returns:
        bool: True if workers are cleaned up, False if timeout
    """
    with executor._lock:
        if len(executor.workers) == 0:
            return True


def wait_for_futures(futures, timeout=20):
    """Wait for futures to complete.

    Args:
        futures: List of futures to wait for
        timeout: Maximum time to wait in seconds

    Returns:
        bool: True if all futures completed, False if timeout
    """
    start_time = time.time()
    try:
        for future in futures:
            remaining = max(0, timeout - (time.time() - start_time))
            future.result(timeout=remaining)
        return True
    except TimeoutError:
        return False


def test_pool_executor():
    """Test PoolExecutor functionality."""
    executor = PoolExecutor(max_workers=2)
    futures = []
    try:
        # Test submit and has_idle_workers
        assert executor.has_idle_workers()
        future = executor.submit(dummy_task, sleep_time=0.1)
        futures.append(future)
        assert isinstance(future, Future)

        # Test multiple tasks
        for _ in range(2):
            futures.append(executor.submit(dummy_task, sleep_time=0.1))
        # Should have no idle workers when both are running
        assert not executor.has_idle_workers()

        # Wait for all futures to complete before shutdown
        assert wait_for_futures(futures), "Tasks did not complete in time"
        assert all(f.done() for f in futures), "Not all tasks completed"
        assert all(f.result() for f in futures), "Some tasks failed"

        # Should have idle workers after completion
        assert executor.has_idle_workers()
    finally:
        # Wait a bit to ensure all tasks are truly done
        time.sleep(0.1)
        executor.shutdown()


def test_pool_executor_releases_capacity_when_submit_fails(monkeypatch):
    """A rejected task must not consume reusable-pool capacity."""
    executor = PoolExecutor(max_workers=1)
    original_submit = concurrent.futures.ProcessPoolExecutor.submit

    def fail_submit(*_args, **_kwargs):
        raise concurrent.futures.process.BrokenProcessPool('submit failed')

    try:
        monkeypatch.setattr(concurrent.futures.ProcessPoolExecutor, 'submit',
                            fail_submit)
        with pytest.raises(concurrent.futures.process.BrokenProcessPool,
                           match='submit failed'):
            executor.submit(dummy_task)
        assert executor.running.get() == 0
        assert executor.has_idle_workers()

        monkeypatch.setattr(concurrent.futures.ProcessPoolExecutor, 'submit',
                            original_submit)
        assert executor.submit(dummy_task).result(timeout=20)
    finally:
        executor.shutdown()


def test_pool_executor_shutdown_is_idempotent():
    """Repeated shutdown calls must remain safe."""
    executor = PoolExecutor(max_workers=1)
    executor.shutdown()
    executor.shutdown()


def test_pool_executor_forwards_cancel_futures(monkeypatch):
    """The custom shutdown must preserve the standard cancellation option."""
    executor = PoolExecutor(max_workers=1)
    shutdown_calls = []
    original_shutdown = concurrent.futures.ProcessPoolExecutor.shutdown

    def record_shutdown(_executor, wait=True, *, cancel_futures=False):
        shutdown_calls.append((wait, cancel_futures))

    try:
        monkeypatch.setattr(concurrent.futures.ProcessPoolExecutor, 'shutdown',
                            record_shutdown)
        executor.shutdown(cancel_futures=True)
    finally:
        monkeypatch.setattr(concurrent.futures.ProcessPoolExecutor, 'shutdown',
                            original_shutdown)
        executor.shutdown()

    assert shutdown_calls == [(False, True)]


def test_pool_executor_shutdown_includes_concurrent_submission(monkeypatch):
    """Shutdown must terminate a process created by an accepted submit."""
    executor = PoolExecutor(max_workers=1)
    original_adjust = executor._adjust_process_count
    adjust_entered = threading.Event()
    release_adjust = threading.Event()
    shutdown_started = threading.Event()
    snapshot_attempted = threading.Event()
    submitted_futures = []
    submit_errors = []

    class RecordingProcessMap(dict):

        def values(self):
            snapshot_attempted.set()
            return super().values()

    executor._processes = RecordingProcessMap()

    def block_process_start():
        adjust_entered.set()
        assert release_adjust.wait(timeout=20)
        original_adjust()

    monkeypatch.setattr(executor, '_adjust_process_count', block_process_start)

    def submit_worker():
        try:
            submitted_futures.append(executor.submit(dummy_task, sleep_time=2))
        except BaseException as e:  # pylint: disable=broad-except
            submit_errors.append(e)

    def shutdown_executor():
        shutdown_started.set()
        executor.shutdown()

    submit_thread = threading.Thread(target=submit_worker)
    shutdown_thread = threading.Thread(target=shutdown_executor)
    try:
        submit_thread.start()
        assert adjust_entered.wait(timeout=20)
        shutdown_thread.start()
        assert shutdown_started.wait(timeout=20)
        # The custom lifecycle lock must keep shutdown away from the worker
        # snapshot until the accepted submission has registered its process.
        assert not snapshot_attempted.wait(timeout=0.5)
    finally:
        release_adjust.set()
        submit_thread.join(timeout=20)
        shutdown_thread.join(timeout=20)
        executor.shutdown()

    assert not submit_thread.is_alive()
    assert not shutdown_thread.is_alive()
    assert not submit_errors
    assert len(submitted_futures) == 1
    assert snapshot_attempted.is_set()
    with pytest.raises(concurrent.futures.process.BrokenProcessPool):
        submitted_futures[0].result(timeout=20)


def test_disposable_executor():
    """Test DisposableExecutor functionality."""
    executor = DisposableExecutor(max_workers=2)
    try:
        futs = []
        # Test submit and has_idle_workers
        assert executor.has_idle_workers()
        futs.append(executor.submit(dummy_task))

        # Test multiple tasks
        futs.append(executor.submit(dummy_task))
        assert not executor.has_idle_workers()  # No idle workers when full

        concurrent.futures.wait(futs)
        assert verify_workers_cleanup(executor), "Workers not cleaned up"
        assert executor.has_idle_workers()  # Should have idle workers now

        # Test with failing task
        failed_fut = executor.submit(failing_task)
        concurrent.futures.wait([failed_fut])
        with pytest.raises(ValueError, match='Task failed'):
            failed_fut.result()
        assert verify_workers_cleanup(
            executor), "Failed task worker not cleaned up"
        assert executor.has_idle_workers()  # Worker should be cleaned up
    finally:
        executor.shutdown()


def test_disposable_executor_reports_worker_exit():
    """A worker exit must fail its future instead of returning None."""
    executor = DisposableExecutor(max_workers=1)
    try:
        future = executor.submit(abruptly_exiting_task)
        with pytest.raises(concurrent.futures.process.BrokenProcessPool,
                           match='exit code 7'):
            future.result(timeout=20)
    finally:
        executor.shutdown()


def test_disposable_executor_returns_exception_object():
    """An exception-shaped return value must not become a task failure."""
    executor = DisposableExecutor(max_workers=1)
    try:
        result = executor.submit(exception_result_task).result(timeout=20)
        assert isinstance(result, ValueError)
        assert str(result) == 'task data'
    finally:
        executor.shutdown()


def test_disposable_executor_marks_started_future_running():
    """A started process must not expose a cancellable pending future."""
    executor = DisposableExecutor(max_workers=1)
    try:
        future = executor.submit(dummy_task, sleep_time=1)
        assert future.running()
        assert not future.cancel()
        assert future.result(timeout=20)
    finally:
        executor.shutdown()


def test_disposable_executor_contains_terminating_exception():
    """A child BaseException must not terminate the future's caller."""
    executor = DisposableExecutor(max_workers=1)
    try:
        future = executor.submit(terminating_task)
        with pytest.raises(RuntimeError, match='SystemExit: 3') as exc_info:
            future.result(timeout=20)
        assert isinstance(exc_info.value.__cause__, SystemExit)
    finally:
        executor.shutdown()


def test_disposable_executor_reports_unserializable_result():
    """An unserializable result must fail its future."""
    executor = DisposableExecutor(max_workers=1)
    try:
        future = executor.submit(unserializable_result_task)
        with pytest.raises(TypeError,
                           match='cannot serialize disposable result'):
            future.result(timeout=20)
    finally:
        executor.shutdown()


def test_disposable_executor_returns_large_result_without_deadlock():
    """The monitor must drain a large result before joining its worker."""
    executor = DisposableExecutor(max_workers=1)
    try:
        future = executor.submit(large_result_task)
        assert future.result(timeout=20) == b'x' * 1024 * 1024
    finally:
        executor.shutdown()


def test_disposable_executor_reserves_starting_worker(monkeypatch):
    """A starting process must consume capacity before it has a PID."""
    executor = DisposableExecutor(max_workers=1)
    original_start = multiprocessing.Process.start
    first_start_entered = threading.Event()
    release_first_start = threading.Event()
    start_count = 0
    start_count_lock = threading.Lock()

    def block_first_start(process):
        nonlocal start_count
        with start_count_lock:
            start_count += 1
            current_start = start_count
        if current_start == 1:
            first_start_entered.set()
            assert release_first_start.wait(timeout=20)
        return original_start(process)

    monkeypatch.setattr(multiprocessing.Process, 'start', block_first_start)
    first_futures = []
    first_errors = []

    def submit_first():
        try:
            first_futures.append(executor.submit(dummy_task))
        except BaseException as e:  # pylint: disable=broad-except
            first_errors.append(e)

    submit_thread = threading.Thread(target=submit_first)
    try:
        submit_thread.start()
        assert first_start_entered.wait(timeout=20)
        with pytest.raises(exceptions.ExecutionPoolFullError):
            executor.submit(dummy_task)
    finally:
        release_first_start.set()
        submit_thread.join(timeout=20)
        executor.shutdown()

    assert not submit_thread.is_alive()
    assert not first_errors
    assert len(first_futures) == 1


def test_disposable_executor_releases_failed_start_reservation(monkeypatch):
    """A failed process start must restore disposable capacity."""
    executor = DisposableExecutor(max_workers=1)
    original_start = multiprocessing.Process.start

    def fail_start(_):
        raise OSError('process start failed')

    try:
        monkeypatch.setattr(multiprocessing.Process, 'start', fail_start)
        with pytest.raises(OSError, match='process start failed'):
            executor.submit(dummy_task)
        assert executor.has_idle_workers()

        monkeypatch.setattr(multiprocessing.Process, 'start', original_start)
        assert executor.submit(dummy_task).result(timeout=20)
    finally:
        executor.shutdown()


def test_disposable_executor_cleans_up_failed_monitor_start(monkeypatch):
    """A monitor-thread failure must not strand its child process."""
    executor = DisposableExecutor(max_workers=1)
    original_thread_start = threading.Thread.start

    def fail_monitor_start(thread):
        if thread._target == executor._monitor_worker:
            raise RuntimeError('monitor start failed')
        return original_thread_start(thread)

    try:
        monkeypatch.setattr(threading.Thread, 'start', fail_monitor_start)
        with pytest.raises(RuntimeError, match='monitor start failed'):
            executor.submit(dummy_task, sleep_time=30)
        assert executor.has_idle_workers()
        with executor._lock:
            assert not executor.workers
    finally:
        executor.shutdown()


def test_disposable_executor_shutdown_waits_for_starting_worker(monkeypatch):
    """Shutdown must include an accepted worker that has no PID yet."""
    executor = DisposableExecutor(max_workers=1)
    original_start = multiprocessing.Process.start
    start_entered = threading.Event()
    release_start = threading.Event()
    shutdown_started = threading.Event()
    shutdown_returned = threading.Event()
    submitted_futures = []
    submit_errors = []

    def block_start(process):
        start_entered.set()
        assert release_start.wait(timeout=20)
        return original_start(process)

    monkeypatch.setattr(multiprocessing.Process, 'start', block_start)

    def submit_worker():
        try:
            submitted_futures.append(executor.submit(dummy_task, sleep_time=30))
        except BaseException as e:  # pylint: disable=broad-except
            submit_errors.append(e)

    def shutdown_executor():
        shutdown_started.set()
        executor.shutdown()
        shutdown_returned.set()

    submit_thread = threading.Thread(target=submit_worker)
    shutdown_thread = threading.Thread(target=shutdown_executor)
    try:
        submit_thread.start()
        assert start_entered.wait(timeout=20)
        shutdown_thread.start()
        assert shutdown_started.wait(timeout=20)
        assert not shutdown_returned.wait(timeout=0.5)
    finally:
        release_start.set()
        submit_thread.join(timeout=20)
        shutdown_thread.join(timeout=20)
        executor.shutdown()

    assert not submit_thread.is_alive()
    assert not shutdown_thread.is_alive()
    assert not submit_errors
    assert len(submitted_futures) == 1
    with pytest.raises(concurrent.futures.process.BrokenProcessPool):
        submitted_futures[0].result(timeout=20)


def test_burstable_executor():
    """Test BurstableExecutor functionality."""
    executor = BurstableExecutor(garanteed_workers=1, burst_workers=1)
    try:
        # Submit tasks that should go to guaranteed pool first
        executor.submit_until_success(dummy_task)
        # Submit another task that should go to burst pool
        executor.submit_until_success(dummy_task)
        # Submit one more task that should wait and go to guaranteed pool
        executor.submit_until_success(dummy_task)
        # Wait for tasks to complete
        time.sleep(0.3)
    finally:
        executor.shutdown()


def test_burstable_executor_no_guaranteed():
    """Test BurstableExecutor with only burst workers."""
    executor = BurstableExecutor(garanteed_workers=0, burst_workers=1)
    try:
        # Should use burst pool
        executor.submit_until_success(dummy_task)
        time.sleep(0.2)
        # Should be able to submit another task after first one completes
        executor.submit_until_success(dummy_task)
    finally:
        executor.shutdown()


def test_burstable_executor_no_burst():
    """Test BurstableExecutor with only guaranteed workers."""
    executor = BurstableExecutor(garanteed_workers=1, burst_workers=0)
    try:
        # Should use guaranteed pool
        executor.submit_until_success(dummy_task)
        # Should queue to guaranteed pool even when busy
        executor.submit_until_success(dummy_task)
    finally:
        executor.shutdown()


def test_burstable_executor_pool_recovery():
    """Test BurstableExecutor recovery from BrokenProcessPool exception."""

    executor = BurstableExecutor(garanteed_workers=1, burst_workers=0)

    try:
        # Store reference to original executor
        original_executor = executor._executor
        submit_call_count = 0

        # Mock the PoolExecutor.submit method at class level to control
        # behavior across all instances.
        original_submit = PoolExecutor.submit

        def mock_submit(self, fn, *args, **kwargs):
            nonlocal submit_call_count
            submit_call_count += 1
            if submit_call_count == 1:
                # First call raises BrokenProcessPool to simulate pool failure
                raise concurrent.futures.process.BrokenProcessPool(
                    "Simulated process pool failure")
            # Subsequent calls exercise the replacement process pool.
            return original_submit(self, fn, *args, **kwargs)

        with unittest.mock.patch.object(PoolExecutor, 'submit',
                                        new=mock_submit):
            # This should trigger the pool recovery logic in
            # _submit_to_guaranteed_pool
            future = executor.submit_until_success(dummy_task)
            # Process startup can be slow under the full xdist suite, but a
            # bounded timeout still detects a hung replacement pool.
            result = future.result(timeout=60.0)

            # Verify the task completed successfully despite initial failure
            assert result is True

            # Verify that submit was called exactly twice
            # (initial failure + successful retry)
            assert submit_call_count == 2

            # Verify that a new executor was created after the failure
            assert executor._executor is not original_executor

    finally:
        executor.shutdown()
