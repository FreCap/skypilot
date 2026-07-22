"""Unit tests for sky/server/requests/process.py."""
from concurrent.futures import Future
import concurrent.futures.process
import os
import time
import unittest.mock

import pytest

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
