"""Tests for non-blocking managed-jobs controller filesystem access."""

import asyncio
from unittest import mock

import pytest

from sky.jobs import controller
from sky.utils import asyncio_utils


@pytest.mark.asyncio
async def test_start_job_creates_log_directory_off_event_loop(tmp_path):
    manager = controller.ControllerManager('test-uuid')
    original_to_thread = asyncio.to_thread

    def close_background_coroutine(coro):
        coro.close()

    with mock.patch.object(controller.jobs_constants,
                           'JOBS_CONTROLLER_LOGS_DIR', str(tmp_path)), \
         mock.patch.object(controller.asyncio,
                           'to_thread', wraps=original_to_thread) as to_thread, \
         mock.patch.object(manager,
                           'run_job_loop',
                           wraps=manager.run_job_loop) as run_job_loop, \
         mock.patch.object(controller,
                           'create_background_task',
                           side_effect=close_background_coroutine):
        await manager.start_job(3)

    to_thread.assert_awaited_once_with(
        controller._prepare_job_log_path,  # pylint: disable=protected-access
        3)
    run_job_loop.assert_called_once_with(3, str(tmp_path / '3.log'), None)
    assert tmp_path.is_dir()


@pytest.mark.asyncio
async def test_run_job_loop_releases_ownership_after_repeated_cancel():
    # This regression exercises the manager's private ownership locks/state.
    # pylint: disable=protected-access
    manager = controller.ControllerManager('test-uuid')
    manager.starting.add(3)
    manager.job_tasks[3] = mock.Mock()
    manager._cancel_info[3] = (False, None)

    cleanup_waiting = asyncio.Event()
    original_acquire = manager._job_tasks_lock.acquire

    async def track_cleanup_waiter():
        cleanup_waiting.set()
        return await original_acquire()

    background_before = set(asyncio_utils._background_tasks)
    await manager._job_tasks_lock.acquire()
    try:
        with mock.patch.object(manager._job_tasks_lock,
                               'acquire',
                               side_effect=track_cleanup_waiter), \
             mock.patch.object(manager,
                               '_run_job_loop',
                               new_callable=mock.AsyncMock,
                               side_effect=asyncio.CancelledError):
            runner = asyncio.create_task(manager.run_job_loop(3, '/tmp/3.log'))
            await cleanup_waiting.wait()

            # Simulate a second shutdown/cancel signal while the outer
            # finally block is waiting to acquire the bookkeeping lock.
            runner.cancel()
            with pytest.raises(asyncio.CancelledError):
                await runner
            cleanup_tasks = (asyncio_utils._background_tasks -
                             background_before)
            assert len(cleanup_tasks) == 1
    finally:
        manager._job_tasks_lock.release()

    await asyncio.gather(*cleanup_tasks)
    assert 3 not in manager.starting
    assert 3 not in manager.job_tasks
    assert 3 not in manager._cancel_info
