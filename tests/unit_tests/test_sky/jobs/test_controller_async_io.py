"""Tests for non-blocking managed-jobs controller filesystem access."""

import asyncio
from unittest import mock

import pytest

from sky.jobs import controller


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
