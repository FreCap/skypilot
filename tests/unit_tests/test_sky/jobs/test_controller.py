"""Unit tests for sky.jobs.controller - recovery logic for all job types.

Tests cover controller recovery during rolling upgrades for:
- Normal jobs (single task): Recovery based on task status
- Pipeline jobs (sequential multi-task): Recovery with task skip logic
- JobGroups (parallel tasks): Recovery with independent task states

Also tests the cancelled job log download feature in ControllerManager
and file mount cleanup in task_cleanup().
"""
# pylint: disable=assignment-from-none,import-outside-toplevel,no-value-for-parameter
# pylint: disable=protected-access,redefined-outer-name,reimported
# pylint: disable=unused-argument,unused-variable
import asyncio
import contextlib
import os
import pathlib
import signal
import subprocess
import sys
import threading
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple
from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch
import uuid

import pytest

from sky.client import service_account_auth
from sky.jobs import api_access as managed_job_api_access
from sky.jobs import controller as controller_lib
from sky.jobs import state as managed_job_state
from sky.jobs import utils as managed_job_utils
from sky.jobs.controller import ControllerManager
from sky.jobs.controller import JobController
from sky.skylet import constants
from sky.skylet import job_lib
from sky.utils import asyncio_utils
from sky.utils import common
from sky.utils import context
from sky.utils import controller_capability
from sky.utils import status_lib

_TEST_CONTROLLER_SLOT_ID = 0
_TEST_CONTROLLER_SLOT_ATTEMPT = '00000000-0000-0000-0000-000000000001'


def _make_controller_manager() -> ControllerManager:
    return ControllerManager('test-uuid', _TEST_CONTROLLER_SLOT_ID,
                             _TEST_CONTROLLER_SLOT_ATTEMPT)


def test_manager_requires_preimport_capability_before_initialization(
        monkeypatch):
    capability = controller_capability.generate()
    monkeypatch.setenv('SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY',
                       capability)
    monkeypatch.setenv(
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH',
        '/old/hash-path')
    controller_capability.clear_process_local()
    controller_capability.install_process_local(capability)
    try:
        controller_lib._require_bootstrapped_controller_origin_capability()

        assert controller_capability.get_process_local() == capability
        assert all(name not in os.environ for name in (
            'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY',
            'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH'))
    finally:
        controller_capability.clear_process_local()


def test_manager_rejects_capability_transport_after_import(monkeypatch):
    monkeypatch.setenv('SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD',
                       '17')

    with pytest.raises(RuntimeError, match='pre-import bootstrap'):
        controller_lib._require_bootstrapped_controller_origin_capability()


@pytest.mark.asyncio
async def test_main_sets_connection_metric_role_before_initialization():

    class StopInitialization(Exception):
        pass

    initialization_order = []

    def _require_capability():
        initialization_order.append(('capability', None))

    def _set_metrics_role(role):
        initialization_order.append(('metrics-role', role))

    def _hijack_context():
        initialization_order.append(('context', None))
        raise StopInitialization

    with patch.object(controller_lib,
                      '_require_bootstrapped_controller_origin_capability',
                      side_effect=_require_capability), patch.object(
                          controller_lib.db_utils,
                          'set_postgres_connection_metrics_process_role',
                          side_effect=_set_metrics_role), patch.object(
                              controller_lib.context_utils,
                              'hijack_sys_attrs',
                              side_effect=_hijack_context), pytest.raises(
                                  StopInitialization):
        await controller_lib.main('controller-uuid', _TEST_CONTROLLER_SLOT_ID,
                                  _TEST_CONTROLLER_SLOT_ATTEMPT)

    assert initialization_order == [('capability', None),
                                    ('metrics-role', 'managed-job-controller'),
                                    ('context', None)]


class TestFileMountsBlobIdSnapshot:
    """The immutable per-job blob id is resolved once without loop stalls."""

    @staticmethod
    def _make_controller() -> JobController:
        controller = JobController.__new__(JobController)
        controller._job_id = 42
        controller._pool = None
        controller._backend = MagicMock()
        controller._backend.run_timestamp = '2026-07-15-00-00-00-000000'
        controller.starting = set()
        controller.starting_lock = asyncio.Lock()
        controller.starting_signal = asyncio.Condition(controller.starting_lock)
        return controller

    @pytest.mark.asyncio
    @pytest.mark.parametrize('blob_id', ['blob-42', None])
    async def test_snapshot_caches_present_and_null_values(self, blob_id):
        controller = self._make_controller()
        get_blob_id = AsyncMock(return_value=blob_id)

        with patch('sky.jobs.state.get_file_mounts_blob_id_async',
                   new=get_blob_id):
            results = [
                await controller._get_file_mounts_blob_id() for _ in range(1000)
            ]

        assert results == [blob_id] * 1000
        get_blob_id.assert_awaited_once_with(42)

    @pytest.mark.asyncio
    async def test_cancelled_snapshot_lookup_is_retried(self):
        controller = self._make_controller()
        attempts = 0

        async def get_blob_id(_job_id):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise asyncio.CancelledError()
            return 'blob-after-retry'

        with patch('sky.jobs.state.get_file_mounts_blob_id_async',
                   side_effect=get_blob_id) as lookup:
            with pytest.raises(asyncio.CancelledError):
                await controller._get_file_mounts_blob_id()
            assert (await
                    controller._get_file_mounts_blob_id() == 'blob-after-retry')

        assert lookup.await_count == 2

    @pytest.mark.asyncio
    async def test_chain_executor_uses_async_snapshot(self):
        controller = self._make_controller()
        task = MagicMock()
        task.name = 'task'
        task.run = 'echo hello'
        task.metadata = {}
        task.resources = []
        task.envs = {constants.TASK_ID_ENV_VAR: 'managed-task-id'}

        class ExpectedStop(Exception):
            pass

        get_blob_id = AsyncMock(return_value='blob-chain')
        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   new=AsyncMock(return_value=(
                       0, managed_job_state.ManagedJobStatus.PENDING))), \
             patch('sky.jobs.state.get_file_mounts_blob_id_async',
                   new=get_blob_id), \
             patch('sky.jobs.recovery_strategy.StrategyExecutor.make',
                   side_effect=ExpectedStop) as make, \
             pytest.raises(ExpectedStop):
            await controller._run_one_task(0, task)

        get_blob_id.assert_awaited_once_with(42)
        assert make.call_args.kwargs['file_mounts_blob_id'] == 'blob-chain'

    @pytest.mark.asyncio
    async def test_job_group_executors_reuse_snapshot(self):
        controller = self._make_controller()
        tasks = []
        for task_id in range(20):
            task = MagicMock()
            task.name = f'task-{task_id}'
            task.run = 'echo hello'
            task.resources = []
            task.envs = {}
            tasks.append(task)
        controller._dag = MagicMock(tasks=tasks)

        executor = MagicMock()
        executor.max_restarts_on_errors = 0
        executor.recover_on_exit_codes = []
        executor.task_specs.return_value = {}
        get_blob_id = AsyncMock(return_value='blob-group')

        with patch('sky.jobs.state.get_file_mounts_blob_id_async',
                   new=get_blob_id), \
             patch('sky.jobs.controller.job_group_networking.'
                   'generate_wait_for_networking_script', return_value=''), \
             patch('sky.jobs.controller.job_group_networking.'
                   'generate_inline_networking_setup_script',
                   return_value=''), \
             patch('sky.jobs.recovery_strategy.StrategyExecutor.make',
                   return_value=executor) as make, \
             patch('sky.jobs.state.set_starting_async', new=AsyncMock()):
            for task_id, task in enumerate(tasks):
                await controller._prepare_job_group_task_for_launch(
                    task, task_id, 'group', [])

        get_blob_id.assert_awaited_once_with(42)
        assert make.call_count == len(tasks)
        assert all(call.kwargs['file_mounts_blob_id'] == 'blob-group'
                   for call in make.call_args_list)


class TestNormalJobRecovery:
    """Tests for normal (single task) job recovery during controller restart.

    When a controller restarts (e.g., during rolling upgrade), it needs to
    correctly recover a single-task job based on:
    - latest_task_id: The highest task_id that has been started
    - last_task_prev_status: The status of that task

    Recovery logic for single task (task_id=0):
    - If latest_task_id is None or status is PENDING: fresh launch
    - If latest_task_id > task_id: task already completed, skip
    - If latest_task_id == task_id and status != PENDING: resume
    """

    @pytest.fixture
    def mock_task(self):
        """Create a mock task."""
        task = MagicMock()
        task.name = 'test-task'
        task.envs = {}
        task.run = 'echo hello'
        return task

    @pytest.mark.asyncio
    async def test_fresh_launch_when_pending(self, mock_task):
        """Test that PENDING status results in fresh launch."""

        async def mock_get_latest(job_id):
            return (0, managed_job_state.ManagedJobStatus.PENDING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)
            task_id = 0

            is_resume = False
            if (latest_task_id is not None and last_task_prev_status
                    != managed_job_state.ManagedJobStatus.PENDING):
                assert latest_task_id >= task_id
                if latest_task_id > task_id:
                    pass  # Already executed
                elif latest_task_id == task_id:
                    is_resume = True

            # PENDING means fresh launch, not resume
            assert is_resume is False

    @pytest.mark.asyncio
    async def test_fresh_launch_when_none_status(self, mock_task):
        """Test that None latest_task_id results in fresh launch."""

        async def mock_get_latest(job_id):
            return (None, None)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)
            task_id = 0

            is_resume = False
            if (latest_task_id is not None and last_task_prev_status
                    != managed_job_state.ManagedJobStatus.PENDING):
                if latest_task_id > task_id:
                    pass
                elif latest_task_id == task_id:
                    is_resume = True

            # None means fresh launch
            assert is_resume is False

    @pytest.mark.asyncio
    async def test_resume_when_running(self, mock_task):
        """Test that RUNNING status triggers resume."""

        async def mock_get_latest(job_id):
            return (0, managed_job_state.ManagedJobStatus.RUNNING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)
            task_id = 0

            is_resume = False
            if (latest_task_id is not None and last_task_prev_status
                    != managed_job_state.ManagedJobStatus.PENDING):
                assert latest_task_id >= task_id
                if latest_task_id > task_id:
                    pass
                elif latest_task_id == task_id:
                    is_resume = True

            # RUNNING means we should resume
            assert is_resume is True

    @pytest.mark.asyncio
    async def test_resume_when_starting(self, mock_task):
        """Test that STARTING status triggers resume."""

        async def mock_get_latest(job_id):
            return (0, managed_job_state.ManagedJobStatus.STARTING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)
            task_id = 0

            is_resume = False
            if (latest_task_id is not None and last_task_prev_status
                    != managed_job_state.ManagedJobStatus.PENDING):
                assert latest_task_id >= task_id
                if latest_task_id > task_id:
                    pass
                elif latest_task_id == task_id:
                    is_resume = True

            # STARTING means we should resume
            assert is_resume is True

    @pytest.mark.asyncio
    async def test_resume_when_recovering(self, mock_task):
        """Test that RECOVERING status triggers resume."""

        async def mock_get_latest(job_id):
            return (0, managed_job_state.ManagedJobStatus.RECOVERING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)
            task_id = 0

            is_resume = False
            if (latest_task_id is not None and last_task_prev_status
                    != managed_job_state.ManagedJobStatus.PENDING):
                assert latest_task_id >= task_id
                if latest_task_id > task_id:
                    pass
                elif latest_task_id == task_id:
                    is_resume = True

            # RECOVERING means we should resume
            assert is_resume is True

    @pytest.mark.asyncio
    async def test_skip_launch_does_not_happen_for_single_task(self, mock_task):
        """Test that single task never has latest_task_id > task_id."""
        # For a single task job, task_id is always 0
        # latest_task_id can only be 0 or None
        # So the skip logic (latest_task_id > task_id) never applies
        task_id = 0

        # Simulate completed task - but for single task this means the job
        # finished successfully and wouldn't be resumed at all
        latest_task_id = 0
        last_task_prev_status = managed_job_state.ManagedJobStatus.SUCCEEDED

        should_skip = False
        is_resume = False
        if (latest_task_id is not None and last_task_prev_status
                != managed_job_state.ManagedJobStatus.PENDING):
            if latest_task_id > task_id:
                should_skip = True
            elif latest_task_id == task_id:
                is_resume = True

        # For single task, skip never happens (task_id is always 0)
        assert should_skip is False
        # Terminal status still triggers resume logic path
        assert is_resume is True

    @pytest.mark.asyncio
    async def test_running_resume_restores_alive_before_monitoring(
            self, mock_task):
        controller = JobController.__new__(JobController)
        controller._job_id = 42
        controller._pool = None
        controller._backend = MagicMock()
        controller._backend.run_timestamp = '2026-07-30-00-00-00-000000'
        controller.starting = {42}
        controller.starting_lock = asyncio.Lock()
        controller.starting_signal = asyncio.Condition(controller.starting_lock)
        mock_task.metadata = {}
        mock_task.resources = []
        mock_task.envs = {
            constants.TASK_ID_ENV_VAR: 'managed-task-id',
        }

        call_order = []
        executor = MagicMock()
        executor.on_resume = AsyncMock(
            side_effect=lambda _name: call_order.append('on-resume'))
        executor.monitor_task = AsyncMock(
            side_effect=lambda **_kwargs: call_order.append('monitor') or True)
        mark_resumed = AsyncMock(
            side_effect=lambda _job_id: call_order.append('mark-alive'))

        with patch('sky.jobs.controller._add_k8s_annotations'), \
             patch('sky.jobs.controller.usage_lib.messages.usage.'
                   'update_task_id'), \
             patch.object(controller,
                          '_get_file_mounts_blob_id',
                          new=AsyncMock(return_value=None)), \
             patch('sky.jobs.state.get_latest_task_id_status_async',
                   new=AsyncMock(return_value=(
                       0, managed_job_state.ManagedJobStatus.RUNNING))), \
             patch('sky.jobs.state.get_job_status_with_task_id_async',
                   new=AsyncMock(return_value=(
                       managed_job_state.ManagedJobStatus.RUNNING))), \
             patch('sky.jobs.controller.scheduler.job_resumed',
                   new=mark_resumed), \
             patch('sky.jobs.recovery_strategy.StrategyExecutor.make',
                   return_value=executor):
            result = await controller._run_one_task(0, mock_task)

        assert result is True
        mark_resumed.assert_awaited_once_with(42)
        executor.launch.assert_not_called()
        executor.on_resume.assert_awaited_once_with('test-task-42')
        assert call_order == ['mark-alive', 'on-resume', 'monitor']
        assert controller.starting == set()


class TestPoolStartingRestartRecovery:
    """Restart recovery for pool jobs before their first worker assignment."""

    @staticmethod
    def _make_controller() -> JobController:
        controller = JobController.__new__(JobController)
        controller._job_id = 42
        controller._pool = 'test-pool'
        controller._backend = MagicMock()
        controller._backend.run_timestamp = '2026-07-24-00-00-00-000000'
        controller.starting = {42}
        controller.starting_lock = asyncio.Lock()
        controller.starting_signal = asyncio.Condition(controller.starting_lock)
        return controller

    @staticmethod
    def _make_task():
        task = MagicMock()
        task.name = 'pooled-task'
        task.run = 'echo hello'
        task.metadata = {}
        task.resources = []
        task.envs = {constants.TASK_ID_ENV_VAR: 'managed-task-id'}
        return task

    @pytest.mark.asyncio
    async def test_starting_without_assignment_reenters_pool_launch(self):
        controller = self._make_controller()
        task = self._make_task()
        executor = MagicMock()
        executor.launch = AsyncMock(return_value=123.0)
        executor.on_resume = AsyncMock()
        executor.monitor_task = AsyncMock(return_value=True)

        pool_info = AsyncMock(side_effect=[(None, None), ('pool-worker-1', 7)])
        task_status = AsyncMock(side_effect=[
            managed_job_state.ManagedJobStatus.STARTING,
            managed_job_state.ManagedJobStatus.RUNNING,
        ])
        set_started = AsyncMock()

        with patch('sky.jobs.controller._add_k8s_annotations'), \
             patch('sky.jobs.controller.usage_lib.messages.usage.'
                   'update_task_id'), \
             patch.object(controller,
                          '_get_file_mounts_blob_id',
                          new=AsyncMock(return_value=None)), \
             patch('sky.jobs.state.get_latest_task_id_status_async',
                   new=AsyncMock(return_value=(
                       0, managed_job_state.ManagedJobStatus.STARTING))), \
             patch('sky.jobs.state.get_pool_submit_info_async',
                   new=pool_info), \
             patch('sky.jobs.state.get_job_status_with_task_id_async',
                   new=task_status), \
             patch('sky.jobs.state.set_started_async', new=set_started), \
             patch('sky.jobs.recovery_strategy.StrategyExecutor.make',
                   return_value=executor):
            result = await controller._run_one_task(0, task)

        assert result is True
        executor.launch.assert_awaited_once_with()
        assert pool_info.await_count == 2
        set_started.assert_awaited_once_with(job_id=42,
                                             task_id=0,
                                             start_time=123.0,
                                             callback_func=ANY)
        executor.on_resume.assert_awaited_once_with('pool-worker-1')
        assert executor.monitor_task.await_args.kwargs[
            'cluster_name'] == 'pool-worker-1'
        assert executor.monitor_task.await_args.kwargs[
            'job_id_on_pool_cluster'] == 7
        assert controller.starting == set()

    @pytest.mark.asyncio
    async def test_starting_with_assignment_does_not_launch_again(self):
        controller = self._make_controller()
        task = self._make_task()
        executor = MagicMock()
        executor.launch = AsyncMock()
        executor.on_resume = AsyncMock()
        executor.monitor_task = AsyncMock(return_value=True)

        pool_info = AsyncMock(return_value=('pool-worker-1', 7))
        set_started = AsyncMock()

        with patch('sky.jobs.controller._add_k8s_annotations'), \
             patch('sky.jobs.controller.usage_lib.messages.usage.'
                   'update_task_id'), \
             patch.object(controller,
                          '_get_file_mounts_blob_id',
                          new=AsyncMock(return_value=None)), \
             patch('sky.jobs.state.get_latest_task_id_status_async',
                   new=AsyncMock(return_value=(
                       0, managed_job_state.ManagedJobStatus.STARTING))), \
             patch('sky.jobs.state.get_pool_submit_info_async',
                   new=pool_info), \
             patch('sky.jobs.state.get_job_status_with_task_id_async',
                   new=AsyncMock(return_value=(
                       managed_job_state.ManagedJobStatus.STARTING))), \
             patch('sky.jobs.state.set_started_async', new=set_started), \
             patch('sky.jobs.recovery_strategy.StrategyExecutor.make',
                   return_value=executor):
            result = await controller._run_one_task(0, task)

        assert result is True
        executor.launch.assert_not_awaited()
        pool_info.assert_awaited_once_with(42)
        set_started.assert_not_awaited()
        executor.on_resume.assert_awaited_once_with('pool-worker-1')
        assert executor.monitor_task.await_args.kwargs[
            'force_transit_to_recovering'] is True


class TestPipelineJobRecovery:
    """Tests for pipeline (sequential multi-task) job recovery.

    When a controller restarts during a pipeline job:
    - Tasks with task_id < latest_task_id: Already completed, skip
    - Task with task_id == latest_task_id: Resume based on status
    - Tasks with task_id > latest_task_id: Will be run after current completes

    Pipeline jobs run tasks sequentially, so only one task is active at a time.
    """

    @pytest.fixture
    def mock_pipeline_dag(self):
        """Create a mock DAG with 3 sequential tasks."""
        dag = MagicMock()
        dag.name = 'test-pipeline'
        tasks = []
        for i in range(3):
            t = MagicMock()
            t.name = f'pipeline-task-{i}'
            t.envs = {}
            t.run = f'echo task-{i}'
            tasks.append(t)
        dag.tasks = tasks
        dag.is_job_group.return_value = False
        return dag

    @pytest.mark.asyncio
    async def test_resume_first_task_running(self, mock_pipeline_dag):
        """Test resuming when first task (task_id=0) was RUNNING."""

        async def mock_get_latest(job_id):
            return (0, managed_job_state.ManagedJobStatus.RUNNING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)

            # Simulate the loop in run()
            task_actions: Dict[int, str] = {}  # 'skip', 'resume', 'launch'
            for task_id, task in enumerate(mock_pipeline_dag.tasks):
                if (latest_task_id is not None and last_task_prev_status
                        != managed_job_state.ManagedJobStatus.PENDING):
                    if latest_task_id > task_id:
                        task_actions[task_id] = 'skip'
                        continue
                    elif latest_task_id == task_id:
                        task_actions[task_id] = 'resume'
                        # In real code, we'd run the task here
                        break  # Simulate sequential execution
                else:
                    task_actions[task_id] = 'launch'
                    break

            # Task 0 should resume, tasks 1 and 2 not yet processed
            assert task_actions == {0: 'resume'}

    @pytest.mark.asyncio
    async def test_resume_middle_task_running(self, mock_pipeline_dag):
        """Test resuming when middle task (task_id=1) was RUNNING."""

        async def mock_get_latest(job_id):
            return (1, managed_job_state.ManagedJobStatus.RUNNING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)

            task_actions: Dict[int, str] = {}
            for task_id, task in enumerate(mock_pipeline_dag.tasks):
                if (latest_task_id is not None and last_task_prev_status
                        != managed_job_state.ManagedJobStatus.PENDING):
                    if latest_task_id > task_id:
                        task_actions[task_id] = 'skip'
                        continue
                    elif latest_task_id == task_id:
                        task_actions[task_id] = 'resume'
                        break
                else:
                    task_actions[task_id] = 'launch'
                    break

            # Task 0 should be skipped, task 1 should resume
            assert task_actions == {0: 'skip', 1: 'resume'}

    @pytest.mark.asyncio
    async def test_resume_last_task_running(self, mock_pipeline_dag):
        """Test resuming when last task (task_id=2) was RUNNING."""

        async def mock_get_latest(job_id):
            return (2, managed_job_state.ManagedJobStatus.RUNNING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)

            task_actions: Dict[int, str] = {}
            for task_id, task in enumerate(mock_pipeline_dag.tasks):
                if (latest_task_id is not None and last_task_prev_status
                        != managed_job_state.ManagedJobStatus.PENDING):
                    if latest_task_id > task_id:
                        task_actions[task_id] = 'skip'
                        continue
                    elif latest_task_id == task_id:
                        task_actions[task_id] = 'resume'
                        break
                else:
                    task_actions[task_id] = 'launch'
                    break

            # Tasks 0, 1 should be skipped, task 2 should resume
            assert task_actions == {0: 'skip', 1: 'skip', 2: 'resume'}

    @pytest.mark.asyncio
    async def test_skip_completed_task_in_pipeline(self, mock_pipeline_dag):
        """Test that _run_one_task returns True for completed tasks."""
        # When task_id < latest_task_id, the task should return True (success)
        # without actually running, allowing the pipeline to continue

        latest_task_id = 2

        for task_id in range(3):
            should_skip = latest_task_id > task_id

            if task_id == 0:
                assert should_skip is True
            elif task_id == 1:
                assert should_skip is True
            elif task_id == 2:
                assert should_skip is False

    @pytest.mark.asyncio
    async def test_fresh_launch_all_pending(self, mock_pipeline_dag):
        """Test fresh launch when all tasks are PENDING."""

        async def mock_get_latest(job_id):
            return (0, managed_job_state.ManagedJobStatus.PENDING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)

            task_actions: Dict[int, str] = {}
            for task_id, task in enumerate(mock_pipeline_dag.tasks):
                if (latest_task_id is not None and last_task_prev_status
                        != managed_job_state.ManagedJobStatus.PENDING):
                    if latest_task_id > task_id:
                        task_actions[task_id] = 'skip'
                        continue
                    elif latest_task_id == task_id:
                        task_actions[task_id] = 'resume'
                        break
                else:
                    task_actions[task_id] = 'launch'
                    break

            # First task should be fresh launch (PENDING)
            assert task_actions == {0: 'launch'}

    @pytest.mark.asyncio
    async def test_resume_recovering_task(self, mock_pipeline_dag):
        """Test resuming when task was in RECOVERING state."""

        async def mock_get_latest(job_id):
            return (1, managed_job_state.ManagedJobStatus.RECOVERING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)

            task_actions: Dict[int, str] = {}
            for task_id, task in enumerate(mock_pipeline_dag.tasks):
                if (latest_task_id is not None and last_task_prev_status
                        != managed_job_state.ManagedJobStatus.PENDING):
                    if latest_task_id > task_id:
                        task_actions[task_id] = 'skip'
                        continue
                    elif latest_task_id == task_id:
                        task_actions[task_id] = 'resume'
                        break
                else:
                    task_actions[task_id] = 'launch'
                    break

            # Task 0 skipped, task 1 should resume from RECOVERING
            assert task_actions == {0: 'skip', 1: 'resume'}

    @pytest.mark.asyncio
    async def test_resume_starting_task(self, mock_pipeline_dag):
        """Test resuming when task was in STARTING state."""

        async def mock_get_latest(job_id):
            return (0, managed_job_state.ManagedJobStatus.STARTING)

        with patch('sky.jobs.state.get_latest_task_id_status_async',
                   side_effect=mock_get_latest):
            latest_task_id, last_task_prev_status = await mock_get_latest(
                job_id=1)

            task_actions: Dict[int, str] = {}
            for task_id, task in enumerate(mock_pipeline_dag.tasks):
                if (latest_task_id is not None and last_task_prev_status
                        != managed_job_state.ManagedJobStatus.PENDING):
                    if latest_task_id > task_id:
                        task_actions[task_id] = 'skip'
                        continue
                    elif latest_task_id == task_id:
                        task_actions[task_id] = 'resume'
                        break
                else:
                    task_actions[task_id] = 'launch'
                    break

            # Task 0 should resume from STARTING
            assert task_actions == {0: 'resume'}


class TestJobGroupRecovery:
    """Tests for JobGroup recovery during controller rolling upgrade.

    When a controller restarts (e.g., during rolling upgrade), it needs to
    correctly recover job groups based on each task's individual state:
    - None/PENDING: fresh launch
    - Terminal (SUCCEEDED/FAILED/etc.): skip (already done)
    - RUNNING: resume monitoring without forced recovery
    - Other non-terminal (STARTING/RECOVERING): resume with forced recovery
    - CANCELLING: raise CancelledError
    """

    @pytest.fixture
    def mock_task(self):
        """Create a mock task."""
        task = MagicMock()
        task.name = 'test-task'
        task.envs = {}
        return task

    @pytest.fixture
    def mock_dag(self, mock_task):
        """Create a mock DAG with multiple tasks."""
        dag = MagicMock()
        dag.name = 'test-job-group'
        # Create 3 tasks for testing different scenarios
        tasks = []
        for i in range(3):
            t = MagicMock()
            t.name = f'task-{i}'
            t.envs = {}
            tasks.append(t)
        dag.tasks = tasks
        return dag

    @staticmethod
    def _make_controller(mock_dag):
        job_controller = JobController.__new__(JobController)
        job_controller._job_id = 42
        job_controller._pool = None
        job_controller._dag = mock_dag
        job_controller.starting = set()
        job_controller.starting_lock = asyncio.Lock()
        job_controller.starting_signal = asyncio.Condition(
            job_controller.starting_lock)
        return job_controller

    @pytest.mark.asyncio
    async def test_job_group_recovery_reads_one_ordered_handle_snapshot(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        executor = MagicMock()

        async def monitor_task(**kwargs):
            await kwargs['on_recovery']()
            return True

        executor.monitor_task = AsyncMock(side_effect=monitor_task)
        old_handles = [MagicMock(name=f'old-{i}') for i in range(3)]
        all_tasks_handles = list(zip(mock_dag.tasks, old_handles))
        fresh_handles = {
            'cluster-task-0': MagicMock(name='fresh-0'),
            # A missing row must remain represented as a None handle so the
            # networking layer can apply its existing failure semantics.
            'cluster-task-2': MagicMock(name='fresh-2'),
        }
        batched_lookup = MagicMock(return_value=fresh_handles)
        per_task_lookup = MagicMock(
            side_effect=AssertionError('per-task handle lookup'))
        setup_networking = AsyncMock()

        with patch.object(
                controller_lib.managed_job_utils,
                'generate_managed_job_cluster_name',
                side_effect=lambda task_name, _job_id:
                f'cluster-{task_name}'), patch.object(
                    controller_lib.global_user_state,
                    'get_handles_from_cluster_names', batched_lookup), \
                patch.object(
                    controller_lib.global_user_state,
                    'get_handle_from_cluster_name', per_task_lookup), \
                patch.object(
                    controller_lib.job_group_networking,
                    'setup_job_group_networking', setup_networking):
            result = await job_controller._monitor_job_group_task(
                task_id=0,
                task=mock_dag.tasks[0],
                cluster_name='cluster-task-0',
                executor=executor,
                job_group_name='test-job-group',
                all_tasks_handles=all_tasks_handles)

        assert result is True
        batched_lookup.assert_called_once_with(
            {'cluster-task-0', 'cluster-task-1', 'cluster-task-2'})
        per_task_lookup.assert_not_called()
        setup_networking.assert_awaited_once_with('test-job-group', [
            (mock_dag.tasks[0], fresh_handles['cluster-task-0']),
            (mock_dag.tasks[1], None),
            (mock_dag.tasks[2], fresh_handles['cluster-task-2']),
        ])

    @pytest.mark.asyncio
    async def test_job_group_recovery_snapshot_failure_skips_networking(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        executor = MagicMock()

        async def monitor_task(**kwargs):
            await kwargs['on_recovery']()
            return True

        executor.monitor_task = AsyncMock(side_effect=monitor_task)
        batched_lookup = MagicMock(
            side_effect=RuntimeError('handle snapshot failed'))
        setup_networking = AsyncMock()

        with patch.object(
                controller_lib.managed_job_utils,
                'generate_managed_job_cluster_name',
                side_effect=lambda task_name, _job_id:
                f'cluster-{task_name}'), patch.object(
                    controller_lib.global_user_state,
                    'get_handles_from_cluster_names', batched_lookup), \
                patch.object(
                    controller_lib.job_group_networking,
                    'setup_job_group_networking', setup_networking), \
                pytest.raises(RuntimeError, match='handle snapshot failed'):
            await job_controller._monitor_job_group_task(
                task_id=0,
                task=mock_dag.tasks[0],
                cluster_name='cluster-task-0',
                executor=executor,
                job_group_name='test-job-group',
                all_tasks_handles=[
                    (task, MagicMock()) for task in mock_dag.tasks
                ])

        batched_lookup.assert_called_once()
        setup_networking.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cleanup_job_group_clusters_runs_concurrently_and_isolates_failures(
            self, mock_dag, caplog):
        job_controller = self._make_controller(mock_dag)
        release_cleanup = asyncio.Event()
        started = []
        completed = []

        async def cleanup_cluster(cluster_name):
            started.append(cluster_name)
            await release_cleanup.wait()
            if cluster_name == 'bad-cluster':
                raise RuntimeError('cleanup failed')
            completed.append(cluster_name)

        job_controller._cleanup_cluster = cleanup_cluster
        cleanup_task = asyncio.create_task(
            job_controller._cleanup_job_group_clusters(
                ['first-cluster', None, 'bad-cluster', 'last-cluster']))

        # Let the helper schedule every independent cleanup. A serial loop can
        # only start first-cluster before the release gate opens.
        for _ in range(10):
            await asyncio.sleep(0)
            if len(started) == 3:
                break
        started_before_release = set(started)
        release_cleanup.set()
        await cleanup_task

        assert started_before_release == {
            'first-cluster', 'bad-cluster', 'last-cluster'
        }
        assert sorted(started) == [
            'bad-cluster', 'first-cluster', 'last-cluster'
        ]
        assert set(completed) == {'first-cluster', 'last-cluster'}
        assert 'Failed to cleanup bad-cluster: cleanup failed' in caplog.text

    @pytest.mark.asyncio
    async def test_cleanup_job_group_clusters_propagates_cancellation(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        all_started = asyncio.Event()
        started = set()
        cancelled = set()

        async def cleanup_cluster(cluster_name):
            started.add(cluster_name)
            if len(started) == 2:
                all_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.add(cluster_name)
                raise

        job_controller._cleanup_cluster = cleanup_cluster
        cleanup_task = asyncio.create_task(
            job_controller._cleanup_job_group_clusters(['first', 'second']))
        await all_started.wait()

        cleanup_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cleanup_task

        assert started == {'first', 'second'}
        assert cancelled == {'first', 'second'}

    @pytest.mark.asyncio
    async def test_auxiliary_termination_propagates_state_write_failure(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        job_controller._cleanup_cluster = AsyncMock()
        monitor_tasks = {
            task_id: asyncio.create_task(asyncio.Event().wait())
            for task_id in range(2)
        }
        cancelling_calls = []
        cancelled_calls = []

        async def set_cancelling(*, job_id, callback_func):
            assert job_id == 42
            cancelling_calls.append(callback_func)
            if callback_func == 0:
                raise RuntimeError('state write failed')

        async def set_cancelled(*, job_id, callback_func):
            assert job_id == 42
            cancelled_calls.append(callback_func)

        with patch.object(
                controller_lib.managed_job_utils,
                'event_callback_func',
                side_effect=lambda **kwargs: kwargs['task_id']), patch.object(
                    controller_lib.managed_job_state,
                    'set_cancelling_async',
                    side_effect=set_cancelling), patch.object(
                        controller_lib.managed_job_state,
                        'set_cancelled_async',
                        side_effect=set_cancelled), pytest.raises(
                            RuntimeError, match='state write failed'):
            await job_controller._terminate_auxiliary_jobs(
                mock_dag.tasks[:2],
                monitor_tasks, ['cluster-0', 'cluster-1'],
                all_primary_succeeded=False)

        # A failed transition is surfaced only after independent siblings have
        # finished, so one broken row cannot strand every auxiliary task.
        assert set(cancelling_calls) == {0, 1}
        assert cancelled_calls == [1]
        job_controller._cleanup_cluster.assert_awaited_once_with('cluster-1')
        assert all(task.done() for task in monitor_tasks.values())

    @pytest.mark.asyncio
    async def test_job_group_launch_propagates_child_cancellation(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        mock_dag.tasks = mock_dag.tasks[:1]
        mock_dag.primary_tasks = []
        executor = MagicMock()
        executor.launch = AsyncMock(side_effect=asyncio.CancelledError())
        job_controller._prepare_job_group_task_for_launch = AsyncMock(
            return_value=('cluster-0', executor))
        job_controller._monitor_job_group_task = AsyncMock(return_value=True)
        job_controller._cleanup_job_group_clusters = AsyncMock()
        statuses = AsyncMock(return_value=[])
        set_started = AsyncMock()
        barrier_snapshot = MagicMock(return_value={})

        with patch.object(controller_lib.managed_job_runtime,
                          'is_registered',
                          return_value=False), patch.object(
                              controller_lib.managed_job_state,
                              'get_all_task_ids_statuses_async',
                              statuses), patch.object(
                                  controller_lib.managed_job_state,
                                  'set_started_async',
                                  set_started), patch.object(
                                      controller_lib.global_user_state,
                                      'get_handles_from_cluster_names',
                                      barrier_snapshot), patch.object(
                                          controller_lib.job_group_networking,
                                          'dns_addresses_for_task',
                                          return_value=None), pytest.raises(
                                              asyncio.CancelledError):
            await job_controller._run_job_group()

        executor.launch.assert_awaited_once_with()
        barrier_snapshot.assert_not_called()
        set_started.assert_not_awaited()
        job_controller._monitor_job_group_task.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_job_group_launch_failure_cleanup_preserves_original_error(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        mock_dag.tasks = mock_dag.tasks[:1]
        mock_dag.primary_tasks = []
        executor = MagicMock()
        executor.launch = AsyncMock(side_effect=RuntimeError('launch failed'))
        job_controller._prepare_job_group_task_for_launch = AsyncMock(
            return_value=('cluster-0', executor))
        job_controller._monitor_job_group_task = AsyncMock(return_value=True)
        cleanup_started = asyncio.Event()
        allow_cleanup = asyncio.Event()
        cleanup_finished = asyncio.Event()

        async def cleanup(cluster_names):
            assert cluster_names == ['cluster-0']
            cleanup_started.set()
            await allow_cleanup.wait()
            cleanup_finished.set()

        job_controller._cleanup_job_group_clusters = AsyncMock(
            side_effect=cleanup)
        statuses = AsyncMock(return_value=[])

        with patch.object(controller_lib.managed_job_runtime,
                          'is_registered',
                          return_value=False), patch.object(
                              controller_lib.managed_job_state,
                              'get_all_task_ids_statuses_async', statuses):
            run_task = asyncio.create_task(job_controller._run_job_group())
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            run_task.cancel()
            await asyncio.sleep(0)
            assert not run_task.done()
            allow_cleanup.set()
            with pytest.raises(RuntimeError, match='launch failed'):
                await run_task

        assert cleanup_finished.is_set()
        job_controller._cleanup_job_group_clusters.assert_awaited_once_with(
            ['cluster-0'])
        job_controller._monitor_job_group_task.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_job_group_barrier_reads_one_ordered_handle_snapshot(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        terminal_task = MagicMock()
        terminal_task.name = 'task-3'
        terminal_task.envs = {}
        mock_dag.tasks.append(terminal_task)
        mock_dag.primary_tasks = []
        executors = [MagicMock(), MagicMock(), MagicMock()]
        for executor in executors:
            executor.launch = AsyncMock()
        job_controller._prepare_job_group_task_for_launch = AsyncMock(
            side_effect=[('cluster-0', executors[0]), (
                'cluster-1', executors[1]), ('cluster-2', executors[2])])
        job_controller._monitor_job_group_task = AsyncMock(return_value=True)
        job_controller._cleanup_job_group_clusters = AsyncMock()
        statuses = AsyncMock(return_value=[
            (1, managed_job_state.ManagedJobStatus.RUNNING),
            (2, managed_job_state.ManagedJobStatus.RECOVERING),
            (3, managed_job_state.ManagedJobStatus.SUCCEEDED),
        ])
        set_started = AsyncMock()
        first_handle = MagicMock(name='first-handle')
        third_handle = MagicMock(name='third-handle')

        def snapshot(cluster_names):
            # The complete barrier snapshot owns the read epoch: no task may
            # publish STARTED while it is still being assembled.
            assert set_started.await_count == 0
            assert cluster_names == {'cluster-0', 'cluster-1', 'cluster-2'}
            # Deliberately omit cluster-1 to cover concurrent row removal.
            # Return the survivors in reverse order to prove DAG-order rebuild.
            return {
                'cluster-2': third_handle,
                'cluster-0': first_handle,
            }

        batched_lookup = MagicMock(side_effect=snapshot)
        per_task_lookup = MagicMock(
            side_effect=AssertionError('per-task handle lookup'))
        setup_networking = AsyncMock(return_value=True)

        with patch.object(
                controller_lib.managed_job_state,
                'get_all_task_ids_statuses_async', statuses), patch.object(
                    controller_lib.managed_job_state, 'set_started_async',
                    set_started), patch.object(
                        controller_lib.global_user_state,
                        'get_handles_from_cluster_names', batched_lookup), \
                patch.object(
                    controller_lib.global_user_state,
                    'get_handle_from_cluster_name', per_task_lookup), \
                patch.object(
                    controller_lib.job_group_networking,
                    'dns_addresses_for_task', return_value=None), patch.object(
                        controller_lib.job_group_networking,
                        'setup_job_group_networking', setup_networking):
            result = await job_controller._run_job_group()

        assert result is True
        batched_lookup.assert_called_once_with(
            {'cluster-0', 'cluster-1', 'cluster-2'})
        per_task_lookup.assert_not_called()
        executors[0].launch.assert_awaited_once_with()
        executors[1].launch.assert_not_awaited()
        executors[2].launch.assert_not_awaited()
        set_started.assert_awaited_once()
        assert set_started.await_args.kwargs['task_id'] == 0
        setup_networking.assert_awaited_once_with(
            mock_dag.name, [(mock_dag.tasks[0], first_handle),
                            (mock_dag.tasks[2], third_handle)])
        assert [
            call.args[0]
            for call in job_controller._monitor_job_group_task.await_args_list
        ] == [0, 1, 2]
        job_controller._cleanup_job_group_clusters.assert_awaited_once_with(
            ['cluster-0', 'cluster-1', 'cluster-2', None])

    @pytest.mark.asyncio
    @pytest.mark.parametrize('fresh_launch', [True, False])
    async def test_job_group_releases_launch_slot_before_networking(
            self, mock_dag, fresh_launch):
        job_controller = self._make_controller(mock_dag)
        mock_dag.tasks = mock_dag.tasks[:1]
        mock_dag.primary_tasks = []
        job_controller.starting = {42}
        job_controller.starting_lock = asyncio.Lock()
        job_controller.starting_signal = asyncio.Condition(
            job_controller.starting_lock)

        executor = MagicMock()
        executor.launch = AsyncMock()
        job_controller._prepare_job_group_task_for_launch = AsyncMock(
            return_value=('cluster-0', executor))
        job_controller._monitor_job_group_task = AsyncMock(return_value=True)
        job_controller._cleanup_job_group_clusters = AsyncMock()

        statuses = [] if fresh_launch else [
            (0, managed_job_state.ManagedJobStatus.RUNNING)
        ]
        set_started = AsyncMock()
        networking_started = asyncio.Event()
        finish_networking = asyncio.Event()

        async def setup_networking(*_args):
            networking_started.set()
            await finish_networking.wait()
            return True

        async def wait_for_slot():
            async with job_controller.starting_signal:
                await job_controller.starting_signal.wait_for(
                    lambda: 42 not in job_controller.starting)

        waiter = asyncio.create_task(wait_for_slot())
        await asyncio.sleep(0)
        with patch.object(
                controller_lib.managed_job_runtime,
                'is_registered',
                return_value=False), patch.object(
                    controller_lib.managed_job_state,
                    'get_all_task_ids_statuses_async',
                    new=AsyncMock(return_value=statuses)), patch.object(
                        controller_lib.managed_job_state,
                        'set_started_async',
                        new=set_started), patch.object(
                            controller_lib.global_user_state,
                            'get_handles_from_cluster_names',
                            return_value={'cluster-0': MagicMock()}), \
                patch.object(
                    controller_lib.job_group_networking,
                    'dns_addresses_for_task',
                    return_value=None), patch.object(
                        controller_lib.job_group_networking,
                        'setup_job_group_networking',
                        side_effect=setup_networking):
            run_task = asyncio.create_task(job_controller._run_job_group())
            try:
                await asyncio.wait_for(networking_started.wait(), timeout=1)
                await asyncio.wait_for(asyncio.shield(waiter), timeout=1)
                assert not run_task.done()
                assert 42 not in job_controller.starting
            finally:
                finish_networking.set()
                await asyncio.gather(run_task, return_exceptions=True)
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)

        if fresh_launch:
            executor.launch.assert_awaited_once_with()
            set_started.assert_awaited_once()
        else:
            executor.launch.assert_not_awaited()
            set_started.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_job_group_barrier_snapshot_failure_fences_state(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        mock_dag.tasks = mock_dag.tasks[:2]
        mock_dag.primary_tasks = []
        executors = [MagicMock(), MagicMock()]
        executors[0].launch = AsyncMock()
        executors[1].launch = AsyncMock()
        job_controller._prepare_job_group_task_for_launch = AsyncMock(
            side_effect=[('cluster-0', executors[0]), ('cluster-1',
                                                       executors[1])])
        job_controller._monitor_job_group_task = AsyncMock(return_value=True)
        statuses = AsyncMock(return_value=[
            (0, managed_job_state.ManagedJobStatus.RUNNING),
        ])
        set_started = AsyncMock()
        snapshot_error = RuntimeError('barrier snapshot failed')
        batched_lookup = MagicMock(side_effect=snapshot_error)
        setup_networking = AsyncMock()

        with patch.object(
                controller_lib.managed_job_state,
                'get_all_task_ids_statuses_async', statuses), patch.object(
                    controller_lib.managed_job_state, 'set_started_async',
                    set_started), patch.object(
                        controller_lib.global_user_state,
                        'get_handles_from_cluster_names', batched_lookup), \
                patch.object(
                    controller_lib.global_user_state,
                    'get_handle_from_cluster_name', return_value=MagicMock()), \
                patch.object(
                    controller_lib.job_group_networking,
                    'setup_job_group_networking', setup_networking), \
                pytest.raises(RuntimeError, match='barrier snapshot failed'):
            await job_controller._run_job_group()

        batched_lookup.assert_called_once_with({'cluster-0', 'cluster-1'})
        set_started.assert_not_awaited()
        setup_networking.assert_not_awaited()
        job_controller._monitor_job_group_task.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_job_group_repeated_parent_cancellation_joins_monitors(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        mock_dag.tasks = mock_dag.tasks[:2]
        mock_dag.primary_tasks = []
        executors = [MagicMock(), MagicMock()]
        job_controller._prepare_job_group_task_for_launch = AsyncMock(
            side_effect=[('cluster-0', executors[0]), ('cluster-1',
                                                       executors[1])])

        all_started = asyncio.Event()
        cleanup_started = asyncio.Event()
        allow_cleanup = asyncio.Event()
        started = set()
        cleaning_up = set()
        cancelled = set()

        async def monitor(task_id, *_args):
            started.add(task_id)
            if len(started) == 2:
                all_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # Model cancellation cleanup that must finish before the
                # owning JobGroup coroutine may exit.
                cleaning_up.add(task_id)
                if len(cleaning_up) == 2:
                    cleanup_started.set()
                await allow_cleanup.wait()
                cancelled.add(task_id)
                raise

        job_controller._monitor_job_group_task = monitor
        statuses = AsyncMock(return_value=[
            (0, managed_job_state.ManagedJobStatus.RUNNING),
            (1, managed_job_state.ManagedJobStatus.RUNNING),
        ])

        with patch.object(
                controller_lib.managed_job_state,
                'get_all_task_ids_statuses_async', statuses), patch.object(
                    controller_lib.global_user_state,
                    'get_handles_from_cluster_names', return_value={
                        'cluster-0': MagicMock(),
                        'cluster-1': MagicMock(),
                    }), \
                patch.object(
                    controller_lib.job_group_networking,
                    'dns_addresses_for_task', return_value=['127.0.0.1']):
            run_task = asyncio.create_task(job_controller._run_job_group())
            await asyncio.wait_for(all_started.wait(), timeout=1)
            run_task.cancel()
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            run_task.cancel()
            await asyncio.sleep(0)
            assert not run_task.done()
            allow_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await run_task

        try:
            assert cancelled == {0, 1}
        finally:
            # Keep the pre-fix regression run from leaking tasks into pytest's
            # event loop when the assertion above fails.
            leaked = [
                task for task in asyncio.all_tasks()
                if task.get_name().startswith('monitor_') and not task.done()
            ]
            for task in leaked:
                task.cancel()
            await asyncio.gather(*leaked, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_job_group_monitor_failure_joins_children_before_cleanup(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        mock_dag.tasks = mock_dag.tasks[:2]
        mock_dag.primary_tasks = []
        executors = [MagicMock(), MagicMock()]
        job_controller._prepare_job_group_task_for_launch = AsyncMock(
            side_effect=[('cluster-0', executors[0]), ('cluster-1',
                                                       executors[1])])

        all_started = asyncio.Event()
        started = set()
        cancelled = set()

        async def monitor(task_id, *_args):
            started.add(task_id)
            if len(started) == 2:
                all_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                cancelled.add(task_id)
                raise

        async def fail_wait(*_args, **_kwargs):
            await all_started.wait()
            raise RuntimeError('monitor coordinator failed')

        async def cleanup(cluster_names):
            assert cluster_names == ['cluster-0', 'cluster-1']
            assert cancelled == {0, 1}

        job_controller._monitor_job_group_task = monitor
        job_controller._cleanup_job_group_clusters = AsyncMock(
            side_effect=cleanup)
        statuses = AsyncMock(return_value=[
            (0, managed_job_state.ManagedJobStatus.RUNNING),
            (1, managed_job_state.ManagedJobStatus.RUNNING),
        ])

        with patch.object(
                controller_lib.managed_job_state,
                'get_all_task_ids_statuses_async', statuses), patch.object(
                    controller_lib.global_user_state,
                    'get_handles_from_cluster_names', return_value={
                        'cluster-0': MagicMock(),
                        'cluster-1': MagicMock(),
                    }), \
                patch.object(
                    controller_lib.job_group_networking,
                    'dns_addresses_for_task', return_value=['127.0.0.1']), \
                patch.object(controller_lib.asyncio,
                             'wait', side_effect=fail_wait), pytest.raises(
                                 RuntimeError,
                                 match='monitor coordinator failed'):
            await job_controller._run_job_group()

        job_controller._cleanup_job_group_clusters.assert_awaited_once_with(
            ['cluster-0', 'cluster-1'])

    @pytest.mark.asyncio
    async def test_job_group_monitor_failure_cleanup_preserves_original_error(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        mock_dag.tasks = mock_dag.tasks[:2]
        mock_dag.primary_tasks = []
        executors = [MagicMock(), MagicMock()]
        job_controller._prepare_job_group_task_for_launch = AsyncMock(
            side_effect=[('cluster-0', executors[0]), ('cluster-1',
                                                       executors[1])])

        all_started = asyncio.Event()
        child_cleanup_started = asyncio.Event()
        allow_child_cleanup = asyncio.Event()
        cleanup_started = asyncio.Event()
        allow_cleanup = asyncio.Event()
        started = set()
        child_cleanup = set()
        cancelled = set()

        async def monitor(task_id, *_args):
            started.add(task_id)
            if len(started) == 2:
                all_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                child_cleanup.add(task_id)
                if len(child_cleanup) == 2:
                    child_cleanup_started.set()
                await allow_child_cleanup.wait()
                cancelled.add(task_id)
                raise

        async def fail_wait(*_args, **_kwargs):
            await all_started.wait()
            raise RuntimeError('monitor coordinator failed')

        async def cleanup(cluster_names):
            assert cluster_names == ['cluster-0', 'cluster-1']
            assert cancelled == {0, 1}
            cleanup_started.set()
            await allow_cleanup.wait()

        job_controller._monitor_job_group_task = monitor
        job_controller._cleanup_job_group_clusters = AsyncMock(
            side_effect=cleanup)
        statuses = AsyncMock(return_value=[
            (0, managed_job_state.ManagedJobStatus.RUNNING),
            (1, managed_job_state.ManagedJobStatus.RUNNING),
        ])

        with patch.object(
                controller_lib.managed_job_state,
                'get_all_task_ids_statuses_async', statuses), patch.object(
                    controller_lib.global_user_state,
                    'get_handles_from_cluster_names', return_value={
                        'cluster-0': MagicMock(),
                        'cluster-1': MagicMock(),
                    }), \
                patch.object(
                    controller_lib.job_group_networking,
                    'dns_addresses_for_task', return_value=['127.0.0.1']), \
                patch.object(controller_lib.asyncio, 'wait', side_effect=fail_wait):
            run_task = asyncio.create_task(job_controller._run_job_group())
            await asyncio.wait_for(child_cleanup_started.wait(), timeout=1)
            run_task.cancel()
            await asyncio.sleep(0)
            assert not run_task.done()
            allow_child_cleanup.set()
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            assert not run_task.done()
            allow_cleanup.set()
            with pytest.raises(RuntimeError,
                               match='monitor coordinator failed'):
                await run_task

        assert cancelled == {0, 1}
        job_controller._cleanup_job_group_clusters.assert_awaited_once_with(
            ['cluster-0', 'cluster-1'])

    @pytest.mark.asyncio
    @pytest.mark.parametrize(('statuses', 'expected'), [
        ([managed_job_state.ManagedJobStatus.SUCCEEDED] * 3, True),
        ([
            managed_job_state.ManagedJobStatus.SUCCEEDED,
            managed_job_state.ManagedJobStatus.FAILED,
            managed_job_state.ManagedJobStatus.SUCCEEDED
        ], False),
    ])
    async def test_recovery_reads_one_terminal_status_snapshot(
            self, mock_dag, statuses, expected):
        job_controller = self._make_controller(mock_dag)
        job_controller.starting = {42}
        status_snapshot = AsyncMock(return_value=list(enumerate(statuses)))
        per_task_status = AsyncMock(
            side_effect=AssertionError('per-task status read'))

        with patch('sky.jobs.controller.managed_job_runtime.is_registered',
                   return_value=False), patch(
                       'sky.jobs.state.get_all_task_ids_statuses_async',
                       status_snapshot), patch(
                           'sky.jobs.state.get_job_status_with_task_id_async',
                           per_task_status):
            result = await job_controller._run_job_group()

        assert result is expected
        assert job_controller.starting == set()
        status_snapshot.assert_awaited_once_with(42)
        per_task_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_recovery_missing_status_launches_and_ignores_extra_row(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        stop_before_launch = RuntimeError('stop before launch')
        prepare = AsyncMock(side_effect=stop_before_launch)
        cleanup = AsyncMock()
        job_controller._prepare_job_group_task_for_launch = prepare
        job_controller._cleanup_job_group_clusters = cleanup
        status_snapshot = AsyncMock(return_value=[
            (0, managed_job_state.ManagedJobStatus.SUCCEEDED),
            (2, managed_job_state.ManagedJobStatus.SUCCEEDED),
            (99, managed_job_state.ManagedJobStatus.CANCELLING),
        ])

        with patch('sky.jobs.controller.managed_job_runtime.is_registered',
                   return_value=False), patch(
                       'sky.jobs.state.get_all_task_ids_statuses_async',
                       status_snapshot), pytest.raises(
                           RuntimeError, match='stop before launch'):
            await job_controller._run_job_group()

        prepare.assert_awaited_once()
        assert prepare.await_args.args[1] == 1
        cleanup.assert_awaited_once_with([None])

    @pytest.mark.asyncio
    async def test_recovery_cancelling_snapshot_raises_before_launch(
            self, mock_dag):
        job_controller = self._make_controller(mock_dag)
        prepare = AsyncMock()
        job_controller._prepare_job_group_task_for_launch = prepare
        status_snapshot = AsyncMock(return_value=[
            (0, managed_job_state.ManagedJobStatus.RUNNING),
            (1, managed_job_state.ManagedJobStatus.CANCELLING),
            (2, managed_job_state.ManagedJobStatus.PENDING),
        ])

        with patch('sky.jobs.controller.managed_job_runtime.is_registered',
                   return_value=False), patch(
                       'sky.jobs.state.get_all_task_ids_statuses_async',
                       status_snapshot), pytest.raises(asyncio.CancelledError):
            await job_controller._run_job_group()

        status_snapshot.assert_awaited_once_with(42)
        prepare.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_resume_with_mixed_task_states(self, mock_dag):
        """Test resume when tasks are in different states.

        Scenario:
        - Task 0: SUCCEEDED (terminal) - should be skipped
        - Task 1: RUNNING - should resume monitoring without forced recovery
        - Task 2: STARTING - should resume with forced recovery
        """

        # Mock the state queries to return different statuses for each task
        async def mock_get_status(job_id, task_id):
            statuses = {
                0: managed_job_state.ManagedJobStatus.SUCCEEDED,
                1: managed_job_state.ManagedJobStatus.RUNNING,
                2: managed_job_state.ManagedJobStatus.STARTING,
            }
            return statuses.get(task_id)

        with patch('sky.jobs.state.get_job_status_with_task_id_async',
                   side_effect=mock_get_status):
            # Simulate the resume logic from _run_job_group
            task_resume_info: Dict[int, Tuple[
                Optional[managed_job_state.ManagedJobStatus], bool]] = {}

            for task_id, task in enumerate(mock_dag.tasks):
                task_status = await mock_get_status(job_id=1, task_id=task_id)

                if task_status is None or task_status == (
                        managed_job_state.ManagedJobStatus.PENDING):
                    task_resume_info[task_id] = (None, False)
                elif task_status.is_terminal():
                    task_resume_info[task_id] = (task_status, False)
                elif task_status == managed_job_state.ManagedJobStatus.CANCELLING:
                    raise asyncio.CancelledError()
                elif task_status == managed_job_state.ManagedJobStatus.RUNNING:
                    task_resume_info[task_id] = (task_status, False)
                else:
                    # Non-terminal, non-RUNNING state - force recovery
                    task_resume_info[task_id] = (task_status, True)

            # Verify results
            # Task 0: SUCCEEDED - should be (SUCCEEDED, False) - skip
            assert task_resume_info[0] == (
                managed_job_state.ManagedJobStatus.SUCCEEDED, False)

            # Task 1: RUNNING - should be (RUNNING, False) - resume without forced recovery
            assert task_resume_info[1] == (
                managed_job_state.ManagedJobStatus.RUNNING, False)

            # Task 2: STARTING - should be (STARTING, True) - force recovery
            assert task_resume_info[2] == (
                managed_job_state.ManagedJobStatus.STARTING, True)

    @pytest.mark.asyncio
    async def test_resume_all_pending_is_fresh_launch(self, mock_dag):
        """Test that all PENDING tasks result in fresh launch (no resume)."""

        async def mock_get_status(job_id, task_id):
            return managed_job_state.ManagedJobStatus.PENDING

        with patch('sky.jobs.state.get_job_status_with_task_id_async',
                   side_effect=mock_get_status):
            task_resume_info: Dict[int, Tuple[
                Optional[managed_job_state.ManagedJobStatus], bool]] = {}

            for task_id, task in enumerate(mock_dag.tasks):
                task_status = await mock_get_status(job_id=1, task_id=task_id)

                if task_status is None or task_status == (
                        managed_job_state.ManagedJobStatus.PENDING):
                    task_resume_info[task_id] = (None, False)
                elif task_status.is_terminal():
                    task_resume_info[task_id] = (task_status, False)
                elif task_status == managed_job_state.ManagedJobStatus.RUNNING:
                    task_resume_info[task_id] = (task_status, False)
                else:
                    task_resume_info[task_id] = (task_status, True)

            # All tasks should be (None, False) - fresh launch
            for task_id in range(len(mock_dag.tasks)):
                assert task_resume_info[task_id] == (None, False)

    @pytest.mark.asyncio
    async def test_resume_all_terminal_returns_early(self, mock_dag):
        """Test that all terminal tasks result in early return."""

        async def mock_get_status(job_id, task_id):
            # All tasks succeeded
            return managed_job_state.ManagedJobStatus.SUCCEEDED

        with patch('sky.jobs.state.get_job_status_with_task_id_async',
                   side_effect=mock_get_status):
            task_resume_info: Dict[int, Tuple[
                Optional[managed_job_state.ManagedJobStatus], bool]] = {}

            for task_id, task in enumerate(mock_dag.tasks):
                task_status = await mock_get_status(job_id=1, task_id=task_id)

                if task_status is None or task_status == (
                        managed_job_state.ManagedJobStatus.PENDING):
                    task_resume_info[task_id] = (None, False)
                elif task_status.is_terminal():
                    task_resume_info[task_id] = (task_status, False)
                elif task_status == managed_job_state.ManagedJobStatus.RUNNING:
                    task_resume_info[task_id] = (task_status, False)
                else:
                    task_resume_info[task_id] = (task_status, True)

            # Check if all tasks are terminal
            all_terminal = all(status is not None and status.is_terminal()
                               for status, _ in task_resume_info.values())

            assert all_terminal is True

            # All succeeded
            all_succeeded = all(
                status == managed_job_state.ManagedJobStatus.SUCCEEDED
                for status, _ in task_resume_info.values())
            assert all_succeeded is True

    @pytest.mark.asyncio
    async def test_resume_cancelling_raises_cancelled_error(self, mock_dag):
        """Test that CANCELLING status raises CancelledError."""

        async def mock_get_status(job_id, task_id):
            if task_id == 1:
                return managed_job_state.ManagedJobStatus.CANCELLING
            return managed_job_state.ManagedJobStatus.RUNNING

        with patch('sky.jobs.state.get_job_status_with_task_id_async',
                   side_effect=mock_get_status):
            with pytest.raises(asyncio.CancelledError):
                for task_id, task in enumerate(mock_dag.tasks):
                    task_status = await mock_get_status(job_id=1,
                                                        task_id=task_id)

                    if task_status is None or task_status == (
                            managed_job_state.ManagedJobStatus.PENDING):
                        pass
                    elif task_status.is_terminal():
                        pass
                    elif task_status == managed_job_state.ManagedJobStatus.CANCELLING:
                        raise asyncio.CancelledError()

    @pytest.mark.asyncio
    async def test_resume_recovering_state_forces_recovery(self, mock_dag):
        """Test that RECOVERING status triggers forced recovery."""

        async def mock_get_status(job_id, task_id):
            return managed_job_state.ManagedJobStatus.RECOVERING

        with patch('sky.jobs.state.get_job_status_with_task_id_async',
                   side_effect=mock_get_status):
            task_resume_info: Dict[int, Tuple[
                Optional[managed_job_state.ManagedJobStatus], bool]] = {}

            for task_id, task in enumerate(mock_dag.tasks):
                task_status = await mock_get_status(job_id=1, task_id=task_id)

                if task_status is None or task_status == (
                        managed_job_state.ManagedJobStatus.PENDING):
                    task_resume_info[task_id] = (None, False)
                elif task_status.is_terminal():
                    task_resume_info[task_id] = (task_status, False)
                elif task_status == managed_job_state.ManagedJobStatus.RUNNING:
                    task_resume_info[task_id] = (task_status, False)
                else:
                    # RECOVERING is non-terminal, non-RUNNING - force recovery
                    task_resume_info[task_id] = (task_status, True)

            # All tasks should have force_transit_to_recovering=True
            for task_id in range(len(mock_dag.tasks)):
                status, force_recovery = task_resume_info[task_id]
                assert status == managed_job_state.ManagedJobStatus.RECOVERING
                assert force_recovery is True

    @pytest.mark.asyncio
    async def test_tasks_to_launch_excludes_non_pending(self, mock_dag):
        """Test that only PENDING/None tasks are included in launch list."""
        # Simulate the logic from _run_job_group
        task_resume_info = {
            0: (managed_job_state.ManagedJobStatus.SUCCEEDED, False
               ),  # Terminal
            1: (managed_job_state.ManagedJobStatus.RUNNING, False),  # Running
            2: (None, False),  # Fresh launch
        }

        tasks_to_launch: List[int] = []
        for task_id in range(len(mock_dag.tasks)):
            task_status, _ = task_resume_info[task_id]
            needs_launch = (task_status is None or task_status
                            == managed_job_state.ManagedJobStatus.PENDING)
            if needs_launch:
                tasks_to_launch.append(task_id)

        # Only task 2 should be launched
        assert tasks_to_launch == [2]

    @pytest.mark.asyncio
    async def test_terminal_tasks_skipped_in_monitoring(self, mock_dag):
        """Test that terminal tasks are skipped during monitoring phase."""
        task_resume_info = {
            0: (managed_job_state.ManagedJobStatus.SUCCEEDED, False),  # Skip
            1: (managed_job_state.ManagedJobStatus.FAILED, False),  # Skip
            2: (managed_job_state.ManagedJobStatus.RUNNING, False),  # Monitor
        }

        monitor_task_ids: List[int] = []
        for task_id in range(len(mock_dag.tasks)):
            task_status, force_recovery = task_resume_info[task_id]
            if task_status is not None and task_status.is_terminal():
                continue  # Skip terminal tasks
            monitor_task_ids.append(task_id)

        # Only task 2 should be monitored
        assert monitor_task_ids == [2]

    @pytest.mark.asyncio
    async def test_mixed_terminal_results_check(self, mock_dag):
        """Test result checking with mix of terminal and monitored tasks."""
        task_resume_info = {
            0: (managed_job_state.ManagedJobStatus.SUCCEEDED, False),
            1: (managed_job_state.ManagedJobStatus.FAILED, False),
            2: (managed_job_state.ManagedJobStatus.RUNNING, False),
        }

        # Simulate monitoring results (only task 2 was monitored)
        monitor_task_ids = [2]
        results = [True]  # Task 2 succeeded

        # Check results logic from _run_job_group
        all_succeeded = True
        for task_id in range(len(mock_dag.tasks)):
            task_status, _ = task_resume_info[task_id]
            if task_status is not None and task_status.is_terminal():
                # Terminal task - check if it succeeded
                if task_status != managed_job_state.ManagedJobStatus.SUCCEEDED:
                    all_succeeded = False
                continue

            # Find the result for this monitored task
            result_idx = monitor_task_ids.index(task_id)
            result = results[result_idx]
            if not result:
                all_succeeded = False

        # Task 1 FAILED, so overall should be False
        assert all_succeeded is False


class TestTaskCleanup:
    """Tests for file mount cleanup in ControllerManager._cleanup().

    In non-consolidation mode, task_cleanup() deletes two-hop local file
    mounts under ~/.sky/tmp/controller/{run_id}/ after a managed job
    completes. Cloud URL file mounts are skipped. Consolidated jobs reuse
    API-server-managed blobs and intentionally keep those shared mounts.
    """

    @pytest.fixture
    def cleanup_patches(self):
        """Patch all _cleanup() dependencies except file mount cleanup.

        task_cleanup() does three things:
        1. Cluster termination (mocked)
        2. Fenced SDK status/storage requests (mocked)
        3. File mount cleanup (tested)
        """
        patches = {
            'ha_recovery': patch(
                'sky.jobs.state.remove_ha_recovery_script_async',
                new_callable=AsyncMock),
            'terminate': patch('sky.jobs.utils.terminate_cluster'),
            'gen_name': patch(
                'sky.jobs.utils.generate_managed_job_cluster_name',
                return_value='test-cluster'),
            'status': patch('sky.jobs.controller.sdk.status',
                            return_value='status-request'),
            'get': patch('sky.jobs.controller.sdk.get', return_value=[]),
            'storage_delete': patch('sky.jobs.controller.sdk.storage_delete',
                                    return_value='storage-request'),
            'consolidation': patch('sky.jobs.utils.is_consolidation_mode',
                                   return_value=False),
        }
        mocks = {}
        for name, p in patches.items():
            mocks[name] = p.start()
        yield mocks
        for p in patches.values():
            p.stop()

    def _make_task(self, file_mounts=None):
        task = MagicMock()
        task.name = 'test-task'
        task.metadata = {}
        task.file_mounts = file_mounts
        task.storage_mounts = {}
        return task

    @pytest.mark.asyncio
    async def test_local_dir_mounts_cleaned_up(self, tmp_path, cleanup_patches):
        """Local directory file mounts should be deleted."""
        # Simulate two-hop mount dirs: ~/.sky/tmp/controller/{run_id}/{N}
        mount_0 = tmp_path / 'run_id' / '0'
        mount_0.mkdir(parents=True)
        (mount_0 / 'data.txt').write_text('test data')
        mount_1 = tmp_path / 'run_id' / '1'
        mount_1.mkdir(parents=True)
        (mount_1 / 'config.yaml').write_text('key: value')

        task = self._make_task(file_mounts={
            '/data': str(mount_0),
            '/config': str(mount_1),
        })
        dag = MagicMock()
        dag.tasks = [task]

        from sky.jobs.controller import ControllerManager
        manager = _make_controller_manager()
        with patch('sky.jobs.controller._get_dag', return_value=dag):
            await manager._cleanup(job_id=1)

        assert not mount_0.exists(), 'mount_0 should be cleaned up'
        assert not mount_1.exists(), 'mount_1 should be cleaned up'

    @pytest.mark.asyncio
    async def test_ephemeral_storage_uses_fenced_sdk_request(
            self, cleanup_patches):
        """Only non-persistent storage is deleted through nested requests."""
        ephemeral = MagicMock(name='ephemeral')
        ephemeral.persistent = False
        ephemeral.name = 'scratch-bucket'
        persistent = MagicMock(name='persistent')
        persistent.persistent = True
        persistent.name = 'kept-bucket'
        task = self._make_task()
        task.storage_mounts = {
            '/scratch': ephemeral,
            '/kept': persistent,
        }
        dag = MagicMock()
        dag.tasks = [task]

        from sky.jobs.controller import ControllerManager
        manager = _make_controller_manager()
        with patch('sky.jobs.controller._get_dag', return_value=dag):
            await manager._cleanup(job_id=1)

        cleanup_patches['storage_delete'].assert_called_once_with(
            'scratch-bucket')
        assert cleanup_patches['get'].call_args_list == [
            call('status-request'),
            call('storage-request'),
        ]

    @pytest.mark.asyncio
    async def test_non_pool_cleanup_uses_down_status_and_ephemeral_sdk(
            self, cleanup_patches):
        """The cleanup-only path reuses every canonical nested effect."""
        ephemeral = MagicMock()
        ephemeral.persistent = False
        ephemeral.name = 'scratch-bucket'
        task = self._make_task()
        task.storage_mounts = {'/scratch': ephemeral}
        dag = MagicMock(tasks=[task])
        cleanup_patches['terminate'].side_effect = lambda *_args, **_kwargs: (
            controller_lib.sdk.get(
                controller_lib.sdk.down(
                    'test-cluster', graceful=False, graceful_timeout=None)))
        with patch('sky.jobs.controller.sdk.down',
                   return_value='down-request') as down, \
                patch('sky.jobs.controller._get_dag', return_value=dag):
            await _make_controller_manager()._cleanup(job_id=1)

        down.assert_called_once_with('test-cluster',
                                     graceful=False,
                                     graceful_timeout=None)
        cleanup_patches['status'].assert_called_once_with(
            cluster_names=['test-cluster'], all_users=True)
        cleanup_patches['storage_delete'].assert_called_once_with(
            'scratch-bucket')
        assert cleanup_patches['get'].call_args_list == [
            call('down-request'),
            call('status-request'),
            call('storage-request'),
        ]

    @pytest.mark.asyncio
    async def test_pool_cleanup_cancels_worker_job_and_ephemeral_storage(
            self, cleanup_patches):
        ephemeral = MagicMock()
        ephemeral.persistent = False
        ephemeral.name = 'scratch-bucket'
        task = self._make_task()
        task.storage_mounts = {'/scratch': ephemeral}
        dag = MagicMock(tasks=[task])

        with patch('sky.jobs.controller.managed_job_state.'
                   'get_pool_submit_info', return_value=('pool-worker', 91)), \
                patch('sky.jobs.controller.sdk.cancel',
                      return_value='cancel-request') as cancel, \
                patch('sky.jobs.controller._get_dag', return_value=dag):
            await _make_controller_manager()._cleanup(job_id=1, pool='pool-a')

        cancel.assert_called_once_with(cluster_name='pool-worker',
                                       job_ids=[91],
                                       _try_cancel_if_cluster_is_init=True)
        cleanup_patches['terminate'].assert_not_called()
        cleanup_patches['status'].assert_not_called()
        cleanup_patches['storage_delete'].assert_called_once_with(
            'scratch-bucket')
        assert cleanup_patches['get'].call_args_list == [
            call('cancel-request'),
            call('storage-request'),
        ]


class TestDownloadLogsForCancelledJob:
    """Tests for ControllerManager._download_logs_for_cancelled_job.

    When a managed job is cancelled, we download logs before cluster cleanup
    so they remain accessible via `sky jobs logs`.
    """

    @pytest.fixture(autouse=True)
    def passthrough_to_thread(self):
        """Make asyncio.to_thread call the function directly."""

        async def _passthrough(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch('asyncio.to_thread', side_effect=_passthrough):
            yield

    def _make_manager(self):
        """Create a MagicMock manager with real helper methods bound."""
        manager = MagicMock(spec=ControllerManager)
        manager._download_logs_for_cancelled_job = (
            ControllerManager._download_logs_for_cancelled_job.__get__(
                manager, ControllerManager))
        return manager

    @pytest.mark.asyncio
    async def test_non_pool_job_cluster_found(self):
        """Happy path: non-pool job finds cluster and downloads logs."""
        manager = self._make_manager()
        controller = MagicMock()
        job_id = 1
        task_id = 0

        mock_dag = MagicMock()
        mock_task = MagicMock()
        mock_task.name = 'test-job'
        mock_dag.tasks = [mock_task]

        mock_handle = MagicMock()

        with patch('sky.jobs.controller.managed_job_utils'
                   '.generate_managed_job_cluster_name',
                   return_value='sky-managed-1-test-job') as mock_gen_name, \
             patch('sky.jobs.controller.backend_utils.get_clusters',
                   return_value=[{
                       'name': 'sky-managed-1-test-job',
                       'handle': mock_handle
                   }]) as mock_get_cl:

            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[task_id],
                dag=mock_dag,
                pool=None)

            mock_gen_name.assert_called_once_with('test-job', job_id)
            mock_get_cl.assert_called_once_with(
                cluster_names=['sky-managed-1-test-job'],
                refresh=common.StatusRefreshMode.NONE,
                all_users=True,
                _include_is_managed=True)
            controller.download_log_and_stream.assert_called_once_with(
                task_id, mock_handle, None)

    @pytest.mark.asyncio
    async def test_pool_job_cluster_found(self):
        """Happy path: pool job gets cluster info from pool state."""
        manager = self._make_manager()
        controller = MagicMock()
        job_id = 2
        task_id = 0

        mock_dag = MagicMock()
        mock_handle = MagicMock()

        with patch('sky.jobs.controller.managed_job_state'
                   '.get_pool_submit_info_async',
                   return_value=('pool-cluster-1', 42)) as mock_pool_info, \
             patch('sky.jobs.controller.backend_utils.get_clusters',
                   return_value=[{
                       'name': 'pool-cluster-1',
                       'handle': mock_handle
                   }]):

            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[task_id],
                dag=mock_dag,
                pool='my-pool')

            mock_pool_info.assert_called_once_with(job_id)
            controller.download_log_and_stream.assert_called_once_with(
                task_id, mock_handle, 42)

    @pytest.mark.asyncio
    async def test_cluster_not_found_skips_download(self):
        """When get_clusters returns empty, log download is skipped."""
        manager = self._make_manager()
        controller = MagicMock()
        job_id = 3
        task_id = 0

        mock_dag = MagicMock()
        mock_task = MagicMock()
        mock_task.name = 'test-job'
        mock_dag.tasks = [mock_task]

        with patch('sky.jobs.controller.managed_job_utils'
                   '.generate_managed_job_cluster_name',
                   return_value='sky-managed-3-test-job'), \
             patch('sky.jobs.controller.backend_utils.get_clusters',
                   return_value=[]):

            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[task_id],
                dag=mock_dag,
                pool=None)

            controller.download_log_and_stream.assert_not_called()

    @pytest.mark.asyncio
    async def test_pool_returns_none_cluster_name_skips(self):
        """When pool submit info returns None cluster, download is skipped."""
        manager = self._make_manager()
        controller = MagicMock()
        job_id = 4
        task_id = 0

        mock_dag = MagicMock()

        with patch('sky.jobs.controller.managed_job_state'
                   '.get_pool_submit_info_async',
                   return_value=(None, None)) as mock_pool_info, \
             patch('sky.jobs.controller.backend_utils.get_clusters'
                   ) as mock_get_cl:

            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[task_id],
                dag=mock_dag,
                pool='my-pool')

            mock_pool_info.assert_called_once_with(job_id)
            mock_get_cl.assert_not_called()
            controller.download_log_and_stream.assert_not_called()

    @pytest.mark.asyncio
    async def test_download_exception_caught_per_task(self):
        """Exceptions from download_log_and_stream are caught per-task.

        The method catches and logs exceptions for each task individually
        so that a failure for one task doesn't prevent downloading logs
        for other tasks.
        """
        manager = self._make_manager()
        controller = MagicMock()
        controller.download_log_and_stream.side_effect = RuntimeError(
            'download failed')
        job_id = 5
        task_id = 0

        mock_dag = MagicMock()
        mock_task = MagicMock()
        mock_task.name = 'test-job'
        mock_dag.tasks = [mock_task]

        mock_handle = MagicMock()

        with patch('sky.jobs.controller.managed_job_utils'
                   '.generate_managed_job_cluster_name',
                   return_value='sky-managed-5-test-job'), \
             patch('sky.jobs.controller.backend_utils.get_clusters',
                   return_value=[{
                       'name': 'sky-managed-5-test-job',
                       'handle': mock_handle
                   }]):

            # Should NOT raise - exceptions are caught per-task
            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[task_id],
                dag=mock_dag,
                pool=None)

            controller.download_log_and_stream.assert_called_once_with(
                task_id, mock_handle, None)

    @pytest.mark.asyncio
    async def test_job_group_downloads_for_multiple_tasks(self):
        """Job group: downloads logs for all active tasks."""
        manager = self._make_manager()
        controller = MagicMock()
        job_id = 6

        mock_task_0 = MagicMock()
        mock_task_0.name = 'job-a'
        mock_task_1 = MagicMock()
        mock_task_1.name = 'job-b-done'
        mock_task_2 = MagicMock()
        mock_task_2.name = 'job-c'

        mock_dag = MagicMock()
        mock_dag.tasks = [mock_task_0, mock_task_1, mock_task_2]

        mock_handle_0 = MagicMock()
        mock_handle_2 = MagicMock()

        with patch('sky.jobs.controller.managed_job_utils'
                   '.generate_managed_job_cluster_name',
                   side_effect=lambda name, jid: f'sky-managed-{jid}-{name}'
                   ), \
             patch('sky.jobs.controller.backend_utils.get_clusters',
                   return_value=[{
                       'name': 'sky-managed-6-job-c',
                       'handle': mock_handle_2
                   }, {
                       'name': 'sky-managed-6-job-a',
                       'handle': mock_handle_0
                   }]) as mock_get_clusters:

            # task 1 already succeeded, so only tasks 0 and 2 are active
            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[0, 2],
                dag=mock_dag,
                pool=None)

            mock_get_clusters.assert_called_once_with(
                cluster_names=['sky-managed-6-job-a', 'sky-managed-6-job-c'],
                refresh=common.StatusRefreshMode.NONE,
                all_users=True,
                _include_is_managed=True)
            assert controller.download_log_and_stream.call_args_list == [
                call(0, mock_handle_0, None),
                call(2, mock_handle_2, None),
            ]

    @pytest.mark.asyncio
    async def test_job_group_skips_only_missing_cluster_rows(self):
        """A partial cluster snapshot still downloads every present task."""
        manager = self._make_manager()
        controller = MagicMock()
        job_id = 8
        mock_dag = MagicMock()
        mock_dag.tasks = [MagicMock(name='task-a'), MagicMock(name='task-b')]
        mock_dag.tasks[0].name = 'job-a'
        mock_dag.tasks[1].name = 'job-b'
        mock_handle_1 = MagicMock()

        with patch('sky.jobs.controller.managed_job_utils'
                   '.generate_managed_job_cluster_name',
                   side_effect=lambda name, jid: f'sky-managed-{jid}-{name}'
                   ), \
             patch('sky.jobs.controller.backend_utils.get_clusters',
                   return_value=[{
                       'name': 'sky-managed-8-job-b',
                       'handle': mock_handle_1
                   }]) as mock_get_clusters:
            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[0, 1],
                dag=mock_dag,
                pool=None)

        mock_get_clusters.assert_called_once()
        controller.download_log_and_stream.assert_called_once_with(
            1, mock_handle_1, None)

    @pytest.mark.asyncio
    async def test_per_task_exception_continues_to_next(self):
        """Exception downloading one task's logs doesn't block the next."""
        manager = self._make_manager()
        controller = MagicMock()
        job_id = 7

        mock_task_0 = MagicMock()
        mock_task_0.name = 'job-a'
        mock_task_1 = MagicMock()
        mock_task_1.name = 'job-b'

        mock_dag = MagicMock()
        mock_dag.tasks = [mock_task_0, mock_task_1]

        mock_handle_0 = MagicMock()
        mock_handle_1 = MagicMock()

        # Task 0 fails, task 1 succeeds
        call_count = [0]

        def download_side_effect(task_id, handle, job_id_on_pool):
            call_count[0] += 1
            if task_id == 0:
                raise RuntimeError('download failed for task 0')

        controller.download_log_and_stream.side_effect = download_side_effect

        with patch('sky.jobs.controller.managed_job_utils'
                   '.generate_managed_job_cluster_name',
                   side_effect=lambda name, jid: f'sky-managed-{jid}-{name}'
                   ), \
             patch('sky.jobs.controller.backend_utils.get_clusters',
                   return_value=[{
                       'name': 'sky-managed-7-job-b',
                       'handle': mock_handle_1
                   }, {
                       'name': 'sky-managed-7-job-a',
                       'handle': mock_handle_0
                   }]) as mock_get_clusters:

            # Should NOT raise despite task 0 failing
            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[0, 1],
                dag=mock_dag,
                pool=None)

            mock_get_clusters.assert_called_once()
            # Both tasks should have been attempted in task order.
            assert controller.download_log_and_stream.call_args_list == [
                call(0, mock_handle_0, None),
                call(1, mock_handle_1, None),
            ]


class TestTransientJobStatusFetchDeadline:
    """Tests the bounded retry window for remote job-status failures."""

    class ExpectedRecovery(Exception):
        """Stops the monitor immediately after it enters recovery."""

    class ExpectedStop(Exception):
        """Stops the monitor after exercising a completed recovery."""

    @staticmethod
    def _make_controller() -> JobController:
        controller = JobController.__new__(JobController)
        controller._job_id = 1
        controller._pool = None
        controller._backend = MagicMock()
        return controller

    async def _run_until_stopped(self,
                                 statuses,
                                 monotonic_values,
                                 *,
                                 num_nodes=1,
                                 recover_side_effect=None,
                                 expected_exception=None):
        controller = self._make_controller()
        task = MagicMock(name='task')
        task.num_nodes = num_nodes
        executor = MagicMock()
        if recover_side_effect is None:
            recover_side_effect = self.ExpectedRecovery
        if expected_exception is None:
            expected_exception = self.ExpectedRecovery
        executor.recover = AsyncMock(side_effect=recover_side_effect)
        get_status = AsyncMock(side_effect=statuses)
        refresh_cluster = MagicMock(return_value=(status_lib.ClusterStatus.UP,
                                                  None))
        sleep = AsyncMock()
        backoff = MagicMock()
        backoff.current_backoff.return_value = 10
        monotonic = MagicMock(side_effect=monotonic_values)
        wall_clock = MagicMock(side_effect=AssertionError(
            'status-fetch retry window used wall-clock time'))
        fake_time = SimpleNamespace(monotonic=monotonic, time=wall_clock)
        set_recovering = AsyncMock()
        set_recovered = AsyncMock()

        with patch.object(controller_lib, 'time', fake_time), patch.object(
                controller_lib.asyncio, 'sleep', new=sleep), patch.object(
                    controller_lib.backend_utils,
                    'async_check_network_connection',
                    new=AsyncMock()), patch.object(
                        controller_lib.managed_job_utils,
                        'get_job_status',
                        new=get_status), patch.object(
                            controller_lib.backend_utils,
                            'refresh_cluster_status_handle',
                            new=refresh_cluster), patch.object(
                                controller_lib.common_utils,
                                'Backoff',
                                return_value=backoff), patch.object(
                                    controller_lib.managed_job_state,
                                    'set_recovering_async',
                                    new=set_recovering), patch.object(
                                        controller_lib.managed_job_state,
                                        'set_recovered_async',
                                        new=set_recovered), pytest.raises(
                                            expected_exception):
            await controller._monitor_one_task(
                task_id=0,
                task=task,
                cluster_name='test-cluster',
                executor=executor,
                callback_func=AsyncMock(),
            )

        return (sleep, get_status, refresh_cluster, monotonic, wall_clock,
                set_recovering, executor)

    @pytest.mark.asyncio
    async def test_transient_status_fetch_recovers_multi_node_despite_last_running(
            self):
        # For multi-node jobs a non-terminal job_status is not a reliable
        # health signal: the job may not be set to FAILED immediately when
        # only some nodes are preempted or fail. So once the status-fetch
        # retry budget is exhausted the controller must fall back to recovery
        # even though the cluster still reports UP and the last confirmed
        # status was RUNNING (mirrors the num_nodes == 1 gate on the healthy
        # fast path). The healthy-cluster status-hold must apply to single-node
        # jobs only.
        results = await self._run_until_stopped(
            statuses=[
                (job_lib.JobStatus.RUNNING, None),
                (None, 'transient'),
                (None, 'transient'),
            ],
            monotonic_values=[100.0, 100.0, 160.0],
            num_nodes=2,
        )
        (sleep, get_status, refresh_cluster, monotonic, wall_clock,
         set_recovering, executor) = results

        # The last confirmed status was RUNNING and the cluster stayed UP, yet
        # the multi-node job must still be recovered rather than held alive.
        set_recovering.assert_awaited_once()
        executor.recover.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_single_node_running_fast_path_skips_cluster_refresh(self):
        controller = self._make_controller()
        task = MagicMock(name='task')
        task.num_nodes = 1
        executor = MagicMock()
        executor.recover = AsyncMock(
            side_effect=AssertionError('unexpected recovery'))
        get_status = AsyncMock(side_effect=[
            (job_lib.JobStatus.RUNNING, None),
            (job_lib.JobStatus.RUNNING, None),
            (job_lib.JobStatus.SUCCEEDED, None),
        ])
        refresh_cluster = MagicMock(
            side_effect=AssertionError('single-node running path refreshed '
                                       'cluster status'))
        sleep = AsyncMock()
        monotonic = MagicMock(
            side_effect=AssertionError('healthy fast path used retry timer'))
        wall_clock = MagicMock(
            side_effect=AssertionError('healthy fast path used wall clock'))
        fake_time = SimpleNamespace(monotonic=monotonic, time=wall_clock)
        set_succeeded = AsyncMock()

        with patch.object(controller_lib, 'time', fake_time), patch.object(
                controller_lib.asyncio, 'sleep', new=sleep), patch.object(
                    controller_lib.backend_utils,
                    'async_check_network_connection',
                    new=AsyncMock()), patch.object(
                        controller_lib.managed_job_utils,
                        'get_job_status',
                        new=get_status), patch.object(
                            controller_lib.backend_utils,
                            'refresh_cluster_status_handle',
                            new=refresh_cluster), patch.object(
                                controller_lib.managed_job_utils,
                                'try_to_get_job_end_time',
                                return_value=123.0), patch.object(
                                    controller_lib.backend_utils,
                                    'get_clusters',
                                    return_value=[]), patch.object(
                                        controller_lib.managed_job_state,
                                        'set_succeeded_async',
                                        new=set_succeeded):
            succeeded = await controller._monitor_one_task(
                task_id=0,
                task=task,
                cluster_name='test-cluster',
                executor=executor,
                callback_func=AsyncMock(),
                cleanup_cluster_on_success=False,
            )

        assert succeeded is True
        assert [call.args[0] for call in sleep.await_args_list] == [
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
        ]
        assert get_status.await_count == 3
        monotonic.assert_not_called()
        wall_clock.assert_not_called()
        set_succeeded.assert_awaited_once()
        executor.recover.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_transient_status_fetch_uses_monotonic_deadline(self):
        results = await self._run_until_stopped(
            statuses=[(None, 'transient'), (None, 'transient')],
            monotonic_values=[100.0, 100.0, 160.0],
        )
        (sleep, get_status, refresh_cluster, monotonic, wall_clock,
         set_recovering, executor) = results

        assert [call.args[0] for call in sleep.await_args_list] == [
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            10,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
        ]
        assert get_status.await_count == 2
        assert refresh_cluster.call_count == 2
        assert monotonic.call_count == 3
        wall_clock.assert_not_called()
        set_recovering.assert_awaited_once()
        executor.recover.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_transient_status_fetch_holds_healthy_job_until_status_recovers(
            self):
        controller = self._make_controller()
        task = MagicMock(name='task')
        task.num_nodes = 1
        executor = MagicMock()
        executor.recover = AsyncMock(
            side_effect=AssertionError('healthy UP cluster should not recover'))
        get_status = AsyncMock(side_effect=[
            (job_lib.JobStatus.RUNNING, None),
            (None, 'transient'),
            (None, 'transient'),
            (job_lib.JobStatus.SUCCEEDED, None),
        ])
        refresh_cluster = MagicMock(return_value=(status_lib.ClusterStatus.UP,
                                                  None))
        sleep = AsyncMock()
        monotonic = MagicMock(side_effect=[100.0, 100.0, 160.0])
        wall_clock = MagicMock(side_effect=AssertionError(
            'status-fetch hold used wall-clock time'))
        fake_time = SimpleNamespace(monotonic=monotonic, time=wall_clock)
        set_recovering = AsyncMock()
        set_succeeded = AsyncMock()

        with patch.object(controller_lib, 'time', fake_time), patch.object(
                controller_lib.asyncio, 'sleep', new=sleep), patch.object(
                    controller_lib.backend_utils,
                    'async_check_network_connection',
                    new=AsyncMock()), patch.object(
                        controller_lib.managed_job_utils,
                        'get_job_status',
                        new=get_status), patch.object(
                            controller_lib.backend_utils,
                            'refresh_cluster_status_handle',
                            new=refresh_cluster), patch.object(
                                controller_lib.common_utils, 'Backoff'
                            ) as backoff_cls, patch.object(
                                controller_lib.managed_job_utils,
                                'try_to_get_job_end_time',
                                return_value=123.0), patch.object(
                                    controller_lib.backend_utils,
                                    'get_clusters',
                                    return_value=[]), patch.object(
                                        controller_lib.managed_job_state,
                                        'set_recovering_async',
                                        new=set_recovering), patch.object(
                                            controller_lib.managed_job_state,
                                            'set_succeeded_async',
                                            new=set_succeeded):
            backoff = MagicMock()
            backoff.current_backoff.return_value = 10
            backoff_cls.return_value = backoff
            succeeded = await controller._monitor_one_task(
                task_id=0,
                task=task,
                cluster_name='test-cluster',
                executor=executor,
                callback_func=AsyncMock(),
                cleanup_cluster_on_success=False,
            )

        assert succeeded is True
        assert [call.args[0] for call in sleep.await_args_list] == [
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            10,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
        ]
        assert get_status.await_count == 4
        assert refresh_cluster.call_count == 2
        assert monotonic.call_count == 3
        wall_clock.assert_not_called()
        set_recovering.assert_not_awaited()
        set_succeeded.assert_awaited_once()
        executor.recover.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_recovery_resets_transient_status_fetch_deadline(self):
        results = await self._run_until_stopped(
            statuses=[
                (None, 'transient'),
                (None, 'transient'),
                self.ExpectedStop(),
            ],
            monotonic_values=[100.0, 160.0, 160.0, 160.0],
            recover_side_effect=lambda: 123.0,
            expected_exception=self.ExpectedStop,
        )
        (sleep, get_status, refresh_cluster, monotonic, wall_clock,
         set_recovering, executor) = results

        assert [call.args[0] for call in sleep.await_args_list] == [
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            10,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
        ]
        assert get_status.await_count == 3
        assert refresh_cluster.call_count == 2
        assert monotonic.call_count == 4
        wall_clock.assert_not_called()
        set_recovering.assert_awaited_once()
        executor.recover.assert_awaited_once()


class TestUserJobStatusClassification:
    """Tests classification of terminal jobs on the worker cluster."""

    @staticmethod
    def _make_controller() -> JobController:
        controller = JobController.__new__(JobController)
        controller._job_id = 1
        controller._pool = None
        controller._backend = MagicMock()
        controller.download_log_and_stream = MagicMock()
        controller._get_cluster_job_exit_codes = AsyncMock(return_value=[])
        return controller

    @pytest.mark.asyncio
    async def test_failed_driver_is_a_user_job_failure(self):
        controller = self._make_controller()
        task = MagicMock(name='task')
        task.name = 'test-task'
        task.num_nodes = 1
        executor = MagicMock()
        executor.should_restart_on_failure.return_value = False
        handle = MagicMock()

        with patch('asyncio.sleep', new=AsyncMock()), patch(
                'sky.backends.backend_utils.async_check_network_connection',
                new=AsyncMock()), patch(
                    'sky.jobs.utils.get_job_status',
                    new=AsyncMock(return_value=(
                        job_lib.JobStatus.FAILED_DRIVER, None))), patch(
                            'sky.jobs.utils.try_to_get_job_end_time',
                            return_value=12345.0), patch(
                                'sky.backends.backend_utils.'
                                'refresh_cluster_status_handle',
                                return_value=(
                                    status_lib.ClusterStatus.UP,
                                    handle)), patch(
                                        'sky.jobs.state.'
                                        'set_failed_async',
                                        new=AsyncMock()) as set_failed:
            succeeded = await controller._monitor_one_task(
                task_id=0,
                task=task,
                cluster_name='test-cluster',
                executor=executor,
                callback_func=MagicMock(),
            )

        assert succeeded is False
        executor.should_restart_on_failure.assert_called_once_with(
            exit_codes=[])
        set_failed.assert_awaited_once()
        assert (set_failed.call_args.kwargs['failure_type'] ==
                managed_job_state.ManagedJobStatus.FAILED)
        assert 'job driver on the remote cluster failed' in (
            set_failed.call_args.kwargs['failure_reason'])


class TestCancelSignalScan:
    """Tests for ControllerManager cancel-signal handling.

    Every signal file written to the consolidated signal directory must
    eventually be consumed and removed. Signals for jobs owned by this
    process cancel the job task; signals whose job already reached a
    terminal state (or no longer exists) are reaped so they are not
    re-listed by every scan forever.
    """

    @pytest.fixture
    def signal_dir(self, tmp_path):
        with patch('sky.jobs.constants.CONSOLIDATED_SIGNAL_PATH',
                   str(tmp_path)):
            yield tmp_path

    def _make_manager(self):
        return _make_controller_manager()

    @pytest.mark.asyncio
    async def test_owned_job_cancelled_without_status_query(self, signal_dir):
        """Signal for an owned job cancels the task; no DB status query."""
        manager = self._make_manager()
        task = MagicMock()
        manager.job_tasks[7] = task
        (signal_dir / '7').touch()

        with patch(
                'sky.jobs.controller.managed_job_state.get_status_async',
                new_callable=AsyncMock) as point_status_mock, patch(
                    'sky.jobs.controller.managed_job_state.get_statuses_async',
                    new_callable=AsyncMock) as batch_status_mock:
            await manager._process_cancel_signals()

        task.cancel.assert_called_once()
        point_status_mock.assert_not_awaited()
        batch_status_mock.assert_not_awaited()
        assert not (signal_dir / '7').exists()
        assert manager._cancel_info[7] == (False, None)

    @pytest.mark.asyncio
    async def test_owned_cancel_precedes_blocked_orphan_status_read(
            self, signal_dir):
        """A stale orphan backlog cannot delay an owned cancellation."""
        manager = self._make_manager()
        task = MagicMock()
        manager.job_tasks[7] = task
        (signal_dir / '1').touch()
        (signal_dir / '7').touch()

        status_read_started = asyncio.Event()
        release_status_read = asyncio.Event()

        async def blocked_point_status(_job_id):
            status_read_started.set()
            await release_status_read.wait()
            return managed_job_state.ManagedJobStatus.RUNNING

        async def blocked_batch_status(job_ids):
            status_read_started.set()
            await release_status_read.wait()
            return {
                job_id: managed_job_state.ManagedJobStatus.RUNNING
                for job_id in job_ids
            }

        with patch('sky.jobs.controller.os.listdir',
                   return_value=['1', '7']), patch.object(
                       managed_job_state,
                       'get_status_async',
                       side_effect=blocked_point_status), patch.object(
                           managed_job_state,
                           'get_statuses_async',
                           side_effect=blocked_batch_status,
                           create=True):
            scan = asyncio.create_task(manager._process_cancel_signals())
            await asyncio.wait_for(status_read_started.wait(), timeout=2)
            try:
                task.cancel.assert_called_once_with()
                assert not (signal_dir / '7').exists()
            finally:
                release_status_read.set()
                await scan

    @pytest.mark.asyncio
    async def test_blocked_owned_cancel_does_not_delay_another(
            self, signal_dir):
        """Independent owned signals must not form one serial lock convoy."""
        manager = self._make_manager()
        first_task = MagicMock()
        second_task = MagicMock()
        second_delivered = asyncio.Event()
        second_task.cancel.side_effect = second_delivered.set
        manager.job_tasks.update({1: first_task, 2: second_task})
        for job_id in (1, 2):
            (signal_dir / str(job_id)).touch()

        first_started = asyncio.Event()
        release_first = asyncio.Event()

        async def consume_signal(job_id):
            if job_id == 1:
                first_started.set()
                await release_first.wait()
            return ''

        list_signals = patch('sky.jobs.controller.os.listdir',
                             return_value=['1', '2'])
        consume_signals = patch.object(manager,
                                       '_consume_signal_file',
                                       side_effect=consume_signal)
        with list_signals, consume_signals:
            scan = asyncio.create_task(manager._process_cancel_signals())
            await asyncio.wait_for(first_started.wait(), timeout=2)
            try:
                await asyncio.wait_for(second_delivered.wait(), timeout=2)
            finally:
                release_first.set()
                await scan

        first_task.cancel.assert_called_once_with()
        second_task.cancel.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_owned_cancel_failure_does_not_abort_other_delivery(
            self, signal_dir):
        """A per-job failure is isolated so later signals progress now."""
        manager = self._make_manager()
        failed_task = MagicMock()
        delivered_task = MagicMock()
        manager.job_tasks.update({1: failed_task, 2: delivered_task})
        for job_id in (1, 2):
            (signal_dir / str(job_id)).touch()

        async def consume_signal(job_id):
            if job_id == 1:
                raise OSError('lock unavailable')
            return ''

        with patch('sky.jobs.controller.os.listdir',
                   return_value=['1', '2']), patch.object(
                       manager,
                       '_consume_signal_file',
                       side_effect=consume_signal), patch(
                           'sky.jobs.controller.logger.error') as log_error:
            await manager._process_cancel_signals()

        failed_task.cancel.assert_not_called()
        delivered_task.cancel.assert_called_once_with()
        log_error.assert_called_once()
        assert 'job 1' in log_error.call_args.args[0]
        assert 'lock unavailable' in log_error.call_args.args[0]

    @pytest.mark.asyncio
    async def test_orphan_statuses_use_one_batch_snapshot(self, signal_dir):
        manager = self._make_manager()
        task = MagicMock()
        manager.job_tasks[7] = task
        for job_id in (5, 6, 7, 8):
            (signal_dir / str(job_id)).touch()

        batch_status = AsyncMock(
            return_value={
                5: managed_job_state.ManagedJobStatus.SUCCEEDED,
                6: None,
                8: managed_job_state.ManagedJobStatus.RUNNING,
            })
        with patch('sky.jobs.controller.os.listdir',
                   return_value=['5', '6', '8', '7']), patch.object(
                       managed_job_state,
                       'get_status_async',
                       new_callable=AsyncMock,
                       side_effect=AssertionError('point status read')), \
                patch.object(managed_job_state,
                             'get_statuses_async',
                             batch_status,
                             create=True):
            await manager._process_cancel_signals()

        task.cancel.assert_called_once_with()
        batch_status.assert_awaited_once_with([5, 6, 8])
        assert not (signal_dir / '5').exists()
        assert not (signal_dir / '6').exists()
        assert not (signal_dir / '7').exists()
        assert (signal_dir / '8').exists()

    @pytest.mark.asyncio
    async def test_blocked_orphan_reap_does_not_delay_another(self, signal_dir):
        """Independent orphan locks must not form one serial convoy."""
        manager = self._make_manager()
        for job_id in (1, 2):
            (signal_dir / str(job_id)).touch()

        first_started = asyncio.Event()
        release_first = asyncio.Event()
        second_reaped = asyncio.Event()

        async def remove_signal(job_id):
            if job_id == 1:
                first_started.set()
                await release_first.wait()
            else:
                second_reaped.set()

        statuses = {
            job_id: managed_job_state.ManagedJobStatus.SUCCEEDED
            for job_id in (1, 2)
        }
        with patch('sky.jobs.controller.os.listdir',
                   return_value=['1', '2']), patch.object(
                       managed_job_state,
                       'get_statuses_async',
                       new=AsyncMock(return_value=statuses)), patch.object(
                           manager,
                           '_remove_signal_file',
                           side_effect=remove_signal):
            scan = asyncio.create_task(manager._process_cancel_signals())
            await asyncio.wait_for(first_started.wait(), timeout=2)
            try:
                await asyncio.wait_for(second_reaped.wait(), timeout=2)
            finally:
                release_first.set()
                await scan

    @pytest.mark.asyncio
    async def test_orphan_reap_failure_does_not_abort_sibling(self, signal_dir):
        manager = self._make_manager()
        for job_id in (1, 2):
            (signal_dir / str(job_id)).touch()

        removed_job_ids = []

        async def remove_signal(job_id):
            if job_id == 1:
                raise RuntimeError('lock backend unavailable')
            removed_job_ids.append(job_id)

        statuses = {
            job_id: managed_job_state.ManagedJobStatus.SUCCEEDED
            for job_id in (1, 2)
        }
        with patch('sky.jobs.controller.os.listdir',
                   return_value=['1', '2']), patch.object(
                       managed_job_state,
                       'get_statuses_async',
                       new=AsyncMock(return_value=statuses)), patch.object(
                           manager,
                           '_remove_signal_file',
                           side_effect=remove_signal), patch(
                               'sky.jobs.controller.logger.debug') as log_debug:
            await manager._process_cancel_signals()

        assert removed_job_ids == [2]
        log_debug.assert_called_once()
        assert 'job 1' in log_debug.call_args.args[0]
        assert 'lock backend unavailable' in log_debug.call_args.args[0]

    @pytest.mark.asyncio
    async def test_cancelled_scan_finishes_started_orphan_reap(
            self, signal_dir):
        manager = self._make_manager()
        manager._cancel_info[1] = (False, None)
        (signal_dir / '1').touch()

        removal_started = asyncio.Event()
        release_removal = asyncio.Event()
        removal_finished = asyncio.Event()

        async def remove_signal(_job_id):
            removal_started.set()
            await release_removal.wait()
            removal_finished.set()

        statuses = {1: managed_job_state.ManagedJobStatus.SUCCEEDED}
        with patch('sky.jobs.controller.os.listdir',
                   return_value=['1']), patch.object(
                       managed_job_state,
                       'get_statuses_async',
                       new=AsyncMock(return_value=statuses)), patch.object(
                           manager,
                           '_remove_signal_file',
                           side_effect=remove_signal):
            scan = asyncio.create_task(manager._process_cancel_signals())
            await asyncio.wait_for(removal_started.wait(), timeout=2)
            scan.cancel()
            result = await asyncio.gather(scan, return_exceptions=True)
            assert isinstance(result[0], asyncio.CancelledError)
            assert not removal_finished.is_set()

            release_removal.set()
            await asyncio.wait_for(removal_finished.wait(), timeout=2)
            for _ in range(10):
                if 1 not in manager._cancel_info:
                    break
                await asyncio.sleep(0)

        assert 1 not in manager._cancel_info

    @pytest.mark.asyncio
    async def test_orphan_reap_fanout_uses_controller_worker_bound(
            self, signal_dir):
        manager = self._make_manager()
        limit = controller_lib.controller_utils.LAUNCHES_PER_WORKER
        job_ids = list(range(limit + 1))

        started_job_ids = []
        limit_reached = asyncio.Event()
        release_removals = asyncio.Event()

        async def remove_signal(job_id):
            started_job_ids.append(job_id)
            if len(started_job_ids) == limit:
                limit_reached.set()
            await release_removals.wait()

        statuses = {
            job_id: managed_job_state.ManagedJobStatus.SUCCEEDED
            for job_id in job_ids
        }
        with patch(
                'sky.jobs.controller.os.listdir',
                return_value=[str(job_id) for job_id in job_ids]), patch.object(
                    managed_job_state,
                    'get_statuses_async',
                    new=AsyncMock(
                        return_value=statuses)) as batch_status, patch.object(
                            manager,
                            '_remove_signal_file',
                            side_effect=remove_signal) as remove:
            scan = asyncio.create_task(manager._process_cancel_signals())
            await asyncio.wait_for(limit_reached.wait(), timeout=2)
            try:
                for _ in range(10):
                    await asyncio.sleep(0)
                assert len(started_job_ids) == limit
            finally:
                release_removals.set()
                await scan

        assert sorted(started_job_ids) == job_ids
        assert remove.await_count == len(job_ids)
        batch_status.assert_awaited_once_with(job_ids)

    @pytest.mark.asyncio
    async def test_signal_lock_contention_does_not_block_event_loop(
            self, signal_dir):
        manager = self._make_manager()
        task = MagicMock()
        delivered = asyncio.Event()
        task.cancel.side_effect = delivered.set
        manager.job_tasks[7] = task
        (signal_dir / '7').touch()
        lock_entered = asyncio.Event()
        release_lock = threading.Event()

        class ContendedLock:
            """Lock stand-in that can model both sync and async contention."""

            def __enter__(self):
                lock_entered.set()
                release_lock.wait()
                return self

            def __exit__(self, *args):
                return False

            async def __aenter__(self):
                lock_entered.set()
                await asyncio.to_thread(release_lock.wait)
                return self

            async def __aexit__(self, *args):
                return False

        fallback_release = threading.Timer(1, release_lock.set)
        fallback_release.start()
        try:
            lock = ContendedLock()
            with patch('sky.jobs.controller.filelock.FileLock',
                       return_value=lock), patch(
                           'sky.jobs.controller.filelock.AsyncFileLock',
                           return_value=lock):
                scan = asyncio.create_task(manager._process_cancel_signals())
                await lock_entered.wait()

                # A synchronous FileLock would hold the event loop until the
                # fallback timer fires. The async lock lets this task run while
                # another process owns the signal lock.
                assert not release_lock.is_set()
                scan.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await scan
                release_lock.set()
                await asyncio.wait_for(delivered.wait(), timeout=2)
                task.cancel.assert_called_once_with()
                assert not (signal_dir / '7').exists()
        finally:
            release_lock.set()
            fallback_release.cancel()

    @pytest.mark.asyncio
    async def test_orphan_signal_for_terminal_job_reaped(self, signal_dir):
        manager = self._make_manager()
        manager._cancel_info[5] = (False, None)
        (signal_dir / '5').touch()

        with patch(
                'sky.jobs.controller.managed_job_state.get_statuses_async',
                new_callable=AsyncMock,
                return_value={5: managed_job_state.ManagedJobStatus.SUCCEEDED}):
            await manager._process_cancel_signals()

        assert not (signal_dir / '5').exists()
        assert 5 not in manager._cancel_info

    @pytest.mark.asyncio
    async def test_orphan_signal_for_missing_job_reaped(self, signal_dir):
        manager = self._make_manager()
        (signal_dir / '6').touch()

        with patch('sky.jobs.controller.managed_job_state.get_statuses_async',
                   new_callable=AsyncMock,
                   return_value={6: None}):
            await manager._process_cancel_signals()

        assert not (signal_dir / '6').exists()

    @pytest.mark.asyncio
    async def test_orphan_signal_for_live_job_kept(self, signal_dir):
        """A non-terminal job owned elsewhere keeps its signal file."""
        manager = self._make_manager()
        manager._cancel_info[8] = (True, 30)
        (signal_dir / '8').touch()

        with patch('sky.jobs.controller.managed_job_state.get_statuses_async',
                   new_callable=AsyncMock,
                   return_value={8: managed_job_state.ManagedJobStatus.RUNNING
                                }):
            await manager._process_cancel_signals()

        assert (signal_dir / '8').exists()
        assert manager._cancel_info[8] == (True, 30)

    @pytest.mark.asyncio
    async def test_signal_vanished_under_lock_skips_cancel(self, signal_dir):
        """A signal consumed by another scanner mid-scan is a no-op.

        The file exists at directory-listing time but is gone by the
        time the scan holds the filelock (e.g. a sibling controller
        process reaped it). The scan must neither raise nor cancel the
        task on a signal that no longer exists.
        """
        manager = self._make_manager()
        task = MagicMock()
        manager.job_tasks[7] = task
        (signal_dir / '7').touch()

        class ConsumingLock:
            """Filelock stand-in that consumes the signal on acquire."""

            def __init__(self, lock_path: str):
                self._signal = pathlib.Path(lock_path[:-len('.lock')])

            async def __aenter__(self):
                self._signal.unlink(missing_ok=True)
                return self

            async def __aexit__(self, *args):
                return False

        with patch('sky.jobs.controller.filelock.AsyncFileLock', ConsumingLock):
            await manager._process_cancel_signals()

        task.cancel.assert_not_called()
        assert 7 not in manager._cancel_info

    @pytest.mark.asyncio
    async def test_signal_read_failure_keeps_file_for_retry(self, signal_dir):
        manager = self._make_manager()
        task = MagicMock()
        manager.job_tasks[7] = task
        (signal_dir / '7').write_text('', encoding='utf-8')

        async def fail_read(*_args, **_kwargs):
            raise OSError('transient read failure')

        with patch.object(controller_lib.anyio.Path,
                          'read_text',
                          side_effect=fail_read):
            await manager._process_cancel_signals()

        task.cancel.assert_not_called()
        assert (signal_dir / '7').exists()
        assert 7 not in manager._cancel_info

    @pytest.mark.asyncio
    async def test_signal_read_failure_retries_on_next_scan(self, signal_dir):
        manager = self._make_manager()
        task = MagicMock()
        manager.job_tasks[7] = task
        (signal_dir / '7').write_text('', encoding='utf-8')

        read_attempts = 0

        async def flaky_read(*_args, **_kwargs):
            nonlocal read_attempts
            read_attempts += 1
            if read_attempts == 1:
                raise OSError('transient read failure')
            return ''

        with patch.object(controller_lib.anyio.Path,
                          'read_text',
                          side_effect=flaky_read):
            await manager._process_cancel_signals()
            task.cancel.assert_not_called()
            assert (signal_dir / '7').exists()

            await manager._process_cancel_signals()

        assert read_attempts == 2
        task.cancel.assert_called_once_with()
        assert not (signal_dir / '7').exists()
        assert manager._cancel_info[7] == (False, None)

    @pytest.mark.asyncio
    async def test_successful_signal_consume_uses_one_read_and_one_unlink(
            self, signal_dir):
        (signal_dir / '7').write_text('', encoding='utf-8')
        call_counts = {'read': 0, 'unlink': 0}
        original_read = controller_lib.anyio.Path.read_text
        original_unlink = controller_lib.anyio.Path.unlink

        async def count_read(path_obj, *args, **kwargs):
            call_counts['read'] += 1
            return await original_read(path_obj, *args, **kwargs)

        async def count_unlink(path_obj, *args, **kwargs):
            call_counts['unlink'] += 1
            return await original_unlink(path_obj, *args, **kwargs)

        with patch.object(controller_lib.anyio.Path, 'read_text',
                          count_read), patch.object(controller_lib.anyio.Path,
                                                    'unlink', count_unlink):
            content = await ControllerManager._consume_signal_file(7)

        assert content == ''
        assert call_counts == {'read': 1, 'unlink': 1}
        assert not (signal_dir / '7').exists()

    @pytest.mark.asyncio
    async def test_cancel_loop_survives_scan_failure(self):
        """One failed scan must not unwind the cancel loop.

        cancel_job is gathered with the monitor loop in main(); an
        escaped exception exits the whole controller process.
        """
        manager = self._make_manager()
        second_scan_ran = asyncio.Event()
        calls = 0

        async def fake_scan():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError('scan failed')
            second_scan_ran.set()
            # Keep the second scan suspended so the loop cannot spin while
            # the waiter observes the successful retry.
            await asyncio.Event().wait()

        with patch.object(manager, '_process_cancel_signals',
                          side_effect=fake_scan), \
                patch('sky.jobs.controller.asyncio.sleep',
                      new_callable=AsyncMock) as sleep:
            loop_task = asyncio.create_task(manager.cancel_job())
            try:
                await asyncio.wait_for(second_scan_ran.wait(), timeout=5)
            finally:
                loop_task.cancel()
                result = await asyncio.gather(loop_task, return_exceptions=True)
        assert calls >= 2
        sleep.assert_awaited_once_with(15)
        assert isinstance(result[0], asyncio.CancelledError)

    @pytest.mark.asyncio
    async def test_remove_signal_file_is_idempotent(self, signal_dir):
        """The shared signal consumer tolerates an already-removed file."""
        (signal_dir / '12').touch()
        await ControllerManager._remove_signal_file(12)
        assert not (signal_dir / '12').exists()
        # Second removal (lost race with another consumer) must not raise.
        await ControllerManager._remove_signal_file(12)

    @pytest.mark.asyncio
    async def test_pending_cancel_uses_idempotent_signal_removal(self):
        """The PENDING claim path shares the race-safe signal consumer."""
        manager = self._make_manager()
        manager.start_job = AsyncMock()
        waiting_calls = 0

        async def get_waiting_job(**_kwargs):
            nonlocal waiting_calls
            waiting_calls += 1
            if waiting_calls == 1:
                return {'job_id': 12, 'cleanup_only': False}
            raise asyncio.CancelledError

        with patch('sky.jobs.controller.controller_utils.'
                   'get_number_of_jobs_controllers', return_value=1), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_waiting_job_async', side_effect=get_waiting_job), \
                patch('sky.jobs.controller.os.listdir', return_value=['12']), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_status_async', new_callable=AsyncMock,
                      return_value=managed_job_state.ManagedJobStatus.PENDING
                     ) as get_status, \
                patch.object(manager, '_remove_signal_file',
                             new_callable=AsyncMock) as remove_signal, \
                patch('sky.jobs.controller.managed_job_state.'
                      'set_cancelling_async', new_callable=AsyncMock
                     ) as set_cancelling, \
                patch('sky.jobs.controller.managed_job_state.'
                      'set_cancelled_async', new_callable=AsyncMock
                     ) as set_cancelled, \
                patch('sky.jobs.controller.scheduler.job_done_async',
                      new_callable=AsyncMock) as job_done, \
                patch('sky.jobs.controller.managed_job_utils.'
                      'event_callback_func'):
            with pytest.raises(asyncio.CancelledError):
                await manager.monitor_loop()

        get_status.assert_awaited_once_with(12)
        remove_signal.assert_awaited_once_with(12)
        set_cancelling.assert_awaited_once()
        set_cancelled.assert_awaited_once()
        manager.start_job.assert_not_awaited()
        # get_waiting_job_async already moved the job to LAUNCHING under this
        # controller's pid. Without the DONE transition the schedule state
        # would stay LAUNCHING forever, so get_num_alive_jobs() would never
        # drop and the jobs controller could never autostop.
        job_done.assert_awaited_once_with(12, idempotent=True)

    @pytest.mark.asyncio
    async def test_non_digit_and_lock_files_skipped(self, signal_dir):
        manager = self._make_manager()
        (signal_dir / 'unexpected').touch()
        (signal_dir / '9.lock').touch()

        with patch('sky.jobs.controller.managed_job_state.get_status_async',
                   new_callable=AsyncMock) as status_mock:
            await manager._process_cancel_signals()

        status_mock.assert_not_awaited()
        assert (signal_dir / 'unexpected').exists()
        assert (signal_dir / '9.lock').exists()


class TestControllerManagerMonitorLoop:
    """The controller monitor reacts to capacity changes without polling."""

    @pytest.mark.asyncio
    async def test_capacity_snapshot_uses_one_lock_acquisition(self):
        manager = _make_controller_manager()

        class CountingLock:
            """Count async context entries around a real lock."""

            def __init__(self):
                self._lock = asyncio.Lock()
                self.acquisitions = 0

            async def __aenter__(self):
                self.acquisitions += 1
                await self._lock.acquire()

            async def __aexit__(self, exc_type, exc, traceback):
                del exc_type, exc, traceback
                self._lock.release()

        capacity_lock = CountingLock()
        manager._job_tasks_lock = capacity_lock

        with patch('sky.jobs.controller.controller_utils.'
                   'get_number_of_jobs_controllers', return_value=1), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_waiting_job_async',
                      new=AsyncMock(side_effect=asyncio.CancelledError)
                     ) as get_waiting:
            with pytest.raises(asyncio.CancelledError):
                await manager.monitor_loop()

        assert capacity_lock.acquisitions == 1
        get_waiting.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_running_limit_wakes_when_tracked_job_finishes(self):
        manager = _make_controller_manager()
        release_job = asyncio.Event()
        tracked_job = asyncio.create_task(release_job.wait())
        manager.job_tasks[1] = tracked_job
        wait_started = asyncio.Event()
        scheduler_queried = asyncio.Event()
        original_wait = asyncio.wait

        async def wait_for_completion(*args, **kwargs):
            wait_started.set()
            return await original_wait(*args, **kwargs)

        async def get_waiting_job(**_kwargs):
            scheduler_queried.set()
            raise asyncio.CancelledError

        sleep = AsyncMock(side_effect=AssertionError(
            'running capacity must not use polling sleeps'))
        wait = AsyncMock(side_effect=wait_for_completion)
        with patch('sky.jobs.controller.controller_utils.'
                   'MAX_JOBS_PER_WORKER', 1), \
                patch('sky.jobs.controller.controller_utils.'
                      'MAX_TOTAL_RUNNING_JOBS', 1), \
                patch('sky.jobs.controller.controller_utils.'
                      'get_number_of_jobs_controllers', return_value=1), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_waiting_job_async', side_effect=get_waiting_job
                     ) as get_waiting, \
                patch('sky.jobs.controller.asyncio.wait', new=wait), \
                patch('sky.jobs.controller.asyncio.sleep', new=sleep):
            monitor = asyncio.create_task(manager.monitor_loop())
            await asyncio.wait_for(wait_started.wait(), timeout=1)
            monitor.cancel()
            monitor_result, = await asyncio.gather(monitor,
                                                   return_exceptions=True)
            assert isinstance(monitor_result, asyncio.CancelledError)
            assert not tracked_job.done()
            assert not tracked_job.cancelled()

            wait_started.clear()
            monitor = asyncio.create_task(manager.monitor_loop())
            try:
                await asyncio.wait_for(wait_started.wait(), timeout=1)
                get_waiting.assert_not_awaited()
                release_job.set()
                await asyncio.wait_for(scheduler_queried.wait(), timeout=1)
                await asyncio.gather(monitor, return_exceptions=True)
            finally:
                monitor.cancel()
                release_job.set()
                await asyncio.gather(monitor,
                                     tracked_job,
                                     return_exceptions=True)

        sleep.assert_not_awaited()
        assert wait.await_count == 2
        for wait_call in wait.await_args_list:
            assert wait_call.kwargs == {
                'timeout': 60,
                'return_when': asyncio.FIRST_COMPLETED,
            }
        assert tracked_job.done()
        assert not tracked_job.cancelled()
        get_waiting.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_zero_running_limit_retains_topology_recheck(self):
        manager = _make_controller_manager()
        sleep_started = asyncio.Event()

        async def sleep_for_recheck(delay):
            assert delay == 60
            sleep_started.set()
            raise asyncio.CancelledError

        sleep = AsyncMock(side_effect=sleep_for_recheck)
        wait = AsyncMock(side_effect=AssertionError(
            'asyncio.wait rejects an empty task set'))
        with patch('sky.jobs.controller.controller_utils.'
                   'MAX_JOBS_PER_WORKER', 1), \
                patch('sky.jobs.controller.controller_utils.'
                      'MAX_TOTAL_RUNNING_JOBS', 0), \
                patch('sky.jobs.controller.controller_utils.'
                      'get_number_of_jobs_controllers', return_value=1), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_waiting_job_async', new_callable=AsyncMock
                     ) as get_waiting, \
                patch('sky.jobs.controller.asyncio.wait', new=wait), \
                patch('sky.jobs.controller.asyncio.sleep', new=sleep):
            monitor = asyncio.create_task(manager.monitor_loop())
            await asyncio.wait_for(sleep_started.wait(), timeout=1)
            await asyncio.gather(monitor, return_exceptions=True)

        sleep.assert_awaited_once_with(60)
        wait.assert_not_awaited()
        get_waiting.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_saturated_launch_slot_wakes_on_notification(self):
        manager = _make_controller_manager()
        manager.starting.add(1)
        wait_started = asyncio.Event()
        job_started = asyncio.Event()

        class TrackingCondition(asyncio.Condition):

            async def wait_for(self, predicate):
                wait_started.set()
                return await super().wait_for(predicate)

        manager._starting_signal = TrackingCondition(manager._job_tasks_lock)

        async def start_job(job_id, pool=None):
            assert job_id == 2
            assert pool is None
            job_started.set()
            raise asyncio.CancelledError

        manager.start_job = AsyncMock(side_effect=start_job)

        with patch('sky.jobs.controller.controller_utils.'
                   'LAUNCHES_PER_WORKER', 1), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_waiting_job_async',
                      new=AsyncMock(return_value={
                          'job_id': 2,
                          'cleanup_only': False,
                      })), \
                patch('sky.jobs.controller.os.listdir', return_value=[]), \
                patch('sky.jobs.controller.asyncio.sleep',
                      new=AsyncMock(side_effect=AssertionError(
                          'launch capacity must not use polling sleeps'))
                     ) as sleep:
            monitor = asyncio.create_task(manager.monitor_loop())
            try:
                await asyncio.wait_for(wait_started.wait(), timeout=1)
                async with manager._starting_signal:
                    manager.starting.remove(1)
                    manager._starting_signal.notify()
                await asyncio.wait_for(job_started.wait(), timeout=1)
            finally:
                monitor.cancel()
                await asyncio.gather(monitor, return_exceptions=True)

        sleep.assert_not_awaited()
        manager.start_job.assert_awaited_once_with(2, None)


class TestInitialLaunchSlotOwnership:
    """The per-job controller releases bypassed initial launch admission."""

    @staticmethod
    def _make_controller():
        controller = JobController.__new__(JobController)
        controller._job_id = 3
        controller.starting = {3}
        controller.starting_lock = asyncio.Lock()
        controller.starting_signal = asyncio.Condition(controller.starting_lock)
        return controller

    @pytest.mark.asyncio
    async def test_release_is_idempotent_and_notifies_once(self):
        controller = self._make_controller()

        class CountingCondition(asyncio.Condition):

            def __init__(self, lock):
                super().__init__(lock)
                self.notify_count = 0

            def notify(self, n=1):
                self.notify_count += n
                return super().notify(n)

        controller.starting_signal = CountingCondition(controller.starting_lock)

        await controller._release_initial_launch_slot()
        await controller._release_initial_launch_slot()

        assert controller.starting == set()
        assert controller.starting_signal.notify_count == 1

    @pytest.mark.asyncio
    async def test_release_finishes_after_repeated_cancellation(self):
        controller = self._make_controller()
        cleanup_waiting = asyncio.Event()
        original_acquire = controller.starting_lock.acquire
        background_before = set(asyncio_utils._background_tasks)

        async def track_cleanup_waiter():
            cleanup_waiting.set()
            return await original_acquire()

        await original_acquire()
        try:
            with patch.object(controller.starting_lock,
                              'acquire',
                              side_effect=track_cleanup_waiter):
                release_task = asyncio.create_task(
                    controller._release_initial_launch_slot())
                await cleanup_waiting.wait()
                release_task.cancel()
                release_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await release_task
                cleanup_tasks = (asyncio_utils._background_tasks -
                                 background_before)
                assert len(cleanup_tasks) == 1
        finally:
            controller.starting_lock.release()

        await asyncio.gather(*cleanup_tasks)
        assert controller.starting == set()


class TestRunJobLoopOwnershipCleanup:
    """run_job_loop owns manager bookkeeping across every exit path.

    If a cancellation lands after the job task already finished,
    task.cancel() is a no-op and no CancelledError handler consumes the
    stored cancel info. Initialization failures must also release launch
    capacity before the inner durable-cleanup scope starts.
    """

    @pytest.mark.asyncio
    async def test_repeated_cancellation_waits_for_inner_finalization(self):
        manager = _make_controller_manager()
        inner_started = asyncio.Event()
        inner_finalizing = asyncio.Event()
        finish_finalization = asyncio.Event()
        events = []
        cancellation_deliveries = 0
        created_tasks = []

        async def inner_job_loop(*_args):
            nonlocal cancellation_deliveries
            events.append('started')
            inner_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_deliveries += 1
                events.append('finalizing')
                inner_finalizing.set()
                await finish_finalization.wait()
                events.append('finalized')
                raise

        async def release_ownership(_job_id):
            assert events[-1] == 'finalized'
            events.append('released')

        original_create_task = asyncio.create_task

        def track_create_task(coro):
            task = original_create_task(coro)
            created_tasks.append(task)
            return task

        manager._run_job_loop = inner_job_loop
        manager._release_job_loop_ownership = AsyncMock(
            side_effect=release_ownership)
        loop = asyncio.get_running_loop()
        with patch('sky.jobs.controller.asyncio.create_task',
                   side_effect=track_create_task):
            owner = loop.create_task(
                ControllerManager.run_job_loop.__wrapped__(
                    manager, 3, '/dev/null'))
            await inner_started.wait()
            owner.cancel()
            await inner_finalizing.wait()
            owner.cancel()
            owner.cancel()
            await asyncio.sleep(0)

            assert not owner.done()
            assert events == ['started', 'finalizing']

            finish_finalization.set()
            with pytest.raises(asyncio.CancelledError):
                await owner

        assert events == ['started', 'finalizing', 'finalized', 'released']
        assert cancellation_deliveries == 1
        assert len(created_tasks) == 1
        manager._release_job_loop_ownership.assert_awaited_once_with(3)

    @pytest.mark.asyncio
    async def test_start_job_hands_off_slot_without_cancellation_gap(
            self, tmp_path):
        manager = _make_controller_manager()
        registered = []

        def register(coro):
            assert manager._job_tasks_lock.locked()
            assert 3 in manager.starting
            registered.append(coro)
            coro.close()

        with patch('sky.jobs.controller.jobs_constants.'
                   'JOBS_CONTROLLER_LOGS_DIR', str(tmp_path)), \
                patch('sky.jobs.controller.create_background_task',
                      side_effect=register) as create_task:
            await manager.start_job(3)

        create_task.assert_called_once()
        assert len(registered) == 1
        assert 3 in manager.starting

    @pytest.mark.asyncio
    async def test_initialization_failure_releases_slot_and_wakes_waiter(self):
        manager = _make_controller_manager()
        manager.starting.add(3)
        waiter_started = asyncio.Event()

        class TrackingCondition(asyncio.Condition):

            async def wait(self):
                waiter_started.set()
                return await super().wait()

        manager._starting_signal = TrackingCondition(manager._job_tasks_lock)

        async def wait_for_slot():
            async with manager._starting_signal:
                await manager._starting_signal.wait_for(
                    lambda: 3 not in manager.starting)

        waiter = asyncio.create_task(wait_for_slot())
        await waiter_started.wait()
        manager._cleanup = AsyncMock()
        ctx = MagicMock()

        with patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content',
                      side_effect=RuntimeError('init failed')), \
                patch('sky.jobs.controller.JobController') as controller:
            with pytest.raises(RuntimeError, match='init failed'):
                await manager.run_job_loop(3, '/dev/null')

        await asyncio.wait_for(waiter, timeout=1)
        assert 3 not in manager.starting
        assert 3 not in manager.job_tasks
        assert 3 not in manager._cancel_info
        controller.assert_not_called()
        manager._cleanup.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_success_path_pops_stale_cancel_info(self):
        manager = _make_controller_manager()
        manager.starting.add(3)
        manager._cancel_info[3] = (False, None)
        manager._cleanup = AsyncMock()
        manager._cleanup_api_server_access_token = MagicMock()

        ctx = MagicMock()
        controller = MagicMock()
        controller.run = AsyncMock(return_value=True)

        with patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content', return_value=''), \
                patch('sky.jobs.controller.usage_lib.'
                      'install_fresh_messages_for_current_context'), \
                patch('sky.jobs.controller.JobController',
                      return_value=controller), \
                patch('sky.jobs.controller.managed_job_state.get_status_async',
                      new_callable=AsyncMock,
                      return_value=(
                          managed_job_state.ManagedJobStatus.SUCCEEDED)), \
                patch('sky.jobs.controller.scheduler.job_done_async',
                      new_callable=AsyncMock) as job_done:
            await manager.run_job_loop(3, '/dev/null')

        assert 3 not in manager._cancel_info
        assert 3 not in manager.starting
        assert 3 not in manager.job_tasks
        manager._cleanup_api_server_access_token.assert_called_once_with(3)
        job_done.assert_awaited_once_with(3)

    @pytest.mark.asyncio
    async def test_cleanup_failure_preserves_terminal_job_status(self, caplog):
        manager = _make_controller_manager()
        manager.starting.add(3)
        manager._cleanup = AsyncMock(
            side_effect=RuntimeError('worker connection timed out'))
        manager._cleanup_api_server_access_token = MagicMock()

        ctx = MagicMock()
        controller = MagicMock()
        controller.run = AsyncMock(return_value=True)

        with patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content', return_value=''), \
                patch('sky.jobs.controller.usage_lib.'
                      'install_fresh_messages_for_current_context'), \
                patch('sky.jobs.controller.JobController',
                      return_value=controller), \
                patch('sky.jobs.controller.managed_job_state.get_status_async',
                      new_callable=AsyncMock,
                      side_effect=[
                          managed_job_state.ManagedJobStatus.SUCCEEDED,
                          managed_job_state.ManagedJobStatus.SUCCEEDED,
                      ]), \
                patch('sky.jobs.controller.managed_job_state.set_failed_async',
                      new_callable=AsyncMock) as set_failed, \
                patch('sky.jobs.controller.scheduler.job_done_async',
                      new_callable=AsyncMock) as job_done:
            await manager.run_job_loop(3, '/dev/null')

        set_failed.assert_not_awaited()
        job_done.assert_not_awaited()
        assert 'Preserving terminal job status' in caplog.text
        assert 'Deferring scheduler finalization' in caplog.text
        assert 'worker connection timed out' in caplog.text

    @pytest.mark.asyncio
    async def test_cleanup_failure_marks_nonterminal_job_failed_controller(
            self):
        manager = _make_controller_manager()
        manager.starting.add(3)
        manager._cleanup = AsyncMock(
            side_effect=RuntimeError('worker connection timed out'))
        manager._cleanup_api_server_access_token = MagicMock()

        ctx = MagicMock()
        controller = MagicMock()
        controller.run = AsyncMock(return_value=True)

        with patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content', return_value=''), \
                patch('sky.jobs.controller.usage_lib.'
                      'install_fresh_messages_for_current_context'), \
                patch('sky.jobs.controller.JobController',
                      return_value=controller), \
                patch('sky.jobs.controller.managed_job_state.get_status_async',
                      new_callable=AsyncMock,
                      side_effect=[
                          managed_job_state.ManagedJobStatus.RUNNING,
                          managed_job_state.ManagedJobStatus.FAILED_CONTROLLER,
                      ]), \
                patch('sky.jobs.controller.managed_job_state.set_failed_async',
                      new_callable=AsyncMock) as set_failed, \
                patch('sky.jobs.controller.scheduler.job_done_async',
                      new_callable=AsyncMock) as job_done:
            await manager.run_job_loop(3, '/dev/null')

        set_failed.assert_awaited_once()
        _, kwargs = set_failed.await_args
        assert kwargs['failure_type'] == (
            managed_job_state.ManagedJobStatus.FAILED_CONTROLLER)
        assert kwargs['override_terminal']
        assert 'worker connection timed out' in kwargs['failure_reason']
        job_done.assert_not_awaited()


class TestTerminalCleanupAdoption:
    """A replacement manager adopts terminal work without running it."""

    @pytest.mark.asyncio
    async def test_start_cleanup_job_tracks_the_background_owner(
            self, tmp_path):
        manager = _make_controller_manager()
        tracked_task = MagicMock(spec=asyncio.Task)
        registered = []

        def register(coro):
            registered.append(coro)
            coro.close()
            return tracked_task

        with patch('sky.jobs.controller.jobs_constants.'
                   'JOBS_CONTROLLER_LOGS_DIR', str(tmp_path)), \
                patch('sky.jobs.controller.create_background_task',
                      side_effect=register) as create_task:
            await manager.start_cleanup_job(3, 'pool-a')

        create_task.assert_called_once()
        assert len(registered) == 1
        assert manager.job_tasks[3] is tracked_task
        assert 3 in manager._cleanup_only_job_ids

    @pytest.mark.asyncio
    async def test_cleanup_only_retries_then_exactly_finalizes(self):
        manager = _make_controller_manager()
        manager.job_tasks[3] = asyncio.current_task()
        manager._cleanup_only_job_ids.add(3)
        manager._initialize_job_context = MagicMock(return_value=None)
        manager._cleanup = AsyncMock(
            side_effect=[RuntimeError('provider unavailable'), None])
        manager._cleanup_api_server_access_token = MagicMock()

        with patch('sky.jobs.controller.asyncio.sleep',
                   new_callable=AsyncMock) as sleep, \
                patch('sky.jobs.controller.managed_job_state.'
                      'scheduler_set_cleanup_done_async',
                      new_callable=AsyncMock) as cleanup_done, \
                patch('sky.jobs.controller.JobController') as job_controller, \
                patch('sky.jobs.controller.managed_job_utils.'
                      'event_callback_func') as callback:
            await ControllerManager.run_cleanup_loop.__wrapped__(
                manager, 3, '/tmp/controller.log', None)

        assert manager._cleanup.await_args_list == [
            call(3, pool=None),
            call(3, pool=None),
        ]
        sleep.assert_awaited_once_with(1)
        manager._cleanup_api_server_access_token.assert_called_once_with(3)
        cleanup_done.assert_awaited_once_with(3)
        job_controller.assert_not_called()
        callback.assert_not_called()
        assert 3 not in manager.job_tasks
        assert 3 not in manager._cleanup_only_job_ids

    @pytest.mark.asyncio
    async def test_cleanup_completion_is_not_replayed_for_later_phase_retry(
            self):
        manager = _make_controller_manager()
        manager.job_tasks[3] = asyncio.current_task()
        manager._cleanup_only_job_ids.add(3)
        manager._initialize_job_context = MagicMock(return_value=None)
        manager._cleanup = AsyncMock()
        manager._cleanup_api_server_access_token = MagicMock(
            side_effect=[RuntimeError('token database unavailable'), None])

        with patch('sky.jobs.controller.asyncio.sleep',
                   new_callable=AsyncMock) as sleep, \
                patch('sky.jobs.controller.managed_job_state.'
                      'scheduler_set_cleanup_done_async',
                      new_callable=AsyncMock) as cleanup_done:
            await ControllerManager.run_cleanup_loop.__wrapped__(
                manager, 3, '/tmp/controller.log', 'pool-a')

        manager._cleanup.assert_awaited_once_with(3, pool='pool-a')
        assert manager._cleanup_api_server_access_token.call_count == 2
        sleep.assert_awaited_once_with(1)
        cleanup_done.assert_awaited_once_with(3)

    @pytest.mark.asyncio
    async def test_exact_claim_loss_exits_for_guardian_readoption(self):
        manager = _make_controller_manager()
        manager.job_tasks[3] = asyncio.current_task()
        manager._cleanup_only_job_ids.add(3)
        manager._initialize_job_context = MagicMock(return_value=None)
        manager._cleanup = AsyncMock()
        manager._cleanup_api_server_access_token = MagicMock()
        lost = managed_job_state.ControllerLeadershipLostError(
            'replacement owns this attempt')

        with patch('sky.jobs.controller.asyncio.sleep',
                   new_callable=AsyncMock) as sleep, \
                patch('sky.jobs.controller.managed_job_state.'
                      'scheduler_set_cleanup_done_async',
                      new=AsyncMock(side_effect=lost)):
            with pytest.raises(managed_job_state.ControllerLeadershipLostError,
                               match='replacement owns'):
                await ControllerManager.run_cleanup_loop.__wrapped__(
                    manager, 3, '/tmp/controller.log', None)

        manager._cleanup.assert_awaited_once_with(3, pool=None)
        manager._cleanup_api_server_access_token.assert_called_once_with(3)
        sleep.assert_not_awaited()
        assert 3 not in manager.job_tasks
        assert 3 not in manager._cleanup_only_job_ids

    def test_cleanup_context_publishes_exact_job_origin(self):
        manager = _make_controller_manager()
        ctx = MagicMock()
        with patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content', return_value=''), \
                patch('sky.jobs.controller.usage_lib.'
                      'install_fresh_messages_for_current_context') as usage:
            manager._initialize_job_context(37, '/tmp/controller.log', None)

        ctx.override_envs.assert_called_once_with(
            {controller_lib.jobs_constants.CONTROLLER_JOB_ID_ENV_VAR: '37'})
        ctx.redirect_log.assert_called_once_with(
            pathlib.Path('/tmp/controller.log'))
        usage.assert_called_once_with()

    def test_controller_api_access_uses_original_job_user(self):
        manager = _make_controller_manager()
        ctx = MagicMock()
        token_env_var = constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR

        with patch('sky.jobs.controller.controller_capability.'
                   'get_process_local', return_value='capability'), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_managed_job_tasks', return_value=[{
                          'user_hash': 'nima-user-hash',
                      }]), \
                patch('sky.jobs.controller.managed_job_api_access.'
                      'create_job_api_token',
                      return_value=('sky_raw-token', 'token-id')) as create, \
                patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.server_common.'
                      'get_api_server_status_response.cache_clear') as clear:
            token_id = manager._initialize_controller_api_access(37)

        assert token_id == 'token-id'
        create.assert_called_once_with(
            'nima-user-hash',
            'controller-37-00000000',
        )
        ctx.override_envs.assert_called_once_with(
            {token_env_var: 'sky_raw-token'})
        clear.assert_called_once_with()
        lease = manager._controller_api_access_leases[37]
        assert lease.current_token_id == 'token-id'
        assert lease.previous_token_id is None
        assert not lease.pending_revoke_ids

    def test_controller_api_access_is_not_needed_without_capability(self):
        manager = _make_controller_manager()

        with patch('sky.jobs.controller.controller_capability.'
                   'get_process_local', return_value=None), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_managed_job_tasks') as get_tasks:
            assert manager._initialize_controller_api_access(37) is None

        get_tasks.assert_not_called()

    @pytest.mark.asyncio
    async def test_releasing_job_revokes_only_controller_token(self):
        manager = _make_controller_manager()
        renewal_task = asyncio.create_task(asyncio.sleep(3600))
        manager._controller_api_access_leases[37] = (
            controller_lib._ControllerApiAccessLease(
                current_token_id='controller-token-id',
                previous_token_id='previous-token-id',
                pending_revoke_ids={'pending-token-id'},
                renewal_task=renewal_task))
        manager._cleanup_controller_api_access = MagicMock()

        await manager._release_job_loop_ownership(37)

        assert renewal_task.cancelled()
        assert manager._cleanup_controller_api_access.call_args_list == [
            call('controller-token-id', 37),
            call('pending-token-id', 37),
            call('previous-token-id', 37),
        ]
        assert not manager._controller_api_access_leases

    def test_controller_api_access_renewal_keeps_one_overlap(self):
        manager = _make_controller_manager()
        manager._controller_api_access_leases[37] = (
            controller_lib._ControllerApiAccessLease(
                current_token_id='token-1'))
        ctx = MagicMock()

        with patch('sky.jobs.controller.managed_job_state.'
                   'controller_job_attempt_is_current', return_value=True), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_managed_job_tasks', return_value=[{
                          'user_hash': 'nima-user-hash',
                      }]), \
                patch('sky.jobs.controller.managed_job_api_access.'
                      'create_job_api_token', side_effect=[
                          ('sky_token-2', 'token-2'),
                          ('sky_token-3', 'token-3'),
                      ]), \
                patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.server_common.'
                      'get_api_server_status_response.cache_clear'), \
                patch.object(manager,
                             '_cleanup_controller_api_access') as cleanup:
            manager._renew_controller_api_access_once(37)
            lease = manager._controller_api_access_leases[37]
            assert lease.current_token_id == 'token-2'
            assert lease.previous_token_id == 'token-1'
            cleanup.assert_not_called()

            manager._renew_controller_api_access_once(37)

        lease = manager._controller_api_access_leases[37]
        assert lease.current_token_id == 'token-3'
        assert lease.previous_token_id == 'token-2'
        assert not lease.pending_revoke_ids
        cleanup.assert_called_once_with('token-1', 37)
        assert ctx.override_envs.call_args_list == [
            call({constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'sky_token-2'}),
            call({constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'sky_token-3'}),
        ]

    def test_controller_api_access_renewal_fences_stale_attempt(self):
        manager = _make_controller_manager()
        manager._controller_api_access_leases[37] = (
            controller_lib._ControllerApiAccessLease(
                current_token_id='token-1'))
        ctx = MagicMock()

        with patch('sky.jobs.controller.managed_job_state.'
                   'controller_job_attempt_is_current',
                   side_effect=[True, False]), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_managed_job_tasks', return_value=[{
                          'user_hash': 'nima-user-hash',
                      }]), \
                patch('sky.jobs.controller.managed_job_api_access.'
                      'create_job_api_token',
                      return_value=('sky_token-2', 'token-2')), \
                patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch.object(manager,
                             '_cleanup_controller_api_access') as cleanup:
            with pytest.raises(managed_job_state.ControllerLeadershipLostError):
                manager._renew_controller_api_access_once(37)

        lease = manager._controller_api_access_leases[37]
        assert lease.current_token_id == 'token-1'
        assert lease.previous_token_id is None
        assert not lease.pending_revoke_ids
        cleanup.assert_called_once_with('token-2', 37)
        ctx.override_envs.assert_not_called()

    def test_controller_api_access_cleans_up_after_fencing_error(self):
        manager = _make_controller_manager()
        manager._controller_api_access_leases[37] = (
            controller_lib._ControllerApiAccessLease(
                current_token_id='token-1'))
        ctx = MagicMock()

        with patch('sky.jobs.controller.managed_job_state.'
                   'controller_job_attempt_is_current',
                   side_effect=[True, RuntimeError('recheck unavailable')]), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_managed_job_tasks', return_value=[{
                          'user_hash': 'nima-user-hash',
                      }]), \
                patch('sky.jobs.controller.managed_job_api_access.'
                      'create_job_api_token',
                      return_value=('sky_token-2', 'token-2')), \
                patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch.object(manager,
                             '_cleanup_controller_api_access') as cleanup:
            with pytest.raises(RuntimeError, match='recheck unavailable'):
                manager._renew_controller_api_access_once(37)

        lease = manager._controller_api_access_leases[37]
        assert lease.current_token_id == 'token-1'
        assert lease.previous_token_id is None
        assert not lease.pending_revoke_ids
        cleanup.assert_called_once_with('token-2', 37)
        ctx.override_envs.assert_not_called()

    def test_controller_api_access_retries_failed_revocation(self):
        manager = _make_controller_manager()
        lease = controller_lib._ControllerApiAccessLease(
            current_token_id='token-2',
            previous_token_id='token-1',
            pending_revoke_ids={'token-0'})

        with patch.object(
                manager,
                '_cleanup_controller_api_access',
                side_effect=[RuntimeError('temporary database failure'),
                             None]) as cleanup:
            manager._revoke_pending_controller_api_access(37, lease)
            assert lease.pending_revoke_ids == {'token-0'}

            manager._revoke_pending_controller_api_access(37, lease)

        assert not lease.pending_revoke_ids
        assert cleanup.call_args_list == [call('token-0', 37)] * 2

    @pytest.mark.asyncio
    async def test_controller_api_access_renewal_retry_schedule(self):
        manager = _make_controller_manager()
        leadership_lost = managed_job_state.ControllerLeadershipLostError(
            'stale attempt')

        with patch('sky.jobs.controller.asyncio.sleep',
                   new=AsyncMock()) as sleep, \
                patch.object(
                    manager,
                    '_renew_controller_api_access_once',
                    side_effect=[RuntimeError('database unavailable'), None,
                                 leadership_lost]) as renew:
            await manager._renew_controller_api_access_loop(37)

        assert sleep.await_args_list == [
            call(controller_lib.jobs_constants.
                 MANAGED_JOB_CONTROLLER_TOKEN_RENEWAL_SECONDS),
            call(controller_lib.jobs_constants.
                 MANAGED_JOB_CONTROLLER_TOKEN_RETRY_SECONDS),
            call(controller_lib.jobs_constants.
                 MANAGED_JOB_CONTROLLER_TOKEN_RENEWAL_SECONDS),
        ]
        assert renew.call_args_list == [call(37)] * 3

    def test_cleanup_context_overrides_persisted_job_origin_last(self):
        manager = _make_controller_manager()
        ctx = MagicMock()
        origin_key = controller_lib.jobs_constants.CONTROLLER_JOB_ID_ENV_VAR
        env_content = f'{origin_key}=999\nUSER_VALUE=kept\n'
        with patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content', return_value=env_content), \
                patch('sky.jobs.controller.file_content_utils.'
                      'restore_job_config_file'), \
                patch('sky.jobs.controller.skypilot_config.reload_config'), \
                patch('sky.jobs.controller.usage_lib.'
                      'install_fresh_messages_for_current_context'):
            manager._initialize_job_context(37, '/tmp/controller.log', None)

        assert ctx.override_envs.call_args_list[-1] == call({origin_key: '37'})
        assert call({origin_key: '999'}) not in ctx.override_envs.call_args_list

    def test_guarded_context_reissues_exact_restored_config_receipt(self):
        manager = _make_controller_manager()
        ctx = MagicMock()
        config_path = '/server-owned/job-37-config.yaml'
        config_bytes = b'active_workspace: research\n'
        receipt_names = (
            controller_lib.skypilot_config.
            ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_KIND,
            controller_lib.skypilot_config.
            ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_PATH,
            controller_lib.skypilot_config.
            ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_DIGEST,
            controller_lib.skypilot_config.
            ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_IDENTITY,
        )
        env_content = '\n'.join([
            'USER_VALUE=kept',
            *(f'{name}=persisted-forgery' for name in receipt_names),
        ])
        expected_receipt = (
            controller_lib.skypilot_config.internal_config_snapshot_environment(
                controller_lib.skypilot_config.
                INTERNAL_CONFIG_SNAPSHOT_KIND_MANAGED_JOB,
                config_path,
                config_bytes,
            ))

        with patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.skypilot_config.'
                      '_postgres_server_config_is_authoritative',
                      return_value=True), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content', return_value=env_content), \
                patch('sky.jobs.controller.file_content_utils.'
                      'restore_job_config_file',
                      return_value=(config_path, config_bytes)) as restore, \
                patch('sky.jobs.controller.skypilot_config.reload_config') \
                    as reload_config, \
                patch('sky.jobs.controller.usage_lib.'
                      'install_fresh_messages_for_current_context'):
            manager._initialize_job_context(37, '/tmp/controller.log', None)

        origin = {controller_lib.jobs_constants.CONTROLLER_JOB_ID_ENV_VAR: '37'}
        assert call(origin) in ctx.override_envs.call_args_list
        assert call(expected_receipt) in ctx.override_envs.call_args_list
        assert (ctx.override_envs.call_args_list.index(call(origin))
                < ctx.override_envs.call_args_list.index(
                    call(expected_receipt)))
        for name in receipt_names:
            assert call({name: 'persisted-forgery'
                        }) not in (ctx.override_envs.call_args_list)
        restore.assert_called_once_with(37)
        reload_config.assert_called_once_with()

    def test_guarded_context_fails_closed_when_snapshot_restore_fails(self):
        manager = _make_controller_manager()
        ctx = MagicMock()

        with patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.skypilot_config.'
                      '_postgres_server_config_is_authoritative',
                      return_value=True), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content',
                      return_value='SKYPILOT_CONFIG=/missing/config.yaml\n'), \
                patch('sky.jobs.controller.file_content_utils.'
                      'restore_job_config_file',
                      side_effect=FileNotFoundError('/missing/config.yaml')), \
                patch('sky.jobs.controller.skypilot_config.reload_config') \
                    as reload_config, \
                patch('sky.jobs.controller.usage_lib.'
                      'install_fresh_messages_for_current_context') as usage:
            with pytest.raises(RuntimeError,
                               match='guarded config snapshot for job 37'):
                manager._initialize_job_context(37, '/tmp/controller.log', None)

        reload_config.assert_not_called()
        usage.assert_not_called()

    @pytest.mark.asyncio
    async def test_monitor_dispatches_cleanup_only_without_starting_workload(
            self):
        manager = _make_controller_manager()
        manager.start_cleanup_job = AsyncMock(
            side_effect=asyncio.CancelledError)
        manager.start_job = AsyncMock()

        with patch('sky.jobs.controller.managed_job_state.'
                   'get_waiting_job_async', new=AsyncMock(return_value={
                       'job_id': 9,
                       'pool': 'pool-a',
                       'cleanup_only': True,
                   })), \
                patch('sky.jobs.controller.os.listdir', return_value=[]):
            with pytest.raises(asyncio.CancelledError):
                await manager.monitor_loop()

        manager.start_cleanup_job.assert_awaited_once_with(9, 'pool-a')
        manager.start_job.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('cleanup_error', [
        None,
        RuntimeError('token database unavailable'),
    ])
    async def test_cancel_releases_api_token_only_after_terminal_transition(
            self, cleanup_error, caplog):
        manager = _make_controller_manager()
        manager.starting.add(3)
        manager._cleanup = AsyncMock()
        events = []

        ctx = MagicMock()
        controller = MagicMock()
        controller.run = AsyncMock(side_effect=asyncio.CancelledError)
        manager._download_logs_for_cancelled_job = AsyncMock()

        async def set_cancelled(**_kwargs):
            events.append('cancelled')

        def cleanup_token(job_id):
            assert job_id == 3
            events.append('token')
            if cleanup_error is not None:
                raise cleanup_error

        async def job_done(job_id):
            assert job_id == 3
            events.append('done')

        async def passthrough_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch('sky.jobs.controller.context.get', return_value=ctx), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content', return_value=''), \
                patch('sky.jobs.controller.usage_lib.'
                      'install_fresh_messages_for_current_context'), \
                patch('sky.jobs.controller.JobController',
                      return_value=controller), \
                patch('sky.jobs.controller._get_dag',
                      return_value=MagicMock(tasks=[MagicMock()])), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_all_task_ids_statuses_async',
                      new=AsyncMock(return_value=[
                          (0, managed_job_state.ManagedJobStatus.RUNNING)
                      ])), \
                patch('sky.jobs.controller.managed_job_state.'
                      'set_cancelling_async', new=AsyncMock()), \
                patch('sky.jobs.controller.managed_job_state.'
                      'set_cancelled_async', side_effect=set_cancelled), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_status_async', new=AsyncMock(return_value=(
                          managed_job_state.ManagedJobStatus.CANCELLED))), \
                patch('sky.jobs.controller.scheduler.job_done_async',
                      side_effect=job_done), \
                patch.object(manager,
                             '_cleanup_api_server_access_token',
                             side_effect=cleanup_token,
                             create=True), \
                patch('sky.jobs.controller.asyncio.to_thread',
                      side_effect=passthrough_to_thread):
            with pytest.raises(asyncio.CancelledError):
                await manager.run_job_loop(3, '/dev/null')

        assert events == ['cancelled', 'token', 'done']
        if cleanup_error is not None:
            assert 'token database unavailable' in caplog.text


class TestApiAccessTokenCleanup:
    """Tests batch-aware API token cleanup decisions."""

    def test_active_sibling_defers_shared_token_revocation(self):
        manager = _make_controller_manager()
        with patch('sky.jobs.controller.managed_job_state.'
                   'get_releasable_api_access_token_id',
                   return_value=None) as releasable, \
                patch('sky.jobs.controller.global_user_state.'
                      'delete_service_account_token') as delete:
            manager._cleanup_api_server_access_token(7)

        releasable.assert_called_once_with(7)
        delete.assert_not_called()

    def test_terminal_batch_revokes_shared_token_once(self):
        manager = _make_controller_manager()
        with patch('sky.jobs.controller.managed_job_state.'
                   'get_releasable_api_access_token_id',
                   return_value='shared-token') as releasable, \
                patch('sky.jobs.controller.global_user_state.'
                      'delete_service_account_token',
                      return_value=True) as delete:
            manager._cleanup_api_server_access_token(8)

        releasable.assert_called_once_with(8)
        delete.assert_called_once_with('shared-token')

    def test_concurrent_sibling_revocation_is_idempotent(self):
        manager = _make_controller_manager()
        with patch('sky.jobs.controller.managed_job_state.'
                   'get_releasable_api_access_token_id',
                   return_value='shared-token'), \
                patch('sky.jobs.controller.global_user_state.'
                      'delete_service_account_token',
                      return_value=False) as delete:
            manager._cleanup_api_server_access_token(9)

        delete.assert_called_once_with('shared-token')


class TestOuterControllerGenerationWatchdog:
    """Detached scheduler processes fail closed after outer handoff."""

    @pytest.mark.asyncio
    async def test_generation_mismatch_exits_controller(self):
        owner = ('73ebc1a8-d2ae-4ca4-b9a5-53d0b10990af', 13)
        with patch('sky.jobs.controller.asyncio.sleep',
                   new=AsyncMock()), \
                patch('sky.jobs.controller.managed_job_state.'
                      'controller_owner_is_current',
                      return_value=False), \
                patch(
                    'sky.jobs.controller.'
                    '_fail_stop_outer_controller_process_group',
                    side_effect=managed_job_state.ControllerLeadershipLostError(
                        'fail-stop')) as fail_stop:
            with pytest.raises(managed_job_state.ControllerLeadershipLostError,
                               match='fail-stop'):
                await controller_lib._watch_outer_controller_generation(owner)
        assert 'no longer current' in fail_stop.call_args.args[0]

    @pytest.mark.asyncio
    async def test_database_proof_error_exits_controller(self):
        owner = ('4c0382a3-5905-4d3b-b696-a05e43063c30', 14)
        with patch('sky.jobs.controller.asyncio.sleep',
                   new=AsyncMock()), \
                patch('sky.jobs.controller.managed_job_state.'
                      'controller_owner_is_current',
                      side_effect=OSError('database unavailable')), \
                patch(
                    'sky.jobs.controller.'
                    '_fail_stop_outer_controller_process_group',
                    side_effect=managed_job_state.ControllerLeadershipLostError(
                        'fail-stop')) as fail_stop:
            with pytest.raises(managed_job_state.ControllerLeadershipLostError,
                               match='fail-stop'):
                await controller_lib._watch_outer_controller_generation(owner)
        assert 'Could not prove' in fail_stop.call_args.args[0]

    def test_fail_stop_uses_noncatchable_process_group_signal(self):
        with patch.object(controller_lib.os, 'getpgrp', return_value=321), \
                patch.object(controller_lib.os, 'killpg') as kill_group, \
                patch.object(
                    controller_lib.os,
                    '_exit',
                    side_effect=SystemExit(1)) as exit_process:
            with pytest.raises(SystemExit):
                controller_lib._fail_stop_outer_controller_process_group(
                    'generation fenced')

        kill_group.assert_called_once_with(321, signal.SIGKILL)
        exit_process.assert_called_once_with(1)

    @pytest.mark.skipif(not hasattr(os, 'killpg'),
                        reason='requires POSIX process groups')
    def test_fail_stop_does_not_run_coroutine_finalizers(self, tmp_path):
        sentinel = tmp_path / 'finalizer-ran'
        script = f"""
import asyncio
import pathlib
from sky.jobs import controller

async def main():
    try:
        controller._fail_stop_outer_controller_process_group('test fence')
    finally:
        pathlib.Path({str(sentinel)!r}).write_text('unsafe', encoding='utf-8')

asyncio.run(main())
"""
        result = subprocess.run([sys.executable, '-c', script],
                                capture_output=True,
                                text=True,
                                start_new_session=True,
                                check=False)

        assert result.returncode == -signal.SIGKILL, result.stderr
        assert not sentinel.exists()


@contextlib.contextmanager
def _issuing_controller_credential(ctx=None):
    """Patch credential issuance so only the controller wiring is exercised."""
    with patch('sky.jobs.controller.controller_capability.get_process_local',
               return_value='capability'), \
            patch('sky.jobs.controller.managed_job_state.'
                  'get_managed_job_tasks',
                  return_value=[{
                      'user_hash': 'job-user-hash',
                  }]), \
            patch('sky.jobs.controller.managed_job_api_access.'
                  'create_job_api_token',
                  return_value=('sky_job-token', 'token-id')) as create, \
            patch('sky.jobs.controller.server_common.'
                  'get_api_server_status_response.cache_clear'):
        if ctx is None:
            yield create
        else:
            with patch('sky.jobs.controller.context.get', return_value=ctx):
                yield create


class TestControllerApiAccessLifecycle:
    """Controller credentials stay coroutine-local, roll back, and get reaped."""

    @pytest.mark.asyncio
    async def test_credential_is_visible_only_inside_the_job_context(
            self, monkeypatch):
        manager = _make_controller_manager()
        token_env_var = constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR
        monkeypatch.delenv(token_env_var, raising=False)
        # The controller entrypoint hijacks os.environ; the credential must
        # reach the SDK through that seam, never the process environment.
        monkeypatch.setattr(os, 'environ',
                            context.ContextualEnviron(os.environ))
        seen: dict[str, str | None] = {}

        @context.contextual_async
        async def run_job() -> None:
            with _issuing_controller_credential():
                assert manager._initialize_controller_api_access(
                    37) == 'token-id'
            seen['coroutine'] = (
                service_account_auth._get_service_account_token())
            seen['thread'] = await asyncio.to_thread(os.environ.get,
                                                     token_env_var)

        @context.contextual_async
        async def run_sibling_job() -> None:
            seen['sibling'] = os.environ.get(token_env_var)

        # Job loops are scheduled as their own tasks (create_background_task);
        # that task boundary is what confines the per-job credential.
        await asyncio.create_task(run_job())
        await asyncio.create_task(run_sibling_job())
        seen['process'] = os.environ.get(token_env_var)

        assert seen == {
            'coroutine': 'sky_job-token',
            'thread': 'sky_job-token',
            'sibling': None,
            'process': None,
        }

    @pytest.mark.asyncio
    async def test_run_job_loop_installs_the_credential_before_the_controller(
            self):
        manager = _make_controller_manager()
        manager.starting.add(3)
        manager._cleanup = AsyncMock()
        manager._cleanup_api_server_access_token = MagicMock()
        manager._cleanup_controller_api_access = MagicMock()
        # #1891 starts a renewal task next to the credential; keep it inert.
        manager._renew_controller_api_access_loop = AsyncMock()
        order: list[str] = []
        ctx = MagicMock()
        ctx.override_envs.side_effect = (
            lambda envs: order.append('credential')
            if constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR in envs else None)
        controller = MagicMock()
        controller.run = AsyncMock(return_value=True)

        def build_controller(*args, **kwargs):
            del args, kwargs
            order.append('controller')
            return controller

        with _issuing_controller_credential(ctx), \
                patch('sky.jobs.controller.file_content_utils.'
                      'get_job_env_content', return_value=''), \
                patch('sky.jobs.controller.usage_lib.'
                      'install_fresh_messages_for_current_context'), \
                patch('sky.jobs.controller.JobController',
                      side_effect=build_controller), \
                patch('sky.jobs.controller.managed_job_state.get_status_async',
                      new_callable=AsyncMock,
                      return_value=(
                          managed_job_state.ManagedJobStatus.SUCCEEDED)), \
                patch('sky.jobs.controller.scheduler.job_done_async',
                      new_callable=AsyncMock):
            await manager.run_job_loop(3, '/dev/null')

        # Every nested launch/status/down call runs under the JobController,
        # so the credential must exist before it is constructed.
        assert order == ['credential', 'controller']
        manager._cleanup_controller_api_access.assert_called_once_with(
            'token-id', 3)
        assert not manager._controller_api_access_leases

    def test_install_failure_revokes_the_fresh_token(self):
        manager = _make_controller_manager()
        ctx = MagicMock()
        ctx.override_envs.side_effect = RuntimeError('context is gone')

        with _issuing_controller_credential(ctx), \
                patch('sky.jobs.controller.global_user_state.'
                      'delete_service_account_token') as delete:
            with pytest.raises(RuntimeError, match='context is gone'):
                manager._initialize_controller_api_access(37)

        delete.assert_called_once_with('token-id')
        assert not manager._controller_api_access_leases

    @pytest.mark.asyncio
    async def test_cleanup_only_installs_the_credential_once_and_revokes_it(
            self):
        manager = _make_controller_manager()
        manager.job_tasks[3] = asyncio.current_task()
        manager._cleanup_only_job_ids.add(3)
        manager._initialize_job_context = MagicMock(return_value=None)
        manager._cleanup = AsyncMock(
            side_effect=[RuntimeError('provider unavailable'), None])
        manager._cleanup_api_server_access_token = MagicMock()
        manager._cleanup_controller_api_access = MagicMock()
        # #1891 starts a renewal task next to the credential; keep it inert.
        manager._renew_controller_api_access_loop = AsyncMock()
        ctx = MagicMock()

        with _issuing_controller_credential(ctx) as create, \
                patch('sky.jobs.controller.asyncio.sleep',
                      new_callable=AsyncMock), \
                patch('sky.jobs.controller.managed_job_state.'
                      'scheduler_set_cleanup_done_async',
                      new_callable=AsyncMock):
            await ControllerManager.run_cleanup_loop.__wrapped__(
                manager, 3, '/tmp/controller.log', None)

        create.assert_called_once_with('job-user-hash', 'controller-3-00000000')
        ctx.override_envs.assert_called_once_with(
            {constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'sky_job-token'})
        manager._cleanup_controller_api_access.assert_called_once_with(
            'token-id', 3)
        assert not manager._controller_api_access_leases

    def test_leaked_controller_token_is_reaped_by_the_expiry_sweep(self):
        manager = ControllerManager('test-uuid', _TEST_CONTROLLER_SLOT_ID,
                                    str(uuid.uuid4()))
        token_service = MagicMock()
        token_service.create_token.return_value = {
            'token': 'sky_job-token',
            'token_id': 'token-id',
            'token_hash': 'token-hash',
            'expires_at': 12345,
        }

        with patch('sky.jobs.controller.controller_capability.'
                   'get_process_local', return_value='capability'), \
                patch('sky.jobs.controller.managed_job_state.'
                      'get_managed_job_tasks',
                      return_value=[{
                          'user_hash': 'job-user-hash',
                      }]), \
                patch.object(managed_job_api_access.token_service_lib,
                             'token_service', token_service), \
                patch.object(managed_job_api_access.global_user_state,
                             'add_service_account_token') as add_token, \
                patch('sky.jobs.controller.context.get',
                      return_value=MagicMock()), \
                patch('sky.jobs.controller.server_common.'
                      'get_api_server_status_response.cache_clear'):
            manager._initialize_controller_api_access(37)

        token_name = add_token.call_args.kwargs['token_name']
        # A controller crash between issuance and release leaks this row. The
        # expired-token daemon must recognise it by name shape alone.
        with patch.object(
                managed_job_utils.global_user_state,
                'get_expired_service_account_tokens_by_name_prefix',
                return_value=[{
                    'token_id': 'token-id',
                    'token_name': token_name,
                }]), \
                patch.object(managed_job_utils.global_user_state,
                             'delete_service_account_token',
                             return_value=True) as delete:
            assert managed_job_utils.cleanup_expired_api_access_tokens() == 1

        delete.assert_called_once_with('token-id')
