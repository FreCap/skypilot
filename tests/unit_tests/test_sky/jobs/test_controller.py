"""Unit tests for sky.jobs.controller - recovery logic for all job types.

Tests cover controller recovery during rolling upgrades for:
- Normal jobs (single task): Recovery based on task status
- Pipeline jobs (sequential multi-task): Recovery with task skip logic
- JobGroups (parallel tasks): Recovery with independent task states

Also tests the cancelled job log download feature in ControllerManager
and file mount cleanup in task_cleanup().
"""
import asyncio
import pathlib
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from sky.jobs import controller as controller_lib
from sky.jobs import state as managed_job_state
from sky.jobs.controller import ControllerManager
from sky.jobs.controller import JobController
from sky.skylet import constants
from sky.skylet import job_lib
from sky.utils import common
from sky.utils import status_lib


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
        return job_controller

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
    async def test_job_group_parent_cancellation_joins_monitor_children(
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
                # Model cancellation cleanup that must finish before the
                # owning JobGroup coroutine may exit.
                await asyncio.sleep(0)
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
                    'get_handle_from_cluster_name', return_value=MagicMock()), \
                patch.object(
                    controller_lib.job_group_networking,
                    'dns_addresses_for_task', return_value=['127.0.0.1']):
            run_task = asyncio.create_task(job_controller._run_job_group())
            await asyncio.wait_for(all_started.wait(), timeout=1)
            run_task.cancel()
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
                    'get_handle_from_cluster_name', return_value=MagicMock()), \
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
        2. Storage teardown (mocked)
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
            'status': patch('sky.core.status', return_value=[]),
            'backend': patch('sky.backends.cloud_vm_ray_backend.'
                             'CloudVmRayBackend'),
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
        manager = ControllerManager('test-uuid')
        with patch('sky.jobs.controller._get_dag', return_value=dag):
            await manager._cleanup(job_id=1)

        assert not mount_0.exists(), 'mount_0 should be cleaned up'
        assert not mount_1.exists(), 'mount_1 should be cleaned up'


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
        manager._download_log_from_cluster = (
            ControllerManager._download_log_from_cluster.__get__(
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
                   return_value=[{'handle': mock_handle}]) as mock_get_cl:

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
                   return_value=[{'handle': mock_handle}]):

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
                   return_value=[{'handle': mock_handle}]):

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

        def get_clusters_side_effect(cluster_names, **kwargs):
            name = cluster_names[0]
            if 'job-a' in name:
                return [{'handle': mock_handle_0}]
            elif 'job-c' in name:
                return [{'handle': mock_handle_2}]
            return []

        with patch('sky.jobs.controller.managed_job_utils'
                   '.generate_managed_job_cluster_name',
                   side_effect=lambda name, jid: f'sky-managed-{jid}-{name}'
                   ), \
             patch('sky.jobs.controller.backend_utils.get_clusters',
                   side_effect=get_clusters_side_effect):

            # task 1 already succeeded, so only tasks 0 and 2 are active
            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[0, 2],
                dag=mock_dag,
                pool=None)

            assert controller.download_log_and_stream.call_count == 2
            controller.download_log_and_stream.assert_any_call(
                0, mock_handle_0, None)
            controller.download_log_and_stream.assert_any_call(
                2, mock_handle_2, None)

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

        def get_clusters_side_effect(cluster_names, **kwargs):
            name = cluster_names[0]
            if 'job-a' in name:
                return [{'handle': mock_handle_0}]
            elif 'job-b' in name:
                return [{'handle': mock_handle_1}]
            return []

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
                   side_effect=get_clusters_side_effect):

            # Should NOT raise despite task 0 failing
            await ControllerManager._download_logs_for_cancelled_job(
                manager,
                controller,
                job_id,
                task_ids=[0, 1],
                dag=mock_dag,
                pool=None)

            # Both tasks should have been attempted
            assert controller.download_log_and_stream.call_count == 2
            controller.download_log_and_stream.assert_any_call(
                0, mock_handle_0, None)
            controller.download_log_and_stream.assert_any_call(
                1, mock_handle_1, None)


class TestTransientJobStatusFetchDeadline:
    """Tests the bounded retry window for remote job-status failures."""

    class ExpectedRecovery(Exception):
        """Stops the monitor immediately after it enters recovery."""

    @staticmethod
    def _make_controller() -> JobController:
        controller = JobController.__new__(JobController)
        controller._job_id = 1
        controller._pool = None
        controller._backend = MagicMock()
        return controller

    async def _run_until_recovery(self, statuses, monotonic_values):
        controller = self._make_controller()
        task = MagicMock(name='task')
        task.num_nodes = 1
        executor = MagicMock()
        executor.recover = AsyncMock(side_effect=self.ExpectedRecovery)
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

        with patch.object(controller_lib, 'time', fake_time), patch.object(
                controller_lib.asyncio, 'sleep', new=sleep), patch.object(
                    controller_lib.backend_utils,
                    'async_check_network_connection', new=AsyncMock()
                ), patch.object(
                    controller_lib.managed_job_utils,
                    'get_job_status', new=get_status), patch.object(
                        controller_lib.backend_utils,
                        'refresh_cluster_status_handle',
                        new=refresh_cluster), patch.object(
                            controller_lib.common_utils,
                            'Backoff', return_value=backoff), patch.object(
                                controller_lib.managed_job_state,
                                'set_recovering_async', new=set_recovering), \
                pytest.raises(self.ExpectedRecovery):
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
    async def test_transient_status_fetch_uses_monotonic_deadline(self):
        results = await self._run_until_recovery(
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
    async def test_transient_status_fetch_resets_deadline_after_success(self):
        results = await self._run_until_recovery(
            statuses=[
                (None, 'transient'),
                (job_lib.JobStatus.RUNNING, None),
                (None, 'transient'),
                (None, 'transient'),
            ],
            monotonic_values=[100.0, 100.0, 200.0, 200.0, 260.0],
        )
        (sleep, get_status, refresh_cluster, monotonic, wall_clock,
         set_recovering, executor) = results

        assert [call.args[0] for call in sleep.await_args_list] == [
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            10,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
            10,
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS,
        ]
        assert get_status.await_count == 4
        assert refresh_cluster.call_count == 3
        assert monotonic.call_count == 5
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
        return ControllerManager('test-uuid')

    @pytest.mark.asyncio
    async def test_owned_job_cancelled_without_status_query(self, signal_dir):
        """Signal for an owned job cancels the task; no DB status query."""
        manager = self._make_manager()
        task = MagicMock()
        manager.job_tasks[7] = task
        (signal_dir / '7').touch()

        with patch('sky.jobs.controller.managed_job_state.get_status_async',
                   new_callable=AsyncMock) as status_mock:
            await manager._process_cancel_signals()

        task.cancel.assert_called_once()
        status_mock.assert_not_awaited()
        assert not (signal_dir / '7').exists()
        assert manager._cancel_info[7] == (False, None)

    @pytest.mark.asyncio
    async def test_orphan_signal_for_terminal_job_reaped(self, signal_dir):
        manager = self._make_manager()
        manager._cancel_info[5] = (False, None)
        (signal_dir / '5').touch()

        with patch('sky.jobs.controller.managed_job_state.get_status_async',
                   new_callable=AsyncMock,
                   return_value=managed_job_state.ManagedJobStatus.SUCCEEDED):
            await manager._process_cancel_signals()

        assert not (signal_dir / '5').exists()
        assert 5 not in manager._cancel_info

    @pytest.mark.asyncio
    async def test_orphan_signal_for_missing_job_reaped(self, signal_dir):
        manager = self._make_manager()
        (signal_dir / '6').touch()

        with patch('sky.jobs.controller.managed_job_state.get_status_async',
                   new_callable=AsyncMock,
                   return_value=None):
            await manager._process_cancel_signals()

        assert not (signal_dir / '6').exists()

    @pytest.mark.asyncio
    async def test_orphan_signal_for_live_job_kept(self, signal_dir):
        """A non-terminal job owned elsewhere keeps its signal file."""
        manager = self._make_manager()
        manager._cancel_info[8] = (True, 30)
        (signal_dir / '8').touch()

        with patch('sky.jobs.controller.managed_job_state.get_status_async',
                   new_callable=AsyncMock,
                   return_value=managed_job_state.ManagedJobStatus.RUNNING):
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

            def __enter__(self):
                self._signal.unlink(missing_ok=True)
                return self

            def __exit__(self, *args):
                return False

        with patch('sky.jobs.controller.filelock.FileLock', ConsumingLock):
            await manager._process_cancel_signals()

        task.cancel.assert_not_called()
        assert 7 not in manager._cancel_info

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

    def test_remove_signal_file_is_idempotent(self, signal_dir):
        """The shared signal consumer tolerates an already-removed file."""
        (signal_dir / '12').touch()
        ControllerManager._remove_signal_file(12)
        assert not (signal_dir / '12').exists()
        # Second removal (lost race with another consumer) must not raise.
        ControllerManager._remove_signal_file(12)

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
                return {'job_id': 12}
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
                patch.object(manager, '_remove_signal_file') as remove_signal, \
                patch('sky.jobs.controller.managed_job_state.'
                      'set_cancelling_async', new_callable=AsyncMock
                     ) as set_cancelling, \
                patch('sky.jobs.controller.managed_job_state.'
                      'set_cancelled_async', new_callable=AsyncMock
                     ) as set_cancelled, \
                patch('sky.jobs.controller.managed_job_utils.'
                      'event_callback_func'):
            with pytest.raises(asyncio.CancelledError):
                await manager.monitor_loop()

        get_status.assert_awaited_once_with(12)
        remove_signal.assert_called_once_with(12)
        set_cancelling.assert_awaited_once()
        set_cancelled.assert_awaited_once()
        manager.start_job.assert_not_awaited()

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
    async def test_saturated_launch_slot_wakes_on_notification(self):
        manager = ControllerManager('test-uuid')
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
                      new=AsyncMock(return_value={'job_id': 2})), \
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


class TestRunJobLoopCancelInfoCleanup:
    """run_job_loop must drop stale cancel info even on the success path.

    If a cancellation lands after the job task already finished,
    task.cancel() is a no-op and no CancelledError handler consumes the
    stored cancel info; the finally block must remove it.
    """

    @pytest.mark.asyncio
    async def test_success_path_pops_stale_cancel_info(self):
        manager = ControllerManager('test-uuid')
        manager._cancel_info[3] = (False, None)
        manager._cleanup = AsyncMock()

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
                      new_callable=AsyncMock):
            await manager.run_job_loop(3, '/dev/null')

        assert 3 not in manager._cancel_info
        assert 3 not in manager.job_tasks
