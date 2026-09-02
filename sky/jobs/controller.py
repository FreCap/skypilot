"""Controller: handles scheduling and the life cycle of a managed job.
"""
import asyncio
import io
import json
import os
import pathlib
import resource
import shutil
import signal
import sys
import threading
import time
import traceback
import typing
from typing import Any, Optional

import anyio
import dotenv
import filelock

import sky
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend
from sky.batch import coordinator as batch_coordinator
from sky.client import sdk
from sky.data import data_utils
from sky.jobs import api_access as managed_job_api_access
from sky.jobs import constants as jobs_constants
from sky.jobs import file_content_utils
from sky.jobs import job_group_networking
from sky.jobs import log_gc
from sky.jobs import recovery_strategy
from sky.jobs import runtime as managed_job_runtime
from sky.jobs import scheduler
from sky.jobs import state as managed_job_state
from sky.jobs import utils as managed_job_utils
from sky.metrics import utils as metrics_lib
from sky.server import common as server_common
from sky.server import plugins
from sky.skylet import constants
from sky.skylet import job_lib
from sky.usage import usage_lib
from sky.utils import annotations
from sky.utils import asyncio_utils
from sky.utils import common
from sky.utils import common_utils
from sky.utils import context
from sky.utils import context_utils
from sky.utils import controller_capability
from sky.utils import controller_utils
from sky.utils import dag_utils
from sky.utils import status_lib
from sky.utils import ux_utils
from sky.utils.db import db_utils
from sky.utils.plugin_extensions import ExternalClusterFailure
from sky.utils.plugin_extensions import ExternalFailureSource

if typing.TYPE_CHECKING:
    import psutil

    from sky import task as task_lib
    from sky.schemas.generated import jobsv1_pb2
else:
    psutil = adaptors_common.LazyImport('psutil')
    jobsv1_pb2 = adaptors_common.LazyImport('sky.schemas.generated.jobsv1_pb2')

logger = sky_logging.init_logger('sky.jobs.controller')

_background_tasks: set[asyncio.Task] = set()

# How many consecutive monitor ticks must observe a non-UP cluster while the
# job itself still reports a non-terminal status, or its status is temporarily
# unavailable after a confirmed healthy observation. The cluster health probe
# is all-or-nothing (every node must appear in `ray status`), so a transiently
# lagging raylet or probe-timing hiccup can flag a healthy cluster and recovery
# then tears it down. Requiring consecutive INIT confirmations (~30s at the 15s
# tick) protects the ambiguous path; confirmed STOPPED/missing clusters and
# failures without prior healthy evidence recover immediately.
_NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY = 3
_FILE_MOUNTS_BLOB_ID_UNSET = object()
_OUTER_CONTROLLER_PROBE_SECONDS = 2
_TERMINAL_CLEANUP_RETRY_INITIAL_SECONDS = 1
_TERMINAL_CLEANUP_RETRY_MAX_SECONDS = 30
_CONTROLLER_RUNTIME_ENV_VARS = frozenset({
    jobs_constants.CONTROLLER_OWNER_MODE_ENV_VAR,
    jobs_constants.CONTROLLER_OWNER_INSTANCE_ID_ENV_VAR,
    jobs_constants.CONTROLLER_OWNER_GENERATION_ENV_VAR,
    jobs_constants.CONTROLLER_OWNER_PID_ENV_VAR,
    jobs_constants.CONTROLLER_OWNER_START_TICKS_ENV_VAR,
    jobs_constants.CONTROLLER_JOB_ID_ENV_VAR,
    jobs_constants.CONTROLLER_SLOT_ID_ENV_VAR,
    jobs_constants.CONTROLLER_SLOT_ATTEMPT_ENV_VAR,
    jobs_constants.CONTROLLER_READY_FD_ENV_VAR,
    jobs_constants.CONTROLLER_CAPABILITY_FD_ENV_VAR,
    jobs_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR,
    jobs_constants.CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR,
    skypilot_config.ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_KIND,
    skypilot_config.ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_PATH,
    skypilot_config.ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_DIGEST,
    skypilot_config.ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_IDENTITY,
})


def _fail_stop_outer_controller_process_group(reason: str) -> typing.NoReturn:
    """Exit a fenced scheduler without running managed-job finalizers."""
    logger.error(reason)
    try:
        os.killpg(os.getpgrp(), signal.SIGKILL)
    except OSError:
        logger.exception(
            'Failed to SIGKILL the fenced scheduler process group.')
    # killpg() does not return on success.  This fallback preserves fail-stop
    # semantics if the process group disappeared or could not be signaled.
    os._exit(1)  # pylint: disable=protected-access


async def _watch_outer_controller_generation(owner: tuple[str, int]) -> None:
    """Exit the detached scheduler when its outer generation is fenced."""
    instance_id, generation = owner
    while True:
        await asyncio.sleep(_OUTER_CONTROLLER_PROBE_SECONDS)
        try:
            is_current = await asyncio.to_thread(
                managed_job_state.controller_owner_is_current, owner)
        except Exception as e:  # pylint: disable=broad-except
            _fail_stop_outer_controller_process_group(
                'Could not prove managed-job outer controller generation '
                f'{generation} for instance {instance_id}: '
                f'{common_utils.format_exception(e)}')
        if not is_current:
            _fail_stop_outer_controller_process_group(
                'Managed-job outer controller generation '
                f'{generation} for instance {instance_id} is no longer '
                'current.')


class _ClusterNotUpDebouncer:
    """Debounce ambiguous INIT observations for a possibly running job.

    Multi-node jobs use the configured threshold for every ambiguous INIT.
    Single-node jobs normally recover on the first observation, but prior
    healthy status can raise the effective threshold for a transient fetch
    outage. The current effective threshold is retained for diagnostics.
    """

    def __init__(self, num_nodes: int) -> None:
        self._threshold = (_NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
                           if num_nodes > 1 else 1)
        self._required_confirmations = self._threshold
        self._consecutive_not_up = 0

    def should_recover_now(self,
                           required_confirmations: int | None = None) -> bool:
        """Record an ambiguous INIT observation.

        Returns True once enough consecutive observations accumulated for
        recovery to proceed.
        """
        self._consecutive_not_up += 1
        self._required_confirmations = (self._threshold
                                        if required_confirmations is None else
                                        required_confirmations)
        return self._consecutive_not_up >= self._required_confirmations

    @property
    def observations(self) -> int:
        return self._consecutive_not_up

    @property
    def threshold(self) -> int:
        return self._threshold

    @property
    def required_confirmations(self) -> int:
        return self._required_confirmations

    def reset(self) -> None:
        self._required_confirmations = self._threshold
        self._consecutive_not_up = 0


def _should_wait_for_cluster_not_up_confirmation(
        cluster_status: status_lib.ClusterStatus | None,
        job_status: job_lib.JobStatus | None,
        transient_job_check_error_reason: str | None,
        last_known_job_status: job_lib.JobStatus | None,
        debouncer: _ClusterNotUpDebouncer) -> bool:
    """Return whether a not-UP cluster verdict needs more confirmation.

    ``INIT`` can be a transient health-probe false positive. That same
    control-plane flap can also make the job-status fetch temporarily
    unavailable, so both cases share the same confirmation gate. For
    single-node jobs, require prior healthy evidence before delaying
    recovery on a transient status-fetch outage.
    """
    if cluster_status != status_lib.ClusterStatus.INIT:
        return False
    if job_status is not None:
        if job_status.is_terminal():
            return False
    elif transient_job_check_error_reason is None:
        return False
    else:
        if debouncer.threshold == 1:
            if (last_known_job_status is None or
                    last_known_job_status.is_terminal()):
                return False
            return not debouncer.should_recover_now(
                _NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY)
    return not debouncer.should_recover_now()


def _should_keep_monitoring_healthy_cluster(
        last_known_job_status: job_lib.JobStatus | None,
        transient_job_check_error_reason: str | None, num_nodes: int) -> bool:
    """Return whether a healthy cluster should keep waiting for job status.

    A transient control-plane failure is not evidence that a previously
    running job has died. If the cluster is still UP and the last confirmed
    remote status was non-terminal, keep monitoring instead of tearing the job
    down and relaunching it.

    Only single-node jobs qualify. For multi-node jobs a non-terminal
    job_status is not a reliable health signal: the job may not be set to
    FAILED immediately when only some of the nodes are preempted or fail, so a
    stale non-terminal status must not keep a possibly-dead multi-node job
    alive. This mirrors the ``task.num_nodes == 1`` gate on the healthy job
    fast path in ``_monitor_one_task``.
    """
    if transient_job_check_error_reason is None:
        return False
    if last_known_job_status is None:
        return False
    if num_nodes != 1:
        return False
    return not last_known_job_status.is_terminal()


def create_background_task(coro: typing.Coroutine) -> asyncio.Task:
    """Create a background task and add it to the set of background tasks.

    Main reason we do this is since tasks are only held as a weak reference in
    the executor, we need to keep a strong reference to the task to avoid it
    being garbage collected.

    Args:
        coro: The coroutine to create a task for.
    """
    # Registration and callbacks run on the controller's single event-loop
    # thread, so a second asyncio lock only added a cancellation point between
    # reserving a launch slot and handing ownership to the task.
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


# Make sure to limit the size as we don't want to cache too many DAGs in memory.
@annotations.lru_cache(scope='global', maxsize=50)
def _get_dag(job_id: int) -> 'sky.Dag':
    dag_content = file_content_utils.get_job_dag_content(job_id)
    if dag_content is None:
        raise RuntimeError('Managed job DAG YAML content is unavailable for '
                           f'job {job_id}. This can happen if the job was '
                           'submitted before file migration completed or if '
                           'the submission failed to persist the DAG. Please '
                           're-submit the job.')

    # Auto-detect YAML type (JobGroup or chain DAG) and parse accordingly
    dag = dag_utils.load_dag_from_yaml_str(dag_content)
    assert dag.name is not None, dag
    return dag


def _add_k8s_annotations(task: 'sky.Task', job_id: int) -> None:
    """Adds Kubernetes pod config annotations to the task resources.

    This function is a NOP for non-Kubernetes resources, as
    the kubernetes specific config is not used when launching
    a cluster on other clouds.
    """
    original_resources = task.resources
    new_resources_list: list[sky.Resources] = []
    for original_resource in original_resources:
        # Get existing config overrides or create new dict
        config_overrides = original_resource.cluster_config_overrides.copy()

        # Initialize nested structure and add annotations
        pod_annotations = config_overrides.setdefault(
            'kubernetes',
            {}).setdefault('pod_config',
                           {}).setdefault('metadata',
                                          {}).setdefault('annotations', {})
        pod_annotations['skypilot-managed-job-id'] = str(job_id)
        pod_annotations['skypilot-managed-job-name'] = str(task.name)
        # Create new resource with updated config
        new_resource = original_resource.copy(
            _cluster_config_overrides=config_overrides)
        new_resources_list.append(new_resource)

    # Set the new resources back to the task
    task.set_resources(new_resources_list)


def _build_task_specs(
    executor: 'recovery_strategy.StrategyExecutor',) -> dict[str, Any]:
    """Merge base and strategy-specific task specs with collision detection."""
    base_specs: dict[str, Any] = {
        'max_restarts_on_errors': executor.max_restarts_on_errors,
        'recover_on_exit_codes': executor.recover_on_exit_codes,
    }
    strategy_specs = executor.task_specs()
    overlap = set(base_specs) & set(strategy_specs)
    if overlap:
        raise ValueError(f'Strategy task_specs() conflicts with base spec '
                         f'keys: {overlap}')
    base_specs.update(strategy_specs)
    return base_specs


class JobController:
    """Controls the lifecycle of a single managed job.

    This controller executes the chain DAG recorded for the job by:
    - Loading the DAG and preparing per-task environment variables so each task
      has a stable global job identifier across recoveries.
    - Launching the task on the configured backend (``CloudVmRayBackend``),
      optionally via a pool.
    - Persisting state transitions to the managed jobs state store
      (e.g., STARTING → RUNNING → SUCCEEDED/FAILED/CANCELLED).
    - Monitoring execution, downloading/streaming logs, detecting failures or
      preemptions, and invoking recovery through
      ``recovery_strategy.StrategyExecutor``.
    - Cleaning up clusters and ephemeral resources when tasks finish.

    Concurrency and coordination:
    - Runs inside an ``asyncio`` event loop.
    - Shares a ``starting`` set, guarded by ``starting_lock`` and signaled via
      ``starting_signal``, to throttle concurrent launches across jobs that the
      top-level ``Controller`` manages.

    Key attributes:
    - ``_job_id``: Integer identifier of this managed job.
    - ``_dag`` / ``_dag_name``: The job definition and metadata loaded from the
      database-backed job YAML.
    - ``_backend``: Backend used to launch and manage clusters.
    - ``_pool``: Optional pool name if using a pool.
    - ``starting`` / ``starting_lock`` / ``starting_signal``: Shared scheduler
      coordination primitives. ``starting_lock`` must be used for accessing
      ``starting_signal`` and ``starting``
    - ``_strategy_executor``: Recovery/launch strategy executor (created per
      task).
    """

    def __init__(
        self,
        job_id: int,
        starting: set[int],
        starting_lock: asyncio.Lock,
        starting_signal: asyncio.Condition,
        pool: str | None = None,
        rank: int | None = None,
    ) -> None:
        """Initialize a ``JobsController``.

        Args:
            job_id: Integer ID of the managed job.
            starting: Shared set of job IDs currently in the STARTING phase,
                used to limit concurrent launches.
            starting_lock: ``asyncio.Lock`` guarding access to the shared
                scheduler state (e.g., the ``starting`` set).
            starting_signal: ``asyncio.Condition`` used to notify when a job
                exits STARTING so more jobs can be admitted.
            pool: Optional pool name. When provided, the job is
                submitted to the pool rather than launching a dedicated
                cluster.
            rank: Optional rank of the job that can be used to partition
                workloads.
        """

        self.starting = starting
        self.starting_lock = starting_lock
        self.starting_signal = starting_signal

        logger.info('Initializing JobsController for job_id=%s', job_id)

        self._job_id = job_id
        self._file_mounts_blob_id: object = _FILE_MOUNTS_BLOB_ID_UNSET
        self._dag = _get_dag(job_id)
        self._dag_name = self._dag.name
        logger.info(f'Loaded DAG: {self._dag}')

        self._backend = cloud_vm_ray_backend.CloudVmRayBackend()
        self._pool = pool
        self._rank = rank
        logger.info(f'Rank for job {self._job_id}: {self._rank}')

        # pylint: disable=line-too-long
        # Add a unique identifier to the task environment variables, so that
        # the user can have the same id for multiple recoveries.
        #   Example value: sky-2022-10-04-22-46-52-467694_my-spot-name_spot_id-17-0
        job_id_env_vars = []
        for i, task in enumerate(self._dag.tasks):
            if len(self._dag.tasks) <= 1:
                task_name = self._dag_name
            else:
                assert task.name is not None, task
                task_name = task.name
                # This is guaranteed by the jobs.launch API, where we fill in
                # the task.name with
                # dag_utils.maybe_infer_and_fill_dag_and_task_names.
                assert task_name is not None, self._dag
                task_name = f'{self._dag_name}_{task_name}'

            job_id_env_var = common_utils.get_global_job_id(
                self._backend.run_timestamp,
                f'{task_name}',
                str(self._job_id),
                task_id=i,
                is_managed_job=True)
            job_id_env_vars.append(job_id_env_var)

        for i, task in enumerate(self._dag.tasks):
            task_envs = task.envs or {}
            task_envs[constants.TASK_ID_ENV_VAR] = job_id_env_vars[i]
            task_envs[constants.TASK_ID_LIST_ENV_VAR] = '\n'.join(
                job_id_env_vars)
            task_envs[constants.MANAGED_JOB_ID_ENV_VAR] = str(self._job_id)
            # Add SKYPILOT_JOB_RANK if it's set in the context or os.environ
            # (os.environ may be hijacked to use ContextualEnviron which includes context overrides)
            if self._rank is not None:
                task_envs['SKYPILOT_JOB_RANK'] = str(self._rank)
            else:
                task_envs['SKYPILOT_JOB_RANK'] = '0'
            task.update_envs(task_envs)

    async def _get_file_mounts_blob_id(self) -> str | None:
        """Return the controller-lifetime snapshot of immutable blob metadata."""
        blob_id = getattr(self, '_file_mounts_blob_id',
                          _FILE_MOUNTS_BLOB_ID_UNSET)
        if blob_id is _FILE_MOUNTS_BLOB_ID_UNSET:
            blob_id = await managed_job_state.get_file_mounts_blob_id_async(
                self._job_id)
            self._file_mounts_blob_id = blob_id
        return typing.cast(str | None, blob_id)

    @asyncio_utils.shield
    async def _release_initial_launch_slot(self) -> None:
        """Release manager-owned launch admission and wake one waiter."""
        async with self.starting_lock:
            if self._job_id not in self.starting:
                return
            self.starting.remove(self._job_id)
            self.starting_signal.notify()

    def download_log_and_stream(
        self,
        task_id: int | None,
        handle: Optional['cloud_vm_ray_backend.CloudVmRayResourceHandle'],
        job_id_on_pool_cluster: int | None,
    ) -> None:
        """Downloads and streams the logs of the current job with given task ID.

        We do not stream the logs from the cluster directly, as the
        download and stream should be faster, and more robust against
        preemptions or ssh disconnection during the streaming.
        """
        if handle is None:
            logger.info(f'Cluster for job {self._job_id} is not found. '
                        'Skipping downloading and streaming the logs.')
            return

        managed_job_logs_dir = os.path.join(constants.SKY_LOGS_DIRECTORY,
                                            'managed_jobs',
                                            f'job-id-{self._job_id}')

        def _persist_local_log_file(local_log_file: str) -> None:
            # Persist the log path for the current task so it can be accessed
            # after the job finishes. Do this as early as possible -- right
            # after the log is synced down, before the (potentially minutes-
            # long for multi-GB logs) re-stream into the controller log --
            # so the dashboard can serve the job's logs immediately instead
            # of showing "already in terminal state" until the re-stream
            # completes.
            managed_job_state.set_local_log_file(self._job_id, task_id,
                                                 local_log_file)

        log_file = None
        if managed_job_runtime.is_registered():
            log_file = managed_job_runtime.download_logs(
                handle, self._job_id, task_id)
            if log_file is not None:
                _persist_local_log_file(log_file)
        if log_file is None:
            log_file = controller_utils.download_and_stream_job_log(
                self._backend,
                handle,
                managed_job_logs_dir,
                job_ids=[str(job_id_on_pool_cluster)]
                if job_id_on_pool_cluster is not None else None,
                on_downloaded=_persist_local_log_file)
        if log_file is None:
            logger.warning(
                f'No log file was downloaded for job {self._job_id}, '
                f'task {task_id}')

        logger.info(f'\n== End of logs (ID: {self._job_id}) ==')

    async def _cleanup_cluster(self, cluster_name: str | None) -> None:
        if cluster_name is None:
            return
        if self._pool is None:
            await asyncio.to_thread(managed_job_utils.terminate_cluster,
                                    cluster_name)

    async def _get_cluster_job_exit_codes(
            self, job_id: int | None,
            handle: 'cloud_vm_ray_backend.CloudVmRayResourceHandle'
    ) -> list | None:
        """Retrieve exit codes from the remote cluster.

        Args:
            job_id: The job ID on the remote cluster.
            handle: The handle to the cluster.

        Returns:
            List of exit codes, or None if not available.
        """
        if managed_job_runtime.is_registered():
            exit_codes = managed_job_runtime.get_exit_codes(handle)
            if exit_codes is not None:
                return exit_codes
        try:
            # Try gRPC first if enabled
            if handle.is_grpc_enabled_with_flag:
                try:
                    request = jobsv1_pb2.GetJobExitCodesRequest()
                    if job_id is not None:
                        request.job_id = job_id

                    response = await asyncio.to_thread(
                        backend_utils.invoke_skylet_with_retries,
                        lambda: cloud_vm_ray_backend.SkyletClient(
                            handle.get_grpc_channel()).get_job_exit_codes(
                                request))

                    return list(
                        response.exit_codes) if response.exit_codes else None
                except exceptions.SkyletMethodNotImplementedError:
                    pass  # Fall back to legacy SSH-based method

            # Legacy SSH-based method
            code = job_lib.JobLibCodeGen.get_job_exit_codes(job_id)
            returncode, stdout, stderr = await asyncio.to_thread(
                self._backend.run_on_head,
                handle,
                code,
                stream_logs=False,
                require_outputs=True,
                separate_stderr=True)

            if returncode != 0:
                logger.debug(f'Failed to retrieve exit codes: {stderr}')
                return None

            return json.loads(stdout.strip())
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f'Failed to retrieve job exit codes: {e}')
            return None

    async def _run_one_task(self, task_id: int, task: 'sky.Task') -> bool:
        """Busy loop monitoring cluster status and handling recovery.

        When the task is successfully completed, this function returns True,
        and will terminate the cluster before returning.

        If the user program fails, i.e. the task is set to FAILED or
        FAILED_SETUP, this function will return False.
        In other cases, the function will raise exceptions.
        All the failure cases will rely on the caller to clean up the spot
        cluster(s) and storages.

        Returns:
            True if the job is successfully completed; False otherwise.

        Raises:
            exceptions.ProvisionPrechecksError: This will be raised when the
                underlying `sky.launch` fails due to precheck errors only.
                I.e., none of the failover exceptions, if
                any, is due to resources unavailability. This exception
                includes the following cases:
                1. The optimizer cannot find a feasible solution.
                2. Precheck errors: invalid cluster name, failure in getting
                cloud user identity, or unsupported feature.
            exceptions.ManagedJobReachedMaxRetriesError: This will be raised
                when all prechecks passed but the maximum number of retries is
                reached for `sky.launch`. The failure of `sky.launch` can be
                due to:
                1. Any of the underlying failover exceptions is due to resources
                unavailability.
                2. The cluster is preempted or failed before the job is
                submitted.
                3. Any unexpected error happens during the `sky.launch`.
        Other exceptions may be raised depending on the backend.
        """
        _add_k8s_annotations(task, self._job_id)
        logger.info(
            f'Starting task {task_id} ({task.name}) for job {self._job_id}')

        latest_task_id, last_task_prev_status = (
            await
            managed_job_state.get_latest_task_id_status_async(self._job_id))

        is_resume = False
        if (latest_task_id is not None and last_task_prev_status
                != managed_job_state.ManagedJobStatus.PENDING):
            assert latest_task_id >= task_id, (latest_task_id, task_id)
            if latest_task_id > task_id:
                logger.info(f'Task {task_id} ({task.name}) has already '
                            'been executed. Skipping...')
                return True
            if latest_task_id == task_id:
                # Start recovery.
                is_resume = True
                logger.info(f'Resuming task {task_id} from previous execution')

        callback_func = managed_job_utils.event_callback_func(
            job_id=self._job_id, task_id=task_id, task=task)

        if task.metadata.get('batch_coordinator'):
            return await self._run_batch_coordinator_task(task_id,
                                                          task,
                                                          callback_func,
                                                          is_resume=is_resume)

        if task.run is None:
            logger.info(f'Skip running task {task_id} ({task.name}) due to its '
                        'run commands being empty.')
            # Call set_started first to initialize columns in the state table,
            # including start_at and last_recovery_at to avoid issues for
            # uninitialized columns.
            await managed_job_state.set_started_async(
                job_id=self._job_id,
                task_id=task_id,
                start_time=time.time(),
                callback_func=callback_func)
            await managed_job_state.set_succeeded_async(
                job_id=self._job_id,
                task_id=task_id,
                end_time=time.time(),
                callback_func=callback_func)
            logger.info(f'Empty task {task_id} marked as succeeded immediately')
            return True

        usage_lib.messages.usage.update_task_id(task_id)
        task_id_env_var = task.envs[constants.TASK_ID_ENV_VAR]
        assert task.name is not None, task
        # Set the cluster name to None if the job is submitted
        # to a pool. This will be updated when we later calls the `launch`
        # or `recover` function from the strategy executor.
        cluster_name = managed_job_utils.generate_managed_job_cluster_name(
            task.name, self._job_id) if self._pool is None else None
        self._strategy_executor = recovery_strategy.StrategyExecutor.make(
            cluster_name,
            self._backend,
            task,
            self._job_id,
            task_id,
            self._pool,
            self.starting,
            self.starting_lock,
            self.starting_signal,
            file_mounts_blob_id=await self._get_file_mounts_blob_id())
        if not is_resume:
            submitted_at = time.time()
            if task_id == 0:
                submitted_at = backend_utils.get_timestamp_from_run_timestamp(
                    self._backend.run_timestamp)

            resources_str = backend_utils.get_task_resources_str(
                task, is_managed_job=True)

            # Get full_resources_json using get_resource_config which handles
            # heterogeneous resource configurations (any_of/ordered).
            full_resources_json = None
            if task.resources:
                full_resources_json = task.get_resource_config()

            await managed_job_state.set_starting_async(
                self._job_id,
                task_id,
                self._backend.run_timestamp,
                submitted_at,
                resources_str=resources_str,
                specs=_build_task_specs(self._strategy_executor),
                callback_func=callback_func,
                full_resources_json=full_resources_json)
            logger.info(f'Submitted managed job {self._job_id} '
                        f'(task: {task_id}, name: {task.name!r}); '
                        f'{constants.TASK_ID_ENV_VAR}: {task_id_env_var}')

        logger.info('Started monitoring.')

        # Only do the initial cluster launch if not resuming from a controller
        # failure. A resumed pool task may still need this launch when the
        # previous controller stopped before assigning a worker; that case is
        # handled after reading its persisted pool submission info below.
        remote_job_submitted_at = time.time()
        launched_task = False
        if not is_resume:
            launch_start = time.time()

            # Run the launch in a separate thread to avoid blocking the event
            # loop. The scheduler functions used internally already have their
            # own file locks.
            remote_job_submitted_at = await self._strategy_executor.launch()
            launched_task = True

            launch_time = time.time() - launch_start
            logger.info(f'Cluster launch completed in {launch_time:.2f}s')
            assert remote_job_submitted_at is not None, remote_job_submitted_at
        job_id_on_pool_cluster: int | None = None
        if self._pool:
            # Update the cluster name when using pool.
            cluster_name, job_id_on_pool_cluster = (
                await
                managed_job_state.get_pool_submit_info_async(self._job_id))
        if cluster_name is None:
            # Check if we have been cancelled here, in the case where a user
            # quickly cancels the job we want to gracefully handle it here,
            # otherwise we will end up in the FAILED_CONTROLLER state.
            logger.info(f'Cluster name is None for job {self._job_id}, '
                        f'task {task_id}. Checking if we have been '
                        'cancelled.')
            status = await (managed_job_state.get_job_status_with_task_id_async(
                job_id=self._job_id, task_id=task_id))
            logger.debug(f'Status for job {self._job_id}, task {task_id}:'
                         f'{status}')
            if status == managed_job_state.ManagedJobStatus.CANCELLED:
                logger.info(f'Job {self._job_id}, task {task_id} has '
                            'been quickly cancelled.')
                raise asyncio.CancelledError()
            if (is_resume and self._pool is not None and
                    status == managed_job_state.ManagedJobStatus.STARTING):
                # STARTING is persisted before pool scheduling. The controller
                # can restart while a job is legitimately waiting for its
                # first worker, leaving both pool submit fields unset. Re-enter
                # the initial launch path instead of treating that durable
                # pre-assignment state as controller corruption.
                logger.info(
                    f'Job {self._job_id}, task {task_id} was STARTING with no '
                    'persisted pool worker assignment. Re-entering pool '
                    'scheduling.')
                launch_start = time.time()
                remote_job_submitted_at = (await
                                           self._strategy_executor.launch())
                launched_task = True
                launch_time = time.time() - launch_start
                logger.info('Pool worker assignment completed after controller '
                            f'restart in {launch_time:.2f}s')
                assert remote_job_submitted_at is not None, (
                    remote_job_submitted_at)
                cluster_name, job_id_on_pool_cluster = (
                    await
                    managed_job_state.get_pool_submit_info_async(self._job_id))
        assert cluster_name is not None, (cluster_name, job_id_on_pool_cluster)

        if launched_task:
            await managed_job_state.set_started_async(
                job_id=self._job_id,
                task_id=task_id,
                start_time=remote_job_submitted_at,
                callback_func=callback_func)

        await self._release_initial_launch_slot()

        # NOTE: if we are resuming from a controller failure, we only keep
        # monitoring if the job is in RUNNING state. For all other cases,
        # we will directly transit to recovering since we have no idea what
        # the cluster status is.
        # Handle resume logic before starting the monitoring loop.
        # If resuming from a controller failure, check the previous state
        # and determine if we need to force recovery.
        force_transit_to_recovering = False
        if is_resume:
            prev_status = await (
                managed_job_state.get_job_status_with_task_id_async(
                    job_id=self._job_id, task_id=task_id))

            if prev_status is not None:
                if prev_status.is_terminal():
                    logger.info(f'Task {task_id} already in terminal state: '
                                f'{prev_status}')
                    return (prev_status ==
                            managed_job_state.ManagedJobStatus.SUCCEEDED)
                if prev_status == managed_job_state.ManagedJobStatus.CANCELLING:
                    # If the controller is down when cancelling the job,
                    # we re-raise the error to run the `_cleanup` function
                    # again to clean up any remaining resources.
                    logger.info(f'Task {task_id} was being cancelled, '
                                're-raising cancellation')
                    raise asyncio.CancelledError()
            if prev_status != managed_job_state.ManagedJobStatus.RUNNING:
                force_transit_to_recovering = True
            elif (last_task_prev_status
                  == managed_job_state.ManagedJobStatus.RUNNING and
                  not launched_task):
                # A resumed RUNNING task skips StrategyExecutor.launch(), whose
                # scheduled_launch context normally restores ALIVE. Complete
                # that generation-fenced transition before monitoring so the
                # replacement controller does not remain LAUNCHING forever.
                await scheduler.job_resumed(self._job_id)

            await self._strategy_executor.on_resume(cluster_name)

        logger.info('Started monitoring.')
        # TODO(kevin): If StrategyExecutor grew pluggable detection methods
        # (check_status, get_recovery_targets), this two-path dispatch could
        # become a single generic monitor loop on the controller. See the
        # TODO on StrategyExecutor.monitor_task().
        result = await self._strategy_executor.monitor_task(
            task_id=task_id,
            task=task,
            cluster_name=cluster_name,
            job_id_on_pool_cluster=job_id_on_pool_cluster,
            callback_func=callback_func,
            cleanup_cluster_on_success=True,
            force_transit_to_recovering=force_transit_to_recovering,
        )
        if result is not None:
            return result
        return await self._monitor_one_task(
            task_id=task_id,
            task=task,
            cluster_name=cluster_name,
            executor=self._strategy_executor,
            job_id_on_pool_cluster=job_id_on_pool_cluster,
            callback_func=callback_func,
            cleanup_cluster_on_success=True,
            force_transit_to_recovering=force_transit_to_recovering,
        )

    async def _run_batch_coordinator_task(
        self,
        task_id: int,
        task: 'sky.Task',
        callback_func: typing.Callable,
        is_resume: bool = False,
    ) -> bool:
        """Run the BatchCoordinator inline on the controller.

        The coordinator is lightweight orchestration (count items, split
        batches, dispatch via ``sdk.exec()``).  Running it here avoids
        provisioning a separate CPU cluster.

        When ``is_resume=True``, the coordinator reloads persisted batch
        state from the DB and resumes dispatch from where it left off.
        """
        if is_resume:
            # Check if the previous run already reached a terminal status.
            _, prev_status = (await
                              managed_job_state.get_latest_task_id_status_async(
                                  self._job_id))
            if (prev_status is not None and prev_status.is_terminal()):
                logger.info(f'Batch task {task_id} already in terminal status '
                            f'{prev_status.value}, skipping.')
                await self._release_initial_launch_slot()
                return prev_status == (
                    managed_job_state.ManagedJobStatus.SUCCEEDED)
            if prev_status == managed_job_state.ManagedJobStatus.CANCELLING:
                raise asyncio.CancelledError(
                    'Batch coordinator resuming into CANCELLING state')

        metadata = task.metadata

        coordinator = batch_coordinator.BatchCoordinator(
            dataset_path=metadata['batch_dataset_path'],
            output_path=metadata['batch_output_path'],
            batch_size=metadata['batch_size'],
            pool_name=metadata['batch_pool_name'],
            serialized_fn=metadata['batch_serialized_fn'],
            activate_env=metadata.get('batch_activate_env', ''),
            job_id=self._job_id,
            is_resume=is_resume,
            input_format_dict=metadata['batch_input_format'],
            output_formats_dict=metadata['batch_output_formats'],
        )

        if not is_resume:
            submitted_at = backend_utils.get_timestamp_from_run_timestamp(
                self._backend.run_timestamp) if task_id == 0 else time.time()

            await managed_job_state.set_starting_async(
                self._job_id,
                task_id,
                self._backend.run_timestamp,
                submitted_at,
                resources_str='-',
                specs={
                    'max_restarts_on_errors': 0,
                    'recover_on_exit_codes': []
                },
                callback_func=callback_func)
            await managed_job_state.set_started_async(
                job_id=self._job_id,
                task_id=task_id,
                start_time=time.time(),
                callback_func=callback_func)

        await self._release_initial_launch_slot()

        try:
            await asyncio.to_thread(coordinator.run)
            await asyncio.to_thread(coordinator.mark_succeeded, time.time())
            await callback_func('SUCCEEDED')
            try:
                await asyncio.to_thread(coordinator.cleanup)
            except batch_coordinator.SupersededCoordinator:
                raise
            except Exception as e:  # pylint: disable=broad-except
                # Output publication is complete and SUCCEEDED is durable.
                # Temp-file cleanup must never turn a successful batch into a
                # failed job; a later GC can remove leftovers.
                logger.warning('Failed to clean up batch attempt files: %s',
                               e,
                               exc_info=True)
            return True
        except batch_coordinator.SupersededCoordinator:
            logger.info('Batch coordinator was superseded; leaving job state '
                        'to the replacement coordinator.')
            await _finish_superseded_cleanup(coordinator)  # noqa: ASYNC120
            raise
        except asyncio.CancelledError:
            coordinator.cancel()
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Batch coordinator failed: {e}', exc_info=True)
            try:
                await asyncio.to_thread(coordinator.mark_failed, str(e))
            except batch_coordinator.SupersededCoordinator:
                await _finish_superseded_cleanup(coordinator)  # noqa: ASYNC120
                raise
            await callback_func('FAILED')
            return False

    async def _monitor_one_task(
        self,
        task_id: int,
        task: 'sky.Task',
        cluster_name: str,
        executor: 'recovery_strategy.StrategyExecutor',
        job_id_on_pool_cluster: int | None = None,
        callback_func: typing.Callable | None = None,
        cleanup_cluster_on_success: bool = True,
        force_transit_to_recovering: bool = False,
        on_recovery: typing.Callable[[], typing.Coroutine] | None = None,
    ) -> bool:
        """Monitor a single task until completion with recovery support.

        This is the core monitoring loop shared by both single-task execution
        and JobGroup parallel execution. It handles:
        - Periodic job status checks with transient error handling
        - Success/failure detection with exit code-based restart logic
        - External failure detection
        - Preemption detection and recovery

        Args:
            task_id: Task ID.
            task: The task to monitor.
            cluster_name: Name of the cluster running the task.
            executor: Recovery strategy executor for handling preemptions.
            job_id_on_pool_cluster: Job ID on the cluster (for pools).
            callback_func: Callback function for state updates.
            cleanup_cluster_on_success: Whether to clean up cluster on success.
            force_transit_to_recovering: If True, force recovery on first
                iteration (used when resuming from controller failure).
            on_recovery: Optional async callback called after recovery.
                Used by JobGroups to re-setup networking.

        Returns:
            True if the task succeeded, False otherwise.
        """
        if callback_func is None:
            callback_func = managed_job_utils.event_callback_func(
                job_id=self._job_id, task_id=task_id, task=task)

        transient_job_check_retry: tuple[float,
                                         common_utils.Backoff] | None = None
        not_up_debouncer = _ClusterNotUpDebouncer(task.num_nodes)
        last_known_job_status: job_lib.JobStatus | None = None
        healthy_cluster_hold_logged = False

        while True:
            # Get job status (skip on first iteration if forcing recovery)
            job_status = None
            transient_job_check_error_reason = None

            if not force_transit_to_recovering:
                await asyncio.sleep(
                    managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS)

                # Check the network connection to avoid false alarm for job
                # failure. Network glitch was observed even in the VM.
                try:
                    await backend_utils.async_check_network_connection()
                except exceptions.NetworkError:
                    logger.info(
                        'Network is not available. Retrying again in '
                        f'{managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS} '
                        'seconds.')
                    continue

                # NOTE: we do not check cluster status first because race
                # condition can occur, i.e. cluster can be down during the job
                # status check.
                # NOTE: If fetching the job status fails or we force to transit
                # to recovering, we will set the job status to None, which will
                # force enter the recovering logic.
                try:
                    job_status, transient_job_check_error_reason = (
                        await managed_job_utils.get_job_status(
                            self._backend,
                            cluster_name,
                            job_id=job_id_on_pool_cluster,
                        ))
                except exceptions.FetchClusterInfoError as fetch_e:
                    logger.info(
                        'Failed to fetch the job status. Start recovery.\n'
                        f'Exception: {common_utils.format_exception(fetch_e)}\n'
                        f'Traceback: {traceback.format_exc()}')
                    # Fall through to recovery logic below

            if job_status is not None:
                healthy_cluster_hold_logged = False
                if job_status.is_terminal():
                    last_known_job_status = None
                else:
                    last_known_job_status = job_status

            # When job status check fails, we need to retry to avoid false alarm
            # for job failure, as it could be a transient error for
            # communication issue.
            if transient_job_check_error_reason is not None:
                logger.info(
                    'Potential transient error when fetching the job '
                    f'status. Reason: {transient_job_check_error_reason}.\n'
                    'Check cluster status to determine if the job is '
                    'preempted or failed.')
            else:
                transient_job_check_retry = None

            # Handle success
            if job_status == job_lib.JobStatus.SUCCEEDED:
                logger.info(f'Task {task_id} succeeded! '
                            'Getting end time and cleaning up')
                try:
                    success_end_time = await asyncio.to_thread(
                        managed_job_utils.try_to_get_job_end_time,
                        self._backend, cluster_name, job_id_on_pool_cluster)
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        f'Failed to get job end time: '
                        f'{common_utils.format_exception(e)}',
                        exc_info=True)
                    success_end_time = 0

                # The job is done. Set the job to SUCCEEDED first before start
                # downloading and streaming the logs to make it more responsive.
                await managed_job_state.set_succeeded_async(
                    self._job_id,
                    task_id,
                    end_time=success_end_time,
                    callback_func=callback_func)
                logger.info(
                    f'Managed job {self._job_id} (task: {task_id}) SUCCEEDED. '
                    f'Cleaning up the cluster {cluster_name}.')

                try:
                    logger.info(f'Downloading logs on cluster {cluster_name} '
                                f'and job id {job_id_on_pool_cluster}.')
                    clusters = await asyncio.to_thread(
                        backend_utils.get_clusters,
                        cluster_names=[cluster_name],
                        refresh=common.StatusRefreshMode.NONE,
                        all_users=True,
                        _include_is_managed=True)
                    if clusters:
                        assert len(clusters) == 1, (clusters, cluster_name)
                        handle = clusters[0].get('handle')
                        # Best effort to download and stream the logs.
                        await asyncio.to_thread(self.download_log_and_stream,
                                                task_id, handle,
                                                job_id_on_pool_cluster)
                except Exception as e:  # pylint: disable=broad-except
                    # We don't want to crash here, so just log and continue.
                    logger.warning(
                        f'Failed to download and stream logs: '
                        f'{common_utils.format_exception(e)}',
                        exc_info=True)

                if cleanup_cluster_on_success:
                    # Only clean up the cluster, not the storages, because tasks
                    # may share storages.
                    await self._cleanup_cluster(cluster_name)
                return True

            # For single-node jobs, non-terminated job_status indicates a
            # healthy cluster. We can safely continue monitoring.
            # For multi-node jobs, since the job may not be set to FAILED
            # immediately (depending on user program) when only some of the
            # nodes are preempted or failed, need to check the actual cluster
            # status.
            if (job_status is not None and not job_status.is_terminal() and
                    task.num_nodes == 1):
                not_up_debouncer.reset()
                continue

            if job_status in job_lib.JobStatus.user_code_failure_states():
                # Add a grace period before the check of preemption to avoid
                # false alarm for job failure.
                await asyncio.sleep(5)

            # Pull the actual cluster status from the cloud provider to
            # determine whether the cluster is preempted or failed.
            # NOTE: Some failures may not be reflected in the cluster status
            # depending on the cloud, which can also cause failure of the job.
            # Plugins can report such failures via ExternalFailureSource.
            # TODO(cooperc): do we need to add this to asyncio thread?
            (cluster_status, handle) = await asyncio.to_thread(
                backend_utils.refresh_cluster_status_handle,
                cluster_name,
                force_refresh_statuses=set(status_lib.ClusterStatus))

            external_failures: list[ExternalClusterFailure] | None = None
            cluster_event_reason = None
            if cluster_status != status_lib.ClusterStatus.UP:
                # NOTE: the status-fetch retry budget is deliberately NOT
                # cleared here. A flapping cluster alternates UP/INIT, and the
                # INIT confirmation streak resets on every UP tick, so the
                # budget is the only wall-clock backstop that still forces
                # recovery. Clearing it on each not-UP tick restarts that
                # clock forever and the job never recovers and never fails.
                # It is cleared where it is actually satisfied: on a
                # successful job-status fetch, and after recovery.
                healthy_cluster_hold_logged = False
                # The cluster is (partially) preempted or failed. It can be
                # down, INIT or STOPPED, based on the interruption behavior of
                # the cloud. Spot recovery is needed (will be done later in the
                # code).
                cluster_status_str = ('' if cluster_status is None else
                                      f' (status: {cluster_status.value})')
                if _should_wait_for_cluster_not_up_confirmation(
                        cluster_status, job_status,
                        transient_job_check_error_reason, last_known_job_status,
                        not_up_debouncer):
                    # INIT may be a transient probe false positive. Confirm
                    # over consecutive ticks before tearing the cluster down,
                    # even if the same control-plane flap temporarily hid the
                    # job status as well.
                    job_status_str = (job_status.value if job_status is not None
                                      else 'job status temporarily unavailable')
                    logger.info(
                        f'Cluster is not UP{cluster_status_str} and '
                        f'{job_status_str}; waiting for confirmation before '
                        'recovery '
                        f'({not_up_debouncer.observations}/'
                        f'{not_up_debouncer.required_confirmations} consecutive '
                        'observations).')
                    continue
                logger.info(
                    f'Cluster is preempted or failed{cluster_status_str}. '
                    'Recovering...')

                # Fetch and log cluster events to provide context on why the
                # cluster entered INIT/non-UP state.
                try:
                    events = await asyncio.to_thread(
                        global_user_state.get_cluster_events,
                        cluster_name=cluster_name,
                        cluster_hash=None,
                        event_type=global_user_state.ClusterEventType.
                        STATUS_CHANGE,
                        include_timestamps=True,
                        limit=5)
                    if events:
                        event_strs = []
                        for event in events:
                            # Need cast due to dictionary semantics
                            transitioned_at = int(event['transitioned_at'])
                            timestamp = time.strftime(
                                '%Y-%m-%d %H:%M:%S',
                                time.localtime(transitioned_at))
                            event_strs.append(
                                f'  {timestamp}: {event["reason"]}')
                        events_str = '\n'.join(event_strs)
                        logger.info(f'Recent cluster events:\n{events_str}')
                        cluster_event_reason = str(events[-1]['reason'])
                except Exception as e:  # pylint: disable=broad-except
                    logger.debug('Failed to fetch cluster events: '
                                 f'{common_utils.format_exception(e)}')

                if ExternalFailureSource.is_registered():
                    cluster_failures = await asyncio.to_thread(
                        ExternalFailureSource.get, cluster_name=cluster_name)
                    if cluster_failures:
                        logger.info(
                            f'Detected cluster failures: {cluster_failures}')
                        external_failures = (
                            ExternalClusterFailure.from_failure_list(
                                cluster_failures))
            else:
                # Cluster is UP
                not_up_debouncer.reset()
                if job_status is not None and not job_status.is_terminal():
                    # The multi-node job is still running, continue monitoring.
                    continue
                elif (job_status
                      in job_lib.JobStatus.user_code_failure_states() or
                      job_status == job_lib.JobStatus.FAILED_DRIVER):
                    # The user code has probably crashed, fail immediately.
                    logger.info(
                        f'Task {task_id} failed with status: {job_status}')
                    end_time = await asyncio.to_thread(
                        managed_job_utils.try_to_get_job_end_time,
                        self._backend, cluster_name, job_id_on_pool_cluster)
                    logger.info(
                        f'The user job failed ({job_status}). Please check the '
                        'logs below.\n'
                        f'== Logs of the user job (ID: {self._job_id}) ==\n')

                    await asyncio.to_thread(self.download_log_and_stream,
                                            task_id, handle,
                                            job_id_on_pool_cluster)

                    failure_reason = (
                        'To see the details, run: '
                        f'sky jobs logs --controller {self._job_id}')

                    managed_job_status = (
                        managed_job_state.ManagedJobStatus.FAILED)
                    if job_status == job_lib.JobStatus.FAILED_SETUP:
                        managed_job_status = (
                            managed_job_state.ManagedJobStatus.FAILED_SETUP)
                    elif job_status == job_lib.JobStatus.FAILED_DRIVER:
                        # The remote user-job driver failed, not this jobs
                        # controller. Classify it as a workload failure so it
                        # does not trigger controller-failure alerting.
                        managed_job_status = (
                            managed_job_state.ManagedJobStatus.FAILED)
                        failure_reason = (
                            'The job driver on the remote cluster failed. This '
                            'can be caused by the job taking too much memory '
                            'or other resources. Try adding more memory, CPU, '
                            f'or disk in your job definition. {failure_reason}')

                    # Retrieve exit codes from the failed job
                    assert handle is not None, (
                        'Handle should not be None when cluster is UP', handle)
                    exit_codes = await self._get_cluster_job_exit_codes(
                        job_id_on_pool_cluster, handle)

                    should_restart_on_failure = (
                        executor.should_restart_on_failure(
                            exit_codes=exit_codes))
                    if should_restart_on_failure:
                        max_restarts = executor.max_restarts_on_errors
                        exit_code_msg = (
                            '(Retry the job as '
                            f'max_restarts_on_errors is set to {max_restarts}. '
                            f'[{executor.restart_cnt_on_failure}'
                            f'/{max_restarts}])')
                        if exit_codes and executor.recover_on_exit_codes:
                            recover_codes = executor.recover_on_exit_codes
                            matching_codes = [
                                c for c in exit_codes if c in recover_codes
                            ]
                            if matching_codes:
                                exit_code_msg = (
                                    f'(Exit code(s) {matching_codes} matched '
                                    'recover_on_exit_codes '
                                    f'[{recover_codes}])')
                        logger.info(
                            'User program crashed '
                            f'({managed_job_status.value}). {exit_code_msg}')
                        # Fall through to recovery
                    else:
                        logger.info(
                            f'Task {task_id} failed and will not be retried')
                        await managed_job_state.set_failed_async(
                            self._job_id,
                            task_id,
                            failure_type=managed_job_status,
                            failure_reason=failure_reason,
                            end_time=end_time,
                            callback_func=callback_func)
                        return False

                elif job_status is not None:
                    # Either the job is cancelled (should not happen) or in some
                    # unknown new state that we do not handle.
                    logger.error(f'Unknown job status: {job_status}')
                    failure_reason = (
                        f'Unknown job status {job_status}. To see the details, '
                        f'run: sky jobs logs --controller {self._job_id}')
                    await managed_job_state.set_failed_async(
                        self._job_id,
                        task_id,
                        failure_type=managed_job_state.ManagedJobStatus.
                        FAILED_CONTROLLER,
                        failure_reason=failure_reason,
                        callback_func=callback_func)
                    return False
                else:
                    # job_status is None but cluster is UP - transient error
                    # Although the cluster is healthy, we fail to access the
                    # job status. Try to recover the job (will not restart the
                    # cluster, if the cluster is healthy).
                    if transient_job_check_error_reason is not None:
                        if transient_job_check_retry is None:
                            transient_job_check_retry = (
                                time.monotonic() + managed_job_utils.
                                JOB_STATUS_FETCH_TOTAL_TIMEOUT_SECONDS,
                                common_utils.Backoff(initial_backoff=1,
                                                     max_backoff_factor=5),
                            )
                        assert transient_job_check_retry is not None, (
                            transient_job_check_error_reason)
                        (transient_job_check_deadline,
                         job_check_backoff) = transient_job_check_retry
                        remaining_timeout = (transient_job_check_deadline -
                                             time.monotonic())
                        if remaining_timeout > 0:
                            backoff_time = min(
                                job_check_backoff.current_backoff(),
                                remaining_timeout)
                            logger.info(
                                'Failed to fetch the job status while the '
                                'cluster is healthy. Retrying to avoid false'
                                'alarm for job failure. Retrying in '
                                f'{backoff_time:.1f} seconds...')
                            await asyncio.sleep(backoff_time)
                            continue
                        else:
                            if _should_keep_monitoring_healthy_cluster(
                                    last_known_job_status,
                                    transient_job_check_error_reason,
                                    task.num_nodes):
                                if not healthy_cluster_hold_logged:
                                    assert last_known_job_status is not None
                                    logger.warning(
                                        'Failed to fetch the job status after '
                                        'retrying for '
                                        f'{managed_job_utils.JOB_STATUS_FETCH_TOTAL_TIMEOUT_SECONDS:.1f} '
                                        'seconds, but the cluster is still UP '
                                        'and the last confirmed job status was '
                                        f'{last_known_job_status.value}. '
                                        'Keep monitoring instead of '
                                        'restarting the job/cluster.')
                                    healthy_cluster_hold_logged = True
                                continue
                            logger.info(
                                'Failed to fetch the job status after retrying '
                                'for '
                                f'{managed_job_utils.JOB_STATUS_FETCH_TOTAL_TIMEOUT_SECONDS:.1f} '
                                'seconds. Try to recover '
                                'the job by restarting the job/cluster.')
                    else:
                        logger.info(
                            'Failed to fetch the job status due to '
                            'unrecoverable error. Try to recover the job by'
                            ' restarting the job/cluster.')

            # When the handle is None, the cluster should be cleaned up already.
            if handle is not None:
                resources = handle.launched_resources
                assert resources is not None, handle
                # If we are forcing to transit to recovering, we need to clean
                # up the cluster as it is possible that we already submitted the
                # job to the worker cluster, but state is not updated yet. In
                # this case, it is possible that we will double-submit the job
                # to the worker cluster. So we always clean up the cluster here.
                # TODO(tian,cooperc): We can check if there is a running job on
                # the worker cluster, and if so, we can skip the cleanup.
                # Challenge: race condition when the worker cluster thought it
                # does not have a running job yet but later the job is launched.
                if (resources.need_cleanup_after_preemption_or_failure() or
                        force_transit_to_recovering):
                    # Some spot resource (e.g., Spot TPU VM) may need to be
                    # cleaned up after preemption, as running launch again on
                    # those clusters again may fail.
                    logger.info('Cleaning up the preempted or failed cluster'
                                '...')
                    await self._cleanup_cluster(cluster_name)

            # Try to recover the managed jobs, when the cluster is preempted or
            # failed or the job status is failed to be fetched.
            logger.info(f'Starting recovery for task {task_id}, '
                        f'it is currently {job_status}')
            await managed_job_state.set_recovering_async(
                job_id=self._job_id,
                task_id=task_id,
                force_transit_to_recovering=force_transit_to_recovering,
                callback_func=callback_func,
                external_failures=external_failures,
                cluster_event_reason=cluster_event_reason,
            )

            recovered_time = await executor.recover()

            # Update cluster_name for pools after recovery
            if self._pool is not None:
                pool_cluster_name, job_id_on_pool_cluster = (
                    await
                    managed_job_state.get_pool_submit_info_async(self._job_id))
                assert pool_cluster_name is not None
                cluster_name = pool_cluster_name

            await managed_job_state.set_recovered_async(
                self._job_id,
                task_id,
                recovered_time=recovered_time,
                callback_func=callback_func)

            # Call recovery callback if provided
            if on_recovery is not None:
                await on_recovery()

            logger.info(f'Task {task.name} recovered, continuing monitoring')

            # Recovery starts a fresh monitoring epoch. Retry state from the
            # old cluster must not shorten the next transient-error budget.
            transient_job_check_retry = None
            last_known_job_status = None
            healthy_cluster_hold_logged = False
            force_transit_to_recovering = False
            # Observations accumulated against the old cluster must not count
            # toward recovering the fresh one.
            not_up_debouncer.reset()

    async def _prepare_job_group_task_for_launch(
        self, task: 'sky.Task', task_id: int, job_group_name: str,
        other_job_names: list[str]
    ) -> tuple[str, recovery_strategy.StrategyExecutor]:
        """Prepare a JobGroup task for launch.

        This function:
        1. Injects a wait script to ensure networking is ready
        2. Creates the recovery strategy executor
        3. Sets task state to STARTING

        Args:
            task: Task to prepare.
            task_id: Task ID.
            job_group_name: JobGroup name.
            other_job_names: Other task names in the group (to wait for).

        Returns:
            Tuple of (cluster_name, executor). cluster_name is always
            deterministic for JobGroups (no pool support).
        """
        task_name = task.name
        assert task_name is not None, f'Task {task_id} must have a name'

        # Inject wait script to ensure networking is ready before task runs.
        # We inject this into task.run (not task.setup) because:
        # - setup runs during cluster provisioning (Phase 1)
        # - DNS mappings file is written in Phase 3 (after clusters are UP)
        # - If we block in setup, it times out before Phase 3 can run
        wait_script = job_group_networking.generate_wait_for_networking_script(
            job_group_name, other_job_names)
        # When non-empty, this prelude is prepended to the task's run
        # section to start the JobGroup DNS updater from there. Phase 3
        # below does the same delivery via SSH for tasks not covered here.
        inline_networking_setup_script = (
            job_group_networking.generate_inline_networking_setup_script(
                job_group_name, self._dag.tasks, self._job_id))
        run_prefixes = [
            script for script in (inline_networking_setup_script, wait_script)
            if script
        ]
        if run_prefixes:
            current_run = task.run or ''
            task.run = '\n\n'.join(run_prefixes + [current_run])

        # JobGroups don't support pools, so cluster name is always deterministic
        cluster_name = managed_job_utils.generate_managed_job_cluster_name(
            task_name, self._job_id)

        executor = recovery_strategy.StrategyExecutor.make(
            cluster_name,
            self._backend,
            task,
            self._job_id,
            task_id,
            None,
            self.starting,
            self.starting_lock,
            self.starting_signal,
            file_mounts_blob_id=await self._get_file_mounts_blob_id())

        callback_func = managed_job_utils.event_callback_func(
            job_id=self._job_id, task_id=task_id, task=task)
        resources_str = backend_utils.get_task_resources_str(
            task, is_managed_job=True)
        await managed_job_state.set_starting_async(
            self._job_id,
            task_id,
            self._backend.run_timestamp,
            time.time(),
            resources_str=resources_str,
            specs=_build_task_specs(executor),
            callback_func=callback_func)

        return cluster_name, executor

    async def _monitor_job_group_task(
        self,
        task_id: int,
        task: 'sky.Task',
        cluster_name: str,
        executor: recovery_strategy.StrategyExecutor,
        job_group_name: str,
        all_tasks_handles: list[tuple['sky.Task', typing.Any]],
        force_transit_to_recovering: bool = False,
    ) -> bool:
        """Monitor a single task in a JobGroup until completion.

        Wraps _monitor_one_task with JobGroup-specific recovery callback
        for re-setting up networking after recovery.

        Args:
            task_id: Task ID.
            task: The task to monitor.
            cluster_name: Name of the cluster running the task.
            executor: Recovery strategy executor.
            job_group_name: Name of the JobGroup.
            all_tasks_handles: List of (task, handle) tuples for all tasks.
            force_transit_to_recovering: If True, force recovery on first
                iteration (used when resuming from controller failure).

        Returns:
            True if task succeeded, False otherwise.
        """

        async def on_recovery() -> None:
            """Re-setup networking after recovery (new node may have new IP).

            Unlike Phase 3, we do NOT skip tasks that inline the DNS
            mapping — a recovered peer may have a new IP, so every
            task's /etc/hosts needs refreshing.
            """
            task_clusters = []
            for t, _ in all_tasks_handles:
                t_name = t.name
                assert t_name is not None
                # JobGroups don't support pools, cluster name is deterministic
                t_cluster = managed_job_utils.generate_managed_job_cluster_name(
                    t_name, self._job_id)
                task_clusters.append((t, t_cluster))
            handles = await asyncio.to_thread(
                global_user_state.get_handles_from_cluster_names,
                {cluster_name for _, cluster_name in task_clusters})
            updated_handles = [(t, handles.get(cluster_name))
                               for t, cluster_name in task_clusters]

            await job_group_networking.setup_job_group_networking(
                job_group_name, updated_handles)

        # Mirror the dispatch in `_run_one_task`: give the recovery
        # strategy first refusal at owning the per-task monitor loop so
        # both code paths behave consistently. Strategies that return
        # None fall through to `_monitor_one_task` below unchanged.
        result = await executor.monitor_task(
            task_id=task_id,
            task=task,
            cluster_name=cluster_name,
            job_id_on_pool_cluster=None,
            cleanup_cluster_on_success=False,  # JobGroup cleans up all at end
            force_transit_to_recovering=force_transit_to_recovering,
            on_recovery=on_recovery,
        )
        if result is not None:
            return result
        return await self._monitor_one_task(
            task_id=task_id,
            task=task,
            cluster_name=cluster_name,
            executor=executor,
            job_id_on_pool_cluster=None,
            cleanup_cluster_on_success=False,  # JobGroup cleans up all at end
            force_transit_to_recovering=force_transit_to_recovering,
            on_recovery=on_recovery,
        )

    async def _run_job_group(self) -> bool:
        """Run a JobGroup with parallel execution.

        Phases:
        1. Launch clusters (all on same infrastructure)
        2. Barrier sync - wait for all clusters to be ready
        3. Set up networking (/etc/hosts injection)
        4. Monitor all jobs in parallel with recovery support

        Returns:
            True if all jobs succeeded, False otherwise.
        """
        job_group_name = self._dag.name
        assert job_group_name is not None, 'JobGroup name must be set'
        assert self._pool is None, 'JobGroups do not support pools'
        tasks = self._dag.tasks
        logger.info(f'Starting JobGroup "{job_group_name}" with '
                    f'{len(tasks)} jobs: {[t.name for t in tasks]}')

        # Inject JobGroup environment variables into all tasks
        runtime_envs: dict[str, str] = {}
        if managed_job_runtime.is_registered():
            extra_envs = await asyncio.to_thread(
                managed_job_runtime.job_group_envs, tasks, self._job_id)
            if extra_envs:
                runtime_envs = extra_envs
        for task in tasks:
            task_envs = task.envs or {}
            task_envs[jobs_constants.SKYPILOT_JOBGROUP_NAME_ENV_VAR] = (
                job_group_name)
            task_envs.update(runtime_envs)
            task.update_envs(task_envs)

        # Collect task statuses and determine which tasks need launch vs resume.
        # For JobGroups, all tasks run in parallel. Each task's action is
        # determined by its own status:
        #   - None/PENDING: fresh launch
        #   - Terminal: skip (already done)
        #   - RUNNING: resume monitoring without forced recovery
        #   - Other non-terminal: resume with forced recovery
        # Key: task_id, Value: (task_status, force_transit_to_recovering)
        task_resume_info: dict[int,
                               tuple[managed_job_state.ManagedJobStatus | None,
                                     bool]] = {}

        task_statuses = dict(await
                             managed_job_state.get_all_task_ids_statuses_async(
                                 self._job_id))
        for task_id, task in enumerate(tasks):
            task_status = task_statuses.get(task_id)

            if task_status is None or task_status == (
                    managed_job_state.ManagedJobStatus.PENDING):
                # Fresh launch
                task_resume_info[task_id] = (None, False)
            elif task_status.is_terminal():
                # Task already completed - no need to monitor
                task_resume_info[task_id] = (task_status, False)
                logger.info(f'Task {task_id} ({task.name}) already in '
                            f'terminal state: {task_status}')
            elif task_status == managed_job_state.ManagedJobStatus.CANCELLING:
                # Job was being cancelled when controller went down
                logger.info('JobGroup was being cancelled, '
                            're-raising cancellation')
                raise asyncio.CancelledError()
            elif task_status == managed_job_state.ManagedJobStatus.RUNNING:
                # Task was running - resume monitoring without forced recovery
                task_resume_info[task_id] = (task_status, False)
                logger.info(f'Task {task_id} ({task.name}) was RUNNING, '
                            'resuming monitoring')
            else:
                # Task was in non-RUNNING, non-terminal state - force recovery
                task_resume_info[task_id] = (task_status, True)
                logger.info(f'Task {task_id} ({task.name}) was in '
                            f'{task_status}, will force recovery')

        def is_terminal(task_id: int) -> bool:
            """Check if task is in terminal state."""
            status, _ = task_resume_info[task_id]
            return status is not None and status.is_terminal()

        def needs_launch(task_id: int) -> bool:
            """Check if task needs fresh launch (None or PENDING)."""
            status, _ = task_resume_info[task_id]
            return (status is None or
                    status == managed_job_state.ManagedJobStatus.PENDING)

        # Check if all tasks are already in terminal state
        if all(is_terminal(tid) for tid in range(len(tasks))):
            logger.info('All tasks already in terminal state')
            all_succeeded = all(task_resume_info[tid][0] ==
                                managed_job_state.ManagedJobStatus.SUCCEEDED
                                for tid in range(len(tasks)))
            await self._release_initial_launch_slot()
            return all_succeeded

        # Phase 1: Launch clusters for tasks that need launching
        launch_start = time.time()
        cluster_names: list[str | None] = []
        strategy_executors: list[recovery_strategy.StrategyExecutor | None] = []
        tasks_to_launch = [
            tid for tid in range(len(tasks)) if needs_launch(tid)
        ]
        launch_failure: Exception | None = None

        try:
            # Prepare all tasks (create executors and set STARTING state)
            for task_id, task in enumerate(tasks):
                if is_terminal(task_id):
                    cluster_names.append(None)
                    strategy_executors.append(None)
                    continue

                # Get list of other job names (excluding current task)
                other_job_names = [t.name for t in tasks if t.name != task.name]
                name, prepared_executor = (
                    await self._prepare_job_group_task_for_launch(
                        task, task_id, job_group_name, other_job_names))
                cluster_names.append(name)
                strategy_executors.append(prepared_executor)

            # Only launch tasks that need launching
            if tasks_to_launch:
                logger.info(f'Phase 1: Launching clusters for tasks '
                            f'{tasks_to_launch}...')
                # Each launch gets its own SkyPilotContext copy so that
                # the env-var pop/restore in _launch() doesn't race
                # across concurrent tasks sharing the same context.
                launch_coros = []
                for task_id in tasks_to_launch:
                    launch_executor = strategy_executors[task_id]
                    if launch_executor is not None:
                        launch_coros.append(
                            context.contextual_async(launch_executor.launch)())

                if launch_coros:
                    results = await asyncio.gather(*launch_coros,
                                                   return_exceptions=True)
                    for result in results:
                        if isinstance(result, BaseException):
                            raise result
                    logger.info(f'Clusters launched in '
                                f'{time.time()-launch_start:.2f}s')
            else:
                logger.info('Phase 1: Skipping launch - resuming from '
                            'previous execution')

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f'Failed to launch clusters: {e}')
            launch_failure = e

        if launch_failure is not None:
            try:
                await self._finish_failure_cleanup(
                    self._cleanup_job_group_clusters(cluster_names))
            except asyncio.CancelledError:  # noqa: ASYNC103
                pass
            raise launch_failure.with_traceback(launch_failure.__traceback__)

        # Phase 2: Barrier sync - collect handles and set RUNNING state
        logger.info('Phase 2: Waiting for all clusters to be ready...')

        active_tasks: list[tuple[int, sky.Task, str, bool]] = []
        for task_id, task in enumerate(tasks):
            if is_terminal(task_id):
                continue
            # Task is resuming if it has a status (i.e., was already started)
            task_is_resuming = task_resume_info[task_id][0] is not None
            # JobGroups always have deterministic cluster names
            task_cluster_name = cluster_names[task_id]
            assert task_cluster_name is not None, (
                f'cluster_name should be set for non-terminal task {task_id}')
            active_tasks.append(
                (task_id, task, task_cluster_name, task_is_resuming))

        handle_snapshot = await asyncio.to_thread(
            global_user_state.get_handles_from_cluster_names,
            {cluster_name for _, _, cluster_name, _ in active_tasks})
        handles: list[cloud_vm_ray_backend.CloudVmRayResourceHandle |
                      None] = [None] * len(tasks)
        start_coros = []
        for task_id, task, active_cluster_name, is_resuming in active_tasks:
            handles[task_id] = typing.cast(
                cloud_vm_ray_backend.CloudVmRayResourceHandle | None,
                handle_snapshot.get(active_cluster_name))
            if not is_resuming:
                callback_func = managed_job_utils.event_callback_func(
                    job_id=self._job_id, task_id=task_id, task=task)
                start_coros.append(
                    managed_job_state.set_started_async(
                        job_id=self._job_id,
                        task_id=task_id,
                        start_time=time.time(),
                        callback_func=callback_func))
        if start_coros:
            await asyncio.gather(*start_coros)

        await self._release_initial_launch_slot()

        # Phase 3: Set up networking
        logger.info('Phase 3: Setting up JobGroup networking...')
        # Build list of (task, handle) for non-terminal tasks with valid
        # handles. Skip tasks that inline the DNS mapping — they already
        # start the DNS updater from task.run.
        tasks_handles: list[tuple[
            sky.Task, cloud_vm_ray_backend.CloudVmRayResourceHandle]] = []
        for tid, task in enumerate(tasks):
            task_handle = handles[tid]
            if task_handle is None:
                continue
            if (job_group_networking.dns_addresses_for_task(task, self._job_id)
                    is not None):
                continue
            tasks_handles.append((task, task_handle))

        if tasks_handles:
            networking_success = await (
                job_group_networking.setup_job_group_networking(
                    job_group_name, tasks_handles))
            if not networking_success:
                logger.warning(
                    'Some networking setup failed, continuing anyway')

        logger.info('JobGroup setup complete, all jobs are running')

        # Phase 4: Monitor all jobs in parallel with primary/auxiliary support
        logger.info('Phase 4: Monitoring all jobs...')

        # Determine primary vs auxiliary jobs
        primary_job_names = self._dag.primary_tasks
        if not primary_job_names:
            # All jobs are primary (traditional behavior)
            primary_task_ids: set[int] = set(range(len(tasks)))
            auxiliary_task_ids: set[int] = set()
        else:
            primary_task_ids = {
                tid for tid, t in enumerate(tasks)
                if t.name in primary_job_names
            }
            auxiliary_task_ids = set(range(len(tasks))) - primary_task_ids

        if auxiliary_task_ids:
            logger.info(
                f'Primary jobs: {[tasks[tid].name for tid in primary_task_ids]}'
            )
            logger.info(f'Auxiliary jobs: '
                        f'{[tasks[tid].name for tid in auxiliary_task_ids]}')

        # Create asyncio.Task objects for all non-terminal tasks
        # Maps task_id -> asyncio.Task
        monitor_async_tasks: dict[int, asyncio.Task] = {}
        for task_id, task in enumerate(tasks):
            if is_terminal(task_id):
                continue

            _, force_recovery = task_resume_info[task_id]
            task_handle = handles[task_id]
            monitor_executor = strategy_executors[task_id]
            cluster_name = cluster_names[task_id]
            assert cluster_name is not None
            assert monitor_executor is not None
            coro = self._monitor_job_group_task(task_id, task, cluster_name,
                                                monitor_executor,
                                                job_group_name, tasks_handles,
                                                force_recovery)
            monitor_async_tasks[task_id] = asyncio.create_task(
                coro, name=f'monitor_{task.name}')

        # Track results: task_id -> success (True/False/Exception)
        task_results: dict[int, bool | Exception] = {}
        # Track remaining primary task IDs (non-terminal ones)
        remaining_primary = primary_task_ids - {
            tid for tid in range(len(tasks)) if is_terminal(tid)
        }
        # Reverse mapping: asyncio.Task -> task_id for efficient lookup
        async_task_to_id: dict[asyncio.Task, int] = {
            at: tid for tid, at in monitor_async_tasks.items()
        }
        monitor_failure: Exception | None = None

        async def cancel_remaining_monitors() -> None:
            """Cancel and join monitors without interrupting their cleanup."""
            remaining_tasks = list(monitor_async_tasks.values())
            for monitor_task in remaining_tasks:
                monitor_task.cancel()
            if remaining_tasks:
                join_future = asyncio.gather(*remaining_tasks,
                                             return_exceptions=True)
                while True:
                    try:
                        await asyncio.shield(join_future)
                        break
                    except asyncio.CancelledError:  # noqa: ASYNC103
                        # The owning scope already records and re-raises its
                        # first cancellation. Delay later cancellations until
                        # every child has finished its own cleanup.
                        continue  # noqa: ASYNC104

        try:
            # Monitor with primary/auxiliary termination logic
            while monitor_async_tasks:
                # Wait for any task to complete
                done, _ = await asyncio.wait(
                    monitor_async_tasks.values(),
                    return_when=asyncio.FIRST_COMPLETED)

                for completed_task in done:
                    completed_task_id = async_task_to_id[completed_task]

                    # Remove from monitoring
                    del monitor_async_tasks[completed_task_id]
                    del async_task_to_id[completed_task]

                    # Get result
                    try:
                        task_result: bool | Exception = (
                            completed_task.result())
                        task_results[completed_task_id] = task_result
                        if task_result:
                            logger.info(
                                f'Job {tasks[completed_task_id].name} succeeded'
                            )
                        else:
                            logger.info(
                                f'Job {tasks[completed_task_id].name} failed')
                    except asyncio.CancelledError:
                        # Task was cancelled (auxiliary job termination)
                        task_results[completed_task_id] = False
                        task_name = tasks[completed_task_id].name
                        logger.info(f'Job {task_name} was terminated')
                    except Exception as e:  # pylint: disable=broad-except
                        # TODO: avoid broad except
                        task_results[completed_task_id] = e
                        logger.error(
                            f'Job {tasks[completed_task_id].name} failed with '
                            f'exception: {e}')

                    # If this was a primary task, check if all primary done
                    if completed_task_id in remaining_primary:
                        remaining_primary.discard(completed_task_id)

                        if not remaining_primary:
                            # All primary jobs are done
                            logger.info('All primary jobs completed')

                            # Check if all primary jobs succeeded. For terminal
                            # tasks, check their status; for others, check
                            # result.
                            def primary_task_succeeded(tid: int) -> bool:
                                if is_terminal(tid):
                                    return (task_resume_info[tid][0] ==
                                            managed_job_state.ManagedJobStatus.
                                            SUCCEEDED)
                                return task_results.get(tid, True) is True

                            all_primary_succeeded = all(
                                primary_task_succeeded(tid)
                                for tid in primary_task_ids)

                            # Terminate remaining auxiliary jobs
                            if monitor_async_tasks:
                                await self._terminate_auxiliary_jobs(
                                    tasks, monitor_async_tasks, cluster_names,
                                    all_primary_succeeded)
                                # All auxiliary jobs terminated, exit loop
                                break

        except asyncio.CancelledError:
            # Monitor tasks are independent asyncio tasks, so cancelling this
            # parent does not cancel them automatically. Join them before the
            # controller manager starts tearing down their clusters; otherwise
            # a child can keep polling or recover while cleanup is in progress.
            await cancel_remaining_monitors()
            raise
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f'Monitoring failed: {e}')
            monitor_failure = e

        if monitor_failure is not None:
            try:
                await self._finish_failure_cleanup(
                    cancel_remaining_monitors(),
                    self._cleanup_job_group_clusters(cluster_names))
            except asyncio.CancelledError:  # noqa: ASYNC103
                pass
            raise monitor_failure.with_traceback(monitor_failure.__traceback__)

        # Check results (include terminal tasks)
        all_succeeded = True
        for task_id, task in enumerate(tasks):
            if is_terminal(task_id):
                # Terminal task - check if it succeeded
                task_status = task_resume_info[task_id][0]
                if task_status != managed_job_state.ManagedJobStatus.SUCCEEDED:
                    all_succeeded = False
                continue

            # Check the result for this task
            check_result = task_results.get(task_id)
            if isinstance(check_result, Exception):
                logger.error(
                    f'Job {task.name} monitoring failed: {check_result}')
                all_succeeded = False
            elif check_result is not True:
                all_succeeded = False

        await self._cleanup_job_group_clusters(cluster_names)
        return all_succeeded

    async def _terminate_auxiliary_jobs(self, tasks: list['task_lib.Task'],
                                        monitor_async_tasks: dict[int,
                                                                  asyncio.Task],
                                        cluster_names: list[str | None],
                                        all_primary_succeeded: bool) -> None:
        """Terminate auxiliary jobs after all primary jobs complete.

        Args:
            tasks: List of all tasks in the job group.
            monitor_async_tasks: Dict mapping task_id to asyncio.Task for
                remaining (auxiliary) jobs.
            cluster_names: List of cluster names for each task.
            all_primary_succeeded: Whether all primary jobs succeeded. If True,
                use configured termination delays. If False, terminate
                immediately.
        """
        if not monitor_async_tasks:
            return

        async def terminate_one(task_id: int, async_task: asyncio.Task,
                                delay_secs: int) -> None:
            """Terminate a single auxiliary job after optional delay."""
            task_name = tasks[task_id].name
            if delay_secs > 0:
                logger.info(f'Waiting {delay_secs}s before terminating '
                            f'auxiliary job {task_name}...')
                await asyncio.sleep(delay_secs)

            logger.info(f'Terminating auxiliary job {task_name}')

            # Cancel the monitoring task
            async_task.cancel()
            try:
                await async_task
            except asyncio.CancelledError:
                pass

            # Set the task status to cancelled
            callback_func = managed_job_utils.event_callback_func(
                job_id=self._job_id, task_id=task_id, task=tasks[task_id])
            await managed_job_state.set_cancelling_async(
                job_id=self._job_id, callback_func=callback_func)

            # Clean up the cluster
            cluster_name = cluster_names[task_id]
            if cluster_name is not None:
                try:
                    await self._cleanup_cluster(cluster_name)
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        f'Failed to cleanup cluster for {task_name}: {e}')

            await managed_job_state.set_cancelled_async(
                job_id=self._job_id, callback_func=callback_func)

        # Build termination coroutines with appropriate delays
        termination_coros = []
        for task_id, async_task in list(monitor_async_tasks.items()):
            if all_primary_succeeded:
                delay_secs = self._dag.get_termination_delay_secs(
                    tasks[task_id].name)
            else:
                # Primary job failed - terminate immediately
                delay_secs = 0
            termination_coros.append(
                terminate_one(task_id, async_task, delay_secs))

        # Run all terminations in parallel
        termination_results = await asyncio.gather(*termination_coros,
                                                   return_exceptions=True)
        for result in termination_results:
            if isinstance(result, BaseException):
                # Wait for every independent auxiliary cleanup before
                # surfacing a failed state transition. Silently returning here
                # can leave the affected task nonterminal after its monitor and
                # cluster have already been stopped.
                raise result

    async def _cleanup_job_group_clusters(
            self, cluster_names: list[str | None]) -> None:
        """Clean up all clusters in a JobGroup."""

        async def cleanup_cluster(cluster_name: str) -> None:
            try:
                await self._cleanup_cluster(cluster_name)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(f'Failed to cleanup {cluster_name}: {e}')

        await asyncio.gather(*(cleanup_cluster(cluster_name)
                               for cluster_name in cluster_names
                               if cluster_name is not None))

    async def _finish_failure_cleanup(
            self, *cleanup_coros: typing.Coroutine[typing.Any, typing.Any,
                                                   None]) -> None:
        """Finish failure cleanup before surfacing the original error."""
        cancelled = False
        for cleanup_coro in cleanup_coros:
            cleanup_task: asyncio.Task[None] = asyncio.create_task(cleanup_coro)
            try:
                await asyncio.shield(cleanup_task)
                continue
            except asyncio.CancelledError:  # noqa: ASYNC103
                cancelled = True
            while not cleanup_task.done():
                try:
                    await cleanup_task
                except asyncio.CancelledError:  # noqa: ASYNC103
                    cancelled = True
        if cancelled:
            raise asyncio.CancelledError()

    async def run(self):
        """Run controller logic and handle exceptions."""
        logger.info(f'Starting JobsController run for job {self._job_id}')
        task_id = 0
        cancelled = False
        superseded = False

        try:
            succeeded = True

            # Check if this is a JobGroup (parallel execution)
            if self._dag.is_job_group():
                logger.info(f'Running as JobGroup with {len(self._dag.tasks)} '
                            f'parallel jobs')
                succeeded = await self._run_job_group()
            else:
                # Traditional chain DAG: serial execution
                for task_id, task in enumerate(self._dag.tasks):
                    logger.info(
                        f'Processing task {task_id}/{len(self._dag.tasks)-1}: '
                        f'{task.name}')
                    task_start = time.time()
                    succeeded = await self._run_one_task(task_id, task)
                    task_time = time.time() - task_start
                    logger.info(f'Task {task_id} completed in {task_time:.2f}s '
                                f'with success={succeeded}')

                    if not succeeded:
                        logger.info(
                            f'Task {task_id} failed, stopping execution')
                        break

        except exceptions.ProvisionPrechecksError as e:
            # Please refer to the docstring of self._run for the cases when
            # this exception can occur.
            logger.error(f'Provision prechecks failed for task {task_id}')
            failure_reason = ('; '.join(
                common_utils.format_exception(reason, use_bracket=True)
                for reason in e.reasons))
            logger.error(failure_reason)
            await self._update_failed_task_state(
                task_id, managed_job_state.ManagedJobStatus.FAILED_PRECHECKS,
                failure_reason)
        except exceptions.ManagedJobReachedMaxRetriesError as e:
            # Please refer to the docstring of self._run for the cases when
            # this exception can occur.
            logger.error(f'Managed job reached max retries for task {task_id}')
            failure_reason = common_utils.format_exception(e)
            logger.error(failure_reason)
            # The managed job should be marked as FAILED_NO_RESOURCE, as the
            # managed job may be able to launch next time.
            await self._update_failed_task_state(
                task_id, managed_job_state.ManagedJobStatus.FAILED_NO_RESOURCE,
                failure_reason)
        except exceptions.ClusterSetUpError as e:
            # Raised by the launch path for a non-retryable setup failure, e.g.
            # the job's pod was OOMKilled during cluster/runtime setup. The
            # failure is deterministic, so we mark the job terminal (rather than
            # retrying forever) and surface the reason to the CLI/dashboard.
            logger.error(f'Cluster setup failed for task {task_id}')
            failure_reason = common_utils.format_exception(e, use_bracket=True)
            logger.error(failure_reason)
            await self._update_failed_task_state(
                task_id, managed_job_state.ManagedJobStatus.FAILED_SETUP,
                failure_reason)
        except asyncio.CancelledError:  # pylint: disable=try-except-raise
            # have this here to avoid getting caught by the general except block
            # below.
            cancelled = True
            raise
        except batch_coordinator.SupersededCoordinator:
            superseded = True
            logger.info(
                'JobsController for Batch job %s was superseded; '
                'skipping terminal state transitions.', self._job_id)
            raise
        except (Exception, SystemExit) as e:  # pylint: disable=broad-except
            logger.error(
                f'Unexpected error in JobsController run for task {task_id}')
            with ux_utils.enable_traceback():
                logger.error(traceback.format_exc())
            msg = ('Unexpected error occurred: ' +
                   common_utils.format_exception(e, use_bracket=True))
            logger.error(msg)
            await self._update_failed_task_state(
                task_id, managed_job_state.ManagedJobStatus.FAILED_CONTROLLER,
                msg)
        finally:
            if not superseded:
                callback_func = managed_job_utils.event_callback_func(
                    job_id=self._job_id,
                    task_id=task_id,
                    task=self._dag.tasks[task_id])
                await managed_job_state.set_cancelling_async(
                    job_id=self._job_id, callback_func=callback_func)
                if not cancelled:
                    # the others haven't been run yet so we can set them to
                    # cancelled immediately (no resources to clean up).
                    # if we are running and get cancelled, we need to clean up
                    # the resources first so this will be done later.
                    await managed_job_state.set_cancelled_async(
                        job_id=self._job_id, callback_func=callback_func)

    async def _update_failed_task_state(
            self, task_id: int,
            failure_type: managed_job_state.ManagedJobStatus,
            failure_reason: str):
        """Update the state of the failed task."""
        logger.info(f'Updating failed task state: task_id={task_id}, '
                    f'failure_type={failure_type}')
        await managed_job_state.set_failed_async(
            self._job_id,
            task_id=task_id,
            failure_type=failure_type,
            failure_reason=failure_reason,
            callback_func=managed_job_utils.event_callback_func(
                job_id=self._job_id,
                task_id=task_id,
                task=self._dag.tasks[task_id]))


def _prepare_job_log_path(job_id: int) -> str:
    """Create the controller log directory and return this job's log path."""
    log_dir = os.path.expanduser(jobs_constants.JOBS_CONTROLLER_LOGS_DIR)
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f'{job_id}.log')


class ControllerManager:
    """Main loop for a job controller process.

    Many jobs will be handled by this, each by a single JobController.
    """

    def __init__(self,
                 controller_uuid: str,
                 controller_slot_id: int | None = None,
                 controller_slot_attempt: str | None = None) -> None:
        self._controller_uuid = controller_uuid
        self._controller_slot_id = controller_slot_id
        self._controller_slot_attempt = controller_slot_attempt
        # Global state for active jobs
        self.job_tasks: dict[int, asyncio.Task] = {}
        self._cleanup_only_job_ids: set[int] = set()
        self.starting: set[int] = set()

        # Lock for synchronizing access to global state dictionary
        # Must always hold _job_tasks_lock when accessing the _starting_signal.
        self._job_tasks_lock = asyncio.Lock()
        # We signal whenever a job leaves the api server launching state. Feel
        # free to signal as much as you want to be safe from leaks (if you
        # do not signal enough there may be some jobs forever waiting to
        # launch).
        self._starting_signal = asyncio.Condition(lock=self._job_tasks_lock)

        # Store graceful cancel info per job, keyed by job_id.
        # Populated by cancel_job() and consumed by run_job().
        self._cancel_info: dict[int, tuple[bool, int | None]] = {}
        self._cancel_info_lock = asyncio.Lock()
        self._controller_api_token_ids: dict[int, str] = {}

        self._pid = os.getpid()
        self._pid_started_at = psutil.Process(self._pid).create_time()

    def _require_controller_slot_id(self) -> int:
        if self._controller_slot_id is None:
            raise RuntimeError('ControllerManager has no runtime slot ID.')
        return self._controller_slot_id

    def _require_controller_slot_attempt(self) -> str:
        if self._controller_slot_attempt is None:
            raise RuntimeError('ControllerManager has no runtime slot attempt.')
        return self._controller_slot_attempt

    @staticmethod
    def _cleanup_api_server_access_token(job_id: int) -> None:
        """Revoke a managed-job token after its whole batch is terminal."""
        token_id = managed_job_state.get_releasable_api_access_token_id(job_id)
        if token_id is None:
            return
        if global_user_state.delete_service_account_token(token_id):
            logger.info(f'Revoked API server access token for job {job_id}')
        else:
            logger.debug(
                'API server access token for job %s was already '
                'revoked by a sibling finalizer.', job_id)

    def _initialize_controller_api_access(self, job_id: int) -> str | None:
        """Authenticate nested SDK requests from a guarded controller.

        The process-local controller capability authenticates the controller
        origin but deliberately does not replace ordinary user
        authentication. Issue a distinct, coroutine-local token as the job's
        original user so launch, status, and cleanup requests satisfy both
        checks.
        """
        if controller_capability.get_process_local() is None:
            return None

        tasks = managed_job_state.get_managed_job_tasks(job_id)
        if not tasks or not tasks[0].get('user_hash'):
            raise RuntimeError(
                f'Cannot determine the original user for managed job {job_id}.')
        user_hash = tasks[0]['user_hash']
        token, token_id = managed_job_api_access.create_job_api_token(
            user_hash,
            f'controller-{job_id}-{self._require_controller_slot_attempt()[:8]}',
        )
        ctx = context.get()
        assert ctx is not None, 'Context is not initialized'
        try:
            ctx.override_envs({constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: token})
            # A health probe may have cached NEEDS_AUTH before the per-job
            # credential was installed. Force the first authenticated probe
            # to observe the new credential immediately.
            server_common.get_api_server_status_response.cache_clear()
        except Exception:
            global_user_state.delete_service_account_token(token_id)
            raise
        self._controller_api_token_ids[job_id] = token_id
        return token_id

    @staticmethod
    def _cleanup_controller_api_access(token_id: str | None,
                                       job_id: int) -> None:
        """Revoke a controller-only token without touching workload tokens."""
        if token_id is None:
            return
        if global_user_state.delete_service_account_token(token_id):
            logger.info('Revoked controller API access token for job %s',
                        job_id)

    async def _cleanup(self,
                       job_id: int,
                       pool: str | None = None,
                       graceful: bool = False,
                       graceful_timeout: int | None = None):
        """Clean up the cluster(s) and storages.

        (1) Clean up the succeeded task(s)' ephemeral storage. The storage has
            to be cleaned up after the whole job is finished, as the tasks
            may share the same storage.
        (2) Clean up the cluster(s) that are not cleaned up yet, which can
            happen when the task failed or cancelled. At most one cluster
            should be left when reaching here, as we currently only support
            chain DAGs, and only one task is executed at a time.
        """
        # Cleanup the HA recovery script first as it is possible that some error
        # was raised when we construct the task object (e.g.,
        # sky.exceptions.ResourcesUnavailableError).
        await managed_job_state.remove_ha_recovery_script_async(job_id)

        def task_cleanup(task: 'sky.Task', job_id: int):
            assert task.name is not None, task
            error = None
            cluster_name = None

            try:
                if task.metadata.get('batch_coordinator'):
                    # Batch coordinator tasks run inline on the controller
                    # — no separate cluster was provisioned, so skip
                    # cluster termination.
                    logger.info('Batch coordinator task — skipping cluster '
                                'termination.')
                elif pool is None:
                    cluster_name = (
                        managed_job_utils.generate_managed_job_cluster_name(
                            task.name, job_id))
                    managed_job_utils.terminate_cluster(
                        cluster_name,
                        graceful=graceful,
                        graceful_timeout=graceful_timeout)
                    status_request_id = sdk.status(cluster_names=[cluster_name],
                                                   all_users=True)
                    status = sdk.get(status_request_id)
                    assert (len(status) == 0 or
                            status[0]['status'] == sky.ClusterStatus.STOPPED), (
                                f'{cluster_name} is not down: {status}')
                    logger.info(f'{cluster_name} is down')
                else:
                    pool_cluster_name, job_id_on_pool_cluster = (
                        managed_job_state.get_pool_submit_info(job_id))
                    if pool_cluster_name is not None:
                        cluster_name = pool_cluster_name
                        if job_id_on_pool_cluster is not None:
                            cancel_request_id = sdk.cancel(
                                cluster_name=cluster_name,
                                job_ids=[job_id_on_pool_cluster],
                                _try_cancel_if_cluster_is_init=True)
                            sdk.get(cancel_request_id)
            except Exception as e:  # pylint: disable=broad-except
                error = e
                cluster_display_name = cluster_name or task.name
                logger.warning(
                    f'Failed to terminate cluster {cluster_display_name}: {e}')
                # we continue to try cleaning up whatever else we can.
            # Provider cleanup must use the same nested request boundary as
            # launch/cancel/down.  A stale slot attempt is then rejected before
            # effect admission, while reset waits for any already-admitted
            # request to publish exact process-family quiescence.
            for storage in task.storage_mounts.values():
                if storage.persistent:
                    continue
                try:
                    if storage.name is None:
                        raise exceptions.StorageSpecError(
                            'Ephemeral storage has no durable name.')
                    storage_request_id = sdk.storage_delete(storage.name)
                    sdk.get(storage_request_id)
                except ValueError:
                    # Missing durable storage state is an idempotent cleanup
                    # result, matching an already-deleted cluster.
                    logger.info(f'Ephemeral storage {storage.name!r} is '
                                'already deleted.')
                except Exception as e:  # pylint: disable=broad-except
                    error = e
                    logger.warning(f'Failed to teardown ephemeral storage '
                                   f'{storage.name!r}: {e}')
                    # Continue cleaning independent mounts and local files.

            # Clean up any files mounted from the local disk, such as two-hop
            # file mounts for non-consolidation mode.
            # For consolidation mode, the file_mounts are shared across
            # workloads and the lifecycle will be managed by API server.
            if not managed_job_utils.is_consolidation_mode():
                for file_mount in (task.file_mounts or {}).values():
                    try:
                        # Skip if we are using cloud storage as the source.
                        if data_utils.is_cloud_store_url(file_mount):
                            continue
                        path = os.path.expanduser(file_mount)
                        if os.path.isdir(path):
                            shutil.rmtree(path)
                        else:
                            os.remove(path)
                    except Exception as e:  # pylint: disable=broad-except
                        logger.warning(
                            f'Failed to clean up file mount {file_mount}: {e}')

            if error is not None:
                raise error

        dag = _get_dag(job_id)
        error = None
        for task in dag.tasks:
            # most things in this function are blocking
            try:
                await asyncio.to_thread(task_cleanup, task, job_id)
            except Exception as e:  # pylint: disable=broad-except
                error = e

        if error is not None:
            # we only raise the last error that occurred, but its fine to lose
            # some data here.
            raise error

    async def _download_logs_for_cancelled_job(self, controller: JobController,
                                               job_id: int, task_ids: list[int],
                                               dag: 'sky.Dag',
                                               pool: str | None) -> None:
        """Download logs for a cancelled job before cleanup.

        This ensures that logs remain accessible after job cancellation,
        using the same code path as successful/failed jobs by calling the
        JobController's download_log_and_stream method.

        The download is best-effort - if a cluster is already down or
        unreachable, we skip gracefully. For job groups, multiple tasks
        may have been running simultaneously, so we download logs for all
        of them.

        Args:
            controller: The JobController instance for this job.
            job_id: The managed job ID.
            task_ids: The task IDs that were actively running (need log
                download). For single tasks and pipelines this is typically
                one ID; for job groups it can be multiple.
            dag: The DAG for the job (used to get task names for cluster
                name generation).
            pool: Optional pool name if using a pool.
        """
        logger.info(f'Downloading logs for cancelled job {job_id}, '
                    f'task_ids {task_ids}')

        task_clusters: list[tuple[int, str, int | None]] = []
        if pool is not None:
            # Pool jobs are single-task; job groups don't support pools.
            cluster_name, job_id_on_pool_cluster = (
                await managed_job_state.get_pool_submit_info_async(job_id))

            if cluster_name is None:
                logger.info(f'No cluster found for job {job_id}. '
                            'Skipping log download.')
                return

            task_clusters.append(
                (task_ids[0], cluster_name, job_id_on_pool_cluster))
        else:
            for task_id in task_ids:
                try:
                    task = dag.tasks[task_id]
                    assert task.name is not None, task
                    cluster_name = (
                        managed_job_utils.generate_managed_job_cluster_name(
                            task.name, job_id))
                    task_clusters.append((task_id, cluster_name, None))
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        f'Failed to resolve cluster for job {job_id}, '
                        f'task {task_id}: '
                        f'{common_utils.format_exception(e)}')

        if not task_clusters:
            return

        cluster_names = [cluster_name for _, cluster_name, _ in task_clusters]
        try:
            clusters = await asyncio.to_thread(
                backend_utils.get_clusters,
                cluster_names=cluster_names,
                refresh=common.StatusRefreshMode.NONE,
                all_users=True,
                _include_is_managed=True)
        except Exception as e:  # pylint: disable=broad-except
            if pool is not None:
                raise
            logger.warning(
                f'Failed to resolve clusters for cancelled job {job_id}: '
                f'{common_utils.format_exception(e)}')
            return
        handles_by_cluster = {
            cluster['name']: cluster.get('handle') for cluster in clusters
        }

        for task_id, cluster_name, job_id_on_cluster in task_clusters:
            handle = handles_by_cluster.get(cluster_name)
            if handle is None:
                logger.info(
                    f'Cluster {cluster_name} not found for job {job_id}, '
                    f'task {task_id}. Skipping log download.')
                continue
            try:
                await asyncio.to_thread(controller.download_log_and_stream,
                                        task_id, handle, job_id_on_cluster)
            except Exception as e:  # pylint: disable=broad-except
                if pool is not None:
                    raise
                logger.warning(
                    f'Failed to download logs for job {job_id}, '
                    f'task {task_id}: {common_utils.format_exception(e)}')

    def _initialize_job_context(self, job_id: int, log_file: str,
                                pool: str | None) -> int | None:
        """Install the exact per-job context shared by run and cleanup work."""
        ctx = context.get()
        assert ctx is not None, 'Context is not initialized'
        ctx.redirect_log(pathlib.Path(log_file))
        logger.info(f'Starting job lifecycle for {job_id}')
        logger.info(f'  log_file={log_file}')
        logger.info(f'  pool={pool}')
        logger.info(f'From controller {self._controller_uuid}')
        logger.info(f'  pid={self._pid}')

        guarded_config_authority = (
            skypilot_config._postgres_server_config_is_authoritative())  # pylint: disable=protected-access
        job_rank = None
        env_vars: dict[str, str | None] = {}
        persisted_env_loaded = False
        env_content = file_content_utils.get_job_env_content(job_id)
        if env_content:
            try:
                env_vars = dotenv.dotenv_values(stream=io.StringIO(env_content))
                logger.info('Loading %d environment variables for job %s',
                            len(env_vars), job_id)
                for key, value in env_vars.items():
                    if key in _CONTROLLER_RUNTIME_ENV_VARS:
                        logger.warning(
                            'Ignoring persisted controller-owned '
                            'environment variable %s for job %s.', key, job_id)
                        continue
                    if value is not None:
                        ctx.override_envs({key: value})
                        logger.debug('Set environment variable: %s=%s', key,
                                     value)

                if ('SKYPILOT_JOB_ID_TO_RANK' in env_vars and
                        env_vars['SKYPILOT_JOB_ID_TO_RANK']):
                    try:
                        job_id_to_rank = json.loads(
                            env_vars['SKYPILOT_JOB_ID_TO_RANK'])
                        logger.debug('Loaded job_id_to_rank map: %s',
                                     job_id_to_rank)
                        job_rank = job_id_to_rank.get(str(job_id))
                    except json.JSONDecodeError as e:
                        logger.warning(
                            'Failed to parse SKYPILOT_JOB_ID_TO_RANK for job '
                            '%s: %s', job_id, e)
                else:
                    logger.debug('SKYPILOT_JOB_ID_TO_RANK not found in '
                                 'environment variables')
                persisted_env_loaded = True
            except Exception as e:  # pylint: disable=broad-except
                logger.error(
                    'Failed to load environment variables for job '
                    '%s: %s', job_id, e)
                if guarded_config_authority:
                    raise

        # Install the server-owned field after persisted user/legacy env so it
        # cannot replace the exact job bound to this coroutine.  It must be
        # visible before config reload so guarded child classification sees
        # the complete immutable job/slot/attempt identity.
        ctx.override_envs(
            {jobs_constants.CONTROLLER_JOB_ID_ENV_VAR: str(job_id)})

        try:
            # Cleanup needs the same user config/auth context as launch.  A
            # guarded controller never trusts a receipt copied through the
            # persisted environment: restore the exact snapshot first, then
            # mint a fresh receipt in this server-owned execution context.
            if guarded_config_authority or persisted_env_loaded:
                restored_snapshot = (
                    file_content_utils.restore_job_config_file(job_id))
                if (guarded_config_authority and restored_snapshot is not None):
                    config_path, config_bytes = restored_snapshot
                    ctx.override_envs(
                        skypilot_config.internal_config_snapshot_environment(
                            skypilot_config.
                            INTERNAL_CONFIG_SNAPSHOT_KIND_MANAGED_JOB,
                            config_path,
                            config_bytes,
                        ))
                skypilot_config.reload_config()
        except Exception as e:  # pylint: disable=broad-except
            logger.error('Failed to restore config snapshot for job %s: %s',
                         job_id, e)
            if guarded_config_authority:
                raise RuntimeError(
                    f'Failed to install guarded config snapshot for job '
                    f'{job_id}.') from e

        # Bind usage state after the per-job environment is installed.
        usage_lib.install_fresh_messages_for_current_context()
        return job_rank

    # Use context.contextual to enable per-job output redirection and env var
    # isolation.
    @asyncio_utils.shield
    async def _release_job_loop_ownership(self, job_id: int) -> None:
        """Release manager bookkeeping even under repeated cancellation."""
        async with self._job_tasks_lock:
            if job_id in self.starting:
                self.starting.remove(job_id)
                self._starting_signal.notify()
            self.job_tasks.pop(job_id, None)
            self._cleanup_only_job_ids.discard(job_id)
            controller_api_token_id = self._controller_api_token_ids.pop(
                job_id, None)

        # A cancellation that lands after the job task already finished
        # stores cancel info that no CancelledError handler will consume.
        async with self._cancel_info_lock:
            self._cancel_info.pop(job_id, None)

        try:
            await asyncio.to_thread(self._cleanup_controller_api_access,
                                    controller_api_token_id, job_id)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to revoke controller API access token '
                           f'for job {job_id}: {e}')

    @context.contextual_async
    async def run_job_loop(self,
                           job_id: int,
                           log_file: str,
                           pool: str | None = None):
        """Run one job while owning its controller-manager bookkeeping."""
        job_loop_task = asyncio.create_task(
            self._run_job_loop(job_id, log_file, pool))
        owner_cancelled = False
        cancellation_delivered = False
        try:
            while True:
                try:
                    await asyncio.shield(job_loop_task)
                    break
                except asyncio.CancelledError:  # noqa: ASYNC103
                    owner_cancelled = True
                    if job_loop_task.done():
                        break  # noqa: ASYNC104
                    if not cancellation_delivered:
                        # The first request starts inner cancellation
                        # finalization. Later requests must not interrupt it.
                        cancellation_delivered = True
                        job_loop_task.cancel()

            # Preserve an inner failure over owner cancellation. If the inner
            # task suppressed cancellation, the owner still reports its
            # cancellation after finalization completes.
            job_loop_task.result()
            if owner_cancelled:
                raise asyncio.CancelledError()
        finally:
            # Own launch admission at the outermost scope. Initialization can
            # fail before _run_job_loop reaches its durable-cleanup try/finally;
            # leaking this slot would stop a saturated controller indefinitely.
            # Shield the complete two-lock cleanup so a repeated cancellation
            # cannot strand launch capacity or stale ownership indefinitely.
            await self._release_job_loop_ownership(job_id)

    async def _run_job_loop(self,
                            job_id: int,
                            log_file: str,
                            pool: str | None = None):
        """Background task that runs the job loop."""
        job_rank = self._initialize_job_context(job_id, log_file, pool)

        cancelling = False
        superseded = False
        graceful, graceful_timeout = False, None
        controller = None
        task_id = None
        dag = None
        try:
            self._initialize_controller_api_access(job_id)
            controller = JobController(job_id, self.starting,
                                       self._job_tasks_lock,
                                       self._starting_signal, pool, job_rank)

            async with self._job_tasks_lock:
                if job_id in self.job_tasks:
                    logger.error(f'Job {job_id} already exists in job_tasks')
                    raise ValueError(f'Job {job_id} already exists')

                # Create the task and store it
                # This function should return instantly and run the job loop in
                # the background.
                task = asyncio.create_task(controller.run())
                self.job_tasks[job_id] = task
            await task
        except asyncio.CancelledError:
            logger.info(f'Job {job_id} was cancelled')

            async with self._cancel_info_lock:
                cancel_info = self._cancel_info.pop(job_id, None)
            if cancel_info is not None:
                graceful, graceful_timeout = cancel_info
                logger.debug(f'Job {job_id} graceful cancel: '
                             f'graceful={graceful}, timeout={graceful_timeout}')

            dag = _get_dag(job_id)

            # Query all task statuses BEFORE set_cancelling_async changes
            # them. At this point, statuses accurately reflect which tasks
            # were actually started vs still pending.
            id_statuses = await (
                managed_job_state.get_all_task_ids_statuses_async(job_id))

            # The "latest" non-terminal task - needed for
            # set_cancelling_async callback and set_cancelled_async later.
            task_id, _ = (
                managed_job_state.get_latest_task_id_from_statuses(id_statuses))
            assert task_id is not None, job_id
            logger.info(f'Cancelling managed job, job_id: {job_id}, '
                        f'task_id: {task_id}')

            # Tasks that were actually started (have clusters with logs to
            # download). PENDING tasks never had a cluster; terminal tasks
            # already had logs downloaded via the normal path.
            # - Pipeline: only the currently-running task is active; later
            #   tasks are still PENDING.
            # - Job group: all tasks that haven't already finished are
            #   active (they run in parallel).
            active_task_ids = [
                tid for tid, status in id_statuses
                if not status.is_terminal() and
                status != managed_job_state.ManagedJobStatus.PENDING
            ]

            await managed_job_state.set_cancelling_async(
                job_id=job_id,
                callback_func=managed_job_utils.event_callback_func(
                    job_id=job_id, task_id=task_id, task=dag.tasks[task_id]))

            # Download logs before cleanup so they remain accessible after
            # cancellation. This is best-effort - if the cluster is already
            # down, we skip gracefully.
            if active_task_ids:
                try:
                    if controller is None:
                        logger.warning('Skipping log download because the job '
                                       'controller was not initialized before '
                                       'cancellation.')
                    else:
                        await self._download_logs_for_cancelled_job(
                            controller, job_id, active_task_ids, dag, pool)
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        f'Failed to download logs for cancelled job '
                        f'{job_id}: {common_utils.format_exception(e)}')

            cancelling = True
            raise
        except batch_coordinator.SupersededCoordinator:
            superseded = True
            logger.info(
                'Job loop for Batch job %s was superseded; preserving '
                'replacement-owned durable state and resources.', job_id)
            raise
        except Exception as e:
            logger.error(f'Unexpected error in job loop for {job_id}: '
                         f'{common_utils.format_exception(e)}')
            raise
        finally:
            if not superseded:
                cleanup_succeeded = False
                try:
                    await self._cleanup(job_id,
                                        pool=pool,
                                        graceful=graceful,
                                        graceful_timeout=graceful_timeout)
                    cleanup_succeeded = True
                    logger.info(
                        f'Cluster of managed job {job_id} has been cleaned up.')
                except Exception as e:  # pylint: disable=broad-except
                    failure_reason = ('Failed to clean up: '
                                      f'{common_utils.format_exception(e)}')
                    job_status = await managed_job_state.get_status_async(
                        job_id=job_id)
                    if job_status is not None and job_status.is_terminal():
                        # The workload outcome is authoritative once it is
                        # terminal. Pool cleanup only releases worker-local
                        # resources and can fail transiently (for example,
                        # while SSM is reconnecting). Do not turn a successful
                        # workload into FAILED_CONTROLLER because best-effort
                        # cleanup could not reach the worker.
                        logger.warning(
                            f'{failure_reason}. Preserving terminal job '
                            f'status {job_status.value!r} for job {job_id}.')
                    else:
                        await managed_job_state.set_failed_async(
                            job_id,
                            task_id=None,
                            failure_type=managed_job_state.ManagedJobStatus.
                            FAILED_CONTROLLER,
                            failure_reason=failure_reason,
                            override_terminal=True)

                if cancelling:
                    # Since it's set with cancelling
                    assert task_id is not None, job_id
                    assert dag is not None, job_id
                    await managed_job_state.set_cancelled_async(
                        job_id=job_id,
                        callback_func=managed_job_utils.event_callback_func(
                            job_id=job_id,
                            task_id=task_id,
                            task=dag.tasks[task_id]))

                # Check status after set_cancelled so cancellation is terminal.
                job_status = await managed_job_state.get_status_async(job_id)
                assert job_status is not None
                if not job_status.is_terminal():
                    logger.info(f'Previous job status: {job_status.value}')
                    await managed_job_state.set_failed_async(
                        job_id,
                        task_id=None,
                        failure_type=managed_job_state.ManagedJobStatus.
                        FAILED_CONTROLLER,
                        failure_reason=(
                            'Unexpected error occurred. For details, '
                            f'run: sky jobs logs --controller {job_id}'))

                try:
                    await asyncio.to_thread(
                        self._cleanup_api_server_access_token, job_id)
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning('Failed to revoke API server access token '
                                   f'for job {job_id}: {e}')

                if cleanup_succeeded or pool is not None:
                    await scheduler.job_done_async(job_id)
                else:
                    # Keep the job visible to the status reconciler until
                    # provider cleanup succeeds. Marking it DONE here would
                    # exclude a terminal job from ordinary status sweeps and
                    # leave its cluster row accruing estimated spend forever.
                    logger.warning(
                        f'Deferring scheduler finalization for managed job '
                        f'{job_id} until cluster cleanup succeeds.')

    @context.contextual_async
    async def run_cleanup_loop(self,
                               job_id: int,
                               log_file: str,
                               pool: str | None = None) -> None:
        """Adopt terminal work without entering the workload execution path."""
        initialized = False
        cleanup_complete = False
        retry_seconds = _TERMINAL_CLEANUP_RETRY_INITIAL_SECONDS
        try:
            while True:
                try:
                    if not initialized:
                        self._initialize_job_context(job_id, log_file, pool)
                        self._initialize_controller_api_access(job_id)
                        initialized = True
                    if not cleanup_complete:
                        # This is the same canonical provider/storage cleanup
                        # used by the ordinary job finalizer.  No JobController
                        # is constructed and no workload callback/state path is
                        # entered for an already-terminal task family.
                        await self._cleanup(job_id, pool=pool)
                        cleanup_complete = True
                        logger.info('Terminal cleanup completed for managed '
                                    f'job {job_id}.')
                    await asyncio.to_thread(
                        self._cleanup_api_server_access_token, job_id)
                    await managed_job_state.scheduler_set_cleanup_done_async(
                        job_id)
                    logger.info('Cleanup-only managed job %s is DONE.', job_id)
                    return
                except asyncio.CancelledError:
                    raise
                except managed_job_state.ControllerLeadershipLostError:
                    # The guardian owns death/replacement.  A fenced manager
                    # must leave this loop so its exact attempt can drain and
                    # the terminal row can be re-adopted; retrying under a
                    # lost claim would only replay stale finalizer phases.
                    raise
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        'Cleanup-only managed job %s remains claimed after '
                        'failure; retrying in %ss: %s', job_id, retry_seconds,
                        common_utils.format_exception(e))
                    await asyncio.sleep(retry_seconds)
                    retry_seconds = min(retry_seconds * 2,
                                        _TERMINAL_CLEANUP_RETRY_MAX_SECONDS)
        finally:
            await self._release_job_loop_ownership(job_id)

    async def start_cleanup_job(self,
                                job_id: int,
                                pool: str | None = None) -> None:
        """Hand one cleanup-only claim to a tracked contextual coroutine."""
        log_file = await asyncio.to_thread(_prepare_job_log_path, job_id)
        async with self._job_tasks_lock:
            if job_id in self.job_tasks:
                raise ValueError(
                    f'Managed job {job_id} already has local lifecycle work.')
            task = create_background_task(
                self.run_cleanup_loop(job_id, log_file, pool))
            self.job_tasks[job_id] = task
            self._cleanup_only_job_ids.add(job_id)

    async def start_job(
        self,
        job_id: int,
        pool: str | None = None,
    ):
        """Start a new job.

        Args:
            job_id: The ID of the job to start.
        """
        log_file = await asyncio.to_thread(_prepare_job_log_path, job_id)

        logger.info(f'Starting job {job_id} with log_file={log_file}')

        async with self._job_tasks_lock:
            self.starting.add(job_id)
            # No await between reserving capacity and scheduling its owner.
            create_background_task(self.run_job_loop(job_id, log_file, pool))

        logger.info(f'Job {job_id} started successfully')

    async def cancel_job(self):
        """Cancel an existing job."""
        while True:
            try:
                await self._process_cancel_signals()
            except Exception as e:  # pylint: disable=broad-except
                # A failed scan (e.g. a transient filesystem error) must not
                # unwind this loop: it is gathered with the monitor loop in
                # main(), so an escaped exception exits the whole controller
                # process and kills every running job task. Unconsumed
                # signals are re-listed by the next scan.
                logger.error('Cancel signal scan failed: '
                             f'{common_utils.format_exception(e)}')
            await asyncio.sleep(15)

    async def _process_cancel_signals(self):
        """Run one scan of the cancel signal directory."""
        cancels = await asyncio.to_thread(
            os.listdir, jobs_constants.CONSOLIDATED_SIGNAL_PATH)
        cancel_job_ids = []
        for cancel in cancels:
            if not cancel.isdigit():
                # There maybe unexpected files that are written to the
                # signal directory. We for sure write filelocks to the
                # directory, so we need to skip.
                if not cancel.endswith('.lock'):
                    logger.debug('Detected unexpected file in signal '
                                 f'directory: {cancel}. Skipping...')
                continue
            cancel_job_ids.append(int(cancel))

        # Snapshot ownership once, then deliver local cancellations before
        # waiting on status I/O for signals owned by other controllers. A
        # concurrent local claim conservatively leaves its non-terminal signal
        # for the next scan.
        async with self._job_tasks_lock:
            owned_tasks = [(job_id, self.job_tasks[job_id])
                           for job_id in cancel_job_ids
                           if (job_id in self.job_tasks and
                               job_id not in self._cleanup_only_job_ids)]
        owned_job_ids = {job_id for job_id, _ in owned_tasks}
        orphan_job_ids = [
            job_id for job_id in cancel_job_ids if job_id not in owned_job_ids
        ]

        await asyncio.gather(*(self._deliver_owned_cancel(job_id, task)
                               for job_id, task in owned_tasks))

        if not orphan_job_ids:
            return
        orphan_statuses = await managed_job_state.get_statuses_async(
            orphan_job_ids)
        reap_job_ids = [
            job_id for job_id in orphan_job_ids
            if orphan_statuses[job_id] is None or
            orphan_statuses[job_id].is_terminal()
        ]
        reap_job_ids_iter = iter(reap_job_ids)

        async def reap_signals() -> None:
            for job_id in reap_job_ids_iter:
                await self._reap_orphan_cancel_signal(job_id)

        worker_count = min(len(reap_job_ids),
                           controller_utils.LAUNCHES_PER_WORKER)
        await asyncio.gather(*(reap_signals() for _ in range(worker_count)))

    async def _deliver_owned_cancel(self, job_id: int,
                                    task: asyncio.Task) -> None:
        """Deliver one owned cancellation without aborting sibling work."""
        logger.info(f'Cancelling job {job_id}')
        try:
            await self._consume_and_cancel_task(job_id, task)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Failed to cancel job {job_id}: '
                         f'{common_utils.format_exception(e)}')

    @asyncio_utils.shield
    async def _consume_and_cancel_task(self, job_id: int,
                                       task: asyncio.Task) -> None:
        """Consume a cancel signal and deliver it without interruption.

        Shield the complete consume-and-deliver operation rather than only
        the file-lock critical section. Otherwise cancellation of the scan
        could let the background lock holder delete the signal without
        cancelling the job.
        """
        content = await self._consume_signal_file(job_id)
        if content is None:
            # The signal was consumed between the directory listing and
            # acquiring the lock (e.g. by a sibling controller process).
            return

        # Parse and store graceful cancel info before cancelling the task.
        graceful, graceful_timeout = (
            managed_job_utils.parse_job_cancel_file(content))
        async with self._cancel_info_lock:
            self._cancel_info[job_id] = (graceful, graceful_timeout)
        task.cancel()
        logger.info(f'Job {job_id} cancelled successfully')

    @staticmethod
    async def _consume_signal_file(job_id: int) -> str | None:
        """Read and consume a cancel signal without blocking the event loop.

        The caller must shield the complete operation that owns delivery of
        the consumed signal. This helper takes the same file lock as signal
        writers and other consumers; missing_ok covers a lost race with a
        sibling controller process.
        """
        signal_path = pathlib.Path(jobs_constants.CONSOLIDATED_SIGNAL_PATH,
                                   str(job_id))
        async with filelock.AsyncFileLock(f'{signal_path}.lock'):
            try:
                content = (await
                           anyio.Path(signal_path).read_text(encoding='utf-8'
                                                            )).strip()
            except FileNotFoundError:
                return None
            except Exception as e:  # pylint: disable=broad-except
                logger.debug('Problem occurred when reading '
                             f'{signal_path}: '
                             f'{common_utils.format_exception(e)}')
                return None
            await anyio.Path(signal_path).unlink(missing_ok=True)
            return content

    @staticmethod
    @asyncio_utils.shield
    async def _remove_signal_file(job_id: int) -> None:
        """Consume a job's cancel signal file, tolerating a lost race."""
        await ControllerManager._consume_signal_file(job_id)

    @asyncio_utils.shield
    async def _reap_orphan_cancel_signal(self, job_id: int) -> None:
        """Remove one eligible orphan signal and its local bookkeeping.

        A signal file is normally consumed either by the owning job task
        in this process, or at claim time while the job is still PENDING.
        If the job reaches a terminal state in between (e.g. it finished
        right as the cancellation landed), neither consumer ever runs
        again for it, and the file would be re-listed by every scan of
        every controller process forever. The caller only schedules jobs
        whose batched status snapshot is terminal or absent.

        Shield removal together with cancel-info cleanup so scan cancellation
        cannot consume the durable signal but leave stale local bookkeeping.
        """
        try:
            await self._remove_signal_file(job_id)
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f'Failed to reap cancel signal for job {job_id}: '
                         f'{common_utils.format_exception(e)}')
            return
        async with self._cancel_info_lock:
            self._cancel_info.pop(job_id, None)
        logger.info(f'Reaped cancel signal for terminated job {job_id}.')

    async def monitor_loop(self):
        """Monitor the job loop."""
        logger.info(f'Starting monitor loop for pid {self._pid}...')
        pid_str = str(self._pid)

        while True:
            async with self._job_tasks_lock:
                running_tasks = [
                    task for task in self.job_tasks.values() if not task.done()
                ]
                starting_count = len(self.starting)

            # Report per-process metrics.
            if metrics_lib.METRICS_ENABLED:
                metrics_lib.SKY_MANAGED_JOBS_CONTROLLER_STARTING_COUNT.labels(
                    pid=pid_str).set(starting_count)
                metrics_lib.SKY_MANAGED_JOBS_CONTROLLER_RUNNING_COUNT.labels(
                    pid=pid_str).set(len(running_tasks))
                metrics_lib.SKY_MANAGED_JOBS_LIMIT_LAUNCHES_PER_WORKER.labels(
                    pid=pid_str).set(controller_utils.LAUNCHES_PER_WORKER)

            if starting_count >= controller_utils.LAUNCHES_PER_WORKER:
                logger.info('Too many jobs starting, waiting for a slot')
                async with self._starting_signal:
                    await self._starting_signal.wait_for(lambda: len(
                        self.starting) < controller_utils.LAUNCHES_PER_WORKER)
                continue

            # Normally, 200 jobs can run on each controller. But if we have a
            # ton of controllers, we need to limit the number of jobs that can
            # run on each controller, to achieve a total of 2000 jobs across all
            # controllers.
            max_jobs = min(controller_utils.MAX_JOBS_PER_WORKER,
                           (controller_utils.MAX_TOTAL_RUNNING_JOBS //
                            controller_utils.get_number_of_jobs_controllers()))

            if metrics_lib.METRICS_ENABLED:
                metrics_lib.SKY_MANAGED_JOBS_CONTROLLER_MAX_JOBS.labels(
                    pid=pid_str).set(max_jobs)

            if len(running_tasks) >= max_jobs:
                logger.info('Too many jobs running, waiting for capacity')
                if running_tasks:
                    # Recheck immediately when a task that contributed to the
                    # limit finishes. Keep the timeout because max_jobs can
                    # change when the controller process count changes.
                    await asyncio.wait(running_tasks,
                                       timeout=60,
                                       return_when=asyncio.FIRST_COMPLETED)
                else:
                    # max_jobs may be zero when the controller process count
                    # exceeds MAX_TOTAL_RUNNING_JOBS. asyncio.wait() rejects
                    # an empty task set, so retain the topology recheck here.
                    await asyncio.sleep(60)
                continue

            # Check if there are any jobs that are waiting to launch
            try:
                waiting_job = await managed_job_state.get_waiting_job_async(
                    pid=self._pid,
                    pid_started_at=self._pid_started_at,
                    controller_slot_id=self._require_controller_slot_id(),
                    controller_slot_attempt=(
                        self._require_controller_slot_attempt()))
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Failed to get waiting job: {e}')
                await asyncio.sleep(5)
                continue

            if waiting_job is None:
                logger.info('No waiting job, waiting for 10 seconds')
                await asyncio.sleep(10)
                continue

            logger.info(f'Claiming job {waiting_job["job_id"]}')
            job_id = waiting_job['job_id']
            pool = waiting_job.get('pool', None)
            cleanup_only = waiting_job['cleanup_only']

            cancels = await asyncio.to_thread(
                os.listdir, jobs_constants.CONSOLIDATED_SIGNAL_PATH)
            if str(job_id) in cancels:
                status = await managed_job_state.get_status_async(job_id)
                if status == managed_job_state.ManagedJobStatus.PENDING:
                    logger.info(f'Job {job_id} cancelled')
                    await self._remove_signal_file(job_id)
                    await managed_job_state.set_cancelling_async(
                        job_id=job_id,
                        callback_func=managed_job_utils.event_callback_func(
                            job_id=job_id, task_id=None, task=None))
                    await managed_job_state.set_cancelled_async(
                        job_id=job_id,
                        callback_func=managed_job_utils.event_callback_func(
                            job_id=job_id, task_id=None, task=None))
                    # get_waiting_job_async already moved this job to LAUNCHING
                    # under our pid. Without this the schedule state would stay
                    # LAUNCHING forever: get_num_alive_jobs() would never drop,
                    # so the controller could never autostop, and every status
                    # sweep would keep re-checking the job.
                    await scheduler.job_done_async(job_id, idempotent=True)
                    continue

            if cleanup_only:
                await self.start_cleanup_job(job_id, pool)
            else:
                await self.start_job(job_id, pool)


async def _finish_superseded_cleanup(
        coordinator: batch_coordinator.BatchCoordinator) -> None:
    """Finish bounded cleanup before preserving the supersession signal."""
    cleanup_task = asyncio.create_task(coordinator.handle_superseded())
    while True:
        try:
            await asyncio.shield(cleanup_task)
            break
        except asyncio.CancelledError:  # noqa: ASYNC103
            # Cancellation must not let the old controller resume durable
            # finalization while the replacement controller owns the job.
            if cleanup_task.done():
                break  # noqa: ASYNC104

    cleanup_task.result()


def _require_bootstrapped_controller_origin_capability() -> None:
    """Fail closed unless the stdlib bootstrap installed manager authority."""
    raw_capability_fd = os.environ.pop(
        jobs_constants.CONTROLLER_CAPABILITY_FD_ENV_VAR, None)
    os.environ.pop(jobs_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR, None)
    os.environ.pop(
        jobs_constants.CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR,
        None)
    if raw_capability_fd is not None:
        raise RuntimeError(
            'ControllerManager capability bypassed the pre-import bootstrap.')
    if controller_capability.get_process_local() is None:
        raise RuntimeError(
            'ControllerManager has no process-local capability authority.')


async def main(controller_uuid: str, controller_slot_id: int,
               controller_slot_attempt: str):
    _require_bootstrapped_controller_origin_capability()
    db_utils.set_postgres_connection_metrics_process_role(
        'managed-job-controller')
    logger.info(f'Starting controller {controller_uuid}')

    context_utils.hijack_sys_attrs()

    plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.CONTROLLER))

    controller = ControllerManager(controller_uuid, controller_slot_id,
                                   controller_slot_attempt)

    # Will happen multiple times, who cares though
    os.makedirs(jobs_constants.CONSOLIDATED_SIGNAL_PATH, exist_ok=True)

    # Increase number of files we can open
    soft = None
    hard = None
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        logger.info(f'Current rlimits for NOFILE: soft={soft}, hard={hard}')
        logger.info(f'Increasing soft limit to {hard}')
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except OSError as e:
        logger.warning(f'Failed to increase number of files we can open: {e}\n'
                       f'Current soft limit: {soft}, hard limit: {hard}')

    # Will loop forever, do it in the background
    cancel_job_task = asyncio.create_task(controller.cancel_job())
    monitor_loop_task = asyncio.create_task(controller.monitor_loop())
    controller_tasks = [cancel_job_task, monitor_loop_task]
    outer_owner = managed_job_state.get_current_controller_owner()
    if outer_owner is not None:
        controller_tasks.append(
            asyncio.create_task(
                _watch_outer_controller_generation(outer_owner)))
    # A successful fork is not manager readiness: imports, plugin setup, or
    # long-lived loop construction can still fail immediately.  Give every
    # loop one turn, reject an already-failed loop, then complete the one-shot
    # guardian handshake before the runtime marks this slot started.
    await asyncio.sleep(0)
    for controller_task in controller_tasks:
        if controller_task.done():
            controller_task.result()
            raise RuntimeError(
                'Managed-job controller loop exited during initialization.')
    raw_ready_fd = os.environ.pop(jobs_constants.CONTROLLER_READY_FD_ENV_VAR,
                                  None)
    if raw_ready_fd is None:
        raise RuntimeError('Managed-job controller has no readiness channel.')
    try:
        ready_fd = int(raw_ready_fd)
        os.write(ready_fd, b'1')
    finally:
        try:
            os.close(int(raw_ready_fd))
        except (OSError, ValueError):
            pass
    # Run the garbage collector in a dedicated daemon thread to avoid affecting
    # the main event loop.
    gc_thread = threading.Thread(target=log_gc.elect_for_log_gc, daemon=True)
    gc_thread.start()
    try:
        await asyncio.gather(*controller_tasks)
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Controller server crashed: {e}')
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        raise RuntimeError('ControllerManager requires UUID, slot ID, and '
                           'slot-attempt arguments.')
    asyncio.run(main(sys.argv[1], int(sys.argv[2]), sys.argv[3]))
