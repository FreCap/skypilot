"""User interfaces with managed jobs.

NOTE: whenever an API change is made in this file, we need to bump the
jobs.constants.MANAGED_JOBS_VERSION and handle the API change in the
ManagedJobCodeGen.
"""
import asyncio
import contextlib
from datetime import datetime
import os
import pathlib
import re
import select
import signal
import sys
import threading
import time
import traceback
import typing
from typing import Any, Optional

import colorama
import filelock

from sky import backends
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend
from sky.jobs import constants as managed_job_constants
from sky.jobs import controller_log_stream
from sky.jobs import debug_dump as managed_job_debug_dump
from sky.jobs import managed_job_codegen
from sky.jobs import queue_utils as managed_job_queue_utils
from sky.jobs import runtime as managed_job_runtime
from sky.jobs import scheduler
from sky.jobs import state as managed_job_state
from sky.jobs.naming import generate_managed_job_cluster_name
from sky.skylet import constants
from sky.skylet import job_lib
from sky.skylet import log_lib
from sky.usage import usage_lib
from sky.utils import annotations
from sky.utils import common as common_lib
from sky.utils import common_utils
from sky.utils import context_utils
from sky.utils import controller_utils
from sky.utils import debug_dump_helpers
from sky.utils import message_utils
from sky.utils import rich_utils
from sky.utils import subprocess_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from google.protobuf import descriptor
    from google.protobuf import json_format
    import grpc
    import psutil

    import sky
    from sky.schemas.generated import jobsv1_pb2
    from sky.schemas.generated import managed_jobsv1_pb2
else:
    json_format = adaptors_common.LazyImport('google.protobuf.json_format')
    descriptor = adaptors_common.LazyImport('google.protobuf.descriptor')
    psutil = adaptors_common.LazyImport('psutil')
    grpc = adaptors_common.LazyImport('grpc')
    jobsv1_pb2 = adaptors_common.LazyImport('sky.schemas.generated.jobsv1_pb2')
    managed_jobsv1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.managed_jobsv1_pb2')

logger = sky_logging.init_logger(__name__)

ManagedJobQueueResultType = managed_job_queue_utils.ManagedJobQueueResultType
load_managed_job_queue = managed_job_queue_utils.load_managed_job_queue
filter_jobs = managed_job_queue_utils.filter_jobs
format_job_table = managed_job_queue_utils.format_job_table
decode_managed_job_protos = managed_job_queue_utils.decode_managed_job_protos
infra_utils = managed_job_queue_utils.infra_utils
resources_utils = managed_job_queue_utils.resources_utils
_CLUSTER_HANDLE_FIELDS = getattr(managed_job_queue_utils,
                                 '_CLUSTER_HANDLE_FIELDS')
_NON_DB_FIELDS = getattr(managed_job_queue_utils, '_NON_DB_FIELDS')
_cluster_handle_not_required = getattr(managed_job_queue_utils,
                                       '_cluster_handle_not_required')
_update_fields = getattr(managed_job_queue_utils, '_update_fields')
_format_job_details = getattr(managed_job_queue_utils, '_format_job_details')
_populate_job_records_from_handles = getattr(
    managed_job_queue_utils, '_populate_job_records_from_handles')
_populate_job_record_from_handle = getattr(managed_job_queue_utils,
                                           '_populate_job_record_from_handle')
_job_proto_to_dict = getattr(managed_job_queue_utils, '_job_proto_to_dict')


def _sync_queue_facade() -> None:
    managed_job_queue_utils.generate_managed_job_cluster_name = (
        generate_managed_job_cluster_name)


def get_managed_job_queue(*args, **kwargs):
    _sync_queue_facade()
    return managed_job_queue_utils.get_managed_job_queue(*args, **kwargs)


def dump_managed_job_queue(*args, **kwargs):
    _sync_queue_facade()
    return managed_job_queue_utils.dump_managed_job_queue(*args, **kwargs)


# Controller checks its job's status every this many seconds.
# This is a tradeoff between the latency and the resource usage.
JOB_STATUS_CHECK_GAP_SECONDS = 15

# Controller checks if its job has started every this many seconds.
JOB_STARTED_STATUS_CHECK_GAP_SECONDS = 5

_LOG_STREAM_CHECK_CONTROLLER_GAP_SECONDS = 5

# While a managed job is provisioning, we poll the jobs controller log this
# often to relay the cluster-launch spinner messages (e.g. "Preparing SkyPilot
# runtime (1/3)") to the user. This is faster than JOB_STATUS_CHECK_GAP_SECONDS
# so the spinner feels responsive without polling the job-status DB as often.
_PROVISION_LOG_POLL_GAP_SECONDS = 1

_JOB_STATUS_FETCH_TIMEOUT_SECONDS = 30
JOB_STATUS_FETCH_TOTAL_TIMEOUT_SECONDS = 60

_JOB_WAITING_STATUS_MESSAGE = ux_utils.spinner_message(
    'Waiting for task to start[/]'
    '{status_str}. It may take a few minutes.{provision_str}\n'
    '  [dim]View controller logs: sky jobs logs --controller {job_id}')
_JOB_CANCELLED_MESSAGE = (
    ux_utils.spinner_message('Waiting for task status to be updated.') +
    ' It may take a minute.')

# The maximum time to wait for the managed job status to transition to terminal
# state, after the job finished. This is a safeguard to avoid the case where
# the managed job status fails to be updated and keep the `sky jobs logs`
# blocking for a long time. This should be significantly longer than the
# JOB_STATUS_CHECK_GAP_SECONDS to avoid timing out before the controller can
# update the state.
_FINAL_JOB_STATUS_WAIT_TIMEOUT_SECONDS = 120

# Content written to the jobs cancel signal file.
_JOBS_GRACEFUL_CANCEL_SIGNAL = 'graceful'


# ====== internal functions ======
def _sleep_log_follow_wait(seconds: float) -> None:
    """Sleep between log-follow polls while honoring request cancellation."""
    context_utils.sleep_with_cancellation(seconds)


def terminate_cluster(
    cluster_name: str,
    max_retry: int = 6,
    graceful: bool = False,
    graceful_timeout: int | None = None,
) -> None:
    """Terminate the cluster."""
    from sky import core  # pylint: disable=import-outside-toplevel

    # Pin the active workspace to the cluster's recorded workspace before
    # calling `core.down`. Controller-side callers (cancel and recovery
    # teardown paths) run in the system/daemon process, without this pin
    # `skypilot_config.get_active_workspace()` falls back to the default
    # workspace and the owner-identity check at
    # `backend_utils._check_owner_identity_with_record` fails for any
    # cluster whose recorded workspace is not 'default'.
    # DB lookup once outside the loop — cluster workspace is immutable. This
    # is also the authoritative existence check: callers do not need a
    # separate handle lookup before teardown.
    record = global_user_state.get_cluster_from_name(cluster_name,
                                                     include_user_info=False,
                                                     summary_response=True)
    if record is None:
        logger.debug(f'The cluster {cluster_name} is already down.')
        return
    cluster_workspace = record.get('workspace')

    retry_cnt = 0
    # In some cases, e.g. botocore.exceptions.NoCredentialsError due to AWS
    # metadata service throttling, the failed sky.down attempt can take 10-11
    # seconds. In this case, we need the backoff to significantly reduce the
    # rate of requests - that is, significantly increase the time between
    # requests. We set the initial backoff to 15 seconds, so that once it grows
    # exponentially it will quickly dominate the 10-11 seconds that we already
    # see between requests. We set the max backoff very high, since it's
    # generally much more important to eventually succeed than to fail fast.
    backoff = common_utils.Backoff(
        initial_backoff=15,
        # 1.6 ** 5 = 10.48576 < 20, so we won't hit this with default max_retry
        max_backoff_factor=20)
    while True:
        try:
            usage_lib.messages.usage.set_internal()
            # Construct the ctx inside the loop: `local_active_workspace_ctx`
            # is a `@contextlib.contextmanager` generator and cannot be
            # re-entered — reusing one instance across retries raises
            # `RuntimeError` from the spent generator and masks the real
            # failure.
            workspace_ctx: contextlib.AbstractContextManager = (
                skypilot_config.local_active_workspace_ctx(cluster_workspace)
                if cluster_workspace else contextlib.nullcontext())
            with workspace_ctx:
                core.down(cluster_name,
                          graceful=graceful,
                          graceful_timeout=graceful_timeout)
            return
        except exceptions.ClusterDoesNotExist:
            # The cluster is already down.
            logger.debug(f'The cluster {cluster_name} is already down.')
            return
        except Exception as e:  # pylint: disable=broad-except
            retry_cnt += 1
            if retry_cnt >= max_retry:
                raise RuntimeError(
                    f'Failed to terminate the cluster {cluster_name}.') from e
            logger.error(
                f'Failed to terminate the cluster {cluster_name}. Retrying.'
                f'Details: {common_utils.format_exception(e)}')
            with ux_utils.enable_traceback():
                logger.error(f'  Traceback: {traceback.format_exc()}')
            time.sleep(backoff.current_backoff())


def setup_consolidation_mode_on_startup(deploy: bool) -> None:
    """Set up consolidation mode signal file on API server startup.

    Must be called AFTER global_user_state DB is initialized and
    server user hash is restored, so we can query for existing controller
    clusters.

    For explicit config (True/False): touches or removes signal file.
    For unset config (None):
      - in local mode (deploy=False): default to disabled
      - in deploy mode: default to enabled if no existing controller clusters
        found in DB, otherwise disabled (to continue using existing controller)
    """
    config_value = skypilot_config.get_nested(
        ('jobs', 'controller', 'consolidation_mode'), default_value=None)
    signal_file = pathlib.Path(
        managed_job_constants.JOBS_CONSOLIDATION_RELOADED_SIGNAL_FILE
    ).expanduser()

    if config_value is not None:
        assert isinstance(config_value, bool), config_value
        enabled = config_value
    else:
        # config_value is None — not explicitly set
        if deploy:
            # Deploy mode, config not set: auto-enable unless controllers exist
            existing = global_user_state.get_cluster_names_start_with(
                common_lib.JOB_CONTROLLER_PREFIX)
            if existing:
                logger.info(
                    'Found existing jobs controller cluster(s): '
                    f'{existing}. Not auto-enabling consolidation mode.')
                enabled = False
            else:
                logger.info('Auto-enabling jobs consolidation mode for deploy '
                            'mode server.')
                enabled = True
        else:
            # Local API server: don't auto-enable
            enabled = False

    controller_utils.warn_jobs_consolidation_mode_intent(enabled)

    if enabled:
        signal_file.touch()
    elif signal_file.exists():
        signal_file.unlink()


# Whether to use consolidation mode or not. When this is enabled, the managed
# jobs controller will not be running on a separate cluster, but locally on the
# API Server. Under the hood, we submit the job monitoring logic as processes
# directly in the API Server.
# Thin wrapper around controller_utils.is_jobs_consolidation_mode — the helper
# owns the signal-file read, the config-vs-signal restart warning, and the
# jobs validator call. See controller_utils for the full contract.
# INVARIANT: serve_utils.is_consolidation_mode(pool=True) routes through the
# same helper, so pool and managed-jobs readers cannot diverge.
@annotations.lru_cache(scope='request', maxsize=1)
def is_consolidation_mode() -> bool:
    return controller_utils.is_jobs_consolidation_mode()


_MANAGED_JOB_TOKEN_NAME_RE = re.compile(
    f'^{re.escape(managed_job_constants.MANAGED_JOB_TOKEN_NAME_PREFIX)}'
    r'.+-[0-9a-f]{8}$')


def cleanup_expired_api_access_tokens() -> int:
    """Delete expired managed-job API access tokens.

    Scans the service_account_tokens table for any token whose name starts
    with the managed-job prefix and whose expires_at is in the past, then
    requires the name to also end with the 8-hex-char dag_uuid suffix
    produced by _create_job_api_token. Matching tokens are deleted.

    Driving the sweep off the name shape means tokens that leaked due to
    a controller crash mid-cleanup, or that were issued by older code
    paths, are still reaped once their TTL passes.

    Limitation: a user could in principle create a custom service-account
    token whose name happens to match `managed-job-<anything>-<8 hex>` and
    let it expire. The daemon would treat such a token as a leaked
    managed-job token and remove it once expired. The prefix + 8-hex-char
    suffix combination makes accidental collisions unlikely in practice,
    but custom token names should avoid this shape if expired tokens are
    meant to be retained for audit.

    Returns the number of tokens removed.
    """
    now = int(time.time())
    prefix = managed_job_constants.MANAGED_JOB_TOKEN_NAME_PREFIX
    expired = (
        global_user_state.get_expired_service_account_tokens_by_name_prefix(
            prefix, now))
    removed = 0
    for token in expired:
        token_name = token.get('token_name') or ''
        if not _MANAGED_JOB_TOKEN_NAME_RE.match(token_name):
            # Prefix matched but the suffix does not look like a managed-job
            # dag_uuid; leave it alone to avoid touching user-created tokens
            # that happen to share the prefix.
            continue
        token_id = token['token_id']
        try:
            global_user_state.delete_service_account_token(token_id)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                f'Failed to delete expired managed-job token {token_id}: {e}')
            continue
        removed += 1
        logger.info(f'Cleaned up expired managed-job API access token '
                    f'{token_id} ({token_name})')
    return removed


def ha_recovery_for_consolidation_mode() -> None:
    """Recovery logic for consolidation mode.

    This should only be called from the managed-job-status-refresh-daemon, due
    so that we have correct ordering recovery -> controller start -> job status
    updates. This also should ensure correct operation during a rolling update.
    """
    # No setup recovery is needed in consolidation mode, as the API server
    # already has all runtime installed. Reset stale outer-generation
    # ownership before any replacement scheduler process can claim work.
    # Refers to sky/templates/kubernetes-ray.yml.j2 for more details.
    stale_owner_count = (
        managed_job_state.reset_stale_jobs_for_current_controller())
    with open(constants.HA_PERSISTENT_RECOVERY_LOG_PATH.format('jobs_'),
              'a',
              encoding='utf-8') as f:
        start = time.time()
        f.write(f'Starting HA recovery at {datetime.now()}\n')
        if stale_owner_count:
            message = (
                f'Reset {stale_owner_count} managed job(s) owned by a stale '
                'outer controller generation.\n')
            logger.info(message.rstrip())
            f.write(message)
        jobs, _ = managed_job_state.get_managed_jobs_with_filters(fields=[
            'job_id', 'controller_pid', 'controller_pid_started_at',
            'controller_instance_id', 'controller_generation', 'schedule_state',
            'status'
        ])
        for job in jobs:
            job_id = job['job_id']
            controller_pid = job['controller_pid']
            controller_pid_started_at = job.get('controller_pid_started_at')

            # In consolidation mode, it is possible that only the API server
            # process is restarted, and the controller process is not. In such
            # case, we don't need to do anything and the controller process will
            # just keep running. However, in most cases, the controller process
            # will also be stopped - either by a pod restart in k8s API server,
            # or by `sky api stop`, which will stop controllers.
            # TODO(cooperc): Make sure we cannot have a controller process
            # running across API server restarts for consistency.
            if controller_pid is not None:
                try:
                    if controller_process_alive(
                            managed_job_state.ControllerPidRecord(
                                pid=controller_pid,
                                started_at=controller_pid_started_at)):
                        message = (f'Controller pid {controller_pid} for '
                                   f'job {job_id} is still running. '
                                   'Skipping recovery.\n')
                        logger.debug(message)
                        f.write(message)
                        continue
                except Exception:  # pylint: disable=broad-except
                    # _controller_process_alive may raise if psutil fails; we
                    # should not crash the recovery logic because of this.
                    message = ('Error checking controller pid '
                               f'{controller_pid} for job {job_id}\n')
                    logger.warning(message, exc_info=True)
                    f.write(message)

            # Controller process is not set or not alive.
            if job['schedule_state'] not in [
                    managed_job_state.ManagedJobScheduleState.DONE,
                    managed_job_state.ManagedJobScheduleState.WAITING,
                    # INACTIVE job may be mid-submission, don't set to WAITING.
                    managed_job_state.ManagedJobScheduleState.INACTIVE,
            ]:
                managed_job_state.reset_job_for_recovery(job_id)
                message = (f'Job {job_id} completed recovery at '
                           f'{datetime.now()}\n')
                logger.info(message)
                f.write(message)
        # Start schedulers only after every stale or dead PID has been reset.
        # Starting them before the scan lets a replacement claim race the
        # recovery writes and can stamp a PID that recovery immediately
        # invalidates.
        scheduler.maybe_start_controllers()
        f.write(f'HA recovery completed at {datetime.now()}\n')
        f.write(f'Total recovery time: {time.time() - start} seconds\n')


async def get_job_status(
        backend: 'backends.CloudVmRayBackend', cluster_name: str,
        job_id: int | None) -> tuple[Optional['job_lib.JobStatus'], str | None]:
    """Check the status of the job running on a managed job cluster.

    It can be None, INIT, RUNNING, SUCCEEDED, FAILED, FAILED_DRIVER,
    FAILED_SETUP or CANCELLED.

    Returns:
        job_status: The status of the job.
        transient_error_reason: None if successful or fatal error; otherwise,
            the detailed reason for the transient error.
    """
    # TODO(zhwu, cooperc): Make this get job status aware of cluster status, so
    # that it can exit retry early if the cluster is down.
    # TODO(luca) make this async
    handle = await asyncio.to_thread(
        global_user_state.get_handle_from_cluster_name, cluster_name)

    def _log_job_status(status: Optional['job_lib.JobStatus']) -> None:
        if status is None:
            logger.info('No job found.')
        else:
            logger.info(f'Job status: {status}')
        logger.info('=' * 34)

    logger.info('=== Checking the job status... ===')

    if managed_job_runtime.is_registered():
        result = await asyncio.to_thread(managed_job_runtime.get_job_status,
                                         handle, cluster_name)
        if result is not None:
            status, _ = result
            _log_job_status(status)
            return result

    if handle is None:
        # This can happen if the cluster was preempted and background status
        # refresh already noticed and cleaned it up.
        logger.info(f'Cluster {cluster_name} not found.')
        return None, None
    assert isinstance(handle, backends.CloudVmRayResourceHandle), handle
    job_ids = None if job_id is None else [job_id]
    try:
        statuses = await asyncio.wait_for(
            asyncio.to_thread(backend.get_job_status,
                              handle,
                              job_ids=job_ids,
                              stream_logs=False),
            timeout=_JOB_STATUS_FETCH_TIMEOUT_SECONDS)
        status = list(statuses.values())[0]
        _log_job_status(status)
        return status, None
    except (exceptions.CommandError, exceptions.CommandFailureException,
            grpc.RpcError, grpc.FutureTimeoutError, ValueError, TypeError,
            asyncio.TimeoutError) as e:
        # Note: Each of these exceptions has some additional conditions to
        # limit how we handle it and whether or not we catch it.
        potential_transient_error_reason = None
        if isinstance(e, exceptions.CommandError):
            returncode = e.returncode
            potential_transient_error_reason = (f'Returncode: {returncode}. '
                                                f'{e.detailed_reason}')
        elif isinstance(e, exceptions.CommandFailureException):
            # Note: this should come after the CommandError handler, as this is
            # the supertype of CommandError
            potential_transient_error_reason = (f'Command {e.failure}. '
                                                f'{e.detailed_reason}')
        elif isinstance(e, grpc.RpcError):
            potential_transient_error_reason = e.details()
        elif isinstance(e, grpc.FutureTimeoutError):
            potential_transient_error_reason = 'grpc timeout'
        elif isinstance(e, asyncio.TimeoutError):
            potential_transient_error_reason = (
                'Job status check timed out after '
                f'{_JOB_STATUS_FETCH_TIMEOUT_SECONDS}s')
        # TODO(cooperc): Gracefully handle these exceptions in the backend.
        elif isinstance(e, ValueError):
            # If the cluster yaml is deleted in the middle of getting the
            # SSH credentials, we could see this. See
            # sky/global_user_state.py get_cluster_yaml_dict.
            if re.search(r'Cluster yaml .* not found', str(e)):
                potential_transient_error_reason = 'Cluster yaml was deleted'
            else:
                raise
        elif isinstance(e, TypeError):
            # We will grab the SSH credentials from the cluster yaml, but if
            # handle.cluster_yaml is None, we will just return an empty dict
            # for the credentials. See
            # backend_utils.ssh_credential_from_yaml. Then, the credentials
            # are passed as kwargs to SSHCommandRunner.__init__ - see
            # cloud_vm_ray_backend.get_command_runners. So we can hit this
            # TypeError if the cluster yaml is removed from the handle right
            # when we pull it before the cluster is fully deleted.
            error_msg_to_check = (
                'SSHCommandRunner.__init__() missing 2 required positional '
                'arguments: \'ssh_user\' and \'ssh_private_key\'')
            if str(e) == error_msg_to_check:
                potential_transient_error_reason = ('SSH credentials were '
                                                    'already cleaned up')
            else:
                raise
        return None, potential_transient_error_reason


def controller_process_alive(record: managed_job_state.ControllerPidRecord,
                             quiet: bool = True) -> bool:
    """Check if the controller process is alive.

    Controller PID records must include ``started_at`` so pid reuse cannot
    resurrect an unrelated process as a live controller.
    """
    if record.started_at is None:
        if not quiet:
            logger.debug(f'Controller process {record.pid} is missing '
                         'started_at; treating it as dead.')
        return False

    try:
        process = psutil.Process(record.pid)
        if process.create_time() != record.started_at:
            if not quiet:
                logger.debug(f'Controller process {record.pid} has started '
                             f'at {record.started_at} but process has '
                             f'started at {process.create_time()}')
            return False

        return process.is_running()

    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess,
            OSError) as e:
        if not quiet:
            logger.debug(f'Controller process {record.pid} is not running: {e}')
        return False


def _controller_is_restarting() -> bool:
    """Whether a controller process is being restarted under us.

    The signal file is created while the controller is recovering from a
    failure (see sky/templates/kubernetes-ray.yml.j2). While it is present,
    update_managed_jobs_statuses must NOT mark jobs FAILED_CONTROLLER -- the
    controller process the job depends on is being restarted, not gone for good.
    """
    return os.path.exists(
        os.path.expanduser(constants.PERSISTENT_RUN_RESTARTING_SIGNAL_FILE))


def _task_has_launch_attempt(task: dict[str, Any]) -> bool:
    """Whether cleanup must treat this task as having launched.

    A later pipeline stage that never left the backlog still has a durable
    ``spot`` row, but it never owned a cluster and should not trigger a best-
    effort teardown. Launch and recovery paths stamp at least one of the
    lifecycle timestamps below and keep it when the task falls back to
    ``PENDING`` during retry backoff, so the marker remains correct on the
    failure path without refetching the full task row.
    """
    return any(
        task.get(field) is not None
        for field in ('submitted_at', 'start_at', 'last_recovered_at'))


def update_managed_jobs_statuses(job_ids: list[int] | None = None):
    """Update managed job status if the controller process failed abnormally.

    Check the status of the controller process. If it is not running, it must
    have exited abnormally, and we should set the job status to
    FAILED_CONTROLLER. `end_at` will be set to the current timestamp for the job
    when above happens, which could be not accurate based on the frequency this
    function is called.

    Note: we expect that job_ids, if provided, refer to nonterminal jobs or
    jobs that have not completed their cleanup (schedule state not DONE).
    """
    # The signal file suggests that the controller is recovering from a
    # failure. See sky/templates/kubernetes-ray.yml.j2 for more details.
    # When restarting the controller processes, we don't want this event to
    # set the job status to FAILED_CONTROLLER.
    # TODO(tian): Change this to restart the controller process. For now we
    # disabled it when recovering because we want to avoid caveats of infinite
    # restart of last controller process that fully occupied the controller VM.
    if _controller_is_restarting():
        return
    current_controller_owner = (
        managed_job_state.get_current_controller_owner())

    def _cleanup_job_clusters(job_id: int, tasks: list[dict[str, Any]],
                              pool: str | None) -> str | None:
        """Clean up clusters for a job. Returns error message if any.

        This function should not throw any exception. If it fails, it will
        capture the error message, and log/return it.

        ``tasks`` is the launch-identity snapshot already fetched by
        ``get_jobs_to_check_status_info``. Reusing it avoids a second task join
        on the failure path and keeps cleanup keyed off ``task_name``, which is
        what the controller uses to name task clusters.
        """
        if pool is not None:
            return None
        cluster_names = []
        for task in tasks:
            if not _task_has_launch_attempt(task):
                continue
            cluster_name = generate_managed_job_cluster_name(
                task['task_name'], job_id)
            if cluster_name is not None:
                cluster_names.append(cluster_name)

        def _terminate_one(cluster_name: str) -> str | None:
            try:
                terminate_cluster(cluster_name)
                return None
            except Exception as e:  # pylint: disable=broad-except
                error_msg = (
                    f'Failed to terminate cluster {cluster_name}: '
                    f'{common_utils.format_exception(e, use_bracket=True)}')
                logger.exception(error_msg, exc_info=e)
                return error_msg

        # Terminate the task clusters in parallel: each task in a JobGroup has
        # a distinct cluster, and a single teardown can take minutes, so a
        # serial walk holds up the whole refresh tick and widens the window in
        # which the batched status snapshot goes stale.
        error_msgs = [
            msg for msg in subprocess_utils.run_in_parallel(
                _terminate_one, cluster_names) if msg is not None
        ]
        if not error_msgs:
            return None
        return '; '.join(error_msgs)

    def _snapshot_kwargs(snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            'schedule_state': snapshot['schedule_state'],
            'controller_pid': snapshot['controller_pid'],
            'controller_pid_started_at': snapshot['controller_pid_started_at'],
            'controller_instance_id': snapshot.get('controller_instance_id'),
            'controller_generation': snapshot.get('controller_generation'),
        }

    def _snapshot_is_unchanged(info: dict[str, Any],
                               fresh_info: dict[str, Any] | None) -> bool:
        return (fresh_info is not None and
                fresh_info['schedule_state'] == info['schedule_state'] and
                fresh_info['controller_pid'] == info['controller_pid'] and
                fresh_info['controller_pid_started_at']
                == info['controller_pid_started_at'] and
                fresh_info.get('controller_instance_id')
                == info.get('controller_instance_id') and
                fresh_info.get('controller_generation')
                == info.get('controller_generation'))

    def _finish_terminal_cleanup(job_id: int, tasks: list[dict[str, Any]],
                                 pool: str | None, snapshot: dict[str,
                                                                  Any]) -> None:
        """Clean a terminal job, then finish its exact durable snapshot."""
        if _controller_is_restarting():
            logger.info(f'Controller restart in progress for terminal job '
                        f'{job_id}; deferring cleanup/finalization to the next '
                        'status update cycle.')
            return
        cleanup_error = _cleanup_job_clusters(job_id, tasks, pool)
        if cleanup_error:
            logger.error(cleanup_error)
            return
        finished = (
            managed_job_state.finish_controller_cleanup_if_current_snapshot(
                job_id, **_snapshot_kwargs(snapshot)))
        if not finished:
            logger.info(f'Job {job_id} changed while terminal cleanup was in '
                        'progress; deferring DONE to the next status update.')

    # Fetch the jobs that need checking together with the small per-job fields
    # the loop consumes. This keeps the refresh tick on a single slim query
    # instead of a filtered job-id query followed by a second detail query.
    jobs_info = managed_job_state.get_jobs_to_check_status_info(job_ids)
    if not jobs_info:
        # The given jobs are already terminal, or if job_ids is None, there
        # are no jobs that need to be checked.
        return

    for job_id, info in jobs_info.items():
        tasks = info['tasks']
        # Note: controller_pid and schedule_state are in the job_info table
        # which is joined to the spot table, so all tasks with the same job_id
        # share these columns. get_jobs_to_check_status_info returns them once
        # per job.
        schedule_state = info['schedule_state']

        # Handle jobs with schedule state (non-legacy jobs):
        pid = info['controller_pid']
        pid_started_at = info['controller_pid_started_at']
        snapshot_all_tasks_terminal = all(
            task['status'].is_terminal() for task in tasks)
        if snapshot_all_tasks_terminal:
            fresh_info = managed_job_state.get_job_status_check_state(job_id)
            if not _snapshot_is_unchanged(info, fresh_info):
                logger.info(f'Job {job_id} changed since the terminal status '
                            'snapshot; deferring cleanup.')
                continue
            assert fresh_info is not None
            if not fresh_info['all_tasks_terminal']:
                logger.info(f'Job {job_id} is no longer terminal; deferring '
                            'cleanup.')
                continue
            _finish_terminal_cleanup(job_id, tasks, info['pool'], fresh_info)
            continue
        if current_controller_owner is not None:
            recorded_owner = (info.get('controller_instance_id'),
                              info.get('controller_generation'))
            pure_backlog = (pid is None and schedule_state in [
                managed_job_state.ManagedJobScheduleState.INACTIVE,
                managed_job_state.ManagedJobScheduleState.WAITING,
            ])
            if not pure_backlog and recorded_owner != current_controller_owner:
                reset = managed_job_state.reset_job_for_recovery_if_stale(
                    job_id, current_controller_owner)
                recovery_result = ('resetting it for recovery' if reset else
                                   'its owner changed before recovery')
                logger.info(f'Job {job_id} belongs to stale outer controller '
                            f'{recorded_owner}; {recovery_result}.')
                continue
        if schedule_state == managed_job_state.ManagedJobScheduleState.DONE:
            # There are two cases where we could get a job that is DONE.
            # 1. At snapshot time (get_jobs_to_check_status_info), the job was
            #    not yet DONE, but since then it has hit a terminal status,
            #    marked itself done, and exited. This is fine.
            # 2. The job is DONE, but in a non-terminal status. This is
            #    unexpected. For instance, the task status is RUNNING, but the
            #    job schedule_state is DONE.
            if all(task['status'].is_terminal() for task in tasks):
                # Turns out this job is fine, even though it got pulled by
                # get_jobs_to_check_status_info. Probably case #1 above.
                continue

            logger.error(f'Job {job_id} has DONE schedule state, but some '
                         f'tasks are not terminal. Task statuses: '
                         f'{", ".join(task["status"].value for task in tasks)}')
            failure_reason = ('Inconsistent internal job state. This is a bug.')
        elif pid is None:
            # Non-legacy job and controller process has not yet started.
            if (schedule_state in [
                    managed_job_state.ManagedJobScheduleState.INACTIVE,
                    managed_job_state.ManagedJobScheduleState.WAITING,
            ]):
                # It is expected that the controller hasn't been started yet.
                # The controller process has not run, so there is no controller
                # status to read; skip the per-job filelock + SQLite read in
                # job_lib.get_status(). This is the common backlog state under a
                # large submission fan-out, so avoiding it per job per refresh
                # tick removes a lock acquisition + DB query for every pending
                # job.
                continue
            controller_status = job_lib.get_status(job_id)
            if controller_status == job_lib.JobStatus.FAILED_SETUP:
                # We should fail the case where the controller status is
                # FAILED_SETUP, as it is due to the failure of dependency setup
                # on the controller.
                # TODO(cooperc): We should also handle the case where controller
                # status is FAILED_DRIVER or FAILED.
                logger.error('Failed to setup the cloud dependencies for '
                             'the managed job.')
            elif (schedule_state ==
                  managed_job_state.ManagedJobScheduleState.LAUNCHING):
                # This is unlikely but technically possible. There's a brief
                # period between marking job as scheduled (LAUNCHING) and
                # actually launching the controller process and writing the pid
                # back to the table.
                # TODO(cooperc): Find a way to detect if we get stuck in this
                # state.
                logger.info(f'Job {job_id} is in {schedule_state.value} state, '
                            'but controller process hasn\'t started yet.')
                continue

            logger.error(f'Expected to find a controller pid for state '
                         f'{schedule_state.value} but found none.')
            failure_reason = f'No controller pid set for {schedule_state.value}'
        else:
            logger.debug(f'Checking controller pid {pid}')
            if controller_process_alive(
                    managed_job_state.ControllerPidRecord(
                        pid=pid, started_at=pid_started_at)):
                # The controller is still running, so this job is fine.
                continue
            logger.error(f'Controller process for {job_id} seems to be dead.')
            failure_reason = 'Controller process is dead'

        # At this point, either pid is None or process is dead.

        # The judgment above was made from the batched snapshot taken before
        # the loop, which can be minutes stale by now (each earlier iteration
        # that reaches the destructive path synchronously terminates a
        # cluster). In that window the job may have been reset for recovery
        # (schedule_state=WAITING, pid cleared; see reset_jobs_for_recovery)
        # or re-claimed by a new controller process. Only act if a fresh read
        # confirms the exact values the judgment was based on; otherwise defer
        # to the next status-update cycle, which will re-judge the job from
        # fresh state.
        fresh_info = managed_job_state.get_job_status_check_state(job_id)
        if (fresh_info is not None and fresh_info['schedule_state']
                == managed_job_state.ManagedJobScheduleState.DONE):
            # The controller marked the job done and exited between the batched
            # snapshot and the destructive path. This is fine.
            continue
        if not _snapshot_is_unchanged(info, fresh_info):
            logger.info(f'Job {job_id} schedule state or controller pid '
                        'changed since the status snapshot was taken; '
                        'deferring to the next status update cycle.')
            continue
        assert fresh_info is not None

        # The controller can also die AFTER all tasks are already terminal but
        # BEFORE it flips schedule_state to DONE, e.g. during log streaming or
        # cluster teardown. Preserve the terminal task outcome and only
        # finalize scheduler state; rewriting the job to FAILED_CONTROLLER here
        # would clobber a real SUCCEEDED/FAILED result with a cleanup crash.
        if fresh_info['all_tasks_terminal']:
            logger.info(f'Job {job_id} already reached terminal task status; '
                        'finalizing schedule state without rewriting the job '
                        'to FAILED_CONTROLLER.')
            _finish_terminal_cleanup(job_id, tasks, info['pool'], fresh_info)
            continue

        # The controller process for this managed job is not running: it must
        # have exited abnormally, and we should set the job status to
        # FAILED_CONTROLLER.
        logger.error(f'Controller process for job {job_id} has exited '
                     'abnormally. Setting the job status to FAILED_CONTROLLER.')

        # Re-check the restart signal right before the destructive action. The
        # top-of-function check is a stale snapshot: marking many jobs takes
        # time, and a controller restart (which creates the signal file) can
        # begin in that window. Acting on the stale snapshot would terminate
        # this job's cluster and mark it FAILED_CONTROLLER while its controller
        # is being restarted under it -- losing a job that would otherwise
        # resume.
        if _controller_is_restarting():
            logger.info(
                f'Controller restart in progress; deferring FAILED_CONTROLLER '
                f'for job {job_id} (will re-check on the next status update).')
            continue

        failure_message = (
            f'Controller process has exited abnormally ({failure_reason}). '
            f'For more details, run: sky jobs logs --controller {job_id}')
        terminalized = (
            managed_job_state.set_failed_controller_if_current_snapshot(
                job_id,
                **_snapshot_kwargs(fresh_info),
                failure_reason=failure_message))
        if not terminalized:
            logger.info(f'Job {job_id} changed before FAILED_CONTROLLER could '
                        'be committed; deferring cleanup.')
            continue

        # Terminal task state is the durable no-recovery decision. Provider
        # cleanup follows it so a handoff can retry teardown but cannot relaunch
        # the workload underneath an old generation's destructive request.
        logger.info(failure_message)
        _finish_terminal_cleanup(job_id, tasks, info['pool'], fresh_info)


def get_job_timestamp(backend: 'backends.CloudVmRayBackend',
                      handle: 'backends.CloudVmRayResourceHandle',
                      job_id: int | None, get_end_time: bool) -> float:
    """Get the submitted/ended time using one cluster-handle snapshot."""
    if handle.is_grpc_enabled_with_flag:
        try:
            if get_end_time:
                end_ts_request = jobsv1_pb2.GetJobEndedTimestampRequest(
                    job_id=job_id)
                end_ts_response = backend_utils.invoke_skylet_with_retries(
                    lambda: cloud_vm_ray_backend.SkyletClient(
                        handle.get_grpc_channel()).get_job_ended_timestamp(
                            end_ts_request))
                return end_ts_response.timestamp
            else:
                submit_ts_request = jobsv1_pb2.GetJobSubmittedTimestampRequest(
                    job_id=job_id)
                submit_ts_response = backend_utils.invoke_skylet_with_retries(
                    lambda: cloud_vm_ray_backend.SkyletClient(
                        handle.get_grpc_channel()).get_job_submitted_timestamp(
                            submit_ts_request))
                return submit_ts_response.timestamp
        except exceptions.SkyletMethodNotImplementedError:
            pass

    code = (job_lib.JobLibCodeGen.get_job_submitted_or_ended_timestamp_payload(
        job_id=job_id, get_ended_time=get_end_time))
    returncode, stdout, stderr = backend.run_on_head(handle,
                                                     code,
                                                     stream_logs=False,
                                                     require_outputs=True)
    subprocess_utils.handle_returncode(returncode, code,
                                       'Failed to get job time.',
                                       stdout + stderr)
    stdout = message_utils.decode_payload(stdout)
    return float(stdout)


def try_to_get_job_end_time(backend: 'backends.CloudVmRayBackend',
                            cluster_name: str, job_id: int | None) -> float:
    """Try to get the end time of the job.

    If the job is preempted or we can't connect to the instance for whatever
    reason, fall back to the current time.
    """
    handle = global_user_state.get_handle_from_cluster_name(cluster_name)
    if managed_job_runtime.is_registered():
        runtime_ended_at = managed_job_runtime.get_job_ended_at(
            handle, cluster_name)
        if runtime_ended_at is not None:
            return runtime_ended_at
    try:
        assert handle is not None, (
            f'handle for cluster {cluster_name!r} should not be None')
        return get_job_timestamp(backend,
                                 handle,
                                 job_id=job_id,
                                 get_end_time=True)
    except (exceptions.CommandError, grpc.RpcError,
            grpc.FutureTimeoutError) as e:
        if isinstance(e, exceptions.CommandError) and e.returncode == 255 or \
                (isinstance(e, grpc.RpcError) and e.code() in [
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                ]) or isinstance(e, grpc.FutureTimeoutError):
            # Failed to connect - probably the instance was preempted since the
            # job completed. We shouldn't crash here, so just log and use the
            # current time.
            logger.info(f'Failed to connect to the instance {cluster_name} '
                        'since the job completed. Assuming the instance '
                        'was preempted.')
            return time.time()
        else:
            raise


def event_callback_func(
        job_id: int, task_id: int | None,
        task: Optional['sky.Task']) -> managed_job_state.AsyncCallbackType:
    """Run event callback for the task."""

    def callback_func(status: str):
        event_callback = task.event_callback if task else None
        if event_callback is None or task is None:
            return
        event_callback = event_callback.strip()
        pool, cluster_name = (
            managed_job_state.get_pool_and_current_cluster_name(job_id))
        if pool is None:
            cluster_name = generate_managed_job_cluster_name(
                task.name, job_id) if task.name else None
        logger.info(f'=== START: event callback for {status!r} ===')
        log_path = os.path.join(constants.SKY_LOGS_DIRECTORY,
                                'managed_job_event',
                                f'jobs-callback-{job_id}-{task_id}.log')
        env_vars = task.envs.copy() if task.envs else {}
        env_vars.update(
            dict(
                SKYPILOT_TASK_ID=str(
                    task.envs.get(constants.TASK_ID_ENV_VAR, 'N.A.')),
                SKYPILOT_TASK_IDS=str(
                    task.envs.get(constants.TASK_ID_LIST_ENV_VAR, 'N.A.')),
                TASK_ID=str(task_id),
                JOB_ID=str(job_id),
                JOB_STATUS=status,
                CLUSTER_NAME=cluster_name or '',
                TASK_NAME=task.name or '',
                # TODO(MaoZiming): Future event type Job or Spot.
                EVENT_TYPE='Spot'))
        result = log_lib.run_bash_command_with_log(bash_command=event_callback,
                                                   log_path=log_path,
                                                   env_vars=env_vars)
        logger.info(
            f'Bash:{event_callback},log_path:{log_path},result:{result}')
        logger.info(f'=== END: event callback for {status!r} ===')

    async def async_callback_func(status: str):
        return await asyncio.to_thread(callback_func, status)

    return async_callback_func


# ======== user functions ========


def collect_debug_dump_manifest(job_ids: list[int]) -> dict[str, Any]:
    """Collect a debug dump manifest from the controller.

    This function runs ON the controller via CodeGen/SSH. It gathers small
    DB-derived data inline (as JSON strings) and returns remote file paths
    for large log files (to be rsynced by the caller).

    Returns:
        Dict with:
          'inline_data': list of {'relative_path': str, 'content': str}
          'file_paths': list of {'remote_path': str, 'relative_path': str}
          'errors': list of {'component': str, 'resource': str, 'error': str}
    """
    return managed_job_debug_dump.collect_debug_dump_manifest(
        job_ids,
        collect_job_debug_manifest_func=_collect_job_debug_manifest,
        collect_cluster_debug_manifest_func=_collect_cluster_debug_manifest,
        collect_controller_system_log_paths_func=(
            _collect_controller_system_log_paths),
    )


def _collect_job_debug_manifest(
    job_id: int,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]],
           str | None, set[str]]:
    return managed_job_debug_dump.collect_job_debug_manifest(
        job_id,
        jobs_controller_logs_dir=(
            managed_job_constants.JOBS_CONTROLLER_LOGS_DIR),
        managed_job_state=managed_job_state,
        debug_dump_helpers=debug_dump_helpers,
        generate_cluster_name=generate_managed_job_cluster_name,
    )


def _collect_cluster_debug_manifest(cluster_name: str, job_prefix: str,
                                    inline_data: list[dict[str, str]],
                                    errors: list[dict[str, str]]) -> None:
    managed_job_debug_dump.collect_cluster_debug_manifest(
        cluster_name,
        job_prefix,
        inline_data,
        errors,
        global_user_state=global_user_state,
        debug_dump_helpers=debug_dump_helpers,
    )


def _collect_controller_system_log_paths(file_paths: list[dict[str, str]],
                                         errors: list[dict[str, str]],
                                         relevant_uuids: set[str]) -> None:
    managed_job_debug_dump.collect_controller_system_log_paths(
        file_paths,
        errors,
        relevant_uuids,
        jobs_controller_logs_dir=(
            managed_job_constants.JOBS_CONTROLLER_LOGS_DIR),
    )


def cancel_jobs_by_id(job_ids: list[int] | None,
                      all_users: bool = False,
                      current_workspace: str | None = None,
                      user_hash: str | None = None,
                      graceful: bool = False,
                      graceful_timeout: int | None = None) -> str:
    """Cancel jobs by id.

    If job_ids is None, cancel all jobs.
    """
    if job_ids is None:
        job_ids = managed_job_state.get_nonterminal_job_ids_by_name(
            None, user_hash, all_users)
    job_ids = list(dict.fromkeys(job_ids))
    if not job_ids:
        return 'No job to cancel.'
    if current_workspace is None:
        current_workspace = constants.SKYPILOT_DEFAULT_WORKSPACE

    cancelled_job_ids: list[int] = []
    wrong_workspace_job_ids: list[int] = []
    jobs_to_refresh: list[int] = []
    initial_states = managed_job_state.get_job_cancellation_states(job_ids)
    for job_id in job_ids:
        snapshot = initial_states.get(job_id)
        if snapshot is None:
            logger.info(f'Job {job_id} not found. Skipped.')
            continue
        if snapshot.status.is_terminal():
            logger.info(f'Job {job_id} is already in terminal state '
                        f'{snapshot.status.value}. Skipped.')
            continue
        if snapshot.workspace != current_workspace:
            wrong_workspace_job_ids.append(job_id)
            continue
        if snapshot.status == managed_job_state.ManagedJobStatus.PENDING:
            # the "if PENDING" is a short circuit, this will be atomic.
            cancelled = managed_job_state.set_pending_cancelled(job_id)
            if cancelled:
                cancelled_job_ids.append(job_id)
                continue

        jobs_to_refresh.append(job_id)

    if jobs_to_refresh:
        # One batched refresh sweep for every job that needs it, instead of a
        # sweep per job: all jobs are judged against a single status snapshot,
        # and a cancel of N live jobs issues one refresh query instead of N.
        update_managed_jobs_statuses(jobs_to_refresh)
    fresh_states = managed_job_state.get_job_cancellation_states(
        jobs_to_refresh)
    for job_id in jobs_to_refresh:
        snapshot = fresh_states.get(job_id)
        if snapshot is None:
            logger.info(
                f'Job {job_id} not found after status refresh. Skipped.')
            continue
        if snapshot.status.is_terminal():
            logger.info(f'Job {job_id} reached terminal state '
                        f'{snapshot.status.value} during status refresh. '
                        'Skipped.')
            continue
        if snapshot.workspace != current_workspace:
            wrong_workspace_job_ids.append(job_id)
            continue
        if snapshot.status == managed_job_state.ManagedJobStatus.PENDING:
            # A refresh can move a stale live snapshot back into the
            # pre-launch backlog. Reuse the same atomic finalizer here before
            # falling back to controller-signal delivery.
            cancelled = managed_job_state.set_pending_cancelled(job_id)
            if cancelled:
                cancelled_job_ids.append(job_id)
                continue

        try:
            signal_file = pathlib.Path(
                managed_job_constants.CONSOLIDATED_SIGNAL_PATH, f'{job_id}')
            with filelock.FileLock(str(signal_file) + '.lock'):
                if graceful:
                    content = _JOBS_GRACEFUL_CANCEL_SIGNAL
                    if graceful_timeout is not None:
                        content += f':{graceful_timeout}'
                    signal_file.write_text(content, encoding='utf-8')
                else:
                    signal_file.touch()
        except OSError as e:
            logger.error(f'Failed to cancel job {job_id}: {e}')
            # Don't add it to the to be cancelled job ids
            continue

        cancelled_job_ids.append(job_id)

    wrong_workspace_job_str = ''
    if wrong_workspace_job_ids:
        plural = 's' if len(wrong_workspace_job_ids) > 1 else ''
        plural_verb = 'are' if len(wrong_workspace_job_ids) > 1 else 'is'
        wrong_workspace_job_str = (
            f' Job{plural} with ID{plural}'
            f' {", ".join(map(str, wrong_workspace_job_ids))} '
            f'{plural_verb} skipped as they are not in the active workspace '
            f'{current_workspace!r}. Check the workspace of the job with: '
            f'sky jobs queue')

    if not cancelled_job_ids:
        return f'No job to cancel.{wrong_workspace_job_str}'
    identity_str = f'Job with ID {cancelled_job_ids[0]} is'
    if len(cancelled_job_ids) > 1:
        cancelled_job_ids_str = ', '.join(map(str, cancelled_job_ids))
        identity_str = f'Jobs with IDs {cancelled_job_ids_str} are'

    msg = f'{identity_str} scheduled to be cancelled.{wrong_workspace_job_str}'
    return msg


def cancel_job_by_name(job_name: str,
                       current_workspace: str | None = None,
                       graceful: bool = False,
                       graceful_timeout: int | None = None) -> str:
    """Cancel a job by name."""
    job_ids = managed_job_state.get_nonterminal_job_ids_by_name(job_name)
    if not job_ids:
        return f'No running job found with name {job_name!r}.'
    if len(job_ids) > 1:
        return (f'{colorama.Fore.RED}Multiple running jobs found '
                f'with name {job_name!r}.\n'
                f'Job IDs: {job_ids}{colorama.Style.RESET_ALL}')
    msg = cancel_jobs_by_id(job_ids,
                            current_workspace=current_workspace,
                            graceful=graceful,
                            graceful_timeout=graceful_timeout)
    return f'{job_name!r} {msg}'


def cancel_jobs_by_pool(pool_name: str,
                        current_workspace: str | None = None) -> str:
    """Cancel all jobs in a pool."""
    job_ids = managed_job_state.get_nonterminal_job_ids_by_pool(pool_name)
    if not job_ids:
        return f'No running job found in pool {pool_name!r}.'
    return cancel_jobs_by_id(job_ids, current_workspace=current_workspace)


def cancel_managed_jobs(
    *,
    name: str | None = None,
    job_ids: list[int] | None = None,
    pool: str | None = None,
    all: bool = False,  # pylint: disable=redefined-builtin
    all_users: bool = False,
    graceful: bool = False,
    graceful_timeout: int | None = None,
    current_workspace: str | None = None,
    user_hash: str | None = None,
) -> str:
    """Dispatch to the correct cancel variant based on selector args.

    One of ``job_ids``/``name``/``pool``/``all``/``all_users`` should be set.
    Precedence:

      - ``all_users`` or ``all`` or ``job_ids`` -> ``cancel_jobs_by_id``
      - ``name`` -> ``cancel_job_by_name``
      - ``pool`` -> ``cancel_jobs_by_pool``

    Single source of truth for the dispatch precedence. Direct callers
    (including plugins registering a custom ``ManagedJobRunner``) invoke
    this function; the codegen path
    (``ManagedJobCodeGen.cancel_managed_jobs``) also references it by
    name on controllers running ``MANAGED_JOBS_VERSION >= 19``.
    """
    if all_users or all or job_ids:
        return cancel_jobs_by_id(
            job_ids,
            all_users=all_users,
            current_workspace=current_workspace,
            user_hash=user_hash,
            graceful=graceful,
            graceful_timeout=graceful_timeout,
        )
    if name is not None:
        return cancel_job_by_name(
            name,
            current_workspace=current_workspace,
            graceful=graceful,
            graceful_timeout=graceful_timeout,
        )
    assert pool is not None, (job_ids, name, pool, all)
    return cancel_jobs_by_pool(
        pool,
        current_workspace=current_workspace,
    )


def controller_log_file_for_job(job_id: int,
                                create_if_not_exists: bool = False) -> str:
    log_dir = os.path.expanduser(managed_job_constants.JOBS_CONTROLLER_LOGS_DIR)
    if create_if_not_exists:
        os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f'{job_id}.log')


def read_provision_status_from_log(
        log_path: str, pos: int,
        current_msg: str | None) -> tuple[int, str | None]:
    """Reads rich-status spinner messages relayed into a controller log.

    The jobs controller relays the inner cluster-launch rich-status payloads
    into its per-job log (see ``recovery_strategy._launch``'s
    ``relay_rich_status=True``). This decodes any payloads appended since
    ``pos`` and returns the new read position together with the latest
    provisioning spinner message, so ``sky jobs launch`` / ``sky jobs logs``
    can show the same provisioning progress (e.g. "Preparing SkyPilot runtime
    (1/3)") that ``sky launch`` displays.

    Args:
        log_path: Path to the jobs controller log for the job.
        pos: Byte/character offset to resume reading from (0 on first call).
        current_msg: The previously returned spinner message.

    Returns:
        A tuple ``(new_pos, latest_msg)``. ``latest_msg`` is ``None`` if
        provisioning has not emitted a spinner yet, or if it has finished
        (an EXIT control clears the message).
    """
    msg = current_msg
    try:
        # If the log was truncated or recreated (e.g. controller recovery or a
        # job retry), the saved offset can be past the new EOF; restart from the
        # beginning so following doesn't get stuck reading nothing.
        if os.path.exists(log_path) and pos > os.path.getsize(log_path):
            pos = 0
            msg = None
        with open(log_path, encoding='utf-8') as f:
            f.seek(pos)
            while True:
                line_start = f.tell()
                line = f.readline()
                if line == '':
                    # EOF.
                    break
                if not line.endswith('\n'):
                    # Partial line still being written; re-read it next time.
                    f.seek(line_start)
                    break
                pos = f.tell()
                is_payload, decoded = message_utils.decode_payload(
                    line, raise_for_mismatch=False)
                if not is_payload:
                    continue
                control, encoded_status = rich_utils.Control.decode(decoded)
                if control in (rich_utils.Control.INIT,
                               rich_utils.Control.UPDATE):
                    # INIT/UPDATE carry the live spinner text.
                    msg = encoded_status
                elif control == rich_utils.Control.EXIT:
                    # The spinner is done.
                    msg = None
                # START/STOP only toggle the spinner's visibility and carry the
                # original (possibly stale) init message rather than the live
                # one: entering a nested status emits UPDATE(nested) then
                # START(original), so updating `msg` on START would revert the
                # headline to the stale text. Leave `msg` unchanged for both.
    except (OSError, ValueError):
        # Best-effort: the log may not exist yet (FileNotFoundError) or be
        # mid-write; never let log following break job-log streaming.
        pass
    return pos, msg


def _is_relayed_status_payload_line(line: str) -> bool:
    """Whether a controller-log line is a relayed rich-status payload.

    With ``relay_rich_status=True``, the jobs controller writes the inner
    cluster launch's encoded rich-status payloads into its per-job log to drive
    the provisioning spinner (see ``read_provision_status_from_log``). These
    encoded ``<sky-payload>`` lines are control-plane only and must be hidden
    from the human-readable ``sky jobs logs --controller`` output.
    """
    is_payload, _ = message_utils.decode_payload(line, raise_for_mismatch=False)
    return is_payload


def _provision_status_headline(provision_msg: str) -> str | None:
    """Returns the blue headline of a provisioning spinner message.

    Provisioning messages from the cluster launch are built by
    ``ux_utils.spinner_message`` and look like
    ``[bold cyan]Preparing SkyPilot runtime (1/3)[/]  <dim log hint>``, where
    the trailing hint is colored with raw ANSI (colorama) codes rather than
    rich markup -- so the message does *not* end at the headline's ``[/]``. We
    keep only the ``[bold cyan]...[/]`` headline and drop the trailing hint, so
    the caller can show it as a secondary detail under the "Waiting for task to
    start" line. Returns ``None`` when the message has no ``[bold cyan]``
    headline, so the caller can show nothing rather than a raw/unstyled message.
    """
    open_tag = '[bold cyan]'
    start = provision_msg.find(open_tag)
    if start == -1:
        return None
    start += len(open_tag)
    # Walk the rich-markup tags after the opening tag, tracking nesting depth,
    # and stop at the ``[/]`` that closes this ``[bold cyan]``. This keeps any
    # nested markup (e.g. ``[bold]X[/]``) inside the headline intact -- instead
    # of truncating at the first ``[/]`` -- and ignores the trailing log hint
    # (which is ANSI-colored and contains no rich tags).
    depth = 1
    for match in re.finditer(r'\[[^\]]*\]', provision_msg[start:]):
        if match.group(0).startswith('[/'):
            depth -= 1
            if depth == 0:
                return provision_msg[start:start + match.start()]
        else:
            depth += 1
    return None


def _should_keep_logging(status: managed_job_state.ManagedJobStatus) -> bool:
    # If we see CANCELLING, just exit - we could miss some job logs but the
    # job will be terminated momentarily anyway so we don't really care.
    return (not status.is_terminal() and
            status != managed_job_state.ManagedJobStatus.CANCELLING)


def _wait_for_next_task(
        job_id: int,
        current_task_id: int) -> managed_job_state.JobLogStreamSnapshot:
    """Wait for and return the next task's log-target snapshot."""
    while True:
        snapshot = managed_job_state.get_latest_log_stream_snapshot(job_id)
        assert snapshot.status is not None, (job_id, snapshot)
        assert snapshot.task_id is not None, (job_id, snapshot)
        if (snapshot.task_id != current_task_id or
                not _should_keep_logging(snapshot.status)):
            return snapshot
        _sleep_log_follow_wait(JOB_STATUS_CHECK_GAP_SECONDS)


def _wait_for_initial_log_stream_snapshot(
    get_snapshot: typing.Callable[[], managed_job_state.JobLogStreamSnapshot]
) -> managed_job_state.JobLogStreamSnapshot:
    """Wait until one log-target snapshot exposes a concrete lifecycle."""
    while True:
        snapshot = get_snapshot()
        if snapshot.status is not None:
            return snapshot
        _sleep_log_follow_wait(1)


def stream_logs_by_id(job_id: int,
                      follow: bool = True,
                      tail: int | None = None,
                      tail_offset: int | None = None,
                      task: str | int | None = None) -> tuple[str, int]:
    """Stream logs by job id.

    Args:
        job_id: The job ID to stream logs for.
        follow: Whether to follow the logs.
        tail: Number of lines to tail from the end of the log file.
        tail_offset: Skip the last ``tail_offset`` lines before applying
            ``tail``. Used by the dashboard live-tail UI to fetch a window
            of older history without re-reading the whole file.
        task: Task identifier to view logs for a specific task in a JobGroup.
            If an int, it is treated as a task ID. If a str, it is treated as
            a task name. If None, logs for all tasks are shown.

    Returns:
        A tuple containing the log message and an exit code based on success or
        failure of the job. 0 if success, 100 if the job failed.
        See exceptions.JobExitCode for possible exit codes.
    """

    # Start a background watchdog thread that detects when the kubectl
    # exec connection has been dropped (client disconnect). On Kubernetes,
    # kubectl exec -i does not allocate a PTY, so no SIGHUP is sent when
    # the connection drops. The only signal is that stdin reaches EOF
    # (the kubelet closes the stdin pipe). This thread monitors stdin and
    # terminates the process when disconnection is detected, preventing
    # leaked stream_logs processes on the controller. Changing the exec call to
    # also include -t does not result in the kubelet sending a SIGHUP to the
    # remote end of the connection.
    #
    # The API server now passes stdin=subprocess.PIPE (instead of
    # DEVNULL) to kubectl exec -i, so stdin on the controller is a live
    # pipe that only reaches EOF when the connection actually drops.
    #
    # For SSH controllers, stdin is a PTY (from ssh -tt), so SIGHUP
    # handles cleanup natively. For consolidation mode or other local
    # invocations, stdin may be /dev/null or already closed (EOF). We
    # check at startup: if stdin is already at EOF, we skip stdin
    # monitoring entirely to avoid false positives. Only a live stdin
    # (not yet at EOF) is worth monitoring this is the case for
    # kubectl exec -i with stdin=subprocess.PIPE.
    check_stdin_eof = False
    try:
        readable, _, _ = select.select([sys.stdin], [], [], 0)
        if readable:
            # stdin is immediately readable check if it's already EOF
            data = os.read(sys.stdin.fileno(), 1)
            if data:
                # Got actual data (unexpected but harmless); stdin is live
                check_stdin_eof = True
            # else: EOF at startup, don't monitor
        else:
            # stdin is not immediately readable it's a live pipe/TTY
            # waiting for input, meaning we have a real connection
            check_stdin_eof = True
    except (ValueError, OSError):
        # stdin is already closed or invalid — not useful for monitoring
        pass

    def _orphan_watchdog() -> None:
        """Background thread that monitors for connection drop."""
        initial_parent_pid = os.getppid()
        while True:
            time.sleep(5)
            # Check 1: Parent PID changed (reparented to init/subreaper)
            if os.getppid() != initial_parent_pid:
                logger.info('Parent process died, terminating.')
                os.kill(os.getpid(), signal.SIGTERM)
                return
            # Check 2: stdin EOF (kubectl exec -i connection dropped).
            # Only checked when stdin is a pipe (Kubernetes), not a TTY
            # (SSH). With SSH -tt, the PTY delivers SIGHUP on disconnect,
            # so this check is unnecessary and could cause false positives.
            if not check_stdin_eof:
                continue
            try:
                readable, _, _ = select.select([sys.stdin], [], [], 0)
                if readable:
                    data = os.read(sys.stdin.fileno(), 1)
                    if not data:
                        logger.info('stdin EOF detected (connection dropped), '
                                    'terminating.')
                        os.kill(os.getpid(), signal.SIGTERM)
                        return
            except (ValueError, OSError):
                logger.info('stdin closed, terminating.')
                os.kill(os.getpid(), signal.SIGTERM)
                return

    watchdog = threading.Thread(target=_orphan_watchdog, daemon=True)
    watchdog.start()

    def matches_task_filter(task_id: int, task_name: str,
                            task_filter: str | int | None) -> bool:
        """Check if a task matches the task filter.

        If task_filter is an int, it is matched against task_id.
        If task_filter is a str, it is matched against task_name.
        """
        if task_filter is None:
            return True
        if isinstance(task_filter, int):
            return task_id == task_filter
        # task_filter is a str, match by task name
        return task_name == task_filter

    msg = _JOB_WAITING_STATUS_MESSAGE.format(status_str='',
                                             provision_str='',
                                             job_id=job_id)
    status_display = rich_utils.safe_status(msg)
    task_info: list[tuple[int, str, managed_job_state.ManagedJobStatus, str,
                          float | None]] | None = None
    if task is not None:
        task_info = managed_job_state.get_all_task_ids_names_statuses_logs(
            job_id)
        num_tasks = len(task_info)
    else:
        num_tasks = managed_job_state.get_num_tasks(job_id)

    # Check if job exists - if num_tasks is 0, the job doesn't exist
    if num_tasks == 0:
        return (f'Job {job_id} not found.', exceptions.JobExitCode.NOT_FOUND)

    # Resolve task filter to a specific task_id if provided
    # This is used for running jobs to stream logs from the correct task
    filtered_task_id: int | None = None
    if task is not None:
        assert task_info is not None, task
        for t_id, t_name, _, _, _ in task_info:
            if matches_task_filter(t_id, t_name, task):
                filtered_task_id = t_id
                break
        if filtered_task_id is None:
            valid_range = f'0-{num_tasks - 1}' if num_tasks > 1 else '0'
            return (f'No task found matching {task!r} in job {job_id}. '
                    f'Valid task IDs are {valid_range}.',
                    exceptions.JobExitCode.NOT_FOUND)

    def get_stream_target_snapshot() -> managed_job_state.JobLogStreamSnapshot:
        if filtered_task_id is None:
            return managed_job_state.get_latest_log_stream_snapshot(job_id)
        return managed_job_state.get_task_log_stream_snapshot(
            job_id, filtered_task_id)

    # Follow the jobs controller log during provisioning so the user sees the
    # same spinner messages that `sky launch` shows. The controller relays the
    # inner cluster-launch rich-status payloads into its per-job log (see
    # recovery_strategy._launch's relay_rich_status=True); here we decode them
    # to drive the single status spinner.
    controller_log_path = controller_log_file_for_job(job_id)
    provision_pos = 0
    provision_msg: str | None = None

    def _latest_provision_status_msg() -> str | None:
        nonlocal provision_pos, provision_msg
        provision_pos, provision_msg = read_provision_status_from_log(
            controller_log_path, provision_pos, provision_msg)
        return provision_msg

    with status_display:
        prev_msg = msg
        initial_snapshot = (
            _wait_for_initial_log_stream_snapshot(get_stream_target_snapshot))
        snapshot: managed_job_state.JobLogStreamSnapshot | None = (
            initial_snapshot)
        managed_job_status = initial_snapshot.status
        managed_job_status: managed_job_state.ManagedJobStatus | None = (
            managed_job_status)
        assert managed_job_status is not None, job_id
        task_id: int | None = None
        pool: str | None = None
        cluster_name: str | None = None
        job_id_to_tail: int | None = None
        task_name: str | None = None

        # Show hint about per-task filtering when there are multiple tasks
        if num_tasks > 1 and task is None:
            print(f'{colorama.Fore.CYAN}Hint: This job has {num_tasks} tasks. '
                  f'Use \'sky jobs logs {job_id} TASK\' to view logs for a '
                  f'specific task (TASK can be task ID or name).'
                  f'{colorama.Style.RESET_ALL}')

        if not _should_keep_logging(managed_job_status):
            job_msg = ''
            if managed_job_status.is_failed():
                job_msg = ('\nFailure reason: '
                           f'{managed_job_state.get_failure_reason(job_id)}')
            log_file_ever_existed = False
            if filtered_task_id is not None:
                terminal_task_row = managed_job_state.get_task_id_name_status_log(
                    job_id, filtered_task_id)
                if terminal_task_row is None:
                    valid_range = f'0-{num_tasks - 1}' if num_tasks > 1 else '0'
                    return (f'No task found matching {task!r} in job {job_id}. '
                            f'Valid task IDs are {valid_range}.',
                            exceptions.JobExitCode.NOT_FOUND)
                terminal_task_info = [terminal_task_row]
            else:
                terminal_task_info = (
                    managed_job_state.get_all_task_ids_names_statuses_logs(
                        job_id))
                assert terminal_task_info is not None, job_id
            num_tasks = len(terminal_task_info)
            for (task_id, task_name, task_status, log_file,
                 logs_cleaned_at) in terminal_task_info:
                if log_file:
                    log_file_ever_existed = True
                    if logs_cleaned_at is not None:
                        ts_str = datetime.fromtimestamp(
                            logs_cleaned_at).strftime('%Y-%m-%d %H:%M:%S')
                        print(f'Task {task_name}({task_id}) log has been '
                              f'cleaned at {ts_str}.')
                        continue
                    task_str = (f'Task {task_name}({task_id})'
                                if task_name else f'Task {task_id}')
                    # Show task header when multiple tasks OR when filtering
                    if num_tasks > 1 or task is not None:
                        print(f'=== {task_str} ===')
                    log_path = os.path.expanduser(log_file)
                    if tail is not None:
                        assert tail > 0
                        # Backward-seek tail: O(tail × line) instead of
                        # scanning the whole file. The previous
                        # `collections.deque(f, maxlen=tail)` scanned every
                        # byte of the cached log, making dashboard log
                        # loading 10+ s for multi-GB cancelled jobs.
                        offset = max(tail_offset or 0, 0)
                        lines, _ = log_lib.tail_lines_from_end(
                            log_path, tail, offset)
                        # Apply the same start-stream-marker filter that
                        # log_lib.tail_logs_iter uses: when the marker
                        # appears in both the head of the file and the
                        # tail window (small log fully covered), filter
                        # so pre-marker boilerplate (Ray INFO lines etc.)
                        # is hidden.
                        with open(log_path, encoding='utf-8') as peek_f:
                            head_lines = log_lib._peek_head_lines(peek_f)  # type: ignore[attr-defined] # pylint: disable=protected-access
                        start_streaming = (
                            log_lib._should_stream_the_whole_tail_lines(  # type: ignore[attr-defined] # pylint: disable=protected-access
                                head_lines, lines,
                                log_lib.LOG_FILE_START_STREAMING_AT))
                        for line in lines:
                            if log_lib.LOG_FILE_START_STREAMING_AT in line:
                                start_streaming = True
                            if start_streaming:
                                print(line, end='', flush=True)
                    else:
                        with open(log_path, encoding='utf-8') as f:
                            start_streaming = False
                            for line in f:
                                if (log_lib.LOG_FILE_START_STREAMING_AT
                                        in line):
                                    start_streaming = True
                                if start_streaming:
                                    print(line, end='', flush=True)
                    # Show task finished message for multi-task or filtering
                    if num_tasks > 1 or task is not None:
                        # Add the "Task finished" message for terminal states
                        if task_status.is_terminal():
                            print(ux_utils.finishing_message(
                                f'{task_str} finished '
                                f'(status: {task_status.value}).'),
                                  flush=True)
            if log_file_ever_existed:
                # Add the "Job finished" message for terminal states
                if managed_job_status.is_terminal():
                    print(ux_utils.finishing_message(
                        f'Job finished (status: {managed_job_status.value}).'),
                          flush=True)
                return '', exceptions.JobExitCode.from_managed_job_status(
                    managed_job_status)
            return (f'{colorama.Fore.YELLOW}'
                    f'Job {job_id} is already in terminal state '
                    f'{managed_job_status.value}. For more details, run: '
                    f'sky jobs logs --controller {job_id}'
                    f'{colorama.Style.RESET_ALL}'
                    f'{job_msg}',
                    exceptions.JobExitCode.from_managed_job_status(
                        managed_job_status))
        # Batch coordinator jobs run inline on the controller — no
        # separate cluster is provisioned. Stream controller logs instead
        # of trying to find a worker cluster handle.
        if managed_job_state.is_batch_job(job_id):
            return stream_logs(job_id,
                               job_name=None,
                               controller=True,
                               follow=follow,
                               tail=tail,
                               tail_offset=tail_offset)

        backend = backends.CloudVmRayBackend()

        while True:
            assert managed_job_status is not None, job_id
            if not _should_keep_logging(managed_job_status):
                break
            assert snapshot is not None, job_id
            task_id = snapshot.task_id
            managed_job_status = snapshot.status
            pool = snapshot.pool
            cluster_name = snapshot.cluster_name
            job_id_to_tail = snapshot.job_id_on_pool_cluster
            task_name = snapshot.task_name
            snapshot = None
            # We wait for managed_job_status to be not None above. Once we see
            # that it's not None, we don't expect it to every become None
            # again.
            assert managed_job_status is not None, (job_id, task_id,
                                                    managed_job_status)
            if not _should_keep_logging(managed_job_status):
                break

            assert task_id is not None, (job_id, task_id)

            handle = None
            if pool is None and task_name is not None:
                cluster_name = generate_managed_job_cluster_name(
                    task_name, job_id)
            if cluster_name is not None:
                handle = global_user_state.get_handle_from_cluster_name(
                    cluster_name)

            # Check the handle: The cluster can be preempted and removed from
            # the table before the managed job state is updated by the
            # controller. In this case, we should skip the logging, and wait for
            # the next round of status check.
            if (handle is None or managed_job_status
                    != managed_job_state.ManagedJobStatus.RUNNING):
                if not follow:
                    return '', exceptions.JobExitCode.SUCCEEDED
                status_str = ''
                if (managed_job_status is not None and managed_job_status
                        != managed_job_state.ManagedJobStatus.RUNNING):
                    status_str = f' (status: {managed_job_status.value})'
                logger.debug(
                    f'INFO: The log is not ready yet{status_str}. '
                    f'Waiting for {JOB_STATUS_CHECK_GAP_SECONDS} seconds.')
                # Poll the controller log frequently for provisioning spinner
                # updates, but only re-check the (more expensive) managed job
                # status every JOB_STATUS_CHECK_GAP_SECONDS.
                waited = 0.0
                while True:
                    # Keep the "Waiting for task to start" context and append
                    # the live cluster-launch status, so it's clear the job is
                    # waiting on its cluster to be provisioned.
                    provision_msg = _latest_provision_status_msg()
                    # Show only the blue headline of the cluster-launch status
                    # as a secondary detail under the waiting line; show nothing
                    # when there is no headline to display.
                    headline = (None if provision_msg is None else
                                _provision_status_headline(provision_msg))
                    provision_str = (''
                                     if headline is None else f'\n  {headline}')
                    msg = _JOB_WAITING_STATUS_MESSAGE.format(
                        status_str=status_str,
                        provision_str=provision_str,
                        job_id=job_id)
                    if msg != prev_msg:
                        status_display.update(msg)
                        prev_msg = msg
                    if waited >= JOB_STATUS_CHECK_GAP_SECONDS:
                        break
                    _sleep_log_follow_wait(_PROVISION_LOG_POLL_GAP_SECONDS)
                    waited += _PROVISION_LOG_POLL_GAP_SECONDS
                snapshot = get_stream_target_snapshot()
                continue
            assert (managed_job_status ==
                    managed_job_state.ManagedJobStatus.RUNNING)
            assert isinstance(handle, backends.CloudVmRayResourceHandle), handle
            status_display.stop()
            returncode = None
            if managed_job_runtime.is_registered():
                returncode = managed_job_runtime.tail_logs(
                    handle,
                    backend=backend,
                    job_id=job_id,
                    task_id=task_id,
                    job_id_on_cluster=job_id_to_tail,
                    follow=follow,
                    tail=tail,
                    tail_offset=tail_offset)
            if returncode is None:
                # OSS default: stream via backend.tail_logs (skylet/SSH/gRPC).
                # require_outputs defaults to False, so the return is int
                # (not Tuple[int, str, str]).
                tail_param = tail if tail is not None else 0
                returncode = typing.cast(
                    int,
                    backend.tail_logs(handle,
                                      job_id=job_id_to_tail,
                                      managed_job_id=job_id,
                                      follow=follow,
                                      tail=tail_param,
                                      tail_offset=tail_offset))
            if returncode in [rc.value for rc in exceptions.JobExitCode]:
                # If the log tailing exits with a known exit code we can safely
                # break the loop because it indicates the tailing process
                # succeeded (even though the real job can be SUCCEEDED or
                # FAILED). We use the status in job queue to show the
                # information, as the ManagedJobStatus is not updated yet.
                job_status: job_lib.JobStatus | None = None
                # handle being non-None implies cluster_name was set.
                assert cluster_name is not None, (job_id, task_id)
                if managed_job_runtime.is_registered():
                    runtime_result = managed_job_runtime.get_job_status(
                        handle, cluster_name, returncode=returncode)
                    if runtime_result is not None:
                        job_status, _ = runtime_result
                if job_status is None:
                    # OSS default: query skylet via backend.
                    job_statuses = backend.get_job_status(handle,
                                                          stream_logs=False)
                    job_status = list(job_statuses.values())[0]
                assert job_status is not None, 'No job found.'
                assert task_id is not None, job_id

                if job_status != job_lib.JobStatus.CANCELLED:
                    if not follow:
                        break

                    # Logs for retrying failed tasks.
                    if (job_status
                            in job_lib.JobStatus.user_code_failure_states()):
                        task_specs = managed_job_state.get_task_specs(
                            job_id, task_id)
                        if task_specs.get('max_restarts_on_errors', 0) == 0:
                            # We don't need to wait for the managed job status
                            # update, as the job is guaranteed to be in terminal
                            # state afterwards.
                            break
                        print()
                        status_display.update(
                            ux_utils.spinner_message(
                                'Waiting for next restart for the failed task'))
                        status_display.start()

                        def is_managed_job_status_updated(
                            status: managed_job_state.ManagedJobStatus | None
                        ) -> bool:
                            """Check if local managed job status reflects remote
                            job failure.

                            Ensures synchronization between remote cluster
                            failure detection (JobStatus.FAILED) and controller
                            retry logic.
                            """
                            return (
                                status
                                != managed_job_state.ManagedJobStatus.RUNNING)

                        while not is_managed_job_status_updated(
                                managed_job_status :=
                                managed_job_state.get_status(job_id)):
                            _sleep_log_follow_wait(JOB_STATUS_CHECK_GAP_SECONDS)
                        assert managed_job_status is not None, (
                            job_id, managed_job_status)
                        continue

                    if task_id == num_tasks - 1:
                        break

                    # If a task filter was specified, we're done with the
                    # specific task - don't wait for other tasks.
                    if filtered_task_id is not None:
                        break

                    # The log for the current job is finished. We need to
                    # wait until next job to be started.
                    logger.debug(
                        f'INFO: Log for the current task ({task_id}) '
                        'is finished. Waiting for the next task\'s log '
                        'to be started.')
                    # Add a newline to avoid the status display below
                    # removing the last line of the task output.
                    print()
                    status_display.update(
                        ux_utils.spinner_message(
                            f'Waiting for the next task: {task_id + 1}'))
                    status_display.start()
                    snapshot = _wait_for_next_task(job_id, task_id)
                    managed_job_status = snapshot.status
                    assert managed_job_status is not None, (job_id, snapshot)
                    continue

                # The job can be cancelled by the user or the controller (when
                # the cluster is partially preempted).
                logger.debug(
                    'INFO: Job is cancelled. Waiting for the status update in '
                    f'{JOB_STATUS_CHECK_GAP_SECONDS} seconds.')
            else:
                logger.debug(
                    f'INFO: (Log streaming) Got return code {returncode}. '
                    f'Retrying in {JOB_STATUS_CHECK_GAP_SECONDS} seconds.')
            # Finish early if the managed job status is already in terminal
            # state.
            managed_job_status = managed_job_state.get_status(job_id)
            assert managed_job_status is not None, job_id
            if not _should_keep_logging(managed_job_status):
                break
            logger.info(f'{colorama.Fore.YELLOW}The job cluster is preempted '
                        f'or failed.{colorama.Style.RESET_ALL}')
            msg = _JOB_CANCELLED_MESSAGE
            status_display.update(msg)
            prev_msg = msg
            status_display.start()
            # If the tailing fails, it is likely that the cluster fails, so we
            # wait a while to make sure the managed job state is updated by the
            # controller, and check the managed job queue again.
            # Wait a bit longer than the controller, so as to make sure the
            # managed job state is updated.
            _sleep_log_follow_wait(3 * JOB_STATUS_CHECK_GAP_SECONDS)
            managed_job_status = managed_job_state.get_status(job_id)
            assert managed_job_status is not None, (job_id, managed_job_status)

    # Preserve the latest-task verdict already observed by the follow loop.
    # get_status() repeats the same latest-task query, so only a non-terminal
    # snapshot needs the final-wait refresh below.
    assert managed_job_status is not None, job_id
    if not managed_job_status.is_terminal():
        # The managed_job_status may not be in terminal status yet, since the
        # controller has not updated the managed job state yet. We wait for a
        # while, until the managed job state is updated.
        wait_seconds = 0
        managed_job_status = managed_job_state.get_status(job_id)
        assert managed_job_status is not None, job_id
        while (_should_keep_logging(managed_job_status) and follow and
               wait_seconds < _FINAL_JOB_STATUS_WAIT_TIMEOUT_SECONDS):
            _sleep_log_follow_wait(1)
            wait_seconds += 1
            managed_job_status = managed_job_state.get_status(job_id)
            assert managed_job_status is not None, job_id

    assert managed_job_status is not None, job_id
    if not follow and not managed_job_status.is_terminal():
        # The job is not in terminal state and we are not following,
        # just return.
        return '', exceptions.JobExitCode.SUCCEEDED
    logger.info(
        ux_utils.finishing_message(f'Managed job finished: {job_id} '
                                   f'(status: {managed_job_status.value}).'))
    return '', exceptions.JobExitCode.from_managed_job_status(
        managed_job_status)


def stream_logs(job_id: int | None,
                job_name: str | None,
                controller: bool = False,
                follow: bool = True,
                tail: int | None = None,
                tail_offset: int | None = None,
                task: str | int | None = None) -> tuple[str, int]:
    """Stream logs by job id or job name.

    Args:
        job_id: The job ID to stream logs for.
        job_name: The job name to stream logs for.
        controller: Whether to stream controller logs.
        follow: Whether to follow the logs.
        tail: Number of lines to tail from the end of the log file.
        task: Task identifier to view logs for a specific task in a JobGroup.
            If an int, it is treated as a task ID. If a str, it is treated as
            a task name. If None, logs for all tasks are shown.

    Returns:
        A tuple containing the log message and the exit code based on success
        or failure of the job. 0 if success, 100 if the job failed.
        See exceptions.JobExitCode for possible exit codes.
    """
    if job_id is None and job_name is None:
        job_id = managed_job_state.get_latest_job_id()
        if job_id is None:
            return 'No managed job found.', exceptions.JobExitCode.NOT_FOUND

    if controller:
        return controller_log_stream.stream_controller_logs(
            job_id,
            job_name,
            follow,
            tail,
            tail_offset,
            controller_log_file_for_job_func=controller_log_file_for_job,
            is_relayed_status_payload_line_func=(
                _is_relayed_status_payload_line),
        )

    if job_id is None:
        assert job_name is not None
        job_ids = managed_job_state.get_nonterminal_job_ids_by_name(job_name)
        if not job_ids:
            return (f'No running managed job found with name {job_name!r}.',
                    exceptions.JobExitCode.NOT_FOUND)
        if len(job_ids) > 1:
            raise ValueError(
                f'Multiple running jobs found with name {job_name!r}.')
        job_id = job_ids[0]

    return stream_logs_by_id(job_id, follow, tail, tail_offset, task)


def parse_job_cancel_file(content: str) -> tuple[bool, int | None]:
    """Parse the job cancel signal file to check if graceful cancel is enabled.

    Args:
        content: content of the signal file, if any.

    Returns:
        A tuple of whether graceful cancel is enabled, and cancel timeout if
        present.
    """
    graceful, graceful_timeout = False, None
    if content and content.startswith(_JOBS_GRACEFUL_CANCEL_SIGNAL):
        graceful = True
        if ':' in content:
            try:
                graceful_timeout = int(content.split(':')[1])
            except (ValueError, IndexError):
                logger.warning('Incorrect graceful signal contents. Got: '
                               f'{content}. Ignoring timeout...')
    return graceful, graceful_timeout


ManagedJobCodeGen = managed_job_codegen.ManagedJobCodeGen
ManagedJobCodeGen.__module__ = __name__
