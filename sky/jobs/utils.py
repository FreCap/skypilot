"""User interfaces with managed jobs.

NOTE: whenever an API change is made in this file, we need to bump the
jobs.constants.MANAGED_JOBS_VERSION and handle the API change in the
ManagedJobCodeGen.
"""
import asyncio
from datetime import datetime
import os
import pathlib
import re
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
from sky.jobs import log_streaming as managed_job_log_streaming
from sky.jobs import managed_job_codegen
from sky.jobs import queue_utils as managed_job_queue_utils
from sky.jobs import runtime as managed_job_runtime
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
from sky.utils import message_utils
from sky.utils import status_lib
from sky.utils import subprocess_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky.schemas.generated import managed_jobsv1_pb2

if typing.TYPE_CHECKING:
    from google.protobuf import descriptor
    from google.protobuf import json_format
    import grpc
    import psutil

    import sky
    from sky.client import sdk
    from sky.schemas.generated import jobsv1_pb2
    from sky.schemas.generated import managed_jobsv1_pb2
    from sky.utils import controller_utils
    from sky.utils import debug_dump_helpers
else:
    json_format = adaptors_common.LazyImport('google.protobuf.json_format')
    descriptor = adaptors_common.LazyImport('google.protobuf.descriptor')
    psutil = adaptors_common.LazyImport('psutil')
    grpc = adaptors_common.LazyImport('grpc')
    jobsv1_pb2 = adaptors_common.LazyImport('sky.schemas.generated.jobsv1_pb2')
    managed_jobsv1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.managed_jobsv1_pb2')
    sdk = adaptors_common.LazyImport('sky.client.sdk')
    # jobs.utils is imported while the public jobs package is still being
    # initialized.  Both helpers eventually import task/Serve modules, so
    # defer them until an actual controller operation needs them.
    controller_utils = adaptors_common.LazyImport('sky.utils.controller_utils')
    debug_dump_helpers = adaptors_common.LazyImport(
        'sky.utils.debug_dump_helpers')

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
    """Terminate a managed-job cluster through the fenced request path.

    Provider effects from a managed-job controller must be represented by a
    nested API request.  The request carries the authenticated outer
    generation and exact job slot attempt, allowing handoff to revoke and
    quiesce the effect before a successor attempt is admitted.
    """
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
            request_id = sdk.down(cluster_name,
                                  graceful=graceful,
                                  graceful_timeout=graceful_timeout)
            sdk.get(request_id)
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
    produced by create_job_api_token. Matching tokens are deleted.

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
    terminal_cleanup_count = requeue_terminal_done_jobs_with_live_clusters()
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
        if terminal_cleanup_count:
            message = (
                f'Requeued {terminal_cleanup_count} terminal managed job(s) '
                'with live cluster rows for cleanup.\n')
            logger.info(message.rstrip())
            f.write(message)
        # Disposable ControllerManager processes are fixed runtime-owned
        # slots.  The successor generation resets all stale/null-slot rows
        # above, then runtime admission starts the complete fixed slot set.
        # PID inspection and scheduler-triggered process birth are deliberately
        # absent: Linux identities are Pod-local supervision diagnostics.
        f.write(f'HA recovery completed at {datetime.now()}\n')
        f.write(f'Total recovery time: {time.time() - start} seconds\n')


def _task_has_launch_attempt(task: dict[str, Any]) -> bool:
    """Whether a task can own a generated managed-job cluster."""
    return any(
        task.get(field) is not None
        for field in ('submitted_at', 'start_at', 'last_recovered_at'))


def requeue_terminal_done_jobs_with_live_clusters() -> int:
    """Re-admit legacy DONE cluster orphans to scheduler-owned cleanup.

    This function only repairs durable scheduler state. The cleanup-only
    controller manager remains the sole owner of provider and storage effects.
    """
    cluster_candidates = (
        global_user_state.get_managed_job_cluster_cleanup_candidates())
    cluster_names_by_job_id: dict[int, set[str]] = {}
    for cluster_name, workload_id in cluster_candidates.items():
        candidate_id = workload_id
        if candidate_id is None:
            _, separator, suffix = cluster_name.rpartition('-')
            if not separator:
                continue
            candidate_id = suffix
        try:
            job_id = int(candidate_id)
        except (TypeError, ValueError):
            continue
        cluster_names_by_job_id.setdefault(job_id, set()).add(cluster_name)

    job_snapshots = managed_job_state.get_jobs_status_check_info(
        list(cluster_names_by_job_id))
    cleanup_job_ids = []
    for job_id, info in job_snapshots.items():
        if (info['schedule_state']
                != managed_job_state.ManagedJobScheduleState.DONE or
                info['pool'] is not None or
                not all(task['status'].is_terminal()
                        for task in info['tasks'])):
            continue
        expected_cluster_names = {
            generate_managed_job_cluster_name(task['task_name'], job_id)
            for task in info['tasks']
            if _task_has_launch_attempt(task)
        }
        if expected_cluster_names.isdisjoint(cluster_names_by_job_id[job_id]):
            continue
        cleanup_job_ids.append(job_id)

    return managed_job_state.requeue_terminal_done_jobs_for_cleanup(
        cleanup_job_ids)


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
    # TODO(luca) make this async
    handle, cluster_status = await asyncio.to_thread(
        global_user_state.get_cluster_handle_status_from_name, cluster_name)

    def _log_job_status(status: Optional['job_lib.JobStatus']) -> None:
        if status is None:
            logger.info('No job found.')
        else:
            logger.info(f'Job status: {status}')
        logger.info('=' * 34)

    logger.info('=== Checking the job status... ===')

    if handle is None:
        # This can happen if the cluster was preempted and background status
        # refresh already noticed and cleaned it up.
        logger.info(f'Cluster {cluster_name} not found.')
        return None, None
    if cluster_status not in (status_lib.ClusterStatus.UP,
                              status_lib.ClusterStatus.AUTOSTOPPING):
        logger.info(f'Cluster {cluster_name} is not UP-like '
                    f'(status: {cluster_status.value}); skipping remote job '
                    'status check.')
        return None, f'Cluster is not UP-like ({cluster_status.value})'
    if managed_job_runtime.is_registered():
        result = await asyncio.to_thread(managed_job_runtime.get_job_status,
                                         handle, cluster_name)
        if result is not None:
            status, _ = result
            _log_job_status(status)
            return result
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


def update_managed_jobs_statuses(job_ids: list[int] | None = None,
                                 jobs_info: dict[int, dict[str, Any]] |
                                 None = None):
    """Update managed job status if the controller process failed abnormally.

    Check the status of the controller process. If it is not running, it must
    have exited abnormally, and we should set the job status to
    FAILED_CONTROLLER. `end_at` will be set to the current timestamp for the job
    when above happens, which could be not accurate based on the frequency this
    function is called.

    Note: we expect that job_ids, if provided, refer to nonterminal jobs or
    jobs that have not completed their cleanup (schedule state not DONE).
    Callers that already fetched ``get_jobs_status_check_info()`` for the same
    explicit ``job_ids`` may pass that snapshot via ``jobs_info`` to reuse the
    first lifecycle read instead of re-querying before refresh.
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

    def _snapshot_kwargs(snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            'schedule_state': snapshot['schedule_state'],
            'controller_pid': snapshot['controller_pid'],
            'controller_pid_started_at': snapshot['controller_pid_started_at'],
            'controller_instance_id': snapshot.get('controller_instance_id'),
            'controller_generation': snapshot.get('controller_generation'),
            'controller_slot_id': snapshot.get('controller_slot_id'),
            'controller_slot_attempt': snapshot.get('controller_slot_attempt'),
            'controller_slot_quiescing':
                snapshot.get('controller_slot_quiescing'),
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
                == info.get('controller_generation') and
                fresh_info.get('controller_slot_id')
                == info.get('controller_slot_id') and
                fresh_info.get('controller_slot_attempt')
                == info.get('controller_slot_attempt') and
                fresh_info.get('controller_slot_quiescing')
                == info.get('controller_slot_quiescing'))

    controller_liveness_cache: dict[managed_job_state.ControllerPidRecord,
                                    bool] = {}

    def _controller_process_alive_once(
            record: managed_job_state.ControllerPidRecord) -> bool:
        cached = controller_liveness_cache.get(record)
        if cached is not None:
            return cached
        alive = controller_process_alive(record)
        controller_liveness_cache[record] = alive
        return alive

    # Fetch the jobs that need checking together with the small per-job fields
    # the loop consumes. This keeps the refresh tick on a single slim query
    # instead of a filtered job-id query followed by a second detail query.
    if jobs_info is None:
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
        has_complete_fixed_slot_identity = all(
            info.get(field) is not None
            for field in ('controller_instance_id', 'controller_generation',
                          'controller_slot_id', 'controller_slot_attempt'))
        if has_complete_fixed_slot_identity:
            # Fixed-slot jobs are owned by the runtime slot supervisor.  It
            # proves the complete process family absent, quiesces exact nested
            # requests, and hands the row to the scheduler for recovery or
            # cleanup adoption.  A periodic refresher cannot establish those
            # facts from a Pod-local PID and must have no provider or terminal
            # lifecycle effects for these rows.
            logger.debug(f'Job {job_id} has a complete fixed-slot identity; '
                         'deferring liveness and cleanup to slot supervision.')
            continue
        snapshot_all_tasks_terminal = all(
            task['status'].is_terminal() for task in tasks)
        if snapshot_all_tasks_terminal:
            # Terminal cleanup has one canonical owner: the controller manager
            # or a replacement scheduler cleanup adopter.  Keep old/incomplete
            # rows visible as a deployment gate instead of reintroducing a
            # second best-effort provider cleanup path in the refresh daemon.
            logger.info(f'Job {job_id} has terminal tasks but an incomplete '
                        'fixed-slot identity; deferring cleanup adoption.')
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
            if _controller_process_alive_once(
                    managed_job_state.ControllerPidRecord(
                        pid=pid, started_at=pid_started_at)):
                # The controller is still running, so this job is fine.
                continue
            logger.error(f'Controller process for {job_id} seems to be dead.')
            failure_reason = 'Controller process is dead'

        # At this point, either pid is None or process is dead.

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
        failure_decision = (
            managed_job_state.set_failed_controller_if_current_snapshot(
                job_id,
                **_snapshot_kwargs(info),
                failure_reason=failure_message))
        if (failure_decision ==
                managed_job_state.ControllerFailureDecision.TERMINALIZED):
            logger.info(failure_message)
            continue
        if (failure_decision ==
                managed_job_state.ControllerFailureDecision.ALREADY_TERMINAL):
            logger.info(f'Job {job_id} already reached terminal task status; '
                        'deferring cleanup adoption without rewriting the job '
                        'to FAILED_CONTROLLER.')
            continue

        # The atomic FAILED_CONTROLLER write already locked and rechecked the
        # exact snapshot. Only the declined-CAS path pays for a fresh point
        # read so the common dead-controller case stays on one exact DB write.
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
        logger.info(f'Job {job_id} changed before FAILED_CONTROLLER could '
                    'be committed; deferring cleanup.')


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
    initial_info = managed_job_state.get_jobs_status_check_info(job_ids)
    initial_states = (
        managed_job_state.get_job_cancellation_states_from_status_check_info(
            initial_info))
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
        # and a cancel of N live jobs reuses the same initial lifecycle read
        # instead of issuing a second pre-refresh snapshot query.
        refresh_jobs_info = {
            job_id: initial_info[job_id]
            for job_id in jobs_to_refresh
            if job_id in initial_info
        }
        update_managed_jobs_statuses(jobs_to_refresh,
                                     jobs_info=refresh_jobs_info)
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


class ManagedJobCancelCriteriaError(ValueError):
    """A cancel request that does not name exactly one valid selector."""


def cancel_jobs_from_request(
        request: 'managed_jobsv1_pb2.CancelJobsRequest') -> str:
    """Dispatch one managed-job cancel request to its selector variant.

    This is the single cancel dispatch. The skylet ``CancelJobs`` servicer
    calls it for gRPC-enabled controllers, and the API server calls it
    in-process in consolidation mode, where the jobs controller runs inside
    the server deployment and no skylet gRPC server exists.
    """
    criteria = request.WhichOneof('cancellation_criteria')
    if criteria is None:
        raise ManagedJobCancelCriteriaError(
            'exactly one cancellation criteria must be specified.')
    graceful = request.graceful if request.HasField('graceful') else False
    graceful_timeout = (request.graceful_timeout
                        if request.HasField('graceful_timeout') else None)
    if criteria == 'all_users':
        user_hash = (request.user_hash
                     if request.HasField('user_hash') else None)
        if not request.all_users and user_hash is None:
            raise ManagedJobCancelCriteriaError(
                'user_hash is required when all_users is False')
        return cancel_jobs_by_id(job_ids=None,
                                 all_users=request.all_users,
                                 current_workspace=request.current_workspace,
                                 user_hash=user_hash,
                                 graceful=graceful,
                                 graceful_timeout=graceful_timeout)
    if criteria == 'job_ids':
        return cancel_jobs_by_id(job_ids=list(request.job_ids.ids),
                                 current_workspace=request.current_workspace,
                                 graceful=graceful,
                                 graceful_timeout=graceful_timeout)
    if criteria == 'job_name':
        return cancel_job_by_name(job_name=request.job_name,
                                  current_workspace=request.current_workspace,
                                  graceful=graceful,
                                  graceful_timeout=graceful_timeout)
    if criteria == 'pool_name':
        return cancel_jobs_by_pool(pool_name=request.pool_name,
                                   current_workspace=request.current_workspace)
    raise ManagedJobCancelCriteriaError(
        f'invalid cancellation criteria: {criteria}')


def _sync_log_streaming_facade() -> None:
    """Keep historical replaceable bindings effective after extraction."""
    managed_job_log_streaming.sync_facade(
        sleep_log_follow_wait=_sleep_log_follow_wait,
        name_generator=generate_managed_job_cluster_name,
        provision_status_reader=read_provision_status_from_log,
        batch_streamer=stream_logs,
        logger_override=logger,
        status_gap_seconds=JOB_STATUS_CHECK_GAP_SECONDS,
        provision_poll_seconds=_PROVISION_LOG_POLL_GAP_SECONDS,
        final_status_timeout_seconds=_FINAL_JOB_STATUS_WAIT_TIMEOUT_SECONDS,
        waiting_status_message=_JOB_WAITING_STATUS_MESSAGE,
        cancelled_message=_JOB_CANCELLED_MESSAGE,
    )


controller_log_file_for_job = (
    managed_job_log_streaming.controller_log_file_for_job)
read_provision_status_from_log = (
    managed_job_log_streaming.read_provision_status_from_log)
_is_relayed_status_payload_line = (
    managed_job_log_streaming.is_relayed_status_payload_line)
_provision_status_headline = (
    managed_job_log_streaming.provision_status_headline)
_should_keep_logging = managed_job_log_streaming.should_keep_logging
select = managed_job_log_streaming.select_module
signal = managed_job_log_streaming.signal_module
sys = managed_job_log_streaming.sys_module
threading = managed_job_log_streaming.threading_module
rich_utils = managed_job_log_streaming.rich_utils_module


def _wait_for_next_task(
        job_id: int,
        current_task_id: int) -> managed_job_state.JobLogStreamSnapshot:
    _sync_log_streaming_facade()
    return managed_job_log_streaming.wait_for_next_task(job_id, current_task_id)


def _wait_for_initial_log_stream_snapshot(
    get_snapshot: typing.Callable[[], managed_job_state.JobLogStreamSnapshot]
) -> managed_job_state.JobLogStreamSnapshot:
    _sync_log_streaming_facade()
    return managed_job_log_streaming.wait_for_initial_log_stream_snapshot(
        get_snapshot)


def stream_logs_by_id(job_id: int,
                      follow: bool = True,
                      tail: int | None = None,
                      tail_offset: int | None = None,
                      task: str | int | None = None) -> tuple[str, int]:
    """Stream logs by job id through the focused log lifecycle."""
    _sync_log_streaming_facade()
    return managed_job_log_streaming.stream_logs_by_id(job_id, follow, tail,
                                                       tail_offset, task)


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
