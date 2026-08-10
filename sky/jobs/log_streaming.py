"""Managed-job log streaming and provisioning presentation."""

from datetime import datetime
import os
import re
import select
import signal
import sys
import threading
import time
import typing

import colorama

from sky import backends
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky.jobs import constants as managed_job_constants
from sky.jobs import runtime as managed_job_runtime
from sky.jobs import state as managed_job_state
from sky.jobs.naming import generate_managed_job_cluster_name
from sky.skylet import job_lib
from sky.skylet import log_lib
from sky.utils import context_utils
from sky.utils import message_utils
from sky.utils import rich_utils
from sky.utils import ux_utils

logger = sky_logging.init_logger(__name__)

# Defaults match the historical sky.jobs.utils facade. The facade refreshes
# these replaceable bindings before dispatch so existing patch paths remain
# effective.
JOB_STATUS_CHECK_GAP_SECONDS = 15
_PROVISION_LOG_POLL_GAP_SECONDS = 1
_JOB_WAITING_STATUS_MESSAGE = ux_utils.spinner_message(
    'Waiting for task to start[/]'
    '{status_str}. It may take a few minutes.{provision_str}\n'
    '  [dim]View controller logs: sky jobs logs --controller {job_id}')
_JOB_CANCELLED_MESSAGE = (
    ux_utils.spinner_message('Waiting for task status to be updated.') +
    ' It may take a minute.')
_FINAL_JOB_STATUS_WAIT_TIMEOUT_SECONDS = 120


def _sleep_log_follow_wait(seconds: float) -> None:
    """Sleep between log-follow polls while honoring cancellation."""
    context_utils.sleep_with_cancellation(seconds)


def _missing_batch_streamer(*args: typing.Any,
                            **kwargs: typing.Any) -> tuple[str, int]:
    del args, kwargs
    raise RuntimeError('The managed-job log facade was not initialized.')


stream_logs: typing.Callable[..., tuple[str, int]] = _missing_batch_streamer


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


def _task_filter_not_found(job_id: int, task_filter: str | int,
                           num_tasks: int) -> tuple[str, int]:
    if num_tasks == 0:
        return f'Job {job_id} not found.', exceptions.JobExitCode.NOT_FOUND
    valid_range = f'0-{num_tasks - 1}' if num_tasks > 1 else '0'
    return (f'No task found matching {task_filter!r} in job {job_id}. '
            f'Valid task IDs are {valid_range}.',
            exceptions.JobExitCode.NOT_FOUND)


def _render_stopped_snapshot_logs(
    job_id: int,
    managed_job_status: managed_job_state.ManagedJobStatus,
    *,
    task_filter: str | int | None,
    filtered_task_id: int | None,
    filtered_lookup: managed_job_state.TaskLogStreamLookup | None = None,
    num_tasks: int | None,
    tail: int | None,
    tail_offset: int | None,
) -> tuple[str, int]:
    """Render cached logs for a snapshot that should stop active following."""
    terminal_task_info: typing.Sequence[tuple[
        int,
        str | None,
        managed_job_state.ManagedJobStatus,
        str | None,
        float | None,
    ]]
    job_msg = ''
    if managed_job_status.is_failed():
        job_msg = ('\nFailure reason: '
                   f'{managed_job_state.get_failure_reason(job_id)}')
    log_file_ever_existed = False
    if filtered_task_id is not None:
        lookup = (filtered_lookup if filtered_lookup is not None else
                  managed_job_state.get_task_log_stream_lookup(
                      job_id, filtered_task_id))
        if lookup.snapshot.task_id is None:
            if num_tasks is None:
                num_tasks = lookup.num_tasks
            assert task_filter is not None, filtered_task_id
            return _task_filter_not_found(job_id, task_filter, num_tasks)
        assert lookup.snapshot.status is not None, (job_id, filtered_task_id)
        terminal_task_info = [(
            lookup.snapshot.task_id,
            lookup.snapshot.task_name,
            lookup.snapshot.status,
            lookup.local_log_file,
            lookup.logs_cleaned_at,
        )]
    else:
        terminal_task_info = managed_job_state.get_all_task_ids_names_statuses_logs(
            job_id)
        assert terminal_task_info is not None, job_id

    num_terminal_tasks = len(terminal_task_info)
    for (task_id, task_name, task_status, log_file,
         logs_cleaned_at) in terminal_task_info:
        if log_file:
            log_file_ever_existed = True
            if logs_cleaned_at is not None:
                ts_str = datetime.fromtimestamp(logs_cleaned_at).strftime(
                    '%Y-%m-%d %H:%M:%S')
                print(f'Task {task_name}({task_id}) log has been '
                      f'cleaned at {ts_str}.')
                continue
            task_str = (f'Task {task_name}({task_id})'
                        if task_name else f'Task {task_id}')
            # Show task header when multiple tasks OR when filtering
            if num_terminal_tasks > 1 or task_filter is not None:
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
                lines, _ = log_lib.tail_lines_from_end(log_path, tail, offset)
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
                        head_lines, lines, log_lib.LOG_FILE_START_STREAMING_AT))
                for line in lines:
                    if log_lib.LOG_FILE_START_STREAMING_AT in line:
                        start_streaming = True
                    if start_streaming:
                        print(line, end='', flush=True)
            else:
                with open(log_path, encoding='utf-8') as f:
                    start_streaming = False
                    for line in f:
                        if log_lib.LOG_FILE_START_STREAMING_AT in line:
                            start_streaming = True
                        if start_streaming:
                            print(line, end='', flush=True)
            # Show task finished message for multi-task or filtering
            if num_terminal_tasks > 1 or task_filter is not None:
                # Add the "Task finished" message for terminal states
                if task_status.is_terminal():
                    print(ux_utils.finishing_message(
                        f'{task_str} finished (status: {task_status.value}).'),
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
            exceptions.JobExitCode.from_managed_job_status(managed_job_status))


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

    # Batch coordinator jobs never stream worker-task logs: the coordinator
    # runs inline on the controller, so the controller log is the only durable
    # log source regardless of any task filter.
    if managed_job_state.is_batch_job(job_id):
        return stream_logs(job_id,
                           job_name=None,
                           controller=True,
                           follow=follow,
                           tail=tail,
                           tail_offset=tail_offset)

    msg = _JOB_WAITING_STATUS_MESSAGE.format(status_str='',
                                             provision_str='',
                                             job_id=job_id)
    status_display = rich_utils.safe_status(msg)
    num_tasks: int | None = None
    if task is None:
        num_tasks = managed_job_state.get_num_tasks(job_id)

    # Resolve task filter to a specific task_id if provided
    # This is used for running jobs to stream logs from the correct task
    filtered_task_id: int | None = None
    prefetched_snapshot: managed_job_state.JobLogStreamSnapshot | None = None
    prefetched_lookup: managed_job_state.TaskLogStreamLookup | None = None
    if task is not None:
        if isinstance(task, int):
            lookup = managed_job_state.get_task_log_stream_lookup(job_id, task)
            prefetched_lookup = lookup
            prefetched_snapshot = lookup.snapshot
            num_tasks = lookup.num_tasks
            if prefetched_snapshot.task_id is None:
                if num_tasks == 0:
                    return (f'Job {job_id} not found.',
                            exceptions.JobExitCode.NOT_FOUND)
                return _task_filter_not_found(job_id, task, num_tasks)
            filtered_task_id = task
        else:
            lookup = managed_job_state.get_task_log_stream_lookup_by_name(
                job_id, task)
            prefetched_lookup = lookup
            prefetched_snapshot = lookup.snapshot
            num_tasks = lookup.num_tasks
            if prefetched_snapshot.task_id is None:
                if num_tasks == 0:
                    return (f'Job {job_id} not found.',
                            exceptions.JobExitCode.NOT_FOUND)
                return _task_filter_not_found(job_id, task, num_tasks)
            filtered_task_id = prefetched_snapshot.task_id

    # Check if job exists - if num_tasks is 0, the job doesn't exist
    if num_tasks == 0:
        return (f'Job {job_id} not found.', exceptions.JobExitCode.NOT_FOUND)

    def get_stream_target_snapshot() -> managed_job_state.JobLogStreamSnapshot:
        nonlocal prefetched_snapshot
        if prefetched_snapshot is not None:
            snapshot = prefetched_snapshot
            prefetched_snapshot = None
            return snapshot
        if filtered_task_id is None:
            return managed_job_state.get_latest_log_stream_snapshot(job_id)
        return managed_job_state.get_task_log_stream_snapshot(
            job_id, filtered_task_id)

    def get_follow_status() -> managed_job_state.ManagedJobStatus | None:
        if filtered_task_id is None:
            return managed_job_state.get_status(job_id)
        # Task-filtered follow already resolved the exact task identity and
        # routing snapshot. Later lifecycle polls only need the current task
        # status, so keep them on the slim wait lookup instead of re-reading
        # log-routing metadata on every retry/final-wait iteration.
        lookup = managed_job_state.get_task_wait_status_lookup(
            job_id, filtered_task_id)
        if lookup.task_id is None:
            return None
        return lookup.status

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
        if task is None:
            assert num_tasks is not None, job_id
            if num_tasks > 1:
                print(f'{colorama.Fore.CYAN}Hint: This job has {num_tasks} '
                      'tasks. '
                      f'Use \'sky jobs logs {job_id} TASK\' to view logs for '
                      'a specific task (TASK can be task ID or name).'
                      f'{colorama.Style.RESET_ALL}')

        if not _should_keep_logging(managed_job_status):
            initial_filtered_lookup = None
            if (prefetched_lookup is not None and
                    prefetched_lookup.snapshot == initial_snapshot):
                initial_filtered_lookup = prefetched_lookup
            return _render_stopped_snapshot_logs(
                job_id,
                managed_job_status,
                task_filter=task,
                filtered_task_id=filtered_task_id,
                filtered_lookup=initial_filtered_lookup,
                num_tasks=num_tasks,
                tail=tail,
                tail_offset=tail_offset)
        prefetched_lookup = None
        backend = backends.CloudVmRayBackend()

        while True:
            assert managed_job_status is not None, job_id
            if not _should_keep_logging(managed_job_status):
                break
            if snapshot is None:
                # The startup wait seeds the first iteration, and the
                # not-ready and next-task waits seed their own. The remaining
                # re-entries - a broken/preempted tail falling through the
                # bottom of the loop, and a failed task waiting on its
                # restart - clear the snapshot without replacing it, so they
                # must re-read the routing target here.
                snapshot = get_stream_target_snapshot()
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
                    refreshed_lookup = None
                    if (handle is None and managed_job_status
                            == managed_job_state.ManagedJobStatus.RUNNING):
                        if filtered_task_id is None:
                            refreshed_snapshot = get_stream_target_snapshot()
                        else:
                            refreshed_lookup = (
                                managed_job_state.get_task_log_stream_lookup(
                                    job_id, filtered_task_id))
                            refreshed_snapshot = refreshed_lookup.snapshot
                        refreshed_status = refreshed_snapshot.status
                        assert refreshed_status is not None, (
                            job_id, refreshed_snapshot)
                        if not _should_keep_logging(refreshed_status):
                            return _render_stopped_snapshot_logs(
                                job_id,
                                refreshed_status,
                                task_filter=task,
                                filtered_task_id=filtered_task_id,
                                filtered_lookup=refreshed_lookup,
                                num_tasks=num_tasks,
                                tail=tail,
                                tail_offset=tail_offset)
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
                                managed_job_status := get_follow_status()):
                            _sleep_log_follow_wait(JOB_STATUS_CHECK_GAP_SECONDS)
                        assert managed_job_status is not None, (
                            job_id, managed_job_status)
                        continue

                    # If a task filter was specified, we're done with the
                    # specific task - don't wait for other tasks.
                    if filtered_task_id is not None:
                        break

                    assert num_tasks is not None, job_id
                    if task_id == num_tasks - 1:
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
            managed_job_status = get_follow_status()
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
            managed_job_status = get_follow_status()
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
        managed_job_status = get_follow_status()
        assert managed_job_status is not None, job_id
        while (_should_keep_logging(managed_job_status) and follow and
               wait_seconds < _FINAL_JOB_STATUS_WAIT_TIMEOUT_SECONDS):
            _sleep_log_follow_wait(1)
            wait_seconds += 1
            managed_job_status = get_follow_status()
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


# Public implementation names let the historical facade forward private names
# without reaching through another module's protected namespace.
is_relayed_status_payload_line: typing.Callable[[str], bool] = (
    _is_relayed_status_payload_line)
provision_status_headline: typing.Callable[[str], str |
                                           None] = (_provision_status_headline)
should_keep_logging: typing.Callable[[managed_job_state.ManagedJobStatus],
                                     bool] = _should_keep_logging
wait_for_next_task = _wait_for_next_task
wait_for_initial_log_stream_snapshot = _wait_for_initial_log_stream_snapshot
select_module = select
signal_module = signal
sys_module = sys
threading_module = threading
rich_utils_module = rich_utils


def sync_facade(
    *,
    sleep_log_follow_wait: typing.Callable[[float], None],
    name_generator: typing.Callable[[str, int], str],
    provision_status_reader: typing.Callable[[str, int, str | None],
                                             tuple[int, str | None]],
    batch_streamer: typing.Callable[..., tuple[str, int]],
    logger_override: typing.Any,
    status_gap_seconds: int,
    provision_poll_seconds: int,
    final_status_timeout_seconds: int,
    waiting_status_message: str,
    cancelled_message: str,
) -> None:
    """Refresh bindings historically patched through ``sky.jobs.utils``."""
    global stream_logs
    global logger
    global JOB_STATUS_CHECK_GAP_SECONDS
    global _PROVISION_LOG_POLL_GAP_SECONDS
    global _FINAL_JOB_STATUS_WAIT_TIMEOUT_SECONDS
    global _JOB_WAITING_STATUS_MESSAGE
    global _JOB_CANCELLED_MESSAGE

    module_bindings = globals()
    module_bindings['_sleep_log_follow_wait'] = sleep_log_follow_wait
    module_bindings['generate_managed_job_cluster_name'] = name_generator
    module_bindings['read_provision_status_from_log'] = provision_status_reader
    stream_logs = batch_streamer
    logger = logger_override
    JOB_STATUS_CHECK_GAP_SECONDS = status_gap_seconds
    _PROVISION_LOG_POLL_GAP_SECONDS = provision_poll_seconds
    _FINAL_JOB_STATUS_WAIT_TIMEOUT_SECONDS = final_status_timeout_seconds
    _JOB_WAITING_STATUS_MESSAGE = waiting_status_message
    _JOB_CANCELLED_MESSAGE = cancelled_message
