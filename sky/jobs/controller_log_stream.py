"""Local controller-log streaming for managed jobs."""

from collections.abc import Callable
import os
import time

from sky import exceptions
from sky.jobs import state as managed_job_state
from sky.skylet import log_lib
from sky.utils import ux_utils


def _controller_log_follow_is_complete(
    follow_state: managed_job_state.ControllerLogFollowState,) -> bool:
    """Whether controller-log following has observed final controller exit."""
    status = follow_state.status
    if status is None:
        return False
    if follow_state.schedule_state is None:
        return status.is_terminal()
    return follow_state.schedule_state == (
        managed_job_state.ManagedJobScheduleState.DONE)


def stream_controller_logs(
    job_id: int | None,
    job_name: str | None,
    follow: bool,
    tail: int | None,
    tail_offset: int | None,
    *,
    controller_log_file_for_job_func: Callable[[int], str],
    is_relayed_status_payload_line_func: Callable[[str], bool],
) -> tuple[str, int]:
    """Stream a managed job's local controller log."""
    if job_id is None:
        assert job_name is not None
        managed_jobs, _ = managed_job_state.get_managed_jobs_with_filters(
            name_match=job_name, fields=['job_id', 'job_name', 'status'])
        # We manually filter the jobs by name, instead of using
        # get_nonterminal_job_ids_by_name, as controller logs should be
        # available for jobs in terminal states.
        managed_job_ids: set[int] = {
            job['job_id'] for job in managed_jobs if job['job_name'] == job_name
        }
        if not managed_job_ids:
            return (f'No managed job found with name {job_name!r}.',
                    exceptions.JobExitCode.NOT_FOUND)
        if len(managed_job_ids) > 1:
            job_ids_str = ', '.join(str(job_id) for job_id in managed_job_ids)
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    f'Multiple managed jobs found with name {job_name!r} '
                    f'(Job IDs: {job_ids_str}). Please specify the job_id '
                    'instead.')
        job_id = managed_job_ids.pop()
    assert job_id is not None, (job_id, job_name)

    controller_log_path = controller_log_file_for_job_func(job_id)
    follow_state = managed_job_state.ControllerLogFollowState(None, None)

    # Wait for the log file to be written.
    while not os.path.exists(controller_log_path):
        if not follow:
            # Assume that the log file hasn't been written yet. Since we
            # aren't following, just return.
            return '', exceptions.JobExitCode.SUCCEEDED

        follow_state = managed_job_state.get_controller_log_follow_state(job_id)
        if follow_state.status is None:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'Job {job_id} not found.')
        if _controller_log_follow_is_complete(follow_state):
            # Don't keep waiting. If the log file is not created by this
            # point, it never will be. This job may have been submitted using
            # an old version that did not create the file, so this is not an
            # exceptional case.
            return '', exceptions.JobExitCode.from_managed_job_status(
                follow_state.status)

        time.sleep(log_lib.SKY_LOG_WAITING_GAP_SECONDS)

    # This code is based on log_lib.tail_logs. We can't use that code exactly
    # because state works differently between managed jobs and normal jobs.
    offset_arg = (tail_offset
                  if tail_offset is not None and tail_offset > 0 else 0)
    # Phase 1: emit the historical window. For tail!=None we use a
    # backward-seek read so cost is O(tail) instead of O(file_size); otherwise
    # stream the whole file (the legacy tail=None controller-log behavior).
    end_pos = 0
    if tail is not None:
        assert tail > 0
        tail_lines, end_pos = log_lib.tail_lines_from_end(
            controller_log_path, tail, offset_arg)
        for line in tail_lines:
            if is_relayed_status_payload_line_func(line):
                continue
            print(line, end='')
        print(end='', flush=True)
    else:
        with open(controller_log_path, newline='', encoding='utf-8') as f:
            for line in f:
                if is_relayed_status_payload_line_func(line):
                    continue
                print(line, end='')
            end_pos = f.tell()
            print(end='', flush=True)

    # Phase 2: optionally follow new bytes from where the tail read stopped.
    # Reopen so the prior file handle (which may have been binary in the seek
    # branch) doesn't leak.
    if follow:
        with open(controller_log_path, newline='', encoding='utf-8') as f:
            f.seek(end_pos)
            while True:
                # Print all new lines, if there are any.
                line = f.readline()
                while line is not None and line != '':
                    if not is_relayed_status_payload_line_func(line):
                        print(line, end='')
                    line = f.readline()

                # Flush.
                print(end='', flush=True)

                follow_state = managed_job_state.get_controller_log_follow_state(
                    job_id)
                assert follow_state.status is not None, (job_id, job_name)
                if _controller_log_follow_is_complete(follow_state):
                    break

                time.sleep(log_lib.SKY_LOG_TAILING_GAP_SECONDS)

            # Wait for final logs to be written.
            time.sleep(1 + log_lib.SKY_LOG_TAILING_GAP_SECONDS)

            # Print any remaining logs including an incomplete line.
            remaining = f.read()
            if remaining:
                print(''.join(
                    line for line in remaining.splitlines(keepends=True)
                    if not is_relayed_status_payload_line_func(line)),
                      end='',
                      flush=True)

    if follow:
        assert follow_state.status is not None, (job_id, job_name)
        return ux_utils.finishing_message(
            f'Job finished (status: {follow_state.status}).'
        ), exceptions.JobExitCode.from_managed_job_status(follow_state.status)

    return '', exceptions.JobExitCode.SUCCEEDED
