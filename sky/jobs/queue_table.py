"""Managed-jobs queue table presentation."""
import collections
from typing import Any

from sky.jobs import state as managed_job_state
from sky.schemas.api import responses
from sky.skylet import constants
from sky.utils import infra_utils
from sky.utils import log_utils


def _get_job_status_from_tasks(
    job_tasks: list[responses.ManagedJobRecord] | list[dict[str, Any]]
) -> tuple[managed_job_state.ManagedJobStatus, int]:
    """Get the current task status and the current task id for a job.

    For job groups with primary/auxiliary tasks, the job status is determined
    only by the primary tasks. If all primary tasks succeed, the job is
    considered successful even if auxiliary tasks were cancelled.
    """
    # Filter to only primary tasks for status determination.
    # is_primary_in_job_group: True/False for job groups, None for non-groups.
    # For non-job-groups (None), all tasks count for status.
    # For job groups, only tasks with is_primary_in_job_group=True count.
    primary_job_tasks = [
        t for t in job_tasks
        if t.get('is_primary_in_job_group') is None or  # Non-job-group
        t.get('is_primary_in_job_group') is True  # Primary task in job group
    ]
    # Use primary tasks for status; fall back to all tasks if none match
    job_tasks_for_status: list[responses.ManagedJobRecord] | list[dict[
        str, Any]] = (primary_job_tasks if primary_job_tasks else job_tasks)

    managed_task_status = managed_job_state.ManagedJobStatus.SUCCEEDED
    current_task_id = 0
    for task in job_tasks_for_status:
        task_status = task['status']
        # Handle both enum and string status values
        if isinstance(task_status, str):
            task_status = managed_job_state.ManagedJobStatus(task_status)
        managed_task_status = task_status
        current_task_id = task['task_id']

        # Use the first non-succeeded status.
        if managed_task_status != managed_job_state.ManagedJobStatus.SUCCEEDED:
            # TODO(zhwu): we should not blindly use the first non-
            # succeeded as the status could be changed to PENDING
            # when going from one task to the next one, which can be
            # confusing.
            break
    return managed_task_status, current_task_id


def format_job_table(
    tasks: list[dict[str, Any]],
    show_all: bool,
    show_user: bool,
    return_rows: bool = False,
    pool_status: list[dict[str, Any]] | None = None,
    max_jobs: int | None = None,
    job_status_counts: dict[str, int] | None = None,
) -> str | list[list[str]]:
    """Returns managed jobs as a formatted string.

    Args:
        jobs: A list of managed jobs.
        show_all: Whether to show all columns.
        max_jobs: The maximum number of jobs to show in the table.
        return_rows: If True, return the rows as a list of strings instead of
          all rows concatenated into a single string.
        pool_status: List of pool status dictionaries with replica_info.
        job_status_counts: The counts of each job status.

    Returns: A formatted string of managed jobs, if not `return_rows`; otherwise
      a list of "rows" (each of which is a list of str).
    """
    jobs = collections.defaultdict(list)
    # Check if the tasks have user information from kubernetes.
    # This is only used for sky status-kubernetes.
    tasks_have_k8s_user = any([task.get('user') for task in tasks])
    if max_jobs and tasks_have_k8s_user:
        raise ValueError('max_jobs is not supported when tasks have user info.')

    def get_hash(task):
        if tasks_have_k8s_user:
            return (task['user'], task['job_id'])
        return task['job_id']

    def _get_job_id_to_worker_map(
            pool_status: list[dict[str, Any]] | None) -> dict[int, int]:
        """Create a mapping from job_id to worker replica_id.

        Jobs that appear on multiple workers (e.g. batch coordinators
        that orchestrate across the whole pool) are excluded — they
        should not display a single ``(worker=N)`` annotation.

        Args:
            pool_status: List of pool status dictionaries with replica_info.

        Returns:
            Dictionary mapping job_id to replica_id (worker ID).
        """
        job_to_worker: dict[int, int] = {}
        multi_worker_jobs: set[int] = set()
        if pool_status is None:
            return job_to_worker
        for pool in pool_status:
            replica_info = pool.get('replica_info', [])
            for replica in replica_info:
                used_by = replica.get('used_by')
                if used_by is not None:
                    for job_id in used_by:
                        if job_id in job_to_worker:
                            multi_worker_jobs.add(job_id)
                        job_to_worker[job_id] = replica.get('replica_id')
        for job_id in multi_worker_jobs:
            del job_to_worker[job_id]
        return job_to_worker

    # Create mapping from job_id to worker replica_id
    job_to_worker = _get_job_id_to_worker_map(pool_status)

    for task in tasks:
        # The tasks within the same job_id are already sorted
        # by the task_id.
        jobs[get_hash(task)].append(task)

    workspaces = set()
    for job_tasks in jobs.values():
        workspaces.add(job_tasks[0].get('workspace',
                                        constants.SKYPILOT_DEFAULT_WORKSPACE))

    show_workspace = len(workspaces) > 1 or show_all

    user_cols: list[str] = []
    if show_user:
        user_cols = ['USER']
        if show_all:
            user_cols.append('USER_ID')

    def _fmt_batch_progress(task_or_tasks) -> str:
        """Format batch progress as 'completed/total' or '-' if not a batch."""
        if isinstance(task_or_tasks, list):
            t = task_or_tasks[0]
        else:
            t = task_or_tasks
        total = t.get('batch_total_batches')
        if not total:
            return '-'
        status = t.get('status')
        if (isinstance(status, managed_job_state.ManagedJobStatus) and
                status == managed_job_state.ManagedJobStatus.WINDING_DOWN):
            return 'Winding down'
        completed = t.get('batch_completed_batches') or 0
        pct = int(completed * 100 / total)
        return f'{pct}% {completed}/{total}'

    columns = [
        'ID',
        'TASK',
        *(['WORKSPACE'] if show_workspace else []),
        'NAME',
        *user_cols,
        'REQUESTED',
        'SUBMITTED',
        'TOT. DURATION',
        'JOB DURATION',
        '#RECOVERIES',
        'STATUS',
        'PROGRESS',
        'POOL',
    ]
    if show_all:
        # TODO: move SCHED. STATE to a separate flag (e.g. --debug)
        columns += [
            'WORKER_CLUSTER',
            'WORKER_JOB_ID',
            'STARTED',
            'INFRA',
            'RESOURCES',
            'SCHED. STATE',
            'DETAILS',
            'GIT_COMMIT',
        ]
    if tasks_have_k8s_user:
        columns.insert(0, 'USER')
    job_table = log_utils.create_table(columns)

    status_counts: dict[str, int] = collections.defaultdict(int)
    if job_status_counts:
        for status_value, count in job_status_counts.items():
            status = managed_job_state.ManagedJobStatus(status_value)
            if not status.is_terminal():
                status_counts[status_value] = count
    else:
        for task in tasks:
            if not task['status'].is_terminal():
                status_counts[task['status'].value] += 1

    all_tasks = tasks
    if max_jobs is not None:
        all_tasks = tasks[:max_jobs]
    jobs = collections.defaultdict(list)
    for task in all_tasks:
        # The tasks within the same job_id are already sorted
        # by the task_id.
        jobs[get_hash(task)].append(task)

    def generate_details(details: str | None,
                         failure_reason: str | None) -> str:
        if details is not None:
            return details
        if failure_reason is not None:
            return f'Failure: {failure_reason}'
        return '-'

    def get_user_column_values(task: dict[str, Any]) -> list[str]:
        user_values: list[str] = []
        if show_user:
            user_name = '-'  # default value

            task_user_name = task.get('user_name', None)
            task_user_hash = task.get('user_hash', None)
            if task_user_name is not None:
                user_name = task_user_name
            elif task_user_hash is not None:
                # Fallback to the user hash if we are somehow missing the name.
                user_name = task_user_hash

            user_values = [user_name]

            if show_all:
                user_values.append(
                    task_user_hash if task_user_hash is not None else '-')

        return user_values

    for job_hash, job_tasks in jobs.items():
        if show_all:
            schedule_state = job_tasks[0]['schedule_state']
        workspace = job_tasks[0].get('workspace',
                                     constants.SKYPILOT_DEFAULT_WORKSPACE)

        if len(job_tasks) > 1:
            # Aggregate the tasks into a new row in the table.
            job_name = job_tasks[0]['job_name']
            job_duration = 0
            submitted_at = None
            end_at: int | None = 0
            recovery_cnt = 0
            managed_job_status, current_task_id = _get_job_status_from_tasks(
                job_tasks)
            for task in job_tasks:
                job_duration += task['job_duration']
                if task['submitted_at'] is not None:
                    if (submitted_at is None or
                            submitted_at > task['submitted_at']):
                        submitted_at = task['submitted_at']
                if task['end_at'] is not None:
                    if end_at is not None and end_at < task['end_at']:
                        end_at = task['end_at']
                else:
                    end_at = None
                recovery_cnt += task['recovery_count']

            job_duration = log_utils.readable_time_duration(0,
                                                            job_duration,
                                                            absolute=True)
            submitted = log_utils.readable_time_duration(submitted_at)
            total_duration = log_utils.readable_time_duration(submitted_at,
                                                              end_at,
                                                              absolute=True)

            status_str = managed_job_status.colored_str()
            if not managed_job_status.is_terminal():
                status_str += f' (task: {current_task_id})'

            user_values = get_user_column_values(job_tasks[0])

            pool = job_tasks[0].get('pool')
            if pool is None:
                pool = '-'

            # Add worker information if job is assigned to a worker
            job_id = job_hash[1] if tasks_have_k8s_user else job_hash
            # job_id is now always an integer, use it to look up worker
            if job_id in job_to_worker and pool != '-':
                pool = f'{pool} (worker={job_to_worker[job_id]})'

            job_values = [
                job_id,
                '',
                *([''] if show_workspace else []),
                job_name,
                *user_values,
                '-',
                submitted,
                total_duration,
                job_duration,
                recovery_cnt,
                status_str,
                _fmt_batch_progress(job_tasks),
                pool,
            ]
            if show_all:
                details = job_tasks[current_task_id].get('details')
                failure_reason = job_tasks[current_task_id]['failure_reason']
                job_values.extend([
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    job_tasks[0]['schedule_state'],
                    generate_details(details, failure_reason),
                    job_tasks[0].get('metadata', {}).get('git_commit', '-'),
                ])
            if tasks_have_k8s_user:
                job_values.insert(0, job_tasks[0].get('user', '-'))
            job_table.add_row(job_values)

        # Check if this is a job group with auxiliary tasks.
        # is_primary_in_job_group: True/False for job groups, None otherwise.
        # We show [P] markers only for job groups that have auxiliary tasks.
        has_auxiliary_tasks = any(
            t.get('is_primary_in_job_group') is False for t in job_tasks)

        for task in job_tasks:
            # The job['job_duration'] is already calculated in
            # dump_managed_job_queue().
            job_duration = log_utils.readable_time_duration(
                0, task['job_duration'], absolute=True)
            submitted = log_utils.readable_time_duration(task['submitted_at'])
            user_values = get_user_column_values(task)
            task_workspace = '-' if len(job_tasks) > 1 else workspace
            pool = task.get('pool')
            if pool is None:
                pool = '-'

            # Add worker information if task is assigned to a worker
            task_job_id = task['job_id']
            if task_job_id in job_to_worker and pool != '-':
                pool = f'{pool} (worker={job_to_worker[task_job_id]})'

            # Add [P] marker for primary tasks in job groups with auxiliaries
            task_name = task['task_name']
            if has_auxiliary_tasks and task.get('is_primary_in_job_group'):
                task_name = f'{task_name} [P]'

            values = [
                task['job_id'] if len(job_tasks) == 1 else ' \u21B3',
                task['task_id'] if len(job_tasks) > 1 else '-',
                *([task_workspace] if show_workspace else []),
                task_name,
                *user_values,
                task['resources'],
                # SUBMITTED
                submitted if submitted != '-' else submitted,
                # TOT. DURATION
                log_utils.readable_time_duration(task['submitted_at'],
                                                 task['end_at'],
                                                 absolute=True),
                job_duration,
                task['recovery_count'],
                task['status'].colored_str(),
                _fmt_batch_progress(task),
                pool,
            ]
            if show_all:
                # schedule_state is only set at the job level, so if we have
                # more than one task, only display on the aggregated row.
                schedule_state = (task['schedule_state']
                                  if len(job_tasks) == 1 else '-')
                infra_str = task.get('infra')
                if infra_str is None:
                    cloud = task.get('cloud')
                    if cloud is None:
                        # Backward compatibility for old jobs controller without
                        # cloud info returned, we parse it from the cluster
                        # resources
                        # TODO(zhwu): remove this after 0.12.0
                        cloud = task['cluster_resources'].split('(')[0].split(
                            'x')[-1]
                        task['cluster_resources'] = task[
                            'cluster_resources'].replace(f'{cloud}(',
                                                         '(').replace(
                                                             'x ', 'x')
                    region = task['region']
                    zone = task.get('zone')
                    if cloud == '-':
                        cloud = None
                    if region == '-':
                        region = None
                    if zone == '-':
                        zone = None
                    infra_str = infra_utils.InfraInfo(cloud, region,
                                                      zone).formatted_str()
                values.extend([
                    task.get('current_cluster_name', '-'),
                    task.get('job_id_on_pool_cluster', '-'),
                    # STARTED
                    log_utils.readable_time_duration(task['start_at']),
                    infra_str,
                    task['cluster_resources'],
                    schedule_state,
                    generate_details(task.get('details'),
                                     task['failure_reason']),
                ])

                values.append(task.get('metadata', {}).get('git_commit', '-'))
            if tasks_have_k8s_user:
                values.insert(0, task.get('user', '-'))
            job_table.add_row(values)

        if len(job_tasks) > 1:
            # Add a row to separate the aggregated job from the next job.
            job_table.add_row([''] * len(columns))
    status_str = ', '.join([
        f'{count} {status}' for status, count in sorted(status_counts.items())
    ])
    if status_str:
        status_str = f'In progress tasks: {status_str}'
    else:
        status_str = 'No in-progress managed jobs.'
    output = status_str
    if str(job_table):
        output += f'\n{job_table}'
    if return_rows:
        return job_table.rows
    return output
