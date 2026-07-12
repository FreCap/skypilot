"""Managed-jobs queue query and formatting helpers."""
import collections
import enum
import time
import typing
from typing import (Any, Dict, Iterable, List, Literal, Optional, Set, Tuple,
                    Union)

from sky import backends
from sky import global_user_state
from sky.adaptors import common as adaptors_common
from sky.dag import DagExecution
from sky.jobs import state as managed_job_state
from sky.jobs.naming import generate_managed_job_cluster_name
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.schemas.api import responses
from sky.skylet import constants
from sky.utils import infra_utils
from sky.utils import log_utils
from sky.utils import message_utils
from sky.utils import resources_utils

if typing.TYPE_CHECKING:
    from google.protobuf import descriptor
    from google.protobuf import json_format
    import sqlalchemy

    from sky.schemas.generated import managed_jobsv1_pb2
else:
    json_format = adaptors_common.LazyImport('google.protobuf.json_format')
    descriptor = adaptors_common.LazyImport('google.protobuf.descriptor')

# The response fields for managed jobs that require cluster handle
_CLUSTER_HANDLE_FIELDS = [
    'cluster_resources',
    'cluster_resources_full',
    'cloud',
    'region',
    'zone',
    'infra',
    'accelerators',
    'cluster_name_on_cloud',
    'labels',
    # Network endpoint information (extracted from cluster handle)
    'internal_external_ips',
    'internal_services',
]

# The response fields for managed jobs that are not stored in the database
# These fields will be mapped to the DB fields in the `_update_fields`.
_NON_DB_FIELDS = _CLUSTER_HANDLE_FIELDS + [
    'user_yaml',
    'user_name',
    'details',
    # is_job_group is derived from execution column (execution == 'parallel')
    'is_job_group',
]


class ManagedJobQueueResultType(enum.Enum):
    """The type of the managed job queue result."""
    DICT = 'DICT'
    LIST = 'LIST'


def dump_managed_job_queue(
    skip_finished: bool = False,
    accessible_workspaces: Optional[List[str]] = None,
    job_ids: Optional[List[int]] = None,
    workspace_match: Optional[str] = None,
    name_match: Optional[str] = None,
    pool_match: Optional[str] = None,
    page: Optional[int] = None,
    limit: Optional[int] = None,
    user_hashes: Optional[List[Optional[str]]] = None,
    statuses: Optional[List[str]] = None,
    fields: Optional[List[str]] = None,
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = None,
    submitted_after: Optional[float] = None,
    submitted_before: Optional[float] = None,
) -> str:
    return message_utils.encode_payload(
        get_managed_job_queue(skip_finished, accessible_workspaces, job_ids,
                              workspace_match, name_match, pool_match, page,
                              limit, user_hashes, statuses, fields, sort_by,
                              sort_order, submitted_after, submitted_before))


def _update_fields(fields: List[str],) -> Tuple[List[str], bool]:
    """Update the fields list to include the necessary fields.

    Args:
        fields: The fields to update.

    It will:
    - Add the necessary dependent fields to the list.
    - Remove the fields that are not in the DB.
    - Determine if cluster handle is required.

    Returns:
        A tuple containing the updated fields and a boolean indicating if
        cluster handle is required.
    """
    cluster_handle_required = True
    if _cluster_handle_not_required(fields):
        cluster_handle_required = False
    # Copy the list to avoid modifying the original list
    new_fields = fields.copy()
    # status and job_id are always included
    if 'status' not in new_fields:
        new_fields.append('status')
    if 'job_id' not in new_fields:
        new_fields.append('job_id')
    # user_hash is required if user_name is present
    if 'user_name' in new_fields and 'user_hash' not in new_fields:
        new_fields.append('user_hash')
    if 'job_duration' in new_fields:
        if 'last_recovered_at' not in new_fields:
            new_fields.append('last_recovered_at')
        if 'end_at' not in new_fields:
            new_fields.append('end_at')
    if 'job_name' in new_fields and 'task_name' not in new_fields:
        new_fields.append('task_name')
    if 'details' in new_fields:
        if 'schedule_state' not in new_fields:
            new_fields.append('schedule_state')
        if 'priority' not in new_fields:
            new_fields.append('priority')
        if 'failure_reason' not in new_fields:
            new_fields.append('failure_reason')
    if 'user_yaml' in new_fields:
        if 'original_user_yaml_path' not in new_fields:
            new_fields.append('original_user_yaml_path')
        if 'original_user_yaml_content' not in new_fields:
            new_fields.append('original_user_yaml_content')
    # is_job_group is derived from execution column
    if 'is_job_group' in fields:
        if 'execution' not in new_fields:
            new_fields.append('execution')
    if cluster_handle_required:
        if 'task_name' not in new_fields:
            new_fields.append('task_name')
        if 'current_cluster_name' not in new_fields:
            new_fields.append('current_cluster_name')
    # Remove _NON_DB_FIELDS
    # These fields have been mapped to the DB fields in the above code, so we
    # don't need to include them in the updated fields.
    for field in _NON_DB_FIELDS:
        if field in new_fields:
            new_fields.remove(field)
    if cluster_handle_required:
        # When a job has reached a terminal state, its cluster handle is gone,
        # so infra/resources can no longer be read from the handle. Make sure
        # the last-cached infra ('cloud'/'region'/'zone') and the requested
        # 'resources' string are still selected from the DB so they can be used
        # as a fallback in get_managed_job_queue. These are real DB columns
        # ('cloud'/'region'/'zone' are also in _NON_DB_FIELDS and were removed
        # above, so re-add them here).
        for field in ('cloud', 'region', 'zone', 'resources'):
            if field not in new_fields:
                new_fields.append(field)
    return new_fields, cluster_handle_required


def _cluster_handle_not_required(fields: List[str]) -> bool:
    """Determine if cluster handle is not required.

    Args:
        fields: The fields to check if they contain any of the cluster handle
        fields.

    Returns:
        True if the fields do not contain any of the cluster handle fields,
        False otherwise.
    """
    return not any(field in fields for field in _CLUSTER_HANDLE_FIELDS)


def _format_job_details(*,
                        job: Dict[str, Any],
                        highest_blocking_priority: int,
                        recovery_reason: Optional[str] = None) -> None:
    """Add details about schedule state / backoff / recovery."""
    state_details = None
    if job['schedule_state'] == 'ALIVE_BACKOFF':
        state_details = 'In backoff, waiting for resources'
    elif job['schedule_state'] in ('WAITING', 'ALIVE_WAITING'):
        priority = job.get('priority')
        if (priority is not None and priority < highest_blocking_priority):
            # Job is lower priority than some other blocking job.
            state_details = 'Waiting for higher priority jobs to launch'
        else:
            state_details = 'Waiting for other jobs to launch'

    if state_details and job['failure_reason']:
        job['details'] = f'{state_details} - {job["failure_reason"]}'
    elif state_details:
        job['details'] = state_details
    elif job['failure_reason']:
        job['details'] = f'Failure: {job["failure_reason"]}'
    elif recovery_reason:
        # Surface why a job is recovering (e.g. an OOMKilled pod) so the
        # transient recovery cause is visible in the CLI and dashboard, not
        # just the controller logs. The reason (e.g. from
        # _get_pod_termination_reason) may be multi-line; collapse whitespace
        # so it renders as a single line in the details column.
        flattened = ' '.join(recovery_reason.split())
        detail = f'Recovering: {flattened}'
        # Append an actionable remediation hint when the cause is a known
        # Kubernetes pod failure (e.g. OOMKilled -> raise resources.memory).
        # Guarded on the job's cloud (job['cloud'] is str(cloud), exactly
        # 'Kubernetes') so a non-k8s reason that happens to contain a matched
        # word (e.g. 'Insufficient') is not mis-hinted.
        if str(job.get('cloud', '')).lower() == 'kubernetes':
            hint = kubernetes_utils.match_kubernetes_failure_hint_text(
                flattened)
            if hint is not None:
                detail += f' ({hint})'
        job['details'] = detail
    else:
        job['details'] = None


def _populate_job_records_from_handles(
        jobs_with_handle: List[Dict[str, Any]]) -> None:
    """Populate the job records from the handles."""
    for job_with_handle in jobs_with_handle:
        _populate_job_record_from_handle(
            job=job_with_handle['job'],
            cluster_name=job_with_handle['cluster_name'],
            handle=job_with_handle['handle'])


def _populate_job_record_from_handle(
        *, job: Dict[str, Any], cluster_name: str,
        handle: 'backends.CloudVmRayResourceHandle') -> None:
    """Populate the job record from the handle."""
    del cluster_name
    resources_str_simple, resources_str_full = (
        resources_utils.get_readable_resources_repr(handle,
                                                    simplified_only=False))
    assert resources_str_full is not None
    job['cluster_resources'] = resources_str_simple
    job['cluster_resources_full'] = resources_str_full
    job['cloud'] = str(handle.launched_resources.cloud)
    job['region'] = handle.launched_resources.region
    job['zone'] = handle.launched_resources.zone
    job['infra'] = infra_utils.InfraInfo(
        str(handle.launched_resources.cloud), handle.launched_resources.region,
        handle.launched_resources.zone).formatted_str()
    job['accelerators'] = handle.launched_resources.accelerators
    job['labels'] = handle.launched_resources.labels
    job['cluster_name_on_cloud'] = handle.cluster_name_on_cloud
    # Network endpoint information
    job['internal_external_ips'] = handle.stable_internal_external_ips
    # Extract internal_svc entries if available
    internal_services = None
    if handle.cached_cluster_info is not None:
        internal_services = {}
        for instance_id, instance_infos in (
                handle.cached_cluster_info.instances.items()):
            for info in instance_infos:
                if info.internal_svc is not None:
                    internal_services[instance_id] = info.internal_svc
    job['internal_services'] = internal_services


def get_managed_job_queue(
    skip_finished: bool = False,
    accessible_workspaces: Optional[List[str]] = None,
    job_ids: Optional[List[int]] = None,
    workspace_match: Optional[str] = None,
    name_match: Optional[str] = None,
    pool_match: Optional[str] = None,
    page: Optional[int] = None,
    limit: Optional[int] = None,
    user_hashes: Optional[List[Optional[str]]] = None,
    statuses: Optional[List[str]] = None,
    fields: Optional[List[str]] = None,
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = None,
    submitted_after: Optional[float] = None,
    submitted_before: Optional[float] = None,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> Dict[str, Any]:
    """Get the managed job queue.

    Args:
        skip_finished: Whether to skip finished jobs.
        accessible_workspaces: The accessible workspaces.
        job_ids: The job ids.
        workspace_match: The workspace name to match.
        name_match: The job name to match.
        pool_match: The pool name to match.
        page: The page number.
        limit: The limit number.
        user_hashes: The user hashes.
        statuses: The statuses.
        fields: The fields to include in the response.
        sort_by: The field to sort by.
        sort_order: The sort order ('asc' or 'desc').
        submitted_after: Only include jobs submitted at or after this epoch
            time (seconds).
        submitted_before: Only include jobs submitted at or before this epoch
            time (seconds).

    Returns:
        A dictionary containing the managed job queue.
    """
    cluster_handle_required = True
    updated_fields = None
    # The caller only need to specify the fields in the
    # `class ManagedJobRecord` in `response.py`, and the `_update_fields`
    # function will add the necessary dependent fields to the list, for
    # example, if the caller specifies `['user_name']`, the `_update_fields`
    # function will add `['user_hash']` to the list.
    if fields:
        updated_fields, cluster_handle_required = _update_fields(fields)

    total_no_filter = managed_job_state.get_managed_jobs_total()

    status_counts = managed_job_state.get_status_count_with_filters(
        fields=fields,
        job_ids=job_ids,
        accessible_workspaces=accessible_workspaces,
        workspace_match=workspace_match,
        name_match=name_match,
        pool_match=pool_match,
        user_hashes=user_hashes,
        skip_finished=skip_finished,
        submitted_after=submitted_after,
        submitted_before=submitted_before,
        status_expr=status_expr,
    )

    jobs, total = managed_job_state.get_managed_jobs_with_filters(
        fields=updated_fields,
        job_ids=job_ids,
        accessible_workspaces=accessible_workspaces,
        workspace_match=workspace_match,
        name_match=name_match,
        pool_match=pool_match,
        user_hashes=user_hashes,
        statuses=statuses,
        skip_finished=skip_finished,
        submitted_after=submitted_after,
        submitted_before=submitted_before,
        page=page,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
        status_expr=status_expr,
    )

    if cluster_handle_required:
        # Fetch the cluster name to handle map for managed clusters only.
        cluster_name_to_handle = (
            global_user_state.get_cluster_name_to_handle_map(is_managed=True))

    highest_blocking_priority = constants.MIN_PRIORITY
    if not fields or 'details' in fields:
        # Figure out what the highest priority blocking job is. We need to know
        # in order to determine if other jobs are blocked by a higher priority
        # job, or just by the limited controller resources.
        highest_blocking_priority = (
            managed_job_state.get_managed_jobs_highest_priority())

    jobs_with_handle = []
    for job in jobs:
        if not fields or 'job_duration' in fields:
            end_at = job['end_at']
            if end_at is None:
                end_at = time.time()

            job_submitted_at = job['last_recovered_at'] - job['job_duration']
            if job['status'] == managed_job_state.ManagedJobStatus.RECOVERING:
                # When job is recovering, the duration is exact
                # job['job_duration']
                job_duration = job['job_duration']
            elif job_submitted_at > 0:
                job_duration = end_at - job_submitted_at
            else:
                # When job_start_at <= 0, that means the last_recovered_at
                # is not set yet, i.e. the job is not started.
                job_duration = 0
            job['job_duration'] = job_duration
        job['status'] = job['status'].value
        if not fields or 'schedule_state' in fields:
            job['schedule_state'] = job['schedule_state'].value
        else:
            job['schedule_state'] = None

        if cluster_handle_required:
            cluster_name = job.get('current_cluster_name', None)
            if cluster_name is None:
                cluster_name = generate_managed_job_cluster_name(
                    job['task_name'], job['job_id'])
            handle = cluster_name_to_handle.get(
                cluster_name, None) if cluster_name is not None else None
            if isinstance(handle, backends.CloudVmRayResourceHandle):
                jobs_with_handle.append({
                    'job': job,
                    'handle': handle,
                    'cluster_name': cluster_name,
                })
            else:
                # The cluster handle is no longer available (e.g. the job has
                # reached a terminal state and its cluster has been torn down),
                # so infra/resources can no longer be read from the live
                # handle. Fall back to the last-cached infra
                # ('cloud'/'region'/'zone', persisted on each successful
                # launch/recovery via set_job_infra) and the requested
                # resources string from the jobs DB, so the dashboard/CLI can
                # still show where the job last ran instead of a bare '-'.
                cloud = job.get('cloud')
                region = job.get('region')
                zone = job.get('zone')
                # formatted_str() returns '-' when cloud is None/empty (e.g.
                # legacy jobs without persisted infra).
                job['infra'] = infra_utils.InfraInfo(cloud, region,
                                                     zone).formatted_str()
                job['cloud'] = cloud if cloud else '-'
                job['region'] = region if region else '-'
                job['zone'] = zone if zone else '-'
                # The launched cluster resources string is not persisted, so
                # fall back to the requested resources string from the DB.
                # Only do so if the job was actually launched at least once
                # (i.e. the infra was persisted); otherwise (e.g. PENDING
                # jobs, or jobs that failed before launching) showing the
                # requested resources as the launched resources is
                # misleading.
                cached_resources = job.get('resources') if cloud else None
                job['cluster_resources'] = (cached_resources
                                            if cached_resources else '-')
                job['cluster_resources_full'] = (cached_resources
                                                 if cached_resources else '-')
                job['labels'] = None
                job['cluster_name_on_cloud'] = None
                job['internal_services'] = None
                job['internal_external_ips'] = None

    _populate_job_records_from_handles(jobs_with_handle)

    # Batch-fetch the reason a recovering job is recovering (e.g. an OOMKilled
    # pod), so it can be surfaced in `details`. Scoped to RECOVERING jobs (a
    # small, transient subset) and done in one query to stay off the per-job
    # path. `job['status']` is already stringified above.
    recovery_reasons: Dict[int, str] = {}
    if not fields or 'details' in fields:
        recovering_job_ids = [
            job['job_id'] for job in jobs if job['status'] ==
            managed_job_state.ManagedJobStatus.RECOVERING.value
        ]
        recovery_reasons = managed_job_state.get_latest_recovery_reasons(
            recovering_job_ids)

    for job in jobs:
        if not fields or 'details' in fields:
            _format_job_details(
                job=job,
                highest_blocking_priority=highest_blocking_priority,
                recovery_reason=recovery_reasons.get(job['job_id']))

        # Derive is_job_group from execution column
        job['is_job_group'] = (
            job.get('execution') == DagExecution.PARALLEL.value)

    return {
        'jobs': jobs,
        'total': total,
        'total_no_filter': total_no_filter,
        'status_counts': status_counts
    }


def filter_jobs(
    jobs: List[Dict[str, Any]],
    workspace_match: Optional[str],
    name_match: Optional[str],
    pool_match: Optional[str],
    page: Optional[int],
    limit: Optional[int],
    user_match: Optional[str] = None,
    enable_user_match: bool = False,
    statuses: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], int, Dict[str, int]]:
    """Filter jobs based on the given criteria.

    Args:
        jobs: List of jobs to filter.
        workspace_match: Workspace name to filter.
        name_match: Job name to filter.
        pool_match: Pool name to filter.
        page: Page to filter.
        limit: Limit to filter.
        user_match: User name to filter.
        enable_user_match: Whether to enable user match.
        statuses: Statuses to filter.

    Returns:
        List of filtered jobs
        Total number of jobs
        Dictionary of status counts
    """

    # TODO(hailong): refactor the whole function including the
    # `dump_managed_job_queue()` to use DB filtering.

    def _pattern_matches(job: Dict[str, Any], key: str,
                         pattern: Optional[str]) -> bool:
        if pattern is None:
            return True
        if key not in job:
            return False
        value = job[key]
        if not value:
            return False
        return pattern in str(value)

    def _handle_page_and_limit(
        result: List[Dict[str, Any]],
        page: Optional[int],
        limit: Optional[int],
    ) -> List[Dict[str, Any]]:
        if page is None and limit is None:
            return result
        assert page is not None and limit is not None, (page, limit)
        # page starts from 1
        start = (page - 1) * limit
        end = min(start + limit, len(result))
        return result[start:end]

    status_counts: Dict[str, int] = collections.defaultdict(int)
    result = []
    checks = [
        ('workspace', workspace_match),
        ('job_name', name_match),
        ('pool', pool_match),
    ]
    if enable_user_match:
        checks.append(('user_name', user_match))

    for job in jobs:
        if not all(
                _pattern_matches(job, key, pattern) for key, pattern in checks):
            continue
        status_counts[job['status'].value] += 1
        if statuses:
            if job['status'].value not in statuses:
                continue
        result.append(job)

    total = len(result)

    return _handle_page_and_limit(result, page, limit), total, status_counts


def load_managed_job_queue(
    payload: str
) -> Tuple[List[Dict[str, Any]], int, ManagedJobQueueResultType, int, Dict[
        str, int]]:
    """Load job queue from json string."""
    result = message_utils.decode_payload(payload)
    result_type = ManagedJobQueueResultType.DICT
    status_counts: Dict[str, int] = {}
    if isinstance(result, dict):
        jobs: List[Dict[str, Any]] = result['jobs']
        total: int = result['total']
        status_counts = result.get('status_counts', {})
        total_no_filter: int = result.get('total_no_filter', total)
    else:
        jobs = result
        total = len(jobs)
        total_no_filter = total
        result_type = ManagedJobQueueResultType.LIST

    all_users = global_user_state.get_all_users()
    all_users_map = {user.id: user.name for user in all_users}
    for job in jobs:
        job['status'] = managed_job_state.ManagedJobStatus(job['status'])
        if 'user_hash' in job and job['user_hash'] is not None:
            # Skip jobs that do not have user_hash info.
            # TODO(cooperc): Remove check before 0.12.0.
            job['user_name'] = all_users_map.get(job['user_hash'])
    return jobs, total, result_type, total_no_filter, status_counts


def _get_job_status_from_tasks(
    job_tasks: Union[List[responses.ManagedJobRecord], List[Dict[str, Any]]]
) -> Tuple[managed_job_state.ManagedJobStatus, int]:
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
    job_tasks_for_status: Union[List[responses.ManagedJobRecord],
                                List[Dict[str, Any]]] = (primary_job_tasks
                                                         if primary_job_tasks
                                                         else job_tasks)

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


@typing.overload
def format_job_table(
    tasks: List[Dict[str, Any]],
    show_all: bool,
    show_user: bool,
    return_rows: Literal[False] = False,
    pool_status: Optional[List[Dict[str, Any]]] = None,
    max_jobs: Optional[int] = None,
    job_status_counts: Optional[Dict[str, int]] = None,
) -> str:
    ...


@typing.overload
def format_job_table(
    tasks: List[Dict[str, Any]],
    show_all: bool,
    show_user: bool,
    return_rows: Literal[True],
    pool_status: Optional[List[Dict[str, Any]]] = None,
    max_jobs: Optional[int] = None,
    job_status_counts: Optional[Dict[str, int]] = None,
) -> List[List[str]]:
    ...


def format_job_table(
    tasks: List[Dict[str, Any]],
    show_all: bool,
    show_user: bool,
    return_rows: bool = False,
    pool_status: Optional[List[Dict[str, Any]]] = None,
    max_jobs: Optional[int] = None,
    job_status_counts: Optional[Dict[str, int]] = None,
) -> Union[str, List[List[str]]]:
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
            pool_status: Optional[List[Dict[str, Any]]]) -> Dict[int, int]:
        """Create a mapping from job_id to worker replica_id.

        Jobs that appear on multiple workers (e.g. batch coordinators
        that orchestrate across the whole pool) are excluded — they
        should not display a single ``(worker=N)`` annotation.

        Args:
            pool_status: List of pool status dictionaries with replica_info.

        Returns:
            Dictionary mapping job_id to replica_id (worker ID).
        """
        job_to_worker: Dict[int, int] = {}
        multi_worker_jobs: Set[int] = set()
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

    user_cols: List[str] = []
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

    status_counts: Dict[str, int] = collections.defaultdict(int)
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

    def generate_details(details: Optional[str],
                         failure_reason: Optional[str]) -> str:
        if details is not None:
            return details
        if failure_reason is not None:
            return f'Failure: {failure_reason}'
        return '-'

    def get_user_column_values(task: Dict[str, Any]) -> List[str]:
        user_values: List[str] = []
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
            end_at: Optional[int] = 0
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


def decode_managed_job_protos(
    job_protos: Iterable['managed_jobsv1_pb2.ManagedJobInfo']
) -> List[Dict[str, Any]]:
    """Decode job protos to dicts. Similar to load_managed_job_queue."""
    user_hash_to_user = global_user_state.get_users(
        set(job.user_hash for job in job_protos if job.user_hash))

    jobs = []
    for job_proto in job_protos:
        job_dict = _job_proto_to_dict(job_proto)
        user_hash = job_dict.get('user_hash', None)
        if user_hash is not None:
            # Skip jobs that do not have user_hash info.
            # TODO(cooperc): Remove check before 0.12.0.
            user = user_hash_to_user.get(user_hash, None)
            job_dict['user_name'] = user.name if user is not None else None
        jobs.append(job_dict)
    return jobs


def _job_proto_to_dict(
        job_proto: 'managed_jobsv1_pb2.ManagedJobInfo') -> Dict[str, Any]:
    job_dict = json_format.MessageToDict(
        job_proto,
        always_print_fields_with_no_presence=True,
        # Our API returns fields in snake_case.
        preserving_proto_field_name=True,
        use_integers_for_enums=True)
    for field in job_proto.DESCRIPTOR.fields:
        # Ensure optional fields are present with None values for
        # backwards compatibility with older clients.
        if field.has_presence and field.name not in job_dict:
            job_dict[field.name] = None
        # json_format.MessageToDict is meant for encoding to JSON,
        # and Protobuf encodes int64 as decimal strings in JSON,
        # so we need to convert them back to ints.
        # https://protobuf.dev/programming-guides/json/#field-representation
        if (field.type == descriptor.FieldDescriptor.TYPE_INT64 and
                job_dict.get(field.name) is not None):
            job_dict[field.name] = int(job_dict[field.name])
    job_dict['status'] = managed_job_state.ManagedJobStatus.from_protobuf(
        job_dict['status'])
    # For backwards compatibility, convert schedule_state to a string,
    # as we don't have the logic to handle it in our request
    # encoder/decoder, unlike status.
    schedule_state_enum = (
        managed_job_state.ManagedJobScheduleState.from_protobuf(
            job_dict['schedule_state']))
    job_dict['schedule_state'] = (schedule_state_enum.value
                                  if schedule_state_enum is not None else None)
    # Convert internal_external_ips from list of dicts to list of tuples
    # MessageToDict converts IpPair messages to dicts like
    # {"internal_ip": "...", "external_ip": "..."}, but ManagedJobRecord
    # expects a list of (internal_ip, external_ip) tuples.
    if 'internal_external_ips' in job_dict:
        ip_pairs = job_dict['internal_external_ips']
        if ip_pairs:
            job_dict['internal_external_ips'] = [
                (ip_pair.get('internal_ip', ''), ip_pair.get('external_ip', ''))
                for ip_pair in ip_pairs
            ]
        else:
            job_dict['internal_external_ips'] = None
    # Convert empty internal_services dict to None for consistency
    if 'internal_services' in job_dict and not job_dict['internal_services']:
        job_dict['internal_services'] = None
    return job_dict
