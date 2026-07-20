"""Managed-jobs queue query and formatting helpers."""
import collections
from collections.abc import Iterable
import enum
import time
import typing
from typing import Any, Literal, Optional

from sky import backends
from sky import global_user_state
from sky.adaptors import common as adaptors_common
from sky.dag import DagExecution
from sky.jobs import queue_table
from sky.jobs import state as managed_job_state
from sky.jobs.naming import generate_managed_job_cluster_name
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.skylet import constants
# Keep the historical patch surface used by table-formatting callers.
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
    accessible_workspaces: list[str] | None = None,
    job_ids: list[int] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    page: int | None = None,
    limit: int | None = None,
    user_hashes: list[str | None] | None = None,
    statuses: list[str] | None = None,
    fields: list[str] | None = None,
    sort_by: str | None = None,
    sort_order: str | None = None,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
) -> str:
    return message_utils.encode_payload(
        get_managed_job_queue(skip_finished, accessible_workspaces, job_ids,
                              workspace_match, name_match, pool_match, page,
                              limit, user_hashes, statuses, fields, sort_by,
                              sort_order, submitted_after, submitted_before))


def _update_fields(fields: list[str],) -> tuple[list[str], bool]:
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


def _cluster_handle_not_required(fields: list[str]) -> bool:
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
                        job: dict[str, Any],
                        highest_blocking_priority: int,
                        recovery_reason: str | None = None,
                        pending_reason: str | None = None) -> None:
    """Add details about schedule state, failures, and pending reasons."""
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
    elif pending_reason:
        job['details'] = ' '.join(pending_reason.split())
    else:
        job['details'] = None


def _populate_job_records_from_handles(
        jobs_with_handle: list[dict[str, Any]]) -> None:
    """Populate the job records from the handles."""
    for job_with_handle in jobs_with_handle:
        _populate_job_record_from_handle(
            job=job_with_handle['job'],
            cluster_name=job_with_handle['cluster_name'],
            handle=job_with_handle['handle'])


def _populate_job_record_from_handle(
        *, job: dict[str, Any], cluster_name: str,
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
    accessible_workspaces: list[str] | None = None,
    job_ids: list[int] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    page: int | None = None,
    limit: int | None = None,
    user_hashes: list[str | None] | None = None,
    statuses: list[str] | None = None,
    fields: list[str] | None = None,
    sort_by: str | None = None,
    sort_order: str | None = None,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> dict[str, Any]:
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

    cluster_name_to_handle = {}
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

    # Batch-fetch why a job is recovering or still pending. Both reasons are
    # persisted as job events and fetched together to stay off the per-job
    # path. `job['status']` is already stringified above.
    recovery_reasons: dict[int, str] = {}
    pending_reasons: dict[int, str] = {}
    if not fields or 'details' in fields:
        recovering_job_ids = [
            job['job_id'] for job in jobs if job['status'] ==
            managed_job_state.ManagedJobStatus.RECOVERING.value
        ]
        pending_job_ids = [
            job['job_id']
            for job in jobs
            if job['status'] == managed_job_state.ManagedJobStatus.PENDING.value
        ]
        recovery_reasons, pending_reasons = (
            managed_job_state.get_latest_recovery_and_pending_reasons(
                recovering_job_ids, pending_job_ids))

    for job in jobs:
        if not fields or 'details' in fields:
            _format_job_details(
                job=job,
                highest_blocking_priority=highest_blocking_priority,
                recovery_reason=recovery_reasons.get(job['job_id']),
                pending_reason=pending_reasons.get(job['job_id']))

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
    jobs: list[dict[str, Any]],
    workspace_match: str | None,
    name_match: str | None,
    pool_match: str | None,
    page: int | None,
    limit: int | None,
    user_match: str | None = None,
    enable_user_match: bool = False,
    statuses: list[str] | None = None,
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
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

    def _pattern_matches(job: dict[str, Any], key: str,
                         pattern: str | None) -> bool:
        if pattern is None:
            return True
        if key not in job:
            return False
        value = job[key]
        if not value:
            return False
        return pattern in str(value)

    def _handle_page_and_limit(
        result: list[dict[str, Any]],
        page: int | None,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        if page is None and limit is None:
            return result
        assert page is not None and limit is not None, (page, limit)
        # page starts from 1
        start = (page - 1) * limit
        end = min(start + limit, len(result))
        return result[start:end]

    status_counts: dict[str, int] = collections.defaultdict(int)
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
) -> tuple[list[dict[str, Any]], int, ManagedJobQueueResultType, int, dict[
        str, int]]:
    """Load job queue from json string."""
    result = message_utils.decode_payload(payload)
    result_type = ManagedJobQueueResultType.DICT
    status_counts: dict[str, int] = {}
    if isinstance(result, dict):
        jobs: list[dict[str, Any]] = result['jobs']
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


@typing.overload
def format_job_table(
    tasks: list[dict[str, Any]],
    show_all: bool,
    show_user: bool,
    return_rows: Literal[False] = False,
    pool_status: list[dict[str, Any]] | None = None,
    max_jobs: int | None = None,
    job_status_counts: dict[str, int] | None = None,
) -> str:
    ...


@typing.overload
def format_job_table(
    tasks: list[dict[str, Any]],
    show_all: bool,
    show_user: bool,
    return_rows: Literal[True],
    pool_status: list[dict[str, Any]] | None = None,
    max_jobs: int | None = None,
    job_status_counts: dict[str, int] | None = None,
) -> list[list[str]]:
    ...


def format_job_table(
    tasks: list[dict[str, Any]],
    show_all: bool,
    show_user: bool,
    return_rows: bool = False,
    pool_status: list[dict[str, Any]] | None = None,
    max_jobs: int | None = None,
    job_status_counts: dict[str, int] | None = None,
) -> str | list[list[str]]:
    """Return managed jobs as a formatted string or table rows."""
    # Preserve the historical queue_utils.log_utils monkeypatch point.
    queue_table.log_utils = log_utils
    return queue_table.format_job_table(
        tasks,
        show_all,
        show_user,
        return_rows=return_rows,
        pool_status=pool_status,
        max_jobs=max_jobs,
        job_status_counts=job_status_counts,
    )


def decode_managed_job_protos(
    job_protos: Iterable['managed_jobsv1_pb2.ManagedJobInfo']
) -> list[dict[str, Any]]:
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
        job_proto: 'managed_jobsv1_pb2.ManagedJobInfo') -> dict[str, Any]:
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
