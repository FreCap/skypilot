"""Read-only query and response projection for managed jobs."""
import json
import time
import typing
from typing import Any, Optional

import sqlalchemy
from sqlalchemy import orm

from sky import sky_logging
from sky.jobs import state_storage
from sky.jobs.state_schema import batch_state_table
from sky.jobs.state_schema import job_info_table
from sky.jobs.state_schema import spot_table
from sky.jobs.status_types import ManagedJobScheduleState
from sky.jobs.status_types import ManagedJobStatus
from sky.utils import common_utils

if typing.TYPE_CHECKING:
    from sqlalchemy.engine import row

logger = sky_logging.init_logger('sky.jobs.state')
_db_manager = state_storage.db_manager

# Subquery that aggregates batch_state into per-job progress counts.
# Used by jobs-queue queries to supply batch_total_batches and
# batch_completed_batches without denormalized columns on job_info.
_batch_progress_subquery = sqlalchemy.select(
    batch_state_table.c.job_id,
    sqlalchemy.func.count().label(  # pylint: disable=not-callable
        'batch_total_batches'),
    sqlalchemy.func.count(  # pylint: disable=not-callable
        sqlalchemy.case((batch_state_table.c.status
                         == 'COMPLETED', 1),)).label('batch_completed_batches'),
).group_by(batch_state_table.c.job_id).subquery('batch_progress')


def _get_jobs_dict(r: 'row.RowMapping') -> dict[str, Any]:
    # WARNING: If you update these you may also need to update GetJobTable in
    # the skylet ManagedJobsServiceImpl.
    return {
        '_job_id': r.get('job_id'),  # from spot table
        '_task_name': r.get('job_name'),  # deprecated, from spot table
        'resources': r.get('resources'),
        'submitted_at': r.get('submitted_at'),
        'status': r.get('status'),
        'run_timestamp': r.get('run_timestamp'),
        'start_at': r.get('start_at'),
        'end_at': r.get('end_at'),
        'last_recovered_at': r.get('last_recovered_at'),
        'recovery_count': r.get('recovery_count'),
        'job_duration': r.get('job_duration'),
        'failure_reason': r.get('failure_reason'),
        'job_id': r.get(spot_table.c.spot_job_id
                       ),  # ambiguous, use table.column
        'task_id': r.get('task_id'),
        'task_name': r.get('task_name'),
        'specs': r.get('specs'),
        'local_log_file': r.get('local_log_file'),
        'metadata': r.get('metadata'),
        'links': r.get('links'),  # SQLAlchemy JSON type, already parsed
        # columns from job_info table (some may be None for legacy jobs)
        '_job_info_job_id': r.get(job_info_table.c.spot_job_id
                                 ),  # ambiguous, use table.column
        'job_name': r.get('name'),  # from job_info table
        'schedule_state': r.get('schedule_state'),
        'controller_pid': r.get('controller_pid'),
        'controller_pid_started_at': r.get('controller_pid_started_at'),
        'controller_instance_id': r.get('controller_instance_id'),
        'controller_generation': r.get('controller_generation'),
        # the _path columns are for backwards compatibility, use the _content
        # columns instead
        'dag_yaml_path': r.get('dag_yaml_path'),
        'env_file_path': r.get('env_file_path'),
        'dag_yaml_content': r.get('dag_yaml_content'),
        'env_file_content': r.get('env_file_content'),
        'config_file_content': r.get('config_file_content'),
        'user_hash': r.get('user_hash'),
        'workspace': r.get('workspace'),
        'priority': r.get('priority'),
        'priority_class': r.get('priority_class'),
        'entrypoint': r.get('entrypoint'),
        'original_user_yaml_path': r.get('original_user_yaml_path'),
        'original_user_yaml_content': r.get('original_user_yaml_content'),
        'pool': r.get('pool'),
        'current_cluster_name': r.get('current_cluster_name'),
        'job_id_on_pool_cluster': r.get('job_id_on_pool_cluster'),
        'pool_hash': r.get('pool_hash'),
        # Whether this task is primary (True) or auxiliary (False) in a job
        # group. NULL for non-job-group jobs.
        'is_primary_in_job_group': r.get('is_primary_in_job_group'),
        # Execution mode: 'parallel' (job group) or 'serial' (pipeline/single)
        'execution': r.get('execution'),
        # Infrastructure columns for filtering/sorting
        'cloud': r.get('cloud'),
        'region': r.get('region'),
        'zone': r.get('zone'),
        # Batch progress columns
        'is_batch': r.get('is_batch'),
        'batch_total_batches': r.get('batch_total_batches'),
        'batch_completed_batches': r.get('batch_completed_batches'),
        'node_names': common_utils.get_display_node_names(r.get('node_names')),
    }


def _map_response_field_to_db_column(field: str):
    """Map the response field name to an actual SQLAlchemy ColumnElement.

    This ensures we never pass plain strings to SQLAlchemy 2.0 APIs like
    Select.with_only_columns().
    """
    # Explicit aliases differing from actual DB column names
    alias_mapping = {
        '_job_id': spot_table.c.job_id,  # spot.job_id
        '_task_name': spot_table.c.job_name,  # deprecated, from spot table
        'job_id': spot_table.c.spot_job_id,  # public job id -> spot.spot_job_id
        '_job_info_job_id': job_info_table.c.spot_job_id,
        'job_name': job_info_table.c.name,  # public job name -> job_info.name
        # Batch progress from batch_state aggregation subquery
        'batch_total_batches': _batch_progress_subquery.c.batch_total_batches,
        'batch_completed_batches':
            _batch_progress_subquery.c.batch_completed_batches,
    }
    if field in alias_mapping:
        return alias_mapping[field]

    # Try direct match on the `spot` table columns
    if field in spot_table.c:
        return spot_table.c[field]

    # Try direct match on the `job_info` table columns
    if field in job_info_table.c:
        return job_info_table.c[field]

    raise ValueError(f'Unknown field: {field}')


def get_managed_jobs_total() -> int:
    """Get the total number of managed jobs."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(spot_table)).fetchone()
        return result[0] if result else 0


def build_managed_jobs_with_filters_no_status_query(
    fields: list[str] | None = None,
    job_ids: list[int] | None = None,
    accessible_workspaces: list[str] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    user_hashes: list[str | None] | None = None,
    skip_finished: bool = False,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
    count_only: bool = False,
    count_unique_jobs: bool = False,
    status_count: bool = False,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> sqlalchemy.Select:
    """Build a query to get managed jobs from the database with filters.

    status_expr is an optional SQLAlchemy expression used in place of the raw
    ``spot.status`` column whenever a user-facing status is needed (the
    status-count grouping column). It lets a caller surface a refined status
    (e.g. a plugin override) without changing the underlying column. When None,
    the raw ``spot.status`` column is used.

    submitted_after / submitted_before are epoch seconds (matching the
    ``submitted_at`` column) and restrict the result to jobs submitted within
    the inclusive window. A still-active job that hasn't been submitted yet
    (NULL ``submitted_at``) is treated as submitted now, so it is kept or
    dropped by the window like a job submitted at the current moment; a
    terminal job that never got a ``submitted_at`` is excluded from the window.
    """
    # Join spot and job_info tables to get the job name for each task.
    # We use LEFT OUTER JOIN mainly for backward compatibility, as for an
    # existing controller before #1982, the job_info table may not exist,
    # and all the managed jobs created before will not present in the
    # job_info.
    # Note: we will get the user_hash here, but don't try to call
    # global_user_state.get_user() on it. This runs on the controller, which may
    # not have the user info. Prefer to do it on the API server side.
    if count_unique_jobs:
        # Count unique jobs (by spot_job_id), not tasks
        query = sqlalchemy.select(
            sqlalchemy.func.count(  # pylint: disable=not-callable
                sqlalchemy.distinct(spot_table.c.spot_job_id)).label('count'))
    elif count_only:
        query = sqlalchemy.select(sqlalchemy.func.count().label('count'))  # pylint: disable=not-callable
    elif status_count:
        status_col = (status_expr
                      if status_expr is not None else spot_table.c.status)
        query = sqlalchemy.select(status_col.label('status'),
                                  sqlalchemy.func.count().label('count'))  # pylint: disable=not-callable
    else:
        query = sqlalchemy.select(
            spot_table,
            job_info_table,
            _batch_progress_subquery.c.batch_total_batches,
            _batch_progress_subquery.c.batch_completed_batches,
        )
    query = query.select_from(
        spot_table.outerjoin(
            job_info_table,
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id).outerjoin(
                _batch_progress_subquery,
                spot_table.c.spot_job_id == _batch_progress_subquery.c.job_id))
    if skip_finished:
        # Filter out finished jobs at the DB level. If a multi-task job is
        # partially finished, include all its tasks. We do this by first
        # selecting job_ids that have at least one non-terminal task, then
        # restricting the main query to those job_ids.
        terminal_status_values = [
            s.value for s in ManagedJobStatus.terminal_statuses()
        ]
        non_terminal_job_ids_subquery = (sqlalchemy.select(
            spot_table.c.spot_job_id).where(
                sqlalchemy.or_(
                    spot_table.c.status.is_(None),
                    sqlalchemy.not_(
                        spot_table.c.status.in_(terminal_status_values)),
                )).distinct())
        query = query.where(
            spot_table.c.spot_job_id.in_(non_terminal_job_ids_subquery))
    if not count_only and not status_count and fields:
        # Resolve requested field names to explicit ColumnElements from
        # the joined tables.
        selected_columns = [_map_response_field_to_db_column(f) for f in fields]
        query = query.with_only_columns(*selected_columns)
    if job_ids is not None:
        query = query.where(spot_table.c.spot_job_id.in_(job_ids))
    if accessible_workspaces is not None:
        query = query.where(
            job_info_table.c.workspace.in_(accessible_workspaces))
    if workspace_match is not None:
        query = query.where(
            job_info_table.c.workspace.like(f'%{workspace_match}%'))
    if name_match is not None:
        query = query.where(job_info_table.c.name.like(f'%{name_match}%'))
    if pool_match is not None:
        query = query.where(job_info_table.c.pool.like(f'%{pool_match}%'))
    if user_hashes is not None:
        query = query.where(job_info_table.c.user_hash.in_(user_hashes))
    if submitted_after is not None or submitted_before is not None:
        # submitted_at is NULL until a job leaves PENDING (it is set at
        # STARTING). For a still-active job that just means "not submitted
        # yet", so treat it as submitted "now". A terminal job with no
        # submitted_at never started (cancelled/failed before STARTING) and
        # has no submission time, so leave it NULL to exclude it from the
        # window rather than letting it masquerade as "now".
        terminal_values = [
            s.value for s in ManagedJobStatus.terminal_statuses()
        ]
        effective_submitted_at = sqlalchemy.case(
            (spot_table.c.submitted_at.is_not(None), spot_table.c.submitted_at),
            (sqlalchemy.or_(
                spot_table.c.status.is_(None),
                ~spot_table.c.status.in_(terminal_values)), time.time()),
        )
        if submitted_after is not None:
            query = query.where(effective_submitted_at >= submitted_after)
        if submitted_before is not None:
            query = query.where(effective_submitted_at <= submitted_before)
    return query


def build_managed_jobs_with_filters_query(
    fields: list[str] | None = None,
    job_ids: list[int] | None = None,
    accessible_workspaces: list[str] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    user_hashes: list[str | None] | None = None,
    statuses: list[str] | None = None,
    skip_finished: bool = False,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
    count_only: bool = False,
    count_unique_jobs: bool = False,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> sqlalchemy.Select:
    """Build a query to get managed jobs from the database with filters.

    See build_managed_jobs_with_filters_no_status_query for the meaning of
    status_expr; here it is also used to match the ``statuses`` filter against
    the refined status instead of the raw ``spot.status`` column.
    """
    query = build_managed_jobs_with_filters_no_status_query(
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
        count_only=count_only,
        count_unique_jobs=count_unique_jobs,
        status_expr=status_expr,
    )
    if statuses is not None:
        status_col = (status_expr
                      if status_expr is not None else spot_table.c.status)
        query = query.where(status_col.in_(statuses))
    return query


def get_status_count_with_filters(
    fields: list[str] | None = None,
    job_ids: list[int] | None = None,
    accessible_workspaces: list[str] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    user_hashes: list[str | None] | None = None,
    skip_finished: bool = False,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> dict[str, int]:
    """Get the status count of the managed jobs with filters.

    status_expr, when provided, replaces the raw ``spot.status`` column as the
    grouping key, so counts are bucketed by the refined user-facing status.
    """
    query = build_managed_jobs_with_filters_no_status_query(
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
        status_count=True,
        status_expr=status_expr,
    )
    status_col = (status_expr
                  if status_expr is not None else spot_table.c.status)
    query = query.group_by(status_col)
    results: dict[str, int] = {}
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
        for status_value, count in rows:
            # status_value is already a string (enum value)
            results[str(status_value)] = int(count)
    return results


def get_status_counts() -> dict[str, int]:
    """Get count of tasks grouped by ManagedJobStatus.

    This is used by the Prometheus ManagedJobsCollector.
    """
    query = sqlalchemy.select(
        spot_table.c.status,
        sqlalchemy.func.count().label('cnt'),  # pylint: disable=not-callable
    ).group_by(spot_table.c.status)

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    results: dict[str, int] = {}
    for status_value, count in rows:
        results[str(status_value)] = int(count)
    return results


def get_status_counts_by_workspace_user_cloud(
) -> list[tuple[str | None, str | None, str | None, str, int]]:
    """Return task counts grouped by workspace/user/cloud/status.

    Each tuple is (workspace, user_hash, cloud, status, count). NULL values
    are returned as None. Used by the Prometheus collector to emit
    per-workspace/user/cloud labeled gauges. Includes both active and
    terminal statuses — terminal counts on a gauge grow monotonically as
    the DB accumulates rows, which is awkward (a Counter incremented at
    state-transition would be more semantically correct), but operators
    explicitly want success/failure visibility and `delta(...)` over a
    window approximates the per-period rate.

    The join is on (spot, job_info) — spot rows whose job_info parent has
    been deleted are skipped, but spot rows whose job_info has NULL
    workspace/user_hash/cloud (PENDING jobs, pre-workspaces rows) are
    kept with None labels.
    """
    query = sqlalchemy.select(
        job_info_table.c.workspace,
        job_info_table.c.user_hash,
        job_info_table.c.cloud,
        spot_table.c.status,
        sqlalchemy.func.count().label('cnt'),  # pylint: disable=not-callable
    ).select_from(
        spot_table.join(
            job_info_table,
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
        )).group_by(
            job_info_table.c.workspace,
            job_info_table.c.user_hash,
            job_info_table.c.cloud,
            spot_table.c.status,
        )

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    return [(row[0], row[1], row[2], str(row[3]), int(row[4])) for row in rows]


def get_managed_jobs_with_filters(
    fields: list[str] | None = None,
    job_ids: list[int] | None = None,
    accessible_workspaces: list[str] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    user_hashes: list[str | None] | None = None,
    statuses: list[str] | None = None,
    skip_finished: bool = False,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
    page: int | None = None,
    limit: int | None = None,
    sort_by: str | None = None,
    sort_order: str | None = None,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> tuple[list[dict[str, Any]], int]:
    """Get managed jobs from the database with filters.

    status_expr, when provided, is used to match the ``statuses`` filter
    against a refined user-facing status instead of the raw ``spot.status``
    column (see build_managed_jobs_with_filters_no_status_query). The returned
    rows still carry the raw ``status``; callers that want the refined value in
    the result should surface it separately.

    Pagination is by unique jobs (spot_job_id), not by tasks. This means
    if you request page 1 with limit 10, you get all tasks for 10 unique jobs.

    Args:
        sort_by: Field to sort by. Valid values: 'job_id', 'id', 'job_name',
            'name', 'submitted_at', 'status', 'job_duration', 'duration',
            'recovery_count', 'recoveries', 'resources', 'user_hash', 'user',
            'cloud', 'infra'.
        sort_order: Sort direction, 'asc' or 'desc'. Defaults to 'desc'.

    Returns:
        A tuple containing
         - the list of managed jobs (all tasks for the paginated jobs)
         - the total number of unique jobs (not tasks)
    """
    # Column mapping for sorting
    sort_field_map = {
        'job_id': spot_table.c.spot_job_id,
        'id': spot_table.c.spot_job_id,
        'job_name': spot_table.c.job_name,
        'name': spot_table.c.job_name,
        'submitted_at': spot_table.c.submitted_at,
        # Sort by the refined status (status_expr) when provided, so the order
        # matches the displayed/grouped status instead of the raw column.
        'status':
            (status_expr if status_expr is not None else spot_table.c.status),
        'job_duration': spot_table.c.job_duration,
        'duration': spot_table.c.job_duration,
        'recovery_count': spot_table.c.recovery_count,
        'recoveries': spot_table.c.recovery_count,
        'resources': spot_table.c.resources,
        'user_hash': job_info_table.c.user_hash,
        'user': job_info_table.c.user_hash,
        'cloud': job_info_table.c.cloud,
        'infra': job_info_table.c.cloud,  # Sort by cloud for infra
    }

    engine = _db_manager.get_engine()

    # Count unique jobs (by spot_job_id), not tasks
    count_query = build_managed_jobs_with_filters_query(
        fields=None,
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
        count_unique_jobs=True,
        status_expr=status_expr,
    )
    with orm.Session(engine) as session:
        total = session.execute(count_query).fetchone()[0]

    # For pagination, first get the unique job_ids for the current page,
    # then fetch all tasks for those jobs
    if page is not None and limit is not None:
        # Get paginated unique job IDs with ordering
        # Use GROUP BY instead of DISTINCT to allow ORDER BY on different
        # columns (PostgreSQL requires ORDER BY columns to be in SELECT list
        # when using DISTINCT).
        job_ids_subquery = build_managed_jobs_with_filters_query(
            fields=None,
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
            status_expr=status_expr,
        ).with_only_columns(spot_table.c.spot_job_id).group_by(
            spot_table.c.spot_job_id)

        # Apply sorting to pagination query - this determines which jobs appear
        # on each page. Use MAX aggregate for columns not in GROUP BY to ensure
        # PostgreSQL compatibility.
        if sort_by and sort_by in sort_field_map:
            sort_column = sort_field_map[sort_by]
            # Use MAX aggregate for columns that aren't the grouped column
            if sort_column != spot_table.c.spot_job_id:
                sort_column = sqlalchemy.func.max(sort_column)
            if sort_order == 'asc':
                job_ids_subquery = job_ids_subquery.order_by(sort_column.asc())
            else:
                job_ids_subquery = job_ids_subquery.order_by(sort_column.desc())
        else:
            # Default sort: job_id desc (newest first)
            job_ids_subquery = job_ids_subquery.order_by(
                spot_table.c.spot_job_id.desc())

        job_ids_subquery = job_ids_subquery.offset(
            (page - 1) * limit).limit(limit)

        with orm.Session(engine) as session:
            paginated_job_ids = [
                row[0] for row in session.execute(job_ids_subquery).fetchall()
            ]

        if not paginated_job_ids:
            return [], total

        # Now get all tasks for those job IDs
        query = build_managed_jobs_with_filters_query(
            fields=fields,
            job_ids=paginated_job_ids,  # Filter to only paginated jobs
            accessible_workspaces=accessible_workspaces,
            workspace_match=workspace_match,
            name_match=name_match,
            pool_match=pool_match,
            user_hashes=user_hashes,
            statuses=statuses,
            skip_finished=skip_finished,
            status_expr=status_expr,
        )
    else:
        # No pagination - get all jobs
        query = build_managed_jobs_with_filters_query(
            fields=fields,
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
            status_expr=status_expr,
        )

    # Apply sorting
    if sort_by and sort_by in sort_field_map:
        sort_column = sort_field_map[sort_by]
        if sort_order == 'asc':
            query = query.order_by(sort_column.asc(),
                                   spot_table.c.task_id.asc())
        else:
            query = query.order_by(sort_column.desc(),
                                   spot_table.c.task_id.asc())
    else:
        # Default sort: job_id desc, task_id asc
        query = query.order_by(spot_table.c.spot_job_id.desc(),
                               spot_table.c.task_id.asc())
    rows = None
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    jobs = []
    for row in rows:
        job_dict = _get_jobs_dict(row._mapping)  # pylint: disable=protected-access
        if job_dict.get('status') is not None:
            job_dict['status'] = ManagedJobStatus(job_dict['status'])
        if job_dict.get('schedule_state') is not None:
            job_dict['schedule_state'] = ManagedJobScheduleState(
                job_dict['schedule_state'])
        if job_dict.get('job_name') is None:
            job_dict['job_name'] = job_dict.get('task_name')
        if job_dict.get('metadata') is not None:
            job_dict['metadata'] = json.loads(job_dict['metadata'])

        # Add user YAML content for managed jobs.
        job_dict['user_yaml'] = job_dict.get('original_user_yaml_content')
        if job_dict['user_yaml'] is None:
            # Backwards compatibility - try to read from file path
            yaml_path = job_dict.get('original_user_yaml_path')
            if yaml_path:
                try:
                    with open(yaml_path, encoding='utf-8') as f:
                        job_dict['user_yaml'] = f.read()
                except (FileNotFoundError, OSError) as e:
                    job_id = job_dict.get('job_id')
                    if job_id is not None:
                        logger.debug('Failed to read original user YAML for '
                                     f'job {job_id} from {yaml_path}: {e}')
                    else:
                        logger.debug('Failed to read original user YAML from '
                                     f'{yaml_path}: {e}')

        jobs.append(job_dict)
    return jobs, total
