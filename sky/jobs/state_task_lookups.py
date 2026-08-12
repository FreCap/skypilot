"""Task-filtered wait and log lookup persistence gateway."""

import typing
from typing import Any

import sqlalchemy
from sqlalchemy import orm

from sky.jobs.state_schema import job_info_table
from sky.jobs.state_schema import spot_table
from sky.jobs.state_storage import db_manager as _db_manager
from sky.jobs.status_types import JobLogStreamSnapshot
from sky.jobs.status_types import ManagedJobStatus
from sky.utils.db import retries as db_retries


class TaskLogStreamLookup(typing.NamedTuple):
    """One task-filtered log snapshot plus exact task-count context."""
    snapshot: JobLogStreamSnapshot
    local_log_file: str | None
    logs_cleaned_at: float | None
    num_tasks: int


class TaskWaitStatusLookup(typing.NamedTuple):
    """One task-filtered wait snapshot plus exact task-count context."""
    task_id: int | None
    status: ManagedJobStatus | None
    num_tasks: int


# Preserve the historical public and pickle identities exposed by the facade.
TaskLogStreamLookup.__module__ = 'sky.jobs.state'
TaskWaitStatusLookup.__module__ = 'sky.jobs.state'


def _task_wait_job_scope(job_id: int) -> Any:
    task_count = sqlalchemy.select(
        sqlalchemy.func.count(spot_table.c.task_id)  # pylint: disable=not-callable
    ).where(spot_table.c.spot_job_id == job_id).scalar_subquery()
    return sqlalchemy.select(
        sqlalchemy.literal(job_id).label('spot_job_id'),
        task_count.label('num_tasks'),
    ).subquery()


def _task_wait_lookup(row: Any) -> TaskWaitStatusLookup:
    return TaskWaitStatusLookup(
        task_id=row.task_id,
        status=None if row.status is None else ManagedJobStatus(row.status),
        num_tasks=int(row.num_tasks or 0),
    )


@db_retries.retry
def get_task_wait_status_lookup(job_id: int,
                                task_id: int) -> TaskWaitStatusLookup:
    """Return one task-filtered status lookup for the wait polling path.

    ``wait()`` only needs the matched task id, its current status, and the
    exact task count for missing-task classification. Keep that contract on one
    database snapshot without paying for log-routing or log-file metadata.
    """
    job_scope = _task_wait_job_scope(job_id)
    matching_task = sqlalchemy.select(
        spot_table.c.spot_job_id,
        spot_table.c.task_id,
        spot_table.c.status,
    ).where(
        sqlalchemy.and_(
            spot_table.c.spot_job_id == job_id,
            spot_table.c.task_id == task_id,
        )).subquery()
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                matching_task.c.task_id,
                matching_task.c.status,
                job_scope.c.num_tasks,
            ).select_from(
                job_scope.outerjoin(
                    matching_task, matching_task.c.spot_job_id ==
                    job_scope.c.spot_job_id))).fetchone()
    assert row is not None, (job_id, task_id)
    return _task_wait_lookup(row)


@db_retries.retry
def get_task_wait_status_lookup_by_name(job_id: int,
                                        task_name: str) -> TaskWaitStatusLookup:
    """Return one name-filtered status lookup for the wait polling path.

    String task filters historically matched the first task with that name in
    task_id order. Preserve that contract while keeping the wait path on one
    slim database snapshot.
    """
    job_scope = _task_wait_job_scope(job_id)
    matching_task = sqlalchemy.select(
        spot_table.c.spot_job_id,
        spot_table.c.task_id,
        spot_table.c.status,
    ).where(
        sqlalchemy.and_(
            spot_table.c.spot_job_id == job_id,
            spot_table.c.task_name == task_name,
        )).order_by(spot_table.c.task_id.asc()).limit(1).subquery()
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                matching_task.c.task_id,
                matching_task.c.status,
                job_scope.c.num_tasks,
            ).select_from(
                job_scope.outerjoin(
                    matching_task, matching_task.c.spot_job_id ==
                    job_scope.c.spot_job_id))).fetchone()
    assert row is not None, (job_id, task_name)
    return _task_wait_lookup(row)


@db_retries.retry
def get_task_log_stream_lookup(job_id: int,
                               task_id: int) -> TaskLogStreamLookup:
    """Return one task-filtered lookup plus exact task-count context.

    When callers follow a specific task in a JobGroup, the task status and its
    routing context must come from the same database snapshot. Otherwise a
    later task can advance the job-level latest-task status between the two
    reads and make the follower wait on the wrong lifecycle. The same lookup
    also returns the exact task count for this job snapshot so a missing task
    can be classified as either "job missing" or "task ID invalid" without a
    second point query.
    """
    task_count = sqlalchemy.select(
        sqlalchemy.func.count(spot_table.c.task_id)  # pylint: disable=not-callable
    ).where(spot_table.c.spot_job_id == job_id).scalar_subquery()
    job_scope = sqlalchemy.select(
        sqlalchemy.literal(job_id).label('spot_job_id'),
        task_count.label('num_tasks'),
    ).subquery()
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                spot_table.c.task_id,
                spot_table.c.status,
                job_info_table.c.pool,
                job_info_table.c.current_cluster_name,
                job_info_table.c.job_id_on_pool_cluster,
                spot_table.c.task_name,
                spot_table.c.local_log_file,
                spot_table.c.logs_cleaned_at,
                job_scope.c.num_tasks,
            ).select_from(
                job_scope.outerjoin(
                    spot_table,
                    sqlalchemy.and_(
                        spot_table.c.spot_job_id == job_scope.c.spot_job_id,
                        spot_table.c.task_id == task_id,
                    )).outerjoin(
                        job_info_table, job_info_table.c.spot_job_id ==
                        spot_table.c.spot_job_id))).fetchone()
    assert row is not None, (job_id, task_id)
    snapshot = JobLogStreamSnapshot(
        row.task_id,
        (None if row.status is None else ManagedJobStatus(row.status)),
        row.pool,
        row.current_cluster_name,
        row.job_id_on_pool_cluster,
        row.task_name,
    )
    return TaskLogStreamLookup(
        snapshot=snapshot,
        local_log_file=row.local_log_file,
        logs_cleaned_at=row.logs_cleaned_at,
        num_tasks=int(row.num_tasks or 0),
    )


@db_retries.retry
def get_task_log_stream_lookup_by_name(job_id: int,
                                       task_name: str) -> TaskLogStreamLookup:
    """Return one name-filtered lookup plus exact task-count context.

    String task filters historically matched the first task with that name in
    task_id order. Preserve that contract while resolving the match, task
    status, and the exact task count from one database snapshot.
    """
    task_count = sqlalchemy.select(
        sqlalchemy.func.count(spot_table.c.task_id)  # pylint: disable=not-callable
    ).where(spot_table.c.spot_job_id == job_id).scalar_subquery()
    job_scope = sqlalchemy.select(
        sqlalchemy.literal(job_id).label('spot_job_id'),
        task_count.label('num_tasks'),
    ).subquery()
    matching_task = sqlalchemy.select(
        spot_table.c.spot_job_id,
        spot_table.c.task_id,
        spot_table.c.status,
        spot_table.c.task_name,
        spot_table.c.local_log_file,
        spot_table.c.logs_cleaned_at,
    ).where(
        sqlalchemy.and_(
            spot_table.c.spot_job_id == job_id,
            spot_table.c.task_name == task_name,
        )).order_by(spot_table.c.task_id.asc()).limit(1).subquery()
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                matching_task.c.task_id,
                matching_task.c.status,
                job_info_table.c.pool,
                job_info_table.c.current_cluster_name,
                job_info_table.c.job_id_on_pool_cluster,
                matching_task.c.task_name,
                matching_task.c.local_log_file,
                matching_task.c.logs_cleaned_at,
                job_scope.c.num_tasks,
            ).select_from(
                job_scope.outerjoin(
                    matching_task, matching_task.c.spot_job_id ==
                    job_scope.c.spot_job_id).outerjoin(
                        job_info_table, job_info_table.c.spot_job_id ==
                        matching_task.c.spot_job_id))).fetchone()
    assert row is not None, (job_id, task_name)
    snapshot = JobLogStreamSnapshot(
        row.task_id,
        (None if row.status is None else ManagedJobStatus(row.status)),
        row.pool,
        row.current_cluster_name,
        row.job_id_on_pool_cluster,
        row.task_name,
    )
    return TaskLogStreamLookup(
        snapshot=snapshot,
        local_log_file=row.local_log_file,
        logs_cleaned_at=row.logs_cleaned_at,
        num_tasks=int(row.num_tasks or 0),
    )


# Preserve reflection and function pickle lookup through the historical facade.
for _lookup_function in (
        get_task_wait_status_lookup,
        get_task_wait_status_lookup_by_name,
        get_task_log_stream_lookup,
        get_task_log_stream_lookup_by_name,
):
    _lookup_function.__module__ = 'sky.jobs.state'
    if hasattr(_lookup_function, '__wrapped__'):
        _lookup_function.__wrapped__.__module__ = 'sky.jobs.state'
del _lookup_function
