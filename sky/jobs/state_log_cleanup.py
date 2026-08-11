"""Persistence repository for managed-job log cleanup metadata."""

from collections.abc import Collection
import time
from typing import Any

import sqlalchemy
from sqlalchemy import orm

from sky.jobs.state_schema import job_info_table
from sky.jobs.state_schema import spot_table
from sky.jobs.state_storage import db_manager as _db_manager
from sky.jobs.status_types import ManagedJobScheduleState


def get_task_logs_to_clean(
    retention_seconds: int,
    batch_size: int,
    exclude_tasks: Collection[tuple[int, int]] | None = None
) -> list[dict[str, Any]]:
    """Get the logs of job tasks to clean.

    The logs of a task will only cleaned when:
    - the job schedule state is DONE
    - AND the end time of the task is older than the retention period

    Tasks in `exclude_tasks` ((job_id, task_id) pairs) are skipped so a
    caller can page past rows whose cleanup already failed in this pass.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        now = time.time()
        conditions = [
            job_info_table.c.schedule_state ==
            ManagedJobScheduleState.DONE.value,
            spot_table.c.end_at.isnot(None),
            spot_table.c.end_at < (now - retention_seconds),
            spot_table.c.logs_cleaned_at.is_(None),
            # The local log file is set AFTER the task is finished,
            # add this condition to ensure the entire log file has
            # been written.
            spot_table.c.local_log_file.isnot(None),
        ]
        if exclude_tasks:
            conditions.append(
                sqlalchemy.tuple_(spot_table.c.spot_job_id,
                                  spot_table.c.task_id).notin_(
                                      list(exclude_tasks)))
        result = session.execute(
            sqlalchemy.select(
                spot_table.c.spot_job_id,
                spot_table.c.task_id,
                spot_table.c.local_log_file,
            ).select_from(
                spot_table.join(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
                )).where(sqlalchemy.and_(*conditions)).limit(batch_size))
        rows = result.fetchall()
        return [{
            'job_id': row[0],
            'task_id': row[1],
            'local_log_file': row[2]
        } for row in rows]


def get_controller_logs_to_clean(
        retention_seconds: int,
        batch_size: int,
        exclude_job_ids: Collection[int] | None = None) -> list[dict[str, Any]]:
    """Get the controller logs to clean.

    The controller logs will only cleaned when:
    - the job schedule state is DONE
    - AND the end time of the latest task is older than the retention period

    Jobs in `exclude_job_ids` are skipped so a caller can page past rows
    whose cleanup already failed in this pass.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        now = time.time()
        conditions = [
            job_info_table.c.schedule_state ==
            ManagedJobScheduleState.DONE.value,
            spot_table.c.local_log_file.isnot(None),
            job_info_table.c.controller_logs_cleaned_at.is_(None),
        ]
        if exclude_job_ids:
            conditions.append(
                job_info_table.c.spot_job_id.notin_(list(exclude_job_ids)))
        result = session.execute(
            sqlalchemy.select(job_info_table.c.spot_job_id,).select_from(
                job_info_table.join(
                    spot_table,
                    job_info_table.c.spot_job_id == spot_table.c.spot_job_id,
                )).where(sqlalchemy.and_(*conditions)).group_by(
                    job_info_table.c.spot_job_id,
                    job_info_table.c.current_cluster_name,
                ).having(sqlalchemy.func.max(
                    spot_table.c.end_at).isnot(None),).having(
                        sqlalchemy.func.max(spot_table.c.end_at) < (
                            now - retention_seconds)).limit(batch_size))
        rows = result.fetchall()
        return [{'job_id': row[0]} for row in rows]


def set_task_logs_cleaned(tasks: list[tuple[int, int]], logs_cleaned_at: float):
    """Set the task logs cleaned at."""
    if not tasks:
        return
    task_keys = list(dict.fromkeys(tasks))
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.tuple_(spot_table.c.spot_job_id,
                                  spot_table.c.task_id).in_(task_keys)).values(
                                      logs_cleaned_at=logs_cleaned_at))
        session.commit()


def set_controller_logs_cleaned(job_ids: list[int], logs_cleaned_at: float):
    """Set the controller logs cleaned at."""
    if not job_ids:
        return
    job_ids = list(dict.fromkeys(job_ids))
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(job_info_table).where(
                job_info_table.c.spot_job_id.in_(job_ids)).values(
                    controller_logs_cleaned_at=logs_cleaned_at))
        session.commit()


# Preserve reflection and function pickle lookup through the historical facade.
for _cleanup_function in (
        get_task_logs_to_clean,
        get_controller_logs_to_clean,
        set_task_logs_cleaned,
        set_controller_logs_cleaned,
):
    _cleanup_function.__module__ = 'sky.jobs.state'
del _cleanup_function
