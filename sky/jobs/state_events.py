"""Persistence and retention repository for managed-job events."""
import asyncio
import datetime
from typing import Any

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.ext import asyncio as sql_async

from sky import sky_logging
from sky.jobs import state_storage
from sky.jobs.state_schema import job_events_table
from sky.jobs.state_schema import spot_table
from sky.jobs.status_types import ManagedJobStatus
from sky.utils.db import retries as db_retries

logger = sky_logging.init_logger('sky.jobs.state')
_db_manager = state_storage.db_manager

# 30 days retention for job events
DEFAULT_JOB_EVENT_RETENTION_HOURS = 30 * 24.0
# Run the job event retention daemon every hour
JOB_EVENT_DAEMON_INTERVAL_SECONDS = 3600


def _normalize_timestamp(
        timestamp: datetime.datetime | None = None) -> datetime.datetime:
    """Return a UTC-aware timestamp for managed job event persistence.

    Migration 010 interprets legacy naive job-event timestamps as UTC. Keep the
    same contract for explicit timestamps while ensuring new timestamps carry
    their timezone through PostgreSQL's ``TIMESTAMP WITH TIME ZONE`` binding.
    """
    if timestamp is None:
        return datetime.datetime.now(datetime.timezone.utc)
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=datetime.timezone.utc)
    return timestamp.astimezone(datetime.timezone.utc)


def job_event_insert_statement(
    job_id: int,
    task_id: int | None,
    new_status: ManagedJobStatus,
    reason: str,
    code: str | None = None,
    timestamp: datetime.datetime | None = None,
) -> sqlalchemy.sql.dml.Insert:
    """Build the canonical event insert for a caller-owned transaction."""
    return job_events_table.insert().values(
        spot_job_id=job_id,
        task_id=task_id,
        new_status=new_status.value,
        code=code,
        reason=reason,
        timestamp=_normalize_timestamp(timestamp),
    )


def add_job_event(job_id: int,
                  task_id: int | None,
                  new_status: ManagedJobStatus,
                  reason: str,
                  timestamp: datetime.datetime | None = None) -> None:
    """Add a job event record to the audit log.

    Args:
        job_id: The spot_job_id of the managed job.
        task_id: The task_id within the managed job. If None, adds a
            job-level event that applies to all tasks.
        new_status: The new status being transitioned to. Can be a
            ManagedJobStatus enum.
        reason: A description of why the event occurred.
        timestamp: The timestamp of the event. If None, uses current time.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            job_event_insert_statement(job_id,
                                       task_id,
                                       new_status,
                                       reason,
                                       timestamp=timestamp))
        session.commit()


async def _get_all_task_ids_async(job_id: int) -> list[int]:
    """Get all task IDs for a job (async version)."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(spot_table.c.task_id).where(
                spot_table.c.spot_job_id == job_id).order_by(
                    spot_table.c.task_id.asc()))
        return [row[0] for row in result.fetchall()]


@db_retries.retry_async
async def add_job_event_async(
        job_id: int,
        task_id: int | None,
        new_status: ManagedJobStatus,
        reason: str,
        code: str | None = None,
        timestamp: datetime.datetime | None = None) -> None:
    """Add a job event record to the audit log (async version).

    Args:
        job_id: The spot_job_id of the managed job.
        task_id: The task_id within the managed job. If None, adds a
            job-level event that applies to all tasks.
        new_status: The new status being transitioned to. Can be a
            ManagedJobStatus enum.
        reason: A description of why the event occurred.
        code: Optional error category code for failures.
        timestamp: The timestamp of the event. If None, uses current time.
    """
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        await session.execute(
            job_event_insert_statement(job_id,
                                       task_id,
                                       new_status,
                                       reason,
                                       code=code,
                                       timestamp=timestamp))
        await session.commit()


def get_job_events(job_id: int,
                   task_id: int | None = None,
                   limit: int | None = None) -> list[dict[str, Any]]:
    """Get task events for a managed job.

    Args:
        job_id: The spot_job_id of the managed job.
        task_id: Optional task_id to filter by. If None, returns events
            for all tasks. If specified, returns events for that task plus
            job-level events (where task_id is None).
        limit: Optional limit on number of events to return. If specified,
            returns the most recent N events.

    Returns:
        List of event records, ordered by timestamp descending
        (most recent first) if limit is specified, otherwise ascending.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            job_events_table.c.spot_job_id,
            job_events_table.c.task_id,
            job_events_table.c.new_status,
            job_events_table.c.code,
            job_events_table.c.reason,
            job_events_table.c.timestamp,
        ).where(job_events_table.c.spot_job_id == job_id)

        if task_id is not None:
            # Include events for the specific task AND job-level events
            # (task_id is None)
            query = query.where(
                sqlalchemy.or_(job_events_table.c.task_id == task_id,
                               job_events_table.c.task_id.is_(None)))

        # Order by timestamp descending to get most recent first
        query = query.order_by(job_events_table.c.timestamp.desc())

        if limit is not None:
            query = query.limit(limit)

        rows = session.execute(query).fetchall()
    return [{
        'spot_job_id': row[0],
        'task_id': row[1],
        'new_status': ManagedJobStatus(row[2]),
        'code': row[3],
        'reason': row[4],
        'timestamp': row[5],
    } for row in rows]


def _get_latest_event_reasons(
    job_ids_by_status: dict[ManagedJobStatus, list[int]]
) -> dict[ManagedJobStatus, dict[int, str]]:
    """Return the latest event reason for each requested status and job."""
    reasons: dict[ManagedJobStatus, dict[int, str]] = {
        status: {} for status in job_ids_by_status
    }
    conditions = [
        sqlalchemy.and_(
            job_events_table.c.new_status == status.value,
            job_events_table.c.spot_job_id.in_(job_ids),
        ) for status, job_ids in job_ids_by_status.items() if job_ids
    ]
    if not conditions:
        return reasons

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        ranked_events = sqlalchemy.select(
            job_events_table.c.spot_job_id.label('spot_job_id'),
            job_events_table.c.new_status.label('new_status'),
            job_events_table.c.reason.label('reason'),
            sqlalchemy.func.row_number().over(
                partition_by=(job_events_table.c.spot_job_id,
                              job_events_table.c.new_status),
                order_by=(
                    job_events_table.c.timestamp.desc(),
                    job_events_table.c.id.desc(),
                ),
            ).label('rank'),
        ).where(sqlalchemy.or_(*conditions)).subquery('ranked_job_events')
        rows = session.execute(
            sqlalchemy.select(
                ranked_events.c.spot_job_id,
                ranked_events.c.new_status,
                ranked_events.c.reason,
            ).where(
                sqlalchemy.and_(
                    ranked_events.c.rank == 1,
                    ranked_events.c.reason.is_not(None),
                    ranked_events.c.reason != '',
                ))).fetchall()
    for spot_job_id, new_status, reason in rows:
        reasons[ManagedJobStatus(new_status)][spot_job_id] = reason
    return reasons


def get_latest_recovery_and_pending_reasons(
        recovering_job_ids: list[int],
        pending_job_ids: list[int]) -> tuple[dict[int, str], dict[int, str]]:
    """Return latest recovery and pending reasons in one database query."""
    reasons = _get_latest_event_reasons({
        ManagedJobStatus.RECOVERING: recovering_job_ids,
        ManagedJobStatus.PENDING: pending_job_ids,
    })
    return (reasons[ManagedJobStatus.RECOVERING],
            reasons[ManagedJobStatus.PENDING])


def get_latest_recovery_reasons(job_ids: list[int]) -> dict[int, str]:
    """Return {job_id: reason} for the latest RECOVERING event per job."""
    recovery_reasons, _ = get_latest_recovery_and_pending_reasons(job_ids, [])
    return recovery_reasons


async def cleanup_job_events_with_retention_async(
        retention_hours: float) -> None:
    """Delete job events older than the retention period.

    Args:
        retention_hours: Number of hours to retain job events.
    """
    engine = await _db_manager.get_async_engine()
    cutoff_time = (datetime.datetime.now(datetime.timezone.utc) -
                   datetime.timedelta(hours=retention_hours))

    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.delete(job_events_table).where(
                job_events_table.c.timestamp < cutoff_time))
        count = result.rowcount
        if count > 0:
            logger.debug(f'Deleted {count} job events older than '
                         f'{retention_hours} hours.')
        await session.commit()


async def job_event_retention_daemon():
    """Garbage collect job events periodically."""
    while True:
        logger.info('Running job event retention daemon...')
        try:
            await cleanup_job_events_with_retention_async(
                DEFAULT_JOB_EVENT_RETENTION_HOURS)
        except asyncio.CancelledError:
            logger.info('Job event retention daemon cancelled')
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Error running job event retention daemon: {e}')

        await asyncio.sleep(JOB_EVENT_DAEMON_INTERVAL_SECONDS)
