"""Read repository for managed-job file-mount blob references."""

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.ext import asyncio as sql_async

from sky.jobs.state_schema import job_info_table
from sky.jobs.state_schema import spot_table
from sky.jobs.state_storage import db_manager as _db_manager
from sky.jobs.status_types import ManagedJobStatus
from sky.utils.db import retries as db_retries


def get_active_file_mounts_blob_ids() -> set[str]:
    """Return blob ids referenced by jobs still in a non-terminal state.

    Used by the API server's blob GC so that a blob is not reclaimed while
    any managed job (including queued / recovering / winding-down jobs) still
    needs its contents.
    """
    engine = _db_manager.get_engine()
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
    query = sqlalchemy.select(
        job_info_table.c.file_mounts_blob_id).distinct().where(
            sqlalchemy.and_(
                job_info_table.c.file_mounts_blob_id.is_not(None),
                job_info_table.c.spot_job_id.in_(non_terminal_job_ids_subquery),
            ))
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    return {row[0] for row in rows if row[0] is not None}


@db_retries.retry_async
async def get_file_mounts_blob_id_async(job_id: int) -> str | None:
    """Return the file_mounts_blob_id persisted for a job, if any."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        row = (await session.execute(
            sqlalchemy.select(job_info_table.c.file_mounts_blob_id).where(
                job_info_table.c.spot_job_id == job_id))).fetchone()
        if row is None:
            return None
        return row[0]


# Preserve reflection and function pickle lookup through the historical facade.
get_active_file_mounts_blob_ids.__module__ = 'sky.jobs.state'
get_file_mounts_blob_id_async.__module__ = 'sky.jobs.state'
get_file_mounts_blob_id_async.__wrapped__.__module__ = 'sky.jobs.state'
