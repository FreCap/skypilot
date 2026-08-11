"""Persistence repository for managed-job pool execution metadata."""

from typing import Any

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.ext import asyncio as sql_async

from sky.jobs.state_schema import job_info_table
from sky.jobs.state_schema import spot_table
from sky.jobs.state_storage import db_manager as _db_manager
from sky.utils import common_utils
from sky.utils.db import retries as db_retries


@db_retries.retry
def get_pool_from_job_id(job_id: int) -> str | None:
    """Get the pool from the job id."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        pool = session.execute(
            sqlalchemy.select(job_info_table.c.pool).where(
                job_info_table.c.spot_job_id == job_id)).fetchone()
        return pool[0] if pool else None


@db_retries.retry
def get_pool_and_current_cluster_name(
        job_id: int) -> tuple[str | None, str | None]:
    """Read the pool binding and current pool worker from one job row."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        info = session.execute(
            sqlalchemy.select(
                job_info_table.c.pool,
                job_info_table.c.current_cluster_name).where(
                    job_info_table.c.spot_job_id == job_id)).fetchone()
        if info is None:
            return None, None
        return info[0], info[1]


@db_retries.retry_async
async def get_pool_and_execution_from_job_id_async(
        job_id: int) -> tuple[str | None, str | None]:
    """Get the pool and DAG execution mode from the job id in one query.

    Both columns are fixed at submission time, so they can always be read
    together. Each is None when the job is unknown or its row has no recorded
    value (writers may store an explicit NULL for execution, e.g. legacy code
    paths that predate the column). Callers use execution to decide
    JobGroup-ness ('parallel' == JobGroup) without fetching and re-parsing the
    full DAG YAML.
    """
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(job_info_table.c.pool,
                              job_info_table.c.execution).where(
                                  job_info_table.c.spot_job_id == job_id))
        info = result.fetchone()
        if info is None:
            return None, None
        return info[0], info[1]


def set_current_cluster_name(job_id: int, current_cluster_name: str) -> None:
    """Set the current cluster name for a job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(job_info_table).filter(
            job_info_table.c.spot_job_id == job_id).update(
                {job_info_table.c.current_cluster_name: current_cluster_name})
        session.commit()


@db_retries.retry
def set_job_infra(job_id: int,
                  cloud: str | None = None,
                  region: str | None = None,
                  zone: str | None = None,
                  current_node_names: list[str] | None = None) -> None:
    """Update the infrastructure info for a job.

    This is called after a job is launched to record the cloud/region/zone
    and node names for sorting, filtering, and dashboard display purposes.

    Args:
        job_id: The job ID to update.
        cloud: The cloud provider (e.g., 'GCP', 'AWS').
        region: The region (e.g., 'us-central1').
        zone: The zone (e.g., 'us-central1-a').
        current_node_names: List of current node names (head first) to merge
            into the existing lineage.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        update_values: dict[Any, Any] = {}
        if cloud is not None:
            update_values[job_info_table.c.cloud] = cloud
        if region is not None:
            update_values[job_info_table.c.region] = region
        if zone is not None:
            update_values[job_info_table.c.zone] = zone
        if current_node_names is not None:
            row = session.query(job_info_table.c.node_names).filter(
                job_info_table.c.spot_job_id ==
                job_id).with_for_update().first()
            existing_json = row.node_names if row else None
            node_names = common_utils.merge_node_names_lineage(
                existing_json, current_node_names)
            update_values[job_info_table.c.node_names] = node_names
        if update_values:
            session.query(job_info_table).filter(
                job_info_table.c.spot_job_id == job_id).update(update_values)
            session.commit()


def update_job_full_resources(job_id: int,
                              full_resources_json: dict[str, Any]) -> None:
    """Update the full_resources column for a job.

    This is called after scheduling to set the specific resource that was
    selected from an any_of or ordered list. The update happens within the
    filelock in get_next_cluster_name to ensure atomicity.

    Args:
        job_id: The spot_job_id to update
        full_resources_json: The resolved resource configuration (single
            resource, not any_of/ordered)
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(spot_table).where(
                spot_table.c.spot_job_id == job_id).values(
                    {spot_table.c.full_resources: full_resources_json}))
        session.commit()


async def set_job_id_on_pool_cluster_async(job_id: int,
                                           job_id_on_pool_cluster: int) -> None:
    """Set the job id on the pool cluster for a job."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        await session.execute(
            sqlalchemy.update(job_info_table).
            where(job_info_table.c.spot_job_id == job_id).values({
                job_info_table.c.job_id_on_pool_cluster: job_id_on_pool_cluster
            }))
        await session.commit()


@db_retries.retry
def get_pool_submit_info(job_id: int) -> tuple[str | None, int | None]:
    """Get the cluster name and job id on the pool from the managed job id."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        info = session.execute(
            sqlalchemy.select(
                job_info_table.c.current_cluster_name,
                job_info_table.c.job_id_on_pool_cluster).where(
                    job_info_table.c.spot_job_id == job_id)).fetchone()
        if info is None:
            return None, None
        return info[0], info[1]


@db_retries.retry_async
async def get_pool_submit_info_async(
        job_id: int) -> tuple[str | None, int | None]:
    """Get the cluster name and job id on the pool from the managed job id."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(job_info_table.c.current_cluster_name,
                              job_info_table.c.job_id_on_pool_cluster).where(
                                  job_info_table.c.spot_job_id == job_id))
        info = result.fetchone()
        if info is None:
            return None, None
        return info[0], info[1]


# Preserve reflection and function pickle lookup through the historical facade.
get_pool_from_job_id.__module__ = 'sky.jobs.state'
get_pool_and_current_cluster_name.__module__ = 'sky.jobs.state'
get_pool_and_execution_from_job_id_async.__module__ = 'sky.jobs.state'
set_current_cluster_name.__module__ = 'sky.jobs.state'
set_job_infra.__module__ = 'sky.jobs.state'
update_job_full_resources.__module__ = 'sky.jobs.state'
set_job_id_on_pool_cluster_async.__module__ = 'sky.jobs.state'
get_pool_submit_info.__module__ = 'sky.jobs.state'
get_pool_submit_info_async.__module__ = 'sky.jobs.state'

get_pool_from_job_id.__wrapped__.__module__ = 'sky.jobs.state'
get_pool_and_current_cluster_name.__wrapped__.__module__ = 'sky.jobs.state'
get_pool_and_execution_from_job_id_async.__wrapped__.__module__ = (
    'sky.jobs.state')
set_job_infra.__wrapped__.__module__ = 'sky.jobs.state'
get_pool_submit_info.__wrapped__.__module__ = 'sky.jobs.state'
get_pool_submit_info_async.__wrapped__.__module__ = 'sky.jobs.state'
