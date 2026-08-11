"""Read-only managed-job pool projections and resource accounting."""

from typing import Any, Optional

import sqlalchemy
from sqlalchemy import orm

from sky import resources as resources_lib
from sky.jobs.state_schema import job_info_table
from sky.jobs.state_schema import spot_table
from sky.jobs.state_storage import db_manager as _db_manager
from sky.jobs.status_types import ManagedJobStatus


def get_pending_jobs_count_by_pool(pool: str) -> int:
    """Get the count of pending jobs in a pool.

    Pending jobs are distinct managed jobs that are waiting for a worker.
    A single job can contribute multiple task rows while it remains queued, so
    the queue length must count unique job IDs instead of raw task rows. Jobs
    already assigned to a replica must not keep contributing queued demand just
    because later task rows are still pending in the task table.

    Args:
        pool: The pool name

    Returns:
        The number of pending jobs in the pool
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        # pylint: disable=not-callable
        query = sqlalchemy.select(
            sqlalchemy.func.count(
                spot_table.c.spot_job_id.distinct())).select_from(
                    job_info_table.join(
                        spot_table, job_info_table.c.spot_job_id ==
                        spot_table.c.spot_job_id)).where(
                            sqlalchemy.and_(
                                spot_table.c.status ==
                                ManagedJobStatus.PENDING.value,
                                job_info_table.c.pool == pool,
                                job_info_table.c.current_cluster_name.is_(None),
                            ))
        result = session.execute(query).fetchone()
        return result[0] if result else 0


def get_nonterminal_job_ids_by_pool(pool: str,
                                    cluster_name: str | None = None
                                   ) -> list[int]:
    """Get nonterminal job ids in a pool."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            spot_table.c.spot_job_id).distinct().select_from(
                spot_table.outerjoin(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id))
        and_conditions = [
            ~spot_table.c.status.in_([
                status.value for status in ManagedJobStatus.terminal_statuses()
            ]),
            job_info_table.c.pool == pool,
        ]
        if cluster_name is not None:
            and_conditions.append(
                job_info_table.c.current_cluster_name == cluster_name)
        query = query.where(sqlalchemy.and_(*and_conditions)).order_by(
            spot_table.c.spot_job_id.asc())
        rows = session.execute(query).fetchall()
        job_ids = [row[0] for row in rows if row[0] is not None]
        return job_ids


def get_nonterminal_job_counts_by_pool(pool: str) -> dict[str, int]:
    """Get the number of nonterminal jobs per cluster in a pool.

    Returns a dict mapping cluster_name to the count of nonterminal jobs
    running on that cluster. Uses a single GROUP BY query instead of
    per-cluster queries.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            job_info_table.c.current_cluster_name,
            # pylint: disable=not-callable
            sqlalchemy.func.count(
                spot_table.c.spot_job_id.distinct()
            )).select_from(
                spot_table.outerjoin(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id)
            ).where(
                sqlalchemy.and_(
                    ~spot_table.c.status.in_([
                        status.value
                        for status in ManagedJobStatus.terminal_statuses()
                    ]),
                    job_info_table.c.pool == pool,
                )).group_by(job_info_table.c.current_cluster_name)
        rows = session.execute(query).fetchall()
        return {row[0]: row[1] for row in rows if row[0] is not None}


def get_nonterminal_job_status_counts_by_pool(pool: str) -> dict[str, int]:
    """Get nonterminal pool queue-row counts grouped by status.

    The pool dashboard badges historically counted the nonterminal task rows
    returned by ``jobs/queue/v2`` for a pool, not distinct job ids. Keep that
    semantics while replacing the dashboard's second full queue fetch with one
    grouped DB query owned by the pool-status snapshot.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            spot_table.c.status,
            # pylint: disable=not-callable
            sqlalchemy.func.count(spot_table.c.task_id),
        ).select_from(
            spot_table.outerjoin(
                job_info_table, spot_table.c.spot_job_id ==
                job_info_table.c.spot_job_id)).where(
                    sqlalchemy.and_(
                        ~spot_table.c.status.in_([
                            status.value
                            for status in ManagedJobStatus.terminal_statuses()
                        ]),
                        job_info_table.c.pool == pool,
                    )).group_by(spot_table.c.status)
        rows = session.execute(query).fetchall()
        return {row[0]: row[1] for row in rows if row[0] is not None}


def get_nonterminal_job_ids_by_pool_grouped(
        pool: str) -> dict[str | None, list[int]]:
    """Get nonterminal job ids in a pool, grouped by current_cluster_name.

    Equivalent to calling get_nonterminal_job_ids_by_pool once per replica
    (plus once for the pool as a whole), but executed in a single query so
    callers like pool_status avoid the N+1 round-trips that dominate
    dashboard latency when there are many finished jobs.

    Returns:
        A dict mapping current_cluster_name to the list of nonterminal
        spot_job_ids assigned to that cluster. Jobs not yet bound to a
        specific cluster (current_cluster_name IS NULL) are grouped under
        the ``None`` key. Each list is sorted by spot_job_id ascending.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            job_info_table.c.current_cluster_name,
            spot_table.c.spot_job_id,
        ).distinct().select_from(
            spot_table.outerjoin(
                job_info_table, spot_table.c.spot_job_id ==
                job_info_table.c.spot_job_id)).where(
                    sqlalchemy.and_(
                        ~spot_table.c.status.in_([
                            status.value
                            for status in ManagedJobStatus.terminal_statuses()
                        ]),
                        job_info_table.c.pool == pool,
                    )).order_by(spot_table.c.spot_job_id.asc())
        rows = session.execute(query).fetchall()
        result: dict[str | None, list[int]] = {}
        for cluster_name, job_id in rows:
            if job_id is None:
                continue
            result.setdefault(cluster_name, []).append(job_id)
        return result


def _is_any_of_or_ordered(resource_config: dict[str, Any]) -> bool:
    """Check if resource config is heterogeneous (any_of or ordered).

    Args:
        resource_config: Resource configuration dictionary

    Returns:
        True if the config contains 'any_of' or 'ordered' keys, indicating
        heterogeneous resources that haven't been resolved to a specific
        resource yet.
    """
    return 'any_of' in resource_config or 'ordered' in resource_config


def _parse_job_full_resources(
    resource_config: dict[str, Any] | None
) -> Optional['resources_lib.Resources']:
    """Parse one persisted full_resources payload."""
    if resource_config is None:
        return None
    if _is_any_of_or_ordered(resource_config):
        return None
    resources_set = resources_lib.Resources.from_yaml_config(resource_config)
    if len(resources_set) == 0:
        return None
    return next(iter(resources_set))


def _ranked_nonterminal_job_resources(
    *,
    job_ids: set[int] | None = None,
    pool: str | None = None,
) -> Any:
    """Return nonterminal task resources ranked within each Managed Job.

    ``full_resources`` is a PostgreSQL ``json`` column, so it cannot
    participate in ``DISTINCT`` or ``GROUP BY``. Rank rows using scalar task
    identity instead, then let callers select rank one.
    """
    columns = [
        spot_table.c.spot_job_id,
        spot_table.c.full_resources,
        sqlalchemy.func.row_number().over(
            partition_by=spot_table.c.spot_job_id,
            order_by=spot_table.c.task_id.asc(),
        ).label('task_rank'),
    ]
    from_clause = spot_table
    conditions = [
        ~spot_table.c.status.in_(
            [status.value for status in ManagedJobStatus.terminal_statuses()])
    ]
    if job_ids is not None:
        conditions.append(spot_table.c.spot_job_id.in_(job_ids))
    if pool is not None:
        columns.insert(0, job_info_table.c.current_cluster_name)
        from_clause = spot_table.join(
            job_info_table,
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
        )
        conditions.append(job_info_table.c.pool == pool)
    return sqlalchemy.select(*columns).select_from(from_clause).where(
        sqlalchemy.and_(*conditions)).subquery()


def get_pool_worker_used_resources(
        job_ids: set[int]) -> Optional['resources_lib.Resources']:
    """Get the total used resources by running jobs.

    Args:
        job_ids: Set of spot_job_id values to check

    Returns:
        Resources object with summed resources from all running jobs, or None
        if we couldn't parse the resources string for any job.
    """
    if not job_ids:
        return None

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        # Count only live task rows for each job. Multi-task managed jobs keep
        # terminal task history in spot_table, and those historical rows can
        # retain older full_resources values that should not contribute to the
        # worker's active resource usage.
        ranked_resources = _ranked_nonterminal_job_resources(job_ids=job_ids)
        query = sqlalchemy.select(
            ranked_resources.c.spot_job_id,
            ranked_resources.c.full_resources,
        ).where(ranked_resources.c.task_rank == 1)
        rows = session.execute(query).fetchall()

        resource_configs = []
        for row in rows:
            if row[1] is None:
                # We don't have full_resources for this job. We should return
                # none since we can't make any guarantees about what resources
                # are being used.
                return None
            resource_configs.append(row[1])

    # Parse resources dicts into Resources objects and sum them using +.
    # If any job on the worker has an empty resource request, fail closed for
    # resource-aware scheduling by treating the worker as fully occupied.
    total_resources = None
    saw_empty_request = False
    for resource_config in resource_configs:
        parsed = _parse_job_full_resources(resource_config)
        if parsed is None:
            return None
        if parsed.is_empty():
            saw_empty_request = True
            continue
        if total_resources is None:
            total_resources = parsed
        else:
            total_resources = total_resources + parsed
    if saw_empty_request:
        return resources_lib.Resources()
    return total_resources


def get_pool_worker_used_resources_by_cluster(
        pool: str) -> dict[str | None, 'resources_lib.Resources'] | None:
    """Get used resources for all nonterminal jobs in a pool in one query."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        ranked_resources = _ranked_nonterminal_job_resources(pool=pool)
        query = sqlalchemy.select(
            ranked_resources.c.current_cluster_name,
            ranked_resources.c.spot_job_id,
            ranked_resources.c.full_resources,
        ).where(ranked_resources.c.task_rank == 1)
        rows = session.execute(query).fetchall()

    totals: dict[str | None, resources_lib.Resources] = {}
    clusters_with_empty_request: set[str | None] = set()
    for cluster_name, _, resource_config in rows:
        parsed = _parse_job_full_resources(resource_config)
        if parsed is None:
            return None
        if parsed.is_empty():
            clusters_with_empty_request.add(cluster_name)
            continue
        if cluster_name in clusters_with_empty_request:
            continue
        total = totals.get(cluster_name)
        if total is None:
            totals[cluster_name] = parsed
        else:
            combined = total + parsed
            assert combined is not None
            totals[cluster_name] = combined

    for cluster_name in clusters_with_empty_request:
        totals[cluster_name] = resources_lib.Resources()
    return totals


# Preserve reflection and function pickle lookup through the historical facade.
for _pool_query_function in (
        get_pending_jobs_count_by_pool,
        get_nonterminal_job_ids_by_pool,
        get_nonterminal_job_counts_by_pool,
        get_nonterminal_job_status_counts_by_pool,
        get_nonterminal_job_ids_by_pool_grouped,
        _is_any_of_or_ordered,
        _parse_job_full_resources,
        _ranked_nonterminal_job_resources,
        get_pool_worker_used_resources,
        get_pool_worker_used_resources_by_cluster,
):
    _pool_query_function.__module__ = 'sky.jobs.state'
del _pool_query_function
