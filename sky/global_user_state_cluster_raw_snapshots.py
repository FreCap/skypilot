"""Raw cluster-table snapshot read gateway."""

from collections.abc import Callable
from typing import Any, TypeVar

import sqlalchemy

_ManagedSnapshotT = TypeVar('_ManagedSnapshotT')
_RefreshSnapshotT = TypeVar('_RefreshSnapshotT')


def get_cluster_status_fields(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    cluster_in_query_chunk_size: int,
    cluster_names: list[str] | None,
    *,
    exclude_managed_clusters: bool = False,
) -> dict[str, tuple[str | None, int | None]]:
    """Return raw status fields for named or all matching clusters."""
    result: dict[str, tuple[str | None, int | None]] = {}
    if cluster_names == []:
        return result
    engine = engine_getter()
    with session_factory(engine) as session:
        query = session.query(cluster_table.c.name, cluster_table.c.status,
                              cluster_table.c.status_updated_at)
        if exclude_managed_clusters:
            query = query.filter(cluster_table.c.is_managed == int(False))
        if cluster_names is None:
            rows = query.all()
            return {
                row.name: (row.status, row.status_updated_at) for row in rows
            }
        for offset in range(0, len(cluster_names), cluster_in_query_chunk_size):
            batch = cluster_names[offset:offset + cluster_in_query_chunk_size]
            rows = query.filter(cluster_table.c.name.in_(batch)).all()
            for row in rows:
                result[row.name] = (row.status, row.status_updated_at)
    return result


def get_cluster_workload_fields(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    cluster_in_query_chunk_size: int,
    cluster_names: list[str],
) -> dict[str, tuple[str | None, str | None]]:
    """Return workload type and ID for a bounded cluster-name batch.

    This deliberately reads only the two attribution columns.  Infrastructure
    inventory must not deserialize every cluster handle merely to identify the
    SkyServe subset of accelerator allocations.
    """
    result: dict[str, tuple[str | None, str | None]] = {}
    if not cluster_names:
        return result
    engine = engine_getter()
    with session_factory(engine) as session:
        query = session.query(
            cluster_table.c.name,
            cluster_table.c.workload_type,
            cluster_table.c.workload_id,
        )
        for offset in range(0, len(cluster_names), cluster_in_query_chunk_size):
            batch = cluster_names[offset:offset + cluster_in_query_chunk_size]
            rows = query.filter(cluster_table.c.name.in_(batch)).all()
            for row in rows:
                result[row.name] = (row.workload_type, row.workload_id)
    return result


def get_cluster_status_fields_by_prefix(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    cluster_name_prefix: str,
    *,
    row_limit: int,
) -> dict[str, tuple[str | None, int | None]]:
    """Return a bounded, ordered raw status inventory for one prefix."""
    if (not isinstance(cluster_name_prefix, str) or not cluster_name_prefix or
            type(row_limit) is not int or row_limit < 1):
        raise ValueError('A nonempty cluster prefix and positive integer row '
                         'limit are required.')
    engine = engine_getter()
    with session_factory(engine) as session:
        rows = session.query(
            cluster_table.c.name,
            cluster_table.c.status,
            cluster_table.c.status_updated_at,
        ).filter(
            cluster_table.c.name.startswith(
                cluster_name_prefix, autoescape=True)).order_by(
                    cluster_table.c.name).limit(row_limit + 1).all()
    if len(rows) > row_limit:
        raise ValueError('Cluster prefix inventory exceeds its explicit row '
                         'limit.')
    return {row.name: (row.status, row.status_updated_at) for row in rows}


def get_managed_cluster_status_fields(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    managed_cluster_status_fields_constructor: Callable[
        [str | None, int | None, str | None], _ManagedSnapshotT],
    workload_type: str,
) -> dict[str, _ManagedSnapshotT]:
    """Return generation-fenced fields for one managed workload type."""
    engine = engine_getter()
    with session_factory(engine) as session:
        rows = session.query(
            cluster_table.c.name,
            cluster_table.c.status,
            cluster_table.c.status_updated_at,
            cluster_table.c.cluster_hash,
        ).filter(
            cluster_table.c.is_managed == int(True),
            cluster_table.c.workload_type == workload_type,
            cluster_table.c.cluster_hash.is_not(None),
            cluster_table.c.cluster_hash != '',
        ).all()
    return {
        row.name: managed_cluster_status_fields_constructor(
            row.status, row.status_updated_at, row.cluster_hash) for row in rows
    }


def get_managed_job_cluster_cleanup_candidates(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
) -> dict[str, str | None]:
    """Return managed-job cluster names and durable workload IDs."""
    engine = engine_getter()
    with session_factory(engine) as session:
        rows = session.query(
            cluster_table.c.name,
            cluster_table.c.workload_id,
        ).filter(
            cluster_table.c.is_managed == int(True),
            sqlalchemy.or_(
                cluster_table.c.workload_type == 'managed_job',
                cluster_table.c.workload_type.is_(None),
            ),
        ).all()
    return {row.name: row.workload_id for row in rows}


def get_cluster_refresh_fields(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    cluster_refresh_fields_constructor: Callable[
        [str | None, int | None, int, bool, str | None, bool, str | None],
        _RefreshSnapshotT],
    cluster_name: str,
) -> _RefreshSnapshotT | None:
    """Return raw columns that fence one cluster status refresh."""
    engine = engine_getter()
    with session_factory(engine) as session:
        row = session.query(
            cluster_table.c.status,
            cluster_table.c.status_updated_at,
            cluster_table.c.autostop,
            cluster_table.c.to_down,
            cluster_table.c.cluster_hash,
            cluster_table.c.is_managed,
            cluster_table.c.workload_type,
        ).filter_by(name=cluster_name).first()
    if row is None:
        return None
    return cluster_refresh_fields_constructor(row.status, row.status_updated_at,
                                              row.autostop, bool(row.to_down),
                                              row.cluster_hash,
                                              bool(row.is_managed),
                                              row.workload_type)
