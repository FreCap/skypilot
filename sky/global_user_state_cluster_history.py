"""Read gateway for global-state cluster-history reports."""

from collections.abc import Callable
import pickle
from typing import Any

import sqlalchemy
from sqlalchemy import orm

from sky.utils import status_lib


def get_clusters_from_history(
        get_engine: Callable[[], Any],
        session_factory: Callable[..., orm.Session],
        cluster_history_table: Any,
        cluster_table: Any,
        time_fn: Callable[[], float],
        get_current_user_hash: Callable[[], str],
        get_users: Callable[[set[str]], dict[str, Any]],
        get_last_or_terminal_cluster_event_multiple: Callable[[set[str]],
                                                              dict[str, Any]],
        get_cluster_duration: Callable[[list[tuple[int, int | None]] | None],
                                       int],
        get_display_node_names: Callable[[str | None], str | None],
        *,
        days: int | None = None,
        abbreviate_response: bool = False,
        cluster_hashes: list[str] | None = None,
        cluster_names: list[str] | None = None,
        exclude_managed_clusters: bool = False) -> list[dict[str, Any]]:
    """Queries, enriches, and projects cluster-history report records."""
    engine = get_engine()
    current_user_hash = get_current_user_hash()

    cutoff_time = 0
    if days is not None:
        cutoff_time = int(time_fn()) - (days * 24 * 60 * 60)

    # last_creation_yaml / last_creation_command hold the full task YAML and
    # launch command for each history row. In aggregate these are by far the
    # largest columns (a 30-day report can span thousands of clusters), yet
    # only targeted by-hash / by-name lookups (e.g. a single cluster's detail
    # view) actually read them. Bulk reports such as `sky cost-report` and the
    # dashboard history list never use them, so fetch them only for filtered
    # queries to avoid loading every cluster's YAML into memory.
    include_creation_yaml = (not abbreviate_response and
                             (cluster_hashes is not None or
                              cluster_names is not None))

    with session_factory(engine) as session:
        # Explicitly select columns from both tables to avoid ambiguity.
        selected_columns = [
            cluster_history_table.c.cluster_hash,
            cluster_history_table.c.name,
            cluster_history_table.c.num_nodes,
            cluster_history_table.c.launched_resources,
            cluster_history_table.c.usage_intervals,
            cluster_history_table.c.user_hash,
            cluster_history_table.c.workspace.label('history_workspace'),
            cluster_history_table.c.last_activity_time,
            cluster_history_table.c.launched_at,
            cluster_history_table.c.node_names,
            cluster_table.c.status,
            cluster_table.c.workspace,
        ]
        if include_creation_yaml:
            selected_columns.extend([
                cluster_history_table.c.last_creation_yaml,
                cluster_history_table.c.last_creation_command,
            ])
        query = session.query(*selected_columns)

        query = query.select_from(
            cluster_history_table.join(cluster_table,
                                       cluster_history_table.c.cluster_hash ==
                                       cluster_table.c.cluster_hash,
                                       isouter=True))

        # Only include clusters that are either active (status is not None)
        # or are within the cutoff time (cutoff_time <= last_activity_time).
        # If days is not specified, we include all clusters by setting
        # cutoff_time to 0.
        query = query.filter(
            cluster_table.c.status.isnot(None) |
            (cluster_history_table.c.last_activity_time >= cutoff_time))

        # Order by launched_at descending (most recent first)
        query = query.order_by(
            sqlalchemy.desc(cluster_history_table.c.launched_at))

        identifier_filters = []
        if cluster_hashes is not None:
            identifier_filters.append(
                cluster_history_table.c.cluster_hash.in_(cluster_hashes))
        if cluster_names is not None:
            identifier_filters.append(
                cluster_history_table.c.name.in_(cluster_names))
        if identifier_filters:
            query = query.filter(sqlalchemy.or_(*identifier_filters))
        if exclude_managed_clusters:
            # Treat NULL (rows predating the is_managed column) as not managed.
            query = query.filter(
                sqlalchemy.or_(
                    cluster_history_table.c.is_managed.is_(None),
                    cluster_history_table.c.is_managed == int(False)))
        rows = query.all()

    usage_intervals_dict = {}
    row_to_user_hash = {}
    for row in rows:
        row_usage_intervals: list[tuple[int, int | None]] = []
        if row.usage_intervals:
            try:
                row_usage_intervals = pickle.loads(row.usage_intervals)
            except (pickle.PickleError, AttributeError):
                pass
        usage_intervals_dict[row.cluster_hash] = row_usage_intervals
        user_hash = (row.user_hash
                     if row.user_hash is not None else current_user_hash)
        row_to_user_hash[row.cluster_hash] = user_hash

    user_hashes = set(row_to_user_hash.values())
    user_hash_to_user = get_users(user_hashes)
    cluster_hashes = set(row_to_user_hash.keys())
    last_cluster_event_dict = get_last_or_terminal_cluster_event_multiple(
        cluster_hashes)

    records = []
    for row in rows:
        user_hash = row_to_user_hash[row.cluster_hash]
        user = user_hash_to_user.get(user_hash, None)
        user_name = user.name if user is not None else None
        last_event = last_cluster_event_dict.get(row.cluster_hash, None)
        launched_at = row.launched_at
        usage_intervals: list[tuple[int, int |
                                    None]] | None = usage_intervals_dict.get(
                                        row.cluster_hash, None)
        duration = get_cluster_duration(usage_intervals)

        # Parse status
        status = None
        if row.status:
            status = status_lib.ClusterStatus[row.status]

        # Parse launched resources safely
        launched_resources = None
        if row.launched_resources:
            try:
                launched_resources = pickle.loads(row.launched_resources)
            except (pickle.PickleError, AttributeError):
                launched_resources = None

        workspace = (row.history_workspace
                     if row.history_workspace else row.workspace)

        record = {
            'name': row.name,
            'launched_at': launched_at,
            'duration': duration,
            'num_nodes': row.num_nodes,
            'resources': launched_resources,
            'priority': launched_resources.priority
                        if launched_resources is not None else None,
            'priority_class': launched_resources.priority_class
                              if launched_resources is not None else None,
            'cluster_hash': row.cluster_hash,
            'usage_intervals': usage_intervals,
            'status': status,
            'user_hash': user_hash,
            'user_name': user_name,
            'workspace': workspace,
            'last_event': last_event,
            'node_names': get_display_node_names(row.node_names),
        }
        if include_creation_yaml:
            record['last_creation_yaml'] = row.last_creation_yaml
            record['last_creation_command'] = row.last_creation_command
        elif not abbreviate_response:
            # Preserve the dict schema for non-abbreviated callers: these keys
            # were always present (possibly None) before we stopped fetching
            # the heavy columns on bulk paths. The columns were not selected
            # here, so this is None at zero memory cost.
            record['last_creation_yaml'] = None
            record['last_creation_command'] = None

        records.append(record)

    # sort by launch time, descending in recency
    records = sorted(records, key=lambda record: -(record['launched_at'] or 0))
    return records
