"""Read gateway for active-cluster listing projections."""

from collections.abc import Callable
import json
import pickle
from typing import Any

import sqlalchemy

from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import status_lib


def get_clusters(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    user_table: sqlalchemy.Table,
    cluster_in_query_chunk_size: int,
    get_last_cluster_event_of_type_multiple: Callable[[set[str], Any],
                                                      dict[str, str]],
    get_last_or_terminal_cluster_event_multiple: Callable[[set[str]],
                                                          dict[str, str]],
    launch_progress_event_type: Any,
    cluster_user_join_key: Callable[[str], sqlalchemy.ColumnElement],
    load_owner: Callable[[str | None], list[str] | None],
    *,
    exclude_managed_clusters: bool = False,
    workspaces_filter: set[str] | None = None,
    user_hashes_filter: set[str] | None = None,
    cluster_names: list[str] | None = None,
    summary_response: bool = False,
) -> list[dict[str, Any]]:
    """Return active clusters using the listing and display projection."""
    # If a cluster has a null user_hash, treat it as belonging to the current
    # user for backwards compatibility.
    current_user_hash = common_utils.get_user_hash()
    engine = engine_getter()
    deduped_cluster_names = None
    if cluster_names is not None:
        deduped_cluster_names = list(dict.fromkeys(cluster_names))
    query_fields = [
        cluster_table.c.name,
        cluster_table.c.launched_at,
        cluster_table.c.handle,
        cluster_table.c.status,
        cluster_table.c.autostop,
        cluster_table.c.to_down,
        cluster_table.c.cluster_hash,
        cluster_table.c.cluster_ever_up,
        cluster_table.c.user_hash,
        cluster_table.c.workspace,
        cluster_table.c.node_names,
        user_table.c.name.label('user_name'),
    ]
    if not summary_response:
        query_fields.extend([
            cluster_table.c.last_creation_yaml,
            cluster_table.c.last_creation_command,
            cluster_table.c.config_hash,
            cluster_table.c.owner,
            cluster_table.c.metadata,
            cluster_table.c.last_use,
            cluster_table.c.status_updated_at,
            cluster_table.c.links,
        ])
    if not exclude_managed_clusters:
        query_fields.append(cluster_table.c.is_managed)
    with session_factory(engine) as session:
        query = session.query(*query_fields).outerjoin(
            user_table,
            cluster_user_join_key(current_user_hash) == user_table.c.id)
        if exclude_managed_clusters:
            query = query.filter(cluster_table.c.is_managed == int(False))
        if workspaces_filter is not None:
            query = query.filter(
                cluster_table.c.workspace.in_(workspaces_filter))
        if user_hashes_filter is not None:
            if current_user_hash in user_hashes_filter:
                query = query.filter(
                    cluster_table.c.user_hash.in_(user_hashes_filter) |
                    cluster_table.c.user_hash.is_(None))
            else:
                query = query.filter(
                    cluster_table.c.user_hash.in_(user_hashes_filter))
        query = query.order_by(sqlalchemy.desc(cluster_table.c.launched_at))
        if deduped_cluster_names is None:
            rows = query.all()
        elif len(deduped_cluster_names) <= cluster_in_query_chunk_size:
            rows = query.filter(
                cluster_table.c.name.in_(deduped_cluster_names)).all()
        else:
            rows = []
            for offset in range(0, len(deduped_cluster_names),
                                cluster_in_query_chunk_size):
                batch = deduped_cluster_names[offset:offset +
                                              cluster_in_query_chunk_size]
                rows.extend(query.filter(cluster_table.c.name.in_(batch)).all())
            rows.sort(key=lambda row: row.launched_at, reverse=True)

    records = []
    cluster_hashes = {row.cluster_hash for row in rows}
    init_cluster_hashes = {
        row.cluster_hash
        for row in rows
        if status_lib.ClusterStatus[row.status] is status_lib.ClusterStatus.INIT
    }
    launch_progress_dict = get_last_cluster_event_of_type_multiple(
        init_cluster_hashes, launch_progress_event_type)

    last_cluster_event_dict = {}
    if not summary_response:
        last_cluster_event_dict = get_last_or_terminal_cluster_event_multiple(
            cluster_hashes)

    for row in rows:
        handle = pickle.loads(row.handle)
        priority = (handle.launched_resources.priority
                    if handle.launched_resources is not None else None)
        priority_class = (handle.launched_resources.priority_class
                          if handle.launched_resources is not None else None)
        record = {
            'name': row.name,
            'launched_at': row.launched_at,
            'handle': handle,
            'status': status_lib.ClusterStatus[row.status],
            'priority': priority
                        if priority is not None else constants.DEFAULT_PRIORITY,
            'priority_class': priority_class,
            'autostop': row.autostop,
            'to_down': bool(row.to_down),
            'cluster_hash': row.cluster_hash,
            'cluster_ever_up': bool(row.cluster_ever_up),
            'user_hash': (row.user_hash
                          if row.user_hash is not None else current_user_hash),
            'user_name': row.user_name,
            'workspace': row.workspace,
            'is_managed': False
                          if exclude_managed_clusters else bool(row.is_managed),
            'node_names': common_utils.get_display_node_names(row.node_names),
        }
        if record['status'] is status_lib.ClusterStatus.INIT:
            record['launch_status_reason'] = launch_progress_dict.get(
                row.cluster_hash)
        else:
            record['launch_status_reason'] = None
        if not summary_response:
            record['last_creation_yaml'] = row.last_creation_yaml
            record['last_creation_command'] = row.last_creation_command
            record['last_event'] = last_cluster_event_dict.get(
                row.cluster_hash, None)
            record['config_hash'] = row.config_hash
            record['links'] = row.links if isinstance(row.links, dict) else {}
            record['owner'] = load_owner(row.owner)
            record['metadata'] = json.loads(row.metadata)
            record['last_use'] = row.last_use
            record['status_updated_at'] = row.status_updated_at

        records.append(record)
    return records
