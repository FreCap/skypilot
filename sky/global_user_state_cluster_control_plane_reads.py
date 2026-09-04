"""Read gateway for live-cluster control-plane records."""

from collections.abc import Callable
import json
import pickle
from typing import Any

import sqlalchemy

from sky import global_user_state_cluster_name_batches
from sky.utils import status_lib


def _query_fields(cluster_table: sqlalchemy.Table) -> list[Any]:
    """Return the columns shared by exact and batched record snapshots."""
    return [
        cluster_table.c.name,
        cluster_table.c.launched_at,
        cluster_table.c.handle,
        cluster_table.c.last_use,
        cluster_table.c.status,
        cluster_table.c.autostop,
        cluster_table.c.to_down,
        cluster_table.c.owner,
        cluster_table.c.metadata,
        cluster_table.c.cluster_hash,
        cluster_table.c.cluster_ever_up,
        cluster_table.c.status_updated_at,
        cluster_table.c.user_hash,
        cluster_table.c.config_hash,
        cluster_table.c.workspace,
        cluster_table.c.is_managed,
        cluster_table.c.workload_type,
    ]


def _project_summary_record(
    row: Any,
    load_owner: Callable[[str | None], list[str] | None],
) -> dict[str, Any]:
    """Project the response fields shared by exact and batched reads."""
    return {
        'name': row.name,
        'launched_at': row.launched_at,
        'handle': pickle.loads(row.handle),
        'last_use': row.last_use,
        'status': status_lib.ClusterStatus[row.status],
        'autostop': row.autostop,
        'to_down': bool(row.to_down),
        'owner': load_owner(row.owner),
        'metadata': json.loads(row.metadata),
        'cluster_hash': row.cluster_hash,
        'cluster_ever_up': bool(row.cluster_ever_up),
        'status_updated_at': row.status_updated_at,
        'workspace': row.workspace,
        'is_managed': bool(row.is_managed),
        'workload_type': row.workload_type,
        'config_hash': row.config_hash,
    }


def get_cluster_from_name(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    user_table: sqlalchemy.Table,
    get_user_hash: Callable[[], str],
    cluster_user_join_key: Callable[[str], sqlalchemy.ColumnElement],
    load_owner: Callable[[str | None], list[str] | None],
    get_terminal_or_last_status_change_event: Callable[[str], str | None],
    cluster_name: str | None,
    *,
    include_user_info: bool = True,
    summary_response: bool = False,
) -> dict[str, Any] | None:
    """Return one live-cluster control-plane record."""
    engine = engine_getter()
    query_fields = _query_fields(cluster_table)
    joined_user_name_label = 'joined_user_name'
    current_user_hash = ''
    if include_user_info:
        current_user_hash = get_user_hash()
        query_fields.append(user_table.c.name.label(joined_user_name_label))
    if not summary_response:
        query_fields.extend([
            cluster_table.c.last_creation_yaml,
            cluster_table.c.last_creation_command,
        ])
    with session_factory(engine) as session:
        query = session.query(*query_fields)
        if include_user_info:
            query = query.outerjoin(
                user_table,
                cluster_user_join_key(current_user_hash) == user_table.c.id)
        row = query.filter(cluster_table.c.name == cluster_name).first()
        if row is None:
            return None
        user_hash = None
        user_name = None
        if include_user_info:
            user_hash = (row.user_hash
                         if row.user_hash is not None else current_user_hash)
            user_name = getattr(row, joined_user_name_label)

    last_event = None
    if not summary_response:
        last_event = get_terminal_or_last_status_change_event(row.cluster_hash)
    record = _project_summary_record(row, load_owner)
    if not summary_response:
        record['last_creation_yaml'] = row.last_creation_yaml
        record['last_creation_command'] = row.last_creation_command
        record['last_event'] = last_event
    if include_user_info:
        record['user_hash'] = user_hash
        record['user_name'] = user_name
    return record


def get_clusters_from_names(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    cluster_in_query_chunk_size: int,
    get_user_hash_or_current_user: Callable[[str | None], str],
    get_users_in_session: Callable[[Any, set[str]], dict[str, Any]],
    load_owner: Callable[[str | None], list[str] | None],
    cluster_names: list[str],
    *,
    include_user_info: bool = False,
) -> dict[str, dict[str, Any] | None]:
    """Return summary records for a bounded batch of live-cluster names."""
    result: dict[str, dict[str, Any] | None] = {
        name: None for name in cluster_names
    }
    if not cluster_names:
        return result

    engine = engine_getter()
    query_fields = _query_fields(cluster_table)
    cluster_name_batches = (
        global_user_state_cluster_name_batches.get_unique_cluster_name_batches(
            cluster_names, cluster_in_query_chunk_size))
    with session_factory(engine) as session:
        row_snapshots: list[tuple[Any, str | None]] = []
        effective_user_hashes: set[str] = set()
        for batch in cluster_name_batches:
            rows = session.query(*query_fields).filter(
                cluster_table.c.name.in_(batch)).all()
            for row in rows:
                effective_user_hash = None
                if include_user_info:
                    effective_user_hash = get_user_hash_or_current_user(
                        row.user_hash)
                    effective_user_hashes.add(effective_user_hash)
                row_snapshots.append((row, effective_user_hash))
        users_by_hash = {}
        if include_user_info:
            users_by_hash = get_users_in_session(session, effective_user_hashes)
        for row, effective_user_hash in row_snapshots:
            record = _project_summary_record(row, load_owner)
            if include_user_info:
                assert effective_user_hash is not None, row.name
                user = users_by_hash.get(effective_user_hash)
                record['user_hash'] = effective_user_hash
                record['user_name'] = (user.name if user is not None else None)
            result[row.name] = record
    return result
