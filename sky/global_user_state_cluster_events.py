"""Persistence repository for cluster event rows."""

from collections.abc import Callable
import contextlib
import re
from typing import Any

import sqlalchemy

from sky.utils.db import db_utils


def add_cluster_event(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    sqlite_dialect: Any,
    postgresql_dialect: Any,
    cluster_table: sqlalchemy.Table,
    cluster_event_table: sqlalchemy.Table,
    get_last_event: Callable[..., str | None],
    logger: Any,
    request_id_getter: Callable[[], str | None],
    time_fn: Callable[[], float],
    unique_constraint_messages: list[str],
    cluster_name: str,
    new_status: Any | None,
    reason: str,
    event_type: Any,
    nop_if_duplicate: bool = False,
    duplicate_regex: str | None = None,
    expose_duplicate_error: bool = False,
    transitioned_at: int | None = None,
    existing_cluster_hash: str | None = None,
) -> None:
    """Add one cluster event within a generation-fenced transaction."""
    engine = engine_getter()
    if transitioned_at is None:
        transitioned_at = int(time_fn())
    with session_factory(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite_dialect.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql_dialect.insert
        else:
            session.rollback()
            raise ValueError('Unsupported database dialect')

        # Read hash and status in a single query so they come from the same
        # row snapshot (a separate hash pre-fetch could pair a stale hash
        # with a newer status under concurrent removal/re-creation).
        query = session.query(
            cluster_table.c.cluster_hash,
            cluster_table.c.status).filter_by(name=cluster_name)
        if existing_cluster_hash is not None:
            query = query.filter_by(cluster_hash=existing_cluster_hash)
        cluster_row = query.first()
        if cluster_row is None or cluster_row.cluster_hash is None:
            logger.debug(f'Hash for cluster {cluster_name} not found. '
                         'Skipping event.')
            return
        cluster_hash = cluster_row.cluster_hash
        last_status = cluster_row.status
        if nop_if_duplicate:
            # Reuse this session: add_cluster_event already holds a pooled
            # connection here, and a nested checkout self-deadlocks a
            # single-connection sync pool.
            last_event = get_last_event(cluster_hash,
                                        event_type=event_type,
                                        session=session)
            if duplicate_regex is not None and last_event is not None:
                if re.search(duplicate_regex, last_event):
                    return
            elif last_event == reason:
                return
        try:
            request_id = request_id_getter()
            session.execute(
                insert_func(cluster_event_table).values(
                    cluster_hash=cluster_hash,
                    name=cluster_name,
                    starting_status=last_status,
                    ending_status=new_status.value if new_status else None,
                    reason=reason,
                    transitioned_at=transitioned_at,
                    type=event_type.value,
                    request_id=request_id,
                ))
            session.commit()
        except sqlalchemy.exc.IntegrityError as e:
            for msg in unique_constraint_messages:
                if msg in str(e):
                    # This can happen if the cluster event is added twice.
                    # Ignore it unless the caller requests the error.
                    if expose_duplicate_error:
                        raise db_utils.UniqueConstraintViolationError(
                            value=reason, message=str(e))
                    return
            raise e


def get_last_cluster_event(
        session_scope: contextlib.AbstractContextManager[Any],
        cluster_event_table: sqlalchemy.Table, cluster_hash: str,
        event_type: Any) -> str | None:
    """Return the latest reason for one cluster and event type."""
    with session_scope as active_session:
        row = active_session.query(cluster_event_table).filter_by(
            cluster_hash=cluster_hash, type=event_type.value).order_by(
                cluster_event_table.c.transitioned_at.desc()).first()
    if row is None:
        return None
    return row.reason


def get_terminal_or_last_status_change_event(
        engine: sqlalchemy.engine.Engine, session_factory: Any,
        cluster_event_table: sqlalchemy.Table, event_type: Any,
        cluster_hash: str) -> str | None:
    """Return terminal reason when present, otherwise the latest status."""
    with session_factory(engine) as session:
        type_priority = sqlalchemy.case(
            (cluster_event_table.c.type == event_type.TERMINAL.value, 0),
            else_=1)
        row = session.query(cluster_event_table).filter(
            cluster_event_table.c.cluster_hash == cluster_hash,
            cluster_event_table.c.type.in_([
                event_type.TERMINAL.value,
                event_type.STATUS_CHANGE.value,
            ])).order_by(type_priority,
                         cluster_event_table.c.transitioned_at.desc()).first()
    if row is None:
        return None
    return row.reason


def get_last_or_terminal_cluster_event_multiple(
        engine: sqlalchemy.engine.Engine, session_factory: Any,
        cluster_event_table: sqlalchemy.Table, event_type: Any,
        cluster_hashes: set[str]) -> dict[str, str]:
    """Return the terminal-priority latest event for every cluster."""
    with session_factory(engine) as session:
        type_priority = sqlalchemy.case(
            (cluster_event_table.c.type == event_type.TERMINAL.value, 0),
            else_=1)
        row_number = sqlalchemy.func.row_number().over(
            partition_by=cluster_event_table.c.cluster_hash,
            order_by=[
                type_priority,
                cluster_event_table.c.transitioned_at.desc(),
            ]).label('rn')
        ranked_events = session.query(
            cluster_event_table.c.cluster_hash,
            cluster_event_table.c.reason,
            row_number,
        ).filter(
            cluster_event_table.c.cluster_hash.in_(cluster_hashes),
            cluster_event_table.c.type.notin_([
                event_type.DEBUG.value,
                event_type.LAUNCH_PROGRESS.value,
            ])).subquery()
        rows = session.query(
            ranked_events.c.cluster_hash,
            ranked_events.c.reason,
        ).filter(ranked_events.c.rn == 1).all()
    return {row.cluster_hash: row.reason for row in rows}


def get_last_cluster_event_of_type_multiple(
        engine_getter: Callable[[], sqlalchemy.engine.Engine],
        session_factory: Any, cluster_event_table: sqlalchemy.Table,
        cluster_hashes: set[str], event_type: Any) -> dict[str, str]:
    """Return the latest event of one type for every supplied cluster."""
    if not cluster_hashes:
        return {}
    engine = engine_getter()
    with session_factory(engine) as session:
        row_number = sqlalchemy.func.row_number().over(
            partition_by=cluster_event_table.c.cluster_hash,
            order_by=cluster_event_table.c.transitioned_at.desc()).label('rn')
        ranked = session.query(
            cluster_event_table.c.cluster_hash,
            cluster_event_table.c.reason,
            row_number,
        ).filter(
            cluster_event_table.c.cluster_hash.in_(cluster_hashes),
            cluster_event_table.c.type == event_type.value,
        ).subquery()
        rows = session.query(
            ranked.c.cluster_hash,
            ranked.c.reason,
        ).filter(ranked.c.rn == 1).all()
    return {row.cluster_hash: row.reason for row in rows}


def get_last_status_change_times(engine_getter: Callable[
    [], sqlalchemy.engine.Engine], session_factory: Any,
                                 cluster_event_table: sqlalchemy.Table,
                                 chunk_size: int, status_change_value: str,
                                 cluster_hashes: set[str],
                                 ending_status: Any) -> dict[str, int]:
    """Return the latest matching status-change time for each cluster."""
    if not cluster_hashes:
        return {}
    engine = engine_getter()
    hashes_list = list(cluster_hashes)
    result: dict[str, int] = {}
    with session_factory(engine) as session:
        for offset in range(0, len(hashes_list), chunk_size):
            batch = hashes_list[offset:offset + chunk_size]
            row_number = sqlalchemy.func.row_number().over(
                partition_by=cluster_event_table.c.cluster_hash,
                order_by=cluster_event_table.c.transitioned_at.desc()).label(
                    'rn')
            ranked = session.query(
                cluster_event_table.c.cluster_hash,
                cluster_event_table.c.transitioned_at,
                row_number,
            ).filter(
                cluster_event_table.c.cluster_hash.in_(batch),
                cluster_event_table.c.type == status_change_value,
                cluster_event_table.c.ending_status == ending_status.value,
            ).subquery()
            rows = session.query(
                ranked.c.cluster_hash,
                ranked.c.transitioned_at,
            ).filter(ranked.c.rn == 1).all()
            for row in rows:
                result[row.cluster_hash] = int(row.transitioned_at)
    return result


def get_first_status_change_time_since(engine: sqlalchemy.engine.Engine,
                                       session_factory: Any,
                                       cluster_event_table: sqlalchemy.Table,
                                       status_change_value: str,
                                       cluster_hash: str, ending_status: Any,
                                       since: float) -> int | None:
    """Return the earliest matching status change at or after ``since``."""
    with session_factory(engine) as session:
        row = session.query(
            sqlalchemy.func.min(cluster_event_table.c.transitioned_at)).filter(
                cluster_event_table.c.cluster_hash == cluster_hash,
                cluster_event_table.c.type == status_change_value,
                cluster_event_table.c.ending_status == ending_status.value,
                cluster_event_table.c.transitioned_at >= since,
            ).scalar()
    return None if row is None else int(row)


def cleanup_cluster_events_with_retention(engine: sqlalchemy.engine.Engine,
                                          session_factory: Any,
                                          cluster_event_table: sqlalchemy.Table,
                                          logger: Any, time_fn: Callable[[],
                                                                         float],
                                          retention_hours: float,
                                          event_type: Any) -> None:
    """Delete event rows older than the configured retention window."""
    with session_factory(engine) as session:
        query = session.query(cluster_event_table).filter(
            cluster_event_table.c.transitioned_at
            < time_fn() - retention_hours * 3600,
            cluster_event_table.c.type == event_type.value)
        logger.debug(f'Deleting {query.count()} cluster events.')
        query.delete()
        session.commit()


def get_cluster_events(
        engine: sqlalchemy.engine.Engine,
        session_factory: Any,
        cluster_event_table: sqlalchemy.Table,
        cluster_hash: str,
        event_type_class: Any,
        event_type: Any | list[Any],
        include_timestamps: bool = False,
        limit: int | None = None) -> list[str] | list[dict[str, str | int]]:
    """Return ordered event projections for one cluster hash."""
    event_types = ([event_type]
                   if isinstance(event_type, event_type_class) else event_type)
    type_filter = cluster_event_table.c.type.in_(
        [item.value for item in event_types])
    with session_factory(engine) as session:
        if limit is not None:
            subquery = session.query(cluster_event_table).filter(
                cluster_event_table.c.cluster_hash == cluster_hash,
                type_filter).order_by(
                    cluster_event_table.c.transitioned_at.desc()).limit(
                        limit).subquery()
            rows = session.query(subquery).order_by(
                subquery.c.transitioned_at.asc()).all()
        else:
            rows = session.query(cluster_event_table).filter(
                cluster_event_table.c.cluster_hash == cluster_hash,
                type_filter).order_by(
                    cluster_event_table.c.transitioned_at.asc()).all()
    if include_timestamps:
        return [{
            'reason': row.reason,
            'transitioned_at': row.transitioned_at,
        } for row in rows]
    return [row.reason for row in rows]


def get_cluster_events_by_names(
        engine_getter: Callable[[], sqlalchemy.engine.Engine],
        session_factory: Any,
        cluster_event_table: sqlalchemy.Table,
        chunk_size: int,
        cluster_names: list[str],
        event_types: list[Any],
        limit: int | None = None) -> list[dict[str, str | int]]:
    """Return newest-first event projections for persisted cluster names."""
    cluster_names = list(dict.fromkeys(cluster_names))
    if not cluster_names or not event_types or limit == 0:
        return []
    engine = engine_getter()
    type_values = [event_type.value for event_type in event_types]
    rows = []
    with session_factory(engine) as session:
        for start in range(0, len(cluster_names), chunk_size):
            names = cluster_names[start:start + chunk_size]
            query = session.query(
                cluster_event_table.c.reason,
                cluster_event_table.c.transitioned_at,
            ).filter(cluster_event_table.c.name.in_(names),
                     cluster_event_table.c.type.in_(type_values)).order_by(
                         cluster_event_table.c.transitioned_at.desc())
            if limit is not None:
                query = query.limit(limit)
            rows.extend(query.all())
    if len(cluster_names) > chunk_size:
        rows.sort(key=lambda row: row.transitioned_at, reverse=True)
        if limit is not None and limit >= 0:
            rows = rows[:limit]
    return [{
        'reason': row.reason,
        'transitioned_at': row.transitioned_at,
    } for row in rows]
