"""Operator notification persistence for the global user state database."""

from typing import Any

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky import global_user_state_schema
from sky.utils.db import db_utils

operator_notification_cursor_table = (
    global_user_state_schema.operator_notification_cursor_table)
operator_notification_sequence_table = (
    global_user_state_schema.operator_notification_sequence_table)
operator_notification_table = global_user_state_schema.operator_notification_table


def _operator_notification_insert_func(
    engine: sqlalchemy.engine.Engine,) -> Any:
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        return sqlite.insert
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return postgresql.insert
    raise ValueError('Unsupported database dialect')


def _next_operator_notification_sequence(session: orm.Session,
                                         insert_func: Any) -> int:
    ensure_counter = insert_func(operator_notification_sequence_table).values(
        singleton_id=1, value=0
    ).on_conflict_do_nothing(
        index_elements=[operator_notification_sequence_table.c.singleton_id])
    session.execute(ensure_counter)
    # This UPDATE takes the row's write lock before computing value + 1 on
    # both SQLite and PostgreSQL. Reading it back in the same transaction is
    # therefore race-free without depending on SQLite RETURNING support.
    session.execute(operator_notification_sequence_table.update().where(
        operator_notification_sequence_table.c.singleton_id == 1).values(
            value=operator_notification_sequence_table.c.value + 1))
    sequence = session.execute(
        sqlalchemy.select(operator_notification_sequence_table.c.value).where(
            operator_notification_sequence_table.c.singleton_id ==
            1)).scalar_one()
    return int(sequence)


def record_operator_notification(engine: sqlalchemy.engine.Engine,
                                 category: str, message: str,
                                 dedupe_window_seconds: int,
                                 emitted_at: int) -> None:
    """Record or coalesce one low-cardinality operator notification."""
    insert_func = _operator_notification_insert_func(engine)
    with orm.Session(engine) as session:
        sequence = _next_operator_notification_sequence(session, insert_func)
        insert_stmnt = insert_func(operator_notification_table).values(
            category=category,
            message=message,
            first_seen_at=emitted_at,
            last_seen_at=emitted_at,
            occurrence_count=1,
            sequence=sequence,
        )
        current = operator_notification_table.c
        starts_new_incident = (current.last_seen_at
                               <= emitted_at - dedupe_window_seconds)
        is_earliest_occurrence = current.first_seen_at > emitted_at
        advances_last_seen = current.last_seen_at < emitted_at
        is_latest_occurrence = current.last_seen_at <= emitted_at
        upsert_stmnt = insert_stmnt.on_conflict_do_update(
            index_elements=[current.category],
            set_={
                current.message: sqlalchemy.case(
                    (is_latest_occurrence, message), else_=current.message),
                current.first_seen_at: sqlalchemy.case(
                    (is_earliest_occurrence, emitted_at),
                    else_=current.first_seen_at),
                current.last_seen_at: sqlalchemy.case(
                    (advances_last_seen, emitted_at),
                    else_=current.last_seen_at),
                current.occurrence_count: current.occurrence_count + 1,
                current.sequence: sqlalchemy.case(
                    (starts_new_incident, sequence), else_=current.sequence),
            })
        session.execute(upsert_stmnt)
        session.commit()


def get_operator_notifications(engine: sqlalchemy.engine.Engine, user_id: str,
                               since: int) -> dict[str, Any]:
    """Return recent notification categories and one user's unread state."""
    with orm.Session(engine) as session:
        cursor = session.execute(
            sqlalchemy.select(
                operator_notification_cursor_table.c.last_seen_sequence).where(
                    operator_notification_cursor_table.c.user_id ==
                    user_id)).scalar_one_or_none()
        last_seen_sequence = int(cursor or 0)
        rows = session.execute(
            sqlalchemy.select(operator_notification_table).
            where(operator_notification_table.c.last_seen_at >= since).order_by(
                operator_notification_table.c.last_seen_at.desc(),
                operator_notification_table.c.category.asc())).mappings().all()

    notifications = []
    latest_sequence = 0
    unread_count = 0
    for item in rows:
        sequence = int(item['sequence'])
        unread = sequence > last_seen_sequence
        latest_sequence = max(latest_sequence, sequence)
        unread_count += int(unread)
        notifications.append({
            'category': item['category'],
            'message': item['message'],
            'first_seen_at': int(item['first_seen_at']),
            'last_seen_at': int(item['last_seen_at']),
            'occurrence_count': int(item['occurrence_count']),
            'sequence': sequence,
            'unread': unread,
        })
    return {
        'notifications': notifications,
        'unread_count': unread_count,
        'latest_sequence': latest_sequence,
        'last_seen_sequence': last_seen_sequence,
    }


def mark_operator_notifications_read(engine: sqlalchemy.engine.Engine,
                                     user_id: str, through_sequence: int,
                                     updated_at: int) -> int:
    """Monotonically advance a user's cursor, clamped to issued sequences."""
    insert_func = _operator_notification_insert_func(engine)
    with orm.Session(engine) as session:
        issued_sequence = session.execute(
            sqlalchemy.select(
                operator_notification_sequence_table.c.value).where(
                    operator_notification_sequence_table.c.singleton_id ==
                    1)).scalar_one_or_none()
        clamped_sequence = min(through_sequence, int(issued_sequence or 0))
        insert_stmnt = insert_func(operator_notification_cursor_table).values(
            user_id=user_id,
            last_seen_sequence=clamped_sequence,
            updated_at=updated_at)
        current = operator_notification_cursor_table.c
        advances_cursor = current.last_seen_sequence < clamped_sequence
        upsert_stmnt = insert_stmnt.on_conflict_do_update(
            index_elements=[current.user_id],
            set_={
                current.last_seen_sequence: sqlalchemy.case(
                    (advances_cursor, clamped_sequence),
                    else_=current.last_seen_sequence),
                current.updated_at: sqlalchemy.case(
                    (advances_cursor, updated_at), else_=current.updated_at),
            })
        session.execute(upsert_stmnt)
        session.commit()
        effective_cursor = session.execute(
            sqlalchemy.select(current.last_seen_sequence).where(
                current.user_id == user_id)).scalar_one()
    return int(effective_cursor)
