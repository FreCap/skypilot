"""Persistence repository for user identity and workspace preference rows."""

from collections.abc import Callable
import contextlib
from typing import Any

import sqlalchemy

from sky import models
from sky.utils.db import db_utils


def add_or_update_user(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    sqlite_dialect: Any,
    postgresql_dialect: Any,
    user_table: sqlalchemy.Table,
    sqlite_supports_returning: Callable[[], bool],
    time_fn: Callable[[], float],
    user: models.User,
    allow_duplicate_name: bool = True,
    return_user: bool = False,
) -> bool | tuple[bool, models.User]:
    """Insert or update one user row."""
    if user.name is None:
        return (False, user) if return_user else False
    engine = engine_getter()
    # Set created_at if not already set
    created_at = user.created_at
    if created_at is None:
        created_at = int(time_fn())
    with session_factory(engine) as session:
        # Check for duplicate names if not allowed (within the same transaction)
        if not allow_duplicate_name:
            existing_user = session.query(user_table).filter(
                user_table.c.name == user.name).first()
            if existing_user is not None:
                return (False, user) if return_user else False

        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            # For SQLite, use INSERT OR IGNORE followed by UPDATE to detect new
            # vs existing
            insert_func = sqlite_dialect.insert

            # First try INSERT OR IGNORE - this won't fail if user exists
            insert_stmnt = insert_func(user_table).prefix_with(
                'OR IGNORE').values(
                    id=user.id,
                    name=user.name,
                    password=user.password,
                    created_at=created_at,
                    type=user.user_type,
                )
            use_returning = return_user and sqlite_supports_returning()
            if use_returning:
                insert_stmnt = insert_stmnt.returning(
                    user_table.c.id,
                    user_table.c.name,
                    user_table.c.password,
                    user_table.c.created_at,
                    user_table.c.type,
                    user_table.c.preferred_workspace,
                )
            result = session.execute(insert_stmnt)

            row = None
            if use_returning:
                # With RETURNING, check if we got a row back.
                row = result.fetchone()
                was_inserted = row is not None
            else:
                # Without RETURNING, use rowcount.
                was_inserted = result.rowcount > 0

            if not was_inserted:
                # User existed, so update it (but don't update created_at)
                update_values = {user_table.c.name: user.name}
                if user.password:
                    update_values[user_table.c.password] = user.password
                if user.user_type:
                    update_values[user_table.c.type] = user.user_type

                update_stmnt = sqlalchemy.update(user_table).where(
                    user_table.c.id == user.id).values(update_values)
                if use_returning:
                    update_stmnt = update_stmnt.returning(
                        user_table.c.id,
                        user_table.c.name,
                        user_table.c.password,
                        user_table.c.created_at,
                        user_table.c.type,
                        user_table.c.preferred_workspace,
                    )

                result = session.execute(update_stmnt)
                if use_returning:
                    row = result.fetchone()

            session.commit()

            if return_user:
                if row is None:
                    # row=None means the sqlite used has no RETURNING support,
                    # so we need to do a separate query
                    row = session.query(user_table).filter_by(
                        id=user.id).first()
                updated_user = models.User(
                    id=row.id,
                    name=row.name,
                    password=row.password,
                    created_at=row.created_at,
                    user_type=row.type,
                    preferred_workspace=row.preferred_workspace,
                )
                return was_inserted, updated_user
            else:
                return was_inserted

        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            # For PostgreSQL, use INSERT ... ON CONFLICT with RETURNING to
            # detect insert vs update
            insert_func = postgresql_dialect.insert

            insert_stmnt = insert_func(user_table).values(
                id=user.id,
                name=user.name,
                password=user.password,
                created_at=created_at,
                type=user.user_type,
            )

            # Use a sentinel in the RETURNING clause to detect insert vs update
            if user.password:
                set_ = {
                    user_table.c.name: user.name,
                    user_table.c.password: user.password
                }
            else:
                set_ = {user_table.c.name: user.name}
            if user.user_type:
                set_[user_table.c.type] = user.user_type
            upsert_stmnt = insert_stmnt.on_conflict_do_update(
                index_elements=[user_table.c.id], set_=set_).returning(
                    user_table.c.id,
                    user_table.c.name,
                    user_table.c.password,
                    user_table.c.created_at,
                    user_table.c.type,
                    user_table.c.preferred_workspace,
                    # This will be True for INSERT, False for UPDATE
                    sqlalchemy.literal_column('(xmax = 0)').label('was_inserted'
                                                                 ))

            result = session.execute(upsert_stmnt)
            row = result.fetchone()

            was_inserted = bool(row.was_inserted) if row else False
            session.commit()

            if return_user:
                updated_user = models.User(
                    id=row.id,
                    name=row.name,
                    password=row.password,
                    created_at=row.created_at,
                    user_type=row.type,
                    preferred_workspace=row.preferred_workspace,
                )
                return was_inserted, updated_user
            else:
                return was_inserted
        else:
            raise ValueError('Unsupported database dialect')


def get_user(session_scope: contextlib.AbstractContextManager[Any],
             user_table: sqlalchemy.Table, user_id: str) -> models.User | None:
    """Project one user row by ID."""
    with session_scope as active_session:
        row = active_session.query(user_table).filter_by(id=user_id).first()
    if row is None:
        return None
    return models.User(
        id=row.id,
        name=row.name,
        password=row.password,
        created_at=row.created_at,
        user_type=row.type,
        preferred_workspace=row.preferred_workspace,
    )


def get_users(engine: sqlalchemy.engine.Engine, session_factory: Any,
              user_table: sqlalchemy.Table,
              user_ids: set[str]) -> dict[str, models.User]:
    """Project a batch of user rows keyed by ID."""
    with session_factory(engine) as session:
        rows = session.query(user_table).filter(
            user_table.c.id.in_(user_ids)).all()
    return {
        row.id: models.User(
            id=row.id,
            name=row.name,
            password=row.password,
            created_at=row.created_at,
            user_type=row.type,
            preferred_workspace=row.preferred_workspace,
        ) for row in rows
    }


def get_user_by_name(engine: sqlalchemy.engine.Engine, session_factory: Any,
                     user_table: sqlalchemy.Table,
                     username: str) -> list[models.User]:
    """Project user rows with an exact display name."""
    with session_factory(engine) as session:
        rows = session.query(user_table).filter_by(name=username).all()
    if len(rows) == 0:
        return []
    return [
        models.User(
            id=row.id,
            name=row.name,
            password=row.password,
            created_at=row.created_at,
            user_type=row.type,
            preferred_workspace=row.preferred_workspace,
        ) for row in rows
    ]


def get_user_by_name_match(engine: sqlalchemy.engine.Engine,
                           session_factory: Any, user_table: sqlalchemy.Table,
                           username_match: str) -> list[models.User]:
    """Project user rows whose display name contains the supplied value."""
    with session_factory(engine) as session:
        rows = session.query(user_table).filter(
            user_table.c.name.like(f'%{username_match}%')).all()
    return [
        models.User(
            id=row.id,
            name=row.name,
            created_at=row.created_at,
            user_type=row.type,
            preferred_workspace=row.preferred_workspace,
        ) for row in rows
    ]


def delete_user(engine: sqlalchemy.engine.Engine, session_factory: Any,
                user_table: sqlalchemy.Table, user_id: str) -> None:
    """Delete one user row by ID."""
    with session_factory(engine) as session:
        session.query(user_table).filter_by(id=user_id).delete()
        session.commit()


def get_all_users(engine: sqlalchemy.engine.Engine, session_factory: Any,
                  user_table: sqlalchemy.Table) -> list[models.User]:
    """Project every user row."""
    with session_factory(engine) as session:
        rows = session.query(user_table).all()
    return [
        models.User(
            id=row.id,
            name=row.name,
            password=row.password,
            created_at=row.created_at,
            user_type=row.type,
            preferred_workspace=row.preferred_workspace,
        ) for row in rows
    ]


def set_user_preferred_workspace(engine: sqlalchemy.engine.Engine,
                                 session_factory: Any,
                                 user_table: sqlalchemy.Table, user_id: str,
                                 workspace: str | None) -> bool:
    """Set or clear one user's raw preferred-workspace value."""
    with session_factory(engine) as session:
        result = session.execute(
            sqlalchemy.update(user_table).where(
                user_table.c.id == user_id).values(
                    preferred_workspace=workspace))
        session.commit()
        return result.rowcount > 0
