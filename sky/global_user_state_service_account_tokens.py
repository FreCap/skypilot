"""Service-account-token persistence for the global user state database."""

from collections.abc import Callable
from typing import Any

import sqlalchemy
from sqlalchemy import orm

from sky import global_user_state_schema
from sky.utils.db import db_utils

service_account_token_table: sqlalchemy.Table = (
    global_user_state_schema.
    service_account_token_table  # type: ignore[has-type]
)


def add_service_account_token(
    engine: sqlalchemy.engine.Engine,
    session_factory: type[orm.Session],
    sqlite_insert: Callable[[sqlalchemy.Table], Any],
    postgresql_insert: Callable[[sqlalchemy.Table], Any],
    token_id: str,
    token_name: str,
    token_hash: str,
    creator_user_hash: str,
    service_account_user_id: str,
    expires_at: int | None,
    created_at: int,
) -> None:
    """Add a service account token to the database."""
    with session_factory(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite_insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql_insert
        else:
            raise ValueError('Unsupported database dialect')

        insert_stmnt = insert_func(service_account_token_table).values(
            token_id=token_id,
            token_name=token_name,
            token_hash=token_hash,
            created_at=created_at,
            expires_at=expires_at,
            creator_user_hash=creator_user_hash,
            service_account_user_id=service_account_user_id)
        session.execute(insert_stmnt)
        session.commit()


def get_service_account_token(engine: sqlalchemy.engine.Engine,
                              session_factory: type[orm.Session],
                              token_id: str) -> dict[str, Any] | None:
    """Get a service account token by token_id."""
    with session_factory(engine) as session:
        row = session.query(service_account_token_table).filter_by(
            token_id=token_id).first()
    if row is None:
        return None
    return {
        'token_id': row.token_id,
        'token_name': row.token_name,
        'token_hash': row.token_hash,
        'created_at': row.created_at,
        'last_used_at': row.last_used_at,
        'expires_at': row.expires_at,
        'creator_user_hash': row.creator_user_hash,
        'service_account_user_id': row.service_account_user_id,
    }


def get_service_account_token_by_hash(engine: sqlalchemy.engine.Engine,
                                      session_factory: type[orm.Session],
                                      token_hash: str) -> dict[str, Any] | None:
    """Get a service account token by its sha256 hash.

    Used by the request-auth middleware: hashing the incoming bearer token
    and matching against this column is what makes revocation and rotation
    take effect (the DB row's hash is updated on rotation, so old JWTs
    stop matching). Relies on the unique index on token_hash.
    """
    with session_factory(engine) as session:
        row = session.query(service_account_token_table).filter_by(
            token_hash=token_hash).first()
    if row is None:
        return None
    return {
        'token_id': row.token_id,
        'token_name': row.token_name,
        'token_hash': row.token_hash,
        'created_at': row.created_at,
        'last_used_at': row.last_used_at,
        'expires_at': row.expires_at,
        'creator_user_hash': row.creator_user_hash,
        'service_account_user_id': row.service_account_user_id,
    }


def get_user_service_account_tokens(engine: sqlalchemy.engine.Engine,
                                    session_factory: type[orm.Session],
                                    user_hash: str) -> list[dict[str, Any]]:
    """Get all service account tokens for a user (as creator)."""
    with session_factory(engine) as session:
        rows = session.query(service_account_token_table).filter_by(
            creator_user_hash=user_hash).all()
    return [{
        'token_id': row.token_id,
        'token_name': row.token_name,
        'token_hash': row.token_hash,
        'created_at': row.created_at,
        'last_used_at': row.last_used_at,
        'expires_at': row.expires_at,
        'creator_user_hash': row.creator_user_hash,
        'service_account_user_id': row.service_account_user_id,
    } for row in rows]


def update_service_account_token_last_used(engine: sqlalchemy.engine.Engine,
                                           session_factory: type[orm.Session],
                                           token_id: str,
                                           last_used_at: int) -> None:
    """Update the last_used_at timestamp for a service account token."""
    with session_factory(engine) as session:
        session.query(service_account_token_table).filter_by(
            token_id=token_id).update(
                {service_account_token_table.c.last_used_at: last_used_at})
        session.commit()


def delete_service_account_token(engine: sqlalchemy.engine.Engine,
                                 session_factory: type[orm.Session],
                                 token_id: str) -> bool:
    """Delete a service account token."""
    with session_factory(engine) as session:
        result = session.query(service_account_token_table).filter_by(
            token_id=token_id).delete()
        session.commit()
    return result > 0


def rotate_service_account_token(engine: sqlalchemy.engine.Engine,
                                 session_factory: type[orm.Session],
                                 token_id: str, new_token_hash: str,
                                 new_expires_at: int | None,
                                 current_time: int) -> None:
    """Rotate a service account token's hash and expiration."""
    with session_factory(engine) as session:
        count = session.query(service_account_token_table).filter_by(
            token_id=token_id).update({
                service_account_token_table.c.token_hash: new_token_hash,
                service_account_token_table.c.expires_at: new_expires_at,
                service_account_token_table.c.last_used_at: None,
                service_account_token_table.c.created_at: current_time,
            })
        session.commit()

    if count == 0:
        raise ValueError(f'Service account token {token_id} not found.')


def get_expired_service_account_tokens_by_name_prefix(
        engine: sqlalchemy.engine.Engine, session_factory: type[orm.Session],
        name_prefix: str, now: int) -> list[dict[str, Any]]:
    """Return expired service-account tokens matching a name prefix."""
    escaped_prefix = name_prefix.replace('\\', '\\\\').replace('%',
                                                               '\\%').replace(
                                                                   '_', '\\_')
    like_pattern = f'{escaped_prefix}%'
    with session_factory(engine) as session:
        rows = session.query(service_account_token_table).filter(
            service_account_token_table.c.token_name.like(like_pattern,
                                                          escape='\\'),
            service_account_token_table.c.expires_at.isnot(None),
            service_account_token_table.c.expires_at < now,
        ).all()
    return [{
        'token_id': row.token_id,
        'token_name': row.token_name,
        'token_hash': row.token_hash,
        'created_at': row.created_at,
        'last_used_at': row.last_used_at,
        'expires_at': row.expires_at,
        'creator_user_hash': row.creator_user_hash,
        'service_account_user_id': row.service_account_user_id,
    } for row in rows]


def get_all_service_account_tokens(
        engine: sqlalchemy.engine.Engine,
        session_factory: type[orm.Session]) -> list[dict[str, Any]]:
    """Get all service account tokens across all users."""
    with session_factory(engine) as session:
        rows = session.query(service_account_token_table).all()
    return [{
        'token_id': row.token_id,
        'token_name': row.token_name,
        'token_hash': row.token_hash,
        'created_at': row.created_at,
        'last_used_at': row.last_used_at,
        'expires_at': row.expires_at,
        'creator_user_hash': row.creator_user_hash,
        'service_account_user_id': row.service_account_user_id,
    } for row in rows]
