"""Persistent KV cache, backed by a sqlite or postgres database."""
import threading
import time

import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext import declarative

from sky import __commit__ as sky_commit
from sky import __version__ as sky_version
from sky import sky_logging
from sky.metrics import utils as metrics_lib
from sky.utils import common_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

logger = sky_logging.init_logger(__name__)

_COMPONENT = 'kv_cache'
_LEGACY_BACKEND_EVENT = 'skypilot.persistence.legacy_backend_used'
_legacy_sqlite_marker_lock = threading.Lock()
_legacy_sqlite_marker_emitted = False

Base = declarative.declarative_base()

kv_cache_table = sqlalchemy.Table(
    'kv_cache',
    Base.metadata,
    sqlalchemy.Column('key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('value', sqlalchemy.Text),
    sqlalchemy.Column('expires_at', sqlalchemy.Float),
)


def create_table(engine: sqlalchemy.engine.Engine):
    # Enable WAL mode to avoid locking issues.
    # See: issue #1441 and PR #1509
    # https://github.com/microsoft/WSL/issues/2395
    # TODO(romilb): We do not enable WAL for WSL because of known issue in WSL.
    #  This may cause the database locked problem from WSL issue #1441.
    if (engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value and
            not common_utils.is_wsl()):
        try:
            with orm.Session(engine) as session:
                session.execute(sqlalchemy.text('PRAGMA journal_mode=WAL'))
                session.commit()
        except sqlalchemy_exc.OperationalError as e:
            if 'database is locked' not in str(e):
                raise
            # If the database is locked, it is OK to continue, as the WAL mode
            # is not critical and is likely to be enabled by other processes.

    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.KV_CACHE_DB_NAME,
                                         migration_utils.KV_CACHE_VERSION)


_db_manager = db_utils.DatabaseManager('kv_cache', create_table)


def _get_engine(operation: str, phase: str) -> sqlalchemy.engine.Engine:
    """Returns the cache engine and records migration observability."""
    engine = _db_manager.get_engine()
    backend = engine.dialect.name
    if metrics_lib.METRICS_ENABLED:
        metrics_lib.record_persistence_operation(_COMPONENT, operation, phase,
                                                 backend)
    if backend == db_utils.SQLAlchemyDialect.SQLITE.value:
        _emit_legacy_sqlite_marker(operation, phase)
    return engine


def _emit_legacy_sqlite_marker(operation: str, phase: str) -> None:
    """Emits one non-sensitive SQLite-use marker in each process."""
    global _legacy_sqlite_marker_emitted
    if _legacy_sqlite_marker_emitted:
        return
    with _legacy_sqlite_marker_lock:
        if _legacy_sqlite_marker_emitted:
            return
        logger.warning(
            'event_name=%s component=%s operation=%s phase=%s backend=%s '
            'server_version=%s server_commit=%s', _LEGACY_BACKEND_EVENT,
            _COMPONENT, operation, phase,
            db_utils.SQLAlchemyDialect.SQLITE.value, sky_version, sky_commit)
        _legacy_sqlite_marker_emitted = True


@metrics_lib.time_me
def add_or_update_cache_entry(
    key: str,
    value: str,
    expires_at: float,
) -> None:
    """Store the mapping from user hash to user name for display purposes.

    Args:
        key: The key of the cache entry.
        value: The value of the cache entry.
        expires_at: The timestamp when the cache entry expires.
    """
    engine = _get_engine('add_or_update', 'write')
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        insert_func = sqlite.insert
    elif engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        insert_func = postgresql.insert
    else:
        raise ValueError('Unsupported database dialect')

    with orm.Session(engine) as session:
        insert_stmt = insert_func(kv_cache_table).values(key=key,
                                                         value=value,
                                                         expires_at=expires_at)
        do_update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[kv_cache_table.c.key],
            set_={
                kv_cache_table.c.value: value,
                kv_cache_table.c.expires_at: expires_at
            })
        session.execute(do_update_stmt)

        session.commit()


@metrics_lib.time_me
def add_or_extend_cache_entry(
    key: str,
    value: str,
    expires_at: float,
) -> None:
    """Store an entry without shortening an existing expiration.

    This is useful for negative-cache hints written concurrently by multiple
    worker processes: a delayed older writer must not shorten a newer hint.
    """
    engine = _get_engine('add_or_extend', 'write')
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        insert_func = sqlite.insert
        greatest = sqlalchemy.func.max
    elif engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        insert_func = postgresql.insert
        greatest = sqlalchemy.func.greatest
    else:
        raise ValueError('Unsupported database dialect')

    with orm.Session(engine) as session:
        insert_stmt = insert_func(kv_cache_table).values(key=key,
                                                         value=value,
                                                         expires_at=expires_at)
        existing_expiry = sqlalchemy.func.coalesce(
            kv_cache_table.c.expires_at, insert_stmt.excluded.expires_at)
        value_at_latest_expiry = sqlalchemy.case((sqlalchemy.or_(
            kv_cache_table.c.expires_at.is_(None),
            insert_stmt.excluded.expires_at
            >= kv_cache_table.c.expires_at), insert_stmt.excluded.value),
                                                 else_=kv_cache_table.c.value)
        do_update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[kv_cache_table.c.key],
            set_={
                kv_cache_table.c.value: value_at_latest_expiry,
                kv_cache_table.c.expires_at: greatest(
                    existing_expiry, insert_stmt.excluded.expires_at)
            })
        session.execute(do_update_stmt)
        session.commit()


@metrics_lib.time_me
def get_cache_entry(key: str) -> str | None:
    """Get the value of the cache entry.

    Args:
        key: The key of the cache entry.
    """
    engine = _get_engine('get', 'read')
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(kv_cache_table.c.value).where(
                kv_cache_table.c.key == key).where(
                    kv_cache_table.c.expires_at > time.time()))
        return result.scalar()


@metrics_lib.time_me
def delete_cache_entry(key: str) -> None:
    """Delete exactly one cache entry."""
    engine = _get_engine('delete', 'write')
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(kv_cache_table).where(
                kv_cache_table.c.key == key))
        session.commit()


_LIKE_ESCAPE_CHAR = '\\'


def _escape_like(value: str) -> str:
    """Escape SQL LIKE wildcard characters (%, _) in a literal value."""
    return (value.replace(_LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2).replace(
        '%', f'{_LIKE_ESCAPE_CHAR}%').replace('_', f'{_LIKE_ESCAPE_CHAR}_'))


@metrics_lib.time_me
def delete_cache_entries_by_prefix(prefix: str) -> None:
    """Delete all cache entries whose key starts with the given prefix.

    Any SQL LIKE wildcards (%, _) in *prefix* are escaped so they are
    matched literally.

    Args:
        prefix: The literal prefix to match against cache keys.
    """
    escaped = _escape_like(prefix)
    engine = _get_engine('delete_prefix', 'write')
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(kv_cache_table).where(
                kv_cache_table.c.key.like(f'{escaped}%',
                                          escape=_LIKE_ESCAPE_CHAR)))
        session.commit()


@metrics_lib.time_me
def delete_cache_entries_by_prefix_suffix(prefix: str, suffix: str) -> None:
    """Delete all cache entries whose key starts with *prefix* and ends
    with *suffix*, with any content in between.

    Both *prefix* and *suffix* are treated as literal strings — any SQL
    LIKE wildcards (%, _) they contain are escaped automatically.

    Args:
        prefix: Literal prefix to match against cache keys.
        suffix: Literal suffix to match against cache keys.
    """
    escaped_prefix = _escape_like(prefix)
    escaped_suffix = _escape_like(suffix)
    pattern = f'{escaped_prefix}%{escaped_suffix}'
    engine = _get_engine('delete_prefix_suffix', 'write')
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(kv_cache_table).where(
                kv_cache_table.c.key.like(pattern, escape=_LIKE_ESCAPE_CHAR)))
        session.commit()
