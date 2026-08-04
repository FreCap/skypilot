"""Persistence repository for storage metadata and lifecycle state."""

import pickle
import time
from typing import Any

import sqlalchemy

from sky.utils import common_utils
from sky.utils import status_lib
from sky.utils.db import db_utils


def add_or_update_storage(engine: sqlalchemy.engine.Engine,
                          session_factory: Any, sqlite_dialect: Any,
                          postgresql_dialect: Any,
                          storage_table: sqlalchemy.Table, storage_name: str,
                          storage_handle: Any,
                          storage_status: status_lib.StorageStatus) -> None:
    """Insert or replace a storage row."""
    storage_launched_at = int(time.time())
    handle = pickle.dumps(storage_handle)
    last_use = common_utils.get_current_command()

    def status_check(status):
        return status in status_lib.StorageStatus

    if not status_check(storage_status):
        raise ValueError(f'Error in updating global state. Storage Status '
                         f'{storage_status} is passed in incorrectly')
    with session_factory(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite_dialect.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql_dialect.insert
        else:
            raise ValueError('Unsupported database dialect')
        insert_stmnt = insert_func(storage_table).values(
            name=storage_name,
            handle=handle,
            last_use=last_use,
            launched_at=storage_launched_at,
            status=storage_status.value)
        do_update_stmt = insert_stmnt.on_conflict_do_update(
            index_elements=[storage_table.c.name],
            set_={
                storage_table.c.handle: handle,
                storage_table.c.last_use: last_use,
                storage_table.c.launched_at: storage_launched_at,
                storage_table.c.status: storage_status.value
            })
        session.execute(do_update_stmt)
        session.commit()


def remove_storage(engine: sqlalchemy.engine.Engine, session_factory: Any,
                   storage_table: sqlalchemy.Table, storage_name: str) -> None:
    """Remove one storage row."""
    with session_factory(engine) as session:
        session.query(storage_table).filter_by(name=storage_name).delete()
        session.commit()


def set_storage_status(engine: sqlalchemy.engine.Engine, session_factory: Any,
                       storage_table: sqlalchemy.Table, storage_name: str,
                       status: status_lib.StorageStatus) -> None:
    """Set one storage lifecycle status."""
    with session_factory(engine) as session:
        count = session.query(storage_table).filter_by(
            name=storage_name).update({storage_table.c.status: status.value})
        session.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Storage {storage_name} not found.')


def get_storage_status(engine: sqlalchemy.engine.Engine, session_factory: Any,
                       storage_table: sqlalchemy.Table,
                       storage_name: str) -> status_lib.StorageStatus | None:
    """Get one storage lifecycle status."""
    assert storage_name is not None, 'storage_name cannot be None'
    with session_factory(engine) as session:
        row = session.query(storage_table).filter_by(name=storage_name).first()
    if row:
        return status_lib.StorageStatus[row.status]
    return None


def set_storage_handle(engine: sqlalchemy.engine.Engine, session_factory: Any,
                       storage_table: sqlalchemy.Table, storage_name: str,
                       handle: Any) -> None:
    """Replace one serialized storage handle."""
    with session_factory(engine) as session:
        count = session.query(storage_table).filter_by(
            name=storage_name).update(
                {storage_table.c.handle: pickle.dumps(handle)})
        session.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Storage{storage_name} not found.')


def get_handle_from_storage_name(engine: sqlalchemy.engine.Engine,
                                 session_factory: Any,
                                 storage_table: sqlalchemy.Table,
                                 storage_name: str | None) -> Any | None:
    """Get and deserialize one storage handle."""
    if storage_name is None:
        return None
    with session_factory(engine) as session:
        row = session.query(storage_table).filter_by(name=storage_name).first()
    if row:
        return pickle.loads(row.handle)
    return None


def get_glob_storage_name(engine: sqlalchemy.engine.Engine,
                          session_factory: Any, storage_table: sqlalchemy.Table,
                          storage_name: str, glob_to_similar: Any) -> list[str]:
    """Get storage names matching the database-specific glob expression."""
    assert storage_name is not None, 'storage_name cannot be None'
    with session_factory(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            rows = session.query(storage_table).filter(
                storage_table.c.name.op('GLOB')(storage_name)).all()
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            rows = session.query(storage_table).filter(
                storage_table.c.name.op('SIMILAR TO')(
                    glob_to_similar(storage_name))).all()
        else:
            raise ValueError('Unsupported database dialect')
    return [row.name for row in rows]


def get_storage_names_start_with(engine: sqlalchemy.engine.Engine,
                                 session_factory: Any,
                                 storage_table: sqlalchemy.Table,
                                 starts_with: str) -> list[str]:
    """Get storage names with the supplied prefix."""
    with session_factory(engine) as session:
        rows = session.query(storage_table).filter(
            storage_table.c.name.like(f'{starts_with}%')).all()
    return [row.name for row in rows]


def get_storage(engine: sqlalchemy.engine.Engine, session_factory: Any,
                storage_table: sqlalchemy.Table) -> list[dict[str, Any]]:
    """Project all storage rows for user-facing list operations."""
    with session_factory(engine) as session:
        rows = session.query(storage_table).all()
    records = []
    for row in rows:
        # TODO: use namedtuple instead of dict
        records.append({
            'name': row.name,
            'launched_at': row.launched_at,
            'handle': pickle.loads(row.handle),
            'last_use': row.last_use,
            'status': status_lib.StorageStatus[row.status],
        })
    return records
