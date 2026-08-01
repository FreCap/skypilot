"""Persistence repository for volume metadata and lifecycle state."""

from collections.abc import Callable
import json
import pickle
import time
from typing import Any

import sqlalchemy

from sky import models
from sky import skypilot_config
from sky.utils import common_utils
from sky.utils import status_lib
from sky.utils.db import db_utils


def get_volume_names_start_with(engine: sqlalchemy.engine.Engine,
                                session_factory: Any,
                                volume_table: sqlalchemy.Table,
                                starts_with: str) -> list[str]:
    """Get volume names with the supplied prefix."""
    with session_factory(engine) as session:
        rows = session.query(volume_table).filter(
            volume_table.c.name.like(f'{starts_with}%')).all()
    return [row.name for row in rows]


def get_volumes(engine: sqlalchemy.engine.Engine,
                session_factory: Any,
                volume_table: sqlalchemy.Table,
                is_ephemeral: bool | None = None,
                name: str | None = None) -> list[dict[str, Any]]:
    """Project volume rows for list operations."""
    with session_factory(engine) as session:
        filters: dict[str, Any] = {}
        if name is not None:
            filters['name'] = name
        if is_ephemeral is not None:
            filters['is_ephemeral'] = int(is_ephemeral)
        query = session.query(volume_table)
        rows = query.filter_by(**filters).all() if filters else query.all()
    records = []
    for row in rows:
        # Decode JSON-encoded usedby fields.
        usedby_pods = json.loads(row.usedby_pods) if row.usedby_pods else []
        usedby_clusters = (json.loads(row.usedby_clusters)
                           if row.usedby_clusters else [])
        records.append({
            'name': row.name,
            'launched_at': row.launched_at,
            'handle': pickle.loads(row.handle),
            'user_hash': row.user_hash,
            'workspace': row.workspace,
            'last_attached_at': row.last_attached_at,
            'last_use': row.last_use,
            'status': status_lib.VolumeStatus[row.status],
            'is_ephemeral': bool(row.is_ephemeral),
            'error_message': row.error_message,
            'usedby_pods': usedby_pods,
            'usedby_clusters': usedby_clusters,
            'creation_yaml': row.creation_yaml,
        })
    return records


def get_volume_configs_by_names(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    volume_table: sqlalchemy.Table,
    names: list[str],
) -> dict[str, models.VolumeConfig]:
    """Return one snapshot of the requested volume configs, keyed by name."""
    unique_names = tuple(dict.fromkeys(names))
    if not unique_names:
        return {}
    engine = engine_getter()
    with session_factory(engine) as session:
        rows = session.query(volume_table.c.name, volume_table.c.handle).filter(
            volume_table.c.name.in_(unique_names)).all()
    return {row.name: pickle.loads(row.handle) for row in rows}


def get_volume_by_name(engine: sqlalchemy.engine.Engine, session_factory: Any,
                       volume_table: sqlalchemy.Table,
                       name: str) -> dict[str, Any] | None:
    """Project one volume row by name."""
    with session_factory(engine) as session:
        row = session.query(volume_table).filter_by(name=name).first()
    if row:
        # Decode JSON-encoded usedby fields.
        usedby_pods = json.loads(row.usedby_pods) if row.usedby_pods else []
        usedby_clusters = (json.loads(row.usedby_clusters)
                           if row.usedby_clusters else [])
        return {
            'name': row.name,
            'launched_at': row.launched_at,
            'handle': pickle.loads(row.handle),
            'user_hash': row.user_hash,
            'workspace': row.workspace,
            'last_attached_at': row.last_attached_at,
            'last_use': row.last_use,
            'status': status_lib.VolumeStatus[row.status],
            'error_message': row.error_message,
            'usedby_pods': usedby_pods,
            'usedby_clusters': usedby_clusters,
            'creation_yaml': row.creation_yaml,
        }
    return None


def add_volume(engine: sqlalchemy.engine.Engine, session_factory: Any,
               sqlite_dialect: Any, postgresql_dialect: Any,
               volume_table: sqlalchemy.Table, name: str,
               config: models.VolumeConfig, status: status_lib.VolumeStatus,
               is_ephemeral: bool, creation_yaml: str | None) -> None:
    """Insert one volume row, leaving an existing row unchanged."""
    volume_launched_at = int(time.time())
    handle = pickle.dumps(config)
    last_use = common_utils.get_current_command()
    user_hash = common_utils.get_current_user().id
    active_workspace = skypilot_config.get_active_workspace()
    if is_ephemeral:
        last_attached_at = int(time.time())
        status = status_lib.VolumeStatus.IN_USE
    else:
        last_attached_at = None

    with session_factory(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite_dialect.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql_dialect.insert
        else:
            raise ValueError('Unsupported database dialect')
        insert_stmnt = insert_func(volume_table).values(
            name=name,
            launched_at=volume_launched_at,
            handle=handle,
            user_hash=user_hash,
            workspace=active_workspace,
            last_attached_at=last_attached_at,
            last_use=last_use,
            status=status.value,
            is_ephemeral=int(is_ephemeral),
            creation_yaml=creation_yaml,
        )
        do_update_stmt = insert_stmnt.on_conflict_do_nothing()
        session.execute(do_update_stmt)
        session.commit()


def update_volume_config(engine: sqlalchemy.engine.Engine, session_factory: Any,
                         volume_table: sqlalchemy.Table, name: str,
                         config: models.VolumeConfig) -> None:
    """Replace one serialized volume configuration."""
    with session_factory(engine) as session:
        session.query(volume_table).filter_by(name=name).update({
            volume_table.c.handle: pickle.dumps(config),
        })
        session.commit()


def update_volume(engine: sqlalchemy.engine.Engine, session_factory: Any,
                  volume_table: sqlalchemy.Table, name: str,
                  last_attached_at: int,
                  status: status_lib.VolumeStatus) -> None:
    """Update one volume attachment timestamp and status."""
    with session_factory(engine) as session:
        session.query(volume_table).filter_by(name=name).update({
            volume_table.c.last_attached_at: last_attached_at,
            volume_table.c.status: status.value,
        })
        session.commit()


def update_volume_status(engine: sqlalchemy.engine.Engine, session_factory: Any,
                         volume_table: sqlalchemy.Table, name: str,
                         status: status_lib.VolumeStatus,
                         error_message: str | None,
                         usedby_pods: list[str] | None,
                         usedby_clusters: list[str] | None) -> None:
    """Update volume status and optional error and attachment projections."""
    with session_factory(engine) as session:
        update_dict: dict[str, Any] = {
            volume_table.c.status: status.value,
        }
        # Always update error_message (None clears it).
        update_dict[volume_table.c.error_message] = error_message
        if usedby_pods is not None:
            update_dict[volume_table.c.usedby_pods] = json.dumps(usedby_pods)
        if usedby_clusters is not None:
            update_dict[volume_table.c.usedby_clusters] = json.dumps(
                usedby_clusters)
        session.query(volume_table).filter_by(name=name).update(update_dict)
        session.commit()


def delete_volume(engine: sqlalchemy.engine.Engine, session_factory: Any,
                  volume_table: sqlalchemy.Table, name: str) -> None:
    """Delete one volume row."""
    with session_factory(engine) as session:
        session.query(volume_table).filter_by(name=name).delete()
        session.commit()
