"""Cloud-check cache persistence for the global user state database."""

import json
import logging
import typing
from typing import Any

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky import global_user_state_schema
from sky.utils import registry
from sky.utils.db import db_utils

if typing.TYPE_CHECKING:
    from sky import clouds
    from sky.clouds import cloud

_ENABLED_CLOUDS_KEY_PREFIX = 'enabled_clouds_'
_ALLOWED_CLOUDS_KEY_PREFIX = 'allowed_clouds_'
_CHECK_RESULTS_KEY_PREFIX = 'check_results_'

config_table = global_user_state_schema.config_table


def _get_enabled_clouds_key(cloud_capability: 'cloud.CloudCapability',
                            workspace: str) -> str:
    return _ENABLED_CLOUDS_KEY_PREFIX + workspace + '_' + cloud_capability.value


def get_cached_enabled_clouds(engine: sqlalchemy.engine.Engine,
                              cloud_capability: 'cloud.CloudCapability',
                              workspace: str) -> list['clouds.Cloud']:
    with orm.Session(engine) as session:
        row = session.query(config_table).filter_by(
            key=_get_enabled_clouds_key(cloud_capability, workspace)).first()
    ret = []
    if row:
        ret = json.loads(row.value)
    enabled_clouds: list[clouds.Cloud] = []
    for c in ret:
        try:
            cloud = registry.CLOUD_REGISTRY.from_str(c)
        except ValueError:
            # Handle the case for the clouds whose support has been
            # removed from SkyPilot, e.g., 'local' was a cloud in the past
            # and may be stored in the database for users before #3037.
            # We should ignore removed clouds and continue.
            continue
        if cloud is not None:
            enabled_clouds.append(cloud)
    return enabled_clouds


def set_enabled_clouds(engine: sqlalchemy.engine.Engine,
                       enabled_clouds: list[str],
                       cloud_capability: 'cloud.CloudCapability',
                       workspace: str) -> None:
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')
        insert_stmnt = insert_func(config_table).values(
            key=_get_enabled_clouds_key(cloud_capability, workspace),
            value=json.dumps(enabled_clouds))
        do_update_stmt = insert_stmnt.on_conflict_do_update(
            index_elements=[config_table.c.key],
            set_={config_table.c.value: json.dumps(enabled_clouds)})
        session.execute(do_update_stmt)
        session.commit()


def _get_check_results_key(workspace: str) -> str:
    return f'{_CHECK_RESULTS_KEY_PREFIX}{workspace}'


def get_cached_check_results(
        engine: sqlalchemy.engine.Engine, workspace: str,
        logger: logging.Logger) -> dict[str, dict[str, dict[str, Any]]]:
    with orm.Session(engine) as session:
        row = session.query(config_table).filter_by(
            key=_get_check_results_key(workspace)).first()
    if row is None or row.value is None:
        return {}
    try:
        return json.loads(row.value)
    except (json.JSONDecodeError, TypeError):
        logger.warning(
            f'Corrupt check_results row for workspace {workspace!r}; '
            f'returning empty dict.')
        return {}


def set_check_results(
    engine: sqlalchemy.engine.Engine,
    results: dict[str, dict[str, dict[str, Any]]],
    workspace: str,
    logger: logging.Logger,
    *,
    is_full_workspace_run: bool,
) -> None:
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        insert_func = sqlite.insert
    elif engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        insert_func = postgresql.insert
    else:
        raise ValueError('Unsupported database dialect')

    key = _get_check_results_key(workspace)
    with orm.Session(engine) as session:
        if is_full_workspace_run:
            new_value = results
        else:
            # Read-modify-write under the default session isolation. This
            # is NOT race-safe against concurrent scoped writes for
            # different clouds in the same workspace: SQLAlchemy
            # `orm.Session` does not acquire row locks, and under the
            # default isolation (READ COMMITTED on Postgres, deferred on
            # SQLite) two interleaved RMW cycles can clobber each
            # other's per-cloud updates. The blast radius is limited
            # (one scoped run's leaves get overwritten until the next
            # write rewrites the row) and the source-of-truth
            # enabled_clouds_* rows are unaffected, so we accept the
            # race here rather than serialize through a per-workspace
            # advisory lock. If this row ever becomes load-bearing for
            # correctness, switch to `with_for_update()` (postgres) and
            # an explicit BEGIN IMMEDIATE (sqlite).
            row = session.query(config_table).filter_by(key=key).first()
            existing: dict[str, dict[str, dict[str, Any]]] = {}
            if row is not None and row.value is not None:
                try:
                    existing = json.loads(row.value)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f'Corrupt check_results row for workspace '
                                   f'{workspace!r}; replacing.')
                    existing = {}
            new_value = dict(existing)
            for cloud_repr, ctx_dict in results.items():
                existing_for_cloud = new_value.get(cloud_repr)
                if not isinstance(existing_for_cloud, dict):
                    existing_for_cloud = {}
                new_value[cloud_repr] = {**existing_for_cloud, **ctx_dict}

        serialized = json.dumps(new_value)
        insert_stmnt = insert_func(config_table).values(key=key,
                                                        value=serialized)
        do_update_stmt = insert_stmnt.on_conflict_do_update(
            index_elements=[config_table.c.key],
            set_={config_table.c.value: serialized})
        session.execute(do_update_stmt)
        session.commit()


def _get_allowed_clouds_key(workspace: str) -> str:
    return _ALLOWED_CLOUDS_KEY_PREFIX + workspace


def get_allowed_clouds(engine: sqlalchemy.engine.Engine,
                       workspace: str) -> list[str]:
    with orm.Session(engine) as session:
        row = session.query(config_table).filter_by(
            key=_get_allowed_clouds_key(workspace)).first()
    if row:
        return json.loads(row.value)
    return []


def set_allowed_clouds(engine: sqlalchemy.engine.Engine,
                       allowed_clouds: list[str], workspace: str) -> None:
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')
        insert_stmnt = insert_func(config_table).values(
            key=_get_allowed_clouds_key(workspace),
            value=json.dumps(allowed_clouds))
        do_update_stmt = insert_stmnt.on_conflict_do_update(
            index_elements=[config_table.c.key],
            set_={config_table.c.value: json.dumps(allowed_clouds)})
        session.execute(do_update_stmt)
        session.commit()
