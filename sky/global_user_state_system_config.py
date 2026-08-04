"""Persistence for process-wide system configuration values."""

from typing import Any

import sqlalchemy
from sqlalchemy import orm

from sky import global_user_state_schema
from sky.utils.db import db_utils


def _insert_for_engine(engine: sqlalchemy.engine.Engine, sqlite_dialect: Any,
                       postgresql_dialect: Any) -> Any:
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        return sqlite_dialect.insert
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return postgresql_dialect.insert
    raise ValueError('Unsupported database dialect')


def get_system_config(engine: sqlalchemy.engine.Engine,
                      config_key: str) -> str | None:
    """Get a system configuration value by key."""
    with orm.Session(engine) as session:
        row = session.query(
            global_user_state_schema.system_config_table).filter_by(
                config_key=config_key).first()
    if row is None:
        return None
    return row.config_value


def get_or_set_system_config(engine: sqlalchemy.engine.Engine, config_key: str,
                             default_value: str, current_time: int,
                             sqlite_dialect: Any,
                             postgresql_dialect: Any) -> str:
    """Atomically return an existing configuration or install a default."""
    insert_func = _insert_for_engine(engine, sqlite_dialect, postgresql_dialect)
    system_config_table = global_user_state_schema.system_config_table
    insert_stmnt = insert_func(system_config_table).values(
        config_key=config_key,
        config_value=default_value,
        created_at=current_time,
        updated_at=current_time).on_conflict_do_nothing(
            index_elements=[system_config_table.c.config_key])
    with orm.Session(engine) as session:
        session.execute(insert_stmnt)
        value = session.execute(
            sqlalchemy.select(system_config_table.c.config_value).where(
                system_config_table.c.config_key == config_key)).scalar_one()
        session.commit()
    return str(value)


def set_system_config(engine: sqlalchemy.engine.Engine, config_key: str,
                      config_value: str, current_time: int, sqlite_dialect: Any,
                      postgresql_dialect: Any) -> None:
    """Set a system configuration value."""
    system_config_table = global_user_state_schema.system_config_table
    with orm.Session(engine) as session:
        insert_func = _insert_for_engine(engine, sqlite_dialect,
                                         postgresql_dialect)
        insert_stmnt = insert_func(system_config_table).values(
            config_key=config_key,
            config_value=config_value,
            created_at=current_time,
            updated_at=current_time)

        upsert_stmnt = insert_stmnt.on_conflict_do_update(
            index_elements=[system_config_table.c.config_key],
            set_={
                system_config_table.c.config_value: config_value,
                system_config_table.c.updated_at: current_time,
            })
        session.execute(upsert_stmnt)
        session.commit()
