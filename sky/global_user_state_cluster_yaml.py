"""Persistence gateway for cluster YAML strings."""

from typing import Any

import sqlalchemy

from sky.utils.db import db_utils


def get_cluster_yaml(engine: sqlalchemy.engine.Engine, session_factory: Any,
                     cluster_yaml_table: sqlalchemy.Table,
                     cluster_name: str) -> tuple[bool, str | None]:
    """Read one cluster YAML string by cluster name."""
    with session_factory(engine) as session:
        row = session.query(cluster_yaml_table).filter_by(
            cluster_name=cluster_name).first()
    if row is None:
        return False, None
    return True, row.yaml


def get_cluster_yamls(engine: sqlalchemy.engine.Engine, session_factory: Any,
                      cluster_yaml_table: sqlalchemy.Table,
                      cluster_names: list[str]) -> dict[str, str | None]:
    """Read cluster YAML strings for a deduplicated name list."""
    with session_factory(engine) as session:
        rows = session.query(cluster_yaml_table).filter(
            cluster_yaml_table.c.cluster_name.in_(cluster_names)).all()
    return {row.cluster_name: row.yaml for row in rows}


def set_cluster_yaml(engine: sqlalchemy.engine.Engine,
                     session_engine: sqlalchemy.engine.Engine,
                     session_factory: Any, sqlite_dialect: Any,
                     postgresql_dialect: Any,
                     cluster_yaml_table: sqlalchemy.Table, cluster_name: str,
                     yaml_str: str) -> None:
    """Upsert one cluster YAML string."""
    with session_factory(session_engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite_dialect.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql_dialect.insert
        else:
            raise ValueError('Unsupported database dialect')
        insert_stmnt = insert_func(cluster_yaml_table).values(
            cluster_name=cluster_name, yaml=yaml_str)
        do_update_stmt = insert_stmnt.on_conflict_do_update(
            index_elements=[cluster_yaml_table.c.cluster_name],
            set_={cluster_yaml_table.c.yaml: yaml_str})
        session.execute(do_update_stmt)
        session.commit()


def remove_cluster_yaml(engine: sqlalchemy.engine.Engine, session_factory: Any,
                        cluster_yaml_table: sqlalchemy.Table,
                        cluster_name: str) -> None:
    """Delete one cluster YAML string."""
    with session_factory(engine) as session:
        session.query(cluster_yaml_table).filter_by(
            cluster_name=cluster_name).delete()
        session.commit()
