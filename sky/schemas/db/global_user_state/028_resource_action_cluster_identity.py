"""Add the write-once resource-action cluster identity commitment.

Revision ID: 028
Revises: 027
Create Date: 2026-08-01

"""
# pylint: disable=invalid-name
from collections.abc import Mapping
from collections.abc import Sequence
import re
from typing import Any

from alembic import op
import sqlalchemy

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '028'
down_revision: str | Sequence[str] | None = '027'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'clusters'
_COLUMN = 'cluster_record_uuid'
_INDEX = 'uq_clusters_cluster_record_uuid_nonnull'
_PREDICATE = 'cluster_record_uuid IS NOT NULL'


def _supported_dialect(bind: sqlalchemy.engine.Connection) -> str:
    dialect = bind.dialect.name
    supported = {
        db_utils.SQLAlchemyDialect.POSTGRESQL.value,
        db_utils.SQLAlchemyDialect.SQLITE.value,
    }
    if dialect not in supported:
        raise RuntimeError(
            'Migration 028 supports only PostgreSQL and SQLite global state; '
            f'found {dialect!r}.')
    return dialect


def _columns(
        bind: sqlalchemy.engine.Connection) -> dict[str, Mapping[str, Any]]:
    return {
        str(column['name']): column
        for column in sqlalchemy.inspect(bind).get_columns(_TABLE)
    }


def _validate_column(bind: sqlalchemy.engine.Connection) -> None:
    column = _columns(bind).get(_COLUMN)
    if column is None:
        raise RuntimeError(
            'Migration 028 could not create clusters.cluster_record_uuid.')
    actual_type = str(
        column['type'].compile(dialect=bind.dialect)).upper().replace(' ', '')
    expected_type = str(sqlalchemy.Uuid().compile(
        dialect=bind.dialect)).upper().replace(' ', '')
    if (actual_type != expected_type or not bool(column.get('nullable')) or
            column.get('default') is not None or
            column.get('computed') is not None or
            column.get('identity') is not None):
        raise RuntimeError(
            'Migration 028 found an incompatible '
            'clusters.cluster_record_uuid column; expected nullable '
            f'{expected_type} without a default, found type={actual_type}, '
            f'nullable={column.get("nullable")!r}, '
            f'default={column.get("default")!r}, '
            f'computed={column.get("computed")!r}, '
            f'identity={column.get("identity")!r}.')


def _indexes(
        bind: sqlalchemy.engine.Connection) -> dict[str, Mapping[str, Any]]:
    return {
        str(index['name']): index
        for index in sqlalchemy.inspect(bind).get_indexes(_TABLE)
    }


def _normalized_predicate(predicate: object) -> str:
    return re.sub(r'[\s()"`]', '', str(predicate)).lower()


def _validate_postgres_index_state(bind: sqlalchemy.engine.Connection) -> None:
    state = bind.execute(
        sqlalchemy.text("""
            SELECT index_state.indisvalid,
                   index_state.indisready,
                   index_state.indislive,
                   index_state.indnkeyatts,
                   index_state.indnatts,
                   access_method.amname
            FROM pg_catalog.pg_index AS index_state
            JOIN pg_catalog.pg_class AS index_relation
              ON index_relation.oid = index_state.indexrelid
            JOIN pg_catalog.pg_class AS table_relation
              ON table_relation.oid = index_state.indrelid
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = table_relation.relnamespace
            JOIN pg_catalog.pg_am AS access_method
              ON access_method.oid = index_relation.relam
            WHERE namespace.nspname = current_schema()
              AND table_relation.relname = :table_name
              AND index_relation.relname = :index_name
        """), {
            'table_name': _TABLE,
            'index_name': _INDEX,
        }).one_or_none()
    if state is None or tuple(state) != (True, True, True, 1, 1, 'btree'):
        raise RuntimeError(
            f'Migration 028 found reserved index {_INDEX!r} in an invalid or '
            f'unsupported PostgreSQL state: {state!r}.')


def _validate_index(bind: sqlalchemy.engine.Connection,
                    index: Mapping[str, Any]) -> None:
    dialect = bind.dialect.name
    dialect_options = index.get('dialect_options') or {}
    predicate = dialect_options.get(f'{dialect}_where')
    if (list(index.get('column_names') or
             ()) != [_COLUMN] or not bool(index.get('unique')) or
            predicate is None or _normalized_predicate(predicate)
            != _normalized_predicate(_PREDICATE)):
        raise RuntimeError(
            f'Migration 028 found reserved index {_INDEX!r} with an '
            f'unexpected shape: {dict(index)!r}.')
    if dialect == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        _validate_postgres_index_state(bind)


def _ensure_index(bind: sqlalchemy.engine.Connection) -> None:
    existing = _indexes(bind).get(_INDEX)
    if existing is not None:
        _validate_index(bind, existing)
        return
    op.create_index(
        _INDEX,
        _TABLE,
        [_COLUMN],
        unique=True,
        postgresql_where=sqlalchemy.text(_PREDICATE),
        sqlite_where=sqlalchemy.text(_PREDICATE),
    )
    created = _indexes(bind).get(_INDEX)
    if created is None:
        raise RuntimeError(f'Migration 028 could not create index {_INDEX!r}.')
    _validate_index(bind, created)


def upgrade() -> None:
    """Add the nullable UUID commitment without backfilling historical rows."""
    bind = op.get_bind()
    _supported_dialect(bind)
    inspector = sqlalchemy.inspect(bind)
    if not inspector.has_table(_TABLE):
        raise RuntimeError('Migration 028 requires the clusters table.')
    if _COLUMN not in _columns(bind):
        op.add_column(
            _TABLE,
            sqlalchemy.Column(_COLUMN, sqlalchemy.Uuid(), nullable=True),
        )
    _validate_column(bind)
    _ensure_index(bind)


def downgrade() -> None:
    """Remove the commitment only while every cluster identity is null."""
    bind = op.get_bind()
    dialect = _supported_dialect(bind)
    if not sqlalchemy.inspect(bind).has_table(_TABLE):
        return
    if _COLUMN not in _columns(bind):
        return
    if dialect == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        # The following null proof and destructive DDL are one indivisible
        # maintenance operation.  Without a table lock, a writer can commit a
        # UUID after the proof and have that commitment erased by DROP COLUMN.
        bind.execute(
            sqlalchemy.text(f'LOCK TABLE {_TABLE} IN ACCESS EXCLUSIVE MODE'))
    committed = int(
        bind.execute(
            sqlalchemy.text(f'SELECT COUNT(*) FROM {_TABLE} '
                            f'WHERE {_COLUMN} IS NOT NULL')).scalar_one())
    if committed:
        raise RuntimeError(
            'Migration 028 downgrade requires every cluster-record UUID '
            'commitment to be null.')
    existing_index = _indexes(bind).get(_INDEX)
    if existing_index is not None:
        _validate_index(bind, existing_index)
        op.drop_index(_INDEX, table_name=_TABLE)
    db_utils.drop_column_from_table_alembic(_TABLE, _COLUMN)
