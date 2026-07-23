"""Index exact managed-job task identities.

Revision ID: 025
Revises: 024
Create Date: 2026-07-22

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '025'
down_revision: str | Sequence[str] | None = '024'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = 'ix_spot_job_task'
_TABLE_NAME = 'spot'
_COLUMNS = ('spot_job_id', 'task_id')


def _postgres_index_state(
        bind: sa.engine.Connection) -> sa.engine.RowMapping | None:
    """Returns the complete current-schema shape for the reserved index name."""
    return bind.execute(
        sa.text("""
            SELECT table_namespace.nspname AS table_schema,
                   table_class.relname AS table_name,
                   index_namespace.nspname AS index_schema,
                   index_row.indisvalid AS is_valid,
                   index_row.indisready AS is_ready,
                   index_row.indisunique AS is_unique,
                   index_row.indisprimary AS is_primary,
                   index_row.indisexclusion AS is_exclusion,
                   index_row.indpred IS NULL AS is_unfiltered,
                   index_row.indexprs IS NULL AS is_expression_free,
                   access_method.amname AS access_method,
                   index_row.indnkeyatts AS key_count,
                   index_row.indnatts AS attribute_count,
                   ARRAY(
                       SELECT pg_get_indexdef(
                           index_row.indexrelid, key_position, TRUE)
                       FROM generate_series(
                           1, index_row.indnkeyatts) AS key_position
                   ) AS key_columns,
                   NOT EXISTS (
                       SELECT 1
                       FROM unnest(index_row.indoption::smallint[])
                            AS options(value)
                       WHERE value <> 0
                   ) AS has_default_ordering
            FROM pg_index AS index_row
            JOIN pg_class AS index_class
              ON index_class.oid = index_row.indexrelid
            JOIN pg_namespace AS index_namespace
              ON index_namespace.oid = index_class.relnamespace
            JOIN pg_class AS table_class
              ON table_class.oid = index_row.indrelid
            JOIN pg_namespace AS table_namespace
              ON table_namespace.oid = table_class.relnamespace
            JOIN pg_am AS access_method
              ON access_method.oid = index_class.relam
            WHERE index_namespace.nspname = current_schema()
              AND index_class.relname = :name
              AND index_class.relkind = 'i'
            """), {
            'name': _INDEX_NAME
        }).mappings().one_or_none()


def _postgres_shape_matches(index: sa.engine.RowMapping) -> bool:
    return (str(index['table_schema']) == str(index['index_schema']) and
            str(index['table_name']) == _TABLE_NAME and
            bool(index['is_valid']) and bool(index['is_ready']) and
            not bool(index['is_unique']) and not bool(index['is_primary']) and
            not bool(index['is_exclusion']) and bool(index['is_unfiltered']) and
            bool(index['is_expression_free']) and
            str(index['access_method']) == 'btree' and
            int(index['key_count']) == len(_COLUMNS) and
            int(index['attribute_count']) == len(_COLUMNS) and
            tuple(index['key_columns'] or
                  ()) == _COLUMNS and bool(index['has_default_ordering']))


def _generic_shape_matches(index: dict) -> bool:
    dialect_options = index.get('dialect_options') or {}
    has_predicate = any(
        key.endswith('_where') and value is not None
        for key, value in dialect_options.items())
    return (tuple(index.get('column_names') or
                  ()) == _COLUMNS and not bool(index.get('unique')) and
            not has_predicate and not bool(index.get('column_sorting')))


def _ensure_index(bind: sa.engine.Connection) -> None:
    """Creates the exact index, repairing only interrupted PG residue."""
    if bind.dialect.name == 'postgresql':
        index = _postgres_index_state(bind)
        if index is not None:
            if (str(index['table_schema']) != str(index['index_schema']) or
                    str(index['table_name']) != _TABLE_NAME):
                raise RuntimeError(
                    f'Existing managed-job index {_INDEX_NAME!r} belongs to '
                    'an unexpected table.')
            if not bool(index['is_valid']) or not bool(index['is_ready']):
                preparer = bind.dialect.identifier_preparer
                qualified_name = (
                    f'{preparer.quote_schema(str(index["index_schema"]))}.'
                    f'{preparer.quote(_INDEX_NAME)}')
                bind.exec_driver_sql(
                    f'DROP INDEX CONCURRENTLY IF EXISTS {qualified_name}')
                index = None
            elif not _postgres_shape_matches(index):
                raise RuntimeError(
                    f'Existing managed-job index {_INDEX_NAME!r} has an '
                    'unexpected shape.')
        if index is not None:
            return
    else:
        indexes = {
            index['name']: index
            for index in sa.inspect(bind).get_indexes(_TABLE_NAME)
        }
        index = indexes.get(_INDEX_NAME)
        if index is not None:
            if not _generic_shape_matches(index):
                raise RuntimeError(
                    f'Existing managed-job index {_INDEX_NAME!r} has an '
                    'unexpected shape.')
            return

    op.create_index(_INDEX_NAME,
                    _TABLE_NAME,
                    list(_COLUMNS),
                    postgresql_concurrently=(bind.dialect.name == 'postgresql'))


def upgrade():
    """Converge the exact job-task lookup index."""
    with op.get_context().autocommit_block():
        _ensure_index(op.get_bind())


def downgrade():
    """No-op for forward-only managed-jobs schema migrations."""
    pass
