"""Index the ordered managed-job scheduler lookup.

Revision ID: 027
Revises: 026
Create Date: 2026-08-02

"""
# pylint: disable=invalid-name
from collections.abc import Sequence
from typing import Any

from alembic import op
import sqlalchemy as sa

revision: str = '027'
down_revision: str | Sequence[str] | None = '026'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = 'ix_job_info_schedule_priority'
_TABLE_NAME = 'job_info'
_COLUMNS = ('schedule_state', 'priority', 'spot_job_id')
_DESCENDING = (False, True, False)
# PostgreSQL pg_index.indoption bits are DESC = 1 and NULLS FIRST = 2.
# The query's default ordering is ASC NULLS LAST, DESC NULLS FIRST, then ASC
# NULLS LAST, so its physical key options must be exactly 0, 3, 0.
_POSTGRES_KEY_OPTIONS = (0, 3, 0)


def _postgres_index_state(
        bind: sa.engine.Connection) -> sa.engine.RowMapping | None:
    """Returns the complete current-schema shape for the reserved name."""
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
                       SELECT attribute.attname
                       FROM unnest(index_row.indkey::smallint[])
                            WITH ORDINALITY AS key(attnum, key_position)
                       JOIN pg_attribute AS attribute
                         ON attribute.attrelid = index_row.indrelid
                        AND attribute.attnum = key.attnum
                       WHERE key.key_position <= index_row.indnkeyatts
                       ORDER BY key.key_position
                   ) AS key_columns,
                   index_row.indoption::smallint[] AS key_options
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
            tuple(index['key_columns'] or ()) == _COLUMNS and
            tuple(int(option) for option in (index['key_options'] or ()))
            == _POSTGRES_KEY_OPTIONS)


def _sqlite_index_state(bind: sa.engine.Connection) -> dict[str, Any] | None:
    """Returns SQLite's table, uniqueness, predicate, and ordered keys."""
    index = bind.execute(
        sa.text("""
            SELECT tbl_name AS table_name
            FROM sqlite_master
            WHERE type = 'index' AND name = :name
            """), {
            'name': _INDEX_NAME
        }).mappings().one_or_none()
    if index is None:
        return None

    preparer = bind.dialect.identifier_preparer
    table_indexes = bind.exec_driver_sql(
        f'PRAGMA index_list({preparer.quote(_TABLE_NAME)})').mappings().all()
    summary = next(
        (row for row in table_indexes if str(row['name']) == _INDEX_NAME), None)
    if summary is None:
        return {'table_name': str(index['table_name'])}

    key_rows = [
        row for row in bind.exec_driver_sql(
            f'PRAGMA index_xinfo({preparer.quote(_INDEX_NAME)})').mappings()
        if bool(row['key'])
    ]
    return {
        'table_name': str(index['table_name']),
        'is_unique': bool(summary['unique']),
        'is_partial': bool(summary['partial']),
        'origin': str(summary['origin']),
        'key_columns': tuple(str(row['name']) for row in key_rows),
        'descending': tuple(bool(row['desc']) for row in key_rows),
    }


def _sqlite_shape_matches(index: dict[str, Any]) -> bool:
    return (str(index.get('table_name')) == _TABLE_NAME and
            not bool(index.get('is_unique')) and
            not bool(index.get('is_partial')) and
            str(index.get('origin')) == 'c' and
            tuple(index.get('key_columns') or
                  ()) == _COLUMNS and tuple(index.get('descending') or
                                            ()) == _DESCENDING)


def _create_index(bind: sa.engine.Connection) -> None:
    op.create_index(_INDEX_NAME,
                    _TABLE_NAME, [
                        sa.column(_COLUMNS[0]),
                        sa.column(_COLUMNS[1]).desc(),
                        sa.column(_COLUMNS[2]).asc(),
                    ],
                    postgresql_concurrently=(bind.dialect.name == 'postgresql'))


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
    elif bind.dialect.name == 'sqlite':
        sqlite_index = _sqlite_index_state(bind)
        if sqlite_index is not None:
            if str(sqlite_index['table_name']) != _TABLE_NAME:
                raise RuntimeError(
                    f'Existing managed-job index {_INDEX_NAME!r} belongs to '
                    'an unexpected table.')
            if not _sqlite_shape_matches(sqlite_index):
                raise RuntimeError(
                    f'Existing managed-job index {_INDEX_NAME!r} has an '
                    'unexpected shape.')
            return
    else:
        raise RuntimeError('Managed-job scheduler indexes require SQLite or '
                           'PostgreSQL.')

    _create_index(bind)


def upgrade():
    """Converge the exact ordered waiting-job lookup index."""
    with op.get_context().autocommit_block():
        _ensure_index(op.get_bind())


def downgrade():
    """No-op for forward-only managed-jobs schema migrations."""
    pass
