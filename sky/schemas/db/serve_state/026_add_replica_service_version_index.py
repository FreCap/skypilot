"""Index exact service-version replica ownership.

Revision ID: 026
Revises: 025
Create Date: 2026-07-23

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.schemas.db import legacy_replica_pickle
from sky.utils.db import db_utils

revision: str = '026'
down_revision: str | Sequence[str] | None = '025'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = 'replicas_service_version_idx'
_STATUS_INDEX_NAME = 'replicas_service_status_idx'
_BACKFILL_BATCH_SIZE = 500


def _replicas_table() -> sa.Table:
    metadata = sa.MetaData()
    return sa.Table(
        'replicas',
        metadata,
        sa.Column('service_name', sa.Text()),
        sa.Column('replica_id', sa.Integer()),
        sa.Column('replica_info', sa.LargeBinary()),
        sa.Column('replica_state_version', sa.Integer()),
        sa.Column('status', sa.Text()),
        sa.Column('sky_down_status', sa.Text()),
        sa.Column('version', sa.Integer()),
        sa.Column('cluster_name', sa.Text()),
        sa.Column('created_at', sa.Float()),
        sa.Column('is_spot', sa.Boolean()),
        sa.Column('replica_state',
                  sa.JSON().with_variant(postgresql.JSONB(), 'postgresql')),
    )


def _ensure_replica_json_state(bind: sa.engine.Connection) -> None:
    """Converge predecessor-stamped previews that skipped revision 010."""
    json_type = sa.JSON().with_variant(postgresql.JSONB(), 'postgresql')
    columns = (
        ('replica_info', sa.LargeBinary()),
        ('replica_state_version', sa.Integer()),
        ('status', sa.Text()),
        ('sky_down_status', sa.Text()),
        ('version', sa.Integer()),
        ('cluster_name', sa.Text()),
        ('created_at', sa.Float()),
        ('is_spot', sa.Boolean()),
        ('replica_state', json_type),
    )
    existing_columns = {
        column['name'] for column in sa.inspect(bind).get_columns('replicas')
    }
    had_replica_info = 'replica_info' in existing_columns
    for name, column_type in columns:
        if name not in existing_columns:
            db_utils.add_column_to_table_alembic('replicas', name, column_type)

    replicas = _replicas_table()
    incomplete = sa.or_(replicas.c.replica_state_version.is_(None),
                        replicas.c.status.is_(None),
                        replicas.c.version.is_(None),
                        replicas.c.cluster_name.is_(None),
                        replicas.c.is_spot.is_(None),
                        replicas.c.replica_state.is_(None))
    incomplete_count = bind.execute(
        sa.select(sa.func.count()  # pylint: disable=not-callable
                 ).select_from(replicas).where(incomplete)).scalar_one()
    if not incomplete_count:
        return
    if not had_replica_info:
        raise RuntimeError(
            'Replica JSON convergence cannot reconstruct nonempty rows '
            'without legacy replica_info state.')
    rows = bind.execute(
        sa.select(replicas.c.service_name, replicas.c.replica_id,
                  replicas.c.replica_info).where(incomplete).order_by(
                      replicas.c.service_name, replicas.c.replica_id))
    update = sa.update(replicas).where(
        replicas.c.service_name == sa.bindparam('_service_name'),
        replicas.c.replica_id == sa.bindparam('_replica_id')).values(
            replica_state_version=sa.bindparam('replica_state_version'),
            status=sa.bindparam('status'),
            sky_down_status=sa.bindparam('sky_down_status'),
            version=sa.bindparam('version'),
            cluster_name=sa.bindparam('cluster_name'),
            created_at=sa.bindparam('created_at'),
            is_spot=sa.bindparam('is_spot'),
            replica_state=sa.bindparam('replica_state'))
    while True:
        batch = rows.fetchmany(_BACKFILL_BATCH_SIZE)
        if not batch:
            break
        values = []
        for service_name, replica_id, replica_info_bytes in batch:
            if replica_info_bytes is None:
                raise RuntimeError(
                    'Replica JSON convergence found an incomplete row '
                    'without legacy replica_info state.')
            row_values = (
                legacy_replica_pickle.frozen_replica_row_values_from_pickle(
                    replica_info_bytes, maximum_version=11))
            values.append({
                '_service_name': service_name,
                '_replica_id': replica_id,
                **row_values,
            })
        bind.execute(update, values)

    remaining_count = bind.execute(
        sa.select(sa.func.count()  # pylint: disable=not-callable
                 ).select_from(replicas).where(incomplete)).scalar_one()
    if remaining_count:
        raise RuntimeError('Replica JSON convergence left '
                           f'{remaining_count} incomplete row(s).')


def _postgres_index_state(bind: sa.engine.Connection,
                          name: str) -> sa.engine.RowMapping | None:
    """Returns the complete current-schema shape for one reserved index."""
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
            'name': name
        }).mappings().one_or_none()


def _postgres_shape_matches(index: sa.engine.RowMapping,
                            columns: list[str]) -> bool:
    return (str(index['table_schema']) == str(index['index_schema']) and
            str(index['table_name']) == 'replicas' and
            bool(index['is_valid']) and bool(index['is_ready']) and
            not bool(index['is_unique']) and not bool(index['is_primary']) and
            not bool(index['is_exclusion']) and bool(index['is_unfiltered']) and
            bool(index['is_expression_free']) and
            str(index['access_method']) == 'btree' and
            int(index['key_count']) == len(columns) and
            int(index['attribute_count']) == len(columns) and
            list(index['key_columns'] or
                 ()) == columns and bool(index['has_default_ordering']))


def _generic_shape_matches(index: dict, columns: list[str]) -> bool:
    dialect_options = index.get('dialect_options') or {}
    has_predicate = any(
        key.endswith('_where') and value is not None
        for key, value in dialect_options.items())
    return (list(index.get('column_names') or
                 ()) == columns and not bool(index.get('unique')) and
            not has_predicate and not bool(index.get('column_sorting')))


def _ensure_index(bind: sa.engine.Connection, name: str,
                  columns: list[str]) -> None:
    """Creates an exact replica index, repairing only interrupted residue."""
    if bind.dialect.name == 'postgresql':
        index = _postgres_index_state(bind, name)
        if index is not None:
            if (str(index['table_schema']) != str(index['index_schema']) or
                    str(index['table_name']) != 'replicas'):
                raise RuntimeError(
                    f'Existing replica index {name!r} belongs to an '
                    'unexpected table.')
            if not bool(index['is_valid']) or not bool(index['is_ready']):
                preparer = bind.dialect.identifier_preparer
                qualified_name = (
                    f'{preparer.quote_schema(str(index["index_schema"]))}.'
                    f'{preparer.quote(name)}')
                bind.exec_driver_sql(
                    f'DROP INDEX CONCURRENTLY IF EXISTS {qualified_name}')
                index = None
            elif not _postgres_shape_matches(index, columns):
                raise RuntimeError(
                    f'Existing replica index {name!r} has an unexpected '
                    'shape.')
        if index is not None:
            return
    else:
        indexes = {
            index['name']: index
            for index in sa.inspect(bind).get_indexes('replicas')
        }
        index = indexes.get(name)
        if index is not None:
            if not _generic_shape_matches(index, columns):
                raise RuntimeError(
                    f'Existing replica index {name!r} has an unexpected '
                    'shape.')
            return

    op.create_index(name,
                    'replicas',
                    columns,
                    postgresql_concurrently=(bind.dialect.name == 'postgresql'))


def upgrade():
    """Converge replica state and add exact lookup indexes if absent."""
    with op.get_context().autocommit_block():
        bind = op.get_bind()
        _ensure_replica_json_state(bind)
        _ensure_index(bind, _STATUS_INDEX_NAME, ['service_name', 'status'])
        _ensure_index(bind, _INDEX_NAME, ['service_name', 'version'])


def downgrade():
    """No-op for forward-only Serve schema migrations."""
    pass
