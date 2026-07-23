"""Index exact service-version replica ownership.

Revision ID: 026
Revises: 025
Create Date: 2026-07-23

"""
# pylint: disable=invalid-name
from collections.abc import Sequence
import pickle

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.serve import serve_state
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
            replica_info = pickle.loads(replica_info_bytes)
            row_values = serve_state._replica_row_values(  # pylint: disable=protected-access
                service_name, replica_id, replica_info)
            values.append({
                '_service_name': service_name,
                '_replica_id': replica_id,
                **{
                    key: value for key, value in row_values.items() if key not in ('service_name', 'replica_id', 'replica_info')
                },
            })
        bind.execute(update, values)

    remaining_count = bind.execute(
        sa.select(sa.func.count()  # pylint: disable=not-callable
                 ).select_from(replicas).where(incomplete)).scalar_one()
    if remaining_count:
        raise RuntimeError('Replica JSON convergence left '
                           f'{remaining_count} incomplete row(s).')


def _ensure_index(bind: sa.engine.Connection, name: str,
                  columns: list[str]) -> None:
    existing = {
        index['name'] for index in sa.inspect(bind).get_indexes('replicas')
    }
    if name in existing:
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
