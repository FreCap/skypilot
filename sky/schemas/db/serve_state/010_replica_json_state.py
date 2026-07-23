"""Migrate SkyServe replica control state from pickle to JSON.

Revision ID: 010
Revises: 009
Create Date: 2026-07-15

This transition must be deployed with the chart's default Recreate strategy.
An old writer in a RollingUpdate only knows how to update ``replica_info`` and
could make the migrated JSON state stale after this backfill.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence
import pickle
from typing import Any

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '010'
down_revision: str | Sequence[str] | None = '009'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_BACKFILL_BATCH_SIZE = 500
_JSON_ROW_KEYS = (
    'replica_state_version',
    'status',
    'sky_down_status',
    'version',
    'cluster_name',
    'created_at',
    'is_spot',
    'replica_state',
)


def _legacy_replica_row_values(replica_info: Any) -> dict[str, Any]:
    """Convert one legacy pickle using this migration's frozen projection."""
    replica_state = replica_info.to_storage_dict()
    sky_down_status = replica_info.status_property.sky_down_status
    return {
        'replica_state_version': 1,
        'status': replica_info.status.value,
        'sky_down_status':
            (sky_down_status.value if sky_down_status is not None else None),
        'version': replica_info.version,
        'cluster_name': replica_info.cluster_name,
        'created_at': getattr(replica_info, 'created_at', None),
        'is_spot': replica_info.is_spot,
        'replica_state': replica_state,
    }


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


def _add_columns() -> None:
    json_type = sa.JSON().with_variant(postgresql.JSONB(), 'postgresql')
    columns = (
        ('replica_state_version', sa.Integer()),
        ('status', sa.Text()),
        ('sky_down_status', sa.Text()),
        ('version', sa.Integer()),
        ('cluster_name', sa.Text()),
        ('created_at', sa.Float()),
        ('is_spot', sa.Boolean()),
        ('replica_state', json_type),
    )
    for name, column_type in columns:
        db_utils.add_column_to_table_alembic('replicas', name, column_type)


def _backfill_and_verify() -> None:
    bind = op.get_bind()
    replicas = _replicas_table()
    rows = bind.execute(
        sa.select(replicas.c.service_name, replicas.c.replica_id,
                  replicas.c.replica_info).order_by(replicas.c.service_name,
                                                    replicas.c.replica_id))
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
            replica_info = pickle.loads(replica_info_bytes)
            row_values = _legacy_replica_row_values(replica_info)
            values.append({
                '_service_name': service_name,
                '_replica_id': replica_id,
                **{
                    key: row_values[key] for key in _JSON_ROW_KEYS
                },
            })
        bind.execute(update, values)

    incomplete_count = bind.execute(
        sa.select(sa.func.count()  # pylint: disable=not-callable
                 ).select_from(replicas).where(
                     sa.or_(replicas.c.replica_state_version.is_(None),
                            replicas.c.status.is_(None),
                            replicas.c.version.is_(None),
                            replicas.c.cluster_name.is_(None),
                            replicas.c.is_spot.is_(None),
                            replicas.c.replica_state.is_(None)))).scalar_one()
    if incomplete_count:
        raise RuntimeError('Replica JSON migration left '
                           f'{incomplete_count} incomplete row(s).')

    verification_rows = bind.execute(
        sa.select(replicas.c.service_name, replicas.c.replica_id,
                  replicas.c.replica_info, replicas.c.replica_state_version,
                  replicas.c.status, replicas.c.sky_down_status,
                  replicas.c.version, replicas.c.cluster_name,
                  replicas.c.created_at, replicas.c.is_spot,
                  replicas.c.replica_state).order_by(replicas.c.service_name,
                                                     replicas.c.replica_id))
    while True:
        batch = verification_rows.fetchmany(_BACKFILL_BATCH_SIZE)
        if not batch:
            break
        for row in batch:
            legacy = pickle.loads(row.replica_info)
            expected = _legacy_replica_row_values(legacy)
            actual = {
                'replica_state_version': row.replica_state_version,
                'status': row.status,
                'sky_down_status': row.sky_down_status,
                'version': row.version,
                'cluster_name': row.cluster_name,
                'created_at': row.created_at,
                'is_spot': row.is_spot,
                'replica_state': row.replica_state,
            }
            expected_without_legacy = {
                key: expected[key] for key in _JSON_ROW_KEYS
            }
            if actual != expected_without_legacy:
                raise RuntimeError(
                    'Replica JSON migration parity check failed for '
                    f'{row.service_name!r} replica {row.replica_id}.')


def upgrade():
    """Add, backfill, and verify the authoritative replica JSON state."""
    with op.get_context().autocommit_block():
        _add_columns()
        _backfill_and_verify()
        op.create_index('replicas_service_status_idx',
                        'replicas', ['service_name', 'status'],
                        if_not_exists=True)


def downgrade():
    pass
