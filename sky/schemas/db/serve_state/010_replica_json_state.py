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

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.schemas.db import legacy_replica_pickle
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
        # Revision 001 historically created this column through the live
        # Serve declarative model. Keep the migration boundary self-contained
        # so a future model without the retired column can still replay 000 to
        # head on an empty database.
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
            if replica_info_bytes is None:
                raise RuntimeError(
                    'Replica JSON migration found a row without legacy '
                    'replica_info state.')
            row_values = (
                legacy_replica_pickle.frozen_replica_row_values_from_pickle(
                    replica_info_bytes, maximum_version=7))
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
            if row.replica_info is None:
                raise RuntimeError(
                    'Replica JSON migration parity check found a row without '
                    'legacy replica_info state.')
            expected = (
                legacy_replica_pickle.frozen_replica_row_values_from_pickle(
                    row.replica_info, maximum_version=7))
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
