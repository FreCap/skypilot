"""Add global SkyServe paid-capacity admission state.

Revision ID: 027
Revises: 026
Create Date: 2026-07-23

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '027'
down_revision: str | Sequence[str] | None = '026'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CLAIM_POOL_INDEX = 'paid_capacity_claims_pool_idx'
_WAITER_POOL_INDEX = 'paid_capacity_waiters_pool_idx'


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    replica_columns = {
        column['name'] for column in inspector.get_columns('replicas')
    }
    if 'paid_capacity_pool_key' not in replica_columns:
        op.add_column(
            'replicas',
            sa.Column('paid_capacity_pool_key', sa.Text(), nullable=True))

    table_names = set(inspector.get_table_names())
    if 'paid_capacity_pools' not in table_names:
        op.create_table(
            'paid_capacity_pools',
            sa.Column('pool_key', sa.Text(), primary_key=True),
            sa.Column('current_limit', sa.Integer(), nullable=False),
            sa.Column('successes_since_resize',
                      sa.Integer(),
                      nullable=False,
                      server_default='0'),
            sa.Column('last_success_at', sa.Float(), nullable=True),
            sa.Column('last_failure_at', sa.Float(), nullable=True),
            sa.Column('updated_at', sa.Float(), nullable=False),
        )
    if 'paid_capacity_claims' not in table_names:
        op.create_table(
            'paid_capacity_claims',
            sa.Column('service_name', sa.Text(), primary_key=True),
            sa.Column('service_hash', sa.Text(), primary_key=True),
            sa.Column('replica_id', sa.Integer(), primary_key=True),
            sa.Column('pool_key',
                      sa.Text(),
                      sa.ForeignKey('paid_capacity_pools.pool_key',
                                    ondelete='CASCADE'),
                      nullable=False),
            sa.Column('priority', sa.Integer(), nullable=False),
            sa.Column('claimed_at', sa.Float(), nullable=False),
        )
    if 'paid_capacity_waiters' not in table_names:
        op.create_table(
            'paid_capacity_waiters',
            sa.Column('pool_key',
                      sa.Text(),
                      sa.ForeignKey('paid_capacity_pools.pool_key',
                                    ondelete='CASCADE'),
                      primary_key=True),
            sa.Column('service_name', sa.Text(), primary_key=True),
            sa.Column('service_hash', sa.Text(), primary_key=True),
            sa.Column('priority', sa.Integer(), nullable=False),
            sa.Column('first_wait_at', sa.Float(), nullable=False),
            sa.Column('heartbeat_at', sa.Float(), nullable=False),
        )
    op.create_index(_CLAIM_POOL_INDEX,
                    'paid_capacity_claims', ['pool_key'],
                    if_not_exists=True)
    op.create_index(_WAITER_POOL_INDEX,
                    'paid_capacity_waiters', ['pool_key'],
                    if_not_exists=True)


def downgrade() -> None:
    op.drop_index(_WAITER_POOL_INDEX, table_name='paid_capacity_waiters')
    op.drop_table('paid_capacity_waiters')
    op.drop_index(_CLAIM_POOL_INDEX, table_name='paid_capacity_claims')
    op.drop_table('paid_capacity_claims')
    op.drop_table('paid_capacity_pools')
    op.drop_column('replicas', 'paid_capacity_pool_key')
