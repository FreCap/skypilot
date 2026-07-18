"""Persist the user workspace and reconcile former revision 016 layouts.

Revision ID: 018
Revises: 017
Create Date: 2026-07-18

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '018'
down_revision: str | Sequence[str] | None = '017'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Persist workspace and converge both former revision 016 schemas."""
    with op.get_context().autocommit_block():
        # Before these branches were merged, the managed-image preview used
        # revision 016 for workspace persistence while improvements used it for
        # load-balancer cutover authority. A database stamped 016 may therefore
        # have either schema. Re-applying the nullable/defaulted columns makes
        # revision 018 converge both histories safely.
        existing_columns = {
            column['name']
            for column in sa.inspect(op.get_bind()).get_columns('services')
        }
        columns = (
            sa.Column('workspace', sa.Text()),
            sa.Column('lb_ha_enabled',
                      sa.Integer,
                      nullable=False,
                      server_default='0'),
            sa.Column('lb_active_slot', sa.Text),
            sa.Column('lb_cutover_generation',
                      sa.Integer,
                      nullable=False,
                      server_default='0'),
            sa.Column('lb_pending_slot', sa.Text),
            sa.Column('lb_cutover_phase',
                      sa.Text,
                      nullable=False,
                      server_default='STABLE'),
            sa.Column('lb_drain_started_at', sa.Float),
            sa.Column('lb_demand_handoff_generation', sa.Integer),
            sa.Column('lb_demand_handoff_snapshot', sa.Text),
            sa.Column('lb_demand_handoff_complete_at', sa.Float),
            sa.Column('lb_last_demand_snapshot', sa.Text),
        )
        for column in columns:
            if column.name not in existing_columns:
                op.add_column('services', column)


def downgrade():
    # Load-balancer columns belong to revision 016 and must survive a downgrade
    # from 018 to 017. Only the workspace column is owned by this revision.
    op.drop_column('services', 'workspace')
