"""Persist the user workspace and reconcile former revision 018 layouts.

Revision ID: 021
Revises: 020
Create Date: 2026-07-18

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.serve import placement_history
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '021'
down_revision: str | Sequence[str] | None = '020'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Persist workspace and converge both former revision 018 schemas."""
    with op.get_context().autocommit_block():
        # Before these branches were merged, the managed-image preview and the
        # placement-history branch both used revision 018. A preview database
        # stamped 018 may therefore lack the placement table that current 018
        # owns. Repair it idempotently before adding the preview's workspace
        # column so either history converges at revision 021.
        bind = op.get_bind()
        if bind.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            placement_history.serve_placement_events_table.create(
                bind, checkfirst=True)
        existing_columns = {
            column['name']
            for column in sa.inspect(bind).get_columns('services')
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
    # Placement and autoscaler history belong to revisions 018 and 019. Only
    # the workspace column is owned by this convergence revision.
    op.drop_column('services', 'workspace')
