"""Persist the user workspace and reconcile former preview layouts.

Revision ID: 022
Revises: 021
Create Date: 2026-07-18

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.serve import placement_history
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '022'
down_revision: str | Sequence[str] | None = '021'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_AUTOSCALER_HISTORY_TABLE = 'serve_autoscaler_history'
_ACCELERATOR_BREAKDOWN = 'accelerator_breakdown'
_ACCELERATOR_BREAKDOWN_OBSERVED_AT = 'accelerator_breakdown_observed_at'


def _ensure_exact_accelerator_history(bind: sa.engine.Connection) -> None:
    """Converge databases stamped by the former workspace revision 021."""
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    existing_columns = {
        column['name']
        for column in sa.inspect(bind).get_columns(_AUTOSCALER_HISTORY_TABLE)
    }
    if _ACCELERATOR_BREAKDOWN not in existing_columns:
        op.add_column(
            _AUTOSCALER_HISTORY_TABLE,
            sa.Column(_ACCELERATOR_BREAKDOWN,
                      postgresql.JSONB,
                      nullable=False,
                      server_default=sa.text("'{}'::jsonb")))
    if _ACCELERATOR_BREAKDOWN_OBSERVED_AT not in existing_columns:
        op.add_column(
            _AUTOSCALER_HISTORY_TABLE,
            sa.Column(_ACCELERATOR_BREAKDOWN_OBSERVED_AT,
                      sa.DateTime(timezone=True),
                      nullable=True))


def upgrade():
    """Persist workspace and converge former preview migration layouts."""
    with op.get_context().autocommit_block():
        bind = op.get_bind()
        # The feature preview and the exact-accelerator history branch both
        # used revision 021. A preview database stamped 021 therefore lacks
        # the columns owned by the canonical revision 021. Adopt them before
        # applying the workspace revision.
        _ensure_exact_accelerator_history(bind)
        # An older preview also collided at revision 018 and may lack the
        # placement table owned by canonical revision 018.
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
    # Placement and accelerator history belong to predecessor revisions. Only
    # the workspace column is owned by this convergence revision.
    op.drop_column('services', 'workspace')
