"""Persist the user workspace and reconcile colliding preview layouts.

Revision ID: 025
Revises: 024
Create Date: 2026-07-23

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.serve import placement_history
from sky.serve import serve_history
from sky.utils.db import db_utils

revision: str = '025'
down_revision: str | Sequence[str] | None = '024'
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


def _ensure_colliding_upstream_revisions(bind: sa.engine.Connection) -> None:
    """Converge databases stamped by either revision-022/023/024 lineage."""
    # The managed-image preview used 022 for workspace, 023 for the replica
    # lookup index, and 024 for response history. Upstream independently used
    # those revisions for response history, prediction history, and version
    # quarantine. A database stamped by either lineage skips the other
    # lineage's same-numbered migration. Revision 025 is the first migration
    # every ambiguous layout executes, so it materializes all upstream-owned
    # state idempotently before adding the managed-image workspace state.
    if bind.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        serve_history.serve_response_time_history_table.create(bind,
                                                               checkfirst=True)
        serve_history.serve_prediction_time_history_table.create(
            bind, checkfirst=True)

    existing_version_columns = {
        column['name']
        for column in sa.inspect(bind).get_columns('version_specs')
    }
    quarantine_columns = (
        sa.Column('quarantined_at', sa.Float()),
        sa.Column('quarantine_reason', sa.Text()),
    )
    for column in quarantine_columns:
        if column.name not in existing_version_columns:
            op.add_column('version_specs', column)


def upgrade():
    """Persist workspace and converge every known colliding migration."""
    with op.get_context().autocommit_block():
        bind = op.get_bind()
        _ensure_exact_accelerator_history(bind)
        _ensure_colliding_upstream_revisions(bind)
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
    # Revision 025 converges ambiguous preview state. Removing a column cannot
    # determine which lineage originally owned it, so downgrade is deliberately
    # non-destructive.
    pass
