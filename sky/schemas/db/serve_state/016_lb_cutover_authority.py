"""Add durable external load balancer cutover authority.

Revision ID: 016
Revises: 015
Create Date: 2026-07-16

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

revision: str = '016'
down_revision: str | Sequence[str] | None = '015'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add the fenced warm-standby cutover state to each service row."""
    # Revision 001 creates a fresh database from current metadata. Therefore
    # these columns already exist when a new database subsequently walks the
    # revision chain, but must still be added to an upgraded database.
    existing_columns = {
        column['name']
        for column in sqlalchemy.inspect(op.get_bind()).get_columns('services')
    }
    columns = (
        sqlalchemy.Column('lb_ha_enabled',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('lb_active_slot', sqlalchemy.Text),
        sqlalchemy.Column('lb_cutover_generation',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('lb_pending_slot', sqlalchemy.Text),
        sqlalchemy.Column('lb_cutover_phase',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='STABLE'),
        sqlalchemy.Column('lb_drain_started_at', sqlalchemy.Float),
        sqlalchemy.Column('lb_demand_handoff_generation', sqlalchemy.Integer),
        sqlalchemy.Column('lb_demand_handoff_snapshot', sqlalchemy.Text),
        sqlalchemy.Column('lb_demand_handoff_complete_at', sqlalchemy.Float),
        sqlalchemy.Column('lb_last_demand_snapshot', sqlalchemy.Text),
    )
    for column in columns:
        if column.name not in existing_columns:
            op.add_column('services', column)


def downgrade():
    for column_name in (
            'lb_last_demand_snapshot',
            'lb_demand_handoff_complete_at',
            'lb_demand_handoff_snapshot',
            'lb_demand_handoff_generation',
            'lb_drain_started_at',
            'lb_cutover_phase',
            'lb_pending_slot',
            'lb_cutover_generation',
            'lb_active_slot',
            'lb_ha_enabled',
    ):
        op.drop_column('services', column_name)
