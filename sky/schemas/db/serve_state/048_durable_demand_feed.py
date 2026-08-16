"""Add the controller-independent SkyServe demand feed.

Revision ID: 048
Revises: 047
Create Date: 2026-08-16

Serve048 is additive and PostgreSQL-only.  Load balancers dark-write reports;
a later migration owns the explicit per-service promotion fields that make the
feed an autoscaling authority.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '048'
down_revision: str | Sequence[str] | None = '047'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REPORTS = 'serve_lb_demand_reports'
_GENERATIONS = 'serve_demand_feed_generations'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('The durable Serve demand feed is PostgreSQL-only.')


def upgrade() -> None:
    """Install dark-write report state."""
    _require_postgresql()
    op.create_table(
        _GENERATIONS,
        sa.Column('service_name',
                  sa.Text(),
                  sa.ForeignKey('services.name', ondelete='CASCADE'),
                  primary_key=True),
        sa.Column('service_hash', sa.Text(), nullable=False),
        sa.Column('generation', sa.BigInteger(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint('generation > 0',
                           name='serve048_demand_generation_positive_ck'),
    )
    op.create_table(
        _REPORTS,
        sa.Column('service_name',
                  sa.Text(),
                  sa.ForeignKey('services.name', ondelete='CASCADE'),
                  primary_key=True),
        sa.Column('service_hash', sa.Text(), primary_key=True),
        sa.Column('reporter_session_id', sa.Text(), primary_key=True),
        sa.Column('lb_session_id', sa.Text(), nullable=False),
        sa.Column('lb_slot', sa.Text()),
        sa.Column('protocol_version', sa.Integer(), nullable=False),
        sa.Column('sequence', sa.BigInteger(), nullable=False),
        sa.Column('routing_version', sa.Integer()),
        sa.Column('reporter_observed_at',
                  sa.DateTime(timezone=True),
                  nullable=False),
        sa.Column('received_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('valid_until', sa.DateTime(timezone=True), nullable=False),
        sa.Column('payload_sha256', sa.Text(), nullable=False),
        sa.Column('complete', sa.Boolean(), nullable=False),
        sa.Column('payload',
                  postgresql.JSONB(none_as_null=True),
                  nullable=False),
        sa.CheckConstraint('protocol_version = 1',
                           name='serve048_demand_protocol_ck'),
        sa.CheckConstraint('sequence > 0',
                           name='serve048_demand_sequence_positive_ck'),
        sa.CheckConstraint("payload_sha256 ~ '^[0-9a-f]{64}$'",
                           name='serve048_demand_digest_ck'),
        sa.CheckConstraint('valid_until > received_at',
                           name='serve048_demand_expiry_ck'),
    )
    op.create_index('ix_serve048_demand_reports_fresh', _REPORTS,
                    ['service_name', 'service_hash', 'valid_until'])


def downgrade() -> None:
    """Preserve demand evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve048 is forward-only. Stop every durable demand-feed writer '
        'before rolling application code back.')
