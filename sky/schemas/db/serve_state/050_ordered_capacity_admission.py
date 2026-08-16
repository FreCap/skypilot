"""Add durable demand authority and ordered capacity plans.

Revision ID: 050
Revises: 049
Create Date: 2026-08-16

Serve050 is additive, dark by default, and PostgreSQL-only.  Existing services
continue to use controller-sync demand until an explicit per-service
promotion. Existing paid claims remain valid transition evidence but cannot be
used as API012 planner authority.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '050'
down_revision: str | Sequence[str] | None = '049'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SERVICES = 'services'
_PLANS = 'serve_capacity_plans'
_HEADS = 'serve_capacity_plan_heads'
_CLAIMS = 'paid_capacity_claims'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('Ordered capacity admission is PostgreSQL-only.')


def upgrade() -> None:
    """Install dark demand ownership and content-addressed plan state."""
    _require_postgresql()
    op.drop_constraint('serve048_demand_protocol_ck',
                       'serve_lb_demand_reports',
                       type_='check')
    op.create_check_constraint('serve048_demand_protocol_ck',
                               'serve_lb_demand_reports',
                               'protocol_version IN (1, 2)')
    op.add_column(
        _SERVICES,
        sa.Column('demand_source_mode',
                  sa.Text(),
                  nullable=False,
                  server_default='LEGACY_CONTROLLER'))
    op.add_column(
        _SERVICES,
        sa.Column('demand_source_epoch',
                  sa.BigInteger(),
                  nullable=False,
                  server_default='0'))
    op.add_column(
        _SERVICES,
        sa.Column('demand_authority_capable',
                  sa.Boolean(),
                  nullable=False,
                  server_default=sa.false()))
    op.add_column(
        _SERVICES,
        sa.Column('demand_authority_controller_incarnation', sa.Uuid()))
    op.add_column(_SERVICES,
                  sa.Column('demand_authority_protocol_version', sa.Integer()))
    op.create_check_constraint(
        'serve050_demand_source_mode_ck', _SERVICES,
        "demand_source_mode IN ('LEGACY_CONTROLLER', 'DURABLE_FEED')")
    op.create_check_constraint('serve050_demand_source_epoch_ck', _SERVICES,
                               'demand_source_epoch >= 0')
    op.create_check_constraint(
        'serve050_demand_capability_shape_ck', _SERVICES,
        '((NOT demand_authority_capable AND '
        'demand_authority_controller_incarnation IS NULL AND '
        'demand_authority_protocol_version IS NULL) OR '
        '(demand_authority_capable AND '
        'demand_authority_controller_incarnation IS NOT NULL AND '
        'demand_authority_protocol_version = 1))')
    op.create_check_constraint(
        'serve050_durable_demand_capability_ck', _SERVICES,
        "demand_source_mode <> 'DURABLE_FEED' OR "
        '(demand_source_epoch > 0 AND demand_authority_capable)')

    op.create_table(
        _PLANS,
        sa.Column('service_name',
                  sa.Text(),
                  sa.ForeignKey('services.name', ondelete='CASCADE'),
                  primary_key=True),
        sa.Column('generation', sa.BigInteger(), primary_key=True),
        sa.Column('service_hash', sa.Text(), nullable=False),
        sa.Column('service_lifecycle_epoch', sa.BigInteger(), nullable=False),
        sa.Column('service_version', sa.Integer(), nullable=False),
        sa.Column('demand_source_epoch', sa.BigInteger(), nullable=False),
        sa.Column('demand_feed_generation', sa.BigInteger(), nullable=False),
        sa.Column('route_generation', sa.BigInteger(), nullable=False),
        sa.Column('route_sha256', sa.Text(), nullable=False),
        sa.Column('route_source_epoch', sa.BigInteger(), nullable=False),
        sa.Column('protocol_version', sa.Integer(), nullable=False),
        sa.Column('content_sha256', sa.Text(), nullable=False),
        sa.Column('payload',
                  postgresql.JSONB(none_as_null=True),
                  nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint('generation > 0',
                           name='serve050_plan_generation_positive_ck'),
        sa.CheckConstraint(
            'service_lifecycle_epoch > 0 AND '
            'service_version > 0 AND demand_source_epoch > 0',
            name='serve050_plan_service_fence_positive_ck'),
        sa.CheckConstraint(
            'demand_feed_generation > 0 AND '
            'route_generation > 0 AND route_source_epoch > 0',
            name='serve050_plan_source_fence_positive_ck'),
        sa.CheckConstraint('protocol_version = 1',
                           name='serve050_plan_protocol_ck'),
        sa.CheckConstraint("content_sha256 ~ '^[0-9a-f]{64}$'",
                           name='serve050_plan_digest_ck'),
    )
    op.create_table(
        _HEADS,
        sa.Column('service_name', sa.Text(), primary_key=True),
        sa.Column('generation', sa.BigInteger(), nullable=False),
        sa.Column('demand_feed_generation', sa.BigInteger(), nullable=False),
        sa.Column('receipt_watermark_sha256', sa.Text(), nullable=False),
        sa.Column('refreshed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('valid_until', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ['service_name', 'generation'],
            [f'{_PLANS}.service_name', f'{_PLANS}.generation'],
            name='serve050_plan_head_plan_fk',
            ondelete='CASCADE'),
        sa.CheckConstraint('generation > 0',
                           name='serve050_plan_head_positive_ck'),
        sa.CheckConstraint('demand_feed_generation > 0',
                           name='serve050_plan_head_demand_positive_ck'),
        sa.CheckConstraint("receipt_watermark_sha256 ~ '^[0-9a-f]{64}$'",
                           name='serve050_plan_head_watermark_ck'),
        sa.CheckConstraint('valid_until > refreshed_at',
                           name='serve050_plan_head_expiry_ck'),
    )
    op.create_index('ix_serve050_capacity_plan_heads_fresh', _HEADS,
                    ['valid_until'])

    for column in (
            sa.Column('capacity_plan_generation', sa.BigInteger()),
            sa.Column('capacity_plan_sha256', sa.Text()),
            sa.Column('demand_feed_generation', sa.BigInteger()),
            sa.Column('demand_source_epoch', sa.BigInteger()),
            sa.Column('capacity_plan_accelerator', sa.Text()),
            sa.Column('capacity_plan_units', sa.Integer()),
    ):
        op.add_column(_CLAIMS, column)
    op.create_foreign_key('serve050_paid_claim_capacity_plan_fk',
                          _CLAIMS,
                          _PLANS, ['service_name', 'capacity_plan_generation'],
                          ['service_name', 'generation'],
                          ondelete='CASCADE')
    op.create_check_constraint(
        'serve050_paid_claim_plan_complete_ck', _CLAIMS,
        'num_nonnulls(capacity_plan_generation, capacity_plan_sha256, '
        'demand_feed_generation, demand_source_epoch, '
        'capacity_plan_accelerator, capacity_plan_units) IN (0, 6)')
    op.create_check_constraint(
        'serve050_paid_claim_plan_values_ck', _CLAIMS,
        '(capacity_plan_generation IS NULL OR '
        '(capacity_plan_generation > 0 AND demand_feed_generation > 0 AND '
        'demand_source_epoch > 0 AND '
        "capacity_plan_sha256 ~ '^[0-9a-f]{64}$' AND "
        'length(capacity_plan_accelerator) > 0 AND '
        'capacity_plan_units > 0))')


def downgrade() -> None:
    """Preserve planner evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve050 is forward-only. Demote every DURABLE_FEED service and '
        'settle every planner-bound paid claim before application rollback.')
