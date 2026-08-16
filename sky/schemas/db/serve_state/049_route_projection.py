"""Add provider-free SkyServe route snapshots.

Revision ID: 049
Revises: 048
Create Date: 2026-08-16

Serve049 is additive, dark by default, and PostgreSQL-only.  Existing services
continue to proxy LB syncs to their controller until an explicit later
promotion changes their route source mode.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '049'
down_revision: str | Sequence[str] | None = '048'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SERVICES = 'services'
_SNAPSHOTS = 'serve_route_snapshots'
_HEADS = 'serve_route_heads'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('The Serve route projection is PostgreSQL-only.')


def upgrade() -> None:
    """Install dark route publication and explicit response ownership."""
    _require_postgresql()
    op.add_column(
        _SERVICES,
        sa.Column('route_source_mode',
                  sa.Text(),
                  nullable=False,
                  server_default='LEGACY_PROXY'))
    op.add_column(
        _SERVICES,
        sa.Column('route_source_epoch',
                  sa.BigInteger(),
                  nullable=False,
                  server_default='0'))
    op.add_column(
        _SERVICES,
        sa.Column('route_projection_capable',
                  sa.Boolean(),
                  nullable=False,
                  server_default=sa.false()))
    op.add_column(
        _SERVICES,
        sa.Column('route_projection_controller_incarnation',
                  sa.Uuid(as_uuid=True)))
    op.add_column(_SERVICES,
                  sa.Column('route_projection_protocol_version', sa.Integer()))
    op.create_check_constraint(
        'serve049_route_source_mode_ck', _SERVICES,
        "route_source_mode IN ('LEGACY_PROXY', 'DURABLE_PROJECTED')")
    op.create_check_constraint('serve049_route_source_epoch_ck', _SERVICES,
                               'route_source_epoch >= 0')
    op.create_check_constraint(
        'serve049_route_capability_shape_ck', _SERVICES,
        '((NOT route_projection_capable AND '
        'route_projection_controller_incarnation IS NULL AND '
        'route_projection_protocol_version IS NULL) OR '
        '(route_projection_capable AND '
        'route_projection_controller_incarnation IS NOT NULL AND '
        'route_projection_protocol_version = 1))')
    op.create_check_constraint(
        'serve049_route_projected_capability_ck', _SERVICES,
        "route_source_mode <> 'DURABLE_PROJECTED' OR "
        '(route_source_epoch > 0 AND route_projection_capable)')

    op.create_table(
        _SNAPSHOTS,
        sa.Column('service_name',
                  sa.Text(),
                  sa.ForeignKey('services.name', ondelete='CASCADE'),
                  primary_key=True),
        sa.Column('generation', sa.BigInteger(), primary_key=True),
        sa.Column('service_hash', sa.Text(), nullable=False),
        sa.Column('service_lifecycle_epoch', sa.BigInteger(), nullable=False),
        sa.Column('controller_incarnation',
                  sa.Uuid(as_uuid=True),
                  nullable=False),
        sa.Column('controller_owner_epoch', sa.BigInteger(), nullable=False),
        sa.Column('controller_pid', sa.Integer(), nullable=False),
        sa.Column('controller_ip', sa.Text(), nullable=False),
        sa.Column('service_version', sa.Integer(), nullable=False),
        sa.Column('protocol_version', sa.Integer(), nullable=False),
        sa.Column('content_sha256', sa.Text(), nullable=False),
        sa.Column('response_payload',
                  postgresql.JSONB(none_as_null=True),
                  nullable=False),
        sa.Column('identity_payload',
                  postgresql.JSONB(none_as_null=True),
                  nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint('generation > 0',
                           name='serve049_route_generation_positive_ck'),
        sa.CheckConstraint('service_lifecycle_epoch > 0',
                           name='serve049_route_lifecycle_positive_ck'),
        sa.CheckConstraint('controller_owner_epoch > 0',
                           name='serve049_route_owner_positive_ck'),
        sa.CheckConstraint('controller_pid > 0 AND length(controller_ip) > 0',
                           name='serve049_route_owner_address_ck'),
        sa.CheckConstraint('service_version > 0',
                           name='serve049_route_version_positive_ck'),
        sa.CheckConstraint('protocol_version = 1',
                           name='serve049_route_protocol_ck'),
        sa.CheckConstraint("content_sha256 ~ '^[0-9a-f]{64}$'",
                           name='serve049_route_digest_ck'),
    )
    op.create_table(
        _HEADS,
        sa.Column('service_name', sa.Text(), primary_key=True),
        sa.Column('generation', sa.BigInteger(), nullable=False),
        sa.Column('refreshed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('valid_until', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ['service_name', 'generation'],
            [f'{_SNAPSHOTS}.service_name', f'{_SNAPSHOTS}.generation'],
            name='serve049_route_head_snapshot_fk',
            ondelete='CASCADE'),
        sa.CheckConstraint('generation > 0',
                           name='serve049_route_head_positive_ck'),
        sa.CheckConstraint('valid_until > refreshed_at',
                           name='serve049_route_head_expiry_ck'),
    )
    op.create_index('ix_serve049_route_heads_fresh', _HEADS, ['valid_until'])


def downgrade() -> None:
    """Retain route evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve049 is forward-only. Roll every service back to LEGACY_PROXY '
        'and stop route publishers before application rollback.')
