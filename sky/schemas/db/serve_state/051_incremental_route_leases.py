"""Add incremental provider-free SkyServe route leases.

Revision ID: 051
Revises: 050
Create Date: 2026-08-17

Serve051 is additive, dark by default, and PostgreSQL-only.  Protocol 1 route
publishers remain available for unconverted services while protocol 2 writes
per-replica material and readiness leases before composing the unchanged
protocol 1 load-balancer snapshot document.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '051'
down_revision: str | Sequence[str] | None = '050'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SERVICES = 'services'
_LEASES = 'serve_route_replica_leases'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('Incremental route leases are PostgreSQL-only.')


def upgrade() -> None:
    """Install dark protocol-2 per-replica route material."""
    _require_postgresql()
    op.drop_constraint('serve049_route_capability_shape_ck',
                       _SERVICES,
                       type_='check')
    op.create_check_constraint(
        'serve049_route_capability_shape_ck', _SERVICES,
        '((NOT route_projection_capable AND '
        'route_projection_controller_incarnation IS NULL AND '
        'route_projection_protocol_version IS NULL) OR '
        '(route_projection_capable AND '
        'route_projection_controller_incarnation IS NOT NULL AND '
        'route_projection_protocol_version IN (1, 2)))')
    op.add_column(
        'serve_route_snapshots',
        sa.Column('producer_protocol_version',
                  sa.Integer(),
                  nullable=False,
                  server_default='1'))
    op.create_check_constraint('serve051_route_producer_protocol_ck',
                               'serve_route_snapshots',
                               'producer_protocol_version IN (1, 2)')

    op.create_table(
        _LEASES,
        sa.Column('service_name', sa.Text(), primary_key=True),
        sa.Column('service_hash', sa.Text(), primary_key=True),
        sa.Column('replica_id', sa.Integer(), primary_key=True),
        sa.Column('replica_record_id', sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column('service_lifecycle_epoch', sa.BigInteger(), nullable=False),
        sa.Column('controller_incarnation',
                  sa.Uuid(as_uuid=True),
                  nullable=False),
        sa.Column('controller_owner_epoch', sa.BigInteger(), nullable=False),
        sa.Column('controller_pid', sa.Integer(), nullable=False),
        sa.Column('controller_ip', sa.Text(), nullable=False),
        sa.Column('service_version', sa.Integer(), nullable=False),
        sa.Column('route_url', sa.Text(), nullable=False),
        sa.Column('gpu_type', sa.Text(), nullable=False),
        sa.Column('gpu_count', sa.Integer(), nullable=False),
        sa.Column('probe_method', sa.Text(), nullable=False),
        sa.Column('readiness_path', sa.Text(), nullable=False),
        sa.Column('probe_timeout_seconds', sa.Integer(), nullable=False),
        sa.Column('probe_post_data', postgresql.JSONB(none_as_null=True)),
        sa.Column('probe_headers', postgresql.JSONB(none_as_null=True)),
        sa.Column('async_occupancy', sa.Boolean()),
        sa.Column('uses_logical_replicas', sa.Boolean(), nullable=False),
        sa.Column('is_zero_cost', sa.Boolean(), nullable=False),
        sa.Column('planned_capacity', sa.Integer(), nullable=False),
        sa.Column('route_allowed', sa.Boolean(), nullable=False),
        sa.Column('requires_route_marker', sa.Boolean(), nullable=False),
        sa.Column('route_marker_payload', postgresql.JSONB(none_as_null=True)),
        sa.Column('material_sha256', sa.Text(), nullable=False),
        sa.Column('material_generation', sa.BigInteger(), nullable=False),
        sa.Column('readiness_generation',
                  sa.BigInteger(),
                  nullable=False,
                  server_default='0'),
        sa.Column('ready',
                  sa.Boolean(),
                  nullable=False,
                  server_default=sa.false()),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('observed_at', sa.DateTime(timezone=True)),
        sa.Column('valid_until', sa.DateTime(timezone=True)),
        sa.Column('revocation_generation',
                  sa.BigInteger(),
                  nullable=False,
                  server_default='0'),
        sa.Column('revoked_at', sa.DateTime(timezone=True)),
        sa.Column('revocation_reason', sa.Text()),
        sa.ForeignKeyConstraint(['service_name'], ['services.name'],
                                name='serve051_route_lease_service_fk',
                                ondelete='CASCADE'),
        sa.CheckConstraint(
            'replica_id > 0 AND service_lifecycle_epoch > 0 AND '
            'controller_owner_epoch > 0 AND controller_pid > 0 AND '
            'service_version > 0',
            name='serve051_route_lease_owner_positive_ck'),
        sa.CheckConstraint(
            'length(service_hash) > 0 AND length(controller_ip) > 0 AND '
            'length(route_url) > 0 AND length(gpu_type) > 0',
            name='serve051_route_lease_text_nonempty_ck'),
        sa.CheckConstraint(
            "probe_method IN ('GET', 'POST') AND "
            "left(readiness_path, 1) = '/'",
            name='serve051_route_lease_probe_ck'),
        sa.CheckConstraint(
            'gpu_count > 0 AND planned_capacity > 0 AND '
            'probe_timeout_seconds > 0 AND probe_timeout_seconds <= 86400',
            name='serve051_route_lease_capacity_positive_ck'),
        sa.CheckConstraint("material_sha256 ~ '^[0-9a-f]{64}$'",
                           name='serve051_route_lease_digest_ck'),
        sa.CheckConstraint(
            'material_generation > 0 AND readiness_generation >= 0 AND '
            'revocation_generation >= 0',
            name='serve051_route_lease_generation_ck'),
        sa.CheckConstraint(
            '((observed_at IS NULL AND valid_until IS NULL AND NOT ready) OR '
            '(observed_at IS NOT NULL AND valid_until IS NOT NULL AND '
            'valid_until > observed_at))',
            name='serve051_route_lease_readiness_shape_ck'),
        sa.CheckConstraint(
            '((revoked_at IS NULL AND revocation_reason IS NULL) OR '
            '(revoked_at IS NOT NULL AND length(revocation_reason) > 0))',
            name='serve051_route_lease_revocation_shape_ck'),
    )
    op.create_index(
        'ix_serve051_route_lease_candidates', _LEASES,
        ['service_name', 'service_hash', 'revoked_at', 'valid_until'])
    op.create_index('ix_serve051_route_lease_replica', _LEASES,
                    ['service_name', 'replica_id'])


def downgrade() -> None:
    """Preserve exact lease evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve051 is forward-only. Demote every protocol-2 route service and '
        'stop incremental route workers before application rollback.')
