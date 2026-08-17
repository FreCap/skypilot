"""Add grant-before-row zero-cost actuation intents.

Revision ID: 052
Revises: 051
Create Date: 2026-08-17

Serve052 is additive, dark by default, and PostgreSQL-only. Existing services
retain direct reserved-fill replica materialization until explicitly promoted.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '052'
down_revision: str | Sequence[str] | None = '051'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SERVICES = 'services'
_INTENTS = 'serve_zero_cost_actuation_intents'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('Zero-cost actuation intents are PostgreSQL-only.')


def upgrade() -> None:
    """Install the dark durable-intent actuation path."""
    _require_postgresql()
    op.add_column(
        _SERVICES,
        sa.Column('reserved_fill_actuation_mode',
                  sa.Text(),
                  nullable=False,
                  server_default='DIRECT_REPLICA'))
    op.add_column(
        _SERVICES,
        sa.Column('reserved_fill_actuation_epoch',
                  sa.BigInteger(),
                  nullable=False,
                  server_default='0'))
    op.add_column(
        _SERVICES,
        sa.Column('reserved_fill_actuation_capable',
                  sa.Boolean(),
                  nullable=False,
                  server_default=sa.false()))
    op.add_column(
        _SERVICES,
        sa.Column('reserved_fill_actuation_controller_incarnation', sa.Uuid()))
    op.add_column(
        _SERVICES,
        sa.Column('reserved_fill_actuation_protocol_version', sa.Integer()))
    op.create_check_constraint(
        'serve052_fill_actuation_mode_ck', _SERVICES,
        "reserved_fill_actuation_mode IN ('DIRECT_REPLICA', "
        "'DURABLE_INTENT')")
    op.create_check_constraint('serve052_fill_actuation_epoch_ck', _SERVICES,
                               'reserved_fill_actuation_epoch >= 0')
    op.create_check_constraint(
        'serve052_fill_actuation_capability_shape_ck', _SERVICES,
        '((NOT reserved_fill_actuation_capable AND '
        'reserved_fill_actuation_controller_incarnation IS NULL AND '
        'reserved_fill_actuation_protocol_version IS NULL) OR '
        '(reserved_fill_actuation_capable AND '
        'reserved_fill_actuation_controller_incarnation IS NOT NULL AND '
        'reserved_fill_actuation_protocol_version = 1))')
    op.create_check_constraint(
        'serve052_durable_fill_actuation_capability_ck', _SERVICES,
        "reserved_fill_actuation_mode <> 'DURABLE_INTENT' OR "
        '(reserved_fill_actuation_epoch > 0 AND '
        'reserved_fill_actuation_capable)')

    op.create_table(
        _INTENTS,
        sa.Column('intent_idempotency_key', sa.Text(), primary_key=True),
        sa.Column('service_name', sa.Text(), nullable=False),
        sa.Column('service_hash', sa.Text(), nullable=False),
        sa.Column('service_lifecycle_epoch', sa.BigInteger(), nullable=False),
        sa.Column('actuation_epoch', sa.BigInteger(), nullable=False),
        sa.Column('service_version', sa.Integer(), nullable=False),
        sa.Column('controller_owner', sa.Text(), nullable=False),
        sa.Column('ordinal', sa.Integer(), nullable=False),
        sa.Column('protocol_version', sa.Integer(), nullable=False),
        sa.Column('policy_revision', sa.Integer(), nullable=False),
        sa.Column('reconcile_generation', sa.BigInteger(), nullable=False),
        sa.Column('allocation_generation', sa.BigInteger(), nullable=False),
        sa.Column('allocation_input_sha256', sa.Text(), nullable=False),
        sa.Column('allocation_claim_generation',
                  sa.BigInteger(),
                  nullable=False),
        sa.Column('reconciliation_gate_generation',
                  sa.BigInteger(),
                  nullable=False),
        sa.Column('reclaim_fleet_bundle_sha256', sa.Text(), nullable=False),
        sa.Column('reclaim_policy_revision', sa.Text(), nullable=False),
        sa.Column('reclaim_provider_inventory_sha256',
                  sa.Text(),
                  nullable=False),
        sa.Column('service_generation', sa.BigInteger(), nullable=False),
        sa.Column('pool_key', sa.Text(), nullable=False),
        sa.Column('pool_epoch', sa.BigInteger(), nullable=False),
        sa.Column('physical_cluster_uid', sa.Text(), nullable=False),
        sa.Column('kubernetes_context', sa.Text(), nullable=False),
        sa.Column('worker_projection_sha256', sa.Text(), nullable=False),
        sa.Column('observation_generation', sa.BigInteger(), nullable=False),
        sa.Column('observation_sequence', sa.BigInteger(), nullable=False),
        sa.Column('ordinary_zero_cost_admission_sequence',
                  sa.BigInteger(),
                  nullable=False),
        # PostgreSQL timestamps round to microseconds.  Preserve the exact
        # double used by FillIntent's idempotency payload as separate durable
        # authority while retaining a timestamp for indexed database-clock
        # expiry comparisons.
        sa.Column('valid_until_epoch', sa.Float(), nullable=False),
        sa.Column('valid_until', sa.DateTime(timezone=True), nullable=False),
        sa.Column('accelerator', sa.Text(), nullable=False),
        sa.Column('accelerator_count', sa.Integer(), nullable=False),
        sa.Column('capacity_unit', sa.Text(), nullable=False),
        sa.Column('planned_capacity', sa.Integer(), nullable=False),
        sa.Column('allowed_locations',
                  postgresql.JSONB(none_as_null=True),
                  nullable=False),
        sa.Column('state', sa.Text(), nullable=False),
        sa.Column('lease_owner', sa.Uuid()),
        sa.Column('lease_generation',
                  sa.BigInteger(),
                  nullable=False,
                  server_default='0'),
        sa.Column('lease_expires_at', sa.DateTime(timezone=True)),
        sa.Column('replica_id', sa.Integer()),
        sa.Column('replica_record_id', sa.Uuid()),
        sa.Column('last_error', sa.Text()),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('committed_at', sa.DateTime(timezone=True)),
        sa.Column('terminal_at', sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(['service_name'], ['services.name'],
                                name='serve052_zero_cost_intent_service_fk',
                                ondelete='CASCADE'),
        sa.CheckConstraint(
            "intent_idempotency_key ~ '^[0-9a-f]{64}$' AND "
            "allocation_input_sha256 ~ '^[0-9a-f]{64}$' AND "
            "reclaim_fleet_bundle_sha256 ~ '^[0-9a-f]{64}$' AND "
            "reclaim_provider_inventory_sha256 ~ '^[0-9a-f]{64}$' AND "
            "worker_projection_sha256 ~ '^[0-9a-f]{64}$'",
            name='serve052_zero_cost_intent_digest_ck'),
        sa.CheckConstraint(
            'service_lifecycle_epoch > 0 AND actuation_epoch > 0 AND '
            'service_version > 0 AND ordinal >= 0 AND '
            'protocol_version = 2 AND policy_revision > 0 AND '
            'reconcile_generation > 0 AND allocation_generation > 0 AND '
            'allocation_claim_generation > 0 AND '
            'reconciliation_gate_generation > 0 AND '
            'service_generation > 0 AND pool_epoch > 0 AND '
            'observation_generation > 0 AND observation_sequence >= 0 AND '
            'ordinary_zero_cost_admission_sequence >= 0 AND '
            'ordinary_zero_cost_admission_sequence <= observation_sequence '
            'AND accelerator_count > 0 AND planned_capacity > 0 AND '
            'lease_generation >= 0',
            name='serve052_zero_cost_intent_positive_ck'),
        sa.CheckConstraint(
            "length(service_name) > 0 AND length(service_hash) > 0 AND "
            "length(controller_owner) > 0 AND length(pool_key) > 0 AND "
            "length(physical_cluster_uid) > 0 AND "
            "length(kubernetes_context) > 0 AND length(accelerator) > 0 AND "
            "length(reclaim_policy_revision) > 0",
            name='serve052_zero_cost_intent_text_ck'),
        sa.CheckConstraint("capacity_unit IN ('physical', 'logical')",
                           name='serve052_zero_cost_intent_unit_ck'),
        sa.CheckConstraint(
            "state IN ('GRANTED', 'ACTUATING', 'COMMITTED', 'RETRYABLE', "
            "'TERMINAL')",
            name='serve052_zero_cost_intent_state_ck'),
        sa.CheckConstraint(
            "valid_until_epoch > 0 AND valid_until_epoch < 'Infinity'::"
            'double precision AND '
            'abs(extract(epoch FROM valid_until)::double precision - '
            'valid_until_epoch) < 0.000001 AND '
            'valid_until > created_at',
            name='serve052_zero_cost_intent_expiry_ck'),
        sa.CheckConstraint(
            "((state IN ('GRANTED', 'RETRYABLE') AND "
            'lease_owner IS NULL AND lease_expires_at IS NULL AND '
            'replica_id IS NULL AND replica_record_id IS NULL AND '
            'committed_at IS NULL AND terminal_at IS NULL) OR '
            "(state = 'ACTUATING' AND lease_owner IS NOT NULL AND "
            'lease_generation > 0 AND lease_expires_at IS NOT NULL AND '
            'replica_id IS NULL AND replica_record_id IS NULL AND '
            'committed_at IS NULL AND terminal_at IS NULL) OR '
            "(state = 'COMMITTED' AND lease_owner IS NULL AND "
            'lease_expires_at IS NULL AND replica_id IS NOT NULL AND '
            'replica_record_id IS NOT NULL AND committed_at IS NOT NULL AND '
            'terminal_at IS NULL) OR '
            "(state = 'TERMINAL' AND lease_owner IS NULL AND "
            'lease_expires_at IS NULL AND replica_id IS NULL AND '
            'replica_record_id IS NULL AND committed_at IS NULL AND '
            'terminal_at IS NOT NULL))',
            name='serve052_zero_cost_intent_state_shape_ck'),
        sa.UniqueConstraint('service_name',
                            'replica_id',
                            name='serve052_zero_cost_intent_replica_uq'),
    )
    op.create_index('ix_serve052_zero_cost_intent_actionable', _INTENTS,
                    ['pool_key', 'state', 'valid_until'])
    op.create_index('ix_serve052_zero_cost_intent_service', _INTENTS,
                    ['service_name', 'state'])


def downgrade() -> None:
    """Preserve actuation evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve052 is forward-only. Demote every DURABLE_INTENT service and '
        'settle every zero-cost actuation intent before application rollback.')
