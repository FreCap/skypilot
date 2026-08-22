"""Add PostgreSQL authority for three-state Kueue admissions.

Revision ID: 057
Revises: 056
Create Date: 2026-08-22

The earlier source-only Serve057 shape was never deployed.  This revision is
therefore the canonical additive schema and intentionally performs no backfill.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '057'
down_revision: str | Sequence[str] | None = '056'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INTENTS = 'serve_zero_cost_actuation_intents'
_REPLICAS = 'replicas'
_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_ADMISSIONS = 'serve_kueue_admissions'
_GUARD_FUNCTION = 'skyserve057_guard_kueue_admission'
_GUARD_TRIGGER = 'skyserve057_kueue_admission_guard'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('Kueue admission authority is PostgreSQL-only.')


def _install_admission_guard() -> None:
    """Keep authority immutable and admission/lease transitions monotonic."""
    op.execute(f'''
        CREATE OR REPLACE FUNCTION {_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF NEW.intent_idempotency_key IS DISTINCT FROM
                    OLD.intent_idempotency_key OR
               NEW.service_name IS DISTINCT FROM OLD.service_name OR
               NEW.unresolved_domain_sha256 IS DISTINCT FROM
                    OLD.unresolved_domain_sha256 OR
               NEW.service_hash IS DISTINCT FROM OLD.service_hash OR
               NEW.service_lifecycle_epoch IS DISTINCT FROM
                    OLD.service_lifecycle_epoch OR
               NEW.service_version IS DISTINCT FROM OLD.service_version OR
               NEW.pool_key IS DISTINCT FROM OLD.pool_key OR
               NEW.pool_epoch IS DISTINCT FROM OLD.pool_epoch OR
               NEW.physical_cluster_uid IS DISTINCT FROM
                    OLD.physical_cluster_uid OR
               NEW.kubernetes_context IS DISTINCT FROM
                    OLD.kubernetes_context OR
               NEW.accelerator IS DISTINCT FROM OLD.accelerator OR
               NEW.accelerator_count IS DISTINCT FROM OLD.accelerator_count OR
               NEW.worker_projection_sha256 IS DISTINCT FROM
                    OLD.worker_projection_sha256 OR
               NEW.capacity_unit IS DISTINCT FROM OLD.capacity_unit OR
               NEW.planned_capacity IS DISTINCT FROM OLD.planned_capacity OR
               NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'Kueue admission identity is immutable';
            END IF;

            IF OLD.replica_id IS NOT NULL AND
               (NEW.replica_id IS DISTINCT FROM OLD.replica_id OR
                NEW.replica_record_id IS DISTINCT FROM
                    OLD.replica_record_id OR
                NEW.provider_cluster_generation IS DISTINCT FROM
                    OLD.provider_cluster_generation OR
                NEW.association_id IS DISTINCT FROM OLD.association_id) THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'Kueue admission materialization is immutable';
            END IF;

            IF OLD.pod_uid IS NOT NULL AND
               (NEW.pod_namespace IS DISTINCT FROM OLD.pod_namespace OR
                NEW.pod_name IS DISTINCT FROM OLD.pod_name OR
                NEW.pod_uid IS DISTINCT FROM OLD.pod_uid) THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'Kueue admission Pod identity is immutable';
            END IF;

            IF OLD.replacement_surge_units = 0 AND
               (NEW.replacement_surge_units <> 0 OR
                NEW.replacement_compatibility_sha256 IS NOT NULL) THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'Kueue replacement surge cannot be acquired after grant';
            END IF;
            IF OLD.replacement_surge_units > 0 AND NOT (
               (NEW.replacement_surge_units = OLD.replacement_surge_units AND
                NEW.replacement_compatibility_sha256 IS NOT DISTINCT FROM
                    OLD.replacement_compatibility_sha256) OR
               (NEW.replacement_surge_units = 0 AND
                NEW.replacement_compatibility_sha256 IS NULL)) THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'Kueue replacement surge may only be released';
            END IF;

            IF NEW.state IS DISTINCT FROM OLD.state AND NOT (
               (OLD.state = 'INTENT_PENDING' AND
                NEW.state IN ('POD_WAITING', 'POLICY_ADMITTED')) OR
               (OLD.state = 'POD_WAITING' AND
                NEW.state = 'POLICY_ADMITTED')) THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'invalid Kueue admission state transition';
            END IF;

            IF OLD.admitted_at IS NOT NULL AND
               NEW.admitted_at IS DISTINCT FROM OLD.admitted_at THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'Kueue admitted fact is immutable';
            END IF;
            IF OLD.observed_at IS NOT NULL AND NEW.observed_at IS NOT NULL AND
               NEW.observed_at < OLD.observed_at THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'Kueue admission observation time is monotonic';
            END IF;
            RETURN NEW;
        END;
        $function$
    ''')
    op.execute(f'''
        CREATE TRIGGER {_GUARD_TRIGGER}
        BEFORE UPDATE ON {_ADMISSIONS}
        FOR EACH ROW EXECUTE FUNCTION {_GUARD_FUNCTION}()
    ''')


def upgrade() -> None:
    """Install the canonical three-state Kueue admission relation."""
    _require_postgresql()
    op.create_table(
        _ADMISSIONS,
        sa.Column('intent_idempotency_key', sa.Text(), primary_key=True),
        sa.Column('service_name', sa.Text(), nullable=False),
        sa.Column('unresolved_domain_sha256', sa.Text(), nullable=False),
        sa.Column('service_hash', sa.Text(), nullable=False),
        sa.Column('service_lifecycle_epoch', sa.BigInteger(), nullable=False),
        sa.Column('service_version', sa.Integer(), nullable=False),
        sa.Column('pool_key', sa.Text(), nullable=False),
        sa.Column('pool_epoch', sa.BigInteger(), nullable=False),
        sa.Column('physical_cluster_uid', sa.Text(), nullable=False),
        sa.Column('kubernetes_context', sa.Text(), nullable=False),
        sa.Column('accelerator', sa.Text(), nullable=False),
        sa.Column('accelerator_count', sa.Integer(), nullable=False),
        sa.Column('worker_projection_sha256', sa.Text(), nullable=False),
        sa.Column('capacity_unit', sa.Text(), nullable=False),
        sa.Column('planned_capacity', sa.Integer(), nullable=False),
        sa.Column('state', sa.Text(), nullable=False),
        sa.Column('replica_id', sa.Integer()),
        sa.Column('replica_record_id', sa.Uuid(as_uuid=True)),
        sa.Column('provider_cluster_generation', sa.BigInteger()),
        sa.Column('association_id', sa.Uuid(as_uuid=True)),
        sa.Column('pod_namespace', sa.Text()),
        sa.Column('pod_name', sa.Text()),
        sa.Column('pod_uid', sa.Text()),
        sa.Column('pod_receipt', postgresql.JSONB(none_as_null=True)),
        sa.Column('pod_receipt_sha256', sa.Text()),
        sa.Column('observed_at', sa.DateTime(timezone=True)),
        sa.Column('valid_until', sa.DateTime(timezone=True)),
        sa.Column('admitted_at', sa.DateTime(timezone=True)),
        sa.Column('replacement_surge_units',
                  sa.Integer(),
                  nullable=False,
                  server_default='0'),
        sa.Column('replacement_compatibility_sha256', sa.Text()),
        sa.Column('created_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.text('clock_timestamp()')),
        sa.Column('updated_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.text('clock_timestamp()')),
        sa.ForeignKeyConstraint(
            ['service_name', 'intent_idempotency_key'],
            [f'{_INTENTS}.service_name', f'{_INTENTS}.intent_idempotency_key'],
            name='serve057_kueue_admission_intent_fk',
            ondelete='RESTRICT'),
        sa.ForeignKeyConstraint(
            ['service_name', 'replica_id'],
            [f'{_REPLICAS}.service_name', f'{_REPLICAS}.replica_id'],
            name='serve057_kueue_admission_replica_fk',
            ondelete='RESTRICT'),
        sa.ForeignKeyConstraint(['association_id'],
                                [f'{_ASSOCIATIONS}.association_id'],
                                name='serve057_kueue_admission_association_fk',
                                ondelete='RESTRICT'),
        sa.UniqueConstraint('service_name',
                            'replica_id',
                            name='serve057_kueue_admission_replica_uq'),
        sa.UniqueConstraint('association_id',
                            name='serve057_kueue_admission_association_uq'),
        sa.CheckConstraint(
            "intent_idempotency_key ~ '^[0-9a-f]{64}$' AND "
            "unresolved_domain_sha256 ~ '^[0-9a-f]{64}$' AND "
            "worker_projection_sha256 ~ '^[0-9a-f]{64}$' AND "
            '(pod_receipt_sha256 IS NULL OR '
            "pod_receipt_sha256 ~ '^[0-9a-f]{64}$') AND "
            '(replacement_compatibility_sha256 IS NULL OR '
            "replacement_compatibility_sha256 ~ '^[0-9a-f]{64}$')",
            name='serve057_kueue_admission_digest_ck'),
        sa.CheckConstraint(
            'unresolved_domain_sha256 = encode(sha256(convert_to('
            "octet_length(service_name)::text || ':' || service_name || "
            "'|' || service_lifecycle_epoch::text || '|' || "
            "octet_length(physical_cluster_uid)::text || ':' || "
            "physical_cluster_uid || '|' || "
            "octet_length(accelerator)::text || ':' || accelerator || '|' || "
            "accelerator_count::text, 'UTF8')), 'hex')",
            name='serve057_kueue_admission_domain_ck'),
        sa.CheckConstraint(
            'service_lifecycle_epoch > 0 AND service_version > 0 AND '
            'pool_epoch > 0 AND accelerator_count > 0 AND '
            'planned_capacity > 0 AND replacement_surge_units >= 0 AND '
            'replacement_surge_units <= planned_capacity AND '
            '(replica_id IS NULL OR replica_id > 0) AND '
            '(provider_cluster_generation IS NULL OR '
            'provider_cluster_generation > 0)',
            name='serve057_kueue_admission_positive_ck'),
        sa.CheckConstraint(
            'octet_length(service_name) > 0 AND '
            'octet_length(service_hash) > 0 AND octet_length(pool_key) > 0 '
            'AND octet_length(physical_cluster_uid) > 0 AND '
            'octet_length(kubernetes_context) > 0 AND '
            'octet_length(accelerator) > 0 AND '
            'accelerator = lower(accelerator)',
            name='serve057_kueue_admission_text_ck'),
        sa.CheckConstraint(
            "(capacity_unit = 'physical' AND planned_capacity = 1) OR "
            "(capacity_unit = 'logical' AND "
            'planned_capacity = accelerator_count)',
            name='serve057_kueue_admission_capacity_ck'),
        sa.CheckConstraint(
            "state IN ('INTENT_PENDING', 'POD_WAITING', "
            "'POLICY_ADMITTED')",
            name='serve057_kueue_admission_state_ck'),
        sa.CheckConstraint(
            'num_nonnulls(replica_id, replica_record_id, '
            'provider_cluster_generation, association_id) IN (0, 4)',
            name='serve057_kueue_admission_materialization_ck'),
        sa.CheckConstraint(
            'num_nonnulls(pod_namespace, pod_name, pod_uid, pod_receipt, '
            'pod_receipt_sha256) IN (0, 5) AND '
            '(pod_namespace IS NULL OR '
            '(octet_length(pod_namespace) > 0 AND '
            'octet_length(pod_name) > 0 AND octet_length(pod_uid) > 0 AND '
            "jsonb_typeof(pod_receipt) = 'object' AND "
            'octet_length(pod_receipt::text) <= 65536 AND '
            'pod_receipt_sha256 = encode(sha256(convert_to('
            "pod_receipt::text, 'UTF8')), 'hex')))",
            name='serve057_kueue_admission_pod_receipt_ck'),
        sa.CheckConstraint(
            '(replacement_surge_units = 0 AND '
            'replacement_compatibility_sha256 IS NULL) OR '
            '(replacement_surge_units > 0 AND '
            'replacement_compatibility_sha256 IS NOT NULL)',
            name='serve057_kueue_admission_surge_ck'),
        sa.CheckConstraint(
            'updated_at >= created_at AND '
            "((state = 'INTENT_PENDING' AND pod_namespace IS NULL AND "
            'observed_at IS NULL AND valid_until IS NULL AND '
            'admitted_at IS NULL) OR '
            "(state = 'POD_WAITING' AND replica_id IS NOT NULL AND "
            'pod_namespace IS NOT NULL AND observed_at IS NOT NULL AND '
            "valid_until = observed_at + INTERVAL '15 seconds' AND "
            'admitted_at IS NULL) OR '
            "(state = 'POLICY_ADMITTED' AND replica_id IS NOT NULL AND "
            'pod_namespace IS NOT NULL AND observed_at IS NOT NULL AND '
            'valid_until IS NULL AND admitted_at IS NOT NULL AND '
            'admitted_at <= observed_at))',
            name='serve057_kueue_admission_state_shape_ck'),
    )
    op.create_index('uq_serve057_kueue_admission_surge',
                    _ADMISSIONS, ['service_name'],
                    unique=True,
                    postgresql_where=sa.text('replacement_surge_units > 0'))
    op.create_index('ix_serve057_kueue_admission_service_state', _ADMISSIONS,
                    ['service_name', 'state'])
    _install_admission_guard()


def downgrade() -> None:
    """Preserve live Kueue admission authority across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve057 is forward-only; clean every Kueue admission before '
        'application rollback.')
