"""Bind each sequenced reserved-fill generation to exact authority.

Revision ID: 045
Revises: 044
Create Date: 2026-08-12

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

revision: str = '045'
down_revision: str | Sequence[str] | None = '044'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PROTOCOL_STATE = 'reserved_fill_protocol_state'
_LEGACY_ACTIVE = 'LEGACY_ACTIVE'
_SEQUENCED_ACTIVE = 'SEQUENCED_ACTIVE'
_OLD_IDENTITY_CHECK = 'ck_reserved_fill_reclaim_policy_identity'
_AUTHORIZATION_CHECK = 'ck_reserved_fill_reclaim_authorization'
_OLD_GATE_GUARD_FUNCTION = 'skyserve044_guard_reconciliation_gate'
_OLD_GATE_GUARD_TRIGGER = 'skyserve044_reconciliation_gate_guard'
_GATE_GUARD_FUNCTION = 'skyserve045_guard_reconciliation_gate'
_GATE_GUARD_TRIGGER = 'skyserve045_reconciliation_gate_guard'
_GATE_TRUNCATE_GUARD_TRIGGER = (
    'skyserve045_reconciliation_gate_truncate_guard')
_SHA256_PATTERN = '^[0-9a-f]{64}$'
_AUTHORIZATION_CHECK_SQL = f"""
(
  (
    reconciliation_gate_state = '{_LEGACY_ACTIVE}'
    AND reclaim_fleet_bundle_sha256 IS NULL
    AND reclaim_policy_revision IS NULL
    AND reclaim_provider_inventory_sha256 IS NULL
    AND reclaim_claim_scope_count IS NULL
    AND reclaim_claim_scope_sha256 IS NULL
    AND reclaim_evidence_sha256 IS NULL
    AND reclaim_authorized_at IS NULL
  )
  OR
  (
    reconciliation_gate_state = '{_SEQUENCED_ACTIVE}'
    AND protocol_version = 2
    AND reclaim_fleet_bundle_sha256 IS NOT NULL
    AND reclaim_fleet_bundle_sha256 ~ '{_SHA256_PATTERN}'
    AND reclaim_policy_revision IS NOT NULL
    AND octet_length(reclaim_policy_revision) BETWEEN 1 AND 1024
    AND reclaim_provider_inventory_sha256 IS NOT NULL
    AND reclaim_provider_inventory_sha256 ~ '{_SHA256_PATTERN}'
    AND reclaim_claim_scope_count IS NOT NULL
    AND reclaim_claim_scope_count >= 0
    AND reclaim_claim_scope_sha256 IS NOT NULL
    AND reclaim_claim_scope_sha256 ~ '{_SHA256_PATTERN}'
    AND reclaim_evidence_sha256 IS NOT NULL
    AND reclaim_evidence_sha256 ~ '{_SHA256_PATTERN}'
    AND reclaim_authorized_at IS NOT NULL
    AND reclaim_authorized_at >= 0
    AND image_digest IS NOT NULL
    AND image_digest ~ '^sha256:[0-9a-f]{{64}}$'
    AND deployment_generation IS NOT NULL
    AND octet_length(deployment_generation) BETWEEN 1 AND 1024
    AND deployment_uid IS NOT NULL
    AND octet_length(deployment_uid) BETWEEN 1 AND 1024
    AND pod_inventory_count IS NOT NULL
    AND pod_inventory_count > 0
    AND pod_inventory_sha256 IS NOT NULL
    AND pod_inventory_sha256 ~ '{_SHA256_PATTERN}'
  )
)
""".strip()


def _authorization_columns() -> tuple[sa.Column, ...]:
    """Return the revision-frozen authorization column definitions."""
    return (
        sa.Column('reclaim_fleet_bundle_sha256', sa.Text(), nullable=True),
        sa.Column('reclaim_policy_revision', sa.Text(), nullable=True),
        sa.Column('reclaim_provider_inventory_sha256', sa.Text(),
                  nullable=True),
        sa.Column('reclaim_claim_scope_count', sa.BigInteger(), nullable=True),
        sa.Column('reclaim_claim_scope_sha256', sa.Text(), nullable=True),
        sa.Column('reclaim_evidence_sha256', sa.Text(), nullable=True),
        sa.Column('reclaim_authorized_at', sa.Float(), nullable=True),
        sa.Column('image_digest', sa.Text(), nullable=True),
        sa.Column('deployment_generation', sa.Text(), nullable=True),
        sa.Column('deployment_uid', sa.Text(), nullable=True),
        sa.Column('pod_inventory_count', sa.Integer(), nullable=True),
        sa.Column('pod_inventory_sha256', sa.Text(), nullable=True),
    )


def _reject_unbound_sequenced_gate(bind: sa.engine.Connection) -> None:
    """Refuse to invent authority for a Serve044 gate already in use."""
    row = bind.execute(
        sa.text('SELECT reconciliation_gate_state '
                f'FROM {_PROTOCOL_STATE} WHERE id = 1 FOR UPDATE')).mappings(
                ).one_or_none()
    if row is None:
        raise RuntimeError(
            'Cannot install Serve045 without protocol singleton.')
    if row['reconciliation_gate_state'] == _SEQUENCED_ACTIVE:
        raise RuntimeError(
            'Serve045 cannot backfill reclaim authorization for an '
            'already-sequenced Serve044 database; fix forward with an '
            'explicit audited repair.')
    if row['reconciliation_gate_state'] != _LEGACY_ACTIVE:
        raise RuntimeError(
            'Cannot install Serve045 with a malformed reconciliation gate.')


def _add_authorization_columns(bind: sa.engine.Connection) -> None:
    existing = {
        str(column['name'])
        for column in sa.inspect(bind).get_columns(_PROTOCOL_STATE)
    }
    for column in _authorization_columns():
        if column.name not in existing:
            op.add_column(_PROTOCOL_STATE, column)


def _replace_authorization_check(bind: sa.engine.Connection) -> None:
    checks = {
        str(check['name'])
        for check in sa.inspect(bind).get_check_constraints(_PROTOCOL_STATE)
        if check['name'] is not None
    }
    for name in (_OLD_IDENTITY_CHECK, _AUTHORIZATION_CHECK):
        if name in checks:
            op.drop_constraint(name, _PROTOCOL_STATE, type_='check')
    op.create_check_constraint(_AUTHORIZATION_CHECK, _PROTOCOL_STATE,
                               _AUTHORIZATION_CHECK_SQL)


def _replace_gate_guard() -> None:
    """Install the sole generation-fenced authorization guard."""
    op.execute(f'DROP TRIGGER IF EXISTS {_OLD_GATE_GUARD_TRIGGER} '
               f'ON {_PROTOCOL_STATE}')
    op.execute(f'DROP FUNCTION IF EXISTS {_OLD_GATE_GUARD_FUNCTION}()')
    op.execute(f'DROP TRIGGER IF EXISTS {_GATE_GUARD_TRIGGER} '
               f'ON {_PROTOCOL_STATE}')
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_GATE_GUARD_FUNCTION}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
                RAISE EXCEPTION
                    'SkyServe reconciliation gate singleton cannot be removed';
            END IF;
            IF NEW.reconciliation_gate_generation <
                   OLD.reconciliation_gate_generation THEN
                RAISE EXCEPTION
                    'SkyServe reconciliation gate generation cannot decrease';
            END IF;

            -- Protocol-v2 rollout proof may still be refreshed before the
            -- sequenced path is activated. No reclaim authority exists yet.
            IF OLD.reconciliation_gate_state = '{_LEGACY_ACTIVE}'
               AND NEW.reconciliation_gate_state = '{_LEGACY_ACTIVE}'
               AND NEW.reconciliation_gate_generation =
                       OLD.reconciliation_gate_generation
               AND NEW.reclaim_fleet_bundle_sha256 IS NOT DISTINCT FROM
                       OLD.reclaim_fleet_bundle_sha256
               AND NEW.reclaim_policy_revision IS NOT DISTINCT FROM
                       OLD.reclaim_policy_revision
               AND NEW.reclaim_provider_inventory_sha256 IS NOT DISTINCT FROM
                       OLD.reclaim_provider_inventory_sha256
               AND NEW.reclaim_claim_scope_count IS NOT DISTINCT FROM
                       OLD.reclaim_claim_scope_count
               AND NEW.reclaim_claim_scope_sha256 IS NOT DISTINCT FROM
                       OLD.reclaim_claim_scope_sha256
               AND NEW.reclaim_evidence_sha256 IS NOT DISTINCT FROM
                       OLD.reclaim_evidence_sha256
               AND NEW.reclaim_authorized_at IS NOT DISTINCT FROM
                       OLD.reclaim_authorized_at THEN
                RETURN NEW;
            END IF;

            -- Exact no-op writes remain harmless and make retries stable.
            IF NEW.reconciliation_gate_state IS NOT DISTINCT FROM
                   OLD.reconciliation_gate_state
               AND NEW.reconciliation_gate_generation IS NOT DISTINCT FROM
                   OLD.reconciliation_gate_generation
               AND NEW.reclaim_fleet_bundle_sha256 IS NOT DISTINCT FROM
                   OLD.reclaim_fleet_bundle_sha256
               AND NEW.reclaim_policy_revision IS NOT DISTINCT FROM
                   OLD.reclaim_policy_revision
               AND NEW.reclaim_provider_inventory_sha256 IS NOT DISTINCT FROM
                   OLD.reclaim_provider_inventory_sha256
               AND NEW.reclaim_claim_scope_count IS NOT DISTINCT FROM
                   OLD.reclaim_claim_scope_count
               AND NEW.reclaim_claim_scope_sha256 IS NOT DISTINCT FROM
                   OLD.reclaim_claim_scope_sha256
               AND NEW.reclaim_evidence_sha256 IS NOT DISTINCT FROM
                   OLD.reclaim_evidence_sha256
               AND NEW.reclaim_authorized_at IS NOT DISTINCT FROM
                   OLD.reclaim_authorized_at
               AND NEW.image_digest IS NOT DISTINCT FROM OLD.image_digest
               AND NEW.deployment_generation IS NOT DISTINCT FROM
                       OLD.deployment_generation
               AND NEW.deployment_uid IS NOT DISTINCT FROM OLD.deployment_uid
               AND NEW.pod_inventory_count IS NOT DISTINCT FROM
                       OLD.pod_inventory_count
               AND NEW.pod_inventory_sha256 IS NOT DISTINCT FROM
                       OLD.pod_inventory_sha256 THEN
                RETURN NEW;
            END IF;

            -- First authorization installs the complete receipt. Demotion has
            -- no valid shape.
            IF OLD.reconciliation_gate_state = '{_LEGACY_ACTIVE}'
               AND NEW.reconciliation_gate_state = '{_SEQUENCED_ACTIVE}'
               AND NEW.reconciliation_gate_generation =
                       OLD.reconciliation_gate_generation + 1
               AND NEW.reclaim_fleet_bundle_sha256 IS NOT NULL
               AND NEW.reclaim_policy_revision IS NOT NULL
               AND NEW.reclaim_provider_inventory_sha256 IS NOT NULL
               AND NEW.reclaim_claim_scope_count IS NOT NULL
               AND NEW.reclaim_claim_scope_sha256 IS NOT NULL
               AND NEW.reclaim_evidence_sha256 IS NOT NULL
               AND NEW.reclaim_authorized_at IS NOT NULL
               AND NEW.image_digest IS NOT NULL
               AND NEW.deployment_generation IS NOT NULL
               AND NEW.deployment_uid IS NOT NULL
               AND NEW.pod_inventory_count IS NOT NULL
               AND NEW.pod_inventory_sha256 IS NOT NULL THEN
                RETURN NEW;
            END IF;

            -- A fix-forward reauthorization is the same exact successor CAS,
            -- but must replace authority material. Replaying an identical
            -- receipt is an application-level no-op, never a generation bump.
            IF OLD.reconciliation_gate_state = '{_SEQUENCED_ACTIVE}'
               AND NEW.reconciliation_gate_state = '{_SEQUENCED_ACTIVE}'
               AND NEW.reconciliation_gate_generation =
                       OLD.reconciliation_gate_generation + 1
               AND NEW.reclaim_evidence_sha256 IS DISTINCT FROM
                       OLD.reclaim_evidence_sha256
               AND NEW.reclaim_fleet_bundle_sha256 IS NOT NULL
               AND NEW.reclaim_policy_revision IS NOT NULL
               AND NEW.reclaim_provider_inventory_sha256 IS NOT NULL
               AND NEW.reclaim_claim_scope_count IS NOT NULL
               AND NEW.reclaim_claim_scope_sha256 IS NOT NULL
               AND NEW.reclaim_evidence_sha256 IS NOT NULL
               AND NEW.reclaim_authorized_at IS NOT NULL
               AND NEW.image_digest IS NOT NULL
               AND NEW.deployment_generation IS NOT NULL
               AND NEW.deployment_uid IS NOT NULL
               AND NEW.pod_inventory_count IS NOT NULL
               AND NEW.pod_inventory_sha256 IS NOT NULL THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION
                'SkyServe reclaim authorization requires one exact successor generation';
        END;
        $function$
    """)
    op.execute(f"""
        CREATE TRIGGER {_GATE_GUARD_TRIGGER}
        BEFORE UPDATE OF reconciliation_gate_state,
                         reconciliation_gate_generation,
                         reclaim_fleet_bundle_sha256,
                         reclaim_policy_revision,
                         reclaim_provider_inventory_sha256,
                         reclaim_claim_scope_count,
                         reclaim_claim_scope_sha256,
                         reclaim_evidence_sha256,
                         reclaim_authorized_at,
                         image_digest,
                         deployment_generation,
                         deployment_uid,
                         pod_inventory_count,
                         pod_inventory_sha256
               OR DELETE
        ON {_PROTOCOL_STATE}
        FOR EACH ROW
        EXECUTE FUNCTION {_GATE_GUARD_FUNCTION}()
    """)
    op.execute(f'DROP TRIGGER IF EXISTS {_GATE_TRUNCATE_GUARD_TRIGGER} '
               f'ON {_PROTOCOL_STATE}')
    op.execute(f"""
        CREATE TRIGGER {_GATE_TRUNCATE_GUARD_TRIGGER}
        BEFORE TRUNCATE
        ON {_PROTOCOL_STATE}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {_GATE_GUARD_FUNCTION}()
    """)
    op.execute(f'ALTER TABLE {_PROTOCOL_STATE} ENABLE ALWAYS TRIGGER '
               f'{_GATE_GUARD_TRIGGER}')
    op.execute(f'ALTER TABLE {_PROTOCOL_STATE} ENABLE ALWAYS TRIGGER '
               f'{_GATE_TRUNCATE_GUARD_TRIGGER}')


def upgrade() -> None:
    """Install generation-bound reclaim authorization on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    _reject_unbound_sequenced_gate(bind)
    _add_authorization_columns(bind)
    _replace_authorization_check(bind)
    _replace_gate_guard()


def downgrade() -> None:
    raise RuntimeError(
        'Serve045 is forward-only. Preserve the durable reclaim receipt and '
        'iterate policy and writer changes through reauthorization.')
