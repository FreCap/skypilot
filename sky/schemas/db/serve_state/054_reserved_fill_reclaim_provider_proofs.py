"""Add process-safe short-lived reserved-fill provider proofs.

Revision ID: 054
Revises: 053
Create Date: 2026-08-20

Serve054 is additive and PostgreSQL-only.  It shares completed provider facts
inside the existing five-second launch horizon without sharing launch scope.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '054'
down_revision: str | Sequence[str] | None = '053'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PROOFS = 'serve_reserved_fill_reclaim_provider_proofs'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Reserved-fill reclaim provider proofs are PostgreSQL-only.')


def upgrade() -> None:
    """Install the bounded completed-provider-proof receipt."""
    _require_postgresql()
    op.create_table(
        _PROOFS,
        sa.Column('receipt_nonce', sa.Text(), primary_key=True),
        sa.Column('reconciliation_gate_generation',
                  sa.BigInteger(),
                  nullable=False),
        sa.Column('reclaim_fleet_bundle_sha256', sa.Text(), nullable=False),
        sa.Column('reclaim_policy_revision', sa.Text(), nullable=False),
        sa.Column('reclaim_provider_inventory_sha256',
                  sa.Text(),
                  nullable=False),
        sa.Column('kubernetes_context', sa.Text(), nullable=False),
        sa.Column('proof_schema_version', sa.Integer(), nullable=False),
        sa.Column('proof_payload',
                  postgresql.JSONB(none_as_null=True),
                  nullable=False),
        sa.Column('proof_sha256', sa.Text(), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "receipt_nonce ~ '^[0-9a-f]{64}$' AND "
            "reclaim_fleet_bundle_sha256 ~ '^[0-9a-f]{64}$' AND "
            "reclaim_provider_inventory_sha256 ~ '^[0-9a-f]{64}$' AND "
            "proof_sha256 ~ '^[0-9a-f]{64}$'",
            name='serve054_reclaim_proof_digest_ck'),
        sa.CheckConstraint(
            'reconciliation_gate_generation > 0 AND '
            'proof_schema_version > 0',
            name='serve054_reclaim_proof_positive_ck'),
        sa.CheckConstraint(
            'octet_length(reclaim_policy_revision) BETWEEN 1 AND 1024 AND '
            'octet_length(kubernetes_context) BETWEEN 1 AND 1024',
            name='serve054_reclaim_proof_text_ck'),
        sa.CheckConstraint(
            "jsonb_typeof(proof_payload) = 'object' AND "
            'octet_length(proof_payload::text) <= 65536',
            name='serve054_reclaim_proof_payload_ck'),
        sa.UniqueConstraint('reconciliation_gate_generation',
                            'reclaim_fleet_bundle_sha256',
                            'reclaim_policy_revision',
                            'reclaim_provider_inventory_sha256',
                            'kubernetes_context',
                            name='serve054_reclaim_proof_authority_uq'),
    )


def downgrade() -> None:
    """Preserve provider-proof evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve054 is forward-only; active launch tickets may reference its '
        'provider-proof receipts.')
