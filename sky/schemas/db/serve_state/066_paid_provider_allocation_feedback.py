"""Add durable paid provider-allocation feedback markers.

Revision ID: 066
Revises: 065
Create Date: 2026-09-01

Serve066 adds the nullable, one-shot receipt slot used to record that an exact
paid provider allocation has materialized.  The migration installs only the
durable schema boundary; writer and launch-reducer semantics are introduced by
the corresponding runtime change.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '066'
down_revision: str | Sequence[str] | None = '065'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CLAIMS = 'paid_capacity_claims'
_RECORDED_AT = 'provider_allocation_recorded_at'
_RECEIPT_SHA256 = 'provider_allocation_receipt_sha256'
_COMPLETE_CONSTRAINT = ('serve066_paid_claim_provider_allocation_complete_ck')
_DIGEST_CONSTRAINT = 'serve066_paid_claim_provider_allocation_digest_ck'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Paid provider-allocation feedback is PostgreSQL-only.')


def upgrade() -> None:
    """Install the durable provider-allocation receipt slot."""
    _require_postgresql()
    op.add_column(
        _CLAIMS,
        sa.Column(_RECORDED_AT, sa.DateTime(timezone=True), nullable=True))
    op.add_column(_CLAIMS, sa.Column(_RECEIPT_SHA256, sa.Text(), nullable=True))
    op.create_check_constraint(
        _COMPLETE_CONSTRAINT, _CLAIMS,
        f'num_nonnulls({_RECORDED_AT}, {_RECEIPT_SHA256}) IN (0, 2)')
    op.create_check_constraint(
        _DIGEST_CONSTRAINT, _CLAIMS, f"({_RECEIPT_SHA256} IS NULL OR "
        f"{_RECEIPT_SHA256} ~ '^[0-9a-f]{{64}}$')")


def downgrade() -> None:
    """Preserve materialization receipts across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve066 is forward-only; paid provider allocations may already '
        'have durable feedback receipts.')
