"""Advertise ordered SkyServe capacity-admission capability.

Revision ID: 012
Revises: 011
Create Date: 2026-08-16

The migration is additive.  Older processes retain the false default, so a
mixed API fleet cannot promote a service to durable demand authority.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '012'
down_revision: str | Sequence[str] | None = '011'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INSTANCES = 'api_server_instances'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Ordered capacity admission requires PostgreSQL API state.')


def upgrade() -> None:
    """Add a closed API-fleet capability tuple."""
    _require_postgresql()
    op.add_column(
        _INSTANCES,
        sa.Column('ordered_capacity_admission_capable',
                  sa.Boolean(),
                  nullable=False,
                  server_default=sa.false()))
    op.add_column(
        _INSTANCES,
        sa.Column('ordered_capacity_admission_protocol_version', sa.Integer()))
    op.add_column(
        _INSTANCES,
        sa.Column('ordered_capacity_admission_cohort_epoch', sa.BigInteger()))
    op.create_check_constraint(
        'ck_api_server_instances_ordered_capacity_complete', _INSTANCES,
        '((NOT ordered_capacity_admission_capable AND '
        'ordered_capacity_admission_protocol_version IS NULL AND '
        'ordered_capacity_admission_cohort_epoch IS NULL) OR '
        '(ordered_capacity_admission_capable AND '
        'ordered_capacity_admission_protocol_version IS NOT NULL AND '
        'ordered_capacity_admission_cohort_epoch IS NOT NULL AND '
        'ordered_capacity_admission_protocol_version = 1 AND '
        'ordered_capacity_admission_cohort_epoch = 1))')


def downgrade() -> None:
    """Retain capability evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API012 is forward-only. Demote every DURABLE_FEED service before '
        'rolling application code back.')
