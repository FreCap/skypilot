"""Extend ordered capacity admission to pending zero-cost intents.

Revision ID: 015
Revises: 014
Create Date: 2026-08-17

Protocol 2 proves that every live request participant debits an unmaterialized
reserved-fill grant before admitting paid demand capacity.  Protocol-1 rows
remain valid during rollout but cannot pass the exact fleet promotion gate.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

revision: str = '015'
down_revision: str | Sequence[str] | None = '014'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INSTANCES = 'api_server_instances'
_CONSTRAINT = 'ck_api_server_instances_ordered_capacity_complete'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Ordered capacity admission requires PostgreSQL API state.')


def upgrade() -> None:
    """Permit the exact protocol-2 capability cohort."""
    _require_postgresql()
    op.drop_constraint(_CONSTRAINT, _INSTANCES, type_='check')
    op.create_check_constraint(
        _CONSTRAINT, _INSTANCES, '((NOT ordered_capacity_admission_capable AND '
        'ordered_capacity_admission_protocol_version IS NULL AND '
        'ordered_capacity_admission_cohort_epoch IS NULL) OR '
        '(ordered_capacity_admission_capable AND '
        'ordered_capacity_admission_protocol_version IS NOT NULL AND '
        'ordered_capacity_admission_cohort_epoch IS NOT NULL AND '
        'ordered_capacity_admission_protocol_version IN (1, 2) AND '
        'ordered_capacity_admission_cohort_epoch = '
        'ordered_capacity_admission_protocol_version))')


def downgrade() -> None:
    """Retain protocol-2 capability evidence across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API015 is forward-only. Roll application code forward while any '
        'service can use durable zero-cost actuation.')
