"""Add attempt fencing and retry leases to batch state.

Revision ID: 022
Revises: 021
Create Date: 2026-07-10

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '022'
down_revision: str | Sequence[str] | None = '021'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add fencing token, lease expiry, and durable retry timestamp."""
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('batch_state',
                                             'attempt_id',
                                             sa.Integer(),
                                             server_default='0')
        db_utils.add_column_to_table_alembic('batch_state',
                                             'lease_expires_at',
                                             sa.Float(),
                                             server_default=None)
        db_utils.add_column_to_table_alembic('batch_state',
                                             'next_retry_at',
                                             sa.Float(),
                                             server_default=None)


def downgrade():
    """No-op for backward compatibility."""
    pass
