"""Add durable SkyServe version quarantine metadata.

Revision ID: 024
Revises: 023
Create Date: 2026-07-23

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

revision: str = '024'
down_revision: str | Sequence[str] | None = '023'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add nullable quarantine metadata without rewriting version history."""
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('version_specs', 'quarantined_at',
                                             sa.Float())
        db_utils.add_column_to_table_alembic('version_specs',
                                             'quarantine_reason', sa.Text())


def downgrade():
    pass
