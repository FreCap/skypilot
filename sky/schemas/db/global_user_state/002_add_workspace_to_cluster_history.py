"""add workspace column to cluster_history table

Revision ID: 002
Revises: 001
Create Date: 2025-08-06

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: str | Sequence[str] | None = '001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('cluster_history',
                                             'workspace',
                                             sa.Text(),
                                             server_default=None)
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
