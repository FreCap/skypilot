"""Add the low-cardinality operator notification inbox.

Revision ID: 022
Revises: 021
Create Date: 2026-07-15

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

from sky.global_user_state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '022'
down_revision: str | Sequence[str] | None = '021'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Create notification, cursor, and sequence tables."""
    with op.get_context().autocommit_block():
        db_utils.add_all_tables_to_db_sqlalchemy(Base.metadata, op.get_bind())


def downgrade():
    """No-op for backward compatibility."""
    pass
