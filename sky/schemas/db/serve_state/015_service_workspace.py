"""Persist the user workspace for service replica launches.

Revision ID: 015
Revises: 014
Create Date: 2026-07-14

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '015'
down_revision: str | Sequence[str] | None = '014'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add the workspace snapshot used by every controller recovery."""
    with op.get_context().autocommit_block():
        # A pre-015 row has no trustworthy workspace to backfill. Keep it NULL
        # so update and recovery paths can identify it and fail closed instead
        # of silently moving replicas into the default workspace.
        db_utils.add_column_to_table_alembic('services', 'workspace', sa.Text())


def downgrade():
    pass
