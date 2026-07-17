"""Add SkyServe version commit provenance.

Revision ID: 015
Revises: 014
Create Date: 2026-07-17

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '015'
down_revision: str | Sequence[str] | None = '014'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Record when and by whom committed versions were created."""
    db_utils.add_column_to_table_alembic('version_specs', 'created_at',
                                         sa.Float())
    db_utils.add_column_to_table_alembic('version_specs', 'created_by',
                                         sa.Text())


def downgrade():
    pass
