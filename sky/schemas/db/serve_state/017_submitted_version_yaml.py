"""Retain the user-submitted YAML for each SkyServe version.

Revision ID: 017
Revises: 016
Create Date: 2026-07-17

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

revision: str = '017'
down_revision: str | Sequence[str] | None = '016'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add the optional submitted YAML alongside the execution YAML."""
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('version_specs',
                                             'submitted_yaml_content',
                                             sa.Text())


def downgrade():
    op.drop_column('version_specs', 'submitted_yaml_content')
