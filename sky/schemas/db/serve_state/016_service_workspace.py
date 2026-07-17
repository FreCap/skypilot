"""Persist the user workspace for service replica launches.

Revision ID: 016
Revises: 015
Create Date: 2026-07-17

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '016'
down_revision: str | Sequence[str] | None = '015'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Persist workspace and reconcile the two former revision 015 layouts."""
    with op.get_context().autocommit_block():
        # Before the branches were merged, the feature preview used revision
        # 015 for workspace while improvements used it for version provenance.
        # A database stamped 015 may therefore have either schema. Re-applying
        # all nullable columns makes 016 converge both layouts safely.
        db_utils.add_column_to_table_alembic('services', 'workspace', sa.Text())
        db_utils.add_column_to_table_alembic('version_specs', 'created_at',
                                             sa.Float())
        db_utils.add_column_to_table_alembic('version_specs', 'created_by',
                                             sa.Text())


def downgrade():
    pass
