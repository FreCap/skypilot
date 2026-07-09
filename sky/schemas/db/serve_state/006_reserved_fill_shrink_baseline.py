"""Add shrink_baseline column to reserved_fill_rounds.

Revision ID: 006
Revises: 005
Create Date: 2026-07-09

"""
# pylint: disable=invalid-name
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '006'
down_revision: Union[str, Sequence[str], None] = '005'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    """Add the pending-shrink baseline to round rows.

    A conserved-holdings shrink must persist across two consecutive rounds
    before it bypasses grant damping (a drain completing between the
    cluster query and the row scan fakes a one-round shrink); the pending
    candidate's pre-shrink baseline is persisted on the round row so the
    confirmation survives broker writer rotation. DBs created at revision
    005 after the column joined the table metadata already have it;
    add_column_to_table_alembic is a no-op in that case.
    """
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('reserved_fill_rounds',
                                             'shrink_baseline',
                                             sa.Integer(),
                                             server_default=None)


def downgrade():
    pass
