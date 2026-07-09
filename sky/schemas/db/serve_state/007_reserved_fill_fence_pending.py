"""Add fence_pending column to reserved_fill_rounds.

Revision ID: 007
Revises: 006
Create Date: 2026-07-09

"""
# pylint: disable=invalid-name
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '007'
down_revision: Union[str, Sequence[str], None] = '006'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    """Add the dead-gap fence marker to round rows.

    Set (for every pool) atomically with a post-expiry lease-token
    acquisition and cleared only by a successful publish, which is forced
    to bump the pool's fencing epoch while the marker is set: a
    post-expiry writer that acquired its token (committing a fresh
    expires_at) and died before publishing must not leave the next writer
    republishing the old epoch on an unexpired lease. DBs created at
    revision 006 after the column joined the table metadata already have
    it; add_column_to_table_alembic is a no-op in that case.
    """
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('reserved_fill_rounds',
                                             'fence_pending',
                                             sa.Integer(),
                                             server_default='0')


def downgrade():
    pass
