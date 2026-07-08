"""Add phantom_streak column to reserved_fill_rounds.

Revision ID: 005
Revises: 004
Create Date: 2026-07-08

"""
# pylint: disable=invalid-name
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '005'
down_revision: Union[str, Sequence[str], None] = '004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    """Add the consecutive-phantom-observation counter to round rows.

    Persisted on the round row so the phantom-pool claim rejection gate
    (N consecutive phantom observations required) survives broker writer
    rotation. DBs created at revision 004 after the column joined the
    table metadata already have it; add_column_to_table_alembic is a
    no-op in that case.
    """
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('reserved_fill_rounds',
                                             'phantom_streak',
                                             sa.Integer(),
                                             server_default='0')


def downgrade():
    pass
