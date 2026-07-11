"""Add durable scoped ephemeral-storage cleanup intents.

Revision ID: 009
Revises: 008
Create Date: 2026-07-10

"""
# pylint: disable=invalid-name
from typing import Sequence, Union

from alembic import op

from sky.serve.serve_state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '009'
down_revision: Union[str, Sequence[str], None] = '008'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    """Create the pre-upload cleanup-inventory table."""
    with op.get_context().autocommit_block():
        db_utils.add_all_tables_to_db_sqlalchemy(Base.metadata, op.get_bind())


def downgrade():
    pass
