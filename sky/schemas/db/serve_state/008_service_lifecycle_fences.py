"""Add durable service lifecycle fencing and resource scopes.

Revision ID: 008
Revises: 007
Create Date: 2026-07-10

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.serve.serve_state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '008'
down_revision: str | Sequence[str] | None = '007'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Create the durable name fence and add incarnation metadata."""
    with op.get_context().autocommit_block():
        # New databases create every current metadata table at revision 001;
        # checkfirst makes this a no-op there and creates only the fence table
        # on upgraded databases.
        db_utils.add_all_tables_to_db_sqlalchemy(Base.metadata, op.get_bind())
        db_utils.add_column_to_table_alembic('services', 'lifecycle_epoch',
                                             sa.Integer())
        db_utils.add_column_to_table_alembic('services', 'resource_scope',
                                             sa.Text())


def downgrade():
    pass
