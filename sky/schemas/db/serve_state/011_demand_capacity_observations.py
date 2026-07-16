"""Add the shared demand-capacity observation table.

Revision ID: 011
Revises: 010
Create Date: 2026-07-15

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.serve.serve_state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '011'
down_revision: str | Sequence[str] | None = '010'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Create demand observations and the logical-semantics fence."""
    with op.get_context().autocommit_block():
        db_utils.add_all_tables_to_db_sqlalchemy(Base.metadata, op.get_bind())
        db_utils.add_column_to_table_alembic('services',
                                             'logical_replica_semantics',
                                             sa.Integer(),
                                             server_default='0')


def downgrade():
    pass
