"""Reserved-fill broker tables (claims / rounds / lease).

Revision ID: 004
Revises: 003
Create Date: 2026-07-08

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

from sky.serve.serve_state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '004'
down_revision: str | Sequence[str] | None = '003'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Create the reserved-fill broker tables.

    add_all_tables_to_db_sqlalchemy creates only the tables missing from the
    bound database (checkfirst), so this picks up exactly the three broker
    tables newly declared on Base.metadata (reserved_fill_claims,
    reserved_fill_rounds, reserved_fill_lease).
    """
    with op.get_context().autocommit_block():
        db_utils.add_all_tables_to_db_sqlalchemy(Base.metadata, op.get_bind())


def downgrade():
    pass
