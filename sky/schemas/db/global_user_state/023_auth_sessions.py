"""Store browser-login sessions in shared API-server state.

Revision ID: 023
Revises: 022
Create Date: 2026-07-21

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

from sky.global_user_state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '023'
down_revision: str | Sequence[str] | None = '022'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Create the shared auth-session table."""
    with op.get_context().autocommit_block():
        db_utils.add_all_tables_to_db_sqlalchemy(Base.metadata, op.get_bind())


def downgrade():
    """No-op for backward compatibility."""
    pass
