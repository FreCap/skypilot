"""Add PostgreSQL aggregate SkyServe replica status history.

Revision ID: 012
Revises: 011
Create Date: 2026-07-16

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

from sky.serve import serve_history
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '012'
down_revision: str | Sequence[str] | None = '011'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Create central history only on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    serve_history.serve_replica_status_history_table.create(bind,
                                                            checkfirst=True)


def downgrade():
    pass
