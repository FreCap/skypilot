"""Add PostgreSQL aggregate SkyServe request activity history.

Revision ID: 013
Revises: 012
Create Date: 2026-07-16

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

from sky.serve import serve_history
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '013'
down_revision: str | Sequence[str] | None = '012'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Create central request history only on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    serve_history.serve_request_activity_history_table.create(bind,
                                                              checkfirst=True)


def downgrade():
    pass
