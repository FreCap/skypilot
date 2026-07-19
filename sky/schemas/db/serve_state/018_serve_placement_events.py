"""Add PostgreSQL SkyServe placement decision history.

Revision ID: 018
Revises: 017
Create Date: 2026-07-19

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

from sky.serve import placement_history
from sky.utils.db import db_utils

revision: str = '018'
down_revision: str | Sequence[str] | None = '017'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Create central placement history only on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    placement_history.serve_placement_events_table.create(bind, checkfirst=True)


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    placement_history.serve_placement_events_table.drop(bind, checkfirst=True)
