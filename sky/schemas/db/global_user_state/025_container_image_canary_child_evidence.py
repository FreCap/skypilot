"""Persist restart-safe managed-image canary child evidence.

Revision ID: 025
Revises: 024
Create Date: 2026-07-24

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '025'
down_revision: str | Sequence[str] | None = '024'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add nullable child evidence without changing old-worker parsing."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    op.add_column(
        'container_image_operations',
        sqlalchemy.Column('canary_child_evidence_json', sqlalchemy.Text),
    )


def downgrade():
    """No-op for backward compatibility."""
    pass
