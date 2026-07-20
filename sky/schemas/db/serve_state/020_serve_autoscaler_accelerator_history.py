"""Add exact-accelerator maps to PostgreSQL SkyServe history.

Revision ID: 020
Revises: 019
Create Date: 2026-07-20

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

revision: str = '020'
down_revision: str | Sequence[str] | None = '019'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'serve_autoscaler_history'
_BREAKDOWN = 'accelerator_breakdown'
_OBSERVED_AT = 'accelerator_breakdown_observed_at'


def upgrade():
    """Add bounded exact-card history only on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    columns = {
        column['name']
        for column in sqlalchemy.inspect(bind).get_columns(_TABLE)
    }
    if _BREAKDOWN not in columns:
        op.add_column(
            _TABLE,
            sqlalchemy.Column(_BREAKDOWN,
                              postgresql.JSONB,
                              nullable=False,
                              server_default=sqlalchemy.text("'{}'::jsonb")))
    if _OBSERVED_AT not in columns:
        op.add_column(
            _TABLE,
            sqlalchemy.Column(_OBSERVED_AT,
                              sqlalchemy.DateTime(timezone=True),
                              nullable=True))


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    columns = {
        column['name']
        for column in sqlalchemy.inspect(bind).get_columns(_TABLE)
    }
    if _OBSERVED_AT in columns:
        op.drop_column(_TABLE, _OBSERVED_AT)
    if _BREAKDOWN in columns:
        op.drop_column(_TABLE, _BREAKDOWN)
