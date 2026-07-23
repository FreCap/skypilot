"""Add PostgreSQL SkyServe prediction-time history.

Revision ID: 023
Revises: 022
Create Date: 2026-07-22

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

from sky.serve import serve_history
from sky.utils.db import db_utils

revision: str = '023'
down_revision: str | Sequence[str] | None = '022'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add aggregate prediction histograms only on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    serve_history.serve_prediction_time_history_table.create(bind,
                                                             checkfirst=True)


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    serve_history.serve_prediction_time_history_table.drop(bind,
                                                           checkfirst=True)
