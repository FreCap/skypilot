"""Persist durable daily SkyServe request activity.

Revision ID: 031
Revises: 030
Create Date: 2026-07-29

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '031'
down_revision: str | Sequence[str] | None = '030'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the PostgreSQL-only daily service request rollup."""
    if (op.get_bind().dialect.name
            != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        return
    if sa.inspect(op.get_bind()).has_table('serve_request_activity_daily'):
        # downgrade() intentionally retains durable request history. A later
        # forward deployment must adopt that table without recreating it.
        return
    op.create_table(
        'serve_request_activity_daily',
        sa.Column('day_start', sa.DateTime(timezone=True), nullable=False),
        sa.Column('service_name', sa.Text(), nullable=False),
        sa.Column('service_hash', sa.Text(), nullable=False),
        sa.Column('first_bucket_start',
                  sa.DateTime(timezone=True),
                  nullable=False),
        sa.Column('last_bucket_start',
                  sa.DateTime(timezone=True),
                  nullable=False),
        sa.Column('request_count', sa.BigInteger(), nullable=False),
        sa.Column('observed_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint('request_count >= 0',
                           name='serve_request_activity_daily_nonnegative'),
        sa.PrimaryKeyConstraint('day_start', 'service_name', 'service_hash'),
    )
    op.create_index('serve_request_activity_daily_day_idx',
                    'serve_request_activity_daily', ['day_start'])
    op.create_index('serve_request_activity_daily_service_day_idx',
                    'serve_request_activity_daily',
                    ['service_name', 'day_start'])


def downgrade() -> None:
    # Daily history is safe for an older binary to ignore. Keep it so a
    # rollback and subsequent forward deployment do not lose request totals.
    pass
