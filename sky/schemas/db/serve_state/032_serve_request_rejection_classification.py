"""Add exact SkyServe request rejection classification counters.

Revision ID: 032
Revises: 031
Create Date: 2026-08-01

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '032'
down_revision: str | Sequence[str] | None = '031'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RAW_TABLE = 'serve_request_activity_history'
_DAILY_TABLE = 'serve_request_activity_daily'
_RAW_PAIR_CONSTRAINT = 'serve_request_activity_history_classified_pair'
_DAILY_PAIR_CONSTRAINT = 'serve_request_activity_daily_classified_pair'


def _column_names(bind: sa.engine.Connection, table_name: str) -> set[str]:
    return {
        str(column['name'])
        for column in sa.inspect(bind).get_columns(table_name)
    }


def _constraint_names(bind: sa.engine.Connection,
                      table_name: str) -> set[str | None]:
    return {
        constraint['name']
        for constraint in sa.inspect(bind).get_check_constraints(table_name)
    }


def upgrade() -> None:
    """Add paired raw and durable classification state on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return

    raw_columns = _column_names(bind, _RAW_TABLE)
    if 'classified_request_count' not in raw_columns:
        op.add_column(
            _RAW_TABLE,
            sa.Column('classified_request_count', sa.Integer(), nullable=True))
    if 'counted_rejected_count' not in raw_columns:
        op.add_column(
            _RAW_TABLE,
            sa.Column('counted_rejected_count', sa.Integer(), nullable=True))
    if _RAW_PAIR_CONSTRAINT not in _constraint_names(bind, _RAW_TABLE):
        op.create_check_constraint(
            _RAW_PAIR_CONSTRAINT, _RAW_TABLE,
            '(classified_request_count IS NULL AND '
            'counted_rejected_count IS NULL) OR '
            '(classified_request_count IS NOT NULL AND '
            'counted_rejected_count IS NOT NULL AND '
            'classified_request_count >= 0 AND '
            'counted_rejected_count >= 0 AND '
            'counted_rejected_count <= classified_request_count)')

    daily_columns = _column_names(bind, _DAILY_TABLE)
    if 'classified_request_count' not in daily_columns:
        op.add_column(
            _DAILY_TABLE,
            sa.Column('classified_request_count',
                      sa.BigInteger(),
                      nullable=True))
    if 'counted_rejected_count' not in daily_columns:
        op.add_column(
            _DAILY_TABLE,
            sa.Column('counted_rejected_count', sa.BigInteger(), nullable=True))
    if 'classified_first_bucket_start' not in daily_columns:
        op.add_column(
            _DAILY_TABLE,
            sa.Column('classified_first_bucket_start',
                      sa.DateTime(timezone=True),
                      nullable=True))
    if 'classified_last_bucket_start' not in daily_columns:
        op.add_column(
            _DAILY_TABLE,
            sa.Column('classified_last_bucket_start',
                      sa.DateTime(timezone=True),
                      nullable=True))
    if 'classification_incomplete' not in daily_columns:
        op.add_column(
            _DAILY_TABLE,
            sa.Column('classification_incomplete',
                      sa.Boolean(),
                      nullable=False,
                      server_default=sa.false()))
    # Existing durable attempt rows predate exact classification. Repeat this
    # on re-upgrade as well: an older binary may have inserted more attempt-only
    # rows while the additive columns were retained across rollback.
    op.execute(
        sa.text('UPDATE serve_request_activity_daily '
                'SET classification_incomplete = true '
                'WHERE request_count > 0 AND '
                '(classified_request_count IS NULL OR '
                'counted_rejected_count IS NULL)'))
    if _DAILY_PAIR_CONSTRAINT not in _constraint_names(bind, _DAILY_TABLE):
        op.create_check_constraint(
            _DAILY_PAIR_CONSTRAINT, _DAILY_TABLE,
            '(classified_request_count IS NULL AND '
            'counted_rejected_count IS NULL AND '
            'classified_first_bucket_start IS NULL AND '
            'classified_last_bucket_start IS NULL) OR '
            '(classified_request_count IS NOT NULL AND '
            'counted_rejected_count IS NOT NULL AND '
            'classified_request_count >= 0 AND '
            'counted_rejected_count >= 0 AND '
            'counted_rejected_count <= classified_request_count AND '
            'classified_first_bucket_start IS NOT NULL AND '
            'classified_last_bucket_start IS NOT NULL AND '
            'classified_first_bucket_start <= '
            'classified_last_bucket_start)')


def downgrade() -> None:
    # The additive columns contain durable telemetry and are safe for an older
    # binary to ignore. Preserve them so a later forward deployment resumes
    # without losing classification history or completeness latches.
    pass
