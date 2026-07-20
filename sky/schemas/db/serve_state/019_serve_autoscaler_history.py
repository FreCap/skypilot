"""Add PostgreSQL SkyServe autoscaler and rejection history.

Revision ID: 019
Revises: 018
Create Date: 2026-07-19

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

from sky.serve import serve_history
from sky.utils.db import db_utils

revision: str = '019'
down_revision: str | Sequence[str] | None = '018'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUEST_TABLE = 'serve_request_activity_history'
_REJECTED_CONSTRAINT = 'serve_request_activity_history_rejected_nonnegative'
_REJECTION_AVAILABLE_COLUMN = 'rejection_count_available'


def upgrade():
    """Add central demand history only on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return

    # Migrations 012/013 create tables from current module metadata. On a
    # fresh database they may therefore have already created this new column
    # and constraint before revision 019 runs.
    inspector = sqlalchemy.inspect(bind)
    columns = {
        column['name'] for column in inspector.get_columns(_REQUEST_TABLE)
    }
    if 'rejected_count' not in columns:
        op.add_column(
            _REQUEST_TABLE,
            sqlalchemy.Column('rejected_count',
                              sqlalchemy.Integer,
                              nullable=False,
                              server_default=sqlalchemy.text('0')))
    if _REJECTION_AVAILABLE_COLUMN not in columns:
        op.add_column(
            _REQUEST_TABLE,
            sqlalchemy.Column(_REJECTION_AVAILABLE_COLUMN,
                              sqlalchemy.Boolean,
                              nullable=False,
                              server_default=sqlalchemy.false()))
    inspector = sqlalchemy.inspect(bind)
    constraints = {
        constraint['name']
        for constraint in inspector.get_check_constraints(_REQUEST_TABLE)
    }
    if _REJECTED_CONSTRAINT not in constraints:
        op.create_check_constraint(_REJECTED_CONSTRAINT, _REQUEST_TABLE,
                                   'rejected_count >= 0')
    serve_history.serve_autoscaler_history_table.create(bind, checkfirst=True)


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    serve_history.serve_autoscaler_history_table.drop(bind, checkfirst=True)
    inspector = sqlalchemy.inspect(bind)
    constraints = {
        constraint['name']
        for constraint in inspector.get_check_constraints(_REQUEST_TABLE)
    }
    if _REJECTED_CONSTRAINT in constraints:
        op.drop_constraint(_REJECTED_CONSTRAINT, _REQUEST_TABLE, type_='check')
    columns = {
        column['name'] for column in inspector.get_columns(_REQUEST_TABLE)
    }
    if 'rejected_count' in columns:
        op.drop_column(_REQUEST_TABLE, 'rejected_count')
    if _REJECTION_AVAILABLE_COLUMN in columns:
        op.drop_column(_REQUEST_TABLE, _REJECTION_AVAILABLE_COLUMN)
