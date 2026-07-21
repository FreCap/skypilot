"""Add logical and reserved-fill SkyServe replica history.

Revision ID: 020
Revises: 019
Create Date: 2026-07-20

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

from sky.utils.db import db_utils

revision: str = '020'
down_revision: str | Sequence[str] | None = '019'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'serve_replica_status_history'
_COLUMNS = (
    'ready_reserved_count',
    'logical_ready_count',
    'logical_ready_reserved_count',
    'logical_provisioning_count',
    'logical_not_ready_count',
    'logical_errored_count',
    'logical_preempted_count',
    'logical_stopping_count',
    'logical_total_count',
)
_RESERVED_CONSTRAINT = 'serve_replica_status_history_reserved_ready'
_LOGICAL_CONSTRAINT = 'serve_replica_status_history_logical_counts'


def upgrade():
    """Add capacity-mode history only on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return

    # Migration 012 creates the table from current module metadata. On a fresh
    # database it may therefore already contain these columns and constraints.
    inspector = sqlalchemy.inspect(bind)
    columns = {column['name'] for column in inspector.get_columns(_TABLE)}
    for column in _COLUMNS:
        if column not in columns:
            op.add_column(
                _TABLE,
                sqlalchemy.Column(column, sqlalchemy.Integer, nullable=True))

    inspector = sqlalchemy.inspect(bind)
    constraints = {
        constraint['name']
        for constraint in inspector.get_check_constraints(_TABLE)
    }
    if _RESERVED_CONSTRAINT not in constraints:
        op.create_check_constraint(
            _RESERVED_CONSTRAINT, _TABLE, 'ready_reserved_count IS NULL OR '
            '(ready_reserved_count >= 0 AND '
            'ready_reserved_count <= ready_count)')
    if _LOGICAL_CONSTRAINT not in constraints:
        op.create_check_constraint(
            _LOGICAL_CONSTRAINT, _TABLE, '(logical_ready_count IS NULL AND '
            'logical_ready_reserved_count IS NULL AND '
            'logical_provisioning_count IS NULL AND '
            'logical_not_ready_count IS NULL AND '
            'logical_errored_count IS NULL AND '
            'logical_preempted_count IS NULL AND '
            'logical_stopping_count IS NULL AND '
            'logical_total_count IS NULL) OR '
            '(logical_ready_count >= 0 AND '
            'logical_ready_reserved_count >= 0 AND '
            'logical_ready_reserved_count <= logical_ready_count AND '
            'logical_provisioning_count >= 0 AND '
            'logical_not_ready_count >= 0 AND '
            'logical_errored_count >= 0 AND '
            'logical_preempted_count >= 0 AND '
            'logical_stopping_count >= 0 AND '
            'logical_total_count = logical_ready_count + '
            'logical_provisioning_count + logical_not_ready_count + '
            'logical_errored_count + logical_preempted_count + '
            'logical_stopping_count)')


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    inspector = sqlalchemy.inspect(bind)
    constraints = {
        constraint['name']
        for constraint in inspector.get_check_constraints(_TABLE)
    }
    if _LOGICAL_CONSTRAINT in constraints:
        op.drop_constraint(_LOGICAL_CONSTRAINT, _TABLE, type_='check')
    if _RESERVED_CONSTRAINT in constraints:
        op.drop_constraint(_RESERVED_CONSTRAINT, _TABLE, type_='check')
    columns = {column['name'] for column in inspector.get_columns(_TABLE)}
    for column in reversed(_COLUMNS):
        if column in columns:
            op.drop_column(_TABLE, column)
