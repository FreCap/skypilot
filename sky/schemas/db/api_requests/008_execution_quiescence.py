"""Add exact-generation request execution quiescence evidence.

Revision ID: 008
Revises: 007
Create Date: 2026-08-04

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '008'
down_revision: str | Sequence[str] | None = '007'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUESTS = 'api_requests'
_QUIESCENCE_CONSTRAINT = 'ck_api_requests_execution_quiescence'
_QUIESCENCE_INDEX = 'ix_api_requests_quiescence_cluster_status'


def _require_postgresql() -> None:
    if op.get_bind(
    ).dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('The central API request store is PostgreSQL-only.')


def upgrade() -> None:
    """Add nullable, generation-bound execution completion evidence."""
    _require_postgresql()
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('execution_quiescence_required',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()))
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('execution_quiesced_generation',
                          sqlalchemy.BigInteger,
                          nullable=True))
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('execution_quiesced_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=True))
    op.create_check_constraint(
        _QUIESCENCE_CONSTRAINT, _REQUESTS,
        '(execution_quiesced_generation IS NULL AND '
        'execution_quiesced_at IS NULL) OR '
        '(execution_quiesced_generation >= 0 AND '
        'execution_quiesced_at IS NOT NULL)')
    op.create_index(
        _QUIESCENCE_INDEX,
        _REQUESTS, ['cluster_name', 'status'],
        postgresql_where=sqlalchemy.text(
            'execution_quiescence_required AND '
            '(execution_quiesced_generation IS DISTINCT FROM '
            'execution_generation OR execution_quiesced_at IS NULL)'))


def downgrade() -> None:
    """Remove additive quiescence evidence for an explicit schema rollback."""
    _require_postgresql()
    op.drop_index(_QUIESCENCE_INDEX, table_name=_REQUESTS)
    op.drop_constraint(_QUIESCENCE_CONSTRAINT, _REQUESTS, type_='check')
    op.drop_column(_REQUESTS, 'execution_quiesced_at')
    op.drop_column(_REQUESTS, 'execution_quiesced_generation')
    op.drop_column(_REQUESTS, 'execution_quiescence_required')
