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
_INSTANCES = 'api_server_instances'
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
    # These defaults keep API007 writers insert-compatible during the rolling
    # migration.  Only API008 writers overwrite them with resolved runtime
    # capability evidence, so legacy or plugin-overridden processes fail the
    # protocol-v2 activation fence closed.
    op.add_column(
        _INSTANCES,
        sqlalchemy.Column('request_storage_backend',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='unknown'))
    op.add_column(
        _INSTANCES,
        sqlalchemy.Column('request_queue_backend',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='unknown'))
    op.add_column(
        _INSTANCES,
        sqlalchemy.Column('execution_quiescence_capable',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()))


def downgrade() -> None:
    """Retain execution-quiescence evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API request schema 008 is additive and cannot be downgraded. Roll '
        'back the application against the retained schema instead.')
