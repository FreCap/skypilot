"""Add durable controller leadership and action fencing.

Revision ID: 003
Revises: 002
Create Date: 2026-07-30

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '003'
down_revision: str | Sequence[str] | None = '002'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUESTS = 'api_requests'
_LEADERSHIP = 'api_controller_leadership'
_ACTION_RESERVATIONS = 'api_controller_action_reservations'


def _require_postgresql() -> None:
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('The central API request store is PostgreSQL-only.')


def upgrade() -> None:
    """Create the controller generation and external-action fences."""
    _require_postgresql()
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('controller_generation', sqlalchemy.BigInteger))
    op.create_check_constraint(
        'ck_api_requests_controller_generation', _REQUESTS,
        'controller_generation IS NULL OR '
        'controller_generation > 0')
    op.create_index(
        'ix_api_requests_controller_generation',
        _REQUESTS, ['controller_generation'],
        postgresql_where=sqlalchemy.text('controller_generation IS NOT NULL'))

    op.create_table(
        _LEADERSHIP,
        sqlalchemy.Column('leadership_key', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('generation', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('instance_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=False),
        sqlalchemy.Column('lock_backend_pid',
                          sqlalchemy.Integer,
                          nullable=False),
        sqlalchemy.Column('generation_lock_key',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('acquired_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('heartbeat_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('released_at', sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.CheckConstraint(
            'generation > 0', name='ck_api_controller_leadership_generation'),
    )
    op.create_index('ix_api_controller_leadership_heartbeat', _LEADERSHIP,
                    ['heartbeat_at'])

    op.create_table(
        _ACTION_RESERVATIONS,
        sqlalchemy.Column('logical_action_id',
                          sqlalchemy.Text,
                          primary_key=True),
        sqlalchemy.Column('resource_identity', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('action_type', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('controller_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('controller_instance_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=False),
        sqlalchemy.Column('provider_operation_id', sqlalchemy.Text),
        sqlalchemy.Column('created_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('updated_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('reconciliation_at',
                          sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.CheckConstraint(
            "state IN ('reserved', 'running', 'completed', 'failed', "
            "'ambiguous')",
            name='ck_api_controller_action_reservations_state'),
        sqlalchemy.CheckConstraint(
            'controller_generation > 0',
            name='ck_api_controller_action_reservations_generation'),
    )
    op.create_index('ix_api_controller_action_reservations_owner',
                    _ACTION_RESERVATIONS,
                    ['controller_instance_id', 'controller_generation'])
    op.create_index('ix_api_controller_action_reservations_state',
                    _ACTION_RESERVATIONS, ['state'])


def downgrade() -> None:
    """Drop controller fencing while preserving durable request history."""
    _require_postgresql()
    op.drop_table(_ACTION_RESERVATIONS)
    op.drop_table(_LEADERSHIP)
    op.drop_index('ix_api_requests_controller_generation', table_name=_REQUESTS)
    op.drop_constraint('ck_api_requests_controller_generation',
                       _REQUESTS,
                       type_='check')
    op.drop_column(_REQUESTS, 'controller_generation')
