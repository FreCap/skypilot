"""Create the durable PostgreSQL API request store.

Revision ID: 001
Revises:
Create Date: 2026-07-30

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUESTS = 'api_requests'
_QUEUE = 'api_request_queue'
_METADATA = 'api_request_store_metadata'


def _require_postgresql() -> None:
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('The central API request store is PostgreSQL-only.')


def upgrade() -> None:
    """Create request, queue-delivery, and cutover metadata tables."""
    _require_postgresql()
    op.create_table(
        _REQUESTS,
        sqlalchemy.Column('request_id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('handler_name', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('payload_type', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('payload_format', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('payload_version', sqlalchemy.Integer,
                          nullable=False),
        sqlalchemy.Column('producer_version', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('payload_json', postgresql.JSONB, nullable=False),
        sqlalchemy.Column('execution_class', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('status', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('return_value', postgresql.JSONB),
        sqlalchemy.Column('error', postgresql.JSONB),
        sqlalchemy.Column('pid', sqlalchemy.Integer),
        sqlalchemy.Column('created_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('cluster_name', sqlalchemy.Text),
        sqlalchemy.Column('schedule_type', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('user_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('status_msg', sqlalchemy.Text),
        sqlalchemy.Column('should_retry',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()),
        sqlalchemy.Column('finished_at', sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('file_mounts_blob_id', sqlalchemy.Text),
        sqlalchemy.Column('ignore_return_value',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()),
        sqlalchemy.Column('retryable',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()),
        sqlalchemy.Column('execution_generation',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('claim_token', postgresql.UUID(as_uuid=True)),
        sqlalchemy.Column('worker_instance_id', postgresql.UUID(as_uuid=True)),
        sqlalchemy.Column('lease_expires_at',
                          sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('heartbeat_at', sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('cancel_requested_at',
                          sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('cancel_acknowledged_at',
                          sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('interrupted_reason', sqlalchemy.Text),
        sqlalchemy.Column('updated_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.CheckConstraint(
            "execution_class IN ('normal', 'controller')",
            name='ck_api_requests_execution_class'),
        sqlalchemy.CheckConstraint("schedule_type IN ('long', 'short')",
                                   name='ck_api_requests_schedule_type'),
        sqlalchemy.CheckConstraint(
            "status IN ('PENDING', 'WAITING', 'RUNNING', 'SUCCEEDED', "
            "'FAILED', 'CANCELLED')",
            name='ck_api_requests_status'),
        sqlalchemy.CheckConstraint(
            "(claim_token IS NULL AND worker_instance_id IS NULL "
            "AND lease_expires_at IS NULL) OR "
            "(claim_token IS NOT NULL AND worker_instance_id IS NOT NULL "
            "AND lease_expires_at IS NOT NULL)",
            name='ck_api_requests_claim'),
    )
    op.create_index('ix_api_requests_active_status_name',
                    _REQUESTS, ['status', 'name'],
                    postgresql_where=sqlalchemy.text(
                        "status IN ('PENDING', 'WAITING', 'RUNNING')"))
    op.create_index('ix_api_requests_active_cluster',
                    _REQUESTS, ['cluster_name'],
                    postgresql_where=sqlalchemy.text(
                        "status IN ('PENDING', 'WAITING', 'RUNNING')"))
    op.create_index('ix_api_requests_created_at', _REQUESTS, ['created_at'])
    op.create_index('ix_api_requests_finished_at',
                    _REQUESTS, ['finished_at'],
                    postgresql_where=sqlalchemy.text(
                        "status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')"))
    op.create_index(
        'ix_api_requests_worker_lease',
        _REQUESTS, ['worker_instance_id', 'lease_expires_at'],
        postgresql_where=sqlalchemy.text('worker_instance_id IS NOT NULL'))

    op.create_table(
        _QUEUE,
        sqlalchemy.Column('request_id',
                          sqlalchemy.Text,
                          sqlalchemy.ForeignKey(f'{_REQUESTS}.request_id',
                                                ondelete='CASCADE'),
                          primary_key=True),
        sqlalchemy.Column('schedule_type', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('priority',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('available_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('enqueued_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('sequence',
                          sqlalchemy.BigInteger,
                          sqlalchemy.Identity(),
                          nullable=False,
                          unique=True),
        sqlalchemy.Column('ignore_return_value',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()),
        sqlalchemy.Column('retryable',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()),
        sqlalchemy.Column('precondition_type', sqlalchemy.Text),
        sqlalchemy.Column('precondition_payload', postgresql.JSONB),
        sqlalchemy.Column('precondition_deadline',
                          sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('precondition_attempts',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('delivery_state',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='queued'),
        sqlalchemy.Column('claim_generation', sqlalchemy.BigInteger),
        sqlalchemy.Column('updated_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.CheckConstraint("schedule_type IN ('long', 'short')",
                                   name='ck_api_request_queue_schedule_type'),
        sqlalchemy.CheckConstraint("delivery_state IN ('queued', 'claimed')",
                                   name='ck_api_request_queue_delivery_state'),
        sqlalchemy.CheckConstraint(
            '(delivery_state = \'queued\' AND claim_generation IS NULL) OR '
            '(delivery_state = \'claimed\' AND claim_generation IS NOT NULL)',
            name='ck_api_request_queue_claim'),
        sqlalchemy.CheckConstraint(
            '(precondition_type IS NULL AND precondition_payload IS NULL '
            'AND precondition_deadline IS NULL) OR '
            '(precondition_type IS NOT NULL AND '
            'precondition_payload IS NOT NULL)',
            name='ck_api_request_queue_precondition'),
    )
    op.create_index('ix_api_request_queue_claim_order', _QUEUE, [
        'schedule_type', 'delivery_state', 'available_at', 'priority',
        'sequence'
    ])

    op.create_table(
        _METADATA,
        sqlalchemy.Column('key', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('value', postgresql.JSONB, nullable=False),
        sqlalchemy.Column('updated_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
    )


def downgrade() -> None:
    """Drop the request schema only when it contains no durable requests."""
    _require_postgresql()
    bind = op.get_bind()
    count = int(
        bind.execute(
            sqlalchemy.text(f'SELECT COUNT(*) FROM {_REQUESTS}')).scalar_one())
    if count:
        raise RuntimeError(
            'Cannot downgrade the API request schema while durable requests '
            'exist.')
    op.drop_table(_METADATA)
    op.drop_table(_QUEUE)
    op.drop_table(_REQUESTS)
