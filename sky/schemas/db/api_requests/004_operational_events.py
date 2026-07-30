"""Add the actor-aware operational event plane.

Revision ID: 004
Revises: 003
Create Date: 2026-07-30

"""
# pylint: disable=invalid-name
from collections.abc import Sequence
import json
import uuid

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '004'
down_revision: str | Sequence[str] | None = '003'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUESTS = 'api_requests'
_EVENTS = 'resource_events'
_TARGETS = 'resource_event_targets'
_METADATA = 'api_request_store_metadata'
_AUTHORITY_KEY = 'operational_event_cursor_authority_v1'


def _require_postgresql() -> None:
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('The operational event plane is PostgreSQL-only.')


def upgrade() -> None:
    """Create actor-aware resource events and request emission context."""
    _require_postgresql()
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('event_context', postgresql.JSONB, nullable=True))
    op.create_table(
        _EVENTS,
        sqlalchemy.Column('event_id',
                          postgresql.UUID(as_uuid=True),
                          primary_key=True),
        sqlalchemy.Column('event_sequence',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('occurred_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('phase', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('outcome', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('cause', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('message', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('actor_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('actor_name', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('actor_type', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('source_request_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('source_execution_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.UniqueConstraint('source_request_id',
                                    'source_execution_generation',
                                    'phase',
                                    name='uq_resource_events_source_phase'),
        sqlalchemy.UniqueConstraint('event_sequence',
                                    name='uq_resource_events_sequence'),
        sqlalchemy.CheckConstraint(
            "kind IN ('cluster.launch', 'cluster.start', 'cluster.stop', "
            "'cluster.down', 'cluster.autostop')",
            name='ck_resource_events_kind'),
        sqlalchemy.CheckConstraint("phase IN ('terminal')",
                                   name='ck_resource_events_phase'),
        sqlalchemy.CheckConstraint(
            "outcome IN ('succeeded', 'failed', 'canceled')",
            name='ck_resource_events_outcome'),
        sqlalchemy.CheckConstraint(
            "cause IN ('handler_succeeded', 'handler_failed', "
            "'dispatcher_submit_failed', 'explicit_cancel', "
            "'coroutine_disconnected', 'graceful_shutdown_retry', "
            "'compatibility_restart', 'controller_leadership_lost', "
            "'execution_lease_expired', 'precondition_failed', "
            "'controller_reservation_conflict')",
            name='ck_resource_events_cause'),
        sqlalchemy.CheckConstraint(
            "actor_type IN ('system', 'basic', 'sa', 'sso', 'legacy', "
            "'unknown')",
            name='ck_resource_events_actor_type'),
    )
    op.create_index('ix_resource_events_workspace_sequence', _EVENTS,
                    ['workspace', 'event_sequence'])
    op.create_index('ix_resource_events_workspace_actor_sequence', _EVENTS,
                    ['workspace', 'actor_id', 'event_sequence'])
    op.create_index('ix_resource_events_request', _EVENTS,
                    ['source_request_id'])
    op.create_index('ix_resource_events_retention', _EVENTS, ['occurred_at'])

    op.create_table(
        _TARGETS,
        sqlalchemy.Column('event_id',
                          postgresql.UUID(as_uuid=True),
                          sqlalchemy.ForeignKey(f'{_EVENTS}.event_id',
                                                ondelete='CASCADE'),
                          primary_key=True),
        sqlalchemy.Column('position', sqlalchemy.SmallInteger,
                          primary_key=True),
        sqlalchemy.Column('target_type', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('target_id', sqlalchemy.Text),
        sqlalchemy.Column('target_name', sqlalchemy.Text, nullable=False),
        sqlalchemy.CheckConstraint("target_type IN ('cluster')",
                                   name='ck_resource_event_targets_type'),
        sqlalchemy.CheckConstraint('position >= 0',
                                   name='ck_resource_event_targets_position'),
    )
    op.create_index('ix_resource_event_targets_id', _TARGETS,
                    ['target_type', 'target_id', 'event_id'])
    op.create_index('ix_resource_event_targets_name', _TARGETS,
                    ['target_type', 'target_name', 'event_id'])

    authority = json.dumps({
        'authority_id': str(uuid.uuid4()),
        'event_sequence': 0,
    })
    op.get_bind().execute(
        sqlalchemy.text(
            f'INSERT INTO {_METADATA} (key, value, updated_at) '
            'VALUES (:key, CAST(:value AS JSONB), clock_timestamp()) '
            'ON CONFLICT (key) DO NOTHING'), {
                'key': _AUTHORITY_KEY,
                'value': authority,
            })


def downgrade() -> None:
    """Drop empty event storage while preserving non-empty history."""
    _require_postgresql()
    bind = op.get_bind()
    # Match the writer's lock order: sequence metadata, then event table. A
    # writer that already allocated a sequence can finish before the check;
    # later writers cannot begin. The table exclusion is then held through the
    # count and DDL, closing the check/drop race without a lock-order deadlock.
    bind.execute(
        sqlalchemy.text(f'SELECT key FROM {_METADATA} WHERE key = :key '
                        'FOR UPDATE'), {'key': _AUTHORITY_KEY})
    bind.execute(
        sqlalchemy.text(f'LOCK TABLE {_EVENTS} IN ACCESS EXCLUSIVE MODE'))
    count = int(
        bind.execute(
            sqlalchemy.text(f'SELECT COUNT(*) FROM {_EVENTS}')).scalar_one())
    if count:
        raise RuntimeError(
            'Cannot downgrade the operational event schema while events '
            'exist.')
    bind.execute(sqlalchemy.text(f'DELETE FROM {_METADATA} WHERE key = :key'),
                 {'key': _AUTHORITY_KEY})
    op.drop_table(_TARGETS)
    op.drop_table(_EVENTS)
    op.drop_column(_REQUESTS, 'event_context')
