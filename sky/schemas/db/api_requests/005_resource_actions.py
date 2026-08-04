"""Add the inert durable resource-action journal.

Revision ID: 005
Revises: 004
Create Date: 2026-07-31

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '005'
down_revision: str | Sequence[str] | None = '004'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUESTS = 'api_requests'
_ACTIONS = 'api_resource_actions'
_ATTEMPTS = 'api_resource_action_attempts'


def _require_postgresql() -> None:
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('The resource-action journal is PostgreSQL-only.')


def upgrade() -> None:
    """Create inert action history and nullable request correlation."""
    _require_postgresql()
    op.create_table(
        _ACTIONS,
        sqlalchemy.Column('action_id',
                          postgresql.UUID(as_uuid=True),
                          primary_key=True),
        sqlalchemy.Column('domain', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('resource_type', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('resource_identity', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('desired_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('action_type', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('immutable_spec', postgresql.JSONB, nullable=False),
        sqlalchemy.Column('immutable_spec_sha256',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('kernel_state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('current_attempt',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('next_attempt_at',
                          sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('last_result', postgresql.JSONB),
        sqlalchemy.Column('last_result_sha256', sqlalchemy.Text),
        sqlalchemy.Column('terminal_disposition', sqlalchemy.Text),
        sqlalchemy.Column('revision',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='1'),
        sqlalchemy.Column('created_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('updated_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('terminal_at', sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.UniqueConstraint('domain',
                                    'resource_type',
                                    'resource_identity',
                                    'desired_generation',
                                    'action_type',
                                    name='uq_api_resource_actions_identity'),
        sqlalchemy.CheckConstraint(
            'octet_length(domain) BETWEEN 1 AND 64 AND '
            'octet_length(resource_type) BETWEEN 1 AND 64 AND '
            'octet_length(action_type) BETWEEN 1 AND 64 AND '
            'octet_length(resource_identity) BETWEEN 1 AND 1024',
            name='ck_api_resource_actions_identity_bounds'),
        sqlalchemy.CheckConstraint(
            'desired_generation > 0',
            name='ck_api_resource_actions_generation_positive'),
        sqlalchemy.CheckConstraint(
            "jsonb_typeof(immutable_spec) IS NOT DISTINCT FROM 'object' AND "
            'octet_length(CAST(immutable_spec AS TEXT)) <= 65536',
            name='ck_api_resource_actions_spec_shape'),
        sqlalchemy.CheckConstraint("immutable_spec_sha256 ~ '^[0-9a-f]{64}$'",
                                   name='ck_api_resource_actions_spec_sha256'),
        sqlalchemy.CheckConstraint(
            "kernel_state IN ('READY', 'QUEUED', 'BLOCKED', 'TERMINAL')",
            name='ck_api_resource_actions_state'),
        sqlalchemy.CheckConstraint(
            'current_attempt >= 0',
            name='ck_api_resource_actions_attempt_nonnegative'),
        sqlalchemy.CheckConstraint(
            "(kernel_state = 'READY' AND next_attempt_at IS NOT NULL) OR "
            "(kernel_state <> 'READY' AND next_attempt_at IS NULL)",
            name='ck_api_resource_actions_due_shape'),
        sqlalchemy.CheckConstraint(
            "kernel_state <> 'QUEUED' OR current_attempt > 0",
            name='ck_api_resource_actions_queued_attempt'),
        sqlalchemy.CheckConstraint(
            '(last_result IS NULL AND last_result_sha256 IS NULL) OR '
            '(last_result IS NOT NULL AND '
            'last_result_sha256 IS NOT NULL AND '
            "jsonb_typeof(last_result) IS NOT DISTINCT FROM 'object' AND "
            'octet_length(CAST(last_result AS TEXT)) <= 65536 AND '
            "last_result_sha256 ~ '^[0-9a-f]{64}$')",
            name='ck_api_resource_actions_result_shape'),
        sqlalchemy.CheckConstraint(
            "(kernel_state = 'TERMINAL' AND last_result IS NOT NULL AND "
            'terminal_disposition IS NOT NULL AND '
            'octet_length(terminal_disposition) BETWEEN 1 AND 64 AND '
            'terminal_at IS NOT NULL) OR '
            "(kernel_state <> 'TERMINAL' AND "
            'terminal_disposition IS NULL AND terminal_at IS NULL)',
            name='ck_api_resource_actions_terminal_shape'),
        sqlalchemy.CheckConstraint(
            'revision > 0', name='ck_api_resource_actions_revision_positive'),
        sqlalchemy.CheckConstraint(
            'updated_at >= created_at AND '
            '(terminal_at IS NULL OR terminal_at >= created_at)',
            name='ck_api_resource_actions_timestamp_order'),
    )
    op.create_index('ix_api_resource_actions_due',
                    _ACTIONS, ['next_attempt_at', 'action_id'],
                    postgresql_where=sqlalchemy.text("kernel_state = 'READY'"))
    op.create_index('ix_api_resource_actions_queued',
                    _ACTIONS, ['updated_at', 'action_id'],
                    postgresql_where=sqlalchemy.text("kernel_state = 'QUEUED'"))

    op.create_table(
        _ATTEMPTS,
        sqlalchemy.Column('action_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=False),
        sqlalchemy.Column('attempt', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('request_input_sha256',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('provider_operation_id', sqlalchemy.Text),
        sqlalchemy.Column('mutation_boundary',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='NOT_STARTED'),
        sqlalchemy.Column('typed_outcome', postgresql.JSONB),
        sqlalchemy.Column('typed_outcome_sha256', sqlalchemy.Text),
        sqlalchemy.Column('request_terminal_state', sqlalchemy.Text),
        sqlalchemy.Column('admitted_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('updated_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('settled_at', sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.PrimaryKeyConstraint('action_id',
                                        'attempt',
                                        name='pk_api_resource_action_attempts'),
        sqlalchemy.UniqueConstraint(
            'request_id', name='uq_api_resource_action_attempts_request'),
        sqlalchemy.UniqueConstraint(
            'action_id',
            'attempt',
            'request_id',
            name='uq_api_resource_action_attempts_binding'),
        sqlalchemy.ForeignKeyConstraint(
            ['action_id'], [f'{_ACTIONS}.action_id'],
            name='fk_api_resource_action_attempts_action'),
        sqlalchemy.CheckConstraint(
            'attempt > 0',
            name='ck_api_resource_action_attempts_attempt_positive'),
        sqlalchemy.CheckConstraint(
            'octet_length(request_id) BETWEEN 1 AND 128',
            name='ck_api_resource_action_attempts_request_bounds'),
        sqlalchemy.CheckConstraint(
            "request_input_sha256 ~ '^[0-9a-f]{64}$'",
            name='ck_api_resource_action_attempts_input_sha256'),
        sqlalchemy.CheckConstraint(
            'provider_operation_id IS NULL OR '
            'octet_length(provider_operation_id) BETWEEN 1 AND 1024',
            name='ck_api_resource_action_attempts_provider_id_bounds'),
        sqlalchemy.CheckConstraint(
            "mutation_boundary IN ('NOT_STARTED', 'INTENT_COMMITTED', "
            "'SUBMITTED_OR_AMBIGUOUS', 'SETTLED')",
            name='ck_api_resource_action_attempts_boundary'),
        sqlalchemy.CheckConstraint(
            "mutation_boundary IN ('SUBMITTED_OR_AMBIGUOUS', 'SETTLED') OR "
            'provider_operation_id IS NULL',
            name='ck_api_resource_action_attempts_provider_id_state'),
        sqlalchemy.CheckConstraint(
            "request_terminal_state IS NULL OR request_terminal_state IN ("
            "'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name='ck_api_resource_action_attempts_terminal_state'),
        sqlalchemy.CheckConstraint(
            "(mutation_boundary = 'SETTLED' AND typed_outcome IS NOT NULL "
            'AND typed_outcome_sha256 IS NOT NULL AND '
            'request_terminal_state IS NOT NULL AND settled_at IS NOT NULL '
            "AND jsonb_typeof(typed_outcome) IS NOT DISTINCT FROM 'object' AND "
            'octet_length(CAST(typed_outcome AS TEXT)) <= 65536 AND '
            "typed_outcome_sha256 ~ '^[0-9a-f]{64}$') OR "
            "(mutation_boundary <> 'SETTLED' AND typed_outcome IS NULL AND "
            'typed_outcome_sha256 IS NULL AND '
            'request_terminal_state IS NULL AND settled_at IS NULL)',
            name='ck_api_resource_action_attempts_settled_shape'),
        sqlalchemy.CheckConstraint(
            'updated_at >= admitted_at AND '
            '(settled_at IS NULL OR settled_at >= admitted_at)',
            name='ck_api_resource_action_attempts_timestamp_order'),
    )

    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('resource_action_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=True))
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('resource_action_attempt',
                          sqlalchemy.Integer,
                          nullable=True))
    op.create_check_constraint(
        'ck_api_requests_resource_action_pair', _REQUESTS,
        '(resource_action_id IS NULL AND resource_action_attempt IS NULL) OR '
        '(resource_action_id IS NOT NULL AND '
        'resource_action_attempt IS NOT NULL)')
    op.create_check_constraint(
        'ck_api_requests_resource_action_attempt_positive', _REQUESTS,
        'resource_action_attempt IS NULL OR resource_action_attempt > 0')
    op.create_foreign_key(
        'fk_api_requests_resource_action_attempt', _REQUESTS, _ATTEMPTS,
        ['resource_action_id', 'resource_action_attempt', 'request_id'],
        ['action_id', 'attempt', 'request_id'])
    op.create_index(
        'ix_api_requests_resource_action_attempt',
        _REQUESTS, ['resource_action_id', 'resource_action_attempt'],
        postgresql_where=sqlalchemy.text('resource_action_id IS NOT NULL'))


def downgrade() -> None:
    """Retain the additive journal across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API request schema 005 is additive and cannot be downgraded. '
        'Roll back the application against the retained schema instead.')
