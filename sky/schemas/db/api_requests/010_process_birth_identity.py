"""Add exact process and managed-job origin identity for request claims.

Revision ID: 010
Revises: 009
Create Date: 2026-08-13

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

revision: str = '010'
down_revision: str | Sequence[str] | None = '009'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUESTS = 'api_requests'
_PROCESS_START_CONSTRAINT = 'ck_api_requests_process_start_time'
_PID_CONSTRAINT = 'ck_api_requests_pid'
_MANAGED_JOB_ORIGIN_COMPLETE_CONSTRAINT = (
    'ck_api_requests_managed_job_origin_complete')
_MANAGED_JOB_ORIGIN_VALUES_CONSTRAINT = (
    'ck_api_requests_managed_job_origin_values')
_MANAGED_JOB_ATTEMPT_INDEX = 'ix_api_requests_managed_job_attempt'
_CONTROLLER_LEADERSHIP = 'api_controller_leadership'
_CONTROLLER_CAPABILITY_CONSTRAINT = (
    'ck_api_controller_leadership_capability_sha256')


def _require_postgresql() -> None:
    # Keep historical migrations independent from mutable runtime modules.
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('The central API request store is PostgreSQL-only.')


def upgrade() -> None:
    """Add exact executor and managed-job controller-attempt identities."""
    _require_postgresql()
    # API009 leaders remain insert-compatible during a rolling deployment.
    # API010 leadership acquisition always replaces NULL with the SHA-256
    # digest of a freshly generated capability before publishing the owner.
    op.add_column(
        _CONTROLLER_LEADERSHIP,
        sqlalchemy.Column('origin_capability_sha256',
                          postgresql.BYTEA,
                          nullable=True))
    op.create_check_constraint(
        _CONTROLLER_CAPABILITY_CONSTRAINT, _CONTROLLER_LEADERSHIP,
        'origin_capability_sha256 IS NULL OR '
        'octet_length(origin_capability_sha256) = 32')
    # Nullable preserves API009 writer compatibility during a rolling update.
    # API010 claimed executions write this atomically with PID/RUNNING. A NULL
    # legacy claim remains fail-closed for process-death recovery; ordinary
    # wrapper receipts remain its only replay authority.
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('execution_process_start_time_ticks',
                          sqlalchemy.BigInteger,
                          nullable=True))
    op.create_check_constraint(
        _PROCESS_START_CONSTRAINT, _REQUESTS,
        'execution_process_start_time_ticks IS NULL OR '
        'execution_process_start_time_ticks > 0')
    # Historical rows were not constrained.  NOT VALID enforces the invariant
    # for every API010 write without making deployment depend on cleaning an
    # unrelated legacy tombstone first.
    op.execute(f'ALTER TABLE {_REQUESTS} ADD CONSTRAINT {_PID_CONSTRAINT} '
               'CHECK (pid IS NULL OR pid > 0) NOT VALID')
    # API010 is the first writer of this tuple.  Nullable columns keep API009
    # writers and historical rows insert-compatible during rolling deploys;
    # the completeness constraint prevents a partially fenced nested request.
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('managed_job_id',
                          sqlalchemy.BigInteger,
                          nullable=True))
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('managed_job_controller_instance_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=True))
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('managed_job_controller_generation',
                          sqlalchemy.BigInteger,
                          nullable=True))
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('managed_job_controller_slot_id',
                          sqlalchemy.Integer,
                          nullable=True))
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('managed_job_controller_slot_attempt',
                          postgresql.UUID(as_uuid=True),
                          nullable=True))
    op.create_check_constraint(
        _MANAGED_JOB_ORIGIN_COMPLETE_CONSTRAINT, _REQUESTS,
        'num_nonnulls(managed_job_id, '
        'managed_job_controller_instance_id, '
        'managed_job_controller_generation, '
        'managed_job_controller_slot_id, '
        'managed_job_controller_slot_attempt) IN (0, 5)')
    op.create_check_constraint(
        _MANAGED_JOB_ORIGIN_VALUES_CONSTRAINT, _REQUESTS,
        '(managed_job_id IS NULL OR managed_job_id > 0) AND '
        '(managed_job_controller_generation IS NULL OR '
        'managed_job_controller_generation > 0) AND '
        '(managed_job_controller_slot_id IS NULL OR '
        'managed_job_controller_slot_id >= 0)')
    op.create_index(
        _MANAGED_JOB_ATTEMPT_INDEX,
        _REQUESTS, [
            'managed_job_id', 'managed_job_controller_instance_id',
            'managed_job_controller_generation',
            'managed_job_controller_slot_id',
            'managed_job_controller_slot_attempt'
        ],
        postgresql_where=sqlalchemy.text('managed_job_id IS NOT NULL'))


def downgrade() -> None:
    """Retain process-birth and managed-job evidence across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API request schema 010 is additive and cannot be downgraded. Roll '
        'back the application against the retained schema instead.')
