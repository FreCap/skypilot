"""Persist exact executor-termination evidence.

Revision ID: 013
Revises: 012
Create Date: 2026-08-16

The relation is append-only evidence.  It never rewrites request terminal or
execution-quiescence state, and older processes simply retain the false
capability default during a mixed-version rollout.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '013'
down_revision: str | Sequence[str] | None = '012'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INSTANCES = 'api_server_instances'
_EVIDENCE = 'api_request_executor_termination_evidence'
_IMMUTABILITY_FUNCTION = 'skyapi013_reject_termination_evidence_mutation'
_IMMUTABILITY_TRIGGER = 'skyapi013_termination_evidence_immutable'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Executor-termination evidence requires PostgreSQL API state.')


def upgrade() -> None:
    """Add the closed capability and append-only evidence relation."""
    _require_postgresql()
    op.add_column(_INSTANCES, sa.Column('pod_namespace', sa.Text()))
    op.add_column(
        _INSTANCES,
        sa.Column('executor_termination_evidence_capable',
                  sa.Boolean(),
                  nullable=False,
                  server_default=sa.false()))
    op.add_column(
        _INSTANCES,
        sa.Column('executor_termination_evidence_protocol_version',
                  sa.Integer()))
    op.create_check_constraint(
        'ck_api_server_instances_executor_termination_evidence', _INSTANCES,
        '((NOT executor_termination_evidence_capable AND '
        'executor_termination_evidence_protocol_version IS NULL) OR '
        '(executor_termination_evidence_capable AND '
        'executor_termination_evidence_protocol_version = 1))')

    op.create_table(
        _EVIDENCE,
        sa.Column('evidence_id',
                  postgresql.UUID(as_uuid=True),
                  primary_key=True),
        sa.Column('request_id', sa.Text(), nullable=False),
        sa.Column('execution_generation', sa.BigInteger(), nullable=False),
        sa.Column('claim_token', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('worker_instance_id',
                  postgresql.UUID(as_uuid=True),
                  nullable=False),
        sa.Column('worker_role', sa.Text(), nullable=False),
        sa.Column('kubernetes_cluster_uid', sa.Text(), nullable=False),
        sa.Column('pod_namespace', sa.Text(), nullable=False),
        sa.Column('pod_name', sa.Text(), nullable=False),
        sa.Column('pod_uid', sa.Text(), nullable=False),
        sa.Column('container_name', sa.Text(), nullable=False),
        sa.Column('pod_resource_version', sa.Text(), nullable=False),
        sa.Column('pod_deletion_timestamp',
                  sa.DateTime(timezone=True),
                  nullable=False),
        sa.Column('container_finished_at',
                  sa.DateTime(timezone=True),
                  nullable=False),
        sa.Column('container_exit_code', sa.Integer(), nullable=False),
        sa.Column('container_reason', sa.Text()),
        sa.Column('source', sa.Text(), nullable=False),
        sa.Column('evidence_payload',
                  postgresql.JSONB(none_as_null=True),
                  nullable=False),
        sa.Column('evidence_digest', sa.Text(), nullable=False),
        sa.Column('observer_instance_id',
                  postgresql.UUID(as_uuid=True),
                  nullable=False),
        sa.Column('observer_controller_generation',
                  sa.BigInteger(),
                  nullable=False),
        sa.Column('observed_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.clock_timestamp()),
        sa.Column('created_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.clock_timestamp()),
        sa.CheckConstraint(
            'execution_generation > 0 AND '
            'observer_controller_generation > 0 AND '
            'container_exit_code >= 0',
            name='ck_api013_executor_termination_numeric'),
        sa.CheckConstraint(
            'length(request_id) > 0 AND length(worker_role) > 0 AND '
            'length(kubernetes_cluster_uid) > 0 AND '
            'length(pod_namespace) > 0 AND length(pod_name) > 0 AND '
            'length(pod_uid) > 0 AND length(container_name) > 0 AND '
            'length(pod_resource_version) > 0',
            name='ck_api013_executor_termination_text'),
        sa.CheckConstraint(
            "pod_uid = worker_instance_id::text AND worker_role IN "
            "('all', 'executor', 'controller')",
            name='ck_api013_executor_termination_worker'),
        sa.CheckConstraint(
            'container_finished_at >= pod_deletion_timestamp AND '
            'observed_at >= container_finished_at',
            name='ck_api013_executor_termination_time'),
        sa.CheckConstraint(
            "jsonb_typeof(evidence_payload) = 'object' AND "
            "evidence_digest ~ '^[0-9a-f]{64}$'",
            name='ck_api013_executor_termination_payload'),
        sa.CheckConstraint("source = 'KUBERNETES_POD_TERMINATED_V1'",
                           name='ck_api013_executor_termination_source'),
    )
    op.create_index('uq_api013_executor_termination_execution',
                    _EVIDENCE, [
                        'request_id', 'execution_generation', 'claim_token',
                        'worker_instance_id'
                    ],
                    unique=True)
    op.create_index('ix_api013_executor_termination_worker', _EVIDENCE,
                    ['worker_instance_id', 'container_finished_at'])
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_IMMUTABILITY_FUNCTION}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            RAISE EXCEPTION
                'API executor-termination evidence is append-only';
        END;
        $function$
    """)
    op.execute(f"""
        CREATE TRIGGER {_IMMUTABILITY_TRIGGER}
        BEFORE UPDATE OR DELETE OR TRUNCATE ON {_EVIDENCE}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {_IMMUTABILITY_FUNCTION}()
    """)


def downgrade() -> None:
    """Retain termination evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API013 is forward-only. Retain executor-termination evidence and '
        'roll application code forward.')
