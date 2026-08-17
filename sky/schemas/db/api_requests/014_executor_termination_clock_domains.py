"""Remove cross-clock ordering from executor-termination evidence.

Revision ID: 014
Revises: 013
Create Date: 2026-08-17

Kubernetes API-server ``deletionTimestamp``, kubelet ``finishedAt``, and
PostgreSQL ``observed_at`` timestamps come from independent clocks.  The
final resource-versioned `Succeeded` state from a Pod deletion, rather than
wall-clock ordering, proves that the exact role container cannot restart.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '014'
down_revision: str | Sequence[str] | None = '013'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_EVIDENCE = 'api_request_executor_termination_evidence'
_CROSS_CLOCK_CONSTRAINT = 'ck_api013_executor_termination_time'
_INSTANCES = 'api_server_instances'
_INSTANCE_CAPABILITY_CONSTRAINT = (
    'ck_api_server_instances_executor_termination_evidence')
_V1_SOURCE_CONSTRAINT = 'ck_api013_executor_termination_source'
_V2_SOURCE_CONSTRAINT = 'ck_api014_executor_termination_source'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Executor-termination evidence requires PostgreSQL API state.')


def upgrade() -> None:
    """Replace cross-clock V1 evidence with terminal-deletion V2 evidence."""
    _require_postgresql()
    op.drop_constraint(_CROSS_CLOCK_CONSTRAINT, _EVIDENCE, type_='check')
    op.drop_constraint(_INSTANCE_CAPABILITY_CONSTRAINT,
                       _INSTANCES,
                       type_='check')
    op.create_check_constraint(
        _INSTANCE_CAPABILITY_CONSTRAINT, _INSTANCES,
        '((NOT executor_termination_evidence_capable AND '
        'executor_termination_evidence_protocol_version IS NULL) OR '
        '(executor_termination_evidence_capable AND '
        'executor_termination_evidence_protocol_version IN (1, 2)))')
    op.add_column(_EVIDENCE, sa.Column('pod_event_type', sa.Text()))
    op.add_column(_EVIDENCE, sa.Column('pod_phase', sa.Text()))
    op.drop_constraint(_V1_SOURCE_CONSTRAINT, _EVIDENCE, type_='check')
    op.create_check_constraint(
        _V2_SOURCE_CONSTRAINT, _EVIDENCE,
        "((source = 'KUBERNETES_POD_TERMINATED_V1' AND "
        'pod_event_type IS NULL AND pod_phase IS NULL) OR '
        "(source = 'KUBERNETES_POD_FINAL_SUCCEEDED_V2' AND "
        "pod_event_type = 'DELETED' AND pod_phase = 'Succeeded' AND "
        'container_exit_code = 0))')


def downgrade() -> None:
    """Retain the corrected evidence contract across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API014 is forward-only. Retain cross-clock termination evidence and '
        'roll application code forward.')
