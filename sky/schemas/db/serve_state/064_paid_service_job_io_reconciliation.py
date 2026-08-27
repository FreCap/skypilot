"""Reconcile exact paid allocations after service-job I/O begins.

Revision ID: 064
Revises: 063
Create Date: 2026-08-27

A paid launch can create provider resources and then fail after entering
SERVICE_JOB_IO but before recording a service job.  Serve064 aligns the
existing exact paid-provider projection and trigger guards with that durable
phase.  It writes no evidence and retains the null-service-job, terminal
quiescence, immutable provider identity, and canonical receipt requirements.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '064'
down_revision: str | Sequence[str] | None = '063'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_PROJECTION_CONSTRAINT = 'serve047_provider_absence_projection_ck'
_ASSOCIATION_GUARD_FUNCTION = 'skyserve042_guard_ordinary_association'
_REPLICA_GUARD_FUNCTION = 'skyserve042_guard_replica_binding'

_GCP_REPLACEMENT_POOL = (
    "paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '1' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true'")
_AWS_REPLACEMENT_POOL = (
    "paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
    "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
    "'aws_account_id' ~ '^[0-9]{12}$'")
_TERMINAL_CHECK = (
    "((terminal_status = 'FAILED' AND terminal_cause = 'handler_failed') OR "
    "(terminal_status = 'CANCELLED' AND terminal_cause = 'explicit_cancel' "
    f"AND (({_GCP_REPLACEMENT_POOL}) OR ({_AWS_REPLACEMENT_POOL}))))")
_PROJECTION_CHECK = (
    "resolution <> 'PROJECTED' OR effect_phase = 'SERVICE_JOB_RECORDED' "
    "OR (binding_protocol_version = 2 AND profile_kind = 'RESERVED_FILL' "
    "AND reconciliation_outcome = 'PROJECTED' AND "
    "provider_evidence = 'ABSENT' AND "
    "provider_evidence_observed_at >= execution_quiesced_at) OR "
    "(binding_protocol_version = 2 AND "
    "(profile_kind = 'ORDINARY_PAID' OR "
    "(profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' AND "
    f"(({_GCP_REPLACEMENT_POOL}) OR ({_AWS_REPLACEMENT_POOL})))) AND "
    "reconciliation_outcome = 'PROJECTED' AND "
    "provider_evidence = 'ABSENT' AND "
    "execution_quiesced_at IS NOT NULL AND "
    "provider_evidence_observed_at >= execution_quiesced_at AND "
    "effect_phase IN ('PROVIDER_IO', 'SERVICE_JOB_IO') AND "
    "paid_capacity_pool_key IS NOT NULL AND service_job_id IS NULL AND "
    f"{_TERMINAL_CHECK})")

_ASSOCIATION_OLD_PHASE_SOURCE = (
    "                 OLD.effect_phase = 'PROVIDER_IO' AND\n")
_ASSOCIATION_OLD_PHASE_REPLACEMENT = ("                 OLD.effect_phase IN "
                                      "('PROVIDER_IO', 'SERVICE_JOB_IO') AND\n")
_ASSOCIATION_NEW_PHASE_SOURCE = (
    "                 NEW.effect_phase = 'PROVIDER_IO' AND\n")
_ASSOCIATION_NEW_PHASE_REPLACEMENT = ("                 NEW.effect_phase IN "
                                      "('PROVIDER_IO', 'SERVICE_JOB_IO') AND\n")
_REPLICA_PHASE_SOURCE = (
    "                     association.effect_phase = 'PROVIDER_IO' AND\n")
_REPLICA_PHASE_REPLACEMENT = (
    "                     association.effect_phase IN "
    "('PROVIDER_IO', 'SERVICE_JOB_IO') AND\n")


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Paid service-job-I/O reconciliation is PostgreSQL-only.')


def _replace_function_fragment(function_name: str, source: str,
                               replacement: str) -> None:
    """Replace one exact fragment without owning historical function DDL."""
    bind = op.get_bind()
    definition = bind.execute(
        sa.text('SELECT pg_get_functiondef('
                'CAST(:signature AS regprocedure))'), {
                    'signature': f'{function_name}()'
                }).scalar_one()
    if replacement in definition:
        return
    if definition.count(source) != 1:
        raise RuntimeError(
            f'Serve064 found an unexpected {function_name} definition.')
    bind.exec_driver_sql(definition.replace(source, replacement))


def upgrade() -> None:
    """Install the closed paid service-job-I/O reconciliation shape."""
    _require_postgresql()
    op.drop_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS, type_='check')
    op.create_check_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS,
                               _PROJECTION_CHECK)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_OLD_PHASE_SOURCE,
                               _ASSOCIATION_OLD_PHASE_REPLACEMENT)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_NEW_PHASE_SOURCE,
                               _ASSOCIATION_NEW_PHASE_REPLACEMENT)
    _replace_function_fragment(_REPLICA_GUARD_FUNCTION, _REPLICA_PHASE_SOURCE,
                               _REPLICA_PHASE_REPLACEMENT)


def downgrade() -> None:
    """Preserve paid service-job-I/O absence history across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve064 is forward-only; paid service-job-I/O allocations may '
        'already have been retired.')
