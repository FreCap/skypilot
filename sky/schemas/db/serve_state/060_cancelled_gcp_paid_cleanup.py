"""Allow exact GCP paid cleanup after provider-I/O cancellation.

Revision ID: 060
Revises: 059
Create Date: 2026-08-26

An executor may be explicitly cancelled after entering GCP provider I/O.  Its
terminal request and exact execution-quiescence receipt are authoritative, but
the allocation must still be observed and removed before the claim, retention
pin, replica pointer, or association can settle.  Serve060 adds only that GCP
Spot terminal shape to the existing provider PRESENCE/ABSENCE transaction.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '060'
down_revision: str | Sequence[str] | None = '059'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_PROJECTION_CONSTRAINT = 'serve047_provider_absence_projection_ck'
_ASSOCIATION_GUARD_FUNCTION = 'skyserve042_guard_ordinary_association'
_REPLICA_GUARD_FUNCTION = 'skyserve042_guard_replica_binding'

_TERMINAL_CHECK = (
    "((terminal_status = 'FAILED' AND terminal_cause = 'handler_failed') OR "
    "(terminal_status = 'CANCELLED' AND terminal_cause = 'explicit_cancel' "
    "AND paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' "
    "AND paid_capacity_pool_key::jsonb ->> 'version' = '1' "
    "AND paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true'))")
_PROJECTION_CHECK = (
    "resolution <> 'PROJECTED' OR effect_phase = 'SERVICE_JOB_RECORDED' "
    "OR (binding_protocol_version = 2 AND profile_kind = 'RESERVED_FILL' "
    "AND reconciliation_outcome = 'PROJECTED' AND "
    "provider_evidence = 'ABSENT' AND "
    "provider_evidence_observed_at >= execution_quiesced_at) OR "
    "(binding_protocol_version = 2 AND "
    "profile_kind = 'ORDINARY_PAID' AND "
    "reconciliation_outcome = 'PROJECTED' AND "
    "provider_evidence = 'ABSENT' AND "
    "execution_quiesced_at IS NOT NULL AND "
    "provider_evidence_observed_at >= execution_quiesced_at AND "
    "effect_phase = 'PROVIDER_IO' AND "
    "paid_capacity_pool_key IS NOT NULL AND service_job_id IS NULL AND "
    f"{_TERMINAL_CHECK})")

_ASSOCIATION_OLD_TERMINAL_SOURCE = (
    "OLD.terminal_status = 'FAILED' AND\n"
    "                 OLD.terminal_cause = 'handler_failed' AND\n")
_ASSOCIATION_OLD_TERMINAL_REPLACEMENT = (
    "((OLD.terminal_status = 'FAILED' AND\n"
    "                  OLD.terminal_cause = 'handler_failed') OR\n"
    "                 (OLD.terminal_status = 'CANCELLED' AND\n"
    "                  OLD.terminal_cause = 'explicit_cancel' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'gcp' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'1' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true')) AND\n")
_ASSOCIATION_NEW_TERMINAL_SOURCE = (
    "NEW.terminal_status = 'FAILED' AND\n"
    "                 NEW.terminal_cause = 'handler_failed')")
_ASSOCIATION_NEW_TERMINAL_REPLACEMENT = (
    "((NEW.terminal_status = 'FAILED' AND\n"
    "                  NEW.terminal_cause = 'handler_failed') OR\n"
    "                 (NEW.terminal_status = 'CANCELLED' AND\n"
    "                  NEW.terminal_cause = 'explicit_cancel' AND\n"
    "                  NEW.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'gcp' AND\n"
    "                  NEW.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'1' AND\n"
    "                  NEW.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true')))")
_REPLICA_TERMINAL_SOURCE = (
    "association.terminal_status = 'FAILED' AND\n"
    "                     association.terminal_cause = 'handler_failed' AND\n")
_REPLICA_TERMINAL_REPLACEMENT = (
    "((association.terminal_status = 'FAILED' AND\n"
    "                      association.terminal_cause = 'handler_failed') OR\n"
    "                     (association.terminal_status = 'CANCELLED' AND\n"
    "                      association.terminal_cause = 'explicit_cancel' "
    "AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'cloud' = 'gcp' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'version' = '1' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'use_spot' = 'true')) AND\n")


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('Cancelled GCP paid cleanup is PostgreSQL-only.')


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
            f'Serve060 found an unexpected {function_name} definition.')
    bind.exec_driver_sql(definition.replace(source, replacement))


def upgrade() -> None:
    """Install the closed cancelled-GCP provider cleanup transaction."""
    _require_postgresql()
    op.drop_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS, type_='check')
    op.create_check_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS,
                               _PROJECTION_CHECK)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_OLD_TERMINAL_SOURCE,
                               _ASSOCIATION_OLD_TERMINAL_REPLACEMENT)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_NEW_TERMINAL_SOURCE,
                               _ASSOCIATION_NEW_TERMINAL_REPLACEMENT)
    _replace_function_fragment(_REPLICA_GUARD_FUNCTION,
                               _REPLICA_TERMINAL_SOURCE,
                               _REPLICA_TERMINAL_REPLACEMENT)


def downgrade() -> None:
    """Preserve cancelled GCP cleanup history across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve060 is forward-only; cancelled GCP allocations may already '
        'have been retired.')
