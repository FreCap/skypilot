"""Allow exact paid replacement cleanup from GCP provider evidence.

Revision ID: 061
Revises: 060
Create Date: 2026-08-26

An UNKNOWN-capacity replacement can itself be funded by an exact paid claim.
If its GCP create handler terminates without a service-job result, the same
post-quiescence VM, managed-boot-disk, and retained-create-operation census as
an ordinary paid launch can prove PRESENT or ABSENT.  Serve061 adds only the
paid GCP Spot replacement projection shape; zero-cost replacements and all
other providers remain fail-closed.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '061'
down_revision: str | Sequence[str] | None = '060'
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
    "(profile_kind = 'ORDINARY_PAID' OR "
    "(profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' AND "
    "paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '1' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true')) AND "
    "reconciliation_outcome = 'PROJECTED' AND "
    "provider_evidence = 'ABSENT' AND "
    "execution_quiesced_at IS NOT NULL AND "
    "provider_evidence_observed_at >= execution_quiesced_at AND "
    "effect_phase = 'PROVIDER_IO' AND "
    "paid_capacity_pool_key IS NOT NULL AND service_job_id IS NULL AND "
    f"{_TERMINAL_CHECK})")

_ASSOCIATION_PROFILE_SOURCE = (
    "                 OLD.profile_kind = 'ORDINARY_PAID' AND\n")
_ASSOCIATION_PROFILE_REPLACEMENT = (
    "                 (OLD.profile_kind = 'ORDINARY_PAID' OR\n"
    "                 (OLD.profile_kind = "
    "'UNKNOWN_CAPACITY_REPLACEMENT' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'gcp' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'1' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true')) AND\n")
_REPLICA_PROFILE_SOURCE = (
    "                     association.profile_kind = 'ORDINARY_PAID' AND\n")
_REPLICA_PROFILE_REPLACEMENT = (
    "                     (association.profile_kind = 'ORDINARY_PAID' OR\n"
    "                     (association.profile_kind = "
    "'UNKNOWN_CAPACITY_REPLACEMENT' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'cloud' = 'gcp' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'version' = '1' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'use_spot' = 'true')) AND\n")


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Paid replacement GCP reconciliation is PostgreSQL-only.')


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
            f'Serve061 found an unexpected {function_name} definition.')
    bind.exec_driver_sql(definition.replace(source, replacement))


def upgrade() -> None:
    """Install the closed paid-replacement GCP reconciliation shape."""
    _require_postgresql()
    op.drop_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS, type_='check')
    op.create_check_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS,
                               _PROJECTION_CHECK)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_PROFILE_SOURCE,
                               _ASSOCIATION_PROFILE_REPLACEMENT)
    _replace_function_fragment(_REPLICA_GUARD_FUNCTION, _REPLICA_PROFILE_SOURCE,
                               _REPLICA_PROFILE_REPLACEMENT)


def downgrade() -> None:
    """Preserve paid replacement absence history across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve061 is forward-only; paid replacement allocations may already '
        'have been retired.')
