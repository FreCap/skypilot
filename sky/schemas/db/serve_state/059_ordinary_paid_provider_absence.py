"""Allow exact ordinary-paid projection after a provider rejection.

Revision ID: 059
Revises: 058
Create Date: 2026-08-25

Serve059 widens the existing provider-absence settlement transaction for one
closed ordinary-paid shape.  It does not infer absence from a missing cluster
row or rewrite retained associations: the application must first persist an
exact post-quiescence provider ``ABSENT`` receipt for the failed provider call.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '059'
down_revision: str | Sequence[str] | None = '058'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_PROJECTION_CONSTRAINT = 'serve047_provider_absence_projection_ck'
_PAID_POOL_SCOPE_CONSTRAINT = 'serve059_paid_pool_scope_ck'
_PAID_RECEIPT_SCOPE_CONSTRAINT = 'serve059_paid_receipt_scope_ck'
_ASSOCIATION_GUARD_FUNCTION = 'skyserve042_guard_ordinary_association'
_REPLICA_GUARD_FUNCTION = 'skyserve042_guard_replica_binding'

_PAID_POOL_SCOPE_CHECK = (
    "CASE WHEN profile_kind IS DISTINCT FROM 'ORDINARY_PAID' THEN TRUE "
    "WHEN capability_cohort_epoch < 11 THEN TRUE ELSE "
    "COALESCE((paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
    "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
    "'aws_account_id' ~ '^[0-9]{12}$') OR "
    "(paid_capacity_pool_key::jsonb ->> 'cloud' <> 'aws' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '1' AND NOT "
    "(paid_capacity_pool_key::jsonb ? 'provider_identity')), FALSE) END")

_PAID_RECEIPT_SCOPE_CHECK = (
    "CASE WHEN profile_kind IS DISTINCT FROM 'ORDINARY_PAID' THEN TRUE "
    "WHEN provider_evidence IS DISTINCT FROM 'ABSENT' THEN TRUE ELSE "
    "CASE WHEN capability_cohort_epoch < 11 THEN TRUE "
    "WHEN COALESCE(paid_capacity_pool_key::jsonb ->> 'cloud' <> 'aws', "
    "FALSE) THEN TRUE ELSE "
    "COALESCE(((provider_evidence_payload #>> "
    "'{receipt,aws_account_id}') = (paid_capacity_pool_key::jsonb #>> "
    "'{provider_identity,aws_account_id}') AND "
    "provider_evidence_payload #>> '{receipt,client_token}' ~ "
    "'^[0-9a-f]{64}$'), FALSE) END END")

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
    "terminal_status = 'FAILED' AND terminal_cause = 'handler_failed')")

# Serve053 widened only the reserved-fill arm to include NOT_STARTED.  Match
# the exact current head and retain that arm without changing its semantics.
_ASSOCIATION_TRANSITION_SOURCE = (
    "NEW.effect_phase IN ('NOT_STARTED', 'PROVIDER_IO', "
    "'SERVICE_JOB_IO'))")
_ASSOCIATION_TRANSITION_REPLACEMENT = (
    _ASSOCIATION_TRANSITION_SOURCE + " OR\n"
    "                (OLD.resolution = 'AMBIGUOUS' AND\n"
    "                 NEW.resolution = 'PROJECTED' AND\n"
    "                 OLD.binding_protocol_version = 2 AND\n"
    "                 OLD.profile_kind = 'ORDINARY_PAID' AND\n"
    "                 OLD.reconciliation_outcome = "
    "'POST_EFFECT_AMBIGUOUS' AND\n"
    "                 OLD.provider_evidence = 'ABSENT' AND\n"
    "                 OLD.effect_phase = 'PROVIDER_IO' AND\n"
    "                 OLD.paid_capacity_pool_key IS NOT NULL AND\n"
    "                 OLD.service_job_id IS NULL AND\n"
    "                 OLD.terminal_status = 'FAILED' AND\n"
    "                 OLD.terminal_cause = 'handler_failed' AND\n"
    "                 OLD.execution_quiesced_at IS NOT NULL AND\n"
    "                 OLD.provider_evidence_observed_at >=\n"
    "                    OLD.execution_quiesced_at AND\n"
    "                 NEW.reconciliation_outcome = 'PROJECTED' AND\n"
    "                 NEW.ambiguity_code IS NULL AND\n"
    "                 NEW.execution_quiesced_at IS NOT NULL AND\n"
    "                 NEW.provider_evidence_observed_at >=\n"
    "                    NEW.execution_quiesced_at AND\n"
    "                 NEW.effect_phase = 'PROVIDER_IO' AND\n"
    "                 NEW.paid_capacity_pool_key IS NOT NULL AND\n"
    "                 NEW.service_job_id IS NULL AND\n"
    "                 NEW.terminal_status = 'FAILED' AND\n"
    "                 NEW.terminal_cause = 'handler_failed')")

_REPLICA_POINTER_SOURCE = (
    "(association.resolution = 'AMBIGUOUS' AND\n"
    "                     association.binding_protocol_version = 2 AND\n"
    "                     association.profile_kind = 'RESERVED_FILL' AND\n"
    "                     association.reconciliation_outcome =\n"
    "                        'POST_EFFECT_AMBIGUOUS' AND\n"
    "                     association.provider_evidence = 'ABSENT' AND\n"
    "                     association.provider_evidence_observed_at IS NOT "
    "NULL)")
_REPLICA_POINTER_REPLACEMENT = (
    _REPLICA_POINTER_SOURCE + " OR\n"
    "                    (association.resolution = 'AMBIGUOUS' AND\n"
    "                     association.binding_protocol_version = 2 AND\n"
    "                     association.profile_kind = 'ORDINARY_PAID' AND\n"
    "                     association.reconciliation_outcome =\n"
    "                        'POST_EFFECT_AMBIGUOUS' AND\n"
    "                     association.provider_evidence = 'ABSENT' AND\n"
    "                     association.effect_phase = 'PROVIDER_IO' AND\n"
    "                     association.paid_capacity_pool_key IS NOT NULL AND\n"
    "                     association.service_job_id IS NULL AND\n"
    "                     association.terminal_status = 'FAILED' AND\n"
    "                     association.terminal_cause = 'handler_failed' AND\n"
    "                     association.execution_quiesced_at IS NOT NULL AND\n"
    "                     association.provider_evidence_observed_at >=\n"
    "                        association.execution_quiesced_at)")


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Ordinary-paid provider-absence projection is PostgreSQL-only.')


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
            f'Serve059 found an unexpected {function_name} definition.')
    bind.exec_driver_sql(definition.replace(source, replacement))


def upgrade() -> None:
    """Install the closed ordinary-paid provider-absence transaction."""
    _require_postgresql()
    op.create_check_constraint(_PAID_POOL_SCOPE_CONSTRAINT, _ASSOCIATIONS,
                               _PAID_POOL_SCOPE_CHECK)
    op.create_check_constraint(_PAID_RECEIPT_SCOPE_CONSTRAINT, _ASSOCIATIONS,
                               _PAID_RECEIPT_SCOPE_CHECK)
    op.drop_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS, type_='check')
    op.create_check_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS,
                               _PROJECTION_CHECK)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_TRANSITION_SOURCE,
                               _ASSOCIATION_TRANSITION_REPLACEMENT)
    _replace_function_fragment(_REPLICA_GUARD_FUNCTION, _REPLICA_POINTER_SOURCE,
                               _REPLICA_POINTER_REPLACEMENT)


def downgrade() -> None:
    """Preserve projected paid-provider absence across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve059 is forward-only; ordinary-paid provider-absence history '
        'may already have been projected.')
