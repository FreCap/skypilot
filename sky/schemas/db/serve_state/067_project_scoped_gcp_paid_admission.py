"""Admit project-scoped GCP paid launches and their exact cleanup.

Revision ID: 067
Revises: 066
Create Date: 2026-09-02

Fresh cohort-15 GCP paid effects use a version-2 pool key that freezes the
project ID.  Serve067 aligns the two deployed association constraints and the
existing provider-absence transition guards with that already-typed runtime
contract.  Retained version-1 GCP rows remain cleanup-only; this migration
does not rewrite any row or manufacture provider evidence.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '067'
down_revision: str | Sequence[str] | None = '066'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_PROJECTION_CONSTRAINT = 'serve047_provider_absence_projection_ck'
_PAID_POOL_SCOPE_CONSTRAINT = 'serve059_paid_pool_scope_ck'
_ASSOCIATION_GUARD_FUNCTION = 'skyserve042_guard_ordinary_association'
_REPLICA_GUARD_FUNCTION = 'skyserve042_guard_replica_binding'

_GCP_V1_POOL = ("paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
                "paid_capacity_pool_key::jsonb ->> 'version' = '1' AND "
                "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true'")
_GCP_V2_POOL = ("paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
                "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
                "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
                "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
                "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$'")
_GCP_RECONCILIATION_POOL = (
    "paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
    "((paid_capacity_pool_key::jsonb ->> 'version' = '1') OR "
    "(paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
    "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
    "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$'))")
_AWS_V2_POOL = ("paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
                "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
                "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
                "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
                "'aws_account_id' ~ '^[0-9]{12}$'")
_TERMINAL_CHECK = (
    "((terminal_status = 'FAILED' AND terminal_cause = 'handler_failed') OR "
    "(terminal_status = 'CANCELLED' AND terminal_cause = 'explicit_cancel' "
    f"AND (({_GCP_RECONCILIATION_POOL}) OR ({_AWS_V2_POOL}))))")

_PAID_POOL_SCOPE_CHECK = (
    "CASE WHEN profile_kind IS DISTINCT FROM 'ORDINARY_PAID' AND "
    "profile_kind IS DISTINCT FROM 'UNKNOWN_CAPACITY_REPLACEMENT' THEN TRUE "
    "WHEN capability_cohort_epoch < 11 THEN TRUE "
    "WHEN profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' THEN "
    f"COALESCE(({_GCP_RECONCILIATION_POOL}) OR ({_AWS_V2_POOL}), FALSE) "
    "ELSE COALESCE((paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
    "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
    "'aws_account_id' ~ '^[0-9]{12}$') OR "
    "(paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
    "((capability_cohort_epoch < 15 AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '1' AND NOT "
    "(paid_capacity_pool_key::jsonb ? 'provider_identity')) OR "
    "(paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
    "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
    "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$'))), FALSE) END")

_PROJECTION_CHECK = (
    "resolution <> 'PROJECTED' OR effect_phase = 'SERVICE_JOB_RECORDED' "
    "OR (binding_protocol_version = 2 AND profile_kind = 'RESERVED_FILL' "
    "AND reconciliation_outcome = 'PROJECTED' AND "
    "provider_evidence = 'ABSENT' AND "
    "provider_evidence_observed_at >= execution_quiesced_at) OR "
    "(binding_protocol_version = 2 AND "
    "(profile_kind = 'ORDINARY_PAID' OR "
    "(profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' AND "
    f"(({_GCP_RECONCILIATION_POOL}) OR ({_AWS_V2_POOL})))) AND "
    "reconciliation_outcome = 'PROJECTED' AND "
    "provider_evidence = 'ABSENT' AND "
    "execution_quiesced_at IS NOT NULL AND "
    "provider_evidence_observed_at >= execution_quiesced_at AND "
    "effect_phase IN ('PROVIDER_IO', 'SERVICE_JOB_IO') AND "
    "paid_capacity_pool_key IS NOT NULL AND service_job_id IS NULL AND "
    f"{_TERMINAL_CHECK})")


def _gcp_v1_guard_fragment(prefix: str, indent: str) -> str:
    return (f"{prefix}.paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND\n"
            f"{indent}{prefix}.paid_capacity_pool_key::jsonb ->> 'version' = "
            "'1' AND\n"
            f"{indent}{prefix}.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
            "'true'")


def _gcp_v1_v2_guard_fragment(prefix: str, indent: str) -> str:
    return (f"{prefix}.paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND\n"
            f"{indent}{prefix}.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
            "'true' AND\n"
            f"{indent}(({prefix}.paid_capacity_pool_key::jsonb ->> 'version' = "
            "'1') OR\n"
            f"{indent} ({prefix}.paid_capacity_pool_key::jsonb ->> 'version' = "
            "'2' AND\n"
            f"{indent}  {prefix}.paid_capacity_pool_key::jsonb -> "
            "'provider_identity' ->>\n"
            f"{indent}  'gcp_project_id' ~ "
            "'^[a-z][a-z0-9-]{4,28}[a-z0-9]$'))")


_ASSOCIATION_OLD_GCP_SOURCE = _gcp_v1_guard_fragment('OLD',
                                                     '                    ')
_ASSOCIATION_OLD_GCP_REPLACEMENT = _gcp_v1_v2_guard_fragment(
    'OLD', '                    ')
_ASSOCIATION_NEW_GCP_SOURCE = _gcp_v1_guard_fragment('NEW',
                                                     '                    ')
_ASSOCIATION_NEW_GCP_REPLACEMENT = _gcp_v1_v2_guard_fragment(
    'NEW', '                    ')
_REPLICA_GCP_SOURCE = _gcp_v1_guard_fragment('association',
                                             '                        ')
_REPLICA_GCP_REPLACEMENT = _gcp_v1_v2_guard_fragment(
    'association', '                        ')


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Project-scoped GCP paid admission is PostgreSQL-only.')


def _replace_function_fragment(function_name: str, source: str,
                               replacement: str, expected_count: int) -> None:
    """Replace an exact current-head fragment without owning historical DDL."""
    bind = op.get_bind()
    definition = bind.execute(
        sa.text('SELECT pg_get_functiondef('
                'CAST(:signature AS regprocedure))'), {
                    'signature': f'{function_name}()'
                }).scalar_one()
    source_count = definition.count(source)
    replacement_count = definition.count(replacement)
    if source_count == 0 and replacement_count == expected_count:
        return
    if source_count != expected_count or replacement_count != 0:
        raise RuntimeError(
            f'Serve067 found an unexpected {function_name} definition.')
    bind.exec_driver_sql(definition.replace(source, replacement))


def upgrade() -> None:
    """Install exact project-scoped GCP admission and cleanup gates."""
    _require_postgresql()
    for constraint in (_PAID_POOL_SCOPE_CONSTRAINT, _PROJECTION_CONSTRAINT):
        op.drop_constraint(constraint, _ASSOCIATIONS, type_='check')
    op.create_check_constraint(_PAID_POOL_SCOPE_CONSTRAINT, _ASSOCIATIONS,
                               _PAID_POOL_SCOPE_CHECK)
    op.create_check_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS,
                               _PROJECTION_CHECK)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_OLD_GCP_SOURCE,
                               _ASSOCIATION_OLD_GCP_REPLACEMENT, 2)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_NEW_GCP_SOURCE,
                               _ASSOCIATION_NEW_GCP_REPLACEMENT, 1)
    _replace_function_fragment(_REPLICA_GUARD_FUNCTION, _REPLICA_GCP_SOURCE,
                               _REPLICA_GCP_REPLACEMENT, 2)


def downgrade() -> None:
    """Preserve project-scoped paid launch history across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve067 is forward-only; project-scoped GCP paid allocations may '
        'already have durable launch or cleanup history.')
