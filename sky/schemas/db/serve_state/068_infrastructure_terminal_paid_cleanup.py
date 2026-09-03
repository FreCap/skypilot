"""Allow exact paid cleanup after infrastructure terminal requests.

Revision ID: 068
Revises: 067
Create Date: 2026-09-03

Once an executor is durably quiescent, the existing exact AWS and GCP
version-2 provider censuses can safely prove whether resources are present or
absent.  The diagnostic terminal cause adds no authority to that structural
proof.  This migration admits every terminal request to the existing exact-v2
provider-evidence transition; it neither rewrites rows nor manufactures
evidence.  Narrow legacy handler-failure and GCP explicit-cancel behavior is
preserved.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '068'
down_revision: str | Sequence[str] | None = '067'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_PROJECTION_CONSTRAINT = 'serve047_provider_absence_projection_ck'
_ASSOCIATION_GUARD_FUNCTION = 'skyserve042_guard_ordinary_association'
_REPLICA_GUARD_FUNCTION = 'skyserve042_guard_replica_binding'

_GCP_V1_V2_POOL = ("paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
                   "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
                   "((paid_capacity_pool_key::jsonb ->> 'version' = '1') OR "
                   "(paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
                   "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
                   "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$'))")
_GCP_V2_POOL = ("paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
                "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
                "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
                "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
                "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$'")
_AWS_V2_POOL = ("paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
                "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
                "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
                "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
                "'aws_account_id' ~ '^[0-9]{12}$'")
_EXACT_V2_PROVIDER_POOL = f"(({_GCP_V2_POOL}) OR ({_AWS_V2_POOL}))"
_TERMINAL_CHECK = (
    "(((terminal_status IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND "
    "terminal_cause IS NOT NULL AND terminal_cause <> '') AND "
    f"{_EXACT_V2_PROVIDER_POOL}) OR "
    "(terminal_status = 'FAILED' AND terminal_cause = 'handler_failed') OR "
    "(terminal_status = 'CANCELLED' AND terminal_cause = 'explicit_cancel' "
    "AND paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '1' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true'))")

_PROJECTION_CHECK = (
    "resolution <> 'PROJECTED' OR effect_phase = 'SERVICE_JOB_RECORDED' "
    "OR (binding_protocol_version = 2 AND profile_kind = 'RESERVED_FILL' "
    "AND reconciliation_outcome = 'PROJECTED' AND "
    "provider_evidence = 'ABSENT' AND "
    "provider_evidence_observed_at >= execution_quiesced_at) OR "
    "(binding_protocol_version = 2 AND "
    "(profile_kind = 'ORDINARY_PAID' OR "
    "(profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' AND "
    f"(({_GCP_V1_V2_POOL}) OR ({_AWS_V2_POOL})))) AND "
    "reconciliation_outcome = 'PROJECTED' AND "
    "provider_evidence = 'ABSENT' AND "
    "execution_quiesced_at IS NOT NULL AND "
    "provider_evidence_observed_at >= execution_quiesced_at AND "
    "effect_phase IN ('PROVIDER_IO', 'SERVICE_JOB_IO') AND "
    "paid_capacity_pool_key IS NOT NULL AND service_job_id IS NULL AND "
    f"{_TERMINAL_CHECK})")


def _terminal_source(prefix: str, indent: str, trailing: str) -> str:
    arm_indent = indent[:-1]
    return (
        f"(({prefix}.terminal_status = 'FAILED' AND\n"
        f"{indent}{prefix}.terminal_cause = 'handler_failed') OR\n"
        f"{arm_indent}({prefix}.terminal_status = 'CANCELLED' AND\n"
        f"{indent}{prefix}.terminal_cause = 'explicit_cancel' AND\n"
        f"{indent}(({prefix}.paid_capacity_pool_key::jsonb ->> 'cloud' = "
        "'gcp' AND\n"
        f"{indent}  {prefix}.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
        "'true' AND\n"
        f"{indent}  (({prefix}.paid_capacity_pool_key::jsonb ->> 'version' = "
        "'1') OR\n"
        f"{indent}   ({prefix}.paid_capacity_pool_key::jsonb ->> 'version' = "
        "'2' AND\n"
        f"{indent}    {prefix}.paid_capacity_pool_key::jsonb -> "
        "'provider_identity' ->>\n"
        f"{indent}    'gcp_project_id' ~ "
        "'^[a-z][a-z0-9-]{4,28}[a-z0-9]$'))) OR\n"
        f"{arm_indent}  ({prefix}.paid_capacity_pool_key::jsonb ->> 'cloud' "
        "= 'aws' AND\n"
        f"{indent}  {prefix}.paid_capacity_pool_key::jsonb ->> 'version' = "
        "'2' AND\n"
        f"{indent}  {prefix}.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
        "'true' AND\n"
        f"{indent}  {prefix}.paid_capacity_pool_key::jsonb -> "
        "'provider_identity' ->> 'aws_account_id' ~ "
        f"'^[0-9]{{12}}$')))){trailing}")


def _provider_pool(prefix: str) -> str:
    return (
        f"(({prefix}.paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
        f"{prefix}.paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
        f"{prefix}.paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
        f"{prefix}.paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
        "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$') OR "
        f"({prefix}.paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
        f"{prefix}.paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
        f"{prefix}.paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
        f"{prefix}.paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
        "'aws_account_id' ~ '^[0-9]{12}$'))")


def _terminal_replacement(prefix: str, indent: str, trailing: str) -> str:
    arm_indent = indent[:-1]
    return (f"((({prefix}.terminal_status IN "
            "('SUCCEEDED', 'FAILED', 'CANCELLED') AND\n"
            f"{indent}{prefix}.terminal_cause IS NOT NULL AND\n"
            f"{indent}{prefix}.terminal_cause <> '') AND\n"
            f"{indent}{_provider_pool(prefix)}) OR\n"
            f"{arm_indent}({prefix}.terminal_status = 'FAILED' AND\n"
            f"{indent}{prefix}.terminal_cause = 'handler_failed') OR\n"
            f"{arm_indent}({prefix}.terminal_status = 'CANCELLED' AND\n"
            f"{indent}{prefix}.terminal_cause = 'explicit_cancel' AND\n"
            f"{indent}{prefix}.paid_capacity_pool_key::jsonb ->> 'cloud' = "
            "'gcp' AND\n"
            f"{indent}{prefix}.paid_capacity_pool_key::jsonb ->> 'version' = "
            "'1' AND\n"
            f"{indent}{prefix}.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
            f"'true')){trailing}")


_ASSOCIATION_OLD_TERMINAL_SOURCE = _terminal_source('OLD', ' ' * 18, ' AND\n')
_ASSOCIATION_OLD_TERMINAL_REPLACEMENT = _terminal_replacement(
    'OLD', ' ' * 18, ' AND\n')
_ASSOCIATION_NEW_TERMINAL_SOURCE = _terminal_source('NEW', ' ' * 18, '')
_ASSOCIATION_NEW_TERMINAL_REPLACEMENT = _terminal_replacement(
    'NEW', ' ' * 18, '')
_REPLICA_TERMINAL_SOURCE = _terminal_source('association', ' ' * 22, ' AND\n')
_REPLICA_TERMINAL_REPLACEMENT = _terminal_replacement('association', ' ' * 22,
                                                      ' AND\n')


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Infrastructure-terminal paid cleanup is PostgreSQL-only.')


def _replace_function_fragment(function_name: str, source: str,
                               replacement: str) -> None:
    """Replace one exact revision-067 fragment without owning prior DDL."""
    bind = op.get_bind()
    definition = bind.execute(
        sa.text('SELECT pg_get_functiondef('
                'CAST(:signature AS regprocedure))'), {
                    'signature': f'{function_name}()'
                }).scalar_one()
    source_count = definition.count(source)
    replacement_count = definition.count(replacement)
    if source_count == 0 and replacement_count == 1:
        return
    if source_count != 1 or replacement_count != 0:
        raise RuntimeError(
            f'Serve068 found an unexpected {function_name} definition.')
    bind.exec_driver_sql(definition.replace(source, replacement))


def upgrade() -> None:
    """Admit structurally terminal exact-v2 provider cleanup."""
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
    """Preserve cleanup history produced by infrastructure terminals."""
    _require_postgresql()
    raise RuntimeError(
        'Serve068 is forward-only; infrastructure-terminal allocations may '
        'already have been retired.')
