"""Allow exact paid replacement cleanup from AWS provider evidence.

Revision ID: 063
Revises: 062
Create Date: 2026-08-27

An UNKNOWN-capacity replacement funded by an exact AWS Spot claim shares the
ordinary paid association's stable EC2 ClientToken identity.  Serve063 admits
only the version-2 account-scoped AWS pool and canonical post-quiescence census
shape.  It manufactures no evidence and keeps every incomplete allocation
fail-closed.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '063'
down_revision: str | Sequence[str] | None = '062'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_PROJECTION_CONSTRAINT = 'serve047_provider_absence_projection_ck'
_PAID_POOL_SCOPE_CONSTRAINT = 'serve059_paid_pool_scope_ck'
_PAID_RECEIPT_SCOPE_CONSTRAINT = 'serve059_paid_receipt_scope_ck'
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
    "effect_phase = 'PROVIDER_IO' AND "
    "paid_capacity_pool_key IS NOT NULL AND service_job_id IS NULL AND "
    f"{_TERMINAL_CHECK})")

_PAID_POOL_SCOPE_CHECK = (
    "CASE WHEN profile_kind IS DISTINCT FROM 'ORDINARY_PAID' AND "
    "profile_kind IS DISTINCT FROM 'UNKNOWN_CAPACITY_REPLACEMENT' THEN TRUE "
    "WHEN capability_cohort_epoch < 11 THEN TRUE "
    "WHEN profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' THEN "
    f"COALESCE(({_GCP_REPLACEMENT_POOL}) OR ({_AWS_REPLACEMENT_POOL}), FALSE) "
    "ELSE COALESCE((paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
    "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
    "'aws_account_id' ~ '^[0-9]{12}$') OR "
    "(paid_capacity_pool_key::jsonb ->> 'cloud' <> 'aws' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '1' AND NOT "
    "(paid_capacity_pool_key::jsonb ? 'provider_identity')), FALSE) END")

_AWS_CENSUS_SCOPE = (
    "provider_evidence_payload ->> 'probe_contract' = "
    "'aws-client-token-instance-presence-v1' AND "
    "provider_evidence_payload ->> 'result' = 'ABSENT' AND "
    "jsonb_typeof(provider_evidence_payload -> 'instances') = 'array' AND "
    "(provider_evidence_payload #>> '{provider_identity,aws_account_id}') = "
    "(paid_capacity_pool_key::jsonb #>> "
    "'{provider_identity,aws_account_id}') AND "
    "(provider_evidence_payload #>> '{provider_identity,workspace}') = "
    "(paid_capacity_pool_key::jsonb ->> 'workspace') AND "
    "(provider_evidence_payload #>> '{provider_identity,region}') = "
    "(paid_capacity_pool_key::jsonb ->> 'region') AND "
    "(provider_evidence_payload #>> '{provider_identity,zone}') = "
    "(paid_capacity_pool_key::jsonb ->> 'zone') AND "
    "(provider_evidence_payload #>> '{provider_identity,instance_type}') = "
    "(paid_capacity_pool_key::jsonb ->> 'instance_type') AND "
    "(provider_evidence_payload #> '{provider_identity,num_nodes}') = "
    "(paid_capacity_pool_key::jsonb -> 'num_nodes') AND "
    "(provider_evidence_payload #> '{provider_identity,use_spot}') = "
    "(paid_capacity_pool_key::jsonb -> 'use_spot') AND "
    "provider_evidence_payload #>> '{provider_identity,client_token}' ~ "
    "'^[0-9a-f]{64}$' AND length(provider_evidence_payload #>> "
    "'{provider_identity,cluster_name_on_cloud}') > 0")
_PAID_RECEIPT_SCOPE_CHECK = (
    "CASE WHEN profile_kind IS DISTINCT FROM 'ORDINARY_PAID' AND NOT "
    "(profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' AND "
    f"({_AWS_REPLACEMENT_POOL})) THEN TRUE "
    "WHEN provider_evidence IS DISTINCT FROM 'ABSENT' THEN TRUE ELSE "
    "CASE WHEN capability_cohort_epoch < 11 THEN TRUE "
    "WHEN COALESCE(paid_capacity_pool_key::jsonb ->> 'cloud' <> 'aws', "
    "FALSE) THEN TRUE ELSE "
    "COALESCE((profile_kind = 'ORDINARY_PAID' AND "
    "(provider_evidence_payload #>> '{receipt,aws_account_id}') = "
    "(paid_capacity_pool_key::jsonb #>> "
    "'{provider_identity,aws_account_id}') AND "
    "provider_evidence_payload #>> '{receipt,client_token}' ~ "
    "'^[0-9a-f]{64}$') OR "
    f"({_AWS_CENSUS_SCOPE}), FALSE) END END")

_ASSOCIATION_PROFILE_SOURCE = (
    "                 (OLD.profile_kind = 'ORDINARY_PAID' OR\n"
    "                 (OLD.profile_kind = "
    "'UNKNOWN_CAPACITY_REPLACEMENT' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'gcp' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'1' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true')) AND\n")
_ASSOCIATION_PROFILE_REPLACEMENT = (
    "                 (OLD.profile_kind = 'ORDINARY_PAID' OR\n"
    "                 (OLD.profile_kind = "
    "'UNKNOWN_CAPACITY_REPLACEMENT' AND\n"
    "                  ((OLD.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'gcp' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'1' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true') OR\n"
    "                   (OLD.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'aws' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'2' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb -> "
    "'provider_identity' ->> 'aws_account_id' ~ "
    "'^[0-9]{12}$')))) AND\n")
_REPLICA_PROFILE_SOURCE = (
    "                     (association.profile_kind = 'ORDINARY_PAID' OR\n"
    "                     (association.profile_kind = "
    "'UNKNOWN_CAPACITY_REPLACEMENT' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'cloud' = 'gcp' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'version' = '1' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'use_spot' = 'true')) AND\n")
_REPLICA_PROFILE_REPLACEMENT = (
    "                     (association.profile_kind = 'ORDINARY_PAID' OR\n"
    "                     (association.profile_kind = "
    "'UNKNOWN_CAPACITY_REPLACEMENT' AND\n"
    "                      ((association.paid_capacity_pool_key::jsonb ->> "
    "'cloud' = 'gcp' AND\n"
    "                        association.paid_capacity_pool_key::jsonb ->> "
    "'version' = '1' AND\n"
    "                        association.paid_capacity_pool_key::jsonb ->> "
    "'use_spot' = 'true') OR\n"
    "                       (association.paid_capacity_pool_key::jsonb ->> "
    "'cloud' = 'aws' AND\n"
    "                        association.paid_capacity_pool_key::jsonb ->> "
    "'version' = '2' AND\n"
    "                        association.paid_capacity_pool_key::jsonb ->> "
    "'use_spot' = 'true' AND\n"
    "                        association.paid_capacity_pool_key::jsonb -> "
    "'provider_identity' ->> 'aws_account_id' ~ "
    "'^[0-9]{12}$')))) AND\n")


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Paid replacement AWS reconciliation is PostgreSQL-only.')


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
            f'Serve063 found an unexpected {function_name} definition.')
    bind.exec_driver_sql(definition.replace(source, replacement))


def upgrade() -> None:
    """Install the closed paid-replacement AWS reconciliation shape."""
    _require_postgresql()
    for constraint in (_PROJECTION_CONSTRAINT, _PAID_POOL_SCOPE_CONSTRAINT,
                       _PAID_RECEIPT_SCOPE_CONSTRAINT):
        op.drop_constraint(constraint, _ASSOCIATIONS, type_='check')
    op.create_check_constraint(_PROJECTION_CONSTRAINT, _ASSOCIATIONS,
                               _PROJECTION_CHECK)
    op.create_check_constraint(_PAID_POOL_SCOPE_CONSTRAINT, _ASSOCIATIONS,
                               _PAID_POOL_SCOPE_CHECK)
    op.create_check_constraint(_PAID_RECEIPT_SCOPE_CONSTRAINT, _ASSOCIATIONS,
                               _PAID_RECEIPT_SCOPE_CHECK)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_PROFILE_SOURCE,
                               _ASSOCIATION_PROFILE_REPLACEMENT)
    _replace_function_fragment(_REPLICA_GUARD_FUNCTION, _REPLICA_PROFILE_SOURCE,
                               _REPLICA_PROFILE_REPLACEMENT)


def downgrade() -> None:
    """Preserve paid replacement absence history across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve063 is forward-only; AWS replacement allocations may already '
        'have been retired.')
