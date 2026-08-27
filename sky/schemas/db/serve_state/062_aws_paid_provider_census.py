"""Allow exact AWS paid provider-census evidence.

Revision ID: 062
Revises: 061
Create Date: 2026-08-27

Serve062 widens only the existing ordinary-paid AWS receipt-scope constraint.
It retains the negative-ack receipt arm and additionally admits the canonical
post-quiescence EC2 ClientToken census envelope.  Application reduction still
validates the complete association-derived identity before this schema guard.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

revision: str = '062'
down_revision: str | Sequence[str] | None = '061'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_PAID_RECEIPT_SCOPE_CONSTRAINT = 'serve059_paid_receipt_scope_ck'

_PAID_RECEIPT_SCOPE_CHECK = (
    "CASE WHEN profile_kind IS DISTINCT FROM 'ORDINARY_PAID' THEN TRUE "
    "WHEN provider_evidence IS DISTINCT FROM 'ABSENT' THEN TRUE ELSE "
    "CASE WHEN capability_cohort_epoch < 11 THEN TRUE "
    "WHEN COALESCE(paid_capacity_pool_key::jsonb ->> 'cloud' <> 'aws', "
    "FALSE) THEN TRUE ELSE "
    "COALESCE((((provider_evidence_payload #>> "
    "'{receipt,aws_account_id}') = (paid_capacity_pool_key::jsonb #>> "
    "'{provider_identity,aws_account_id}') AND "
    "provider_evidence_payload #>> '{receipt,client_token}' ~ "
    "'^[0-9a-f]{64}$') OR "
    "(provider_evidence_payload ->> 'probe_contract' = "
    "'aws-client-token-instance-presence-v1' AND "
    "provider_evidence_payload ->> 'result' = 'ABSENT' AND "
    "jsonb_typeof(provider_evidence_payload -> 'instances') = 'array' AND "
    "(provider_evidence_payload #>> "
    "'{provider_identity,aws_account_id}') = "
    "(paid_capacity_pool_key::jsonb #>> "
    "'{provider_identity,aws_account_id}') AND "
    "(provider_evidence_payload #>> '{provider_identity,workspace}') = "
    "(paid_capacity_pool_key::jsonb ->> 'workspace') AND "
    "(provider_evidence_payload #>> '{provider_identity,region}') = "
    "(paid_capacity_pool_key::jsonb ->> 'region') AND "
    "(provider_evidence_payload #>> '{provider_identity,zone}') = "
    "(paid_capacity_pool_key::jsonb ->> 'zone') AND "
    "(provider_evidence_payload #>> "
    "'{provider_identity,instance_type}') = "
    "(paid_capacity_pool_key::jsonb ->> 'instance_type') AND "
    "(provider_evidence_payload #> '{provider_identity,num_nodes}') = "
    "(paid_capacity_pool_key::jsonb -> 'num_nodes') AND "
    "(provider_evidence_payload #> '{provider_identity,use_spot}') = "
    "(paid_capacity_pool_key::jsonb -> 'use_spot') AND "
    "provider_evidence_payload #>> '{provider_identity,client_token}' ~ "
    "'^[0-9a-f]{64}$' AND length(provider_evidence_payload #>> "
    "'{provider_identity,cluster_name_on_cloud}') > 0)), FALSE) END END")


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('AWS paid provider census is PostgreSQL-only.')


def upgrade() -> None:
    """Install the closed AWS provider-census receipt shape."""
    _require_postgresql()
    op.drop_constraint(_PAID_RECEIPT_SCOPE_CONSTRAINT,
                       _ASSOCIATIONS,
                       type_='check')
    op.create_check_constraint(_PAID_RECEIPT_SCOPE_CONSTRAINT, _ASSOCIATIONS,
                               _PAID_RECEIPT_SCOPE_CHECK)


def downgrade() -> None:
    """Preserve AWS provider-census history across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve062 is forward-only; AWS provider-census evidence may already '
        'have been retained.')
