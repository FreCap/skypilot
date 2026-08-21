"""PostgreSQL schema for short-lived reserved-fill provider proofs."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

PROVIDER_PROOF_PAYLOAD_STORAGE_MAX_BYTES = 64 * 1024

metadata = sqlalchemy.MetaData()

serve_reserved_fill_reclaim_provider_proofs_table = sqlalchemy.Table(
    'serve_reserved_fill_reclaim_provider_proofs',
    metadata,
    sqlalchemy.Column('receipt_nonce', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('reconciliation_gate_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('reclaim_fleet_bundle_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('reclaim_policy_revision',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('reclaim_provider_inventory_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('kubernetes_context', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('proof_schema_version',
                      sqlalchemy.Integer,
                      nullable=False),
    sqlalchemy.Column('proof_payload',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('proof_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('completed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.CheckConstraint(
        "receipt_nonce ~ '^[0-9a-f]{64}$' AND "
        "reclaim_fleet_bundle_sha256 ~ '^[0-9a-f]{64}$' AND "
        "reclaim_provider_inventory_sha256 ~ '^[0-9a-f]{64}$' AND "
        "proof_sha256 ~ '^[0-9a-f]{64}$'",
        name='serve054_reclaim_proof_digest_ck'),
    sqlalchemy.CheckConstraint(
        'reconciliation_gate_generation > 0 AND proof_schema_version > 0',
        name='serve054_reclaim_proof_positive_ck'),
    sqlalchemy.CheckConstraint(
        'octet_length(reclaim_policy_revision) BETWEEN 1 AND 1024 AND '
        'octet_length(kubernetes_context) BETWEEN 1 AND 1024',
        name='serve054_reclaim_proof_text_ck'),
    sqlalchemy.CheckConstraint(
        "jsonb_typeof(proof_payload) = 'object' AND "
        'octet_length(proof_payload::text) <= '
        f'{PROVIDER_PROOF_PAYLOAD_STORAGE_MAX_BYTES}',
        name='serve054_reclaim_proof_payload_ck'),
    sqlalchemy.UniqueConstraint('reconciliation_gate_generation',
                                'reclaim_fleet_bundle_sha256',
                                'reclaim_policy_revision',
                                'reclaim_provider_inventory_sha256',
                                'kubernetes_context',
                                name='serve054_reclaim_proof_authority_uq'),
)
