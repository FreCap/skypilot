"""PostgreSQL schema for grant-before-row zero-cost actuation."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()

serve_zero_cost_actuation_intents_table = sqlalchemy.Table(
    'serve_zero_cost_actuation_intents',
    metadata,
    sqlalchemy.Column('intent_idempotency_key',
                      sqlalchemy.Text,
                      primary_key=True),
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('service_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('controller_owner', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('ordinal', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('protocol_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('policy_revision', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('reconcile_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('allocation_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('allocation_input_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('allocation_claim_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
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
    sqlalchemy.Column('service_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('pool_key', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('pool_epoch', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('physical_cluster_uid', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('kubernetes_context', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('worker_projection_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('observation_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('observation_sequence',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('ordinary_zero_cost_admission_sequence',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('valid_until_epoch', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('valid_until',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('accelerator', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('accelerator_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('capacity_unit', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('planned_capacity', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('allowed_locations',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('lease_owner', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('lease_generation',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('lease_expires_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer),
    sqlalchemy.Column('replica_record_id', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('last_error', sqlalchemy.Text),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('committed_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('terminal_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.CheckConstraint(
        "intent_idempotency_key ~ '^[0-9a-f]{64}$' AND "
        "allocation_input_sha256 ~ '^[0-9a-f]{64}$' AND "
        "reclaim_fleet_bundle_sha256 ~ '^[0-9a-f]{64}$' AND "
        "reclaim_provider_inventory_sha256 ~ '^[0-9a-f]{64}$' AND "
        "worker_projection_sha256 ~ '^[0-9a-f]{64}$'",
        name='serve052_zero_cost_intent_digest_ck'),
    sqlalchemy.CheckConstraint(
        'service_lifecycle_epoch > 0 AND service_version > 0 AND ordinal >= 0 '
        'AND '
        'protocol_version = 2 AND policy_revision > 0 AND '
        'reconcile_generation > 0 AND allocation_generation > 0 AND '
        'allocation_claim_generation > 0 AND '
        'reconciliation_gate_generation > 0 AND service_generation > 0 AND '
        'pool_epoch > 0 AND observation_generation > 0 AND '
        'observation_sequence >= 0 AND '
        'ordinary_zero_cost_admission_sequence >= 0 AND '
        'ordinary_zero_cost_admission_sequence <= observation_sequence AND '
        'accelerator_count > 0 AND planned_capacity > 0 AND '
        'lease_generation >= 0',
        name='serve052_zero_cost_intent_positive_ck'),
    sqlalchemy.CheckConstraint(
        "length(service_name) > 0 AND length(service_hash) > 0 AND "
        "length(controller_owner) > 0 AND length(pool_key) > 0 AND "
        "length(physical_cluster_uid) > 0 AND "
        "length(kubernetes_context) > 0 AND length(accelerator) > 0 AND "
        "length(reclaim_policy_revision) > 0",
        name='serve052_zero_cost_intent_text_ck'),
    sqlalchemy.CheckConstraint("capacity_unit IN ('physical', 'logical')",
                               name='serve052_zero_cost_intent_unit_ck'),
    sqlalchemy.CheckConstraint(
        "state IN ('GRANTED', 'ACTUATING', 'COMMITTED', 'RETRYABLE', "
        "'TERMINAL')",
        name='serve052_zero_cost_intent_state_ck'),
    sqlalchemy.CheckConstraint(
        "valid_until_epoch > 0 AND valid_until_epoch < 'Infinity'::"
        'double precision AND '
        'abs(extract(epoch FROM valid_until)::double precision - '
        'valid_until_epoch) < 0.000001 AND '
        'valid_until > created_at',
        name='serve052_zero_cost_intent_expiry_ck'),
    sqlalchemy.CheckConstraint(
        "((state IN ('GRANTED', 'RETRYABLE') AND lease_owner IS NULL AND "
        'lease_expires_at IS NULL AND replica_id IS NULL AND '
        'replica_record_id IS NULL AND committed_at IS NULL AND '
        'terminal_at IS NULL) OR '
        "(state = 'ACTUATING' AND lease_owner IS NOT NULL AND "
        'lease_generation > 0 AND lease_expires_at IS NOT NULL AND '
        'replica_id IS NULL AND replica_record_id IS NULL AND '
        'committed_at IS NULL AND terminal_at IS NULL) OR '
        "(state = 'COMMITTED' AND lease_owner IS NULL AND "
        'lease_expires_at IS NULL AND replica_id IS NOT NULL AND '
        'replica_record_id IS NOT NULL AND committed_at IS NOT NULL AND '
        'terminal_at IS NULL) OR '
        "(state = 'TERMINAL' AND lease_owner IS NULL AND "
        'lease_expires_at IS NULL AND replica_id IS NULL AND '
        'replica_record_id IS NULL AND committed_at IS NULL AND '
        'terminal_at IS NOT NULL))',
        name='serve052_zero_cost_intent_state_shape_ck'),
    sqlalchemy.UniqueConstraint('service_name',
                                'replica_id',
                                name='serve052_zero_cost_intent_replica_uq'),
)
sqlalchemy.Index('ix_serve052_zero_cost_intent_actionable',
                 serve_zero_cost_actuation_intents_table.c.pool_key,
                 serve_zero_cost_actuation_intents_table.c.state,
                 serve_zero_cost_actuation_intents_table.c.valid_until)
sqlalchemy.Index('ix_serve052_zero_cost_intent_service',
                 serve_zero_cost_actuation_intents_table.c.service_name,
                 serve_zero_cost_actuation_intents_table.c.state)
