"""PostgreSQL schema for durable Kueue reserved-fill admissions."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()

serve_kueue_admissions_table = sqlalchemy.Table(
    'serve_kueue_admissions',
    metadata,
    sqlalchemy.Column('intent_idempotency_key',
                      sqlalchemy.Text,
                      primary_key=True),
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('unresolved_domain_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('service_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('pool_key', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('pool_epoch', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('physical_cluster_uid', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('kubernetes_context', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('accelerator', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('accelerator_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('worker_projection_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    # Copied from the immutable intent at the grant linearization point.  This
    # is the unit used by both the normal ceiling and any replacement surge.
    sqlalchemy.Column('capacity_unit', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('planned_capacity', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer),
    sqlalchemy.Column('replica_record_id', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('provider_cluster_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('association_id', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('pod_namespace', sqlalchemy.Text),
    sqlalchemy.Column('pod_name', sqlalchemy.Text),
    sqlalchemy.Column('pod_uid', sqlalchemy.Text),
    sqlalchemy.Column('pod_receipt', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('pod_receipt_sha256', sqlalchemy.Text),
    sqlalchemy.Column('observed_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('valid_until', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('admitted_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('replacement_surge_units',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('replacement_compatibility_sha256', sqlalchemy.Text),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.ForeignKeyConstraint(
        ['service_name', 'intent_idempotency_key'], [
            'serve_zero_cost_actuation_intents.service_name',
            'serve_zero_cost_actuation_intents.intent_idempotency_key'
        ],
        name='serve057_kueue_admission_intent_fk',
        ondelete='RESTRICT'),
    sqlalchemy.ForeignKeyConstraint(
        ['service_name', 'replica_id'],
        ['replicas.service_name', 'replicas.replica_id'],
        name='serve057_kueue_admission_replica_fk',
        ondelete='RESTRICT'),
    sqlalchemy.ForeignKeyConstraint(
        ['association_id'],
        ['serve_ordinary_launch_associations.association_id'],
        name='serve057_kueue_admission_association_fk',
        ondelete='RESTRICT'),
    sqlalchemy.UniqueConstraint('service_name',
                                'replica_id',
                                name='serve057_kueue_admission_replica_uq'),
    sqlalchemy.UniqueConstraint('association_id',
                                name='serve057_kueue_admission_association_uq'),
    sqlalchemy.CheckConstraint(
        "intent_idempotency_key ~ '^[0-9a-f]{64}$' AND "
        "unresolved_domain_sha256 ~ '^[0-9a-f]{64}$' AND "
        "worker_projection_sha256 ~ '^[0-9a-f]{64}$' AND "
        '(pod_receipt_sha256 IS NULL OR '
        "pod_receipt_sha256 ~ '^[0-9a-f]{64}$') AND "
        '(replacement_compatibility_sha256 IS NULL OR '
        "replacement_compatibility_sha256 ~ '^[0-9a-f]{64}$')",
        name='serve057_kueue_admission_digest_ck'),
    sqlalchemy.CheckConstraint(
        'unresolved_domain_sha256 = encode(sha256(convert_to('
        "octet_length(service_name)::text || ':' || service_name || '|' || "
        "service_lifecycle_epoch::text || '|' || "
        "octet_length(physical_cluster_uid)::text || ':' || "
        "physical_cluster_uid || '|' || "
        "octet_length(accelerator)::text || ':' || accelerator || '|' || "
        "accelerator_count::text, 'UTF8')), 'hex')",
        name='serve057_kueue_admission_domain_ck'),
    sqlalchemy.CheckConstraint(
        'service_lifecycle_epoch > 0 AND service_version > 0 AND '
        'pool_epoch > 0 AND accelerator_count > 0 AND planned_capacity > 0 '
        'AND replacement_surge_units >= 0 AND '
        'replacement_surge_units <= planned_capacity AND '
        '(replica_id IS NULL OR replica_id > 0) AND '
        '(provider_cluster_generation IS NULL OR '
        'provider_cluster_generation > 0)',
        name='serve057_kueue_admission_positive_ck'),
    sqlalchemy.CheckConstraint(
        'octet_length(service_name) > 0 AND octet_length(service_hash) > 0 '
        'AND octet_length(pool_key) > 0 AND '
        'octet_length(physical_cluster_uid) > 0 AND '
        'octet_length(kubernetes_context) > 0 AND '
        'octet_length(accelerator) > 0 AND accelerator = lower(accelerator)',
        name='serve057_kueue_admission_text_ck'),
    sqlalchemy.CheckConstraint(
        "(capacity_unit = 'physical' AND planned_capacity = 1) OR "
        "(capacity_unit = 'logical' AND "
        'planned_capacity = accelerator_count)',
        name='serve057_kueue_admission_capacity_ck'),
    sqlalchemy.CheckConstraint(
        "state IN ('INTENT_PENDING', 'POD_WAITING', 'POLICY_ADMITTED')",
        name='serve057_kueue_admission_state_ck'),
    sqlalchemy.CheckConstraint(
        'num_nonnulls(replica_id, replica_record_id, '
        'provider_cluster_generation, association_id) IN (0, 4)',
        name='serve057_kueue_admission_materialization_ck'),
    sqlalchemy.CheckConstraint(
        'num_nonnulls(pod_namespace, pod_name, pod_uid, pod_receipt, '
        'pod_receipt_sha256) IN (0, 5) AND '
        '(pod_namespace IS NULL OR '
        '(octet_length(pod_namespace) > 0 AND octet_length(pod_name) > 0 '
        'AND octet_length(pod_uid) > 0 AND '
        "jsonb_typeof(pod_receipt) = 'object' AND "
        'octet_length(pod_receipt::text) <= 65536 AND '
        'pod_receipt_sha256 = encode(sha256(convert_to('
        "pod_receipt::text, 'UTF8')), 'hex')))",
        name='serve057_kueue_admission_pod_receipt_ck'),
    sqlalchemy.CheckConstraint(
        '(replacement_surge_units = 0 AND '
        'replacement_compatibility_sha256 IS NULL) OR '
        '(replacement_surge_units > 0 AND '
        'replacement_compatibility_sha256 IS NOT NULL)',
        name='serve057_kueue_admission_surge_ck'),
    sqlalchemy.CheckConstraint(
        'updated_at >= created_at AND '
        "((state = 'INTENT_PENDING' AND pod_namespace IS NULL AND "
        'observed_at IS NULL AND valid_until IS NULL AND '
        'admitted_at IS NULL) OR '
        "(state = 'POD_WAITING' AND replica_id IS NOT NULL AND "
        'pod_namespace IS NOT NULL AND observed_at IS NOT NULL AND '
        "valid_until = observed_at + INTERVAL '15 seconds' AND "
        'admitted_at IS NULL) OR '
        "(state = 'POLICY_ADMITTED' AND replica_id IS NOT NULL AND "
        'pod_namespace IS NOT NULL AND observed_at IS NOT NULL AND '
        'valid_until IS NULL AND admitted_at IS NOT NULL AND '
        'admitted_at <= observed_at))',
        name='serve057_kueue_admission_state_shape_ck'),
)

sqlalchemy.Index(
    'uq_serve057_kueue_admission_surge',
    serve_kueue_admissions_table.c.service_name,
    unique=True,
    postgresql_where=(serve_kueue_admissions_table.c.replacement_surge_units
                      > 0))
sqlalchemy.Index('ix_serve057_kueue_admission_service_state',
                 serve_kueue_admissions_table.c.service_name,
                 serve_kueue_admissions_table.c.state)
