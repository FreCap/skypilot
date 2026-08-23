"""PostgreSQL schema for exact asynchronous request dispatch receipts."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()

serve_async_requests_table = sqlalchemy.Table(
    'serve_async_requests',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('request_key_sha256', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('intent_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('current_attempt_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('current_attempt_no', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.text('clock_timestamp()')),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.text('clock_timestamp()')),
    sqlalchemy.CheckConstraint(
        "request_key_sha256 ~ '^[0-9a-f]{64}$' AND "
        "intent_sha256 ~ '^[0-9a-f]{64}$'",
        name='serve058_async_request_digest_ck'),
    sqlalchemy.CheckConstraint(
        'current_attempt_no > 0 AND updated_at >= created_at AND '
        'octet_length(service_name) BETWEEN 1 AND 512 AND '
        'octet_length(service_hash) BETWEEN 1 AND 512',
        name='serve058_async_request_identity_ck'),
)

serve_async_request_attempts_table = sqlalchemy.Table(
    'serve_async_request_attempts',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('request_key_sha256', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('attempt_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('attempt_no', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('revision', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('dispatch_binding', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('accepted_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('terminal_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('terminal_status', sqlalchemy.Text),
    sqlalchemy.Column('processing_time_us', sqlalchemy.BigInteger),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.text('clock_timestamp()')),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.text('clock_timestamp()')),
    sqlalchemy.ForeignKeyConstraint(
        ['service_name', 'service_hash', 'request_key_sha256'], [
            serve_async_requests_table.c.service_name,
            serve_async_requests_table.c.service_hash,
            serve_async_requests_table.c.request_key_sha256,
        ],
        name='serve058_async_attempt_request_fk',
        ondelete='RESTRICT',
        deferrable=True,
        initially='DEFERRED'),
    sqlalchemy.UniqueConstraint('service_name',
                                'service_hash',
                                'request_key_sha256',
                                'attempt_no',
                                name='serve058_async_attempt_number_uq'),
    sqlalchemy.CheckConstraint(
        'attempt_no > 0 AND revision > 0 AND updated_at >= created_at',
        name='serve058_async_attempt_positive_ck'),
    sqlalchemy.CheckConstraint(
        "state IN ('REJECTED_PRE_DISPATCH', "
        "'DISPATCH_MAY_HAVE_OCCURRED', 'ACCEPTED', 'AMBIGUOUS', "
        "'SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED')",
        name='serve058_async_attempt_state_ck'),
    sqlalchemy.CheckConstraint(
        "state = 'REJECTED_PRE_DISPATCH' OR dispatch_binding IS NOT NULL",
        name='serve058_async_attempt_binding_ck'),
    sqlalchemy.CheckConstraint(
        "(state IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED') AND "
        'accepted_at IS NOT NULL AND terminal_at IS NOT NULL AND '
        'terminal_status = state AND processing_time_us IS NOT NULL AND '
        'processing_time_us >= 0) OR '
        "(state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED') AND "
        'terminal_at IS NULL AND terminal_status IS NULL AND '
        'processing_time_us IS NULL)',
        name='serve058_async_attempt_terminal_ck'),
    sqlalchemy.CheckConstraint(
        'dispatch_binding IS NULL OR '
        "(jsonb_typeof(dispatch_binding) = 'object' AND "
        'octet_length(dispatch_binding::text) <= 8192 AND '
        "dispatch_binding ?& ARRAY['schema_version', "
        "'route_contract_service_version', "
        "'selected_worker_service_version', "
        "'route_projection_generation', 'route_projection_sha256', "
        "'route_source_epoch', 'replica_id', 'replica_record_id', "
        "'projected_accelerator', 'projected_accelerator_count', "
        "'is_zero_cost', 'location', 'worker_admission'] AND "
        "dispatch_binding - ARRAY['schema_version', "
        "'route_contract_service_version', "
        "'selected_worker_service_version', "
        "'route_projection_generation', 'route_projection_sha256', "
        "'route_source_epoch', 'replica_id', 'replica_record_id', "
        "'projected_accelerator', 'projected_accelerator_count', "
        "'is_zero_cost', 'location', 'worker_admission'] = '{}'::jsonb)",
        name='serve058_async_attempt_dispatch_json_ck'),
)

# Added with ALTER TABLE by migration 058 after both sides of the cycle exist.
sqlalchemy.ForeignKeyConstraint(
    [
        serve_async_requests_table.c.service_name,
        serve_async_requests_table.c.service_hash,
        serve_async_requests_table.c.request_key_sha256,
        serve_async_requests_table.c.current_attempt_id
    ], [
        serve_async_request_attempts_table.c.service_name,
        serve_async_request_attempts_table.c.service_hash,
        serve_async_request_attempts_table.c.request_key_sha256,
        serve_async_request_attempts_table.c.attempt_id,
    ],
    name='serve058_async_request_current_attempt_fk',
    deferrable=True,
    initially='DEFERRED',
    use_alter=True)

sqlalchemy.Index('serve058_async_attempt_state_idx',
                 serve_async_request_attempts_table.c.service_name,
                 serve_async_request_attempts_table.c.service_hash,
                 serve_async_request_attempts_table.c.state,
                 serve_async_request_attempts_table.c.updated_at)
sqlalchemy.Index('serve058_async_attempt_terminal_idx',
                 serve_async_request_attempts_table.c.service_name,
                 serve_async_request_attempts_table.c.service_hash,
                 serve_async_request_attempts_table.c.terminal_at)
