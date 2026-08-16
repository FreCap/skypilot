"""SQLAlchemy record schema for durable PostgreSQL request delivery."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()
REQUESTS = sqlalchemy.Table(
    'api_requests',
    metadata,
    sqlalchemy.Column('request_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('handler_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('payload_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('payload_format', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('payload_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('producer_version', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('payload_json', postgresql.JSONB, nullable=False),
    sqlalchemy.Column('execution_class', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('status', sqlalchemy.Text, nullable=False),
    # Nullable for API008 rows upgraded in place.  Every terminal transition
    # written by API009 persists one of the closed operational-event causes;
    # a bound reducer treats a legacy NULL as ambiguous evidence.
    sqlalchemy.Column('terminal_cause', sqlalchemy.Text),
    sqlalchemy.Column('return_value', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('error', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('pid', sqlalchemy.Integer),
    sqlalchemy.Column('execution_process_start_time_ticks',
                      sqlalchemy.BigInteger),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text),
    sqlalchemy.Column('schedule_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('user_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('status_msg', sqlalchemy.Text),
    sqlalchemy.Column('should_retry', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('finished_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('file_mounts_blob_id', sqlalchemy.Text),
    sqlalchemy.Column('ignore_return_value', sqlalchemy.Boolean,
                      nullable=False),
    sqlalchemy.Column('retryable', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('execution_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('claim_token', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('worker_instance_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('controller_generation', sqlalchemy.BigInteger),
    # Immutable origin of a request submitted by one exact managed-job
    # controller slot attempt.  Legacy and ordinary requests keep the entire
    # tuple NULL; managed-job nested requests persist every member together.
    sqlalchemy.Column('managed_job_id', sqlalchemy.BigInteger),
    sqlalchemy.Column('managed_job_controller_instance_id',
                      postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('managed_job_controller_generation',
                      sqlalchemy.BigInteger),
    sqlalchemy.Column('managed_job_controller_slot_id', sqlalchemy.Integer),
    sqlalchemy.Column('managed_job_controller_slot_attempt',
                      postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('lease_expires_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('heartbeat_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('cancel_requested_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('cancel_acknowledged_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('execution_quiescence_required',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.false()),
    sqlalchemy.Column('execution_quiesced_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('execution_quiesced_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('interrupted_reason', sqlalchemy.Text),
    sqlalchemy.Column('event_context', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('resource_action_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('resource_action_attempt', sqlalchemy.Integer),
    # Immutable request-to-association correlation. Request retention is owned
    # independently by REQUEST_RETENTION_PINS, so projection can release the
    # active pin without rewriting this evidence.
    sqlalchemy.Column('ordinary_launch_association_id',
                      postgresql.UUID(as_uuid=True)),
    # API011 generic non-pool launch envelope.  The complete tuple is NULL on
    # historical ordinary-bound and unrelated requests.  Only the distinct
    # generic handler may carry the complete protocol-v2 tuple.
    sqlalchemy.Column('binding_protocol_version', sqlalchemy.Integer),
    sqlalchemy.Column('profile_kind', sqlalchemy.Text),
    sqlalchemy.Column('profile_version', sqlalchemy.Integer),
    sqlalchemy.Column('profile_digest', sqlalchemy.Text),
    sqlalchemy.Column('capability_cohort_epoch', sqlalchemy.BigInteger),
    sqlalchemy.Column('capability_profile_set_digest', sqlalchemy.Text),
    sqlalchemy.Column('receipt_protocol_version', sqlalchemy.Integer),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.CheckConstraint('pid IS NULL OR pid > 0',
                               name='ck_api_requests_pid'),
    sqlalchemy.CheckConstraint(
        'execution_process_start_time_ticks IS NULL OR '
        'execution_process_start_time_ticks > 0',
        name='ck_api_requests_process_start_time'),
    sqlalchemy.CheckConstraint(
        "((handler_name = 'sky.server.requests.ordinary_launch:launch' AND "
        'ordinary_launch_association_id IS NOT NULL AND '
        'num_nonnulls(binding_protocol_version, profile_kind, '
        'profile_version, profile_digest, capability_cohort_epoch, '
        'capability_profile_set_digest, receipt_protocol_version) = 0) OR '
        "(handler_name = 'sky.server.requests.non_pool_launch:launch' AND "
        'ordinary_launch_association_id IS NOT NULL AND '
        'num_nonnulls(binding_protocol_version, profile_kind, '
        'profile_version, profile_digest, capability_cohort_epoch, '
        'capability_profile_set_digest, receipt_protocol_version) = 7) OR '
        "(handler_name NOT IN ('sky.server.requests.ordinary_launch:launch', "
        "'sky.server.requests.non_pool_launch:launch') AND "
        'ordinary_launch_association_id IS NULL AND '
        'num_nonnulls(binding_protocol_version, profile_kind, '
        'profile_version, profile_digest, capability_cohort_epoch, '
        'capability_profile_set_digest, receipt_protocol_version) = 0))',
        name='ck_api_requests_non_pool_launch_handler'),
    sqlalchemy.CheckConstraint(
        'num_nonnulls(binding_protocol_version, profile_kind, '
        'profile_version, profile_digest, capability_cohort_epoch, '
        'capability_profile_set_digest, receipt_protocol_version) IN (0, 7)',
        name='ck_api_requests_non_pool_launch_profile_complete'),
    sqlalchemy.CheckConstraint(
        '(binding_protocol_version IS NULL OR '
        'binding_protocol_version = 2) AND '
        "(profile_kind IS NULL OR profile_kind IN ('ORDINARY_PAID', "
        "'ORDINARY_ZERO_COST', 'RESERVED_FILL', "
        "'UNKNOWN_CAPACITY_REPLACEMENT', 'COST_REBALANCE', "
        "'SYSTEM_OOM_RECOVERY')) AND "
        '(profile_version IS NULL OR profile_version = 1) AND '
        "(profile_digest IS NULL OR profile_digest ~ '^[0-9a-f]{64}$') AND "
        '(capability_cohort_epoch IS NULL OR capability_cohort_epoch > 0) AND '
        '(capability_profile_set_digest IS NULL OR '
        "capability_profile_set_digest ~ '^[0-9a-f]{64}$') AND "
        '(receipt_protocol_version IS NULL OR receipt_protocol_version = 1)',
        name='ck_api_requests_non_pool_launch_profile_values'),
    sqlalchemy.CheckConstraint(
        'num_nonnulls(managed_job_id, '
        'managed_job_controller_instance_id, '
        'managed_job_controller_generation, '
        'managed_job_controller_slot_id, '
        'managed_job_controller_slot_attempt) IN (0, 5)',
        name='ck_api_requests_managed_job_origin_complete'),
    sqlalchemy.CheckConstraint(
        '(managed_job_id IS NULL OR managed_job_id > 0) AND '
        '(managed_job_controller_generation IS NULL OR '
        'managed_job_controller_generation > 0) AND '
        '(managed_job_controller_slot_id IS NULL OR '
        'managed_job_controller_slot_id >= 0)',
        name='ck_api_requests_managed_job_origin_values'),
    sqlalchemy.CheckConstraint(
        "terminal_cause IS NULL OR (status IN ('SUCCEEDED', 'FAILED', "
        "'CANCELLED') AND terminal_cause IN ('handler_succeeded', "
        "'handler_failed', 'dispatcher_submit_failed', 'explicit_cancel', "
        "'coroutine_disconnected', 'graceful_shutdown_retry', "
        "'compatibility_restart', 'controller_leadership_lost', "
        "'execution_lease_expired', 'precondition_failed', "
        "'controller_reservation_conflict'))",
        name='ck_api_requests_terminal_cause'),
)
sqlalchemy.Index('ix_api_requests_managed_job_attempt',
                 REQUESTS.c.managed_job_id,
                 REQUESTS.c.managed_job_controller_instance_id,
                 REQUESTS.c.managed_job_controller_generation,
                 REQUESTS.c.managed_job_controller_slot_id,
                 REQUESTS.c.managed_job_controller_slot_attempt,
                 postgresql_where=REQUESTS.c.managed_job_id.is_not(None))
REQUEST_RETENTION_PINS = sqlalchemy.Table(
    'api_request_retention_pins',
    metadata,
    sqlalchemy.Column('pin_kind', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('pin_id', postgresql.UUID(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('request_id',
                      sqlalchemy.Text,
                      sqlalchemy.ForeignKey(
                          'api_requests.request_id',
                          name='fk_api_request_retention_pins_request',
                          ondelete='RESTRICT'),
                      nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.CheckConstraint('char_length(pin_kind) BETWEEN 1 AND 128',
                               name='ck_api_request_retention_pins_kind'),
)
sqlalchemy.Index('ix_api_request_retention_pins_request',
                 REQUEST_RETENTION_PINS.c.request_id)
RESOURCE_ACTIONS = sqlalchemy.Table(
    'api_resource_actions',
    metadata,
    sqlalchemy.Column('action_id',
                      postgresql.UUID(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('domain', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('resource_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('resource_identity', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('desired_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('action_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('immutable_spec',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('immutable_spec_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('kernel_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('current_attempt', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('next_attempt_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('last_result', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('last_result_sha256', sqlalchemy.Text),
    sqlalchemy.Column('terminal_disposition', sqlalchemy.Text),
    sqlalchemy.Column('revision', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('terminal_at', sqlalchemy.DateTime(timezone=True)),
)
RESOURCE_ACTION_ATTEMPTS = sqlalchemy.Table(
    'api_resource_action_attempts',
    metadata,
    sqlalchemy.Column('action_id',
                      postgresql.UUID(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('attempt', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_input_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider_operation_id', sqlalchemy.Text),
    sqlalchemy.Column('mutation_boundary', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider_io_boundary',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='NOT_STARTED'),
    sqlalchemy.Column('provider_progress', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('provider_progress_sha256', sqlalchemy.Text),
    sqlalchemy.Column('provider_progress_revision',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('typed_outcome', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('typed_outcome_sha256', sqlalchemy.Text),
    sqlalchemy.Column('request_terminal_state', sqlalchemy.Text),
    sqlalchemy.Column('admitted_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('settled_at', sqlalchemy.DateTime(timezone=True)),
)
QUEUE = sqlalchemy.Table(
    'api_request_queue',
    metadata,
    sqlalchemy.Column('request_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('schedule_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('priority', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('available_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('enqueued_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('sequence',
                      sqlalchemy.BigInteger,
                      sqlalchemy.Identity(),
                      nullable=False,
                      unique=True),
    sqlalchemy.Column('ignore_return_value', sqlalchemy.Boolean,
                      nullable=False),
    sqlalchemy.Column('retryable', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('precondition_type', sqlalchemy.Text),
    sqlalchemy.Column('precondition_payload',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('precondition_deadline',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('precondition_attempts',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('delivery_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('claim_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
)
STORE_METADATA = sqlalchemy.Table(
    'api_request_store_metadata',
    metadata,
    sqlalchemy.Column('key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('value', postgresql.JSONB, nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
)
SERVER_INSTANCES = sqlalchemy.Table(
    'api_server_instances',
    metadata,
    sqlalchemy.Column('instance_id',
                      postgresql.UUID(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('role', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('pod_name', sqlalchemy.Text),
    sqlalchemy.Column('pod_uid', sqlalchemy.Text),
    sqlalchemy.Column('pod_ip', sqlalchemy.Text),
    sqlalchemy.Column('version', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('started_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('heartbeat_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('draining_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('ready', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('health_detail',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('supported_handlers',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('supported_payload_versions',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('request_storage_backend',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=sqlalchemy.text("'unknown'")),
    sqlalchemy.Column('request_queue_backend',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=sqlalchemy.text("'unknown'")),
    sqlalchemy.Column('execution_quiescence_capable',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.false()),
    sqlalchemy.Column('ordinary_launch_binding_capable',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.false()),
    # API011 capability is independent from API009's ordinary-only bit. A
    # process participates only with a complete exact profile-set tuple.
    sqlalchemy.Column('non_pool_launch_binding_capable',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.false()),
    sqlalchemy.Column('non_pool_launch_binding_protocol_version',
                      sqlalchemy.Integer),
    sqlalchemy.Column('non_pool_launch_capability_profile_set_digest',
                      sqlalchemy.Text),
    sqlalchemy.Column('non_pool_launch_capability_cohort_epoch',
                      sqlalchemy.BigInteger),
    sqlalchemy.Column('non_pool_launch_receipt_protocol_version',
                      sqlalchemy.Integer),
    sqlalchemy.CheckConstraint(
        '((NOT non_pool_launch_binding_capable AND '
        'num_nonnulls(non_pool_launch_binding_protocol_version, '
        'non_pool_launch_capability_profile_set_digest, '
        'non_pool_launch_capability_cohort_epoch, '
        'non_pool_launch_receipt_protocol_version) = 0) OR '
        '(non_pool_launch_binding_capable AND '
        'num_nonnulls(non_pool_launch_binding_protocol_version, '
        'non_pool_launch_capability_profile_set_digest, '
        'non_pool_launch_capability_cohort_epoch, '
        'non_pool_launch_receipt_protocol_version) = 4))',
        name='ck_api_server_instances_non_pool_launch_capability_complete'),
    sqlalchemy.CheckConstraint(
        '(non_pool_launch_binding_protocol_version IS NULL OR '
        'non_pool_launch_binding_protocol_version = 2) AND '
        '(non_pool_launch_capability_profile_set_digest IS NULL OR '
        "non_pool_launch_capability_profile_set_digest ~ '^[0-9a-f]{64}$') "
        'AND (non_pool_launch_capability_cohort_epoch IS NULL OR '
        'non_pool_launch_capability_cohort_epoch > 0) AND '
        '(non_pool_launch_receipt_protocol_version IS NULL OR '
        'non_pool_launch_receipt_protocol_version = 1)',
        name='ck_api_server_instances_non_pool_launch_capability_values'),
)
CONTROLLER_LEADERSHIP = sqlalchemy.Table(
    'api_controller_leadership',
    metadata,
    sqlalchemy.Column('leadership_key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('generation', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('lock_backend_pid', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('generation_lock_key',
                      sqlalchemy.BigInteger,
                      nullable=False),
    # API009 controller writers leave this NULL during a rolling migration.
    # API010 leaders always bind a fresh 256-bit capability digest atomically
    # with generation advancement, and origin admission rejects NULL.
    sqlalchemy.Column('origin_capability_sha256', postgresql.BYTEA),
    sqlalchemy.Column('acquired_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('heartbeat_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('released_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.CheckConstraint(
        'origin_capability_sha256 IS NULL OR '
        'octet_length(origin_capability_sha256) = 32',
        name='ck_api_controller_leadership_capability_sha256'),
)
CONTROLLER_ACTION_RESERVATIONS = sqlalchemy.Table(
    'api_controller_action_reservations',
    metadata,
    sqlalchemy.Column('logical_action_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('resource_identity', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('action_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('controller_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('controller_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('provider_operation_id', sqlalchemy.Text),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('reconciliation_at', sqlalchemy.DateTime(timezone=True)),
)
PG_LOCKS = sqlalchemy.table(
    'pg_locks',
    sqlalchemy.column('locktype', sqlalchemy.Text),
    sqlalchemy.column('pid', sqlalchemy.Integer),
    sqlalchemy.column('classid'),
    sqlalchemy.column('objid'),
    sqlalchemy.column('objsubid', sqlalchemy.Integer),
    sqlalchemy.column('mode', sqlalchemy.Text),
    sqlalchemy.column('granted', sqlalchemy.Boolean),
    schema='pg_catalog',
)
