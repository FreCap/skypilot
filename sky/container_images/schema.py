"""PostgreSQL schema for managed container image distribution.

The image control plane deliberately owns a small, closed set of tables.  This
module is the runtime SQLAlchemy description of those tables.  Migration 023 is
kept literal and independent from this metadata so an unrelated runtime import
cannot change an already-reviewed database migration.
"""

import sqlalchemy

metadata = sqlalchemy.MetaData()

PUBLICATION_STATES = ('PENDING', 'INSPECTING', 'READY', 'FAILED')
LOCATION_STATES = ('PENDING', 'COPYING', 'VERIFYING', 'READY', 'FAILED',
                   'MISSING', 'EVICTING', 'EVICTED')
OPERATION_STATES = ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')
PROFILE_STATES = ('QUALIFYING', 'ACTIVE', 'FAILED', 'SUPERSEDED', 'RETIRED')
SHARD_STATES = ('PENDING', 'READY', 'FULL', 'DRIFTED', 'DISABLED')
DEMAND_STATES = ('WARMING', 'READY', 'FAILED', 'SUPERSEDED', 'RELEASED')
WORKER_KINDS = ('COPY', 'LIFECYCLE', 'CANARY')
LOCATION_LEASE_KINDS = ('COPY', 'VERIFY', 'EVICT', 'RECONCILE')


def _one_of(column: str, values: tuple[str, ...],
            name: str) -> sqlalchemy.CheckConstraint:
    rendered = ', '.join(f"'{value}'" for value in values)
    return sqlalchemy.CheckConstraint(f'{column} IN ({rendered})', name=name)


catalog = sqlalchemy.Table(
    'container_image_catalog',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('authority_id',
                      sqlalchemy.Text,
                      nullable=False,
                      unique=True),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
)

profile_revisions = sqlalchemy.Table(
    'container_image_profile_revisions',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('profile', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('revision', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('desired_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('config_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('config_json', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('terraform_hash', sqlalchemy.Text),
    sqlalchemy.Column('physical_manifest_hash', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('attestations_json',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='{}'),
    sqlalchemy.Column('attestations_hash', sqlalchemy.Text),
    sqlalchemy.Column('qualified_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('failed_code', sqlalchemy.Text),
    sqlalchemy.Column('canary_window_day', sqlalchemy.Text),
    sqlalchemy.Column('canary_reserved_microusd',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('max_daily_canary_microusd',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.UniqueConstraint('workspace',
                                'profile',
                                'revision',
                                name='uq_container_image_profile_revision'),
    sqlalchemy.UniqueConstraint('workspace',
                                'profile',
                                'desired_generation',
                                name='uq_container_image_profile_generation'),
    _one_of('state', PROFILE_STATES,
            'ck_container_image_profile_revision_state'),
    sqlalchemy.CheckConstraint('revision > 0 AND desired_generation > 0',
                               name='ck_container_image_profile_positive'),
    sqlalchemy.CheckConstraint(
        'canary_reserved_microusd >= 0 AND max_daily_canary_microusd >= 0 '
        'AND canary_reserved_microusd <= max_daily_canary_microusd',
        name='ck_container_image_profile_canary_budget'),
    sqlalchemy.Index('uq_container_image_profile_desired',
                     'workspace',
                     'profile',
                     unique=True,
                     postgresql_where=sqlalchemy.text("state = 'QUALIFYING'")),
    sqlalchemy.Index('uq_container_image_profile_active',
                     'workspace',
                     'profile',
                     unique=True,
                     postgresql_where=sqlalchemy.text("state = 'ACTIVE'")),
    sqlalchemy.Index('ix_container_image_profile_state', 'state', 'updated_at',
                     'id'),
)

operations = sqlalchemy.Table(
    'container_image_operations',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('authority_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('scope', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('actor_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('idempotency_key', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('result_kind', sqlalchemy.Text),
    sqlalchemy.Column('result_id', sqlalchemy.Text),
    sqlalchemy.Column('result_json', sqlalchemy.Text),
    sqlalchemy.Column('error_code', sqlalchemy.Text),
    sqlalchemy.Column('lease_token', sqlalchemy.Text),
    sqlalchemy.Column('lease_expires_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('child_launch_id', sqlalchemy.Text),
    sqlalchemy.Column('teardown_deadline', sqlalchemy.BigInteger),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('terminal_expires_at', sqlalchemy.BigInteger),
    sqlalchemy.UniqueConstraint(
        'authority_id',
        'scope',
        'actor_hash',
        'kind',
        'idempotency_key',
        name='uq_container_image_operation_idempotency'),
    _one_of('state', OPERATION_STATES, 'ck_container_image_operation_state'),
    sqlalchemy.CheckConstraint("length(idempotency_key) BETWEEN 16 AND 128",
                               name='ck_container_image_operation_key_length'),
    sqlalchemy.CheckConstraint(
        "(kind = 'PROFILE_CANARY' AND state = 'RUNNING' AND lease_token IS "
        "NOT NULL AND lease_expires_at IS NOT NULL AND teardown_deadline IS "
        "NOT NULL) OR NOT (lease_token IS NOT NULL OR lease_expires_at IS "
        "NOT NULL OR child_launch_id IS NOT NULL OR teardown_deadline IS NOT "
        "NULL)",
        name='ck_container_image_operation_canary_lease'),
    sqlalchemy.CheckConstraint(
        "(state IN ('SUCCEEDED', 'FAILED') AND terminal_expires_at IS NOT "
        "NULL) OR (state IN ('PENDING', 'RUNNING') AND terminal_expires_at IS "
        "NULL)",
        name='ck_container_image_operation_terminal_expiry'),
    sqlalchemy.Index('ix_container_image_operations_lookup', 'scope',
                     'updated_at', 'id'),
    sqlalchemy.Index('ix_container_image_operations_canary_queue',
                     'state',
                     'lease_expires_at',
                     'id',
                     postgresql_where=sqlalchemy.text(
                         "kind = 'PROFILE_CANARY' AND state = 'RUNNING'")),
    sqlalchemy.Index(
        'ix_container_image_operations_expiry',
        'terminal_expires_at',
        'id',
        postgresql_where=sqlalchemy.text('terminal_expires_at IS NOT NULL')),
)

images = sqlalchemy.Table(
    'container_images',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('platform', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('config_digest', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('manifest_media_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('manifest_size_bytes',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('declared_size_bytes',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('creator_user_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('producer_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('producer_spec_hash', sqlalchemy.Text),
    sqlalchemy.Column('builder_version', sqlalchemy.Text),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.UniqueConstraint('workspace',
                                'runtime_digest',
                                'platform',
                                name='uq_container_images_runtime_identity'),
    sqlalchemy.CheckConstraint(
        'manifest_size_bytes >= 0 AND declared_size_bytes >= 0',
        name='ck_container_images_nonnegative_sizes'),
    sqlalchemy.Index('ix_container_images_workspace_created', 'workspace',
                     'created_at', 'id'),
)

sources = sqlalchemy.Table(
    'container_image_sources',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('image_id',
                      sqlalchemy.Text,
                      sqlalchemy.ForeignKey('container_images.id'),
                      nullable=False),
    sqlalchemy.Column('source_ref', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_root_digest', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_root_media_type', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('requested_platform', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('selected_child_digest', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_auth_binding_id', sqlalchemy.Text),
    sqlalchemy.Column('source_auth_fingerprint', sqlalchemy.Text),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.UniqueConstraint('workspace',
                                'source_ref',
                                'requested_platform',
                                name='uq_container_image_source_selection'),
    sqlalchemy.Index('ix_container_image_sources_image', 'image_id',
                     'created_at', 'id'),
)

provider_budgets = sqlalchemy.Table(
    'container_image_provider_budgets',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('provider', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('partition', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('account', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('region', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('api_family', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('applied_rate_milli',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('burst_milli', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('tokens_milli', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('refilled_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('blocked_until', sqlalchemy.BigInteger),
    sqlalchemy.Column('throttle_count',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.UniqueConstraint('provider',
                                'partition',
                                'account',
                                'region',
                                'api_family',
                                name='uq_container_image_provider_budget'),
    sqlalchemy.CheckConstraint(
        'applied_rate_milli > 0 AND burst_milli > 0 AND tokens_milli >= 0 '
        'AND tokens_milli <= burst_milli',
        name='ck_container_image_provider_budget_tokens'),
    sqlalchemy.Index('ix_container_image_provider_budgets_blocked',
                     'blocked_until', 'id'),
)

registry_shards = sqlalchemy.Table(
    'container_image_registry_shards',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('profile', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('target_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('partition', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('account', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('region', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('shard_generation', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('shard_index', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('physical_fingerprint', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('registry', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('repository_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('repository_arn', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('max_manifests', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('max_declared_bytes',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('reserved_manifests',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('reserved_declared_bytes',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('observed_manifests',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('max_in_flight', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('in_flight',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('qualified_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('last_dispatch_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('inventory_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('inventory_cursor', sqlalchemy.Text),
    sqlalchemy.Column('inventory_started_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('inventory_completed_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('inventory_lease_token', sqlalchemy.Text),
    sqlalchemy.Column('inventory_lease_expires_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.UniqueConstraint('workspace',
                                'profile',
                                'target_id',
                                'shard_generation',
                                'shard_index',
                                name='uq_container_image_registry_shard_slot'),
    sqlalchemy.UniqueConstraint('physical_fingerprint',
                                name='uq_container_image_registry_physical'),
    _one_of('state', SHARD_STATES, 'ck_container_image_registry_shard_state'),
    sqlalchemy.CheckConstraint(
        'shard_generation >= 0 AND shard_index >= 0 AND max_manifests > 0 '
        'AND max_declared_bytes > 0 AND reserved_manifests >= 0 AND '
        'reserved_manifests <= max_manifests AND reserved_declared_bytes >= 0 '
        'AND reserved_declared_bytes <= max_declared_bytes AND '
        'observed_manifests >= 0 AND max_in_flight > 0 AND in_flight >= 0 '
        'AND in_flight <= max_in_flight',
        name='ck_container_image_registry_shard_capacity'),
    sqlalchemy.CheckConstraint(
        '(inventory_lease_token IS NULL AND inventory_lease_expires_at IS '
        'NULL) OR (inventory_lease_token IS NOT NULL AND '
        'inventory_lease_expires_at IS NOT NULL)',
        name='ck_container_image_registry_inventory_lease'),
    sqlalchemy.Index('ix_container_image_registry_shard_dispatch', 'workspace',
                     'profile', 'target_id', 'state', 'last_dispatch_at', 'id'),
    sqlalchemy.Index('ix_container_image_registry_shard_inventory', 'state',
                     'inventory_lease_expires_at', 'id'),
)

locations = sqlalchemy.Table(
    'container_image_locations',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('image_id',
                      sqlalchemy.Text,
                      sqlalchemy.ForeignKey('container_images.id'),
                      nullable=False),
    sqlalchemy.Column(
        'shard_id',
        sqlalchemy.Text,
        sqlalchemy.ForeignKey('container_image_registry_shards.id'),
        nullable=False),
    sqlalchemy.Column('target_fingerprint', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('physical_fingerprint', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('canonical', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('canonical_location_id', sqlalchemy.Text,
                      sqlalchemy.ForeignKey('container_image_locations.id')),
    sqlalchemy.Column('target_ref', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('lease_kind', sqlalchemy.Text),
    sqlalchemy.Column('lease_token', sqlalchemy.Text),
    sqlalchemy.Column('lease_expires_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('attempt_count',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('next_retry_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('error_code', sqlalchemy.Text),
    sqlalchemy.Column('last_verified_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('last_used_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('inventory_epoch_seen', sqlalchemy.BigInteger),
    sqlalchemy.Column('reserved_declared_bytes',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.UniqueConstraint('image_id',
                                'target_fingerprint',
                                'runtime_digest',
                                name='uq_container_image_location_target'),
    sqlalchemy.UniqueConstraint('shard_id',
                                'target_ref',
                                name='uq_container_image_location_target_ref'),
    _one_of('state', LOCATION_STATES, 'ck_container_image_location_state'),
    sqlalchemy.CheckConstraint(
        '(canonical IS TRUE AND canonical_location_id IS NULL) OR '
        '(canonical IS FALSE AND canonical_location_id IS NOT NULL)',
        name='ck_container_image_location_canonical_relation'),
    sqlalchemy.CheckConstraint(
        "(state IN ('COPYING', 'VERIFYING', 'EVICTING') AND lease_kind IS "
        "NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT "
        "NULL) OR (state NOT IN ('COPYING', 'VERIFYING', 'EVICTING') AND "
        "lease_kind IS NULL AND lease_token IS NULL AND lease_expires_at IS "
        "NULL)",
        name='ck_container_image_location_lease'),
    sqlalchemy.CheckConstraint(
        "lease_kind IS NULL OR lease_kind IN ('COPY', 'VERIFY', 'EVICT')",
        name='ck_container_image_location_lease_kind'),
    sqlalchemy.CheckConstraint(
        "canonical IS FALSE OR state NOT IN ('EVICTING', 'EVICTED')",
        name='ck_container_image_location_canonical_permanent'),
    sqlalchemy.CheckConstraint(
        'attempt_count >= 0 AND reserved_declared_bytes >= 0',
        name='ck_container_image_location_nonnegative'),
    sqlalchemy.Index('ix_container_image_locations_queue', 'state',
                     'next_retry_at', 'lease_expires_at', 'id'),
    sqlalchemy.Index('ix_container_image_locations_shard_queue', 'shard_id',
                     'state', 'next_retry_at', 'updated_at', 'id'),
    sqlalchemy.Index('ix_container_image_locations_shard_readiness', 'shard_id',
                     'state', 'updated_at', 'id'),
    sqlalchemy.Index('ix_container_image_locations_canonical',
                     'canonical_location_id', 'state', 'id'),
    sqlalchemy.Index('ix_container_image_locations_artifact', 'image_id',
                     'created_at', 'id'),
    sqlalchemy.Index('ix_container_image_locations_failed_canonical',
                     'updated_at',
                     'id',
                     postgresql_where=sqlalchemy.text(
                         "canonical IS TRUE AND state = 'FAILED'")),
)

publications = sqlalchemy.Table(
    'container_image_publications',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('operation_id',
                      sqlalchemy.Text,
                      sqlalchemy.ForeignKey('container_image_operations.id'),
                      nullable=False),
    sqlalchemy.Column(
        'profile_revision_id',
        sqlalchemy.Text,
        sqlalchemy.ForeignKey('container_image_profile_revisions.id'),
        nullable=False),
    sqlalchemy.Column('requested_release', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('reservation_active',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.true()),
    sqlalchemy.Column('source_ref', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_root_digest', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('requested_platform', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_auth_binding_id', sqlalchemy.Text),
    sqlalchemy.Column('source_auth_fingerprint', sqlalchemy.Text),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('inspection_lease_token', sqlalchemy.Text),
    sqlalchemy.Column('inspection_lease_expires_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('attempt_count',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('next_retry_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('error_code', sqlalchemy.Text),
    sqlalchemy.Column('image_id', sqlalchemy.Text,
                      sqlalchemy.ForeignKey('container_images.id')),
    sqlalchemy.Column('source_id', sqlalchemy.Text,
                      sqlalchemy.ForeignKey('container_image_sources.id')),
    sqlalchemy.Column('canonical_location_id', sqlalchemy.Text,
                      sqlalchemy.ForeignKey('container_image_locations.id')),
    sqlalchemy.Column('reservation_expires_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('record_expires_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
    _one_of('state', PUBLICATION_STATES,
            'ck_container_image_publication_state'),
    sqlalchemy.CheckConstraint(
        "(state = 'INSPECTING' AND inspection_lease_token IS NOT NULL AND "
        "inspection_lease_expires_at IS NOT NULL) OR (state <> 'INSPECTING' "
        "AND inspection_lease_token IS NULL AND inspection_lease_expires_at "
        "IS NULL)",
        name='ck_container_image_publication_inspection_lease'),
    sqlalchemy.CheckConstraint(
        '(canonical_location_id IS NULL AND image_id IS NULL AND source_id IS '
        'NULL) OR (canonical_location_id IS NOT NULL AND image_id IS NOT NULL '
        'AND source_id IS NOT NULL)',
        name='ck_container_image_publication_binding'),
    sqlalchemy.CheckConstraint(
        "state <> 'READY' OR (reservation_active IS TRUE AND image_id IS NOT "
        "NULL AND canonical_location_id IS NOT NULL)",
        name='ck_container_image_publication_ready'),
    sqlalchemy.Index(
        'uq_container_image_publication_release_reservation',
        'workspace',
        'requested_release',
        unique=True,
        postgresql_where=sqlalchemy.text('reservation_active IS TRUE')),
    sqlalchemy.Index('ix_container_image_publications_inspection_queue',
                     'state', 'next_retry_at', 'inspection_lease_expires_at',
                     'id'),
    sqlalchemy.Index('ix_container_image_publications_canonical_queue',
                     'canonical_location_id', 'state', 'id'),
    sqlalchemy.Index('ix_container_image_publications_image', 'image_id',
                     'created_at', 'id'),
    sqlalchemy.Index('ix_container_image_publications_ready_release',
                     'workspace',
                     'requested_release',
                     'image_id',
                     postgresql_where=sqlalchemy.text("state = 'READY'")),
    sqlalchemy.Index(
        'ix_container_image_publications_expiry',
        'record_expires_at',
        'id',
        postgresql_where=sqlalchemy.text('record_expires_at IS NOT NULL')),
)

demands = sqlalchemy.Table(
    'container_image_demands',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('authority_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('consumer_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('consumer_owner', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text),
    sqlalchemy.Column('consumer_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('target_key', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('owner_epoch', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('retry_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('image_id',
                      sqlalchemy.Text,
                      sqlalchemy.ForeignKey('container_images.id'),
                      nullable=False),
    sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column(
        'profile_revision_id',
        sqlalchemy.Text,
        sqlalchemy.ForeignKey('container_image_profile_revisions.id'),
        nullable=False),
    sqlalchemy.Column('target_fingerprint', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('location_id',
                      sqlalchemy.Text,
                      sqlalchemy.ForeignKey('container_image_locations.id'),
                      nullable=False),
    sqlalchemy.Column('placement_json', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('pull_plan_json', sqlalchemy.Text),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('error_code', sqlalchemy.Text),
    sqlalchemy.Column('consumer_attached',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.false()),
    sqlalchemy.Column('first_terminal_observed_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('last_terminal_observed_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('terminal_observation_count',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('terminal_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('expires_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.UniqueConstraint('workspace',
                                'consumer_kind',
                                'consumer_owner',
                                'consumer_generation',
                                'target_key',
                                name='uq_container_image_demand_identity'),
    _one_of('state', DEMAND_STATES, 'ck_container_image_demand_state'),
    sqlalchemy.CheckConstraint(
        'consumer_generation >= 0 AND owner_epoch >= 0 AND retry_epoch >= 0 '
        'AND terminal_observation_count >= 0',
        name='ck_container_image_demand_nonnegative'),
    sqlalchemy.CheckConstraint(
        "(state = 'READY' AND pull_plan_json IS NOT NULL) OR state <> 'READY'",
        name='ck_container_image_demand_ready_plan'),
    sqlalchemy.Index('ix_container_image_demands_location_fence', 'location_id',
                     'state', 'id'),
    sqlalchemy.Index('ix_container_image_demands_consumer', 'workspace',
                     'consumer_kind', 'consumer_owner', 'consumer_generation',
                     'target_key'),
    sqlalchemy.Index('ix_container_image_demands_owner_epoch', 'consumer_kind',
                     'owner_epoch', 'state'),
    sqlalchemy.Index(
        'ix_container_image_demands_cluster_request',
        'request_id',
        'state',
        'id',
        postgresql_where=sqlalchemy.text(
            "consumer_kind = 'cluster' AND consumer_attached IS false AND "
            'request_id IS NOT NULL')),
    sqlalchemy.Index('ix_container_image_demands_terminal', 'state',
                     'expires_at', 'id'),
    sqlalchemy.Index('ix_container_image_demands_reconcile',
                     'state',
                     'updated_at',
                     'id',
                     postgresql_where=sqlalchemy.text(
                         "state IN ('WARMING', 'READY', 'FAILED')")),
)

consumer_watermarks = sqlalchemy.Table(
    'container_image_consumer_watermarks',
    metadata,
    sqlalchemy.Column('workspace', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('consumer_kind', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('consumer_owner', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('controller_epoch', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('controller_sequence', sqlalchemy.BigInteger),
    sqlalchemy.Column('owner_epoch', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('max_seen_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('max_terminal_generation',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='-1'),
    sqlalchemy.Column('owner_deleted_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.CheckConstraint(
        '(controller_sequence IS NULL OR controller_sequence >= 0) AND '
        'owner_epoch >= 0 AND max_seen_generation >= 0 AND '
        'max_terminal_generation >= -1 AND '
        'max_terminal_generation <= max_seen_generation',
        name='ck_container_image_consumer_watermark_generation'),
    sqlalchemy.CheckConstraint(
        'length(controller_epoch) BETWEEN 1 AND 1024',
        name='ck_container_image_consumer_controller_epoch'),
    sqlalchemy.Index(
        'ix_container_image_consumer_watermarks_compaction',
        'owner_deleted_at',
        postgresql_where=sqlalchemy.text('owner_deleted_at IS NOT NULL')),
)

workers = sqlalchemy.Table(
    'container_image_workers',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('version', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('started_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('heartbeat_at', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('last_success_at', sqlalchemy.BigInteger),
    sqlalchemy.Column('in_flight',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('max_in_flight', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column(
        'grant_budget_id', sqlalchemy.Text,
        sqlalchemy.ForeignKey('container_image_provider_budgets.id')),
    sqlalchemy.Column('grant_tokens_milli',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('grant_expires_at', sqlalchemy.BigInteger),
    _one_of('kind', WORKER_KINDS, 'ck_container_image_worker_kind'),
    sqlalchemy.CheckConstraint(
        'in_flight >= 0 AND max_in_flight > 0 AND in_flight <= max_in_flight '
        'AND grant_tokens_milli >= 0',
        name='ck_container_image_worker_capacity'),
    sqlalchemy.CheckConstraint(
        '(grant_budget_id IS NULL AND grant_tokens_milli = 0 AND '
        'grant_expires_at IS NULL) OR (grant_budget_id IS NOT NULL AND '
        'grant_tokens_milli > 0 AND grant_expires_at IS NOT NULL)',
        name='ck_container_image_worker_grant'),
    sqlalchemy.Index('ix_container_image_workers_kind_heartbeat', 'kind',
                     'heartbeat_at', 'id'),
)

TABLES = (
    catalog,
    profile_revisions,
    operations,
    images,
    sources,
    publications,
    provider_budgets,
    registry_shards,
    locations,
    demands,
    consumer_watermarks,
    workers,
)

TABLE_NAMES = tuple(table.name for table in TABLES)
