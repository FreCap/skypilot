"""SQLAlchemy schema objects for the global user state database."""

import sqlalchemy
from sqlalchemy.ext import declarative

from sky.skylet import constants

Base = declarative.declarative_base()

config_table = sqlalchemy.Table(
    'config',
    Base.metadata,
    sqlalchemy.Column('key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('value', sqlalchemy.Text),
)

user_table = sqlalchemy.Table(
    'users',
    Base.metadata,
    sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('name', sqlalchemy.Text),
    sqlalchemy.Column('password', sqlalchemy.Text),
    sqlalchemy.Column('created_at', sqlalchemy.Integer),
    sqlalchemy.Column('type', sqlalchemy.Text, server_default=None),
    # User-set default workspace; null when unset. Resolution and RBAC
    # validation are handled in sky/workspaces/; this column is the
    # persisted value only.
    sqlalchemy.Column('preferred_workspace',
                      sqlalchemy.Text,
                      server_default=None),
)

auth_session_table = sqlalchemy.Table(
    'auth_sessions',
    Base.metadata,
    sqlalchemy.Column('code_challenge', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('token', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_at', sqlalchemy.Float, nullable=False),
)

cluster_table = sqlalchemy.Table(
    'clusters',
    Base.metadata,
    sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('launched_at', sqlalchemy.Integer),
    sqlalchemy.Column('handle', sqlalchemy.LargeBinary),
    sqlalchemy.Column('last_use', sqlalchemy.Text),
    sqlalchemy.Column('status', sqlalchemy.Text),
    sqlalchemy.Column('autostop', sqlalchemy.Integer, server_default='-1'),
    sqlalchemy.Column('to_down', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('metadata', sqlalchemy.Text, server_default='{}'),
    sqlalchemy.Column('owner', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('cluster_hash', sqlalchemy.Text, server_default=None),
    # Write-once provider identity for action-aware launches. Ordinary cluster
    # persistence omits this column so it cannot initialize, clear, or replace
    # an existing commitment.
    sqlalchemy.Column('cluster_record_uuid',
                      sqlalchemy.Uuid,
                      nullable=True,
                      server_default=None),
    sqlalchemy.Column('storage_mounts_metadata',
                      sqlalchemy.LargeBinary,
                      server_default=None),
    sqlalchemy.Column('cluster_ever_up', sqlalchemy.Integer,
                      server_default='0'),
    sqlalchemy.Column('status_updated_at',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('config_hash', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('user_hash', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('workspace',
                      sqlalchemy.Text,
                      server_default=constants.SKYPILOT_DEFAULT_WORKSPACE),
    sqlalchemy.Column('last_creation_yaml',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('last_creation_command',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('is_managed', sqlalchemy.Integer, server_default='0'),
    # Exact managed-image owner bound by the INIT transaction. A false known bit
    # means pre-binding or indeterminate; true plus NULL fields means no owner.
    sqlalchemy.Column('container_image_binding_known',
                      sqlalchemy.Integer,
                      server_default='0'),
    sqlalchemy.Column('container_image_consumer_kind',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('container_image_consumer_owner',
                      sqlalchemy.Text,
                      server_default=None),
    # Best-effort cost attribution. These scalar fields are populated during
    # launch without adding a separate lifecycle write or lookup.
    sqlalchemy.Column('workload_type', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('workload_id', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('workload_task_id',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('provision_log_path',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('skylet_ssh_tunnel_metadata',
                      sqlalchemy.LargeBinary,
                      server_default=None),
    # Infrastructure columns for efficient filtering
    sqlalchemy.Column('cloud', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('region', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('zone', sqlalchemy.Text, server_default=None),
    # Node names for dashboard display (comma-separated)
    sqlalchemy.Column('node_names', sqlalchemy.Text, server_default=None),
    # External links for dashboard display, e.g. cloud-provider instance
    # console URLs generated at launch time. Same shape as the `links` field
    # on managed-job rows: a JSON object mapping {label: url}.
    sqlalchemy.Column('links', sqlalchemy.JSON, server_default=None),
)

_CLUSTER_RECORD_UUID_INDEX = 'uq_clusters_cluster_record_uuid_nonnull'
sqlalchemy.Index(
    _CLUSTER_RECORD_UUID_INDEX,
    cluster_table.c.cluster_record_uuid,
    unique=True,
    postgresql_where=cluster_table.c.cluster_record_uuid.is_not(None),
    sqlite_where=cluster_table.c.cluster_record_uuid.is_not(None),
)

storage_table = sqlalchemy.Table(
    'storage',
    Base.metadata,
    sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('launched_at', sqlalchemy.Integer),
    sqlalchemy.Column('handle', sqlalchemy.LargeBinary),
    sqlalchemy.Column('last_use', sqlalchemy.Text),
    sqlalchemy.Column('status', sqlalchemy.Text),
)

volume_table = sqlalchemy.Table(
    'volumes',
    Base.metadata,
    sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('launched_at', sqlalchemy.Integer),
    sqlalchemy.Column('handle', sqlalchemy.LargeBinary),
    sqlalchemy.Column('user_hash', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('workspace',
                      sqlalchemy.Text,
                      server_default=constants.SKYPILOT_DEFAULT_WORKSPACE),
    sqlalchemy.Column('last_attached_at',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('last_use', sqlalchemy.Text),
    sqlalchemy.Column('status', sqlalchemy.Text),
    sqlalchemy.Column('is_ephemeral', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('error_message', sqlalchemy.Text, server_default=None),
    # JSON-encoded lists of pods/clusters using the volume
    sqlalchemy.Column('usedby_pods', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('usedby_clusters', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('creation_yaml', sqlalchemy.Text, server_default=None),
)

# Table for Cluster History
# usage_intervals: List[Tuple[int, int]]
#  Specifies start and end timestamps of cluster.
#  When the last end time is None, the cluster is still UP.
#  Example: [(start1, end1), (start2, end2), (start3, None)]

# requested_resources: Set[resource_lib.Resource]
#  Requested resources fetched from task that user specifies.

# launched_resources: Optional[resources_lib.Resources]
#  Actual launched resources fetched from handle for cluster.

# num_nodes: Optional[int] number of nodes launched.
cluster_history_table = sqlalchemy.Table(
    'cluster_history',
    Base.metadata,
    sqlalchemy.Column('cluster_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('name', sqlalchemy.Text),
    sqlalchemy.Column('num_nodes', sqlalchemy.Integer),
    sqlalchemy.Column('requested_resources', sqlalchemy.LargeBinary),
    sqlalchemy.Column('launched_resources', sqlalchemy.LargeBinary),
    sqlalchemy.Column('usage_intervals', sqlalchemy.LargeBinary),
    sqlalchemy.Column('user_hash', sqlalchemy.Text),
    sqlalchemy.Column('last_creation_yaml',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('last_creation_command',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('workspace', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('provision_log_path',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('last_activity_time',
                      sqlalchemy.Integer,
                      server_default=None,
                      index=True),
    sqlalchemy.Column('launched_at',
                      sqlalchemy.Integer,
                      server_default=None,
                      index=True),
    # Infrastructure columns for efficient filtering
    sqlalchemy.Column('cloud', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('region', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('zone', sqlalchemy.Text, server_default=None),
    # Node names for dashboard display (comma-separated)
    sqlalchemy.Column('node_names', sqlalchemy.Text, server_default=None),
    # Whether the cluster was launched by a controller (managed job or
    # service). Mirrors the `is_managed` column on the clusters table so that
    # history queries (e.g. the dashboard's cost report) can filter out
    # controller-backed clusters even after they are terminated, since at that
    # point the clusters table row is gone and the join can no longer supply
    # the flag.
    sqlalchemy.Column('is_managed', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('workload_type', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('workload_id', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('workload_task_id',
                      sqlalchemy.Integer,
                      server_default=None),
    # Updated only when usage_intervals changes. The estimated-spend daemon
    # uses this as an incremental watermark.
    sqlalchemy.Column('usage_updated_at',
                      sqlalchemy.Integer,
                      server_default='0',
                      index=True),
)

# Materialized, best-effort compute-cost estimates. One row represents the
# overlap of one cluster-history record with one UTC day. Request paths only
# aggregate this table; pricing and interval splitting happen in a daemon.
estimated_spend_daily_table = sqlalchemy.Table(
    'estimated_spend_daily',
    Base.metadata,
    sqlalchemy.Column('day_start_utc', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('cluster_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('workload_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('workload_id', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('workload_task_id',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('user_hash', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('workspace', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('cloud', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('region', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('use_spot', sqlalchemy.Boolean, server_default=None),
    sqlalchemy.Column('num_nodes', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('machine_seconds', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('catalog_hourly_rate',
                      sqlalchemy.Float,
                      server_default=None),
    sqlalchemy.Column('estimated_cost', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('exclusion_reason', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('priced_at', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('updated_at', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Index('idx_estimated_spend_day_workspace', 'day_start_utc',
                     'workspace'),
    sqlalchemy.Index('idx_estimated_spend_day_workload', 'day_start_utc',
                     'workload_type', 'workload_id'),
)

estimated_spend_state_table = sqlalchemy.Table(
    'estimated_spend_state',
    Base.metadata,
    sqlalchemy.Column('singleton_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('last_started_at',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('last_success_at',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('source_watermark',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('source_watermark_hash',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('active_cursor_hash',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('backfill_cursor_launched_at',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('backfill_cursor_hash',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('backfill_complete',
                      sqlalchemy.Boolean,
                      server_default=sqlalchemy.sql.expression.false()),
    sqlalchemy.Column('coverage_start_utc',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('last_error', sqlalchemy.Text, server_default=None),
)

# Table for cluster status change events.
# starting_status: Status of the cluster at the start of the event.
# ending_status: Status of the cluster at the end of the event.
# reason: Reason for the transition.
# transitioned_at: Timestamp of the transition.
cluster_event_table = sqlalchemy.Table(
    'cluster_events',
    Base.metadata,
    sqlalchemy.Column('cluster_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('name', sqlalchemy.Text),
    sqlalchemy.Column('starting_status', sqlalchemy.Text),
    sqlalchemy.Column('ending_status', sqlalchemy.Text),
    sqlalchemy.Column('reason', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('transitioned_at', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('type', sqlalchemy.Text),
    sqlalchemy.Column('request_id', sqlalchemy.Text, server_default=None),
)

ssh_key_table = sqlalchemy.Table(
    'ssh_key',
    Base.metadata,
    sqlalchemy.Column('user_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('ssh_public_key', sqlalchemy.Text),
    sqlalchemy.Column('ssh_private_key', sqlalchemy.Text),
)

service_account_token_table = sqlalchemy.Table(
    'service_account_tokens',
    Base.metadata,
    sqlalchemy.Column('token_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('token_name', sqlalchemy.Text),
    # Indexed + unique: the auth middleware looks up rows by hash on every
    # request to enforce revocation/rotation/expiration.
    sqlalchemy.Column('token_hash', sqlalchemy.Text, index=True, unique=True),
    sqlalchemy.Column('created_at', sqlalchemy.Integer),
    sqlalchemy.Column('last_used_at', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('expires_at', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('creator_user_hash',
                      sqlalchemy.Text),  # Who created this token
    sqlalchemy.Column('service_account_user_id',
                      sqlalchemy.Text),  # Service account's own user ID
)

cluster_yaml_table = sqlalchemy.Table(
    'cluster_yaml',
    Base.metadata,
    sqlalchemy.Column('cluster_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('yaml', sqlalchemy.Text),
)

system_config_table = sqlalchemy.Table(
    'system_config',
    Base.metadata,
    sqlalchemy.Column('config_key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('config_value', sqlalchemy.Text),
    sqlalchemy.Column('created_at', sqlalchemy.Integer),
    sqlalchemy.Column('updated_at', sqlalchemy.Integer),
)

# Low-cardinality operator notifications. One row is retained per category;
# repeated occurrences update the same row instead of appending an event.
operator_notification_table = sqlalchemy.Table(
    'operator_notifications',
    Base.metadata,
    sqlalchemy.Column('category', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('message', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('first_seen_at', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('last_seen_at',
                      sqlalchemy.Integer,
                      nullable=False,
                      index=True),
    sqlalchemy.Column('occurrence_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('sequence',
                      sqlalchemy.Integer,
                      nullable=False,
                      index=True),
)

# Each dashboard operator acknowledges a single global sequence, rather than
# multiplying notification rows by (user, category).
operator_notification_cursor_table = sqlalchemy.Table(
    'operator_notification_cursors',
    Base.metadata,
    sqlalchemy.Column('user_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('last_seen_sequence',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('updated_at', sqlalchemy.Integer, nullable=False),
)

# A singleton counter gives notification incidents a total order across
# categories. Gaps are harmless: suppressed occurrences consume a sequence but
# keep their category's previous incident sequence.
operator_notification_sequence_table = sqlalchemy.Table(
    'operator_notification_sequence',
    Base.metadata,
    sqlalchemy.Column('singleton_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('value',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
)
