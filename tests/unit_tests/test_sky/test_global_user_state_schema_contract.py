"""Characterization tests for the global user state schema facade."""

from sky import global_user_state
from sky import global_user_state_schema

_EXPECTED_TABLE_COLUMNS = {
    'auth_session_table':
        ('auth_sessions', ('code_challenge', 'token', 'created_at')),
    'cluster_event_table':
        ('cluster_events',
         ('cluster_hash', 'name', 'starting_status', 'ending_status', 'reason',
          'transitioned_at', 'type', 'request_id')),
    'cluster_history_table':
        ('cluster_history',
         ('cluster_hash', 'name', 'num_nodes', 'requested_resources',
          'launched_resources', 'usage_intervals', 'user_hash',
          'last_creation_yaml', 'last_creation_command', 'workspace',
          'provision_log_path', 'last_activity_time', 'launched_at', 'cloud',
          'region', 'zone', 'node_names', 'is_managed', 'workload_type',
          'workload_id', 'workload_task_id', 'usage_updated_at')),
    'cluster_table': (
        'clusters',
        ('name', 'launched_at', 'handle', 'last_use', 'status', 'autostop',
         'to_down', 'metadata', 'owner', 'cluster_hash', 'cluster_record_uuid',
         'storage_mounts_metadata', 'cluster_ever_up', 'status_updated_at',
         'config_hash', 'user_hash', 'workspace', 'last_creation_yaml',
         'last_creation_command', 'is_managed', 'container_image_binding_known',
         'container_image_consumer_kind', 'container_image_consumer_owner',
         'workload_type', 'workload_id', 'workload_task_id',
         'provision_log_path', 'skylet_ssh_tunnel_metadata', 'cloud', 'region',
         'zone', 'node_names', 'links')),
    'cluster_yaml_table': ('cluster_yaml', ('cluster_name', 'yaml')),
    'config_table': ('config', ('key', 'value')),
    'estimated_spend_daily_table':
        ('estimated_spend_daily',
         ('day_start_utc', 'cluster_hash', 'cluster_name', 'workload_type',
          'workload_id', 'workload_task_id', 'user_hash', 'workspace', 'cloud',
          'region', 'use_spot', 'num_nodes', 'machine_seconds',
          'catalog_hourly_rate', 'estimated_cost', 'exclusion_reason',
          'priced_at', 'updated_at')),
    'estimated_spend_state_table':
        ('estimated_spend_state',
         ('singleton_id', 'last_started_at', 'last_success_at',
          'source_watermark', 'source_watermark_hash', 'active_cursor_hash',
          'backfill_cursor_launched_at', 'backfill_cursor_hash',
          'backfill_complete', 'coverage_start_utc', 'last_error')),
    'operator_notification_cursor_table':
        ('operator_notification_cursors', ('user_id', 'last_seen_sequence',
                                           'updated_at')),
    'operator_notification_sequence_table':
        ('operator_notification_sequence', ('singleton_id', 'value')),
    'operator_notification_table':
        ('operator_notifications',
         ('category', 'message', 'first_seen_at', 'last_seen_at',
          'occurrence_count', 'sequence')),
    'service_account_token_table':
        ('service_account_tokens',
         ('token_id', 'token_name', 'token_hash', 'created_at', 'last_used_at',
          'expires_at', 'creator_user_hash', 'service_account_user_id')),
    'ssh_key_table':
        ('ssh_key', ('user_hash', 'ssh_public_key', 'ssh_private_key')),
    'storage_table':
        ('storage', ('name', 'launched_at', 'handle', 'last_use', 'status')),
    'system_config_table': ('system_config', ('config_key', 'config_value',
                                              'created_at', 'updated_at')),
    'user_table': ('users', ('id', 'name', 'password', 'created_at', 'type',
                             'preferred_workspace')),
    'volume_table':
        ('volumes',
         ('name', 'launched_at', 'handle', 'user_hash', 'workspace',
          'last_attached_at', 'last_use', 'status', 'is_ephemeral',
          'error_message', 'usedby_pods', 'usedby_clusters', 'creation_yaml')),
}


def test_global_user_state_schema_contract() -> None:
    assert set(global_user_state.Base.metadata.tables) == {
        table_name for table_name, _ in _EXPECTED_TABLE_COLUMNS.values()
    }
    for public_name, (table_name, columns) in _EXPECTED_TABLE_COLUMNS.items():
        table = getattr(global_user_state, public_name)
        assert table is getattr(global_user_state_schema, public_name)
        assert table.name == table_name
        assert tuple(table.columns.keys()) == columns
        assert global_user_state.Base.metadata.tables[table_name] is table
    assert global_user_state.Base is global_user_state_schema.Base
