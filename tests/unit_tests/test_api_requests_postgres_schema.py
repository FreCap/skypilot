"""Characterization tests for the durable request PostgreSQL record schema."""

from sky.server.requests import postgres


def test_postgres_record_schema_topology() -> None:
    expected_columns = {
        'api_requests': (
            'request_id', 'name', 'handler_name', 'payload_type',
            'payload_format', 'payload_version', 'producer_version',
            'payload_json', 'execution_class', 'status', 'return_value',
            'error', 'pid', 'created_at', 'cluster_name', 'schedule_type',
            'user_id', 'status_msg', 'should_retry', 'finished_at',
            'file_mounts_blob_id', 'ignore_return_value', 'retryable',
            'execution_generation', 'claim_token', 'worker_instance_id',
            'controller_generation', 'lease_expires_at', 'heartbeat_at',
            'cancel_requested_at', 'cancel_acknowledged_at',
            'interrupted_reason', 'event_context', 'updated_at'),
        'api_request_queue': (
            'request_id', 'schedule_type', 'priority', 'available_at',
            'enqueued_at', 'sequence', 'ignore_return_value', 'retryable',
            'precondition_type', 'precondition_payload',
            'precondition_deadline', 'precondition_attempts',
            'delivery_state', 'claim_generation', 'updated_at'),
        'api_request_store_metadata': ('key', 'value', 'updated_at'),
        'api_server_instances': (
            'instance_id', 'role', 'pod_name', 'pod_uid', 'pod_ip', 'version',
            'started_at', 'heartbeat_at', 'draining_at', 'ready',
            'health_detail', 'supported_handlers',
            'supported_payload_versions'),
        'api_controller_leadership': (
            'leadership_key', 'generation', 'instance_id', 'lock_backend_pid',
            'generation_lock_key', 'acquired_at', 'heartbeat_at',
            'released_at'),
        'api_controller_action_reservations': (
            'logical_action_id', 'resource_identity', 'action_type', 'state',
            'controller_generation', 'controller_instance_id',
            'provider_operation_id', 'created_at', 'updated_at',
            'reconciliation_at'),
    }

    assert set(postgres._METADATA.tables) == set(expected_columns)
    for table_name, columns in expected_columns.items():
        table = postgres._METADATA.tables[table_name]
        assert tuple(column.name for column in table.columns) == columns
        assert table.metadata is postgres._METADATA

    assert postgres._PG_LOCKS.schema == 'pg_catalog'
    assert tuple(column.name for column in postgres._PG_LOCKS.columns) == (
        'locktype', 'pid', 'classid', 'objid', 'objsubid', 'mode', 'granted')
