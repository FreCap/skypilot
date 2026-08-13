"""Characterization tests for the durable request PostgreSQL record schema."""
# pylint: disable=protected-access

from sky.server.requests import postgres
from sky.server.requests import postgres_schema


def test_postgres_record_schema_topology() -> None:
    expected_columns = {
        'api_requests':
            ('request_id', 'name', 'handler_name', 'payload_type',
             'payload_format', 'payload_version', 'producer_version',
             'payload_json', 'execution_class', 'status', 'terminal_cause',
             'return_value', 'error', 'pid',
             'execution_process_start_time_ticks', 'created_at', 'cluster_name',
             'schedule_type', 'user_id', 'status_msg', 'should_retry',
             'finished_at', 'file_mounts_blob_id', 'ignore_return_value',
             'retryable', 'execution_generation', 'claim_token',
             'worker_instance_id', 'controller_generation', 'managed_job_id',
             'managed_job_controller_instance_id',
             'managed_job_controller_generation',
             'managed_job_controller_slot_id',
             'managed_job_controller_slot_attempt', 'lease_expires_at',
             'heartbeat_at', 'cancel_requested_at', 'cancel_acknowledged_at',
             'execution_quiescence_required', 'execution_quiesced_generation',
             'execution_quiesced_at', 'interrupted_reason', 'event_context',
             'resource_action_id', 'resource_action_attempt',
             'ordinary_launch_association_id', 'updated_at'),
        'api_request_retention_pins':
            ('pin_kind', 'pin_id', 'request_id', 'created_at'),
        'api_resource_actions':
            ('action_id', 'domain', 'resource_type', 'resource_identity',
             'desired_generation', 'action_type', 'immutable_spec',
             'immutable_spec_sha256', 'kernel_state', 'current_attempt',
             'next_attempt_at', 'last_result', 'last_result_sha256',
             'terminal_disposition', 'revision', 'created_at', 'updated_at',
             'terminal_at'),
        'api_resource_action_attempts':
            ('action_id', 'attempt', 'request_id', 'request_input_sha256',
             'provider_operation_id', 'mutation_boundary',
             'provider_io_boundary', 'provider_progress',
             'provider_progress_sha256', 'provider_progress_revision',
             'typed_outcome', 'typed_outcome_sha256', 'request_terminal_state',
             'admitted_at', 'updated_at', 'settled_at'),
        'api_request_queue':
            ('request_id', 'schedule_type', 'priority', 'available_at',
             'enqueued_at', 'sequence', 'ignore_return_value', 'retryable',
             'precondition_type', 'precondition_payload',
             'precondition_deadline', 'precondition_attempts', 'delivery_state',
             'claim_generation', 'updated_at'),
        'api_request_store_metadata': ('key', 'value', 'updated_at'),
        'api_server_instances':
            ('instance_id', 'role', 'pod_name', 'pod_uid', 'pod_ip', 'version',
             'started_at', 'heartbeat_at', 'draining_at', 'ready',
             'health_detail', 'supported_handlers',
             'supported_payload_versions', 'request_storage_backend',
             'request_queue_backend', 'execution_quiescence_capable',
             'ordinary_launch_binding_capable'),
        'api_controller_leadership':
            ('leadership_key', 'generation', 'instance_id', 'lock_backend_pid',
             'generation_lock_key', 'origin_capability_sha256', 'acquired_at',
             'heartbeat_at', 'released_at'),
        'api_controller_action_reservations':
            ('logical_action_id', 'resource_identity', 'action_type', 'state',
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
    assert tuple(postgres._PG_LOCKS.columns.keys()) == ('locktype', 'pid',
                                                        'classid', 'objid',
                                                        'objsubid', 'mode',
                                                        'granted')


def test_postgres_schema_objects_keep_historical_facade_identity() -> None:
    assert postgres._METADATA is postgres_schema.metadata
    assert postgres.REQUESTS is postgres_schema.REQUESTS
    assert (postgres.REQUEST_RETENTION_PINS
            is postgres_schema.REQUEST_RETENTION_PINS)
    assert postgres.RESOURCE_ACTIONS is postgres_schema.RESOURCE_ACTIONS
    assert (postgres.RESOURCE_ACTION_ATTEMPTS
            is postgres_schema.RESOURCE_ACTION_ATTEMPTS)
    assert postgres.QUEUE is postgres_schema.QUEUE
    assert postgres.STORE_METADATA is postgres_schema.STORE_METADATA
    assert postgres.SERVER_INSTANCES is postgres_schema.SERVER_INSTANCES
    assert (postgres.CONTROLLER_LEADERSHIP
            is postgres_schema.CONTROLLER_LEADERSHIP)
    assert (postgres.CONTROLLER_ACTION_RESERVATIONS
            is postgres_schema.CONTROLLER_ACTION_RESERVATIONS)
    assert postgres._PG_LOCKS is postgres_schema.PG_LOCKS


def test_execution_quiescence_required_keeps_api007_insert_default() -> None:
    column = postgres_schema.REQUESTS.c.execution_quiescence_required

    assert column.server_default is not None
    assert str(column.server_default.arg) == 'false'


def test_managed_job_origin_schema_is_complete_and_exactly_indexed() -> None:
    constraints = {
        constraint.name: str(constraint.sqltext)
        for constraint in postgres_schema.REQUESTS.constraints
        if constraint.name is not None
    }
    complete = ''.join(
        constraints['ck_api_requests_managed_job_origin_complete'].split())
    values = ''.join(
        constraints['ck_api_requests_managed_job_origin_values'].split())

    for field in ('managed_job_id', 'managed_job_controller_instance_id',
                  'managed_job_controller_generation',
                  'managed_job_controller_slot_id',
                  'managed_job_controller_slot_attempt'):
        assert field in complete
    assert 'IN(0,5)' in complete
    assert 'managed_job_id>0' in values
    assert 'managed_job_controller_generation>0' in values
    assert 'managed_job_controller_slot_id>=0' in values

    indexes = {index.name: index for index in postgres_schema.REQUESTS.indexes}
    attempt_index = indexes['ix_api_requests_managed_job_attempt']
    assert tuple(column.name for column in attempt_index.columns) == (
        'managed_job_id', 'managed_job_controller_instance_id',
        'managed_job_controller_generation', 'managed_job_controller_slot_id',
        'managed_job_controller_slot_attempt')
    assert attempt_index.dialect_options['postgresql']['where'] is not None
