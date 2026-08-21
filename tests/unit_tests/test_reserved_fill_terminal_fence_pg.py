"""Real-PostgreSQL tests for terminal reserved-fill row binding."""
# pylint: disable=protected-access,redefined-outer-name,unused-import

import copy

import pytest
import sqlalchemy
from test_serve_ordinary_launch_binding_pg import _RECORD_ID
from test_serve_ordinary_launch_binding_pg import _stored_replica_state
from test_serve_ordinary_launch_binding_pg import _unbound_context
from test_serve_ordinary_launch_binding_pg import binding_database  # noqa: F401
from test_serve_resource_actions_pg import empty_postgres  # noqa: F401
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import constants
from sky.serve import kubernetes_identity
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker as broker
from sky.serve import serve_state
from sky.serve import serve_state_schema

pytestmark = pytest.mark.xdist_group(
    name='serve_ordinary_launch_binding_schema_042_pg')

_FLEET_DIGEST = 'a' * 64
_INVENTORY_DIGEST = 'b' * 64
_ALLOCATION_DIGEST = 'c' * 64
_INTENT_DIGEST = 'd' * 64
_GATE_GENERATION = 1


def _worker_projection(*,
                       candidate_id: str = 'kubernetes-0000',
                       context: str = 'phx-context',
                       accelerator: str = 'H200',
                       product_label: str = 'NVIDIA-H200') -> dict[str, object]:
    return {
        'projection_version': 2,
        'candidate_id': candidate_id,
        'kubernetes_context': context,
        'namespace': 'inference',
        'service_account_name': 'inference-sa',
        'scheduler_name': 'default-scheduler',
        'priority_class_name': 'inference-low',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'kueue_admission': {
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        },
        'pod_identity_role_arn':
            ('arn:aws:iam::123456789012:role/inference-worker'),
        'accelerator_name': accelerator,
        'accelerator_count': 1,
        'accelerator_scheduling': {
            'label_key': 'nvidia.com/gpu.product',
            'label_values': [product_label],
            'resource_key': 'nvidia.com/gpu',
        },
        'cache': {
            'kind': 'none',
        },
    }


_WORKER_PROJECTION = _worker_projection()
_WORKER_PROJECTION_DIGEST = kubernetes_identity.worker_projection_sha256(
    _WORKER_PROJECTION)


def _pool_key() -> str:
    return broker.make_pool_key('phx-context',
                                'H200',
                                protocol_version=broker.PROTOCOL_V2,
                                physical_cluster_uid='physical-uid')


def _location_state() -> dict[str, object]:
    return {
        'cloud': 'Kubernetes',
        'region': 'phx-context',
        'zone': None,
        'accelerators': {
            'H200': 1
        },
        'use_spot': False,
        'image_id': None,
        'container_image': None,
        'disk_tier': None,
        'ephemeral_storage': None,
        'instance_type': None,
    }


def _reserved_fill_state(
    *,
    intent_idempotency_key: str = _INTENT_DIGEST,
) -> dict[str, object]:
    location = _location_state()
    return _stored_replica_state({
        'reserved_fill': True,
        'is_zero_cost': True,
        'reserved_fill_pool_key': _pool_key(),
        'reserved_fill_service_generation': 7,
        'reserved_fill_physical_cluster_uid': 'physical-uid',
        'reserved_fill_kubernetes_context': 'phx-context',
        'reserved_fill_allocation_generation': 5,
        'reserved_fill_allocation_input_sha256': _ALLOCATION_DIGEST,
        'reserved_fill_allocation_claim_generation': 7,
        'reserved_fill_reconciliation_gate_generation': _GATE_GENERATION,
        'reserved_fill_reclaim_fleet_bundle_sha256': _FLEET_DIGEST,
        'reserved_fill_reclaim_policy_revision': 'policy-v1',
        'reserved_fill_reclaim_provider_inventory_sha256': _INVENTORY_DIGEST,
        'reserved_fill_worker_projection_sha256': _WORKER_PROJECTION_DIGEST,
        'reserved_fill_observation_generation': 13,
        'reserved_fill_observation_sequence': 17,
        'reserved_fill_intent_idempotency_key': intent_idempotency_key,
        'zero_cost_admission_sequence': 19,
        'location': location,
        'resources_override': copy.deepcopy(location),
    })


def _launch_context() -> dict[str, object]:
    context = _unbound_context()
    context.update({
        constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
            constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
        constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_RECORD_ID),
    })
    context.update(
        reserved_capacity.make_protocol_v2_launch_fence(
            pool_key=_pool_key(),
            service_generation=7,
            service_version=2,
            physical_cluster_uid='physical-uid',
            kubernetes_context='phx-context',
            accelerator='H200',
            accelerator_count=1,
            reconciliation_gate_generation=_GATE_GENERATION,
            reclaim_fleet_bundle_sha256=_FLEET_DIGEST,
            reclaim_policy_revision='policy-v1',
            reclaim_provider_inventory_sha256=_INVENTORY_DIGEST,
            worker_projection_sha256=_WORKER_PROJECTION_DIGEST))
    return context


def test_terminal_snapshot_reads_and_binds_exact_replica_row(
        binding_database) -> None:
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING',
                    replica_state=_reserved_fill_state()))

    statements: list[str] = []

    def _capture_statement(_connection, _cursor, statement, _parameters,
                           _context, _executemany):
        statements.append(statement)

    sqlalchemy.event.listen(binding_database, 'before_cursor_execute',
                            _capture_statement)
    try:
        snapshot = serve_state.service_replica_launch_fence_snapshot(
            _launch_context())
    finally:
        sqlalchemy.event.remove(binding_database, 'before_cursor_execute',
                                _capture_statement)

    assert snapshot is not None
    assert snapshot.durable_replica_info is not None
    fence = reserved_capacity.parse_protocol_v2_launch_fence(_launch_context())
    assert fence is not None
    reserved_capacity.validate_protocol_v2_launch_fence_against_replica(
        fence, snapshot.durable_replica_info)
    row_snapshot_statements = [
        statement for statement in statements
        if 'FROM services' in statement and 'FROM replicas' in statement
    ]
    assert len(row_snapshot_statements) == 1


def test_terminal_snapshot_exposes_row_tamper_to_exact_fence_validation(
        binding_database) -> None:
    state = _reserved_fill_state()
    state['reserved_fill_service_generation'] = 8
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING', replica_state=state))

    snapshot = serve_state.service_replica_launch_fence_snapshot(
        _launch_context())
    assert snapshot is not None
    assert snapshot.durable_replica_info is not None
    fence = reserved_capacity.parse_protocol_v2_launch_fence(_launch_context())
    assert fence is not None
    with pytest.raises(ValueError, match='admitted allocation provenance'):
        reserved_capacity.validate_protocol_v2_launch_fence_against_replica(
            fence, snapshot.durable_replica_info)
