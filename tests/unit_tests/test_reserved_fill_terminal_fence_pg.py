"""Real-PostgreSQL tests for terminal reserved-fill row binding."""
# pylint: disable=protected-access,redefined-outer-name,unused-import

import copy
import json
import time
import uuid

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
from sky.serve import pool_capacity_observation_schema
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker as broker
from sky.serve import reserved_fill_planner
from sky.serve import reserved_fill_reclaim_attestation as reclaim
from sky.serve import reserved_fill_reclaim_proof_schema as proof_schema
from sky.serve import reserved_fill_reclaim_proofs as proofs
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import serve_utils
from sky.serve import zero_cost_actuation
from sky.serve import zero_cost_actuation_schema

pytestmark = pytest.mark.xdist_group(
    name='serve_ordinary_launch_binding_schema_042_pg')

_FLEET_DIGEST = 'a' * 64
_INVENTORY_DIGEST = 'b' * 64
_ALLOCATION_DIGEST = 'c' * 64
_INTENT_DIGEST = 'd' * 64
_GATE_GENERATION = 1
_PROOF_NONCE = 'f' * 64
_SERVICE_HASH = 'svc-hash'
_CONTROLLER_PORT = 8123


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
_SUCCESSOR_WORKER_PROJECTION = _worker_projection(
    candidate_id='kubernetes-0001',
    context='east-context',
    accelerator='A100',
    product_label='NVIDIA-A100')
_SUCCESSOR_WORKER_PROJECTION_DIGEST = (
    kubernetes_identity.worker_projection_sha256(_SUCCESSOR_WORKER_PROJECTION))
_IDENTITY = reclaim.ReclaimPolicyIdentity(
    fleet_bundle_sha256=_FLEET_DIGEST,
    policy_revision='policy-v1',
    provider_inventory_sha256=_INVENTORY_DIGEST)
_PROOF_PAYLOAD, _PROOF_DIGEST = proofs.canonical_proof_payload({
    'kubernetes': {
        'physical_cluster_uid': 'physical-uid',
    },
})


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


def _launch_scope(context: dict[str, object]) -> reclaim.ReclaimLaunchScope:
    fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
    assert fence is not None
    _, projected_admission = reserved_capacity.require_reclaim_worker_projection(
        fence, [_WORKER_PROJECTION])
    return reclaim.ReclaimLaunchScope(
        service_name='svc',
        service_version=fence.service_version,
        pool_key=fence.pool_key,
        service_generation=fence.service_generation,
        physical_cluster_uid=fence.physical_cluster_uid,
        kubernetes_context=fence.kubernetes_context,
        accelerator=fence.accelerator,
        accelerator_count=fence.accelerator_count,
        projected_admission=projected_admission)


def _fill_intent() -> reserved_fill_planner.FillIntent:
    location = reserved_fill_planner.LocationSnapshot.from_pickleable(
        _location_state())
    return reserved_fill_planner.FillIntent.create(
        ordinal=0,
        protocol_version=broker.PROTOCOL_V2,
        policy_revision=2,
        reconcile_generation=3,
        allocation_generation=5,
        allocation_input_sha256=_ALLOCATION_DIGEST,
        allocation_claim_generation=7,
        reconciliation_gate_generation=_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_FLEET_DIGEST,
        reclaim_policy_revision='policy-v1',
        reclaim_provider_inventory_sha256=_INVENTORY_DIGEST,
        service_incarnation=_SERVICE_HASH,
        service_version=2,
        controller_owner=serve_utils.make_controller_owner_fingerprint(
            _SERVICE_HASH, 123, '10.0.0.2', _CONTROLLER_PORT),
        service_generation=7,
        pool_key=_pool_key(),
        pool_epoch=23,
        physical_cluster_uid='physical-uid',
        worker_projection_sha256=_WORKER_PROJECTION_DIGEST,
        observation_generation=13,
        observation_sequence=17,
        ordinary_zero_cost_admission_sequence=17,
        valid_until=time.time() + 300,
        accelerator='H200',
        accelerator_count=1,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL,
        allowed_locations=(location,))


def _fill_plan(
    intent: reserved_fill_planner.FillIntent,
) -> reserved_fill_planner.FillPlan:
    return reserved_fill_planner.FillPlan(
        policy_revision=intent.policy_revision,
        reconcile_generation=intent.reconcile_generation,
        allocation_generation=intent.allocation_generation,
        allocation_input_sha256=intent.allocation_input_sha256,
        allocation_claim_generation=intent.allocation_claim_generation,
        reconciliation_gate_generation=(intent.reconciliation_gate_generation),
        reclaim_fleet_bundle_sha256=intent.reclaim_fleet_bundle_sha256,
        reclaim_policy_revision=intent.reclaim_policy_revision,
        reclaim_provider_inventory_sha256=(
            intent.reclaim_provider_inventory_sha256),
        capacity_unit=intent.capacity_unit,
        intents=(intent,))


def _claim_edge_values(*, pool_key: str, context: str, physical_uid: str,
                       accelerator: str, projection_digest: str,
                       generation: int) -> dict[str, object]:
    return {
        'service_name': 'svc',
        'pool_key': pool_key,
        'legacy_pool_key': json.dumps([context, accelerator.casefold()]),
        'pool_position': 0,
        'access_context': context,
        'physical_cluster_uid': physical_uid,
        'accelerator_names': json.dumps([accelerator.casefold()]),
        'worker_projection_sha256_by_accelerator': {
            accelerator.casefold(): projection_digest,
        },
        'service_generation': generation,
        'weight': 1000,
        'floor_replicas': 0,
        'gpus_per_replica': 1,
        'holdings_fill': 0,
        'effective_cap': 1,
        'launchable': 1,
        'heartbeat_ts': time.time(),
    }


def _successor_claim_edge() -> dict[str, object]:
    return {
        'pool_key': broker.make_pool_key(
            'east-context',
            'A100',
            protocol_version=broker.PROTOCOL_V2,
            physical_cluster_uid='successor-physical-uid'),
        'legacy_pool_key': json.dumps(['east-context', 'a100']),
        'pool_position': 0,
        'access_context': 'east-context',
        'physical_cluster_uid': 'successor-physical-uid',
        'accelerator_names': ['a100'],
        'worker_projection_sha256_by_accelerator': {
            'a100': _SUCCESSOR_WORKER_PROJECTION_DIGEST,
        },
        'weight': 1000,
        'floor_replicas': 0,
        'gpus_per_replica': 1,
        'holdings_fill': 0,
        'effective_cap': 1,
        'launchable': True,
    }


def _successor_claim_authority(
    semantic_hash: str,
) -> tuple[reclaim.ReclaimClaimSetScope, reclaim.ReclaimClaimAuthorization]:
    projected_admissions = serve_state.reserved_fill_reclaim_projected_admissions(
        [_WORKER_PROJECTION, _SUCCESSOR_WORKER_PROJECTION],
        access_context='east-context',
        accelerator_names=('a100',),
        accelerator_count=1)
    scope = reclaim.ReclaimClaimSetScope(
        service_name='svc',
        service_incarnation=_SERVICE_HASH,
        service_version=2,
        semantic_hash=semantic_hash,
        edges=(reclaim.ReclaimClaimEdge(
            pool_key=str(_successor_claim_edge()['pool_key']),
            access_context='east-context',
            physical_cluster_uid='successor-physical-uid',
            accelerator_names=('a100',),
            projected_admissions=projected_admissions),))
    return scope, reclaim.ReclaimClaimAuthorization(
        identity=_IDENTITY,
        gate_generation=_GATE_GENERATION,
        scope=scope,
        completed_monotonic=time.monotonic())


def _fresh_launch_authorization(
    database: sqlalchemy.engine.Engine,
    scope: reclaim.ReclaimLaunchScope,
) -> reclaim.ReclaimLaunchAuthorization:
    with database.begin() as connection:
        result = connection.execute(
            sqlalchemy.update(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table).
            where(proof_schema.serve_reserved_fill_reclaim_provider_proofs_table
                  .c.receipt_nonce == _PROOF_NONCE).values(
                      completed_at=sqlalchemy.func.clock_timestamp()))
        assert result.rowcount == 1
    completed_monotonic = time.monotonic()
    reference = reclaim.ReclaimProviderProofReference(
        receipt_nonce=_PROOF_NONCE,
        proof_sha256=_PROOF_DIGEST,
        identity=_IDENTITY,
        gate_generation=_GATE_GENERATION,
        kubernetes_context=scope.kubernetes_context,
        completed_monotonic=completed_monotonic)
    return reclaim.ReclaimLaunchAuthorization(
        identity=_IDENTITY,
        gate_generation=_GATE_GENERATION,
        scope=scope,
        provider_proof_reference=reference,
        completed_monotonic=completed_monotonic)


def _terminal_authority_holds(
    database: sqlalchemy.engine.Engine,
    context: dict[str, object],
    snapshot: serve_state.ServiceReplicaLaunchFenceSnapshot,
) -> bool:
    scope = _launch_scope(context)
    return serve_state.reserved_fill_reclaim_launch_authority_holds(
        scope, _fresh_launch_authorization(database, scope), context, snapshot)


def _install_committed_generation(
    database: sqlalchemy.engine.Engine,
) -> tuple[reserved_fill_planner.FillIntent, dict[str, object]]:
    intent = _fill_intent()
    services = serve_state_schema.services_table
    protocol = serve_state_schema.reserved_fill_protocol_state_table
    claim_sets = serve_state_schema.reserved_fill_service_claim_sets_table
    edges = serve_state_schema.reserved_fill_pool_claims_table
    replicas = serve_state_schema.replicas_table
    with database.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == 'svc').with_for_update()).mappings().one()
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                resource_scope=_SERVICE_HASH,
                controller_port=_CONTROLLER_PORT,
                reserved_fill_actuation_mode='DURABLE_INTENT',
                reserved_fill_actuation_epoch=1,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=(
                    service['controller_incarnation']),
                reserved_fill_actuation_protocol_version=1))
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 2).values(
                    worker_placement_projections=[
                        _WORKER_PROJECTION, _SUCCESSOR_WORKER_PROJECTION
                    ]))
        connection.execute(
            sqlalchemy.update(protocol).where(protocol.c.id == 1).values(
                protocol_version=broker.PROTOCOL_V2,
                claim_generation=7,
                image_digest=f'sha256:{"1" * 64}',
                deployment_generation='deployment-1',
                deployment_uid='deployment-uid-1',
                pod_inventory_count=1,
                pod_inventory_sha256='2' * 64,
                changed_at=time.time()))
        gate = pool_capacity_observation_schema.protocol_state_sequence_table
        connection.execute(
            sqlalchemy.update(gate).where(gate.c.id == 1).values(
                protocol_version=broker.PROTOCOL_V2,
                reconciliation_gate_state=(
                    pool_capacity_observation_schema.SEQUENCED_ACTIVE),
                reconciliation_gate_generation=_GATE_GENERATION,
                reclaim_fleet_bundle_sha256=_FLEET_DIGEST,
                reclaim_policy_revision='policy-v1',
                reclaim_provider_inventory_sha256=_INVENTORY_DIGEST,
                reclaim_claim_scope_count=1,
                reclaim_claim_scope_sha256='3' * 64,
                reclaim_evidence_sha256='4' * 64,
                reclaim_authorized_at=1.0))
        connection.execute(
            sqlalchemy.insert(claim_sets).values(
                service_name='svc',
                claim_set_state=(
                    serve_state.RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2),
                generation=7,
                edge_count=1,
                semantic_hash='generation-7',
                service_version=2,
                global_headroom=1,
                utilization_ceiling=1,
                heartbeat_ts=time.time()))
        connection.execute(
            sqlalchemy.insert(edges).values(**_claim_edge_values(
                pool_key=_pool_key(),
                context='phx-context',
                physical_uid='physical-uid',
                accelerator='H200',
                projection_digest=_WORKER_PROJECTION_DIGEST,
                generation=7)))
        connection.execute(
            sqlalchemy.delete(replicas).where(replicas.c.service_name == 'svc',
                                              replicas.c.replica_id == 3))
        connection.execute(
            sqlalchemy.insert(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table).
            values(receipt_nonce=_PROOF_NONCE,
                   reconciliation_gate_generation=_GATE_GENERATION,
                   reclaim_fleet_bundle_sha256=_FLEET_DIGEST,
                   reclaim_policy_revision='policy-v1',
                   reclaim_provider_inventory_sha256=_INVENTORY_DIGEST,
                   kubernetes_context='phx-context',
                   proof_schema_version=proofs.PROVIDER_PROOF_SCHEMA_VERSION,
                   proof_payload=_PROOF_PAYLOAD,
                   proof_sha256=_PROOF_DIGEST,
                   completed_at=sqlalchemy.func.clock_timestamp()))
        controller_incarnation = service['controller_incarnation']
        controller_owner_epoch = service['controller_owner_epoch']

    repository = zero_cost_actuation.ZeroCostActuationRepository(database)
    receipt = repository.grant_plan(
        'svc',
        _fill_plan(intent),
        max_capacity=1,
        expected_controller_incarnation=controller_incarnation,
        expected_controller_owner_epoch=controller_owner_epoch)
    assert [accepted.intent_idempotency_key for accepted in receipt.accepted
           ] == [intent.idempotency_key]
    lease = repository.lease_next(service_name='svc',
                                  pool_key=_pool_key(),
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    replica_info = replica_managers.ReplicaInfo.from_storage_dict(
        _reserved_fill_state(intent_idempotency_key=intent.idempotency_key))
    with database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(replicas).values(
                **serve_state._replica_row_values('svc', 3, replica_info)))
        zero_cost_actuation.commit_lease_in_connection(
            connection,
            lease,
            service_name='svc',
            replica_id=3,
            replica_record_id=_RECORD_ID,
            replica_info=replica_info)
    with database.connect() as connection:
        committed_row = dict(
            connection.execute(
                sqlalchemy.select(
                    zero_cost_actuation_schema.
                    serve_zero_cost_actuation_intents_table)).mappings().one())
    return intent, committed_row


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


def test_committed_generation_survives_successor_edge_replacement(
        binding_database) -> None:
    _, committed_row = _install_committed_generation(binding_database)
    context = _launch_context()
    snapshot = serve_state.service_replica_launch_fence_snapshot(context)
    assert snapshot is not None
    assert snapshot.durable_replica_info is not None

    # Generation G is authorized by its live exact edge. Even a committed
    # intent cannot replace that same-generation edge authority.
    assert _terminal_authority_holds(binding_database, context, snapshot)
    edges = serve_state_schema.reserved_fill_pool_claims_table
    successor_edge = _successor_claim_edge()
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(edges).where(edges.c.service_name == 'svc'))
        connection.execute(
            sqlalchemy.insert(edges).values(**_claim_edge_values(
                pool_key=str(successor_edge['pool_key']),
                context=str(successor_edge['access_context']),
                physical_uid=str(successor_edge['physical_cluster_uid']),
                accelerator='A100',
                projection_digest=_SUCCESSOR_WORKER_PROJECTION_DIGEST,
                generation=7)))
    assert not _terminal_authority_holds(binding_database, context, snapshot)
    with binding_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(edges).where(edges.c.service_name == 'svc'))
        connection.execute(
            sqlalchemy.insert(edges).values(**_claim_edge_values(
                pool_key=_pool_key(),
                context='phx-context',
                physical_uid='physical-uid',
                accelerator='H200',
                projection_digest=_WORKER_PROJECTION_DIGEST,
                generation=7)))
    assert _terminal_authority_holds(binding_database, context, snapshot)

    # The canonical G+1 publication removes G's H200 edge and replaces it with
    # one valid A100 edge. The exact committed G intent becomes the immutable
    # handoff authority for the historical H200 launch.
    successor_scope, successor_authorization = _successor_claim_authority(
        'generation-8-successor')
    successor_generation = serve_state.replace_reserved_fill_claim_set(
        'svc',
        semantic_hash='generation-8-successor',
        global_headroom=1,
        utilization_ceiling=1,
        utilization_state=None,
        edges=(successor_edge,),
        heartbeat_ts=time.time(),
        expected_service_hash=_SERVICE_HASH,
        service_version=2,
        expected_controller_owner=(123, '10.0.0.2'),
        reclaim_claim_scope=successor_scope,
        reclaim_claim_authorization=successor_authorization)
    assert successor_generation == 8
    assert _terminal_authority_holds(binding_database, context, snapshot)

    # The successor path is available only to the exact committed handoff.
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with binding_database.begin() as connection:
        connection.execute(sqlalchemy.delete(intents))
    assert not _terminal_authority_holds(binding_database, context, snapshot)

    corrupted_row = dict(committed_row)
    corrupted_row['allocation_input_sha256'] = '8' * 64
    with binding_database.begin() as connection:
        connection.execute(sqlalchemy.insert(intents).values(**corrupted_row))
    assert not _terminal_authority_holds(binding_database, context, snapshot)
