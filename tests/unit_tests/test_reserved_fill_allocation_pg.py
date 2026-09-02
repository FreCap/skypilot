"""Real-PostgreSQL contracts for authenticated fill allocation maps."""

# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import
import concurrent.futures
import copy
import dataclasses
import json
import pickle
import threading
import time
import typing
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_pool_capacity_observation_pg import observation_engine  # noqa: F401
from test_pool_capacity_observation_pg import pg_server  # noqa: F401

from sky import clouds
from sky import global_user_state_schema
from sky.client import sdk
from sky.serve import compatibility_matching
from sky.serve import constants as serve_constants
from sky.serve import kubernetes_identity
from sky.serve import ordinary_launch_binding
from sky.serve import pool_capacity_observation
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_allocation
from sky.serve import reserved_fill_planner
from sky.serve import reserved_fill_reclaim_attestation
from sky.serve import reserved_fill_reclaim_proofs
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import serve_utils
from sky.serve import service_spec
from sky.serve import zero_cost_actuation
from sky.server import constants as server_constants
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import reserved_fill_admission
from sky.skylet import constants as skylet_constants
from sky.utils import common_utils
from sky.utils.db import migration_utils

_SERVICE = 'svc'
_SERVICE_HASH = 'service-hash'
_OWNER = (17, '10.0.0.17')
_CONTEXT = 'research-east'
_UID = 'physical-uid'
_POOL_KEY = json.dumps(['v2', _UID, ['a100-80gb', 'h200']])
_PEER_SERVICE = 'svc-b'
_PEER_HASH = 'peer-service-hash'
_PEER_OWNER = (18, '10.0.0.18')
_CONTROLLER_PORT = 8123
_PEER_CONTROLLER_PORT = 8124
_CONTROLLER_INCARNATION = uuid.UUID('11111111-1111-4111-8111-111111111111')
_PEER_CONTROLLER_INCARNATION = uuid.UUID('22222222-2222-4222-8222-222222222222')
_CONTROLLER_OWNER_EPOCH = 2
_BINDING_EPOCH = 1
_CREATOR_ID = 'reserved-fill-allocation-owner'
_CREATOR_NAME = 'reserved-fill-allocation-owner@example.com'
_WORKSPACE = 'workspace-a'


def _worker_projection(card: str,
                       candidate_id: str,
                       context: str = _CONTEXT,
                       count: int = 1) -> dict[str, object]:
    return {
        'projection_version':
            (kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION),
        'candidate_id': candidate_id,
        'kubernetes_context': context,
        'namespace': 'default',
        'service_account_name': 'skyserve-worker',
        'scheduler_name': 'default-scheduler',
        'priority_class_name': 'skyserve-preemptible',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'provision_timeout': -1,
        'kueue_admission': {
            'local_queue_name': 'skyserve-reserved',
            'workload_priority_class_name': 'skyserve-preemptible',
        },
        'pod_identity_role_arn':
            ('arn:aws:iam::123456789012:role/skyserve-worker'),
        'accelerator_name': card,
        'accelerator_count': count,
        'accelerator_scheduling': {
            'label_key': 'nvidia.com/gpu.product',
            'label_values': [f'NVIDIA-{card}'],
            'resource_key': 'nvidia.com/gpu',
        },
        'cache': {
            'kind': 'none',
        },
        'scratch': {
            'kind': 'none',
        },
    }


_WORKER_PROJECTIONS = [
    _worker_projection('A100-80GB', 'kubernetes-0000'),
    _worker_projection('H200', 'kubernetes-0001'),
]
_PROJECTION_MAP = {
    projection['accelerator_name'].casefold():
        kubernetes_identity.worker_projection_sha256(projection)
    for projection in _WORKER_PROJECTIONS
}


def _service_spec(max_replicas: int = 8) -> service_spec.SkyServiceSpec:
    return service_spec.SkyServiceSpec(
        readiness_path='/health',
        initial_delay_seconds=0,
        readiness_timeout_seconds=5,
        endpoint_probe_interval_seconds=1,
        lb_stream_timeout_seconds=10,
        min_replicas=0,
        max_replicas=max_replicas,
        target_concurrency_per_replica=1,
        lb_high_availability=False,
    )


def _claim_policy_authority(
    semantic_hash: str,
) -> tuple[reserved_fill_reclaim_attestation.ReclaimClaimSetScope,
           reserved_fill_reclaim_attestation.ReclaimClaimAuthorization]:
    scope = reserved_fill_reclaim_attestation.ReclaimClaimSetScope(
        service_name=_SERVICE,
        service_incarnation=_SERVICE_HASH,
        service_version=1,
        semantic_hash=semantic_hash,
        edges=(reserved_fill_reclaim_attestation.ReclaimClaimEdge(
            pool_key=_POOL_KEY,
            access_context=_CONTEXT,
            physical_cluster_uid=_UID,
            accelerator_names=('a100-80gb', 'h200'),
            projected_admissions=(
                serve_state.reserved_fill_reclaim_projected_admissions(
                    _WORKER_PROJECTIONS,
                    access_context=_CONTEXT,
                    accelerator_names=('a100-80gb', 'h200'),
                    accelerator_count=1))),))
    identity = reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256='a' * 64,
        policy_revision='test-policy-v1',
        provider_inventory_sha256='b' * 64)
    return scope, reserved_fill_reclaim_attestation.ReclaimClaimAuthorization(
        identity=identity,
        gate_generation=1,
        scope=scope,
        completed_monotonic=time.monotonic())


def _claim_policy_authorizer(
    semantic_hash: str,
    *,
    on_authorize: typing.Callable[[], None] | None = None,
) -> typing.Callable[[
        reserved_fill_reclaim_attestation.ReclaimClaimSetScope,
        reserved_fill_reclaim_attestation.ReclaimPolicyIdentity, int
], reserved_fill_reclaim_attestation.ReclaimClaimAuthorization]:
    expected_scope, template = _claim_policy_authority(semantic_hash)

    def _authorize(
        scope: reserved_fill_reclaim_attestation.ReclaimClaimSetScope,
        identity: reserved_fill_reclaim_attestation.ReclaimPolicyIdentity,
        gate_generation: int,
    ) -> reserved_fill_reclaim_attestation.ReclaimClaimAuthorization:
        assert scope == expected_scope
        assert identity == template.identity
        assert gate_generation == template.gate_generation
        if on_authorize is not None:
            on_authorize()
        return reserved_fill_reclaim_attestation.ReclaimClaimAuthorization(
            identity=identity,
            gate_generation=gate_generation,
            scope=scope,
            completed_monotonic=time.monotonic())

    return _authorize


@pytest.fixture
def allocation_engine(observation_engine, monkeypatch):  # noqa: F811
    global_user_state_schema.user_table.create(observation_engine,
                                               checkfirst=True)
    config = migration_utils.get_alembic_config(observation_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, migration_utils.SERVE_VERSION)
    request_postgres._initialize_schema(observation_engine)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine',
                        observation_engine)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        observation_engine)
    monkeypatch.setattr(
        request_postgres, '_resolved_request_backend_capability', lambda:
        (request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE,
         request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE, True))
    with observation_engine.begin() as connection:
        connection.execute(global_user_state_schema.user_table.insert().values(
            id=_CREATOR_ID, name=_CREATOR_NAME, created_at=int(time.time())))
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO services
                    (name, hash, resource_scope, controller_pid,
                     controller_ip, status, current_version,
                     logical_replica_semantics, workspace,
                     owner_user_id, owner_user_name)
                VALUES (:name, :hash, :resource_scope, :controller_pid,
                        :controller_ip, 'READY', 1, 0, :workspace,
                        :owner_user_id, :owner_user_name)
            """), {
                'name': _SERVICE,
                'hash': _SERVICE_HASH,
                'resource_scope': _SERVICE_HASH,
                'controller_pid': _OWNER[0],
                'controller_ip': _OWNER[1],
                'workspace': _WORKSPACE,
                'owner_user_id': _CREATOR_ID,
                'owner_user_name': _CREATOR_NAME,
            })
        connection.execute(
            serve_state_schema.version_specs_table.insert().values(
                service_name=_SERVICE,
                version=1,
                spec=pickle.dumps(_service_spec()),
                yaml_content='service: v1\n',
                placement_catalog={
                    'schema_version': 1,
                    'entries': [],
                    'num_nodes': 1,
                },
                worker_placement_projections=_WORKER_PROJECTIONS))
        connection.execute(
            serve_state_schema.service_lifecycle_fences_table.insert().values(
                name=_SERVICE, epoch=1))
        connection.execute(
            sqlalchemy.text("""
                UPDATE reserved_fill_protocol_state
                SET claim_generation = 11
                WHERE id = 1
            """))
        connection.execute(
            serve_state_schema.reserved_fill_service_claim_sets_table.insert(
            ).values(service_name=_SERVICE,
                     claim_set_state='authoritative_v2',
                     generation=11,
                     edge_count=1,
                     semantic_hash='semantic-a',
                     service_version=1,
                     global_headroom=8,
                     utilization_ceiling=8,
                     heartbeat_ts=1))
        connection.execute(
            serve_state_schema.reserved_fill_pool_claims_table.insert().values(
                service_name=_SERVICE,
                pool_key=_POOL_KEY,
                legacy_pool_key=json.dumps([_CONTEXT, ['a100-80gb', 'h200']]),
                pool_position=0,
                access_context=_CONTEXT,
                physical_cluster_uid=_UID,
                accelerator_names=json.dumps(['a100-80gb', 'h200']),
                worker_projection_sha256_by_accelerator=_PROJECTION_MAP,
                service_generation=11,
                weight=1000,
                floor_replicas=0,
                gpus_per_replica=1,
                holdings_fill=0,
                effective_cap=8,
                launchable=1,
                heartbeat_ts=1))
    identity = reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256='a' * 64,
        policy_revision='test-policy-v1',
        provider_inventory_sha256='b' * 64)
    claimed_context = reserved_fill_reclaim_attestation.ReservedContextClaim(
        service_name=_SERVICE,
        service_version=1,
        service_generation=11,
        pool_key=_POOL_KEY,
        access_context=_CONTEXT,
        physical_cluster_uid=_UID,
        accelerator_names=('a100-80gb', 'h200'),
        projected_admissions=(
            serve_state.reserved_fill_reclaim_projected_admissions(
                _WORKER_PROJECTIONS,
                access_context=_CONTEXT,
                accelerator_names=('a100-80gb', 'h200'),
                accelerator_count=1)))
    evidence = reserved_fill_reclaim_attestation.ReclaimEnforcementEvidence(
        contract=(reserved_fill_reclaim_attestation.ReclaimEnforcementContract.
                  GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2),
        fleet_bundle_sha256=identity.fleet_bundle_sha256,
        policy_revision=identity.policy_revision,
        provider_inventory_sha256=identity.provider_inventory_sha256,
        claimed_contexts=(claimed_context,),
        completed_monotonic=1.0)
    receipt = reserved_fill_reclaim_attestation.activation_receipt(
        evidence,
        writer_image_digest='sha256:' + 'c' * 64,
        writer_deployment_generation='1',
        writer_deployment_uid='deployment-uid',
        writer_pod_inventory_count=1,
        writer_pod_inventory_sha256='d' * 64)
    activated = pool_capacity_observation.PoolCapacityObservationRepository(
        observation_engine).authorize_sequenced_reconciliation(
            expected_generation=0, receipt=receipt)
    assert activated.changed
    return observation_engine


def _location(
    card: str,
    context: str = _CONTEXT,
    count: int = 1,
) -> reserved_fill_planner.LocationSnapshot:
    location = reserved_fill_planner.spot_placer.Location(
        cloud=clouds.Kubernetes(),
        region=context,
        zone=None,
        accelerators={card: count},
        use_spot=False)
    return reserved_fill_planner.LocationSnapshot.from_pickleable(
        location.to_pickleable())


def _topology_snapshot(
    card: str,
    *,
    context: str = _CONTEXT,
    physical_uid: str = _UID,
    width: int = 1,
) -> reserved_fill_planner.PoolFillSnapshot:
    pool_key = reserved_capacity_broker.make_pool_key(
        context,
        card,
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=physical_uid)
    return reserved_fill_planner.PoolFillSnapshot(
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        pool_key=pool_key,
        physical_cluster_uid=physical_uid,
        service_generation=11,
        worker_projection_sha256_by_accelerator=((card.casefold(), 'a' * 64),),
        edge_cap=1,
        broker_slot_width=width,
        free_slots=1,
        free_slots_by_accelerator=((card.casefold(), 1),),
        grant=1,
        grant_epoch=23,
        observation_generation=1,
        observation_sequence=1,
        ordinary_zero_cost_admission_sequence=1,
        valid_until=10_000.0,
        locations=(_location(card, context, width),))


def _topology_edge(
    snapshot: reserved_fill_planner.PoolFillSnapshot,) -> dict[str, object]:
    identity = reserved_capacity_broker.parse_pool_identity(snapshot.pool_key)
    return {
        'pool_key': snapshot.pool_key,
        'physical_cluster_uid': snapshot.physical_cluster_uid,
        'service_generation': snapshot.service_generation,
        'effective_cap': snapshot.edge_cap,
        'launchable': 1,
        'access_context': snapshot.locations[0].region,
        'accelerator_names': json.dumps(list(identity.gpu_names)),
        'worker_projection_sha256_by_accelerator': dict(
            snapshot.worker_projection_sha256_by_accelerator),
        'gpus_per_replica': snapshot.broker_slot_width,
    }


def test_snapshot_topology_keys_uniqueness_by_physical_card() -> None:
    a100 = _topology_snapshot('A100')
    h200 = _topology_snapshot('H200', width=8)
    validate = (reserved_fill_allocation.ReservedFillAllocationRepository.
                _validate_snapshot_topology)

    assert validate((a100, h200), 11,
                    (_topology_edge(a100), _topology_edge(h200)))

    a100_alias = _topology_snapshot('A100', context='research-east-alias')
    assert not validate((a100, a100_alias), 11,
                        (_topology_edge(a100), _topology_edge(a100_alias)))


def _commit_evidence(
    engine: sqlalchemy.engine.Engine,
    *,
    feed_by_accelerator: dict[str, int] | None = None,
    free_slots: int = 3,
    broker_slot_width: int = 1,
    observation_context: str = _CONTEXT,
    placement_context: str = _CONTEXT,
    accelerator_names: tuple[str, ...] = ('a100-80gb', 'h200'),
    edge_cap: int = 8,
    grant: int = 8,
    raw_grant: int | None = None,
) -> tuple[pool_capacity_observation.PoolCapacityObservation,
           reserved_fill_planner.PoolFillSnapshot]:
    display_names = {
        'a100': 'A100',
        'a100-80gb': 'A100-80GB',
        'h200': 'H200',
    }
    worker_projections = [
        _worker_projection(display_names[name], f'kubernetes-{index:04d}',
                           placement_context, broker_slot_width)
        for index, name in enumerate(accelerator_names)
    ]
    projection_map = {
        projection['accelerator_name'].casefold():
            kubernetes_identity.worker_projection_sha256(projection)
        for projection in worker_projections
    }
    repository = pool_capacity_observation.PoolCapacityObservationRepository(
        engine)
    pool_key = reserved_capacity_broker.make_pool_key(
        placement_context,
        accelerator_names,
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=_UID)
    lease = repository.begin_observation(pool_key=pool_key,
                                         physical_cluster_uid=_UID,
                                         accelerator_names=accelerator_names,
                                         access_context=observation_context,
                                         lease_duration_seconds=60,
                                         authority_horizon_seconds=600)
    remaining = free_slots
    observed_gpu_counts: dict[str, int] = {}
    for index, name in enumerate(accelerator_names):
        count = remaining if index == len(accelerator_names) - 1 else min(
            1, remaining)
        observed_gpu_counts[name] = count
        remaining -= count
    observation = repository.complete_success(
        lease,
        pool_capacity_observation.PoolCapacitySuccess.from_counts(
            free_slots, observed_gpu_counts),
        access_context=lease.access_context)
    observed_slot_counts = {
        name: count // broker_slot_width
        for name, count in observed_gpu_counts.items()
    }
    service_counts = ({
        name: count for name, count in observed_slot_counts.items() if count > 0
    } if feed_by_accelerator is None else feed_by_accelerator)
    feed = sum(service_counts.values())
    exact_feed = {
        _SERVICE: service_counts,
        reserved_capacity_broker.OBSERVED_FREE_BY_ACCELERATOR_KEY: observed_slot_counts,
        reserved_capacity_broker.SPENDABLE_FREE_BY_ACCELERATOR_KEY: observed_slot_counts,
        reserved_capacity_broker.BROKER_SLOT_WIDTH_KEY: broker_slot_width,
    }
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_pool_claims_table).where(
                    serve_state_schema.reserved_fill_pool_claims_table.c.
                    service_name == _SERVICE).values(
                        pool_key=pool_key,
                        legacy_pool_key=json.dumps(
                            [placement_context,
                             list(accelerator_names)]),
                        access_context=placement_context,
                        accelerator_names=json.dumps(list(accelerator_names)),
                        gpus_per_replica=broker_slot_width,
                        effective_cap=edge_cap,
                        worker_projection_sha256_by_accelerator=(
                            projection_map)))
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name ==
                _SERVICE,
                serve_state_schema.version_specs_table.c.version == 1).values(
                    worker_placement_projections=worker_projections))
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO reserved_fill_rounds
                    (pool_key, round_id, snapshot_time, epoch,
                     protocol_version, claim_generations, grants, feeds,
                     feed_by_accelerator, raw_grants, feed_state,
                     sum_holdings, last_observed_free,
                     last_observed_free_ts, phantom_streak, fence_pending,
                     observation_generation, observation_sequence,
                     observation_materialization_sequence,
                     observation_payload_sha256)
                VALUES
                    (:pool_key, 7, :snapshot_time, 23, 2,
                     :claim_generations, :grants, :feeds,
                     :feed_by_accelerator, :raw_grants, '{}', 0,
                     :last_observed_free, :last_observed_free_ts, 0, 0,
                     :observation_generation, :observation_sequence,
                     :observation_materialization_sequence,
                     :observation_payload_sha256)
            """), {
                'pool_key': pool_key,
                'snapshot_time': observation.observed_at,
                'claim_generations': json.dumps({_SERVICE: 11}),
                'grants': json.dumps({_SERVICE: grant}),
                'feeds': json.dumps({_SERVICE: feed}),
                'feed_by_accelerator': json.dumps(exact_feed),
                'raw_grants': json.dumps(
                    {_SERVICE: grant if raw_grant is None else raw_grant}),
                'last_observed_free': sum(observed_slot_counts.values()),
                'last_observed_free_ts': observation.observed_at,
                'observation_generation': observation.observation_generation,
                'observation_sequence': observation.observation_sequence,
                'observation_materialization_sequence':
                    (observation.materialization_sequence),
                'observation_payload_sha256': observation.payload_sha256,
            })
    snapshot = reserved_fill_planner.PoolFillSnapshot(
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        pool_key=pool_key,
        physical_cluster_uid=_UID,
        service_generation=11,
        worker_projection_sha256_by_accelerator=tuple(
            sorted(projection_map.items())),
        edge_cap=edge_cap,
        broker_slot_width=broker_slot_width,
        free_slots=feed,
        free_slots_by_accelerator=tuple(service_counts.items()),
        grant=grant,
        grant_epoch=23,
        observation_generation=observation.observation_generation,
        observation_sequence=observation.observation_sequence,
        ordinary_zero_cost_admission_sequence=(
            observation.ordinary_admission_sequence),
        valid_until=observation.valid_until,
        locations=tuple(
            _location(display_names[name], placement_context, broker_slot_width)
            for name in accelerator_names))
    return observation, snapshot


def _repository(
    engine: sqlalchemy.engine.Engine,
) -> reserved_fill_allocation.ReservedFillAllocationRepository:
    return reserved_fill_allocation.ReservedFillAllocationRepository(engine)


def _insert_peer_service(
    engine: sqlalchemy.engine.Engine,
    *,
    with_claim: bool,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO services
                    (name, hash, resource_scope, controller_pid,
                     controller_ip, status, current_version,
                     logical_replica_semantics, workspace,
                     owner_user_id, owner_user_name)
                VALUES (:name, :hash, :resource_scope, :controller_pid,
                        :controller_ip, 'READY', 1, 0, :workspace,
                        :owner_user_id, :owner_user_name)
            """), {
                'name': _PEER_SERVICE,
                'hash': _PEER_HASH,
                'resource_scope': _PEER_HASH,
                'controller_pid': _PEER_OWNER[0],
                'controller_ip': _PEER_OWNER[1],
                'workspace': _WORKSPACE,
                'owner_user_id': _CREATOR_ID,
                'owner_user_name': _CREATOR_NAME,
            })
        connection.execute(
            serve_state_schema.version_specs_table.insert().values(
                service_name=_PEER_SERVICE,
                version=1,
                spec=pickle.dumps(_service_spec()),
                yaml_content='service: v1\n',
                placement_catalog={
                    'schema_version': 1,
                    'entries': [],
                    'num_nodes': 1,
                },
                worker_placement_projections=_WORKER_PROJECTIONS))
        connection.execute(
            serve_state_schema.service_lifecycle_fences_table.insert().values(
                name=_PEER_SERVICE, epoch=1))
        if not with_claim:
            return
        connection.execute(
            serve_state_schema.reserved_fill_service_claim_sets_table.insert(
            ).values(service_name=_PEER_SERVICE,
                     claim_set_state='authoritative_v2',
                     generation=11,
                     edge_count=1,
                     semantic_hash='semantic-b',
                     service_version=1,
                     global_headroom=4,
                     utilization_ceiling=4,
                     heartbeat_ts=1))
        connection.execute(
            serve_state_schema.reserved_fill_pool_claims_table.insert().values(
                service_name=_PEER_SERVICE,
                pool_key=_POOL_KEY,
                legacy_pool_key=json.dumps([_CONTEXT, ['a100-80gb', 'h200']]),
                pool_position=0,
                access_context=_CONTEXT,
                physical_cluster_uid=_UID,
                accelerator_names=json.dumps(['a100-80gb', 'h200']),
                worker_projection_sha256_by_accelerator=_PROJECTION_MAP,
                service_generation=11,
                weight=1000,
                floor_replicas=0,
                gpus_per_replica=1,
                holdings_fill=0,
                effective_cap=4,
                launchable=1,
                heartbeat_ts=1))


def _ordinary_replica(replica_id: int) -> replica_managers.ReplicaInfo:
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'{_PEER_SERVICE}-{replica_id}',
        replica_port='8080',
        is_spot=False,
        location=_location('A100-80GB').to_location(),
        version=1,
        resources_override={'accelerators': {
            'A100-80GB': 1
        }})
    info.is_zero_cost = True
    return info


def _typed_fill_replica(
    service_name: str,
    replica_id: int,
    snapshot: reserved_fill_planner.PoolFillSnapshot,
    allocation: reserved_fill_planner.AuthenticatedAllocationMap,
    *,
    card: str,
    intent_key: str = 'd' * 64,
    planned_capacity: int = 1,
) -> replica_managers.ReplicaInfo:
    location = _location(card, count=snapshot.broker_slot_width).to_location()
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'{service_name}-{replica_id}',
        replica_port='8080',
        is_spot=False,
        location=location,
        version=1,
        resources_override=location.to_dict(),
        planned_capacity=planned_capacity)
    info.reserved_fill = True
    info.is_zero_cost = True
    info.reserved_fill_pool_key = snapshot.pool_key
    info.reserved_fill_service_generation = snapshot.service_generation
    info.reserved_fill_physical_cluster_uid = snapshot.physical_cluster_uid
    info.reserved_fill_kubernetes_context = location.region
    info.reserved_fill_allocation_generation = allocation.allocation_generation
    info.reserved_fill_allocation_input_sha256 = (
        allocation.allocation_input_sha256)
    info.reserved_fill_allocation_claim_generation = (
        allocation.allocation_claim_generation)
    info.reserved_fill_reconciliation_gate_generation = (
        allocation.reconciliation_gate_generation)
    info.reserved_fill_reclaim_fleet_bundle_sha256 = (
        allocation.reclaim_fleet_bundle_sha256)
    info.reserved_fill_reclaim_policy_revision = (
        allocation.reclaim_policy_revision)
    info.reserved_fill_reclaim_provider_inventory_sha256 = (
        allocation.reclaim_provider_inventory_sha256)
    info.reserved_fill_worker_projection_sha256 = dict(
        snapshot.worker_projection_sha256_by_accelerator)[card.casefold()]
    info.reserved_fill_observation_generation = (
        snapshot.observation_generation)
    info.reserved_fill_observation_sequence = snapshot.observation_sequence
    info.reserved_fill_intent_idempotency_key = intent_key
    return info


def _publish_current_allocation(
    engine: sqlalchemy.engine.Engine,
    snapshot: reserved_fill_planner.PoolFillSnapshot,
) -> reserved_fill_planner.AuthenticatedAllocationMap:
    allocation = _repository(engine).publish(
        _SERVICE,
        expected_service_hash=_SERVICE_HASH,
        expected_controller_owner=_OWNER,
        expected_claim_generation=11,
        expected_gate_generation=1,
        pool_snapshots=(snapshot,))
    assert allocation is not None
    with engine.begin() as connection:
        connection.execute(
            serve_state_schema.reserved_fill_lease_table.insert().values(
                id=1, epoch=7))
    return allocation


def _activate_durable_intent_service(
    engine: sqlalchemy.engine.Engine,
    *,
    service_name: str = _SERVICE,
    service_hash: str = _SERVICE_HASH,
    controller_owner: tuple[int, str] = _OWNER,
    controller_port: int = _CONTROLLER_PORT,
    controller_incarnation: uuid.UUID = _CONTROLLER_INCARNATION,
) -> str:
    """Install the exact durable-intent controller authority under test."""
    with engine.begin() as connection:
        result = connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == service_name).
            values(
                pool=0,
                lifecycle_epoch=1,
                controller_port=controller_port,
                controller_incarnation=controller_incarnation,
                controller_owner_epoch=_CONTROLLER_OWNER_EPOCH,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode=(
                    ordinary_launch_binding.BindingMode.BOUND.value),
                ordinary_launch_binding_epoch=_BINDING_EPOCH,
                non_pool_launch_binding_capable=True,
                non_pool_launch_controller_incarnation=(controller_incarnation),
                non_pool_launch_binding_protocol_version=(
                    ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
                non_pool_launch_capability_profile_set_digest=(
                    ordinary_launch_binding.
                    supported_non_pool_profile_set_digest()),
                non_pool_launch_capability_cohort_epoch=(
                    ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
                non_pool_launch_receipt_protocol_version=(
                    ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION),
                reserved_fill_actuation_mode=(
                    zero_cost_actuation.ActuationMode.DURABLE_INTENT.value),
                reserved_fill_actuation_epoch=1,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=(
                    controller_incarnation),
                reserved_fill_actuation_protocol_version=1))
    assert result.rowcount == 1
    return serve_utils.make_controller_owner_fingerprint(
        service_hash, controller_owner[0], controller_owner[1], controller_port)


def _plan_durable_fill(
    allocation: reserved_fill_planner.AuthenticatedAllocationMap,
    controller_owner: str,
    *,
    service_hash: str = _SERVICE_HASH,
    max_replicas: int = 8,
    planned_replicas: int = 0,
    capacity_unit: reserved_fill_planner.FillCapacityUnit = (
        reserved_fill_planner.FillCapacityUnit.PHYSICAL),
) -> reserved_fill_planner.FillPlan:
    return reserved_fill_planner.ReservedFillPlanner.plan(
        policy_revision=1,
        reconcile_generation=1,
        allocation_map=allocation,
        service_incarnation=service_hash,
        service_version=1,
        controller_owner=controller_owner,
        max_replicas=max_replicas,
        planned_replicas=planned_replicas,
        capacity_unit=capacity_unit)


def _grant_durable_plan(
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    plan: reserved_fill_planner.FillPlan,
    *,
    max_capacity: int,
    controller_incarnation: uuid.UUID = _CONTROLLER_INCARNATION,
) -> tuple[zero_cost_actuation.ZeroCostActuationRepository,
           reserved_fill_planner.FillCommitResult]:
    repository = zero_cost_actuation.ZeroCostActuationRepository(engine)
    receipt = repository.grant_plan(
        service_name,
        plan,
        max_capacity=max_capacity,
        expected_controller_incarnation=controller_incarnation,
        expected_controller_owner_epoch=_CONTROLLER_OWNER_EPOCH)
    accepted_keys = {item.intent_idempotency_key for item in receipt.accepted}
    if accepted_keys:
        _install_fresh_provider_proofs(
            engine,
            tuple(intent for intent in plan.intents
                  if intent.idempotency_key in accepted_keys))
    return repository, receipt


def _install_fresh_provider_proofs(
    engine: sqlalchemy.engine.Engine,
    intents: tuple[reserved_fill_planner.FillIntent, ...],
) -> None:
    """Publish the exact provider-free prerequisite for active test intents."""
    assert intents
    first = intents[0]
    identity = reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256=first.reclaim_fleet_bundle_sha256,
        policy_revision=first.reclaim_policy_revision,
        provider_inventory_sha256=first.reclaim_provider_inventory_sha256)
    repository = reserved_fill_reclaim_proofs.ReclaimProviderProofRepository(
        engine)
    try:
        authorities = {(intent.allowed_locations[0].region,
                        intent.physical_cluster_uid) for intent in intents}
        for context, physical_uid in sorted(authorities):
            payload = {
                'aws': {},
                'kubernetes': {
                    'physical_cluster_uid': physical_uid,
                },
            }
            repository.renew(
                identity=identity,
                gate_generation=first.reconciliation_gate_generation,
                kubernetes_context=context,
                deadline_monotonic=(time.monotonic() +
                                    reserved_fill_reclaim_attestation.
                                    PROVIDER_PROOF_REFRESH_TIMEOUT_SECONDS),
                prove=lambda payload=payload: reserved_fill_reclaim_proofs.
                ReclaimProviderProofCandidate(proof_payload=payload,
                                              oldest_completed_monotonic=time.
                                              monotonic()),
                validate=lambda _payload: True,
                minimum_remaining_seconds=(
                    reserved_fill_reclaim_attestation.
                    PROVIDER_PROOF_RENEW_MIN_REMAINING_SECONDS))
    finally:
        repository._proof_engine.dispose()


def _lease_next(
    repository: zero_cost_actuation.ZeroCostActuationRepository,
    service_name: str,
    pool_key: str,
) -> zero_cost_actuation.IntentLease | None:
    return repository.lease_next(service_name=service_name,
                                 pool_key=pool_key,
                                 owner=uuid.uuid4(),
                                 lease_seconds=30)


def _stage_durable_fill(
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    replica_id: int,
    info: replica_managers.ReplicaInfo,
    lease: zero_cost_actuation.IntentLease,
    *,
    controller_owner: tuple[int, str] = _OWNER,
) -> bool:
    """Commit one lease through the complete production admission graph."""
    if service_name == _SERVICE:
        service_hash = _SERVICE_HASH
    elif service_name == _PEER_SERVICE:
        service_hash = _PEER_HASH
    else:
        raise ValueError(f'Unknown test service: {service_name!r}')
    with engine.connect() as current_connection:
        service = current_connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.workspace,
                serve_state_schema.services_table.c.lifecycle_epoch,
                serve_state_schema.services_table.c.controller_incarnation,
                serve_state_schema.services_table.c.controller_owner_epoch).
            where(serve_state_schema.services_table.c.name ==
                  service_name)).mappings().one()
    controller_incarnation = service['controller_incarnation']
    controller_owner_epoch = service['controller_owner_epoch']
    lifecycle_epoch = service['lifecycle_epoch']
    assert service['workspace'] == _WORKSPACE
    assert isinstance(controller_incarnation, uuid.UUID)
    assert isinstance(controller_owner_epoch, int)
    assert isinstance(lifecycle_epoch, int)
    authority = ordinary_launch_binding.ControllerBindingAuthority(
        service_name=service_name,
        service_hash=service_hash,
        service_workspace=_WORKSPACE,
        service_lifecycle_epoch=lifecycle_epoch,
        controller_pid=controller_owner[0],
        controller_ip=controller_owner[1],
        controller_incarnation=controller_incarnation,
        controller_owner_epoch=controller_owner_epoch,
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        binding_epoch=_BINDING_EPOCH,
        non_pool_capable=True,
        non_pool_binding_protocol_version=(
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
        non_pool_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        non_pool_capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        non_pool_receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))
    launch_context = {
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: service_name,
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: service_hash,
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 1,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY:
            controller_owner[0],
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY:
            controller_owner[1],
        ordinary_launch_binding.REPLICA_ID_KEY: replica_id,
        ordinary_launch_binding.REPLICA_RECORD_ID_KEY: info.replica_record_id,
        ordinary_launch_binding.LIFECYCLE_EPOCH_KEY: lifecycle_epoch,
        ordinary_launch_binding.BINDING_EPOCH_KEY: _BINDING_EPOCH,
        ordinary_launch_binding.CONTROLLER_INCARNATION_KEY:
            str(controller_incarnation),
        ordinary_launch_binding.CONTROLLER_OWNER_EPOCH_KEY: controller_owner_epoch,
    }
    body = payloads.LaunchBody(
        task=('name: allocation-fill\nresources:\n  accelerators: '
              'A100-80GB:1\n'),
        cluster_name=info.cluster_name,
        is_launched_by_sky_serve_controller=True,
        client_api_version=server_constants.API_VERSION,
        extra_launch_context=launch_context,
        env_vars={
            skylet_constants.USER_ID_ENV_VAR: _CREATOR_ID,
            skylet_constants.USER_ENV_VAR: _CREATOR_NAME,
        },
        override_skypilot_config={'active_workspace': _WORKSPACE})
    prepared = sdk.PreparedLaunchRequest(sdk._canonical_launch_body_bytes(body))
    info.status_property.sky_launch_status = common_utils.ProcessStatus.RUNNING
    spec = reserved_fill_admission.AdmissionSpec(
        prepared_request=prepared,
        submission_id=uuid.uuid5(
            uuid.UUID('33333333-3333-4333-8333-333333333333'),
            f'{service_name}:{replica_id}:{info.replica_record_id}'),
        authority=authority,
        replica_info=info,
        actuation_lease=lease,
        launch_limit=32)
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            staged, _ = reserved_fill_admission._stage_and_bind(
                connection, spec, 7, require_existing=False)
        except reserved_fill_admission._Rejected:
            transaction.rollback()
            return False
        transaction.commit()
    staged.publish_after_commit()
    return True


def _typed_fill_for_lease(
    service_name: str,
    replica_id: int,
    snapshot: reserved_fill_planner.PoolFillSnapshot,
    allocation: reserved_fill_planner.AuthenticatedAllocationMap,
    lease: zero_cost_actuation.IntentLease,
) -> replica_managers.ReplicaInfo:
    return _typed_fill_replica(
        service_name,
        replica_id,
        snapshot,
        allocation,
        card=lease.intent.accelerator,
        intent_key=lease.intent.idempotency_key,
        planned_capacity=lease.intent.capacity_unit.intent_cost(
            lease.intent.accelerator_count))


def _persisted_replica_count(engine: sqlalchemy.engine.Engine) -> int:
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE)).scalar_one()


def _publish_split_round(
    engine: sqlalchemy.engine.Engine,
    observation: pool_capacity_observation.PoolCapacityObservation,
    *,
    peer_feed: int,
) -> None:
    assert isinstance(observation.payload,
                      pool_capacity_observation.PoolCapacitySuccess)
    observed_counts = dict(observation.payload.free_gpus_by_accelerator)
    service_counts = {'a100-80gb': 1}
    peer_counts = ({} if peer_feed == 0 else {'h200': peer_feed})
    exact_feed = {
        _SERVICE: service_counts,
        _PEER_SERVICE: peer_counts,
        reserved_capacity_broker.OBSERVED_FREE_BY_ACCELERATOR_KEY: observed_counts,
        reserved_capacity_broker.BROKER_SLOT_WIDTH_KEY: 1,
    }
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_pool_claims_table).where(
                    serve_state_schema.reserved_fill_pool_claims_table.c.
                    service_name == _SERVICE).values(effective_cap=4))
        connection.execute(
            sqlalchemy.text("""
                UPDATE reserved_fill_rounds
                SET snapshot_time = :snapshot_time,
                    claim_generations = :claim_generations,
                    grants = :grants,
                    feeds = :feeds,
                    feed_by_accelerator = :feed_by_accelerator,
                    raw_grants = :raw_grants,
                    last_observed_free = :last_observed_free,
                    last_observed_free_ts = :last_observed_free_ts,
                    observation_generation = :observation_generation,
                    observation_sequence = :observation_sequence,
                    observation_materialization_sequence =
                        :observation_materialization_sequence,
                    observation_payload_sha256 = :observation_payload_sha256
                WHERE pool_key = :pool_key
            """), {
                'snapshot_time': observation.observed_at,
                'claim_generations': json.dumps({
                    _SERVICE: 11,
                    _PEER_SERVICE: 11,
                }),
                'grants': json.dumps({
                    _SERVICE: 4,
                    _PEER_SERVICE: 4,
                }),
                'feeds': json.dumps({
                    _SERVICE: 1,
                    _PEER_SERVICE: peer_feed,
                }),
                'feed_by_accelerator': json.dumps(exact_feed),
                'raw_grants': json.dumps({
                    _SERVICE: 4,
                    _PEER_SERVICE: 4,
                }),
                'last_observed_free': observation.payload.free_gpus,
                'last_observed_free_ts': observation.observed_at,
                'observation_generation': observation.observation_generation,
                'observation_sequence': observation.observation_sequence,
                'observation_materialization_sequence':
                    (observation.materialization_sequence),
                'observation_payload_sha256': observation.payload_sha256,
                'pool_key': _POOL_KEY,
            })


def _service_snapshot(
    base: reserved_fill_planner.PoolFillSnapshot,
    observation: pool_capacity_observation.PoolCapacityObservation,
    *,
    service_name: str,
    peer_feed: int = 1,
) -> reserved_fill_planner.PoolFillSnapshot:
    if service_name == _SERVICE:
        exact_feed = (('a100-80gb', 1),)
        free_slots = 1
    else:
        exact_feed = (() if peer_feed == 0 else (('h200', peer_feed),))
        free_slots = peer_feed
    return dataclasses.replace(
        base,
        edge_cap=4,
        free_slots=free_slots,
        free_slots_by_accelerator=exact_feed,
        grant=4,
        observation_generation=observation.observation_generation,
        observation_sequence=observation.observation_sequence,
        ordinary_zero_cost_admission_sequence=(
            observation.ordinary_admission_sequence),
        valid_until=observation.valid_until)


def test_pool_round_locks_use_global_order_not_claim_order() -> None:
    first_key = reserved_capacity_broker.make_pool_key(
        'context-z',
        'h200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='uid-z')
    second_key = reserved_capacity_broker.make_pool_key(
        'context-a',
        'a100',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='uid-a')

    def snapshot(pool_key: str, uid: str, context: str,
                 card: str) -> reserved_fill_planner.PoolFillSnapshot:
        location = reserved_fill_planner.spot_placer.Location(
            cloud=clouds.Kubernetes(),
            region=context,
            zone=None,
            accelerators={card: 1},
            use_spot=False)
        return reserved_fill_planner.PoolFillSnapshot(
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            pool_key=pool_key,
            physical_cluster_uid=uid,
            service_generation=1,
            worker_projection_sha256_by_accelerator=((card.casefold(),
                                                      'a' * 64),),
            edge_cap=0,
            free_slots=0,
            free_slots_by_accelerator=(),
            grant=0,
            grant_epoch=None,
            observation_generation=1,
            observation_sequence=1,
            ordinary_zero_cost_admission_sequence=1,
            valid_until=1.0,
            locations=(reserved_fill_planner.LocationSnapshot.from_pickleable(
                location.to_pickleable()),))

    snapshots = (
        snapshot(first_key, 'uid-z', 'context-z', 'H200'),
        snapshot(second_key, 'uid-a', 'context-a', 'A100'),
    )
    observed: list[str] = []

    class _Result:

        def __init__(self, pool_key: str) -> None:
            self._pool_key = pool_key

        def mappings(self) -> '_Result':
            return self

        def one_or_none(self) -> dict[str, str]:
            return {'pool_key': self._pool_key}

    class _Connection:

        def execute(self, statement):
            pool_key = next(
                value for value in statement.compile().params.values()
                if value in {first_key, second_key})
            observed.append(pool_key)
            return _Result(pool_key)

    locked = reserved_fill_allocation.ReservedFillAllocationRepository._lock_rounds(
        _Connection(), snapshots, read=False)  # type: ignore[arg-type]

    assert observed == sorted((first_key, second_key))
    assert locked is not None
    assert set(locked) == {first_key, second_key}


def test_publish_is_complete_authenticated_and_idempotent(
        allocation_engine) -> None:
    _, snapshot = _commit_evidence(allocation_engine)
    repository = _repository(allocation_engine)

    first = repository.publish(_SERVICE,
                               expected_service_hash=_SERVICE_HASH,
                               expected_controller_owner=_OWNER,
                               expected_claim_generation=11,
                               expected_gate_generation=1,
                               pool_snapshots=(snapshot,))
    assert first is not None
    assert first.allocation_generation == 1
    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) == first
    assert repository.current_generation(_SERVICE, _SERVICE_HASH, _OWNER) == 1

    retry = repository.publish(_SERVICE,
                               expected_service_hash=_SERVICE_HASH,
                               expected_controller_owner=_OWNER,
                               expected_claim_generation=11,
                               expected_gate_generation=1,
                               pool_snapshots=(snapshot,))
    assert retry == first


def test_publish_complete_map_keeps_healthy_pool_with_confirmed_phantom(
        allocation_engine) -> None:
    _, a100_snapshot = _commit_evidence(allocation_engine,
                                        free_slots=2,
                                        accelerator_names=('a100-80gb',),
                                        edge_cap=2,
                                        grant=2)
    h200_pool = reserved_capacity_broker.make_pool_key(
        _CONTEXT,
        'h200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=_UID)
    observation_repository = (
        pool_capacity_observation.PoolCapacityObservationRepository(
            allocation_engine))
    h200_lease = observation_repository.begin_observation(
        pool_key=h200_pool,
        physical_cluster_uid=_UID,
        accelerator_names=('h200',),
        access_context=_CONTEXT,
        lease_duration_seconds=60,
        authority_horizon_seconds=600)
    h200_observation = observation_repository.complete_success(
        h200_lease,
        pool_capacity_observation.PoolCapacitySuccess.from_counts(
            0, {'h200': 0}, present_accelerator_names=()),
        access_context=_CONTEXT)

    h200_projection = {'h200': _PROJECTION_MAP['h200']}
    exact_zero = {
        _SERVICE: {},
        reserved_capacity_broker.OBSERVED_FREE_BY_ACCELERATOR_KEY: {
            'h200': 0
        },
        reserved_capacity_broker.SPENDABLE_FREE_BY_ACCELERATOR_KEY: {
            'h200': 0
        },
        reserved_capacity_broker.BROKER_SLOT_WIDTH_KEY: 1,
    }
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_service_claim_sets_table).
            where(serve_state_schema.reserved_fill_service_claim_sets_table.c.
                  service_name == _SERVICE).values(edge_count=2))
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name ==
                _SERVICE,
                serve_state_schema.version_specs_table.c.version == 1).values(
                    worker_placement_projections=_WORKER_PROJECTIONS))
        connection.execute(
            serve_state_schema.reserved_fill_pool_claims_table.insert().values(
                service_name=_SERVICE,
                pool_key=h200_pool,
                legacy_pool_key=json.dumps([_CONTEXT, ['h200']]),
                pool_position=1,
                access_context=_CONTEXT,
                physical_cluster_uid=_UID,
                accelerator_names=json.dumps(['h200']),
                worker_projection_sha256_by_accelerator=h200_projection,
                service_generation=11,
                weight=1000,
                floor_replicas=0,
                gpus_per_replica=1,
                holdings_fill=0,
                effective_cap=0,
                launchable=1,
                heartbeat_ts=1))
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO reserved_fill_rounds
                    (pool_key, round_id, snapshot_time, epoch,
                     protocol_version, claim_generations, grants, feeds,
                     feed_by_accelerator, raw_grants, feed_state,
                     sum_holdings, last_observed_free,
                     last_observed_free_ts, phantom_streak, fence_pending,
                     observation_generation, observation_sequence,
                     observation_materialization_sequence,
                     observation_payload_sha256)
                VALUES
                    (:pool_key, 7, :snapshot_time, 23, 2,
                     :claim_generations, :grants, :feeds,
                     :feed_by_accelerator, :raw_grants, '{}', 0, 0,
                     :snapshot_time, :phantom_streak, 0,
                     :observation_generation, :observation_sequence,
                     :observation_materialization_sequence,
                     :observation_payload_sha256)
            """), {
                'pool_key': h200_pool,
                'snapshot_time': h200_observation.observed_at,
                'claim_generations': json.dumps({_SERVICE: 11}),
                'grants': json.dumps({_SERVICE: 0}),
                'feeds': json.dumps({_SERVICE: 0}),
                'feed_by_accelerator': json.dumps(exact_zero),
                'raw_grants': json.dumps({_SERVICE: 0}),
                'phantom_streak':
                    serve_constants.RESERVED_FILL_PHANTOM_CONFIRM_ROUNDS,
                'observation_generation':
                    h200_observation.observation_generation,
                'observation_sequence': h200_observation.observation_sequence,
                'observation_materialization_sequence':
                    h200_observation.materialization_sequence,
                'observation_payload_sha256': h200_observation.payload_sha256,
            })

    h200_snapshot = reserved_fill_planner.PoolFillSnapshot(
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        pool_key=h200_pool,
        physical_cluster_uid=_UID,
        service_generation=11,
        worker_projection_sha256_by_accelerator=tuple(h200_projection.items()),
        edge_cap=0,
        broker_slot_width=1,
        free_slots=0,
        free_slots_by_accelerator=(),
        grant=0,
        grant_epoch=23,
        observation_generation=h200_observation.observation_generation,
        observation_sequence=h200_observation.observation_sequence,
        ordinary_zero_cost_admission_sequence=(
            h200_observation.ordinary_admission_sequence),
        valid_until=h200_observation.valid_until,
        locations=(_location('H200'),))
    snapshots = (
        a100_snapshot,
        h200_snapshot,
    )
    allocation = _repository(allocation_engine).publish(
        _SERVICE,
        expected_service_hash=_SERVICE_HASH,
        expected_controller_owner=_OWNER,
        expected_claim_generation=11,
        expected_gate_generation=1,
        pool_snapshots=snapshots)

    assert allocation is not None
    assert allocation.allocation_generation == 1
    assert len(allocation.pool_snapshots) == 2
    published = {
        snapshot.pool_key: snapshot for snapshot in allocation.pool_snapshots
    }
    assert published[a100_snapshot.pool_key].free_slots == 2
    assert (published[a100_snapshot.pool_key].free_slots_by_accelerator == ((
        'a100-80gb', 2),))
    assert published[h200_pool].free_slots == 0
    assert published[h200_pool].free_slots_by_accelerator == ()
    assert published[h200_pool].grant == 0


def test_current_allocation_can_be_validated_on_caller_transaction(
        allocation_engine) -> None:
    _, snapshot = _commit_evidence(allocation_engine)
    repository = _repository(allocation_engine)
    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))
    assert allocation is not None

    with allocation_engine.begin() as connection:
        assert repository.read_current_in_connection(connection, _SERVICE,
                                                     _SERVICE_HASH,
                                                     _OWNER) == allocation


def test_current_allocation_prelocked_mode_does_not_relock_prefix(
        allocation_engine, monkeypatch) -> None:
    _, snapshot = _commit_evidence(allocation_engine)
    repository = _repository(allocation_engine)
    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))
    assert allocation is not None

    with allocation_engine.begin() as connection:
        assert repository._lock_protocol(connection, read=False) is not None
        assert repository._lock_service(connection,
                                        _SERVICE,
                                        _SERVICE_HASH,
                                        _OWNER,
                                        read=False) is not None

        def reject_relock(*args, **kwargs):
            del args, kwargs
            raise AssertionError('prelocked validation reacquired prefix')

        monkeypatch.setattr(repository, '_lock_protocol', reject_relock)
        monkeypatch.setattr(repository, '_lock_service', reject_relock)
        assert repository.read_current_in_connection(
            connection,
            _SERVICE,
            _SERVICE_HASH,
            _OWNER,
            protocol_and_service_prelocked=True) == allocation


def test_current_allocation_prelocked_mode_revalidates_broker_round(
        allocation_engine) -> None:
    _, snapshot = _commit_evidence(allocation_engine)
    repository = _repository(allocation_engine)
    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))
    assert allocation is not None
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_rounds_table).where(
                    serve_state_schema.reserved_fill_rounds_table.c.pool_key ==
                    _POOL_KEY).values(epoch=24))

    with allocation_engine.begin() as connection:
        assert repository._lock_protocol(connection, read=False) is not None
        assert repository._lock_service(connection,
                                        _SERVICE,
                                        _SERVICE_HASH,
                                        _OWNER,
                                        read=False) is not None
        assert repository.read_current_in_connection(
            connection,
            _SERVICE,
            _SERVICE_HASH,
            _OWNER,
            protocol_and_service_prelocked=True) is None


def test_current_allocation_connection_requires_caller_transaction(
        allocation_engine) -> None:
    repository = _repository(allocation_engine)
    with allocation_engine.connect() as connection:
        with pytest.raises(ValueError, match='active caller transaction'):
            repository.read_current_in_connection(connection, _SERVICE,
                                                  _SERVICE_HASH, _OWNER)


def test_publish_converts_width_eight_raw_gpus_exactly_once(
        allocation_engine) -> None:
    observation, snapshot = _commit_evidence(allocation_engine,
                                             free_slots=10,
                                             broker_slot_width=8)
    repository = _repository(allocation_engine)

    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))

    assert observation.payload.free_gpus == 10
    assert snapshot.broker_slot_width == 8
    assert snapshot.free_slots == 1
    assert snapshot.free_slots_by_accelerator == (('h200', 1),)
    assert allocation is not None
    assert repository.read_current(_SERVICE, _SERVICE_HASH,
                                   _OWNER) == allocation


def test_reader_rejects_tampered_broker_slot_width(allocation_engine) -> None:
    _, snapshot = _commit_evidence(allocation_engine,
                                   free_slots=10,
                                   broker_slot_width=8)
    allocation_repository = _repository(allocation_engine)
    allocation = allocation_repository.publish(
        _SERVICE,
        expected_service_hash=_SERVICE_HASH,
        expected_controller_owner=_OWNER,
        expected_claim_generation=11,
        expected_gate_generation=1,
        pool_snapshots=(snapshot,))
    assert allocation is not None

    table = serve_state_schema.reserved_fill_rounds_table
    with allocation_engine.begin() as connection:
        encoded = connection.execute(
            sqlalchemy.select(table.c.feed_by_accelerator).where(
                table.c.pool_key == _POOL_KEY)).scalar_one()
        envelope = json.loads(encoded)
        envelope[reserved_capacity_broker.BROKER_SLOT_WIDTH_KEY] = 4
        connection.execute(
            sqlalchemy.update(table).where(
                table.c.pool_key == _POOL_KEY).values(
                    feed_by_accelerator=json.dumps(envelope)))

    assert allocation_repository.read_current(_SERVICE, _SERVICE_HASH,
                                              _OWNER) is None


def test_physical_observation_is_shared_across_context_aliases(
        allocation_engine) -> None:
    alias_context = 'research-east-alias'
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE reserved_fill_pool_claims
                SET access_context = :access_context,
                    legacy_pool_key = :legacy_pool_key
                WHERE service_name = :service_name
                  AND pool_key = :pool_key
            """), {
                'access_context': alias_context,
                'legacy_pool_key': json.dumps(
                    [alias_context, ['a100-80gb', 'h200']]),
                'service_name': _SERVICE,
                'pool_key': _POOL_KEY,
            })

    observation, snapshot = _commit_evidence(allocation_engine,
                                             observation_context=_CONTEXT,
                                             placement_context=alias_context)
    repository = _repository(allocation_engine)

    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))

    assert observation.access_context == _CONTEXT
    assert snapshot.locations[0].region == alias_context
    assert allocation is not None
    assert repository.read_current(_SERVICE, _SERVICE_HASH,
                                   _OWNER) == allocation


@pytest.mark.parametrize('mutation', ['service_context', 'service_uid'])
def test_alias_consumption_preserves_service_access_and_uid_fences(
        allocation_engine, mutation: str) -> None:
    alias_context = 'research-east-alias'
    _, snapshot = _commit_evidence(allocation_engine,
                                   observation_context=_CONTEXT,
                                   placement_context=alias_context)
    values = {
        'access_context': alias_context,
        'legacy_pool_key': json.dumps([alias_context, ['a100-80gb', 'h200']]),
    }
    if mutation == 'service_context':
        values['access_context'] = 'unclaimed-context'
    else:
        values['physical_cluster_uid'] = 'replacement-physical-uid'
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_pool_claims_table).where(
                    serve_state_schema.reserved_fill_pool_claims_table.c.
                    service_name == _SERVICE,
                    serve_state_schema.reserved_fill_pool_claims_table.c.
                    pool_key == _POOL_KEY).values(**values))

    assert _repository(allocation_engine).publish(
        _SERVICE,
        expected_service_hash=_SERVICE_HASH,
        expected_controller_owner=_OWNER,
        expected_claim_generation=11,
        expected_gate_generation=1,
        pool_snapshots=(snapshot,)) is None


@pytest.mark.parametrize('mutation', [
    'owner',
    'claim_generation',
    'gate_generation',
    'round_feed',
    'round_digest',
    'round_materialization_sequence',
    'newer_blackout',
    'projection_source',
])
def test_publish_rejects_every_moved_or_unauthenticated_input(
        allocation_engine, mutation: str) -> None:
    observation, snapshot = _commit_evidence(allocation_engine)
    kwargs = {
        'expected_service_hash': _SERVICE_HASH,
        'expected_controller_owner': _OWNER,
        'expected_claim_generation': 11,
        'expected_gate_generation': 1,
        'pool_snapshots': (snapshot,),
    }
    if mutation == 'owner':
        kwargs['expected_controller_owner'] = (18, _OWNER[1])
    elif mutation == 'claim_generation':
        kwargs['expected_claim_generation'] = 10
    elif mutation == 'gate_generation':
        kwargs['expected_gate_generation'] = 2
    elif mutation == 'round_feed':
        with allocation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    UPDATE reserved_fill_rounds
                    SET feeds = '{"svc": 2}'
                    WHERE pool_key = :pool_key
                """), {'pool_key': _POOL_KEY})
    elif mutation == 'round_digest':
        with allocation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    UPDATE reserved_fill_rounds
                    SET observation_payload_sha256 = :digest
                    WHERE pool_key = :pool_key
                """), {
                    'pool_key': _POOL_KEY,
                    'digest': 'f' * 64,
                })
    elif mutation == 'round_materialization_sequence':
        with allocation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    UPDATE reserved_fill_rounds
                    SET observation_materialization_sequence =
                        observation_materialization_sequence + 1
                    WHERE pool_key = :pool_key
                """), {'pool_key': _POOL_KEY})
    elif mutation == 'newer_blackout':
        repository = pool_capacity_observation.PoolCapacityObservationRepository(
            allocation_engine)
        lease = repository.begin_observation(pool_key=_POOL_KEY,
                                             physical_cluster_uid=_UID,
                                             accelerator_names=('a100-80gb',
                                                                'h200'),
                                             access_context=_CONTEXT,
                                             lease_duration_seconds=60,
                                             authority_horizon_seconds=600)
        repository.complete_blackout(
            lease,
            pool_capacity_observation.PoolCapacityBlackout(
                reason=pool_capacity_observation.PoolCapacityBlackoutReason.
                PROVIDER_ERROR))
        assert lease.observation_generation > observation.observation_generation
    elif mutation == 'projection_source':
        changed_projections = copy.deepcopy(_WORKER_PROJECTIONS)
        changed_projections[0]['priority_value'] = -999
        with allocation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.version_specs_table).where(
                    serve_state_schema.version_specs_table.c.service_name ==
                    _SERVICE,
                    serve_state_schema.version_specs_table.c.version ==
                    1).values(worker_placement_projections=changed_projections))

    assert _repository(allocation_engine).publish(_SERVICE, **kwargs) is None


def test_reader_revalidates_owner_gate_and_claim_generation(
        allocation_engine) -> None:
    _, snapshot = _commit_evidence(allocation_engine)
    repository = _repository(allocation_engine)
    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))
    assert allocation is not None

    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE services SET controller_pid = 18 WHERE name = :name
            """), {'name': _SERVICE})
    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) is None


def test_reader_rejects_projection_source_drift(allocation_engine) -> None:
    _, snapshot = _commit_evidence(allocation_engine)
    repository = _repository(allocation_engine)
    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))
    assert allocation is not None

    changed_projections = copy.deepcopy(_WORKER_PROJECTIONS)
    changed_projections[0]['priority_value'] = -999
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name ==
                _SERVICE,
                serve_state_schema.version_specs_table.c.version == 1).values(
                    worker_placement_projections=changed_projections))

    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) is None


def test_reader_rejects_round_advance_after_publication(
        allocation_engine) -> None:
    _, snapshot = _commit_evidence(allocation_engine)
    repository = _repository(allocation_engine)
    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))
    assert allocation is not None
    assert repository.read_current(_SERVICE, _SERVICE_HASH,
                                   _OWNER) == allocation

    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE reserved_fill_rounds
                SET epoch = epoch + 1
                WHERE pool_key = :pool_key
            """), {'pool_key': _POOL_KEY})

    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) is None


def test_cross_service_ordinary_admission_invalidates_map_and_fill_persist(
        allocation_engine, monkeypatch) -> None:
    """Service B demand cannot race service A's stale free-slot authority."""
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine)
    allocation_repository = _repository(allocation_engine)
    allocation = allocation_repository.publish(
        _SERVICE,
        expected_service_hash=_SERVICE_HASH,
        expected_controller_owner=_OWNER,
        expected_claim_generation=11,
        expected_gate_generation=1,
        pool_snapshots=(snapshot,))
    assert allocation is not None
    _insert_peer_service(allocation_engine, with_claim=False)
    with allocation_engine.begin() as connection:
        connection.execute(
            serve_state_schema.reserved_fill_lease_table.insert().values(
                id=1, epoch=7))
    controller_owner = _activate_durable_intent_service(allocation_engine)
    plan = _plan_durable_fill(allocation, controller_owner, max_replicas=1)
    actuation_repository, receipt = _grant_durable_plan(allocation_engine,
                                                        _SERVICE,
                                                        plan,
                                                        max_capacity=1)
    assert len(receipt.accepted) == 1
    lease = _lease_next(actuation_repository, _SERVICE, snapshot.pool_key)
    assert lease is not None
    stale_fill = _typed_fill_replica(_SERVICE,
                                     1,
                                     snapshot,
                                     allocation,
                                     card=lease.intent.accelerator,
                                     intent_key=lease.intent.idempotency_key)

    ordinary = _ordinary_replica(1)
    assert serve_state.add_or_update_replicas(_PEER_SERVICE, [(1, ordinary)])
    assert ordinary.zero_cost_admission_sequence == 1
    with allocation_engine.connect() as connection:
        sequences = connection.execute(
            sqlalchemy.text("""
                SELECT zero_cost_admission_sequence,
                       ordinary_zero_cost_admission_sequence
                FROM reserved_fill_protocol_state WHERE id = 1
            """)).one()
    assert tuple(sequences) == (1, 1)
    assert allocation_repository.read_current(_SERVICE, _SERVICE_HASH,
                                              _OWNER) is None

    assert not _stage_durable_fill(allocation_engine, _SERVICE, 1, stale_fill,
                                   lease)
    with allocation_engine.connect() as connection:
        stale_row_count = connection.execute(
            sqlalchemy.select(sqlalchemy.text('count(*)')).select_from(
                serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE, serve_state_schema.replicas_table.c.replica_id ==
                    1)).scalar_one()
    assert stale_row_count == 0


def test_durable_intent_handoff_survives_successor_pool_epoch(
        allocation_engine, monkeypatch) -> None:
    """An admitted ledger debit transfers to its row after a heartbeat."""
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine,
                                   feed_by_accelerator={'a100-80gb': 1},
                                   free_slots=1)
    allocation = _publish_current_allocation(allocation_engine, snapshot)
    controller_port = 8123
    with allocation_engine.connect() as connection:
        controller_state = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.controller_incarnation,
                serve_state_schema.services_table.c.controller_owner_epoch).
            where(serve_state_schema.services_table.c.name ==
                  _SERVICE)).mappings().one()
    previous_controller_incarnation = controller_state['controller_incarnation']
    controller_owner_epoch = int(controller_state['controller_owner_epoch']) + 1
    assert isinstance(previous_controller_incarnation, uuid.UUID)
    controller_incarnation = uuid.uuid4()
    controller_owner = serve_utils.make_controller_owner_fingerprint(
        _SERVICE_HASH, _OWNER[0], _OWNER[1], controller_port)
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).
            where(serve_state_schema.services_table.c.name == _SERVICE).values(
                pool=0,
                lifecycle_epoch=1,
                controller_port=controller_port,
                controller_incarnation=controller_incarnation,
                controller_owner_epoch=controller_owner_epoch,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode=(
                    ordinary_launch_binding.BindingMode.BOUND.value),
                ordinary_launch_binding_epoch=_BINDING_EPOCH,
                non_pool_launch_binding_capable=True,
                non_pool_launch_controller_incarnation=(controller_incarnation),
                non_pool_launch_binding_protocol_version=(
                    ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
                non_pool_launch_capability_profile_set_digest=(
                    ordinary_launch_binding.
                    supported_non_pool_profile_set_digest()),
                non_pool_launch_capability_cohort_epoch=(
                    ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
                non_pool_launch_receipt_protocol_version=(
                    ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION),
                reserved_fill_actuation_mode=(
                    zero_cost_actuation.ActuationMode.DURABLE_INTENT.value),
                reserved_fill_actuation_epoch=1,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=(
                    controller_incarnation),
                reserved_fill_actuation_protocol_version=1))

    plan = reserved_fill_planner.ReservedFillPlanner.plan(
        policy_revision=1,
        reconcile_generation=1,
        allocation_map=allocation,
        service_incarnation=_SERVICE_HASH,
        service_version=1,
        controller_owner=controller_owner,
        max_replicas=8,
        planned_replicas=0,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)
    assert len(plan.intents) == 1
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        allocation_engine)
    receipt = repository.grant_plan(
        _SERVICE,
        plan,
        max_capacity=8,
        expected_controller_incarnation=controller_incarnation,
        expected_controller_owner_epoch=controller_owner_epoch)
    assert len(receipt.accepted) == 1
    _install_fresh_provider_proofs(allocation_engine, plan.intents)
    assert zero_cost_actuation.pending_pool_debits(
        snapshot.pool_key,
        engine=allocation_engine) == (zero_cost_actuation.PendingPoolDebit(
            service_name=_SERVICE,
            pool_key=snapshot.pool_key,
            accelerator='a100-80gb',
            replica_slots=1),)
    lease = repository.lease_next(service_name=_SERVICE,
                                  pool_key=snapshot.pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _typed_fill_replica(_SERVICE,
                               1,
                               snapshot,
                               allocation,
                               card=lease.intent.accelerator,
                               intent_key=lease.intent.idempotency_key)

    successor_snapshot = dataclasses.replace(snapshot,
                                             grant_epoch=snapshot.grant_epoch +
                                             1)
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_rounds_table).where(
                    serve_state_schema.reserved_fill_rounds_table.c.pool_key ==
                    snapshot.pool_key).values(
                        epoch=successor_snapshot.grant_epoch))
    successor_allocation = _repository(allocation_engine).publish(
        _SERVICE,
        expected_service_hash=_SERVICE_HASH,
        expected_controller_owner=_OWNER,
        expected_claim_generation=11,
        expected_gate_generation=1,
        pool_snapshots=(successor_snapshot,))
    assert successor_allocation is not None
    assert (successor_allocation.allocation_generation
            > allocation.allocation_generation)

    assert _stage_durable_fill(allocation_engine, _SERVICE, 1, info, lease)
    with allocation_engine.connect() as connection:
        intent_state = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation._INTENTS.c.state)).scalar_one()
    assert intent_state == zero_cost_actuation.IntentState.COMMITTED.value
    assert _persisted_replica_count(allocation_engine) == 1


def test_peer_fill_advances_only_total_and_remains_observable(
        allocation_engine, monkeypatch) -> None:
    """A broker-partitioned fill is not mistaken for ordinary demand."""
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    initial_observation, base = _commit_evidence(
        allocation_engine, feed_by_accelerator={'a100-80gb': 1}, free_slots=3)
    _insert_peer_service(allocation_engine, with_claim=True)
    _publish_split_round(allocation_engine, initial_observation, peer_feed=1)
    service_snapshot = _service_snapshot(base,
                                         initial_observation,
                                         service_name=_SERVICE)
    peer_snapshot = _service_snapshot(base,
                                      initial_observation,
                                      service_name=_PEER_SERVICE)
    repository = _repository(allocation_engine)
    service_allocation = repository.publish(_SERVICE,
                                            expected_service_hash=_SERVICE_HASH,
                                            expected_controller_owner=_OWNER,
                                            expected_claim_generation=11,
                                            expected_gate_generation=1,
                                            pool_snapshots=(service_snapshot,))
    peer_allocation = repository.publish(_PEER_SERVICE,
                                         expected_service_hash=_PEER_HASH,
                                         expected_controller_owner=_PEER_OWNER,
                                         expected_claim_generation=11,
                                         expected_gate_generation=1,
                                         pool_snapshots=(peer_snapshot,))
    assert service_allocation is not None
    assert peer_allocation is not None
    with allocation_engine.begin() as connection:
        connection.execute(
            serve_state_schema.reserved_fill_lease_table.insert().values(
                id=1, epoch=7))
    peer_controller_owner = _activate_durable_intent_service(
        allocation_engine,
        service_name=_PEER_SERVICE,
        service_hash=_PEER_HASH,
        controller_owner=_PEER_OWNER,
        controller_port=_PEER_CONTROLLER_PORT,
        controller_incarnation=_PEER_CONTROLLER_INCARNATION)
    plan = _plan_durable_fill(peer_allocation,
                              peer_controller_owner,
                              service_hash=_PEER_HASH,
                              max_replicas=1)
    actuation_repository, receipt = _grant_durable_plan(
        allocation_engine,
        _PEER_SERVICE,
        plan,
        max_capacity=1,
        controller_incarnation=_PEER_CONTROLLER_INCARNATION)
    assert len(receipt.accepted) == 1
    lease = _lease_next(actuation_repository, _PEER_SERVICE,
                        peer_snapshot.pool_key)
    assert lease is not None

    peer_fill = _typed_fill_replica(_PEER_SERVICE,
                                    1,
                                    peer_snapshot,
                                    peer_allocation,
                                    card=lease.intent.accelerator,
                                    intent_key=lease.intent.idempotency_key)
    assert _stage_durable_fill(allocation_engine,
                               _PEER_SERVICE,
                               1,
                               peer_fill,
                               lease,
                               controller_owner=_PEER_OWNER)
    with allocation_engine.connect() as connection:
        sequences = connection.execute(
            sqlalchemy.text("""
                SELECT zero_cost_admission_sequence,
                       ordinary_zero_cost_admission_sequence
                FROM reserved_fill_protocol_state WHERE id = 1
            """)).one()
    assert tuple(sequences) == (1, 0)
    # Service A's independently brokered grant remains current: peer fill did
    # not mutate the ordinary-demand invalidation generation.
    assert repository.read_current(_SERVICE, _SERVICE_HASH,
                                   _OWNER) == service_allocation

    observation_repository = (
        pool_capacity_observation.PoolCapacityObservationRepository(
            allocation_engine))
    second_lease = observation_repository.begin_observation(
        pool_key=_POOL_KEY,
        physical_cluster_uid=_UID,
        accelerator_names=('a100-80gb', 'h200'),
        access_context=_CONTEXT,
        lease_duration_seconds=60,
        authority_horizon_seconds=600)
    assert second_lease.observation_sequence == 1
    assert second_lease.ordinary_admission_sequence == 0
    second_observation = observation_repository.complete_success(
        second_lease,
        pool_capacity_observation.PoolCapacitySuccess.from_counts(
            2, {
                'a100-80gb': 1,
                'h200': 1,
            }),
        access_context=second_lease.access_context)
    _publish_split_round(allocation_engine, second_observation, peer_feed=0)
    refreshed_snapshot = _service_snapshot(base,
                                           second_observation,
                                           service_name=_SERVICE)
    refreshed = repository.publish(_SERVICE,
                                   expected_service_hash=_SERVICE_HASH,
                                   expected_controller_owner=_OWNER,
                                   expected_claim_generation=11,
                                   expected_gate_generation=1,
                                   pool_snapshots=(refreshed_snapshot,))
    assert refreshed is not None
    assert refreshed.allocation_generation == 2
    assert refreshed.pool_snapshots[0].observation_sequence == 1
    assert (refreshed.ordinary_zero_cost_admission_sequence_high_water == 0)
    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) == refreshed


def test_fill_persist_rejects_service_wide_intent_replay(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine,
                                   feed_by_accelerator={
                                       'a100-80gb': 1,
                                       'h200': 1,
                                   },
                                   free_slots=2)
    allocation = _publish_current_allocation(allocation_engine, snapshot)
    controller_owner = _activate_durable_intent_service(allocation_engine)
    plan = _plan_durable_fill(allocation, controller_owner, max_replicas=1)
    repository, receipt = _grant_durable_plan(allocation_engine,
                                              _SERVICE,
                                              plan,
                                              max_capacity=1)
    assert len(receipt.accepted) == 1
    lease = _lease_next(repository, _SERVICE, snapshot.pool_key)
    assert lease is not None
    first = _typed_fill_for_lease(_SERVICE, 1, snapshot, allocation, lease)
    replay = _typed_fill_for_lease(_SERVICE, 2, snapshot, allocation, lease)

    assert _stage_durable_fill(allocation_engine, _SERVICE, 1, first, lease)
    assert not _stage_durable_fill(allocation_engine, _SERVICE, 2, replay,
                                   lease)
    assert _persisted_replica_count(allocation_engine) == 1


def test_fill_persist_enforces_exact_card_slots_before_aggregate_exhaustion(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine,
                                   feed_by_accelerator={
                                       'a100-80gb': 1,
                                       'h200': 1,
                                   },
                                   free_slots=2)
    allocation = _publish_current_allocation(allocation_engine, snapshot)
    controller_owner = _activate_durable_intent_service(allocation_engine)
    plan = _plan_durable_fill(allocation, controller_owner, max_replicas=3)
    # The planner is the sole exact-card spender.  It can issue one authority
    # for each card, never a second A100 authority that consumes H200 feed.
    assert sorted(intent.accelerator.casefold() for intent in plan.intents) == [
        'a100-80gb', 'h200'
    ]
    repository, receipt = _grant_durable_plan(allocation_engine,
                                              _SERVICE,
                                              plan,
                                              max_capacity=3)
    assert len(receipt.accepted) == 2
    for replica_id in range(1, 3):
        lease = _lease_next(repository, _SERVICE, snapshot.pool_key)
        assert lease is not None
        info = _typed_fill_for_lease(_SERVICE, replica_id, snapshot, allocation,
                                     lease)
        assert _stage_durable_fill(allocation_engine, _SERVICE, replica_id,
                                   info, lease)
    assert _persisted_replica_count(allocation_engine) == 2


def test_fill_persist_enforces_aggregate_snapshot_slots(allocation_engine,
                                                        monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine,
                                   feed_by_accelerator={
                                       'a100-80gb': 1,
                                       'h200': 1,
                                   },
                                   free_slots=2)
    allocation = _publish_current_allocation(allocation_engine, snapshot)
    controller_owner = _activate_durable_intent_service(allocation_engine)
    plan = _plan_durable_fill(allocation, controller_owner, max_replicas=3)
    # Two aggregate slots produce exactly two durable authorities; there is
    # no third object that can reach replica/request admission.
    assert len(plan.intents) == 2
    repository, receipt = _grant_durable_plan(allocation_engine,
                                              _SERVICE,
                                              plan,
                                              max_capacity=3)
    assert len(receipt.accepted) == 2
    for replica_id in range(1, 3):
        lease = _lease_next(repository, _SERVICE, snapshot.pool_key)
        assert lease is not None
        info = _typed_fill_for_lease(_SERVICE, replica_id, snapshot, allocation,
                                     lease)
        assert _stage_durable_fill(allocation_engine, _SERVICE, replica_id,
                                   info, lease)
    assert _lease_next(repository, _SERVICE, snapshot.pool_key) is None
    assert _persisted_replica_count(allocation_engine) == 2


def test_fill_persist_enforces_locked_durable_service_maximum(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name ==
                _SERVICE,
                serve_state_schema.version_specs_table.c.version == 1).values(
                    spec=pickle.dumps(_service_spec(max_replicas=1))))
    _, snapshot = _commit_evidence(allocation_engine,
                                   feed_by_accelerator={
                                       'a100-80gb': 1,
                                       'h200': 1,
                                   },
                                   free_slots=2)
    allocation = _publish_current_allocation(allocation_engine, snapshot)
    controller_owner = _activate_durable_intent_service(allocation_engine)
    # Exercise the repository's locked maximum with a valid two-intent plan.
    # H200 still has authenticated feed, but the durable grant ceiling is one.
    plan = _plan_durable_fill(allocation, controller_owner, max_replicas=2)
    assert len(plan.intents) == 2
    repository, receipt = _grant_durable_plan(allocation_engine,
                                              _SERVICE,
                                              plan,
                                              max_capacity=1)
    assert len(receipt.accepted) == 1
    assert len(receipt.deferred) == 1
    lease = _lease_next(repository, _SERVICE, snapshot.pool_key)
    assert lease is not None
    first = _typed_fill_for_lease(_SERVICE, 1, snapshot, allocation, lease)
    assert _stage_durable_fill(allocation_engine, _SERVICE, 1, first, lease)
    assert _lease_next(repository, _SERVICE, snapshot.pool_key) is None
    assert _persisted_replica_count(allocation_engine) == 1


def test_fill_persist_rejects_locked_launch_blocking_service_without_effects(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine,
                                   feed_by_accelerator={'a100-80gb': 1},
                                   free_slots=1)
    allocation = _publish_current_allocation(allocation_engine, snapshot)
    controller_owner = _activate_durable_intent_service(allocation_engine)
    plan = _plan_durable_fill(allocation, controller_owner, max_replicas=1)
    repository, receipt = _grant_durable_plan(allocation_engine,
                                              _SERVICE,
                                              plan,
                                              max_capacity=1)
    assert len(receipt.accepted) == 1
    lease = _lease_next(repository, _SERVICE, snapshot.pool_key)
    assert lease is not None
    candidate = _typed_fill_for_lease(_SERVICE, 1, snapshot, allocation, lease)
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == _SERVICE).values(
                    status=serve_state.ServiceStatus.SHUTTING_DOWN.value))

    assert not _stage_durable_fill(allocation_engine, _SERVICE, 1, candidate,
                                   lease)
    assert _persisted_replica_count(allocation_engine) == 0
    with allocation_engine.connect() as connection:
        intent_state = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation._INTENTS.c.state)).scalar_one()
        sequences = connection.execute(
            sqlalchemy.text("""
                SELECT zero_cost_admission_sequence,
                       ordinary_zero_cost_admission_sequence
                FROM reserved_fill_protocol_state
                WHERE id = 1
            """)).one()
    assert intent_state == zero_cost_actuation.IntentState.ACTUATING.value
    assert tuple(sequences) == (0, 0)


def test_fill_persist_separates_physical_slot_debit_from_logical_ceiling(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == _SERVICE).values(
                    logical_replica_semantics=1))
    _, snapshot = _commit_evidence(allocation_engine,
                                   feed_by_accelerator={'h200': 2},
                                   free_slots=17,
                                   broker_slot_width=8)
    allocation = _publish_current_allocation(allocation_engine, snapshot)
    controller_owner = _activate_durable_intent_service(allocation_engine)
    plan = _plan_durable_fill(
        allocation,
        controller_owner,
        max_replicas=8,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    assert len(plan.intents) == 1
    repository, receipt = _grant_durable_plan(allocation_engine,
                                              _SERVICE,
                                              plan,
                                              max_capacity=8)
    assert len(receipt.accepted) == 1
    lease = _lease_next(repository, _SERVICE, snapshot.pool_key)
    assert lease is not None
    malformed_width = _typed_fill_for_lease(_SERVICE, 1, snapshot, allocation,
                                            lease)
    malformed_width.planned_capacity = 1
    assert not _stage_durable_fill(allocation_engine, _SERVICE, 1,
                                   malformed_width, lease)
    exact_width = _typed_fill_for_lease(_SERVICE, 2, snapshot, allocation,
                                        lease)
    assert _stage_durable_fill(allocation_engine, _SERVICE, 2, exact_width,
                               lease)

    # A fresh plan can describe the remaining physical slot, but grant
    # admission locks the committed width-eight row and defers that intent at
    # the logical service maximum.
    expanded_plan = _plan_durable_fill(
        allocation,
        controller_owner,
        max_replicas=16,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    assert len(expanded_plan.intents) == 2
    _, expanded_receipt = _grant_durable_plan(allocation_engine,
                                              _SERVICE,
                                              expanded_plan,
                                              max_capacity=8)
    assert len(expanded_receipt.accepted) == 1
    assert len(expanded_receipt.deferred) == 1
    assert _persisted_replica_count(allocation_engine) == 1


def test_concurrent_fill_persists_serialize_one_physical_slot(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine,
                                   feed_by_accelerator={'a100-80gb': 1},
                                   free_slots=1)
    allocation = _publish_current_allocation(allocation_engine, snapshot)
    controller_owner = _activate_durable_intent_service(allocation_engine)
    plan = _plan_durable_fill(allocation, controller_owner, max_replicas=1)
    repository, receipt = _grant_durable_plan(allocation_engine,
                                              _SERVICE,
                                              plan,
                                              max_capacity=1)
    assert len(receipt.accepted) == 1
    lease = _lease_next(repository, _SERVICE, snapshot.pool_key)
    assert lease is not None
    candidates = tuple(
        _typed_fill_for_lease(_SERVICE, replica_id, snapshot, allocation, lease)
        for replica_id in (1, 2))
    barrier = threading.Barrier(2)

    def _race(replica_id: int, info: replica_managers.ReplicaInfo) -> bool:
        barrier.wait(timeout=10)
        return _stage_durable_fill(allocation_engine, _SERVICE, replica_id,
                                   info, lease)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(_race, replica_id, info)
            for replica_id, info in enumerate(candidates, start=1)
        ]
        results = [future.result(timeout=20) for future in futures]

    assert sorted(results) == [False, True]
    assert _persisted_replica_count(allocation_engine) == 1


def _claim_set_edge() -> dict[str, object]:
    return {
        'pool_key': _POOL_KEY,
        'legacy_pool_key': json.dumps([_CONTEXT, ['a100-80gb', 'h200']]),
        'pool_position': 0,
        'access_context': _CONTEXT,
        'physical_cluster_uid': _UID,
        'accelerator_names': ['a100-80gb', 'h200'],
        'worker_projection_sha256_by_accelerator': _PROJECTION_MAP,
        'weight': 1000,
        'floor_replicas': 0,
        'gpus_per_replica': 1,
        'holdings_fill': 0,
        'effective_cap': 8,
        'launchable': True,
    }


def _utilization_state(*, cap: int, demonstrated_need: int,
                       stepped_at: float) -> dict[str, object]:
    return {
        'cap': cap,
        'hot_until': stepped_at + 60,
        'stepped_at': stepped_at,
        'blind_since': None,
        'demonstrated_need': demonstrated_need,
        'demand_witness_sha256': None,
        'reservation_acquisition_classes': None,
        'reservation_acquisition_binding_sha256': None,
        'boot_hold': False,
        'blind': False,
    }


def _exact_utilization_state(*, cap: int, demonstrated_need: int,
                             accelerator: str) -> dict[str, object]:
    state = _utilization_state(cap=cap,
                               demonstrated_need=demonstrated_need,
                               stepped_at=1.0)
    state['demand_witness_sha256'] = 'd' * 64
    demand = compatibility_matching.CompatibilityDemand(
        priority=0,
        compatible_cards=(accelerator.casefold(),),
        count=demonstrated_need)
    state['reservation_acquisition_classes'] = [{
        'priority': demand.priority,
        'compatible_cards': list(demand.compatible_cards),
        'count': demand.count,
    }]
    state['reservation_acquisition_binding_sha256'] = (
        reserved_fill_allocation.reservation_acquisition_binding_sha256(
            'd' * 64, (demand,)))
    return state


def test_semantic_claim_replacement_clears_but_noop_preserves_publication(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine)
    repository = _repository(allocation_engine)
    published = repository.publish(_SERVICE,
                                   expected_service_hash=_SERVICE_HASH,
                                   expected_controller_owner=_OWNER,
                                   expected_claim_generation=11,
                                   expected_gate_generation=1,
                                   pool_snapshots=(snapshot,))
    assert published is not None

    edge = _claim_set_edge()
    same_authorizer = _claim_policy_authorizer('semantic-a')
    same_generation = serve_state.replace_reserved_fill_claim_set(
        _SERVICE,
        semantic_hash='semantic-a',
        global_headroom=8,
        utilization_ceiling=8,
        utilization_state=None,
        edges=(edge,),
        heartbeat_ts=2,
        expected_service_hash=_SERVICE_HASH,
        service_version=1,
        expected_controller_owner=_OWNER,
        reclaim_claim_authorizer=same_authorizer)
    assert same_generation == 11
    with allocation_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.text("""
                SELECT allocation_generation
                FROM reserved_fill_service_claim_sets
                WHERE service_name = :name
            """), {
                'name': _SERVICE
            }).scalar_one() == 1
    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) == published

    runtime_edge = dict(edge)
    runtime_edge.update(holdings_fill=3, floor_replicas=2)
    runtime_generation = serve_state.replace_reserved_fill_claim_set(
        _SERVICE,
        semantic_hash='semantic-a',
        global_headroom=5,
        utilization_ceiling=4,
        utilization_state=_utilization_state(cap=4,
                                             demonstrated_need=3,
                                             stepped_at=3.0),
        edges=(runtime_edge,),
        heartbeat_ts=3,
        expected_service_hash=_SERVICE_HASH,
        service_version=1,
        expected_controller_owner=_OWNER,
        reclaim_claim_authorizer=same_authorizer)
    assert runtime_generation == 11
    # The topology generation is unchanged, but schema 6 treats a new demand
    # witness as new paid-planning authority.  It cannot reuse the idle map.
    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) is None
    republished = repository.publish(_SERVICE,
                                     expected_service_hash=_SERVICE_HASH,
                                     expected_controller_owner=_OWNER,
                                     expected_claim_generation=11,
                                     expected_gate_generation=1,
                                     pool_snapshots=(snapshot,))
    assert republished is not None
    assert republished.allocation_generation == 2
    assert republished.utilization_gate_armed
    assert republished.utilization_demonstrated_need == 3
    assert republished.utilization_ceiling == 4

    lower_cap_edge = dict(runtime_edge)
    lower_cap_edge['effective_cap'] = 4
    lower_cap_generation = serve_state.replace_reserved_fill_claim_set(
        _SERVICE,
        semantic_hash='semantic-a',
        global_headroom=4,
        utilization_ceiling=4,
        utilization_state=_utilization_state(cap=4,
                                             demonstrated_need=3,
                                             stepped_at=4.0),
        edges=(lower_cap_edge,),
        heartbeat_ts=4,
        expected_service_hash=_SERVICE_HASH,
        service_version=1,
        expected_controller_owner=_OWNER,
        reclaim_claim_authorizer=same_authorizer)
    assert lower_cap_generation == 11
    # Same-generation cap changes do not clear the publication row, but the
    # schema-6 reader immediately rejects the stale edge-cap authority.
    with allocation_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.text("""
                SELECT allocation_generation
                FROM reserved_fill_service_claim_sets
                WHERE service_name = :name
            """), {
                'name': _SERVICE
            }).scalar_one() == 2
    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) is None

    next_authorizer = _claim_policy_authorizer('semantic-b')
    next_generation = serve_state.replace_reserved_fill_claim_set(
        _SERVICE,
        semantic_hash='semantic-b',
        global_headroom=8,
        utilization_ceiling=8,
        utilization_state=None,
        edges=(edge,),
        heartbeat_ts=5,
        expected_service_hash=_SERVICE_HASH,
        service_version=1,
        expected_controller_owner=_OWNER,
        reclaim_claim_authorizer=next_authorizer)
    assert next_generation == 12
    with allocation_engine.connect() as connection:
        cleared = connection.execute(
            sqlalchemy.text("""
                SELECT allocation_generation, allocation_input_sha256,
                       allocation_map, allocation_gate_generation
                FROM reserved_fill_service_claim_sets
                WHERE service_name = :name
            """), {
                'name': _SERVICE
            }).one()
    assert tuple(cleared) == (0, None, None, None)


def test_publication_records_unsettled_upward_grant(allocation_engine,
                                                    monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine)
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE reserved_fill_rounds
                SET grants = :grants, raw_grants = :raw_grants
                WHERE pool_key = :pool_key
            """), {
                'grants': json.dumps({_SERVICE: 4}),
                'raw_grants': json.dumps({_SERVICE: 8}),
                'pool_key': _POOL_KEY,
            })
    snapshot = dataclasses.replace(snapshot, grant=4)
    repository = _repository(allocation_engine)

    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))

    assert allocation is not None
    assert not allocation.upward_grants_settled
    assert repository.read_current(_SERVICE, _SERVICE_HASH,
                                   _OWNER) == allocation


def test_exact_acquisition_waits_for_complete_discovery_round(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine,
                                   accelerator_names=('a100',),
                                   free_slots=47,
                                   feed_by_accelerator={'a100': 1},
                                   edge_cap=1,
                                   grant=1)
    utilization_state = _exact_utilization_state(cap=47,
                                                 demonstrated_need=47,
                                                 accelerator='a100')
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_service_claim_sets_table).
            where(serve_state_schema.reserved_fill_service_claim_sets_table.c.
                  service_name == _SERVICE).values(
                      global_headroom=47,
                      utilization_ceiling=47,
                      utilization_state=json.dumps(utilization_state)))
    repository = _repository(allocation_engine)

    first = repository.publish(_SERVICE,
                               expected_service_hash=_SERVICE_HASH,
                               expected_controller_owner=_OWNER,
                               expected_claim_generation=11,
                               expected_gate_generation=1,
                               pool_snapshots=(snapshot,))

    assert first is not None
    assert not first.upward_grants_settled
    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) == first

    exact_feed = {
        _SERVICE: {
            'a100': 47
        },
        reserved_capacity_broker.OBSERVED_FREE_BY_ACCELERATOR_KEY: {
            'a100': 47
        },
        reserved_capacity_broker.SPENDABLE_FREE_BY_ACCELERATOR_KEY: {
            'a100': 47
        },
        reserved_capacity_broker.BROKER_SLOT_WIDTH_KEY: 1,
    }
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_pool_claims_table).where(
                    serve_state_schema.reserved_fill_pool_claims_table.c.
                    service_name == _SERVICE).values(effective_cap=47))
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_rounds_table).where(
                    serve_state_schema.reserved_fill_rounds_table.c.pool_key ==
                    snapshot.pool_key).values(
                        grants=json.dumps({_SERVICE: 47}),
                        raw_grants=json.dumps({_SERVICE: 47}),
                        feeds=json.dumps({_SERVICE: 47}),
                        feed_by_accelerator=json.dumps(exact_feed)))
    complete_snapshot = dataclasses.replace(snapshot,
                                            edge_cap=47,
                                            free_slots=47,
                                            free_slots_by_accelerator=(('a100',
                                                                        47),),
                                            grant=47)

    complete = repository.publish(_SERVICE,
                                  expected_service_hash=_SERVICE_HASH,
                                  expected_controller_owner=_OWNER,
                                  expected_claim_generation=11,
                                  expected_gate_generation=1,
                                  pool_snapshots=(complete_snapshot,))

    assert complete is not None
    assert complete.upward_grants_settled
    assert repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER) == complete


def test_flexible_acquisition_settles_against_exact_locked_supply(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine,
                                   accelerator_names=('a100',),
                                   free_slots=3,
                                   feed_by_accelerator={'a100': 3},
                                   edge_cap=3,
                                   grant=3)
    state = _exact_utilization_state(cap=3,
                                     demonstrated_need=3,
                                     accelerator='a100')
    flexible_class = compatibility_matching.CompatibilityDemand(
        priority=0, compatible_cards=('a100', 'h200'), count=3)
    state['reservation_acquisition_classes'] = [{
        'priority': flexible_class.priority,
        'compatible_cards': list(flexible_class.compatible_cards),
        'count': flexible_class.count,
    }]
    state['reservation_acquisition_binding_sha256'] = (
        reserved_fill_allocation.reservation_acquisition_binding_sha256(
            'd' * 64, (flexible_class,)))
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_service_claim_sets_table).
            where(serve_state_schema.reserved_fill_service_claim_sets_table.c.
                  service_name == _SERVICE).values(
                      global_headroom=3,
                      utilization_ceiling=3,
                      utilization_state=json.dumps(state)))
    repository = _repository(allocation_engine)

    allocation = repository.publish(_SERVICE,
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_controller_owner=_OWNER,
                                    expected_claim_generation=11,
                                    expected_gate_generation=1,
                                    pool_snapshots=(snapshot,))

    assert allocation is not None
    assert allocation.upward_grants_settled
    assert repository.read_current(_SERVICE, _SERVICE_HASH,
                                   _OWNER) == allocation


def test_acquisition_class_tamper_revokes_settled_allocation(
        allocation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, snapshot = _commit_evidence(allocation_engine,
                                   accelerator_names=('a100',),
                                   free_slots=3,
                                   feed_by_accelerator={'a100': 3},
                                   edge_cap=3,
                                   grant=3)
    state = _exact_utilization_state(cap=3,
                                     demonstrated_need=3,
                                     accelerator='a100')
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_service_claim_sets_table).
            where(serve_state_schema.reserved_fill_service_claim_sets_table.c.
                  service_name == _SERVICE).values(
                      global_headroom=3,
                      utilization_ceiling=3,
                      utilization_state=json.dumps(state)))
    repository = _repository(allocation_engine)
    settled = repository.publish(_SERVICE,
                                 expected_service_hash=_SERVICE_HASH,
                                 expected_controller_owner=_OWNER,
                                 expected_claim_generation=11,
                                 expected_gate_generation=1,
                                 pool_snapshots=(snapshot,))
    assert settled is not None
    assert settled.upward_grants_settled

    tampered = copy.deepcopy(state)
    tampered['reservation_acquisition_classes'] = [{
        'priority': 0,
        'compatible_cards': ['h200'],
        'count': 3,
    }]
    with allocation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_service_claim_sets_table).
            where(serve_state_schema.reserved_fill_service_claim_sets_table.c.
                  service_name == _SERVICE).values(
                      utilization_state=json.dumps(tampered)))

    with pytest.raises(
            reserved_fill_allocation.ReservedFillAllocationCorruptionError,
            match='do not match their binding'):
        repository.read_current(_SERVICE, _SERVICE_HASH, _OWNER)
    with pytest.raises(
            reserved_fill_allocation.ReservedFillAllocationCorruptionError,
            match='do not match their binding'):
        repository.publish(_SERVICE,
                           expected_service_hash=_SERVICE_HASH,
                           expected_controller_owner=_OWNER,
                           expected_claim_generation=11,
                           expected_gate_generation=1,
                           pool_snapshots=(snapshot,))


@pytest.mark.parametrize('lock_target', ['protocol', 'legacy-projection'])
def test_claim_authorization_is_minted_after_all_write_set_locks(
        allocation_engine, monkeypatch, lock_target) -> None:
    """A pre-lock wait longer than the ticket lifetime cannot expire it."""
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    monotonic = [100.0]
    monkeypatch.setattr(reserved_fill_reclaim_attestation.time, 'monotonic',
                        lambda: monotonic[0])
    lock_attempted = threading.Event()
    blocker_ready = threading.Event()
    release_blocker = threading.Event()
    authorization_called = threading.Event()
    events = []
    if lock_target == 'protocol':
        original_lock = serve_state._lock_zero_cost_protocol_sequence_for_update

        def _record_lock_attempt(session):
            events.append('lock-attempt')
            lock_attempted.set()
            return original_lock(session)

        monkeypatch.setattr(serve_state,
                            '_lock_zero_cost_protocol_sequence_for_update',
                            _record_lock_attempt)
        lock_sql = """
            SELECT id
            FROM reserved_fill_protocol_state
            WHERE id = 1
            FOR UPDATE
        """
        lock_params = {}
    else:
        with allocation_engine.begin() as connection:
            connection.execute(
                serve_state_schema.reserved_fill_claims_table.insert().values(
                    service_name=_SERVICE,
                    pool_key='legacy-pool',
                    heartbeat_ts=1))
        original_lock = (
            serve_state._lock_reserved_fill_claim_write_set_for_update)

        def _record_lock_attempt(session, service_name):
            events.append('lock-attempt')
            lock_attempted.set()
            return original_lock(session, service_name)

        monkeypatch.setattr(serve_state,
                            '_lock_reserved_fill_claim_write_set_for_update',
                            _record_lock_attempt)
        lock_sql = """
            SELECT service_name
            FROM reserved_fill_claims
            WHERE service_name = :service_name
            FOR UPDATE
        """
        lock_params = {'service_name': _SERVICE}

    def _record_authorization() -> None:
        events.append('authorize')
        authorization_called.set()

    authorizer = _claim_policy_authorizer('semantic-b',
                                          on_authorize=_record_authorization)

    def _hold_protocol_lock() -> None:
        with allocation_engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.execute(sqlalchemy.text(lock_sql), lock_params).one()
                blocker_ready.set()
                assert release_blocker.wait(timeout=20)
            finally:
                transaction.rollback()

    def _replace() -> int | None:
        return serve_state.replace_reserved_fill_claim_set(
            _SERVICE,
            semantic_hash='semantic-b',
            global_headroom=8,
            utilization_ceiling=8,
            utilization_state=None,
            edges=(_claim_set_edge(),),
            heartbeat_ts=2,
            expected_service_hash=_SERVICE_HASH,
            service_version=1,
            expected_controller_owner=_OWNER,
            reclaim_claim_authorizer=authorizer)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        blocker = executor.submit(_hold_protocol_lock)
        try:
            assert blocker_ready.wait(timeout=10)
            replacement = executor.submit(_replace)
            assert lock_attempted.wait(timeout=10)
            assert not authorization_called.wait(timeout=0.1)
            monotonic[0] += (
                reserved_fill_reclaim_attestation.AUTHORIZATION_MAX_AGE_SECONDS
                + 1)
            release_blocker.set()
            assert replacement.result(timeout=20) == 12
            blocker.result(timeout=20)
        finally:
            release_blocker.set()

    assert events == ['lock-attempt', 'authorize']


def test_locked_claim_authorizer_can_read_fresh_proof_on_second_connection(
        allocation_engine, monkeypatch) -> None:
    """The receipt-only callback cannot self-deadlock on the gate lock."""
    monkeypatch.setattr(serve_state._db_manager, '_engine', allocation_engine)
    _, template = _claim_policy_authority('semantic-b')
    repository = reserved_fill_reclaim_proofs.ReclaimProviderProofRepository(
        allocation_engine)
    try:
        repository.renew(
            identity=template.identity,
            gate_generation=template.gate_generation,
            kubernetes_context=_CONTEXT,
            deadline_monotonic=(time.monotonic() +
                                reserved_fill_reclaim_attestation.
                                PROVIDER_PROOF_REFRESH_TIMEOUT_SECONDS),
            prove=lambda: reserved_fill_reclaim_proofs.
            ReclaimProviderProofCandidate(proof_payload={
                'kubernetes': {
                    'physical_cluster_uid': _UID,
                },
            },
                                          oldest_completed_monotonic=time.
                                          monotonic()),
            validate=lambda _payload: True)

        def _authorize(
            scope: reserved_fill_reclaim_attestation.ReclaimClaimSetScope,
            identity: reserved_fill_reclaim_attestation.ReclaimPolicyIdentity,
            gate_generation: int,
        ) -> reserved_fill_reclaim_attestation.ReclaimClaimAuthorization:
            repository.get_fresh(
                identity=identity,
                gate_generation=gate_generation,
                kubernetes_context=_CONTEXT,
                deadline_monotonic=(time.monotonic() +
                                    reserved_fill_reclaim_attestation.
                                    PROVIDER_PROOF_READ_TIMEOUT_SECONDS),
                validate=lambda _payload: True,
                minimum_remaining_seconds=(
                    reserved_fill_reclaim_attestation.
                    PROVIDER_PROOF_CONSUMER_MIN_REMAINING_SECONDS))
            return (reserved_fill_reclaim_attestation.ReclaimClaimAuthorization(
                identity=identity,
                gate_generation=gate_generation,
                scope=scope,
                completed_monotonic=time.monotonic()))

        assert serve_state.replace_reserved_fill_claim_set(
            _SERVICE,
            semantic_hash='semantic-b',
            global_headroom=8,
            utilization_ceiling=8,
            utilization_state=None,
            edges=(_claim_set_edge(),),
            heartbeat_ts=2,
            expected_service_hash=_SERVICE_HASH,
            service_version=1,
            expected_controller_owner=_OWNER,
            reclaim_claim_authorizer=_authorize) == 12
    finally:
        repository._proof_engine.dispose()
