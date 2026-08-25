"""Real-PostgreSQL contracts for three-state Kueue admissions."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import concurrent.futures
import contextlib
import datetime
import threading
import time
import types
from unittest import mock
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.serve import capacity_admission
from sky.serve import kubernetes_identity
from sky.serve import kueue_lane_capacity
from sky.serve import kueue_lane_lineage
from sky.serve import kueue_lane_lineage_schema
from sky.serve import kueue_lane_observer
from sky.serve import ordinary_launch_binding
from sky.serve import pool_capacity_observation_schema
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import service
from sky.serve import zero_cost_actuation
from sky.serve import zero_cost_actuation_schema
from sky.server.requests import postgres as request_postgres
from sky.server.requests import postgres_schema as request_postgres_schema
from sky.utils import common_utils
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(name='serve_kueue_admission_057_pg')

_SERVICE = 'svc'
_SERVICE_HASH = 'service-incarnation'
_LIFECYCLE_EPOCH = 3
_SERVICE_VERSION = 19
_POOL_EPOCH = 7
_PHYSICAL_UID = 'cluster-uid-a'
_CONTEXT = 'phx'
_ACCELERATOR = 'h200'
_ACCELERATOR_COUNT = 1
_POOL_KEY = reserved_capacity_broker.make_pool_key(
    _CONTEXT,
    _ACCELERATOR,
    protocol_version=reserved_capacity_broker.PROTOCOL_V2,
    physical_cluster_uid=_PHYSICAL_UID)
_WORKER_PROJECTION = {
    'projection_version':
        kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION,
    'candidate_id': 'kubernetes-0000',
    'kubernetes_context': _CONTEXT,
    'namespace': 'skypilot',
    'service_account_name': 'skypilot-pool-sa',
    'scheduler_name': 'gpu-binpack-scheduler',
    'priority_class_name': 'skypilot-low',
    'priority_value': -1000,
    'preemption_policy': 'Never',
    'kueue_admission': {
        'local_queue_name': 'be',
        'workload_priority_class_name': 'be-ls',
    },
    'pod_identity_role_arn': None,
    'accelerator_name': 'H200',
    'accelerator_count': _ACCELERATOR_COUNT,
    'accelerator_scheduling': {
        'label_key': 'nvidia.com/gpu.product',
        'label_values': ['NVIDIA-H200'],
        'resource_key': 'nvidia.com/gpu',
    },
    'cache': {
        'kind': 'none',
    },
    'provision_timeout': -1,
    'scratch': {
        'kind': 'none',
    },
}
_EAST_WORKER_PROJECTION = {
    **_WORKER_PROJECTION,
    'candidate_id': 'kubernetes-0001',
    'kubernetes_context': 'east',
    'service_account_name': 'skyserve-worker',
    'scheduler_name': 'default-scheduler',
    'priority_class_name': 'skyserve-preemptible',
    'kueue_admission': None,
}
_PROJECTION = kubernetes_identity.worker_projection_sha256(_WORKER_PROJECTION)
_EAST_PROJECTION = kubernetes_identity.worker_projection_sha256(
    _EAST_WORKER_PROJECTION)


def _identity(**overrides) -> kueue_lane_lineage.KueueAdmissionIdentity:
    values = {
        'service_name': _SERVICE,
        'service_hash': _SERVICE_HASH,
        'service_lifecycle_epoch': _LIFECYCLE_EPOCH,
        'service_version': _SERVICE_VERSION,
        'pool_key': _POOL_KEY,
        'pool_epoch': _POOL_EPOCH,
        'physical_cluster_uid': _PHYSICAL_UID,
        'kubernetes_context': _CONTEXT,
        'accelerator': _ACCELERATOR,
        'accelerator_count': _ACCELERATOR_COUNT,
        'worker_projection_sha256': _PROJECTION,
    }
    values.update(overrides)
    return kueue_lane_lineage.KueueAdmissionIdentity(**values)


def _reserved_location_state() -> dict:
    return {
        'cloud': 'Kubernetes',
        'region': _CONTEXT,
        'zone': None,
        'accelerators': {
            _ACCELERATOR: _ACCELERATOR_COUNT
        },
        'use_spot': False,
        'image_id': None,
        'container_image': None,
        'disk_tier': None,
        'ephemeral_storage': None,
        'instance_type': None,
    }


def _intent_values(intent_key: str, **overrides) -> dict:
    created_at = datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc)
    valid_until = created_at + datetime.timedelta(minutes=5)
    values = {
        'intent_idempotency_key': intent_key,
        'service_name': _SERVICE,
        'service_hash': _SERVICE_HASH,
        'service_lifecycle_epoch': _LIFECYCLE_EPOCH,
        'actuation_epoch': 1,
        'service_version': _SERVICE_VERSION,
        'controller_owner': 'controller-owner',
        'ordinal': 0,
        'protocol_version': 2,
        'policy_revision': 1,
        'reconcile_generation': 1,
        'allocation_generation': 1,
        'allocation_input_sha256': 'a' * 64,
        'allocation_claim_generation': 1,
        'reconciliation_gate_generation': 1,
        'reclaim_fleet_bundle_sha256': 'b' * 64,
        'reclaim_policy_revision': 'reclaim-v1',
        'reclaim_provider_inventory_sha256': 'c' * 64,
        'service_generation': 1,
        'pool_key': _POOL_KEY,
        'pool_epoch': _POOL_EPOCH,
        'physical_cluster_uid': _PHYSICAL_UID,
        'kubernetes_context': _CONTEXT,
        'worker_projection_sha256': _PROJECTION,
        'observation_generation': 1,
        'observation_sequence': 1,
        'ordinary_zero_cost_admission_sequence': 1,
        'valid_until_epoch': valid_until.timestamp(),
        'valid_until': valid_until,
        'accelerator': _ACCELERATOR,
        'accelerator_count': _ACCELERATOR_COUNT,
        'capacity_unit': 'physical',
        'planned_capacity': 1,
        'allowed_locations': [_reserved_location_state()],
        'state': 'GRANTED',
        'lease_generation': 0,
        'created_at': created_at,
        'updated_at': created_at,
    }
    values.update(overrides)
    return values


def _canonical_intent_key(**overrides) -> str:
    """Build the exact deterministic key for ``_intent_values`` authority."""
    values = _intent_values('0' * 64, **overrides)
    locations = tuple(
        reserved_fill_planner.LocationSnapshot.from_pickleable(location)
        for location in values['allowed_locations'])
    intent = reserved_fill_planner.FillIntent.create(
        ordinal=values['ordinal'],
        protocol_version=values['protocol_version'],
        policy_revision=values['policy_revision'],
        reconcile_generation=values['reconcile_generation'],
        allocation_generation=values['allocation_generation'],
        allocation_input_sha256=values['allocation_input_sha256'],
        allocation_claim_generation=values['allocation_claim_generation'],
        reconciliation_gate_generation=values['reconciliation_gate_generation'],
        reclaim_fleet_bundle_sha256=values['reclaim_fleet_bundle_sha256'],
        reclaim_policy_revision=values['reclaim_policy_revision'],
        reclaim_provider_inventory_sha256=values[
            'reclaim_provider_inventory_sha256'],
        service_incarnation=values['service_hash'],
        service_version=values['service_version'],
        controller_owner=values['controller_owner'],
        service_generation=values['service_generation'],
        pool_key=values['pool_key'],
        pool_epoch=values['pool_epoch'],
        physical_cluster_uid=values['physical_cluster_uid'],
        worker_projection_sha256=values['worker_projection_sha256'],
        observation_generation=values['observation_generation'],
        observation_sequence=values['observation_sequence'],
        ordinary_zero_cost_admission_sequence=values[
            'ordinary_zero_cost_admission_sequence'],
        valid_until=values['valid_until_epoch'],
        accelerator=values['accelerator'],
        accelerator_count=values['accelerator_count'],
        capacity_unit=reserved_fill_planner.FillCapacityUnit(
            values['capacity_unit']),
        allowed_locations=locations)
    return intent.idempotency_key


@pytest.fixture
def admission_database(empty_postgres):  # noqa: F811
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    migration_utils.safe_alembic_upgrade(empty_postgres,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         migration_utils.API_REQUESTS_VERSION)
    incarnation = uuid.uuid4()
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name=_SERVICE, epoch=_LIFECYCLE_EPOCH))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name=_SERVICE,
                workspace='workspace-a',
                status='READY',
                hash=_SERVICE_HASH,
                resource_scope=_SERVICE_HASH,
                current_version=_SERVICE_VERSION,
                active_versions=f'[{_SERVICE_VERSION}]',
                pool=0,
                lifecycle_epoch=_LIFECYCLE_EPOCH,
                controller_incarnation=incarnation,
                controller_owner_epoch=1,
                controller_pid=1,
                controller_ip='10.0.0.1',
                controller_port=8000,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=1,
                demand_source_mode='DURABLE_FEED',
                demand_source_epoch=1,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=incarnation,
                demand_authority_protocol_version=(
                    capacity_admission.PROTOCOL_VERSION),
                reserved_fill_actuation_mode='DURABLE_INTENT',
                reserved_fill_actuation_epoch=1,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=incarnation,
                reserved_fill_actuation_protocol_version=1))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name=_SERVICE,
                version=_SERVICE_VERSION,
                yaml_content='service: {}',
                placement_catalog={
                    'schema_version': 1,
                    'entries': [],
                    'num_nodes': 1,
                },
                worker_placement_projections=[
                    _WORKER_PROJECTION, _EAST_WORKER_PROJECTION
                ]))
        assert ordinary_launch_binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name=_SERVICE,
            controller_incarnation=incarnation,
            controller_owner_epoch=1,
            expected_binding_epoch=1,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True) == 2
    return empty_postgres


def _insert_intent(engine, intent_key: str, **overrides) -> None:
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(zero_cost_actuation_schema.
                              serve_zero_cost_actuation_intents_table).values(
                                  **_intent_values(intent_key, **overrides)))


def _receipt(
    state: kueue_lane_lineage.KueueAdmissionState,
    intent_key: str,
    record_id: uuid.UUID,
    *,
    identity: kueue_lane_lineage.KueueAdmissionIdentity,
    phase: str = 'Pending',
    workload_name: str | None = None,
    unconstrained_topology: str | None = None,
) -> dict:
    admitted = (state is kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED)
    return {
        'schema_version': 1,
        'state': state.value,
        'pod': {
            'namespace': 'skypilot',
            'name': 'worker-1',
            'uid': 'pod-uid-1',
            'phase': phase,
            'deletion_timestamp_absent': True,
            'scheduling_gates':
                ([] if admitted else ['kueue.x-k8s.io/admission']),
        },
        'skypilot': {
            'cluster_name_on_cloud': 'worker-1',
            'intent_key': intent_key,
            'replica_record_uuid': str(record_id),
            'pool_physical_uid': identity.physical_cluster_uid,
            'worker_projection_sha256': identity.worker_projection_sha256,
        },
        'kueue': {
            'managed_finalizer': 'kueue.x-k8s.io/managed',
            'managed_label': True,
            'local_queue_name': 'be',
            'cluster_queue_name': 'research',
            'admission_local_queue_name': ('be' if admitted else None),
            'admission_cluster_queue_name': ('research' if admitted else None),
            'workload_priority_class_name': 'be-ls',
            'pod_group_name': 'worker-1',
            'pod_group_total_count': 1,
            'retriable_in_group': False,
            'role_hash': '0123abcd',
            'podset': ('0123abcd' if admitted else None),
            'workload_name': workload_name,
            'unconstrained_topology': unconstrained_topology,
        },
        'priority': {
            'class_name': 'skypilot-low',
            'value': -1000,
            'preemption_policy': 'Never',
        },
        'scheduler_name': 'gpu-binpack-scheduler',
        'service_account_name': 'skypilot-pool-sa',
        'accelerator': {
            'name': identity.accelerator,
            'label_key': 'nvidia.com/gpu.product',
            'label_values': ['NVIDIA-H200'],
            'resource_key': 'nvidia.com/gpu',
            'count': identity.accelerator_count,
            'sole_ray_node_resource_owner': True,
            'dynamic_resource_claims_absent': True,
        },
    }


def _receipt_digest(connection, receipt: dict) -> str:
    payload = sqlalchemy.literal(receipt, type_=postgresql.JSONB)
    return connection.execute(
        sqlalchemy.select(
            sqlalchemy.func.encode(
                sqlalchemy.func.sha256(
                    sqlalchemy.func.convert_to(
                        sqlalchemy.cast(payload, sqlalchemy.Text), 'UTF8')),
                'hex'))).scalar_one()


def _postgres_now(
        connection: sqlalchemy.engine.Connection) -> datetime.datetime:
    return connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()


def _install_materialized_graph(
    connection: sqlalchemy.engine.Connection,
    intent_key: str,
    replica_id: int,
    record_id: uuid.UUID,
    provider_generation: int,
) -> uuid.UUID:
    """Install an exact graph without re-testing Serve047/056 triggers."""
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    replicas = serve_state_schema.replicas_table
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    for table in (intents.name, replicas.name, associations.name):
        connection.exec_driver_sql(f'ALTER TABLE {table} DISABLE TRIGGER USER')
    association_id = uuid.uuid4()
    now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    controller_incarnation = connection.execute(
        sqlalchemy.select(
            serve_state_schema.services_table.c.controller_incarnation).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE)).scalar_one()
    intent_row = connection.execute(
        sqlalchemy.select(intents).where(
            intents.c.intent_idempotency_key == intent_key)).mappings().one()
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference=f'reserved-fill:{intent_key}',
        authorization_generation=1,
        authorization_payload={'intent_idempotency_key': intent_key})
    info = replica_managers.ReplicaInfo(replica_id, f'{_SERVICE}-{replica_id}',
                                        '8000', False, None, _SERVICE_VERSION,
                                        None)
    location = _reserved_location_state()
    info.location = dict(location)
    info.resources_override = dict(location)
    info.replica_record_id = str(record_id)
    info.reserved_fill = True
    info.reserved_fill_pool_key = _POOL_KEY
    info.reserved_fill_service_generation = 1
    info.reserved_fill_physical_cluster_uid = _PHYSICAL_UID
    info.reserved_fill_kubernetes_context = _CONTEXT
    info.reserved_fill_allocation_generation = 1
    info.reserved_fill_allocation_input_sha256 = 'a' * 64
    info.reserved_fill_allocation_claim_generation = 1
    info.reserved_fill_reconciliation_gate_generation = 1
    info.reserved_fill_reclaim_fleet_bundle_sha256 = 'b' * 64
    info.reserved_fill_reclaim_policy_revision = 'reclaim-v1'
    info.reserved_fill_reclaim_provider_inventory_sha256 = 'c' * 64
    info.reserved_fill_worker_projection_sha256 = _PROJECTION
    info.reserved_fill_observation_generation = 1
    info.reserved_fill_observation_sequence = int(
        intent_row['observation_sequence'])
    info.reserved_fill_intent_idempotency_key = intent_key
    info.zero_cost_admission_sequence = replica_id
    info.is_zero_cost = True
    info.planned_capacity = 1
    connection.execute(
        sqlalchemy.insert(associations).values(
            association_id=association_id,
            submission_id=uuid.uuid4(),
            tenant_scope='tenant-a',
            service_name=_SERVICE,
            service_hash=_SERVICE_HASH,
            service_workspace='workspace-a',
            service_lifecycle_epoch=_LIFECYCLE_EPOCH,
            service_binding_epoch=2,
            service_version=_SERVICE_VERSION,
            replica_id=replica_id,
            replica_record_id=record_id,
            launch_generation=provider_generation,
            cluster_name=f'{_SERVICE}-{replica_id}',
            request_id=f'request-{uuid.uuid4()}',
            input_digest='a' * 64,
            owner_controller_incarnation=controller_incarnation,
            owner_controller_epoch=1,
            effect_phase='NOT_STARTED',
            effect_phase_changed_at=now,
            resolution='BOUND',
            created_at=now,
            updated_at=now,
            binding_protocol_version=2,
            profile_kind='RESERVED_FILL',
            profile_version=1,
            profile_digest=profile.digest,
            capability_cohort_epoch=(
                ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
            capability_profile_set_digest=(
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
            ),
            receipt_protocol_version=1,
            authorization_kind=profile.authorization_kind.value,
            authorization_reference=profile.authorization_reference,
            authorization_generation=profile.authorization_generation,
            authorization_digest=profile.authorization_digest,
            reconciliation_outcome='ACTIVE_ADOPT',
            provider_evidence='NOT_QUERIED'))
    connection.execute(
        sqlalchemy.update(intents).where(
            intents.c.intent_idempotency_key == intent_key).values(
                state='COMMITTED',
                replica_id=replica_id,
                replica_record_id=record_id,
                committed_at=now,
                updated_at=now))
    connection.execute(
        sqlalchemy.insert(replicas).values(
            service_name=_SERVICE,
            replica_id=replica_id,
            replica_state_version=1,
            replica_state=info.to_storage_dict(),
            status='PENDING',
            version=_SERVICE_VERSION,
            cluster_name=f'{_SERVICE}-{replica_id}',
            is_spot=False,
            ordinary_launch_association_id=association_id,
            reserved_fill_intent_idempotency_key=intent_key))
    for table in (intents.name, replicas.name, associations.name):
        connection.exec_driver_sql(f'ALTER TABLE {table} ENABLE TRIGGER USER')
    return association_id


def _materialize(
    engine: sqlalchemy.engine.Engine,
    repository: kueue_lane_lineage.KueueAdmissionRepository,
    identity: kueue_lane_lineage.KueueAdmissionIdentity,
    intent_key: str,
    *,
    replica_id: int = 1,
    provider_generation: int = 9,
) -> tuple[uuid.UUID, uuid.UUID]:
    record_id = uuid.uuid4()
    with engine.begin() as connection:
        association_id = _install_materialized_graph(connection, intent_key,
                                                     replica_id, record_id,
                                                     provider_generation)
        repository.bind_materialized_in_connection(
            connection,
            identity,
            intent_idempotency_key=intent_key,
            replica_id=replica_id,
            replica_record_id=record_id,
            provider_cluster_generation=provider_generation,
            association_id=association_id)
    return record_id, association_id


def _install_pre_effect_terminal_reserved_fill_graph(
    engine: sqlalchemy.engine.Engine,
    repository: kueue_lane_lineage.KueueAdmissionRepository,
    *,
    intent_key: str,
    replica_id: int = 1,
    admission_state: kueue_lane_lineage.KueueAdmissionState = (
        kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING),
    cancel_before_terminal: bool = False,
) -> tuple[uuid.UUID, uuid.UUID, str]:
    """Install the exact provider-free projection retained by row 465."""
    identity = _identity()
    _insert_intent(engine,
                   intent_key,
                   ordinal=replica_id - 1,
                   observation_sequence=replica_id - 1,
                   ordinary_zero_cost_admission_sequence=replica_id - 1)
    with engine.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, identity,
                                                       intent_key)
    record_id, association_id = _materialize(engine,
                                             repository,
                                             identity,
                                             intent_key,
                                             replica_id=replica_id,
                                             provider_generation=replica_id + 8)
    _install_canonical_cleanup_profile_authority(engine,
                                                 intent_key=intent_key,
                                                 replica_id=replica_id,
                                                 association_id=association_id)
    if admission_state is kueue_lane_lineage.KueueAdmissionState.POD_WAITING:
        receipt = _receipt(admission_state,
                           intent_key,
                           record_id,
                           identity=identity)
        with engine.begin() as connection:
            repository.observe_pod_waiting_in_connection(
                connection,
                identity,
                intent_idempotency_key=intent_key,
                replica_id=replica_id,
                replica_record_id=record_id,
                provider_cluster_generation=replica_id + 8,
                association_id=association_id,
                pod_namespace='skypilot',
                pod_name='worker-1',
                pod_uid='pod-uid-1',
                pod_receipt=receipt,
                provider_read_started_at=_postgres_now(connection))
    elif admission_state is not (
            kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING):
        raise ValueError('Unsupported pre-effect admission fixture state.')

    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with engine.begin() as connection:
        association = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one()
        context = ordinary_launch_binding.bound_context_from_association(
            association)
        if cancel_before_terminal:
            ordinary_launch_binding.request_cancel_in_connection(
                connection, context, 'replica-teardown')
        now = _postgres_now(connection)
        evidence = ordinary_launch_binding.TerminalEvidence(
            status=ordinary_launch_binding.TerminalStatus.FAILED,
            cause='dispatcher_submit_failed',
            execution_generation=1,
            quiescence_required=True,
            quiesced_generation=1,
            quiesced_at=now)
        assert ordinary_launch_binding.record_terminal_in_connection(
            connection, context,
            evidence) is (ordinary_launch_binding.StartupClassification.
                          PRE_EFFECT_TERMINALIZE)
        assert ordinary_launch_binding.project_in_connection(
            connection, context, pre_effect_terminal=True, service_job_id=None)
        association = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one()
        request_id = str(association['request_id'])
        connection.execute(
            sqlalchemy.insert(request_postgres_schema.REQUESTS).values(
                request_id=request_id,
                name='sky.launch',
                handler_name='sky.server.requests.non_pool_launch:launch',
                payload_type='test-payload',
                payload_format='json',
                payload_version=1,
                producer_version='test',
                payload_json={},
                execution_class='normal',
                status='FAILED',
                terminal_cause='dispatcher_submit_failed',
                created_at=now,
                schedule_type='short',
                user_id='test-user',
                should_retry=False,
                finished_at=now,
                ignore_return_value=True,
                retryable=False,
                execution_generation=1,
                execution_quiescence_required=True,
                execution_quiesced_generation=1,
                execution_quiesced_at=now,
                ordinary_launch_association_id=association_id,
                binding_protocol_version=2,
                profile_kind='RESERVED_FILL',
                profile_version=1,
                profile_digest=association['profile_digest'],
                capability_cohort_epoch=association['capability_cohort_epoch'],
                capability_profile_set_digest=association[
                    'capability_profile_set_digest'],
                receipt_protocol_version=association[
                    'receipt_protocol_version'],
                updated_at=now))
    return record_id, association_id, request_id


def _observation_authority(
    *,
    intent_key: str,
    replica_id: int,
    record_id: uuid.UUID,
    association_id: uuid.UUID,
    provider_generation: int,
) -> kueue_lane_observer._ObservationAuthority:
    fence = reserved_capacity.ProtocolV2LaunchFence(
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        pool_key=_POOL_KEY,
        service_generation=1,
        service_version=_SERVICE_VERSION,
        physical_cluster_uid=_PHYSICAL_UID,
        kubernetes_context=_CONTEXT,
        accelerator=_ACCELERATOR,
        accelerator_count=_ACCELERATOR_COUNT,
        reconciliation_gate_generation=1,
        reclaim_fleet_bundle_sha256='b' * 64,
        reclaim_policy_revision='reclaim-v1',
        reclaim_provider_inventory_sha256='c' * 64,
        worker_projection_sha256=_PROJECTION)
    return kueue_lane_observer._ObservationAuthority(
        service_name=_SERVICE,
        service_hash=_SERVICE_HASH,
        service_lifecycle_epoch=_LIFECYCLE_EPOCH,
        service_version=_SERVICE_VERSION,
        intent_key=intent_key,
        replica_id=replica_id,
        replica_record_id=record_id,
        association_id=association_id,
        provider_cluster_generation=provider_generation,
        fence=fence)


def _install_observation_graphs(
    engine: sqlalchemy.engine.Engine,
    count: int,
) -> tuple[tuple[kueue_lane_observer._ObservationAuthority, ...], tuple[
        replica_managers.ReplicaInfo, ...]]:
    repository = kueue_lane_lineage.KueueAdmissionRepository(engine)
    authorities = []
    for offset in range(count):
        replica_id = offset + 1
        provider_generation = offset + 9
        intent_key = f'{replica_id:064x}'
        _insert_intent(engine,
                       intent_key,
                       ordinal=offset,
                       observation_sequence=replica_id,
                       ordinary_zero_cost_admission_sequence=replica_id)
        with engine.begin() as connection:
            repository.insert_intent_pending_in_connection(
                connection, _identity(), intent_key)
        record_id, association_id = _materialize(
            engine,
            repository,
            _identity(),
            intent_key,
            replica_id=replica_id,
            provider_generation=provider_generation)
        authorities.append(
            _observation_authority(intent_key=intent_key,
                                   replica_id=replica_id,
                                   record_id=record_id,
                                   association_id=association_id,
                                   provider_generation=provider_generation))
    with engine.connect() as connection:
        states = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.replica_state).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE).order_by(serve_state_schema.replicas_table.c.
                                       replica_id)).scalars().all()
    infos = tuple(
        replica_managers.ReplicaInfo.from_storage_dict(dict(state))
        for state in states)
    return tuple(authorities), infos


def test_materialization_observers_parallelize_but_fence_prefix_writers(
        admission_database, monkeypatch) -> None:
    """Twenty-four callbacks share read fences; writers remain exclusive."""
    observer_count = 24
    authorities, _ = _install_observation_graphs(admission_database,
                                                 observer_count)
    parallel_engine = sqlalchemy.create_engine(admission_database.url,
                                               pool_size=observer_count + 5,
                                               max_overflow=0)
    original_prefix = (
        kueue_lane_observer._lock_materialization_validation_prefix)
    all_prefixes_locked = threading.Event()
    release_observers = threading.Event()
    count_lock = threading.Lock()
    locked_count = 0

    def hold_shared_prefix(connection, authority):
        nonlocal locked_count
        original_prefix(connection, authority)
        with count_lock:
            locked_count += 1
            if locked_count == observer_count:
                all_prefixes_locked.set()
        assert release_observers.wait(timeout=10)

    monkeypatch.setattr(kueue_lane_observer,
                        '_lock_materialization_validation_prefix',
                        hold_shared_prefix)

    def validate(authority):
        with parallel_engine.begin() as connection:
            _, _, admission = (
                kueue_lane_observer._lock_and_validate_materialization(
                    connection, authority))
            return admission.intent_idempotency_key

    writer_pids: dict[str, int] = {}
    writer_connected = {
        'protocol': threading.Event(),
        'lifecycle': threading.Event(),
        'service': threading.Event(),
    }

    def take_writer_lock(target: str) -> None:
        with parallel_engine.begin() as connection:
            writer_pids[target] = connection.execute(
                sqlalchemy.text('SELECT pg_backend_pid()')).scalar_one()
            writer_connected[target].set()
            if target == 'protocol':
                table = (pool_capacity_observation_schema.
                         protocol_state_sequence_table)
                statement = sqlalchemy.select(table.c.id).where(table.c.id == 1)
            elif target == 'lifecycle':
                table = serve_state_schema.service_lifecycle_fences_table
                statement = sqlalchemy.select(
                    table.c.epoch).where(table.c.name == _SERVICE)
            else:
                table = serve_state_schema.services_table
                statement = sqlalchemy.select(
                    table.c.hash).where(table.c.name == _SERVICE)
            connection.execute(statement.with_for_update()).one()

    def wait_until_blocked(target: str) -> None:
        assert writer_connected[target].wait(timeout=5)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with parallel_engine.connect() as connection:
                blockers = connection.execute(
                    sqlalchemy.text('SELECT pg_blocking_pids(:pid)'), {
                        'pid': writer_pids[target]
                    }).scalar_one()
            if blockers:
                return
            time.sleep(0.02)
        raise AssertionError(f'{target} writer was not fenced')

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=observer_count +
                                                   3) as executor:
            observer_futures = [
                executor.submit(validate, authority)
                for authority in authorities
            ]
            assert all_prefixes_locked.wait(timeout=10)
            writer_futures = {
                target: executor.submit(take_writer_lock, target)
                for target in writer_connected
            }
            for target in writer_futures:
                wait_until_blocked(target)
            release_observers.set()
            observed_keys = {
                future.result(timeout=15) for future in observer_futures
            }
            for future in writer_futures.values():
                future.result(timeout=15)
    finally:
        release_observers.set()
        parallel_engine.dispose()

    assert observed_keys == {authority.intent_key for authority in authorities}


def test_capacity_snapshot_and_materialization_observer_have_one_lock_order(
        admission_database, monkeypatch) -> None:
    """Force the production intent/service overlap without a deadlock."""
    (authority,), infos = _install_observation_graphs(admission_database, 1)
    parallel_engine = sqlalchemy.create_engine(admission_database.url,
                                               pool_size=4,
                                               max_overflow=0)
    monkeypatch.setattr(serve_state_schema, 'get_database_engine',
                        lambda: parallel_engine)
    observer_prefix_locked = threading.Event()
    snapshot_intents_locked = threading.Event()
    original_prefix = (
        kueue_lane_observer._lock_materialization_validation_prefix)
    original_projection = (
        kueue_lane_capacity.lock_capacity_projection_in_connection)

    def pause_observer_after_prefix(connection, observed_authority):
        original_prefix(connection, observed_authority)
        observer_prefix_locked.set()
        assert snapshot_intents_locked.wait(timeout=10)

    def mark_snapshot_intents_locked(*args, **kwargs):
        snapshot_intents_locked.set()
        return original_projection(*args, **kwargs)

    monkeypatch.setattr(kueue_lane_observer,
                        '_lock_materialization_validation_prefix',
                        pause_observer_after_prefix)
    monkeypatch.setattr(kueue_lane_capacity,
                        'lock_capacity_projection_in_connection',
                        mark_snapshot_intents_locked)

    def validate() -> str:
        with parallel_engine.begin() as connection:
            _, _, admission = (
                kueue_lane_observer._lock_and_validate_materialization(
                    connection, authority))
            return admission.intent_idempotency_key

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            observer = executor.submit(validate)
            assert observer_prefix_locked.wait(timeout=5)
            snapshot = executor.submit(
                kueue_lane_capacity.snapshot_replica_capacity_classes, _SERVICE,
                list(infos))
            capacity = snapshot.result(timeout=10)
            assert observer.result(timeout=10) == authority.intent_key
    finally:
        snapshot_intents_locked.set()
        parallel_engine.dispose()

    assert capacity.by_replica_id == {
        authority.replica_id:
            kueue_lane_capacity.KueueReplicaCapacityClass.UNKNOWN,
    }


def _install_retirable_materialized_graph(
    engine: sqlalchemy.engine.Engine,
    repository: kueue_lane_lineage.KueueAdmissionRepository,
    *,
    intent_key: str,
    replica_id: int,
    cleanup_marker: bool = True,
    effect_phase: str | None = None,
    admission_state: kueue_lane_lineage.KueueAdmissionState = (
        kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING),
    worker_projection_sha256: str = _PROJECTION,
    observation_generation: int = 1,
    observation_sequence: int | None = None,
    ordinary_admission_sequence: int | None = None,
) -> tuple[uuid.UUID, uuid.UUID, str]:
    """Install the exact terminal, quiesced, provider-absent delete graph."""
    identity = _identity(worker_projection_sha256=worker_projection_sha256)
    if observation_sequence is None:
        observation_sequence = replica_id - 1
    if ordinary_admission_sequence is None:
        ordinary_admission_sequence = replica_id - 1
    _insert_intent(
        engine,
        intent_key,
        ordinal=replica_id - 1,
        worker_projection_sha256=worker_projection_sha256,
        observation_generation=observation_generation,
        observation_sequence=observation_sequence,
        ordinary_zero_cost_admission_sequence=(ordinary_admission_sequence))
    with engine.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, identity,
                                                       intent_key)
    record_id, association_id = _materialize(engine,
                                             repository,
                                             identity,
                                             intent_key,
                                             replica_id=replica_id,
                                             provider_generation=replica_id + 8)
    if admission_state is not kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING:
        if admission_state not in (
                kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
                kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED):
            raise ValueError('Unsupported fixture admission state.')
        policy_admitted = (
            admission_state is
            kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED)
        receipt = _receipt(
            admission_state,
            intent_key,
            record_id,
            identity=identity,
            phase='Running' if policy_admitted else 'Pending',
            workload_name=('worker-1' if policy_admitted else None),
            unconstrained_topology=('true' if policy_admitted else None))
        with engine.begin() as connection:
            observe = (repository.observe_policy_admitted_in_connection
                       if policy_admitted else
                       repository.observe_pod_waiting_in_connection)
            observe(connection,
                    identity,
                    intent_idempotency_key=intent_key,
                    replica_id=replica_id,
                    replica_record_id=record_id,
                    provider_cluster_generation=replica_id + 8,
                    association_id=association_id,
                    provider_read_started_at=_postgres_now(connection),
                    pod_namespace='skypilot',
                    pod_name='worker-1',
                    pod_uid='pod-uid-1',
                    pod_receipt=receipt)

    info = replica_managers.ReplicaInfo(replica_id, f'{_SERVICE}-{replica_id}',
                                        '8000', False, None, _SERVICE_VERSION,
                                        None)
    location = _reserved_location_state()
    info.location = dict(location)
    info.resources_override = dict(location)
    info.replica_record_id = str(record_id)
    info.reserved_fill = True
    info.reserved_fill_pool_key = _POOL_KEY
    info.reserved_fill_service_generation = 1
    info.reserved_fill_physical_cluster_uid = _PHYSICAL_UID
    info.reserved_fill_kubernetes_context = _CONTEXT
    info.reserved_fill_allocation_generation = 1
    info.reserved_fill_allocation_input_sha256 = 'a' * 64
    info.reserved_fill_allocation_claim_generation = 1
    info.reserved_fill_reconciliation_gate_generation = 1
    info.reserved_fill_reclaim_fleet_bundle_sha256 = 'b' * 64
    info.reserved_fill_reclaim_policy_revision = 'reclaim-v1'
    info.reserved_fill_reclaim_provider_inventory_sha256 = 'c' * 64
    info.reserved_fill_worker_projection_sha256 = worker_projection_sha256
    info.reserved_fill_observation_generation = observation_generation
    info.reserved_fill_observation_sequence = observation_sequence
    info.reserved_fill_intent_idempotency_key = intent_key
    info.zero_cost_admission_sequence = replica_id
    info.is_zero_cost = True
    info.planned_capacity = 1
    if cleanup_marker:
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.INTERRUPTED)
        info.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        info.status_property.is_scale_down = True
        info.status_property.drain_cap_seconds = 0
    else:
        # Production lifecycle 84 retained this terminal pre-job shape.  It is
        # provider-clean but deliberately not the immediate-cleanup marker.
        info.status_property.sky_launch_status = common_utils.ProcessStatus.FAILED
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        info.status_property.is_scale_down = False
        info.status_property.drain_cap_seconds = None

    associations = ordinary_launch_binding.ordinary_launch_associations_table
    replicas = serve_state_schema.replicas_table
    with engine.begin() as connection:
        association = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one()
        request_id = str(association['request_id'])
        now = _postgres_now(connection)
        payload, payload_digest = (
            ordinary_launch_binding._reserved_fill_provider_evidence(  # pylint: disable=protected-access
                association, info,
                ordinary_launch_binding.ProviderEvidence.ABSENT))
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} DISABLE TRIGGER USER')
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
        replica_values = serve_state._replica_row_values(  # pylint: disable=protected-access
            _SERVICE, replica_id, info)
        connection.execute(
            sqlalchemy.update(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == replica_id).values(
                    **replica_values,
                    ordinary_launch_association_id=None,
                    reserved_fill_intent_idempotency_key=intent_key))
        connection.execute(
            sqlalchemy.update(associations).
            where(associations.c.association_id == association_id).values(
                **({} if effect_phase is None else {
                    'effect_phase': effect_phase,
                    'effect_phase_changed_at': now,
                }),
                resolution=ordinary_launch_binding.Resolution.PROJECTED.value,
                reconciliation_outcome=ordinary_launch_binding.
                ReconciliationOutcome.PROJECTED.value,
                provider_evidence=ordinary_launch_binding.ProviderEvidence.
                ABSENT.value,
                provider_evidence_observed_at=now,
                provider_evidence_payload=payload,
                provider_evidence_digest=payload_digest,
                terminal_status=ordinary_launch_binding.TerminalStatus.
                CANCELLED.value,
                terminal_cause='execution_lease_expired',
                terminal_execution_generation=1,
                execution_quiescence_required=True,
                execution_quiesced_generation=1,
                execution_quiesced_at=now,
                projected_at=now,
                pin_released_at=now,
                tombstone_not_before=(now + datetime.timedelta(days=60)),
                updated_at=now))
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} ENABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.insert(request_postgres_schema.REQUESTS).values(
                request_id=request_id,
                name='sky.launch',
                handler_name='sky.server.requests.non_pool_launch:launch',
                payload_type='test-payload',
                payload_format='json',
                payload_version=1,
                producer_version='test',
                payload_json={},
                execution_class='normal',
                status='CANCELLED',
                terminal_cause='execution_lease_expired',
                created_at=now,
                schedule_type='short',
                user_id='test-user',
                should_retry=False,
                finished_at=now,
                ignore_return_value=True,
                retryable=False,
                execution_generation=1,
                execution_quiescence_required=True,
                execution_quiesced_generation=1,
                execution_quiesced_at=now,
                ordinary_launch_association_id=association_id,
                binding_protocol_version=2,
                profile_kind='RESERVED_FILL',
                profile_version=1,
                profile_digest=association['profile_digest'],
                capability_cohort_epoch=association['capability_cohort_epoch'],
                capability_profile_set_digest=association[
                    'capability_profile_set_digest'],
                receipt_protocol_version=association[
                    'receipt_protocol_version'],
                updated_at=now))
    return record_id, association_id, request_id


def _install_normal_admitted_teardown_graph(
    engine: sqlalchemy.engine.Engine,
    repository: kueue_lane_lineage.KueueAdmissionRepository,
    *,
    intent_key: str,
    replica_id: int,
    is_scale_down: bool,
    down_status: common_utils.ProcessStatus | None,
    terminal_status: str = 'SUCCEEDED',
    terminal_cause: str = 'handler_succeeded',
    launch_status: common_utils.ProcessStatus = common_utils.ProcessStatus.
    SUCCEEDED,
    projection_owner: str = 'manager',
    observation_sequence: int | None = None,
    ordinary_zero_cost_admission_sequence: int | None = None,
    worker_projection_sha256: str = _PROJECTION,
) -> tuple[uuid.UUID, uuid.UUID, str]:
    """Run the production projector for a launch with one admitted Pod."""
    if projection_owner not in ('manager', 'service'):
        raise ValueError('projection_owner must be manager or service.')
    identity = _identity(worker_projection_sha256=worker_projection_sha256)
    ordinary_high_water = (0 if ordinary_zero_cost_admission_sequence is None
                           else ordinary_zero_cost_admission_sequence)
    observation_high_water = (0 if observation_sequence is None else
                              observation_sequence)
    _insert_intent(engine,
                   intent_key,
                   ordinal=replica_id - 1,
                   worker_projection_sha256=worker_projection_sha256,
                   observation_sequence=observation_high_water,
                   ordinary_zero_cost_admission_sequence=ordinary_high_water)
    with engine.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, identity,
                                                       intent_key)
    record_id, association_id = _materialize(engine,
                                             repository,
                                             identity,
                                             intent_key,
                                             replica_id=replica_id,
                                             provider_generation=replica_id + 8)
    admitted_receipt = _receipt(
        kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
        intent_key,
        record_id,
        identity=identity,
        phase='Running',
        workload_name='worker-1',
        unconstrained_topology='true')
    with engine.begin() as connection:
        repository.observe_policy_admitted_in_connection(
            connection,
            identity,
            intent_idempotency_key=intent_key,
            replica_id=replica_id,
            replica_record_id=record_id,
            provider_cluster_generation=replica_id + 8,
            association_id=association_id,
            provider_read_started_at=_postgres_now(connection),
            pod_namespace='skypilot',
            pod_name='worker-1',
            pod_uid='pod-uid-1',
            pod_receipt=admitted_receipt)

    info = replica_managers.ReplicaInfo(replica_id, f'{_SERVICE}-{replica_id}',
                                        '8000', False, None, _SERVICE_VERSION,
                                        None)
    location = _reserved_location_state()
    info.location = dict(location)
    info.resources_override = dict(location)
    info.replica_record_id = str(record_id)
    info.reserved_fill = True
    info.reserved_fill_pool_key = _POOL_KEY
    info.reserved_fill_service_generation = 1
    info.reserved_fill_physical_cluster_uid = _PHYSICAL_UID
    info.reserved_fill_kubernetes_context = _CONTEXT
    info.reserved_fill_allocation_generation = 1
    info.reserved_fill_allocation_input_sha256 = 'a' * 64
    info.reserved_fill_allocation_claim_generation = 1
    info.reserved_fill_reconciliation_gate_generation = 1
    info.reserved_fill_reclaim_fleet_bundle_sha256 = 'b' * 64
    info.reserved_fill_reclaim_policy_revision = 'reclaim-v1'
    info.reserved_fill_reclaim_provider_inventory_sha256 = 'c' * 64
    info.reserved_fill_worker_projection_sha256 = worker_projection_sha256
    info.reserved_fill_observation_generation = 1
    info.reserved_fill_observation_sequence = observation_high_water
    info.reserved_fill_intent_idempotency_key = intent_key
    info.zero_cost_admission_sequence = replica_id
    info.is_zero_cost = True
    info.planned_capacity = 1
    info.status_property.sky_launch_status = launch_status
    info.status_property.sky_down_status = down_status
    info.status_property.is_scale_down = is_scale_down

    associations = ordinary_launch_binding.ordinary_launch_associations_table
    replicas = serve_state_schema.replicas_table
    with engine.begin() as connection:
        association = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one()
        request_id = str(association['request_id'])
        service_job_id = 1000 + replica_id
        now = _postgres_now(connection)
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} DISABLE TRIGGER USER')
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
        replica_values = serve_state._replica_row_values(  # pylint: disable=protected-access
            _SERVICE, replica_id, info)
        connection.execute(
            sqlalchemy.update(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == replica_id).values(
                    **replica_values,
                    ordinary_launch_association_id=association_id,
                    reserved_fill_intent_idempotency_key=intent_key))
        connection.execute(
            sqlalchemy.update(associations).where(
                associations.c.association_id == association_id).values(
                    effect_phase='SERVICE_JOB_RECORDED',
                    effect_phase_changed_at=now,
                    resolution='RESULT_RECORDED',
                    reconciliation_outcome='RESULT_RECORDED',
                    terminal_status=terminal_status,
                    terminal_cause=terminal_cause,
                    terminal_execution_generation=1,
                    execution_quiescence_required=True,
                    execution_quiesced_generation=1,
                    execution_quiesced_at=now,
                    service_job_id=service_job_id,
                    result_recorded_at=now,
                    updated_at=now))
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} ENABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.insert(request_postgres_schema.REQUESTS).values(
                request_id=request_id,
                name='sky.launch',
                handler_name='sky.server.requests.non_pool_launch:launch',
                payload_type='test-payload',
                payload_format='json',
                payload_version=1,
                producer_version='test',
                payload_json={},
                execution_class='normal',
                status=terminal_status,
                terminal_cause=terminal_cause,
                created_at=now,
                schedule_type='short',
                user_id='test-user',
                should_retry=False,
                finished_at=now,
                ignore_return_value=True,
                retryable=False,
                execution_generation=1,
                execution_quiescence_required=True,
                execution_quiesced_generation=1,
                execution_quiesced_at=now,
                ordinary_launch_association_id=association_id,
                binding_protocol_version=2,
                profile_kind='RESERVED_FILL',
                profile_version=1,
                profile_digest=association['profile_digest'],
                capability_cohort_epoch=association['capability_cohort_epoch'],
                capability_profile_set_digest=association[
                    'capability_profile_set_digest'],
                receipt_protocol_version=association[
                    'receipt_protocol_version'],
                updated_at=now))
        profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
            authorization_reference=f'reserved-fill:{intent_key}',
            authorization_generation=1,
            authorization_payload={'intent_idempotency_key': intent_key})
        context = ordinary_launch_binding.BoundNonPoolLaunchContext(
            association_id=association_id,
            request_id=request_id,
            service_name=_SERVICE,
            replica_id=replica_id,
            replica_record_id=record_id,
            launch_generation=replica_id + 8,
            input_digest=str(association['input_digest']),
            profile=profile,
            capability_cohort_epoch=int(association['capability_cohort_epoch']),
            capability_profile_set_digest=str(
                association['capability_profile_set_digest']),
            receipt_protocol_version=int(
                association['receipt_protocol_version']))
        projection = types.SimpleNamespace(
            locked_replica_info=info,
            request=types.SimpleNamespace(error=None),
            status=types.SimpleNamespace(value=terminal_status),
            cause=terminal_cause,
            context=context,
            pre_effect_terminal=False,
            service_job_id=service_job_id,
            cancel_reason=None,
            paid_capacity_pool_key=None)
        if projection_owner == 'manager':
            manager = replica_managers.SkyPilotReplicaManager.__new__(
                replica_managers.SkyPilotReplicaManager)
            manager._service_name = _SERVICE
            manager._ordinary_launch_binding_authority = types.SimpleNamespace(
                service_hash=_SERVICE_HASH)
            projected = manager._project_bound_ordinary_launch(
                None, connection, projection)
        else:
            authority = types.SimpleNamespace(service_name=_SERVICE,
                                              service_hash=_SERVICE_HASH)
            projected = service._project_bound_ordinary_launch_for_teardown(
                authority, connection, projection)
        assert projected
        assert info.launch_request_id is None
        assert info.service_job_id is None
        assert ordinary_launch_binding.project_in_connection(
            connection,
            context,
            pre_effect_terminal=False,
            service_job_id=service_job_id)
    return record_id, association_id, request_id


def _install_canonical_cleanup_profile_authority(
    engine: sqlalchemy.engine.Engine,
    *,
    intent_key: str,
    replica_id: int,
    association_id: uuid.UUID,
    initialize_protocol: bool = True,
    install_observation: bool | None = None,
    protocol_sequence: int | None = None,
) -> None:
    """Replace the generic fixture profile with production fill authority."""
    if install_observation is None:
        install_observation = initialize_protocol
    if protocol_sequence is None:
        protocol_sequence = replica_id
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    requests = request_postgres_schema.REQUESTS
    replicas = serve_state_schema.replicas_table
    services = serve_state_schema.services_table
    protocol = pool_capacity_observation_schema.protocol_state_sequence_table
    observations = (
        pool_capacity_observation_schema.demand_capacity_observations_v2_table)
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with engine.begin() as connection:
        now = _postgres_now(connection)
        now_epoch = now.timestamp()
        intent_row = connection.execute(
            sqlalchemy.select(intents).where(intents.c.intent_idempotency_key ==
                                             intent_key)).mappings().one()
        protocol_values: dict[str, object] = {
            'zero_cost_admission_sequence': sqlalchemy.func.greatest(
                protocol.c.zero_cost_admission_sequence, protocol_sequence),
            'ordinary_zero_cost_admission_sequence': sqlalchemy.func.greatest(
                protocol.c.ordinary_zero_cost_admission_sequence,
                protocol_sequence),
        }
        if initialize_protocol:
            protocol_values.update({
                'protocol_version': 2,
                'reconciliation_gate_state':
                    (pool_capacity_observation_schema.SEQUENCED_ACTIVE),
                'reconciliation_gate_generation': 1,
                'reclaim_fleet_bundle_sha256': 'b' * 64,
                'reclaim_policy_revision': 'reclaim-v1',
                'reclaim_provider_inventory_sha256': 'c' * 64,
                'reclaim_claim_scope_count': 0,
                'reclaim_claim_scope_sha256': 'd' * 64,
                'reclaim_evidence_sha256': 'e' * 64,
                'reclaim_authorized_at': now_epoch,
                'image_digest': 'sha256:' + 'f' * 64,
                'deployment_generation': 'fixture-deployment-1',
                'deployment_uid': 'fixture-deployment-uid-1',
                'pod_inventory_count': 1,
                'pod_inventory_sha256': '1' * 64,
            })
        connection.execute(
            sqlalchemy.update(protocol).where(protocol.c.id == 1).values(
                **protocol_values))
        if install_observation:
            observation_payload = {
                'fixture': 'canonical-reserved-fill-cleanup',
            }
            connection.execute(
                sqlalchemy.insert(observations).values(
                    context=f'{_CONTEXT}-cleanup-{replica_id}',
                    snapshot_time=now_epoch,
                    completed_at=now_epoch,
                    availability='AVAILABLE',
                    pool_key=_POOL_KEY,
                    physical_cluster_uid=_PHYSICAL_UID,
                    accelerator_names=[_ACCELERATOR],
                    access_context=_CONTEXT,
                    observation_generation=int(
                        intent_row['observation_generation']),
                    lease_token=uuid.uuid4(),
                    lease_expires_at=now_epoch + 60,
                    observation_sequence=int(
                        intent_row['observation_sequence']),
                    ordinary_admission_sequence=int(
                        intent_row['ordinary_zero_cost_admission_sequence']),
                    materialization_sequence=0,
                    observation_status=(
                        pool_capacity_observation_schema.SUCCESS),
                    payload=observation_payload,
                    payload_sha256=_receipt_digest(connection,
                                                   observation_payload),
                    observed_at=now_epoch,
                    valid_until=now_epoch + 60,
                    published_at=now_epoch))
        service_row = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == _SERVICE)).mappings().one()
        replica_row = connection.execute(
            sqlalchemy.select(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == replica_id)).mappings().one()
        info = serve_state.decode_replica_state_for_authority(
            replica_row['replica_state_version'], replica_row['replica_state'])
        payload = ordinary_launch_binding._reserved_fill_cleanup_payload(  # pylint: disable=protected-access
            connection, service_row, info)
        profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
            authorization_reference=f'reserved-fill:{intent_key}',
            authorization_generation=1,
            authorization_payload=payload)
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
        connection.exec_driver_sql(
            f'ALTER TABLE {requests.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(associations).where(
                associations.c.association_id == association_id).values(
                    profile_digest=profile.digest,
                    authorization_kind=profile.authorization_kind.value,
                    authorization_reference=profile.authorization_reference,
                    authorization_generation=profile.authorization_generation,
                    authorization_digest=profile.authorization_digest,
                    updated_at=now))
        connection.execute(
            sqlalchemy.update(requests).where(
                requests.c.ordinary_launch_association_id ==
                association_id).values(profile_digest=profile.digest,
                                       updated_at=now))
        connection.exec_driver_sql(
            f'ALTER TABLE {requests.name} ENABLE TRIGGER USER')
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')


def _stamp_canonical_materialization_receipt(
    engine: sqlalchemy.engine.Engine,
    *,
    replica_id: int,
) -> None:
    """Advance the post-profile materialization sequence as production does."""
    replicas = serve_state_schema.replicas_table
    protocol = pool_capacity_observation_schema.protocol_state_sequence_table
    with engine.begin() as connection:
        row = connection.execute(
            sqlalchemy.select(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == replica_id)).mappings().one()
        info = serve_state.decode_replica_state_for_authority(
            row['replica_state_version'], row['replica_state'])
        info.zero_cost_materialization_sequence = replica_id
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == replica_id).values(
                    **serve_state._replica_row_values(  # pylint: disable=protected-access
                        _SERVICE, replica_id, info)))
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} ENABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(protocol).where(protocol.c.id == 1).values(
                zero_cost_materialization_sequence=sqlalchemy.func.greatest(
                    protocol.c.zero_cost_materialization_sequence, replica_id)))


def _advance_reconciliation_gate(
    engine: sqlalchemy.engine.Engine,
    *,
    generation: int = 2,
) -> None:
    protocol = pool_capacity_observation_schema.protocol_state_sequence_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(protocol).where(protocol.c.id == 1).values(
                protocol_version=2,
                reconciliation_gate_state=(
                    pool_capacity_observation_schema.SEQUENCED_ACTIVE),
                reconciliation_gate_generation=generation,
                reclaim_evidence_sha256=f'{generation:064x}',
                reclaim_authorized_at=time.time(),
                deployment_generation=f'fixture-deployment-{generation}'))


def _install_generation_fenced_pre_job_graph(
    engine: sqlalchemy.engine.Engine,
    *,
    admission_state: kueue_lane_lineage.KueueAdmissionState = (
        kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING),
    effect_phase: str = 'PROVIDER_IO',
    replica_id: int = 1,
    expire_waiting_receipt: bool = True,
    initialize_protocol: bool = True,
) -> tuple[str, uuid.UUID, uuid.UUID, str]:
    repository = kueue_lane_lineage.KueueAdmissionRepository(engine)
    ordinal = replica_id - 1
    observation_sequence = 0
    ordinary_admission_sequence = 0
    key = _canonical_intent_key(
        ordinal=ordinal,
        observation_generation=1,
        observation_sequence=observation_sequence,
        ordinary_zero_cost_admission_sequence=ordinary_admission_sequence)
    record_id, association_id, request_id = (
        _install_retirable_materialized_graph(
            engine,
            repository,
            intent_key=key,
            replica_id=replica_id,
            cleanup_marker=False,
            effect_phase=effect_phase,
            admission_state=admission_state,
            observation_generation=1,
            observation_sequence=(observation_sequence),
            ordinary_admission_sequence=(ordinary_admission_sequence)))
    _install_canonical_cleanup_profile_authority(
        engine,
        intent_key=key,
        replica_id=replica_id,
        association_id=association_id,
        initialize_protocol=(initialize_protocol))
    if (admission_state is kueue_lane_lineage.KueueAdmissionState.POD_WAITING
            and expire_waiting_receipt):
        admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
        with engine.begin() as connection:
            observed_at = (_postgres_now(connection) -
                           datetime.timedelta(seconds=16))
            connection.exec_driver_sql(
                f'ALTER TABLE {admissions.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.update(admissions).where(
                    admissions.c.intent_idempotency_key == key).values(
                        created_at=observed_at,
                        observed_at=observed_at,
                        valid_until=(observed_at +
                                     datetime.timedelta(seconds=15)),
                        updated_at=(observed_at +
                                    datetime.timedelta(seconds=1))))
            connection.exec_driver_sql(
                f'ALTER TABLE {admissions.name} ENABLE TRIGGER USER')
    _set_physical_provider_evidence(
        engine, association_id, ordinary_launch_binding.ProviderEvidence.ABSENT)
    return key, record_id, association_id, request_id


def _install_historical_v5_worker_projections(
        engine: sqlalchemy.engine.Engine) -> str:
    """Install the exact projections under which a retained v5 row formed."""
    historical_projection = dict(_WORKER_PROJECTION)
    historical_projection['projection_version'] = 5
    historical_east_projection = dict(_EAST_WORKER_PROJECTION)
    historical_east_projection['projection_version'] = 5
    projection_sha256 = kubernetes_identity.worker_projection_sha256(
        historical_projection)
    versions = serve_state_schema.version_specs_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(versions).where(
                versions.c.service_name == _SERVICE,
                versions.c.version == _SERVICE_VERSION).values(
                    worker_placement_projections=[
                        historical_projection,
                        historical_east_projection,
                    ]))
    return projection_sha256


def _configure_serve_state_for_kueue_retirement(
    monkeypatch: pytest.MonkeyPatch,
    engine: sqlalchemy.engine.Engine,
) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)

    def _resolve_historical_projection(
        _connection,
        _intent,
    ):
        # Teardown validates the exact immutable projection which admitted the
        # retained row.  It must decode released historical protocols instead
        # of applying the fresh-admission current-protocol gate.
        return _identity()

    monkeypatch.setattr(
        zero_cost_actuation,
        'kueue_teardown_identity_for_locked_intent_in_connection',
        _resolve_historical_projection)


def _mark_service_shutting_down(engine: sqlalchemy.engine.Engine) -> None:
    del engine
    result = ordinary_launch_binding.begin_service_teardown_if_owner(
        _SERVICE, _SERVICE_HASH, (1, '10.0.0.1'))
    assert result.disposition == (
        ordinary_launch_binding.ServiceTeardownDisposition.MARKED_BOUND)


def _claim_restricted_teardown_owner(
) -> ordinary_launch_binding.ControllerBindingAuthority:
    authority = ordinary_launch_binding.claim_controller_incarnation(
        _SERVICE,
        _SERVICE_HASH, (1, '10.0.0.1'),
        uuid.uuid4(),
        new_parent_owner=(1, '10.0.0.1'),
        expected_status=serve_state.ServiceStatus.SHUTTING_DOWN)
    assert authority is not None
    return authority


def _delete_kueue_admission(engine: sqlalchemy.engine.Engine,
                            intent_key: str) -> dict:
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with engine.begin() as connection:
        deleted = connection.execute(
            sqlalchemy.delete(admissions).where(
                admissions.c.intent_idempotency_key == intent_key).returning(
                    *admissions.c)).mappings().one()
    return dict(deleted)


def _set_physical_provider_evidence(
    engine: sqlalchemy.engine.Engine,
    association_id: uuid.UUID,
    evidence: ordinary_launch_binding.ProviderEvidence,
    *,
    cluster_name: str | None = None,
    observed_before_quiescence: bool = False,
) -> None:
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    replicas = serve_state_schema.replicas_table
    with engine.begin() as connection:
        association = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(replicas).where(
                replicas.c.service_name == _SERVICE, replicas.c.replica_id ==
                association['replica_id'])).mappings().one()
        info = serve_state.decode_replica_state_for_authority(
            replica['replica_state_version'], replica['replica_state'])
        if cluster_name is not None:
            info.cluster_name = cluster_name
        payload, digest = (
            ordinary_launch_binding._reserved_fill_provider_evidence(  # pylint: disable=protected-access
                association, info, evidence))
        observed_at = _postgres_now(connection)
        if observed_before_quiescence:
            observed_at = (association['execution_quiesced_at'] -
                           datetime.timedelta(seconds=1))
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(associations).where(
                associations.c.association_id == association_id).values(
                    provider_evidence=evidence.value,
                    provider_evidence_observed_at=observed_at,
                    provider_evidence_payload=payload,
                    provider_evidence_digest=digest,
                    updated_at=sqlalchemy.func.clock_timestamp()))
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')


def _admissionless_graph_snapshot(
    engine: sqlalchemy.engine.Engine,
    association_id: uuid.UUID,
) -> dict[str, object]:
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with engine.connect() as connection:
        return {
            'service': connection.execute(
                sqlalchemy.select(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name == _SERVICE)
            ).mappings().one(),
            'replicas': tuple(
                connection.execute(
                    sqlalchemy.select(serve_state_schema.replicas_table).where(
                        serve_state_schema.replicas_table.c.service_name ==
                        _SERVICE)).mappings().all()),
            'intents': tuple(
                connection.execute(
                    sqlalchemy.select(
                        zero_cost_actuation_schema.
                        serve_zero_cost_actuation_intents_table).where(
                            zero_cost_actuation_schema.
                            serve_zero_cost_actuation_intents_table.c.
                            service_name == _SERVICE)).mappings().all()),
            'association': connection.execute(
                sqlalchemy.select(associations).where(
                    associations.c.association_id == association_id)
            ).mappings().one(),
            'admission_count': connection.execute(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    kueue_lane_lineage_schema.serve_kueue_admissions_table)
            ).scalar_one(),
        }


def _assert_retired_graph(
    engine: sqlalchemy.engine.Engine,
    *,
    intent_keys: tuple[str, ...],
    replica_ids: tuple[int, ...],
    association_ids: tuple[uuid.UUID, ...],
) -> None:
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    replicas = serve_state_schema.replicas_table
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()).select_from(admissions).where(
                    admissions.c.intent_idempotency_key.in_(
                        intent_keys))).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()).select_from(intents).where(
                    intents.c.intent_idempotency_key.in_(
                        intent_keys))).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()).select_from(replicas).where(
                    replicas.c.service_name == _SERVICE,
                    replicas.c.replica_id.in_(replica_ids))).scalar_one() == 0
        retained = connection.execute(
            sqlalchemy.select(associations.c.association_id).where(
                associations.c.association_id.in_(
                    association_ids))).scalars().all()
        assert set(retained) == set(association_ids)


def _current_binding_authority(
    engine: sqlalchemy.engine.Engine
) -> ordinary_launch_binding.ControllerBindingAuthority:
    with engine.connect() as connection:
        service_row = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE)).mappings().one()
    return ordinary_launch_binding._authority_from_service(  # pylint: disable=protected-access
        service_row,
        controller_pid=service_row['controller_pid'],
        controller_ip=service_row['controller_ip'],
        controller_incarnation=service_row['controller_incarnation'],
        controller_owner_epoch=service_row['controller_owner_epoch'],
        capable=True)


def test_live_pre_effect_fill_retirement_releases_exact_atomic_handoff(
        admission_database, monkeypatch) -> None:
    """Row 465's retained graph is replaced without provider discovery."""
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, request_id = (
        _install_pre_effect_terminal_reserved_fill_graph(admission_database,
                                                         repository,
                                                         intent_key=key))
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    stale_association_id = uuid.uuid4()
    with admission_database.begin() as connection:
        current = dict(
            connection.execute(
                sqlalchemy.select(associations).where(
                    associations.c.association_id ==
                    association_id)).mappings().one())
        now = _postgres_now(connection)
        current.update({
            'association_id': stale_association_id,
            'submission_id': uuid.uuid4(),
            'replica_record_id': uuid.uuid4(),
            'request_id': f'stale-request-{uuid.uuid4()}',
            'resolution': 'PROJECTED',
            'reconciliation_outcome': 'PROJECTED',
            'terminal_status': 'FAILED',
            'terminal_cause': 'dispatcher_submit_failed',
            'provider_evidence': 'ABSENT',
            'provider_evidence_observed_at': now,
            'provider_evidence_payload': {
                'fixture': 'stale-predecessor-record'
            },
            'provider_evidence_digest': 'f' * 64,
            'updated_at': now,
        })
        # Production replica 465 also has retained history for an older record
        # with the same numeric ID.  The immutable record fence must isolate it.
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
        connection.execute(sqlalchemy.insert(associations).values(**current))
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    provider_probe = mock.Mock(side_effect=AssertionError('provider I/O'))
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        provider_probe)

    retired = (
        ordinary_launch_binding.retire_pre_admission_non_pool_launch_intent(
            _current_binding_authority(admission_database), 1, record_id))

    assert retired == ordinary_launch_binding.PreAdmissionRetirement(
        ordinary_launch_binding.PreAdmissionRetirementDisposition.RETIRED,
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL)
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,
                                           stale_association_id))
    with admission_database.connect() as connection:
        retained = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one()
        request = connection.execute(
            sqlalchemy.select(request_postgres_schema.REQUESTS).where(
                request_postgres_schema.REQUESTS.c.request_id ==
                request_id)).mappings().one()
    assert retained['resolution'] == 'PRE_EFFECT_TERMINAL'
    assert retained['effect_phase'] == 'NOT_STARTED'
    assert retained['provider_evidence'] == 'NOT_QUERIED'
    assert retained['execution_quiesced_generation'] == 1
    assert request['status'] == 'FAILED'
    assert request['terminal_cause'] == 'dispatcher_submit_failed'
    provider_probe.assert_not_called()


@pytest.mark.parametrize(
    ('admission_state', 'cancel_before_terminal'),
    ((kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING, True),
     (kueue_lane_lineage.KueueAdmissionState.POD_WAITING, False)),
)
def test_live_pre_effect_fill_retirement_rejects_cancel_or_pod_authority(
        admission_database, monkeypatch, admission_state,
        cancel_before_terminal) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = (
        _install_pre_effect_terminal_reserved_fill_graph(
            admission_database,
            repository,
            intent_key=key,
            admission_state=admission_state,
            cancel_before_terminal=cancel_before_terminal))
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    before = _admissionless_graph_snapshot(admission_database, association_id)

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='provider-free and exact'):
        ordinary_launch_binding.retire_pre_admission_non_pool_launch_intent(
            _current_binding_authority(admission_database), 1, record_id)

    assert _admissionless_graph_snapshot(admission_database,
                                         association_id) == before


@pytest.mark.parametrize('effect_phase', (None, 'PROVIDER_IO'))
def test_live_pre_effect_fill_retirement_preserves_ambiguous_action(
        admission_database, monkeypatch, effect_phase) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    _insert_intent(admission_database,
                   key,
                   observation_sequence=0,
                   ordinary_zero_cost_admission_sequence=0)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, _identity(),
                                                       key)
    record_id, association_id = _materialize(admission_database,
                                             repository,
                                             _identity(),
                                             key,
                                             replica_id=1,
                                             provider_generation=9)
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.begin() as connection:
        if effect_phase is not None:
            owner_revision = connection.execute(
                sqlalchemy.select(associations.c.owner_revision).where(
                    associations.c.association_id ==
                    association_id)).scalar_one()
            connection.execute(
                sqlalchemy.update(associations).where(
                    associations.c.association_id == association_id).values(
                        effect_phase=effect_phase,
                        effect_phase_changed_at=_postgres_now(connection),
                        owner_revision=owner_revision + 1,
                        updated_at=sqlalchemy.func.clock_timestamp()))
        row = connection.execute(
            sqlalchemy.select(associations).where(
                associations.c.association_id ==
                association_id)).mappings().one()
        context = ordinary_launch_binding.bound_context_from_association(row)
        assert ordinary_launch_binding.mark_ambiguous_in_connection(
            connection, context, 'provider-result-uncertain')
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    before = _admissionless_graph_snapshot(admission_database, association_id)

    retired = (
        ordinary_launch_binding.retire_pre_admission_non_pool_launch_intent(
            _current_binding_authority(admission_database), 1, record_id))

    assert retired.disposition is (
        ordinary_launch_binding.PreAdmissionRetirementDisposition.ASSOCIATED)
    assert _admissionless_graph_snapshot(admission_database,
                                         association_id) == before


def test_serve_state_single_replica_retirement_is_atomic_and_retains_history(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_retirable_materialized_graph(
        admission_database, repository, intent_key=key, replica_id=1)
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)

    assert serve_state.remove_replica(_SERVICE,
                                      1,
                                      expected_service_hash=_SERVICE_HASH,
                                      expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
                                      expected_replica_record_id=str(record_id))
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))


@pytest.mark.parametrize('projection_owner', ('manager', 'service'))
def test_normal_admitted_teardown_exact_pod_404_retires_replica(
        admission_database, monkeypatch, projection_owner) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = 'd' * 64
    record_id, association_id, _ = _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=True,
        down_status=common_utils.ProcessStatus.SCHEDULED,
        projection_owner=projection_owner)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        lambda _mode: contextlib.nullcontext())
    monkeypatch.setattr(kubernetes_adaptor, 'physical_cluster_uid_fence',
                        lambda *_args, **_kwargs: contextlib.nullcontext())

    class _MissingPodApi:

        def read_namespaced_pod(self, *_args, **_kwargs):
            raise kubernetes_adaptor.api_exception()(status=404)

    core_api = mock.Mock(return_value=_MissingPodApi())
    monkeypatch.setattr(kubernetes_adaptor, 'core_api', core_api)

    assert kueue_lane_observer.project_exact_pod_absence_after_teardown(
        _SERVICE, 1, record_id)
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.connect() as connection:
        evidence = connection.execute(
            sqlalchemy.select(associations.c.provider_evidence,
                              associations.c.provider_evidence_payload).where(
                                  associations.c.association_id ==
                                  association_id)).mappings().one()
        assert evidence['provider_evidence'] == 'ABSENT'
        assert evidence['provider_evidence_payload']['probe_contract'] == (
            'kueue-exact-pod-absence-v1')
        assert evidence['provider_evidence_payload']['pod'] == {
            'namespace': 'skypilot',
            'name': 'worker-1',
            'uid': 'pod-uid-1',
        }
    core_api.reset_mock()
    core_api.side_effect = AssertionError(
        'durable exact-Pod absence replay attempted provider I/O')
    assert kueue_lane_observer.project_exact_pod_absence_after_teardown(
        _SERVICE, 1, record_id)
    core_api.assert_not_called()
    assert serve_state.remove_replica(_SERVICE,
                                      1,
                                      expected_service_hash=_SERVICE_HASH,
                                      expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
                                      expected_replica_record_id=str(record_id))
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))


def test_exact_pod_replacement_fails_closed_without_evidence_stamp(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = 'd' * 64
    record_id, association_id, _ = _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=True,
        down_status=common_utils.ProcessStatus.SCHEDULED)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        lambda _mode: contextlib.nullcontext())
    monkeypatch.setattr(kubernetes_adaptor, 'physical_cluster_uid_fence',
                        lambda *_args, **_kwargs: contextlib.nullcontext())

    class _ReplacedPodApi:

        def read_namespaced_pod(self, *_args, **_kwargs):
            return types.SimpleNamespace(metadata=types.SimpleNamespace(
                uid='replacement-pod-uid'))

    monkeypatch.setattr(kubernetes_adaptor, 'core_api',
                        lambda _context: _ReplacedPodApi())
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='REPLACED'):
        kueue_lane_observer.project_exact_pod_absence_after_teardown(
            _SERVICE, 1, record_id)
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.connect() as connection:
        evidence = connection.execute(
            sqlalchemy.select(associations.c.provider_evidence).where(
                associations.c.association_id == association_id)).scalar_one()
        assert evidence == 'NOT_QUERIED'


@pytest.mark.parametrize(
    ('terminal_status', 'terminal_cause', 'launch_status'), (
        ('FAILED', 'handler_failed', common_utils.ProcessStatus.FAILED),
        ('CANCELLED', 'explicit_cancel',
         common_utils.ProcessStatus.INTERRUPTED),
    ))
def test_post_job_terminal_launch_exact_pod_absence_remains_retirable(
        admission_database, monkeypatch, terminal_status, terminal_cause,
        launch_status) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = 'd' * 64
    record_id, association_id, _ = _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=True,
        down_status=common_utils.ProcessStatus.SCHEDULED,
        terminal_status=terminal_status,
        terminal_cause=terminal_cause,
        launch_status=launch_status)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    with admission_database.connect() as connection:
        decision = repository.load_exact_pod_absence_probe_target_in_connection(
            connection,
            service_name=_SERVICE,
            replica_id=1,
            replica_record_id=record_id)
        assert decision.state is (
            kueue_lane_lineage.PhysicalAbsenceLoadState.NEEDS_PROBE)
        assert decision.target is not None
        target = decision.target
    with admission_database.begin() as connection:
        repository.record_exact_pod_absence_after_normal_teardown_in_connection(
            connection,
            target,
            provider_read_started_at=_postgres_now(connection))
    assert serve_state.remove_replica(_SERVICE,
                                      1,
                                      expected_service_hash=_SERVICE_HASH,
                                      expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
                                      expected_replica_record_id=str(record_id))
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))


def test_missing_cluster_record_whole_service_uses_exact_pod_404_with_no_down_status(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = 'd' * 64
    record_id, association_id, _ = _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=False,
        down_status=None)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)
    teardown_owner = _claim_restricted_teardown_owner()
    # Normal request GC may run long before a live replica is retired.  The
    # projected association is the durable copied terminal/quiescence receipt.
    with admission_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(request_postgres_schema.REQUESTS).where(
                request_postgres_schema.REQUESTS.c.
                ordinary_launch_association_id == association_id))
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        lambda _mode: contextlib.nullcontext())
    monkeypatch.setattr(kubernetes_adaptor, 'physical_cluster_uid_fence',
                        lambda *_args, **_kwargs: contextlib.nullcontext())

    class _MissingPodApi:

        def read_namespaced_pod(self, *_args, **_kwargs):
            raise kubernetes_adaptor.api_exception()(status=404)

    monkeypatch.setattr(kubernetes_adaptor, 'core_api',
                        lambda _context: _MissingPodApi())
    assert kueue_lane_observer.project_exact_pod_absence_after_teardown(
        _SERVICE, 1, record_id)

    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.connect() as connection:
        association_owner = connection.execute(
            sqlalchemy.select(associations.c.owner_controller_incarnation,
                              associations.c.owner_controller_epoch).where(
                                  associations.c.association_id ==
                                  association_id)).mappings().one()
    assert association_owner['owner_controller_incarnation'] == (
        teardown_owner.controller_incarnation)
    assert association_owner['owner_controller_epoch'] == (
        teardown_owner.controller_owner_epoch)

    assert serve_state.remove_service_completely(
        _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))


def test_association_gc_skips_live_kueue_admission_and_collects_other_tombstone(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    protected_key = 'd' * 64
    protected_record, protected_association, _ = (
        _install_normal_admitted_teardown_graph(
            admission_database,
            repository,
            intent_key=protected_key,
            replica_id=1,
            is_scale_down=True,
            down_status=common_utils.ProcessStatus.SCHEDULED))
    collectible_key = _canonical_intent_key(
        ordinal=1,
        observation_sequence=1,
        ordinary_zero_cost_admission_sequence=1)
    collectible_record, collectible_association, _ = (
        _install_retirable_materialized_graph(admission_database,
                                              repository,
                                              intent_key=collectible_key,
                                              replica_id=2))
    _install_canonical_cleanup_profile_authority(
        admission_database,
        intent_key=collectible_key,
        replica_id=2,
        association_id=collectible_association)
    _set_physical_provider_evidence(
        admission_database, collectible_association,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    assert serve_state.remove_replica(
        _SERVICE,
        2,
        expected_service_hash=_SERVICE_HASH,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_replica_record_id=str(collectible_record))

    associations = ordinary_launch_binding.ordinary_launch_associations_table
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    requests = request_postgres_schema.REQUESTS
    with admission_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(requests).where(
                requests.c.ordinary_launch_association_id.in_((
                    protected_association,
                    collectible_association,
                ))))
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(associations).where(
                associations.c.association_id.in_((
                    protected_association,
                    collectible_association,
                ))).values(
                    tombstone_not_before=(sqlalchemy.func.clock_timestamp() -
                                          datetime.timedelta(days=1))))
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')

        assert request_postgres.gc_bound_ordinary_launch_tombstones_in_transaction(
            connection) == 1

    with admission_database.connect() as connection:
        retained = connection.execute(
            sqlalchemy.select(associations.c.association_id).where(
                associations.c.association_id.in_((
                    protected_association,
                    collectible_association,
                )))).scalars().all()
        assert retained == [protected_association]
        admission = connection.execute(
            sqlalchemy.select(admissions.c.association_id).where(
                admissions.c.intent_idempotency_key ==
                protected_key)).scalar_one()
        assert admission == protected_association
        replica_state = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.replica_state).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE, serve_state_schema.replicas_table.c.replica_id ==
                    1)).scalar_one()
        assert replica_state['replica_record_id'] == str(protected_record)


def test_association_gc_compiles_before_serve057_relation(
        admission_database) -> None:
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with admission_database.begin() as connection:
        connection.exec_driver_sql(f'DROP TABLE {admissions.name}')
        assert request_postgres.gc_bound_ordinary_launch_tombstones_in_transaction(
            connection) == 0


def test_serve_state_batch_retirement_is_atomic_and_retains_history(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    first_key = _canonical_intent_key(observation_sequence=0,
                                      ordinary_zero_cost_admission_sequence=0)
    second_key = _canonical_intent_key(ordinal=1,
                                       observation_generation=2,
                                       observation_sequence=1,
                                       ordinary_zero_cost_admission_sequence=1)
    first_record, first_association, _ = (_install_retirable_materialized_graph(
        admission_database, repository, intent_key=first_key, replica_id=1))
    second_record, second_association, _ = (
        _install_retirable_materialized_graph(admission_database,
                                              repository,
                                              intent_key=second_key,
                                              replica_id=2,
                                              observation_generation=2))
    _install_canonical_cleanup_profile_authority(
        admission_database,
        intent_key=first_key,
        replica_id=1,
        association_id=first_association,
        protocol_sequence=2)
    _install_canonical_cleanup_profile_authority(
        admission_database,
        intent_key=second_key,
        replica_id=2,
        association_id=second_association,
        initialize_protocol=False,
        install_observation=True)
    _set_physical_provider_evidence(
        admission_database, first_association,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _set_physical_provider_evidence(
        admission_database, second_association,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)

    assert serve_state.remove_replicas(
        _SERVICE, [2, 1],
        expected_service_hash=_SERVICE_HASH,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_replica_record_ids={
            1: str(first_record),
            2: str(second_record),
        })
    _assert_retired_graph(admission_database,
                          intent_keys=(first_key, second_key),
                          replica_ids=(1, 2),
                          association_ids=(first_association,
                                           second_association))


def test_serve_state_batch_ambiguous_evidence_rolls_back_every_graph(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    first_key = _canonical_intent_key(observation_sequence=0,
                                      ordinary_zero_cost_admission_sequence=0)
    second_key = _canonical_intent_key(ordinal=1,
                                       observation_generation=2,
                                       observation_sequence=1,
                                       ordinary_zero_cost_admission_sequence=1)
    first_record, first_association, _ = (_install_retirable_materialized_graph(
        admission_database, repository, intent_key=first_key, replica_id=1))
    second_record, second_association, _ = (
        _install_retirable_materialized_graph(admission_database,
                                              repository,
                                              intent_key=second_key,
                                              replica_id=2,
                                              observation_generation=2))
    _install_canonical_cleanup_profile_authority(
        admission_database,
        intent_key=first_key,
        replica_id=1,
        association_id=first_association,
        protocol_sequence=2)
    _install_canonical_cleanup_profile_authority(
        admission_database,
        intent_key=second_key,
        replica_id=2,
        association_id=second_association,
        initialize_protocol=False,
        install_observation=True)
    _set_physical_provider_evidence(
        admission_database, first_association,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _set_physical_provider_evidence(
        admission_database, second_association,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(associations).
            where(associations.c.association_id == second_association).values(
                resolution=ordinary_launch_binding.Resolution.AMBIGUOUS.value,
                reconciliation_outcome=ordinary_launch_binding.
                ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value,
                ambiguity_code='test-provider-evidence-uncertain',
                projected_at=None,
                pin_released_at=None,
                tombstone_not_before=None,
                owner_revision=associations.c.owner_revision + 1,
                updated_at=sqlalchemy.func.clock_timestamp()))
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)

    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='provider-absence authority'):
        serve_state.remove_replicas(_SERVICE, [1, 2],
                                    expected_service_hash=_SERVICE_HASH,
                                    expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
                                    expected_replica_record_ids={
                                        1: str(first_record),
                                        2: str(second_record),
                                    })

    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    replicas = serve_state_schema.replicas_table
    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                admissions)).scalar_one() == 2
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()).select_from(intents)).scalar_one() == 2
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                replicas)).scalar_one() == 2
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()).select_from(associations).where(
                    associations.c.association_id.in_((
                        first_association,
                        second_association,
                    )))).scalar_one() == 2


def test_serve_state_missing_kueue_admission_fails_closed_without_mutation(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, _, _ = _install_retirable_materialized_graph(admission_database,
                                                            repository,
                                                            intent_key=key,
                                                            replica_id=1)
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with admission_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(admissions).where(
                admissions.c.intent_idempotency_key == key))
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)

    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='lost its admission'):
        serve_state.remove_replica(_SERVICE,
                                   1,
                                   expected_service_hash=_SERVICE_HASH,
                                   expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
                                   expected_replica_record_id=str(record_id))

    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one() == 1
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table)).scalar_one() == 1


def test_live_east_missing_admission_fallback_is_not_applicable(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=True,
        down_status=common_utils.ProcessStatus.SCHEDULED,
        observation_sequence=0,
        ordinary_zero_cost_admission_sequence=0)
    _delete_kueue_admission(admission_database, key)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)

    # The immutable worker projection is the scheduler authority: ``None``
    # classifies the ordinary East path, which owns no Kueue admission row.

    def _resolve_historical_east(
        _connection,
        _intent,
    ):
        return None

    monkeypatch.setattr(
        zero_cost_actuation,
        'kueue_teardown_identity_for_locked_intent_in_connection',
        _resolve_historical_east)
    provider_probe = mock.Mock()
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        provider_probe)

    assert not kueue_lane_observer.project_exact_pod_absence_after_teardown(
        _SERVICE, 1, record_id)
    provider_probe.assert_not_called()
    snapshot = _admissionless_graph_snapshot(admission_database, association_id)
    assert snapshot['service']['status'] == 'READY'
    assert snapshot['association']['provider_evidence'] == 'NOT_QUERIED'
    assert snapshot['admission_count'] == 0


@pytest.mark.parametrize('retain_request', (True, False))
def test_whole_service_teardown_retires_provider_absent_admissionless_graph(
        admission_database, monkeypatch, retain_request) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=False,
        down_status=None,
        observation_sequence=0,
        ordinary_zero_cost_admission_sequence=0)
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _stamp_canonical_materialization_receipt(admission_database, replica_id=1)
    _delete_kueue_admission(admission_database, key)
    if not retain_request:
        with admission_database.begin() as connection:
            connection.execute(
                sqlalchemy.delete(request_postgres_schema.REQUESTS).where(
                    request_postgres_schema.REQUESTS.c.
                    ordinary_launch_association_id == association_id))
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)
    teardown_owner = _claim_restricted_teardown_owner()
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        lambda _mode: contextlib.nullcontext())
    monkeypatch.setattr(kubernetes_adaptor, 'physical_cluster_uid_fence',
                        lambda *_args, **_kwargs: contextlib.nullcontext())
    probe = mock.Mock(
        return_value=reserved_capacity.PhysicalReplicaPresence.ABSENT)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        probe)
    assert (kueue_lane_observer.
            project_admissionless_physical_absence_after_teardown(
                _SERVICE, 1, record_id))
    probe.assert_called_once_with(reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context=_CONTEXT, physical_cluster_uid=_PHYSICAL_UID),
                                  f'{_SERVICE}-1',
                                  observed_after=mock.ANY)

    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.connect() as connection:
        evidence = connection.execute(
            sqlalchemy.select(associations.c.provider_evidence,
                              associations.c.provider_evidence_observed_at,
                              associations.c.execution_quiesced_at,
                              associations.c.provider_evidence_payload).where(
                                  associations.c.association_id ==
                                  association_id)).mappings().one()
        assert evidence['provider_evidence'] == 'ABSENT'
        assert (evidence['provider_evidence_observed_at'] >=
                evidence['execution_quiesced_at'])
        assert evidence['provider_evidence_payload'] == {
            'association_id': str(association_id),
            'cluster_name': f'{_SERVICE}-1',
            'kubernetes_context': _CONTEXT,
            'physical_cluster_uid': _PHYSICAL_UID,
            'probe_contract': 'kubernetes-physical-replica-presence-v1',
            'profile_kind': 'RESERVED_FILL',
            'replica_record_id': str(record_id),
            'result': 'ABSENT',
        }
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                kueue_lane_lineage_schema.serve_kueue_admissions_table)
        ).scalar_one() == 0
        association_owner = connection.execute(
            sqlalchemy.select(associations.c.owner_controller_incarnation,
                              associations.c.owner_controller_epoch).where(
                                  associations.c.association_id ==
                                  association_id)).mappings().one()
        assert association_owner['owner_controller_incarnation'] == (
            teardown_owner.controller_incarnation)
        assert association_owner['owner_controller_epoch'] == (
            teardown_owner.controller_owner_epoch)

    probe.reset_mock()
    probe.side_effect = AssertionError(
        'durable admissionless absence replay attempted provider I/O')
    assert (kueue_lane_observer.
            project_admissionless_physical_absence_after_teardown(
                _SERVICE, 1, record_id))
    probe.assert_not_called()

    assert serve_state.remove_service_completely(
        _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))
    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.services_table)).scalar_one() == 0


def test_historical_v5_teardown_fails_closed(admission_database) -> None:
    """Cleanup accepts only the current projection and two predecessors."""
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    projection_sha256 = _install_historical_v5_worker_projections(
        admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0,
                                worker_projection_sha256=projection_sha256)
    _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=False,
        down_status=None,
        observation_sequence=0,
        ordinary_zero_cost_admission_sequence=0,
        worker_projection_sha256=projection_sha256)

    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with admission_database.connect() as connection:
        intent = connection.execute(
            sqlalchemy.select(intents).where(
                intents.c.intent_idempotency_key == key)).mappings().one()
        with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict,
                           match='no longer resolves'):
            (zero_cost_actuation.
             kueue_admission_identity_for_locked_intent_in_connection)(
                 connection, intent)
        with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict,
                           match='no longer resolves'):
            (zero_cost_actuation.
             kueue_teardown_identity_for_locked_intent_in_connection)(
                 connection, intent)


def test_admissionless_probe_and_retirement_share_exact_row_validator(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=False,
        down_status=None,
        observation_sequence=0,
        ordinary_zero_cost_admission_sequence=0)
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _stamp_canonical_materialization_receipt(admission_database, replica_id=1)
    _delete_kueue_admission(admission_database, key)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)

    associations = ordinary_launch_binding.ordinary_launch_associations_table
    requests = request_postgres_schema.REQUESTS
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with admission_database.connect() as connection:
        lifecycle_epoch = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table.c.epoch).
            where(serve_state_schema.service_lifecycle_fences_table.c.name ==
                  _SERVICE)).scalar_one()
        service_row = dict(
            connection.execute(
                sqlalchemy.select(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    _SERVICE)).mappings().one())
        intent_row = dict(
            connection.execute(
                sqlalchemy.select(intents).where(
                    intents.c.intent_idempotency_key == key)).mappings().one())
        replica_row = dict(
            connection.execute(
                sqlalchemy.select(serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE, serve_state_schema.replicas_table.c.replica_id ==
                    1)).mappings().one())
        association_row = dict(
            connection.execute(
                sqlalchemy.select(associations).where(
                    associations.c.association_id ==
                    association_id)).mappings().one())
        request_row = dict(
            connection.execute(
                sqlalchemy.select(requests).where(
                    requests.c.ordinary_launch_association_id ==
                    association_id)).mappings().one())

        common = {
            'lifecycle_epoch': lifecycle_epoch,
            'service': service_row,
            'intent': intent_row,
            'identity': _identity(),
            'intent_idempotency_key': key,
            'replica': replica_row,
            'association': association_row,
            'request': request_row,
            'replica_id': 1,
            'replica_record_id': record_id,
        }
        validator = getattr(
            kueue_lane_lineage,
            '_validate_admissionless_retirement_rows_in_connection')
        graph = validator(connection, **common)
        assert graph.provider_absence_state is (
            kueue_lane_lineage.PhysicalAbsenceLoadState.ALREADY_PROVEN)

        invalid_rows = []
        for field, value in (('service_hash', 'replacement-service'),
                             ('service_lifecycle_epoch', _LIFECYCLE_EPOCH + 1),
                             ('service_version',
                              _SERVICE_VERSION + 1), ('projected_at', None)):
            changed = dict(association_row)
            changed[field] = value
            invalid_rows.append({'association': changed})
        changed_association = dict(association_row)
        changed_association['terminal_cause'] = ''
        changed_request = dict(request_row)
        changed_request['terminal_cause'] = ''
        invalid_rows.append({
            'association': changed_association,
            'request': changed_request,
        })
        changed_replica = dict(replica_row)
        changed_state = dict(changed_replica['replica_state'])
        changed_state['reserved_fill_intent_idempotency_key'] = '0' * 64
        changed_replica['replica_state'] = changed_state
        invalid_rows.append({'replica': changed_replica})
        changed_request = dict(request_row)
        changed_request['profile_digest'] = '0' * 64
        invalid_rows.append({'request': changed_request})

        for changes in invalid_rows:
            arguments = dict(common)
            arguments.update(changes)
            with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
                validator(connection, **arguments)


def test_admissionless_absence_publication_expires_after_slow_update(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=False,
        down_status=None,
        observation_sequence=0,
        ordinary_zero_cost_admission_sequence=0)
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _stamp_canonical_materialization_receipt(admission_database, replica_id=1)
    _delete_kueue_admission(admission_database, key)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)
    with admission_database.connect() as connection:
        decision = (
            repository.
            load_admissionless_physical_absence_probe_target_in_connection(
                connection,
                service_name=_SERVICE,
                replica_id=1,
                replica_record_id=record_id))
    assert decision.state is (
        kueue_lane_lineage.PhysicalAbsenceLoadState.NEEDS_PROBE)
    assert decision.target is not None
    target = decision.target

    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.begin() as connection:
        connection.exec_driver_sql(
            'CREATE FUNCTION delay_admissionless_absence() RETURNS trigger '
            'LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1.1); RETURN NEW; '
            'END $$')
        connection.exec_driver_sql(
            f'CREATE TRIGGER delay_admissionless_absence BEFORE UPDATE OF '
            f'provider_evidence ON {associations.name} FOR EACH ROW WHEN '
            "(NEW.provider_evidence = 'ABSENT') EXECUTE FUNCTION "
            'delay_admissionless_absence()')
    monkeypatch.setattr(kueue_lane_lineage, 'WAITING_OBSERVATION_TTL_SECONDS',
                        1)
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='expired during database publication'):
        with admission_database.begin() as connection:
            repository.record_admissionless_physical_absence_after_teardown_in_connection(
                connection,
                target,
                provider_read_started_at=_postgres_now(connection))

    with admission_database.connect() as connection:
        evidence = connection.execute(
            sqlalchemy.select(associations.c.provider_evidence).where(
                associations.c.association_id == association_id)).scalar_one()
    assert evidence == 'NOT_QUERIED'


@pytest.mark.parametrize('concurrent_change', ('admission', 'generation'))
def test_admissionless_absence_revalidates_after_unlocked_provider_read(
        admission_database, monkeypatch, concurrent_change) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_normal_admitted_teardown_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        is_scale_down=False,
        down_status=None,
        observation_sequence=0,
        ordinary_zero_cost_admission_sequence=0)
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _stamp_canonical_materialization_receipt(admission_database, replica_id=1)
    deleted_admission = _delete_kueue_admission(admission_database, key)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)
    with admission_database.connect() as connection:
        decision = (
            repository.
            load_admissionless_physical_absence_probe_target_in_connection(
                connection,
                service_name=_SERVICE,
                replica_id=1,
                replica_record_id=record_id))
        provider_read_started_at = _postgres_now(connection)
    assert decision.state is (
        kueue_lane_lineage.PhysicalAbsenceLoadState.NEEDS_PROBE)
    assert decision.target is not None

    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.begin() as connection:
        if concurrent_change == 'admission':
            connection.exec_driver_sql(
                f'ALTER TABLE {admissions.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.insert(admissions).values(**deleted_admission))
            connection.exec_driver_sql(
                f'ALTER TABLE {admissions.name} ENABLE TRIGGER USER')
        else:
            connection.exec_driver_sql(
                f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.update(associations).where(
                    associations.c.association_id == association_id).values(
                        launch_generation=associations.c.launch_generation + 1,
                        updated_at=sqlalchemy.func.clock_timestamp()))
            connection.exec_driver_sql(
                f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')

    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
        with admission_database.begin() as connection:
            (repository.
             record_admissionless_physical_absence_after_teardown_in_connection
            )(connection,
              decision.target,
              provider_read_started_at=provider_read_started_at)

    with admission_database.connect() as connection:
        evidence = connection.execute(
            sqlalchemy.select(associations.c.provider_evidence).where(
                associations.c.association_id == association_id)).scalar_one()
    assert evidence == 'NOT_QUERIED'


@pytest.mark.parametrize('invalid_evidence', (
    'not-queried',
    'present',
    'mismatched-envelope',
    'pre-quiescence',
    'profile-digest',
    'self-consistent-forged-profile',
    'request-mismatch',
    'queued-request',
    'retention-pin',
    'paid-claim',
))
def test_whole_service_admissionless_retirement_requires_exact_absence(
        admission_database, monkeypatch, invalid_evidence) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, request_id = (
        _install_normal_admitted_teardown_graph(
            admission_database,
            repository,
            intent_key=key,
            replica_id=1,
            is_scale_down=False,
            down_status=None,
            observation_sequence=0,
            ordinary_zero_cost_admission_sequence=0))
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _stamp_canonical_materialization_receipt(admission_database, replica_id=1)
    _delete_kueue_admission(admission_database, key)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    if invalid_evidence == 'present':
        _set_physical_provider_evidence(
            admission_database, association_id,
            ordinary_launch_binding.ProviderEvidence.PRESENT)
    elif invalid_evidence == 'mismatched-envelope':
        _set_physical_provider_evidence(
            admission_database,
            association_id,
            ordinary_launch_binding.ProviderEvidence.ABSENT,
            cluster_name='replacement-cluster')
    elif invalid_evidence == 'pre-quiescence':
        _set_physical_provider_evidence(
            admission_database,
            association_id,
            ordinary_launch_binding.ProviderEvidence.ABSENT,
            observed_before_quiescence=True)
    elif invalid_evidence == 'profile-digest':
        associations = (
            ordinary_launch_binding.ordinary_launch_associations_table)
        with admission_database.begin() as connection:
            connection.exec_driver_sql(
                f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.update(associations).where(
                    associations.c.association_id == association_id).values(
                        profile_digest='0' * 64,
                        updated_at=sqlalchemy.func.clock_timestamp()))
            connection.exec_driver_sql(
                f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')
        _set_physical_provider_evidence(
            admission_database, association_id,
            ordinary_launch_binding.ProviderEvidence.ABSENT)
    elif invalid_evidence == 'self-consistent-forged-profile':
        associations = (
            ordinary_launch_binding.ordinary_launch_associations_table)
        requests = request_postgres_schema.REQUESTS
        with admission_database.begin() as connection:
            service_row = connection.execute(
                sqlalchemy.select(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    _SERVICE)).mappings().one()
            replica_row = connection.execute(
                sqlalchemy.select(serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE, serve_state_schema.replicas_table.c.replica_id ==
                    1)).mappings().one()
            info = serve_state.decode_replica_state_for_authority(
                replica_row['replica_state_version'],
                replica_row['replica_state'])
            forged_payload = (
                ordinary_launch_binding._reserved_fill_cleanup_payload(  # pylint: disable=protected-access
                    connection, service_row, info))
            forged_payload['worker_projection_sha256'] = '9' * 64
            forged_profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
                ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
                authorization_reference=f'reserved-fill:{key}',
                authorization_generation=1,
                authorization_payload=forged_payload)
            connection.exec_driver_sql(
                f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
            connection.exec_driver_sql(
                f'ALTER TABLE {requests.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.update(associations).where(
                    associations.c.association_id == association_id).values(
                        profile_digest=forged_profile.digest,
                        authorization_digest=(
                            forged_profile.authorization_digest),
                        updated_at=sqlalchemy.func.clock_timestamp()))
            connection.execute(
                sqlalchemy.update(requests).where(
                    requests.c.request_id == request_id).values(
                        profile_digest=forged_profile.digest,
                        updated_at=sqlalchemy.func.clock_timestamp()))
            connection.exec_driver_sql(
                f'ALTER TABLE {requests.name} ENABLE TRIGGER USER')
            connection.exec_driver_sql(
                f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')
        _set_physical_provider_evidence(
            admission_database, association_id,
            ordinary_launch_binding.ProviderEvidence.ABSENT)
    elif invalid_evidence == 'request-mismatch':
        requests = request_postgres_schema.REQUESTS
        with admission_database.begin() as connection:
            connection.exec_driver_sql(
                f'ALTER TABLE {requests.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.update(requests).where(
                    requests.c.request_id == request_id).values(
                        terminal_cause='explicit_cancel',
                        updated_at=sqlalchemy.func.clock_timestamp()))
            connection.exec_driver_sql(
                f'ALTER TABLE {requests.name} ENABLE TRIGGER USER')
        _set_physical_provider_evidence(
            admission_database, association_id,
            ordinary_launch_binding.ProviderEvidence.ABSENT)
    elif invalid_evidence == 'queued-request':
        with admission_database.begin() as connection:
            now = _postgres_now(connection)
            connection.execute(
                sqlalchemy.insert(request_postgres_schema.QUEUE).values(
                    request_id=request_id,
                    schedule_type='short',
                    priority=0,
                    available_at=now,
                    enqueued_at=now,
                    ignore_return_value=True,
                    retryable=False,
                    precondition_attempts=0,
                    delivery_state='queued',
                    updated_at=now))
        _set_physical_provider_evidence(
            admission_database, association_id,
            ordinary_launch_binding.ProviderEvidence.ABSENT)
    elif invalid_evidence == 'retention-pin':
        with admission_database.begin() as connection:
            request_postgres.insert_request_retention_pin_in_transaction(
                connection, request_id, 'reserved-fill-test.v1', association_id)
        _set_physical_provider_evidence(
            admission_database, association_id,
            ordinary_launch_binding.ProviderEvidence.ABSENT)
    elif invalid_evidence == 'paid-claim':
        with admission_database.begin() as connection:
            connection.execute(
                sqlalchemy.insert(
                    serve_state_schema.paid_capacity_pools_table).values(
                        pool_key='forbidden-paid-pool',
                        current_limit=1,
                        successes_since_resize=0,
                        updated_at=time.time()))
            connection.execute(
                sqlalchemy.insert(
                    serve_state_schema.paid_capacity_claims_table).values(
                        service_name=_SERVICE,
                        service_hash=_SERVICE_HASH,
                        replica_id=1,
                        pool_key='forbidden-paid-pool',
                        priority=1,
                        claimed_at=time.time()))
        _set_physical_provider_evidence(
            admission_database, association_id,
            ordinary_launch_binding.ProviderEvidence.ABSENT)

    before = _admissionless_graph_snapshot(admission_database, association_id)
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
        serve_state.remove_service_completely(
            _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    assert _admissionless_graph_snapshot(admission_database,
                                         association_id) == before
    assert before['admission_count'] == 0
    assert before['replicas'][0]['replica_state']['replica_record_id'] == str(
        record_id)


def test_serve_state_whole_service_teardown_retires_exact_graph(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    _, association_id, _ = _install_retirable_materialized_graph(
        admission_database, repository, intent_key=key, replica_id=1)
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    assert serve_state.remove_service_completely(
        _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))
    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.services_table)).scalar_one() == 0


@pytest.mark.parametrize(('admission_state', 'effect_phase'), (
    (kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING, 'PROVIDER_IO'),
    (kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED, 'PROVIDER_IO'),
    (kueue_lane_lineage.KueueAdmissionState.POD_WAITING, 'PROVIDER_IO'),
    (kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED, 'SERVICE_JOB_IO'),
))
@pytest.mark.parametrize('gate_generation', (1, 2))
def test_normal_failure_atomically_retires_provider_free_pre_job_absence(
        admission_database, monkeypatch, admission_state, effect_phase,
        gate_generation) -> None:
    """The current or a newer gate replaces an exactly absent failed try."""
    key, record_id, association_id, _ = (
        _install_generation_fenced_pre_job_graph(
            admission_database,
            admission_state=admission_state,
            effect_phase=effect_phase))
    if gate_generation > 1:
        _advance_reconciliation_gate(admission_database,
                                     generation=gate_generation)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)

    provider_phase = mock.Mock(side_effect=AssertionError(
        'normal adjudication entered provider phase'))
    pod_probe = mock.Mock(
        side_effect=AssertionError('normal adjudication read a Pod'))
    cluster_probe = mock.Mock(
        side_effect=AssertionError('normal adjudication read a cluster'))
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        provider_phase)
    monkeypatch.setattr(kubernetes_adaptor, 'core_api', pod_probe)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        cluster_probe)

    assert kueue_lane_observer.project_exact_pod_absence_after_teardown(
        _SERVICE, 1, record_id)
    provider_phase.assert_not_called()
    pod_probe.assert_not_called()
    cluster_probe.assert_not_called()
    assert serve_state.remove_replica(_SERVICE,
                                      1,
                                      expected_service_hash=_SERVICE_HASH,
                                      expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
                                      expected_controller_owner=(1, '10.0.0.1'),
                                      expected_replica_record_id=str(record_id),
                                      allow_active_provider_free_pre_job=True)
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))


def test_active_provider_free_pre_job_rejects_unexpired_waiting_receipt(
        admission_database, monkeypatch) -> None:
    key, record_id, association_id, _ = (
        _install_generation_fenced_pre_job_graph(
            admission_database,
            admission_state=(
                kueue_lane_lineage.KueueAdmissionState.POD_WAITING),
            expire_waiting_receipt=False))
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)

    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='has not expired'):
        serve_state.remove_replica(_SERVICE,
                                   1,
                                   expected_service_hash=_SERVICE_HASH,
                                   expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
                                   expected_controller_owner=(1, '10.0.0.1'),
                                   expected_replica_record_id=str(record_id),
                                   allow_active_provider_free_pre_job=True)

    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                kueue_lane_lineage_schema.serve_kueue_admissions_table).where(
                    kueue_lane_lineage_schema.serve_kueue_admissions_table.c.
                    intent_idempotency_key == key)).scalar_one() == 1
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE, serve_state_schema.replicas_table.c.replica_id ==
                    1)).scalar_one() == 1
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == association_id)).scalar_one() == 1


def test_provider_free_pre_job_rejects_nonnull_service_job_identity(
        admission_database) -> None:
    """SERVICE_JOB_IO/null is accepted; a job identity is never pre-job."""
    _, _, association_id, _ = _install_generation_fenced_pre_job_graph(
        admission_database,
        admission_state=(
            kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED),
        effect_phase='SERVICE_JOB_IO')
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.connect() as connection:
        association = dict(
            connection.execute(
                sqlalchemy.select(associations).where(
                    associations.c.association_id ==
                    association_id)).mappings().one())
    association['service_job_id'] = 91
    assert not kueue_lane_lineage._is_provider_absent_pre_job_association(  # pylint: disable=protected-access
        association)

    # The durable schema makes the rejected SERVICE_JOB_IO/non-null shape
    # unrepresentable as well: a non-null job id requires RECORDED phase.
    with pytest.raises(sqlalchemy.exc.DBAPIError):
        with admission_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(associations).where(
                    associations.c.association_id == association_id).values(
                        service_job_id=91))


def test_normal_pre_job_absence_accepts_same_active_generation_gate(
        admission_database) -> None:
    _, record_id, _, _ = _install_generation_fenced_pre_job_graph(
        admission_database)
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)

    with admission_database.connect() as connection:
        decision = repository.load_generation_fenced_pre_job_absence_in_connection(
            connection,
            service_name=_SERVICE,
            replica_id=1,
            replica_record_id=record_id)
    assert decision.state is (
        kueue_lane_lineage.PhysicalAbsenceLoadState.ALREADY_PROVEN)


@pytest.mark.parametrize('invalid_evidence', (
    'request-mismatch',
    'queued-request',
    'retention-pin',
    'paid-claim',
    'provider-digest',
    'admission-receipt',
    'live-replica',
))
def test_normal_pre_job_absence_rejects_incomplete_durable_graph(
        admission_database, invalid_evidence) -> None:
    _, record_id, association_id, request_id = (
        _install_generation_fenced_pre_job_graph(
            admission_database,
            admission_state=(
                kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED
                if invalid_evidence == 'admission-receipt' else
                kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING)))
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    requests = request_postgres_schema.REQUESTS
    if invalid_evidence == 'request-mismatch':
        with admission_database.begin() as connection:
            connection.exec_driver_sql(
                f'ALTER TABLE {requests.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.update(requests).where(
                    requests.c.request_id == request_id).values(
                        terminal_cause='explicit_cancel',
                        updated_at=sqlalchemy.func.clock_timestamp()))
            connection.exec_driver_sql(
                f'ALTER TABLE {requests.name} ENABLE TRIGGER USER')
    elif invalid_evidence == 'queued-request':
        with admission_database.begin() as connection:
            now = _postgres_now(connection)
            connection.execute(
                sqlalchemy.insert(request_postgres_schema.QUEUE).values(
                    request_id=request_id,
                    schedule_type='short',
                    priority=0,
                    available_at=now,
                    enqueued_at=now,
                    ignore_return_value=True,
                    retryable=False,
                    precondition_attempts=0,
                    delivery_state='queued',
                    updated_at=now))
    elif invalid_evidence == 'retention-pin':
        with admission_database.begin() as connection:
            request_postgres.insert_request_retention_pin_in_transaction(
                connection, request_id, 'reserved-fill-test.v1', association_id)
    elif invalid_evidence == 'paid-claim':
        with admission_database.begin() as connection:
            connection.execute(
                sqlalchemy.insert(
                    serve_state_schema.paid_capacity_pools_table).values(
                        pool_key='forbidden-paid-pool',
                        current_limit=1,
                        successes_since_resize=0,
                        updated_at=time.time()))
            connection.execute(
                sqlalchemy.insert(
                    serve_state_schema.paid_capacity_claims_table).values(
                        service_name=_SERVICE,
                        service_hash=_SERVICE_HASH,
                        replica_id=1,
                        pool_key='forbidden-paid-pool',
                        priority=1,
                        claimed_at=time.time()))
    elif invalid_evidence == 'provider-digest':
        with admission_database.begin() as connection:
            connection.exec_driver_sql(
                f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.update(associations).where(
                    associations.c.association_id == association_id).values(
                        provider_evidence_digest='9' * 64,
                        updated_at=sqlalchemy.func.clock_timestamp()))
            connection.exec_driver_sql(
                f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')
    elif invalid_evidence == 'admission-receipt':
        admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
        with admission_database.begin() as connection:
            receipt = connection.execute(
                sqlalchemy.select(admissions.c.pod_receipt).where(
                    admissions.c.service_name == _SERVICE,
                    admissions.c.replica_id == 1)).scalar_one()
            receipt = dict(receipt)
            receipt['pod'] = dict(receipt['pod'])
            receipt['pod']['uid'] = 'forged-pod-uid'
            connection.exec_driver_sql(
                f'ALTER TABLE {admissions.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.update(admissions).where(
                    admissions.c.service_name == _SERVICE,
                    admissions.c.replica_id == 1).values(
                        pod_receipt=receipt,
                        pod_receipt_sha256=_receipt_digest(connection, receipt),
                        updated_at=sqlalchemy.func.clock_timestamp()))
            connection.exec_driver_sql(
                f'ALTER TABLE {admissions.name} ENABLE TRIGGER USER')
    else:
        replicas = serve_state_schema.replicas_table
        with admission_database.begin() as connection:
            replica = connection.execute(
                sqlalchemy.select(replicas).where(
                    replicas.c.service_name == _SERVICE,
                    replicas.c.replica_id == 1)).mappings().one()
            info = serve_state.decode_replica_state_for_authority(
                replica['replica_state_version'], replica['replica_state'])
            info.status_property.sky_launch_status = (
                common_utils.ProcessStatus.SUCCEEDED)
            info.status_property.sky_down_status = None
            info.status_property.service_ready_now = True
            connection.exec_driver_sql(
                f'ALTER TABLE {replicas.name} DISABLE TRIGGER USER')
            connection.execute(
                sqlalchemy.update(replicas).where(
                    replicas.c.service_name == _SERVICE,
                    replicas.c.replica_id == 1).values(
                        **serve_state._replica_row_values(  # pylint: disable=protected-access
                            _SERVICE, 1, info)))
            connection.exec_driver_sql(
                f'ALTER TABLE {replicas.name} ENABLE TRIGGER USER')
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)

    with admission_database.connect() as connection:
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
            repository.load_generation_fenced_pre_job_absence_in_connection(
                connection,
                service_name=_SERVICE,
                replica_id=1,
                replica_record_id=record_id)


def test_whole_service_retires_pre_serve057_provider_absence_without_admission(
        admission_database, monkeypatch) -> None:
    """Keep the pre-job tombstone path for retained pre-Serve057 replicas."""
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_retirable_materialized_graph(
        admission_database, repository, intent_key=key, replica_id=1)
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _delete_kueue_admission(admission_database, key)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    assert serve_state.remove_service_completely(
        _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))
    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.services_table)).scalar_one() == 0
        retained = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                replica_record_id).where(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.association_id == association_id)).scalar_one()
        assert retained == record_id


@pytest.mark.parametrize('retain_pending_admission', (False, True))
def test_whole_service_retires_marker_free_pre_job_provider_absence(
        admission_database, monkeypatch, retain_pending_admission) -> None:
    """Retire the exact pre-job shape observed in production lifecycle 84."""
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_retirable_materialized_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        cleanup_marker=False,
        effect_phase='PROVIDER_IO')
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    if not retain_pending_admission:
        _delete_kueue_admission(admission_database, key)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    provider_phase = mock.Mock(
        side_effect=AssertionError('pre-job replay attempted provider phase'))
    provider_probe = mock.Mock(
        side_effect=AssertionError('pre-job replay attempted provider read'))
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        provider_phase)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        provider_probe)
    assert kueue_lane_observer.project_exact_pod_absence_after_teardown(
        _SERVICE, 1, record_id)
    provider_phase.assert_not_called()
    provider_probe.assert_not_called()

    assert serve_state.remove_service_completely(
        _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    _assert_retired_graph(admission_database,
                          intent_keys=(key,),
                          replica_ids=(1,),
                          association_ids=(association_id,))


@pytest.mark.parametrize('service_status', ('SHUTTING_DOWN', 'FAILED_CLEANUP'))
def test_whole_service_retires_mixed_retained_pre_job_provider_absence(
        admission_database, monkeypatch, service_status) -> None:
    """Atomically retire the POLICY_ADMITTED plus expired-waiting batch."""
    admitted_key, admitted_record_id, admitted_association_id, _ = (
        _install_generation_fenced_pre_job_graph(
            admission_database,
            admission_state=(
                kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED)))
    waiting_key, waiting_record_id, waiting_association_id, _ = (
        _install_generation_fenced_pre_job_graph(
            admission_database,
            admission_state=(
                kueue_lane_lineage.KueueAdmissionState.POD_WAITING),
            replica_id=2,
            initialize_protocol=False))
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)
    if service_status == 'FAILED_CLEANUP':
        with admission_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    _SERVICE).values(status=service_status))

    provider_phase = mock.Mock(side_effect=AssertionError(
        'admitted pre-job replay attempted provider phase'))
    pod_probe = mock.Mock(
        side_effect=AssertionError('admitted pre-job replay read a Pod'))
    cluster_probe = mock.Mock(
        side_effect=AssertionError('admitted pre-job replay read a cluster'))
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        provider_phase)
    monkeypatch.setattr(kubernetes_adaptor, 'core_api', pod_probe)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        cluster_probe)

    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    for replica_id, record_id in ((1, admitted_record_id), (2,
                                                            waiting_record_id)):
        with admission_database.connect() as connection:
            decision = (
                repository.load_whole_service_pre_job_absence_in_connection(
                    connection,
                    service_name=_SERVICE,
                    replica_id=replica_id,
                    replica_record_id=record_id))
        assert decision.state is (
            kueue_lane_lineage.PhysicalAbsenceLoadState.ALREADY_PROVEN)
        assert kueue_lane_observer.project_exact_pod_absence_after_teardown(
            _SERVICE, replica_id, record_id)
    provider_phase.assert_not_called()
    pod_probe.assert_not_called()
    cluster_probe.assert_not_called()

    if service_status == 'FAILED_CLEANUP':
        # The supported purge entrypoint moves a retry back through
        # SHUTTING_DOWN before this final transactional deletion boundary.
        with admission_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    _SERVICE).values(status='SHUTTING_DOWN'))
    assert serve_state.remove_service_completely(
        _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    _assert_retired_graph(admission_database,
                          intent_keys=(admitted_key, waiting_key),
                          replica_ids=(1, 2),
                          association_ids=(admitted_association_id,
                                           waiting_association_id))
    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.services_table)).scalar_one() == 0


@pytest.mark.parametrize('admission_state', (
    kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
    kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
))
def test_whole_service_pre_job_absence_rejects_live_service(
        admission_database, monkeypatch, admission_state) -> None:
    _, record_id, _, _ = _install_generation_fenced_pre_job_graph(
        admission_database, admission_state=admission_state)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)

    with admission_database.connect() as connection:
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                           match='outside whole-service teardown'):
            repository.load_whole_service_pre_job_absence_in_connection(
                connection,
                service_name=_SERVICE,
                replica_id=1,
                replica_record_id=record_id)


@pytest.mark.parametrize('admission_state', (
    kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
    kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
))
def test_whole_service_pre_job_absence_rejects_malformed_admission_receipt(
        admission_database, monkeypatch, admission_state) -> None:
    _, record_id, association_id, _ = (_install_generation_fenced_pre_job_graph(
        admission_database, admission_state=admission_state))
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with admission_database.begin() as connection:
        receipt = connection.execute(
            sqlalchemy.select(admissions.c.pod_receipt).where(
                admissions.c.service_name == _SERVICE,
                admissions.c.replica_id == 1)).scalar_one()
        receipt = dict(receipt)
        receipt['pod'] = dict(receipt['pod'])
        receipt['pod']['uid'] = 'forged-pod-uid'
        connection.exec_driver_sql(
            f'ALTER TABLE {admissions.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(admissions).where(
                admissions.c.service_name == _SERVICE,
                admissions.c.replica_id == 1).values(
                    pod_receipt=receipt,
                    pod_receipt_sha256=_receipt_digest(connection, receipt)))
        connection.exec_driver_sql(
            f'ALTER TABLE {admissions.name} ENABLE TRIGGER USER')

    before = _admissionless_graph_snapshot(admission_database, association_id)
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.connect() as connection:
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
            repository.load_whole_service_pre_job_absence_in_connection(
                connection,
                service_name=_SERVICE,
                replica_id=1,
                replica_record_id=record_id)
    assert _admissionless_graph_snapshot(admission_database,
                                         association_id) == before


@pytest.mark.parametrize('admission_state', (
    kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
    kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
))
def test_whole_service_pre_job_absence_rejects_non_absent_provider_evidence(
        admission_database, monkeypatch, admission_state) -> None:
    _, record_id, association_id, _ = (_install_generation_fenced_pre_job_graph(
        admission_database, admission_state=admission_state))
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(associations).where(
                associations.c.association_id == association_id).values(
                    resolution='AMBIGUOUS',
                    reconciliation_outcome='POST_EFFECT_AMBIGUOUS',
                    ambiguity_code='provider-present-test',
                    projected_at=None,
                    pin_released_at=None,
                    tombstone_not_before=None,
                    updated_at=sqlalchemy.func.clock_timestamp()))
        connection.exec_driver_sql(
            f'ALTER TABLE {associations.name} ENABLE TRIGGER USER')
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.PRESENT)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    before = _admissionless_graph_snapshot(admission_database, association_id)
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.connect() as connection:
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                           match='neither a materialized service launch nor'):
            repository.load_whole_service_pre_job_absence_in_connection(
                connection,
                service_name=_SERVICE,
                replica_id=1,
                replica_record_id=record_id)
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='provider-absence authority'):
        serve_state.remove_service_completely(
            _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    assert _admissionless_graph_snapshot(admission_database,
                                         association_id) == before


def test_whole_service_pre_job_absence_rejects_unexpired_waiting_receipt(
        admission_database, monkeypatch) -> None:
    _, record_id, association_id, _ = (_install_generation_fenced_pre_job_graph(
        admission_database,
        admission_state=kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
        expire_waiting_receipt=False))
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    before = _admissionless_graph_snapshot(admission_database, association_id)
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.connect() as connection:
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                           match='has not expired'):
            repository.load_whole_service_pre_job_absence_in_connection(
                connection,
                service_name=_SERVICE,
                replica_id=1,
                replica_record_id=record_id)
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
        serve_state.remove_service_completely(
            _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    assert _admissionless_graph_snapshot(admission_database,
                                         association_id) == before


def test_whole_service_pre_job_absence_rejects_receipt_newer_than_provider_evidence(
        admission_database, monkeypatch) -> None:
    _, record_id, association_id, _ = (_install_generation_fenced_pre_job_graph(
        admission_database,
        admission_state=(
            kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED)))
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with admission_database.begin() as connection:
        provider_evidence_observed_at = connection.execute(
            sqlalchemy.select(
                associations.c.provider_evidence_observed_at).where(
                    associations.c.association_id ==
                    association_id)).scalar_one()
        connection.exec_driver_sql(
            f'ALTER TABLE {admissions.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(admissions).where(
                admissions.c.service_name == _SERVICE,
                admissions.c.replica_id == 1).values(
                    updated_at=(provider_evidence_observed_at +
                                datetime.timedelta(microseconds=1))))
        connection.exec_driver_sql(
            f'ALTER TABLE {admissions.name} ENABLE TRIGGER USER')
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    before = _admissionless_graph_snapshot(admission_database, association_id)
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.connect() as connection:
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                           match='receipt is newer than provider absence'):
            repository.load_whole_service_pre_job_absence_in_connection(
                connection,
                service_name=_SERVICE,
                replica_id=1,
                replica_record_id=record_id)
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
        serve_state.remove_service_completely(
            _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    assert _admissionless_graph_snapshot(admission_database,
                                         association_id) == before


def test_pre_job_absence_rejects_foreign_admission_for_same_replica(
        admission_database, monkeypatch) -> None:
    """A stale admission cannot hide behind a different intent/record pair."""
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_retirable_materialized_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        cleanup_marker=False,
        effect_phase='PROVIDER_IO')
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)

    foreign_key = _canonical_intent_key(ordinal=1,
                                        observation_sequence=1,
                                        ordinary_zero_cost_admission_sequence=1)
    _insert_intent(admission_database,
                   foreign_key,
                   ordinal=1,
                   observation_sequence=1,
                   ordinary_zero_cost_admission_sequence=1)
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with admission_database.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {admissions.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(admissions).where(
                admissions.c.intent_idempotency_key == key).values(
                    intent_idempotency_key=foreign_key,
                    replica_record_id=uuid.uuid4(),
                    updated_at=sqlalchemy.func.clock_timestamp()))
        connection.exec_driver_sql(
            f'ALTER TABLE {admissions.name} ENABLE TRIGGER USER')
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    provider_phase = mock.Mock(side_effect=AssertionError(
        'foreign admission attempted provider phase'))
    provider_probe = mock.Mock(
        side_effect=AssertionError('foreign admission attempted provider read'))
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        provider_phase)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        provider_probe)
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
        kueue_lane_observer.project_exact_pod_absence_after_teardown(
            _SERVICE, 1, record_id)
    provider_phase.assert_not_called()
    provider_probe.assert_not_called()


@pytest.mark.parametrize(('column', 'value'),
                         (('cluster_name', 'foreign'), ('status', 'READY')))
def test_pre_job_absence_rejects_divergent_replica_scalar(
        admission_database, monkeypatch, column, value) -> None:
    """Scalar indexes cannot contradict the versioned replica authority."""
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_retirable_materialized_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        cleanup_marker=False,
        effect_phase='PROVIDER_IO')
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _delete_kueue_admission(admission_database, key)
    replicas = serve_state_schema.replicas_table
    with admission_database.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == 1).values(**{column: value}))
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} ENABLE TRIGGER USER')
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    provider_phase = mock.Mock(side_effect=AssertionError(
        'divergent replica attempted provider phase'))
    provider_probe = mock.Mock(
        side_effect=AssertionError('divergent replica attempted provider read'))
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        provider_phase)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        provider_probe)
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='canonical provider-absence authority'):
        kueue_lane_observer.project_exact_pod_absence_after_teardown(
            _SERVICE, 1, record_id)
    provider_phase.assert_not_called()
    provider_probe.assert_not_called()


def test_whole_service_rejects_live_replica_with_pre_job_absence(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = _canonical_intent_key(observation_sequence=0,
                                ordinary_zero_cost_admission_sequence=0)
    record_id, association_id, _ = _install_retirable_materialized_graph(
        admission_database,
        repository,
        intent_key=key,
        replica_id=1,
        cleanup_marker=False,
        effect_phase='PROVIDER_IO')
    _install_canonical_cleanup_profile_authority(admission_database,
                                                 intent_key=key,
                                                 replica_id=1,
                                                 association_id=association_id)
    _set_physical_provider_evidence(
        admission_database, association_id,
        ordinary_launch_binding.ProviderEvidence.ABSENT)
    _delete_kueue_admission(admission_database, key)
    replicas = serve_state_schema.replicas_table
    with admission_database.begin() as connection:
        replica = connection.execute(
            sqlalchemy.select(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == 1)).mappings().one()
        info = serve_state.decode_replica_state_for_authority(
            replica['replica_state_version'], replica['replica_state'])
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.sky_down_status = None
        info.status_property.service_ready_now = True
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == 1).values(
                    **serve_state._replica_row_values(  # pylint: disable=protected-access
                        _SERVICE, 1, info)))
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} ENABLE TRIGGER USER')
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    provider_phase = mock.Mock(
        side_effect=AssertionError('invalid replay attempted provider phase'))
    provider_probe = mock.Mock(
        side_effect=AssertionError('invalid replay attempted provider read'))
    monkeypatch.setattr(kueue_lane_observer.provider_phase, 'provider_phase',
                        provider_phase)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        provider_probe)
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='live or materialized replica state'):
        kueue_lane_observer.project_exact_pod_absence_after_teardown(
            _SERVICE, 1, record_id)
    provider_phase.assert_not_called()
    provider_probe.assert_not_called()


@pytest.mark.parametrize('intent_state', ('GRANTED', 'RETRYABLE', 'ACTUATING'))
def test_serve_state_whole_service_teardown_terminalizes_provider_free_intent(
        admission_database, monkeypatch, intent_state) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = '8' * 64
    overrides = {'state': intent_state}
    if intent_state == 'ACTUATING':
        overrides.update({
            'lease_generation': 1,
            'lease_owner': uuid.uuid4(),
            'lease_expires_at': datetime.datetime.now(datetime.timezone.utc) +
                                datetime.timedelta(minutes=5),
        })
    _insert_intent(admission_database, key, **overrides)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, _identity(),
                                                       key)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    assert serve_state.remove_service_completely(
        _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)

    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.services_table)).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                kueue_lane_lineage_schema.serve_kueue_admissions_table)
        ).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table)).scalar_one() == 0


def test_serve_state_whole_service_teardown_keeps_materialization_race(
        admission_database, monkeypatch) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    key = '8' * 64
    _insert_intent(admission_database, key)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, _identity(),
                                                       key)
    _materialize(admission_database, repository, _identity(), key)
    _configure_serve_state_for_kueue_retirement(monkeypatch, admission_database)
    _mark_service_shutting_down(admission_database)

    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
        serve_state.remove_service_completely(
            _SERVICE, _SERVICE_HASH, expected_lifecycle_epoch=_LIFECYCLE_EPOCH)

    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.services_table)).scalar_one() == 1
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                kueue_lane_lineage_schema.serve_kueue_admissions_table)
        ).scalar_one() == 1
        state = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table.c.state)).scalar_one()
        assert state == 'COMMITTED'


def test_serve057_is_linear_postgresql_only_predecessor() -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    assert scripts.get_heads() == ['059']
    assert scripts.get_revision('057').down_revision == '056'
    assert migration_utils.SERVE_VERSION == '059'
    assert migration_utils.serve_target_version(sqlite) == '037'
    with pytest.raises(RuntimeError, match='PostgreSQL-only'):
        alembic_command.upgrade(config, '057')


def test_schema_is_one_three_state_relation_with_restrictive_graph_fks(
        admission_database) -> None:
    inspector = sqlalchemy.inspect(admission_database)
    table = kueue_lane_lineage_schema.serve_kueue_admissions_table.name
    assert table == 'serve_kueue_admissions'
    assert inspector.get_pk_constraint(table)['constrained_columns'] == [
        'intent_idempotency_key'
    ]
    indexes = {item['name']: item for item in inspector.get_indexes(table)}
    assert 'uq_serve057_kueue_admission_preadmission_domain' not in indexes
    surge = indexes['uq_serve057_kueue_admission_surge']
    assert surge['unique']
    assert surge['column_names'] == ['service_name']
    assert 'replacement_surge_units > 0' in str(
        surge['dialect_options']['postgresql_where'])

    foreign_keys = {
        item['name']: item for item in inspector.get_foreign_keys(table)
    }
    assert set(foreign_keys) == {
        'serve057_kueue_admission_intent_fk',
        'serve057_kueue_admission_replica_fk',
        'serve057_kueue_admission_association_fk',
    }
    assert foreign_keys['serve057_kueue_admission_intent_fk'][
        'referred_table'] == 'serve_zero_cost_actuation_intents'
    assert foreign_keys['serve057_kueue_admission_replica_fk'][
        'referred_table'] == 'replicas'
    assert foreign_keys['serve057_kueue_admission_association_fk'][
        'referred_table'] == 'serve_ordinary_launch_associations'
    assert all(value['options']['ondelete'] == 'RESTRICT'
               for value in foreign_keys.values())
    checks = ' '.join(
        item['sqltext'] for item in inspector.get_check_constraints(table))
    for state in ('INTENT_PENDING', 'POD_WAITING', 'POLICY_ADMITTED'):
        assert state in checks
    for removed in ('PAID_HANDOFF', 'PAID_OCCUPIED', 'REPROBE_READY'):
        assert removed not in checks


def test_grant_time_insert_copies_frozen_unit_and_replays_exactly(
        admission_database) -> None:
    key = '1' * 64
    _insert_intent(admission_database,
                   key,
                   accelerator_count=8,
                   capacity_unit='logical',
                   planned_capacity=8)
    identity = _identity(accelerator_count=8)
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    compatibility = 'f' * 64
    with admission_database.begin() as connection:
        first = repository.insert_intent_pending_in_connection(
            connection,
            identity,
            key,
            replacement_surge_units=6,
            replacement_compatibility_sha256=compatibility)
        replay = repository.insert_intent_pending_in_connection(
            connection,
            identity,
            key,
            replacement_surge_units=6,
            replacement_compatibility_sha256=compatibility)
    assert replay == first
    assert first.state is kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING
    assert first.capacity_unit == 'logical'
    assert first.planned_capacity == 8
    assert first.replacement_surge_units == 6
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
        with admission_database.begin() as connection:
            repository.insert_intent_pending_in_connection(
                connection,
                identity,
                key,
                replacement_surge_units=5,
                replacement_compatibility_sha256=compatibility)


def test_same_domain_grant_batch_creates_one_admission_per_intent(
        admission_database) -> None:
    keys = ('c' * 64, 'd' * 64, 'e' * 64)
    for ordinal, key in enumerate(keys):
        _insert_intent(admission_database, key, ordinal=ordinal)
    identity = _identity()
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.begin() as connection:
        rows = tuple(
            repository.insert_intent_pending_in_connection(
                connection, identity, key) for key in keys)
    assert tuple(row.intent_idempotency_key for row in rows) == keys
    assert len({row.unresolved_domain_sha256 for row in rows}) == 1
    with admission_database.begin() as connection:
        locked = repository.lock_service_admissions_in_connection(
            connection, _SERVICE, _SERVICE_HASH)
    assert tuple(row.intent_idempotency_key for row in locked) == keys


def test_materialization_binds_association_and_enforces_delete_order(
        admission_database) -> None:
    key = '2' * 64
    _insert_intent(admission_database, key)
    identity = _identity()
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, identity,
                                                       key)
    record_id, association_id = _materialize(admission_database, repository,
                                             identity, key)
    with admission_database.begin() as connection:
        replay = repository.bind_materialized_in_connection(
            connection,
            identity,
            intent_idempotency_key=key,
            replica_id=1,
            replica_record_id=record_id,
            provider_cluster_generation=9,
            association_id=association_id)
        assert replay.association_id == association_id
        assert repository.validate_materialized_in_connection(
            connection,
            identity,
            intent_idempotency_key=key,
            replica_id=1,
            replica_record_id=record_id,
            provider_cluster_generation=9,
            association_id=association_id) == replay
        assert repository.validate_materialized_in_connection(
            connection,
            identity,
            intent_idempotency_key=key,
            replica_id=1,
            replica_record_id=record_id,
            provider_cluster_generation=9,
            association_id=uuid.uuid4()) is None

    admission = kueue_lane_lineage_schema.serve_kueue_admissions_table
    association = ordinary_launch_binding.ordinary_launch_associations_table
    replica = serve_state_schema.replicas_table
    intent = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    for parent, statement in (
        (association, sqlalchemy.delete(association).where(
            association.c.association_id == association_id)),
        (replica,
         sqlalchemy.delete(replica).where(replica.c.service_name == _SERVICE,
                                          replica.c.replica_id == 1)),
        (intent, sqlalchemy.delete(intent).where(
            intent.c.intent_idempotency_key == key)),
    ):
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            with admission_database.begin() as connection:
                # Isolate the Serve057 FK from the older user-trigger guards;
                # the failing transaction rolls this ALTER back as well.
                connection.exec_driver_sql(
                    f'ALTER TABLE {parent.name} DISABLE TRIGGER USER')
                connection.execute(statement)

    # Evidence-backed cleanup settles/clears the ordinary edge before this
    # structural order.  The admission must still precede every graph parent.
    with admission_database.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {replica.name} DISABLE TRIGGER USER')
        connection.exec_driver_sql(
            f'ALTER TABLE {intent.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.delete(admission).where(
                admission.c.intent_idempotency_key == key))
        connection.execute(
            sqlalchemy.delete(replica).where(replica.c.service_name == _SERVICE,
                                             replica.c.replica_id == 1))
        connection.execute(
            sqlalchemy.delete(intent).where(
                intent.c.intent_idempotency_key == key))
        connection.exec_driver_sql(
            f'ALTER TABLE {intent.name} ENABLE TRIGGER USER')
        connection.exec_driver_sql(
            f'ALTER TABLE {replica.name} ENABLE TRIGGER USER')
    with admission_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(association.c.association_id).where(
                association.c.association_id ==
                association_id)).scalar_one() == association_id


def test_waiting_receipt_renews_then_admission_is_monotonic(
        admission_database) -> None:
    key = '3' * 64
    _insert_intent(admission_database, key)
    identity = _identity()
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, identity,
                                                       key)
    record_id, association_id = _materialize(admission_database, repository,
                                             identity, key)
    waiting_receipt = _receipt(
        kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
        key,
        record_id,
        identity=identity)
    with admission_database.begin() as connection:
        provider_read_started_at = _postgres_now(connection)
        first = repository.observe_pod_waiting_in_connection(
            connection,
            identity,
            intent_idempotency_key=key,
            replica_id=1,
            replica_record_id=record_id,
            provider_cluster_generation=9,
            association_id=association_id,
            pod_namespace='skypilot',
            pod_name='worker-1',
            pod_uid='pod-uid-1',
            pod_receipt=waiting_receipt,
            provider_read_started_at=provider_read_started_at)
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                           match='expired while waiting'):
            repository.observe_pod_waiting_in_connection(
                connection,
                identity,
                intent_idempotency_key=key,
                replica_id=1,
                replica_record_id=record_id,
                provider_cluster_generation=9,
                association_id=association_id,
                pod_namespace='skypilot',
                pod_name='worker-1',
                pod_uid='pod-uid-1',
                pod_receipt=waiting_receipt,
                provider_read_started_at=(_postgres_now(connection) -
                                          datetime.timedelta(seconds=16)))
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                           match='later than PostgreSQL'):
            repository.observe_pod_waiting_in_connection(
                connection,
                identity,
                intent_idempotency_key=key,
                replica_id=1,
                replica_record_id=record_id,
                provider_cluster_generation=9,
                association_id=association_id,
                pod_namespace='skypilot',
                pod_name='worker-1',
                pod_uid='pod-uid-1',
                pod_receipt=waiting_receipt,
                provider_read_started_at=(_postgres_now(connection) +
                                          datetime.timedelta(seconds=1)))
        connection.execute(sqlalchemy.select(sqlalchemy.func.pg_sleep(0.01)))
        provider_read_started_at = _postgres_now(connection)
        renewed = repository.observe_pod_waiting_in_connection(
            connection,
            identity,
            intent_idempotency_key=key,
            replica_id=1,
            replica_record_id=record_id,
            provider_cluster_generation=9,
            association_id=association_id,
            pod_namespace='skypilot',
            pod_name='worker-1',
            pod_uid='pod-uid-1',
            pod_receipt=waiting_receipt,
            provider_read_started_at=provider_read_started_at)
        assert renewed.observed_at > first.observed_at
        assert renewed.valid_until - renewed.observed_at == (datetime.timedelta(
            seconds=15))
        assert renewed.pod_receipt_sha256 == _receipt_digest(
            connection, waiting_receipt)

        admitted_receipt = _receipt(
            kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
            key,
            record_id,
            identity=identity)
        provider_read_started_at = _postgres_now(connection)
        admitted = repository.observe_policy_admitted_in_connection(
            connection,
            identity,
            intent_idempotency_key=key,
            replica_id=1,
            replica_record_id=record_id,
            provider_cluster_generation=9,
            association_id=association_id,
            pod_namespace='skypilot',
            pod_name='worker-1',
            pod_uid='pod-uid-1',
            pod_receipt=admitted_receipt,
            provider_read_started_at=provider_read_started_at)
        assert admitted.state is (
            kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED)
        assert admitted.valid_until is None
        assert admitted.admitted_at is not None
        admitted_at = admitted.admitted_at
        refreshed_receipt = _receipt(
            kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
            key,
            record_id,
            identity=identity,
            phase='Running',
            workload_name='worker-1',
            unconstrained_topology='true')
        connection.execute(sqlalchemy.select(sqlalchemy.func.pg_sleep(0.01)))
        provider_read_started_at = _postgres_now(connection)
        refreshed = repository.observe_policy_admitted_in_connection(
            connection,
            identity,
            intent_idempotency_key=key,
            replica_id=1,
            replica_record_id=record_id,
            provider_cluster_generation=9,
            association_id=association_id,
            pod_namespace='skypilot',
            pod_name='worker-1',
            pod_uid='pod-uid-1',
            pod_receipt=refreshed_receipt,
            provider_read_started_at=provider_read_started_at)
        assert refreshed.admitted_at == admitted_at
        assert refreshed.observed_at > admitted.observed_at
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
            repository.observe_pod_waiting_in_connection(
                connection,
                identity,
                intent_idempotency_key=key,
                replica_id=1,
                replica_record_id=record_id,
                provider_cluster_generation=9,
                association_id=association_id,
                pod_namespace='skypilot',
                pod_name='worker-1',
                pod_uid='pod-uid-1',
                pod_receipt=waiting_receipt,
                provider_read_started_at=_postgres_now(connection))


def test_database_rejects_partial_shapes_removed_states_and_regression(
        admission_database) -> None:
    key = '4' * 64
    _insert_intent(admission_database, key)
    identity = _identity()
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, identity,
                                                       key)
    table = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with admission_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(table).where(
                    table.c.intent_idempotency_key == key).values(replica_id=1))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with admission_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(table).where(
                    table.c.intent_idempotency_key == key).values(
                        state='PAID_HANDOFF'))
    with pytest.raises(sqlalchemy.exc.IntegrityError,
                       match='identity is immutable'):
        with admission_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(table).where(
                    table.c.intent_idempotency_key == key).values(
                        service_version=_SERVICE_VERSION + 1))


def test_outgoing_update_holds_only_lower_unresolved_versions(
        admission_database) -> None:
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    pending_key = '1' * 64
    incoming_key = '2' * 64
    waiting_key = '3' * 64
    admitted_key = '4' * 64
    for ordinal, key, version in (
        (0, pending_key, _SERVICE_VERSION),
        (1, incoming_key, _SERVICE_VERSION + 1),
        (2, waiting_key, _SERVICE_VERSION),
        (3, admitted_key, _SERVICE_VERSION),
    ):
        _insert_intent(admission_database,
                       key,
                       ordinal=ordinal,
                       service_version=version,
                       observation_sequence=ordinal + 1,
                       ordinary_zero_cost_admission_sequence=ordinal + 1)
        with admission_database.begin() as connection:
            repository.insert_intent_pending_in_connection(
                connection, _identity(service_version=version), key)

    waiting_record, waiting_association = _materialize(admission_database,
                                                       repository,
                                                       _identity(),
                                                       waiting_key,
                                                       replica_id=1,
                                                       provider_generation=9)
    admitted_record, admitted_association = _materialize(admission_database,
                                                         repository,
                                                         _identity(),
                                                         admitted_key,
                                                         replica_id=2,
                                                         provider_generation=10)
    with admission_database.begin() as connection:
        repository.observe_pod_waiting_in_connection(
            connection,
            _identity(),
            intent_idempotency_key=waiting_key,
            replica_id=1,
            replica_record_id=waiting_record,
            provider_cluster_generation=9,
            association_id=waiting_association,
            provider_read_started_at=_postgres_now(connection),
            pod_namespace='skypilot',
            pod_name='worker-1',
            pod_uid='pod-uid-1',
            pod_receipt=_receipt(
                kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
                waiting_key,
                waiting_record,
                identity=_identity()))
        repository.observe_policy_admitted_in_connection(
            connection,
            _identity(),
            intent_idempotency_key=admitted_key,
            replica_id=2,
            replica_record_id=admitted_record,
            provider_cluster_generation=10,
            association_id=admitted_association,
            provider_read_started_at=_postgres_now(connection),
            pod_namespace='skypilot',
            pod_name='worker-1',
            pod_uid='pod-uid-1',
            pod_receipt=_receipt(
                kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED,
                admitted_key,
                admitted_record,
                identity=_identity()))

    with admission_database.begin() as connection:
        holds = repository.lock_outgoing_update_holds_in_connection(
            connection, _SERVICE, _SERVICE_HASH, _SERVICE_VERSION + 1)
        assert {row.intent_idempotency_key for row in holds
               } == {pending_key, waiting_key}


def test_expired_provider_free_terminal_cleanup_requires_locked_exact_graph(
        admission_database) -> None:
    predecessor_key = 'a' * 64
    successor_key = 'b' * 64
    now = datetime.datetime.now(datetime.timezone.utc)
    valid_until = now + datetime.timedelta(milliseconds=500)
    _insert_intent(admission_database,
                   predecessor_key,
                   created_at=now - datetime.timedelta(seconds=1),
                   updated_at=now - datetime.timedelta(seconds=1),
                   valid_until=valid_until,
                   valid_until_epoch=valid_until.timestamp())
    identity = _identity()
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(connection, identity,
                                                       predecessor_key)
    time.sleep(0.6)
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with admission_database.begin() as connection:
        terminal_at = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        connection.execute(
            sqlalchemy.update(intents).where(
                intents.c.intent_idempotency_key == predecessor_key).values(
                    state='TERMINAL',
                    terminal_at=terminal_at,
                    updated_at=terminal_at))

    replicas = serve_state_schema.replicas_table
    # Even a partial/corrupt provider path fails closed.  This row cannot be
    # reached through the normal writer, so disable only the pre-existing user
    # trigger while constructing the adversarial database state.
    with admission_database.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.insert(replicas).values(
                service_name=_SERVICE,
                replica_id=99,
                replica_state_version=18,
                replica_state={
                    'replica_record_id': str(uuid.uuid4()),
                    'reserved_fill': True,
                },
                status='PENDING',
                version=_SERVICE_VERSION,
                cluster_name=f'{_SERVICE}-99',
                is_spot=False,
                reserved_fill_intent_idempotency_key=predecessor_key))
        connection.exec_driver_sql(
            f'ALTER TABLE {replicas.name} ENABLE TRIGGER USER')
    with admission_database.begin() as connection:
        assert not repository.prelock_provider_free_terminal_admissions_in_connection(
            connection, _SERVICE, _SERVICE_HASH)
        assert repository.get_for_intent_in_connection(
            connection, _SERVICE, predecessor_key) is not None
    with admission_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(replicas).where(
                replicas.c.service_name == _SERVICE,
                replicas.c.replica_id == 99))

    # A proof is authority only inside the transaction that locked every
    # possible provider/request path.
    with admission_database.begin() as connection:
        stale_proof, = (
            repository.prelock_provider_free_terminal_admissions_in_connection(
                connection, _SERVICE, _SERVICE_HASH))
    with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                       match='another transaction'):
        with admission_database.begin() as connection:
            repository.delete_provider_free_terminal_admission_in_connection(
                connection, stale_proof)

    # Cleanup does not mint a privileged successor.  A fresh ordinary grant may
    # follow in the same sequenced transaction and obeys normal batch bounds.
    with admission_database.begin() as connection:
        proof, = (
            repository.prelock_provider_free_terminal_admissions_in_connection(
                connection, _SERVICE, _SERVICE_HASH))
        repository.lock_service_admissions_in_connection(
            connection, _SERVICE, _SERVICE_HASH)
        removed = (
            repository.delete_provider_free_terminal_admission_in_connection(
                connection, proof))
        assert removed.intent_idempotency_key == predecessor_key
        connection.execute(
            sqlalchemy.insert(intents).values(
                **_intent_values(successor_key, ordinal=1)))
        successor = repository.insert_intent_pending_in_connection(
            connection, identity, successor_key)
        assert successor.intent_idempotency_key == successor_key
        assert successor.state is (
            kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING)
        assert repository.get_for_intent_in_connection(connection, _SERVICE,
                                                       predecessor_key) is None
        retained_terminal = connection.execute(
            sqlalchemy.select(
                intents.c.state).where(intents.c.intent_idempotency_key ==
                                       predecessor_key)).scalar_one()
        assert retained_terminal == 'TERMINAL'


@pytest.mark.parametrize(
    ('capacity_unit', 'accelerator_count', 'planned_capacity', 'surge_units'), (
        ('physical', 1, 1, 1),
        ('logical', 8, 8, 6),
    ))
def test_surge_is_frozen_in_configured_unit_and_clears_for_any_clean_reduction(
        admission_database, capacity_unit, accelerator_count, planned_capacity,
        surge_units) -> None:
    key = '5' * 64
    _insert_intent(admission_database,
                   key,
                   accelerator_count=accelerator_count,
                   capacity_unit=capacity_unit,
                   planned_capacity=planned_capacity)
    identity = _identity(accelerator_count=accelerator_count)
    compatibility = '9' * 64
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(
            connection,
            identity,
            key,
            replacement_surge_units=surge_units,
            replacement_compatibility_sha256=compatibility)
        validated = repository.validate_replacement_surge_in_connection(
            connection,
            identity,
            intent_idempotency_key=key,
            expected_compatibility_sha256=compatibility)
        assert validated.capacity_unit == capacity_unit
        assert validated.replacement_surge_units == surge_units
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict):
            repository.release_satisfied_replacement_surge_in_connection(
                connection,
                service_name=_SERVICE,
                service_hash=_SERVICE_HASH,
                capacity_unit=('logical'
                               if capacity_unit == 'physical' else 'physical'),
                physical_capacity_debit=planned_capacity,
                max_capacity=planned_capacity)
        released = repository.release_satisfied_replacement_surge_in_connection(
            connection,
            service_name=_SERVICE,
            service_hash=_SERVICE_HASH,
            capacity_unit=capacity_unit,
            physical_capacity_debit=planned_capacity - 1,
            max_capacity=planned_capacity)
        assert released is not None
        assert released.replacement_surge_units == 0
        assert released.replacement_compatibility_sha256 is None


def test_cross_domain_waiting_surge_serializes_the_empty_gap_and_blocks_chain(
        admission_database) -> None:
    first_key = '6' * 64
    second_key = '7' * 64
    _insert_intent(admission_database, first_key)
    _insert_intent(admission_database,
                   second_key,
                   ordinal=1,
                   physical_cluster_uid='cluster-uid-b')
    first_identity = _identity()
    second_identity = _identity(physical_cluster_uid='cluster-uid-b')
    compatibility = '8' * 64
    repository = kueue_lane_lineage.KueueAdmissionRepository(admission_database)
    with admission_database.begin() as connection:
        repository.insert_intent_pending_in_connection(
            connection,
            first_identity,
            first_key,
            replacement_surge_units=1,
            replacement_compatibility_sha256=compatibility)
    record_id, association_id = _materialize(admission_database, repository,
                                             first_identity, first_key)
    waiting = _receipt(kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
                       first_key,
                       record_id,
                       identity=first_identity)
    with admission_database.begin() as connection:
        provider_read_started_at = _postgres_now(connection)
        row = repository.observe_pod_waiting_in_connection(
            connection,
            first_identity,
            intent_idempotency_key=first_key,
            replica_id=1,
            replica_record_id=record_id,
            provider_cluster_generation=9,
            association_id=association_id,
            pod_namespace='skypilot',
            pod_name='worker-1',
            pod_uid='pod-uid-1',
            pod_receipt=waiting,
            provider_read_started_at=provider_read_started_at)
        assert row.replacement_surge_units == 1

    locked = threading.Event()
    release = threading.Event()

    def hold_service_gap() -> None:
        with admission_database.begin() as connection:
            rows = repository.lock_service_admissions_in_connection(
                connection, _SERVICE, _SERVICE_HASH)
            assert rows[0].state is (
                kueue_lane_lineage.KueueAdmissionState.POD_WAITING)
            locked.set()
            assert release.wait(timeout=5)

    def try_cross_domain_chain() -> None:
        assert locked.wait(timeout=5)
        with admission_database.begin() as connection:
            repository.lock_service_admissions_in_connection(
                connection, _SERVICE, _SERVICE_HASH)
            repository.insert_intent_pending_in_connection(
                connection,
                second_identity,
                second_key,
                replacement_surge_units=1,
                replacement_compatibility_sha256=compatibility)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        holder = executor.submit(hold_service_gap)
        assert locked.wait(timeout=5)
        contender = executor.submit(try_cross_domain_chain)
        time.sleep(0.15)
        assert not contender.done()
        release.set()
        holder.result(timeout=5)
        with pytest.raises(kueue_lane_lineage.KueueAdmissionConflict,
                           match='one replacement surge'):
            contender.result(timeout=5)
