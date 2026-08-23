"""PostgreSQL contracts for ordered SkyServe capacity admission."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import dataclasses
import datetime
import pickle
import threading
import time
import types
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
from test_kueue_lane_lineage_pg import _intent_values as _kueue_intent_values
from test_kueue_lane_lineage_pg import _receipt as _kueue_receipt
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import capacity_admission
from sky.serve import capacity_admission_schema
from sky.serve import constants
from sky.serve import demand_state
from sky.serve import kubernetes_identity
from sky.serve import kueue_lane_lineage
from sky.serve import kueue_lane_lineage_schema
from sky.serve import ordinary_launch_binding
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import service_spec
from sky.serve import zero_cost_actuation_schema
from sky.utils import common_utils
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_capacity_admission_schema_052_pg')

_URL = 'http://replica:8000'
_CAPACITY_KUEUE_PROJECTION = {
    'projection_version':
        kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION,
    'candidate_id': 'kubernetes-0000',
    'kubernetes_context': 'phx',
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
    'accelerator_name': 'L4',
    'accelerator_count': 1,
    'accelerator_scheduling': {
        'label_key': 'nvidia.com/gpu.product',
        'label_values': ['NVIDIA-L4'],
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
_CAPACITY_KUEUE_PROJECTION_SHA256 = (
    kubernetes_identity.worker_projection_sha256(_CAPACITY_KUEUE_PROJECTION))
_CAPACITY_EAST_PROJECTION = {
    **_CAPACITY_KUEUE_PROJECTION,
    'candidate_id': 'kubernetes-0001',
    'kubernetes_context': 'east',
    'service_account_name': 'skyserve-worker',
    'scheduler_name': 'default-scheduler',
    'priority_class_name': 'skyserve-preemptible',
    'kueue_admission': None,
}
_CAPACITY_EAST_PROJECTION_SHA256 = (
    kubernetes_identity.worker_projection_sha256(_CAPACITY_EAST_PROJECTION))


def _capacity_service_spec(
    reserved_fill_enabled: bool,
    *,
    max_replicas: int = 10,
    replica_unit: str = 'physical_backend',
) -> service_spec.SkyServiceSpec:
    assert replica_unit in ('physical_backend', 'logical')
    return service_spec.SkyServiceSpec(
        readiness_path='/health',
        initial_delay_seconds=0,
        readiness_timeout_seconds=5,
        endpoint_probe_interval_seconds=1,
        lb_stream_timeout_seconds=10,
        min_replicas=0,
        max_replicas=max_replicas,
        target_concurrency_per_replica=1,
        spot_placer=('dynamic_fallback_per_gpu'
                     if replica_unit == 'logical' else None),
        graceful_drain_async_occupancy=(True
                                        if replica_unit == 'logical' else None),
        lb_high_availability=False,
        reserved_capacity_fill=reserved_fill_enabled)


_COPIED_ADMISSION_MUTATIONS = (
    ('intent_idempotency_key', '0' * 64),
    ('unresolved_domain_sha256', '0' * 64),
    ('service_name', 'other-service'),
    ('service_hash', 'other-incarnation'),
    ('service_lifecycle_epoch', 4),
    ('service_version', 2),
    ('pool_key', 'other-pool'),
    ('pool_epoch', 8),
    ('physical_cluster_uid', 'other-cluster'),
    ('kubernetes_context', 'other-context'),
    ('accelerator', 'h200'),
    ('accelerator_count', 2),
    ('worker_projection_sha256', '0' * 64),
    ('capacity_unit', 'logical'),
    ('planned_capacity', 2),
)


def _route_response():
    return {
        'replica_info': {
            _URL: {
                'gpu_type': 'L4',
                'gpu_count': '1',
            }
        },
        'num_ready_replicas': 1,
        'routing_spec': {
            'load_balancing_policy_name': 'round_robin',
        },
        'capacity_hint': {
            'replica_unit': 'physical_backend',
        },
        'request_history_accepted': False,
        'request_classification_history_accepted': False,
        'response_time_history_accepted': False,
        'prediction_time_history_accepted': False,
        'queued_compatibility_demand_supported': True,
        'service_version': 1,
    }


def _route_identities(record_id: str):
    return {
        _URL: {
            'replica_id': 1,
            'replica_record_id': record_id,
            'service_version': 1,
            'gpu_type': 'L4',
            'gpu_count': 1,
            'advertised': True,
            'alias_expires_at': None,
        }
    }


def _demand_report(now: float,
                   route_receipt: route_projection.RoutePublicationReceipt,
                   *,
                   sequence: int = 1,
                   request_count: int = 1,
                   occupancy_sample_age_seconds: float = 0.1) -> dict:
    bucket_seconds = constants.LB_DEMAND_WINDOW_BUCKET_SECONDS
    profiles = ([{
        'priority': 50,
        'compatible_accelerators': ['L4'],
        'count': request_count,
    }] if request_count else [])
    return {
        'protocol_version': 2,
        'sequence': sequence,
        'reporter_session_id': 'process-a',
        'reporter_observed_at': now,
        'lb_session_id': 'pod-a',
        'lb_slot': 'a',
        'routing_version': 1,
        'armed_generation': None,
        'applied_role': 'ACTIVE',
        'applied_generation': 1,
        'local_in_flight': request_count,
        'http_in_flight': {
            _URL: request_count,
        },
        'async_occupancy': {
            _URL: 0,
        },
        'occupancy_sample_generation': {
            _URL: sequence,
        },
        'occupancy_sample_age_seconds': {
            _URL: occupancy_sample_age_seconds,
        },
        'occupancy_sampled_urls': [_URL],
        'total_slots_by_url': {
            _URL: 1,
        },
        'routing_urls': [_URL],
        'unknown_in_flight_urls': [],
        'draining_urls': [],
        'demand_window': {
            'bucket_seconds': bucket_seconds,
            'window_seconds': constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS,
            'coverage_started_at':
                (now - constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS),
            'buckets': [{
                'bucket_start': int(now // bucket_seconds) * bucket_seconds,
                'request_count': request_count,
                'compatibility_profiles': profiles,
            }],
            'compatibility_complete': True,
            'saturated': False,
        },
        'request_history': None,
        'request_classification_history': None,
        'prediction_time_history': None,
        'configured_accelerators': ['L4'],
        'request_accelerator_compatibility_version': 1,
        'route_projection_generation': route_receipt.generation,
        'route_projection_sha256': route_receipt.content_sha256,
        'route_source_epoch': 1,
        'queue_depth': 0,
        'queued_requests_by_compatibility': [],
        'rejected_requests_by_compatibility': [],
        'queue_depth_by_priority': {},
        'rejected_in_window': 0,
        'rejected_in_recent_window': 0,
        'rejected_in_window_by_priority': {},
        'rejected_in_recent_window_by_priority': {},
        'unique_job_arrivals_60s': request_count,
        'unique_job_arrivals_300s': request_count,
        'headerless_arrivals_60s': 0,
        'headerless_arrivals_300s': 0,
        'offered_arrival_tracking_saturated': False,
    }


@pytest.fixture
def capacity_database(empty_postgres, monkeypatch):
    serve_config = migration_utils.get_alembic_config(
        empty_postgres, migration_utils.SERVE_DB_NAME)
    # Capacity admission uses the current service metadata and atomically
    # accounts for grant-before-row intents. Keep the behavioral fixture at
    # the current additive head; revision-specific migration tests build their
    # historical schemas separately below.
    alembic_command.upgrade(serve_config, migration_utils.SERVE_VERSION)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        empty_postgres)
    incarnation = uuid.uuid4()
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name='svc', epoch=3))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='svc',
                workspace='workspace-a',
                status='READY',
                hash='svc-hash',
                current_version=1,
                active_versions='[1]',
                pool=0,
                lifecycle_epoch=3,
                controller_incarnation=incarnation,
                controller_owner_epoch=4,
                controller_pid=123,
                controller_ip='10.0.0.5',
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=2))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name='svc',
                version=1,
                spec=pickle.dumps(_capacity_service_spec(False), protocol=4),
                yaml_content='service: {}\n'))
        ordinary_launch_binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            controller_owner_epoch=4,
            expected_binding_epoch=2,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True)
    route_repository = route_projection.RouteProjectionRepository(
        empty_postgres)
    identity = route_projection.RoutePublisherIdentity(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        controller_incarnation=incarnation,
        controller_owner_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.5')
    record_id = str(uuid.uuid4())
    route_receipt = route_repository.publish(identity,
                                             1,
                                             _route_response(),
                                             _route_identities(record_id),
                                             {record_id},
                                             ttl_seconds=60)
    # Characterize a retained projected-protocol-1 cohort after Serve051.
    # New promotion selects protocol 2; an already-promoted service retains
    # its exact writer until the explicit migration gate.
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    route_source_mode='DURABLE_PROJECTED',
                    route_source_epoch=1))
    demand_state.ingest_report('svc', 'svc-hash',
                               _demand_report(time.time(), route_receipt))
    with empty_postgres.begin() as connection:
        epoch = capacity_admission.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            participant_barrier_passed=lambda _connection: True)
    assert epoch == 1
    return empty_postgres, incarnation, route_receipt


def _plan(
    demand_target: int,
    *,
    normalized_demand: dict | None = None,
    capacity_target_by_accelerator: dict[str, int] | None = None,
    reserved_fill_authority: (capacity_admission.ReservedFillPlanAuthority |
                              None) = None,
    allocation_reserved_capacity_by_accelerator: dict[str, int] | None = None,
    expected_pending_zero_cost_capacity_by_accelerator: dict[str, int] |
    None = None,
    expected_economic_capacity_graph_sha256: str | None = None,
) -> capacity_admission.CapacityPlanInput:
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    return capacity_admission.CapacityPlanInput(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        demand_source_epoch=snapshot.demand_source_epoch,
        demand_feed_generation=snapshot.demand_feed_generation,
        receipt_watermark=snapshot.receipt_watermark,
        route_generation=snapshot.route_generation,
        route_sha256=snapshot.route_sha256,
        route_source_epoch=snapshot.route_source_epoch,
        normalized_demand=(snapshot.normalized_demand
                           if normalized_demand is None else normalized_demand),
        capacity_target_by_accelerator=({
            'l4': demand_target
        } if capacity_target_by_accelerator is None else
                                        capacity_target_by_accelerator),
        reserved_fill_authority=(
            capacity_admission.ReservedFillPlanAuthority.not_applicable()
            if reserved_fill_authority is None else reserved_fill_authority),
        allocation_reserved_capacity_by_accelerator=(
            {} if allocation_reserved_capacity_by_accelerator is None else
            allocation_reserved_capacity_by_accelerator),
        expected_pending_zero_cost_capacity_by_accelerator=(
            {} if expected_pending_zero_cost_capacity_by_accelerator is None
            else expected_pending_zero_cost_capacity_by_accelerator),
        expected_economic_capacity_graph_sha256=(
            expected_economic_capacity_graph_sha256))


def _replica_values(replica_id: int,
                    *,
                    zero_cost: bool,
                    accelerator: str = 'L4') -> dict:
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'svc-{replica_id}',
        replica_port='8000',
        is_spot=not zero_cost,
        location=None,
        version=1,
        resources_override={'accelerators': {
            accelerator: 1,
        }})
    info.is_zero_cost = zero_cost
    if not zero_cost:
        info.paid_capacity_pool_key = 'gcp:L4'
    return {
        'service_name': 'svc',
        'replica_id': replica_id,
        'replica_state_version': 1,
        'status': info.status.value,
        'version': 1,
        'cluster_name': f'svc-{replica_id}',
        'created_at': info.created_at,
        'is_spot': not zero_cost,
        'paid_capacity_pool_key': info.paid_capacity_pool_key,
        'replica_state': info.to_storage_dict(),
    }


def _install_waiting_kueue_capacity(engine, *, admitted: bool = False) -> str:
    """Install one exact waiting or policy-admitted Kueue L4 graph."""
    key = '9' * 64
    identity = kueue_lane_lineage.KueueAdmissionIdentity(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        pool_key=reserved_capacity_broker.make_pool_key(
            'phx',
            'l4',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='cluster-phx'),
        pool_epoch=7,
        physical_cluster_uid='cluster-phx',
        kubernetes_context='phx',
        accelerator='l4',
        accelerator_count=1,
        worker_projection_sha256=_CAPACITY_KUEUE_PROJECTION_SHA256)
    intent_values = _kueue_intent_values(key)
    intent_values.update(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        pool_key=identity.pool_key,
        pool_epoch=identity.pool_epoch,
        physical_cluster_uid=identity.physical_cluster_uid,
        kubernetes_context=identity.kubernetes_context,
        worker_projection_sha256=identity.worker_projection_sha256,
        accelerator='l4',
        accelerator_count=1,
        allowed_locations=[{
            'cloud': 'Kubernetes',
            'region': 'phx',
            'zone': None,
            'accelerators': {
                'l4': 1,
            },
            'use_spot': False,
            'image_id': None,
            'container_image': None,
            'disk_tier': None,
            'ephemeral_storage': None,
            'instance_type': None,
        }])
    repository = kueue_lane_lineage.KueueAdmissionRepository(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    yaml_content='service: {}',
                    placement_catalog={
                        'schema_version': 1,
                        'entries': [],
                        'num_nodes': 1,
                    },
                    worker_placement_projections=[_CAPACITY_KUEUE_PROJECTION]))
        connection.execute(
            sqlalchemy.insert(zero_cost_actuation_schema.
                              serve_zero_cost_actuation_intents_table).values(
                                  **intent_values))
        repository.insert_intent_pending_in_connection(connection, identity,
                                                       key)

    record_id = uuid.uuid4()
    association_id = uuid.uuid4()
    replica_id = 90
    provider_generation = 9
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    intents = (
        zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table)
    replicas = serve_state_schema.replicas_table
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference=f'reserved-fill:{key}',
        authorization_generation=1,
        authorization_payload={'intent_idempotency_key': key})
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc')).mappings().one()
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        for table in (intents.name, replicas.name, associations.name):
            connection.exec_driver_sql(
                f'ALTER TABLE {table} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.insert(associations).values(
                association_id=association_id,
                submission_id=uuid.uuid4(),
                tenant_scope='tenant-a',
                service_name='svc',
                service_hash='svc-hash',
                service_workspace='workspace-a',
                service_lifecycle_epoch=3,
                service_binding_epoch=service['ordinary_launch_binding_epoch'],
                service_version=1,
                replica_id=replica_id,
                replica_record_id=record_id,
                launch_generation=provider_generation,
                cluster_name='svc-90',
                request_id=f'request-{uuid.uuid4()}',
                input_digest='a' * 64,
                owner_controller_incarnation=service['controller_incarnation'],
                owner_controller_epoch=service['controller_owner_epoch'],
                effect_phase='NOT_STARTED',
                effect_phase_changed_at=now,
                resolution='BOUND',
                created_at=now,
                updated_at=now,
                binding_protocol_version=2,
                profile_kind='RESERVED_FILL',
                profile_version=1,
                profile_digest=profile.digest,
                capability_cohort_epoch=1,
                capability_profile_set_digest='c' * 64,
                receipt_protocol_version=1,
                authorization_kind=profile.authorization_kind.value,
                authorization_reference=profile.authorization_reference,
                authorization_generation=profile.authorization_generation,
                authorization_digest=profile.authorization_digest,
                reconciliation_outcome='ACTIVE_ADOPT',
                provider_evidence='NOT_QUERIED'))
        connection.execute(
            sqlalchemy.update(intents).where(
                intents.c.intent_idempotency_key == key).values(
                    state='COMMITTED',
                    replica_id=replica_id,
                    replica_record_id=record_id,
                    committed_at=now,
                    updated_at=now))
        replica_values = _replica_values(replica_id,
                                         zero_cost=True,
                                         accelerator='L4')
        replica_values['replica_state']['replica_record_id'] = str(record_id)
        replica_values.update(ordinary_launch_association_id=association_id,
                              reserved_fill_intent_idempotency_key=key)
        connection.execute(sqlalchemy.insert(replicas).values(**replica_values))
        for table in (intents.name, replicas.name, associations.name):
            connection.exec_driver_sql(
                f'ALTER TABLE {table} ENABLE TRIGGER USER')
        repository.bind_materialized_in_connection(
            connection,
            identity,
            intent_idempotency_key=key,
            replica_id=replica_id,
            replica_record_id=record_id,
            provider_cluster_generation=provider_generation,
            association_id=association_id)
        state = (kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED
                 if admitted else
                 kueue_lane_lineage.KueueAdmissionState.POD_WAITING)
        receipt = _kueue_receipt(state, key, record_id, identity=identity)
        observe = (repository.observe_policy_admitted_in_connection if admitted
                   else repository.observe_pod_waiting_in_connection)
        observe(connection,
                identity,
                intent_idempotency_key=key,
                replica_id=replica_id,
                replica_record_id=record_id,
                provider_cluster_generation=provider_generation,
                association_id=association_id,
                pod_namespace='skypilot',
                pod_name='worker-1',
                pod_uid='pod-uid-1',
                pod_receipt=receipt,
                provider_read_started_at=now)
    return key


def _install_pending_east_capacity(engine) -> str:
    """Install one immutable ordinary-scheduler intent without admission."""
    key = '8' * 64
    intent_values = _kueue_intent_values(key)
    intent_values.update(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        pool_key=reserved_capacity_broker.make_pool_key(
            'east',
            'l4',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='cluster-east'),
        pool_epoch=7,
        physical_cluster_uid='cluster-east',
        kubernetes_context='east',
        worker_projection_sha256=_CAPACITY_EAST_PROJECTION_SHA256,
        accelerator='l4',
        accelerator_count=1,
        allowed_locations=[{
            'cloud': 'Kubernetes',
            'region': 'east',
            'zone': None,
            'accelerators': {
                'l4': 1,
            },
            'use_spot': False,
            'image_id': None,
            'container_image': None,
            'disk_tier': None,
            'ephemeral_storage': None,
            'instance_type': None,
        }])
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    yaml_content='service: {}',
                    placement_catalog={
                        'schema_version': 1,
                        'entries': [],
                        'num_nodes': 1,
                    },
                    worker_placement_projections=[_CAPACITY_EAST_PROJECTION]))
        connection.execute(
            sqlalchemy.insert(zero_cost_actuation_schema.
                              serve_zero_cost_actuation_intents_table).values(
                                  **intent_values))
    return key


def _insert_claim(engine, authority, replica_id: int) -> dict:
    claim = authority.claim_values('L4')
    claims = serve_state_schema.paid_capacity_claims_table
    pools = serve_state_schema.paid_capacity_pools_table
    with engine.begin() as connection:
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(
            connection, {
                **service, 'name': 'svc'
            }, {
                **claim,
                'replica_id': replica_id,
            },
            prospective=True,
            protocol_and_service_prelocked=True)
        connection.execute(
            postgresql.insert(pools).values(
                pool_key='gcp:L4',
                current_limit=10,
                successes_since_resize=0,
                updated_at=time.time()).on_conflict_do_nothing())
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **_replica_values(replica_id, zero_cost=False)))
        connection.execute(
            sqlalchemy.insert(claims).values(service_name='svc',
                                             service_hash='svc-hash',
                                             replica_id=replica_id,
                                             pool_key='gcp:L4',
                                             priority=50,
                                             claimed_at=time.time(),
                                             **claim))
    return claim


def _route_record_id(engine) -> str:
    with engine.connect() as connection:
        identity_payload = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_snapshots_table.c.
                identity_payload).where(
                    route_projection_schema.serve_route_snapshots_table.c.
                    service_name == 'svc')).scalar_one()
    return str(identity_payload[_URL]['replica_record_id'])


def _prepare_logical_retirement(capacity_database):
    engine, _, route_receipt = capacity_database
    info = replica_managers.ReplicaInfo(
        replica_id=1,
        cluster_name='svc-1',
        replica_port='8000',
        is_spot=True,
        location=None,
        version=1,
        resources_override={'accelerators': {
            'L4': 1,
        }})
    info.replica_record_id = _route_record_id(engine)
    status = info.status_property
    status.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    status.service_ready_now = True
    status.first_ready_time = time.time()
    status.is_scale_down = True
    status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
    status.wait_for_idle_before_termination = False
    status.logical_retirement_version = 1
    status.logical_retirement_controller_epoch = 'logical-controller-a'
    status.logical_retirement_generation = 1
    status.logical_retirement_target_capacity = 0
    status.logical_retirement_confirmed_generation = None
    status.logical_retirement_bounded_deadline = False
    status.logical_retirement_committed = False
    assert serve_state.add_or_update_replica('svc', 1, info)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=0))
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    assert snapshot.demand_feed_generation == 2
    return info, snapshot.reconcile_authority, route_receipt


def _commit_logical(info, authority, allocation_identity=None):
    return serve_state.commit_logical_retirement(
        'svc',
        1,
        info,
        authority,
        expected_service_hash='svc-hash',
        expected_controller_owner=(123, '10.0.0.5'),
        expected_logical_controller_epoch='logical-controller-a',
        expected_reserved_fill_allocation_identity=allocation_identity)


def _allocation_identity(
    generation: int = 1
) -> reserved_fill_planner.ReservedFillAllocationIdentity:
    return reserved_fill_planner.ReservedFillAllocationIdentity(
        allocation_generation=generation,
        allocation_input_sha256=f'{generation:064x}',
        allocation_claim_generation=11,
        service_version=1,
        ordinary_zero_cost_admission_sequence_high_water=0,
        reconciliation_gate_generation=1,
        reclaim_fleet_bundle_sha256='a' * 64,
        reclaim_policy_revision='test-policy',
        reclaim_provider_inventory_sha256='b' * 64)


def _enable_durable_intent(engine,
                           incarnation,
                           *,
                           reserved_fill_enabled: bool = True,
                           max_replicas: int = 10,
                           replica_unit: str = 'physical_backend') -> None:
    with engine.begin() as connection:
        result = connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    reserved_fill_actuation_mode='DURABLE_INTENT',
                    reserved_fill_actuation_epoch=1,
                    reserved_fill_actuation_capable=True,
                    reserved_fill_actuation_controller_incarnation=incarnation,
                    reserved_fill_actuation_protocol_version=1))
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    spec=pickle.dumps(_capacity_service_spec(
                        reserved_fill_enabled,
                        max_replicas=max_replicas,
                        replica_unit=replica_unit),
                                      protocol=4)))
    assert result.rowcount == 1


def _validate_prospective_claim(engine, claim) -> None:
    with engine.begin() as connection:
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(
            connection,
            service,
            claim,
            prospective=True,
            protocol_and_service_prelocked=True)


def _mock_current_allocation(monkeypatch,
                             identity,
                             *,
                             callback=None,
                             pool_snapshots=()) -> None:

    def _read_current(_repository,
                      connection,
                      service_name,
                      expected_service_hash,
                      expected_controller_owner,
                      *,
                      protocol_and_service_prelocked=False):
        assert connection.in_transaction()
        assert service_name == 'svc'
        assert expected_service_hash == 'svc-hash'
        assert expected_controller_owner == (123, '10.0.0.5')
        assert protocol_and_service_prelocked is True
        if callback is not None:
            callback()
        current_identity = identity() if callable(identity) else identity
        if isinstance(current_identity,
                      reserved_fill_planner.AuthenticatedAllocationMap):
            return current_identity
        return types.SimpleNamespace(identity=current_identity,
                                     pool_snapshots=pool_snapshots)

    monkeypatch.setattr(
        serve_state.reserved_fill_allocation.ReservedFillAllocationRepository,
        'read_current_in_connection', _read_current)


def _allocation_map(
    free_by_accelerator: dict[str, int],
    *,
    accelerator_count: int = 1,
    valid_until: float | None = None,
) -> reserved_fill_planner.AuthenticatedAllocationMap:
    """Build one exact fresh allocation for paid-admission contracts."""
    cards = tuple(card.casefold() for card in free_by_accelerator)
    pool_key = reserved_capacity_broker.make_pool_key(
        'east',
        cards,
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='cluster-east')
    free_slots = sum(free_by_accelerator.values())
    snapshot = reserved_fill_planner.PoolFillSnapshot.from_mapping({
        'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
        'pool_key': pool_key,
        'physical_cluster_uid': 'cluster-east',
        'service_generation': 7,
        'worker_projection_sha256_by_accelerator': {
            card: f'{index + 1:064x}' for index, card in enumerate(cards)
        },
        'edge_cap': free_slots,
        'broker_slot_width': accelerator_count,
        'free_slots': free_slots,
        'free_slots_by_accelerator': free_by_accelerator,
        'grant': free_slots,
        'grant_epoch': 11 if free_slots else None,
        'observation_generation': 13,
        'observation_sequence': 17,
        'ordinary_zero_cost_admission_sequence': 17,
        'valid_until':
            (time.time() + 60 if valid_until is None else valid_until),
        'zero_cost_location_keys': [{
            'cloud': 'Kubernetes',
            'region': 'east',
            'zone': None,
            'accelerators': {
                card: accelerator_count,
            },
            'use_spot': False,
            'image_id': None,
            'container_image': None,
            'disk_tier': None,
            'ephemeral_storage': None,
            'instance_type': None,
        } for card in cards],
    })
    return reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=1,
        ordinary_zero_cost_admission_sequence_high_water=17,
        reconciliation_gate_generation=19,
        reclaim_fleet_bundle_sha256='a' * 64,
        reclaim_policy_revision='test-policy',
        reclaim_provider_inventory_sha256='b' * 64,
        pool_snapshots=(snapshot,))


def _insert_current_allocation_pending(
    engine,
    allocation: reserved_fill_planner.AuthenticatedAllocationMap,
    *,
    intent_key: str = 'e' * 64,
) -> str:
    """Insert one provider-free East intent from the exact allocation."""
    snapshot = allocation.pool_snapshots[0]
    card, free_slots = snapshot.free_slots_by_accelerator[0]
    assert free_slots > 0
    location = next(location for location in snapshot.locations
                    if location.accelerator.casefold() == card)
    projection_sha256 = dict(
        snapshot.worker_projection_sha256_by_accelerator)[card]
    valid_until = datetime.datetime.fromtimestamp(snapshot.valid_until,
                                                  datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)
    values = _kueue_intent_values(intent_key)
    values.update(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        actuation_epoch=1,
        service_version=1,
        controller_owner='controller-owner',
        allocation_generation=allocation.allocation_generation,
        allocation_input_sha256=allocation.allocation_input_sha256,
        allocation_claim_generation=allocation.allocation_claim_generation,
        reconciliation_gate_generation=(
            allocation.reconciliation_gate_generation),
        reclaim_fleet_bundle_sha256=allocation.reclaim_fleet_bundle_sha256,
        reclaim_policy_revision=allocation.reclaim_policy_revision,
        reclaim_provider_inventory_sha256=(
            allocation.reclaim_provider_inventory_sha256),
        service_generation=snapshot.service_generation,
        pool_key=snapshot.pool_key,
        pool_epoch=snapshot.grant_epoch,
        physical_cluster_uid=snapshot.physical_cluster_uid,
        kubernetes_context=location.region,
        worker_projection_sha256=projection_sha256,
        observation_generation=snapshot.observation_generation,
        observation_sequence=snapshot.observation_sequence,
        ordinary_zero_cost_admission_sequence=(
            snapshot.ordinary_zero_cost_admission_sequence),
        valid_until_epoch=snapshot.valid_until,
        valid_until=valid_until,
        accelerator=card,
        accelerator_count=location.accelerator_count,
        capacity_unit='physical',
        planned_capacity=1,
        allowed_locations=[location.to_pickleable()],
        state='GRANTED',
        created_at=now,
        updated_at=now)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table).values(**values))
    return intent_key


def _materialize_current_allocation_pending(engine, intent_key: str,
                                            replica_id: int) -> None:
    """Move one test intent atomically to a provider-possible replica row."""
    intents = (
        zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table)
    replicas = serve_state_schema.replicas_table
    record_id = uuid.uuid4()
    with engine.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        for table in (intents.name, replicas.name):
            connection.exec_driver_sql(
                f'ALTER TABLE {table} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.update(intents).where(
                intents.c.intent_idempotency_key == intent_key).values(
                    state='COMMITTED',
                    replica_id=replica_id,
                    replica_record_id=record_id,
                    committed_at=now,
                    updated_at=now))
        replica = _replica_values(replica_id,
                                  zero_cost=True,
                                  accelerator='H200')
        replica['replica_state']['replica_record_id'] = str(record_id)
        replica['reserved_fill_intent_idempotency_key'] = intent_key
        connection.execute(sqlalchemy.insert(replicas).values(**replica))
        for table in (intents.name, replicas.name):
            connection.exec_driver_sql(
                f'ALTER TABLE {table} ENABLE TRIGGER USER')


def _allocation_bound_plan(
    repository: capacity_admission.CapacityAdmissionRepository,
    allocation: (reserved_fill_planner.AuthenticatedAllocationMap |
                 reserved_fill_planner.ReservedFillAllocationIdentity),
    capacity_target_by_accelerator: dict[str, int],
) -> tuple[capacity_admission.CapacityPlanInput,
           capacity_admission.ReservedSupplyProjection]:
    """Project, then construct the optimistic plan compared at publication."""
    identity = (allocation.identity if isinstance(
        allocation, reserved_fill_planner.AuthenticatedAllocationMap) else
                allocation)
    projection = repository.project_reserved_supply(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards=capacity_target_by_accelerator,
        authority=capacity_admission.ReservedFillPlanAuthority.bound(identity))
    plan = _plan(
        sum(capacity_target_by_accelerator.values()),
        capacity_target_by_accelerator=capacity_target_by_accelerator,
        reserved_fill_authority=(
            capacity_admission.ReservedFillPlanAuthority.bound(identity)),
        allocation_reserved_capacity_by_accelerator=dict(
            projection.allocation_reserved_capacity_by_accelerator),
        expected_pending_zero_cost_capacity_by_accelerator=dict(
            projection.pending_zero_cost_capacity_by_accelerator),
        expected_economic_capacity_graph_sha256=(
            projection.economic_capacity_graph_sha256))
    return plan, projection


def _capacity_plan_payload(engine, generation: int) -> dict:
    with engine.connect() as connection:
        payload = connection.execute(
            sqlalchemy.select(
                capacity_admission_schema.serve_capacity_plans_table.c.payload).
            where(
                capacity_admission_schema.serve_capacity_plans_table.c.
                service_name == 'svc',
                capacity_admission_schema.serve_capacity_plans_table.c.
                generation == generation)).scalar_one()
    return dict(payload)


def test_serve050_schema_and_promotion_are_explicit(capacity_database):
    engine, incarnation, _ = capacity_database
    inspector = sqlalchemy.inspect(engine)
    assert inspector.has_table(
        capacity_admission_schema.serve_capacity_plans_table.name)
    assert inspector.has_table(
        capacity_admission_schema.serve_capacity_plan_heads_table.name)
    claim_foreign_keys = inspector.get_foreign_keys('paid_capacity_claims')
    assert any(foreign_key['referred_table'] == 'serve_capacity_plans' and
               foreign_key['constrained_columns'] ==
               ['service_name', 'capacity_plan_generation'] and
               (foreign_key.get('options') or {}).get('ondelete') == 'CASCADE'
               for foreign_key in claim_foreign_keys)
    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.demand_source_mode,
                serve_state_schema.services_table.c.demand_source_epoch,
                serve_state_schema.services_table.c.
                demand_authority_controller_incarnation)).one()
    assert service == ('DURABLE_FEED', 1, incarnation)


@pytest.mark.parametrize('target', [0, 1])
def test_fill_disabled_durable_service_uses_not_applicable(
        capacity_database, target):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)

    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(target))

    assert (authority.reserved_fill_authority.mode
            is capacity_admission.ReservedFillPlanAuthorityMode.NOT_APPLICABLE)


def test_fill_enabled_direct_plan_fails_after_durable_activation(
        capacity_database):
    engine, incarnation, _ = capacity_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).
            values(spec=pickle.dumps(_capacity_service_spec(True), protocol=4)))
    direct_authority = (
        capacity_admission.CapacityAdmissionRepository(engine).publish(
            _plan(1)))
    assert (direct_authority.reserved_fill_authority.mode
            is capacity_admission.ReservedFillPlanAuthorityMode.NOT_APPLICABLE)

    _enable_durable_intent(engine, incarnation)
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='exact current reserved-fill allocation'):
        _validate_prospective_claim(engine, direct_authority.claim_values('l4'))


def test_durable_plan_modes_are_exact(capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    identity = _allocation_identity()
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, identity)

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='actuation mode and target'):
        capacity_admission.CapacityAdmissionRepository(engine).publish(_plan(1))
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='actuation mode and target'):
        capacity_admission.CapacityAdmissionRepository(engine).publish(
            _plan(0,
                  reserved_fill_authority=(
                      capacity_admission.ReservedFillPlanAuthority.
                      not_applicable())))

    zero = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(
            0,
            reserved_fill_authority=(
                capacity_admission.ReservedFillPlanAuthority.zero_revocation()
            )))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **_replica_values(17, zero_cost=True)))
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    positive_plan, _ = _allocation_bound_plan(repository, identity, {'l4': 1})
    positive = repository.publish(positive_plan)

    assert (zero.reserved_fill_authority.mode is capacity_admission.
            ReservedFillPlanAuthorityMode.UNBOUND_ZERO_REVOCATION)
    assert positive.reserved_fill_authority.allocation == identity
    assert not positive.paid_residual_by_accelerator


def test_pre_binding_positive_plan_fails_closed_after_durable_promotion(
        capacity_database):
    engine, incarnation, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    plans = capacity_admission_schema.serve_capacity_plans_table
    with engine.begin() as connection:
        payload = dict(
            connection.execute(
                sqlalchemy.select(plans.c.payload).where(
                    plans.c.service_name == 'svc',
                    plans.c.generation == authority.generation)).scalar_one())
        payload.pop('reserved_fill_authority')
        legacy_digest = capacity_admission._sha256(payload)
        connection.execute(
            sqlalchemy.update(plans).where(
                plans.c.service_name == 'svc',
                plans.c.generation == authority.generation).values(
                    payload=payload, content_sha256=legacy_digest))
    _enable_durable_intent(engine, incarnation)
    claim = authority.claim_values('l4')
    claim['capacity_plan_sha256'] = legacy_digest

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='reserved-fill plan authority'):
        _validate_prospective_claim(engine, claim)


def test_present_null_binding_is_not_legacy_compatible(capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    plans = capacity_admission_schema.serve_capacity_plans_table
    with engine.begin() as connection:
        payload = dict(
            connection.execute(
                sqlalchemy.select(plans.c.payload).where(
                    plans.c.service_name == 'svc',
                    plans.c.generation == authority.generation)).scalar_one())
        payload['reserved_fill_authority'] = None
        malformed_digest = capacity_admission._sha256(payload)
        connection.execute(
            sqlalchemy.update(plans).where(
                plans.c.service_name == 'svc',
                plans.c.generation == authority.generation).values(
                    payload=payload, content_sha256=malformed_digest))
    claim = authority.claim_values('l4')
    claim['capacity_plan_sha256'] = malformed_digest

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='reserved-fill plan authority'):
        _validate_prospective_claim(engine, claim)


def test_allocation_bound_paid_validation_holds_protocol_share(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    identity = _allocation_identity()
    _enable_durable_intent(engine, incarnation)
    validation_entered = threading.Event()
    release_validation = threading.Event()

    def _pause_validation():
        if threading.current_thread().name == 'paid-validator':
            validation_entered.set()
            assert release_validation.wait(timeout=10)

    _mock_current_allocation(monkeypatch, identity, callback=_pause_validation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, _ = _allocation_bound_plan(repository, identity, {'l4': 1})
    authority = repository.publish(plan)
    claim = authority.claim_values('l4')
    validation_errors: list[BaseException] = []

    def _validate():
        try:
            _validate_prospective_claim(engine, claim)
        except BaseException as error:  # pylint: disable=broad-except
            validation_errors.append(error)

    validator = threading.Thread(target=_validate, name='paid-validator')
    validator.start()
    assert validation_entered.wait(timeout=10)
    try:
        with pytest.raises(sqlalchemy.exc.DBAPIError):
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.text("SET LOCAL lock_timeout = '100ms'"))
                serve_state.lock_zero_cost_protocol_for_bound_launch_projection(
                    connection)
    finally:
        release_validation.set()
        validator.join(timeout=10)

    assert not validator.is_alive()
    assert not validation_errors


def test_delayed_allocation_validation_rechecks_final_expiry(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 1}, valid_until=time.time() + 1.5)
    _enable_durable_intent(engine, incarnation)
    delay_validation = threading.Event()

    def _delay_after_initial_clock():
        if delay_validation.is_set():
            time.sleep(1.7)

    _mock_current_allocation(monkeypatch,
                             allocation,
                             callback=_delay_after_initial_clock)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, _ = _allocation_bound_plan(repository, allocation, {
        'l4': 1,
        'h200': 1,
    })
    authority = repository.publish(plan)
    delay_validation.set()

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='expired while validation waited'):
        _validate_prospective_claim(engine, authority.claim_values('l4'))


def test_allocation_successor_wins_before_paid_validation(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    planned_identity = _allocation_identity(1)
    successor_identity = _allocation_identity(2)
    current_identity = [planned_identity]
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, lambda: current_identity[0])
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, _ = _allocation_bound_plan(repository, planned_identity, {'l4': 1})
    authority = repository.publish(plan)
    claim = authority.claim_values('l4')
    validator_pid: list[int] = []
    pid_ready = threading.Event()
    validation_errors: list[BaseException] = []

    def _validate():
        try:
            with engine.begin() as connection:
                validator_pid.append(
                    connection.execute(
                        sqlalchemy.text(
                            'SELECT pg_backend_pid()')).scalar_one())
                pid_ready.set()
                serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
                    connection)
                service = connection.execute(
                    sqlalchemy.select(serve_state_schema.services_table).where(
                        serve_state_schema.services_table.c.name ==
                        'svc').with_for_update()).mappings().one()
                capacity_admission.validate_paid_claim_in_connection(
                    connection,
                    service,
                    claim,
                    prospective=True,
                    protocol_and_service_prelocked=True)
        except BaseException as error:  # pylint: disable=broad-except
            validation_errors.append(error)

    with engine.begin() as writer:
        serve_state.lock_zero_cost_protocol_for_bound_launch_projection(writer)
        current_identity[0] = successor_identity
        validator = threading.Thread(target=_validate, name='paid-validator')
        validator.start()
        assert pid_ready.wait(timeout=10)
        _wait_for_blocked_postgres_backend(engine, validator_pid[0])

    validator.join(timeout=10)
    assert not validator.is_alive()
    assert len(validation_errors) == 1
    assert isinstance(validation_errors[0],
                      capacity_admission.CapacityAdmissionConflict)
    assert 'exact current reserved-fill allocation' in str(validation_errors[0])


def test_autoscaling_snapshot_is_one_repeatable_read_generation(
        capacity_database):
    """A generation-N read cannot synthesize report rows from N+1."""
    _, _, route_receipt = capacity_database
    generation_read = threading.Event()
    writer_done = threading.Event()
    reader_ident: list[int] = []
    result: list[demand_state.DurableAutoscalingSnapshot | None] = []
    errors: list[BaseException] = []

    def _pause_after_generation(_connection, _cursor, statement, _parameters,
                                _context, _executemany):
        if (reader_ident and threading.get_ident() == reader_ident[0] and
                'serve_demand_feed_generations' in statement and
                statement.lstrip().upper().startswith('SELECT')):
            generation_read.set()
            assert writer_done.wait(timeout=10)

    sqlalchemy.event.listen(sqlalchemy.engine.Engine, 'after_cursor_execute',
                            _pause_after_generation)
    try:

        def _read():
            reader_ident.append(threading.get_ident())
            try:
                result.append(
                    demand_state.get_autoscaling_snapshot('svc', 'svc-hash'))
            except BaseException as error:  # pylint: disable=broad-except
                errors.append(error)

        reader = threading.Thread(target=_read)
        reader.start()
        assert generation_read.wait(timeout=10)
        demand_state.ingest_report(
            'svc', 'svc-hash',
            _demand_report(time.time(),
                           route_receipt,
                           sequence=2,
                           request_count=0))
        writer_done.set()
        reader.join(timeout=10)
    finally:
        writer_done.set()
        sqlalchemy.event.remove(sqlalchemy.engine.Engine,
                                'after_cursor_execute', _pause_after_generation)
    assert not reader.is_alive()
    assert not errors
    assert result[0] is not None
    assert result[0].demand_feed_generation == 1
    assert result[0].receipt_watermark[0]['sequence'] == 1
    current = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert current is not None
    assert current.demand_feed_generation == 2
    assert current.receipt_watermark[0]['sequence'] == 2


def test_authority_deadline_includes_selected_occupancy_age(capacity_database):
    _, _, route_receipt = capacity_database
    max_age = constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(),
                       route_receipt,
                       sequence=2,
                       request_count=0,
                       occupancy_sample_age_seconds=max_age - 1))

    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')

    assert snapshot is not None
    authority = snapshot.reconcile_authority
    assert 0 < (authority.deadline_monotonic -
                authority.read_started_monotonic) <= 1


def test_logical_retirement_preserves_global_sql_lock_order(capacity_database):
    """Retirement composes with the canonical global row-lock order."""
    info, authority, _ = _prepare_logical_retirement(capacity_database)
    ordered_locks: list[str] = []
    relevant_tables = ('reserved_fill_protocol_state',
                       'service_lifecycle_fences', 'services', 'replicas')

    def _record_lock(_connection, _cursor, statement, _parameters, _context,
                     _executemany):
        lowered = statement.lower()
        if 'for update' not in lowered:
            return
        for table in relevant_tables:
            if table in lowered and table not in ordered_locks:
                ordered_locks.append(table)

    sqlalchemy.event.listen(sqlalchemy.engine.Engine, 'after_cursor_execute',
                            _record_lock)
    try:
        result = _commit_logical(info, authority)
    finally:
        sqlalchemy.event.remove(sqlalchemy.engine.Engine,
                                'after_cursor_execute', _record_lock)

    assert result.state is serve_state.LogicalRetirementCommitState.COMMITTED
    assert ordered_locks == list(relevant_tables)


def test_logical_retirement_rejects_allocation_successor(
        capacity_database, monkeypatch):
    """A successor map observed at the final commit fence preserves routing."""
    info, authority, _ = _prepare_logical_retirement(capacity_database)
    planned_identity = _allocation_identity(1)
    successor_identity = _allocation_identity(2)

    def _read_successor(_repository,
                        connection,
                        service_name,
                        expected_service_hash,
                        expected_controller_owner,
                        *,
                        protocol_and_service_prelocked=False):
        assert connection.in_transaction()
        assert service_name == 'svc'
        assert expected_service_hash == 'svc-hash'
        assert expected_controller_owner == (123, '10.0.0.5')
        assert protocol_and_service_prelocked is True
        return types.SimpleNamespace(identity=successor_identity,
                                     pool_snapshots=())

    monkeypatch.setattr(
        serve_state.reserved_fill_allocation.ReservedFillAllocationRepository,
        'read_current_in_connection', _read_successor)

    result = _commit_logical(info, authority, planned_identity)

    assert result.state is serve_state.LogicalRetirementCommitState.REJECTED
    durable = serve_state.get_replica_info_from_id('svc', 1)
    assert durable is not None
    assert durable.status_property.logical_retirement_committed is False
    assert (durable.status_property.sky_down_status ==
            common_utils.ProcessStatus.SCHEDULED)


def test_logical_retirement_resamples_clock_after_allocation_wait(
        capacity_database, monkeypatch):
    """Authority expiry while allocation locks block cannot authorize down."""
    info, authority, _ = _prepare_logical_retirement(capacity_database)
    identity = _allocation_identity()
    authority = dataclasses.replace(
        authority,
        valid_until=(datetime.datetime.now(datetime.timezone.utc) +
                     datetime.timedelta(milliseconds=50)))

    def _delayed_current(_repository,
                         connection,
                         service_name,
                         expected_service_hash,
                         expected_controller_owner,
                         *,
                         protocol_and_service_prelocked=False):
        del service_name, expected_service_hash, expected_controller_owner
        assert connection.in_transaction()
        assert protocol_and_service_prelocked is True
        time.sleep(0.1)
        return types.SimpleNamespace(identity=identity, pool_snapshots=())

    monkeypatch.setattr(
        serve_state.reserved_fill_allocation.ReservedFillAllocationRepository,
        'read_current_in_connection', _delayed_current)

    result = _commit_logical(info, authority, identity)

    assert result.state is serve_state.LogicalRetirementCommitState.REJECTED
    durable = serve_state.get_replica_info_from_id('svc', 1)
    assert durable is not None
    assert durable.status_property.logical_retirement_committed is False


def _wait_for_blocked_postgres_backend(engine, backend_pid: int) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with engine.connect() as connection:
            blocked = connection.execute(
                sqlalchemy.text(
                    'SELECT cardinality(pg_blocking_pids(:pid)) > 0'), {
                        'pid': backend_pid
                    }).scalar_one()
        if blocked:
            return
        time.sleep(0.01)
    pytest.fail(f'PostgreSQL backend {backend_pid} did not block as expected')


def test_logical_retirement_serializes_with_lifecycle_takeover(
        capacity_database):
    """Retirement and takeover share lifecycle-before-service ordering."""
    engine, _, _ = capacity_database
    info, authority, _ = _prepare_logical_retirement(capacity_database)
    retirement_service_locked = threading.Event()
    release_retirement = threading.Event()
    retirement_results: list[serve_state.LogicalRetirementCommitResult] = []
    lifecycle_epochs: list[int] = []
    errors: list[BaseException] = []

    raw_connection = engine.raw_connection()
    cursor = raw_connection.cursor()
    try:
        cursor.execute('SELECT pg_backend_pid()')
        lifecycle_backend_pid = int(cursor.fetchone()[0])
    finally:
        cursor.close()

    def _pause_after_retirement_service(_connection, _cursor, statement,
                                        _parameters, _context, _executemany):
        if threading.current_thread().name != 'logical-retirement':
            return
        lowered = statement.lower()
        if ('from services' in lowered and 'for update' in lowered and
                not retirement_service_locked.is_set()):
            retirement_service_locked.set()
            assert release_retirement.wait(timeout=10)

    def _retire():
        try:
            retirement_results.append(_commit_logical(info, authority))
        except BaseException as error:  # pylint: disable=broad-except
            errors.append(error)

    def _take_over():
        try:
            lifecycle_epochs.append(
                serve_state.claim_service_lifecycle_epoch(
                    'svc', raw_connection))
        except BaseException as error:  # pylint: disable=broad-except
            errors.append(error)
        finally:
            raw_connection.close()

    sqlalchemy.event.listen(sqlalchemy.engine.Engine, 'after_cursor_execute',
                            _pause_after_retirement_service)
    retirement_thread = threading.Thread(target=_retire,
                                         name='logical-retirement')
    lifecycle_thread = threading.Thread(target=_take_over,
                                        name='lifecycle-takeover')
    try:
        retirement_thread.start()
        assert retirement_service_locked.wait(timeout=10)
        lifecycle_thread.start()
        # At this point takeover must be waiting on a row the retirement
        # already owns. Under the old service-before-lifecycle order it owned
        # the lifecycle row while waiting on the service, forming a cycle when
        # retirement resumed and entered the generic replica upsert.
        _wait_for_blocked_postgres_backend(engine, lifecycle_backend_pid)
        release_retirement.set()
        retirement_thread.join(timeout=10)
        lifecycle_thread.join(timeout=10)
    finally:
        release_retirement.set()
        if retirement_thread.is_alive():
            retirement_thread.join(timeout=10)
        if lifecycle_thread.is_alive():
            lifecycle_thread.join(timeout=10)
        sqlalchemy.event.remove(sqlalchemy.engine.Engine,
                                'after_cursor_execute',
                                _pause_after_retirement_service)
        # ``close`` is idempotent and avoids leaking the connection when setup
        # fails before the lifecycle thread starts.
        raw_connection.close()

    assert not retirement_thread.is_alive()
    assert not lifecycle_thread.is_alive()
    assert not errors
    assert lifecycle_epochs == [4]
    assert len(retirement_results) == 1
    assert (retirement_results[0].state
            is serve_state.LogicalRetirementCommitState.COMMITTED)


@pytest.mark.parametrize('first', ['report', 'retirement'])
def test_logical_retirement_serializes_with_next_report(capacity_database,
                                                        first):
    """The shared service lock gives both N+1 orderings one outcome."""
    info, authority, route_receipt = _prepare_logical_retirement(
        capacity_database)
    lock_acquired = threading.Event()
    release_lock = threading.Event()
    results: list[serve_state.LogicalRetirementCommitResult] = []
    errors: list[BaseException] = []
    blocker_name = f'{first}-first'

    def _pause_after_service_lock(_connection, _cursor, statement, _parameters,
                                  _context, _executemany):
        if (threading.current_thread().name == blocker_name and
                'FROM services' in statement and 'FOR UPDATE' in statement):
            lock_acquired.set()
            assert release_lock.wait(timeout=10)

    def _commit():
        try:
            results.append(_commit_logical(info, authority))
        except BaseException as error:  # pylint: disable=broad-except
            errors.append(error)

    def _report():
        try:
            demand_state.ingest_report(
                'svc', 'svc-hash',
                _demand_report(time.time(),
                               route_receipt,
                               sequence=3,
                               request_count=0))
        except BaseException as error:  # pylint: disable=broad-except
            errors.append(error)

    sqlalchemy.event.listen(sqlalchemy.engine.Engine, 'after_cursor_execute',
                            _pause_after_service_lock)
    first_target = _report if first == 'report' else _commit
    second_target = _commit if first == 'report' else _report
    first_thread = threading.Thread(target=first_target, name=blocker_name)
    second_thread = threading.Thread(target=second_target, name='second')
    try:
        first_thread.start()
        assert lock_acquired.wait(timeout=10)
        second_thread.start()
        release_lock.set()
        first_thread.join(timeout=10)
        second_thread.join(timeout=10)
    finally:
        release_lock.set()
        sqlalchemy.event.remove(sqlalchemy.engine.Engine,
                                'after_cursor_execute',
                                _pause_after_service_lock)
    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert not errors
    assert len(results) == 1
    expected = (serve_state.LogicalRetirementCommitState.REJECTED
                if first == 'report' else
                serve_state.LogicalRetirementCommitState.COMMITTED)
    assert results[0].state is expected
    durable = serve_state.get_replica_info_from_id('svc', 1)
    assert durable is not None
    if first == 'report':
        assert durable.status_property.logical_retirement_committed is False
        assert (durable.status_property.sky_down_status ==
                common_utils.ProcessStatus.SCHEDULED)
    else:
        assert durable.status_property.logical_retirement_committed is True
        assert (durable.status_property.sky_down_status ==
                common_utils.ProcessStatus.RUNNING)


def test_logical_retirement_commit_lost_ack_is_ambiguous(
        capacity_database, monkeypatch):
    info, authority, _ = _prepare_logical_retirement(capacity_database)
    original_commit = sqlalchemy.orm.Session.commit
    injected = False

    def _commit_then_lose_ack(session):
        nonlocal injected
        original_commit(session)
        if not injected:
            injected = True
            raise sqlalchemy.exc.OperationalError('COMMIT', {},
                                                  RuntimeError('lost ack'))

    monkeypatch.setattr(sqlalchemy.orm.Session, 'commit', _commit_then_lose_ack)

    result = _commit_logical(info, authority)

    assert result.state is serve_state.LogicalRetirementCommitState.AMBIGUOUS
    durable = serve_state.get_replica_info_from_id('svc', 1)
    assert durable is not None
    assert durable.status_property.logical_retirement_committed is True
    assert (durable.status_property.sky_down_status ==
            common_utils.ProcessStatus.RUNNING)


def test_heartbeat_refresh_keeps_plan_and_bounded_claims(capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    first = repository.publish(_plan(2))
    first_claim = _insert_claim(engine, first, 10)

    demand_state.ingest_report(
        'svc', 'svc-hash', _demand_report(time.time(),
                                          route_receipt,
                                          sequence=2))
    duplicate = repository.publish(_plan(2))

    assert duplicate.generation == first.generation
    assert duplicate.demand_feed_generation > first.demand_feed_generation
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(
            connection, {
                **service, 'name': 'svc'
            }, first_claim)

    _insert_claim(engine, duplicate, 11)
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='exceed'):
            capacity_admission.validate_paid_claim_in_connection(
                connection, {
                    **service, 'name': 'svc'
                }, {
                    **duplicate.claim_values('L4'),
                    'replica_id': 12,
                },
                prospective=True)


def test_zero_cost_commit_after_plan_revokes_paid_claim(capacity_database):
    engine, _, _ = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    authority = repository.publish(_plan(2))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **_replica_values(20, zero_cost=True)))
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='Committed capacity changed'):
            capacity_admission.validate_paid_claim_in_connection(
                connection, {
                    **service, 'name': 'svc'
                }, {
                    **authority.claim_values('L4'),
                    'replica_id': 21,
                },
                prospective=True)


@pytest.mark.parametrize(('admitted', 'expected_paid'), [(False, 1), (True, 0)],
                         ids=['pod-waiting', 'quota-assigned'])
def test_only_quota_assigned_kueue_capacity_is_reserved_supply(
        capacity_database, admitted, expected_paid):
    engine, _, _ = capacity_database
    _install_waiting_kueue_capacity(engine, admitted=admitted)

    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))

    assert authority.remaining() == ({
        'l4': expected_paid
    } if expected_paid else {})


@pytest.mark.parametrize('provider_effect', [False, True],
                         ids=['final-claim', 'provider-effect'])
def test_missing_kueue_admission_fails_closed_at_final_paid_boundaries(
        capacity_database, provider_effect):
    engine, _, _ = capacity_database
    key = _install_waiting_kueue_capacity(engine)
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    claim = (authority.claim_values('L4')
             if not provider_effect else _insert_claim(engine, authority, 100))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(
                kueue_lane_lineage_schema.serve_kueue_admissions_table).where(
                    kueue_lane_lineage_schema.serve_kueue_admissions_table.c.
                    intent_idempotency_key == key))
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='Committed capacity changed'):
            capacity_admission.validate_paid_claim_in_connection(
                connection, {
                    **service, 'name': 'svc'
                }, {
                    **claim, 'replica_id': 101
                },
                prospective=not provider_effect)


@pytest.mark.parametrize('provider_effect', [False, True],
                         ids=['final-claim', 'provider-effect'])
@pytest.mark.parametrize(('field', 'value'), _COPIED_ADMISSION_MUTATIONS)
def test_each_copied_kueue_identity_mismatch_fails_closed_at_paid_boundaries(
        capacity_database, monkeypatch, provider_effect, field, value):
    engine, _, _ = capacity_database
    key = _install_waiting_kueue_capacity(engine)
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    claim = (authority.claim_values('L4')
             if not provider_effect else _insert_claim(engine, authority, 102))
    original = (kueue_lane_lineage.KueueAdmissionRepository.
                lock_service_admissions_in_connection)

    def _corrupt(self, connection, service_name, service_hash):
        rows = original(self, connection, service_name, service_hash)
        return tuple(
            dataclasses.replace(row, **{field: value}) if row.
            intent_idempotency_key == key else row for row in rows)

    monkeypatch.setattr(kueue_lane_lineage.KueueAdmissionRepository,
                        'lock_service_admissions_in_connection', _corrupt)
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='Committed capacity changed'):
            capacity_admission.validate_paid_claim_in_connection(
                connection, {
                    **service, 'name': 'svc'
                }, {
                    **claim, 'replica_id': 103
                },
                prospective=not provider_effect)


def test_proven_east_intent_without_admission_allows_final_paid_claim(
        capacity_database):
    engine, _, _ = capacity_database
    _install_pending_east_capacity(engine)
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(2))
    assert authority.remaining() == {'l4': 1}
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(connection, {
            **service, 'name': 'svc'
        }, {
            **authority.claim_values('L4'), 'replica_id': 104
        },
                                                             prospective=True)


def test_cross_card_reserved_capacity_satisfies_supply_aware_target(
        capacity_database):
    engine, _, _ = capacity_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **_replica_values(22, zero_cost=True, accelerator='A100')))
    plan = _plan(2)
    plan = dataclasses.replace(plan,
                               normalized_demand={
                                   **plan.normalized_demand,
                                   'demand_target_by_accelerator': {
                                       'L4': 2,
                                   },
                               },
                               capacity_target_by_accelerator={
                                   'L4': 0,
                                   'A100': 2,
                               })

    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        plan)

    assert authority.remaining() == {'a100': 1}


def test_allocation_tail_satisfies_flexible_demand_before_paid_residual(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 1})
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, projection = _allocation_bound_plan(repository, allocation, {
        'l4': 0,
        'h200': 1,
    })

    authority = repository.publish(plan)

    assert projection.pending_zero_cost_capacity_by_accelerator == {
        'h200': 0,
        'l4': 0,
    }
    assert projection.allocation_reserved_capacity_by_accelerator == {
        'h200': 1,
        'l4': 0,
    }
    assert not authority.remaining()


def test_allocation_tail_uses_logical_multi_gpu_card_width(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 2}, accelerator_count=4)
    _enable_durable_intent(engine,
                           incarnation,
                           max_replicas=8,
                           replica_unit='logical')
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, projection = _allocation_bound_plan(repository, allocation, {
        'l4': 0,
        'h200': 8,
    })

    authority = repository.publish(plan)

    assert projection.allocation_reserved_capacity_by_accelerator == {
        'h200': 8,
        'l4': 0,
    }
    assert not authority.remaining()


def test_allocation_tail_leaves_only_genuinely_uncovered_paid_residual(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 1})
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, _ = _allocation_bound_plan(repository, allocation, {
        'l4': 1,
        'h200': 1,
    })

    authority = repository.publish(plan)

    assert authority.remaining() == {'l4': 1}
    _validate_prospective_claim(engine, authority.claim_values('l4'))


def test_tail_to_pending_to_replica_never_double_counts_reserved_supply(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 2})
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    target = {'l4': 0, 'h200': 2}

    plan, projection = _allocation_bound_plan(repository, allocation, target)
    tail_authority = repository.publish(plan)
    tail_payload = _capacity_plan_payload(engine, tail_authority.generation)
    assert projection.additional_capacity_by_accelerator() == {
        'h200': 2,
        'l4': 0,
    }

    intent_key = _insert_current_allocation_pending(engine, allocation)
    plan, projection = _allocation_bound_plan(repository, allocation, target)
    pending_authority = repository.publish(plan)
    pending_payload = _capacity_plan_payload(engine,
                                             pending_authority.generation)
    assert projection.pending_zero_cost_capacity_by_accelerator['h200'] == 1
    assert projection.allocation_reserved_capacity_by_accelerator['h200'] == 1

    _materialize_current_allocation_pending(engine, intent_key, 81)
    plan, projection = _allocation_bound_plan(repository, allocation, target)
    replica_authority = repository.publish(plan)
    replica_payload = _capacity_plan_payload(engine,
                                             replica_authority.generation)
    assert projection.pending_zero_cost_capacity_by_accelerator['h200'] == 0
    assert projection.allocation_reserved_capacity_by_accelerator['h200'] == 1

    def _reserved_total(payload: dict) -> int:
        return sum(payload[field].get('h200', 0)
                   for field in ('existing_zero_cost_capacity_by_accelerator',
                                 'pending_zero_cost_capacity_by_accelerator',
                                 'allocation_reserved_capacity_by_accelerator'))

    assert [
        _reserved_total(payload)
        for payload in (tail_payload, pending_payload, replica_payload)
    ] == [2, 2, 2]
    assert not tail_authority.remaining()
    assert not pending_authority.remaining()
    assert not replica_authority.remaining()


def test_provider_start_rejects_tail_to_pending_inventory_change(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 1})
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, _ = _allocation_bound_plan(repository, allocation, {
        'l4': 1,
        'h200': 1,
    })
    authority = repository.publish(plan)
    claim = _insert_claim(engine, authority, 82)
    _insert_current_allocation_pending(engine, allocation)

    with engine.begin() as connection:
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='Reserved economic supply changed'):
            capacity_admission.validate_paid_claim_in_connection(
                connection,
                service,
                claim,
                prospective=False,
                protocol_and_service_prelocked=True)


def test_provider_start_accepts_plan_own_paid_replica_row(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 1})
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, _ = _allocation_bound_plan(repository, allocation, {
        'l4': 1,
        'h200': 1,
    })
    authority = repository.publish(plan)
    claim = _insert_claim(engine, authority, 84)

    with engine.begin() as connection:
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(
            connection,
            service,
            claim,
            prospective=False,
            protocol_and_service_prelocked=True)


def test_provider_start_rejects_reserved_lifecycle_change_at_same_inventory(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 1})
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, allocation)
    zero_cost_replica_id = 85
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**_replica_values(
                    zero_cost_replica_id, zero_cost=True, accelerator='H200')))

    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, _ = _allocation_bound_plan(repository, allocation, {
        'l4': 1,
        'h200': 2,
    })
    authority = repository.publish(plan)
    claim = _insert_claim(engine, authority, 86)

    replicas = serve_state_schema.replicas_table
    with engine.begin() as connection:
        state = connection.execute(
            sqlalchemy.select(replicas.c.replica_state).where(
                replicas.c.service_name == 'svc', replicas.c.replica_id ==
                zero_cost_replica_id).with_for_update()).scalar_one()
        state['status_property'].update(
            sky_launch_status=common_utils.ProcessStatus.SUCCEEDED.value,
            service_ready_now=True,
            first_ready_time=time.time())
        connection.execute(
            sqlalchemy.update(replicas).where(
                replicas.c.service_name == 'svc',
                replicas.c.replica_id == zero_cost_replica_id).values(
                    status='READY', replica_state=state))

    with engine.begin() as connection:
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='Reserved economic supply changed'):
            capacity_admission.validate_paid_claim_in_connection(
                connection,
                service,
                claim,
                prospective=False,
                protocol_and_service_prelocked=True)


def test_running_paid_replica_is_not_mutated_by_future_supply_change(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 1})
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    target = {'l4': 1, 'h200': 1}
    plan, _ = _allocation_bound_plan(repository, allocation, target)
    authority = repository.publish(plan)
    _insert_claim(engine, authority, 83)
    replicas = serve_state_schema.replicas_table
    with engine.begin() as connection:
        state = connection.execute(
            sqlalchemy.select(replicas.c.replica_state).where(
                replicas.c.service_name == 'svc',
                replicas.c.replica_id == 83).with_for_update()).scalar_one()
        state['status_property'].update(
            sky_launch_status=common_utils.ProcessStatus.SUCCEEDED.value,
            service_ready_now=True,
            first_ready_time=time.time(),
            is_scale_down=False)
        connection.execute(
            sqlalchemy.update(replicas).where(
                replicas.c.service_name == 'svc',
                replicas.c.replica_id == 83).values(status='READY',
                                                    replica_state=state))

    _insert_current_allocation_pending(engine, allocation)
    plan, projection = _allocation_bound_plan(repository, allocation, target)
    successor = repository.publish(plan)

    assert projection.pending_zero_cost_capacity_by_accelerator['h200'] == 1
    assert projection.allocation_reserved_capacity_by_accelerator['h200'] == 0
    assert not successor.remaining()
    with engine.connect() as connection:
        retained = connection.execute(
            sqlalchemy.select(
                replicas.c.status, replicas.c.replica_state).where(
                    replicas.c.service_name == 'svc',
                    replicas.c.replica_id == 83)).mappings().one()
        claim_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.paid_capacity_claims_table).where(
                    serve_state_schema.paid_capacity_claims_table.c.service_name
                    == 'svc',
                    serve_state_schema.paid_capacity_claims_table.c.replica_id
                    == 83)).scalar_one()
    assert retained['status'] == 'READY'
    assert retained['replica_state']['status_property'][
        'is_scale_down'] is False
    assert claim_count == 1


def test_full_allocation_tail_requires_rotation_independent_headroom(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 1})
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, allocation)
    with engine.begin() as connection:
        connection.execute(sqlalchemy.insert(serve_state_schema.replicas_table),
                           [
                               _replica_values(replica_id, zero_cost=False)
                               for replica_id in range(200, 210)
                           ])

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='rotation-independent service headroom'):
        capacity_admission.CapacityAdmissionRepository(
            engine).project_reserved_supply(
                service_name='svc',
                service_hash='svc-hash',
                service_lifecycle_epoch=3,
                service_version=1,
                accounting_cards={
                    'l4': 10,
                    'h200': 0,
                },
                authority=capacity_admission.ReservedFillPlanAuthority.bound(
                    allocation.identity))


def test_zero_target_mints_revoking_semantic_generation(capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    first = repository.publish(_plan(1))
    claim = _insert_claim(engine, first, 30)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=0))
    revoked = repository.publish(_plan(0))

    assert revoked.generation == first.generation + 1
    assert not revoked.remaining()
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='lost its current'):
            capacity_admission.validate_paid_claim_in_connection(
                connection, {
                    **service, 'name': 'svc'
                }, claim)


def test_protocol2_full_window_zero_revokes_paid_authority_without_exact_cards(
        capacity_database):
    engine, incarnation, route_receipt = capacity_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_snapshots_table).values(
                    producer_protocol_version=2))
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    route_projection_protocol_version=2,
                    route_projection_controller_incarnation=incarnation))
    report = _demand_report(time.time(),
                            route_receipt,
                            sequence=2,
                            request_count=0)
    report['demand_window']['compatibility_complete'] = False
    demand_state.ingest_report('svc', 'svc-hash', report)

    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    assert snapshot.fresh_aggregate_zero
    assert not snapshot.request_information['compatibility_demand_complete']
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    zero = repository.publish(_plan(0))
    assert not zero.remaining()

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='demand receipts'):
        repository.publish(_plan(1))


def test_superseded_unclaimed_plan_is_collected(capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    first = repository.publish(_plan(1))
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=0))
    second = repository.publish(_plan(0))

    assert second.generation == first.generation + 1
    with engine.connect() as connection:
        generations = connection.execute(
            sqlalchemy.select(
                capacity_admission_schema.serve_capacity_plans_table.c.
                generation)).scalars().all()
    assert generations == [second.generation]


def test_corrupt_route_snapshot_blocks_demand_and_plan(capacity_database):
    engine, _, route_receipt = capacity_database
    plan = _plan(1)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_snapshots_table).where(
                    route_projection_schema.serve_route_snapshots_table.c.
                    service_name == 'svc',
                    route_projection_schema.serve_route_snapshots_table.c.
                    generation == route_receipt.generation).values(
                        response_payload={}))

    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is None
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='corrupt'):
        capacity_admission.CapacityAdmissionRepository(engine).publish(plan)


def test_ha_generation_change_revokes_stale_demand_and_plan(capacity_database):
    engine, _, route_receipt = capacity_database
    plan = _plan(1)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    lb_ha_enabled=1,
                    lb_active_slot='a',
                    lb_cutover_generation=2))

    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is None
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='demand receipts'):
        capacity_admission.CapacityAdmissionRepository(engine).publish(plan)

    current = _demand_report(time.time(), route_receipt, sequence=2)
    current['applied_generation'] = 2
    demand_state.ingest_report('svc', 'svc-hash', current)
    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is not None


def test_promotion_rejects_unnormalized_live_replica(capacity_database):
    engine, incarnation, _ = capacity_database
    with engine.begin() as connection:
        capacity_admission.demote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            expected_source_epoch=1)
        malformed = _replica_values(40, zero_cost=True)
        malformed['replica_state'].pop('replica_info_version')
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**malformed))
    with engine.begin() as connection:
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='normalized ReplicaInfo v18'):
            capacity_admission.promote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=incarnation,
                participant_barrier_passed=lambda _connection: True)


def test_exact_card_plan_rejects_unclassified_committed_replica(
        capacity_database):
    engine, _, _ = capacity_database
    unclassified = _replica_values(41, zero_cost=True)
    unclassified['replica_state']['location'] = None
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**unclassified))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='exact-card accounting'):
        capacity_admission.CapacityAdmissionRepository(engine).publish(_plan(1))
