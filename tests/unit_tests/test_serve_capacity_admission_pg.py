"""PostgreSQL contracts for ordered SkyServe capacity admission."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import dataclasses
import datetime
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
    normalized_demand: dict | None = None
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
        capacity_target_by_accelerator={'l4': demand_target})


def _replica_values(replica_id: int,
                    *,
                    zero_cost: bool,
                    accelerator: str = 'L4') -> dict:
    return {
        'service_name': 'svc',
        'replica_id': replica_id,
        'replica_state_version': 1,
        'status': 'PENDING',
        'version': 1,
        'cluster_name': f'svc-{replica_id}',
        'created_at': time.time(),
        'is_spot': not zero_cost,
        'replica_state': {
            'replica_info_version': 18,
            'planned_capacity': 1,
            'is_zero_cost': zero_cost,
            'location': {
                'accelerators': {
                    accelerator: 1,
                },
            },
            'resources_override': None,
            'status_property': {
                'is_scale_down': False,
            },
        },
    }


def _install_waiting_kueue_capacity(engine) -> str:
    """Install one exact fresh-waiting L4 intent for final accounting."""
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
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name='svc',
                version=1,
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
        receipt = _kueue_receipt(
            kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
            key,
            record_id,
            identity=identity)
        repository.observe_pod_waiting_in_connection(
            connection,
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
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name='svc',
                version=1,
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
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(connection, {
            **service, 'name': 'svc'
        }, {
            **claim,
            'replica_id': replica_id,
        },
                                                             prospective=True)
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
    assert (retirement_results[0].state is
            serve_state.LogicalRetirementCommitState.COMMITTED)


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
