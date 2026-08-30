"""PostgreSQL contracts for ordered SkyServe capacity admission."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import copy
import dataclasses
import datetime
import json
import pickle
import threading
import time
import types
from unittest import mock
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
from sky.serve import capacity_planning
from sky.serve import constants
from sky.serve import demand_state
from sky.serve import demand_state_schema
from sky.serve import kubernetes_identity
from sky.serve import kueue_lane_lineage
from sky.serve import kueue_lane_lineage_schema
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
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
    lb_high_availability: bool = False,
    utilization_gate: bool = False,
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
        lb_high_availability=lb_high_availability,
        reserved_capacity_fill=({
            'utilization_gate': utilization_gate,
        } if reserved_fill_enabled else False))


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
                   occupancy_sample_age_seconds: float = 0.1,
                   reporter_session_id: str = 'process-a',
                   lb_session_id: str = 'pod-a',
                   lb_slot: str = 'a',
                   applied_role: str = 'ACTIVE',
                   applied_generation: int = 1,
                   routing_version: int = 1) -> dict:
    bucket_seconds = constants.LB_DEMAND_WINDOW_BUCKET_SECONDS
    profiles = ([{
        'priority': 50,
        'compatible_accelerators': ['L4'],
        'count': request_count,
    }] if request_count else [])
    return {
        'protocol_version': 2,
        'sequence': sequence,
        'reporter_session_id': reporter_session_id,
        'reporter_observed_at': now,
        'lb_session_id': lb_session_id,
        'lb_slot': lb_slot,
        'routing_version': routing_version,
        'armed_generation': None,
        'applied_role': applied_role,
        'applied_generation': applied_generation,
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
    planner_supply: capacity_admission.ReservedSupplyProjection | None = None,
) -> capacity_admission.CapacityPlanInput:
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    target = ({
        'l4': demand_target
    } if capacity_target_by_accelerator is None else
              capacity_target_by_accelerator)
    decision = _current_decision(snapshot,
                                 planner_supply,
                                 demand_target,
                                 target_by_accelerator=target)
    _, candidate = decision.decode_planner()
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
        capacity_target_by_accelerator=(
            decision.capacity_target_by_accelerator),
        reserved_fill_authority=(
            capacity_admission.ReservedFillPlanAuthority.not_applicable()
            if reserved_fill_authority is None else reserved_fill_authority),
        paid_residual=candidate.paid_residual,
        paid_launch_target=candidate.paid_launch_target,
        allocation_reserved_capacity_by_accelerator=(
            {} if allocation_reserved_capacity_by_accelerator is None else
            allocation_reserved_capacity_by_accelerator),
        expected_pending_zero_cost_capacity_by_accelerator=(
            {} if expected_pending_zero_cost_capacity_by_accelerator is None
            else expected_pending_zero_cost_capacity_by_accelerator),
        expected_economic_capacity_graph_sha256=(
            expected_economic_capacity_graph_sha256),
        planner_payload=decision.planner_payload)


def _current_decision(
    demand_snapshot: demand_state.DurableAutoscalingSnapshot,
    supply: capacity_admission.ReservedSupplyProjection | None,
    target: int,
    *,
    accelerator: str = 'l4',
    target_by_accelerator: dict[str, int] | None = None,
    source_fingerprint: str | None = None,
    prospective_paid_accelerators: tuple[str, ...] | None = None,
    compatible_accelerators: tuple[str, ...] | None = None,
    cold_accelerator_order: tuple[str, ...] | None = None,
    capacity_unit: capacity_planning.CapacityUnit = (
        capacity_planning.CapacityUnit.PHYSICAL_BACKEND),
    physical_gpu_width_by_accelerator: dict[str, int] | None = None,
    backend_num_nodes: int = 1,
    provisioning_by_accelerator: dict[str, int] | None = None,
    max_live_paid_gpu_units: int | None = None,
    return_candidate: bool = False,
) -> (capacity_admission.CapacityPlanDecision |
      capacity_planning.CapacityPlanCandidate):
    """Build a decision through the production typed allocation kernel."""

    def _capacity(values):
        return capacity_planning.AcceleratorCapacity.from_mapping(values)

    requested_target = ({
        accelerator: target
    } if target_by_accelerator is None else {
        card.casefold(): count for card, count in target_by_accelerator.items()
    })
    configured = tuple(
        sorted(
            set(requested_target) |
            (set() if supply is None else set(supply.reserved_accelerators))))
    if supply is None:
        policy = capacity_planning.ReservationGatePolicy.NOT_CONFIGURED
        evidence = capacity_planning.ReservationEvidenceState.NOT_APPLICABLE
        reservation_capacities = {
            field: {
                card: 0 for card in configured
            } for field in ('authenticated', 'eligible', 'pending', 'zero_cost',
                           'paid')
        }
        evidence_fingerprint = ''
        planner_source_fingerprint = source_fingerprint or '0' * 64
    else:
        policy = {
            capacity_admission.ReservedSupplyPolicy.DISABLED:
                capacity_planning.ReservationGatePolicy.NOT_CONFIGURED,
            capacity_admission.ReservedSupplyPolicy.STATIC_PREFILL:
                capacity_planning.ReservationGatePolicy.UNGATED,
            capacity_admission.ReservedSupplyPolicy.DEMAND_GATED:
                capacity_planning.ReservationGatePolicy.DEMAND_GATED,
        }[supply.policy]
        evidence = {
            capacity_admission.ReservedSupplyEvidenceState.NOT_APPLICABLE:
                capacity_planning.ReservationEvidenceState.NOT_APPLICABLE,
            capacity_admission.ReservedSupplyEvidenceState.AUTHENTICATED_SETTLED:
                capacity_planning.ReservationEvidenceState.
                AUTHENTICATED_SETTLED,
            capacity_admission.ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED:
                capacity_planning.ReservationEvidenceState.
                AUTHENTICATED_UNSETTLED,
            capacity_admission.ReservedSupplyEvidenceState.UNAVAILABLE:
                capacity_planning.ReservationEvidenceState.UNAVAILABLE,
        }[supply.evidence_state]
        reservation_capacities = {
            'authenticated': supply.authenticated_capacity_by_accelerator,
            'eligible': supply.eligible_capacity_by_accelerator,
            'pending': supply.pending_zero_cost_capacity_by_accelerator,
            'zero_cost': supply.existing_zero_cost_capacity_by_accelerator,
            'paid': supply.existing_paid_capacity_by_accelerator,
        }
        evidence_fingerprint = supply.reservation_evidence_sha256
        planner_source_fingerprint = (
            capacity_admission.locked_planning_source_fingerprint(
                source_fingerprint, supply.economic_capacity_graph_sha256))
    reservation = capacity_planning.ReservationPlanningInput(
        gate_policy=policy,
        evidence_state=evidence,
        authenticated_capacity=_capacity(
            reservation_capacities['authenticated']),
        eligible_capacity=_capacity(reservation_capacities['eligible']),
        pending_zero_cost_capacity=_capacity(reservation_capacities['pending']),
        existing_zero_cost_capacity=_capacity(
            reservation_capacities['zero_cost']),
        existing_paid_capacity=_capacity(reservation_capacities['paid']),
        charged_paid_gpu_units=(0 if supply is None else
                                supply.charged_paid_gpu_units),
        evidence_fingerprint=evidence_fingerprint,
        allocation_demand_witness_sha256=(
            None
            if supply is None else supply.allocation_demand_witness_sha256),
        allocation_demonstrated_need=(None if supply is None else
                                      supply.allocation_demonstrated_need),
        allocation_ceiling=(0 if supply is None else supply.allocation_ceiling))
    profiles = tuple(
        capacity_planning.CompatibilityDemand(
            sequence=sequence,
            priority=50,
            compatible_accelerators=((card,) if compatible_accelerators is
                                     None else compatible_accelerators),
            work=float(count))
        for sequence, (card,
                       count) in enumerate(sorted(requested_target.items()))
        if count > 0)
    planner_snapshot = capacity_planning.CapacityPlanningSnapshot(
        source_generation=demand_snapshot.demand_feed_generation,
        service_version=1,
        configured_accelerators=configured,
        capacity_unit=capacity_unit,
        backend_num_nodes=backend_num_nodes,
        physical_gpu_width_by_accelerator=_capacity(
            ({
                card: 1 for card in configured
            } if physical_gpu_width_by_accelerator is None else
             physical_gpu_width_by_accelerator)),
        capacity_per_accelerator=(
            capacity_planning.AcceleratorWork.from_mapping(
                {card: 1 for card in configured})),
        floors=_capacity({card: 0 for card in configured}),
        minimum_capacity=0,
        paid_minimum_capacity=0,
        actuation_minimum_capacity=0,
        maximum_capacity=max(10, sum(requested_target.values())),
        demand_profiles=profiles,
        explicit_demand_profiles=profiles,
        paid_demand_profiles=profiles,
        fixed_work=capacity_planning.AcceleratorWork(),
        explicit_fixed_work=capacity_planning.AcceleratorWork(),
        paid_fixed_work=capacity_planning.AcceleratorWork(),
        retention_work=capacity_planning.AcceleratorWork(),
        ready_zero_cost=capacity_planning.AcceleratorCapacity(),
        ready=capacity_planning.AcceleratorCapacity(),
        provisioning=_capacity({} if provisioning_by_accelerator is
                               None else provisioning_by_accelerator),
        reservation=reservation,
        cold_accelerator_order=(configured if cold_accelerator_order is None
                                else cold_accelerator_order),
        prospective_paid_accelerator_order=(
            configured if prospective_paid_accelerators is None else
            prospective_paid_accelerators),
        planning_purpose=(
            capacity_planning.CapacityPlanningPurpose.ECONOMIC_ADMISSION),
        actuation_supply_policy=(
            capacity_planning.ActuationSupplyPolicy.REUSE_CURRENT_SUPPLY),
        attribution_complete=True,
        planning_time=1.0,
        max_live_paid_gpu_units=max_live_paid_gpu_units,
        retirement_shelter_target=capacity_planning.AcceleratorCapacity(),
        source_fingerprint=planner_source_fingerprint,
        configured_reservation_accelerators=(() if supply is None else
                                             supply.reserved_accelerators),
        demand_witness_scope_sha256=('' if supply is None else
                                     supply.demand_witness_scope_sha256))
    candidate = capacity_planning.plan_capacity(planner_snapshot)
    if return_candidate:
        return candidate
    candidate_target = candidate.supply_aware_demand_target.as_dict()
    target_by_accelerator = {
        card: candidate_target.get(card, 0) for card in configured
    }
    return capacity_admission.CapacityPlanDecision(
        capacity_target_by_accelerator=target_by_accelerator,
        normalized_demand_extensions={
            'autoscaler_target': sum(requested_target.values()),
            'replica_unit': ('logical' if capacity_unit
                             is capacity_planning.CapacityUnit.LOGICAL_GPU else
                             'physical_backend'),
            'demand_target_by_accelerator': target_by_accelerator,
        },
        reserved_capacity_commitment_by_accelerator=(
            candidate.new_reserved_capacity_committed.as_dict()),
        expected_paid_residual_by_accelerator=(
            candidate.paid_residual.as_dict()),
        expected_paid_launch_target_by_accelerator=(
            candidate.paid_launch_target.as_dict()),
        static_reserved_fill_target_by_accelerator=(
            candidate.static_prefill_target.as_dict()),
        planner_payload=capacity_planning.planner_envelope(
            planner_snapshot, candidate))


def _paid_pool_key(accelerator: str = 'L4',
                   accelerator_count: int = 1,
                   num_nodes: int = 1) -> str:
    return json.dumps(
        {
            'version': 1,
            'workspace': 'workspace-a',
            'cloud': 'gcp',
            'region': 'us-central1',
            'zone': None,
            'instance_type': None,
            'accelerators': [[accelerator.casefold(), accelerator_count]],
            'use_spot': True,
            'num_nodes': num_nodes,
        },
        sort_keys=True,
        separators=(',', ':'))


def _replica_values(replica_id: int,
                    *,
                    zero_cost: bool,
                    accelerator: str = 'L4',
                    accelerator_count: int = 1,
                    planned_capacity: int = 1,
                    num_nodes: int = 1) -> dict:
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'svc-{replica_id}',
        replica_port='8000',
        is_spot=not zero_cost,
        location=None,
        version=1,
        resources_override={'accelerators': {
            accelerator: accelerator_count,
        }},
        planned_capacity=planned_capacity)
    info.is_zero_cost = zero_cost
    if not zero_cost:
        info.paid_capacity_pool_key = _paid_pool_key(accelerator,
                                                     accelerator_count,
                                                     num_nodes)
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
                pool_key=_paid_pool_key(),
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
                                             pool_key=_paid_pool_key(),
                                             priority=50,
                                             claimed_at=time.time(),
                                             **claim))
    return claim


def test_paid_plan_claimed_units_projection_is_exact_and_fails_closed(
        capacity_database):
    engine, _, _ = capacity_database
    pools = serve_state_schema.paid_capacity_pools_table
    claims = serve_state_schema.paid_capacity_claims_table
    digest = 'a' * 64

    def _claim_values(*,
                      replica_id,
                      units,
                      generation=8,
                      service_name='svc',
                      service_hash='svc-hash',
                      content_sha256=digest):
        return {
            'service_name': service_name,
            'service_hash': service_hash,
            'replica_id': replica_id,
            'pool_key': 'gcp:L4',
            'priority': 50,
            'claimed_at': time.time(),
            'capacity_plan_generation': generation,
            'capacity_plan_sha256': content_sha256,
            'demand_feed_generation': 9,
            'demand_source_epoch': 1,
            'capacity_plan_accelerator': 'L4',
            'capacity_plan_units': units,
        }

    with engine.begin() as connection:
        connection.execute(
            postgresql.insert(pools).values(
                pool_key='gcp:L4',
                current_limit=10,
                successes_since_resize=0,
                updated_at=time.time()).on_conflict_do_nothing())
        valid_replicas = [
            _replica_values(replica_id, zero_cost=False)
            for replica_id in (101, 102, 103)
        ]
        retained = _replica_values(108, zero_cost=False)
        retained.update(status='READY',
                        ordinary_launch_association_id=uuid.uuid4())
        connection.execute(sqlalchemy.insert(serve_state_schema.replicas_table),
                           [*valid_replicas, retained])
        connection.execute(
            sqlalchemy.insert(claims),
            [
                _claim_values(replica_id=101, units=4),
                _claim_values(replica_id=102, units=8),
                _claim_values(replica_id=103, units=16, generation=9),
                _claim_values(replica_id=104, units=32, service_name='other'),
                _claim_values(
                    replica_id=105, units=64, service_hash='other-hash'),
                # No matching replica row: this stale same-plan debit must not
                # suppress the next candidate cohort before Phase A can prune it.
                _claim_values(replica_id=107, units=128),
                # A provider association retains claim ownership even after the
                # replica leaves PENDING/PROVISIONING.
                _claim_values(replica_id=108, units=2),
            ])

    assert serve_state.get_paid_capacity_plan_claimed_units(
        'svc', 'svc-hash', 8, digest) == {
            'l4': 14
        }

    # A same-generation row carrying a different immutable plan digest is a
    # corrupt debit ledger, not an ignorable claim from another plan.
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **_replica_values(106, zero_cost=False)))
        connection.execute(
            sqlalchemy.insert(claims).values(**_claim_values(
                replica_id=106, units=1, content_sha256='b' * 64)))
    with pytest.raises(ValueError, match='debit ledger is malformed'):
        serve_state.get_paid_capacity_plan_claimed_units(
            'svc', 'svc-hash', 8, digest)


def _route_record_id(engine) -> str:
    with engine.connect() as connection:
        identity_payload = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_snapshots_table.c.
                identity_payload).where(
                    route_projection_schema.serve_route_snapshots_table.c.
                    service_name == 'svc').order_by(
                        route_projection_schema.serve_route_snapshots_table.c.
                        generation.desc()).limit(1)).scalar_one()
    return str(identity_payload[_URL]['replica_record_id'])


def _publish_route_snapshot(
    engine: sqlalchemy.engine.Engine,
    incarnation: uuid.UUID,
    response: dict,
    identities: dict,
    current_record_ids: set[str],
) -> route_projection.RoutePublicationReceipt:
    identity = route_projection.RoutePublisherIdentity(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        controller_incarnation=incarnation,
        controller_owner_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.5')
    return route_projection.RouteProjectionRepository(engine).publish(
        identity, 1, response, identities, current_record_ids, ttl_seconds=60)


def _publish_successor_route(
    engine: sqlalchemy.engine.Engine,
    incarnation: uuid.UUID,
    marker: int,
) -> route_projection.RoutePublicationReceipt:
    response = _route_response()
    response['capacity_hint']['test_route_marker'] = marker
    record_id = _route_record_id(engine)
    return _publish_route_snapshot(engine, incarnation, response,
                                   _route_identities(record_id), {record_id})


def _validate_committed_claim(engine: sqlalchemy.engine.Engine,
                              claim: dict) -> datetime.datetime:
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        fresh_until = capacity_admission.validate_paid_claim_in_connection(
            connection, service, claim, prospective=False)
    assert fresh_until is not None
    return fresh_until


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
                           replica_unit: str = 'physical_backend',
                           utilization_gate: bool = False) -> None:
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
                        replica_unit=replica_unit,
                        utilization_gate=utilization_gate),
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


def _validate_prospective_claim_batch(engine, claims):
    with engine.begin() as connection:
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        return (capacity_admission.
                validate_prospective_paid_claim_batch_in_connection(
                    connection,
                    service,
                    claims,
                    protocol_and_service_prelocked=True))


def _mock_prospective_snapshot_normalized_demand(monkeypatch, transform):
    original = demand_state.get_autoscaling_snapshot
    observed = []

    def _read(*args, **kwargs):
        snapshot = original(*args, **kwargs)
        assert snapshot is not None
        observed.append(snapshot)
        normalized = transform(copy.deepcopy(snapshot.normalized_demand))
        return dataclasses.replace(snapshot, normalized_demand=normalized)

    monkeypatch.setattr(demand_state, 'get_autoscaling_snapshot', _read)
    return observed


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
    grant: int | None = None,
    edge_cap: int | None = None,
    valid_until: float | None = None,
    utilization_gate_armed: bool = False,
    utilization_demonstrated_need: int | None = None,
    utilization_demand_witness_sha256: str | None = None,
    utilization_ceiling: int = 0,
    upward_grants_settled: bool = True,
) -> reserved_fill_planner.AuthenticatedAllocationMap:
    """Build one exact fresh allocation for paid-admission contracts."""
    cards = tuple(card.casefold() for card in free_by_accelerator)
    pool_key = reserved_capacity_broker.make_pool_key(
        'east',
        cards,
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='cluster-east')
    free_slots = sum(free_by_accelerator.values())
    if grant is None:
        grant = free_slots
    if edge_cap is None:
        edge_cap = grant
    snapshot = reserved_fill_planner.PoolFillSnapshot.from_mapping({
        'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
        'pool_key': pool_key,
        'physical_cluster_uid': 'cluster-east',
        'service_generation': 7,
        'worker_projection_sha256_by_accelerator': {
            card: f'{index + 1:064x}' for index, card in enumerate(cards)
        },
        'edge_cap': edge_cap,
        'broker_slot_width': accelerator_count,
        'free_slots': free_slots,
        'free_slots_by_accelerator': free_by_accelerator,
        'grant': grant,
        'grant_epoch': 11 if grant else None,
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
        utilization_gate_armed=utilization_gate_armed,
        utilization_demonstrated_need=utilization_demonstrated_need,
        utilization_demand_witness_sha256=(utilization_demand_witness_sha256),
        utilization_ceiling=utilization_ceiling,
        upward_grants_settled=upward_grants_settled,
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


def _materialize_current_allocation_pending(engine,
                                            intent_key: str,
                                            replica_id: int,
                                            *,
                                            accelerator: str = 'H200') -> None:
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
                                  accelerator=accelerator)
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
            projection.economic_capacity_graph_sha256),
        planner_supply=projection)
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


def test_current_planner_uses_demand_committed_before_service_lock(
        capacity_database):
    engine, incarnation, route_receipt = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=2))
    observed = []
    planning_fingerprint = (
        serve_state.get_scale_planning_state_fingerprint('svc'))
    assert planning_fingerprint is not None

    def _planner(snapshot, supply):
        observed.append(snapshot)
        assert supply.policy is capacity_admission.ReservedSupplyPolicy.DISABLED
        return _current_decision(snapshot,
                                 supply,
                                 2,
                                 source_fingerprint=planning_fingerprint)

    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=_planner,
            expected_planning_state_fingerprint=planning_fingerprint))
    authority = committed.authority
    snapshot = committed.demand_snapshot

    assert observed == [snapshot]
    assert snapshot.demand_feed_generation == 2
    assert snapshot.normalized_demand['recent_request_count'] == 2
    assert authority.demand_feed_generation == 2
    assert authority.remaining_launch_capacity() == {'l4': 2}


def test_current_planner_never_persists_paid_authority_for_reservation_only_card(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)

    repository = capacity_admission.CapacityAdmissionRepository(engine)

    def commit() -> capacity_admission.CommittedCapacityPlan:
        return repository.plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot, supply, 1, prospective_paid_accelerators=()))

    committed = commit()
    duplicate = commit()
    payload = _capacity_plan_payload(engine, committed.authority.generation)
    assert committed.candidate.supply_aware_demand_target.as_dict() == {'l4': 1}
    assert committed.candidate.paid_residual.total() == 0
    assert committed.authority.remaining_launch_capacity() == {}
    assert duplicate.authority.generation == committed.authority.generation
    assert duplicate.authority.remaining_launch_capacity() == {}
    assert payload['capacity_target_by_accelerator'] == {'l4': 1}
    assert payload['paid_residual_by_accelerator'] == {}


def test_current_planner_persists_one_candidate_without_recomputing_or_livelock(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    repository = capacity_admission.CapacityAdmissionRepository(engine)

    def commit() -> capacity_admission.CommittedCapacityPlan:
        return repository.plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot, supply, 1, prospective_paid_accelerators=()))

    committed = commit()
    duplicate = commit()
    payload = _capacity_plan_payload(engine, committed.authority.generation)

    assert committed.candidate.supply_aware_demand_target.as_dict() == {'l4': 1}
    assert committed.candidate.paid_residual.total() == 0
    assert committed.authority.remaining_launch_capacity() == {}
    assert duplicate.authority.generation == committed.authority.generation
    assert payload['paid_residual_by_accelerator'] == {}
    assert payload['planner']['candidate']['paid_residual']['entries'] == []


def test_current_planner_rejects_stale_prepared_fingerprint(capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    planning_fingerprint = (
        serve_state.get_scale_planning_state_fingerprint('svc'))
    assert planning_fingerprint is not None
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **_replica_values(101, zero_cost=False)))
    planner = mock.Mock(side_effect=lambda snapshot, supply: _current_decision(
        snapshot, supply, 1, source_fingerprint=planning_fingerprint))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='Prepared planning state changed'):
        (capacity_admission.CapacityAdmissionRepository(
            engine).plan_and_publish_current(
                service_name='svc',
                service_hash='svc-hash',
                service_lifecycle_epoch=3,
                service_version=1,
                accounting_cards={'l4': 0},
                sequenced_reserved_fill=False,
                planner=planner,
                expected_planning_state_fingerprint=planning_fingerprint))

    planner.assert_not_called()


def test_current_planner_accepts_semantic_noop_replica_rewrite(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **_replica_values(101, zero_cost=False)))
    planning_fingerprint = (
        serve_state.get_scale_planning_state_fingerprint('svc'))
    assert planning_fingerprint is not None
    revision = sqlalchemy.literal_column('xmin::text').label('revision')
    with engine.connect() as connection:
        before_revision = connection.execute(
            sqlalchemy.select(revision).select_from(
                serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id ==
                    101)).scalar_one()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 101).values(
                    replica_state=(
                        serve_state_schema.replicas_table.c.replica_state)))
    with engine.connect() as connection:
        after_revision = connection.execute(
            sqlalchemy.select(revision).select_from(
                serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id ==
                    101)).scalar_one()
    assert after_revision != before_revision
    assert (serve_state.get_scale_planning_state_fingerprint('svc') ==
            planning_fingerprint)
    planner = mock.Mock(side_effect=lambda snapshot, supply: _current_decision(
        snapshot, supply, 1, source_fingerprint=planning_fingerprint))

    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=planner,
            expected_planning_state_fingerprint=planning_fingerprint))
    authority = committed.authority

    planner.assert_called_once()
    assert not authority.remaining_launch_capacity()


def test_current_planner_serializes_concurrent_report_writer(capacity_database):
    engine, incarnation, route_receipt = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    writer_started = threading.Event()
    writer_finished = threading.Event()
    writer_errors = []
    writer_thread = None

    def _writer():
        writer_started.set()
        try:
            demand_state.ingest_report(
                'svc', 'svc-hash',
                _demand_report(time.time(),
                               route_receipt,
                               sequence=2,
                               request_count=3))
        except Exception as error:  # pylint: disable=broad-except
            writer_errors.append(error)
        finally:
            writer_finished.set()

    def _planner(snapshot, supply):
        nonlocal writer_thread
        assert supply.policy is capacity_admission.ReservedSupplyPolicy.DISABLED
        assert snapshot.demand_feed_generation == 1
        writer_thread = threading.Thread(target=_writer, daemon=True)
        writer_thread.start()
        assert writer_started.wait(timeout=2)
        # The reporter takes the service row first.  It cannot publish the
        # next generation while this transaction owns that same row.
        assert not writer_finished.wait(timeout=0.25)
        return _current_decision(snapshot, supply, 1)

    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(service_name='svc',
                                         service_hash='svc-hash',
                                         service_lifecycle_epoch=3,
                                         service_version=1,
                                         accounting_cards={'l4': 0},
                                         sequenced_reserved_fill=False,
                                         planner=_planner))
    authority = committed.authority
    snapshot = committed.demand_snapshot
    assert writer_thread is not None
    writer_thread.join(timeout=5)

    assert not writer_thread.is_alive()
    assert not writer_errors
    assert writer_finished.is_set()
    assert snapshot.demand_feed_generation == 1
    assert authority.demand_feed_generation == 1
    current = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert current is not None
    assert current.demand_feed_generation == 2
    assert current.normalized_demand['recent_request_count'] == 3


def test_successor_plan_publisher_serializes_with_prospective_admission(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    first = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 1))
    old_claim = first.authority.claim_values('L4')
    publisher_holds_service = threading.Event()
    release_publisher = threading.Event()
    publisher_done = threading.Event()
    validator_started = threading.Event()
    validator_done = threading.Event()
    publisher_results = []
    publisher_errors = []
    validator_errors = []

    def _successor_planner(snapshot, supply):
        publisher_holds_service.set()
        assert release_publisher.wait(timeout=5)
        return _current_decision(snapshot, supply, 2)

    def _publish_successor():
        try:
            publisher_results.append(
                repository.plan_and_publish_current(
                    service_name='svc',
                    service_hash='svc-hash',
                    service_lifecycle_epoch=3,
                    service_version=1,
                    accounting_cards={'l4': 0},
                    sequenced_reserved_fill=False,
                    planner=_successor_planner))
        except Exception as error:  # pylint: disable=broad-except
            publisher_errors.append(error)
        finally:
            publisher_done.set()

    def _validate_old_claim():
        validator_started.set()
        try:
            _validate_prospective_claim(engine, old_claim)
        except Exception as error:  # pylint: disable=broad-except
            validator_errors.append(error)
        finally:
            validator_done.set()

    publisher_thread = threading.Thread(target=_publish_successor)
    publisher_thread.start()
    assert publisher_holds_service.wait(timeout=5)
    validator_thread = threading.Thread(target=_validate_old_claim)
    validator_thread.start()
    assert validator_started.wait(timeout=5)
    # The successor owns the service/head publication prefix.  Prospective
    # admission cannot pass the old head while that transaction is open.
    assert not validator_done.wait(timeout=0.2)
    release_publisher.set()
    assert publisher_done.wait(timeout=5)
    assert validator_done.wait(timeout=5)
    publisher_thread.join(timeout=5)
    validator_thread.join(timeout=5)

    assert not publisher_thread.is_alive()
    assert not validator_thread.is_alive()
    assert not publisher_errors
    assert len(publisher_results) == 1
    assert publisher_results[0].authority.generation == (
        first.authority.generation + 1)
    assert len(validator_errors) == 1
    assert isinstance(validator_errors[0],
                      capacity_admission.CapacityAdmissionConflict)
    assert 'current fresh capacity-plan authority' in str(validator_errors[0])


def test_current_planner_callback_failure_rolls_back(capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)

    def _planner(_snapshot, _supply):
        raise ValueError('injected planner failure')

    with pytest.raises(ValueError, match='injected planner failure'):
        (capacity_admission.CapacityAdmissionRepository(
            engine).plan_and_publish_current(service_name='svc',
                                             service_hash='svc-hash',
                                             service_lifecycle_epoch=3,
                                             service_version=1,
                                             accounting_cards={'l4': 0},
                                             sequenced_reserved_fill=False,
                                             planner=_planner))
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                capacity_admission_schema.serve_capacity_plans_table)
        ).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                capacity_admission_schema.serve_capacity_plan_heads_table)
        ).scalar_one() == 0


def test_gate_covered_plan_commits_reservation_before_paid_residual(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=True,
                           utilization_gate=True)
    acquiring_allocation = _allocation_map({'l4': 1},
                                           utilization_gate_armed=True,
                                           utilization_demonstrated_need=2,
                                           utilization_ceiling=2)
    _mock_current_allocation(monkeypatch, acquiring_allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    acquiring_supply = repository.project_reserved_supply(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        authority=capacity_admission.ReservedFillPlanAuthority.bound(
            acquiring_allocation.identity))
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    acquisition = _current_decision(snapshot, acquiring_supply, 2)
    _, acquisition_candidate = acquisition.decode_planner()
    assert acquisition_candidate.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    witness = acquisition_candidate.demand_witness_sha256
    assert witness is not None
    allocation = _allocation_map({'l4': 1},
                                 utilization_gate_armed=True,
                                 utilization_demonstrated_need=2,
                                 utilization_demand_witness_sha256=witness,
                                 utilization_ceiling=2)
    _mock_current_allocation(monkeypatch, allocation)
    observed_supply = []

    def _planner(snapshot, supply):
        assert not snapshot.fresh_aggregate_zero
        assert supply is not None
        observed_supply.append(supply)
        assert supply.allocation_reserved_capacity_by_accelerator == {'l4': 1}
        return _current_decision(snapshot, supply, 2)

    committed = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=_planner)
    authority = committed.authority

    assert len(observed_supply) == 1
    assert committed.candidate.new_reserved_capacity_committed.as_dict() == {
        'l4': 1
    }
    assert committed.candidate.paid_residual.as_dict() == {'l4': 1}
    assert authority.remaining_launch_capacity() == {'l4': 1}
    assert (
        authority.reserved_fill_authority.mode
        is capacity_admission.ReservedFillPlanAuthorityMode.ALLOCATION_BOUND)


def test_stale_grant_holdings_cannot_unlock_paid_before_reserved_feed(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=True,
                           utilization_gate=True)
    current_allocation = [
        _allocation_map({'l4': 0},
                        utilization_gate_armed=True,
                        utilization_demonstrated_need=2,
                        utilization_ceiling=2)
    ]
    _mock_current_allocation(monkeypatch, lambda: current_allocation[0])
    repository = capacity_admission.CapacityAdmissionRepository(engine)

    acquisition = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    assert acquisition.candidate.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    witness = acquisition.candidate.demand_witness_sha256
    assert witness is not None

    # The broker grant may still be backed only by a stale claim heartbeat.
    # With no current service row and no feed, it is entitlement rather than
    # spendable reserved supply and must not authorize the paid residual.
    current_allocation[0] = _allocation_map(
        {'l4': 0},
        grant=1,
        edge_cap=1,
        utilization_gate_armed=True,
        utilization_demonstrated_need=2,
        utilization_demand_witness_sha256=witness,
        utilization_ceiling=2)
    blocked = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    assert blocked.candidate.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    assert blocked.candidate.reserved_launch_target.total() == 0
    assert blocked.candidate.paid_launch_target.total() == 0

    # Once the reclaimed slot is visible in the same locked projection, the
    # plan atomically commits it before admitting only the genuine residual.
    current_allocation[0] = _allocation_map(
        {'l4': 1},
        utilization_gate_armed=True,
        utilization_demonstrated_need=2,
        utilization_demand_witness_sha256=witness,
        utilization_ceiling=2)
    released = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    assert released.candidate.new_reserved_capacity_committed.as_dict() == {
        'l4': 1
    }
    assert released.candidate.paid_residual.as_dict() == {'l4': 1}
    assert released.authority.remaining_launch_capacity() == {'l4': 1}


def test_ungated_stale_grant_cannot_unlock_paid_before_reserved_feed(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=True,
                           utilization_gate=False)
    current_allocation = [_allocation_map({'l4': 0}, grant=1, edge_cap=1)]
    _mock_current_allocation(monkeypatch, lambda: current_allocation[0])
    repository = capacity_admission.CapacityAdmissionRepository(engine)

    blocked_supply = repository.project_reserved_supply(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        authority=capacity_admission.ReservedFillPlanAuthority.bound(
            current_allocation[0].identity))
    assert blocked_supply.policy is (
        capacity_admission.ReservedSupplyPolicy.STATIC_PREFILL)
    assert blocked_supply.evidence_state is (
        capacity_admission.ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED)
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    blocked_candidate = _current_decision(snapshot,
                                          blocked_supply,
                                          2,
                                          return_candidate=True)
    assert isinstance(blocked_candidate,
                      capacity_planning.CapacityPlanCandidate)
    assert blocked_candidate.kind is (
        capacity_planning.CapacityPlanKind.INCOMPLETE)
    assert not blocked_candidate.attribution_complete
    assert blocked_candidate.reserved_launch_target.total() == 0
    assert blocked_candidate.paid_launch_target.total() == 0

    current_allocation[0] = _allocation_map({'l4': 1})
    released = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    assert released.candidate.new_reserved_capacity_committed.as_dict() == {
        'l4': 1
    }
    assert released.candidate.paid_residual.as_dict() == {'l4': 1}
    assert released.authority.remaining_launch_capacity() == {'l4': 1}


def test_fill_demand_witness_rebinds_equivalent_heartbeat(
        capacity_database, monkeypatch):
    engine, incarnation, route_receipt = capacity_database
    # A fresh aggregate-zero proof is only authoritative for the current
    # projected-route protocol.  The shared fixture intentionally retains a
    # protocol-1 cohort for compatibility tests, so select protocol 2 for this
    # current-path regression before publishing the plan.
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
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=True,
                           utilization_gate=True)
    allocation = _allocation_map({'l4': 1},
                                 utilization_gate_armed=True,
                                 utilization_demonstrated_need=2,
                                 utilization_ceiling=2)
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    committed = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    assert committed.candidate.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    initial = repository.read_current_fill_demand_witness('svc',
                                                          'svc-hash',
                                                          (123, '10.0.0.5'),
                                                          max_age_seconds=60)
    assert initial is not None

    demand_state.ingest_report(
        'svc', 'svc-hash', _demand_report(time.time(),
                                          route_receipt,
                                          sequence=2))
    refreshed = repository.read_current_fill_demand_witness('svc',
                                                            'svc-hash',
                                                            (123, '10.0.0.5'),
                                                            max_age_seconds=60)

    assert refreshed is not None
    assert refreshed.demand_feed_generation > initial.demand_feed_generation
    assert refreshed.capacity_plan_generation == initial.capacity_plan_generation
    assert refreshed.semantic_sha256 == initial.semantic_sha256
    assert refreshed.target_capacity == initial.target_capacity
    assert refreshed.reservation_acquisition_classes == (
        initial.reservation_acquisition_classes)

    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=3, request_count=2))
    assert repository.read_current_fill_demand_witness(
        'svc', 'svc-hash', (123, '10.0.0.5'), max_age_seconds=60) is None

    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=4, request_count=0))
    zero_snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert zero_snapshot is not None
    assert zero_snapshot.fresh_aggregate_zero
    assert repository.read_current_fill_demand_witness(
        'svc', 'svc-hash', (123, '10.0.0.5'), max_age_seconds=60) is None


def test_gate_disabled_still_uses_current_reservation_without_witness(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=True,
                           utilization_gate=False)
    allocation = _allocation_map({'l4': 1})
    _mock_current_allocation(monkeypatch, allocation)

    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=True,
            planner=lambda snapshot, supply: _current_decision(
                snapshot, supply, 2)))

    assert committed.candidate.new_reserved_capacity_committed.as_dict() == {
        'l4': 1
    }
    assert committed.candidate.paid_residual.as_dict() == {'l4': 1}
    assert committed.authority.remaining_launch_capacity() == {'l4': 1}


@pytest.mark.parametrize(('utilization_gate', 'allocation_kwargs'), ((True, {
    'utilization_gate_armed': True,
    'utilization_demonstrated_need': 2,
    'utilization_ceiling': 2,
}), (False, {})),
                         ids=('utilization-gated', 'ungated'))
def test_reserved_then_paid_liveness_uses_immutable_gate_policy(
        capacity_database, monkeypatch, utilization_gate, allocation_kwargs):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=True,
                           utilization_gate=utilization_gate)
    allocation = _allocation_map({'l4': 1}, **allocation_kwargs)
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    observed_supply = []

    def _planner(snapshot, supply):
        assert supply is not None
        observed_supply.append(supply)
        return _current_decision(snapshot, supply, 2)

    acquisition = None
    if utilization_gate:
        acquisition = repository.plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=True,
            planner=_planner)
        assert acquisition.candidate.kind is (
            capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
        assert acquisition.candidate.reserved_launch_target.total() == 0
        assert acquisition.candidate.paid_launch_target.total() == 0
        witness = acquisition.candidate.demand_witness_sha256
        assert witness is not None
        allocation = _allocation_map({'l4': 1},
                                     **allocation_kwargs,
                                     utilization_demand_witness_sha256=witness)
        _mock_current_allocation(monkeypatch, allocation)

    first = repository.plan_and_publish_current(service_name='svc',
                                                service_hash='svc-hash',
                                                service_lifecycle_epoch=3,
                                                service_version=1,
                                                accounting_cards={'l4': 0},
                                                sequenced_reserved_fill=True,
                                                planner=_planner)

    assert first.candidate.new_reserved_capacity_committed.as_dict() == {
        'l4': 1
    }
    assert first.candidate.reserved_launch_target.as_dict() == {'l4': 1}
    assert first.candidate.paid_residual.as_dict() == {'l4': 1}
    assert first.authority.remaining_launch_capacity() == {'l4': 1}
    expected_policy = (capacity_admission.ReservedSupplyPolicy.DEMAND_GATED
                       if utilization_gate else
                       capacity_admission.ReservedSupplyPolicy.STATIC_PREFILL)
    assert all(supply.policy is expected_policy for supply in observed_supply)
    if acquisition is not None:
        assert first.authority.generation == acquisition.authority.generation + 1

    _insert_current_allocation_pending(engine, allocation)
    successor = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=_planner)

    assert len(observed_supply) == (3 if utilization_gate else 2)
    assert observed_supply[-1].policy is expected_policy
    assert observed_supply[-1].pending_zero_cost_capacity_by_accelerator == {
        'l4': 1
    }
    assert successor.authority.generation == first.authority.generation + 1
    assert successor.candidate.reserved_capacity_committed.as_dict() == {
        'l4': 1
    }
    assert successor.candidate.new_reserved_capacity_committed.total() == 0
    assert successor.candidate.reserved_launch_target.total() == 0
    assert successor.candidate.paid_residual.as_dict() == {'l4': 1}
    assert successor.authority.remaining_launch_capacity() == {'l4': 1}
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='no whole-backend paid launch authority'):
        successor.authority.claim_values('L4', units=2)

    claim = _insert_claim(engine, successor.authority, 210)
    assert claim['capacity_plan_accelerator'] == 'l4'
    assert claim['capacity_plan_units'] == 1
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='exceed'):
        _validate_prospective_claim(engine, {
            **successor.authority.claim_values('L4'),
            'replica_id': 211,
        })


@pytest.mark.parametrize('reserved_state', ('pending', 'existing'))
@pytest.mark.parametrize('allocation_state',
                         ('disappeared', 'held-grant', 'unsettled'))
def test_committed_reservation_survives_gated_entitlement_change(
        capacity_database, monkeypatch, reserved_state, allocation_state):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=True,
                           utilization_gate=True)
    initial = _allocation_map({'l4': 1},
                              utilization_gate_armed=True,
                              utilization_demonstrated_need=2,
                              utilization_ceiling=2)
    current_allocation = [initial]
    _mock_current_allocation(monkeypatch, lambda: current_allocation[0])
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    acquisition = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    assert acquisition.candidate.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    witness = acquisition.candidate.demand_witness_sha256
    assert witness is not None
    initial = _allocation_map({'l4': 1},
                              utilization_gate_armed=True,
                              utilization_demonstrated_need=2,
                              utilization_demand_witness_sha256=witness,
                              utilization_ceiling=2)
    current_allocation[0] = initial
    first = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    assert first.candidate.new_reserved_capacity_committed.as_dict() == {
        'l4': 1
    }

    intent_key = _insert_current_allocation_pending(engine, initial)
    if reserved_state == 'existing':
        _materialize_current_allocation_pending(engine,
                                                intent_key,
                                                212,
                                                accelerator='L4')
    if allocation_state == 'disappeared':
        current_allocation[0] = _allocation_map(
            {'l4': 0},
            utilization_gate_armed=True,
            utilization_demonstrated_need=2,
            utilization_demand_witness_sha256=witness,
            utilization_ceiling=2)
    elif allocation_state == 'held-grant':
        current_allocation[0] = _allocation_map(
            {'l4': 0},
            grant=1,
            edge_cap=1,
            utilization_gate_armed=True,
            utilization_demonstrated_need=2,
            utilization_demand_witness_sha256=witness,
            utilization_ceiling=2)
    else:
        current_allocation[0] = _allocation_map(
            {'l4': 1},
            utilization_gate_armed=True,
            utilization_demonstrated_need=2,
            utilization_demand_witness_sha256=witness,
            utilization_ceiling=2,
            upward_grants_settled=False)

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='exact current reserved-fill allocation'):
        _validate_prospective_claim(engine, {
            **first.authority.claim_values('L4'),
            'pool_key': _paid_pool_key(),
        })

    observed = []

    def _successor_planner(snapshot, supply):
        assert supply is not None
        decision = _current_decision(snapshot, supply, 2)
        _, candidate = decision.decode_planner()
        observed.append((supply, candidate))
        return decision

    def _publish_successor():
        return repository.plan_and_publish_current(service_name='svc',
                                                   service_hash='svc-hash',
                                                   service_lifecycle_epoch=3,
                                                   service_version=1,
                                                   accounting_cards={'l4': 0},
                                                   sequenced_reserved_fill=True,
                                                   planner=_successor_planner)

    successor = _publish_successor()

    assert len(observed) == 1
    supply, candidate = observed[0]
    assert supply.policy is capacity_admission.ReservedSupplyPolicy.DEMAND_GATED
    expected_evidence = (
        capacity_admission.ReservedSupplyEvidenceState.AUTHENTICATED_SETTLED
        if allocation_state in ('disappeared', 'held-grant') else
        capacity_admission.ReservedSupplyEvidenceState.AUTHENTICATED_UNSETTLED)
    assert supply.evidence_state is expected_evidence
    expected_pending = 1 if reserved_state == 'pending' else 0
    expected_existing = 1 if reserved_state == 'existing' else 0
    assert supply.pending_zero_cost_capacity_by_accelerator.get(
        'l4', 0) == expected_pending
    assert supply.existing_zero_cost_capacity_by_accelerator.get(
        'l4', 0) == expected_existing
    if allocation_state in ('disappeared', 'held-grant'):
        assert candidate.reserved_capacity_committed.as_dict() == {'l4': 1}
        assert candidate.new_reserved_capacity_committed.total() == 0
        assert candidate.reserved_launch_target.total() == 0
        assert candidate.paid_residual.as_dict() == {'l4': 1}
    else:
        assert candidate.kind is (
            capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
        assert candidate.reserved_launch_target.total() == 0
        assert candidate.paid_launch_target.total() == 0

    assert successor.authority.generation == first.authority.generation + 1
    if allocation_state in ('disappeared', 'held-grant'):
        assert successor.authority.remaining_launch_capacity() == {'l4': 1}
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='no whole-backend paid launch authority'):
            successor.authority.claim_values('L4', units=2)
        _validate_prospective_claim(engine, {
            **successor.authority.claim_values('L4'),
            'pool_key': _paid_pool_key(),
        })
    else:
        # An unsettled observer publishes a durable effect-free acquisition
        # generation.  This revokes the stale paid authority while preserving
        # the already materialized reservation rows outside the plan ledger.
        assert successor.candidate.kind is (
            capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
        assert successor.authority.remaining_launch_capacity() == {}
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='no whole-backend paid launch authority'):
            successor.authority.claim_values('L4')
        with engine.connect() as connection:
            # Superseded unclaimed plan rows are collected; only the new
            # effect-free head remains.  The reservation intent itself is
            # independent durable state and must survive that collection.
            retained_plan = connection.execute(
                sqlalchemy.select(
                    capacity_admission_schema.serve_capacity_plans_table.c.
                    generation, capacity_admission_schema.
                    serve_capacity_plans_table.c.content_sha256)).one()
            head = connection.execute(
                sqlalchemy.select(
                    capacity_admission_schema.serve_capacity_plan_heads_table.c.
                    generation)).one()
            expected_head = (successor.authority.generation,
                             successor.authority.content_sha256)
            assert tuple(retained_plan) == expected_head
            assert tuple(head) == (successor.authority.generation,)
            intent = connection.execute(
                sqlalchemy.select(
                    zero_cost_actuation_schema.
                    serve_zero_cost_actuation_intents_table.c.state).where(
                        zero_cost_actuation_schema.
                        serve_zero_cost_actuation_intents_table.c.
                        intent_idempotency_key == intent_key)).scalar_one()
            assert intent == ('GRANTED'
                              if reserved_state == 'pending' else 'COMMITTED')


@pytest.mark.parametrize('allocation_kwargs', ({
    'utilization_gate_armed': False,
    'utilization_demonstrated_need': None,
    'utilization_ceiling': 0,
}, {
    'utilization_gate_armed': True,
    'utilization_demonstrated_need': None,
    'utilization_ceiling': 2,
}, {
    'utilization_gate_armed': True,
    'utilization_demonstrated_need': 2,
    'utilization_ceiling': 2,
    'upward_grants_settled': False,
}, {
    'utilization_gate_armed': True,
    'utilization_demonstrated_need': 1,
    'utilization_ceiling': 2,
}, {
    'utilization_gate_armed': True,
    'utilization_demonstrated_need': 2,
    'utilization_ceiling': 1,
}, {
    'utilization_gate_armed': True,
    'utilization_demonstrated_need': 2,
    'utilization_demand_witness_sha256': '0' * 64,
    'utilization_ceiling': 2,
}),
                         ids=('unarmed', 'blind', 'unsettled', 'need-too-small',
                              'ceiling-too-small', 'wrong-witness'))
def test_usage_gate_publishes_no_effect_acquisition_for_noncausal_evidence(
        capacity_database, monkeypatch, allocation_kwargs):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=True,
                           utilization_gate=True)
    current_allocation = [
        _allocation_map({'l4': 1},
                        utilization_gate_armed=True,
                        utilization_demonstrated_need=2,
                        utilization_ceiling=2)
    ]
    _mock_current_allocation(monkeypatch, lambda: current_allocation[0])
    repository = capacity_admission.CapacityAdmissionRepository(engine)

    bootstrap = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    assert bootstrap.candidate.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    exact_witness = bootstrap.candidate.demand_witness_sha256
    assert exact_witness is not None
    current_allocation[0] = _allocation_map(
        {'l4': 1},
        utilization_gate_armed=True,
        utilization_demonstrated_need=2,
        utilization_demand_witness_sha256=exact_witness,
        utilization_ceiling=2)
    positive = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    assert positive.candidate.kind is capacity_planning.CapacityPlanKind.DEMAND
    assert positive.authority.remaining_launch_capacity() == {'l4': 1}

    noncausal_kwargs = dict(allocation_kwargs)
    # Exercise the distinct causal failures precisely.  Armed cases with a
    # numeric need use the exact committed witness unless the parameter is the
    # explicit wrong-witness case; otherwise a missing witness would make the
    # smaller-need/ceiling and unsettled rows indistinguishable from `blind`.
    if (noncausal_kwargs.get('utilization_gate_armed') and
            noncausal_kwargs.get('utilization_demonstrated_need') is not None
            and 'utilization_demand_witness_sha256' not in noncausal_kwargs):
        noncausal_kwargs['utilization_demand_witness_sha256'] = exact_witness
    current_allocation[0] = _allocation_map({'l4': 1}, **noncausal_kwargs)
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='exact current reserved-fill allocation'):
        _validate_prospective_claim(engine,
                                    positive.authority.claim_values('L4'))

    committed = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={'l4': 0},
        sequenced_reserved_fill=True,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2))
    candidate = committed.candidate
    assert candidate.kind is (
        capacity_planning.CapacityPlanKind.GATE_ACQUISITION)
    assert candidate.demand_witness_sha256 == exact_witness
    assert candidate.next_policy_state is None
    no_effect_fields = (
        'supply_aware_demand_target',
        'reserved_capacity_committed',
        'new_reserved_capacity_committed',
        'reserved_launch_target',
        'reserved_packing_padding_target',
        'paid_residual',
        'paid_launch_target',
        'paid_packing_padding_target',
        'zero_cost_padding_target',
        'static_prefill_target',
        'retained_existing_target',
        'transition_retention_target',
        'wave_limited_actuation_target',
        'supply_aware_actuation_target',
        'explicit_demand_attribution',
        'paid_demand_attribution',
        'warm_retention_target',
        'deadline_target',
        'retirement_floor_target',
    )
    assert all(
        getattr(candidate, field).total() == 0 for field in no_effect_fields)
    assert committed.authority.remaining_launch_capacity() == {}
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='no whole-backend paid launch authority'):
        committed.authority.claim_values('L4')
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='current fresh capacity-plan authority'):
        _validate_prospective_claim(engine,
                                    positive.authority.claim_values('L4'))

    with engine.connect() as connection:
        plan = connection.execute(
            sqlalchemy.select(capacity_admission_schema.
                              serve_capacity_plans_table)).mappings().one()
        head = connection.execute(
            sqlalchemy.select(
                capacity_admission_schema.serve_capacity_plan_heads_table)
        ).mappings().one()
        assert plan['generation'] == committed.authority.generation
        assert plan['content_sha256'] == committed.authority.content_sha256
        assert head['generation'] == committed.authority.generation
        assert plan['payload']['planner']['candidate'][
            'demand_witness_sha256'] == exact_witness
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table)).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one() == 0


@pytest.mark.parametrize('utilization_gate', [False, True])
def test_exact_paid_plan_ignores_only_statically_incompatible_reserved_cards(
        capacity_database, monkeypatch, utilization_gate):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=True,
                           utilization_gate=utilization_gate)
    h200_projection = copy.deepcopy(_CAPACITY_KUEUE_PROJECTION)
    h200_projection['accelerator_name'] = 'H200'
    h200_projection['accelerator_scheduling']['label_values'] = ['NVIDIA-H200']
    projections = [h200_projection]
    projection_digest = capacity_admission._sha256(projections)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    worker_placement_projections=projections))

    current_allocation = [_allocation_map({
        'H200': 0,
    }, grant=0, edge_cap=0)]
    _mock_current_allocation(monkeypatch, lambda: current_allocation[0])

    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={
                'h200': 0,
                'l4': 0
            },
            sequenced_reserved_fill=True,
            planner=lambda snapshot, supply:
            (_current_decision(snapshot, supply, 1, accelerator='L4')
             if supply is not None else pytest.fail(
                 'static authority still requires the locked projection'))))
    authority = committed.authority

    assert authority.remaining_launch_capacity() == {'l4': 1}
    assert (authority.reserved_fill_authority.mode is capacity_admission.
            ReservedFillPlanAuthorityMode.STATICALLY_INCOMPATIBLE)
    assert authority.reserved_fill_authority.incompatible_accelerators == (
        'l4',)
    assert (authority.reserved_fill_authority.worker_projection_sha256 ==
            projection_digest)
    assert committed.planner_snapshot.reservation.evidence_state is (
        capacity_planning.ReservationEvidenceState.AUTHENTICATED_UNSETTLED
        if utilization_gate else
        capacity_planning.ReservationEvidenceState.AUTHENTICATED_SETTLED)

    # Static incompatibility is projection-bound, not allocation-bound.  An
    # unrelated H200 broker generation may change before the paid L4 claim.
    current_allocation[0] = _allocation_map({
        'H200': 2,
    }, grant=2, edge_cap=2)
    _validate_prospective_claim(engine, {
        **authority.claim_values('l4'),
        'pool_key': _paid_pool_key(),
    })

    # The worker projection is immutable in production. Simulate corruption to
    # prove a previously issued claim fails closed if that fence ever changes.
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    worker_placement_projections=[_CAPACITY_KUEUE_PROJECTION]))
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='no longer proves static incompatibility'):
        _validate_prospective_claim(engine, {
            **authority.claim_values('l4'),
            'pool_key': _paid_pool_key(),
        })


def test_flexible_demand_cannot_bypass_reserved_allocation_by_landing_on_l4(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=True)
    h200_projection = copy.deepcopy(_CAPACITY_KUEUE_PROJECTION)
    h200_projection['accelerator_name'] = 'H200'
    h200_projection['accelerator_scheduling']['label_values'] = ['NVIDIA-H200']
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    worker_placement_projections=[h200_projection]))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='no current reserved allocation'):
        capacity_admission.CapacityAdmissionRepository(
            engine).plan_and_publish_current(
                service_name='svc',
                service_hash='svc-hash',
                service_lifecycle_epoch=3,
                service_version=1,
                accounting_cards={
                    'l4': 0,
                    'h200': 0
                },
                sequenced_reserved_fill=True,
                planner=lambda snapshot, supply: _current_decision(
                    snapshot,
                    supply,
                    1,
                    target_by_accelerator={
                        'l4': 1,
                        'h200': 0
                    },
                    compatible_accelerators=('l4', 'h200'),
                    cold_accelerator_order=('l4', 'h200')))


@pytest.mark.parametrize('target', ({'h200': 1}, {'*': 1}))
def test_compatible_or_flexible_paid_plan_requires_current_allocation(
        capacity_database, target):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=True)
    h200_projection = copy.deepcopy(_CAPACITY_KUEUE_PROJECTION)
    h200_projection['accelerator_name'] = 'H200'
    h200_projection['accelerator_scheduling']['label_values'] = ['NVIDIA-H200']
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    worker_placement_projections=[h200_projection]))

    accelerator = next(iter(target))
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='no current reserved allocation'):
        capacity_admission.CapacityAdmissionRepository(
            engine).plan_and_publish_current(
                service_name='svc',
                service_hash='svc-hash',
                service_lifecycle_epoch=3,
                service_version=1,
                accounting_cards={card: 0 for card in target},
                sequenced_reserved_fill=True,
                planner=lambda snapshot, supply: _current_decision(
                    snapshot, supply, 1, accelerator=accelerator))

    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                capacity_admission_schema.serve_capacity_plans_table)
        ).scalar_one() == 0


@pytest.mark.parametrize(
    'row_status, down_status, expected_existing, expected_residual',
    [('SHUTTING_DOWN', 'SCHEDULED', 1, 0),
     ('SHUTTING_DOWN', 'SUCCEEDED', 0, 1)])
def test_shutting_down_paid_row_leaves_baseline_only_after_cleanup_proof(
        capacity_database, row_status, down_status, expected_existing,
        expected_residual):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    replica = _replica_values(101, zero_cost=False)
    replica['status'] = row_status
    replica['replica_state']['status_property'][
        'sky_launch_status'] = 'SUCCEEDED'
    replica['replica_state']['status_property']['is_scale_down'] = True
    replica['replica_state']['status_property']['sky_down_status'] = down_status
    replica['sky_down_status'] = down_status
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**replica))

    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot, supply, 1)))
    authority = committed.authority
    payload = _capacity_plan_payload(engine, authority.generation)

    assert payload['existing_paid_capacity_by_accelerator'] == {
        'l4': expected_existing
    }
    assert payload['paid_residual_by_accelerator'] == ({
        'l4': expected_residual
    } if expected_residual else {})
    assert authority.remaining_launch_capacity() == ({
        'l4': expected_residual
    } if expected_residual else {})


@pytest.mark.parametrize('down_status, expected_charged, expected_launch',
                         [('SCHEDULED', 1, 0), ('SUCCEEDED', 0, 1)])
def test_old_version_paid_row_charges_cap_without_covering_current_demand(
        capacity_database, down_status, expected_charged, expected_launch):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    replica = _replica_values(102, zero_cost=False)
    replica['version'] = 2
    replica['replica_state']['version'] = 2
    replica['status'] = 'SHUTTING_DOWN'
    replica['replica_state']['status_property'][
        'sky_launch_status'] = 'SUCCEEDED'
    replica['replica_state']['status_property']['is_scale_down'] = True
    replica['replica_state']['status_property']['sky_down_status'] = down_status
    replica['sky_down_status'] = down_status
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**replica))

    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot, supply, 1, max_live_paid_gpu_units=1)))

    assert committed.candidate.paid_residual.as_dict() == {'l4': 1}
    assert committed.candidate.paid_cap.charged_paid_gpu_units == (
        expected_charged)
    assert committed.candidate.paid_launch_target.as_dict() == ({
        'l4': expected_launch
    } if expected_launch else {})
    assert committed.authority.remaining_launch_capacity() == ({
        'l4': expected_launch
    } if expected_launch else {})


@pytest.mark.parametrize(
    ('replica_unit', 'capacity_unit', 'physical_width', 'planned_capacity',
     'num_nodes', 'target', 'cap', 'expected_existing', 'expected_charged',
     'expected_launch'),
    [('logical', capacity_planning.CapacityUnit.LOGICAL_GPU, 4, 4, 1, 8, 8, 4,
      4, 4),
     ('physical_backend', capacity_planning.CapacityUnit.PHYSICAL_BACKEND, 8, 1,
      2, 2, 16, 1, 16, 0)])
def test_paid_cap_separates_service_units_from_physical_gpu_debit(
        capacity_database, replica_unit, capacity_unit, physical_width,
        planned_capacity, num_nodes, target, cap, expected_existing,
        expected_charged, expected_launch):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=False,
                           replica_unit=replica_unit)
    replica = _replica_values(104,
                              zero_cost=False,
                              accelerator_count=physical_width,
                              planned_capacity=planned_capacity,
                              num_nodes=num_nodes)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**replica))

    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot,
                supply,
                target,
                capacity_unit=capacity_unit,
                backend_num_nodes=num_nodes,
                physical_gpu_width_by_accelerator={'l4': physical_width},
                max_live_paid_gpu_units=cap)))

    payload = _capacity_plan_payload(engine, committed.authority.generation)
    assert payload['existing_paid_capacity_by_accelerator'] == {
        'l4': expected_existing
    }
    assert committed.candidate.paid_cap.charged_paid_gpu_units == (
        expected_charged)
    assert committed.candidate.paid_launch_target.as_dict() == ({
        'l4': expected_launch
    } if expected_launch else {})


def test_paid_cap_rejects_malformed_physical_width_attribution(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    replica = _replica_values(105,
                              zero_cost=False,
                              accelerator_count=8,
                              planned_capacity=1)
    replica['replica_state']['resources_override']['accelerators'] = {'L4': 4}
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**replica))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='physical GPU attribution is malformed'):
        capacity_admission.CapacityAdmissionRepository(
            engine).plan_and_publish_current(
                service_name='svc',
                service_hash='svc-hash',
                service_lifecycle_epoch=3,
                service_version=1,
                accounting_cards={'l4': 0},
                sequenced_reserved_fill=False,
                planner=lambda snapshot, supply: _current_decision(
                    snapshot,
                    supply,
                    1,
                    physical_gpu_width_by_accelerator={'l4': 8},
                    max_live_paid_gpu_units=8))


@pytest.mark.parametrize('contradiction',
                         ('pool_copy', 'missing_copy', 'missing_scalar',
                          'zero_cost_copy', 'cleanup_copy'))
def test_locked_paid_replica_relational_authority_rejects_json_contradiction(
        capacity_database, contradiction):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    replica = _replica_values(106,
                              zero_cost=False,
                              accelerator_count=8,
                              planned_capacity=1)
    if contradiction == 'pool_copy':
        replica['replica_state']['paid_capacity_pool_key'] = _paid_pool_key(
            accelerator_count=4)
    elif contradiction == 'missing_copy':
        replica['replica_state']['paid_capacity_pool_key'] = None
    elif contradiction == 'missing_scalar':
        replica['paid_capacity_pool_key'] = None
    elif contradiction == 'zero_cost_copy':
        replica['status'] = 'SHUTTING_DOWN'
        replica['replica_state']['status_property']['is_scale_down'] = True
        replica['replica_state']['is_zero_cost'] = True
    else:
        replica['sky_down_status'] = 'SCHEDULED'
        replica['replica_state']['status_property'][
            'sky_down_status'] = 'SUCCEEDED'
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**replica))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict):
        capacity_admission.CapacityAdmissionRepository(
            engine).plan_and_publish_current(
                service_name='svc',
                service_hash='svc-hash',
                service_lifecycle_epoch=3,
                service_version=1,
                accounting_cards={'l4': 0},
                sequenced_reserved_fill=False,
                planner=lambda snapshot, supply: _current_decision(
                    snapshot,
                    supply,
                    1,
                    backend_num_nodes=1,
                    physical_gpu_width_by_accelerator={'l4': 8},
                    max_live_paid_gpu_units=8))


def test_noncurrent_removed_reserved_card_is_retirement_only_inventory(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    replica = _replica_values(103, zero_cost=True, accelerator='L4')
    replica['version'] = 2
    replica['replica_state']['version'] = 2
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**replica))

    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'a100': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot, supply, 1, accelerator='a100')))

    payload = _capacity_plan_payload(engine, committed.authority.generation)
    assert payload['existing_zero_cost_capacity_by_accelerator'] == {'a100': 0}
    assert committed.candidate.paid_residual.as_dict() == {'a100': 1}
    assert committed.authority.remaining_launch_capacity() == {'a100': 1}


def test_disabled_fill_keeps_surviving_pending_intent_in_economic_baseline(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    _install_pending_east_capacity(engine)

    committed = capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(service_name='svc',
                                         service_hash='svc-hash',
                                         service_lifecycle_epoch=3,
                                         service_version=1,
                                         accounting_cards={'l4': 0},
                                         sequenced_reserved_fill=False,
                                         planner=lambda snapshot, supply:
                                         _current_decision(snapshot, supply, 1))
    payload = _capacity_plan_payload(engine, committed.authority.generation)

    assert payload['pending_zero_cost_capacity_by_accelerator'] == {'l4': 1}
    assert payload['paid_residual_by_accelerator'] == {}
    assert committed.candidate.reserved_capacity_committed.as_dict() == {
        'l4': 1
    }
    assert committed.candidate.new_reserved_capacity_committed.total() == 0
    assert committed.candidate.reserved_launch_target.total() == 0
    assert not committed.authority.remaining_launch_capacity()
    assert (committed.authority.reserved_fill_authority.mode
            is capacity_admission.ReservedFillPlanAuthorityMode.NOT_APPLICABLE)


def test_durable_unknown_replacement_requires_and_persists_plan_tuple(
        capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    replica_id = 301
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'svc-{replica_id}',
        replica_port='8000',
        is_spot=True,
        location=None,
        version=1,
        resources_override={'accelerators': {
            'L4': 1,
        }},
        unknown_capacity_replacement=True)
    admission_kwargs = {
        'pool_key': _paid_pool_key(),
        'priority': 50,
        'base_limit': 10,
        'max_limit': 10,
        'max_live_paid_gpu_units': 10,
        'now': None,
        'success_ttl_seconds': 60.0,
        'waiter_ttl_seconds': 60.0,
        'expected_controller_owner': (123, '10.0.0.5'),
    }

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='unbound paid claim'):
        serve_state.try_add_replica_with_paid_capacity_claim(
            'svc', 'svc-hash', replica_id, info, **admission_kwargs)
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.replica_id).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id ==
                    replica_id)).scalar_one_or_none() is None
        assert connection.execute(
            sqlalchemy.select(
                serve_state_schema.paid_capacity_claims_table.c.replica_id).
            where(
                serve_state_schema.paid_capacity_claims_table.c.service_name ==
                'svc',
                serve_state_schema.paid_capacity_claims_table.c.replica_id ==
                replica_id)).scalar_one_or_none() is None

    assert serve_state.try_add_replica_with_paid_capacity_claim(
        'svc',
        'svc-hash',
        replica_id,
        info,
        capacity_plan_claim=authority.claim_values('l4'),
        **admission_kwargs) == 'acquired'
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.replica_state,
                serve_state_schema.paid_capacity_claims_table.c.
                capacity_plan_generation,
                serve_state_schema.paid_capacity_claims_table.c.
                capacity_plan_sha256,
            ).join(
                serve_state_schema.paid_capacity_claims_table,
                sqlalchemy.and_(
                    serve_state_schema.paid_capacity_claims_table.c.service_name
                    == serve_state_schema.replicas_table.c.service_name,
                    serve_state_schema.paid_capacity_claims_table.c.replica_id
                    == serve_state_schema.replicas_table.c.replica_id)).where(
                        serve_state_schema.replicas_table.c.service_name ==
                        'svc', serve_state_schema.replicas_table.c.replica_id ==
                        replica_id)).mappings().one()
    assert row['replica_state']['unknown_capacity_replacement'] is True
    assert row['capacity_plan_generation'] == authority.generation
    assert row['capacity_plan_sha256'] == authority.content_sha256


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


def test_authority_splits_scale_up_from_occupancy_deadline(capacity_database):
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
    destructive_window = (authority.deadline_monotonic -
                          authority.read_started_monotonic)
    scale_up_window = (authority.scale_up_deadline_monotonic -
                       authority.read_started_monotonic)
    assert 0 < destructive_window <= 1
    assert 1 < scale_up_window <= constants.LB_DEMAND_REPORT_TTL_SECONDS
    assert authority.deadline_monotonic < (
        authority.scale_up_deadline_monotonic)


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


def test_logical_retirement_accepts_equivalent_retained_report(
        capacity_database):
    engine, incarnation, reported_route = capacity_database
    current_route = _publish_successor_route(engine, incarnation, 2)
    info, authority, _ = _prepare_logical_retirement(capacity_database)

    result = _commit_logical(info, authority)

    assert reported_route.generation < current_route.generation
    assert authority.route_generation == current_route.generation
    assert authority.route_sha256 == current_route.content_sha256
    assert result.state is serve_state.LogicalRetirementCommitState.COMMITTED


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


def test_logical_retirement_resamples_clock_after_route_context_hashing(
        capacity_database, monkeypatch):
    """Retained-route validation cannot run after the final expiry clock."""
    engine, incarnation, _ = capacity_database
    _publish_successor_route(engine, incarnation, 2)
    info, authority, _ = _prepare_logical_retirement(capacity_database)
    authority = dataclasses.replace(
        authority,
        valid_until=(datetime.datetime.now(datetime.timezone.utc) +
                     datetime.timedelta(milliseconds=50)))
    validate = demand_state.validate_report_route_contexts

    def _delayed_validation(*args, **kwargs):
        result = validate(*args, **kwargs)
        time.sleep(0.1)
        return result

    monkeypatch.setattr(demand_state, 'validate_report_route_contexts',
                        _delayed_validation)

    result = _commit_logical(info, authority)

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
                common_utils.ProcessStatus.SCHEDULED)
        assert serve_state.logical_retirement_commit_identity(
            durable) is not None


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
            common_utils.ProcessStatus.SCHEDULED)
    assert serve_state.logical_retirement_commit_identity(durable) is not None


def test_heartbeat_refresh_keeps_plan_and_bounded_claims(capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    first = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={
            'l4': 0
        },
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2
                                                          )).authority
    first_claim = _insert_claim(engine, first, 10)

    demand_state.ingest_report(
        'svc', 'svc-hash', _demand_report(time.time(),
                                          route_receipt,
                                          sequence=2))
    successor = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={
            'l4': 0
        },
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2
                                                          )).authority

    assert successor.generation == first.generation + 1
    assert successor.demand_feed_generation > first.demand_feed_generation
    assert successor.remaining_launch_capacity() == {'l4': 1}
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(
            connection, {
                **service, 'name': 'svc'
            }, first_claim)

    _insert_claim(engine, successor, 11)
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
                    **successor.claim_values('L4'),
                    'replica_id': 12,
                },
                prospective=True)


def test_normalized_demand_uses_json_replica_id_keys(capacity_database):
    """The semantic plan must be stable across its PostgreSQL JSONB roundtrip."""
    engine, incarnation, _ = capacity_database
    urls = ('http://replica-42:8000', 'http://replica-116:8000')
    record_ids = (str(uuid.uuid4()), str(uuid.uuid4()))
    response = _route_response()
    response['replica_info'] = {
        url: {
            'gpu_type': 'L4',
            'gpu_count': '1',
        } for url in urls
    }
    identities = {
        url: {
            'replica_id': replica_id,
            'replica_record_id': record_id,
            'service_version': 1,
            'gpu_type': 'L4',
            'gpu_count': 1,
            'advertised': True,
            'alias_expires_at': None,
        } for url, replica_id, record_id in zip(urls, (42, 116), record_ids)
    }
    route = route_projection.RouteProjectionRepository(engine).publish(
        route_projection.RoutePublisherIdentity(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            controller_incarnation=incarnation,
            controller_owner_epoch=4,
            controller_pid=123,
            controller_ip='10.0.0.5'),
        1,
        response,
        identities,
        set(record_ids),
        ttl_seconds=60)
    report = _demand_report(time.time(), route, sequence=2, request_count=2)
    report.update(
        http_in_flight={url: 1 for url in urls},
        async_occupancy={url: 0 for url in urls},
        occupancy_sample_generation={url: 2 for url in urls},
        occupancy_sample_age_seconds={url: 0.1 for url in urls},
        occupancy_sampled_urls=list(urls),
        total_slots_by_url={url: 1 for url in urls},
        routing_urls=list(urls),
    )
    demand_state.ingest_report('svc', 'svc-hash', report)
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    assert snapshot.request_information['in_flight_by_replica_id'] == {
        42: 1,
        116: 1,
    }
    assert snapshot.normalized_demand['in_flight_by_replica_id'] == {
        '42': 1,
        '116': 1,
    }

    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(2))
    with engine.begin() as connection:
        persisted = connection.execute(
            sqlalchemy.select(
                capacity_admission_schema.serve_capacity_plans_table.c.payload).
            where(capacity_admission_schema.serve_capacity_plans_table.c.
                  service_name == 'svc')).scalar_one()
        assert persisted['normalized_demand']['in_flight_by_replica_id'] == {
            '42': 1,
            '116': 1,
        }
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(connection, {
            **service, 'name': 'svc'
        }, {
            **authority.claim_values('L4'), 'replica_id': 42
        },
                                                             prospective=True)


def test_committed_claim_rechecks_immutable_plan_debit_ledger(
        capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(2))
    first_claim = _insert_claim(engine, authority, 13)
    _insert_claim(engine, authority, 14)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.paid_capacity_claims_table).where(
                    serve_state_schema.paid_capacity_claims_table.c.service_name
                    == 'svc',
                    serve_state_schema.paid_capacity_claims_table.c.replica_id
                    == 14).values(capacity_plan_units=2))
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='immutable plan debit'):
            capacity_admission.validate_paid_claim_in_connection(
                connection, service, first_claim, prospective=False)


def test_plan_publication_rebases_equivalent_heartbeat(capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan = _plan(1)
    plan = dataclasses.replace(plan,
                               normalized_demand={
                                   **plan.normalized_demand,
                                   'autoscaler_target': 1,
                                   'replica_unit': 'logical',
                               })

    receipt = demand_state.ingest_report(
        'svc', 'svc-hash', _demand_report(time.time(),
                                          route_receipt,
                                          sequence=2))
    authority = repository.publish(plan)

    current = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert current is not None
    assert authority.demand_feed_generation == receipt.generation
    assert authority.demand_feed_generation == current.demand_feed_generation
    with engine.connect() as connection:
        head = connection.execute(
            sqlalchemy.select(
                capacity_admission_schema.serve_capacity_plan_heads_table).
            where(capacity_admission_schema.serve_capacity_plan_heads_table.c.
                  service_name == 'svc')).mappings().one()
        published = connection.execute(
            sqlalchemy.select(
                capacity_admission_schema.serve_capacity_plans_table).where(
                    capacity_admission_schema.serve_capacity_plans_table.c.
                    service_name == 'svc')).mappings().one()
    assert head['demand_feed_generation'] == receipt.generation
    assert head['receipt_watermark_sha256'] == capacity_admission._sha256(
        current.receipt_watermark)
    assert published['demand_feed_generation'] == receipt.generation
    assert published['payload']['normalized_demand']['autoscaler_target'] == 1


def test_plan_publication_rejects_advanced_demand_change(capacity_database):
    engine, _, route_receipt = capacity_database
    plan = _plan(1)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=2))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='advanced with changed or unavailable semantics'):
        capacity_admission.CapacityAdmissionRepository(engine).publish(plan)


@pytest.mark.parametrize(('expected', 'current'), [
    ({
        'queue_depth': None
    }, {}),
    ({}, {
        'queue_depth': None
    }),
])
def test_demand_semantics_rejects_missing_and_extra_keys(expected, current):
    assert capacity_admission._changed_demand_semantics(
        expected, current) == ['queue_depth']


def test_prospective_claim_accepts_later_clock_arrival_expiry(
        capacity_database, monkeypatch):
    engine, _, _ = capacity_database
    planned_snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert planned_snapshot is not None
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    claim = {**authority.claim_values('L4'), 'pool_key': _paid_pool_key()}
    assert authority.remaining_launch_capacity() == {'l4': 1}
    assert claim['capacity_plan_accelerator'] == 'l4'
    assert claim['capacity_plan_units'] == 1

    def _expire_arrival(normalized):
        normalized['recent_request_count'] = 0
        normalized['compatibility_demand']['arrivals'] = []
        return normalized

    observed = _mock_prospective_snapshot_normalized_demand(
        monkeypatch, _expire_arrival)

    _validate_prospective_claim(engine, claim)
    _insert_claim(engine, authority, 10)
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='exceed'):
        _validate_prospective_claim(engine, claim)
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='no whole-backend paid launch authority'):
        authority.claim_values('H200')

    assert len(observed) == 3
    for snapshot in observed:
        assert (snapshot.demand_feed_generation ==
                planned_snapshot.demand_feed_generation)
        assert snapshot.receipt_watermark == planned_snapshot.receipt_watermark
        assert snapshot.route_generation == planned_snapshot.route_generation
        assert snapshot.route_sha256 == planned_snapshot.route_sha256
    assert authority.remaining_launch_capacity() == {'l4': 1}


def test_prospective_claim_batch_sums_same_card_before_writes(
        capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(2))
    claim = authority.claim_values('L4')

    fresh_until = _validate_prospective_claim_batch(engine, [claim, claim])

    assert fresh_until > datetime.datetime.now(datetime.timezone.utc)
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.paid_capacity_claims_table)).scalar_one(
                ) == 0
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='Paid claims exceed'):
        _validate_prospective_claim_batch(engine, [claim, claim, claim])


def test_prospective_claim_batch_includes_prior_committed_debits(
        capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(2))
    claim = authority.claim_values('L4')
    _insert_claim(engine, authority, 10)

    _validate_prospective_claim_batch(engine, [claim])
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='Paid claims exceed'):
        _validate_prospective_claim_batch(engine, [claim, claim])


def test_logical_paid_residual_authorizes_one_whole_multi_gpu_backend(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine,
                           incarnation,
                           reserved_fill_enabled=False,
                           replica_unit='logical')
    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'a100': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot,
                supply,
                1,
                accelerator='a100',
                capacity_unit=capacity_planning.CapacityUnit.LOGICAL_GPU,
                physical_gpu_width_by_accelerator={'a100': 8})))
    authority = committed.authority

    assert authority.economic_residual() == {'a100': 1}
    assert authority.remaining_launch_capacity() == {'a100': 8}
    exact_backend = authority.claim_values('a100', units=8)
    _validate_prospective_claim(engine, exact_backend)

    for malformed_units in (1, 9):
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='exact whole-backend'):
            _validate_prospective_claim(engine, {
                **exact_backend,
                'capacity_plan_units': malformed_units,
            })
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='Paid claims exceed'):
        _validate_prospective_claim_batch(engine,
                                          [exact_backend, exact_backend])


def test_prospective_claim_batch_groups_each_exact_debit_card(
        capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(5, capacity_target_by_accelerator={
            'l4': 2,
            'h200': 3,
        }))
    l4 = authority.claim_values('L4')
    h200 = authority.claim_values('H200')

    _validate_prospective_claim_batch(engine, [l4, h200, l4, h200, h200])
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='Paid claims exceed'):
        _validate_prospective_claim_batch(engine,
                                          [l4, h200, l4, h200, h200, h200])


@pytest.mark.parametrize('claims, message',
                         [([], 'nonempty ordered sequence'),
                          ([{}], 'no exact planner authority')],
                         ids=['empty', 'unbound'])
def test_prospective_claim_batch_requires_exact_authority(
        capacity_database, claims, message):
    engine, _, _ = capacity_database

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match=message):
        _validate_prospective_claim_batch(engine, claims)


def test_prospective_claim_batch_rejects_mixed_plan_authorities(
        capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(2))
    claim = authority.claim_values('L4')
    mismatched = {
        **claim,
        'demand_feed_generation': claim['demand_feed_generation'] + 1,
    }

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='multiple planner authorities'):
        _validate_prospective_claim_batch(engine, [claim, mismatched])


def test_prospective_claim_accepts_positive_snapshot_churn(
        capacity_database, monkeypatch):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    claim = {**authority.claim_values('L4'), 'pool_key': _paid_pool_key()}
    assert authority.remaining_launch_capacity() == {'l4': 1}
    assert claim['capacity_plan_accelerator'] == 'l4'

    def _mutate(normalized):
        arrivals = normalized['compatibility_demand']['arrivals']
        assert len(arrivals) == 1
        arrivals[0]['compatible_accelerators'] = ['H200']
        return normalized

    _mock_prospective_snapshot_normalized_demand(monkeypatch, _mutate)

    _validate_prospective_claim(engine, claim)
    assert authority.remaining_launch_capacity() == {'l4': 1}


def test_committed_claim_survives_successor_plan_that_accounts_for_it(
        capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    first = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={
            'l4': 0
        },
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2
                                                          )).authority
    first_claim = _insert_claim(engine, first, 10)

    # Persisting the claim also persists its paid replica.  An independent
    # demand change then mints a semantic successor that moves that unit from
    # residual demand into the existing-paid baseline.  This successor is
    # exactly what used to revoke the claim before its association/request
    # could be committed.
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=2))
    successor = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={
            'l4': 0
        },
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2
                                                          )).authority
    assert successor.generation == first.generation + 1
    assert successor.remaining_launch_capacity() == {'l4': 1}

    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(connection,
                                                             service,
                                                             first_claim,
                                                             prospective=False)

    second_claim = _insert_claim(engine, successor, 11)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=3, request_count=1))
    fully_committed = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={
            'l4': 0
        },
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2
                                                          )).authority
    assert fully_committed.generation == successor.generation + 1
    assert not fully_committed.remaining_launch_capacity()
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(connection,
                                                             service,
                                                             first_claim,
                                                             prospective=False)
        capacity_admission.validate_paid_claim_in_connection(connection,
                                                             service,
                                                             second_claim,
                                                             prospective=False)


def test_paid_claims_survive_unpublished_semantic_heartbeats(capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    first = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={
            'l4': 0
        },
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2
                                                          )).authority
    first_claim = _insert_claim(engine, first, 10)

    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=2))
    successor = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={
            'l4': 0
        },
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 2
                                                          )).authority
    assert successor.generation == first.generation + 1
    assert successor.remaining_launch_capacity() == {'l4': 1}

    # A heartbeat can advance the live feed again while a large provider-free
    # candidate wave is being prepared. Both the committed debit and a new
    # prospective debit remain authorized by unchanged demand semantics.
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=3, request_count=2))
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(connection,
                                                             service,
                                                             first_claim,
                                                             prospective=False)
        capacity_admission.validate_paid_claim_in_connection(connection, {
            **service, 'name': 'svc'
        }, {
            **successor.claim_values('L4'), 'replica_id': 11
        },
                                                             prospective=True)


def test_prospective_hundred_claim_batch_survives_positive_demand_churn(
        capacity_database):
    engine, _, route_receipt = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(100))
    claim = {**authority.claim_values('L4'), 'pool_key': _paid_pool_key()}

    # Model production pressure moving between arrivals, the queue, and the
    # rejection window while the provider-free 100-VM wave is prepared. The
    # current unexpired head remains the quantitative lease until the canonical
    # planner publishes a successor.
    report = _demand_report(time.time(),
                            route_receipt,
                            sequence=2,
                            request_count=1_000)
    report['queue_depth'] = 600
    report['queue_depth_by_priority'] = {'50': 600}
    report['queued_requests_by_compatibility'] = [{
        'priority': 50,
        'compatible_accelerators': ['L4'],
        'count': 600,
    }]
    report['queued_request_deadline_buckets'] = [{
        'priority': 50,
        'compatible_accelerators': ['L4'],
        'remaining_seconds': 300,
        'count': 600,
    }]
    report['rejected_in_window'] = 200
    report['rejected_in_recent_window'] = 150
    report['rejected_in_window_by_priority'] = {'50': 200}
    report['rejected_in_recent_window_by_priority'] = {'50': 150}
    report['rejected_requests_by_compatibility'] = [{
        'priority': 50,
        'compatible_accelerators': ['L4'],
        'count': 200,
        'recent_count': 150,
    }]
    demand_state.ingest_report('svc', 'svc-hash', report)
    current = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert current is not None
    assert current.demand_feed_generation > authority.demand_feed_generation
    assert current.normalized_demand['queue_depth'] == 600
    assert current.normalized_demand['rejected_in_window'] == 200

    fresh_until = _validate_prospective_claim_batch(engine, [{
        **claim, 'replica_id': replica_id
    } for replica_id in range(1, 101)])

    assert fresh_until > datetime.datetime.now(datetime.timezone.utc)


@pytest.mark.parametrize('cap, sequential, expected_results', [
    (16, False, ['acquired', 'service_saturated']),
    (32, True, ['acquired', 'acquired']),
])
def test_atomic_paid_cap_charges_physical_backend_gpu_width(
        capacity_database, monkeypatch, cap, sequential, expected_results):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot,
                supply,
                2,
                backend_num_nodes=2,
                physical_gpu_width_by_accelerator={'l4': 8},
                max_live_paid_gpu_units=cap)))
    claim = committed.authority.claim_values('l4')
    pool_key = _paid_pool_key(accelerator_count=8, num_nodes=2)
    specs = []
    for replica_id in (201, 202):
        info = replica_managers.ReplicaInfo(
            replica_id=replica_id,
            cluster_name=f'svc-{replica_id}',
            replica_port='8000',
            is_spot=True,
            location=None,
            version=1,
            resources_override={'accelerators': {
                'L4': 8,
            }},
            planned_capacity=1)
        specs.append(
            paid_capacity.PaidClaimPersistenceSpec(
                candidate=paid_capacity.PaidClaimCandidate(
                    replica_id=replica_id,
                    replica_info=info,
                    location=None,  # type: ignore[arg-type]
                    priority=50,
                    capacity_plan_claim=claim),
                pool_key=pool_key,
                frontier_key=('l4',),
                frontier_limit=2))

    monkeypatch.setattr(serve_state,
                        '_current_max_live_paid_gpu_units_in_session',
                        lambda *_args, **_kwargs: (True, cap))

    def _admit(batch):
        return serve_state.try_add_replicas_with_paid_capacity_claims(
            'svc',
            'svc-hash',
            batch,
            base_limit=2,
            max_limit=2,
            service_limit=2,
            max_live_paid_gpu_units=cap,
            now=None,
            success_ttl_seconds=60,
            failure_cooldown_seconds=60,
            waiter_ttl_seconds=30,
            frontier_default_limit=2,
            expected_controller_owner=(123, '10.0.0.5'))

    if sequential:
        results = _admit(specs[:1]) + _admit(specs[1:])
    else:
        results = _admit(specs)

    assert results == expected_results
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one() == (
                    expected_results.count('acquired'))


def test_atomic_cpu_paid_pool_succeeds_without_gpu_cap(capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    with engine.begin() as connection:
        capacity_admission.demote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            expected_source_epoch=1)
    pool_key = json.dumps(
        {
            'version': 1,
            'workspace': 'workspace-a',
            'cloud': 'gcp',
            'region': 'us-central1',
            'zone': None,
            'instance_type': None,
            'accelerators': [],
            'use_spot': True,
            'num_nodes': 3,
        },
        sort_keys=True,
        separators=(',', ':'))
    info = replica_managers.ReplicaInfo(replica_id=220,
                                        cluster_name='svc-220',
                                        replica_port='8000',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override={},
                                        planned_capacity=1)
    spec = paid_capacity.PaidClaimPersistenceSpec(
        candidate=paid_capacity.PaidClaimCandidate(
            replica_id=220,
            replica_info=info,
            location=None,  # type: ignore[arg-type]
            priority=50,
            capacity_plan_claim=None),
        pool_key=pool_key,
        frontier_key=(),
        frontier_limit=1)

    result = serve_state.try_add_replicas_with_paid_capacity_claims(
        'svc',
        'svc-hash', [spec],
        base_limit=1,
        max_limit=1,
        service_limit=1,
        max_live_paid_gpu_units=None,
        now=None,
        success_ttl_seconds=60,
        failure_cooldown_seconds=60,
        waiter_ttl_seconds=30,
        frontier_default_limit=1,
        expected_controller_owner=(123, '10.0.0.5'))

    assert result == ['acquired']
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.paid_capacity_pool_key).
            where(serve_state_schema.replicas_table.c.replica_id ==
                  220)).scalar_one()
        assert paid_capacity.paid_pool_gpu_units(row) == 0


def test_atomic_paid_census_rejects_relational_cleanup_contradiction(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot,
                supply,
                1,
                physical_gpu_width_by_accelerator={'l4': 1},
                max_live_paid_gpu_units=8)))
    existing = _replica_values(230, zero_cost=False)
    existing['sky_down_status'] = 'SUCCEEDED'
    existing['replica_state']['status_property'][
        'sky_down_status'] = 'SCHEDULED'
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**existing))

    info = replica_managers.ReplicaInfo(
        replica_id=231,
        cluster_name='svc-231',
        replica_port='8000',
        is_spot=True,
        location=None,
        version=1,
        resources_override={'accelerators': {
            'L4': 1,
        }},
        planned_capacity=1)
    spec = paid_capacity.PaidClaimPersistenceSpec(
        candidate=paid_capacity.PaidClaimCandidate(
            replica_id=231,
            replica_info=info,
            location=None,  # type: ignore[arg-type]
            priority=50,
            capacity_plan_claim=committed.authority.claim_values('l4')),
        pool_key=_paid_pool_key(),
        frontier_key=('l4',),
        frontier_limit=1)
    monkeypatch.setattr(serve_state,
                        '_current_max_live_paid_gpu_units_in_session',
                        lambda *_args, **_kwargs: (True, 8))
    validate_batch = mock.Mock(
        side_effect=AssertionError('Phase A census was bypassed'))
    monkeypatch.setattr(capacity_admission,
                        'validate_prospective_paid_claim_batch_in_connection',
                        validate_batch)

    result = serve_state.try_add_replicas_with_paid_capacity_claims(
        'svc',
        'svc-hash', [spec],
        base_limit=1,
        max_limit=1,
        service_limit=1,
        max_live_paid_gpu_units=8,
        now=None,
        success_ttl_seconds=60,
        failure_cooldown_seconds=60,
        waiter_ttl_seconds=30,
        frontier_default_limit=1,
        expected_controller_owner=(123, '10.0.0.5'))

    assert result == ['service_saturated']
    assert not validate_batch.called


def test_atomic_paid_census_cleanup_proof_dominates_stale_shape_copies(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    cleaned = _replica_values(232, zero_cost=False)
    cleaned['status'] = 'SHUTTING_DOWN'
    cleaned['sky_down_status'] = 'SUCCEEDED'
    cleaned['replica_state']['status_property']['sky_down_status'] = 'SUCCEEDED'
    cleaned['replica_state']['is_zero_cost'] = True
    cleaned['replica_state']['paid_capacity_pool_key'] = _paid_pool_key(
        accelerator_count=4)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**cleaned))
    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot,
                supply,
                1,
                physical_gpu_width_by_accelerator={'l4': 1},
                max_live_paid_gpu_units=8)))

    info = replica_managers.ReplicaInfo(
        replica_id=233,
        cluster_name='svc-233',
        replica_port='8000',
        is_spot=True,
        location=None,
        version=1,
        resources_override={'accelerators': {
            'L4': 1,
        }},
        planned_capacity=1)
    spec = paid_capacity.PaidClaimPersistenceSpec(
        candidate=paid_capacity.PaidClaimCandidate(
            replica_id=233,
            replica_info=info,
            location=None,  # type: ignore[arg-type]
            priority=50,
            capacity_plan_claim=committed.authority.claim_values('l4')),
        pool_key=_paid_pool_key(),
        frontier_key=('l4',),
        frontier_limit=1)
    monkeypatch.setattr(serve_state,
                        '_current_max_live_paid_gpu_units_in_session',
                        lambda *_args, **_kwargs: (True, 8))

    result = serve_state.try_add_replicas_with_paid_capacity_claims(
        'svc',
        'svc-hash', [spec],
        base_limit=1,
        max_limit=1,
        service_limit=1,
        max_live_paid_gpu_units=8,
        now=None,
        success_ttl_seconds=60,
        failure_cooldown_seconds=60,
        waiter_ttl_seconds=30,
        frontier_default_limit=1,
        expected_controller_owner=(123, '10.0.0.5'))

    assert result == ['acquired']


def test_concurrent_atomic_multinode_paid_cap_admits_exactly_one(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 0},
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot,
                supply,
                2,
                backend_num_nodes=2,
                physical_gpu_width_by_accelerator={'l4': 8},
                max_live_paid_gpu_units=16)))
    claim = committed.authority.claim_values('l4')
    pool_key = _paid_pool_key(accelerator_count=8, num_nodes=2)

    def _spec(replica_id):
        info = replica_managers.ReplicaInfo(
            replica_id=replica_id,
            cluster_name=f'svc-{replica_id}',
            replica_port='8000',
            is_spot=True,
            location=None,
            version=1,
            resources_override={'accelerators': {
                'L4': 8,
            }},
            planned_capacity=1)
        return paid_capacity.PaidClaimPersistenceSpec(
            candidate=paid_capacity.PaidClaimCandidate(
                replica_id=replica_id,
                replica_info=info,
                location=None,  # type: ignore[arg-type]
                priority=50,
                capacity_plan_claim=claim),
            pool_key=pool_key,
            frontier_key=('l4',),
            frontier_limit=2)

    monkeypatch.setattr(serve_state,
                        '_current_max_live_paid_gpu_units_in_session',
                        lambda *_args, **_kwargs: (True, 16))
    barrier = threading.Barrier(2)
    results = []

    def _run(replica_id):
        barrier.wait()
        result = serve_state.try_add_replicas_with_paid_capacity_claims(
            'svc',
            'svc-hash', [_spec(replica_id)],
            base_limit=2,
            max_limit=2,
            service_limit=2,
            max_live_paid_gpu_units=16,
            now=None,
            success_ttl_seconds=60,
            failure_cooldown_seconds=60,
            waiter_ttl_seconds=30,
            frontier_default_limit=2,
            expected_controller_owner=(123, '10.0.0.5'))
        results.append(result[0])

    threads = [
        threading.Thread(target=_run, args=(replica_id,))
        for replica_id in (221, 222)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert sorted(results) == ['acquired', 'service_saturated']
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table.c.
                              paid_capacity_pool_key)).scalars().all()
    assert sum(paid_capacity.paid_pool_gpu_units(row) for row in rows) == 16


def test_atomic_hundred_claim_batch_rejects_fresh_zero_without_rows(
        capacity_database):
    engine, incarnation, route_receipt = capacity_database
    # Fresh aggregate zero is an explicit projected-route protocol-2 proof.
    # The shared fixture deliberately models a retained protocol-1 cohort, so
    # promote only this test's exact route writer before constructing the plan.
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
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(100))
    claim = authority.claim_values('L4')
    specs = []
    for replica_id in range(1, 101):
        info = replica_managers.ReplicaInfo(
            replica_id=replica_id,
            cluster_name=f'svc-{replica_id}',
            replica_port='8000',
            is_spot=True,
            location=None,
            version=1,
            resources_override={'accelerators': {
                'L4': 1,
            }})
        specs.append(
            paid_capacity.PaidClaimPersistenceSpec(
                candidate=paid_capacity.PaidClaimCandidate(
                    replica_id=replica_id,
                    replica_info=info,
                    location=None,  # type: ignore[arg-type]
                    priority=50,
                    capacity_plan_claim=claim),
                pool_key=_paid_pool_key(),
                frontier_key=('l4',),
                frontier_limit=100))

    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=0))
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    assert snapshot.fresh_aggregate_zero

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='Fresh aggregate zero revoked paid claim'):
        serve_state.try_add_replicas_with_paid_capacity_claims(
            'svc',
            'svc-hash',
            specs,
            base_limit=100,
            max_limit=100,
            service_limit=100,
            now=time.time(),
            success_ttl_seconds=60,
            failure_cooldown_seconds=60,
            waiter_ttl_seconds=30,
            frontier_default_limit=100,
            expected_controller_owner=(123, '10.0.0.5'))

    with engine.connect() as connection:
        for table in (serve_state_schema.replicas_table,
                      serve_state_schema.paid_capacity_claims_table,
                      serve_state_schema.paid_capacity_waiters_table,
                      serve_state_schema.paid_capacity_pools_table):
            assert connection.execute(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    table)).scalar_one() == 0


def test_capacity_hint_only_route_successor_accepts_retained_report(
        capacity_database):
    engine, incarnation, reported_route = capacity_database
    current_route = _publish_successor_route(engine, incarnation, 2)

    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')

    assert snapshot is not None
    assert snapshot.route_generation == current_route.generation
    assert snapshot.route_sha256 == current_route.content_sha256
    assert snapshot.reconcile_authority.route_generation == (
        current_route.generation)
    assert snapshot.reconcile_authority.route_sha256 == (
        current_route.content_sha256)
    assert snapshot.receipt_watermark
    assert reported_route.generation < snapshot.route_generation


def test_protocol_two_retained_report_publishes_and_commits_current_plan(
        capacity_database):
    engine, incarnation, reported_route = capacity_database
    current_route = _publish_successor_route(engine, incarnation, 2)
    with engine.begin() as connection:
        # Model the production incremental writer after its one-way promotion.
        # The wire snapshot remains protocol 1; both retained generations and
        # the elected producer cohort are protocol 2.
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_snapshots_table).values(
                    producer_protocol_version=2))
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    route_projection_protocol_version=2,
                    route_projection_controller_incarnation=incarnation))
    repository = capacity_admission.CapacityAdmissionRepository(engine)

    authority = repository.publish(_plan(1))
    claim = _insert_claim(engine, authority, 10)

    assert reported_route.generation < current_route.generation
    with engine.connect() as connection:
        plan = connection.execute(
            sqlalchemy.select(
                capacity_admission_schema.serve_capacity_plans_table).where(
                    capacity_admission_schema.serve_capacity_plans_table.c.
                    service_name == 'svc',
                    capacity_admission_schema.serve_capacity_plans_table.c.
                    generation == authority.generation)).mappings().one()
    assert plan['route_generation'] == current_route.generation
    assert plan['route_sha256'] == current_route.content_sha256
    assert claim['capacity_plan_generation'] == authority.generation


def test_promotion_accepts_equivalent_retained_report(capacity_database):
    engine, incarnation, reported_route = capacity_database
    with engine.begin() as connection:
        demoted_epoch = capacity_admission.demote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            expected_source_epoch=1)
    current_route = _publish_successor_route(engine, incarnation, 2)

    with engine.begin() as connection:
        promoted_epoch = capacity_admission.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            participant_barrier_passed=lambda _connection: True)

    assert demoted_epoch == 2
    assert promoted_epoch == 3
    assert reported_route.generation < current_route.generation
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    assert snapshot.route_generation == current_route.generation


@pytest.mark.parametrize('semantic_change', [
    'replica_route', 'route_expansion', 'route_contraction', 'routing_spec',
    'queue_compatibility_mode', 'identity'
])
def test_retained_report_rejects_changed_demand_report_route_context(
        capacity_database, semantic_change):
    engine, incarnation, _ = capacity_database
    response = _route_response()
    record_id = _route_record_id(engine)
    identities = _route_identities(record_id)
    current_record_ids = {record_id}
    if semantic_change == 'replica_route':
        response['replica_info'][_URL]['async_occupancy'] = 'true'
    elif semantic_change == 'route_expansion':
        added_url = 'http://replica-two:8000'
        added_record_id = str(uuid.uuid4())
        response['replica_info'][added_url] = {
            'gpu_type': 'L4',
            'gpu_count': '1',
        }
        identities[added_url] = {
            **identities[_URL],
            'replica_id': 2,
            'replica_record_id': added_record_id,
        }
        current_record_ids.add(added_record_id)
    elif semantic_change == 'route_contraction':
        response['replica_info'] = {}
        response['num_ready_replicas'] = 0
        identities = {}
        current_record_ids = set()
    elif semantic_change == 'routing_spec':
        response['routing_spec'][
            'request_accelerator_compatibility_version'] = 1
    elif semantic_change == 'queue_compatibility_mode':
        response['queued_compatibility_demand_supported'] = False
    else:
        record_id = str(uuid.uuid4())
        identities = _route_identities(record_id)
        current_record_ids = {record_id}
    _publish_route_snapshot(engine, incarnation, response, identities,
                            current_record_ids)

    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is None


def test_missing_retained_report_route_snapshot_fails_closed(capacity_database):
    engine, incarnation, reported_route = capacity_database
    current_route = _publish_successor_route(engine, incarnation, 2)
    assert current_route.generation > reported_route.generation
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(
                route_projection_schema.serve_route_snapshots_table).where(
                    route_projection_schema.serve_route_snapshots_table.c.
                    service_name == 'svc',
                    route_projection_schema.serve_route_snapshots_table.c.
                    generation == reported_route.generation))

    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is None


@pytest.mark.parametrize('retained_corruption', ['content_digest', 'owner'])
def test_corrupt_retained_report_route_fails_snapshot_and_claim(
        capacity_database, retained_corruption):
    engine, incarnation, reported_route = capacity_database
    _publish_successor_route(engine, incarnation, 2)
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    values = ({
        'content_sha256': '0' * 64
    } if retained_corruption == 'content_digest' else {
        'controller_owner_epoch': 5
    })
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_snapshots_table).where(
                    route_projection_schema.serve_route_snapshots_table.c.
                    service_name == 'svc',
                    route_projection_schema.serve_route_snapshots_table.c.
                    generation == reported_route.generation).values(**values))

    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is None
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='fresh route context'):
        _insert_claim(engine, authority, 10)


def test_semantically_changed_retained_report_blocks_prospective_claim(
        capacity_database):
    engine, incarnation, reported_route = capacity_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    lb_ha_enabled=1,
                    lb_active_slot='a',
                    lb_cutover_generation=1))
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    spec=pickle.dumps(_capacity_service_spec(
                        False, lb_high_availability=True),
                                      protocol=4)))
    response = _route_response()
    response['queued_compatibility_demand_supported'] = False
    record_id = _route_record_id(engine)
    current_route = _publish_route_snapshot(engine, incarnation, response,
                                            _route_identities(record_id),
                                            {record_id})
    demand_state.ingest_report(
        'svc', 'svc-hash', _demand_report(time.time(),
                                          current_route,
                                          sequence=2))
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))

    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(),
                       reported_route,
                       reporter_session_id='process-b',
                       lb_session_id='pod-b',
                       lb_slot='b',
                       applied_role='DRAINING'))

    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is None
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='fresh route context'):
        _insert_claim(engine, authority, 10)


def test_mixed_equivalent_report_routes_bind_current_plan_authority(
        capacity_database):
    engine, incarnation, admitted_route = capacity_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    lb_ha_enabled=1,
                    lb_active_slot='a',
                    lb_cutover_generation=1))
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    spec=pickle.dumps(_capacity_service_spec(
                        False, lb_high_availability=True),
                                      protocol=4)))
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    authority = repository.publish(_plan(1))
    claim = _insert_claim(engine, authority, 10)
    successor_route = _publish_successor_route(engine, incarnation, 2)
    assert successor_route.generation == admitted_route.generation + 1

    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), admitted_route, sequence=2))
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(),
                       successor_route,
                       reporter_session_id='process-b',
                       lb_session_id='pod-b',
                       lb_slot='b',
                       applied_role='DRAINING'))

    # Both immutable routes have the same demand-report route context, so
    # their reports compose while returned translation and authority bind the
    # successor.
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    assert snapshot.route_generation == successor_route.generation
    assert snapshot.route_sha256 == successor_route.content_sha256
    assert snapshot.reconcile_authority.route_generation == (
        successor_route.generation)
    assert len(snapshot.receipt_watermark) == 2

    # A plan admitted under the predecessor remains stale.  Semantic report
    # compatibility does not make old plan authority current.
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        with pytest.raises(capacity_admission.CapacityAdmissionConflict):
            capacity_admission.validate_paid_claim_in_connection(
                connection, {
                    **service, 'name': 'svc'
                }, {
                    **authority.claim_values('L4'), 'replica_id': 11
                },
                prospective=True)

    # The already committed bounded debit may perform its one provider effect.
    # Mutable reports are no longer provider authority after the atomic claim;
    # the independently fresh current route head remains required.
    _validate_committed_claim(engine, claim)


def test_committed_claim_survives_noncurrent_ha_report_but_prospective_rejects(
        capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    claim = _insert_claim(engine, authority, 10)

    # Cut over to slot b while only the old slot-a report remains fresh.  It
    # cannot authorize a new debit, but cannot revoke the exact debit that
    # already committed under the prior authority.
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    lb_ha_enabled=1,
                    lb_active_slot='b',
                    lb_cutover_generation=2))
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 1).values(
                    spec=pickle.dumps(_capacity_service_spec(
                        False, lb_high_availability=True),
                                      protocol=4)))
    _validate_committed_claim(engine, claim)
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='fresh demand receipt watermark'):
        _validate_prospective_claim(engine, authority.claim_values('L4'))


def test_committed_claim_rejects_route_before_admitted_plan(capacity_database):
    engine, incarnation, first_route = capacity_database
    admitted_route = _publish_successor_route(engine, incarnation, 2)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), admitted_route, sequence=2))
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    claim = _insert_claim(engine, authority, 10)

    # Simulate an impossible/corrupt route-head regression.  Reported route
    # churn is not post-commit authority, but the durable current route head
    # must remain at or beyond the route admitted by the plan.
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_heads_table).where(
                    route_projection_schema.serve_route_heads_table.c.
                    service_name == 'svc').values(
                        generation=first_route.generation))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='lifecycle or route authority'):
        _validate_committed_claim(engine, claim)


@pytest.mark.parametrize('mutation', [
    'future_generation',
    'wrong_digest',
    'wrong_routing_version',
    'wrong_source_epoch',
    'stored_payload_route_digest_corruption',
])
def test_prospective_claim_rejects_invalid_report_route_reference(
        capacity_database, mutation):
    engine, _, admitted_route = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    report_route = admitted_route
    report_kwargs = {}
    report_mutation = None

    if mutation == 'future_generation':
        report_route = dataclasses.replace(
            admitted_route,
            generation=admitted_route.generation + 1,
            content_sha256='f' * 64)
    elif mutation == 'wrong_digest':
        report_route = dataclasses.replace(admitted_route,
                                           content_sha256='f' * 64)
    elif mutation == 'wrong_routing_version':
        report_kwargs['routing_version'] = 2
    elif mutation == 'wrong_source_epoch':
        report_mutation = ('route_source_epoch', 2)
    report = _demand_report(time.time(),
                            report_route,
                            sequence=2,
                            **report_kwargs)
    if report_mutation is not None:
        report[report_mutation[0]] = report_mutation[1]
    demand_state.ingest_report('svc', 'svc-hash', report)
    if mutation == 'stored_payload_route_digest_corruption':
        reports = demand_state_schema.serve_lb_demand_reports_table
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(reports.c.payload).where(
                    reports.c.service_name == 'svc',
                    reports.c.service_hash == 'svc-hash',
                    reports.c.reporter_session_id == 'process-a')).scalar_one()
            corrupt = dict(row)
            corrupt['route_projection_sha256'] = 'f' * 64
            connection.execute(
                sqlalchemy.update(reports).where(
                    reports.c.service_name == 'svc',
                    reports.c.service_hash == 'svc-hash',
                    reports.c.reporter_session_id == 'process-a').values(
                        payload=corrupt))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict):
        _validate_prospective_claim(engine, authority.claim_values('L4'))


@pytest.mark.parametrize(('field', 'value'), [
    ('service_hash', 'other-hash'),
    ('service_lifecycle_epoch', 4),
    ('service_version', 2),
    ('controller_owner_epoch', 5),
    ('controller_pid', 456),
    ('controller_ip', '10.0.0.6'),
    ('producer_protocol_version', 2),
])
def test_committed_claim_rejects_current_snapshot_owner_drift(
        capacity_database, field, value):
    engine, _, admitted_route = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    claim = _insert_claim(engine, authority, 10)
    snapshots = route_projection_schema.serve_route_snapshots_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(snapshots).where(
                snapshots.c.service_name == 'svc',
                snapshots.c.generation == admitted_route.generation).values(
                    **{field: value}))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict):
        _validate_committed_claim(engine, claim)


def test_committed_claim_rejects_stale_route_head(capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    claim = _insert_claim(engine, authority, 10)
    with engine.begin() as connection:
        expired_at = datetime.datetime.now(
            datetime.timezone.utc) - datetime.timedelta(seconds=1)
        observed_at = expired_at - datetime.timedelta(seconds=1)
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_heads_table).where(
                    route_projection_schema.serve_route_heads_table.c.
                    service_name == 'svc').values(refreshed_at=observed_at,
                                                  valid_until=expired_at))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='lifecycle or route authority'):
        _validate_committed_claim(engine, claim)


def test_committed_claim_survives_report_expiry_but_prospective_rejects(
        capacity_database):
    engine, _, _ = capacity_database
    authority = capacity_admission.CapacityAdmissionRepository(engine).publish(
        _plan(1))
    claim = _insert_claim(engine, authority, 10)
    with engine.begin() as connection:
        expired_at = datetime.datetime.now(
            datetime.timezone.utc) - datetime.timedelta(seconds=1)
        observed_at = expired_at - datetime.timedelta(seconds=1)
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_lb_demand_reports_table).where(
                    demand_state_schema.serve_lb_demand_reports_table.c.
                    service_name == 'svc').values(received_at=observed_at,
                                                  valid_until=expired_at))
        route_valid_until = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_heads_table.c.valid_until).
            where(route_projection_schema.serve_route_heads_table.c.service_name
                  == 'svc')).scalar_one()

    # A report-ingress blackout cannot revoke the bounded provider handoff.
    assert _validate_committed_claim(engine, claim) == route_valid_until
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='fresh demand receipt watermark'):
        _validate_prospective_claim(engine, authority.claim_values('L4'))


def test_committed_claim_survives_fresh_aggregate_zero(capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    authority = repository.publish(_plan(1))
    claim = _insert_claim(engine, authority, 10)

    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=0))
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(connection,
                                                             service,
                                                             claim,
                                                             prospective=False)


def test_committed_claim_survives_nonzero_demand_decrease(capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=2))
    authority = repository.publish(_plan(2))
    claim = _insert_claim(engine, authority, 10)

    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=3, request_count=1))
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(connection,
                                                             service,
                                                             claim,
                                                             prospective=False)


def test_committed_claim_survives_high_churn_compatibility_demand(
        capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    authority = repository.publish(_plan(1))
    claim = _insert_claim(engine, authority, 10)
    with engine.connect() as connection:
        admitted_at = connection.execute(
            sqlalchemy.select(
                serve_state_schema.paid_capacity_claims_table.c.claimed_at).
            where(
                serve_state_schema.paid_capacity_claims_table.c.service_name ==
                'svc',
                serve_state_schema.paid_capacity_claims_table.c.replica_id ==
                10)).scalar_one()

    # Each report advances both recent_request_count and the exact L4
    # compatibility profile before the deferred provider handler validates.
    # The atomically persisted global-cap debit is immutable across this
    # mutable demand churn; no successor plan publication is required.
    for sequence, request_count in enumerate((7, 2, 19, 1, 31, 0, 11), start=2):
        demand_state.ingest_report(
            'svc', 'svc-hash',
            _demand_report(time.time(),
                           route_receipt,
                           sequence=sequence,
                           request_count=request_count))
        with engine.begin() as connection:
            service = connection.execute(
                sqlalchemy.select(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    'svc').with_for_update()).mappings().one()
            capacity_admission.validate_paid_claim_in_connection(
                connection, service, claim, prospective=False)

    # A demand-derived zero successor accounts for the already committed paid
    # baseline but authorizes no additional claim.  It still cannot revoke the
    # original provider effect.
    successor = repository.publish(_plan(0))
    assert not successor.remaining_launch_capacity()
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        capacity_admission.validate_paid_claim_in_connection(connection,
                                                             service,
                                                             claim,
                                                             prospective=False)

    with engine.connect() as connection:
        persisted = connection.execute(
            sqlalchemy.select(
                serve_state_schema.paid_capacity_claims_table).where(
                    serve_state_schema.paid_capacity_claims_table.c.service_name
                    == 'svc',
                    serve_state_schema.paid_capacity_claims_table.c.replica_id
                    == 10)).mappings().one()
    assert persisted['capacity_plan_generation'] == authority.generation
    assert persisted['capacity_plan_sha256'] == authority.content_sha256
    assert persisted['claimed_at'] == admitted_at


def test_allocation_bound_claim_survives_unbound_zero_successor(
        capacity_database, monkeypatch):
    engine, incarnation, route_receipt = capacity_database
    allocation = _allocation_map({'H200': 1})
    _enable_durable_intent(engine, incarnation)
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, _ = _allocation_bound_plan(repository, allocation, {
        'l4': 1,
        'h200': 1,
    })
    authority = repository.publish(plan)
    claim = _insert_claim(engine, authority, 18)

    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=0))
    zero = repository.publish(
        _plan(
            0,
            capacity_target_by_accelerator={
                'l4': 0,
                'h200': 0,
            },
            reserved_fill_authority=(
                capacity_admission.ReservedFillPlanAuthority.zero_revocation()
            )))
    assert not zero.remaining_launch_capacity()
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='no whole-backend paid launch authority'):
        zero.claim_values('L4')

    with engine.begin() as connection:
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

    authority = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={
                'l4': 0
            },
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot, supply, 1)).authority)

    assert authority.remaining_launch_capacity() == ({
        'l4': expected_paid
    } if expected_paid else {})


@pytest.mark.parametrize('provider_effect', [False, True],
                         ids=['final-claim', 'provider-effect'])
def test_missing_kueue_admission_is_fenced_only_before_claim_commit(
        capacity_database, provider_effect):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    key = _install_waiting_kueue_capacity(engine)
    committed = capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(service_name='svc',
                                         service_hash='svc-hash',
                                         service_lifecycle_epoch=3,
                                         service_version=1,
                                         accounting_cards={'l4': 0},
                                         sequenced_reserved_fill=False,
                                         planner=lambda snapshot, supply:
                                         _current_decision(snapshot, supply, 1))
    authority = committed.authority
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
        if provider_effect:
            capacity_admission.validate_paid_claim_in_connection(
                connection, {
                    **service, 'name': 'svc'
                }, {
                    **claim, 'replica_id': 101
                },
                prospective=not provider_effect)
        else:
            with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                               match=('Economic supply or scheduler capacity '
                                      'changed')):
                capacity_admission.validate_paid_claim_in_connection(
                    connection, {
                        **service, 'name': 'svc'
                    }, {
                        **claim, 'replica_id': 101
                    },
                    prospective=True)


@pytest.mark.parametrize('provider_effect', [False, True],
                         ids=['final-claim', 'provider-effect'])
@pytest.mark.parametrize(('field', 'value'), _COPIED_ADMISSION_MUTATIONS)
def test_copied_kueue_identity_is_fenced_only_before_claim_commit(
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
        if provider_effect:
            capacity_admission.validate_paid_claim_in_connection(
                connection, {
                    **service, 'name': 'svc'
                }, {
                    **claim, 'replica_id': 103
                },
                prospective=not provider_effect)
        else:
            with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                               match='Committed capacity changed'):
                capacity_admission.validate_paid_claim_in_connection(
                    connection, {
                        **service, 'name': 'svc'
                    }, {
                        **claim, 'replica_id': 103
                    },
                    prospective=True)


def test_proven_east_intent_without_admission_allows_final_paid_claim(
        capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)
    _install_pending_east_capacity(engine)
    committed = capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(service_name='svc',
                                         service_hash='svc-hash',
                                         service_lifecycle_epoch=3,
                                         service_version=1,
                                         accounting_cards={'l4': 0},
                                         sequenced_reserved_fill=False,
                                         planner=lambda snapshot, supply:
                                         _current_decision(snapshot, supply, 2))
    authority = committed.authority
    assert authority.remaining_launch_capacity() == {'l4': 1}
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
    committed = (capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_publish_current(
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={
                'l4': 0,
                'a100': 0,
            },
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot,
                supply,
                2,
                target_by_accelerator={
                    'l4': 2,
                    'a100': 0,
                },
                compatible_accelerators=('l4', 'a100'),
                cold_accelerator_order=('l4', 'a100'),
                prospective_paid_accelerators=('l4', 'a100'),
                provisioning_by_accelerator=(
                    supply.existing_zero_cost_capacity_by_accelerator))))

    assert committed.candidate.supply_aware_actuation_target.as_dict() == {
        'a100': 1,
        'l4': 1,
    }
    assert committed.candidate.reserved_capacity_committed.as_dict() == {
        'a100': 1
    }
    assert committed.candidate.paid_residual.as_dict() == {'l4': 1}
    assert committed.authority.remaining_launch_capacity() == {'l4': 1}


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
    assert not authority.remaining_launch_capacity()


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
    assert not authority.remaining_launch_capacity()


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

    assert authority.remaining_launch_capacity() == {'l4': 1}
    _validate_prospective_claim(engine, authority.claim_values('l4'))


def test_nonfitting_h200_tail_does_not_block_exact_l4_paid_claim(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    allocation = _allocation_map({'H200': 1}, accelerator_count=8)
    # Seven logical slots cannot admit the allocation's eight-GPU H200
    # backend.  That accelerator-local packing remainder must not suppress an
    # independent one-GPU L4 paid launch.
    _enable_durable_intent(engine,
                           incarnation,
                           max_replicas=7,
                           replica_unit='logical')
    _mock_current_allocation(monkeypatch, allocation)
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    plan, projection = _allocation_bound_plan(repository, allocation, {
        'l4': 1,
        'h200': 0,
    })

    authority = repository.publish(plan)

    assert projection.allocation_reserved_capacity_by_accelerator == {
        'h200': 0,
        'l4': 0,
    }
    assert authority.economic_residual() == {'l4': 1}
    assert authority.remaining_launch_capacity() == {'l4': 1}
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
    assert not tail_authority.remaining_launch_capacity()
    assert not pending_authority.remaining_launch_capacity()
    assert not replica_authority.remaining_launch_capacity()


@pytest.mark.parametrize('provider_effect', [False, True],
                         ids=['final-claim', 'provider-effect'])
def test_tail_to_pending_change_is_fenced_only_before_claim_commit(
        capacity_database, monkeypatch, provider_effect):
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
    claim = (authority.claim_values('L4')
             if not provider_effect else _insert_claim(engine, authority, 82))
    _insert_current_allocation_pending(engine, allocation)

    with engine.begin() as connection:
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
        if provider_effect:
            capacity_admission.validate_paid_claim_in_connection(
                connection,
                service,
                claim,
                prospective=False,
                protocol_and_service_prelocked=True)
        else:
            with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                               match=('Economic supply or scheduler capacity '
                                      'changed')):
                capacity_admission.validate_paid_claim_in_connection(
                    connection,
                    service,
                    claim,
                    prospective=True,
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


@pytest.mark.parametrize('provider_effect', [False, True],
                         ids=['final-claim', 'provider-effect'])
def test_reserved_lifecycle_change_is_fenced_only_before_claim_commit(
        capacity_database, monkeypatch, provider_effect):
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
    claim = (authority.claim_values('L4')
             if not provider_effect else _insert_claim(engine, authority, 86))

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
        if provider_effect:
            capacity_admission.validate_paid_claim_in_connection(
                connection,
                service,
                claim,
                prospective=False,
                protocol_and_service_prelocked=True)
        else:
            with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                               match=('Economic supply or scheduler capacity '
                                      'changed')):
                capacity_admission.validate_paid_claim_in_connection(
                    connection,
                    service,
                    claim,
                    prospective=True,
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
    assert not successor.remaining_launch_capacity()
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


def test_full_other_card_does_not_create_allocation_tail(
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

    projection = capacity_admission.CapacityAdmissionRepository(
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

    assert projection.allocation_reserved_capacity_by_accelerator == {
        'h200': 0,
        'l4': 0,
    }


def test_committed_claim_survives_zero_target_successor(capacity_database):
    engine, _, route_receipt = capacity_database
    repository = capacity_admission.CapacityAdmissionRepository(engine)
    first = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={
            'l4': 0
        },
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 1
                                                          )).authority
    claim = _insert_claim(engine, first, 30)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=0))
    zero_target = repository.plan_and_publish_current(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        service_version=1,
        accounting_cards={
            'l4': 0
        },
        sequenced_reserved_fill=False,
        planner=lambda snapshot, supply: _current_decision(snapshot, supply, 0
                                                          )).authority

    assert zero_target.generation == first.generation + 1
    assert not zero_target.remaining_launch_capacity()
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='no whole-backend paid launch authority'):
        zero_target.claim_values('L4')
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).mappings().one()
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
    assert not zero.remaining_launch_capacity()

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
    unclassified['replica_state']['resources_override'] = None
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**unclassified))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='exact-card accounting'):
        capacity_admission.CapacityAdmissionRepository(engine).publish(_plan(1))
