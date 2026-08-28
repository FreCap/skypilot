"""PostgreSQL contracts for exact-idle paid replica retirement."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import dataclasses
import datetime
import time
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_capacity_admission_pg import _demand_report
from test_serve_capacity_admission_pg import _route_identities
from test_serve_capacity_admission_pg import _route_response
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import capacity_admission
from sky.serve import capacity_admission_schema
from sky.serve import capacity_planning
from sky.serve import demand_state
from sky.serve import demand_state_schema
from sky.serve import paid_retirement
from sky.serve import replica_managers
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.utils import common_utils
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(name='serve_paid_retirement_schema_052_pg')

_OWNER = (123, '10.0.0.5')
_ROUTE_URL = 'http://replica:8000'


def _planner_payload(
    snapshot: demand_state.DurableAutoscalingSnapshot,
    target: int,
    *,
    retained_paid: int = 0,
    existing_paid: int = 1,
) -> tuple[dict, capacity_planning.CapacityPlanCandidate]:
    capacity = capacity_planning.AcceleratorCapacity.from_mapping
    work = capacity_planning.AcceleratorWork.from_mapping
    profiles = (() if target == 0 else (capacity_planning.CompatibilityDemand(
        sequence=0,
        priority=50,
        compatible_accelerators=('l4',),
        work=float(target)),))
    planning_snapshot = capacity_planning.CapacityPlanningSnapshot(
        source_generation=snapshot.demand_feed_generation,
        service_version=1,
        configured_accelerators=('l4',),
        capacity_unit=capacity_planning.CapacityUnit.LOGICAL_GPU,
        physical_gpu_width_by_accelerator=capacity({'l4': 1}),
        capacity_per_accelerator=work({'l4': 1}),
        floors=capacity({}),
        minimum_capacity=0,
        paid_minimum_capacity=0,
        actuation_minimum_capacity=retained_paid,
        maximum_capacity=10,
        demand_profiles=profiles,
        explicit_demand_profiles=profiles,
        paid_demand_profiles=profiles,
        fixed_work=work({}),
        explicit_fixed_work=work({}),
        paid_fixed_work=work({}),
        retention_work=work({}),
        ready_zero_cost=capacity({}),
        ready=capacity({'l4': existing_paid}),
        provisioning=capacity({}),
        reservation=capacity_planning.ReservationPlanningInput(
            gate_policy=(
                capacity_planning.ReservationGatePolicy.NOT_CONFIGURED),
            evidence_state=(
                capacity_planning.ReservationEvidenceState.NOT_APPLICABLE),
            authenticated_capacity=capacity({}),
            eligible_capacity=capacity({}),
            pending_zero_cost_capacity=capacity({}),
            existing_zero_cost_capacity=capacity({}),
            existing_paid_capacity=capacity({'l4': existing_paid}),
            charged_paid_gpu_units=existing_paid,
            evidence_fingerprint=''),
        cold_accelerator_order=('l4',),
        prospective_paid_accelerator_order=('l4',),
        planning_purpose=(
            capacity_planning.CapacityPlanningPurpose.FRESH_ZERO_RETENTION
            if target == 0 else
            capacity_planning.CapacityPlanningPurpose.ECONOMIC_ADMISSION),
        actuation_supply_policy=(
            capacity_planning.ActuationSupplyPolicy.REUSE_CURRENT_SUPPLY),
        attribution_complete=True,
        planning_time=1.0,
        max_live_paid_gpu_units=None,
        retirement_shelter_target=capacity({}),
        source_fingerprint='f' * 64)
    candidate = capacity_planning.plan_capacity(planning_snapshot)
    return (capacity_planning.planner_envelope(planning_snapshot,
                                               candidate), candidate)


def _zero_plan_payload(
    snapshot: demand_state.DurableAutoscalingSnapshot,
    *,
    target: int = 0,
    retained_paid: int = 0,
    existing_paid: int = 1,
) -> dict:
    existing_paid_capacity = {'l4': existing_paid}
    planner_payload, candidate = _planner_payload(snapshot,
                                                  target,
                                                  retained_paid=retained_paid,
                                                  existing_paid=existing_paid)
    plan = capacity_admission.CapacityPlanInput(
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
        normalized_demand=snapshot.normalized_demand,
        capacity_target_by_accelerator={'l4': target},
        reserved_fill_authority=(
            capacity_admission.ReservedFillPlanAuthority.not_applicable()),
        paid_residual=candidate.paid_residual,
        paid_launch_target=candidate.paid_launch_target,
        planner_payload=planner_payload)
    return plan.payload(
        existing_zero_cost_capacity_by_accelerator={'l4': 0},
        existing_paid_capacity_by_accelerator=(existing_paid_capacity))


@pytest.fixture
def retirement_database(empty_postgres, monkeypatch):
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, migration_utils.SERVE_VERSION)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        empty_postgres)
    now = datetime.datetime.now(datetime.timezone.utc)
    incarnation = uuid.uuid4()
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8000',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.service_ready_now = True
    info.status_property.first_ready_time = time.time()
    replica_values = serve_state._replica_row_values('svc', 1, info)
    route_response = _route_response()
    route_identities = _route_identities(info.replica_record_id)
    route_sha256 = route_projection._content_sha256(route_response,
                                                    route_identities)
    route_valid_until = now + datetime.timedelta(seconds=60)
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name='svc', epoch=3))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='svc',
                status='READY',
                hash='svc-hash',
                current_version=1,
                active_versions='[1]',
                pool=0,
                lifecycle_epoch=3,
                controller_incarnation=incarnation,
                controller_owner_epoch=4,
                controller_pid=_OWNER[0],
                controller_ip=_OWNER[1],
                route_source_mode='DURABLE_PROJECTED',
                route_source_epoch=1,
                route_projection_capable=True,
                route_projection_controller_incarnation=incarnation,
                route_projection_protocol_version=2,
                demand_source_mode='DURABLE_FEED',
                demand_source_epoch=1,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=incarnation,
                demand_authority_protocol_version=1))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**replica_values))
        connection.execute(
            sqlalchemy.insert(
                route_projection_schema.serve_route_snapshots_table).values(
                    service_name='svc',
                    generation=1,
                    service_hash='svc-hash',
                    service_lifecycle_epoch=3,
                    controller_incarnation=incarnation,
                    controller_owner_epoch=4,
                    controller_pid=_OWNER[0],
                    controller_ip=_OWNER[1],
                    service_version=1,
                    protocol_version=1,
                    producer_protocol_version=2,
                    content_sha256=route_sha256,
                    response_payload=route_response,
                    identity_payload=route_identities,
                    created_at=now))
        connection.execute(
            sqlalchemy.insert(
                route_projection_schema.serve_route_heads_table).values(
                    service_name='svc',
                    generation=1,
                    refreshed_at=now,
                    valid_until=route_valid_until))
        connection.execute(
            sqlalchemy.insert(
                route_projection_schema.serve_route_replica_leases_table).
            values(service_name='svc',
                   service_hash='svc-hash',
                   replica_id=1,
                   replica_record_id=uuid.UUID(info.replica_record_id),
                   service_lifecycle_epoch=3,
                   controller_incarnation=incarnation,
                   controller_owner_epoch=4,
                   controller_pid=_OWNER[0],
                   controller_ip=_OWNER[1],
                   service_version=1,
                   route_url=_ROUTE_URL,
                   gpu_type='L4',
                   gpu_count=1,
                   probe_method='GET',
                   readiness_path='/health',
                   probe_timeout_seconds=15,
                   probe_post_data=None,
                   probe_headers=None,
                   async_occupancy=True,
                   uses_logical_replicas=False,
                   is_zero_cost=False,
                   planned_capacity=1,
                   route_allowed=True,
                   requires_route_marker=False,
                   route_marker_payload=None,
                   material_sha256='c' * 64,
                   material_generation=1,
                   readiness_generation=1,
                   ready=True,
                   created_at=now,
                   resolved_at=now,
                   observed_at=now,
                   valid_until=now + datetime.timedelta(seconds=60),
                   revocation_generation=0,
                   revoked_at=None,
                   revocation_reason=None))
    route_receipt = route_projection.RoutePublicationReceipt(
        generation=1,
        content_sha256=route_sha256,
        duplicate=False,
        valid_until=route_valid_until)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, request_count=0))
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None and snapshot.fresh_aggregate_zero
    payload = _zero_plan_payload(snapshot)
    plan_sha256 = capacity_admission.capacity_plan_content_sha256(payload)
    with empty_postgres.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        connection.execute(
            sqlalchemy.insert(
                capacity_admission_schema.serve_capacity_plans_table).values(
                    service_name='svc',
                    generation=1,
                    service_hash='svc-hash',
                    service_lifecycle_epoch=3,
                    service_version=1,
                    demand_source_epoch=1,
                    demand_feed_generation=snapshot.demand_feed_generation,
                    route_generation=snapshot.route_generation,
                    route_sha256=snapshot.route_sha256,
                    route_source_epoch=snapshot.route_source_epoch,
                    protocol_version=1,
                    content_sha256=plan_sha256,
                    payload=payload,
                    created_at=now))
        connection.execute(
            sqlalchemy.insert(
                capacity_admission_schema.serve_capacity_plan_heads_table).
            values(service_name='svc',
                   generation=1,
                   demand_feed_generation=snapshot.demand_feed_generation,
                   receipt_watermark_sha256=(
                       capacity_admission.capacity_plan_content_sha256(
                           snapshot.receipt_watermark)),
                   refreshed_at=now,
                   valid_until=now + datetime.timedelta(seconds=60)))
    authority = paid_retirement.FreshZeroAuthority(
        service_hash='svc-hash',
        demand_source_epoch=snapshot.demand_source_epoch,
        demand_feed_generation=snapshot.demand_feed_generation,
        capacity_plan_generation=1,
        capacity_plan_sha256=plan_sha256,
        route_generation=snapshot.route_generation)
    return empty_postgres, info, authority


def test_delete_cleanup_noops_before_serve051(empty_postgres):
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '050')
    with sqlalchemy.orm.Session(empty_postgres) as session, session.begin():
        paid_retirement.delete_in_session(session, 'svc', [1])


def _mark_retiring(info):
    info.status_property.is_scale_down = True
    info.status_property.sky_down_status = common_utils.ProcessStatus.SCHEDULED
    info.status_property.wait_for_idle_before_termination = True
    info.status_property.drain_cap_seconds = None
    info.status_property.drain_started_at = None


def _publish_zero_successor(
    engine: sqlalchemy.engine.Engine,
    info: replica_managers.ReplicaInfo,
    *,
    target: int = 0,
) -> tuple[demand_state.DurableAutoscalingSnapshot, str]:
    with engine.connect() as connection:
        owner = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc')).mappings().one()
    response = _route_response()
    identities = _route_identities(info.replica_record_id)
    route_sha256 = route_projection._content_sha256(response, identities)
    now = datetime.datetime.now(datetime.timezone.utc)
    route_valid_until = now + datetime.timedelta(seconds=60)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                route_projection_schema.serve_route_snapshots_table).values(
                    service_name='svc',
                    generation=2,
                    service_hash='svc-hash',
                    service_lifecycle_epoch=3,
                    controller_incarnation=owner['controller_incarnation'],
                    controller_owner_epoch=owner['controller_owner_epoch'],
                    controller_pid=owner['controller_pid'],
                    controller_ip=owner['controller_ip'],
                    service_version=1,
                    protocol_version=1,
                    producer_protocol_version=2,
                    content_sha256=route_sha256,
                    response_payload=response,
                    identity_payload=identities,
                    created_at=now))
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_heads_table).values(
                    generation=2,
                    refreshed_at=now,
                    valid_until=route_valid_until))
    route_receipt = route_projection.RoutePublicationReceipt(
        generation=2,
        content_sha256=route_sha256,
        duplicate=False,
        valid_until=route_valid_until)
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=0))
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None and snapshot.fresh_aggregate_zero
    payload = _zero_plan_payload(snapshot, target=target)
    digest = capacity_admission.capacity_plan_content_sha256(payload)
    with engine.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        connection.execute(
            sqlalchemy.insert(
                capacity_admission_schema.serve_capacity_plans_table).values(
                    service_name='svc',
                    generation=2,
                    service_hash='svc-hash',
                    service_lifecycle_epoch=3,
                    service_version=1,
                    demand_source_epoch=snapshot.demand_source_epoch,
                    demand_feed_generation=snapshot.demand_feed_generation,
                    route_generation=snapshot.route_generation,
                    route_sha256=snapshot.route_sha256,
                    route_source_epoch=snapshot.route_source_epoch,
                    protocol_version=1,
                    content_sha256=digest,
                    payload=payload,
                    created_at=now))
        connection.execute(
            sqlalchemy.update(
                capacity_admission_schema.serve_capacity_plan_heads_table).
            values(generation=2,
                   demand_feed_generation=snapshot.demand_feed_generation,
                   receipt_watermark_sha256=(
                       capacity_admission.capacity_plan_content_sha256(
                           snapshot.receipt_watermark)),
                   refreshed_at=now,
                   valid_until=now + datetime.timedelta(seconds=60)))
    return snapshot, digest


def _refresh_duplicate_zero_plan_head(
    engine: sqlalchemy.engine.Engine,
) -> demand_state.DurableAutoscalingSnapshot:
    """Advance only the mutable head, as duplicate publication does."""
    with engine.connect() as connection:
        route = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_snapshots_table).where(
                    route_projection_schema.serve_route_snapshots_table.c.
                    service_name == 'svc',
                    route_projection_schema.serve_route_snapshots_table.c.
                    generation == 1)).mappings().one()
        route_head = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_heads_table).where(
                    route_projection_schema.serve_route_heads_table.c.
                    service_name == 'svc')).mappings().one()
        original_plan = connection.execute(
            sqlalchemy.select(
                capacity_admission_schema.serve_capacity_plans_table).where(
                    capacity_admission_schema.serve_capacity_plans_table.c.
                    service_name == 'svc',
                    capacity_admission_schema.serve_capacity_plans_table.c.
                    generation == 1)).mappings().one()
    route_receipt = route_projection.RoutePublicationReceipt(
        generation=1,
        content_sha256=route['content_sha256'],
        duplicate=True,
        valid_until=route_head['valid_until'])
    demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=0))
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None and snapshot.fresh_aggregate_zero
    assert snapshot.demand_feed_generation > original_plan[
        'demand_feed_generation']
    # Duplicate publication advances only the mutable demand binding.  The
    # immutable planner candidate remains bound to the source generation at
    # which it was committed; retirement revalidates the newer live zero
    # observation before rebasing to that retained candidate.
    planner_snapshot, candidate = capacity_planning.decode_planner_envelope(
        original_plan['payload']['planner'])
    assert planner_snapshot.source_generation == original_plan[
        'demand_feed_generation']
    assert candidate.kind is (
        capacity_planning.CapacityPlanKind.FRESH_ZERO_RETENTION)
    with engine.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        connection.execute(
            sqlalchemy.update(
                capacity_admission_schema.serve_capacity_plan_heads_table).
            where(capacity_admission_schema.serve_capacity_plan_heads_table.c.
                  service_name == 'svc').values(
                      generation=1,
                      demand_feed_generation=(snapshot.demand_feed_generation),
                      receipt_watermark_sha256=(
                          capacity_admission.capacity_plan_content_sha256(
                              snapshot.receipt_watermark)),
                      refreshed_at=now,
                      valid_until=now + datetime.timedelta(seconds=60)))
    return snapshot


def test_admission_atomically_revokes_route_and_persists_exact_intent(
        retirement_database):
    engine, info, authority = retirement_database
    _mark_retiring(info)

    record = serve_state.admit_paid_retirement('svc',
                                               1,
                                               info,
                                               authority,
                                               requires_idle_proof=True,
                                               expected_route_url=_ROUTE_URL,
                                               expected_service_hash='svc-hash',
                                               expected_controller_owner=_OWNER)

    assert record is not None
    assert record['state'] == paid_retirement.PaidRetirementState.ACTIVE.value
    assert record['route_url'] == _ROUTE_URL
    with engine.connect() as connection:
        intent = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table)).mappings(
                ).one()
        lease = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table)
        ).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.status)).scalar_one()
    assert intent['state'] == paid_retirement.PaidRetirementState.ACTIVE.value
    assert lease['revocation_reason'] == 'replica_became_route_ineligible'
    assert replica == 'SHUTTING_DOWN'


def test_admission_accepts_fresh_zero_retained_paid_actuation(
        retirement_database):
    engine, info, authority = retirement_database
    retained = replica_managers.ReplicaInfo(replica_id=2,
                                            cluster_name='svc-2',
                                            replica_port='8000',
                                            is_spot=True,
                                            location=None,
                                            version=1,
                                            resources_override=None)
    retained.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    retained.status_property.service_ready_now = True
    retained.status_property.first_ready_time = time.time()
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None and snapshot.fresh_aggregate_zero
    payload = _zero_plan_payload(snapshot, retained_paid=1, existing_paid=2)
    planner_snapshot, candidate = capacity_planning.decode_planner_envelope(
        payload['planner'])
    assert planner_snapshot.planning_purpose is (
        capacity_planning.CapacityPlanningPurpose.FRESH_ZERO_RETENTION)
    assert candidate.supply_aware_demand_target.total() == 0
    assert candidate.retained_existing_target.as_dict() == {'l4': 1}
    assert candidate.wave_limited_actuation_target.as_dict() == {'l4': 1}
    assert candidate.paid_residual.total() == 0
    assert candidate.paid_launch_target.total() == 0
    digest = capacity_admission.capacity_plan_content_sha256(payload)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **serve_state._replica_row_values('svc', 2, retained)))
        connection.execute(
            sqlalchemy.update(
                capacity_admission_schema.serve_capacity_plans_table).where(
                    capacity_admission_schema.serve_capacity_plans_table.c.
                    service_name == 'svc',
                    capacity_admission_schema.serve_capacity_plans_table.c.
                    generation == 1).values(payload=payload,
                                            content_sha256=digest))
    authority = dataclasses.replace(authority, capacity_plan_sha256=digest)
    _mark_retiring(info)

    record = serve_state.admit_paid_retirement('svc',
                                               1,
                                               info,
                                               authority,
                                               requires_idle_proof=True,
                                               expected_route_url=_ROUTE_URL,
                                               expected_service_hash='svc-hash',
                                               expected_controller_owner=_OWNER)

    assert record is not None
    assert record['state'] == paid_retirement.PaidRetirementState.ACTIVE.value
    with engine.connect() as connection:
        statuses = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.replica_id,
                serve_state_schema.replicas_table.c.status).order_by(
                    serve_state_schema.replicas_table.c.replica_id)).all()
    assert statuses == [(1, 'SHUTTING_DOWN'), (2, 'READY')]


def test_admission_rejects_route_not_acknowledged_by_load_balancer(
        retirement_database):
    engine, info, authority = retirement_database
    route_urls = paid_retirement.list_active_route_urls(
        'svc', 'svc-hash', {1: info.replica_record_id})
    assert route_urls == {1: _ROUTE_URL}
    _mark_retiring(info)

    record = serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        requires_idle_proof=True,
        expected_route_url='http://stale-route:8000',
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER)

    assert record is None
    with engine.connect() as connection:
        intent_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                paid_retirement.serve_paid_replica_retirements_table)
        ).scalar_one()
        lease = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table)
        ).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.status)).scalar_one()
    assert intent_count == 0
    assert lease['revoked_at'] is None
    assert replica == 'READY'


def test_admission_defers_while_provider_holds_shared_authority(
        retirement_database):
    engine, info, authority = retirement_database
    _mark_retiring(info)

    with serve_state.service_replica_launch_authority_guard('svc'):
        started = time.monotonic()
        record = serve_state.admit_paid_retirement(
            'svc',
            1,
            info,
            authority,
            requires_idle_proof=True,
            expected_route_url=_ROUTE_URL,
            expected_service_hash='svc-hash',
            expected_controller_owner=_OWNER)
        elapsed = time.monotonic() - started
        second_reader_started = time.monotonic()
        with serve_state.service_replica_launch_authority_guard('svc'):
            pass
        second_reader_elapsed = time.monotonic() - second_reader_started

    assert record is None
    assert elapsed < 2
    assert second_reader_elapsed < 2
    with engine.connect() as connection:
        intent_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                paid_retirement.serve_paid_replica_retirements_table)
        ).scalar_one()
        lease = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table)
        ).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.status)).scalar_one()
    assert intent_count == 0
    assert lease['revoked_at'] is None
    assert lease['revocation_reason'] is None
    assert replica == 'READY'


def test_admission_accepts_current_duplicate_plan_head(retirement_database):
    engine, info, original_authority = retirement_database
    snapshot = _refresh_duplicate_zero_plan_head(engine)
    authority = dataclasses.replace(
        original_authority,
        demand_feed_generation=snapshot.demand_feed_generation)
    _mark_retiring(info)

    record = serve_state.admit_paid_retirement('svc',
                                               1,
                                               info,
                                               authority,
                                               requires_idle_proof=True,
                                               expected_route_url=_ROUTE_URL,
                                               expected_service_hash='svc-hash',
                                               expected_controller_owner=_OWNER)

    assert record is not None
    assert record['demand_feed_generation'] == snapshot.demand_feed_generation
    assert record['capacity_plan_generation'] == 1
    assert record['capacity_plan_sha256'] == (
        original_authority.capacity_plan_sha256)


def test_idle_commit_is_generation_fenced_and_irreversible(retirement_database):
    engine, info, authority = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        requires_idle_proof=True,
        expected_route_url=_ROUTE_URL,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_demand_feed_generations_table).values(
                    generation=2))
    info.status_property.wait_for_idle_before_termination = False

    assert not serve_state.commit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER)
    with engine.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.state)
        ).scalar_one()
    assert state == paid_retirement.PaidRetirementState.ACTIVE.value


def test_idle_commit_rebases_to_current_equivalent_zero_authority(
        retirement_database):
    engine, info, original_authority = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        original_authority,
        requires_idle_proof=True,
        expected_route_url=_ROUTE_URL,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    successor, successor_digest = _publish_zero_successor(engine, info)
    info.status_property.wait_for_idle_before_termination = False

    assert serve_state.commit_paid_retirement('svc',
                                              1,
                                              info,
                                              original_authority,
                                              expected_service_hash='svc-hash',
                                              expected_controller_owner=_OWNER)
    with engine.connect() as connection:
        record = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table)).mappings(
                ).one()
    assert record[
        'state'] == paid_retirement.PaidRetirementState.COMMITTED.value
    assert record['demand_feed_generation'] == successor.demand_feed_generation
    assert record['capacity_plan_generation'] == 2
    assert record['capacity_plan_sha256'] == successor_digest
    assert record['route_generation'] == successor.route_generation


def test_idle_commit_rebases_to_current_duplicate_plan_head(
        retirement_database):
    engine, info, original_authority = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        original_authority,
        requires_idle_proof=True,
        expected_route_url=_ROUTE_URL,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    snapshot = _refresh_duplicate_zero_plan_head(engine)
    info.status_property.wait_for_idle_before_termination = False

    assert serve_state.commit_paid_retirement('svc',
                                              1,
                                              info,
                                              original_authority,
                                              expected_service_hash='svc-hash',
                                              expected_controller_owner=_OWNER)
    with engine.connect() as connection:
        record = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table)).mappings(
                ).one()
        immutable_plan_generation = connection.execute(
            sqlalchemy.select(
                capacity_admission_schema.serve_capacity_plans_table.c.
                demand_feed_generation).where(
                    capacity_admission_schema.serve_capacity_plans_table.c.
                    service_name == 'svc', capacity_admission_schema.
                    serve_capacity_plans_table.c.generation == 1)).scalar_one()
    assert record[
        'state'] == paid_retirement.PaidRetirementState.COMMITTED.value
    assert record['demand_feed_generation'] == snapshot.demand_feed_generation
    assert record['capacity_plan_generation'] == 1
    assert record['capacity_plan_sha256'] == (
        original_authority.capacity_plan_sha256)
    assert immutable_plan_generation < snapshot.demand_feed_generation


@pytest.mark.parametrize('mutation', [
    'positive_target',
    'nonempty_paid_residual',
    'nonempty_paid_launch_target',
    'nonempty_planner_paid_residual',
    'nonempty_planner_paid_launch_target',
    'nonempty_paid_packing_padding',
    'planner_wrong_kind',
    'planner_wrong_source',
    'payload_digest_mismatch',
    'future_plan_demand_generation',
    'plan_route_mismatch',
    'expired_plan_head',
    'expired_route_head',
    'expired_demand_report',
    'changed_demand_source',
    'reactivated_route_lease',
    'replica_not_scale_down',
])
def test_idle_commit_rejects_non_equivalent_or_stale_successor(
        retirement_database, mutation):
    engine, info, authority = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        requires_idle_proof=True,
        expected_route_url=_ROUTE_URL,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    _publish_zero_successor(engine,
                            info,
                            target=2 if mutation == 'positive_target' else 0)
    with engine.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        if mutation in ('nonempty_paid_residual', 'nonempty_paid_launch_target',
                        'nonempty_planner_paid_residual',
                        'nonempty_planner_paid_launch_target',
                        'nonempty_paid_packing_padding', 'planner_wrong_kind',
                        'planner_wrong_source'):
            plans = capacity_admission_schema.serve_capacity_plans_table
            payload = dict(
                connection.execute(
                    sqlalchemy.select(plans.c.payload).where(
                        plans.c.service_name == 'svc',
                        plans.c.generation == 2)).scalar_one())
            if mutation == 'nonempty_paid_residual':
                payload['paid_residual_by_accelerator'] = {'l4': 1}
            elif mutation == 'nonempty_paid_launch_target':
                payload['paid_launch_target_by_accelerator'] = {'l4': 1}
            elif mutation in ('planner_wrong_kind', 'planner_wrong_source'):
                planner_snapshot, _ = (
                    capacity_planning.decode_planner_envelope(
                        payload['planner']))
                if mutation == 'planner_wrong_kind':
                    planner_snapshot = dataclasses.replace(
                        planner_snapshot,
                        planning_purpose=(
                            capacity_planning.CapacityPlanningPurpose.
                            ECONOMIC_ADMISSION))
                else:
                    planner_snapshot = dataclasses.replace(
                        planner_snapshot,
                        source_generation=(planner_snapshot.source_generation +
                                           1))
                candidate = capacity_planning.plan_capacity(planner_snapshot)
                payload['planner'] = capacity_planning.planner_envelope(
                    planner_snapshot, candidate)
            else:
                planner = dict(payload['planner'])
                candidate = dict(planner['candidate'])
                field = {
                    'nonempty_planner_paid_residual': 'paid_residual',
                    'nonempty_planner_paid_launch_target': 'paid_launch_target',
                    'nonempty_paid_packing_padding': 'paid_packing_padding_target',
                }[mutation]
                candidate[field] = {'entries': [['l4', 1]]}
                planner['candidate'] = candidate
                payload['planner'] = planner
            connection.execute(
                sqlalchemy.update(plans).where(
                    plans.c.service_name == 'svc',
                    plans.c.generation == 2).values(
                        payload=payload,
                        content_sha256=(capacity_admission.
                                        capacity_plan_content_sha256(payload))))
        elif mutation == 'payload_digest_mismatch':
            plans = capacity_admission_schema.serve_capacity_plans_table
            payload = dict(
                connection.execute(
                    sqlalchemy.select(plans.c.payload).where(
                        plans.c.service_name == 'svc',
                        plans.c.generation == 2)).scalar_one())
            payload['normalized_demand'] = {'fresh_aggregate_zero': False}
            connection.execute(
                sqlalchemy.update(plans).where(
                    plans.c.service_name == 'svc',
                    plans.c.generation == 2).values(payload=payload))
        elif mutation == 'future_plan_demand_generation':
            connection.execute(
                sqlalchemy.update(
                    capacity_admission_schema.serve_capacity_plans_table).where(
                        capacity_admission_schema.serve_capacity_plans_table.c.
                        service_name == 'svc',
                        capacity_admission_schema.serve_capacity_plans_table.c.
                        generation == 2).values(demand_feed_generation=3))
        elif mutation == 'plan_route_mismatch':
            connection.execute(
                sqlalchemy.update(
                    capacity_admission_schema.serve_capacity_plans_table).where(
                        capacity_admission_schema.serve_capacity_plans_table.c.
                        service_name == 'svc',
                        capacity_admission_schema.serve_capacity_plans_table.c.
                        generation == 2).values(route_generation=1))
        elif mutation == 'expired_plan_head':
            connection.execute(
                sqlalchemy.update(
                    capacity_admission_schema.serve_capacity_plan_heads_table).
                values(refreshed_at=now - datetime.timedelta(seconds=2),
                       valid_until=now - datetime.timedelta(seconds=1)))
        elif mutation == 'expired_route_head':
            connection.execute(
                sqlalchemy.update(
                    route_projection_schema.serve_route_heads_table).values(
                        refreshed_at=now - datetime.timedelta(seconds=2),
                        valid_until=now - datetime.timedelta(seconds=1)))
        elif mutation == 'expired_demand_report':
            connection.execute(
                sqlalchemy.update(
                    demand_state_schema.serve_lb_demand_reports_table).values(
                        received_at=now - datetime.timedelta(seconds=2),
                        valid_until=now - datetime.timedelta(seconds=1)))
        elif mutation == 'changed_demand_source':
            connection.execute(
                sqlalchemy.update(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name == 'svc').values(
                        demand_source_epoch=2))
        elif mutation == 'reactivated_route_lease':
            connection.execute(
                sqlalchemy.update(route_projection_schema.
                                  serve_route_replica_leases_table).values(
                                      revoked_at=None, revocation_reason=None))
        elif mutation == 'replica_not_scale_down':
            replicas = serve_state_schema.replicas_table
            state = dict(
                connection.execute(
                    sqlalchemy.select(replicas.c.replica_state).where(
                        replicas.c.service_name == 'svc',
                        replicas.c.replica_id == 1)).scalar_one())
            status = dict(state['status_property'])
            status['is_scale_down'] = False
            state['status_property'] = status
            connection.execute(
                sqlalchemy.update(replicas).where(
                    replicas.c.service_name == 'svc',
                    replicas.c.replica_id == 1).values(replica_state=state))
    info.status_property.wait_for_idle_before_termination = False

    assert not serve_state.commit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER)
    with engine.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.state)
        ).scalar_one()
    assert state == paid_retirement.PaidRetirementState.ACTIVE.value


def test_immediate_retirement_rejects_expired_live_zero_reports(
        retirement_database):
    engine, info, authority = retirement_database
    _mark_retiring(info)
    info.status_property.wait_for_idle_before_termination = False
    with engine.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_lb_demand_reports_table).values(
                    received_at=now - datetime.timedelta(seconds=2),
                    valid_until=now - datetime.timedelta(seconds=1)))

    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        requires_idle_proof=False,
        expected_route_url=None,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is None
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.
                replica_id)).scalar_one_or_none() is None
        assert connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table.c.
                revoked_at)).scalar_one() is None


def test_exact_idle_commit_cannot_be_cancelled(retirement_database):
    engine, info, authority = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        requires_idle_proof=True,
        expected_route_url=_ROUTE_URL,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    info.status_property.wait_for_idle_before_termination = False

    assert serve_state.commit_paid_retirement('svc',
                                              1,
                                              info,
                                              authority,
                                              expected_service_hash='svc-hash',
                                              expected_controller_owner=_OWNER)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_demand_feed_generations_table).values(
                    generation=2))
    info.status_property.is_scale_down = False
    info.status_property.sky_down_status = None
    assert not serve_state.cancel_paid_retirement(
        'svc',
        1,
        info,
        2,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER)
    with engine.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.state)
        ).scalar_one()
    assert state == paid_retirement.PaidRetirementState.COMMITTED.value


def test_newer_positive_demand_cancels_only_uncommitted_intent(
        retirement_database):
    engine, info, authority = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        requires_idle_proof=True,
        expected_route_url=_ROUTE_URL,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                demand_state_schema.serve_demand_feed_generations_table).values(
                    generation=2))
    info.status_property.is_scale_down = False
    info.status_property.sky_down_status = None
    info.status_property.wait_for_idle_before_termination = False

    assert serve_state.cancel_paid_retirement('svc',
                                              1,
                                              info,
                                              2,
                                              expected_service_hash='svc-hash',
                                              expected_controller_owner=_OWNER)
    with engine.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.state)
        ).scalar_one()
        lease = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table)
        ).mappings().one()
    assert state == paid_retirement.PaidRetirementState.CANCELLED.value
    assert lease['revoked_at'] is not None


def test_positive_demand_batch_cancellation_accepts_newer_positive_generation(
        retirement_database):
    engine, info, authority = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        requires_idle_proof=True,
        expected_route_url=_ROUTE_URL,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None

    info2 = replica_managers.ReplicaInfo(replica_id=2,
                                         cluster_name='svc-2',
                                         replica_port='8000',
                                         is_spot=True,
                                         location=None,
                                         version=1,
                                         resources_override=None)
    info2.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    info2.status_property.service_ready_now = True
    info2.status_property.first_ready_time = time.time()
    _mark_retiring(info2)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **serve_state._replica_row_values('svc', 2, info2)))
        retirement = dict(
            connection.execute(
                sqlalchemy.select(
                    paid_retirement.serve_paid_replica_retirements_table).where(
                        paid_retirement.serve_paid_replica_retirements_table.c.
                        replica_id == 1)).mappings().one())
        retirement.update(replica_id=2,
                          replica_record_id=uuid.UUID(info2.replica_record_id))
        connection.execute(
            sqlalchemy.insert(
                paid_retirement.serve_paid_replica_retirements_table).values(
                    **retirement))

    route_receipt = route_projection.RoutePublicationReceipt(
        generation=1,
        content_sha256=route_projection._content_sha256(
            _route_response(), _route_identities(info.replica_record_id)),
        duplicate=True,
        valid_until=datetime.datetime.now(datetime.timezone.utc) +
        datetime.timedelta(seconds=60))
    first_positive = demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=2, request_count=2))
    latest_positive = demand_state.ingest_report(
        'svc', 'svc-hash',
        _demand_report(time.time(), route_receipt, sequence=3, request_count=3))
    assert latest_positive.generation > first_positive.generation

    for candidate in (info, info2):
        candidate.status_property.is_scale_down = False
        candidate.status_property.sky_down_status = None
        candidate.status_property.wait_for_idle_before_termination = False
    assert serve_state.cancel_paid_retirements(
        'svc', [(1, info), (2, info2)],
        first_positive.generation,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) == {1, 2}

    with engine.connect() as connection:
        states = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.
                replica_id,
                paid_retirement.serve_paid_replica_retirements_table.c.state).
            order_by(paid_retirement.serve_paid_replica_retirements_table.c.
                     replica_id)).all()
    assert states == [
        (1, paid_retirement.PaidRetirementState.CANCELLED.value),
        (2, paid_retirement.PaidRetirementState.CANCELLED.value),
    ]
    persisted = serve_state.get_replica_infos_from_ids('svc', [1, 2])
    assert all(not candidate.status_property.is_scale_down
               for candidate in persisted.values())
    assert all(candidate.status_property.sky_down_status is None
               for candidate in persisted.values())


def test_batch_cancellation_rejects_current_fresh_zero(retirement_database):
    engine, info, authority = retirement_database
    _mark_retiring(info)
    assert serve_state.admit_paid_retirement(
        'svc',
        1,
        info,
        authority,
        requires_idle_proof=True,
        expected_route_url=_ROUTE_URL,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    info.status_property.is_scale_down = False
    info.status_property.sky_down_status = None
    info.status_property.wait_for_idle_before_termination = False

    assert serve_state.cancel_paid_retirements(
        'svc', [(1, info)],
        authority.demand_feed_generation,
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) == set()
    with engine.connect() as connection:
        state = connection.execute(
            sqlalchemy.select(
                paid_retirement.serve_paid_replica_retirements_table.c.state)
        ).scalar_one()
    assert state == paid_retirement.PaidRetirementState.ACTIVE.value
