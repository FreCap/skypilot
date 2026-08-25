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


def _zero_plan_payload(
    snapshot: demand_state.DurableAutoscalingSnapshot,
    *,
    target: int = 0,
) -> dict:
    existing_paid = {'l4': 1}
    paid_residual = {'l4': target - 1} if target > 1 else {}
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
            capacity_admission.ReservedFillPlanAuthority.not_applicable()))
    return plan.payload(existing_zero_cost_capacity_by_accelerator={'l4': 0},
                        existing_paid_capacity_by_accelerator=existing_paid,
                        paid_residual_by_accelerator=paid_residual)


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
    # demand_feed_generation and its receipt watermark deliberately live in
    # the mutable plan head.  They do not change the semantic plan payload, so
    # CapacityAdmissionRepository.publish() retains generation 1 here.
    assert capacity_admission.capacity_plan_content_sha256(
        _zero_plan_payload(snapshot)) == original_plan['content_sha256']
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
        expected_service_hash='svc-hash',
        expected_controller_owner=_OWNER) is not None
    _publish_zero_successor(engine,
                            info,
                            target=2 if mutation == 'positive_target' else 0)
    with engine.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        if mutation == 'payload_digest_mismatch':
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
