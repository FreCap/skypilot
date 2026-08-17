"""PostgreSQL contracts for ordered SkyServe capacity admission."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import dataclasses
import time
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import capacity_admission
from sky.serve import capacity_admission_schema
from sky.serve import constants
from sky.serve import demand_state
from sky.serve import ordinary_launch_binding
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_capacity_admission_schema_051_pg')

_URL = 'http://replica:8000'


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
                   request_count: int = 1) -> dict:
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
            _URL: 0.1,
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
    alembic_command.upgrade(serve_config, '051')
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
