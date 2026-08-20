"""PostgreSQL contracts for grant-before-row zero-cost actuation."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import datetime
import time
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky import clouds
from sky.serve import capacity_admission
from sky.serve import constants as serve_constants
from sky.serve import pool_capacity_observation
from sky.serve import pool_capacity_observation_schema
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import serve_utils
from sky.serve import service_spec
from sky.serve import spot_placer
from sky.serve import zero_cost_actuation
from sky.serve import zero_cost_actuation_schema
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_zero_cost_actuation_schema_052_pg')

_SERVICE_HASH = 'service-incarnation'
_CONTROLLER_PID = 41
_CONTROLLER_IP = '10.0.0.7'
_CONTROLLER_PORT = 8123
_OWNER = serve_utils.make_controller_owner_fingerprint(_SERVICE_HASH,
                                                       _CONTROLLER_PID,
                                                       _CONTROLLER_IP,
                                                       _CONTROLLER_PORT)


def _plan(
    *,
    free_slots: int = 2,
    accelerator_count: int = 1,
    context: str = 'context-a',
    physical_uid: str = 'uid-a',
    valid_until: float | None = None,
    capacity_unit: reserved_fill_planner.FillCapacityUnit = (
        reserved_fill_planner.FillCapacityUnit.PHYSICAL)
) -> reserved_fill_planner.FillPlan:
    location = spot_placer.Location(cloud=clouds.Kubernetes(),
                                    region=context,
                                    zone=None,
                                    accelerators={'L4': accelerator_count},
                                    use_spot=False)
    pool_key = reserved_capacity_broker.make_pool_key(
        context,
        'L4',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=physical_uid)
    snapshot = reserved_fill_planner.PoolFillSnapshot.from_mapping({
        'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
        'pool_key': pool_key,
        'physical_cluster_uid': physical_uid,
        'service_generation': 7,
        'worker_projection_sha256_by_accelerator': {
            'l4': 'e' * 64,
        },
        'edge_cap': free_slots,
        'broker_slot_width': accelerator_count,
        'free_slots': free_slots,
        'free_slots_by_accelerator': {
            'l4': free_slots,
        },
        'grant': free_slots,
        'grant_epoch': 23 if free_slots else None,
        'observation_generation': 13,
        'observation_sequence': 17,
        'ordinary_zero_cost_admission_sequence': 17,
        'valid_until':
            (time.time() + 60 if valid_until is None else valid_until),
        'zero_cost_location_keys': [location.to_pickleable()],
    })
    allocation = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=19,
        ordinary_zero_cost_admission_sequence_high_water=17,
        reconciliation_gate_generation=29,
        reclaim_fleet_bundle_sha256='c' * 64,
        reclaim_policy_revision='reclaim-v1',
        reclaim_provider_inventory_sha256='d' * 64,
        pool_snapshots=(snapshot,))
    return reserved_fill_planner.ReservedFillPlanner.plan(
        policy_revision=2,
        reconcile_generation=3,
        allocation_map=allocation,
        service_incarnation=_SERVICE_HASH,
        service_version=19,
        controller_owner=_OWNER,
        max_replicas=100,
        planned_replicas=0,
        capacity_unit=capacity_unit)


@pytest.fixture
def actuation_database(empty_postgres, monkeypatch):
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '052')
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        empty_postgres)
    incarnation = uuid.uuid4()
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='svc',
                workspace='workspace-a',
                status='READY',
                hash=_SERVICE_HASH,
                current_version=19,
                active_versions='[19]',
                pool=0,
                lifecycle_epoch=3,
                controller_incarnation=incarnation,
                controller_owner_epoch=4,
                controller_pid=_CONTROLLER_PID,
                controller_ip=_CONTROLLER_IP,
                controller_port=_CONTROLLER_PORT,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=1,
                reserved_fill_actuation_mode='DURABLE_INTENT',
                reserved_fill_actuation_epoch=1,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=incarnation,
                reserved_fill_actuation_protocol_version=1))
    return empty_postgres


def _grant_plan(
    repository: zero_cost_actuation.ZeroCostActuationRepository,
    plan: reserved_fill_planner.FillPlan,
    *,
    max_capacity: int,
) -> reserved_fill_planner.FillCommitResult:
    with repository.engine.connect() as connection:
        controller = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.controller_incarnation,
                serve_state_schema.services_table.c.controller_owner_epoch).
            where(serve_state_schema.services_table.c.name ==
                  'svc')).mappings().one()
    return repository.grant_plan(
        'svc',
        plan,
        max_capacity=max_capacity,
        expected_controller_incarnation=controller['controller_incarnation'],
        expected_controller_owner_epoch=controller['controller_owner_epoch'])


def test_serve052_lineage_and_postgresql_only() -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    revision = scripts.get_revision('052')
    assert scripts.get_heads() == ['055']
    assert revision.down_revision == '051'
    assert migration_utils.SERVE_VERSION == '055'
    assert migration_utils.serve_target_version(sqlite) == '037'
    with pytest.raises(RuntimeError, match='PostgreSQL-only'):
        alembic_command.upgrade(config, '052')


def test_grant_is_idempotent_and_allocates_no_replica(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=2)

    first = _grant_plan(repository, plan, max_capacity=2)
    second = _grant_plan(repository, plan, max_capacity=2)

    assert len(first.accepted) == 2
    assert [item.replica_id for item in first.accepted] == [None, None]
    assert second.accepted == first.accepted
    with actuation_database.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table)).mappings().all()
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one()
    assert len(rows) == 2
    assert {row['state'] for row in rows} == {'GRANTED'}
    assert replica_count == 0


def test_status_summary_keeps_intents_separate_from_replicas(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=2)
    _grant_plan(repository, plan, max_capacity=2)

    summary = zero_cost_actuation.get_status_summary('svc',
                                                     _SERVICE_HASH,
                                                     engine=actuation_database)

    assert summary == {
        'zero_cost_actuation_status': 'available',
        'zero_cost_actuation_reason': 'complete',
        'zero_cost_actuation_mode': 'DURABLE_INTENT',
        'zero_cost_actuation_epoch': 1,
        'zero_cost_actuation_state_counts': {
            'GRANTED': 2,
            'ACTUATING': 0,
            'COMMITTED': 0,
            'RETRYABLE': 0,
            'TERMINAL': 0,
        },
        'pending_zero_cost_actuation_count': 2,
    }
    assert zero_cost_actuation.get_status_summary(
        'svc', 'stale-hash', engine=actuation_database
    ) == zero_cost_actuation.unavailable_status_summary('service_hash_mismatch')


def test_capability_advertisement_does_not_promote_service(
        actuation_database) -> None:
    services = serve_state_schema.services_table
    with actuation_database.begin() as connection:
        incarnation = connection.execute(
            sqlalchemy.select(services.c.controller_incarnation).where(
                services.c.name == 'svc')).scalar_one()
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                reserved_fill_actuation_mode='DIRECT_REPLICA',
                reserved_fill_actuation_epoch=0,
                reserved_fill_actuation_capable=False,
                reserved_fill_actuation_controller_incarnation=None,
                reserved_fill_actuation_protocol_version=None))

    mode = zero_cost_actuation.advertise_capability('svc',
                                                    incarnation,
                                                    engine=actuation_database)

    with actuation_database.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == 'svc')).mappings().one()
    assert mode is zero_cost_actuation.ActuationMode.DIRECT_REPLICA
    assert service['reserved_fill_actuation_mode'] == 'DIRECT_REPLICA'
    assert service['reserved_fill_actuation_epoch'] == 0
    assert service['reserved_fill_actuation_capable'] is True
    assert (service['reserved_fill_actuation_controller_incarnation'] ==
            incarnation)
    assert service['reserved_fill_actuation_protocol_version'] == 1


def test_promotion_requires_fleet_barrier_and_is_one_way(
        actuation_database) -> None:
    services = serve_state_schema.services_table
    with actuation_database.begin() as connection:
        incarnation = connection.execute(
            sqlalchemy.select(services.c.controller_incarnation).where(
                services.c.name == 'svc')).scalar_one()
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                reserved_fill_actuation_mode='DIRECT_REPLICA',
                reserved_fill_actuation_epoch=0,
                ordinary_launch_binding_epoch=2,
                non_pool_launch_binding_capable=True,
                non_pool_launch_controller_incarnation=incarnation,
                non_pool_launch_binding_protocol_version=2,
                non_pool_launch_capability_profile_set_digest='a' * 64,
                non_pool_launch_capability_cohort_epoch=1,
                non_pool_launch_receipt_protocol_version=1,
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
            sqlalchemy.update(
                pool_capacity_observation_schema.protocol_state_sequence_table).
            where(pool_capacity_observation_schema.
                  protocol_state_sequence_table.c.id == 1).values(
                      protocol_version=2,
                      image_digest='sha256:' + '1' * 64,
                      deployment_generation='deployment-1',
                      deployment_uid='deployment-uid-1',
                      pod_inventory_count=1,
                      pod_inventory_sha256='2' * 64,
                      reconciliation_gate_state='SEQUENCED_ACTIVE',
                      reconciliation_gate_generation=1,
                      reclaim_fleet_bundle_sha256='3' * 64,
                      reclaim_policy_revision='reclaim-v1',
                      reclaim_provider_inventory_sha256='4' * 64,
                      reclaim_claim_scope_count=0,
                      reclaim_claim_scope_sha256='5' * 64,
                      reclaim_evidence_sha256='6' * 64,
                      reclaim_authorized_at=1.0))

    with pytest.raises(zero_cost_actuation.ZeroCostActuationUnavailable):
        with actuation_database.begin() as connection:
            zero_cost_actuation.promote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=incarnation,
                expected_actuation_epoch=0,
                participant_barrier_passed=False)
    with actuation_database.begin() as connection:
        epoch = zero_cost_actuation.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            expected_actuation_epoch=0,
            participant_barrier_passed=True)
    assert epoch == 1
    with actuation_database.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == 'svc')).mappings().one()
    assert service['reserved_fill_actuation_mode'] == 'DURABLE_INTENT'
    assert service['reserved_fill_actuation_epoch'] == 1
    with actuation_database.begin() as connection:
        assert zero_cost_actuation.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            expected_actuation_epoch=0,
            participant_barrier_passed=False) == 1
    with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict):
        with actuation_database.begin() as connection:
            zero_cost_actuation.promote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=incarnation,
                expected_actuation_epoch=1,
                participant_barrier_passed=True)


def test_pending_grants_enforce_headroom_and_debit_paid_residual(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=2)
    receipt = _grant_plan(repository, plan, max_capacity=1)
    assert len(receipt.accepted) == 1
    assert len(receipt.deferred) == 1
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)

    with actuation_database.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        pending = capacity_admission._locked_pending_zero_cost_inventory(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            service_version=19,
            accounting_cards={'l4'},
            now=now)
    assert pending == {'l4': 1}
    assert capacity_admission._paid_residual({'l4': 2}, {'l4': 0}, pending,
                                             {'l4': 0}) == {
                                                 'l4': 1
                                             }


@pytest.mark.parametrize(('utilization_gate', 'expected_intents'), [(False, 2),
                                                                    (True, 0)],
                         ids=('full-backfill', 'idle-gated'))
def test_idle_gate_controls_width_adjusted_durable_intents_without_paid_spill(
        actuation_database, monkeypatch, utilization_gate: bool,
        expected_intents: int) -> None:
    spec = service_spec.SkyServiceSpec(readiness_path='/health',
                                       initial_delay_seconds=0,
                                       readiness_timeout_seconds=5,
                                       endpoint_probe_interval_seconds=1,
                                       lb_stream_timeout_seconds=10,
                                       min_replicas=0,
                                       max_replicas=16,
                                       target_concurrency_per_replica=1,
                                       reserved_capacity_fill={
                                           'floor_replicas': 0,
                                           'weight': 100,
                                           'utilization_gate': utilization_gate,
                                       })
    rendered_spec = spec.to_yaml_config()
    assert rendered_spec['replica_policy']['min_replicas'] == 0
    assert rendered_spec['replica_policy']['reserved_capacity_fill'] == {
        'weight': 100.0,
        'utilization_gate': utilization_gate,
    }
    spec = service_spec.SkyServiceSpec.from_yaml_config(rendered_spec)
    assert spec.min_replicas == 0
    assert spec.reserved_fill_floor_replicas == 0
    assert spec.reserved_fill_utilization_gate is utilization_gate

    raw_capacity = pool_capacity_observation.PoolCapacitySuccess.from_counts(
        16, {'L4': 16})
    slots_by_accelerator = dict(raw_capacity.slot_counts(8))
    assert slots_by_accelerator == {'l4': 2}
    available_slots = sum(slots_by_accelerator.values())

    claims = {
        'svc': reserved_capacity_broker.ClaimInput(
            floor=spec.reserved_fill_floor_replicas,
            weight=spec.reserved_fill_weight,
            holdings_fill=0,
            launchable=True,
            effective_cap=available_slots)
    }
    monkeypatch.delenv(serve_constants.RESERVED_FILL_UTILIZATION_GATE_ENV_VAR,
                       raising=False)
    gated_claims, _ = reserved_capacity_broker._apply_utilization_gate(
        claims, {
            'svc': reserved_capacity_broker.ActivityInput(
                armed=spec.reserved_fill_utilization_gate,
                demonstrated_need=0,
                boot_hold=False,
                blind=not spec.reserved_fill_utilization_gate)
        }, {}, 1000.0)
    entitlement = reserved_capacity_broker.compute_entitlements(
        available_slots, gated_claims)['svc']
    assert entitlement == expected_intents

    plan = _plan(free_slots=entitlement, accelerator_count=8)
    assert len(plan.intents) == expected_intents
    assert all(intent.accelerator_count == 8 for intent in plan.intents)
    assert all(
        location.cloud.casefold() == 'kubernetes' and not location.use_spot
        for intent in plan.intents
        for location in intent.allowed_locations)

    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    receipt = _grant_plan(repository, plan, max_capacity=2)
    assert len(receipt.accepted) == expected_intents
    assert not receipt.deferred

    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with actuation_database.begin() as connection:
        intent_rows = connection.execute(
            sqlalchemy.select(intents)).mappings().all()
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one()
        paid_claim_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.paid_capacity_claims_table)).scalar_one()
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        pending = capacity_admission._locked_pending_zero_cost_inventory(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            service_version=19,
            accounting_cards={'l4'},
            now=now)

    assert len(intent_rows) == expected_intents
    assert {row['state'] for row in intent_rows
           } == ({'GRANTED'} if expected_intents else set())
    assert replica_count == 0
    assert paid_claim_count == 0
    assert pending == {'l4': expected_intents}
    assert capacity_admission._paid_residual({'l4': expected_intents},
                                             {'l4': 0}, pending,
                                             {'l4': 0}) == {}
    assert capacity_admission._paid_residual({'l4': 0}, {'l4': 0}, pending,
                                             {'l4': 0}) == {}


def test_pool_leases_are_independent_and_retryable(actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    east = _plan(free_slots=1, context='east', physical_uid='uid-east')
    west = _plan(free_slots=1, context='west', physical_uid='uid-west')
    _grant_plan(repository, east, max_capacity=2)
    _grant_plan(repository, west, max_capacity=2)
    owner = uuid.uuid4()

    east_lease = repository.lease_next(service_name='svc',
                                       pool_key=east.intents[0].pool_key,
                                       owner=owner,
                                       lease_seconds=30)
    west_lease = repository.lease_next(service_name='svc',
                                       pool_key=west.intents[0].pool_key,
                                       owner=owner,
                                       lease_seconds=30)

    assert east_lease is not None
    assert west_lease is not None
    assert repository.lease_next(service_name='svc',
                                 pool_key=east.intents[0].pool_key,
                                 owner=owner,
                                 lease_seconds=30) is None
    assert repository.release_retryable(east_lease, 'provider_busy')
    retried = repository.lease_next(service_name='svc',
                                    pool_key=east.intents[0].pool_key,
                                    owner=owner,
                                    lease_seconds=30)
    assert retried is not None
    assert retried.generation == east_lease.generation + 1


def _replica_for_intent(intent: reserved_fill_planner.FillIntent,
                        replica_id: int) -> replica_managers.ReplicaInfo:
    location = intent.allowed_locations[0].to_location()
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'svc-{replica_id}',
        replica_port='8080',
        is_spot=False,
        location=location,
        version=intent.service_version,
        resources_override=location.to_dict(),
        planned_capacity=intent.capacity_unit.intent_cost(
            intent.accelerator_count))
    info.reserved_fill = True
    info.is_zero_cost = True
    info.reserved_fill_pool_key = intent.pool_key
    info.reserved_fill_service_generation = intent.service_generation
    info.reserved_fill_physical_cluster_uid = intent.physical_cluster_uid
    info.reserved_fill_kubernetes_context = intent.allowed_locations[0].region
    info.reserved_fill_allocation_generation = intent.allocation_generation
    info.reserved_fill_allocation_input_sha256 = intent.allocation_input_sha256
    info.reserved_fill_allocation_claim_generation = (
        intent.allocation_claim_generation)
    info.reserved_fill_reconciliation_gate_generation = (
        intent.reconciliation_gate_generation)
    info.reserved_fill_reclaim_fleet_bundle_sha256 = (
        intent.reclaim_fleet_bundle_sha256)
    info.reserved_fill_reclaim_policy_revision = intent.reclaim_policy_revision
    info.reserved_fill_reclaim_provider_inventory_sha256 = (
        intent.reclaim_provider_inventory_sha256)
    info.reserved_fill_worker_projection_sha256 = (
        intent.worker_projection_sha256)
    info.reserved_fill_observation_generation = intent.observation_generation
    info.reserved_fill_observation_sequence = intent.observation_sequence
    info.reserved_fill_intent_idempotency_key = intent.idempotency_key
    return info


def test_replica_and_intent_commit_in_one_transaction(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    _grant_plan(repository, plan, max_capacity=1)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 1)
    record_id = uuid.UUID(info.replica_record_id)

    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **serve_state._replica_row_values('svc', 1, info)))
        zero_cost_actuation.commit_lease_in_connection(
            connection,
            lease,
            service_name='svc',
            replica_id=1,
            replica_record_id=record_id,
            replica_info=info)

    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with actuation_database.connect() as connection:
        row = connection.execute(sqlalchemy.select(intents)).mappings().one()
    assert row['state'] == 'COMMITTED'
    assert row['replica_id'] == 1
    assert row['replica_record_id'] == record_id


def test_committed_intent_exactly_owns_replica(actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    _grant_plan(repository, plan, max_capacity=1)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 1)
    record_id = uuid.UUID(info.replica_record_id)
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **serve_state._replica_row_values('svc', 1, info)))
        zero_cost_actuation.commit_lease_in_connection(
            connection,
            lease,
            service_name='svc',
            replica_id=1,
            replica_record_id=record_id,
            replica_info=info)

    with actuation_database.connect() as connection:
        assert zero_cost_actuation.committed_intent_for_replica_in_connection(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            replica_info=info) == lease.intent
        assert zero_cost_actuation.committed_intent_matches_replica_in_connection(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            replica_info=info)
        info.reserved_fill_physical_cluster_uid = 'other-uid'
        assert zero_cost_actuation.committed_intent_for_replica_in_connection(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            replica_info=info) is None
        assert not zero_cost_actuation.committed_intent_matches_replica_in_connection(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            replica_info=info)


def test_committed_replica_id_high_water_survives_replica_cleanup(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    _grant_plan(repository, plan, max_capacity=1)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 9)
    record_id = uuid.UUID(info.replica_record_id)

    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **serve_state._replica_row_values('svc', 9, info)))
        zero_cost_actuation.commit_lease_in_connection(
            connection,
            lease,
            service_name='svc',
            replica_id=9,
            replica_record_id=record_id,
            replica_info=info)
        connection.execute(
            sqlalchemy.delete(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 9))

    assert repository.committed_replica_id_high_water('svc') == 9
    assert repository.committed_replica_id_high_water('other') == 0


def test_intent_mismatch_rolls_back_replica_insert(actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    _grant_plan(repository, plan, max_capacity=1)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 1)
    info.reserved_fill_physical_cluster_uid = 'different-uid'

    with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict):
        with actuation_database.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state_schema.replicas_table).values(
                    **serve_state._replica_row_values('svc', 1, info)))
            zero_cost_actuation.commit_lease_in_connection(
                connection,
                lease,
                service_name='svc',
                replica_id=1,
                replica_record_id=uuid.UUID(info.replica_record_id),
                replica_info=info)

    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with actuation_database.connect() as connection:
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one()
        state = connection.execute(sqlalchemy.select(
            intents.c.state)).scalar_one()
    assert replica_count == 0
    assert state == 'ACTUATING'


def test_expired_retryable_grant_releases_paid_debit(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1, valid_until=time.time() + 0.3)
    _grant_plan(repository, plan, max_capacity=1)
    time.sleep(0.4)

    with actuation_database.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        pending = capacity_admission._locked_pending_zero_cost_inventory(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            service_version=19,
            accounting_cards={'l4'},
            now=now)
        state = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table.c.state)).scalar_one()
    assert pending == {'l4': 0}
    assert state == 'TERMINAL'


def test_schema_rejects_invalid_state_shape(actuation_database) -> None:
    plan = _plan(free_slots=1)
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    _grant_plan(repository, plan, max_capacity=1)
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with actuation_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(intents).values(
                    state='COMMITTED',
                    replica_id=1,
                    committed_at=datetime.datetime.now(datetime.timezone.utc)))
