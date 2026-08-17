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
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import serve_state_schema
from sky.serve import serve_utils
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
    context: str = 'context-a',
    physical_uid: str = 'uid-a',
    valid_until: float | None = None,
    capacity_unit: reserved_fill_planner.FillCapacityUnit = (
        reserved_fill_planner.FillCapacityUnit.PHYSICAL)
) -> reserved_fill_planner.FillPlan:
    location = spot_placer.Location(cloud=clouds.Kubernetes(),
                                    region=context,
                                    zone=None,
                                    accelerators={'L4': 1},
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
        'broker_slot_width': 1,
        'free_slots': free_slots,
        'free_slots_by_accelerator': {
            'l4': free_slots,
        },
        'grant': free_slots,
        'grant_epoch': 23,
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
                reserved_fill_actuation_mode='DURABLE_INTENT',
                reserved_fill_actuation_epoch=1,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=incarnation,
                reserved_fill_actuation_protocol_version=1))
    return empty_postgres


def test_serve052_lineage_and_postgresql_only() -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    revision = scripts.get_revision('052')
    assert scripts.get_heads() == ['052']
    assert revision.down_revision == '051'
    assert migration_utils.SERVE_VERSION == '052'
    assert migration_utils.serve_target_version(sqlite) == '037'
    with pytest.raises(RuntimeError, match='PostgreSQL-only'):
        alembic_command.upgrade(config, '052')


def test_grant_is_idempotent_and_allocates_no_replica(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=2)

    first = repository.grant_plan('svc', plan, max_capacity=2)
    second = repository.grant_plan('svc', plan, max_capacity=2)

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


def test_pending_grants_enforce_headroom_and_debit_paid_residual(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=2)
    receipt = repository.grant_plan('svc', plan, max_capacity=1)
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


def test_pool_leases_are_independent_and_retryable(actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    east = _plan(free_slots=1, context='east', physical_uid='uid-east')
    west = _plan(free_slots=1, context='west', physical_uid='uid-west')
    repository.grant_plan('svc', east, max_capacity=2)
    repository.grant_plan('svc', west, max_capacity=2)
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


def test_expired_retryable_grant_releases_paid_debit(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1, valid_until=time.time() + 0.3)
    repository.grant_plan('svc', plan, max_capacity=1)
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
    repository.grant_plan('svc', plan, max_capacity=1)
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with actuation_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(intents).values(
                    state='COMMITTED',
                    replica_id=1,
                    committed_at=datetime.datetime.now(datetime.timezone.utc)))
