"""PostgreSQL contracts for promoted capacity-authority takeover."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import concurrent.futures
import threading
import time
import uuid

import pytest
import sqlalchemy
from test_serve_capacity_admission_pg import _demand_report
from test_serve_capacity_admission_pg import _plan_and_admit_target
from test_serve_capacity_admission_pg import _route_identities
from test_serve_capacity_admission_pg import _route_response
from test_serve_capacity_admission_pg import capacity_database
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401
from test_zero_cost_actuation_pg import _commit_and_insert_replica
from test_zero_cost_actuation_pg import _grant_plan as _grant_zero_cost_plan
from test_zero_cost_actuation_pg import _install_fresh_provider_proofs
from test_zero_cost_actuation_pg import _plan as _zero_cost_plan
from test_zero_cost_actuation_pg import _replica_for_intent
from test_zero_cost_actuation_pg import actuation_database

from sky.serve import capacity_admission
from sky.serve import demand_state
from sky.serve import demand_state_schema
from sky.serve import ordinary_launch_binding
from sky.serve import route_projection
from sky.serve import serve_state_schema
from sky.serve import zero_cost_actuation
from sky.serve import zero_cost_actuation_schema

pytestmark = pytest.mark.xdist_group(
    name='serve_capacity_takeover_schema_052_pg')


def _install_promoted_pair(engine, incarnation: uuid.UUID) -> None:
    services = serve_state_schema.services_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                reserved_fill_actuation_mode='DURABLE_INTENT',
                reserved_fill_actuation_epoch=1,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=incarnation,
                reserved_fill_actuation_protocol_version=1))


def _transfer(engine, expected_incarnation: uuid.UUID, expected_epoch: int,
              new_incarnation: uuid.UUID):
    with engine.begin() as connection:
        return ordinary_launch_binding.transfer_service_owner_in_connection(
            connection,
            service_name='svc',
            expected_incarnation=expected_incarnation,
            expected_owner_epoch=expected_epoch,
            new_incarnation=new_incarnation,
            new_controller_pid=123,
            new_controller_ip='10.0.0.5',
            capable=True)


def test_takeover_rebinds_pair_but_requires_new_route_and_report(
        capacity_database) -> None:
    engine, old_incarnation, old_route = capacity_database
    _install_promoted_pair(engine, old_incarnation)
    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is not None
    reports = demand_state_schema.serve_lb_demand_reports_table
    with engine.connect() as connection:
        predecessor_report = connection.execute(
            sqlalchemy.select(reports.c.payload).where(
                reports.c.service_name == 'svc')).scalar_one()

    new_incarnation = uuid.uuid4()
    authority = _transfer(engine, old_incarnation, 4, new_incarnation)
    assert authority.controller_incarnation == new_incarnation

    services = serve_state_schema.services_table
    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == 'svc')).mappings().one()
    assert service['demand_source_mode'] == 'DURABLE_FEED'
    assert service['demand_source_epoch'] == 1
    assert service['reserved_fill_actuation_mode'] == 'DURABLE_INTENT'
    assert service['reserved_fill_actuation_epoch'] == 1
    assert (
        service['demand_authority_controller_incarnation'] == new_incarnation)
    assert (service['reserved_fill_actuation_controller_incarnation'] ==
            new_incarnation)
    # The takeover does not bless the old controller's route/report lineage.
    assert service['route_projection_controller_incarnation'] == old_incarnation
    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is None

    # Replaying the predecessor's exact accepted sequence/digest remains a
    # telemetry duplicate.  It neither refreshes its DB-owned validity nor
    # restores demand authority after the owner fence changes.
    replay_receipt = demand_state.ingest_report('svc', 'svc-hash',
                                                predecessor_report)
    assert replay_receipt.duplicate
    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is None

    repository = route_projection.RouteProjectionRepository(engine)
    identity = route_projection.RoutePublisherIdentity(
        service_name='svc',
        service_hash='svc-hash',
        service_lifecycle_epoch=3,
        controller_incarnation=new_incarnation,
        controller_owner_epoch=5,
        controller_pid=123,
        controller_ip='10.0.0.5')
    record_id = str(uuid.uuid4())
    new_route = repository.publish(identity,
                                   1,
                                   _route_response(),
                                   _route_identities(record_id), {record_id},
                                   ttl_seconds=60)
    assert new_route.generation > old_route.generation
    # The old fresh report names old_route, so a new route alone grants no
    # demand or actuation authority.
    assert demand_state.get_autoscaling_snapshot('svc', 'svc-hash') is None

    demand_state.ingest_report(
        'svc', 'svc-hash', _demand_report(time.time(), new_route, sequence=2))
    snapshot = demand_state.get_autoscaling_snapshot('svc', 'svc-hash')
    assert snapshot is not None
    assert snapshot.demand_source_epoch == 1
    assert snapshot.route_generation == new_route.generation
    paid_authority = _plan_and_admit_target(engine, 1)
    assert paid_authority.demand_source_epoch == 1
    assert paid_authority.demand_feed_generation == (
        snapshot.demand_feed_generation)


def test_restart_repairs_stale_demand_owner_without_advancing_epochs(
        capacity_database) -> None:
    engine, current_incarnation, _ = capacity_database
    _install_promoted_pair(engine, current_incarnation)
    stale_incarnation = uuid.uuid4()
    services = serve_state_schema.services_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                demand_authority_controller_incarnation=stale_incarnation))

    replacement_incarnation = uuid.uuid4()
    authority = ordinary_launch_binding.claim_controller_incarnation(
        'svc', 'svc-hash', (123, '10.0.0.5'), replacement_incarnation)
    assert authority is not None
    assert authority.controller_incarnation == replacement_incarnation
    assert authority.controller_owner_epoch == 5

    # A second supervised restart repeats the same idempotent re-advertisement
    # under a fresh owner fence; neither one-way source epoch changes.
    next_incarnation = uuid.uuid4()
    next_authority = ordinary_launch_binding.claim_controller_incarnation(
        'svc', 'svc-hash', (123, '10.0.0.5'), next_incarnation)
    assert next_authority is not None
    assert next_authority.controller_owner_epoch == 6
    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == 'svc')).mappings().one()
    assert service['controller_incarnation'] == next_incarnation
    assert service[
        'demand_authority_controller_incarnation'] == next_incarnation
    assert (service['reserved_fill_actuation_controller_incarnation'] ==
            next_incarnation)
    assert service['demand_source_epoch'] == 1
    assert service['reserved_fill_actuation_epoch'] == 1


@pytest.mark.parametrize('demand_mode,actuation_mode', [
    ('DURABLE_FEED', 'DIRECT_REPLICA'),
    ('LEGACY_CONTROLLER', 'DURABLE_INTENT'),
])
def test_takeover_rejects_partial_promotion_and_rolls_back_owner(
        capacity_database, demand_mode: str, actuation_mode: str) -> None:
    engine, old_incarnation, _ = capacity_database
    new_incarnation = uuid.uuid4()
    services = serve_state_schema.services_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                demand_source_mode=demand_mode,
                demand_source_epoch=(1 if demand_mode == 'DURABLE_FEED' else 0),
                demand_authority_capable=(demand_mode == 'DURABLE_FEED'),
                demand_authority_controller_incarnation=(
                    old_incarnation if demand_mode == 'DURABLE_FEED' else None),
                demand_authority_protocol_version=(1 if demand_mode
                                                   == 'DURABLE_FEED' else None),
                reserved_fill_actuation_mode=actuation_mode,
                reserved_fill_actuation_epoch=(1 if actuation_mode
                                               == 'DURABLE_INTENT' else 0),
                reserved_fill_actuation_capable=(
                    actuation_mode == 'DURABLE_INTENT'),
                reserved_fill_actuation_controller_incarnation=(
                    old_incarnation
                    if actuation_mode == 'DURABLE_INTENT' else None),
                reserved_fill_actuation_protocol_version=(
                    1 if actuation_mode == 'DURABLE_INTENT' else None)))

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict):
        _transfer(engine, old_incarnation, 4, new_incarnation)

    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == 'svc')).mappings().one()
    assert service['controller_incarnation'] == old_incarnation
    assert service['controller_owner_epoch'] == 4
    assert service['demand_source_mode'] == demand_mode
    assert service['reserved_fill_actuation_mode'] == actuation_mode
    assert service['demand_authority_controller_incarnation'] == (
        old_incarnation if demand_mode == 'DURABLE_FEED' else None)
    assert service['reserved_fill_actuation_controller_incarnation'] == (
        old_incarnation if actuation_mode == 'DURABLE_INTENT' else None)


def test_losing_concurrent_takeover_cannot_rebind_capacity_pair(
        capacity_database) -> None:
    engine, old_incarnation, _ = capacity_database
    _install_promoted_pair(engine, old_incarnation)
    winner_incarnation = uuid.uuid4()
    loser_incarnation = uuid.uuid4()
    winner_transferred = threading.Event()
    release_winner = threading.Event()
    loser_started = threading.Event()

    def _winner():
        with engine.begin() as connection:
            authority = (
                ordinary_launch_binding.transfer_service_owner_in_connection(
                    connection,
                    service_name='svc',
                    expected_incarnation=old_incarnation,
                    expected_owner_epoch=4,
                    new_incarnation=winner_incarnation,
                    new_controller_pid=123,
                    new_controller_ip='10.0.0.5',
                    capable=True))
            winner_transferred.set()
            assert release_winner.wait(5)
            return authority

    def _loser():
        loser_started.set()
        return _transfer(engine, old_incarnation, 4, loser_incarnation)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        winner = executor.submit(_winner)
        assert winner_transferred.wait(5)
        loser = executor.submit(_loser)
        assert loser_started.wait(5)
        assert not loser.done()
        release_winner.set()
        assert winner.result(
            timeout=5).controller_incarnation == (winner_incarnation)
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict):
            loser.result(timeout=5)

    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc')).mappings().one()
    assert service['controller_incarnation'] == winner_incarnation
    assert service['controller_owner_epoch'] == 5
    assert (service['demand_authority_controller_incarnation'] ==
            winner_incarnation)
    assert (service['reserved_fill_actuation_controller_incarnation'] ==
            winner_incarnation)


def test_takeover_retires_pre_row_intents_before_new_demand(
        actuation_database) -> None:
    engine = actuation_database
    repository = zero_cost_actuation.ZeroCostActuationRepository(engine)
    plan = _zero_cost_plan(free_slots=2)
    _grant_zero_cost_plan(repository, plan, max_capacity=2)
    services = serve_state_schema.services_table
    with engine.begin() as connection:
        incarnation = connection.execute(
            sqlalchemy.select(services.c.controller_incarnation).where(
                services.c.name == 'svc')).scalar_one()
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                demand_source_mode='DURABLE_FEED',
                demand_source_epoch=1,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=incarnation,
                demand_authority_protocol_version=1))

    _transfer(engine, incarnation, 4, uuid.uuid4())

    intents = (
        zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table)
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(intents.c.state, intents.c.last_error,
                              intents.c.terminal_at).order_by(
                                  intents.c.intent_idempotency_key)).all()
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one()
    assert len(rows) == 2
    assert all(row.state == 'TERMINAL' for row in rows)
    assert all(row.last_error == 'controller_owner_changed' for row in rows)
    assert all(row.terminal_at is not None for row in rows)
    assert not repository.actionable_pool_keys(service_name='svc')
    assert replica_count == 0


def test_takeover_wins_uncommitted_replica_handoff_and_rolls_back_row(
        actuation_database) -> None:
    engine = actuation_database
    repository = zero_cost_actuation.ZeroCostActuationRepository(engine)
    plan = _zero_cost_plan(free_slots=1)
    _grant_zero_cost_plan(repository, plan, max_capacity=1)
    _install_fresh_provider_proofs(engine, plan.intents)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 1)
    services = serve_state_schema.services_table
    with engine.begin() as connection:
        incarnation = connection.execute(
            sqlalchemy.select(services.c.controller_incarnation).where(
                services.c.name == 'svc')).scalar_one()
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                demand_source_mode='DURABLE_FEED',
                demand_source_epoch=1,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=incarnation,
                demand_authority_protocol_version=1))

    takeover_locked = threading.Event()
    release_takeover = threading.Event()
    handoff_started = threading.Event()
    replacement = uuid.uuid4()

    def _takeover():
        with engine.begin() as connection:
            authority = (
                ordinary_launch_binding.transfer_service_owner_in_connection(
                    connection,
                    service_name='svc',
                    expected_incarnation=incarnation,
                    expected_owner_epoch=4,
                    new_incarnation=replacement,
                    new_controller_pid=123,
                    new_controller_ip='10.0.0.5',
                    capable=True))
            takeover_locked.set()
            assert release_takeover.wait(5)
            return authority

    def _commit_handoff():
        try:
            with engine.begin() as connection:
                handoff_started.set()
                _commit_and_insert_replica(connection, lease, info)
        except Exception as error:  # pylint: disable=broad-except
            return error
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        takeover = executor.submit(_takeover)
        assert takeover_locked.wait(5)
        handoff = executor.submit(_commit_handoff)
        assert handoff_started.wait(5)
        assert not handoff.done()
        release_takeover.set()
        assert takeover.result(timeout=5).controller_incarnation == replacement
        assert isinstance(handoff.result(timeout=5),
                          zero_cost_actuation.ZeroCostActuationConflict)

    intents = (
        zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table)
    with engine.connect() as connection:
        intent = connection.execute(sqlalchemy.select(intents)).mappings().one()
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one()
    assert intent['state'] == 'TERMINAL'
    assert intent['last_error'] == 'controller_owner_changed'
    assert replica_count == 0


def test_takeover_rejects_unpersisted_plan_after_transport_fingerprint_aba(
        actuation_database) -> None:
    engine = actuation_database
    repository = zero_cost_actuation.ZeroCostActuationRepository(engine)
    predecessor_plan = _zero_cost_plan(free_slots=1)
    services = serve_state_schema.services_table
    with engine.begin() as connection:
        predecessor_incarnation = connection.execute(
            sqlalchemy.select(services.c.controller_incarnation).where(
                services.c.name == 'svc')).scalar_one()
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                demand_source_mode='DURABLE_FEED',
                demand_source_epoch=1,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=(
                    predecessor_incarnation),
                demand_authority_protocol_version=1))

    replacement_incarnation = uuid.uuid4()
    with engine.begin() as connection:
        replacement_authority = (
            ordinary_launch_binding.transfer_service_owner_in_connection(
                connection,
                service_name='svc',
                expected_incarnation=predecessor_incarnation,
                expected_owner_epoch=4,
                new_incarnation=replacement_incarnation,
                new_controller_pid=41,
                new_controller_ip='10.0.0.7',
                capable=True))
    assert ordinary_launch_binding.publish_controller_port_if_authority(
        replacement_authority, 8123)

    # PID, IP, and port now reproduce the predecessor plan's fingerprint.  The
    # old process authority must still fail inside the grant transaction.
    with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict,
                       match='authority changed before grant'):
        repository.grant_plan(
            'svc',
            predecessor_plan,
            max_capacity=1,
            expected_controller_incarnation=predecessor_incarnation,
            expected_controller_owner_epoch=4)

    intents = (
        zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table)
    with engine.connect() as connection:
        intent_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()).select_from(intents)).scalar_one()
    assert intent_count == 0


def test_handoff_wins_takeover_and_committed_replica_survives(
        actuation_database) -> None:
    engine = actuation_database
    repository = zero_cost_actuation.ZeroCostActuationRepository(engine)
    plan = _zero_cost_plan(free_slots=1)
    _grant_zero_cost_plan(repository, plan, max_capacity=1)
    _install_fresh_provider_proofs(engine, plan.intents)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 1)
    record_id = uuid.UUID(info.replica_record_id)
    services = serve_state_schema.services_table
    with engine.begin() as connection:
        incarnation = connection.execute(
            sqlalchemy.select(services.c.controller_incarnation).where(
                services.c.name == 'svc')).scalar_one()
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                demand_source_mode='DURABLE_FEED',
                demand_source_epoch=1,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=incarnation,
                demand_authority_protocol_version=1))

    handoff_locked = threading.Event()
    release_handoff = threading.Event()
    takeover_started = threading.Event()
    replacement = uuid.uuid4()

    def _commit_handoff():
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.select(services.c.name).where(
                    services.c.name == 'svc').with_for_update()).scalar_one()
            _commit_and_insert_replica(connection, lease, info)
            handoff_locked.set()
            assert release_handoff.wait(5)

    def _takeover():
        takeover_started.set()
        return _transfer(engine, incarnation, 4, replacement)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        handoff = executor.submit(_commit_handoff)
        assert handoff_locked.wait(5)
        takeover = executor.submit(_takeover)
        assert takeover_started.wait(5)
        assert not takeover.done()
        release_handoff.set()
        assert handoff.result(timeout=5) is None
        assert takeover.result(timeout=5).controller_incarnation == replacement

    intents = (
        zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table)
    with engine.connect() as connection:
        intent = connection.execute(sqlalchemy.select(intents)).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table.c.replica_id)
        ).mappings().one()
    assert intent['state'] == 'COMMITTED'
    assert intent['replica_id'] == 1
    assert intent['replica_record_id'] == record_id
    assert replica['replica_id'] == 1
