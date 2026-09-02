"""PostgreSQL contracts for committed capacity-plan history ownership."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import datetime
import time

import pytest
import sqlalchemy
from test_serve_capacity_admission_pg import _current_decision
from test_serve_capacity_admission_pg import _current_owner_kwargs
from test_serve_capacity_admission_pg import capacity_database
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import capacity_admission
from sky.serve import capacity_admission_schema
from sky.serve import serve_history
from sky.serve import serve_state_schema

pytestmark = pytest.mark.xdist_group(
    name='serve_capacity_admission_schema_052_pg')


def _commit(engine: sqlalchemy.engine.Engine,
            target: int,
            *,
            owner_kwargs=None) -> capacity_admission.CommittedCapacityPlan:
    return capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_admit_current(
            **(owner_kwargs or _current_owner_kwargs(engine)),
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 1},
            backend_num_nodes=1,
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot, supply, target))


def test_committed_plan_immediately_persists_typed_history(
        capacity_database, monkeypatch):
    engine, incarnation, _ = capacity_database
    owner_kwargs = _current_owner_kwargs(engine)
    connect_calls = 0
    real_connect = engine.connect

    def counted_connect(*args, **kwargs):
        nonlocal connect_calls
        connect_calls += 1
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(engine, 'connect', counted_connect)
    committed = _commit(engine, 3, owner_kwargs=owner_kwargs)

    # Plan commit and the bounded history transaction reuse one checkout.
    assert connect_calls == 1

    table = serve_history.serve_autoscaler_history_table
    plans = capacity_admission_schema.serve_capacity_plans_table
    with engine.connect() as connection:
        row = connection.execute(sqlalchemy.select(table)).mappings().one()
        plan_created_at = connection.execute(
            sqlalchemy.select(plans.c.created_at).where(
                plans.c.service_name == 'svc', plans.c.generation ==
                committed.authority.generation)).scalar_one()

    assert row['controller_session_id'] == incarnation.hex
    assert row['observed_at'] == plan_created_at
    assert row['replica_unit'] == 'physical_backend'
    assert row['demand_target'] == 3
    assert row['capacity_target'] == 3
    assert row['ready_capacity'] == 0
    assert row['provisioning_capacity'] == 0
    assert row['total_capacity'] == 0
    assert row['peak_in_flight'] == sum(
        committed.demand_snapshot.normalized_demand['in_flight_by_replica_id'].
        values())
    assert row['peak_queue_depth'] == (
        committed.demand_snapshot.normalized_demand['queue_depth'])
    breakdown = row['accelerator_breakdown']
    assert breakdown['demand_target'] == {'l4': 3}
    assert breakdown['fill_target'] == {'l4': 0}
    assert breakdown['capacity_plan_generation'] == (
        committed.authority.generation)
    assert breakdown['capacity_plan_sha256'] == (
        committed.authority.content_sha256)
    assert breakdown['capacity_plan_valid_until'] > row[
        'observed_at'].timestamp()


def test_history_failure_does_not_roll_back_plan_and_duplicate_retries(
        capacity_database, monkeypatch):
    engine, _, _ = capacity_database
    writer = serve_history.record_autoscaler_snapshot_in_connection

    def fail_history(*_args, **_kwargs):
        raise RuntimeError('history write failed')

    monkeypatch.setattr(serve_history,
                        'record_autoscaler_snapshot_in_connection',
                        fail_history)

    committed = _commit(engine, 2)

    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                capacity_admission_schema.serve_capacity_plans_table)
        ).scalar_one() == 1
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                capacity_admission_schema.serve_capacity_plan_heads_table)
        ).scalar_one() == 1
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_history.serve_autoscaler_history_table)).scalar_one() == 0

    monkeypatch.setattr(serve_history,
                        'record_autoscaler_snapshot_in_connection', writer)
    retried = _commit(engine, 2)

    assert retried.authority.generation > committed.authority.generation
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_history.serve_autoscaler_history_table)).scalar_one() == 1


def _history_snapshot(
    *,
    observed_at: datetime.datetime,
    demand_target: int,
    peak_in_flight: int,
    committed_plan: bool,
    service_name: str = 'mixed-svc',
    service_hash: str = 'mixed-hash',
) -> serve_history.AutoscalerHistorySnapshot:
    breakdown = {
        'configured_accelerators': ['l4'],
        'demand_target': {
            'l4': demand_target
        },
        'fill_target': {
            'l4': demand_target
        },
    }
    if committed_plan:
        breakdown.update({
            'capacity_plan_generation': 7,
            'capacity_plan_sha256': 'a' * 64,
            'capacity_plan_valid_until':
                (observed_at + datetime.timedelta(seconds=15)).timestamp(),
        })
    return serve_history.AutoscalerHistorySnapshot(
        service_name=service_name,
        service_hash=service_hash,
        controller_session_id=('a' if committed_plan else 'b') * 32,
        version=1,
        replica_unit='logical_slot',
        demand_target=demand_target,
        capacity_target=demand_target,
        ready_capacity=0,
        provisioning_capacity=0,
        total_capacity=0,
        peak_in_flight=peak_in_flight,
        peak_queue_depth=0,
        accelerator_breakdown=breakdown,
        observed_at=observed_at)


def test_committed_writer_wins_same_minute_mixed_rollout(capacity_database):
    engine, _, _ = capacity_database
    bucket = datetime.datetime(2026, 9, 1, 12, 0, tzinfo=datetime.timezone.utc)
    legacy = _history_snapshot(observed_at=bucket +
                               datetime.timedelta(seconds=50),
                               demand_target=9,
                               peak_in_flight=9,
                               committed_plan=False)
    committed = _history_snapshot(observed_at=bucket +
                                  datetime.timedelta(seconds=10),
                                  demand_target=3,
                                  peak_in_flight=3,
                                  committed_plan=True)
    later_legacy = _history_snapshot(observed_at=bucket +
                                     datetime.timedelta(seconds=55),
                                     demand_target=11,
                                     peak_in_flight=11,
                                     committed_plan=False)

    with engine.begin() as connection:
        serve_history.record_autoscaler_snapshot_in_connection(
            connection, legacy)
        serve_history.record_autoscaler_snapshot_in_connection(
            connection, committed)
        serve_history.record_autoscaler_snapshot_in_connection(
            connection, later_legacy)

    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_history.serve_autoscaler_history_table).where(
                    serve_history.serve_autoscaler_history_table.c.service_name
                    == 'mixed-svc')).mappings().one()
    assert row['observed_at'] == committed.observed_at
    assert row['controller_session_id'] == committed.controller_session_id
    assert row['demand_target'] == 3
    assert row['capacity_target'] == 3
    assert row['peak_in_flight'] == 11
    assert row['accelerator_breakdown']['capacity_plan_generation'] == 7


def test_legacy_writer_is_gated_by_locked_source_mode(capacity_database,
                                                      monkeypatch):
    engine, _, _ = capacity_database
    monkeypatch.setattr(serve_history, '_postgres_engine', lambda: engine)
    observed_at = datetime.datetime.now(datetime.timezone.utc)
    legacy = _history_snapshot(observed_at=observed_at,
                               demand_target=1,
                               peak_in_flight=1,
                               committed_plan=False,
                               service_name='svc',
                               service_hash='svc-hash')

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    demand_source_mode='LEGACY_CONTROLLER'))
    assert serve_history.record_autoscaler_snapshot(
        service_name=legacy.service_name,
        service_hash=legacy.service_hash,
        controller_session_id=legacy.controller_session_id,
        version=legacy.version,
        replica_unit=legacy.replica_unit,
        demand_target=legacy.demand_target,
        capacity_target=legacy.capacity_target,
        ready_capacity=legacy.ready_capacity,
        provisioning_capacity=legacy.provisioning_capacity,
        total_capacity=legacy.total_capacity,
        peak_in_flight=legacy.peak_in_flight,
        peak_queue_depth=legacy.peak_queue_depth,
        accelerator_breakdown=legacy.accelerator_breakdown,
        timestamp=legacy.observed_at.timestamp(),
        required_source_mode='LEGACY_CONTROLLER') == 1

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == 'svc').values(
                    demand_source_mode='DURABLE_FEED'))
    assert serve_history.record_autoscaler_snapshot(
        service_name=legacy.service_name,
        service_hash=legacy.service_hash,
        controller_session_id=legacy.controller_session_id,
        version=legacy.version,
        replica_unit=legacy.replica_unit,
        demand_target=9,
        capacity_target=9,
        ready_capacity=legacy.ready_capacity,
        provisioning_capacity=legacy.provisioning_capacity,
        total_capacity=legacy.total_capacity,
        timestamp=(legacy.observed_at +
                   datetime.timedelta(seconds=1)).timestamp(),
        required_source_mode='LEGACY_CONTROLLER') == 0

    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_history.serve_autoscaler_history_table)).mappings().one()
    assert row['demand_target'] == 1


def test_history_only_exposes_projection_matching_current_plan_head(
        capacity_database, monkeypatch):
    engine, _, _ = capacity_database
    monkeypatch.setattr(serve_history, '_postgres_engine', lambda: engine)
    first = _commit(engine, 3)
    writer = serve_history.record_autoscaler_snapshot_in_connection

    def fail_history(*_args, **_kwargs):
        raise RuntimeError('history write failed')

    monkeypatch.setattr(serve_history,
                        'record_autoscaler_snapshot_in_connection',
                        fail_history)
    second = _commit(engine, 4)
    assert second.authority.generation > first.authority.generation

    stale = serve_history.get_status_history('svc', sections={'autoscaler'})
    assert stale['autoscaler_projection_mode'] == 'DURABLE_FEED'
    assert len(stale['autoscaler_samples']) == 1
    assert 'capacity_plan_generation' not in stale['autoscaler_samples'][0][
        'accelerator_breakdown']

    monkeypatch.setattr(serve_history,
                        'record_autoscaler_snapshot_in_connection', writer)
    retried = _commit(engine, 4)
    current = serve_history.get_status_history('svc', sections={'autoscaler'})
    assert current['autoscaler_samples'][0]['accelerator_breakdown'][
        'capacity_plan_generation'] == retried.authority.generation
    assert current['window_end'] <= datetime.datetime.now(
        datetime.timezone.utc).timestamp()


def _latest_history_row(engine: sqlalchemy.engine.Engine):
    table = serve_history.serve_autoscaler_history_table
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.select(table).order_by(
                table.c.bucket_start.desc())).mappings().first()


def test_history_projection_lock_wait_is_bounded_and_never_blocks_plan(
        capacity_database):
    """A locked minute row cannot hold the plan commit past the lock timeout.

    The history transaction runs after the authority commit with a local
    ``lock_timeout``; a concurrent holder of the minute row must therefore
    cost at most that bound, leave the committed plan usable, and lose the
    projection only until the next reconciliation.
    """
    engine, _, _ = capacity_database
    first = _commit(engine, 3)
    assert _latest_history_row(engine)['accelerator_breakdown'][
        'capacity_plan_generation'] == first.authority.generation

    table = serve_history.serve_autoscaler_history_table
    blocker = engine.connect()
    blocking_transaction = blocker.begin()
    try:
        assert len(
            blocker.execute(sqlalchemy.select(
                table).with_for_update()).mappings().all()) == 1
        started = time.monotonic()
        second = _commit(engine, 3)
        elapsed = time.monotonic() - started
    finally:
        blocking_transaction.rollback()
        blocker.close()

    # Plan/head advanced while the projection was locked out within the
    # 250 ms lock timeout (allow generous CI headroom, far below the 1 s
    # statement timeout plus any pool wait).
    assert second.authority.generation > first.authority.generation
    assert elapsed < 5.0
    locked_out = _latest_history_row(engine)
    assert locked_out['accelerator_breakdown'][
        'capacity_plan_generation'] == first.authority.generation
    stale = serve_history.get_status_history('svc', sections={'autoscaler'})
    assert 'capacity_plan_generation' not in stale['autoscaler_samples'][-1][
        'accelerator_breakdown']

    third = _commit(engine, 3)
    assert third.authority.generation > second.authority.generation
    projected = _latest_history_row(engine)
    assert projected['accelerator_breakdown'][
        'capacity_plan_generation'] == third.authority.generation
    current = serve_history.get_status_history('svc', sections={'autoscaler'})
    assert current['autoscaler_samples'][-1]['accelerator_breakdown'][
        'capacity_plan_generation'] == third.authority.generation
