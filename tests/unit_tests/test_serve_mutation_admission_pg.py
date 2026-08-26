"""PostgreSQL contracts for bounded Serve provider-mutation admission."""

# pylint: disable=protected-access,redefined-outer-name,unused-import

import concurrent.futures
import dataclasses
import threading

import pytest
import sqlalchemy
from sqlalchemy import orm
from test_reserved_fill_allocation_pg import _OWNER
from test_reserved_fill_allocation_pg import _SERVICE
from test_reserved_fill_allocation_pg import _SERVICE_HASH
from test_reserved_fill_atomic_admission_pg import _atomic_spec
from test_reserved_fill_atomic_admission_pg import _atomic_specs
from test_reserved_fill_atomic_admission_pg import _authority
from test_reserved_fill_atomic_admission_pg import (  # noqa: F401
    allocation_engine)
from test_reserved_fill_atomic_admission_pg import atomic_database  # noqa: F401
from test_reserved_fill_atomic_admission_pg import (  # noqa: F401
    observation_engine)
from test_reserved_fill_atomic_admission_pg import pg_server  # noqa: F401

from sky.serve import ordinary_launch_binding
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.server.requests import postgres as request_postgres
from sky.server.requests import reserved_fill_admission
from sky.utils import common_utils
from sky.utils import locks

pytestmark = pytest.mark.xdist_group(name='reserved_fill_atomic_admission_pg')

_LIFECYCLE_EPOCH = 4


def _replica(replica_id: int, *, teardown: bool = False
            ) -> replica_managers.ReplicaInfo:
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'{_SERVICE}-mutation-{replica_id}',
        replica_port='8080',
        is_spot=False,
        location=None,
        version=1,
        resources_override={'accelerators': {
            'A100-80GB': 1
        }})
    if teardown:
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
    return info


def _persist(info: replica_managers.ReplicaInfo,
             *,
             expected_exists: bool = False) -> None:
    assert serve_state.add_or_update_replica(
        _SERVICE,
        info.replica_id,
        info,
        expected_service_hash=_SERVICE_HASH,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_controller_owner=_OWNER,
        expected_replica_exists=expected_exists)


def _launch(info: replica_managers.ReplicaInfo,
            *,
            limit: int,
            require_bound: bool = False,
            ) -> dict[int, replica_managers.ReplicaInfo]:
    return serve_state.reserve_replica_launches_running_if_capacity(
        _SERVICE,
        [(info.replica_id, info.replica_record_id, require_bound)],
        launch_limit=limit,
        expected_service_hash=_SERVICE_HASH,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_controller_owner=_OWNER)


def _down(info: replica_managers.ReplicaInfo,
          *,
          limit: int) -> dict[int, replica_managers.ReplicaInfo]:
    return serve_state.reserve_replica_teardowns_running_if_capacity(
        _SERVICE, [(info.replica_id, info.replica_record_id)],
        termination_limit=limit,
        expected_service_hash=_SERVICE_HASH,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_controller_owner=_OWNER)


def _stored_row(engine: sqlalchemy.engine.Engine,
                replica_id: int) -> tuple[str, replica_managers.ReplicaInfo]:
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.status,
                serve_state_schema.replicas_table.c.replica_state_version,
                serve_state_schema.replicas_table.c.replica_state).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE,
                    serve_state_schema.replicas_table.c.replica_id ==
                    replica_id)).one()
    return row.status, serve_state.decode_replica_state_for_authority(
        row.replica_state_version, row.replica_state)


def test_two_connections_serialize_one_p_slot_and_charge_running_row(
        atomic_database) -> None:
    infos = [_replica(101), _replica(102)]
    for info in infos:
        _persist(info)

    first_reserved = threading.Event()
    release_first = threading.Event()

    def _hold_first_uncommitted() -> None:
        with orm.Session(atomic_database) as session:
            assert serve_state.try_acquire_serve_mutation_admission_in_transaction(
                session)
            assert serve_state.try_acquire_replica_launch_authority_in_transaction(
                session, atomic_database, _SERVICE)
            assert serve_state._prelock_serve_mutation_rows(
                session, atomic_database, _SERVICE, _SERVICE_HASH,
                _LIFECYCLE_EPOCH, _OWNER)
            row = session.execute(
                sqlalchemy.select(
                    serve_state_schema.replicas_table.c.replica_state_version,
                    serve_state_schema.replicas_table.c.replica_state).where(
                        serve_state_schema.replicas_table.c.service_name ==
                        _SERVICE,
                        serve_state_schema.replicas_table.c.replica_id ==
                        infos[0].replica_id).with_for_update()).one()
            current = serve_state.decode_replica_state_for_authority(*row)
            current.status_property.sky_launch_status = (
                common_utils.ProcessStatus.RUNNING)
            assert serve_state._update_exact_locked_replica_in_session(
                session,
                _SERVICE,
                current.replica_id,
                current.replica_record_id,
                current,
                require_no_association=True)
            first_reserved.set()
            assert release_first.wait(timeout=15)
            session.commit()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        holder = executor.submit(_hold_first_uncommitted)
        assert first_reserved.wait(timeout=15)
        try:
            # This is a genuinely overlapping second connection. It sees the
            # transaction-scoped global gate as busy and never reaches the
            # uncommitted row/count state.
            assert _launch(infos[1], limit=1) == {}
        finally:
            release_first.set()
        holder.result(timeout=15)

    # Once the first transaction commits, the second request is rejected by
    # the exact committed P count rather than by stale process-local state.
    assert _launch(infos[1], limit=1) == {}
    scalar_status, persisted = _stored_row(atomic_database,
                                           infos[0].replica_id)
    assert scalar_status == serve_state.ReplicaStatus.PROVISIONING.value
    assert (persisted.status_property.sky_launch_status ==
            common_utils.ProcessStatus.RUNNING)
    assert serve_state.get_replica_mutation_counts() == (1, 0)


def test_killing_gate_transaction_backend_rolls_back_status_and_charge(
        atomic_database) -> None:
    info = _replica(111)
    _persist(info)
    session = orm.Session(atomic_database)
    backend_pid = session.execute(
        sqlalchemy.text('SELECT pg_backend_pid()')).scalar_one()
    assert serve_state.try_acquire_serve_mutation_admission_in_transaction(
        session)
    assert serve_state.try_acquire_replica_launch_authority_in_transaction(
        session, atomic_database, _SERVICE)
    assert serve_state._prelock_serve_mutation_rows(
        session, atomic_database, _SERVICE, _SERVICE_HASH, _LIFECYCLE_EPOCH,
        _OWNER)
    current = serve_state.get_replica_info_from_id(_SERVICE, info.replica_id)
    assert current is not None
    current.status_property.sky_launch_status = (
        common_utils.ProcessStatus.RUNNING)
    assert serve_state._update_exact_locked_replica_in_session(
        session, _SERVICE, info.replica_id, info.replica_record_id, current,
        require_no_association=True)
    with atomic_database.begin() as killer:
        assert killer.execute(
            sqlalchemy.text('SELECT pg_terminate_backend(:pid)'), {
                'pid': backend_pid
            }).scalar_one()
    with pytest.raises(sqlalchemy.exc.DBAPIError):
        session.commit()
    session.close()

    scalar_status, persisted = _stored_row(atomic_database, info.replica_id)
    assert scalar_status == serve_state.ReplicaStatus.PENDING.value
    assert (persisted.status_property.sky_launch_status ==
            common_utils.ProcessStatus.SCHEDULED)
    assert serve_state.get_replica_mutation_counts() == (0, 0)
    assert set(_launch(info, limit=1)) == {info.replica_id}
    assert serve_state.get_replica_mutation_counts() == (1, 0)


def test_p_and_d_budgets_are_independent(atomic_database) -> None:
    launch_one = _replica(121)
    launch_two = _replica(122)
    down_one = _replica(123, teardown=True)
    down_two = _replica(124, teardown=True)
    for info in (launch_one, launch_two, down_one, down_two):
        _persist(info)

    assert set(_launch(launch_one, limit=1)) == {launch_one.replica_id}
    assert set(_down(down_one, limit=1)) == {down_one.replica_id}
    assert _launch(launch_two, limit=1) == {}
    assert _down(down_two, limit=1) == {}
    assert serve_state.get_replica_mutation_counts() == (1, 1)


def test_d_and_p_budgets_are_independent_in_reverse_order(
        atomic_database) -> None:
    down_one = _replica(125, teardown=True)
    down_two = _replica(126, teardown=True)
    launch_one = _replica(127)
    launch_two = _replica(128)
    for info in (down_one, down_two, launch_one, launch_two):
        _persist(info)

    assert set(_down(down_one, limit=1)) == {down_one.replica_id}
    assert set(_launch(launch_one, limit=1)) == {launch_one.replica_id}
    assert _down(down_two, limit=1) == {}
    assert _launch(launch_two, limit=1) == {}
    assert serve_state.get_replica_mutation_counts() == (1, 1)


def test_same_row_cannot_cross_from_p_to_d_or_d_to_p(
        atomic_database) -> None:
    del atomic_database
    launch_row = _replica(129)
    down_row = _replica(130, teardown=True)
    _persist(launch_row)
    _persist(down_row)

    assert set(_launch(launch_row, limit=10)) == {launch_row.replica_id}
    launch_current = serve_state.get_replica_info_from_id(
        _SERVICE, launch_row.replica_id)
    assert launch_current is not None
    launch_current.status_property.sky_down_status = (
        common_utils.ProcessStatus.SCHEDULED)
    _persist(launch_current, expected_exists=True)
    assert _down(launch_current, limit=10) == {}

    assert set(_down(down_row, limit=10)) == {down_row.replica_id}
    down_current = serve_state.get_replica_info_from_id(_SERVICE,
                                                        down_row.replica_id)
    assert down_current is not None
    down_current.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SCHEDULED)
    _persist(down_current, expected_exists=True)
    assert _launch(down_current, limit=10) == {}


def test_shared_provider_guard_allows_unrelated_p_and_d(
        atomic_database) -> None:
    launch = _replica(131)
    down = _replica(132, teardown=True)
    _persist(launch)
    _persist(down)
    lock_id = serve_state._replica_launch_authority_lock_id(
        _SERVICE, atomic_database)
    provider_guard = locks.PostgresLock(lock_id,
                                        shared_lock=True,
                                        engine=atomic_database)
    with provider_guard.acquire(blocking=True):
        assert set(_launch(launch, limit=2)) == {launch.replica_id}
        assert set(_down(down, limit=2)) == {down.replica_id}
    assert serve_state.get_replica_mutation_counts() == (1, 1)


def test_exclusive_service_invalidator_defers_p_and_d(
        atomic_database) -> None:
    launch = _replica(141)
    down = _replica(142, teardown=True)
    _persist(launch)
    _persist(down)
    lock_id = serve_state._replica_launch_authority_lock_id(
        _SERVICE, atomic_database)
    invalidator = locks.PostgresLock(lock_id,
                                     shared_lock=False,
                                     engine=atomic_database)
    with invalidator.acquire(blocking=True):
        assert _launch(launch, limit=2) == {}
        assert _down(down, limit=2) == {}
    assert serve_state.get_replica_mutation_counts() == (0, 0)


def test_atomic_v2_saturation_leaves_second_graph_absent(
        atomic_database) -> None:
    first, second = _atomic_specs(atomic_database, 2)
    first = dataclasses.replace(first, launch_limit=1)
    second = dataclasses.replace(second, launch_limit=1)
    _, receipt = reserved_fill_admission._transaction(
        first, 7, require_existing=False)
    with pytest.raises(reserved_fill_admission._Rejected):
        reserved_fill_admission._transaction(second,
                                              7,
                                              require_existing=False)
    assert receipt.replica_id == first.replica_info.replica_id
    with atomic_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one() == 1
    assert serve_state.get_replica_mutation_counts() == (1, 0)


def test_restore_fresh_launch_requires_null_association(
        atomic_database) -> None:
    fresh = _replica(151)
    _persist(fresh)
    assert set(_launch(fresh, limit=2)) == {fresh.replica_id}
    restored = serve_state.restore_never_started_replica_launch_to_scheduled(
        _SERVICE,
        fresh.replica_id,
        fresh.replica_record_id,
        expected_service_hash=_SERVICE_HASH,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_controller_owner=_OWNER)
    assert restored is not None
    assert (restored.status_property.sky_launch_status ==
            common_utils.ProcessStatus.SCHEDULED)

    spec = dataclasses.replace(_atomic_spec(atomic_database), launch_limit=2)
    reserved_fill_admission._transaction(spec, 7, require_existing=False)
    assert serve_state.restore_never_started_replica_launch_to_scheduled(
        _SERVICE,
        spec.replica_info.replica_id,
        spec.replica_info.replica_record_id,
        expected_service_hash=_SERVICE_HASH,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_controller_owner=_OWNER) is None
    assert serve_state.get_replica_mutation_counts() == (1, 0)


def test_launch_admission_requires_exact_association_shape(
        atomic_database) -> None:
    fresh = _replica(161)
    _persist(fresh)
    assert _launch(fresh, limit=2, require_bound=True) == {}
    assert set(_launch(fresh, limit=2)) == {fresh.replica_id}

    # Materialize one real V2 association, then return only its replica launch
    # receipt to SCHEDULED while preserving the immutable association pointer.
    # A fresh submitter must reject it; only the exact adopter may reserve it.
    spec = dataclasses.replace(_atomic_spec(atomic_database), launch_limit=2)
    reserved_fill_admission._transaction(spec, 7, require_existing=False)
    with orm.Session(atomic_database) as session:
        row = session.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.replica_state_version,
                serve_state_schema.replicas_table.c.replica_state,
                serve_state_schema.replicas_table.c.
                ordinary_launch_association_id).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE,
                    serve_state_schema.replicas_table.c.replica_id ==
                    spec.replica_info.replica_id).with_for_update()).one()
        associated = serve_state.decode_replica_state_for_authority(
            row.replica_state_version, row.replica_state)
        associated.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SCHEDULED)
        assert serve_state._update_exact_locked_replica_in_session(
            session,
            _SERVICE,
            associated.replica_id,
            associated.replica_record_id,
            associated,
            association_id=row.ordinary_launch_association_id)
        session.commit()

    assert _launch(associated, limit=3) == {}
    assert set(_launch(associated, limit=3,
                       require_bound=True)) == {associated.replica_id}


def test_projected_cancelled_bound_launch_consumes_d_slot(
        atomic_database) -> None:
    """A normal exact cancellation must not strand provider cleanup."""
    spec = dataclasses.replace(_atomic_spec(atomic_database), launch_limit=2)
    _, receipt = reserved_fill_admission._transaction(
        spec, 7, require_existing=False)
    with atomic_database.connect() as connection:
        request_row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                receipt.request_id)).mappings().one()
    request = request_postgres.request_from_mapping(request_row)
    context = ordinary_launch_binding.parse_bound_non_pool_launch_context(
        request.request_body.extra_launch_context)

    def _project(connection, projection):
        assert projection.context == context
        assert projection.cancel_reason == 'replica-teardown'
        assert projection.pre_effect_terminal
        info = projection.locked_replica_info
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.INTERRUPTED)
        info.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        return serve_state.update_replica_for_bound_ordinary_launch_in_transaction(
            connection,
            context.service_name,
            _SERVICE_HASH,
            context.replica_id,
            str(context.replica_record_id),
            context.association_id,
            info,
            provider_launch_succeeded=False,
            paid_capacity_pool_key=projection.paid_capacity_pool_key,
            paid_capacity_outcome=None)

    reduction = request_postgres.cancel_bound_ordinary_launch_request(
        context,
        _authority(),
        'replica-teardown',
        project_replica_result=_project)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.
        PRE_EFFECT_TERMINAL)
    assert reduction.projected

    with atomic_database.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                resolution).where(
                    ordinary_launch_binding.
                    ordinary_launch_associations_table.c.association_id ==
                    context.association_id)).scalar_one()
        pointer = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.
                ordinary_launch_association_id).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    _SERVICE,
                    serve_state_schema.replicas_table.c.replica_id ==
                    receipt.replica_id)).scalar_one()
        queue_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                request_postgres.QUEUE).where(
                    request_postgres.QUEUE.c.request_id ==
                    receipt.request_id)).scalar_one()
        pin_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                request_postgres.REQUEST_RETENTION_PINS).where(
                    request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                    receipt.request_id)).scalar_one()
    assert association == (
        ordinary_launch_binding.Resolution.PRE_EFFECT_TERMINAL.value)
    assert pointer is None
    assert queue_count == 0
    assert pin_count == 0

    persisted = serve_state.get_replica_info_from_id(_SERVICE,
                                                      receipt.replica_id)
    assert persisted is not None
    assert set(_down(persisted, limit=1)) == {receipt.replica_id}
    _, running = _stored_row(atomic_database, receipt.replica_id)
    assert running.status_property.sky_down_status == (
        common_utils.ProcessStatus.RUNNING)


def test_pointerless_interrupted_legacy_row_stays_out_of_d(
        atomic_database) -> None:
    del atomic_database
    info = _replica(171, teardown=True)
    info.status_property.sky_launch_status = (
        common_utils.ProcessStatus.INTERRUPTED)
    _persist(info)

    assert _down(info, limit=1) == {}
