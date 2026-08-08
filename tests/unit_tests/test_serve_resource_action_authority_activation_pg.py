"""Real-PostgreSQL tests for initial Serve038 cohort activation."""
# pylint: disable=protected-access,redefined-outer-name,too-many-locals,unused-import

import concurrent.futures
import dataclasses
import datetime
import threading
import time
import uuid

import pytest
import sqlalchemy
from test_serve_resource_action_authority_state_pg import _cohort
from test_serve_resource_action_authority_state_pg import _database_now
from test_serve_resource_action_authority_state_pg import _install_release
from test_serve_resource_action_authority_state_pg import _timestamp
from test_serve_resource_action_authority_state_pg import _worker
from test_serve_resource_action_schema_038_pg import postgres_engine

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_authority_state as authority_state
from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_action_state_schema as state_schema
from sky.serve import resource_actions
from sky.server.requests import authority_worker_bootstrap as bootstrap_v1
from sky.server.requests import authority_worker_bootstrap_v2 as bootstrap_v2
from sky.server.requests import postgres_schema as request_postgres_schema
from sky.utils.db import migration_utils

_FIRST_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_SECOND_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')


@pytest.fixture(scope='module')
def activation_database(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         migration_utils.API_REQUESTS_VERSION)
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.SERVE_DB_NAME, '038')
    return postgres_engine


@pytest.fixture
def activation_store(activation_database):
    engine = activation_database
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'TRUNCATE TABLE '
            'serve_resource_action_worker_registration_leases, '
            'serve_resource_action_worker_cohorts, '
            'serve_resource_action_authority_release_cohorts, '
            'serve_resource_action_authority_releases, '
            'api_server_instances CASCADE')
    _install_release(engine)
    return engine, authority_state.ServeResourceActionAuthorityStore(engine)


def _register_pair(
    engine: sqlalchemy.engine.Engine,
    store: authority_state.ServeResourceActionAuthorityStore,
    *,
    reverse_insertion: bool = False,
) -> authority_state.WorkerCohortV2Record:
    cohort = _cohort()
    observed_at = _database_now(engine)
    first_id, second_id = ((_SECOND_ID, _FIRST_ID) if reverse_insertion else
                           (_FIRST_ID, _SECOND_ID))
    store.register_initial_member(
        helm_release_name=cohort.manifest.pod_template_binding.release_inputs.
        helm_full_name,
        cohort=cohort,
        worker=_worker(first_id, observed_at),
        operation_id=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1'))
    appended = store.append_registering_member(
        helm_release_name=cohort.manifest.pod_template_binding.release_inputs.
        helm_full_name,
        cohort=cohort,
        worker=_worker(second_id, observed_at),
        expected_cohort_revision=1,
        operation_id=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2'))
    assert appended.cohort.revision == 2
    return appended.cohort


def _insert_api_instances(
    engine: sqlalchemy.engine.Engine,
    *,
    heartbeat_at: datetime.datetime | None = None,
    ready: bool = False,
) -> None:
    heartbeat = heartbeat_at or _database_now(engine)
    with engine.begin() as connection:
        for index, instance_id in enumerate((_FIRST_ID, _SECOND_ID)):
            connection.execute(
                request_postgres_schema.SERVER_INSTANCES.insert().values(
                    instance_id=instance_id,
                    role='authority-worker',
                    pod_name=f'authority-worker-{index}',
                    pod_uid=str(instance_id),
                    pod_ip=f'10.0.0.{index + 1}',
                    version='serve038-test',
                    started_at=heartbeat,
                    heartbeat_at=heartbeat,
                    draining_at=None,
                    ready=ready,
                    health_detail={'phase': 'bootstrap'},
                    supported_handlers=[],
                    supported_payload_versions={}))


def _snapshot(
    observed_at: datetime.datetime,
    *,
    deployment_generation: int = 5,
) -> authority.ProviderAuthorityWorkerDeploymentSnapshotV2:
    cohort = _cohort()
    return authority.ProviderAuthorityWorkerDeploymentSnapshotV2(
        version=2,
        deployment_name=cohort.manifest.deployment_name,
        deployment_uid=cohort.deployment_uid,
        deployment_resource_version='deployment-rv-final',
        deployment_generation=deployment_generation,
        deployment_observed_generation=deployment_generation,
        pod_template_contract_sha256=cohort.manifest.pod_template_contract.
        sha256,
        deployment_strategy='RollingUpdate',
        deployment_max_surge=0,
        deployment_max_unavailable=1,
        deployment_spec_replicas=2,
        deployment_status_replicas=2,
        deployment_updated_replicas=2,
        deployment_ready_replicas=2,
        deployment_available_replicas=2,
        deployment_unavailable_replicas=0,
        observed_at=_timestamp(observed_at))


def _cohort_record(
    engine: sqlalchemy.engine.Engine,) -> authority_state.WorkerCohortV2Record:
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(m4_schema.WORKER_COHORTS_V2).where(
                m4_schema.WORKER_COHORTS_V2.c.cohort_id ==
                _cohort().cohort_id)).mappings().one()
    return authority_state.ServeResourceActionAuthorityStore._cohort_record(row)


def _assert_registering_revision_two(engine: sqlalchemy.engine.Engine) -> None:
    record = _cohort_record(engine)
    assert record.lifecycle_state is (
        resource_actions.WorkerCohortLifecycleState.REGISTERING)
    assert record.revision == record.registration_set.revision == 2
    assert record.registration_set.deployment_snapshot is None


def _replace_lease_registration(
    engine: sqlalchemy.engine.Engine,
    worker_instance_id: uuid.UUID,
    registration: authority.ProviderAuthorityWorkerRegistrationV2,
    *,
    renewed_at: datetime.datetime | None = None,
) -> None:
    values: dict[str, object] = {
        'renewal_registration': registration.canonical_value(),
        'renewal_registration_sha256': registration.sha256,
    }
    if renewed_at is not None:
        values.update(renewed_at=renewed_at,
                      expires_at=renewed_at + datetime.timedelta(seconds=60))
    with engine.begin() as connection:
        updated = connection.execute(
            sqlalchemy.update(m4_schema.WORKER_REGISTRATION_LEASES).where(
                m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                _cohort().cohort_id,
                m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                worker_instance_id).values(**values))
    assert updated.rowcount == 1


def _expire_first_registration_lease(engine: sqlalchemy.engine.Engine) -> None:
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                _cohort().cohort_id,
                m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                _FIRST_ID)).mappings().one()
    lease = authority_state.ServeResourceActionAuthorityStore._lease_record(row)
    renewed_at = _database_now(engine) - datetime.timedelta(seconds=61)
    worker = dataclasses.replace(
        lease.renewal_registration.worker,
        observed_at=_timestamp(renewed_at - datetime.timedelta(seconds=1)))
    registration = authority.ProviderAuthorityWorkerRegistrationV2(
        version=2,
        worker_instance_id=_FIRST_ID,
        worker=worker,
        pod_ready=True,
        registered_at=_timestamp(renewed_at))
    _replace_lease_registration(engine,
                                _FIRST_ID,
                                registration,
                                renewed_at=renewed_at)


def _drift_first_lease_identity(engine: sqlalchemy.engine.Engine) -> None:
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                _cohort().cohort_id,
                m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                _FIRST_ID)).mappings().one()
    lease = authority_state.ServeResourceActionAuthorityStore._lease_record(row)
    registration = dataclasses.replace(lease.renewal_registration,
                                       worker=dataclasses.replace(
                                           lease.renewal_registration.worker,
                                           pod_name='stable-identity-drift'))
    _replace_lease_registration(engine, _FIRST_ID, registration)


@pytest.mark.parametrize('reverse_insertion', (False, True))
def test_activation_locks_canonical_rows_and_installs_current_renewals(
        activation_store, reverse_insertion: bool) -> None:
    engine, store = activation_store
    registering = _register_pair(engine,
                                 store,
                                 reverse_insertion=reverse_insertion)
    _insert_api_instances(engine)
    snapshot = _snapshot(_database_now(engine))
    statements: list[str] = []

    def capture_statement(_connection, _cursor, statement, _parameters,
                          _context, _executemany) -> None:
        if ' FOR UPDATE' in statement.upper():
            statements.append(statement.lower())

    sqlalchemy.event.listen(engine, 'before_cursor_execute', capture_statement)
    try:
        mutation = store.activate_initial_cohort(
            cohort_id=registering.cohort_id,
            expected_cohort_revision=2,
            deployment_snapshot=snapshot)
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                capture_statement)

    assert not mutation.adopted
    assert mutation.cohort.lifecycle_state is (
        resource_actions.WorkerCohortLifecycleState.ACCEPTING)
    assert mutation.cohort.revision == mutation.cohort.registration_set.revision
    assert mutation.cohort.revision == 3
    assert mutation.cohort.state_changed_at >= registering.state_changed_at
    assert mutation.cohort.registration_set.deployment_snapshot.canonical_bytes == (
        snapshot.canonical_bytes)
    assert len(statements) == 3
    assert state_schema.WORKER_COHORTS.name in statements[0]
    assert m4_schema.WORKER_REGISTRATION_LEASES.name in statements[1]
    assert request_postgres_schema.SERVER_INSTANCES.name in statements[2]

    retained = _cohort_record(engine)
    assert retained.registration_set.canonical_bytes == (
        mutation.cohort.registration_set.canonical_bytes)
    with engine.connect() as connection:
        lease_rows = connection.execute(
            sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                registering.cohort_id).order_by(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id)
        ).mappings().all()
        ready_values = connection.execute(
            sqlalchemy.select(
                request_postgres_schema.SERVER_INSTANCES.c.ready).order_by(
                    request_postgres_schema.SERVER_INSTANCES.c.instance_id)
        ).scalars().all()
    renewal_bytes = tuple(
        authority_state.ServeResourceActionAuthorityStore._lease_record(
            row).renewal_registration.canonical_bytes for row in lease_rows)
    assert tuple(
        worker.canonical_bytes
        for worker in retained.registration_set.workers) == renewal_bytes
    assert ready_values == [False, False]


def test_activation_lost_ack_exactly_adopts(activation_store) -> None:
    engine, store = activation_store
    registering = _register_pair(engine, store)
    _insert_api_instances(engine)
    snapshot = _snapshot(_database_now(engine))
    committed = store.activate_initial_cohort(cohort_id=registering.cohort_id,
                                              expected_cohort_revision=2,
                                              deployment_snapshot=snapshot)
    adopted = store.activate_initial_cohort(cohort_id=registering.cohort_id,
                                            expected_cohort_revision=2,
                                            deployment_snapshot=snapshot)
    assert adopted.adopted
    assert adopted.cohort.registration_set.canonical_bytes == (
        committed.cohort.registration_set.canonical_bytes)
    assert _cohort_record(engine).revision == 3


@pytest.mark.parametrize('stale_kind', ('registration', 'api'))
def test_activation_precommit_stale_lease_rolls_back(activation_store,
                                                     stale_kind: str) -> None:
    engine, store = activation_store
    registering = _register_pair(engine, store)
    if stale_kind == 'registration':
        _expire_first_registration_lease(engine)
        _insert_api_instances(engine)
    else:
        _insert_api_instances(engine,
                              heartbeat_at=_database_now(engine) -
                              datetime.timedelta(seconds=21))
    snapshot = _snapshot(_database_now(engine))
    with pytest.raises(authority_state.AuthorityStateConflict, match='lease'):
        store.activate_initial_cohort(cohort_id=registering.cohort_id,
                                      expected_cohort_revision=2,
                                      deployment_snapshot=snapshot)
    _assert_registering_revision_two(engine)


def test_activation_rejects_api_ready_before_acceptance(
        activation_store) -> None:
    engine, store = activation_store
    registering = _register_pair(engine, store)
    _insert_api_instances(engine, ready=True)
    with pytest.raises(authority_state.AuthorityStateConflict,
                       match='bootstrap-only'):
        store.activate_initial_cohort(cohort_id=registering.cohort_id,
                                      expected_cohort_revision=2,
                                      deployment_snapshot=_snapshot(
                                          _database_now(engine)))
    _assert_registering_revision_two(engine)


def test_activation_rejects_fresh_snapshot_that_predates_latest_registration(
        activation_store) -> None:
    engine, store = activation_store
    registering = _register_pair(engine, store)
    _insert_api_instances(engine)
    latest_registration = max(
        authority.timestamp_to_datetime(worker.registered_at,
                                        name='registered_at')
        for worker in registering.registration_set.workers)
    snapshot = _snapshot(latest_registration -
                         datetime.timedelta(microseconds=1))
    with pytest.raises(authority_state.AuthorityStateConflict,
                       match='snapshot|drifted'):
        store.activate_initial_cohort(cohort_id=registering.cohort_id,
                                      expected_cohort_revision=2,
                                      deployment_snapshot=snapshot)
    _assert_registering_revision_two(engine)


@pytest.mark.parametrize('drift_kind', ('worker_identity', 'snapshot_identity'))
def test_activation_identity_or_snapshot_drift_rolls_back(
        activation_store, drift_kind: str) -> None:
    engine, store = activation_store
    registering = _register_pair(engine, store)
    _insert_api_instances(engine)
    if drift_kind == 'worker_identity':
        _drift_first_lease_identity(engine)
        snapshot = _snapshot(_database_now(engine))
    else:
        snapshot = _snapshot(_database_now(engine), deployment_generation=6)
    with pytest.raises(authority_state.AuthorityStateError,
                       match='drift|lineage'):
        store.activate_initial_cohort(cohort_id=registering.cohort_id,
                                      expected_cohort_revision=2,
                                      deployment_snapshot=snapshot)
    _assert_registering_revision_two(engine)


def test_activation_revision_conflict_is_fail_closed(activation_store) -> None:
    engine, store = activation_store
    registering = _register_pair(engine, store)
    crossed_set = dataclasses.replace(registering.registration_set, revision=3)
    with engine.begin() as connection:
        changed = connection.execute(
            sqlalchemy.update(m4_schema.WORKER_COHORTS_V2).where(
                m4_schema.WORKER_COHORTS_V2.c.cohort_id ==
                registering.cohort_id).values(
                    registration_attestations=crossed_set.canonical_value(),
                    registration_attestations_sha256=crossed_set.sha256,
                    revision=3,
                    state_changed_at=sqlalchemy.func.clock_timestamp()))
    assert changed.rowcount == 1
    with pytest.raises(authority_state.AuthorityStateSuperseded,
                       match='state/revision CAS'):
        store.activate_initial_cohort(cohort_id=registering.cohort_id,
                                      expected_cohort_revision=2,
                                      deployment_snapshot=_snapshot(
                                          _database_now(engine)))
    assert _cohort_record(engine).revision == 3


def test_concurrent_activation_advances_revision_once(activation_store) -> None:
    engine, store = activation_store
    registering = _register_pair(engine, store)
    _insert_api_instances(engine)
    snapshot = _snapshot(_database_now(engine))

    def activate() -> authority_state.WorkerCohortActivationMutation:
        return store.activate_initial_cohort(cohort_id=registering.cohort_id,
                                             expected_cohort_revision=2,
                                             deployment_snapshot=snapshot)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        mutations = tuple(executor.map(lambda _index: activate(), range(2)))
    assert sorted(mutation.adopted for mutation in mutations) == [False, True]
    retained = _cohort_record(engine)
    assert retained.revision == retained.registration_set.revision == 3


def test_bootstrap_mutation_lock_wait_has_transaction_local_postgres_timeout(
        activation_store) -> None:
    engine, store = activation_store
    registering = _register_pair(engine, store)
    worker = _worker(_FIRST_ID, _database_now(engine))

    with engine.connect() as blocker:
        transaction = blocker.begin()
        blocker.execute(
            sqlalchemy.select(m4_schema.WORKER_COHORTS_V2).where(
                m4_schema.WORKER_COHORTS_V2.c.cohort_id ==
                registering.cohort_id).with_for_update())
        started = time.monotonic()
        try:
            with pytest.raises(sqlalchemy.exc.OperationalError) as error:
                store.renew_own_lease(
                    cohort_id=registering.cohort_id,
                    worker=worker,
                    expected_generation=1,
                    operation_id=uuid.UUID(
                        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3'))
        finally:
            transaction.rollback()

    elapsed = time.monotonic() - started
    assert getattr(error.value.orig, 'pgcode', None) == '55P03'
    assert elapsed < (
        authority_state._BOOTSTRAP_MUTATION_STATEMENT_TIMEOUT_MILLISECONDS /
        1000 + 1)
    with engine.connect() as connection:
        generation = connection.execute(
            sqlalchemy.select(
                m4_schema.WORKER_REGISTRATION_LEASES.c.generation).where(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                    registering.cohort_id,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                    _FIRST_ID)).scalar_one()
    assert generation == 1


def test_coordinator_stop_crosses_real_postgres_lock_wait_before_return(
        activation_store) -> None:
    engine, _ = activation_store
    registering = _register_pair(
        engine, authority_state.ServeResourceActionAuthorityStore(engine))
    cohort = _cohort()
    mutation_waiting = threading.Event()

    class Observer:

        def observe(self, database_now: datetime.datetime):
            return bootstrap_v2.AuthorityWorkerObservationV2(
                cohort, _worker(_FIRST_ID, database_now),
                _snapshot(database_now))

    worker = _worker(_FIRST_ID, _database_now(engine))
    coordinator = bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_FIRST_ID)),
        Observer(),
        authority_state.ServeResourceActionAuthorityStore(engine),
        reconcile_interval_seconds=60,
        stop_join_timeout_seconds=1)

    def capture_wait(_connection, _cursor, statement, _parameters, _context,
                     _executemany) -> None:
        if (threading.current_thread().name == 'authority-worker-bootstrap-v2'
                and m4_schema.WORKER_COHORTS_V2.name in statement and
                ' FOR UPDATE' in statement.upper()):
            mutation_waiting.set()

    with engine.connect() as blocker:
        transaction = blocker.begin()
        blocker.execute(
            sqlalchemy.select(m4_schema.WORKER_COHORTS_V2).where(
                m4_schema.WORKER_COHORTS_V2.c.cohort_id ==
                registering.cohort_id).with_for_update())
        sqlalchemy.event.listen(engine, 'before_cursor_execute', capture_wait)
        try:
            coordinator.start()
            assert mutation_waiting.wait(timeout=2)
            started = time.monotonic()
            coordinator.stop()
            elapsed = time.monotonic() - started
        finally:
            sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                    capture_wait)
            transaction.rollback()

    assert elapsed < (
        authority_state._BOOTSTRAP_MUTATION_STATEMENT_TIMEOUT_MILLISECONDS /
        1000 + 1)
    assert not coordinator._thread.is_alive()
    assert coordinator.accepted_manifest() is None
    with engine.connect() as connection:
        generation = connection.execute(
            sqlalchemy.select(
                m4_schema.WORKER_REGISTRATION_LEASES.c.generation).where(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                    registering.cohort_id,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                    _FIRST_ID)).scalar_one()
    assert generation == 1


def test_two_real_coordinators_race_to_one_postgres_membership(
        activation_store) -> None:
    engine, _ = activation_store
    cohort = _cohort()
    _insert_api_instances(engine)

    class Observer:

        def __init__(self, worker_id: uuid.UUID) -> None:
            self._worker_id = worker_id

        def observe(self, database_now: datetime.datetime):
            return bootstrap_v2.AuthorityWorkerObservationV2(
                cohort, _worker(self._worker_id, database_now),
                _snapshot(database_now))

    def coordinator(worker_id: uuid.UUID):
        worker = _worker(worker_id, _database_now(engine))
        return bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
            cohort.manifest,
            bootstrap_v1.AuthorityWorkerPodIdentity(worker.pod_name,
                                                    cohort.manifest.namespace,
                                                    str(worker_id)),
            Observer(worker_id),
            authority_state.ServeResourceActionAuthorityStore(engine))

    coordinators = (coordinator(_FIRST_ID), coordinator(_SECOND_ID))

    def drive(instance):
        last = None
        for _ in range(4):
            try:
                last = instance.run_once()
            except bootstrap_v1.BootstrapUnavailable:
                continue
            if last.lifecycle_state is (
                    resource_actions.WorkerCohortLifecycleState.ACCEPTING):
                return last
        return last

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(drive, coordinators))

    assert all(outcome is not None and outcome.lifecycle_state is (
        resource_actions.WorkerCohortLifecycleState.ACCEPTING)
               for outcome in outcomes)
    assert all(instance.accepted_manifest() == cohort.manifest
               for instance in coordinators)
    retained = _cohort_record(engine)
    assert retained.revision == retained.registration_set.revision == 3
    assert tuple(
        worker.worker_instance_id
        for worker in retained.registration_set.workers) == (_FIRST_ID,
                                                             _SECOND_ID)
    with engine.connect() as connection:
        leases = connection.execute(
            sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                cohort.cohort_id)).mappings().all()
    assert len(leases) == 2
    assert all(row['generation'] >= 2 for row in leases)
