"""PostgreSQL serialization tests for stale authority-cohort aborts."""
# pylint: disable=redefined-outer-name,protected-access

import concurrent.futures
import datetime
import os
import shutil
import threading
import time
import typing
import uuid

import pytest
import serve_resource_action_test_fixtures as authority_fixtures
import sqlalchemy
from sqlalchemy import orm
import test_serve_resource_action_serve033_store_pg as store_fixtures

from sky.serve import resource_action_state
from sky.serve import resource_action_state_schema
from sky.serve import resource_actions as actions
from sky.serve import serve_state_schema
from sky.server.requests import postgres as request_postgres
from sky.server.requests import resource_actions as kernel_actions

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
testcontainers_postgres = None
if _POSTGRES_URL is None:
    testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    _POSTGRES_URL is None and shutil.which('docker') is None,
    reason='docker unavailable; skipping worker-cohort PostgreSQL race tests')

_CURRENT_FOR = datetime.timedelta(seconds=2)
_MAX_REGISTRATION_AGE = datetime.timedelta(minutes=5)


@pytest.fixture(scope='module')
def postgres_engine():
    container = None
    admin_engine = None
    temporary_database = None
    if _POSTGRES_URL is None:
        assert testcontainers_postgres is not None
        try:
            container = testcontainers_postgres.PostgresContainer('postgres:16')
            container.start()
        except Exception as e:  # pylint: disable=broad-except
            pytest.skip(f'could not start postgres container: {e}')
        assert container is not None
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_cohort_abort_races_{uuid.uuid4().hex}'
        admin_engine = sqlalchemy.create_engine(_POSTGRES_URL,
                                                isolation_level='AUTOCOMMIT')
        quoted = admin_engine.dialect.identifier_preparer.quote(
            temporary_database)
        try:
            with admin_engine.connect() as connection:
                connection.exec_driver_sql(f'CREATE DATABASE {quoted}')
        except Exception as e:  # pylint: disable=broad-except
            admin_engine.dispose()
            pytest.skip(f'could not create temporary postgres database: {e}')
        postgres_url = sqlalchemy.engine.make_url(_POSTGRES_URL).set(
            database=temporary_database).render_as_string(hide_password=False)
    engine = sqlalchemy.create_engine(postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()
        if temporary_database is not None:
            assert admin_engine is not None
            quoted = admin_engine.dialect.identifier_preparer.quote(
                temporary_database)
            with admin_engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text('SELECT pg_terminate_backend(pid) '
                                    'FROM pg_stat_activity '
                                    'WHERE datname = :database AND '
                                    'pid <> pg_backend_pid()'),
                    {'database': temporary_database})
                connection.exec_driver_sql(f'DROP DATABASE {quoted}')
            admin_engine.dispose()
        elif container is not None:
            container.stop()


@pytest.fixture
def cohort_store(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    serve_state_schema.Base.metadata.create_all(postgres_engine)
    resource_action_state_schema.RESOURCE_ACTION_STATE_METADATA.create_all(
        postgres_engine)
    resource_action_state_schema.RESOURCE_ACTION_AUTHORITY_RELEASE_METADATA.create_all(
        postgres_engine)
    request_postgres._METADATA.create_all(  # pylint: disable=protected-access
        postgres_engine)
    store = resource_action_state.PostgresServeResourceActionStateStore(
        postgres_engine)
    store.preflight_authority_release(
        authority_fixtures.NAMESPACE, authority_fixtures.HELM_FULL_NAME,
        authority_fixtures.HELM_FULL_NAME, authority_fixtures.INSTALLATION_ID,
        True, (actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
            authority_fixtures.authority_manifest_value()),), ())
    return postgres_engine, store


def _near_expiry_registrations(
    engine: sqlalchemy.engine.Engine,
    *pod_uids: str,
) -> tuple[actions.WorkerCohortIdentityV1,
           actions.WorkerCohortRegistrationSetV1, datetime.datetime]:
    evidence_time = (store_fixtures._database_now(engine) -
                     _MAX_REGISTRATION_AGE + _CURRENT_FOR)
    cohort, registrations = store_fixtures._cohort_and_registrations(
        engine, *pod_uids, evidence_time=evidence_time)
    return cohort, registrations, evidence_time + _MAX_REGISTRATION_AGE


def _wait_until_database_after(engine: sqlalchemy.engine.Engine,
                               boundary: datetime.datetime) -> None:
    deadline = time.monotonic() + 10
    while True:
        database_now = store_fixtures._database_now(engine)
        if database_now > boundary:
            return
        if time.monotonic() >= deadline:
            pytest.fail('PostgreSQL clock did not cross the stale boundary')
        remaining = (boundary - database_now).total_seconds()
        threading.Event().wait(timeout=min(max(remaining, 0), 0.05))


def _wait_until_blocked_by(engine: sqlalchemy.engine.Engine, blocked_pid: int,
                           blocker_pid: int) -> None:
    deadline = time.monotonic() + 10
    while True:
        with engine.connect() as connection:
            blocking_pids = connection.execute(
                sqlalchemy.select(sqlalchemy.func.pg_blocking_pids(
                    blocked_pid))).scalar_one()
        if blocker_pid in blocking_pids:
            return
        if time.monotonic() >= deadline:
            pytest.fail(f'backend {blocked_pid} did not block on '
                        f'backend {blocker_pid}')
        threading.Event().wait(timeout=0.01)


def _race_committing_operation_against_abort(
    engine: sqlalchemy.engine.Engine,
    store: resource_action_state.PostgresServeResourceActionStateStore,
    cohort: actions.WorkerCohortIdentityV1,
    expected_revision: int,
    expected_registrations: actions.WorkerCohortRegistrationSetV1,
    operation: typing.Callable[
        [orm.Session], resource_action_state.WorkerCohortTransition |
        resource_action_state.WorkerCohortReferenceTransition],
    *,
    before_abort: typing.Callable[[], None],
) -> resource_action_state.WorkerCohortTransition | resource_action_state.WorkerCohortReferenceTransition:
    operation_holds_lock = threading.Event()
    release_operation = threading.Event()
    abort_connected = threading.Event()
    operation_pid: list[int] = []
    abort_pid: list[int] = []

    def run_operation():
        with orm.Session(engine) as session, session.begin():
            operation_pid.append(
                session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.pg_backend_pid())).scalar_one())
            result = operation(session)
            operation_holds_lock.set()
            if not release_operation.wait(timeout=10):
                raise TimeoutError('timed out while holding the cohort lock')
            return result

    def run_abort():
        with orm.Session(engine) as session, session.begin():
            abort_pid.append(
                session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.pg_backend_pid())).scalar_one())
            abort_connected.set()
            return store.authorize_stale_worker_cohort_removal_in_session(
                session, cohort, expected_revision, expected_registrations)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        operation_future = executor.submit(run_operation)
        assert operation_holds_lock.wait(timeout=10)
        before_abort()
        abort_future = executor.submit(run_abort)
        assert abort_connected.wait(timeout=10)
        try:
            _wait_until_blocked_by(engine, abort_pid[0], operation_pid[0])
            assert not abort_future.done()
        finally:
            release_operation.set()
        result = operation_future.result(timeout=10)
        with pytest.raises(kernel_actions.StaleRevision,
                           match='abort predecessor changed'):
            abort_future.result(timeout=10)
    return result


def test_stale_abort_serializes_after_registration_append(cohort_store) -> None:
    engine, store = cohort_store
    cohort, first, stale_after = _near_expiry_registrations(engine, 'pod-a')
    _, both = store_fixtures._cohort_and_registrations(
        engine,
        'pod-a',
        'pod-b',
        evidence_time=stale_after - _MAX_REGISTRATION_AGE)
    registered = store.register_worker_cohort(cohort, first)

    appended = _race_committing_operation_against_abort(
        engine,
        store,
        cohort,
        registered.record.revision,
        first,
        lambda session: store.append_worker_cohort_registration_in_session(
            session, cohort, registered.record.revision, first, both.
            registrations[1]),
        before_abort=lambda: _wait_until_database_after(engine, stale_after))

    assert appended.record.lifecycle_state is (
        actions.WorkerCohortLifecycleState.REGISTERING)
    assert appended.record.revision == registered.record.revision + 1
    stored = store.get_worker_cohort(cohort.cohort_id)
    assert stored is not None
    assert stored.registration_attestations == both
    assert stored.lifecycle_state is actions.WorkerCohortLifecycleState.REGISTERING


def test_stale_abort_serializes_after_promotion(cohort_store) -> None:
    engine, store = cohort_store
    cohort, first, stale_after = _near_expiry_registrations(engine, 'pod-a')
    _, both = store_fixtures._cohort_and_registrations(
        engine,
        'pod-a',
        'pod-b',
        evidence_time=stale_after - _MAX_REGISTRATION_AGE)
    registered = store.register_worker_cohort(cohort, first)
    appended = store.append_worker_cohort_registration(
        cohort, registered.record.revision, first, both.registrations[1])

    promoted = _race_committing_operation_against_abort(
        engine,
        store,
        cohort,
        appended.record.revision,
        both,
        lambda session: store.promote_worker_cohort_in_session(
            session, cohort.cohort_id, appended.record.revision, both),
        before_abort=lambda: _wait_until_database_after(engine, stale_after))

    assert promoted.record.lifecycle_state is (
        actions.WorkerCohortLifecycleState.ACCEPTING)
    stored = store.get_worker_cohort(cohort.cohort_id)
    assert stored is not None
    assert stored.lifecycle_state is actions.WorkerCohortLifecycleState.ACCEPTING


def test_stale_registering_abort_serializes_after_reference_admission(
        cohort_store) -> None:
    engine, store = cohort_store
    cohort, first, stale_after = _near_expiry_registrations(engine, 'pod-a')
    _, both = store_fixtures._cohort_and_registrations(
        engine,
        'pod-a',
        'pod-b',
        evidence_time=stale_after - _MAX_REGISTRATION_AGE)
    registered = store.register_worker_cohort(cohort, first)
    appended = store.append_worker_cohort_registration(
        cohort, registered.record.revision, first, both.registrations[1])
    store.promote_worker_cohort(cohort.cohort_id, appended.record.revision,
                                both)
    _wait_until_database_after(engine, stale_after)
    reference = store_fixtures._reference(store_fixtures._coverage_identity())

    admitted = _race_committing_operation_against_abort(
        engine,
        store,
        cohort,
        appended.record.revision,
        both,
        lambda session: store.prepare_worker_cohort_reference_in_session(
            session, reference),
        before_abort=lambda: None)

    assert admitted.record.reference_state is (
        actions.WorkerCohortReferenceState.PREPARING)
    stored = store.get_worker_cohort(cohort.cohort_id)
    assert stored is not None
    assert stored.lifecycle_state is actions.WorkerCohortLifecycleState.ACCEPTING
    assert store.get_worker_cohort_reference(reference.decision_id) is not None
