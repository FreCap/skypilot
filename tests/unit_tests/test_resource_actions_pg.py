"""Real-PostgreSQL tests for the dark resource-action store."""
# pylint: disable=protected-access,redefined-outer-name

import concurrent.futures
import os
import shutil
import threading
import time
import uuid

import pytest
import sqlalchemy

from sky import core
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import requests
from sky.server.requests import resource_actions as actions
from sky.server.requests import resource_actions_postgres
from sky.server.requests import storage

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
testcontainers_postgres = None
if _POSTGRES_URL is None:
    testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    _POSTGRES_URL is None and shutil.which('docker') is None,
    reason='docker unavailable; skipping resource-action PostgreSQL tests')


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
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_resource_actions_{uuid.uuid4().hex}'
        admin_engine = sqlalchemy.create_engine(_POSTGRES_URL,
                                                isolation_level='AUTOCOMMIT')
        quoted_database = admin_engine.dialect.identifier_preparer.quote(
            temporary_database)
        try:
            with admin_engine.connect() as connection:
                connection.exec_driver_sql(f'CREATE DATABASE {quoted_database}')
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
            quoted_database = admin_engine.dialect.identifier_preparer.quote(
                temporary_database)
            with admin_engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text(
                        'SELECT pg_terminate_backend(pid) '
                        'FROM pg_stat_activity '
                        'WHERE datname = :database AND pid <> pg_backend_pid()'
                    ), {'database': temporary_database})
                connection.exec_driver_sql(f'DROP DATABASE {quoted_database}')
            admin_engine.dispose()
        elif container is not None:
            container.stop()


@pytest.fixture
def action_database(postgres_engine, monkeypatch):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    request_postgres._initialize_schema(postgres_engine)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine',
                        postgres_engine)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine_async', None)
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'all')
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       str(uuid.uuid4()))
    backend = request_postgres.PostgresRequestBackend()
    store = resource_actions_postgres.PostgresResourceActionStore(
        postgres_engine)
    return postgres_engine, backend, store


def _new_action(*,
                immutable_spec=None,
                replica_id: int = 7) -> actions.NewResourceAction:
    identity = actions.ResourceActionIdentity(
        service_hash='service-hash',
        service_incarnation=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        replica_id=replica_id,
        replica_incarnation=uuid.UUID('22222222-2222-4222-8222-222222222222'),
        desired_generation=3,
        action_kind=actions.ActionKind.LAUNCH,
    )
    if immutable_spec is None:
        immutable_spec = {
            'version': 1,
            'provider': 'kubernetes',
            'locator': {
                'namespace': 'default',
                'name': 'replica-7',
            },
        }
    return actions.NewResourceAction(identity, immutable_spec)


def _request(action_id: uuid.UUID, attempt: int = 1) -> requests.Request:
    body = payloads.StopOrDownBody(
        cluster_name='replica-7',
        env_vars={},
        entrypoint='',
        entrypoint_command='',
        using_remote_api_server=False,
        override_skypilot_config={},
        override_skypilot_config_path=None,
        file_mounts_blob_id=None,
        client_api_version=None,
    )
    return requests.Request(
        request_id=actions.request_id_for_attempt(action_id, attempt),
        name='sky.down',
        entrypoint=core.down,
        request_body=body,
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='user',
        schedule_type=requests.ScheduleType.SHORT,
        cluster_name='replica-7',
        should_enqueue=True,
        producer_version='resource-action-test-v1',
    )


def _admit(engine, store, new_action):
    with engine.begin() as connection:
        return store.admit_in_transaction(connection, new_action)


def _claim(backend, request_id):
    item = request_postgres.PostgresQueueBackend('short').get()
    assert item is not None
    assert item.request_id == request_id
    assert item.claim_token is not None
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token)
    return item


def _commit_intent_with_claim(store, item):
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        return store.commit_intent(item.request_id)
    finally:
        storage.deactivate_execution_claim(claim_token)


def test_admission_due_discovery_and_immutable_conflict(action_database):
    engine, _, store = action_database
    new_action = _new_action()
    admitted = _admit(engine, store, new_action)
    assert admitted.action_id == new_action.action_id
    assert admitted.kernel_state is actions.KernelState.READY
    assert admitted.revision == 1
    assert admitted.current_attempt == 0

    # Idempotent admission may return an action that has progressed, but its
    # identity/spec commitment cannot change.
    readmitted = _admit(engine, store, new_action)
    assert readmitted == admitted
    assert store.list_due() == [
        actions.ActionCandidate(admitted.action_id, 1, 1,
                                admitted.next_attempt_at)
    ]
    with pytest.raises(actions.ActionConflict, match='immutable bytes'):
        _admit(
            engine, store,
            _new_action(immutable_spec={
                'version': 1,
                'provider': 'different',
            }))
    with pytest.raises(ValueError, match='positive integer'):
        store.list_due(True)


def test_materialization_lost_ack_adopts_without_second_delivery(
        action_database):
    engine, _, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    created = store.materialize(action.action_id, action.revision, 1, request)
    assert created is not None and created.created
    assert created.action.kernel_state is actions.KernelState.QUEUED
    assert created.action.revision == 2
    assert not created.adopted

    adopted = store.materialize(action.action_id, action.revision, 1, request)
    assert adopted is not None and adopted.adopted
    assert adopted.action.revision == 2
    with engine.connect() as connection:
        request_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(
                                 request_postgres.REQUESTS)).scalar_one()
        queue_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(
                                 request_postgres.QUEUE)).scalar_one()
        attempt_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(
                request_postgres.RESOURCE_ACTION_ATTEMPTS)).scalar_one()
    assert (request_count, queue_count, attempt_count) == (1, 1, 1)


def test_two_dispatchers_create_one_delivery_and_adopt_same_tuple(
        action_database):
    engine, _, store = action_database
    action = _admit(engine, store, _new_action())
    requests_to_materialize = [
        _request(action.action_id),
        _request(action.action_id),
    ]
    start = threading.Barrier(2)

    def _dispatch(request):
        start.wait(timeout=5)
        return store.materialize(action.action_id, action.revision, 1, request)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(_dispatch, request)
            for request in requests_to_materialize
        ]
        results = [future.result(timeout=5) for future in futures]
    assert sum(result is not None and result.created for result in results) == 1
    for result, request in zip(results, requests_to_materialize):
        if result is None:
            result = store.materialize(action.action_id, action.revision, 1,
                                       request)
        assert result is not None
        assert result.created or result.adopted
    with engine.connect() as connection:
        counts = tuple(
            connection.execute(
                sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                                 ).select_from(table)).scalar_one()
            for table in (request_postgres.RESOURCE_ACTION_ATTEMPTS,
                          request_postgres.REQUESTS, request_postgres.QUEUE))
    assert counts == (1, 1, 1)


def test_lost_ack_input_mismatch_persists_blocked(action_database):
    engine, _, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    created = store.materialize(action.action_id, 1, 1, request)
    assert created is not None and created.created
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id).values(user_id='different-user'))
    blocked = store.materialize(action.action_id, 1, 1, request)
    assert blocked is not None and blocked.blocked
    assert blocked.action.kernel_state is actions.KernelState.BLOCKED
    assert blocked.action.revision == 3
    assert blocked.action.last_result is not None
    assert blocked.action.last_result['code'] == 'adoption_user_id'


def test_attempt_request_id_collision_is_locked_and_blocks(action_database):
    engine, _, store = action_database
    action = _admit(engine, store, _new_action())
    foreign_action = _admit(engine, store, _new_action(replica_id=8))
    request = _request(action.action_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.RESOURCE_ACTION_ATTEMPTS).values(
                action_id=foreign_action.action_id,
                attempt=1,
                request_id=request.request_id,
                request_input_sha256='0' * 64,
                provider_operation_id=None,
                mutation_boundary=(actions.MutationBoundary.NOT_STARTED.value),
                typed_outcome=None,
                typed_outcome_sha256=None,
                request_terminal_state=None,
                admitted_at=sqlalchemy.func.clock_timestamp(),
                updated_at=sqlalchemy.func.clock_timestamp(),
                settled_at=None))
    blocked = store.materialize(action.action_id, 1, 1, request)
    assert blocked is not None and blocked.blocked
    assert blocked.action.kernel_state is actions.KernelState.BLOCKED
    assert blocked.action.last_result is not None
    assert blocked.action.last_result['code'] == 'ready_request_id_conflict'
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(
                                 request_postgres.REQUESTS)).scalar_one() == 0


def test_attempt_decoder_revalidates_deterministic_preimage(action_database):
    engine, _, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    with engine.connect() as connection:
        persisted = dict(
            connection.execute(
                sqlalchemy.select(request_postgres.RESOURCE_ACTION_ATTEMPTS)).
            mappings().one())
    corruptions = {
        'request_id': 'not-the-derived-request-id',
        'request_input_sha256': 'A' * 64,
        'admitted_at': persisted['admitted_at'].replace(tzinfo=None),
    }
    for field, value in corruptions.items():
        corrupted = dict(persisted)
        corrupted[field] = value
        with pytest.raises(actions.InvariantViolation):
            resource_actions_postgres._attempt_record(corrupted)


def test_claim_journal_and_retry_reduction_replay_keep_deadline(
        action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    materialized = store.materialize(action.action_id, 1, 1, request)
    assert materialized is not None and materialized.created
    with pytest.raises(actions.ClaimLost, match='no active'):
        store.commit_intent(request.request_id)

    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        intent = store.commit_intent(request.request_id)
        assert intent.mutation_boundary is (
            actions.MutationBoundary.INTENT_COMMITTED)
        ambiguous = store.record_submission(request.request_id, None)
        assert ambiguous.mutation_boundary is (
            actions.MutationBoundary.SUBMITTED_OR_AMBIGUOUS)
        assert ambiguous.provider_operation_id is None
        submitted = store.record_submission(request.request_id, 'operation-1')
        assert submitted.mutation_boundary is (
            actions.MutationBoundary.SUBMITTED_OR_AMBIGUOUS)
        assert submitted.provider_operation_id == 'operation-1'
        replayed_submission = store.record_submission(request.request_id, None)
        assert replayed_submission.provider_operation_id == 'operation-1'
        with pytest.raises(actions.ActionConflict, match='different provider'):
            store.record_submission(request.request_id, 'operation-2')
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.SUCCEEDED,
            'handler_succeeded')
    finally:
        storage.deactivate_execution_claim(claim_token)

    assert store.list_reducible() == [
        actions.ActionCandidate(action.action_id,
                                revision=2,
                                attempt=1,
                                request_id=request.request_id)
    ]
    request_input = actions.ActionRequestInput.from_request(
        action.action_id, 1, request)
    callbacks = []

    def reducer(connection, action_record, attempt_record, terminal_request):
        del connection
        callbacks.append((action_record.revision, attempt_record.attempt,
                          terminal_request.status))
        return actions.ActionReduction(
            kernel_state=actions.KernelState.READY,
            typed_outcome={
                'version': 1,
                'disposition': 'retryable',
                'provider_operation_id': None,
            },
            result={
                'version': 1,
                'classification': 'transient',
            },
            retry_after_seconds=17,
        )

    with engine.begin() as connection:
        reduced = store.reduce_in_transaction(connection, action.action_id, 1,
                                              2, request_input, reducer)
    assert not reduced.replayed
    assert reduced.action.kernel_state is actions.KernelState.READY
    assert reduced.action.revision == 3
    assert reduced.attempt.mutation_boundary is actions.MutationBoundary.SETTLED
    assert reduced.attempt.request_terminal_state == 'SUCCEEDED'
    assert reduced.attempt.provider_operation_id == 'operation-1'
    assert reduced.attempt.typed_outcome is not None
    assert (
        reduced.attempt.typed_outcome['provider_operation_id'] == 'operation-1')
    assert reduced.action.next_attempt_at is not None
    assert reduced.attempt.settled_at is not None
    assert (reduced.action.next_attempt_at -
            reduced.attempt.settled_at).total_seconds() == 17
    original_deadline = reduced.action.next_attempt_at
    assert len(callbacks) == 1

    def must_not_replay(*unused_args):
        del unused_args
        raise AssertionError('settled reduction replay invoked callback')

    with engine.begin() as connection:
        replayed = store.reduce_in_transaction(connection, action.action_id, 1,
                                               2, request_input,
                                               must_not_replay)
    assert replayed.replayed
    assert replayed.action.next_attempt_at == original_deadline
    assert replayed.action.revision == 3
    assert len(callbacks) == 1


def test_expired_claim_cannot_commit_intent(action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    item = _claim(backend, request.request_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request.request_id).
            values(lease_expires_at=(sqlalchemy.func.clock_timestamp() -
                                     sqlalchemy.text("INTERVAL '1 second'"))))
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        with pytest.raises(actions.ClaimLost, match='live claim'):
            store.commit_intent(request.request_id)
    finally:
        storage.deactivate_execution_claim(claim_token)


def test_claim_expiring_while_request_lock_waits_uses_fresh_clock(
        action_database, monkeypatch):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    item = _claim(backend, request.request_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id).values(lease_expires_at=(
                    sqlalchemy.func.clock_timestamp() +
                    sqlalchemy.text("INTERVAL '500 milliseconds'"))))

    attempt_locked = threading.Event()
    original_locked_attempt = store._locked_attempt

    def _signal_attempt_lock(connection, action_id, attempt):
        row = original_locked_attempt(connection, action_id, attempt)
        attempt_locked.set()
        return row

    monkeypatch.setattr(store, '_locked_attempt', _signal_attempt_lock)
    blocker = engine.connect()
    blocker_transaction = blocker.begin()
    blocker.execute(
        sqlalchemy.select(request_postgres.REQUESTS.c.request_id).where(
            request_postgres.REQUESTS.c.request_id ==
            request.request_id).with_for_update()).one()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_commit_intent_with_claim, store, item)
            assert attempt_locked.wait(timeout=5)
            deadline = time.monotonic() + 5
            while True:
                with engine.connect() as observer:
                    database_now, lease_expires_at = observer.execute(
                        sqlalchemy.select(
                            sqlalchemy.func.clock_timestamp(),
                            request_postgres.REQUESTS.c.lease_expires_at).where(
                                request_postgres.REQUESTS.c.request_id ==
                                request.request_id)).one()
                if database_now >= lease_expires_at:
                    break
                if time.monotonic() >= deadline:
                    pytest.fail('request lease did not expire in database time')
                time.sleep(0.01)
            blocker_transaction.commit()
            with pytest.raises(actions.ClaimLost, match='live claim'):
                future.result(timeout=5)
    finally:
        if blocker_transaction.is_active:
            blocker_transaction.rollback()
        blocker.close()


def test_terminalizer_linearizes_before_waiting_evidence_writer(
        action_database, monkeypatch):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    item = _claim(backend, request.request_id)

    terminal_locked = threading.Event()
    release_terminal = threading.Event()
    attempt_locked = threading.Event()
    original_terminalize = request_postgres._terminalize_locked_request
    original_locked_attempt = store._locked_attempt

    def _pause_terminalizer(*args, **kwargs):
        terminal_locked.set()
        if not release_terminal.wait(timeout=5):
            raise TimeoutError('timed out while holding the request lock')
        return original_terminalize(*args, **kwargs)

    def _signal_attempt_lock(connection, action_id, attempt):
        row = original_locked_attempt(connection, action_id, attempt)
        attempt_locked.set()
        return row

    monkeypatch.setattr(request_postgres, '_terminalize_locked_request',
                        _pause_terminalizer)
    monkeypatch.setattr(store, '_locked_attempt', _signal_attempt_lock)

    def _terminalize():
        return backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.SUCCEEDED,
            'handler_succeeded')

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            terminal_future = executor.submit(_terminalize)
            assert terminal_locked.wait(timeout=5)
            evidence_future = executor.submit(_commit_intent_with_claim, store,
                                              item)
            assert attempt_locked.wait(timeout=5)
            assert not evidence_future.done()
            release_terminal.set()
            assert terminal_future.result(timeout=5)
            with pytest.raises(actions.ClaimLost, match='live claim'):
                evidence_future.result(timeout=5)
    finally:
        release_terminal.set()


def test_intent_writer_linearizes_before_waiting_terminalizer(
        action_database, monkeypatch):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    item = _claim(backend, request.request_id)

    evidence_locked = threading.Event()
    release_evidence = threading.Event()
    terminal_select_started = threading.Event()
    terminal_thread_id: list[int] = []
    original_lock_claimed_attempt = store._lock_claimed_attempt

    def _pause_with_evidence_locks(connection, request_id):
        result = original_lock_claimed_attempt(connection, request_id)
        evidence_locked.set()
        if not release_evidence.wait(timeout=5):
            raise TimeoutError('timed out while holding evidence locks')
        return result

    def _observe_terminal_select(conn, cursor, statement, parameters, context,
                                 executemany):
        del conn, cursor, parameters, context, executemany
        if (terminal_thread_id and
                threading.get_ident() == terminal_thread_id[0] and
                'api_requests' in statement and
                'FOR UPDATE' in statement.upper()):
            terminal_select_started.set()

    def _terminalize():
        terminal_thread_id.append(threading.get_ident())
        return backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.SUCCEEDED,
            'handler_succeeded')

    monkeypatch.setattr(store, '_lock_claimed_attempt',
                        _pause_with_evidence_locks)
    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _observe_terminal_select)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            evidence_future = executor.submit(_commit_intent_with_claim, store,
                                              item)
            assert evidence_locked.wait(timeout=5)
            terminal_future = executor.submit(_terminalize)
            assert terminal_select_started.wait(timeout=5)
            assert not terminal_future.done()
            release_evidence.set()
            intent = evidence_future.result(timeout=5)
            assert intent.mutation_boundary is (
                actions.MutationBoundary.INTENT_COMMITTED)
            assert terminal_future.result(timeout=5)
    finally:
        release_evidence.set()
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _observe_terminal_select)

    with engine.connect() as connection:
        attempt_row = connection.execute(
            sqlalchemy.select(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action.action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                1)).mappings().one()
        request_status = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS.c.status).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id)).scalar_one()
    assert attempt_row['mutation_boundary'] == 'INTENT_COMMITTED'
    assert request_status == requests.RequestStatus.SUCCEEDED.value
