"""Real-PostgreSQL tests for the dark resource-action store."""
# pylint: disable=protected-access,redefined-outer-name

import concurrent.futures
import copy
import datetime
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


class _TestProviderProgressContract:
    """Closed miniature provider contract used to exercise API006 fencing."""

    _PHASES = ('CREATE_INTENT', 'OBJECT_CREATED', 'SUCCEEDED')

    def __init__(self) -> None:
        self.transitions = []
        self.snapshot_validations = 0
        self.reject_next_snapshot = False
        self.retry_seed_calls = []

    @staticmethod
    def _attestation(fence: actions.AttemptExecutionFence) -> dict:
        return {
            'request_id': fence.request_id,
            'execution_generation': fence.execution_generation,
            'claim_token': str(fence.claim_token),
            'worker_instance_id': str(fence.worker_instance_id),
            'controller_generation': fence.controller_generation,
        }

    def _parse_progress(self, value):
        if not isinstance(value, dict) or set(value) != {
                'version', 'cursor', 'worker_attestation'
        }:
            raise ValueError('progress envelope is not closed')
        if value['version'] != 1 or isinstance(value['version'], bool):
            raise ValueError('progress version is not 1')
        cursor = value['cursor']
        if not isinstance(cursor, dict) or set(cursor) != {'phase'}:
            raise ValueError('progress cursor is not closed')
        if cursor['phase'] not in self._PHASES:
            raise ValueError('progress phase is unsupported')
        attestation = value['worker_attestation']
        if attestation is not None and (
                not isinstance(attestation, dict) or set(attestation) != {
                    'request_id', 'execution_generation', 'claim_token',
                    'worker_instance_id', 'controller_generation'
                }):
            raise ValueError('worker attestation is not closed')
        if attestation is not None:
            if (not isinstance(attestation['request_id'], str) or
                    not isinstance(attestation['execution_generation'], int) or
                    isinstance(attestation['execution_generation'], bool) or
                    not isinstance(attestation['claim_token'], str) or
                    not isinstance(attestation['worker_instance_id'], str) or
                (attestation['controller_generation'] is not None and
                 (not isinstance(attestation['controller_generation'], int) or
                  isinstance(attestation['controller_generation'], bool)))):
                raise ValueError('worker attestation field types are invalid')
            uuid.UUID(attestation['claim_token'])
            uuid.UUID(attestation['worker_instance_id'])
        return cursor['phase'], attestation

    @staticmethod
    def _parse_outcome(value):
        if not isinstance(value, dict) or set(value) != {
                'version', 'disposition', 'provider_operation_id'
        }:
            raise ValueError('typed outcome is not closed')
        if value['version'] != 1 or isinstance(value['version'], bool):
            raise ValueError('typed outcome version is not 1')
        if value['disposition'] not in {
                'retryable', 'uncertain', 'succeeded', 'terminal_error',
                'cancelled'
        }:
            raise ValueError('typed outcome disposition is unsupported')
        if (value['provider_operation_id'] is not None and
                not isinstance(value['provider_operation_id'], str)):
            raise ValueError('typed outcome operation ID is invalid')

    def _seed_from_predecessor(self, predecessor):
        assert predecessor.typed_outcome is not None
        self._parse_outcome(predecessor.typed_outcome)
        if predecessor.typed_outcome['disposition'] not in {
                'retryable', 'uncertain'
        }:
            raise actions.ActionConflict(
                'typed outcome does not authorize retry')
        if predecessor.provider_progress is None:
            return None
        phase, _ = self._parse_progress(predecessor.provider_progress)
        if phase == 'SUCCEEDED':
            raise actions.ActionConflict('SUCCEEDED progress cannot be retried')
        seed = copy.deepcopy(predecessor.provider_progress)
        seed['worker_attestation'] = None
        return seed

    def retry_seed(self, action, lineage_predecessor, predecessor):
        del action
        self.retry_seed_calls.append(
            (None if lineage_predecessor is None else
             lineage_predecessor.attempt, predecessor.attempt))
        return self._seed_from_predecessor(predecessor)

    def validate_attempt_snapshot(self, action, predecessor, attempt,
                                  execution_fence):
        del action
        self.snapshot_validations += 1
        if self.reject_next_snapshot:
            self.reject_next_snapshot = False
            raise ValueError('synthetic snapshot rejection')
        progress = attempt.provider_progress
        if progress is None:
            if (attempt.provider_io_boundary
                    is not actions.ProviderIOBoundary.NOT_STARTED or
                    attempt.provider_progress_revision != 0 or
                    attempt.provider_progress_sha256 is not None):
                raise ValueError('null progress crossed provider I/O')
        else:
            _, attestation = self._parse_progress(progress)
            if attempt.provider_progress_revision <= 0:
                raise ValueError('nonnull progress has no revision')
            if attempt.provider_io_boundary is (
                    actions.ProviderIOBoundary.NOT_STARTED):
                if (predecessor is None or
                        attempt.provider_progress_revision != 1 or
                        attestation is not None):
                    raise ValueError('invalid inherited retry seed')
                expected = self._seed_from_predecessor(predecessor)
                if actions.canonical_json_bytes(progress) != (
                        actions.canonical_json_bytes(expected)):
                    raise ValueError('inherited retry seed differs')
            elif attestation is None:
                raise ValueError('crossed progress has no worker attestation')
            if (execution_fence is not None and attempt.provider_io_boundary
                    is not actions.ProviderIOBoundary.NOT_STARTED and
                    actions.canonical_json_bytes(attestation)
                    != actions.canonical_json_bytes(
                        self._attestation(execution_fence))):
                raise ValueError('worker attestation differs from claim fence')
        if attempt.mutation_boundary is actions.MutationBoundary.SETTLED:
            self._parse_outcome(attempt.typed_outcome)

    def validate_progress_transition(self, action, predecessor, attempt,
                                     execution_fence, proposed_progress):
        del action, predecessor
        new_phase, new_attestation = self._parse_progress(proposed_progress)
        if (actions.canonical_json_bytes(new_attestation)
                != actions.canonical_json_bytes(
                    self._attestation(execution_fence))):
            raise ValueError('proposed attestation differs from claim fence')
        if attempt.provider_progress is None:
            if new_phase != 'CREATE_INTENT':
                raise ValueError('fresh progress must start at CREATE_INTENT')
            old_phase = None
        else:
            old_phase, old_attestation = self._parse_progress(
                attempt.provider_progress)
            if (attempt.provider_io_boundary
                    is actions.ProviderIOBoundary.NOT_STARTED):
                if (new_phase != old_phase or old_attestation is not None):
                    raise ValueError('inherited seed may only bind attestation')
            elif (self._PHASES.index(new_phase) -
                  self._PHASES.index(old_phase)) not in (0, 1):
                raise ValueError('progress phase is not monotonic')
            elif (actions.canonical_json_bytes(old_attestation)
                  != actions.canonical_json_bytes(new_attestation)):
                raise ValueError('worker attestation changed')
        self.transitions.append((old_phase, new_phase))

    def validate_reduction(self, action, predecessor, attempt, reduction,
                           context):
        self.validate_attempt_snapshot(action, predecessor, attempt, None)
        if (context.terminal_request.request_id != attempt.request_id or
                context.terminal_request.status not in {
                    requests.RequestStatus.SUCCEEDED,
                    requests.RequestStatus.FAILED,
                    requests.RequestStatus.CANCELLED,
                } or not isinstance(context.database_now, datetime.datetime)):
            raise ValueError('reduction context differs from terminal request')
        outcome = dict(reduction.typed_outcome)
        self._parse_outcome(outcome)
        if outcome['provider_operation_id'] != attempt.provider_operation_id:
            raise ValueError('typed outcome conflicts with journaled provider '
                             'operation ID')
        if reduction.kernel_state is actions.KernelState.READY:
            if outcome['disposition'] not in {'retryable', 'uncertain'}:
                raise ValueError('typed outcome does not authorize retry')
            if attempt.provider_progress is not None:
                phase, _ = self._parse_progress(attempt.provider_progress)
                if phase == 'SUCCEEDED':
                    raise ValueError('SUCCEEDED progress cannot reduce READY')


class _LineageProviderProgressContract(_TestProviderProgressContract):
    """Synthetic domain contract with a durable immediate-lineage prefix."""

    _LINEAGE_FIELD = 'lineage_predecessor_sha256'

    @staticmethod
    def lineage_commitment(attempt):
        return actions.canonical_sha256({
            'version': 1,
            'action_id': str(attempt.action_id),
            'attempt': attempt.attempt,
            'request_id': attempt.request_id,
            'request_input_sha256': attempt.request_input_sha256,
            'provider_progress_sha256': attempt.provider_progress_sha256,
            'typed_outcome_sha256': attempt.typed_outcome_sha256,
            'request_terminal_state': attempt.request_terminal_state,
        })

    @staticmethod
    def _parse_outcome(value):
        if not isinstance(value, dict):
            raise ValueError('typed outcome is not closed')
        base_value = dict(value)
        if _LineageProviderProgressContract._LINEAGE_FIELD not in base_value:
            raise ValueError('typed outcome lacks its lineage commitment')
        lineage_sha256 = base_value.pop(
            _LineageProviderProgressContract._LINEAGE_FIELD)
        _TestProviderProgressContract._parse_outcome(base_value)
        if (lineage_sha256 is not None and
            (not isinstance(lineage_sha256, str) or len(lineage_sha256) != 64)):
            raise ValueError('typed outcome lineage commitment is invalid')

    def retry_seed(self, action, lineage_predecessor, predecessor):
        expected = (None if predecessor.attempt == 1 else
                    self.lineage_commitment(lineage_predecessor))
        assert predecessor.typed_outcome is not None
        self._parse_outcome(predecessor.typed_outcome)
        actual = predecessor.typed_outcome[self._LINEAGE_FIELD]
        if actual != expected:
            raise ValueError('retry lineage commitment differs from the exact '
                             'immediate earlier attempt')
        return super().retry_seed(action, lineage_predecessor, predecessor)

    def validate_reduction(self, action, predecessor, attempt, reduction,
                           context):
        super().validate_reduction(action, predecessor, attempt, reduction,
                                   context)
        expected = (None if predecessor is None else
                    self.lineage_commitment(predecessor))
        if reduction.typed_outcome[self._LINEAGE_FIELD] != expected:
            raise ValueError('reduction lineage commitment differs from the '
                             'locked immediate predecessor')


def _typed_progress(item, store, phase='CREATE_INTENT'):
    contract = store._provider_progress_contract
    fence = actions.AttemptExecutionFence(
        request_id=item.request_id,
        execution_generation=item.execution_generation,
        claim_token=uuid.UUID(item.claim_token),
        worker_instance_id=uuid.UUID(store._instance_id),
        controller_generation=None)
    return {
        'version': 1,
        'cursor': {
            'phase': phase
        },
        'worker_attestation': contract._attestation(fence),
    }


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
        postgres_engine,
        provider_progress_contract=_TestProviderProgressContract())
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
        return store.commit_intent_with_progress(item.request_id,
                                                 _typed_progress(item, store),
                                                 0)
    finally:
        storage.deactivate_execution_claim(claim_token)


def _reduce_retry(engine, store, action, request):
    request_input = actions.ActionRequestInput.from_request(
        action.action_id, 1, request)

    def reducer(connection, action_record, attempt_record, context):
        del connection, action_record, attempt_record, context
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
            retry_after_seconds=0,
        )

    with engine.begin() as connection:
        return store.reduce_in_transaction(connection, action.action_id, 1, 2,
                                           request_input, reducer)


def _settle_first_attempt_for_retry(engine, backend, store):
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    materialized = store.materialize(action.action_id, 1, 1, request)
    assert materialized is not None and materialized.created
    item = _claim(backend, request.request_id)
    progress = _typed_progress(item, store)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        store.commit_intent_with_progress(request.request_id, progress, 0)
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.FAILED, 'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)
    reduced = _reduce_retry(engine, store, action, request)
    return action, request, progress, reduced


def _settle_lineage_attempt_for_retry(engine, backend, store, materialized,
                                      request):
    assert materialized.attempt is not None
    attempt_record = materialized.attempt
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        current_progress = attempt_record.provider_progress
        if current_progress is None:
            proposed_progress = _typed_progress(item, store)
            expected_progress_revision = 0
        else:
            proposed_progress = copy.deepcopy(current_progress)
            proposed_progress['worker_attestation'] = _typed_progress(
                item, store)['worker_attestation']
            expected_progress_revision = 1
        store.commit_intent_with_progress(request.request_id, proposed_progress,
                                          expected_progress_revision)
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.FAILED, 'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)

    request_input = actions.ActionRequestInput.from_request(
        materialized.action.action_id, attempt_record.attempt, request)
    contract = store._provider_progress_contract

    def reducer(unused_connection, unused_action, settled_attempt, context):
        del unused_connection, unused_action
        lineage_sha256 = (None if context.predecessor_attempt is None else
                          contract.lineage_commitment(
                              context.predecessor_attempt))
        return actions.ActionReduction(
            kernel_state=actions.KernelState.READY,
            typed_outcome={
                'version': 1,
                'disposition': 'retryable',
                'provider_operation_id': settled_attempt.provider_operation_id,
                'lineage_predecessor_sha256': lineage_sha256,
            },
            result={
                'version': 1,
                'classification': 'transient',
            },
            retry_after_seconds=0)

    with engine.begin() as connection:
        return store.reduce_in_transaction(connection,
                                           materialized.action.action_id,
                                           attempt_record.attempt,
                                           materialized.action.revision,
                                           request_input, reducer)


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


def test_materialization_snapshot_rejection_creates_no_delivery(
        action_database):
    engine, _, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    contract = store._provider_progress_contract
    contract.reject_next_snapshot = True

    blocked = store.materialize(action.action_id, action.revision, 1, request)

    assert blocked is not None and blocked.blocked
    assert blocked.action.kernel_state is actions.KernelState.BLOCKED
    assert blocked.action.last_result is not None
    assert (blocked.action.last_result['code'] ==
            'inserted_attempt_progress_contract')
    with engine.connect() as connection:
        counts = tuple(
            connection.execute(
                sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                                 ).select_from(table)).scalar_one()
            for table in (request_postgres.RESOURCE_ACTION_ATTEMPTS,
                          request_postgres.REQUESTS, request_postgres.QUEUE))
    assert counts == (1, 0, 0)


def test_lost_ack_adopts_after_worker_advances_typed_progress(action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    created = store.materialize(action.action_id, action.revision, 1, request)
    assert created is not None and created.created
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        first = _typed_progress(item, store, 'CREATE_INTENT')
        second = _typed_progress(item, store, 'OBJECT_CREATED')
        store.commit_intent_with_progress(request.request_id, first, 0)
        store.write_provider_progress(request.request_id, second, 1)
        adopted = store.materialize(action.action_id, action.revision, 1,
                                    request)
    finally:
        storage.deactivate_execution_claim(claim_token)
    assert adopted is not None and adopted.adopted
    assert not adopted.blocked
    assert adopted.attempt is not None
    assert adopted.attempt.provider_progress == second
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(
                request_postgres.RESOURCE_ACTION_ATTEMPTS)).scalar_one() == 1


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

    corrupted = dict(persisted)
    decomposed = {'label': 'e\u0301'}
    corrupted['provider_progress'] = decomposed
    corrupted['provider_progress_sha256'] = actions.canonical_sha256(decomposed)
    corrupted['provider_progress_revision'] = 1
    with pytest.raises(actions.InvariantViolation, match='already canonical'):
        resource_actions_postgres._attempt_record(corrupted)


@pytest.mark.parametrize('values', [
    {
        'provider_io_boundary': 'BROKEN'
    },
    {
        'mutation_boundary': 'INTENT_COMMITTED'
    },
    {
        'provider_operation_id': 'operation-before-submission'
    },
    {
        'provider_progress': {
            'version': 1
        },
        'provider_progress_sha256': None,
        'provider_progress_revision': 0,
    },
])
def test_api006_attempt_constraints_reject_invalid_boundary_and_progress_shape(
        action_database, values):
    engine, _, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                        action.action_id,
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                        1).values(**values))


def test_active_provider_io_rejects_bounded_progress_hash_mismatch(
        action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        store.commit_intent_with_progress(request.request_id,
                                          _typed_progress(item, store), 0)
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                        action.action_id,
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                        1).values(provider_progress_sha256='0' * 64))
        with pytest.raises(actions.InvariantViolation,
                           match='Active provider progress hash'):
            store.record_submission(request.request_id, None)
    finally:
        storage.deactivate_execution_claim(claim_token)


def test_terminal_reducer_classifies_bounded_progress_hash_mismatch(
        action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    materialized = store.materialize(action.action_id, 1, 1, request)
    assert materialized is not None and materialized.created
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        store.commit_intent_with_progress(request.request_id,
                                          _typed_progress(item, store), 0)
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.FAILED, 'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)
    declared_hash = '0' * 64
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action.action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                1).values(provider_progress_sha256=declared_hash))
    request_input = actions.ActionRequestInput.from_request(
        action.action_id, 1, request)
    reducer_calls = []

    def reducer(unused_connection, unused_action, attempt_record,
                unused_context):
        del unused_connection, unused_action, unused_context
        reducer_calls.append(attempt_record)
        assert attempt_record.provider_progress is not None
        assert attempt_record.provider_progress_sha256 == declared_hash
        assert actions.canonical_sha256(
            attempt_record.provider_progress) != declared_hash
        return actions.ActionReduction(kernel_state=actions.KernelState.BLOCKED,
                                       typed_outcome={
                                           'version': 1,
                                           'disposition': 'terminal_error',
                                           'provider_operation_id': None,
                                       },
                                       result={
                                           'version': 1,
                                           'classification': 'invalid_journal',
                                       })

    with engine.begin() as connection:
        settled = store.reduce_in_transaction(connection, action.action_id, 1,
                                              materialized.action.revision,
                                              request_input, reducer)
    assert len(reducer_calls) == 1
    assert settled.action.kernel_state is actions.KernelState.BLOCKED
    assert settled.attempt.provider_progress_sha256 == declared_hash

    def replay_reducer(*unused_args):
        del unused_args
        raise AssertionError('settled replay invoked reducer')

    with engine.begin() as connection:
        replayed = store.reduce_in_transaction(connection, action.action_id, 1,
                                               materialized.action.revision,
                                               request_input, replay_reducer)
    assert replayed.replayed
    assert replayed.attempt.provider_progress_sha256 == declared_hash


def test_claim_journal_and_retry_reduction_replay_keep_deadline(
        action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    materialized = store.materialize(action.action_id, 1, 1, request)
    assert materialized is not None and materialized.created
    assert materialized.attempt is not None
    assert materialized.attempt.provider_io_boundary is (
        actions.ProviderIOBoundary.NOT_STARTED)
    assert materialized.attempt.provider_progress is None
    assert materialized.attempt.provider_progress_revision == 0
    assert not hasattr(store, 'commit_intent')
    with pytest.raises(actions.ClaimLost, match='no active'):
        store.commit_intent_with_progress(
            request.request_id, {
                'version': 1,
                'cursor': {
                    'phase': 'CREATE_INTENT'
                },
                'worker_attestation': {
                    'request_id': request.request_id,
                    'execution_generation': 1,
                    'claim_token': str(uuid.uuid4()),
                    'worker_instance_id': str(uuid.uuid4()),
                    'controller_generation': None,
                },
            }, 0)

    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        intent = store.commit_intent_with_progress(request.request_id,
                                                   _typed_progress(item, store),
                                                   0)
        assert intent.mutation_boundary is (
            actions.MutationBoundary.INTENT_COMMITTED)
        assert intent.provider_io_boundary is (
            actions.ProviderIOBoundary.INTENT_COMMITTED)
        ambiguous = store.record_submission(request.request_id, None)
        assert ambiguous.mutation_boundary is (
            actions.MutationBoundary.SUBMITTED_OR_AMBIGUOUS)
        assert ambiguous.provider_io_boundary is (
            actions.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS)
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

    def reducer(connection, action_record, attempt_record, context):
        del connection
        assert context.predecessor_attempt is None
        callbacks.append((action_record.revision, attempt_record.attempt,
                          context.terminal_request.status))
        return actions.ActionReduction(
            kernel_state=actions.KernelState.READY,
            typed_outcome={
                'version': 1,
                'disposition': 'retryable',
                'provider_operation_id': attempt_record.provider_operation_id,
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
    assert reduced.attempt.provider_io_boundary is (
        actions.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS)
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


def test_typed_progress_is_claim_fenced_and_retains_io_boundary(
        action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    materialized = store.materialize(action.action_id, 1, 1, request)
    assert materialized is not None and materialized.created
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    contract = store._provider_progress_contract
    contract.transitions.clear()
    starting_snapshot_validations = contract.snapshot_validations
    first = _typed_progress(item, store, 'CREATE_INTENT')
    second = _typed_progress(item, store, 'OBJECT_CREATED')
    try:
        intent = store.commit_intent_with_progress(request.request_id, first, 0)
        assert intent.provider_progress == first
        assert intent.provider_progress_revision == 1
        assert intent.mutation_boundary is (
            actions.MutationBoundary.INTENT_COMMITTED)
        assert intent.provider_io_boundary is (
            actions.ProviderIOBoundary.INTENT_COMMITTED)
        replayed = store.commit_intent_with_progress(request.request_id, first,
                                                     0)
        assert replayed.provider_progress_revision == 1

        checkpoint = store.write_provider_progress(request.request_id, second,
                                                   1)
        assert checkpoint.provider_progress == second
        assert checkpoint.provider_progress_revision == 2
        checkpoint_replay = store.write_provider_progress(
            request.request_id, second, 1)
        assert checkpoint_replay.provider_progress_revision == 2
        with pytest.raises(actions.StaleRevision, match='revision changed'):
            store.write_provider_progress(
                request.request_id, _typed_progress(item, store, 'SUCCEEDED'),
                1)
        submitted = store.record_submission(request.request_id, None)
        assert submitted.provider_io_boundary is (
            actions.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS)
        assert submitted.provider_progress == second
        assert submitted.provider_progress_revision == 2
    finally:
        storage.deactivate_execution_claim(claim_token)
    assert contract.transitions == [(None, 'CREATE_INTENT'),
                                    ('CREATE_INTENT', 'CREATE_INTENT'),
                                    ('CREATE_INTENT', 'OBJECT_CREATED'),
                                    ('OBJECT_CREATED', 'OBJECT_CREATED'),
                                    ('OBJECT_CREATED', 'SUCCEEDED')]
    assert contract.snapshot_validations >= starting_snapshot_validations + 6


def test_bool_int_alias_cannot_replay_typed_progress(action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        progress = _typed_progress(item, store)
        store.commit_intent_with_progress(request.request_id, progress, 0)
        aliased = copy.deepcopy(progress)
        aliased['worker_attestation']['execution_generation'] = True
        with pytest.raises(actions.ActionConflict,
                           match='transition is invalid'):
            store.commit_intent_with_progress(request.request_id, aliased, 0)
    finally:
        storage.deactivate_execution_claim(claim_token)


def test_retry_materialization_requires_typed_inherited_seed(action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    materialized = store.materialize(action.action_id, 1, 1, request)
    assert materialized is not None and materialized.created
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    predecessor_progress = _typed_progress(item, store)
    try:
        store.commit_intent_with_progress(request.request_id,
                                          predecessor_progress, 0)
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.FAILED, 'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)
    reduced = _reduce_retry(engine, store, action, request)
    assert reduced.attempt.provider_io_boundary is (
        actions.ProviderIOBoundary.INTENT_COMMITTED)

    request_two = _request(action.action_id, 2)
    seed = {
        'version': 1,
        'cursor': {
            'phase': 'CREATE_INTENT'
        },
        'worker_attestation': None,
    }
    with pytest.raises(TypeError, match='unexpected keyword'):
        store.materialize(action.action_id,
                          reduced.action.revision,
                          2,
                          request_two,
                          provider_progress_seed=seed)

    second = store.materialize(action.action_id, reduced.action.revision, 2,
                               request_two)
    assert second is not None and second.created
    assert second.attempt is not None
    assert second.attempt.provider_io_boundary is (
        actions.ProviderIOBoundary.NOT_STARTED)
    assert second.attempt.provider_progress == seed
    assert second.attempt.provider_progress_revision == 1

    item_two = _claim(backend, request_two.request_id)
    claim_token = storage.activate_execution_claim(
        item_two.request_id, item_two.execution_generation,
        item_two.claim_token)
    try:
        assert not hasattr(store, 'commit_intent')
        with pytest.raises(actions.InvariantViolation,
                           match='crossed active intent'):
            store.write_provider_progress(request_two.request_id, seed, 1)
        bound = dict(seed)
        bound['worker_attestation'] = _typed_progress(
            item_two, store)['worker_attestation']
        intent = store.commit_intent_with_progress(request_two.request_id,
                                                   bound, 1)
        assert intent.provider_progress_revision == 2
        assert intent.provider_progress == bound
        assert intent.provider_io_boundary is (
            actions.ProviderIOBoundary.INTENT_COMMITTED)
    finally:
        storage.deactivate_execution_claim(claim_token)


def test_retry_materialization_and_lost_ack_use_locked_lineage_in_order(
        action_database, monkeypatch):
    engine, backend, _ = action_database
    contract = _LineageProviderProgressContract()
    store = resource_actions_postgres.PostgresResourceActionStore(
        engine, provider_progress_contract=contract)
    action = _admit(engine, store, _new_action())

    request_one = _request(action.action_id, 1)
    first = store.materialize(action.action_id, action.revision, 1, request_one)
    assert first is not None and first.created
    reduced_one = _settle_lineage_attempt_for_retry(engine, backend, store,
                                                    first, request_one)

    request_two = _request(action.action_id, 2)
    second = store.materialize(action.action_id, reduced_one.action.revision, 2,
                               request_two)
    assert second is not None and second.created
    second_replay = store.materialize(action.action_id,
                                      reduced_one.action.revision, 2,
                                      request_two)
    assert second_replay is not None and second_replay.adopted
    assert contract.retry_seed_calls == [(None, 1), (None, 1)]
    reduced_two = _settle_lineage_attempt_for_retry(engine, backend, store,
                                                    second, request_two)

    lock_order = []
    original_action = store._locked_action
    original_attempt = store._locked_attempt

    def _record_action(*args, **kwargs):
        lock_order.append(('action', str(args[1])))
        return original_action(*args, **kwargs)

    def _record_attempt(*args, **kwargs):
        lock_order.append(('attempt', args[2]))
        return original_attempt(*args, **kwargs)

    monkeypatch.setattr(store, '_locked_action', _record_action)
    monkeypatch.setattr(store, '_locked_attempt', _record_attempt)
    request_three = _request(action.action_id, 3)
    third = store.materialize(action.action_id, reduced_two.action.revision, 3,
                              request_three)
    assert third is not None and third.created
    assert lock_order[:4] == [('action', str(action.action_id)), ('attempt', 1),
                              ('attempt', 2), ('attempt', 3)]
    assert contract.retry_seed_calls[-1] == (1, 2)

    lock_order.clear()
    third_replay = store.materialize(action.action_id,
                                     reduced_two.action.revision, 3,
                                     request_three)
    assert third_replay is not None and third_replay.adopted
    assert lock_order[:4] == [('action', str(action.action_id)), ('attempt', 1),
                              ('attempt', 2), ('attempt', 3)]
    assert contract.retry_seed_calls[-2:] == [(1, 2), (1, 2)]


@pytest.mark.parametrize('lost_ack', [False, True])
def test_attempt_two_lineage_tampering_cannot_seed_or_adopt_attempt_three(
        action_database, lost_ack):
    engine, backend, _ = action_database
    contract = _LineageProviderProgressContract()
    store = resource_actions_postgres.PostgresResourceActionStore(
        engine, provider_progress_contract=contract)
    action = _admit(engine, store, _new_action())

    request_one = _request(action.action_id, 1)
    first = store.materialize(action.action_id, action.revision, 1, request_one)
    assert first is not None and first.created
    reduced_one = _settle_lineage_attempt_for_retry(engine, backend, store,
                                                    first, request_one)
    request_two = _request(action.action_id, 2)
    second = store.materialize(action.action_id, reduced_one.action.revision, 2,
                               request_two)
    assert second is not None and second.created
    reduced_two = _settle_lineage_attempt_for_retry(engine, backend, store,
                                                    second, request_two)
    request_three = _request(action.action_id, 3)
    if lost_ack:
        third = store.materialize(action.action_id, reduced_two.action.revision,
                                  3, request_three)
        assert third is not None and third.created

    with engine.begin() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action.action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                2).with_for_update()).mappings().one()
        outcome = copy.deepcopy(row['typed_outcome'])
        outcome['lineage_predecessor_sha256'] = '0' * 64
        connection.execute(
            sqlalchemy.update(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action.action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                2).values(
                    typed_outcome=outcome,
                    typed_outcome_sha256=actions.canonical_sha256(outcome)))

    with pytest.raises(actions.ActionConflict,
                       match='lineage commitment differs'):
        store.materialize(action.action_id, reduced_two.action.revision, 3,
                          request_three)
    if not lost_ack:
        with engine.connect() as connection:
            attempt_three = connection.execute(
                sqlalchemy.select(
                    request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt).where(
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                        action.action_id,
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                        3)).scalar_one_or_none()
        assert attempt_three is None


@pytest.mark.parametrize('corruption,match', [
    ('succeeded_cursor', 'SUCCEEDED progress cannot be retried'),
    ('unauthorized_outcome', 'does not authorize retry'),
])
def test_retry_materialization_rejects_invalid_settled_predecessor(
        action_database, corruption, match):
    engine, backend, store = action_database
    action, _, _, reduced = _settle_first_attempt_for_retry(
        engine, backend, store)
    with engine.begin() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action.action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                1).with_for_update()).mappings().one()
        if corruption == 'succeeded_cursor':
            progress = copy.deepcopy(row['provider_progress'])
            progress['cursor']['phase'] = 'SUCCEEDED'
            values = {
                'provider_progress': progress,
                'provider_progress_sha256': actions.canonical_sha256(progress),
            }
        else:
            outcome = copy.deepcopy(row['typed_outcome'])
            outcome['disposition'] = 'succeeded'
            values = {
                'typed_outcome': outcome,
                'typed_outcome_sha256': actions.canonical_sha256(outcome),
            }
        connection.execute(
            sqlalchemy.update(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action.action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                1).values(**values))
    with pytest.raises(actions.ActionConflict, match=match):
        store.materialize(action.action_id, reduced.action.revision, 2,
                          _request(action.action_id, 2))


@pytest.mark.parametrize(
    'corruption',
    ['attested_seed', 'mismatched_cursor', 'wrong_local_revision'])
def test_lost_ack_rejects_corrupt_inherited_retry_seed(action_database,
                                                       corruption):
    engine, backend, store = action_database
    action, _, predecessor_progress, reduced = (_settle_first_attempt_for_retry(
        engine, backend, store))
    request_two = _request(action.action_id, 2)
    created = store.materialize(action.action_id, reduced.action.revision, 2,
                                request_two)
    assert created is not None and created.created
    with engine.begin() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action.action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                2).with_for_update()).mappings().one()
        progress = copy.deepcopy(row['provider_progress'])
        values = {}
        if corruption == 'attested_seed':
            progress['worker_attestation'] = predecessor_progress[
                'worker_attestation']
        elif corruption == 'mismatched_cursor':
            progress['cursor']['phase'] = 'OBJECT_CREATED'
        else:
            values['provider_progress_revision'] = 2
        if corruption != 'wrong_local_revision':
            values.update({
                'provider_progress': progress,
                'provider_progress_sha256': actions.canonical_sha256(progress),
            })
        connection.execute(
            sqlalchemy.update(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action.action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                2).values(**values))
    adopted = store.materialize(action.action_id, reduced.action.revision, 2,
                                request_two)
    assert adopted is not None and adopted.blocked
    assert adopted.action.kernel_state is actions.KernelState.BLOCKED


def test_null_progress_retry_takes_fresh_cursor_branch(action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    materialized = store.materialize(action.action_id, 1, 1, request)
    assert materialized is not None and materialized.created
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.FAILED, 'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)
    reduced = _reduce_retry(engine, store, action, request)
    assert reduced.attempt.provider_io_boundary is (
        actions.ProviderIOBoundary.NOT_STARTED)
    assert reduced.attempt.provider_progress is None

    request_two = _request(action.action_id, 2)
    second = store.materialize(action.action_id, reduced.action.revision, 2,
                               request_two)
    assert second is not None and second.created
    assert second.attempt is not None
    assert second.attempt.provider_progress is None
    assert second.attempt.provider_progress_revision == 0
    item_two = _claim(backend, request_two.request_id)
    claim_token = storage.activate_execution_claim(
        item_two.request_id, item_two.execution_generation,
        item_two.claim_token)
    try:
        first = _typed_progress(item_two, store)
        intent = store.commit_intent_with_progress(request_two.request_id,
                                                   first, 0)
        assert intent.provider_progress == first
        assert intent.provider_progress_revision == 1
        assert intent.provider_io_boundary is (
            actions.ProviderIOBoundary.INTENT_COMMITTED)
    finally:
        storage.deactivate_execution_claim(claim_token)


def test_ready_reduction_rejects_crossed_provider_io_with_null_progress(
        action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    materialized = store.materialize(action.action_id, 1, 1, request)
    assert materialized is not None and materialized.created
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        # Simulate a self-consistent SQL-level corruption that API006's removed
        # null-intent API can no longer create.
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                        action.action_id,
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt == 1
                    ).values(
                        mutation_boundary=(
                            actions.MutationBoundary.INTENT_COMMITTED.value),
                        provider_io_boundary=(
                            actions.ProviderIOBoundary.INTENT_COMMITTED.value)))
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.FAILED, 'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)
    with pytest.raises(actions.ActionConflict,
                       match='exact pre-I/O null cursor'):
        _reduce_retry(engine, store, action, request)


def test_reducer_locks_predecessor_before_current_attempt(
        action_database, monkeypatch):
    engine, backend, store = action_database
    action, _, _, reduced = _settle_first_attempt_for_retry(
        engine, backend, store)
    request_two = _request(action.action_id, 2)
    created = store.materialize(action.action_id, reduced.action.revision, 2,
                                request_two)
    assert created is not None and created.created
    item = _claim(backend, request_two.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        assert backend.transition_request_terminal(
            request_two.request_id, requests.RequestStatus.FAILED,
            'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)

    lock_order = []
    original_action = store._locked_action
    original_attempt = store._locked_attempt

    def _record_action(*args, **kwargs):
        lock_order.append(('action', str(args[1])))
        return original_action(*args, **kwargs)

    def _record_attempt(*args, **kwargs):
        lock_order.append(('attempt', args[2]))
        return original_attempt(*args, **kwargs)

    monkeypatch.setattr(store, '_locked_action', _record_action)
    monkeypatch.setattr(store, '_locked_attempt', _record_attempt)
    request_input = actions.ActionRequestInput.from_request(
        action.action_id, 2, request_two)

    def reducer(unused_connection, unused_action, unused_attempt, context):
        del unused_connection, unused_action, unused_attempt
        assert context.predecessor_attempt is not None
        assert context.predecessor_attempt.attempt == 1
        return actions.ActionReduction(kernel_state=actions.KernelState.READY,
                                       typed_outcome={
                                           'version': 1,
                                           'disposition': 'retryable',
                                           'provider_operation_id': None,
                                       },
                                       result={
                                           'version': 1,
                                           'classification': 'transient',
                                       },
                                       retry_after_seconds=0)

    with engine.begin() as connection:
        store.reduce_in_transaction(connection, action.action_id, 2,
                                    created.action.revision, request_input,
                                    reducer)
    assert lock_order[:3] == [('action', str(action.action_id)), ('attempt', 1),
                              ('attempt', 2)]


@pytest.mark.parametrize('journal_id,handler_id,expected_id,error_match', [
    (None, None, None, None),
    ('operation-1', None, 'operation-1', None),
    ('operation-1', 'operation-1', 'operation-1', None),
    ('operation-1', 'operation-2', None, 'conflicts with journaled'),
    (None, 'operation-1', None, 'conflicts with journaled'),
])
def test_domain_contract_provider_operation_id_matrix(action_database,
                                                      journal_id, handler_id,
                                                      expected_id, error_match):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    materialized = store.materialize(action.action_id, 1, 1, request)
    assert materialized is not None and materialized.created
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        store.commit_intent_with_progress(request.request_id,
                                          _typed_progress(item, store), 0)
        store.record_submission(request.request_id, journal_id)
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.FAILED, 'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)
    request_input = actions.ActionRequestInput.from_request(
        action.action_id, 1, request)

    def reducer(unused_connection, unused_action, attempt_record,
                unused_context):
        del unused_connection, unused_action, unused_context
        normalized_operation_id = (attempt_record.provider_operation_id
                                   if handler_id is None else handler_id)
        return actions.ActionReduction(
            kernel_state=actions.KernelState.TERMINAL,
            typed_outcome={
                'version': 1,
                'disposition': 'terminal_error',
                'provider_operation_id': normalized_operation_id,
            },
            result={
                'version': 1,
                'classification': 'terminal',
            },
            terminal_disposition='terminal_error')

    if error_match is not None:
        with engine.begin() as connection:
            with pytest.raises(actions.ActionConflict, match=error_match):
                store.reduce_in_transaction(connection, action.action_id, 1, 2,
                                            request_input, reducer)
        return
    with engine.begin() as connection:
        settled = store.reduce_in_transaction(connection, action.action_id, 1,
                                              2, request_input, reducer)
    assert settled.attempt.provider_operation_id == expected_id
    assert settled.attempt.typed_outcome is not None
    assert settled.attempt.typed_outcome['provider_operation_id'] == expected_id


def test_generic_store_preserves_domain_nested_operation_id(action_database):

    class _NestedOperationContract(_TestProviderProgressContract):
        """Synthetic contract with a nested operation-ID projection."""

        @staticmethod
        def _parse_outcome(value):
            if not isinstance(value, dict) or set(value) != {
                    'version', 'provider_result'
            }:
                raise ValueError('nested typed outcome is not closed')
            provider_result = value['provider_result']
            if (value['version'] != 1 or
                    not isinstance(provider_result, dict) or
                    set(provider_result) != {'provider_operation_id'} or
                (provider_result['provider_operation_id'] is not None and
                 not isinstance(provider_result['provider_operation_id'], str))
               ):
                raise ValueError('nested provider result is invalid')

        def validate_reduction(self, action, predecessor, attempt, reduction,
                               context):
            self.validate_attempt_snapshot(action, predecessor, attempt, None)
            self._parse_outcome(reduction.typed_outcome)
            if (reduction.typed_outcome['provider_result']
                ['provider_operation_id'] != attempt.provider_operation_id):
                raise ValueError('nested operation ID differs from journal')
            if context.terminal_request.request_id != attempt.request_id:
                raise ValueError('terminal request differs from attempt')

    engine, backend, store = action_database
    store._provider_progress_contract = _NestedOperationContract()
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        store.commit_intent_with_progress(request.request_id,
                                          _typed_progress(item, store), 0)
        store.record_submission(request.request_id, 'nested-operation')
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.FAILED, 'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)
    request_input = actions.ActionRequestInput.from_request(
        action.action_id, 1, request)

    def reducer(unused_connection, unused_action, attempt_record,
                unused_context):
        del unused_connection, unused_action, unused_context
        return actions.ActionReduction(
            kernel_state=actions.KernelState.TERMINAL,
            typed_outcome={
                'version': 1,
                'provider_result': {
                    'provider_operation_id':
                        attempt_record.provider_operation_id,
                },
            },
            result={
                'version': 1,
            },
            terminal_disposition='nested_terminal')

    with engine.begin() as connection:
        settled = store.reduce_in_transaction(connection, action.action_id, 1,
                                              2, request_input, reducer)
    assert settled.attempt.typed_outcome == {
        'version': 1,
        'provider_result': {
            'provider_operation_id': 'nested-operation',
        },
    }


def test_settled_replay_revalidates_closed_typed_outcome(action_database):
    engine, backend, store = action_database
    action = _admit(engine, store, _new_action())
    request = _request(action.action_id)
    store.materialize(action.action_id, 1, 1, request)
    item = _claim(backend, request.request_id)
    claim_token = storage.activate_execution_claim(item.request_id,
                                                   item.execution_generation,
                                                   item.claim_token)
    try:
        store.commit_intent_with_progress(request.request_id,
                                          _typed_progress(item, store), 0)
        assert backend.transition_request_terminal(
            request.request_id, requests.RequestStatus.FAILED, 'handler_failed')
    finally:
        storage.deactivate_execution_claim(claim_token)
    request_input = actions.ActionRequestInput.from_request(
        action.action_id, 1, request)

    def reducer(*unused_args):
        del unused_args
        return actions.ActionReduction(
            kernel_state=actions.KernelState.TERMINAL,
            typed_outcome={
                'version': 1,
                'disposition': 'terminal_error',
                'provider_operation_id': None,
            },
            result={
                'version': 1,
                'classification': 'terminal',
            },
            terminal_disposition='terminal_error')

    with engine.begin() as connection:
        store.reduce_in_transaction(connection, action.action_id, 1, 2,
                                    request_input, reducer)
    malformed = {'version': 1, 'provider_operation_id': None}
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action.action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                1).values(
                    typed_outcome=malformed,
                    typed_outcome_sha256=actions.canonical_sha256(malformed)))
    with engine.begin() as connection:
        with pytest.raises(actions.InvariantViolation,
                           match='snapshot is invalid'):
            store.reduce_in_transaction(
                connection, action.action_id, 1, 2, request_input,
                lambda *args: pytest.fail(f'reducer replayed: {args!r}'))


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
            store.commit_intent_with_progress(request.request_id,
                                              _typed_progress(item, store), 0)
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
