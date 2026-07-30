"""Real-PostgreSQL tests for durable API request delivery."""
# pylint: disable=protected-access,redefined-outer-name

import asyncio
import concurrent.futures
import dataclasses
import datetime
import os
import shutil
import sqlite3
import stat
import threading
import time
from unittest import mock
import uuid

import pytest
import sqlalchemy
from sqlalchemy.ext import asyncio as sqlalchemy_async

from sky import core
from sky import global_user_state
from sky.events import api_models as event_api_models
from sky.jobs.server import core as managed_jobs_core
from sky.serve.server import core as serve_core
from sky.server import daemons
from sky.server.events import cursors as event_cursors
from sky.server.events import emission as event_emission
from sky.server.events import schema as event_schema
from sky.server.events import store as event_store
from sky.server.requests import cutover
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import preconditions
from sky.server.requests import registry
from sky.server.requests import requests
from sky.server.requests import storage
from sky.server.requests.queues import base as queue_base
from sky.skylet import constants
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    shutil.which('docker') is None,
    reason='docker unavailable; skipping durable request PostgreSQL tests')


@pytest.fixture(scope='module')
def postgres_engine():
    container = testcontainers_postgres.PostgresContainer('postgres:16')
    try:
        container.start()
    except Exception as e:  # pylint: disable=broad-except
        pytest.skip(f'could not start postgres container: {e}')
    engine = sqlalchemy.create_engine(container.get_connection_url())
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


@pytest.fixture
def request_database(postgres_engine, monkeypatch):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    request_postgres._initialize_schema(postgres_engine)
    async_url = postgres_engine.url.render_as_string(
        hide_password=False).replace('postgresql+psycopg2',
                                     'postgresql+asyncpg')
    async_engine = sqlalchemy_async.create_async_engine(
        async_url, poolclass=sqlalchemy.NullPool)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine',
                        postgres_engine)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine_async',
                        async_engine)
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       str(uuid.uuid4()))
    backend = request_postgres.PostgresRequestBackend()
    yield postgres_engine, backend
    asyncio.run(async_engine.dispose())


def _request(request_id: str,
             *,
             should_enqueue: bool = True,
             schedule_type: requests.ScheduleType = requests.ScheduleType.SHORT,
             entrypoint=core.enabled_clouds) -> requests.Request:
    return requests.Request(
        request_id=request_id,
        name='sky.enabled_clouds',
        entrypoint=entrypoint,
        request_body=payloads.EnabledCloudsBody(workspace=None, expand=False),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='user',
        schedule_type=schedule_type,
        should_enqueue=should_enqueue,
    )


def _controller_request(
    request_id: str,
    *,
    replayable: bool = False,
) -> requests.Request:
    if replayable:
        daemon = daemons.INTERNAL_REQUEST_DAEMONS[0]
        request = requests.build_internal_daemon_request(daemon)
        request.request_id = request_id
        return request
    return requests.Request(
        request_id=request_id,
        name='sky.jobs.launch',
        entrypoint=managed_jobs_core.launch,
        request_body=payloads.JobsLaunchBody(task='run: echo controller',
                                             name='controller-test'),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='user',
        cluster_name='managed-job:test',
        schedule_type=requests.ScheduleType.SHORT,
        should_enqueue=True,
    )


def _event_request(
    request_id: str,
    *,
    workspace: str = 'default',
    actor_id: str = 'user',
    actor_name: str = 'alice@example.com',
    actor_type: str = 'sso',
    cluster_name: str = 'trainer',
    kind: event_api_models.EventKind = (
        event_api_models.EventKind.CLUSTER_LAUNCH),
    should_enqueue: bool = True,
) -> requests.Request:
    request = _request(request_id, should_enqueue=should_enqueue)
    request.name = f'sky.{kind.value.split(".", 1)[1]}'
    request.user_id = actor_id
    request.cluster_name = cluster_name
    request.event_context = {
        'version': 1,
        'kind': kind.value,
        'actor_name': actor_name,
        'actor_type': actor_type,
        'workspace': workspace,
        'targets': [{
            'type': 'cluster',
            'id': f'hash-{cluster_name}',
            'name': cluster_name,
        }],
    }
    return request


def _controller_leader(
    engine: sqlalchemy.engine.Engine,
    monkeypatch,
    instance_id: str,
) -> request_postgres.ControllerLeaderLease:
    monkeypatch.setattr(global_user_state, 'initialize_and_get_db',
                        lambda: engine)
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'controller')
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR, instance_id)
    leader = request_postgres.ControllerLeaderLease(instance_id)
    assert leader.try_acquire()
    return leader


def _write_legacy_database(path, legacy_requests):
    connection = sqlite3.connect(path)
    try:
        cursor = connection.cursor()
        requests.create_table(cursor, connection)
        columns = ', '.join(requests.REQUEST_COLUMNS)
        placeholders = ', '.join('?' for _ in requests.REQUEST_COLUMNS)
        cursor.executemany(
            f'INSERT INTO {requests.REQUEST_TABLE} '
            f'({columns}) VALUES ({placeholders})',
            [request.to_row() for request in legacy_requests])
        connection.commit()
    finally:
        connection.close()


def _claim(backend: request_postgres.PostgresRequestBackend,
           request_id: str) -> queue_base.QueueItem:
    queue = request_postgres.PostgresQueueBackend('short')
    item = queue.get()
    assert item is not None
    assert item.request_id == request_id
    assert item.claim_token is not None
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token)
    return item


def test_schema_bootstrap_is_postgres_only_and_versioned(request_database):
    engine, _ = request_database
    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.API_REQUESTS_DB_NAME) == '004'
    inspector = sqlalchemy.inspect(engine)
    assert {
        'api_requests', 'api_request_queue', 'api_server_instances',
        'api_request_store_metadata', 'api_controller_leadership',
        'api_controller_action_reservations', 'resource_events',
        'resource_event_targets'
    }.issubset(inspector.get_table_names())
    request_columns = {
        column['name'] for column in inspector.get_columns('api_requests')
    }
    assert 'event_context' in request_columns
    assert {
        'ix_resource_events_workspace_sequence',
        'ix_resource_events_workspace_actor_sequence',
        'ix_resource_events_request',
        'ix_resource_events_retention',
    }.issubset(
        {index['name'] for index in inspector.get_indexes('resource_events')})
    with engine.connect() as connection:
        authority = connection.execute(
            sqlalchemy.select(
                event_schema.REQUEST_STORE_METADATA.c.value).where(
                    event_schema.REQUEST_STORE_METADATA.c.key ==
                    event_schema.CURSOR_AUTHORITY_METADATA_KEY)).scalar_one()
    assert uuid.UUID(authority['authority_id'])
    assert authority['event_sequence'] == 0
    leadership_columns = {
        column['name']
        for column in inspector.get_columns('api_controller_leadership')
    }
    assert {'lock_backend_pid',
            'generation_lock_key'}.issubset(leadership_columns)
    sqlite_engine = sqlalchemy.create_engine('sqlite://')
    with pytest.raises(RuntimeError, match='requires PostgreSQL'):
        request_postgres._initialize_schema(sqlite_engine)


def test_server_instance_lease_publishes_ready_and_draining(
        request_database, monkeypatch, tmp_path):
    engine, _ = request_database
    instance_id = str(uuid.uuid4())
    drain_marker = tmp_path / 'draining'
    monkeypatch.setattr(request_postgres, 'ROLE_DRAIN_MARKER_PATH',
                        str(drain_marker))
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR, instance_id)
    monkeypatch.setenv('HOSTNAME', 'executor-pod')
    monkeypatch.setenv('SKYPILOT_POD_UID', 'pod-uid')
    monkeypatch.setenv('POD_IP', '10.0.0.1')
    lease = request_postgres.ServerInstanceLease('executor',
                                                 heartbeat_interval_seconds=60)
    lease.start()
    lease.set_ready(True, health_detail={'phase': 'claiming'})
    assert lease.is_locally_ready()
    assert request_postgres.current_instance_is_ready()
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id == uuid.UUID(
                    instance_id))).mappings().one()
    assert row['role'] == 'executor'
    assert row['pod_name'] == 'executor-pod'
    assert row['pod_uid'] == 'pod-uid'
    assert row['ready']
    assert row['draining_at'] is None
    assert row['health_detail'] == {'phase': 'claiming'}
    assert row['supported_handlers']
    drain_marker.touch()
    assert not lease.is_locally_ready()
    assert not request_postgres.current_instance_is_ready()
    assert lease._heartbeat()
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id == uuid.UUID(
                    instance_id))).mappings().one()
    assert not row['ready']
    assert row['draining_at'] is not None
    assert row['health_detail'] == {'phase': 'draining'}
    lease.stop()
    assert not lease.is_locally_ready()
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id == uuid.UUID(
                    instance_id))).mappings().one()
    assert not row['ready']
    assert row['draining_at'] is not None


def test_controller_cutover_waits_for_recent_m2_executor_heartbeat(
        request_database):
    engine, _ = request_database
    instance_id = uuid.uuid4()
    legacy_handler = registry.registration_for_handler(
        managed_jobs_core.launch).name
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                instance_id=instance_id,
                role='executor',
                pod_name='old-executor',
                pod_uid='old-executor',
                pod_ip='10.0.0.2',
                version='m2',
                started_at=sqlalchemy.func.clock_timestamp(),
                heartbeat_at=sqlalchemy.func.clock_timestamp(),
                draining_at=sqlalchemy.func.clock_timestamp(),
                ready=False,
                health_detail={'phase': 'draining'},
                supported_handlers=[legacy_handler],
                supported_payload_versions={}))
    assert request_postgres.recent_legacy_controller_consumers(70) == [
        str(instance_id)
    ]

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id == instance_id).
            values(heartbeat_at=sqlalchemy.func.clock_timestamp() -
                   datetime.timedelta(seconds=71)))
    assert not request_postgres.recent_legacy_controller_consumers(70)


def test_request_control_pool_survives_saturated_ordinary_pool(
        postgres_engine, monkeypatch):
    """Ordinary DB work cannot starve request and role heartbeats."""
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')

    connection_url = postgres_engine.url.render_as_string(hide_password=False)
    monkeypatch.setenv(constants.ENV_VAR_IS_SKYPILOT_SERVER, 'true')
    monkeypatch.setenv(constants.ENV_VAR_DB_CONNECTION_URI, connection_url)
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       str(uuid.uuid4()))
    isolated_cache = {}
    isolated_lock_cache = {}
    monkeypatch.setattr(db_utils, '_postgres_engine_cache', isolated_cache)
    monkeypatch.setattr(db_utils, '_postgres_lock_engine_cache',
                        isolated_lock_cache)
    monkeypatch.setattr(db_utils, '_max_connections', 1)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine', None)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine_async', None)

    ordinary_engine = db_utils.get_engine('state')
    control_engine = request_postgres.initialize_and_get_db()
    assert ordinary_engine is not control_engine
    assert ordinary_engine.pool.size() == 1
    assert control_engine.pool.size() == 1

    request = _request('isolated-control-heartbeat')
    with control_engine.begin() as connection:
        connection.execute(request_postgres.REQUESTS.insert().values(
            **request_postgres._request_values_for_db(request)))
        connection.execute(request_postgres.QUEUE.insert().values(
            **request_postgres._queue_values(request)))
    backend = request_postgres.PostgresRequestBackend()
    item = _claim(backend, request.request_id)
    assert item.claim_token is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token)
    lease = request_postgres.ServerInstanceLease('executor',
                                                 heartbeat_interval_seconds=60)
    lease.start()
    lease.set_ready(True, health_detail={'phase': 'claiming'})

    ordinary_checkout = ordinary_engine.connect()
    try:
        assert ordinary_engine.pool.checkedout() == 1
        assert lease._heartbeat()
        assert backend.heartbeat_claim(claim)
        assert lease.is_locally_ready()
    finally:
        ordinary_checkout.close()
        lease.stop()
        for engine in isolated_cache.values():
            engine.dispose()
        for engine in isolated_lock_cache.values():
            engine.dispose()


def test_distributed_singleton_promotes_one_standby(request_database):
    del request_database

    async def exercise() -> None:
        started: asyncio.Queue[str] = asyncio.Queue()
        release = {
            'first': asyncio.Event(),
            'second': asyncio.Event(),
        }

        def factory(name: str):

            async def owned() -> None:
                await started.put(name)
                await release[name].wait()

            return owned

        first = asyncio.create_task(
            request_postgres.run_distributed_singleton(
                'test-singleton-promotion',
                factory('first'),
                retry_interval_seconds=0.05,
                connection_check_interval_seconds=0.05))
        second = asyncio.create_task(
            request_postgres.run_distributed_singleton(
                'test-singleton-promotion',
                factory('second'),
                retry_interval_seconds=0.05,
                connection_check_interval_seconds=0.05))
        winner = await asyncio.wait_for(started.get(), timeout=5)
        await asyncio.sleep(0.15)
        assert started.empty()
        winner_task = first if winner == 'first' else second
        standby_task = second if winner == 'first' else first
        winner_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await winner_task
        assert await asyncio.wait_for(started.get(), timeout=5) != winner
        standby_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await standby_task

    asyncio.run(exercise())


def test_controller_leadership_uses_same_session_and_monotonic_generation(
        request_database, monkeypatch):
    engine, _ = request_database
    first_id = str(uuid.uuid4())
    second_id = str(uuid.uuid4())
    first = _controller_leader(engine, monkeypatch, first_id)
    second = request_postgres.ControllerLeaderLease(second_id)
    try:
        assert first.generation == 1
        assert not second.try_acquire()
        lock_backend_pid = first.backend_pid()
        assert lock_backend_pid is not None
        with engine.begin() as connection:
            assert connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'), {
                    'pid': lock_backend_pid
                }).scalar_one()
        assert not first.heartbeat()
        assert not request_postgres.controller_leadership_is_current(
            first_id, 1)

        deadline = time.monotonic() + 5
        while not second.try_acquire() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert second.generation == 2
        assert request_postgres.controller_leadership_is_current(second_id, 2)
        assert not request_postgres.controller_leadership_is_current(
            first_id, 1)
    finally:
        first.release()
        second.release()


def test_stale_controller_cannot_refresh_daemons_or_fence_new_generation(
        request_database, monkeypatch):
    engine, backend = request_database
    first_id = str(uuid.uuid4())
    second_id = str(uuid.uuid4())
    first = _controller_leader(engine, monkeypatch, first_id)
    second = request_postgres.ControllerLeaderLease(second_id)
    request = requests.build_internal_daemon_request(
        daemons.INTERNAL_REQUEST_DAEMONS[0])
    try:
        monkeypatch.setenv(request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR,
                           first_id)
        monkeypatch.setenv(request_postgres.CONTROLLER_GENERATION_ENV_VAR,
                           str(first.generation))
        assert asyncio.run(
            backend.create_or_refresh_internal_daemon_async(request))

        lock_backend_pid = first.backend_pid()
        assert lock_backend_pid is not None
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'),
                {'pid': lock_backend_pid})
        assert second.try_acquire()

        with pytest.raises(RuntimeError, match='leadership changed'):
            asyncio.run(
                backend.create_or_refresh_internal_daemon_async(request))
        with pytest.raises(RuntimeError, match='leadership changed'):
            request_postgres.fence_stale_controller_claims(
                first_id, first.generation)

        monkeypatch.setenv(request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR,
                           second_id)
        monkeypatch.setenv(request_postgres.CONTROLLER_GENERATION_ENV_VAR,
                           str(second.generation))
        assert not asyncio.run(
            backend.create_or_refresh_internal_daemon_async(request))
    finally:
        first.release()
        second.release()


def test_role_scoped_queues_isolate_normal_and_controller_claims(
        request_database, monkeypatch):
    engine, fixture_backend = request_database
    instance_id = str(uuid.uuid4())
    leader = _controller_leader(engine, monkeypatch, instance_id)
    backend = request_postgres.PostgresRequestBackend()
    try:
        assert asyncio.run(
            fixture_backend.create_if_not_exists_async(
                _request('normal-class')))
        assert asyncio.run(
            fixture_backend.create_if_not_exists_async(
                _controller_request('controller-class')))

        normal_queue = request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset({registry.ExecutionClass.NORMAL.value}))
        normal_item = normal_queue.get()
        assert normal_item is not None
        assert normal_item.request_id == 'normal-class'
        assert normal_queue.get() is None

        controller_queue = request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=leader.generation)
        controller_item = controller_queue.get()
        assert controller_item is not None
        assert controller_item.request_id == 'controller-class'
        assert backend.try_mark_running(controller_item.request_id, 1234,
                                        controller_item.execution_generation,
                                        controller_item.claim_token)

        restored = backend.get_request('controller-class')
        assert restored.controller_generation == leader.generation
        assert restored.worker_instance_id == instance_id
        with engine.connect() as connection:
            reservation = connection.execute(
                sqlalchemy.select(
                    request_postgres.CONTROLLER_ACTION_RESERVATIONS).where(
                        request_postgres.CONTROLLER_ACTION_RESERVATIONS.c.
                        logical_action_id ==
                        'controller-class')).mappings().one()
        assert reservation['state'] == 'running'
        assert reservation['controller_generation'] == leader.generation
        assert str(reservation['controller_instance_id']) == instance_id
    finally:
        leader.release()


def test_cancelling_running_controller_action_marks_outcome_ambiguous(
        request_database, monkeypatch):
    engine, fixture_backend = request_database
    instance_id = str(uuid.uuid4())
    leader = _controller_leader(engine, monkeypatch, instance_id)
    backend = request_postgres.PostgresRequestBackend()
    try:
        request = _controller_request('cancel-controller-action')
        assert asyncio.run(fixture_backend.create_if_not_exists_async(request))
        queue = request_postgres.PostgresQueueBackend(
            request.schedule_type.value,
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=leader.generation)
        item = queue.get()
        assert item is not None
        assert backend.try_mark_running(item.request_id, 1234,
                                        item.execution_generation,
                                        item.claim_token)
        kill = mock.Mock()
        monkeypatch.setattr(request_postgres.os, 'kill', kill)

        assert backend.kill_requests([item.request_id]) == [item.request_id]

        kill.assert_called_once_with(1234, request_postgres.signal.SIGTERM)
        with engine.connect() as connection:
            reservation = connection.execute(
                sqlalchemy.select(
                    request_postgres.CONTROLLER_ACTION_RESERVATIONS).where(
                        request_postgres.CONTROLLER_ACTION_RESERVATIONS.c.
                        logical_action_id == item.request_id)).mappings().one()
        assert reservation['state'] == 'ambiguous'
        assert reservation['reconciliation_at'] is not None
    finally:
        leader.release()


def test_role_filter_is_rechecked_after_precondition_evaluation(
        request_database, monkeypatch):
    engine, backend = request_database
    request = _request('role-change-during-precondition')
    request.precondition_type = 'cluster-start-complete.v1'
    request.precondition_payload = {
        'cluster_name': 'cluster',
        'request_id': 'launch',
        'check_interval': 0,
    }
    request.precondition_deadline = time.time() + 10
    assert asyncio.run(backend.create_if_not_exists_async(request))

    def change_execution_class(*args, **kwargs):
        del args, kwargs
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id == request.request_id
                ).values(
                    execution_class=registry.ExecutionClass.CONTROLLER.value))
        return True, None

    monkeypatch.setattr(preconditions, 'check_once', change_execution_class)
    normal_queue = request_postgres.PostgresQueueBackend(
        'short',
        execution_classes=frozenset({registry.ExecutionClass.NORMAL.value}))
    assert normal_queue.get() is None
    with engine.connect() as connection:
        execution_class, delivery_state = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS.c.execution_class,
                              request_postgres.QUEUE.c.delivery_state).join(
                                  request_postgres.QUEUE,
                                  request_postgres.QUEUE.c.request_id ==
                                  request_postgres.REQUESTS.c.request_id).where(
                                      request_postgres.REQUESTS.c.request_id ==
                                      request.request_id)).one()
    assert execution_class == registry.ExecutionClass.CONTROLLER.value
    assert delivery_state == 'queued'


def test_controller_handoff_interrupts_ambiguous_mutation_and_fences_write(
        request_database, monkeypatch):
    engine, fixture_backend = request_database
    first_id = str(uuid.uuid4())
    second_id = str(uuid.uuid4())
    first = _controller_leader(engine, monkeypatch, first_id)
    first_backend = request_postgres.PostgresRequestBackend()
    second = request_postgres.ControllerLeaderLease(second_id)
    try:
        assert asyncio.run(
            fixture_backend.create_if_not_exists_async(
                _controller_request('ambiguous-controller-action')))
        queue = request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=first.generation)
        item = queue.get()
        assert item is not None
        assert first_backend.try_mark_running(item.request_id, 1234,
                                              item.execution_generation,
                                              item.claim_token)
        stale_context = storage.activate_execution_claim(
            item.request_id, item.execution_generation, item.claim_token)

        lock_backend_pid = first.backend_pid()
        assert lock_backend_pid is not None
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'),
                {'pid': lock_backend_pid})
        assert not first.heartbeat()
        assert not first_backend.set_request_finished(
            item.request_id, requests.RequestStatus.SUCCEEDED, result=[])
        assert second.try_acquire()
        assert second.generation == 2
        fenced = request_postgres.fence_stale_controller_claims(
            second_id, second.generation)
        assert fenced == {'replayed': 0, 'interrupted': 1}
        try:
            assert not first_backend.set_request_finished(
                item.request_id, requests.RequestStatus.SUCCEEDED, result=[])
        finally:
            storage.deactivate_execution_claim(stale_context)

        restored = first_backend.get_request(item.request_id)
        assert restored.status is requests.RequestStatus.CANCELLED
        assert restored.should_retry
        assert 'ambiguous mutating outcome' in restored.interrupted_reason
        with engine.connect() as connection:
            reservation_state = connection.execute(
                sqlalchemy.select(
                    request_postgres.CONTROLLER_ACTION_RESERVATIONS.c.state).
                where(request_postgres.CONTROLLER_ACTION_RESERVATIONS.c.
                      logical_action_id == item.request_id)).scalar_one()
        assert reservation_state == 'ambiguous'
    finally:
        first.release()
        second.release()


def test_controller_handoff_requeues_reconcilable_work(request_database,
                                                       monkeypatch):
    engine, fixture_backend = request_database
    first_id = str(uuid.uuid4())
    second_id = str(uuid.uuid4())
    first = _controller_leader(engine, monkeypatch, first_id)
    first_backend = request_postgres.PostgresRequestBackend()
    second = request_postgres.ControllerLeaderLease(second_id)
    try:
        request = _controller_request('reconcilable-controller-action',
                                      replayable=True)
        assert asyncio.run(fixture_backend.create_if_not_exists_async(request))
        first_queue = request_postgres.PostgresQueueBackend(
            request.schedule_type.value,
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=first.generation)
        first_item = first_queue.get()
        assert first_item is not None
        assert first_backend.try_mark_running(first_item.request_id, 1234,
                                              first_item.execution_generation,
                                              first_item.claim_token)

        lock_backend_pid = first.backend_pid()
        assert lock_backend_pid is not None
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'),
                {'pid': lock_backend_pid})
        assert second.try_acquire()
        assert second.generation == 2
        fenced = request_postgres.fence_stale_controller_claims(
            second_id, second.generation)
        assert fenced == {'replayed': 1, 'interrupted': 0}

        monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                           second_id)
        second_queue = request_postgres.PostgresQueueBackend(
            request.schedule_type.value,
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=second.generation)
        second_item = second_queue.get()
        assert second_item is not None
        assert second_item.request_id == first_item.request_id
        assert (second_item.execution_generation ==
                first_item.execution_generation + 1)
        assert second_item.claim_token != first_item.claim_token
    finally:
        first.release()
        second.release()


def test_create_round_trip_and_atomic_enqueue(request_database):
    engine, backend = request_database
    request = _request('round-trip')
    assert asyncio.run(backend.create_if_not_exists_async(request))
    assert not asyncio.run(backend.create_if_not_exists_async(request))
    restored = backend.get_request(request.request_id)
    assert restored is not None
    assert restored.entrypoint is core.enabled_clouds
    assert restored.request_body == request.request_body
    with engine.connect() as connection:
        queue_row = connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                request.request_id)).mappings().one()
    assert queue_row['delivery_state'] == 'queued'
    assert queue_row['precondition_payload'] is None


def test_concurrent_creation_has_one_winner(request_database):
    _, backend = request_database

    async def create_all() -> list[bool]:
        start = asyncio.Event()

        async def create() -> bool:
            await start.wait()
            return await backend.create_if_not_exists_async(
                _request('create-race'))

        tasks = [asyncio.create_task(create()) for _ in range(8)]
        start.set()
        return await asyncio.gather(*tasks)

    results = asyncio.run(create_all())
    assert results.count(True) == 1
    assert results.count(False) == 7


def test_api_only_durable_schedule_needs_no_local_queue(request_database,
                                                        monkeypatch):
    _, backend = request_database
    monkeypatch.setattr(storage, '_storage_backend', backend)
    monkeypatch.setattr(executor, '_queue_factory', None)
    request = _request('api-only-enqueue')
    assert asyncio.run(backend.create_if_not_exists_async(request))
    asyncio.run(executor.schedule_prepared_request(request))
    assert request_postgres.PostgresQueueBackend('short').qsize() == 1


def test_concurrent_claims_deliver_once(request_database):
    _, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('claim-race')))
    barrier = threading.Barrier(8)

    def claim():
        barrier.wait()
        return request_postgres.PostgresQueueBackend('short').get()

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: claim(), range(8)))
    claimed = [item for item in results if item is not None]
    assert len(claimed) == 1
    assert claimed[0].request_id == 'claim-race'


def test_durable_precondition_reschedules_then_claims(request_database,
                                                      monkeypatch):
    engine, backend = request_database
    request = _request('durable-precondition')
    request.precondition_type = 'cluster-start-complete.v1'
    request.precondition_payload = {
        'cluster_name': 'test-cluster',
        'check_interval': 0.01,
    }
    request.precondition_deadline = time.time() + 10
    assert asyncio.run(backend.create_if_not_exists_async(request))
    check_once = mock.Mock(return_value=(False, 'Waiting for test cluster'))
    monkeypatch.setattr(preconditions, 'check_once', check_once)
    assert request_postgres.PostgresQueueBackend('short').get() is None
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                request.request_id)).mappings().one()
    assert row['delivery_state'] == 'queued'
    assert row['precondition_attempts'] == 1
    assert backend.get_request(
        request.request_id).status_msg == 'Waiting for test cluster'

    check_once.return_value = (True, None)
    time.sleep(0.02)
    item = request_postgres.PostgresQueueBackend('short').get()
    assert item is not None
    assert item.request_id == request.request_id
    assert check_once.call_count == 2


def test_expired_precondition_fails_and_removes_delivery(request_database):
    engine, backend = request_database
    request = _request('expired-precondition')
    request.precondition_type = 'cluster-start-complete.v1'
    request.precondition_payload = {
        'cluster_name': 'test-cluster',
        'check_interval': 0.01,
    }
    request.precondition_deadline = time.time() - 1
    assert asyncio.run(backend.create_if_not_exists_async(request))
    assert request_postgres.PostgresQueueBackend('short').get() is None
    restored = backend.get_request(request.request_id)
    assert restored.status is requests.RequestStatus.FAILED
    assert restored.get_error()['type'] == 'TimeoutError'
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.QUEUE).where(
                                 request_postgres.QUEUE.c.request_id ==
                                 request.request_id)).scalar_one() == 0


def test_heartbeat_and_terminal_writes_are_fenced(request_database):
    engine, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('fenced-write')))
    item = _claim(backend, 'fenced-write')
    assert item.claim_token is not None
    stale_claim = storage.ExecutionClaim(item.request_id,
                                         item.execution_generation,
                                         str(uuid.uuid4()))
    assert not backend.heartbeat_claim(stale_claim)
    stale_context = storage.activate_execution_claim(
        stale_claim.request_id, stale_claim.execution_generation,
        stale_claim.claim_token)
    try:
        backend.set_request_finished('fenced-write',
                                     requests.RequestStatus.SUCCEEDED,
                                     result=[])
    finally:
        storage.deactivate_execution_claim(stale_context)
    assert backend.get_request(
        'fenced-write').status is requests.RequestStatus.RUNNING
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(request_postgres.QUEUE.c.request_id).where(
                request_postgres.QUEUE.c.request_id ==
                'fenced-write')).scalar_one() == 'fenced-write'

    valid_claim = storage.ExecutionClaim(item.request_id,
                                         item.execution_generation,
                                         item.claim_token)
    valid_context = storage.activate_execution_claim(
        valid_claim.request_id, valid_claim.execution_generation,
        valid_claim.claim_token)
    try:
        assert backend.heartbeat_claim(valid_claim)
        backend.set_request_finished('fenced-write',
                                     requests.RequestStatus.SUCCEEDED,
                                     result=[])
    finally:
        storage.deactivate_execution_claim(valid_context)
    assert backend.get_request(
        'fenced-write').status is requests.RequestStatus.SUCCEEDED
    assert request_postgres.PostgresQueueBackend('short').qsize() == 0


def test_stale_terminal_write_skips_downstream_completion_side_effect(
        request_database, monkeypatch):
    _, backend = request_database
    monkeypatch.setattr(storage, '_storage_backend', backend)
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('stale-side-effect')))
    item = _claim(backend, 'stale-side-effect')
    stale_context = storage.activate_execution_claim(
        item.request_id, item.execution_generation - 1, item.claim_token)
    completion = mock.Mock()
    monkeypatch.setattr(requests, '_mark_container_image_request_terminal',
                        completion)
    try:
        requests.set_request_succeeded(item.request_id, [])
    finally:
        storage.deactivate_execution_claim(stale_context)
    completion.assert_not_called()
    assert backend.get_request(
        item.request_id).status is requests.RequestStatus.RUNNING


def test_expired_mutating_claim_is_not_replayed(request_database, monkeypatch):
    engine, backend = request_database
    monkeypatch.setattr(request_postgres, '_CLAIM_LEASE_SECONDS', 0.05)
    request = _request('expired-claim')
    request.name = 'sky.stop'
    request.entrypoint = core.stop
    request.request_body = payloads.StopOrDownBody(cluster_name='cluster')
    assert asyncio.run(backend.create_if_not_exists_async(request))
    item = request_postgres.PostgresQueueBackend('short').get()
    assert item is not None
    time.sleep(0.1)
    assert request_postgres.PostgresQueueBackend('short').get() is None
    restored = backend.get_request('expired-claim')
    assert restored.status is requests.RequestStatus.CANCELLED
    assert restored.should_retry
    assert 'ambiguous mutating outcome' in restored.interrupted_reason
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.QUEUE).where(
                                 request_postgres.QUEUE.c.request_id ==
                                 'expired-claim')).scalar_one() == 0


def test_expired_read_only_claim_replays_with_new_generation(
        request_database, monkeypatch):
    _, backend = request_database
    monkeypatch.setattr(request_postgres, '_CLAIM_LEASE_SECONDS', 0.05)
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('replay-read-only')))
    first = request_postgres.PostgresQueueBackend('short').get()
    assert first is not None
    time.sleep(0.1)
    second = request_postgres.PostgresQueueBackend('short').get()
    assert second is not None
    assert second.request_id == first.request_id
    assert second.execution_generation == first.execution_generation + 1
    assert second.claim_token != first.claim_token
    assert not backend.heartbeat_claim(
        storage.ExecutionClaim(first.request_id, first.execution_generation,
                               first.claim_token))


def test_terminal_internal_daemon_is_revived_with_fresh_delivery(
        request_database):
    _, backend = request_database
    request = requests.build_internal_daemon_request(
        daemons.INTERNAL_REQUEST_DAEMONS[0])
    assert asyncio.run(backend.create_or_refresh_internal_daemon_async(request))
    queue = request_postgres.PostgresQueueBackend('short')
    item = queue.get()
    assert item is not None
    context = storage.activate_execution_claim(item.request_id,
                                               item.execution_generation,
                                               item.claim_token)
    try:
        assert backend.try_mark_running(item.request_id, 1234,
                                        item.execution_generation,
                                        item.claim_token)
        backend.set_request_finished(item.request_id,
                                     requests.RequestStatus.FAILED,
                                     error=RuntimeError('daemon stopped'))
    finally:
        storage.deactivate_execution_claim(context)
    assert asyncio.run(backend.create_or_refresh_internal_daemon_async(request))
    restored = backend.get_request(request.request_id)
    assert restored.status is requests.RequestStatus.PENDING
    assert restored.error is None
    assert queue.qsize() == 1


def test_cancel_never_signals_a_different_instance(request_database,
                                                   monkeypatch):
    engine, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('cancel-fence')))
    item = _claim(backend, 'cancel-fence')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                'cancel-fence').values(worker_instance_id=uuid.uuid4()))
    kill = mock.Mock()
    monkeypatch.setattr(os, 'kill', kill)
    assert backend.kill_requests(['cancel-fence']) == ['cancel-fence']
    kill.assert_not_called()
    restored = backend.get_request('cancel-fence')
    assert restored.status is requests.RequestStatus.CANCELLED
    assert restored.cancel_requested_at is not None
    assert item.claim_token is not None


def test_remote_cancel_is_acknowledged_by_owning_executor(
        request_database, monkeypatch):
    engine, executor_backend = request_database
    executor_instance_id = executor_backend.instance_id
    assert asyncio.run(
        executor_backend.create_if_not_exists_async(_request('remote-cancel')))
    item = _claim(executor_backend, 'remote-cancel')
    assert item.claim_token is not None

    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       str(uuid.uuid4()))
    api_backend = request_postgres.PostgresRequestBackend()
    kill = mock.Mock()
    monkeypatch.setattr(request_postgres.os, 'kill', kill)
    assert api_backend.kill_requests(['remote-cancel']) == ['remote-cancel']
    kill.assert_not_called()

    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       executor_instance_id)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token)
    assert executor_backend.interrupt_cancelled_claim(claim)
    kill.assert_called_once_with(1234, request_postgres.signal.SIGTERM)
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.cancel_acknowledged_at).where(
                    request_postgres.REQUESTS.c.request_id ==
                    'remote-cancel')).one()
    assert row.cancel_acknowledged_at is not None


def test_cancel_never_signals_an_expired_local_claim(request_database,
                                                     monkeypatch):
    engine, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('cancel-expired')))
    item = _claim(backend, 'cancel-expired')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == 'cancel-expired').
            values(lease_expires_at=sqlalchemy.func.clock_timestamp() -
                   datetime.timedelta(seconds=1)))
    kill = mock.Mock()
    monkeypatch.setattr(os, 'kill', kill)
    assert backend.kill_requests(['cancel-expired']) == ['cancel-expired']
    kill.assert_not_called()
    restored = backend.get_request('cancel-expired')
    assert restored.status is requests.RequestStatus.CANCELLED
    assert item.claim_token is not None


def test_registry_rejects_row_selected_code_and_execution_class():
    registry.register_builtin_handlers()
    with pytest.raises(ValueError, match='Unknown durable request handler'):
        registry.resolve_handler('os:system')
    request = _request('class-fence')
    values = request.durable_values()
    values['execution_class'] = registry.ExecutionClass.CONTROLLER.value
    with pytest.raises(ValueError, match='execution class'):
        requests.Request.from_durable_values(values)


def test_registry_owns_controller_classes_and_replay_policies():
    jobs_launch = registry.registration_for_handler(managed_jobs_core.launch)
    jobs_queue = registry.registration_for_handler(managed_jobs_core.queue)
    serve_status = registry.registration_for_handler(serve_core.status)
    normal_read = registry.registration_for_handler(core.enabled_clouds)
    daemon = registry.registration_for_handler(
        daemons.INTERNAL_REQUEST_DAEMONS[0].run_event)

    assert jobs_launch.execution_class is registry.ExecutionClass.CONTROLLER
    assert jobs_launch.replay_policy is registry.ReplayPolicy.NEVER
    assert jobs_queue.execution_class is registry.ExecutionClass.CONTROLLER
    assert jobs_queue.replay_policy is registry.ReplayPolicy.READ_ONLY
    assert serve_status.execution_class is registry.ExecutionClass.CONTROLLER
    assert serve_status.replay_policy is registry.ReplayPolicy.READ_ONLY
    assert normal_read.execution_class is registry.ExecutionClass.NORMAL
    assert normal_read.replay_policy is registry.ReplayPolicy.READ_ONLY
    assert daemon.execution_class is registry.ExecutionClass.CONTROLLER
    assert daemon.replay_policy is registry.ReplayPolicy.RECONCILE


def test_sqlite_cutover_is_atomic_verified_and_idempotent(
        request_database, tmp_path, monkeypatch):
    engine, backend = request_database
    source = tmp_path / 'legacy-requests.db'
    gate = tmp_path / 'cutover-gate.json'
    monkeypatch.setenv(cutover.CUTOVER_GATE_PATH_ENV_VAR, str(gate))
    monkeypatch.delenv(request_postgres.REQUEST_BACKEND_ENV_VAR, raising=False)
    finished = _request('legacy-finished', should_enqueue=False)
    finished.status = requests.RequestStatus.SUCCEEDED
    finished.finished_at = time.time()
    finished.set_return_value([])
    pending = _request('legacy-pending', should_enqueue=False)
    _write_legacy_database(source, [finished, pending])

    cutover.block_legacy_submissions(str(source))
    assert cutover.legacy_submissions_blocked()
    with pytest.raises(cutover.RequestCutoverInProgressError):
        cutover.require_legacy_submissions_allowed()
    report = cutover.import_legacy_requests(str(source),
                                            confirm_source_writers_stopped=True)
    assert report.request_count == 2
    assert report.queue_count == 1
    assert not report.already_completed
    assert backend.get_request(
        finished.request_id).status is requests.RequestStatus.SUCCEEDED
    assert backend.get_request(
        pending.request_id).status is requests.RequestStatus.PENDING
    with engine.connect() as connection:
        marker = connection.execute(
            sqlalchemy.select(request_postgres.STORE_METADATA.c.value).where(
                request_postgres.STORE_METADATA.c.key ==
                cutover.CUTOVER_METADATA_KEY)).scalar_one()
    assert marker['logical_sha256'] == report.logical_sha256
    assert marker['request_count'] == 2
    assert stat.S_IMODE(source.stat().st_mode) & (stat.S_IWUSR | stat.S_IWGRP |
                                                  stat.S_IWOTH) == 0

    repeated = cutover.import_legacy_requests(
        str(source), confirm_source_writers_stopped=True)
    assert repeated.already_completed
    assert repeated.logical_sha256 == report.logical_sha256


def test_sqlite_cutover_requires_explicit_running_interrupt(
        request_database, tmp_path, monkeypatch):
    _, backend = request_database
    source = tmp_path / 'running-requests.db'
    monkeypatch.setenv(cutover.CUTOVER_GATE_PATH_ENV_VAR,
                       str(tmp_path / 'cutover-gate.json'))
    running = _request('legacy-running', should_enqueue=False)
    running.status = requests.RequestStatus.RUNNING
    running.pid = 4321
    _write_legacy_database(source, [running])
    with pytest.raises(RuntimeError, match='still RUNNING'):
        cutover.import_legacy_requests(str(source),
                                       confirm_source_writers_stopped=True)
    report = cutover.import_legacy_requests(str(source),
                                            confirm_source_writers_stopped=True,
                                            interrupt_running=True)
    assert report.interrupted_request_ids == ('legacy-running',)
    restored = backend.get_request('legacy-running')
    assert restored.status is requests.RequestStatus.CANCELLED
    assert restored.should_retry
    assert restored.pid is None
    repeated = cutover.import_legacy_requests(
        str(source),
        confirm_source_writers_stopped=True,
        interrupt_running=True)
    assert repeated.already_completed
    assert repeated.logical_sha256 == report.logical_sha256
    assert repeated.completed_at == report.completed_at


def test_sqlite_cutover_serializes_concurrent_running_importers(
        request_database, tmp_path, monkeypatch):
    del request_database
    source = tmp_path / 'concurrent-running-requests.db'
    monkeypatch.setenv(cutover.CUTOVER_GATE_PATH_ENV_VAR,
                       str(tmp_path / 'cutover-gate.json'))
    running = _request('legacy-concurrent-running', should_enqueue=False)
    running.status = requests.RequestStatus.RUNNING
    running.pid = 4321
    _write_legacy_database(source, [running])
    barrier = threading.Barrier(2)

    def import_source():
        barrier.wait()
        return cutover.import_legacy_requests(
            str(source),
            confirm_source_writers_stopped=True,
            interrupt_running=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(import_source) for _ in range(2)]
        reports = [future.result() for future in futures]

    assert sorted(
        report.already_completed for report in reports) == [False, True]
    assert len({report.logical_sha256 for report in reports}) == 1
    assert len({report.completed_at for report in reports}) == 1


def test_claim_predicate_uses_database_clock(request_database):
    """A claim with an expired database timestamp cannot start or heartbeat."""
    engine, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('database-clock')))
    item = request_postgres.PostgresQueueBackend('short').get()
    assert item is not None
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == item.request_id).
            values(
                lease_expires_at=datetime.datetime.now(datetime.timezone.utc) -
                datetime.timedelta(seconds=1)))
    assert not backend.try_mark_running(
        item.request_id, 1234, item.execution_generation, item.claim_token)
    assert not backend.heartbeat_claim(
        storage.ExecutionClaim(item.request_id, item.execution_generation,
                               item.claim_token))


def test_terminal_event_commits_with_request_and_queue_exactly_once(
        request_database):
    engine, backend = request_database
    request = _event_request('event-success')
    assert asyncio.run(backend.create_if_not_exists_async(request))

    assert backend.transition_request_terminal(
        request.request_id, requests.RequestStatus.SUCCEEDED,
        event_api_models.EventCause.HANDLER_SUCCEEDED.value)
    assert not backend.transition_request_terminal(
        request.request_id, requests.RequestStatus.SUCCEEDED,
        event_api_models.EventCause.HANDLER_SUCCEEDED.value)

    with engine.connect() as connection:
        stored_request = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id)).mappings().one()
        event_rows = list(
            connection.execute(
                sqlalchemy.select(event_schema.RESOURCE_EVENTS).where(
                    event_schema.RESOURCE_EVENTS.c.source_request_id ==
                    request.request_id)).mappings())
        target_rows = list(
            connection.execute(
                sqlalchemy.select(
                    event_schema.RESOURCE_EVENT_TARGETS)).mappings())
        queue_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(
                                 request_postgres.QUEUE)).scalar_one()

    assert stored_request['status'] == requests.RequestStatus.SUCCEEDED.value
    assert len(event_rows) == 1
    assert event_rows[0]['event_sequence'] == 1
    assert event_rows[0]['kind'] == 'cluster.launch'
    assert event_rows[0]['outcome'] == 'succeeded'
    assert event_rows[0]['cause'] == 'handler_succeeded'
    assert event_rows[0]['message'] == 'Cluster launch succeeded.'
    assert event_rows[0]['actor_id'] == 'user'
    assert event_rows[0]['actor_name'] == 'alice@example.com'
    assert event_rows[0]['actor_type'] == 'sso'
    assert event_rows[0]['workspace'] == 'default'
    assert event_rows[0]['source_execution_generation'] == 0
    assert len(target_rows) == 1
    assert target_rows[0]['target_id'] == 'hash-trainer'
    assert target_rows[0]['target_name'] == 'trainer'
    assert queue_count == 0


def test_event_insert_failure_rolls_back_terminal_transition_and_delivery(
        request_database, monkeypatch):
    engine, backend = request_database
    request = _event_request('event-rollback')
    assert asyncio.run(backend.create_if_not_exists_async(request))
    monkeypatch.setattr(
        request_postgres.event_emission, 'emit_terminal_event',
        mock.Mock(side_effect=RuntimeError('injected event failure')))

    with pytest.raises(RuntimeError, match='injected event failure'):
        backend.transition_request_terminal(
            request.request_id,
            requests.RequestStatus.FAILED,
            event_api_models.EventCause.HANDLER_FAILED.value,
            error=RuntimeError('provider detail'))

    with engine.connect() as connection:
        status = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS.c.status).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id)).scalar_one()
        queue_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(
                                 request_postgres.QUEUE)).scalar_one()
        event_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(
                                 event_schema.RESOURCE_EVENTS)).scalar_one()
    assert status == requests.RequestStatus.PENDING.value
    assert queue_count == 1
    assert event_count == 0


def test_null_context_and_nonterminal_retry_emit_nothing(request_database):
    engine, backend = request_database
    request = _request('event-opt-out')
    request.name = 'sky.start'
    request.cluster_name = 'trainer'
    assert asyncio.run(backend.create_if_not_exists_async(request))
    assert backend.set_event_workspace(request.request_id, 'default')
    asyncio.run(
        backend.update_status_async(request.request_id,
                                    requests.RequestStatus.WAITING))
    assert backend.transition_request_terminal(
        request.request_id, requests.RequestStatus.CANCELLED,
        event_api_models.EventCause.EXPLICIT_CANCEL.value)
    with engine.connect() as connection:
        count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(
                                 event_schema.RESOURCE_EVENTS)).scalar_one()
    assert count == 0


def test_incomplete_context_pre_execution_terminal_emits_nothing(
        request_database):
    engine, backend = request_database
    request = _event_request('event-before-workspace')
    assert request.event_context is not None
    request.event_context['workspace'] = None
    assert asyncio.run(backend.create_if_not_exists_async(request))

    assert backend.transition_request_terminal(
        request.request_id,
        requests.RequestStatus.FAILED,
        event_api_models.EventCause.PRECONDITION_FAILED.value,
        error=RuntimeError('precondition detail'),
    )
    with engine.connect() as connection:
        count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(
                                 event_schema.RESOURCE_EVENTS)).scalar_one()
    assert count == 0


def test_ambiguous_terminal_cause_emits_safe_canceled_event(request_database):
    engine, backend = request_database
    request = _event_request('event-ambiguous')
    assert asyncio.run(backend.create_if_not_exists_async(request))

    assert backend.transition_request_terminal(
        request.request_id,
        requests.RequestStatus.CANCELLED,
        event_api_models.EventCause.EXECUTION_LEASE_EXPIRED.value,
    )
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(event_schema.RESOURCE_EVENTS).where(
                event_schema.RESOURCE_EVENTS.c.source_request_id ==
                request.request_id)).mappings().one()
    assert row['outcome'] == event_api_models.EventOutcome.CANCELED.value
    assert row['cause'] == (
        event_api_models.EventCause.EXECUTION_LEASE_EXPIRED.value)
    assert row['message'] == (
        'Cluster launch was interrupted. The external outcome may be '
        'uncertain.')


def test_event_store_enforces_workspace_filters_and_signed_cursors(
        request_database, monkeypatch):
    _, backend = request_database
    monkeypatch.setenv(request_postgres.REQUEST_BACKEND_ENV_VAR,
                       request_postgres.POSTGRES_REQUEST_BACKEND)
    for request_id, workspace, cluster_name in [
        ('research-old', 'research', 'trainer-a'),
        ('research-new', 'research', 'trainer-b'),
        ('finance-hidden', 'finance', 'ledger'),
    ]:
        request = _event_request(request_id,
                                 workspace=workspace,
                                 cluster_name=cluster_name)
        assert asyncio.run(backend.create_if_not_exists_async(request))
        assert backend.transition_request_terminal(
            request_id, requests.RequestStatus.SUCCEEDED,
            event_api_models.EventCause.HANDLER_SUCCEEDED.value)

    scope = event_store.AuthorizationScope(
        principal_id='alice',
        is_admin=False,
        effective_workspaces=('research',),
    )
    targeted = event_store.list_events(
        event_store.EventQuery(
            target_type=event_api_models.EventTargetType.CLUSTER,
            target_id='hash-trainer-a',
            limit=100,
        ), scope)
    assert [item.request_id for item in targeted.items] == ['research-old']
    query = event_store.EventQuery(workspaces=('research',), limit=1)
    first = event_store.list_events(query, scope)
    assert len(first.items) == 1
    assert first.items[0].workspace == 'research'
    assert first.has_more
    assert first.next_cursor is not None

    second = event_store.list_events(
        dataclasses.replace(query, cursor=first.next_cursor), scope)
    assert len(second.items) == 1
    assert second.items[0].workspace == 'research'
    assert second.items[0].id != first.items[0].id
    assert not second.has_more

    changed_filter = dataclasses.replace(
        query,
        cursor=first.next_cursor,
        outcomes=(event_api_models.EventOutcome.SUCCEEDED,))
    with pytest.raises(event_cursors.StaleCursorError):
        event_store.list_events(changed_filter, scope)
    with pytest.raises(event_cursors.StaleCursorError):
        event_store.list_events(
            dataclasses.replace(query, cursor=first.next_cursor),
            dataclasses.replace(scope, principal_id='bob'))
    with pytest.raises(event_cursors.StaleCursorError):
        event_store.list_events(
            dataclasses.replace(query, cursor=first.next_cursor),
            dataclasses.replace(scope,
                                effective_workspaces=('finance', 'research')))

    new_request = _event_request('research-latest',
                                 workspace='research',
                                 cluster_name='trainer-c')
    assert asyncio.run(backend.create_if_not_exists_async(new_request))
    assert backend.transition_request_terminal(
        new_request.request_id, requests.RequestStatus.FAILED,
        event_api_models.EventCause.HANDLER_FAILED.value)
    newer = event_store.list_events(
        event_store.EventQuery(
            workspaces=('research',),
            direction=(event_api_models.TraversalDirection.NEWER),
            cursor=first.poll_cursor,
            limit=100), scope)
    assert [item.request_id for item in newer.items] == ['research-latest']
    assert newer.items[0].outcome == event_api_models.EventOutcome.FAILED


def test_poll_cursor_does_not_skip_an_event_committed_after_snapshot(
        request_database, monkeypatch):
    engine, _ = request_database
    monkeypatch.setenv(request_postgres.REQUEST_BACKEND_ENV_VAR,
                       request_postgres.POSTGRES_REQUEST_BACKEND)
    request = _event_request('late-commit')
    emission_row = {
        'request_id': request.request_id,
        'name': request.name,
        'user_id': request.user_id,
        'execution_generation': 1,
        'event_context': request.event_context,
    }
    scope = event_store.AuthorizationScope(principal_id='admin',
                                           is_admin=True,
                                           effective_workspaces=None)

    writer = engine.connect()
    transaction = writer.begin()
    try:
        assert event_emission.emit_terminal_event(
            writer,
            emission_row,
            status=requests.RequestStatus.SUCCEEDED.value,
            cause=event_api_models.EventCause.HANDLER_SUCCEEDED,
        )
        before_commit = event_store.list_events(
            event_store.EventQuery(
                direction=event_api_models.TraversalDirection.NEWER), scope)
        assert before_commit.items == []
        transaction.commit()
    finally:
        if transaction.is_active:
            transaction.rollback()
        writer.close()

    after_commit = event_store.list_events(
        event_store.EventQuery(
            direction=event_api_models.TraversalDirection.NEWER,
            cursor=before_commit.poll_cursor,
        ), scope)
    assert [item.request_id for item in after_commit.items] == ['late-commit']


def test_event_retention_batches_and_cascades_targets(request_database,
                                                      monkeypatch):
    engine, backend = request_database
    monkeypatch.setenv(request_postgres.REQUEST_BACKEND_ENV_VAR,
                       request_postgres.POSTGRES_REQUEST_BACKEND)
    for request_id in ('expired-event', 'fresh-event'):
        request = _event_request(request_id)
        assert asyncio.run(backend.create_if_not_exists_async(request))
        assert backend.transition_request_terminal(
            request_id, requests.RequestStatus.SUCCEEDED,
            event_api_models.EventCause.HANDLER_SUCCEEDED.value)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(event_schema.RESOURCE_EVENTS).where(
                event_schema.RESOURCE_EVENTS.c.source_request_id ==
                'expired-event').values(
                    occurred_at=(sqlalchemy.func.clock_timestamp() -
                                 datetime.timedelta(hours=2))))

    assert event_store.delete_expired_events(1, batch_size=1) == 1
    assert event_store.delete_expired_events(1, batch_size=1) == 0
    with engine.connect() as connection:
        events = list(
            connection.execute(
                sqlalchemy.select(event_schema.RESOURCE_EVENTS.c.
                                  source_request_id)).scalars())
        target_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(event_schema.RESOURCE_EVENT_TARGETS)).scalar_one()
    assert events == ['fresh-event']
    assert target_count == 1
