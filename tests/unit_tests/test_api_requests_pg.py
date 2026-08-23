"""Real-PostgreSQL tests for durable API request delivery."""
# pylint: disable=protected-access,redefined-outer-name,unexpected-keyword-arg

import ast
import asyncio
import concurrent.futures
import dataclasses
import datetime
import os
import pathlib
import shutil
import signal
import sqlite3
import stat
import threading
import time
from unittest import mock
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from sqlalchemy.ext import asyncio as sqlalchemy_async

from sky import core
from sky import exceptions
from sky import execution
from sky import global_user_state
from sky import global_user_state_schema
from sky import models
from sky.events import api_models as event_api_models
from sky.jobs import state_schema as managed_job_state_schema
from sky.jobs.server import core as managed_jobs_core
from sky.serve import constants as serve_constants
from sky.serve import ordinary_launch_binding
from sky.serve import pool_capacity_observation_schema
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import service
from sky.serve import zero_cost_actuation
from sky.serve.server import core as serve_core
from sky.server import daemons
from sky.server.events import cursors as event_cursors
from sky.server.events import emission as event_emission
from sky.server.events import schema as event_schema
from sky.server.events import store as event_store
from sky.server.requests import cutover
from sky.server.requests import executor
from sky.server.requests import non_pool_admission
from sky.server.requests import non_pool_launch as non_pool_launch_request
from sky.server.requests import ordinary_launch as ordinary_launch_request
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import preconditions
from sky.server.requests import registry
from sky.server.requests import request_names
from sky.server.requests import requests
from sky.server.requests import storage
from sky.server.requests.queues import base as queue_base
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import controller_capability
from sky.utils.db import db_utils
from sky.utils.db import migration_utils
from sky.volumes.server import core as volume_core

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
testcontainers_postgres = None
if _POSTGRES_URL is None:
    testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    _POSTGRES_URL is None and shutil.which('docker') is None,
    reason='docker unavailable; skipping durable request PostgreSQL tests')

_GC_REPLICA_RECORD_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_GC_CONTROLLER_ID = uuid.UUID('33333333-3333-4333-8333-333333333333')


def _gc_replica_state() -> dict[str, object]:
    info = replica_managers.ReplicaInfo(replica_id=3,
                                        cluster_name='gc-service-3',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=2,
                                        resources_override=None)
    info.replica_record_id = str(_GC_REPLICA_RECORD_ID)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.RUNNING
    return info.to_storage_dict()


def _gc_reserved_fill_info() -> replica_managers.ReplicaInfo:
    info = replica_managers.ReplicaInfo.from_storage_dict(_gc_replica_state())
    info.reserved_fill = True
    info.is_zero_cost = True
    info.zero_cost_admission_sequence = 1
    values = {
        'reserved_fill_pool_key': 'kubernetes/context-a/l4/physical-uid-a/v2',
        'reserved_fill_service_generation': 7,
        'reserved_fill_physical_cluster_uid': 'physical-uid-a',
        'reserved_fill_kubernetes_context': 'context-a',
        'reserved_fill_allocation_generation': 8,
        'reserved_fill_allocation_input_sha256': 'a' * 64,
        'reserved_fill_allocation_claim_generation': 7,
        'reserved_fill_reconciliation_gate_generation': 9,
        'reserved_fill_reclaim_fleet_bundle_sha256': 'b' * 64,
        'reserved_fill_reclaim_policy_revision': 'policy-v1',
        'reserved_fill_reclaim_provider_inventory_sha256': 'c' * 64,
        'reserved_fill_worker_projection_sha256': 'd' * 64,
        'reserved_fill_observation_generation': 10,
        'reserved_fill_observation_sequence': 0,
        'reserved_fill_intent_idempotency_key': 'e' * 64,
    }
    for field, value in values.items():
        setattr(info, field, value)
    return info


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
        temporary_database = f'skypilot_actions_test_{uuid.uuid4().hex}'
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
def request_database(postgres_engine, monkeypatch):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    global_user_state_schema.user_table.create(postgres_engine, checkfirst=True)
    request_postgres._initialize_schema(postgres_engine)
    async_url = postgres_engine.url.set(
        drivername='postgresql+asyncpg').render_as_string(hide_password=False)
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


@pytest.fixture
def bound_request_database(request_database, monkeypatch):
    """Request and Serve binding schemas on one central PostgreSQL database."""
    engine, backend = request_database
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    # Bound launch reduction acquires the global zero-cost event sequencer
    # before its existing lifecycle/service/replica locks. Exercise the
    # current additive-stack schema boundary even while the reconciliation
    # gate remains in legacy mode.
    alembic_command.upgrade(config, migration_utils.SERVE_VERSION)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine', engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name='gc-service', epoch=4))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='gc-service',
                workspace='workspace-a',
                status='READY',
                hash='gc-service-hash',
                current_version=2,
                active_versions='[2]',
                pool=0,
                controller_pid=123,
                controller_ip='10.0.0.2',
                lifecycle_epoch=4,
                controller_incarnation=_GC_CONTROLLER_ID,
                controller_owner_epoch=6,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=5))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name='gc-service',
                version=2,
                yaml_content='service:\n  min_replicas: 0\n',
                controller_applied_at=1.0))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name='gc-service',
                replica_id=3,
                replica_state_version=1,
                status='PROVISIONING',
                version=2,
                cluster_name='gc-service-3',
                is_spot=False,
                replica_state=_gc_replica_state()))
    return engine, backend


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


def _bound_request(request_id: str) -> requests.Request:
    return requests.Request(
        request_id=request_id,
        name='sky.launch',
        entrypoint=ordinary_launch_request.launch,
        request_body=payloads.LaunchBody(
            task='name: ordinary-serve-launch\nrun: echo bound\n',
            cluster_name='svc-1',
            is_launched_by_sky_serve_controller=True,
            extra_launch_context={}),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='user',
        cluster_name='svc-1',
        schedule_type=requests.ScheduleType.SHORT,
        retryable=False,
        should_enqueue=True,
    )


def _bound_non_pool_request(request_id: str) -> requests.Request:
    return requests.Request(
        request_id=request_id,
        name='sky.launch',
        entrypoint=non_pool_launch_request.launch,
        request_body=payloads.LaunchBody(
            task='name: generic-serve-launch\nrun: echo bound\n',
            cluster_name='gc-service-3',
            is_launched_by_sky_serve_controller=True,
            env_vars={constants.USER_ID_ENV_VAR: 'tenant-a'},
            extra_launch_context={}),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='tenant-a',
        cluster_name='gc-service-3',
        schedule_type=requests.ScheduleType.SHORT,
        retryable=False,
        should_enqueue=True,
    )


def _gc_unbound_non_pool_launch_body() -> payloads.LaunchBody:
    return payloads.LaunchBody(
        task='name: generic-serve-launch\nrun: echo bound\n',
        cluster_name='gc-service-3',
        is_launched_by_sky_serve_controller=True,
        client_api_version=None,
        env_vars={
            constants.USER_ID_ENV_VAR: 'submitted-owner',
            constants.USER_ENV_VAR: 'Submitted Owner',
        },
        extra_launch_context={
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'gc-service',
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'gc-service-hash',
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 2,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.2',
            ordinary_launch_binding.REPLICA_ID_KEY: 3,
            ordinary_launch_binding.REPLICA_RECORD_ID_KEY:
                str(_GC_REPLICA_RECORD_ID),
            ordinary_launch_binding.LIFECYCLE_EPOCH_KEY: 4,
            ordinary_launch_binding.BINDING_EPOCH_KEY: 6,
            ordinary_launch_binding.CONTROLLER_INCARNATION_KEY:
                str(_GC_CONTROLLER_ID),
            ordinary_launch_binding.CONTROLLER_OWNER_EPOCH_KEY: 6,
        })


def _legacy_serve_launch_request(request_id: str) -> requests.Request:
    return requests.Request(
        request_id=request_id,
        name='sky.launch',
        entrypoint=execution.launch,
        request_body=payloads.LaunchBody(
            task='name: legacy-ordinary-serve-launch\nrun: echo legacy\n',
            cluster_name='gc-service-3',
            is_launched_by_sky_serve_controller=True,
            extra_launch_context={
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'gc-service',
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'gc-service-hash',
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 2,
                serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
                serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.2',
            }),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='user',
        cluster_name='gc-service-3',
        schedule_type=requests.ScheduleType.SHORT,
        retryable=False,
        should_enqueue=True,
    )


def _gc_binding_identity(
    submission_id: uuid.UUID | None = None
) -> ordinary_launch_binding.BindingIdentity:
    if submission_id is None:
        submission_id = uuid.UUID('11111111-1111-4111-8111-111111111111')
    intent = ordinary_launch_binding.BindingIntent(
        service_name='gc-service',
        service_hash='gc-service-hash',
        service_version=2,
        replica_id=3,
        replica_record_id=_GC_REPLICA_RECORD_ID,
        lifecycle_epoch=4,
        binding_epoch=5,
        controller_incarnation=_GC_CONTROLLER_ID,
        controller_owner_epoch=6,
        controller_pid=123,
        controller_ip='10.0.0.2')
    return ordinary_launch_binding.build_binding_identity(
        intent,
        submission_id=submission_id,
        tenant_scope='tenant-a',
        service_workspace='workspace-a',
        cluster_name='gc-service-3',
        input_digest='a' * 64)


def _gc_non_pool_binding_identity(
    profile: ordinary_launch_binding.NonPoolLaunchProfile,
    submission_id: uuid.UUID | None = None,
    input_digest: str = 'b' * 64,
) -> ordinary_launch_binding.NonPoolBindingIdentity:
    if submission_id is None:
        submission_id = uuid.UUID('44444444-4444-4444-8444-444444444444')
    intent = ordinary_launch_binding.BindingIntent(
        service_name='gc-service',
        service_hash='gc-service-hash',
        service_version=2,
        replica_id=3,
        replica_record_id=_GC_REPLICA_RECORD_ID,
        lifecycle_epoch=4,
        binding_epoch=6,
        controller_incarnation=_GC_CONTROLLER_ID,
        controller_owner_epoch=6,
        controller_pid=123,
        controller_ip='10.0.0.2')
    return ordinary_launch_binding.build_non_pool_binding_identity(
        intent,
        submission_id=submission_id,
        tenant_scope='tenant-a',
        service_workspace='workspace-a',
        cluster_name='gc-service-3',
        input_digest=input_digest,
        profile=profile,
        capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))


def _gc_binding_context(
    identity: ordinary_launch_binding.BindingIdentity,
    launch_generation: int,
) -> ordinary_launch_binding.BoundLaunchContext:
    return ordinary_launch_binding.BoundLaunchContext(
        association_id=identity.association_id,
        request_id=identity.request_id,
        service_name=identity.service_name,
        replica_id=identity.replica_id,
        replica_record_id=identity.replica_record_id,
        launch_generation=launch_generation,
        input_digest=identity.input_digest)


def _gc_binding_authority(
) -> ordinary_launch_binding.ControllerBindingAuthority:
    return ordinary_launch_binding.ControllerBindingAuthority(
        service_name='gc-service',
        service_hash='gc-service-hash',
        service_workspace='workspace-a',
        service_lifecycle_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.2',
        controller_incarnation=_GC_CONTROLLER_ID,
        controller_owner_epoch=6,
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        binding_epoch=5)


def _gc_legacy_identity(
        request_id: str) -> (ordinary_launch_binding.LegacyLaunchIdentity):
    return ordinary_launch_binding.LegacyLaunchIdentity(
        service_name='gc-service',
        service_hash='gc-service-hash',
        service_lifecycle_epoch=4,
        replica_id=3,
        replica_record_id=_GC_REPLICA_RECORD_ID,
        replica_version=2,
        cluster_name='gc-service-3',
        request_id=request_id,
        provider_context='kubernetes-context-a',
        provider_physical_resource_uid='cluster-uid-a')


def _controller_request(
    request_id: str,
    *,
    replayable: bool = False,
) -> requests.Request:
    if replayable:
        return requests.Request(
            request_id=request_id,
            name='sky.jobs.queue',
            entrypoint=managed_jobs_core.queue,
            request_body=payloads.JobsQueueBody(),
            status=requests.RequestStatus.PENDING,
            created_at=time.time(),
            user_id='user',
            schedule_type=requests.ScheduleType.SHORT,
            should_enqueue=True,
        )
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


def _legacy_serve_status_request(request_id: str) -> requests.Request:
    return requests.Request(
        request_id=request_id,
        name='serve.status',
        entrypoint=serve_core.status,
        request_body=payloads.ServeStatusBody(
            service_names=['owned'], authorized_owner_user_id='owner-a'),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='owner-a',
        schedule_type=requests.ScheduleType.SHORT,
        should_enqueue=True,
    )


def _legacy_serve_placement_request(request_id: str) -> requests.Request:
    return requests.Request(
        request_id=request_id,
        name='serve.placement',
        entrypoint=serve_core.placement,
        request_body=payloads.ServePlacementBody(
            service_name='owned', authorized_owner_user_id='owner-a'),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='owner-a',
        schedule_type=requests.ScheduleType.SHORT,
        should_enqueue=True,
    )


def _authorized_serve_status_request(request_id: str) -> requests.Request:
    return requests.Request(
        request_id=request_id,
        name='serve.status',
        entrypoint=serve_core.authorized_status,
        request_body=payloads.ServeAuthorizedStatusBody(
            service_names=['owned'], authorized_owner_user_id='owner-a'),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='owner-a',
        schedule_type=requests.ScheduleType.SHORT,
        should_enqueue=True,
    )


def _authorized_serve_placement_request(request_id: str) -> requests.Request:
    return requests.Request(
        request_id=request_id,
        name='serve.placement',
        entrypoint=serve_core.authorized_placement,
        request_body=payloads.ServeAuthorizedPlacementBody(
            service_name='owned', authorized_owner_user_id='owner-a'),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='owner-a',
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


def _seed_managed_job_attempt(
    engine: sqlalchemy.engine.Engine,
    leader: request_postgres.ControllerLeaderLease,
    *,
    job_id: int,
    slot_id: int,
    slot_attempt: uuid.UUID,
    schedule_state: str = 'ALIVE',
    quiescing: bool = False,
) -> None:
    assert leader.generation is not None
    managed_job_state_schema.job_info_table.create(engine, checkfirst=True)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(managed_job_state_schema.job_info_table).values(
                spot_job_id=job_id,
                schedule_state=schedule_state,
                controller_instance_id=leader.instance_id,
                controller_generation=leader.generation,
                controller_slot_id=slot_id,
                controller_slot_attempt=str(slot_attempt),
                controller_slot_quiescing=quiescing))


def _managed_job_request(
    request_id: str,
    leader: request_postgres.ControllerLeaderLease,
    *,
    job_id: int,
    slot_id: int,
    slot_attempt: uuid.UUID,
) -> requests.Request:
    assert leader.generation is not None
    request = _request(request_id)
    request.managed_job_id = job_id
    request.managed_job_controller_instance_id = leader.instance_id
    request.managed_job_controller_generation = leader.generation
    request.managed_job_controller_slot_id = slot_id
    request.managed_job_controller_slot_attempt = str(slot_attempt)
    return request


def _seed_legacy_managed_job(
    engine: sqlalchemy.engine.Engine,
    *,
    job_id: int,
    schedule_state: str = 'ALIVE',
    controller_instance_id: str | None = None,
    controller_generation: int | None = None,
) -> None:
    managed_job_state_schema.job_info_table.create(engine, checkfirst=True)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(managed_job_state_schema.job_info_table).values(
                spot_job_id=job_id,
                schedule_state=schedule_state,
                controller_instance_id=controller_instance_id,
                controller_generation=controller_generation,
                controller_slot_id=None,
                controller_slot_attempt=None,
                controller_slot_quiescing=False))


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
                                    item.execution_generation, item.claim_token,
                                    424242)
    return item


def _claim_bound(backend: request_postgres.PostgresRequestBackend,
                 request_id: str) -> queue_base.QueueItem:
    queue = request_postgres.PostgresQueueBackend('short')
    candidate = queue.peek_provider_mutation()
    assert candidate is not None
    assert candidate.request_id == request_id
    item = queue.claim_provider_mutation(candidate)
    assert item is not None
    assert item.request_id == request_id
    assert item.claim_token is not None
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token,
                                    424242)
    return item


def _insert_execution_owner(
    engine: sqlalchemy.engine.Engine,
    instance_id: str,
    *,
    pod_name: str,
    pod_uid: str | None,
    heartbeat_at: datetime.datetime,
    role: str = 'executor',
) -> None:
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                instance_id=uuid.UUID(instance_id),
                role=role,
                pod_name=pod_name,
                pod_uid=pod_uid,
                pod_ip='10.0.0.10',
                version='owner-death-test',
                started_at=heartbeat_at,
                heartbeat_at=heartbeat_at,
                ready=True,
                health_detail={'phase': 'claiming'},
                supported_handlers=[],
                supported_payload_versions={},
                request_storage_backend=(
                    request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE),
                request_queue_backend=(
                    request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE),
                execution_quiescence_capable=True,
                ordinary_launch_binding_capable=True))


def _action_values(action_id: uuid.UUID,
                   *,
                   resource_identity: str = 'service:replica:1',
                   generation: int = 1) -> dict[str, object]:
    return {
        'action_id': action_id,
        'domain': 'serve',
        'resource_type': 'replica',
        'resource_identity': resource_identity,
        'desired_generation': generation,
        'action_type': 'launch',
        'immutable_spec': {
            'version': 1,
            'resource_identity': resource_identity,
        },
        'immutable_spec_sha256': 'a' * 64,
        'kernel_state': 'READY',
        'current_attempt': 0,
        'next_attempt_at': sqlalchemy.func.clock_timestamp(),
    }


def _attempt_values(action_id: uuid.UUID, attempt: int,
                    request_id: str) -> dict[str, object]:
    return {
        'action_id': action_id,
        'attempt': attempt,
        'request_id': request_id,
        'request_input_sha256': 'b' * 64,
        'mutation_boundary': 'NOT_STARTED',
    }


def _normalized_index_predicate(index: dict[str, object]) -> str:
    dialect_options = index['dialect_options']
    assert isinstance(dialect_options, dict)
    predicate = dialect_options['postgresql_where']
    return ''.join(str(predicate).replace('::text', '').split()).replace(
        '(', '').replace(')', '')


def _drop_api010_request_identity(values: dict[str, object]) -> None:
    """Remove fields that do not exist before API request schema 010."""
    for field in ('execution_process_start_time_ticks', 'managed_job_id',
                  'managed_job_controller_instance_id',
                  'managed_job_controller_generation',
                  'managed_job_controller_slot_id',
                  'managed_job_controller_slot_attempt'):
        values.pop(field, None)


def test_api005_upgrade_preserves_ordinary_api004_rows(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '004',
                                         mode='upgrade')

    request = _request('pre-api005')
    request_values = request_postgres._request_values_for_db(request)
    request_values.pop('execution_quiescence_required')
    request_values.pop('execution_quiesced_generation')
    request_values.pop('execution_quiesced_at')
    request_values.pop('terminal_cause', None)
    request_values.pop('ordinary_launch_association_id', None)
    _drop_api010_request_identity(request_values)
    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                request_postgres.REQUESTS).values(**request_values))
        connection.execute(
            sqlalchemy.insert(request_postgres.QUEUE).values(
                **request_postgres._queue_values(request)))

    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '005',
                                         mode='upgrade')
    with postgres_engine.connect() as connection:
        stored = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.status,
                request_postgres.REQUESTS.c.resource_action_id,
                request_postgres.REQUESTS.c.resource_action_attempt).where(
                    request_postgres.REQUESTS.c.request_id ==
                    request.request_id)).mappings().one()
        queue_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.QUEUE).where(
                                 request_postgres.QUEUE.c.request_id ==
                                 request.request_id)).scalar_one()
    assert stored['status'] == requests.RequestStatus.PENDING.value
    assert stored['resource_action_id'] is None
    assert stored['resource_action_attempt'] is None
    assert queue_count == 1


def test_api006_upgrade_requires_empty_action_attempts(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '005',
                                         mode='upgrade')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '006',
                                         mode='upgrade')
    inspector = sqlalchemy.inspect(postgres_engine)
    attempt_columns = {
        column['name']
        for column in inspector.get_columns('api_resource_action_attempts')
    }
    assert {
        'provider_io_boundary', 'provider_progress', 'provider_progress_sha256',
        'provider_progress_revision'
    }.issubset(attempt_columns)

    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '005',
                                         mode='upgrade')
    action_id = uuid.uuid4()
    request_id = str(uuid.uuid4())
    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.RESOURCE_ACTIONS).values(
                **_action_values(action_id)))
        connection.execute(
            sqlalchemy.insert(request_postgres.RESOURCE_ACTION_ATTEMPTS).values(
                **_attempt_values(action_id, 1, request_id)))
    with pytest.raises(RuntimeError, match='cannot reconstruct'):
        migration_utils.safe_alembic_upgrade(
            postgres_engine,
            migration_utils.API_REQUESTS_DB_NAME,
            '006',
            mode='upgrade')
    assert migration_utils.get_current_alembic_revision(
        postgres_engine, migration_utils.API_REQUESTS_DB_NAME) == '005'


def test_api007_upgrade_preserves_instances_and_widens_only_role_check(
        postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '006',
                                         mode='upgrade')
    ordinary_id = uuid.uuid4()
    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                instance_id=ordinary_id,
                role='executor',
                version='pre-api007',
                ready=False,
                health_detail={},
                supported_handlers=[],
                supported_payload_versions={}))

    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '007',
                                         mode='upgrade')
    authority_id = uuid.uuid4()
    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                instance_id=authority_id,
                role='authority-worker',
                version='api007',
                ready=False,
                health_detail={},
                supported_handlers=[],
                supported_payload_versions={}))
        roles = connection.execute(
            sqlalchemy.select(request_postgres.SERVER_INSTANCES.c.role).where(
                request_postgres.SERVER_INSTANCES.c.instance_id.in_(
                    (ordinary_id, authority_id))).
            order_by(request_postgres.SERVER_INSTANCES.c.role)).scalars().all()
    assert roles == ['authority-worker', 'executor']

    checks = {
        check['name']: ''.join(check['sqltext'].split()).replace('::text', '')
        for check in sqlalchemy.inspect(postgres_engine).get_check_constraints(
            'api_server_instances')
    }
    role_check = checks['ck_api_server_instances_role']
    for role in ('all', 'api', 'executor', 'controller', 'authority-worker'):
        assert f"'{role}'" in role_check
    assert role_check.count("'") == 10


def test_api009_upgrade_preserves_rows_as_legacy_unproven(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '007',
                                         mode='upgrade')
    request = _request('pre-api008')
    request_values = request_postgres._request_values_for_db(request)
    request_values.pop('execution_quiescence_required')
    request_values.pop('execution_quiesced_generation')
    request_values.pop('execution_quiesced_at')
    request_values.pop('terminal_cause', None)
    request_values.pop('ordinary_launch_association_id', None)
    _drop_api010_request_identity(request_values)
    legacy_instance_id = uuid.uuid4()
    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                request_postgres.REQUESTS).values(**request_values))
        connection.execute(
            sqlalchemy.insert(request_postgres.QUEUE).values(
                **request_postgres._queue_values(request)))
        connection.execute(
            sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                instance_id=legacy_instance_id,
                role='api',
                version='api007',
                ready=True,
                health_detail={},
                supported_handlers=[],
                supported_payload_versions={}))

    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '009',
                                         mode='upgrade')

    with postgres_engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.execution_quiescence_required,
                request_postgres.REQUESTS.c.execution_quiesced_generation,
                request_postgres.REQUESTS.c.execution_quiesced_at,
                request_postgres.REQUESTS.c.ordinary_launch_association_id,
            ).where(request_postgres.REQUESTS.c.request_id ==
                    request.request_id)).mappings().one()
        legacy_instance = connection.execute(
            sqlalchemy.select(
                request_postgres.SERVER_INSTANCES.c.request_storage_backend,
                request_postgres.SERVER_INSTANCES.c.request_queue_backend,
                request_postgres.SERVER_INSTANCES.c.
                execution_quiescence_capable,
                request_postgres.SERVER_INSTANCES.c.
                ordinary_launch_binding_capable,
            ).where(request_postgres.SERVER_INSTANCES.c.instance_id ==
                    legacy_instance_id)).mappings().one()
    assert row['execution_quiescence_required'] is False
    assert row['execution_quiesced_generation'] is None
    assert row['execution_quiesced_at'] is None
    assert row['ordinary_launch_association_id'] is None
    assert legacy_instance['request_storage_backend'] == 'unknown'
    assert legacy_instance['request_queue_backend'] == 'unknown'
    assert legacy_instance['execution_quiescence_capable'] is False
    assert legacy_instance['ordinary_launch_binding_capable'] is False
    indexes = {
        index['name']: index for index in sqlalchemy.inspect(
            postgres_engine).get_indexes('api_requests')
    }
    quiescence_index = indexes['ix_api_requests_quiescence_cluster_status']
    assert quiescence_index['column_names'] == ['cluster_name', 'status']
    assert _normalized_index_predicate(quiescence_index) == (
        'execution_quiescence_requiredAND'
        'execution_quiesced_generationISDISTINCTFROMexecution_generationOR'
        'execution_quiesced_atISNULL')


def test_api009_migration_is_runtime_module_independent():
    migration_path = (pathlib.Path(__file__).parents[2] / 'sky' / 'schemas' /
                      'db' / 'api_requests' / '009_ordinary_launch_binding.py')
    tree = ast.parse(migration_path.read_text(encoding='utf-8'))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.append(node.module)
    assert all(
        name != 'sky' and not name.startswith('sky.') for name in imports)


def test_api010_upgrade_preserves_legacy_process_identity_as_unknown(
        postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '009',
                                         mode='upgrade')
    request = _request('pre-api010', should_enqueue=False)
    request_values = request_postgres._request_values_for_db(request)
    _drop_api010_request_identity(request_values)
    request_values.update(status=requests.RequestStatus.RUNNING.value, pid=1234)
    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                request_postgres.REQUESTS).values(**request_values))

    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '010',
                                         mode='upgrade')

    with postgres_engine.connect() as connection:
        stored = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.execution_process_start_time_ticks,
                request_postgres.REQUESTS.c.managed_job_id,
                request_postgres.REQUESTS.c.managed_job_controller_instance_id,
                request_postgres.REQUESTS.c.managed_job_controller_generation,
                request_postgres.REQUESTS.c.managed_job_controller_slot_id,
                request_postgres.REQUESTS.c.managed_job_controller_slot_attempt,
            ).where(request_postgres.REQUESTS.c.request_id ==
                    request.request_id)).mappings().one()
    assert stored['execution_process_start_time_ticks'] is None
    assert stored['managed_job_id'] is None
    assert stored['managed_job_controller_instance_id'] is None
    assert stored['managed_job_controller_generation'] is None
    assert stored['managed_job_controller_slot_id'] is None
    assert stored['managed_job_controller_slot_attempt'] is None
    checks = {
        check['name']: ''.join(check['sqltext'].split())
        for check in sqlalchemy.inspect(postgres_engine).get_check_constraints(
            'api_requests')
    }
    assert 'ck_api_requests_pid' in checks
    assert 'ck_api_requests_process_start_time' in checks
    assert 'ck_api_requests_managed_job_origin_complete' in checks
    assert 'ck_api_requests_managed_job_origin_values' in checks

    indexes = {
        index['name']: index for index in sqlalchemy.inspect(
            postgres_engine).get_indexes('api_requests')
    }
    assert indexes['ix_api_requests_managed_job_attempt']['column_names'] == [
        'managed_job_id', 'managed_job_controller_instance_id',
        'managed_job_controller_generation', 'managed_job_controller_slot_id',
        'managed_job_controller_slot_attempt'
    ]
    assert _normalized_index_predicate(
        indexes['ix_api_requests_managed_job_attempt']) == (
            'managed_job_idISNOTNULL')

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with postgres_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id == request.request_id
                ).values(execution_process_start_time_ticks=0))


def test_api010_migration_is_runtime_module_independent():
    migration_path = (pathlib.Path(__file__).parents[2] / 'sky' / 'schemas' /
                      'db' / 'api_requests' / '010_process_birth_identity.py')
    tree = ast.parse(migration_path.read_text(encoding='utf-8'))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.append(node.module)
    assert all(
        name != 'sky' and not name.startswith('sky.') for name in imports)


def test_api009_upgrades_without_serve_schema_or_migration_order(
        postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '008',
                                         mode='upgrade')
    inspector = sqlalchemy.inspect(postgres_engine)
    assert 'serve_ordinary_launch_associations' not in inspector.get_table_names(
    )
    assert 'api_request_retention_pins' not in inspector.get_table_names()

    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '009',
                                         mode='upgrade')

    assert migration_utils.get_current_alembic_revision(
        postgres_engine, migration_utils.API_REQUESTS_DB_NAME) == '009'
    tables = sqlalchemy.inspect(postgres_engine).get_table_names()
    assert 'api_request_retention_pins' in tables
    assert 'serve_ordinary_launch_associations' not in tables


def test_api009_binding_catalog_matches_literal_contract(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '009',
                                         mode='upgrade')
    inspector = sqlalchemy.inspect(postgres_engine)
    assert [
        column['name']
        for column in inspector.get_columns('api_request_retention_pins')
    ] == ['pin_kind', 'pin_id', 'request_id', 'created_at']
    primary_key = inspector.get_pk_constraint('api_request_retention_pins')
    assert primary_key['constrained_columns'] == ['pin_kind', 'pin_id']
    assert primary_key['name'] == 'pk_api_request_retention_pins'
    foreign_keys = inspector.get_foreign_keys('api_request_retention_pins')
    assert len(foreign_keys) == 1
    foreign_key = foreign_keys[0]
    assert foreign_key['name'] == 'fk_api_request_retention_pins_request'
    assert foreign_key['constrained_columns'] == ['request_id']
    assert foreign_key['referred_table'] == 'api_requests'
    assert foreign_key['referred_columns'] == ['request_id']
    assert foreign_key['options'].get('ondelete') == 'RESTRICT'
    pin_checks = {
        check['name']:
            ''.join(check['sqltext'].split()).replace('(', '').replace(')', '')
        for check in inspector.get_check_constraints(
            'api_request_retention_pins')
    }
    pin_kind_check = pin_checks['ck_api_request_retention_pins_kind']
    assert pin_kind_check in (
        'char_lengthpin_kindBETWEEN1AND128',
        'char_lengthpin_kind>=1ANDchar_lengthpin_kind<=128',
    )
    pin_indexes = {
        index['name']: index
        for index in inspector.get_indexes('api_request_retention_pins')
    }
    assert pin_indexes['ix_api_request_retention_pins_request'][
        'column_names'] == ['request_id']

    request_checks = {
        check['name']: ''.join(check['sqltext'].replace(
            '::text', '').split()).replace('(', '').replace(
                ')',
                '') for check in inspector.get_check_constraints('api_requests')
    }
    assert request_checks['ck_api_requests_ordinary_launch_handler'] == (
        "ordinary_launch_association_idISNULL=handler_name<>"
        "'sky.server.requests.ordinary_launch:launch'")
    binding_index = {
        index['name']: index for index in inspector.get_indexes('api_requests')
    }['uq_api_requests_ordinary_launch_association']
    assert binding_index['unique']
    assert binding_index['column_names'] == ['ordinary_launch_association_id']
    assert _normalized_index_predicate(binding_index) == (
        'ordinary_launch_association_idISNOTNULL')

    assert list(request_postgres.REQUEST_RETENTION_PINS.c.keys()) == [
        'pin_kind', 'pin_id', 'request_id', 'created_at'
    ]
    runtime_foreign_key = next(
        iter(request_postgres.REQUEST_RETENTION_PINS.foreign_keys))
    assert runtime_foreign_key.target_fullname == 'api_requests.request_id'
    assert runtime_foreign_key.ondelete == 'RESTRICT'


def test_api006_upgrade_serializes_with_uncommitted_api005_insert(
        postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         '005',
                                         mode='upgrade')
    action_id = uuid.uuid4()
    request_id = str(uuid.uuid4())
    writer = postgres_engine.connect()
    writer_transaction = writer.begin()
    writer.execute(
        sqlalchemy.insert(request_postgres.RESOURCE_ACTIONS).values(
            **_action_values(action_id)))
    writer.execute(
        sqlalchemy.insert(request_postgres.RESOURCE_ACTION_ATTEMPTS).values(
            **_attempt_values(action_id, 1, request_id)))

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(migration_utils.safe_alembic_upgrade,
                             postgres_engine,
                             migration_utils.API_REQUESTS_DB_NAME,
                             '006',
                             mode='upgrade')
    try:
        deadline = time.monotonic() + 10
        while True:
            with postgres_engine.connect() as observer:
                lock_waiting = observer.execute(
                    sqlalchemy.text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE datname = current_database() "
                        "AND state = 'active' "
                        "AND wait_event_type = 'Lock' "
                        "AND query LIKE "
                        "'LOCK TABLE api_resource_action_attempts%')")
                ).scalar_one()
            if lock_waiting:
                break
            if future.done():
                future.result()
                pytest.fail('API006 migration did not wait for the API005 '
                            'attempt insert.')
            if time.monotonic() >= deadline:
                pytest.fail('API006 migration did not reach its table lock.')
            time.sleep(0.01)
        assert not future.done()
        writer_transaction.commit()
        with pytest.raises(RuntimeError, match='cannot reconstruct'):
            future.result(timeout=10)
    finally:
        if writer_transaction.is_active:
            writer_transaction.rollback()
        writer.close()
        executor.shutdown(wait=True, cancel_futures=True)
    assert migration_utils.get_current_alembic_revision(
        postgres_engine, migration_utils.API_REQUESTS_DB_NAME) == '005'


def test_ordinary_request_lifecycle_does_not_create_actions(request_database):
    engine, backend = request_database
    request_id = str(uuid.uuid4())
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))

    with engine.connect() as connection:
        stored = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id)).mappings().one()
        action_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(request_postgres.RESOURCE_ACTIONS)).scalar_one()
        attempt_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(
                request_postgres.RESOURCE_ACTION_ATTEMPTS)).scalar_one()
    assert stored['resource_action_id'] is None
    assert stored['resource_action_attempt'] is None
    assert action_count == 0
    assert attempt_count == 0

    item = _claim(backend, request_id)
    assert item.claim_token is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token)
    context = storage.activate_execution_claim(claim.request_id,
                                               claim.execution_generation,
                                               claim.claim_token)
    try:
        backend.set_request_finished(request_id,
                                     requests.RequestStatus.SUCCEEDED,
                                     result=[])
    finally:
        storage.deactivate_execution_claim(context)
    assert backend.acknowledge_execution_quiescence(claim)
    asyncio.run(backend.delete_requests([request_id]))

    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(request_postgres.RESOURCE_ACTIONS)).scalar_one() == 0
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(
                request_postgres.RESOURCE_ACTION_ATTEMPTS)).scalar_one() == 0


def test_retention_waits_for_required_execution_quiescence(request_database):
    engine, backend = request_database
    request_id = 'retention-unproven'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    assert item.claim_token is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token)
    context = storage.activate_execution_claim(claim.request_id,
                                               claim.execution_generation,
                                               claim.claim_token)
    try:
        assert backend.set_request_finished(request_id,
                                            requests.RequestStatus.SUCCEEDED,
                                            result=[])
    finally:
        storage.deactivate_execution_claim(context)

    asyncio.run(backend.delete_requests([request_id]))
    assert backend.get_request(request_id) is not None

    assert backend.acknowledge_execution_quiescence(claim)
    asyncio.run(backend.delete_requests([request_id]))
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.REQUESTS).where(
                                 request_postgres.REQUESTS.c.request_id ==
                                 request_id)).scalar_one() == 0


def test_retention_allows_pre_api008_unrequired_terminal_row(request_database):
    engine, backend = request_database
    request_id = 'retention-legacy'
    request = _request(request_id, should_enqueue=False)
    assert asyncio.run(backend.create_if_not_exists_async(request))
    assert backend.set_request_finished(request_id,
                                        requests.RequestStatus.SUCCEEDED,
                                        result=[])
    restored = backend.get_request(request_id)
    assert not restored.execution_quiescence_required

    asyncio.run(backend.delete_requests([request_id]))

    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.REQUESTS).where(
                                 request_postgres.REQUESTS.c.request_id ==
                                 request_id)).scalar_one() == 0


def test_bound_request_handler_correlation_and_pin_constraints(
        request_database):
    engine, _ = request_database
    association_id = uuid.uuid4()
    bound = _bound_request('bound-catalog')
    with engine.begin() as connection:
        assert request_postgres.insert_bound_request_and_queue_in_transaction(
            connection, bound, ordinary_launch_association_id=association_id)
    with engine.connect() as connection:
        stored = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.handler_name,
                request_postgres.REQUESTS.c.ordinary_launch_association_id).
            where(request_postgres.REQUESTS.c.request_id ==
                  bound.request_id)).one()
        pin = connection.execute(
            sqlalchemy.select(request_postgres.REQUEST_RETENTION_PINS).where(
                request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                bound.request_id)).mappings().one()
    assert stored.handler_name == (
        ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME)
    assert stored.ordinary_launch_association_id == association_id
    assert pin['pin_kind'] == (
        request_postgres.ORDINARY_LAUNCH_RETENTION_PIN_KIND)
    assert pin['pin_id'] == association_id

    legacy_values = request_postgres._request_values_for_db(
        _request('legacy-with-correlation', should_enqueue=False))
    legacy_values['ordinary_launch_association_id'] = uuid.uuid4()
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(
                    request_postgres.REQUESTS).values(**legacy_values))

    uncorrelated_bound_values = request_postgres._request_values_for_db(
        _bound_request('bound-without-correlation'))
    uncorrelated_bound_values['ordinary_launch_association_id'] = None
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(request_postgres.REQUESTS).values(
                    **uncorrelated_bound_values))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.delete(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id == bound.request_id))


def test_generic_binding_atomically_commits_exact_profile_request_queue_and_pin(
        bound_request_database, monkeypatch) -> None:
    engine, _ = bound_request_database
    info = replica_managers.ReplicaInfo.from_storage_dict(_gc_replica_state())
    info.paid_capacity_pool_key = 'paid-pool-a'
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='READY'))
        assert ordinary_launch_binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='gc-service',
            controller_incarnation=_GC_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True) == 6
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.paid_capacity_pools_table).values(
                    pool_key='paid-pool-a',
                    current_limit=1,
                    successes_since_resize=0,
                    updated_at=time.time()))
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING',
                    paid_capacity_pool_key='paid-pool-a',
                    replica_state=info.to_storage_dict()))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.paid_capacity_claims_table).values(
                    service_name='gc-service',
                    service_hash='gc-service-hash',
                    replica_id=3,
                    pool_key='paid-pool-a',
                    priority=1,
                    claimed_at=time.time()))

    profile = ordinary_launch_binding.resolve_non_pool_launch_profile(
        'gc-service', 3, _GC_REPLICA_RECORD_ID)
    submission_id = uuid.UUID('44444444-4444-4444-8444-444444444444')
    submitted_body = _gc_unbound_non_pool_launch_body()
    legacy_digest = ordinary_launch_binding.canonical_launch_digest(
        submitted_body)
    identity = _gc_non_pool_binding_identity(profile,
                                             submission_id=submission_id,
                                             input_digest=legacy_digest)
    auth_user = models.User(id='tenant-a', name='Tenant A')
    # Reproduce the ordering used by the original HTTP writer: derive the
    # immutable identity from submitted bytes, then normalize the executable
    # request body in the request builder.
    request = executor._build_request(
        request_id=identity.request_id,
        request_name=request_names.RequestName.CLUSTER_LAUNCH,
        request_body=submitted_body.model_copy(deep=True),
        func=non_pool_launch_request.launch,
        request_cluster_name=submitted_body.cluster_name,
        schedule_type=requests.ScheduleType.LONG,
        auth_user=auth_user,
        retryable=False,
        should_enqueue=True,
        precondition=preconditions.OrdinaryLaunchBindingPrecondition(
            identity.request_id, str(identity.association_id)),
        client_api_version=77)
    monkeypatch.setattr(request_postgres,
                        '_resolved_request_backend_capability', lambda:
                        ('postgres-storage', 'postgres-queue', True))
    monkeypatch.setattr(request_postgres,
                        'non_pool_launch_binding_fleet_capable',
                        lambda **_kwargs: True)

    first = request_postgres.bind_and_enqueue_non_pool_launch(request, identity)
    retry_authority = non_pool_admission.AdmissionAuthority(
        tenant_id='tenant-a',
        creator_name='Tenant A',
        service_workspace='workspace-a',
        capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))
    retry = non_pool_admission.build(_gc_unbound_non_pool_launch_body(),
                                     submission_id,
                                     profile,
                                     retry_authority,
                                     auth_user=auth_user,
                                     client_api_version=77)
    assert retry.identity == identity
    assert ordinary_launch_binding.canonical_launch_digest(
        retry.request.request_body) != retry.identity.input_digest
    monkeypatch.setattr(
        ordinary_launch_binding,
        'resolve_non_pool_launch_profile_in_connection',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError('exact retry must not re-resolve planner state')))
    second = request_postgres.bind_and_enqueue_non_pool_launch(
        retry.request, retry.identity)

    changed_body = _gc_unbound_non_pool_launch_body()
    changed_body.task += 'run: echo changed\n'
    changed = non_pool_admission.build(changed_body,
                                       submission_id,
                                       profile,
                                       retry_authority,
                                       auth_user=auth_user,
                                       client_api_version=77)
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict):
        request_postgres.bind_and_enqueue_non_pool_launch(
            changed.request, changed.identity)

    for creator_name, api_version in (('Renamed Tenant A', 77), ('Tenant A',
                                                                 78)):
        normalized_drift = non_pool_admission.build(
            _gc_unbound_non_pool_launch_body(),
            submission_id,
            profile,
            dataclasses.replace(retry_authority, creator_name=creator_name),
            auth_user=models.User(id='tenant-a', name=creator_name),
            client_api_version=api_version)
        assert normalized_drift.identity == identity
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                match='exact binding intent'):
            request_postgres.bind_and_enqueue_non_pool_launch(
                normalized_drift.request, normalized_drift.identity)

    assert first.created
    assert not second.created
    assert first.association_id == second.association_id
    with engine.connect() as connection:
        stored = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id)).mappings().one()
        queue_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.QUEUE).where(
                                 request_postgres.QUEUE.c.request_id ==
                                 identity.request_id)).scalar_one()
        pin = connection.execute(
            sqlalchemy.select(request_postgres.REQUEST_RETENTION_PINS).where(
                request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                identity.request_id)).mappings().one()
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == identity.association_id)).mappings().one()

    assert stored['handler_name'] == (
        non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME)
    assert stored['binding_protocol_version'] == 2
    assert stored['profile_kind'] == profile.kind.value
    assert stored['profile_digest'] == profile.digest
    assert queue_count == 1
    assert pin['pin_id'] == identity.association_id
    assert association['profile_digest'] == profile.digest
    assert association['authorization_digest'] == profile.authorization_digest

    context = ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=identity.association_id,
        request_id=identity.request_id,
        service_name=identity.service_name,
        replica_id=identity.replica_id,
        replica_record_id=identity.replica_record_id,
        launch_generation=first.launch_generation,
        input_digest=identity.input_digest,
        profile=identity.profile,
        capability_cohort_epoch=identity.capability_cohort_epoch,
        capability_profile_set_digest=(identity.capability_profile_set_digest),
        receipt_protocol_version=identity.receipt_protocol_version)
    authority = dataclasses.replace(
        _gc_binding_authority(),
        binding_epoch=6,
        non_pool_capable=True,
        non_pool_binding_protocol_version=(
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
        non_pool_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        non_pool_capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        non_pool_receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))
    with engine.begin() as connection:
        assert ordinary_launch_binding.mark_ambiguous_in_connection(
            connection, context, 'provider-result-uncertain')
    assert not request_postgres.bound_non_pool_provider_reconciliation_ready(
        context, authority)
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='exact request quiescence'):
        request_postgres.record_bound_non_pool_provider_evidence(
            context, authority, ordinary_launch_binding.ProviderEvidence.ABSENT,
            {'result': 'ABSENT'})

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == identity.request_id))
        now = sqlalchemy.func.clock_timestamp()
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == identity.request_id).
            values(
                status=requests.RequestStatus.CANCELLED.value,
                terminal_cause=(
                    event_api_models.EventCause.DISPATCHER_SUBMIT_FAILED.value),
                execution_quiescence_required=True,
                execution_quiesced_generation=0,
                execution_quiesced_at=now,
                finished_at=now,
                updated_at=now))
    assert request_postgres.bound_non_pool_provider_reconciliation_ready(
        context, authority)
    assert request_postgres.record_bound_non_pool_provider_evidence(
        context, authority, ordinary_launch_binding.ProviderEvidence.ABSENT,
        {'result': 'ABSENT'})
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='launch profile or phase'):
        request_postgres.project_bound_non_pool_provider_absence(
            context,
            authority,
            project_replica_result=lambda *_args: pytest.fail(
                'paid profiles must fail before replica projection'))


@pytest.mark.parametrize('effect_phase', [
    ordinary_launch_binding.EffectPhase.NOT_STARTED,
    ordinary_launch_binding.EffectPhase.PROVIDER_IO
])
def test_reserved_fill_provider_absence_projects_replica_and_pin_atomically(
        bound_request_database, monkeypatch, effect_phase) -> None:
    engine, _ = bound_request_database
    info = _gc_reserved_fill_info()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='READY'))
        ordinary_launch_binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='gc-service',
            controller_incarnation=_GC_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True)
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING',
                    paid_capacity_pool_key=None,
                    replica_state=info.to_storage_dict()))
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference='reserved-fill:' + 'e' * 64,
        authorization_generation=8,
        authorization_payload={'physical_cluster_uid': 'physical-uid-a'})
    monkeypatch.setattr(ordinary_launch_binding,
                        'resolve_non_pool_launch_profile_in_connection',
                        lambda *_args, **_kwargs: profile)
    identity = _gc_non_pool_binding_identity(profile)
    request = _bound_non_pool_request(identity.request_id)
    monkeypatch.setattr(request_postgres,
                        '_resolved_request_backend_capability', lambda:
                        ('postgres-storage', 'postgres-queue', True))
    monkeypatch.setattr(request_postgres,
                        'non_pool_launch_binding_fleet_capable',
                        lambda **_kwargs: True)
    admission = request_postgres.bind_and_enqueue_non_pool_launch(
        request, identity)
    context = ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=identity.association_id,
        request_id=identity.request_id,
        service_name=identity.service_name,
        replica_id=identity.replica_id,
        replica_record_id=identity.replica_record_id,
        launch_generation=admission.launch_generation,
        input_digest=identity.input_digest,
        profile=profile,
        capability_cohort_epoch=identity.capability_cohort_epoch,
        capability_profile_set_digest=identity.capability_profile_set_digest,
        receipt_protocol_version=identity.receipt_protocol_version)
    authority = dataclasses.replace(
        _gc_binding_authority(),
        binding_epoch=6,
        non_pool_capable=True,
        non_pool_binding_protocol_version=(
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
        non_pool_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        non_pool_capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        non_pool_receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == identity.association_id).values(
                      effect_phase=effect_phase.value,
                      effect_phase_changed_at=sqlalchemy.func.clock_timestamp(),
                      owner_revision=2,
                      updated_at=sqlalchemy.func.clock_timestamp()))
        assert ordinary_launch_binding.mark_ambiguous_in_connection(
            connection, context, 'provider-result-uncertain')
        connection.execute(
            sqlalchemy.delete(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == identity.request_id))
        now = sqlalchemy.func.clock_timestamp()
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id).values(
                    status=requests.RequestStatus.CANCELLED.value,
                    terminal_cause=event_api_models.EventCause.
                    EXECUTION_LEASE_EXPIRED.value,
                    execution_quiescence_required=True,
                    execution_quiesced_generation=0,
                    execution_quiesced_at=now,
                    finished_at=now,
                    updated_at=now))
    provider_payload = {
        'association_id': str(identity.association_id),
        'cluster_name': 'gc-service-3',
        'kubernetes_context': 'context-a',
        'physical_cluster_uid': 'physical-uid-a',
        'probe_contract': 'kubernetes-physical-replica-presence-v1',
        'profile_kind': ordinary_launch_binding.NonPoolLaunchProfileKind.
                        RESERVED_FILL.value,
        'replica_record_id': str(_GC_REPLICA_RECORD_ID),
        'result': 'ABSENT',
    }
    assert request_postgres.record_bound_non_pool_provider_evidence(
        context, authority, ordinary_launch_binding.ProviderEvidence.ABSENT,
        provider_payload)
    assert request_postgres.bound_non_pool_provider_absence_is_recorded(
        context, authority)

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='replica projection'):
        request_postgres.project_bound_non_pool_provider_absence(
            context, authority, project_replica_result=lambda *_args: False)

    def _project_replica(connection, projection):
        assert projection.provider_evidence == (
            ordinary_launch_binding.ProviderEvidence.ABSENT)
        projected_info = projection.locked_replica_info
        projected_info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        return serve_state.update_replica_for_bound_ordinary_launch_in_transaction(
            connection,
            'gc-service',
            'gc-service-hash',
            3,
            str(_GC_REPLICA_RECORD_ID),
            identity.association_id,
            projected_info,
            provider_launch_succeeded=False,
            paid_capacity_pool_key=None,
            paid_capacity_outcome=None)

    assert request_postgres.project_bound_non_pool_provider_absence(
        context, authority, project_replica_result=_project_replica)
    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == identity.association_id)).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service', serve_state_schema.replicas_table.c.replica_id ==
                3)).mappings().one()
        pin_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(request_postgres.REQUEST_RETENTION_PINS).where(
                request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                identity.request_id)).scalar_one()
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.PROJECTED.value)
    assert association['reconciliation_outcome'] == (
        ordinary_launch_binding.ReconciliationOutcome.PROJECTED.value)
    assert association['ambiguity_code'] is None
    assert association[
        'terminal_status'] == requests.RequestStatus.CANCELLED.value
    assert association['pin_released_at'] is not None
    assert replica['ordinary_launch_association_id'] is None
    assert replica['status'] == 'FAILED_CLEANUP'
    assert pin_count == 0


def test_provider_present_cleanup_requires_exact_digest_and_owner_relations(
        bound_request_database) -> None:
    """Atomic digest equality cannot replace tenant/owner/cluster checks."""
    engine, _ = bound_request_database
    body = _gc_unbound_non_pool_launch_body()
    body.env_vars[constants.USER_ID_ENV_VAR] = 'tenant-a'
    body.env_vars[constants.USER_ENV_VAR] = 'Tenant A'
    body.client_api_version = 77
    request = requests.Request(request_id='atomic-cleanup-request',
                               name='sky.launch',
                               entrypoint=non_pool_launch_request.launch,
                               request_body=body,
                               status=requests.RequestStatus.CANCELLED,
                               created_at=time.time(),
                               user_id='tenant-a',
                               cluster_name='gc-service-3',
                               schedule_type=requests.ScheduleType.LONG,
                               retryable=False,
                               should_enqueue=False)
    association = {
        'tenant_scope': 'tenant-a',
        'cluster_name': 'gc-service-3',
    }
    request_row = {
        'user_id': 'tenant-a',
        'cluster_name': 'gc-service-3',
    }
    context = mock.Mock(
        service_name='gc-service',
        input_digest=ordinary_launch_binding.canonical_launch_digest(body))
    pre_normalization_body = body.model_copy(deep=True)
    pre_normalization_body.client_api_version = None
    pre_normalization_digest = (
        ordinary_launch_binding.canonical_launch_digest(pre_normalization_body))
    original = body.model_dump_json()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(global_user_state_schema.user_table).values(
                id='tenant-a', name='Tenant A', created_at=int(time.time())))
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'gc-service').values(owner_user_id='tenant-a',
                                     owner_user_name='Tenant A'))
        assert request_postgres._provider_present_cleanup_input_digest_matches(
            connection, association, request_row, request, context)

        changed_name = body.model_copy(deep=True)
        changed_name.env_vars[constants.USER_ENV_VAR] = 'Different Name'
        changed_request = dataclasses.replace(request,
                                              request_body=changed_name)
        changed_context = mock.Mock(
            service_name='gc-service',
            input_digest=ordinary_launch_binding.canonical_launch_digest(
                changed_name))
        assert not request_postgres._provider_present_cleanup_input_digest_matches(
            connection, association, request_row, changed_request,
            changed_context)
        assert not request_postgres._provider_present_cleanup_input_digest_matches(
            connection, {
                **association, 'tenant_scope': 'different-tenant'
            }, request_row, request, context)
        legacy_context = mock.Mock(service_name='gc-service',
                                   input_digest=pre_normalization_digest)
        assert not request_postgres._provider_present_cleanup_input_digest_matches(
            connection, association, request_row, request, legacy_context)
    assert body.model_dump_json() == original


def test_reserved_fill_provider_presence_authorizes_only_fenced_cleanup(
        bound_request_database, monkeypatch) -> None:
    """PRESENT retains authority until a later exact ABSENT projection."""
    engine, _ = bound_request_database
    info = _gc_reserved_fill_info()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='READY'))
        ordinary_launch_binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='gc-service',
            controller_incarnation=_GC_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True)
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    status='PROVISIONING',
                    paid_capacity_pool_key=None,
                    replica_state=info.to_storage_dict()))
        connection.execute(
            sqlalchemy.insert(global_user_state_schema.user_table).values(
                id='tenant-a', name='Tenant A', created_at=int(time.time())))
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'gc-service').values(
                    owner_user_id='tenant-a',
                    owner_user_name='Tenant A',
                    reserved_fill_actuation_mode=(
                        zero_cost_actuation.ActuationMode.DURABLE_INTENT.value),
                    reserved_fill_actuation_epoch=1,
                    reserved_fill_actuation_capable=True,
                    reserved_fill_actuation_controller_incarnation=(
                        _GC_CONTROLLER_ID),
                    reserved_fill_actuation_protocol_version=1))

    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference='reserved-fill:' + 'e' * 64,
        authorization_generation=8,
        authorization_payload={'physical_cluster_uid': 'physical-uid-a'})
    monkeypatch.setattr(ordinary_launch_binding,
                        'resolve_non_pool_launch_profile_in_connection',
                        lambda *_args, **_kwargs: profile)
    monkeypatch.setattr(
        ordinary_launch_binding, '_reserved_fill_cleanup_payload',
        lambda *_args, **_kwargs: {'physical_cluster_uid': 'physical-uid-a'})
    launch_body = _gc_unbound_non_pool_launch_body()
    # Atomic admission stamps trusted fields before the shared builder hashes
    # the body, so the submitted and executable digests are identical.
    launch_body.env_vars[constants.USER_ID_ENV_VAR] = 'tenant-a'
    launch_body.env_vars[constants.USER_ENV_VAR] = 'Tenant A'
    launch_body.client_api_version = 77
    built = non_pool_admission.build(
        launch_body,
        uuid.UUID('44444444-4444-4444-8444-444444444444'),
        profile,
        non_pool_admission.AdmissionAuthority(
            tenant_id='tenant-a',
            creator_name='Tenant A',
            service_workspace='workspace-a',
            capability_cohort_epoch=(
                ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
            capability_profile_set_digest=(
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
            ),
            receipt_protocol_version=(
                ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)),
        auth_user=models.User(id='tenant-a', name='Tenant A'),
        client_api_version=77)
    identity = built.identity
    request = built.request
    monkeypatch.setattr(request_postgres,
                        '_resolved_request_backend_capability', lambda:
                        ('postgres-storage', 'postgres-queue', True))
    monkeypatch.setattr(request_postgres,
                        'non_pool_launch_binding_fleet_capable',
                        lambda **_kwargs: True)
    admission = request_postgres.bind_and_enqueue_non_pool_launch(
        request, identity)
    assert ordinary_launch_binding.canonical_launch_digest(
        request.request_body) == identity.input_digest
    context = ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=identity.association_id,
        request_id=identity.request_id,
        service_name=identity.service_name,
        replica_id=identity.replica_id,
        replica_record_id=identity.replica_record_id,
        launch_generation=admission.launch_generation,
        input_digest=identity.input_digest,
        profile=profile,
        capability_cohort_epoch=identity.capability_cohort_epoch,
        capability_profile_set_digest=identity.capability_profile_set_digest,
        receipt_protocol_version=identity.receipt_protocol_version)
    authority = dataclasses.replace(
        _gc_binding_authority(),
        binding_epoch=6,
        non_pool_capable=True,
        non_pool_binding_protocol_version=(
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
        non_pool_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        non_pool_capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        non_pool_receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))
    with engine.begin() as connection:
        assert ordinary_launch_binding.mark_ambiguous_in_connection(
            connection, context, 'provider-result-uncertain')
        connection.execute(
            sqlalchemy.delete(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == identity.request_id))
        now = sqlalchemy.func.clock_timestamp()
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id).values(
                    status=requests.RequestStatus.CANCELLED.value,
                    terminal_cause=event_api_models.EventCause.
                    EXECUTION_LEASE_EXPIRED.value,
                    execution_generation=1,
                    execution_quiescence_required=True,
                    execution_quiesced_generation=1,
                    execution_quiesced_at=now,
                    finished_at=now,
                    updated_at=now))

    def _project_replica(connection, projection):
        projected_info = projection.locked_replica_info
        if (projection.provider_evidence ==
                ordinary_launch_binding.ProviderEvidence.PRESENT):
            status = projected_info.status_property
            status.sky_launch_status = common_utils.ProcessStatus.INTERRUPTED
            status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
            status.service_ready_now = False
            status.is_scale_down = True
            status.preempted = False
            status.purged = False
            status.failed_spot_availability = False
            status.drain_cap_seconds = 0
            status.drain_started_at = None
            status.wait_for_idle_before_termination = False
            status.logical_retirement_version = None
            status.logical_retirement_controller_epoch = None
            status.logical_retirement_generation = None
            status.logical_retirement_target_capacity = None
            status.logical_retirement_confirmed_generation = None
            status.logical_retirement_bounded_deadline = False
            status.logical_retirement_committed = False
        return serve_state.update_replica_for_bound_ordinary_launch_in_transaction(
            connection,
            'gc-service',
            'gc-service-hash',
            3,
            str(_GC_REPLICA_RECORD_ID),
            identity.association_id,
            projected_info,
            provider_launch_succeeded=False,
            paid_capacity_pool_key=None,
            paid_capacity_outcome=None)

    def _provider_payload(result: str) -> dict[str, str]:
        return {
            'association_id': str(identity.association_id),
            'cluster_name': 'gc-service-3',
            'kubernetes_context': 'context-a',
            'physical_cluster_uid': 'physical-uid-a',
            'probe_contract': 'kubernetes-physical-replica-presence-v1',
            'profile_kind': ordinary_launch_binding.NonPoolLaunchProfileKind.
                            RESERVED_FILL.value,
            'replica_record_id': str(_GC_REPLICA_RECORD_ID),
            'result': result,
        }

    assert request_postgres.record_bound_non_pool_provider_evidence(
        context, authority, ordinary_launch_binding.ProviderEvidence.PRESENT,
        _provider_payload('PRESENT'))
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='launch profile or phase'):
        request_postgres.authorize_bound_non_pool_provider_present_cleanup(
            context, authority, project_replica_result=_project_replica)

    # PRESENT cannot be attributed before the durable provider-effect
    # boundary. Once that exact phase proof exists, the same evidence can
    # authorize only the fenced cleanup marker.
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == identity.association_id).values(
                      effect_phase=ordinary_launch_binding.EffectPhase.
                      PROVIDER_IO.value,
                      effect_phase_changed_at=sqlalchemy.func.clock_timestamp(),
                      owner_revision=(
                          ordinary_launch_binding.
                          ordinary_launch_associations_table.c.owner_revision +
                          1),
                      updated_at=sqlalchemy.func.clock_timestamp()))
    with engine.connect() as connection:
        original_payload = dict(
            connection.execute(
                sqlalchemy.select(
                    request_postgres.REQUESTS.c.payload_json).where(
                        request_postgres.REQUESTS.c.request_id ==
                        identity.request_id)).scalar_one())
    tampered_payloads = []
    tampered_task = dict(original_payload)
    tampered_task['task'] = str(tampered_task['task']) + '\n# tampered'
    tampered_payloads.append(tampered_task)
    tampered_env = dict(original_payload)
    tampered_env['env_vars'] = dict(tampered_env['env_vars'])
    tampered_env['env_vars']['UNRELATED_TAMPER'] = 'true'
    tampered_payloads.append(tampered_env)
    tampered_owner = dict(original_payload)
    tampered_owner['env_vars'] = dict(tampered_owner['env_vars'])
    tampered_owner['env_vars'][constants.USER_ENV_VAR] = 'Tampered Owner'
    tampered_payloads.append(tampered_owner)
    tampered_tenant = dict(original_payload)
    tampered_tenant['env_vars'] = dict(tampered_tenant['env_vars'])
    tampered_tenant['env_vars'][constants.USER_ID_ENV_VAR] = 'other-tenant'
    tampered_payloads.append(tampered_tenant)
    tampered_cluster = dict(original_payload)
    tampered_cluster['cluster_name'] = 'different-cluster'
    tampered_payloads.append(tampered_cluster)
    tampered_api = dict(original_payload)
    tampered_api['client_api_version'] = 78
    tampered_payloads.append(tampered_api)
    for tampered_payload in tampered_payloads:
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    identity.request_id).values(payload_json=tampered_payload))
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict):
            request_postgres.authorize_bound_non_pool_provider_present_cleanup(
                context, authority, project_replica_result=_project_replica)
    for context_key in (ordinary_launch_binding.INPUT_DIGEST_KEY,
                        ordinary_launch_binding.PROFILE_DIGEST_KEY):
        tampered_payload = dict(original_payload)
        tampered_context = dict(tampered_payload['extra_launch_context'])
        tampered_context[context_key] = 'f' * 64
        tampered_payload['extra_launch_context'] = tampered_context
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    identity.request_id).values(payload_json=tampered_payload))
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict):
            request_postgres.authorize_bound_non_pool_provider_present_cleanup(
                context, authority, project_replica_result=_project_replica)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id).values(payload_json=original_payload))
    # Historical cancellation rows retain the source exception metadata while
    # the encoded object is the client-safe CloudError projection.  That exact
    # serializer shape is valid terminal evidence; arbitrary metadata drift is
    # not.
    wrapped_cancel_error = requests._build_error_dict(
        concurrent.futures.CancelledError())
    assert wrapped_cancel_error['type'] == 'CancelledError'
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id).values(error=wrapped_cancel_error))
    assert request_postgres.authorize_bound_non_pool_provider_present_cleanup(
        context, authority, project_replica_result=_project_replica)
    with engine.connect() as connection:
        assert dict(
            connection.execute(
                sqlalchemy.select(
                    request_postgres.REQUESTS.c.payload_json).where(
                        request_postgres.REQUESTS.c.request_id ==
                        identity.request_id)).scalar_one()) == original_payload
    assert (
        request_postgres.bound_non_pool_provider_present_cleanup_is_authorized(
            context, authority))
    # Exact replay is read-only and remains authorized.
    assert request_postgres.authorize_bound_non_pool_provider_present_cleanup(
        context, authority, project_replica_result=_project_replica)
    tampered_error = dict(wrapped_cancel_error)
    tampered_error['type'] = 'DifferentError'
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id).values(error=tampered_error))
    assert not (
        request_postgres.bound_non_pool_provider_present_cleanup_is_authorized(
            context, authority))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id).values(error=wrapped_cancel_error))
    assert (
        request_postgres.bound_non_pool_provider_present_cleanup_is_authorized(
            context, authority))
    with engine.begin() as connection:
        stored_state = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.replica_state).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    'gc-service', serve_state_schema.replicas_table.c.replica_id
                    == 3)).scalar_one()
        tampered = replica_managers.ReplicaInfo.from_storage_dict(stored_state)
        tampered.status_property.drain_cap_seconds = 30
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    replica_state=tampered.to_storage_dict()))
    assert not (
        request_postgres.bound_non_pool_provider_present_cleanup_is_authorized(
            context, authority))
    with engine.begin() as connection:
        tampered.status_property.drain_cap_seconds = 0
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service',
                serve_state_schema.replicas_table.c.replica_id == 3).values(
                    replica_state=tampered.to_storage_dict()))
    assert (
        request_postgres.bound_non_pool_provider_present_cleanup_is_authorized(
            context, authority))

    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == identity.association_id)).mappings().one()
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                'gc-service', serve_state_schema.replicas_table.c.replica_id ==
                3)).mappings().one()
        pin_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(request_postgres.REQUEST_RETENTION_PINS).where(
                request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                identity.request_id)).scalar_one()
    persisted = replica_managers.ReplicaInfo.from_storage_dict(
        replica['replica_state'])
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.AMBIGUOUS.value)
    assert association['provider_evidence'] == (
        ordinary_launch_binding.ProviderEvidence.PRESENT.value)
    assert replica['ordinary_launch_association_id'] == identity.association_id
    assert persisted.status_property.sky_launch_status == (
        common_utils.ProcessStatus.INTERRUPTED)
    assert persisted.status_property.sky_down_status == (
        common_utils.ProcessStatus.SCHEDULED)
    assert persisted.zero_cost_materialization_sequence is None
    assert pin_count == 1

    assert request_postgres.record_bound_non_pool_provider_evidence(
        context, authority, ordinary_launch_binding.ProviderEvidence.ABSENT,
        _provider_payload('ABSENT'))
    assert request_postgres.project_bound_non_pool_provider_absence(
        context, authority, project_replica_result=_project_replica)
    assert not (
        request_postgres.bound_non_pool_provider_present_cleanup_is_authorized(
            context, authority))
    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == identity.association_id)).mappings().one()
        replica_pointer = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.
                ordinary_launch_association_id).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    'gc-service', serve_state_schema.replicas_table.c.replica_id
                    == 3)).scalar_one()
        pin_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(request_postgres.REQUEST_RETENTION_PINS).where(
                request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                identity.request_id)).scalar_one()
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.PROJECTED.value)
    assert replica_pointer is None
    assert pin_count == 0
    assert request_postgres.bound_non_pool_projected_provider_absence_is_authorized(
        'gc-service', 3, str(_GC_REPLICA_RECORD_ID))

    # Association pin release is part of the removal authorization, not an
    # inference from the cleared replica pointer alone.
    with engine.begin() as connection:
        request_postgres.insert_request_retention_pin_in_transaction(
            connection, identity.request_id,
            request_postgres.ORDINARY_LAUNCH_RETENTION_PIN_KIND,
            identity.association_id)
    assert not request_postgres.bound_non_pool_projected_provider_absence_is_authorized(
        'gc-service', 3, str(_GC_REPLICA_RECORD_ID))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(request_postgres.REQUEST_RETENTION_PINS).where(
                request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                identity.request_id))
    assert request_postgres.bound_non_pool_projected_provider_absence_is_authorized(
        'gc-service', 3, str(_GC_REPLICA_RECORD_ID))

    # Even provider-free row removal remains tied to the exact controller
    # lifecycle that authorized the association. A later epoch must reject the
    # old tombstone rather than grant historical-origin authority.
    assert serve_state.claim_service_lifecycle_epoch('gc-service') == 5
    assert not request_postgres.bound_non_pool_projected_provider_absence_is_authorized(
        'gc-service', 3, str(_GC_REPLICA_RECORD_ID))


def test_legacy_request_evidence_preserves_missing_quiescence_receipt(
        bound_request_database):
    engine, backend = bound_request_database
    request_id = 'legacy-mixed-version-cancelled'
    request = _legacy_serve_launch_request(request_id)
    assert asyncio.run(backend.create_if_not_exists_async(request))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == request_id))
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    status=requests.RequestStatus.CANCELLED.value,
                    terminal_cause=event_api_models.EventCause.EXPLICIT_CANCEL.
                    value,
                    finished_at=None,
                    execution_quiescence_required=False,
                    execution_quiesced_generation=None,
                    execution_quiesced_at=None))

    identity = _gc_legacy_identity(request_id)
    ordinary_launch_binding.create_legacy_reconciliation_scope(
        [identity],
        reviewed_by='operator@example.com',
        review_reason='Mixed-version executor termination review.')
    terminated_at = datetime.datetime.now(datetime.timezone.utc)
    evidence = request_postgres.read_legacy_launch_request_evidence(
        identity,
        executor_terminated_at=terminated_at,
        executor_termination_evidence={
            'pod_uid': 'old-api-pod-uid',
            'termination': 'observed',
        })

    assert evidence.observed_request_status == 'CANCELLED'
    assert evidence.observed_request_execution_generation == 0
    assert not evidence.observed_request_queue_present
    assert not evidence.observed_request_claim_present
    assert evidence.observed_request_evidence[
        'execution_quiescence_required'] is False
    assert evidence.observed_request_evidence[
        'execution_quiesced_generation'] is None
    assert evidence.observed_request_evidence['finished_at'] is None
    assert evidence.executor_terminated_at == terminated_at
    assert evidence.provider_evidence is (
        ordinary_launch_binding.ProviderEvidence.NOT_QUERIED)


def test_generic_kill_requests_skips_correlated_bound_launch(request_database):
    engine, backend = request_database
    request_id = 'bound-generic-cancel-must-skip'
    association_id = uuid.uuid4()
    with engine.begin() as connection:
        assert request_postgres.insert_bound_request_and_queue_in_transaction(
            connection,
            _bound_request(request_id),
            ordinary_launch_association_id=association_id)

    assert backend.kill_requests([request_id]) == []

    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id)).mappings().one()
        queue_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.QUEUE).where(
                                 request_postgres.QUEUE.c.request_id ==
                                 request_id)).scalar_one()
    assert row['status'] == requests.RequestStatus.PENDING.value
    assert row['cancel_requested_at'] is None
    assert row['ordinary_launch_association_id'] == association_id
    assert queue_count == 1


def test_bound_tombstone_gc_rechecks_request_absence_at_delete(
        bound_request_database):
    engine, backend = bound_request_database
    identity, context, _, item = _claim_gc_bound_request(engine, backend)
    association_id = identity.association_id
    request_id = identity.request_id
    now = datetime.datetime.now(datetime.timezone.utc)
    facts = request_postgres.request_bound_ordinary_launch_cancel(
        context, _gc_binding_authority(), 'gc-fixture-settlement')
    assert facts.status is requests.RequestStatus.CANCELLED
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    assert backend.acknowledge_execution_quiescence(claim)
    reduction = request_postgres.reduce_bound_ordinary_launch(
        context,
        _gc_binding_authority(),
        project_replica_result=_keep_replica_projection)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.PRE_EFFECT_TERMINAL)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id))
    with engine.begin() as connection:
        # Production creates this deadline at least 60 days in the future.
        # This GC race needs an already-aged canonical tombstone, so bypass
        # user triggers only for the deadline backdate in this transaction.
        # SET LOCAL restores guarded writes automatically at transaction end.
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            sqlalchemy.update(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == association_id).values(
                      tombstone_not_before=(now -
                                            datetime.timedelta(seconds=1))))

    request_values = request_postgres._request_values_for_db(
        _bound_request(request_id))
    request_values.update({
        'ordinary_launch_association_id': association_id,
        'status': requests.RequestStatus.SUCCEEDED.value,
        'finished_at': now,
        'terminal_cause': event_api_models.EventCause.HANDLER_SUCCEEDED.value,
    })
    inserted_during_delete = False

    def _restore_request_before_delete(_connection, _cursor, statement,
                                       _parameters, _context, _executemany):
        nonlocal inserted_during_delete
        if (inserted_during_delete or not statement.lstrip().startswith(
                'DELETE FROM serve_ordinary_launch_associations')):
            return
        inserted_during_delete = True
        with engine.begin() as contender:
            contender.execute(
                sqlalchemy.insert(
                    request_postgres.REQUESTS).values(**request_values))

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _restore_request_before_delete)
    try:
        with engine.begin() as connection:
            assert request_postgres.gc_bound_ordinary_launch_tombstones_in_transaction(
                connection) == 0
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _restore_request_before_delete)
    assert inserted_during_delete
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == association_id)).scalar_one() == 1

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id))
    with engine.begin() as connection:
        assert request_postgres.gc_bound_ordinary_launch_tombstones_in_transaction(
            connection) == 1


def test_bound_handler_uses_reserved_selector_and_claim_carries_owner(
        request_database):
    engine, backend = request_database
    request_id = 'bound-local-handler-filter'
    association_id = uuid.uuid4()
    with engine.begin() as connection:
        assert request_postgres.insert_bound_request_and_queue_in_transaction(
            connection,
            _bound_request(request_id),
            ordinary_launch_association_id=association_id)

    legacy_handler = registry.registration_for_handler(core.enabled_clouds).name
    legacy_queue = request_postgres.PostgresQueueBackend(
        'short', supported_handler_names=frozenset({legacy_handler}))
    assert legacy_queue.qsize() == 0
    assert legacy_queue.get() is None
    with engine.connect() as connection:
        delivery = connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                request_id)).mappings().one()
    assert delivery['delivery_state'] == 'queued'

    capable_queue = request_postgres.PostgresQueueBackend(
        'short',
        supported_handler_names=frozenset(
            {ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME}))
    # Every kind in the closed provider classification is excluded from the
    # generic claim path.
    assert set(
        request_postgres._PROVIDER_MUTATION_HANDLER_KINDS.values()) == set(
            queue_base.ProviderMutationRequestKind)
    assert capable_queue.get() is None
    candidate = capable_queue.peek_provider_mutation()
    assert candidate == queue_base.ProviderMutationCandidate(
        request_id=request_id,
        kind=(queue_base.ProviderMutationRequestKind.BOUND_ORDINARY_LAUNCH))
    item = capable_queue.claim_provider_mutation(candidate)
    assert item is not None
    assert item.request_id == request_id
    assert item.worker_instance_id == capable_queue._instance_id
    assert item.claim_token is not None
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token,
                                    424242)


def test_bound_provider_candidate_has_one_concurrent_claim_winner(
        request_database):
    engine, _ = request_database
    request_id = 'bound-provider-single-winner'
    with engine.begin() as connection:
        assert request_postgres.insert_bound_request_and_queue_in_transaction(
            connection,
            _bound_request(request_id),
            ordinary_launch_association_id=uuid.uuid4())
    queues = [request_postgres.PostgresQueueBackend('short') for _ in range(2)]
    candidates = [queue.peek_provider_mutation() for queue in queues]
    assert all(candidate is not None for candidate in candidates)
    barrier = threading.Barrier(3)
    results = []

    def claim(index):
        barrier.wait(timeout=5)
        results.append(queues[index].claim_provider_mutation(candidates[index]))

    threads = [
        threading.Thread(target=claim, args=(index,)) for index in range(2)
    ]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert sum(item is not None for item in results) == 1
    winner = next(item for item in results if item is not None)
    assert winner.request_id == request_id
    assert winner.execution_generation == 1


def test_bound_provider_handoff_failure_is_durable_and_quiescent(
        request_database, monkeypatch):
    engine, backend = request_database
    request_id = 'bound-provider-handoff-failed'
    with engine.begin() as connection:
        assert request_postgres.insert_bound_request_and_queue_in_transaction(
            connection,
            _bound_request(request_id),
            ordinary_launch_association_id=uuid.uuid4())
    monkeypatch.setattr(storage, '_storage_backend', backend)
    queue = executor.RequestQueue(
        request_postgres.PostgresQueueBackend('short'))
    worker = executor.RequestWorker(schedule_type=requests.ScheduleType.SHORT,
                                    config=executor.server_config.WorkerConfig(
                                        garanteed_parallelism=1,
                                        burstable_parallelism=0,
                                        num_db_connections_per_worker=0))
    reservation = mock.sentinel.bound_provider_reservation
    proc_executor = mock.Mock(spec=executor.process.BurstableExecutor)
    proc_executor.try_reserve_idle_worker.return_value = reservation
    proc_executor.submit_reserved.side_effect = RuntimeError(
        'reserved handoff failed')

    worker.process_request(proc_executor, queue)

    proc_executor.submit_reserved.assert_called_once()
    proc_executor.release_idle_worker_reservation.assert_called_once_with(
        reservation)
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id)).mappings().one()
        queue_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.QUEUE).where(
                                 request_postgres.QUEUE.c.request_id ==
                                 request_id)).scalar_one()
    assert row['status'] == requests.RequestStatus.FAILED.value
    assert row['terminal_cause'] == (
        event_api_models.EventCause.DISPATCHER_SUBMIT_FAILED.value)
    assert row['execution_generation'] == 1
    assert row['execution_quiesced_generation'] == 1
    assert row['execution_quiesced_at'] is not None
    assert queue_count == 0


def test_generic_handoff_failure_is_durable_and_quiescent(
        request_database, monkeypatch):
    engine, backend = request_database
    request_id = 'generic-handoff-failed'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    monkeypatch.setattr(storage, '_storage_backend', backend)
    queue = executor.RequestQueue(
        request_postgres.PostgresQueueBackend('short'))
    worker = executor.RequestWorker(schedule_type=requests.ScheduleType.SHORT,
                                    config=executor.server_config.WorkerConfig(
                                        garanteed_parallelism=1,
                                        burstable_parallelism=0,
                                        num_db_connections_per_worker=0))
    reservation = mock.sentinel.generic_reservation
    proc_executor = mock.Mock(spec=executor.process.BurstableExecutor)
    proc_executor.try_reserve_idle_worker.return_value = reservation
    proc_executor.submit_reserved.side_effect = RuntimeError(
        'generic handoff failed')

    worker.process_request(proc_executor, queue)

    proc_executor.submit_reserved.assert_called_once()
    proc_executor.release_idle_worker_reservation.assert_called_once_with(
        reservation)
    request_row, queue_row = _execution_claim_state(engine, request_id)
    assert request_row['status'] == requests.RequestStatus.FAILED.value
    assert request_row['terminal_cause'] == (
        event_api_models.EventCause.DISPATCHER_SUBMIT_FAILED.value)
    assert request_row['execution_quiesced_generation'] == 1
    assert request_row['execution_quiesced_at'] is not None
    assert not queue_row


def test_bound_effect_claim_requires_exact_request_queue_pin_and_owner(
        bound_request_database):
    engine, backend = bound_request_database
    identity, _, _, item = _claim_gc_bound_request(engine, backend)
    request_id = identity.request_id
    association_id = identity.association_id
    assert item.claim_token is not None
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token,
                                    424242)
    assert item.worker_instance_id is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    with engine.begin() as connection:
        assert (request_postgres.
                validate_bound_ordinary_launch_claim_in_transaction(
                    connection, association_id, claim))

    invalid_claims = (
        dataclasses.replace(claim,
                            execution_generation=claim.execution_generation +
                            1),
        dataclasses.replace(claim, claim_token=str(uuid.uuid4())),
        dataclasses.replace(claim, worker_instance_id=str(uuid.uuid4())),
        dataclasses.replace(claim, request_id='different-request'),
    )
    for invalid_claim in invalid_claims:
        with engine.begin() as connection:
            assert not (request_postgres.
                        validate_bound_ordinary_launch_claim_in_transaction(
                            connection, association_id, invalid_claim))

    with engine.begin() as connection:
        assert request_postgres.delete_request_retention_pin_in_transaction(
            connection, request_id,
            request_postgres.ORDINARY_LAUNCH_RETENTION_PIN_KIND, association_id)
    with engine.begin() as connection:
        assert not (request_postgres.
                    validate_bound_ordinary_launch_claim_in_transaction(
                        connection, association_id, claim))


def _claim_gc_bound_request(engine, _backend):
    """Admit one canonical association and stop in the claim handoff."""
    identity = _gc_binding_identity()
    request = _bound_request(identity.request_id)
    with engine.begin() as connection:
        admission = ordinary_launch_binding.insert_or_get_locked(
            connection, identity)
        assert request_postgres.insert_bound_request_and_queue_in_transaction(
            connection,
            request,
            ordinary_launch_association_id=identity.association_id)
    context = _gc_binding_context(identity, admission.launch_generation)
    queue = request_postgres.PostgresQueueBackend('short')
    candidate = queue.peek_provider_mutation()
    assert candidate is not None
    item = queue.claim_provider_mutation(candidate)
    assert item is not None
    assert item.request_id == identity.request_id
    assert item.claim_token is not None
    assert item.worker_instance_id is not None
    return identity, context, queue, item


def _expire_claim(engine, request_id):
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    lease_expires_at=sqlalchemy.func.clock_timestamp() -
                    datetime.timedelta(seconds=1)))


def _keep_replica_projection(_connection, _projection):
    return True


def test_generic_reaper_leaves_expired_bound_claim_for_canonical_reducer(
        bound_request_database):
    engine, backend = bound_request_database
    identity, context, queue, item = _claim_gc_bound_request(engine, backend)
    _expire_claim(engine, identity.request_id)

    # Exercise the losing scheduler order explicitly: the generic reaper gets
    # the first transaction, but it cannot erase association-aware evidence.
    with engine.begin() as connection:
        queue._reap_expired_claims(connection)
    with engine.connect() as connection:
        request_row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id)).mappings().one()
        queue_row = connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                identity.request_id)).mappings().one()
    assert request_row['status'] == requests.RequestStatus.PENDING.value
    assert request_row['claim_token'] == uuid.UUID(item.claim_token)
    assert request_row['worker_instance_id'] == uuid.UUID(
        item.worker_instance_id)
    assert queue_row['delivery_state'] == 'claimed'
    assert queue_row['claim_generation'] == item.execution_generation

    reduction = request_postgres.reduce_bound_ordinary_launch(
        context,
        _gc_binding_authority(),
        project_replica_result=_keep_replica_projection)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.PRE_EFFECT_TERMINAL)
    assert reduction.request.quiescent
    assert reduction.request.execution_quiesced_generation == (
        item.execution_generation)


def test_bound_reducer_blocks_on_protocol_before_authority_rows(
        bound_request_database, monkeypatch):
    engine, backend = bound_request_database
    identity, context, queue, _ = _claim_gc_bound_request(engine, backend)
    _expire_claim(engine, identity.request_id)
    with engine.begin() as connection:
        queue._reap_expired_claims(connection)

    reducer_pid = []
    reducer_connected = threading.Event()
    original_lock = (
        serve_state.lock_zero_cost_protocol_for_bound_launch_projection)

    def capture_pid_before_protocol(connection):
        reducer_pid.append(
            connection.execute(
                sqlalchemy.text('SELECT pg_backend_pid()')).scalar_one())
        reducer_connected.set()
        original_lock(connection)

    monkeypatch.setattr(serve_state,
                        'lock_zero_cost_protocol_for_bound_launch_projection',
                        capture_pid_before_protocol)
    protocol = pool_capacity_observation_schema.protocol_state_sequence_table
    with engine.connect() as holder:
        transaction = holder.begin()
        holder.execute(
            sqlalchemy.select(protocol.c.id).where(
                protocol.c.id == 1).with_for_update()).one()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                request_postgres.reduce_bound_ordinary_launch,
                context,
                _gc_binding_authority(),
                project_replica_result=_keep_replica_projection)
            assert reducer_connected.wait(timeout=5)
            deadline = time.monotonic() + 5
            blockers = []
            while time.monotonic() < deadline:
                blockers = holder.execute(
                    sqlalchemy.text('SELECT pg_blocking_pids(:pid)'), {
                        'pid': reducer_pid[0]
                    }).scalar_one()
                if blockers:
                    break
                time.sleep(0.05)
            assert blockers

            # A protocol-owning participant can still take every downstream
            # authority mutex. If the reducer took any of them before protocol,
            # this exact schedule would form a cycle instead of completing.
            holder.execute(
                sqlalchemy.select(
                    serve_state_schema.service_lifecycle_fences_table.c.name).
                where(serve_state_schema.service_lifecycle_fences_table.c.name
                      == 'gc-service').with_for_update()).one()
            holder.execute(
                sqlalchemy.select(
                    serve_state_schema.services_table.c.name).where(
                        serve_state_schema.services_table.c.name ==
                        'gc-service').with_for_update()).one()
            holder.execute(
                sqlalchemy.select(
                    serve_state_schema.replicas_table.c.replica_id).where(
                        serve_state_schema.replicas_table.c.service_name ==
                        'gc-service',
                        serve_state_schema.replicas_table.c.replica_id ==
                        3).with_for_update()).one()
            holder.execute(
                sqlalchemy.select(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.association_id).where(
                        ordinary_launch_binding.
                        ordinary_launch_associations_table.c.association_id ==
                        identity.association_id).with_for_update()).one()
            transaction.commit()
            reduction = future.result(timeout=5)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.PRE_EFFECT_TERMINAL)


def test_terminal_bound_pidless_claim_settles_immediately_before_provider_io(
        bound_request_database):
    engine, backend = bound_request_database
    _, context, _, item = _claim_gc_bound_request(engine, backend)
    facts = request_postgres.request_bound_ordinary_launch_cancel(
        context, _gc_binding_authority(), 'replica-teardown')
    assert facts.status is requests.RequestStatus.CANCELLED
    assert not facts.queue_exists
    assert facts.claim_token == uuid.UUID(item.claim_token)
    assert facts.quiescent
    assert facts.execution_quiesced_generation == item.execution_generation

    reduction = request_postgres.reduce_bound_ordinary_launch(
        context,
        _gc_binding_authority(),
        project_replica_result=_keep_replica_projection)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.PRE_EFFECT_TERMINAL)
    assert reduction.request.terminal_cause is (
        event_api_models.EventCause.EXPLICIT_CANCEL)
    assert reduction.request.quiescent
    assert reduction.request.execution_quiesced_generation == (
        item.execution_generation)
    # Atomic pre-effect proof keeps the exact owner identity, so a late wrapper
    # acknowledgement is an idempotent success.
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    assert backend.acknowledge_execution_quiescence(claim)


def test_active_expired_bound_claim_preserves_prior_exact_quiescence(
        bound_request_database):
    engine, backend = bound_request_database
    identity, context, _, item = _claim_gc_bound_request(engine, backend)
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token,
                                    424242)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    # The handler's finally block can win after its terminal status CAS loses
    # the lease race. Preserve this genuine receipt when expiry settles the
    # still-active NOT_STARTED request.
    assert backend.acknowledge_execution_quiescence(claim)
    with engine.connect() as connection:
        prior_quiesced_at = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.execution_quiesced_at).where(
                    request_postgres.REQUESTS.c.request_id ==
                    identity.request_id)).scalar_one()
    _expire_claim(engine, identity.request_id)

    reduction = request_postgres.reduce_bound_ordinary_launch(
        context,
        _gc_binding_authority(),
        project_replica_result=_keep_replica_projection)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.PRE_EFFECT_TERMINAL)
    assert reduction.request.quiescent
    assert reduction.request.execution_quiesced_at == prior_quiesced_at
    assert reduction.request.terminal_cause is (
        event_api_models.EventCause.EXECUTION_LEASE_EXPIRED)


@pytest.mark.parametrize('terminal_before_expiry', [False, True])
def test_expired_bound_claim_after_provider_io_is_ambiguous(
        bound_request_database, terminal_before_expiry):
    engine, backend = bound_request_database
    identity, context, queue, item = _claim_gc_bound_request(engine, backend)
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token,
                                    424242)
    body = _legacy_serve_launch_request(identity.request_id).request_body
    ordinary_launch_binding.install_bound_context(body, identity,
                                                  context.launch_generation)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    with ordinary_launch_binding.provider_effect_guard(
            body.extra_launch_context,
            claim,
            claim_validator=(
                request_postgres.
                validate_bound_ordinary_launch_claim_in_transaction)):
        pass
    if terminal_before_expiry:
        facts = request_postgres.request_bound_ordinary_launch_cancel(
            context, _gc_binding_authority(), 'replica-teardown')
        assert facts.status is requests.RequestStatus.CANCELLED
        assert not facts.queue_exists
    _expire_claim(engine, identity.request_id)
    # When the queue still exists, generic request expiry must remain a no-op.
    with engine.begin() as connection:
        queue._reap_expired_claims(connection)

    reduction = request_postgres.reduce_bound_ordinary_launch(
        context,
        _gc_binding_authority(),
        project_replica_result=_keep_replica_projection)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.AMBIGUOUS)
    with engine.connect() as connection:
        resolution = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                resolution).where(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.association_id == identity.association_id)).scalar_one()
        request_row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id)).mappings().one()
        queue_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count(  # pylint: disable=not-callable
                )).select_from(request_postgres.QUEUE).where(
                    request_postgres.QUEUE.c.request_id ==
                    identity.request_id)).scalar_one()
    assert resolution == ordinary_launch_binding.Resolution.AMBIGUOUS.value
    assert reduction.request.status is requests.RequestStatus.CANCELLED
    assert request_row['cancel_requested_at'] is not None
    assert request_row['execution_quiescence_required'] is True
    assert request_row['execution_quiesced_generation'] is None
    assert request_row['execution_quiesced_at'] is None
    assert request_row['pid'] == 1234
    assert request_row['claim_token'] == uuid.UUID(item.claim_token)
    assert request_row['worker_instance_id'] == uuid.UUID(
        item.worker_instance_id)
    assert request_row['should_retry'] is False
    assert queue_count == 0
    assert not reduction.request.quiescent
    if terminal_before_expiry:
        assert reduction.request.terminal_cause is (
            event_api_models.EventCause.EXPLICIT_CANCEL)
    else:
        assert reduction.request.terminal_cause is (
            event_api_models.EventCause.EXECUTION_LEASE_EXPIRED)

    # PROVIDER_IO makes the provider outcome ambiguous, but does not erase the
    # exact local execution address or manufacture proof that its handler has
    # stopped. The real wrapper receipt remains accepted and idempotent later.
    assert backend.acknowledge_execution_quiescence(claim)
    assert backend.acknowledge_execution_quiescence(claim)
    replay = request_postgres.reduce_bound_ordinary_launch(
        context,
        _gc_binding_authority(),
        project_replica_result=_keep_replica_projection)
    assert replay.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.AMBIGUOUS)
    assert replay.request.quiescent


def test_failed_teardown_keeps_protocol_v1_ambiguity_fail_closed(
        bound_request_database):
    engine, backend = bound_request_database
    identity, context, _, item = _claim_gc_bound_request(engine, backend)
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token,
                                    424242)
    body = _legacy_serve_launch_request(identity.request_id).request_body
    ordinary_launch_binding.install_bound_context(body, identity,
                                                  context.launch_generation)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    with ordinary_launch_binding.provider_effect_guard(
            body.extra_launch_context,
            claim,
            claim_validator=(
                request_postgres.
                validate_bound_ordinary_launch_claim_in_transaction)):
        pass
    request_postgres.request_bound_ordinary_launch_cancel(
        context, _gc_binding_authority(), 'failed-service-teardown-test')
    assert backend.acknowledge_execution_quiescence(claim)
    reduction = request_postgres.reduce_bound_ordinary_launch(
        context,
        _gc_binding_authority(),
        project_replica_result=_keep_replica_projection)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.AMBIGUOUS)
    teardown = ordinary_launch_binding.begin_service_teardown_if_owner(
        'gc-service', 'gc-service-hash', (123, '10.0.0.2'))
    assert teardown.authority is not None
    info = serve_state.get_replica_info_from_id('gc-service', 3)
    assert info is not None

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='ambiguous association cannot authorize'):
        service._settle_bound_ordinary_launches_for_teardown(
            teardown.authority, [info])

    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == identity.association_id)).mappings().one()
        replica_pointer = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.
                ordinary_launch_association_id).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    'gc-service', serve_state_schema.replicas_table.c.replica_id
                    == 3)).scalar_one()
        pin_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(request_postgres.REQUEST_RETENTION_PINS).where(
                request_postgres.REQUEST_RETENTION_PINS.c.request_id ==
                identity.request_id)).scalar_one()
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.AMBIGUOUS.value)
    assert replica_pointer == identity.association_id
    assert pin_count == 1


def test_malformed_success_result_is_durably_ambiguous(bound_request_database):
    engine, backend = bound_request_database
    identity, context, _, item = _claim_gc_bound_request(engine, backend)
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token,
                                    424242)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    body = _legacy_serve_launch_request(identity.request_id).request_body
    ordinary_launch_binding.install_bound_context(body, identity,
                                                  context.launch_generation)
    with ordinary_launch_binding.provider_effect_guard(
            body.extra_launch_context,
            claim,
            claim_validator=(
                request_postgres.
                validate_bound_ordinary_launch_claim_in_transaction)):
        ordinary_launch_binding.begin_service_job_io(body.extra_launch_context)
        ordinary_launch_binding.record_service_job(body.extra_launch_context,
                                                   17)
    active_claim = storage.activate_execution_claim(claim.request_id,
                                                    claim.execution_generation,
                                                    claim.claim_token)
    try:
        # The launch encoder accepts the two-tuple, but a null resource handle
        # violates the reducer's exact success-result contract.
        assert backend.set_request_finished(identity.request_id,
                                            requests.RequestStatus.SUCCEEDED,
                                            result=(17, None))
    finally:
        storage.deactivate_execution_claim(active_claim)
    assert backend.acknowledge_execution_quiescence(claim)

    reduction = request_postgres.reduce_bound_ordinary_launch(
        context,
        _gc_binding_authority(),
        project_replica_result=_keep_replica_projection)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.AMBIGUOUS)
    with engine.connect() as connection:
        ambiguity_code = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                ambiguity_code).where(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.association_id == identity.association_id)).scalar_one()
    assert ambiguity_code == 'service-job-result-malformed-or-mismatched'


def test_malformed_request_error_is_durably_ambiguous(bound_request_database):
    engine, backend = bound_request_database
    identity, context, _, _ = _claim_gc_bound_request(engine, backend)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                identity.request_id).values(
                    error={
                        'object': 'not-valid-encoded-exception',
                        'type': 'ResourcesUnavailableError',
                        'message': 'corrupt durable error',
                    }))

    reduction = request_postgres.reduce_bound_ordinary_launch(
        context,
        _gc_binding_authority(),
        project_replica_result=_keep_replica_projection)
    assert reduction.disposition is (
        request_postgres.OrdinaryLaunchReductionDisposition.AMBIGUOUS)
    with engine.connect() as connection:
        ambiguity_code = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                ambiguity_code).where(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.association_id == identity.association_id)).scalar_one()
    assert ambiguity_code == 'request-error-malformed'


def test_generic_retention_pin_blocks_candidates_and_final_delete(
        request_database):
    engine, backend = request_database
    request_id = 'generically-pinned'
    request = _request(request_id, should_enqueue=False)
    assert asyncio.run(backend.create_if_not_exists_async(request))
    assert backend.set_request_finished(request_id,
                                        requests.RequestStatus.SUCCEEDED,
                                        result=[])
    pin_id = uuid.uuid4()
    with engine.begin() as connection:
        request_postgres.insert_request_retention_pin_in_transaction(
            connection, request_id, 'test-owner.v1', pin_id)

    candidates = backend.query_requests(
        requests.RequestTaskFilter(request_ids=[request_id],
                                   retention_safe=True))
    assert candidates == []
    asyncio.run(backend.delete_requests([request_id]))
    assert backend.get_request(request_id) is not None

    with engine.begin() as connection:
        assert request_postgres.delete_request_retention_pin_in_transaction(
            connection, request_id, 'test-owner.v1', pin_id)
    assert [
        request.request_id for request in backend.query_requests(
            requests.RequestTaskFilter(request_ids=[request_id],
                                       retention_safe=True))
    ] == [request_id]
    asyncio.run(backend.delete_requests([request_id]))
    assert backend.get_request(request_id) is None


def test_retention_pin_primary_key_kind_check_and_request_fk(request_database):
    engine, backend = request_database
    first_id = 'pin-owner-first'
    second_id = 'pin-owner-second'
    assert asyncio.run(
        backend.create_if_not_exists_async(
            _request(first_id, should_enqueue=False)))
    assert asyncio.run(
        backend.create_if_not_exists_async(
            _request(second_id, should_enqueue=False)))
    pin_id = uuid.uuid4()
    with engine.begin() as connection:
        request_postgres.insert_request_retention_pin_in_transaction(
            connection, first_id, 'same-kind', pin_id)

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            request_postgres.insert_request_retention_pin_in_transaction(
                connection, second_id, 'same-kind', pin_id)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(
                    request_postgres.REQUEST_RETENTION_PINS).values(
                        request_id=second_id, pin_kind='', pin_id=uuid.uuid4()))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            request_postgres.insert_request_retention_pin_in_transaction(
                connection, 'missing-request', 'test-owner.v1', uuid.uuid4())


def test_schema_bootstrap_is_postgres_only_and_versioned(request_database):
    engine, _ = request_database
    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.API_REQUESTS_DB_NAME) == '015'
    inspector = sqlalchemy.inspect(engine)
    assert {
        'api_requests', 'api_request_queue', 'api_server_instances',
        'api_request_store_metadata', 'api_controller_leadership',
        'api_controller_action_reservations', 'resource_events',
        'resource_event_targets', 'api_resource_actions',
        'api_resource_action_attempts', 'api_request_retention_pins',
        'api_request_executor_termination_evidence'
    }.issubset(inspector.get_table_names())
    request_columns = {
        column['name'] for column in inspector.get_columns('api_requests')
    }
    assert 'event_context' in request_columns
    assert {'resource_action_id',
            'resource_action_attempt'}.issubset(request_columns)
    assert 'ordinary_launch_association_id' in request_columns
    assert {
        'binding_protocol_version', 'profile_kind', 'profile_version',
        'profile_digest', 'capability_cohort_epoch',
        'capability_profile_set_digest', 'receipt_protocol_version'
    }.issubset(request_columns)
    assert 'execution_process_start_time_ticks' in request_columns
    assert {
        'execution_quiescence_required', 'execution_quiesced_generation',
        'execution_quiesced_at'
    }.issubset(request_columns)
    instance_columns = {
        column['name']
        for column in inspector.get_columns('api_server_instances')
    }
    assert 'ordinary_launch_binding_capable' in instance_columns
    assert {
        'non_pool_launch_binding_capable',
        'non_pool_launch_binding_protocol_version',
        'non_pool_launch_capability_profile_set_digest',
        'non_pool_launch_capability_cohort_epoch',
        'non_pool_launch_receipt_protocol_version'
    }.issubset(instance_columns)
    assert {
        'ordered_capacity_admission_capable',
        'ordered_capacity_admission_protocol_version',
        'ordered_capacity_admission_cohort_epoch', 'pod_namespace',
        'executor_termination_evidence_capable',
        'executor_termination_evidence_protocol_version'
    }.issubset(instance_columns)
    evidence_checks = {
        constraint['name'] for constraint in inspector.get_check_constraints(
            'api_request_executor_termination_evidence')
    }
    assert 'ck_api013_executor_termination_time' not in evidence_checks
    assert 'ck_api014_executor_termination_source' in evidence_checks
    binding_index = {
        index['name']: index for index in inspector.get_indexes('api_requests')
    }['uq_api_requests_ordinary_launch_association']
    assert binding_index['unique']
    assert binding_index['column_names'] == ['ordinary_launch_association_id']
    attempt_columns = {
        column['name']
        for column in inspector.get_columns('api_resource_action_attempts')
    }
    assert {
        'provider_io_boundary', 'provider_progress', 'provider_progress_sha256',
        'provider_progress_revision'
    }.issubset(attempt_columns)
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


def test_resource_action_schema_is_bounded_and_request_points_to_attempt(
        request_database):
    engine, _ = request_database
    inspector = sqlalchemy.inspect(engine)

    action_tables = {
        table for table in inspector.get_table_names()
        if table.startswith('api_resource_action')
    }
    assert action_tables == {
        'api_resource_actions', 'api_resource_action_attempts'
    }
    action_columns = {
        column['name']
        for column in inspector.get_columns('api_resource_actions')
    }
    assert action_columns == {
        'action_id', 'domain', 'resource_type', 'resource_identity',
        'desired_generation', 'action_type', 'immutable_spec',
        'immutable_spec_sha256', 'kernel_state', 'current_attempt',
        'next_attempt_at', 'last_result', 'last_result_sha256',
        'terminal_disposition', 'revision', 'created_at', 'updated_at',
        'terminal_at'
    }
    attempt_columns = {
        column['name']
        for column in inspector.get_columns('api_resource_action_attempts')
    }
    assert attempt_columns == {
        'action_id', 'attempt', 'request_id', 'request_input_sha256',
        'provider_operation_id', 'mutation_boundary', 'provider_io_boundary',
        'provider_progress', 'provider_progress_sha256',
        'provider_progress_revision', 'typed_outcome', 'typed_outcome_sha256',
        'request_terminal_state', 'admitted_at', 'updated_at', 'settled_at'
    }
    assert 'TEXT' in str(
        next(column['type']
             for column in inspector.get_columns('api_resource_action_attempts')
             if column['name'] == 'request_id')).upper()

    request_fks = {
        foreign_key['name']: foreign_key
        for foreign_key in inspector.get_foreign_keys('api_requests')
    }
    correlation_fk = request_fks['fk_api_requests_resource_action_attempt']
    assert correlation_fk['constrained_columns'] == [
        'resource_action_id', 'resource_action_attempt', 'request_id'
    ]
    assert correlation_fk['referred_table'] == ('api_resource_action_attempts')
    assert correlation_fk['referred_columns'] == [
        'action_id', 'attempt', 'request_id'
    ]
    attempt_fks = {
        foreign_key['name']: foreign_key for foreign_key in
        inspector.get_foreign_keys('api_resource_action_attempts')
    }
    action_fk = attempt_fks['fk_api_resource_action_attempts_action']
    assert action_fk['constrained_columns'] == ['action_id']
    assert action_fk['referred_table'] == 'api_resource_actions'
    assert action_fk['referred_columns'] == ['action_id']
    assert all(foreign_key['referred_table'] != 'api_requests'
               for foreign_key in attempt_fks.values())

    action_indexes = {
        index['name']: index
        for index in inspector.get_indexes('api_resource_actions')
    }
    due_index = action_indexes['ix_api_resource_actions_due']
    assert due_index['column_names'] == ['next_attempt_at', 'action_id']
    assert _normalized_index_predicate(due_index) == "kernel_state='READY'"
    queued_index = action_indexes['ix_api_resource_actions_queued']
    assert queued_index['column_names'] == ['updated_at', 'action_id']
    assert _normalized_index_predicate(queued_index) == "kernel_state='QUEUED'"

    request_indexes = {
        index['name']: index for index in inspector.get_indexes('api_requests')
    }
    correlation_index = request_indexes[
        'ix_api_requests_resource_action_attempt']
    assert correlation_index['column_names'] == [
        'resource_action_id', 'resource_action_attempt'
    ]
    assert _normalized_index_predicate(
        correlation_index) == 'resource_action_idISNOTNULL'


def test_resource_action_identity_and_request_binding_constraints(
        request_database):
    engine, backend = request_database
    action_id = uuid.uuid4()
    request_id = str(uuid.uuid4())
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.RESOURCE_ACTIONS).values(
                **_action_values(action_id)))
        connection.execute(
            sqlalchemy.insert(request_postgres.RESOURCE_ACTION_ATTEMPTS).values(
                **_attempt_values(action_id, 1, request_id)))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(request_postgres.RESOURCE_ACTIONS).values(
                    **_action_values(uuid.uuid4())))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(
                    request_postgres.RESOURCE_ACTION_ATTEMPTS).values(
                        **_attempt_values(action_id, 2, request_id)))

    request = _request(request_id)
    assert asyncio.run(backend.create_if_not_exists_async(request))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    resource_action_id=action_id, resource_action_attempt=1))

    ordinary_id = str(uuid.uuid4())
    assert asyncio.run(backend.create_if_not_exists_async(
        _request(ordinary_id)))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    ordinary_id).values(resource_action_id=action_id,
                                        resource_action_attempt=1))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    ordinary_id).values(resource_action_id=action_id))


def test_resource_action_json_constraints_reject_json_null(request_database):
    engine, _ = request_database
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(request_postgres.RESOURCE_ACTIONS).values({
                    **_action_values(uuid.uuid4(),
                                     resource_identity='json-null-spec'),
                    'immutable_spec': sqlalchemy.text("'null'::jsonb"),
                }))

    action_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                request_postgres.RESOURCE_ACTIONS).values(**_action_values(
                    action_id, resource_identity='json-null-outcome')))
        connection.execute(
            sqlalchemy.insert(request_postgres.RESOURCE_ACTION_ATTEMPTS).values(
                **_attempt_values(action_id, 1, str(uuid.uuid4()))))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.RESOURCE_ACTIONS).where(
                    request_postgres.RESOURCE_ACTIONS.c.action_id ==
                    action_id).values(last_result={'disposition': 'retry'}))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                        action_id).values(
                            mutation_boundary='SETTLED',
                            typed_outcome=sqlalchemy.text("'null'::jsonb"),
                            typed_outcome_sha256='c' * 64,
                            request_terminal_state='SUCCEEDED',
                            settled_at=sqlalchemy.func.clock_timestamp(),
                            updated_at=sqlalchemy.func.clock_timestamp()))


def test_correlated_request_gc_waits_for_settled_attempt(
        request_database, monkeypatch, tmp_path):
    engine, backend = request_database
    action_id = uuid.uuid4()
    request_id = str(uuid.uuid4())
    request = _request(request_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.RESOURCE_ACTIONS).values(
                **_action_values(action_id)))
        connection.execute(
            sqlalchemy.insert(request_postgres.RESOURCE_ACTION_ATTEMPTS).values(
                **_attempt_values(action_id, 1, request_id)))
        assert request_postgres._insert_request_and_queue(
            connection,
            request,
            resource_action_id=action_id,
            resource_action_attempt=1)

    item = _claim(backend, request_id)
    assert item.claim_token is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token)
    context = storage.activate_execution_claim(claim.request_id,
                                               claim.execution_generation,
                                               claim.claim_token)
    try:
        assert backend.heartbeat_claim(claim)
        backend.set_request_finished(request_id,
                                     requests.RequestStatus.SUCCEEDED,
                                     result=[])
    finally:
        storage.deactivate_execution_claim(context)
    assert backend.acknowledge_execution_quiescence(claim)
    assert backend.get_request(
        request_id).status is requests.RequestStatus.SUCCEEDED
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    finished_at=sqlalchemy.func.clock_timestamp() -
                    datetime.timedelta(minutes=1)))

    log_dir = tmp_path / 'logs'
    legacy_log_dir = tmp_path / 'legacy-logs'
    debug_log_dir = tmp_path / 'debug-logs'
    monkeypatch.setattr(requests.server_constants, 'REQUEST_LOG_PATH_PREFIX',
                        str(log_dir))
    monkeypatch.setattr(requests, 'LEGACY_REQUEST_LOG_PATH_PREFIX',
                        str(legacy_log_dir))
    monkeypatch.setattr(requests.sky_logging, 'DEBUG_LOG_DIR',
                        str(debug_log_dir))
    monkeypatch.setattr(storage, '_storage_backend', backend)
    files = [
        request.log_path,
        requests._get_legacy_log_path(request_id),
        (debug_log_dir / request_id).with_suffix('.log'),
        pathlib.Path(requests.request_lock_path(request_id)),
    ]
    for path in files:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

    asyncio.run(requests.clean_finished_requests_with_retention(0))
    with engine.connect() as connection:
        request_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.REQUESTS).where(
                                 request_postgres.REQUESTS.c.request_id ==
                                 request_id)).scalar_one()
    assert request_count == 1
    assert all(path.exists() for path in files)

    # The final delete repeats the predicate, protecting callers that did not
    # use the retention-safe candidate filter.
    asyncio.run(backend.delete_requests([request_id]))
    with engine.connect() as connection:
        request_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.REQUESTS).where(
                                 request_postgres.REQUESTS.c.request_id ==
                                 request_id)).scalar_one()
    assert request_count == 1

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action_id, request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt
                == 1).values(mutation_boundary='SETTLED',
                             typed_outcome={'disposition': 'succeeded'},
                             typed_outcome_sha256='c' * 64,
                             request_terminal_state='SUCCEEDED',
                             settled_at=sqlalchemy.func.clock_timestamp(),
                             updated_at=sqlalchemy.func.clock_timestamp()))
    asyncio.run(requests.clean_finished_requests_with_retention(0))

    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.REQUESTS).where(
                                 request_postgres.REQUESTS.c.request_id ==
                                 request_id)).scalar_one() == 0
        attempt = connection.execute(
            sqlalchemy.select(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action_id, request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt
                == 1)).mappings().one()
        action_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(request_postgres.RESOURCE_ACTIONS).where(
                request_postgres.RESOURCE_ACTIONS.c.action_id ==
                action_id)).scalar_one()
    assert attempt['request_id'] == request_id
    assert attempt['request_terminal_state'] == 'SUCCEEDED'
    assert attempt['typed_outcome'] == {'disposition': 'succeeded'}
    assert action_count == 1
    assert all(not path.exists() for path in files)


def test_api015_downgrade_guard_retains_head(request_database):
    engine, _ = request_database
    config = migration_utils.get_alembic_config(
        engine, migration_utils.API_REQUESTS_DB_NAME)
    with pytest.raises(RuntimeError, match='API015 is forward-only'):
        alembic_command.downgrade(config, '005')

    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.API_REQUESTS_DB_NAME) == '015'
    inspector = sqlalchemy.inspect(engine)
    assert 'api_resource_actions' in inspector.get_table_names()
    assert 'api_resource_action_attempts' in inspector.get_table_names()
    assert 'api_request_retention_pins' in inspector.get_table_names()


def test_api015_downgrade_guard_retains_binding_evidence(request_database):
    engine, _ = request_database
    columns_before = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('api_requests')
    }
    instance_columns_before = {
        column['name'] for column in sqlalchemy.inspect(engine).get_columns(
            'api_server_instances')
    }
    config = migration_utils.get_alembic_config(
        engine, migration_utils.API_REQUESTS_DB_NAME)

    with pytest.raises(RuntimeError, match='API015 is forward-only'):
        alembic_command.downgrade(config, '008')

    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.API_REQUESTS_DB_NAME) == '015'
    assert {
        'execution_quiescence_required', 'execution_quiesced_generation',
        'execution_quiesced_at', 'ordinary_launch_association_id',
        'binding_protocol_version', 'profile_kind', 'profile_version',
        'profile_digest', 'capability_cohort_epoch',
        'capability_profile_set_digest', 'receipt_protocol_version',
        'managed_job_id', 'managed_job_controller_instance_id',
        'managed_job_controller_generation', 'managed_job_controller_slot_id',
        'managed_job_controller_slot_attempt'
    } <= columns_before
    assert columns_before == {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('api_requests')
    }
    assert {
        'request_storage_backend', 'request_queue_backend',
        'execution_quiescence_capable', 'ordinary_launch_binding_capable',
        'non_pool_launch_binding_capable',
        'non_pool_launch_binding_protocol_version',
        'non_pool_launch_capability_profile_set_digest',
        'non_pool_launch_capability_cohort_epoch',
        'non_pool_launch_receipt_protocol_version',
        'ordered_capacity_admission_capable',
        'ordered_capacity_admission_protocol_version',
        'ordered_capacity_admission_cohort_epoch', 'pod_namespace',
        'executor_termination_evidence_capable',
        'executor_termination_evidence_protocol_version'
    } <= instance_columns_before
    assert instance_columns_before == {
        column['name'] for column in sqlalchemy.inspect(engine).get_columns(
            'api_server_instances')
    }
    assert 'api_request_retention_pins' in sqlalchemy.inspect(
        engine).get_table_names()


def test_server_instance_lease_publishes_ready_and_draining(
        request_database, monkeypatch, tmp_path):
    engine, backend = request_database
    instance_id = str(uuid.uuid4())
    drain_marker = tmp_path / 'draining'
    monkeypatch.setattr(request_postgres, 'ROLE_DRAIN_MARKER_PATH',
                        str(drain_marker))
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR, instance_id)
    monkeypatch.setenv('HOSTNAME', 'executor-pod')
    monkeypatch.setenv('SKYPILOT_POD_UID', instance_id)
    monkeypatch.setenv('SKYPILOT_POD_NAMESPACE', 'skypilot')
    monkeypatch.setenv('POD_IP', '10.0.0.1')
    monkeypatch.setenv(request_postgres.REQUEST_BACKEND_ENV_VAR,
                       request_postgres.POSTGRES_REQUEST_BACKEND)
    monkeypatch.setattr(storage, '_storage_backend', backend)
    monkeypatch.setattr(queue_base, '_queue_backend_factory',
                        request_postgres.PostgresQueueFactory())
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
    assert row['pod_uid'] == instance_id
    assert row['pod_namespace'] == 'skypilot'
    assert row['ready']
    assert row['draining_at'] is None
    assert row['health_detail'] == {'phase': 'claiming'}
    assert row['supported_handlers']
    assert row['request_storage_backend'] == (
        request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE)
    assert row['request_queue_backend'] == (
        request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE)
    assert row['execution_quiescence_capable'] is True
    assert row['ordinary_launch_binding_capable'] is True
    assert row['ordered_capacity_admission_capable'] is True
    assert row['ordered_capacity_admission_protocol_version'] == 2
    assert row['ordered_capacity_admission_cohort_epoch'] == 2
    assert row['executor_termination_evidence_capable'] is True
    assert row['executor_termination_evidence_protocol_version'] == 2
    assert (ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME
            in row['supported_handlers'])
    monkeypatch.setenv('HOSTNAME', 'request-overlaid-pod')
    monkeypatch.setenv('SKYPILOT_POD_UID', 'request-overlaid-uid')
    monkeypatch.setenv('POD_IP', '203.0.113.99')
    assert lease._heartbeat()
    with engine.connect() as connection:
        immutable_row = connection.execute(
            sqlalchemy.select(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id == uuid.UUID(
                    instance_id))).mappings().one()
    assert immutable_row['pod_name'] == 'executor-pod'
    assert immutable_row['pod_uid'] == instance_id
    assert immutable_row['pod_namespace'] == 'skypilot'
    assert immutable_row['pod_ip'] == '10.0.0.1'
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
    drain_started_at = row['draining_at']
    prior_heartbeat_at = row['heartbeat_at']
    time.sleep(0.01)
    lease.begin_draining()
    with engine.connect() as connection:
        continuing_drain = connection.execute(
            sqlalchemy.select(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id == uuid.UUID(
                    instance_id))).mappings().one()
    assert continuing_drain['draining_at'] == drain_started_at
    assert continuing_drain['heartbeat_at'] > prior_heartbeat_at
    lease.stop()
    assert not lease.is_locally_ready()
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id == uuid.UUID(
                    instance_id))).mappings().one()
    assert not row['ready']
    assert row['draining_at'] is not None


def test_executor_termination_evidence_is_exact_idempotent_and_diagnostic(
        request_database, monkeypatch):
    engine, _ = request_database
    worker_id = str(uuid.uuid4())
    observer_id = str(uuid.uuid4())
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR, worker_id)
    backend = request_postgres.PostgresRequestBackend()
    request = _request('terminated-executor-request')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.REQUESTS).values(
                **request_postgres._request_values_for_db(request)))
        connection.execute(
            sqlalchemy.insert(request_postgres.QUEUE).values(
                **request_postgres._queue_values(request)))
    item = _claim(backend, request.request_id)
    assert item.claim_token is not None
    now = datetime.datetime.now(datetime.timezone.utc)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                instance_id=uuid.UUID(worker_id),
                role='executor',
                pod_name='executor-pod',
                pod_uid=worker_id,
                pod_namespace='skypilot',
                pod_ip='10.0.0.1',
                version='api014',
                started_at=now,
                heartbeat_at=now,
                ready=False,
                health_detail={'phase': 'draining'},
                supported_handlers=[],
                supported_payload_versions={},
                request_storage_backend=(
                    request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE),
                request_queue_backend=(
                    request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE),
                execution_quiescence_capable=True,
                executor_termination_evidence_capable=True,
                executor_termination_evidence_protocol_version=2))
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id).values(execution_quiescence_required=True))

    leader = _controller_leader(engine, monkeypatch, observer_id)
    assert leader.generation is not None
    observation = request_postgres.ExecutorTerminationObservation(
        kubernetes_cluster_uid=str(uuid.uuid4()),
        pod_namespace='skypilot',
        pod_name='executor-pod',
        pod_uid=worker_id,
        container_name='skypilot-executor',
        pod_resource_version='44',
        pod_event_type='DELETED',
        pod_phase='Succeeded',
        # API-server, kubelet, and database clocks are independent.  Exercise
        # both formerly rejected comparisons in one durable write.
        pod_deletion_timestamp=now + datetime.timedelta(seconds=120),
        container_finished_at=now + datetime.timedelta(seconds=60),
        container_exit_code=0,
        container_reason='Completed')
    owner = (observer_id, leader.generation)
    try:
        with pytest.raises(request_postgres.ExecutorTerminationEvidenceRejected,
                           match='successful final DELETED'):
            request_postgres.record_executor_termination_evidence(
                dataclasses.replace(observation, pod_event_type='MODIFIED'),
                observer_owner=owner)
        first = request_postgres.record_executor_termination_evidence(
            observation, observer_owner=owner)
        assert len(first) == 1
        assert request_postgres.record_executor_termination_evidence(
            observation, observer_owner=owner) == first
        changed = dataclasses.replace(observation, pod_resource_version='45')
        with pytest.raises(
                request_postgres.ExecutorTerminationEvidenceConflict):
            request_postgres.record_executor_termination_evidence(
                changed, observer_owner=owner)
        with engine.connect() as connection:
            evidence = connection.execute(
                sqlalchemy.select(request_postgres.EXECUTOR_TERMINATION_EVIDENCE
                                 )).mappings().one()
            stored_request = connection.execute(
                sqlalchemy.select(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    request.request_id)).mappings().one()
        assert str(evidence['evidence_id']) == first[0]
        assert evidence['request_id'] == request.request_id
        assert evidence['execution_generation'] == item.execution_generation
        assert str(evidence['claim_token']) == item.claim_token
        assert str(evidence['worker_instance_id']) == worker_id
        assert evidence['source'] == 'KUBERNETES_POD_FINAL_SUCCEEDED_V2'
        assert evidence['pod_event_type'] == 'DELETED'
        assert evidence['pod_phase'] == 'Succeeded'
        assert evidence['observer_controller_generation'] == leader.generation
        assert evidence['container_finished_at'] < evidence[
            'pod_deletion_timestamp']
        assert evidence['observed_at'] < evidence['container_finished_at']
        assert evidence['evidence_digest'] == (
            request_postgres._canonical_evidence_sha256(
                evidence['evidence_payload']))
        assert stored_request['execution_quiesced_at'] is None
        assert stored_request['execution_quiesced_generation'] is None
        with pytest.raises(sqlalchemy.exc.DBAPIError, match='append-only'):
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.update(
                        request_postgres.EXECUTOR_TERMINATION_EVIDENCE).values(
                            container_reason='Changed'))
        with pytest.raises(sqlalchemy.exc.DBAPIError, match='append-only'):
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.delete(
                        request_postgres.EXECUTOR_TERMINATION_EVIDENCE))
    finally:
        leader.release()

    with pytest.raises(request_postgres.ExecutorTerminationEvidenceRejected,
                       match='no longer owns'):
        request_postgres.record_executor_termination_evidence(
            observation, observer_owner=owner)


def test_binding_fleet_requires_every_recent_participant_and_local_handler(
        request_database):
    engine, _ = request_database
    bound_handler = (ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME)

    def _insert_instance(connection,
                         role,
                         *,
                         capable=True,
                         handlers=(),
                         ready=True,
                         draining=False):
        instance_id = uuid.uuid4()
        connection.execute(
            sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                instance_id=instance_id,
                role=role,
                version='api009',
                started_at=sqlalchemy.func.clock_timestamp(),
                heartbeat_at=sqlalchemy.func.clock_timestamp(),
                draining_at=(sqlalchemy.func.clock_timestamp()
                             if draining else None),
                ready=ready,
                health_detail={},
                supported_handlers=list(handlers),
                supported_payload_versions={},
                ordinary_launch_binding_capable=capable))
        return instance_id

    with engine.begin() as connection:
        _insert_instance(connection, 'api')
        _insert_instance(connection, 'executor', handlers=(bound_handler,))
        _insert_instance(connection, 'controller')
    assert request_postgres.ordinary_launch_binding_fleet_capable()

    with engine.begin() as connection:
        legacy_controller = _insert_instance(connection,
                                             'controller',
                                             capable=False,
                                             ready=False,
                                             draining=True)
    assert not request_postgres.ordinary_launch_binding_fleet_capable()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id ==
                legacy_controller).values(
                    heartbeat_at=sqlalchemy.func.clock_timestamp() -
                    datetime.timedelta(seconds=(
                        request_postgres.
                        ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS +
                        1))))
    assert request_postgres.ordinary_launch_binding_fleet_capable()

    with engine.begin() as connection:
        draining_legacy = _insert_instance(connection,
                                           'executor',
                                           capable=False,
                                           ready=False,
                                           draining=True)
    assert not request_postgres.ordinary_launch_binding_fleet_capable()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id ==
                draining_legacy).values(
                    heartbeat_at=sqlalchemy.func.clock_timestamp() -
                    datetime.timedelta(seconds=(
                        request_postgres.
                        ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS +
                        1))))
    assert request_postgres.ordinary_launch_binding_fleet_capable()

    with engine.begin() as connection:
        _insert_instance(connection,
                         'executor',
                         capable=True,
                         handlers=(),
                         ready=False)
    assert not request_postgres.ordinary_launch_binding_fleet_capable()


def test_non_pool_cohort_promotion_barrier_blocks_heartbeat_phantoms(
        request_database):
    engine, _ = request_database
    instance_id = uuid.uuid4()

    with engine.begin() as barrier_connection:
        assert isinstance(
            request_postgres.
            _non_pool_launch_binding_participants_quiesced_in_transaction(
                barrier_connection), bool)
        with pytest.raises(sqlalchemy.exc.DBAPIError, match='lock timeout'):
            with engine.begin() as heartbeat_connection:
                heartbeat_connection.execute(
                    sqlalchemy.text("SET LOCAL lock_timeout = '100ms'"))
                heartbeat_connection.execute(
                    sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                        instance_id=instance_id,
                        role='controller',
                        version='cohort-racer',
                        started_at=sqlalchemy.func.clock_timestamp(),
                        heartbeat_at=sqlalchemy.func.clock_timestamp(),
                        ready=False,
                        health_detail={},
                        supported_handlers=[],
                        supported_payload_versions={}))

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                instance_id=instance_id,
                role='controller',
                version='cohort-racer',
                started_at=sqlalchemy.func.clock_timestamp(),
                heartbeat_at=sqlalchemy.func.clock_timestamp(),
                ready=False,
                health_detail={},
                supported_handlers=[],
                supported_payload_versions={}))


def test_non_pool_fleet_rejects_recent_previous_cohort_participant(
        request_database):
    engine, _ = request_database
    current_cohort = ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH

    def _insert(connection, role, cohort):
        instance_id = uuid.uuid4()
        handlers = ([non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME]
                    if role == 'executor' else [])
        connection.execute(
            sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                instance_id=instance_id,
                role=role,
                version=f'cohort-{cohort}',
                started_at=sqlalchemy.func.clock_timestamp(),
                heartbeat_at=sqlalchemy.func.clock_timestamp(),
                ready=True,
                health_detail={},
                supported_handlers=handlers,
                supported_payload_versions={},
                non_pool_launch_binding_capable=True,
                non_pool_launch_binding_protocol_version=(
                    ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
                non_pool_launch_capability_profile_set_digest=(
                    ordinary_launch_binding.
                    supported_non_pool_profile_set_digest()),
                non_pool_launch_capability_cohort_epoch=cohort,
                non_pool_launch_receipt_protocol_version=(
                    ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)))
        return instance_id

    with engine.begin() as connection:
        _insert(connection, 'api', current_cohort)
        _insert(connection, 'executor', current_cohort)
        _insert(connection, 'controller', current_cohort)
    assert request_postgres.non_pool_launch_binding_fleet_capable()

    with engine.begin() as connection:
        previous = _insert(connection, 'executor', current_cohort - 1)
    assert not request_postgres.non_pool_launch_binding_fleet_capable()

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id == previous).
            values(heartbeat_at=sqlalchemy.func.clock_timestamp() -
                   datetime.timedelta(seconds=(
                       request_postgres.
                       ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS +
                       1))))
    assert request_postgres.non_pool_launch_binding_fleet_capable()


def test_ordered_capacity_fleet_requires_exact_recent_api015_cohort(
        request_database):
    engine, _ = request_database

    def _insert_instance(connection, role, *, capable=True, protocol=2):
        instance_id = uuid.uuid4()
        values = {
            'instance_id': instance_id,
            'role': role,
            'version': 'api015',
            'started_at': sqlalchemy.func.clock_timestamp(),
            'heartbeat_at': sqlalchemy.func.clock_timestamp(),
            'ready': True,
            'health_detail': {},
            'supported_handlers': [],
            'supported_payload_versions': {},
        }
        if capable:
            values.update(ordered_capacity_admission_capable=True,
                          ordered_capacity_admission_protocol_version=protocol,
                          ordered_capacity_admission_cohort_epoch=protocol)
        connection.execute(
            sqlalchemy.insert(
                request_postgres.SERVER_INSTANCES).values(**values))
        return instance_id

    with engine.begin() as connection:
        _insert_instance(connection, 'api')
        _insert_instance(connection, 'executor')
    assert request_postgres.ordered_capacity_admission_fleet_capable()

    with engine.begin() as connection:
        legacy = _insert_instance(connection, 'controller', protocol=1)
    assert not request_postgres.ordered_capacity_admission_fleet_capable()

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id == legacy).
            values(heartbeat_at=sqlalchemy.func.clock_timestamp() -
                   datetime.timedelta(seconds=(
                       request_postgres.
                       ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS +
                       1))))
    assert request_postgres.ordered_capacity_admission_fleet_capable()

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(request_postgres.SERVER_INSTANCES).values(
                    instance_id=uuid.uuid4(),
                    role='controller',
                    version='api015',
                    started_at=sqlalchemy.func.clock_timestamp(),
                    heartbeat_at=sqlalchemy.func.clock_timestamp(),
                    ready=False,
                    health_detail={},
                    supported_handlers=[],
                    supported_payload_versions={},
                    ordered_capacity_admission_capable=True))


def test_legacy_admission_waits_behind_service_promotion_lock(
        bound_request_database):
    engine, backend = bound_request_database
    request = _legacy_serve_launch_request('legacy-admission-after-scan')
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        with serve_state.service_replica_launch_authority_write_session(
                'gc-service') as (_, session):
            assert request_postgres._legacy_ordinary_launch_requests_drained_in_transaction(
                session.connection(), 'gc-service')
            future = executor.submit(lambda: asyncio.run(
                backend.create_if_not_exists_async(request)))

            deadline = time.monotonic() + 10
            while True:
                with engine.connect() as observer:
                    waiting = observer.execute(
                        sqlalchemy.text(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                            "WHERE datname = current_database() "
                            "AND pid <> pg_backend_pid() "
                            "AND state = 'active' "
                            "AND wait_event_type = 'Lock' "
                            "AND query LIKE "
                            "'SELECT pg_catalog.pg_advisory_xact_lock_shared%')"
                        )).scalar_one()
                if waiting:
                    break
                if future.done():
                    future.result()
                    pytest.fail('Legacy admission crossed the exclusive '
                                'service transition lock.')
                if time.monotonic() >= deadline:
                    pytest.fail('Legacy admission did not reach the shared '
                                'service transition lock.')
                time.sleep(0.01)
            assert not future.done()

        assert future.result(timeout=10)
    finally:
        executor.shutdown(wait=True, cancel_futures=True)

    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id)).mappings().one()
    assert row['handler_name'] == 'sky.execution:launch'


def test_legacy_drain_does_not_deadlock_queue_claimant_lock_upgrade(
        bound_request_database):
    engine, backend = bound_request_database
    request = _legacy_serve_launch_request('legacy-claim-during-drain')
    assert asyncio.run(backend.create_if_not_exists_async(request))

    worker = engine.connect()
    worker_transaction = worker.begin()
    worker.exec_driver_sql("SET LOCAL lock_timeout = '1s'")
    locked = worker.execute(
        sqlalchemy.select(
            request_postgres.QUEUE, request_postgres.REQUESTS).join(
                request_postgres.REQUESTS,
                request_postgres.REQUESTS.c.request_id ==
                request_postgres.QUEUE.c.request_id).where(
                    request_postgres.QUEUE.c.request_id ==
                    request.request_id).with_for_update()).mappings().one()
    execution_generation = int(locked['execution_generation'])

    scan_started = threading.Event()
    scanner_pid: list[int] = []

    def _scan_legacy_requests() -> bool:
        with engine.begin() as connection:
            scanner_pid.append(
                int(
                    connection.execute(
                        sqlalchemy.text(
                            'SELECT pg_backend_pid()')).scalar_one()))
            scan_started.set()
            return request_postgres._legacy_ordinary_launch_requests_drained_in_transaction(
                connection, 'gc-service')

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_scan_legacy_requests)
    try:
        assert scan_started.wait(timeout=10)
        deadline = time.monotonic() + 10
        while True:
            with engine.connect() as observer:
                waiting = observer.execute(
                    sqlalchemy.text("SELECT wait_event_type = 'Lock' "
                                    'FROM pg_stat_activity WHERE pid = :pid'), {
                                        'pid': scanner_pid[0]
                                    }).scalar_one_or_none()
            if waiting:
                break
            if future.done():
                future.result()
                pytest.fail('Legacy drain did not wait for the claimed row.')
            if time.monotonic() >= deadline:
                pytest.fail('Legacy drain did not reach the claimed row lock.')
            time.sleep(0.01)

        now = sqlalchemy.func.clock_timestamp()
        worker.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id).values(
                    status=requests.RequestStatus.CANCELLED.value,
                    terminal_cause=(
                        event_api_models.EventCause.EXPLICIT_CANCEL.value),
                    execution_quiescence_required=True,
                    execution_quiesced_generation=execution_generation,
                    execution_quiesced_at=now,
                    finished_at=now))
        worker.execute(
            sqlalchemy.delete(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == request.request_id))
        worker_transaction.commit()
        assert future.result(timeout=10)
    finally:
        if worker_transaction.is_active:
            worker_transaction.rollback()
        worker.close()
        executor.shutdown(wait=True, cancel_futures=True)


def test_legacy_drain_accepts_only_exact_projected_cleanup_tombstone(
        bound_request_database):
    engine, backend = bound_request_database
    request = _legacy_serve_launch_request('legacy-projected-cleanup')
    context = request.request_body.extra_launch_context
    context.update({
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY: 3,
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
            str(_GC_REPLICA_RECORD_ID),
        serve_constants.RESERVED_FILL_LAUNCH_KUBERNETES_CONTEXT_KEY: 'kubernetes-context-a',
        serve_constants.RESERVED_FILL_LAUNCH_PHYSICAL_CLUSTER_UID_KEY: 'cluster-uid-a',
    })
    assert asyncio.run(backend.create_if_not_exists_async(request))

    identity = _gc_legacy_identity(request.request_id)
    now = datetime.datetime.now(datetime.timezone.utc)
    with engine.begin() as connection:
        scope_id = (ordinary_launch_binding.
                    create_legacy_reconciliation_scope_in_connection(
                        connection, [identity],
                        reviewed_by='operator@example.com',
                        review_reason='Exact mixed-version request review.'))
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request.request_id).
            values(
                status=requests.RequestStatus.CANCELLED.value,
                terminal_cause=(
                    event_api_models.EventCause.EXECUTION_LEASE_EXPIRED.value),
                execution_generation=1,
                execution_quiescence_required=True,
                execution_quiesced_generation=None,
                execution_quiesced_at=None,
                finished_at=now))
        connection.execute(
            sqlalchemy.delete(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == request.request_id))

    terminated_at = now - datetime.timedelta(minutes=2)
    evidence = request_postgres.read_legacy_launch_request_evidence(
        identity,
        executor_terminated_at=terminated_at,
        executor_termination_evidence={
            'kind': 'kubernetes_container_terminated',
            'pod_uid': 'old-executor-pod-uid',
        })
    with engine.begin() as connection:
        ordinary_launch_binding.append_legacy_reconciliation_in_connection(
            connection,
            scope_id,
            identity,
            ordinary_launch_binding.LegacyReconciliationResolution.
            EFFECT_AMBIGUOUS,
            evidence,
            actor='reconciler',
            reason='The old executor published no request receipt.')

    provider_at = evidence.observed_request_at
    absent = dataclasses.replace(
        evidence,
        provider_evidence=ordinary_launch_binding.ProviderEvidence.ABSENT,
        provider_evidence_observed_at=provider_at,
        provider_evidence_payload={
            'cluster_name': identity.cluster_name,
            'kubernetes_context': identity.provider_context,
            'physical_cluster_uid': identity.provider_physical_resource_uid,
            'result': 'ABSENT',
        })
    with engine.begin() as connection:
        ordinary_launch_binding.append_legacy_reconciliation_in_connection(
            connection,
            scope_id,
            identity,
            ordinary_launch_binding.LegacyReconciliationResolution.
            CLEANUP_AUTHORIZED,
            absent,
            actor='reconciler',
            reason='Provider UID is absent after executor exit.')
        # Provider absence authorizes cleanup but does not yet prove that the
        # possible effect has been projected out of Serve state.
        assert not (request_postgres.
                    _legacy_ordinary_launch_requests_drained_in_transaction(
                        connection, 'gc-service'))
        assert (ordinary_launch_binding.
                project_legacy_replica_cleanup_in_connection(
                    connection,
                    scope_id,
                    identity,
                    actor='reconciler',
                    reason='Project the exact legacy replica tombstone.',
                    cleanup_completion_evidence={
                        'deleted_replica_record_id': str(_GC_REPLICA_RECORD_ID),
                        'operation': 'database-projection',
                    }))

    with engine.begin() as connection:
        stored = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id)).mappings().one()
        wrong_context = dict(context)
        wrong_context[
            serve_constants.RESERVED_FILL_LAUNCH_PHYSICAL_CLUSTER_UID_KEY] = (
                'different-cluster-uid')
        assert not (request_postgres.
                    _legacy_projected_cleanup_drains_request_in_transaction(
                        connection, stored, wrong_context, 'gc-service'))
        assert (request_postgres.
                _legacy_ordinary_launch_requests_drained_in_transaction(
                    connection, 'gc-service'))
    assert stored['execution_quiescence_required'] is True
    assert stored['execution_quiesced_generation'] is None
    assert stored['execution_quiesced_at'] is None


def test_bound_cancel_intent_does_not_wait_for_shared_provider_guard(
        bound_request_database):
    engine, _ = bound_request_database
    identity = _gc_binding_identity()
    with engine.begin() as connection:
        admission = ordinary_launch_binding.insert_or_get_locked(
            connection, identity)
    context = _gc_binding_context(identity, admission.launch_generation)
    authority = _gc_binding_authority()

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        with serve_state.service_replica_launch_authority_guard('gc-service'):
            future = executor.submit(
                request_postgres._commit_bound_ordinary_launch_cancel_intent,
                context, authority, 'replica-teardown')
            # Cancellation is the operation that interrupts an opaque provider
            # call.  It must commit while that call still owns the shared guard.
            assert future.result(timeout=2) is None
    finally:
        executor.shutdown(wait=True, cancel_futures=True)

    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == identity.association_id)).mappings().one()
    assert association['resolution'] == (
        ordinary_launch_binding.Resolution.CANCEL_REQUESTED.value)
    assert association['cancel_reason'] == 'replica-teardown'
    assert association['cancel_requested_at'] is not None


def test_demotion_retry_is_idempotent_only_for_exact_transition(
        bound_request_database):
    engine, _ = bound_request_database
    authority = ordinary_launch_binding.ControllerBindingAuthority(
        service_name='gc-service',
        service_hash='gc-service-hash',
        service_workspace='workspace-a',
        service_lifecycle_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.2',
        controller_incarnation=uuid.UUID(
            '33333333-3333-4333-8333-333333333333'),
        controller_owner_epoch=6,
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        binding_epoch=5)

    assert request_postgres.demote_ordinary_launch_binding_service(
        authority) == 6
    # The stale bound authority deliberately fails the wrapper's normal BOUND
    # barrier. Success proves the exact already-legacy path is evaluated first.
    assert request_postgres.demote_ordinary_launch_binding_service(
        authority) == 6

    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.
                ordinary_launch_binding_mode, serve_state_schema.services_table.
                c.ordinary_launch_binding_epoch).where(
                    serve_state_schema.services_table.c.name ==
                    'gc-service')).one()
    assert row.ordinary_launch_binding_mode == 'legacy'
    assert row.ordinary_launch_binding_epoch == 6


def test_demotion_waits_for_legacy_request_admitted_while_bound(
        bound_request_database):
    engine, backend = bound_request_database
    request = _legacy_serve_launch_request('legacy-admitted-while-bound')
    assert asyncio.run(backend.create_if_not_exists_async(request))
    authority = ordinary_launch_binding.ControllerBindingAuthority(
        service_name='gc-service',
        service_hash='gc-service-hash',
        service_workspace='workspace-a',
        service_lifecycle_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.2',
        controller_incarnation=_GC_CONTROLLER_ID,
        controller_owner_epoch=6,
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        binding_epoch=5)

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingUnavailable,
                       match='request/pin quiescence barrier'):
        request_postgres.demote_ordinary_launch_binding_service(authority)

    now = sqlalchemy.func.clock_timestamp()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id).values(
                    status=requests.RequestStatus.CANCELLED.value,
                    terminal_cause=(
                        event_api_models.EventCause.EXPLICIT_CANCEL.value),
                    execution_quiescence_required=True,
                    execution_quiesced_generation=(
                        request_postgres.REQUESTS.c.execution_generation),
                    execution_quiesced_at=now,
                    finished_at=now))
        connection.execute(
            sqlalchemy.delete(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == request.request_id))

    assert request_postgres.demote_ordinary_launch_binding_service(
        authority) == 6


def test_server_instance_lease_rejects_plugin_backend_subclasses(
        request_database, monkeypatch):
    engine, _ = request_database

    class PluginRequestBackend(request_postgres.PostgresRequestBackend):
        pass

    class PluginQueueFactory(request_postgres.PostgresQueueFactory):
        pass

    instance_id = str(uuid.uuid4())
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR, instance_id)
    monkeypatch.setenv(request_postgres.REQUEST_BACKEND_ENV_VAR,
                       request_postgres.POSTGRES_REQUEST_BACKEND)
    monkeypatch.setattr(storage, '_storage_backend', PluginRequestBackend())
    monkeypatch.setattr(queue_base, '_queue_backend_factory',
                        PluginQueueFactory())
    lease = request_postgres.ServerInstanceLease('api',
                                                 heartbeat_interval_seconds=60)
    lease.start()
    try:
        with engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(request_postgres.SERVER_INSTANCES).where(
                    request_postgres.SERVER_INSTANCES.c.instance_id ==
                    uuid.UUID(instance_id))).mappings().one()
        assert row['request_storage_backend'].endswith('.PluginRequestBackend')
        assert row['request_queue_backend'].endswith('.PluginQueueFactory')
        assert row['execution_quiescence_capable'] is False
        assert row['ordinary_launch_binding_capable'] is False
    finally:
        lease.stop()


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


def test_distributed_singleton_lock_session_stays_outside_transaction(
        request_database):
    engine, _ = request_database
    lock_name = f'test-singleton-transaction-{uuid.uuid4()}'
    holder_statement = sqlalchemy.text("""
        SELECT activity.pid,
               activity.state,
               activity.xact_start,
               activity.backend_xmin,
               activity.state_change
        FROM pg_stat_activity AS activity
        JOIN pg_locks AS held_lock ON held_lock.pid = activity.pid
        WHERE activity.datname = current_database()
          AND held_lock.locktype = 'advisory'
          AND held_lock.objsubid = 1
          AND held_lock.mode = 'ExclusiveLock'
          AND held_lock.granted
          AND ((held_lock.classid::bigint << 32) |
               held_lock.objid::bigint) =
              hashtextextended(CAST(:lock_name AS text), 0)
        ORDER BY activity.pid
        """)

    def read_holders() -> list[dict[str, object]]:
        with engine.connect() as connection:
            return [
                dict(row) for row in connection.execute(holder_statement, {
                    'lock_name': lock_name
                }).mappings().all()
            ]

    def backend_exists(pid: int) -> bool:
        with engine.connect() as connection:
            return bool(
                connection.execute(
                    sqlalchemy.text(
                        'SELECT EXISTS('
                        'SELECT 1 FROM pg_stat_activity WHERE pid = :pid)'), {
                            'pid': pid
                        }).scalar_one())

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

        async def wait_for_holder(
            *,
            expected_pid: int | None = None,
            after_state_change: datetime.datetime | None = None,
        ) -> dict[str, object]:
            deadline = time.monotonic() + 5
            last_rows: list[dict[str, object]] = []
            while time.monotonic() < deadline:
                last_rows = await asyncio.to_thread(read_holders)
                if len(last_rows) == 1:
                    row = last_rows[0]
                    pid_matches = (expected_pid is None or
                                   row['pid'] == expected_pid)
                    probe_matches = (after_state_change is None and
                                     row['state'] == 'idle')
                    if after_state_change is not None:
                        state_change = row['state_change']
                        assert isinstance(state_change, datetime.datetime)
                        probe_matches = (row['state'] == 'idle' and
                                         state_change > after_state_change)
                    if pid_matches and probe_matches:
                        return row
                await asyncio.sleep(0.01)
            raise AssertionError(
                f'Expected one matching singleton holder, got {last_rows!r}')

        def assert_transaction_idle(row: dict[str, object]) -> None:
            assert row['state'] == 'idle'
            assert row['xact_start'] is None
            assert row['backend_xmin'] is None

        first = asyncio.create_task(
            request_postgres.run_distributed_singleton(
                lock_name,
                factory('first'),
                retry_interval_seconds=0.05,
                connection_check_interval_seconds=0.05))
        second = asyncio.create_task(
            request_postgres.run_distributed_singleton(
                lock_name,
                factory('second'),
                retry_interval_seconds=0.05,
                connection_check_interval_seconds=0.05))
        tasks = {'first': first, 'second': second}
        try:
            winner = await asyncio.wait_for(started.get(), timeout=5)
            initial = await wait_for_holder()
            assert_transaction_idle(initial)
            winner_pid = int(initial['pid'])
            initial_state_change = initial['state_change']
            assert isinstance(initial_state_change, datetime.datetime)

            after_probe = await wait_for_holder(
                expected_pid=winner_pid,
                after_state_change=initial_state_change)
            assert_transaction_idle(after_probe)
            assert started.empty()

            winner_task = tasks[winner]
            standby_name = 'second' if winner == 'first' else 'first'
            standby_task = tasks[standby_name]
            winner_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await winner_task

            assert await asyncio.wait_for(started.get(),
                                          timeout=5) == (standby_name)
            successor = await wait_for_holder()
            assert_transaction_idle(successor)
            successor_pid = int(successor['pid'])
            assert successor_pid != winner_pid

            deadline = time.monotonic() + 5
            while (await asyncio.to_thread(backend_exists, winner_pid) and
                   time.monotonic() < deadline):
                await asyncio.sleep(0.01)
            assert not await asyncio.to_thread(backend_exists, winner_pid)

            successor_state_change = successor['state_change']
            assert isinstance(successor_state_change, datetime.datetime)
            successor_after_probe = await wait_for_holder(
                expected_pid=successor_pid,
                after_state_change=successor_state_change)
            assert_transaction_idle(successor_after_probe)
            assert started.empty()

            standby_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await standby_task
        finally:
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

    asyncio.run(exercise())


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
        first_capability = first.origin_capability
        assert len(first_capability) == 43
        with engine.connect() as connection:
            stored_digest = connection.execute(
                sqlalchemy.select(request_postgres.CONTROLLER_LEADERSHIP.c.
                                  origin_capability_sha256)).scalar_one()
        assert stored_digest == controller_capability.digest(first_capability)
        assert request_postgres.controller_origin_capability_is_current(
            first_id, 1, first_capability)
        guessed_capability = ('A' if first_capability[0] != 'A' else
                              'B') + first_capability[1:]
        assert not request_postgres.controller_origin_capability_is_current(
            first_id, 1, guessed_capability)
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
        assert second.origin_capability != first_capability
        assert request_postgres.controller_origin_capability_is_current(
            second_id, 2, second.origin_capability)
        assert not request_postgres.controller_origin_capability_is_current(
            second_id, 2, first_capability)
        assert request_postgres.controller_leadership_is_current(second_id, 2)
        assert not request_postgres.controller_leadership_is_current(
            first_id, 1)
    finally:
        first.release()
        second.release()


def test_stale_controller_cannot_retire_legacy_daemon_rows(
        request_database, monkeypatch):
    engine, backend = request_database
    first_id = str(uuid.uuid4())
    second_id = str(uuid.uuid4())
    first = _controller_leader(engine, monkeypatch, first_id)
    second = request_postgres.ControllerLeaderLease(second_id)
    daemon_ids = sorted(daemons.LEGACY_REQUEST_DAEMON_IDS)
    first_request = _request(daemon_ids[0])
    normal_request = _request('normal-row')
    assert asyncio.run(backend.create_if_not_exists_async(first_request))
    assert asyncio.run(backend.create_if_not_exists_async(normal_request))
    try:
        monkeypatch.setenv(request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR,
                           first_id)
        monkeypatch.setenv(request_postgres.CONTROLLER_GENERATION_ENV_VAR,
                           str(first.generation))
        assert backend.retire_legacy_internal_daemon_rows() == 1
        assert backend.retire_legacy_internal_daemon_rows() == 0
        assert backend.get_request('normal-row') is not None
        second_request = _request(daemon_ids[1])
        assert asyncio.run(backend.create_if_not_exists_async(second_request))

        lock_backend_pid = first.backend_pid()
        assert lock_backend_pid is not None
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'),
                {'pid': lock_backend_pid})
        assert second.try_acquire()

        with pytest.raises(RuntimeError, match='leadership changed'):
            backend.retire_legacy_internal_daemon_rows()
        with pytest.raises(RuntimeError, match='leadership changed'):
            request_postgres.fence_stale_controller_claims(
                first_id, first.generation)

        monkeypatch.setenv(request_postgres.CONTROLLER_INSTANCE_ID_ENV_VAR,
                           second_id)
        monkeypatch.setenv(request_postgres.CONTROLLER_GENERATION_ENV_VAR,
                           str(second.generation))
        assert backend.retire_legacy_internal_daemon_rows() == 1
        assert backend.get_request(daemon_ids[1]) is None
        assert backend.get_request('normal-row') is not None
    finally:
        first.release()
        second.release()


def test_legacy_daemon_retirement_covers_all_states_and_only_allowlist(
        request_database, monkeypatch):
    engine, backend = request_database
    leader = _controller_leader(engine, monkeypatch, backend.instance_id)
    daemon_ids = sorted(daemons.LEGACY_REQUEST_DAEMON_IDS)[:3]
    ordinary_suffix_id = 'user-selected-daemon'
    try:
        assert leader.generation is not None
        controller_owner = (leader.instance_id, leader.generation)
        for request_id in (*daemon_ids, ordinary_suffix_id):
            assert asyncio.run(
                backend.create_if_not_exists_async(_request(request_id)))

        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id == daemon_ids[1]).
                values(status=requests.RequestStatus.RUNNING.value))
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    daemon_ids[2]).values(
                        status=requests.RequestStatus.SUCCEEDED.value,
                        terminal_cause='handler_succeeded'))
            request_postgres.insert_request_retention_pin_in_transaction(
                connection, daemon_ids[2], 'legacy-daemon-test', uuid.uuid4())

        assert backend.retire_legacy_internal_daemon_rows(
            controller_owner=controller_owner) == len(daemon_ids)
        assert backend.retire_legacy_internal_daemon_rows(
            controller_owner=controller_owner) == 0
        for request_id in daemon_ids:
            assert backend.get_request(request_id) is None
        assert backend.get_request(ordinary_suffix_id) is not None
        with engine.connect() as connection:
            assert connection.execute(
                sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                                 ).select_from(request_postgres.QUEUE).where(
                                     request_postgres.QUEUE.c.request_id.in_(
                                         daemon_ids))).scalar_one() == 0
            assert connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(request_postgres.REQUEST_RETENTION_PINS).where(
                    request_postgres.REQUEST_RETENTION_PINS.c.request_id.in_(
                        daemon_ids))).scalar_one() == 0
    finally:
        leader.release()


def test_role_scoped_queues_isolate_normal_and_controller_claims(
        request_database, monkeypatch):
    engine, fixture_backend = request_database
    instance_id = str(uuid.uuid4())
    leader = _controller_leader(engine, monkeypatch, instance_id)
    # Exercise both execution-class views in the compatibility process.  A
    # dedicated controller role intentionally advertises no normal handlers.
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'all')
    backend = request_postgres.PostgresRequestBackend()
    try:
        normal_request = _request('normal-class')
        controller_request = _controller_request('controller-class')
        assert asyncio.run(
            fixture_backend.create_if_not_exists_async(normal_request))
        assert asyncio.run(
            fixture_backend.create_if_not_exists_async(controller_request))

        normal_queue = request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset({registry.ExecutionClass.NORMAL.value}),
            supported_handler_names=frozenset({
                registry.registration_for_handler(
                    normal_request.entrypoint).name
            }))
        normal_item = normal_queue.get()
        assert normal_item is not None
        assert normal_item.request_id == 'normal-class'
        assert normal_queue.get() is None

        controller_queue = request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=leader.generation,
            supported_handler_names=frozenset({
                registry.registration_for_handler(
                    controller_request.entrypoint).name
            }))
        controller_item = controller_queue.get()
        assert controller_item is not None
        assert controller_item.request_id == 'controller-class'
        assert backend.try_mark_running(controller_item.request_id, 1234,
                                        controller_item.execution_generation,
                                        controller_item.claim_token, 424242)

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


@pytest.mark.parametrize(
    ('request_factory', 'legacy_handler', 'authorized_handler', 'body_type'),
    [
        (_authorized_serve_status_request, serve_core.status,
         serve_core.authorized_status, payloads.ServeAuthorizedStatusBody),
        (_authorized_serve_placement_request, serve_core.placement,
         serve_core.authorized_placement,
         payloads.ServeAuthorizedPlacementBody),
    ],
    ids=['status', 'placement'],
)
def test_old_controller_cannot_claim_authorized_serve_read(
        request_database, monkeypatch, request_factory, legacy_handler,
        authorized_handler, body_type):
    """New API rows wait for a worker that understands their scope."""
    engine, fixture_backend = request_database
    leader = _controller_leader(engine, monkeypatch, str(uuid.uuid4()))
    try:
        request = request_factory(
            f'authorized-serve-{body_type.__name__.lower()}-capability')
        assert asyncio.run(fixture_backend.create_if_not_exists_async(request))

        legacy_handler_name = registry.registration_for_handler(
            legacy_handler).name
        legacy_queue = request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=leader.generation,
            supported_handler_names=frozenset({legacy_handler_name}),
        )
        assert legacy_queue.get() is None
        persisted = request_postgres.PostgresRequestBackend().get_request(
            request.request_id)
        assert persisted is not None
        assert persisted.status is requests.RequestStatus.PENDING
        assert type(persisted.request_body) is body_type
        assert persisted.request_body.authorized_owner_user_id == 'owner-a'

        authorized_handler_name = registry.registration_for_handler(
            authorized_handler).name
        authorized_queue = request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=leader.generation,
            supported_handler_names=frozenset({authorized_handler_name}),
        )
        claimed = authorized_queue.get()
        assert claimed is not None
        assert claimed.request_id == request.request_id
    finally:
        leader.release()


@pytest.mark.parametrize(
    ('request_factory', 'legacy_handler', 'body_type', 'payload_name'),
    [
        (_legacy_serve_status_request, serve_core.status,
         payloads.ServeStatusBody,
         'sky.server.requests.payloads:ServeStatusBody'),
        (_legacy_serve_placement_request, serve_core.placement,
         payloads.ServePlacementBody,
         'sky.server.requests.payloads:ServePlacementBody'),
    ],
    ids=['status', 'placement'],
)
def test_new_controller_claims_legacy_scoped_serve_read(
        request_database, monkeypatch, request_factory, legacy_handler,
        body_type, payload_name):
    """Old API rows retain their scope when claimed by a new worker."""
    engine, fixture_backend = request_database
    leader = _controller_leader(engine, monkeypatch, str(uuid.uuid4()))
    try:
        request = request_factory(
            f'legacy-serve-{body_type.__name__.lower()}-capability')
        durable_values = request.durable_values()
        assert durable_values['payload_type'] == payload_name
        assert durable_values['payload_json'][
            'authorized_owner_user_id'] == 'owner-a'
        assert asyncio.run(fixture_backend.create_if_not_exists_async(request))

        legacy_handler_name = registry.registration_for_handler(
            legacy_handler).name
        legacy_queue = request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=leader.generation,
            supported_handler_names=frozenset({legacy_handler_name}),
        )
        claimed = legacy_queue.get()
        assert claimed is not None
        assert claimed.request_id == request.request_id
        restored = fixture_backend.get_request(claimed.request_id)
        assert restored is not None
        assert type(restored.request_body) is body_type
        assert restored.request_body.authorized_owner_user_id == 'owner-a'
        assert restored.request_body.to_kwargs(
        )['authorized_owner_user_id'] == 'owner-a'
    finally:
        leader.release()


def test_all_mode_mixed_queue_fences_only_controller_claims(
        request_database, monkeypatch):
    engine, fixture_backend = request_database
    instance_id = fixture_backend.instance_id
    leader = _controller_leader(engine, monkeypatch, instance_id)
    backend = request_postgres.PostgresRequestBackend()
    first_generation = leader.generation
    assert first_generation is not None
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'all')
    try:
        controller_request = _controller_request('all-mode-controller')
        assert asyncio.run(
            fixture_backend.create_if_not_exists_async(controller_request))
        queue = request_postgres.PostgresQueueBackend(
            'short', controller_generation=first_generation)
        controller_item = queue.get()
        assert controller_item is not None
        assert controller_item.request_id == controller_request.request_id
        assert backend.try_mark_running(controller_item.request_id, 1234,
                                        controller_item.execution_generation,
                                        controller_item.claim_token, 424242)
        restored = backend.get_request(controller_item.request_id)
        assert restored.controller_generation == first_generation
        assert restored.worker_instance_id == instance_id

        claim = storage.ExecutionClaim(controller_item.request_id,
                                       controller_item.execution_generation,
                                       controller_item.claim_token)
        leader.release()

        assert not backend.heartbeat_claim(claim)
        stale_context = storage.activate_execution_claim(
            claim.request_id, claim.execution_generation, claim.claim_token)
        try:
            assert not backend.set_request_finished(
                claim.request_id, requests.RequestStatus.SUCCEEDED, result=[])
        finally:
            storage.deactivate_execution_claim(stale_context)

        stale_request = _controller_request('all-mode-stale-controller')
        assert asyncio.run(
            fixture_backend.create_if_not_exists_async(stale_request))
        assert queue.get() is None

        normal_request = _request('all-mode-normal')
        assert asyncio.run(
            fixture_backend.create_if_not_exists_async(normal_request))
        normal_item = request_postgres.PostgresQueueBackend('short').get()
        assert normal_item is not None
        assert normal_item.request_id == normal_request.request_id
        normal_restored = backend.get_request(normal_item.request_id)
        assert normal_restored.controller_generation is None
    finally:
        leader.release()


def test_all_mode_explicit_controller_queue_requires_generation(
        request_database, monkeypatch):
    _, backend = request_database
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'all')

    with pytest.raises(ValueError, match='active controller generation'):
        request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}))
    with pytest.raises(RuntimeError, match='active controller generation'):
        backend.retire_legacy_internal_daemon_rows()
    with pytest.raises(RuntimeError, match='active controller generation'):
        backend.recover_on_startup()


def test_all_mode_startup_fence_requeues_null_generation_claim(
        request_database, monkeypatch):
    engine, fixture_backend = request_database
    leader = _controller_leader(engine, monkeypatch,
                                fixture_backend.instance_id)
    backend = request_postgres.PostgresRequestBackend()
    assert leader.generation is not None
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'all')
    try:
        request = _controller_request('all-mode-null-generation',
                                      replayable=True)
        assert asyncio.run(fixture_backend.create_if_not_exists_async(request))
        queue = request_postgres.PostgresQueueBackend(
            'short', controller_generation=leader.generation)
        legacy_item = queue.get()
        assert legacy_item is not None
        assert legacy_item.claim_token is not None
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    legacy_item.request_id).values(controller_generation=None))

        assert not backend.try_mark_running(legacy_item.request_id, 1234,
                                            legacy_item.execution_generation,
                                            legacy_item.claim_token, 424242)
        assert request_postgres.fence_stale_controller_claims(
            leader.instance_id, leader.generation) == {
                'replayed': 1,
                'interrupted': 0,
            }
        _assert_execution_claim_requeued(engine, legacy_item.request_id)

        replacement = queue.get()
        assert replacement is not None
        assert replacement.request_id == legacy_item.request_id
        assert (replacement.execution_generation ==
                legacy_item.execution_generation + 1)
        restored = backend.get_request(replacement.request_id)
        assert restored.controller_generation == leader.generation
        assert replacement.claim_token is not None
        assert backend.try_mark_running(replacement.request_id, 1234,
                                        replacement.execution_generation,
                                        replacement.claim_token, 424242)
        replacement_claim = storage.ExecutionClaim(
            replacement.request_id, replacement.execution_generation,
            replacement.claim_token, replacement.worker_instance_id)
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    replacement.request_id).values(controller_generation=None))
        assert not backend.heartbeat_claim(replacement_claim)
        assert not backend.handoff_execution_retry(replacement_claim,
                                                   'must stay fenced', 1)
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
                                        item.claim_token, 424242)
        signal_exact = mock.Mock(return_value=True)
        monkeypatch.setattr(request_postgres, '_signal_exact_executor_process',
                            signal_exact)

        assert backend.kill_requests([item.request_id]) == [item.request_id]

        signal_exact.assert_called_once_with(1234, 424242,
                                             request_postgres.signal.SIGTERM)
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
                                              item.claim_token, 424242)
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


def test_controller_handoff_retains_old_owner_interrupt_and_receipt_address(
        request_database, monkeypatch):
    """Takeover revokes a mutator without erasing its local stop handshake."""
    engine, fixture_backend = request_database
    first_id = str(uuid.uuid4())
    second_id = str(uuid.uuid4())
    first = _controller_leader(engine, monkeypatch, first_id)
    first_backend = request_postgres.PostgresRequestBackend()
    second = request_postgres.ControllerLeaderLease(second_id)
    request_id = 'controller-takeover-quiescence-handshake'
    try:
        assert asyncio.run(
            fixture_backend.create_if_not_exists_async(
                _controller_request(request_id)))
        queue = request_postgres.PostgresQueueBackend(
            'short',
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=first.generation)
        item = queue.get()
        assert item is not None
        assert item.claim_token is not None
        assert item.worker_instance_id == first_id
        assert first_backend.try_mark_running(item.request_id, 1234,
                                              item.execution_generation,
                                              item.claim_token, 424242)
        claim = storage.ExecutionClaim(item.request_id,
                                       item.execution_generation,
                                       item.claim_token,
                                       item.worker_instance_id)

        lock_backend_pid = first.backend_pid()
        assert lock_backend_pid is not None
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'),
                {'pid': lock_backend_pid})
        assert not first.heartbeat()
        assert second.try_acquire()
        assert second.generation == 2
        assert request_postgres.fence_stale_controller_claims(
            second_id, second.generation) == {
                'replayed': 0,
                'interrupted': 1,
            }

        with engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    request_id)).mappings().one()
            queue_count = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count(  # pylint: disable=not-callable
                    )).select_from(request_postgres.QUEUE).where(
                        request_postgres.QUEUE.c.request_id ==
                        request_id)).scalar_one()
        assert row['status'] == requests.RequestStatus.CANCELLED.value
        assert row['terminal_cause'] == (
            event_api_models.EventCause.CONTROLLER_LEADERSHIP_LOST.value)
        assert row['cancel_requested_at'] is not None
        assert row['execution_quiescence_required'] is True
        assert row['execution_quiesced_generation'] is None
        assert row['execution_quiesced_at'] is None
        assert row['pid'] == 1234
        assert row['claim_token'] == uuid.UUID(item.claim_token)
        assert row['worker_instance_id'] == uuid.UUID(first_id)
        assert row['controller_generation'] == first.generation
        assert row['lease_expires_at'] is not None
        assert row['heartbeat_at'] is not None
        assert queue_count == 0

        signal_exact = mock.Mock(return_value=True)
        monkeypatch.setattr(request_postgres, '_signal_exact_executor_process',
                            signal_exact)
        assert first_backend.interrupt_cancelled_claim(claim)
        signal_exact.assert_called_once_with(1234, 424242,
                                             request_postgres.signal.SIGTERM)
        with engine.connect() as connection:
            delivered = connection.execute(
                sqlalchemy.select(
                    request_postgres.REQUESTS.c.cancel_acknowledged_at,
                    request_postgres.REQUESTS.c.execution_quiesced_generation,
                    request_postgres.REQUESTS.c.execution_quiesced_at).where(
                        request_postgres.REQUESTS.c.request_id ==
                        request_id)).one()
        assert delivered.cancel_acknowledged_at is not None
        assert delivered.execution_quiesced_generation is None
        assert delivered.execution_quiesced_at is None

        assert first_backend.acknowledge_execution_quiescence(claim)
        assert first_backend.acknowledge_execution_quiescence(claim)
        with engine.connect() as connection:
            receipt = connection.execute(
                sqlalchemy.select(
                    request_postgres.REQUESTS.c.execution_quiesced_generation,
                    request_postgres.REQUESTS.c.execution_quiesced_at,
                    request_postgres.REQUESTS.c.pid,
                    request_postgres.REQUESTS.c.claim_token,
                    request_postgres.REQUESTS.c.worker_instance_id,
                    request_postgres.REQUESTS.c.lease_expires_at).where(
                        request_postgres.REQUESTS.c.request_id ==
                        request_id)).one()
        assert receipt.execution_quiesced_generation == item.execution_generation
        assert receipt.execution_quiesced_at is not None
        assert receipt.pid == 1234
        assert receipt.claim_token == uuid.UUID(item.claim_token)
        assert receipt.worker_instance_id == uuid.UUID(first_id)
        assert receipt.lease_expires_at is not None
    finally:
        first.release()
        second.release()


def test_controller_handoff_requeues_reconcilable_work_after_exact_receipt(
        request_database, monkeypatch):
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
                                              first_item.claim_token, 424242)

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

        # Replayability does not make loss of the old controller session into
        # execution stop proof. Keep its exact claim and delivery until its
        # wrapper publishes the generation-bound receipt.
        with engine.connect() as connection:
            blocked = connection.execute(
                sqlalchemy.select(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    first_item.request_id)).mappings().one()
            delivery = connection.execute(
                sqlalchemy.select(request_postgres.QUEUE).where(
                    request_postgres.QUEUE.c.request_id ==
                    first_item.request_id)).mappings().one()
        assert blocked['status'] == requests.RequestStatus.RUNNING.value
        assert blocked['cancel_requested_at'] is not None
        assert blocked['execution_quiescence_required'] is True
        assert blocked['execution_quiesced_generation'] is None
        assert blocked['execution_quiesced_at'] is None
        assert blocked['pid'] == 1234
        assert blocked['claim_token'] == uuid.UUID(first_item.claim_token)
        assert blocked['worker_instance_id'] == uuid.UUID(first_id)
        assert blocked['controller_generation'] == first.generation
        assert blocked['lease_expires_at'] is not None
        assert blocked['heartbeat_at'] is not None
        assert delivery['delivery_state'] == 'claimed'
        assert delivery['claim_generation'] == first_item.execution_generation

        monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                           second_id)
        second_queue = request_postgres.PostgresQueueBackend(
            request.schedule_type.value,
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=second.generation)
        assert second_queue.get() is None

        claim = storage.ExecutionClaim(first_item.request_id,
                                       first_item.execution_generation,
                                       first_item.claim_token, first_id)
        context = storage.activate_execution_claim(
            first_item.request_id, first_item.execution_generation,
            first_item.claim_token)
        try:
            assert not first_backend.set_request_finished(
                first_item.request_id,
                requests.RequestStatus.SUCCEEDED,
                result=[])
        finally:
            storage.deactivate_execution_claim(context)
        assert first_backend.acknowledge_execution_quiescence(claim)
        with engine.connect() as connection:
            released = connection.execute(
                sqlalchemy.select(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    first_item.request_id)).mappings().one()
            delivery = connection.execute(
                sqlalchemy.select(request_postgres.QUEUE).where(
                    request_postgres.QUEUE.c.request_id ==
                    first_item.request_id)).mappings().one()
        assert released['status'] == requests.RequestStatus.WAITING.value
        assert released['terminal_cause'] is None
        assert released['claim_token'] is None
        assert released['worker_instance_id'] is None
        assert released['controller_generation'] is None
        assert released['cancel_requested_at'] is None
        assert not released['execution_quiescence_required']
        assert delivery['delivery_state'] == 'queued'
        assert delivery['claim_generation'] is None
        second_item = second_queue.get()
        assert second_item is not None
        assert second_item.request_id == first_item.request_id
        assert (second_item.execution_generation ==
                first_item.execution_generation + 1)
        assert second_item.claim_token != first_item.claim_token
    finally:
        first.release()
        second.release()


def test_controller_handoff_requeues_pending_pidless_claim_pre_effect(
        request_database, monkeypatch):
    engine, fixture_backend = request_database
    first_id = str(uuid.uuid4())
    second_id = str(uuid.uuid4())
    first = _controller_leader(engine, monkeypatch, first_id)
    first_backend = request_postgres.PostgresRequestBackend()
    second = request_postgres.ControllerLeaderLease(second_id)
    try:
        request = _controller_request('pending-controller-handoff',
                                      replayable=True)
        assert asyncio.run(fixture_backend.create_if_not_exists_async(request))
        queue = request_postgres.PostgresQueueBackend(
            request.schedule_type.value,
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=first.generation)
        item = queue.get()
        assert item is not None
        # Deliberately stop before the child commits try_mark_running().
        assert first_backend.get_request(
            item.request_id).status is requests.RequestStatus.PENDING

        lock_backend_pid = first.backend_pid()
        assert lock_backend_pid is not None
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'),
                {'pid': lock_backend_pid})
        assert second.try_acquire()
        assert request_postgres.fence_stale_controller_claims(
            second_id, second.generation) == {
                'replayed': 1,
                'interrupted': 0,
            }

        _assert_execution_claim_requeued(engine, item.request_id)
        assert not first_backend.try_mark_running(
            item.request_id, 1234, item.execution_generation, item.claim_token,
            424242)
    finally:
        first.release()
        second.release()


def test_controller_handoff_sweep_consumes_receipt_published_first(
        request_database, monkeypatch):
    engine, fixture_backend = request_database
    first_id = str(uuid.uuid4())
    second_id = str(uuid.uuid4())
    first = _controller_leader(engine, monkeypatch, first_id)
    second = request_postgres.ControllerLeaderLease(second_id)
    try:
        request = _controller_request('receipt-first-controller-action',
                                      replayable=True)
        assert asyncio.run(fixture_backend.create_if_not_exists_async(request))
        first_queue = request_postgres.PostgresQueueBackend(
            request.schedule_type.value,
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=first.generation)
        first_item = first_queue.get()
        assert first_item is not None
        first_backend = request_postgres.PostgresRequestBackend()
        assert first_backend.try_mark_running(first_item.request_id, 1234,
                                              first_item.execution_generation,
                                              first_item.claim_token, 424242)

        # Model the wrapper receipt committing immediately before the new
        # controller's revocation transaction. The fence must consume it in
        # the same shared reducer instead of leaving a claimed delivery stuck.
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    first_item.request_id).values(
                        execution_quiesced_generation=(
                            first_item.execution_generation),
                        execution_quiesced_at=(
                            sqlalchemy.func.clock_timestamp())))

        lock_backend_pid = first.backend_pid()
        assert lock_backend_pid is not None
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'),
                {'pid': lock_backend_pid})
        assert second.try_acquire()
        assert request_postgres.fence_stale_controller_claims(
            second_id, second.generation) == {
                'replayed': 1,
                'interrupted': 0,
            }

        with engine.connect() as connection:
            released = connection.execute(
                sqlalchemy.select(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    first_item.request_id)).mappings().one()
            delivery = connection.execute(
                sqlalchemy.select(request_postgres.QUEUE).where(
                    request_postgres.QUEUE.c.request_id ==
                    first_item.request_id)).mappings().one()
        assert released['status'] == requests.RequestStatus.WAITING.value
        assert released['terminal_cause'] is None
        assert released['claim_token'] is None
        assert released['worker_instance_id'] is None
        assert not released['execution_quiescence_required']
        assert delivery['delivery_state'] == 'queued'
        assert delivery['claim_generation'] is None

        monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                           second_id)
        second_queue = request_postgres.PostgresQueueBackend(
            request.schedule_type.value,
            execution_classes=frozenset(
                {registry.ExecutionClass.CONTROLLER.value}),
            controller_generation=second.generation)
        second_item = second_queue.get()
        assert second_item is not None
        assert (second_item.execution_generation ==
                first_item.execution_generation + 1)
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


def test_managed_job_origin_mapping_round_trip_without_database() -> None:
    controller_instance_id = uuid.uuid4()
    slot_attempt = uuid.uuid4()
    request = _request('managed-job-origin-mapping-round-trip')
    request.managed_job_id = 42
    request.managed_job_controller_instance_id = str(controller_instance_id)
    request.managed_job_controller_generation = 7
    request.managed_job_controller_slot_id = 3
    request.managed_job_controller_slot_attempt = str(slot_attempt)

    db_values = request_postgres._request_values_for_db(request)
    assert db_values['managed_job_controller_instance_id'] == (
        controller_instance_id)
    assert db_values['managed_job_controller_slot_attempt'] == slot_attempt
    restored = request_postgres._request_from_mapping(db_values)
    assert restored.managed_job_id == 42
    assert restored.managed_job_controller_instance_id == str(
        controller_instance_id)
    assert restored.managed_job_controller_generation == 7
    assert restored.managed_job_controller_slot_id == 3
    assert restored.managed_job_controller_slot_attempt == str(slot_attempt)


def test_managed_job_origin_round_trip_and_database_constraints(
        request_database, monkeypatch):
    engine, backend = request_database
    controller_instance_id = uuid.UUID(backend.instance_id)
    slot_attempt = uuid.uuid4()
    leader = _controller_leader(engine, monkeypatch,
                                str(controller_instance_id))
    try:
        _seed_managed_job_attempt(engine,
                                  leader,
                                  job_id=42,
                                  slot_id=3,
                                  slot_attempt=slot_attempt)
        request = _managed_job_request('managed-job-origin-round-trip',
                                       leader,
                                       job_id=42,
                                       slot_id=3,
                                       slot_attempt=slot_attempt)

        db_values = request_postgres._request_values_for_db(request)
        assert db_values['managed_job_controller_instance_id'] == (
            controller_instance_id)
        assert db_values['managed_job_controller_slot_attempt'] == slot_attempt
        assert asyncio.run(backend.create_if_not_exists_async(request))

        restored = backend.get_request(request.request_id)
        assert restored is not None
        assert restored.managed_job_id == 42
        assert restored.managed_job_controller_instance_id == str(
            controller_instance_id)
        assert restored.managed_job_controller_generation == leader.generation
        assert restored.managed_job_controller_slot_id == 3
        assert restored.managed_job_controller_slot_attempt == str(slot_attempt)

        with pytest.raises(sqlalchemy.exc.IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.update(request_postgres.REQUESTS).where(
                        request_postgres.REQUESTS.c.request_id ==
                        request.request_id).values(
                            managed_job_controller_slot_attempt=None))

        with pytest.raises(sqlalchemy.exc.IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.update(request_postgres.REQUESTS).where(
                        request_postgres.REQUESTS.c.request_id == request.
                        request_id).values(managed_job_controller_slot_id=-1))
    finally:
        leader.release()


@pytest.mark.parametrize(('schedule_state', 'quiescing'), [
    ('ALIVE', True),
    ('DONE', False),
])
def test_managed_job_request_creation_rejects_closed_attempt(
        request_database, monkeypatch, schedule_state, quiescing):
    engine, backend = request_database
    leader = _controller_leader(engine, monkeypatch, backend.instance_id)
    slot_attempt = uuid.uuid4()
    request_id = f'managed-job-create-closed-{schedule_state}-{quiescing}'
    try:
        _seed_managed_job_attempt(engine,
                                  leader,
                                  job_id=43,
                                  slot_id=4,
                                  slot_attempt=slot_attempt,
                                  schedule_state=schedule_state,
                                  quiescing=quiescing)
        request = _managed_job_request(request_id,
                                       leader,
                                       job_id=43,
                                       slot_id=4,
                                       slot_attempt=slot_attempt)
        with pytest.raises(storage.ManagedJobRequestQuiescenceError):
            asyncio.run(backend.create_if_not_exists_async(request))
        assert backend.get_request(request_id) is None
    finally:
        leader.release()


def test_managed_job_claim_terminalizes_stale_pre_effect_request(
        request_database, monkeypatch):
    engine, backend = request_database
    leader = _controller_leader(engine, monkeypatch, backend.instance_id)
    slot_attempt = uuid.uuid4()
    request_id = 'managed-job-stale-queued-claim'
    try:
        _seed_managed_job_attempt(engine,
                                  leader,
                                  job_id=44,
                                  slot_id=5,
                                  slot_attempt=slot_attempt)
        request = _managed_job_request(request_id,
                                       leader,
                                       job_id=44,
                                       slot_id=5,
                                       slot_attempt=slot_attempt)
        assert asyncio.run(backend.create_if_not_exists_async(request))
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    managed_job_state_schema.job_info_table).where(
                        managed_job_state_schema.job_info_table.c.spot_job_id ==
                        44).values(controller_slot_attempt=str(uuid.uuid4())))

        # The nested request uses an ordinary API handler. Its executor may
        # share this process in compatibility mode while the leader's dedicated
        # PostgreSQL lock session continues to prove outer authority.
        monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'all')
        assert request_postgres.PostgresQueueBackend('short').get() is None
        restored = backend.get_request(request_id)
        assert restored is not None
        assert restored.status is requests.RequestStatus.CANCELLED
        assert restored.execution_quiescence_required
        assert restored.execution_quiesced_generation == 0
        assert restored.execution_quiesced_at is not None
        assert request_postgres.PostgresQueueBackend('short').qsize() == 0
    finally:
        leader.release()


def test_managed_job_running_admission_revalidates_exact_attempt(
        request_database, monkeypatch):
    engine, backend = request_database
    leader = _controller_leader(engine, monkeypatch, backend.instance_id)
    slot_attempt = uuid.uuid4()
    request_id = 'managed-job-running-admission-revalidation'
    try:
        _seed_managed_job_attempt(engine,
                                  leader,
                                  job_id=45,
                                  slot_id=6,
                                  slot_attempt=slot_attempt)
        request = _managed_job_request(request_id,
                                       leader,
                                       job_id=45,
                                       slot_id=6,
                                       slot_attempt=slot_attempt)
        assert asyncio.run(backend.create_if_not_exists_async(request))
        monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'all')
        item = request_postgres.PostgresQueueBackend('short').get()
        assert item is not None
        assert item.claim_token is not None
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    managed_job_state_schema.job_info_table).where(
                        managed_job_state_schema.job_info_table.c.spot_job_id ==
                        45).values(controller_slot_attempt=str(uuid.uuid4())))

        assert not backend.try_mark_running(
            request_id, 1234, item.execution_generation, item.claim_token,
            424242)
        identity = (leader.instance_id, leader.generation, 6, str(slot_attempt))
        assert backend.quiesce_managed_job_slot_requests(identity,
                                                         timeout_seconds=0) == 1
        restored = backend.get_request(request_id)
        assert restored is not None
        assert restored.status is requests.RequestStatus.CANCELLED
        assert restored.execution_quiesced_generation == (
            item.execution_generation)
        assert restored.execution_quiesced_at is not None
    finally:
        leader.release()


def test_managed_job_first_slot_rollout_marks_request_free_legacy_jobs(
        request_database, monkeypatch):
    engine, backend = request_database
    leader = _controller_leader(engine, monkeypatch, backend.instance_id)
    old_instance_id = str(uuid.uuid4())
    try:
        _seed_legacy_managed_job(engine,
                                 job_id=46,
                                 controller_instance_id=old_instance_id,
                                 controller_generation=1)
        _seed_legacy_managed_job(engine, job_id=47)
        _seed_legacy_managed_job(engine, job_id=48, schedule_state='DONE')
        assert leader.generation is not None
        assert backend.quiesce_stale_managed_job_requests(
            (leader.instance_id, leader.generation), timeout_seconds=0) == 0
        with engine.connect() as connection:
            rows = connection.execute(
                sqlalchemy.select(
                    managed_job_state_schema.job_info_table.c.spot_job_id,
                    managed_job_state_schema.job_info_table.c.
                    controller_slot_quiescing).where(
                        managed_job_state_schema.job_info_table.c.spot_job_id.
                        in_([46, 47, 48
                            ])).order_by(managed_job_state_schema.
                                         job_info_table.c.spot_job_id)).all()
        assert rows == [(46, True), (47, True), (48, False)]
    finally:
        leader.release()


def test_managed_job_first_slot_rollout_rejects_correlated_request(
        request_database, monkeypatch):
    engine, backend = request_database
    leader = _controller_leader(engine, monkeypatch, backend.instance_id)
    old_instance_id = str(uuid.uuid4())
    old_attempt = uuid.uuid4()
    try:
        _seed_legacy_managed_job(engine,
                                 job_id=49,
                                 controller_instance_id=old_instance_id,
                                 controller_generation=1)
        correlated = _request('legacy-job-correlated', should_enqueue=False)
        correlated.managed_job_id = 49
        correlated.managed_job_controller_instance_id = old_instance_id
        correlated.managed_job_controller_generation = 1
        correlated.managed_job_controller_slot_id = 0
        correlated.managed_job_controller_slot_attempt = str(old_attempt)
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(request_postgres.REQUESTS).values(
                    **request_postgres._request_values_for_db(correlated)))

        assert leader.generation is not None
        with pytest.raises(storage.ManagedJobRequestQuiescenceError,
                           match='pre-slot managed jobs'):
            backend.quiesce_stale_managed_job_requests(
                (leader.instance_id, leader.generation), timeout_seconds=0)
        with engine.connect() as connection:
            quiescing = connection.execute(
                sqlalchemy.select(
                    managed_job_state_schema.job_info_table.c.
                    controller_slot_quiescing).where(
                        managed_job_state_schema.job_info_table.c.spot_job_id ==
                        49)).scalar_one()
        assert not quiescing
    finally:
        leader.release()


def test_managed_job_first_slot_rollout_requires_fresh_outer_generation(
        request_database, monkeypatch):
    engine, backend = request_database
    leader = _controller_leader(engine, monkeypatch, backend.instance_id)
    try:
        assert leader.generation is not None
        _seed_legacy_managed_job(engine,
                                 job_id=50,
                                 controller_instance_id=leader.instance_id,
                                 controller_generation=leader.generation)
        with pytest.raises(storage.ManagedJobRequestQuiescenceError,
                           match='unsafe prior controller identity'):
            backend.quiesce_stale_managed_job_requests(
                (leader.instance_id, leader.generation), timeout_seconds=0)
    finally:
        leader.release()


def test_managed_job_origin_storage_validation() -> None:
    partial = _request('managed-job-origin-partial')
    partial.managed_job_id = 42
    with pytest.raises(ValueError, match='all five fields'):
        request_postgres._request_values_for_db(partial)

    invalid_uuid = _request('managed-job-origin-invalid-uuid')
    invalid_uuid.managed_job_id = 42
    invalid_uuid.managed_job_controller_instance_id = 'not-a-uuid'
    invalid_uuid.managed_job_controller_generation = 7
    invalid_uuid.managed_job_controller_slot_id = 3
    invalid_uuid.managed_job_controller_slot_attempt = str(uuid.uuid4())
    with pytest.raises(
            ValueError,
            match='managed_job_controller_instance_id must be a UUID'):
        request_postgres._request_values_for_db(invalid_uuid)

    invalid_generation = _request('managed-job-origin-invalid-generation')
    invalid_generation.managed_job_id = 42
    invalid_generation.managed_job_controller_instance_id = str(uuid.uuid4())
    invalid_generation.managed_job_controller_generation = 0
    invalid_generation.managed_job_controller_slot_id = 3
    invalid_generation.managed_job_controller_slot_attempt = str(uuid.uuid4())
    with pytest.raises(ValueError,
                       match='managed_job_controller_generation must be'):
        request_postgres._request_values_for_db(invalid_generation)


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


def test_expired_mutating_claim_retains_exact_interrupt_and_receipt_address(
        request_database, monkeypatch):
    """Expiry revokes execution without erasing its quiescence handshake."""
    engine, backend = request_database
    request_id = 'expired-claim-quiescence-handshake'
    request = _request(request_id)
    request.name = 'sky.stop'
    request.entrypoint = core.stop
    request.request_body = payloads.StopOrDownBody(cluster_name='cluster')
    assert asyncio.run(backend.create_if_not_exists_async(request))
    item = _claim(backend, request_id)
    assert item.claim_token is not None
    assert item.worker_instance_id is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    _expire_claim(engine, request_id)

    queue = request_postgres.PostgresQueueBackend('short')
    with engine.begin() as connection:
        queue._reap_expired_claims(connection)

    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id)).mappings().one()
        queue_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count(  # pylint: disable=not-callable
                )).select_from(request_postgres.QUEUE).where(
                    request_postgres.QUEUE.c.request_id ==
                    request_id)).scalar_one()
    assert row['status'] == requests.RequestStatus.CANCELLED.value
    assert row['terminal_cause'] == (
        event_api_models.EventCause.EXECUTION_LEASE_EXPIRED.value)
    assert row['cancel_requested_at'] is not None
    assert row['execution_quiescence_required'] is True
    assert row['execution_quiesced_generation'] is None
    assert row['execution_quiesced_at'] is None
    assert row['pid'] == 1234
    assert row['claim_token'] == uuid.UUID(item.claim_token)
    assert row['worker_instance_id'] == uuid.UUID(item.worker_instance_id)
    assert row['lease_expires_at'] is not None
    assert queue_count == 0

    signal_exact = mock.Mock(return_value=True)
    monkeypatch.setattr(request_postgres, '_signal_exact_executor_process',
                        signal_exact)
    assert backend.interrupt_cancelled_claim(claim)
    signal_exact.assert_called_once_with(1234, 424242,
                                         request_postgres.signal.SIGTERM)
    # Signal delivery is not a receipt. Only the exact wrapper may publish
    # the generation-bound proof, and replaying that receipt is idempotent.
    with engine.connect() as connection:
        before_receipt = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.cancel_acknowledged_at,
                request_postgres.REQUESTS.c.execution_quiesced_generation,
                request_postgres.REQUESTS.c.execution_quiesced_at).
            where(request_postgres.REQUESTS.c.request_id == request_id)).one()
    assert before_receipt.cancel_acknowledged_at is not None
    assert before_receipt.execution_quiesced_generation is None
    assert before_receipt.execution_quiesced_at is None
    assert backend.acknowledge_execution_quiescence(claim)
    assert backend.acknowledge_execution_quiescence(claim)

    with engine.connect() as connection:
        receipt = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.execution_quiesced_generation,
                request_postgres.REQUESTS.c.execution_quiesced_at,
                request_postgres.REQUESTS.c.claim_token,
                request_postgres.REQUESTS.c.worker_instance_id).
            where(request_postgres.REQUESTS.c.request_id == request_id)).one()
    assert receipt.execution_quiesced_generation == item.execution_generation
    assert receipt.execution_quiesced_at is not None
    # Keep the exact tombstone after receipt so a duplicate finally-block write
    # remains an idempotent success until normal request GC removes the row.
    assert receipt.claim_token == uuid.UUID(item.claim_token)
    assert receipt.worker_instance_id == uuid.UUID(item.worker_instance_id)


def test_receipt_before_reaper_preserves_expired_mutating_claim_tombstone(
        request_database):
    """The wrapper receipt may win the lease-expiry terminalization race."""
    engine, backend = request_database
    request_id = 'expired-claim-receipt-before-reaper'
    request = _request(request_id)
    request.name = 'sky.stop'
    request.entrypoint = core.stop
    request.request_body = payloads.StopOrDownBody(cluster_name='cluster')
    assert asyncio.run(backend.create_if_not_exists_async(request))
    item = _claim(backend, request_id)
    assert item.claim_token is not None
    assert item.worker_instance_id is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    _expire_claim(engine, request_id)

    # The effect-bearing wrapper can finish after its lease expires but before
    # the queue sweep terminalizes the still-RUNNING request. Its exact receipt
    # is genuine and must survive the later reaper transaction.
    assert backend.acknowledge_execution_quiescence(claim)
    with engine.connect() as connection:
        receipt_before_reap = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.execution_quiesced_at).where(
                    request_postgres.REQUESTS.c.request_id ==
                    request_id)).scalar_one()

    queue = request_postgres.PostgresQueueBackend('short')
    with engine.begin() as connection:
        queue._reap_expired_claims(connection)

    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id)).mappings().one()
        queue_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count(  # pylint: disable=not-callable
                )).select_from(request_postgres.QUEUE).where(
                    request_postgres.QUEUE.c.request_id ==
                    request_id)).scalar_one()
    assert row['status'] == requests.RequestStatus.CANCELLED.value
    assert row['terminal_cause'] == (
        event_api_models.EventCause.EXECUTION_LEASE_EXPIRED.value)
    assert row['cancel_requested_at'] is not None
    assert row['execution_quiescence_required'] is True
    assert row['execution_quiesced_generation'] == item.execution_generation
    assert row['execution_quiesced_at'] == receipt_before_reap
    assert row['pid'] == 1234
    assert row['claim_token'] == uuid.UUID(item.claim_token)
    assert row['worker_instance_id'] == uuid.UUID(item.worker_instance_id)
    assert row['lease_expires_at'] is not None
    assert row['heartbeat_at'] is not None
    assert queue_count == 0
    assert backend.acknowledge_execution_quiescence(claim)


def test_expired_read_only_claim_replays_after_exact_receipt(request_database):
    engine, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(_event_request('replay-read-only')))
    first = _claim(backend, 'replay-read-only')
    _expire_claim(engine, first.request_id)
    queue = request_postgres.PostgresQueueBackend('short')

    assert queue.get() is None
    with engine.connect() as connection:
        blocked = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                first.request_id)).mappings().one()
        delivery = connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                first.request_id)).mappings().one()
    assert blocked['status'] == requests.RequestStatus.RUNNING.value
    assert blocked['cancel_requested_at'] is not None
    assert blocked['execution_quiescence_required'] is True
    assert blocked['execution_quiesced_generation'] is None
    assert blocked['execution_quiesced_at'] is None
    assert blocked['pid'] == 1234
    assert blocked['claim_token'] == uuid.UUID(first.claim_token)
    assert blocked['worker_instance_id'] == uuid.UUID(first.worker_instance_id)
    assert blocked['lease_expires_at'] is not None
    assert blocked['heartbeat_at'] is not None
    assert delivery['delivery_state'] == 'claimed'
    assert delivery['claim_generation'] == first.execution_generation

    claim = storage.ExecutionClaim(first.request_id, first.execution_generation,
                                   first.claim_token, first.worker_instance_id)
    assert not backend.heartbeat_claim(claim)
    context = storage.activate_execution_claim(first.request_id,
                                               first.execution_generation,
                                               first.claim_token)
    try:
        assert not backend.set_request_finished(
            first.request_id, requests.RequestStatus.SUCCEEDED, result=[])
    finally:
        storage.deactivate_execution_claim(context)
    with engine.connect() as connection:
        event_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count(  # pylint: disable=not-callable
                )).select_from(event_schema.RESOURCE_EVENTS).where(
                    event_schema.RESOURCE_EVENTS.c.source_request_id ==
                    first.request_id)).scalar_one()
    assert event_count == 0

    assert backend.acknowledge_execution_quiescence(claim)
    with engine.connect() as connection:
        released = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                first.request_id)).mappings().one()
        delivery = connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                first.request_id)).mappings().one()
        event_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count(  # pylint: disable=not-callable
                )).select_from(event_schema.RESOURCE_EVENTS).where(
                    event_schema.RESOURCE_EVENTS.c.source_request_id ==
                    first.request_id)).scalar_one()
    assert released['status'] == requests.RequestStatus.WAITING.value
    assert released['terminal_cause'] is None
    assert released['claim_token'] is None
    assert released['worker_instance_id'] is None
    assert released['cancel_requested_at'] is None
    assert not released['execution_quiescence_required']
    assert delivery['delivery_state'] == 'queued'
    assert delivery['claim_generation'] is None
    assert event_count == 0

    second = queue.get()
    assert second is not None
    assert second.request_id == first.request_id
    assert second.execution_generation == first.execution_generation + 1
    assert second.claim_token != first.claim_token


def test_expired_read_only_sweep_consumes_receipt_published_first(
        request_database):
    engine, backend = request_database
    request_id = 'replay-read-only-receipt-first'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    first = _claim(backend, request_id)
    _expire_claim(engine, request_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    execution_quiesced_generation=first.execution_generation,
                    execution_quiesced_at=(sqlalchemy.func.clock_timestamp())))

    queue = request_postgres.PostgresQueueBackend('short')
    with engine.begin() as connection:
        queue._reap_expired_claims(connection)
    with engine.connect() as connection:
        released = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id)).mappings().one()
        delivery = connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                request_id)).mappings().one()
    assert released['status'] == requests.RequestStatus.WAITING.value
    assert released['terminal_cause'] is None
    assert released['claim_token'] is None
    assert released['worker_instance_id'] is None
    assert released['cancel_requested_at'] is None
    assert not released['execution_quiescence_required']
    assert delivery['delivery_state'] == 'queued'
    assert delivery['claim_generation'] is None

    second = queue.get()
    assert second is not None
    assert second.request_id == request_id
    assert second.execution_generation == first.execution_generation + 1


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


def test_pending_durable_cancel_records_immediate_quiescence(request_database):
    _, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('pending-cancel')))
    inserted = backend.get_request('pending-cancel')
    assert not inserted.execution_quiescence_required
    assert inserted.execution_quiesced_generation is None
    assert inserted.execution_quiesced_at is None

    assert backend.kill_requests(['pending-cancel']) == ['pending-cancel']

    restored = backend.get_request('pending-cancel')
    assert restored.status is requests.RequestStatus.CANCELLED
    assert restored.execution_quiescence_required
    assert restored.execution_generation == 0
    assert restored.execution_quiesced_generation == 0
    assert restored.execution_quiesced_at is not None


@pytest.mark.parametrize(
    'status', [requests.RequestStatus.PENDING, requests.RequestStatus.WAITING])
def test_claimed_pidless_cancel_records_exact_quiescence(
        request_database, status):
    engine, backend = request_database
    request_id = f'claimed-pidless-{status.value.lower()}-cancel'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = request_postgres.PostgresQueueBackend('short').get()
    assert item is not None
    assert item.claim_token is not None
    if status is requests.RequestStatus.WAITING:
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    request_id).values(status=status.value))

    assert backend.kill_requests([request_id]) == [request_id]

    request_row, queue_row = _execution_claim_state(engine, request_id)
    assert request_row['status'] == requests.RequestStatus.CANCELLED.value
    assert request_row['pid'] is None
    assert request_row['execution_process_start_time_ticks'] is None
    assert request_row['execution_quiesced_generation'] == (
        item.execution_generation)
    assert request_row['execution_quiesced_at'] is not None
    assert not queue_row


def test_unclaimed_insert_opts_into_quiescence_only_when_claimed(
        request_database):
    _, backend = request_database
    request_id = 'quiescence-opt-in-on-claim'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))

    inserted = backend.get_request(request_id)
    assert not inserted.execution_quiescence_required
    assert inserted.execution_quiesced_generation is None
    assert inserted.execution_quiesced_at is None

    item = _claim(backend, request_id)

    claimed = backend.get_request(request_id)
    assert claimed.execution_quiescence_required
    assert claimed.execution_generation == item.execution_generation
    assert claimed.execution_quiesced_generation is None
    assert claimed.execution_quiesced_at is None


def test_execution_quiescence_candidates_use_cluster_and_required_predicate(
        request_database):
    engine, backend = request_database
    active = _request('candidate-active')
    active.cluster_name = 'target-cluster'
    required_terminal = _request('candidate-required-terminal')
    required_terminal.cluster_name = 'target-cluster'
    legacy_terminal = _request('candidate-legacy-terminal',
                               should_enqueue=False)
    legacy_terminal.cluster_name = 'target-cluster'
    other = _request('candidate-other')
    other.cluster_name = 'other-cluster'
    for request in (required_terminal, active, legacy_terminal, other):
        assert asyncio.run(backend.create_if_not_exists_async(request))
    _claim(backend, required_terminal.request_id)
    assert backend.kill_requests([required_terminal.request_id
                                 ]) == [required_terminal.request_id]
    assert backend.set_request_finished(legacy_terminal.request_id,
                                        requests.RequestStatus.SUCCEEDED,
                                        result=[])

    candidates = backend.query_requests(
        requests.RequestTaskFilter(cluster_names=['target-cluster'],
                                   execution_quiescence_candidates_only=True,
                                   sort=False))

    assert {request.request_id for request in candidates
           } == {active.request_id, required_terminal.request_id}

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                required_terminal.request_id).values(
                    execution_quiesced_generation=(
                        request_postgres.REQUESTS.c.execution_generation),
                    execution_quiesced_at=sqlalchemy.func.clock_timestamp()))
    candidates = backend.query_requests(
        requests.RequestTaskFilter(cluster_names=['target-cluster'],
                                   execution_quiescence_candidates_only=True,
                                   sort=False))
    assert {request.request_id for request in candidates} == {active.request_id}


def test_exact_request_id_filter_does_not_match_prefixes(request_database):
    _, backend = request_database
    for request_id in ('exact-filter', 'exact-filter-sibling', 'other'):
        assert asyncio.run(
            backend.create_if_not_exists_async(_request(request_id)))

    matched = backend.query_requests(
        requests.RequestTaskFilter(request_ids=['exact-filter'], sort=False))
    empty = backend.query_requests(
        requests.RequestTaskFilter(request_ids=[], sort=False))

    assert [request.request_id for request in matched] == ['exact-filter']
    assert empty == []


def test_scalar_status_projection_does_not_decode_large_payload(
        request_database, monkeypatch):
    """Quiescence polling must not deserialize or transfer launch payloads."""
    _, backend = request_database
    request_id = 'scalar-status-projection'
    request = requests.Request(
        request_id=request_id,
        name='sky.launch',
        entrypoint=execution.launch,
        request_body=payloads.LaunchBody(
            task='resources:\n  cpus: 2\n' + '# large-payload\n' * 10000,
            cluster_name='projection-cluster',
            env_vars={'STORED_PROJECTION_SECRET': 'stored-canary'}),
        status=requests.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='user',
        cluster_name='projection-cluster',
        schedule_type=requests.ScheduleType.LONG,
        should_enqueue=True)
    assert asyncio.run(backend.create_if_not_exists_async(request))
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'ambient-canary')
    decode_payload = mock.Mock(
        side_effect=AssertionError('payload must not be decoded'))
    monkeypatch.setattr(registry, 'decode_payload', decode_payload)

    [projected] = backend.query_requests(
        requests.RequestTaskFilter(request_ids=[request_id],
                                   fields=[
                                       'request_id', 'name', 'cluster_name',
                                       'status', 'execution_generation',
                                       'execution_quiescence_required',
                                       'execution_quiesced_generation',
                                       'execution_quiesced_at'
                                   ],
                                   sort=False))

    assert projected.request_id == request_id
    assert projected.cluster_name == 'projection-cluster'
    assert projected.request_body == payloads.RequestBody.projection_placeholder(
    )
    decode_payload.assert_not_called()


def test_pending_direct_cancel_does_not_invent_quiescence(request_database):
    _, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(
            _request('direct-pending-cancel', should_enqueue=False)))

    assert backend.kill_requests(['direct-pending-cancel'
                                 ]) == ['direct-pending-cancel']

    restored = backend.get_request('direct-pending-cancel')
    assert restored.status is requests.RequestStatus.CANCELLED
    # Direct coroutines never carry a durable execution claim. Cancellation
    # must not opt them into an acknowledgement they cannot publish.
    assert not restored.execution_quiescence_required
    assert restored.execution_quiesced_generation is None
    assert restored.execution_quiesced_at is None


def test_waiting_cancel_without_receipt_does_not_invent_quiescence(
        request_database):
    engine, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('waiting-cancel')))
    item = _claim(backend, 'waiting-cancel')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                'waiting-cancel').values(
                    status=requests.RequestStatus.WAITING.value,
                    pid=None,
                    claim_token=None,
                    worker_instance_id=None,
                    lease_expires_at=None,
                    heartbeat_at=None))
        connection.execute(
            sqlalchemy.update(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id == 'waiting-cancel').values(
                    delivery_state='queued', claim_generation=None))

    assert backend.kill_requests(['waiting-cancel']) == ['waiting-cancel']

    restored = backend.get_request('waiting-cancel')
    assert restored.execution_generation == item.execution_generation
    assert restored.execution_quiescence_required
    assert restored.execution_quiesced_generation is None
    assert restored.execution_quiesced_at is None


def test_running_pidless_cancel_does_not_invent_quiescence(request_database):
    engine, backend = request_database
    assert asyncio.run(
        backend.create_if_not_exists_async(_request('running-pidless')))
    item = _claim(backend, 'running-pidless')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                'running-pidless').values(pid=None))

    assert backend.kill_requests(['running-pidless']) == ['running-pidless']

    restored = backend.get_request('running-pidless')
    assert restored.execution_generation == item.execution_generation
    assert restored.execution_quiescence_required
    assert restored.execution_quiesced_generation is None
    assert restored.execution_quiesced_at is None


def test_expired_running_pidless_replayable_claim_fails_closed(
        request_database):
    engine, backend = request_database
    request_id = 'expired-running-pidless'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    pid=None,
                    execution_process_start_time_ticks=None,
                    lease_expires_at=sqlalchemy.func.clock_timestamp() -
                    datetime.timedelta(seconds=1)))

    queue = request_postgres.PostgresQueueBackend('short')
    with engine.begin() as connection:
        queue._reap_expired_claims(connection)

    _assert_execution_claim_blocked(engine, request_id, item)
    with engine.connect() as connection:
        first = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id)).mappings().one()
    assert first['cancel_requested_at'] is not None
    with engine.begin() as connection:
        queue._reap_expired_claims(connection)
    with engine.connect() as connection:
        second = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id)).mappings().one()
    assert second['updated_at'] == first['updated_at']


def test_expired_pending_pidless_replayable_claim_requeues_pre_effect(
        request_database):
    engine, backend = request_database
    request_id = 'expired-pending-pidless'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    queue = request_postgres.PostgresQueueBackend('short')
    item = queue.get()
    assert item is not None
    assert item.request_id == request_id
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    lease_expires_at=sqlalchemy.func.clock_timestamp() -
                    datetime.timedelta(seconds=1)))
        queue._reap_expired_claims(connection)

    _assert_execution_claim_requeued(engine, request_id)


def test_expired_claim_reaper_filters_handled_rows_before_limit(
        request_database):
    engine, backend = request_database
    queue = request_postgres.PostgresQueueBackend('short')
    target_id = 'expired-target-after-handled-limit'
    assert asyncio.run(backend.create_if_not_exists_async(_request(target_id)))
    target = queue.get()
    assert target is not None
    assert target.request_id == target_id
    _expire_claim(engine, target_id)

    now = datetime.datetime.now(datetime.timezone.utc)
    expired = now - datetime.timedelta(minutes=5)
    request_rows = []
    queue_rows = []
    for index in range(101):
        request_id = f'already-handled-expired-{index:03d}'
        never = index >= 51
        request = _request(request_id,
                           entrypoint=(volume_core.volume_apply
                                       if never else core.enabled_clouds))
        request_values = request_postgres._request_values_for_db(request)
        generation = 1
        request_values.update(
            status=(requests.RequestStatus.CANCELLED.value
                    if never else requests.RequestStatus.RUNNING.value),
            terminal_cause='explicit_cancel' if never else None,
            execution_generation=generation,
            claim_token=uuid.uuid4(),
            worker_instance_id=uuid.UUID(backend.instance_id),
            lease_expires_at=expired,
            heartbeat_at=expired,
            pid=20000 + index,
            execution_process_start_time_ticks=424242 + index,
            cancel_requested_at=now if not never else None,
            execution_quiescence_required=not never,
            should_retry=never,
            finished_at=now if never else None,
            updated_at=expired)
        request_rows.append(request_values)
        queue_values = request_postgres._queue_values(request)
        queue_values.update(available_at=expired,
                            enqueued_at=expired,
                            delivery_state='claimed',
                            claim_generation=generation,
                            updated_at=expired)
        queue_rows.append(queue_values)
    with engine.begin() as connection:
        connection.execute(sqlalchemy.insert(request_postgres.REQUESTS),
                           request_rows)
        connection.execute(sqlalchemy.insert(request_postgres.QUEUE),
                           queue_rows)
        queue._reap_expired_claims(connection)

    _assert_execution_claim_requeued(engine, target_id)


def test_quiescence_reducer_filters_never_policy_before_limit(request_database):
    engine, backend = request_database
    blockers = []
    old = datetime.datetime.now(
        datetime.timezone.utc) - datetime.timedelta(minutes=5)
    for index in range(101):
        request = _request(f'never-reducer-blocker-{index:03d}',
                           entrypoint=volume_core.volume_apply,
                           should_enqueue=False)
        values = request_postgres._request_values_for_db(request)
        values.update(status=requests.RequestStatus.CANCELLED.value,
                      terminal_cause='explicit_cancel',
                      should_retry=True,
                      finished_at=old,
                      updated_at=old)
        blockers.append(values)
    with engine.begin() as connection:
        connection.execute(sqlalchemy.insert(request_postgres.REQUESTS),
                           blockers)

    target_id = 'reducer-target-after-never-limit'
    assert asyncio.run(backend.create_if_not_exists_async(_request(target_id)))
    item = _claim(backend, target_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == target_id).values(
                    cancel_requested_at=sqlalchemy.func.clock_timestamp(),
                    execution_quiescence_required=True,
                    execution_quiesced_generation=item.execution_generation,
                    execution_quiesced_at=sqlalchemy.func.clock_timestamp()))
        assert request_postgres._requeue_quiesced_replayable_requests(
            connection) == 1

    _assert_execution_claim_requeued(engine, target_id)


def test_remote_cancel_signal_is_not_execution_quiescence(
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
    signal_exact = mock.Mock(return_value=True)
    monkeypatch.setattr(request_postgres, '_signal_exact_executor_process',
                        signal_exact)
    assert api_backend.kill_requests(['remote-cancel']) == ['remote-cancel']
    signal_exact.assert_not_called()
    assert not api_backend.acknowledge_execution_quiescence(
        storage.ExecutionClaim(item.request_id, item.execution_generation,
                               item.claim_token))

    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       executor_instance_id)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token)
    assert executor_backend.interrupt_cancelled_claim(claim)
    signal_exact.assert_called_once_with(1234, 424242,
                                         request_postgres.signal.SIGTERM)
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.cancel_acknowledged_at,
                request_postgres.REQUESTS.c.execution_quiesced_generation,
                request_postgres.REQUESTS.c.execution_quiesced_at).where(
                    request_postgres.REQUESTS.c.request_id ==
                    'remote-cancel')).one()
    assert row.cancel_acknowledged_at is not None
    assert row.execution_quiesced_generation is None
    assert row.execution_quiesced_at is None

    stale_claim = storage.ExecutionClaim(item.request_id,
                                         item.execution_generation,
                                         str(uuid.uuid4()))
    assert not executor_backend.acknowledge_execution_quiescence(stale_claim)
    stale_generation = storage.ExecutionClaim(item.request_id,
                                              item.execution_generation - 1,
                                              item.claim_token)
    assert not executor_backend.acknowledge_execution_quiescence(
        stale_generation)
    assert executor_backend.acknowledge_execution_quiescence(claim)
    assert executor_backend.acknowledge_execution_quiescence(claim)
    restored = executor_backend.get_request('remote-cancel')
    assert (restored.execution_quiesced_generation == item.execution_generation)
    assert restored.execution_quiesced_at is not None


def test_cancel_signal_and_receipt_are_serialized_by_request_lock(
        request_database, monkeypatch):
    """A wrapper cannot return/reuse its PID while cancellation signals it."""
    _, backend = request_database
    request_id = 'cancel-row-lock-ordering'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    assert item.claim_token is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token)
    signal_entered = threading.Event()
    signal_release = threading.Event()
    receipt_returned = threading.Event()
    receipt_result: list[bool] = []

    def blocking_signal(_pid, _start_time_ticks, _signal):
        signal_entered.set()
        assert signal_release.wait(timeout=5)
        return True

    monkeypatch.setattr(request_postgres, '_signal_exact_executor_process',
                        blocking_signal)

    cancel_thread = threading.Thread(target=backend.kill_requests,
                                     args=([request_id],))
    cancel_thread.start()
    assert signal_entered.wait(timeout=5)

    def publish_receipt():
        receipt_result.append(backend.acknowledge_execution_quiescence(claim))
        receipt_returned.set()

    receipt_thread = threading.Thread(target=publish_receipt)
    receipt_thread.start()
    assert not receipt_returned.wait(timeout=0.2)

    signal_release.set()
    cancel_thread.join(timeout=5)
    receipt_thread.join(timeout=5)
    assert not cancel_thread.is_alive()
    assert not receipt_thread.is_alive()
    assert receipt_result == [True]


@pytest.mark.parametrize('terminal_status', [
    requests.RequestStatus.SUCCEEDED,
    requests.RequestStatus.FAILED,
])
def test_exact_worker_records_terminal_execution_quiescence(
        request_database, terminal_status):
    _, backend = request_database
    request_id = f'quiesced-{terminal_status.value.lower()}'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    assert item.claim_token is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token)
    context = storage.activate_execution_claim(claim.request_id,
                                               claim.execution_generation,
                                               claim.claim_token)
    try:
        if terminal_status is requests.RequestStatus.SUCCEEDED:
            assert backend.set_request_finished(request_id,
                                                terminal_status,
                                                result=[])
        else:
            assert backend.set_request_finished(request_id,
                                                terminal_status,
                                                error=RuntimeError('failed'))
    finally:
        storage.deactivate_execution_claim(context)

    assert backend.acknowledge_execution_quiescence(claim)
    restored = backend.get_request(request_id)
    assert restored.execution_quiescence_required
    assert (restored.execution_quiesced_generation == item.execution_generation)
    assert restored.execution_quiesced_at is not None


def test_parent_transport_exception_atomically_fails_and_quiesces(
        request_database):
    engine, backend = request_database
    request_id = 'parent-transport-exception'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)

    assert backend.converge_execution_completion(
        claim, error=RuntimeError('transported callable failure'))

    request_row, queue_row = _execution_claim_state(engine, request_id)
    assert request_row['status'] == requests.RequestStatus.FAILED.value
    assert request_row['terminal_cause'] == (
        event_api_models.EventCause.HANDLER_FAILED.value)
    assert request_row['execution_quiesced_generation'] == (
        item.execution_generation)
    assert request_row['execution_quiesced_at'] is not None
    assert not queue_row
    restored = backend.get_request(request_id)
    assert restored.get_error()['type'] == 'RuntimeError'


def test_parent_normal_completion_preserves_child_terminal_result(
        request_database):
    engine, backend = request_database
    request_id = 'parent-preserves-child-terminal'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    context = storage.activate_execution_claim(item.request_id,
                                               item.execution_generation,
                                               item.claim_token)
    try:
        assert backend.set_request_finished(request_id,
                                            requests.RequestStatus.SUCCEEDED,
                                            result=[])
    finally:
        storage.deactivate_execution_claim(context)

    assert backend.converge_execution_completion(claim)

    request_row, queue_row = _execution_claim_state(engine, request_id)
    assert request_row['status'] == requests.RequestStatus.SUCCEEDED.value
    assert request_row['return_value'] == []
    assert request_row['execution_quiesced_generation'] == (
        item.execution_generation)
    assert request_row['execution_quiesced_at'] is not None
    assert not queue_row


def test_new_claim_resets_prior_generation_quiescence(request_database):
    _, backend = request_database
    request_id = 'quiescence-reset-on-claim'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    first = _claim(backend, request_id)
    assert first.claim_token is not None
    first_claim = storage.ExecutionClaim(first.request_id,
                                         first.execution_generation,
                                         first.claim_token)
    assert backend.acknowledge_execution_quiescence(first_claim)
    request_postgres.PostgresQueueBackend('short').put(first)
    waiting = backend.get_request(request_id)
    assert waiting.execution_quiesced_generation == first.execution_generation

    second = request_postgres.PostgresQueueBackend('short').get()

    assert second is not None
    assert second.execution_generation == first.execution_generation + 1
    restored = backend.get_request(request_id)
    assert restored.execution_quiescence_required
    assert restored.execution_quiesced_generation is None
    assert restored.execution_quiesced_at is None


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


def test_cancel_never_signals_same_pid_with_different_birth(
        request_database, monkeypatch):
    _, backend = request_database
    request_id = 'cancel-reused-unowned-pid'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    signal_exact = mock.Mock(return_value=False)
    monkeypatch.setattr(request_postgres, '_signal_exact_executor_process',
                        signal_exact)

    assert backend.kill_requests([request_id]) == [request_id]

    signal_exact.assert_called_once_with(1234, 424242,
                                         request_postgres.signal.SIGTERM)
    restored = backend.get_request(request_id)
    assert restored.status is requests.RequestStatus.CANCELLED
    assert restored.execution_quiescence_required
    assert restored.execution_quiesced_generation is None
    assert item.claim_token is not None


def test_exact_signal_uses_pidfd_and_process_birth_identity(monkeypatch):
    pidfd_send_signal = mock.Mock()
    monkeypatch.setattr(request_postgres.os,
                        'pidfd_open',
                        lambda pid, flags: 9,
                        raising=False)
    monkeypatch.setattr(request_postgres.signal,
                        'pidfd_send_signal',
                        pidfd_send_signal,
                        raising=False)
    monkeypatch.setattr(request_postgres.os, 'close', mock.Mock())
    monkeypatch.setattr(request_postgres, '_is_owned_executor_process',
                        lambda _pid: True)
    monkeypatch.setattr(storage, 'read_linux_process_start_time_ticks',
                        lambda _pid: 777)

    assert not request_postgres._signal_exact_executor_process(
        1234, 424242, request_postgres.signal.SIGTERM)
    pidfd_send_signal.assert_not_called()

    assert request_postgres._signal_exact_executor_process(
        1234, 777, request_postgres.signal.SIGTERM)
    pidfd_send_signal.assert_called_once_with(9,
                                              request_postgres.signal.SIGTERM,
                                              None, 0)


def test_registry_rejects_row_selected_code_and_execution_class():
    registry.register_builtin_handlers()
    with pytest.raises(ValueError, match='Unknown durable request handler'):
        registry.resolve_handler('os:system')
    request = _request('class-fence')
    values = request.durable_values()
    values['execution_class'] = registry.ExecutionClass.CONTROLLER.value
    with pytest.raises(ValueError, match='execution class'):
        requests.Request.from_durable_values(values)


def test_bound_handler_and_effect_bridge_require_active_full_claim(monkeypatch):
    monkeypatch.setattr(ordinary_launch_request, 'execution', mock.Mock())
    ordinary_launch_request.execution.launch.return_value = 'launched'
    with pytest.raises(exceptions.RequestCancelled,
                       match='no exact durable execution claim'):
        ordinary_launch_request.launch()

    claim = storage.ExecutionClaim('request-id', 3, str(uuid.uuid4()),
                                   str(uuid.uuid4()))
    token = storage.activate_execution_claim(claim.request_id,
                                             claim.execution_generation,
                                             claim.claim_token,
                                             claim.worker_instance_id)
    try:
        assert ordinary_launch_request.launch('task') == 'launched'
        ordinary_launch_request.execution.launch.assert_called_once_with('task')

        fake_binding = mock.Mock()
        guard = mock.MagicMock()
        fake_binding.provider_effect_guard.return_value = guard
        validator = mock.Mock()
        fake_postgres = mock.Mock()
        fake_postgres.validate_bound_ordinary_launch_claim_in_transaction = (
            validator)
        monkeypatch.setattr(ordinary_launch_request, 'ordinary_launch_binding',
                            fake_binding)
        monkeypatch.setattr(ordinary_launch_request, 'request_postgres',
                            fake_postgres)
        bound_context = {
            'sky_serve_ordinary_launch_association_id': str(uuid.uuid4()),
            'sky_serve_ordinary_launch_generation': 1,
            'sky_serve_ordinary_launch_request_id': claim.request_id,
            'sky_serve_ordinary_launch_input_digest': 'a' * 64,
        }
        assert ordinary_launch_request._provider_effect_guard(
            bound_context) is guard
        fake_binding.parse_bound_launch_context.assert_called_once_with(
            bound_context)
        fake_binding.provider_effect_guard.assert_called_once_with(
            bound_context, claim, claim_validator=validator)
    finally:
        storage.deactivate_execution_claim(token)

    with ordinary_launch_request._provider_effect_guard({}) as authorization:
        assert authorization is None
    fake_binding.reset_mock()
    fake_binding.parse_bound_launch_context.side_effect = ValueError(
        'partial bound context')
    with pytest.raises(ValueError, match='partial bound context'):
        ordinary_launch_request._provider_effect_guard(
            {'sky_serve_ordinary_launch_request_id': 'partial'})


def test_registry_owns_controller_classes_and_replay_policies():
    jobs_launch = registry.registration_for_handler(managed_jobs_core.launch)
    jobs_queue = registry.registration_for_handler(managed_jobs_core.queue)
    serve_status = registry.registration_for_handler(serve_core.status)
    authorized_serve_status = registry.registration_for_handler(
        serve_core.authorized_status)
    authorized_serve_placement = registry.registration_for_handler(
        serve_core.authorized_placement)
    normal_read = registry.registration_for_handler(core.enabled_clouds)
    bound_launch = registry.registration_for_handler(
        ordinary_launch_request.launch)
    volume_apply = registry.registration_for_handler(volume_core.volume_apply)
    volume_delete = registry.registration_for_handler(volume_core.volume_delete)
    volume_list = registry.registration_for_handler(volume_core.volume_list)

    assert jobs_launch.execution_class is registry.ExecutionClass.CONTROLLER
    assert jobs_launch.replay_policy is registry.ReplayPolicy.NEVER
    assert jobs_queue.execution_class is registry.ExecutionClass.CONTROLLER
    assert jobs_queue.replay_policy is registry.ReplayPolicy.READ_ONLY
    assert serve_status.execution_class is registry.ExecutionClass.CONTROLLER
    assert serve_status.replay_policy is registry.ReplayPolicy.READ_ONLY
    assert (authorized_serve_status.execution_class
            is registry.ExecutionClass.CONTROLLER)
    assert authorized_serve_status.replay_policy is registry.ReplayPolicy.READ_ONLY
    assert (authorized_serve_placement.execution_class
            is registry.ExecutionClass.CONTROLLER)
    assert (authorized_serve_placement.replay_policy
            is registry.ReplayPolicy.READ_ONLY)
    assert authorized_serve_status.name != serve_status.name
    assert normal_read.execution_class is registry.ExecutionClass.NORMAL
    assert normal_read.replay_policy is registry.ReplayPolicy.READ_ONLY
    assert bound_launch.name == (
        ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME)
    assert bound_launch.execution_class is registry.ExecutionClass.NORMAL
    assert bound_launch.replay_policy is registry.ReplayPolicy.NEVER
    assert volume_apply.execution_class is registry.ExecutionClass.NORMAL
    assert volume_apply.replay_policy is registry.ReplayPolicy.NEVER
    assert volume_delete.execution_class is registry.ExecutionClass.NORMAL
    assert volume_delete.replay_policy is registry.ReplayPolicy.NEVER
    assert volume_list.execution_class is registry.ExecutionClass.NORMAL
    assert volume_list.replay_policy is registry.ReplayPolicy.READ_ONLY
    with pytest.raises(ValueError, match='not registered'):
        registry.registration_for_handler(daemons.RUNTIME_DAEMONS[0].run_event)
    assert all(not registration.name.startswith('daemon:')
               for registration in registry.registered_handlers())


def test_local_sqlite_request_claim_arguments_are_ignored(
        tmp_path, monkeypatch):
    """The legacy SQLite worker has no durable claim or lease fence."""
    database_path = tmp_path / 'requests.db'
    log_path = tmp_path / 'logs'
    log_path.mkdir()
    monkeypatch.setattr('sky.server.constants.API_SERVER_REQUEST_DB_PATH',
                        str(database_path))
    monkeypatch.setattr('sky.server.constants.REQUEST_LOG_PATH_PREFIX',
                        str(log_path))
    requests._DB = None
    backend = requests.SqliteRequestBackend()
    request = _request('sqlite-volume-delete')
    request.name = 'sky.volumes.delete'
    request.entrypoint = volume_core.volume_delete
    request.request_body = payloads.VolumeDeleteBody(names=['test-volume'])
    try:
        assert asyncio.run(backend.create_if_not_exists_async(request))
        assert backend.claim_heartbeat_interval_seconds is None
        assert backend.try_mark_running(  # pylint: disable=too-many-function-args
            request.request_id, 1234, 2**31, 'not-a-durable-sqlite-claim')
        stored = backend.get_request(request.request_id)
        assert stored is not None
        assert stored.status is requests.RequestStatus.RUNNING
        assert stored.pid == 1234
    finally:
        asyncio.run(requests.close_db_async())


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
        item.request_id, 1234, item.execution_generation, item.claim_token,
        424242)
    assert not backend.heartbeat_claim(
        storage.ExecutionClaim(item.request_id, item.execution_generation,
                               item.claim_token))


def test_retry_handoff_is_atomic_after_lease_expiry(request_database):
    """Family proof, not lease age, authorizes a completed delayed retry."""
    engine, backend = request_database
    request_id = 'retry-handoff-after-expiry'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    assert item.claim_token is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    lease_expires_at=(sqlalchemy.func.clock_timestamp() -
                                      datetime.timedelta(seconds=1))))

    assert backend.handoff_execution_retry(
        claim, 'capacity pending (retrying in 200s)', 200)

    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.status,
                request_postgres.REQUESTS.c.claim_token,
                request_postgres.REQUESTS.c.worker_instance_id,
                request_postgres.REQUESTS.c.lease_expires_at,
                request_postgres.REQUESTS.c.execution_quiescence_required,
                request_postgres.REQUESTS.c.status_msg,
                request_postgres.QUEUE.c.delivery_state,
                request_postgres.QUEUE.c.claim_generation,
                request_postgres.QUEUE.c.available_at,
                sqlalchemy.func.clock_timestamp().label('database_now')).join(
                    request_postgres.QUEUE, request_postgres.QUEUE.c.request_id
                    == request_postgres.REQUESTS.c.request_id).where(
                        request_postgres.REQUESTS.c.request_id ==
                        request_id)).mappings().one()
    assert row['status'] == requests.RequestStatus.WAITING.value
    assert row['claim_token'] is None
    assert row['worker_instance_id'] is None
    assert row['lease_expires_at'] is None
    assert not row['execution_quiescence_required']
    assert row['status_msg'] == 'capacity pending (retrying in 200s)'
    assert row['delivery_state'] == 'queued'
    assert row['claim_generation'] is None
    delay = (row['available_at'] - row['database_now']).total_seconds()
    assert 195 <= delay <= 200


def test_retry_handoff_does_not_resurrect_cancellation(request_database):
    engine, backend = request_database
    request_id = 'retry-handoff-cancel-wins'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    assert item.claim_token is not None
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    assert backend.kill_requests([request_id], user_id=None) == [request_id]

    assert not backend.handoff_execution_retry(claim, 'must not retry', 200)

    with engine.connect() as connection:
        status = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS.c.status).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id)).scalar_one()
        delivery_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(request_postgres.QUEUE).where(
                                 request_postgres.QUEUE.c.request_id ==
                                 request_id)).scalar_one()
    assert status == requests.RequestStatus.CANCELLED.value
    assert delivery_count == 0


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


def _orphaned_quiescence_row(engine,
                             backend,
                             request_id,
                             *,
                             finished_ago,
                             lease_offset,
                             status='CANCELLED',
                             quiescence_required=True):
    """Persist one terminal request awaiting an execution-quiescence ack."""
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    values = {
        'status': status,
        'execution_generation': 1,
        'execution_quiescence_required': quiescence_required,
        'execution_quiesced_generation': None,
        'execution_quiesced_at': None,
        'finished_at': (sqlalchemy.func.clock_timestamp() - finished_ago),
        'lease_expires_at': (None if lease_offset is None else
                             sqlalchemy.func.clock_timestamp() + lease_offset),
    }
    if lease_offset is not None:
        # ck_api_requests_claim: a lease only exists alongside its claim.
        values.update({
            'claim_token': uuid.uuid4(),
            'worker_instance_id': uuid.uuid4(),
            'pid': 4242,
        })
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    **values))


def _quiescence_state(engine, request_id):
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.execution_quiesced_generation,
                request_postgres.REQUESTS.c.execution_quiesced_at).where(
                    request_postgres.REQUESTS.c.request_id ==
                    request_id)).first()


def test_expired_or_absent_lease_does_not_invent_quiescence(request_database):
    """Lease timeout revokes authority but is not positive process death."""
    engine, backend = request_database
    stale = datetime.timedelta(seconds=360)
    _orphaned_quiescence_row(engine,
                             backend,
                             'orphaned-quiescence',
                             finished_ago=stale,
                             lease_offset=None)

    queue = request_postgres.PostgresQueueBackend('short')
    assert queue.get() is None

    row = _quiescence_state(engine, 'orphaned-quiescence')
    assert row.execution_quiesced_generation is None
    assert row.execution_quiesced_at is None


def test_recently_finished_execution_keeps_its_own_acknowledgement(
        request_database):
    """Inside the grace window the owner's own ack must still win."""
    engine, backend = request_database
    _orphaned_quiescence_row(engine,
                             backend,
                             'recently-finished',
                             finished_ago=datetime.timedelta(seconds=5),
                             lease_offset=None)

    queue = request_postgres.PostgresQueueBackend('short')
    assert queue.get() is None

    row = _quiescence_state(engine, 'recently-finished')
    assert row.execution_quiesced_generation is None
    assert row.execution_quiesced_at is None


def test_live_lease_holder_is_never_declared_quiescent(request_database):
    """A live lease is the one thing that may still run effect-bearing code."""
    engine, backend = request_database
    stale = datetime.timedelta(seconds=360)
    _orphaned_quiescence_row(engine,
                             backend,
                             'live-lease',
                             finished_ago=stale,
                             lease_offset=datetime.timedelta(seconds=30))

    queue = request_postgres.PostgresQueueBackend('short')
    assert queue.get() is None

    row = _quiescence_state(engine, 'live-lease')
    assert row.execution_quiesced_generation is None
    assert row.execution_quiesced_at is None


def test_requests_without_a_quiescence_contract_are_untouched(request_database):
    engine, backend = request_database
    stale = datetime.timedelta(seconds=360)
    _orphaned_quiescence_row(engine,
                             backend,
                             'no-contract',
                             finished_ago=stale,
                             lease_offset=None,
                             quiescence_required=False)

    queue = request_postgres.PostgresQueueBackend('short')
    assert queue.get() is None

    row = _quiescence_state(engine, 'no-contract')
    assert row.execution_quiesced_generation is None
    assert row.execution_quiesced_at is None


def test_non_terminal_requests_are_never_reaped(request_database):
    """An unfinished execution is still running; it owns its own proof."""
    engine, backend = request_database
    stale = datetime.timedelta(seconds=360)
    _orphaned_quiescence_row(engine,
                             backend,
                             'still-running',
                             finished_ago=stale,
                             lease_offset=None,
                             status='RUNNING')

    queue = request_postgres.PostgresQueueBackend('short')
    assert queue.get() is None

    row = _quiescence_state(engine, 'still-running')
    assert row.execution_quiesced_generation is None
    assert row.execution_quiesced_at is None


def _execution_claim_state(engine: sqlalchemy.engine.Engine,
                           request_id: str) -> tuple[dict, dict]:
    with engine.connect() as connection:
        request_row = dict(
            connection.execute(
                sqlalchemy.select(request_postgres.REQUESTS).where(
                    request_postgres.REQUESTS.c.request_id ==
                    request_id)).mappings().one())
        queue_mapping = connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                request_id)).mappings().one_or_none()
        queue_row = dict(queue_mapping) if queue_mapping is not None else {}
    return request_row, queue_row


def _assert_execution_claim_requeued(engine: sqlalchemy.engine.Engine,
                                     request_id: str) -> None:
    request_row, queue_row = _execution_claim_state(engine, request_id)
    assert request_row['status'] == requests.RequestStatus.WAITING.value
    assert request_row['pid'] is None
    assert request_row['claim_token'] is None
    assert request_row['worker_instance_id'] is None
    assert not request_row['execution_quiescence_required']
    assert request_row['execution_quiesced_generation'] is None
    assert request_row['execution_quiesced_at'] is None
    assert queue_row['delivery_state'] == 'queued'
    assert queue_row['claim_generation'] is None


def _assert_execution_claim_blocked(engine: sqlalchemy.engine.Engine,
                                    request_id: str,
                                    item: queue_base.QueueItem) -> None:
    request_row, queue_row = _execution_claim_state(engine, request_id)
    assert request_row['status'] == requests.RequestStatus.RUNNING.value
    assert request_row['claim_token'] == uuid.UUID(item.claim_token)
    assert request_row['worker_instance_id'] == uuid.UUID(
        item.worker_instance_id)
    assert request_row['execution_quiesced_generation'] is None
    assert request_row['execution_quiesced_at'] is None
    assert queue_row['delivery_state'] == 'claimed'
    assert queue_row['claim_generation'] == item.execution_generation


@pytest.mark.parametrize('invalid_pid', [True, 0, -1])
def test_durable_running_claim_rejects_invalid_pid(request_database,
                                                   invalid_pid):
    _, backend = request_database
    request_id = f'invalid-pid-{invalid_pid!r}'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = request_postgres.PostgresQueueBackend('short').get()
    assert item is not None
    assert item.claim_token is not None

    with pytest.raises(ValueError, match='positive PID'):
        backend.try_mark_running(request_id, invalid_pid,
                                 item.execution_generation, item.claim_token,
                                 424242)


@pytest.mark.parametrize('signal_delivered', [False, True])
def test_shutdown_retry_waits_for_exact_quiescence_receipt(
        request_database, monkeypatch, signal_delivered):
    engine, backend = request_database
    request_id = f'shutdown-retry-receipt-{signal_delivered}'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    signal_exact = mock.Mock(return_value=signal_delivered)
    monkeypatch.setattr(request_postgres, '_signal_exact_executor_process',
                        signal_exact)

    assert (backend.interrupt_request_for_shutdown_retry(request_id)
            is signal_delivered)
    signal_exact.assert_called_once_with(1234, 424242, signal.SIGTERM)
    request_row, queue_row = _execution_claim_state(engine, request_id)
    # Signal delivery is not stop proof: polling cannot observe client retry
    # until the exact wrapper publishes its generation-bound receipt.
    assert request_row['status'] == requests.RequestStatus.RUNNING.value
    assert not request_row['should_retry']
    assert request_row['execution_quiesced_at'] is None
    assert queue_row['delivery_state'] == 'claimed'

    claim = storage.ExecutionClaim(request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    assert backend.acknowledge_execution_quiescence(claim)
    request_row, queue_row = _execution_claim_state(engine, request_id)
    assert request_row['status'] == requests.RequestStatus.CANCELLED.value
    assert request_row['should_retry']
    assert request_row['execution_quiesced_generation'] == (
        item.execution_generation)
    assert not queue_row
