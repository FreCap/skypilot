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
from sky.events import api_models as event_api_models
from sky.jobs.server import core as managed_jobs_core
from sky.serve import constants as serve_constants
from sky.serve import ordinary_launch_binding
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve.server import core as serve_core
from sky.server import daemons
from sky.server.events import cursors as event_cursors
from sky.server.events import emission as event_emission
from sky.server.events import schema as event_schema
from sky.server.events import store as event_store
from sky.server.requests import cutover
from sky.server.requests import executor
from sky.server.requests import ordinary_launch as ordinary_launch_request
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import preconditions
from sky.server.requests import registry
from sky.server.requests import requests
from sky.server.requests import storage
from sky.server.requests.queues import base as queue_base
from sky.skylet import constants
from sky.utils import common_utils
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
    alembic_command.upgrade(config, '042')
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
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request.request_id)).mappings().one()
        legacy_instance = connection.execute(
            sqlalchemy.select(request_postgres.SERVER_INSTANCES).where(
                request_postgres.SERVER_INSTANCES.c.instance_id ==
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
    engine, _ = bound_request_database
    association_id = uuid.UUID('44444444-4444-4444-8444-444444444444')
    request_id = 'gc-bound-request'
    now = datetime.datetime.now(datetime.timezone.utc)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                ordinary_launch_binding.ordinary_launch_associations_table).
            values(
                association_id=association_id,
                submission_id=uuid.UUID('11111111-1111-4111-8111-111111111111'),
                tenant_scope='tenant-a',
                service_name='gc-service',
                service_hash='gc-service-hash',
                service_workspace='workspace-a',
                service_lifecycle_epoch=4,
                service_binding_epoch=5,
                service_version=2,
                replica_id=3,
                replica_record_id=uuid.UUID(
                    '22222222-2222-4222-8222-222222222222'),
                launch_generation=1,
                cluster_name='gc-service-3',
                request_id=request_id,
                input_digest='a' * 64,
                owner_controller_incarnation=uuid.UUID(
                    '33333333-3333-4333-8333-333333333333'),
                owner_controller_epoch=6,
                effect_phase='NOT_STARTED',
                resolution='PRE_EFFECT_TERMINAL',
                terminal_status='CANCELLED',
                terminal_cause='dispatcher_submit_failed',
                terminal_execution_generation=0,
                execution_quiescence_required=True,
                execution_quiesced_generation=0,
                execution_quiesced_at=now,
                result_recorded_at=now,
                projected_at=now,
                pin_released_at=now,
                tombstone_not_before=now - datetime.timedelta(seconds=1)))

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


def test_bound_handler_is_filtered_by_local_inventory_and_claim_carries_owner(
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
    item = capable_queue.get()
    assert item is not None
    assert item.request_id == request_id
    assert item.worker_instance_id == capable_queue._instance_id
    assert item.claim_token is not None
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token)


def test_bound_effect_claim_requires_exact_request_queue_pin_and_owner(
        request_database):
    engine, backend = request_database
    request_id = 'bound-effect-claim'
    association_id = uuid.uuid4()
    with engine.begin() as connection:
        assert request_postgres.insert_bound_request_and_queue_in_transaction(
            connection,
            _bound_request(request_id),
            ordinary_launch_association_id=association_id)
    item = _claim(backend, request_id)
    assert item.claim_token is not None
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
    item = queue.get()
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


def test_terminal_expired_bound_claim_settles_only_before_provider_io(
        bound_request_database):
    engine, backend = bound_request_database
    identity, context, _, item = _claim_gc_bound_request(engine, backend)
    facts = request_postgres.request_bound_ordinary_launch_cancel(
        context, _gc_binding_authority(), 'replica-teardown')
    assert facts.status is requests.RequestStatus.CANCELLED
    assert not facts.queue_exists
    assert facts.claim_token == uuid.UUID(item.claim_token)
    assert not facts.quiescent
    _expire_claim(engine, identity.request_id)

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
    # Synthesized proof keeps the exact owner identity, so a late genuine ACK
    # is an idempotent success instead of losing its generation.
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    assert backend.acknowledge_execution_quiescence(claim)


def test_active_expired_bound_claim_preserves_prior_exact_quiescence(
        bound_request_database):
    engine, backend = bound_request_database
    identity, context, _, item = _claim_gc_bound_request(engine, backend)
    assert backend.try_mark_running(item.request_id, 1234,
                                    item.execution_generation, item.claim_token)
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
                                    item.execution_generation, item.claim_token)
    body = _bound_request(identity.request_id).request_body
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
    assert resolution == ordinary_launch_binding.Resolution.AMBIGUOUS.value


def test_malformed_success_result_is_durably_ambiguous(bound_request_database):
    engine, backend = bound_request_database
    identity, context, _, item = _claim_gc_bound_request(engine, backend)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == identity.association_id).values(
                      effect_phase=(ordinary_launch_binding.EffectPhase.
                                    SERVICE_JOB_RECORDED.value),
                      service_job_id=17,
                      owner_revision=(
                          ordinary_launch_binding.
                          ordinary_launch_associations_table.c.owner_revision +
                          1)))
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    active_claim = storage.activate_execution_claim(claim.request_id,
                                                    claim.execution_generation,
                                                    claim.claim_token)
    try:
        # A generic list is serializable, but it is not the exact
        # ``(service_job_id, CloudVmRayResourceHandle)`` success contract.
        assert backend.set_request_finished(identity.request_id,
                                            requests.RequestStatus.SUCCEEDED,
                                            result=[])
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
        engine, migration_utils.API_REQUESTS_DB_NAME) == '009'
    inspector = sqlalchemy.inspect(engine)
    assert {
        'api_requests', 'api_request_queue', 'api_server_instances',
        'api_request_store_metadata', 'api_controller_leadership',
        'api_controller_action_reservations', 'resource_events',
        'resource_event_targets', 'api_resource_actions',
        'api_resource_action_attempts', 'api_request_retention_pins'
    }.issubset(inspector.get_table_names())
    request_columns = {
        column['name'] for column in inspector.get_columns('api_requests')
    }
    assert 'event_context' in request_columns
    assert {'resource_action_id',
            'resource_action_attempt'}.issubset(request_columns)
    assert 'ordinary_launch_association_id' in request_columns
    assert {
        'execution_quiescence_required', 'execution_quiesced_generation',
        'execution_quiesced_at'
    }.issubset(request_columns)
    instance_columns = {
        column['name']
        for column in inspector.get_columns('api_server_instances')
    }
    assert 'ordinary_launch_binding_capable' in instance_columns
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


def test_api009_downgrade_guard_retains_head(request_database):
    engine, _ = request_database
    config = migration_utils.get_alembic_config(
        engine, migration_utils.API_REQUESTS_DB_NAME)
    with pytest.raises(RuntimeError, match='009 is additive'):
        alembic_command.downgrade(config, '005')

    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.API_REQUESTS_DB_NAME) == '009'
    inspector = sqlalchemy.inspect(engine)
    assert 'api_resource_actions' in inspector.get_table_names()
    assert 'api_resource_action_attempts' in inspector.get_table_names()
    assert 'api_request_retention_pins' in inspector.get_table_names()


def test_api009_downgrade_guard_retains_binding_evidence(request_database):
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

    with pytest.raises(RuntimeError, match='009 is additive'):
        alembic_command.downgrade(config, '008')

    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.API_REQUESTS_DB_NAME) == '009'
    assert {
        'execution_quiescence_required', 'execution_quiesced_generation',
        'execution_quiesced_at', 'ordinary_launch_association_id'
    } <= columns_before
    assert columns_before == {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('api_requests')
    }
    assert {
        'request_storage_backend', 'request_queue_backend',
        'execution_quiescence_capable', 'ordinary_launch_binding_capable'
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
    monkeypatch.setenv('SKYPILOT_POD_UID', 'pod-uid')
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
    assert row['pod_uid'] == 'pod-uid'
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
    assert (ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME
            in row['supported_handlers'])
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
        monkeypatch.setattr(request_postgres, '_is_owned_executor_process',
                            lambda _pid: True)

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
    assert not restored.execution_quiescence_required
    assert restored.execution_quiesced_generation is None
    assert restored.execution_quiesced_at is None
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
    kill = mock.Mock()
    monkeypatch.setattr(request_postgres.os, 'kill', kill)
    assert api_backend.kill_requests(['remote-cancel']) == ['remote-cancel']
    kill.assert_not_called()
    assert not api_backend.acknowledge_execution_quiescence(
        storage.ExecutionClaim(item.request_id, item.execution_generation,
                               item.claim_token))

    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       executor_instance_id)
    monkeypatch.setattr(request_postgres, '_is_owned_executor_process',
                        lambda _pid: True)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token)
    assert executor_backend.interrupt_cancelled_claim(claim)
    kill.assert_called_once_with(1234, request_postgres.signal.SIGTERM)
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

    def blocking_kill(_pid, _signal):
        signal_entered.set()
        assert signal_release.wait(timeout=5)

    monkeypatch.setattr(request_postgres.os, 'kill', blocking_kill)
    monkeypatch.setattr(request_postgres, '_is_owned_executor_process',
                        lambda _pid: True)

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
    storage.clear_execution_cancellation(
        storage.execution_cancellation_marker_path(1234, claim))


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


def test_cancel_never_signals_a_reused_unowned_pid(request_database,
                                                   monkeypatch):
    _, backend = request_database
    request_id = 'cancel-reused-unowned-pid'
    assert asyncio.run(backend.create_if_not_exists_async(_request(request_id)))
    item = _claim(backend, request_id)
    kill = mock.Mock()
    monkeypatch.setattr(request_postgres.os, 'kill', kill)
    monkeypatch.setattr(request_postgres, '_is_owned_executor_process',
                        lambda _pid: False)

    assert backend.kill_requests([request_id]) == [request_id]

    kill.assert_not_called()
    restored = backend.get_request(request_id)
    assert restored.status is requests.RequestStatus.CANCELLED
    assert restored.execution_quiescence_required
    assert restored.execution_quiesced_generation is None
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
    normal_read = registry.registration_for_handler(core.enabled_clouds)
    bound_launch = registry.registration_for_handler(
        ordinary_launch_request.launch)
    volume_apply = registry.registration_for_handler(volume_core.volume_apply)
    volume_delete = registry.registration_for_handler(volume_core.volume_delete)
    volume_list = registry.registration_for_handler(volume_core.volume_list)
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
    assert daemon.execution_class is registry.ExecutionClass.CONTROLLER
    assert daemon.replay_policy is registry.ReplayPolicy.RECONCILE


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
