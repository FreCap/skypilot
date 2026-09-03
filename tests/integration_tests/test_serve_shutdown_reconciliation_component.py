"""PostgreSQL component regression for a late paid create during shutdown.

This test enters SkyServe's production bound-launch guard, whole-service
``_run_cleanup_and_finalize`` orchestration, controller-owner recovery transfer,
provider-present cleanup worker, and request/tombstone retention paths.  The
only provider-network boundary replaced is the AWS session used for the exact
client-token census and termination call.  It is intentionally a ``component``
test, not an unpaid provider E2E: AWS's exact paid-cleanup path does not yet use
the generic typed provisioner facet, and the launch's provider call is driven
by this harness after the production provider-I/O authority guard.

The regression rejects the pre-fix behavior that removed the HA recovery row
after the first exact cleanup deadline while the provider still reported the
late-created instance as PRESENT.  Its structural controls also require the
real owner transfer, provider cleanup worker, and request-owned GC paths.
"""
# pylint: disable=not-callable,protected-access,redefined-outer-name

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import hashlib
import json
import os
import threading
import time
from typing import Any
from unittest import mock
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from sqlalchemy.ext import asyncio as sqlalchemy_async

from sky import clouds
from sky import global_user_state
from sky import global_user_state_schema
from sky import models
from sky.serve import constants as serve_constants
from sky.serve import non_pool_launch_reconciliation
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import serve_utils
from sky.serve import service
from sky.serve import service_spec
from sky.serve import spot_placer
from sky.server.requests import non_pool_admission
from sky.server.requests import non_pool_launch as non_pool_launch_request
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import requests
from sky.server.requests import storage
from sky.skylet import constants as skylet_constants
from sky.utils import common_utils
from sky.utils import thread_utils
from sky.utils.db import migration_utils

pytestmark = pytest.mark.component

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
testcontainers_postgres = None
if _POSTGRES_URL is None:
    testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

_SERVICE_NAME = 'shutdown-component'
_SERVICE_HASH = 'shutdown-component-hash'
_SERVICE_VERSION = 2
_LIFECYCLE_EPOCH = 4
_ORIGINAL_OWNER = (123, '10.0.0.2')
_RECOVERY_OWNER = (456, '10.0.0.3')
_REPLICA_ID = 3
_REPLICA_RECORD_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_CONTROLLER_ID = uuid.UUID('33333333-3333-4333-8333-333333333333')


@dataclasses.dataclass(frozen=True)
class _Graph:
    engine: sqlalchemy.engine.Engine
    backend: request_postgres.PostgresRequestBackend
    context: ordinary_launch_binding.BoundNonPoolLaunchContext
    allocation: paid_capacity.PaidProviderAllocationReceipt
    provider_identity: dict[str, Any]


class _ProviderClock:
    """Deterministic monotonic time for the bounded provider cleanup loop."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class _FakePaginator:

    def __init__(self, provider: '_DelayedAwsProvider') -> None:
        self._provider = provider

    def paginate(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return [self._provider.census_page()]


class _FakeStsClient:

    def get_caller_identity(self) -> dict[str, str]:
        return {'Account': '123456789012'}


class _FakeEc2Client:
    """Narrow EC2 census and termination boundary used by production code."""

    def __init__(self, provider: '_DelayedAwsProvider') -> None:
        self._provider = provider

    def get_paginator(self, operation: str) -> _FakePaginator:
        assert operation == 'describe_instances'
        return _FakePaginator(self._provider)

    def terminate_instances(self, *, InstanceIds: list[str]) -> None:
        self._provider.request_down(InstanceIds)


class _FakeAwsSession:

    def __init__(self, provider: '_DelayedAwsProvider') -> None:
        self._provider = provider

    def client(self, service_name: str, **_kwargs: Any) -> Any:
        if service_name == 'sts':
            return _FakeStsClient()
        assert service_name == 'ec2'
        return _FakeEc2Client(self._provider)


class _DelayedAwsProvider:
    """One stateful provider allocation with deletion visibility lag."""

    def __init__(self) -> None:
        self.create_entered = threading.Event()
        self.allow_create_return = threading.Event()
        self._lock = threading.Lock()
        self._identity: dict[str, Any] | None = None
        self._state = 'absent'
        self.create_calls = 0
        self.down_calls = 0
        self.census_calls = 0

    def create(self, identity: dict[str, Any]) -> None:
        with self._lock:
            assert self._state == 'absent'
            self._identity = dict(identity)
            self._state = 'running'
            self.create_calls += 1
        self.create_entered.set()
        assert self.allow_create_return.wait(timeout=10)

    def request_down(self, instance_ids: list[str]) -> None:
        with self._lock:
            assert self._identity is not None
            assert instance_ids == ['i-shutdown-component']
            self.down_calls += 1
            # Model an accepted delete whose provider-visible instance remains
            # running beyond this controller attempt's cleanup deadline.

    def make_absent(self) -> None:
        with self._lock:
            assert self.down_calls == 1
            self._state = 'absent'

    def session(self, **_kwargs: Any) -> _FakeAwsSession:
        return _FakeAwsSession(self)

    def census_page(self) -> dict[str, Any]:
        with self._lock:
            self.census_calls += 1
            if self._identity is None or self._state == 'absent':
                return {'Reservations': []}
            identity = dict(self._identity)
            state = self._state
        return {
            'Reservations': [{
                'Instances': [{
                    'InstanceId': 'i-shutdown-component',
                    'ClientToken': identity['client_token'],
                    'InstanceType': identity['instance_type'],
                    'InstanceLifecycle': 'spot',
                    'State': {
                        'Name': state,
                    },
                    'Placement': {
                        'AvailabilityZone': identity['zone'],
                    },
                    'Tags': [{
                        'Key': 'ray-cluster-name',
                        'Value': identity['cluster_name_on_cloud'],
                    }],
                    'BlockDeviceMappings': [{
                        'Ebs': {
                            'DeleteOnTermination': True,
                        },
                    }],
                }],
            }],
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
        except Exception as error:  # pylint: disable=broad-except
            pytest.skip(f'could not start postgres container: {error}')
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_shutdown_test_{uuid.uuid4().hex}'
        admin_engine = sqlalchemy.create_engine(_POSTGRES_URL,
                                                isolation_level='AUTOCOMMIT')
        quoted_database = admin_engine.dialect.identifier_preparer.quote(
            temporary_database)
        try:
            with admin_engine.connect() as connection:
                connection.exec_driver_sql(f'CREATE DATABASE {quoted_database}')
        except Exception as error:  # pylint: disable=broad-except
            admin_engine.dispose()
            pytest.skip(
                f'could not create temporary postgres database: {error}')
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
    config = migration_utils.get_alembic_config(postgres_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, migration_utils.SERVE_VERSION)
    async_url = postgres_engine.url.set(
        drivername='postgresql+asyncpg').render_as_string(hide_password=False)
    async_engine = sqlalchemy_async.create_async_engine(
        async_url, poolclass=sqlalchemy.NullPool)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine',
                        postgres_engine)
    monkeypatch.setattr(request_postgres._DB_MANAGER, '_engine_async',
                        async_engine)
    monkeypatch.setattr(global_user_state._db_manager, '_engine',
                        postgres_engine)
    monkeypatch.setattr(global_user_state._db_manager, '_engine_async',
                        async_engine)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        postgres_engine)
    monkeypatch.setenv(request_postgres.REQUEST_BACKEND_ENV_VAR,
                       request_postgres.POSTGRES_REQUEST_BACKEND)
    monkeypatch.setenv(
        request_postgres.EXECUTION_QUIESCENCE_BACKEND_GUARD_ENV_VAR, 'true')
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       str(uuid.uuid4()))
    backend = request_postgres.PostgresRequestBackend()
    prior_backend = storage._storage_backend
    storage.set_request_backend(backend)
    try:
        yield postgres_engine, backend
    finally:
        storage._storage_backend = prior_backend
        asyncio.run(async_engine.dispose())


def _pool_key() -> str:
    return json.dumps(
        {
            'accelerators': [['l4', 1]],
            'cloud': 'aws',
            'instance_type': 'g6.xlarge',
            'num_nodes': 1,
            'provider_identity': {
                'aws_account_id': '123456789012',
            },
            'region': 'us-east-1',
            'use_spot': True,
            'version': 2,
            'workspace': 'workspace-a',
            'zone': 'us-east-1a',
        },
        sort_keys=True,
        separators=(',', ':'))


def _replica(pool_key: str) -> replica_managers.ReplicaInfo:
    location = spot_placer.Location(cloud=clouds.AWS(),
                                    region='us-east-1',
                                    zone='us-east-1a',
                                    accelerators={'L4': 1},
                                    use_spot=True,
                                    instance_type='g6.xlarge')
    info = replica_managers.ReplicaInfo(replica_id=_REPLICA_ID,
                                        cluster_name=f'{_SERVICE_NAME}-3',
                                        replica_port='8080',
                                        is_spot=True,
                                        location=location,
                                        version=_SERVICE_VERSION,
                                        resources_override=location.to_dict(),
                                        planned_capacity=1)
    info.replica_record_id = str(_REPLICA_RECORD_ID)
    info.is_zero_cost = False
    info.reserved_fill = False
    info.paid_capacity_pool_key = pool_key
    info.status_property.sky_launch_status = common_utils.ProcessStatus.RUNNING
    return info


def _service_spec() -> service_spec.SkyServiceSpec:
    return service_spec.SkyServiceSpec(readiness_path='/health',
                                       ports='8080',
                                       initial_delay_seconds=0,
                                       readiness_timeout_seconds=5,
                                       endpoint_probe_interval_seconds=1,
                                       lb_stream_timeout_seconds=10,
                                       min_replicas=0,
                                       max_replicas=10,
                                       target_concurrency_per_replica=1,
                                       spot_placer='dynamic_fallback')


def _prepare_graph(request_database, monkeypatch: pytest.MonkeyPatch) -> _Graph:
    engine, backend = request_database
    pool_key = _pool_key()
    info = _replica(pool_key)
    controller_config = b'''\
active_workspace: workspace-a
workspaces:
  workspace-a:
    aws:
      profile: prod
'''
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(global_user_state_schema.user_table).values(
                id='tenant-a', name='Tenant A', created_at=int(time.time())))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name=_SERVICE_NAME, epoch=_LIFECYCLE_EPOCH))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name=_SERVICE_NAME,
                workspace='workspace-a',
                status='READY',
                hash=_SERVICE_HASH,
                current_version=_SERVICE_VERSION,
                active_versions=json.dumps([_SERVICE_VERSION]),
                pool=0,
                controller_pid=_ORIGINAL_OWNER[0],
                controller_ip=_ORIGINAL_OWNER[1],
                lifecycle_epoch=_LIFECYCLE_EPOCH,
                controller_incarnation=_CONTROLLER_ID,
                controller_owner_epoch=6,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=5,
                owner_user_id='tenant-a',
                owner_user_name='Tenant A'))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name=_SERVICE_NAME,
                version=_SERVICE_VERSION,
                yaml_content='service:\n  min_replicas: 0\n',
                spec=serve_state._serialize_current_service_spec(
                    _service_spec()),
                controller_config=controller_config,
                controller_config_digest=hashlib.sha256(
                    controller_config).hexdigest(),
                controller_config_snapshot_id='c' * 64,
                controller_applied_at=1.0))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name=_SERVICE_NAME,
                replica_id=_REPLICA_ID,
                replica_state_version=1,
                status='READY',
                version=_SERVICE_VERSION,
                cluster_name=info.cluster_name,
                is_spot=True,
                paid_capacity_pool_key=pool_key,
                replica_state=info.to_storage_dict()))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.paid_capacity_pools_table).values(
                    pool_key=pool_key,
                    current_limit=1,
                    successes_since_resize=0,
                    updated_at=time.time()))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.paid_capacity_claims_table).values(
                    service_name=_SERVICE_NAME,
                    service_hash=_SERVICE_HASH,
                    replica_id=_REPLICA_ID,
                    pool_key=pool_key,
                    priority=1,
                    claimed_at=time.time()))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.serve_ha_recovery_script_table).values(
                    service_name=_SERVICE_NAME, script='recover shutdown'))
        assert ordinary_launch_binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name=_SERVICE_NAME,
            controller_incarnation=_CONTROLLER_ID,
            controller_owner_epoch=6,
            expected_binding_epoch=5,
            participant_barrier_passed=lambda _connection: True,
            legacy_requests_drained=lambda _connection: True) == 6
        connection.execute(
            sqlalchemy.update(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                _SERVICE_NAME, serve_state_schema.replicas_table.c.replica_id ==
                _REPLICA_ID).values(status='PROVISIONING'))

    profile = ordinary_launch_binding.resolve_non_pool_launch_profile(
        _SERVICE_NAME, _REPLICA_ID, _REPLICA_RECORD_ID)
    launch_body = payloads.LaunchBody(
        task='name: shutdown-component\nrun: echo bound\n',
        cluster_name=info.cluster_name,
        is_launched_by_sky_serve_controller=True,
        env_vars={
            skylet_constants.USER_ID_ENV_VAR: 'tenant-a',
            skylet_constants.USER_ENV_VAR: 'Tenant A',
        },
        override_skypilot_config={'active_workspace': 'workspace-a'},
        extra_launch_context={
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: _SERVICE_NAME,
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: _SERVICE_HASH,
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: _SERVICE_VERSION,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY:
                _ORIGINAL_OWNER[0],
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY:
                _ORIGINAL_OWNER[1],
            ordinary_launch_binding.REPLICA_ID_KEY: _REPLICA_ID,
            ordinary_launch_binding.REPLICA_RECORD_ID_KEY:
                str(_REPLICA_RECORD_ID),
            ordinary_launch_binding.LIFECYCLE_EPOCH_KEY: _LIFECYCLE_EPOCH,
            ordinary_launch_binding.BINDING_EPOCH_KEY: 6,
            ordinary_launch_binding.CONTROLLER_INCARNATION_KEY:
                str(_CONTROLLER_ID),
            ordinary_launch_binding.CONTROLLER_OWNER_EPOCH_KEY: 6,
        })
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
    monkeypatch.setattr(
        request_postgres, '_resolved_request_backend_capability', lambda:
        (request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE,
         request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE, True))
    monkeypatch.setattr(request_postgres,
                        'non_pool_launch_binding_fleet_capable',
                        lambda **_kwargs: True)
    admission = request_postgres.bind_and_enqueue_non_pool_launch(
        built.request, built.identity)
    context = ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=built.identity.association_id,
        request_id=built.identity.request_id,
        service_name=built.identity.service_name,
        replica_id=built.identity.replica_id,
        replica_record_id=built.identity.replica_record_id,
        launch_generation=admission.launch_generation,
        input_digest=built.identity.input_digest,
        profile=profile,
        capability_cohort_epoch=built.identity.capability_cohort_epoch,
        capability_profile_set_digest=(
            built.identity.capability_profile_set_digest),
        receipt_protocol_version=built.identity.receipt_protocol_version)
    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == context.association_id)).mappings().one()
    # This is the same pure identity builder used by the terminal provider
    # observer.  The observer itself correctly refuses to expose census scope
    # until cancellation and executor quiescence are durable.
    identity = ordinary_launch_binding.ordinary_paid_aws_provider_identity(
        association, credential_profile=None)
    allocation = paid_capacity.PaidProviderAllocationReceipt(
        association_id=str(context.association_id),
        replica_record_id=str(context.replica_record_id),
        provider='aws',
        workspace=identity['workspace'],
        provider_identity=identity['aws_account_id'],
        region=identity['region'],
        zone=identity['zone'],
        instance_type=identity['instance_type'],
        cluster_name_on_cloud=identity['cluster_name_on_cloud'],
        requested_num_nodes=identity['num_nodes'],
        head_instance_id='i-shutdown-component',
        created_instance_ids=('i-shutdown-component',),
        resumed_instance_ids=(),
        use_spot=True)
    return _Graph(engine, backend, context, allocation, identity)


def _wait_for_status(engine: sqlalchemy.engine.Engine, status: str) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with engine.connect() as connection:
            actual = connection.execute(
                sqlalchemy.select(
                    serve_state_schema.services_table.c.status).where(
                        serve_state_schema.services_table.c.name ==
                        _SERVICE_NAME)).scalar_one_or_none()
        if actual == status:
            return
        time.sleep(0.01)
    raise AssertionError(f'service never reached {status}; last={actual}')


def _run_recovery_retry(spec: service_spec.SkyServiceSpec) -> None:
    teardown = ordinary_launch_binding.begin_service_teardown_if_owner(
        _SERVICE_NAME, _SERVICE_HASH, _ORIGINAL_OWNER)
    assert teardown.authority is not None
    assert service._settle_teardown_recovery_launches(_SERVICE_NAME,
                                                      _SERVICE_HASH,
                                                      _ORIGINAL_OWNER,
                                                      teardown.authority)
    authority = service._claim_teardown_recovery_controller(
        _SERVICE_NAME,
        _SERVICE_HASH,
        _ORIGINAL_OWNER,
        _RECOVERY_OWNER,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_status=serve_state.ServiceStatus.SHUTTING_DOWN,
        binding_expected_recovery_version=_SERVICE_VERSION,
        legacy_expected_recovery_version=_SERVICE_VERSION)
    assert authority is not None
    service._run_cleanup_and_finalize(_SERVICE_NAME, spec, '/unused', 987,
                                      _SERVICE_HASH, *_RECOVERY_OWNER)


def _count(connection: sqlalchemy.engine.Connection, table: sqlalchemy.Table,
           *predicates: Any) -> int:
    return connection.execute(
        sqlalchemy.select(sqlalchemy.func.count()).select_from(table).where(
            *predicates)).scalar_one()


def _graph_counts(graph: _Graph) -> dict[str, int]:
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    scopes = (
        ('service', serve_state_schema.services_table,
         serve_state_schema.services_table.c.name, _SERVICE_NAME),
        ('replica', serve_state_schema.replicas_table,
         serve_state_schema.replicas_table.c.service_name, _SERVICE_NAME),
        ('claim', serve_state_schema.paid_capacity_claims_table,
         serve_state_schema.paid_capacity_claims_table.c.service_name,
         _SERVICE_NAME),
        ('association', associations, associations.c.association_id,
         graph.context.association_id),
        ('request', request_postgres.REQUESTS,
         request_postgres.REQUESTS.c.request_id, graph.context.request_id),
        ('pin', request_postgres.REQUEST_RETENTION_PINS,
         request_postgres.REQUEST_RETENTION_PINS.c.request_id,
         graph.context.request_id),
        ('queue', request_postgres.QUEUE, request_postgres.QUEUE.c.request_id,
         graph.context.request_id),
        ('recovery', serve_state_schema.serve_ha_recovery_script_table,
         serve_state_schema.serve_ha_recovery_script_table.c.service_name,
         _SERVICE_NAME),
    )
    with graph.engine.connect() as connection:
        return {
            name: _count(connection, table, column == value)
            for name, table, column, value in scopes
        }


def _expected_graph_counts(*present: str) -> dict[str, int]:
    names = ('service', 'replica', 'claim', 'association', 'request', 'pin',
             'queue', 'recovery')
    assert set(present) <= set(names)
    return {name: int(name in present) for name in names}


def _association(graph: _Graph) -> dict[str, Any]:
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with graph.engine.connect() as connection:
        return dict(
            connection.execute(
                sqlalchemy.select(associations).where(
                    associations.c.association_id ==
                    graph.context.association_id)).mappings().one())


def test_late_paid_create_shutdown_recovers_to_exact_zero(
        request_database, monkeypatch, tmp_path) -> None:
    graph = _prepare_graph(request_database, monkeypatch)
    provider = _DelayedAwsProvider()
    provider_clock = _ProviderClock()
    monkeypatch.setattr(non_pool_launch_reconciliation.aws_adaptor, 'session',
                        provider.session)
    monkeypatch.setattr(non_pool_launch_reconciliation, 'time', provider_clock)
    monkeypatch.setattr(non_pool_launch_reconciliation,
                        '_AWS_POST_TEARDOWN_ABSENCE_TIMEOUT_SECONDS', 1.0)
    monkeypatch.setattr(non_pool_launch_reconciliation,
                        '_AWS_POST_TEARDOWN_ABSENCE_POLL_SECONDS', 1.0)
    monkeypatch.setattr(non_pool_launch_reconciliation,
                        '_AWS_EMPTY_CENSUS_INTERVAL_SECONDS', 0.0)
    monkeypatch.setattr(ordinary_launch_binding,
                        'ORDINARY_PAID_AWS_ABSENCE_SETTLE_SECONDS', 0)
    monkeypatch.setattr(service,
                        '_BOUND_ORDINARY_LAUNCH_SETTLE_INTERVAL_SECONDS', 0.01)
    monkeypatch.setattr(serve_utils, '_LAUNCH_QUIESCE_POLL_SECONDS', 0.01)
    monkeypatch.setattr(serve_utils, 'get_existing_replica_cluster_names',
                        lambda _infos: set())
    # Kubernetes LB deletion is adjacent infrastructure, not the provider
    # lifecycle under test.  Assert cleanup crosses it without installing a
    # Kubernetes client in this hermetic component test.
    lb_deletes = []
    monkeypatch.setattr(service.lb_k8s, 'get_api_deployment_owner_uid',
                        lambda **_kwargs: 'api-owner')
    monkeypatch.setattr(service.lb_k8s, 'delete_lb_objects',
                        lambda name, **_kwargs: lb_deletes.append(name))
    monkeypatch.setattr(service.skylet_constants, 'PERSISTENT_RUN_SCRIPT_DIR',
                        str(tmp_path / 'run-scripts'))
    generic_terminate = mock.Mock(side_effect=AssertionError(
        'paid PRESENT cleanup bypassed the exact provider worker'))
    monkeypatch.setattr(replica_managers, 'terminate_cluster',
                        generic_terminate)

    queue = request_postgres.PostgresQueueBackend(
        requests.ScheduleType.LONG.value,
        supported_handler_names=frozenset(
            {non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME}))
    candidate = queue.peek_provider_mutation()
    assert candidate is not None
    item = queue.claim_provider_mutation(candidate)
    assert item is not None
    assert graph.backend.try_mark_running(item.request_id, 1234,
                                          item.execution_generation,
                                          item.claim_token, 424242)
    claim = storage.ExecutionClaim(item.request_id, item.execution_generation,
                                   item.claim_token, item.worker_instance_id)
    launch_context = graph.backend.get_request(
        graph.context.request_id).request_body.extra_launch_context

    def _provider_worker() -> None:
        try:
            with ordinary_launch_binding.non_pool_provider_effect_guard(
                    launch_context,
                    claim,
                    claim_validator=(
                        request_postgres.
                        validate_bound_non_pool_launch_claim_in_transaction)):
                provider.create(graph.provider_identity)
                with pytest.raises(
                        ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                        match='no longer authorizes provider effects'):
                    ordinary_launch_binding.record_paid_provider_allocation(
                        launch_context,
                        graph.allocation,
                        request_validator=lambda *_args: True)
        finally:
            assert graph.backend.converge_execution_completion(claim)

    provider_thread = thread_utils.SafeThread(target=_provider_worker)
    provider_thread.start()
    assert provider.create_entered.wait(timeout=10)
    spec = _service_spec()
    cleanup_thread = thread_utils.SafeThread(
        target=service._run_cleanup_and_finalize,
        args=(_SERVICE_NAME, spec, '/unused', 987, _SERVICE_HASH,
              *_ORIGINAL_OWNER))
    cleanup_thread.start()
    _wait_for_status(graph.engine, 'SHUTTING_DOWN')
    provider.allow_create_return.set()
    provider_thread.join(timeout=10)
    cleanup_thread.join(timeout=10)
    assert not provider_thread.is_alive()
    assert not cleanup_thread.is_alive()
    assert provider_thread.exception is None
    assert cleanup_thread.exception is None

    # The first controller accepted exactly one provider delete, but its
    # deadline expired before visibility changed.  Recovery authority and the
    # complete graph must remain durable.
    assert provider.create_calls == provider.down_calls == 1
    census_after_timeout = provider.census_calls
    assert census_after_timeout > 0
    assert serve_state.get_ha_recovery_script(
        _SERVICE_NAME) == 'recover shutdown'
    first = serve_state.get_service_from_name(_SERVICE_NAME)
    assert first is not None
    assert first['status'] is serve_state.ServiceStatus.FAILED_CLEANUP
    assert _graph_counts(graph) == _expected_graph_counts(
        'service', 'replica', 'claim', 'association', 'request', 'pin',
        'recovery')
    terminal_request = graph.backend.get_request(graph.context.request_id)
    assert terminal_request is not None
    assert terminal_request.status is requests.RequestStatus.CANCELLED
    assert terminal_request.execution_quiescence_required
    assert (terminal_request.execution_quiesced_generation ==
            terminal_request.execution_generation)
    assert terminal_request.execution_quiesced_at is not None
    retained_association = _association(graph)
    assert retained_association['resolution'] == 'AMBIGUOUS'
    assert retained_association['provider_evidence'] == 'PRESENT'
    retained_replica = serve_state.get_replica_info_from_id(
        _SERVICE_NAME, _REPLICA_ID)
    assert retained_replica is not None
    assert ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
        retained_replica)

    # Provider visibility converges while the first controller is gone.  The
    # production recovery owner-transfer and finalizer consume the same graph;
    # no second create or down call is permitted.
    provider.make_absent()
    _run_recovery_retry(spec)
    assert provider.create_calls == provider.down_calls == 1
    assert provider.census_calls > census_after_timeout
    assert lb_deletes == [_SERVICE_NAME, _SERVICE_NAME]
    generic_terminate.assert_not_called()
    assert serve_state.get_ha_recovery_script(_SERVICE_NAME) is None
    assert _graph_counts(graph) == _expected_graph_counts(
        'association', 'request')
    settled_association = _association(graph)
    assert settled_association['resolution'] == 'PROJECTED'
    assert settled_association['provider_evidence'] == 'ABSENT'
    assert settled_association['pin_released_at'] is not None
    assert settled_association['tombstone_not_before'] is not None

    # Run the real request-retention and request-owned tombstone GC paths.  The
    # 60-day timestamp change represents elapsed test time only; it does not
    # compose or bypass either production reducer.
    asyncio.run(
        requests.clean_finished_requests_with_retention(
            0, include_request_names=['sky.launch']))
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with graph.engine.begin() as connection:
        # The association outlives its service specifically so request GC can
        # enforce the retention horizon.  Advance only that clock boundary;
        # trigger suppression is required because there is intentionally no
        # longer a live service authority to transfer.
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            sqlalchemy.update(associations).where(
                associations.c.association_id == graph.context.association_id).
            values(tombstone_not_before=(sqlalchemy.func.clock_timestamp() -
                                         datetime.timedelta(seconds=1))))
    assert asyncio.run(graph.backend.gc_request_owned_tombstones()) == 1

    assert _graph_counts(graph) == _expected_graph_counts()
