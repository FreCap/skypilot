"""Mechanical rollout-gate tests for reserved-fill protocol v2."""
# pylint: disable=protected-access

import base64
import contextlib
import dataclasses
import hashlib
import json
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from sky.serve import reserved_capacity_activation
from sky.serve import reserved_capacity_broker as broker
from sky.serve import serve_state
from sky.server.requests import postgres as request_postgres
from sky.skylet import constants as skylet_constants

_DIGEST_A = 'sha256:' + 'a' * 64
_DIGEST_B = 'sha256:' + 'b' * 64
_TOKEN = 'exact-mounted-token'


class _TrackedLock:
    """Test lock that exposes whether its context is currently held."""

    def __init__(self) -> None:
        self.held = False

    @contextlib.contextmanager
    def acquire(self, blocking=True):
        assert blocking
        assert not self.held
        self.held = True
        try:
            yield
        finally:
            self.held = False


def _env(name, value):
    return SimpleNamespace(name=name, value=value, value_from=None)


def _owner_reference(kind, name, uid):
    return SimpleNamespace(api_version='apps/v1',
                           kind=kind,
                           name=name,
                           uid=uid,
                           controller=True)


def _deployment(*,
                kind='api',
                server_role=None,
                request_backend='postgres',
                quiescence_backend_guard='true',
                generation=42,
                observed_generation=42,
                resource_version='deployment-rv-1',
                replicas=2,
                updated_replicas=2,
                ready_replicas=2,
                available_replicas=2,
                unavailable_replicas=None):
    assert kind in ('api', 'controller', 'executor')
    name = {
        'api': 'skypilot-api-server',
        'controller': 'skypilot-controller',
        'executor': 'skypilot-executor',
    }[kind]
    uid = {
        'api': 'deployment-uid',
        'controller': 'controller-deployment-uid',
        'executor': 'executor-deployment-uid',
    }[kind]
    container_name = {
        'api': 'skypilot-api',
        'controller': 'skypilot-controller',
        'executor': 'skypilot-executor',
    }[kind]
    resolved_server_role = server_role or ('all' if kind == 'api' else kind)
    selector_labels = {
        'component': kind,
        'app': 'skypilot',
    }
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace='skypilot',
            generation=generation,
            resource_version=resource_version,
            uid=uid,
            deletion_timestamp=None,
            labels={'app.kubernetes.io/instance': 'release'}),
        spec=SimpleNamespace(
            replicas=replicas,
            selector=SimpleNamespace(match_labels=selector_labels,
                                     match_expressions=None),
            template=SimpleNamespace(
                metadata=SimpleNamespace(labels={
                    **selector_labels,
                    'app.kubernetes.io/instance': 'release',
                }),
                spec=SimpleNamespace(containers=[
                    SimpleNamespace(
                        name=container_name,
                        env=[
                            _env('SKYPILOT_RELEASE_NAME', 'skypilot'),
                            _env('SKYPILOT_API_SERVER_ROLE',
                                 resolved_server_role),
                            _env('SKYPILOT_API_REQUEST_BACKEND',
                                 request_backend),
                            _env(
                                'SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS',
                                quiescence_backend_guard),
                        ]),
                    SimpleNamespace(name='metrics-sidecar', env=[]),
                ]))),
        status=SimpleNamespace(observed_generation=observed_generation,
                               replicas=replicas,
                               updated_replicas=updated_replicas,
                               ready_replicas=ready_replicas,
                               available_replicas=available_replicas,
                               unavailable_replicas=unavailable_replicas))


def _pod(index: int,
         *,
         kind: str = 'api',
         server_role: str | None = None,
         request_backend: str = 'postgres',
         quiescence_backend_guard: str = 'true',
         digest: str = _DIGEST_A,
         resource_version: str | None = None):
    assert kind in ('api', 'controller', 'executor')
    prefix = kind
    container_name = {
        'api': 'skypilot-api',
        'controller': 'skypilot-controller',
        'executor': 'skypilot-executor',
    }[kind]
    resolved_server_role = server_role or ('all' if kind == 'api' else kind)
    replica_set_name = f'skypilot-{kind}-rs'
    replica_set_uid = f'{kind}-rs-uid'
    return SimpleNamespace(
        metadata=SimpleNamespace(name=f'{prefix}-{index}',
                                 namespace='skypilot',
                                 uid=f'{prefix}-pod-uid-{index}',
                                 labels={
                                     'component': kind,
                                     'app': 'skypilot',
                                     'app.kubernetes.io/instance': 'release',
                                 },
                                 resource_version=(resource_version or
                                                   f'{prefix}-pod-rv-{index}'),
                                 deletion_timestamp=None,
                                 owner_references=[
                                     _owner_reference('ReplicaSet',
                                                      replica_set_name,
                                                      replica_set_uid)
                                 ]),
        spec=SimpleNamespace(containers=[
            SimpleNamespace(
                name=container_name,
                env=[
                    _env('SKYPILOT_RELEASE_NAME', 'skypilot'),
                    _env('SKYPILOT_API_SERVER_ROLE', resolved_server_role),
                    _env('SKYPILOT_API_REQUEST_BACKEND', request_backend),
                    _env('SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS',
                         quiescence_backend_guard),
                ]),
            SimpleNamespace(name='metrics-sidecar', env=[]),
        ]),
        status=SimpleNamespace(
            phase='Running',
            conditions=[SimpleNamespace(type='Ready', status='True')],
            container_statuses=[
                SimpleNamespace(name=container_name,
                                ready=True,
                                image_id=('docker-pullable://registry/image@' +
                                          digest)),
                SimpleNamespace(name='metrics-sidecar',
                                ready=True,
                                image_id=('containerd://' + _DIGEST_B)),
            ]))


def _pod_list(*pods):
    return SimpleNamespace(items=list(pods))


def _migration_pod(index=0):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=f'skypilot-db-migration-{index}',
            namespace='skypilot',
            uid=f'migration-pod-uid-{index}',
            labels={
                'app.kubernetes.io/instance': 'release',
                'app.kubernetes.io/component': 'database-migration',
            },
            resource_version=f'migration-pod-rv-{index}',
            deletion_timestamp=None,
            owner_references=[]),
        spec=SimpleNamespace(
            containers=[SimpleNamespace(name='migration', env=[])]),
        status=SimpleNamespace(phase='Running',
                               conditions=[],
                               container_statuses=[]))


def _replica_set():
    return SimpleNamespace(metadata=SimpleNamespace(
        name='skypilot-api-rs',
        namespace='skypilot',
        uid='api-rs-uid',
        owner_references=[
            _owner_reference('Deployment', 'skypilot-api-server',
                             'deployment-uid')
        ]))


def _jwt(payload):

    def encode(value):
        raw = json.dumps(value, separators=(',', ':')).encode('utf-8')
        return base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')

    return f'{encode({"alg": "RS256", "typ": "JWT"})}.{encode(payload)}.sig'


def _identity(name='api-0', uid='api-pod-uid-0'):
    return broker._TokenBoundPodIdentity(namespace='skypilot',
                                         name=name,
                                         uid=uid)


def _install_clients(monkeypatch,
                     lock,
                     deployments,
                     pod_lists,
                     *,
                     identity=None,
                     bound_pod=None,
                     writer_instance_lists=None):
    deployment_snapshots = []
    for snapshot in deployments:
        if isinstance(snapshot, (list, tuple)):
            deployment_snapshots.append(SimpleNamespace(items=list(snapshot)))
        else:
            deployment_snapshots.append(SimpleNamespace(items=[snapshot]))
    pod_lists = list(pod_lists)
    token_identity = identity or _identity()
    if bound_pod is None:
        bound_pod = _pod(0)
    apps_api = mock.Mock()
    apps_api.list_namespaced_deployment.side_effect = deployment_snapshots
    apps_api.read_namespaced_replica_set.return_value = _replica_set()
    core_api = mock.Mock()
    core_api.list_namespaced_pod.side_effect = pod_lists
    core_api.read_namespaced_pod.return_value = bound_pod

    if writer_instance_lists is None:
        writer_instance_lists = [
            _writer_instances(snapshot) for snapshot in pod_lists
        ]
    monkeypatch.setattr(serve_state,
                        'get_recent_reserved_fill_writer_instances',
                        mock.Mock(side_effect=writer_instance_lists))

    @contextlib.contextmanager
    def exact_token_clients(token):
        assert lock.held
        assert token == _TOKEN
        yield core_api, apps_api

    monkeypatch.setattr(broker.locks, 'get_lock', lambda *_args: lock)
    monkeypatch.setattr(serve_state, 'get_database_engine',
                        lambda: _assert_locked_and_return(lock, object()))

    def current_revision(_engine, section):
        assert lock.held
        return {
            broker.migration_utils.SERVE_DB_NAME: '035',
            broker.migration_utils.API_REQUESTS_DB_NAME: '008',
        }[section]

    monkeypatch.setattr(broker.migration_utils, 'get_current_alembic_revision',
                        current_revision)
    monkeypatch.setattr(
        serve_state, 'get_reserved_fill_protocol_state',
        lambda: _assert_locked_and_return(lock, {'protocol_version': 1}))
    monkeypatch.setattr(broker, '_read_token_bound_pod_identity', lambda:
                        (_TOKEN, token_identity))
    monkeypatch.setattr(broker.kubernetes,
                        'in_cluster_core_and_apps_apis_for_token',
                        exact_token_clients)
    return apps_api, core_api


def _writer_instances(pod_list):
    instances = []
    for pod in pod_list.items:
        for container in pod.spec.containers:
            role_entries = [
                entry for entry in container.env
                if entry.name == 'SKYPILOT_API_SERVER_ROLE'
            ]
            if len(role_entries) != 1 or role_entries[0].value not in (
                    'all', 'api', 'controller', 'executor'):
                continue
            instances.append(
                serve_state.ReservedFillWriterInstance(
                    role=role_entries[0].value,
                    instance_id=pod.metadata.uid,
                    pod_name=pod.metadata.name,
                    pod_uid=pod.metadata.uid,
                    version='test-version',
                    ready=True,
                    draining=False,
                    request_storage_backend=(
                        request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE),
                    request_queue_backend=(
                        request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE),
                    execution_quiescence_capable=True))
    return tuple(
        sorted(instances,
               key=lambda item: (item.role, item.pod_uid, item.instance_id)))


def _assert_locked_and_return(lock, value):
    assert lock.held
    return value


def test_mounted_bound_token_identity_is_parsed_without_env_fallback(
        tmp_path, monkeypatch):
    token = _jwt({
        'kubernetes.io': {
            'namespace': 'skypilot',
            'pod': {
                'name': 'api-token-pod',
                'uid': 'api-token-pod-uid',
            },
        },
    })
    token_path = tmp_path / 'token'
    token_path.write_text(token, encoding='ascii')
    monkeypatch.setattr(broker.kubernetes, 'IN_CLUSTER_TOKEN_PATH',
                        str(token_path))
    monkeypatch.setenv('HOSTNAME', 'spoofed-hostname')
    monkeypatch.setenv('SKYPILOT_POD_NAME', 'spoofed-downward-api')

    observed_token, identity = broker._read_token_bound_pod_identity()

    assert observed_token == token
    assert identity == broker._TokenBoundPodIdentity(namespace='skypilot',
                                                     name='api-token-pod',
                                                     uid='api-token-pod-uid')


@pytest.mark.parametrize(('token', 'message'), [
    ('not-a-jwt', 'not a JWT'),
    ('header.!!!.signature', 'payload is malformed'),
    (_jwt({
        'kubernetes.io/serviceaccount/namespace': 'skypilot',
    }), 'not pod-bound'),
    (_jwt({
        'kubernetes.io': {
            'namespace': 'skypilot',
            'pod': {
                'name': 'api-0',
            },
        },
    }), 'no complete pod binding'),
])
def test_activation_rejects_malformed_or_unbound_service_account_token(
        token, message):
    with pytest.raises(broker.ProtocolV2ActivationError, match=message):
        broker._decode_token_bound_pod_identity(token)


def test_exact_token_client_rejects_token_swap(monkeypatch):

    class ConfigException(Exception):
        pass

    fake_kubernetes = SimpleNamespace(config=SimpleNamespace(
        config_exception=SimpleNamespace(ConfigException=ConfigException)),
                                      client=SimpleNamespace())
    monkeypatch.setattr(broker.kubernetes, 'kubernetes', fake_kubernetes)
    configuration = SimpleNamespace(
        api_key={'authorization': 'bearer rotated-token'},
        refresh_api_key_hook=mock.Mock())
    raw_client = SimpleNamespace(configuration=configuration,
                                 rest_client=None,
                                 close=mock.Mock())
    load = mock.Mock(return_value=raw_client)
    monkeypatch.setattr(broker.kubernetes, '_get_api_client', load)

    with pytest.raises(ConfigException, match='changed during client binding'):
        with broker.kubernetes.in_cluster_core_and_apps_apis_for_token(_TOKEN):
            pytest.fail('A token-swapped client must never be yielded.')

    load.assert_called_once_with(broker.kubernetes.in_cluster_context_name())
    raw_client.close.assert_called_once_with()


def test_exact_token_client_shares_frozen_credential_between_apis(monkeypatch):
    refresh = mock.Mock()
    configuration = SimpleNamespace(
        api_key={'authorization': f'Bearer {_TOKEN}'},
        refresh_api_key_hook=refresh)
    raw_client = SimpleNamespace(configuration=configuration,
                                 rest_client=None,
                                 close=mock.Mock())
    monkeypatch.setattr(broker.kubernetes, '_get_api_client',
                        mock.Mock(return_value=raw_client))
    core = SimpleNamespace(kind='core')
    apps = SimpleNamespace(kind='apps')
    core_factory = mock.Mock(return_value=core)
    apps_factory = mock.Mock(return_value=apps)
    fake_kubernetes = SimpleNamespace(
        client=SimpleNamespace(CoreV1Api=core_factory, AppsV1Api=apps_factory))
    monkeypatch.setattr(broker.kubernetes, 'kubernetes', fake_kubernetes)

    with broker.kubernetes.in_cluster_core_and_apps_apis_for_token(
            _TOKEN) as clients:
        assert clients == (core, apps)
        assert configuration.refresh_api_key_hook is None
        core_factory.assert_called_once_with(api_client=raw_client)
        apps_factory.assert_called_once_with(api_client=raw_client)

    raw_client.close.assert_called_once_with()


def test_activation_derives_stable_rollout_proof_under_global_lock(monkeypatch):
    broker.clear_caches()
    lock = _TrackedLock()
    deployment = _deployment()
    pods = _pod_list(_pod(0), _pod(1))
    apps_api, core_api = _install_clients(monkeypatch, lock,
                                          [deployment, deployment],
                                          [pods, pods])
    broker._GRANT_CACHE[('service', None)] = broker._GrantCacheEntry(1, 0.0)

    def persist_protocol(*args, **kwargs):
        assert lock.held
        assert args == (broker.PROTOCOL_V2,)
        expected_cohort = (
            ('api', 'skypilot-api-server', 'skypilot-api', 'api-0',
             'api-pod-uid-0', 'api-pod-rv-0'),
            ('api', 'skypilot-api-server', 'skypilot-api', 'api-1',
             'api-pod-uid-1', 'api-pod-rv-1'),
        )
        expected_processes = [
            ('all', 'api-pod-uid-0', 'api-0', 'api-pod-uid-0', 'test-version',
             True, False,
             request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE,
             request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE, True),
            ('all', 'api-pod-uid-1', 'api-1', 'api-pod-uid-1', 'test-version',
             True, False,
             request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE,
             request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE, True),
        ]
        inventory_hash = hashlib.sha256(
            json.dumps(
                {
                    'pods': expected_cohort,
                    'writer_instances': expected_processes,
                },
                separators=(',', ':'),
                ensure_ascii=True).encode('utf-8')).hexdigest()
        assert kwargs == {
            'expected_protocol_version': broker.PROTOCOL_V1,
            'image_digest': _DIGEST_A,
            'deployment_generation': ('[["api","skypilot-api-server",'
                                      '"42"]]'),
            'deployment_uid': ('[["api","skypilot-api-server",'
                               '"deployment-uid"]]'),
            'pod_inventory_count': 2,
            'pod_inventory_sha256': inventory_hash,
            'changed_at': 1234.0,
        }
        return True

    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        persist_protocol)
    monkeypatch.setattr(broker.time, 'time', lambda: 1234.0)

    assert broker.activate_protocol_v2()
    assert not lock.held
    assert not broker._GRANT_CACHE
    assert apps_api.list_namespaced_deployment.call_args_list == [
        mock.call(namespace='skypilot',
                  _request_timeout=broker.kubernetes.API_TIMEOUT),
        mock.call(namespace='skypilot',
                  _request_timeout=broker.kubernetes.API_TIMEOUT),
    ]
    apps_api.read_namespaced_replica_set.assert_called_once_with(
        name='skypilot-api-rs',
        namespace='skypilot',
        _request_timeout=broker.kubernetes.API_TIMEOUT)
    assert core_api.list_namespaced_pod.call_count == 2
    core_api.read_namespaced_pod.assert_called_once_with(
        name='api-0',
        namespace='skypilot',
        _request_timeout=broker.kubernetes.API_TIMEOUT)
    assert core_api.list_namespaced_pod.call_args_list == [
        mock.call(namespace='skypilot',
                  _request_timeout=broker.kubernetes.API_TIMEOUT),
        mock.call(namespace='skypilot',
                  _request_timeout=broker.kubernetes.API_TIMEOUT),
    ]


def test_activation_attests_ha_api_controller_and_executor_rollout(monkeypatch):
    lock = _TrackedLock()
    api_deployment = _deployment(server_role='api')
    controller_deployment = _deployment(kind='controller')
    executor_deployment = _deployment(kind='executor')
    pods = _pod_list(_pod(0, server_role='api'), _pod(1, server_role='api'),
                     _pod(0, kind='controller'), _pod(1, kind='controller'),
                     _pod(0, kind='executor'), _pod(1, kind='executor'))
    _install_clients(
        monkeypatch,
        lock, [[api_deployment, controller_deployment, executor_deployment],
               [api_deployment, controller_deployment, executor_deployment]],
        [pods, pods],
        bound_pod=pods.items[0])
    persisted = {}

    def persist_protocol(*args, **kwargs):
        assert lock.held
        assert args == (broker.PROTOCOL_V2,)
        persisted.update(kwargs)
        return True

    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        persist_protocol)

    assert broker.activate_protocol_v2()
    assert persisted['image_digest'] == _DIGEST_A
    assert json.loads(persisted['deployment_generation']) == [
        ['api', 'skypilot-api-server', '42'],
        ['controller', 'skypilot-controller', '42'],
        ['executor', 'skypilot-executor', '42'],
    ]
    assert json.loads(persisted['deployment_uid']) == [
        ['api', 'skypilot-api-server', 'deployment-uid'],
        ['controller', 'skypilot-controller', 'controller-deployment-uid'],
        ['executor', 'skypilot-executor', 'executor-deployment-uid'],
    ]
    assert persisted['pod_inventory_count'] == 6
    assert len(persisted['pod_inventory_sha256']) == 64


def test_activation_rejects_missing_recent_executor_lease(monkeypatch):
    lock = _TrackedLock()
    deployments = [
        _deployment(server_role='api'),
        _deployment(kind='controller'),
        _deployment(kind='executor'),
    ]
    pods = _pod_list(_pod(0, server_role='api'), _pod(1, server_role='api'),
                     _pod(0, kind='controller'), _pod(1, kind='controller'),
                     _pod(0, kind='executor'), _pod(1, kind='executor'))
    instances_without_executor = tuple(
        instance for instance in _writer_instances(pods)
        if instance.role != 'executor')
    _install_clients(monkeypatch,
                     lock, [deployments], [pods],
                     bound_pod=pods.items[0],
                     writer_instance_lists=[instances_without_executor])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='database writer-process inventory'):
        broker.activate_protocol_v2()


def test_activation_rejects_missing_recent_ha_api_lease(monkeypatch):
    lock = _TrackedLock()
    deployments = [
        _deployment(server_role='api'),
        _deployment(kind='controller'),
        _deployment(kind='executor'),
    ]
    pods = _pod_list(_pod(0, server_role='api'), _pod(1, server_role='api'),
                     _pod(0, kind='controller'), _pod(1, kind='controller'),
                     _pod(0, kind='executor'), _pod(1, kind='executor'))
    instances_without_api = tuple(
        instance for instance in _writer_instances(pods)
        if instance.role != 'api')
    _install_clients(monkeypatch,
                     lock, [deployments], [pods],
                     bound_pod=pods.items[0],
                     writer_instance_lists=[instances_without_api])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='database writer-process inventory'):
        broker.activate_protocol_v2()


def test_activation_rejects_ha_release_without_controller_deployment(
        monkeypatch):
    lock = _TrackedLock()
    _install_clients(monkeypatch, lock, [_deployment(server_role='api')], [])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='does not have exactly its controller'):
        broker.activate_protocol_v2()


def test_activation_rejects_ha_release_without_executor_deployment(monkeypatch):
    lock = _TrackedLock()
    api_deployment = _deployment(server_role='api')
    controller_deployment = _deployment(kind='controller')
    _install_clients(monkeypatch, lock,
                     [[api_deployment, controller_deployment]], [])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='does not have exactly its executor'):
        broker.activate_protocol_v2()


def test_activation_rejects_incomplete_ha_controller_deployment(monkeypatch):
    lock = _TrackedLock()
    api_deployment = _deployment(server_role='api')
    controller_deployment = _deployment(kind='controller', ready_replicas=1)
    executor_deployment = _deployment(kind='executor')
    _install_clients(
        monkeypatch, lock,
        [[api_deployment, controller_deployment, executor_deployment]], [])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='controller writer Deployment rollout'):
        broker.activate_protocol_v2()


def test_activation_rejects_incomplete_ha_executor_deployment(monkeypatch):
    lock = _TrackedLock()
    api_deployment = _deployment(server_role='api')
    controller_deployment = _deployment(kind='controller')
    executor_deployment = _deployment(kind='executor', ready_replicas=1)
    _install_clients(
        monkeypatch, lock,
        [[api_deployment, controller_deployment, executor_deployment]], [])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='executor writer Deployment rollout'):
        broker.activate_protocol_v2()


@pytest.mark.parametrize('non_postgres_role', ['api', 'controller', 'executor'])
def test_activation_rejects_non_postgres_writer_deployment(
        monkeypatch, non_postgres_role):
    lock = _TrackedLock()
    deployments = [
        _deployment(server_role='api',
                    request_backend=('sqlite' if non_postgres_role == 'api' else
                                     'postgres')),
        _deployment(kind='controller',
                    request_backend=('sqlite' if non_postgres_role
                                     == 'controller' else 'postgres')),
        _deployment(kind='executor',
                    request_backend=('sqlite' if non_postgres_role == 'executor'
                                     else 'postgres')),
    ]
    _install_clients(monkeypatch, lock, [deployments], [])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='does not use the PostgreSQL API request backend'):
        broker.activate_protocol_v2()


@pytest.mark.parametrize('non_postgres_role', ['api', 'controller', 'executor'])
def test_activation_rejects_non_postgres_writer_pod(monkeypatch,
                                                    non_postgres_role):
    lock = _TrackedLock()
    deployments = [
        _deployment(server_role='api'),
        _deployment(kind='controller'),
        _deployment(kind='executor'),
    ]

    def backend(role):
        return 'sqlite' if non_postgres_role == role else 'postgres'

    pods = _pod_list(
        _pod(0, server_role='api', request_backend=backend('api')),
        _pod(1, server_role='api'),
        _pod(0, kind='controller', request_backend=backend('controller')),
        _pod(1, kind='controller'),
        _pod(0, kind='executor', request_backend=backend('executor')),
        _pod(1, kind='executor'))
    _install_clients(monkeypatch,
                     lock, [deployments], [pods],
                     bound_pod=pods.items[0])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='does not use the PostgreSQL API request backend'):
        broker.activate_protocol_v2()


@pytest.mark.parametrize('unguarded_role', ['api', 'controller', 'executor'])
def test_activation_rejects_unguarded_writer_deployment(monkeypatch,
                                                        unguarded_role):
    lock = _TrackedLock()
    deployments = [
        _deployment(server_role='api',
                    quiescence_backend_guard=('false' if unguarded_role == 'api'
                                              else 'true')),
        _deployment(kind='controller',
                    quiescence_backend_guard=('false' if unguarded_role
                                              == 'controller' else 'true')),
        _deployment(kind='executor',
                    quiescence_backend_guard=('false' if unguarded_role
                                              == 'executor' else 'true')),
    ]
    _install_clients(monkeypatch, lock, [deployments], [])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='does not enforce built-in'):
        broker.activate_protocol_v2()


@pytest.mark.parametrize('unguarded_role', ['api', 'controller', 'executor'])
def test_activation_rejects_unguarded_writer_pod(monkeypatch, unguarded_role):
    lock = _TrackedLock()
    deployments = [
        _deployment(server_role='api'),
        _deployment(kind='controller'),
        _deployment(kind='executor'),
    ]

    def guard(role):
        return 'false' if unguarded_role == role else 'true'

    pods = _pod_list(
        _pod(0, server_role='api', quiescence_backend_guard=guard('api')),
        _pod(1, server_role='api'),
        _pod(0, kind='controller',
             quiescence_backend_guard=guard('controller')),
        _pod(1, kind='controller'),
        _pod(0, kind='executor', quiescence_backend_guard=guard('executor')),
        _pod(1, kind='executor'))
    _install_clients(monkeypatch,
                     lock, [deployments], [pods],
                     bound_pod=pods.items[0])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='does not enforce built-in'):
        broker.activate_protocol_v2()


@pytest.mark.parametrize(
    ('field', 'value'),
    (('request_storage_backend', 'test_plugin.backends.CustomRequestBackend'),
     ('request_queue_backend', 'test_plugin.queues.CustomQueueFactory'),
     ('execution_quiescence_capable', False)))
def test_activation_rejects_plugin_overridden_runtime_backend(
        monkeypatch, field, value):
    lock = _TrackedLock()
    pods = _pod_list(_pod(0), _pod(1))
    instances = list(_writer_instances(pods))
    instances[0] = dataclasses.replace(instances[0], **{field: value})
    _install_clients(monkeypatch,
                     lock, [_deployment()], [pods],
                     writer_instance_lists=[tuple(instances)])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='does not attest the built-in PostgreSQL'):
        broker.activate_protocol_v2()


def test_activation_rejects_independently_overridden_controller_image(
        monkeypatch):
    lock = _TrackedLock()
    api_deployment = _deployment(server_role='api')
    controller_deployment = _deployment(kind='controller')
    executor_deployment = _deployment(kind='executor')
    pods = _pod_list(_pod(0, server_role='api'), _pod(1, server_role='api'),
                     _pod(0, kind='controller', digest=_DIGEST_B),
                     _pod(1, kind='controller', digest=_DIGEST_B),
                     _pod(0, kind='executor'), _pod(1, kind='executor'))
    _install_clients(
        monkeypatch,
        lock, [[api_deployment, controller_deployment, executor_deployment]],
        [pods],
        bound_pod=pods.items[0])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='writer fleet has mixed'):
        broker.activate_protocol_v2()


def test_activation_rejects_independently_overridden_executor_image(
        monkeypatch):
    lock = _TrackedLock()
    api_deployment = _deployment(server_role='api')
    controller_deployment = _deployment(kind='controller')
    executor_deployment = _deployment(kind='executor')
    pods = _pod_list(_pod(0, server_role='api'), _pod(1, server_role='api'),
                     _pod(0, kind='controller'), _pod(1, kind='controller'),
                     _pod(0, kind='executor', digest=_DIGEST_B),
                     _pod(1, kind='executor', digest=_DIGEST_B))
    _install_clients(
        monkeypatch,
        lock, [[api_deployment, controller_deployment, executor_deployment]],
        [pods],
        bound_pod=pods.items[0])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='writer fleet has mixed'):
        broker.activate_protocol_v2()


def test_activation_rejects_active_same_release_migration_pod(monkeypatch):
    lock = _TrackedLock()
    pods = _pod_list(_pod(0), _pod(1), _migration_pod())
    _install_clients(monkeypatch, lock, [_deployment()], [pods])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='migration Pod is still active'):
        broker.activate_protocol_v2()


def test_activation_rejects_unattested_live_release_writer_pod(monkeypatch):
    lock = _TrackedLock()
    pods = _pod_list(_pod(0), _pod(1), _pod(9, kind='controller'))
    _install_clients(monkeypatch, lock, [_deployment()], [pods])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='not exactly the attested Deployment cohort'):
        broker.activate_protocol_v2()


def test_activation_rejects_release_writer_pod_without_chart_identity(
        monkeypatch):
    lock = _TrackedLock()
    extra = _pod(9, kind='controller')
    extra.metadata.labels.pop('app.kubernetes.io/instance')
    pods = _pod_list(_pod(0), _pod(1), extra)
    _install_clients(monkeypatch, lock, [_deployment()], [pods])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='crosses Helm release scope'):
        broker.activate_protocol_v2()


def test_activation_rejects_recent_database_writer_outside_rollout(monkeypatch):
    lock = _TrackedLock()
    pods = _pod_list(_pod(0), _pod(1))
    instances = list(_writer_instances(pods))
    instances.append(
        serve_state.ReservedFillWriterInstance(
            role='controller',
            instance_id='old-pod-uid',
            pod_name='old-controller',
            pod_uid='old-pod-uid',
            version='old-version',
            ready=True,
            draining=False,
            request_storage_backend=(
                request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE),
            request_queue_backend=(
                request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE),
            execution_quiescence_capable=(True)))
    _install_clients(monkeypatch,
                     lock, [_deployment()], [pods],
                     writer_instance_lists=[tuple(instances)])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='database writer-process inventory'):
        broker.activate_protocol_v2()


@pytest.mark.parametrize(('ready', 'draining'), [(False, False), (True, True)])
def test_activation_rejects_unhealthy_recent_database_writer(
        monkeypatch, ready, draining):
    lock = _TrackedLock()
    pods = _pod_list(_pod(0), _pod(1))
    instances = list(_writer_instances(pods))
    original = instances[0]
    instances[0] = serve_state.ReservedFillWriterInstance(
        role=original.role,
        instance_id=original.instance_id,
        pod_name=original.pod_name,
        pod_uid=original.pod_uid,
        version=original.version,
        ready=ready,
        draining=draining,
        request_storage_backend=original.request_storage_backend,
        request_queue_backend=original.request_queue_backend,
        execution_quiescence_capable=original.execution_quiescence_capable)
    _install_clients(monkeypatch,
                     lock, [_deployment()], [pods],
                     writer_instance_lists=[tuple(instances)])

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='not one healthy Pod-bound instance'):
        broker.activate_protocol_v2()


@pytest.mark.parametrize('changed_object', ('replica-set', 'deployment'))
def test_activation_rejects_spoofed_owner_uid_chain(monkeypatch,
                                                    changed_object):
    lock = _TrackedLock()
    pods = _pod_list(_pod(0), _pod(1))
    apps_api, _ = _install_clients(monkeypatch, lock, [_deployment()], [pods])
    replica_set = apps_api.read_namespaced_replica_set.return_value
    if changed_object == 'replica-set':
        replica_set.metadata.uid = 'replacement-rs-uid'
    else:
        replica_set.metadata.owner_references[0].uid = 'replacement-deploy-uid'

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match=('ReplicaSet identity changed|Deployment UID '
                              'changed')):
        broker.activate_protocol_v2()


@pytest.mark.parametrize(('deployment', 'message'), [
    (_deployment(observed_generation=41), 'has not observed'),
    (_deployment(updated_replicas=1), 'not fully available'),
    (_deployment(ready_replicas=1), 'not fully available'),
    (_deployment(available_replicas=1), 'not fully available'),
    (_deployment(unavailable_replicas=1), 'unavailable replicas'),
    (_deployment(replicas=0), 'positive integer'),
])
def test_activation_rejects_incomplete_deployment(monkeypatch, deployment,
                                                  message):
    lock = _TrackedLock()
    _install_clients(monkeypatch, lock, [deployment], [])
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV2ActivationError, match=message):
        broker.activate_protocol_v2()
    setter.assert_not_called()


@pytest.mark.parametrize('case',
                         ('terminating', 'pending', 'not-ready',
                          'container-not-ready', 'missing-api-container'))
def test_activation_rejects_unhealthy_pod_cohort(monkeypatch, case):
    lock = _TrackedLock()
    first = _pod(0)
    if case == 'terminating':
        first.metadata.deletion_timestamp = 'now'
    elif case == 'pending':
        first.status.phase = 'Pending'
    elif case == 'not-ready':
        first.status.conditions[0].status = 'False'
    elif case == 'container-not-ready':
        first.status.container_statuses[0].ready = False
    else:
        first.status.container_statuses[0].name = 'other'
    _install_clients(monkeypatch, lock, [_deployment()],
                     [_pod_list(first, _pod(1))])
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV2ActivationError):
        broker.activate_protocol_v2()
    setter.assert_not_called()


def test_activation_rejects_mixed_rollout_image_digests(monkeypatch):
    lock = _TrackedLock()
    _install_clients(monkeypatch, lock, [_deployment()],
                     [_pod_list(_pod(0), _pod(1, digest=_DIGEST_B))])
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV2ActivationError, match='mixed'):
        broker.activate_protocol_v2()
    setter.assert_not_called()


@pytest.mark.parametrize('changed_object', ('deployment', 'pod'))
def test_activation_rejects_unstable_double_read(monkeypatch, changed_object):
    lock = _TrackedLock()
    first_deployment = _deployment()
    second_deployment = _deployment(
        resource_version=('deployment-rv-2' if changed_object ==
                          'deployment' else 'deployment-rv-1'))
    first_pods = _pod_list(_pod(0), _pod(1))
    second_pods = _pod_list(
        _pod(0,
             resource_version=('pod-rv-changed'
                               if changed_object == 'pod' else 'pod-rv-0')),
        _pod(1))
    _install_clients(monkeypatch, lock, [first_deployment, second_deployment],
                     [first_pods, second_pods])
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='changed between'):
        broker.activate_protocol_v2()
    setter.assert_not_called()


@pytest.mark.parametrize(
    'changed_object',
    ('controller-deployment', 'controller-pod', 'controller-process',
     'executor-deployment', 'executor-pod', 'executor-process'))
def test_activation_rejects_unstable_ha_writer_double_read(
        monkeypatch, changed_object):
    lock = _TrackedLock()
    api_deployment = _deployment(server_role='api')
    first_controller = _deployment(kind='controller')
    second_controller = _deployment(
        kind='controller',
        resource_version=('controller-deployment-rv-2' if changed_object
                          == 'controller-deployment' else 'deployment-rv-1'))
    first_executor = _deployment(kind='executor')
    second_executor = _deployment(
        kind='executor',
        resource_version=('executor-deployment-rv-2' if changed_object
                          == 'executor-deployment' else 'deployment-rv-1'))
    first_pods = _pod_list(_pod(0, server_role='api'), _pod(1,
                                                            server_role='api'),
                           _pod(0, kind='controller'), _pod(1,
                                                            kind='controller'),
                           _pod(0, kind='executor'), _pod(1, kind='executor'))
    second_pods = _pod_list(
        _pod(0, server_role='api'), _pod(1, server_role='api'),
        _pod(0,
             kind='controller',
             resource_version=('controller-pod-rv-changed' if changed_object
                               == 'controller-pod' else 'controller-pod-rv-0')),
        _pod(1, kind='controller'),
        _pod(0,
             kind='executor',
             resource_version=('executor-pod-rv-changed' if changed_object
                               == 'executor-pod' else 'executor-pod-rv-0')),
        _pod(1, kind='executor'))
    first_instances = _writer_instances(first_pods)
    second_instances = list(_writer_instances(second_pods))
    if changed_object in ('controller-process', 'executor-process'):
        changed_role = changed_object.removesuffix('-process')
        changed_index = next(
            index for index, instance in enumerate(second_instances)
            if instance.role == changed_role)
        original = second_instances[changed_index]
        second_instances[
            changed_index] = serve_state.ReservedFillWriterInstance(
                role=original.role,
                instance_id=original.instance_id,
                pod_name=original.pod_name,
                pod_uid=original.pod_uid,
                version='changed-version',
                ready=original.ready,
                draining=original.draining,
                request_storage_backend=original.request_storage_backend,
                request_queue_backend=original.request_queue_backend,
                execution_quiescence_capable=(
                    original.execution_quiescence_capable))
    _install_clients(
        monkeypatch,
        lock, [[api_deployment, first_controller, first_executor],
               [api_deployment, second_controller, second_executor]],
        [first_pods, second_pods],
        bound_pod=first_pods.items[0],
        writer_instance_lists=[first_instances,
                               tuple(second_instances)])
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='changed between'):
        broker.activate_protocol_v2()
    setter.assert_not_called()


def test_activation_rejects_spoofed_env_when_token_bound_uid_is_absent(
        monkeypatch):
    lock = _TrackedLock()
    deployment = _deployment()
    pods = _pod_list(_pod(0), _pod(1))
    bound_pod = _pod(0)
    bound_pod.metadata.uid = 'token-pod-uid'
    token_identity = _identity(name='api-0', uid='token-pod-uid')
    _install_clients(monkeypatch,
                     lock, [deployment, deployment], [pods, pods],
                     identity=token_identity,
                     bound_pod=bound_pod)
    monkeypatch.setenv('HOSTNAME', 'api-0')
    monkeypatch.setenv('SKYPILOT_POD_NAME', 'api-0')
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='token-bound pod UID'):
        broker.activate_protocol_v2()
    setter.assert_not_called()


def test_activation_rejects_token_claim_that_mismatches_live_pod(monkeypatch):
    lock = _TrackedLock()
    deployment = _deployment()
    pods = _pod_list(_pod(0), _pod(1))
    bound_pod = _pod(0)
    bound_pod.metadata.uid = 'different-live-uid'
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    _install_clients(monkeypatch,
                     lock, [deployment, deployment], [pods, pods],
                     identity=_identity(),
                     bound_pod=bound_pod)
    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='does not match the live Pod'):
        broker.activate_protocol_v2()
    setter.assert_not_called()


def test_activation_rejects_schema_other_than_exact_035(monkeypatch):
    lock = _TrackedLock()
    monkeypatch.setattr(broker.locks, 'get_lock', lambda *_args: lock)
    monkeypatch.setattr(serve_state, 'get_database_engine',
                        mock.Mock(return_value=object()))
    monkeypatch.setattr(broker.migration_utils, 'get_current_alembic_revision',
                        mock.Mock(return_value='034'))
    token_reader = mock.Mock()
    monkeypatch.setattr(broker, '_read_token_bound_pod_identity', token_reader)
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='exact Serve schema revision 035'):
        broker.activate_protocol_v2()
    token_reader.assert_not_called()
    setter.assert_not_called()


def test_activation_rejects_api_request_schema_other_than_exact_008(
        monkeypatch):
    lock = _TrackedLock()
    engine = object()
    monkeypatch.setattr(broker.locks, 'get_lock', lambda *_args: lock)
    monkeypatch.setattr(serve_state, 'get_database_engine',
                        mock.Mock(return_value=engine))

    def current_revision(observed_engine, section):
        assert lock.held
        assert observed_engine is engine
        return {
            broker.migration_utils.SERVE_DB_NAME: '035',
            broker.migration_utils.API_REQUESTS_DB_NAME: '007',
        }[section]

    monkeypatch.setattr(broker.migration_utils, 'get_current_alembic_revision',
                        current_revision)
    token_reader = mock.Mock()
    monkeypatch.setattr(broker, '_read_token_bound_pod_identity', token_reader)
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='exact API-request schema revision 008'):
        broker.activate_protocol_v2()
    token_reader.assert_not_called()
    setter.assert_not_called()


def test_activation_rejects_already_active_protocol_before_observing_rollout(
        monkeypatch):
    lock = _TrackedLock()
    monkeypatch.setattr(broker.locks, 'get_lock', lambda *_args: lock)
    monkeypatch.setattr(serve_state, 'get_database_engine',
                        mock.Mock(return_value=object()))
    monkeypatch.setattr(
        broker.migration_utils, 'get_current_alembic_revision',
        mock.Mock(
            side_effect=lambda _engine, section: {
                broker.migration_utils.SERVE_DB_NAME: '035',
                broker.migration_utils.API_REQUESTS_DB_NAME: '008',
            }[section]))
    monkeypatch.setattr(serve_state, 'get_reserved_fill_protocol_state',
                        mock.Mock(return_value={'protocol_version': 2}))
    token_reader = mock.Mock()
    monkeypatch.setattr(broker, '_read_token_bound_pod_identity', token_reader)

    with pytest.raises(broker.ProtocolV2ActivationError,
                       match='already active'):
        broker.activate_protocol_v2()
    token_reader.assert_not_called()


def test_activation_does_not_accept_operator_supplied_rollout_proof():
    activation: Any = broker.activate_protocol_v2
    with pytest.raises(TypeError):
        # pylint: disable-next=unexpected-keyword-arg
        activation(activator_pod_name='api-0',
                   namespace='skypilot',
                   image_digest=_DIGEST_A)


def test_activation_cli_accepts_no_identity_input_and_reports_proof(
        monkeypatch):
    monkeypatch.setenv(skylet_constants.ENV_VAR_DB_CONNECTION_URI,
                       'postgresql://configured')
    monkeypatch.setattr(
        serve_state, 'get_database_engine',
        lambda: SimpleNamespace(dialect=SimpleNamespace(name='postgresql')))
    activate = mock.Mock(return_value=True)
    monkeypatch.setattr(reserved_capacity_activation.reserved_capacity_broker,
                        'activate_protocol_v2', activate)
    monkeypatch.setattr(
        serve_state, 'get_reserved_fill_protocol_state', lambda: {
            'protocol_version': 2,
            'deployment_generation': '42',
            'deployment_uid': 'deployment-uid',
            'image_digest': _DIGEST_A,
            'pod_inventory_count': 2,
            'pod_inventory_sha256': 'c' * 64,
        })

    exit_code, output = reserved_capacity_activation.run_cli([])

    assert exit_code == 0
    assert '"protocol_version":2' in output
    assert _DIGEST_A in output
    activate.assert_called_once_with()


def test_activation_cli_rejects_identity_override_arguments():
    with pytest.raises(SystemExit):
        reserved_capacity_activation.run_cli(
            ['--namespace', 'spoofed', '--deployment', 'old-deployment'])
