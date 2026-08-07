"""Tests for Kubernetes adaptor."""
# pylint: disable=protected-access

import concurrent.futures
import contextvars
import gc
import json
import os
import select
import shlex
import signal
import sys
import tempfile
import threading
import time
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sky import exceptions as sky_exceptions
from sky.adaptors import kubernetes
from sky.utils import annotations
from sky.utils import subprocess_utils


def _clear_refresh_interval_cache():
    """Clear the refresh interval env parse cache so tests can change the env."""
    kubernetes._get_kubeconfig_refresh_interval_seconds.cache_clear()  # pylint: disable=protected-access


def test_ssh_node_pool_repair_command_renders_context(monkeypatch):
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    new_client = MagicMock(
        side_effect=config_exception('Expected key current-context'))
    monkeypatch.setattr(kubernetes.kubernetes.config, 'new_client_from_config',
                        new_client)

    with pytest.raises(ValueError) as exc_info:
        kubernetes._get_api_client('ssh-test-pool')  # pylint: disable=protected-access

    message = str(exc_info.value)
    assert 'sky ssh up --infra test-pool' in message
    assert '{context_name}' not in message


@pytest.mark.parametrize(
    'ctor_name, api_func',
    [
        ('CoreV1Api', kubernetes.core_api),
        ('StorageV1Api', kubernetes.storage_api),
        ('RbacAuthorizationV1Api', kubernetes.auth_api),
        ('NetworkingV1Api', kubernetes.networking_api),
        ('CustomObjectsApi', kubernetes.custom_objects_api),
        ('AppsV1Api', kubernetes.apps_api),
        ('BatchV1Api', kubernetes.batch_api),
        ('CustomObjectsApi', kubernetes.custom_resources_api),
    ],
)
def test_typed_clients_cleanup(monkeypatch, ctor_name, api_func):
    """Verify typed client api_client.close() is called on GC."""
    api_client_mock = MagicMock()
    monkeypatch.setattr(kubernetes,
                        '_get_api_client',
                        lambda context=None: api_client_mock)
    monkeypatch.setattr(
        kubernetes.kubernetes.client,
        ctor_name,
        lambda api_client=None: SimpleNamespace(api_client=api_client),
    )
    obj = api_func()
    del obj
    annotations.clear_request_level_cache()
    gc.collect()

    assert api_client_mock.close.call_count == 1


def test_api_client_cleanup(monkeypatch):
    """Verify ApiClient.close() is called on GC."""
    instances = []

    class FakeApiClient:

        def __init__(self):
            self.close = MagicMock()
            instances.append(self)

    # Mock _get_api_client to return a FakeApiClient instance
    monkeypatch.setattr(kubernetes,
                        '_get_api_client',
                        lambda context=None: FakeApiClient())
    # Also mock the ApiClient class so isinstance checks work
    monkeypatch.setattr(kubernetes.kubernetes.client, 'ApiClient',
                        FakeApiClient)

    client = kubernetes.api_client()
    del client
    annotations.clear_request_level_cache()
    gc.collect()

    assert len(instances) == 1
    assert instances[0].close.call_count == 1


def test_watch_cleanup(monkeypatch):
    """Verify Watch.stop() and underlying api_client.close() are called."""
    api_client_mock = MagicMock()
    monkeypatch.setattr(kubernetes,
                        '_get_api_client',
                        lambda context=None: api_client_mock)

    class FakeWatch:

        def __init__(self, return_type=None):
            self._raw_return_type = return_type

    monkeypatch.setattr(kubernetes.kubernetes.watch, 'Watch', FakeWatch)

    w = kubernetes.watch()
    # Keep a handle to the underlying watch object so we can assert its
    # _api_client.close() was called on GC.
    underlying = w._client
    del w
    annotations.clear_request_level_cache()
    gc.collect()

    assert underlying._api_client.close.call_count == 1


def test_kubeconfig_refresh_interval_refreshes_client(monkeypatch):
    """When SKYPILOT_KUBECONFIG_REFRESH_INTERVAL_SECONDS is set and interval
    has elapsed, the next API call refreshes the client and closes the old one.
    """
    api_clients = []

    def track_get_api_client(_context=None):
        mock_client = MagicMock()
        api_clients.append(mock_client)
        return mock_client

    def make_mock_core_api(api_client=None):
        # Explicit client object so getattr(self._client, 'list_namespaced_pod') always works.
        client = SimpleNamespace(api_client=api_client)
        client.list_namespaced_pod = MagicMock(return_value=MagicMock())
        return client

    monkeypatch.setattr(kubernetes, '_get_api_client', track_get_api_client)
    monkeypatch.setattr(kubernetes.kubernetes.client, 'CoreV1Api',
                        make_mock_core_api)

    monkeypatch.setenv(kubernetes.KUBECONFIG_REFRESH_INTERVAL_ENV_VAR, '1')
    _clear_refresh_interval_cache()

    # Time 0 when wrapper is created (mark refreshed), then 10 when we call
    # method so interval has elapsed.
    time_values = iter([0.0, 10.0, 10.0, 10.0])
    monkeypatch.setattr(time, 'time', lambda: next(time_values, 10.0))

    annotations.clear_request_level_cache()
    api = kubernetes.core_api()
    assert len(api_clients) == 1

    api.list_namespaced_pod(namespace='default')

    assert len(api_clients) == 2, 'Refresh should have created a second client'
    assert api_clients[0].close.call_count == 1, (
        'Old client should be closed when refresh runs')


def test_kubeconfig_refresh_interval_no_refresh_when_interval_not_elapsed(
        monkeypatch):
    """When interval has not elapsed, no refresh runs (single client)."""
    api_clients = []

    def track_get_api_client(_context=None):
        mock_client = MagicMock()
        api_clients.append(mock_client)
        return mock_client

    def make_mock_core_api(api_client=None):
        client = SimpleNamespace(api_client=api_client)
        client.list_namespaced_pod = MagicMock(return_value=MagicMock())
        return client

    monkeypatch.setattr(kubernetes, '_get_api_client', track_get_api_client)
    monkeypatch.setattr(kubernetes.kubernetes.client, 'CoreV1Api',
                        make_mock_core_api)

    monkeypatch.setenv(kubernetes.KUBECONFIG_REFRESH_INTERVAL_ENV_VAR, '10')
    _clear_refresh_interval_cache()

    # Time 0 at creation, then 5 when we call method (5 < 10, no refresh).
    time_values = iter([0.0, 5.0, 5.0])
    monkeypatch.setattr(time, 'time', lambda: next(time_values, 5.0))

    annotations.clear_request_level_cache()
    api = kubernetes.core_api()
    assert len(api_clients) == 1

    api.list_namespaced_pod(namespace='default')

    assert len(api_clients) == 1
    assert api_clients[0].close.call_count == 0


def test_kubeconfig_refresh_interval_disabled_when_unset(monkeypatch):
    """When env var is unset, interval refresh is disabled."""
    monkeypatch.delenv(kubernetes.KUBECONFIG_REFRESH_INTERVAL_ENV_VAR,
                       raising=False)
    _clear_refresh_interval_cache()

    interval = kubernetes._get_kubeconfig_refresh_interval_seconds()  # pylint: disable=protected-access
    assert interval == 0.0


def test_kubeconfig_refresh_interval_invalid_value_disables_refresh(
        monkeypatch):
    """Invalid env value disables refresh and returns 0."""
    monkeypatch.setenv(kubernetes.KUBECONFIG_REFRESH_INTERVAL_ENV_VAR,
                       'not-a-number')
    _clear_refresh_interval_cache()

    interval = kubernetes._get_kubeconfig_refresh_interval_seconds()  # pylint: disable=protected-access
    assert interval == 0.0


class _IdentityApiClient:
    """Minimal raw ApiClient target carrying one physical-cluster UID."""

    def __init__(self, uid):
        self.uid = uid
        self.identity_reads = 0
        self.provider_calls = []
        self.close = MagicMock()


class _IdentityCoreApi:
    """Core facade that separates identity reads from provider calls."""

    def __init__(self, api_client):
        self.api_client = api_client

    def read_namespace(self, name, **kwargs):
        del kwargs
        assert name == 'kube-system'
        self.api_client.identity_reads += 1
        return SimpleNamespace(metadata=SimpleNamespace(
            uid=self.api_client.uid))

    def list_namespaced_pod(self, namespace):
        self.api_client.provider_calls.append(
            ('list_namespaced_pod', namespace))
        return SimpleNamespace(items=[])


class _IdentityTypedApi:
    """Non-Core facade used to prove every typed target is fenced."""

    def __init__(self, api_client):
        self.api_client = api_client

    def create_namespaced_deployment(self, namespace, body):
        self.api_client.provider_calls.append(
            ('create_namespaced_deployment', namespace, body))
        return SimpleNamespace()

    def create_namespaced_ingress(self, namespace, body):
        self.api_client.provider_calls.append(
            ('create_namespaced_ingress', namespace, body))
        return SimpleNamespace()


def _install_identity_client_fakes(monkeypatch, uids):
    clients = []
    remaining_uids = iter(uids)

    def get_api_client(_context=None):
        client = _IdentityApiClient(next(remaining_uids))
        clients.append(client)
        return client

    monkeypatch.setattr(kubernetes.kubernetes.client, 'ApiClient',
                        _IdentityApiClient)
    monkeypatch.setattr(kubernetes, '_get_api_client', get_api_client)
    monkeypatch.setattr(kubernetes.kubernetes.client, 'CoreV1Api',
                        _IdentityCoreApi)
    return clients


def _install_physical_fence_capture(monkeypatch, tmp_path, uid='physical-a'):
    """Avoid ambient kubeconfig/network dependencies in fence unit tests."""
    capture_index = 0

    def capture(_context):
        nonlocal capture_index
        capture_index += 1
        path = tmp_path / f'fence-{capture_index}.yaml'
        path.write_text('captured', encoding='utf-8')
        return str(path)

    monkeypatch.setattr(kubernetes, '_capture_fenced_kubeconfig', capture)
    monkeypatch.setattr(kubernetes, '_new_api_client_from_fence_capture',
                        lambda _context, _path: _IdentityApiClient(uid))
    monkeypatch.setattr(kubernetes.kubernetes.client, 'CoreV1Api',
                        _IdentityCoreApi)


def test_physical_uid_fence_rechecks_refreshed_exact_client(
        monkeypatch, tmp_path):
    """A retarget after the executor read cannot reach a provider method."""
    annotations.clear_request_level_cache()
    _install_physical_fence_capture(monkeypatch, tmp_path)
    clients = _install_identity_client_fakes(monkeypatch,
                                             ['physical-a', 'physical-b'])
    monkeypatch.setenv(kubernetes.KUBECONFIG_REFRESH_INTERVAL_ENV_VAR, '1')
    _clear_refresh_interval_cache()
    monkeypatch.setattr(time, 'time', lambda: 0.0)
    with kubernetes.physical_cluster_uid_fence('phx-context', 'physical-a'):
        api = kubernetes.core_api('phx-context')
        # Models the final executor observation on the initially selected
        # target. The wrapper records that exact underlying ApiClient proof.
        api.list_namespaced_pod('default')
        monkeypatch.setattr(time, 'time', lambda: 2.0)
        with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError):
            api.list_namespaced_pod('default')
    _clear_refresh_interval_cache()

    assert len(clients) == 2
    assert clients[0].provider_calls == [('list_namespaced_pod', 'default')]
    assert clients[0].identity_reads == 1
    # A rejected refresh candidate is closed; the last proved client remains
    # installed so a later call can retry without a use-after-close race.
    assert clients[0].close.call_count == 0
    assert clients[1].identity_reads == 1
    assert clients[1].provider_calls == []
    assert clients[1].close.call_count == 1


@pytest.mark.parametrize(
    ('constructor_name', 'api_getter', 'method_name'),
    [
        ('AppsV1Api', kubernetes.apps_api, 'create_namespaced_deployment'),
        ('NetworkingV1Api', kubernetes.networking_api,
         'create_namespaced_ingress'),
    ],
)
def test_physical_uid_fence_checks_each_typed_client_before_call(
        monkeypatch, tmp_path, constructor_name, api_getter, method_name):
    """A newly loaded non-Core client cannot mutate a retargeted cluster."""
    annotations.clear_request_level_cache()
    _install_physical_fence_capture(monkeypatch, tmp_path)
    clients = _install_identity_client_fakes(monkeypatch,
                                             ['physical-a', 'physical-b'])
    monkeypatch.setattr(kubernetes.kubernetes.client, constructor_name,
                        _IdentityTypedApi)

    with kubernetes.physical_cluster_uid_fence('phx-context', 'physical-a'):
        core = kubernetes.core_api('phx-context')
        core.list_namespaced_pod('default')
        retargeted_api = api_getter('phx-context')
        with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError):
            getattr(retargeted_api, method_name)('default', {})

    assert clients[0].identity_reads == 1
    assert clients[0].provider_calls == [('list_namespaced_pod', 'default')]
    assert clients[1].identity_reads == 1
    assert clients[1].provider_calls == []


def test_physical_uid_fence_refcounts_and_rejects_conflicts(
        monkeypatch, tmp_path):
    _install_physical_fence_capture(monkeypatch, tmp_path)
    context = 'phx-context'
    with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
        assert kubernetes._active_physical_cluster_uid_fence(  # pylint: disable=protected-access
            context) == 'physical-a'
        with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
            assert kubernetes._active_physical_cluster_uid_fence(  # pylint: disable=protected-access
                context) == 'physical-a'
        with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError):
            with kubernetes.physical_cluster_uid_fence(context, 'physical-b'):
                raise AssertionError('conflicting scope must not be entered')
    assert kubernetes._active_physical_cluster_uid_fence(  # pylint: disable=protected-access
        context) is None


def test_in_cluster_alias_and_provider_none_share_fence(monkeypatch, tmp_path):
    _install_physical_fence_capture(monkeypatch, tmp_path)
    context = kubernetes.in_cluster_context_name()

    with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
        target = kubernetes.active_physical_cluster_command_target(None)
        assert target is not None
        assert target.context_name == context
        assert target.provider_context is None
        assert target.in_cluster
        assert target.kubeconfig_path is None
        assert kubernetes._active_physical_cluster_uid_fence(  # pylint: disable=protected-access
            None) == 'physical-a'


def test_provider_calls_share_verified_client_concurrently(
        monkeypatch, tmp_path):
    """The first proof is exclusive; provider calls are then parallel."""
    _install_physical_fence_capture(monkeypatch, tmp_path)
    entered = 0
    entered_lock = threading.Lock()
    both_entered = threading.Event()
    release = threading.Event()

    class ConcurrentCoreApi(_IdentityCoreApi):

        def list_namespaced_pod(self, namespace):
            nonlocal entered
            with entered_lock:
                entered += 1
                if entered == 2:
                    both_entered.set()
            assert release.wait(timeout=5)
            return super().list_namespaced_pod(namespace)

    clients = _install_identity_client_fakes(monkeypatch, ['physical-a'])
    monkeypatch.setattr(kubernetes.kubernetes.client, 'CoreV1Api',
                        ConcurrentCoreApi)
    with kubernetes.physical_cluster_uid_fence('phx-context', 'physical-a'):
        api = kubernetes.core_api('phx-context')
        with subprocess_utils.ContextThreadPoolExecutor(
                max_workers=2) as executor:
            futures = [
                executor.submit(api.list_namespaced_pod, 'default')
                for _ in range(2)
            ]
            assert both_entered.wait(timeout=5)
            release.set()
            for future in futures:
                future.result(timeout=5)

    assert clients[0].identity_reads == 1
    assert len(clients[0].provider_calls) == 2


def test_unleased_overlap_cannot_borrow_active_fence(monkeypatch, tmp_path):
    """A raw worker without the caller lease fails closed during overlap."""
    _install_physical_fence_capture(monkeypatch, tmp_path)
    context = 'unleased-active-overlap-context'

    with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                kubernetes.active_physical_cluster_command_target, context)
            with pytest.raises(
                    kubernetes.KubernetesPhysicalClusterFenceBusyError
            ) as exc_info:
                future.result(timeout=5)
            assert exc_info.value.context == context
            assert exc_info.value.failure_generation == 0

    # Ordinary callers regain ambient behavior once no fenced scope exists.
    assert kubernetes.active_physical_cluster_command_target(context) is None


def test_unleased_overlap_fails_closed_while_fence_initializes(
        monkeypatch, tmp_path):
    """Ambient callers cannot race the capture-before-publication window."""
    context = 'unleased-initializing-overlap-context'
    capture_started = threading.Event()
    release_capture = threading.Event()
    initializer_errors = []

    def _capture(_context):
        capture_started.set()
        assert release_capture.wait(timeout=5)
        path = tmp_path / 'initializing-fence.yaml'
        path.write_text('captured', encoding='utf-8')
        return str(path)

    monkeypatch.setattr(kubernetes, '_capture_fenced_kubeconfig', _capture)
    monkeypatch.setattr(
        kubernetes, '_new_api_client_from_fence_capture',
        lambda _context, _path: _IdentityApiClient('physical-a'))
    monkeypatch.setattr(kubernetes.kubernetes.client, 'CoreV1Api',
                        _IdentityCoreApi)
    ambient_loader = MagicMock()
    monkeypatch.setattr(kubernetes.kubernetes.config, 'new_client_from_config',
                        ambient_loader)

    def _initialize() -> None:
        try:
            with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
                pass
        except BaseException as error:  # pylint: disable=broad-exception-caught
            initializer_errors.append(error)

    initializer = threading.Thread(target=_initialize, daemon=True)
    initializer.start()
    assert capture_started.wait(timeout=5)
    try:
        with pytest.raises(kubernetes.KubernetesPhysicalClusterFenceBusyError,
                           match='being initialized') as exc_info:
            kubernetes._get_api_client(context)
        assert exc_info.value.context == context
        assert exc_info.value.failure_generation == 0
        with pytest.raises(kubernetes.KubernetesPhysicalClusterFenceBusyError,
                           match='being initialized'):
            with kubernetes.physical_cluster_uid_fence(
                    context, 'physical-a', wait_for_initializer=False):
                raise AssertionError('nonwaiting fence joined initializer')
        ambient_loader.assert_not_called()
    finally:
        release_capture.set()
        initializer.join(timeout=5)

    assert not initializer.is_alive()
    assert not initializer_errors
    assert kubernetes.active_physical_cluster_command_target(context) is None


def test_tokenless_waiter_retries_only_after_owner_retires(
        monkeypatch, tmp_path):
    _install_physical_fence_capture(monkeypatch, tmp_path)
    context = 'retirement-context'
    busy_seen = threading.Event()
    wait_done = threading.Event()
    result = []

    def wait_for_owner() -> None:
        try:
            kubernetes.active_physical_cluster_command_target(context)
        except kubernetes.KubernetesPhysicalClusterFenceBusyError as error:
            busy_seen.set()
            result.append(
                kubernetes.wait_for_physical_cluster_uid_fence_retirement(
                    context,
                    time.monotonic() + 2, error.failure_generation))
        finally:
            wait_done.set()

    with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
        waiter = threading.Thread(target=wait_for_owner)
        waiter.start()
        assert busy_seen.wait(timeout=2)
        assert not wait_done.wait(timeout=0.05)
    assert wait_done.wait(timeout=2)
    waiter.join(timeout=2)
    assert not waiter.is_alive()
    assert result == [True]


def test_physical_fence_retirement_wait_fails_closed(monkeypatch, tmp_path):
    _install_physical_fence_capture(monkeypatch, tmp_path)
    context = 'retirement-failure-context'

    def expired_wait() -> bool:
        try:
            kubernetes.active_physical_cluster_command_target(context)
        except kubernetes.KubernetesPhysicalClusterFenceBusyError as error:
            return kubernetes.wait_for_physical_cluster_uid_fence_retirement(
                context, time.monotonic(), error.failure_generation)
        raise AssertionError('active owner was not reported busy')

    with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            assert executor.submit(expired_wait).result(timeout=2) is False
        with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError,
                           match='cannot wait for ambient'):
            kubernetes.wait_for_physical_cluster_uid_fence_retirement(
                context, time.monotonic(), 0)

    with kubernetes._PHYSICAL_CLUSTER_UID_FENCES_CONDITION:
        kubernetes._PHYSICAL_CLUSTER_UID_FENCE_FAILURE_GENERATIONS[context] = 1
    try:
        with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError,
                           match='failed before ambient retry'):
            kubernetes.wait_for_physical_cluster_uid_fence_retirement(
                context,
                time.monotonic() + 1, 0)
    finally:
        with kubernetes._PHYSICAL_CLUSTER_UID_FENCES_CONDITION:
            kubernetes._PHYSICAL_CLUSTER_UID_FENCE_FAILURE_GENERATIONS.pop(
                context, None)


def test_fenced_caller_cannot_escape_to_an_unleased_context(
        monkeypatch, tmp_path):
    """A fence for A cannot silently fall through to ambient context B."""
    _install_physical_fence_capture(monkeypatch, tmp_path)
    ambient_loader = MagicMock()
    monkeypatch.setattr(kubernetes.kubernetes.config, 'new_client_from_config',
                        ambient_loader)

    with kubernetes.physical_cluster_uid_fence('context-a', 'physical-a'):
        with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError,
                           match='unleased context'):
            kubernetes._get_api_client('context-b')

    ambient_loader.assert_not_called()


def test_nested_fence_cannot_expand_authority_to_second_context(
        monkeypatch, tmp_path):
    """One launch or cleanup scope has authority for exactly one context."""
    _install_physical_fence_capture(monkeypatch, tmp_path)

    with kubernetes.physical_cluster_uid_fence('context-a', 'physical-a'):
        with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError,
                           match='cannot acquire a second'):
            with kubernetes.physical_cluster_uid_fence('context-b',
                                                       'physical-a'):
                raise AssertionError('Cross-context fence must not be entered')
        target_a = kubernetes.active_physical_cluster_command_target(
            'context-a')
        assert target_a is not None
        assert target_a.context_name == 'context-a'


def test_stale_copied_fence_context_cannot_reenter_or_borrow(
        monkeypatch, tmp_path):
    """A copied old token stays invalid across a new same-UID scope."""
    _install_physical_fence_capture(monkeypatch, tmp_path)
    context = 'phx-context'

    def enter_fence() -> None:
        with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
            raise AssertionError('A stale fence token entered a new scope.')

    with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
        stale_context = contextvars.copy_context()

    with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError):
        stale_context.run(kubernetes.active_physical_cluster_command_target,
                          context)
    with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError):
        stale_context.run(enter_fence)

    with kubernetes.physical_cluster_uid_fence(context, 'physical-a'):
        with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError):
            stale_context.run(kubernetes.active_physical_cluster_command_target,
                              context)
        with pytest.raises(kubernetes.KubernetesPhysicalClusterIdentityError):
            stale_context.run(enter_fence)


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='requires os.fork')
def test_physical_fence_registry_resets_locked_state_after_fork(tmp_path):
    """A child starts ambient even when a vanished thread owned the mutex."""
    context = 'fork-context'
    capture_path = tmp_path / 'parent-capture.yaml'
    capture_path.write_text('parent-owned', encoding='utf-8')
    target = kubernetes.PhysicalClusterUidFenceTarget(
        context_name=context,
        provider_context=context,
        expected_uid='parent-uid',
        kubeconfig_path=str(capture_path),
        in_cluster=False,
        token='parent-token')
    with kubernetes._PHYSICAL_CLUSTER_UID_FENCES_CONDITION:
        kubernetes._PHYSICAL_CLUSTER_UID_FENCES[context] = (
            kubernetes._PhysicalClusterUidFenceEntry(target, 1))
        kubernetes._PHYSICAL_CLUSTER_UID_FENCE_INITIALIZERS[
            'initializing-context'] = 'parent-uid'
        kubernetes._PHYSICAL_CLUSTER_UID_FENCE_FAILURE_GENERATIONS[context] = 7
    token_reset = kubernetes._PHYSICAL_CLUSTER_UID_FENCE_TOKENS.set(
        {context: 'parent-token'})

    parent_locked = threading.Event()
    parent_release = threading.Event()

    def hold_parent_lock() -> None:
        with kubernetes._PHYSICAL_CLUSTER_UID_FENCES_CONDITION:
            parent_locked.set()
            assert parent_release.wait(timeout=10)

    holder = threading.Thread(target=hold_parent_lock)
    holder.start()
    assert parent_locked.wait(timeout=2)
    read_fd, write_fd = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        os.close(read_fd)
        result = b'failure'
        try:
            assert kubernetes.active_physical_cluster_command_target(
                context) is None
            assert not kubernetes._PHYSICAL_CLUSTER_UID_FENCES
            assert not kubernetes._PHYSICAL_CLUSTER_UID_FENCE_INITIALIZERS
            assert not kubernetes._PHYSICAL_CLUSTER_UID_FENCE_FAILURE_GENERATIONS
            assert kubernetes._PHYSICAL_CLUSTER_UID_FENCE_TOKENS.get() is None
            assert capture_path.exists()
            result = b'ok'
        except BaseException:  # pylint: disable=broad-exception-caught
            pass
        os.write(write_fd, result)
        os.close(write_fd)
        os._exit(0)

    os.close(write_fd)
    try:
        readable, _, _ = select.select([read_fd], [], [], 5)
        if not readable:
            os.kill(child_pid, signal.SIGKILL)
            pytest.fail('forked child deadlocked on physical fence registry')
        assert os.read(read_fd, 16) == b'ok'
    finally:
        os.close(read_fd)
        parent_release.set()
        holder.join(timeout=2)
        _, status = os.waitpid(child_pid, 0)
        kubernetes._PHYSICAL_CLUSTER_UID_FENCE_TOKENS.reset(token_reset)
        with kubernetes._PHYSICAL_CLUSTER_UID_FENCES_CONDITION:
            kubernetes._PHYSICAL_CLUSTER_UID_FENCES.clear()
            kubernetes._PHYSICAL_CLUSTER_UID_FENCE_INITIALIZERS.clear()
            kubernetes._PHYSICAL_CLUSTER_UID_FENCE_FAILURE_GENERATIONS.clear()
    assert not holder.is_alive()
    assert os.waitstatus_to_exitcode(status) == 0
    assert capture_path.exists()


def test_wrapper_leaves_capture_for_fresh_ambient_client(monkeypatch, tmp_path):
    """A cached wrapper cannot retain a fence target after scope exit."""
    annotations.clear_request_level_cache()
    _install_physical_fence_capture(monkeypatch, tmp_path)
    clients = _install_identity_client_fakes(monkeypatch,
                                             ['physical-a', 'physical-b'])

    with kubernetes.physical_cluster_uid_fence('phx-context', 'physical-a'):
        api = kubernetes.core_api('phx-context')
        api.list_namespaced_pod('fenced')
    api.list_namespaced_pod('ambient')

    assert clients[0].provider_calls == [('list_namespaced_pod', 'fenced')]
    assert clients[0].close.call_count == 1
    assert clients[1].identity_reads == 0
    assert clients[1].provider_calls == [('list_namespaced_pod', 'ambient')]


def _write_exec_kubeconfig(tmp_path, script, *, command=sys.executable):
    path = tmp_path / 'exec-kubeconfig.json'
    path.write_text(json.dumps({
        'apiVersion': 'v1',
        'kind': 'Config',
        'clusters': [{
            'cluster': {
                'server': 'https://bounded.example.test',
                'insecure-skip-tls-verify': True,
            },
            'name': 'bounded-cluster',
        }],
        'contexts': [{
            'context': {
                'cluster': 'bounded-cluster',
                'user': 'bounded-user',
            },
            'name': 'bounded-context',
        }],
        'current-context': 'bounded-context',
        'users': [{
            'name': 'bounded-user',
            'user': {
                'exec': {
                    'apiVersion': 'client.authentication.k8s.io/v1beta1',
                    'command': command,
                    'args': ['-c', script],
                },
            },
        }],
    }),
                    encoding='utf-8')
    return path


class _ExecConfig(dict):

    def safe_get(self, key):
        return self.get(key)


class _ExecCluster:
    value = {}


def _object_graph_contains_marker(root, marker: str) -> bool:
    """Traverses an object graph without invoking application properties."""
    seen: set[int] = set()
    pending = [root]
    while pending:
        if len(seen) >= 50000:
            raise AssertionError('Credential object-graph traversal exceeded.')
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, str):
            if marker in current:
                return True
            continue
        if isinstance(current, bytes):
            if marker.encode() in current:
                return True
            continue
        if current is None or isinstance(current, (bool, int, float, complex)):
            continue
        if isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())
            continue
        if isinstance(current, (list, tuple, set, frozenset)):
            pending.extend(current)
            continue
        if isinstance(current, BaseException):
            pending.extend(current.args)
            pending.extend((current.__cause__, current.__context__))
            pending.extend(vars(current).values())
            continue
        if isinstance(current, types.MethodType):
            pending.append(current.__self__)
            continue
        if isinstance(current, (types.ModuleType, types.FunctionType, type)):
            continue
        try:
            pending.extend(vars(current).values())
        except TypeError:
            continue
    return False


def _kubernetes_traceback_contains(error: BaseException, marker: str) -> bool:
    """Searches adaptor frame object graphs, excluding the test caller."""
    adaptor_path = os.path.realpath(kubernetes.__file__)
    seen: set[int] = set()
    pending: list[BaseException | None] = [error]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if marker in repr(current):
            return True
        pending.extend((current.__cause__, current.__context__))
        traceback = current.__traceback__
        while traceback is not None:
            frame = traceback.tb_frame
            if os.path.realpath(frame.f_code.co_filename) == adaptor_path:
                for value in frame.f_locals.values():
                    if _object_graph_contains_marker(value, marker):
                        return True
            traceback = traceback.tb_next
    return False


def _assert_value_free_credential_error(error: BaseException, marker: str,
                                        captured_logs: str) -> None:
    assert marker not in str(error)
    assert marker not in repr(error)
    assert not _kubernetes_traceback_contains(error, marker)
    assert marker not in json.dumps(sky_exceptions.serialize_exception(error),
                                    sort_keys=True)
    assert marker not in captured_logs


def test_bounded_core_api_exec_credential_has_no_transparent_refresh(
        monkeypatch, tmp_path):
    response = json.dumps({
        'apiVersion': 'client.authentication.k8s.io/v1beta1',
        'kind': 'ExecCredential',
        'status': {
            'token': 'bounded-token',
            'expirationTimestamp': '2099-01-01T00:00:00Z',
        },
    })
    path = _write_exec_kubeconfig(
        tmp_path, f'import sys; sys.stdout.write({response!r})')
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))
    monkeypatch.setattr(kubernetes.time, 'time', lambda: 100.0)
    monkeypatch.setattr(kubernetes.time, 'monotonic', lambda: 1000.0)
    fences = []

    core, refresh_deadline = kubernetes._bounded_core_api(  # pylint: disable=protected-access
        'bounded-context',
        exec_credential_timeout_seconds=2,
        provider_fence=lambda: fences.append('fence'))
    try:
        configuration = core.api_client.configuration
        assert configuration.host == 'https://bounded.example.test'
        assert configuration.api_key['authorization'] == (
            'Bearer bounded-token')
        assert configuration.refresh_api_key_hook is None
        assert refresh_deadline == 4070909695.0
        assert fences == ['fence'] * 4
    finally:
        core.api_client.close()


def test_bounded_in_cluster_core_api_schedules_explicit_token_rotation(
        monkeypatch):
    current = [100.0]
    configuration = SimpleNamespace(refresh_api_key_hook=object())
    api_client = SimpleNamespace(configuration=configuration)
    core = SimpleNamespace(api_client=api_client)
    fences = []
    monkeypatch.setattr(kubernetes, '_get_api_client',
                        lambda _context: api_client)
    monkeypatch.setattr(kubernetes.kubernetes.client, 'CoreV1Api',
                        lambda api_client: core)
    monkeypatch.setattr(kubernetes.time, 'monotonic', lambda: current[0])

    def provider_fence():
        fences.append('fence')
        if len(fences) == 2:
            current[0] = 130.0

    result, refresh_deadline = kubernetes._bounded_core_api(
        kubernetes.in_cluster_context_name(),
        exec_credential_timeout_seconds=2,
        provider_fence=provider_fence)

    assert result is core
    assert configuration.refresh_api_key_hook is None
    assert refresh_deadline == 160.0
    assert fences == ['fence', 'fence']


def test_bounded_in_cluster_core_api_closes_partial_client_on_failure(
        monkeypatch, caplog):
    marker = 'IN_CLUSTER_CONSTRUCTOR_SECRET'
    configuration = SimpleNamespace(refresh_api_key_hook=MagicMock())
    api_client = SimpleNamespace(configuration=configuration, close=MagicMock())
    monkeypatch.setattr(kubernetes, '_get_api_client',
                        lambda _context: api_client)
    monkeypatch.setattr(kubernetes.kubernetes.client, 'CoreV1Api',
                        MagicMock(side_effect=RuntimeError(marker)))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)

    with pytest.raises(config_exception) as exc_info:
        kubernetes._bounded_core_api(kubernetes.in_cluster_context_name(),
                                     exec_credential_timeout_seconds=2,
                                     provider_fence=lambda: None)

    error = exc_info.value
    assert str(error) == (
        'Failed to load bounded in-cluster Kubernetes credentials.')
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_value_free_credential_error(error, marker, caplog.text)
    api_client.close.assert_called_once_with()


def test_exec_credential_expiry_converts_once_to_monotonic_deadline(
        monkeypatch):
    monkeypatch.setattr(kubernetes.time, 'time', lambda: 1000.0)
    monkeypatch.setattr(kubernetes.time, 'monotonic', lambda: 500.0)

    valid, deadline = kubernetes._exec_credential_refresh_deadline(
        '1970-01-01T00:20:00Z')

    assert valid
    assert deadline == 695.0


def test_exec_credential_expiry_requires_one_bounded_api_call(monkeypatch):
    monkeypatch.setattr(kubernetes.time, 'time', lambda: 1000.0)
    monkeypatch.setattr(kubernetes.time, 'monotonic', lambda: 500.0)

    valid, deadline = kubernetes._exec_credential_refresh_deadline(
        '1970-01-01T00:16:44Z')

    assert not valid
    assert deadline is None


def test_provider_fenced_refresh_ignores_later_wall_clock_jumps(monkeypatch):
    client = SimpleNamespace(api_client=MagicMock())
    clocks = {'wall': 100.0, 'monotonic': 1000.0}
    monkeypatch.setattr(kubernetes.time, 'time', lambda: clocks['wall'])
    monkeypatch.setattr(kubernetes.time, 'monotonic',
                        lambda: clocks['monotonic'])
    monkeypatch.setattr(kubernetes, '_bounded_core_api',
                        lambda *_args, **_kwargs: (client, 1060.0))
    monkeypatch.setattr(kubernetes, '_get_kubeconfig_refresh_interval_seconds',
                        lambda: 0)
    core = kubernetes.ProviderFencedCoreApi('bounded-context',
                                            exec_credential_timeout_seconds=2,
                                            provider_fence=lambda: None)

    clocks['wall'] = -3500.0
    clocks['monotonic'] = 1059.0
    assert not core._should_refresh()
    clocks['wall'] = 5000.0
    assert not core._should_refresh()
    clocks['monotonic'] = 1060.0
    assert core._should_refresh()


def test_provider_fenced_interval_uses_only_monotonic_elapsed_time(monkeypatch):
    client = SimpleNamespace(api_client=MagicMock())
    clocks = {'wall': 100.0, 'monotonic': 1000.0}
    monkeypatch.setattr(kubernetes.time, 'time', lambda: clocks['wall'])
    monkeypatch.setattr(kubernetes.time, 'monotonic',
                        lambda: clocks['monotonic'])
    monkeypatch.setattr(kubernetes, '_bounded_core_api',
                        lambda *_args, **_kwargs: (client, None))
    monkeypatch.setattr(kubernetes, '_get_kubeconfig_refresh_interval_seconds',
                        lambda: 60)
    core = kubernetes.ProviderFencedCoreApi('bounded-context',
                                            exec_credential_timeout_seconds=2,
                                            provider_fence=lambda: None)

    clocks['wall'] = 5000.0
    clocks['monotonic'] = 1059.0
    assert not core._should_refresh()
    clocks['wall'] = -3500.0
    assert not core._should_refresh()
    clocks['monotonic'] = 1060.0
    assert core._should_refresh()


def test_bounded_core_api_terminates_timed_out_exec_credential(
        monkeypatch, tmp_path, caplog):
    marker = 'TIMEOUT_CREDENTIAL_SECRET'
    script = ('import os,time; '
              'os.write(1, b"TIMEOUT_CREDENTIAL_" + b"SECRET"); time.sleep(60)')
    path = _write_exec_kubeconfig(tmp_path, script)
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    started = time.monotonic()

    with pytest.raises(config_exception, match='bounded timeout') as exc_info:
        kubernetes._bounded_core_api(  # pylint: disable=protected-access
            'bounded-context',
            exec_credential_timeout_seconds=0.05,
            provider_fence=lambda: None)

    assert time.monotonic() - started < 2
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_value_free_credential_error(exc_info.value, marker, caplog.text)


@pytest.mark.skipif(os.name == 'nt',
                    reason='POSIX process-group descendant regression')
def test_bounded_core_api_terminates_timed_out_exec_credential_group(
        monkeypatch, tmp_path, caplog):
    marker = 'DESCENDANT_CREDENTIAL_SECRET'
    parent_pid = tmp_path / 'parent-pid'
    child_started = tmp_path / 'child-started'
    child_survived = tmp_path / 'child-survived'
    parent_script = (f'printf "%s" "$$" > {shlex.quote(str(parent_pid))}; '
                     f'(sleep 1.5; printf survived > '
                     f'{shlex.quote(str(child_survived))}) & '
                     f'child=$!; printf "%s" "$child" > '
                     f'{shlex.quote(str(child_started))}; '
                     'printf DESCENDANT_CREDENTIAL_SECRET; sleep 60')
    path = _write_exec_kubeconfig(tmp_path, parent_script, command='/bin/sh')
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)

    with pytest.raises(config_exception, match='bounded timeout') as exc_info:
        kubernetes._bounded_core_api('bounded-context',
                                     exec_credential_timeout_seconds=1,
                                     provider_fence=lambda: None)

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_value_free_credential_error(exc_info.value, marker, caplog.text)
    assert parent_pid.exists()
    assert child_started.exists()
    with pytest.raises(ChildProcessError):
        os.waitpid(int(parent_pid.read_text()), os.WNOHANG)
    time.sleep(1.6)
    assert not child_survived.exists()


@pytest.mark.parametrize(
    ('stream', 'message'),
    ((1, 'response exceeds 1 MiB'), (2, 'diagnostics exceed 1 MiB')),
    ids=('stdout', 'stderr'))
def test_bounded_core_api_stops_exec_credential_output_flood(
        monkeypatch, tmp_path, caplog, stream, message):
    marker = 'FLOOD_CREDENTIAL_SECRET'
    script = (f'import os,time; os.write({stream}, '
              f'b"FLOOD_CREDENTIAL_" + b"SECRET" + '
              f'b"x" * {kubernetes._MAX_EXEC_CREDENTIAL_OUTPUT_BYTES + 1}); '
              'time.sleep(60)')
    path = _write_exec_kubeconfig(tmp_path, script)
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    started = time.monotonic()

    with pytest.raises(config_exception, match=message) as exc_info:
        kubernetes._bounded_core_api('bounded-context',
                                     exec_credential_timeout_seconds=5,
                                     provider_fence=lambda: None)

    assert time.monotonic() - started < 2
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_value_free_credential_error(exc_info.value, marker, caplog.text)


def test_bounded_core_api_does_not_reflect_exec_credential_stderr(
        monkeypatch, tmp_path, caplog):
    marker = 'credential=secret'
    path = _write_exec_kubeconfig(
        tmp_path,
        'import sys; sys.stderr.write("credential=" + "secret"); sys.exit(7)')
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)

    with pytest.raises(config_exception) as exc_info:
        kubernetes._bounded_core_api('bounded-context',
                                     exec_credential_timeout_seconds=2,
                                     provider_fence=lambda: None)

    assert str(exc_info.value) == 'exec: process returned 7'
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_value_free_credential_error(exc_info.value, marker, caplog.text)


@pytest.mark.parametrize(
    'script', ('import sys; sys.stdout.write("credential=secret-not-json")',
               'import os; os.write(1, b"\\xffcredential=secret-not-utf8")'),
    ids=('invalid-json', 'invalid-utf8'))
def test_bounded_core_api_does_not_retain_malformed_credential_output(
        monkeypatch, tmp_path, script, caplog):
    path = _write_exec_kubeconfig(tmp_path, script)
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)

    with pytest.raises(config_exception) as exc_info:
        kubernetes._bounded_core_api('bounded-context',
                                     exec_credential_timeout_seconds=2,
                                     provider_fence=lambda: None)

    error = exc_info.value
    assert str(error) == 'exec: failed to decode process output'
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_value_free_credential_error(error, 'credential=secret', caplog.text)


@pytest.mark.parametrize(
    ('response', 'message', 'marker'),
    ((None, 'exec: malformed response object', None),
     ('SCALAR_SECRET', 'exec: malformed response object', 'SCALAR_SECRET'),
     (['LIST_SECRET'], 'exec: malformed response object', 'LIST_SECRET'),
     ({
         'untrusted': 'MISSING_FIELD_SECRET',
     }, "exec: malformed response. missing key 'apiVersion'",
      'MISSING_FIELD_SECRET'), ({
          'apiVersion': 'client.authentication.k8s.io/v1beta1',
          'kind': 'ExecCredential',
          'status': ['STATUS_SECRET'],
      }, 'exec: malformed response status', 'STATUS_SECRET')),
    ids=('null', 'scalar', 'list', 'missing-field', 'malformed-status'))
def test_bounded_core_api_rejects_malformed_response_shapes_value_free(
        monkeypatch, tmp_path, caplog, response, message, marker):
    encoded = json.dumps(response)
    path = _write_exec_kubeconfig(tmp_path,
                                  f'import sys; sys.stdout.write({encoded!r})')
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)

    with pytest.raises(config_exception) as exc_info:
        kubernetes._bounded_core_api('bounded-context',
                                     exec_credential_timeout_seconds=2,
                                     provider_fence=lambda: None)

    error = exc_info.value
    assert str(error) == message
    assert error.__cause__ is None
    assert error.__context__ is None
    if marker is not None:
        _assert_value_free_credential_error(error, marker, caplog.text)


@pytest.mark.parametrize(
    ('response', 'message', 'marker'),
    (({
        'apiVersion': 'VERSION_SECRET',
        'kind': 'ExecCredential',
        'status': {
            'token': 'TOKEN_SECRET',
        },
    }, 'exec: response api version does not match request', 'VERSION_SECRET'),
     ({
         'apiVersion': 'client.authentication.k8s.io/v1beta1',
         'kind': 'KIND_SECRET',
         'status': {
             'token': 'TOKEN_SECRET',
         },
     }, 'exec: response kind is not ExecCredential', 'KIND_SECRET'),
     ({
         'apiVersion': 'client.authentication.k8s.io/v1beta1',
         'kind': 'ExecCredential',
         'status': {
             'expirationTimestamp': 'EXPIRY_ONLY_SECRET',
         },
     }, 'exec: missing token or complete client certificate data',
      'EXPIRY_ONLY_SECRET'),
     ({
         'apiVersion': 'client.authentication.k8s.io/v1beta1',
         'kind': 'ExecCredential',
         'status': {
             'token': 'TOKEN_WITH_BAD_EXPIRY_SECRET',
             'expirationTimestamp': 'BAD_EXPIRY_SECRET',
         },
     }, 'exec: unusable credential expiration timestamp', 'BAD_EXPIRY_SECRET')),
    ids=('api-version', 'kind', 'missing-credential', 'expiration'))
def test_bounded_core_api_validates_complete_value_free_response(
        monkeypatch, tmp_path, caplog, response, message, marker):
    encoded = json.dumps(response)
    path = _write_exec_kubeconfig(tmp_path,
                                  f'import sys; sys.stdout.write({encoded!r})')
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)

    with pytest.raises(config_exception) as exc_info:
        kubernetes._bounded_core_api('bounded-context',
                                     exec_credential_timeout_seconds=2,
                                     provider_fence=lambda: None)

    error = exc_info.value
    assert str(error) == message
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_value_free_credential_error(error, marker, caplog.text)


class _InjectedCredentialPipe:
    """Injects deterministic bytes and read failures into a reader thread."""

    def __init__(self, values):
        self._values = iter(values)

    def read(self, _size):
        value = next(self._values)
        if isinstance(value, BaseException):
            raise value
        return value


class _InjectedCredentialProcess:
    """Minimal process double for bounded exec-credential collection."""

    def __init__(self, stdout_values):
        self.stdout = _InjectedCredentialPipe(stdout_values)
        self.stderr = _InjectedCredentialPipe((b'',))
        self.returncode = 0
        self.pid = 999999999

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        del timeout
        return self.returncode


class _InjectedCredentialAbort(BaseException):
    """Exercises isolation for failures outside the Exception hierarchy."""


@pytest.mark.parametrize(
    ('stdout_values', 'marker'),
    (((OSError('OSERROR_BEFORE_OUTPUT_SECRET'),),
      'OSERROR_BEFORE_OUTPUT_SECRET'),
     ((b'{"apiVersion":', OSError('OSERROR_AFTER_PARTIAL_SECRET')),
      'OSERROR_AFTER_PARTIAL_SECRET'), ((json.dumps({
          'apiVersion': 'client.authentication.k8s.io/v1beta1',
          'kind': 'ExecCredential',
          'status': {
              'token': 'TOKEN_AFTER_OSERROR_SECRET',
          },
      }).encode(), OSError('OSERROR_AFTER_VALID_SECRET')),
                                        'OSERROR_AFTER_VALID_SECRET'),
     ((RuntimeError('RUNTIME_BEFORE_OUTPUT_SECRET'),),
      'RUNTIME_BEFORE_OUTPUT_SECRET'),
     ((b'{"apiVersion":', RuntimeError('RUNTIME_AFTER_PARTIAL_SECRET')),
      'RUNTIME_AFTER_PARTIAL_SECRET'), ((json.dumps({
          'apiVersion': 'client.authentication.k8s.io/v1beta1',
          'kind': 'ExecCredential',
          'status': {
              'token': 'TOKEN_AFTER_RUNTIME_FAILURE_SECRET',
          },
      }).encode(), RuntimeError('RUNTIME_AFTER_VALID_SECRET')),
                                        'RUNTIME_AFTER_VALID_SECRET')),
    ids=('oserror-before-output', 'oserror-after-partial',
         'oserror-after-valid', 'runtime-before-output',
         'runtime-after-partial', 'runtime-after-valid'))
def test_exec_credential_pipe_read_failure_is_fail_closed(
        monkeypatch, caplog, stdout_values, marker):
    process = _InjectedCredentialProcess(stdout_values)
    monkeypatch.setattr(kubernetes.subprocess, 'Popen',
                        lambda *_args, **_kwargs: process)
    config = _ExecConfig(command='ignored',
                         apiVersion='client.authentication.k8s.io/v1beta1')
    result = kubernetes._run_bounded_exec_credential_isolated(
        config,
        _ExecCluster(),
        None,
        timeout_seconds=2,
        provider_fence=lambda: None)

    assert result.status is None
    assert result.failure_message == (
        'exec: failed to read credential process output')
    assert result.control_error is None
    assert marker not in repr(result)
    assert marker not in caplog.text


@pytest.mark.parametrize('failure_type',
                         (RuntimeError, _InjectedCredentialAbort),
                         ids=('exception', 'base-exception'))
def test_exec_credential_unexpected_collection_failure_reaps_process(
        monkeypatch, caplog, failure_type):
    marker = 'POLL_FAILURE_SECRET'
    process = _InjectedCredentialProcess((b'',))
    process.poll = MagicMock(side_effect=failure_type(marker))
    process.wait = MagicMock(return_value=0)
    monkeypatch.setattr(kubernetes.subprocess, 'Popen',
                        lambda *_args, **_kwargs: process)
    config = _ExecConfig(command='ignored',
                         apiVersion='client.authentication.k8s.io/v1beta1')
    result = kubernetes._run_bounded_exec_credential_isolated(
        config,
        _ExecCluster(),
        None,
        timeout_seconds=2,
        provider_fence=lambda: None)

    assert result.status is None
    assert result.failure_message == (
        'exec: credential command failed inside isolation boundary')
    assert result.control_error is None
    assert marker not in repr(result)
    assert marker not in caplog.text
    process.wait.assert_called()


def test_bounded_core_api_accepts_complete_client_certificate(
        monkeypatch, tmp_path):
    response = json.dumps({
        'apiVersion': 'client.authentication.k8s.io/v1beta1',
        'kind': 'ExecCredential',
        'status': {
            'clientCertificateData': 'CERTIFICATE_DATA',
            'clientKeyData': 'PRIVATE_KEY_DATA',
        },
    })
    path = _write_exec_kubeconfig(
        tmp_path, f'import sys; sys.stdout.write({response!r})')
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))

    core, refresh_deadline = kubernetes._bounded_core_api(
        'bounded-context',
        exec_credential_timeout_seconds=2,
        provider_fence=lambda: None)

    try:
        configuration = core.api_client.configuration
        assert refresh_deadline is None
        with open(configuration.cert_file, encoding='utf-8') as cert_file:
            assert cert_file.read() == 'CERTIFICATE_DATA'
        with open(configuration.key_file, encoding='utf-8') as key_file:
            assert key_file.read() == 'PRIVATE_KEY_DATA'
        assert configuration.refresh_api_key_hook is None
    finally:
        core.api_client.close()


@pytest.mark.parametrize('failure_type',
                         (RuntimeError, _InjectedCredentialAbort),
                         ids=('exception', 'base-exception'))
def test_bounded_core_api_drain_wins_without_hidden_credential_context(
        monkeypatch, caplog, failure_type):

    class _User(dict):

        @property
        def value(self):
            return self

    response = json.dumps({
        'apiVersion': 'client.authentication.k8s.io/v1beta1',
        'kind': 'ExecCredential',
        'status': {
            'token': 'LOADER_TOKEN_SECRET',
        },
    })
    config = _ExecConfig(
        command=sys.executable,
        apiVersion='client.authentication.k8s.io/v1beta1',
        args=['-c', f'import sys; sys.stdout.write({response!r})'])
    user = _User(exec=config)

    def fail_loader(_configuration):
        raise failure_type('loader failed')

    loader = SimpleNamespace(_user=user,
                             _cluster=SimpleNamespace(path=None, value={}),
                             _temp_file_path=None,
                             _get_base_path=lambda _path: None,
                             load_and_set=fail_loader)
    monkeypatch.setattr(kubernetes.kubernetes.config.kube_config,
                        '_get_kube_config_loader_for_yaml_file',
                        lambda *_args, **_kwargs: loader)
    fence_count = 0

    def provider_fence():
        nonlocal fence_count
        fence_count += 1
        if fence_count == 4:
            raise RuntimeError('drain won')

    with pytest.raises(RuntimeError, match='drain won') as exc_info:
        kubernetes._bounded_core_api('bounded-context',
                                     exec_credential_timeout_seconds=2,
                                     provider_fence=provider_fence)

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_value_free_credential_error(error, 'LOADER_TOKEN_SECRET',
                                        caplog.text)


def _provider_client_with_token(marker, *, method=None):
    configuration = SimpleNamespace(
        api_key={'authorization': f'Bearer {marker}'},
        api_key_prefix={'authorization': 'Bearer'},
        username=None,
        password=None,
        cert_file=None,
        key_file=None,
        refresh_api_key_hook=None)
    api_client = SimpleNamespace(configuration=configuration,
                                 default_headers={},
                                 cookie=None,
                                 close=MagicMock())
    return SimpleNamespace(api_client=api_client,
                           list_node=method or MagicMock())


def _provider_fenced_core_for_test(client, deadline):
    core = object.__new__(kubernetes.ProviderFencedCoreApi)
    core._context = 'bounded-context'
    core._exec_credential_timeout_seconds = 2
    core._refresh_lock = threading.Lock()
    core._client = client
    core._credential_refresh_deadline = deadline
    core._last_refresh_monotonic = 0
    return core


def test_bounded_in_cluster_core_rejects_deadline_consumed_by_final_fence(
        monkeypatch, caplog):
    marker = 'EXPIRED_IN_CLUSTER_TOKEN_SECRET'
    current = [100.0]
    client = _provider_client_with_token(marker)
    core = SimpleNamespace(api_client=client.api_client)
    monkeypatch.setattr(kubernetes, '_get_api_client',
                        lambda _context: client.api_client)
    monkeypatch.setattr(kubernetes.kubernetes.client, 'CoreV1Api',
                        lambda api_client: core)
    monkeypatch.setattr(kubernetes.time, 'monotonic', lambda: current[0])
    fence_count = 0

    def provider_fence():
        nonlocal fence_count
        fence_count += 1
        if fence_count == 2:
            current[0] = 160.0

    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    with pytest.raises(config_exception) as exc_info:
        kubernetes._bounded_core_api(kubernetes.in_cluster_context_name(),
                                     exec_credential_timeout_seconds=2,
                                     provider_fence=provider_fence)

    assert str(exc_info.value) == (
        'Kubernetes credential expired before client admission.')
    assert client.api_client.configuration.api_key == {}
    client.api_client.close.assert_called_once_with()
    _assert_value_free_credential_error(exc_info.value, marker, caplog.text)


def test_provider_fenced_constructor_rejects_elapsed_candidate(
        monkeypatch, caplog):
    marker = 'EXPIRED_CONSTRUCTOR_TOKEN_SECRET'
    client = _provider_client_with_token(marker)
    monkeypatch.setattr(kubernetes, '_bounded_core_api',
                        lambda *_args, **_kwargs: (client, 99.0))
    monkeypatch.setattr(kubernetes.time, 'monotonic', lambda: 100.0)
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)

    with pytest.raises(config_exception) as exc_info:
        kubernetes.ProviderFencedCoreApi('bounded-context',
                                         exec_credential_timeout_seconds=2,
                                         provider_fence=lambda: None)

    assert client.api_client.configuration.api_key == {}
    client.api_client.close.assert_called_once_with()
    _assert_value_free_credential_error(exc_info.value, marker, caplog.text)


def test_provider_fenced_refresh_failure_scrubs_installed_credential(
        monkeypatch, caplog):
    marker = 'OLD_EXEC_TOKEN_SECRET'
    initial = _provider_client_with_token(marker)
    core = _provider_fenced_core_for_test(initial, None)
    monkeypatch.setattr(core, '_should_refresh', lambda: True)
    monkeypatch.setattr(
        kubernetes, '_bounded_core_api_isolated',
        lambda *_args, **_kwargs: kubernetes._BoundedCoreApiResult(
            None, None, 'fixed refresh failure', None))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)

    with pytest.raises(config_exception) as exc_info:
        core.call_with_provider_fence('list_node', lambda: None, None)

    assert core._client is None
    assert initial.api_client.configuration.api_key == {}
    initial.api_client.close.assert_called_once_with()
    _assert_value_free_credential_error(exc_info.value, marker, caplog.text)


def test_provider_fenced_refresh_rejects_deadline_consumed_by_fence(
        monkeypatch, caplog):
    old_marker = 'OLD_REFRESH_TOKEN_SECRET'
    new_marker = 'NEW_REFRESH_TOKEN_SECRET'
    initial = _provider_client_with_token(old_marker)
    replacement = _provider_client_with_token(new_marker)
    core = _provider_fenced_core_for_test(initial, None)
    current = [100.0]
    monkeypatch.setattr(core, '_should_refresh', lambda: True)
    monkeypatch.setattr(kubernetes.time, 'monotonic', lambda: current[0])
    monkeypatch.setattr(
        kubernetes, '_bounded_core_api_isolated',
        lambda *_args, **_kwargs: kubernetes._BoundedCoreApiResult(
            replacement, 101.0, None, None))
    fence_count = 0

    def provider_fence():
        nonlocal fence_count
        fence_count += 1
        if fence_count == 2:
            current[0] = 101.0

    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    with pytest.raises(config_exception) as exc_info:
        core.call_with_provider_fence('list_node', provider_fence, None)

    assert core._client is None
    initial.list_node.assert_not_called()
    replacement.list_node.assert_not_called()
    assert initial.api_client.configuration.api_key == {}
    assert replacement.api_client.configuration.api_key == {}
    initial.api_client.close.assert_called_once_with()
    replacement.api_client.close.assert_called_once_with()
    _assert_value_free_credential_error(exc_info.value, old_marker, caplog.text)
    _assert_value_free_credential_error(exc_info.value, new_marker, caplog.text)


def test_provider_fenced_rechecks_deadline_after_on_start(monkeypatch, caplog):
    marker = 'PRECALL_TOKEN_SECRET'
    current = [100.0]
    client = _provider_client_with_token(marker)
    core = _provider_fenced_core_for_test(client, 101.0)
    monkeypatch.setattr(core, '_should_refresh', lambda: False)
    monkeypatch.setattr(kubernetes.time, 'monotonic', lambda: current[0])

    def on_start():
        current[0] = 101.0

    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    with pytest.raises(config_exception) as exc_info:
        core.call_with_provider_fence('list_node', lambda: None, on_start)

    assert core._client is None
    client.list_node.assert_not_called()
    assert client.api_client.configuration.api_key == {}
    client.api_client.close.assert_called_once_with()
    _assert_value_free_credential_error(exc_info.value, marker, caplog.text)


def test_provider_fenced_core_refresh_observes_new_stop_before_raw_call(
        monkeypatch):
    initial = SimpleNamespace(api_client=MagicMock(), list_node=MagicMock())
    replacement = SimpleNamespace(api_client=MagicMock(), list_node=MagicMock())
    stopped = False

    def build_isolated(*_args, **_kwargs):
        nonlocal stopped
        stopped = True
        return kubernetes._BoundedCoreApiResult(replacement, None, None, None)

    def provider_fence():
        if stopped:
            raise RuntimeError('worker stopped')

    monkeypatch.setattr(kubernetes, '_bounded_core_api',
                        lambda *_args, **_kwargs: (initial, None))
    monkeypatch.setattr(kubernetes, '_bounded_core_api_isolated',
                        build_isolated)
    core = kubernetes.ProviderFencedCoreApi('bounded-context',
                                            exec_credential_timeout_seconds=2,
                                            provider_fence=provider_fence)
    monkeypatch.setattr(core, '_should_refresh', lambda: True)

    with pytest.raises(RuntimeError, match='worker stopped'):
        core.call_with_provider_fence('list_node', provider_fence, None)

    initial.list_node.assert_not_called()
    replacement.list_node.assert_not_called()
    initial.api_client.close.assert_called_once_with()
    replacement.api_client.close.assert_called_once_with()


@pytest.mark.parametrize('method_fails', (False, True),
                         ids=('response', 'failure'))
def test_provider_fenced_core_control_error_drops_losing_method_state(
        monkeypatch, method_fails):
    marker = 'LOSING_KUBERNETES_METHOD_SECRET'
    method = MagicMock()
    if method_fails:
        method.side_effect = RuntimeError(marker)
    else:
        method.return_value = {'secret': marker}
    raw_client = _provider_client_with_token(marker, method=method)
    core = _provider_fenced_core_for_test(raw_client, None)
    monkeypatch.setattr(core, '_should_refresh', lambda: False)
    fence_count = 0

    def provider_fence():
        nonlocal fence_count
        fence_count += 1
        if fence_count == 3:
            raise RuntimeError('drain won')

    with pytest.raises(RuntimeError, match='drain won') as exc_info:
        core.call_with_provider_fence('list_node', provider_fence, None)

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    assert not _kubernetes_traceback_contains(error, marker)
    assert core._client is None
    assert raw_client.api_client.configuration.api_key == {}
    raw_client.api_client.close.assert_called_once_with()
    method.assert_called_once_with()


def _create_test_kubeconfig(num_contexts):
    """Create a temporary kubeconfig with multiple contexts."""
    clusters = '\n'.join(f'- cluster:\n'
                         f'    server: https://cluster-{i}.example.com\n'
                         f'  name: cluster-{i}' for i in range(num_contexts))

    contexts = '\n'.join(f'- context:\n'
                         f'    cluster: cluster-{i}\n'
                         f'    user: user-{i}\n'
                         f'  name: context-{i}' for i in range(num_contexts))

    users = '\n'.join(f'- name: user-{i}\n'
                      f'  user: {{}}' for i in range(num_contexts))

    kubeconfig = (f'apiVersion: v1\n'
                  f'kind: Config\n'
                  f'clusters:\n'
                  f'{clusters}\n'
                  f'contexts:\n'
                  f'{contexts}\n'
                  f'current-context: context-0\n'
                  f'users:\n'
                  f'{users}\n')
    fd, path = tempfile.mkstemp(suffix='.yaml')
    os.write(fd, kubeconfig.encode())
    os.close(fd)
    return path


@pytest.mark.parametrize(
    'api_func',
    [
        kubernetes.core_api,
        kubernetes.storage_api,
        kubernetes.auth_api,
        kubernetes.networking_api,
        kubernetes.custom_objects_api,
        kubernetes.apps_api,
        kubernetes.batch_api,
        kubernetes.custom_resources_api,
    ],
)
def test_concurrent_context_isolation(monkeypatch, api_func):
    """Verify concurrent API calls with different contexts get isolated clients.

    This is a regression test for a race condition where the old implementation
    would:
    1. Call _load_config(context) which modified global
       kubernetes.client.configuration
    2. Create an API client that used that global config

    If two threads interleaved:
    - Thread A: _load_config('context-a')
    - Thread B: _load_config('context-b')  # overwrites global config
    - Thread A: CoreV1Api()  # incorrectly uses context-b!

    The fix uses new_client_from_config() which returns an ApiClient with an
    isolated Configuration object, avoiding global state.
    """
    num_contexts = 10
    iterations = 5
    contexts = [f'context-{i}' for i in range(num_contexts)]
    expected_hosts = {
        f'context-{i}': f'https://cluster-{i}.example.com'
        for i in range(num_contexts)
    }

    config_file = _create_test_kubeconfig(num_contexts)
    try:
        monkeypatch.setattr(kubernetes, '_get_config_file', lambda: config_file)

        original_get_api_client = kubernetes._get_api_client  # pylint: disable=protected-access

        def slow_get_api_client(context=None):
            assert (context is not None)
            client = original_get_api_client(context)
            time.sleep(0.001)
            return client

        monkeypatch.setattr(kubernetes, '_get_api_client', slow_get_api_client)

        for iteration in range(iterations):
            annotations.clear_request_level_cache()

            def get_api_for_context(ctx):
                api = api_func(ctx)
                # pylint: disable=protected-access
                return (ctx, api._client.api_client.configuration.host)

            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=num_contexts) as executor:
                futures = [
                    executor.submit(get_api_for_context, ctx)
                    for ctx in contexts
                ]
                results = [f.result() for f in futures]

            for requested_ctx, actual_host in results:
                expected_host = expected_hosts[requested_ctx]
                assert actual_host == expected_host, (
                    f'Iteration {iteration}: Host mismatch for '
                    f'{requested_ctx}: expected {expected_host}, '
                    f'got {actual_host}. Race condition detected.')
    finally:
        os.unlink(config_file)
