"""Tests for Kubernetes adaptor."""
# pylint: disable=protected-access

import concurrent.futures
import gc
import json
import os
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sky.adaptors import kubernetes
from sky.utils import annotations


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


def _write_exec_kubeconfig(tmp_path, script):
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
                    'command': sys.executable,
                    'args': ['-c', script],
                },
            },
        }],
    }),
                    encoding='utf-8')
    return path


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
    fences = []

    core, expires_at = kubernetes._bounded_core_api(  # pylint: disable=protected-access
        'bounded-context',
        exec_credential_timeout_seconds=2,
        provider_fence=lambda: fences.append('fence'))
    try:
        configuration = core.api_client.configuration
        assert configuration.host == 'https://bounded.example.test'
        assert configuration.api_key['authorization'] == (
            'Bearer bounded-token')
        assert configuration.refresh_api_key_hook is None
        assert expires_at == 4070908800.0
        assert fences == ['fence'] * 4
    finally:
        core.api_client.close()


def test_bounded_core_api_terminates_timed_out_exec_credential(
        monkeypatch, tmp_path):
    path = _write_exec_kubeconfig(tmp_path, 'import time; time.sleep(60)')
    monkeypatch.setattr(kubernetes, '_get_config_file', lambda: str(path))
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    started = time.monotonic()

    with pytest.raises(config_exception, match='bounded timeout'):
        kubernetes._bounded_core_api(  # pylint: disable=protected-access
            'bounded-context',
            exec_credential_timeout_seconds=0.05,
            provider_fence=lambda: None)

    assert time.monotonic() - started < 2


def test_provider_fenced_core_refresh_observes_new_stop_before_raw_call(
        monkeypatch):
    initial = SimpleNamespace(api_client=MagicMock(), list_node=MagicMock())
    replacement = SimpleNamespace(api_client=MagicMock(), list_node=MagicMock())
    stopped = False
    build_count = 0

    def build(*_args, **_kwargs):
        nonlocal build_count, stopped
        build_count += 1
        if build_count == 1:
            return initial, None
        stopped = True
        return replacement, None

    def provider_fence():
        if stopped:
            raise RuntimeError('worker stopped')

    monkeypatch.setattr(kubernetes, '_bounded_core_api', build)
    core = kubernetes.ProviderFencedCoreApi('bounded-context',
                                            exec_credential_timeout_seconds=2,
                                            provider_fence=provider_fence)
    monkeypatch.setattr(core, '_should_refresh', lambda: True)

    with pytest.raises(RuntimeError, match='worker stopped'):
        core.call_with_provider_fence('list_node', provider_fence, None)

    initial.list_node.assert_not_called()
    replacement.list_node.assert_not_called()
    replacement.api_client.close.assert_called_once_with()


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
