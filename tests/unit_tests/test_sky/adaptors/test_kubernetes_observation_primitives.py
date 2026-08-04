"""Security and ownership tests for Kubernetes observation primitives."""
# pylint: disable=protected-access

import base64
import gc
import json
import os
from pathlib import Path
import threading
import types
from typing import Any
from unittest.mock import MagicMock
import weakref

import pytest

from sky.adaptors import kubernetes


def _object_graph_contains_marker(root: Any, marker: str) -> bool:
    """Search values retained by adaptor traceback locals."""
    pending = [root]
    seen: set[int] = set()
    while pending:
        value = pending.pop()
        if id(value) in seen:
            continue
        seen.add(id(value))
        if isinstance(value, str):
            if marker in value:
                return True
            continue
        if isinstance(value, bytes):
            if marker.encode() in value:
                return True
            continue
        if value is None or isinstance(value, (bool, int, float, complex)):
            continue
        if isinstance(value, dict):
            pending.extend(value.keys())
            pending.extend(value.values())
            continue
        if isinstance(value, (list, tuple, set, frozenset)):
            pending.extend(value)
            continue
        if isinstance(value, BaseException):
            pending.extend(value.args)
            pending.extend((value.__cause__, value.__context__))
            pending.extend(vars(value).values())
            continue
        if isinstance(value, types.MethodType):
            pending.append(value.__self__)
            continue
        if isinstance(value, (types.ModuleType, types.FunctionType, type)):
            continue
        try:
            pending.extend(vars(value).values())
        except TypeError:
            pass
        slots = getattr(type(value), '__slots__', ())
        if isinstance(slots, str):
            slots = (slots,)
        for slot in slots:
            try:
                pending.append(getattr(value, slot))
            except AttributeError:
                pass
    return False


def _adaptor_traceback_contains(error: BaseException, marker: str) -> bool:
    """Walk only adaptor traceback frames, excluding the test caller."""
    adaptor_path = os.path.realpath(kubernetes.__file__)
    traceback = error.__traceback__
    while traceback is not None:
        frame = traceback.tb_frame
        if os.path.realpath(frame.f_code.co_filename) == adaptor_path:
            if any(
                    _object_graph_contains_marker(value, marker)
                    for value in frame.f_locals.values()):
                return True
        traceback = traceback.tb_next
    return False


def _assert_isolated_error(error: BaseException, marker: str) -> None:
    assert marker not in str(error)
    assert marker not in repr(error)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert not _adaptor_traceback_contains(error, marker)


def _write_kubeconfig(path,
                      *,
                      users,
                      current_context='second-context',
                      ca_path=None):
    clusters: list[dict[str, Any]] = [
        {
            'cluster': {
                'server': 'https://first.example.com',
            },
            'name': 'first-cluster',
        },
        {
            'cluster': {
                'server': 'https://second.example.com',
            },
            'name': 'second-cluster',
        },
    ]
    if ca_path is not None:
        clusters[1]['cluster']['certificate-authority'] = str(ca_path)
    path.write_text(json.dumps({
        'apiVersion': 'v1',
        'kind': 'Config',
        'clusters': clusters,
        'contexts': [
            {
                'context': {
                    'cluster': 'first-cluster',
                    'user': 'first-user',
                },
                'name': 'first-context',
            },
            {
                'context': {
                    'cluster': 'second-cluster',
                    'namespace': 'team-two',
                    'user': 'second-user',
                },
                'name': 'second-context',
            },
        ],
        'current-context': current_context,
        'users': users,
    }),
                    encoding='utf-8')


def _disable_in_cluster(monkeypatch):
    monkeypatch.setattr(kubernetes, '_is_in_cluster_config_available',
                        MagicMock(return_value=False))


@pytest.mark.parametrize(
    ('context_data', 'expected'),
    (
        ({
            'cluster': 'cluster',
            'user': 'user'
        }, 'cluster_user_default'),
        ({
            'cluster': 'cluster',
            'user': 'user',
            'namespace': 'default'
        }, 'cluster_user_default'),
        ({
            'cluster': 'cluster_with_under',
            'user': 'user__with_under',
            'namespace': 'namespace_with_under'
        }, 'cluster_with_under_user__with_under_namespace_with_under'),
        ({
            'cluster': 'cluster',
            'user': 'user',
            'namespace': ''
        }, 'cluster_user_'),
    ),
)
def test_context_identity_normalization_preserves_legacy_shape(
        context_data, expected):
    context = {'context': context_data}

    assert kubernetes.normalize_kubernetes_context_identity(context) == expected


def test_in_cluster_identity_delegates_to_shared_normalizer(monkeypatch):
    context_name = 'in_cluster_context_with_underscores'
    normalizer = MagicMock(return_value='normalized-in-cluster-identity')
    monkeypatch.setenv(kubernetes.IN_CLUSTER_CONTEXT_NAME_ENV_VAR, context_name)
    monkeypatch.setattr(kubernetes, 'normalize_kubernetes_in_cluster_identity',
                        normalizer)

    identity = kubernetes.in_cluster_identity()

    assert identity == ['normalized-in-cluster-identity']
    normalizer.assert_called_once_with(context_name)


def test_context_session_uses_one_frozen_load_and_exact_identity(
        monkeypatch, tmp_path):
    kubeconfig_path = tmp_path / 'config.json'
    users = [
        {
            'name': 'first-user',
            'user': {
                'token': 'first-token',
            },
        },
        {
            'name': 'second-user',
            'user': {
                'token': 'second-token',
            },
        },
    ]
    _write_kubeconfig(kubeconfig_path, users=users)
    config_file = MagicMock(return_value=str(kubeconfig_path))
    monkeypatch.setattr(kubernetes, '_get_config_file', config_file)
    _disable_in_cluster(monkeypatch)
    merger_type = kubernetes.kubernetes.config.kube_config.KubeConfigMerger
    merger_constructor = MagicMock(side_effect=merger_type)
    monkeypatch.setattr(kubernetes.kubernetes.config.kube_config,
                        'KubeConfigMerger', merger_constructor)

    session = kubernetes.load_kubernetes_contexts_uncached()
    assert session.inventory == kubernetes.KubernetesContextInventory(
        available_context_names=('first-context', 'second-context'),
        kubeconfig_current_context_name='second-context',
        in_cluster_available=False,
        in_cluster_context_name='in-cluster')
    assert 'first-token' not in repr(session)
    assert 'second-token' not in repr(session)

    _write_kubeconfig(
        kubeconfig_path,
        users=users,
        current_context='first-context',
    )
    changed = json.loads(kubeconfig_path.read_text(encoding='utf-8'))
    changed['clusters'][0]['cluster']['server'] = 'https://changed.example.com'
    changed['clusters'][1]['cluster']['server'] = 'https://changed.example.com'
    kubeconfig_path.write_text(json.dumps(changed), encoding='utf-8')

    identity_normalizer = MagicMock(
        wraps=kubernetes.normalize_kubernetes_context_identity)
    monkeypatch.setattr(kubernetes, 'normalize_kubernetes_context_identity',
                        identity_normalizer)
    first = session.new_api_client_target('first-context')
    second = session.new_api_client_target('second-context')
    try:
        assert first.api_client.configuration.host == (
            'https://first.example.com')
        assert second.api_client.configuration.host == (
            'https://second.example.com')
        assert first.context_identity == kubernetes.KubernetesContextIdentity(
            context_name='first-context',
            identity=('first-cluster_first-user_default',),
            in_cluster=False,
            namespace='default')
        assert second.context_identity == kubernetes.KubernetesContextIdentity(
            context_name='second-context',
            identity=('second-cluster_second-user_team-two',),
            in_cluster=False,
            namespace='team-two')
        config_file.assert_called_once_with()
        merger_constructor.assert_called_once_with(str(kubeconfig_path))
        assert [
            call.args[0]['name'] for call in identity_normalizer.call_args_list
        ] == ['first-context', 'second-context']
    finally:
        first.close()
        second.close()
        session.close()


def test_malformed_kubeconfig_is_an_isolated_empty_inventory(
        monkeypatch, tmp_path):
    marker = 'KUBECONFIG_PARSE_TRACE_SECRET'
    kubeconfig_path = tmp_path / 'malformed-config.yaml'
    kubeconfig_path.write_text(
        'apiVersion: v1\n'
        'users:\n'
        '- name: secret-user\n'
        '  user:\n'
        f'    token: {marker}\n'
        'contexts: [\n',
        encoding='utf-8')
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    log = MagicMock(side_effect=AssertionError('kubeconfig failure logged'))
    monkeypatch.setattr(kubernetes.logger, 'warning', log)
    monkeypatch.setattr(kubernetes.logger, 'error', log)

    session = kubernetes.load_kubernetes_contexts_uncached()

    assert session.inventory == kubernetes.KubernetesContextInventory(
        available_context_names=(),
        kubeconfig_current_context_name=None,
        in_cluster_available=False,
        in_cluster_context_name='in-cluster')
    assert session._kubeconfig_snapshot is None
    assert marker not in repr(session)
    log.assert_not_called()
    session.close()


def test_kubeconfig_capture_detaches_control_exception_and_preserves_type(
        monkeypatch, tmp_path):
    marker = 'KUBECONFIG_BASEEXCEPTION_TRACE_SECRET'
    kubeconfig_path = tmp_path / 'config.json'
    _write_kubeconfig(
        kubeconfig_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': {
                    'token': marker,
                },
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    interrupt = KeyboardInterrupt('fixed capture interruption')
    loader = MagicMock(side_effect=interrupt)
    monkeypatch.setattr(kubernetes.kubernetes.config.kube_config,
                        'KubeConfigLoader', loader)

    with pytest.raises(KeyboardInterrupt) as exc_info:
        kubernetes.load_kubernetes_contexts_uncached()

    assert exc_info.value is interrupt
    _assert_isolated_error(exc_info.value, marker)
    assert not _object_graph_contains_marker(exc_info.value, marker)
    loader.assert_called_once()


def test_kubeconfig_token_file_rotates_live_and_closes_cleanly(
        monkeypatch, tmp_path):
    kubeconfig_path = tmp_path / 'config.json'
    token_path = tmp_path / 'rotating-token'
    token_path.write_text('first-token', encoding='utf-8')
    _write_kubeconfig(
        kubeconfig_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': {
                    'tokenFile': token_path.name,
                },
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()
    target = session.new_api_client_target('second-context')
    configuration = target.api_client.configuration
    refresh_hook = configuration.refresh_api_key_hook

    assert configuration.get_api_key_with_prefix(
        'authorization') == 'Bearer first-token'
    token_path.write_text('second-token', encoding='utf-8')
    assert configuration.get_api_key_with_prefix(
        'authorization') == 'Bearer second-token'

    target.close()

    assert configuration.api_key == {}
    assert configuration.refresh_api_key_hook is None
    assert configuration.get_api_key_with_prefix('authorization') is None
    assert repr(refresh_hook).endswith('(closed=True)')
    session.close()


def test_kubeconfig_token_file_rotation_failure_drops_last_token(
        monkeypatch, tmp_path):
    marker = 'ROTATED_TOKEN_PATH_SECRET'
    kubeconfig_path = tmp_path / 'config.json'
    token_path = tmp_path / marker
    token_path.write_text('last-valid-token', encoding='utf-8')
    _write_kubeconfig(
        kubeconfig_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': {
                    'tokenFile': token_path.name,
                },
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()
    target = session.new_api_client_target('second-context')
    configuration = target.api_client.configuration
    refresh_hook = configuration.refresh_api_key_hook
    assert configuration.get_api_key_with_prefix(
        'authorization') == 'Bearer last-valid-token'
    token_path.unlink()
    token_path.mkdir()

    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    with pytest.raises(config_exception) as exc_info:
        configuration.get_api_key_with_prefix('authorization')

    _assert_isolated_error(exc_info.value, marker)
    assert configuration.api_key.get('authorization') is None
    assert configuration.refresh_api_key_hook is None
    assert repr(refresh_hook).endswith('(closed=True)')
    target.close()
    session.close()


@pytest.mark.parametrize('invalid_kind', ('oversized', 'directory'))
def test_kubeconfig_token_file_initial_read_is_bounded_and_regular(
        monkeypatch, tmp_path, invalid_kind):
    marker = 'INVALID_TOKEN_FILE_SECRET'
    kubeconfig_path = tmp_path / 'config.json'
    token_path = tmp_path / 'token-file'
    if invalid_kind == 'oversized':
        token_path.write_bytes(
            (marker.encode() *
             (kubernetes._MAX_KUBECONFIG_TOKEN_BYTES // len(marker) + 2)))
    else:
        token_path.mkdir()
    _write_kubeconfig(
        kubeconfig_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': {
                    'tokenFile': token_path.name,
                },
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)

    with pytest.raises(config_exception) as exc_info:
        session.new_api_client_target('second-context')

    assert str(exc_info.value) == (
        'Kubernetes observation client target could not be created safely.')
    _assert_isolated_error(exc_info.value, marker)
    session.close()


def test_inline_kubeconfig_token_keeps_precedence_over_invalid_token_file(
        monkeypatch, tmp_path):
    kubeconfig_path = tmp_path / 'config.json'
    token_path = tmp_path / 'ignored-token-directory'
    token_path.mkdir()
    _write_kubeconfig(
        kubeconfig_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': {
                    'token': 'inline-token',
                    'tokenFile': token_path.name,
                },
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()

    target = session.new_api_client_target('second-context')

    assert target.api_client.configuration.get_api_key_with_prefix(
        'authorization') == 'Bearer inline-token'
    target.close()
    session.close()


def test_bounded_token_reader_rejects_symlink_and_fifo_without_blocking(
        tmp_path):
    token_path = tmp_path / 'real-token'
    token_path.write_text('credential-secret', encoding='utf-8')
    symlink_path = tmp_path / 'token-symlink'
    symlink_path.symlink_to(token_path)

    symlink_result = kubernetes._read_bounded_kubeconfig_token_file(
        str(symlink_path))

    assert symlink_result.token is None
    assert symlink_result.failure_message == (
        'Kubernetes token file must not be a symbolic link.')

    if not hasattr(os, 'mkfifo'):
        return
    fifo_path = tmp_path / 'token-fifo'
    os.mkfifo(fifo_path)
    results = []
    reader = threading.Thread(target=lambda: results.append(
        kubernetes._read_bounded_kubeconfig_token_file(str(fifo_path))),
                              daemon=True)
    reader.start()
    reader.join(timeout=1)

    assert not reader.is_alive()
    assert len(results) == 1
    assert results[0].token is None
    assert results[0].failure_message == (
        'Kubernetes token file must be a regular file.')


@pytest.mark.parametrize(
    'unsafe_user',
    (
        {
            'exec': {
                'apiVersion': 'client.authentication.k8s.io/v1',
                'command': '/secret/credential-command',
                'env': [{
                    'name': 'SECRET_ENV',
                    'value': 'credential-secret',
                }],
            },
        },
        {
            'auth-provider': {
                'name': 'gcp',
                'config': {
                    'cmd-path': '/secret/auth-provider-command',
                    'access-token': 'credential-secret',
                },
            },
        },
    ),
)
def test_unsafe_credential_modes_fail_before_upstream_load(
        monkeypatch, tmp_path, unsafe_user):
    marker = 'credential-secret'
    kubeconfig_path = tmp_path / 'config.json'
    _write_kubeconfig(
        kubeconfig_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': unsafe_user,
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()

    loader = MagicMock(side_effect=AssertionError('upstream loader ran'))
    process = MagicMock(side_effect=AssertionError('credential process ran'))
    client = MagicMock(side_effect=AssertionError('client was constructed'))
    log = MagicMock(side_effect=AssertionError('unsafe configuration logged'))
    monkeypatch.setattr(kubernetes.kubernetes.config.kube_config,
                        'KubeConfigLoader', loader)
    monkeypatch.setattr(kubernetes.subprocess, 'Popen', process)
    monkeypatch.setattr(kubernetes.kubernetes.client, 'ApiClient', client)
    monkeypatch.setattr(kubernetes.logger, 'warning', log)
    monkeypatch.setattr(kubernetes.logger, 'error', log)

    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    with pytest.raises(config_exception) as exc_info:
        session.new_api_client_target('second-context')

    message = str(exc_info.value)
    assert message == (
        'Kubernetes observation does not support kubeconfig exec or '
        'auth-provider credentials.')
    _assert_isolated_error(exc_info.value, marker)
    assert '/secret/' not in message
    assert marker not in repr(session)
    loader.assert_not_called()
    process.assert_not_called()
    client.assert_not_called()
    log.assert_not_called()
    session.close()


@pytest.mark.parametrize(
    'failure_stage', ('loader', 'configuration', 'load-and-set', 'api-client'))
def test_target_build_failures_do_not_retain_credentials_or_owned_files(
        monkeypatch, tmp_path, failure_stage):
    marker = 'TARGET_BUILD_EXCEPTION_SECRET'
    kubeconfig_path = tmp_path / 'config.json'
    ca_path = tmp_path / 'cluster-ca.pem'
    cert_path = tmp_path / 'client.crt'
    key_path = tmp_path / f'{marker}.key'
    ca_path.write_text('captured-ca', encoding='utf-8')
    cert_path.write_text('client-certificate', encoding='utf-8')
    key_path.write_text('client-key', encoding='utf-8')
    _write_kubeconfig(
        kubeconfig_path,
        ca_path=ca_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': {
                    'token': marker,
                    'client-certificate': cert_path.name,
                    'client-key': key_path.name,
                },
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()

    owned_directories: list[Path] = []

    def tracked_mkdtemp(*, prefix):
        assert prefix == 'skypilot-k8s-target-'
        owned_directory = tmp_path / f'owned-{len(owned_directories)}'
        os.mkdir(owned_directory, mode=0o700)
        owned_directories.append(owned_directory)
        return str(owned_directory)

    monkeypatch.setattr(kubernetes.tempfile, 'mkdtemp', tracked_mkdtemp)
    captured_configurations = []
    loader_type = kubernetes.kubernetes.config.kube_config.KubeConfigLoader

    if failure_stage == 'loader':
        monkeypatch.setattr(
            kubernetes.kubernetes.config.kube_config,
            'KubeConfigLoader',
            MagicMock(side_effect=RuntimeError(marker)),
        )
    elif failure_stage == 'configuration':
        monkeypatch.setattr(
            kubernetes.kubernetes.client,
            'Configuration',
            MagicMock(side_effect=RuntimeError(marker)),
        )
    elif failure_stage == 'load-and-set':

        def fail_load_and_set(_loader, configuration):
            captured_configurations.append(configuration)
            configuration.api_key['authorization'] = marker
            configuration.api_key_prefix['authorization'] = marker
            configuration.cert_file = marker
            configuration.key_file = marker
            configuration.refresh_api_key_hook = lambda: marker
            raise RuntimeError(marker)

        monkeypatch.setattr(loader_type, 'load_and_set', fail_load_and_set)
    else:

        def fail_api_client(*, configuration):
            captured_configurations.append(configuration)
            raise RuntimeError(marker)

        monkeypatch.setattr(kubernetes.kubernetes.client, 'ApiClient',
                            fail_api_client)

    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    with pytest.raises(config_exception) as exc_info:
        session.new_api_client_target('second-context')

    assert str(exc_info.value) == (
        'Kubernetes observation client target could not be created safely.')
    _assert_isolated_error(exc_info.value, marker)
    assert all(
        not owned_directory.exists() for owned_directory in owned_directories)
    assert ca_path.exists()
    assert cert_path.exists()
    assert key_path.exists()
    for configuration in captured_configurations:
        assert configuration.api_key == {}
        assert configuration.api_key_prefix == {}
        assert configuration.cert_file is None
        assert configuration.key_file is None
        assert configuration.refresh_api_key_hook is None
    session.close()


def test_target_close_scrubs_client_and_removes_ca_while_retained(
        monkeypatch, tmp_path):
    kubeconfig_path = tmp_path / 'config.json'
    ca_path = tmp_path / 'cluster-ca.pem'
    ca_path.write_text('captured-ca', encoding='utf-8')
    _write_kubeconfig(
        kubeconfig_path,
        ca_path=ca_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': {
                    'token': 'credential-secret',
                },
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()
    target = session.new_api_client_target('second-context')
    raw_client = target.api_client
    frozen_ca_path = raw_client.configuration.ssl_ca_cert
    close = MagicMock(wraps=raw_client.close)
    raw_client.close = close
    assert frozen_ca_path != str(ca_path)
    assert os.path.exists(frozen_ca_path)
    assert raw_client.configuration.api_key
    assert raw_client.configuration.get_api_key_with_prefix(
        'authorization') == 'Bearer credential-secret'

    target.close()
    target.close()

    assert target.closed
    assert target._api_client is None
    with pytest.raises(RuntimeError, match='target is closed'):
        _ = target.api_client
    assert close.call_count == 1
    assert not os.path.exists(frozen_ca_path)
    assert raw_client.configuration.api_key == {}
    assert raw_client.configuration.api_key_prefix == {}
    assert raw_client.configuration.refresh_api_key_hook is None
    assert raw_client.configuration.get_api_key_with_prefix(
        'authorization') is None
    assert raw_client.configuration.cert_file is None
    assert raw_client.configuration.key_file is None
    session.close()


def test_target_concurrent_close_waits_for_reentrant_cleanup():
    marker = 'CONCURRENT_CLOSE_CREDENTIAL_SECRET'
    cleanup_started = threading.Event()
    cleanup_reentered = threading.Event()
    release_cleanup = threading.Event()

    class BlockingRefreshHook:
        """Blocks cleanup after exercising same-thread close reentrancy."""

        def __init__(self):
            self.target = None

        def close(self):
            cleanup_started.set()
            assert self.target is not None
            self.target.close()
            cleanup_reentered.set()
            assert release_cleanup.wait(timeout=5)

    refresh_hook = BlockingRefreshHook()
    configuration = kubernetes.kubernetes.client.Configuration()
    configuration.api_key['authorization'] = marker
    configuration.refresh_api_key_hook = refresh_hook
    raw_client = kubernetes.kubernetes.client.ApiClient(
        configuration=configuration)
    target = kubernetes.KubernetesApiClientTarget(
        api_client=raw_client,
        context_identity=kubernetes.KubernetesContextIdentity(
            context_name='concurrent-context',
            identity=('cluster_user_default',),
            in_cluster=False,
            namespace='default',
        ),
        owned_files=None,
    )
    refresh_hook.target = target
    first_done = threading.Event()
    second_started = threading.Event()
    second_done = threading.Event()

    def first_close():
        target.close()
        first_done.set()

    def second_close():
        second_started.set()
        target.close()
        second_done.set()

    first = threading.Thread(target=first_close, daemon=True)
    second = threading.Thread(target=second_close, daemon=True)
    first.start()
    assert cleanup_started.wait(timeout=1)
    assert cleanup_reentered.wait(timeout=1)
    second.start()
    assert second_started.wait(timeout=1)
    try:
        assert not second_done.wait(timeout=0.1)
        assert not target.closed
        assert configuration.api_key['authorization'] == marker
        assert configuration.refresh_api_key_hook is refresh_hook
    finally:
        release_cleanup.set()
    first.join(timeout=1)
    second.join(timeout=1)

    assert not first.is_alive()
    assert not second.is_alive()
    assert first_done.is_set()
    assert second_done.is_set()
    assert target.closed
    assert configuration.api_key == {}
    assert configuration.refresh_api_key_hook is None


def test_target_close_continues_after_refresh_and_client_close_failures():
    marker = 'THROWING_REFRESH_CLEANUP_SECRET'
    refresh_hook = MagicMock()
    refresh_hook.close.side_effect = RuntimeError('refresh cleanup failed')
    configuration = kubernetes.kubernetes.client.Configuration()
    configuration.api_key['authorization'] = marker
    configuration.cert_file = f'/tmp/{marker}-certificate'
    configuration.refresh_api_key_hook = refresh_hook
    raw_client = kubernetes.kubernetes.client.ApiClient(
        configuration=configuration)
    raw_client.default_headers['Authorization'] = marker
    raw_client.cookie = marker
    raw_client.close = MagicMock(
        side_effect=RuntimeError('client close failed'))
    owned_files = MagicMock()
    target = kubernetes.KubernetesApiClientTarget(
        api_client=raw_client,
        context_identity=kubernetes.KubernetesContextIdentity(
            context_name='throwing-refresh-context',
            identity=('cluster_user_default',),
            in_cluster=False,
            namespace='default',
        ),
        owned_files=owned_files,
    )

    target.close()
    target.close()

    assert target.closed
    assert configuration.api_key == {}
    assert configuration.cert_file is None
    assert configuration.refresh_api_key_hook is None
    assert raw_client.default_headers == {}
    assert raw_client.cookie is None
    assert not _object_graph_contains_marker(raw_client, marker)
    refresh_hook.close.assert_called_once_with()
    raw_client.close.assert_called_once_with()
    owned_files.close.assert_called_once_with()


def test_target_close_scrubs_retained_transport_tls_and_proxy_credentials():
    marker = 'RETAINED_TRANSPORT_SECRET'
    configuration = kubernetes.kubernetes.client.Configuration()
    configuration.host = f'https://user:{marker}@cluster.example.com'
    configuration.cert_file = f'/tmp/{marker}-cert'
    configuration.key_file = f'/tmp/{marker}-key'
    configuration.key_password = marker
    configuration.proxy = f'http://user:{marker}@proxy.example.com'
    configuration.proxy_headers = {'Proxy-Authorization': marker}
    raw_client = kubernetes.kubernetes.client.ApiClient(
        configuration=configuration)
    raw_client.default_headers['X-Credential'] = marker
    raw_client.cookie = marker
    pool_manager = raw_client.rest_client.pool_manager
    assert _object_graph_contains_marker(raw_client, marker)
    target = kubernetes.KubernetesApiClientTarget(
        api_client=raw_client,
        context_identity=kubernetes.KubernetesContextIdentity(
            context_name='retained-context',
            identity=('cluster_user_default',),
            in_cluster=False,
            namespace='default',
        ),
        owned_files=None,
    )

    target.close()

    assert not _object_graph_contains_marker(raw_client, marker)
    assert not _object_graph_contains_marker(pool_manager, marker)
    assert raw_client.rest_client.pool_manager is None
    assert pool_manager.connection_pool_kw == {}
    assert pool_manager.proxy is None
    assert pool_manager.proxy_headers == {}


def test_target_close_continues_after_pool_manager_clear_failure():
    marker = 'THROWING_TRANSPORT_CLEANUP_SECRET'

    class FailingPoolManager:
        """Retained pool manager whose primary cleanup step fails."""

        def __init__(self):
            self.clear_calls = 0
            self.connection_pool_kw = {'cert_file': f'/tmp/{marker}-cert'}
            self.headers = {'Authorization': marker}
            self.proxy_headers = {'Proxy-Authorization': marker}
            self.proxy = f'http://user:{marker}@proxy.example.com'
            self.proxy_config = marker
            self.proxy_ssl_context = marker

        def clear(self):
            self.clear_calls += 1
            raise RuntimeError('pool manager cleanup failed')

    class RetainedClient:
        """Minimal API client retaining every transport credential surface."""

        def __init__(self, pool_manager):
            self.configuration = kubernetes.kubernetes.client.Configuration()
            self.configuration.api_key['authorization'] = marker
            self.default_headers = {'X-Credential': marker}
            self.cookie = marker
            self.rest_client = types.SimpleNamespace(pool_manager=pool_manager)
            self.close_calls = 0

        def close(self):
            self.close_calls += 1

    pool_manager = FailingPoolManager()
    raw_client = RetainedClient(pool_manager)
    target = kubernetes.KubernetesApiClientTarget(
        api_client=raw_client,
        context_identity=kubernetes.KubernetesContextIdentity(
            context_name='throwing-transport-context',
            identity=('cluster_user_default',),
            in_cluster=False,
            namespace='default',
        ),
        owned_files=None,
    )
    assert _object_graph_contains_marker(raw_client, marker)

    target.close()

    assert target.closed
    assert pool_manager.clear_calls == 1
    assert not pool_manager.connection_pool_kw
    assert not pool_manager.headers
    assert not pool_manager.proxy_headers
    assert pool_manager.proxy is None
    assert pool_manager.proxy_config is None
    assert pool_manager.proxy_ssl_context is None
    assert raw_client.rest_client.pool_manager is None
    assert raw_client.close_calls == 1
    assert not _object_graph_contains_marker(raw_client, marker)
    assert not _object_graph_contains_marker(pool_manager, marker)


def test_outer_client_scrub_continues_after_configuration_scrub_failure(
        monkeypatch):
    marker = 'THROWING_CONFIGURATION_PHASE_SECRET'
    pool_manager = types.SimpleNamespace(
        clear=MagicMock(),
        connection_pool_kw={},
        headers={},
        proxy_headers={},
        proxy=None,
        proxy_config=None,
        proxy_ssl_context=None,
    )
    raw_client = types.SimpleNamespace(
        configuration=object(),
        default_headers={'Authorization': marker},
        cookie=marker,
        rest_client=types.SimpleNamespace(pool_manager=pool_manager),
    )
    configuration_scrub = MagicMock(
        side_effect=RuntimeError('configuration scrub failed'))
    monkeypatch.setattr(kubernetes,
                        '_scrub_kubernetes_configuration_credentials',
                        configuration_scrub)

    kubernetes._scrub_bounded_api_client_credentials(raw_client)

    configuration_scrub.assert_called_once_with(raw_client.configuration)
    assert not raw_client.default_headers
    assert raw_client.cookie is None
    assert raw_client.rest_client.pool_manager is None
    pool_manager.clear.assert_called_once_with()


def test_target_close_removes_minimum_client_instance_refresh_override():
    marker = 'MINIMUM_CLIENT_ROTATING_TOKEN_SECRET'
    configuration = kubernetes.kubernetes.client.Configuration()

    class LegacyInClusterLoader:

        def __init__(self):
            self.token = f'bearer {marker}'
            self.token_filename = f'/secret/{marker}'

    loader = LegacyInClusterLoader()
    loader_ref = weakref.ref(loader)

    def legacy_get_api_key_with_prefix(*_args):
        # kubernetes 20 and 24 install the equivalent instance closure.
        return loader.token

    configuration.api_key['authorization'] = loader.token
    configuration.get_api_key_with_prefix = legacy_get_api_key_with_prefix
    raw_client = kubernetes.kubernetes.client.ApiClient(
        configuration=configuration)
    target = kubernetes.KubernetesApiClientTarget(
        api_client=raw_client,
        context_identity=kubernetes.KubernetesContextIdentity(
            context_name='in-cluster',
            identity=('in-cluster-identity',),
            in_cluster=True,
            namespace='default',
        ),
        owned_files=None,
    )
    assert marker in configuration.get_api_key_with_prefix('authorization')

    target.close()
    del legacy_get_api_key_with_prefix, loader
    gc.collect()

    assert 'get_api_key_with_prefix' not in vars(configuration)
    assert configuration.get_api_key_with_prefix('authorization') is None
    assert loader_ref() is None
    assert not _object_graph_contains_marker(raw_client, marker)


def test_target_finalizer_is_cleanup_fallback(monkeypatch, tmp_path):
    kubeconfig_path = tmp_path / 'config.json'
    ca_path = tmp_path / 'cluster-ca.pem'
    ca_path.write_text('captured-ca', encoding='utf-8')
    _write_kubeconfig(
        kubeconfig_path,
        ca_path=ca_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': {
                    'token': 'credential-secret',
                },
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()
    target = session.new_api_client_target('second-context')
    raw_client = target.api_client
    frozen_ca_path = raw_client.configuration.ssl_ca_cert
    close = MagicMock(wraps=raw_client.close)
    raw_client.close = close
    target_ref = weakref.ref(target)

    del target
    gc.collect()

    assert target_ref() is None
    assert close.call_count == 1
    assert not os.path.exists(frozen_ca_path)
    assert raw_client.configuration.api_key == {}
    assert raw_client.configuration.refresh_api_key_hook is None
    assert raw_client.configuration.get_api_key_with_prefix(
        'authorization') is None
    session.close()


def test_target_close_removes_embedded_client_certificate_and_key(
        monkeypatch, tmp_path):
    kubeconfig_path = tmp_path / 'config.json'
    users = [
        {
            'name': 'first-user',
            'user': {},
        },
        {
            'name': 'second-user',
            'user': {
                'client-certificate-data': base64.standard_b64encode(
                    b'embedded-client-certificate').decode(),
                'client-key-data':
                    base64.standard_b64encode(b'embedded-client-key').decode(),
            },
        },
    ]
    config = tmp_path / 'base.json'
    _write_kubeconfig(config, users=users)
    raw_config = json.loads(config.read_text(encoding='utf-8'))
    raw_config['clusters'][1]['cluster'][
        'certificate-authority-data'] = base64.standard_b64encode(
            b'embedded-ca').decode()
    kubeconfig_path.write_text(json.dumps(raw_config), encoding='utf-8')
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()

    target = session.new_api_client_target('second-context')
    configuration = target.api_client.configuration
    owned_paths = (
        configuration.ssl_ca_cert,
        configuration.cert_file,
        configuration.key_file,
    )
    owned_directory = os.path.dirname(owned_paths[0])
    assert all(
        path is not None and os.path.exists(path) for path in owned_paths)
    assert all(os.path.dirname(path) == owned_directory for path in owned_paths)

    target.close()

    assert all(not os.path.exists(path) for path in owned_paths)
    assert not os.path.exists(owned_directory)
    assert configuration.ssl_ca_cert == owned_paths[0]
    assert configuration.cert_file is None
    assert configuration.key_file is None
    session.close()


def test_external_client_certificate_and_key_rotation_paths_remain_live(
        monkeypatch, tmp_path):
    kubeconfig_path = tmp_path / 'config.json'
    cert_path = tmp_path / 'client.crt'
    key_path = tmp_path / 'client.key'
    cert_path.write_text('initial-certificate', encoding='utf-8')
    key_path.write_text('initial-key', encoding='utf-8')
    _write_kubeconfig(
        kubeconfig_path,
        users=[
            {
                'name': 'first-user',
                'user': {},
            },
            {
                'name': 'second-user',
                'user': {
                    'client-certificate': cert_path.name,
                    'client-key': key_path.name,
                },
            },
        ],
    )
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(kubeconfig_path))
    _disable_in_cluster(monkeypatch)
    session = kubernetes.load_kubernetes_contexts_uncached()

    target = session.new_api_client_target('second-context')
    configuration = target.api_client.configuration
    assert configuration.cert_file == str(cert_path)
    assert configuration.key_file == str(key_path)

    cert_path.write_text('rotated-certificate', encoding='utf-8')
    key_path.write_text('rotated-key', encoding='utf-8')
    assert cert_path.read_text(encoding='utf-8') == 'rotated-certificate'
    assert key_path.read_text(encoding='utf-8') == 'rotated-key'

    target.close()

    assert cert_path.exists()
    assert key_path.exists()
    assert configuration.cert_file is None
    assert configuration.key_file is None
    session.close()


def test_incluster_target_owns_ca_cleanup_not_session(monkeypatch, tmp_path):
    token_path = tmp_path / 'token'
    ca_path = tmp_path / 'ca.crt'
    token_path.write_text('projected-token', encoding='utf-8')
    ca_path.write_text('captured-ca', encoding='utf-8')
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(tmp_path / 'missing-kubeconfig'))
    monkeypatch.setattr(kubernetes, '_is_in_cluster_config_available',
                        MagicMock(return_value=True))
    monkeypatch.setattr(kubernetes, 'IN_CLUSTER_TOKEN_PATH', str(token_path))
    monkeypatch.setattr(kubernetes, 'IN_CLUSTER_CA_PATH', str(ca_path))
    monkeypatch.setenv(kubernetes.IN_CLUSTER_CONTEXT_NAME_ENV_VAR,
                       'captured-in-cluster')
    monkeypatch.setenv(kubernetes.IN_CLUSTER_NAMESPACE_ENV_VAR,
                       'captured-namespace')
    monkeypatch.setenv('KUBERNETES_SERVICE_HOST', 'captured.example.com')
    monkeypatch.setenv('KUBERNETES_SERVICE_PORT', '7443')
    identity_normalizer = MagicMock(
        wraps=kubernetes.normalize_kubernetes_in_cluster_identity)
    monkeypatch.setattr(kubernetes, 'normalize_kubernetes_in_cluster_identity',
                        identity_normalizer)

    session = kubernetes.load_kubernetes_contexts_uncached()
    target = session.new_api_client_target('captured-in-cluster')
    configuration = target.api_client.configuration
    frozen_ca_path = configuration.ssl_ca_cert
    assert configuration.host == 'https://captured.example.com:7443'
    assert configuration.api_key['authorization'] == 'bearer projected-token'
    assert os.path.exists(frozen_ca_path)
    assert target.context_identity.identity == (
        'skypilot-in-cluster-identity-captured-in-cluster',)
    identity_normalizer.assert_called_once_with('captured-in-cluster')

    target.close()

    assert not session.closed
    assert not os.path.exists(frozen_ca_path)
    assert configuration.api_key == {}
    assert configuration.refresh_api_key_hook is None
    assert configuration.get_api_key_with_prefix('authorization') is None
    session.close()


def test_incluster_target_never_invents_missing_endpoint(monkeypatch, tmp_path):
    token_path = tmp_path / 'token'
    ca_path = tmp_path / 'ca.crt'
    token_path.write_text('projected-token', encoding='utf-8')
    ca_path.write_text('captured-ca', encoding='utf-8')
    monkeypatch.setattr(kubernetes, '_get_config_file',
                        lambda: str(tmp_path / 'missing-kubeconfig'))
    monkeypatch.setattr(kubernetes, '_is_in_cluster_config_available',
                        MagicMock(return_value=True))
    monkeypatch.setattr(kubernetes, 'IN_CLUSTER_TOKEN_PATH', str(token_path))
    monkeypatch.setattr(kubernetes, 'IN_CLUSTER_CA_PATH', str(ca_path))
    monkeypatch.delenv('KUBERNETES_SERVICE_HOST', raising=False)
    monkeypatch.delenv('KUBERNETES_SERVICE_PORT', raising=False)

    session = kubernetes.load_kubernetes_contexts_uncached()
    config_exception = (
        kubernetes.kubernetes.config.config_exception.ConfigException)
    with pytest.raises(config_exception) as exc_info:
        session.new_api_client_target(session.inventory.in_cluster_context_name)

    assert str(exc_info.value) == (
        'In-cluster Kubernetes endpoint was unavailable when contexts were '
        'captured.')
    assert 'KUBERNETES_SERVICE_HOST' not in os.environ
    assert 'KUBERNETES_SERVICE_PORT' not in os.environ
    session.close()
