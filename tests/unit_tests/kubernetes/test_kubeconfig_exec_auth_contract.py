"""Characterization for Kubernetes exec-auth kubeconfig handling."""

import hashlib
import inspect
from pathlib import Path
from typing import Any
from unittest import mock

from sky.provision.kubernetes import constants as kubernetes_constants
from sky.provision.kubernetes import utils as kubernetes_utils


def test_public_callable_contract() -> None:
    rewrite = kubernetes_utils.format_kubeconfig_exec_auth
    cached_rewrite = kubernetes_utils.format_kubeconfig_exec_auth_with_cache

    assert rewrite.__module__ == 'sky.provision.kubernetes.utils'
    assert rewrite.__qualname__ == 'format_kubeconfig_exec_auth'
    rewrite_signature = inspect.signature(rewrite)
    assert list(rewrite_signature.parameters) == [
        'config', 'output_path', 'inject_wrapper'
    ]
    assert rewrite_signature.parameters['config'].annotation is Any
    assert rewrite_signature.parameters['output_path'].annotation is str
    assert rewrite_signature.parameters['inject_wrapper'].annotation is bool
    assert rewrite_signature.parameters['inject_wrapper'].default is True
    assert rewrite_signature.return_annotation is bool
    assert cached_rewrite.__module__ == 'sky.provision.kubernetes.utils'
    assert cached_rewrite.__qualname__ == (
        'format_kubeconfig_exec_auth_with_cache')
    cache_signature = inspect.signature(cached_rewrite)
    assert list(cache_signature.parameters) == ['kubeconfig_path']
    assert cache_signature.parameters['kubeconfig_path'].annotation is str
    assert cache_signature.return_annotation is str


def test_rewrite_preserves_historical_transform_order(tmp_path: Path) -> None:
    config = {
        'users': [{
            'name': 'gke',
            'user': {
                'exec': {
                    'command': '/opt/google/gke-gcloud-auth-plugin'
                }
            },
        }, {
            'name': 'already-wrapped',
            'user': {
                'exec': {
                    'command': kubernetes_constants.SKY_K8S_EXEC_AUTH_WRAPPER,
                    'args': ['original-command'],
                }
            },
        }, {
            'name': 'nebius',
            'user': {
                'exec': {
                    'command': '/usr/local/bin/nebius',
                    'args': ['iam', '--profile', 'personal'],
                }
            },
        }, {
            'name': 'static-token',
            'user': {},
        }]
    }
    output_path = tmp_path / 'nested' / 'config.yaml'

    assert kubernetes_utils.format_kubeconfig_exec_auth(config,
                                                        str(output_path))

    gke_exec = config['users'][0]['user']['exec']
    assert gke_exec == {
        'command': kubernetes_constants.SKY_K8S_EXEC_AUTH_WRAPPER,
        'args': ['gke-gcloud-auth-plugin'],
    }
    assert config['users'][1]['user']['exec'] == {
        'command': kubernetes_constants.SKY_K8S_EXEC_AUTH_WRAPPER,
        'args': ['original-command'],
    }
    assert config['users'][2]['user']['exec'] == {
        'command': kubernetes_constants.SKY_K8S_EXEC_AUTH_WRAPPER,
        'args': ['nebius', 'iam', '--profile', 'sky'],
    }
    assert kubernetes_utils.yaml.safe_load(
        output_path.read_text(encoding='utf-8')) == config


def test_rewrite_without_wrapper_only_normalizes_executable(
        tmp_path: Path) -> None:
    config = {
        'users': [{
            'name': 'gke',
            'user': {
                'exec': {
                    'command': '/opt/google/gke-gcloud-auth-plugin',
                    'args': ['--use_application_default_credentials'],
                }
            },
        }]
    }
    output_path = tmp_path / 'config.yaml'

    assert kubernetes_utils.format_kubeconfig_exec_auth(config,
                                                        str(output_path),
                                                        inject_wrapper=False)
    assert config['users'][0]['user']['exec'] == {
        'command': 'gke-gcloud-auth-plugin',
        'args': ['--use_application_default_credentials'],
    }


def test_cache_miss_uses_facade_rewriter_and_content_hash(
        tmp_path: Path, monkeypatch) -> None:
    config = {'apiVersion': 'v1', 'users': []}
    source = tmp_path / 'source.yaml'
    source.write_text(kubernetes_utils.yaml.safe_dump(config), encoding='utf-8')
    cache_dir = tmp_path / 'cache'
    monkeypatch.setattr(
        kubernetes_constants,
        'SKY_K8S_EXEC_AUTH_KUBECONFIG_CACHE',
        str(cache_dir),
    )
    calls = []

    def _rewrite(value, output_path, inject_wrapper=True):
        calls.append((value, output_path, inject_wrapper))
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(kubernetes_utils.yaml.safe_dump(value),
                                     encoding='utf-8')
        return True

    monkeypatch.setattr(kubernetes_utils, 'format_kubeconfig_exec_auth',
                        _rewrite)

    result = kubernetes_utils.format_kubeconfig_exec_auth_with_cache(
        str(source))

    normalized = kubernetes_utils.yaml.dump(config, sort_keys=True)
    digest = hashlib.sha1(normalized.encode('utf-8'),
                          usedforsecurity=False).hexdigest()
    expected = cache_dir / f'{digest}.yaml'
    assert result == str(expected)
    assert calls == [(config, str(expected), True)]


def test_cache_hit_skips_rewrite(tmp_path: Path, monkeypatch) -> None:
    config = {'apiVersion': 'v1', 'users': []}
    source = tmp_path / 'source.yaml'
    source.write_text(kubernetes_utils.yaml.safe_dump(config), encoding='utf-8')
    cache_dir = tmp_path / 'cache'
    monkeypatch.setattr(
        kubernetes_constants,
        'SKY_K8S_EXEC_AUTH_KUBECONFIG_CACHE',
        str(cache_dir),
    )
    normalized = kubernetes_utils.yaml.dump(config, sort_keys=True)
    digest = hashlib.sha1(normalized.encode('utf-8'),
                          usedforsecurity=False).hexdigest()
    expected = cache_dir / f'{digest}.yaml'
    expected.parent.mkdir()
    expected.write_text('cached', encoding='utf-8')

    with mock.patch.object(kubernetes_utils,
                           'format_kubeconfig_exec_auth') as rewrite:
        result = kubernetes_utils.format_kubeconfig_exec_auth_with_cache(
            str(source))

    assert result == str(expected)
    rewrite.assert_not_called()


def test_cache_rewrite_failure_warns_and_returns_source(tmp_path: Path,
                                                        monkeypatch) -> None:
    source = tmp_path / 'source.yaml'
    source.write_text(kubernetes_utils.yaml.safe_dump({'users': []}),
                      encoding='utf-8')
    monkeypatch.setattr(
        kubernetes_constants,
        'SKY_K8S_EXEC_AUTH_KUBECONFIG_CACHE',
        str(tmp_path / 'cache'),
    )

    def _raise(*_args, **_kwargs):
        raise RuntimeError('rewrite failed')

    monkeypatch.setattr(kubernetes_utils, 'format_kubeconfig_exec_auth', _raise)
    with mock.patch.object(kubernetes_utils.logger, 'warning') as warning:
        result = kubernetes_utils.format_kubeconfig_exec_auth_with_cache(
            str(source))

    assert result == str(source)
    warning.assert_called_once()
    message = warning.call_args.args[0]
    assert f'Failed to format kubeconfig at {source}' in message
    assert 'Reason: RuntimeError: rewrite failed' in message


def test_facade_yaml_bindings_remain_late_bound(tmp_path: Path,
                                                monkeypatch) -> None:
    output = tmp_path / 'rewritten.yaml'
    config = {'users': []}
    safe_dump = mock.Mock()
    monkeypatch.setattr(kubernetes_utils.yaml, 'safe_dump', safe_dump)

    assert not kubernetes_utils.format_kubeconfig_exec_auth(config, str(output))
    safe_dump.assert_called_once_with(config, mock.ANY)

    source = tmp_path / 'source.yaml'
    source.write_text('ignored', encoding='utf-8')
    parsed = {'apiVersion': 'v1', 'users': []}
    monkeypatch.setattr(kubernetes_utils.yaml_utils, 'safe_load',
                        mock.Mock(return_value=parsed))
    monkeypatch.setattr(kubernetes_utils.yaml, 'dump',
                        mock.Mock(return_value='normalized'))
    monkeypatch.setattr(
        kubernetes_constants,
        'SKY_K8S_EXEC_AUTH_KUBECONFIG_CACHE',
        str(tmp_path / 'cache'),
    )
    monkeypatch.setattr(kubernetes_utils, 'format_kubeconfig_exec_auth',
                        mock.Mock(side_effect=RuntimeError('stop')))

    assert kubernetes_utils.format_kubeconfig_exec_auth_with_cache(
        str(source)) == str(source)
    kubernetes_utils.yaml_utils.safe_load.assert_called_once()
    kubernetes_utils.yaml.dump.assert_called_once_with(parsed, sort_keys=True)
