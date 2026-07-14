"""Characterization tests for Kubernetes SSH transport helpers."""

import os
from pathlib import Path
import stat
import subprocess
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.provision.kubernetes import utils as kubernetes_utils


def test_construct_ssh_jump_command_without_proxy() -> None:
    command = kubernetes_utils.construct_ssh_jump_command('/tmp/key',
                                                          '127.0.0.1',
                                                          ssh_jump_port=2222)

    assert command == ('ssh -tt -i /tmp/key -o StrictHostKeyChecking=no '
                       '-o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes '
                       r'-W \[%h\]:%p sky@127.0.0.1 -p 2222 ')


def test_construct_ssh_jump_command_embeds_proxy_context(
        tmp_path: Path) -> None:
    proxy_path = tmp_path / 'proxy command.sh'
    proxy_path.write_text('#!/bin/sh\n', encoding='utf-8')
    proxy_path.chmod(0o600)

    command = kubernetes_utils.construct_ssh_jump_command(
        '/tmp/key',
        '127.0.0.1',
        ssh_jump_user='alice',
        proxy_cmd_path=str(proxy_path),
        proxy_cmd_target_pod='head-pod',
        current_kube_context='dev-context',
        current_kube_namespace='workspace',
        host_network=True)

    assert command == (
        'ssh -tt -i /tmp/key -o StrictHostKeyChecking=no '
        '-o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes '
        rf"-W \[%h\]:%p alice@127.0.0.1 -o ProxyCommand='{proxy_path} "
        "-c dev-context -n workspace -N head-pod'")
    assert proxy_path.stat().st_mode & stat.S_IXUSR


def test_create_proxy_command_script_copies_versioned_template(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('HOME', str(tmp_path))

    returned_path = kubernetes_utils.create_proxy_command_script()

    assert returned_path == kubernetes_utils.PORT_FORWARD_PROXY_CMD_PATH
    copied_path = Path(os.path.expanduser(returned_path))
    template_path = (Path(kubernetes_utils.__file__).parents[2] / 'templates' /
                     kubernetes_utils.PORT_FORWARD_PROXY_CMD_TEMPLATE)
    assert copied_path.read_bytes() == template_path.read_bytes()
    assert stat.S_IMODE(copied_path.stat().st_mode) == 0o700


def test_get_ssh_proxy_command_preserves_host_network_flag(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('HOME', str(tmp_path))

    command = kubernetes_utils.get_ssh_proxy_command('cluster-head',
                                                     '/tmp/key',
                                                     context='dev-context',
                                                     namespace='workspace',
                                                     host_network=True)

    assert '127.0.0.1' in command
    assert '-c dev-context -n workspace -N cluster-head' in command
    assert os.path.expanduser(
        kubernetes_utils.PORT_FORWARD_PROXY_CMD_PATH) in command


def test_port_forward_dependency_check_success() -> None:
    completed = [
        SimpleNamespace(returncode=0, stderr=b''),
        SimpleNamespace(returncode=0, stderr=b''),
    ]
    with mock.patch('subprocess.run', side_effect=completed) as run:
        assert kubernetes_utils.check_port_forward_mode_dependencies() is None

    assert run.call_args_list == [
        mock.call(['socat', '-V'],
                  stdout=subprocess.DEVNULL,
                  stderr=subprocess.DEVNULL,
                  check=True),
        mock.call(['nc', '-h'], capture_output=True, check=False),
    ]


def test_port_forward_dependency_check_reports_missing_tools() -> None:
    with mock.patch('subprocess.run', side_effect=FileNotFoundError):
        reasons = kubernetes_utils.check_port_forward_mode_dependencies(False)

    assert reasons is not None
    assert reasons[:2] == [
        '`socat` is required to setup Kubernetes cloud with `portforward` '
        'default networking mode and it is not installed. ',
        '`nc` is required to setup Kubernetes cloud with `portforward` '
        'default networking mode and it is not installed. ',
    ]
    assert reasons[-1] == '  $ brew install socat netcat'


def test_port_forward_dependency_check_raises_complete_guidance() -> None:
    with mock.patch('subprocess.run', side_effect=FileNotFoundError):
        with pytest.raises(RuntimeError, match='sudo apt install socat netcat'):
            kubernetes_utils.check_port_forward_mode_dependencies()


@pytest.mark.parametrize('symbol_name', [
    'construct_ssh_jump_command',
    'get_ssh_proxy_command',
    'create_proxy_command_script',
    'check_port_forward_mode_dependencies',
])
def test_ssh_helper_keeps_facade_identity(symbol_name: str) -> None:
    symbol = getattr(kubernetes_utils, symbol_name)

    assert symbol.__module__ == kubernetes_utils.__name__
