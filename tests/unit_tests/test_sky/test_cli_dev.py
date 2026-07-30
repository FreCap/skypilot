"""Tests for the ``sky dev`` command group."""

from __future__ import annotations

import pathlib
import subprocess
from unittest import mock

import click
from click import testing as click_testing
import pytest

from sky.client.cli import command
from sky.utils import common
from sky.utils import status_lib

# pylint: disable=protected-access


def _write_manifest(tmp_path: pathlib.Path,
                    *,
                    ide: str = 'vscode',
                    remote_path: str = '/home/sky/sky_workdir',
                    setup: str | None = None) -> pathlib.Path:
    manifest_path = tmp_path / 'dev.yaml'
    setup_yaml = '' if setup is None else f'setup: |\n  {setup}\n'
    manifest_path.write_text(
        f'dev:\n'
        f'  version: 1\n'
        f'  name: test-dev\n'
        f'  ide: {ide}\n'
        f'  remote_path: {remote_path}\n'
        f'resources:\n'
        f'  cpus: 2\n'
        f'  autostop:\n'
        f'    idle_minutes: 30\n'
        f'{setup_yaml}',
        encoding='utf-8')
    return manifest_path


def _fake_handle() -> mock.Mock:
    handle = mock.Mock()
    handle.get_cluster_name.return_value = 'test-dev'
    return handle


def test_dev_group_registration_and_order() -> None:
    root_commands = command.cli.list_commands(click.Context(command.cli))
    dev_index = root_commands.index('dev')

    assert command.cli.commands['dev'] is command.dev
    assert root_commands[dev_index - 1:dev_index +
                         2] == ['down', 'dev', 'check']
    assert command.dev.list_commands(click.Context(
        command.dev)) == ['up', 'open', 'ssh', 'status', 'stop', 'down']


def test_dev_group_help_order() -> None:
    result = click_testing.CliRunner().invoke(command.dev, ['--help'])

    assert result.exit_code == 0
    command_section = result.output.split('Commands:\n', maxsplit=1)[1]
    command_names = [
        line.strip().split(maxsplit=1)[0]
        for line in command_section.splitlines()
        if line.startswith('  ')
    ]
    assert command_names == ['up', 'open', 'ssh', 'status', 'stop', 'down']


@pytest.mark.parametrize('reconfigure, expected_fast', [(False, True),
                                                        (True, False)])
def test_dev_up_launches_with_fast_reconcile(tmp_path: pathlib.Path,
                                             monkeypatch: pytest.MonkeyPatch,
                                             reconfigure: bool,
                                             expected_fast: bool) -> None:
    manifest_path = _write_manifest(tmp_path)
    handle = _fake_handle()
    launch = mock.Mock(return_value='request-id')
    wait = mock.Mock(return_value=(None, handle, {'ssh_user': 'sky'}))
    set_ssh = mock.Mock()
    fallback = mock.Mock()
    opener = mock.Mock()
    monkeypatch.setattr(command.sdk, 'launch', launch)
    monkeypatch.setattr(command, '_async_call_or_wait', wait)
    monkeypatch.setattr(command, '_set_ssh_config_from_launch_response',
                        set_ssh)
    monkeypatch.setattr(command, '_get_cluster_records_and_set_ssh_config',
                        fallback)
    monkeypatch.setattr(command, '_open_dev_editor', opener)
    usage_update = mock.Mock()
    monkeypatch.setattr(command.usage_lib.messages.usage,
                        'update_user_task_yaml', usage_update)
    args = ['-f', str(manifest_path), '--yes']
    if reconfigure:
        args.append('--reconfigure')

    result = click_testing.CliRunner().invoke(command.dev_up, args)

    assert result.exit_code == 0, result.output
    kwargs = launch.call_args.kwargs
    assert kwargs['cluster_name'] == 'test-dev'
    assert kwargs['fast'] is expected_fast
    assert kwargs['_need_confirmation'] is False
    assert kwargs['_include_credentials'] is True
    assert launch.call_args.args[0].run is None
    wait.assert_called_once_with('request-id', False, 'sky.dev.up')
    set_ssh.assert_called_once_with(handle, {'ssh_user': 'sky'})
    fallback.assert_not_called()
    opener.assert_not_called()
    usage_config = usage_update.call_args.args[0]
    assert isinstance(usage_config, dict)
    assert 'dev' not in usage_config
    assert usage_config['resources']['cpus'] == 2
    assert 'ssh test-dev' in result.output
    assert 'vscode://vscode-remote/ssh-remote+test-dev' in result.output


def test_dev_up_legacy_launch_result_uses_status_fallback(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = _write_manifest(tmp_path)
    handle = _fake_handle()
    monkeypatch.setattr(command.sdk, 'launch',
                        mock.Mock(return_value='request-id'))
    monkeypatch.setattr(command, '_async_call_or_wait',
                        mock.Mock(return_value=(None, handle)))
    direct = mock.Mock()
    fallback = mock.Mock()
    monkeypatch.setattr(command, '_set_ssh_config_from_launch_response', direct)
    monkeypatch.setattr(command, '_get_cluster_records_and_set_ssh_config',
                        fallback)
    monkeypatch.setattr(command.usage_lib.messages.usage,
                        'update_user_task_yaml', mock.Mock())

    result = click_testing.CliRunner().invoke(
        command.dev_up, ['-f', str(manifest_path), '--yes'])

    assert result.exit_code == 0, result.output
    direct.assert_not_called()
    fallback.assert_called_once_with(clusters=['test-dev'])


def test_dev_up_open_is_explicit(tmp_path: pathlib.Path,
                                 monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = _write_manifest(tmp_path)
    handle = _fake_handle()
    monkeypatch.setattr(command.sdk, 'launch',
                        mock.Mock(return_value='request-id'))
    monkeypatch.setattr(
        command, '_async_call_or_wait',
        mock.Mock(return_value=(None, handle, {
            'ssh_user': 'sky'
        })))
    monkeypatch.setattr(command, '_set_ssh_config_from_launch_response',
                        mock.Mock())
    monkeypatch.setattr(command.usage_lib.messages.usage,
                        'update_user_task_yaml', mock.Mock())
    opener = mock.Mock()
    monkeypatch.setattr(command, '_open_dev_editor', opener)

    result = click_testing.CliRunner().invoke(
        command.dev_up, ['-f', str(manifest_path), '--yes', '--open'])

    assert result.exit_code == 0, result.output
    opener.assert_called_once_with(
        'vscode://vscode-remote/ssh-remote+test-dev/home/sky/sky_workdir')


def test_dev_up_rejects_open_for_none_before_launch(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = _write_manifest(tmp_path, ide='none')
    launch = mock.Mock()
    monkeypatch.setattr(command.sdk, 'launch', launch)

    result = click_testing.CliRunner().invoke(
        command.dev_up, ['-f', str(manifest_path), '--yes', '--open'])

    assert result.exit_code != 0
    assert 'dev.ide is set to none' in result.output
    launch.assert_not_called()


def test_dev_up_applies_config_override_to_every_resource(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = _write_manifest(tmp_path)
    handle = _fake_handle()
    launch = mock.Mock(return_value='request-id')
    monkeypatch.setattr(command.sdk, 'launch', launch)
    monkeypatch.setattr(
        command, '_async_call_or_wait',
        mock.Mock(return_value=(None, handle, {
            'ssh_user': 'sky'
        })))
    monkeypatch.setattr(command, '_set_ssh_config_from_launch_response',
                        mock.Mock())
    monkeypatch.setattr(command.usage_lib.messages.usage,
                        'update_user_task_yaml', mock.Mock())

    result = click_testing.CliRunner().invoke(command.dev_up, [
        '-f',
        str(manifest_path),
        '--yes',
        '--config',
        'active_workspace=team-a',
    ])

    assert result.exit_code == 0, result.output
    task = launch.call_args.args[0]
    for resource in task.resources:
        assert resource.cluster_config_overrides['active_workspace'] == (
            'team-a')


def test_dev_up_job_id_fails_without_cleanup(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = _write_manifest(tmp_path)
    handle = _fake_handle()
    monkeypatch.setattr(command.sdk, 'launch',
                        mock.Mock(return_value='request-id'))
    monkeypatch.setattr(
        command, '_async_call_or_wait',
        mock.Mock(return_value=(7, handle, {
            'ssh_user': 'sky'
        })))
    monkeypatch.setattr(command, '_set_ssh_config_from_launch_response',
                        mock.Mock())
    monkeypatch.setattr(command.usage_lib.messages.usage,
                        'update_user_task_yaml', mock.Mock())
    down = mock.Mock()
    monkeypatch.setattr(command.sdk, 'down', down)

    result = click_testing.CliRunner().invoke(
        command.dev_up, ['-f', str(manifest_path), '--yes'])

    assert result.exit_code != 0
    assert 'unexpectedly submitted a user job' in result.output
    down.assert_not_called()


def test_dev_up_tails_setup_carrier_before_ready(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = _write_manifest(tmp_path, setup='echo setup')
    handle = _fake_handle()
    monkeypatch.setattr(command.sdk, 'launch',
                        mock.Mock(return_value='request-id'))
    monkeypatch.setattr(
        command, '_async_call_or_wait',
        mock.Mock(return_value=(7, handle, {
            'ssh_user': 'sky'
        })))
    monkeypatch.setattr(command, '_set_ssh_config_from_launch_response',
                        mock.Mock())
    monkeypatch.setattr(command.usage_lib.messages.usage,
                        'update_user_task_yaml', mock.Mock())
    events = []
    tail_logs = mock.Mock(
        side_effect=lambda *args, **kwargs: events.append('tail') or 0)
    monkeypatch.setattr(command.sdk, 'tail_logs', tail_logs)
    monkeypatch.setattr(command, '_print_dev_connection_hints',
                        lambda cluster_name: events.append('ready'))

    result = click_testing.CliRunner().invoke(
        command.dev_up, ['-f', str(manifest_path), '--yes'])

    assert result.exit_code == 0, result.output
    tail_logs.assert_called_once_with('test-dev', 7, follow=True)
    assert events == ['tail', 'ready']


def test_dev_up_setup_carrier_failure_preserves_cluster(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = _write_manifest(tmp_path, setup='false')
    handle = _fake_handle()
    monkeypatch.setattr(command.sdk, 'launch',
                        mock.Mock(return_value='request-id'))
    monkeypatch.setattr(
        command, '_async_call_or_wait',
        mock.Mock(return_value=(9, handle, {
            'ssh_user': 'sky'
        })))
    monkeypatch.setattr(command, '_set_ssh_config_from_launch_response',
                        mock.Mock())
    monkeypatch.setattr(command.usage_lib.messages.usage,
                        'update_user_task_yaml', mock.Mock())
    monkeypatch.setattr(command.sdk, 'tail_logs', mock.Mock(return_value=23))
    down = mock.Mock()
    monkeypatch.setattr(command.sdk, 'down', down)

    result = click_testing.CliRunner().invoke(
        command.dev_up, ['-f', str(manifest_path), '--yes'])

    assert result.exit_code == 23
    assert 'setup job 9 failed' in result.output
    assert 'left running for inspection' in result.output
    assert 'is ready' not in result.output
    down.assert_not_called()


def test_dev_up_prints_ready_hint_before_editor_resolution_failure(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = _write_manifest(tmp_path, remote_path='~/sky_workdir')
    handle = _fake_handle()
    monkeypatch.setattr(command.sdk, 'launch',
                        mock.Mock(return_value='request-id'))
    monkeypatch.setattr(
        command, '_async_call_or_wait',
        mock.Mock(return_value=(None, handle, {
            'ssh_user': 'sky'
        })))
    monkeypatch.setattr(command, '_set_ssh_config_from_launch_response',
                        mock.Mock())
    monkeypatch.setattr(
        command, '_resolve_dev_remote_path',
        mock.Mock(side_effect=click.ClickException('remote home unavailable')))
    monkeypatch.setattr(command.usage_lib.messages.usage,
                        'update_user_task_yaml', mock.Mock())

    result = click_testing.CliRunner().invoke(
        command.dev_up, ['-f', str(manifest_path), '--yes'])

    assert result.exit_code != 0
    assert 'Development environment' in result.output
    assert 'SSH: ssh test-dev' in result.output
    assert 'remote home unavailable' in result.output


def test_require_up_dev_cluster_refreshes_ssh_config(
        monkeypatch: pytest.MonkeyPatch) -> None:
    status = mock.Mock(return_value=[{
        'name': 'test-dev',
        'status': status_lib.ClusterStatus.UP
    }])
    monkeypatch.setattr(command, '_get_cluster_records_and_set_ssh_config',
                        status)

    record = command._require_up_dev_cluster('test-dev')

    assert record['name'] == 'test-dev'
    status.assert_called_once_with(clusters=['test-dev'],
                                   refresh=common.StatusRefreshMode.AUTO)


@pytest.mark.parametrize('records, expected', [
    ([], 'does not exist or is not visible'),
    ([{
        'name': 'test-dev',
        'status': status_lib.ClusterStatus.STOPPED
    }], 'is STOPPED, not UP'),
])
def test_require_up_dev_cluster_rejects_non_up(monkeypatch: pytest.MonkeyPatch,
                                               records: list[dict[str, object]],
                                               expected: str) -> None:
    monkeypatch.setattr(command, '_get_cluster_records_and_set_ssh_config',
                        mock.Mock(return_value=records))
    with pytest.raises(click.UsageError, match=expected):
        command._require_up_dev_cluster('test-dev')


def test_resolve_home_relative_remote_path_uses_fixed_ssh_argv(
        monkeypatch: pytest.MonkeyPatch) -> None:
    marker = command._DEV_REMOTE_HOME_MARKER
    run = mock.Mock(return_value=subprocess.CompletedProcess(
        args=[], returncode=0, stdout=f'banner\n{marker}/home/sky{marker}\n'))
    monkeypatch.setattr(command.subprocess_utils, 'run', run)

    resolved = command._resolve_dev_remote_path('test-dev', '~/sky work')

    assert resolved == '/home/sky/sky work'
    argv = run.call_args.args[0]
    assert argv[:7] == [
        'ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
        'test-dev'
    ]
    assert marker in argv[7]
    assert run.call_args.kwargs['shell'] is False
    assert run.call_args.kwargs['timeout'] == 30


@pytest.mark.parametrize('completed', [
    subprocess.CompletedProcess(args=[], returncode=1, stdout=''),
    subprocess.CompletedProcess(args=[], returncode=0, stdout='no marker'),
    subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=(f'{command._DEV_REMOTE_HOME_MARKER}'
                f'relative{command._DEV_REMOTE_HOME_MARKER}\n')),
    subprocess.CompletedProcess(
        args=[], returncode=0, stdout='x' * (64 * 1024 + 1)),
])
def test_resolve_home_relative_remote_path_fails_safely(
        monkeypatch: pytest.MonkeyPatch,
        completed: subprocess.CompletedProcess[str]) -> None:
    monkeypatch.setattr(command.subprocess_utils, 'run',
                        mock.Mock(return_value=completed))
    with pytest.raises(click.ClickException, match='remote home|Remote home'):
        command._resolve_dev_remote_path('test-dev', '~/sky_workdir')


def test_resolve_home_relative_remote_path_times_out(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        command.subprocess_utils, 'run',
        mock.Mock(side_effect=subprocess.TimeoutExpired('ssh', 30)))
    with pytest.raises(click.ClickException, match='Timed out'):
        command._resolve_dev_remote_path('test-dev', '~/sky_workdir')


def test_dev_open_print_only_does_not_invoke_opener(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(command, '_require_up_dev_cluster', mock.Mock())
    opener = mock.Mock()
    monkeypatch.setattr(command, '_open_dev_editor', opener)

    result = click_testing.CliRunner().invoke(
        command.dev_open,
        ['test-dev', '--remote-path', '/workspace', '--print-only'])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == (
        'vscode://vscode-remote/ssh-remote+test-dev/workspace')
    opener.assert_not_called()


def test_dev_ssh_uses_argv_and_propagates_exit(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(command, '_require_up_dev_cluster', mock.Mock())
    run = mock.Mock(
        return_value=subprocess.CompletedProcess(args=[], returncode=23))
    monkeypatch.setattr(command.subprocess_utils, 'run', run)

    result = click_testing.CliRunner().invoke(command.dev_ssh, ['test-dev'])

    assert result.exit_code == 23
    run.assert_called_once_with(['ssh', 'test-dev'], shell=False, check=False)


@pytest.mark.parametrize('dev_command, root_command, args, expected', [
    ('dev_status', 'status', ['test-dev', '--refresh'], {
        'clusters': ['test-dev'],
        'refresh': True,
        'show_managed_jobs': False,
        'show_services': False,
        'show_pools': False,
    }),
    ('dev_stop', 'stop', ['test-dev', '--yes'], {
        'clusters': ['test-dev'],
        'all': False,
        'all_users': False,
        'yes': True,
        'async_call': False,
    }),
    ('dev_down', 'down', ['test-dev', '--yes'], {
        'clusters': ['test-dev'],
        'all': False,
        'all_users': False,
        'yes': True,
        'purge': False,
        'async_call': False,
    }),
])
def test_dev_lifecycle_delegates_to_root_command(
        monkeypatch: pytest.MonkeyPatch, dev_command: str, root_command: str,
        args: list[str], expected: dict[str, object]) -> None:
    root = getattr(command, root_command)
    callback = mock.Mock()
    monkeypatch.setattr(root, 'callback', callback)

    result = click_testing.CliRunner().invoke(getattr(command, dev_command),
                                              args)

    assert result.exit_code == 0, result.output
    callback.assert_called_once()
    actual = callback.call_args.kwargs
    for key, value in expected.items():
        assert actual[key] == value
