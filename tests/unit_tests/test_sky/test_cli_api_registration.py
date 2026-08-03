"""Characterization tests for the ``sky api`` command group."""

from unittest import mock

import click
from click import testing as click_testing

from sky.client.cli import api as api_commands
from sky.client.cli import command


def test_api_group_registration_and_command_identity() -> None:
    api_group = command.cli.commands['api']
    root_commands = command.cli.list_commands(click.Context(command.cli))

    assert api_group is command.api
    api_index = root_commands.index('api')
    assert root_commands[api_index - 1:api_index +
                         2] == ['serve', 'api', 'workspace']
    assert api_group.list_commands(click.Context(api_group)) == [
        'start',
        'stop',
        'logs',
        'cancel',
        'status',
        'login',
        'logout',
        'info',
    ]
    for command_name in api_group.commands:
        callback = getattr(command, f'api_{command_name}')
        assert api_group.commands[command_name] is callback
        assert callback.__module__ == command.__name__


def test_api_group_help_lists_commands_in_definition_order() -> None:
    result = click_testing.CliRunner().invoke(command.api, ['--help'])

    assert result.exit_code == 0
    command_section = result.output.split('Commands:\n', maxsplit=1)[1]
    command_lines = [
        line.strip().split(maxsplit=1)[0]
        for line in command_section.splitlines()
        if line.startswith('  ')
    ]
    assert command_lines == [
        'start',
        'stop',
        'logs',
        'cancel',
        'status',
        'login',
        'logout',
        'info',
    ]


def test_api_start_forwards_metrics_configuration(monkeypatch) -> None:
    start = mock.Mock()
    server_info = mock.sentinel.server_info
    monkeypatch.setattr(api_commands.sdk, 'api_start', start)
    monkeypatch.setattr(api_commands.server_common, 'get_server_url',
                        lambda host: f'http://{host}:46580')
    monkeypatch.setattr(api_commands.server_common, 'get_api_server_status',
                        lambda endpoint: server_info)
    monkeypatch.setattr(api_commands.server_common,
                        'check_and_print_upgrade_hint', mock.Mock())

    result = click_testing.CliRunner().invoke(
        command.api_start,
        ['--deploy', '--foreground', '--metrics', '--metrics-port', '9191'])

    assert result.exit_code == 0, result.output
    start.assert_called_once_with(deploy=True,
                                  host='127.0.0.1',
                                  foreground=True,
                                  metrics=True,
                                  metrics_port=9191,
                                  enable_basic_auth=False)
