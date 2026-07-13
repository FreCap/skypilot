"""Characterization tests for the ``sky workspace`` command group."""

import click
from click import testing as click_testing

from sky.client.cli import command


def test_workspace_group_registration_and_command_identity() -> None:
    workspace_group = command.cli.commands['workspace']
    root_commands = command.cli.list_commands(click.Context(command.cli))

    assert workspace_group is command.workspace
    workspace_index = root_commands.index('workspace')
    assert root_commands[workspace_index - 1:workspace_index +
                         2] == ['api', 'workspace', 'ssh']
    assert workspace_group.list_commands(
        click.Context(workspace_group)) == ['use', 'info']
    for command_name in workspace_group.commands:
        callback = getattr(command, f'workspace_{command_name}')
        assert workspace_group.commands[command_name] is callback
        assert callback.callback is not None
        assert callback.callback.__module__ == command.__name__


def test_workspace_group_help_lists_commands_in_definition_order() -> None:
    result = click_testing.CliRunner().invoke(command.workspace, ['--help'])

    assert result.exit_code == 0
    command_section = result.output.split('Commands:\n', maxsplit=1)[1]
    command_lines = [
        line.strip().split(maxsplit=1)[0]
        for line in command_section.splitlines()
        if line.startswith('  ')
    ]
    assert command_lines == ['use', 'info']
