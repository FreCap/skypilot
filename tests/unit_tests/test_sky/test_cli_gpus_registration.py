"""Characterization tests for the GPU CLI command family."""

import pickle
import re

from click.testing import CliRunner

from sky.client.cli import command


def test_gpu_commands_keep_root_registration_and_callback_identity():
    root_commands = list(command.cli.commands)
    assert root_commands[root_commands.index('check'
                                            ):root_commands.index('storage') +
                         1] == ['check', 'show-gpus', 'gpus', 'storage']
    assert command.cli.commands['show-gpus'] is command.show_gpus
    assert command.cli.commands['gpus'] is command.gpus_cli
    assert command.show_gpus.hidden
    assert list(command.gpus_cli.commands) == ['list', 'label']
    assert command.gpus_cli.commands['list'] is command.gpus_list
    assert command.gpus_cli.commands['label'] is command.gpus_label


def test_gpu_command_callbacks_keep_historical_name_and_module_identity():
    expected_names = ('show_gpus', 'gpus_cli', 'gpus_list', 'gpus_label')
    for gpu_command, expected_name in zip(
        (command.show_gpus, command.gpus_cli, command.gpus_list,
         command.gpus_label), expected_names):
        callback = gpu_command.callback
        assert callback is not None
        assert callback.__name__ == expected_name
        assert callback.__module__ == command.__name__

    show_gpus_impl = getattr(command, '_show_gpus_impl')
    assert show_gpus_impl.__module__ == command.__name__
    assert pickle.loads(pickle.dumps(show_gpus_impl)) is show_gpus_impl


def test_gpu_command_help_and_visibility_are_stable():
    runner = CliRunner()
    root_help = runner.invoke(command.cli, ['--help'])
    assert root_help.exit_code == 0
    assert 'show-gpus' not in root_help.output
    assert re.search(r'^  gpus\s+SkyPilot GPU/Accelerator CLI\.$',
                     root_help.output, re.MULTILINE)

    gpus_help = runner.invoke(command.gpus_cli, ['--help'])
    assert gpus_help.exit_code == 0
    assert gpus_help.output.index('list ') < gpus_help.output.index('label ')
    assert 'Show supported GPU/TPU/accelerators and their prices.' in (
        gpus_help.output)
    assert 'Label GPU nodes in a Kubernetes cluster for use with SkyPilot.' in (
        gpus_help.output)
