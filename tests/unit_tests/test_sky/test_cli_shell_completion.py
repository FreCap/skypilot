"""Characterization tests for the root CLI shell-completion callbacks."""

# pylint: disable=protected-access

import pickle
import shutil
import subprocess

from click import testing as click_testing
import pytest

from sky.client.cli import command


def _option_callback(name: str):
    option = next(param for param in command.cli.params if param.name == name)
    return option.callback


def test_shell_completion_callbacks_keep_facade_identity():
    install = command._install_shell_completion
    uninstall = command._uninstall_shell_completion

    assert _option_callback('install_shell_completion') is install
    assert _option_callback('uninstall_shell_completion') is uninstall
    assert install.__module__ == command.__name__
    assert uninstall.__module__ == command.__name__
    assert pickle.loads(pickle.dumps(install)) is install
    assert pickle.loads(pickle.dumps(uninstall)) is uninstall


@pytest.mark.parametrize(
    ('shell', 'command_fragments', 'reload_fragment'),
    [
        ('bash',
         ('bash_source', '.sky-complete.bash', '.bashrc'), 'source ~/.bashrc'),
        ('zsh',
         ('zsh_source', '.sky-complete.zsh', '.zshrc'), 'source ~/.zshrc'),
        ('fish', ('fish_source', '.config/fish/completions/sky.fish'), None),
    ],
)
def test_install_shell_completion_command(monkeypatch, shell, command_fragments,
                                          reload_fragment):
    calls = []

    def _record_run(*args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args=args, returncode=0)

    monkeypatch.setattr(subprocess, 'run', _record_run)
    monkeypatch.setattr(shutil, 'which', lambda executable: '/bin/bash')

    result = click_testing.CliRunner().invoke(
        command.cli, ['--install-shell-completion', shell])

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert len(args) == 1
    for fragment in command_fragments:
        assert fragment in args[0]
    assert kwargs == {
        'shell': True,
        'check': True,
        'executable': '/bin/bash',
    }
    assert f'Shell completion installed for {shell}' in result.output
    if reload_fragment is None:
        assert 'restart the terminal' not in result.output
    else:
        assert reload_fragment in result.output


@pytest.mark.parametrize(
    ('shell', 'command_fragments', 'reload_fragment'),
    [
        ('bash', ('.sky-complete.bash', '.bashrc'), 'source ~/.bashrc'),
        ('zsh', ('.sky-complete.zsh', '.zshrc'), 'source ~/.zshrc'),
        ('fish', ('.config/fish/completions/sky.fish',), None),
    ],
)
def test_uninstall_shell_completion_command(monkeypatch, shell,
                                            command_fragments, reload_fragment):
    calls = []

    def _record_run(*args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args=args, returncode=0)

    monkeypatch.setattr(subprocess, 'run', _record_run)

    result = click_testing.CliRunner().invoke(
        command.cli, ['--uninstall-shell-completion', shell])

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert len(args) == 1
    for fragment in command_fragments:
        assert fragment in args[0]
    assert kwargs == {'shell': True, 'check': True}
    assert f'Shell completion uninstalled for {shell}' in result.output
    if reload_fragment is None:
        assert 'restart the terminal' not in result.output
    else:
        assert reload_fragment in result.output


def test_install_shell_completion_auto_requires_shell(monkeypatch):
    monkeypatch.delenv('SHELL', raising=False)

    result = click_testing.CliRunner().invoke(
        command.cli, ['--install-shell-completion', 'auto'])

    assert result.exit_code == 0
    assert 'Cannot auto-detect shell' in result.output
