"""Shell completion installation and removal callbacks."""

import os
import shutil
import subprocess

import click

_RELOAD_ZSH_CMD = 'source ~/.zshrc'
_RELOAD_BASH_CMD = 'source ~/.bashrc'


def _install_shell_completion(ctx: click.Context, param: click.Parameter,
                              value: str):
    """A callback for installing shell completion for click."""
    del param  # Unused.
    if not value or ctx.resilient_parsing:
        return

    if value == 'auto':
        if 'SHELL' not in os.environ:
            click.secho(
                'Cannot auto-detect shell. Please specify shell explicitly.',
                fg='red')
            ctx.exit()
        else:
            value = os.path.basename(os.environ['SHELL'])

    zshrc_diff = '\n# For SkyPilot shell completion\n. ~/.sky/.sky-complete.zsh'
    bashrc_diff = ('\n# For SkyPilot shell completion'
                   '\n. ~/.sky/.sky-complete.bash')

    cmd: str | None = None
    reload_cmd: str | None = None

    if value == 'bash':
        install_cmd = f'_SKY_COMPLETE=bash_source sky > \
                ~/.sky/.sky-complete.bash && \
                echo "{bashrc_diff}" >> ~/.bashrc'

        cmd = (f'(grep -q "SkyPilot" ~/.bashrc) || '
               f'([[ ${{BASH_VERSINFO[0]}} -ge 4 ]] && ({install_cmd}) || '
               f'(echo "Bash must be version 4 or above." && exit 1))')

        reload_cmd = _RELOAD_BASH_CMD

    elif value == 'fish':
        cmd = '_SKY_COMPLETE=fish_source sky > \
                ~/.config/fish/completions/sky.fish'

        # Fish does not need to be reloaded and will automatically pick up
        # completions.
        reload_cmd = None

    elif value == 'zsh':
        install_cmd = f'_SKY_COMPLETE=zsh_source sky > \
                ~/.sky/.sky-complete.zsh && \
                echo "{zshrc_diff}" >> ~/.zshrc'

        cmd = f'(grep -q "SkyPilot" ~/.zshrc) || ({install_cmd})'
        reload_cmd = _RELOAD_ZSH_CMD

    else:
        click.secho(f'Unsupported shell: {value}', fg='red')
        ctx.exit()

    assert cmd is not None  # This should never be None due to ctx.exit() above
    try:
        subprocess.run(cmd,
                       shell=True,
                       check=True,
                       executable=shutil.which('bash'))
        click.secho(f'Shell completion installed for {value}', fg='green')
        if reload_cmd is not None:
            click.echo(
                'Completion will take effect once you restart the terminal: ' +
                click.style(f'{reload_cmd}', bold=True))
    except subprocess.CalledProcessError as e:
        click.secho(f'> Installation failed with code {e.returncode}', fg='red')
    ctx.exit()


def _uninstall_shell_completion(ctx: click.Context, param: click.Parameter,
                                value: str):
    """A callback for uninstalling shell completion for click."""
    del param  # Unused.
    if not value or ctx.resilient_parsing:
        return

    if value == 'auto':
        if 'SHELL' not in os.environ:
            click.secho(
                'Cannot auto-detect shell. Please specify shell explicitly.',
                fg='red')
            ctx.exit()
        else:
            value = os.path.basename(os.environ['SHELL'])

    cmd: str | None = None
    reload_cmd: str | None = None

    if value == 'bash':
        cmd = 'sed -i"" -e "/# For SkyPilot shell completion/d" ~/.bashrc && \
               sed -i"" -e "/sky-complete.bash/d" ~/.bashrc && \
               rm -f ~/.sky/.sky-complete.bash'

        reload_cmd = _RELOAD_BASH_CMD

    elif value == 'fish':
        cmd = 'rm -f ~/.config/fish/completions/sky.fish'
        # Fish does not need to be reloaded and will automatically pick up
        # completions.
        reload_cmd = None

    elif value == 'zsh':
        cmd = 'sed -i"" -e "/# For SkyPilot shell completion/d" ~/.zshrc && \
               sed -i"" -e "/sky-complete.zsh/d" ~/.zshrc && \
               rm -f ~/.sky/.sky-complete.zsh'

        reload_cmd = _RELOAD_ZSH_CMD

    else:
        click.secho(f'Unsupported shell: {value}', fg='red')
        ctx.exit()

    assert cmd is not None  # This should never be None due to ctx.exit() above
    try:
        subprocess.run(cmd, shell=True, check=True)
        click.secho(f'Shell completion uninstalled for {value}', fg='green')
        if reload_cmd is not None:
            click.echo(
                'Changes will take effect once you restart the terminal: ' +
                click.style(f'{reload_cmd}', bold=True))
    except subprocess.CalledProcessError as e:
        click.secho(f'> Uninstallation failed with code {e.returncode}',
                    fg='red')
    ctx.exit()
