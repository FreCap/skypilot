"""Shared Click command and group helpers."""

import click

from sky.usage import usage_lib


class NaturalOrderGroup(click.Group):
    """Lists commands in definition order, excluding aliases and hidden commands."""

    def list_commands(self, ctx):  # pylint: disable=unused-argument
        seen_commands = set()
        names = []
        for name, command in self.commands.items():
            if getattr(command, 'hidden', False):
                continue
            command_id = id(command)
            if command_id in seen_commands:
                continue
            seen_commands.add(command_id)
            names.append(name)
        return names

    @usage_lib.entrypoint('sky.cli', fallback=True)
    def invoke(self, ctx):
        return super().invoke(ctx)


class DocumentedCodeCommand(click.Command):
    """Renders documented code blocks correctly in Click help output."""

    def get_help(self, ctx):
        help_str = ctx.command.help
        ctx.command.help = help_str.replace('.. code-block:: bash\n', '\b')
        return super().get_help(ctx)


def get_click_major_version() -> int:
    return int(click.__version__.split('.', maxsplit=1)[0])


def get_shell_complete_args(complete_fn):
    """Returns the Click shell-completion keyword when it is supported."""
    if get_click_major_version() >= 8:
        return dict(shell_complete=complete_fn)
    return {}
