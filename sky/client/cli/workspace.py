"""Commands for selecting and inspecting the current workspace."""

import json

import click

from sky import exceptions
from sky.client import sdk
from sky.client.cli import click_utils
from sky.client.cli import flags
from sky.usage import usage_lib
from sky.utils import ux_utils
from sky.workspaces import constants as workspace_constants


@click.group(cls=click_utils.NaturalOrderGroup)
def workspace():
    """Per-user workspace commands."""
    pass


@workspace.command('use', cls=click_utils.DocumentedCodeCommand)
@click.argument('name', required=False, type=str)
@click.option('--clear',
              is_flag=True,
              default=False,
              help='Clear the saved preferred workspace.')
@flags.config_option(expose_value=False)
@usage_lib.entrypoint
def workspace_use(name: str | None, clear: bool):
    """Sets (or clears with --clear) your default workspace on the server.

    This default is picked up by ``sky launch`` / ``sky jobs launch`` when
    no explicit ``active_workspace`` is in effect. Anything that DOES set
    ``active_workspace`` still wins — including a per-command
    ``--workspace`` / ``-w`` flag, ``--config active_workspace=X``,
    project ``./.sky.yaml``, user ``~/.sky/config.yaml``, or a server-
    side ``active_workspace`` pinned by an admin.

    Examples:

    .. code-block:: bash

      # Set team-a as your default.
      sky workspace use team-a
      \b
      # Clear the default.
      sky workspace use --clear
    """
    if clear and name:
        raise click.UsageError('Cannot pass both --clear and a workspace name.')
    if not clear and not name:
        raise click.UsageError(
            'Specify a workspace name, or pass --clear to remove your '
            'current default.')
    target = None if clear else name
    sdk.set_preferred_workspace(target)
    if clear:
        click.secho('Cleared preferred workspace.', fg='green')
    else:
        click.secho(f'Set preferred workspace to {target!r}.', fg='green')


@workspace.command('info', cls=click_utils.DocumentedCodeCommand)
@flags.config_option(expose_value=False)
@click.option('-o',
              '--output',
              'output_format',
              type=click.Choice(flags.OUTPUT_FORMAT_CHOICES,
                                case_sensitive=False),
              default=flags.OUTPUT_FORMAT_TABLE,
              help='Output format (default: table). Use "json" for a '
              'machine-readable shape.')
@usage_lib.entrypoint
def workspace_info(output_format: str):
    """Shows the workspace your next request lands in by default, plus
    your saved preferred and the workspaces you can access.

    A one-off ``--workspace <name>`` flag on the next command still wins;
    this view reflects what happens when no such override is passed.
    """
    info = sdk.get_user_workspace()
    if output_format == flags.OUTPUT_FORMAT_JSON:
        click.echo(json.dumps(info, indent=2))
        return

    workspace_str = (f'{info["workspace"]!r}'
                     if info.get('workspace') is not None else '(none)')
    source_str = info.get('source') or '-'
    preferred = info.get('preferred')
    preferred_str = (f'{preferred!r}' if preferred is not None else '(not set)')
    accessible = info.get('accessible') or []
    accessible_str = (', '.join(
        repr(w) for w in accessible) if accessible else '(none)')
    note = info.get('note')
    lines = [
        f'Workspace: {workspace_str}',
        f'{ux_utils.INDENT_SYMBOL}Source: {source_str}',
    ]
    if note:
        lines.append(f'{ux_utils.INDENT_SYMBOL}Note: {note}')
    lines.extend([
        f'{ux_utils.INDENT_SYMBOL}Preferred: {preferred_str}',
        f'{ux_utils.INDENT_LAST_SYMBOL}Accessible: {accessible_str}',
    ])
    click.echo('\n'.join(lines))

    # AMBIGUOUS is the only state whose recovery message is multi-line
    # (5+ lines) — inlining it into `Note:` would break the tree
    # alignment, so render it as a separate paragraph below. The text
    # comes from `WorkspaceAmbiguousError.recovery_hint()` so the CLI
    # and launch-path error message share a single source.
    if info.get('source') == workspace_constants.WORKSPACE_SOURCE_AMBIGUOUS:
        click.echo()
        click.echo(exceptions.WorkspaceAmbiguousError.recovery_hint())
