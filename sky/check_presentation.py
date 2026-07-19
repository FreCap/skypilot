"""Terminal presentation helpers for cloud credential checks."""
from collections.abc import Callable
from types import ModuleType

import click
import colorama

from sky import clouds as sky_clouds
from sky.clouds import cloud as sky_cloud
from sky.utils import common_utils
from sky.utils import registry
from sky.utils import ux_utils

PARTY_POPPER_EMOJI = '\U0001F389'  # Party popper unicode


def _print_checked_cloud(
    echo: Callable,
    verbose: bool,
    cloud_tuple: tuple[str, sky_clouds.Cloud | ModuleType],
    cloud_capabilities: list[tuple[sky_cloud.CloudCapability, bool,
                                   str | dict[str, str] | None]],
    ctx2text: dict[str, str],
) -> None:
    """Prints whether a cloud is enabled, and the capabilities that are enabled.
    If any hints (for enabled capabilities) or
    reasons (for disabled capabilities) are provided, they will be printed.

    Args:
        echo: The function to use to print the message.
        verbose: Whether to print the verbose output.
        cloud_tuple: The cloud to print the capabilities for.
        cloud_capabilities: The capabilities for the cloud.
    """

    def _yellow_color(str_to_format: str) -> str:
        return (f'{colorama.Fore.LIGHTYELLOW_EX}'
                f'{str_to_format}'
                f'{colorama.Style.RESET_ALL}')

    cloud_repr, cloud = cloud_tuple
    # Print the capabilities for the cloud.
    # consider cloud enabled if any capability is enabled.
    enabled_capabilities: list[sky_cloud.CloudCapability] = []
    hints_to_capabilities: dict[str, list[sky_cloud.CloudCapability]] = {}
    reasons_to_capabilities: dict[str, list[sky_cloud.CloudCapability]] = {}
    for capability, ok, reason in cloud_capabilities:
        if ok:
            enabled_capabilities.append(capability)
        # `dict` reasons for K8s and SSH will be printed in detail in
        # _format_enabled_cloud. Skip here unless the cloud is disabled.
        if not isinstance(reason, str):
            if not ok and isinstance(
                    cloud_tuple[1],
                (sky_clouds.SSH, sky_clouds.Kubernetes, sky_clouds.Slurm)):
                if reason is not None:
                    reason_str = _format_context_details(cloud_tuple[1],
                                                         show_details=True,
                                                         ctx2text=reason)
                    reason_str = '\n'.join(
                        '    ' + line for line in reason_str.splitlines())
                    reasons_to_capabilities.setdefault(reason_str,
                                                       []).append(capability)
            continue
        if ok:
            if reason is not None:
                hints_to_capabilities.setdefault(reason, []).append(capability)
        elif reason is not None:
            reasons_to_capabilities.setdefault(reason, []).append(capability)
    style_str = f'{colorama.Style.DIM}'
    status_msg: str = 'disabled'
    capability_string: str = ''
    detail_string: str = ''
    activated_account: str | None = None
    if enabled_capabilities:
        style_str = f'{colorama.Fore.GREEN}{colorama.Style.NORMAL}'
        status_msg = 'enabled'
        capability_string = f'[{", ".join(enabled_capabilities)}]'
        if verbose and isinstance(cloud, sky_cloud.Cloud):
            activated_account = cloud.get_active_user_identity_str()
        if isinstance(
                cloud_tuple[1],
            (sky_clouds.SSH, sky_clouds.Kubernetes, sky_clouds.Slurm)):
            detail_string = _format_context_details(cloud_tuple[1],
                                                    show_details=True,
                                                    ctx2text=ctx2text)
    echo(
        click.style(
            f'{style_str}  {cloud_repr}: {status_msg} {capability_string}'
            f'{colorama.Style.RESET_ALL}{detail_string}'))
    if activated_account is not None:
        echo(f'    Activated account: {activated_account}')
    for reason, capabilities in hints_to_capabilities.items():
        echo(f'    Hint [{", ".join(capabilities)}]: {_yellow_color(reason)}')
    for reason, capabilities in reasons_to_capabilities.items():
        echo(f'    Reason [{", ".join(capabilities)}]: {reason}')


def _green_color(str_to_format: str) -> str:
    return f'{colorama.Fore.GREEN}{str_to_format}{colorama.Style.RESET_ALL}'


def _format_context_details(cloud: str | sky_clouds.Cloud,
                            show_details: bool,
                            ctx2text: dict[str, str] | None = None) -> str:
    if isinstance(cloud, str):
        cloud_type = registry.CLOUD_REGISTRY.from_str(cloud)
        assert cloud_type is not None
    else:
        cloud_type = cloud
    if isinstance(cloud_type, sky_clouds.SSH):
        # Get the cluster names by reading from the node pools file
        contexts = sky_clouds.SSH.get_ssh_node_pool_contexts()
    elif isinstance(cloud_type, sky_clouds.Slurm):
        # Get the cluster names from SLURM config
        contexts = sky_clouds.Slurm.existing_allowed_clusters()
    else:
        assert isinstance(cloud_type, sky_clouds.Kubernetes)
        contexts = sky_clouds.Kubernetes.existing_allowed_contexts()

    filtered_contexts = []
    for context in contexts:
        if not show_details:
            # Skip
            if (ctx2text is None or context not in ctx2text or
                    'disabled' in ctx2text[context]):
                continue
        filtered_contexts.append(context)

    if not filtered_contexts:
        return ''

    def _red_color(str_to_format: str) -> str:
        return (f'{colorama.Fore.LIGHTRED_EX}'
                f'{str_to_format}'
                f'{colorama.Style.RESET_ALL}')

    def _dim_color(str_to_format: str) -> str:
        return (f'{colorama.Style.DIM}'
                f'{str_to_format}'
                f'{colorama.Style.RESET_ALL}')

    # For SSH, determine which contexts are disabled due to allowed_node_pools
    disabled_due_to_allowed_node_pools = set()
    if isinstance(cloud_type, sky_clouds.SSH):
        # Get all node pool contexts from file
        all_node_pool_contexts = sky_clouds.SSH.get_ssh_node_pool_contexts()
        # Get allowed contexts (after filtering)
        allowed_contexts = sky_clouds.SSH.existing_allowed_contexts()
        # Contexts that exist in file but not in allowed list are disabled
        # due to allowed_node_pools configuration
        disabled_due_to_allowed_node_pools = (set(all_node_pool_contexts) -
                                              set(allowed_contexts))

    # Format the context info with consistent styling
    contexts_formatted = []
    for i, context in enumerate(filtered_contexts):
        if isinstance(cloud_type, sky_clouds.SSH):
            # TODO: This is a hack to remove the 'ssh-' prefix from the
            # context name. Once we have a separate kubeconfig for SSH,
            # this will not be required.
            cleaned_context = common_utils.removeprefix(context, 'ssh-')
        else:
            cleaned_context = context
        symbol = (ux_utils.INDENT_LAST_SYMBOL if i == len(filtered_contexts) -
                  1 else ux_utils.INDENT_SYMBOL)
        text_suffix = ''
        if show_details:
            if ctx2text is not None:
                if context in ctx2text:
                    text_suffix = f': {ctx2text[context]}'
                elif (isinstance(cloud_type, sky_clouds.SSH) and
                      context in disabled_due_to_allowed_node_pools):
                    # Context is disabled due to allowed_node_pools config
                    text_suffix = (': ' + _red_color('disabled. ') +
                                   _dim_color('Reason: Not included in '
                                              'allowed_node_pools '
                                              'configuration.'))
                else:
                    # Default case - not set up
                    text_suffix = (': ' + _red_color('disabled. ') + _dim_color(
                        'Reason: Not set up. Use '
                        '`sky ssh up --infra '
                        f'{common_utils.removeprefix(context, "ssh-")}` '
                        'to set up.'))
        contexts_formatted.append(
            f'\n    {symbol}{cleaned_context}{text_suffix}')
    if isinstance(cloud_type, sky_clouds.SSH):
        identity_str = 'SSH Node Pools'
    elif isinstance(cloud_type, sky_clouds.Slurm):
        identity_str = 'Allowed clusters'
    else:
        identity_str = 'Allowed contexts'
    return f'\n    {identity_str}:{"".join(contexts_formatted)}'


def _format_enabled_cloud(cloud_name: str,
                          capabilities: list[sky_cloud.CloudCapability],
                          ctx2text: dict[str, str] | None = None) -> str:
    """Format the summary of enabled cloud and its enabled capabilities.

    Args:
        cloud_name: The name of the cloud.
        capabilities: The capabilities of the cloud.

    Returns:
        A string of the formatted cloud and capabilities.
    """
    cloud_and_capabilities = f'{cloud_name} [{", ".join(capabilities)}]'
    title = _green_color(cloud_and_capabilities)

    if cloud_name in [
            repr(sky_clouds.Kubernetes()),
            repr(sky_clouds.SSH()),
            repr(sky_clouds.Slurm())
    ]:
        return (f'{title}' + _format_context_details(
            cloud_name, show_details=False, ctx2text=ctx2text))
    return _green_color(cloud_and_capabilities)


def _summary_message(
    enabled_clouds: dict[str, list[sky_cloud.CloudCapability]],
    cloud2ctx2text: dict[str, dict[str, str]],
    current_workspace_name: str,
    hide_workspace_str: bool,
    disallowed_cloud_names: list[str],
) -> str:
    if not enabled_clouds:
        enabled_clouds_str = '\n  No infra to check/enabled.'
    else:
        enabled_clouds_str = '\n  ' + '\n  '.join([
            _format_enabled_cloud(cloud, capabilities,
                                  cloud2ctx2text.get(cloud, None))
            for cloud, capabilities in sorted(enabled_clouds.items(),
                                              key=lambda item: item[0])
        ])

    workspace_str = f' for workspace: {current_workspace_name!r}'
    if hide_workspace_str:
        workspace_str = ''

    disallowed_clouds_hint = ''
    if disallowed_cloud_names:
        disable_for_workspace_hint = (
            f' or disabled for this workspace {current_workspace_name!r}')
        if hide_workspace_str:
            disable_for_workspace_hint = ''
        disallowed_clouds_hint = (
            '\nNote: The following clouds were disabled because they were not '
            'included in allowed_clouds in ~/.sky/config.yaml'
            f'{disable_for_workspace_hint}: '
            f'{", ".join([c for c in disallowed_cloud_names])}')

    return (f'\n{colorama.Fore.GREEN}{PARTY_POPPER_EMOJI} '
            f'Enabled infra{workspace_str} '
            f'{PARTY_POPPER_EMOJI}'
            f'{colorama.Style.RESET_ALL}{enabled_clouds_str}'
            f'{disallowed_clouds_hint}')
