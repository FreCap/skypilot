"""Credential checks: check cloud credentials and enable clouds."""
import collections
from collections.abc import Iterable
import os
import re
import traceback
from types import ModuleType
from typing import Any

import click
import colorama

from sky import check_presentation as _check_presentation
from sky import clouds as sky_clouds
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import cloudflare
from sky.adaptors import coreweave
from sky.adaptors import huggingface
from sky.adaptors import vastdata
from sky.clouds import cloud as sky_cloud
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import registry
from sky.utils import rich_utils
from sky.utils import subprocess_utils
from sky.utils import ux_utils

CHECK_MARK_EMOJI = '\U00002714'  # Heavy check mark unicode
PARTY_POPPER_EMOJI = _check_presentation.PARTY_POPPER_EMOJI
STORAGE_ONLY_CLOUDS = (cloudflare.NAME, coreweave.NAME, vastdata.NAME,
                       huggingface.NAME)

# Preserve the historical sky.check import and monkeypatch surface while the
# implementation is owned by check_presentation.
# pylint: disable=protected-access
_format_context_details = _check_presentation._format_context_details
_format_enabled_cloud = _check_presentation._format_enabled_cloud
_green_color = _check_presentation._green_color
_print_checked_cloud = _check_presentation._print_checked_cloud
_summary_message = _check_presentation._summary_message
# pylint: enable=protected-access

logger = sky_logging.init_logger(__name__)

_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')


def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub('', s) if isinstance(s, str) else ''


def _build_check_results(
    cloud2ctx2text: dict[str, dict[str, str]],
    check_results_dict: dict[Any, list[tuple]],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Construct the persistable {cloud: {ctx: {enabled, reason}}} dict.

    Combines two sources because cloud2ctx2text is only populated for
    per-context (k8s/SSH) checks; non-k8s clouds' string reasons live in
    check_results_dict.
    """
    out: dict[str, dict[str, dict[str, Any]]] = {}

    # Per-context entries (k8s, SSH).
    for cloud_repr, ctx2text in cloud2ctx2text.items():
        out.setdefault(cloud_repr, {})
        for ctx, text in ctx2text.items():
            stripped = _strip_ansi(text)
            # Match a positive 'enabled.' prefix so that exception messages
            # (which start with neither 'enabled.' nor 'disabled.') are
            # correctly classified as not-enabled. The trailing period is
            # intentional: it matches 'enabled.' and 'enabled. Reason: ...'
            # but not hypothetical future variants like 'enabled but degraded'.
            out[cloud_repr][ctx] = {
                'enabled': stripped.lower().startswith('enabled.'),
                'reason': stripped,
            }

    # Non-k8s clouds: aggregate string reasons across capabilities. A cloud
    # isn't fully usable if ANY capability fails, so prefer a failure reason
    # when present; only show a success reason if every capability succeeded.
    for cloud_tuple, result_list in check_results_dict.items():
        cloud_repr = cloud_tuple[0]
        if cloud_repr in out:
            # k8s / SSH already populated above; don't overwrite.
            continue
        string_reasons = [(ok, reason)
                          for _, ok, reason in result_list
                          if not isinstance(reason, dict)]
        if not string_reasons:
            continue
        all_ok = all(ok for ok, _ in string_reasons)
        # Prefer the first failure reason if any; otherwise the first
        # success reason.
        failure_reasons = [r for ok, r in string_reasons if not ok]
        chosen_reason = (failure_reasons[0]
                         if failure_reasons else string_reasons[0][1])
        out.setdefault(cloud_repr, {})
        out[cloud_repr][''] = {
            'enabled': all_ok,
            'reason': _strip_ansi(chosen_reason or ''),
        }

    return out


def _get_workspace_allowed_clouds(workspace: str) -> list[str]:
    # Use allowed_clouds from config if it exists, otherwise check all
    # clouds. Also validate names with get_cloud_tuple.
    config_allowed_cloud_names = skypilot_config.get_nested(
        ('allowed_clouds',),
        [repr(c) for c in registry.CLOUD_REGISTRY.values()] +
        list(STORAGE_ONLY_CLOUDS))
    # filter out the clouds that are disabled in the workspace config
    workspace_disabled_clouds = []
    for cloud in config_allowed_cloud_names:
        cloud_config = skypilot_config.get_workspace_cloud(cloud.lower(),
                                                           workspace=workspace)
        cloud_disabled = cloud_config.get('disabled', False)
        if cloud_disabled:
            workspace_disabled_clouds.append(cloud.lower())

    config_allowed_cloud_names = [
        c for c in config_allowed_cloud_names
        if c.lower() not in workspace_disabled_clouds
    ]
    return config_allowed_cloud_names


def get_workspace_allowed_clouds(
    workspace: str | None = None,
    capability: sky_cloud.CloudCapability | None = None,
) -> list[str]:
    """Return clouds permitted by config for one workspace and capability.

    Unlike ``get_cached_enabled_clouds_or_refresh()``, this is a policy-only
    lookup: it does not probe credentials or provider control planes.  Runtime
    consumers can therefore reject candidates disabled by ``allowed_clouds`` or
    the workspace's ``disabled``/``capabilities`` policy without adding
    provider work to a hot or boot-critical path. If ``capability`` is omitted,
    this preserves the capability-agnostic allowed-cloud contract.
    """
    if workspace is None:
        workspace = skypilot_config.get_active_workspace()
    allowed_clouds = _get_workspace_allowed_clouds(workspace)
    if capability is None:
        return allowed_clouds
    return [
        cloud for cloud in allowed_clouds
        if (configured_capabilities := _get_workspace_cloud_capabilities(
            workspace, cloud)) is None or capability in configured_capabilities
    ]


def _get_workspace_cloud_capabilities(
        workspace: str, cloud: str) -> list[sky_cloud.CloudCapability] | None:
    """Get the capabilities for a cloud in a workspace.

    Returns:
        A list of capabilities for the cloud in the workspace.
        None if the capabilities are not explicitly specified
        in the workspace or global config.
        Returned value of None does not mean the cloud is disabled.
    """
    cloud_config = skypilot_config.get_workspace_cloud(cloud,
                                                       workspace=workspace)
    cloud_capabilities = cloud_config.get('capabilities', None)
    if cloud_capabilities is None:
        # get the capabilities from the global config
        cloud_capabilities = skypilot_config.get_nested(
            (cloud.lower(), 'capabilities'), default_value=None)
    if cloud_capabilities is not None:
        return [
            sky_cloud.CloudCapability(capability.lower())
            for capability in cloud_capabilities
        ]
    return None


def check_capabilities(
    quiet: bool = False,
    verbose: bool = False,
    clouds: Iterable[str] | None = None,
    capabilities: list[sky_cloud.CloudCapability] | None = None,
    workspace: str | None = None,
) -> dict[str, dict[str, list[sky_cloud.CloudCapability]]]:
    # pylint: disable=import-outside-toplevel
    from sky.workspaces import core

    echo = (lambda *_args, **_kwargs: None
           ) if quiet else lambda *args, **kwargs: click.echo(
               *args, **kwargs, color=True)
    all_workspaces_results: dict[str,
                                 dict[str,
                                      list[sky_cloud.CloudCapability]]] = {}
    available_workspaces = list(core.get_accessible_workspace_names())
    hide_workspace_str = (available_workspaces == [
        constants.SKYPILOT_DEFAULT_WORKSPACE
    ])
    initial_hint = 'Checking credentials to enable infra for SkyPilot.'
    if len(available_workspaces) > 1:
        initial_hint = (f'Checking credentials to enable infra for SkyPilot '
                        f'(Workspaces: {", ".join(available_workspaces)}).')
    echo(initial_hint)
    if capabilities is None:
        capabilities = sky_cloud.ALL_CAPABILITIES
    assert capabilities is not None

    def get_all_clouds() -> tuple[str, ...]:
        return tuple([repr(c) for c in registry.CLOUD_REGISTRY.values()] +
                     list(STORAGE_ONLY_CLOUDS))

    def _execute_check_logic_for_workspace(
        current_workspace_name: str,
        hide_per_cloud_details: bool,
        hide_workspace_str: bool,
    ) -> dict[str, list[sky_cloud.CloudCapability]]:
        nonlocal echo, verbose, clouds, quiet

        enabled_clouds: dict[str, list[sky_cloud.CloudCapability]] = {}
        disabled_clouds: dict[str, list[sky_cloud.CloudCapability]] = {}

        def check_one_cloud_one_capability(
            payload: tuple[tuple[str, sky_clouds.Cloud | ModuleType],
                           sky_cloud.CloudCapability, bool]
        ) -> tuple[sky_cloud.CloudCapability, bool, str | dict[str, str] |
                   None] | None:
            cloud_tuple, capability, allowed = payload
            if not allowed:
                return (capability, False, f'{cloud_tuple[0]} is not included '
                        'in allowed_clouds in ~/.sky/config.yaml')
            with skypilot_config.local_active_workspace_ctx(
                    current_workspace_name):
                # Have to override again for specific thread, as the
                # local_active_workspace_ctx is thread-local.
                _, cloud = cloud_tuple
                try:
                    ok, reason = cloud.check_credentials(capability)
                except exceptions.NotSupportedError:
                    return None
                except Exception:  # pylint: disable=broad-except
                    ok, reason = False, traceback.format_exc()
                if not isinstance(reason, dict):
                    reason = reason.strip() if reason else None
                return (capability, ok, reason)

        def get_cloud_tuple(
                cloud_name: str) -> tuple[str, sky_clouds.Cloud | ModuleType]:
            # Validates cloud_name and returns a tuple of the cloud's name and
            # the cloud object. Includes special handling for storage-only
            # providers (Cloudflare, CoreWeave, VastData, HuggingFace).
            if cloud_name.lower().startswith('cloudflare'):
                return cloudflare.NAME, cloudflare
            elif cloud_name.lower().startswith('coreweave'):
                return coreweave.NAME, coreweave
            elif cloud_name.lower().startswith('vastdata'):
                return vastdata.NAME, vastdata
            elif cloud_name.lower().startswith('huggingface'):
                return huggingface.NAME, huggingface
            else:
                try:
                    cloud_obj = registry.CLOUD_REGISTRY.from_str(cloud_name)
                except ValueError:
                    all_clouds = sorted(c.lower() for c in get_all_clouds())
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(f'Cloud {cloud_name!r} is not a valid '
                                         f'cloud among {all_clouds}') from None
                assert cloud_obj is not None, f'Cloud {cloud_name!r} not found'
                return repr(cloud_obj), cloud_obj

        if clouds is not None:
            cloud_list = clouds
            check_explicit = True
        else:
            cloud_list = get_all_clouds()
            check_explicit = False

        clouds_to_check = [get_cloud_tuple(c) for c in cloud_list]

        # Use allowed_clouds from config if it exists, otherwise check all
        # clouds. Also validate names with get_cloud_tuple.
        config_allowed_cloud_names = sorted([
            get_cloud_tuple(c)[0] for c in skypilot_config.get_nested((
                'allowed_clouds',), get_all_clouds())
        ])

        # filter out the clouds that are disabled in the workspace config
        workspace_disabled_clouds = []
        workspace_cloud_capabilities: dict[
            str, list[sky_cloud.CloudCapability]] = {}
        for cloud in config_allowed_cloud_names:
            cloud_config = skypilot_config.get_workspace_cloud(
                cloud, workspace=current_workspace_name)
            cloud_disabled = cloud_config.get('disabled', False)
            if cloud_disabled:
                workspace_disabled_clouds.append(cloud)
            else:
                specified_capabilities = _get_workspace_cloud_capabilities(
                    current_workspace_name, cloud)
                if specified_capabilities is not None:
                    # filter the capabilities to only the ones passed
                    # in as argument to this function
                    workspace_cloud_capabilities[cloud] = [
                        enabled_capability
                        for enabled_capability in specified_capabilities
                        if enabled_capability in capabilities
                    ]
                    # mark capabilities that are not enabled
                    # in the workspace config as disabled
                    for capability in capabilities:
                        if capability not in workspace_cloud_capabilities[
                                cloud]:
                            disabled_clouds.setdefault(cloud,
                                                       []).append(capability)

        config_allowed_cloud_names = [
            c for c in config_allowed_cloud_names
            if c not in workspace_disabled_clouds
        ]
        global_user_state.set_allowed_clouds(
            [c for c in config_allowed_cloud_names], current_workspace_name)

        # Use disallowed_cloud_names for logging the clouds that will be
        # disabled because they are not included in allowed_clouds in
        # config.yaml.
        disallowed_cloud_names = [
            c for c in get_all_clouds() if c not in config_allowed_cloud_names
        ]

        combinations = []
        for c in clouds_to_check:
            allowed = c[0] in config_allowed_cloud_names
            if allowed or check_explicit:
                for capability in workspace_cloud_capabilities.get(
                        c[0], capabilities):
                    combinations.append((c, capability, allowed))

        cloud2ctx2text: dict[str, dict[str, str]] = {}

        workspace_str = f' for workspace: {current_workspace_name!r}'
        if hide_workspace_str:
            workspace_str = ''
        with rich_utils.safe_status(
                ux_utils.spinner_message(
                    f'Checking infra choices{workspace_str}...')):
            check_results = subprocess_utils.run_in_parallel(
                check_one_cloud_one_capability, combinations)

        check_results_dict: dict[tuple[str, sky_clouds.Cloud | ModuleType],
                                 list[tuple[sky_cloud.CloudCapability, bool,
                                            str | dict[str, str] | None]]] = (
                                                collections.defaultdict(list))
        for combination, check_result in zip(combinations, check_results):
            if check_result is None:
                continue
            capability, ok, ctx2text = check_result
            cloud_tuple, _, _ = combination
            cloud_repr = cloud_tuple[0]
            if isinstance(ctx2text, dict):
                cloud2ctx2text[cloud_repr] = ctx2text
            if ok:
                enabled_clouds.setdefault(cloud_repr, []).append(capability)
            else:
                disabled_clouds.setdefault(cloud_repr, []).append(capability)
            check_results_dict[cloud_tuple].append(check_result)

        if not hide_per_cloud_details:
            for cloud_tuple, check_result_list in sorted(
                    check_results_dict.items(), key=lambda item: item[0][0]):
                _print_checked_cloud(echo, verbose, cloud_tuple,
                                     check_result_list,
                                     cloud2ctx2text.get(cloud_tuple[0], {}))

        # Determine the set of enabled clouds: (previously enabled clouds +
        # newly enabled clouds - newly disabled clouds) intersected with
        # config_allowed_clouds, if specified in config.yaml.
        # This means that if a cloud is already enabled and is not included in
        # allowed_clouds in config.yaml, it will be disabled.
        all_enabled_clouds: set[str] = set()
        for capability in capabilities:
            # Cloudflare, CoreWeave, VastData, and HuggingFace are not real
            # clouds in registry.CLOUD_REGISTRY, and should not be inserted
            # into the DB
            # (otherwise `sky launch` and other code would error out when it's
            # trying to look it up in the registry).
            enabled_clouds_set = {
                cloud for cloud, capabilities in enabled_clouds.items()
                if capability in capabilities and not any(
                    cloud.startswith(s) for s in STORAGE_ONLY_CLOUDS)
            }
            disabled_clouds_set = {
                cloud for cloud, capabilities in disabled_clouds.items()
                if capability in capabilities and not any(
                    cloud.startswith(s) for s in STORAGE_ONLY_CLOUDS)
            }
            config_allowed_clouds_set = {
                cloud for cloud in config_allowed_cloud_names
                if not any(cloud.startswith(s) for s in STORAGE_ONLY_CLOUDS)
            }
            previously_enabled_clouds_set = {
                repr(cloud)
                for cloud in global_user_state.get_cached_enabled_clouds(
                    capability, current_workspace_name)
            }
            enabled_clouds_for_capability = (config_allowed_clouds_set & (
                (previously_enabled_clouds_set | enabled_clouds_set) -
                disabled_clouds_set))

            global_user_state.set_enabled_clouds(
                list(enabled_clouds_for_capability), capability,
                current_workspace_name)
            all_enabled_clouds = all_enabled_clouds.union(
                enabled_clouds_for_capability)

        # Persist the per-(cloud, context) status so downstream consumers
        # (dashboard endpoints, plugins, etc.) can read it without
        # re-running cloud probes. Full-workspace runs replace the row;
        # scoped runs (clouds is not None) merge at cloud granularity.
        # Wrapped in try/except: this row is a cache, and the
        # source-of-truth enabled_clouds_<workspace>_<cap> rows have
        # already been written above. A transient DB failure or
        # unsupported dialect must not fail the user-visible
        # `sky check` command.
        try:
            results_to_persist = _build_check_results(cloud2ctx2text,
                                                      check_results_dict)
            global_user_state.set_check_results(
                results_to_persist,
                current_workspace_name,
                is_full_workspace_run=(clouds is None),
            )
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to persist check_results for workspace '
                           f'{current_workspace_name!r}: {e}')

        echo(
            _summary_message(enabled_clouds, cloud2ctx2text,
                             current_workspace_name, hide_workspace_str,
                             disallowed_cloud_names))

        return enabled_clouds

    # --- Main check_capabilities logic ---

    if workspace is not None:
        # Check only the specified workspace
        if workspace not in available_workspaces:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    f'Workspace {workspace!r} not found in SkyPilot '
                    'configuration. '
                    f'Available workspaces: {", ".join(available_workspaces)}')

        # Always show details for single specified check (if verbose)
        hide_per_cloud_details_flag = False
        with skypilot_config.local_active_workspace_ctx(workspace):
            enabled_ws_clouds = _execute_check_logic_for_workspace(
                workspace, hide_per_cloud_details_flag, hide_workspace_str)
            all_workspaces_results[workspace] = enabled_ws_clouds
    else:
        # Check all workspaces
        workspaces_to_check = available_workspaces

        hide_per_cloud_details_flag = (not verbose and clouds is None and
                                       len(workspaces_to_check) > 1)

        for ws_name in workspaces_to_check:
            if not hide_workspace_str:
                echo(f'\nChecking enabled infra for workspace: {ws_name!r}')
            with skypilot_config.local_active_workspace_ctx(ws_name):
                enabled_ws_clouds = _execute_check_logic_for_workspace(
                    ws_name, hide_per_cloud_details_flag, hide_workspace_str)
                all_workspaces_results[ws_name] = enabled_ws_clouds

    # Global "To enable a cloud..." message, printed once if relevant
    if not quiet:
        echo(
            click.style(
                '\nTo enable a cloud, follow the hints above and rerun: ',
                dim=True) + click.style('sky check', bold=True) + '\n' +
            click.style(
                'If any problems remain, refer to detailed docs at: '
                'https://docs.skypilot.co/en/latest/getting-started/installation.html',  # pylint: disable=line-too-long
                dim=True))

    return all_workspaces_results


def check_capability(
    capability: sky_cloud.CloudCapability,
    quiet: bool = False,
    verbose: bool = False,
    clouds: Iterable[str] | None = None,
    workspace: str | None = None,
) -> dict[str, list[str]]:
    clouds_with_capability = collections.defaultdict(list)
    workspace_enabled_clouds = check_capabilities(quiet, verbose, clouds,
                                                  [capability], workspace)
    for workspace, enabled_clouds in workspace_enabled_clouds.items():
        for cloud, capabilities in enabled_clouds.items():
            if capability in capabilities:
                clouds_with_capability[workspace].append(cloud)
    return clouds_with_capability


def check(
    quiet: bool = False,
    verbose: bool = False,
    clouds: Iterable[str] | None = None,
    workspace: str | None = None,
) -> dict[str, dict[str, list[str]]]:
    if workspace is not None:
        # Import here to avoid circular import:
        # pylint: disable=import-outside-toplevel
        from sky.workspaces import core as workspaces_core
        workspaces_core.check_workspace_permission(
            common_utils.get_current_user(), workspace)
    capabilities_result = check_capabilities(quiet, verbose, clouds,
                                             sky_cloud.ALL_CAPABILITIES,
                                             workspace)
    # Convert CloudCapability enums to strings for JSON serialization.
    result: dict[str, dict[str, list[str]]] = {}
    for ws_name, clouds_with_caps in capabilities_result.items():
        result[ws_name] = {
            cloud: [cap.value for cap in caps]
            for cloud, caps in clouds_with_caps.items()
        }
    return result


def get_cached_enabled_clouds_or_refresh(
        capability: sky_cloud.CloudCapability,
        raise_if_no_cloud_access: bool = False) -> list[sky_clouds.Cloud]:
    """Returns cached enabled clouds and if no cloud is enabled, refresh.

    This function will perform a refresh if no public cloud is enabled.

    Args:
        raise_if_no_cloud_access: if True, raise an exception if no public
            cloud is enabled.

    Raises:
        exceptions.NoCloudAccessError: if no public cloud is enabled and
            raise_if_no_cloud_access is set to True.
    """
    active_workspace = skypilot_config.get_active_workspace()
    allowed_clouds_changed = False
    cached_allowed_clouds = global_user_state.get_allowed_clouds(
        active_workspace)
    skypilot_config_allowed_clouds = _get_workspace_allowed_clouds(
        active_workspace)
    if sorted(cached_allowed_clouds) != sorted(skypilot_config_allowed_clouds):
        allowed_clouds_changed = True

    cached_enabled_clouds = global_user_state.get_cached_enabled_clouds(
        capability, active_workspace)
    if not cached_enabled_clouds or allowed_clouds_changed:
        try:
            check_capability(capability, quiet=True, workspace=active_workspace)
            if allowed_clouds_changed:
                global_user_state.set_allowed_clouds(
                    skypilot_config_allowed_clouds, active_workspace)
        except SystemExit:
            # If no cloud is enabled, check() will raise SystemExit.
            # Here we catch it and raise the exception later only if
            # raise_if_no_cloud_access is set to True.
            pass
        cached_enabled_clouds = global_user_state.get_cached_enabled_clouds(
            capability, active_workspace)
    if raise_if_no_cloud_access and not cached_enabled_clouds:
        with ux_utils.print_exception_no_traceback():
            raise exceptions.NoCloudAccessError(
                'Cloud access is not set up. Run: '
                f'{colorama.Style.BRIGHT}sky check{colorama.Style.RESET_ALL}')
    return cached_enabled_clouds


def get_cloud_credential_file_mounts(
        excluded_clouds: Iterable[sky_clouds.Cloud] | None) -> dict[str, str]:
    """Returns the files necessary to access all clouds.

    Returns a dictionary that will be added to a task's file mounts
    and a list of patterns that will be excluded (used as rsync_exclude).
    """
    # Uploading credentials for all clouds instead of only sky check
    # enabled clouds because users may have partial credentials for some
    # clouds to access their specific resources (e.g. cloud storage) but
    # not have the complete credentials to pass sky check.
    clouds = registry.CLOUD_REGISTRY.values()
    file_mounts = {}
    for cloud in clouds:
        if (excluded_clouds is not None and
                sky_clouds.cloud_in_iterable(cloud, excluded_clouds)):
            continue
        cloud_file_mounts = cloud.get_credential_file_mounts()
        for remote_path, local_path in cloud_file_mounts.items():
            if os.path.exists(os.path.expanduser(local_path)):
                file_mounts[remote_path] = os.path.realpath(
                    os.path.expanduser(local_path))
    # Currently, get_cached_enabled_clouds_or_refresh() does not support
    # storage-only clouds as only clouds with computing instances are
    # marked as enabled by skypilot.
    # TODO (kyuds): recognize storage-only clouds as clouds.
    r2_is_enabled, _ = cloudflare.check_storage_credentials()
    if r2_is_enabled:
        r2_credential_mounts = cloudflare.get_credential_file_mounts()
        file_mounts.update(r2_credential_mounts)

    coreweave_is_enabled, _ = coreweave.check_storage_credentials()
    if coreweave_is_enabled:
        coreweave_credential_mounts = coreweave.get_credential_file_mounts()
        file_mounts.update(coreweave_credential_mounts)

    vastdata_is_enabled, _ = vastdata.check_storage_credentials()
    if vastdata_is_enabled:
        vastdata_credential_mounts = vastdata.get_credential_file_mounts()
        file_mounts.update(vastdata_credential_mounts)

    hf_is_enabled, _ = huggingface.check_storage_credentials()
    if hf_is_enabled:
        hf_credential_mounts = huggingface.get_credential_file_mounts()
        file_mounts.update(hf_credential_mounts)
    return file_mounts
