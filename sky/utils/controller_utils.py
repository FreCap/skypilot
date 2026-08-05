"""Util constants/functions for SkyPilot Controllers."""
from collections.abc import Callable
from collections.abc import Iterable
import copy
import os
import pathlib
import tempfile
import typing
from typing import Any

import colorama

from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky import resources
from sky import sky_logging
from sky import skypilot_config
from sky.jobs import constants as managed_job_constants
from sky.jobs import state as managed_job_state
from sky.serve import constants as serve_constants
from sky.serve import serve_state
from sky.server import config as server_config
from sky.server import constants as server_constants
from sky.server import plugin_utils
from sky.server import plugins
from sky.skylet import constants
from sky.skylet import log_lib
from sky.usage import constants as usage_constants
from sky.utils import annotations
from sky.utils import common_utils
from sky.utils import config_utils
from sky.utils import controller_dependency_installation
from sky.utils import controller_mount_translation
from sky.utils import controller_types
from sky.utils import env_options
from sky.utils import registry
from sky.utils import ux_utils
from sky.utils import yaml_utils
from sky.utils.controller_types import _ControllerSpec

if typing.TYPE_CHECKING:
    import psutil

    from sky import task as task_lib
    from sky.backends import cloud_vm_ray_backend
else:
    from sky.adaptors import common as adaptors_common
    psutil = adaptors_common.LazyImport('psutil')

logger = sky_logging.init_logger(__name__)

# Message thrown when APIs sky.jobs.launch(), sky.serve.up() received an invalid
# controller resources spec.
CONTROLLER_RESOURCES_NOT_VALID_MESSAGE = (
    '{controller_type} controller resources is not valid, please check '
    '~/.sky/config.yaml file and make sure '
    '{controller_type}.controller.resources is a valid resources spec. '
    'Details:\n  {err}')

# The suffix for local skypilot config path for a job/service in file mounts
# that tells the controller logic to update the config with specific settings,
# e.g., removing the ssh_proxy_command when a job/service is launched in a same
# cloud as controller.
_LOCAL_SKYPILOT_CONFIG_PATH_SUFFIX = (
    '__skypilot:local_skypilot_config_path.yaml')

# Preserve established imports and introspection identities.
Controllers = controller_types.Controllers
get_controller_for_pool = controller_types.get_controller_for_pool
high_availability_specified = controller_types.high_availability_specified
_ControllerSpec.__module__ = __name__
Controllers.__module__ = __name__
get_controller_for_pool.__module__ = __name__
high_availability_specified.__module__ = __name__

# The private name is the established controller_utils facade contract.
# pylint: disable=protected-access
_get_cloud_dependencies_installation_commands = (
    controller_dependency_installation.
    _get_cloud_dependencies_installation_commands)
# pylint: enable=protected-access
_get_cloud_dependencies_installation_commands.__module__ = __name__
# Preserve established monkeypatch paths for provider command generation.
sky_check = controller_dependency_installation.sky_check
sky_cloud = controller_dependency_installation.sky_cloud
gcp = controller_dependency_installation.gcp
kubernetes_constants = (controller_dependency_installation.kubernetes_constants)
dependencies = controller_dependency_installation.dependencies
storage_lib = controller_dependency_installation.storage_lib


def check_cluster_name_not_controller(cluster_name: str | None,
                                      operation_str: str | None = None) -> None:
    """Errors out if the cluster name is a controller name.

    Raises:
      sky.exceptions.NotSupportedError: if the cluster name is a controller
        name, raise with an error message explaining 'operation_str' is not
        allowed.

    Returns:
      None, if the cluster name is not a controller name.
    """
    controller = Controllers.from_name(cluster_name, expect_exact_match=False)
    if controller is not None:
        msg = controller.value.check_cluster_name_hint
        if operation_str is not None:
            msg += f' {operation_str} is not allowed.'
        with ux_utils.print_exception_no_traceback():
            raise exceptions.NotSupportedError(msg)


# Internal only:
def download_and_stream_job_log(
        backend: 'cloud_vm_ray_backend.CloudVmRayBackend',
        handle: 'cloud_vm_ray_backend.CloudVmRayResourceHandle',
        local_dir: str,
        job_ids: list[str] | None = None,
        on_downloaded: Callable[[str], None] | None = None) -> str | None:
    """Downloads and streams the latest job log.

    This function is only used by jobs controller and sky serve controller.

    Args:
        on_downloaded: Optional callback invoked with the local log path as
            soon as the log has been synced down, BEFORE the (potentially
            slow) re-streaming of the log into the controller log. The jobs
            controller uses this to persist ``local_log_file`` immediately so
            the dashboard can serve the job's logs without waiting for the
            full re-stream to finish.

    If the log cannot be fetched for any reason, return None.
    """
    os.makedirs(os.path.expanduser(local_dir), exist_ok=True)
    log_file = None
    try:
        log_dirs = backend.sync_down_logs(
            handle,
            # Download the log of the latest job.
            # The job_id for the managed job running on the cluster is not
            # necessarily 1, as it is possible that the worker node in a
            # multi-node cluster is preempted, and we recover the managed job
            # on the existing cluster, which leads to a larger job_id. Those
            # job_ids all represent the same logical managed job.
            job_ids=job_ids,
            local_dir=local_dir)
    except Exception as e:  # pylint: disable=broad-except
        # We want to avoid crashing the controller. sync_down_logs() is pretty
        # complicated and could crash in various places (creating remote
        # runners, executing remote code, decoding the payload, etc.). So, we
        # use a broad except and just return None.
        logger.info(
            f'Failed to download the logs: '
            f'{common_utils.format_exception(e)}',
            exc_info=True)
        return None

    if not log_dirs:
        logger.error('Failed to find the logs for the user program.')
        return None

    log_dir = list(log_dirs.values())[0]
    log_file = os.path.expanduser(os.path.join(log_dir, 'run.log'))

    # The log is now on local disk. Notify the caller immediately so it can
    # persist the path (e.g. local_log_file) before the slow re-stream below,
    # which can take minutes for multi-GB logs and would otherwise block the
    # dashboard from serving the logs.
    if on_downloaded is not None:
        on_downloaded(log_file)

    # Print the logs to the console.
    # TODO(zhwu): refactor this into log_utils, along with the refactoring for
    # the log_lib.tail_logs.
    try:
        # newline='\n' so we split lines ONLY on '\n'. The default
        # universal-newline mode treats every '\r' as a line boundary, which
        # for carriage-return progress output (e.g. `aws s3 cp`'s in-place
        # "Completed X GiB ..." updates) explodes a multi-GB log into millions
        # of "lines" -- making this loop O(carriage-returns) (minutes for a
        # ~160MB log) and bloating the controller log accordingly. Splitting
        # only on '\n' keeps it O(real lines). We also drop the per-line
        # flush: stdout is block-buffered (~8KB), so a hard crash loses at
        # most the last buffer, not the whole copy -- and the authoritative
        # copy is the synced run.log on disk anyway. errors='replace' so a
        # stray invalid-UTF-8 byte in the user log can't abort the copy
        # mid-stream (matches log_lib's decode handling).
        with open(log_file, encoding='utf-8', newline='\n',
                  errors='replace') as f:
            # Stream the logs to the console without reading the whole file into
            # memory.
            start_streaming = False
            for line in f:
                if log_lib.LOG_FILE_START_STREAMING_AT in line:
                    start_streaming = True
                if start_streaming:
                    print(line, end='')
        # Flush once after the full copy instead of once per line.
        print(end='', flush=True)
    except FileNotFoundError:
        logger.error('Failed to find the logs for the user '
                     f'program at {log_file}.')
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            f'Failed to stream the logs for the user program at '
            f'{log_file}: {common_utils.format_exception(e)}',
            exc_info=True)

    return log_file


def shared_controller_vars_to_fill(
        controller: Controllers, remote_user_config_path: str,
        local_user_config: dict[str, Any]) -> dict[str, str]:
    local_user_config = controller_config_snapshot(local_user_config)
    if not local_user_config:
        local_user_config_path = None
    else:
        with tempfile.NamedTemporaryFile(
                delete=False,
                suffix=_LOCAL_SKYPILOT_CONFIG_PATH_SUFFIX) as temp_file:
            yaml_utils.dump_yaml(temp_file.name, dict(**local_user_config))
        local_user_config_path = temp_file.name

    vars_to_fill: dict[str, Any] = controller_only_vars_to_fill(controller)
    vars_to_fill.update({
        'sky_activate_python_env': constants.ACTIVATE_SKY_REMOTE_PYTHON_ENV,
        'sky_python_cmd': constants.SKY_PYTHON_CMD,
        'local_user_config_path': local_user_config_path,
    })
    env_vars: dict[str, Any] = {
        env.env_key: str(int(env.get())) for env in env_options.Options
    }
    env_vars.update({
        # Make sure the clusters launched by the controller are marked as
        # launched with a remote API server if the controller is launched
        # with a remote API server.
        constants.USING_REMOTE_API_SERVER_ENV_VAR: str(
            common_utils.get_using_remote_api_server()),
    })
    # Only set the SKYPILOT_CONFIG env var when we actually file_mount a
    # config to the controller (i.e. local_user_config was non-empty so
    # local_user_config_path is a real tempfile that gets rsynced/SSH'd to
    # remote_user_config_path on the controller). Previously this gated on
    # `skypilot_config.loaded()` (API server's own config), which can be True
    # even when local_user_config is empty — pointing the controller's
    # SKYPILOT_CONFIG env to a file that was never created and crashing it
    # with FileNotFoundError on startup.
    if local_user_config_path is not None:
        env_vars[
            skypilot_config.ENV_VAR_SKYPILOT_CONFIG] = remote_user_config_path
    vars_to_fill['controller_envs'].update(env_vars)
    return vars_to_fill


def controller_config_snapshot(local_user_config: dict[str, Any],
                               workspace: str | None = None) -> dict[str, Any]:
    """Return the config a controller process is allowed to consume.

    Always a plain dict. The API server's own config is a
    ``config_utils.Config`` (a dict subclass), and yaml.safe_dump refuses to
    represent subclasses: the update path serializes this snapshot directly,
    so returning the subclass fails every `sky serve update` on the server
    with a RepresenterError. Only the top level needs coercion --
    ``Config.from_dict`` is ``cls(**config)``, so nested values are the plain
    dicts the YAML loader produced.
    """
    config = dict(copy.deepcopy(local_user_config))
    # These are API-side concerns and must not be re-applied by a controller.
    config.pop('admin_policy', None)
    config.pop('api_server', None)
    config.pop('db', None)
    # A Kubernetes-hosted controller commonly uses in-cluster auth and may not
    # have the API server's local kubeconfig contexts.
    # TODO(romilb): Retain this key when the controller is not on Kubernetes.
    config.pop('allowed_contexts', None)
    if workspace is None:
        active_workspace = config.get('active_workspace')
        if isinstance(active_workspace, str) and active_workspace:
            workspace = active_workspace
    workspaces = config.get('workspaces')
    if workspace is not None and isinstance(workspaces, dict):
        workspace_config = workspaces.get(workspace)
        if isinstance(workspace_config, dict):
            workspace_config.pop('allowed_users', None)
            workspace_config.pop('private', None)
        config['workspaces'] = ({
            workspace: workspace_config
        } if workspace_config is not None else {})
    return config


def controller_only_vars_to_fill(controller: Controllers) -> dict[str, str]:
    # Get plugins config and wheel file mounts/commands together to ensure
    # consistency between the uploaded wheel paths and installation commands.
    # Only upload plugins specified in remote_plugins.yaml - plugins in
    # plugins.yaml are intended for local API server use only.
    local_plugins_config_path = None
    plugin_wheel_file_mounts, plugins_wheel_install_commands = (
        plugin_utils.get_plugin_mounts_and_commands())
    if plugin_wheel_file_mounts and plugins_wheel_install_commands:
        local_plugins_config_path = (
            plugin_utils.get_filtered_plugins_config_path())
    vars_to_fill: dict[str, Any] = {
        'sky_activate_python_env': constants.ACTIVATE_SKY_REMOTE_PYTHON_ENV,
        'cloud_dependencies_installation_commands':
            _get_cloud_dependencies_installation_commands(controller),
        # Plugin-related template variables
        'local_plugins_config_path': local_plugins_config_path,
        'remote_plugins_config_path': plugins.REMOTE_PLUGINS_CONFIG_PATH,
        'plugin_wheel_file_mounts': plugin_wheel_file_mounts,
        'plugins_wheel_install_commands': plugins_wheel_install_commands,
    }
    env_vars: dict[str, Any] = {
        env.env_key: str(int(env.get())) for env in env_options.Options
    }
    env_vars.update({
        # Should not use $USER here, as that env var can be empty when
        # running in a container.
        constants.USER_ENV_VAR: common_utils.get_current_user_name(),
        constants.USER_ID_ENV_VAR: common_utils.get_user_hash(),
        # Skip cloud identity check to avoid the overhead.
        env_options.Options.SKIP_CLOUD_IDENTITY_CHECK.env_key: '1',
        # Disable minimize logging to get more details on the controller.
        env_options.Options.MINIMIZE_LOGGING.env_key: '0',
        constants.IS_SKYPILOT_SERVE_CONTROLLER:
            ('true'
             if controller == Controllers.SKY_SERVE_CONTROLLER else 'false'),
    })
    override_concurrent_launches = os.environ.get(
        constants.SERVE_OVERRIDE_CONCURRENT_LAUNCHES, None)
    if override_concurrent_launches is not None:
        env_vars[constants.SERVE_OVERRIDE_CONCURRENT_LAUNCHES] = str(
            int(override_concurrent_launches))
    if controller == Controllers.SKY_SERVE_CONTROLLER:
        for paid_service_window_variable in (
                constants.SERVE_PAID_SERVICE_MAX_LAUNCH_WINDOW,
                constants.SERVE_PAID_SERVICE_LAUNCH_WINDOW_PROFILES,
                constants.SERVE_PAID_LOCATION_MAX_EXPLORATION_FRONTIER):
            value = os.environ.get(paid_service_window_variable)
            if value is not None:
                env_vars[paid_service_window_variable] = value
    # Forward the client's usage run id so the controller (and the worker
    # clusters it provisions) report heartbeats under the same run id as
    # the originating launch operation. Without this, in consolidation mode
    # the controller process would fall back to its own
    # usage_lib.messages.usage singleton, which is shared across all jobs
    # served by that process and so cannot distinguish between them.
    client_usage_run_id = os.environ.get(usage_constants.USAGE_RUN_ID_ENV_VAR)
    if client_usage_run_id is not None:
        env_vars[usage_constants.USAGE_RUN_ID_ENV_VAR] = client_usage_run_id
    vars_to_fill['controller_envs'] = env_vars
    return vars_to_fill


def get_controller_resources(
    controller: Controllers,
    task_resources: Iterable['resources.Resources'],
) -> set['resources.Resources']:
    """Read the skypilot config and setup the controller resources.

    Returns:
        A set of controller resources that will be used to launch the
        controller. All fields are the same except for the cloud. If no
        controller exists and the controller resources has no cloud
        specified, the controller will be launched on one of the clouds
        of the task resources for better connectivity.
    """
    controller_resources_config_copied: dict[str, Any] = copy.copy(
        controller.value.default_resources_config)
    if skypilot_config.loaded():
        # Override the controller resources with the ones specified in the
        # config.
        custom_controller_resources_config = skypilot_config.get_nested(
            (controller.value.controller_type, 'controller', 'resources'), None)
        if custom_controller_resources_config is not None:
            controller_resources_config_copied.update(
                custom_controller_resources_config)
        # Compatibility with the old way of specifying the controller autostop
        # config. TODO(cooperc): Remove this before 0.12.0.
        custom_controller_autostop_config = skypilot_config.get_nested(
            (controller.value.controller_type, 'controller', 'autostop'), None)
        if custom_controller_autostop_config is not None:
            logger.warning(
                f'{colorama.Fore.YELLOW}Warning: Config value '
                f'`{controller.value.controller_type}.controller.autostop` '
                'is deprecated. Please use '
                f'`{controller.value.controller_type}.controller.resources.'
                f'autostop` instead.{colorama.Style.RESET_ALL}')
            # Only set the autostop config if it is not already specified.
            if controller_resources_config_copied.get('autostop') is None:
                controller_resources_config_copied['autostop'] = (
                    custom_controller_autostop_config)
            else:
                logger.warning(f'{colorama.Fore.YELLOW}Ignoring the old '
                               'config, since it is already specified in '
                               f'resources.{colorama.Style.RESET_ALL}')
    # Set the default autostop config for the controller, if not already
    # specified.
    if controller_resources_config_copied.get('autostop') is None:
        controller_resources_config_copied['autostop'] = (
            controller.value.default_autostop_config)

    try:
        controller_resources = resources.Resources.from_yaml_config(
            controller_resources_config_copied)
    except ValueError as e:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                CONTROLLER_RESOURCES_NOT_VALID_MESSAGE.format(
                    controller_type=controller.value.controller_type,
                    err=common_utils.format_exception(
                        e, use_bracket=True)).capitalize()) from e
    # TODO(tian): Support multiple resources for the controller. One blocker
    # here is the semantic if controller resources use `ordered` and we want
    # to override it with multiple cloud from task resources.
    if len(controller_resources) != 1:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                CONTROLLER_RESOURCES_NOT_VALID_MESSAGE.format(
                    controller_type=controller.value.controller_type,
                    err=f'Expected exactly one resource, got '
                    f'{len(controller_resources)} resources: '
                    f'{controller_resources}').capitalize())
    controller_resources_to_use: resources.Resources = list(
        controller_resources)[0]

    controller_handle = global_user_state.get_handle_from_cluster_name(
        controller.value.cluster_name)
    if controller_handle is not None:
        if controller_handle is not None:
            # Use the existing resources, but override the autostop config with
            # the one currently specified in the config.
            controller_resources_to_use = (
                controller_handle.launched_resources.copy(
                    autostop=controller_resources_config_copied.get('autostop'))
            )

    # If the controller and replicas are from the same cloud (and region/zone),
    # it should provide better connectivity. We will let the controller choose
    # from the clouds (and regions/zones) of the resources if the user does not
    # specify the cloud (and region/zone) for the controller.

    requested_clouds_with_region_zone: dict[str, dict[str | None,
                                                      set[str | None]]] = {}
    for resource in task_resources:
        if resource.cloud is not None:
            cloud_name = str(resource.cloud)
            if cloud_name not in requested_clouds_with_region_zone:
                try:
                    resource.cloud.check_features_are_supported(
                        resources.Resources(),
                        {clouds.CloudImplementationFeatures.HOST_CONTROLLERS})
                except exceptions.NotSupportedError:
                    # Skip the cloud if it does not support hosting controllers.
                    continue
                requested_clouds_with_region_zone[cloud_name] = {}
            if resource.region is None:
                # If one of the resource.region is None, this could represent
                # that the user is unsure about which region the resource is
                # hosted in. In this case, we allow any region for this cloud.
                requested_clouds_with_region_zone[cloud_name] = {None: {None}}
            elif None not in requested_clouds_with_region_zone[cloud_name]:
                if resource.region not in requested_clouds_with_region_zone[
                        cloud_name]:
                    requested_clouds_with_region_zone[cloud_name][
                        resource.region] = set()
                # If one of the resource.zone is None, allow any zone in the
                # region.
                if resource.zone is None:
                    requested_clouds_with_region_zone[cloud_name][
                        resource.region] = {None}
                elif None not in requested_clouds_with_region_zone[cloud_name][
                        resource.region]:
                    requested_clouds_with_region_zone[cloud_name][
                        resource.region].add(resource.zone)
        else:
            # if one of the resource.cloud is None, this could represent user
            # does not know which cloud is best for the specified resources.
            # For example:
            #   resources:
            #     - accelerators: L4     # Both available on AWS and GCP
            #     - cloud: runpod
            #       accelerators: A40
            # In this case, we allow the controller to be launched on any cloud.
            requested_clouds_with_region_zone.clear()
            break

    # Extract filtering criteria from the controller resources specified by the
    # user.
    controller_cloud = str(
        controller_resources_to_use.cloud
    ) if controller_resources_to_use.cloud is not None else None
    controller_region = controller_resources_to_use.region
    controller_zone = controller_resources_to_use.zone

    # Filter clouds if controller_resources_to_use.cloud is specified.
    filtered_clouds: set[str] = {controller_cloud
                                } if controller_cloud is not None else set(
                                    requested_clouds_with_region_zone.keys())

    # Filter regions and zones and construct the result.
    result: set[resources.Resources] = set()
    for cloud_name in filtered_clouds:
        regions = requested_clouds_with_region_zone.get(cloud_name,
                                                        {None: {None}})

        # Filter regions if controller_resources_to_use.region is specified.
        filtered_regions: set[str |
                              None] = ({controller_region} if controller_region
                                       is not None else set(regions.keys()))

        for region in filtered_regions:
            zones = regions.get(region, {None})

            # Filter zones if controller_resources_to_use.zone is specified.
            filtered_zones: set[str |
                                None] = ({controller_zone} if controller_zone
                                         is not None else set(zones))

            # Create combinations of cloud, region, and zone.
            for zone in filtered_zones:
                resource_copy = controller_resources_to_use.copy(
                    cloud=registry.CLOUD_REGISTRY.from_str(cloud_name),
                    region=region,
                    zone=zone)
                result.add(resource_copy)

    if not result:
        return {controller_resources_to_use}
    return result


def get_controller_mem_size_gb() -> float:
    try:
        with open(os.path.expanduser(constants.CONTROLLER_K8S_MEMORY_FILE),
                  encoding='utf-8') as f:
            return float(f.read())
    except FileNotFoundError:
        pass
    return common_utils.get_mem_size_gb()


def _setup_proxy_command_on_controller(
        controller_launched_cloud: 'clouds.Cloud',
        user_config: dict[str, Any]) -> config_utils.Config:
    """Sets up proxy command on the controller.

    This function should be called on the controller (remote cluster), which
    has the `~/.sky/sky_ray.yaml` file.
    """
    # Look up the contents of the already loaded configs via the
    # 'skypilot_config' module. Don't simply read the on-disk file as
    # it may have changed since this process started.
    #
    # Set any proxy command to None, because the controller would've
    # been launched behind the proxy, and in general any nodes we
    # launch may not have or need the proxy setup. (If the controller
    # needs to launch mew clusters in another region/VPC, the user
    # should properly set up VPC peering, which will allow the
    # cross-region/VPC communication. The proxy command is orthogonal
    # to this scenario.)
    #
    # This file will be uploaded to the controller node and will be
    # used throughout the managed job's / service's recovery attempts
    # (i.e., if it relaunches due to preemption, we make sure the
    # same config is used).
    #
    # NOTE: suppose that we have a controller in old VPC, then user
    # changes 'vpc_name' in the config and does a 'job launch' /
    # 'serve up'. In general, the old controller may not successfully
    # launch the job in the new VPC. This happens if the two VPCs don't
    # have peering set up. Like other places in the code, we assume
    # properly setting up networking is user's responsibilities.
    # TODO(zongheng): consider adding a basic check that checks
    # controller VPC (or name) == the managed job's / service's VPC
    # (or name). It may not be a sufficient check (as it's always
    # possible that peering is not set up), but it may catch some
    # obvious errors.
    config = config_utils.Config.from_dict(user_config)
    proxy_command_key = (str(controller_launched_cloud).lower(),
                         'ssh_proxy_command')
    ssh_proxy_command = skypilot_config.get_effective_region_config(
        cloud=str(controller_launched_cloud).lower(),
        region=None,
        keys=('ssh_proxy_command',),
        default_value=None)
    if isinstance(ssh_proxy_command, str):
        config.set_nested(proxy_command_key, None)
    elif isinstance(ssh_proxy_command, dict):
        # Instead of removing the key, we set the value to empty string
        # so that the controller will only try the regions specified by
        # the keys.
        ssh_proxy_command = {k: None for k in ssh_proxy_command}
        config.set_nested(proxy_command_key, ssh_proxy_command)

    return config


def replace_skypilot_config_path_in_file_mounts(
        cloud: 'clouds.Cloud', file_mounts: dict[str, str] | None):
    """Replaces the SkyPilot config path in file mounts with the real path."""
    # TODO(zhwu): This function can be moved to `backend_utils` once we have
    # more predefined file mounts that needs to be replaced after the cluster
    # is provisioned, e.g., we may need to decide which cloud to create a bucket
    # to be mounted to the cluster based on the cloud the cluster is actually
    # launched on (after failover).
    if file_mounts is None:
        return
    replaced = False
    for remote_path, local_path in list(file_mounts.items()):
        if local_path is None:
            del file_mounts[remote_path]
            continue
        if local_path.endswith(_LOCAL_SKYPILOT_CONFIG_PATH_SUFFIX):
            with tempfile.NamedTemporaryFile('w', delete=False) as f:
                user_config = yaml_utils.read_yaml(local_path)
                config = _setup_proxy_command_on_controller(cloud, user_config)
                yaml_utils.dump_yaml(f.name, dict(**config))
                file_mounts[remote_path] = f.name
                replaced = True
    if replaced:
        logger.debug(f'Replaced {_LOCAL_SKYPILOT_CONFIG_PATH_SUFFIX} '
                     f'with the real path in file mounts: {file_mounts}')


# Preserve the established mount-translation facade and serialized identities.
# pylint: disable=protected-access
_generate_run_uuid = controller_mount_translation._generate_run_uuid
translate_local_file_mounts_to_two_hop = (
    controller_mount_translation.translate_local_file_mounts_to_two_hop)
maybe_translate_local_file_mounts_and_sync_up = (
    controller_mount_translation.maybe_translate_local_file_mounts_and_sync_up)
# pylint: enable=protected-access
for _mount_translation_function in (
        _generate_run_uuid, translate_local_file_mounts_to_two_hop,
        maybe_translate_local_file_mounts_and_sync_up):
    _mount_translation_function.__module__ = __name__
# Preserve the established dependency patch path used by characterization tests.
bs = controller_mount_translation.bs

# ======================= Resources Management Functions =======================

# Monitoring process for service is 512MB. This is based on an old
# estimation but we keep it here for now.
# TODO(tian): Remeasure this.
SERVE_MONITORING_MEMORY_MB = 512
# The resource consumption ratio of service launch to serve down.
SERVE_LAUNCH_RATIO = 2.0

# The _RESOURCES_LOCK should be held whenever we are checking the parallelism
# control or updating the schedule_state of any job or service. Any code that
# takes this lock must conclude by calling maybe_schedule_next_jobs.
_RESOURCES_LOCK = '~/.sky/locks/controller_resources.lock'

# keep 2GB reserved after the controllers
MAXIMUM_CONTROLLER_RESERVED_MEMORY_MB = 2048

# NOTE: In the current implementation, we only consider the memory
# The ratio of resources consumption for managed jobs and pool/serve.
# This measures pool_resources / jobs_resources. If 2 GB memory is allocated to
# jobs, then 2 * POOL_JOBS_RESOURCES_RATIO GB memory is allocated to pool/serve.
POOL_JOBS_RESOURCES_RATIO = 1
# Number of ongoing launches launches allowed per worker. Can probably be
# increased a bit to around 16 but keeping it lower to just to be safe
LAUNCHES_PER_WORKER = 8
# Number of ongoing launches allowed per service. Can probably be increased
# a bit as well.
LAUNCHES_PER_SERVICE = 4

# Based on testing, each worker takes around 200-300MB memory. Keeping it
# higher to be safe.
JOB_WORKER_MEMORY_MB = 400
# this can probably be increased to around 300-400 but keeping it lower to just
# to be safe
MAX_JOBS_PER_WORKER = 200
# Maximum number of controllers that can be running. Hard to handle more than
# 512 launches at once.
MAX_CONTROLLERS = 512 // LAUNCHES_PER_WORKER
# Limit the number of jobs that can be running at once on the entire jobs
# controller cluster. It's hard to handle cancellation of more than 2000 jobs at
# once.
# TODO(cooperc): Once we eliminate static bottlenecks (e.g. sqlite), remove this
# hardcoded max limit.
MAX_TOTAL_RUNNING_JOBS = 2000

# In consolidation mode, cap the fraction of available memory (after
# controller reservation) that server workers can consume. The remainder is
# reserved for service/job controllers so that both workers and services
# scale with system memory. Without this cap, short workers grow linearly
# with memory and consume nearly all of it, leaving a roughly fixed number
# of services regardless of system memory size.
_CONSOLIDATION_WORKER_MEMORY_FRACTION = 0.7


def compute_memory_reserved_for_controllers(
        reserve_for_controllers: bool, reserve_extra_for_pool: bool) -> float:
    reserved_memory_mb = 0.0
    if reserve_for_controllers:
        reserved_memory_mb = float(MAXIMUM_CONTROLLER_RESERVED_MEMORY_MB)
        if reserve_extra_for_pool:
            reserved_memory_mb *= (1. + POOL_JOBS_RESOURCES_RATIO)
    return reserved_memory_mb


def _get_total_usable_memory_mb(pool: bool, consolidation_mode: bool) -> float:
    controller_reserved = compute_memory_reserved_for_controllers(
        reserve_for_controllers=True, reserve_extra_for_pool=pool)
    total_memory_mb = (common_utils.get_mem_size_gb() * 1024 -
                       controller_reserved)
    if not consolidation_mode:
        return total_memory_mb
    # Cap the memory available for server workers so that both workers and
    # services scale with system memory. Without this cap, short workers
    # grow linearly with memory, consuming nearly all of it and leaving a
    # roughly fixed amount for services regardless of system memory size.
    # In low-memory scenarios (total_memory_mb <= MIN_AVAIL_MB), skip the
    # service reservation so workers get all available memory; otherwise
    # guarantee workers at least MIN_AVAIL_MB and cap them at the fraction.
    min_avail_mb = (server_constants.MIN_AVAIL_MEM_GB_CONSOLIDATION_MODE * 1024)
    service_reserved = min(
        total_memory_mb * (1 - _CONSOLIDATION_WORKER_MEMORY_FRACTION),
        max(0, total_memory_mb - min_avail_mb))
    worker_reserved = controller_reserved + service_reserved
    config = server_config.compute_server_config(
        deploy=True, quiet=True, reserved_memory_mb=worker_reserved)
    used = 0.0
    used += ((config.long_worker_config.garanteed_parallelism +
              config.long_worker_config.burstable_parallelism) *
             server_config.LONG_WORKER_MEM_GB * 1024)
    used += ((config.short_worker_config.garanteed_parallelism +
              config.short_worker_config.burstable_parallelism) *
             server_config.SHORT_WORKER_MEM_GB * 1024)
    return total_memory_mb - used


def _is_consolidation_mode(pool: bool) -> bool:
    # Note: `pool` here really means "jobs" - whether we fetch the jobs
    # consolidation or the serve consolidation value.
    # TODO(cooperc): rename the argument
    if pool:
        # For jobs, the signal file is the source of truth (managed by
        # setup_consolidation_mode_on_startup at server start).
        return _read_jobs_consolidation_signal()
    return skypilot_config.get_nested(
        ('serve', 'controller', 'consolidation_mode'), default_value=False)


def _read_jobs_consolidation_signal() -> bool:
    """Return whether the jobs consolidation signal file is present.

    Source of truth for jobs-controller consolidation state. The file is
    written by setup_consolidation_mode_on_startup at API server start.
    """
    signal_file = pathlib.Path(
        managed_job_constants.JOBS_CONSOLIDATION_RELOADED_SIGNAL_FILE
    ).expanduser()
    return signal_file.exists()


def warn_jobs_consolidation_mode_intent(enabled: bool) -> None:
    """Warn about leftover state that would block a consolidation-mode flip.

    - enabled=True: warn if a separate jobs-controller cluster still exists.
    - enabled=False: warn if managed jobs are still running.

    Called from is_jobs_consolidation_mode (server-side) and from
    setup_consolidation_mode_on_startup (at API server start).
    """
    if enabled:
        controller_cn = (Controllers.JOBS_CONTROLLER.value.cluster_name)
        if global_user_state.cluster_with_name_exists(controller_cn):
            logger.warning(
                f'{colorama.Fore.RED}Consolidation mode for jobs is enabled, '
                f'but the controller cluster {controller_cn} is still running. '
                'Please terminate the controller cluster first.'
                f'{colorama.Style.RESET_ALL}')
    else:
        total_jobs = managed_job_state.get_managed_jobs_total()
        if total_jobs > 0:
            nonterminal_jobs = (
                managed_job_state.get_nonterminal_job_ids_by_name(
                    None, None, all_users=True))
            if nonterminal_jobs:
                logger.warning(
                    f'{colorama.Fore.YELLOW}Consolidation mode is disabled, '
                    f'but there are still {len(nonterminal_jobs)} managed jobs '
                    'running. Please terminate those jobs first.'
                    f'{colorama.Style.RESET_ALL}')
            else:
                logger.warning(
                    f'{colorama.Fore.YELLOW}Consolidation mode is disabled, '
                    f'but there are {total_jobs} jobs from previous '
                    'consolidation mode. Reset the `jobs.controller.'
                    'consolidation_mode` to `true` and run `sky jobs queue` '
                    'to see those jobs. Switching to normal mode will '
                    f'lose the job history.{colorama.Style.RESET_ALL}')


@annotations.lru_cache(scope='request', maxsize=1)
def _effective_jobs_consolidation_with_warnings() -> tuple[bool, bool | None]:
    """Compute effective jobs consolidation and emit warnings once per request.

    Returns (effective, intent_arg). intent_arg is None when not on the API
    server (no guidance to emit); otherwise it is the value validators should
    check — `config_value` when explicitly set, else `effective`.

    Cached on the request scope so the jobs validator and config-vs-signal
    warning fire at most once per request, even when both managed-jobs and
    pool readers resolve in the same request.
    """
    if os.environ.get(constants.OVERRIDE_CONSOLIDATION_MODE) is not None:
        # Inside the controller process. Always consolidated from its own
        # perspective; no admin-facing guidance to emit.
        return True, None
    effective = _read_jobs_consolidation_signal()
    if os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER) is None:
        # Not on the API server — no config to consult. See #6611.
        return effective, None
    config_value = skypilot_config.get_nested(
        ('jobs', 'controller', 'consolidation_mode'), default_value=None)
    if config_value is not None and config_value != effective:
        expected = 'enabled' if config_value else 'disabled'
        logger.warning(
            f'{colorama.Fore.YELLOW}Consolidation mode for managed jobs '
            f'is {expected} in the server config, but the API server has '
            'not been restarted yet. Please restart the API server to '
            f'apply the change.{colorama.Style.RESET_ALL}')
    arg = config_value if config_value is not None else effective
    warn_jobs_consolidation_mode_intent(arg)
    return effective, arg


def is_jobs_consolidation_mode(
        extra_validator: Callable[[bool], None] | None = None) -> bool:
    """Return effective jobs-controller consolidation state.

    Single source of truth for whether the jobs controller is running in
    consolidation mode. Used by both managed-jobs and pool readers — pool
    operations run on the jobs controller, so both callers must see the
    same value.

    Behavior:
    - OVERRIDE_CONSOLIDATION_MODE env forces True (used inside the
      controller process itself, which is always consolidated from its
      own perspective).
    - Otherwise reads the JOBS_CONSOLIDATION_RELOADED_SIGNAL_FILE, written
      by setup_consolidation_mode_on_startup at API server start.
    - On the API server (IS_SKYPILOT_SERVER env set): warns if the config
      disagrees with effective state (user needs to restart), runs the
      jobs validator against intent (config when set, effective otherwise),
      and calls extra_validator (if supplied) with the same arg. Callers
      may use extra_validator for domain-specific warnings (e.g. the pool
      reader warns about leftover pools in addition to leftover jobs).

    The shared/warning portion is cached per request via
    _effective_jobs_consolidation_with_warnings so warnings fire once even
    when multiple readers resolve in the same request. extra_validator is
    called per invocation; callers should cache their own wrappers if
    their extra_validator is expensive.
    """
    effective, arg = _effective_jobs_consolidation_with_warnings()
    if extra_validator is not None and arg is not None:
        extra_validator(arg)
    return effective


@annotations.lru_cache(scope='request')
def _get_parallelism(pool: bool, raw_resource_per_unit: float) -> int:
    """Returns the number of jobs controllers / services that should be running.

    This is the number of controllers / services that should be running
    to maximize resource utilization.

    In consolidation mode, we use the existing API server so our resource
    requirements are just for the job controllers / services. We try taking
    up as much memory as possible left over from the API server.

    In non-consolidation mode, we have to take into account the memory of the
    API server workers. We limit to only 8 launches per worker, so our logic is
    each controller will take CONTROLLER_MEMORY_MB + 8 * WORKER_MEMORY_MB. We
    leave some leftover room for ssh codegen and ray status overhead.
    """
    consolidation_mode = _is_consolidation_mode(pool)

    total_memory_mb = _get_total_usable_memory_mb(pool, consolidation_mode)

    # In consolidation mode, we assume the API server is running in deployment
    # mode, hence resource management (i.e. how many requests are allowed) is
    # done by the API server.
    resource_per_unit_worker = 0.
    # Otherwise, it runs a local API server on the jobs/serve controller.
    # We need to do the resource management ourselves.
    if not consolidation_mode:
        launches_per_worker = (LAUNCHES_PER_WORKER
                               if pool else LAUNCHES_PER_SERVICE)
        resource_per_unit_worker = (launches_per_worker *
                                    server_config.LONG_WORKER_MEM_GB * 1024)

    # If running pool on jobs controller, we need to account for the resources
    # consumed by the jobs.
    ratio = (1. + POOL_JOBS_RESOURCES_RATIO) if pool else 1.
    resource_per_unit = ratio * (raw_resource_per_unit +
                                 resource_per_unit_worker)

    return max(int(total_memory_mb / resource_per_unit), 1)


def get_number_of_jobs_controllers() -> int:
    return min(
        MAX_CONTROLLERS,
        _get_parallelism(pool=True, raw_resource_per_unit=JOB_WORKER_MEMORY_MB))


@annotations.lru_cache(scope='global', maxsize=1)
def get_resources_lock_path() -> str:
    path = os.path.expanduser(_RESOURCES_LOCK)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def _get_number_of_services(pool: bool) -> int:
    # TODO(cooperc): This should divide by POOL_JOBS_RESOURCES_RATIO, not
    # multiply. The intent is to give pools R times more memory than jobs, but
    # _get_parallelism already applies (1 + R) to the per-unit cost. Multiplying
    # here applies the ratio twice (quadratically), so with R != 1 services
    # would get far fewer slots than intended. Masked by R=1 today.
    return _get_parallelism(pool=pool,
                            raw_resource_per_unit=SERVE_MONITORING_MEMORY_MB *
                            POOL_JOBS_RESOURCES_RATIO)


@annotations.lru_cache(scope='request')
def _get_request_parallelism(pool: bool) -> int:
    # NOTE(dev): One smoke test depends on this value.
    # tests/smoke_tests/test_sky_serve.py::test_skyserve_new_autoscaler_update
    # assumes 4 concurrent launches.
    override_concurrent_launches = os.environ.get(
        constants.SERVE_OVERRIDE_CONCURRENT_LAUNCHES, None)
    if override_concurrent_launches is not None and not pool:
        return int(override_concurrent_launches)
    # Limitation per service x number of services
    launches_per_worker = (LAUNCHES_PER_WORKER
                           if pool else LAUNCHES_PER_SERVICE)
    derived_parallelism = (launches_per_worker * POOL_JOBS_RESOURCES_RATIO *
                           _get_number_of_services(pool))
    serve_consolidated = (
        os.environ.get(constants.OVERRIDE_CONSOLIDATION_MODE) is not None or
        os.environ.get(serve_constants.EXTERNAL_LB_ENABLED_ENV_VAR,
                       '').lower() == 'true' or _is_consolidation_mode(pool))
    if pool or not serve_consolidated:
        return derived_parallelism

    published_parallelism = os.environ.get(
        server_config.SKYPILOT_API_SERVER_LONG_WORKER_PARALLELISM)
    try:
        if published_parallelism is None:
            raise ValueError
        guaranteed_parallelism = int(published_parallelism)
        if guaranteed_parallelism <= 0:
            raise ValueError
    except (TypeError, ValueError):
        logger.warning(
            'Missing or invalid '
            f'{server_config.SKYPILOT_API_SERVER_LONG_WORKER_PARALLELISM} '
            f'value {published_parallelism!r}; preserving the derived Serve '
            f'launch bound of {derived_parallelism}.')
        return derived_parallelism
    return min(derived_parallelism, guaranteed_parallelism)


def in_flight_launch_count() -> float:
    """Launch-budget occupancy: provisioning + terminating / SERVE_LAUNCH_RATIO.

    NOTE: this scans the whole replica table once and unpickles every row
    (O(N)). Callers that evaluate the launch budget for many replicas in a
    single pass (e.g. ``ReplicaManager._refresh_thread_pool``) MUST compute this
    once and track the delta locally -- passing it as ``in_flight`` to
    ``can_provision``/``can_terminate`` -- rather than calling those per
    replica, otherwise the cost is O(K*N) pickle.loads per refresh tick.
    """
    provisioning, terminating = serve_state.get_replica_launch_budget_counts()
    return provisioning + terminating / SERVE_LAUNCH_RATIO


def can_provision(pool: bool, in_flight: float | None = None) -> bool:
    # TODO(tian): probe API server to see if there is any pending provision
    # requests.
    return can_terminate(pool, in_flight=in_flight)


def can_start_new_process(pool: bool) -> bool:
    return serve_state.get_num_services() < _get_number_of_services(pool)


def get_max_services_error_message(pool: bool) -> str:
    """Returns a detailed error message when max services is reached."""
    current = serve_state.get_num_services()
    maximum = _get_number_of_services(pool)
    consolidation = _is_consolidation_mode(pool)
    controller_type = 'jobs' if pool else 'serve'

    msg = (f'{serve_constants.MAX_NUMBER_OF_SERVICES_REACHED_ERROR}: '
           f'{current}/{maximum} services are running.')
    msg += ' To spin up more services, please tear down some existing ones.'

    docs_link = ('https://skypilot.readthedocs.io/en/latest/serving/'
                 'sky-serve.html#sky-serve-max-services-calculation')
    if consolidation:
        msg += (f'\n\nThe {controller_type} controller is running in '
                'consolidation mode, sharing memory with the API server. '
                'The max number of concurrent services is calculated based '
                'on the available memory after reserving resources for the '
                'API server workers. To increase the limit, allocate more '
                f'memory to the API server pod. For more information, see: '
                f'{docs_link}')
    else:
        msg += (f'\n\nThe max number of concurrent services is calculated '
                f'based on the controller VM memory. To increase the limit, '
                f'use a controller with more memory by configuring '
                f'`{controller_type}.controller.resources` in '
                f'~/.sky/config.yaml. For more information, see: '
                f'{docs_link}')

    return msg


def can_terminate(pool: bool, in_flight: float | None = None) -> bool:
    # TODO(tian): probe API server to see if there is any pending terminate
    # requests.
    if in_flight is None:
        in_flight = in_flight_launch_count()
    return in_flight < _get_request_parallelism(pool)
