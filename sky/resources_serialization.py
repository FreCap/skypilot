"""Pickle projection and historical migration for ``sky.Resources``."""

# This module owns the Resources private pickle state by design.
# pylint: disable=protected-access

from collections.abc import Callable
import typing
from typing import Any

from sky import clouds
from sky import container_images as container_images_lib
from sky.provision import docker_utils
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.utils import accelerator_registry
from sky.utils import resources_utils


def getstate(self: Any) -> dict[str, Any]:
    """Return the builtin-only pickle state for a Resources instance."""
    self._validate_container_image_docker_credentials()
    state = self.__dict__.copy()
    if self._container_image is not None:
        if self._container_image_from_legacy_image_id:
            state['_container_image'] = self._container_image.ref
        else:
            state['_container_image'] = self._container_image.to_yaml_config()
    if self._resolved_container_image is not None:
        state['_resolved_container_image'] = (
            self._resolved_container_image.to_dict())
    return state


def setstate(
    self: Any,
    state: dict[str, Any],
    *,
    current_version: int,
    default_disk_size_gb: int,
    maybe_add_docker_prefix_to_image_id: Callable[
        [dict[str | None, str] | None], None],
    normalize_hook_entry: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    """Restore and migrate a pickled Resources state dictionary."""
    self._version = current_version

    # TODO (zhwu): Design our persistent state format with `__getstate__`,
    # so that to get rid of the version tracking.
    version = state.pop('_version', None)
    # Handle old version(s) here.
    if version is None:
        version = -1
    if version < 0:
        cloud = state.pop('cloud', None)
        state['_cloud'] = cloud

        instance_type = state.pop('instance_type', None)
        state['_instance_type'] = instance_type

        use_spot = state.pop('use_spot', False)
        state['_use_spot'] = use_spot

        accelerator_args = state.pop('accelerator_args', None)
        state['_accelerator_args'] = accelerator_args

        disk_size = state.pop('disk_size', default_disk_size_gb)
        state['_disk_size'] = disk_size

    if version < 2:
        self._region = None

    # spot_recovery is deprecated. We keep the history just for readability,
    # it should be removed by chunk in the future.
    if version < 3:
        self._spot_recovery = None

    if version < 4:
        self._image_id = None

    if version < 5:
        self._zone = None

    if version < 6:
        accelerators = state.pop('_accelerators', None)
        if accelerators is not None:
            accelerators = {
                accelerator_registry.canonicalize_accelerator_name(acc,
                                                                   cloud=None): acc_count
                for acc, acc_count in accelerators.items()
            }
        state['_accelerators'] = accelerators

    if version < 7:
        self._cpus = None

    if version < 8:
        self._memory = None

    image_id = state.get('_image_id', None)
    if isinstance(image_id, str):
        state['_image_id'] = {state.get('_region', None): image_id}

    if version < 9:
        self._disk_tier = None

    if version < 10:
        self._is_image_managed = None

    if version < 11:
        self._ports = None

    if version < 12:
        self._docker_login_config = None

    if version < 13:
        original_ports = state.get('_ports', None)
        if original_ports is not None:
            state['_ports'] = resources_utils.simplify_ports(
                [str(port) for port in original_ports])

    if version < 14:
        # Backward compatibility: we change the default value for TPU VM to
        # True in version 14 (#1758), so we need to explicitly set it to
        # False when loading the old handle.
        accelerators = state.get('_accelerators', None)
        if accelerators is not None:
            for acc in accelerators.keys():
                if acc.startswith('tpu'):
                    accelerator_args = state.get('_accelerator_args', {})
                    accelerator_args['tpu_vm'] = accelerator_args.get(
                        'tpu_vm', False)
                    state['_accelerator_args'] = accelerator_args

    if version < 15:
        original_disk_tier = state.get('_disk_tier', None)
        if original_disk_tier is not None:
            state['_disk_tier'] = resources_utils.DiskTier(original_disk_tier)

    if version < 16:
        # Kubernetes clusters launched prior to version 16 run in privileged
        # mode and have FUSE support enabled by default. As a result, we
        # set the default to True for backward compatibility.
        state['_requires_fuse'] = state.get('_requires_fuse', True)

    if version < 17:
        state['_labels'] = state.get('_labels', None)

    if version < 18:
        state['_job_recovery'] = state.pop('_spot_recovery', None)
    # Resources pickled after job_recovery was introduced but before its
    # mapping form was added can also contain the original string form.
    legacy_job_recovery = state.get('_job_recovery')
    if isinstance(legacy_job_recovery, str):
        state['_job_recovery'] = {'strategy': legacy_job_recovery}

    if version < 19:
        self._cluster_config_overrides = state.pop('_cluster_config_overrides',
                                                   None)

    if version < 20:
        # Pre-0.7.0, we used 'kubernetes' as the default region for Kubernetes
        # clusters. With multiple contexts, migrate to the active context.
        legacy_region = 'kubernetes'
        original_cloud = state.get('_cloud', None)
        original_region = state.get('_region', None)
        if (isinstance(original_cloud, clouds.Kubernetes) and
                original_region == legacy_region):
            current_context = (
                kubernetes_utils.get_current_kube_config_context_name())
            state['_region'] = current_context
            if isinstance(state['_image_id'], dict):
                if legacy_region in state['_image_id']:
                    state['_image_id'][current_context] = state['_image_id'][
                        legacy_region]
                    del state['_image_id'][legacy_region]

    if version < 21:
        self._cached_repr = None

    if version < 22:
        self._docker_username_for_runpod = state.pop(
            '_docker_username_for_runpod', None)

    if version < 23:
        self._autostop_config = None

    if version < 24:
        self._volumes = None

    if version < 25:
        if isinstance(state.get('_cloud', None), clouds.Kubernetes):
            maybe_add_docker_prefix_to_image_id(state['_image_id'])

    if version < 26:
        self._network_tier = state.get('_network_tier', None)

    if version < 27:
        self._priority = None

    if version < 30:
        self._priority_class = None

    if version < 28:
        self._no_missing_accel_warnings = state.get(
            '_no_missing_accel_warnings', None)

    if version < 29:
        self._local_disk = None

    if version < 31:
        self._max_hourly_cost = None

    if version < 32:
        self._ephemeral_storage = None

    if version < 34:
        self._docker_image = None

    if version < 35:
        docker_image = state.get('_docker_image')
        legacy_image_id = state.get('_image_id')
        if isinstance(legacy_image_id, dict):
            docker_values = {
                value[len('docker:'):]
                for value in legacy_image_id.values()
                if value.startswith('docker:')
            }
            if docker_image is None and len(docker_values) == 1:
                docker_image = next(iter(docker_values))
            if docker_image is None:
                region_image = (legacy_image_id.get(state.get('_region')) or
                                legacy_image_id.get(None))
                if (region_image is not None and
                        region_image.startswith('docker:')):
                    docker_image = region_image[len('docker:'):]
            if docker_image is not None:
                state['_image_id'] = {
                    key: value
                    for key, value in legacy_image_id.items()
                    if not value.startswith('docker:')
                } or None
        state['_container_image'] = (
            container_images_lib.ContainerImage.from_legacy_ref(docker_image)
            if docker_image is not None else None)
        state['_resolved_container_image'] = None
        state['_docker_image'] = (state['_container_image'].ref
                                  if state['_container_image'] is not None else
                                  None)
    else:
        container_image = state.get('_container_image')
        if container_image is not None:
            legacy_container_image = bool(
                state.get('_container_image_from_legacy_image_id', False))
            if legacy_container_image:
                if not isinstance(container_image, str):
                    raise ValueError('Legacy Docker image state must be a '
                                     'string reference.')
                state['_container_image'] = (container_images_lib.ContainerImage
                                             .from_legacy_ref(container_image))
            else:
                state['_container_image'] = (container_images_lib.ContainerImage
                                             .from_config(container_image))
        resolved = state.get('_resolved_container_image')
        if resolved is not None:
            state['_resolved_container_image'] = (
                container_images_lib.ResolvedContainerImage.from_dict(resolved))

    if version < 36:
        state['_container_image_from_legacy_image_id'] = (
            version < 35 and state.get('_container_image') is not None)

    if version < 33:
        # Route legacy AutostopConfig.hook / hook_timeout attrs into the new
        # _hooks list, and scrub them from AutostopConfig.
        hooks = list(state.get('_hooks') or [])
        autostop = state.get('_autostop_config')
        legacy_hook = getattr(autostop, 'hook', None) if autostop else None
        legacy_timeout = (getattr(autostop, 'hook_timeout', None)
                          if autostop else None)
        if legacy_hook is not None:
            legacy_event = ('down'
                            if getattr(autostop, 'down', False) else 'stop')
            hooks.append(
                normalize_hook_entry({
                    'run': legacy_hook,
                    'events': [legacy_event],
                    'timeout': legacy_timeout,
                }))
        if autostop is not None:
            for attr in ('hook', 'hook_timeout'):
                if hasattr(autostop, attr):
                    try:
                        delattr(autostop, attr)
                    except AttributeError:
                        pass
        state['_hooks'] = hooks or None

    self.__dict__.update(state)
    docker_login_config = getattr(self, '_docker_login_config', None)
    if isinstance(docker_login_config, dict):
        login_config_dict = typing.cast(dict[str, str], docker_login_config)
        self._docker_login_config = docker_utils.DockerLoginConfig(
            username=login_config_dict['username'],
            password=login_config_dict['password'],
            server=login_config_dict['server'])
    self._validate_container_image_docker_credentials()
