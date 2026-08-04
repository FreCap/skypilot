"""Serialization policy for Cloud VM resource handles."""

# This module owns the handle's durable private-field representation.
# pylint: disable=protected-access

import dataclasses
import os
import typing

from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky import resources as resources_lib
from sky.provision import common as provision_common
from sky.provision.kubernetes import utils as kubernetes_utils

if typing.TYPE_CHECKING:
    from sky.backends.cloud_vm_ray_backend import CloudVmRayResourceHandle


def to_dict(self) -> dict:
    """Serialize to a JSON-compatible dict."""
    return {
        'cluster_name': self.cluster_name,
        'cluster_name_on_cloud': self.cluster_name_on_cloud,
        'cluster_yaml': self._cluster_yaml,
        'launched_nodes': self.launched_nodes,
        'launched_resources': (self.launched_resources.to_yaml_config() if
                               self.launched_resources is not None else None),
        'stable_internal_external_ips': self.stable_internal_external_ips,
        'stable_ssh_ports': self.stable_ssh_ports,
        'docker_user': self.docker_user,
        'is_grpc_enabled': self.is_grpc_enabled,
        'ssh_user': self.ssh_user,
        'provision_runtime_metadata': dataclasses.asdict(
            self.provision_runtime_metadata),
    }


def from_dict(cls, d: dict) -> 'CloudVmRayResourceHandle':
    """Reconstruct from a dict produced by to_dict()."""
    resources_dict = d.get('launched_resources')
    launched_resources: resources_lib.Resources | None
    if resources_dict is not None:
        launched_resources = (
            resources_lib.Resources._from_yaml_config_single(  # pylint: disable=protected-access
                resources_dict.copy(),
                _allow_resolved_container_image=True))
    else:
        launched_resources = None

    handle = cls.__new__(cls)
    handle._version = cls._VERSION
    handle.cluster_name = d['cluster_name']
    handle.cluster_name_on_cloud = d.get('cluster_name_on_cloud', '')
    handle._cluster_yaml = d.get('cluster_yaml')
    handle.launched_nodes = d.get('launched_nodes', 0)
    handle.launched_resources = launched_resources
    handle.stable_internal_external_ips = d.get('stable_internal_external_ips')
    handle.stable_ssh_ports = d.get('stable_ssh_ports')
    handle.docker_user = d.get('docker_user')
    handle.is_grpc_enabled = d.get('is_grpc_enabled', True)
    handle.cached_cluster_info = None
    handle._ssh_user = d.get('ssh_user')
    runtime_metadata = d.get('provision_runtime_metadata')
    if runtime_metadata is not None:
        known = {
            f.name for f in dataclasses.fields(
                provision_common.ProvisionRuntimeMetadata)
        }
        handle.provision_runtime_metadata = (
            provision_common.ProvisionRuntimeMetadata(**{
                k: v for k, v in runtime_metadata.items() if k in known
            }))
    else:
        handle.provision_runtime_metadata = (
            provision_common.ProvisionRuntimeMetadata())
    return handle


def __getstate__(self):
    state = self.__dict__.copy()
    # For backwards compatibility. Refer to
    # https://github.com/skypilot-org/skypilot/pull/7133
    state.setdefault('skylet_ssh_tunnel', None)
    # Serialize provision_runtime_metadata as a plain dict.
    runtime_metadata = state.get('provision_runtime_metadata')
    if isinstance(runtime_metadata, provision_common.ProvisionRuntimeMetadata):
        state['provision_runtime_metadata'] = dataclasses.asdict(
            runtime_metadata)
    return state


def __setstate__(self, state):
    self._version = self._VERSION

    version = state.pop('_version', None)
    if version is None:
        version = -1
        state.pop('cluster_region', None)
    if version < 2:
        state['_cluster_yaml'] = state.pop('cluster_yaml')
    head_ip = None
    if version < 3:
        head_ip = state.pop('head_ip', None)
        state['stable_internal_external_ips'] = None
    if version < 4:
        # Version 4 adds self.stable_ssh_ports for Kubernetes support
        state['stable_ssh_ports'] = None
    if version < 5:
        state['docker_user'] = None

    if version < 6:
        state['cluster_name_on_cloud'] = state['cluster_name']

    if version < 8:
        self.cached_cluster_info = None

    if version < 9:
        # For backward compatibility, we should update the region of a
        # SkyPilot cluster on Kubernetes to the actual context it is using.
        # pylint: disable=import-outside-toplevel
        launched_resources = state['launched_resources']
        if isinstance(launched_resources.cloud, clouds.Kubernetes):
            yaml_config = global_user_state.get_cluster_yaml_dict(
                os.path.expanduser(state['_cluster_yaml']))
            context = kubernetes_utils.get_context_from_config(
                yaml_config['provider'])
            state['launched_resources'] = launched_resources.copy(
                region=context)

    if version < 10:
        # In #4660, we keep the cluster entry in the database even when it
        # is in the transition from one region to another during the
        # failover. We allow `handle.cluster_yaml` to be None to indicate
        # that the cluster yaml is intentionally removed. Before that PR,
        # the `handle.cluster_yaml` is always not None, even if it is
        # intentionally removed.
        #
        # For backward compatibility, we set the `_cluster_yaml` to None
        # if the file does not exist, assuming all the removal of the
        # _cluster_yaml for existing clusters are intentional by SkyPilot.
        # are intentional by SkyPilot.
        if state['_cluster_yaml'] is not None and not os.path.exists(
                os.path.expanduser(state['_cluster_yaml'])):
            state['_cluster_yaml'] = None

    if version < 11:
        state['is_grpc_enabled'] = False
        state['skylet_ssh_tunnel'] = None

    if version >= 12:
        # DEPRECATED in favor of skylet_ssh_tunnel_metadata column in the DB
        state.pop('skylet_ssh_tunnel', None)

    # provision_runtime_metadata is serialized as a plain dict (see
    # __getstate__). Reconstruct the dataclass here, defaulting if absent.
    runtime_metadata = state.get('provision_runtime_metadata')
    if isinstance(runtime_metadata, dict):
        known = {
            f.name for f in dataclasses.fields(
                provision_common.ProvisionRuntimeMetadata)
        }
        state['provision_runtime_metadata'] = (
            provision_common.ProvisionRuntimeMetadata(**{
                k: v for k, v in runtime_metadata.items() if k in known
            }))
    elif runtime_metadata is None:
        state['provision_runtime_metadata'] = (
            provision_common.ProvisionRuntimeMetadata())

    self.__dict__.update(state)

    # Because the update_cluster_ips and update_ssh_ports
    # functions use the handle, we call it on the current instance
    # after the state is updated.
    if version < 3 and head_ip is not None:
        try:
            self.update_cluster_ips()
        except exceptions.FetchClusterInfoError:
            # This occurs when an old cluster from was autostopped,
            # so the head IP in the database is not updated.
            pass
    if version < 4:
        self.update_ssh_ports()

    if version < 8:
        try:
            self._update_cluster_info()
        except exceptions.FetchClusterInfoError:
            # This occurs when an old cluster from was autostopped,
            # so the head IP in the database is not updated.
            pass


_HISTORICAL_MODULE = 'sky.backends.cloud_vm_ray_backend'
for _name in ('to_dict', 'from_dict', '__getstate__', '__setstate__'):
    _function = globals()[_name]
    _function.__module__ = _HISTORICAL_MODULE
    _function.__qualname__ = f'CloudVmRayResourceHandle.{_name}'
