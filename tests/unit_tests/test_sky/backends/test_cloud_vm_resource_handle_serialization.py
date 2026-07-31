"""Characterization tests for Cloud VM resource-handle serialization."""

# The compatibility contract includes the handle's durable private fields.
# pylint: disable=protected-access

import copy
import inspect
import os
import pickle
import typing
from unittest import mock

from sky import resources as resources_lib
from sky.backends import cloud_vm_ray_backend
from sky.backends import cloud_vm_resource_handle_serialization
from sky.provision import common as provision_common


def _make_handle() -> cloud_vm_ray_backend.CloudVmRayResourceHandle:
    handle = cloud_vm_ray_backend.CloudVmRayResourceHandle(
        cluster_name='cluster',
        cluster_name_on_cloud='cluster-1234',
        cluster_yaml=os.path.join(os.path.expanduser('~'), 'cluster.yaml'),
        launched_nodes=2,
        launched_resources=resources_lib.Resources(cpus='2+', memory='4+'),
        stable_internal_external_ips=[('10.0.0.1', '1.2.3.4'),
                                      ('10.0.0.2', '1.2.3.5')],
        stable_ssh_ports=[22, 2200])
    handle.docker_user = 'sky'
    handle.is_grpc_enabled = False
    handle._ssh_user = 'ubuntu'
    handle.provision_runtime_metadata = (
        provision_common.ProvisionRuntimeMetadata(has_ray=True,
                                                  has_skylet=False,
                                                  has_job_queue=False,
                                                  ssh_available=True,
                                                  runtime_setup_done=True,
                                                  workdir_synced=True,
                                                  file_mounts_synced=False,
                                                  setup_done=True,
                                                  run_started=True))
    return handle


def test_serialization_hook_contract() -> None:
    cls = cloud_vm_ray_backend.CloudVmRayResourceHandle
    expected = {
        'to_dict': ('(self) -> dict', 'CloudVmRayResourceHandle.to_dict'),
        'from_dict': ("(cls, d: dict) -> 'CloudVmRayResourceHandle'",
                      'CloudVmRayResourceHandle.from_dict'),
        '__getstate__': ('(self)', 'CloudVmRayResourceHandle.__getstate__'),
        '__setstate__':
            ('(self, state)', 'CloudVmRayResourceHandle.__setstate__'),
    }

    for name, (signature, qualname) in expected.items():
        descriptor = cls.__dict__[name]
        function = (descriptor.__func__
                    if isinstance(descriptor, classmethod) else descriptor)
        assert str(inspect.signature(function)) == signature
        assert function.__module__ == 'sky.backends.cloud_vm_ray_backend'
        assert function.__qualname__ == qualname

    assert isinstance(cls.__dict__['from_dict'], classmethod)
    assert typing.get_type_hints(
        cls.__dict__['from_dict'].__func__)['return'] is cls


def test_serialization_hooks_are_direct_implementation_methods() -> None:
    cls = cloud_vm_ray_backend.CloudVmRayResourceHandle

    assert cls.__dict__[
        'to_dict'] is cloud_vm_resource_handle_serialization.to_dict
    assert (cls.__dict__['from_dict'].__func__
            is cloud_vm_resource_handle_serialization.from_dict)
    assert cls.__dict__[
        '__getstate__'] is cloud_vm_resource_handle_serialization.__getstate__
    assert cls.__dict__[
        '__setstate__'] is cloud_vm_resource_handle_serialization.__setstate__


def test_dict_round_trip_preserves_fields_and_input() -> None:
    handle = _make_handle()

    data = handle.to_dict()
    assert data == {
        'cluster_name': 'cluster',
        'cluster_name_on_cloud': 'cluster-1234',
        'cluster_yaml': '~/cluster.yaml',
        'launched_nodes': 2,
        'launched_resources': handle.launched_resources.to_yaml_config(),
        'stable_internal_external_ips': [('10.0.0.1', '1.2.3.4'),
                                         ('10.0.0.2', '1.2.3.5')],
        'stable_ssh_ports': [22, 2200],
        'docker_user': 'sky',
        'is_grpc_enabled': False,
        'ssh_user': 'ubuntu',
        'provision_runtime_metadata': {
            'has_ray': True,
            'has_skylet': False,
            'has_job_queue': False,
            'ssh_available': True,
            'runtime_setup_done': True,
            'workdir_synced': True,
            'file_mounts_synced': False,
            'setup_done': True,
            'run_started': True,
        },
    }
    data['provision_runtime_metadata']['future_field'] = 'ignored'
    original_data = copy.deepcopy(data)

    restored = cloud_vm_ray_backend.CloudVmRayResourceHandle.from_dict(data)

    assert data == original_data
    assert restored.cluster_name == handle.cluster_name
    assert restored.cluster_name_on_cloud == handle.cluster_name_on_cloud
    assert restored._cluster_yaml == handle._cluster_yaml
    assert restored.launched_nodes == handle.launched_nodes
    assert (restored.launched_resources.to_yaml_config() ==
            handle.launched_resources.to_yaml_config())
    assert (restored.stable_internal_external_ips ==
            handle.stable_internal_external_ips)
    assert restored.stable_ssh_ports == handle.stable_ssh_ports
    assert restored.docker_user == handle.docker_user
    assert restored.is_grpc_enabled == handle.is_grpc_enabled
    assert restored.ssh_user == handle.ssh_user
    assert restored.cached_cluster_info is None
    assert restored.provision_runtime_metadata == (
        handle.provision_runtime_metadata)


def test_pickle_round_trip_preserves_identity_and_runtime_metadata() -> None:
    handle = _make_handle()

    restored = pickle.loads(pickle.dumps(handle, protocol=4))

    assert type(restored) is cloud_vm_ray_backend.CloudVmRayResourceHandle
    assert restored.to_dict() == handle.to_dict()
    assert isinstance(restored.provision_runtime_metadata,
                      provision_common.ProvisionRuntimeMetadata)


def test_legacy_state_runs_refresh_migrations_once() -> None:
    current = _make_handle()
    state = current.__dict__.copy()
    state.update({
        '_version': 2,
        'head_ip': '1.2.3.4',
        'skylet_ssh_tunnel': object(),
    })
    for name in ('stable_internal_external_ips', 'stable_ssh_ports',
                 'docker_user', 'cluster_name_on_cloud', 'cached_cluster_info',
                 'is_grpc_enabled', 'provision_runtime_metadata'):
        state.pop(name, None)

    restored = cloud_vm_ray_backend.CloudVmRayResourceHandle.__new__(
        cloud_vm_ray_backend.CloudVmRayResourceHandle)
    restored.update_cluster_ips = mock.Mock()
    restored.update_ssh_ports = mock.Mock()
    restored._update_cluster_info = mock.Mock()

    restored.__setstate__(state)

    restored.update_cluster_ips.assert_called_once_with()
    restored.update_ssh_ports.assert_called_once_with()
    restored._update_cluster_info.assert_called_once_with()
    assert restored._version == restored._VERSION
    assert restored.stable_internal_external_ips is None
    assert restored.stable_ssh_ports is None
    assert restored.docker_user is None
    assert restored.cluster_name_on_cloud == restored.cluster_name
    assert restored.cached_cluster_info is None
    assert restored.is_grpc_enabled is False
    assert restored.skylet_ssh_tunnel is None
    assert restored.provision_runtime_metadata == (
        provision_common.ProvisionRuntimeMetadata())
