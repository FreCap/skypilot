"""Unpaid production-interface tests for Serve job-status transport.

These tests exercise the real handle-to-runner and backend-to-runner seams.  No
provider network call is made.  They are component contract tests, not provider
end-to-end evidence.
"""

from collections.abc import Mapping
import time
from typing import Any
from unittest import mock

import pytest

from sky import clouds
from sky import resources as resources_lib
from sky.backends import cloud_vm_ray_backend
from sky.provision import common as provision_common
from sky.skylet import job_lib
from sky.skylet import log_lib
from sky.utils import command_runner
from sky.utils import message_utils
from sky.utils import yaml_utils


def _cluster_info(
    provider_name: str,
    *,
    instance_id: str = 'head',
    external_ip: str | None = '203.0.113.10',
    ssh_port: int = 2200,
) -> provision_common.ClusterInfo:
    provider_config = None
    if provider_name == 'kubernetes':
        provider_config = {
            'context': 'test-context',
            'namespace': 'test-namespace',
        }
    return provision_common.ClusterInfo(
        instances={
            instance_id: [
                provision_common.InstanceInfo(instance_id=instance_id,
                                              internal_ip='10.0.0.10',
                                              external_ip=external_ip,
                                              tags={},
                                              ssh_port=ssh_port)
            ]
        },
        head_instance_id=instance_id,
        provider_name=provider_name,
        provider_config=provider_config,
    )


def _handle(
    *,
    name: str,
    cloud: clouds.Cloud,
    instance_type: str,
    cluster_info: provision_common.ClusterInfo | None = None,
    stable_ips: list[tuple[str, str]] | None = None,
    stable_ports: list[int] | None = None,
) -> cloud_vm_ray_backend.CloudVmRayResourceHandle:
    return cloud_vm_ray_backend.CloudVmRayResourceHandle(
        cluster_name=name,
        cluster_name_on_cloud=f'{name}-on-cloud',
        cluster_yaml=f'/clusters/{name}.yaml',
        launched_nodes=1,
        launched_resources=resources_lib.Resources(cloud=cloud,
                                                   instance_type=instance_type),
        stable_internal_external_ips=stable_ips,
        stable_ssh_ports=stable_ports,
        cluster_info=cluster_info,
    )


def _cached_credentials() -> dict[str, Any]:
    return {
        'ssh_user': 'ubuntu',
        'ssh_private_key': None,
        'ssh_control_name': 'must-not-survive',
    }


def _assert_no_dynamic_refresh(handle, callback):
    """Run a cached-only callback while every dynamic resolver is fatal."""
    with mock.patch.object(
            handle,
            '_update_cluster_info',
            side_effect=AssertionError('provider refresh used')) as update, \
         mock.patch.object(
             handle,
             'external_ips',
             side_effect=AssertionError('dynamic IP lookup used')) as ips, \
         mock.patch.object(
             handle,
             'external_ssh_ports',
             side_effect=AssertionError('dynamic port lookup used')) as ports:
        result = callback()
    update.assert_not_called()
    ips.assert_not_called()
    ports.assert_not_called()
    return result


def test_legacy_cached_runner_uses_only_persisted_ips_and_ports(monkeypatch):
    legacy_cloud = mock.MagicMock(spec=clouds.Cloud)
    legacy_cloud.PROVISIONER_VERSION = clouds.ProvisionerVersion.RAY_AUTOSCALER
    handle = _handle(name='legacy',
                     cloud=legacy_cloud,
                     instance_type='legacy-instance',
                     stable_ips=[('10.0.0.10', '203.0.113.10')],
                     stable_ports=[2200])
    credentials = mock.Mock(return_value=_cached_credentials())
    monkeypatch.setattr(cloud_vm_ray_backend.backend_utils,
                        'ssh_credential_from_yaml', credentials)

    runners = _assert_no_dynamic_refresh(
        handle,
        lambda: handle.get_cached_command_runners(avoid_ssh_control=True))

    assert len(runners) == 1
    runner = runners[0]
    assert isinstance(runner, command_runner.SSHCommandRunner)
    assert (runner.ip, runner.port) == ('203.0.113.10', 2200)
    assert runner.ssh_control_name is None
    ssh_command = runner.ssh_base_command(
        ssh_mode=command_runner.SshMode.NON_INTERACTIVE,
        port_forward=None,
        connect_timeout=10)
    assert not any('ControlMaster' in argument or 'ControlPersist' in argument
                   for argument in ssh_command)
    credentials.assert_called_once_with(handle.cluster_yaml, None, None)


def test_new_provisioner_cached_runner_uses_only_cluster_info(monkeypatch):
    handle = _handle(name='new-aws',
                     cloud=clouds.AWS(),
                     instance_type='g6.xlarge',
                     cluster_info=_cluster_info('aws'))
    monkeypatch.setattr(cloud_vm_ray_backend.backend_utils,
                        'ssh_credential_from_yaml',
                        mock.Mock(return_value=_cached_credentials()))

    runners = _assert_no_dynamic_refresh(
        handle,
        lambda: handle.get_cached_command_runners(avoid_ssh_control=True))

    assert len(runners) == 1
    runner = runners[0]
    assert isinstance(runner, command_runner.SSHCommandRunner)
    assert (runner.ip, runner.port) == ('203.0.113.10', 2200)
    assert runner.ssh_control_name is None


def test_kubernetes_cached_runner_does_not_refresh_ha_pod_identity(monkeypatch):
    handle = _handle(name='new-kubernetes',
                     cloud=clouds.Kubernetes(),
                     instance_type='4CPU--16GB',
                     cluster_info=_cluster_info('kubernetes',
                                                instance_id='head-pod',
                                                external_ip=None,
                                                ssh_port=22))
    monkeypatch.setattr(cloud_vm_ray_backend.backend_utils,
                        'ssh_credential_from_yaml',
                        mock.Mock(return_value=_cached_credentials()))
    high_availability = mock.Mock(
        side_effect=AssertionError('HA identity refresh used'))
    monkeypatch.setattr(
        cloud_vm_ray_backend.controller_utils,
        'high_availability_specified',
        high_availability,
    )

    runners = _assert_no_dynamic_refresh(
        handle,
        lambda: handle.get_cached_command_runners(avoid_ssh_control=True))

    assert len(runners) == 1
    runner = runners[0]
    assert isinstance(runner, command_runner.KubernetesCommandRunner)
    assert runner.namespace == 'test-namespace'
    assert runner.context == 'test-context'
    assert runner.pod_name == 'head-pod'
    high_availability.assert_not_called()


def test_local_cached_runner_has_no_credential_or_provider_dependency(
        monkeypatch):
    handle = cloud_vm_ray_backend.LocalResourcesHandle(
        cluster_name='local',
        cluster_name_on_cloud='local',
        cluster_yaml=None,
        launched_nodes=1,
        launched_resources=resources_lib.Resources(cpus='1'),
    )
    credentials = mock.Mock(
        side_effect=AssertionError('local credentials resolved'))
    monkeypatch.setattr(cloud_vm_ray_backend.backend_utils,
                        'ssh_credential_from_yaml', credentials)

    runners = _assert_no_dynamic_refresh(
        handle,
        lambda: handle.get_cached_command_runners(avoid_ssh_control=True))

    assert len(runners) == 1
    assert isinstance(runners[0], command_runner.LocalProcessCommandRunner)
    credentials.assert_not_called()


class _PreparationHandle:
    """Lightweight implementation of the transport preparation boundary."""

    def __init__(self, index: int, *, fail: bool = False):
        self.index = index
        self.cluster_yaml = f'/clusters/{index}.yaml'
        self.docker_user = None
        self.ssh_user = 'ubuntu'
        self.fail = fail
        self.runner = mock.Mock(enable_interactive_auth=False)
        self.cached_calls: list[tuple[Mapping[str, Any], bool]] = []

    def _get_cached_command_runners_with_credentials(
        self,
        credentials: Mapping[str, Any],
        *,
        avoid_ssh_control: bool = False,
    ) -> list[command_runner.CommandRunner]:
        self.cached_calls.append((credentials, avoid_ssh_control))
        if self.fail:
            raise ValueError(f'bad cached metadata at {self.index}')
        return [self.runner]


def test_preparation_isolates_one_bad_handle_at_max_cardinality(monkeypatch):
    cardinality = 800
    bad_index = 417
    handles = [
        _PreparationHandle(index, fail=index == bad_index)
        for index in range(cardinality)
    ]
    configs = [{
        'auth': {
            'ssh_user': 'ubuntu'
        },
        'provider': {
            'module': 'sky.aws'
        },
    } for _ in handles]
    yaml_strings = [yaml_utils.dump_yaml_str(config) for config in configs]
    read_many = mock.Mock(return_value=yaml_strings)
    read_one = mock.Mock(
        side_effect=AssertionError('healthy batch fell back to N reads'))
    credential_reads = mock.Mock(return_value=_cached_credentials())
    monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                        'get_cluster_yaml_str_multiple', read_many)
    monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                        'get_cluster_yaml_dict', read_one)
    monkeypatch.setattr(cloud_vm_ray_backend.backend_utils,
                        'ssh_credential_from_yaml', credential_reads)

    preparations = (cloud_vm_ray_backend.CloudVmRayBackend.
                    build_serve_job_status_transports(
                        handles, command_timeout_seconds=10))

    assert len(preparations) == cardinality
    assert [preparation.source_handle for preparation in preparations
           ] == handles
    failures = [
        preparation for preparation in preparations
        if preparation.error is not None
    ]
    assert len(failures) == 1
    assert failures[0].source_handle is handles[bad_index]
    assert str(failures[0].error) == (f'bad cached metadata at {bad_index}')
    assert sum(preparation.transport is not None
               for preparation in preparations) == cardinality - 1
    for index, preparation in enumerate(preparations):
        handle = handles[index]
        assert handle.cached_calls == [(_cached_credentials(), True)]
        if index != bad_index:
            assert preparation.transport is not None
            assert preparation.transport.source_handle is handle
            assert preparation.transport.head_runner is handle.runner
    read_many.assert_called_once_with(
        [handle.cluster_yaml for handle in handles])
    read_one.assert_not_called()
    assert credential_reads.call_count == cardinality


def test_preparation_isolates_malformed_yaml_with_one_batch_read(monkeypatch):
    cardinality = 800
    bad_index = 417
    handles = [_PreparationHandle(index) for index in range(cardinality)]
    valid_yaml = yaml_utils.dump_yaml_str({
        'auth': {
            'ssh_user': 'ubuntu'
        },
        'provider': {
            'module': 'sky.aws'
        },
    })
    yaml_strings = [valid_yaml] * cardinality
    yaml_strings[bad_index] = 'auth: [unterminated'
    read_many = mock.Mock(return_value=yaml_strings)
    read_one = mock.Mock(
        side_effect=AssertionError('malformed batch fell back to N reads'))
    monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                        'get_cluster_yaml_str_multiple', read_many)
    monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                        'get_cluster_yaml_dict', read_one)
    monkeypatch.setattr(cloud_vm_ray_backend.backend_utils,
                        'ssh_credential_from_yaml',
                        mock.Mock(return_value=_cached_credentials()))

    preparations = (cloud_vm_ray_backend.CloudVmRayBackend.
                    build_serve_job_status_transports(
                        handles, command_timeout_seconds=10))

    assert len(preparations) == cardinality
    assert sum(preparation.transport is not None
               for preparation in preparations) == cardinality - 1
    assert preparations[bad_index].transport is None
    assert preparations[bad_index].error is not None
    read_many.assert_called_once_with(
        [handle.cluster_yaml for handle in handles])
    read_one.assert_not_called()


class _ExactRunner:
    """Minimal runner that exposes only the strict transport entrypoint."""

    enable_interactive_auth = False

    def __init__(self, payload: str):
        self.payload = payload
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def run_driver(self, code: str, **kwargs):
        self.calls.append((code, kwargs))
        return 0, self.payload, ''


def test_exact_transport_binds_handle_and_bypasses_dynamic_helpers(monkeypatch):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    source = mock.MagicMock(spec=cloud_vm_ray_backend.CloudVmRayResourceHandle)
    source.is_grpc_enabled_with_flag = True
    other = mock.MagicMock(spec=cloud_vm_ray_backend.CloudVmRayResourceHandle)
    runner = _ExactRunner(message_utils.encode_payload({7: 'RUNNING'}))
    transport = cloud_vm_ray_backend.ServeJobStatusTransport(
        source_handle=source,
        head_runner=runner,
        command_timeout_seconds=10,
    )
    run_on_head = mock.Mock(
        side_effect=AssertionError('generic runner resolution used'))
    skylet = mock.Mock(side_effect=AssertionError('Skylet helper used'))
    invoke_skylet = mock.Mock(side_effect=AssertionError('Skylet retry used'))
    monkeypatch.setattr(backend, 'run_on_head', run_on_head)
    monkeypatch.setattr(cloud_vm_ray_backend, 'SkyletClient', skylet)
    monkeypatch.setattr(cloud_vm_ray_backend.backend_utils,
                        'invoke_skylet_with_retries', invoke_skylet)

    started = time.monotonic()
    # Pylint resolves the committed backend signature rather than this
    # worktree's new keyword-only transport contract.
    # pylint: disable-next=unexpected-keyword-arg
    statuses = backend.get_job_status(source, [7],
                                      stream_logs=False,
                                      serve_transport=transport)

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert len(runner.calls) == 1
    _, kwargs = runner.calls[0]
    assert kwargs['stream_logs'] is False
    assert kwargs['require_outputs'] is True
    assert kwargs['separate_stderr'] is True
    assert kwargs['process_stream'] is False
    assert kwargs['connect_timeout'] == 10
    capture = kwargs['bounded_capture']
    assert isinstance(capture, log_lib.BoundedSubprocessCapture)
    assert capture.max_output_bytes == 1024 * 1024
    assert started < capture.deadline_monotonic <= time.monotonic() + 10
    run_on_head.assert_not_called()
    skylet.assert_not_called()
    invoke_skylet.assert_not_called()

    with pytest.raises(ValueError, match='another handle'):
        # pylint: disable-next=unexpected-keyword-arg
        backend.get_job_status(other, [7],
                               stream_logs=False,
                               serve_transport=transport)
    assert len(runner.calls) == 1


@pytest.mark.parametrize('timeout', [True, False, 0, -1, 1.5, float('inf')])
def test_exact_transport_rejects_invalid_deadlines(timeout):
    with pytest.raises(ValueError, match='positive integer'):
        cloud_vm_ray_backend.ServeJobStatusTransport(
            source_handle=mock.MagicMock(),
            head_runner=mock.Mock(enable_interactive_auth=False),
            command_timeout_seconds=timeout,
        )


def test_exact_transport_rejects_interactive_auth():
    with pytest.raises(ValueError, match='interactive auth'):
        cloud_vm_ray_backend.ServeJobStatusTransport(
            source_handle=mock.MagicMock(),
            head_runner=mock.Mock(enable_interactive_auth=True),
            command_timeout_seconds=10,
        )
