"""Unit tests for backend utility helpers."""

# pylint: disable=protected-access,unused-argument

import base64
import copy
import functools
import gzip
import hashlib
import io
import json
import os
import pathlib
import socket
import subprocess
from types import SimpleNamespace
from unittest import mock

import pytest

from sky import backends
from sky import check as sky_check
from sky import clouds
from sky import exceptions
from sky import skypilot_config
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend
from sky.exceptions import ClusterNotUpError
from sky.provision import docker_utils
from sky.provision import instance_setup
from sky.provision import ray_commands
from sky.provision.kubernetes import pod_spec as kubernetes_pod_spec
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.resources import Resources
from sky.utils import command_runner
from sky.utils import common
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import registry
from sky.utils import resources_utils
from sky.utils import status_lib
from sky.utils import yaml_utils


def _with_skypilot_config(path):
    """Load one process config and restore the prior environment on exit."""

    def decorator(test):

        @functools.wraps(test)
        def wrapped(*args, **kwargs):
            key = skypilot_config.ENV_VAR_SKYPILOT_CONFIG
            previous = os.environ.get(key)
            try:
                os.environ[key] = path
                skypilot_config.reload_config()
                return test(*args, **kwargs)
            finally:
                if previous is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = previous
                skypilot_config.reload_config()

        return wrapped

    return decorator


def test_provider_templates_use_shared_wheel_installer():
    """Every reachable provider template delegates worker installation."""
    templates = {
        cloud_vm_ray_backend._get_cluster_config_template(cloud)
        for _, cloud in registry.CLOUD_REGISTRY.items()
    }
    assert 'local-ray.yml.j2' not in templates

    template_root = pathlib.Path(__file__).parents[2] / 'sky' / 'templates'
    shared_installers = ('ray_skypilot_installation_commands',
                         'skypilot_wheel_installation_commands')
    for template in templates:
        contents = (template_root / template).read_text(encoding='utf-8')
        assert any(installer in contents for installer in shared_installers), (
            f'{template} bypasses the shared SkyPilot wheel installer')


def test_add_auth_rejects_unsupported_cloud(tmp_path):
    cloud = object.__new__(clouds.Cloud)
    yaml_path = tmp_path / 'cluster.yaml'
    yaml_utils.dump_yaml(yaml_path, {'auth': {}})

    with pytest.raises(AssertionError):
        backend_utils._add_auth_to_cluster_config(cloud, str(yaml_path))


def test_optimize_file_mounts_quotes_local_sources(monkeypatch, tmp_path):
    source = tmp_path / 'research\'s "final" credentials.json'
    source.write_text('credential', encoding='utf-8')
    runtime_dir = tmp_path / 'runtime'
    runtime_dir.mkdir()
    yaml_path = tmp_path / 'cluster.yaml'
    yaml_utils.dump_yaml(yaml_path, {
        'file_mounts': {
            '/remote/credential': str(source),
        },
    })
    monkeypatch.setattr(backend_utils.tempstore, 'mkdtemp',
                        lambda: str(runtime_dir))

    backend_utils._optimize_file_mounts(str(yaml_path))

    copied_files = list(runtime_dir.iterdir())
    assert len(copied_files) == 1
    assert copied_files[0].read_text(encoding='utf-8') == 'credential'


def test_path_size_megabytes_quotes_path(tmp_path):
    source = tmp_path / 'research\'s "final" data.json'
    source.write_text('data', encoding='utf-8')

    assert backend_utils.path_size_megabytes(str(source)) == 0


@pytest.mark.parametrize('credential_helper', ['ecr-login', None])
def test_aws_template_preserves_docker_credential_helper(
        tmp_path, credential_helper):
    output_path = tmp_path / 'aws-ray.yaml'
    login = docker_utils.DockerLoginConfig(
        username='',
        password='',
        server='123456789012.dkr.ecr.us-east-1.amazonaws.com')
    login.credential_helper = credential_helper
    common_utils.fill_template(
        'aws-ray.yml.j2', {
            'cluster_name_on_cloud': 'cluster',
            'num_nodes': 1,
            'docker_image': 'registry/image@sha256:' + 'a' * 64,
            'docker_container_name': 'sky_container',
            'docker_run_options': [],
            'docker_login_config': login,
            'region': 'us-east-1',
            'zones': 'us-east-1a',
            'security_group': 'sg',
            'security_group_managed_by_skypilot': 'true',
            'vpc_name': None,
            'subnet_names': None,
            'use_internal_ips': False,
            'max_efa_interfaces': 0,
            'ssh_user': 'ubuntu',
            'ssh_private_key': '/tmp/key',
            'ssh_proxy_command': None,
            'remote_identity': 'LOCAL_CREDENTIALS',
            'instance_type': 'g5.xlarge',
            'image_id': 'ami-1234',
            'root_device_name': '/dev/sda1',
            'disk_size': 256,
            'disk_tier': 'gp3',
            'disk_encrypted': True,
            'disk_iops': None,
            'disk_throughput': None,
            'use_spot': False,
            'specific_reservations': None,
            'runcmd': None,
            'user': 'sky',
            'labels': {},
            'sky_ray_yaml_remote_path': '/tmp/ray.yaml',
            'sky_ray_yaml_local_path': '/tmp/ray-local.yaml',
            'sky_remote_path': '~/.sky',
            'sky_wheel_hash': 'hash',
            'sky_local_path': '/tmp/sky.whl',
            'credentials': {},
            'initial_setup_commands': [],
            'conda_installation_commands': '',
            'uv_installation_commands': '',
            'ray_skypilot_installation_commands': '',
            'copy_skypilot_templates_commands': '',
            'ssh_max_sessions_config': '',
        }, str(output_path))

    login_config = yaml_utils.read_yaml(
        output_path)['docker']['docker_login_config']
    if credential_helper is None:
        assert 'credential_helper' not in login_config
    else:
        assert login_config['credential_helper'] == credential_helper


# Set env var to test config file.
@mock.patch.object(skypilot_config, '_global_config_context',
                   skypilot_config.ConfigContext())
@mock.patch('sky.catalog.instance_type_exists', return_value=True)
@mock.patch('sky.catalog.get_accelerators_from_instance_type',
            return_value={'fake-acc': 2})
@mock.patch('sky.catalog.get_image_id_from_tag', return_value='fake-image')
@mock.patch.object(clouds.AWS,
                   'get_image_root_device_name',
                   return_value='/dev/sda1')
@mock.patch.object(clouds.aws, 'DEFAULT_SECURITY_GROUP_NAME', 'fake-default-sg')
@mock.patch('sky.check.get_cloud_credential_file_mounts',
            return_value='~/.aws/credentials')
@mock.patch('sky.catalog.get_arch_from_instance_type', return_value='fake-arch')
@mock.patch('sky.backends.backend_utils._get_yaml_path_from_cluster_name',
            return_value='/tmp/fake/path')
@mock.patch('sky.backends.backend_utils._deterministic_cluster_yaml_hash',
            return_value='fake-hash')
@mock.patch('sky.utils.common_utils.fill_template')
@_with_skypilot_config('./tests/test_yamls/test_aws_config.yaml')
def test_write_cluster_config_w_remote_identity(mock_fill_template,
                                                *mocks) -> None:

    cloud = clouds.AWS()

    region = clouds.Region(name='fake-region')
    zones = [clouds.Zone(name='fake-zone')]
    resource = Resources(cloud=cloud, instance_type='fake-type: 3')

    cluster_config_template = 'aws-ray.yml.j2'

    # test default
    backend_utils.write_cluster_config(
        to_provision=resource,
        num_nodes=2,
        cluster_config_template=cluster_config_template,
        cluster_name='display',
        local_wheel_path=pathlib.Path('/tmp/fake'),
        wheel_hash='b1bd84059bc0342f7843fcbe04ab563e',
        region=region,
        zones=zones,
        dryrun=True,
        keep_launch_fields_in_existing_config=True)

    expected_subset = {
        'instance_type': 'fake-type: 3',
        'custom_resources': '{"fake-acc":2}',
        'region': 'fake-region',
        'zones': 'fake-zone',
        'image_id': 'fake-image',
        'security_group': 'fake-default-sg',
        'security_group_managed_by_skypilot': 'true',
        'vpc_name': 'fake-vpc',
        'remote_identity': 'LOCAL_CREDENTIALS',  # remote identity
        'sky_local_path': '/tmp/fake',
        'sky_wheel_hash': 'b1bd84059bc0342f7843fcbe04ab563e',
    }

    mock_fill_template.assert_called_once()
    assert mock_fill_template.call_args[0][
        0] == cluster_config_template, 'config template incorrect'
    assert mock_fill_template.call_args[0][1].items() >= expected_subset.items(
    ), 'config fill values incorrect'

    # test using cluster matches regex, top
    mock_fill_template.reset_mock()
    expected_subset.update({
        'security_group': 'fake-1-sg',
        'security_group_managed_by_skypilot': 'false',
        'remote_identity': 'fake1-skypilot-role'
    })
    backend_utils.write_cluster_config(
        to_provision=resource,
        num_nodes=2,
        cluster_config_template=cluster_config_template,
        cluster_name='sky-serve-fake1-1234',
        local_wheel_path=pathlib.Path('/tmp/fake'),
        wheel_hash='b1bd84059bc0342f7843fcbe04ab563e',
        region=region,
        zones=zones,
        dryrun=True,
        keep_launch_fields_in_existing_config=True)

    mock_fill_template.assert_called_once()
    assert mock_fill_template.call_args[0][0] == cluster_config_template, (
        'config template incorrect')
    assert mock_fill_template.call_args[0][1].items() >= expected_subset.items(
    ), 'config fill values incorrect'

    # test using cluster matches regex, middle
    mock_fill_template.reset_mock()
    expected_subset.update({
        'security_group': 'fake-2-sg',
        'security_group_managed_by_skypilot': 'false',
        'remote_identity': 'fake2-skypilot-role'
    })
    backend_utils.write_cluster_config(
        to_provision=resource,
        num_nodes=2,
        cluster_config_template=cluster_config_template,
        cluster_name='sky-serve-fake2-1234',
        local_wheel_path=pathlib.Path('/tmp/fake'),
        wheel_hash='b1bd84059bc0342f7843fcbe04ab563e',
        region=region,
        zones=zones,
        dryrun=True,
        keep_launch_fields_in_existing_config=True)

    mock_fill_template.assert_called_once()
    assert mock_fill_template.call_args[0][0] == cluster_config_template, (
        'config template incorrect')
    assert mock_fill_template.call_args[0][1].items() >= expected_subset.items(
    ), 'config fill values incorrect'


@mock.patch.object(skypilot_config, '_global_config_context',
                   skypilot_config.ConfigContext())
@mock.patch('sky.catalog.instance_type_exists', return_value=True)
@mock.patch('sky.catalog.get_accelerators_from_instance_type',
            return_value={'fake-acc': 2})
@mock.patch('sky.catalog.get_image_id_from_tag', return_value='fake-image')
@mock.patch.object(clouds.AWS,
                   'get_image_root_device_name',
                   return_value='/dev/sda1')
@mock.patch('sky.catalog.get_arch_from_instance_type', return_value='fake-arch')
@mock.patch('sky.backends.backend_utils._get_yaml_path_from_cluster_name',
            return_value='/tmp/fake/path')
@mock.patch('sky.utils.common_utils.fill_template')
@_with_skypilot_config('./tests/test_yamls/test_aws_config_runcmd.yaml')
def test_write_cluster_config_w_post_provision_runcmd_aws(
        mock_fill_template, *mocks):

    cloud = clouds.AWS()
    region = clouds.Region(name='fake-region')
    zones = [clouds.Zone(name='fake-zone')]
    resource = Resources(cloud=cloud, instance_type='fake-type: 3')
    cluster_config_template = 'aws-ray.yml.j2'

    backend_utils.write_cluster_config(
        to_provision=resource,
        num_nodes=2,
        cluster_config_template=cluster_config_template,
        cluster_name='display',
        local_wheel_path=pathlib.Path('/tmp/fake'),
        wheel_hash='b1bd84059bc0342f7843fcbe04ab563e',
        region=region,
        zones=zones,
        dryrun=True,
        keep_launch_fields_in_existing_config=True)

    expected_runcmd = [
        'echo "hello world!"',
        ['ls', '-l', '/'],
    ]
    mock_fill_template.assert_called_once()
    assert mock_fill_template.call_args[0][
        0] == cluster_config_template, 'config template incorrect'
    assert mock_fill_template.call_args[0][1][
        'runcmd'] == expected_runcmd, 'runcmd not passed correctly'


@mock.patch.object(skypilot_config, '_global_config_context',
                   skypilot_config.ConfigContext())
@mock.patch('sky.provision.kubernetes.utils.get_kubernetes_nodes',
            return_value=[])
@mock.patch('sky.utils.common_utils.fill_template',
            wraps=common_utils.fill_template)
@_with_skypilot_config('./tests/test_yamls/test_k8s_config_runcmd.yaml')
def test_write_cluster_config_w_post_provision_runcmd_kubernetes(
        mock_fill_template, *mocks):

    cloud = clouds.Kubernetes()
    region = clouds.Region(name='fake-context')
    resource = Resources(cloud=cloud, instance_type='4CPU--16GB')
    cluster_config_template = 'kubernetes-ray.yml.j2'
    backend_utils.write_cluster_config(
        to_provision=resource,
        num_nodes=2,
        cluster_config_template=cluster_config_template,
        cluster_name='display',
        local_wheel_path=pathlib.Path('/tmp/fake'),
        wheel_hash='b1bd84059bc0342f7843fcbe04ab563e',
        region=region,
        dryrun=True,
        keep_launch_fields_in_existing_config=True)
    expected_runcmd = ['echo "hello world!"']
    mock_fill_template.assert_called_once()
    assert mock_fill_template.call_args[0][
        0] == cluster_config_template, 'config template incorrect'
    assert mock_fill_template.call_args[0][1][
        'runcmd'] == expected_runcmd, 'runcmd not passed correctly'


def _builtin_kubernetes_writer_kwargs(monkeypatch, tmp_path, test_name):
    """Return one hermetic built-in Kubernetes writer invocation."""
    monkeypatch.setenv('SKYPILOT_USER', 'test-user')
    monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, os.devnull)
    monkeypatch.setattr(skypilot_config, '_global_config_context',
                        skypilot_config.ConfigContext())
    skypilot_config.reload_config()
    assert not skypilot_config.loaded()
    monkeypatch.setattr(kubernetes_utils, 'get_kubernetes_nodes',
                        lambda *_args, **_kwargs: [])
    monkeypatch.setattr(kubernetes_utils, 'get_namespace',
                        lambda **_kwargs: 'default')
    monkeypatch.setattr(
        clouds.Kubernetes, '_detect_network_type', lambda *_args, **_kwargs:
        (kubernetes_utils.KubernetesHighPerformanceNetworkType.NONE, None))
    monkeypatch.setattr(clouds.Kubernetes,
                        '_unsupported_features_for_resources',
                        lambda *_args, **_kwargs: {})
    monkeypatch.setattr(backend_utils.auth_utils, 'get_or_generate_keys',
                        lambda: ('/tmp/test-key', 'test-public-key'))
    monkeypatch.setattr(backend_utils.sky_check,
                        'get_cloud_credential_file_mounts', lambda *_args: {})
    monkeypatch.setattr(backend_utils.logs, 'get_logging_agent', lambda: None)
    monkeypatch.setattr('sky.catalog.get_image_id_from_tag',
                        lambda *_args, **_kwargs: 'test-image:latest')
    monkeypatch.setattr(common_utils, 'get_user_hash', lambda: '00000000')
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        lambda: 'default')
    monkeypatch.setattr(backend_utils.sky, '__version__', '1.0.0')
    monkeypatch.setattr(backend_utils, '_deterministic_cluster_yaml_hash',
                        lambda _path: 'test-hash')

    output_path = tmp_path / test_name / 'cluster.yaml'
    monkeypatch.setattr(backend_utils, '_get_yaml_path_from_cluster_name',
                        lambda *_args, **_kwargs: str(output_path))
    return ({
        'to_provision': Resources(cloud=clouds.Kubernetes(),
                                  instance_type='4CPU--16GB'),
        'num_nodes': 2,
        'cluster_config_template': 'kubernetes-ray.yml.j2',
        'cluster_name': 'display',
        'local_wheel_path': pathlib.Path('/tmp/test-wheel'),
        'wheel_hash': 'b1bd84059bc0342f7843fcbe04ab563e',
        'region': clouds.Region(name='test-context'),
        'dryrun': True,
        'keep_launch_fields_in_existing_config': True,
    }, output_path)


def _projected_h200_worker(protocol_version=6):
    return {
        'projection_version': protocol_version,
        'candidate_id': 'kubernetes-0000',
        'kubernetes_context': 'test-context',
        'namespace': 'inference',
        'service_account_name': 'worker-sa',
        'priority_class_name': 'preemptible-inference-low',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'pod_identity_role_arn': 'arn:aws:iam::123456789012:role/skyserve-worker-test',
        'accelerator_name': 'H200',
        'accelerator_count': 1,
        'accelerator_scheduling': {
            'label_key': 'nvidia.com/gpu.product',
            'label_values': ['NVIDIA-H200'],
            'resource_key': 'nvidia.com/gpu',
        },
        'cache': {
            'kind': 'none',
        },
        'scheduler_name': 'default-scheduler',
        'kueue_admission': None,
        'provision_timeout': -1,
        'scratch': {
            'kind': 'none',
        },
    }


def _projected_h200_worker_memory(protocol_version):
    assert protocol_version in (5, 6)
    projection = _projected_h200_worker(protocol_version)
    projection['scratch'] = {
        'kind': 'memory',
        'mount_path': '/tmp',
        'volume_name': 'skypilot-serve-worker-tmp',
        'size_limit_bytes': 20 * 1024**3,
    }
    return projection


def test_builtin_kubernetes_writer_preserves_fill_template_facade(
        monkeypatch, tmp_path):
    writer_kwargs, output_path = _builtin_kubernetes_writer_kwargs(
        monkeypatch, tmp_path, 'direct-facade')
    original_fill_template = common_utils.fill_template
    fill_template = mock.Mock(wraps=original_fill_template)
    safe_load = mock.Mock(wraps=yaml_utils.safe_load)
    monkeypatch.setattr(common_utils, 'fill_template', fill_template)
    monkeypatch.setattr(yaml_utils, 'safe_load', safe_load)

    result = backend_utils.write_cluster_config(**writer_kwargs)

    fill_template.assert_called_once()
    call = fill_template.call_args
    assert call.args[0] == 'kubernetes-ray.yml.j2'
    assert type(call.args[1]) is dict
    assert len(call.args) == 2
    assert call.kwargs == {'output_path': f'{output_path}.tmp'}
    assert result['ray'] == f'{output_path}.tmp'
    safe_load.assert_called_once()


def test_projected_serve_worker_suppresses_all_static_credential_mounts(
        monkeypatch, tmp_path):
    writer_kwargs, _ = _builtin_kubernetes_writer_kwargs(
        monkeypatch, tmp_path, 'projected-worker-no-static-credentials')
    credential_mounts = mock.Mock(
        return_value={
            '~/.aws/credentials': '/server/.aws/credentials',
            '~/.kube/config': '/server/.kube/config',
        })
    logging_agent = mock.MagicMock()
    logging_agent.get_credential_file_mounts.return_value = {
        '~/.logging-agent/credentials': '/server/logging-agent/credentials',
    }
    get_logging_agent = mock.Mock(return_value=logging_agent)
    monkeypatch.setattr(backend_utils.sky_check,
                        'get_cloud_credential_file_mounts', credential_mounts)
    monkeypatch.setattr(backend_utils.logs, 'get_logging_agent',
                        get_logging_agent)
    live_gpu_detection = mock.Mock(side_effect=AssertionError(
        'projected rendering must not inspect live GPU nodes'))
    label_discovery = mock.Mock(side_effect=AssertionError(
        'projected rendering must use frozen accelerator labels'))
    resource_discovery = mock.Mock(side_effect=AssertionError(
        'projected rendering must use the frozen resource key'))
    allocatable_discovery = mock.Mock(side_effect=AssertionError(
        'projected rendering must support zero live capacity'))
    monkeypatch.setattr(kubernetes_utils, 'detect_accelerator_resource',
                        live_gpu_detection)
    monkeypatch.setattr(kubernetes_utils, 'get_accelerator_label_key_values',
                        label_discovery)
    monkeypatch.setattr(kubernetes_utils, 'get_gpu_resource_key',
                        resource_discovery)
    monkeypatch.setattr(kubernetes_utils, 'adjust_resources_to_allocatable',
                        allocatable_discovery)
    monkeypatch.setenv('CUSTOM_GPU_RESOURCE_KEY', 'mutable.example/gpu')
    writer_kwargs['to_provision'] = Resources(
        cloud=clouds.Kubernetes(), instance_type='4CPU--16GB--H200:1')
    writer_kwargs['worker_placement_projections'] = [_projected_h200_worker()]
    original_fill_template = common_utils.fill_template
    rendered_variables = {}

    def capture_variables(template_ref, variables, output_path):
        rendered_variables.update(variables)
        return original_fill_template(template_ref,
                                      variables,
                                      output_path=output_path)

    monkeypatch.setattr(common_utils, 'fill_template', capture_variables)

    result = backend_utils.write_cluster_config(**writer_kwargs)
    rendered = yaml_utils.read_yaml(result['ray'])
    serialized_mounts = json.dumps(rendered.get('file_mounts', {}),
                                   sort_keys=True)
    pod_spec = rendered['available_node_types']['ray_head_default'][
        'node_config']['spec']
    ray_node = next(container for container in pod_spec['containers']
                    if container['name'] == 'ray-node')

    credential_mounts.assert_not_called()
    get_logging_agent.assert_not_called()
    live_gpu_detection.assert_not_called()
    label_discovery.assert_not_called()
    resource_discovery.assert_not_called()
    allocatable_discovery.assert_not_called()
    assert rendered_variables[
        'k8s_projected_serve_worker_runtime_readiness'] is True
    assert rendered['provider'][
        'serve_worker_expected_runtime_bootstrap_sha256'] == (
            kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(
                pod_spec))
    assert pod_spec['affinity']['nodeAffinity'][
        'requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][
            0]['matchExpressions'][-1] == {
                'key': 'nvidia.com/gpu.product',
                'operator': 'In',
                'values': ['NVIDIA-H200'],
            }
    assert ray_node['resources']['requests']['nvidia.com/gpu'] == 1
    assert ray_node['resources']['limits']['nvidia.com/gpu'] == 1
    assert pod_spec['restartPolicy'] == 'Never'
    assert ray_node['startupProbe']['failureThreshold'] == 900
    assert ray_node['readinessProbe']['failureThreshold'] == 1
    runtime_script = ray_node['args'][0]
    runtime_marker = '/tmp/skypilot-serve-worker-runtime-ready'
    assert f'rm -f {runtime_marker}' in runtime_script
    runcmd_boundary = runtime_script.index(
        '# Execute user-provided post-provision runcmd')
    bootstrap_preamble_end = runtime_script.index(
        '# Helper function to conditionally use sudo')
    bootstrap_preamble = runtime_script[:bootstrap_preamble_end]
    first_clear = bootstrap_preamble.index(f'rm -f {runtime_marker}')
    second_clear = bootstrap_preamble.index(f'rm -f {runtime_marker}',
                                            first_clear + 1)
    assert first_clear < runcmd_boundary < second_clear
    assert bootstrap_preamble.rfind('set -e', 0, first_clear) != -1
    assert bootstrap_preamble.rfind('set -e', runcmd_boundary,
                                    second_clear) != -1
    for stale_marker in (runtime_marker, f'{runtime_marker}.tmp',
                         '/tmp/ray_skypilot_runtime_complete',
                         '/tmp/apt_ssh_setup_complete',
                         '/tmp/ray_skypilot_installation_complete',
                         '/tmp/env_setup_complete', '/tmp/apt-ssh-setup.failed',
                         '/tmp/runtime-setup.failed', '/tmp/env-setup.failed',
                         '/tmp/sky_host_network_ports.env'):
        assert stale_marker in bootstrap_preamble[:runcmd_boundary]
        assert stale_marker in bootstrap_preamble[runcmd_boundary:]
    assert 'touch /tmp/ray_skypilot_runtime_complete' in runtime_script
    assert 'printf \'%s\\n\' "$SKYPILOT_POD_UID"' in runtime_script
    assert f'{runtime_marker}.tmp' in runtime_script
    ray_status_capture = runtime_script.index('RAY_START_STATUS=$?')
    ray_completion = runtime_script.index(
        'touch /tmp/ray_skypilot_runtime_complete')
    readiness_publication = runtime_script.index(
        'printf \'%s\\n\' "$SKYPILOT_POD_UID"')
    assert ray_status_capture < ray_completion < readiness_publication
    assert 'exit "$RAY_START_STATUS"' in runtime_script
    subprocess.run(['bash', '-n'],
                   input=runtime_script,
                   text=True,
                   capture_output=True,
                   check=True)

    # A completed install and Ray launch are insufficient: the marker must
    # remain absent until the exact final sshd port returns an SSH banner.
    readiness_start = runtime_script.index(
        '# The historical runtime-install marker precedes Ray start.')
    readiness_end = runtime_script.rindex(runtime_marker) + len(runtime_marker)
    readiness_script = runtime_script[readiness_start:readiness_end]
    readiness_script = readiness_script.replace('/tmp/', f'{tmp_path}/')
    (tmp_path / 'ray_skypilot_runtime_complete').touch()
    with socket.socket() as closed_listener:
        closed_listener.bind(('127.0.0.1', 0))
        closed_port = closed_listener.getsockname()[1]
        (tmp_path / 'sky_host_network_ports.env').write_text(
            f'export SKYPILOT_SSHD_PORT={closed_port}\n', encoding='utf-8')
        completed = subprocess.run(
            [
                'bash', '-c', f'set -e\nSTEPS=(apt runtime env)\n'
                f'{readiness_script}'
            ],
            text=True,
            capture_output=True,
            env={
                **os.environ,
                'SKYPILOT_HOST_NETWORK': '1',
                'SKYPILOT_POD_UID': 'exact-pod-uid',
            },
            check=False)
    assert completed.returncode != 0
    assert 'projected worker sshd listener is not ready' in completed.stdout
    assert not (tmp_path / 'skypilot-serve-worker-runtime-ready').exists()
    assert 'mutable.example/gpu' not in json.dumps(pod_spec)
    assert '.aws' not in serialized_mounts
    assert '.kube' not in serialized_mounts
    assert 'logging-agent' not in serialized_mounts


def test_projected_worker_persists_authenticated_bootstrap_through_finalizer(
        monkeypatch, tmp_path):
    writer_kwargs, _ = _builtin_kubernetes_writer_kwargs(
        monkeypatch, tmp_path, 'projected-worker-authenticated-bootstrap')
    writer_kwargs['to_provision'] = Resources(
        cloud=clouds.Kubernetes(), instance_type='4CPU--16GB--H200:1')
    writer_kwargs['worker_placement_projections'] = [_projected_h200_worker()]
    writer_kwargs['dryrun'] = False

    private_key_path = tmp_path / 'test-key'
    public_key_path = tmp_path / 'test-key.pub'
    private_key_path.write_text('test-private-key', encoding='utf-8')
    public_key = 'ssh-rsa AAAATEST projected-worker@test'
    public_key_path.write_text(public_key, encoding='utf-8')
    monkeypatch.setattr(backend_utils.auth_utils, 'get_or_generate_keys',
                        lambda: (str(private_key_path), str(public_key_path)))
    monkeypatch.setattr(kubernetes_utils,
                        'check_port_forward_mode_dependencies', lambda: None)
    monkeypatch.setattr(kubernetes_utils, 'get_ssh_proxy_command',
                        lambda *_args, **_kwargs: 'test-proxy-command')
    monkeypatch.setattr(backend_utils.global_user_state, 'get_cluster_yaml_str',
                        lambda _path: None)
    persisted = {}
    monkeypatch.setattr(
        backend_utils.global_user_state, 'set_cluster_yaml',
        lambda cluster_name, content: persisted.update({
            'cluster_name': cluster_name,
            'content': content,
        }))
    monkeypatch.setattr(backend_utils, '_optimize_file_mounts',
                        lambda _path: None)
    monkeypatch.setattr(backend_utils.usage_lib.messages.usage,
                        'update_ray_yaml', lambda _path: None)

    backend_utils.write_cluster_config(**writer_kwargs)

    rendered = yaml_utils.safe_load(persisted['content'])
    node_config = rendered['available_node_types']['ray_head_default'][
        'node_config']
    pod_spec = node_config['spec']
    runtime_script = next(container for container in pod_spec['containers']
                          if container['name'] == 'ray-node')['args'][0]
    expected_bootstrap_sha256 = rendered['provider'][
        'serve_worker_expected_runtime_bootstrap_sha256']
    assert persisted['cluster_name'] == 'display'
    assert 'skypilot:ssh_public_key_content' not in runtime_script
    assert public_key in runtime_script
    assert len(runtime_script.encode('utf-8')) == 39363
    assert hashlib.sha256(runtime_script.encode('utf-8')).hexdigest() == (
        '69bc9ab023e8a5ee164d3603f1bdae2fe318e2fc5a128139fcd063da87451704')
    assert expected_bootstrap_sha256 == (
        'b51e7c955e4abffc39c2d882672e2142822f44f2c050a0b3db58db57a7d83c2a')
    assert expected_bootstrap_sha256 == (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))

    finalized = kubernetes_pod_spec.finalize_pod_spec(
        node_config,
        role='head',
        pod_name='projected-worker-head',
        cluster_name_on_cloud='projected-worker',
        node_count=2,
        nvidia_runtime_exists=False,
        needs_gpus=True,
        needs_gpus_nvidia=True,
        gpu_resource_key='nvidia.com/gpu',
        needs_tpu=False,
        resolved_base_affinity=pod_spec.get('affinity'),
        docker_config=None,
        docker_pvc_name=None,
        context='test-context',
        namespace='inference',
    )
    contract = (
        kubernetes_pod_spec.enforce_projected_worker_runtime_readiness_contract(
            finalized['spec'],
            rewrite=False,
            expected_bootstrap_sha256=expected_bootstrap_sha256))
    assert contract.matches


def test_projected_v5_worker_is_rejected_before_renderer(monkeypatch, tmp_path):
    writer_kwargs, output_path = _builtin_kubernetes_writer_kwargs(
        monkeypatch, tmp_path, 'projected-v5-decode-only')
    writer_kwargs['to_provision'] = Resources(
        cloud=clouds.Kubernetes(), instance_type='4CPU--16GB--H200:1')
    writer_kwargs['worker_placement_projections'] = [
        _projected_h200_worker_memory(5)
    ]

    with pytest.raises(ValueError, match='does not satisfy required version 6'):
        backend_utils.write_cluster_config(**writer_kwargs)

    assert not pathlib.Path(f'{output_path}.tmp').exists()


def test_legacy_v5_bootstrap_identity_hash_remains_decode_stable():
    legacy_marker = 'SKYPILOT_SERVE_WORKER_BOOTSTRAP_ENV_V5'
    environment = [{
        'name': key,
        'value': value,
    } for key, value in sorted(
        kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT.items())]
    script_lines = ['canonical bootstrap', f'# {legacy_marker}']
    script_lines.extend(
        f'export {key}={json.dumps(value)}' for key, value in sorted(
            kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT.items()))
    pod_spec = {
        'containers': [{
            'name': 'ray-node',
            'command': ['/bin/bash', '-c', '--'],
            'args': ['\n'.join(script_lines)],
            'env': environment,
        }]
    }

    assert [entry['name'] for entry in environment
           ] == ['SKY_RUNTIME_DIR', 'UV_CACHE_DIR', 'UV_PYTHON_INSTALL_DIR']
    assert (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec)
        == '52fdadc70b46857dd1a7369c3ef20e808168ba0e2f0eb87cba64806b3d265459')
    mutated = copy.deepcopy(pod_spec)
    next(entry for entry in mutated['containers'][0]['env']
         if entry['name'] == 'UV_CACHE_DIR')['value'] = '/root/.cache/uv'
    assert (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(mutated)
        == '8f670f626a292eb2a94440b70aa722761569837aeba69ea2735d22a6198231ed')
    with pytest.raises(
            kubernetes_pod_spec.ProjectedRuntimeReadinessContractError,
            match='Historical'):
        kubernetes_pod_spec.validate_projected_worker_bootstrap_environment(
            pod_spec, {})


def test_projected_v6_bootstrap_env_is_inherited_by_fresh_kubectl_exec(
        monkeypatch, tmp_path):
    writer_kwargs, _ = _builtin_kubernetes_writer_kwargs(
        monkeypatch, tmp_path, 'projected-v6-bootstrap-environment')
    writer_kwargs['to_provision'] = Resources(
        cloud=clouds.Kubernetes(), instance_type='4CPU--16GB--H200:1')
    writer_kwargs['worker_placement_projections'] = [
        _projected_h200_worker_memory(6)
    ]

    result = backend_utils.write_cluster_config(**writer_kwargs)

    rendered = yaml_utils.read_yaml(result['ray'])
    pod_spec = rendered['available_node_types']['ray_head_default'][
        'node_config']['spec']
    ray_node = next(container for container in pod_spec['containers']
                    if container['name'] == 'ray-node')
    expected = kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT
    observed = {
        entry['name']: entry['value']
        for entry in ray_node['env']
        if entry['name'] in expected
    }
    assert observed == expected

    script = ray_node['args'][0]
    runcmd_boundary = script.index(
        '# Execute user-provided post-provision runcmd')
    bootstrap_marker = script.index(
        kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENV_MARKER)
    script_lines = script.splitlines()
    assert script_lines.count(
        f'# {kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENV_MARKER}') == 1
    assert not any(marker in script for marker in kubernetes_pod_spec.
                   SERVE_WORKER_LEGACY_BOOTSTRAP_ENV_MARKERS)
    bootstrap_end = script.index('# Helper function to conditionally use sudo')
    assert runcmd_boundary < bootstrap_marker < bootstrap_end
    for key, value in expected.items():
        assert script_lines.count(f'export {key}={json.dumps(value)}') == 1
    assert rendered['provider'][
        'serve_worker_expected_runtime_bootstrap_sha256'] == (
            kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(
                pod_spec))

    # Kubernetes gives every independent exec process the container's Pod env.
    # SkyPilot setup uses a fresh /bin/bash -c and intentionally adds no
    # process-local prefix that could redirect these three paths to rootfs.
    run_with_log = mock.Mock(return_value=0)
    monkeypatch.setattr(command_runner.log_lib, 'run_with_log', run_with_log)
    runner = command_runner.KubernetesCommandRunner(
        (('inference', 'test-context'), 'projected-worker'),
        container='ray-node')
    monkeypatch.setattr(runner, '_resolve_kubectl_target', lambda:
                        (None, 'test-context', True))
    check = ' && '.join(f'test "${key}" = {json.dumps(value)}'
                        for key, value in expected.items())

    assert runner.run(check, stream_logs=False) == 0

    fresh_exec = run_with_log.call_args.args[0]
    assert 'kubectl exec' in fresh_exec
    assert '-c ray-node -- /bin/bash -c' in fresh_exec
    for key in expected:
        assert f'${key}' in fresh_exec
        assert f'export {key}=' not in fresh_exec


def test_generic_kubernetes_runtime_has_no_projected_readiness_contract(
        monkeypatch, tmp_path):
    writer_kwargs, _ = _builtin_kubernetes_writer_kwargs(
        monkeypatch, tmp_path, 'generic-worker-runtime-readiness')

    result = backend_utils.write_cluster_config(**writer_kwargs)
    rendered = yaml_utils.read_yaml(result['ray'])
    pod_spec = rendered['available_node_types']['ray_head_default'][
        'node_config']['spec']
    ray_node = next(container for container in pod_spec['containers']
                    if container['name'] == 'ray-node')

    assert pod_spec['restartPolicy'] == 'Never'
    assert 'startupProbe' not in ray_node
    assert 'readinessProbe' not in ray_node
    assert '/tmp/skypilot-serve-worker-runtime-ready' not in ray_node['args'][0]


def test_builtin_kubernetes_writer_preserves_delegating_wrapper(
        monkeypatch, tmp_path):
    writer_kwargs, output_path = _builtin_kubernetes_writer_kwargs(
        monkeypatch, tmp_path, 'delegating-wrapper')
    original_fill_template = common_utils.fill_template
    wrapper_calls = []

    def delegating_wrapper(template_ref, variables, output_path):
        wrapper_calls.append((template_ref, variables, output_path))
        return original_fill_template(template_ref, variables, output_path)

    monkeypatch.setattr(common_utils, 'fill_template', delegating_wrapper)

    result = backend_utils.write_cluster_config(**writer_kwargs)

    assert len(wrapper_calls) == 1
    template_ref, variables, rendered_path = wrapper_calls[0]
    assert template_ref == 'kubernetes-ray.yml.j2'
    assert type(variables) is dict
    assert rendered_path == f'{output_path}.tmp'
    assert result['ray'] == rendered_path


def test_builtin_kubernetes_writer_preserves_replacement_renderer_authority(
        monkeypatch, tmp_path):
    writer_kwargs, output_path = _builtin_kubernetes_writer_kwargs(
        monkeypatch, tmp_path, 'replacement-renderer')
    replacement_calls = []

    def replacement_renderer(template_ref, variables, output_path):
        replacement_calls.append((template_ref, variables, output_path))
        if os.path.isabs(template_ref):
            template_path = pathlib.Path(template_ref)
        else:
            template_path = (pathlib.Path(common_utils.__file__).parents[1] /
                             'templates' / template_ref)
        source_bytes = template_path.read_bytes()
        assert hashlib.sha256(source_bytes).hexdigest() == (
            '92f99cd27a606ad121dbd5786c0dd55f07fa68a00fda5581f9b8b0e0e0e3d6b4')
        source = source_bytes.decode('utf-8')
        assert '{{ skypilot_kubernetes_node_config_fragment_v1 }}\n' not in source
        rendered = common_utils.jinja2.Template(source).render(**variables)
        rendered += '\nreplacement_renderer_authoritative: true\n'
        rendered_path = pathlib.Path(output_path)
        rendered_path.parent.mkdir(parents=True, exist_ok=True)
        rendered_path.write_text(rendered, encoding='utf-8')

    monkeypatch.setattr(common_utils, 'fill_template', replacement_renderer)

    result = backend_utils.write_cluster_config(**writer_kwargs)

    assert len(replacement_calls) == 1
    assert replacement_calls[0][0] == 'kubernetes-ray.yml.j2'
    assert replacement_calls[0][2] == f'{output_path}.tmp'
    rendered_config = yaml_utils.read_yaml(result['ray'])
    assert rendered_config['replacement_renderer_authoritative'] is True


def _builtin_do_writer_kwargs(monkeypatch, tmp_path, test_name):
    """Return one hermetic built-in DigitalOcean writer invocation."""
    monkeypatch.setenv('SKYPILOT_USER', 'test-user')
    monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, os.devnull)
    monkeypatch.setattr(skypilot_config, '_global_config_context',
                        skypilot_config.ConfigContext())
    skypilot_config.reload_config()
    assert not skypilot_config.loaded()

    # Keep every rendered local path relative to the isolated pytest root. This
    # makes both the byte oracle and the real config-hash oracle independent of
    # the machine-specific temporary-directory prefix.
    monkeypatch.chdir(tmp_path)
    input_dir = pathlib.Path('do-writer-inputs')
    input_dir.mkdir(exist_ok=True)
    private_key_path = input_dir / 'test-key'
    private_key_path.write_text('test-private-key', encoding='utf-8')
    wheel_path = input_dir / 'sky.whl'
    wheel_path.write_bytes(b'test-wheel')

    monkeypatch.setattr(backend_utils.auth_utils, 'get_or_generate_keys',
                        lambda: (str(private_key_path), 'test-public-key'))
    monkeypatch.setattr(backend_utils.sky_check,
                        'get_cloud_credential_file_mounts', lambda *_args: {})
    monkeypatch.setattr(backend_utils.logs, 'get_logging_agent', lambda: None)
    monkeypatch.setattr(common_utils, 'get_user_hash', lambda: '00000000')
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        lambda: 'default')
    monkeypatch.setattr(backend_utils.sky, '__version__', '1.0.0')

    output_path = pathlib.Path(test_name) / 'cluster.yaml'
    monkeypatch.setattr(backend_utils, '_get_yaml_path_from_cluster_name',
                        lambda *_args, **_kwargs: str(output_path))
    return ({
        'to_provision': Resources(cloud=clouds.DO(),
                                  instance_type='g-2vcpu-8gb'),
        'num_nodes': 2,
        'cluster_config_template': 'do-ray.yml.j2',
        'cluster_name': 'display',
        'local_wheel_path': wheel_path,
        'wheel_hash': 'b1bd84059bc0342f7843fcbe04ab563e',
        'region': clouds.Region(name='nyc1'),
        'dryrun': True,
        'keep_launch_fields_in_existing_config': True,
    }, output_path)


def test_builtin_do_writer_is_byte_and_hash_deterministic(
        monkeypatch, tmp_path):
    writer_kwargs, output_path = _builtin_do_writer_kwargs(
        monkeypatch, tmp_path, 'do-deterministic')

    first_result = backend_utils.write_cluster_config(**writer_kwargs)
    first_bytes = pathlib.Path(first_result['ray']).read_bytes()
    first_real_hash = backend_utils._deterministic_cluster_yaml_hash(
        first_result['ray'])

    second_result = backend_utils.write_cluster_config(**writer_kwargs)
    second_bytes = pathlib.Path(second_result['ray']).read_bytes()
    second_real_hash = backend_utils._deterministic_cluster_yaml_hash(
        second_result['ray'])

    normalized_config = yaml_utils.safe_load(first_bytes.decode('utf-8'))
    setup_commands = normalized_config.pop('setup_commands')

    assert first_result['ray'] == f'{output_path}.tmp'
    assert second_result['ray'] == first_result['ray']
    assert second_bytes == first_bytes
    assert str(tmp_path).encode('utf-8') not in first_bytes
    assert hashlib.sha256(first_bytes).hexdigest() == (
        'afd42bbb1181835d8c12f2e9e5520b3b66bee87710d3513f3c1bbaab685e9787')
    assert normalized_config == {
        'cluster_name': 'display-00000000',
        'max_workers': 1,
        'upscaling_speed': 1,
        'idle_timeout_minutes': 60,
        'provider': {
            'type': 'external',
            'module': 'sky.provision.do',
            'region': 'nyc1',
        },
        'auth': {
            'ssh_user': 'root',
            'ssh_private_key': 'do-writer-inputs/test-key',
            'ssh_public_key': 'skypilot:ssh_public_key_content',
        },
        'available_node_types': {
            'ray_head_default': {
                'resources': {},
                'node_config': {
                    'InstanceType': 'g-2vcpu-8gb',
                    'DiskSize': 256,
                    'ImageId': None,
                },
            },
        },
        'head_node_type': 'ray_head_default',
        'file_mounts': {
            '~/.sky/sky_ray.yml': 'do-deterministic/cluster.yaml.tmp',
            '~/.sky/wheels/b1bd84059bc0342f7843fcbe04ab563e': 'do-writer-inputs/sky.whl',
        },
        'rsync_exclude': [],
        'initialization_commands': [],
    }
    assert len(setup_commands) == 1
    assert hashlib.sha256(setup_commands[0].encode('utf-8')).hexdigest() == (
        'd25c4252af5d93deed9c403d5fab38d58eecc9e50b8064f0e8cc4e235e39f261')
    assert first_result['config_hash'] == first_real_hash
    assert second_result['config_hash'] == second_real_hash
    assert second_result['config_hash'] == first_result['config_hash']
    assert first_result['config_hash'] == (
        '2f5a14c8e3bce79349f48db4984c941493126af4934bd02701d3ad02e5dfa96d')


def test_builtin_do_writer_restores_existing_cluster_before_hash_and_name(
        monkeypatch, tmp_path):
    writer_kwargs, _ = _builtin_do_writer_kwargs(monkeypatch, tmp_path,
                                                 'do-existing-cluster')
    baseline_result = backend_utils.write_cluster_config(**writer_kwargs)
    old_yaml = yaml_utils.read_yaml(baseline_result['ray'])
    old_yaml['cluster_name'] = 'restored-do-name'
    old_yaml['provider']['region'] = 'restored-region'
    old_yaml_content = yaml_utils.dump_yaml_str(old_yaml)

    monkeypatch.setattr(backend_utils.global_user_state, 'get_cluster_yaml_str',
                        lambda _yaml_path: old_yaml_content)
    stored_yaml = {}

    def _capture_cluster_yaml(cluster_name, content):
        stored_yaml['cluster_name'] = cluster_name
        stored_yaml['content'] = content

    monkeypatch.setattr(backend_utils.global_user_state, 'set_cluster_yaml',
                        _capture_cluster_yaml)
    monkeypatch.setattr(backend_utils, '_add_auth_to_cluster_config',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backend_utils, '_optimize_file_mounts',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backend_utils.usage_lib.messages.usage,
                        'update_ray_yaml', lambda *_args, **_kwargs: None)

    real_hash = backend_utils._deterministic_cluster_yaml_hash
    hash_observations = []

    def _recording_real_hash(yaml_path):
        observed_yaml = yaml_utils.read_yaml(yaml_path)
        digest = real_hash(yaml_path)
        hash_observations.append((observed_yaml, digest))
        return digest

    monkeypatch.setattr(backend_utils, '_deterministic_cluster_yaml_hash',
                        _recording_real_hash)
    writer_kwargs['dryrun'] = False

    result = backend_utils.write_cluster_config(**writer_kwargs)

    assert len(hash_observations) == 1
    hashed_yaml, real_restored_hash = hash_observations[0]
    assert hashed_yaml['cluster_name'] == 'restored-do-name'
    assert hashed_yaml['provider']['region'] == 'restored-region'
    assert result['cluster_name_on_cloud'] == 'restored-do-name'
    assert result['config_hash'] == real_restored_hash
    assert stored_yaml['cluster_name'] == 'display'
    assert yaml_utils.safe_load(
        stored_yaml['content'])['cluster_name'] == 'restored-do-name'


@pytest.mark.parametrize(
    ('scenario', 'cloud', 'region_name', 'config_cloud', 'host_network',
     'network_type'), [
         ('kubernetes-host-network-false', clouds.Kubernetes(), 'test-context',
          'kubernetes', False,
          kubernetes_utils.KubernetesHighPerformanceNetworkType.NONE),
         ('kubernetes-host-network-true', clouds.Kubernetes(), 'test-context',
          'kubernetes', True,
          kubernetes_utils.KubernetesHighPerformanceNetworkType.NONE),
         ('kubernetes-oci-roce', clouds.Kubernetes(), 'oci-context',
          'kubernetes', False,
          kubernetes_utils.KubernetesHighPerformanceNetworkType.OCI_ROCE),
         ('ssh-host-network-false', clouds.SSH(), 'ssh-test-context', 'ssh',
          False, kubernetes_utils.KubernetesHighPerformanceNetworkType.NONE),
         ('ssh-host-network-true', clouds.SSH(), 'ssh-test-context', 'ssh',
          True, kubernetes_utils.KubernetesHighPerformanceNetworkType.NONE),
     ])
def test_host_network_probe_is_only_builtin_render_delta(
        scenario, cloud, region_name, config_cloud, host_network, network_type,
        monkeypatch, tmp_path):
    """Locks the full built-in render differential for probe packaging."""
    monkeypatch.setenv('SKYPILOT_USER', 'test-user')
    # Earlier tests in this module exercise SKYPILOT_CONFIG reloads.  The
    # process-wide environment is intentionally mutable, so make this render
    # golden own an explicit empty config instead of depending on xdist order.
    monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, os.devnull)
    monkeypatch.setattr(skypilot_config, '_global_config_context',
                        skypilot_config.ConfigContext())
    skypilot_config.reload_config()
    assert not skypilot_config.loaded()
    monkeypatch.setattr(kubernetes_utils, 'get_kubernetes_nodes',
                        lambda *_args, **_kwargs: [])
    monkeypatch.setattr(kubernetes_utils, 'get_namespace',
                        lambda **_kwargs: 'default')
    monkeypatch.setattr(clouds.Kubernetes, '_detect_network_type',
                        lambda *_args, **_kwargs: (network_type, None))
    monkeypatch.setattr(clouds.Kubernetes,
                        '_unsupported_features_for_resources',
                        lambda *_args, **_kwargs: {})
    monkeypatch.setattr(backend_utils.auth_utils, 'get_or_generate_keys',
                        lambda: ('/tmp/test-key', 'test-public-key'))
    monkeypatch.setattr(backend_utils.sky_check,
                        'get_cloud_credential_file_mounts', lambda *_args: {})
    monkeypatch.setattr(backend_utils.logs, 'get_logging_agent', lambda: None)
    monkeypatch.setattr('sky.catalog.get_image_id_from_tag',
                        lambda *_args, **_kwargs: 'test-image:latest')
    monkeypatch.setattr(common_utils, 'get_user_hash', lambda: '00000000')
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        lambda: 'default')
    monkeypatch.setattr(backend_utils.sky, '__version__', '1.0.0')

    output_path = tmp_path / scenario / 'cluster.yaml'
    monkeypatch.setattr(backend_utils, '_get_yaml_path_from_cluster_name',
                        lambda *_args, **_kwargs: str(output_path))
    overrides = {}
    if host_network:
        overrides = {
            config_cloud: {
                'pod_config': {
                    'spec': {
                        'hostNetwork': True,
                    },
                },
            },
        }
    network_tier = None
    if network_type == (
            kubernetes_utils.KubernetesHighPerformanceNetworkType.OCI_ROCE):
        network_tier = resources_utils.NetworkTier.BEST
    resource = Resources(cloud=cloud,
                         instance_type='4CPU--16GB',
                         network_tier=network_tier,
                         _cluster_config_overrides=overrides)

    current_b64 = ray_commands.host_network_probe_b64()
    source = gzip.decompress(base64.b64decode(current_b64, validate=True))
    legacy_buffer = io.BytesIO()
    with gzip.GzipFile(filename='',
                       mode='wb',
                       fileobj=legacy_buffer,
                       compresslevel=9,
                       mtime=1) as gzip_file:
        gzip_file.write(source)
    legacy_b64 = base64.b64encode(legacy_buffer.getvalue()).decode('ascii')
    legacy_compressed = base64.b64decode(legacy_b64, validate=True)
    current_compressed = base64.b64decode(current_b64, validate=True)
    assert legacy_compressed[:4] == current_compressed[:4]
    assert legacy_compressed[8:] == current_compressed[8:]
    assert legacy_compressed[4:8] == b'\x01\x00\x00\x00'
    assert current_compressed[4:8] == b'\x00\x00\x00\x00'

    def _render(payload: str) -> bytes:
        monkeypatch.setattr(instance_setup, '_host_network_probe_b64',
                            lambda: payload)
        result = backend_utils.write_cluster_config(
            to_provision=resource,
            num_nodes=2,
            cluster_config_template='kubernetes-ray.yml.j2',
            cluster_name='display',
            local_wheel_path=pathlib.Path('/tmp/test-wheel'),
            wheel_hash='b1bd84059bc0342f7843fcbe04ab563e',
            region=clouds.Region(name=region_name),
            dryrun=True,
            keep_launch_fields_in_existing_config=True)
        return pathlib.Path(result['ray']).read_bytes()

    legacy_render = _render(legacy_b64)
    current_render = _render(current_b64)
    legacy_member = legacy_b64.encode('ascii')
    current_member = current_b64.encode('ascii')
    assert legacy_render.count(legacy_member) == 2
    assert current_render.count(current_member) == 2
    assert legacy_render.replace(legacy_member,
                                 current_member) == current_render

    canonical_root = str(tmp_path).encode('utf-8')
    legacy_hash = hashlib.sha256(legacy_render.replace(canonical_root,
                                                       b'<TMP>')).hexdigest()
    current_hash = hashlib.sha256(
        current_render.replace(canonical_root, b'<TMP>')).hexdigest()
    expected_hashes = {
        'kubernetes-host-network-false': (
            '388196c00e5031bdee6d0d54545ffdc0d34a3e8d7415012b5572809cd0fa4ba5',
            '37ecd33134e545581ace328fe4c106d04091c958cd7956cacde1cc2e0f2ed680',
        ),
        'kubernetes-host-network-true': (
            'e7e19f3fd1690e5dedfe94fdbf69282a144e94590f0be76eb82075a9475ada66',
            'b4aa2a197fcb685a88383185644e4cbdb30d882506f56f90cc7177d358b86b9c',
        ),
        'kubernetes-oci-roce': (
            'b078ee6bbe839458e18e0b79c044cba3126d1b803605b4b0b087fbb3b2ed20cc',
            'f64b78f407cc1e35d2cf1201f81657bbf902a84376c5aefcf38744e5ab44ce64',
        ),
        'ssh-host-network-false': (
            '2407f33438cd47f42d8c76e30d1197ed67950c9e51a34bd19049de457188f4ee',
            '433d1e5c2bc88e309d15bda6ba70b5df2fa58daf07dd34d029a57c3ee5d2281f',
        ),
        'ssh-host-network-true': (
            '48945419d3f9ba8f70d9c2637079b743e4af594cfa5478f3611a1eb738f55b04',
            '62caad95d52a724ab821702cd6b9dd52590f63a3f732395b3c58e326fd00c166',
        ),
    }
    assert (legacy_hash, current_hash) == expected_hashes[scenario]


@mock.patch.object(skypilot_config, '_global_config_context',
                   skypilot_config.ConfigContext())
@mock.patch('sky.provision.kubernetes.utils.get_kubernetes_nodes',
            return_value=[])
@mock.patch('sky.utils.common_utils.fill_template',
            wraps=common_utils.fill_template)
def test_write_cluster_config_snapshots_auto_mount_volumes_once(
        mock_fill_template, _mock_nodes, monkeypatch):
    auto_mounts = [{
        'volume_name': 'shared',
        'mount_paths': ['/data'],
    }, {
        'volume_name': 'shared',
        'mount_paths': ['~/cache'],
    }, {
        'volume_name': 'missing',
        'mount_paths': ['/missing'],
    }]

    def _get_config(cloud=None,
                    keys=(),
                    region=None,
                    default_value=None,
                    override_configs=None,
                    merge_dicts=False,
                    **_kwargs):
        del cloud, region, override_configs, merge_dicts
        if keys == ('auto_mounts',):
            return auto_mounts
        return default_value

    volume_config = SimpleNamespace(type='k8s-hostpath',
                                    config={'host_path': '/host/shared'},
                                    name_on_cloud=None,
                                    id_on_cloud=None)
    get_configs = mock.Mock(return_value={'shared': volume_config})
    monkeypatch.setattr(skypilot_config, 'get_effective_region_config',
                        _get_config)
    monkeypatch.setattr('sky.global_user_state.get_volume_configs_by_names',
                        get_configs)
    point_read = mock.Mock(side_effect=AssertionError('point read used'))
    monkeypatch.setattr('sky.global_user_state.get_volume_by_name', point_read)

    cloud = clouds.Kubernetes()
    region = clouds.Region(name='fake-context')
    resource = Resources(cloud=cloud, instance_type='4CPU--16GB')
    backend_utils.write_cluster_config(
        to_provision=resource,
        num_nodes=2,
        cluster_config_template='kubernetes-ray.yml.j2',
        cluster_name='display',
        local_wheel_path=pathlib.Path('/tmp/fake'),
        wheel_hash='b1bd84059bc0342f7843fcbe04ab563e',
        region=region,
        dryrun=True,
        keep_launch_fields_in_existing_config=True)

    get_configs.assert_called_once_with(['shared', 'shared', 'missing'])
    point_read.assert_not_called()
    rendered_mounts = mock_fill_template.call_args.args[1]['volume_mounts']
    assert [
        (mount.name, mount.path, mount.host_path) for mount in rendered_mounts
    ] == [
        ('shared', '/data', '/host/shared'),
        ('shared', '/home/sky/cache', '/host/shared'),
    ]


def test_get_clusters_launch_refresh(monkeypatch):
    # verifies that `get_clusters` works when one cluster is launching
    # and other is not.
    # https://github.com/skypilot-org/skypilot/pull/7624

    def _mock_cluster(launch, postfix=''):
        cluster_name = 'launch-cluster' if launch else 'up-cluster'
        cluster_name += postfix
        handle = mock.MagicMock()
        handle.cluster_name_on_cloud = f'{cluster_name}-cloud'
        handle.launched_nodes = 1
        handle.launched_resources = None

        if launch:
            status = status_lib.ClusterStatus.INIT
        else:
            status = status_lib.ClusterStatus.UP

        return {
            'name': cluster_name,
            'launched_at': '0',
            'handle': handle,
            'last_use': 'sky launch',
            'status': status,
            'autostop': 0,
            'to_down': False,
            'cluster_hash': '00000',
            'cluster_ever_up': not launch,
            'status_updated_at': 0,
            'user_hash': '00000',
            'user_name': 'pilot',
            'workspace': 'default',
            'is_managed': False,
            'nodes': 0,
        }

    def get_clusters_mock(*args, **kwargs):
        return [
            _mock_cluster(False),
            _mock_cluster(True),
            _mock_cluster(True, 'None')
        ]

    def get_readable_resources_repr(handle, simplified_only):
        return ('', None) if simplified_only else ('', '')

    def ssh_credentials_from_handles(handles):
        return []

    def refresh_cluster(cluster_name, force_refresh_statuses, include_user_info,
                        summary_response, cluster_status_lock_timeout):
        assert cluster_status_lock_timeout == 0
        if cluster_name == 'up-cluster':
            return _mock_cluster(False)
        elif cluster_name == 'launch-cluster':
            return _mock_cluster(True)
        else:
            return None

    monkeypatch.setattr('sky.global_user_state.get_clusters', get_clusters_mock)
    monkeypatch.setattr('sky.utils.resources_utils.get_readable_resources_repr',
                        get_readable_resources_repr)
    monkeypatch.setattr(
        'sky.backends.backend_utils.ssh_credentials_from_handles',
        ssh_credentials_from_handles)
    monkeypatch.setattr('sky.backends.backend_utils._refresh_cluster',
                        refresh_cluster)
    monkeypatch.setattr(
        'sky.server.requests.requests.get_request_tasks',
        mock.Mock(side_effect=AssertionError(
            'cluster refresh must not query request tasks')))

    assert len(
        backend_utils.get_clusters(refresh=common.StatusRefreshMode.FORCE)) == 2


@pytest.mark.parametrize(
    'refresh', [common.StatusRefreshMode.NONE, common.StatusRefreshMode.FORCE])
@pytest.mark.parametrize('include_handle', [False, True])
def test_get_clusters_honors_include_handle_for_incomplete_record(
        monkeypatch, refresh, include_handle):
    """An incomplete launch record must still honor the handle contract."""
    handle = mock.MagicMock()
    handle.cluster_name_on_cloud = 'launch-cluster-cloud'
    handle.launched_nodes = 1
    handle.launched_resources = None
    record = {
        'name': 'launch-cluster',
        'handle': handle,
        'status': status_lib.ClusterStatus.INIT,
    }
    get_clusters = mock.Mock(return_value=[record])
    get_resources_repr = mock.Mock(return_value=('', ''))

    monkeypatch.setattr('sky.global_user_state.get_clusters', get_clusters)
    monkeypatch.setattr('sky.utils.resources_utils.get_readable_resources_repr',
                        get_resources_repr)

    refresh_cluster = mock.Mock(return_value=record)
    monkeypatch.setattr('sky.backends.backend_utils._refresh_cluster',
                        refresh_cluster)
    get_request_tasks = mock.Mock(side_effect=AssertionError(
        'cluster refresh must not query request tasks'))
    monkeypatch.setattr('sky.server.requests.requests.get_request_tasks',
                        get_request_tasks)

    [result] = backend_utils.get_clusters(refresh=refresh,
                                          include_handle=include_handle)

    if include_handle:
        assert result['handle'] is handle
    else:
        assert 'handle' not in result
    get_clusters.assert_called_once()
    get_resources_repr.assert_called_once_with(handle, simplified_only=False)
    if refresh == common.StatusRefreshMode.FORCE:
        refresh_cluster.assert_called_once()
        assert refresh_cluster.call_args.kwargs[
            'cluster_status_lock_timeout'] == 0
    else:
        refresh_cluster.assert_not_called()
    get_request_tasks.assert_not_called()


def test_get_glob_clusters_batches_patterns(monkeypatch):
    get_glob_cluster_names = mock.Mock(
        return_value=['train-a', 'train-b', 'serve-a'])
    monkeypatch.setattr('sky.global_user_state.get_glob_cluster_names',
                        get_glob_cluster_names)

    patterns = ['train-*', '*-a', 'serve-*']
    workspaces = {'alpha', 'beta'}
    assert backend_utils._get_glob_clusters(patterns,
                                            workspaces_filter=workspaces) == [
                                                'train-a', 'train-b', 'serve-a'
                                            ]
    get_glob_cluster_names.assert_called_once_with(patterns,
                                                   workspaces_filter=workspaces)


@pytest.mark.parametrize(
    ('requested_workspaces', 'expected_workspaces'),
    [
        (None, {'alpha', 'beta'}),
        (['beta', 'gamma'], {'beta'}),
        (['gamma'], set()),
        ([], set()),
    ],
)
def test_get_clusters_intersects_requested_workspaces_with_accessible_ones(
        monkeypatch, requested_workspaces, expected_workspaces):
    monkeypatch.setattr('sky.workspaces.core.get_accessible_workspace_names',
                        mock.Mock(return_value={'alpha', 'beta'}))
    monkeypatch.setattr('sky.backends.backend_utils._caller_is_viewer',
                        mock.Mock(return_value=False))
    get_clusters = mock.Mock(return_value=[])
    monkeypatch.setattr('sky.global_user_state.get_clusters', get_clusters)

    backend_utils.get_clusters(refresh=common.StatusRefreshMode.NONE,
                               workspaces_filter=requested_workspaces)

    get_clusters.assert_called_once()
    assert (get_clusters.call_args.kwargs['workspaces_filter'] ==
            expected_workspaces)


def test_get_clusters_reuses_effective_workspace_filter_for_globs(monkeypatch):
    monkeypatch.setattr('sky.workspaces.core.get_accessible_workspace_names',
                        mock.Mock(return_value={'alpha', 'beta'}))
    monkeypatch.setattr('sky.backends.backend_utils._caller_is_viewer',
                        mock.Mock(return_value=False))
    get_glob_clusters = mock.Mock(return_value=['alpha-glob'])
    monkeypatch.setattr('sky.backends.backend_utils._get_glob_clusters',
                        get_glob_clusters)
    get_clusters = mock.Mock(return_value=[])
    monkeypatch.setattr('sky.global_user_state.get_clusters', get_clusters)

    backend_utils.get_clusters(refresh=common.StatusRefreshMode.NONE,
                               cluster_names=['direct', 'alpha-*'],
                               workspaces_filter=['alpha', 'inaccessible'])

    get_glob_clusters.assert_called_once_with(['alpha-*'],
                                              workspaces_filter={'alpha'})
    get_clusters.assert_called_once()
    assert get_clusters.call_args.kwargs['workspaces_filter'] == {'alpha'}
    assert get_clusters.call_args.kwargs['cluster_names'] == [
        'direct', 'alpha-glob'
    ]


def test_get_clusters_refresh_enriches_only_final_records(monkeypatch):
    """Refreshed clusters should be enriched from their final records once."""

    def _mock_cluster(name, status, cluster_name_on_cloud):
        handle = mock.MagicMock()
        handle.cluster_name_on_cloud = cluster_name_on_cloud
        handle.launched_nodes = 1
        handle.launched_resources = None
        return {
            'name': name,
            'launched_at': '0',
            'handle': handle,
            'last_use': 'sky launch',
            'status': status,
            'autostop': 0,
            'to_down': False,
            'cluster_hash': '00000',
            'cluster_ever_up': status != status_lib.ClusterStatus.INIT,
            'status_updated_at': 0,
            'user_hash': '00000',
            'user_name': 'pilot',
            'workspace': 'default',
            'is_managed': False,
            'nodes': 0,
        }

    cached_records = [
        _mock_cluster('up-cluster', status_lib.ClusterStatus.UP,
                      'up-cluster-stale-cloud'),
        _mock_cluster('launch-cluster', status_lib.ClusterStatus.INIT,
                      'launch-cluster-cloud'),
        _mock_cluster('gone-cluster', status_lib.ClusterStatus.UP,
                      'gone-cluster-cloud'),
    ]

    resource_calls = []

    def get_readable_resources_repr(handle, simplified_only):
        assert simplified_only is False
        resource_calls.append(handle.cluster_name_on_cloud)
        cloud_name = handle.cluster_name_on_cloud
        return cloud_name, f'{cloud_name}-full'

    def refresh_cluster(cluster_name, force_refresh_statuses, include_user_info,
                        summary_response, cluster_status_lock_timeout):
        del force_refresh_statuses, include_user_info, summary_response
        assert cluster_status_lock_timeout == 0
        if cluster_name == 'up-cluster':
            return _mock_cluster('up-cluster', status_lib.ClusterStatus.UP,
                                 'up-cluster-fresh-cloud')
        if cluster_name == 'launch-cluster':
            return _mock_cluster('launch-cluster',
                                 status_lib.ClusterStatus.INIT,
                                 'launch-cluster-cloud')
        if cluster_name == 'gone-cluster':
            return None
        raise AssertionError(f'unexpected refresh for {cluster_name!r}')

    monkeypatch.setattr('sky.global_user_state.get_clusters',
                        lambda *args, **kwargs: cached_records)
    monkeypatch.setattr('sky.utils.resources_utils.get_readable_resources_repr',
                        get_readable_resources_repr)
    monkeypatch.setattr(
        'sky.backends.backend_utils.ssh_credentials_from_handles',
        lambda handles: [{} for _ in handles])
    monkeypatch.setattr('sky.backends.backend_utils._refresh_cluster',
                        refresh_cluster)
    monkeypatch.setattr(
        'sky.server.requests.requests.get_request_tasks',
        mock.Mock(side_effect=AssertionError(
            'cluster refresh must not query request tasks')))

    records = backend_utils.get_clusters(refresh=common.StatusRefreshMode.FORCE)

    assert [record['name'] for record in records
           ] == ['up-cluster', 'launch-cluster']
    assert records[0]['resources_str'] == 'up-cluster-fresh-cloud'
    assert records[0]['resources_str_full'] == 'up-cluster-fresh-cloud-full'
    assert records[1]['resources_str'] == 'launch-cluster-cloud'
    assert records[1]['resources_str_full'] == 'launch-cluster-cloud-full'
    assert resource_calls == ['up-cluster-fresh-cloud', 'launch-cluster-cloud']


def test_get_clusters_refresh_credentials_only_final_handles(monkeypatch):
    """Credential lookup should only touch final returned handles once."""

    def _mock_cluster(name, status, cluster_name_on_cloud):
        handle = mock.MagicMock()
        handle.cluster_name_on_cloud = cluster_name_on_cloud
        handle.launched_nodes = 1
        handle.launched_resources = None
        return {
            'name': name,
            'launched_at': '0',
            'handle': handle,
            'last_use': 'sky launch',
            'status': status,
            'autostop': 0,
            'to_down': False,
            'cluster_hash': '00000',
            'cluster_ever_up': status != status_lib.ClusterStatus.INIT,
            'status_updated_at': 0,
            'user_hash': '00000',
            'user_name': 'pilot',
            'workspace': 'default',
            'is_managed': False,
            'nodes': 0,
        }

    cached_records = [
        _mock_cluster('up-cluster', status_lib.ClusterStatus.UP,
                      'up-cluster-stale-cloud'),
        _mock_cluster('launch-cluster', status_lib.ClusterStatus.INIT,
                      'launch-cluster-cloud'),
        _mock_cluster('gone-cluster', status_lib.ClusterStatus.UP,
                      'gone-cluster-cloud'),
    ]

    credential_calls = []

    def ssh_credentials_from_handles(handles):
        credential_calls.append(
            [handle.cluster_name_on_cloud for handle in handles])
        return [{} for _ in handles]

    def refresh_cluster(cluster_name, force_refresh_statuses, include_user_info,
                        summary_response, cluster_status_lock_timeout):
        del force_refresh_statuses, include_user_info, summary_response
        assert cluster_status_lock_timeout == 0
        if cluster_name == 'up-cluster':
            return _mock_cluster('up-cluster', status_lib.ClusterStatus.UP,
                                 'up-cluster-fresh-cloud')
        if cluster_name == 'launch-cluster':
            return _mock_cluster('launch-cluster',
                                 status_lib.ClusterStatus.INIT,
                                 'launch-cluster-cloud')
        if cluster_name == 'gone-cluster':
            return None
        raise AssertionError(f'unexpected refresh for {cluster_name!r}')

    monkeypatch.setattr('sky.global_user_state.get_clusters',
                        lambda *args, **kwargs: cached_records)
    monkeypatch.setattr('sky.utils.resources_utils.get_readable_resources_repr',
                        lambda handle, simplified_only: ('', ''))
    monkeypatch.setattr(
        'sky.backends.backend_utils.ssh_credentials_from_handles',
        ssh_credentials_from_handles)
    monkeypatch.setattr('sky.backends.backend_utils._refresh_cluster',
                        refresh_cluster)
    monkeypatch.setattr(
        'sky.server.requests.requests.get_request_tasks',
        mock.Mock(side_effect=AssertionError(
            'cluster refresh must not query request tasks')))
    monkeypatch.setattr('sky.backends.backend_utils._caller_is_viewer',
                        lambda: False)

    records = backend_utils.get_clusters(refresh=common.StatusRefreshMode.FORCE,
                                         include_credentials=True)

    assert [record['name'] for record in records
           ] == ['up-cluster', 'launch-cluster']
    assert credential_calls == [[
        'up-cluster-fresh-cloud', 'launch-cluster-cloud'
    ]]


def test_kubeconfig_upload_with_kubernetes_exclusion():
    """Tests kubeconfig upload behavior with Kubernetes/SSH cloud exclusion.

    This is a regression test for a bug where kubeconfig was uploaded even when
    `remote_identity: SERVICE_ACCOUNT` was set for a Kubernetes cluster. This
    happened because `SSH` inherits from `Kubernetes` and was not being
    explicitly excluded, causing it to upload the kubeconfig.
    """
    # Mock get_credential_file_mounts on Kubernetes to return kubeconfig.
    # SSH inherits from Kubernetes, so it will also return kubeconfig.
    kubeconfig_mounts = {'~/.kube/config': '~/.kube/config'}

    with mock.patch.object(clouds.Kubernetes,
                           'get_credential_file_mounts',
                           return_value=kubeconfig_mounts):
        # 1. Test the buggy behavior: only Kubernetes is excluded.
        # SSH is not excluded, and since it inherits from Kubernetes, it will
        # upload the kubeconfig via the (mocked) inherited method.
        excluded_clouds_buggy = {clouds.Kubernetes()}

        # Mock os.path functions for the credential collection loop
        with mock.patch('os.path.exists', return_value=True), \
             mock.patch('os.path.expanduser', side_effect=lambda x: x), \
             mock.patch('os.path.realpath', side_effect=lambda x: x):
            credentials_buggy = sky_check.get_cloud_credential_file_mounts(
                excluded_clouds_buggy)

        assert '~/.kube/config' in credentials_buggy, (
            'Kubeconfig should be uploaded when only Kubernetes is excluded. '
            'This demonstrates the buggy behavior that the fix in '
            'write_cluster_config() is meant to prevent.')

        # 2. Test the correct behavior: both Kubernetes and SSH are excluded.
        # Kubeconfig should not be in the returned credentials.
        excluded_clouds_fixed = {clouds.Kubernetes(), clouds.SSH()}

        with mock.patch('os.path.exists', return_value=True), \
             mock.patch('os.path.expanduser', side_effect=lambda x: x), \
             mock.patch('os.path.realpath', side_effect=lambda x: x):
            credentials_fixed = sky_check.get_cloud_credential_file_mounts(
                excluded_clouds_fixed)

        assert '~/.kube/config' not in credentials_fixed, (
            'Kubeconfig should not be uploaded when both Kubernetes and SSH '
            'are excluded.')


@mock.patch('sky.backends.backend_utils.get_backend_from_handle')
@mock.patch('sky.backends.backend_utils.refresh_cluster_status_handle')
def test_check_cluster_available_accepts_autostopping(mock_refresh,
                                                      mock_get_backend):
    """Verify check_cluster_available accepts AUTOSTOPPING status."""
    # Mock AUTOSTOPPING cluster
    mock_handle = mock.MagicMock()
    mock_refresh.return_value = (status_lib.ClusterStatus.AUTOSTOPPING,
                                 mock_handle)
    mock_get_backend.return_value = mock.MagicMock()

    # Should not raise ClusterNotUpError for AUTOSTOPPING
    result = backend_utils.check_cluster_available(
        'test-cluster',
        operation='test_operation',
        check_cloud_vm_ray_backend=False)
    assert result == mock_handle


@mock.patch('sky.backends.backend_utils.get_backend_from_handle')
@mock.patch('sky.backends.backend_utils.refresh_cluster_status_handle')
def test_check_cluster_available_rejects_init(mock_refresh, mock_get_backend):
    """Verify check_cluster_available rejects INIT status."""
    mock_handle = mock.MagicMock()
    mock_refresh.return_value = (status_lib.ClusterStatus.INIT, mock_handle)
    mock_get_backend.return_value = mock.MagicMock()

    # Should raise ClusterNotUpError for INIT
    try:
        backend_utils.check_cluster_available('test-cluster',
                                              operation='test_operation',
                                              check_cloud_vm_ray_backend=False)
        assert False, 'Expected ClusterNotUpError to be raised'
    except ClusterNotUpError:
        pass


def _available_cluster_record(handle, status=status_lib.ClusterStatus.UP):
    return {
        'status': status,
        'handle': handle,
        'autostop': -1,
        'to_down': False,
    }


def test_check_cluster_available_uses_refresh_as_only_healthy_read(monkeypatch):
    handle = mock.MagicMock()
    get_record = mock.Mock(return_value=_available_cluster_record(handle))
    refresh = mock.Mock(return_value=(status_lib.ClusterStatus.UP, handle))
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_from_name', get_record)
    monkeypatch.setattr(backend_utils, 'refresh_cluster_status_handle', refresh)
    monkeypatch.setattr(backend_utils, 'get_backend_from_handle', mock.Mock())

    result = backend_utils.check_cluster_available(
        'test-cluster',
        operation='test operation',
        check_cloud_vm_ray_backend=False)

    assert result is handle
    refresh.assert_called_once_with('test-cluster')
    # One pre-refresh snapshot read: it feeds the terminated-during-refresh
    # diagnostics and must not be repeated on the healthy path.
    get_record.assert_called_once_with('test-cluster',
                                       include_user_info=False,
                                       summary_response=True)


def test_check_cluster_available_removed_during_refresh_keeps_diagnostics(
        monkeypatch):
    """A cluster whose row is deleted by the refresh itself (e.g. spot
    preemption discovered on the cloud) must surface the pre-refresh record's
    preempted/autodowned diagnostics rather than a bare does-not-exist."""
    stale_handle = mock.MagicMock()
    get_record = mock.Mock(return_value=_available_cluster_record(stale_handle))
    # Refresh succeeds but observed all nodes terminated: the row is removed
    # and (None, None) is returned.
    refresh = mock.Mock(return_value=(None, None))
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_from_name', get_record)
    monkeypatch.setattr(backend_utils, 'refresh_cluster_status_handle', refresh)

    with pytest.raises(exceptions.ClusterDoesNotExist):
        backend_utils.check_cluster_available('test-cluster',
                                              operation='test operation',
                                              check_cloud_vm_ray_backend=False)

    # The diagnostic branch consults the snapshot's spot/autostop state; a
    # bare "does not exist" branch never touches the handle.
    stale_handle.launched_resources.use_spot.__bool__.assert_called()


@pytest.mark.parametrize('removed', [False, True])
def test_check_cluster_available_refresh_error_uses_current_record(
        monkeypatch, removed):
    stale_handle = mock.MagicMock()
    current_handle = mock.MagicMock()
    refresh_started = False

    def get_record(*_args, **_kwargs):
        if refresh_started:
            if removed:
                return None
            return _available_cluster_record(
                current_handle, status_lib.ClusterStatus.AUTOSTOPPING)
        return _available_cluster_record(stale_handle)

    def refresh(_cluster_name):
        nonlocal refresh_started
        refresh_started = True
        raise exceptions.ClusterStatusFetchingError('provider unavailable')

    get_record_mock = mock.Mock(side_effect=get_record)
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_from_name', get_record_mock)
    monkeypatch.setattr(backend_utils, 'refresh_cluster_status_handle', refresh)
    monkeypatch.setattr(backend_utils, 'get_backend_from_handle', mock.Mock())

    if removed:
        with pytest.raises(exceptions.ClusterDoesNotExist):
            backend_utils.check_cluster_available(
                'test-cluster',
                operation='test operation',
                check_cloud_vm_ray_backend=False)
    else:
        result = backend_utils.check_cluster_available(
            'test-cluster',
            operation='test operation',
            check_cloud_vm_ray_backend=False)
        assert result is current_handle

    # One pre-refresh snapshot read plus one post-failure re-read; the
    # fallback must use the current record, not the stale snapshot.
    assert get_record_mock.call_args_list == [
        mock.call(
            'test-cluster', include_user_info=False, summary_response=True),
    ] * 2


def test_check_cluster_available_dryrun_reads_once_without_refresh(monkeypatch):
    handle = mock.MagicMock()
    get_record = mock.Mock(return_value=_available_cluster_record(handle))
    refresh = mock.Mock()
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_from_name', get_record)
    monkeypatch.setattr(backend_utils, 'refresh_cluster_status_handle', refresh)

    result = backend_utils.check_cluster_available('test-cluster',
                                                   operation='test operation',
                                                   dryrun=True)

    assert result is handle
    get_record.assert_called_once_with('test-cluster',
                                       include_user_info=False,
                                       summary_response=True)
    refresh.assert_not_called()


def _k8s_owner_check_record(owner_identity):
    launchable = mock.MagicMock()
    launchable.cloud = clouds.Kubernetes()
    # `unsafe=True` so the `assert_launchable` attribute (which MagicMock
    # would otherwise guard as a misspelled assert method) is mockable.
    launched_resources = mock.MagicMock(unsafe=True)
    launched_resources.assert_launchable.return_value = launchable

    handle = mock.MagicMock()
    # Make `isinstance(handle, CloudVmRayResourceHandle)` pass without the
    # attribute restrictions that `spec=` imposes (launched_resources is set
    # in __init__, not on the class).
    handle.__class__ = backends.CloudVmRayResourceHandle
    handle.launched_resources = launched_resources
    return {
        'handle': handle,
        'workspace': 'default',
        'owner': owner_identity,
    }


def test_check_owner_identity_k8s_ignores_name_scope(monkeypatch):
    """A pre-scoping owner should still match the current scoped identity.

    Regression: a Kubernetes cluster whose owner was recorded before the
    kubeconfig `__sky__<context>` name-scoping convention existed must keep
    matching the current (scoped) identity instead of raising an owner
    mismatch, and the stored owner should self-heal to the scoped identity.
    """
    # Identity string shape is `<cluster>_<user>_<namespace>`. The scoped
    # variant appends `__sky__<context>` to the cluster and user names.
    old_identity = 'kube-cluster_kube-user_default'
    scoped_identity = ('kube-cluster__sky__my-context_'
                       'kube-user__sky__my-context_default')

    record = _k8s_owner_check_record([old_identity])

    # CI runs unit tests with SKYPILOT_SKIP_CLOUD_IDENTITY_CHECK=1, which would
    # short-circuit the check before our logic runs; ensure it is enabled here.
    monkeypatch.delenv('SKYPILOT_SKIP_CLOUD_IDENTITY_CHECK', raising=False)

    patched = {}

    def fake_set_owner(cluster_name, identity):
        patched['cluster_name'] = cluster_name
        patched['identity'] = identity

    monkeypatch.setattr('sky.skypilot_config.get_active_workspace',
                        lambda: 'default')
    monkeypatch.setattr('sky.global_user_state.set_owner_identity_for_cluster',
                        fake_set_owner)
    monkeypatch.setattr(clouds.Kubernetes, 'get_user_identities',
                        classmethod(lambda cls: [[scoped_identity]]))

    # Should not raise despite the stored owner predating name scoping.
    backend_utils._check_owner_identity_with_record(  # pylint: disable=protected-access
        'my-cluster', record)

    # The stale, pre-scoping owner should self-heal to the scoped identity.
    assert patched['cluster_name'] == 'my-cluster'
    assert patched['identity'] == [scoped_identity]


def test_check_owner_identity_k8s_name_scope_underscored_context(monkeypatch):
    """Scope stripping must work when the context name contains underscores.

    Default GKE contexts look like `gke_<project>_<zone>_<cluster>`, so the
    scope suffix itself carries underscores. A naive `__sky__[^_]*` strip would
    only remove up to the first underscore of the context and still report a
    mismatch. The cluster/user names here are underscore-free so the test
    isolates the underscored-context case.
    """
    ctx = 'gke_my-project_us-central1-a_my-cluster'
    old_identity = 'kube-cluster_kube-user_default'
    scoped_identity = f'kube-cluster__sky__{ctx}_kube-user__sky__{ctx}_default'

    record = _k8s_owner_check_record([old_identity])

    # CI runs unit tests with SKYPILOT_SKIP_CLOUD_IDENTITY_CHECK=1, which would
    # short-circuit the check before our logic runs; ensure it is enabled here.
    monkeypatch.delenv('SKYPILOT_SKIP_CLOUD_IDENTITY_CHECK', raising=False)

    patched = {}

    def fake_set_owner(cluster_name, identity):
        patched['cluster_name'] = cluster_name
        patched['identity'] = identity

    monkeypatch.setattr('sky.skypilot_config.get_active_workspace',
                        lambda: 'default')
    monkeypatch.setattr('sky.global_user_state.set_owner_identity_for_cluster',
                        fake_set_owner)
    monkeypatch.setattr(clouds.Kubernetes, 'get_user_identities',
                        classmethod(lambda cls: [[scoped_identity]]))

    backend_utils._check_owner_identity_with_record(  # pylint: disable=protected-access
        'my-cluster', record)

    assert patched['identity'] == [scoped_identity]


def test_check_owner_identity_k8s_scope_does_not_overmatch(monkeypatch):
    """Stripping scope suffixes must not let a different identity match."""
    owner_identity = ['ctx-a_user-a_default']
    # Different cluster/user; normalizing the scope suffix still leaves it
    # distinct from the stored owner.
    other_scoped = 'ctx-b__sky__ctx-b_user-b__sky__ctx-b_default'

    record = _k8s_owner_check_record(owner_identity)

    # CI runs unit tests with SKYPILOT_SKIP_CLOUD_IDENTITY_CHECK=1, which would
    # short-circuit the check before our logic runs; ensure it is enabled here.
    monkeypatch.delenv('SKYPILOT_SKIP_CLOUD_IDENTITY_CHECK', raising=False)

    monkeypatch.setattr('sky.skypilot_config.get_active_workspace',
                        lambda: 'default')
    monkeypatch.setattr('sky.global_user_state.set_owner_identity_for_cluster',
                        lambda *a, **k: None)
    monkeypatch.setattr(clouds.Kubernetes, 'get_user_identities',
                        classmethod(lambda cls: [[other_scoped]]))

    with pytest.raises(exceptions.ClusterOwnerIdentityMismatchError):
        backend_utils._check_owner_identity_with_record(  # pylint: disable=protected-access
            'my-cluster', record)


@mock.patch('sky.backends.backend_utils.refresh_cluster_status_handle')
def test_is_controller_accessible_accepts_autostopping(mock_refresh,
                                                       monkeypatch):
    """Verify is_controller_accessible accepts AUTOSTOPPING status."""

    mock_handle = mock.MagicMock()
    mock_handle.head_ip = '1.2.3.4'
    mock_refresh.return_value = (status_lib.ClusterStatus.AUTOSTOPPING,
                                 mock_handle)
    monkeypatch.setattr(backend_utils.managed_job_utils,
                        'is_consolidation_mode', lambda: False)
    monkeypatch.setattr(backend_utils.serve_utils, 'is_consolidation_mode',
                        lambda: False)

    # Should not raise for AUTOSTOPPING controller
    result = backend_utils.is_controller_accessible(
        controller_utils.Controllers.JOBS_CONTROLLER,
        stopped_message='Test stopped',
        exit_if_not_accessible=False)
    mock_refresh.assert_called_once()
    assert result == mock_handle


def test_replace_yaml_dicts_restores_new_nested_field_for_legacy_cluster():
    """Restarting a cluster created before a nested provider field was added.

    Regression test for the Nebius `KeyError: 'security_group'` seen when
    restarting a STOPPED cluster after upgrading to a version that added
    `provider.security_group`. The old (stored) yaml's `provider` block is
    restored wholesale and lacks `security_group`, so reverting the
    `('provider', 'security_group', 'GroupName')` exception must not assume
    the intermediate key exists.
    """
    new_yaml = ('cluster_name: c\n'
                'provider:\n'
                '  type: external\n'
                '  region: r\n'
                '  security_group:\n'
                '    GroupName: new-name\n'
                '    ManagedBySkyPilot: true\n'
                'auth: {ssh_user: ubuntu}\n'
                'node_config: {InstanceType: t}\n')
    # Old yaml predates the security_group feature: no such key under provider.
    old_yaml = ('cluster_name: c\n'
                'provider:\n'
                '  type: external\n'
                '  region: r\n'
                'auth: {ssh_user: ubuntu}\n'
                'node_config: {InstanceType: t}\n')

    out = backend_utils._replace_yaml_dicts(
        new_yaml, old_yaml,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_FOR_BACK_COMPATIBILITY,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_EXCEPTIONS)
    result = yaml_utils.read_yaml_str(out)
    # The new GroupName is applied even though the restored provider block
    # had no security_group; no KeyError is raised.
    assert result['provider']['security_group']['GroupName'] == 'new-name'


def test_replace_yaml_dicts_preserves_old_subfield_on_restart():
    """Existing cluster restart keeps old sibling subfields, new GroupName."""
    new_yaml = ('cluster_name: c\n'
                'provider:\n'
                '  type: external\n'
                '  region: r\n'
                '  security_group:\n'
                '    GroupName: new-name\n'
                '    ManagedBySkyPilot: true\n'
                'auth: {ssh_user: ubuntu}\n'
                'node_config: {InstanceType: t}\n')
    old_yaml = ('cluster_name: c\n'
                'provider:\n'
                '  type: external\n'
                '  region: r\n'
                '  security_group:\n'
                '    GroupName: old-name\n'
                '    ManagedBySkyPilot: false\n'
                'auth: {ssh_user: ubuntu}\n'
                'node_config: {InstanceType: t}\n')

    out = backend_utils._replace_yaml_dicts(
        new_yaml, old_yaml,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_FOR_BACK_COMPATIBILITY,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_EXCEPTIONS)
    sg = yaml_utils.read_yaml_str(out)['provider']['security_group']
    # GroupName is an exception -> taken from new yaml.
    assert sg['GroupName'] == 'new-name'
    # ManagedBySkyPilot is not an exception -> restored from old yaml.
    assert sg['ManagedBySkyPilot'] is False


def test_replace_yaml_dicts_restores_new_nested_field_when_old_is_null():
    """Old yaml has the intermediate key present but null (e.g. `key:`).

    `dict.setdefault(key, {})` would return the existing None here, so the
    revert must explicitly treat a non-dict intermediate as absent and
    rebuild the path rather than crashing.
    """
    new_yaml = ('cluster_name: c\n'
                'provider:\n'
                '  type: external\n'
                '  region: r\n'
                '  security_group:\n'
                '    GroupName: new-name\n'
                '    ManagedBySkyPilot: true\n'
                'auth: {ssh_user: ubuntu}\n'
                'node_config: {InstanceType: t}\n')
    # `security_group:` with no value parses to None.
    old_yaml = ('cluster_name: c\n'
                'provider:\n'
                '  type: external\n'
                '  region: r\n'
                '  security_group:\n'
                'auth: {ssh_user: ubuntu}\n'
                'node_config: {InstanceType: t}\n')

    out = backend_utils._replace_yaml_dicts(
        new_yaml, old_yaml,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_FOR_BACK_COMPATIBILITY,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_EXCEPTIONS)
    result = yaml_utils.read_yaml_str(out)
    assert result['provider']['security_group']['GroupName'] == 'new-name'


def test_stopped_restart_uses_rotated_managed_vm_image_and_auth():
    """Managed restart keeps the new endpoint and removes obsolete login."""
    digest = 'sha256:' + 'a' * 64
    old_ref = f'old.example/skypilot/image@{digest}'
    new_ref = f'new.example/skypilot/image@{digest}'
    new_yaml = ('cluster_name: c\n'
                'docker:\n'
                f'  image: {new_ref}\n'
                '  run_options: [--ipc=host]\n'
                'provider: {type: external}\n'
                'node_config: {InstanceType: t}\n')
    old_yaml = ('cluster_name: c\n'
                'docker:\n'
                f'  image: {old_ref}\n'
                '  docker_login_config:\n'
                '    username: AWS\n'
                '    password: old-token\n'
                '    server: old.example\n'
                'provider: {type: external}\n'
                'node_config: {InstanceType: t}\n')

    restored = backend_utils._replace_yaml_dicts(
        new_yaml, old_yaml,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_FOR_BACK_COMPATIBILITY,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_EXCEPTIONS)
    out = backend_utils._restore_managed_container_image_fields(
        new_yaml, restored, new_ref)
    result = yaml_utils.read_yaml_str(out)
    assert result['docker']['image'] == new_ref
    assert 'docker_login_config' not in result['docker']


def test_stopped_restart_updates_only_managed_kubernetes_containers():
    """Kubernetes restore overlays named workload containers, not sidecars."""
    digest = 'sha256:' + 'b' * 64
    old_ref = f'old.example/skypilot/image@{digest}'
    new_ref = f'new.example/skypilot/image@{digest}'
    new_yaml = ('cluster_name: c\n'
                'available_node_types:\n'
                '  ray_head_default:\n'
                '    node_config:\n'
                '      spec:\n'
                '        containers:\n'
                '        - {name: ray-node, image: ' + new_ref + '}\n'
                '        - {name: metrics, image: metrics:new}\n'
                '        - {name: newly-added, image: ' + new_ref + '}\n')
    old_yaml = ('cluster_name: c\n'
                'available_node_types:\n'
                '  ray_head_default:\n'
                '    node_config:\n'
                '      spec:\n'
                '        containers:\n'
                '        - {name: metrics, image: metrics:old}\n'
                '        - {name: ray-node, image: ' + old_ref + '}\n')

    restored = backend_utils._replace_yaml_dicts(
        new_yaml, old_yaml,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_FOR_BACK_COMPATIBILITY,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_EXCEPTIONS)
    out = backend_utils._restore_managed_container_image_fields(
        new_yaml, restored, new_ref)
    containers = yaml_utils.read_yaml_str(out)['available_node_types'][
        'ray_head_default']['node_config']['spec']['containers']
    by_name = {container['name']: container['image'] for container in containers}
    assert by_name == {'metrics': 'metrics:old', 'ray-node': new_ref}


def test_managed_kubernetes_image_overrides_pod_config_image():
    """A pod override cannot diverge runtime bytes from the catalog fence."""
    digest = 'sha256:' + 'c' * 64
    managed_ref = f'managed.example/skypilot/image@{digest}'
    config = yaml_utils.read_yaml_str(
        'available_node_types:\n'
        '  ray_head_default:\n'
        '    node_config:\n'
        '      spec:\n'
        '        containers:\n'
        '        - {name: ray-node, image: hidden-override:latest}\n'
        '        - {name: metrics, image: metrics:v1}\n'
        '      deployment_spec:\n'
        '        spec:\n'
        '          template:\n'
        '            spec:\n'
        '              initContainers:\n'
        '              - {name: init-copy-home, image: hidden-override:latest}\n'
        '              - {name: another-init, image: helper:v1}\n')

    backend_utils._enforce_managed_kubernetes_image(config, managed_ref)
    node_config = config['available_node_types']['ray_head_default'][
        'node_config']
    containers = node_config['spec']['containers']
    assert {
        item['name']: item['image'] for item in containers
    } == {
        'ray-node': managed_ref,
        'metrics': 'metrics:v1',
    }
    init_containers = node_config['deployment_spec']['spec']['template'][
        'spec']['initContainers']
    assert {
        item['name']: item['image'] for item in init_containers
    } == {
        'init-copy-home': managed_ref,
        'another-init': 'helper:v1',
    }


def test_managed_kubernetes_image_requires_active_head_node_container():
    digest = 'sha256:' + 'd' * 64
    managed_ref = f'managed.example/skypilot/image@{digest}'
    config = {
        'head_node_type': 'active',
        'available_node_types': {
            'ray_head_default': {
                'node_config': {
                    'spec': {
                        'containers': [{
                            'name': 'ray-node',
                            'image': 'unused:latest',
                        }],
                    },
                },
            },
            'active': {
                'node_config': {
                    'spec': {
                        'containers': [{
                            'name': 'workload',
                            'image': 'divergent:latest',
                        }],
                    },
                },
            },
        },
    }
    with pytest.raises(exceptions.InvalidCloudConfigs,
                       match='actively provisioned head node type'):
        backend_utils._enforce_managed_kubernetes_image(config, managed_ref)
    assert config['available_node_types']['active']['node_config']['spec'][
        'containers'][0]['image'] == 'divergent:latest'

    config['available_node_types']['active']['node_config']['spec'][
        'containers'] = [{
            'name': 'ray-node',
            'image': 'divergent:latest',
        }]
    backend_utils._enforce_managed_kubernetes_image(config, managed_ref)
    assert config['available_node_types']['active']['node_config']['spec'][
        'containers'][0]['image'] == managed_ref


def test_managed_kubernetes_image_enforces_qualified_node_selector():
    digest = 'sha256:' + 'e' * 64
    managed_ref = f'managed.example/skypilot/image@{digest}'
    config = yaml_utils.read_yaml_str(
        'available_node_types:\n'
        '  ray_head_default:\n'
        '    node_config:\n'
        '      spec:\n'
        '        nodeSelector: {existing: value}\n'
        '        containers:\n'
        '        - {name: ray-node, image: old:latest}\n'
        '      deployment_spec:\n'
        '        spec:\n'
        '          template:\n'
        '            spec:\n'
        '              initContainers:\n'
        '              - {name: init-copy-home, image: old:latest}\n')
    selector = (('kubernetes.io/arch', 'amd64'), ('skypilot.co/image-pull-role',
                                                  'eks-node'))

    backend_utils._enforce_managed_kubernetes_image(config, managed_ref,
                                                    selector)

    node_config = config['available_node_types']['ray_head_default'][
        'node_config']
    assert node_config['spec']['nodeSelector'] == {
        'existing': 'value',
        'kubernetes.io/arch': 'amd64',
        'skypilot.co/image-pull-role': 'eks-node',
    }
    assert node_config['deployment_spec']['spec']['template']['spec'][
        'nodeSelector'] == {
            'kubernetes.io/arch': 'amd64',
            'skypilot.co/image-pull-role': 'eks-node',
        }


def test_managed_kubernetes_image_rejects_conflicting_qualified_selector():
    digest = 'sha256:' + 'f' * 64
    managed_ref = f'managed.example/skypilot/image@{digest}'
    config = yaml_utils.read_yaml_str(
        'available_node_types:\n'
        '  ray_head_default:\n'
        '    node_config:\n'
        '      spec:\n'
        '        nodeSelector:\n'
        '          skypilot.co/image-pull-role: other\n'
        '        containers:\n'
        '        - {name: ray-node, image: old:latest}\n')

    with pytest.raises(exceptions.InvalidCloudConfigs,
                       match='qualification conflicts'):
        backend_utils._enforce_managed_kubernetes_image(
            config, managed_ref, (('kubernetes.io/arch', 'amd64'),
                                  ('skypilot.co/image-pull-role', 'eks-node')))


def test_make_safe_symlink_command_default_uses_sudo():
    """By default the privileged steps are prefixed with sudo."""
    cmd = backend_utils.FileMountHelper.make_safe_symlink_command(
        source='/etc/config', target='/home/user/.sky/etc/config')
    assert 'sudo mkdir -p /etc' in cmd
    assert 'sudo ln -s /home/user/.sky/etc/config /etc/config' in cmd


def test_make_safe_symlink_command_empty_sudo_cmd_omits_sudo():
    """Passing sudo_cmd='' drops the prefix so the command does not depend on
    a sudo binary (e.g. a container already running as root)."""
    cmd = backend_utils.FileMountHelper.make_safe_symlink_command(
        source='/etc/config', target='/home/user/.sky/etc/config', sudo_cmd='')
    assert 'sudo' not in cmd
    assert cmd.startswith('mkdir -p /etc')
    assert 'ln -s /home/user/.sky/etc/config /etc/config' in cmd


def test_make_safe_symlink_command_leaves_target_unquoted():
    """The target is interpolated unquoted so a leading ~ still expands to
    $HOME at runtime -- the wrapped file-mount dir starts with ~/."""
    cmd = backend_utils.FileMountHelper.make_safe_symlink_command(
        source='/etc/config', target='~/.sky/file_mounts/etc/config')
    assert 'ln -s ~/.sky/file_mounts/etc/config /etc/config' in cmd
    assert '\'~/.sky/file_mounts/etc/config\'' not in cmd
