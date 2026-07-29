"""Unit tests for backend utility helpers."""

# pylint: disable=protected-access,unused-argument

import os
import pathlib
from types import SimpleNamespace
from unittest import mock

import pytest

from sky import backends
from sky import check as sky_check
from sky import clouds
from sky import exceptions
from sky import skypilot_config
from sky.backends import backend_utils
from sky.exceptions import ClusterNotUpError
from sky.provision import docker_utils
from sky.resources import Resources
from sky.utils import common
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import status_lib
from sky.utils import yaml_utils


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
def test_write_cluster_config_w_remote_identity(mock_fill_template,
                                                *mocks) -> None:
    os.environ[
        skypilot_config.
        ENV_VAR_SKYPILOT_CONFIG] = './tests/test_yamls/test_aws_config.yaml'
    skypilot_config.reload_config()

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
def test_write_cluster_config_w_post_provision_runcmd_aws(
        mock_fill_template, *mocks):
    os.environ[skypilot_config.ENV_VAR_SKYPILOT_CONFIG] = (
        './tests/test_yamls/test_aws_config_runcmd.yaml')
    skypilot_config.reload_config()

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
def test_write_cluster_config_w_post_provision_runcmd_kubernetes(
        mock_fill_template, *mocks):
    os.environ[skypilot_config.ENV_VAR_SKYPILOT_CONFIG] = (
        './tests/test_yamls/test_k8s_config_runcmd.yaml')
    skypilot_config.reload_config()

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
