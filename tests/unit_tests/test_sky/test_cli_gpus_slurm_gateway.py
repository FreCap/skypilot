"""Characterization tests for the GPU CLI Slurm gateway."""

from unittest import mock

from click.testing import CliRunner

from sky import clouds
from sky.client import sdk
from sky.client.cli import command
from sky.utils import registry


def _invoke_verbose_slurm(stream_and_get):
    runner = CliRunner()
    with mock.patch.object(registry.CLOUD_REGISTRY,
                           'from_str',
                           return_value=clouds.Slurm()), \
         mock.patch.object(sdk, 'enabled_clouds', return_value='enabled'), \
         mock.patch.object(sdk, 'get', return_value=['slurm']), \
         mock.patch.object(
             sdk,
             'realtime_slurm_gpu_availability',
             return_value='availability'), \
         mock.patch.object(
             sdk,
             'slurm_node_info',
             side_effect=lambda slurm_cluster_name: f'nodes-{slurm_cluster_name}'
         ) as node_info, \
         mock.patch.object(
             sdk, 'stream_and_get', side_effect=stream_and_get) as stream:
        result = runner.invoke(command.gpus_list, ['--cloud', 'slurm', '-v'])
    return result, node_info, stream


def test_slurm_gateway_preserves_compatibility_and_partial_failures():
    availability = [
        ('legacy', [('A100', [1, 2, 4], 8, 6)]),
        ('modern', [('A100', [1, 2], 4, 1)], None),
        ('failed', [], 'scheduler unavailable'),
    ]
    legacy_nodes = [{
        'partition': 'gpu',
        'gpu_type': 'A100',
        'total_gpus': 8,
        'free_gpus': 6,
    }]
    modern_nodes = [{
        'partition': 'batch',
        'gpu_type': 'A100',
        'total_gpus': 4,
        'free_gpus': 1,
    }]

    result, node_info, stream = _invoke_verbose_slurm(
        [availability, legacy_nodes, modern_nodes])

    assert result.exit_code == 0, result.output
    assert '7 of 12 free' in result.output
    assert 'Slurm Cluster: legacy' in result.output
    assert 'Slurm Cluster: modern' in result.output
    assert 'Slurm Cluster: failed' in result.output
    assert 'Error: scheduler unavailable' in result.output
    assert 'Slurm per-partition accelerator availability' in result.output
    assert node_info.call_args_list == [
        mock.call(slurm_cluster_name='legacy'),
        mock.call(slurm_cluster_name='modern'),
    ]
    assert stream.call_args_list == [
        mock.call('availability'),
        mock.call('nodes-legacy'),
        mock.call('nodes-modern'),
    ]


def test_slurm_gateway_dispatches_node_requests_before_partial_wait_failure():
    events = []
    availability = [
        ('alpha', [('A100', [1], 2, 1)]),
        ('beta', [('A100', [1], 2, 2)]),
    ]

    def stream_and_get(request_id):
        if request_id == 'availability':
            return availability
        events.append(f'wait:{request_id}')
        assert events[:2] == ['dispatch:alpha', 'dispatch:beta']
        if request_id == 'nodes-alpha':
            raise RuntimeError('alpha unreachable')
        return [{
            'partition': 'gpu',
            'gpu_type': 'A100',
            'total_gpus': 2,
            'free_gpus': 2,
        }]

    def node_info(slurm_cluster_name):
        events.append(f'dispatch:{slurm_cluster_name}')
        return f'nodes-{slurm_cluster_name}'

    runner = CliRunner()
    with mock.patch.object(registry.CLOUD_REGISTRY,
                           'from_str',
                           return_value=clouds.Slurm()), \
         mock.patch.object(sdk, 'enabled_clouds', return_value='enabled'), \
         mock.patch.object(sdk, 'get', return_value=['slurm']), \
         mock.patch.object(
             sdk,
             'realtime_slurm_gpu_availability',
             return_value='availability'), \
         mock.patch.object(sdk, 'slurm_node_info', side_effect=node_info), \
         mock.patch.object(sdk,
                           'stream_and_get',
                           side_effect=stream_and_get):
        result = runner.invoke(command.gpus_list, ['--cloud', 'slurm', '-v'])

    assert result.exit_code == 0, result.output
    assert events == [
        'dispatch:alpha',
        'dispatch:beta',
        'wait:nodes-alpha',
        'wait:nodes-beta',
    ]
    assert 'skipped unreachable clusters: alpha' in result.output
    assert 'beta' in result.output
