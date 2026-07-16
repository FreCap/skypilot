"""Characterization tests for real-time accelerator availability."""

import inspect
import pickle
from unittest import mock

import pytest

from sky import catalog
from sky import clouds
from sky import core
from sky import models


def test_realtime_kubernetes_gpu_availability_normalizes_catalog_results():

    def query_catalog(**kwargs):
        context = kwargs['region_filter']
        if context == 'context-a':
            return ({
                'A100': [1, 2]
            }, {
                'A100': 8,
                'H100': 4
            }, {
                'A100': 6,
                'H100': 3
            })
        assert context == 'context-b'
        return ({'L4': [1]}, {'L4': 4}, {'L4': 2})

    with mock.patch.object(
            clouds.Kubernetes,
            'existing_allowed_contexts',
            return_value=['context-a', 'context-b']), \
         mock.patch.object(clouds.SSH,
                           'existing_allowed_contexts',
                           return_value=['ssh-a']), \
         mock.patch.object(catalog,
                           'list_accelerator_realtime',
                           side_effect=query_catalog) as mock_list:
        result = core.realtime_kubernetes_gpu_availability(name_filter='gpu',
                                                           quantity_filter=2,
                                                           is_ssh=False)

    assert result == [
        ('context-a', [
            models.RealtimeGpuAvailability('A100', [1, 2], 8, 6),
            models.RealtimeGpuAvailability('H100', [], 4, 3),
        ]),
        ('context-b', [
            models.RealtimeGpuAvailability('L4', [1], 4, 2),
        ]),
    ]
    assert sorted(mock_list.call_args_list,
                  key=lambda call: call.kwargs['region_filter']) == [
                      mock.call(gpus_only=True,
                                clouds='kubernetes',
                                name_filter='gpu',
                                region_filter='context-a',
                                quantity_filter=2,
                                case_sensitive=False),
                      mock.call(gpus_only=True,
                                clouds='kubernetes',
                                name_filter='gpu',
                                region_filter='context-b',
                                quantity_filter=2,
                                case_sensitive=False),
                  ]


def test_realtime_kubernetes_gpu_availability_preserves_no_gpu_error():
    with mock.patch.object(catalog,
                           'list_accelerator_realtime',
                           return_value=({}, {}, {})) as mock_list:
        with pytest.raises(
                ValueError,
                match=
            ("Resources 'A100' with requested quantity 8 not found in SSH "
             'clusters. .*sky gpus list --cloud ssh')):
            core.realtime_kubernetes_gpu_availability(context='ssh-a',
                                                      name_filter='A100',
                                                      quantity_filter=8,
                                                      is_ssh=True)

    mock_list.assert_called_once_with(gpus_only=True,
                                      clouds='ssh',
                                      name_filter='A100',
                                      region_filter='ssh-a',
                                      quantity_filter=8,
                                      case_sensitive=False)


def test_realtime_slurm_gpu_availability_isolates_cluster_failures():

    def query_catalog(**kwargs):
        cluster = kwargs['region_filter']
        if cluster == 'slurm-empty':
            raise ValueError('no matching GPUs')
        if cluster == 'slurm-error':
            raise RuntimeError('scheduler unavailable')
        return ({'A100': [1, 2]}, {'A100': 8}, {'A100': 5})

    with mock.patch.object(clouds.Slurm,
                           'existing_allowed_clusters',
                           return_value=[
                               'slurm-ok', 'slurm-empty', 'slurm-error'
                           ]), \
         mock.patch.object(catalog,
                           'list_accelerator_realtime',
                           side_effect=query_catalog) as mock_list:
        result = core.realtime_slurm_gpu_availability(name_filter='A100',
                                                      quantity_filter=2)

    assert result == [
        ('slurm-ok', [
            models.RealtimeGpuAvailability('A100', [1, 2], 8, 5),
        ], None),
        ('slurm-error', [],
         'Could not query Slurm cluster for info: RuntimeError: scheduler '
         'unavailable'),
    ]
    assert mock_list.call_count == 3
    assert sorted(
        call.kwargs['region_filter'] for call in mock_list.call_args_list) == [
            'slurm-empty', 'slurm-error', 'slurm-ok'
        ]


@pytest.mark.parametrize(('function_name', 'parameter_names'), [
    ('realtime_kubernetes_gpu_availability',
     ['context', 'name_filter', 'quantity_filter', 'is_ssh']),
    ('realtime_slurm_gpu_availability', [
        'slurm_cluster_name', 'name_filter', 'quantity_filter', 'env_vars',
        'kwargs'
    ]),
])
def test_realtime_availability_core_facade_identity(function_name,
                                                    parameter_names):
    function = getattr(core, function_name)

    assert function.__module__ == 'sky.core'
    assert pickle.loads(pickle.dumps(function)) is function

    assert list(inspect.signature(function).parameters) == parameter_names
