"""get_endpoints must raise ClusterNotUpError when the head IP is unknown.

A cluster record can transiently be UP with handle.head_ip=None
(mid-provision, or a recovered serve controller re-driving a launch).
query_ports_passthrough asserts head_ip is not None, and that
AssertionError used to escape get_endpoints and kill the entire serve
status query at fleet scale. ClusterNotUpError is the contract callers
already handle (ReplicaInfo.url returns None on it).
"""
from types import SimpleNamespace
from unittest import mock

import pytest

from sky import backends
from sky import exceptions
from sky.backends import backend_utils
from sky.utils import status_lib


def _up_record_with_headless_handle():
    handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
    handle.head_ip = None
    return {
        'name': 'svc-replica-1',
        'status': status_lib.ClusterStatus.UP,
        'handle': handle,
    }


def test_get_endpoints_raises_cluster_not_up_when_head_ip_missing():
    record = _up_record_with_headless_handle()
    with mock.patch.object(backend_utils, 'get_clusters',
                           return_value=[record]):
        with pytest.raises(exceptions.ClusterNotUpError):
            backend_utils.get_endpoints(record['name'], port=8080)


def test_get_endpoints_reuses_supplied_cluster_record():
    handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
    cloud = mock.MagicMock()
    launched_resources = SimpleNamespace(cloud=cloud, ports='8080')
    launched_resources.assert_launchable = lambda: launched_resources
    handle.head_ip = '1.2.3.4'
    handle.cluster_name_on_cloud = 'svc-replica-1-cloud'
    handle.cluster_yaml = '/tmp/svc-replica-1.yaml'
    handle.launched_resources = launched_resources
    record = {
        'name': 'svc-replica-1',
        'status': status_lib.ClusterStatus.UP,
        'handle': handle,
    }

    with mock.patch.object(
            backend_utils,
            'get_clusters',
            side_effect=AssertionError('must reuse supplied cluster_record')), \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_yaml_dict',
                           return_value={'provider': {}}), \
         mock.patch.object(backend_utils.provision_lib,
                           'query_ports',
                           return_value={
                               8080: [mock.Mock(url=lambda: '1.2.3.4:8080')]
                           }), \
         mock.patch.object(backend_utils.resources_utils,
                           'port_ranges_to_set',
                           return_value={8080}):
        endpoints = backend_utils.get_endpoints(record['name'],
                                                port=8080,
                                                cluster_record=record)

    assert endpoints == {8080: '1.2.3.4:8080'}


def test_get_endpoints_reuses_supplied_provider_config():
    handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
    cloud = mock.MagicMock()
    launched_resources = SimpleNamespace(cloud=cloud, ports='8080')
    launched_resources.assert_launchable = lambda: launched_resources
    handle.head_ip = '1.2.3.4'
    handle.cluster_name_on_cloud = 'svc-replica-1-cloud'
    handle.cluster_yaml = '/tmp/svc-replica-1.yaml'
    handle.launched_resources = launched_resources
    record = {
        'name': 'svc-replica-1',
        'status': status_lib.ClusterStatus.UP,
        'handle': handle,
    }
    provider_config = {'region': 'us-east-1'}

    with mock.patch.object(
            backend_utils.global_user_state,
            'get_cluster_yaml_dict',
            side_effect=AssertionError('must reuse provider_config')), \
         mock.patch.object(
             backend_utils.provision_lib,
             'query_ports',
             return_value={
                 8080: [mock.Mock(url=lambda: '1.2.3.4:8080')]
             }) as query_ports, \
         mock.patch.object(backend_utils.resources_utils,
                           'port_ranges_to_set',
                           return_value={8080}):
        endpoints = backend_utils.get_endpoints(record['name'],
                                                port=8080,
                                                cluster_record=record,
                                                provider_config=provider_config)

    assert endpoints == {8080: '1.2.3.4:8080'}
    assert query_ports.call_args.kwargs['provider_config'] is provider_config
