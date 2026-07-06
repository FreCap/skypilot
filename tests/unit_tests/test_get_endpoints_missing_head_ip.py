"""get_endpoints must raise ClusterNotUpError when the head IP is unknown.

A cluster record can transiently be UP with handle.head_ip=None
(mid-provision, or a recovered serve controller re-driving a launch).
query_ports_passthrough asserts head_ip is not None, and that
AssertionError used to escape get_endpoints and kill the entire serve
status query at fleet scale. ClusterNotUpError is the contract callers
already handle (ReplicaInfo.url returns None on it).
"""
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
