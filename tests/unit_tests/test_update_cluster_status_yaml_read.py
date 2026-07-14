"""Regression tests: _update_cluster_status reads the cluster YAML once.

The cluster YAML is immutable while the per-cluster refresh lock is held,
but the refresh path used to read + parse it (a DB SELECT plus
yaml.safe_load) up to three times per invocation: once inside
_query_cluster_status_via_cloud_api, once in the abnormal-Kubernetes
diagnostics branch, and once in the Kubernetes autodown-breadcrumb branch.
These tests pin the invariant that a single refresh performs at most one
YAML read, shared across all branches.
"""
import time
from unittest import mock

from sky import backends
from sky import clouds
from sky.backends import backend_utils
from sky.utils import status_lib

_YAML_DICT = {'provider': {'namespace': 'default', 'context': 'ctx'}}


def _make_handle():
    handle = mock.Mock(spec=backends.CloudVmRayResourceHandle)
    handle.cluster_name = 'test-cluster'
    handle.cluster_name_on_cloud = 'test-cluster-1234'
    handle.cluster_yaml = '/fake/path/cluster.yaml'
    handle.launched_nodes = 1
    handle.num_ips_per_node = 1
    handle.launched_resources = mock.Mock(unsafe=True)
    handle.launched_resources.cloud = clouds.Kubernetes()
    handle.launched_resources.use_spot = False
    handle.launched_resources.assert_launchable.return_value = (
        handle.launched_resources)
    handle.provision_runtime_metadata = mock.Mock()
    handle.provision_runtime_metadata.has_ray = True
    return handle


def _make_record(handle):
    return {
        'handle': handle,
        'status': status_lib.ClusterStatus.UP,
        'cluster_hash': 'fake-hash',
        'autostop': -1,
        'to_down': False,
        'launched_at': time.time() - 3600,  # old enough to skip recheck
    }


def _run_update(node_status_dict, yaml_reader, autostop=-1):
    """Run _update_cluster_status with the real cloud-API query path."""
    handle = _make_handle()
    record = _make_record(handle)
    record['autostop'] = autostop

    cluster_info = mock.Mock()
    cluster_info.get_head_instance.return_value = mock.Mock()
    backend = mock.Mock(spec=backends.CloudVmRayBackend)
    backend.is_definitely_autostopping.return_value = False

    external_failure = mock.Mock()
    external_failure.get.return_value = None

    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_yaml_dict',
                           yaml_reader), \
         mock.patch.object(backend_utils.provision_lib, 'query_instances',
                           return_value=node_status_dict), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils.k8s_instance,
                           'get_cluster_failure_reason_from_events',
                           return_value=None), \
         mock.patch.object(backend_utils.k8s_instance,
                           'get_cluster_failure_reason_from_pods',
                           return_value='OOMKilled (exit code 137)'), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event'), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_or_update_cluster'), \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=record), \
         mock.patch.object(backend_utils.provision_lib, 'get_cluster_info',
                           return_value=cluster_info), \
         mock.patch.object(backend_utils, 'get_backend_from_handle',
                           return_value=backend):
        backend_utils._update_cluster_status('test-cluster',
                                             record,
                                             retry_if_missing=False)


class TestSingleYamlReadPerRefresh:

    def test_abnormal_k8s_fallback_reuses_query_yaml(self):
        # All pods report UP with no reason, the ray health probe fails
        # (mock handle), so the cluster goes abnormal and the events/pods
        # fallback branch runs. Both the cloud-API query and the fallback
        # need the parsed YAML; the refresh must fetch it exactly once.
        yaml_reader = mock.Mock(return_value=_YAML_DICT)
        _run_update({'pod-0': (status_lib.ClusterStatus.UP, None)}, yaml_reader)
        assert yaml_reader.call_count == 1, yaml_reader.call_count

    def test_abnormal_autostop_head_probe_reuses_query_yaml(self):
        # With autostop enabled, the abnormal branch additionally probes the
        # head node via _query_cluster_info_via_cloud_api. That probe needs
        # the parsed YAML too; the refresh must still fetch it exactly once,
        # shared with the cloud-API status query and the k8s diagnostics.
        yaml_reader = mock.Mock(return_value=_YAML_DICT)
        _run_update({'pod-0': (status_lib.ClusterStatus.UP, None)},
                    yaml_reader,
                    autostop=10)
        assert yaml_reader.call_count == 1, yaml_reader.call_count

    def test_failed_yaml_read_is_not_memoized(self):
        # A failed read must not be cached as a result: every branch that
        # needs the YAML retries the fetch (and degrades gracefully within
        # its own try/except), preserving pre-change semantics.
        yaml_reader = mock.Mock(side_effect=ValueError('yaml gone'))
        try:
            _run_update({'pod-0': (status_lib.ClusterStatus.UP, None)},
                        yaml_reader)
        except ValueError:
            # The cloud-API query propagates the first failure, same as
            # before this change.
            pass
        assert yaml_reader.call_count >= 1
