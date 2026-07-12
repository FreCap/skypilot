"""Tests for refresh paths that return the in-memory record without a
redundant full-row re-read.

_update_cluster_status runs under the per-cluster lock, so after a
set_cluster_status() write (which only touches status/status_updated_at)
the in-memory record can be patched and returned directly, and paths that
write nothing can return the record as-is. These tests assert both the
returned record contents and that no extra get_cluster_from_name SELECT
happens on those paths.
"""
# pylint: disable=protected-access
import time
from unittest import mock

import pytest

from sky import backends
from sky import clouds
from sky import global_user_state
from sky.backends import backend_utils
from sky.skylet import constants
from sky.utils import status_lib
from sky.utils.db import db_utils


def _fresh_db(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    monkeypatch.setattr(
        global_user_state,
        '_db_manager',
        db_utils.DatabaseManager(
            'state',
            global_user_state.create_table,
            post_init_fn=lambda _: global_user_state._sqlite_supports_returning(
            ),
        ),
    )


class _MinimalHandle:
    launched_resources = None


def test_set_cluster_status_returns_written_timestamp(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    global_user_state.add_or_update_cluster(
        cluster_name='c1',
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=True,
    )

    before = int(time.time())
    written_at = global_user_state.set_cluster_status(
        'c1', status_lib.ClusterStatus.AUTOSTOPPING)
    after = int(time.time())

    assert isinstance(written_at, int)
    assert before <= written_at <= after
    record = global_user_state.get_cluster_from_name('c1')
    assert record['status'] == status_lib.ClusterStatus.AUTOSTOPPING
    assert record['status_updated_at'] == written_at


def test_set_cluster_status_missing_cluster_raises(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        global_user_state.set_cluster_status(
            'nonexistent', status_lib.ClusterStatus.AUTOSTOPPING)


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
    # Skip the (slow, SSH-based) ray status check in these tests.
    handle.provision_runtime_metadata.has_ray = False
    return handle


def _make_record(handle):
    return {
        'handle': handle,
        'status': status_lib.ClusterStatus.UP,
        'cluster_hash': 'fake-hash',
        'autostop': 10,
        'to_down': False,
        'launched_at': time.time() - 3600,
    }


def test_autostopping_patches_record_without_reread():
    handle = _make_handle()
    record = _make_record(handle)
    node_statuses = {'pod-0': (status_lib.ClusterStatus.UP, None)}

    autostopping_backend = mock.Mock(spec=backends.CloudVmRayBackend)
    autostopping_backend.is_definitely_autostopping.return_value = True

    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils, 'get_backend_from_handle',
                           return_value=autostopping_backend), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event'), \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status',
                           return_value=12345) as set_status, \
         mock.patch.object(
             backend_utils.global_user_state, 'get_cluster_from_name',
             side_effect=AssertionError(
                 'must not re-read the record on this path')):
        result = backend_utils._update_cluster_status('test-cluster',
                                                      record,
                                                      retry_if_missing=False)

    set_status.assert_called_once_with('test-cluster',
                                       status_lib.ClusterStatus.AUTOSTOPPING)
    assert result is record
    assert result['status'] == status_lib.ClusterStatus.AUTOSTOPPING
    assert result['status_updated_at'] == 12345


def test_external_failures_return_record_without_reread():
    handle = _make_handle()
    record = _make_record(handle)
    node_statuses = {'pod-0': (status_lib.ClusterStatus.UP, None)}

    external_failure = mock.Mock()
    external_failure.get.return_value = [{
        'cluster_hash': 'fake-hash',
        'failure_mode': 'node-failure',
        'failure_reason': 'node went away',
        'cleared_at': None,
    }]

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event'), \
         mock.patch.object(
             backend_utils.global_user_state, 'get_cluster_from_name',
             side_effect=AssertionError(
                 'must not re-read the record on this path')):
        result = backend_utils._update_cluster_status('test-cluster',
                                                      record,
                                                      retry_if_missing=False)

    # Status is left untouched: the external failure was already recorded
    # at detection time.
    assert result is record
    assert result['status'] == status_lib.ClusterStatus.UP
