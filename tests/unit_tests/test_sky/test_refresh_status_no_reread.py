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
import asyncio
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


def test_cluster_refresh_fields_track_autostop_without_status_bump(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    global_user_state.add_or_update_cluster(
        cluster_name='c1',
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=True,
    )

    before = global_user_state.get_cluster_refresh_fields('c1')
    global_user_state.set_cluster_autostop_value('c1', 10, to_down=True)
    after = global_user_state.get_cluster_refresh_fields('c1')

    assert before is not None
    assert after is not None
    assert before[:2] == after[:2]
    assert (before.autostop, before.to_down) == (-1, False)
    assert (after.autostop, after.to_down) == (10, True)
    assert before.cluster_hash == after.cluster_hash


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


@pytest.mark.parametrize(('to_down', 'summary_response', 'expected_last_event'),
                         [
                             (False, False, 'Cluster is autostopping.'),
                             (True, False, 'Cluster is autodowning.'),
                             (False, True, None),
                         ])
def test_autostopping_patches_record_without_reread(to_down, summary_response,
                                                    expected_last_event):
    handle = _make_handle()
    record = _make_record(handle)
    record['to_down'] = to_down
    if not summary_response:
        record['last_event'] = 'All nodes up; SkyPilot runtime healthy.'
    node_statuses = {'pod-0': (status_lib.ClusterStatus.UP, None)}

    autostopping_backend = mock.Mock(spec=backends.CloudVmRayBackend)
    autostopping_backend.probe_autostopping.return_value = True

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
        result = backend_utils._update_cluster_status(
            'test-cluster',
            record,
            retry_if_missing=False,
            summary_response=summary_response)

    set_status.assert_called_once_with('test-cluster',
                                       status_lib.ClusterStatus.AUTOSTOPPING)
    assert result is record
    assert result['status'] == status_lib.ClusterStatus.AUTOSTOPPING
    assert result['status_updated_at'] == 12345
    if expected_last_event is None:
        assert 'last_event' not in result
    else:
        assert result['last_event'] == expected_last_event


def test_stable_up_refresh_only_advances_status_timestamp():
    handle = _make_handle()
    record = _make_record(handle)
    record['status_updated_at'] = 100
    node_statuses = {'pod-0': (status_lib.ClusterStatus.UP, None)}

    backend = mock.Mock(spec=backends.CloudVmRayBackend)
    backend.probe_autostopping.return_value = False
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils, 'get_backend_from_handle',
                           return_value=backend), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status',
                           return_value=12345) as set_status, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_or_update_cluster') as full_write, \
         mock.patch.object(
             backend_utils.global_user_state, 'get_cluster_from_name',
             side_effect=AssertionError(
                 'stable status must not re-read the full record')) as full_read:
        result = backend_utils._update_cluster_status('test-cluster',
                                                      record,
                                                      retry_if_missing=False)

    assert result is record
    assert result['status'] == status_lib.ClusterStatus.UP
    assert result['status_updated_at'] == 12345
    set_status.assert_called_once_with('test-cluster',
                                       status_lib.ClusterStatus.UP)
    add_event.assert_not_called()
    full_write.assert_not_called()
    full_read.assert_not_called()


def test_stable_init_refresh_only_advances_status_timestamp():
    handle = _make_handle()
    record = _make_record(handle)
    record.update({
        'status': status_lib.ClusterStatus.INIT,
        'status_updated_at': 100,
        'autostop': -1,
        'last_event': 'Cluster is abnormal because provisioning stalled.',
    })
    node_statuses = {
        'pod-0': (status_lib.ClusterStatus.INIT, 'provisioning stalled')
    }

    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status',
                           return_value=12345) as set_status, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_or_update_cluster') as full_write, \
         mock.patch.object(
             backend_utils.global_user_state, 'get_cluster_from_name',
             side_effect=AssertionError(
                 'stable status must not re-read the full record')) as full_read:
        result = backend_utils._update_cluster_status('test-cluster',
                                                      record,
                                                      retry_if_missing=False)

    assert result is record
    assert result['status'] == status_lib.ClusterStatus.INIT
    assert result['status_updated_at'] == 12345
    assert result['last_event'].endswith('provisioning stalled.')
    set_status.assert_called_once_with('test-cluster',
                                       status_lib.ClusterStatus.INIT)
    add_event.assert_not_called()
    full_write.assert_not_called()
    full_read.assert_not_called()


def test_up_transition_keeps_full_writer():
    handle = _make_handle()
    record = _make_record(handle)
    record['status'] = status_lib.ClusterStatus.INIT
    node_statuses = {'pod-0': (status_lib.ClusterStatus.UP, None)}
    persisted = dict(record,
                     status=status_lib.ClusterStatus.UP,
                     status_updated_at=12345)

    backend = mock.Mock(spec=backends.CloudVmRayBackend)
    backend.probe_autostopping.return_value = False
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils, 'get_backend_from_handle',
                           return_value=backend), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status') as set_status, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_or_update_cluster') as full_write, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=persisted) as full_read:
        result = backend_utils._update_cluster_status('test-cluster',
                                                      record,
                                                      retry_if_missing=False)

    assert result is persisted
    set_status.assert_not_called()
    add_event.assert_called_once()
    full_write.assert_called_once_with('test-cluster',
                                       handle,
                                       requested_resources=None,
                                       ready=True,
                                       is_launch=False,
                                       existing_cluster_hash='fake-hash')
    full_read.assert_called_once_with('test-cluster',
                                      include_user_info=True,
                                      summary_response=False)


def test_up_transition_summary_reuses_refresh_fields_without_full_reread():
    handle = _make_handle()
    record = _make_record(handle)
    record['status'] = status_lib.ClusterStatus.INIT
    record['launch_status_reason'] = 'launching'
    node_statuses = {'pod-0': (status_lib.ClusterStatus.UP, None)}
    refresh_fields = _refresh_fields(
        dict(record,
             status=status_lib.ClusterStatus.UP,
             status_updated_at=12345))

    backend = mock.Mock(spec=backends.CloudVmRayBackend)
    backend.probe_autostopping.return_value = False
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils, 'get_backend_from_handle',
                           return_value=backend), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status') as set_status, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_or_update_cluster') as full_write, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(
             backend_utils.global_user_state, 'get_cluster_from_name',
             side_effect=AssertionError(
                 'summary transition must not re-read the full record')):
        result = backend_utils._update_cluster_status('test-cluster',
                                                      record,
                                                      retry_if_missing=False,
                                                      summary_response=True)

    assert result is record
    assert result['status'] == status_lib.ClusterStatus.UP
    assert result['status_updated_at'] == 12345
    assert result['cluster_ever_up'] is True
    assert result['launch_status_reason'] is None
    set_status.assert_not_called()
    add_event.assert_called_once()
    full_write.assert_called_once_with('test-cluster',
                                       handle,
                                       requested_resources=None,
                                       ready=True,
                                       is_launch=False,
                                       existing_cluster_hash='fake-hash')
    cheap_read.assert_called_once_with('test-cluster')


def test_init_transition_keeps_full_writer():
    handle = _make_handle()
    record = _make_record(handle)
    record['autostop'] = -1
    node_statuses = {
        'pod-0': (status_lib.ClusterStatus.INIT, 'provisioning stalled')
    }
    persisted = dict(record,
                     status=status_lib.ClusterStatus.INIT,
                     status_updated_at=12345)

    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status') as set_status, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_or_update_cluster') as full_write, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=persisted) as full_read:
        result = backend_utils._update_cluster_status('test-cluster',
                                                      record,
                                                      retry_if_missing=False)

    assert result is persisted
    set_status.assert_not_called()
    add_event.assert_called_once()
    full_write.assert_called_once_with('test-cluster',
                                       handle,
                                       requested_resources=None,
                                       ready=False,
                                       is_launch=False,
                                       existing_cluster_hash='fake-hash')
    full_read.assert_called_once_with('test-cluster',
                                      include_user_info=True,
                                      summary_response=False)


def test_init_transition_summary_reuses_refresh_fields_without_full_reread():
    handle = _make_handle()
    record = _make_record(handle)
    record.update({
        'status': status_lib.ClusterStatus.UP,
        'status_updated_at': 100,
        'autostop': 10,
        'to_down': True,
    })
    node_statuses = {
        'pod-0': (status_lib.ClusterStatus.INIT, 'provisioning stalled')
    }
    refresh_fields = _refresh_fields(
        dict(record,
             status=status_lib.ClusterStatus.INIT,
             status_updated_at=12345,
             autostop=-1,
             to_down=False))

    cluster_info = mock.Mock()
    cluster_info.get_head_instance.return_value = None
    backend = mock.Mock(spec=backends.CloudVmRayBackend)
    backend.probe_autostopping.return_value = False
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils,
                           '_query_cluster_info_via_cloud_api',
                           return_value=cluster_info), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils, 'get_backend_from_handle',
                           return_value=backend), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_autostop_value') as set_autostop, \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status') as set_status, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_or_update_cluster') as full_write, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(
             backend_utils.global_user_state, 'get_cluster_from_name',
             side_effect=AssertionError(
                 'summary transition must not re-read the full record')):
        result = backend_utils._update_cluster_status('test-cluster',
                                                      record,
                                                      retry_if_missing=False,
                                                      summary_response=True)

    assert result is record
    assert result['status'] == status_lib.ClusterStatus.INIT
    assert result['status_updated_at'] == 12345
    assert result['autostop'] == -1
    assert result['to_down'] is False
    set_status.assert_not_called()
    set_autostop.assert_called_once_with('test-cluster', -1, to_down=False)
    add_event.assert_called_once()
    full_write.assert_called_once_with('test-cluster',
                                       handle,
                                       requested_resources=None,
                                       ready=False,
                                       is_launch=False,
                                       existing_cluster_hash='fake-hash')
    cheap_read.assert_called_once_with('test-cluster')


def test_stable_init_with_autostop_keeps_full_writer():
    handle = _make_handle()
    record = _make_record(handle)
    record.update({
        'status': status_lib.ClusterStatus.INIT,
        'status_updated_at': 100,
        'autostop': 10,
        'to_down': True,
    })
    node_statuses = {
        'pod-0': (status_lib.ClusterStatus.INIT, 'provisioning stalled')
    }
    persisted = dict(record,
                     status=status_lib.ClusterStatus.INIT,
                     status_updated_at=12345)

    cluster_info = mock.Mock()
    cluster_info.get_head_instance.return_value = object()
    backend = mock.Mock(spec=backends.CloudVmRayBackend)
    backend.probe_autostopping.return_value = False
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils,
                           '_query_cluster_info_via_cloud_api',
                           return_value=cluster_info), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils, 'get_backend_from_handle',
                           return_value=backend), \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_yaml_dict',
                           return_value={'provider': {}}), \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status') as set_status, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_or_update_cluster') as full_write, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=persisted) as full_read:
        result = backend_utils._update_cluster_status('test-cluster',
                                                      record,
                                                      retry_if_missing=False)

    assert result is persisted
    set_status.assert_not_called()
    add_event.assert_not_called()
    full_write.assert_called_once_with('test-cluster',
                                       handle,
                                       requested_resources=None,
                                       ready=False,
                                       is_launch=False,
                                       existing_cluster_hash='fake-hash')
    full_read.assert_called_once_with('test-cluster',
                                      include_user_info=True,
                                      summary_response=False)


def test_stable_status_write_failure_does_not_publish_timestamp():
    handle = _make_handle()
    record = _make_record(handle)
    record['status_updated_at'] = 100
    node_statuses = {'pod-0': (status_lib.ClusterStatus.UP, None)}

    backend = mock.Mock(spec=backends.CloudVmRayBackend)
    backend.probe_autostopping.return_value = False
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils, 'get_backend_from_handle',
                           return_value=backend), \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status',
                           side_effect=OSError('database unavailable')), \
         mock.patch.object(
             backend_utils.global_user_state, 'add_or_update_cluster',
             side_effect=AssertionError(
                 'stable status must not use the full writer')):
        with pytest.raises(OSError, match='database unavailable'):
            backend_utils._update_cluster_status('test-cluster',
                                                 record,
                                                 retry_if_missing=False)

    assert record['status_updated_at'] == 100


def _make_refreshable_record(handle):
    record = _make_record(handle)
    record['status_updated_at'] = int(time.time()) - 3600
    record['workspace'] = constants.SKYPILOT_DEFAULT_WORKSPACE
    return record


def _refresh_fields(record, *, workload_type=None):
    return global_user_state.ClusterRefreshFields(
        record['status'].name, record['status_updated_at'], record['autostop'],
        record['to_down'], record.get('cluster_hash'),
        record.get('is_managed', False), workload_type)


def _managed_candidate(record):
    return global_user_state.ManagedClusterStatusFields(
        record['status'].name, record['status_updated_at'],
        record['cluster_hash'])


def test_nominated_service_no_yaml_is_retained_under_refresh_locks():
    handle = _make_handle()
    handle.cluster_yaml = None
    record = _make_refreshable_record(handle)
    record['is_managed'] = True
    record['workload_type'] = 'service'
    status_lock = mock.MagicMock()
    status_lock.acquire.return_value.__enter__.return_value = None
    resource_lock = mock.MagicMock()
    resource_lock.acquire.return_value.__enter__.return_value = None

    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=record) as full_read, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=_refresh_fields(
                               record, workload_type='service')) as cheap_read, \
         mock.patch.object(backend_utils,
                           '_check_owner_identity_with_record') as owner, \
         mock.patch.object(backend_utils.locks,
                           'get_lock',
                           side_effect=[status_lock,
                                        resource_lock]) as get_lock, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'remove_cluster') as remove:
        result = backend_utils.refresh_cluster_record(
            'test-cluster',
            force_refresh_statuses=set(status_lib.ClusterStatus),
            include_user_info=False,
            summary_response=True,
            _managed_no_yaml_candidate=_managed_candidate(record))

    assert result is record
    full_read.assert_called_once()
    cheap_read.assert_called_once_with('test-cluster')
    assert get_lock.call_count == 2
    owner.assert_called_once_with('test-cluster', record)
    add_event.assert_not_called()
    remove.assert_not_called()


@pytest.mark.parametrize('cluster_yaml', [None, '/fake/path/cluster.yaml'])
@pytest.mark.parametrize('cheap_snapshot_is_stale_service', [False, True])
@pytest.mark.parametrize(('is_managed', 'workload_type', 'cluster_hash'), [
    (True, 'managed_job', 'fake-hash'),
    (True, 'pool', 'fake-hash'),
    (True, 'service', 'successor-hash'),
    (False, None, 'fake-hash'),
])
def test_stale_service_nomination_cannot_act_on_same_name_successor(
        is_managed, workload_type, cluster_hash,
        cheap_snapshot_is_stale_service, cluster_yaml):
    handle = _make_handle()
    handle.cluster_yaml = cluster_yaml
    predecessor = _make_refreshable_record(handle)
    predecessor['is_managed'] = True
    predecessor['workload_type'] = 'service'
    changed_at = predecessor['status_updated_at'] + 1
    successor = dict(predecessor,
                     is_managed=is_managed,
                     cluster_hash=cluster_hash,
                     status_updated_at=changed_at,
                     workload_type=workload_type)
    if cheap_snapshot_is_stale_service:
        # The cheap B snapshot still describes the old service identity,
        # while the following full read returns successor C at the same
        # timestamp. Refresh fields must come from C, not be copied from B.
        cheap_record = dict(predecessor, status_updated_at=changed_at)
        cheap_workload_type = 'service'
    else:
        cheap_record = successor
        cheap_workload_type = workload_type
    candidate = _managed_candidate(predecessor)
    status_lock = mock.MagicMock()
    status_lock.acquire.return_value.__enter__.return_value = None

    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           side_effect=[predecessor,
                                        successor]) as full_read, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=_refresh_fields(
                               cheap_record,
                               workload_type=cheap_workload_type)) as cheap_read, \
         mock.patch.object(backend_utils,
                           '_check_owner_identity_with_record') as owner, \
         mock.patch.object(backend_utils.locks,
                           'get_lock',
                           return_value=status_lock) as get_lock, \
         mock.patch.object(
             backend_utils,
             '_update_cluster_status_with_resource_lock') as update, \
         mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api') as provider, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'remove_cluster') as remove:
        result = backend_utils.refresh_cluster_record(
            'test-cluster',
            force_refresh_statuses=set(status_lib.ClusterStatus),
            include_user_info=False,
            summary_response=True,
            _managed_no_yaml_candidate=candidate)

    assert result is successor
    assert full_read.call_args_list == [
        mock.call('test-cluster',
                  include_user_info=False,
                  summary_response=True),
        mock.call('test-cluster',
                  include_user_info=False,
                  summary_response=True),
    ]
    cheap_read.assert_called_once_with('test-cluster')
    get_lock.assert_called_once_with(
        backend_utils.cluster_status_lock_id('test-cluster'))
    status_lock.acquire.assert_called_once_with(blocking=False)
    owner.assert_not_called()
    update.assert_not_called()
    provider.assert_not_called()
    add_event.assert_not_called()
    remove.assert_not_called()


@pytest.mark.parametrize('workload_type', ['managed_job', 'pool'])
def test_initial_reclassified_candidate_exits_before_identity_or_lock(
        workload_type):
    record = _make_refreshable_record(_make_handle())
    record.update(is_managed=True, workload_type=workload_type)
    candidate = _managed_candidate(record)

    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=record), \
         mock.patch.object(backend_utils,
                           '_check_owner_identity_with_record') as owner, \
         mock.patch.object(backend_utils.locks, 'get_lock') as get_lock:
        result = backend_utils.refresh_cluster_record(
            'test-cluster',
            force_refresh_statuses=set(status_lib.ClusterStatus),
            _managed_no_yaml_candidate=candidate)

    assert result is record
    owner.assert_not_called()
    get_lock.assert_not_called()


@pytest.mark.parametrize(('workload_type', 'admitted'), [
    ('service', True),
    ('managed_job', False),
    ('pool', False),
])
def test_candidate_with_status_lock_already_held_is_revalidated(
        workload_type, admitted):
    record = _make_refreshable_record(_make_handle())
    record.update(is_managed=True, workload_type=workload_type)
    candidate = _managed_candidate(record)
    refresh_fields = _refresh_fields(record, workload_type=workload_type)

    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=record), \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(backend_utils,
                           '_check_owner_identity_with_record') as owner, \
         mock.patch.object(
             backend_utils,
             '_update_cluster_status_with_resource_lock',
             return_value=record) as update:
        result = backend_utils.refresh_cluster_record(
            'test-cluster',
            force_refresh_statuses=set(status_lib.ClusterStatus),
            cluster_lock_already_held=True,
            cluster_resource_lock_already_held=True,
            _managed_no_yaml_candidate=candidate)

    assert result is record
    if admitted:
        cheap_read.assert_called_once_with('test-cluster')
        owner.assert_called_once_with('test-cluster', record)
        update.assert_called_once_with('test-cluster', record, True, True,
                                       False, True, candidate)
    else:
        cheap_read.assert_not_called()
        owner.assert_not_called()
        update.assert_not_called()


@pytest.mark.parametrize('cluster_yaml', [None, '/fake/path/cluster.yaml'])
@pytest.mark.parametrize('workload_type', ['managed_job', 'pool'])
def test_update_rejects_reclassified_candidate_before_provider_or_cleanup(
        workload_type, cluster_yaml):
    handle = _make_handle()
    handle.cluster_yaml = cluster_yaml
    record = _make_refreshable_record(handle)
    record.update(is_managed=True, workload_type=workload_type)
    candidate = _managed_candidate(record)

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api') as provider, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event') as add_event, \
         mock.patch.object(backend_utils.global_user_state,
                           'remove_cluster') as remove:
        result = backend_utils._update_cluster_status(
            'test-cluster',
            record,
            retry_if_missing=True,
            managed_no_yaml_candidate=candidate)

    assert result is record
    provider.assert_not_called()
    add_event.assert_not_called()
    remove.assert_not_called()


def test_reload_skips_full_read_when_status_unchanged():
    record = _make_refreshable_record(_make_handle())
    refresh_fields = _refresh_fields(record)
    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(
             backend_utils.global_user_state, 'get_cluster_from_name',
             side_effect=AssertionError(
                 'must not re-read the full record when status is unchanged')):
        result, fields = backend_utils._reload_record_if_refresh_fields_changed(
            'test-cluster', record, True, False)
    assert result is record
    assert fields is refresh_fields
    cheap_read.assert_called_once_with('test-cluster')


def test_reload_fetches_full_record_when_status_changed():
    record = _make_refreshable_record(_make_handle())
    record.update(is_managed=True, workload_type='service')
    cheap_record = dict(record,
                        status=status_lib.ClusterStatus.STOPPED,
                        status_updated_at=record['status_updated_at'] + 1)
    fresh_record = dict(cheap_record, workload_type='managed_job')
    refresh_fields = _refresh_fields(cheap_record, workload_type='service')
    expected_fields = _refresh_fields(fresh_record, workload_type='managed_job')
    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=fresh_record) as full_read:
        result, fields = backend_utils._reload_record_if_refresh_fields_changed(
            'test-cluster', record, True, False)
    assert result is fresh_record
    assert fields == expected_fields
    assert fields != refresh_fields
    assert fields is not refresh_fields
    assert fields.workload_type == 'managed_job'
    cheap_read.assert_called_once_with('test-cluster')
    full_read.assert_called_once_with('test-cluster',
                                      include_user_info=True,
                                      summary_response=False)


def test_reload_returns_none_when_cluster_deleted():
    record = _make_refreshable_record(_make_handle())
    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=None), \
         mock.patch.object(
             backend_utils.global_user_state, 'get_cluster_from_name',
             side_effect=AssertionError(
                 'must not re-read the full record for a deleted cluster')):
        result, fields = backend_utils._reload_record_if_refresh_fields_changed(
            'test-cluster', record, True, False)
    assert result is None
    assert fields is None


def test_refresh_lock_path_reads_full_record_once():
    """The lock-acquired path must not do a second full-row read when no
    other process touched the status fields while acquiring the lock."""
    handle = _make_handle()
    record = _make_refreshable_record(handle)
    # Spot cluster with stale status_updated_at -> must refresh.
    handle.launched_resources.use_spot = True
    refresh_fields = _refresh_fields(record)

    lock = mock.MagicMock()
    lock.acquire.return_value.__enter__.return_value = None

    updated = dict(record, status=status_lib.ClusterStatus.UP)
    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=record) as full_read, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(backend_utils,
                           '_check_owner_identity_with_record'), \
         mock.patch.object(backend_utils.locks, 'get_lock',
                           return_value=lock), \
         mock.patch.object(backend_utils, '_update_cluster_status',
                           return_value=updated) as update:
        result = backend_utils.refresh_cluster_record('test-cluster')

    assert result is updated
    full_read.assert_called_once()
    cheap_read.assert_called_once_with('test-cluster')
    update.assert_called_once_with('test-cluster', record, True, True, False,
                                   None)


def test_refresh_reloads_record_when_autostop_changes_before_lock():
    """Autostop fields used by refresh must not remain stale under the lock."""
    handle = _make_handle()
    handle.launched_resources.use_spot = True
    record = _make_refreshable_record(handle)
    record['autostop'] = -1
    fresh_record = dict(record, autostop=10, to_down=True)

    blocked = mock.MagicMock()
    blocked.__enter__.side_effect = backend_utils.locks.LockTimeout
    acquired = mock.MagicMock()
    acquired.__enter__.return_value = None
    lock = mock.MagicMock()
    lock.acquire.side_effect = [blocked, acquired]
    resource_lock = mock.MagicMock()
    resource_lock.acquire.return_value.__enter__.return_value = None
    status_fields = {
        'test-cluster': ('UP', record['status_updated_at']),
    }
    refresh_fields = _refresh_fields(fresh_record)

    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           side_effect=[record, fresh_record]) as full_read, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_status_fields',
                           return_value=status_fields) as legacy_read, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as refresh_read, \
         mock.patch.object(backend_utils,
                           '_check_owner_identity_with_record'), \
         mock.patch.object(
             backend_utils.locks,
             'get_lock',
             side_effect=[lock, resource_lock]) as get_lock, \
         mock.patch.object(backend_utils.context_utils,
                           'sleep_with_cancellation') as sleep, \
         mock.patch.object(backend_utils, '_update_cluster_status',
                           return_value=fresh_record) as update:
        result = backend_utils.refresh_cluster_record(
            'test-cluster', cluster_status_lock_timeout=-1)

    assert result is fresh_record
    assert full_read.call_count == 2
    legacy_read.assert_not_called()
    assert refresh_read.call_count == 2
    assert lock.acquire.call_count == 2
    resource_lock.acquire.assert_called_once_with(blocking=False)
    assert get_lock.call_args_list == [
        mock.call(backend_utils.cluster_status_lock_id('test-cluster')),
        mock.call(
            backend_utils.cluster_resource_operation_lock_id('test-cluster')),
    ]
    sleep.assert_called_once_with(lock.poll_interval)
    update.assert_called_once_with('test-cluster', fresh_record, True, True,
                                   False, None)


def test_refresh_lock_wait_clamps_final_sleep_to_deadline():
    """A finite lock wait must not sleep past its remaining budget."""
    handle = _make_handle()
    handle.launched_resources.use_spot = True
    record = _make_refreshable_record(handle)
    refresh_fields = _refresh_fields(record)

    blocked = mock.MagicMock()
    blocked.__enter__.side_effect = backend_utils.locks.LockTimeout
    lock = mock.MagicMock(poll_interval=1.0)
    lock.acquire.return_value = blocked

    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=record) as full_read, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(backend_utils,
                           '_check_owner_identity_with_record'), \
         mock.patch.object(backend_utils.locks, 'get_lock',
                           return_value=lock), \
         mock.patch.object(
             backend_utils.time,
             'perf_counter',
             side_effect=[100.0, 100.75, 101.001]) as perf_counter, \
         mock.patch.object(backend_utils.context_utils,
                           'sleep_with_cancellation') as sleep, \
         mock.patch.object(backend_utils, '_update_cluster_status') as update:
        result = backend_utils.refresh_cluster_record(
            'test-cluster', cluster_status_lock_timeout=1)

    assert result is record
    full_read.assert_called_once()
    cheap_read.assert_called_once_with('test-cluster')
    assert lock.acquire.call_count == 2
    sleep.assert_called_once_with(pytest.approx(0.25))
    assert perf_counter.call_count == 3
    update.assert_not_called()


def test_refresh_lock_wait_returns_concurrent_update_at_deadline():
    """The final bounded sleep still reloads a concurrent status update."""
    handle = _make_handle()
    handle.launched_resources.use_spot = True
    record = _make_refreshable_record(handle)
    fresh_record = dict(record,
                        status=status_lib.ClusterStatus.STOPPED,
                        status_updated_at=int(time.time()))
    refresh_fields = _refresh_fields(fresh_record)

    blocked = mock.MagicMock()
    blocked.__enter__.side_effect = backend_utils.locks.LockTimeout
    lock = mock.MagicMock(poll_interval=1.0)
    lock.acquire.return_value = blocked

    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           side_effect=[record, fresh_record]) as full_read, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(backend_utils,
                           '_check_owner_identity_with_record'), \
         mock.patch.object(backend_utils.locks, 'get_lock',
                           return_value=lock), \
         mock.patch.object(backend_utils.time,
                           'perf_counter',
                           side_effect=[100.0, 100.75]), \
         mock.patch.object(backend_utils.context_utils,
                           'sleep_with_cancellation') as sleep, \
         mock.patch.object(backend_utils, '_update_cluster_status') as update:
        result = backend_utils.refresh_cluster_record(
            'test-cluster', cluster_status_lock_timeout=1)

    assert result is fresh_record
    assert full_read.call_count == 2
    cheap_read.assert_called_once_with('test-cluster')
    lock.acquire.assert_called_once_with(blocking=False)
    sleep.assert_called_once_with(pytest.approx(0.25))
    update.assert_not_called()


def test_refresh_lock_wait_cancellation_stops_before_next_poll():
    """Cancellation while waiting for the status lock must wake immediately."""
    handle = _make_handle()
    handle.launched_resources.use_spot = True
    record = _make_refreshable_record(handle)
    refresh_fields = _refresh_fields(record)

    blocked = mock.MagicMock()
    blocked.__enter__.side_effect = backend_utils.locks.LockTimeout
    lock = mock.MagicMock(poll_interval=1.0)
    lock.acquire.side_effect = [
        blocked,
        AssertionError('retried the status lock after cancellation'),
    ]

    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=record) as full_read, \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(backend_utils,
                           '_check_owner_identity_with_record'), \
         mock.patch.object(backend_utils.locks, 'get_lock',
                           return_value=lock), \
         mock.patch.object(backend_utils.context_utils,
                           'sleep_with_cancellation',
                           side_effect=asyncio.CancelledError()) as sleep, \
         mock.patch.object(
             backend_utils.time,
             'sleep',
             side_effect=AssertionError(
                 'used raw sleep instead of cancelable wait')), \
         mock.patch.object(backend_utils, '_update_cluster_status') as update:
        with pytest.raises(asyncio.CancelledError):
            backend_utils.refresh_cluster_record('test-cluster',
                                                 cluster_status_lock_timeout=-1)

    full_read.assert_called_once()
    cheap_read.assert_not_called()
    lock.acquire.assert_called_once_with(blocking=False)
    sleep.assert_called_once_with(lock.poll_interval)
    update.assert_not_called()


def test_update_cluster_status_cancellation_stops_before_next_ray_probe():
    handle = _make_handle()
    handle.launched_nodes = 1
    handle.provision_runtime_metadata.has_ray = True
    handle.launched_resources.cloud = mock.Mock()
    handle.launched_resources.cloud.uses_ray.return_value = True
    runner = mock.Mock()
    runner.run.side_effect = [
        (0, 'ray status output', ''),
        AssertionError('probed ray status after cancellation'),
    ]
    handle.get_command_runners.return_value = [runner]
    record = _make_record(handle)
    node_statuses = {'pod-0': (status_lib.ClusterStatus.UP, None)}
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses) as query_status, \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils,
                           '_ray_status_via_skylet_grpc',
                           return_value=None), \
         mock.patch.object(backend_utils,
                           '_count_healthy_nodes_from_ray',
                           return_value=(0, 0)), \
         mock.patch.object(backend_utils.context_utils,
                           'sleep_with_cancellation',
                           side_effect=asyncio.CancelledError()) as sleep, \
         mock.patch.object(
             backend_utils.time,
             'sleep',
             side_effect=AssertionError(
                 'used raw sleep instead of cancelable wait')):
        with pytest.raises(asyncio.CancelledError):
            backend_utils._update_cluster_status('test-cluster',
                                                 record,
                                                 retry_if_missing=False)

    query_status.assert_called_once_with(handle,
                                         retry_if_missing=False,
                                         get_ray_config=mock.ANY)
    runner.run.assert_called_once_with(
        backend_utils.instance_setup.RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND,
        stream_logs=False,
        require_outputs=True,
        separate_stderr=True)
    sleep.assert_called_once_with(1)


def test_update_cluster_status_cancellation_stops_transport_retry():
    handle = _make_handle()
    handle.launched_nodes = 1
    handle.provision_runtime_metadata.has_ray = True
    handle.launched_resources.cloud = clouds.Kubernetes()
    runner = mock.Mock()
    runner.run.side_effect = [
        (1, '', 'transient transport failure'),
        AssertionError('retried ray status after cancellation'),
    ]
    handle.get_command_runners.return_value = [runner]
    record = _make_record(handle)
    node_statuses = {'pod-0': (status_lib.ClusterStatus.UP, None)}
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=node_statuses) as query_status, \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils,
                           '_ray_status_via_skylet_grpc',
                           return_value=None), \
         mock.patch.object(backend_utils.context_utils,
                           'sleep_with_cancellation',
                           side_effect=asyncio.CancelledError()) as sleep, \
         mock.patch.object(
             backend_utils.time,
             'sleep',
             side_effect=AssertionError(
                 'used raw sleep instead of cancelable wait')):
        with pytest.raises(asyncio.CancelledError):
            backend_utils._update_cluster_status('test-cluster',
                                                 record,
                                                 retry_if_missing=False)

    query_status.assert_called_once_with(handle,
                                         retry_if_missing=False,
                                         get_ray_config=mock.ANY)
    runner.run.assert_called_once_with(
        backend_utils.instance_setup.RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND,
        stream_logs=False,
        require_outputs=True,
        separate_stderr=True)
    sleep.assert_called_once_with(1)


def test_update_cluster_status_cancellation_stops_before_launch_double_check_reread(
):
    handle = _make_handle()
    handle.launched_resources.cloud = mock.Mock(
        STATUS_VERSION=clouds.StatusVersion.SKYPILOT)
    handle.launched_resources.assert_launchable.return_value = (
        handle.launched_resources)
    record = _make_record(handle)
    record['status'] = status_lib.ClusterStatus.INIT
    record['launched_at'] = time.time()
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(
            backend_utils,
            '_query_cluster_status_via_cloud_api',
            side_effect=[
                {},
                AssertionError('re-read cluster status after cancellation'),
            ]) as query_status, \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils.context_utils,
                           'sleep_with_cancellation',
                           side_effect=asyncio.CancelledError()) as sleep, \
         mock.patch.object(
             backend_utils.time,
             'sleep',
             side_effect=AssertionError(
                 'used raw sleep instead of cancelable wait')):
        with pytest.raises(asyncio.CancelledError):
            backend_utils._update_cluster_status('test-cluster',
                                                 record,
                                                 retry_if_missing=False)

    query_status.assert_called_once_with(handle,
                                         retry_if_missing=False,
                                         get_ray_config=mock.ANY)
    sleep.assert_called_once_with(backend_utils._LAUNCH_DOUBLE_CHECK_DELAY)


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
