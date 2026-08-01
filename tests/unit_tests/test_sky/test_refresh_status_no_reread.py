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
    assert before[2:] == (-1, False)
    assert after[2:] == (10, True)


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


def test_reload_skips_full_read_when_status_unchanged():
    record = _make_refreshable_record(_make_handle())
    refresh_fields = ('UP', record['status_updated_at'], record['autostop'],
                      record['to_down'])
    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields) as cheap_read, \
         mock.patch.object(
             backend_utils.global_user_state, 'get_cluster_from_name',
             side_effect=AssertionError(
                 'must not re-read the full record when status is unchanged')):
        result = backend_utils._reload_record_if_refresh_fields_changed(
            'test-cluster', record, True, False)
    assert result is record
    cheap_read.assert_called_once_with('test-cluster')


def test_reload_fetches_full_record_when_status_changed():
    record = _make_refreshable_record(_make_handle())
    fresh_record = dict(record,
                        status=status_lib.ClusterStatus.STOPPED,
                        status_updated_at=int(time.time()))
    refresh_fields = ('STOPPED', fresh_record['status_updated_at'],
                      fresh_record['autostop'], fresh_record['to_down'])
    with mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_refresh_fields',
                           return_value=refresh_fields), \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_from_name',
                           return_value=fresh_record) as full_read:
        result = backend_utils._reload_record_if_refresh_fields_changed(
            'test-cluster', record, True, False)
    assert result is fresh_record
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
        result = backend_utils._reload_record_if_refresh_fields_changed(
            'test-cluster', record, True, False)
    assert result is None


def test_refresh_lock_path_reads_full_record_once():
    """The lock-acquired path must not do a second full-row read when no
    other process touched the status fields while acquiring the lock."""
    handle = _make_handle()
    record = _make_refreshable_record(handle)
    # Spot cluster with stale status_updated_at -> must refresh.
    handle.launched_resources.use_spot = True
    refresh_fields = ('UP', record['status_updated_at'], record['autostop'],
                      record['to_down'])

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
    update.assert_called_once_with('test-cluster', record, True, True, False)


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
    refresh_fields = ('UP', record['status_updated_at'], 10, True)

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
                                   False)


def test_refresh_lock_wait_clamps_final_sleep_to_deadline():
    """A finite lock wait must not sleep past its remaining budget."""
    handle = _make_handle()
    handle.launched_resources.use_spot = True
    record = _make_refreshable_record(handle)
    refresh_fields = ('UP', record['status_updated_at'], record['autostop'],
                      record['to_down'])

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
    refresh_fields = ('STOPPED', fresh_record['status_updated_at'],
                      fresh_record['autostop'], fresh_record['to_down'])

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
    refresh_fields = ('UP', record['status_updated_at'], record['autostop'],
                      record['to_down'])

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
