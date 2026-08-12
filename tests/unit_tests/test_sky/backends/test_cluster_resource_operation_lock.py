"""Regressions for cluster provider-operation serialization."""

import asyncio
import threading
from unittest import mock

import pytest

from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend
from sky.utils import status_lib


class _StopTest(RuntimeError):
    """Stops a provision call after it has acquired its locks."""


class _RecordingLock:
    """A test lock that records context-manager and force-unlock calls."""

    def __init__(self, lock_id, events):
        self.lock_id = lock_id
        self.events = events
        self.force_unlock_calls = 0

    def __enter__(self):
        self.events.append(('enter', self.lock_id))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type, exc_value, traceback
        self.events.append(('exit', self.lock_id))

    def force_unlock(self):
        self.force_unlock_calls += 1
        self.events.append(('force_unlock', self.lock_id))


class _BlockingLock(_RecordingLock):
    """A test lock whose context entry waits for an explicit release."""

    def __init__(self, lock_id, events, entered, release):
        super().__init__(lock_id, events)
        self.entered = entered
        self.release = release

    def __enter__(self):
        self.events.append(('waiting', self.lock_id))
        self.entered.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError(f'Test did not release {self.lock_id}')
        return super().__enter__()


def test_resource_operation_lock_ids_are_per_cluster():
    first = backend_utils.cluster_resource_operation_lock_id('cluster-a')
    second = backend_utils.cluster_resource_operation_lock_id('cluster-b')

    assert first == 'cluster-a_resource_operations'
    assert second == 'cluster-b_resource_operations'
    assert first != second


def test_provision_waits_for_resource_operation_lock(monkeypatch):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    check_existing_cluster = mock.MagicMock(side_effect=_StopTest)
    monkeypatch.setattr(backend, '_check_existing_cluster',
                        check_existing_cluster)
    monkeypatch.setattr(cloud_vm_ray_backend.rich_utils, 'force_update_status',
                        mock.MagicMock())

    status_lock_id = 'cluster-status-lock'
    resource_lock_id = backend_utils.cluster_resource_operation_lock_id(
        'test-cluster')
    events = []
    resource_entered = threading.Event()
    release_resource = threading.Event()

    def lock_factory(lock_id, timeout):
        del timeout
        if lock_id == resource_lock_id:
            return _BlockingLock(lock_id, events, resource_entered,
                                 release_resource)
        assert lock_id == status_lock_id
        return _RecordingLock(lock_id, events)

    monkeypatch.setattr(cloud_vm_ray_backend.lock_events,
                        'DistributedLockEvent', lock_factory)
    errors = []

    def run_provision():
        try:
            backend._locked_provision(  # pylint: disable=protected-access
                status_lock_id, mock.MagicMock(), mock.MagicMock(), False,
                False, 'test-cluster')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)

    thread = threading.Thread(target=run_provision)
    thread.start()
    assert resource_entered.wait(timeout=5)
    check_existing_cluster.assert_not_called()

    release_resource.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], _StopTest)
    check_existing_cluster.assert_called_once()
    assert events == [
        ('enter', status_lock_id),
        ('waiting', resource_lock_id),
        ('enter', resource_lock_id),
        ('exit', resource_lock_id),
        ('exit', status_lock_id),
    ]


def test_dryrun_does_not_acquire_resource_operation_lock(monkeypatch):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    monkeypatch.setattr(backend, '_check_existing_cluster',
                        mock.MagicMock(side_effect=_StopTest))
    monkeypatch.setattr(cloud_vm_ray_backend.rich_utils, 'force_update_status',
                        mock.MagicMock())
    lock_factory = mock.MagicMock(return_value=mock.MagicMock())
    monkeypatch.setattr(cloud_vm_ray_backend.lock_events,
                        'DistributedLockEvent', lock_factory)

    with pytest.raises(_StopTest):
        backend._locked_provision(  # pylint: disable=protected-access
            'cluster-status-lock', mock.MagicMock(), mock.MagicMock(), True,
            False, 'test-cluster')

    lock_factory.assert_called_once_with(
        'cluster-status-lock', cloud_vm_ray_backend._CLUSTER_LOCK_TIMEOUT)  # pylint: disable=protected-access


def test_cancelled_provision_exits_after_acquiring_resource_lock(monkeypatch):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    events = []
    monkeypatch.setattr(
        cloud_vm_ray_backend.lock_events, 'DistributedLockEvent',
        lambda lock_id, timeout: _RecordingLock(lock_id, events))
    monkeypatch.setattr(cloud_vm_ray_backend.rich_utils, 'force_update_status',
                        mock.MagicMock())
    cancelled_context = mock.MagicMock()
    cancelled_context.is_canceled.return_value = True
    monkeypatch.setattr(cloud_vm_ray_backend.context_utils.context, 'get',
                        mock.MagicMock(return_value=cancelled_context))
    get_cluster = mock.MagicMock()
    monkeypatch.setattr(cloud_vm_ray_backend.global_user_state,
                        'get_cluster_from_name', get_cluster)

    with pytest.raises(asyncio.CancelledError):
        backend._locked_provision(  # pylint: disable=protected-access
            'cluster-status-lock', mock.MagicMock(), mock.MagicMock(), False,
            False, 'test-cluster')

    resource_lock_id = backend_utils.cluster_resource_operation_lock_id(
        'test-cluster')
    assert events == [
        ('enter', 'cluster-status-lock'),
        ('enter', resource_lock_id),
        ('exit', resource_lock_id),
        ('exit', 'cluster-status-lock'),
    ]
    get_cluster.assert_not_called()


def test_status_refresh_skips_update_when_resource_lock_is_held(monkeypatch):
    record = {'status': 'cached'}
    resource_lock = mock.MagicMock()
    resource_lock.acquire.side_effect = backend_utils.locks.LockTimeout
    monkeypatch.setattr(backend_utils.locks, 'get_lock',
                        mock.MagicMock(return_value=resource_lock))
    update_cluster_status = mock.MagicMock()
    monkeypatch.setattr(backend_utils, '_update_cluster_status',
                        update_cluster_status)

    result = backend_utils._update_cluster_status_with_resource_lock(  # pylint: disable=protected-access
        'test-cluster',
        record,
        retry_if_missing=False,
        include_user_info=False,
        summary_response=True,
        resource_lock_already_held=False)

    assert result is record
    resource_lock.acquire.assert_called_once_with(blocking=False)
    update_cluster_status.assert_not_called()


def test_forced_incomplete_refresh_returns_cached_without_locking(monkeypatch):
    record = {
        'status': status_lib.ClusterStatus.INIT,
        'handle': mock.MagicMock(launched_resources=None),
    }
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_from_name',
                        mock.MagicMock(return_value=record))
    check_owner = mock.MagicMock()
    monkeypatch.setattr(backend_utils, '_check_owner_identity_with_record',
                        check_owner)
    get_lock = mock.MagicMock()
    monkeypatch.setattr(backend_utils.locks, 'get_lock', get_lock)
    update_cluster_status = mock.MagicMock()
    monkeypatch.setattr(backend_utils, '_update_cluster_status',
                        update_cluster_status)

    result = backend_utils.refresh_cluster_record(
        'test-cluster',
        force_refresh_statuses={status_lib.ClusterStatus.INIT},
        cluster_status_lock_timeout=0,
        include_user_info=False,
        summary_response=True)

    assert result is record
    check_owner.assert_not_called()
    get_lock.assert_not_called()
    update_cluster_status.assert_not_called()


def test_forced_refresh_does_not_wait_for_contended_status_lock(monkeypatch):
    record = {
        'status': status_lib.ClusterStatus.INIT,
        'handle':
            mock.MagicMock(launched_resources=mock.MagicMock(use_spot=False)),
        'autostop': -1,
        'status_updated_at': None,
    }
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_from_name',
                        mock.MagicMock(return_value=record))
    monkeypatch.setattr(backend_utils, '_check_owner_identity_with_record',
                        mock.MagicMock())
    status_lock = mock.MagicMock()
    status_lock.acquire.side_effect = backend_utils.locks.LockTimeout
    monkeypatch.setattr(backend_utils.locks, 'get_lock',
                        mock.MagicMock(return_value=status_lock))
    monkeypatch.setattr(
        backend_utils.time, 'sleep',
        mock.MagicMock(side_effect=AssertionError(
            'opportunistic refresh must not wait for the status lock')))
    update_cluster_status = mock.MagicMock()
    monkeypatch.setattr(backend_utils, '_update_cluster_status',
                        update_cluster_status)

    result = backend_utils.refresh_cluster_record(
        'test-cluster',
        force_refresh_statuses={status_lib.ClusterStatus.INIT},
        cluster_status_lock_timeout=0,
        include_user_info=False,
        summary_response=True)

    assert result is record
    status_lock.acquire.assert_called_once_with(blocking=False)
    update_cluster_status.assert_not_called()


def test_forced_refresh_returns_cached_when_resource_lock_is_held(monkeypatch):
    record = {
        'status': status_lib.ClusterStatus.INIT,
        'handle':
            mock.MagicMock(launched_resources=mock.MagicMock(use_spot=False)),
        'autostop': -1,
        'status_updated_at': None,
    }
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_from_name',
                        mock.MagicMock(return_value=record))
    monkeypatch.setattr(backend_utils, '_check_owner_identity_with_record',
                        mock.MagicMock())
    resource_lock = mock.MagicMock()
    resource_lock.acquire.side_effect = backend_utils.locks.LockTimeout
    monkeypatch.setattr(backend_utils.locks, 'get_lock',
                        mock.MagicMock(return_value=resource_lock))
    update_cluster_status = mock.MagicMock()
    monkeypatch.setattr(backend_utils, '_update_cluster_status',
                        update_cluster_status)

    result = backend_utils.refresh_cluster_record(
        'test-cluster',
        force_refresh_statuses={status_lib.ClusterStatus.INIT},
        cluster_lock_already_held=True,
        include_user_info=False,
        summary_response=True)

    assert result is record
    resource_lock.acquire.assert_called_once_with(blocking=False)
    update_cluster_status.assert_not_called()


def test_nested_status_refresh_reuses_resource_lock(monkeypatch):
    record = {'status': 'cached'}
    get_lock = mock.MagicMock()
    monkeypatch.setattr(backend_utils.locks, 'get_lock', get_lock)
    updated_record = {'status': 'updated'}
    update_cluster_status = mock.MagicMock(return_value=updated_record)
    monkeypatch.setattr(backend_utils, '_update_cluster_status',
                        update_cluster_status)

    result = backend_utils._update_cluster_status_with_resource_lock(  # pylint: disable=protected-access
        'test-cluster',
        record,
        retry_if_missing=False,
        include_user_info=False,
        summary_response=True,
        resource_lock_already_held=True)

    assert result is updated_record
    get_lock.assert_not_called()
    update_cluster_status.assert_called_once_with('test-cluster', record, False,
                                                  False, True, None)


def test_teardown_waits_for_resource_operation_lock(monkeypatch):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = mock.MagicMock(cluster_name='test-cluster')
    teardown_no_lock = mock.MagicMock()
    monkeypatch.setattr(backend, 'teardown_no_lock', teardown_no_lock)
    monkeypatch.setattr(cloud_vm_ray_backend.backend_utils,
                        'check_owner_identity', mock.MagicMock())
    monkeypatch.setattr(cloud_vm_ray_backend.requests_lib,
                        'kill_cluster_requests', mock.MagicMock())

    status_lock_id = backend_utils.cluster_status_lock_id('test-cluster')
    resource_lock_id = backend_utils.cluster_resource_operation_lock_id(
        'test-cluster')
    events = []
    resource_entered = threading.Event()
    release_resource = threading.Event()
    status_lock = _RecordingLock(status_lock_id, events)
    resource_lock = _BlockingLock(resource_lock_id, events, resource_entered,
                                  release_resource)

    def get_lock(lock_id, timeout):
        assert timeout == 1
        if lock_id == status_lock_id:
            return status_lock
        assert lock_id == resource_lock_id
        return resource_lock

    monkeypatch.setattr(cloud_vm_ray_backend.locks, 'get_lock', get_lock)
    errors = []

    def run_teardown():
        try:
            backend._teardown(handle, terminate=False)  # pylint: disable=protected-access
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)

    thread = threading.Thread(target=run_teardown)
    thread.start()
    assert resource_entered.wait(timeout=5)
    teardown_no_lock.assert_not_called()

    release_resource.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert not errors
    teardown_no_lock.assert_called_once_with(handle,
                                             False,
                                             False,
                                             refresh_cluster_status=True)
    assert status_lock.force_unlock_calls == 1
    assert resource_lock.force_unlock_calls == 0
    assert events == [
        ('force_unlock', status_lock_id),
        ('enter', status_lock_id),
        ('waiting', resource_lock_id),
        ('enter', resource_lock_id),
        ('exit', resource_lock_id),
        ('exit', status_lock_id),
    ]
