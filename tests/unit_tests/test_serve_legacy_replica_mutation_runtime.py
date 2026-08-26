"""Tests for SkyServe's process-local replica-mutation runtime."""
# pylint: disable=protected-access

import concurrent.futures
import queue
import threading
from unittest import mock

from sky.serve import replica_managers
from sky.utils import thread_utils


def test_runtime_owns_process_local_mutation_state() -> None:
    runtime = replica_managers._ReplicaMutationRuntime()
    other_runtime = replica_managers._ReplicaMutationRuntime()

    assert isinstance(runtime.launch_completion_queue, queue.SimpleQueue)
    assert isinstance(runtime.launch_completion_event, threading.Event)
    assert isinstance(runtime.launch_thread_pool, thread_utils.ThreadSafeDict)
    assert isinstance(runtime.replica_to_request_id,
                      thread_utils.ThreadSafeDict)
    assert isinstance(runtime.replica_to_logical_launch_fence,
                      thread_utils.ThreadSafeDict)
    assert isinstance(runtime.down_thread_pool, thread_utils.ThreadSafeDict)
    assert not runtime.failed_cleanup_retry_attempts
    assert not runtime.failed_cleanup_retry_at
    for field_name in ('launch_completion_queue', 'launch_completion_event',
                       'launch_thread_pool', 'replica_to_request_id',
                       'replica_to_logical_launch_fence', 'down_thread_pool',
                       'failed_cleanup_retry_attempts',
                       'failed_cleanup_retry_at'):
        assert getattr(runtime,
                       field_name) is not getattr(other_runtime, field_name)

    runtime.launch_thread_pool[3] = mock.Mock()
    runtime.replica_to_request_id[3] = 'request-3'
    assert 3 not in other_runtime.launch_thread_pool
    assert 3 not in other_runtime.replica_to_request_id

    attempt, delay = runtime.schedule_failed_cleanup_retry(7, now=100.0)

    assert (attempt, delay) == (1, 60)
    assert runtime.failed_cleanup_retry_attempts == {7: 1}
    assert runtime.failed_cleanup_retry_at == {7: 160.0}
    runtime.clear_failed_cleanup_retry(7)
    assert not runtime.failed_cleanup_retry_attempts
    assert not runtime.failed_cleanup_retry_at


def test_legacy_runtime_adopts_pre_refactor_manager_fields() -> None:
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    # Simulate an object reconstructed from the pre-runtime layout while still
    # exercising the manager's supported allocation path.
    manager.__dict__.pop('_legacy_mutation_runtime')
    old_completion_queue: queue.SimpleQueue[int] = queue.SimpleQueue()
    old_completion_event = threading.Event()
    old_launch_pool = thread_utils.ThreadSafeDict()
    old_request_ids = thread_utils.ThreadSafeDict()
    old_logical_fences = thread_utils.ThreadSafeDict()
    old_down_pool = thread_utils.ThreadSafeDict()
    old_retry_attempts = {1: 3}
    old_retry_at = {1: 200.0}
    manager.__dict__.update({
        '_launch_completion_queue': old_completion_queue,
        '_launch_completion_event': old_completion_event,
        '_launch_thread_pool': old_launch_pool,
        '_replica_to_request_id': old_request_ids,
        '_replica_to_logical_launch_fence': old_logical_fences,
        '_down_thread_pool': old_down_pool,
        '_failed_cleanup_retry_attempts': old_retry_attempts,
        '_failed_cleanup_retry_at': old_retry_at,
    })

    runtime = manager._legacy_mutation_runtime_state()

    assert runtime.launch_completion_queue is old_completion_queue
    assert runtime.launch_completion_event is old_completion_event
    assert runtime.launch_thread_pool is old_launch_pool
    assert runtime.replica_to_request_id is old_request_ids
    assert runtime.replica_to_logical_launch_fence is old_logical_fences
    assert runtime.down_thread_pool is old_down_pool
    assert runtime.failed_cleanup_retry_attempts is old_retry_attempts
    assert runtime.failed_cleanup_retry_at is old_retry_at
    for legacy_name, runtime_name in manager._LEGACY_MUTATION_FIELD_MAP.items():
        assert manager.__dict__[legacy_name] is getattr(runtime, runtime_name)
    assert manager._launch_thread_pool is old_launch_pool

    replacement_pool = thread_utils.ThreadSafeDict()
    manager._launch_thread_pool = replacement_pool
    assert runtime.launch_thread_pool is replacement_pool


def test_legacy_runtime_proxies_restore_instance_patch_by_identity() -> None:
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    runtime = manager._legacy_mutation_runtime_state()

    for create in (False, True):
        for legacy_name, runtime_name in (
                manager._LEGACY_MUTATION_FIELD_MAP.items()):
            original = getattr(manager, legacy_name)
            replacement = object()
            with mock.patch.object(manager,
                                   legacy_name,
                                   replacement,
                                   create=create):
                assert getattr(manager, legacy_name) is replacement
                assert getattr(runtime, runtime_name) is replacement
                assert manager.__dict__[legacy_name] is replacement
            assert getattr(manager, legacy_name) is original
            assert getattr(runtime, runtime_name) is original
            assert manager.__dict__[legacy_name] is original


def test_legacy_runtime_proxies_recreate_defaults_after_delete() -> None:
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    runtime = manager._legacy_mutation_runtime_state()

    for legacy_name, runtime_name in manager._LEGACY_MUTATION_FIELD_MAP.items():
        original = getattr(manager, legacy_name)
        delattr(manager, legacy_name)
        replacement = getattr(manager, legacy_name)
        assert replacement is getattr(runtime, runtime_name)
        assert replacement is not original


def test_legacy_runtime_adoption_is_atomic_across_first_callers() -> None:
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    # Simulate a pre-runtime reconstruction with only the legacy fields below.
    manager.__dict__.pop('_legacy_mutation_runtime')
    old_completion_queue: queue.SimpleQueue[int] = queue.SimpleQueue()
    old_completion_queue.put(17)
    old_completion_event = threading.Event()
    old_completion_event.set()
    old_launch_pool = thread_utils.ThreadSafeDict()
    old_launch_pool[3] = mock.sentinel.worker
    manager.__dict__.update({
        '_launch_completion_queue': old_completion_queue,
        '_launch_completion_event': old_completion_event,
        '_launch_thread_pool': old_launch_pool,
    })

    barrier = threading.Barrier(2)
    serialize = threading.Lock()

    class _BarrierLock:

        def __enter__(self) -> None:
            barrier.wait(timeout=5)
            serialize.acquire()

        def __exit__(self, *_: object) -> None:
            serialize.release()

    with mock.patch.object(replica_managers.SkyPilotReplicaManager,
                           '_LEGACY_MUTATION_RUNTIME_INIT_LOCK',
                           _BarrierLock()):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(lambda _: manager._legacy_mutation_runtime_state(),
                         range(2)))

    assert results[0] is results[1]
    runtime = results[0]
    assert runtime.launch_completion_queue is old_completion_queue
    assert runtime.launch_completion_queue.get_nowait() == 17
    assert runtime.launch_completion_event is old_completion_event
    assert runtime.launch_completion_event.is_set()
    assert runtime.launch_thread_pool is old_launch_pool
    assert runtime.launch_thread_pool[3] is mock.sentinel.worker
    for legacy_name, runtime_name in manager._LEGACY_MUTATION_FIELD_MAP.items():
        assert manager.__dict__[legacy_name] is getattr(runtime, runtime_name)


def test_recovery_and_refresh_route_through_legacy_runtime() -> None:
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager.lock = threading.Lock()
    manager._superseded_prune_pending = False
    runtime = replica_managers._ReplicaMutationRuntime()
    runtime.recover = mock.Mock()
    runtime.refresh = mock.Mock()
    manager._publish_legacy_mutation_runtime_state(runtime)
    recover = mock.Mock()
    refresh = mock.Mock()
    manager._recover_legacy_replica_operations = recover
    manager._refresh_legacy_mutation_runtime = refresh

    manager._recover_replica_operations()
    manager._refresh_thread_pool()

    runtime.recover.assert_called_once()
    runtime.refresh.assert_called_once()
    runtime.recover.call_args.args[0]()
    runtime.refresh.call_args.args[0]()
    recover.assert_called_once_with()
    refresh.assert_called_once_with()
