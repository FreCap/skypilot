"""Process/component regression for Serve remote-I/O scheduling.

This rejects the pre-fix design in which readiness and job-status producers
could create independently bounded pools whose aggregate fan-out exhausted the
controller. Exact worker/queue limits, opposite-phase progress, terminal
shutdown, and OS-observed memory are the negative controls; merely completing
the fake calls is not sufficient.
"""

# pylint: disable=protected-access
import multiprocessing
import threading
import time
import traceback

import pytest

from sky.serve import provider_phase
from sky.serve import replica_managers

pytestmark = pytest.mark.component


def _linux_process_memory_kib(field: str) -> int:
    with open('/proc/self/status', encoding='utf-8') as status_file:
        for line in status_file:
            if line.startswith(f'{field}:'):
                _, value, unit = line.split()
                if unit != 'kB':
                    raise RuntimeError(
                        f'Unexpected /proc memory unit: {unit!r}.')
                return int(value)
    raise RuntimeError(f'/proc/self/status has no {field!r} field.')


def _run_remote_io_budget_probe(result_connection) -> None:
    """Exercise the production scheduling owner with narrow fake callables."""
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    max_workers = manager._REMOTE_IO_MAX_PARALLELISM
    probe_workers = manager._REMOTE_PROBE_PARALLELISM
    status_workers = manager._REMOTE_STATUS_PARALLELISM
    work_items_by_lane = {
        replica_managers._ReplicaRemoteIOLane.PROBE:
            probe_workers + manager._REMOTE_PROBE_QUEUE_CAPACITY,
        replica_managers._ReplicaRemoteIOLane.STATUS:
            status_workers + manager._REMOTE_STATUS_QUEUE_CAPACITY,
    }
    if sum(work_items_by_lane.values()) != manager._REMOTE_IO_MAX_OUTSTANDING:
        raise RuntimeError('Remote-I/O lane budgets do not sum to the owner.')
    if (work_items_by_lane[replica_managers._ReplicaRemoteIOLane.PROBE] <=
            probe_workers or
            work_items_by_lane[replica_managers._ReplicaRemoteIOLane.STATUS] <=
            status_workers):
        raise RuntimeError('Remote-I/O component gate must exercise multiple '
                           'production-sized waves in every lane.')
    manager._get_remote_io_executor()
    baseline_rss_kib = _linux_process_memory_kib('VmRSS')
    baseline_hwm_kib = _linux_process_memory_kib('VmHWM')
    release = threading.Event()
    try:
        # Reproduce the dependency that makes one undifferentiated FIFO pool
        # unsafe. Readiness workers wait for an ambient root while the caller
        # owns a V2 root. Job-status children joining that V2 admission must
        # still run through their dedicated share of the aggregate budget.
        readiness_started = 0
        readiness_started_lock = threading.Lock()
        all_readiness_started = threading.Event()

        def _opposite_phase_waiter() -> None:
            nonlocal readiness_started
            with readiness_started_lock:
                readiness_started += 1
                if readiness_started == probe_workers:
                    all_readiness_started.set()
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
                pass

        def _admitted_phase_child(admission) -> None:
            with provider_phase.join_provider_phase(admission):
                pass

        with provider_phase.provider_phase(
                provider_phase.ProviderPhaseMode.V2_FENCED) as admission:
            readiness_futures = [
                manager._submit_remote_io(
                    _opposite_phase_waiter,
                    lane=replica_managers._ReplicaRemoteIOLane.PROBE)
                for _ in range(probe_workers)
            ]
            if not all_readiness_started.wait(timeout=5):
                raise RuntimeError(
                    'Readiness remote-I/O lane did not saturate.')
            job_status_futures = [
                manager._submit_remote_io(
                    _admitted_phase_child,
                    admission,
                    lane=replica_managers._ReplicaRemoteIOLane.STATUS)
                for _ in range(status_workers)
            ]
            for future in job_status_futures:
                future.result(timeout=5)
        for future in readiness_futures:
            future.result(timeout=5)

        active = 0
        peak_active = 0
        completed = 0
        active_lock = threading.Lock()
        saturated = threading.Event()

        def _fake_remote_io(kind: str, index: int) -> tuple[str, int]:
            nonlocal active, peak_active, completed
            # Touch each page so the OS resident-set assertion observes the
            # payload held by every concurrently active fake remote call.
            retained = bytearray(512 * 1024)
            for offset in range(0, len(retained), 4096):
                retained[offset] = 1
            with active_lock:
                active += 1
                peak_active = max(peak_active, active)
                if active == max_workers:
                    saturated.set()
            if not release.wait(timeout=10):
                raise RuntimeError('Fake remote-I/O release was not signaled.')
            with active_lock:
                active -= 1
                completed += 1
            return kind, index

        futures = []
        futures_lock = threading.Lock()

        def _submit(kind: str,
                    lane: replica_managers._ReplicaRemoteIOLane) -> None:
            batch_size = (probe_workers
                          if lane is replica_managers._ReplicaRemoteIOLane.PROBE
                          else status_workers)
            work_items = work_items_by_lane[lane]
            for offset in range(0, work_items, batch_size):
                submitted = [
                    manager._submit_remote_io(_fake_remote_io,
                                              kind,
                                              index,
                                              lane=lane)
                    for index in range(offset,
                                       min(offset + batch_size, work_items))
                ]
                with futures_lock:
                    futures.extend(submitted)
                for future in submitted:
                    future.result(timeout=15)

        producers = [
            threading.Thread(
                target=_submit,
                args=('readiness',
                      replica_managers._ReplicaRemoteIOLane.PROBE)),
            threading.Thread(
                target=_submit,
                args=('job-status',
                      replica_managers._ReplicaRemoteIOLane.STATUS)),
        ]
        for producer in producers:
            producer.start()
        if not saturated.wait(timeout=5):
            raise RuntimeError('Aggregate remote-I/O budget did not saturate.')

        worker_threads_at_peak = [
            thread for thread in threading.enumerate()
            if thread.name.startswith('serve-remote-io')
        ]
        rss_delta_kib = (_linux_process_memory_kib('VmRSS') - baseline_rss_kib)
        hwm_delta_kib = (_linux_process_memory_kib('VmHWM') - baseline_hwm_kib)
        release.set()
        for producer in producers:
            producer.join(timeout=15)
            if producer.is_alive():
                raise RuntimeError('Remote-I/O producer did not finish.')
        results = [future.result(timeout=15) for future in futures]
        manager._shutdown_remote_io_executor()
        remaining_worker_threads = [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith('serve-remote-io')
        ]
        result_connection.send({
            'peak_active': peak_active,
            'worker_threads_at_peak': len(worker_threads_at_peak),
            'completed': completed,
            'result_count': len(results),
            'rss_delta_kib': rss_delta_kib,
            'hwm_delta_kib': hwm_delta_kib,
            'remaining_worker_threads': remaining_worker_threads,
        })
    except BaseException:  # pylint: disable=broad-except
        result_connection.send({'error': traceback.format_exc()})
    finally:
        release.set()
        manager._shutdown_remote_io_executor()
        result_connection.close()


def test_remote_io_budget_is_aggregate_memory_bounded_and_phase_safe():
    """Readiness and job status share one bounded, non-convoying owner."""
    context = multiprocessing.get_context('spawn')
    result_connection, child_connection = context.Pipe(duplex=False)
    process = context.Process(target=_run_remote_io_budget_probe,
                              args=(child_connection,))
    process.start()
    child_connection.close()
    try:
        if not result_connection.poll(45):
            raise AssertionError('Remote-I/O budget subprocess timed out.')
        result = result_connection.recv()
    finally:
        result_connection.close()
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)

    assert process.exitcode == 0
    assert 'error' not in result, result.get('error')
    assert result['peak_active'] == 72
    assert result['worker_threads_at_peak'] == 72
    expected_work_items = (
        replica_managers.SkyPilotReplicaManager._REMOTE_IO_MAX_OUTSTANDING)
    assert result['completed'] == expected_work_items
    assert result['result_count'] == expected_work_items
    assert result['remaining_worker_threads'] == []
    # Seventy-two retained 512-KiB calls, fixed-size producer waves, and
    # Python/thread overhead stay below these OS-observed ceilings.
    assert result['rss_delta_kib'] < 128 * 1024
    assert result['hwm_delta_kib'] < 192 * 1024


def test_remote_io_shutdown_races_submission_and_cancels_queued_work():
    """Terminal close wins the submit race and cannot recreate an owner."""
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    release = threading.Event()

    def _block() -> None:
        release.wait(timeout=10)

    probe_count = (manager._REMOTE_PROBE_PARALLELISM +
                   manager._REMOTE_PROBE_QUEUE_CAPACITY)
    status_count = (manager._REMOTE_STATUS_PARALLELISM +
                    manager._REMOTE_STATUS_QUEUE_CAPACITY)
    futures = [
        manager._submit_remote_io(
            _block, lane=replica_managers._ReplicaRemoteIOLane.PROBE)
        for _ in range(probe_count)
    ]
    futures.extend(
        manager._submit_remote_io(
            _block, lane=replica_managers._ReplicaRemoteIOLane.STATUS)
        for _ in range(status_count))
    try:
        manager._submit_remote_io(
            _block, lane=replica_managers._ReplicaRemoteIOLane.PROBE)
    except RuntimeError as error:
        assert 'queue capacity exhausted' in str(error)
    else:
        raise AssertionError('Probe lane accepted work beyond its hard bound.')
    try:
        manager._submit_remote_io(
            _block, lane=replica_managers._ReplicaRemoteIOLane.STATUS)
    except RuntimeError as error:
        assert 'queue capacity exhausted' in str(error)
    else:
        raise AssertionError('Status lane accepted work beyond its hard bound.')
    assert len(futures) == manager._REMOTE_IO_MAX_OUTSTANDING
    shutdown = threading.Thread(target=manager._shutdown_remote_io_executor)
    shutdown.start()
    deadline = time.monotonic() + 5
    while not manager._remote_io_executor_closed:  # pylint: disable=protected-access
        assert time.monotonic() < deadline, 'terminal close did not linearize'
        time.sleep(0.01)
    try:
        try:
            manager._submit_remote_io(
                _block, lane=replica_managers._ReplicaRemoteIOLane.PROBE)
        except RuntimeError as error:
            assert 'closed' in str(error)
        else:
            raise AssertionError('Submission reopened a terminal owner.')
    finally:
        release.set()
        shutdown.join(timeout=10)

    assert not shutdown.is_alive()
    assert all(future.done() for future in futures)
    assert any(future.cancelled() for future in futures)
    assert not any(
        thread.name.startswith('serve-remote-io')
        for thread in threading.enumerate())
