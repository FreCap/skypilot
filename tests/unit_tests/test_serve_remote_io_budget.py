"""Unpaid aggregate-concurrency coverage for Serve replica remote I/O."""

# pylint: disable=protected-access
import multiprocessing
import threading
import traceback

from sky.serve import provider_phase
from sky.serve import replica_managers


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
    """Exercise the production scheduler with a narrow fake I/O callable."""
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    max_workers = manager._REMOTE_IO_MAX_PARALLELISM
    admitted_workers = max(1, max_workers // 4)
    general_workers = max_workers - admitted_workers
    manager._get_remote_io_executor()
    baseline_children = {
        child.pid for child in multiprocessing.active_children()
    }
    baseline_rss_kib = _linux_process_memory_kib('VmRSS')
    baseline_hwm_kib = _linux_process_memory_kib('VmHWM')
    release = threading.Event()
    try:
        # Reproduce the dependency that makes one undifferentiated FIFO pool
        # unsafe. General workers wait for an ambient root while the caller
        # owns a V2 root. Children joining that V2 admission must still run.
        general_started = 0
        general_started_lock = threading.Lock()
        all_general_started = threading.Event()

        def _opposite_phase_waiter() -> None:
            nonlocal general_started
            with general_started_lock:
                general_started += 1
                if general_started == general_workers:
                    all_general_started.set()
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
                pass

        def _admitted_phase_child(admission) -> None:
            with provider_phase.join_provider_phase(admission):
                pass

        with provider_phase.provider_phase(
                provider_phase.ProviderPhaseMode.V2_FENCED) as admission:
            general_futures = [
                manager._submit_remote_io(_opposite_phase_waiter)
                for _ in range(general_workers)
            ]
            if not all_general_started.wait(timeout=5):
                raise RuntimeError('General remote-I/O lane did not saturate.')
            admitted_futures = [
                manager._submit_remote_io(_admitted_phase_child,
                                          admission,
                                          provider_phase_admitted=True)
                for _ in range(admitted_workers)
            ]
            for future in admitted_futures:
                future.result(timeout=5)
        for future in general_futures:
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
            retained = bytearray(1024 * 1024)
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

        def _submit(kind: str, admitted: bool) -> None:
            submitted = [
                manager._submit_remote_io(_fake_remote_io,
                                          kind,
                                          index,
                                          provider_phase_admitted=admitted)
                for index in range(100)
            ]
            with futures_lock:
                futures.extend(submitted)

        producers = [
            threading.Thread(target=_submit, args=('readiness', False)),
            threading.Thread(target=_submit, args=('job-status', True)),
        ]
        for producer in producers:
            producer.start()
        for producer in producers:
            producer.join(timeout=5)
            if producer.is_alive():
                raise RuntimeError('Remote-I/O producer did not finish.')
        if not saturated.wait(timeout=5):
            raise RuntimeError('Aggregate remote-I/O budget did not saturate.')

        worker_threads_at_peak = [
            thread for thread in threading.enumerate()
            if thread.name.startswith('serve-remote-io')
        ]
        rss_delta_kib = (_linux_process_memory_kib('VmRSS') - baseline_rss_kib)
        hwm_delta_kib = (_linux_process_memory_kib('VmHWM') - baseline_hwm_kib)
        children_at_peak = {
            child.pid for child in multiprocessing.active_children()
        }
        release.set()
        results = [future.result(timeout=15) for future in futures]
        manager._shutdown_remote_io_executor()
        remaining_worker_threads = [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith('serve-remote-io')
        ]
        final_children = {
            child.pid for child in multiprocessing.active_children()
        }
        result_connection.send({
            'peak_active': peak_active,
            'worker_threads_at_peak': len(worker_threads_at_peak),
            'completed': completed,
            'result_count': len(results),
            'rss_delta_kib': rss_delta_kib,
            'hwm_delta_kib': hwm_delta_kib,
            'children_at_peak': sorted(children_at_peak - baseline_children),
            'final_children': sorted(final_children - baseline_children),
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
    assert result['peak_active'] == 32
    assert result['worker_threads_at_peak'] == 32
    assert result['completed'] == 200
    assert result['result_count'] == 200
    assert result['children_at_peak'] == []
    assert result['final_children'] == []
    assert result['remaining_worker_threads'] == []
    # Thirty-two retained 1-MiB fake calls plus Python/thread overhead should
    # stay comfortably below these OS-observed ceilings. An accidental return
    # to 100+ simultaneous calls fails both concurrency and memory assertions.
    assert result['rss_delta_kib'] < 96 * 1024
    assert result['hwm_delta_kib'] < 160 * 1024
