"""Unit tests for the one-shot request invocation boundary."""
# pylint: disable=protected-access
import concurrent.futures
import dataclasses
import os
import pathlib
import signal
import subprocess
import threading
import time
import unittest.mock

import pytest

from sky import exceptions
from sky.server.requests import process
from sky.server.requests.process import BurstableExecutor
from sky.server.requests.process import DisposableExecutor
from sky.utils import controller_capability


def _wait_until(predicate, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _identity_exists(identity):
    return process._identity_matches(identity)


def _wait_for_direct_child(parent, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for pid in process._direct_child_pids(parent.pid):
            try:
                return process.ProcessIdentity(
                    pid, process._read_process_start_time_ticks(pid))
            except (FileNotFoundError, ProcessLookupError):
                continue
        time.sleep(0.02)
    raise TimeoutError(f'PID {parent.pid} did not publish a direct child')


def dummy_task(sleep_time=0.01):
    time.sleep(sleep_time)
    return True


def failing_task():
    raise ValueError('Task failed')


def retryable_task():
    raise exceptions.ExecutionRetryableError('retry',
                                             hint='retry',
                                             retry_wait_seconds=0)


def terminating_task():
    raise SystemExit(3)


def abruptly_exiting_task():
    os._exit(7)


def hanging_inner_warden(*_args):
    os.setsid()
    while True:
        time.sleep(1)


def guardian_identity_task():
    return os.getpid(), os.getppid()


def capability_owner_proof(expected_capability):
    return {
        'pid': os.getpid(),
        'authorized':
            (controller_capability.get_process_local() == expected_capability),
    }


def touch_task(path):
    pathlib.Path(path).touch()
    return True


def blocking_task(ready_path):
    pathlib.Path(ready_path).touch()
    while True:
        time.sleep(1)


def spawn_session_child_then_wait(handler_path, child_path):
    child = subprocess.Popen(  # pylint: disable=consider-using-with
        [
            '/bin/bash', '-c',
            f'trap "" TERM; echo $$ > {child_path}; while true; do sleep 1; done'
        ],
        start_new_session=True)
    del child
    pathlib.Path(handler_path).touch()
    while True:
        time.sleep(1)


def spawn_unregistered_child_and_succeed(child_path):
    child = subprocess.Popen(  # pylint: disable=consider-using-with
        [
            '/bin/bash', '-c',
            f'trap "" TERM; echo $$ > {child_path}; while true; do sleep 1; done'
        ],
        start_new_session=True)
    deadline = time.monotonic() + 10
    while not pathlib.Path(child_path).exists():
        if time.monotonic() >= deadline:
            raise TimeoutError('child did not publish its PID')
        time.sleep(0.01)
    del child
    return 'success'


def spawn_child_from_background_thread_and_succeed(child_path):
    ready = threading.Event()
    errors = []

    def spawn_and_remain_alive():
        try:
            child = subprocess.Popen(  # pylint: disable=consider-using-with
                [
                    '/bin/bash', '-c', f'trap "" TERM; echo $$ > {child_path}; '
                    'while true; do sleep 1; done'
                ],
                start_new_session=True)
            del child
            deadline = time.monotonic() + 10
            while True:
                try:
                    int(pathlib.Path(child_path).read_text(encoding='utf-8'))
                    break
                except (FileNotFoundError, ValueError) as error:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            'child did not publish its PID') from error
                    time.sleep(0.01)
            ready.set()
            while True:
                time.sleep(1)
        except BaseException as error:  # pylint: disable=broad-except
            errors.append(error)
            ready.set()

    thread = threading.Thread(target=spawn_and_remain_alive, daemon=True)
    thread.start()
    if not ready.wait(timeout=10):
        raise TimeoutError('background thread did not spawn its child')
    if errors:
        raise errors[0]
    return 'success'


def test_guardian_identity_is_published_before_explicit_admission(tmp_path):
    executor = DisposableExecutor(max_workers=1)
    touched = tmp_path / 'admitted'
    try:
        future = executor.submit(touch_task,
                                 str(touched),
                                 admission_gated=True,
                                 receipt_required=True)
        guardian = future.guardian_identity
        assert guardian.pid in executor.workers
        assert process._read_process_start_time_ticks(
            guardian.pid) == guardian.start_time_ticks
        assert not touched.exists()

        future.admit()
        assert future.result(timeout=20) is True
        assert future.boundary_result is not None
        # Durable convergence owns guardian lifetime, not Future visibility.
        assert _identity_exists(guardian)
        future.acknowledge_receipt()
        assert _wait_until(lambda: not _identity_exists(guardian))
    finally:
        executor.shutdown()


def test_cancel_before_admission_is_typed_pre_effect(tmp_path):
    executor = DisposableExecutor(max_workers=1)
    touched = tmp_path / 'not-run'
    try:
        future = executor.submit(touch_task,
                                 str(touched),
                                 admission_gated=True,
                                 receipt_required=True)
        future.request_cancel()
        with pytest.raises(concurrent.futures.CancelledError):
            future.result(timeout=20)
        assert future.boundary_result is not None
        assert (future.boundary_result.outcome.kind
                is process.InvocationOutcomeKind.PRE_EFFECT)
        assert not touched.exists()
        # Cancellation requests drain; they do not waive durable receipt.
        time.sleep(0.2)
        assert _identity_exists(future.guardian_identity)
        future.acknowledge_receipt()
    finally:
        executor.shutdown()


def test_capability_transfer_cancel_before_admission_is_pre_effect(tmp_path):
    """Early cancellation closes the redeemed one-shot authority transport."""
    executor = DisposableExecutor(max_workers=1)
    touched = tmp_path / 'not-run-with-capability'
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b'A' * 43)
    os.close(write_fd)
    try:
        future = executor.submit(touch_task,
                                 str(touched),
                                 admission_gated=True,
                                 receipt_required=True,
                                 capability_fd=read_fd)
        os.close(read_fd)
        read_fd = -1
        future.request_cancel()
        with pytest.raises(concurrent.futures.CancelledError):
            future.result(timeout=20)
        assert future.boundary_result is not None
        assert (future.boundary_result.outcome.kind
                is process.InvocationOutcomeKind.PRE_EFFECT)
        assert not touched.exists()
        future.acknowledge_receipt()
    finally:
        if read_fd >= 0:
            os.close(read_fd)
        executor.shutdown()


def test_capability_skips_setproctitle_until_exact_handler_install(monkeypatch):
    """No outer/inner extension hook runs while their bearer FD is readable."""
    capability = controller_capability.generate()
    capability_fd_read, capability_fd_write = os.pipe()
    os.write(capability_fd_write, capability.encode('ascii'))
    os.close(capability_fd_write)
    calls = []
    real_setproctitle = process.setproctitle.setproctitle

    def hostile_setproctitle(title):
        calls.append((os.getpid(), controller_capability.get_process_local()))
        real_setproctitle(title)

    monkeypatch.setattr(process.setproctitle, 'setproctitle',
                        hostile_setproctitle)
    executor = DisposableExecutor(max_workers=1)
    try:
        future = executor.submit(capability_owner_proof,
                                 capability,
                                 capability_fd=capability_fd_read)
        os.close(capability_fd_read)
        capability_fd_read = -1
        proof = future.result(timeout=20)
    finally:
        if capability_fd_read >= 0:
            os.close(capability_fd_read)
        executor.shutdown()
    # Forked child writes to ``calls`` are copy-on-write, so the parent sees no
    # hook. The callable independently proves only the exact handler is bound.
    assert calls == []
    assert proof['authorized'] is True
    assert proof['pid'] != future.guardian_identity.pid


def test_cancel_drains_term_ignoring_new_session_descendant(tmp_path):
    executor = DisposableExecutor(max_workers=1)
    handler_ready = tmp_path / 'handler-ready'
    child_pid_path = tmp_path / 'child-pid'
    try:
        future = executor.submit(spawn_session_child_then_wait,
                                 str(handler_ready),
                                 str(child_pid_path),
                                 receipt_required=True)
        assert _wait_until(handler_ready.exists)
        assert _wait_until(child_pid_path.exists)
        child_pid = int(child_pid_path.read_text(encoding='utf-8'))
        child_identity = process.ProcessIdentity(
            child_pid, process._read_process_start_time_ticks(child_pid))

        future.request_cancel()
        with pytest.raises(concurrent.futures.CancelledError):
            future.result(timeout=20)
        assert future.boundary_result is not None
        assert (future.boundary_result.outcome.kind
                is process.InvocationOutcomeKind.CANCELLED)
        assert not _identity_exists(child_identity)
        future.acknowledge_receipt()
    finally:
        executor.shutdown()


def test_success_drains_unregistered_descendant(tmp_path):
    executor = DisposableExecutor(max_workers=1)
    child_pid_path = tmp_path / 'child-pid'
    try:
        future = executor.submit(spawn_unregistered_child_and_succeed,
                                 str(child_pid_path),
                                 receipt_required=True)
        assert future.result(timeout=20) == 'success'
        child_pid = int(child_pid_path.read_text(encoding='utf-8'))
        # The unregistered child is absent before the successful result becomes
        # visible, so arbitrary detach cannot hide behind a success receipt.
        with pytest.raises(FileNotFoundError):
            process._read_process_start_time_ticks(child_pid)
        future.acknowledge_receipt()
    finally:
        executor.shutdown()


def test_success_drains_child_forked_by_non_main_handler_thread(tmp_path):
    executor = DisposableExecutor(max_workers=1)
    child_pid_path = tmp_path / 'thread-child-pid'
    try:
        future = executor.submit(spawn_child_from_background_thread_and_succeed,
                                 str(child_pid_path),
                                 receipt_required=True)
        assert future.result(timeout=20) == 'success'
        child_pid = int(child_pid_path.read_text(encoding='utf-8'))
        with pytest.raises(FileNotFoundError):
            process._read_process_start_time_ticks(child_pid)
        future.acknowledge_receipt()
    finally:
        executor.shutdown()


@pytest.mark.parametrize(('task', 'kind', 'error_type'), [
    (failing_task, process.InvocationOutcomeKind.FAILED, ValueError),
    (retryable_task, process.InvocationOutcomeKind.RETRYABLE,
     exceptions.ExecutionRetryableError),
    (terminating_task, process.InvocationOutcomeKind.FAILED,
     process.BoundaryExecutionError),
])
def test_closed_typed_outcomes(task, kind, error_type):
    executor = DisposableExecutor(max_workers=1)
    try:
        future = executor.submit(task, receipt_required=True)
        with pytest.raises(error_type):
            future.result(timeout=20)
        assert future.boundary_result is not None
        assert future.boundary_result.outcome.kind is kind
        future.acknowledge_receipt()
    finally:
        executor.shutdown()


def test_abrupt_handler_exit_is_failed_boundary():
    executor = DisposableExecutor(max_workers=1)
    try:
        future = executor.submit(abruptly_exiting_task, receipt_required=True)
        with pytest.raises(process.BoundaryExecutionError,
                           match='without a typed outcome'):
            future.result(timeout=20)
        assert future.boundary_result is not None
        assert (future.boundary_result.outcome.kind
                is process.InvocationOutcomeKind.FAILED)
        future.acknowledge_receipt()
    finally:
        executor.shutdown()


def test_monitor_start_failure_converges_boundary_synchronously(monkeypatch):
    executor = DisposableExecutor(max_workers=1)
    original_start = threading.Thread.start

    def fail_boundary_monitor(thread):
        if thread.name.startswith('invocation-boundary-monitor-'):
            raise RuntimeError('monitor start failed')
        return original_start(thread)

    monkeypatch.setattr(threading.Thread, 'start', fail_boundary_monitor)
    with pytest.raises(RuntimeError, match='monitor start failed'):
        executor.submit(dummy_task)
    assert not executor.workers
    executor.shutdown()


def test_parent_rejects_guardian_self_reported_birth_tick(monkeypatch):
    executor = DisposableExecutor(max_workers=1)
    original_read = process._read_process_start_time_ticks

    def return_wrong_parent_observation(pid):
        return original_read(pid) + 1

    monkeypatch.setattr(process, '_read_process_start_time_ticks',
                        return_wrong_parent_observation)
    with pytest.raises(process.AmbiguousBoundaryError,
                       match='without an authenticated family-drain'):
        executor.submit(dummy_task)
    assert executor.poisoned
    assert not executor.workers
    with pytest.raises(process.AmbiguousBoundaryError, match='poisoned'):
        executor.shutdown()


def test_pre_ready_inner_hang_is_cancelled_and_reaped_boundedly(monkeypatch):
    executor = DisposableExecutor(max_workers=1)
    monkeypatch.setattr(process, '_SPAWN_CONTEXT',
                        process.multiprocessing.get_context('fork'))
    monkeypatch.setattr(process, '_inner_warden_main', hanging_inner_warden)
    monkeypatch.setattr(process, '_BOUNDARY_START_TIMEOUT_SECONDS', 0.2)
    monkeypatch.setattr(process, '_BOUNDARY_START_CLEANUP_TIMEOUT_SECONDS', 2)

    started_at = time.monotonic()
    with pytest.raises(TimeoutError, match='did not become ready'):
        executor.submit(dummy_task)
    assert time.monotonic() - started_at < 5
    assert not executor.poisoned
    assert not executor.workers
    assert executor.available_slots() == 1
    executor.shutdown()


def test_starting_guardian_consumes_capacity(monkeypatch):
    executor = DisposableExecutor(max_workers=1)
    spawn_process_type = type(process._SPAWN_CONTEXT.Process())
    original_start = spawn_process_type.start
    entered = threading.Event()
    release = threading.Event()

    def blocked_start(child):
        entered.set()
        assert release.wait(timeout=20)
        return original_start(child)

    monkeypatch.setattr(spawn_process_type, 'start', blocked_start)
    futures = []
    errors = []

    def submit_first():
        try:
            futures.append(executor.submit(dummy_task))
        except BaseException as error:  # pylint: disable=broad-except
            errors.append(error)

    thread = threading.Thread(target=submit_first)
    thread.start()
    try:
        assert entered.wait(timeout=20)
        with pytest.raises(exceptions.ExecutionPoolFullError):
            executor.submit(dummy_task)
    finally:
        release.set()
        thread.join(timeout=20)
        executor.shutdown()
    assert not errors
    assert len(futures) == 1


def test_burstable_has_no_reusable_pool_and_both_lanes_are_disposable():
    executor = BurstableExecutor(garanteed_workers=1, burst_workers=1)
    try:
        assert isinstance(executor._executor, DisposableExecutor)
        assert isinstance(executor._burst_executor, DisposableExecutor)
        first = executor.try_reserve_idle_worker()
        second = executor.try_reserve_idle_worker()
        assert first is not None
        assert second is not None
        assert first.lane is process._ExecutorLane.GUARANTEED
        assert second.lane is process._ExecutorLane.BURST
        assert executor.try_reserve_idle_worker() is None
        assert executor.submit_reserved(first, dummy_task).result(timeout=20)
        assert executor.submit_reserved(second, dummy_task).result(timeout=20)
    finally:
        executor.shutdown()


def test_reservation_is_owner_bound_and_one_use():
    first = BurstableExecutor(garanteed_workers=1)
    second = BurstableExecutor(garanteed_workers=1)
    try:
        reservation = first.try_reserve_idle_worker()
        assert reservation is not None
        with pytest.raises(ValueError, match='another executor'):
            second.submit_reserved(reservation, dummy_task)
        forged = dataclasses.replace(reservation)
        with pytest.raises(ValueError, match='stale or consumed'):
            first.submit_reserved(forged, dummy_task)
        future = first.submit_reserved(reservation, dummy_task)
        assert future.result(timeout=20)
        with pytest.raises(ValueError, match='stale or consumed'):
            first.submit_reserved(reservation, dummy_task)
    finally:
        first.shutdown()
        second.shutdown()


def test_reserved_submit_releases_lock_before_process_start(monkeypatch):
    executor = BurstableExecutor(garanteed_workers=1)
    reservation = executor.try_reserve_idle_worker()
    assert reservation is not None
    sentinel = object()

    def submit_without_reservation_lock(*_args, **_kwargs):
        assert executor._reservation_lock.acquire(blocking=False)
        executor._reservation_lock.release()
        return sentinel

    monkeypatch.setattr(executor._executor, '_submit_claimed',
                        submit_without_reservation_lock)
    assert executor.submit_reserved(reservation, dummy_task) is sentinel
    # The stub replaces startup, so release its synthetic starting claim.
    with executor._executor._start_condition:
        executor._executor._starting_workers -= 1
        executor._executor._start_condition.notify_all()
    executor.shutdown()


def test_surviving_inner_reports_pre_effect_after_outer_hard_death():
    executor = DisposableExecutor(max_workers=1)
    future = executor.submit(dummy_task,
                             admission_gated=True,
                             receipt_required=True)
    guardian = future.guardian_identity
    inner = _wait_for_direct_child(guardian)
    try:
        process._send_exact_signal(guardian, signal.SIGKILL)
        with pytest.raises(concurrent.futures.CancelledError):
            future.result(timeout=20)
        assert future.boundary_result is not None
        assert (future.boundary_result.outcome.kind
                is process.InvocationOutcomeKind.PRE_EFFECT)
        assert future.boundary_result.family_drained
        future.acknowledge_receipt()
        assert _wait_until(lambda: not _identity_exists(inner))
    finally:
        executor.shutdown()


def test_surviving_inner_drains_active_effects_after_outer_hard_death(tmp_path):
    executor = DisposableExecutor(max_workers=1)
    handler_ready = tmp_path / 'handler-ready'
    child_pid_path = tmp_path / 'child-pid'
    future = executor.submit(spawn_session_child_then_wait,
                             str(handler_ready),
                             str(child_pid_path),
                             receipt_required=True)
    guardian = future.guardian_identity
    inner = _wait_for_direct_child(guardian)
    try:
        assert _wait_until(handler_ready.exists)
        assert _wait_until(child_pid_path.exists)
        child_pid = int(child_pid_path.read_text(encoding='utf-8'))
        child = process.ProcessIdentity(
            child_pid, process._read_process_start_time_ticks(child_pid))

        process._send_exact_signal(guardian, signal.SIGKILL)
        with pytest.raises(concurrent.futures.CancelledError):
            future.result(timeout=20)
        assert future.boundary_result is not None
        assert (future.boundary_result.outcome.kind
                is process.InvocationOutcomeKind.CANCELLED)
        assert future.boundary_result.family_drained
        assert not _identity_exists(child)
        future.acknowledge_receipt()
        assert _wait_until(lambda: not _identity_exists(inner))
        assert not executor.poisoned
    finally:
        executor.shutdown()


def test_outer_drains_family_after_inner_hard_death(tmp_path):
    executor = DisposableExecutor(max_workers=1)
    handler_ready = tmp_path / 'handler-ready'
    child_pid_path = tmp_path / 'child-pid'
    future = executor.submit(spawn_session_child_then_wait,
                             str(handler_ready),
                             str(child_pid_path),
                             receipt_required=True)
    guardian = future.guardian_identity
    inner = _wait_for_direct_child(guardian)
    try:
        assert _wait_until(handler_ready.exists)
        assert _wait_until(child_pid_path.exists)
        child_pid = int(child_pid_path.read_text(encoding='utf-8'))
        child = process.ProcessIdentity(
            child_pid, process._read_process_start_time_ticks(child_pid))

        process._send_exact_signal(inner, signal.SIGKILL)
        with pytest.raises(process.BoundaryExecutionError,
                           match='Inner warden exited'):
            future.result(timeout=20)
        assert future.boundary_result is not None
        assert future.boundary_result.family_drained
        assert not _identity_exists(child)
        future.acknowledge_receipt()
    finally:
        executor.shutdown()


def test_both_boundary_owners_hard_dead_is_ambiguous_not_terminal():
    executor = DisposableExecutor(max_workers=1)
    future = executor.submit(dummy_task,
                             admission_gated=True,
                             receipt_required=True)
    guardian = future.guardian_identity
    inner = _wait_for_direct_child(guardian)
    try:
        process._send_exact_signal(guardian, signal.SIGSTOP)
        process._send_exact_signal(inner, signal.SIGSTOP)
        process._send_exact_signal(inner, signal.SIGKILL)
        process._send_exact_signal(guardian, signal.SIGKILL)
        with pytest.raises(process.AmbiguousBoundaryError,
                           match='without boundary proof'):
            future.result(timeout=20)
        assert future.boundary_result is None
    finally:
        with pytest.raises(process.AmbiguousBoundaryError, match='poisoned'):
            executor.shutdown()


def test_both_owners_hard_dead_with_active_effects_reports_ambiguous(tmp_path):
    poison_errors = []
    executor = DisposableExecutor(max_workers=1,
                                  on_ambiguous_boundary=poison_errors.append)
    handler_ready = tmp_path / 'handler-ready'
    child_pid_path = tmp_path / 'child-pid'
    future = executor.submit(spawn_session_child_then_wait,
                             str(handler_ready),
                             str(child_pid_path),
                             receipt_required=True)
    guardian = future.guardian_identity
    inner = _wait_for_direct_child(guardian)
    assert _wait_until(handler_ready.exists)
    assert _wait_until(child_pid_path.exists)
    handler = _wait_for_direct_child(inner)
    child_pid = int(child_pid_path.read_text(encoding='utf-8'))
    child = process.ProcessIdentity(
        child_pid, process._read_process_start_time_ticks(child_pid))
    try:
        # Freeze both owners so neither can author a result while the other is
        # killed, then hard-kill them.  The live orphan effects must not retain
        # the spawn sentinel or result endpoint and hide this ambiguity.
        process._send_exact_signal(guardian, signal.SIGSTOP)
        process._send_exact_signal(inner, signal.SIGSTOP)
        process._send_exact_signal(inner, signal.SIGKILL)
        process._send_exact_signal(guardian, signal.SIGKILL)
        with pytest.raises(process.AmbiguousBoundaryError,
                           match='without boundary proof'):
            future.result(timeout=20)
        assert future.boundary_result is None
        assert _identity_exists(handler)
        assert _identity_exists(child)
        assert executor.poisoned
        assert executor.available_slots() == 0
        assert not executor.has_idle_workers()
        with pytest.raises(process.AmbiguousBoundaryError, match='poisoned'):
            executor.submit(dummy_task)
        with pytest.raises(process.AmbiguousBoundaryError, match='poisoned'):
            executor.shutdown(timeout=0)
        assert poison_errors
    finally:
        if _identity_exists(handler):
            process._send_exact_signal(handler, signal.SIGKILL)
        if _identity_exists(child):
            process._send_exact_signal(child, signal.SIGKILL)
        with pytest.raises(process.AmbiguousBoundaryError, match='poisoned'):
            executor.shutdown()


def test_ambiguity_poisons_entire_burstable_facade():
    poison_errors = []
    executor = BurstableExecutor(garanteed_workers=1,
                                 burst_workers=1,
                                 on_ambiguous_boundary=poison_errors.append)
    reservation = executor.try_reserve_idle_worker()
    assert reservation is not None
    future = executor.submit_reserved(reservation,
                                      dummy_task,
                                      admission_gated=True,
                                      receipt_required=True)
    guardian = future.guardian_identity
    inner = _wait_for_direct_child(guardian)
    process._send_exact_signal(guardian, signal.SIGSTOP)
    process._send_exact_signal(inner, signal.SIGSTOP)
    process._send_exact_signal(inner, signal.SIGKILL)
    process._send_exact_signal(guardian, signal.SIGKILL)
    with pytest.raises(process.AmbiguousBoundaryError):
        future.result(timeout=20)
    assert executor.try_reserve_idle_worker() is None
    with pytest.raises(process.AmbiguousBoundaryError, match='shutdown'):
        executor.shutdown(timeout=0)
    assert poison_errors


def test_boundary_result_envelope_rejects_wrong_authentication_token():
    executor = DisposableExecutor(max_workers=1)
    monitor_connection, sender_connection = process.multiprocessing.Pipe(
        duplex=True)
    identity = process.ProcessIdentity(424242, 101)
    future = process.InvocationFuture(identity, monitor_connection, False,
                                      'expected-token')
    assert future.set_running_or_notify_cancel()
    guardian = unittest.mock.Mock(pid=identity.pid, exitcode=0)
    record = process._InvocationRecord(guardian, future)
    monitor = threading.Thread(target=executor._monitor_boundary,
                               args=(record, monitor_connection))
    record.monitor = monitor
    monitor.start()
    result = process.BoundaryResult(
        identity,
        process.InvocationOutcome(process.InvocationOutcomeKind.SUCCEEDED,
                                  value='authenticated'))
    try:
        sender_connection.send(
            process._BoundaryEnvelope('forged-token', process._Event.RESULT,
                                      result))
        time.sleep(0.1)
        assert not future.done()
        sender_connection.send(
            process._BoundaryEnvelope('expected-token', process._Event.RESULT,
                                      result))
        assert future.result(timeout=2) == 'authenticated'
        assert sender_connection.recv() is process._Command.RECEIPT
    finally:
        sender_connection.close()
        monitor.join(timeout=2)
    assert not monitor.is_alive()


def test_shutdown_is_bounded_and_retryable_until_receipt_acknowledged():
    executor = DisposableExecutor(max_workers=1)
    future = executor.submit(dummy_task, receipt_required=True)
    assert future.result(timeout=20)
    guardian = future.guardian_identity

    with pytest.raises(process.BoundaryShutdownPendingError) as error:
        executor.shutdown(timeout=0.1)
    assert error.value.guardians == (guardian,)
    assert _identity_exists(guardian)

    future.acknowledge_receipt()
    executor.shutdown(timeout=5)
    assert not _identity_exists(guardian)


def test_receipt_ack_pipe_loss_after_proven_result_does_not_wedge():
    executor = DisposableExecutor(max_workers=1)
    future = executor.submit(dummy_task, receipt_required=True)
    guardian = future.guardian_identity
    inner = _wait_for_direct_child(guardian)
    assert future.result(timeout=20)
    try:
        process._send_exact_signal(guardian, signal.SIGSTOP)
        process._send_exact_signal(inner, signal.SIGSTOP)
        process._send_exact_signal(inner, signal.SIGKILL)
        process._send_exact_signal(guardian, signal.SIGKILL)
        assert _wait_until(lambda: not _identity_exists(guardian))
        # The peer endpoint is gone, but the already-proven result remains
        # authoritative and acknowledgement is idempotent/non-wedging.
        future.acknowledge_receipt()
        future.acknowledge_receipt()
    finally:
        executor.shutdown()


def test_shutdown_cancels_and_reaps_exact_guardian(tmp_path):
    executor = DisposableExecutor(max_workers=1)
    ready = tmp_path / 'ready'
    future = executor.submit(blocking_task, str(ready), receipt_required=False)
    assert _wait_until(ready.exists)
    guardian = future.guardian_identity
    executor.shutdown()
    assert not _identity_exists(guardian)
    with pytest.raises(concurrent.futures.CancelledError):
        future.result(timeout=1)


def test_process_reap_proof_rejects_alive_child():
    child = unittest.mock.Mock(pid=54321, exitcode=None)
    child.is_alive.return_value = True
    with pytest.raises(RuntimeError, match='54321'):
        process._require_processes_reaped([child])
    child.join.assert_called_once_with(
        timeout=process._PROCESS_REAP_PROOF_TIMEOUT_SECONDS)
