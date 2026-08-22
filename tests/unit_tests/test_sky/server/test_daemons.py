"""Tests for sky.server.daemons."""
# pylint: disable=protected-access
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import threading
import time
from unittest import mock

import pytest

from sky import skypilot_config
from sky.serve import constants as serve_constants
from sky.serve import reserved_capacity
from sky.serve import serve_state_schema
from sky.server import daemons
from sky.server.requests import process as request_process


def _wait_until(predicate, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _wait_for_direct_child(parent, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for pid in request_process._direct_child_pids(parent.pid):
            try:
                return request_process.ProcessIdentity(
                    pid, request_process._read_process_start_time_ticks(pid))
            except (FileNotFoundError, ProcessLookupError):
                continue
        time.sleep(0.02)
    raise TimeoutError(f'PID {parent.pid} did not publish a direct child')


def _stubborn_renewal_family(handler_ready_path, child_pid_path):
    child = subprocess.Popen(  # pylint: disable=consider-using-with
        [
            sys.executable, '-c', 'import signal,time; '
            'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
            'time.sleep(300)'
        ],
        start_new_session=True)
    pathlib.Path(child_pid_path).write_text(str(child.pid), encoding='utf-8')
    pathlib.Path(handler_ready_path).touch()
    del child
    while True:
        time.sleep(1)


def _successful_renewal():
    return True


def _mock_get_nested(max_bytes):
    """Return a patched get_nested that overrides daemon_log_max_bytes."""
    original = skypilot_config.get_nested

    def patched(keys, default=None):
        if keys == ('api_server', 'daemon_log_max_bytes'):
            return max_bytes
        return original(keys, default)

    return patched


def test_reclaim_proof_daemon_owns_renewal_during_controller_hold(monkeypatch):
    monkeypatch.setenv(serve_constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'true')
    monkeypatch.setattr(daemons, '_reserved_fill_reclaim_proof_executor', None)
    executor = mock.Mock()
    executor_factory = mock.Mock(return_value=executor)
    monkeypatch.setattr(request_process, 'DisposableExecutor', executor_factory)
    renew = mock.Mock()
    monkeypatch.setattr(reserved_capacity,
                        'renew_reclaim_provider_proofs_in_boundary', renew)
    sleep = mock.Mock()
    monkeypatch.setattr(daemons.time, 'sleep', sleep)

    daemons.reserved_fill_reclaim_proof_renewal_event()

    executor_factory.assert_called_once_with(max_workers=1)
    renew.assert_called_once_with(executor)
    sleep.assert_called_once_with(
        reserved_capacity.reserved_fill_reclaim_attestation.
        PROVIDER_PROOF_RENEW_INTERVAL_SECONDS)


def test_reclaim_proof_daemon_checks_inactive_gate_inside_boundary(monkeypatch):
    monkeypatch.setattr(daemons, '_reserved_fill_reclaim_proof_executor', None)
    executor = mock.Mock()
    executor_factory = mock.Mock(return_value=executor)
    monkeypatch.setattr(request_process, 'DisposableExecutor', executor_factory)
    renew = mock.Mock(return_value=False)
    monkeypatch.setattr(reserved_capacity,
                        'renew_reclaim_provider_proofs_in_boundary', renew)
    monkeypatch.setattr(daemons.time, 'sleep', mock.Mock())

    daemons.reserved_fill_reclaim_proof_renewal_event()

    executor_factory.assert_called_once()
    renew.assert_called_once_with(executor)


def test_reclaim_proof_daemon_renews_beyond_receipt_ttl(monkeypatch):
    monkeypatch.setattr(daemons, '_reserved_fill_reclaim_proof_executor', None)
    executor = mock.Mock()
    executor_factory = mock.Mock(return_value=executor)
    monkeypatch.setattr(request_process, 'DisposableExecutor', executor_factory)
    renew = mock.Mock()
    monkeypatch.setattr(reserved_capacity,
                        'renew_reclaim_provider_proofs_in_boundary', renew)
    elapsed = 0.0

    def advance(interval):
        nonlocal elapsed
        elapsed += interval

    monkeypatch.setattr(daemons.time, 'sleep', advance)
    interval = (reserved_capacity.reserved_fill_reclaim_attestation.
                PROVIDER_PROOF_RENEW_INTERVAL_SECONDS)
    iterations = int(reserved_capacity.reserved_fill_reclaim_attestation.
                     PROVIDER_PROOF_MAX_AGE_SECONDS / interval) + 1

    for _ in range(iterations):
        daemons.reserved_fill_reclaim_proof_renewal_event()

    assert elapsed > (reserved_capacity.reserved_fill_reclaim_attestation.
                      PROVIDER_PROOF_MAX_AGE_SECONDS)
    assert renew.call_args_list == [mock.call(executor)] * iterations
    executor_factory.assert_called_once()


def test_reclaim_proof_daemon_skips_zero_entrypoint_deployment(monkeypatch):
    discovered = mock.Mock()
    discovered.select.return_value = ()
    monkeypatch.setattr(daemons.importlib.metadata, 'entry_points',
                        lambda: discovered)
    engine = mock.Mock()
    engine.dialect.name = 'postgresql'
    monkeypatch.setattr(serve_state_schema, 'get_database_engine',
                        lambda: engine)

    assert daemons.should_skip_reserved_fill_reclaim_proof_renewal()


def test_reclaim_proof_daemon_skips_non_postgres_with_policy(monkeypatch):
    engine = mock.Mock()
    engine.dialect.name = 'sqlite'
    monkeypatch.setattr(serve_state_schema, 'get_database_engine',
                        lambda: engine)
    discover = mock.Mock(side_effect=pytest.fail)
    monkeypatch.setattr(daemons.importlib.metadata, 'entry_points', discover)

    assert daemons.should_skip_reserved_fill_reclaim_proof_renewal()
    discover.assert_not_called()


def test_reclaim_proof_daemon_selects_installed_policy(monkeypatch):
    discovered = mock.Mock()
    discovered.select.return_value = (mock.sentinel.policy_entrypoint,)
    monkeypatch.setattr(daemons.importlib.metadata, 'entry_points',
                        lambda: discovered)
    engine = mock.Mock()
    engine.dialect.name = 'postgresql'
    monkeypatch.setattr(serve_state_schema, 'get_database_engine',
                        lambda: engine)
    executor_factory = mock.Mock()
    monkeypatch.setattr(request_process, 'DisposableExecutor', executor_factory)

    assert not daemons.should_skip_reserved_fill_reclaim_proof_renewal()
    executor_factory.assert_not_called()


def test_reclaim_proof_daemon_discovery_uncertainty_selects(monkeypatch):
    monkeypatch.setattr(daemons.importlib.metadata, 'entry_points',
                        mock.Mock(side_effect=RuntimeError('discovery failed')))
    engine = mock.Mock()
    engine.dialect.name = 'postgresql'
    monkeypatch.setattr(serve_state_schema, 'get_database_engine',
                        lambda: engine)

    assert not daemons.should_skip_reserved_fill_reclaim_proof_renewal()


def test_reclaim_proof_daemon_cleanup_drains_retained_boundary(monkeypatch):
    executor = mock.Mock()
    shutdown = mock.Mock()
    monkeypatch.setattr(daemons, '_reserved_fill_reclaim_proof_executor',
                        executor)
    monkeypatch.setattr(reserved_capacity,
                        'shutdown_reclaim_provider_proof_boundary', shutdown)

    daemons._close_reserved_fill_reclaim_proof_executor()

    shutdown.assert_called_once_with(executor)
    assert daemons._reserved_fill_reclaim_proof_executor is None


def test_reclaim_proof_ambiguity_parks_deployment_owner(monkeypatch):
    park = mock.Mock(side_effect=SystemExit(1))
    monkeypatch.setattr(daemons.time, 'sleep', park)
    error = request_process.AmbiguousBoundaryError('unproved family')

    with pytest.raises(SystemExit):
        daemons._park_reclaim_provider_proof_owner(error)

    park.assert_called_once_with(3600)


def test_reclaim_proof_daemon_ambiguity_has_outer_park(monkeypatch):
    error = request_process.AmbiguousBoundaryError('unproved family')
    park = mock.Mock(side_effect=SystemExit(1))
    monkeypatch.setattr(daemons, '_reserved_fill_reclaim_proof_executor', None)
    monkeypatch.setattr(daemons.time, 'sleep', mock.Mock())
    monkeypatch.setattr(request_process, 'DisposableExecutor', mock.Mock())
    monkeypatch.setattr(reserved_capacity,
                        'renew_reclaim_provider_proofs_in_boundary',
                        mock.Mock(side_effect=error))
    monkeypatch.setattr(daemons, '_park_reclaim_provider_proof_owner', park)

    with pytest.raises(SystemExit):
        daemons.reserved_fill_reclaim_proof_renewal_event()

    park.assert_called_once_with(error)


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='requires Linux process identities and signals')
def test_async_ambiguity_keeps_poisoned_owner_and_blocks_successor(
        monkeypatch, tmp_path):
    """A detached provider family cannot trigger an in-Pod replacement."""
    handler_ready = tmp_path / 'handler-ready'
    child_pid_path = tmp_path / 'child-pid'
    executor = request_process.DisposableExecutor(max_workers=1)
    future = executor.submit(_stubborn_renewal_family,
                             str(handler_ready),
                             str(child_pid_path),
                             receipt_required=True)
    guardian = future.guardian_identity
    inner = _wait_for_direct_child(guardian)
    handler = None
    child = None
    parked = threading.Event()
    release_test_park = threading.Event()
    park_errors = []

    def park(error):
        park_errors.append(error)
        parked.set()
        assert release_test_park.wait(timeout=20)

    try:
        assert _wait_until(handler_ready.exists)
        assert _wait_until(child_pid_path.exists)
        handler = _wait_for_direct_child(inner)
        child_pid = int(child_pid_path.read_text(encoding='utf-8'))
        child = request_process.ProcessIdentity(
            child_pid,
            request_process._read_process_start_time_ticks(child_pid))

        # Remove both authenticated owners while the handler and its separate
        # session child remain live. The monitor must finish poisoning before
        # the deployment event, rather than parking inside monitor cleanup.
        request_process._send_exact_signal(guardian, signal.SIGSTOP)
        request_process._send_exact_signal(inner, signal.SIGSTOP)
        request_process._send_exact_signal(inner, signal.SIGKILL)
        request_process._send_exact_signal(guardian, signal.SIGKILL)
        with pytest.raises(request_process.AmbiguousBoundaryError,
                           match='without boundary proof'):
            future.result(timeout=20)
        assert executor.poisoned
        assert _wait_until(lambda: not executor.workers)
        assert request_process._identity_matches(handler)
        assert request_process._identity_matches(child)

        monkeypatch.setattr(daemons, '_reserved_fill_reclaim_proof_executor',
                            executor)
        constructor = mock.Mock(
            side_effect=AssertionError('must not construct a successor lane'))
        monkeypatch.setattr(request_process, 'DisposableExecutor', constructor)
        monkeypatch.setattr(daemons, '_park_reclaim_provider_proof_owner', park)
        monkeypatch.setattr(daemons.time, 'sleep', mock.Mock())
        event_thread = threading.Thread(
            target=daemons.reserved_fill_reclaim_proof_renewal_event,
            name='test-renewal-owner',
            daemon=True)
        event_thread.start()

        assert parked.wait(timeout=20)
        assert event_thread.is_alive()
        assert daemons._reserved_fill_reclaim_proof_executor is executor
        constructor.assert_not_called()
        assert request_process._identity_matches(handler)
        assert request_process._identity_matches(child)
        with pytest.raises(request_process.AmbiguousBoundaryError,
                           match='poisoned'):
            executor.submit(bool)

        release_test_park.set()
        event_thread.join(timeout=20)
        assert not event_thread.is_alive()
        assert len(park_errors) == 1
    finally:
        release_test_park.set()
        for identity in (handler, child):
            if (identity is not None and
                    request_process._identity_matches(identity)):
                request_process._send_exact_signal(identity, signal.SIGKILL)
        if handler is not None:
            assert _wait_until(
                lambda: not request_process._identity_matches(handler))
        if child is not None:
            assert _wait_until(
                lambda: not request_process._identity_matches(child))
        with pytest.raises(request_process.AmbiguousBoundaryError,
                           match='poisoned'):
            executor.shutdown(timeout=20)


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='requires Linux process identities and signals')
def test_post_result_reap_ambiguity_parks_on_next_tick(monkeypatch):
    """Late monitor poison after success cannot replace the retained lane."""
    executor = request_process.DisposableExecutor(max_workers=1)
    future = executor.submit(_successful_renewal, receipt_required=True)
    guardian = future.guardian_identity
    inner = _wait_for_direct_child(guardian)
    parked = threading.Event()
    release_test_park = threading.Event()

    def park(_error):
        parked.set()
        assert release_test_park.wait(timeout=20)

    try:
        assert future.result(timeout=20) is True
        record = executor._invocations[guardian.pid]
        real_guardian = record.guardian
        # Fault-inject a missing parent-side lifetime proof after the typed,
        # drained result has already become visible. The real inner warden can
        # still finish its receipt path; only the local guardian-reap proof is
        # made unavailable to the monitor.
        record.guardian = mock.Mock(pid=guardian.pid, exitcode=None)
        request_process._send_exact_signal(guardian, signal.SIGKILL)
        future.acknowledge_receipt()
        assert _wait_until(lambda: executor.poisoned)
        assert future.result() is True
        assert _wait_until(lambda: not executor.workers)
        real_guardian.join(timeout=20)

        monkeypatch.setattr(daemons, '_reserved_fill_reclaim_proof_executor',
                            executor)
        constructor = mock.Mock(
            side_effect=AssertionError('must not construct a successor lane'))
        monkeypatch.setattr(request_process, 'DisposableExecutor', constructor)
        monkeypatch.setattr(daemons, '_park_reclaim_provider_proof_owner', park)
        monkeypatch.setattr(daemons.time, 'sleep', mock.Mock())
        event_thread = threading.Thread(
            target=daemons.reserved_fill_reclaim_proof_renewal_event,
            name='test-post-result-renewal-owner',
            daemon=True)
        event_thread.start()

        assert parked.wait(timeout=20)
        assert event_thread.is_alive()
        assert daemons._reserved_fill_reclaim_proof_executor is executor
        constructor.assert_not_called()
        release_test_park.set()
        event_thread.join(timeout=20)
        assert not event_thread.is_alive()
    finally:
        release_test_park.set()
        if request_process._identity_matches(guardian):
            request_process._send_exact_signal(guardian, signal.SIGKILL)
        if request_process._identity_matches(inner):
            request_process._send_exact_signal(inner, signal.SIGKILL)
        assert _wait_until(
            lambda: not request_process._identity_matches(guardian))
        assert _wait_until(lambda: not request_process._identity_matches(inner))
        with pytest.raises(request_process.AmbiguousBoundaryError,
                           match='poisoned'):
            executor.shutdown(timeout=20)


class TestDaemonLogRotation:
    """Tests for daemon log rotation."""

    def _redirect_stdout_stderr(self, fd: int):
        """Redirect stdout and stderr to the given fd via dup2."""
        os.dup2(fd, sys.stdout.fileno())
        os.dup2(fd, sys.stderr.fileno())

    def test_rotates_when_exceeds_threshold(self, monkeypatch):
        """Log is backed up to .log.1 and truncated when exceeding threshold."""
        threshold = 1024  # 1 KB for testing
        monkeypatch.setattr(skypilot_config, 'get_nested',
                            _mock_get_nested(threshold))

        saved_stdout_fd = os.dup(sys.stdout.fileno())
        saved_stderr_fd = os.dup(sys.stderr.fileno())
        try:
            with tempfile.NamedTemporaryFile(mode='ab',
                                             delete=False,
                                             suffix='.log') as f:
                tmp_path = f.name
                backup_path = tmp_path + '.1'
                # Open with O_APPEND to mimic executor.py behavior.
                append_fd = os.open(tmp_path,
                                    os.O_WRONLY | os.O_APPEND | os.O_CREAT)
                self._redirect_stdout_stderr(append_fd)

                # Write data exceeding the threshold.
                data = b'x' * (threshold + 100)
                os.write(sys.stdout.fileno(), data)
                sys.stdout.flush()
                assert os.fstat(sys.stdout.fileno()).st_size == threshold + 100

                # Rotation should happen.
                daemons._rotate_daemon_log(tmp_path)
                assert os.fstat(sys.stdout.fileno()).st_size == 0

                # Backup should contain the original data.
                with open(backup_path, 'rb') as check:
                    assert check.read() == data

                # Writes after rotation should start from position 0
                # (no sparse hole).
                msg = b'hello after rotation\n'
                os.write(sys.stdout.fileno(), msg)
                sys.stdout.flush()
                assert os.fstat(sys.stdout.fileno()).st_size == len(msg)

                # Verify the content on disk.
                with open(tmp_path, 'rb') as check:
                    assert check.read() == msg

                os.close(append_fd)
        finally:
            # Restore original stdout/stderr.
            os.dup2(saved_stdout_fd, sys.stdout.fileno())
            os.dup2(saved_stderr_fd, sys.stderr.fileno())
            os.close(saved_stdout_fd)
            os.close(saved_stderr_fd)
            os.unlink(tmp_path)
            if os.path.exists(backup_path):
                os.unlink(backup_path)

    def test_old_backup_replaced_on_next_rotation(self, monkeypatch):
        """Old .log.1 backup is replaced on subsequent rotation."""
        threshold = 1024
        monkeypatch.setattr(skypilot_config, 'get_nested',
                            _mock_get_nested(threshold))

        saved_stdout_fd = os.dup(sys.stdout.fileno())
        saved_stderr_fd = os.dup(sys.stderr.fileno())
        try:
            with tempfile.NamedTemporaryFile(mode='ab',
                                             delete=False,
                                             suffix='.log') as f:
                tmp_path = f.name
                backup_path = tmp_path + '.1'
                append_fd = os.open(tmp_path,
                                    os.O_WRONLY | os.O_APPEND | os.O_CREAT)
                self._redirect_stdout_stderr(append_fd)

                # First rotation.
                first_data = b'A' * (threshold + 100)
                os.write(sys.stdout.fileno(), first_data)
                sys.stdout.flush()
                daemons._rotate_daemon_log(tmp_path)
                with open(backup_path, 'rb') as check:
                    assert check.read() == first_data

                # Write new data exceeding threshold again.
                second_data = b'B' * (threshold + 200)
                os.write(sys.stdout.fileno(), second_data)
                sys.stdout.flush()
                daemons._rotate_daemon_log(tmp_path)

                # Backup should now contain second data, not first.
                with open(backup_path, 'rb') as check:
                    assert check.read() == second_data

                os.close(append_fd)
        finally:
            os.dup2(saved_stdout_fd, sys.stdout.fileno())
            os.dup2(saved_stderr_fd, sys.stderr.fileno())
            os.close(saved_stdout_fd)
            os.close(saved_stderr_fd)
            os.unlink(tmp_path)
            if os.path.exists(backup_path):
                os.unlink(backup_path)

    def test_no_rotation_when_under_threshold(self, monkeypatch):
        """No rotation or backup when log size is under threshold."""
        threshold = 1024
        monkeypatch.setattr(skypilot_config, 'get_nested',
                            _mock_get_nested(threshold))

        saved_stdout_fd = os.dup(sys.stdout.fileno())
        saved_stderr_fd = os.dup(sys.stderr.fileno())
        try:
            with tempfile.NamedTemporaryFile(mode='ab',
                                             delete=False,
                                             suffix='.log') as f:
                tmp_path = f.name
                backup_path = tmp_path + '.1'
                append_fd = os.open(tmp_path,
                                    os.O_WRONLY | os.O_APPEND | os.O_CREAT)
                self._redirect_stdout_stderr(append_fd)

                # Write data under the threshold.
                data = b'x' * (threshold - 100)
                os.write(sys.stdout.fileno(), data)
                sys.stdout.flush()

                daemons._rotate_daemon_log(tmp_path)

                # File should not be truncated.
                assert os.fstat(sys.stdout.fileno()).st_size == threshold - 100
                # No backup should be created.
                assert not os.path.exists(backup_path)

                os.close(append_fd)
        finally:
            os.dup2(saved_stdout_fd, sys.stdout.fileno())
            os.dup2(saved_stderr_fd, sys.stderr.fileno())
            os.close(saved_stdout_fd)
            os.close(saved_stderr_fd)
            os.unlink(tmp_path)

    def test_rotation_disabled_when_max_bytes_zero(self, monkeypatch):
        """Rotation is disabled when max_bytes is set to 0."""
        monkeypatch.setattr(skypilot_config, 'get_nested', _mock_get_nested(0))

        saved_stdout_fd = os.dup(sys.stdout.fileno())
        saved_stderr_fd = os.dup(sys.stderr.fileno())
        try:
            with tempfile.NamedTemporaryFile(mode='ab',
                                             delete=False,
                                             suffix='.log') as f:
                tmp_path = f.name
                backup_path = tmp_path + '.1'
                append_fd = os.open(tmp_path,
                                    os.O_WRONLY | os.O_APPEND | os.O_CREAT)
                self._redirect_stdout_stderr(append_fd)

                # Write data that would normally exceed a threshold.
                data = b'x' * 2048
                os.write(sys.stdout.fileno(), data)
                sys.stdout.flush()

                daemons._rotate_daemon_log(tmp_path)

                # File should not be truncated.
                assert os.fstat(sys.stdout.fileno()).st_size == 2048
                # No backup should be created.
                assert not os.path.exists(backup_path)

                os.close(append_fd)
        finally:
            os.dup2(saved_stdout_fd, sys.stdout.fileno())
            os.dup2(saved_stderr_fd, sys.stderr.fileno())
            os.close(saved_stdout_fd)
            os.close(saved_stderr_fd)
            os.unlink(tmp_path)


class TestConsolidationEventInstancePersistence:
    """The consolidation-mode refresh daemons must reuse a single
    SkyletEvent instance across iterations.

    Background: SkyletEvent.run() throttles its expensive `_run()`
    callback via a per-instance counter `_n` that accumulates across
    calls. The outer RuntimeDaemon loop re-invokes the
    daemon's event_fn repeatedly; if the event is freshly instantiated
    on every call, `_n` resets to 0, run() only advances it to 1,
    `_n == 0` never re-fires, and the throttled work
    (update_service_status / managed-job status refresh) is silently
    skipped forever.

    These tests assert the instance is created once and reused so the
    counter persists. With the bug, the constructor is invoked on
    every iteration; with the fix, exactly once."""

    # Attribute names the fix introduces. Use getattr/setattr so that if
    # the fix is reverted the fixture still runs cleanly; the actual test
    # assertions (call_count) then become the failure signal.
    _EVENT_ATTRS = ('_pool_status_update_event', '_serve_status_update_event')

    @pytest.fixture(autouse=True)
    def _reset_module_state(self):
        # Pre-populate the consolidation locks with mocks that look
        # already-locked so the daemon never tries to acquire a real
        # advisory lock during the test.
        fake_lock = mock.MagicMock()
        fake_lock.is_locked.return_value = True
        prior_locks = (daemons._pool_consolidation_mode_lock,
                       daemons._serve_consolidation_mode_lock)
        prior_events = {
            name: getattr(daemons, name, None) for name in self._EVENT_ATTRS
        }
        daemons._pool_consolidation_mode_lock = fake_lock
        daemons._serve_consolidation_mode_lock = fake_lock
        for name in self._EVENT_ATTRS:
            setattr(daemons, name, None)
        yield
        (daemons._pool_consolidation_mode_lock,
         daemons._serve_consolidation_mode_lock) = prior_locks
        for name, value in prior_events.items():
            setattr(daemons, name, value)

    def test_serve_event_instance_is_reused_across_iterations(self):
        with mock.patch(
                'sky.serve.serve_utils.ha_recovery_for_consolidation_mode'), \
             mock.patch('sky.skylet.events.ServiceUpdateEvent') as mock_event, \
             mock.patch.object(daemons.time, 'sleep'):
            for _ in range(3):
                daemons._serve_status_refresh_event(pool=False)
            # Without the fix this would be 3 (one ctor per iteration).
            # With the fix the cached instance is reused.
            mock_event.assert_called_once_with(pool=False)
            # run() is invoked on the SAME instance once per iteration,
            # so its internal `_n` counter accumulates as designed.
            assert mock_event.return_value.run.call_count == 3

    def test_serve_hold_skips_lock_recovery_and_status(self, monkeypatch):
        monkeypatch.setenv(serve_constants.SERVE_CONTROLLER_HOLD_ENV_VAR,
                           'true')
        with mock.patch.object(daemons, '_ensure_leader_lock') as ensure_lock, \
             mock.patch(
                 'sky.serve.serve_utils.ha_recovery_for_consolidation_mode'
             ) as recovery, \
             mock.patch('sky.skylet.events.ServiceUpdateEvent') as event, \
             mock.patch.object(daemons.time, 'sleep') as sleep:
            daemons._serve_status_refresh_event(pool=False)

        ensure_lock.assert_not_called()
        recovery.assert_not_called()
        event.assert_not_called()
        sleep.assert_called_once()

    def test_serve_hold_does_not_block_pool_daemon(self, monkeypatch):
        monkeypatch.setenv(serve_constants.SERVE_CONTROLLER_HOLD_ENV_VAR,
                           'true')
        with mock.patch(
                'sky.serve.serve_utils.ha_recovery_for_consolidation_mode'
        ) as recovery, \
             mock.patch('sky.skylet.events.ServiceUpdateEvent') as event, \
             mock.patch.object(daemons.time, 'sleep'):
            daemons._serve_status_refresh_event(pool=True)

        recovery.assert_called_once()
        event.assert_called_once_with(pool=True)

    def test_pool_event_instance_is_reused_across_iterations(self):
        with mock.patch(
                'sky.serve.serve_utils.ha_recovery_for_consolidation_mode'), \
             mock.patch('sky.skylet.events.ServiceUpdateEvent') as mock_event, \
             mock.patch.object(daemons.time, 'sleep'):
            for _ in range(3):
                daemons._serve_status_refresh_event(pool=True)
            mock_event.assert_called_once_with(pool=True)
            assert mock_event.return_value.run.call_count == 3

    def test_serve_and_pool_use_independent_instances(self):
        # Serve (pool=False) and pool (pool=True) must each get their own
        # cached event — otherwise pool=True daemon iterations would call
        # run() on the pool=False event (or vice versa).
        with mock.patch(
                'sky.serve.serve_utils.ha_recovery_for_consolidation_mode'), \
             mock.patch('sky.skylet.events.ServiceUpdateEvent') as mock_event, \
             mock.patch.object(daemons.time, 'sleep'):
            daemons._serve_status_refresh_event(pool=False)
            daemons._serve_status_refresh_event(pool=True)
            daemons._serve_status_refresh_event(pool=False)
            daemons._serve_status_refresh_event(pool=True)
            # Exactly two ctor calls: one per pool flag.
            assert mock_event.call_count == 2
            assert mock.call(pool=False) in mock_event.call_args_list
            assert mock.call(pool=True) in mock_event.call_args_list
