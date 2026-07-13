"""Tests for sky/serve/service.py.

Focused on the helpers added for HA leader-aware routing:

- _wait_for_controller_ready: must block until the uvicorn subprocess is
  actually accepting connections. Used to gate the DB flip in the recovery
  path so that clients never route to a pod whose listener isn't bound yet.
- _orphan_exit: must NOT call _cleanup. The whole point of this exit path
  is that another instance has already taken over the row, so any cleanup
  here would race with the new owner's replica state writes.
- _cleanup: must NOT delete version_specs. Deleting them on failure leaves
  the `services` row invisible to JOIN-based queries and breaks
  status / down --purge.
"""
import socket
import threading
import time
from unittest import mock

import pytest

from sky.serve import service


def _bind_socket_async(host, port, delay):
    """Helper: bind to host:port after `delay` seconds, then keep the socket
    open until the test thread sets a stop event."""
    stop_event = threading.Event()

    def run():
        time.sleep(delay)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
            s.listen(1)
            stop_event.wait()
        finally:
            s.close()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, stop_event


def _free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestWaitForControllerReady:

    def test_returns_when_listener_already_up(self):
        """Listener is up before we even start polling — fast path."""
        port = _free_port()
        thread, stop = _bind_socket_async('127.0.0.1', port, delay=0)
        try:
            time.sleep(0.1)  # let bind complete
            start = time.time()
            service._wait_for_controller_ready('127.0.0.1', port, timeout=5)
            assert time.time() - start < 1.0
        finally:
            stop.set()
            thread.join(timeout=2)

    def test_polls_until_listener_comes_up(self):
        """Listener comes up partway through — verifies polling actually
        retries instead of returning the first ECONNREFUSED."""
        port = _free_port()
        # Bind 0.5s after we start waiting — well within timeout.
        thread, stop = _bind_socket_async('127.0.0.1', port, delay=0.5)
        try:
            start = time.time()
            service._wait_for_controller_ready('127.0.0.1', port, timeout=5)
            elapsed = time.time() - start
            # Should take at least the bind delay, but well under the timeout.
            assert 0.4 < elapsed < 4.0
        finally:
            stop.set()
            thread.join(timeout=2)

    def test_raises_on_timeout(self):
        """Listener never comes up — must raise RuntimeError, not block
        forever (would otherwise leave the daemon stuck with the old DB row
        intact, blocking subsequent recoveries)."""
        port = _free_port()
        # Don't bind anything.
        with pytest.raises(RuntimeError, match='did not become ready'):
            service._wait_for_controller_ready('127.0.0.1', port, timeout=1)

    def test_treats_zero_zero_as_loopback(self):
        """Controller may be configured to bind 0.0.0.0 (k8s mode); we must
        probe via 127.0.0.1, not literally connect to 0.0.0.0 (which is not
        always a valid connect target on macOS)."""
        port = _free_port()
        thread, stop = _bind_socket_async('127.0.0.1', port, delay=0)
        try:
            time.sleep(0.1)
            service._wait_for_controller_ready('0.0.0.0', port, timeout=5)
        finally:
            stop.set()
            thread.join(timeout=2)


class TestOrphanExit:
    """Critical contract: _orphan_exit must NOT touch any DB state. It only
    kills our controller child and calls os._exit(0). This is what
    distinguishes orphan exit from normal cleanup — the new owner is now
    responsible for replica state, version cleanup, services row deletion,
    etc."""

    def test_calls_os_exit_zero(self):
        with mock.patch('os._exit') as mock_exit, \
             mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes'):
            ctrl = mock.Mock(pid=11111)
            service._orphan_exit(ctrl)
            mock_exit.assert_called_once_with(0)

    def test_does_not_call_cleanup(self):
        with mock.patch('os._exit'), \
             mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes'), \
             mock.patch('sky.serve.service._cleanup') as mock_cleanup, \
             mock.patch('sky.serve.service.serve_state.'
                        'remove_service') as mock_remove, \
            mock.patch('sky.serve.service.serve_state.'
                        'remove_replica') as mock_remove_replica:
            ctrl = mock.Mock(pid=11111)
            service._orphan_exit(ctrl)
            mock_cleanup.assert_not_called()
            mock_remove.assert_not_called()
            mock_remove_replica.assert_not_called()

    def test_kills_controller_child(self):
        with mock.patch('os._exit'), \
            mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes') as mock_kill:
            ctrl = mock.Mock(pid=11111)
            service._orphan_exit(ctrl)
            _, kwargs = mock_kill.call_args
            assert kwargs['parent_pids'] == [11111]
            assert kwargs['force'] is True

    def test_handles_none_subprocesses(self):
        """The child may be None if we crashed before spawning."""
        with mock.patch('os._exit') as mock_exit, \
             mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes') as mock_kill:
            service._orphan_exit(None)
            mock_kill.assert_not_called()
            mock_exit.assert_called_once_with(0)

    def test_swallows_kill_failure(self):
        """If kill_children_processes raises (e.g. pid already gone), we
        still must os._exit. Otherwise an exception leaves the orphan loop
        running."""
        with mock.patch('os._exit') as mock_exit, \
            mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes',
                        side_effect=OSError('no such process')):
            ctrl = mock.Mock(pid=11111)
            service._orphan_exit(ctrl)
            mock_exit.assert_called_once_with(0)


class TestExitOnOwnershipLoss:

    def test_success_does_not_exit(self):
        with mock.patch('sky.serve.service._orphan_exit') as orphan_exit:
            service._exit_on_ownership_loss(True, 'svc', 'publishing', None)
        orphan_exit.assert_not_called()

    def test_failed_cas_discards_child_via_orphan_exit(self):
        controller = mock.Mock(pid=11111)
        with mock.patch('sky.serve.service._orphan_exit') as orphan_exit, \
             mock.patch('sky.serve.service._cleanup') as cleanup:
            service._exit_on_ownership_loss(False, 'svc', 'publishing',
                                            controller)
        orphan_exit.assert_called_once_with(controller)
        cleanup.assert_not_called()

    def test_failed_preclaim_exits_without_child(self):
        with mock.patch('sky.serve.service._orphan_exit') as orphan_exit:
            service._exit_on_ownership_loss(False, 'svc', 'preclaiming', None)
        orphan_exit.assert_called_once_with(None)


class TestBailOnBootFailure:
    """`_bail_on_boot_failure` is the regression fix for the
    catastrophic-cleanup bug: when `_wait_for_controller_ready` times out
    in the recovery branch of `_start`, the previous code re-`raise`d the
    RuntimeError, which fell through to `_start`'s outer `finally` →
    `_cleanup` → `remove_ha_recovery_script` (+ possibly
    `remove_service_completely` on pools with no replicas), turning a
    transient boot failure into permanent service deletion.

    Contract: like `_orphan_exit`, this helper must kill our forked
    subprocess and `os._exit` to bypass the outer finally — it must NOT
    touch any DB state and must NOT call `_cleanup`.
    """

    def test_calls_os_exit_one(self):
        with mock.patch('os._exit') as mock_exit, \
             mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes'):
            ctrl = mock.Mock(pid=11111)
            service._bail_on_boot_failure(
                'svc',
                ctrl,
                timeout_seconds=60,
                boot_err=RuntimeError('did not become ready'))
            mock_exit.assert_called_once_with(1)

    def test_does_not_call_cleanup(self):
        """The whole point of this bailout: do NOT enter the destructive
        cleanup path. No DB mutations of any kind."""
        with mock.patch('os._exit'), \
             mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes'), \
             mock.patch('sky.serve.service._cleanup') as mock_cleanup, \
             mock.patch('sky.serve.service.serve_state.'
                        'remove_ha_recovery_script') as mock_remove_script, \
             mock.patch('sky.serve.service.serve_state.'
                        'remove_service_completely') as mock_remove_svc, \
             mock.patch('sky.serve.service.serve_state.'
                        'remove_service') as mock_remove:
            ctrl = mock.Mock(pid=11111)
            service._bail_on_boot_failure(
                'svc',
                ctrl,
                timeout_seconds=60,
                boot_err=RuntimeError('did not become ready'))
            mock_cleanup.assert_not_called()
            mock_remove_script.assert_not_called()
            mock_remove_svc.assert_not_called()
            mock_remove.assert_not_called()

    def test_kills_controller_subprocess(self):
        """Must SIGKILL the controller subprocess we spawned — otherwise
        the daemon's next ha_recovery iteration spawns a new one and we
        leak the old one (which never bound)."""
        with mock.patch('os._exit'), \
             mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes') as mock_kill:
            ctrl = mock.Mock(pid=11111)
            service._bail_on_boot_failure(
                'svc',
                ctrl,
                timeout_seconds=60,
                boot_err=RuntimeError('did not become ready'))
            _, kwargs = mock_kill.call_args
            assert kwargs['parent_pids'] == [11111]
            assert kwargs['force'] is True

    def test_handles_none_controller_process(self):
        """If the RuntimeError fires before `controller_process.start()`
        (rare, but possible), `controller_process` is None and we have
        nothing to kill — but we still must os._exit."""
        with mock.patch('os._exit') as mock_exit, \
             mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes') as mock_kill:
            service._bail_on_boot_failure(
                'svc',
                None,
                timeout_seconds=60,
                boot_err=RuntimeError('did not become ready'))
            mock_kill.assert_not_called()
            mock_exit.assert_called_once_with(1)

    def test_handles_none_controller_pid(self):
        """If `controller_process` was created but `start()` never set a
        pid (e.g. start() itself raised before assigning pid), we must
        NOT pass `pid=None` to `kill_children_processes`:
        `psutil.Process(None)` resolves to the *calling* process, so
        passing `[None]` would SIGKILL ourselves (and our own children)
        before `os._exit(1)` runs — defeating the cleanup bypass."""
        with mock.patch('os._exit') as mock_exit, \
             mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes') as mock_kill:
            ctrl = mock.Mock()
            ctrl.pid = None
            service._bail_on_boot_failure(
                'svc',
                ctrl,
                timeout_seconds=60,
                boot_err=RuntimeError('did not become ready'))
            mock_kill.assert_not_called()
            mock_exit.assert_called_once_with(1)

    def test_swallows_kill_failure(self):
        """If kill_children_processes raises (e.g. pid already gone
        between when we read it and when we try to kill it), we still
        must os._exit. Otherwise the exception bubbles up to the outer
        try/finally — exactly the cleanup path we are trying to avoid."""
        with mock.patch('os._exit') as mock_exit, \
             mock.patch('sky.serve.service.subprocess_utils.'
                        'kill_children_processes',
                        side_effect=OSError('no such process')):
            ctrl = mock.Mock(pid=11111)
            service._bail_on_boot_failure(
                'svc',
                ctrl,
                timeout_seconds=60,
                boot_err=RuntimeError('did not become ready'))
            mock_exit.assert_called_once_with(1)


class TestCleanupUsesDurableVersionStorageManifests:
    """`_cleanup` must:

    1. Clean each incarnation-scoped version manifest before final removal.

    2. Keep `version_specs` intact. `get_service_from_name` uses an INNER
       JOIN with `version_specs`, so deleting the version rows during a
       _cleanup that may still fail makes the resulting FAILED_CLEANUP row
       invisible to status queries AND to `--purge` — the only escape would
       be raw SQL DELETE. The success path in `_start` removes both
       atomically via `remove_service_completely`; failure leaves the row
       findable so the user can recover with `--purge`.
    """

    def _patch_common(self):
        # Replicas: empty → skip the terminate-thread loop.
        return [
            mock.patch('sky.serve.service.serve_state.get_replica_infos',
                       return_value=[]),
            # _cleanup audit log reads current DB state for the WARN line.
            mock.patch('sky.serve.service.serve_state.get_service_from_name',
                       return_value={
                           'controller_pid': 9999,
                           'controller_ip': '10.0.0.1',
                           'status': 'READY',
                       }),
            mock.patch('sky.serve.service.serve_state.service_owner_matches',
                       return_value=True),
            mock.patch(
                'sky.serve.service.serve_utils.'
                'lifecycle_lock_is_valid',
                return_value=True),
            mock.patch(
                'sky.serve.service.serve_state.get_version_yaml_contents',
                return_value={1: 'yaml-v1'}),
        ]

    def test_scoped_storage_is_cleaned_before_manifest_removal(self):
        patches = self._patch_common()
        with mock.patch('sky.serve.service.cleanup_storage',
                        return_value=True) as cleanup, \
             mock.patch(
                 'sky.serve.service.serve_state.delete_all_versions'
             ) as mock_delete_versions, \
             mock.patch(
                 'sky.serve.service.serve_state.remove_ha_recovery_script'
             ) as mock_remove_recovery:
            for p in patches:
                p.start()
            try:
                failed = service._cleanup('svc',
                                          False,
                                          'incarnation-a',
                                          9999,
                                          '10.0.0.1',
                                          mock.Mock(),
                                          resource_scope='incarnation-a')
            finally:
                for p in patches:
                    p.stop()
            assert failed is False
            cleanup.assert_called_once_with('yaml-v1', 'incarnation-a')
            # version_specs must NOT be touched by _cleanup (success path
            # in _start handles it via remove_service_completely).
            mock_delete_versions.assert_not_called()
            # Finalization owns recovery-script removal so a lifecycle-lock
            # loss cannot strand the row between this helper and final CAS.
            mock_remove_recovery.assert_not_called()

    def test_failed_scoped_cleanup_keeps_version_manifest_retryable(self):
        patches = self._patch_common()
        with mock.patch('sky.serve.service.cleanup_storage',
                        return_value=False) as cleanup, \
             mock.patch(
                 'sky.serve.service.serve_state.delete_all_versions'
             ) as mock_delete_versions, \
             mock.patch(
                 'sky.serve.service.serve_state.remove_ha_recovery_script'
             ) as mock_remove_recovery:
            for p in patches:
                p.start()
            try:
                failed = service._cleanup('svc',
                                          False,
                                          'incarnation-a',
                                          9999,
                                          '10.0.0.1',
                                          mock.Mock(),
                                          resource_scope='incarnation-a')
            finally:
                for p in patches:
                    p.stop()
            assert failed is True
            cleanup.assert_called_once_with('yaml-v1', 'incarnation-a')
            # version_specs preserved → row still findable via JOIN, --purge
            # can clear it.
            mock_delete_versions.assert_not_called()
            mock_remove_recovery.assert_not_called()


class TestRunCleanupAndFinalizeDeletesLb:
    """The external LB is quiesced before destructive replica cleanup."""

    @staticmethod
    def _spec():
        spec = mock.MagicMock()
        spec.pool = False
        return spec

    def test_deletes_lb_on_failed_cleanup(self):
        with mock.patch('sky.serve.service._cleanup', return_value=True), \
             mock.patch('sky.serve.service.serve_state.'
                        'acknowledge_service_controller_teardown_if_owner',
                        return_value=True), \
             mock.patch('sky.serve.service.serve_utils.'
                        'get_service_lifecycle_lock',
                        return_value=mock.MagicMock()), \
             mock.patch('sky.serve.service.serve_utils.'
                        'lifecycle_lock_is_valid', return_value=True), \
             mock.patch('sky.serve.service.serve_state.'
                        'service_owner_matches', return_value=True), \
             mock.patch('sky.serve.service.serve_state.'
                        'set_service_status_and_active_versions_if_owner',
                        return_value=True), \
             mock.patch('sky.serve.service.serve_state.'
                        'remove_ha_recovery_script_if_owner'), \
             mock.patch('sky.serve.service.serve_state.'
                        'remove_service_completely') as mock_remove, \
             mock.patch('sky.serve.service.lb_k8s.'
                        'get_api_deployment_owner_uid',
                        return_value='api-deployment-uid'), \
             mock.patch('sky.serve.service.lb_k8s.delete_lb_objects'
                       ) as mock_delete_lb, \
             mock.patch('sky.serve.service._cleanup_task_run_script'):
            service._run_cleanup_and_finalize('svc', self._spec(), '/tmp/svc',
                                              1, 'incarnation-a', 123,
                                              '10.0.0.1')
        # FAILED_CLEANUP keeps the DB row but tears down the LB.
        mock_remove.assert_not_called()
        mock_delete_lb.assert_called_once_with(
            'svc',
            expected_service_hash='incarnation-a',
            require_runtime=True,
            expected_api_deployment_uid='api-deployment-uid')

    def test_failed_cleanup_lb_delete_error_is_swallowed(self):
        with mock.patch('sky.serve.service._cleanup',
                        return_value=True) as mock_cleanup, \
             mock.patch('sky.serve.service.serve_state.'
                        'acknowledge_service_controller_teardown_if_owner',
                        return_value=True), \
             mock.patch('sky.serve.service.serve_utils.'
                        'get_service_lifecycle_lock',
                        return_value=mock.MagicMock()), \
             mock.patch('sky.serve.service.serve_utils.'
                        'lifecycle_lock_is_valid', return_value=True), \
             mock.patch('sky.serve.service.serve_state.'
                        'service_owner_matches', return_value=True), \
             mock.patch('sky.serve.service.serve_state.'
                        'set_service_status_and_active_versions_if_owner',
                        return_value=True), \
             mock.patch('sky.serve.service.lb_k8s.'
                        'get_api_deployment_owner_uid',
                        return_value='api-deployment-uid'), \
             mock.patch('sky.serve.service.lb_k8s.delete_lb_objects',
                        side_effect=RuntimeError('boom')), \
             mock.patch('sky.serve.service._cleanup_task_run_script'):
            # A best-effort LB delete failure must not propagate.
            service._run_cleanup_and_finalize('svc', self._spec(), '/tmp/svc',
                                              1, 'incarnation-a', 123,
                                              '10.0.0.1')
        # Fail closed: replicas are not destroyed while their public LB may
        # still be accepting requests.
        mock_cleanup.assert_not_called()

    def test_deletes_lb_on_success(self):
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.epoch = 17
        with mock.patch('sky.serve.service._cleanup', return_value=False), \
             mock.patch('sky.serve.service.serve_state.'
                        'set_service_status_and_active_versions_if_owner',
                        return_value=True), \
             mock.patch('sky.serve.service.serve_state.get_replica_infos',
                        return_value=[]), \
             mock.patch('sky.serve.service.serve_utils.'
                        'quiesce_service_replica_launch_requests',
                        return_value=True), \
             mock.patch('sky.serve.service.serve_state.'
                        'acknowledge_service_controller_teardown_if_owner',
                        return_value=True), \
             mock.patch('sky.serve.service.serve_utils.'
                        'get_service_lifecycle_lock',
                        return_value=lifecycle_lock), \
             mock.patch('sky.serve.service.serve_utils.'
                        'lifecycle_lock_is_valid', return_value=True), \
             mock.patch('sky.serve.service.serve_state.'
                        'service_owner_matches', return_value=True), \
             mock.patch('sky.serve.service.serve_state.'
                        'remove_service_completely',
                        return_value=True) as mock_remove, \
             mock.patch('sky.serve.service.lb_k8s.'
                        'get_api_deployment_owner_uid',
                        return_value='api-deployment-uid'), \
             mock.patch('sky.serve.service.lb_k8s.delete_lb_objects'
                       ) as mock_delete_lb, \
             mock.patch('sky.serve.service.serve_utils.'
                        'remove_service_directory'), \
             mock.patch('sky.serve.service._cleanup_task_run_script'):
            service._run_cleanup_and_finalize('svc', self._spec(), '/tmp/svc',
                                              1, 'incarnation-a', 123,
                                              '10.0.0.1')
        mock_remove.assert_called_once_with(
            'svc',
            'incarnation-a',
            expected_controller_owner=(123, '10.0.0.1'),
            expected_lifecycle_epoch=17)
        mock_delete_lb.assert_called_once_with(
            'svc',
            expected_service_hash='incarnation-a',
            require_runtime=True,
            expected_api_deployment_uid='api-deployment-uid')


def test_stale_bootstrap_incarnation_is_rejected_before_file_or_lb_work():
    record = {
        'hash': 'incarnation-b',
        'controller_job_id': 1,
        'controller_pid': 123,
        'controller_ip': '10.0.0.2',
    }
    with mock.patch.object(service.auth_utils, 'get_or_generate_keys'), \
         mock.patch.object(service.serve_state,
                           'get_service_from_name',
                           return_value=record), \
         mock.patch.object(service.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         pytest.raises(RuntimeError, match='stale controller bootstrap'):
        service._start('svc',
                       '/does/not/exist',
                       1,
                       'sky serve up',
                       requested_incarnation='incarnation-a',
                       lifecycle_epoch=7)


def test_legacy_stale_bootstrap_job_is_rejected_before_file_or_lb_work():
    record = {
        'hash': 'incarnation-b',
        'controller_job_id': 2,
        'controller_pid': 123,
        'controller_ip': '10.0.0.2',
    }
    with mock.patch.object(service.auth_utils, 'get_or_generate_keys'), \
         mock.patch.object(service.serve_state,
                           'get_service_from_name',
                           return_value=record), \
         mock.patch.object(service.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         pytest.raises(RuntimeError, match='stale controller bootstrap'):
        # No requested_incarnation models a recovery script generated by an
        # older API server. Its durable controller job ID still identifies the
        # old incarnation and must not be allowed to adopt the successor row.
        service._start('svc', '/does/not/exist', 1, 'sky serve up')


def test_delayed_legacy_recovery_cannot_recreate_absent_service():
    with mock.patch.object(service.auth_utils, 'get_or_generate_keys'), \
         mock.patch.object(service.serve_state,
                           'get_service_from_name',
                           return_value=None), \
         pytest.raises(RuntimeError, match='legacy name-only'):
        service._start('svc', '/does/not/exist', 1, 'sky serve up')


class TestCleanupAuditLog:
    """`_cleanup` logs a WARN with the current DB controller_pid / ip /
    status before terminating replica clusters. An audit trail is essential
    for debugging double-spawn / unexpected-cleanup incidents.
    """

    def _common_patches(self, db_record):
        # sky.serve.service uses sky_logging.init_logger with
        # propagate=False, so caplog (rooted at the root logger) misses
        # its records. Patch the module-level logger instead and inspect
        # its `.warning(...)` calls directly.
        return [
            mock.patch('sky.serve.service.serve_state.get_replica_infos',
                       return_value=[]),
            mock.patch('sky.serve.service.serve_state.get_service_from_name',
                       return_value=db_record),
            mock.patch('sky.serve.service.serve_state.service_owner_matches',
                       return_value=True),
            mock.patch(
                'sky.serve.service.serve_utils.'
                'lifecycle_lock_is_valid',
                return_value=True),
        ]

    def test_logs_db_state_when_row_present(self):
        patches = self._common_patches({
            'controller_pid': 4242,
            'controller_ip': '10.4.7.7',
            'status': 'READY',
        })
        for p in patches:
            p.start()
        try:
            with mock.patch.object(service.logger, 'warning') as mock_warn:
                service._cleanup('audit-svc', True, 'incarnation-a', 4242,
                                 '10.4.7.7', mock.Mock())
        finally:
            for p in patches:
                p.stop()
        joined = '\n'.join(call.args[0] for call in mock_warn.call_args_list)
        # Audit line includes the service name and DB state we observed.
        # Substring checks instead of exact match for copy-edit resilience.
        assert 'audit-svc' in joined
        assert 'db_controller_pid=4242' in joined
        assert 'db_controller_ip=10.4.7.7' in joined

    def test_logs_missing_row_when_db_returns_none(self):
        patches = self._common_patches(None)
        for p in patches:
            p.start()
        try:
            with mock.patch.object(service.logger, 'warning') as mock_warn:
                service._cleanup('gone-svc', True, 'incarnation-a', 4242,
                                 '10.4.7.7', mock.Mock())
        finally:
            for p in patches:
                p.stop()
        joined = '\n'.join(call.args[0] for call in mock_warn.call_args_list)
        assert 'gone-svc' in joined
        assert 'db row not found' in joined


class TestFailedStartupCleansOnlyScopedStorage:
    """A failed bootstrap cleans its disjoint generation, never a successor."""

    @staticmethod
    def _task():
        spec = mock.MagicMock()
        spec.pool = True
        spec.autoscaling_policy_str.return_value = 'policy'
        spec.load_balancing_policy = 'round_robin'
        spec.tls_credential = None
        return mock.MagicMock(service=spec)

    def _common_patches(self, task):
        return [
            mock.patch.object(service.auth_utils, 'get_or_generate_keys'),
            mock.patch.object(service.serve_state,
                              'get_service_from_name',
                              return_value=None),
            mock.patch.object(service.task_lib.Task,
                              'from_yaml_str',
                              return_value=task),
            mock.patch('builtins.open',
                       mock.mock_open(read_data='service: {}')),
            mock.patch.object(service.serve_utils,
                              'generate_remote_service_dir_name',
                              return_value='/tmp/scoped-service'),
            mock.patch.object(service.controller_utils,
                              'get_resources_lock_path',
                              return_value='/tmp/resources.lock'),
            mock.patch.object(service.filelock, 'FileLock'),
        ]

    def test_capacity_rejection_cleans_preallocated_scope(self):
        task = self._task()
        patches = self._common_patches(task)
        with mock.patch.object(service.controller_utils,
                               'can_start_new_process',
                               return_value=False), \
             mock.patch.object(
                 service.controller_utils,
                 'get_max_services_error_message',
                 return_value='at capacity'), \
             mock.patch.object(service, 'cleanup_storage') as cleanup:
            for patcher in patches:
                patcher.start()
            try:
                with pytest.raises(RuntimeError, match='at capacity'):
                    service._start('svc', '/tmp/task.yaml', 7, 'sky serve up',
                                   'incarnation-a', 11)
            finally:
                for patcher in patches:
                    patcher.stop()
        cleanup.assert_called_once_with('service: {}', 'incarnation-a')

    def test_lost_registration_cleans_only_losing_scope(self):
        task = self._task()
        patches = self._common_patches(task)
        with mock.patch.object(service.controller_utils,
                               'can_start_new_process',
                               return_value=True), \
             mock.patch.object(service.os, 'makedirs'), \
             mock.patch.object(service.backend_utils,
                               'get_task_resources_str',
                               return_value='resources'), \
             mock.patch.object(service.serve_state,
                               'add_service',
                               return_value=False), \
             mock.patch.object(service, 'cleanup_storage') as cleanup:
            for patcher in patches:
                patcher.start()
            try:
                with pytest.raises(ValueError, match='already exists'):
                    service._start('svc', '/tmp/task.yaml', 7, 'sky serve up',
                                   'incarnation-a', 11)
            finally:
                for patcher in patches:
                    patcher.stop()
        cleanup.assert_called_once_with('service: {}', 'incarnation-a')


class TestCleanupStorageStaleBucket:
    """When a storage's bucket has already been deleted (e.g. by an earlier
    cleanup pass that succeeded for the bucket but crashed before remove_
    service committed), re-running `cleanup_storage` must NOT mark the
    cleanup as failed — the bucket already being gone IS the cleanup target
    state.

    Without this, FAILED_CLEANUP becomes a self-perpetuating loop:
    `ha_recovery_for_consolidation_mode` respawns the controller, which
    re-reads the same yaml and crashes on the same stale storage entry,
    re-entering FAILED_CLEANUP forever (observed live as a pool flipping
    between FAILED_CLEANUP and NO_REPLICA every time the recovery daemon
    ticked).
    """

    def test_returns_success_when_bucket_already_gone(self):
        from sky import exceptions as sky_exc

        stale_storage = mock.MagicMock()
        stale_storage.name = 'stale-bucket'
        stale_storage.persistent = False
        stale_storage.construct.side_effect = (
            sky_exc.StorageExternalDeletionError(
                'Attempted to use a non-existent bucket as a source: s3://gone')
        )

        live_storage = mock.MagicMock()
        live_storage.name = 'live-bucket'
        live_storage.persistent = False
        # construct() returns normally → storage stays in storage_mounts.

        mock_task = mock.MagicMock()
        mock_task.storage_mounts = {
            '/stale': stale_storage,
            '/live': live_storage,
        }
        mock_task.file_mounts = None
        resource_scope = 'incarnation-a'
        generation = 'version-1'
        scope_id = service.serve_utils.generate_ephemeral_storage_scope_id(
            resource_scope, generation)
        stale_storage.name += f'-{scope_id}'
        live_storage.name += f'-{scope_id}'
        mock_task.metadata = {
            service.constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY: {
                'resource_scope': resource_scope,
                'storage_generation': generation,
                'scope_id': scope_id,
                'storage_mounts': ['/stale', '/live'],
            }
        }

        mock_backend = mock.MagicMock()

        with mock.patch('sky.serve.service.task_lib.Task.from_yaml_str',
                        return_value=mock_task), \
             mock.patch(
                 'sky.serve.service.cloud_vm_ray_backend.CloudVmRayBackend',
                 return_value=mock_backend):
            result = service.cleanup_storage('dummy: yaml', resource_scope)

        assert result is True, (
            'a bucket that is already gone is the cleanup target state, '
            'must not be reported as failure')
        # Stale entry dropped before teardown so backend doesn't retry it.
        assert '/stale' not in mock_task.storage_mounts
        assert '/live' in mock_task.storage_mounts
        mock_backend.teardown_ephemeral_storage.assert_called_once_with(
            mock_task)
        live_storage.construct.assert_called_once()

    def test_returns_failure_for_other_construct_errors(self):
        """Non-bucket-missing construct errors still fail cleanup — we
        don't want to silently swallow real bugs like expired creds."""
        broken_storage = mock.MagicMock()
        broken_storage.name = 'broken-bucket'
        broken_storage.persistent = False
        broken_storage.construct.side_effect = RuntimeError(
            'credential expired')

        mock_task = mock.MagicMock()
        mock_task.storage_mounts = {'/x': broken_storage}
        mock_task.file_mounts = None
        resource_scope = 'incarnation-a'
        generation = 'version-1'
        scope_id = service.serve_utils.generate_ephemeral_storage_scope_id(
            resource_scope, generation)
        broken_storage.name += f'-{scope_id}'
        mock_task.metadata = {
            service.constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY: {
                'resource_scope': resource_scope,
                'storage_generation': generation,
                'scope_id': scope_id,
                'storage_mounts': ['/x'],
            }
        }

        mock_backend = mock.MagicMock()

        with mock.patch('sky.serve.service.task_lib.Task.from_yaml_str',
                        return_value=mock_task), \
             mock.patch(
                 'sky.serve.service.cloud_vm_ray_backend.CloudVmRayBackend',
                 return_value=mock_backend):
            result = service.cleanup_storage('dummy: yaml', resource_scope)

        assert result is False, (
            'unexpected construct errors must propagate as cleanup failure')
        # The broader except block aborted before reaching teardown.
        mock_backend.teardown_ephemeral_storage.assert_not_called()


# ---------------------------------------------------------------------------
# External-only controller port selection.
# ---------------------------------------------------------------------------


def test_select_controller_port_is_always_local_and_ephemeral():
    with mock.patch.object(service.common_utils,
                           'find_free_port',
                           return_value=54321) as find_free_port, \
         mock.patch.object(service.serve_state,
                           'get_services') as get_services:
        assert service._select_controller_port('svc') == 54321

    find_free_port.assert_called_once_with(
        service.constants.CONTROLLER_PORT_START)
    # The stable API proxy removes any need for a DB-wide port allocator.
    get_services.assert_not_called()
