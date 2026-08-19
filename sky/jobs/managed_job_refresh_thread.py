"""Run the managed-job-status-refresh loop as a thread in the API server."""
import os
import pathlib
import signal
import threading
import time
import typing

from sky import sky_logging
from sky.jobs import constants as managed_job_constants
from sky.jobs import state as managed_job_state
from sky.jobs import utils as managed_job_utils
from sky.skylet import constants
from sky.skylet import events
from sky.utils import locks

if typing.TYPE_CHECKING:
    pass

logger = sky_logging.init_logger(__name__)

_LOCK_PROBE_INTERVAL_SECONDS = 5
_ACQUIRE_RETRY_INTERVAL_SECONDS = 5

# How long to wait after acquiring the consolidation-mode lock before running
# recovery. During a rolling update the new leader blocks on acquire() while
# the old API server still holds the lock. The lock is released when the old
# main process exits, but that pod's job controllers are detached subprocesses
# (start_new_session=True), so they are not killed until the container itself
# is torn down a moment later. If recovery ran in that residual window, it would
# reset jobs that the still-alive (but about-to-die) old controllers can briefly
# re-claim, stamping their soon-dead PIDs back onto the jobs;
# update_managed_jobs_statuses would then mark those jobs FAILED_CONTROLLER (a
# split brain across the upgrade overlap). Waiting here lets the old container
# finish terminating before we reset and re-adopt its jobs. The recovery signal
# file stays in place during the wait, so no controllers are started and no job
# is marked FAILED_CONTROLLER in the meantime.
_RECOVERY_WAIT_AFTER_ACQUIRE_SECONDS = 15


def _touch_recovery_signal_file() -> pathlib.Path:
    """Create the recovery gate file, including its parent directory."""
    signal_file = pathlib.Path(
        constants.PERSISTENT_RUN_RESTARTING_SIGNAL_FILE).expanduser()
    signal_file.parent.mkdir(parents=True, exist_ok=True)
    signal_file.touch()
    return signal_file


class ManagedJobRefreshDaemonThread(threading.Thread):
    """Leader-elected thread that runs ha_recovery + ManagedJobEvent.

    See module docstring for motivation and invariants.
    """

    def __init__(self) -> None:
        # daemon=True: when the main interpreter exits we want this thread
        # to go with it; the leader role is meant to track main's lifecycle.
        super().__init__(name='managed-job-refresh', daemon=True)
        self._lock: locks.DistributedLock | None = None
        self._stop_event = threading.Event()
        self._effects_stopped = threading.Event()
        self._ownership_released = threading.Event()
        self._release_requested = threading.Event()
        self._release_failure: BaseException | None = None
        self._cutover_ready = threading.Event()
        self._cutover_failure: BaseException | None = None

    def request_shutdown(self) -> None:
        """Fence future refresh/recovery effects and wake bounded waits.

        The lock remains held until ``run()`` has returned from any in-flight
        recovery/event effect.  Releasing it here would let a replacement
        leader begin while this thread was still mutating managed-job state.
        """
        self._stop_event.set()

    def wait_for_cutover(self) -> None:
        """Block all-mode controller request claims until migration completes."""
        timeout = max(_LOCK_PROBE_INTERVAL_SECONDS,
                      _ACQUIRE_RETRY_INTERVAL_SECONDS) + 30
        if not self._cutover_ready.wait(timeout=timeout):
            raise RuntimeError(
                'Managed-job controller family cutover did not become ready.')
        if self._cutover_failure is not None:
            raise RuntimeError('Managed-job controller family cutover failed.'
                              ) from (self._cutover_failure)

    def wait_for_shutdown(self) -> None:
        """Wait for effect quiescence while the owner thread retains its lock."""
        timeout = max(_LOCK_PROBE_INTERVAL_SECONDS,
                      _ACQUIRE_RETRY_INTERVAL_SECONDS) + 1
        if not self._effects_stopped.wait(timeout=timeout):
            raise RuntimeError(
                'Managed-job refresh effects did not prove quiescence.')
        if not self.is_alive() and not self._ownership_released.is_set():
            raise RuntimeError('Managed-job refresh owner exited before its '
                               'ownership-release barrier.')

    def release_ownership(self) -> None:
        """Release the inner lock after all controller spawners/families stop."""
        if not self._effects_stopped.is_set():
            raise RuntimeError('Managed-job refresh effects must stop before '
                               'ownership release.')
        if self._ownership_released.is_set():
            return
        self._release_requested.set()
        self.join(timeout=max(_LOCK_PROBE_INTERVAL_SECONDS,
                              _ACQUIRE_RETRY_INTERVAL_SECONDS) + 1)
        if self._ownership_released.is_set() and not self.is_alive():
            return
        if self._release_failure is not None:
            raise RuntimeError('Managed-job refresh ownership did not release.'
                              ) from (self._release_failure)
        raise RuntimeError('Managed-job refresh owner did not exit after its '
                           'ownership-release request.')

    def _wait_or_stopping(self, seconds: float) -> bool:
        return self._stop_event.wait(seconds)

    def run(self) -> None:
        synchronous_invocation = threading.current_thread() is not self
        try:
            while not self._stop_event.is_set():
                try:
                    if self._lock is None:
                        self._lock = locks.get_lock(
                            managed_job_constants.CONSOLIDATION_MODE_LOCK_ID)
                    self._become_leader_and_run()
                    # _become_leader_and_run only returns normally after
                    # lock-loss SIGTERM or an explicit shutdown.  Re-entering
                    # with a stale local lock flag could run recovery beside a
                    # successor generation, so let runtime-owned drain finish.
                    return
                except Exception as e:  # pylint: disable=broad-except
                    if not self._cutover_ready.is_set():
                        self._cutover_failure = e
                        self._cutover_ready.set()
                    logger.exception(
                        'managed-job refresh error; '
                        f'retrying in {_ACQUIRE_RETRY_INTERVAL_SECONDS}s')
                    # If we previously held the lock and lost the session
                    # mid-recovery, retrying would run as a stale leader
                    # (local `_acquired` flag still True, server-side lock
                    # released, another replica can grab it).  Hand off via
                    # SIGTERM, same as the steady-state probe path.
                    if (self._lock is not None and self._lock.is_locked() and
                            not self._lock_still_held()):
                        self._suicide_on_lock_loss()
                        return
                    if self._wait_or_stopping(_ACQUIRE_RETRY_INTERVAL_SECONDS):
                        return
                except BaseException:  # pylint: disable=try-except-raise
                    # SystemExit/KeyboardInterrupt are test/process-control
                    # boundaries, not retryable refresh failures.
                    raise
        finally:
            # The runtime releases the lock only after request spawners and all
            # admitted controller families are absent.  This is required by
            # compatibility ``all`` mode, which has no outer leader lease.
            self._effects_stopped.set()
            if synchronous_invocation:
                # Unit-level state-machine calls do not have a concurrent
                # runtime owner to drive the two-phase handoff.
                try:
                    if self._lock is not None:
                        self._lock.release()
                except Exception as e:  # pylint: disable=broad-except
                    self._release_failure = e
                    raise
                self._ownership_released.set()
            else:
                while not self._ownership_released.is_set():
                    self._release_requested.wait()
                    self._release_requested.clear()
                    try:
                        if self._lock is not None:
                            self._lock.release()
                    except Exception as e:  # pylint: disable=broad-except
                        # Keep the owner thread and its object/session alive.
                        # The next runtime convergence iteration can request a
                        # fresh, authoritative release attempt.
                        self._release_failure = e
                        logger.exception(
                            'Failed to release managed-job refresh ownership.')
                        continue
                    self._release_failure = None
                    self._ownership_released.set()

    def _become_leader_and_run(self) -> None:
        assert self._lock is not None

        # Touch the signal file BEFORE acquiring the lock: new controllers
        # must not be started until recovery has run. During a rolling
        # update we block on acquire() while the old API server still holds
        # the lock; if a controller were started on this replica in that
        # window, the old server's update_managed_jobs_statuses wouldn't see
        # its process and could mark the job FAILED_CONTROLLER. The signal
        # file makes update_managed_jobs_statuses and the scheduler's
        # controller-start path early-return until recovery completes.
        # The gate is removed only after recovery succeeds below. Acquire or
        # recovery failures leave it in place while run() retries, and the
        # lock-loss step-down path keeps it through the shutdown drain. This
        # prevents controller starts from observing partially recovered state.
        signal_file = _touch_recovery_signal_file()

        if self._stop_event.is_set():
            return

        if not self._lock.is_locked():
            logger.info(f'Acquiring the consolidation mode lock: {self._lock}')
            # A nonblocking probe keeps shutdown bounded while a previous
            # replica owns the lock.  The outer loop provides the canonical
            # retry cadence for both lock types.
            self._lock.acquire(blocking=False)
            logger.info('Consolidation mode lock acquired')
        if self._stop_event.is_set():
            return

        # Wait before recovery whenever a nonterminal job exists. A previous
        # image may have a detached scheduler that can claim a WAITING row
        # after this check without an outer-generation fence. The fixed wait is
        # a mixed-version drain aid; current images also serialize every claim
        # with the durable generation.
        if managed_job_state.has_jobs_requiring_recovery_grace_wait():
            logger.info(
                f'Waiting {_RECOVERY_WAIT_AFTER_ACQUIRE_SECONDS}s after '
                'acquiring the consolidation mode lock before running '
                'recovery, to let any previous leader finish shutting down')
            lock_still_held = self._wait_for_recovery_grace()
        else:
            logger.info('No nonterminal managed jobs require a post-acquire '
                        'grace wait; running recovery immediately')
            lock_still_held = self._lock_still_held()

        if not lock_still_held:
            self._suicide_on_lock_loss()
            return
        if self._stop_event.is_set():
            return

        managed_job_utils.ha_recovery_for_consolidation_mode()
        signal_file.unlink(missing_ok=True)
        # Runtime admits fixed slots only after stale/null-slot ownership has
        # been recovered under this still-held inner lock.
        self._cutover_ready.set()

        # Event-loop tick at events.EVENT_CHECKING_INTERVAL_SECONDS and lock
        # probe at _LOCK_PROBE_INTERVAL_SECONDS. Sleep until the earlier
        # deadline instead of waking every second to re-check both.
        refresh_event = events.ManagedJobEvent()
        now = time.monotonic()
        next_probe = now + _LOCK_PROBE_INTERVAL_SECONDS
        next_event = now
        while True:
            if self._stop_event.is_set():
                return
            now = time.monotonic()
            if now >= next_probe:
                if not self._lock_still_held():
                    self._suicide_on_lock_loss()
                    return
                next_probe = now + _LOCK_PROBE_INTERVAL_SECONDS
            if now >= next_event:
                if self._stop_event.is_set():
                    return
                try:
                    refresh_event.run()
                except Exception:  # pylint: disable=broad-except
                    logger.exception('ManagedJobEvent tick failed; will retry')
                next_event = now + events.EVENT_CHECKING_INTERVAL_SECONDS
            sleep_seconds = max(0.0, min(next_probe, next_event) - now)
            if sleep_seconds > 0:
                if self._wait_or_stopping(sleep_seconds):
                    return

    def _wait_for_recovery_grace(self) -> bool:
        """Wait for old controllers while probing this leader's lock.

        A dead PostgreSQL session releases its advisory lock immediately.  Do
        not leave local controllers alive for the whole rolling-update grace
        period after another replica can become leader.
        """
        remaining = _RECOVERY_WAIT_AFTER_ACQUIRE_SECONDS
        if remaining <= 0:
            return self._lock_still_held()
        while remaining > 0:
            sleep_seconds = min(_LOCK_PROBE_INTERVAL_SECONDS, remaining)
            if self._wait_or_stopping(sleep_seconds):
                return True
            remaining -= sleep_seconds
            if not self._lock_still_held():
                return False
        return True

    def _lock_still_held(self) -> bool:
        """True iff we are confident this replica still owns the lock."""
        assert self._lock is not None
        if isinstance(self._lock, locks.PostgresLock):
            # Check is only relevant for PG lock
            return self._lock.is_session_alive()
        return True

    def _suicide_on_lock_loss(self) -> None:
        """SIGTERM the API server process so the pod can restart cleanly."""
        logger.error(
            f'Lost consolidation mode lock {self._lock}; sending SIGTERM '
            'to the API server to step down')
        # Re-touch the recovery signal file so no new controllers will be
        # started
        try:
            _touch_recovery_signal_file()
        except OSError:
            logger.warning('Failed to touch recovery signal file on lock-loss')
        # SIGTERM to trigger graceful shutdown
        os.kill(os.getpid(), signal.SIGTERM)


def start_managed_job_refresh_daemon() -> ManagedJobRefreshDaemonThread | None:
    """Start the refresh thread for this API server process, if needed.

    No-op when consolidation mode is off — mirrors the gating that the
    historical ``should_skip_managed_job_status_refresh`` provided.
    """
    if not managed_job_utils.is_consolidation_mode():
        logger.debug('Consolidation mode is off; not starting the managed-job '
                     'refresh thread.')
        return None
    logger.info('Starting the managed-job refresh thread')
    daemon = ManagedJobRefreshDaemonThread()
    daemon.start()
    return daemon
