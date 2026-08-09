"""Run the managed-job-status-refresh loop as a thread in the API server."""
import os
import pathlib
import signal
import threading
import time
import typing

from sky import sky_logging
from sky.jobs import constants as managed_job_constants
from sky.jobs import scheduler as managed_job_scheduler
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

    def run(self) -> None:
        while True:
            try:
                if self._lock is None:
                    self._lock = locks.get_lock(
                        managed_job_constants.CONSOLIDATION_MODE_LOCK_ID)
                self._become_leader_and_run()
                # _become_leader_and_run only returns normally after
                # _suicide_on_lock_loss sent SIGTERM. Re-entering would
                # skip the lock acquire (stale local `_acquired` flag),
                # touch the signal file, and call ha_recovery →
                # maybe_start_controllers — which would spawn fresh
                # controllers under a now-released lock while the new
                # leader on another replica is doing the same. Stop the
                # thread instead so the SIGTERM-driven drain runs to
                # completion without further controller churn.
                return
            except Exception:  # pylint: disable=broad-except
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
                time.sleep(_ACQUIRE_RETRY_INTERVAL_SECONDS)

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
        # NOTE: the acquire is deliberately NOT inside the try/finally that
        # wraps recovery below. The finally unlinks the signal file, but the
        # lock-loss step-down path (_suicide_on_lock_loss) re-touches it to
        # keep controllers gated through the shutdown drain — so that path must
        # not be followed by an unlink. Scoping the finally to recovery only
        # keeps those two concerns from fighting. It also means a raise from
        # acquire() leaves the gate file in place while run() retries, which is
        # what we want (controller starts stay gated until we hold the lock).
        signal_file = _touch_recovery_signal_file()

        if not self._lock.is_locked():
            logger.info(f'Acquiring the consolidation mode lock: {self._lock}')
            self._lock.acquire()
            logger.info('Consolidation mode lock acquired')

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

        try:
            managed_job_utils.ha_recovery_for_consolidation_mode()
        finally:
            signal_file.unlink(missing_ok=True)

        # Event-loop tick at events.EVENT_CHECKING_INTERVAL_SECONDS and lock
        # probe at _LOCK_PROBE_INTERVAL_SECONDS. Sleep until the earlier
        # deadline instead of waking every second to re-check both.
        refresh_event = events.ManagedJobEvent()
        now = time.monotonic()
        next_probe = now + _LOCK_PROBE_INTERVAL_SECONDS
        next_event = now
        while True:
            now = time.monotonic()
            if now >= next_probe:
                if not self._lock_still_held():
                    self._suicide_on_lock_loss()
                    return
                next_probe = now + _LOCK_PROBE_INTERVAL_SECONDS
            if now >= next_event:
                try:
                    refresh_event.run()
                except Exception:  # pylint: disable=broad-except
                    logger.exception('ManagedJobEvent tick failed; will retry')
                next_event = now + events.EVENT_CHECKING_INTERVAL_SECONDS
            sleep_seconds = max(0.0, min(next_probe, next_event) - now)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

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
            time.sleep(sleep_seconds)
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
        # The lock is already released, kill job controllers to avoid split
        # brain, e.g. new job controllers might have been launched on the new
        # replica during rolling-update
        try:
            managed_job_scheduler.fail_stop_local_job_controllers()
        except Exception:  # pylint: disable=broad-except
            logger.exception(
                'Failed to fail-stop local controllers on lock-loss')
        # SIGTERM to trigger graceful shutdown
        os.kill(os.getpid(), signal.SIGTERM)


def start_managed_job_refresh_daemon() -> None:
    """Start the refresh thread for this API server process, if needed.

    No-op when consolidation mode is off — mirrors the gating that the
    historical ``should_skip_managed_job_status_refresh`` provided.
    """
    if not managed_job_utils.is_consolidation_mode():
        logger.debug('Consolidation mode is off; not starting the managed-job '
                     'refresh thread.')
        return
    logger.info('Starting the managed-job refresh thread')
    ManagedJobRefreshDaemonThread().start()
