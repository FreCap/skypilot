"""Internal server daemons that run in the background."""
import atexit
from collections.abc import Callable
import dataclasses
import os
import shutil
import sys
import time

from sky import sky_logging
from sky import skypilot_config
from sky.server import constants as server_constants
from sky.server.requests import request_names
from sky.skylet import constants
from sky.utils import annotations
from sky.utils import common_utils
from sky.utils import env_options
from sky.utils import locks
from sky.utils import timeline
from sky.utils import ux_utils

logger = sky_logging.init_logger(__name__)


def _rotate_daemon_log(log_path: str) -> None:
    """Rotate the daemon log if it exceeds the size threshold.

    Uses the copytruncate pattern: copy current log to a backup file,
    then truncate the original. This keeps one backup for external log
    collectors and debugging.

    The threshold is configurable via api_server.daemon_log_max_bytes in
    ~/.sky/config.yaml, defaulting to DAEMON_LOG_MAX_BYTES.
    """
    try:
        max_bytes = skypilot_config.get_nested(
            ('api_server', 'daemon_log_max_bytes'),
            server_constants.DEFAULT_DAEMON_LOG_MAX_BYTES)
        if max_bytes <= 0:
            return
        sys.stdout.flush()
        sys.stderr.flush()
        fd = sys.stdout.fileno()
        if os.fstat(fd).st_size < max_bytes:
            return
        # Copy current log to backup before truncating.
        backup_path = log_path + '.1'
        shutil.copy2(log_path, backup_path)
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
    except Exception:  # pylint: disable=broad-except
        # Never crash the daemon on rotation failure.
        pass


# Populated lazily on the first run_event() call per process.
#
# Why not snapshot at import time: importing this module can happen before
# the process has finished its startup initialization, so an import-time
# read of env_options.Options.DISABLE_LOGGING may not reflect the final
# intended value.
#
# Why not read inside the event: run_event() overrides DISABLE_LOGGING to
# silence per-daemon usage messages, so a read at gate time would always
# see the override rather than the user's original setting.
#
# Initializing inside run_event, before the override, threads the needle.
_user_disabled_usage_collection: bool | None = None


def _default_should_skip():
    return False


@dataclasses.dataclass
class RuntimeDaemon:
    """Controller-owned maintenance loop run in a supervised subprocess."""

    id: str
    name: request_names.RequestName
    event_fn: Callable[[], None]
    default_log_level: str = 'INFO'
    should_skip: Callable[[], bool] = _default_should_skip

    def refresh_log_level(self) -> int:
        # pylint: disable=import-outside-toplevel
        import logging

        level_str = self.default_log_level
        try:
            # Refresh config within the while loop.
            # Since this is a long running daemon,
            # reload_for_new_request()
            # is not called in between the event runs.
            # We don't need to grab the lock here because each of the daemons
            # run in their own process and thus have their own request context.
            skypilot_config.reload_config()
            # Get the configured log level for the daemon inside the event loop
            # in case the log level changes after the API server is started.
            level_str = skypilot_config.get_nested(
                ('daemons', self.id, 'log_level'), self.default_log_level)
            return getattr(logging, level_str.upper())
        except AttributeError:
            # Bad level should be rejected by
            # schema validation, just in case.
            logger.warning(f'Invalid log level: {level_str}, using DEBUG')
            return logging.DEBUG
        except Exception as e:  # pylint: disable=broad-except
            logger.exception(f'Error refreshing log level for {self.id}: {e}')
            return logging.DEBUG

    def run_event(self):
        """Run the event."""
        global _user_disabled_usage_collection
        # Capture the original DISABLE_LOGGING value once per process,
        # before we override it below.
        if _user_disabled_usage_collection is None:
            _user_disabled_usage_collection = (
                env_options.Options.DISABLE_LOGGING.get())

        # Disable logging for periodic refresh to avoid the usage message being
        # sent multiple times.
        os.environ[env_options.Options.DISABLE_LOGGING.env_key] = '1'

        log_path = os.path.join(
            os.path.expanduser(server_constants.REQUEST_LOG_PATH_PREFIX),
            self.id + '.log')

        level = self.refresh_log_level()
        while True:
            try:
                with ux_utils.enable_traceback(), \
                    sky_logging.set_sky_logging_levels(level):
                    sky_logging.reload_logger()
                    level = self.refresh_log_level()
                    self.event_fn()
            except Exception:  # pylint: disable=broad-except
                # It is OK to fail to run the event, as the event is not
                # critical, but we should log the error.
                logger.exception(
                    f'Error running {self.name} event. '
                    f'Restarting in '
                    f'{server_constants.DAEMON_RESTART_INTERVAL_SECONDS} '
                    'seconds...')
                time.sleep(server_constants.DAEMON_RESTART_INTERVAL_SECONDS)
            finally:
                # Clear request level cache after each run to avoid
                # using too much memory.
                annotations.clear_request_level_cache()
                timeline.save_timeline()
                # The runtime-daemon launcher owns one isolated process-group
                # boundary and drains it when the daemon lifetime ends.  Do
                # not perform request-worker-style broad child cleanup here:
                # the independent group guardian is intentionally a direct
                # child and must survive every successful event iteration.
                common_utils.release_memory()
                _rotate_daemon_log(log_path)


def refresh_cluster_status_event():
    """Periodically refresh the cluster status."""
    # pylint: disable=import-outside-toplevel
    from sky.backends import backend_utils

    logger.info('=== Refreshing cluster status ===')
    # This periodically refresh will hold the lock for the cluster being
    # refreshed, but it is OK because other operations will just wait for
    # the lock and get the just refreshed status without refreshing again.
    backend_utils.refresh_cluster_records()
    logger.info('Status refreshed. Sleeping '
                f'{server_constants.CLUSTER_REFRESH_DAEMON_INTERVAL_SECONDS}'
                ' seconds for the next refresh...\n')
    time.sleep(server_constants.CLUSTER_REFRESH_DAEMON_INTERVAL_SECONDS)


def refresh_volume_status_event():
    """Periodically refresh the volume status."""
    # pylint: disable=import-outside-toplevel
    from sky.volumes.server import core

    # Disable logging for periodic refresh to avoid the usage message being
    # sent multiple times.
    os.environ[env_options.Options.DISABLE_LOGGING.env_key] = '1'

    logger.info('=== Refreshing volume status ===')
    core.volume_refresh()
    logger.info('Volume status refreshed. Sleeping '
                f'{server_constants.VOLUME_REFRESH_DAEMON_INTERVAL_SECONDS}'
                ' seconds for the next refresh...\n')
    time.sleep(server_constants.VOLUME_REFRESH_DAEMON_INTERVAL_SECONDS)


_pool_consolidation_mode_lock = None
_serve_consolidation_mode_lock = None

# Module-scoped SkyletEvent instances. `SkyletEvent.run()` relies on the
# internal `_n` counter accumulating across calls to throttle the heavy
# `_run()` work to once per EVENT_INTERVAL_SECONDS. Recreating the event
# inside each daemon iteration would reset `_n` to 0 every call, run()
# would advance it only to 1, the `_n == 0` trigger would never fire,
# and the throttled work (update_service_status / managed_job_utils
# update) would NEVER execute. The outer `while True` in
# RuntimeDaemon.run_event re-invokes the event_fn, so these
# instances must outlive a single iteration.
# (managed-job uses its own instance held by ManagedJobRefreshDaemonThread
# in sky/jobs/managed_job_refresh_thread.py — it lives on the thread,
# not on this module.)
_pool_status_update_event = None
_serve_status_update_event = None
_serve_status_history_event = None


# Attempt to gracefully release the lock when the process exits.
# If this fails, it's okay, the lock will be released when the process dies.
def _release_serve_and_pool_consolidation_mode_locks() -> None:
    global _pool_consolidation_mode_lock, _serve_consolidation_mode_lock
    if _pool_consolidation_mode_lock is not None:
        _pool_consolidation_mode_lock.release()
        _pool_consolidation_mode_lock = None
    if _serve_consolidation_mode_lock is not None:
        _serve_consolidation_mode_lock.release()
        _serve_consolidation_mode_lock = None


atexit.register(_release_serve_and_pool_consolidation_mode_locks)


# Backward-compatibility no-op stubs for rolling upgrade. Pickled
# Runtime-daemon request rows from older server versions reference these
# symbols via the `event_fn` / `should_skip` attributes, so pickle.loads
# raises AttributeError without them. Orphan rows are then cleaned up by
# startup's explicit legacy-row retirement path.
def managed_job_status_refresh_event():
    """No-op stub for pickle compatibility with older server versions.

    The managed-job-status refresh now runs in-process as a thread; the
    daemon-based variant is no longer scheduled. This stub exists only so
    that old pickled daemon rows can be deserialized and then deleted by
    the daemon-orphan cleanup path.
    """


def should_skip_managed_job_status_refresh():
    """No-op stub for pickle compatibility with older server versions."""
    return True


def _leader_session_alive(lock: locks.DistributedLock) -> bool:
    """Whether the leader lock is still held on a live session.

    Only PostgresLock has a revocable session; other lock types (single-pod
    filelock) cannot be lost silently, so they always count as held. Mirrors
    `_lock_still_held` in sky/jobs/managed_job_refresh_thread.py.
    """
    if isinstance(lock, locks.PostgresLock):
        return lock.is_session_alive()
    return True


def _ensure_leader_lock(lock: locks.DistributedLock | None, lock_id: str,
                        lock_label: str) -> locks.DistributedLock:
    """Return the consolidation leader lock, held on a live session.

    A PostgresLock is a session-scoped advisory lock: if the session that
    holds it dies without this process noticing (RDS failover / maintenance
    restart, LB idle timeout, pg_terminate_backend, network partition), the
    server releases the lock while our local `is_locked()` flag stays True.
    Another pod can then acquire it and run HA recovery concurrently with us
    — split-brain, with two pods spawning controllers for the same services.
    Probe the session each iteration (mirroring `_lock_still_held` in
    sky/jobs/managed_job_refresh_thread.py) and, on loss, discard the stale
    lock object and re-acquire on a fresh session. If another pod took the
    lock in the meantime, the re-acquire blocks and this pod correctly stops
    running recovery/refresh until leadership is regained.
    """
    if lock is None:
        lock = locks.get_lock(lock_id)
    if lock.is_locked() and not _leader_session_alive(lock):
        logger.error(
            f'The {lock_label} session is no longer alive; the advisory '
            'lock was released server-side and another pod may hold it '
            'now. Discarding the stale lock and re-acquiring on a fresh '
            'session.')
        try:
            lock.release()
        except Exception:  # pylint: disable=broad-except
            # release() on a dead session is best-effort; the replacement
            # lock object below owns a fresh connection either way.
            logger.debug(f'Best-effort release of the stale {lock_label} '
                         'failed; discarding it anyway.')
        lock = locks.get_lock(lock_id)
    if not lock.is_locked():
        logger.info(f'Acquiring the {lock_label}: {lock}')
        lock.acquire()
        logger.info(f'{lock_label} acquired')
    return lock


def _serve_status_refresh_event(pool: bool):
    """Refresh the sky serve status for controller consolidation mode."""
    # pylint: disable=import-outside-toplevel
    from sky.serve import constants as serve_constants
    from sky.serve import maintenance
    from sky.serve import serve_utils

    if not pool and maintenance.is_controller_hold_active():
        # Keep the runtime daemon alive, but do no recovery, status mutation,
        # or orphan-LB cleanup while operators rewrite persisted contracts.
        # Returning without sleeping would busy-loop in run_event().
        from sky.skylet import events
        logger.warning('SkyServe controller recovery and status refresh are '
                       'held by the server deployment.')
        time.sleep(events.EVENT_CHECKING_INTERVAL_SECONDS)
        return

    # Acquire an advisory lock so that only one pod runs the recovery /
    # controller-startup path at a time. Re-verified every iteration: a
    # silently-dead PG session would otherwise leave two pods both believing
    # they are the leader (see _ensure_leader_lock).
    global _pool_consolidation_mode_lock, _serve_consolidation_mode_lock
    if pool:
        _pool_consolidation_mode_lock = _ensure_leader_lock(
            _pool_consolidation_mode_lock,
            serve_constants.POOL_CONSOLIDATION_MODE_LOCK_ID,
            'pool consolidation mode lock')
        lock = _pool_consolidation_mode_lock
    else:
        _serve_consolidation_mode_lock = _ensure_leader_lock(
            _serve_consolidation_mode_lock,
            serve_constants.SERVE_CONSOLIDATION_MODE_LOCK_ID,
            'serve consolidation mode lock')
        lock = _serve_consolidation_mode_lock

    def _still_leader() -> bool:
        return _leader_session_alive(lock)

    # We run the recovery logic before starting the event loop as those two are
    # conflicting. Check PERSISTENT_RUN_RESTARTING_SIGNAL_FILE for details.
    # `still_leader` re-fences every recovery launch: the sweep can take a
    # while with many services, and the lock session can die mid-sweep.
    serve_utils.ha_recovery_for_consolidation_mode(pool=pool,
                                                   still_leader=_still_leader)

    # Do not run the status-refresh event without leadership either: its
    # controller-pid liveness checks are pod-local, so a non-leader pod
    # would wrongly mark the (new) leader's controllers CONTROLLER_FAILED.
    # The next iteration re-enters _ensure_leader_lock and blocks/re-acquires.
    if not _still_leader():
        logger.error('Consolidation leader lock session lost; skipping the '
                     'status-refresh event this iteration.')
        return

    # After recovery, we start the event loop. The event instance is
    # cached at module scope so its internal throttling counter
    # accumulates across daemon iterations (see comment near
    # _pool_status_update_event / _serve_status_update_event
    # declarations).
    from sky.skylet import events
    global _pool_status_update_event, _serve_status_update_event
    global _serve_status_history_event
    if pool:
        if _pool_status_update_event is None:
            _pool_status_update_event = events.ServiceUpdateEvent(pool=True)
        event = _pool_status_update_event
    else:
        if _serve_status_update_event is None:
            _serve_status_update_event = events.ServiceUpdateEvent(pool=False)
        event = _serve_status_update_event
    noun = 'pool' if pool else 'serve'
    logger.info(f'=== Running {noun} status refresh event ===')
    if not pool:
        if _serve_status_history_event is None:
            _serve_status_history_event = events.ServiceStatusHistoryEvent()
        _serve_status_history_event.run()
    event.run()
    time.sleep(events.EVENT_CHECKING_INTERVAL_SECONDS)


def _should_skip_serve_status_refresh_event(pool: bool):
    """Check if the serve status refresh event should be skipped."""
    # pylint: disable=import-outside-toplevel
    from sky.serve import serve_utils
    return not serve_utils.is_consolidation_mode(pool=pool)


def sky_serve_status_refresh_event():
    _serve_status_refresh_event(pool=False)


def should_skip_sky_serve_status_refresh():
    return _should_skip_serve_status_refresh_event(pool=False)


def pool_status_refresh_event():
    _serve_status_refresh_event(pool=True)


def should_skip_pool_status_refresh():
    return _should_skip_serve_status_refresh_event(pool=True)


def should_skip_server_heartbeat():
    """Skip server heartbeat when running as a controller."""
    if os.environ.get(constants.OVERRIDE_CONSOLIDATION_MODE) is not None:
        # We are running as a controller.
        return True
    return False


def expired_token_cleanup_event():
    """Periodically remove expired managed-job API access tokens."""
    # pylint: disable=import-outside-toplevel
    from sky.jobs import utils as managed_job_utils

    logger.info('=== Cleaning up expired managed-job API access tokens ===')
    removed = managed_job_utils.cleanup_expired_api_access_tokens()
    # Read the interval from config on every iteration so operators can
    # lower it (e.g., for testing) without restarting the API server.
    interval = skypilot_config.get_nested(
        ('daemons', 'expired-token-cleanup-daemon', 'interval_seconds'),
        server_constants.EXPIRED_TOKEN_CLEANUP_DAEMON_INTERVAL_SECONDS)
    logger.info(f'Expired token cleanup removed {removed} token(s). '
                f'Sleeping {interval} seconds for the next sweep...\n')
    time.sleep(interval)


def server_heartbeat_event():
    """Periodically send server-side plugin metrics to Loki."""
    # pylint: disable=import-outside-toplevel
    from sky.usage import usage_lib

    # Skip if no plugins registered providers (check inside event_fn, not
    # should_skip, because providers register in executor processes via
    # plugin install(), not in the main process where should_skip runs),
    # or if the user explicitly disabled usage collection.
    if (not usage_lib.ServerHeartbeatMessage.has_providers() or
            _user_disabled_usage_collection):
        time.sleep(server_constants.SERVER_HEARTBEAT_INTERVAL_SECONDS)
        return

    # _send_to_loki checks DISABLE_LOGGING, but run_event() sets it to '1'
    # to prevent usage messages from daemons. We temporarily unset it here
    # because the server heartbeat's purpose IS to send to Loki.
    disable_key = env_options.Options.DISABLE_LOGGING.env_key
    original_val = os.environ.pop(disable_key, None)
    try:
        usage_lib.send_server_heartbeat()
        logger.info('Server heartbeat sent')
    finally:
        if original_val is not None:
            os.environ[disable_key] = original_val
    time.sleep(server_constants.SERVER_HEARTBEAT_INTERVAL_SECONDS)


# Runtime-owned maintenance processes.  These are deliberately not durable API
# request handlers: the elected controller supervisor owns their lifecycle and
# the request store contains only finite client/system operations.
RUNTIME_DAEMONS = [
    # This status refresh daemon can cause the autostopp'ed/autodown'ed cluster
    # set to updated status automatically, without showing users the hint of
    # cluster being stopped or down when `sky status -r` is called.
    RuntimeDaemon(id='skypilot-status-refresh-daemon',
                  name=request_names.RequestName.REQUEST_DAEMON_STATUS_REFRESH,
                  event_fn=refresh_cluster_status_event,
                  default_log_level='DEBUG'),
    # Volume status refresh daemon to update the volume status periodically.
    RuntimeDaemon(id='skypilot-volume-status-refresh-daemon',
                  name=request_names.RequestName.REQUEST_DAEMON_VOLUME_REFRESH,
                  event_fn=refresh_volume_status_event),
    RuntimeDaemon(
        id='sky-serve-status-refresh-daemon',
        name=request_names.RequestName.REQUEST_DAEMON_SKY_SERVE_STATUS_REFRESH,
        event_fn=sky_serve_status_refresh_event,
        should_skip=should_skip_sky_serve_status_refresh),
    RuntimeDaemon(
        id='pool-status-refresh-daemon',
        name=request_names.RequestName.REQUEST_DAEMON_POOL_STATUS_REFRESH,
        event_fn=pool_status_refresh_event,
        should_skip=should_skip_pool_status_refresh),
    RuntimeDaemon(
        id='server-heartbeat-daemon',
        name=request_names.RequestName.REQUEST_DAEMON_SERVER_HEARTBEAT,
        event_fn=server_heartbeat_event,
        should_skip=should_skip_server_heartbeat),
    RuntimeDaemon(
        id='expired-token-cleanup-daemon',
        name=request_names.RequestName.REQUEST_DAEMON_EXPIRED_TOKEN_CLEANUP,
        event_fn=expired_token_cleanup_event),
]

_RUNTIME_DAEMONS_BY_ID = {daemon.id: daemon for daemon in RUNTIME_DAEMONS}
if len(_RUNTIME_DAEMONS_BY_ID) != len(RUNTIME_DAEMONS):
    raise RuntimeError('Runtime daemon IDs must be unique.')

# Explicit transition inventory.  Never replace this with a suffix query: a
# user-selected finite request ID may legitimately end in ``-daemon``.  The
# managed-jobs ID belongs to a retired implementation whose pickle symbols are
# kept above solely so an old row can be deleted without decode failure.
LEGACY_REQUEST_DAEMON_IDS = frozenset({
    *(_RUNTIME_DAEMONS_BY_ID.keys()),
    'managed-job-status-refresh-daemon',
})


def get_runtime_daemon(daemon_id: str) -> RuntimeDaemon:
    """Resolve one runtime daemon by its closed, code-owned identifier."""
    try:
        return _RUNTIME_DAEMONS_BY_ID[daemon_id]
    except KeyError as e:
        raise ValueError(f'Unknown runtime daemon ID: {daemon_id!r}.') from e
