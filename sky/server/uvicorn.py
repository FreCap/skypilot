"""Uvicorn wrapper for SkyPilot API server.

This module is a wrapper around uvicorn to customize the behavior of the
server.
"""

import asyncio
import logging
import os
import signal
import sys
import threading
import time
from types import FrameType

import filelock
import uvicorn
from uvicorn.supervisors import multiprocess

from sky import sky_logging
from sky.server import daemons
from sky.server import metrics as metrics_lib
from sky.server import state
from sky.server import watchdog
from sky.server.requests import requests as requests_lib
from sky.server.requests import storage as request_storage
from sky.skylet import constants
from sky.utils import context_utils
from sky.utils import env_options
from sky.utils import perf_utils
from sky.utils import subprocess_utils
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

# A short wait for the endpoints update propagated to the ingress/LB
_GRACE_WAIT_SECONDS = 5
# File lock path for coordinating graceful shutdown across processes
_GRACEFUL_SHUTDOWN_LOCK_PATH = '/tmp/skypilot_graceful_shutdown.lock'

# Interval to check for on-going requests.
_WAIT_REQUESTS_INTERVAL_SECONDS = 5

# Timeout for waiting for on-going requests to finish.
try:
    _WAIT_REQUESTS_TIMEOUT_SECONDS = int(
        os.environ.get(constants.GRACE_PERIOD_SECONDS_ENV_VAR, '60'))
except ValueError:
    _WAIT_REQUESTS_TIMEOUT_SECONDS = 60

# Reserve a margin before the SIGKILL deadline so the final "interrupt all
# on-going requests for retry" sweep -- the only path that flags non-retriable
# requests (sky.launch / sky.exec / sky.jobs.launch / ...) with should_retry --
# actually runs and its DB writes commit before the worker is hard-killed.
# The Helm chart wires terminationGracePeriodSeconds (the SIGKILL deadline) to
# the same value as this timeout, so without a margin the sweep fires at
# ~T+timeout, i.e. no earlier than SIGKILL, and those requests are silently
# dropped on restart instead of being retried by the client.
_INTERRUPT_BEFORE_SHUTDOWN_DEADLINE_SECONDS = (2 *
                                               _WAIT_REQUESTS_INTERVAL_SECONDS)


class _AccessLogQueryFilter(logging.Filter):
    """Removes query strings before Uvicorn formats an access-log record.

    Query parameters are untrusted and several public APIs legitimately carry
    selectors or workspace names in them.  Uvicorn constructs its access-log
    record before FastAPI validation, so endpoint-level sanitization is too
    late.  Preserve method/path/status observability while never emitting the
    query component.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != 'uvicorn.access':
            return True
        args = record.args
        if (isinstance(args, tuple) and len(args) >= 3 and
                isinstance(args[2], str)):
            sanitized_path = args[2].split('?', 1)[0]
            if sanitized_path != args[2]:
                record.args = (*args[:2], sanitized_path, *args[3:])
        return True


_ACCESS_LOG_QUERY_FILTER = _AccessLogQueryFilter()

# TODO(aylei): use decorator to register requests that need to be proactively
# cancelled instead of hardcoding here.
_RETRIABLE_REQUEST_NAMES = {
    'sky.logs',
    'sky.jobs.logs',
    'sky.serve.logs',
}


def add_timestamp_prefix_for_server_logs() -> None:
    """Configure logging for API server.

    Note: we only do this in the main API server process and uvicorn processes,
    to avoid affecting executor logs (including in modules like
    sky.server.requests) that may get sent to the client.
    """
    server_logger = sky_logging.init_logger('sky.server')
    # Clear existing handlers first to prevent duplicates
    server_logger.handlers.clear()
    # Disable propagation to avoid the root logger of SkyPilot being affected
    server_logger.propagate = False
    # Add date prefix to the log message printed by loggers under
    # server.
    stream_handler = logging.StreamHandler(sys.stdout)
    if env_options.Options.SHOW_DEBUG_INFO.get():
        stream_handler.setLevel(logging.DEBUG)
    else:
        stream_handler.setLevel(logging.INFO)
    stream_handler.flush = sys.stdout.flush  # type: ignore
    stream_handler.setFormatter(sky_logging.FORMATTER)
    server_logger.addHandler(stream_handler)
    # Add date prefix to the log message printed by uvicorn.
    for name in ['uvicorn', 'uvicorn.access']:
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        if name == 'uvicorn.access' and not any(
                current is _ACCESS_LOG_QUERY_FILTER
                for current in uvicorn_logger.filters):
            uvicorn_logger.addFilter(_ACCESS_LOG_QUERY_FILTER)
        uvicorn_logger.addHandler(stream_handler)


class Server(uvicorn.Server):
    """Server wrapper for uvicorn.

    Extended functionalities:
    - Handle exit signal and perform custom graceful shutdown.
    - Run the server process with contextually aware.
    """

    def __init__(self,
                 config: uvicorn.Config,
                 max_db_connections: int | None = None):
        super().__init__(config=config)
        self.exiting: bool = False
        self.max_db_connections = max_db_connections
        # Monotonic time at which the first shutdown signal arrived; used to
        # budget the on-going-request wait against the real SIGKILL deadline.
        self._shutdown_started_at: float | None = None

    def handle_exit(self, sig: int, frame: FrameType | None) -> None:
        """Handle exit signal.

        When a server process receives a SIGTERM or SIGINT signal, a graceful
        shutdown will be initiated. If a SIGINT signal is received again, the
        server will be forcefully shutdown.
        """
        if self.exiting and sig == signal.SIGINT:
            # The server has been signaled to exit and received a SIGINT again,
            # do force shutdown.
            logger.info('Force shutdown.')
            self.should_exit = True
            super().handle_exit(sig, frame)
            return
        if not self.exiting:
            self.exiting = True
            # Capture the true signal-arrival time so _wait_requests budgets
            # its timeout against the real shutdown start (and thus the k8s
            # SIGKILL deadline), not against the later moment the elected
            # coordinator reaches _wait_requests after the grace sleeps + lock.
            self._shutdown_started_at = time.monotonic()
            # Perform graceful shutdown in a separate thread to avoid blocking
            # the main thread.
            threading.Thread(target=self._graceful_shutdown,
                             args=(sig, frame),
                             daemon=True).start()

    def _graceful_shutdown(self, sig: int, frame: FrameType | None) -> None:
        """Perform graceful shutdown."""
        time.sleep(_GRACE_WAIT_SECONDS)
        # Block new requests so that we can wait until all on-going requests
        # are finished. Note that /api/$verb operations are still allowed in
        # this stage to ensure the client can still operate the on-going
        # requests, e.g. /api/logs, /api/cancel, etc.
        logger.info('Block new requests being submitted in worker '
                    f'{os.getpid()}.')
        state.set_block_requests(True)
        # Ensure the shutting_down are set on all workers before next step.
        # TODO(aylei): hacky, need a reliable solution.
        time.sleep(1)

        lock = filelock.FileLock(_GRACEFUL_SHUTDOWN_LOCK_PATH)
        # Elect a coordinator process to handle on-going requests check
        with lock.acquire():
            logger.info(f'Worker {os.getpid()} elected as shutdown coordinator')
            self._wait_requests()

        logger.info('Shutting down server...')
        self.should_exit = True
        super().handle_exit(sig, frame)

    def _wait_requests(self) -> None:
        """Wait until all on-going requests are finished or cancelled."""
        if os.environ.get('SKYPILOT_API_SERVER_ROLE', 'all') == 'api':
            # API-only pods do not own request execution. Their durable rows
            # may be running on any executor replica, so a local API shutdown
            # must never wait for or interrupt that fleet-wide work.
            logger.info('API-only role has no local request executions to '
                        'drain.')
            return
        # Budget the final interrupt sweep against the true shutdown start
        # (captured in handle_exit when SIGTERM arrived), reserving a margin
        # before the SIGKILL deadline so the sweep -- and its should_retry DB
        # writes -- complete before the worker is killed. Falls back to "now"
        # if _wait_requests is somehow reached without a recorded start.
        shutdown_started_at = self._shutdown_started_at or time.monotonic()
        interrupt_deadline = shutdown_started_at + max(
            0, _WAIT_REQUESTS_TIMEOUT_SECONDS -
            _INTERRUPT_BEFORE_SHUTDOWN_DEADLINE_SECONDS)
        while True:
            requests = (request_storage.get_request_backend().
                        get_shutdown_active_requests())
            # Replayable requests (launches) neither block shutdown nor get
            # cancelled: their rows are left as-is so startup recovery can
            # requeue and re-execute them (safe until their cluster reaches
            # UP, see requests_lib._find_interrupted_launches_to_requeue).
            # Waiting
            # for them here is pointless -- a provisioning launch outlives
            # any realistic shutdown grace -- and cancelling them would wedge
            # the half-provisioned cluster in INIT with only a client-side
            # retry signal that a disconnected client never sees.
            requests = [(request_id, name)
                        for request_id, name in requests
                        if name not in requests_lib.REPLAYABLE_REQUEST_NAMES]
            if not requests:
                break
            logger.info(f'{len(requests)} on-going requests '
                        'found, waiting for them to finish...')
            # Proactively cancel internal requests and logs requests since
            # they can run for infinite time.
            internal_request_ids = {
                d.id for d in daemons.INTERNAL_REQUEST_DAEMONS
            }
            if time.monotonic() >= interrupt_deadline:
                logger.warning('Timeout waiting for on-going requests to '
                               'finish, cancelling all on-going requests.')
                for request_id, _ in requests:
                    self.interrupt_request_for_retry(request_id)
                break
            interrupted = 0
            for request_id, name in requests:
                if (name in _RETRIABLE_REQUEST_NAMES or
                        request_id in internal_request_ids):
                    self.interrupt_request_for_retry(request_id)
                    interrupted += 1
                # TODO(aylei): interrupt pending requests to accelerate the
                # shutdown.
            # If some requests are not interrupted, wait for them to finish,
            # otherwise we just check again immediately to accelerate the
            # shutdown process.
            if interrupted < len(requests):
                time.sleep(_WAIT_REQUESTS_INTERVAL_SECONDS)

    def interrupt_request_for_retry(self, request_id: str) -> None:
        """Interrupt a request for retry."""
        with requests_lib.update_request(request_id) as req:
            if req is None:
                return
            # A request can finish between the snapshot taken in
            # `_wait_requests` and this call. Every other kill-path
            # (`_should_kill_request`, `kill_request_async`,
            # `set_request_cancelled_async`) skips a request whose status is
            # already terminal; mirror that here. Without the guard we would
            # (a) overwrite a SUCCEEDED/FAILED result with CANCELLED +
            # should_retry -- losing the recorded return value and making the
            # client re-run an operation that already completed -- and
            # (b) SIGTERM a stale `pid`: finished requests do not clear `pid`
            # and the worker pool reuses PIDs, so the signal could hit an
            # unrelated in-flight request.
            if req.status > requests_lib.RequestStatus.RUNNING:
                return
            if req.pid is not None:
                try:
                    os.kill(req.pid, signal.SIGTERM)
                except ProcessLookupError:
                    logger.debug(f'Process {req.pid} already finished.')
            req.status = requests_lib.RequestStatus.CANCELLED
            req.finished_at = time.time()
            req.should_retry = True
        logger.info(
            f'Request {request_id} interrupted and will be retried by client.')

    def run(self, *args, **kwargs):
        """Run the server process."""
        # In multi-worker mode this runs in a spawned worker process. If the
        # main server process dies abruptly (kill -9, OOM), exit with it
        # instead of keeping the API port bound and health checks green while
        # the dispatcher threads (which lived in the main process) are gone.
        # In single-worker mode run() executes in the main process itself and
        # the guard keeps the watchdog unarmed.
        if watchdog.running_in_child_process():
            watchdog.start_parent_death_watchdog()
        if self.max_db_connections is not None:
            db_utils.set_max_connections(self.max_db_connections)
        add_timestamp_prefix_for_server_logs()
        context_utils.hijack_sys_attrs()
        # Use default loop policy of uvicorn (use uvloop if available).
        self.config.setup_event_loop()
        # Reap this worker's per-pid prometheus multiproc files at exit so
        # that recycled workers do not leak stale liveall gauge values
        # (e.g. event-loop-lag peaks recorded just before the worker died)
        # to every subsequent /metrics scrape and liveall-based probe.
        metrics_lib.register_multiproc_cleanup_atexit()
        lag_threshold = perf_utils.get_loop_lag_threshold()

        async def _serve(*serve_args, **serve_kwargs):
            # Configure the serving loop from inside the coroutine:
            # asyncio.run() below creates its own fresh loop, so a loop
            # obtained here via asyncio.get_event_loop() would not be the
            # one that serves (and on Python 3.14+ that call raises when no
            # loop is running).
            _configure_running_loop_lag_debug(lag_threshold)
            try:
                await self.serve(*serve_args, **serve_kwargs)
            finally:
                # Lifespan startup failures are logged and converted into a
                # normal return by Uvicorn. Close the process-local request DB
                # before asyncio.run() closes this loop; otherwise aiosqlite's
                # non-daemon connection thread keeps the failed worker alive
                # and responsive to supervisor pings indefinitely.
                await requests_lib.close_db_async()

        stop_monitor = threading.Event()
        monitor = threading.Thread(
            target=metrics_lib.process_monitor,
            args=('server', stop_monitor),
            daemon=True,
        )
        monitor.start()
        try:
            with self.capture_signals():
                asyncio.run(_serve(*args, **kwargs))
        finally:
            stop_monitor.set()
            monitor.join()


def _configure_running_loop_lag_debug(lag_threshold: float | None) -> None:
    """Enable slow-callback logging on the currently running loop.

    Must be called from inside the serving coroutine so it configures the
    loop created by asyncio.run(), not a bystander loop.
    """
    if lag_threshold is None:
        return
    event_loop = asyncio.get_running_loop()
    # Same as set PYTHONASYNCIODEBUG=1, but with custom threshold.
    event_loop.set_debug(True)
    event_loop.slow_callback_duration = lag_threshold


def run(config: uvicorn.Config, max_db_connections: int | None = None):
    """Run unvicorn server."""
    if config.reload:
        # Reload and multi-workers are mutually exclusive
        # in uvicorn. Since we do not use reload now, simply
        # guard by an exception.
        raise ValueError('Reload is not supported yet.')
    server = Server(config=config, max_db_connections=max_db_connections)
    try:
        if config.workers is not None and config.workers > 1:
            sock = config.bind_socket()
            SlowStartMultiprocess(config, target=server.run,
                                  sockets=[sock]).run()
        else:
            server.run()
    finally:
        # Copied from unvicorn.run()
        if config.uds and os.path.exists(config.uds):
            os.remove(config.uds)


class SlowStartMultiprocess(multiprocess.Multiprocess):
    """Uvicorn Multiprocess wrapper with slow start.

    Slow start offers faster and more stable  start time.
    Profile shows the start time is more stable and accelerated from
    ~7s to ~3.3s on a 12-core machine after switching LONG workers and
    Uvicorn workers to slow start.
    Refer to subprocess_utils.slow_start_processes() for more details.
    """

    def __init__(self, config: uvicorn.Config, **kwargs):
        """Initialize the multiprocess wrapper.

        Args:
            config: The uvicorn config.
        """
        super().__init__(config, **kwargs)
        self._init_thread: threading.Thread | None = None

    def init_processes(self) -> None:
        # Slow start worker processes asynchronously to avoid blocking signal
        # handling of uvicorn.
        self._init_thread = threading.Thread(target=self.slow_start_processes,
                                             daemon=True)
        self._init_thread.start()

    def slow_start_processes(self) -> None:
        """Initialize processes with slow start."""
        to_start = []
        # Init N worker processes
        for _ in range(self.processes_num):
            to_start.append(
                multiprocess.Process(self.config, self.target, self.sockets))
        # Start the processes with slow start, we only append start to
        # self.processes because Uvicorn periodically restarts unstarted
        # workers.
        subprocess_utils.slow_start_processes(to_start,
                                              on_start=self.processes.append,
                                              should_exit=self.should_exit)

    def terminate_all(self) -> None:
        """Wait init thread to finish before terminating all processes."""
        if self._init_thread is not None:
            self._init_thread.join()
        super().terminate_all()
