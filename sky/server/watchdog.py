"""Parent-death watchdog for API server child processes.

The API server runs several child processes (the request queue manager and
uvicorn workers). If the main server process is killed abruptly (kill -9,
OOM), those children are orphaned and reparented: uvicorn workers keep
answering health checks and enqueuing requests that the (dead) dispatcher
threads will never dequeue, so the deployment looks healthy while doing no
work, and `sky api start` refuses to start a fresh server. This watchdog
makes children exit when their parent dies so port/health checks stay
truthful.
"""

from collections.abc import Callable
import multiprocessing
import os
import threading
import time

from sky import sky_logging

logger = sky_logging.init_logger(__name__)

_POLL_INTERVAL_SECONDS = 1.0


def running_in_child_process(
    parent_process: Callable[[], object | None] = multiprocessing.parent_process
) -> bool:
    """Whether this process is a multiprocessing child of another process."""
    return parent_process() is not None


def _watch_parent(initial_ppid: int, poll_interval: float,
                  getppid: Callable[[], int], sleep: Callable[[float], None],
                  on_parent_death: Callable[[int], None]) -> None:
    while True:
        sleep(poll_interval)
        if getppid() != initial_ppid:
            # Parent died; this process was reparented to init/launchd.
            logger.info(f'Parent process (pid={initial_ppid}) died, exiting '
                        f'child process (pid={os.getpid()}).')
            on_parent_death(1)
            return


def start_parent_death_watchdog(
    on_parent_death: Callable[[int], None] | None = None,
    getppid: Callable[[], int] = os.getppid,
    sleep: Callable[[float], None] = time.sleep,
    poll_interval: float = _POLL_INTERVAL_SECONDS,
) -> threading.Thread:
    """Start a daemon thread that exits this process when its parent dies.

    Detection is by ppid change: when the parent dies, the child is
    reparented and os.getppid() no longer returns the pid captured at start.
    os._exit is used because the parent is already gone -- there is no one
    to coordinate a graceful shutdown with, and a deterministic exit is what
    releases the ports/health endpoints.

    Args:
        on_parent_death: Called with exit code 1 when the parent dies;
            defaults to os._exit. Injectable for tests.
        getppid: Injectable for tests; defaults to os.getppid.
        sleep: Injectable for tests; defaults to time.sleep.
        poll_interval: Seconds between ppid checks.

    Returns:
        The started daemon thread.
    """
    if on_parent_death is None:
        on_parent_death = os._exit  # pylint: disable=protected-access
    thread = threading.Thread(target=_watch_parent,
                              args=(getppid(), poll_interval, getppid, sleep,
                                    on_parent_death),
                              name='parent-death-watchdog',
                              daemon=True)
    thread.start()
    return thread
