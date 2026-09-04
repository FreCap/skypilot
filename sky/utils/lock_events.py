"""Lock events."""

import functools
import os
import threading

import filelock

from sky.utils import locks
from sky.utils import timeline


class DistributedLockEvent:
    """Serve both as a distributed lock and event for the lock."""

    def __init__(self, lock_id: str, timeout: float | None = None):
        self._lock_id = lock_id
        self._lock = locks.get_lock(lock_id, timeout)
        self._hold_lock_event = timeline.Event(
            f'[DistributedLock.hold]:{lock_id}')

    def acquire(self):
        was_locked = self._lock.is_locked()
        with timeline.Event(f'[DistributedLock.acquire]:{self._lock_id}'):
            self._lock.acquire()
        if not was_locked and self._lock.is_locked():
            # start holding the lock after initial acquiring
            self._hold_lock_event.begin()

    def release(self):
        was_locked = self._lock.is_locked()
        self._lock.release()
        if was_locked and not self._lock.is_locked():
            # stop holding the lock after initial releasing
            self._hold_lock_event.end()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def __call__(self, f):

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            with self:
                return f(*args, **kwargs)

        return wrapper


class FileLockEvent:
    """Serve both as a file lock and event for the lock."""

    def __init__(self, lockfile: str | os.PathLike, timeout: float = -1):
        self._lockfile = lockfile
        self._timeout = timeout
        os.makedirs(os.path.dirname(os.path.abspath(self._lockfile)),
                    exist_ok=True)
        # A FileLock records the PID that constructed it and deliberately
        # rejects use by a forked child.  FileLockEvent is also used as an
        # import-time decorator, so constructing its lock here would make
        # every executor child inherit an unusable instance.  Keep one
        # reentrant lock per thread and rebuild it whenever the PID changes.
        self._thread_local = threading.local()
        self._hold_lock_event = timeline.Event(
            f'[FileLock.hold]:{self._lockfile}')

    def _lock(self) -> filelock.BaseFileLock:
        pid = os.getpid()
        lock = getattr(self._thread_local, 'lock', None)
        if (getattr(self._thread_local, 'pid', None) != pid or
                not isinstance(lock, filelock.BaseFileLock)):
            lock = filelock.FileLock(self._lockfile, self._timeout)
            self._thread_local.pid = pid
            self._thread_local.lock = lock
        return lock

    def acquire(self):
        lock = self._lock()
        was_locked = lock.is_locked
        with timeline.Event(f'[FileLock.acquire]:{self._lockfile}'):
            lock.acquire()
        if not was_locked and lock.is_locked:
            # start holding the lock after initial acquiring
            self._hold_lock_event.begin()

    def release(self):
        lock = self._lock()
        was_locked = lock.is_locked
        lock.release()
        if was_locked and not lock.is_locked:
            # stop holding the lock after initial releasing
            self._hold_lock_event.end()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def __call__(self, f):
        # Make this class callable as a decorator.
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            with self:
                return f(*args, **kwargs)

        return wrapper
