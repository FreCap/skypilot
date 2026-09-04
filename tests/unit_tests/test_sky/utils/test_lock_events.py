"""Tests for timeline-aware locks."""

import os
import select
from unittest import mock

import pytest

from sky.utils import lock_events


class _ReentrantLock:
    """Minimal reentrant lock implementing the distributed lock surface."""

    def __init__(self) -> None:
        self._depth = 0

    def acquire(self) -> None:
        self._depth += 1

    def release(self) -> None:
        self._depth -= 1

    def is_locked(self) -> bool:
        return self._depth > 0


def test_distributed_lock_event_tracks_outermost_hold(monkeypatch) -> None:
    lock = _ReentrantLock()
    monkeypatch.setattr(lock_events.locks, 'get_lock',
                        lambda *_args, **_kwargs: lock)

    event = lock_events.DistributedLockEvent('test-lock')
    hold_event = mock.Mock()
    monkeypatch.setattr(event, '_hold_lock_event', hold_event)

    event.acquire()
    event.acquire()
    hold_event.begin.assert_called_once_with()

    event.release()
    hold_event.end.assert_not_called()

    event.release()
    hold_event.end.assert_called_once_with()


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='requires POSIX fork')
def test_file_lock_event_decorator_is_safe_after_fork(tmp_path) -> None:
    """An import-time decorator must not carry its FileLock into a child."""
    event = lock_events.FileLockEvent(tmp_path / 'fork-safe.lock')

    @event
    def decorated() -> None:
        return None

    decorated()
    read_fd, write_fd = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        os.close(read_fd)
        try:
            decorated()
        except BaseException as error:  # pylint: disable=broad-exception-caught
            result = f'{type(error).__name__}: {error}'.encode()
        else:
            result = b'ok'
        os.write(write_fd, result)
        os.close(write_fd)
        os._exit(0)  # pylint: disable=protected-access

    os.close(write_fd)
    try:
        result = os.read(read_fd, 4096)
    finally:
        os.close(read_fd)
    _, wait_status = os.waitpid(child_pid, 0)

    assert os.waitstatus_to_exitcode(wait_status) == 0
    assert result == b'ok'


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='requires POSIX fork')
def test_file_lock_event_preserves_exclusion_across_fork(tmp_path) -> None:
    """A child-local lock must still wait for the parent's OS-file lock."""
    event = lock_events.FileLockEvent(tmp_path / 'fork-exclusion.lock')
    ready_read, ready_write = os.pipe()
    result_read, result_write = os.pipe()
    event.acquire()
    child_pid = os.fork()
    if child_pid == 0:
        os.close(ready_read)
        os.close(result_read)
        os.write(ready_write, b'ready')
        os.close(ready_write)
        try:
            with event:
                result = b'acquired'
        except BaseException as error:  # pylint: disable=broad-exception-caught
            result = f'{type(error).__name__}: {error}'.encode()
        os.write(result_write, result)
        os.close(result_write)
        os._exit(0)  # pylint: disable=protected-access

    os.close(ready_write)
    os.close(result_write)
    assert os.read(ready_read, 5) == b'ready'
    os.close(ready_read)
    readable_while_parent_holds, _, _ = select.select([result_read], [], [], 1)
    event.release()
    readable_after_release, _, _ = select.select([result_read], [], [], 5)
    result = os.read(result_read, 4096)
    os.close(result_read)
    _, wait_status = os.waitpid(child_pid, 0)

    assert readable_while_parent_holds == []
    assert readable_after_release == [result_read]
    assert os.waitstatus_to_exitcode(wait_status) == 0
    assert result == b'acquired'
