"""Tests for timeline-aware locks."""

from unittest import mock

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
