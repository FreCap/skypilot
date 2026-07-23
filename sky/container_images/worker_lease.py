"""Shared durable lease heartbeat for provider-facing image workers."""

from __future__ import annotations

from collections.abc import Callable
import threading


class LeaseLostError(RuntimeError):
    """The worker no longer owns the durable provider-operation fence."""


class LeaseHeartbeat:
    """Renews one durable lease and exposes provider-call fencing."""

    def __init__(self, heartbeat: Callable[[], bool], interval: float) -> None:
        self.cancel_event = threading.Event()
        self._stop = threading.Event()
        self._heartbeat = heartbeat
        self._interval = interval
        self._lost = threading.Event()
        self._thread = threading.Thread(target=self._run,
                                        name='image-worker-lease-heartbeat',
                                        daemon=True)

    def __enter__(self) -> LeaseHeartbeat:
        self.assert_owned()
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 1)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                owned = self._heartbeat()
            except Exception:  # pylint: disable=broad-except
                owned = False
            if not owned:
                self._lost.set()
                self.cancel_event.set()
                return

    def assert_owned(self) -> None:
        """Renews synchronously before a provider call or state transition."""
        try:
            owned = not self._lost.is_set() and self._heartbeat()
        except Exception:  # pylint: disable=broad-except
            owned = False
        if not owned:
            self._lost.set()
            self.cancel_event.set()
            raise LeaseLostError('Container image work lease was lost.')
