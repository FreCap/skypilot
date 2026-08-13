"""Lost-wakeup-free coordination for SkyServe scale reconciliation.

This module owns trigger coalescing, not launch authority.  Publishers advance
one in-process generation and wake the consumer.  The consumer reconciles from
durable state without holding the condition lock, then compares generations
before waiting.  A bounded durable-generation reader recovers notifications
that were lost across process or PostgreSQL notification boundaries.
"""

from __future__ import annotations

import collections.abc
import math
import threading
import time

DEFAULT_MAX_IDLE_SECONDS = 5.0

ReconcileCallback = collections.abc.Callable[[int], None]
DurableGenerationReader = collections.abc.Callable[[], collections.abc.Hashable]


class ScaleReconcileCoordinator:
    """Coalesce scale triggers behind one monotonic generation.

    ``reconcile_once`` and ``durable_generation_reader`` always run without
    the condition lock.  The durable reader must return an immutable,
    equality-stable snapshot covering every durable publication generation
    relevant to the owner.  For multiple inputs, an ordered tuple of their
    monotonic generations is suitable.  A changed snapshot is only a wakeup
    hint; reconciliation must reread and validate authoritative state.

    One thread calls :meth:`run`.  Any publisher thread may call
    :meth:`notify`.  Repeated notifications that arrive during one
    reconciliation advance the generation but coalesce into one subsequent
    reconciliation.
    """

    def __init__(
        self,
        reconcile_once: ReconcileCallback,
        *,
        durable_generation_reader: DurableGenerationReader | None = None,
        max_idle_seconds: float = DEFAULT_MAX_IDLE_SECONDS,
    ) -> None:
        if not callable(reconcile_once):
            raise TypeError('reconcile_once must be callable.')
        if (durable_generation_reader is not None and
                not callable(durable_generation_reader)):
            raise TypeError('durable_generation_reader must be callable.')
        if (isinstance(max_idle_seconds, bool) or
                not isinstance(max_idle_seconds, (int, float)) or
                not math.isfinite(max_idle_seconds) or max_idle_seconds <= 0):
            raise ValueError(
                'max_idle_seconds must be a finite positive number.')

        self._reconcile_once = reconcile_once
        self._durable_generation_reader = durable_generation_reader
        self._max_idle_seconds = float(max_idle_seconds)
        self._condition = threading.Condition(threading.Lock())
        self._generation = 0
        self._stop_requested = False
        self._running = False

    @property
    def generation(self) -> int:
        """Return the latest in-process notification generation."""
        with self._condition:
            return self._generation

    @property
    def stopped(self) -> bool:
        """Whether permanent coordinator shutdown has been requested."""
        with self._condition:
            return self._stop_requested

    def notify(self) -> int:
        """Publish a reconciliation hint and return its generation.

        Notification after shutdown is a no-op so teardown races cannot
        resurrect the consumer.
        """
        with self._condition:
            if self._stop_requested:
                return self._generation
            self._generation += 1
            self._condition.notify_all()
            return self._generation

    def stop(self) -> None:
        """Permanently stop the consumer and wake any condition wait."""
        with self._condition:
            self._stop_requested = True
            self._condition.notify_all()

    def run(self) -> None:
        """Reconcile until :meth:`stop` is called.

        The initial generation zero is reconciled immediately.  Exceptions
        from either callback propagate to the caller so the controller's
        thread supervisor can apply its ordinary failure policy.
        """
        with self._condition:
            if self._running:
                raise RuntimeError(
                    'ScaleReconcileCoordinator already has a consumer.')
            if self._stop_requested:
                return
            self._running = True

        try:
            self._run_loop()
        finally:
            with self._condition:
                self._running = False
                self._condition.notify_all()

    def _run_loop(self) -> None:
        durable_generation: collections.abc.Hashable | None = None
        while True:
            with self._condition:
                if self._stop_requested:
                    return

            # Sampling before reconciliation means any durable commit after
            # this point remains detectable by the maximum-idle reread.  The
            # callback performs no authority-bearing work and must be bounded.
            if self._durable_generation_reader is not None:
                durable_generation = self._durable_generation_reader()

            with self._condition:
                if self._stop_requested:
                    return
                reconcile_generation = self._generation

            self._reconcile_once(reconcile_generation)

            recovery_deadline = time.monotonic() + self._max_idle_seconds
            while True:
                with self._condition:
                    if self._stop_requested:
                        return
                    # Compare before waiting.  A publisher that raced with
                    # slow reconciliation cannot be hidden by a later wait.
                    if self._generation != reconcile_generation:
                        break

                    if self._durable_generation_reader is None:
                        self._condition.wait()
                        continue

                    remaining = recovery_deadline - time.monotonic()
                    if remaining > 0:
                        self._condition.wait(timeout=remaining)
                        continue

                # Durable I/O must never hold the in-process condition lock.
                recovered_generation = self._durable_generation_reader()
                with self._condition:
                    if self._stop_requested:
                        return
                    if self._generation != reconcile_generation:
                        break
                    if recovered_generation != durable_generation:
                        self._generation += 1
                        self._condition.notify_all()
                        break

                # An unchanged recovery read establishes the next maximum-idle
                # deadline.  Spurious condition wakes do not reset it.
                recovery_deadline = time.monotonic() + self._max_idle_seconds
