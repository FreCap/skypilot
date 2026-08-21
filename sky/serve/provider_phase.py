"""Fair process-local admission for incompatible provider effect epochs.

Protocol-v2 Kubernetes mutations use immutable physical-cluster captures,
while legacy work deliberately uses ambient credentials.  Their concrete
provider-bearing effect epochs must not overlap in one process.  A capture may
remain active outside this phase during a passive Kubernetes observation: its
own context-local lease admits compatible v2 callers and rejects an unleased
ambient caller for that same context, while isolated clients for other
contexts remain independent.  This module provides the small concurrency
primitive used around the effect epochs; it does not perform provider
operations or own capture lifetimes.
"""
# pylint: disable=protected-access

from __future__ import annotations

import collections
import contextlib
import dataclasses
import enum
import math
import os
import threading
import time
import typing

from sky import exceptions

DEFAULT_PROVIDER_PHASE_TIMEOUT_SECONDS = 30.0


class ProviderPhaseMode(enum.Enum):
    """Mutually exclusive process-local provider authority modes."""

    V2_FENCED = 'v2-fenced'
    AMBIENT_LEGACY = 'ambient-legacy'


class ProviderPhaseAdmission:
    """Opaque capability allowing child threads to join an admitted round.

    The same object may be passed by reference to any number of child threads.
    It intentionally cannot be copied, deep-copied, or pickled.  Gate-side
    identity validation also rejects a manually forged copy.
    """

    __slots__ = ('_boot', '_gate', '_mode', '_phase_epoch', '_pid', '_sealed')

    _boot: object
    _gate: _ProviderPhaseGate
    _mode: ProviderPhaseMode
    _phase_epoch: int
    _pid: int
    _sealed: bool

    def __init__(self, gate: _ProviderPhaseGate, mode: ProviderPhaseMode,
                 pid: int, boot: object, phase_epoch: int) -> None:
        object.__setattr__(self, '_gate', gate)
        object.__setattr__(self, '_mode', mode)
        object.__setattr__(self, '_pid', pid)
        object.__setattr__(self, '_boot', boot)
        object.__setattr__(self, '_phase_epoch', phase_epoch)
        object.__setattr__(self, '_sealed', True)

    def __setattr__(self, name: str, value: typing.Any) -> typing.NoReturn:
        del name, value
        raise AttributeError('Provider phase admissions are immutable.')

    @property
    def mode(self) -> ProviderPhaseMode:
        """The authority mode carried by this admission."""
        return self._mode

    def __copy__(self) -> typing.NoReturn:
        raise TypeError('Provider phase admissions cannot be copied.')

    def __deepcopy__(self, memo: dict[int, typing.Any]) -> typing.NoReturn:
        del memo
        raise TypeError('Provider phase admissions cannot be copied.')

    def __reduce__(self) -> typing.NoReturn:
        raise TypeError('Provider phase admissions cannot be pickled.')

    def __reduce_ex__(self, protocol: typing.SupportsIndex) -> typing.NoReturn:
        del protocol
        raise TypeError('Provider phase admissions cannot be pickled.')


class _WaiterState(enum.Enum):
    PENDING = 'pending'
    GRANTED = 'granted'
    TIMED_OUT = 'timed-out'


@dataclasses.dataclass(eq=False)
class _PhaseWaiter:
    ticket: int
    mode: ProviderPhaseMode
    deadline: float
    state: _WaiterState = _WaiterState.PENDING
    admission: ProviderPhaseAdmission | None = None


@dataclasses.dataclass(eq=False)
class _AdmissionRecord:
    admission: ProviderPhaseAdmission
    root_open: bool = True
    child_users: int = 0


@dataclasses.dataclass
class _ThreadLease:
    admission: ProviderPhaseAdmission
    mode: ProviderPhaseMode
    phase_epoch: int
    is_root: bool
    depth: int = 1
    child_drain_timeout_seconds: float | None = None


class _ProviderPhaseGate:
    """FIFO cohort gate with explicitly transferable child admission."""

    def __init__(self, *, register_at_fork: bool = False) -> None:
        self._pid = os.getpid()
        self._reset_process_state()
        if register_at_fork and hasattr(os, 'register_at_fork'):
            os.register_at_fork(after_in_child=self._after_fork_in_child)

    def _reset_process_state(self) -> None:
        """Replace all synchronization state without touching an old lock."""
        # A lock may have been owned by a vanished parent thread at fork.  Do
        # not acquire it, and do not try to mutate the old condition in place.
        self._condition = threading.Condition(threading.Lock())
        self._queue: collections.deque[_PhaseWaiter] = collections.deque()
        self._active_mode: ProviderPhaseMode | None = None
        self._active_users = 0
        self._phase_epoch = 0
        self._next_ticket = 0
        self._boot = object()
        self._admissions: dict[int, _AdmissionRecord] = {}
        self._local = threading.local()

    def _after_fork_in_child(self) -> None:
        self._pid = os.getpid()
        self._reset_process_state()

    def _ensure_process(self) -> None:
        current_pid = os.getpid()
        if current_pid != self._pid:
            # Defensive fallback for a platform or embedding that did not run
            # register_at_fork.  This check happens before touching the old
            # condition, which may be permanently locked in the child.
            self._pid = current_pid
            self._reset_process_state()

    @staticmethod
    def _deadline(timeout_seconds: float) -> float:
        if (isinstance(timeout_seconds, bool) or
                not isinstance(timeout_seconds, (int, float)) or
                not math.isfinite(timeout_seconds) or timeout_seconds < 0):
            raise exceptions.ProviderPhaseMisuseError(
                'Provider phase timeout must be a finite nonnegative number.')
        return time.monotonic() + float(timeout_seconds)

    def _new_root_locked(self,
                         mode: ProviderPhaseMode) -> ProviderPhaseAdmission:
        if self._active_mode != mode or self._active_users < 0:
            raise RuntimeError('Provider phase gate is internally corrupt.')
        admission = ProviderPhaseAdmission(self, mode, self._pid, self._boot,
                                           self._phase_epoch)
        self._admissions[id(admission)] = _AdmissionRecord(admission)
        self._active_users += 1
        return admission

    def _start_phase_locked(self, mode: ProviderPhaseMode) -> None:
        if self._active_users != 0 or self._active_mode is not None:
            raise RuntimeError('Provider phase gate is internally corrupt.')
        self._phase_epoch += 1
        self._active_mode = mode

    def _advance_locked(self, now: float) -> None:
        """Admit the maximal live active-mode prefix at the FIFO queue head."""
        while True:
            # An expired waiter no longer owns a FIFO barrier, even while a
            # compatible cohort remains active. Without pruning here, an
            # opposite waiter that times out can strand compatible followers
            # until every original root retires.
            while self._queue and self._queue[0].deadline <= now:
                waiter = self._queue.popleft()
                waiter.state = _WaiterState.TIMED_OUT

            if self._active_users == 0:
                self._active_mode = None
                if not self._queue:
                    break
                self._start_phase_locked(self._queue[0].mode)

            cohort_mode = self._active_mode
            if cohort_mode is None:
                raise RuntimeError('Provider phase gate is internally corrupt.')
            if not self._queue or self._queue[0].mode != cohort_mode:
                break

            # Grant one live compatible root into the current epoch, then loop
            # so an expired barrier exposed behind it is removed before the
            # next FIFO decision. A live opposite head always stops admission.
            waiter = self._queue.popleft()
            waiter.admission = self._new_root_locked(cohort_mode)
            waiter.state = _WaiterState.GRANTED

        self._condition.notify_all()

    def _cancel_waiter_locked(self, waiter: _PhaseWaiter) -> None:
        if waiter.state == _WaiterState.PENDING:
            try:
                self._queue.remove(waiter)
            except ValueError as error:
                raise RuntimeError(
                    'Provider phase waiter disappeared from the queue.'
                ) from error
            waiter.state = _WaiterState.TIMED_OUT
        elif waiter.state == _WaiterState.GRANTED:
            admission = waiter.admission
            if admission is None:
                raise RuntimeError('Granted provider phase has no admission.')
            self._close_root_locked(admission)
        self._advance_locked(time.monotonic())
        self._condition.notify_all()

    def _acquire_root(self, mode: ProviderPhaseMode,
                      deadline: float) -> _ThreadLease:
        with self._condition:
            if not self._queue:
                if self._active_users == 0:
                    self._start_phase_locked(mode)
                if self._active_mode == mode:
                    admission = self._new_root_locked(mode)
                    return _ThreadLease(admission, mode, self._phase_epoch,
                                        True)

            waiter = _PhaseWaiter(self._next_ticket, mode, deadline)
            self._next_ticket += 1
            self._queue.append(waiter)
            self._condition.notify_all()
            if self._active_users == 0:
                self._advance_locked(time.monotonic())
            try:
                while waiter.state == _WaiterState.PENDING:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._cancel_waiter_locked(waiter)
                        raise exceptions.ProviderPhaseTimeoutError(
                            f'Timed out waiting for provider phase '
                            f'{mode.value!r}.')
                    self._condition.wait(timeout=remaining)
                if waiter.state == _WaiterState.TIMED_OUT:
                    raise exceptions.ProviderPhaseTimeoutError(
                        f'Timed out waiting for provider phase {mode.value!r}.')
                granted_admission = waiter.admission
                if granted_admission is None:
                    raise RuntimeError(
                        'Granted provider phase has no admission.')
                return _ThreadLease(granted_admission, mode, self._phase_epoch,
                                    True)
            except BaseException:
                if waiter.state != _WaiterState.TIMED_OUT:
                    self._cancel_waiter_locked(waiter)
                raise

    def _try_acquire_root(self, mode: ProviderPhaseMode) -> _ThreadLease:
        """Try once without queueing, sleeping, notifying, or barging."""
        with self._condition:
            if (self._queue or
                (self._active_users != 0 and self._active_mode != mode)):
                raise exceptions.ProviderPhaseBusyError(
                    f'Provider phase {mode.value!r} is busy.')
            if self._active_users == 0:
                self._start_phase_locked(mode)
            admission = self._new_root_locked(mode)
            return _ThreadLease(admission, mode, self._phase_epoch, True)

    def _identity_record_for_locked(
            self, admission: ProviderPhaseAdmission) -> _AdmissionRecord:
        if type(admission) is not ProviderPhaseAdmission:
            raise exceptions.ProviderPhaseMisuseError(
                'Provider child admission has an invalid type.')
        record = self._admissions.get(id(admission))
        if (record is None or record.admission is not admission or
                admission._gate is not self or admission._pid != self._pid or
                admission._boot is not self._boot):
            raise exceptions.ProviderPhaseMisuseError(
                'Provider child admission is stale, copied, or closed.')
        return record

    def _joinable_record_for_locked(
            self, admission: ProviderPhaseAdmission) -> _AdmissionRecord:
        record = self._identity_record_for_locked(admission)
        if (admission._phase_epoch != self._phase_epoch or
                admission._mode != self._active_mode or not record.root_open):
            raise exceptions.ProviderPhaseMisuseError(
                'Provider child admission is stale, copied, or closed.')
        return record

    def _join_child(self, admission: ProviderPhaseAdmission,
                    deadline: float) -> _ThreadLease:
        del deadline  # A valid active capability joins immediately.
        with self._condition:
            record = self._joinable_record_for_locked(admission)
            record.child_users += 1
            self._active_users += 1
            return _ThreadLease(admission, admission._mode,
                                admission._phase_epoch, False)

    def _close_root_locked(
            self,
            admission: ProviderPhaseAdmission,
            child_drain_timeout_seconds: float | None = None) -> None:
        record = self._joinable_record_for_locked(admission)
        record.root_open = False
        self._release_user_locked(admission)
        drain_deadline = (None if child_drain_timeout_seconds is None else
                          time.monotonic() + child_drain_timeout_seconds)
        try:
            while record.child_users:
                if drain_deadline is None:
                    self._condition.wait()
                    continue
                remaining = drain_deadline - time.monotonic()
                if remaining <= 0:
                    raise exceptions.ProviderPhaseTimeoutError(
                        'Timed out draining provider phase child users.')
                self._condition.wait(timeout=remaining)
        finally:
            # If an interrupted root stops waiting, the final child performs
            # this deletion.  Either way, closed admission can never be used
            # to prolong this or a later phase.
            if record.child_users == 0:
                self._admissions.pop(id(admission), None)

    def _release_child_locked(self, admission: ProviderPhaseAdmission) -> None:
        record = self._identity_record_for_locked(admission)
        if record.child_users <= 0:
            raise RuntimeError('Provider child count is internally corrupt.')
        record.child_users -= 1
        self._release_user_locked(admission)
        if record.child_users == 0 and not record.root_open:
            self._admissions.pop(id(admission), None)
        self._condition.notify_all()

    def _release_user_locked(self, admission: ProviderPhaseAdmission) -> None:
        if (self._active_users <= 0 or self._active_mode != admission._mode or
                self._phase_epoch != admission._phase_epoch or
                admission._pid != self._pid or
                admission._boot is not self._boot):
            raise RuntimeError('Provider phase gate is internally corrupt.')
        self._active_users -= 1
        if self._active_users == 0:
            self._active_mode = None
            self._advance_locked(time.monotonic())
        self._condition.notify_all()

    def _release(self, lease: _ThreadLease) -> None:
        with self._condition:
            if lease.is_root:
                self._close_root_locked(lease.admission,
                                        lease.child_drain_timeout_seconds)
            else:
                self._release_child_locked(lease.admission)

    def _current_lease(self) -> _ThreadLease | None:
        return getattr(self._local, 'lease', None)

    def _enter_reentrant(self, lease: _ThreadLease,
                         mode: ProviderPhaseMode) -> ProviderPhaseAdmission:
        if lease.mode != mode:
            raise exceptions.ProviderPhaseMisuseError(
                'A provider phase cannot change authority while active.')
        lease.depth += 1
        return lease.admission

    def _leave_reentrant(self, lease: _ThreadLease) -> None:
        if lease.depth <= 1:
            raise RuntimeError('Provider phase nesting is internally corrupt.')
        lease.depth -= 1

    @contextlib.contextmanager
    def phase(
        self,
        mode: ProviderPhaseMode,
        *,
        timeout_seconds: float = DEFAULT_PROVIDER_PHASE_TIMEOUT_SECONDS,
        child_drain_timeout_seconds: float | None = None,
    ) -> typing.Iterator[ProviderPhaseAdmission]:
        """Enter a fresh provider round or reenter this thread's round."""
        if not isinstance(mode, ProviderPhaseMode):
            raise exceptions.ProviderPhaseMisuseError(
                f'Invalid provider phase mode: {mode!r}.')
        self._ensure_process()
        deadline = self._deadline(timeout_seconds)
        if child_drain_timeout_seconds is not None:
            # Reuse the timeout validator without coupling the child-drain
            # window to the admission deadline.
            self._deadline(child_drain_timeout_seconds)
        current = self._current_lease()
        if current is not None:
            if child_drain_timeout_seconds is not None:
                raise exceptions.ProviderPhaseMisuseError(
                    'A reentrant provider phase cannot change its child-drain '
                    'timeout.')
            admission = self._enter_reentrant(current, mode)
            try:
                yield admission
            finally:
                self._leave_reentrant(current)
            return

        if timeout_seconds == 0:
            lease = self._try_acquire_root(mode)
        else:
            lease = self._acquire_root(mode, deadline)
        lease.child_drain_timeout_seconds = child_drain_timeout_seconds
        self._local.lease = lease
        try:
            yield lease.admission
        finally:
            # A child fork resets the gate and TLS before returning from
            # os.fork().  The inherited parent context may still unwind in the
            # child; it owns no child-process user and must not touch the new
            # registry.
            if (lease.admission._pid == self._pid and
                    lease.admission._boot is self._boot):
                del self._local.lease
                self._release(lease)

    @contextlib.contextmanager
    def join(
        self,
        admission: ProviderPhaseAdmission,
        *,
        timeout_seconds: float = DEFAULT_PROVIDER_PHASE_TIMEOUT_SECONDS,
    ) -> typing.Iterator[ProviderPhaseAdmission]:
        """Join an admitted root from an explicitly supplied child thread."""
        self._ensure_process()
        deadline = self._deadline(timeout_seconds)
        current = self._current_lease()
        if current is not None:
            if current.admission is not admission:
                raise exceptions.ProviderPhaseMisuseError(
                    'A thread cannot join a different provider admission '
                    'while a phase is active.')
            self._enter_reentrant(current, current.mode)
            try:
                yield current.admission
            finally:
                self._leave_reentrant(current)
            return

        lease = self._join_child(admission, deadline)
        self._local.lease = lease
        try:
            yield admission
        finally:
            if (lease.admission._pid == self._pid and
                    lease.admission._boot is self._boot):
                del self._local.lease
                self._release(lease)

    def try_phase(
        self,
        mode: ProviderPhaseMode,
        *,
        child_drain_timeout_seconds: float | None = None,
    ) -> contextlib.AbstractContextManager[ProviderPhaseAdmission]:
        """Try to enter immediately without joining or mutating the queue."""
        return self.phase(
            mode,
            timeout_seconds=0,
            child_drain_timeout_seconds=child_drain_timeout_seconds)


_PROVIDER_PHASE_GATE = _ProviderPhaseGate(register_at_fork=True)


def provider_phase(
    mode: ProviderPhaseMode,
    *,
    timeout_seconds: float = DEFAULT_PROVIDER_PHASE_TIMEOUT_SECONDS,
    child_drain_timeout_seconds: float | None = None,
) -> contextlib.AbstractContextManager[ProviderPhaseAdmission]:
    """Return a bounded context manager for one provider authority round."""
    return _PROVIDER_PHASE_GATE.phase(
        mode,
        timeout_seconds=timeout_seconds,
        child_drain_timeout_seconds=child_drain_timeout_seconds)


def join_provider_phase(
    admission: ProviderPhaseAdmission,
    *,
    timeout_seconds: float = DEFAULT_PROVIDER_PHASE_TIMEOUT_SECONDS,
) -> contextlib.AbstractContextManager[ProviderPhaseAdmission]:
    """Return a context manager explicitly admitting child-thread work."""
    return _PROVIDER_PHASE_GATE.join(admission, timeout_seconds=timeout_seconds)


def try_provider_phase(
    mode: ProviderPhaseMode,
    *,
    child_drain_timeout_seconds: float | None = None,
) -> contextlib.AbstractContextManager[ProviderPhaseAdmission]:
    """Try provider admission without queueing, sleeping, or barging."""
    return _PROVIDER_PHASE_GATE.try_phase(
        mode, child_drain_timeout_seconds=child_drain_timeout_seconds)
