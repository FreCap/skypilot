"""Tests for fair process-local provider phase admission."""
# pylint: disable=protected-access

import copy
import os
import pickle
import select
import signal
import threading
import time

import pytest

from sky import exceptions
from sky.serve import provider_phase

_V2 = provider_phase.ProviderPhaseMode.V2_FENCED
_LEGACY = provider_phase.ProviderPhaseMode.AMBIENT_LEGACY


def _wait_for_queue(gate: provider_phase._ProviderPhaseGate,
                    length: int) -> None:
    with gate._condition:
        assert gate._condition.wait_for(lambda: len(gate._queue) >= length,
                                        timeout=2)


def _join(thread: threading.Thread) -> None:
    thread.join(timeout=2)
    assert not thread.is_alive()


def test_same_mode_callers_overlap() -> None:
    gate = provider_phase._ProviderPhaseGate()
    entered = threading.Event()
    release = threading.Event()

    def same_mode() -> None:
        with gate.phase(_V2):
            entered.set()
            assert release.wait(timeout=2)

    with gate.phase(_V2):
        thread = threading.Thread(target=same_mode)
        thread.start()
        assert entered.wait(timeout=2)
        release.set()
    _join(thread)


def test_fifo_turnstile_prevents_same_mode_barging() -> None:
    gate = provider_phase._ProviderPhaseGate()
    legacy_entered = threading.Event()
    legacy_release = threading.Event()
    later_v2_entered = threading.Event()
    later_v2_release = threading.Event()

    def legacy() -> None:
        with gate.phase(_LEGACY):
            legacy_entered.set()
            assert legacy_release.wait(timeout=2)

    def later_v2() -> None:
        with gate.phase(_V2):
            later_v2_entered.set()
            assert later_v2_release.wait(timeout=2)

    with gate.phase(_V2):
        legacy_thread = threading.Thread(target=legacy)
        legacy_thread.start()
        _wait_for_queue(gate, 1)
        later_v2_thread = threading.Thread(target=later_v2)
        later_v2_thread.start()
        _wait_for_queue(gate, 2)
        next_ticket = gate._next_ticket
        try_busy = threading.Event()

        def try_v2() -> None:
            try:
                with gate.try_phase(_V2):
                    pass
            except exceptions.ProviderPhaseBusyError:
                try_busy.set()

        try_thread = threading.Thread(target=try_v2)
        try_thread.start()
        _join(try_thread)
        assert try_busy.is_set()
        assert gate._next_ticket == next_ticket
        assert not legacy_entered.is_set()
        assert not later_v2_entered.is_set()

    assert legacy_entered.wait(timeout=2)
    assert not later_v2_entered.is_set()
    legacy_release.set()
    assert later_v2_entered.wait(timeout=2)
    later_v2_release.set()
    _join(legacy_thread)
    _join(later_v2_thread)


def test_contiguous_same_mode_waiters_form_one_cohort() -> None:
    gate = provider_phase._ProviderPhaseGate()
    entered = [threading.Event(), threading.Event()]
    release = threading.Event()

    def legacy(index: int) -> None:
        with gate.phase(_LEGACY):
            entered[index].set()
            assert release.wait(timeout=2)

    with gate.phase(_V2):
        threads = [
            threading.Thread(target=legacy, args=(index,)) for index in range(2)
        ]
        for thread in threads:
            thread.start()
        _wait_for_queue(gate, 2)

    assert entered[0].wait(timeout=2)
    assert entered[1].wait(timeout=2)
    release.set()
    for thread in threads:
        _join(thread)


def test_explicit_child_joins_behind_opposite_waiter() -> None:
    gate = provider_phase._ProviderPhaseGate()
    child_entered = threading.Event()
    child_release = threading.Event()
    legacy_entered = threading.Event()

    def legacy() -> None:
        with gate.phase(_LEGACY):
            legacy_entered.set()

    def child(admission: provider_phase.ProviderPhaseAdmission) -> None:
        with gate.join(admission):
            child_entered.set()
            assert child_release.wait(timeout=2)

    with gate.phase(_V2) as admission:
        legacy_thread = threading.Thread(target=legacy)
        legacy_thread.start()
        _wait_for_queue(gate, 1)
        child_thread = threading.Thread(target=child, args=(admission,))
        child_thread.start()
        assert child_entered.wait(timeout=2)
        assert not legacy_entered.is_set()
        child_release.set()
        _join(child_thread)

    assert legacy_entered.wait(timeout=2)
    _join(legacy_thread)


def test_root_close_rejects_new_children_but_existing_child_drains() -> None:
    gate = provider_phase._ProviderPhaseGate()
    root_entered = threading.Event()
    root_leave_body = threading.Event()
    root_exited = threading.Event()
    child_entered = threading.Event()
    child_release = threading.Event()
    legacy_entered = threading.Event()
    admission_box: list[provider_phase.ProviderPhaseAdmission] = []

    def root() -> None:
        with gate.phase(_V2) as admission:
            admission_box.append(admission)
            root_entered.set()
            assert root_leave_body.wait(timeout=2)
        root_exited.set()

    def child(admission: provider_phase.ProviderPhaseAdmission) -> None:
        with gate.join(admission):
            child_entered.set()
            assert child_release.wait(timeout=2)

    root_thread = threading.Thread(target=root)
    root_thread.start()
    assert root_entered.wait(timeout=2)
    admission = admission_box[0]
    child_thread = threading.Thread(target=child, args=(admission,))
    child_thread.start()
    assert child_entered.wait(timeout=2)
    root_leave_body.set()
    with gate._condition:
        assert gate._condition.wait_for(
            lambda: not gate._admissions[id(admission)].root_open, timeout=2)
    assert not root_exited.is_set()

    with pytest.raises(exceptions.ProviderPhaseMisuseError):
        with gate.join(admission, timeout_seconds=0):
            pass

    def legacy() -> None:
        with gate.phase(_LEGACY):
            legacy_entered.set()

    legacy_thread = threading.Thread(target=legacy)
    legacy_thread.start()
    _wait_for_queue(gate, 1)
    assert not legacy_entered.is_set()
    child_release.set()
    assert legacy_entered.wait(timeout=2)
    assert root_exited.wait(timeout=2)
    _join(root_thread)
    _join(child_thread)
    _join(legacy_thread)


def test_root_child_drain_timeout_is_bounded_and_keeps_gate_closed() -> None:
    gate = provider_phase._ProviderPhaseGate()
    child_entered = threading.Event()
    child_release = threading.Event()
    child_thread: threading.Thread | None = None

    def child(admission: provider_phase.ProviderPhaseAdmission) -> None:
        with gate.join(admission):
            child_entered.set()
            assert child_release.wait(timeout=2)

    started = time.monotonic()
    with pytest.raises(exceptions.ProviderPhaseTimeoutError,
                       match='draining provider phase child users'):
        with gate.try_phase(_V2, child_drain_timeout_seconds=0.03) as admission:
            child_thread = threading.Thread(target=child, args=(admission,))
            child_thread.start()
            assert child_entered.wait(timeout=1)
    elapsed = time.monotonic() - started

    assert elapsed < 0.2
    # A non-cooperative child cannot make root exit wait forever, and it also
    # cannot allow the incompatible authority mode to overlap its live work.
    with pytest.raises(exceptions.ProviderPhaseBusyError):
        with gate.try_phase(_LEGACY):
            pass
    child_release.set()
    assert child_thread is not None
    _join(child_thread)
    with gate.try_phase(_LEGACY):
        pass


def test_same_mode_reentrancy_and_cross_mode_rejection() -> None:
    gate = provider_phase._ProviderPhaseGate()
    with gate.phase(_V2, timeout_seconds=0) as outer:
        with gate.phase(_V2, timeout_seconds=0) as nested:
            assert nested is outer
        with gate.join(outer, timeout_seconds=0) as joined:
            assert joined is outer
        with pytest.raises(exceptions.ProviderPhaseMisuseError):
            with gate.phase(_LEGACY, timeout_seconds=0):
                pass

    with gate.phase(_LEGACY, timeout_seconds=0):
        pass


def test_zero_time_busy_is_fail_closed_and_never_enqueues() -> None:
    gate = provider_phase._ProviderPhaseGate()
    holder_entered = threading.Event()
    holder_release = threading.Event()

    def holder() -> None:
        with gate.phase(_V2):
            holder_entered.set()
            assert holder_release.wait(timeout=2)

    holder_thread = threading.Thread(target=holder)
    holder_thread.start()
    assert holder_entered.wait(timeout=2)

    next_ticket = gate._next_ticket
    with pytest.raises(exceptions.ProviderPhaseBusyError):
        with gate.try_phase(_LEGACY):
            pass
    with gate._condition:
        assert not gate._queue
        assert gate._next_ticket == next_ticket
    # The timed-out opposite waiter no longer closes admission to the active
    # same-mode cohort.
    with gate.phase(_V2, timeout_seconds=0):
        pass

    holder_release.set()
    _join(holder_thread)
    with gate.phase(_LEGACY, timeout_seconds=0):
        pass


def test_positive_wait_timeout_is_typed_and_removes_waiter() -> None:
    gate = provider_phase._ProviderPhaseGate()
    holder_entered = threading.Event()
    holder_release = threading.Event()

    def holder() -> None:
        with gate.phase(_V2):
            holder_entered.set()
            assert holder_release.wait(timeout=2)

    holder_thread = threading.Thread(target=holder)
    holder_thread.start()
    assert holder_entered.wait(timeout=2)
    with pytest.raises(exceptions.ProviderPhaseTimeoutError):
        with gate.phase(_LEGACY, timeout_seconds=0.01):
            pass
    with gate._condition:
        assert not gate._queue
    holder_release.set()
    _join(holder_thread)


@pytest.mark.parametrize('timeout', [-1, float('inf'), float('nan'), True])
def test_invalid_timeout_is_typed_misuse(timeout: float) -> None:
    gate = provider_phase._ProviderPhaseGate()
    with pytest.raises(exceptions.ProviderPhaseMisuseError):
        with gate.phase(_V2, timeout_seconds=timeout):
            pass


def test_admission_rejects_copy_pickle_forgery_and_stale_reuse() -> None:
    gate = provider_phase._ProviderPhaseGate()
    with gate.phase(_V2) as admission:
        with pytest.raises(TypeError):
            copy.copy(admission)
        with pytest.raises(TypeError):
            copy.deepcopy(admission)
        with pytest.raises(TypeError):
            pickle.dumps(admission)
        with pytest.raises(AttributeError):
            admission._mode = _LEGACY

        forged = object.__new__(provider_phase.ProviderPhaseAdmission)
        for slot in provider_phase.ProviderPhaseAdmission.__slots__:
            object.__setattr__(forged, slot, getattr(admission, slot))
        with pytest.raises(exceptions.ProviderPhaseMisuseError):
            with gate.join(forged, timeout_seconds=0):
                pass

    # Even when a later phase has the same mode, the old exact object cannot
    # enter it.
    with gate.phase(_V2):
        with pytest.raises(exceptions.ProviderPhaseMisuseError):
            with gate.join(admission, timeout_seconds=0):
                pass


def test_exception_releases_phase_and_wakes_opposite_mode() -> None:
    gate = provider_phase._ProviderPhaseGate()

    class ExpectedError(Exception):
        pass

    with pytest.raises(ExpectedError):
        with gate.phase(_V2):
            raise ExpectedError
    with gate.phase(_LEGACY, timeout_seconds=0):
        pass


def test_expired_opposite_barrier_admits_queued_active_mode() -> None:
    gate = provider_phase._ProviderPhaseGate()
    legacy_timed_out = threading.Event()
    unexpected_legacy_entry = threading.Event()
    later_v2_entered = threading.Event()
    later_v2_release = threading.Event()
    live_legacy_entered = threading.Event()
    later_v2_epochs: list[int] = []
    threads: list[threading.Thread] = []

    def expiring_legacy() -> None:
        try:
            with gate.phase(_LEGACY, timeout_seconds=30):
                unexpected_legacy_entry.set()
        except exceptions.ProviderPhaseTimeoutError:
            legacy_timed_out.set()

    def later_v2() -> None:
        with gate.phase(_V2, timeout_seconds=30) as admission:
            later_v2_epochs.append(admission._phase_epoch)
            later_v2_entered.set()
            assert later_v2_release.wait(timeout=5)

    def live_legacy() -> None:
        with gate.phase(_LEGACY, timeout_seconds=30):
            live_legacy_entered.set()

    try:
        with gate.phase(_V2) as first_v2:
            expiring_thread = threading.Thread(target=expiring_legacy)
            threads.append(expiring_thread)
            expiring_thread.start()
            _wait_for_queue(gate, 1)

            later_v2_thread = threading.Thread(target=later_v2)
            threads.append(later_v2_thread)
            later_v2_thread.start()
            _wait_for_queue(gate, 2)

            with gate._condition:
                assert gate._queue[0].mode == _LEGACY
                gate._queue[0].deadline = time.monotonic() - 1
                gate._advance_locked(time.monotonic())

            assert legacy_timed_out.wait(timeout=2)
            # The compatible follower must join while the original root is
            # still open, reusing that exact phase epoch.
            assert later_v2_entered.wait(timeout=2)
            assert later_v2_epochs == [first_v2._phase_epoch]

            live_legacy_thread = threading.Thread(target=live_legacy)
            threads.append(live_legacy_thread)
            live_legacy_thread.start()
            _wait_for_queue(gate, 1)
            assert not live_legacy_entered.is_set()

        # One V2 root remains, so the live opposite barrier stays excluded.
        assert not live_legacy_entered.is_set()
        later_v2_release.set()
        assert live_legacy_entered.wait(timeout=2)
        assert not unexpected_legacy_entry.is_set()
    finally:
        later_v2_release.set()
        for thread in threads:
            _join(thread)


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='requires os.fork')
def test_actual_fork_replaces_locked_parent_state_and_rejects_token() -> None:
    gate = provider_phase._ProviderPhaseGate(register_at_fork=True)
    parent_locked = threading.Event()
    parent_release = threading.Event()
    admission_box: list[provider_phase.ProviderPhaseAdmission] = []

    def parent_holder() -> None:
        with gate.phase(_V2) as admission:
            admission_box.append(admission)
            # Deliberately fork while this lock is held by a thread that will
            # not exist in the child.  Reusing the inherited Condition would
            # deadlock before a timeout could be evaluated.
            with gate._condition:
                parent_locked.set()
                assert parent_release.wait(timeout=10)

    holder_thread = threading.Thread(target=parent_holder)
    holder_thread.start()
    assert parent_locked.wait(timeout=2)

    read_fd, write_fd = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        os.close(read_fd)
        result = b'failure'
        try:
            try:
                with gate.join(admission_box[0], timeout_seconds=0):
                    pass
            except exceptions.ProviderPhaseMisuseError:
                with gate.phase(_LEGACY, timeout_seconds=0):
                    pass
                result = b'ok'
        except BaseException:  # pylint: disable=broad-except
            pass
        os.write(write_fd, result)
        os.close(write_fd)
        os._exit(0)

    os.close(write_fd)
    try:
        readable, _, _ = select.select([read_fd], [], [], 5)
        if not readable:
            os.kill(child_pid, signal.SIGKILL)
            pytest.fail('forked child deadlocked on inherited provider gate')
        assert os.read(read_fd, 16) == b'ok'
    finally:
        os.close(read_fd)
        parent_release.set()
        _join(holder_thread)
        _, status = os.waitpid(child_pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
