"""Tests for the API server parent-death watchdog.

The main API server process runs the request dispatchers as daemon threads,
while the queue manager and uvicorn workers are separate child processes. If
the main process dies abruptly (kill -9, OOM), the orphaned children keep the
API port bound and health checks green while nothing dequeues work, and
`sky api start` refuses to start a fresh server. The watchdog makes each child
exit when its parent dies; these tests pin the detection loop and the
arm/no-arm guard with injected fakes (no real sleeping, no real processes).
"""
# pylint: disable=protected-access
import threading

import pytest

from sky.server import watchdog


class _StopLoop(Exception):
    """Raised by the fake sleep to break out of the watchdog loop."""


def _fake_sleep(max_iterations: int):
    """Returns a sleep fake that raises after max_iterations calls."""
    calls = []

    def sleep(seconds: float) -> None:  # pylint: disable=unused-argument
        calls.append(None)
        if len(calls) > max_iterations:
            raise _StopLoop()

    return sleep


def test_stable_ppid_never_exits():
    exits = []

    with pytest.raises(_StopLoop):
        watchdog._watch_parent(initial_ppid=42,
                               poll_interval=0.0,
                               getppid=lambda: 42,
                               sleep=_fake_sleep(50),
                               on_parent_death=exits.append)

    assert not exits


def test_ppid_change_exits_exactly_once_with_code_1():
    ppids = iter([42, 42, 42, 1])
    exits = []

    watchdog._watch_parent(initial_ppid=42,
                           poll_interval=0.0,
                           getppid=lambda: next(ppids),
                           sleep=_fake_sleep(50),
                           on_parent_death=exits.append)

    assert exits == [1]


def test_immediate_ppid_change_exits():
    exits = []

    watchdog._watch_parent(initial_ppid=42,
                           poll_interval=0.0,
                           getppid=lambda: 1,
                           sleep=_fake_sleep(50),
                           on_parent_death=exits.append)

    assert exits == [1]


def test_watchdog_thread_calls_exit_on_parent_death():
    """End-to-end through start_parent_death_watchdog (real thread)."""
    exited = threading.Event()
    exits = []

    def on_parent_death(code: int) -> None:
        exits.append(code)
        exited.set()

    ppids = iter([7, 7, 1])
    thread = watchdog.start_parent_death_watchdog(
        on_parent_death=on_parent_death,
        getppid=lambda: next(ppids, 1),
        sleep=lambda _: None,
        poll_interval=0.0)

    assert exited.wait(timeout=5)
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert exits == [1]


def test_guard_arms_only_in_child_process():
    assert watchdog.running_in_child_process(parent_process=lambda: object()  # pylint: disable=unnecessary-lambda
                                            ) is True
    assert watchdog.running_in_child_process(
        parent_process=lambda: None) is False
