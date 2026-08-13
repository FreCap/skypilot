"""Tests for lost-wakeup-free SkyServe scale reconciliation."""

import threading
import time

from sky.serve import scale_reconciliation


def _start(coordinator: scale_reconciliation.ScaleReconcileCoordinator):
    errors = []

    def _run() -> None:
        try:
            coordinator.run()
        except BaseException as error:  # pylint: disable=broad-except
            errors.append(error)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread, errors


def _stop_and_join(coordinator: scale_reconciliation.ScaleReconcileCoordinator,
                   thread: threading.Thread,
                   errors: list[BaseException]) -> None:
    coordinator.stop()
    thread.join(timeout=1)
    assert not thread.is_alive()
    assert not errors


def test_notification_before_wait_is_retained() -> None:
    reconciled = threading.Event()
    generations = []

    def _reconcile(generation: int) -> None:
        generations.append(generation)
        reconciled.set()

    coordinator = scale_reconciliation.ScaleReconcileCoordinator(
        _reconcile, max_idle_seconds=60)
    assert coordinator.notify() == 1

    thread, errors = _start(coordinator)
    try:
        assert reconciled.wait(timeout=1)
        assert generations == [1]
    finally:
        _stop_and_join(coordinator, thread, errors)


def test_notification_during_reconcile_skips_wait() -> None:
    first_started = threading.Event()
    release_first = threading.Event()
    second_finished = threading.Event()
    generations = []

    def _reconcile(generation: int) -> None:
        generations.append(generation)
        if len(generations) == 1:
            first_started.set()
            assert release_first.wait(timeout=1)
        else:
            second_finished.set()

    coordinator = scale_reconciliation.ScaleReconcileCoordinator(
        _reconcile, max_idle_seconds=60)
    thread, errors = _start(coordinator)
    try:
        assert first_started.wait(timeout=1)
        assert coordinator.notify() == 1
        release_first.set()
        assert second_finished.wait(timeout=1)
        assert generations == [0, 1]
    finally:
        release_first.set()
        _stop_and_join(coordinator, thread, errors)


def test_duplicate_notifications_coalesce() -> None:
    first_started = threading.Event()
    release_first = threading.Event()
    second_finished = threading.Event()
    generations = []

    def _reconcile(generation: int) -> None:
        generations.append(generation)
        if len(generations) == 1:
            first_started.set()
            assert release_first.wait(timeout=1)
        else:
            second_finished.set()

    coordinator = scale_reconciliation.ScaleReconcileCoordinator(
        _reconcile, max_idle_seconds=60)
    thread, errors = _start(coordinator)
    try:
        assert first_started.wait(timeout=1)
        assert [coordinator.notify() for _ in range(3)] == [1, 2, 3]
        release_first.set()
        assert second_finished.wait(timeout=1)
        time.sleep(0.05)
        assert generations == [0, 3]
    finally:
        release_first.set()
        _stop_and_join(coordinator, thread, errors)


def test_stop_wakes_waiter_and_late_notify_is_noop() -> None:
    reconciled = threading.Event()
    generations = []

    def _reconcile(generation: int) -> None:
        generations.append(generation)
        reconciled.set()

    coordinator = scale_reconciliation.ScaleReconcileCoordinator(
        _reconcile, max_idle_seconds=60)
    thread, errors = _start(coordinator)

    assert reconciled.wait(timeout=1)
    coordinator.stop()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert not errors
    assert coordinator.stopped
    assert coordinator.notify() == 0
    assert coordinator.generation == 0
    assert generations == [0]


def test_durable_generation_recovers_lost_notification() -> None:
    durable_lock = threading.Lock()
    durable_generation = 0
    first_finished = threading.Event()
    recovered = threading.Event()
    generations = []

    def _read_durable_generation() -> int:
        with durable_lock:
            return durable_generation

    def _reconcile(generation: int) -> None:
        generations.append(generation)
        if len(generations) == 1:
            first_finished.set()
        else:
            recovered.set()

    coordinator = scale_reconciliation.ScaleReconcileCoordinator(
        _reconcile,
        durable_generation_reader=_read_durable_generation,
        max_idle_seconds=0.02)
    thread, errors = _start(coordinator)
    try:
        assert first_finished.wait(timeout=1)
        with durable_lock:
            durable_generation = 1
        assert recovered.wait(timeout=1)
        time.sleep(0.05)
        assert generations == [0, 1]
        assert coordinator.generation == 1
    finally:
        _stop_and_join(coordinator, thread, errors)
