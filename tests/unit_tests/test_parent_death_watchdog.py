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
import functools
import os
import threading

import pytest

from sky.server import watchdog
from sky.server.requests import executor


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


class _FakeThread:
    """Records Thread(...) construction/start without spawning a thread."""

    def __init__(self, *args, **kwargs):  # pylint: disable=unused-argument
        self.daemon = kwargs.get('daemon', False)
        self.started = False

    def start(self):
        self.started = True


@pytest.mark.parametrize('has_parent', [True, False])
@pytest.mark.parametrize(('server_role', 'expected_metrics_role'),
                         [('all', 'executor'), ('executor', 'executor'),
                          ('authority-worker', 'authority-worker')])
def test_executor_initializer_arms_watchdog_only_in_child(
        monkeypatch, has_parent, server_role, expected_metrics_role):
    """executor_initializer arms the watchdog iff a parent process exists.

    Executor pool children (including lazily-spawned burst workers) must die
    with the main API server process; otherwise an orphan keeps executing its
    current request and its late terminal writes race the next server boot's
    startup recovery. All side effects are faked: no real processes/threads.
    """
    armed = []
    # Exercise the real guard logic with a fake parent_process instead of
    # relying on multiprocessing.parent_process (bound at def-time).
    monkeypatch.setattr(
        watchdog, 'running_in_child_process',
        functools.partial(watchdog.running_in_child_process,
                          parent_process=lambda: object()
                          if has_parent else None))
    monkeypatch.setattr(watchdog, 'start_parent_death_watchdog',
                        lambda *args, **kwargs: armed.append(True))
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', server_role)

    # Stub the initializer's other side effects and record they still run.
    initialization_order = []
    metrics_roles = []

    def _set_metrics_role(role):
        metrics_roles.append(role)
        initialization_order.append('metrics-role')

    monkeypatch.setattr(executor.db_utils,
                        'set_postgres_connection_metrics_process_role',
                        _set_metrics_role)
    proctitles = []
    monkeypatch.setattr(executor.setproctitle, 'setproctitle',
                        proctitles.append)
    plugins_loaded = []

    def _load_plugins(context):
        assert os.environ['SKYPILOT_API_REQUEST_BACKEND'] == 'postgres'
        assert os.environ[
            'SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS'] == 'true'
        plugins_loaded.append(context)
        initialization_order.append('plugins')

    monkeypatch.setattr(executor.plugins, 'load_plugins', _load_plugins)
    monkeypatch.setattr(executor.metrics_lib,
                        'register_multiproc_cleanup_atexit', lambda: None)
    clean_envs = []
    clean_snapshot = {
        'FOO': 'BAR',
        'SKYPILOT_API_REQUEST_BACKEND': 'postgres',
        'SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS': 'true',
    }
    monkeypatch.setenv('SKYPILOT_API_REQUEST_BACKEND', 'sqlite')
    monkeypatch.setenv('SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS',
                       'false')

    def _restore_clean_server_env(environment):
        clean_envs.append(environment)
        os.environ['SKYPILOT_API_REQUEST_BACKEND'] = environment[
            'SKYPILOT_API_REQUEST_BACKEND']
        os.environ[
            'SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS'] = environment[
                'SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS']
        initialization_order.append('clean-env')

    monkeypatch.setattr(executor.clean_env_module, 'restore_clean_server_env',
                        _restore_clean_server_env)
    fake_threads = []

    def _fake_thread(*args, **kwargs):
        fake_thread = _FakeThread(*args, **kwargs)
        fake_threads.append(fake_thread)
        return fake_thread

    monkeypatch.setattr(executor.threading, 'Thread', _fake_thread)

    executor.executor_initializer('short', clean_env=clean_snapshot)

    assert armed == ([True] if has_parent else [])
    # Existing initializer behavior still runs regardless of the guard.
    assert metrics_roles == [expected_metrics_role]
    assert initialization_order == ['metrics-role', 'clean-env', 'plugins']
    assert len(proctitles) == 1
    assert len(plugins_loaded) == 1
    assert clean_envs == [clean_snapshot]
    assert len(fake_threads) == 1
    assert fake_threads[0].daemon and fake_threads[0].started
