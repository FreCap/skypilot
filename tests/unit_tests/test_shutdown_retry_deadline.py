"""Regression test: the graceful-shutdown retry sweep must run before SIGKILL.

The only path that flags a non-retriable request (sky.exec / sky.jobs.launch
/ ...) with ``should_retry`` is the timeout branch of
``_wait_requests``, which interrupts the whole snapshot. The Helm chart wires
``terminationGracePeriodSeconds`` (the k8s SIGKILL deadline) to the same value
as ``_WAIT_REQUESTS_TIMEOUT_SECONDS``. The old code started its timeout clock at
``_wait_requests`` entry (after ~6s of grace sleeps + the coordinator lock) and
back-dated only 5s, so the sweep fired at ~T+timeout+1s -- after SIGKILL --
and those requests were silently dropped on restart.

The fix budgets the sweep against the true SIGTERM arrival time (monotonic,
captured in ``handle_exit``) minus a margin, so it runs before SIGKILL. These
tests pin that (a) the sweep fires once the budgeted deadline passes, and
(b) non-retriable requests are not interrupted before it.
"""
# pylint: disable=protected-access
import os
import signal
import subprocess
import sys
import unittest.mock as mock

from sky.server import uvicorn as uvicorn_module


def test_execution_drain_budget_precedes_legacy_full_grace():
    env = dict(os.environ)
    env['SKYPILOT_EXECUTION_DRAIN_SECONDS'] = '30'
    env['SKYPILOT_GRACE_PERIOD_SECONDS'] = '60'
    result = subprocess.run([
        sys.executable, '-c', 'from sky.server import uvicorn; '
        'print(uvicorn._WAIT_REQUESTS_TIMEOUT_SECONDS)'
    ],
                            env=env,
                            check=True,
                            capture_output=True,
                            text=True)

    assert result.stdout.splitlines()[-1] == '30'


def _server_at_shutdown(monkeypatch, *, now):
    server = uvicorn_module.Server.__new__(uvicorn_module.Server)
    # SIGTERM arrived at monotonic T=100; deadline = 100 + (timeout - margin).
    server._shutdown_started_at = 100.0
    monkeypatch.setattr(uvicorn_module.time, 'monotonic', lambda: now)
    slept = []
    monkeypatch.setattr(uvicorn_module.time, 'sleep', slept.append)
    interrupted = []
    monkeypatch.setattr(server, 'interrupt_request_for_retry',
                        interrupted.append)
    return server, slept, interrupted


def _patch_backend(monkeypatch, side_effect):
    backend = mock.Mock()
    backend.get_shutdown_active_requests.side_effect = side_effect
    monkeypatch.setattr(uvicorn_module.request_storage, 'get_request_backend',
                        lambda: backend)


def test_sweep_fires_once_budgeted_deadline_passes(monkeypatch):
    # deadline = 100 + (60 - 10) = 150; now = 151 is past it.
    server, slept, interrupted = _server_at_shutdown(monkeypatch, now=151.0)
    _patch_backend(monkeypatch, side_effect=lambda: [('req-exec', 'sky.exec')])

    server._wait_requests()

    # The non-retriable launch request is interrupted-for-retry by the sweep,
    # and we do NOT keep sleeping past the SIGKILL deadline.
    assert interrupted == ['req-exec']
    assert not slept


def test_handle_exit_captures_sigterm_arrival(monkeypatch):
    # The deadline is only meaningful if handle_exit records the true SIGTERM
    # time. Pin that the first signal stamps `_shutdown_started_at` (monotonic)
    # -- pre-fix code never sets it, so this fails on the old version.
    server = uvicorn_module.Server.__new__(uvicorn_module.Server)
    server.exiting = False
    server.should_exit = False
    server._shutdown_started_at = None
    # Don't actually spawn the graceful-shutdown thread.
    monkeypatch.setattr(uvicorn_module.threading, 'Thread', mock.Mock())
    monkeypatch.setattr(uvicorn_module.time, 'monotonic', lambda: 4242.0)

    server.handle_exit(signal.SIGTERM, None)

    assert server._shutdown_started_at == 4242.0
    # A second signal must not move the recorded start (the budget stays
    # anchored to the first SIGTERM).
    monkeypatch.setattr(uvicorn_module.time, 'monotonic', lambda: 9999.0)
    server.handle_exit(signal.SIGTERM, None)
    assert server._shutdown_started_at == 4242.0


def test_non_retriable_not_interrupted_before_deadline(monkeypatch):
    # now = 120 is before the deadline (150): the per-iteration branch must
    # leave the non-retriable request alone (wait for it), not retry-cancel it.
    server, slept, interrupted = _server_at_shutdown(monkeypatch, now=120.0)
    _patch_backend(monkeypatch, side_effect=[[('req-exec', 'sky.exec')], []])

    server._wait_requests()

    assert not interrupted  # not swept before the deadline
    assert slept == [uvicorn_module._WAIT_REQUESTS_INTERVAL_SECONDS]


def test_replayable_launch_neither_blocks_shutdown_nor_interrupted(monkeypatch):
    # A RUNNING launch (a replayable request) must not hold shutdown open --
    # provisioning outlives any realistic grace -- and must not be flagged
    # should_retry: its row is left RUNNING so startup recovery requeues and
    # re-executes it (sky/server/requests/requests.py,
    # _find_interrupted_launches_to_requeue).
    launch_name = uvicorn_module.requests_lib.REPLAYABLE_REQUEST_NAMES[0]
    # now = 101 is well before the deadline (150): the early exit must come
    # from the replayable filter, not the timeout sweep.
    server, slept, interrupted = _server_at_shutdown(monkeypatch, now=101.0)
    _patch_backend(monkeypatch,
                   side_effect=lambda: [('req-launch', launch_name)])

    server._wait_requests()

    assert not interrupted
    assert not slept


def test_replayable_filter_leaves_other_requests_to_the_sweep(monkeypatch):
    # Past the deadline, non-replayable requests are still interrupted for
    # client retry; the replayable launch alone is exempt.
    launch_name = uvicorn_module.requests_lib.REPLAYABLE_REQUEST_NAMES[0]
    server, slept, interrupted = _server_at_shutdown(monkeypatch, now=151.0)
    _patch_backend(monkeypatch,
                   side_effect=lambda: [('req-launch', launch_name),
                                        ('req-exec', 'sky.exec')])

    server._wait_requests()

    assert interrupted == ['req-exec']
    assert not slept


def test_api_only_shutdown_does_not_touch_executor_owned_requests(monkeypatch):
    server, slept, interrupted = _server_at_shutdown(monkeypatch, now=151.0)
    backend = mock.Mock()
    monkeypatch.setattr(uvicorn_module.request_storage, 'get_request_backend',
                        lambda: backend)
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'api')

    server._wait_requests()

    backend.get_shutdown_active_requests.assert_not_called()
    assert not interrupted
    assert not slept
