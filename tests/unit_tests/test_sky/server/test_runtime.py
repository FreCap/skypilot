"""Role-isolation tests for the API server process supervisors."""

from types import SimpleNamespace
from unittest import mock

import pytest

from sky.jobs import managed_job_refresh_thread
from sky.jobs.server import server as jobs_server
from sky.server import runtime


class _BackgroundLoop:

    def __init__(self) -> None:
        self.stopped = False

    def run(self, coroutine) -> None:
        coroutine.close()

    def stop(self) -> None:
        self.stopped = True


def _args() -> SimpleNamespace:
    return SimpleNamespace(host='127.0.0.1',
                           metrics_port=9090,
                           role_health_port=46581)


def test_api_role_starts_only_public_server(monkeypatch):
    background = _BackgroundLoop()
    lease = mock.Mock()
    config = mock.Mock()
    state = runtime.RuntimeState('api', config, lease, False)
    start_workers = mock.Mock()
    run_uvicorn = mock.Mock()
    monkeypatch.setattr(runtime, '_start_background_loop',
                        lambda *args: background)
    monkeypatch.setattr(runtime.executor, 'start', start_workers)
    monkeypatch.setattr(runtime, '_run_uvicorn', run_uvicorn)
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    runtime.run_role(state, _args())

    run_uvicorn.assert_called_once_with(state, mock.ANY)
    start_workers.assert_not_called()
    lease.stop.assert_called_once_with()
    assert background.stopped


def test_executor_role_starts_workers_without_public_server(monkeypatch):
    background = _BackgroundLoop()
    lease = mock.Mock()
    config = mock.Mock()
    queue_server = mock.Mock()
    worker = mock.Mock()
    state = runtime.RuntimeState('executor', config, lease, False)
    run_uvicorn = mock.Mock()
    health_server = mock.Mock()
    start_refresh = mock.Mock()
    run_in_parallel = mock.Mock()
    monkeypatch.setattr(runtime, '_start_background_loop',
                        lambda *args: background)
    monkeypatch.setattr(runtime.executor, 'start', lambda current:
                        (queue_server, [worker]))
    monkeypatch.setattr(runtime, '_run_uvicorn', run_uvicorn)
    monkeypatch.setattr(runtime, '_wait_for_executor_shutdown', lambda: None)
    monkeypatch.setattr(runtime, '_RoleHealthServer',
                        lambda *args: health_server)
    monkeypatch.setattr(managed_job_refresh_thread,
                        'start_managed_job_refresh_daemon', start_refresh)
    monkeypatch.setattr(runtime.subprocess_utils, 'run_in_parallel',
                        run_in_parallel)
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    runtime.run_role(state, _args())

    run_uvicorn.assert_not_called()
    start_refresh.assert_called_once_with()
    lease.set_ready.assert_called_once()
    health_server.start.assert_called_once_with()
    health_server.stop.assert_called_once_with()
    lease.stop.assert_called_once_with()
    run_in_parallel.assert_called_once()
    queue_server.kill.assert_called_once_with()
    queue_server.join.assert_called_once_with()
    assert background.stopped


@pytest.mark.asyncio
async def test_api_role_routes_jobs_wait_to_durable_executor(monkeypatch):
    request = SimpleNamespace(
        state=SimpleNamespace(request_id='jobs-wait', auth_user=None))
    body = mock.Mock()
    schedule = mock.AsyncMock()
    prepare = mock.AsyncMock()
    execute = mock.Mock()
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'api')
    monkeypatch.setattr(jobs_server.executor, 'schedule_request_async',
                        schedule)
    monkeypatch.setattr(jobs_server.executor, 'prepare_request_async', prepare)
    monkeypatch.setattr(jobs_server.executor, 'execute_request_in_coroutine',
                        execute)

    await jobs_server.wait(request, body)

    schedule.assert_awaited_once()
    prepare.assert_not_awaited()
    execute.assert_not_called()
