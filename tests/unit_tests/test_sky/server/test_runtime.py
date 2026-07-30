"""Role-isolation tests for the API server process supervisors."""

import time
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.jobs import managed_job_refresh_thread
from sky.jobs.server import server as jobs_server
from sky.server import runtime
from sky.server.requests import registry as request_registry


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


def test_role_drain_marker_fails_readiness_before_shutdown(
        monkeypatch, tmp_path):
    drain_marker = tmp_path / 'draining'
    monkeypatch.setattr(runtime.request_postgres, 'ROLE_DRAIN_MARKER_PATH',
                        str(drain_marker))
    lease = runtime.request_postgres.ServerInstanceLease('executor')
    lease._ready = True  # pylint: disable=protected-access
    lease._last_success_monotonic = time.monotonic()  # pylint: disable=protected-access
    assert lease.is_locally_ready()

    drain_marker.touch()

    assert not lease.is_locally_ready()
    assert not runtime.request_postgres.current_instance_is_ready()
    values = lease._values(include_started_at=False)  # pylint: disable=protected-access
    assert not values['ready']
    assert values['draining_at'] is not None
    assert values['health_detail'] == {'phase': 'draining'}


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
    start_workers = mock.Mock(return_value=(queue_server, [worker]))
    monkeypatch.setattr(runtime.executor, 'start', start_workers)
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
    start_workers.assert_called_once_with(
        config,
        execution_classes=frozenset({request_registry.ExecutionClass.NORMAL}))
    start_refresh.assert_not_called()
    lease.set_ready.assert_called_once()
    health_server.start.assert_called_once_with()
    health_server.stop.assert_called_once_with()
    lease.stop.assert_called_once_with()
    run_in_parallel.assert_called_once()
    queue_server.kill.assert_called_once_with()
    queue_server.join.assert_called_once_with()
    assert background.stopped


def test_stop_queue_server_is_idempotent_after_child_cleanup():
    queue_server = mock.Mock()
    queue_server.is_alive.return_value = False

    runtime._stop_queue_server(  # pylint: disable=protected-access
        queue_server)

    queue_server.kill.assert_not_called()
    queue_server.join.assert_called_once_with()


def test_controller_role_fences_children_and_exits_on_lock_loss(monkeypatch):
    background = _BackgroundLoop()
    instance_lease = mock.Mock()
    instance_lease.instance_id = '00000000-0000-0000-0000-000000000001'
    config = mock.Mock()
    state = runtime.RuntimeState('controller', config, instance_lease, False)
    leader = mock.Mock()
    leader.instance_id = instance_lease.instance_id
    leader.generation = 7
    leader.try_acquire.return_value = True
    leader.backend_pid.return_value = 123
    leader.heartbeat.return_value = False
    health_server = mock.Mock()
    queue_server = mock.Mock()
    worker = mock.Mock()
    start_workers = mock.Mock(return_value=(queue_server, [worker]))
    start_refresh = mock.Mock()
    shutdown_workers = mock.Mock()
    stop_queue = mock.Mock()
    kill_children = mock.Mock()
    surface_scan = mock.Mock()
    fence_claims = mock.Mock(return_value={'replayed': 2, 'interrupted': 1})

    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda *args: leader)
    monkeypatch.setattr(runtime.request_postgres,
                        'fence_stale_controller_claims', fence_claims)
    monkeypatch.setattr(runtime.request_postgres,
                        'recent_legacy_controller_consumers', lambda *args: [])
    monkeypatch.setattr(runtime, '_RoleHealthServer',
                        lambda *args: health_server)
    monkeypatch.setattr(runtime, '_start_background_loop',
                        lambda *args: background)
    monkeypatch.setattr(runtime.executor, 'start', start_workers)
    monkeypatch.setattr(managed_job_refresh_thread,
                        'start_managed_job_refresh_daemon', start_refresh)
    monkeypatch.setattr(runtime, '_request_worker_shutdown', shutdown_workers)
    monkeypatch.setattr(runtime, '_stop_queue_server', stop_queue)
    monkeypatch.setattr(runtime, '_kill_local_controller_children',
                        kill_children)
    monkeypatch.setattr(runtime, '_start_surface_interrupted_cluster_launches',
                        surface_scan)
    monkeypatch.setattr(runtime.clean_env_module, 'capture_clean_server_env',
                        mock.Mock())
    monkeypatch.setattr(runtime, '_CONTROLLER_LEADERSHIP_PROBE_SECONDS', 0)
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    with pytest.raises(RuntimeError, match='leadership session was lost'):
        runtime.run_role(state, _args())

    fence_claims.assert_called_once_with(instance_lease.instance_id, 7)
    start_workers.assert_called_once_with(
        config,
        execution_classes=frozenset(
            {request_registry.ExecutionClass.CONTROLLER}),
        controller_generation=7)
    start_refresh.assert_called_once_with()
    surface_scan.assert_called_once_with()
    worker.request_shutdown.assert_called_once_with()
    kill_children.assert_called_once_with()
    shutdown_workers.assert_called_once_with([worker], terminate_children=True)
    stop_queue.assert_called_once_with(queue_server)
    leader.release.assert_called_once_with()
    health_server.start.assert_called_once_with()
    health_server.stop.assert_called_once_with()
    assert instance_lease.set_ready.call_args_list[0] == mock.call(
        False, health_detail={'phase': 'checking-executor-cutover'})
    assert any(
        call.kwargs.get('health_detail', {}).get('phase') == 'standby'
        for call in instance_lease.set_ready.call_args_list)
    assert any(
        call.kwargs.get('health_detail', {}).get('phase') == 'leadership-lost'
        for call in instance_lease.set_ready.call_args_list)
    assert background.stopped


def test_controller_role_becomes_unready_before_graceful_release(monkeypatch):
    background = _BackgroundLoop()
    instance_lease = mock.Mock()
    instance_lease.instance_id = '00000000-0000-0000-0000-000000000001'
    state = runtime.RuntimeState('controller', mock.Mock(), instance_lease,
                                 False)
    leader = mock.Mock()
    leader.instance_id = instance_lease.instance_id
    leader.generation = 8
    leader.try_acquire.return_value = True
    leader.backend_pid.return_value = 123
    health_server = mock.Mock()
    worker = mock.Mock()
    queue_server = mock.Mock()
    lifecycle = mock.Mock()
    lifecycle.attach_mock(instance_lease.set_ready, 'set_ready')
    lifecycle.attach_mock(leader.release, 'release')

    class ShutdownAfterPromotion:
        """Event stub that requests graceful shutdown after leader startup."""

        def is_set(self) -> bool:
            return False

        def wait(self, timeout) -> bool:
            del timeout
            return True

        def set(self) -> None:
            pass

    monkeypatch.setattr(runtime.threading, 'Event', ShutdownAfterPromotion)
    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda *args: leader)
    monkeypatch.setattr(runtime.request_postgres,
                        'fence_stale_controller_claims', lambda *args: {
                            'replayed': 0,
                            'interrupted': 0
                        })
    monkeypatch.setattr(runtime.request_postgres,
                        'recent_legacy_controller_consumers', lambda *args: [])
    monkeypatch.setattr(runtime, '_RoleHealthServer',
                        lambda *args: health_server)
    monkeypatch.setattr(runtime, '_start_background_loop',
                        lambda *args: background)
    monkeypatch.setattr(runtime.executor, 'start', lambda *args, **kwargs:
                        (queue_server, [worker]))
    monkeypatch.setattr(managed_job_refresh_thread,
                        'start_managed_job_refresh_daemon', mock.Mock())
    monkeypatch.setattr(runtime, '_request_worker_shutdown', mock.Mock())
    monkeypatch.setattr(runtime, '_stop_queue_server', mock.Mock())
    monkeypatch.setattr(runtime, '_kill_local_controller_children', mock.Mock())
    monkeypatch.setattr(runtime, '_start_surface_interrupted_cluster_launches',
                        mock.Mock())
    monkeypatch.setattr(runtime.clean_env_module, 'capture_clean_server_env',
                        mock.Mock())
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    runtime.run_role(state, _args())

    draining = mock.call.set_ready(False,
                                   health_detail={
                                       'phase': 'draining',
                                       'controller_generation': 8,
                                   })
    assert draining in lifecycle.mock_calls
    assert lifecycle.mock_calls.index(draining) < lifecycle.mock_calls.index(
        mock.call.release())
    assert background.stopped


def test_controller_role_stays_unready_while_m2_executor_is_recent(monkeypatch):
    instance_lease = mock.Mock()
    instance_lease.instance_id = '00000000-0000-0000-0000-000000000001'
    state = runtime.RuntimeState('controller', mock.Mock(), instance_lease,
                                 False)
    leader = mock.Mock()
    health_server = mock.Mock()
    recent_consumers = mock.Mock(return_value=['legacy-executor'])

    class ShutdownWhileWaiting:
        """Event stub that shuts down after one cutover poll."""

        def __init__(self) -> None:
            self._is_set = False

        def is_set(self) -> bool:
            return self._is_set

        def wait(self, timeout) -> bool:
            del timeout
            self._is_set = True
            return True

        def set(self) -> None:
            self._is_set = True

    monkeypatch.setattr(runtime.threading, 'Event', ShutdownWhileWaiting)
    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda *args: leader)
    monkeypatch.setattr(runtime.request_postgres,
                        'recent_legacy_controller_consumers', recent_consumers)
    monkeypatch.setattr(runtime, '_RoleHealthServer',
                        lambda *args: health_server)
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    runtime.run_role(state, _args())

    recent_consumers.assert_called_once_with(70)
    leader.try_acquire.assert_not_called()
    leader.release.assert_not_called()
    phases = [
        call.kwargs['health_detail']['phase']
        for call in instance_lease.set_ready.call_args_list
    ]
    assert phases == [
        'checking-executor-cutover',
        'waiting-for-executor-cutover',
        'stopped',
    ]
    assert not any(
        call.args[0] for call in instance_lease.set_ready.call_args_list)
    health_server.start.assert_called_once_with()
    health_server.stop.assert_called_once_with()
    instance_lease.stop.assert_called_once_with()


def test_controller_role_rechecks_cutover_after_acquiring_lock(monkeypatch):
    instance_lease = mock.Mock()
    instance_lease.instance_id = '00000000-0000-0000-0000-000000000001'
    state = runtime.RuntimeState('controller', mock.Mock(), instance_lease,
                                 False)
    leader = mock.Mock()
    leader.try_acquire.return_value = True
    health_server = mock.Mock()
    recent_consumers = mock.Mock(side_effect=[[], ['legacy-executor']])
    start_workers = mock.Mock()

    class ShutdownAfterPromotionCheck:
        """Event stub that shuts down after the failed promotion poll."""

        def __init__(self) -> None:
            self._is_set = False

        def is_set(self) -> bool:
            return self._is_set

        def wait(self, timeout) -> bool:
            del timeout
            self._is_set = True
            return True

        def set(self) -> None:
            self._is_set = True

    monkeypatch.setattr(runtime.threading, 'Event', ShutdownAfterPromotionCheck)
    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda *args: leader)
    monkeypatch.setattr(runtime.request_postgres,
                        'recent_legacy_controller_consumers', recent_consumers)
    monkeypatch.setattr(runtime, '_RoleHealthServer',
                        lambda *args: health_server)
    monkeypatch.setattr(runtime.executor, 'start', start_workers)
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    runtime.run_role(state, _args())

    assert recent_consumers.call_count == 2
    leader.try_acquire.assert_called_once_with()
    leader.release.assert_called_once_with()
    start_workers.assert_not_called()
    assert any(
        call.kwargs.get('health_detail', {}).get('phase') ==
        'waiting-for-executor-cutover'
        for call in instance_lease.set_ready.call_args_list)


def test_controller_role_exits_if_m2_executor_reappears(monkeypatch):
    background = _BackgroundLoop()
    instance_lease = mock.Mock()
    instance_lease.instance_id = '00000000-0000-0000-0000-000000000001'
    state = runtime.RuntimeState('controller', mock.Mock(), instance_lease,
                                 False)
    leader = mock.Mock()
    leader.instance_id = instance_lease.instance_id
    leader.generation = 9
    leader.try_acquire.return_value = True
    leader.backend_pid.return_value = 123
    health_server = mock.Mock()
    queue_server = mock.Mock()
    worker = mock.Mock()
    recent_consumers = mock.Mock(side_effect=[[], [], ['legacy-executor']])
    shutdown_workers = mock.Mock()
    stop_queue = mock.Mock()
    kill_children = mock.Mock()

    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda *args: leader)
    monkeypatch.setattr(runtime.request_postgres,
                        'fence_stale_controller_claims', lambda *args: {
                            'replayed': 0,
                            'interrupted': 0
                        })
    monkeypatch.setattr(runtime.request_postgres,
                        'recent_legacy_controller_consumers', recent_consumers)
    monkeypatch.setattr(runtime, '_RoleHealthServer',
                        lambda *args: health_server)
    monkeypatch.setattr(runtime, '_start_background_loop',
                        lambda *args: background)
    monkeypatch.setattr(runtime.executor, 'start', lambda *args, **kwargs:
                        (queue_server, [worker]))
    monkeypatch.setattr(managed_job_refresh_thread,
                        'start_managed_job_refresh_daemon', mock.Mock())
    monkeypatch.setattr(runtime, '_request_worker_shutdown', shutdown_workers)
    monkeypatch.setattr(runtime, '_stop_queue_server', stop_queue)
    monkeypatch.setattr(runtime, '_kill_local_controller_children',
                        kill_children)
    monkeypatch.setattr(runtime, '_start_surface_interrupted_cluster_launches',
                        mock.Mock())
    monkeypatch.setattr(runtime.clean_env_module, 'capture_clean_server_env',
                        mock.Mock())
    monkeypatch.setattr(runtime, '_CONTROLLER_LEADERSHIP_PROBE_SECONDS', 0)
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    with pytest.raises(RuntimeError,
                       match='legacy controller consumer reappeared'):
        runtime.run_role(state, _args())

    assert recent_consumers.call_count == 3
    leader.heartbeat.assert_not_called()
    worker.request_shutdown.assert_called_once_with()
    kill_children.assert_called_once_with()
    shutdown_workers.assert_called_once_with([worker], terminate_children=True)
    stop_queue.assert_called_once_with(queue_server)
    leader.release.assert_called_once_with()
    assert any(
        call.kwargs.get('health_detail', {}).get('phase') ==
        'legacy-consumer-detected'
        for call in instance_lease.set_ready.call_args_list)
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
