"""Role-isolation tests for the API server process supervisors."""

import http.client
import json
import os
import pathlib
import socket
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.jobs import managed_job_refresh_thread
from sky.jobs.server import server as jobs_server
from sky.serve import constants as serve_constants
from sky.server import runtime
from sky.server.requests import cutover as request_cutover
from sky.server.requests import registry as request_registry

_SYSTEM_OOM_METRIC_CHILD_SCRIPT = """
from sky.serve import system_oom_recovery_observability as observability

observability.record(
    'recovery_started', provider='aws', market='on_demand')
"""


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
                           role_health_port=46581,
                           authority_preflight_port=46583)


@pytest.mark.parametrize(('phase', 'backend_kind', 'raises'), [
    ('blocked', 'sqlite', False),
    ('blocked', 'postgres', True),
    ('blocked', 'postgres-subclass', True),
    ('blocked', 'custom', True),
    ('cutover-complete', 'postgres', False),
    ('cutover-complete', 'sqlite', True),
    ('cutover-complete', 'postgres-subclass', True),
])
def test_completed_request_store_cutover_fails_closed(tmp_path, monkeypatch,
                                                      phase, backend_kind,
                                                      raises):
    gate = tmp_path / 'api-request-cutover.json'
    gate.write_text(json.dumps({
        'format_version': 1,
        'phase': phase,
        'source_path': '/root/.sky/api_server/requests.db',
    }),
                    encoding='utf-8')
    monkeypatch.setenv(request_cutover.CUTOVER_GATE_PATH_ENV_VAR, str(gate))

    class PluginPostgresBackend(runtime.request_postgres.PostgresRequestBackend
                               ):
        pass

    if backend_kind == 'postgres':
        backend = runtime.request_postgres.PostgresRequestBackend()
        monkeypatch.setenv(runtime.request_postgres.REQUEST_BACKEND_ENV_VAR,
                           runtime.request_postgres.POSTGRES_REQUEST_BACKEND)
    elif backend_kind == 'postgres-subclass':
        backend = PluginPostgresBackend()
        monkeypatch.setenv(runtime.request_postgres.REQUEST_BACKEND_ENV_VAR,
                           runtime.request_postgres.POSTGRES_REQUEST_BACKEND)
    else:
        backend = (runtime.requests_lib.SqliteRequestBackend()
                   if backend_kind == 'sqlite' else object())
        monkeypatch.setenv(runtime.request_postgres.REQUEST_BACKEND_ENV_VAR,
                           'sqlite')
    monkeypatch.setattr(runtime.request_storage, 'get_request_backend',
                        lambda: backend)

    if raises:
        with pytest.raises(RuntimeError,
                           match='before the import|Refusing stale SQLite'):
            # pylint: disable-next=protected-access
            runtime._guard_completed_request_store_cutover()
    else:
        # pylint: disable-next=protected-access
        runtime._guard_completed_request_store_cutover()


def test_active_reserved_fill_v2_requires_backend_guard(monkeypatch):
    monkeypatch.setattr(runtime.serve_state, 'get_reserved_fill_protocol_state',
                        lambda: {'protocol_version': 2})
    monkeypatch.delenv(
        runtime.request_postgres.EXECUTION_QUIESCENCE_BACKEND_GUARD_ENV_VAR,
        raising=False)

    with pytest.raises(RuntimeError, match='enforcement is disabled'):
        # pylint: disable-next=protected-access
        runtime._guard_active_reserved_fill_protocol('executor')


def test_active_reserved_fill_v2_revalidates_backend_guard(monkeypatch):
    monkeypatch.setattr(runtime.serve_state, 'get_reserved_fill_protocol_state',
                        lambda: {'protocol_version': 2})
    monkeypatch.setenv(
        runtime.request_postgres.EXECUTION_QUIESCENCE_BACKEND_GUARD_ENV_VAR,
        'true')
    validate = mock.Mock()
    monkeypatch.setattr(runtime.request_postgres,
                        'require_builtin_execution_quiescence_backends',
                        validate)

    # pylint: disable-next=protected-access
    runtime._guard_active_reserved_fill_protocol('executor')

    validate.assert_called_once_with(required=True)


def test_reserved_fill_v1_preserves_unguarded_backend_compatibility(
        monkeypatch):
    monkeypatch.setattr(runtime.serve_state, 'get_reserved_fill_protocol_state',
                        lambda: {'protocol_version': 1})
    validate = mock.Mock()
    monkeypatch.setattr(runtime.request_postgres,
                        'require_builtin_execution_quiescence_backends',
                        validate)

    # pylint: disable-next=protected-access
    runtime._guard_active_reserved_fill_protocol('executor')

    validate.assert_not_called()


def test_active_reserved_fill_v2_does_not_gate_authority_worker(monkeypatch):
    state_reader = mock.Mock(return_value={'protocol_version': 2})
    monkeypatch.setattr(runtime.serve_state, 'get_reserved_fill_protocol_state',
                        state_reader)

    # pylint: disable-next=protected-access
    runtime._guard_active_reserved_fill_protocol('authority-worker')

    state_reader.assert_not_called()


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


def test_authority_health_splits_bootstrap_from_claim_readiness(
        monkeypatch, tmp_path):
    monkeypatch.setattr(runtime.request_postgres, 'ROLE_DRAIN_MARKER_PATH',
                        str(tmp_path / 'draining'))
    lease = mock.Mock()
    lease.role = 'authority-worker'
    lease.is_locally_ready.return_value = False
    bootstrap_ready = True
    server = runtime._RoleHealthServer(  # pylint: disable=protected-access
        '127.0.0.1',
        0,
        lease,
        bootstrap_ready=lambda: bootstrap_ready)
    server.start()
    port = server._server.server_port  # pylint: disable=protected-access

    def status(path: str) -> int:
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
        connection.request('GET', path)
        response = connection.getresponse()
        response.read()
        connection.close()
        return response.status

    try:
        assert status('/livez') == 200
        assert status('/bootstrapz') == 200
        assert status('/readyz') == 503
        (tmp_path / 'draining').touch()
        assert status('/bootstrapz') == 503
    finally:
        bootstrap_ready = False
        server.stop()


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


def test_authority_role_is_preflight_only(monkeypatch):
    lease = mock.Mock()
    lease.instance_id = '00000000-0000-4000-8000-000000000001'
    config = mock.Mock()
    state = runtime.RuntimeState('authority-worker', config, lease, False)
    health_server = mock.Mock()
    preflight_server = mock.Mock()
    preflight_server.is_transport_ready.return_value = True
    coordinator = mock.Mock()
    coordinator.failure = None
    manifest = mock.sentinel.manifest
    pod_identity = SimpleNamespace(uid=lease.instance_id)
    events = []
    health_server.start.side_effect = lambda: events.append('health-start')
    preflight_server.start.side_effect = lambda: events.append('preflight-start'
                                                              )
    coordinator.start.side_effect = lambda: events.append('coordinator-start')
    coordinator.clear_acceptance.side_effect = lambda: events.append(
        'acceptance-clear')
    preflight_server.stop.side_effect = lambda: events.append('preflight-stop')
    coordinator.stop.side_effect = lambda: events.append('coordinator-stop')
    health_server.stop.side_effect = lambda: events.append('health-stop')
    start_workers = mock.Mock(side_effect=AssertionError('executor started'))
    start_background = mock.Mock(
        side_effect=AssertionError('background loop started'))
    capture_env = mock.Mock(side_effect=AssertionError('claim env captured'))
    evaluator = mock.Mock()
    accepted_callbacks = []
    health_kwargs = {}

    def build_evaluator(accepted_manifest):
        accepted_callbacks.append(accepted_manifest)
        assert accepted_manifest() is None
        return evaluator

    evaluator_type = mock.Mock(side_effect=build_evaluator)
    preflight_type = mock.Mock(return_value=preflight_server)
    monkeypatch.setattr(runtime, '_start_background_loop', start_background)
    monkeypatch.setattr(runtime.executor, 'start', start_workers)
    monkeypatch.setattr(runtime.clean_env_module, 'capture_clean_server_env',
                        capture_env)

    def build_health_server(*args, **kwargs):
        del args
        health_kwargs.update(kwargs)
        return health_server

    monkeypatch.setattr(runtime, '_RoleHealthServer', build_health_server)

    def load_manifest():
        assert events == ['health-start', 'preflight-start']
        assert not health_kwargs['bootstrap_ready']()
        events.append('manifest-load')
        return manifest

    def build_coordinator(*args):
        assert args == (manifest, pod_identity)
        assert events == ['health-start', 'preflight-start', 'manifest-load']
        assert health_kwargs['bootstrap_ready']()
        events.append('coordinator-build')
        return coordinator

    monkeypatch.setattr(runtime, '_load_authority_static_manifest',
                        load_manifest)
    monkeypatch.setattr(runtime, '_build_authority_bootstrap_coordinator',
                        build_coordinator)
    monkeypatch.setattr(
        'sky.serve.resource_action_provider_preflight.InitialProviderPreflightEvaluator',
        evaluator_type)
    monkeypatch.setattr(
        'sky.server.requests.authority_worker_bootstrap.AuthorityWorkerPodIdentity.from_environment',
        lambda: pod_identity)
    monkeypatch.setattr(
        'sky.server.authority_preflight.AuthorityPreflightServer',
        preflight_type)
    monkeypatch.setattr(
        'sky.server.authority_preflight.authority_preflight_service_dns',
        mock.Mock(return_value='test-authority-preflight.ns.svc'))

    def wait_for_shutdown(value):
        assert value is coordinator
        events.append('wait')

    monkeypatch.setattr(runtime, '_wait_for_authority_shutdown',
                        wait_for_shutdown)
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    runtime.run_role(state, _args())

    start_workers.assert_not_called()
    start_background.assert_not_called()
    capture_env.assert_not_called()
    lease.set_ready.assert_called_once_with(
        False, health_detail={'phase': 'preflight-only'})
    health_server.start.assert_called_once_with()
    health_server.stop.assert_called_once_with()
    evaluator_type.assert_called_once_with(accepted_callbacks[0])
    assert accepted_callbacks[0]() is coordinator.accepted_manifest.return_value
    preflight_type.assert_called_once_with('127.0.0.1',
                                           46583,
                                           'test-authority-preflight.ns.svc',
                                           evaluator,
                                           on_transport_invalid=mock.ANY)
    assert callable(preflight_type.call_args.kwargs['on_transport_invalid'])
    assert events == [
        'health-start', 'preflight-start', 'manifest-load', 'coordinator-build',
        'coordinator-start', 'wait', 'acceptance-clear', 'preflight-stop',
        'coordinator-stop', 'health-stop'
    ]
    lease.stop.assert_called_once_with()


def test_authority_role_unwinds_bound_listeners_if_bootstrap_build_fails(
        monkeypatch):
    lease = mock.Mock()
    lease.instance_id = '00000000-0000-4000-8000-000000000001'
    state = runtime.RuntimeState('authority-worker', mock.Mock(), lease, False)
    pod_identity = SimpleNamespace(uid=lease.instance_id)
    events = []
    health_server = mock.Mock()
    preflight_server = mock.Mock()
    preflight_server.is_transport_ready.return_value = True
    health_server.start.side_effect = lambda: events.append('health-start')
    preflight_server.start.side_effect = lambda: events.append('preflight-start'
                                                              )
    preflight_server.stop.side_effect = lambda: events.append('preflight-stop')
    health_server.stop.side_effect = lambda: events.append('health-stop')
    preflight_type = mock.Mock(return_value=preflight_server)
    monkeypatch.setattr(runtime, '_RoleHealthServer',
                        lambda *args, **kwargs: health_server)
    monkeypatch.setattr(
        runtime, '_load_authority_static_manifest',
        lambda: events.append('manifest-load') or mock.sentinel.manifest)

    def fail_build(*args):
        del args
        assert events == ['health-start', 'preflight-start', 'manifest-load']
        raise RuntimeError('in-cluster client unavailable')

    monkeypatch.setattr(runtime, '_build_authority_bootstrap_coordinator',
                        fail_build)
    monkeypatch.setattr(
        'sky.serve.resource_action_provider_preflight.InitialProviderPreflightEvaluator',
        lambda accepted_manifest: mock.Mock())
    monkeypatch.setattr(
        'sky.server.requests.authority_worker_bootstrap.AuthorityWorkerPodIdentity.from_environment',
        lambda: pod_identity)
    monkeypatch.setattr(
        'sky.server.authority_preflight.AuthorityPreflightServer',
        preflight_type)
    monkeypatch.setattr(
        'sky.server.authority_preflight.authority_preflight_service_dns',
        mock.Mock(return_value='test-authority-preflight.ns.svc'))

    with pytest.raises(RuntimeError, match='in-cluster client unavailable'):
        runtime._run_authority_preflight_role(  # pylint: disable=protected-access
            state, _args())

    assert events == [
        'health-start', 'preflight-start', 'manifest-load', 'preflight-stop',
        'health-stop'
    ]
    lease.set_ready.assert_called_once_with(
        False, health_detail={'phase': 'preflight-only'})


def test_stop_queue_server_is_idempotent_after_child_cleanup():
    queue_server = mock.Mock()
    queue_server.is_alive.return_value = False

    runtime._stop_queue_server(  # pylint: disable=protected-access
        queue_server)

    queue_server.kill.assert_not_called()
    queue_server.join.assert_called_once_with()


def test_controller_supervisor_validates_preflight_auth_before_readiness(
        monkeypatch):
    lease = mock.Mock()
    state = runtime.RuntimeState('controller', mock.Mock(), lease, False)
    monkeypatch.setenv(
        serve_constants.RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE_ENV_VAR,
        '/purpose/tokens')
    monkeypatch.setenv(
        serve_constants.RESOURCE_ACTION_AUTHORITY_ENABLED_ENV_VAR, 'true')
    validate = mock.Mock(side_effect=RuntimeError('rings overlap'))
    monkeypatch.setattr(
        runtime.auth_tokens,
        'validate_resource_action_preflight_auth_token_isolation', validate)

    with pytest.raises(RuntimeError, match='rings overlap'):
        runtime._run_controller_role(  # pylint: disable=protected-access
            state, _args())

    validate.assert_called_once_with(required=True)
    lease.set_ready.assert_not_called()


def test_controller_supervisor_ignores_custom_preflight_env_while_disabled(
        monkeypatch):
    lease = mock.Mock()
    state = runtime.RuntimeState('controller', mock.Mock(), lease, False)
    monkeypatch.delenv(
        serve_constants.RESOURCE_ACTION_AUTHORITY_ENABLED_ENV_VAR,
        raising=False)
    monkeypatch.setenv(
        serve_constants.RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE_ENV_VAR,
        '/compatibility/tokens')
    validate = mock.Mock()
    monkeypatch.setattr(
        runtime.auth_tokens,
        'validate_resource_action_preflight_auth_token_isolation', validate)
    monkeypatch.setattr(
        runtime.request_postgres, 'ControllerLeaderLease',
        mock.Mock(side_effect=RuntimeError('past activation gate')))

    with pytest.raises(RuntimeError, match='past activation gate'):
        runtime._run_controller_role(  # pylint: disable=protected-access
            state, _args())

    validate.assert_not_called()
    lease.set_ready.assert_not_called()


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


@pytest.mark.parametrize(('role', 'register_shared_collector'),
                         [('all', True), ('api', True), ('executor', False),
                          ('controller', False)])
def test_every_api_server_role_exposes_local_multiprocess_metrics(
        monkeypatch, role, register_shared_collector):
    background = mock.Mock()
    metrics_server = mock.Mock()
    serve = mock.sentinel.serve
    reaper = mock.sentinel.reaper
    metrics_server.started = True
    serve_task = mock.Mock()
    background.create_task.side_effect = [serve_task, mock.sentinel.reaper_task]
    register = mock.Mock()
    build = mock.Mock(return_value=metrics_server)
    build_serve = mock.Mock(return_value=serve)
    build_reaper = mock.Mock(return_value=reaper)
    monkeypatch.setenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED, 'true')
    monkeypatch.setenv('PROMETHEUS_MULTIPROC_DIR', '/tmp/metrics')
    monkeypatch.setattr(runtime, '_BackgroundLoop', lambda: background)
    monkeypatch.setattr(runtime.metrics,
                        'maybe_register_managed_jobs_collector', register)
    monkeypatch.setattr(runtime.metrics, 'build_metrics_server', build)
    monkeypatch.setattr(runtime, '_serve_metrics_server', build_serve)
    monkeypatch.setattr(runtime.metrics, 'multiproc_reaper_daemon',
                        build_reaper)

    result = runtime._start_metrics_background_loop(  # pylint: disable=protected-access
        role, '127.0.0.1', 9090)

    assert result is background
    if register_shared_collector:
        register.assert_called_once_with()
    else:
        register.assert_not_called()
    build.assert_called_once_with('127.0.0.1', 9090)
    build_serve.assert_called_once_with(metrics_server)
    assert background.create_task.call_args_list == [
        mock.call(serve), mock.call(reaper)
    ]
    background.add_graceful_shutdown_hook.assert_called_once_with(mock.ANY)
    serve_task.add_done_callback.assert_called_once_with(mock.ANY)
    background.start.assert_called_once_with()


@pytest.mark.parametrize(('value', 'expected'),
                         [('true', True), ('false', False), (None, False)])
def test_metrics_enabled_requires_literal_true(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED,
                           raising=False)
    else:
        monkeypatch.setenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED,
                           value)

    assert runtime._metrics_enabled() is expected  # pylint: disable=protected-access


def test_controller_role_metrics_expose_system_oom_counter_and_zero_baselines(
        monkeypatch, tmp_path):
    """Exercise child and cold-zero emission through the real endpoint."""
    repository_root = pathlib.Path(runtime.__file__).resolve().parents[2]
    child_env = os.environ.copy()
    child_env['PROMETHEUS_MULTIPROC_DIR'] = str(tmp_path)
    child_env[runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED] = 'true'
    child_env['SKYPILOT_API_SERVER_ROLE'] = 'controller'
    existing_pythonpath = child_env.get('PYTHONPATH')
    child_env['PYTHONPATH'] = (
        str(repository_root) if not existing_pythonpath else
        f'{repository_root}{os.pathsep}{existing_pythonpath}')
    subprocess.run([sys.executable, '-c', _SYSTEM_OOM_METRIC_CHILD_SCRIPT],
                   cwd=repository_root,
                   env=child_env,
                   capture_output=True,
                   text=True,
                   check=True,
                   timeout=60)

    monkeypatch.setenv('PROMETHEUS_MULTIPROC_DIR', str(tmp_path))
    monkeypatch.setenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED, 'true')
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'controller')
    built_servers = []
    build_server = runtime.metrics.build_metrics_server

    def capture_server(host, port):
        server = build_server(host, port)
        built_servers.append(server)
        return server

    monkeypatch.setattr(runtime.metrics, 'build_metrics_server', capture_server)
    background = runtime._start_metrics_background_loop(  # pylint: disable=protected-access
        'controller', '127.0.0.1', 0)
    assert background is not None
    try:
        assert len(built_servers) == 1
        sockets = built_servers[0].servers[0].sockets
        assert sockets is not None
        metrics_port = sockets[0].getsockname()[1]
        connection = http.client.HTTPConnection('127.0.0.1', metrics_port, 10)
        try:
            connection.request('GET', '/metrics')
            response = connection.getresponse()
            payload = response.read().decode('utf-8')
        finally:
            connection.close()
        assert response.status == 200
        samples = [
            line for line in payload.splitlines()
            if line.startswith('sky_serve_system_oom_recovery_events_total{')
        ]
        assert len(samples) == 5, payload
        recovery_samples = [
            sample for sample in samples if 'event="recovery_started"' in sample
        ]
        assert len(recovery_samples) == 1, payload
        assert 'provider="aws"' in recovery_samples[0]
        assert 'market="on_demand"' in recovery_samples[0]
        assert recovery_samples[0].endswith(' 1.0')
        for event in (
                'authorization_v1_selected',
                'authorization_v2_selected',
                'runtime_capability_v1_observed',
                'status_only_read',
        ):
            baseline_samples = [
                sample for sample in samples if f'event="{event}"' in sample
            ]
            assert len(baseline_samples) == 1, payload
            assert 'provider="unknown"' in baseline_samples[0]
            assert 'market="unknown"' in baseline_samples[0]
            assert baseline_samples[0].endswith(' 0.0')
    finally:
        background.stop()
    assert background.is_stopping
    assert not background._thread.is_alive()  # pylint: disable=protected-access


def test_metrics_server_death_after_start_terminates_role(monkeypatch):
    background = mock.Mock()
    background.is_stopping = False
    metrics_server = mock.Mock(started=True)
    serve_task = mock.Mock()
    background.create_task.side_effect = [serve_task, mock.Mock()]
    terminate = mock.Mock()
    pid = runtime.os.getpid()
    monkeypatch.setenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED, 'true')
    monkeypatch.setenv('PROMETHEUS_MULTIPROC_DIR', '/tmp/metrics')
    monkeypatch.setattr(runtime, '_BackgroundLoop', lambda: background)
    monkeypatch.setattr(runtime.metrics, 'build_metrics_server',
                        lambda *args: metrics_server)
    monkeypatch.setattr(runtime, '_serve_metrics_server',
                        lambda *args: mock.sentinel.serve)
    monkeypatch.setattr(runtime.metrics, 'multiproc_reaper_daemon',
                        lambda: mock.sentinel.reaper)
    monkeypatch.setattr(runtime.os, 'kill', terminate)

    runtime._start_metrics_background_loop(  # pylint: disable=protected-access
        'executor', '127.0.0.1', 9090)
    done_callback = serve_task.add_done_callback.call_args.args[0]
    failed_task = mock.Mock()
    failed_task.cancelled.return_value = False
    failed_task.exception.return_value = RuntimeError('listener failed')
    done_callback(failed_task)

    terminate.assert_called_once_with(pid, runtime.signal.SIGTERM)
    background.is_stopping = True
    done_callback(failed_task)
    terminate.assert_called_once_with(pid, runtime.signal.SIGTERM)


def test_cancelled_metrics_server_task_is_an_unexpected_failure():
    cancelled_task = mock.Mock()
    cancelled_task.cancelled.return_value = True

    failure = runtime._metrics_task_failure(  # pylint: disable=protected-access
        cancelled_task)

    assert isinstance(failure, RuntimeError)
    assert str(failure) == 'The metrics server task was cancelled unexpectedly.'
    cancelled_task.exception.assert_not_called()


def test_metrics_startup_fails_closed_when_port_is_occupied(
        monkeypatch, tmp_path):
    loops = []
    loop_type = runtime._BackgroundLoop  # pylint: disable=protected-access

    def build_loop():
        loop = loop_type()
        loops.append(loop)
        return loop

    monkeypatch.setenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED, 'true')
    monkeypatch.setenv('PROMETHEUS_MULTIPROC_DIR', str(tmp_path))
    monkeypatch.setattr(runtime, '_BackgroundLoop', build_loop)
    with socket.socket() as occupied:
        occupied.bind(('127.0.0.1', 0))
        occupied.listen()
        port = occupied.getsockname()[1]

        with pytest.raises(RuntimeError, match='failed to become available'):
            runtime._start_metrics_background_loop(  # pylint: disable=protected-access
                'executor', '127.0.0.1', port)

    assert len(loops) == 1
    assert loops[0].is_stopping
    assert not loops[0]._thread.is_alive()  # pylint: disable=protected-access


def test_metrics_background_is_disabled_without_deployment_marker(monkeypatch):
    start = mock.Mock(side_effect=AssertionError('metrics loop started'))
    monkeypatch.delenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED,
                       raising=False)
    monkeypatch.setattr(runtime, '_BackgroundLoop', start)

    result = runtime._start_metrics_background_loop(  # pylint: disable=protected-access
        'controller', '127.0.0.1', 9090)

    assert result is None
    start.assert_not_called()


@pytest.mark.parametrize('role', ['executor', 'controller'])
def test_split_role_metrics_require_multiprocess_directory(monkeypatch, role):
    monkeypatch.setenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED, 'true')
    monkeypatch.delenv('PROMETHEUS_MULTIPROC_DIR', raising=False)

    with pytest.raises(RuntimeError, match='requires PROMETHEUS_MULTIPROC_DIR'):
        runtime._start_metrics_background_loop(  # pylint: disable=protected-access
            role, '127.0.0.1', 9090)


def test_controller_metrics_start_before_leadership_and_stop_after_role(
        monkeypatch):
    events = []
    metrics_background = mock.Mock()
    metrics_background.stop.side_effect = lambda: events.append('metrics-stop')
    plugin = mock.Mock()
    plugin.shutdown.side_effect = lambda: events.append('plugin-stop')
    state = runtime.RuntimeState('controller', mock.Mock(), mock.Mock(), False)
    monkeypatch.setattr(
        runtime, '_start_metrics_background_loop', lambda *args:
        (events.append('metrics-start') or metrics_background))
    monkeypatch.setattr(runtime, '_run_controller_role',
                        lambda *args: events.append('controller-role'))
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [plugin])

    runtime.run_role(state, _args())

    assert events == [
        'metrics-start', 'controller-role', 'metrics-stop', 'plugin-stop'
    ]


@pytest.mark.parametrize('role', ['executor', 'controller'])
def test_role_health_and_metrics_ports_must_differ(monkeypatch, role):
    args = SimpleNamespace(host='127.0.0.1',
                           port=46580,
                           deploy=True,
                           metrics_port=9090,
                           role=role,
                           role_health_port=9090,
                           authority_preflight_port=46583)
    parser = mock.Mock()
    parser.parse_args.return_value = args
    monkeypatch.setenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED, 'true')
    # main() intentionally publishes the selected role process-wide. Register
    # the variables with monkeypatch first so this validation-only invocation
    # cannot leak a split role into later tests on the same xdist worker.
    monkeypatch.setenv(runtime.request_postgres.SERVER_ROLE_ENV_VAR, 'all')
    monkeypatch.setenv(runtime.constants.ENV_VAR_IS_SKYPILOT_SERVER, 'true')
    monkeypatch.setattr(runtime, '_build_parser', lambda: parser)

    with pytest.raises(ValueError,
                       match='role-health-port and metrics-port cannot be'):
        runtime.main()


def test_api_background_uses_release_scoped_retirement_singleton(monkeypatch):
    background = mock.Mock()
    singleton_coroutine = mock.sentinel.singleton_coroutine
    scope = SimpleNamespace(singleton_name=(
        'resource-action-authority-retirement:installation:release-digest'))
    from_environment = mock.Mock(return_value=scope)
    run_singleton = mock.Mock(return_value=singleton_coroutine)
    monkeypatch.delenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED,
                       raising=False)
    monkeypatch.setenv(
        serve_constants.RESOURCE_ACTION_AUTHORITY_ENABLED_ENV_VAR, 'true')
    monkeypatch.setattr(runtime, '_BackgroundLoop', lambda: background)
    monkeypatch.setattr(runtime, '_uses_postgres_requests', lambda: True)
    monkeypatch.setattr(
        runtime.authority_worker_retirement.AuthorityWorkerRetirementScope,
        'from_environment', from_environment)
    monkeypatch.setattr(runtime.request_postgres, 'run_distributed_singleton',
                        run_singleton)

    result = runtime._start_background_loop(  # pylint: disable=protected-access
        'api')

    assert result is background
    from_environment.assert_called_once_with()
    run_singleton.assert_called_once_with(
        'skypilot:api-server-runtime:v1:' + scope.singleton_name,
        runtime.authority_worker_retirement.retirement_verifier_daemon)
    background.create_task.assert_called_once_with(singleton_coroutine)
    background.start.assert_called_once_with()


def test_api_background_ignores_legacy_authority_envs_while_disabled(
        monkeypatch):
    background = mock.Mock()
    monkeypatch.delenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED,
                       raising=False)
    monkeypatch.delenv(
        serve_constants.RESOURCE_ACTION_AUTHORITY_ENABLED_ENV_VAR,
        raising=False)
    monkeypatch.setenv(
        runtime.authority_worker_retirement.INSTALLATION_ID_ENV_VAR,
        'compatibility-installation')
    monkeypatch.setenv(
        runtime.authority_worker_retirement.COHORT_SUFFIXES_ENV_VAR, '[]')
    monkeypatch.setenv(
        runtime.authority_worker_retirement.RETIREMENT_TOMBSTONES_ENV_VAR, '[]')
    monkeypatch.setattr(runtime, '_BackgroundLoop', lambda: background)
    monkeypatch.setattr(runtime, '_uses_postgres_requests', lambda: True)
    from_environment = mock.Mock(
        side_effect=AssertionError('disabled scope was parsed'))
    monkeypatch.setattr(
        runtime.authority_worker_retirement.AuthorityWorkerRetirementScope,
        'from_environment', from_environment)

    result = runtime._start_background_loop(  # pylint: disable=protected-access
        'api')

    assert result is background
    from_environment.assert_not_called()
    background.create_task.assert_not_called()
    background.start.assert_called_once_with()


def test_api_background_enabled_authority_requires_complete_scope(monkeypatch):
    background = mock.Mock()
    monkeypatch.delenv(runtime.constants.ENV_VAR_SERVER_METRICS_ENABLED,
                       raising=False)
    monkeypatch.setenv(
        serve_constants.RESOURCE_ACTION_AUTHORITY_ENABLED_ENV_VAR, 'true')
    for name in (
            runtime.authority_worker_retirement.INSTALLATION_ID_ENV_VAR,
            runtime.authority_worker_retirement.COHORT_SUFFIXES_ENV_VAR,
            runtime.authority_worker_retirement.RETIREMENT_TOMBSTONES_ENV_VAR):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(runtime, '_BackgroundLoop', lambda: background)
    monkeypatch.setattr(runtime, '_uses_postgres_requests', lambda: True)

    with pytest.raises(ValueError, match='environment is incomplete'):
        runtime._start_background_loop(  # pylint: disable=protected-access
            'api')

    background.start.assert_not_called()
