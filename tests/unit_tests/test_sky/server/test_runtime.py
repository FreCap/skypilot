"""Role-isolation tests for the API server process supervisors."""

import http.client
import json
import os
import pathlib
import socket
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.jobs import controller_slots
from sky.jobs import managed_job_refresh_thread
from sky.jobs.server import server as jobs_server
from sky.server import runtime
from sky.server.requests import cutover as request_cutover
from sky.server.requests import registry as request_registry
from sky.utils import controller_capability

_SYSTEM_OOM_METRIC_CHILD_SCRIPT = """
from sky.serve import system_oom_recovery_observability as observability

observability.record(
    'recovery_started', provider='aws', market='on_demand')
"""


class _BackgroundLoop:
    """Minimal owned-loop fake for role-wiring tests."""

    def __init__(self) -> None:
        self.stopped = False
        self.run_coroutines = []

    def run(self, coroutine) -> None:
        self.run_coroutines.append(coroutine.cr_code.co_name)
        coroutine.close()

    def stop(self) -> None:
        self.stopped = True


def _args() -> SimpleNamespace:
    return SimpleNamespace(host='127.0.0.1',
                           metrics_port=9090,
                           role_health_port=46581)


@pytest.fixture(autouse=True)
def _stub_controller_slot_supervisor(monkeypatch):
    """Runtime unit tests never launch real managed-job process families."""
    controller_capability.clear_process_local()
    monkeypatch.setattr(runtime.controller_capability,
                        'make_process_non_dumpable', mock.Mock())
    supervisor = mock.Mock()
    factory = mock.Mock(return_value=supervisor)
    monkeypatch.setattr(controller_slots, 'ManagedJobControllerSlotSupervisor',
                        factory)
    authority = mock.Mock()
    authority.capability = 'A' * 43
    authority.path = '/tmp/test-controller-origin-authority'
    monkeypatch.setattr(controller_slots,
                        'LocalControllerOriginCapabilityAuthority',
                        mock.Mock(return_value=authority))
    yield supervisor, factory
    controller_capability.clear_process_local()


def test_runtime_capability_is_process_local_and_hidden_from_exec_child():
    capability = 'A' * 43
    script = f"""
import ctypes
import json
import os
import subprocess
import sys
from sky.server import runtime
from sky.utils import controller_capability

capability = {capability!r}
os.environ['SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY'] = capability
os.environ['SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH'] = '/old/path'
runtime._publish_controller_origin_capability(capability)
child_script = '''
import json
import os
from sky.utils import controller_capability
try:
    ancestor_environ = open(f"/proc/{{os.getppid()}}/environ", "rb").read().decode("utf-8", "replace")
except OSError as error:
    ancestor_environ = type(error).__name__
print(json.dumps({{
    "capability": controller_capability.get_process_local(),
    "environment": dict(os.environ),
    "ancestor_environ": ancestor_environ,
}}))
'''
child = subprocess.run([sys.executable, '-c', child_script], capture_output=True,
                       text=True, check=True)
print(json.dumps({{
    'registry': controller_capability.get_process_local(),
    'environment': dict(os.environ),
    'dumpable': ctypes.CDLL(None).prctl(3, 0, 0, 0, 0),
    'child': json.loads(child.stdout.splitlines()[-1]),
}}))
"""

    result = subprocess.run([sys.executable, '-c', script],
                            capture_output=True,
                            text=True,
                            check=True)
    proof = json.loads(result.stdout.splitlines()[-1])

    assert proof['registry'] == capability
    assert proof['dumpable'] == 0
    assert capability not in json.dumps(proof['environment'])
    assert proof['child']['capability'] is None
    assert capability not in json.dumps(proof['child']['environment'])
    assert capability not in proof['child']['ancestor_environ']


def test_controller_owns_one_distributed_handoff_retention_task(monkeypatch):
    created_tasks = []

    class FakeBackgroundLoop:

        def __init__(self) -> None:
            self.started = False

        def create_task(self, task):
            created_tasks.append(task)

        def start(self) -> None:
            self.started = True

    singleton_task = mock.Mock(
        side_effect=lambda name, task_factory: (name, task_factory))
    monkeypatch.setattr(runtime, '_BackgroundLoop', FakeBackgroundLoop)
    monkeypatch.setattr(runtime, '_uses_postgres_requests', lambda: True)
    monkeypatch.setattr(runtime, '_singleton_task', singleton_task)

    # pylint: disable-next=protected-access
    background = runtime._start_background_loop('controller')

    retention_task = ('serve-ordinary-launch-handoff-retention',
                      runtime.ordinary_launch_handoff.retention_daemon)
    assert created_tasks.count(retention_task) == 1
    singleton_task.assert_any_call(*retention_task)
    assert background.started


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


def test_role_drain_monitor_publishes_before_requesting_shutdown(tmp_path):
    drain_marker = tmp_path / 'draining'
    lease = mock.Mock()

    def assert_drain_was_published() -> bool:
        lease.begin_draining.assert_called_once_with()
        return True

    shutdown_requested = mock.Mock(side_effect=assert_drain_was_published)
    monitor = runtime._RoleDrainMarkerMonitor(  # pylint: disable=protected-access
        lease,
        shutdown_requested,
        marker_path=str(drain_marker),
        poll_seconds=0.001)
    monitor.start()
    try:
        drain_marker.touch()
        deadline = time.monotonic() + 2
        while not shutdown_requested.called and time.monotonic() < deadline:
            time.sleep(0.001)
    finally:
        monitor.stop()

    shutdown_requested.assert_called_once_with()


def test_role_drain_monitor_still_shuts_down_when_publication_fails(tmp_path):
    drain_marker = tmp_path / 'draining'
    lease = mock.Mock()
    lease.begin_draining.side_effect = RuntimeError('database unavailable')
    shutdown_requested = mock.Mock()
    monitor = runtime._RoleDrainMarkerMonitor(  # pylint: disable=protected-access
        lease,
        shutdown_requested,
        marker_path=str(drain_marker),
        poll_seconds=0.001)
    monitor.start()
    try:
        drain_marker.touch()
        deadline = time.monotonic() + 2
        while not shutdown_requested.called and time.monotonic() < deadline:
            time.sleep(0.001)
    finally:
        monitor.stop()

    lease.begin_draining.assert_called_once_with()
    shutdown_requested.assert_called_once_with()


def test_role_drain_monitor_bounds_hung_publication(tmp_path):
    drain_marker = tmp_path / 'draining'
    publication_release = threading.Event()
    lease = mock.Mock()
    lease.begin_draining.side_effect = publication_release.wait
    shutdown_requested = mock.Mock()
    monitor = runtime._RoleDrainMarkerMonitor(  # pylint: disable=protected-access
        lease,
        shutdown_requested,
        marker_path=str(drain_marker),
        poll_seconds=0.001,
        publication_wait_seconds=0.01)
    monitor.start()
    try:
        drain_marker.touch()
        deadline = time.monotonic() + 2
        while not shutdown_requested.called and time.monotonic() < deadline:
            time.sleep(0.001)
    finally:
        monitor.stop()
        publication_release.set()

    shutdown_requested.assert_called_once_with()


def test_role_drain_monitor_waits_for_role_signal_handler(tmp_path):
    drain_marker = tmp_path / 'draining'
    lease = mock.Mock()
    shutdown_requested = mock.Mock(side_effect=[False, True])
    monitor = runtime._RoleDrainMarkerMonitor(  # pylint: disable=protected-access
        lease,
        shutdown_requested,
        marker_path=str(drain_marker),
        poll_seconds=0.001)
    monitor.start()
    try:
        drain_marker.touch()
        deadline = time.monotonic() + 2
        while shutdown_requested.call_count < 2 and time.monotonic() < deadline:
            time.sleep(0.001)
    finally:
        monitor.stop()

    lease.begin_draining.assert_called_once_with()
    assert shutdown_requested.call_count == 2


def test_runtime_shutdown_waits_until_sigterm_handler_is_installed(monkeypatch):
    terminate = mock.Mock()
    monkeypatch.setattr(runtime.signal, 'getsignal',
                        lambda signum: runtime.signal.SIG_DFL)
    monkeypatch.setattr(runtime.os, 'kill', terminate)

    assert not runtime._request_runtime_shutdown_when_ready(  # pylint: disable=protected-access
    )
    terminate.assert_not_called()


def test_stale_drain_marker_is_cleared_without_erasing_current_drain(
        monkeypatch, tmp_path):
    drain_marker = tmp_path / 'draining'
    monkeypatch.setattr(runtime.request_storage, 'ROLE_DRAIN_MARKER_PATH',
                        str(drain_marker))
    drain_marker.touch()
    marker_time = drain_marker.stat().st_mtime

    assert runtime.request_storage.clear_stale_role_drain_marker(marker_time +
                                                                 1)
    assert not drain_marker.exists()

    drain_marker.touch()
    marker_time = drain_marker.stat().st_mtime
    assert not runtime.request_storage.clear_stale_role_drain_marker(
        marker_time - 1)
    assert drain_marker.exists()


def test_stale_drain_marker_cleanup_revalidates_before_unlink(
        monkeypatch, tmp_path):
    drain_marker = tmp_path / 'draining'
    monkeypatch.setattr(runtime.request_storage, 'ROLE_DRAIN_MARKER_PATH',
                        str(drain_marker))
    drain_marker.touch()
    original_stat = os.stat
    stale_stat = original_stat(drain_marker)
    calls = 0

    def stat_with_concurrent_prestop(path):
        nonlocal calls
        calls += 1
        if calls == 2:
            os.utime(drain_marker,
                     ns=(stale_stat.st_atime_ns,
                         stale_stat.st_mtime_ns + 1_000_000_000))
        return original_stat(path)

    monkeypatch.setattr(runtime.request_storage.os, 'stat',
                        stat_with_concurrent_prestop)

    assert not runtime.request_storage.clear_stale_role_drain_marker(
        stale_stat.st_mtime + 1)
    assert original_stat(drain_marker)


def test_early_execution_fence_attempts_every_boundary_and_reports_failure():
    failed_worker = mock.Mock()
    failed_worker.request_shutdown.side_effect = RuntimeError('fence failed')
    healthy_worker = mock.Mock()
    managed_refresh = mock.Mock()
    managed_slots = mock.Mock()

    fenced = runtime._fence_execution_admission(  # pylint: disable=protected-access
        [failed_worker, healthy_worker],
        managed_job_refresh=managed_refresh,
        managed_job_slots=managed_slots)

    assert not fenced
    failed_worker.request_shutdown.assert_called_once_with()
    healthy_worker.request_shutdown.assert_called_once_with()
    managed_refresh.request_shutdown.assert_called_once_with()
    managed_slots.request_shutdown.assert_called_once_with()


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
    lifecycle = mock.Mock()
    background = _BackgroundLoop()
    lease = mock.Mock()
    config = mock.Mock()
    queue_server = mock.Mock()
    worker = mock.Mock()
    lifecycle.attach_mock(worker.request_shutdown, 'fence_claims')
    lifecycle.attach_mock(lease.set_ready, 'set_ready')
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
    assert lease.set_ready.call_args_list == [
        mock.call(True,
                  health_detail={
                      'phase': 'claiming',
                      'long_workers': mock.ANY,
                      'short_workers': mock.ANY,
                  }),
        mock.call(False, health_detail={'phase': 'draining'}),
    ]
    draining = mock.call.set_ready(False, health_detail={'phase': 'draining'})
    assert lifecycle.mock_calls.index(
        mock.call.fence_claims()) < lifecycle.mock_calls.index(draining)
    health_server.start.assert_called_once_with()
    health_server.stop.assert_called_once_with()
    lease.stop.assert_called_once_with()
    run_in_parallel.assert_called_once()
    queue_server.kill.assert_called_once_with()
    queue_server.join.assert_called_once_with()
    assert background.stopped


def test_all_mode_awaits_managed_job_cutover_before_workers(monkeypatch):
    background = _BackgroundLoop()
    state = runtime.RuntimeState('all', mock.Mock(), None, False)
    queue_server = mock.Mock()
    worker = mock.Mock()
    managed_refresh = mock.Mock()

    def start_after_cutover(*args, **kwargs):
        del args, kwargs
        managed_refresh.wait_for_cutover.assert_called_once_with()
        return queue_server, [worker]

    start_workers = mock.Mock(side_effect=start_after_cutover)
    monkeypatch.setattr(runtime, '_start_background_loop',
                        lambda *args: background)
    monkeypatch.setattr(runtime.managed_job_utils, 'is_consolidation_mode',
                        lambda: True)
    monkeypatch.setattr(runtime.executor, 'start', start_workers)
    monkeypatch.setattr(runtime, '_run_uvicorn', mock.Mock())
    monkeypatch.setattr(managed_job_refresh_thread,
                        'start_managed_job_refresh_daemon',
                        mock.Mock(return_value=managed_refresh))
    monkeypatch.setattr(runtime, '_request_worker_shutdown', mock.Mock())
    monkeypatch.setattr(runtime, '_stop_queue_server', mock.Mock())
    monkeypatch.setattr(runtime.clean_env_module, 'capture_clean_server_env',
                        mock.Mock())
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    runtime.run_role(state, _args())

    managed_refresh.wait_for_cutover.assert_called_once_with()
    start_workers.assert_called_once_with(state.config, execution_classes=None)


def test_postgres_all_mode_fences_and_recovers_before_admission(
        monkeypatch, _stub_controller_slot_supervisor):
    supervisor, _ = _stub_controller_slot_supervisor
    events = []
    background = _BackgroundLoop()
    instance_lease = mock.Mock()
    instance_lease.instance_id = '00000000-0000-0000-0000-000000000001'
    state = runtime.RuntimeState('all', mock.Mock(), instance_lease, False)
    queue_server = mock.Mock()
    worker = mock.Mock()
    managed_refresh = mock.Mock()
    leader = mock.Mock()
    leader.instance_id = instance_lease.instance_id
    leader.generation = 37
    leader.origin_capability = 'A' * 43
    leader.try_acquire.side_effect = lambda: (events.append('acquire') or True)
    request_backend = mock.Mock(
        spec=runtime.request_postgres.PostgresRequestBackend)
    request_backend.retire_legacy_internal_daemon_rows.side_effect = (
        lambda **kwargs: events.append(('retire', kwargs)) or 2)
    request_backend.recover_on_startup.side_effect = (
        lambda **kwargs: events.append(('recover', kwargs)) or True)

    class Transition:

        def __enter__(self):
            events.append('transition-enter')

        def __exit__(self, exc_type, exc_value, traceback):
            del exc_type, exc_value, traceback
            events.append('transition-exit')

    start_workers = mock.Mock(side_effect=lambda *args, **kwargs: (
        events.append(('workers', args, kwargs)) or (queue_server, [worker])))
    managed_refresh.wait_for_cutover.side_effect = lambda: events.append(
        'refresh-cutover')
    supervisor.start.side_effect = lambda: events.append('slots-start')

    start_background = mock.Mock(side_effect=lambda *args, **kwargs: (
        events.append('background-start') or background))
    monkeypatch.setattr(runtime, '_start_background_loop', start_background)
    monkeypatch.setattr(runtime.managed_job_utils, 'is_consolidation_mode',
                        lambda: True)
    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda *args: leader)
    monkeypatch.setattr(runtime.request_storage, 'get_request_backend',
                        lambda: request_backend)
    monkeypatch.setattr(runtime.request_postgres, 'legacy_daemon_transition',
                        Transition)
    monkeypatch.setattr(
        runtime.request_postgres, 'fence_stale_controller_claims',
        lambda *owner: events.append(('fence', owner)) or {
            'replayed': 1,
            'interrupted': 2,
        })
    monkeypatch.setattr(runtime.executor, 'start', start_workers)
    monkeypatch.setattr(runtime.executor, 'reenqueue_recovered_requests',
                        lambda: events.append('reenqueue'))
    monkeypatch.setattr(runtime, '_run_uvicorn', mock.Mock())
    monkeypatch.setattr(runtime, '_start_surface_interrupted_cluster_launches',
                        lambda: events.append('surface-recovery'))
    monkeypatch.setattr(
        managed_job_refresh_thread, 'start_managed_job_refresh_daemon',
        mock.Mock(side_effect=lambda:
                  (events.append('refresh-start') or managed_refresh)))
    monkeypatch.setattr(runtime, '_request_worker_shutdown', mock.Mock())
    monkeypatch.setattr(runtime, '_stop_queue_server', mock.Mock())
    monkeypatch.setattr(runtime.clean_env_module, 'capture_clean_server_env',
                        lambda: events.append('capture-env'))
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    runtime.run_role(state, _args())

    managed_refresh.wait_for_cutover.assert_called_once_with()
    start_workers.assert_called_once_with(state.config,
                                          execution_classes=None,
                                          controller_generation=37)
    owner = (leader.instance_id, 37)
    assert events[:11] == [
        'acquire',
        'transition-enter',
        ('retire', {
            'controller_owner': owner
        }),
        ('fence', owner),
        ('recover', {
            'controller_owner': owner
        }),
        'transition-exit',
        'surface-recovery',
        'capture-env',
        'background-start',
        'refresh-start',
        'refresh-cutover',
    ]
    assert events.index('slots-start') < next(
        index for index, event in enumerate(events)
        if isinstance(event, tuple) and event[0] == 'workers')
    assert events.index('transition-exit') < events.index('slots-start')
    assert events.index('transition-exit') < events.index('background-start')
    assert events.index('reenqueue') > next(
        index for index, event in enumerate(events)
        if isinstance(event, tuple) and event[0] == 'workers')
    assert '_monitor_compat_controller_leadership' not in (
        background.run_coroutines)
    start_background.assert_called_once_with(
        'all', compatibility_controller_lease=leader)
    leader.release.assert_called_once_with()


def test_compat_leadership_monitor_is_owned_before_background_start(
        monkeypatch):
    events = []

    class RecordingBackgroundLoop:

        def create_task(self, coroutine):
            events.append(('create', coroutine.cr_code.co_name))
            coroutine.close()

        def start(self):
            events.append(('start', None))

    leader = mock.Mock()
    monkeypatch.setattr(runtime, '_BackgroundLoop', RecordingBackgroundLoop)
    monkeypatch.setattr(runtime, '_uses_postgres_requests', lambda: False)

    result = runtime._start_background_loop(  # pylint: disable=protected-access
        'all',
        compatibility_controller_lease=leader)

    assert isinstance(result, RecordingBackgroundLoop)
    monitor_event = ('create', '_monitor_compat_controller_leadership')
    assert monitor_event in events
    assert events.index(monitor_event) < events.index(('start', None))


def test_postgres_all_mode_without_consolidation_still_owns_mixed_queue(
        monkeypatch, _stub_controller_slot_supervisor):
    _, slot_factory = _stub_controller_slot_supervisor
    background = _BackgroundLoop()
    instance_lease = mock.Mock()
    instance_lease.instance_id = '00000000-0000-0000-0000-000000000001'
    state = runtime.RuntimeState('all', mock.Mock(), instance_lease, False)
    leader = mock.Mock()
    leader.instance_id = instance_lease.instance_id
    leader.generation = 41
    leader.origin_capability = 'A' * 43
    leader.try_acquire.return_value = True
    request_backend = mock.Mock(
        spec=runtime.request_postgres.PostgresRequestBackend)
    request_backend.recover_on_startup.return_value = False
    queue_server = mock.Mock()
    worker = mock.Mock()
    start_workers = mock.Mock(return_value=(queue_server, [worker]))

    class Transition:

        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc_value, traceback):
            del exc_type, exc_value, traceback

    monkeypatch.setattr(runtime, '_start_metrics_background_loop',
                        lambda *args: None)
    monkeypatch.setattr(runtime, '_start_background_loop',
                        lambda *args, **kwargs: background)
    monkeypatch.setattr(runtime.managed_job_utils, 'is_consolidation_mode',
                        lambda: False)
    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda *args: leader)
    monkeypatch.setattr(runtime.request_storage, 'get_request_backend',
                        lambda: request_backend)
    monkeypatch.setattr(runtime.request_postgres, 'legacy_daemon_transition',
                        Transition)
    monkeypatch.setattr(runtime.request_postgres,
                        'fence_stale_controller_claims', lambda *owner: {
                            'replayed': 0,
                            'interrupted': 0,
                        })
    monkeypatch.setattr(runtime.executor, 'start', start_workers)
    monkeypatch.setattr(runtime, '_run_uvicorn', mock.Mock())
    monkeypatch.setattr(runtime, '_start_surface_interrupted_cluster_launches',
                        mock.Mock())
    start_refresh = mock.Mock(return_value=None)
    monkeypatch.setattr(managed_job_refresh_thread,
                        'start_managed_job_refresh_daemon', start_refresh)
    monkeypatch.setattr(runtime, '_request_worker_shutdown', mock.Mock())
    monkeypatch.setattr(runtime, '_stop_queue_server', mock.Mock())
    monkeypatch.setattr(runtime.clean_env_module, 'capture_clean_server_env',
                        mock.Mock())
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    runtime.run_role(state, _args())

    leader.try_acquire.assert_called_once_with()
    start_workers.assert_called_once_with(state.config,
                                          execution_classes=None,
                                          controller_generation=41)
    start_refresh.assert_called_once_with()
    slot_factory.assert_not_called()
    leader.release.assert_called_once_with()


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
    leader.origin_capability = 'A' * 43
    leader.try_acquire.return_value = True
    leader.backend_pid.return_value = 123
    leader.heartbeat.return_value = False
    health_server = mock.Mock()
    queue_server = mock.Mock()
    worker = mock.Mock()
    managed_refresh = mock.Mock()

    def start_workers_after_cutover(*args, **kwargs):
        del args, kwargs
        managed_refresh.wait_for_cutover.assert_called_once_with()
        return queue_server, [worker]

    start_workers = mock.Mock(side_effect=start_workers_after_cutover)
    start_refresh = mock.Mock(return_value=managed_refresh)
    shutdown_workers = mock.Mock()
    stop_queue = mock.Mock()
    surface_scan = mock.Mock()
    fence_claims = mock.Mock(return_value={'replayed': 2, 'interrupted': 1})
    retire_daemons = mock.Mock()

    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda *args: leader)
    monkeypatch.setattr(runtime.request_postgres,
                        'fence_stale_controller_claims', fence_claims)
    monkeypatch.setattr(
        runtime.request_storage, 'get_request_backend',
        lambda: mock.Mock(retire_legacy_internal_daemon_rows=retire_daemons))
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
    managed_refresh.wait_for_cutover.assert_called_once_with()
    surface_scan.assert_called_once_with()
    worker.request_shutdown.assert_called_once_with()
    shutdown_workers.assert_called_once_with([worker],
                                             terminate_children=True,
                                             request_stop=False)
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


def test_controller_retires_daemon_rows_before_fencing(monkeypatch):
    instance_lease = mock.Mock()
    instance_lease.instance_id = '00000000-0000-0000-0000-000000000001'
    state = runtime.RuntimeState('controller', mock.Mock(), instance_lease,
                                 False)
    leader = mock.Mock(instance_id=instance_lease.instance_id, generation=7)
    leader.origin_capability = 'A' * 43
    leader.try_acquire.return_value = True
    lifecycle = mock.Mock()
    backend = mock.Mock()
    lifecycle.attach_mock(backend.retire_legacy_internal_daemon_rows, 'retire')
    fence = mock.Mock(side_effect=RuntimeError('stop after ordering proof'))
    lifecycle.attach_mock(fence, 'fence')
    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda *args: leader)
    monkeypatch.setattr(runtime.request_postgres,
                        'recent_legacy_controller_consumers', lambda *args: [])
    monkeypatch.setattr(runtime.request_storage, 'get_request_backend',
                        lambda: backend)
    monkeypatch.setattr(runtime.request_postgres,
                        'fence_stale_controller_claims', fence)
    monkeypatch.setattr(runtime, '_RoleHealthServer', lambda *args: mock.Mock())
    monkeypatch.setattr(runtime.plugins, 'get_plugins', lambda: [])

    with pytest.raises(RuntimeError, match='ordering proof'):
        runtime.run_role(state, _args())

    assert lifecycle.mock_calls[:2] == [
        mock.call.retire(),
        mock.call.fence(instance_lease.instance_id, 7)
    ]


def test_controller_role_becomes_unready_before_graceful_release(monkeypatch):
    background = _BackgroundLoop()
    instance_lease = mock.Mock()
    instance_lease.instance_id = '00000000-0000-0000-0000-000000000001'
    state = runtime.RuntimeState('controller', mock.Mock(), instance_lease,
                                 False)
    leader = mock.Mock()
    leader.instance_id = instance_lease.instance_id
    leader.generation = 8
    leader.origin_capability = 'A' * 43
    leader.try_acquire.return_value = True
    leader.backend_pid.return_value = 123
    health_server = mock.Mock()
    worker = mock.Mock()
    queue_server = mock.Mock()
    lifecycle = mock.Mock()
    lifecycle.attach_mock(instance_lease.set_ready, 'set_ready')
    lifecycle.attach_mock(worker.request_shutdown, 'fence_claims')
    background.stop = mock.Mock(
        side_effect=lambda: setattr(background, 'stopped', True))
    lifecycle.attach_mock(background.stop, 'background_stop')
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
    assert lifecycle.mock_calls.index(
        mock.call.fence_claims()) < lifecycle.mock_calls.index(draining)
    assert lifecycle.mock_calls.index(draining) < lifecycle.mock_calls.index(
        mock.call.release())
    assert lifecycle.mock_calls.index(
        mock.call.background_stop()) < lifecycle.mock_calls.index(
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
    leader.origin_capability = 'A' * 43
    leader.try_acquire.return_value = True
    leader.backend_pid.return_value = 123
    health_server = mock.Mock()
    queue_server = mock.Mock()
    worker = mock.Mock()
    recent_consumers = mock.Mock(side_effect=[[], [], ['legacy-executor']])
    shutdown_workers = mock.Mock()
    stop_queue = mock.Mock()

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
    shutdown_workers.assert_called_once_with([worker],
                                             terminate_children=True,
                                             request_stop=False)
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


def test_controller_role_metrics_expose_only_explicit_system_oom_sample(
        monkeypatch, tmp_path):
    """Exercise steady-state counter emission through the real endpoint."""
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
        assert samples == [
            'sky_serve_system_oom_recovery_events_total{'
            'event="recovery_started",market="on_demand",provider="aws"} 1.0'
        ], payload
        for removed_event in (
                'authorization_v1_selected',
                'authorization_v2_selected',
                'runtime_capability_v1_observed',
                'status_only_read',
        ):
            assert f'event="{removed_event}"' not in payload
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
                           role_health_port=9090)
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
