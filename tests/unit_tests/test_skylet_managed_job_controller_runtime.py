"""Lifecycle tests for the remote managed-job controller runtime."""
# pylint: disable=protected-access

import json
import os
import pathlib
import stat
from unittest import mock
import uuid

import pytest

from sky.jobs import constants as managed_job_constants
from sky.jobs import controller_fencing
from sky.jobs import controller_slots
from sky.skylet import configs
from sky.skylet import constants as skylet_constants
from sky.utils import controller_capability


@pytest.fixture
def skylet_module(monkeypatch, tmp_path):
    """Import the daemon with worker-private state for its global events."""
    monkeypatch.setenv(skylet_constants.SKY_RUNTIME_DIR_ENV_VAR_KEY,
                       str(tmp_path))
    monkeypatch.setattr(configs, '_DB_PATH', None)
    from sky.skylet import skylet  # pylint: disable=import-outside-toplevel
    return skylet


class _FakeLocalRuntime:
    """Minimal recording implementation of the local runtime contract."""

    def __init__(self, events, *, start_error=None, failure_error=None):
        self.events = events
        self.start_error = start_error
        self.failure_error = failure_error
        self.started = False

    def start(self):
        self.events.append('runtime.start')
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    def raise_if_failed(self):
        self.events.append('runtime.raise_if_failed')
        if self.failure_error is not None:
            raise self.failure_error

    def request_shutdown(self):
        self.events.append('runtime.request_shutdown')

    def wait_for_shutdown(self):
        self.events.append('runtime.wait_for_shutdown')


def test_remote_runtime_is_marker_gated(skylet_module, monkeypatch, tmp_path):
    indicator = tmp_path / 'is_jobs_controller'
    monkeypatch.setattr(managed_job_constants, 'JOB_CONTROLLER_INDICATOR_FILE',
                        str(indicator))
    local_runtime = mock.Mock()
    monkeypatch.setattr(controller_slots, 'LocalManagedJobControllerRuntime',
                        local_runtime)
    runtime = skylet_module._RemoteManagedJobControllerRuntime()

    runtime.start_if_configured()

    assert not runtime.started
    local_runtime.assert_not_called()


def test_event_loop_detects_marker_created_after_skylet_start(
        skylet_module, monkeypatch, tmp_path):
    indicator = tmp_path / 'is_jobs_controller'
    monkeypatch.setattr(managed_job_constants, 'JOB_CONTROLLER_INDICATOR_FILE',
                        str(indicator))
    events = []
    local_runtime = _FakeLocalRuntime(events)
    monkeypatch.setattr(controller_slots, 'LocalManagedJobControllerRuntime',
                        lambda **kwargs: local_runtime)
    monkeypatch.setattr(skylet_module, 'EVENTS', [])
    monkeypatch.setattr(skylet_module, '_MANAGED_JOB_RUNTIME_POLL_SECONDS', 0)
    runtime = skylet_module._RemoteManagedJobControllerRuntime()
    waits = 0

    class LoopComplete(Exception):
        pass

    def bounded_wait(timeout):
        nonlocal waits
        assert timeout == 0
        waits += 1
        if waits == 1:
            pathlib.Path(indicator).touch()
            return
        raise LoopComplete

    monkeypatch.setattr(runtime, 'wait', bounded_wait)

    with pytest.raises(LoopComplete):
        skylet_module.run_event_loop(runtime)

    assert waits == 2
    assert events == ['runtime.start', 'runtime.raise_if_failed']
    assert runtime.started


def test_slot_failure_wakes_event_loop_wait(skylet_module):
    failure = RuntimeError('slot family failed')
    events = []
    local_runtime = _FakeLocalRuntime(events, failure_error=failure)
    runtime = skylet_module._RemoteManagedJobControllerRuntime()
    runtime._runtime = local_runtime

    runtime._on_failure()

    with pytest.raises(RuntimeError, match='slot family failed'):
        runtime.wait(timeout=60)
    assert runtime._failure.is_set()
    assert events == ['runtime.raise_if_failed']


def test_main_starts_existing_marker_before_grpc_and_drains_in_order(
        skylet_module, monkeypatch):
    events = []
    runtime = mock.Mock()
    grpc_server = mock.Mock()

    runtime.start_if_configured.side_effect = lambda: events.append(
        'runtime.start_if_configured')
    runtime.request_shutdown.side_effect = lambda: events.append(
        'runtime.request_shutdown')
    runtime.wait_for_shutdown.side_effect = lambda: events.append(
        'runtime.wait_for_shutdown')
    grpc_server.stop.side_effect = lambda grace: events.append('grpc.stop')
    monkeypatch.setattr(skylet_module, '_RemoteManagedJobControllerRuntime',
                        lambda: runtime)
    monkeypatch.setattr(
        skylet_module, 'start_grpc_server', lambda port:
        (events.append('grpc.start') or grpc_server))
    monkeypatch.setattr(
        skylet_module, 'run_event_loop', lambda managed_job_runtime:
        (_ for _ in ()).throw(KeyboardInterrupt))
    monkeypatch.setattr(skylet_module.hook_executor, 'clear_teardown_claim',
                        lambda: None)
    monkeypatch.setattr(skylet_module,
                        '_should_install_preemption_sigterm_handler',
                        lambda: False)
    monkeypatch.setattr(skylet_module.sys, 'argv', ['skylet'])

    skylet_module.main()

    assert events == [
        'runtime.start_if_configured',
        'grpc.start',
        'grpc.stop',
        'runtime.request_shutdown',
        'runtime.wait_for_shutdown',
    ]


def _patch_local_runtime_dependencies(monkeypatch, events, supervisor):
    owner_uuid = uuid.UUID('12345678-1234-4abc-9234-56789abcdef0')
    owner = (str(owner_uuid), 12345)
    monkeypatch.setattr(controller_slots.uuid, 'uuid4', lambda: owner_uuid)
    monkeypatch.setattr(controller_slots, '_read_process_start_time_ticks',
                        lambda pid: owner[1])
    monkeypatch.setattr(controller_fencing, 'get_current_owner', lambda: None)
    monkeypatch.setattr(
        controller_fencing, 'publish_owner',
        lambda published, *, mode: events.append(('publish', published, mode)))

    class FakeAuthority:

        def __init__(self, published):
            assert published == owner
            self.capability = 'test-capability'
            self.path = '/test/authority.json'
            events.append(('authority.construct', published))

        def publish(self):
            events.append(('non-dumpable',))
            events.append(('authority.publish',))

        def remove(self):
            events.append(('authority.remove',))

    monkeypatch.setattr(controller_slots,
                        'LocalControllerOriginCapabilityAuthority',
                        FakeAuthority)
    monkeypatch.setattr(controller_slots.managed_job_state,
                        'reset_stale_jobs_for_current_controller', lambda:
                        (events.append(('reset', owner)) or 2))
    monkeypatch.setattr(controller_slots,
                        'ManagedJobControllerSlotSupervisor',
                        lambda published, slot_count=None, on_failure=None,
                        child_env=None, origin_capability=None: (events.append(
                            ('construct', published, slot_count, on_failure,
                             child_env, origin_capability)) or supervisor))
    return owner


def test_local_runtime_publishes_then_resets_then_starts_supervisor(
        monkeypatch):
    events = []
    supervisor = mock.Mock()
    supervisor.start.side_effect = lambda: events.append(('start',))
    owner = _patch_local_runtime_dependencies(monkeypatch, events, supervisor)
    on_failure = mock.Mock()
    runtime = controller_slots.LocalManagedJobControllerRuntime(
        on_failure=on_failure, slot_count=3)

    runtime.start()

    assert runtime.owner == owner
    assert runtime.started
    assert events == [
        ('authority.construct', owner),
        ('non-dumpable',),
        ('authority.publish',),
        ('publish', owner, controller_fencing.LOCAL_OWNER_MODE),
        ('construct', owner, 3, on_failure, None, 'test-capability'),
        ('reset', owner),
        ('start',),
    ]


def test_local_runtime_clears_owner_only_after_shutdown_proof(monkeypatch):
    events = []
    supervisor = mock.Mock()
    supervisor.start.side_effect = lambda: events.append(('start',))
    supervisor.request_shutdown.side_effect = lambda: events.append(
        ('request_shutdown',))
    supervisor.wait_for_shutdown.side_effect = lambda: events.append(
        ('wait_for_shutdown',))
    owner = _patch_local_runtime_dependencies(monkeypatch, events, supervisor)
    runtime = controller_slots.LocalManagedJobControllerRuntime()
    runtime.start()
    monkeypatch.setattr(controller_fencing, 'get_current_owner', lambda: owner)
    monkeypatch.setattr(controller_fencing, 'clear_owner',
                        lambda: events.append(('clear_owner',)))
    events.clear()

    runtime.request_shutdown()
    runtime.wait_for_shutdown()

    assert events == [
        ('request_shutdown',),
        ('wait_for_shutdown',),
        ('authority.remove',),
        ('clear_owner',),
    ]
    assert not runtime.started


def test_local_runtime_preserves_owner_when_shutdown_proof_fails(monkeypatch):
    events = []
    supervisor = mock.Mock()
    supervisor.start.side_effect = lambda: events.append(('start',))
    owner = _patch_local_runtime_dependencies(monkeypatch, events, supervisor)
    runtime = controller_slots.LocalManagedJobControllerRuntime()
    runtime.start()
    supervisor.wait_for_shutdown.side_effect = RuntimeError(
        'stable-empty proof unavailable')
    monkeypatch.setattr(controller_fencing, 'get_current_owner', lambda: owner)
    clear_owner = mock.Mock()
    monkeypatch.setattr(controller_fencing, 'clear_owner', clear_owner)

    with pytest.raises(RuntimeError, match='stable-empty proof unavailable'):
        runtime.wait_for_shutdown()

    clear_owner.assert_not_called()
    assert runtime.started


def test_remote_runtime_retains_partial_start_for_final_drain(
        skylet_module, monkeypatch, tmp_path):
    indicator = tmp_path / 'is_jobs_controller'
    indicator.touch()
    monkeypatch.setattr(managed_job_constants, 'JOB_CONTROLLER_INDICATOR_FILE',
                        str(indicator))
    events = []
    local_runtime = _FakeLocalRuntime(
        events, start_error=RuntimeError('partial slot admission failed'))
    monkeypatch.setattr(controller_slots, 'LocalManagedJobControllerRuntime',
                        lambda **kwargs: local_runtime)
    runtime = skylet_module._RemoteManagedJobControllerRuntime()

    with pytest.raises(RuntimeError, match='partial slot admission failed'):
        runtime.start_if_configured()
    runtime.request_shutdown()
    runtime.wait_for_shutdown()

    assert runtime._runtime is local_runtime
    assert events == [
        'runtime.start',
        'runtime.request_shutdown',
        'runtime.wait_for_shutdown',
    ]


def test_local_capability_authority_is_hash_only_private_and_removable(
        monkeypatch, tmp_path):
    instance_id = '12345678-1234-4abc-9234-56789abcdef0'
    owner = (instance_id, 12345)
    monkeypatch.setenv(skylet_constants.SKY_RUNTIME_DIR_ENV_VAR_KEY,
                       str(tmp_path))
    authority = controller_slots.LocalControllerOriginCapabilityAuthority(owner)

    authority.publish()

    path = pathlib.Path(authority.path)
    payload = json.loads(path.read_text(encoding='utf-8'))
    assert frozenset(payload) == frozenset({
        'controller_instance_id',
        'controller_generation',
        'origin_capability_sha256',
        'owner_pid',
        'owner_process_start_time_ticks',
    })
    assert payload['controller_instance_id'] == instance_id
    assert payload['controller_generation'] == owner[1]
    assert payload['origin_capability_sha256'] == (
        controller_capability.digest_hex(authority.capability))
    assert authority.capability not in path.read_text(encoding='utf-8')
    assert payload['owner_pid'] == os.getpid()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert controller_capability.verify_local_authority(str(path), owner[0],
                                                        owner[1],
                                                        authority.capability)
    assert controller_capability.get_process_local() == authority.capability
    assert managed_job_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR not in (
        os.environ)
    assert (managed_job_constants.
            CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR
            not in os.environ)

    authority.remove()

    assert not path.exists()
    assert managed_job_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR not in (
        os.environ)
    assert (managed_job_constants.
            CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR
            not in os.environ)
    assert controller_capability.get_process_local() is None


def test_local_capability_publish_preserves_unrelated_process_authority(
        monkeypatch, tmp_path):
    """A conflicting publication fails without clearing the current owner."""
    current = controller_capability.generate()
    controller_capability.install_process_local(current)
    monkeypatch.setenv(skylet_constants.SKY_RUNTIME_DIR_ENV_VAR_KEY,
                       str(tmp_path))
    authority = controller_slots.LocalControllerOriginCapabilityAuthority(
        ('12345678-1234-4abc-9234-56789abcdef0', 12345))
    try:
        with pytest.raises(RuntimeError, match='Another process-local'):
            authority.publish()
        authority.remove()
        assert controller_capability.get_process_local() == current
    finally:
        controller_capability.clear_process_local()
