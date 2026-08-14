"""Tests for runtime-owned managed-job controller slots."""
# pylint: disable=protected-access

import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import threading
import time
from unittest import mock

import pytest

from sky.jobs import controller_slots
from sky.jobs import managed_job_controller_runner
from sky.server.requests import requests as api_requests
from sky.server.requests import storage as request_storage
from sky.utils import controller_capability

_ORIGINAL_QUIESCE_NESTED_REQUESTS = (
    controller_slots.ManagedJobControllerSlotSupervisor._quiesce_nested_requests
)


@pytest.fixture(autouse=True)
def _stub_nested_request_quiescence(monkeypatch):
    """Keep slot unit tests independent from the process request database."""
    monkeypatch.setattr(controller_slots.ManagedJobControllerSlotSupervisor,
                        '_quiesce_nested_requests', lambda self, identity: 0)


def _family(slot_id: int, attempt: str) -> controller_slots._SlotFamily:
    identity = ('instance-a', 7, slot_id, attempt)
    process = mock.Mock(pid=10000 + slot_id)
    control = mock.Mock()
    return controller_slots._SlotFamily(identity, process, control)


def _admitted_family(slot_id: int,
                     attempt: str) -> controller_slots._SlotFamily:
    family = _family(slot_id, attempt)
    family.admitted = True
    family.admitted_at = time.monotonic()
    return family


def _wait_until(predicate, timeout: float = 20) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _process_identity(pid: int) -> tuple[int, int]:
    return pid, controller_slots._read_process_start_time_ticks(pid)


def _identity_exists(identity: tuple[int, int]) -> bool:
    pid, start_time_ticks = identity
    try:
        return (controller_slots._read_process_start_time_ticks(pid) ==
                start_time_ticks)
    except (FileNotFoundError, ProcessLookupError, OSError, ValueError):
        return False


def _direct_child_pids(pid: int) -> set[int]:
    """Read one process's direct children across all of its threads."""
    children: set[int] = set()
    try:
        task_names = os.listdir(f'/proc/{pid}/task')
    except FileNotFoundError:
        return children
    for task_name in task_names:
        if not task_name.isdigit():
            continue
        try:
            raw = pathlib.Path(f'/proc/{pid}/task/{task_name}/children'
                              ).read_text(encoding='utf-8').strip()
        except FileNotFoundError:
            continue
        if raw:
            children.update(int(value) for value in raw.split())
    return children


def test_live_adopted_reap_never_waits_on_manager_pid(monkeypatch):
    """Popen remains the sole owner when its PID exits during a reap scan."""
    command = mock.Mock(pid=101)
    command.poll.return_value = None
    monkeypatch.setattr(managed_job_controller_runner, '_direct_child_pids',
                        lambda _pid: (101, 202, 303))
    waitpid = mock.Mock(side_effect=lambda pid, _flags: (pid, 0))
    monkeypatch.setattr(managed_job_controller_runner.os, 'waitpid', waitpid)

    managed_job_controller_runner._reap_adopted_children(command)

    command.poll.assert_called_once_with()
    assert waitpid.call_args_list == [
        mock.call(202, os.WNOHANG),
        mock.call(303, os.WNOHANG),
    ]


def test_manager_child_environment_scrubs_all_capability_transport(monkeypatch):
    capability = controller_capability.generate()
    sensitive_environment = {
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY': capability,
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH': '/private/hash-only-authority',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD': '999',
    }
    for name, value in sensitive_environment.items():
        monkeypatch.setenv(name, value)
    supervisor = controller_slots.ManagedJobControllerSlotSupervisor(
        ('instance-a', 7), slot_count=1, origin_capability=capability)

    child_environment = supervisor._manager_child_environment(0, 'attempt-a')

    assert all(name not in child_environment for name in sensitive_environment)
    assert capability not in json.dumps(child_environment, sort_keys=True)
    assert child_environment[
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ID'] == '0'
    assert child_environment[
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT'] == 'attempt-a'


def test_supervisor_rejects_capability_in_child_environment():
    capability = controller_capability.generate()

    with pytest.raises(controller_slots.ControllerSlotError,
                       match='cannot use child_env'):
        controller_slots.ManagedJobControllerSlotSupervisor(
            ('instance-a', 7),
            slot_count=1,
            child_env={
                'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY': capability,
            },
            origin_capability=capability)


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='controller capability bootstrap requires Linux')
def test_manager_bootstrap_installs_capability_before_plugin_import(tmp_path):
    """A hostile import observer sees protected authority before plugins."""
    proof_path = tmp_path / 'bootstrap-import-proof.json'
    sitecustomize = tmp_path / 'sitecustomize.py'
    sitecustomize.write_text('\n'.join([
        'import builtins',
        'import ctypes',
        'import json',
        'import os',
        'import pathlib',
        'import sys',
        '_real_import = builtins.__import__',
        '_reported = False',
        'def _observe(name, globals=None, locals=None, fromlist=(), level=0):',
        '    global _reported',
        '    result = _real_import(name, globals, locals, fromlist, level)',
        '    if not _reported and "sky.server.plugins" in sys.modules:',
        '        _reported = True',
        '        authority = sys.modules.get(',
        '            "sky.utils.controller_capability")',
        '        names = [',
        '            "SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY",',
        '            "SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH",',
        '            "SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD",',
        '        ]',
        '        pathlib.Path(os.environ["BOOTSTRAP_PROOF_PATH"]).write_text(',
        '            json.dumps({',
        '                "capability_installed": authority is not None and',
        '                    authority.get_process_local() is not None,',
        '                "dumpable": ctypes.CDLL(None).prctl(3, 0, 0, 0, 0),',
        '                "environment": {key: os.environ.get(key)',
        '                                for key in names},',
        '                "argv": sys.argv,',
        '            }), encoding="utf-8")',
        '    return result',
        'builtins.__import__ = _observe',
    ]),
                             encoding='utf-8')
    capability = controller_capability.generate()
    capability_fd = controller_slots._open_capability_transport(capability)
    ready_read_fd, ready_write_fd = os.pipe()
    bootstrap_path = pathlib.Path(controller_slots.__file__).with_name(
        'managed_job_controller_bootstrap.py')
    child_env = dict(os.environ)
    child_env['PYTHONPATH'] = (str(tmp_path) + os.pathsep +
                               child_env.get('PYTHONPATH', ''))
    child_env['BOOTSTRAP_PROOF_PATH'] = str(proof_path)
    child_env['SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD'] = str(
        capability_fd)
    child_env['SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_READY_FD'] = str(
        ready_write_fd)
    manager = subprocess.Popen(  # pylint: disable=consider-using-with
        [
            sys.executable,
            '-S',
            str(bootstrap_path),
            'slot-0-bootstrap-proof',
            '0',
            '00000000-0000-0000-0000-000000000001',
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=child_env,
        pass_fds=(capability_fd, ready_write_fd),
        close_fds=True)
    os.close(capability_fd)
    os.close(ready_write_fd)
    try:
        assert _wait_until(proof_path.exists), (
            manager.stdout.read().decode('utf-8', errors='replace')
            if manager.poll() is not None and manager.stdout is not None else
            'manager import did not reach the plugin boundary')
        proof = json.loads(proof_path.read_text(encoding='utf-8'))
        assert proof['capability_installed'] is True
        assert proof['dumpable'] == 0
        assert all(value is None for value in proof['environment'].values())
        assert capability not in '\x00'.join(proof['argv'])
    finally:
        try:
            os.close(ready_read_fd)
        except OSError:
            pass
        if manager.poll() is None:
            manager.terminate()
            try:
                manager.wait(timeout=10)
            except subprocess.TimeoutExpired:
                manager.kill()
                manager.wait(timeout=10)


def test_start_prepares_and_monitors_every_slot_before_any_admission():
    """Startup exposes no partially prepared fixed-slot set."""
    events: list[tuple[str, int]] = []

    class OrderingSupervisor(controller_slots.ManagedJobControllerSlotSupervisor
                            ):

        def _identity(self, slot_id):
            return ('instance-a', 7, slot_id, f'attempt-{slot_id}')

        def _prepare_family(self, identity):
            slot_id = identity[2]
            assert not any(event[0] == 'admit' for event in events)
            events.append(('prepare', slot_id))
            return _family(slot_id, identity[3])

        def _run_slot(self, initial_family):
            del initial_family
            self._startup_complete.wait(timeout=5)

        def _send_admission(self, family):
            # Every family is published and every monitor thread has been
            # installed before the first effect gate opens.
            assert len(self._families) == self.slot_count
            assert len(self._threads) == self.slot_count
            assert not any(event[0] == 'started' for event in events)
            events.append(('admit', family.identity[2]))
            family.admitted = True

        def _wait_for_started(self, family):
            assert sum(
                event[0] == 'admit' for event in events) == self.slot_count
            events.append(('started', family.identity[2]))

    supervisor = OrderingSupervisor(('instance-a', 7), slot_count=3)
    supervisor.start()
    supervisor.wait_for_shutdown()

    assert events == [
        ('prepare', 0),
        ('prepare', 1),
        ('prepare', 2),
        ('admit', 0),
        ('admit', 1),
        ('admit', 2),
        ('started', 0),
        ('started', 1),
        ('started', 2),
    ]


def test_dead_attempt_is_reset_before_rotated_replacement_admission(
        monkeypatch):
    """A replacement cannot inherit or reset a different attempt's jobs."""
    initial = _admitted_family(2, 'attempt-old')
    replacement = _family(2, 'attempt-new')
    events: list[tuple[str, object]] = []

    class ReplacementSupervisor(
            controller_slots.ManagedJobControllerSlotSupervisor):

        def _identity(self, slot_id):
            assert slot_id == 2
            events.append(('rotate', 'attempt-new'))
            return replacement.identity

        def _prepare_family(self, identity):
            assert identity == replacement.identity
            events.append(('prepare', identity))
            return replacement

        def _wait_family_completion(self, family):
            events.append(('complete', family.identity))
            if family is replacement:
                self._stop.set()

        def _quiesce_nested_requests(self, identity):
            events.append(('quiesce', identity))
            return 2

        def _send_admission(self, family):
            events.append(('admit', family.identity))
            family.admitted = True

        def _wait_for_started(self, family):
            events.append(('started', family.identity))

    reset_identities = []

    def reset_exact_attempt(identity):
        reset_identities.append(identity)
        events.append(('reset', identity))
        return 1

    monkeypatch.setattr(controller_slots.managed_job_state,
                        'reset_jobs_for_controller_slot', reset_exact_attempt)
    supervisor = ReplacementSupervisor(('instance-a', 7), slot_count=3)
    supervisor._startup_succeeded = True
    supervisor._startup_complete.set()
    supervisor._set_family(2, initial)

    supervisor._run_slot(initial)

    assert reset_identities == [initial.identity]
    assert replacement.identity[:3] == initial.identity[:3]
    assert replacement.identity[3] != initial.identity[3]
    assert events.index(('complete', initial.identity)) < events.index(
        ('quiesce', initial.identity))
    assert events.index(('quiesce', initial.identity)) < events.index(
        ('reset', initial.identity))
    assert events.index(('reset', initial.identity)) < events.index(
        ('admit', replacement.identity))
    assert ('reset', replacement.identity) not in events


def test_nested_request_quiescence_failure_blocks_reset_and_replacement(
        monkeypatch):
    """An ambiguous nested effect is retried before ownership can release."""
    initial = _admitted_family(0, 'attempt-old')
    reset = mock.Mock()
    prepare = mock.Mock()
    backend_available = False
    quiescence_calls = []

    class QuiescenceFailureSupervisor(
            controller_slots.ManagedJobControllerSlotSupervisor):

        def _wait_family_completion(self, family):
            assert family is initial

        def _prepare_family(self, identity):
            prepare(identity)
            raise AssertionError('replacement must remain fenced')

    monkeypatch.setattr(controller_slots.ManagedJobControllerSlotSupervisor,
                        '_quiesce_nested_requests',
                        _ORIGINAL_QUIESCE_NESTED_REQUESTS)

    def quiesce_nested_requests(identity):
        quiescence_calls.append(identity)
        if not backend_available:
            raise request_storage.ManagedJobRequestQuiescenceError(
                'exact request family remains ambiguous')
        return 2

    monkeypatch.setattr(api_requests, 'quiesce_managed_job_slot_requests',
                        quiesce_nested_requests)
    monkeypatch.setattr(controller_slots.managed_job_state,
                        'reset_jobs_for_controller_slot', reset)
    supervisor = QuiescenceFailureSupervisor(('instance-a', 7), slot_count=1)
    supervisor._startup_succeeded = True
    supervisor._startup_complete.set()
    supervisor._set_family(0, initial)
    slot_thread = threading.Thread(target=supervisor._run_slot, args=(initial,))
    supervisor._threads.append(slot_thread)
    slot_thread.start()

    with pytest.raises(controller_slots.ControllerSlotNestedRequestProofError,
                       match='nested requests did not prove exact quiescence'):
        supervisor.wait_for_shutdown()

    reset.assert_not_called()
    prepare.assert_not_called()
    assert 0 not in supervisor._families
    assert supervisor._pending_nested_quiescence == {initial.identity}
    assert supervisor._unsafe_failure is None

    # The outer runtime keeps leadership and reruns this authoritative proof.
    # Once the request backend recovers, the exact pending identity is cleared
    # and ownership handoff may finish; replacement/reset remains fenced.
    backend_available = True
    supervisor.wait_for_shutdown()
    assert quiescence_calls == [initial.identity] * 3
    assert supervisor._pending_nested_quiescence == set()
    assert supervisor._unsafe_failure is None
    reset.assert_not_called()
    prepare.assert_not_called()
    with pytest.raises(controller_slots.ControllerSlotError,
                       match='lost its exact family proof'):
        supervisor.raise_if_failed()


def test_safe_restart_failure_does_not_block_proven_shutdown(monkeypatch):
    """A post-proof restart error is fatal but is not an unsafe handoff."""
    initial = _admitted_family(0, 'attempt-old')

    class RestartFailureSupervisor(
            controller_slots.ManagedJobControllerSlotSupervisor):

        def _wait_family_completion(self, family):
            assert family is initial

        def _prepare_family(self, identity):
            del identity
            raise RuntimeError('replacement construction failed')

    monkeypatch.setattr(controller_slots.managed_job_state,
                        'reset_jobs_for_controller_slot', lambda identity: 1)
    supervisor = RestartFailureSupervisor(('instance-a', 7), slot_count=1)
    supervisor._startup_succeeded = True
    supervisor._startup_complete.set()
    supervisor._set_family(0, initial)
    slot_thread = threading.Thread(target=supervisor._run_slot, args=(initial,))
    supervisor._threads.append(slot_thread)
    slot_thread.start()

    # The exact old family completed before replacement construction failed,
    # so shutdown can release ownership even though the runtime must restart.
    supervisor.wait_for_shutdown()
    assert supervisor._unsafe_failure is None
    with pytest.raises(controller_slots.ControllerSlotError,
                       match='lost its exact family proof'):
        supervisor.raise_if_failed()


def test_admitted_replacement_start_failure_drains_before_monitor_exit(
        monkeypatch):
    """A published replacement remains owned until its exact drain proof."""
    initial = _admitted_family(0, 'attempt-old')
    replacement = _family(0, 'attempt-new')
    replacement_drain_started = threading.Event()
    allow_replacement_proof = threading.Event()
    wait_finished = threading.Event()
    wait_errors = []
    events: list[tuple[str, object]] = []

    class AdmissionFailureSupervisor(
            controller_slots.ManagedJobControllerSlotSupervisor):

        def _identity(self, slot_id):
            assert slot_id == 0
            return replacement.identity

        def _prepare_family(self, identity):
            assert identity == replacement.identity
            events.append(('prepare', identity))
            return replacement

        def _wait_family_completion(self, family):
            events.append(('drain-start', family.identity))
            if family is replacement:
                replacement_drain_started.set()
                assert allow_replacement_proof.wait(timeout=5)
                events.append(('proof', family.identity))

        def _quiesce_nested_requests(self, identity):
            events.append(('quiesce', identity))
            return 0

        def _send_admission(self, family):
            assert family is replacement
            family.admitted = True
            events.append(('admit', family.identity))

        def _wait_for_started(self, family):
            assert family is replacement
            events.append(('start-failed', family.identity))
            raise controller_slots.ControllerSlotError(
                'replacement failed readiness')

    def write_message(control, message):
        assert control is replacement.control
        events.append((str(message['type']), replacement.identity))

    monkeypatch.setattr(controller_slots, '_write_message', write_message)
    monkeypatch.setattr(controller_slots.managed_job_state,
                        'reset_jobs_for_controller_slot', lambda identity: 1)
    supervisor = AdmissionFailureSupervisor(('instance-a', 7), slot_count=1)
    supervisor._startup_succeeded = True
    supervisor._startup_complete.set()
    supervisor._set_family(0, initial)
    slot_thread = threading.Thread(target=supervisor._run_slot, args=(initial,))
    supervisor._threads.append(slot_thread)
    slot_thread.start()

    assert replacement_drain_started.wait(timeout=5)
    assert supervisor._families[0] is replacement

    def wait_for_shutdown():
        try:
            supervisor.wait_for_shutdown()
        except BaseException as error:  # pylint: disable=broad-except
            wait_errors.append(error)
        finally:
            events.append(('wait-finished', replacement.identity))
            wait_finished.set()

    wait_thread = threading.Thread(target=wait_for_shutdown)
    wait_thread.start()
    time.sleep(0.2)
    assert not wait_finished.is_set()

    allow_replacement_proof.set()
    wait_thread.join(timeout=5)
    slot_thread.join(timeout=5)

    assert not wait_thread.is_alive()
    assert not slot_thread.is_alive()
    assert wait_errors == []
    assert 0 not in supervisor._families
    assert ('terminate', replacement.identity) in events
    assert events.index(('proof', replacement.identity)) < events.index(
        ('quiesce', replacement.identity))
    assert events.index(('quiesce', replacement.identity)) < events.index(
        ('wait-finished', replacement.identity))
    assert supervisor._unsafe_failure is None
    with pytest.raises(controller_slots.ControllerSlotError,
                       match='lost its exact family proof'):
        supervisor.raise_if_failed()


def test_shutdown_cannot_miss_replacement_between_prepare_and_publish(
        monkeypatch):
    """Shutdown and replacement publication form one atomic admission gate."""
    initial = _admitted_family(1, 'attempt-old')
    replacement = _family(1, 'attempt-new')
    replacement_prepared = threading.Event()
    allow_prepare_return = threading.Event()
    admitted = threading.Event()
    completed: list[controller_slots._SlotFamily] = []

    class ShutdownRaceSupervisor(
            controller_slots.ManagedJobControllerSlotSupervisor):

        def _identity(self, slot_id):
            assert slot_id == 1
            return replacement.identity

        def _prepare_family(self, identity):
            assert identity == replacement.identity
            replacement_prepared.set()
            assert allow_prepare_return.wait(timeout=5)
            return replacement

        def _wait_family_completion(self, family):
            completed.append(family)

        def _send_admission(self, family):
            del family
            admitted.set()

        def _wait_for_started(self, family):
            del family

    monkeypatch.setattr(controller_slots.managed_job_state,
                        'reset_jobs_for_controller_slot', lambda identity: 1)
    supervisor = ShutdownRaceSupervisor(('instance-a', 7), slot_count=2)
    supervisor._startup_succeeded = True
    supervisor._startup_complete.set()
    supervisor._set_family(1, initial)
    slot_thread = threading.Thread(target=supervisor._run_slot, args=(initial,))
    supervisor._threads.append(slot_thread)
    slot_thread.start()
    assert replacement_prepared.wait(timeout=5)

    supervisor.request_shutdown()
    allow_prepare_return.set()
    slot_thread.join(timeout=5)

    assert not slot_thread.is_alive()
    assert not admitted.is_set()
    assert replacement in completed


def test_partial_startup_admission_drains_every_admitted_family():
    """Startup failure cannot abandon effects admitted by an earlier gate."""
    events: list[tuple[str, object]] = []

    class PartialStartupFailureSupervisor(
            controller_slots.ManagedJobControllerSlotSupervisor):

        def _identity(self, slot_id):
            return ('instance-a', 7, slot_id, f'attempt-{slot_id}')

        def _prepare_family(self, identity):
            return _family(identity[2], identity[3])

        def _run_slot(self, initial_family):
            del initial_family
            self._startup_complete.wait(timeout=5)

        def _send_admission(self, family):
            family.admitted = True
            family.admitted_at = time.monotonic()
            events.append(('admit', family.identity))

        def _wait_for_started(self, family):
            events.append(('started', family.identity))
            if family.identity[2] == 1:
                raise controller_slots.ControllerSlotError(
                    'second family failed readiness')

        def _wait_family_completion(self, family):
            events.append(('proof', family.identity))

        def _quiesce_nested_requests(self, identity):
            events.append(('quiesce', identity))
            return 0

    supervisor = PartialStartupFailureSupervisor(('instance-a', 7),
                                                 slot_count=3)
    with pytest.raises(controller_slots.ControllerSlotError,
                       match='failed readiness'):
        supervisor.start()
    supervisor.wait_for_shutdown()

    for slot_id in range(3):
        identity = ('instance-a', 7, slot_id, f'attempt-{slot_id}')
        assert events.index(('admit', identity)) < events.index(
            ('proof', identity))
        assert events.index(('proof', identity)) < events.index(
            ('quiesce', identity))
    assert supervisor._families == {}


def test_admission_send_failure_is_conservatively_effect_bearing(monkeypatch):
    """An ambiguous socket send still requires nested effect quiescence."""
    family = _family(0, 'attempt-ambiguous')
    monkeypatch.setattr(
        controller_slots, '_write_message',
        mock.Mock(side_effect=OSError('admission delivery is ambiguous')))
    supervisor = controller_slots.ManagedJobControllerSlotSupervisor(
        ('instance-a', 7), slot_count=1)

    with pytest.raises(OSError, match='delivery is ambiguous'):
        supervisor._send_admission(family)

    assert family.admitted
    assert family.admitted_at is not None


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='controller family proof requires Linux subreapers')
def test_real_runner_drains_new_session_descendant_before_completion(tmp_path):
    """The live warden reaps orphans and later proves the family absent."""
    if os.uname().machine not in ('x86_64', 'aarch64'):
        pytest.skip('test requires the runner-supported pidfd architectures')

    helper_path = tmp_path / 'controller_helper.py'
    pids_path = tmp_path / 'controller_descendants'
    manager_proof_path = tmp_path / 'manager_capability_proof.json'
    callback_proof_path = tmp_path / 'callback_capability_proof.json'
    orphans_done_path = tmp_path / 'manager_orphans_done'
    manager_exit_path = tmp_path / 'manager_exit'
    helper_path.write_text('\n'.join([
        'import ctypes',
        'import json',
        'import os',
        'import pathlib',
        'import signal',
        'import subprocess',
        'import sys',
        'import time',
        'capability_fd = int(os.environ.pop(',
        '    "SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD"))',
        'libc = ctypes.CDLL(None, use_errno=True)',
        'assert libc.prctl(4, 0, 0, 0, 0) == 0',
        'capability = os.read(capability_fd, 44)',
        'os.close(capability_fd)',
        'assert len(capability) == 43',
        'sensitive_names = [',
        '    "SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY",',
        '    "SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH",',
        '    "SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD",',
        ']',
        'pathlib.Path(sys.argv[2]).write_text(json.dumps({',
        '    "capability_consumed": len(capability) == 43,',
        '    "dumpable": ctypes.CDLL(None).prctl(3, 0, 0, 0, 0),',
        '    "environment": {name: os.environ.get(name)',
        '                    for name in sensitive_names},',
        '    "argv": sys.argv,',
        '}), encoding="utf-8")',
        'callback_code = """',
        'import json',
        'import os',
        'import pathlib',
        'import sys',
        'names = [',
        '    "SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY",',
        '    "SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH",',
        '    "SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD",',
        ']',
        'pathlib.Path(sys.argv[1]).write_text(json.dumps({',
        '    "environment": {name: os.environ.get(name) for name in names},',
        '}), encoding="utf-8")',
        '"""',
        'subprocess.run([sys.executable, "-S", "-c", callback_code,',
        '                sys.argv[3]],',
        '               check=True)',
        'child = subprocess.Popen([',
        '    sys.executable, "-S", "-c",',
        '    "import signal, time; "',
        '    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "',
        '    "time.sleep(3600)"',
        '], start_new_session=True)',
        'pathlib.Path(sys.argv[1]).write_text(',
        '    f"{os.getpid()} {child.pid}", encoding="utf-8")',
        'ready_fd = int(os.environ.pop(',
        '    "SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_READY_FD"))',
        'os.write(ready_fd, b"1")',
        'os.close(ready_fd)',
        '# Give the warden time to enter its normal live-manager loop, then',
        '# repeatedly orphan short-lived grandchildren into the subreaper.',
        'time.sleep(0.1)',
        'for _ in range(128):',
        '    intermediate = os.fork()',
        '    if intermediate == 0:',
        '        orphan = os.fork()',
        '        if orphan == 0:',
        '            os._exit(0)',
        '        os._exit(0)',
        '    os.waitpid(intermediate, 0)',
        'pathlib.Path(sys.argv[4]).write_text("done", encoding="utf-8")',
        'while not pathlib.Path(sys.argv[5]).exists():',
        '    time.sleep(0.01)',
        '# Exit concurrently with the warden\'s continuous adopted-child reap.',
        'raise SystemExit(23)',
    ]),
                           encoding='utf-8')
    command = json.dumps([
        sys.executable,
        '-S',
        str(helper_path),
        str(pids_path),
        str(manager_proof_path),
        str(callback_proof_path),
        str(orphans_done_path),
        str(manager_exit_path),
    ],
                         separators=(',', ':'))
    runtime_control, runner_control = socket.socketpair()
    runtime_control.settimeout(20)
    runner_path = pathlib.Path(
        controller_slots.__file__).with_name('managed_job_controller_runner.py')
    attempt = 'attempt-real-runner'
    capability = controller_capability.generate()
    capability_fd = controller_slots._open_capability_transport(capability)
    runner_env = dict(os.environ)
    for sensitive_name in (
            'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY',
            'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH',
            'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD'):
        runner_env.pop(sensitive_name, None)
    runner = subprocess.Popen(  # pylint: disable=consider-using-with
        [
            sys.executable,
            '-S',
            str(runner_path),
            str(runner_control.fileno()),
            str(capability_fd),
            str(os.getpid()),
            str(controller_slots._read_process_start_time_ticks(os.getpid())),
            'instance-a',
            '7',
            '0',
            attempt,
            command,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=runner_env,
        pass_fds=(runner_control.fileno(), capability_fd),
        close_fds=True)
    os.close(capability_fd)
    runner_control.close()
    descendant_identities: list[tuple[int, int]] = []
    try:
        ready = controller_slots._read_message(runtime_control)
        assert ready is not None
        assert ready['type'] == 'ready'
        assert ready['pid'] == runner.pid
        controller_slots._write_message(runtime_control, {
            'type': 'admit',
            'controller_slot_attempt': attempt,
        })
        started = controller_slots._read_message(runtime_control)
        assert started is not None
        assert started['type'] == 'started'
        assert _wait_until(pids_path.exists)
        assert _wait_until(manager_proof_path.exists)
        assert _wait_until(callback_proof_path.exists)
        manager_proof = json.loads(
            manager_proof_path.read_text(encoding='utf-8'))
        callback_proof = json.loads(
            callback_proof_path.read_text(encoding='utf-8'))
        assert manager_proof['capability_consumed'] is True
        assert manager_proof['dumpable'] == 0
        assert all(
            value is None for value in manager_proof['environment'].values())
        assert all(
            value is None for value in callback_proof['environment'].values())
        assert capability not in manager_proof_path.read_text(encoding='utf-8')
        assert capability not in callback_proof_path.read_text(encoding='utf-8')
        assert capability not in '\x00'.join(manager_proof['argv'])
        assert capability not in '\x00'.join(runner.args)
        assert _wait_until(orphans_done_path.exists)
        manager_pid = int(started['controller_pid'])
        manager_identity = _process_identity(manager_pid)
        assert _wait_until(lambda: len(_direct_child_pids(runner.pid)) == 1)
        inner_pid = next(iter(_direct_child_pids(runner.pid)))
        # The manager remains healthy and owns its long-lived child. Every
        # completed double-fork descendant has been adopted and reaped by the
        # inner warden instead of accumulating as a zombie until shutdown.
        assert _wait_until(
            lambda: _direct_child_pids(inner_pid) == {manager_pid})
        assert _identity_exists(manager_identity)
        descendant_pids = [
            int(value)
            for value in pids_path.read_text(encoding='utf-8').split()
        ]
        descendant_pids.append(int(started['controller_pid']))
        descendant_identities = [
            _process_identity(pid) for pid in set(descendant_pids)
        ]

        manager_exit_path.write_text('exit', encoding='utf-8')
        completion = None
        while True:
            message = controller_slots._read_message(runtime_control)
            if message is None:
                break
            if message.get('type') == 'complete':
                completion = message
        assert completion is not None
        assert completion['controller_instance_id'] == 'instance-a'
        assert completion['controller_generation'] == '7'
        assert completion['controller_slot_id'] == 0
        assert completion['controller_slot_attempt'] == attempt
        assert completion['descendants_empty'] is True
        runner.wait(timeout=20)
        assert all(
            _wait_until(
                lambda identity=identity: not _identity_exists(identity))
            for identity in descendant_identities)
    finally:
        try:
            controller_slots._write_message(runtime_control,
                                            {'type': 'terminate'})
        except OSError:
            pass
        runtime_control.close()
        if runner.poll() is None:
            try:
                os.killpg(runner.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            runner.wait(timeout=10)
        for identity in descendant_identities:
            if _identity_exists(identity):
                try:
                    os.kill(identity[0], signal.SIGKILL)
                except ProcessLookupError:
                    pass
