"""Runtime-owned controller daemon supervision and child-admission tests."""

# pylint: disable=protected-access
import asyncio
import json
import os
import pathlib
import signal
import subprocess
import sys
import time
from unittest import mock

import pytest

from sky import global_user_state
from sky import models
from sky.server import clean_env
from sky.server import common as server_common
from sky.server import daemons
from sky.server import internal_daemon_runner
from sky.server import metrics
from sky.server import plugins
from sky.server import runtime
from sky.server import watchdog
from sky.server.requests import postgres as request_postgres
from sky.utils import controller_capability
from sky.utils.db import db_utils

_STUBBORN_GRANDCHILD_SCRIPT = ('import signal,time; '
                               'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
                               'time.sleep(300)')
_CONTROLLER_OWNER = ('12345678-1234-4abc-9234-56789abcdef0', 7)


def _guardian_launcher_script() -> str:
    return f'''\
import os
import subprocess
import sys
import time
from sky.server import internal_daemon_runner as runner
parent_pid = int(sys.argv[1])
parent_ticks = int(sys.argv[2])
runner._arm_parent_death_contract(parent_pid, parent_ticks)
guardian_pid = runner._start_parent_death_group_guardian(parent_pid,
                                                         parent_ticks)
grandchild = subprocess.Popen([sys.executable, '-c',
                               {_STUBBORN_GRANDCHILD_SCRIPT!r}])
print('RUNTIME-GUARDIAN-READY', os.getpid(), guardian_pid, grandchild.pid,
      flush=True)
time.sleep(300)
'''


def _wait_process_group_absent(process_group_id: int,
                               timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not runtime._runtime_daemon_process_group_exists(process_group_id):
            return
        time.sleep(0.05)
    pytest.fail(f'process group {process_group_id} survived fail-stop')


def _terminate_process_group_if_present(process_group_id: int) -> None:
    if runtime._runtime_daemon_process_group_exists(process_group_id):
        os.killpg(process_group_id, signal.SIGKILL)


def _read_guardian_ready(stream) -> tuple[int, int, int]:
    while True:
        line = stream.readline()
        if not line:
            pytest.fail('guardian launcher exited before its admission barrier')
        fields = line.split()
        if fields and fields[0] == 'RUNTIME-GUARDIAN-READY':
            return tuple(map(int, fields[1:4]))


@pytest.mark.asyncio
async def test_runtime_daemon_inventory_and_skip_evaluated_once(monkeypatch):
    first = mock.Mock(id='first-daemon',
                      should_skip=mock.Mock(return_value=False))
    second = mock.Mock(id='second-daemon',
                       should_skip=mock.Mock(return_value=True))
    background = mock.Mock()
    singleton_task = mock.sentinel.singleton_task
    singleton = mock.Mock(return_value=singleton_task)
    monkeypatch.setattr(runtime.daemons, 'RUNTIME_DAEMONS', [first, second])
    monkeypatch.setattr(runtime.clean_env_module, 'get_clean_server_env',
                        lambda: {'PATH': '/bin'})
    monkeypatch.setattr(runtime, '_executor_process_start_time_ticks',
                        lambda pid: 123)
    monkeypatch.setattr(runtime, '_singleton_task', singleton)
    start_observer = mock.Mock(return_value=None)
    monkeypatch.setattr(runtime.executor_termination_observer, 'start',
                        start_observer)
    pod_identity = request_postgres.ServerPodIdentity(name='controller',
                                                      namespace='skypilot',
                                                      uid=_CONTROLLER_OWNER[0],
                                                      ip='10.0.0.1')
    capability = controller_capability.generate()
    controller_capability.install_process_local(capability)

    try:
        selected = await runtime._register_runtime_daemons_async(
            background,
            4,
            _CONTROLLER_OWNER,
            pod_identity,
            observe_executor_termination=True)
    finally:
        controller_capability.clear_process_local()

    assert selected == ('first-daemon',)
    start_observer.assert_called_once_with(_CONTROLLER_OWNER, pod_identity)
    first.should_skip.assert_called_once_with()
    second.should_skip.assert_called_once_with()
    singleton.assert_called_once_with('internal-daemon:first-daemon', mock.ANY)
    daemon_factory = singleton.call_args.args[1]
    assert daemon_factory.args[-1] == _CONTROLLER_OWNER
    background.create_task.assert_called_once_with(singleton_task)


@pytest.mark.asyncio
async def test_compatibility_daemon_inventory_does_not_start_observer(
        monkeypatch):
    background = mock.Mock()
    monkeypatch.setattr(runtime.daemons, 'RUNTIME_DAEMONS', ())
    monkeypatch.setattr(runtime.clean_env_module, 'get_clean_server_env',
                        lambda: {'PATH': '/bin'})
    monkeypatch.setattr(runtime, '_executor_process_start_time_ticks',
                        lambda pid: 123)
    start_observer = mock.Mock()
    monkeypatch.setattr(runtime.executor_termination_observer, 'start',
                        start_observer)
    pod_identity = request_postgres.ServerPodIdentity(name='compatibility-all',
                                                      namespace='skypilot',
                                                      uid=_CONTROLLER_OWNER[0],
                                                      ip='10.0.0.1')
    capability = controller_capability.generate()
    controller_capability.install_process_local(capability)

    try:
        selected = await runtime._register_runtime_daemons_async(
            background, 4, _CONTROLLER_OWNER, pod_identity)
    finally:
        controller_capability.clear_process_local()

    assert selected == ()
    start_observer.assert_not_called()
    background.add_graceful_shutdown_hook.assert_not_called()


@pytest.mark.asyncio
async def test_unexpected_exit_restarts_with_bounded_backoff(
        monkeypatch, tmp_path):
    processes = [mock.AsyncMock(pid=11), mock.AsyncMock(pid=12)]
    for process in processes:
        process.wait.return_value = 7
    create = mock.AsyncMock(side_effect=processes)
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(runtime.asyncio, 'create_subprocess_exec', create)
    monkeypatch.setattr(runtime.asyncio, 'sleep', sleep)
    monkeypatch.setattr(runtime, '_terminate_runtime_daemon_process',
                        mock.AsyncMock())
    monkeypatch.setattr(runtime.server_constants, 'REQUEST_LOG_PATH_PREFIX',
                        str(tmp_path))

    with pytest.raises(asyncio.CancelledError):
        await runtime._supervise_runtime_daemon(
            'test-daemon', {}, 1, 1, 2, controller_capability.generate(),
            _CONTROLLER_OWNER)

    assert create.await_count == 2
    assert sleeps == [1, 2]
    for spawn_call in create.await_args_list:
        assert spawn_call.args[1:4] == ('-S', '-m', 'internal_daemon_runner')
        assert spawn_call.args[-2:] == (_CONTROLLER_OWNER[0],
                                        str(_CONTROLLER_OWNER[1]))
        assert _CONTROLLER_OWNER[0] not in spawn_call.kwargs['env'].values()
        assert (str(_CONTROLLER_OWNER[1])
                not in spawn_call.kwargs['env'].values())


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='requires Linux process groups')
@pytest.mark.asyncio
async def test_cancel_kills_and_reaps_whole_runtime_daemon_group(monkeypatch):
    grandchild = ('import signal,time; '
                  'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
                  'time.sleep(300)')
    script = ('import signal,subprocess,sys,time; '
              'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
              f'subprocess.Popen([sys.executable, "-c", {grandchild!r}]); '
              'print("ready", flush=True); '
              'time.sleep(300)')
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-c',
        script,
        stdout=asyncio.subprocess.PIPE,
        start_new_session=True)
    process_group_id = process.pid
    monkeypatch.setattr(runtime, '_RUNTIME_DAEMON_TERM_TIMEOUT_SECONDS', 0.01)
    try:
        assert process.stdout is not None
        assert await asyncio.wait_for(process.stdout.readline(),
                                      timeout=5) == b'ready\n'
        await runtime._terminate_runtime_daemon_process(process)
    finally:
        if runtime._runtime_daemon_process_group_exists(process_group_id):
            os.killpg(process_group_id, signal.SIGKILL)
    assert process.returncode is not None
    assert not runtime._runtime_daemon_process_group_exists(process_group_id)


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='requires Linux process groups and prctl')
def test_abrupt_parent_death_kills_launcher_and_stubborn_grandchild(tmp_path):
    repository_root = pathlib.Path(runtime.__file__).resolve().parents[2]
    controller_script = f'''\
import pathlib
import subprocess
import sys
import time
pid = __import__('os').getpid()
stat = (pathlib.Path('/proc') / str(pid) / 'stat').read_text()
ticks = int(stat[stat.rfind(')') + 1:].split()[19])
launcher = subprocess.Popen(
    [sys.executable, '-c', {_guardian_launcher_script()!r}, str(pid),
     str(ticks)], stdout=subprocess.PIPE, text=True, start_new_session=True)
assert launcher.stdout is not None
for line in launcher.stdout:
    if line.startswith('RUNTIME-GUARDIAN-READY '):
        print(line.strip(), flush=True)
        break
time.sleep(300)
'''
    controller = subprocess.Popen(
        [sys.executable, '-c', controller_script],
        cwd=tmp_path,
        env={
            **os.environ, 'PYTHONPATH': str(repository_root)
        },
        stdout=subprocess.PIPE,
        text=True,
    )
    assert controller.stdout is not None
    launcher_pid = 0
    try:
        launcher_pid, _, _ = _read_guardian_ready(controller.stdout)
        os.kill(controller.pid, signal.SIGKILL)
        controller.wait(timeout=5)
        _wait_process_group_absent(launcher_pid)
    finally:
        if controller.poll() is None:
            controller.kill()
            controller.wait(timeout=5)
        if launcher_pid:
            _terminate_process_group_if_present(launcher_pid)


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='requires Linux process groups and prctl')
def test_guardian_death_fail_stops_launcher_and_stubborn_grandchild(tmp_path):
    parent_ticks = runtime._executor_process_start_time_ticks(os.getpid())
    repository_root = pathlib.Path(runtime.__file__).resolve().parents[2]
    launcher = subprocess.Popen(
        [
            sys.executable, '-c',
            _guardian_launcher_script(),
            str(os.getpid()),
            str(parent_ticks)
        ],
        cwd=tmp_path,
        env={
            **os.environ, 'PYTHONPATH': str(repository_root)
        },
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    assert launcher.stdout is not None
    try:
        launcher_pid, guardian_pid, _ = _read_guardian_ready(launcher.stdout)
        assert launcher_pid == launcher.pid
        os.kill(guardian_pid, signal.SIGKILL)
        launcher.wait(timeout=10)
        _wait_process_group_absent(launcher.pid)
    finally:
        _terminate_process_group_if_present(launcher.pid)


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='requires Linux process groups and prctl')
def test_guardian_closes_capability_transport_without_authority(tmp_path):
    proof_path = tmp_path / 'guardian-capability-proof.json'
    repository_root = pathlib.Path(runtime.__file__).resolve().parents[2]
    script = f'''\
import ctypes
import json
import os
import socket
import sys
from sky.server import internal_daemon_runner as runner

proof_path = {str(proof_path)!r}
capability = {'A' * 43!r}
capability_fd, capability_write_fd = os.pipe()
os.write(capability_write_fd, capability.encode('ascii'))
os.close(capability_write_fd)
pid = os.getpid()
parent_ticks = runner._read_process_start_time_ticks(pid)

def observe(control_fd, *unused):
    del unused
    control = socket.socket(fileno=control_fd)
    try:
        try:
            os.fstat(capability_fd)
            fstat_closed = False
        except OSError:
            fstat_closed = True
        try:
            os.read(capability_fd, 1)
            read_closed = False
        except OSError:
            read_closed = True
        authority = sys.modules.get('sky.utils.controller_capability')
        registry = (None if authority is None else
                    authority.get_process_local())
        authority_environment = {{
            name: value for name, value in os.environ.items()
            if name.startswith('SKYPILOT_SERVER_CONTROLLER_') or
               name.startswith('SKYPILOT_SERVER_MANAGED_JOB_')
        }}
        open(proof_path, 'w', encoding='utf-8').write(json.dumps({{
            'fstat_closed': fstat_closed,
            'read_closed': read_closed,
            'registry': registry,
            'authority_environment': authority_environment,
            'raw_in_argv': capability in sys.argv,
            'dumpable': ctypes.CDLL(None).prctl(3, 0, 0, 0, 0),
        }}))
        control.sendall(b'0')
    finally:
        control.close()

runner._run_parent_death_group_guardian = observe
try:
    runner._start_parent_death_group_guardian(pid, parent_ticks,
                                              capability_fd)
except RuntimeError as error:
    assert 'rejected admission' in str(error)
else:
    raise AssertionError('guardian test admission unexpectedly succeeded')
'''
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith('SKYPILOT_SERVER_CONTROLLER_') and
        not name.startswith('SKYPILOT_SERVER_MANAGED_JOB_')
    }
    completed = subprocess.run(
        [sys.executable, '-c', script],
        cwd=tmp_path,
        env={
            **env, 'PYTHONPATH': str(repository_root)
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        start_new_session=True,
    )

    assert completed.returncode == 0, completed.stderr
    proof = json.loads(proof_path.read_text(encoding='utf-8'))
    assert proof == {
        'fstat_closed': True,
        'read_closed': True,
        'registry': None,
        'authority_environment': {},
        'raw_in_argv': False,
        'dumpable': 0,
    }


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='requires Linux process groups and prctl')
def test_normal_daemon_event_tick_preserves_group_guardian(tmp_path):
    """Per-tick cleanup must not kill the launcher's guardian child."""
    parent_ticks = runtime._executor_process_start_time_ticks(os.getpid())
    repository_root = pathlib.Path(runtime.__file__).resolve().parents[2]
    script = '''\
import logging
import os
import sys
from sky.server import daemons
from sky.server import internal_daemon_runner as runner

parent_pid = int(sys.argv[1])
parent_ticks = int(sys.argv[2])
runner._arm_parent_death_contract(parent_pid, parent_ticks)
guardian_pid = runner._start_parent_death_group_guardian(parent_pid,
                                                         parent_ticks)
ticks = 0

def event():
    global ticks
    ticks += 1
    if ticks == 2:
        os.kill(guardian_pid, 0)
        print('SECOND-TICK-GUARDIAN-ALIVE', flush=True)
        os._exit(0)

daemon = daemons.RuntimeDaemon('guardian-tick-test', 'guardian-tick-test',
                               event)
daemon.refresh_log_level = lambda: logging.INFO
daemons.annotations.clear_request_level_cache = lambda: None
daemons.timeline.save_timeline = lambda: None
daemons.common_utils.release_memory = lambda: None
daemons._rotate_daemon_log = lambda path: None
daemon.run_event()
'''
    completed = subprocess.run(
        [sys.executable, '-c', script,
         str(os.getpid()),
         str(parent_ticks)],
        cwd=tmp_path,
        env={
            **os.environ, 'PYTHONPATH': str(repository_root)
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        start_new_session=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert 'SECOND-TICK-GUARDIAN-ALIVE' in completed.stdout


def test_parent_change_after_prctl_fails_before_daemon_import(monkeypatch):
    libc = mock.Mock()
    libc.prctl.return_value = 0
    monkeypatch.setattr(internal_daemon_runner.ctypes, 'CDLL',
                        lambda *args, **kwargs: libc)
    monkeypatch.setattr(internal_daemon_runner.os, 'getppid', lambda: 99)
    read_identity = mock.Mock()
    monkeypatch.setattr(internal_daemon_runner,
                        '_read_process_start_time_ticks', read_identity)

    with pytest.raises(RuntimeError, match='parent changed'):
        internal_daemon_runner._arm_parent_death_contract(42, 123)

    read_identity.assert_not_called()


def test_runner_module_is_stdlib_first_and_rejects_unknown_id(tmp_path):
    runner_dir = pathlib.Path(internal_daemon_runner.__file__).resolve().parent
    env = dict(os.environ)
    env['PYTHONPATH'] = str(runner_dir)
    parent_ticks = runtime._executor_process_start_time_ticks(os.getpid())

    capability_fd = runtime._open_capability_transport(
        controller_capability.generate())
    try:
        completed = subprocess.run(
            [
                sys.executable, '-S', '-m', 'internal_daemon_runner',
                'unknown-runtime-daemon',
                str(os.getpid()),
                str(parent_ticks), '1',
                str(capability_fd), _CONTROLLER_OWNER[0],
                str(_CONTROLLER_OWNER[1])
            ],
            env=env,
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            start_new_session=True,
            pass_fds=(capability_fd,),
        )
    finally:
        os.close(capability_fd)

    assert completed.returncode != 0
    assert 'Unknown runtime daemon ID' in completed.stderr


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='requires Linux prctl')
def test_runner_installs_capability_before_any_nonstdlib_import(tmp_path):
    proof_path = tmp_path / 'runtime-daemon-bootstrap-proof.json'
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
        '    if not _reported and name == "setproctitle":',
        '        _reported = True',
        '        authority = sys.modules.get(',
        '            "sky.utils.controller_capability")',
        '        installed_capability = (None if authority is None else',
        '                                authority.get_process_local())',
        '        try:',
        '            os.fstat(int(sys.argv[5]))',
        '            capability_fd_closed = False',
        '        except OSError:',
        '            capability_fd_closed = True',
        '        pathlib.Path(os.environ["BOOTSTRAP_PROOF_PATH"]).write_text(',
        '            json.dumps({',
        '                "capability_installed": installed_capability is not None,',
        '                "capability_fd_closed": capability_fd_closed,',
        '                "dumpable": ctypes.CDLL(None).prctl(3, 0, 0, 0, 0),',
        '                "raw_capability_in_environment":',
        '                    installed_capability in os.environ.values(),',
        '                "argv": sys.argv,',
        '            }), encoding="utf-8")',
        '    return _real_import(name, globals, locals, fromlist, level)',
        'builtins.__import__ = _observe',
    ]),
                             encoding='utf-8')
    runner_dir = pathlib.Path(internal_daemon_runner.__file__).resolve().parent
    env = dict(os.environ)
    env.pop('SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY', None)
    env['PYTHONPATH'] = os.pathsep.join((str(runner_dir), str(tmp_path)))
    env['BOOTSTRAP_PROOF_PATH'] = str(proof_path)
    parent_ticks = runtime._executor_process_start_time_ticks(os.getpid())
    capability = controller_capability.generate()
    capability_fd = runtime._open_capability_transport(capability)
    try:
        completed = subprocess.run(
            [
                sys.executable, '-S', '-m', 'internal_daemon_runner',
                'unknown-runtime-daemon',
                str(os.getpid()),
                str(parent_ticks), '1',
                str(capability_fd), _CONTROLLER_OWNER[0],
                str(_CONTROLLER_OWNER[1])
            ],
            env=env,
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            start_new_session=True,
            pass_fds=(capability_fd,),
        )
    finally:
        os.close(capability_fd)

    assert completed.returncode != 0
    assert proof_path.exists(), completed.stderr
    proof = json.loads(proof_path.read_text(encoding='utf-8'))
    assert proof['capability_installed'] is True
    assert proof['capability_fd_closed'] is True
    assert proof['dumpable'] == 0
    assert proof['raw_capability_in_environment'] is False
    assert capability not in '\x00'.join(proof['argv'])


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='requires Linux process groups and prctl')
def test_runtime_daemon_capability_produces_nested_headers_without_leak(
        tmp_path):
    """A daemon receives authority, while its environment/argv never do."""
    capability = controller_capability.generate()
    instance_id = '12345678-1234-4abc-9234-56789abcdef0'
    parent_ticks = runtime._executor_process_start_time_ticks(os.getpid())
    repository_root = pathlib.Path(runtime.__file__).resolve().parents[2]
    capability_fd = runtime._open_capability_transport(capability)
    script = '''\
import json
import os
import sys
from sky.client import service_account_auth
from sky.utils import controller_capability

capability_fd = int(sys.argv[1])
controller_instance_id = sys.argv[2]
controller_generation = sys.argv[3]
controller_capability.make_process_non_dumpable()
controller_capability.install_process_local_from_fd(capability_fd)
os.environ["SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID"] = controller_instance_id
os.environ["SKYPILOT_SERVER_CONTROLLER_GENERATION"] = controller_generation
headers = service_account_auth.get_service_account_headers()
print(json.dumps({
    "header_instance_id": headers.get(
        "X-SkyPilot-Controller-Instance-ID"),
    "header_generation": headers.get(
        "X-SkyPilot-Controller-Generation"),
    "header_capability": headers.get(
        "X-SkyPilot-Controller-Origin-Capability"),
    "env_has_raw": any(value == headers.get(
        "X-SkyPilot-Controller-Origin-Capability")
        for value in os.environ.values()),
    "argv_has_raw": headers.get(
        "X-SkyPilot-Controller-Origin-Capability") in sys.argv,
    "dumpable": open("/proc/self/status", encoding="utf-8").read().split(
        "Dumpable:", 1)[1].splitlines()[0].strip() if "Dumpable:" in open(
        "/proc/self/status", encoding="utf-8").read() else "0",
}), flush=True)
'''
    env = dict(os.environ)
    env.pop('SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY', None)
    env.pop('SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID', None)
    env.pop('SKYPILOT_SERVER_CONTROLLER_GENERATION', None)
    completed = None
    try:
        completed = subprocess.run(
            [
                sys.executable, '-c', script,
                str(capability_fd), instance_id, '7'
            ],
            cwd=tmp_path,
            env={
                **env, 'PYTHONPATH': str(repository_root)
            },
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            pass_fds=(capability_fd,),
        )
    finally:
        os.close(capability_fd)
    assert completed is not None
    assert completed.returncode == 0, completed.stderr
    proof = json.loads(completed.stdout.splitlines()[-1])
    assert proof == {
        'header_instance_id': instance_id,
        'header_generation': '7',
        'header_capability': capability,
        'env_has_raw': False,
        'argv_has_raw': False,
        'dumpable': '0',
    }


def test_all_mode_transition_holds_lock_around_startup_mutations(monkeypatch):
    lifecycle = mock.Mock()
    transition_lock = mock.Mock()
    transition_lock.is_session_alive.return_value = True
    blockers = mock.Mock(side_effect=[['legacy-instance'], []])
    lifecycle.attach_mock(transition_lock.acquire, 'acquire')
    lifecycle.attach_mock(transition_lock.is_session_alive, 'alive')
    lifecycle.attach_mock(blockers, 'blockers')
    startup_mutation = mock.Mock()
    lifecycle.attach_mock(startup_mutation, 'startup_mutation')
    lifecycle.attach_mock(transition_lock.release, 'release')
    monkeypatch.setattr(request_postgres.locks, 'PostgresLock',
                        lambda lock_id: transition_lock)
    monkeypatch.setattr(request_postgres,
                        'recent_legacy_daemon_handler_instances', blockers)
    sleep = mock.Mock()
    monkeypatch.setattr(request_postgres.time, 'sleep', sleep)

    with request_postgres.legacy_daemon_transition(poll_seconds=0.25):
        startup_mutation()

    transition_lock.acquire.assert_called_once_with()
    transition_lock.release.assert_called_once_with()
    sleep.assert_called_once_with(0.25)
    assert lifecycle.mock_calls == [
        mock.call.acquire(),
        mock.call.alive(),
        mock.call.blockers(),
        mock.call.alive(),
        mock.call.blockers(),
        mock.call.startup_mutation(),
        mock.call.alive(),
        mock.call.release(),
    ]


def test_parent_start_identity_change_after_prctl_fails_closed(monkeypatch):
    libc = mock.Mock()
    libc.prctl.return_value = 0
    monkeypatch.setattr(internal_daemon_runner.ctypes, 'CDLL',
                        lambda *args, **kwargs: libc)
    monkeypatch.setattr(internal_daemon_runner.os, 'getppid', lambda: 42)
    monkeypatch.setattr(internal_daemon_runner,
                        '_read_process_start_time_ticks', lambda pid: 124)

    with pytest.raises(RuntimeError, match='parent was replaced'):
        internal_daemon_runner._arm_parent_death_contract(42, 123)


def test_runner_initializes_clean_system_executor_context(monkeypatch):
    calls = []
    daemon = mock.Mock()
    daemon.run_event.side_effect = lambda: calls.append('run')
    user = mock.sentinel.system_user

    monkeypatch.setattr(internal_daemon_runner, 'os',
                        mock.Mock(getpid=lambda: 123, environ={'CLEAN': '1'}))
    monkeypatch.setattr('setproctitle.setproctitle', lambda title: calls.append(
        ('title', title)))
    monkeypatch.setattr(db_utils, 'set_max_connections',
                        lambda value: calls.append(('db', value)))
    monkeypatch.setattr(
        daemons, 'get_runtime_daemon', lambda daemon_id: calls.append(
            ('resolve', daemon_id)) or daemon)
    monkeypatch.setattr(watchdog, 'start_parent_death_watchdog',
                        lambda: calls.append('watchdog'))
    monkeypatch.setattr(clean_env, 'restore_clean_server_env',
                        lambda env: calls.append(('clean-env', env)))
    monkeypatch.setattr(
        plugins, 'load_plugins', lambda context: calls.append(
            ('plugins', context.context)))
    monkeypatch.setattr(metrics, 'register_multiproc_cleanup_atexit',
                        lambda: calls.append('metrics-cleanup'))
    monkeypatch.setattr(models, 'User', lambda **kwargs: user)
    monkeypatch.setattr(global_user_state, 'add_or_update_user',
                        lambda added_user, return_user: (False, user))
    monkeypatch.setattr(
        server_common, 'reload_for_new_request', lambda **kwargs: calls.append(
            ('request-context', kwargs)))
    capability = controller_capability.generate()

    controller_capability.clear_process_local()
    try:
        controller_capability.install_process_local(capability)
        internal_daemon_runner._run_daemon('exact-daemon-id', 7,
                                           *_CONTROLLER_OWNER)
        assert controller_capability.get_process_local() == capability
    finally:
        controller_capability.clear_process_local()

    assert calls == [
        ('title', 'SkyPilot:runtime-daemon:exact-daemon-id:123'),
        ('db', 7),
        ('resolve', 'exact-daemon-id'),
        'watchdog',
        ('clean-env', {
            'CLEAN': '1',
            'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': _CONTROLLER_OWNER[0],
            'SKYPILOT_SERVER_CONTROLLER_GENERATION': str(_CONTROLLER_OWNER[1]),
        }),
        ('plugins', plugins.PluginContext.EXECUTOR),
        'metrics-cleanup',
        ('request-context', {
            'client_entrypoint': None,
            'client_command': None,
            'using_remote_api_server': False,
            'user': user,
            'request_id': 'exact-daemon-id',
        }),
        'run',
    ]
