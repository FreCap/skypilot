"""Characterization tests for backend SSH tunnel helpers."""

import io
import queue as queue_lib
import subprocess
from unittest import mock

import pytest

from sky import exceptions
from sky.backends import backend_utils
from sky.backends import ssh_tunnel
from sky.utils import command_runner


class _FakeProcess:
    """Minimal subprocess used by tunnel lifecycle tests."""

    def __init__(self,
                 output: str,
                 *,
                 returncode: int | None = None,
                 stderr: str = '',
                 wait_timeouts: int = 0,
                 terminate_error: bool = False) -> None:
        self.stdout = io.StringIO(output)
        self.stdin = io.StringIO()
        self.stderr = io.StringIO(stderr)
        self.returncode = returncode
        self.pid = 1234
        self.terminate_calls = 0
        self.wait_calls = []
        self.kill_calls = 0
        self._wait_timeouts = wait_timeouts
        self._killed = False
        self._terminate_error = terminate_error

    def poll(self) -> int | None:
        return self.returncode

    def communicate(self) -> tuple[str, str]:
        return self.stdout.read(), self.stderr.read()

    def terminate(self) -> None:
        self.terminate_calls += 1
        if self._terminate_error:
            self.returncode = 7
            raise ProcessLookupError

    def wait(self, timeout=None) -> int:
        self.wait_calls.append(timeout)
        if self._wait_timeouts > 0:
            self._wait_timeouts -= 1
            raise subprocess.TimeoutExpired('tunnel', timeout)
        if self.returncode is None:
            self.returncode = -9 if self._killed else -15
        return self.returncode

    def kill(self) -> None:
        self.kill_calls += 1
        self._killed = True


class _NoPollingQueue(queue_lib.Queue):
    """Queue that rejects the old get-nowait/sleep polling loop."""

    last_instance = None

    def __init__(self):
        super().__init__()
        self.blocking_get_timeouts = []
        type(self).last_instance = self

    def get(self, block=True, timeout=None):
        self.blocking_get_timeouts.append(timeout)
        return super().get(block=block, timeout=timeout)

    def get_nowait(self):
        raise AssertionError('tunnel readiness must use one blocking wait')


class _AlwaysTimeoutQueue(_NoPollingQueue):

    def put(self, item, block=True, timeout=None):
        del item, block, timeout

    def get(self, block=True, timeout=None):
        del block, timeout
        raise queue_lib.Empty


class _FakeSSHRunner:
    """SSH runner test double that records tunnel interactions."""

    def __init__(self) -> None:
        self.disable_control_master = False
        self.port_forward_execute_remote_command = False
        self.port_forward_calls = []
        self.transport_failures = []

    def port_forward_command(self, forwards, *, connect_timeout, ssh_mode):
        self.port_forward_calls.append((forwards, connect_timeout, ssh_mode))
        return ['ssh', '-L', '10000:localhost:46580', 'cluster']

    def note_transport_failure(self, returncode: int) -> None:
        self.transport_failures.append(returncode)


class _FakeKubernetesRunner:
    """Kubernetes runner test double that records readiness checks."""

    def __init__(self) -> None:
        self.port_forward_calls = []
        self.run_calls = []

    def port_forward_command(self, forwards, *, connect_timeout, ssh_mode):
        self.port_forward_calls.append((forwards, connect_timeout, ssh_mode))
        return ['kubectl', 'port-forward', 'pod/head', '10000:46580']

    def run(self, command, *, require_outputs, stream_logs):
        self.run_calls.append((command, require_outputs, stream_logs))
        return 0, '', ''


def test_open_ssh_tunnel_configures_runner_and_waits_for_ack(monkeypatch):
    monkeypatch.setattr(command_runner, 'SSHCommandRunner', _FakeSSHRunner)
    monkeypatch.setattr(command_runner, 'KubernetesCommandRunner',
                        _FakeKubernetesRunner)
    process = _FakeProcess('ack\n')
    popen = mock.Mock(return_value=process)
    monkeypatch.setattr(subprocess, 'Popen', popen)
    runner = _FakeSSHRunner()

    result = backend_utils.open_ssh_tunnel(runner, (10000, 46580))

    assert result is process
    assert runner.disable_control_master
    assert runner.port_forward_execute_remote_command
    assert runner.port_forward_calls == [
        ([(10000, 46580)], 5, command_runner.SshMode.NON_INTERACTIVE)
    ]
    popen.assert_called_once_with(
        'ssh -L 10000:localhost:46580 cluster "echo ack && cat"',
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
        text=True)


def test_open_ssh_tunnel_ignores_output_before_ack_without_polling(monkeypatch):
    monkeypatch.setattr(command_runner, 'SSHCommandRunner', _FakeSSHRunner)
    monkeypatch.setattr(command_runner, 'KubernetesCommandRunner',
                        _FakeKubernetesRunner)
    monkeypatch.setattr(ssh_tunnel.queue_lib, 'Queue', _NoPollingQueue)
    process = _FakeProcess('remote banner\nstill connecting\nack\n')
    monkeypatch.setattr(subprocess, 'Popen', mock.Mock(return_value=process))

    result = backend_utils.open_ssh_tunnel(_FakeSSHRunner(), (10000, 46580))

    assert result is process
    assert process.stdout.tell() == len(
        'remote banner\nstill connecting\nack\n')
    assert _NoPollingQueue.last_instance.blocking_get_timeouts == [30]


@pytest.mark.parametrize('wait_timeouts,expected_kills', [(0, 0), (1, 1)])
def test_open_ssh_tunnel_times_out_and_reaps_process(monkeypatch, wait_timeouts,
                                                     expected_kills):
    monkeypatch.setattr(command_runner, 'SSHCommandRunner', _FakeSSHRunner)
    monkeypatch.setattr(command_runner, 'KubernetesCommandRunner',
                        _FakeKubernetesRunner)
    monkeypatch.setattr(ssh_tunnel.queue_lib, 'Queue', _AlwaysTimeoutQueue)
    monkeypatch.setattr(ssh_tunnel,
                        '_TUNNEL_START_TIMEOUT_SECONDS',
                        0.25,
                        raising=False)
    process = _FakeProcess('',
                           stderr='still connecting',
                           wait_timeouts=wait_timeouts)
    monkeypatch.setattr(subprocess, 'Popen', mock.Mock(return_value=process))
    runner = _FakeSSHRunner()

    with pytest.raises(exceptions.CommandError) as exc_info:
        backend_utils.open_ssh_tunnel(runner, (10000, 46580))

    assert process.terminate_calls == 1
    assert process.wait_calls == ([5, None] if expected_kills else [5])
    assert process.kill_calls == expected_kills
    assert runner.transport_failures == [process.returncode]
    assert exc_info.value.returncode == process.returncode
    assert exc_info.value.error_msg == (
        'Port forward did not become ready within 0.25 seconds')
    assert exc_info.value.detailed_reason == 'still connecting'


def test_terminate_process_reaps_poll_terminate_race():
    process = _FakeProcess('', terminate_error=True)

    assert ssh_tunnel._terminate_process(  # pylint: disable=protected-access
        process) == 7

    assert process.terminate_calls == 1
    assert process.wait_calls == [None]
    assert process.kill_calls == 0


def test_open_ssh_tunnel_checks_kubernetes_remote_port(monkeypatch):
    monkeypatch.setattr(command_runner, 'SSHCommandRunner', _FakeSSHRunner)
    monkeypatch.setattr(command_runner, 'KubernetesCommandRunner',
                        _FakeKubernetesRunner)
    process = _FakeProcess('Forwarding from 127.0.0.1:10000\n')
    monkeypatch.setattr(subprocess, 'Popen', mock.Mock(return_value=process))
    runner = _FakeKubernetesRunner()

    result = backend_utils.open_ssh_tunnel(runner, (10000, 46580))

    assert result is process
    assert runner.port_forward_calls == [
        ([(10000, 46580)], 5, command_runner.SshMode.NON_INTERACTIVE)
    ]
    assert len(runner.run_calls) == 1
    command, require_outputs, stream_logs = runner.run_calls[0]
    assert 'nc -z -w 1 localhost 46580' in command
    assert require_outputs
    assert not stream_logs


def test_open_ssh_tunnel_reaps_failed_kubernetes_port_check(monkeypatch):
    monkeypatch.setattr(command_runner, 'SSHCommandRunner', _FakeSSHRunner)
    monkeypatch.setattr(command_runner, 'KubernetesCommandRunner',
                        _FakeKubernetesRunner)
    process = _FakeProcess('Forwarding from 127.0.0.1:10000\n')
    monkeypatch.setattr(subprocess, 'Popen', mock.Mock(return_value=process))
    runner = _FakeKubernetesRunner()
    runner.run = mock.Mock(return_value=(1, '', 'connection refused'))

    with pytest.raises(exceptions.CommandError, match='Failed to check'):
        backend_utils.open_ssh_tunnel(runner, (10000, 46580))

    assert process.terminate_calls == 1
    assert process.wait_calls == [5]
    assert process.kill_calls == 0


def test_open_ssh_tunnel_reports_transport_failure(monkeypatch):
    monkeypatch.setattr(command_runner, 'SSHCommandRunner', _FakeSSHRunner)
    monkeypatch.setattr(command_runner, 'KubernetesCommandRunner',
                        _FakeKubernetesRunner)
    process = _FakeProcess('partial output', returncode=7, stderr='denied')
    monkeypatch.setattr(subprocess, 'Popen', mock.Mock(return_value=process))
    runner = _FakeSSHRunner()

    with pytest.raises(exceptions.CommandError) as exc_info:
        backend_utils.open_ssh_tunnel(runner, (10000, 46580))

    assert runner.transport_failures == [7]
    assert exc_info.value.returncode == 7
    assert exc_info.value.command.endswith('"echo ack && cat"')
    assert exc_info.value.error_msg == 'Port forward failed'
    assert exc_info.value.detailed_reason == 'denied'


def test_open_ssh_tunnel_closed_stdout_reaps_live_process(monkeypatch):
    monkeypatch.setattr(command_runner, 'SSHCommandRunner', _FakeSSHRunner)
    monkeypatch.setattr(command_runner, 'KubernetesCommandRunner',
                        _FakeKubernetesRunner)
    monkeypatch.setattr(ssh_tunnel.queue_lib, 'Queue', _NoPollingQueue)
    process = _FakeProcess('', stderr='stdout closed')
    monkeypatch.setattr(subprocess, 'Popen', mock.Mock(return_value=process))
    runner = _FakeSSHRunner()

    with pytest.raises(exceptions.CommandError) as exc_info:
        backend_utils.open_ssh_tunnel(runner, (10000, 46580))

    assert process.terminate_calls == 1
    assert process.wait_calls == [5]
    assert process.kill_calls == 0
    assert runner.transport_failures == [-15]
    assert exc_info.value.returncode == -15
    assert exc_info.value.detailed_reason == 'stdout closed'


def test_cluster_tunnel_lock_id_is_stable():
    assert backend_utils.cluster_tunnel_lock_id(
        'controller') == 'controller_ssh_tunnel'


def test_backend_utils_tunnel_facade_is_direct_alias():
    assert backend_utils.cluster_tunnel_lock_id is (
        ssh_tunnel.cluster_tunnel_lock_id)
    assert backend_utils.open_ssh_tunnel is ssh_tunnel.open_ssh_tunnel
    assert backend_utils.open_ssh_tunnel.__module__ == (
        'sky.backends.backend_utils')
