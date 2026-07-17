"""SSH and Kubernetes port-forward tunnel lifecycle helpers."""

import queue as queue_lib
import subprocess
import threading

from sky import exceptions
from sky import sky_logging
from sky.utils import command_runner

logger = sky_logging.init_logger(__name__)

_ACK_MESSAGE = 'ack'
_FORWARDING_FROM_MESSAGE = 'Forwarding from'
_TUNNEL_START_TIMEOUT_SECONDS = 30
_PROCESS_TERMINATION_TIMEOUT_SECONDS = 5


def _terminate_process(process: subprocess.Popen) -> int:
    """Terminate and reap a tunnel process, escalating when necessary."""
    returncode = process.poll()
    if returncode is not None:
        return returncode
    try:
        process.terminate()
    except ProcessLookupError:
        # The process exited between poll() and terminate(). It is still our
        # child, so wait once to reap it and return its real status.
        return process.wait()
    try:
        return process.wait(timeout=_PROCESS_TERMINATION_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        return process.wait()


def cluster_tunnel_lock_id(cluster_name: str) -> str:
    """Get the lock ID for cluster tunnel operations."""
    return f'{cluster_name}_ssh_tunnel'


def open_ssh_tunnel(head_runner: command_runner.SSHCommandRunner |
                    command_runner.KubernetesCommandRunner,
                    port_forward: tuple[int, int]) -> subprocess.Popen:
    local_port, remote_port = port_forward
    if isinstance(head_runner, command_runner.SSHCommandRunner):
        # Disabling ControlMaster makes things easier to reason about
        # with respect to resource management/ownership,
        # as killing the process will close the tunnel too.
        head_runner.disable_control_master = True
        head_runner.port_forward_execute_remote_command = True

    # The default connect_timeout of 1s is too short for
    # connecting to clusters using a jump server.
    # We use NON_INTERACTIVE mode to avoid allocating a pseudo-tty,
    # which is counted towards non-idleness.
    cmd: list[str] = head_runner.port_forward_command(
        [(local_port, remote_port)],
        connect_timeout=5,
        ssh_mode=command_runner.SshMode.NON_INTERACTIVE)
    if isinstance(head_runner, command_runner.SSHCommandRunner):
        # cat so the command doesn't exit until we kill it
        cmd += [f'"echo {_ACK_MESSAGE} && cat"']
    cmd_str = ' '.join(cmd)
    logger.debug(f'Running port forward command: {cmd_str}')
    ssh_tunnel_proc = subprocess.Popen(cmd_str,
                                       shell=True,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       start_new_session=True,
                                       text=True)
    # Read through banners and other pre-readiness output in one daemon thread.
    # The caller uses a single bounded blocking wait instead of waking every
    # 100 ms to poll both the queue and subprocess.
    readiness_queue: queue_lib.Queue[str | None] = queue_lib.Queue()

    def _read_until_ready() -> None:
        assert ssh_tunnel_proc.stdout is not None
        try:
            for line in iter(ssh_tunnel_proc.stdout.readline, ''):
                is_ready = (isinstance(head_runner,
                                       command_runner.SSHCommandRunner) and
                            line == f'{_ACK_MESSAGE}\n')
                is_ready = is_ready or (isinstance(
                    head_runner, command_runner.KubernetesCommandRunner) and
                                        _FORWARDING_FROM_MESSAGE in line)
                if is_ready:
                    readiness_queue.put(line)
                    return
        except (OSError, ValueError):
            # Process termination closes the pipe while the daemon may be
            # blocked in readline(). The caller already owns error reporting.
            pass
        readiness_queue.put(None)

    stdout_thread = threading.Thread(target=_read_until_ready, daemon=True)
    stdout_thread.start()
    try:
        ack = readiness_queue.get(timeout=_TUNNEL_START_TIMEOUT_SECONDS)
    except queue_lib.Empty as e:
        returncode = _terminate_process(ssh_tunnel_proc)
        assert ssh_tunnel_proc.stderr is not None
        stderr = ssh_tunnel_proc.stderr.read()
        if isinstance(head_runner, command_runner.SSHCommandRunner):
            head_runner.note_transport_failure(returncode)
        raise exceptions.CommandError(
            returncode=returncode,
            command=cmd_str,
            error_msg=('Port forward did not become ready within '
                       f'{_TUNNEL_START_TIMEOUT_SECONDS} seconds'),
            detailed_reason=stderr) from e

    if ack is None:
        # EOF before readiness may race process exit. Reap a still-live process
        # so a closed stdout pipe cannot be mistaken for a usable tunnel.
        _terminate_process(ssh_tunnel_proc)

    if (ack is not None and
            isinstance(head_runner, command_runner.KubernetesCommandRunner)):
        # On kind clusters, this error occurs if we make a request immediately
        # after the port-forward is established on a new pod. Poll the remote
        # port before returning; real Kubernetes clusters do not normally need
        # the extra delay.
        timeout = 5
        port_check_cmd = (
            # We install netcat in our ray-node container,
            # so we can use it here. (See kubernetes-ray.yml.j2)
            f'end=$((SECONDS+{timeout})); '
            f'while ! nc -z -w 1 localhost {remote_port}; do '
            'if (( SECONDS >= end )); then exit 1; fi; '
            'sleep 0.1; '
            'done')
        returncode, stdout, stderr = head_runner.run(port_check_cmd,
                                                     require_outputs=True,
                                                     stream_logs=False)
        if returncode != 0:
            _terminate_process(ssh_tunnel_proc)
            error_msg = f'Failed to check remote port {remote_port}'
            if stdout:
                error_msg += f'\n-- stdout --\n{stdout}\n'
            raise exceptions.CommandError(returncode=returncode,
                                          command=cmd_str,
                                          error_msg=error_msg,
                                          detailed_reason=stderr)

    if ssh_tunnel_proc.poll() is not None:
        stdout, stderr = ssh_tunnel_proc.communicate()
        error_msg = 'Port forward failed'
        if stdout:
            error_msg += f'\n-- stdout --\n{stdout}\n'
        # The tunnel executes ssh directly (not via runner.run()), so report
        # the transport failure back to the runner: a bypassed SSM proxy is
        # restored before the caller's retry reuses this runner.
        if isinstance(head_runner, command_runner.SSHCommandRunner):
            head_runner.note_transport_failure(ssh_tunnel_proc.returncode)
        raise exceptions.CommandError(returncode=ssh_tunnel_proc.returncode,
                                      command=cmd_str,
                                      error_msg=error_msg,
                                      detailed_reason=stderr)
    return ssh_tunnel_proc


# Preserve the public facade identity for introspection and serialization.
cluster_tunnel_lock_id.__module__ = 'sky.backends.backend_utils'
open_ssh_tunnel.__module__ = 'sky.backends.backend_utils'
