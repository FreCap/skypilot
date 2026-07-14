"""SSH and Kubernetes port-forward tunnel lifecycle helpers."""

import queue as queue_lib
import subprocess
import threading
import time

from sky import exceptions
from sky import sky_logging
from sky.utils import command_runner

logger = sky_logging.init_logger(__name__)

_ACK_MESSAGE = 'ack'
_FORWARDING_FROM_MESSAGE = 'Forwarding from'


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
    # Wait until we receive an ack from the remote cluster or
    # the SSH connection times out.
    queue: queue_lib.Queue = queue_lib.Queue()
    stdout_thread = threading.Thread(
        target=lambda queue, stdout: queue.put(stdout.readline()),
        args=(queue, ssh_tunnel_proc.stdout),
        daemon=True)
    stdout_thread.start()
    while ssh_tunnel_proc.poll() is None:
        try:
            ack = queue.get_nowait()
        except queue_lib.Empty:
            ack = None
            time.sleep(0.1)
            continue
        assert ack is not None
        if isinstance(
                head_runner,
                command_runner.SSHCommandRunner) and ack == f'{_ACK_MESSAGE}\n':
            break
        elif isinstance(head_runner, command_runner.KubernetesCommandRunner
                       ) and _FORWARDING_FROM_MESSAGE in ack:
            # On kind clusters, this error occurs if we make a request
            # immediately after the port-forward is established on a new pod:
            # "Unhandled Error" err="an error occurred forwarding ... -> 46590:
            # failed to execute portforward in network namespace
            # "/var/run/netns/cni-...": failed to connect to localhost:46590
            # inside namespace "...", IPv4: dial tcp4 127.0.0.1:46590:
            # connect: connection refused
            # So we need to poll the port on the pod to check if it is open.
            # We did not observe this with real Kubernetes clusters.
            timeout = 5
            port_check_cmd = (
                # We install netcat in our ray-node container,
                # so we can use it here.
                # (See kubernetes-ray.yml.j2)
                f'end=$((SECONDS+{timeout})); '
                f'while ! nc -z -w 1 localhost {remote_port}; do '
                'if (( SECONDS >= end )); then exit 1; fi; '
                'sleep 0.1; '
                'done')
            returncode, stdout, stderr = head_runner.run(port_check_cmd,
                                                         require_outputs=True,
                                                         stream_logs=False)
            if returncode != 0:
                try:
                    ssh_tunnel_proc.terminate()
                    ssh_tunnel_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    ssh_tunnel_proc.kill()
                    ssh_tunnel_proc.wait()
                finally:
                    error_msg = (f'Failed to check remote port {remote_port}')
                    if stdout:
                        error_msg += f'\n-- stdout --\n{stdout}\n'
                    raise exceptions.CommandError(returncode=returncode,
                                                  command=cmd_str,
                                                  error_msg=error_msg,
                                                  detailed_reason=stderr)
            break

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
