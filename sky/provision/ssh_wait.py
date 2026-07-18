"""SSH readiness probes for provisioned clusters."""
import collections
import os
import random
import shlex
import socket
import subprocess
import time
import typing

from sky import sky_logging
from sky import skypilot_config
from sky.provision import common as provision_common
from sky.utils import common_utils
from sky.utils import ssm_direct
from sky.utils import subprocess_utils
from sky.utils import ux_utils

logger = sky_logging.init_logger('sky.provisioner')

_SshProbeCommand = typing.Callable[[str, int, str, str, int, str | None],
                                   list[str]]
_SshWaiter = typing.Callable[..., tuple[bool, str]]


def ssh_probe_command(ip: str,
                      ssh_port: int,
                      ssh_user: str,
                      ssh_private_key: str,
                      ssh_probe_timeout: int,
                      ssh_proxy_command: str | None = None) -> list[str]:
    # NOTE: Ray uses 'uptime' command, we use the same setting here.
    command = [
        'ssh',
        '-T',
        '-i',
        ssh_private_key,
        f'{ssh_user}@{ip}',
        '-p',
        str(ssh_port),
        '-o',
        'StrictHostKeyChecking=no',
        '-o',
        'PasswordAuthentication=no',
        '-o',
        f'ConnectTimeout={ssh_probe_timeout}s',
        '-o',
        f'UserKnownHostsFile={os.devnull}',
        '-o',
        'IdentitiesOnly=yes',
        '-o',
        'AddKeysToAgent=yes',
        '-o',
        'ExitOnForwardFailure=yes',
        '-o',
        'ServerAliveInterval=5',
        '-o',
        'ServerAliveCountMax=3',
    ]
    if ssh_proxy_command is not None:
        command += ['-o', f'ProxyCommand={ssh_proxy_command}']
    command += ['uptime']
    return command


def wait_ssh_connection_direct(ip: str, ssh_port: int, ssh_user: str,
                               ssh_private_key: str, ssh_probe_timeout: int,
                               ssh_control_name: str | None,
                               ssh_proxy_command: str | None,
                               indirect_waiter: _SshWaiter,
                               command_builder: _SshProbeCommand,
                               **kwargs) -> tuple[bool, str]:
    """Wait for SSH connection using raw sockets, and a SSH connection.

    Using raw socket is more efficient than using SSH command to probe the
    connection, before the SSH connection is ready. We use a actual SSH command
    connection to test the connection, after the raw socket connection is ready
    to make sure the SSH connection is actually ready.

    Returns:
        A tuple of (success, stderr).
    """
    del kwargs  # unused
    assert ssh_proxy_command is None, 'SSH proxy command is not supported.'
    try:
        success = False
        stderr = ''
        with socket.create_connection((ip, ssh_port), timeout=1) as s:
            if s.recv(100).startswith(b'SSH'):
                # Wait for SSH being actually ready, otherwise we may get the
                # following error:
                # "System is booting up. Unprivileged users are not permitted to
                # log in yet".
                success = True
        if success:
            return indirect_waiter(ip, ssh_port, ssh_user, ssh_private_key,
                                   ssh_probe_timeout, ssh_control_name,
                                   ssh_proxy_command)
    except TimeoutError:  # this is the most expected exception
        stderr = f'Timeout: SSH connection to {ip} is not ready.'
    except Exception as e:  # pylint: disable=broad-except
        stderr = f'Error: {common_utils.format_exception(e)}'
    command = command_builder(ip, ssh_port, ssh_user, ssh_private_key,
                              ssh_probe_timeout, ssh_proxy_command)
    logger.debug(f'Waiting for SSH to {ip}. Try: '
                 f'{shlex.join(command)}. '
                 f'{stderr}')
    return False, stderr


def wait_ssh_connection_indirect(ip: str, ssh_port: int, ssh_user: str,
                                 ssh_private_key: str, ssh_probe_timeout: int,
                                 ssh_control_name: str | None,
                                 ssh_proxy_command: str | None,
                                 command_builder: _SshProbeCommand,
                                 **kwargs) -> tuple[bool, str]:
    """Wait for SSH connection using SSH command.

    Returns:
        A tuple of (success, stderr).
    """
    del ssh_control_name, kwargs  # unused
    command = command_builder(ip, ssh_port, ssh_user, ssh_private_key,
                              ssh_probe_timeout, ssh_proxy_command)
    message = f'Waiting for SSH using command: {shlex.join(command)}'
    logger.debug(message)
    try:
        proc = subprocess.run(command,
                              shell=False,
                              check=False,
                              timeout=ssh_probe_timeout,
                              stdout=subprocess.DEVNULL,
                              stderr=subprocess.PIPE)
        if proc.returncode != 0:
            stderr = proc.stderr.decode('utf-8')
            stderr = f'Error: {stderr}'
            logger.debug(f'{message}{stderr}')
            return False, stderr
    except subprocess.TimeoutExpired as e:
        stderr = f'Error: {str(e)}'
        logger.debug(f'{message}Error: {e}')
        return False, stderr
    return True, ''


def wait_for_ssh(cluster_info: provision_common.ClusterInfo,
                 ssh_credentials: dict[str, str], direct_waiter: _SshWaiter,
                 indirect_waiter: _SshWaiter) -> None:
    """Wait until SSH is ready.

    Raises:
        RuntimeError: If the SSH connection is not ready after timeout.
    """
    if (cluster_info.has_external_ips() and
            ssh_credentials.get('ssh_proxy_command') is None):
        # If we can access public IPs, then it is more efficient to test SSH
        # connection with raw sockets.
        waiter = direct_waiter
    else:
        # See https://github.com/skypilot-org/skypilot/pull/1512
        waiter = indirect_waiter
    ip_list = cluster_info.get_feasible_ips()
    port_list = cluster_info.get_ssh_ports()

    timeout = 60 * 10  # 10-min maximum timeout
    start = time.time()
    # use a queue for SSH querying
    ips = collections.deque(ip_list)
    ssh_ports = collections.deque(port_list)

    # Each SSM-proxied probe opens a session against the account-wide SSM
    # StartSession TPS quota (default 3), so probing every node in parallel
    # once per second saturates the quota for the whole account. Space the
    # probes out with jittered backoff instead; the quota-free transports
    # keep the tight 1s cadence.
    proxy_command = ssh_credentials.get('ssh_proxy_command')
    is_ssm_proxy = 'ssm start-session' in (proxy_command or '')
    # Prefer proxy-less probes when the target is directly reachable: the
    # successful direct handshake is what authorizes SSHCommandRunner to
    # bypass the SSM proxy for the rest of provisioning (ssm_direct.py).
    # skypilot_config is thread-local, so read the kill-switch here in the
    # parent thread, not in the probe threads.
    try_direct = (ssm_direct.is_skypilot_ssm_proxy(proxy_command) and
                  ssm_direct.is_enabled())
    # After this many failed direct probes, alternate direct/proxied
    # attempts: during boot a not-yet-ready sshd fails both transports
    # alike, but a TCP-open-yet-SSH-broken direct path must not be able to
    # stall the wait when only SSM works.
    max_exclusive_direct_failures = 5

    def _retry_ssh_thread(ip_ssh_port: tuple[str, int]):
        ip, ssh_port = ip_ssh_port
        success = False
        ssh_probe_timeout = skypilot_config.get_nested(
            ('provision', 'ssh_timeout'), 10)
        backoff = common_utils.Backoff(initial_backoff=2, max_backoff_factor=5)
        if is_ssm_proxy:
            # Backoff only paces retries; without this the first probe of
            # every worker fires simultaneously once the head is up, which
            # is itself a StartSession burst. Spread the first wave out.
            time.sleep(random.uniform(0, 5))
        attempt = 0
        direct_failures = 0
        while not success:
            attempted_direct = (try_direct and
                                (direct_failures < max_exclusive_direct_failures
                                 or attempt % 2 == 0) and
                                ssm_direct.tcp_reachable(ip, ssh_port))
            attempt += 1
            credentials = ssh_credentials
            if attempted_direct:
                credentials = dict(ssh_credentials)
                # Omitting the optional proxy argument is equivalent to
                # passing None and keeps this Dict[str, str] type-accurate.
                credentials.pop('ssh_proxy_command', None)
            success, stderr = waiter(ip,
                                     ssh_port,
                                     **credentials,
                                     ssh_probe_timeout=ssh_probe_timeout)
            if success:
                if attempted_direct:
                    ssm_direct.mark_direct_ok(ip, ssh_port)
                elif (try_direct and
                      direct_failures >= max_exclusive_direct_failures):
                    # Only SSM worked while the direct path kept failing
                    # past the boot window: stop runners from bypassing.
                    ssm_direct.mark_direct_failed(ip, ssh_port)
                break
            if attempted_direct:
                direct_failures += 1
            if time.time() - start > timeout:
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        f'Failed to SSH to {ip} after timeout {timeout}s, with '
                        f'{stderr}')
            sleep_secs = backoff.current_backoff() if is_ssm_proxy else 1
            logger.debug(f'Retrying in {sleep_secs:.1f} seconds...')
            time.sleep(sleep_secs)

    # try one node and multiprocess the rest
    if ips:
        ip = ips.popleft()
        ssh_port = ssh_ports.popleft()
        _retry_ssh_thread((ip, ssh_port))
    subprocess_utils.run_in_parallel(_retry_ssh_thread,
                                     list(zip(ips, ssh_ports)))
