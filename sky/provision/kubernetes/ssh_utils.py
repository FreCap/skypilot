"""Kubernetes SSH transport helpers."""

import os
import shutil
import subprocess

from sky.skylet import constants
from sky.utils import kubernetes_enums
from sky.utils import ux_utils

# We add a version suffix to the port-forward proxy command to ensure backward
# compatibility and avoid overwriting the older version.
PORT_FORWARD_PROXY_CMD_TEMPLATE: str = (
    'kubernetes-port-forward-proxy-command.sh')
PORT_FORWARD_PROXY_CMD_VERSION: int = 3
PORT_FORWARD_PROXY_CMD_PATH: str = (
    '~/.sky/kubernetes-port-forward-proxy-command-'
    f'v{PORT_FORWARD_PROXY_CMD_VERSION}.sh')


def construct_ssh_jump_command(private_key_path: str,
                               ssh_jump_ip: str,
                               ssh_jump_port: int | None = None,
                               ssh_jump_user: str = 'sky',
                               proxy_cmd_path: str | None = None,
                               proxy_cmd_target_pod: str | None = None,
                               current_kube_context: str | None = None,
                               current_kube_namespace: str | None = None,
                               host_network: bool = False) -> str:
    ssh_jump_proxy_command = (f'ssh -tt -i {private_key_path} '
                              '-o StrictHostKeyChecking=no '
                              '-o UserKnownHostsFile=/dev/null '
                              f'-o IdentitiesOnly=yes '
                              r'-W \[%h\]:%p '
                              f'{ssh_jump_user}@{ssh_jump_ip}')
    if ssh_jump_port is not None:
        ssh_jump_proxy_command += f' -p {ssh_jump_port} '
    if proxy_cmd_path is not None:
        proxy_cmd_path = os.path.expanduser(proxy_cmd_path)
        # adding execution permission to the proxy command script
        os.chmod(proxy_cmd_path, os.stat(proxy_cmd_path).st_mode | 0o111)
        kube_context_flag = f'-c {current_kube_context} ' if (
            current_kube_context is not None) else ''
        kube_namespace_flag = f'-n {current_kube_namespace} ' if (
            current_kube_namespace is not None) else ''
        # Pass hostNetwork as a flag: it's known statically here, so the
        # proxy script avoids a per-connection `kubectl get pod` probe
        # (zero extra kubectl calls on the common non-hostNetwork path).
        host_network_flag = '-N ' if host_network else ''
        ssh_jump_proxy_command += (f' -o ProxyCommand=\'{proxy_cmd_path} '
                                   f'{kube_context_flag}'
                                   f'{kube_namespace_flag}'
                                   f'{host_network_flag}'
                                   f'{proxy_cmd_target_pod}\'')
    return ssh_jump_proxy_command


def get_ssh_proxy_command(
    pod_name: str,
    private_key_path: str,
    context: str | None,
    namespace: str,
    host_network: bool = False,
) -> str:
    """Generates the SSH proxy command to connect to the pod.

    Uses a direct port-forwarding.

    By default, establishing an SSH connection creates a communication
    channel to a remote node by setting up a TCP connection. When a
    ProxyCommand is specified, this default behavior is overridden. The command
    specified in ProxyCommand is executed, and its standard input and output
    become the communication channel for the SSH session.

    Pods within a Kubernetes cluster have internal IP addresses that are
    typically not accessible from outside the cluster. Since the default TCP
    connection of SSH won't allow access to these pods, we employ a
    ProxyCommand to establish the required communication channel.

    'kubectl port-forward' sets up a tunnel between a local port
    (127.0.0.1:23100) and port 22 of the provisioned pod. Then we establish TCP
    connection to the local end of this tunnel, 127.0.0.1:23100, using 'socat'.
    All of this is done in a ProxyCommand script. Any stdin provided on the
    local machine is forwarded through this tunnel to the application
    (SSH server) listening in the pod. Similarly, any output from the
    application in the pod is tunneled back and displayed in the terminal on
    the local machine.

    Args:
        pod_name: str; The Kubernetes pod name that will be used as the
            target for SSH.
        private_key_path: str; Path to the private key to use for SSH.
            This key must be authorized to access the SSH jump pod.
        namespace: Kubernetes namespace to use.
        host_network: bool; Whether the target pod runs with
            ``hostNetwork: true``. When True the proxy script discovers
            the pod's probed sshd port from the cluster's ConfigMap;
            when False it skips that lookup and uses port 22. Passed as
            a flag so the script needs no per-connection `kubectl get
            pod` probe to determine this.
    """
    ssh_jump_ip = '127.0.0.1'  # Local end of the port-forward tunnel
    assert private_key_path is not None, 'Private key path must be provided'
    ssh_jump_proxy_command_path = create_proxy_command_script()
    ssh_jump_proxy_command = construct_ssh_jump_command(
        private_key_path,
        ssh_jump_ip,
        ssh_jump_user=constants.SKY_SSH_USER_PLACEHOLDER,
        proxy_cmd_path=ssh_jump_proxy_command_path,
        proxy_cmd_target_pod=pod_name,
        # We embed both the current context and namespace to the SSH proxy
        # command to make sure SSH still works when the current
        # context/namespace is changed by the user.
        current_kube_context=context,
        current_kube_namespace=namespace,
        host_network=host_network)
    return ssh_jump_proxy_command


def create_proxy_command_script() -> str:
    """Creates a ProxyCommand script that uses kubectl port-forward to setup
    a tunnel between a local port and the SSH server in the pod.

    Returns:
        str: Path to the ProxyCommand script.
    """
    port_fwd_proxy_cmd_path = os.path.expanduser(PORT_FORWARD_PROXY_CMD_PATH)
    os.makedirs(os.path.dirname(port_fwd_proxy_cmd_path),
                exist_ok=True,
                mode=0o700)

    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    template_path = os.path.join(root_dir, 'templates',
                                 PORT_FORWARD_PROXY_CMD_TEMPLATE)
    # Copy the template to the proxy command path. We create a copy to allow
    # different users sharing the same SkyPilot installation to have their own
    # proxy command scripts.
    shutil.copy(template_path, port_fwd_proxy_cmd_path)
    # Set the permissions to 700 to ensure only the owner can read, write,
    # and execute the file.
    os.chmod(port_fwd_proxy_cmd_path, 0o700)
    # Return the path to the proxy command script without expanding the user
    # home directory to be compatible when a SSH is called from a client in
    # client-server mode.
    return PORT_FORWARD_PROXY_CMD_PATH


def check_port_forward_mode_dependencies(
        raise_error: bool = True) -> list[str] | None:
    """Checks if 'socat' and 'nc' are installed

    Args:
        raise_error: set to true when the dependencies need to be present.
            set to false for `sky check`, where reason strings are compiled
            at the end.

    Returns: the reasons list if there are missing dependencies.
    """

    # errors
    socat_message = (
        '`socat` is required to setup Kubernetes cloud with '
        f'`{kubernetes_enums.KubernetesNetworkingMode.PORTFORWARD.value}` '  # pylint: disable=line-too-long
        'default networking mode and it is not installed. ')
    netcat_default_message = (
        '`nc` is required to setup Kubernetes cloud with '
        f'`{kubernetes_enums.KubernetesNetworkingMode.PORTFORWARD.value}` '  # pylint: disable=line-too-long
        'default networking mode and it is not installed. ')
    netcat_macos_message = (
        'The default MacOS `nc` is installed. However, for '
        f'`{kubernetes_enums.KubernetesNetworkingMode.PORTFORWARD.value}` '  # pylint: disable=line-too-long
        'default networking mode, GNU netcat is required. ')

    # save
    reasons = []
    required_binaries = []

    # Ensure socat is installed
    try:
        subprocess.run(['socat', '-V'],
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL,
                       check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        required_binaries.append('socat')
        reasons.append(socat_message)

    # Ensure netcat is installed
    #
    # In some cases, the user may have the default MacOS nc installed, which
    # does not support the -z flag. To use the -z flag for port scanning,
    # they need GNU nc installed. We check for this case and raise an error.
    try:
        netcat_output = subprocess.run(['nc', '-h'],
                                       capture_output=True,
                                       check=False)
        nc_mac_installed = netcat_output.returncode == 1 and 'apple' in str(
            netcat_output.stderr)

        if nc_mac_installed:
            required_binaries.append('netcat')
            reasons.append(netcat_macos_message)
        elif netcat_output.returncode != 0:
            required_binaries.append('netcat')
            reasons.append(netcat_default_message)

    except FileNotFoundError:
        required_binaries.append('netcat')
        reasons.append(netcat_default_message)

    if required_binaries:
        reasons.extend([
            'On Debian/Ubuntu, install the missing dependenc(ies) with:',
            f'  $ sudo apt install {" ".join(required_binaries)}',
            'On MacOS, install with: ',
            f'  $ brew install {" ".join(required_binaries)}',
        ])
        if raise_error:
            with ux_utils.print_exception_no_traceback():
                raise RuntimeError('\n'.join(reasons))
        return reasons
    return None
