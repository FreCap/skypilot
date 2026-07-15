"""Slurm SSH transport helpers."""

import os
import shlex
from typing import Any

from paramiko.config import SSHConfig

from sky.skylet import constants

DEFAULT_SLURM_PATH = '~/.slurm/config'

# SSH host key filename for sshd.
SLURM_SSHD_HOST_KEY_FILENAME = 'skypilot_host_key'


def pyxis_container_name(cluster_name_on_cloud: str) -> str:
    """Get the pyxis container name that gets passed to --container-name."""
    return cluster_name_on_cloud


def get_slurm_ssh_config() -> SSHConfig:
    """Get the Slurm SSH config."""
    slurm_config_path = os.path.expanduser(DEFAULT_SLURM_PATH)
    slurm_config = SSHConfig.from_path(slurm_config_path)
    return slurm_config


def get_identity_file(ssh_config_dict: dict[str, Any]) -> str | None:
    """Get the first identity file from SSH config, or None if not specified."""
    identity_files = ssh_config_dict.get('identityfile')
    if identity_files:
        return identity_files[0]
    return None


def get_identities_only(ssh_config_dict: dict[str, Any]) -> bool:
    """Check if IdentitiesOnly is set to yes in SSH config.

    Returns True if IdentitiesOnly is explicitly set to 'yes', False otherwise.
    """
    identities_only = ssh_config_dict.get('identitiesonly', '')
    return identities_only.lower() == 'yes'


def srun_sshd_command(
    job_id: str,
    target_node: str,
    unix_user: str,
    cluster_name_on_cloud: str,
    is_container_image: bool,
) -> str:
    """Build srun command for launching sshd -i inside a Slurm job.

    This is used by the API server to proxy SSH connections to Slurm jobs
    via sshd running in inetd mode within srun.

    Args:
        job_id: The Slurm job ID
        target_node: The target compute node hostname
        unix_user: The Unix user for the job
        cluster_name_on_cloud: SkyPilot cluster name on Slurm side.
        is_container_image: Whether the cluster is on containers.

    Returns:
        List of command arguments to be extended to ssh base command
    """
    # We use ~username to ensure we use the real home of the user ssh'ing in,
    # because we override the home directory in SlurmCommandRunner.run.
    user_home_ssh_dir = f'~{unix_user}/.ssh'

    # TODO(kevin): SSH sessions don't inherit Slurm env vars (SLURM_*, CUDA_*,
    # etc.) because sshd/dropbear spawns a fresh shell. Fix by capturing env
    # to a file and sourcing it.

    if is_container_image:
        # Dropbear + socat bridge for container mode.
        # See slurm-ray.yml.j2 for why we use Dropbear instead of OpenSSH.
        # Dropbear's -i (inetd) mode expects a socket fd on stdin, but srun
        # provides pipes. socat bridges stdin/stdout to a TCP socket.
        ssh_bootstrap_cmd = (
            # Find dropbear in PATH
            'DROPBEAR=$(command -v dropbear); '
            'if [ -z "$DROPBEAR" ]; then '
            'echo "dropbear not found" >&2; exit 1; fi; '
            # Find a free port in the ephemeral range
            'while :; do '
            'PORT=$((30000 + RANDOM % 30000)); '
            'ss -tln | awk \'{print $4}\' | grep -q ":$PORT$" || break; '
            'done; '
            # Start dropbear and wait for it to bind
            '"$DROPBEAR" -F -s -R -p "127.0.0.1:$PORT" & '
            'DROPBEAR_PID=$!; '
            'trap "kill $DROPBEAR_PID 2>/dev/null" EXIT; '
            'for i in $(seq 1 50); do '
            'ss -tlnp 2>/dev/null | grep -q ":$PORT.*pid=$DROPBEAR_PID" '
            '&& break; sleep 0.1; done; '
            'if ! ss -tlnp 2>/dev/null | '
            'grep -q ":$PORT.*pid=$DROPBEAR_PID"; then '
            'echo "Error: Timed out waiting for dropbear to start." >&2; '
            'exit 1; fi; '
            'socat STDIO TCP:127.0.0.1:$PORT')
        return shlex.join([
            'srun',
            '--overlap',
            '--quiet',
            '--unbuffered',
            '--jobid',
            job_id,
            '--nodes=1',
            '--ntasks=1',
            '--ntasks-per-node=1',
            '-w',
            target_node,
            '--container-remap-root',
            f'--container-name='
            f'{pyxis_container_name(cluster_name_on_cloud)}:exec',
            '/bin/bash',
            '-c',
            ssh_bootstrap_cmd,
        ])

    # Non-container: OpenSSH sshd
    return shlex.join([
        'srun',
        '--quiet',
        '--unbuffered',
        '--overlap',
        '--jobid',
        job_id,
        '-w',
        target_node,
        '/usr/sbin/sshd',
        '-i',  # Uses stdin/stdout
        '-e',  # Writes errors to stderr
        '-f',  # Use /dev/null to avoid reading system sshd_config
        '/dev/null',
        '-h',
        f'{user_home_ssh_dir}/{SLURM_SSHD_HOST_KEY_FILENAME}',
        '-o',
        f'AuthorizedKeysFile={user_home_ssh_dir}/authorized_keys',
        '-o',
        'PasswordAuthentication=no',
        '-o',
        'PubkeyAuthentication=yes',
        # If UsePAM is enabled, we will not be able to run sshd(8)
        # as a non-root user.
        # See https://man7.org/linux/man-pages/man5/sshd_config.5.html
        '-o',
        'UsePAM=no',
        '-o',
        f'AcceptEnv={constants.SKY_CLUSTER_NAME_ENV_VAR_KEY}',
    ])
