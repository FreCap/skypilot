"""Characterization tests for Slurm SSH transport helpers."""

import pickle
import shlex
from unittest import mock

import pytest

from sky.provision.slurm import utils as slurm_utils


def test_get_slurm_ssh_config_uses_expanded_default_path() -> None:
    sentinel_config = object()
    with mock.patch('os.path.expanduser',
                    return_value='/tmp/slurm-config') as expanduser, \
         mock.patch.object(slurm_utils.SSHConfig,
                           'from_path',
                           return_value=sentinel_config) as from_path:
        assert slurm_utils.get_slurm_ssh_config() is sentinel_config

    expanduser.assert_called_once_with(slurm_utils.DEFAULT_SLURM_PATH)
    from_path.assert_called_once_with('/tmp/slurm-config')


@pytest.mark.parametrize('config,identity_file,identities_only', [
    ({
        'identityfile': ['/tmp/key-a', '/tmp/key-b'],
        'identitiesonly': 'yes',
    }, '/tmp/key-a', True),
    ({
        'identitiesonly': 'YES'
    }, None, True),
    ({
        'identityfile': []
    }, None, False),
    ({}, None, False),
])
def test_ssh_identity_projection(config: dict[str, object],
                                 identity_file: str | None,
                                 identities_only: bool) -> None:
    assert slurm_utils.get_identity_file(config) == identity_file
    assert slurm_utils.get_identities_only(config) is identities_only


def test_srun_sshd_command_for_host_job() -> None:
    command = slurm_utils.srun_sshd_command(job_id='123',
                                            target_node='node-1',
                                            unix_user='alice',
                                            cluster_name_on_cloud='cluster-a',
                                            is_container_image=False)

    assert shlex.split(command) == [
        'srun', '--quiet', '--unbuffered', '--overlap', '--jobid', '123', '-w',
        'node-1', '/usr/sbin/sshd', '-i', '-e', '-f', '/dev/null', '-h',
        '~alice/.ssh/skypilot_host_key', '-o',
        'AuthorizedKeysFile=~alice/.ssh/authorized_keys', '-o',
        'PasswordAuthentication=no', '-o', 'PubkeyAuthentication=yes', '-o',
        'UsePAM=no', '-o', 'AcceptEnv=SKY_CLUSTER_NAME'
    ]


def test_srun_sshd_command_for_container_job() -> None:
    command = slurm_utils.srun_sshd_command(job_id='123',
                                            target_node='node-1',
                                            unix_user='alice',
                                            cluster_name_on_cloud='cluster-a',
                                            is_container_image=True)
    tokens = shlex.split(command)

    assert tokens[:14] == [
        'srun', '--overlap', '--quiet', '--unbuffered', '--jobid', '123',
        '--nodes=1', '--ntasks=1', '--ntasks-per-node=1', '-w', 'node-1',
        '--container-remap-root', '--container-name=cluster-a:exec', '/bin/bash'
    ]
    assert tokens[14] == '-c'
    assert 'DROPBEAR=$(command -v dropbear)' in tokens[15]
    assert 'socat STDIO TCP:127.0.0.1:$PORT' in tokens[15]


def test_pyxis_container_name_is_stable() -> None:
    assert slurm_utils.pyxis_container_name('cluster-a') == 'cluster-a'


@pytest.mark.parametrize('symbol_name', [
    'pyxis_container_name',
    'get_slurm_ssh_config',
    'get_identity_file',
    'get_identities_only',
    'srun_sshd_command',
])
def test_ssh_helper_keeps_facade_and_pickle_identity(symbol_name: str) -> None:
    symbol = getattr(slurm_utils, symbol_name)

    assert symbol.__module__ == slurm_utils.__name__
    assert pickle.loads(pickle.dumps(symbol)) is symbol
