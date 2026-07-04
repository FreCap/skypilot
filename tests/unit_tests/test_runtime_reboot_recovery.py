"""Tests for the VM runtime reboot recovery hook installed at provision time.

After an in-VM reboot, ray and skylet (started by provisioning as plain
processes) must be brought back by a boot hook so the cluster recovers
from INIT and autostop enforcement (which runs in skylet, on the VM)
resumes. These tests cover the recovery script generation, the base64
shipping of the installer, and the gating logic.
"""
# pylint: disable=protected-access,missing-class-docstring
import base64
import os
import re
from unittest import mock

import pytest

from sky.provision import common
from sky.provision import instance_setup

_INSTALL_CMD_RE = re.compile(r'^echo ([A-Za-z0-9+/=]+) \| base64 -d \| bash$')
_SCRIPT_SHIP_RE = re.compile(r'echo ([A-Za-z0-9+/=]+) \| base64 -d > ')


def _make_cluster_info(provider_name: str,
                       num_instances: int = 1,
                       docker_user=None) -> common.ClusterInfo:
    head_id = 'i-head'
    instances = {
        head_id: [
            common.InstanceInfo(instance_id=head_id,
                                internal_ip='10.0.0.1',
                                external_ip='1.2.3.4',
                                tags={})
        ]
    }
    for i in range(1, num_instances):
        instance_id = f'i-worker-{i}'
        instances[instance_id] = [
            common.InstanceInfo(instance_id=instance_id,
                                internal_ip=f'10.0.0.{i + 1}',
                                external_ip=None,
                                tags={})
        ]
    return common.ClusterInfo(instances=instances,
                              head_instance_id=head_id,
                              provider_name=provider_name,
                              docker_user=docker_user)


def _decode_installer(install_cmd: str) -> str:
    """Decodes the shipped installer script from the runner command."""
    match = _INSTALL_CMD_RE.match(install_cmd)
    assert match is not None
    return base64.b64decode(match.group(1)).decode('utf-8')


def _decode_recovery_script(installer: str) -> str:
    """Decodes the recovery script embedded in the installer."""
    match = _SCRIPT_SHIP_RE.search(installer)
    assert match is not None
    return base64.b64decode(match.group(1)).decode('utf-8')


class TestRecoveryScriptContent:

    def test_head_script_contains_ray_and_skylet_commands(self):
        ray_head_cmd = instance_setup.ray_head_start_command(
            '{"H100": 8}', None)
        script = instance_setup._runtime_recovery_head_script(ray_head_cmd)
        assert ray_head_cmd in script
        assert instance_setup.MAYBE_SKYLET_RESTART_CMD in script

    def test_head_script_guards_ray_start_with_status_probe(self):
        ray_head_cmd = instance_setup.ray_head_start_command(None, None)
        script = instance_setup._runtime_recovery_head_script(ray_head_cmd)
        # The start command (which begins with `ray stop`) must only run
        # in the negative branch of a `ray status` probe.
        guard_pos = script.index(' status > /dev/null')
        assert script.index(ray_head_cmd) > guard_pos

    def test_worker_script_contains_join_command(self):
        join_cmd = ('export SKYPILOT_RAY_HEAD_IP="10.0.0.1"; '
                    'export SKYPILOT_RAY_PORT=6380; ' +
                    instance_setup.ray_worker_start_command(
                        None, None, no_restart=True))
        script = instance_setup._runtime_recovery_worker_script(join_cmd)
        assert join_cmd in script
        # The no_restart join command carries the raylet/gcs-address guard.
        assert 'gcs-address' in script

    def test_scripts_are_bash_with_no_errexit(self):
        for script in (
                instance_setup._runtime_recovery_head_script('true'),
                instance_setup._runtime_recovery_worker_script('true'),
        ):
            assert script.startswith('#!/usr/bin/env bash')
            assert 'set -e' not in script.replace('deliberately no `set -e`',
                                                  '')


class TestInstallerCommand:

    def test_recovery_script_round_trips_through_base64(self):
        ray_head_cmd = instance_setup.ray_head_start_command(
            '{"A100": 4}', {'num-cpus': 8})
        script = instance_setup._runtime_recovery_head_script(ray_head_cmd)
        install_cmd = instance_setup._runtime_recovery_install_cmd(script)
        installer = _decode_installer(install_cmd)
        assert _decode_recovery_script(installer) == script

    def test_installer_has_systemd_unit_and_cron_fallback(self):
        install_cmd = instance_setup._runtime_recovery_install_cmd('true\n')
        installer = _decode_installer(install_cmd)
        # systemd path.
        assert ('/etc/systemd/system/skypilot-runtime-recovery.service'
                in installer)
        assert 'systemctl daemon-reload' in installer
        assert 'systemctl enable skypilot-runtime-recovery.service' in installer
        # Unit file fields: run as the ssh user, exec the recovery script
        # via an absolute ($HOME-expanded) path.
        assert 'User=$(whoami)' in installer
        assert ('ExecStart=/bin/bash $HOME/.sky/runtime-recovery.sh'
                in installer)
        assert 'WantedBy=multi-user.target' in installer
        # cron fallback.
        assert '@reboot bash $HOME/.sky/runtime-recovery.sh' in installer
        assert 'crontab -' in installer
        # Idempotency guard for the cron fallback.
        assert 'grep -F' in installer


class TestGating:

    @pytest.fixture(autouse=True)
    def _no_kill_switch(self, monkeypatch):
        monkeypatch.delenv('SKYPILOT_DISABLE_REBOOT_RECOVERY', raising=False)

    @pytest.mark.parametrize('provider', ['kubernetes', 'ssh', 'slurm'])
    def test_skips_container_and_shared_providers(self, provider):
        cluster_info = _make_cluster_info(provider)
        assert not instance_setup._should_install_reboot_recovery(cluster_info)

    def test_skips_docker_runtime(self):
        cluster_info = _make_cluster_info('aws', docker_user='docker-user')
        assert not instance_setup._should_install_reboot_recovery(cluster_info)

    def test_skips_when_kill_switch_set(self, monkeypatch):
        monkeypatch.setenv('SKYPILOT_DISABLE_REBOOT_RECOVERY', '1')
        cluster_info = _make_cluster_info('aws')
        assert not instance_setup._should_install_reboot_recovery(cluster_info)

    def test_installs_on_vm_providers_by_default(self):
        for provider in ('aws', 'gcp', 'azure'):
            cluster_info = _make_cluster_info(provider)
            assert instance_setup._should_install_reboot_recovery(cluster_info)

    def test_no_runner_call_when_gated(self):
        runner = mock.MagicMock()
        instance_setup._install_runtime_reboot_recovery(
            runner, 'true\n', _make_cluster_info('kubernetes'), os.devnull)
        runner.run.assert_not_called()

    def test_no_runner_call_when_kill_switch_set(self, monkeypatch):
        monkeypatch.setenv('SKYPILOT_DISABLE_REBOOT_RECOVERY', '1')
        runner = mock.MagicMock()
        instance_setup._install_runtime_reboot_recovery(
            runner, 'true\n', _make_cluster_info('aws'), os.devnull)
        runner.run.assert_not_called()

    def test_runner_called_for_vm_provider(self):
        runner = mock.MagicMock()
        runner.run.return_value = (0, '', '')
        instance_setup._install_runtime_reboot_recovery(
            runner, 'true\n', _make_cluster_info('aws'), os.devnull)
        assert runner.run.call_count == 1

    def test_install_never_raises(self):
        runner = mock.MagicMock()
        runner.run.side_effect = RuntimeError('ssh transport broke')
        # Must not propagate: a failed install cannot fail provisioning.
        instance_setup._install_runtime_reboot_recovery(
            runner, 'true\n', _make_cluster_info('aws'), os.devnull)

        runner_rc = mock.MagicMock()
        runner_rc.run.return_value = (1, '', '')
        instance_setup._install_runtime_reboot_recovery(
            runner_rc, 'true\n', _make_cluster_info('aws'), os.devnull)


class TestProvisionIntegration:

    @pytest.fixture(autouse=True)
    def _no_kill_switch(self, monkeypatch):
        monkeypatch.delenv('SKYPILOT_DISABLE_REBOOT_RECOVERY', raising=False)

    def test_head_start_installs_recovery_hook(self, monkeypatch):
        cluster_info = _make_cluster_info('aws')
        runner = mock.MagicMock()
        runner.run.return_value = (0, '', '')
        monkeypatch.setattr('sky.provision.get_command_runners',
                            lambda *args, **kwargs: [runner])

        instance_setup.start_ray_on_head_node('test-cluster',
                                              custom_resource=None,
                                              cluster_info=cluster_info,
                                              ssh_credentials={})

        assert runner.run.call_count == 2
        ray_cmd = runner.run.call_args_list[0].args[0]
        install_cmd = runner.run.call_args_list[1].args[0]
        installer = _decode_installer(install_cmd)
        script = _decode_recovery_script(installer)
        # The recovery script persists the exact ray start command that
        # was just executed, plus the skylet restart.
        assert ray_cmd in script
        assert instance_setup.MAYBE_SKYLET_RESTART_CMD in script

    def test_head_start_skips_recovery_on_kubernetes(self, monkeypatch):
        cluster_info = _make_cluster_info('kubernetes')
        runner = mock.MagicMock()
        runner.run.return_value = (0, '', '')
        monkeypatch.setattr('sky.provision.get_command_runners',
                            lambda *args, **kwargs: [runner])

        instance_setup.start_ray_on_head_node('test-cluster',
                                              custom_resource=None,
                                              cluster_info=cluster_info,
                                              ssh_credentials={})

        # Only the ray start command: no recovery installation on pods.
        assert runner.run.call_count == 1

    def test_worker_start_installs_recovery_hook(self, monkeypatch, tmp_path):
        cluster_info = _make_cluster_info('aws', num_instances=2)
        head_runner = mock.MagicMock()
        worker_runner = mock.MagicMock()
        worker_runner.run.return_value = (0, '', '')
        monkeypatch.setattr(
            'sky.provision.get_command_runners',
            lambda *args, **kwargs: [head_runner, worker_runner])
        monkeypatch.setattr('sky.provision.metadata_utils.get_instance_log_dir',
                            lambda *args, **kwargs: tmp_path)

        instance_setup.start_ray_on_worker_nodes('test-cluster',
                                                 no_restart=False,
                                                 custom_resource=None,
                                                 ray_port=6380,
                                                 cluster_info=cluster_info,
                                                 ssh_credentials={})

        head_runner.run.assert_not_called()
        assert worker_runner.run.call_count == 2
        install_cmd = worker_runner.run.call_args_list[1].args[0]
        installer = _decode_installer(install_cmd)
        script = _decode_recovery_script(installer)
        # The join command embeds the head internal IP (stable across
        # in-place reboots) and the ray port, and always carries the
        # "already joined" guard.
        assert 'export SKYPILOT_RAY_HEAD_IP="10.0.0.1"' in script
        assert 'export SKYPILOT_RAY_PORT=6380' in script
        assert 'gcs-address' in script

    def test_worker_start_skips_recovery_when_join_fails(
            self, monkeypatch, tmp_path):
        cluster_info = _make_cluster_info('aws', num_instances=2)
        head_runner = mock.MagicMock()
        worker_runner = mock.MagicMock()
        worker_runner.run.return_value = (1, '', 'boom')
        monkeypatch.setattr(
            'sky.provision.get_command_runners',
            lambda *args, **kwargs: [head_runner, worker_runner])
        monkeypatch.setattr('sky.provision.metadata_utils.get_instance_log_dir',
                            lambda *args, **kwargs: tmp_path)
        # Skip the _auto_retry backoff sleeps.
        monkeypatch.setattr('sky.provision.instance_setup.time.sleep',
                            lambda _: None)

        with pytest.raises(RuntimeError):
            instance_setup.start_ray_on_worker_nodes('test-cluster',
                                                     no_restart=False,
                                                     custom_resource=None,
                                                     ray_port=6380,
                                                     cluster_info=cluster_info,
                                                     ssh_credentials={})

        # The join failed on every retry: the recovery hook must never be
        # installed (only join attempts hit the runner).
        for call in worker_runner.run.call_args_list:
            assert 'base64 -d | bash' not in call.args[0]
