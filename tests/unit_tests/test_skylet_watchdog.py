"""Tests for the skylet watchdog installed at provision time.

Skylet enforces autostop/autodown ON the VM; if it dies mid-life nothing
restarted it until the next launch/start/reboot, so the cluster silently
stops self-terminating, and a SIGKILL death leaves no trace. The watchdog
(a minutely systemd timer, cron fallback) restarts skylet with the exact
provision-time start command and appends a timestamped post-mortem to
~/.sky/skylet-watchdog.log. These tests cover script generation, base64
shipping, and gating; the script's runtime behavior (healthy no-op,
dead-path restart + logging, log truncation) is /proc-based and validated
in a Linux container (see the PR test plan).
"""
# pylint: disable=protected-access,missing-class-docstring
import base64
import hashlib
import re
from unittest import mock

import pytest

from sky.provision import common
from sky.provision import instance_setup

_INSTALL_CMD_RE = re.compile(r'^echo ([A-Za-z0-9+/=]+) \| base64 -d \| bash$')
_SCRIPT_SHIP_RE = re.compile(r'echo ([A-Za-z0-9+/=]+) \| base64 -d > ')


def _make_cluster_info(provider_name: str,
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
    return common.ClusterInfo(instances=instances,
                              head_instance_id=head_id,
                              provider_name=provider_name,
                              docker_user=docker_user)


class TestWatchdogScriptGeneration:

    def test_generated_commands_are_byte_for_byte_stable(self):
        script = instance_setup._skylet_watchdog_script(
            "export CLUSTER='demo'; python -m sky.skylet.attempt_skylet;")
        installer = instance_setup._skylet_watchdog_install_cmd(script)
        assert hashlib.sha256(script.encode()).hexdigest() == (
            '1a7e01efc993313a84f0d933de61b25438e9e28c8d66197a1581035e4e475391')
        assert hashlib.sha256(installer.encode()).hexdigest() == (
            'cea81ae762ddf5d968cd14003dde956a26c6050b3a9fedd9c6fdf792a73ffbe4')

    def test_start_command_is_substituted_verbatim(self):
        cmd = ('export SKYPILOT_HEARTBEAT_GPU_TYPE=\'H100\'; '
               'python -m sky.skylet.attempt_skylet;')
        script = instance_setup._skylet_watchdog_script(cmd)
        assert cmd in script
        assert '__SKYLET_START_CMD__' not in script

    def test_healthy_check_precedes_login_reexec(self):
        # The aliveness check must run before the (expensive) login-shell
        # re-exec: the healthy path runs every minute.
        script = instance_setup._skylet_watchdog_script('true')
        check_pos = script.index('/proc/$pid/cmdline')
        reexec_pos = script.index('SKYPILOT_SKYLET_WATCHDOG_REEXEC')
        assert check_pos < reexec_pos

    def test_script_guards(self):
        script = instance_setup._skylet_watchdog_script('true')
        # Overlap guard, pid-file check, bounded log, post-mortem tail.
        assert 'flock -n 9' in script
        assert 'skylet_pid' in script
        assert 'skylet-watchdog.log' in script
        assert 'tail -n 20 "$HOME/.sky/skylet.log"' in script


class TestWatchdogInstaller:

    def _decode(self):
        script = instance_setup._skylet_watchdog_script('true')
        install_cmd = instance_setup._skylet_watchdog_install_cmd(script)
        match = _INSTALL_CMD_RE.match(install_cmd)
        assert match is not None, install_cmd
        installer = base64.b64decode(match.group(1)).decode('utf-8')
        return script, installer

    def test_installer_ships_script_intact(self):
        script, installer = self._decode()
        ship = _SCRIPT_SHIP_RE.search(installer)
        assert ship is not None, installer
        shipped = base64.b64decode(ship.group(1)).decode('utf-8')
        assert shipped == script

    def test_installer_prefers_timer_with_cron_fallback(self):
        _, installer = self._decode()
        assert instance_setup._SKYLET_WATCHDOG_TIMER_NAME in installer
        assert 'OnUnitActiveSec=60' in installer
        assert '* * * * *' in installer  # cron fallback
        # Idempotent cron append: re-running the installer must not stack
        # duplicate entries.
        assert 'grep -F "skylet-watchdog.sh"' in installer


class TestWatchdogGating:

    @pytest.mark.parametrize('provider', ['kubernetes', 'ssh', 'slurm'])
    def test_skipped_on_unsupported_providers(self, provider):
        info = _make_cluster_info(provider)
        assert instance_setup._should_install_skylet_watchdog(info) is False

    def test_skipped_in_docker(self):
        info = _make_cluster_info('aws', docker_user='root')
        assert instance_setup._should_install_skylet_watchdog(info) is False

    def test_kill_switch_env_var(self):
        info = _make_cluster_info('aws')
        with mock.patch.dict(
                'os.environ',
            {instance_setup._DISABLE_SKYLET_WATCHDOG_ENV_VAR: '1'}):
            assert (instance_setup._should_install_skylet_watchdog(info)
                    is False)

    def test_installed_on_vm_clouds(self):
        info = _make_cluster_info('aws')
        assert instance_setup._should_install_skylet_watchdog(info) is True


class TestAttemptSkyletRestartLock:
    """All attempt_skylet invokers (provisioning, reboot recovery, the
    watchdog) must serialize on one inter-process lock: two concurrent runs
    that both observe a dead/stale skylet would otherwise both kill and
    start one, racing on the pid/port files (duplicate skylets, or a pid
    file pointing at the losing child)."""

    def _patched(self, tmp_path, monkeypatch):
        from sky.skylet import attempt_skylet
        lock_file = str(tmp_path / 'restart.lock')
        monkeypatch.setattr(attempt_skylet, 'LOCK_FILE', lock_file)
        return attempt_skylet, lock_file

    def test_check_and_restart_runs_under_the_lock(self, tmp_path, monkeypatch):
        import filelock
        attempt_skylet, lock_file = self._patched(tmp_path, monkeypatch)
        observed = {}

        def _probe():
            # While inside, the lock must be held: an independent
            # non-blocking acquire has to fail.
            probe = filelock.FileLock(lock_file)
            try:
                probe.acquire(timeout=0)
                probe.release()
                observed['locked'] = False
            except filelock.Timeout:
                observed['locked'] = True

        monkeypatch.setattr(attempt_skylet, '_check_and_maybe_restart', _probe)
        attempt_skylet.main()
        assert observed['locked'] is True

    def test_concurrent_mains_serialize(self, tmp_path, monkeypatch):
        import threading
        import time as time_mod
        attempt_skylet, _ = self._patched(tmp_path, monkeypatch)
        intervals = []

        def _slow():
            start = time_mod.monotonic()
            time_mod.sleep(0.3)
            intervals.append((start, time_mod.monotonic()))

        monkeypatch.setattr(attempt_skylet, '_check_and_maybe_restart', _slow)
        threads = [
            threading.Thread(target=attempt_skylet.main) for _ in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert len(intervals) == 2
        (a_start, a_end), (b_start, b_end) = sorted(intervals)
        # The critical sections must not overlap.
        assert a_end <= b_start or b_end <= a_start

    def test_lock_timeout_exits_nonzero_without_touching_state(
            self, tmp_path, monkeypatch):
        import filelock
        attempt_skylet, lock_file = self._patched(tmp_path, monkeypatch)
        monkeypatch.setattr(attempt_skylet, 'LOCK_TIMEOUT_SECONDS', 0.2)
        called = []
        monkeypatch.setattr(attempt_skylet, '_check_and_maybe_restart',
                            lambda: called.append(True))
        holder = filelock.FileLock(lock_file)
        with holder.acquire(timeout=1):
            # Must give up without touching state, and exit NON-zero: the
            # run did not verify/restart skylet, and provisioning (which
            # treats attempt_skylet failure as fatal-after-retries) must
            # not believe it did.
            with pytest.raises(SystemExit) as exc_info:
                attempt_skylet.main()
        assert exc_info.value.code != 0
        assert not called
