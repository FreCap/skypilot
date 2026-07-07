"""Tests for the opportunistic direct-SSH bypass of SSM proxy commands."""
import pytest

from sky import exceptions
from sky.utils import command_runner
from sky.utils import ssm_direct

CURRENT_SSM_CMD = (
    'env AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12 /bin/sh -c '
    '\'aws ssm start-session --target "$(aws ec2 describe-instances)" --region us-east-1 --document-name AWS-StartSSHSession --parameters portNumber=%p\''
)
LEGACY_SSM_CMD = (
    'aws ssm start-session --target "$(aws ec2 describe-instances)" '
    '--region us-east-1 --document-name AWS-StartSSHSession '
    '--parameters portNumber=%p')
CUSTOM_PROXY_CMD = 'ssh -W %h:%p bastion.example.com'


@pytest.fixture(autouse=True)
def _clear_cache():
    with ssm_direct._cache_lock:  # pylint: disable=protected-access
        ssm_direct._cache.clear()  # pylint: disable=protected-access
    yield
    with ssm_direct._cache_lock:  # pylint: disable=protected-access
        ssm_direct._cache.clear()  # pylint: disable=protected-access


class TestShapeMatcher:

    def test_current_command_matches(self):
        assert ssm_direct.is_skypilot_ssm_proxy(CURRENT_SSM_CMD)

    def test_legacy_command_matches(self):
        assert ssm_direct.is_skypilot_ssm_proxy(LEGACY_SSM_CMD)

    def test_custom_proxy_does_not_match(self):
        assert not ssm_direct.is_skypilot_ssm_proxy(CUSTOM_PROXY_CMD)

    def test_none_does_not_match(self):
        assert not ssm_direct.is_skypilot_ssm_proxy(None)

    def test_foreign_ssm_document_does_not_match(self):
        # A user-supplied SSM proxy with a different session document may
        # depend on auth paths we know nothing about: never bypass it.
        cmd = ('aws ssm start-session --target i-123 '
               '--document-name My-CustomDocument')
        assert not ssm_direct.is_skypilot_ssm_proxy(cmd)


class TestBypassCache:

    def test_unverified_target_keeps_proxy(self):
        assert ssm_direct.maybe_bypass_proxy('1.2.3.4', 22,
                                             CURRENT_SSM_CMD) is not None

    def test_verified_target_bypasses(self):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        assert ssm_direct.maybe_bypass_proxy('1.2.3.4', 22,
                                             CURRENT_SSM_CMD) is None

    def test_failed_target_keeps_proxy(self):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        ssm_direct.mark_direct_failed('1.2.3.4', 22)
        assert ssm_direct.maybe_bypass_proxy('1.2.3.4', 22,
                                             CURRENT_SSM_CMD) is not None

    def test_verification_is_per_target(self):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        assert ssm_direct.maybe_bypass_proxy('5.6.7.8', 22,
                                             CURRENT_SSM_CMD) is not None

    def test_custom_proxy_never_bypassed(self):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        assert ssm_direct.maybe_bypass_proxy(
            '1.2.3.4', 22, CUSTOM_PROXY_CMD) == CUSTOM_PROXY_CMD

    def test_positive_entry_expires(self, monkeypatch):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        real_time = ssm_direct.time.time()
        monkeypatch.setattr(
            ssm_direct.time, 'time',
            lambda: real_time + ssm_direct._DIRECT_OK_TTL_SECONDS + 1)  # pylint: disable=protected-access
        assert ssm_direct.maybe_bypass_proxy('1.2.3.4', 22,
                                             CURRENT_SSM_CMD) is not None

    def test_kill_switch_disables_bypass(self, monkeypatch):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        monkeypatch.setattr(ssm_direct, 'is_enabled', lambda: False)
        assert ssm_direct.maybe_bypass_proxy('1.2.3.4', 22,
                                             CURRENT_SSM_CMD) is not None


class TestRunnerBypass:

    def _runner(self, proxy_command):
        return command_runner.SSHCommandRunner(('1.2.3.4', 22),
                                               'ubuntu',
                                               None,
                                               ssh_proxy_command=proxy_command)

    def test_unverified_runner_keeps_proxy(self):
        runner = self._runner(CURRENT_SSM_CMD)
        assert runner._ssh_proxy_command == CURRENT_SSM_CMD  # pylint: disable=protected-access
        assert runner._ssm_bypassed_proxy_command is None  # pylint: disable=protected-access

    def test_verified_runner_drops_proxy(self):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        runner = self._runner(CURRENT_SSM_CMD)
        assert runner._ssh_proxy_command is None  # pylint: disable=protected-access
        assert runner._ssm_bypassed_proxy_command == CURRENT_SSM_CMD  # pylint: disable=protected-access

    def test_transport_failure_reverts_and_poisons(self):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        runner = self._runner(CURRENT_SSM_CMD)
        assert runner.note_transport_failure(255) is True
        assert runner._ssh_proxy_command == CURRENT_SSM_CMD  # pylint: disable=protected-access
        assert runner._ssm_bypassed_proxy_command is None  # pylint: disable=protected-access
        # Cache poisoned: new runners keep the proxy.
        second = self._runner(CURRENT_SSM_CMD)
        assert second._ssh_proxy_command == CURRENT_SSM_CMD  # pylint: disable=protected-access

    def test_success_does_not_revert(self):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        runner = self._runner(CURRENT_SSM_CMD)
        assert runner.note_transport_failure(0) is False
        assert runner._ssh_proxy_command is None  # pylint: disable=protected-access

    def test_custom_proxy_runner_untouched(self):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        runner = self._runner(CUSTOM_PROXY_CMD)
        assert runner._ssh_proxy_command == CUSTOM_PROXY_CMD  # pylint: disable=protected-access

    def test_docker_runner_never_bypasses(self):
        # The docker branch bakes the proxy into the inner-hop command at
        # construction, which would make a bypass irreversible: excluded.
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        runner = command_runner.SSHCommandRunner(
            ('1.2.3.4', 22),
            'ubuntu',
            None,
            ssh_proxy_command=CURRENT_SSM_CMD,
            docker_user='docker-user')
        assert runner._ssm_bypassed_proxy_command is None  # pylint: disable=protected-access

    def test_rsync_salvages_over_ssm_after_direct_failure(self, monkeypatch):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        runner = self._runner(CURRENT_SSM_CMD)
        rsh_options = []

        def fake_rsync(source, target, **kwargs):
            del source, target
            rsh_options.append(kwargs['rsh_option'])
            if len(rsh_options) == 1:
                raise exceptions.CommandError(returncode=255,
                                              command='rsync',
                                              error_msg='transport failed',
                                              detailed_reason=None)

        monkeypatch.setattr(runner, '_rsync', fake_rsync)
        runner.rsync('/src', '/dst', up=True)
        assert len(rsh_options) == 2
        # First attempt ran direct (bypassed proxy), the salvage attempt
        # rebuilt options with the restored SSM proxy.
        assert 'ssm start-session' not in rsh_options[0]
        assert 'ssm start-session' in rsh_options[1]

    def test_rsync_non_transport_failure_raises(self, monkeypatch):
        ssm_direct.mark_direct_ok('1.2.3.4', 22)
        runner = self._runner(CURRENT_SSM_CMD)

        def fake_rsync(source, target, **kwargs):
            del source, target, kwargs
            raise exceptions.CommandError(returncode=23,
                                          command='rsync',
                                          error_msg='partial transfer',
                                          detailed_reason=None)

        monkeypatch.setattr(runner, '_rsync', fake_rsync)
        with pytest.raises(exceptions.CommandError):
            runner.rsync('/src', '/dst', up=True)
        # Not a transport failure: the bypass stays in place.
        assert runner._ssh_proxy_command is None  # pylint: disable=protected-access
