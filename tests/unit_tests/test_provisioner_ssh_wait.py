"""Tests for wait_for_ssh probe pacing in sky.provision.provisioner.

SSM-proxied probes each open a session against the account-wide SSM
StartSession TPS quota, so their retry cadence must back off instead of
polling every second, and directly-reachable targets must be verified with
a proxy-less handshake (which authorizes the SSM bypass in ssm_direct).
Quota-free transports keep the tight 1s cadence.
"""
from unittest import mock

import pytest

from sky import exceptions
from sky.provision import common as provision_common
from sky.provision import provisioner
from sky.utils import resources_utils
from sky.utils import ssm_direct

# Matches ssm_direct.is_skypilot_ssm_proxy (bypass-eligible).
FULL_SSM_PROXY = (
    'env AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12 /bin/sh -c '
    'aws ssm start-session --target "$(aws ec2 describe-instances)" '
    '--region us-east-1 --document-name AWS-StartSSHSession '
    '--parameters portNumber=%p')


def test_post_provision_setup_reports_removed_cluster(monkeypatch):
    launched_resources = mock.Mock()
    launched_resources.cloud = mock.Mock()
    cluster_info = mock.Mock()
    provision_record = mock.Mock(region='us-east-1')
    monkeypatch.setattr(provisioner.global_user_state, 'get_cluster_yaml_dict',
                        lambda _: {})
    monkeypatch.setattr(provisioner.provision, 'get_cluster_info',
                        lambda *args, **kwargs: cluster_info)
    monkeypatch.setattr(provisioner.global_user_state,
                        'get_handle_from_cluster_name',
                        lambda *args, **kwargs: None)

    with pytest.raises(exceptions.ClusterDoesNotExist,
                       match='removed or replaced'):
        provisioner._post_provision_setup(  # pylint: disable=protected-access
            launched_resources,
            resources_utils.ClusterName('race', 'race-on-cloud'),
            '/tmp/cluster.yaml',
            provision_record,
            custom_resource=None,
            existing_cluster_hash='stale-hash',
            provider_effect_guard_factory=None)


def test_provisioner_facade_owns_ssh_wait_callables():
    # pylint: disable=protected-access
    assert provisioner.wait_for_ssh.__module__ == 'sky.provision.provisioner'
    assert (provisioner._wait_ssh_connection_direct.__module__ ==
            'sky.provision.provisioner')
    assert (provisioner._wait_ssh_connection_indirect.__module__ ==
            'sky.provision.provisioner')


def test_ssh_probe_command_projection():
    # pylint: disable=protected-access
    command = provisioner._ssh_probe_command(ip='10.0.0.1',
                                             ssh_port=2222,
                                             ssh_user='ubuntu',
                                             ssh_private_key='/tmp/test-key',
                                             ssh_probe_timeout=17,
                                             ssh_proxy_command='proxy %h %p')

    assert command == [
        'ssh', '-T', '-i', '/tmp/test-key', 'ubuntu@10.0.0.1', '-p', '2222',
        '-o', 'StrictHostKeyChecking=no', '-o', 'PasswordAuthentication=no',
        '-o', 'ConnectTimeout=17s', '-o', 'UserKnownHostsFile=/dev/null', '-o',
        'IdentitiesOnly=yes', '-o', 'AddKeysToAgent=yes', '-o',
        'ExitOnForwardFailure=yes', '-o', 'ServerAliveInterval=5', '-o',
        'ServerAliveCountMax=3', '-o', 'ProxyCommand=proxy %h %p', 'uptime'
    ]


@pytest.fixture(autouse=True)
def _clear_cache():
    with ssm_direct._cache_lock:  # pylint: disable=protected-access
        ssm_direct._cache.clear()  # pylint: disable=protected-access
    yield
    with ssm_direct._cache_lock:  # pylint: disable=protected-access
        ssm_direct._cache.clear()  # pylint: disable=protected-access


def _fake_cluster_info(monkeypatch) -> provision_common.ClusterInfo:
    cluster_info = provision_common.ClusterInfo(instances={},
                                                head_instance_id=None,
                                                provider_name='aws')
    monkeypatch.setattr(cluster_info, 'has_external_ips', lambda: False)
    monkeypatch.setattr(cluster_info, 'get_feasible_ips',
                        lambda *a, **kw: ['10.0.0.1'])
    monkeypatch.setattr(cluster_info, 'get_ssh_ports', lambda: [22])
    return cluster_info


def _run_wait_for_ssh(monkeypatch,
                      ssh_proxy_command: str | None,
                      failures_before_success: int,
                      tcp_reachable: bool = False,
                      waiter=None):
    """Run wait_for_ssh with a stubbed prober; return (sleeps, marks)."""
    attempts = {'n': 0}

    def default_waiter(ip, ssh_port, **kwargs):
        del ip, ssh_port, kwargs
        attempts['n'] += 1
        if attempts['n'] <= failures_before_success:
            return False, 'ssh: not ready'
        return True, ''

    sleeps = []
    marks = []
    monkeypatch.setattr(provisioner, '_wait_ssh_connection_indirect', waiter or
                        default_waiter)
    monkeypatch.setattr(provisioner.time, 'sleep', sleeps.append)
    monkeypatch.setattr(ssm_direct, 'is_enabled', lambda: True)
    monkeypatch.setattr(ssm_direct, 'tcp_reachable',
                        lambda *a, **kw: tcp_reachable)
    monkeypatch.setattr(ssm_direct, 'mark_direct_ok',
                        lambda ip, port: marks.append(('ok', ip, port)))
    monkeypatch.setattr(ssm_direct, 'mark_direct_failed',
                        lambda ip, port: marks.append(('failed', ip, port)))
    provisioner.wait_for_ssh(
        _fake_cluster_info(monkeypatch),
        {'ssh_proxy_command': ssh_proxy_command} if ssh_proxy_command else {})
    return sleeps, marks


def test_ssm_proxy_probes_back_off(monkeypatch):
    # Not bypass-eligible (custom-ish shape) and TCP-unreachable: the pure
    # SSM probe path with pacing.
    proxy = 'aws ssm start-session --target "i-123"'
    sleeps, marks = _run_wait_for_ssh(monkeypatch,
                                      ssh_proxy_command=proxy,
                                      failures_before_success=4)
    # A bounded initial jitter staggers the first probe wave, then jittered
    # exponential backoff paces the retries: intervals trend upward and are
    # not the fixed 1s cadence.
    assert len(sleeps) == 5
    first_wave_jitter, *retry_backoffs = sleeps
    assert 0 <= first_wave_jitter <= 5
    assert all(s > 1 for s in retry_backoffs)
    assert retry_backoffs[-1] > retry_backoffs[0]
    assert not marks


def test_non_ssm_probes_keep_fixed_cadence(monkeypatch):
    sleeps, marks = _run_wait_for_ssh(monkeypatch,
                                      ssh_proxy_command=None,
                                      failures_before_success=3)
    assert sleeps == [1, 1, 1]
    assert not marks


def test_no_sleep_after_success(monkeypatch):
    sleeps, _ = _run_wait_for_ssh(monkeypatch,
                                  ssh_proxy_command=None,
                                  failures_before_success=0)
    assert not sleeps


def test_direct_probe_verifies_and_marks_ok(monkeypatch):
    """TCP-reachable target: probed without the proxy; a successful
    proxy-less handshake authorizes the runner-level SSM bypass."""
    seen_proxies = []

    def waiter(ip, ssh_port, **kwargs):
        del ip, ssh_port
        seen_proxies.append(kwargs.get('ssh_proxy_command'))
        return True, ''

    _, marks = _run_wait_for_ssh(monkeypatch,
                                 ssh_proxy_command=FULL_SSM_PROXY,
                                 failures_before_success=0,
                                 tcp_reachable=True,
                                 waiter=waiter)
    assert seen_proxies == [None]
    assert marks == [('ok', '10.0.0.1', 22)]


def test_direct_failures_fall_back_to_ssm_and_mark_failed(monkeypatch):
    """TCP opens but proxy-less SSH keeps failing: after the exclusive
    direct window, alternate to the SSM probe; when only SSM succeeds,
    poison the bypass cache."""

    def waiter(ip, ssh_port, **kwargs):
        del ip, ssh_port
        return kwargs.get('ssh_proxy_command') is not None, 'direct broken'

    _, marks = _run_wait_for_ssh(monkeypatch,
                                 ssh_proxy_command=FULL_SSM_PROXY,
                                 failures_before_success=0,
                                 tcp_reachable=True,
                                 waiter=waiter)
    assert marks == [('failed', '10.0.0.1', 22)]


def test_unreachable_target_stays_on_ssm(monkeypatch):
    """No TCP path (private cluster): pure SSM probing, no cache marks."""
    seen_proxies = []

    def waiter(ip, ssh_port, **kwargs):
        del ip, ssh_port
        seen_proxies.append(kwargs.get('ssh_proxy_command'))
        return True, ''

    _, marks = _run_wait_for_ssh(monkeypatch,
                                 ssh_proxy_command=FULL_SSM_PROXY,
                                 failures_before_success=0,
                                 tcp_reachable=False,
                                 waiter=waiter)
    assert seen_proxies == [FULL_SSM_PROXY]
    assert not marks
