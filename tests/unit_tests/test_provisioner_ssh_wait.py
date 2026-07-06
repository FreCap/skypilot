"""Tests for wait_for_ssh probe pacing in sky.provision.provisioner.

SSM-proxied probes each open a session against the account-wide SSM
StartSession TPS quota, so their retry cadence must back off instead of
polling every second; quota-free transports keep the tight 1s cadence.
"""
from typing import Optional

from sky.provision import common as provision_common
from sky.provision import provisioner


def _fake_cluster_info(monkeypatch) -> provision_common.ClusterInfo:
    cluster_info = provision_common.ClusterInfo(instances={},
                                                head_instance_id=None,
                                                provider_name='aws')
    monkeypatch.setattr(cluster_info, 'has_external_ips', lambda: False)
    monkeypatch.setattr(cluster_info, 'get_feasible_ips',
                        lambda *a, **kw: ['10.0.0.1'])
    monkeypatch.setattr(cluster_info, 'get_ssh_ports', lambda: [22])
    return cluster_info


def _run_wait_for_ssh(monkeypatch, ssh_proxy_command: Optional[str],
                      failures_before_success: int):
    """Run wait_for_ssh with a stubbed prober; return the recorded sleeps."""
    attempts = {'n': 0}

    def fake_waiter(ip, ssh_port, **kwargs):
        attempts['n'] += 1
        if attempts['n'] <= failures_before_success:
            return False, 'ssh: not ready'
        return True, ''

    sleeps = []
    monkeypatch.setattr(provisioner, '_wait_ssh_connection_indirect',
                        fake_waiter)
    monkeypatch.setattr(provisioner.time, 'sleep', sleeps.append)
    provisioner.wait_for_ssh(
        _fake_cluster_info(monkeypatch),
        {'ssh_proxy_command': ssh_proxy_command} if ssh_proxy_command else {})
    return sleeps


def test_ssm_proxy_probes_back_off(monkeypatch):
    proxy = ('export AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12; '
             'aws ssm start-session --target "i-123"')
    sleeps = _run_wait_for_ssh(monkeypatch,
                               ssh_proxy_command=proxy,
                               failures_before_success=4)
    assert len(sleeps) == 4
    # Jittered exponential backoff: intervals trend upward and are not the
    # fixed 1s cadence.
    assert all(s > 1 for s in sleeps)
    assert sleeps[-1] > sleeps[0]


def test_non_ssm_probes_keep_fixed_cadence(monkeypatch):
    sleeps = _run_wait_for_ssh(monkeypatch,
                               ssh_proxy_command=None,
                               failures_before_success=3)
    assert sleeps == [1, 1, 1]


def test_no_sleep_after_success(monkeypatch):
    sleeps = _run_wait_for_ssh(monkeypatch,
                               ssh_proxy_command=None,
                               failures_before_success=0)
    assert sleeps == []
