"""Tests for _TRANSIENT_SSH_FAILURE_PATTERN in backend_utils.

The ray health probe retries transport-level SSH failures matching this
pattern instead of immediately flagging the cluster abnormal. The pattern
must catch momentary proxy drops (e.g. SSH-over-SSM) while NOT catching
"timed out", which _SSH_CONNECTION_TIMED_OUT_PATTERN maps to the
changed-IP recovery hint on manually restarted clusters.
"""
from sky.backends.backend_utils import _SSH_CONNECTION_TIMED_OUT_PATTERN
from sky.backends.backend_utils import _TRANSIENT_SSH_FAILURE_PATTERN


class TestTransientSshFailurePattern:
    """Retryable transport failures vs. genuine failures."""

    def test_ssm_target_not_connected_is_transient(self):
        # Observed via an SSM ProxyCommand: the agent dropped for a moment
        # while the instance and ray stayed healthy.
        stderr = ('An error occurred (TargetNotConnected) when calling the '
                  'StartSession operation: i-03ec6e6553dd78951 is not '
                  'connected.')
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is not None

    def test_ssm_target_instance_not_found_is_not_transient(self):
        # A successful EC2 lookup with no instance is stale cluster evidence,
        # not a momentary SSM transport drop. Let the caller refresh provider
        # state immediately instead of retrying the same empty lookup.
        stderr = (
            'SkyPilot SSM target instance not found for SSH host 10.0.0.1')
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is None

    def test_kex_exchange_is_transient(self):
        stderr = ('kex_exchange_identification: Connection closed by remote '
                  'host')
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is not None

    def test_connection_reset_is_transient(self):
        stderr = 'client_loop: send disconnect: Connection reset by peer'
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is not None

    def test_broken_pipe_is_transient(self):
        stderr = 'client_loop: send disconnect: Broken pipe'
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is not None

    def test_timed_out_is_not_transient(self):
        # "timed out" must keep flowing to the changed-IP recovery hint,
        # not the retry loop.
        stderr = 'ssh: connect to host 1.2.3.4 port 22: Connection timed out'
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is None
        assert _SSH_CONNECTION_TIMED_OUT_PATTERN.search(stderr) is not None

    def test_ray_failure_is_not_transient(self):
        # A real ray-side failure must not be swallowed by the retry loop.
        output = 'Failed to check ray cluster\'s healthiness.'
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(output) is None

    def test_permission_denied_is_not_transient(self):
        stderr = 'user@1.2.3.4: Permission denied (publickey).'
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is None

    def test_ssm_throttling_is_transient(self):
        # StartSession has a low account-wide TPS quota; a throttled proxy
        # command on a healthy running cluster must be retried, not treated
        # as an abnormal cluster.
        stderr = ('An error occurred (ThrottlingException) when calling the '
                  'StartSession operation (reached max retries: 4): '
                  'Rate exceeded')
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is not None

    def test_ec2_request_limit_is_transient(self):
        # The describe-instances embedded in the SSM proxy command shares
        # the EC2 Describe* throttle bucket.
        stderr = ('An error occurred (RequestLimitExceeded) when calling the '
                  'DescribeInstances operation: Request limit exceeded.')
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is not None

    def test_bare_rate_exceeded_is_not_transient(self):
        # Without an AWS error code, "Rate exceeded" is too generic to
        # attribute to cloud API throttling (e.g. an sshd/bastion rate
        # limiter) and must not be swallowed by the retry loop.
        stderr = 'ssh_exchange_identification: Rate exceeded, try later'
        assert _TRANSIENT_SSH_FAILURE_PATTERN.search(stderr) is None
