"""Tests for the standalone (external) load balancer CLI launcher.

Covers the argument coercion in `sky.serve.load_balancer` that threads the
controller address, listen port, and TLS credential from CLI args into
`run_load_balancer`. The routing spec (load-balancing policy, target QPS,
stream timeout) is NOT a launch arg -- it is fetched from the controller over
the sync channel -- so those args are intentionally absent here.
"""
# pylint: disable=invalid-name,protected-access
import pytest

from sky.serve import load_balancer
from sky.serve import serve_utils


def _resolve(argv):
    parser = load_balancer._build_argument_parser()
    return parser, load_balancer._resolve_launch_kwargs(parser,
                                                        parser.parse_args(argv))


_BASE = [
    '--controller-addr', 'http://ctrl:8001', '--load-balancer-port', '8890',
    '--service-hash', 'incarnation-a'
]


def test_base_args_threaded():
    _, kwargs = _resolve(_BASE)
    assert kwargs['controller_addr'] == 'http://ctrl:8001'
    assert kwargs['load_balancer_port'] == 8890
    assert kwargs['service_hash'] == 'incarnation-a'
    # An unspecified TLS credential must still be threaded through as None.
    assert kwargs['tls_credential'] is None


def test_service_hash_is_required():
    with pytest.raises(SystemExit):
        _resolve([
            '--controller-addr', 'http://ctrl:8001', '--load-balancer-port',
            '8890'
        ])


def test_routing_spec_args_are_not_parsed():
    # The routing spec is sync-fetched, so its flags no longer exist and the
    # standalone kwargs never carry policy / target-qps / stream-timeout.
    for removed in ('--load-balancing-policy', '--target-qps-per-replica',
                    '--stream-timeout-seconds'):
        with pytest.raises(SystemExit):
            _resolve(_BASE + [removed, 'anything'])
    _, kwargs = _resolve(_BASE)
    assert 'load_balancing_policy_name' not in kwargs
    assert 'target_qps_per_replica' not in kwargs
    assert 'stream_timeout_seconds' not in kwargs


def test_tls_both_files_builds_credential():
    _, kwargs = _resolve(
        _BASE + ['--tls-keyfile', '/k.pem', '--tls-certfile', '/c.pem'])
    cred = kwargs['tls_credential']
    assert isinstance(cred, serve_utils.TLSCredential)
    assert cred.keyfile == '/k.pem'
    assert cred.certfile == '/c.pem'


@pytest.mark.parametrize(
    'partial', [['--tls-keyfile', '/k.pem'], ['--tls-certfile', '/c.pem']])
def test_tls_one_file_exits(partial):
    with pytest.raises(SystemExit):
        _resolve(_BASE + partial)
