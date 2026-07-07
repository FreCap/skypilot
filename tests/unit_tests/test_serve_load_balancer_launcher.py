"""Tests for the standalone (external) load balancer CLI launcher.

Covers the argument coercion in `sky.serve.load_balancer` that threads
`target_qps_per_replica` and the TLS credential from CLI args into
`run_load_balancer`. The in-pod load balancer gets these from the service
spec; a standalone load balancer must receive them explicitly, otherwise
`InstanceAwareLeastLoadPolicy` silently falls back to a uniform QPS of 1.0.
"""
# pylint: disable=invalid-name,protected-access
import pytest

from sky.serve import constants
from sky.serve import load_balancer
from sky.serve import serve_utils


def _resolve(argv):
    parser = load_balancer._build_argument_parser()
    return parser, load_balancer._resolve_launch_kwargs(parser,
                                                        parser.parse_args(argv))


_BASE = [
    '--controller-addr', 'http://ctrl:8001', '--load-balancer-port', '8890'
]


def test_base_args_threaded():
    _, kwargs = _resolve(_BASE)
    assert kwargs['controller_addr'] == 'http://ctrl:8001'
    assert kwargs['load_balancer_port'] == 8890
    # The regression guard: an unspecified target QPS / TLS must be threaded
    # through as None (not silently dropped), and the stream timeout defaults.
    assert kwargs['target_qps_per_replica'] is None
    assert kwargs['tls_credential'] is None
    assert kwargs[
        'stream_timeout_seconds'] == constants.DEFAULT_LB_STREAM_TIMEOUT


def test_target_qps_scalar_parsed_as_number():
    _, kwargs = _resolve(_BASE + ['--target-qps-per-replica', '2.5'])
    assert kwargs['target_qps_per_replica'] == 2.5


def test_target_qps_dict_parsed_as_mapping():
    _, kwargs = _resolve(
        _BASE + ['--target-qps-per-replica', '{"H100": 2.5, "A100": 1}'])
    assert kwargs['target_qps_per_replica'] == {'H100': 2.5, 'A100': 1}


def test_target_qps_invalid_json_exits():
    with pytest.raises(SystemExit):
        _resolve(_BASE + ['--target-qps-per-replica', 'not-json'])


def test_target_qps_wrong_type_exits():
    # Valid JSON but neither a number nor an object (a list) is rejected.
    with pytest.raises(SystemExit):
        _resolve(_BASE + ['--target-qps-per-replica', '[1, 2]'])


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


def test_stream_timeout_threaded():
    _, kwargs = _resolve(_BASE + ['--stream-timeout-seconds', '45'])
    assert kwargs['stream_timeout_seconds'] == 45
