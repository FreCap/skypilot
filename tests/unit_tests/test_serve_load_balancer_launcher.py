"""Tests for the standalone external load-balancer CLI contract."""
# pylint: disable=invalid-name,protected-access
import pytest

from sky.serve import load_balancer


def _resolve(argv):
    parser = load_balancer._build_argument_parser()
    return parser, load_balancer._resolve_launch_kwargs(parser.parse_args(argv))


_BASE = [
    '--controller-addr', 'http://ctrl:8001', '--load-balancer-port', '8890',
    '--service-hash', 'incarnation-a', '--service-name', 'svc-a'
]


def test_base_args_threaded():
    _, kwargs = _resolve(_BASE)
    assert kwargs['controller_addr'] == 'http://ctrl:8001'
    assert kwargs['load_balancer_port'] == 8890
    assert kwargs['service_hash'] == 'incarnation-a'
    assert kwargs['service_name'] == 'svc-a'
    assert set(kwargs) == {
        'controller_addr', 'load_balancer_port', 'service_hash', 'service_name'
    }


@pytest.mark.parametrize('missing', ['--service-hash', '--service-name'])
def test_service_identity_is_required(missing):
    with pytest.raises(SystemExit):
        index = _BASE.index(missing)
        _resolve(_BASE[:index] + _BASE[index + 2:])


def test_routing_and_tls_args_are_not_parsed():
    # Routing is sync-fetched and TLS terminates at platform ingress, so none
    # of the legacy in-pod launch flags remain accepted.
    for removed in ('--load-balancing-policy', '--target-qps-per-replica',
                    '--stream-timeout-seconds', '--retriable-status-codes',
                    '--max-retries', '--retry-initial-backoff-seconds',
                    '--tls-keyfile', '--tls-certfile'):
        with pytest.raises(SystemExit):
            _resolve(_BASE + [removed, 'anything'])
    _, kwargs = _resolve(_BASE)
    assert set(kwargs) == {
        'controller_addr', 'load_balancer_port', 'service_hash', 'service_name'
    }
