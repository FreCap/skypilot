"""Tests for passive LB-side replica eviction (W7).

During the controller-pause window the load balancer must shed a replica that
keeps failing with DEAD connections (refused/reset), but NOT one that is merely
saturated (connect/read timeout). Evictions are quarantined so the controller's
next sync doesn't immediately re-add a dead replica (oscillation).
"""
# pylint: disable=invalid-name,protected-access
import time

import httpx
import pytest

from sky.serve import constants
from sky.serve import load_balancer


def _make_lb():
    return load_balancer.SkyServeLoadBalancer(
        controller_url='http://controller:8001',
        load_balancer_port=30001,
        load_balancing_policy_name='least_load')


def _fail(lb, url, exc, times):
    for _ in range(times):
        lb._record_proxy_outcome(url, exc)


# --- classifier ---


@pytest.mark.parametrize('exc', [
    httpx.ConnectError('refused'),
    httpx.RemoteProtocolError('reset'),
    httpx.ReadError('reset'),
])
def test_dead_connection_errors_classified_dead(exc):
    assert load_balancer._is_dead_connection_error(exc) is True


@pytest.mark.parametrize('exc', [
    httpx.ConnectTimeout('slow'),
    httpx.ReadTimeout('slow'),
    RuntimeError('other'),
])
def test_saturated_or_other_errors_not_dead(exc):
    assert load_balancer._is_dead_connection_error(exc) is False


# --- eviction / quarantine ---


def test_evicts_after_threshold_dead_failures():
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['r1', 'r2'])
    # One under threshold: not yet evicted.
    _fail(lb, 'r1', httpx.ConnectError('x'),
          constants.LB_EVICTION_CONSECUTIVE_FAILURES - 1)
    assert 'r1' in lb._load_balancing_policy.ready_replicas
    # Crossing the threshold evicts and quarantines it.
    _fail(lb, 'r1', httpx.ConnectError('x'), 1)
    assert 'r1' not in lb._load_balancing_policy.ready_replicas
    assert 'r2' in lb._load_balancing_policy.ready_replicas
    assert 'r1' in lb._quarantined_replicas()


def test_saturation_never_evicts():
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['r1'])
    _fail(lb, 'r1', httpx.ConnectTimeout('slow'),
          constants.LB_EVICTION_CONSECUTIVE_FAILURES + 3)
    assert 'r1' in lb._load_balancing_policy.ready_replicas
    assert 'r1' not in lb._quarantined_replicas()


def test_success_resets_consecutive_failures():
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['r1'])
    _fail(lb, 'r1', httpx.ConnectError('x'),
          constants.LB_EVICTION_CONSECUTIVE_FAILURES - 1)
    # A success resets the streak, so the next failure doesn't tip eviction.
    lb._record_proxy_outcome('r1', httpx.Response(200))
    assert lb._replica_dead_failures.get('r1', 0) == 0
    _fail(lb, 'r1', httpx.ConnectError('x'), 1)
    assert 'r1' in lb._load_balancing_policy.ready_replicas


def test_quarantine_expires_after_ttl():
    lb = _make_lb()
    lb._load_balancing_policy.set_ready_replicas(['r1'])
    _fail(lb, 'r1', httpx.ConnectError('x'),
          constants.LB_EVICTION_CONSECUTIVE_FAILURES)
    assert 'r1' in lb._quarantined_replicas()
    # Backdate the quarantine: it must no longer be considered active, so the
    # sync loop can route to it again.
    lb._replica_quarantine_until['r1'] = time.time() - 1
    assert 'r1' not in lb._quarantined_replicas()
