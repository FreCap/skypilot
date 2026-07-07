"""Tests for external LB rollout safety (W5): readiness + graceful drain.

The LB must not report ready until it has synced at least once (never route to
a cold LB), and on drain it must fail readiness so k8s pulls it from the
Service before in-flight requests finish.
"""
# pylint: disable=invalid-name,protected-access
import asyncio

from sky.serve import load_balancer


def _make_lb():
    return load_balancer.SkyServeLoadBalancer(
        controller_url='http://controller:8001',
        load_balancer_port=30001,
        load_balancing_policy_name='least_load')


def test_not_ready_before_first_sync():
    lb = _make_lb()
    assert lb._is_ready_to_serve() is False


def test_ready_after_first_sync():
    lb = _make_lb()
    lb._ready = True  # what a successful _sync_with_controller_once sets.
    assert lb._is_ready_to_serve() is True


def test_draining_fails_readiness_even_when_ready():
    lb = _make_lb()
    lb._ready = True
    lb._begin_draining()
    assert lb._draining is True
    assert lb._is_ready_to_serve() is False


def test_begin_draining_is_idempotent():
    lb = _make_lb()
    lb._begin_draining()
    lb._begin_draining()
    assert lb._draining is True


def test_health_endpoint_status_codes():
    lb = _make_lb()
    # Cold (not yet synced) -> 503 so k8s readiness holds traffic off.
    assert asyncio.run(lb._health(None)).status_code == 503
    lb._ready = True
    assert asyncio.run(lb._health(None)).status_code == 200
    # Draining -> 503 so k8s pulls it from the Service endpoints.
    lb._begin_draining()
    assert asyncio.run(lb._health(None)).status_code == 503
