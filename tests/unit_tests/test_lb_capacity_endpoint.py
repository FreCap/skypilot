"""/_lb/capacity: data-plane admission read.

External admission sizes against `sky serve status` (control plane) and
goes blind on every API-server restart/outage. The LB knows the ready
set and in-flight counts and — with an external LB — keeps serving
through control-plane restarts, so it exposes the volatile half of the
sizing read directly.
"""
# pylint: disable=protected-access
import asyncio
import threading
import time
import unittest
from unittest import mock

from sky.serve import load_balancer as lb_module
from sky.serve import load_balancing_policies as lb_policies


def _make_balancer(policy):
    balancer = object.__new__(lb_module.SkyServeLoadBalancer)
    balancer._load_balancing_policy = policy
    balancer._client_pool_lock = threading.Lock()
    balancer._ready = True
    balancer._draining = False
    balancer._last_sync_time = time.monotonic() - 4.0
    # Demand-feed state (normally set in __init__).
    balancer._queue_depth = 0
    balancer._reject_last_seen = {}
    balancer._reject_fallback_seq = 0
    balancer._capacity_hint = None
    balancer._replica_occupancy = {}
    balancer._replica_free_slots = {}
    balancer._last_occupancy_probe_time = None
    return balancer


class TestCapacityEndpoint(unittest.TestCase):

    def test_reports_ready_and_in_flight(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
        policy.load_map['http://a:8080'] = 2
        policy.load_map['http://b:8080'] = 1
        balancer = _make_balancer(policy)
        response = asyncio.run(balancer._capacity(mock.MagicMock()))
        import json
        body = json.loads(response.body)
        self.assertEqual(body['ready_replicas'], 2)
        self.assertEqual(body['in_flight'], 3)
        self.assertFalse(body['draining'])
        self.assertTrue(body['synced'])
        self.assertGreaterEqual(body['last_sync_age_seconds'], 3.0)

    def test_round_robin_reports_unknown_in_flight(self):
        policy = lb_policies.RoundRobinPolicy()
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        import json
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['ready_replicas'], 1)
        self.assertIsNone(body['in_flight'])

    def test_never_synced_reports_null_age(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([])
        balancer = _make_balancer(policy)
        balancer._ready = False
        balancer._last_sync_time = None
        import json
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['ready_replicas'], 0)
        self.assertFalse(body['synced'])
        self.assertIsNone(body['last_sync_age_seconds'])

    def test_demand_fields_default_and_hint_absent(self):
        # Before any sync carries a capacity_hint, the hint-derived fields
        # must read as unknown (null), not zero.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        import json
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['queue_depth'], 0)
        self.assertEqual(body['rejected_in_window'], 0)
        self.assertIsNone(body['provisioning_replicas'])
        self.assertIsNone(body['target_replicas'])

    def test_demand_fields_reflect_gauges_and_hint(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        balancer._queue_depth = 3
        balancer._reject_last_seen = {'job-1': time.monotonic()}
        balancer._capacity_hint = {
            'provisioning_replicas': 4,
            'target_num_replicas': 12,
        }
        import json
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['queue_depth'], 3)
        self.assertEqual(body['rejected_in_window'], 1)
        self.assertEqual(body['provisioning_replicas'], 4)
        self.assertEqual(body['target_replicas'], 12)

    def test_in_flight_ignores_pruned_replicas(self):
        # Load entries for replicas no longer ready must not inflate the
        # aggregate (prune keeps the map clean, but assert the contract).
        policy = lb_policies.InstanceAwareLeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
        policy.load_map['http://a:8080'] = 1
        policy.load_map['http://b:8080'] = 1
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        import json
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['in_flight'], 1)

    def test_never_probed_reports_zero_occupancy_and_null_age(self):
        # Without a completed probe round the endpoint must not invent
        # capacity: no probed replicas, no free slots, null age.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        import json
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['probed_replicas'], 0)
        self.assertEqual(body['busy_replicas'], 0)
        self.assertEqual(body['free_slots'], 0)
        self.assertIsNone(body['occupancy_probe_age_seconds'])

    def test_occupancy_aggregates(self):
        # a: busy (1 running / concurrency 1 -> 0 free), b: idle (1 free),
        # c: ready but unprobed (contributes nothing).
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(
            ['http://a:8080', 'http://b:8080', 'http://c:8080'])
        balancer = _make_balancer(policy)
        balancer._replica_occupancy = {'http://a:8080': 1, 'http://b:8080': 0}
        balancer._replica_free_slots = {'http://a:8080': 0, 'http://b:8080': 1}
        balancer._last_occupancy_probe_time = time.monotonic() - 2.0
        import json
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['probed_replicas'], 2)
        self.assertEqual(body['busy_replicas'], 1)
        self.assertEqual(body['free_slots'], 1)
        self.assertGreaterEqual(body['occupancy_probe_age_seconds'], 1.0)

    def test_occupancy_ignores_pruned_replicas(self):
        # A probe entry for a replica the controller since removed from the
        # ready set must not count toward the aggregates.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        balancer._replica_occupancy = {'http://a:8080': 0, 'http://gone': 0}
        balancer._replica_free_slots = {'http://a:8080': 1, 'http://gone': 1}
        balancer._last_occupancy_probe_time = time.monotonic()
        import json
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['probed_replicas'], 1)
        self.assertEqual(body['free_slots'], 1)
