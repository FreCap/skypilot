"""/_lb/capacity: data-plane admission read.

External admission sizes against `sky serve status` (control plane) and
goes blind on every API-server restart/outage. The LB knows the ready
set and in-flight counts and — with an external LB — keeps serving
through control-plane restarts, so it exposes the volatile half of the
sizing read directly.
"""
# pylint: disable=protected-access
import asyncio
import json
import threading
import time
import unittest
from unittest import mock

from sky.serve import load_balancer as lb_module
from sky.serve import load_balancing_policies as lb_policies


def _make_balancer(policy):
    balancer = lb_module.SkyServeLoadBalancer('http://controller:8001', 0)
    balancer._load_balancing_policy = policy
    balancer._client_pool_lock = threading.Lock()
    balancer._ready = True
    balancer._draining = False
    balancer._last_sync_time = time.monotonic() - 4.0
    # Demand-feed + occupancy state (normally set in __init__).
    balancer._queue_depth = 0
    balancer._active_request_count = 0
    balancer._waiting_request_count = 0
    balancer._waiting_request_body_bytes = 0
    balancer._reject_last_seen = {}
    balancer._reject_fallback_seq = 0
    balancer._capacity_hint = None
    balancer._replica_occupancy = {}
    balancer._replica_total_slots = {}
    balancer._replica_free_slots = {}
    balancer._last_occupancy_probe_time = None
    return balancer


def _publish_occupancy_snapshot(balancer):
    """Install the complete process-state contract emitted by one probe."""
    sampled_at = balancer._last_occupancy_probe_time
    if sampled_at is None:
        sampled_at = time.monotonic()
        balancer._last_occupancy_probe_time = sampled_at
    sampled_urls = set(balancer._replica_occupancy)
    balancer._occupancy_dispatch_generation = {url: 0 for url in sampled_urls}
    balancer._occupancy_sample_generation = {url: 0 for url in sampled_urls}
    balancer._occupancy_sample_time = {url: sampled_at for url in sampled_urls}
    balancer._occupancy_current_round_sampled_urls = sampled_urls
    balancer._occupancy_sample_role_epoch = {
        url: balancer._occupancy_role_epoch for url in sampled_urls
    }


class TestCapacityEndpoint(unittest.TestCase):
    """Capacity endpoint aggregates only current, usable LB state."""

    def test_reports_ready_and_in_flight(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
        policy.load_map['http://a:8080'] = 2
        policy.load_map['http://b:8080'] = 1
        balancer = _make_balancer(policy)
        response = asyncio.run(balancer._capacity(mock.MagicMock()))
        body = json.loads(response.body)
        self.assertEqual(body['ready_replicas'], 2)
        self.assertEqual(body['routing_backend_count'], 2)
        self.assertEqual(body['occupancy_probed_backend_count'], 0)
        self.assertEqual(body['in_flight'], 3)
        self.assertEqual(body['local_in_flight'], 0)
        self.assertEqual(body['request_queue_depth'], 0)
        self.assertEqual(body['waiting_request_body_bytes'], 0)
        self.assertEqual(body['current_capacity'], 2)
        self.assertEqual(body['in_flight_capacity'], 3)
        self.assertIsNone(body['max_capacity'])
        self.assertFalse(body['draining'])
        self.assertTrue(body['synced'])
        self.assertGreaterEqual(body['last_sync_age_seconds'], 3.0)

    def test_attests_exact_service_identity_tuple(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([])
        balancer = _make_balancer(policy)
        balancer._service_name = 'boltz-l4-fleet'
        balancer._service_hash = '11111111-1111-4111-8111-111111111111'
        balancer._async_request_ledger_protocol_version = 1

        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)

        self.assertEqual(body['service_name'], 'boltz-l4-fleet')
        self.assertEqual(body['service_incarnation'],
                         '11111111-1111-4111-8111-111111111111')
        self.assertEqual(body['async_request_ledger_protocol_version'], 1)

    def test_never_advertises_exact_protocol_without_complete_identity(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([])
        balancer = _make_balancer(policy)
        balancer._async_request_ledger_protocol_version = 1

        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)

        self.assertIsNone(body['service_name'])
        self.assertIsNone(body['service_incarnation'])
        self.assertIsNone(body['async_request_ledger_protocol_version'])

    def test_round_robin_reports_unknown_in_flight(self):
        policy = lb_policies.RoundRobinPolicy()
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
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
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['queue_depth'], 0)
        self.assertEqual(body['rejected_in_window'], 0)
        self.assertEqual(body['rejected_in_recent_window'], 0)
        self.assertIsNone(body['provisioning_replicas'])
        self.assertIsNone(body['target_replicas'])
        self.assertIsNone(body['max_replicas'])

    def test_demand_fields_reflect_gauges_and_hint(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        balancer._queue_depth = 3
        balancer._active_request_count = 2
        balancer._waiting_request_count = 1
        balancer._waiting_request_body_bytes = 4096
        balancer._reject_last_seen = {'job-1': time.monotonic()}
        balancer._capacity_hint = {
            'provisioning_replicas': 4,
            'target_num_replicas': 12,
            'max_replicas': 20,
        }
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['queue_depth'], 3)
        self.assertEqual(body['local_in_flight'], 2)
        self.assertEqual(body['request_queue_depth'], 1)
        self.assertEqual(body['waiting_request_body_bytes'], 4096)
        self.assertEqual(body['rejected_in_window'], 1)
        self.assertEqual(body['rejected_in_recent_window'], 1)
        self.assertEqual(body['provisioning_replicas'], 4)
        self.assertEqual(body['target_replicas'], 12)
        self.assertEqual(body['max_replicas'], 20)
        self.assertEqual(body['current_capacity'], 1)
        self.assertEqual(body['max_capacity'], 20)

    def test_in_flight_ignores_pruned_replicas(self):
        # Load entries for replicas no longer ready must not inflate the
        # aggregate (prune keeps the map clean, but assert the contract).
        policy = lb_policies.InstanceAwareLeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
        policy.load_map['http://a:8080'] = 1
        policy.load_map['http://b:8080'] = 1
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['in_flight'], 1)

    def test_never_probed_reports_zero_occupancy_and_null_age(self):
        # Without a completed probe round the endpoint must not invent
        # capacity: no probed replicas, no free slots, null age.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['probed_replicas'], 0)
        self.assertEqual(body['busy_replicas'], 0)
        self.assertEqual(body['total_slots'], 0)
        self.assertEqual(body['running_slots'], 0)
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
        balancer._replica_total_slots = {'http://a:8080': 1, 'http://b:8080': 1}
        balancer._replica_free_slots = {'http://a:8080': 0, 'http://b:8080': 1}
        balancer._last_occupancy_probe_time = time.monotonic() - 2.0
        _publish_occupancy_snapshot(balancer)
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['probed_replicas'], 2)
        self.assertEqual(body['routing_backend_count'], 3)
        self.assertEqual(body['occupancy_probed_backend_count'], 2)
        self.assertEqual(body['busy_replicas'], 1)
        self.assertEqual(body['total_slots'], 2)
        self.assertEqual(body['running_slots'], 1)
        self.assertEqual(body['free_slots'], 1)
        # c is unprobed, so the generic contract falls back to one capacity
        # unit per ready replica instead of exposing a partial slot total.
        self.assertEqual(body['current_capacity'], 3)
        self.assertEqual(body['in_flight_capacity'], 1)
        self.assertGreaterEqual(body['occupancy_probe_age_seconds'], 1.0)

    def test_generic_capacity_does_not_mix_replica_and_slot_units(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://four-gpu:8080'])
        balancer = _make_balancer(policy)
        balancer._capacity_hint = {'max_replicas': 2}
        balancer._replica_occupancy = {'http://four-gpu:8080': 2}
        balancer._replica_total_slots = {'http://four-gpu:8080': 4}
        balancer._replica_free_slots = {'http://four-gpu:8080': 2}
        balancer._last_occupancy_probe_time = time.monotonic() - 1.0
        _publish_occupancy_snapshot(balancer)
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['current_capacity'], 4)
        self.assertEqual(body['in_flight_capacity'], 2)
        # max_replicas is a physical-backend count. Without an authoritative
        # slot-width plan, presenting it as a slot ceiling would erase valid
        # multi-worker headroom or overstate a rolling version's capacity.
        self.assertIsNone(body['max_capacity'])

    def test_generic_capacity_falls_back_when_slot_snapshot_is_stale(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://four-gpu:8080'])
        policy.load_map['http://four-gpu:8080'] = 1
        balancer = _make_balancer(policy)
        balancer._capacity_hint = {'max_replicas': 3}
        balancer._replica_occupancy = {'http://four-gpu:8080': 2}
        balancer._replica_total_slots = {'http://four-gpu:8080': 4}
        balancer._replica_free_slots = {'http://four-gpu:8080': 2}
        balancer._last_occupancy_probe_time = (
            time.monotonic() -
            lb_module.constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS - 1.0)
        _publish_occupancy_snapshot(balancer)
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['current_capacity'], 1)
        # The legacy in-flight aggregate conservatively includes both the
        # HTTP envelope and the last observed async occupancy when a probe is
        # stale, preserving the pre-existing fallback behavior.
        self.assertEqual(body['in_flight_capacity'], 3)
        self.assertEqual(body['max_capacity'], 3)

    def test_logical_mode_reuses_replica_names_for_slot_capacity(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://four-gpu:8080'])
        balancer = _make_balancer(policy)
        balancer._capacity_hint = {
            'replica_unit': 'logical_slot',
            'provisioning_replicas': 8,
            'target_num_replicas': 3,
            'max_replicas': 3,
            'configured_max_replicas': 3,
            'planned_capacity_by_url': {
                'http://four-gpu:8080': 4
            },
        }
        balancer._replica_occupancy = {'http://four-gpu:8080': 2}
        balancer._replica_total_slots = {'http://four-gpu:8080': 4}
        balancer._replica_free_slots = {'http://four-gpu:8080': 2}
        balancer._last_occupancy_probe_time = time.monotonic() - 1.0
        _publish_occupancy_snapshot(balancer)

        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)

        self.assertEqual(body['replica_unit'], 'logical_slot')
        self.assertEqual(body['ready_replicas'], 4)
        self.assertEqual(body['provisioning_replicas'], 8)
        self.assertEqual(body['target_replicas'], 3)
        self.assertEqual(body['configured_max_replicas'], 3)
        # Structural overhang is usable admission capacity.
        self.assertEqual(body['max_replicas'], 4)
        self.assertEqual(body['in_flight'], 2)
        self.assertEqual(body['current_capacity'], body['ready_replicas'])
        self.assertEqual(body['max_capacity'], body['max_replicas'])
        self.assertEqual(body['in_flight_capacity'], body['in_flight'])
        self.assertEqual(body['probed_replicas'], 4)
        # Qualification counts stay in physical backend units even when
        # admission-facing replica fields switch to logical slot units.
        self.assertEqual(body['routing_backend_count'], 1)
        self.assertEqual(body['occupancy_probed_backend_count'], 1)
        self.assertEqual(body['busy_replicas'], 2)

    def test_logical_bridge_ignores_routed_physical_backend_capacity(self):
        physical_url = 'http://physical:8080'
        logical_url = 'http://logical:8080'
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([physical_url, logical_url])
        balancer = _make_balancer(policy)
        balancer._capacity_hint = {
            'replica_unit': 'logical_slot',
            'max_replicas': 20,
            'configured_max_replicas': 20,
            'planned_capacity_by_url': {
                logical_url: 8
            },
            'logical_replica_urls': [logical_url],
        }
        balancer._replica_occupancy = {
            physical_url: 0,
            logical_url: 0,
        }
        balancer._replica_total_slots = {
            physical_url: 8,
            logical_url: 8,
        }
        balancer._replica_free_slots = {
            physical_url: 8,
            logical_url: 8,
        }
        balancer._last_occupancy_probe_time = time.monotonic() - 1.0
        _publish_occupancy_snapshot(balancer)

        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)

        self.assertEqual(body['ready_replicas'], 8)
        self.assertEqual(body['probed_replicas'], 8)
        self.assertEqual(body['total_slots'], 8)
        self.assertEqual(body['free_slots'], 8)

    def test_logical_mode_never_falls_back_to_physical_backend_count(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://eight-gpu:8080'])
        policy.load_map['http://eight-gpu:8080'] = 1
        balancer = _make_balancer(policy)
        balancer._capacity_hint = {
            'replica_unit': 'logical_slot',
            'max_replicas': 20,
            'configured_max_replicas': 20,
            'planned_capacity_by_url': {
                'http://eight-gpu:8080': 8
            },
        }
        balancer._replica_occupancy = {'http://eight-gpu:8080': 2}
        balancer._replica_total_slots = {'http://eight-gpu:8080': 8}
        balancer._replica_free_slots = {'http://eight-gpu:8080': 6}
        balancer._last_occupancy_probe_time = (
            time.monotonic() -
            lb_module.constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS - 1)
        _publish_occupancy_snapshot(balancer)

        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)

        self.assertEqual(body['ready_replicas'], 0)
        self.assertEqual(body['current_capacity'], 0)
        self.assertEqual(body['max_replicas'], 20)
        # Demand remains conservative while capacity fails closed.
        self.assertEqual(body['in_flight'], 3)

    def test_logical_mode_unprobed_ready_url_fails_closed(self):
        url = 'http://unprobed-eight-gpu:8080'
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([url])
        balancer = _make_balancer(policy)
        balancer._capacity_hint = {
            'replica_unit': 'logical_slot',
            'max_replicas': 20,
            'configured_max_replicas': 20,
            'planned_capacity_by_url': {
                url: 8
            },
        }

        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)

        self.assertEqual(body['ready_replicas'], 0)
        self.assertEqual(body['materialized_slots'], 8)
        self.assertEqual(body['total_slots'], 0)
        self.assertEqual(body['free_slots'], 0)
        self.assertEqual(body['current_capacity'], 0)
        self.assertEqual(body['occupancy_fresh_backend_count'], 0)
        self.assertEqual(body['occupancy_unknown_ready_backend_count'], 1)

    def test_logical_inventory_is_stable_during_partial_sampling(self):
        sampled = 'http://sampled-four-gpu:8080'
        unknown = 'http://unknown-four-gpu:8080'
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([sampled, unknown])
        balancer = _make_balancer(policy)
        balancer._capacity_hint = {
            'replica_unit': 'logical_slot',
            'max_replicas': 16,
            'configured_max_replicas': 16,
            'planned_capacity_by_url': {
                sampled: 4,
                unknown: 4,
            },
        }
        balancer._replica_occupancy = {sampled: 0}
        balancer._replica_total_slots = {sampled: 4}
        balancer._replica_free_slots = {sampled: 4}
        balancer._last_occupancy_probe_time = time.monotonic()
        _publish_occupancy_snapshot(balancer)

        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)

        self.assertEqual(body['materialized_slots'], 8)
        self.assertEqual(body['ready_replicas'], 0)
        self.assertEqual(body['free_slots'], 4)
        self.assertEqual(body['occupancy_fresh_backend_count'], 1)
        self.assertEqual(body['occupancy_unknown_ready_backend_count'], 1)

    def test_logical_mode_caps_runtime_slots_at_pinned_width(self):
        url = 'http://eight-gpu:8080'
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([url])
        balancer = _make_balancer(policy)
        balancer._capacity_hint = {
            'replica_unit': 'logical_slot',
            'max_replicas': 20,
            'configured_max_replicas': 20,
            'planned_capacity_by_url': {
                url: 8
            },
        }
        balancer._replica_occupancy = {url: 2}
        balancer._replica_total_slots = {url: 64}
        balancer._replica_free_slots = {url: 62}
        balancer._last_occupancy_probe_time = time.monotonic() - 1.0
        _publish_occupancy_snapshot(balancer)

        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)

        self.assertEqual(body['ready_replicas'], 8)
        self.assertEqual(body['total_slots'], 8)
        self.assertEqual(body['free_slots'], 6)
        self.assertEqual(body['busy_replicas'], 2)

    def test_occupancy_aggregates_debit_assigned_and_unassigned_slots(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://four-gpu:8080'])
        balancer = _make_balancer(policy)
        balancer._replica_occupancy = {'http://four-gpu:8080': 0}
        balancer._replica_total_slots = {'http://four-gpu:8080': 4}
        balancer._replica_free_slots = {'http://four-gpu:8080': 4}
        _publish_occupancy_snapshot(balancer)
        balancer._occupancy_pending_reservations = {'http://four-gpu:8080': 1}
        balancer._occupancy_unassigned_reservations = 1
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['probed_replicas'], 1)
        self.assertEqual(body['total_slots'], 4)
        self.assertEqual(body['running_slots'], 2)
        self.assertEqual(body['free_slots'], 2)

    def test_draining_replica_reports_running_work_without_capacity(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://draining:8080'])
        balancer = _make_balancer(policy)
        balancer._replica_occupancy = {'http://draining:8080': 1}
        balancer._replica_total_slots = {'http://draining:8080': 0}
        balancer._replica_free_slots = {'http://draining:8080': 0}
        _publish_occupancy_snapshot(balancer)
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['probed_replicas'], 1)
        self.assertEqual(body['total_slots'], 0)
        self.assertEqual(body['running_slots'], 1)
        self.assertEqual(body['free_slots'], 0)

    def test_probe_miss_keeps_accepted_reservation_visible_as_running(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://unknown:8080'])
        balancer = _make_balancer(policy)
        balancer._occupancy_pending_reservations = {'http://unknown:8080': 1}
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['probed_replicas'], 0)
        self.assertEqual(body['total_slots'], 0)
        self.assertEqual(body['running_slots'], 1)
        self.assertEqual(body['free_slots'], 0)

    def test_occupancy_ignores_pruned_replicas(self):
        # A probe entry for a replica the controller since removed from the
        # ready set must not count toward the aggregates.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080'])
        balancer = _make_balancer(policy)
        balancer._replica_occupancy = {'http://a:8080': 0, 'http://gone': 0}
        balancer._replica_total_slots = {'http://a:8080': 1, 'http://gone': 1}
        balancer._replica_free_slots = {'http://a:8080': 1, 'http://gone': 1}
        balancer._last_occupancy_probe_time = time.monotonic()
        _publish_occupancy_snapshot(balancer)
        body = json.loads(
            asyncio.run(balancer._capacity(mock.MagicMock())).body)
        self.assertEqual(body['probed_replicas'], 1)
        self.assertEqual(body['total_slots'], 1)
        self.assertEqual(body['running_slots'], 0)
        self.assertEqual(body['free_slots'], 1)
