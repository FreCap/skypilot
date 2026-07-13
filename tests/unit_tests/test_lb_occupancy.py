"""[boltz fork] LB async-occupancy probing.

Async fast-ack workloads finish the HTTP envelope in milliseconds while the
replica crunches for hours, so the envelope-scoped in-flight accounting reads
every replica as idle. The LB probes each ready replica's `async_capacity`
action and (a) deprioritizes busy replicas in routing, (b) reports true free
slots on /_lb/capacity. The probe is a hint: unknown never means busy, and
an all-busy fleet still routes (the replica's own shedding is the backstop).
"""
# pylint: disable=protected-access
import asyncio
import threading
import time
import unittest
from unittest import mock

from sky.serve import load_balancer as lb_module
from sky.serve import load_balancing_policies as lb_policies

A = 'http://a:8080'
B = 'http://b:8080'
C = 'http://c:8080'


class TestOccupancyRouting(unittest.TestCase):
    """Weighted deprioritize: busy replicas sort last, never exiled."""

    def test_least_load_avoids_busy_replica(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A, B])
        # a is crunching a job; b is idle but carries a transient envelope
        # request. Occupancy must dominate the envelope count.
        policy.set_occupancy({A: 1})
        policy.load_map[B] = 3
        for _ in range(10):
            self.assertEqual(policy._select_replica(mock.MagicMock(), [A, B]),
                             B)

    def test_least_load_all_busy_still_selects(self):
        # When EVERY replica is busy the min is still defined — the request
        # proxies and the replica's 429 shed stays the backstop. A stale
        # occupancy map must never black-hole routing.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A, B])
        policy.set_occupancy({A: 1, B: 1})
        self.assertIn(policy._select_replica(mock.MagicMock(), [A, B]), [A, B])

    def test_least_load_unknown_occupancy_is_idle(self):
        # A replica absent from the occupancy map (probe failed) is treated
        # as idle — falling back to today's envelope-only behavior.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A, B])
        policy.set_occupancy({A: 1})
        self.assertEqual(policy._select_replica(mock.MagicMock(), [A, B]), B)

    def test_occupancy_replaced_wholesale(self):
        # Each probe round replaces the map: a replica busy last round and
        # unreported this round no longer counts.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A, B])
        policy.set_occupancy({A: 1})
        policy.set_occupancy({B: 1})
        self.assertEqual(policy._select_replica(mock.MagicMock(), [A, B]), A)

    def test_incremental_occupancy_update_preserves_other_replicas(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A, B])
        policy.set_occupancy({A: 1, B: 2})
        policy.set_occupancy_for_replica(A, 3)
        self.assertEqual(policy.occupancy_map, {A: 3, B: 2})
        policy.set_occupancy_for_replica(A, None)
        self.assertEqual(policy.occupancy_map, {B: 2})

    def test_instance_aware_folds_occupancy_before_normalization(self):
        policy = lb_policies.InstanceAwareLeastLoadPolicy()
        policy.set_ready_replicas([A, B])
        policy.set_replica_info({
            A: {
                'gpu_type': 'L4',
                'gpu_count': '1'
            },
            B: {
                'gpu_type': 'L4',
                'gpu_count': '1'
            },
        })
        policy.set_target_qps_per_accelerator({'L4': 0.1})
        policy.set_occupancy({A: 1})
        for _ in range(10):
            self.assertEqual(policy._select_replica(mock.MagicMock(), [A, B]),
                             B)

    def test_round_robin_ignores_occupancy(self):
        # Policies without load accounting accept the hint as a no-op.
        policy = lb_policies.RoundRobinPolicy()
        policy.set_ready_replicas([A])
        policy.set_occupancy({A: 1})  # Must not raise.
        self.assertEqual(policy._select_replica(mock.MagicMock(), [A]), A)


class TestOccupancyParsing(unittest.TestCase):
    """The defensive parser for the replica's async_capacity payload."""

    def _parse(self, raw):
        return lb_module.SkyServeLoadBalancer._parse_replica_occupancy(raw)

    def test_ready_payload(self):
        self.assertEqual(
            self._parse({
                'status': 'READY',
                'running_count': 1,
                'predict_concurrency': 1
            }), (1, 0))

    def test_idle_payload(self):
        self.assertEqual(
            self._parse({
                'status': 'READY',
                'running_count': 0,
                'predict_concurrency': 1
            }), (0, 1))

    def test_draining_reports_zero_free_slots(self):
        # The wrapper reports predict_concurrency 0 while draining, so free
        # slots are naturally 0 without a special case.
        self.assertEqual(
            self._parse({
                'status': 'DRAINING',
                'running_count': 1,
                'predict_concurrency': 0
            }), (1, 0))

    def test_non_conforming_shapes_are_unknown(self):
        # Older images without the action answer something else entirely —
        # every such shape is "unknown", never a guess.
        for raw in [
                None,
                'error',
            {},
            {
                'running_count': 'one',
                'predict_concurrency': 1
            },
            {
                'running_count': -1,
                'predict_concurrency': 1
            },
            {
                'running_count': True,
                'predict_concurrency': 1
            },
            {
                'running_count': 1
            },
        ]:
            self.assertIsNone(self._parse(raw), msg=f'raw={raw!r}')


def _make_balancer(policy):
    balancer = object.__new__(lb_module.SkyServeLoadBalancer)
    balancer._load_balancing_policy = policy
    balancer._client_pool_lock = threading.Lock()
    balancer._ready = True
    balancer._draining = False
    balancer._last_sync_time = time.monotonic()
    balancer._replica_occupancy = {}
    balancer._replica_free_slots = {}
    balancer._last_occupancy_probe_time = None
    return balancer


class TestProbeRound(unittest.TestCase):
    """One probe round rebuilds the maps and feeds the policy."""

    def _run_round(self, balancer, results_by_url):

        async def fake_fetch(session, url):
            del session
            return results_by_url.get(url)

        with mock.patch.object(balancer, '_fetch_replica_occupancy',
                               fake_fetch):
            asyncio.run(balancer._probe_replica_occupancy_once())

    def test_probe_updates_maps_and_policy(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A, B, C])
        balancer = _make_balancer(policy)
        # a busy, b idle, c unreachable (fetch -> None).
        self._run_round(balancer, {A: (1, 0), B: (0, 1)})
        self.assertEqual(balancer._replica_occupancy, {A: 1, B: 0})
        self.assertEqual(balancer._replica_free_slots, {A: 0, B: 1})
        self.assertIsNotNone(balancer._last_occupancy_probe_time)
        # The policy received the same occupancy: a deprioritized in routing.
        self.assertEqual(policy._select_replica(mock.MagicMock(), [A, B]), B)

    def test_probe_with_empty_ready_set_clears_state(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A])
        balancer = _make_balancer(policy)
        self._run_round(balancer, {A: (1, 0)})
        self.assertEqual(balancer._replica_occupancy, {A: 1})
        policy.set_ready_replicas([])
        self._run_round(balancer, {})
        self.assertEqual(balancer._replica_occupancy, {})
        self.assertEqual(balancer._replica_free_slots, {})
        self.assertEqual(policy.occupancy_map, {})

    def test_probe_round_replaces_stale_entries(self):
        # A replica that finished its job between rounds regains priority.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A, B])
        balancer = _make_balancer(policy)
        self._run_round(balancer, {A: (1, 0), B: (0, 1)})
        self._run_round(balancer, {A: (0, 1), B: (1, 0)})
        self.assertEqual(balancer._replica_occupancy, {A: 0, B: 1})
        self.assertEqual(policy._select_replica(mock.MagicMock(), [A, B]), A)


if __name__ == '__main__':
    unittest.main()
