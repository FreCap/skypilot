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

from aiohttp import web

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
            }), (1, 0, 1))

    def test_idle_payload(self):
        self.assertEqual(
            self._parse({
                'status': 'READY',
                'running_count': 0,
                'predict_concurrency': 1
            }), (0, 1, 1))

    def test_draining_reports_zero_free_slots(self):
        # The wrapper reports predict_concurrency 0 while draining, so free
        # slots are naturally 0 without a special case.
        self.assertEqual(
            self._parse({
                'status': 'DRAINING',
                'running_count': 1,
                'predict_concurrency': 0
            }), (1, 0, 0))

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
    balancer = lb_module.SkyServeLoadBalancer('http://controller:8001', 0)
    balancer._load_balancing_policy = policy
    balancer._client_pool_lock = threading.Lock()
    balancer._ready = True
    balancer._draining = False
    balancer._last_sync_time = time.monotonic()
    balancer._replica_occupancy = {}
    balancer._replica_total_slots = {}
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
        self._run_round(balancer, {A: (1, 0, 1), B: (0, 1, 1)})
        self.assertEqual(balancer._replica_occupancy, {A: 1, B: 0})
        self.assertEqual(balancer._replica_total_slots, {A: 1, B: 1})
        self.assertEqual(balancer._replica_free_slots, {A: 0, B: 1})
        self.assertIsNotNone(balancer._last_occupancy_probe_time)
        # The policy received the same occupancy: a deprioritized in routing.
        self.assertEqual(policy._select_replica(mock.MagicMock(), [A, B]), B)

    def test_probe_with_empty_ready_set_clears_state(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A])
        balancer = _make_balancer(policy)
        self._run_round(balancer, {A: (1, 0, 1)})
        self.assertEqual(balancer._replica_occupancy, {A: 1})
        policy.set_ready_replicas([])
        self._run_round(balancer, {})
        self.assertEqual(balancer._replica_occupancy, {})
        self.assertEqual(balancer._replica_total_slots, {})
        self.assertEqual(balancer._replica_free_slots, {})
        self.assertEqual(policy.occupancy_map, {})

    def test_probe_round_replaces_stale_entries(self):
        # A replica that finished its job between rounds regains priority.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A, B])
        balancer = _make_balancer(policy)
        self._run_round(balancer, {A: (1, 0, 1), B: (0, 1, 1)})
        self._run_round(balancer, {A: (0, 1, 1), B: (1, 0, 1)})
        self.assertEqual(balancer._replica_occupancy, {A: 0, B: 1})
        self.assertEqual(policy._select_replica(mock.MagicMock(), [A, B]), A)

    def test_probe_miss_retains_local_capacity_but_not_idle_proof(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A])
        balancer = _make_balancer(policy)
        self._run_round(balancer, {A: (0, 4, 4)})
        self._run_round(balancer, {})

        with balancer._client_pool_lock:
            self.assertEqual(balancer._effective_replica_free_slots_locked(),
                             {A: 4})
        self.assertEqual(policy.occupancy_map, {A: 0})
        in_flight, _, unknown_urls, sampled_urls = (
            balancer._in_flight_with_draining())
        self.assertNotIn(A, in_flight or {})
        self.assertEqual(unknown_urls, [A])
        self.assertEqual(sampled_urls, [])

        self._run_round(balancer, {A: (1, 3, 4)})
        self.assertEqual(balancer._replica_occupancy, {A: 1})
        self.assertEqual(balancer._in_flight_with_draining()[0], {A: 1})

    def test_sample_expires_per_url_while_probe_loop_is_healthy(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A, B])
        balancer = _make_balancer(policy)
        self._run_round(balancer, {A: (0, 1, 1), B: (0, 1, 1)})
        balancer._occupancy_sample_time[A] -= (
            lb_module.constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS + 1)

        self._run_round(balancer, {B: (0, 1, 1)})

        self.assertLess(time.monotonic() - balancer._last_occupancy_probe_time,
                        1)
        self.assertNotIn(A, balancer._replica_occupancy)
        with balancer._client_pool_lock:
            self.assertEqual(balancer._effective_replica_free_slots_locked(),
                             {B: 1})

    def test_probe_crossing_role_epoch_cannot_republish_sample(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A])
        balancer = _make_balancer(policy)

        async def _run():
            probe_started = asyncio.Event()
            finish_probe = asyncio.Event()

            async def _fetch(session, url):
                del session, url
                probe_started.set()
                await finish_probe.wait()
                return (0, 4, 4)

            balancer._fetch_replica_occupancy = _fetch
            probe = asyncio.create_task(
                balancer._probe_replica_occupancy_once())
            await probe_started.wait()
            with balancer._client_pool_lock:
                balancer._occupancy_role_epoch += 1
                balancer._invalidate_occupancy_samples_locked()
            finish_probe.set()
            await probe

        asyncio.run(_run())

        self.assertEqual(balancer._replica_occupancy, {})
        self.assertEqual(balancer._replica_free_slots, {})
        self.assertEqual(balancer._occupancy_current_round_sampled_urls, set())

    def test_probe_crossing_route_projection_cannot_republish_sample(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas([A])
        balancer = _make_balancer(policy)
        with balancer._client_pool_lock:
            balancer._routing_version = 1
            balancer._route_projection_generation = 10
            balancer._route_projection_sha256 = 'a' * 64
            balancer._route_source_epoch = 2

        async def _run():
            probe_started = asyncio.Event()
            finish_probe = asyncio.Event()

            async def _fetch(session, url):
                del session, url
                probe_started.set()
                await finish_probe.wait()
                return (0, 4, 4)

            balancer._fetch_replica_occupancy = _fetch
            probe = asyncio.create_task(
                balancer._probe_replica_occupancy_once())
            await probe_started.wait()
            with balancer._client_pool_lock:
                balancer._routing_version = 1
                balancer._route_projection_generation = 11
                balancer._route_projection_sha256 = 'b' * 64
                balancer._route_source_epoch = 2
                balancer._occupancy_role_epoch += 1
                balancer._occupancy_current_round_sampled_urls = set()
                balancer._occupancy_sampled_off_ready = set()
            finish_probe.set()
            await probe

            self.assertEqual(balancer._replica_occupancy, {})
            self.assertEqual(balancer._replica_free_slots, {})
            self.assertEqual(balancer._occupancy_current_round_sampled_urls,
                             set())

            await balancer._probe_replica_occupancy_once()

        asyncio.run(_run())

        self.assertEqual(balancer._replica_occupancy, {A: 0})
        self.assertEqual(balancer._replica_free_slots, {A: 4})
        self.assertEqual(balancer._occupancy_current_round_sampled_urls, {A})

    def test_large_partial_probe_round_keeps_bounded_local_inventory(self):
        urls = [f'http://worker-{index}:8080' for index in range(342)]
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(urls)
        balancer = _make_balancer(policy)
        self._run_round(balancer, {url: (0, 1, 1) for url in urls})

        successful_urls = set(urls[:137])
        self._run_round(balancer, {url: (0, 1, 1) for url in successful_urls})

        with balancer._client_pool_lock:
            self.assertEqual(
                len(balancer._effective_replica_free_slots_locked()), 342)
            self.assertEqual(balancer._controller_occupancy_proof_urls_locked(),
                             successful_urls)

    def test_probe_round_reaches_fleet_beyond_default_connector_limit(self):
        """Every replica probe must start within the same timeout window."""

        async def _run():
            replica_count = 101
            entered = 0
            all_entered = asyncio.Event()
            release_responses = asyncio.Event()

            async def _capacity(request):
                nonlocal entered
                del request
                entered += 1
                if entered == replica_count:
                    all_entered.set()
                await release_responses.wait()
                return web.json_response({
                    'running_count': 0,
                    'predict_concurrency': 1,
                })

            app = web.Application()
            app.router.add_post('/{path:.*}', _capacity)
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            site = web.TCPSite(runner, '127.0.0.1', 0)
            await site.start()
            assert site._server is not None
            port = site._server.sockets[0].getsockname()[1]
            urls = [
                f'http://127.0.0.1:{port}/replica-{i}'
                for i in range(replica_count)
            ]
            policy = lb_policies.LeastLoadPolicy()
            policy.set_ready_replicas(urls)
            balancer = _make_balancer(policy)
            reached_whole_fleet = False
            entered_before_release = 0
            try:
                with mock.patch.object(lb_module.constants,
                                       'LB_OCCUPANCY_PROBE_TIMEOUT_SECONDS',
                                       10):
                    probe = asyncio.create_task(
                        balancer._probe_replica_occupancy_once())
                    try:
                        await asyncio.wait_for(all_entered.wait(), timeout=5)
                        reached_whole_fleet = True
                    except asyncio.TimeoutError:
                        entered_before_release = entered
                    finally:
                        release_responses.set()
                    await probe
            finally:
                release_responses.set()
                await runner.cleanup()

            self.assertTrue(
                reached_whole_fleet,
                f'only {entered_before_release}/{replica_count} replica '
                'probes started; '
                'later probes were queued behind the connector limit')

        asyncio.run(_run())


if __name__ == '__main__':
    unittest.main()
