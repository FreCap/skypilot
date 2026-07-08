"""Unit tests for the reserved-capacity fill overlay.

Opt-in (replica_policy.reserved_capacity_fill): the autoscaler additionally
scales up onto FREE zero-cost capacity reported by a controller poller,
bounded by max_replicas. The demand target and the controller's capacity
hint stay demand-only; surplus scale-ups carry a sentinel override that the
launch path pins to zero-cost ACTIVE locations (skipping entirely when none
is available -- fill must never spill to paid capacity).
"""
# pylint: disable=protected-access
import time
import types
import unittest
from unittest import mock

from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import serve_state
from sky.serve import spot_placer

_SCALE_UP = autoscalers.AutoscalerDecisionOperator.SCALE_UP
_SCALE_DOWN = autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
_FILL_KEY = constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY

# Pickleable form of the zero-cost k8s location, as handed to
# collect_reserved_capacity by the poller (Location.to_pickleable()).
_K8S_KEY = {
    'cloud': 'Kubernetes',
    'region': 'research-ctx',
    'zone': None,
    'accelerators': {
        'A100': 1
    },
    'use_spot': False,
    'image_id': None,
    'disk_tier': None,
}


def _spec(min_replicas=1, max_replicas=10, fill=True):
    return types.SimpleNamespace(min_replicas=min_replicas,
                                 max_replicas=max_replicas,
                                 num_overprovision=None,
                                 target_qps_per_replica=None,
                                 upscale_delay_seconds=None,
                                 downscale_delay_seconds=None,
                                 reserved_capacity_fill=fill)


def _make_autoscaler(**spec_kwargs):
    # target_qps_per_replica=None makes the demand target a constant
    # min_replicas, so every fill effect is isolated from demand math.
    return autoscalers.RequestRateAutoscaler('svc',
                                             _spec(**spec_kwargs),
                                             version=1)


def _replica(replica_id,
             location_key=None,
             status=serve_state.ReplicaStatus.READY,
             version=1):
    info = mock.Mock()
    info.replica_id = replica_id
    info.version = version
    info.status = status
    info.is_terminal = status in serve_state.ReplicaStatus.terminal_statuses()
    info.is_ready = status == serve_state.ReplicaStatus.READY
    info.cluster_name = f'cluster-{replica_id}'
    info.status_property.unrecoverable_failure.return_value = False
    info.get_spot_location.return_value = (
        spot_placer.Location.from_pickleable(location_key))
    return info


def _feed(autoscaler, free_slots, keys=(_K8S_KEY,), timestamp=None, polls=2):
    # polls=2 by default: an increase only takes effect after two
    # consecutive snapshots (damping).
    ts = time.time() if timestamp is None else timestamp
    for _ in range(polls):
        autoscaler.collect_reserved_capacity(free_slots, list(keys), ts)


def _decisions(autoscaler, replicas):
    return autoscaler.generate_scaling_decisions(replicas, [1])


def _ups(decisions):
    return [d for d in decisions if d.operator == _SCALE_UP]


def _downs(decisions):
    return [d for d in decisions if d.operator == _SCALE_DOWN]


def _stale_timestamp():
    return time.time() - (reserved_capacity.poll_interval_seconds() *
                          constants.RESERVED_CAPACITY_STALE_AFTER_INTERVALS +
                          30)


class TestFlagOff(unittest.TestCase):
    """Flag off: decisions identical to a fill-less autoscaler."""

    def test_disabled_is_passthrough_even_with_snapshot(self):
        replicas = [_replica(1, _K8S_KEY), _replica(2)]
        disabled = _make_autoscaler(fill=False)
        # Even a (spuriously) fed snapshot must not alter decisions.
        _feed(disabled, 5)
        control_spec = _spec()
        del control_spec.reserved_capacity_fill  # pre-flag spec object
        control = autoscalers.RequestRateAutoscaler('svc',
                                                    control_spec,
                                                    version=1)
        got = _decisions(disabled, replicas)
        expected = _decisions(control, replicas)
        self.assertEqual([(d.operator, d.target) for d in got],
                         [(d.operator, d.target) for d in expected])

    def test_disabled_overlay_returns_same_object(self):
        autoscaler = _make_autoscaler(fill=False)
        decisions = [
            autoscalers.AutoscalerDecision(_SCALE_UP, None),
        ]
        self.assertIs(autoscaler._apply_reserved_capacity_fill([], decisions),
                      decisions)

    def test_disabled_info_has_no_fill_keys(self):
        info = _make_autoscaler(fill=False).info()
        self.assertNotIn('fill_target', info)
        self.assertNotIn('fill_free_slots', info)
        self.assertNotIn('fill_snapshot_age', info)


class TestSentinelScaleUps(unittest.TestCase):
    """Surplus beyond demand carries ONLY the sentinel; demand ups don't."""

    def test_surplus_ups_sentinel_demand_ups_plain(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        _feed(autoscaler, 3)
        decisions = _decisions(autoscaler, [])
        ups = _ups(decisions)
        self.assertEqual(len(ups), 3)  # 1 demand (to min) + 2 fill
        plain = [d for d in ups if d.target is None]
        sentinel = [d for d in ups if d.target == {_FILL_KEY: True}]
        self.assertEqual(len(plain), 1)
        self.assertEqual(len(sentinel), 2)
        # Sentinel override carries NOTHING else.
        for decision in sentinel:
            self.assertEqual(set(decision.target), {_FILL_KEY})
        self.assertEqual(len(_downs(decisions)), 0)

    def test_max_replicas_clamps_fill_target(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=4)
        _feed(autoscaler, 100)
        decisions = _decisions(autoscaler, [])
        ups = _ups(decisions)
        self.assertEqual(len(ups), 4)
        self.assertEqual(len([d for d in ups if d.target == {
            _FILL_KEY: True
        }]), 3)
        self.assertEqual(autoscaler.info()['fill_target'], 4)


class TestScaleDownSuppression(unittest.TestCase):
    """Fill-covered surplus is not scaled down; uncovered surplus is."""

    def test_covered_surplus_suppressed_to_effective_target(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            _replica(3),  # paid
        ]
        _feed(autoscaler, 0)
        decisions = _decisions(autoscaler, replicas)
        # Demand target 1, fill target 2 (2 zero-cost + 0 free): only ONE
        # of the two demand scale-downs survives (down to the effective
        # target of 2), and no fill ups are emitted.
        self.assertEqual(len(_downs(decisions)), 1)
        self.assertEqual(len(_ups(decisions)), 0)

    def test_scale_down_resumes_when_fill_gone(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        # Zero-cost replicas evicted; snapshot reports 0 free.
        replicas = [_replica(3), _replica(4)]
        _feed(autoscaler, 0)
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(len(_downs(decisions)), 1)  # back to demand target
        self.assertEqual(len(_ups(decisions)), 0)


class TestMultiTickSlotSpending(unittest.TestCase):
    """A free slot is spent when the launch decision is emitted.

    Fill launches persist replica rows immediately, so zero_cost_count
    grows on the very next tick while the poller snapshot only refreshes
    on its interval: without spending, the same static snapshot would be
    re-consumed every tick, compounding the fill fleet.
    """

    def test_static_snapshot_not_reconsumed_across_ticks(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=10)
        _feed(autoscaler, 4)
        replicas = []
        next_id = 1
        total_fill_ups = 0
        for _ in range(3):
            decisions = _decisions(autoscaler, replicas)
            fill_ups = [
                d for d in _ups(decisions) if d.target == {
                    _FILL_KEY: True
                }
            ]
            total_fill_ups += len(fill_ups)
            # Emitted launches persist immediately: visible as zero-cost
            # nonterminal replicas on the next tick, before any new poll.
            for _ in fill_ups:
                replicas.append(_replica(next_id, _K8S_KEY))
                next_id += 1
        self.assertEqual(total_fill_ups, 4)
        self.assertEqual(len(replicas), 4)

    def test_spent_slots_not_regranted_by_single_stale_poll(self):
        # The raw-poll memory is deducted too: one poll still reporting
        # the pre-launch level (pods pending) must not re-raise the
        # damped value past the two-poll damping.
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=10)
        _feed(autoscaler, 4)
        decisions = _decisions(autoscaler, [])
        self.assertEqual(len(_ups(decisions)), 4)
        _feed(autoscaler, 4, polls=1)  # stale raw: pods not scheduled yet
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)


def _fill_ups(decisions):
    return [d for d in _ups(decisions) if d.target == {_FILL_KEY: True}]


class TestVersionAwareLaunchBaseline(unittest.TestCase):
    """Old-version zero-cost replicas never inflate fill launches.

    The launch baseline (max(latest nonterminal, demand target)) is
    latest-version-only, so the zero-cost count feeding the launch math
    must be too -- otherwise a rolling update's draining old fleet
    compounds fill launches every tick.
    """

    def test_rolling_update_zero_free_no_fill_launches(self):
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=2,
                                                             max_replicas=20),
                                                       version=2)
        _feed(autoscaler, 0)
        # 5 old-version zero-cost replicas still draining; the reserved
        # capacity they occupy polls as 0 free.
        replicas = [_replica(i, _K8S_KEY, version=1) for i in range(1, 6)]
        next_id = 100
        for _ in range(3):
            decisions = autoscaler.generate_scaling_decisions(replicas, [1])
            self.assertEqual(len(_fill_ups(decisions)), 0)
            # Demand launches persist rows immediately at the latest
            # version, pinned to the zero-cost location.
            for decision in _ups(decisions):
                if decision.target != {_FILL_KEY: True}:
                    replicas.append(
                        _replica(next_id,
                                 _K8S_KEY,
                                 status=serve_state.ReplicaStatus.PROVISIONING,
                                 version=2))
                    next_id += 1

    def test_old_zero_cost_replicas_do_not_inflate_free_slot_fill(self):
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=1,
                                                             max_replicas=20),
                                                       version=2)
        _feed(autoscaler, 3)
        replicas = [_replica(i, _K8S_KEY, version=1) for i in range(1, 6)]
        decisions = autoscaler.generate_scaling_decisions(replicas, [1])
        # Latest zero-cost 0 + 3 free slots, demand target 1: fill ups
        # bounded by the free slots (2 fill + 1 demand), NOT inflated to
        # 5 old + 3 free by the draining old-version fleet.
        self.assertEqual(len(_fill_ups(decisions)), 2)


class TestPendingReplicasOccupySlots(unittest.TestCase):
    """Not-yet-READY fill replicas keep their slots spent across polls.

    Launch threads can queue past the two-poll damping window; while the
    pods are invisible to the poller, raw polls keep reporting the
    pre-launch free level and damping re-raises the damped value. The
    pending rows must be treated as occupying those slots.
    """

    def test_unchanged_polls_with_pending_rows_emit_no_more_fills(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=20)
        _feed(autoscaler, 5)
        first = _decisions(autoscaler, [])
        self.assertEqual(len(_fill_ups(first)), 5)
        # Rows persist immediately, pods not created yet (not READY).
        replicas = [
            _replica(i, _K8S_KEY, status=serve_state.ReplicaStatus.PROVISIONING)
            for i in range(1, 6)
        ]
        # Two more polls still see the pre-launch level: damping re-raises
        # the damped free value, but the 5 pending rows occupy the slots.
        _feed(autoscaler, 5, polls=2)
        second = _decisions(autoscaler, replicas)
        self.assertEqual(len(_fill_ups(second)), 0)

    def test_steady_state_once_pending_turn_ready(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=20)
        _feed(autoscaler, 5)
        self.assertEqual(len(_fill_ups(_decisions(autoscaler, []))), 5)
        replicas = [
            _replica(i, _K8S_KEY, status=serve_state.ReplicaStatus.PROVISIONING)
            for i in range(1, 6)
        ]
        _feed(autoscaler, 5, polls=2)  # pods still invisible
        self.assertEqual(len(_fill_ups(_decisions(autoscaler, replicas))), 0)
        # Pods bind and turn READY; the poller now sees the slots taken
        # (decrease applies immediately).
        replicas = [_replica(i, _K8S_KEY) for i in range(1, 6)]
        _feed(autoscaler, 0, polls=1)
        for _ in range(2):
            decisions = _decisions(autoscaler, replicas)
            # Steady state: no new fills, no scale-down of the fill
            # fleet (no oscillation).
            self.assertEqual(decisions, [])


class TestVictimAwareSuppression(unittest.TestCase):
    """Only zero-cost victims are sheltered; paid downs always pass."""

    def test_paid_down_passes_zero_cost_suppressed(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            # Paid replica with the HIGHEST id: first victim in the
            # subclass's newest-first ordering, so a victim-blind
            # surplus keep would shelter it while killing zero-cost.
            _replica(3),
        ]
        _feed(autoscaler, 2)
        decisions = _decisions(autoscaler, replicas)
        # Demand target 1 -> victims [3, 2]; surplus (fill_target 4 -
        # demand 1 = 3) covers both, but only the zero-cost victim (2)
        # may be sheltered: the paid down (3) must pass through.
        self.assertEqual([d.target for d in _downs(decisions)], [3])


class TestDumpLoadFillState(unittest.TestCase):
    """Fill state survives the update_service autoscaler swap."""

    def test_roundtrip_suppresses_pre_poll(self):
        old = _make_autoscaler(min_replicas=1)
        _feed(old, 3)
        fresh = _make_autoscaler(min_replicas=1)
        fresh.load_dynamic_states(old.dump_dynamic_states())
        self.assertEqual(fresh.info()['fill_free_slots'], 3)
        # Before any poll reaches the fresh instance, it must already
        # protect the fill fleet: paid victim passes, zero-cost victim
        # is suppressed.
        replicas = [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            _replica(3),
        ]
        decisions = _decisions(fresh, replicas)
        self.assertEqual([d.target for d in _downs(decisions)], [3])

    def test_load_tolerates_dump_without_fill_state(self):
        fresh = _make_autoscaler()
        # Dump shape from a build predating the fill feature.
        fresh.load_dynamic_states({
            'latest_version_ever_ready': 1,
            'request_timestamps': [],
        })
        self.assertEqual(fresh.info()['fill_free_slots'], 0)
        self.assertIsNone(fresh.info()['fill_snapshot_age'])


class TestRelaxedZeroCostMatching(unittest.TestCase):
    """Matching ignores image_id/disk_tier; legacy rows match by region."""

    def setUp(self):
        self.autoscaler = _make_autoscaler(min_replicas=1)
        _feed(self.autoscaler, 0)

    def test_legacy_shape_less_row_matches(self):
        legacy = _replica(1, {
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'zone': None,
        })
        self.assertTrue(self.autoscaler._replica_on_zero_cost_location(legacy))

    def test_image_changed_row_matches(self):
        image_changed = _replica(
            2,
            dict(_K8S_KEY, image_id={None: 'docker:model:v2'},
                 disk_tier='high'))
        self.assertTrue(
            self.autoscaler._replica_on_zero_cost_location(image_changed))

    def test_different_shape_or_region_does_not_match(self):
        other_gpu = _replica(3, dict(_K8S_KEY, accelerators={'H100': 1}))
        other_region = _replica(4, dict(_K8S_KEY, region='other-ctx'))
        self.assertFalse(
            self.autoscaler._replica_on_zero_cost_location(other_gpu))
        self.assertFalse(
            self.autoscaler._replica_on_zero_cost_location(other_region))


class TestPollerFlagOff(unittest.TestCase):
    """Poller skips the expensive query when the live flag is off."""

    class _Stop(Exception):
        pass

    def _run_one_cycle(self, autoscaler):
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        with mock.patch.object(reserved_capacity,
                               'query_free_slots',
                               return_value=0) as query, \
             mock.patch.object(reserved_capacity.time,
                               'sleep',
                               side_effect=self._Stop):
            with self.assertRaises(self._Stop):
                reserved_capacity.poller_loop(lambda: autoscaler,
                                              lambda: placer)
        return placer, query

    def test_flag_off_sleeps_without_querying(self):
        autoscaler = _make_autoscaler(fill=False)
        placer, query = self._run_one_cycle(autoscaler)
        query.assert_not_called()
        placer.zero_cost_locations.assert_not_called()
        self.assertIsNone(autoscaler._fill_snapshot_time)

    def test_flag_on_queries_and_feeds(self):
        autoscaler = _make_autoscaler(fill=True)
        _, query = self._run_one_cycle(autoscaler)
        query.assert_called_once()
        self.assertIsNotNone(autoscaler._fill_snapshot_time)


class TestStaleSnapshot(unittest.TestCase):
    """Stale snapshot: 0 free contribution, existing fill still protected."""

    def test_stale_free_slots_contribute_zero(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(1, _K8S_KEY), _replica(2, _K8S_KEY)]
        _feed(autoscaler, 5, timestamp=_stale_timestamp())
        decisions = _decisions(autoscaler, replicas)
        # fill_target = 2 zero-cost + 0 (stale): exactly covers the
        # existing fill replicas -- no new fill ups, no victimization of
        # the live fill fleet just because the poller died.
        self.assertEqual(decisions, [])
        self.assertEqual(autoscaler.info()['fill_target'], 2)

    def test_fresh_snapshot_contributes(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(1, _K8S_KEY), _replica(2, _K8S_KEY)]
        _feed(autoscaler, 5)
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(
            len([d for d in _ups(decisions) if d.target == {
                _FILL_KEY: True
            }]), 5)  # fill_target 7 - current 2


class TestIncreaseDamping(unittest.TestCase):
    """Increases need two consecutive polls; decreases apply at once."""

    def test_two_poll_increase_immediate_decrease(self):
        autoscaler = _make_autoscaler()

        def free_slots():
            return autoscaler.info()['fill_free_slots']

        _feed(autoscaler, 5, polls=1)
        self.assertEqual(free_slots(), 0)  # first sight of an increase
        _feed(autoscaler, 5, polls=1)
        self.assertEqual(free_slots(), 5)  # persisted across two polls
        _feed(autoscaler, 8, polls=1)
        self.assertEqual(free_slots(), 5)  # new increase: wait again
        _feed(autoscaler, 9, polls=1)
        self.assertEqual(free_slots(), 8)  # min of the two above-5 polls
        _feed(autoscaler, 2, polls=1)
        self.assertEqual(free_slots(), 2)  # decrease: immediate

    def test_transient_spike_never_acted_on(self):
        autoscaler = _make_autoscaler()
        _feed(autoscaler, 0, polls=2)
        _feed(autoscaler, 10, polls=1)
        _feed(autoscaler, 0, polls=1)
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)


class TestCapacityHintDemandOnly(unittest.TestCase):
    """Fill never inflates the demand target the capacity hint reports."""

    def test_final_target_unchanged_by_fill(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        _feed(autoscaler, 5)
        decisions = _decisions(autoscaler, [])
        self.assertGreater(len(_ups(decisions)), 1)  # fill engaged
        self.assertEqual(autoscaler.get_final_target_num_replicas(), 1)
        info = autoscaler.info()
        self.assertEqual(info['target_num_replicas'], 1)
        self.assertEqual(info['fill_target'], 5)


def _make_location(region, cost_marker, use_spot=False):
    del cost_marker  # readability only; cost set via _make_placer
    cloud = mock.MagicMock()
    cloud.is_same_cloud = lambda other: str(other) == str(cloud)
    return spot_placer.Location(cloud=cloud,
                                region=region,
                                zone=None,
                                accelerators={'A100': 1},
                                use_spot=use_spot)


def _make_placer(costs):
    placer = spot_placer.DynamicFallbackSpotPlacer.__new__(
        spot_placer.DynamicFallbackSpotPlacer)
    placer.location2status = {
        loc: spot_placer.LocationStatus.ACTIVE for loc in costs
    }
    placer.location2preempted_at = {}
    placer.location2cost = dict(costs)
    return placer


class TestZeroCostSelection(unittest.TestCase):
    """select_next_zero_cost_location: zero-cost ACTIVE or nothing."""

    def setUp(self):
        self.k8s = _make_location('research-ctx', 'free')
        self.paid = _make_location('us-east-1', 'paid', use_spot=True)
        self.placer = _make_placer({self.k8s: 0.0, self.paid: 0.2})

    def test_returns_active_zero_cost(self):
        self.assertEqual(self.placer.select_next_zero_cost_location([]),
                         self.k8s)

    def test_benched_zero_cost_returns_none_never_paid(self):
        with mock.patch.object(spot_placer.time, 'time', return_value=1000.0):
            self.placer.set_preemptive(self.k8s)
            self.assertIsNone(self.placer.select_next_zero_cost_location([]))

    def test_no_zero_cost_at_all_returns_none(self):
        placer = _make_placer({self.paid: 0.2})
        self.assertIsNone(placer.select_next_zero_cost_location([]))

    def test_enumeration_includes_benched(self):
        with mock.patch.object(spot_placer.time, 'time', return_value=1000.0):
            self.placer.set_preemptive(self.k8s)
            self.assertIn(self.k8s, self.placer.zero_cost_locations())

    def test_least_loaded_zero_cost_wins(self):
        other = _make_location('research-ctx-2', 'free')
        placer = _make_placer({self.k8s: 0.0, other: 0.0, self.paid: 0.2})
        selected = placer.select_next_zero_cost_location([self.k8s, self.k8s])
        self.assertEqual(selected, other)


class TestFillLaunchPath(unittest.TestCase):
    """Sentinel launches pin zero-cost-only; aborts leak nothing."""

    def _make_manager(self, placer):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager.yaml_content = 'unused: patched helpers below'
        manager._spot_placer = placer
        manager._launch_thread_pool = {}
        manager._replica_to_request_id = {}
        manager._replica_to_launch_cancelled = {}
        manager._fill_skip_last_log_time = 0.0
        manager._next_replica_id = 7
        manager.latest_version = 1
        return manager

    def test_abort_creates_no_record_and_keeps_id(self):
        placer = mock.Mock()
        placer.select_next_zero_cost_location.return_value = None
        manager = self._make_manager(placer)
        override = {_FILL_KEY: True}
        # use_spot=False: the sentinel alone must force the placer path
        # (zero-cost k8s entries are non-spot).
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=None), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            manager._scale_up_one_locked(override)
        add_mock.assert_not_called()
        self.assertEqual(manager._next_replica_id, 7)
        self.assertEqual(manager._launch_thread_pool, {})
        placer.select_next_location.assert_not_called()
        # The caller's dict is not mutated (pop happens on a copy).
        self.assertIn(_FILL_KEY, override)

    def test_success_pins_location_and_strips_sentinel(self):
        location = _make_location('research-ctx', 'free')
        placer = mock.Mock()
        placer.select_next_zero_cost_location.return_value = location
        manager = self._make_manager(placer)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=None), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            manager._scale_up_one_locked({_FILL_KEY: True})
        add_mock.assert_called_once()
        info = add_mock.call_args[0][2]
        self.assertNotIn(_FILL_KEY, info.resources_override)
        # Launch pinned to the zero-cost location (non-spot k8s).
        self.assertIs(info.resources_override['use_spot'], False)
        self.assertEqual(info.resources_override['region'], 'research-ctx')
        self.assertIs(info.is_spot, False)
        self.assertEqual(manager._next_replica_id, 8)

    def test_sentinel_without_placer_aborts(self):
        manager = self._make_manager(placer=None)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            launched = manager._launch_replica(7, {_FILL_KEY: True})
        self.assertFalse(launched)
        add_mock.assert_not_called()


class TestQueryFreeSlots(unittest.TestCase):
    """Poller free-slot math: -1 is 0 free, slots are per-replica GPUs."""

    def _k8s_location(self, region='research-ctx', gpu='A100', count=1):
        return spot_placer.Location.from_pickleable({
            'cloud': 'Kubernetes',
            'region': region,
            'zone': None,
            'accelerators': {
                gpu: count
            },
            'use_spot': False,
        })

    def test_free_gpus_divided_by_replica_size(self):
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 5
                               })):
            self.assertEqual(
                reserved_capacity.query_free_slots(
                    [self._k8s_location(count=2)]), 2)

    def test_unknown_availability_counts_zero(self):
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': -1
                               })):
            self.assertEqual(
                reserved_capacity.query_free_slots([self._k8s_location()]), 0)

    def test_same_shape_counted_once_and_non_k8s_skipped(self):
        aws = spot_placer.Location.from_pickleable({
            'cloud': 'AWS',
            'region': 'us-east-1',
            'zone': None,
            'accelerators': {
                'L4': 1
            },
            'use_spot': True,
        })
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 3
                               })) as query:
            total = reserved_capacity.query_free_slots(
                [self._k8s_location(),
                 self._k8s_location(), aws])
        self.assertEqual(total, 3)
        query.assert_called_once()


if __name__ == '__main__':
    unittest.main()
