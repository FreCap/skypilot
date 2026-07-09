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
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import spot_placer

_SCALE_UP = autoscalers.AutoscalerDecisionOperator.SCALE_UP
_SCALE_DOWN = autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
_FILL_KEY = constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY
_EPOCH_KEY = constants.RESERVED_FILL_GRANT_EPOCH_OVERRIDE_KEY
_POOL_KEY = constants.RESERVED_FILL_POOL_KEY_OVERRIDE_KEY

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
             version=1,
             created_at=None):
    # created_at=None mirrors a pre-upgrade pickled row (treated as older
    # than any fill snapshot); pass a float to model a row created at a
    # known time relative to the snapshot.
    info = mock.Mock()
    info.replica_id = replica_id
    info.version = version
    info.status = status
    info.is_terminal = status in serve_state.ReplicaStatus.terminal_statuses()
    info.is_ready = status == serve_state.ReplicaStatus.READY
    info.cluster_name = f'cluster-{replica_id}'
    info.created_at = created_at
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


class TestAllVersionOccupancyDebit(unittest.TestCase):
    """The occupancy debit spans ALL versions, not just the latest.

    An old-version zero-cost replica still PROVISIONING has an unbound
    pod: its slot polls free, yet the row holds a claim on it. If only
    latest-version rows were debited, a fill launch would collide with
    that claim and fail on capacity.
    """

    def test_old_version_provisioning_row_debits_spendable(self):
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=1,
                                                             max_replicas=20),
                                                       version=2)
        _feed(autoscaler, 3)
        replicas = [
            _replica(1,
                     _K8S_KEY,
                     status=serve_state.ReplicaStatus.PROVISIONING,
                     version=1)
        ]
        decisions = autoscaler.generate_scaling_decisions(replicas, [1])
        # Spendable 3 - 1 (old-version pending claim) = 2; the launch
        # baseline stays latest-only: fill_target_launch = 0 latest
        # zero-cost + 2 spendable, demand target 1 -> exactly 1 fill up
        # (a latest-only debit would emit 2, one onto the claimed slot).
        self.assertEqual(len(_fill_ups(decisions)), 1)


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


class TestTailSuppression(unittest.TestCase):
    """Partial surplus shelters the LEAST-preferred zero-cost victims."""

    def test_partial_surplus_keeps_ready_kills_provisioning(self):
        # Victims are emitted most-preferred-first (PROVISIONING before
        # READY). With surplus 1 covering only one of the two zero-cost
        # victims, the shelter must go to the READY replica serving
        # traffic -- a prefix keep would shelter the warming one and
        # kill the serving one.
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [
            _replica(1),  # paid READY (oldest): survives as the target.
            _replica(2, _K8S_KEY),  # zero-cost READY
            _replica(3, _K8S_KEY,
                     status=serve_state.ReplicaStatus.PROVISIONING),
        ]
        # Fresh snapshot with 0 free slots: fill_target = zc_count (2),
        # demand 1 -> surplus 1.
        _feed(autoscaler, 0)
        decisions = _decisions(autoscaler, replicas)
        # Demand victims: [3 (PROVISIONING), 2 (READY, newest)]. Tail
        # suppression shelters 2; the PROVISIONING one is retired.
        self.assertEqual([d.target for d in _downs(decisions)], [3])


class TestLocationFromResourcesOverride(unittest.TestCase):
    """Re-driven pinned launches must recover their location."""

    def test_roundtrip_from_inlined_to_dict(self):
        location = spot_placer.Location.from_pickleable(_K8S_KEY)
        override = {'some_user_key': 1}
        override.update(location.to_dict())
        rebuilt = spot_placer.Location.from_resources_override(override)
        self.assertIsNotNone(rebuilt)
        assert rebuilt is not None
        self.assertEqual(rebuilt.region, location.region)
        self.assertEqual(rebuilt.accelerators, location.accelerators)
        self.assertIs(rebuilt.use_spot, False)

    def test_non_pinned_override_returns_none(self):
        self.assertIsNone(spot_placer.Location.from_resources_override(None))
        self.assertIsNone(spot_placer.Location.from_resources_override({}))
        self.assertIsNone(
            spot_placer.Location.from_resources_override({'use_spot': True}))


class TestPostSnapshotZeroCostDebit(unittest.TestCase):
    """Zero-cost rows created after the snapshot occupy free slots.

    A DEMAND launch placed on the zero-cost tier that binds AND turns
    READY within one inter-poll gap escapes the not-READY debit, yet the
    slot it sits on was counted free when the snapshot was taken: it
    must be subtracted from the spendable level regardless of readiness.
    """

    def test_ready_within_gap_debits_slot(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        ts = time.time()
        _feed(autoscaler, 3, timestamp=ts)
        # Demand launch landed zero-cost and turned READY after the
        # snapshot, before the next poll.
        replicas = [_replica(1, _K8S_KEY, created_at=ts + 1)]
        decisions = _decisions(autoscaler, replicas)
        # Spendable 3 - 1 occupied = 2 fill ups (not 3).
        self.assertEqual(len(_fill_ups(decisions)), 2)
        self.assertEqual(len(_ups(decisions)), 2)
        self.assertEqual(len(_downs(decisions)), 0)

    def test_pre_snapshot_ready_row_not_debited(self):
        # A READY row older than the snapshot has a bound pod the poll
        # already excluded: debiting it again would double-subtract.
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        ts = time.time()
        _feed(autoscaler, 3, timestamp=ts)
        replicas = [_replica(1, _K8S_KEY, created_at=ts - 100)]
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(len(_fill_ups(decisions)), 3)

    def test_row_without_created_at_treated_as_pre_snapshot(self):
        # Pre-upgrade pickled rows carry created_at=None: always-debiting
        # them would under-fill for their whole lifetime.
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        _feed(autoscaler, 3)
        replicas = [_replica(1, _K8S_KEY, created_at=None)]
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(len(_fill_ups(decisions)), 3)

    def test_not_ready_and_post_snapshot_debited_once(self):
        # A row that is BOTH not READY and created after the snapshot
        # matches both clauses of the debit rule but occupies one slot:
        # it must be subtracted exactly once.
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        ts = time.time()
        _feed(autoscaler, 3, timestamp=ts)
        replicas = [
            _replica(1,
                     _K8S_KEY,
                     status=serve_state.ReplicaStatus.PROVISIONING,
                     created_at=ts + 1)
        ]
        decisions = _decisions(autoscaler, replicas)
        # Spendable 3 - 1 = 2 (double subtraction would leave 1).
        self.assertEqual(len(_fill_ups(decisions)), 2)


class TestBootSeeding(unittest.TestCase):
    """Respawn: a seeded location set protects the fill fleet at tick 0.

    A respawned controller's autoscaler starts with empty fill state and
    can tick before the first (slow) poll; seeding only the zero-cost
    location set makes suppression work immediately while granting no
    free slots.
    """

    def _fleet(self):
        return [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            # Paid replica with the highest id: first demand victim.
            _replica(3),
        ]

    def test_seeded_fresh_autoscaler_shelters_fill_on_first_tick(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        autoscaler.seed_zero_cost_locations([_K8S_KEY])
        # First tick, before any collect_reserved_capacity: the paid
        # down passes, the zero-cost victim is sheltered.
        decisions = _decisions(autoscaler, self._fleet())
        self.assertEqual([d.target for d in _downs(decisions)], [3])
        # No free slots granted by seeding: no fill launches either.
        self.assertEqual(len(_ups(decisions)), 0)
        self.assertIsNone(autoscaler._fill_snapshot_time)
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)

    def test_unseeded_fresh_autoscaler_terminates_fleet(self):
        # The failure mode the seed prevents: with an empty location set
        # every zero-cost victim is fair game on the first tick.
        autoscaler = _make_autoscaler(min_replicas=1)
        decisions = _decisions(autoscaler, self._fleet())
        self.assertEqual(len(_downs(decisions)), 2)

    def test_seed_never_overwrites_loaded_locations(self):
        old = _make_autoscaler()
        _feed(old, 3)
        fresh = _make_autoscaler()
        fresh.load_dynamic_states(old.dump_dynamic_states())
        fresh.seed_zero_cost_locations([dict(_K8S_KEY, region='other-ctx')])
        self.assertEqual(fresh._fill_zero_cost_locations,
                         [spot_placer.Location.from_pickleable(_K8S_KEY)])


class TestControllerSeeding(unittest.TestCase):
    """Controller-side seeding: boot / update_service swap wiring."""

    def _make_controller(self, autoscaler, placer):
        # pylint: disable=import-outside-toplevel
        from sky.serve import controller as controller_lib
        ctrl = controller_lib.SkyServeController.__new__(
            controller_lib.SkyServeController)
        ctrl._autoscaler = autoscaler
        ctrl._replica_manager = types.SimpleNamespace(spot_placer=placer)
        return ctrl

    def _placer(self):
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [
            spot_placer.Location.from_pickleable(_K8S_KEY)
        ]
        return placer

    def test_swap_without_fill_state_gets_seeded(self):
        # update_service swap where the old autoscaler's dump carried no
        # fill state (build predating the feature): the replacement must
        # still shelter the fill fleet on its first tick.
        new = _make_autoscaler(min_replicas=1)
        new.load_dynamic_states({
            'latest_version_ever_ready': 1,
            'request_timestamps': [],
        })
        ctrl = self._make_controller(new, self._placer())
        ctrl._seed_fill_zero_cost_locations(new)
        replicas = [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            _replica(3),
        ]
        decisions = _decisions(new, replicas)
        self.assertEqual([d.target for d in _downs(decisions)], [3])
        self.assertIsNone(new._fill_snapshot_time)

    def test_flag_off_does_not_seed(self):
        autoscaler = _make_autoscaler(fill=False)
        placer = self._placer()
        ctrl = self._make_controller(autoscaler, placer)
        ctrl._seed_fill_zero_cost_locations(autoscaler)
        self.assertEqual(autoscaler._fill_zero_cost_locations, [])
        placer.zero_cost_locations.assert_not_called()

    def test_no_placer_does_not_seed(self):
        autoscaler = _make_autoscaler()
        ctrl = self._make_controller(autoscaler, placer=None)
        ctrl._seed_fill_zero_cost_locations(autoscaler)
        self.assertEqual(autoscaler._fill_zero_cost_locations, [])

    def test_seed_failure_is_swallowed_and_leaves_unseeded(self):
        # The cost path behind zero_cost_locations() can hit the live
        # Kubernetes API; a blip there must not propagate out of the
        # seed (it runs inside controller __init__), only degrade to
        # the pre-seed behavior.
        autoscaler = _make_autoscaler()
        placer = mock.Mock()
        placer.zero_cost_locations.side_effect = ValueError('api blip')
        ctrl = self._make_controller(autoscaler, placer)
        ctrl._seed_fill_zero_cost_locations(autoscaler)  # must not raise
        self.assertEqual(autoscaler._fill_zero_cost_locations, [])


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

    def test_snapshot_time_predates_the_query(self):
        # A zero-cost row created WHILE the (slow) availability query
        # runs occupies a slot the query may still count free; the
        # post-snapshot debit (created_at > snapshot_time) only catches
        # it if the snapshot timestamp predates the query, not follows
        # it.
        autoscaler = _make_autoscaler(fill=True)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        pre_query_time = None

        def _slow_query(zero_cost):
            del zero_cost
            nonlocal pre_query_time
            pre_query_time = time.time()
            return 0

        with mock.patch.object(reserved_capacity,
                               'query_free_slots',
                               side_effect=_slow_query), \
             mock.patch.object(reserved_capacity.time,
                               'sleep',
                               side_effect=self._Stop):
            with self.assertRaises(self._Stop):
                reserved_capacity.poller_loop(lambda: autoscaler,
                                              lambda: placer)
        self.assertIsNotNone(autoscaler._fill_snapshot_time)
        self.assertLessEqual(autoscaler._fill_snapshot_time, pre_query_time)


class TestPollerClaimLifecycle(unittest.TestCase):
    """Disabling fill (or losing the placer) withdraws the broker claim
    immediately -- a disabled service must not keep absorbing entitlement
    until its claim TTL expires."""

    class _Stop(Exception):
        pass

    def test_disable_transition_removes_claim_once(self):
        autoscaler = _make_autoscaler(fill=True)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        cycles = {'n': 0}

        def _sleep(_seconds):
            cycles['n'] += 1
            if cycles['n'] == 1:
                # An update turns the flag off on the live autoscaler.
                autoscaler.reserved_capacity_fill = False
            if cycles['n'] >= 3:
                raise self._Stop()

        with mock.patch.object(reserved_capacity,
                               '_broker_cycle') as broker_cycle, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'remove_claim') as remove_claim, \
             mock.patch.object(reserved_capacity.time,
                               'sleep',
                               side_effect=_sleep):
            with self.assertRaises(self._Stop):
                reserved_capacity.poller_loop(lambda: autoscaler,
                                              lambda: placer,
                                              service_name='svc')
        broker_cycle.assert_called_once()
        # Withdrawn exactly once on the disable transition, not re-spammed
        # on every subsequent disabled cycle.
        remove_claim.assert_called_once_with('svc')


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
        # The persisted row carries its creation time from the start
        # (PROVISIONING included): the fill overlay's post-snapshot debit
        # relies on it.
        self.assertIsNotNone(info.created_at)

    def test_redriven_pinned_launch_keeps_location(self):
        # Controller crash mid-PENDING: the launch is re-driven with the
        # persisted override (location inlined, sentinel already
        # stripped, use_spot=False). The placer selection path is
        # skipped, but the upserted row must keep the pinned location --
        # location=None would permanently drop the replica from fill
        # accounting and the placer's load counter.
        location = spot_placer.Location.from_pickleable(_K8S_KEY)
        placer = mock.Mock()
        manager = self._make_manager(placer)
        persisted_override = dict(location.to_dict())
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            launched = manager._launch_replica(7, persisted_override)
        self.assertTrue(launched)
        placer.select_next_location.assert_not_called()
        placer.select_next_zero_cost_location.assert_not_called()
        info = add_mock.call_args[0][2]
        recovered = info.get_spot_location()
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered.region, 'research-ctx')
        self.assertIs(info.is_spot, False)

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

    def test_same_shape_different_counts_use_largest_deterministically(self):
        # A100:1 and A100:8 entries over one pool draw from the same
        # free GPUs: the LARGEST per-replica size wins regardless of
        # any_of entry order (first-seen-wins would let YAML order
        # change the fill level).
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 8
                               })):
            for locations in ([
                    self._k8s_location(count=1),
                    self._k8s_location(count=8)
            ], [self._k8s_location(count=8),
                    self._k8s_location(count=1)]):
                self.assertEqual(reserved_capacity.query_free_slots(locations),
                                 1)

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


class TestCostFeasibilityDegradation(unittest.TestCase):
    """Empty feasible list degrades to inf cost, never a boot crash."""

    def test_empty_feasible_list_is_infinite_not_raise(self):
        placer = spot_placer.DynamicFallbackSpotPlacer.__new__(
            spot_placer.DynamicFallbackSpotPlacer)
        placer.location2cost = {}
        placer.num_nodes = 1
        placer.resources = mock.Mock()
        empty = mock.Mock()
        empty.resources_list = []
        copied = mock.Mock()
        copied.cloud.get_feasible_launchable_resources.return_value = empty
        placer.resources.copy.return_value = copied
        location = _make_location('research-ctx', 'free')
        cost = placer._get_cost_per_hour_cached(location)
        self.assertEqual(cost, float('inf'))
        # Not memoized: a transient K8s API blip heals on the next call.
        self.assertEqual(placer.location2cost, {})


def _feed_broker(autoscaler,
                 free_slots,
                 grant,
                 epoch=None,
                 pool_key=None,
                 polls=2):
    """Feed a broker-shaped snapshot (feed + grant + epoch + pool key)."""
    ts = time.time()
    for _ in range(polls):
        autoscaler.collect_reserved_capacity(free_slots, [_K8S_KEY],
                                             ts,
                                             grant=grant,
                                             grant_epoch=epoch,
                                             grant_pool_key=pool_key)


class TestGrantCeiling(unittest.TestCase):
    """Broker grant ceiling: caps fill, strips over-grant shelter."""

    def test_grant_none_is_identity(self):
        with_none = _make_autoscaler(min_replicas=1)
        _feed_broker(with_none, 5, grant=None)
        control = _make_autoscaler(min_replicas=1)
        _feed(control, 5)
        got = _decisions(with_none, [])
        expected = _decisions(control, [])
        self.assertEqual([(d.operator, d.target) for d in got],
                         [(d.operator, d.target) for d in expected])

    def test_ceiling_caps_fill_launches(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        _feed_broker(autoscaler, 5, grant=2)
        decisions = _decisions(autoscaler, [])
        sentinel = [d for d in _ups(decisions) if d.target is not None]
        # Without the ceiling the feed of 5 would fund 4 fill ups (demand
        # target 1); the grant of 2 allows only 2 - 1 = 1 beyond the
        # demand baseline.
        self.assertEqual(len(sentinel), 1)

    def test_snap_back_reclaim_shrinks_borrower(self):
        # The load-bearing broker actuator: the #108 fill target is
        # structurally >= holdings, so only the ceiling can strip the
        # borrower's surplus of its scale-down shelter and let the normal
        # graceful scale-down return the machines.
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(i, _K8S_KEY) for i in range(1, 5)]
        _feed_broker(autoscaler, 0, grant=2)
        decisions = _decisions(autoscaler, replicas)
        # Demand target 1, holdings 4, ceiling 2: fill shelters only ONE
        # of the three demand scale-downs; two pass through and the
        # borrower actually shrinks toward its grant.
        self.assertEqual(len(_downs(decisions)), 2)
        self.assertEqual(len(_ups(decisions)), 0)
        self.assertEqual(autoscaler.info()['fill_target'], 2)

    def test_demand_placed_rows_exempt_from_ceiling(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        fill_rows = [_replica(1, _K8S_KEY), _replica(2, _K8S_KEY)]
        demand_rows = [_replica(3, _K8S_KEY), _replica(4, _K8S_KEY)]
        for row in demand_rows:
            row.reserved_fill = False
        _feed_broker(autoscaler, 0, grant=0)
        decisions = _decisions(autoscaler, fill_rows + demand_rows)
        # Ceiling = grant 0 + 2 demand-placed rows: the demand rows are
        # demand-protected (not broker property), only the two FILL rows
        # are over-ceiling and lose their shelter.
        self.assertEqual(autoscaler.info()['fill_target'], 2)
        self.assertEqual(len(_downs(decisions)), 2)

    def test_old_demand_rows_do_not_inflate_launch_ceiling(self):
        # Rolling update: 3 OLD-version demand-placed zero-cost rows are
        # still draining. The launch-side ceiling must count only
        # LATEST-version demand-placed rows (here 0), so fill launches
        # stay capped at the grant: ceiling 2, demand baseline 1 ->
        # exactly 1 fill up. An all-version launch ceiling (2 + 3 old
        # rows) would fund 4 fill ups, overshooting the grant by the old
        # rows' count.
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=1,
                                                             max_replicas=20),
                                                       version=2)
        _feed_broker(autoscaler, 5, grant=2)
        old_demand = [_replica(i, _K8S_KEY, version=1) for i in range(1, 4)]
        for row in old_demand:
            row.reserved_fill = False
        decisions = autoscaler.generate_scaling_decisions(old_demand, [1])
        self.assertEqual(len(_fill_ups(decisions)), 1)
        # The target/shelter-side ceiling keeps the all-version count
        # (grant 2 + 3 demand-placed rows): existing rows keep their
        # exemption regardless of version.
        self.assertEqual(autoscaler.info()['fill_target'], 5)

    def test_stale_broker_snapshot_still_shelters_holdings(self):
        # Ceiling + staleness compose: a dead poller decays the feed to 0
        # but holdings at-or-under the ceiling keep their shelter.
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(1, _K8S_KEY), _replica(2, _K8S_KEY)]
        ts = _stale_timestamp()
        autoscaler.collect_reserved_capacity(5, [_K8S_KEY],
                                             ts,
                                             grant=2,
                                             grant_epoch=1)
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(decisions, [])


class TestGrantEpochPlumbing(unittest.TestCase):
    """Fill scale-ups carry the grant epoch only when a broker supplied it."""

    def test_epoch_and_pool_key_attached_to_sentinel_override(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        pool = reserved_capacity_broker.make_pool_key('research-ctx', 'a100')
        _feed_broker(autoscaler, 3, grant=5, epoch=7, pool_key=pool)
        sentinel = [
            d for d in _ups(_decisions(autoscaler, [])) if d.target is not None
        ]
        self.assertTrue(sentinel)
        for decision in sentinel:
            self.assertEqual(decision.target, {
                _FILL_KEY: True,
                _EPOCH_KEY: 7,
                _POOL_KEY: pool
            })

    def test_no_epoch_means_pre_broker_decision_shape(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        _feed(autoscaler, 3)
        sentinel = [
            d for d in _ups(_decisions(autoscaler, [])) if d.target is not None
        ]
        self.assertTrue(sentinel)
        for decision in sentinel:
            self.assertEqual(decision.target, {_FILL_KEY: True})

    def test_grant_not_persisted_in_dynamic_state_dump(self):
        # Grants are DB-authoritative: a swapped-in autoscaler must get
        # them from the next poll, never from a stale dump.
        autoscaler = _make_autoscaler(min_replicas=1)
        _feed_broker(autoscaler, 3, grant=5, epoch=7, pool_key='pool')
        dump = autoscaler.dump_dynamic_states()
        fill_state = dump['reserved_capacity_fill_state']
        self.assertNotIn('fill_grant', fill_state)
        self.assertNotIn('fill_grant_epoch', fill_state)
        self.assertNotIn('fill_grant_pool_key', fill_state)


class TestEpochFencedLaunch(unittest.TestCase):
    """A fill launch carrying a superseded epoch skips, leaking nothing."""

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
        manager.latest_version = 1
        return manager

    def _launch(self, pool_epochs, carried_epoch=7, carried_pool='pool-b'):
        """pool_epochs: pool_key -> current round epoch (the fence read)."""
        location = _make_location('research-ctx', 'free')
        placer = mock.Mock()
        placer.select_next_zero_cost_location.return_value = location
        manager = self._make_manager(placer)
        override = {_FILL_KEY: True, _EPOCH_KEY: carried_epoch}
        if carried_pool is not None:
            override[_POOL_KEY] = carried_pool
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.reserved_capacity_broker,
                               'current_epoch',
                               side_effect=pool_epochs.get) as epoch_mock, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            launched = manager._launch_replica(7, override)
        return launched, add_mock, epoch_mock

    def test_stale_epoch_skips_without_persisting_a_row(self):
        launched, add_mock, epoch_mock = self._launch({'pool-b': 8})
        self.assertFalse(launched)
        add_mock.assert_not_called()
        # The fence reads the CARRIED pool's round epoch, not a global one.
        epoch_mock.assert_called_once_with('pool-b')

    def test_current_epoch_launches_and_strips_the_keys(self):
        launched, add_mock, _ = self._launch({'pool-b': 7})
        self.assertTrue(launched)
        add_mock.assert_called_once()
        info = add_mock.call_args[0][2]
        self.assertNotIn(_EPOCH_KEY, info.resources_override)
        self.assertNotIn(_POOL_KEY, info.resources_override)
        self.assertNotIn(_FILL_KEY, info.resources_override)
        self.assertTrue(info.reserved_fill)

    def test_peer_pool_epoch_bump_does_not_fence(self):
        # Cross-pool isolation at the fence: pool A's epoch moved (8) but
        # this launch carries pool B's still-current epoch (7) -- it must
        # launch. Pool B's own stale epoch (6 vs 7) still fences.
        launched, add_mock, _ = self._launch({'pool-a': 8, 'pool-b': 7})
        self.assertTrue(launched)
        add_mock.assert_called_once()
        fenced, fenced_add, _ = self._launch({
            'pool-a': 8,
            'pool-b': 7
        },
                                             carried_epoch=6)
        self.assertFalse(fenced)
        fenced_add.assert_not_called()

    def test_missing_round_fails_open(self):
        # No round row for the pool (current_epoch None): there is no
        # newer allocation to defer to -- proceed rather than deadlock
        # fill forever.
        launched, add_mock, _ = self._launch({})
        self.assertTrue(launched)
        add_mock.assert_called_once()

    def test_epoch_without_pool_key_fails_open(self):
        # Defensive: the epoch is only meaningful against its pool's
        # round; a sentinel missing the pool key (never emitted by the
        # autoscaler) must not fence.
        launched, add_mock, epoch_mock = self._launch({'pool-b': 8},
                                                      carried_pool=None)
        self.assertTrue(launched)
        add_mock.assert_called_once()
        epoch_mock.assert_not_called()


class TestDemandPlacementGate(unittest.TestCase):
    """Zero-cost holdings >= grant: NEW demand launches skip the free tier."""

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
        manager.latest_version = 1
        return manager

    def _launch_demand(self, grant, replicas):
        zero_cost_location = spot_placer.Location.from_pickleable(_K8S_KEY)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [zero_cost_location]
        selected = _make_location('us-east-1', 'paid', use_spot=True)
        placer.select_next_location.return_value = selected
        manager = self._make_manager(placer)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=True), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.reserved_capacity_broker,
                               'get_cached_grant',
                               return_value=grant), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=replicas), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica'):
            manager._launch_replica(9)
        return placer

    def test_holdings_at_grant_skip_zero_cost_preference(self):
        placer = self._launch_demand(1, [_replica(1, _K8S_KEY)])
        placer.select_next_location.assert_called_once()
        self.assertTrue(
            placer.select_next_location.call_args.kwargs.get(
                'skip_zero_cost_preference'))

    def test_holdings_under_grant_keep_preference(self):
        placer = self._launch_demand(5, [_replica(1, _K8S_KEY)])
        placer.select_next_location.assert_called_once()
        self.assertEqual(placer.select_next_location.call_args.kwargs, {})

    def test_no_grant_is_inert(self):
        # Both "no cached entry" and the single-claimant fast path's None
        # grant surface as None from the grant cache: the gate stays inert.
        placer = self._launch_demand(None, [_replica(1, _K8S_KEY)])
        self.assertEqual(placer.select_next_location.call_args.kwargs, {})


class TestPlacerSkipZeroCostPreference(unittest.TestCase):
    """skip_zero_cost_preference excludes the free tier while paid exists."""

    def setUp(self):
        self.k8s = _make_location('research-ctx', 'free')
        self.paid = _make_location('us-east-1', 'paid', use_spot=True)
        self.placer = _make_placer({self.k8s: 0.0, self.paid: 0.2})

    def test_default_prefers_loaded_zero_cost(self):
        selected = self.placer.select_next_location([self.k8s, self.k8s])
        self.assertEqual(selected, self.k8s)

    def test_skip_lets_load_steer_to_paid(self):
        selected = self.placer.select_next_location(
            [self.k8s, self.k8s], skip_zero_cost_preference=True)
        self.assertEqual(selected, self.paid)

    def test_skip_excludes_zero_cost_on_load_tie(self):
        # Fleetless service: every candidate ties at load 0, and the
        # min-cost tiebreak would pick the free tier — the gate must
        # exclude it, not merely stop preferring it, or tied demand
        # launches squat a peer's entitlement one by one.
        selected = self.placer.select_next_location(
            [], skip_zero_cost_preference=True)
        self.assertEqual(selected, self.paid)
        # Equal nonzero load on both tiers: same rule.
        selected = self.placer.select_next_location(
            [self.k8s, self.k8s, self.paid, self.paid],
            skip_zero_cost_preference=True)
        self.assertEqual(selected, self.paid)

    def test_skip_with_zero_cost_only_set_still_serves(self):
        # The gate throttles placement preference, never availability: a
        # zero-cost-only candidate set must still yield a location.
        placer = _make_placer({self.k8s: 0.0})
        selected = placer.select_next_location([],
                                               skip_zero_cost_preference=True)
        self.assertEqual(selected, self.k8s)


class TestBrokerPollerCycle(unittest.TestCase):
    """The broker cycle claims, reads the round, and feeds grant + epoch."""

    def _run_cycle(self, allocation, zero_cost=None, replica_infos=()):
        autoscaler = _make_autoscaler(min_replicas=1)
        if zero_cost is None:
            zero_cost = [spot_placer.Location.from_pickleable(_K8S_KEY)]
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = zero_cost
        placer.active_locations.return_value = list(zero_cost)
        keys = [location.to_pickleable() for location in zero_cost]
        with mock.patch.object(reserved_capacity.serve_state,
                               'get_replica_infos',
                               return_value=list(replica_infos)), \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'upsert_claim') as upsert_mock, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'remove_claim') as remove_mock, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'run_round_if_stale',
                               return_value=allocation) as round_mock:
            reserved_capacity._broker_cycle(autoscaler, placer, 'svc',
                                            zero_cost, keys)
        return autoscaler, upsert_mock, remove_mock, round_mock

    def test_unseeded_autoscaler_still_heartbeats_holdings(self):
        # Respawn path whose best-effort boot seed failed: the cycle must
        # seed the location set itself before counting, or the heartbeat
        # under-reports holdings as 0 and the broker reads a holdings
        # SHRINK -- cutting peers' grants past the down-damping on a pure
        # reporting artifact.
        holder = _replica(1, _K8S_KEY)
        holder.reserved_fill = True
        allocation = reserved_capacity_broker.Allocation(grant=3,
                                                         feed=0,
                                                         round_id=1,
                                                         epoch=1,
                                                         snapshot_time=1.0)
        _, upsert_mock, _, _ = self._run_cycle(allocation,
                                               replica_infos=[holder])
        self.assertEqual(upsert_mock.call_args.kwargs['holdings_fill'], 1)

    def test_allocation_feeds_grant_and_epoch(self):
        allocation = reserved_capacity_broker.Allocation(grant=3,
                                                         feed=2,
                                                         round_id=1,
                                                         epoch=4,
                                                         snapshot_time=1.0)
        autoscaler, upsert_mock, _, round_mock = self._run_cycle(allocation)
        upsert_mock.assert_called_once()
        self.assertEqual(
            upsert_mock.call_args.kwargs['pool_key'],
            reserved_capacity_broker.make_pool_key('research-ctx', 'a100'))
        round_mock.assert_called_once()
        self.assertEqual(autoscaler._fill_grant, 3)
        self.assertEqual(autoscaler._fill_grant_epoch, 4)
        self.assertEqual(
            autoscaler._fill_grant_pool_key,
            reserved_capacity_broker.make_pool_key('research-ctx', 'a100'))
        self.assertEqual(autoscaler._fill_snapshot_time, 1.0)

    def test_no_allocation_feeds_zero_without_grant(self):
        autoscaler, _, _, _ = self._run_cycle(allocation=None)
        self.assertIsNone(autoscaler._fill_grant)
        self.assertIsNotNone(autoscaler._fill_snapshot_time)
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)

    def test_multi_pool_service_withdraws_claim_and_feeds_zero(self):
        other = dict(_K8S_KEY, accelerators={'H100': 1})
        zero_cost = [
            spot_placer.Location.from_pickleable(_K8S_KEY),
            spot_placer.Location.from_pickleable(other),
        ]
        autoscaler, upsert_mock, remove_mock, round_mock = self._run_cycle(
            allocation=None, zero_cost=zero_cost)
        remove_mock.assert_called_once_with('svc')
        upsert_mock.assert_not_called()
        round_mock.assert_not_called()
        self.assertIsNone(autoscaler._fill_grant)
        # The location set is still seeded: existing holdings keep their
        # scale-down shelter even while fill is inactive.
        self.assertIsNotNone(autoscaler._fill_snapshot_time)


class TestReplicaManagerInitIntact(unittest.TestCase):
    """Real __init__ must fully run (fill fixtures bypass it via __new__).

    Pins the regression class where an addition spliced into the middle
    of ReplicaManager.__init__ silently truncated it: the suite passed
    while least_recent_version was never set and the version-cleanup
    refresher AttributeError'd every cycle.
    """

    def test_base_init_sets_version_fields_and_placer_accessor(self):
        spec = types.SimpleNamespace(pool=False,
                                     readiness_headers=None,
                                     readiness_path='/health',
                                     initial_delay_seconds=60,
                                     endpoint_probe_interval_seconds=10,
                                     post_data=None)
        manager = replica_managers.ReplicaManager('svc', spec, version=3)
        self.assertEqual(manager.latest_version, 3)
        self.assertEqual(manager.least_recent_version, 3)
        self.assertIsNone(manager.spot_placer)


if __name__ == '__main__':
    unittest.main()
