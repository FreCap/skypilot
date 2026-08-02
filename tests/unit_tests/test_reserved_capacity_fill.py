"""Unit tests for the reserved-capacity fill overlay.

Opt-in (replica_policy.reserved_capacity_fill): the autoscaler additionally
scales up onto FREE zero-cost capacity reported by a controller poller,
bounded by max_replicas. The demand target and the controller's capacity
hint stay demand-only; every free-slot scale-up carries a sentinel override
that the launch path pins to zero-cost ACTIVE locations (skipping entirely
when none is available -- fill must never spill to paid capacity).
"""
# pylint: disable=protected-access
import time
import types
import unittest
from unittest import mock

from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer as _make_placer

from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import service_spec
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
    """Reserved fill carries ONLY the sentinel; demand ups do not."""

    def test_surplus_ups_sentinel_demand_ups_plain(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        _feed(autoscaler, 3)
        decisions = _decisions(autoscaler, [])
        ups = _ups(decisions)
        self.assertEqual(len(ups), 4)  # 1 demand (to min) + all 3 free slots
        plain = [d for d in ups if d.target is None]
        sentinel = [d for d in ups if d.target == {_FILL_KEY: True}]
        self.assertEqual(len(plain), 1)
        self.assertEqual(len(sentinel), 3)
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


class TestLogicalReplicaFill(unittest.TestCase):
    """Logical fleets account one-GPU fill candidates in slot units."""

    def test_paid_backend_width_is_counted_in_logical_units(self):
        spec = service_spec.SkyServiceSpec(
            readiness_path='/health',
            initial_delay_seconds=60,
            readiness_timeout_seconds=30,
            endpoint_probe_interval_seconds=10,
            lb_stream_timeout_seconds=60,
            min_replicas=1,
            max_replicas=10,
            target_concurrency_per_replica=1,
            graceful_drain_async_occupancy=True,
            spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
            reserved_capacity_fill=True)
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc', spec)
        autoscaler.seed_zero_cost_locations([_K8S_KEY])
        _feed(autoscaler, 5)
        paid_location = make_location('us-east-1',
                                      accelerators={'L4': 4},
                                      cloud_name='AWS')
        paid = _replica(1, paid_location.to_pickleable())
        paid.planned_capacity = 4

        fill_ups = _ups(autoscaler._apply_reserved_capacity_fill([paid], []))

        self.assertEqual(len(fill_ups), 5)
        self.assertTrue(all(up.target[_FILL_KEY] for up in fill_ups))


class TestDemandIndependentFill(unittest.TestCase):
    """Fresh reserved slots launch regardless of demand-target ordering."""

    def test_paid_fleet_satisfying_larger_demand_does_not_block_fill(self):
        autoscaler = _make_autoscaler(min_replicas=5, max_replicas=10)
        paid_replicas = [_replica(replica_id) for replica_id in range(1, 6)]
        _feed(autoscaler, 2)

        decisions = _decisions(autoscaler, paid_replicas)

        self.assertEqual(len(_fill_ups(decisions)), 2)
        self.assertEqual(len(_downs(decisions)), 0)
        self.assertEqual(autoscaler.get_final_target_num_replicas(), 5)
        self.assertEqual(autoscaler.info()['fill_target'], 2)

    def test_planned_demand_reserves_hard_ceiling_headroom(self):
        autoscaler = _make_autoscaler(min_replicas=9, max_replicas=10)
        paid_replicas = [_replica(replica_id) for replica_id in range(1, 9)]
        _feed(autoscaler, 5)

        decisions = _decisions(autoscaler, paid_replicas)

        plain_ups = [
            decision for decision in _ups(decisions) if decision.target is None
        ]
        self.assertEqual(len(plain_ups), 1)
        self.assertEqual(len(_fill_ups(decisions)), 1)

    def test_old_versions_count_against_hard_ceiling(self):
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=2,
                                                             max_replicas=6),
                                                       version=2)
        replicas = [
            _replica(1, version=1),
            _replica(2, version=1),
            _replica(3, version=1),
            _replica(4, version=2),
        ]
        _feed(autoscaler, 5)
        demand_up = autoscalers.AutoscalerDecision(_SCALE_UP, None)
        ordinary_decisions = [demand_up]

        decisions = autoscaler._apply_reserved_capacity_fill(
            replicas, ordinary_decisions)

        self.assertEqual(len(_fill_ups(decisions)), 1)
        self.assertEqual(len(_ups(decisions)), 2)
        self.assertEqual(ordinary_decisions, [demand_up])

    def test_at_hard_ceiling_waits_for_headroom(self):
        autoscaler = _make_autoscaler(min_replicas=5, max_replicas=10)
        paid_replicas = [_replica(replica_id) for replica_id in range(1, 11)]
        _feed(autoscaler, 3)

        decisions = autoscaler._apply_reserved_capacity_fill(paid_replicas, [])

        self.assertEqual(_fill_ups(decisions), [])


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
        replicas: list = []
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

    The zero-cost count feeding the launch target is latest-version-only;
    otherwise a rolling update's draining old fleet compounds fill launches
    every tick. All old-version rows still count against hard-ceiling headroom.
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
        # Latest zero-cost 0 + 3 free slots: all three slots launch,
        # independently of the demand launch, and are NOT inflated to
        # 5 old + 3 free by the draining old-version fleet.
        self.assertEqual(len(_fill_ups(decisions)), 3)


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
        # Spendable 3 - 1 (old-version pending claim) = 2;
        # fill_target_launch = 0 latest zero-cost + 2 spendable. Both truly
        # free slots launch independently of the demand target (a latest-only
        # occupancy debit would emit 3, one onto the claimed slot).
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
        # A malformed or incomplete placer during a rolling upgrade must not
        # propagate out of the seed (it runs inside controller __init__).
        autoscaler = _make_autoscaler()
        placer = mock.Mock()
        placer.zero_cost_locations.side_effect = ValueError(
            'catalog unavailable')
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

    def test_cycle_failure_after_enable_still_withdraws_on_disable(self):
        # A broker cycle can die AFTER upserting its claim (e.g. the
        # round query raises), so the claim-may-exist flag must be set
        # BEFORE the cycle runs -- otherwise a subsequent disable would
        # skip remove_claim and leave a ghost claim absorbing entitlement
        # for the whole claim TTL.
        autoscaler = _make_autoscaler(fill=False)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        cycles = {'n': 0}

        def _sleep(_seconds):
            cycles['n'] += 1
            if cycles['n'] == 1:
                # The first disabled cycle consumed the initial flag; an
                # update now re-enables fill.
                autoscaler.reserved_capacity_fill = True
            elif cycles['n'] == 2:
                # The (failed) enabled cycle ran; disable again.
                autoscaler.reserved_capacity_fill = False
            elif cycles['n'] >= 3:
                raise self._Stop()

        with mock.patch.object(
                reserved_capacity,
                '_broker_cycle',
                side_effect=RuntimeError('cycle died')) as broker_cycle, \
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
        # Once for the initial disabled observation, and AGAIN after the
        # failed enabled cycle: the possibly-upserted claim is withdrawn
        # instead of ghosting until the TTL.
        self.assertEqual(remove_claim.call_count, 2)
        remove_claim.assert_called_with('svc')


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
    return make_location(region, accelerators={'A100': 1}, use_spot=use_spot)


def _make_manager(placer):
    """Bare SkyPilotReplicaManager wired for the launch-path tests."""
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


class TestZeroCostSelection(unittest.TestCase):
    """select_next_zero_cost_location: zero-cost ACTIVE or nothing."""

    def setUp(self):
        self.k8s = _make_location('research-ctx', 'free')
        self.paid = _make_location('us-east-1', 'paid', use_spot=True)
        self.placer = _make_placer({self.k8s: 0.0, self.paid: 0.2})

    def test_returns_active_zero_cost(self):
        self.assertEqual(self.placer.select_next_zero_cost_location(), self.k8s)

    def test_benched_zero_cost_returns_none_never_paid(self):
        with mock.patch.object(spot_placer.time, 'time', return_value=1000.0):
            self.placer.set_preemptive(self.k8s)
            self.assertIsNone(self.placer.select_next_zero_cost_location())

    def test_no_zero_cost_at_all_returns_none(self):
        placer = _make_placer({self.paid: 0.2})
        self.assertIsNone(placer.select_next_zero_cost_location())

    def test_enumeration_includes_benched(self):
        with mock.patch.object(spot_placer.time, 'time', return_value=1000.0):
            self.placer.set_preemptive(self.k8s)
            self.assertIn(self.k8s, self.placer.zero_cost_locations())

    def test_catalog_enumeration_ignores_unknown_paid_candidates(self):
        self.placer.location2cost.pop(self.paid)
        self.placer.location2status.update({
            _make_location(f'paid-region-{index}', 'paid', use_spot=True):
                spot_placer.LocationStatus.ACTIVE for index in range(1058)
        })
        self.assertEqual(self.placer.zero_cost_locations(), [self.k8s])

    def test_equal_cost_reuses_first_candidate(self):
        other = _make_location('research-ctx-2', 'free')
        placer = _make_placer({self.k8s: 0.0, other: 0.0, self.paid: 0.2})
        selected = placer.select_next_zero_cost_location()
        self.assertEqual(selected, self.k8s)


class TestFillLaunchPath(unittest.TestCase):
    """Sentinel launches pin zero-cost-only; aborts leak nothing."""

    def test_abort_creates_no_record_and_keeps_id(self):
        placer = mock.Mock()
        placer.select_next_zero_cost_location.return_value = None
        manager = _make_manager(placer)
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
                               'add_or_update_replica') as add_mock:
            manager._scale_up_one_locked(override, set())
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
        manager = _make_manager(placer)
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
                               'add_or_update_replica') as add_mock:
            manager._scale_up_one_locked({_FILL_KEY: True}, set())
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

    def test_targeted_fill_keeps_a100_variants_exact(self):
        a100 = make_location('research-ctx',
                             accelerators={'A100': 1},
                             use_spot=False)
        a100_80gb = make_location('research-ctx',
                                  accelerators={'A100-80GB': 1},
                                  use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [a100, a100_80gb]
        placer.select_next_zero_cost_location.return_value = a100_80gb
        manager = _make_manager(placer)
        override = {
            _FILL_KEY: True,
            'accelerators': {
                'A100-80GB': 1
            },
        }

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
                               'add_or_update_replica') as add_mock:
            manager._scale_up_one_locked(override, set())

        placer.select_next_zero_cost_location.assert_called_once_with(
            allowed_locations={a100_80gb})
        info = add_mock.call_args[0][2]
        self.assertEqual(info.resources_override['accelerators'],
                         {'A100-80GB': 1})

    def test_redriven_pinned_launch_keeps_location(self):
        # Controller crash mid-PENDING: the launch is re-driven with the
        # persisted override (location inlined, sentinel already
        # stripped, use_spot=False). The placer selection path is
        # skipped, but the upserted row must keep the pinned location --
        # location=None would permanently drop the replica from fill
        # accounting.
        location = spot_placer.Location.from_pickleable(_K8S_KEY)
        placer = mock.Mock()
        manager = _make_manager(placer)
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
        manager = _make_manager(placer=None)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            launched = manager._launch_replica(7, {_FILL_KEY: True})
        self.assertFalse(launched)
        add_mock.assert_not_called()


class TestDemandLaunchBudget(unittest.TestCase):
    """Demand launches obey measured free-GPU capacity."""

    def test_exhausted_zero_cost_only_budget_persists_no_replica(self):
        location = _make_location('research-ctx', 'free')
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [location]
        placer.active_locations.return_value = [location]
        placer.select_next_location.return_value = location
        manager = _make_manager(placer)
        budget = replica_managers._ZeroCostDemandBudget(
            {('research-ctx', 'a100'): 0}, {('research-ctx', 'a100'): 0})

        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=True), \
             mock.patch.object(reserved_capacity_broker,
                               'get_cached_grant',
                               return_value=None), \
             mock.patch.object(manager, '_persist_replica') as persist:
            launched = manager._scale_up_one_locked(
                None, set(), [], zero_cost_demand_budget=budget)

        self.assertFalse(launched)
        persist.assert_not_called()


class TestQueryFreeSlots(unittest.TestCase):
    """Poller free-slot math: slots are per-replica GPUs; unknown (-1)
    availability is a measurement blackout (the standalone sum reads it
    as 0 free for the cycle, but it is never a *successful* 0)."""

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

    def test_pool_shapes_preserve_exact_whole_gpu_counts(self):
        shapes = reserved_capacity.zero_cost_pool_shapes([
            self._k8s_location(gpu='A100', count=2.0),
            self._k8s_location(gpu='H100', count=1),
        ])
        self.assertEqual(shapes, {
            ('research-ctx', 'a100'): 2,
            ('research-ctx', 'h100'): 1,
        })

    def test_pool_shapes_reject_fractional_and_non_finite_counts(self):
        for count in (0.5, 1.5, float('nan'), float('inf')):
            with self.subTest(count=count):
                self.assertEqual(
                    reserved_capacity.zero_cost_pool_shapes(
                        [self._k8s_location(count=count)]), {})

    def test_unknown_availability_counts_zero(self):
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': -1
                               })):
            self.assertEqual(
                reserved_capacity.query_free_slots([self._k8s_location()]), 0)

    def test_unknown_availability_is_measurement_blackout(self):
        # A swallowed cluster-wide failure (e.g. pod-list 403) surfaces
        # as {'A100': -1}: that is NOT an authoritative 0-free
        # measurement. It must read as a blackout (free_slots=None) so
        # the broker's blind-round semantics engage -- grants floored at
        # holdings, feed 0 -- instead of letting a new claimant or
        # weight change redistribute grants while availability is
        # unknown. gpu_names still carries the seen names: an unknown
        # reading is not a phantom pool either.
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': -1
                               })):
            observation = reserved_capacity.query_pool_observation(
                'research-ctx', 'A100', 1)
        self.assertIsNone(observation.free_slots)
        self.assertEqual(observation.gpu_names, ('A100',))

    def test_any_negative_availability_blacks_out_the_whole_pool(self):
        # Partial unknowns poison the sum the same way: one -1 among
        # positive counts means the pool's free level is not measurable.
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 3,
                                   'A100-80GB': -1
                               })):
            observation = reserved_capacity.query_pool_observation(
                'research-ctx', 'A100', 1)
        self.assertIsNone(observation.free_slots)
        self.assertEqual(set(observation.gpu_names), {'A100', 'A100-80GB'})

    def test_group_observation_sums_requested_accelerators_once(self):
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 3,
                                   'A100-80GB': 4,
                                   'H100': 9,
                               })) as query:
            observation = reserved_capacity.query_pool_group_observation(
                'research-ctx', {
                    'a100': 1,
                    'a100-80gb': 2,
                })
        self.assertEqual(observation.free_slots, 5)
        self.assertEqual(set(observation.gpu_names), {'A100', 'A100-80GB'})
        query.assert_called_once()

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

    def test_demand_snapshot_queries_two_shapes_in_context_once(self):
        locations = [
            self._k8s_location(gpu='A100'),
            self._k8s_location(gpu='A100-80GB'),
        ]
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 220,
                                   'A100-80GB': 3,
                                   'L4': 99,
                               })) as query:
            free = reserved_capacity.query_free_slots_by_context(locations)

        self.assertEqual(free, {'research-ctx': 223})
        query.assert_called_once_with(gpus_only=True,
                                      name_filter=None,
                                      region_filter='research-ctx',
                                      quantity_filter=None,
                                      case_sensitive=False,
                                      require_price=False)

    def test_demand_snapshot_pools_per_shape_and_dedupes_to_largest(self):
        # Heterogeneous shapes in one context pool per shape: each shape
        # contributes available // per_replica and the context budget is
        # the sum. Duplicate (context, gpu) entries dedupe to the LARGEST
        # per-replica count (conservative), so A100 x2 and A100 x8 read
        # as a single 8-GPU shape: 220 // 8 + 3 // 1 = 30.
        locations = [
            self._k8s_location(gpu='A100', count=2),
            self._k8s_location(gpu='A100', count=8),
            self._k8s_location(gpu='A100-80GB', count=1),
        ]
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 220,
                                   'A100-80GB': 3,
                               })) as query:
            free = reserved_capacity.query_free_slots_by_context(locations)
        self.assertEqual(free, {'research-ctx': 30})
        query.assert_called_once()

    def test_demand_snapshot_distinguishes_unknown_from_zero(self):
        locations = [self._k8s_location(gpu='A100')]
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': -1
                               })):
            self.assertEqual(
                reserved_capacity.query_free_slots_by_context(locations),
                {'research-ctx': None})
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {})):
            self.assertEqual(
                reserved_capacity.query_free_slots_by_context(locations),
                {'research-ctx': 0})

    def test_shared_demand_cache_returns_raw_gpus_per_accelerator(self):
        locations = [
            self._k8s_location(gpu='A100', count=8),
            self._k8s_location(gpu='H100', count=8),
        ]
        row = {
            'context': 'research-ctx',
            'snapshot_time': 100.0,
            # A slow query completed much later. Freshness must use completion
            # while planner debits retain the query-start snapshot.
            'completed_at': 200.0,
            'availability': '{"a100": 223, "h100": 16}',
        }
        with mock.patch.object(reserved_capacity.time,
                               'time',
                               return_value=201.0), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={'research-ctx': row}), \
             mock.patch.object(reserved_capacity,
                               '_schedule_demand_capacity_refresh') as schedule:
            observations = reserved_capacity.get_cached_free_gpus_by_pool(
                locations)

        self.assertEqual(observations[('research-ctx', 'a100')].free_gpus, 223)
        self.assertEqual(observations[('research-ctx', 'h100')].free_gpus, 16)
        self.assertEqual(observations[('research-ctx', 'a100')].snapshot_time,
                         100.0)
        schedule.assert_called_once_with(set())

    def test_demand_cache_freshness_honors_configured_poll_interval(self):
        location = self._k8s_location(gpu='A100', count=8)
        row = {
            'context': 'research-ctx',
            'snapshot_time': 90.0,
            'completed_at': 100.0,
            'availability': '{"a100": 8}',
        }
        env_var = constants.RESERVED_CAPACITY_POLL_INTERVAL_ENV_VAR
        with mock.patch.dict(reserved_capacity.os.environ, {env_var: '300'}), \
             mock.patch.object(reserved_capacity.time,
                               'time',
                               return_value=220.0), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={'research-ctx': row}), \
             mock.patch.object(reserved_capacity,
                               '_schedule_demand_capacity_refresh') as schedule:
            observations = reserved_capacity.get_cached_free_gpus_by_pool(
                [location])

        self.assertEqual(observations[('research-ctx', 'a100')].free_gpus, 8)
        schedule.assert_called_once_with(set())

    def test_stale_shared_demand_cache_schedules_without_querying_inline(self):
        location = self._k8s_location(gpu='A100', count=8)
        with mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={}), \
             mock.patch.object(reserved_capacity,
                               '_schedule_demand_capacity_refresh') as schedule, \
             mock.patch.object(
                 reserved_capacity.kubernetes_catalog,
                 'list_accelerators_realtime') as query:
            observations = reserved_capacity.get_cached_free_gpus_by_pool(
                [location])

        self.assertIsNone(observations[('research-ctx', 'a100')].free_gpus)
        schedule.assert_called_once_with({'research-ctx'})
        query.assert_not_called()

    def test_background_refresh_publishes_one_raw_context_observation(self):
        lock = mock.MagicMock()
        lock.acquire.return_value = mock.MagicMock()
        with mock.patch.object(reserved_capacity.locks,
                               'get_lock',
                               return_value=lock), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={}), \
             mock.patch.object(
                 reserved_capacity.kubernetes_catalog,
                 'list_accelerators_realtime',
                 return_value=({}, {}, {
                     'A100': 223,
                     'H100': 16,
                 })) as query, \
             mock.patch.object(
                 reserved_capacity.serve_state,
                 'upsert_demand_capacity_observation') as publish:
            reserved_capacity._refresh_demand_capacity_contexts(
                {'research-ctx'})

        query.assert_called_once_with(gpus_only=True,
                                      name_filter=None,
                                      region_filter='research-ctx',
                                      quantity_filter=None,
                                      case_sensitive=False,
                                      require_price=False)
        context, snapshot_time, completed_at, availability = publish.call_args.args
        self.assertEqual(context, 'research-ctx')
        self.assertIsInstance(snapshot_time, float)
        self.assertIsInstance(completed_at, float)
        self.assertGreaterEqual(completed_at, snapshot_time)
        self.assertEqual(availability, {'a100': 223, 'h100': 16})

    def test_background_refresh_caches_query_failure(self):
        lock = mock.MagicMock()
        lock.acquire.return_value = mock.MagicMock()
        with mock.patch.object(reserved_capacity.locks,
                               'get_lock',
                               return_value=lock), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={}), \
             mock.patch.object(
                 reserved_capacity.kubernetes_catalog,
                 'list_accelerators_realtime',
                 side_effect=RuntimeError('API unavailable')), \
             mock.patch.object(
                 reserved_capacity.serve_state,
                 'upsert_demand_capacity_observation') as publish:
            reserved_capacity._refresh_demand_capacity_contexts(
                {'research-ctx'})

        context, snapshot_time, completed_at, availability = publish.call_args.args
        self.assertEqual(context, 'research-ctx')
        self.assertIsInstance(snapshot_time, float)
        self.assertIsInstance(completed_at, float)
        self.assertGreaterEqual(completed_at, snapshot_time)
        self.assertIsNone(availability)


class TestDemandCapacityRefreshScheduling(unittest.TestCase):
    """Worker launch ownership follows the actual worker lifecycle."""

    def setUp(self):
        self._old_running = reserved_capacity._DEMAND_REFRESH_RUNNING
        self._old_pending = set(
            reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS)
        reserved_capacity._DEMAND_REFRESH_RUNNING = False
        reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS.clear()

    def tearDown(self):
        reserved_capacity._DEMAND_REFRESH_RUNNING = self._old_running
        reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS.clear()
        reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS.update(
            self._old_pending)

    def test_launch_failure_releases_ownership_and_preserves_pending(self):
        worker = mock.Mock()

        def _fail_start():
            # Model another reconciliation coalescing work after launch
            # ownership was reserved but before Thread.start() failed.
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-b'})
            raise RuntimeError("can't start new thread")

        worker.start.side_effect = _fail_start
        with mock.patch.object(reserved_capacity.threading,
                               'Thread',
                               return_value=worker), \
             mock.patch.object(reserved_capacity.logger, 'error') as error:
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-a'})

        self.assertFalse(reserved_capacity._DEMAND_REFRESH_RUNNING)
        self.assertEqual(reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS,
                         {'ctx-a', 'ctx-b'})
        error.assert_called_once()

    def test_next_schedule_retries_all_pending_contexts(self):
        worker = mock.Mock()
        starts = 0

        def _start():
            nonlocal starts
            starts += 1
            if starts == 1:
                raise RuntimeError("can't start new thread")

        worker.start.side_effect = _start
        with mock.patch.object(reserved_capacity.threading,
                               'Thread',
                               return_value=worker):
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-a'})
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-b'})

        self.assertEqual(starts, 2)
        self.assertTrue(reserved_capacity._DEMAND_REFRESH_RUNNING)
        self.assertEqual(reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS,
                         {'ctx-a', 'ctx-b'})

    def test_successful_launch_remains_single_flight(self):
        worker = mock.Mock()
        with mock.patch.object(reserved_capacity.threading,
                               'Thread',
                               return_value=worker) as thread:
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-a'})
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-b'})

        thread.assert_called_once_with(
            target=reserved_capacity._demand_capacity_refresh_worker,
            name='serve-demand-capacity-refresh',
            daemon=True)
        worker.start.assert_called_once_with()
        self.assertTrue(reserved_capacity._DEMAND_REFRESH_RUNNING)
        self.assertEqual(reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS,
                         {'ctx-a', 'ctx-b'})


class TestCostFeasibilityDegradation(unittest.TestCase):
    """Unavailable prices are represented completely in the catalog."""

    def test_unavailable_price_is_materialized_as_infinity(self):
        location = _make_location('paid-region', 'missing-price')
        materialized = mock.Mock()
        materialized.get_cost.side_effect = ValueError(
            "No SpotPrice found for instance type 'gr6.8xlarge'.")
        resources = mock.Mock()
        resources.copy.return_value = materialized
        task = types.SimpleNamespace(resources=[resources], num_nodes=1)

        with mock.patch.object(spot_placer,
                               '_get_possible_location_from_task',
                               return_value=[location]):
            catalog = spot_placer.PlacementCatalog.from_task(task)

        self.assertEqual(catalog.costs(), {location: float('inf')})
        materialized.get_cost.assert_called_once_with(seconds=3600)

    def test_kubernetes_zero_cost_seed_avoids_live_reclassification(self):
        location = spot_placer.Location.from_pickleable(_K8S_KEY)
        task = types.SimpleNamespace(resources=[mock.Mock()], num_nodes=1)
        with mock.patch.object(spot_placer,
                               '_get_possible_location_from_task',
                               return_value=[location]):
            placer = spot_placer.SpotPlacer(task)
        placer.resources.copy = mock.Mock(
            side_effect=AssertionError('must not query cluster feasibility'))

        for _ in range(100):
            self.assertEqual(placer.zero_cost_locations(), [location])

        placer.resources.copy.assert_not_called()

    def test_missing_spot_price_does_not_block_zero_cost_enumeration(self):
        placer = spot_placer.DynamicFallbackSpotPlacer.__new__(
            spot_placer.DynamicFallbackSpotPlacer)
        free = _make_location('research-ctx', 'free')
        missing_price = _make_location('paid-region', 'missing-price')
        placer.location2status = {
            free: spot_placer.LocationStatus.ACTIVE,
            missing_price: spot_placer.LocationStatus.ACTIVE,
        }
        placer.location2cost = {free: 0.0, missing_price: float('inf')}

        self.assertEqual(placer.zero_cost_locations(), [free])
        self.assertEqual(placer.location2cost[missing_price], float('inf'))

    def test_missing_price_candidate_does_not_hide_priced_candidate(self):
        missing = _make_location('paid-region', 'missing-price')
        priced = _make_location('other-paid-region', 'priced')
        placer = _make_placer({
            missing: float('inf'),
            priced: 0.42,
        })

        self.assertEqual(placer.cost_per_hour(missing), float('inf'))
        self.assertEqual(placer.cost_per_hour(priced), 0.42)


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
        # Without the ceiling the feed of 5 would fund 5 fill ups. The grant
        # of 2 allows exactly two fill replicas, independent of demand.
        self.assertEqual(len(sentinel), 2)

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
        # stay capped at the grant: ceiling 2 produces exactly 2 fill ups,
        # independent of demand. An all-version launch ceiling (2 + 3 old
        # rows) would fund 5 fill ups, overshooting the grant by the old rows'
        # count.
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=1,
                                                             max_replicas=20),
                                                       version=2)
        _feed_broker(autoscaler, 5, grant=2)
        old_demand = [_replica(i, _K8S_KEY, version=1) for i in range(1, 4)]
        for row in old_demand:
            row.reserved_fill = False
        decisions = autoscaler.generate_scaling_decisions(old_demand, [1])
        self.assertEqual(len(_fill_ups(decisions)), 2)
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
    """A fill launch carrying a superseded epoch skips, leaking nothing.

    Broker-stamped launches persist through the broker's
    persist_fill_replica (the epoch recheck atomic with the row upsert
    and mutually excluded with in-flight rounds -- the pre-check alone is
    TOCTOU); un-stamped launches keep the plain persist.
    """

    def _launch(self,
                pool_epochs,
                carried_epoch=7,
                carried_pool='pool-b',
                persist_epoch_current=True):
        """pool_epochs: pool_key -> current round epoch (the fence read).

        persist_epoch_current: what the atomic persist-time recheck
        reports (False = a new round published between the pre-check and
        the persist).
        """
        location = _make_location('research-ctx', 'free')
        placer = mock.Mock()
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
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
                               'add_or_update_replica') as add_mock, \
             mock.patch.object(replica_managers.reserved_capacity_broker,
                               'persist_fill_replica',
                               return_value=persist_epoch_current
                              ) as fenced_add_mock:
            launched = manager._launch_replica(7, override)
        return launched, add_mock, epoch_mock, fenced_add_mock

    def test_stale_epoch_skips_without_persisting_a_row(self):
        launched, add_mock, epoch_mock, fenced_add = self._launch({'pool-b': 8})
        self.assertFalse(launched)
        add_mock.assert_not_called()
        fenced_add.assert_not_called()
        # The fence reads the CARRIED pool's round epoch, not a global one.
        epoch_mock.assert_called_once_with('pool-b')

    def test_current_epoch_launches_and_strips_the_keys(self):
        launched, add_mock, _, fenced_add = self._launch({'pool-b': 7})
        self.assertTrue(launched)
        # Broker-stamped: persisted through the atomic epoch-rechecking
        # path, carrying the pool key and the carried epoch.
        add_mock.assert_not_called()
        fenced_add.assert_called_once()
        self.assertEqual(fenced_add.call_args.kwargs['pool_key'], 'pool-b')
        self.assertEqual(fenced_add.call_args.kwargs['expected_epoch'], 7)
        info = fenced_add.call_args[0][2]
        self.assertNotIn(_EPOCH_KEY, info.resources_override)
        self.assertNotIn(_POOL_KEY, info.resources_override)
        self.assertNotIn(_FILL_KEY, info.resources_override)
        self.assertTrue(info.reserved_fill)

    def test_peer_pool_epoch_bump_does_not_fence(self):
        # Cross-pool isolation at the fence: pool A's epoch moved (8) but
        # this launch carries pool B's still-current epoch (7) -- it must
        # launch. Pool B's own stale epoch (6 vs 7) still fences.
        launched, _, _, fenced_add = self._launch({'pool-a': 8, 'pool-b': 7})
        self.assertTrue(launched)
        fenced_add.assert_called_once()
        fenced, _, _, fenced_persist = self._launch({
            'pool-a': 8,
            'pool-b': 7
        },
                                                    carried_epoch=6)
        self.assertFalse(fenced)
        fenced_persist.assert_not_called()

    def test_missing_round_fails_open(self):
        # No round row for the pool (current_epoch None): there is no
        # newer allocation to defer to -- proceed rather than deadlock
        # fill forever (add_replica_if_round_epoch fails open the same
        # way at persist time).
        launched, add_mock, _, fenced_add = self._launch({})
        self.assertTrue(launched)
        add_mock.assert_not_called()
        fenced_add.assert_called_once()

    def test_epoch_without_pool_key_fails_open(self):
        # Defensive: the epoch is only meaningful against its pool's
        # round; a sentinel missing the pool key (never emitted by the
        # autoscaler) must not fence -- and without a pool to recheck it
        # takes the plain persist.
        launched, add_mock, epoch_mock, fenced_add = self._launch(
            {'pool-b': 8}, carried_pool=None)
        self.assertTrue(launched)
        add_mock.assert_called_once()
        fenced_add.assert_not_called()
        epoch_mock.assert_not_called()

    def test_round_published_between_precheck_and_persist_skips(self):
        # TOCTOU closed: the pre-check passed (carried epoch 7 is
        # current) but a new round published before the persist; the
        # atomic recheck reports stale and the launch must skip without
        # leaking a row or a launch thread.
        launched, add_mock, _, fenced_add = self._launch(
            {'pool-b': 7}, persist_epoch_current=False)
        self.assertFalse(launched)
        add_mock.assert_not_called()
        fenced_add.assert_called_once()


class TestDemandPlacementGate(unittest.TestCase):
    """Zero-cost holdings >= grant: NEW demand launches skip the free tier."""

    def _launch_demand(self, grant, replicas):
        zero_cost_location = spot_placer.Location.from_pickleable(_K8S_KEY)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [zero_cost_location]
        # This fixture isolates the broker grant gate.  Keep the independent
        # speculative-placement gate inert by reporting no active locations.
        placer.active_locations.return_value = []
        selected = _make_location('us-east-1', 'paid', use_spot=True)
        placer.select_next_location.return_value = selected
        manager = _make_manager(placer)
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

    def test_default_prefers_zero_cost(self):
        selected = self.placer.select_next_location()
        self.assertEqual(selected, self.k8s)

    def test_skip_selects_paid(self):
        selected = self.placer.select_next_location(
            skip_zero_cost_preference=True)
        self.assertEqual(selected, self.paid)

    def test_skip_excludes_zero_cost(self):
        selected = self.placer.select_next_location(
            skip_zero_cost_preference=True)
        self.assertEqual(selected, self.paid)

    def test_skip_with_zero_cost_only_set_still_serves(self):
        # The gate throttles placement preference, never availability: a
        # zero-cost-only candidate set must still yield a location.
        placer = _make_placer({self.k8s: 0.0})
        selected = placer.select_next_location(skip_zero_cost_preference=True)
        self.assertEqual(selected, self.k8s)


class TestBrokerPollerCycle(unittest.TestCase):
    """The broker cycle claims, reads the round, and feeds grant + epoch."""

    def _run_cycle(self,
                   allocation,
                   zero_cost=None,
                   replica_infos=(),
                   utilization_gate=False):
        autoscaler = _make_autoscaler(min_replicas=1)
        autoscaler.reserved_fill_utilization_gate = utilization_gate
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

    def test_gated_writer_without_telemetry_publishes_armed_blind_signal(self):
        allocation = reserved_capacity_broker.Allocation(grant=0,
                                                         feed=0,
                                                         round_id=1,
                                                         epoch=1,
                                                         snapshot_time=1.0)
        _, upsert_mock, _, _ = self._run_cycle(allocation,
                                               utilization_gate=True)
        self.assertEqual(upsert_mock.call_args.kwargs['activity'], {
            'demonstrated_need': None,
            'boot_hold': False,
        })

    def test_explicitly_ungated_writer_omits_utilization(self):
        allocation = reserved_capacity_broker.Allocation(grant=None,
                                                         feed=0,
                                                         round_id=1,
                                                         epoch=1,
                                                         snapshot_time=1.0)
        _, upsert_mock, _, _ = self._run_cycle(allocation,
                                               utilization_gate=False)
        self.assertIsNone(upsert_mock.call_args.kwargs['activity'])

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

    def test_same_context_accelerators_share_one_broker_group(self):
        other = dict(_K8S_KEY, accelerators={'H100': 1})
        zero_cost = [
            spot_placer.Location.from_pickleable(_K8S_KEY),
            spot_placer.Location.from_pickleable(other),
        ]
        autoscaler, upsert_mock, remove_mock, round_mock = self._run_cycle(
            allocation=None, zero_cost=zero_cost)
        upsert_mock.assert_called_once()
        self.assertEqual(
            upsert_mock.call_args.kwargs['pool_key'],
            reserved_capacity_broker.make_pool_key('research-ctx',
                                                   ('a100', 'h100')))
        remove_mock.assert_not_called()
        round_mock.assert_called_once()
        self.assertIsNone(autoscaler._fill_grant)

    def test_multiple_contexts_withdraw_claim_and_feed_zero(self):
        other = dict(_K8S_KEY, region='other-ctx', accelerators={'H100': 1})
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

    def test_logical_multi_gpu_shape_withdraws_claim_and_feeds_zero(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        logical_placer = (
            spot_placer.CapacityAwareDynamicFallbackSpotPlacer.__new__(
                spot_placer.CapacityAwareDynamicFallbackSpotPlacer))
        location_data = dict(_K8S_KEY, accelerators={'A100': 2})
        zero_cost = [spot_placer.Location.from_pickleable(location_data)]
        keys = [location.to_pickleable() for location in zero_cost]

        with mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'remove_claim') as remove_mock, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'upsert_claim') as upsert_mock, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'run_round_if_stale') as round_mock:
            reserved_capacity._broker_cycle(autoscaler, logical_placer, 'svc',
                                            zero_cost, keys)

        remove_mock.assert_called_once_with('svc')
        upsert_mock.assert_not_called()
        round_mock.assert_not_called()
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)


class TestReplicaManagerInitIntact(unittest.TestCase):
    """Real __init__ must fully run (fill fixtures bypass it via __new__).

    Pins the regression class where an addition spliced into the middle
    of ReplicaManager.__init__ silently truncated it.
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
        self.assertIsNone(manager.spot_placer)


if __name__ == '__main__':
    unittest.main()


class TestFillDemandSample(unittest.TestCase):
    """The gate's detailed signal is available only with fresh telemetry."""

    def _autoscaler(self):
        spec = service_spec.SkyServiceSpec(
            readiness_path='/health',
            initial_delay_seconds=60,
            readiness_timeout_seconds=30,
            endpoint_probe_interval_seconds=10,
            lb_stream_timeout_seconds=60,
            min_replicas=1,
            max_replicas=100,
            target_concurrency_per_replica=1,
            reserved_capacity_fill={'utilization_gate': True})
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc', spec)
        autoscaler.seed_zero_cost_locations([_K8S_KEY])
        return autoscaler

    def _fill_replica(self, replica_id, **kwargs):
        info = _replica(replica_id, _K8S_KEY, **kwargs)
        info.reserved_fill = True
        return info

    def test_no_sample_without_a_fresh_report(self):
        # The poller converts this lack of utilization proof to fresh NULL
        # need (armed-but-blind) for a gated service; the autoscaler projection
        # itself stays explicit about having no detailed sample.
        autoscaler = self._autoscaler()
        self.assertIsNone(autoscaler.fill_demand_sample([]))

    def test_base_autoscaler_class_has_no_detailed_sample(self):
        spec = service_spec.SkyServiceSpec(readiness_path='/health',
                                           initial_delay_seconds=60,
                                           readiness_timeout_seconds=30,
                                           endpoint_probe_interval_seconds=10,
                                           lb_stream_timeout_seconds=60,
                                           min_replicas=1,
                                           max_replicas=10,
                                           target_qps_per_replica=1.0,
                                           reserved_capacity_fill=True)
        autoscaler = autoscalers.RequestRateAutoscaler('svc', spec)
        self.assertIsNone(autoscaler.fill_demand_sample([]))

    def test_unknown_occupancy_counts_per_replica_not_per_service(self):
        # The decisive property. A service-level "any unknown occupancy"
        # boolean would let 3 flapping replicas out of 77 pin the whole
        # fleet busy forever, making the gate inert on exactly the service
        # it exists for.
        autoscaler = self._autoscaler()
        replicas = [self._fill_replica(i) for i in range(1, 78)]
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._report_received_at = time.time()
        autoscaler._replica_is_busy = lambda info: info.replica_id <= 3

        sample = autoscaler.fill_demand_sample(replicas)

        self.assertIsNotNone(sample)
        self.assertEqual(sample.busy_fill_holdings, 3)
        self.assertEqual(sample.demonstrated_need(), 3)

    def test_pre_ready_fill_rows_hold_a_release_step(self):
        autoscaler = self._autoscaler()
        booting = self._fill_replica(
            1, status=serve_state.ReplicaStatus.PROVISIONING)
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._report_received_at = time.time()

        sample = autoscaler.fill_demand_sample([booting])

        self.assertIsNotNone(sample)
        self.assertEqual(sample.pre_ready_fill_holdings, 1)
        self.assertTrue(sample.boot_hold())
        self.assertEqual(sample.demonstrated_need(), 1)

    def test_fully_idle_fleet_demonstrates_no_need(self):
        autoscaler = self._autoscaler()
        replicas = [self._fill_replica(i) for i in range(1, 10)]
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._report_received_at = time.time()
        autoscaler._replica_is_busy = lambda info: False

        sample = autoscaler.fill_demand_sample(replicas)

        self.assertIsNotNone(sample)
        self.assertEqual(sample.demonstrated_need(), 0)
        self.assertFalse(sample.boot_hold())

    def test_demand_placed_zero_cost_rows_are_not_counted(self):
        # They are demand-protected and exempt from the grant ceiling, so
        # counting them would inflate need by capacity the gate can never
        # reclaim anyway.
        autoscaler = self._autoscaler()
        demand_row = _replica(1, _K8S_KEY)
        demand_row.reserved_fill = False
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._report_received_at = time.time()
        autoscaler._replica_is_busy = lambda info: True

        sample = autoscaler.fill_demand_sample([demand_row])

        self.assertIsNotNone(sample)
        self.assertEqual(sample.busy_fill_holdings, 0)


class TestOutstandingWorkPartsExtraction(unittest.TestCase):
    """The pure variant must not clobber decision-owned observability."""

    def test_pure_variant_assigns_nothing(self):
        spec = service_spec.SkyServiceSpec(readiness_path='/health',
                                           initial_delay_seconds=60,
                                           readiness_timeout_seconds=30,
                                           endpoint_probe_interval_seconds=10,
                                           lb_stream_timeout_seconds=60,
                                           min_replicas=1,
                                           max_replicas=10,
                                           target_concurrency_per_replica=1,
                                           reserved_capacity_fill=True)
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc', spec)
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._weighted_queue_work = -1.0
        autoscaler._rejected_concurrency = -1.0

        autoscaler._outstanding_work_parts([])

        self.assertEqual(autoscaler._weighted_queue_work, -1.0)
        self.assertEqual(autoscaler._rejected_concurrency, -1.0)

        total = autoscaler._outstanding_work([])

        self.assertEqual(autoscaler._weighted_queue_work, 0.0)
        self.assertEqual(autoscaler._rejected_concurrency, 0.0)
        self.assertEqual(total, 0.0)
