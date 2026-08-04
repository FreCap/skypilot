"""Measured free capacity should govern an observable fill pool.

The blind-probe bench exists for capacity that cannot be measured: a spot
region either has GPUs or it does not, and the only way to find out is to
launch. A Kubernetes fill pool is not like that -- the broker measures its
free slots every round. Rationing probes against a pool you just counted
throws the measurement away and bounds refill by a clock.

These tests define the behaviour that fixes it, and the safety properties
that must survive the fix: a bench recorded AFTER the last measurement still
holds (so a pool that measures free but rejects launches cannot spin), and
paid spot keeps its probe budget untouched.

Every launchability gate in the fill path reads `_effective_status`, directly
or through `active_locations()`, so the tests below drive all of them:

    _effective_status
      |- select_next_zero_cost_location   (the launch gate)
      `- active_locations
           |- _placer_can_launch_zero_cost -> claim.launchable  (v1)
           |- per-pool `launchable` dict                        (v2)
           |- _pool_capacity_hint  -> per-card demand target
           `- compute_feeds        -> the fill feed
"""
# pylint: disable=protected-access,no-member
import time
from unittest import mock

from spot_placer_test_utils import make_location

from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_allocation as allocation
from sky.serve import spot_placer

# Two Kubernetes fill pools on two different clusters, each with its own
# shapes -- the multi-cluster case the broker's v2 protocol supports.
_EAST_A100_80GB = make_location('prod_research_cluster_eks',
                                accelerators={'A100-80GB': 1},
                                use_spot=False,
                                cloud_name='Kubernetes')
_EAST_A100 = make_location('prod_research_cluster_eks',
                           accelerators={'A100': 1},
                           use_spot=False,
                           cloud_name='Kubernetes')
_PHX_H200 = make_location('phx_research_cluster_eks',
                          accelerators={'H200': 1},
                          use_spot=False,
                          cloud_name='Kubernetes')
_AWS_SPOT_L4 = make_location('us-east-1',
                             accelerators={'L4': 1},
                             use_spot=True,
                             cloud_name='AWS')

_EAST = (_EAST_A100_80GB, _EAST_A100)
_PHX = (_PHX_H200,)
_ACTIVE = spot_placer.LocationStatus.ACTIVE
_PREEMPTED = spot_placer.LocationStatus.PREEMPTED


def _placer(*, benched=(), benched_at=None, locations=None):
    """A placer over the given locations (one k8s pool + paid spot default).

    `locations` is explicit because zero-cost launchability is a property of
    the whole placer: an unbenched second pool would satisfy every
    `select_next_zero_cost_location` and mask a single-pool assertion.
    """
    placer = spot_placer.CapacityAwareDynamicFallbackSpotPlacer.__new__(
        spot_placer.CapacityAwareDynamicFallbackSpotPlacer)
    if locations is None:
        locations = [*_EAST, _AWS_SPOT_L4]
    placer.location2status = {location: _ACTIVE for location in locations}
    placer.location2preempted_at = {}
    placer.location2preempted_reason = {}
    placer.location2retry_reserved_at = {}
    placer.location2cost = {
        _EAST_A100_80GB: 0.0,
        _EAST_A100: 0.0,
        _PHX_H200: 0.0,
        _AWS_SPOT_L4: 1.0,
    }
    placer._retry_state_dirty = False
    placer._workspace = None
    stamp = time.time() if benched_at is None else benched_at
    for location in benched:
        placer.location2status[location] = _PREEMPTED
        placer.location2preempted_at[location] = stamp
    return placer


def _drain(placer, limit=300):
    """How many fill launches one wave can actually place."""
    placed = []
    for _ in range(limit):
        location = placer.select_next_zero_cost_location()
        if location is None:
            break
        placed.append(location)
    return placed


def _pool_launchable(placer, pool_locations):
    """The per-pool `launchable` value `_broker_cycle_v2` computes."""
    active = placer.active_locations()
    return any(
        any(
            spot_placer.locations_match_placement(location, candidate)
            for candidate in active)
        for location in pool_locations)


class TestMeasurementReleasesTheBench:

    def test_measured_free_capacity_makes_a_benched_pool_selectable(self):
        placer = _placer(benched=_EAST)
        assert placer.select_next_zero_cost_location() is None

        placer.observe_zero_cost_capacity({
            _EAST_A100_80GB: 165,
            _EAST_A100: 53
        },
                                          observed_at=time.time())

        assert reserved_capacity._placer_can_launch_zero_cost(placer)

    def test_a_measured_pool_serves_the_whole_wave(self):
        placer = _placer(benched=_EAST)
        placer.observe_zero_cost_capacity({
            _EAST_A100_80GB: 165,
            _EAST_A100: 53
        },
                                          observed_at=time.time())
        # 218 measured free slots must place 218 replicas, not 2.
        assert len(_drain(placer, limit=218)) == 218

    def test_selection_does_not_burn_the_probe_budget_when_measured(self):
        placer = _placer(benched=_EAST)
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time())
        placer.select_next_zero_cost_location()
        # Nothing to discover, so no reservation was consumed.
        assert _EAST_A100_80GB not in placer.location2retry_reserved_at

    def test_an_unbenched_pool_is_unaffected_by_a_measurement(self):
        placer = _placer()
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE
        assert len(_drain(placer, limit=50)) == 50


class TestMeasurementSafetyProperties:

    def test_a_bench_after_the_measurement_still_holds(self):
        # Ordering is the guard against spinning: a pool that measures free
        # but rejects launches (taints, affinity, admission) re-benches, and
        # that bench is newer than the reading it would otherwise ignore.
        placer = _placer()
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time() - 60)
        placer.location2status[_EAST_A100_80GB] = _PREEMPTED
        placer.location2preempted_at[_EAST_A100_80GB] = time.time()
        assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED

    def test_set_preemptive_after_a_measurement_re_benches(self):
        # The same property through the real API the launch path uses.
        placer = _placer()
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time() - 60)
        placer.set_preemptive(_EAST_A100_80GB, reason='capacity')
        assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED

    def test_a_failing_pool_cannot_spin_on_one_stale_reading(self):
        # Repeatedly: measure free, launch, fail. Each failure must leave the
        # pool benched until a NEWER reading arrives.
        placer = _placer()
        reading_at = time.time() - 120
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=reading_at)
        for _ in range(5):
            placer.set_preemptive(_EAST_A100_80GB, reason='capacity')
            assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED

    def test_a_stale_measurement_does_not_release_the_bench(self):
        placer = _placer(benched=_EAST)
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time() - 86_400)
        assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED

    def test_a_measurement_of_zero_free_does_not_release_the_bench(self):
        placer = _placer(benched=_EAST)
        placer.observe_zero_cost_capacity({
            _EAST_A100_80GB: 0,
            _EAST_A100: 0
        },
                                          observed_at=time.time())
        assert placer.select_next_zero_cost_location() is None

    def test_expired_bench_still_falls_back_to_the_probe_path(self):
        # With no measurement at all the old TTL behaviour must remain.
        placer = _placer(benched=_EAST, benched_at=time.time() - 10_000)
        assert len(_drain(placer, limit=50)) == 2


class TestPaidTierUnaffected:

    def test_paid_spot_never_gains_measured_availability(self):
        placer = _placer(benched=(_AWS_SPOT_L4,))
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time())
        assert placer._effective_status(_AWS_SPOT_L4) == _PREEMPTED

    def test_spot_keeps_one_probe_per_ttl_window(self):
        placer = _placer(benched=(_AWS_SPOT_L4,),
                         benched_at=time.time() - 10_000)
        assert placer._effective_status(_AWS_SPOT_L4) == _ACTIVE
        placer.reserve_retry(_AWS_SPOT_L4)
        assert placer._effective_status(_AWS_SPOT_L4) == _PREEMPTED

    def test_a_measurement_naming_a_spot_location_is_refused(self):
        # Defensive: only zero-cost locations are measurable. A caller that
        # hands in a paid location must not buy it a bench bypass.
        placer = _placer(benched=(_AWS_SPOT_L4,))
        placer.observe_zero_cost_capacity({_AWS_SPOT_L4: 500},
                                          observed_at=time.time())
        assert placer._effective_status(_AWS_SPOT_L4) == _PREEMPTED


class TestMultipleKubernetesPools:
    """Each cluster's measurement governs only its own locations."""

    def test_one_measured_cluster_does_not_release_another(self):
        placer = _placer(benched=(*_EAST, *_PHX),
                         locations=[*_EAST, *_PHX, _AWS_SPOT_L4])
        placer.observe_zero_cost_capacity({
            _EAST_A100_80GB: 165,
            _EAST_A100: 53
        },
                                          observed_at=time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE
        # The full cluster stays benched: no cross-pool leakage.
        assert placer._effective_status(_PHX_H200) == _PREEMPTED

    def test_a_wave_lands_only_on_the_cluster_with_capacity(self):
        placer = _placer(benched=(*_EAST, *_PHX),
                         locations=[*_EAST, *_PHX, _AWS_SPOT_L4])
        placer.observe_zero_cost_capacity({
            _EAST_A100_80GB: 100,
            _EAST_A100: 40
        },
                                          observed_at=time.time())
        placed = _drain(placer, limit=140)
        assert len(placed) == 140
        assert _PHX_H200 not in placed

    def test_each_cluster_is_released_by_its_own_round(self):
        placer = _placer(benched=(*_EAST, *_PHX),
                         locations=[*_EAST, *_PHX, _AWS_SPOT_L4])
        now = time.time()
        # Two independent broker rounds, one per pool.
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 10},
                                          observed_at=now)
        placer.observe_zero_cost_capacity({_PHX_H200: 22}, observed_at=now)
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE
        assert placer._effective_status(_PHX_H200) == _ACTIVE

    def test_a_later_round_supersedes_an_earlier_reading(self):
        # Bench first, then a reading that post-dates it.
        placer = _placer(benched=_EAST, benched_at=time.time() - 60)
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time() - 30)
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE
        # The pool filled up again; the next round reports zero free.
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 0},
                                          observed_at=time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED

    def test_one_pool_going_stale_leaves_the_other_running(self):
        placer = _placer(benched=(*_EAST, *_PHX),
                         locations=[*_EAST, *_PHX, _AWS_SPOT_L4])
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time() - 86_400)
        placer.observe_zero_cost_capacity({_PHX_H200: 22},
                                          observed_at=time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED
        assert placer._effective_status(_PHX_H200) == _ACTIVE

    def test_per_pool_launchable_tracks_each_measurement(self):
        # The exact dict `_broker_cycle_v2` builds for its claims.
        placer = _placer(benched=(*_EAST, *_PHX),
                         locations=[*_EAST, *_PHX, _AWS_SPOT_L4])
        assert not _pool_launchable(placer, _EAST)
        assert not _pool_launchable(placer, _PHX)
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time())
        assert _pool_launchable(placer, _EAST)
        assert not _pool_launchable(placer, _PHX)

    def test_a_partial_shape_measurement_releases_only_that_shape(self):
        # Within one cluster the two shapes are separate locations: only the
        # measured one becomes selectable.
        placer = _placer(benched=_EAST)
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time())
        placed = _drain(placer, limit=50)
        assert len(placed) == 50
        assert _EAST_A100 not in placed


class TestMeasurementFeedsTheDownstreamGates:
    """The measurement must reach the feed and the per-card target."""

    def test_capacity_hint_surfaces_free_slots_once_measured(self):
        placer = _placer(benched=_EAST)
        assert not reserved_capacity._placer_can_launch_zero_cost(placer)
        placer.observe_zero_cost_capacity({
            _EAST_A100_80GB: 165,
            _EAST_A100: 53
        },
                                          observed_at=time.time())
        # `_pool_capacity_hint` returns `holdings` while unlaunchable and
        # `holdings + observed_free` once launchable; this is the input that
        # flips it, and with it the per-card demand target.
        assert reserved_capacity._placer_can_launch_zero_cost(placer)

    def test_feed_follows_launchability_end_to_end(self):
        placer = _placer(benched=_EAST)
        claims = {
            'boltz-l4-fleet': allocation.ClaimInput(
                floor=10,
                weight=100.0,
                holdings_fill=1,
                launchable=reserved_capacity._placer_can_launch_zero_cost(
                    placer))
        }
        feeds, _ = allocation.compute_feeds(observed_free=218,
                                            grants={'boltz-l4-fleet': 228},
                                            claims=claims,
                                            sticky_state={},
                                            now=time.time(),
                                            sticky_window_seconds=60.0)
        assert feeds['boltz-l4-fleet'] == 0

        placer.observe_zero_cost_capacity({
            _EAST_A100_80GB: 165,
            _EAST_A100: 53
        },
                                          observed_at=time.time())
        claims = {
            'boltz-l4-fleet': allocation.ClaimInput(
                floor=10,
                weight=100.0,
                holdings_fill=1,
                launchable=reserved_capacity._placer_can_launch_zero_cost(
                    placer))
        }
        feeds, _ = allocation.compute_feeds(observed_free=218,
                                            grants={'boltz-l4-fleet': 228},
                                            claims=claims,
                                            sticky_state={},
                                            now=time.time(),
                                            sticky_window_seconds=60.0)
        assert feeds['boltz-l4-fleet'] == 218

    def test_a_benched_peer_no_longer_loses_the_pool_to_the_test_fleet(self):
        # The 2026-08-04 round-4 outcome, once production is measurable again.
        placer = _placer(benched=_EAST)
        placer.observe_zero_cost_capacity({
            _EAST_A100_80GB: 165,
            _EAST_A100: 53
        },
                                          observed_at=time.time())
        prod_launchable = reserved_capacity._placer_can_launch_zero_cost(placer)
        claims = {
            'boltz-l4-fleet': allocation.ClaimInput(floor=10,
                                                    weight=100.0,
                                                    holdings_fill=1,
                                                    launchable=prod_launchable),
            'boltz-l4-fleet-test': allocation.ClaimInput(floor=0,
                                                         weight=0.1,
                                                         holdings_fill=0,
                                                         launchable=True),
        }
        feeds, _ = allocation.compute_feeds(observed_free=218,
                                            grants={
                                                'boltz-l4-fleet': 228,
                                                'boltz-l4-fleet-test': 228,
                                            },
                                            claims=claims,
                                            sticky_state={},
                                            now=time.time(),
                                            sticky_window_seconds=60.0)
        # weight 100 vs 0.1 now decides it, as configured.
        assert feeds['boltz-l4-fleet'] > feeds['boltz-l4-fleet-test']


class TestMeasurementDurability:
    """Service updates rebuild the placer; controllers respawn."""

    def test_inherit_preemption_state_preserves_measured_availability(self):
        old = _placer(benched=_EAST)
        old.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                       observed_at=time.time())
        new = _placer(benched=_EAST)
        new.inherit_preemption_state(old)
        assert new._effective_status(_EAST_A100_80GB) == _ACTIVE

    def test_dump_retry_state_carries_the_measurement(self):
        placer = _placer(benched=_EAST, benched_at=time.time() - 60)
        measured_at = time.time() - 30
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=measured_at)
        bench = placer.dump_retry_state()['benches'][0]
        assert bench['measured_free'] == 165
        assert bench['measured_at'] == measured_at

    def test_load_retry_state_restores_the_measurement(self):
        # `Location` round-trips through `from_pickleable`, which cannot
        # reconstruct the MagicMock cloud these fixtures use, so resolution is
        # stubbed; the assertion is about the restored measurement.
        benched_at = time.time() - 60
        measured_at = time.time() - 30
        state = {
            'version': spot_placer.SpotPlacer._RETRY_STATE_VERSION,
            'benches': [{
                'location': _EAST_A100_80GB.to_pickleable(),
                'reason': 'capacity',
                'observed_at': benched_at,
                'measured_free': 165,
                'measured_at': measured_at,
            }],
        }
        placer = _placer(benched=_EAST, benched_at=time.time())
        with mock.patch.object(spot_placer.Location,
                               'from_pickleable',
                               return_value=_EAST_A100_80GB), \
             mock.patch.object(placer,
                               'resolve_location',
                               side_effect=lambda location, **_: location):
            placer.load_retry_state(state)

        assert (placer.location2observed_free[_EAST_A100_80GB] == (165,
                                                                   measured_at))
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE

    def test_durable_state_stays_json_safe(self):
        import json  # pylint: disable=import-outside-toplevel
        placer = _placer(benched=_EAST)
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=time.time())
        json.dumps(placer.dump_retry_state())


class TestObservationInputHandling:

    def test_an_unknown_location_is_ignored(self):
        placer = _placer(benched=_EAST)
        unknown = make_location('nowhere',
                                accelerators={'A100': 1},
                                use_spot=False,
                                cloud_name='Kubernetes')
        placer.observe_zero_cost_capacity({unknown: 99},
                                          observed_at=time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED

    def test_an_empty_observation_changes_nothing(self):
        placer = _placer(benched=_EAST)
        placer.observe_zero_cost_capacity({}, observed_at=time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED

    def test_a_negative_reading_is_treated_as_no_capacity(self):
        placer = _placer(benched=_EAST)
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: -5},
                                          observed_at=time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED

    def test_repeated_identical_observations_are_idempotent(self):
        placer = _placer(benched=_EAST)
        now = time.time()
        for _ in range(5):
            placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                              observed_at=now)
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE
        assert len(_drain(placer, limit=10)) == 10

    def test_an_older_reading_does_not_overwrite_a_newer_one(self):
        placer = _placer(benched=_EAST)
        now = time.time()
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 165},
                                          observed_at=now)
        # A late-arriving round from before the current one.
        placer.observe_zero_cost_capacity({_EAST_A100_80GB: 0},
                                          observed_at=now - 300)
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE

    def test_a_placer_without_the_field_still_works(self):
        # Old pickled placers predate the observation map.
        placer = _placer(benched=_EAST, benched_at=time.time() - 10_000)
        if hasattr(placer, 'location2observed_free'):
            del placer.location2observed_free
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE


class TestBrokerWiresTheObservationIntoThePlacer:
    """Without this wiring the placer never learns the count."""

    @staticmethod
    def _observation(free_slots, by_accelerator=None):
        from sky.serve import (
            reserved_capacity_broker)  # pylint: disable=import-outside-toplevel
        return reserved_capacity_broker.PoolObservation(
            free_slots=free_slots,
            gpu_names=(),
            free_slots_by_accelerator=by_accelerator)

    def test_per_accelerator_split_is_recorded_per_location(self):
        placer = _placer(benched=_EAST)
        reserved_capacity._record_pool_observation(
            placer, _EAST,
            self._observation(218, (('a100-80gb', 165), ('a100', 53))),
            time.time())
        assert placer.location2observed_free[_EAST_A100_80GB][0] == 165
        assert placer.location2observed_free[_EAST_A100][0] == 53

    def test_a_full_shape_is_not_advertised_as_free(self):
        # A100-80GB has capacity, A100 does not: only the former is released.
        placer = _placer(benched=_EAST)
        reserved_capacity._record_pool_observation(
            placer, _EAST,
            self._observation(165, (('a100-80gb', 165), ('a100', 0))),
            time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE
        assert placer._effective_status(_EAST_A100) == _PREEMPTED

    def test_pool_level_free_is_used_without_a_split(self):
        placer = _placer(benched=_EAST)
        reserved_capacity._record_pool_observation(placer, _EAST,
                                                   self._observation(218),
                                                   time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE
        assert placer._effective_status(_EAST_A100) == _ACTIVE

    def test_a_failed_query_is_a_blackout_not_a_zero(self):
        placer = _placer(benched=_EAST, benched_at=time.time() - 60)
        reserved_capacity._record_pool_observation(
            placer, _EAST, self._observation(218, (('a100-80gb', 165),)),
            time.time() - 30)
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE
        # free_slots None = the cluster query failed; the last good reading
        # keeps its own freshness clock rather than being reset to zero.
        reserved_capacity._record_pool_observation(placer, _EAST,
                                                   self._observation(None),
                                                   time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _ACTIVE

    def test_none_observation_is_tolerated(self):
        placer = _placer(benched=_EAST)
        reserved_capacity._record_pool_observation(placer, _EAST, None,
                                                   time.time())
        assert placer._effective_status(_EAST_A100_80GB) == _PREEMPTED

    def test_each_pool_records_only_its_own_locations(self):
        placer = _placer(benched=(*_EAST, *_PHX),
                         locations=[*_EAST, *_PHX, _AWS_SPOT_L4])
        reserved_capacity._record_pool_observation(
            placer, _EAST,
            self._observation(218, (('a100-80gb', 165), ('a100', 53))),
            time.time())
        assert _PHX_H200 not in placer.location2observed_free
        assert placer._effective_status(_PHX_H200) == _PREEMPTED
