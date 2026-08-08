"""Refill of an observable zero-cost pool after a full-cluster preemption.

The zero-cost tier of a fill-enabled service is Kubernetes capacity whose
free slots the broker MEASURES every round (`query_pool_group_observation`).
That is categorically different from the paid spot tier, where free capacity
is unobservable and the placer has to discover it by launching a probe.

These tests cover what happens after research load takes the whole reserved
cluster and then releases it -- the situation that emptied the production
fleet on 2026-08-04, when 218 of 328 A100s sat free while the service shed
requests onto paid spot that had no capacity.
"""
# pylint: disable=protected-access
import time
from unittest import mock

from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky.serve import placement_policy
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_allocation as allocation
from sky.serve import spot_placer

# The two Kubernetes fill shapes production declares in
# boltz-l4-fleet.serve.yaml, both on the one reserved context.
_K8S_A100_80GB = make_location('prod_research_cluster_eks',
                               accelerators={'A100-80GB': 1},
                               use_spot=False,
                               cloud_name='Kubernetes')
_K8S_A100 = make_location('prod_research_cluster_eks',
                          accelerators={'A100': 1},
                          use_spot=False,
                          cloud_name='Kubernetes')


def _make_zero_cost_placer(benched_at=None):
    """A placer over the two zero-cost k8s shapes, optionally benched."""
    locations = [_K8S_A100_80GB, _K8S_A100]
    placer = make_placer(
        {location: 0.0 for location in locations},
        placement_contract=placement_policy.resolve_fresh_contract(
            placement_policy.CAPACITY_AWARE_SPOT_PLACER, pool=False))
    if benched_at is not None:
        for location in locations:
            placer.location2status[location] = (
                spot_placer.LocationStatus.PREEMPTED)
            placer.location2preempted_at[location] = benched_at
    return placer


def _claim(*,
           holdings_fill,
           launchable,
           floor=10,
           weight=100.0,
           effective_cap=None):
    return allocation.ClaimInput(floor=floor,
                                 weight=weight,
                                 holdings_fill=holdings_fill,
                                 launchable=launchable,
                                 effective_cap=effective_cap)


class TestBenchedZeroCostTierBlocksRefill:
    """A benched zero-cost tier reports itself unlaunchable."""

    def test_full_cluster_bench_makes_the_claim_unlaunchable(self):
        # Every zero-cost location benched, as after a research job takes the
        # whole cluster and the fill launches fail against a full cluster.
        placer = _make_zero_cost_placer(benched_at=time.time())
        assert not reserved_capacity._placer_can_launch_zero_cost(placer)

    def test_expired_bench_restores_launchability(self):
        placer = _make_zero_cost_placer(benched_at=time.time() - 10_000)
        assert reserved_capacity._placer_can_launch_zero_cost(placer)


class TestFeedIgnoresObservedFreeCapacity:
    """The feed split gates on `launchable`, never on measured free slots."""

    def test_benched_claimant_is_fed_nothing_despite_ample_free_capacity(self):
        # The exact production shape on 2026-08-04: the broker measured 218
        # free GPUs and granted the service 228 slots, and it holds 1.
        claims = {'boltz-l4-fleet': _claim(holdings_fill=1, launchable=False)}
        feeds, _ = allocation.compute_feeds(observed_free=218,
                                            grants={'boltz-l4-fleet': 228},
                                            claims=claims,
                                            sticky_state={},
                                            now=time.time(),
                                            sticky_window_seconds=60.0)
        # Nothing is fed, so nothing launches, so the pool stays idle. The
        # measured 218 free slots never enter the decision.
        assert feeds['boltz-l4-fleet'] == 0

    def test_same_claim_is_fed_its_whole_grant_once_launchable(self):
        claims = {'boltz-l4-fleet': _claim(holdings_fill=1, launchable=True)}
        feeds, _ = allocation.compute_feeds(observed_free=218,
                                            grants={'boltz-l4-fleet': 228},
                                            claims=claims,
                                            sticky_state={},
                                            now=time.time(),
                                            sticky_window_seconds=60.0)
        # Bounded by observed free, not by the grant: 218 available, 227 needed.
        assert feeds['boltz-l4-fleet'] == 218

    def test_a_benched_peer_hands_its_share_to_a_launchable_one(self):
        # Production benched, the test overlay not: the test fleet absorbs the
        # pool. This is the 2026-08-04 round-4 allocation
        # (grants={'boltz-l4-fleet': 2, 'boltz-l4-fleet-test': 226}) reproduced
        # from the feed side.
        claims = {
            'boltz-l4-fleet': _claim(holdings_fill=1, launchable=False),
            'boltz-l4-fleet-test': _claim(holdings_fill=0,
                                          launchable=True,
                                          floor=0,
                                          weight=0.1),
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
        assert feeds['boltz-l4-fleet'] == 0
        # weight 0.1 vs 100 is irrelevant once the heavier claimant is benched.
        assert feeds['boltz-l4-fleet-test'] == 218


class TestProbeThrottleBoundsRefillRate:
    """Refill of an observable pool runs at the blind-probe rate."""

    def test_only_one_launch_per_ttl_window_per_location(self):
        # Bench expired, so both shapes are selectable again.
        placer = _make_zero_cost_placer(benched_at=time.time() - 10_000)
        assert reserved_capacity._placer_can_launch_zero_cost(placer)

        # A refill wave selects the shapes. Selection consumes the window's
        # single probe reservation per location.
        for location in (_K8S_A100_80GB, _K8S_A100):
            placer.reserve_retry(location)

        # Both are benched again for another full TTL, even though the pool
        # still has hundreds of measured free GPUs and the grant is unchanged.
        assert not reserved_capacity._placer_can_launch_zero_cost(placer)

    def test_refill_rate_is_ttl_bound_not_capacity_bound(self):
        """218 free GPUs refill at 2 replicas per TTL window, not 218."""
        placer = _make_zero_cost_placer(benched_at=time.time() - 10_000)
        launches = 0
        # Ten consecutive autoscaler ticks inside one TTL window.
        for _ in range(10):
            if not reserved_capacity._placer_can_launch_zero_cost(placer):
                continue
            for location in (_K8S_A100_80GB, _K8S_A100):
                if (placer._effective_status(location) ==
                        spot_placer.LocationStatus.ACTIVE):
                    placer.reserve_retry(location)
                    launches += 1
        # One probe per location per window: the measured free capacity does
        # not raise this, so draining 218 free slots takes ~109 TTL windows
        # (over 18 hours at the deployed 600s TTL).
        assert launches == 2

    def test_ttl_default_is_tuned_for_unobservable_spot_capacity(self):
        # The knob that bounds the refill is the spot-probe TTL, which exists
        # for capacity that cannot be measured. Pinning it here so a change to
        # the zero-cost path has to confront the coupling.
        assert spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT == 600


class TestObservedFreeCapacityShouldUnbenchTheZeroCostTier:
    """The behaviour the fill tier needs, which does not exist today.

    A Kubernetes fill location's free capacity is measured every broker round.
    When the measurement says slots are free, the placer should treat the
    location as available instead of rationing blind probes: there is nothing
    left to discover. Today no such path exists, so this asserts the gap.
    """

    def test_no_way_to_clear_a_bench_from_a_capacity_observation(self):
        placer = _make_zero_cost_placer(benched_at=time.time())
        # The broker has just measured free capacity for these very shapes.
        observed_free_by_location = {_K8S_A100_80GB: 165, _K8S_A100: 53}
        assert sum(observed_free_by_location.values()) == 218

        # Nothing on the placer consumes a capacity observation to release a
        # bench. set_active is keyed to a SUCCESSFUL LAUNCH, which the bench
        # is what prevents.
        assert not hasattr(placer, 'observe_free_capacity')
        assert not hasattr(placer, 'clear_bench_from_observation')

        # So the tier stays unlaunchable while the capacity sits free.
        assert not reserved_capacity._placer_can_launch_zero_cost(placer)


class TestBenchAlsoBlindsTheDemandPath:
    """`launchable` gates the capacity hint, not just the fill feed.

    `_pool_capacity_hint` holds the broker's freshly measured
    `last_observed_free` for the pool, and discards it when the tier is
    benched, collapsing the hint to current holdings. That hint is what
    sizes the autoscaler's per-card target, so one benched tier both
    stops the fill AND makes the demand path blind to the free GPUs --
    which is why an over-subscribed fleet targets `L4: 66, A100: 1`
    while 219 A100s sit free.
    """

    @staticmethod
    def _spec():
        return reserved_capacity.FillPoolSpec(
            position=0,
            context='prod_research_cluster_eks',
            shapes=(('A100-80GB', 1), ('A100', 1)),
            locations=(_K8S_A100_80GB, _K8S_A100),
            physical_cluster_uid='uid',
            pool_key='pool',
            legacy_pool_key='legacy')

    def test_launchable_tier_surfaces_the_measured_free_capacity(self):
        now = time.time()
        round_row = {'last_observed_free': 219, 'last_observed_free_ts': now}
        with mock.patch.object(reserved_capacity.serve_state,
                               'get_reserved_fill_round',
                               return_value=round_row):
            hint = reserved_capacity._pool_capacity_hint(self._spec(),
                                                         holdings=2,
                                                         launchable=True,
                                                         previous_cap=0,
                                                         now=now)
        assert hint == 221  # holdings + every measured free slot

    def test_benched_tier_discards_the_same_measurement(self):
        now = time.time()
        round_row = {'last_observed_free': 219, 'last_observed_free_ts': now}
        with mock.patch.object(reserved_capacity.serve_state,
                               'get_reserved_fill_round',
                               return_value=round_row):
            hint = reserved_capacity._pool_capacity_hint(self._spec(),
                                                         holdings=2,
                                                         launchable=False,
                                                         previous_cap=0,
                                                         now=now)
        # The 219 measured free slots are in hand and dropped: the autoscaler
        # is told this pool can hold exactly what it already holds.
        assert hint == 2

    def test_bench_costs_the_hint_the_entire_free_pool(self):
        now = time.time()
        round_row = {'last_observed_free': 219, 'last_observed_free_ts': now}
        with mock.patch.object(reserved_capacity.serve_state,
                               'get_reserved_fill_round',
                               return_value=round_row):
            launchable_hint = reserved_capacity._pool_capacity_hint(
                self._spec(), 2, True, 0, now)
            benched_hint = reserved_capacity._pool_capacity_hint(
                self._spec(), 2, False, 0, now)
        assert launchable_hint - benched_hint == 219


class TestPaidTierProbeThrottleIsCorrect:
    """The throttle must stay for spot: this is not a blanket removal."""

    def test_spot_location_still_gets_one_probe_per_window(self):
        aws_spot = make_location('us-east-1',
                                 accelerators={'L4': 1},
                                 use_spot=True,
                                 cloud_name='AWS')
        placer = make_placer({aws_spot: 1.0})
        placer.location2status[aws_spot] = (
            spot_placer.LocationStatus.PREEMPTED)
        placer.location2preempted_at = {aws_spot: time.time() - 10_000}

        assert (placer._effective_status(aws_spot) ==
                spot_placer.LocationStatus.ACTIVE)
        placer.reserve_retry(aws_spot)
        # Correct for spot: capacity is unobservable, so a burst must not pile
        # onto a region that may still be dry.
        assert (placer._effective_status(aws_spot) ==
                spot_placer.LocationStatus.PREEMPTED)


class TestSelectionGateBoundsTheBatch:
    """A fixed feed alone cannot refill: selection re-benches per launch.

    `select_next_zero_cost_location` filters to effectively-ACTIVE zero-cost
    locations and consumes the TTL probe on the way out, so the second fill
    launch of the same wave already finds the location benched. The feed can
    say 218; the batch that actually launches is one per location.
    """

    def test_a_218_slot_feed_yields_one_launch_per_location(self):
        placer = _make_zero_cost_placer(benched_at=time.time() - 10_000)
        placer.location2cost = {_K8S_A100_80GB: 0.0, _K8S_A100: 0.0}

        selected = []
        # One fill wave sized by a 218-slot feed.
        for _ in range(218):
            location = placer.select_next_zero_cost_location()
            if location is None:
                break
            selected.append(location)

        assert len(selected) == 2
        assert set(selected) == {_K8S_A100_80GB, _K8S_A100}
        # Everything after is refused for the rest of the TTL window.
        assert placer.select_next_zero_cost_location() is None

    def test_unbenched_locations_serve_the_whole_wave(self):
        # Same wave against locations that were never benched: selection is
        # unbounded, which is what the fill tier is supposed to do.
        placer = _make_zero_cost_placer(benched_at=None)
        placer.location2cost = {_K8S_A100_80GB: 0.0, _K8S_A100: 0.0}
        selected = [placer.select_next_zero_cost_location() for _ in range(218)]
        assert all(location is not None for location in selected)
