"""A full reserved pool must stay reclaimable by weight and by floor.

The per-pool capacity hint becomes each claimant's ``effective_cap``, and
``compute_entitlements`` clamps both the weighted share and the retained floor
by it. Reporting ``own holdings + observed free`` therefore made a full pool
self-locking: with free at zero every claimant's cap collapsed to exactly what
it already held, so the whole-pool allocation the module documents could not
move a single slot.

Measured in production on one A100 pool, across seven consecutive broker
rounds with no movement::

    grants   prod=2   test=63     free=0  sum_holdings=65
    weights  prod=100 test=0.1    prod floor_replicas=10

A 1000:1 weight ratio changed nothing and production's floor of ten was
clamped to its actual two. Deleting the incumbent's Pods by hand moved
production's grant to 57 on the very next round, which is what proved the
arbitration itself was healthy and the cap was the binding constraint.
"""
# pylint: disable=protected-access
import time
from unittest import mock

from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_allocation as allocation

_POOL = 'pool'


def _spec():
    return reserved_capacity.FillPoolSpec(position=0,
                                          context='prod_research_cluster_eks',
                                          shapes=(('A100', 1),),
                                          locations=(),
                                          physical_cluster_uid='uid',
                                          pool_key=_POOL,
                                          legacy_pool_key='legacy')


def _hint(holdings, free, sum_holdings=None, now=None, launchable=True):
    now = time.time() if now is None else now
    row = {'last_observed_free': free, 'last_observed_free_ts': now}
    if sum_holdings is not None:
        row['sum_holdings'] = sum_holdings
    return reserved_capacity._pool_capacity_hint(_spec(),
                                                 holdings=holdings,
                                                 launchable=launchable,
                                                 previous_cap=0,
                                                 now=now,
                                                 round_row=row)


class TestAFullPoolStaysReclaimable:
    """The production shape: a full pool, an incumbent, and a starved peer."""

    def test_the_starved_peer_sees_the_whole_pool(self):
        # 2 held of a 65-slot pool with nothing free. The old hint was 2,
        # which is what pinned its entitlement to its own occupancy.
        assert _hint(holdings=2, free=0, sum_holdings=65) == 65

    def test_the_incumbent_sees_the_same_pool(self):
        assert _hint(holdings=63, free=0, sum_holdings=65) == 65

    def test_free_capacity_still_adds_on_top(self):
        assert _hint(holdings=2, free=8, sum_holdings=65) == 73


class TestTheHintNeverNarrows:
    """sum_holdings is one round old; it must not cost anyone ground."""

    def test_a_stale_total_below_local_holdings_is_ignored(self):
        # This claimant grew to 40 since the round was published at 10.
        assert _hint(holdings=40, free=0, sum_holdings=10) == 40

    def test_a_legacy_row_without_the_total_is_unchanged(self):
        # Pre-existing behaviour, and what the older suite pins: 2 + 219.
        assert _hint(holdings=2, free=219) == 221

    def test_a_malformed_total_falls_back_instead_of_raising(self):
        for bad in ('65', -1, True, None, 1.5):
            assert _hint(holdings=2, free=219, sum_holdings=bad) == 221

    def test_widening_is_monotonic_across_the_grid(self):
        now = time.time()
        for holdings in (0, 1, 2, 40, 63):
            for free in (0, 1, 8, 219):
                for total in (0, 1, 65, 500):
                    new = _hint(holdings, free, total, now=now)
                    old = holdings + free
                    assert new >= old, (holdings, free, total, new, old)


class TestTheOtherBranchesAreUntouched:
    """Only the fresh-observation branch changes."""

    def test_an_unlaunchable_pool_still_reports_only_its_holdings(self):
        assert _hint(holdings=2, free=219, sum_holdings=65,
                     launchable=False) == 2

    def test_a_pool_with_no_observation_still_probes_by_one(self):
        hint = reserved_capacity._pool_capacity_hint(_spec(),
                                                     holdings=2,
                                                     launchable=True,
                                                     previous_cap=0,
                                                     now=time.time(),
                                                     round_row={})
        assert hint == 3

    def test_a_stale_observation_still_carries_the_previous_cap(self):
        now = time.time()
        stale = now - reserved_capacity.poll_interval_seconds() * 1000
        row = {
            'last_observed_free': 219,
            'last_observed_free_ts': stale,
            'sum_holdings': 65,
        }
        hint = reserved_capacity._pool_capacity_hint(_spec(),
                                                     holdings=2,
                                                     launchable=True,
                                                     previous_cap=9,
                                                     now=now,
                                                     round_row=row)
        assert hint == 9

    def test_a_missing_round_row_is_read_from_the_store(self):
        now = time.time()
        row = {
            'last_observed_free': 4,
            'last_observed_free_ts': now,
            'sum_holdings': 65,
        }
        with mock.patch.object(reserved_capacity.serve_state,
                               'get_reserved_fill_round',
                               return_value=row):
            hint = reserved_capacity._pool_capacity_hint(_spec(),
                                                         holdings=2,
                                                         launchable=True,
                                                         previous_cap=0,
                                                         now=now)
        assert hint == 69


def _claims(prod_cap, test_cap):
    """The measured production standoff, parameterized by the cap under test."""
    return {
        'boltz-l4-fleet': allocation.ClaimInput(floor=10,
                                                weight=100.0,
                                                holdings_fill=2,
                                                launchable=True,
                                                effective_cap=prod_cap),
        'boltz-l4-fleet-test': allocation.ClaimInput(floor=0,
                                                     weight=0.1,
                                                     holdings_fill=63,
                                                     launchable=True,
                                                     effective_cap=test_cap),
    }


class TestTheReclaimSignalActuallyAppears:
    """The point of the change: the allocator must be able to move a slot.

    Static caps are not the deliverable. What matters is that the incumbent's
    entitlement drops BELOW its holdings, because that is the signal it scales
    down on, and that the starved peer rises ABOVE its own.
    """

    _TOTAL = 65  # observed free 0 + 65 held, the whole pool

    def test_the_old_cap_reproduces_the_deadlock(self):
        # Exactly what production published: each cap is its own occupancy.
        entitlements = allocation.compute_entitlements(
            self._TOTAL, _claims(prod_cap=2, test_cap=63))
        assert entitlements['boltz-l4-fleet'] == 2
        assert entitlements['boltz-l4-fleet-test'] == 63

    def test_the_old_cap_defeats_the_floor(self):
        entitlements = allocation.compute_entitlements(
            self._TOTAL, _claims(prod_cap=2, test_cap=63))
        # floor_replicas=10 is not honoured: a floor that cannot be reclaimed
        # reserves nothing.
        assert entitlements['boltz-l4-fleet'] < 10

    def test_the_new_cap_restores_the_floor(self):
        entitlements = allocation.compute_entitlements(
            self._TOTAL, _claims(prod_cap=self._TOTAL, test_cap=self._TOTAL))
        assert entitlements['boltz-l4-fleet'] >= 10

    def test_the_new_cap_tells_the_incumbent_to_release(self):
        entitlements = allocation.compute_entitlements(
            self._TOTAL, _claims(prod_cap=self._TOTAL, test_cap=self._TOTAL))
        # Below its 63 holdings, which is the scale-down signal.
        assert entitlements['boltz-l4-fleet-test'] < 63
        # And weight finally applies: 100 against 0.1.
        assert (entitlements['boltz-l4-fleet']
                > entitlements['boltz-l4-fleet-test'])

    def test_the_pool_is_never_oversubscribed(self):
        for caps in ((2, 63), (self._TOTAL, self._TOTAL)):
            entitlements = allocation.compute_entitlements(
                self._TOTAL, _claims(*caps))
            assert sum(entitlements.values()) <= self._TOTAL

    def test_an_equal_weight_peer_does_not_strip_the_incumbent(self):
        # Reclaim must follow weight, not merely punish whoever holds slots.
        claims = {
            'a': allocation.ClaimInput(floor=0,
                                       weight=1.0,
                                       holdings_fill=63,
                                       launchable=True,
                                       effective_cap=self._TOTAL),
            'b': allocation.ClaimInput(floor=0,
                                       weight=1.0,
                                       holdings_fill=2,
                                       launchable=True,
                                       effective_cap=self._TOTAL),
        }
        entitlements = allocation.compute_entitlements(self._TOTAL, claims)
        # 65 is odd, so an exact tie is impossible; largest-remainder gives
        # 33/32. What matters is that holding 63 buys no advantage.
        assert abs(entitlements['a'] - entitlements['b']) <= 1
