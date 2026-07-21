"""Benched spot locations must become retryable after a TTL.

Live finding (2026-07-06, 1000-replica L4 fleet): a location marked
PREEMPTED — including on transient quota errors like
MaxSpotInstanceCountExceeded — was NEVER selected again: selection draws
only from ACTIVE locations, and reactivation required a successful launch
there, which could never happen. Freed quota or recovered capacity was
therefore never picked up. The TTL decay retries each benched location
with one probe launch per window.
"""
# pylint: disable=redefined-outer-name,protected-access,unused-variable
import pytest
from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky.serve import spot_placer


@pytest.fixture
def placer_and_locations():
    cheap = make_location('seoul')
    other = make_location('oregon')
    third = make_location('iowa')
    placer = make_placer({
        cheap: 1.0,
        other: 2.0,
        third: 3.0,
    })
    return placer, cheap, other, third


class TestPreemptionTtlRetry:
    """TTL decay of PREEMPTED marks."""

    def test_benched_within_ttl_stays_excluded(self, placer_and_locations,
                                               monkeypatch):
        placer, cheap, other, third = placer_and_locations
        monkeypatch.setattr(spot_placer.time, 'time', lambda: 1000.0)
        placer.set_preemptive(cheap)
        assert cheap not in placer.active_locations()
        assert cheap in placer.preemptive_locations()

    def test_benched_past_ttl_becomes_active_again(self, placer_and_locations,
                                                   monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert cheap in placer.active_locations()
        assert cheap not in placer.preemptive_locations()
        # And it is selectable again as the cheapest active location.
        assert placer.select_next_location() == cheap

    def test_failed_retry_rebenches_for_full_ttl(self, placer_and_locations,
                                                 monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert cheap in placer.active_locations()
        # Retry launch fails -> benched again for a fresh window.
        placer.set_preemptive(cheap)
        assert cheap not in placer.active_locations()
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT - 10
        assert cheap not in placer.active_locations()
        now[0] += 11
        assert cheap in placer.active_locations()

    def test_successful_retry_clears_the_mark(self, placer_and_locations,
                                              monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        placer.set_active(cheap)
        assert cheap in placer.active_locations()
        assert cheap not in placer.location2preempted_at

    def test_older_success_cannot_clear_newer_failure(self,
                                                      placer_and_locations,
                                                      monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])

        # The successful launch was selected before this newer failure.
        selected_at = now[0]
        now[0] += 1
        placer.set_preemptive(cheap)
        placer.set_active(cheap, selected_at=selected_at)

        assert cheap not in placer.active_locations()
        assert cheap in placer.preemptive_locations()

        # A later retry was selected after the bench, so its success is fresh
        # evidence that the location can be reactivated immediately.
        now[0] += 1
        placer.set_active(cheap, selected_at=now[0])
        assert cheap in placer.active_locations()
        assert cheap not in placer.preemptive_locations()

    def test_total_exhaustion_reset(self, placer_and_locations, monkeypatch):
        """The global reset fires only when NOTHING is selectable —
        a single remaining active location must stay the fallback
        (the old <2 threshold un-benched a full zero-cost pool and
        re-selected it forever instead of spilling to the paid tier)."""
        placer, cheap, other, third = placer_and_locations
        monkeypatch.setattr(spot_placer.time, 'time', lambda: 1000.0)
        placer.set_preemptive(cheap)
        placer.set_preemptive(other)
        # One active location left: NO reset — it serves as fallback.
        assert placer.active_locations() == [third]
        # Benching the last one leaves nothing selectable -> reset.
        placer.set_preemptive(third)
        assert len(placer.active_locations()) == 3
        assert not placer.location2preempted_at

    def test_selection_consumes_the_retry_budget(self, placer_and_locations,
                                                 monkeypatch):
        """A burst of selections within one window must send exactly ONE
        probe launch to a benched location, not pile onto it."""
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        # First selection picks the expired-benched cheapest location...
        assert placer.select_next_location() == cheap
        # ...and consumes its retry: subsequent selections in the same
        # burst must go elsewhere until the next window.
        assert cheap not in placer.active_locations()
        assert placer.select_next_location() == other
        assert placer.select_next_location() == other
        # Next window: retryable again.
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert placer.select_next_location() == cheap

    def test_consumed_retry_remains_admissible_until_newer_failure(
            self, placer_and_locations, monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])

        assert placer.is_launch_admissible(other, selected_at=None)
        placer.set_preemptive(cheap)
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert placer.select_next_location() == cheap
        selected_at = now[0]

        # Selection consumed the retry, so the location is benched again.
        assert not placer.is_active_location(cheap)
        assert placer.is_launch_admissible(cheap, selected_at=selected_at)
        assert not placer.is_launch_admissible(cheap, selected_at=None)

        # A subsequent failure is newer than this queued placement and wins.
        now[0] += 1
        placer.set_preemptive(cheap)
        assert not placer.is_launch_admissible(cheap, selected_at=selected_at)

    def test_env_override_ttl(self, placer_and_locations, monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        monkeypatch.setenv(spot_placer._PREEMPTION_RETRY_SECONDS_ENV_VAR, '60')
        placer.set_preemptive(cheap)
        now[0] += 61
        assert cheap in placer.active_locations()

    def test_snapshot_does_not_consume_retry(self, placer_and_locations,
                                             monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        benched_at = placer.location2preempted_at[cheap]
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1

        snapshot = placer.placement_snapshot()

        location = next(item for item in snapshot['locations']
                        if item['region'] == cheap.region)
        assert location['stored_status'] == 'PREEMPTED'
        assert location['effective_status'] == 'ACTIVE'
        assert location['probe_eligible'] is True
        assert location['benched_at'] == benched_at
        assert placer.location2preempted_at[cheap] == benched_at
        # The first real selection still gets the one probe.
        assert placer.select_next_location() == cheap

    def test_snapshot_uses_only_cached_prices(self, placer_and_locations,
                                              monkeypatch):
        placer, cheap, other, third = placer_and_locations
        monkeypatch.setattr(
            placer, '_get_cost_per_hour_cached',
            lambda _: pytest.fail('snapshot must not look up catalog prices'))

        snapshot = placer.placement_snapshot()

        prices = {
            item['region']: item['cached_hourly_cost']
            for item in snapshot['locations']
        }
        assert prices == {'seoul': 1.0, 'oregon': 2.0, 'iowa': 3.0}
