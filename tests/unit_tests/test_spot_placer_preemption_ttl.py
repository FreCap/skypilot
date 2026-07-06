"""Benched spot locations must become retryable after a TTL.

Live finding (2026-07-06, 1000-replica L4 fleet): a location marked
PREEMPTED — including on transient quota errors like
MaxSpotInstanceCountExceeded — was NEVER selected again: selection draws
only from ACTIVE locations, and reactivation required a successful launch
there, which could never happen. Freed quota or recovered capacity was
therefore never picked up. The TTL decay retries each benched location
with one probe launch per window.
"""
# pylint: disable=redefined-outer-name,protected-access
from unittest import mock

import pytest

from sky.serve import spot_placer


def _make_location(name: str) -> spot_placer.Location:
    cloud = mock.MagicMock()
    cloud.is_same_cloud = lambda other: str(other) == str(cloud)
    return spot_placer.Location(cloud=cloud, region=name, zone=None)


def _make_placer(locations, costs):
    placer = spot_placer.DynamicFallbackSpotPlacer.__new__(
        spot_placer.DynamicFallbackSpotPlacer)
    placer.location2status = {
        loc: spot_placer.LocationStatus.ACTIVE for loc in locations
    }
    placer.location2preempted_at = {}
    placer.location2cost = dict(costs)
    return placer


@pytest.fixture
def placer_and_locations():
    cheap = _make_location('seoul')
    other = _make_location('oregon')
    third = _make_location('iowa')
    placer = _make_placer([cheap, other, third], {
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
        # And it is selectable again (cheapest of the equally-unloaded).
        assert placer.select_next_location([]) == cheap

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

    def test_fewer_than_two_active_reset_still_works(self, placer_and_locations,
                                                     monkeypatch):
        placer, cheap, other, third = placer_and_locations
        monkeypatch.setattr(spot_placer.time, 'time', lambda: 1000.0)
        placer.set_preemptive(cheap)
        # Benching the second leaves <2 active -> global reset fires.
        placer.set_preemptive(other)
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
        assert placer.select_next_location([]) == cheap
        # ...and consumes its retry: subsequent selections in the same
        # burst must go elsewhere until the next window.
        assert cheap not in placer.active_locations()
        assert placer.select_next_location([]) == other
        assert placer.select_next_location([]) == other
        # Next window: retryable again.
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert placer.select_next_location([]) == cheap

    def test_env_override_ttl(self, placer_and_locations, monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        monkeypatch.setenv(spot_placer._PREEMPTION_RETRY_SECONDS_ENV_VAR, '60')
        placer.set_preemptive(cheap)
        now[0] += 61
        assert cheap in placer.active_locations()
