"""DynamicFallbackSpotPlacer must spread load, not pin to the cheapest zone.

Observed live at fleet scale: once every candidate location hosted at
least one replica, the min-cost selection pinned every subsequent launch
to the single cheapest active location (>1000 consecutive failed spot
attempts in one exhausted zone while other zones/clouds sat idle).
Selection is now least-loaded-first with cost as the tiebreak, which is
identical to the old prefer-unused behavior while any location is free.
"""
# pylint: disable=redefined-outer-name,unused-variable
import pytest
from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky.serve import spot_placer


def _make_location(name: str) -> spot_placer.Location:
    """'cloud/region-zone' shorthand for a str-identified mock cloud."""
    cloud_name, region_zone = name.split('/', maxsplit=1)
    return make_location(region_zone, cloud_name=cloud_name)


@pytest.fixture
def three_zone_placer():
    cheap = _make_location('aws/us-east-2c')
    mid = _make_location('aws/us-west-2a')
    pricey = _make_location('gcp/us-central1-a')
    placer = make_placer({
        cheap: 1.0,
        mid: 2.0,
        pricey: 3.0,
    })
    return placer, cheap, mid, pricey


class TestLeastLoadedSelection:
    """select_next_location load/cost ordering contract."""

    def test_unused_location_still_preferred_by_cost(self, three_zone_placer):
        placer, cheap, mid, pricey = three_zone_placer
        # cheap is used once; mid and pricey are free -> cheapest free wins.
        assert placer.select_next_location([cheap]) == mid

    def test_all_used_selects_least_loaded_not_cheapest(self,
                                                        three_zone_placer):
        placer, cheap, mid, pricey = three_zone_placer
        # cheap already hosts 3 replicas, mid 2, pricey 1: the old
        # behavior returned `cheap` here (global min cost), re-hammering
        # the most-loaded zone. Least-loaded must win.
        current = [cheap, cheap, cheap, mid, mid, pricey]
        assert placer.select_next_location(current) == pricey

    def test_load_tie_broken_by_cost(self, three_zone_placer):
        placer, cheap, mid, pricey = three_zone_placer
        # All equally loaded -> cheapest.
        current = [cheap, mid, pricey]
        assert placer.select_next_location(current) == cheap

    def test_empty_current_locations_selects_cheapest(self, three_zone_placer):
        placer, cheap, mid, pricey = three_zone_placer
        assert placer.select_next_location([]) == cheap

    def test_preempted_location_excluded(self, three_zone_placer):
        placer, cheap, mid, pricey = three_zone_placer
        placer.set_preemptive(cheap)
        # cheap is preempted; mid/pricey tie at load 0 -> mid (cheaper).
        assert placer.select_next_location([]) == mid
