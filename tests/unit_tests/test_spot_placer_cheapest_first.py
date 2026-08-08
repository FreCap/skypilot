"""SpotPlacer fills the cheapest usable location first."""
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


class TestCheapestFirstSelection:
    """select_next_location cost and fallback ordering contract."""

    def test_reuses_cheapest_location(self, three_zone_placer):
        placer, cheap, mid, pricey = three_zone_placer
        assert placer.select_next_location() == cheap
        assert placer.select_next_location() == cheap

    def test_preempted_cheapest_falls_through(self, three_zone_placer):
        placer, cheap, mid, pricey = three_zone_placer
        placer.set_preemptive(cheap)
        assert placer.select_next_location() == mid

        placer.set_preemptive(mid)
        assert placer.select_next_location() == pricey
