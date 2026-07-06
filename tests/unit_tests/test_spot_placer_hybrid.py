"""Heterogeneous (hybrid) spot placement: shapes per location + zero-cost tier.

Phase-2 of the research-cluster scavenger design: one any_of set mixes
zero-cost reserved capacity (Kubernetes pool, non-spot, A100) with paid
cloud spot (L4). The placer must fill the zero-cost tier COMPLETELY
before spending on spot, pin each launch's accelerators/use_spot via its
location, and pull load back when reserved capacity frees (TTL retry).
"""
# pylint: disable=redefined-outer-name,protected-access,unused-variable
from unittest import mock

import pytest

from sky.serve import spot_placer


def _make_location(region, accelerators=None, use_spot=True):
    cloud = mock.MagicMock()
    cloud.is_same_cloud = lambda other: str(other) == str(cloud)
    return spot_placer.Location(cloud=cloud,
                                region=region,
                                zone=None,
                                accelerators=accelerators,
                                use_spot=use_spot)


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
def hybrid_placer():
    k8s = _make_location('research-ctx',
                         accelerators={'A100': 1},
                         use_spot=False)
    cheap_spot = _make_location('us-east-1',
                                accelerators={'L4': 1},
                                use_spot=True)
    pricey_spot = _make_location('eu-west-3',
                                 accelerators={'L4': 1},
                                 use_spot=True)
    placer = _make_placer([k8s, cheap_spot, pricey_spot], {
        k8s: 0.0,
        cheap_spot: 0.2,
        pricey_spot: 0.3,
    })
    return placer, k8s, cheap_spot, pricey_spot


class TestZeroCostTierFirst:
    """Free capacity fills completely before any paid launch."""

    def test_zero_cost_wins_even_when_loaded(self, hybrid_placer):
        placer, k8s, cheap_spot, pricey_spot = hybrid_placer
        # k8s already hosts 50 replicas, spot none: k8s STILL wins —
        # only a launch failure (bench) moves load to paid tier.
        current = [k8s] * 50
        assert placer.select_next_location(current) == k8s

    def test_benched_zero_cost_falls_back_to_cheapest_spot(
            self, hybrid_placer, monkeypatch):
        placer, k8s, cheap_spot, pricey_spot = hybrid_placer
        monkeypatch.setattr(spot_placer.time, 'time', lambda: 1000.0)
        placer.set_preemptive(k8s)  # cluster full -> launch failed
        assert placer.select_next_location([]) == cheap_spot

    def test_ttl_retry_pulls_back_to_zero_cost(self, hybrid_placer,
                                               monkeypatch):
        placer, k8s, cheap_spot, pricey_spot = hybrid_placer
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(k8s)
        assert placer.select_next_location([]) == cheap_spot
        # Capacity frees; after the TTL the zero-cost tier is probed
        # again and immediately preferred.
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert placer.select_next_location([cheap_spot] * 10) == k8s


class TestHeterogeneousLocations:
    """Locations are distinct per shape and carry launch overrides."""

    def test_same_region_different_shape_are_distinct(self):
        a = _make_location('us-east-1', accelerators={'L4': 1})
        b = _make_location('us-east-1', accelerators={'L4': 4})
        assert a != b
        assert len({a, b}) == 2

    def test_to_dict_carries_shape_and_spotness(self):
        k8s = _make_location('ctx', accelerators={'A100': 1}, use_spot=False)
        d = k8s.to_dict()
        assert d['accelerators'] == {'A100': 1}
        assert d['use_spot'] is False

    def test_pickleable_roundtrip_and_backcompat(self):
        with mock.patch.object(spot_placer.registry.CLOUD_REGISTRY,
                               'from_str',
                               return_value=mock.MagicMock()):
            loc = spot_placer.Location.from_pickleable({
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': 'us-east-1a',
                'accelerators': {
                    'L4': 1
                },
                'use_spot': False,
            })
            assert loc.accelerators == {'L4': 1}
            assert loc.use_spot is False
            # Rows pickled before this change carry neither key.
            old = spot_placer.Location.from_pickleable({
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': None,
            })
            assert old.accelerators is None
            assert old.use_spot is True


class TestShouldUseSpotMixed:
    """Mixed spot/non-spot any_of engages the placer (ANY semantics)."""

    def test_mixed_any_of(self):
        # pylint: disable=import-outside-toplevel
        # Imported here: replica_managers pulls the full backend graph,
        # which the pure-placer tests above should not pay for.
        from sky.serve import replica_managers
        yaml_mixed = """
resources:
  cpus: 2+
  any_of:
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
    - infra: aws/us-east-2
      accelerators: A10G:1
      use_spot: false
run: echo hi
"""
        assert replica_managers._should_use_spot(yaml_mixed, None) is True
        yaml_none = yaml_mixed.replace('use_spot: true', 'use_spot: false')
        assert replica_managers._should_use_spot(yaml_none, None) is False
        # Override still wins.
        assert replica_managers._should_use_spot(yaml_none,
                                                 {'use_spot': True}) is True
