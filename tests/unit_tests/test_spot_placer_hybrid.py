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
        # image_id/disk_tier are always present (None clears): a location
        # without them must strip them from every copied entry — e.g. a
        # VM-selected launch clearing the k8s entry's docker image.
        assert 'image_id' in d and d['image_id'] is None
        assert 'disk_tier' in d and d['disk_tier'] is None
        # accelerators likewise unconditional: a CPU-only location must
        # strip GPU entries' accelerators from its copies.
        cpu_only = _make_location('cpu-region', accelerators=None)
        d_cpu = cpu_only.to_dict()
        assert 'accelerators' in d_cpu and d_cpu['accelerators'] is None

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


class TestLegacyLocationResolution:
    """Pre-upgrade shape-less locations resolve onto shape-bearing keys."""

    def test_set_preemptive_resolves_by_region(self, monkeypatch):
        monkeypatch.setattr(spot_placer.time, 'time', lambda: 1000.0)
        keyed = _make_location('us-east-1', accelerators={'L4': 1})
        other = _make_location('us-east-2', accelerators={'L4': 1})
        placer = _make_placer([keyed, other], {keyed: 0.2, other: 0.2})
        # Shape-less location, as deserialized from a pre-upgrade row.
        placer.set_preemptive(
            spot_placer.Location(cloud=keyed.cloud,
                                 region='us-east-1',
                                 zone=None))
        assert keyed in placer.preemptive_locations()

    def test_unknown_location_is_ignored_not_asserted(self):
        keyed = _make_location('us-east-1', accelerators={'L4': 1})
        placer = _make_placer([keyed], {keyed: 0.2})
        stranger = _make_location('mars-central-1')
        # Must not raise.
        placer.set_preemptive(stranger)
        placer.set_active(stranger)
        assert keyed in placer.active_locations()

    def test_load_counting_resolves_legacy_rows(self):
        keyed = _make_location('us-east-1', accelerators={'L4': 1})
        other = _make_location('us-east-2', accelerators={'L4': 1})
        placer = _make_placer([keyed, other], {keyed: 0.2, other: 0.2})
        legacy = spot_placer.Location(cloud=keyed.cloud,
                                      region='us-east-1',
                                      zone=None)
        # One legacy replica in us-east-1: the next launch must go to
        # the (unloaded) other region, proving the legacy row counted.
        assert placer.select_next_location([legacy]) == other


class TestMixedValidation:
    """validate_service_task accepts placer-managed mixed sets only."""

    def _task(self, spot_placer_value, k8s_spot):
        import textwrap

        import sky
        yaml_str = textwrap.dedent(f"""
            resources:
              cpus: 2+
              ports: 8080
              any_of:
                - infra: aws/us-east-1
                  accelerators: L4:1
                  use_spot: true
                - infra: aws/us-east-2
                  accelerators: A10G:1
                  use_spot: {str(k8s_spot).lower()}
            service:
              readiness_probe: /health
              replica_policy:
                min_replicas: 1
                max_replicas: 2
                target_qps_per_replica: 0.1
                {'spot_placer: ' + spot_placer_value if spot_placer_value else ''}
            run: echo hi
            """)
        return sky.Task.from_yaml_str(yaml_str)

    def test_mixed_with_placer_accepted(self):
        from sky.serve import serve_utils
        serve_utils.validate_service_task(self._task('dynamic_fallback',
                                                     k8s_spot=False),
                                          pool=False)

    def test_mixed_without_placer_rejected(self):
        from sky.serve import serve_utils
        with pytest.raises(ValueError, match='all use spot'):
            serve_utils.validate_service_task(self._task(None, k8s_spot=False),
                                              pool=False)

    def test_placer_with_no_spot_at_all_rejected(self):
        import textwrap

        import sky
        from sky.serve import serve_utils
        yaml_str = textwrap.dedent("""
            resources:
              cpus: 2+
              ports: 8080
              accelerators: L4:1
              use_spot: false
              infra: aws/us-east-1
            service:
              readiness_probe: /health
              replica_policy:
                min_replicas: 1
                max_replicas: 2
                target_qps_per_replica: 0.1
                spot_placer: dynamic_fallback
            run: echo hi
            """)
        with pytest.raises(ValueError, match='at least one spot'):
            serve_utils.validate_service_task(sky.Task.from_yaml_str(yaml_str),
                                              pool=False)


class TestReservedFillSinglePoolValidation:
    """validate_service_task rejects fill specs spanning >1 k8s pool.

    The broker's v1 arbitration supports exactly one (context, GPU) pool
    per service; the runtime cycle rejects violations too, but only via
    controller error logs, so misconfigurations must also fail at submit
    time. All Kubernetes entries are treated as candidate pool shapes
    (zero-cost-ness is not knowable client-side).
    """

    def _task(self, k8s_entries):
        # pylint: disable=import-outside-toplevel
        import sky
        entries = '\n'.join(f'    - infra: k8s/{ctx}\n'
                            f'      accelerators: {gpu}:1'
                            for ctx, gpu in k8s_entries)
        yaml_str = f"""
resources:
  cpus: 2+
  ports: 8080
  any_of:
{entries}
service:
  readiness_probe: /health
  replica_policy:
    min_replicas: 1
    max_replicas: 4
    target_qps_per_replica: 0.1
    reserved_capacity_fill: true
run: echo hi
"""
        return sky.Task.from_yaml_str(yaml_str)

    def test_two_distinct_pools_rejected(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        for entries in (
            [('ctx-a', 'A100'), ('ctx-a', 'H100')],  # same ctx, two GPUs
            [('ctx-a', 'A100'), ('ctx-b', 'A100')],  # two contexts, one GPU
        ):
            with pytest.raises(ValueError, match='exactly one Kubernetes'):
                serve_utils.validate_service_task(self._task(entries),
                                                  pool=False)

    def test_single_pool_accepted(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        serve_utils.validate_service_task(self._task([('ctx-a', 'A100')]),
                                          pool=False)
        # Same pool enumerated case-insensitively still counts once.
        serve_utils.validate_service_task(self._task([('ctx-a', 'A100'),
                                                      ('ctx-a', 'a100')]),
                                          pool=False)


class TestImageIdNormalizationEndToEnd:
    """Enumerated locations must carry region-independent image dicts."""

    def test_enumeration_normalizes_image_id(self):
        # pylint: disable=import-outside-toplevel
        import sky
        t = sky.Task.from_yaml_str("""
resources:
  cpus: 4+
  ports: 8080
  any_of:
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
      image_id: docker:myrepo/model:v1
run: echo hi
""")
        locs = spot_placer._get_possible_location_from_task(t)
        assert locs, 'expected at least one location'
        for loc in locs:
            assert loc.image_id == {None: 'docker:myrepo/model:v1'}, loc


class TestDiskTierPerLocation:
    """disk_tier is a per-location attribute: VM entries keep 'high'
    without breaking uniformity against k8s entries that reject it."""

    def test_mixed_disk_tier_enumerates(self):
        # pylint: disable=import-outside-toplevel
        import sky
        t = sky.Task.from_yaml_str("""
resources:
  cpus: 4+
  ports: 8080
  any_of:
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
      disk_tier: high
    - infra: aws/us-east-2
      accelerators: A10G:1
      use_spot: false
run: echo hi
""")
        locs = spot_placer._get_possible_location_from_task(t)
        l4 = [l for l in locs if 'L4' in (l.accelerators or {})]
        a10g = [l for l in locs if 'A10G' in (l.accelerators or {})]
        assert l4 and all(l.disk_tier == 'high' for l in l4)
        assert a10g and all(l.disk_tier is None for l in a10g)
        # The launch override pins OR CLEARS the tier: a tier-less
        # location must strip disk_tier from VM-originated entries when
        # the override is applied across the whole any_of set (a
        # Kubernetes copy with disk_tier=high fails validation).
        assert l4[0].to_dict()['disk_tier'] == 'high'
        assert a10g[0].to_dict()['disk_tier'] is None

    def test_shape_passed_to_feasibility_has_no_leaked_attrs(
            self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import aws as aws_cloud
        from sky.utils import resources_utils

        captured = []

        def _capture(self, resources, num_nodes=1):
            del self, num_nodes
            captured.append(resources)
            return resources_utils.FeasibleResources([], [], None)

        monkeypatch.setattr(aws_cloud.AWS, 'get_feasible_launchable_resources',
                            _capture)
        t = sky.Task.from_yaml_str("""
resources:
  cpus: 4+
  ports: 8080
  any_of:
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
      disk_tier: high
      image_id: docker:myrepo/model:v1
    - infra: aws/us-east-2
      accelerators: A10G:1
      use_spot: false
run: echo hi
""")
        spot_placer._get_possible_location_from_task(t)
        by_acc = {list(r.accelerators)[0]: r for r in captured}
        assert by_acc['L4'].disk_tier is not None
        assert by_acc['L4'].image_id is not None
        # The tier-less/image-less entry must not inherit the other
        # entry's disk_tier or image_id in the feasibility shape: clouds
        # that reject those attributes would silently drop the location.
        assert by_acc['A10G'].disk_tier is None
        assert by_acc['A10G'].image_id is None

    def test_pickle_roundtrip_and_backcompat(self):
        with mock.patch.object(spot_placer.registry.CLOUD_REGISTRY,
                               'from_str',
                               return_value=mock.MagicMock()):
            loc = spot_placer.Location.from_pickleable({
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': None,
                'disk_tier': 'high',
            })
            assert loc.disk_tier == 'high'
            old = spot_placer.Location.from_pickleable({
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': None,
            })
            assert old.disk_tier is None
