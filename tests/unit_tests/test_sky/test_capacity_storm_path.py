"""Regression tests for the normal AWS Spot capacity-storm path."""
# pylint: disable=protected-access
from typing import List, Tuple

import sqlalchemy

from sky import clouds
from sky import resources as resources_lib
from sky.backends import cloud_vm_ray_backend as backend
from sky.provision import capacity_cache
from sky.utils import resources_utils
from sky.utils.db import kv_cache

_REGION = 'us-east-1'
_ZONE_NAMES = ('us-east-1a', 'us-east-1b', 'us-east-1c')
_INSTANCE_TYPE = 'g6.4xlarge'
_ACCOUNT = '123456789012'
_SpotAttempt = Tuple[resources_lib.Resources, List[clouds.Zone]]


def _region_with_zones() -> clouds.Region:
    return clouds.Region(_REGION).set_zones(
        [clouds.Zone(name) for name in _ZONE_NAMES])


def _normal_spot_attempts(monkeypatch) -> List[_SpotAttempt]:
    """Returns the optimizer-to-provisioner attempts for the incident shape."""

    def _validate_region_zone(cls, region, zone):
        del cls
        assert region == _REGION
        assert zone is None or zone in _ZONE_NAMES
        return region, zone

    def _accelerators_from_instance_type(cls, instance_type):
        del cls
        assert instance_type == _INSTANCE_TYPE
        return {'L4': 1}

    def _regions_with_offering(cls,
                               instance_type,
                               accelerators,
                               use_spot,
                               region,
                               zone,
                               resources=None):
        del cls, accelerators, resources
        assert instance_type == _INSTANCE_TYPE
        assert use_spot
        if region is not None and region != _REGION:
            return []
        result = _region_with_zones()
        if zone is not None:
            result.set_zones([z for z in result.zones if z.name == zone])
        return [result]

    # Keep this path deterministic and credentialless: construction validates
    # the requested location, while the optimizer helper queries its offerings.
    monkeypatch.setattr(clouds.AWS, 'validate_region_zone',
                        classmethod(_validate_region_zone))
    monkeypatch.setattr(clouds.AWS, 'get_accelerators_from_instance_type',
                        classmethod(_accelerators_from_instance_type))
    monkeypatch.setattr(clouds.AWS, 'regions_with_offering',
                        classmethod(_regions_with_offering))

    region_pinned = resources_lib.Resources(cloud=clouds.AWS(),
                                            region=_REGION,
                                            instance_type=_INSTANCE_TYPE,
                                            use_spot=True)
    launchables = resources_utils.make_launchables_for_valid_region_zones(
        region_pinned)

    provisioner = object.__new__(backend.RetryingVmProvisioner)
    attempts = []
    for launchable in launchables:
        yielded = list(
            provisioner._yield_zones(launchable,
                                     num_nodes=1,
                                     cluster_name='unused',
                                     prev_cluster_status=None,
                                     prev_cluster_ever_up=False))
        assert len(yielded) == 1
        assert yielded[0] is not None
        attempts.append((launchable, yielded[0]))
    return attempts


def test_region_pinned_one_node_spot_uses_exact_zone_cache_keys(monkeypatch):
    attempts = _normal_spot_attempts(monkeypatch)

    assert [launchable.zone for launchable, _ in attempts] == list(_ZONE_NAMES)
    for launchable, zones in attempts:
        assert [zone.name for zone in zones] == [launchable.zone]
        key = backend._capacity_cache_key(launchable, clouds.Region(_REGION),
                                          zones, 1, _ACCOUNT)
        assert key == capacity_cache.ResourceKey(
            cloud='aws',
            account=_ACCOUNT,
            region=_REGION,
            zone=launchable.zone,
            instance_type=_INSTANCE_TYPE,
            accelerators=backend._canonical_accelerators(launchable),
            num_nodes=1,
        )


def test_first_wave_marks_are_visible_to_second_wave(tmp_path, monkeypatch):
    attempts = _normal_spot_attempts(monkeypatch)
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "capacity_storm.db"}')
    monkeypatch.setattr(kv_cache._db_manager, '_engine', engine)
    kv_cache.Base.metadata.create_all(engine)

    keys = []
    for launchable, zones in attempts:
        key = backend._capacity_cache_key(launchable, clouds.Region(_REGION),
                                          zones, 1, _ACCOUNT)
        assert key is not None
        keys.append(key)
        capacity_cache.mark_exhausted(key)

    assert capacity_cache.active_exhausted_keys(keys) == set(keys)
    for launchable, zones in attempts:
        assert backend._capacity_cache_exhausted_zone_names(
            launchable, clouds.Region(_REGION), zones, 1,
            _ACCOUNT) == {launchable.zone}
