"""Shared builders for the serve spot-placer / reserved-fill test suites.

Every spot-placer test file used to carry its own copy of these two
helpers; the semantics are identical, so they live here once.
"""
from typing import Dict, Optional
from unittest import mock

from sky.serve import placement_policy
from sky.serve import spot_placer


def make_location(region: str,
                  accelerators: Optional[Dict[str, int]] = None,
                  use_spot: bool = True,
                  cloud_name: Optional[str] = None,
                  ephemeral_storage: Optional[int] = None,
                  instance_type: Optional[str] = None) -> spot_placer.Location:
    """A Location on a mock cloud whose identity is its str()."""
    cloud = mock.MagicMock()
    cloud.is_same_cloud = lambda other: str(other) == str(cloud)
    if cloud_name is not None:
        # setattr: assigning a lambda to a dunder trips mypy method-assign.
        setattr(cloud, '__str__', lambda self: cloud_name)
    return spot_placer.Location(cloud=cloud,
                                region=region,
                                zone=None,
                                accelerators=accelerators,
                                use_spot=use_spot,
                                ephemeral_storage=ephemeral_storage,
                                instance_type=instance_type)


def make_placer(
    costs: Dict[spot_placer.Location, float]
) -> spot_placer.DynamicFallbackSpotPlacer:
    """A DynamicFallbackSpotPlacer over `costs` keys (all ACTIVE).

    Skips __init__ (task-based location discovery) and wires only the
    state the selection paths read.
    """
    placer = spot_placer.DynamicFallbackSpotPlacer.__new__(
        spot_placer.DynamicFallbackSpotPlacer)
    placer._placement_contract = placement_policy.resolve_fresh_contract(  # pylint: disable=protected-access
        placement_policy.SPOT_HEDGE_PLACER,
        pool=False)
    placer.location2status = {
        location: spot_placer.LocationStatus.ACTIVE for location in costs
    }
    placer.location2preempted_at = {}
    placer.location2cost = dict(costs)
    placer.num_nodes = 1
    return placer
