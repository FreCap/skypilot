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
    costs: Dict[spot_placer.Location, float],
    placement_contract: placement_policy.PlacementContract | None = None,
) -> spot_placer.SpotPlacer:
    """A fully initialized SpotPlacer test double over ACTIVE locations.

    Provider enumeration is deliberately bypassed, but every process-local
    field owned by SpotPlacer is initialized explicitly here. Production no
    longer carries a ``__new__`` hook solely for lightweight test setup.
    """
    placer = object.__new__(spot_placer.SpotPlacer)
    if placement_contract is None:
        placement_contract = placement_policy.resolve_fresh_contract(
            placement_policy.SPOT_HEDGE_PLACER, pool=False)
    placer._placement_contract = placement_contract  # pylint: disable=protected-access
    placer._workspace = None  # pylint: disable=protected-access
    placer.placement_catalog = spot_placer.PlacementCatalog(tuple(
        sorted(costs.items(), key=lambda item: item[0].sort_key())),
                                                            num_nodes=1)
    placer.location2status = {
        location: spot_placer.LocationStatus.ACTIVE for location in costs
    }
    placer.location2preempted_at = {}
    placer.location2preempted_reason = {}
    placer.location2retry_reserved_at = {}
    placer.location2observed_free = {}
    placer._retry_state_dirty = False  # pylint: disable=protected-access
    placer.location2cost = dict(costs)
    placer.resources = mock.MagicMock(cluster_config_overrides=None)
    placer.num_nodes = 1
    return placer
