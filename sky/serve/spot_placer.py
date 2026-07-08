"""Spot Placer for SpotHedge."""

import collections
import dataclasses
import enum
import os
import time
import typing
from typing import Any, Dict, List, Optional, Set

from sky import check as sky_check
from sky import clouds as sky_clouds
from sky import sky_logging
from sky import skypilot_config
from sky.clouds import cloud as sky_cloud
from sky.utils import registry
from sky.utils import resources_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky import resources as resources_lib
    from sky import task as task_lib
    from sky.serve import service_spec

logger = sky_logging.init_logger(__name__)

SPOT_PLACERS = {}
DEFAULT_SPOT_PLACER = None
SPOT_HEDGE_PLACER = 'dynamic_fallback'

# How long a location stays benched after a failed launch or a preemption
# before it becomes eligible for a retry. Without this, a location marked
# PREEMPTED is NEVER selected again (selection only draws from ACTIVE
# locations and reactivation only happened on a successful launch there —
# which could never occur). Measured live (2026-07-06, 1000-replica L4
# fleet): a region benched on a transient spot-vCPU-quota error stayed
# excluded even after quota freed, permanently capping the fleet. Spot
# capacity and quota recover on minute timescales, so retry each benched
# location with one probe launch per TTL window.
_PREEMPTION_RETRY_SECONDS_DEFAULT = 600
_PREEMPTION_RETRY_SECONDS_ENV_VAR = 'SKYPILOT_SPOT_PLACER_RETRY_SECONDS'


def _normalize_image_id(
    image_id: Optional[Dict[Optional[str], str]]
) -> Optional[Dict[Optional[str], str]]:
    """Region-independent form of a single-image dict.

    Parsed YAML keys single-value image dicts by the entry's
    region/context (e.g. {'research-ctx': 'docker:...'}); applying that
    as a cross-region resources_override silently drops the image
    because copy() only honors a key matching the target region.
    """
    if not image_id:
        return image_id
    values = list(image_id.values())
    if len(set(values)) == 1:
        return {None: values[0]}
    return image_id


def _preemption_retry_seconds() -> float:
    override = os.environ.get(_PREEMPTION_RETRY_SECONDS_ENV_VAR)
    if override is not None:
        try:
            return max(0.0, float(override))
        except ValueError:
            logger.warning(f'Invalid {_PREEMPTION_RETRY_SECONDS_ENV_VAR} value '
                           f'{override!r}, using default '
                           f'{_PREEMPTION_RETRY_SECONDS_DEFAULT}s.')
    return _PREEMPTION_RETRY_SECONDS_DEFAULT


@dataclasses.dataclass
class Location:
    """Location of a placer-managed instance.

    Besides cloud/region/zone, a location carries the accelerator shape
    and spot-ness of the resource entry it came from, so heterogeneous
    any_of sets (e.g. cloud L4 spot + reserved-cluster A100 on-demand)
    can be placed by one placer and each launch is pinned to the right
    shape via resources_override.
    """
    cloud: 'sky_clouds.Cloud'
    region: str
    zone: Optional[str]
    accelerators: Optional[Dict[str, int]] = None
    use_spot: bool = True
    # The image the shape entry pins (e.g. a docker: reference for a
    # Kubernetes pool entry whose replicas run inside the model image).
    # Normalized SkyPilot form: {region_or_None: image_ref}.
    image_id: Optional[Dict[Optional[str], str]] = None
    # Per-entry disk tier (e.g. 'high' on VM entries for docker-load
    # throughput; unset on Kubernetes entries, which reject the field).
    disk_tier: Optional[str] = None

    def _accel_key(self) -> str:
        parts = []
        if self.accelerators:
            parts.append(','.join(
                f'{k}:{v}' for k, v in sorted(self.accelerators.items())))
        if self.image_id:
            parts.append(','.join(f'{k}={v}' for k, v in sorted(
                self.image_id.items(), key=lambda kv: str(kv[0]))))
        if self.disk_tier:
            parts.append(f'disk_tier={self.disk_tier}')
        return '|'.join(parts)

    def __eq__(self, other) -> bool:
        if isinstance(other, Location):
            return (self.cloud.is_same_cloud(other.cloud) and
                    self.region == other.region and self.zone == other.zone and
                    self._accel_key() == other._accel_key() and
                    self.use_spot == other.use_spot)
        return False

    def __hash__(self) -> int:
        return hash(
            str(self.cloud) + self.region +
            (self.zone if self.zone is not None else '') + self._accel_key() +
            str(self.use_spot))

    @classmethod
    def from_resources(cls, resources: 'resources_lib.Resources') -> 'Location':
        assert resources.cloud is not None, 'Cloud must be specified'
        assert resources.region is not None, 'Region must be specified'
        image_id = _normalize_image_id(resources.image_id)
        disk_tier = (resources.disk_tier.value
                     if resources.disk_tier is not None else None)
        return cls(resources.cloud,
                   resources.region,
                   resources.zone,
                   accelerators=resources.accelerators,
                   use_spot=resources.use_spot,
                   image_id=image_id,
                   disk_tier=disk_tier)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            'cloud': self.cloud,
            'region': self.region,
            'zone': self.zone,
            'use_spot': self.use_spot,
            # Unconditional (None clears), like image_id/disk_tier below:
            # a CPU-only location must strip GPU entries' accelerators
            # from its copies. Safe for legacy shape-less pickled rows —
            # to_dict is only called on enumerated locations (selection
            # draws from location2status), never on deserialized rows.
            'accelerators': self.accelerators,
        }
        # image_id and disk_tier are ALWAYS included (None clears): the
        # override is applied to every any_of entry, so a location without
        # the attribute must strip it from entries that carry it — a
        # VM-selected launch must clear the Kubernetes entry's
        # context-keyed docker image (invalid on AWS/GCP), and a
        # k8s-selected launch must clear the VM entries' disk_tier
        # (rejected by Kubernetes). Either leftover fails resource
        # validation before optimization.
        d['image_id'] = self.image_id
        d['disk_tier'] = self.disk_tier
        return d

    @classmethod
    def from_pickleable(
        cls,
        data: Optional[Dict[str, Any]],
    ) -> Optional['Location']:
        if data is None:
            return None
        cloud = registry.CLOUD_REGISTRY.from_str(data['cloud'])
        assert cloud is not None
        assert data['region'] is not None
        return cls(
            cloud=cloud,
            region=data['region'],
            zone=data['zone'],
            # Rows pickled before the heterogeneous-placer change carry
            # neither key; default to the old semantics (spot, shape from
            # the task).
            accelerators=data.get('accelerators'),
            use_spot=data.get('use_spot', True),
            image_id=data.get('image_id'),
            disk_tier=data.get('disk_tier'),
        )

    def to_pickleable(self) -> Dict[str, Any]:
        return {
            'cloud': str(self.cloud),
            'region': self.region,
            'zone': self.zone,
            'accelerators': self.accelerators,
            'use_spot': self.use_spot,
            'image_id': self.image_id,
            'disk_tier': self.disk_tier,
        }


class LocationStatus(enum.Enum):
    """Location Spot Status."""
    ACTIVE = 'ACTIVE'
    PREEMPTED = 'PREEMPTED'


def _get_possible_location_from_task(task: 'task_lib.Task') -> List[Location]:

    def _shape_free_config(
            resources: 'resources_lib.Resources') -> Dict[str, Any]:
        # Accelerators and spot-ness are per-location attributes (a
        # heterogeneous any_of mixes e.g. cloud L4 spot with
        # reserved-cluster A100 on-demand); everything else must be
        # uniform across entries. Compare yaml configs with the
        # per-location keys stripped instead of round-tripping through
        # Resources.copy(use_spot=None), whose None handling is not a
        # 'clear this field' contract.
        config = resources.copy(cloud=None, region=None,
                                zone=None).to_yaml_config()
        for key in ('accelerators', 'use_spot', 'spot_recovery', 'image_id',
                    'disk_tier'):
            config.pop(key, None)
        return config

    assert task.resources  # Guaranteed in task constructor
    resources_list = list(task.resources)
    empty_location_resources_config = _shape_free_config(resources_list[0])

    for r in resources_list:
        if _shape_free_config(r) != empty_location_resources_config:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'Different resource configurations are not supported '
                    'for spot placement. All resources must have the same '
                    'configuration except for cloud/region/zone/'
                    'accelerators/use_spot.')

    # Group entries by (accelerators, use_spot) shape: locations are
    # enumerated per shape so each candidate location knows exactly what
    # to launch there.
    possible_locations = set()
    for shape_entry in resources_list:
        location_requirements: Dict[str, Dict[str, Set[str]]] = (
            collections.defaultdict(lambda: collections.defaultdict(set)))
        r = shape_entry
        if r.cloud is not None:
            cloud_str = str(r.cloud)
            if r.region is None:
                _ = location_requirements[cloud_str]
            elif r.zone is None:
                _ = location_requirements[cloud_str][r.region]
            else:
                location_requirements[cloud_str][r.region].add(r.zone)

        clouds_list: List[sky_clouds.Cloud] = []
        for c in location_requirements.keys():
            cloud_obj = registry.CLOUD_REGISTRY.from_str(c)
            assert cloud_obj is not None
            clouds_list.append(cloud_obj)
        if not clouds_list:
            # No location requirement on this entry: all enabled clouds.
            clouds_list = sky_check.get_cached_enabled_clouds_or_refresh(
                capability=sky_cloud.CloudCapability.COMPUTE,
                raise_if_no_cloud_access=False)
            for cloud in clouds_list:
                _ = location_requirements[str(cloud)]

        # The entry itself is the shape template: building the shape from
        # resources_list[0] and overriding only the keys this entry sets
        # leaks the template's per-location attributes into entries that
        # leave them unset (e.g. a cloud entry's disk_tier poisons a
        # Kubernetes entry, whose feasibility check then rejects
        # custom_disk_tier and silently drops the context).
        shape_resources = r.copy(cloud=None, region=None, zone=None)
        for cloud in clouds_list:
            feasible_resources: resources_utils.FeasibleResources = (
                cloud.get_feasible_launchable_resources(
                    shape_resources, num_nodes=task.num_nodes))
            for feasible in feasible_resources.resources_list:
                # We set override_optimize_by_zone=True to force the
                # provisioner to use zone-level provisioning. This is to get
                # accurate location information.
                launchables: List['resources_lib.Resources'] = (
                    resources_utils.make_launchables_for_valid_region_zones(
                        feasible, override_optimize_by_zone=True))
                for launchable in launchables:
                    cloud_str = str(launchable.cloud)
                    region = launchable.region
                    zone = launchable.zone
                    assert region is not None, 'Region must be specified'
                    if (cloud_str not in location_requirements and
                            location_requirements):
                        continue
                    # .get() avoids creating extra entries in
                    # location_requirements that would then be treated as
                    # user requirements for the following regions.
                    cloud_reqs = location_requirements.get(cloud_str, {})
                    if region not in cloud_reqs and cloud_reqs:
                        continue
                    region_reqs = cloud_reqs.get(region, set())
                    if zone not in region_reqs and region_reqs:
                        continue
                    loc = Location.from_resources(launchable)
                    # Pin the shape from the any_of entry, not the launchable
                    # (make_launchables may resolve counts differently).
                    loc.accelerators = r.accelerators
                    loc.use_spot = r.use_spot
                    loc.image_id = _normalize_image_id(r.image_id)
                    loc.disk_tier = (r.disk_tier.value
                                     if r.disk_tier is not None else None)
                    possible_locations.add(loc)

    return list(possible_locations)


class SpotPlacer:
    """Spot Placement specification."""

    def __init__(self, task: 'task_lib.Task') -> None:
        possible_locations = _get_possible_location_from_task(task)
        logger.info(f'{len(possible_locations)} possible location candidates '
                    'are enabled for spot placement.')
        logger.debug(f'All possible locations: {possible_locations}')
        self.location2status: Dict[Location, LocationStatus] = {
            location: LocationStatus.ACTIVE for location in possible_locations
        }
        # When each PREEMPTED mark was set; drives the TTL retry.
        self.location2preempted_at: Dict[Location, float] = {}
        self.location2cost: Dict[Location, float] = {}
        # Already checked there is only one resource in the task.
        self.resources = list(task.resources)[0]
        self.num_nodes = task.num_nodes

    def __init_subclass__(cls, name: str, default: bool = False):
        SPOT_PLACERS[name] = cls
        if default:
            global DEFAULT_SPOT_PLACER
            assert DEFAULT_SPOT_PLACER is None, (
                'Only one policy can be default.')
            DEFAULT_SPOT_PLACER = name

    def select_next_location(self,
                             current_locations: List[Location]) -> Location:
        """Select next location to place spot instance."""
        raise NotImplementedError

    def resolve_location(self, location: Location) -> Optional[Location]:
        """Map a possibly-legacy location onto this placer's key set.

        Replica rows pickled before locations carried
        (accelerators, use_spot, image_id) deserialize shape-less and no
        longer hash-match the shape-bearing keys. Fall back to matching
        on (cloud, region, zone) when that is unambiguous; return None
        (callers skip, never assert) when nothing matches.
        """
        if location in self.location2status:
            return location
        matches = [
            key for key in self.location2status
            if str(key.cloud) == str(location.cloud) and
            key.region == location.region and key.zone == location.zone
        ]
        if len(matches) == 1:
            return matches[0]
        return None

    def set_active(self, location: Location) -> None:
        resolved = self.resolve_location(location)
        if resolved is None:
            logger.warning(f'set_active: unknown location {location}; '
                           'ignoring (likely a pre-upgrade replica row).')
            return
        self.location2status[resolved] = LocationStatus.ACTIVE
        self.location2preempted_at.pop(resolved, None)

    def set_preemptive(self, location: Location) -> None:
        resolved = self.resolve_location(location)
        if resolved is None:
            logger.warning(f'set_preemptive: unknown location {location}; '
                           'ignoring (likely a pre-upgrade replica row).')
            return
        self.location2status[resolved] = LocationStatus.PREEMPTED
        # (Re)start the bench clock: a failed retry benches the location
        # for another full TTL window.
        self.location2preempted_at[resolved] = time.time()

    def clear_preemptive_locations(self) -> None:
        for location in self.location2status:
            self.location2status[location] = LocationStatus.ACTIVE
        self.location2preempted_at.clear()

    def _get_cost_per_hour_cached(self, location: Location) -> float:
        if location in self.location2cost:
            return self.location2cost[location]
        # TODO(tian): Is there a better way to do this? This is for filling
        # instance type so the get_cost() can operate normally.
        r: 'resources_lib.Resources' = self.resources.copy(**location.to_dict())
        assert r.cloud is not None
        rs = r.cloud.get_feasible_launchable_resources(
            r, num_nodes=self.num_nodes).resources_list
        # For some clouds, there might have multiple instance types
        # satisfying the resource request. In such case we choose the
        # cheapest one, as the optimizer does. Reference:
        # sky/optimizer.py::Optimizer::_print_candidates
        cost = min(res.get_cost(seconds=3600) for res in rs)
        self.location2cost[location] = cost
        return cost

    def _min_cost_location(self, locations: List[Location]) -> Location:
        return min(locations, key=self._get_cost_per_hour_cached)

    def _effective_status(self, location: Location) -> LocationStatus:
        """Status with TTL decay: an expired PREEMPTED mark counts ACTIVE.

        The stored status is left untouched — if the retry launch fails,
        set_preemptive refreshes the timestamp (benched for another TTL);
        if it succeeds, set_active clears the mark entirely.
        """
        status = self.location2status[location]
        if status == LocationStatus.PREEMPTED:
            preempted_at = self.location2preempted_at.get(location)
            if (preempted_at is not None and
                    time.time() - preempted_at >= _preemption_retry_seconds()):
                return LocationStatus.ACTIVE
        return status

    def _location_with_status(self, status: LocationStatus) -> List[Location]:
        return [
            location for location in self.location2status
            if self._effective_status(location) == status
        ]

    def _consume_retry_if_benched(self, location: Location) -> None:
        """Consume the TTL retry budget of a benched location on selection.

        An expired PREEMPTED mark makes the location selectable again, but
        the retry must be consumed the moment it is selected — not when the
        probe launch later fails. Otherwise a burst of scale-ups inside one
        window would all pile onto the benched location (it looks like the
        least-loaded ACTIVE candidate) before the first failure re-benches
        it. Refreshing the timestamp here caps it to one probe launch per
        TTL window regardless of batch size; a successful launch clears the
        mark via set_active.
        """
        if self.location2status.get(location) == LocationStatus.PREEMPTED:
            self.location2preempted_at[location] = time.time()

    def active_locations(self) -> List[Location]:
        return self._location_with_status(LocationStatus.ACTIVE)

    def preemptive_locations(self) -> List[Location]:
        return self._location_with_status(LocationStatus.PREEMPTED)

    @classmethod
    def from_task(cls, spec: 'service_spec.SkyServiceSpec',
                  task: 'task_lib.Task') -> Optional['SpotPlacer']:
        if spec.spot_placer is None:
            return None
        return SPOT_PLACERS[spec.spot_placer](task)


class DynamicFallbackSpotPlacer(SpotPlacer,
                                name=SPOT_HEDGE_PLACER,
                                default=True):
    """Dynamic Fallback Placer."""

    def __init__(self, task: 'task_lib.Task') -> None:
        super().__init__(task)
        # INVARIANT: the bench TTL must exceed the worst-case launch
        # FAILURE latency of every managed location, or a full location
        # ping-pongs (its bench expires exactly as a sibling's launch
        # times out) and the service never falls through to the next
        # cost tier. Observed live 2026-07-06 with two Kubernetes shape
        # locations: provision_timeout (600s default) == TTL (600s) ->
        # the service never spilled to cloud. Warn loudly; the fix is
        # kubernetes.provision_timeout << SKYPILOT_SPOT_PLACER_RETRY_SECONDS.
        ttl = _preemption_retry_seconds()
        k8s_contexts = sorted({
            location.region
            for location in self.location2status
            if str(location.cloud).lower() == 'kubernetes'
        })
        for context in k8s_contexts:
            # Only an EXPLICITLY configured provision_timeout can violate
            # the invariant: the built-in kubernetes default is dynamic
            # (10s, capped at 60s) — far below any sane TTL.
            timeout = skypilot_config.get_effective_region_config(
                cloud='kubernetes',
                region=context,
                keys=('provision_timeout',),
                default_value=None)
            if timeout is None:
                continue
            try:
                timeout = float(timeout)
            except (TypeError, ValueError):
                continue
            if timeout >= ttl:
                logger.warning(
                    f'Kubernetes context {context!r} has '
                    f'provision_timeout={timeout:.0f}s >= the spot placer '
                    f'bench TTL ({ttl:.0f}s). A full cluster will ping-pong '
                    'between its locations instead of spilling to the next '
                    'cost tier. Set kubernetes.provision_timeout well below '
                    f'{_PREEMPTION_RETRY_SECONDS_ENV_VAR} (or raise the '
                    'TTL).')

    def select_next_location(self,
                             current_locations: List[Location]) -> Location:
        active_locations = self.active_locations()
        # Zero-cost tier first: locations that cost nothing (reserved /
        # already-paid capacity, e.g. a Kubernetes pool) are filled
        # COMPLETELY before any paid location is considered, regardless
        # of load. When such a location is full, its launches fail fast,
        # it gets benched, and the TTL retry re-probes it as capacity
        # frees — so load drifts back automatically.
        zero_cost = [
            location for location in active_locations
            if self._get_cost_per_hour_cached(location) == 0
        ]
        if zero_cost:
            active_locations = zero_cost
        # Prioritize the least-loaded locations (unused ones have load 0,
        # so this preserves the original prefer-unused behavior while any
        # location is still free). Without the load tiebreak, once every
        # location hosts at least one replica the min-cost rule pins
        # EVERY subsequent launch to the single cheapest active location:
        # at fleet scale this serially hammers one spot-exhausted zone
        # (observed live: >1000 consecutive failed attempts in one zone
        # while other zones and clouds sat idle) instead of spreading.
        # Pre-upgrade replica rows carry shape-less locations; resolve
        # them onto the current key set so they still count as load.
        location_load = collections.Counter(resolved for resolved in (
            self.resolve_location(loc) for loc in current_locations)
                                            if resolved is not None)
        min_load = min(
            (location_load[location] for location in active_locations),
            default=0)
        candidate_locations: List[Location] = [
            location for location in active_locations
            if location_load[location] == min_load
        ]
        # If no candidate locations, use all active locations.
        if not candidate_locations:
            candidate_locations = active_locations
        res = self._min_cost_location(candidate_locations)
        self._consume_retry_if_benched(res)
        logger.info(f'Active locations: {active_locations}\n'
                    f'Current locations: {current_locations}\n'
                    f'Candidate locations: {candidate_locations}\n'
                    f'Selected location: {res}\n')
        return res

    def set_preemptive(self, location: Location) -> None:
        super().set_preemptive(location)
        # Reset only on TOTAL exhaustion (nothing selectable at all). The
        # old <2 threshold defeated small mixed sets: benching a full
        # zero-cost pool with a single paid fallback left active==1,
        # which reset the bench and re-selected the full pool forever
        # instead of spilling over. The TTL decay now guarantees benched
        # locations come back on their own.
        if not self.active_locations():
            self.clear_preemptive_locations()
