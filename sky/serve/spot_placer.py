"""Spot Placer for SpotHedge."""

import collections
from collections.abc import Mapping
import dataclasses
import enum
import math
import os
import re
import time
import typing
from typing import Any, Optional

from sky import catalog
from sky import check as sky_check
from sky import clouds as sky_clouds
from sky import sky_logging
from sky import skypilot_config
from sky.clouds import cloud as sky_cloud
from sky.container_images import models as container_image_models
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
CAPACITY_AWARE_SPOT_PLACER = 'dynamic_fallback_per_gpu'

# These backends build their accelerator catalogs by inspecting the user's
# cluster. Per-GPU placement must not turn service-controller construction into
# a Kubernetes/Slurm API poll; configured shapes remain exact for them.
_LIVE_ACCELERATOR_CATALOG_CLOUDS = frozenset({'kubernetes', 'slurm', 'ssh'})

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
# How long a measured free-slot count stays authoritative for a zero-cost
# location. The broker re-counts every poll interval, so this only has to
# outlive a few missed rounds; past it the location falls back to the blind
# probe path rather than launching on a reading nobody has refreshed.
_MEASURED_CAPACITY_TTL_SECONDS_DEFAULT = 180
_MEASURED_CAPACITY_TTL_ENV_VAR = 'SKYPILOT_MEASURED_CAPACITY_TTL_SECONDS'
_PLACEMENT_SNAPSHOT_MAX_LOCATIONS = 500
_PLACEMENT_CATALOG_SCHEMA_VERSION = 1
# Public reader contract for bounded consumers such as the dashboard.  The
# placement JSON remains versioned independently of the API wire version.
PLACEMENT_CATALOG_SCHEMA_VERSION = _PLACEMENT_CATALOG_SCHEMA_VERSION
_PLACEMENT_CATALOG_MAX_LOCATIONS = 100_000


def _normalize_image_id(
        image_id: dict[str | None, str] | None) -> dict[str | None, str] | None:
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


def _location_image_fields_from_resources(
    resources: 'resources_lib.Resources',
) -> tuple[dict[str | None, str] | None, container_image_models.ContainerImage |
           None]:
    image_id = _normalize_image_id(resources.image_id)
    container_image = resources.container_image
    if (container_image is not None and container_image._legacy_direct):  # pylint: disable=protected-access
        assert container_image.ref is not None
        image_id = dict(image_id or {})
        image_id['docker'] = container_image.ref
        container_image = None
    return image_id, container_image


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


def _measured_capacity_ttl_seconds() -> float:
    override = os.environ.get(_MEASURED_CAPACITY_TTL_ENV_VAR)
    if override is not None:
        try:
            return max(0.0, float(override))
        except ValueError:
            logger.warning(f'Invalid {_MEASURED_CAPACITY_TTL_ENV_VAR} value '
                           f'{override!r}, using default '
                           f'{_MEASURED_CAPACITY_TTL_SECONDS_DEFAULT}s.')
    return _MEASURED_CAPACITY_TTL_SECONDS_DEFAULT


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
    zone: str | None
    # TODO(fcapponi): Split placement coordinates from launch-shape data.
    # Keep cloud/region/zone as Location identity and carry the selected
    # any_of entry's backend-specific fields in a typed resource_overrides
    # value. It must preserve serialization, equality, and explicit None
    # clearing when a heterogeneous launch switches backends.
    accelerators: dict[str, int] | None = None
    use_spot: bool = True
    # The image the shape entry pins (e.g. a docker: reference for a
    # Kubernetes pool entry whose replicas run inside the model image).
    # Normalized SkyPilot form: {region_or_None: image_ref}.
    image_id: dict[str | None, str] | None = None
    container_image: container_image_models.ContainerImage | None = None
    # Per-entry disk tier (e.g. 'high' on VM entries for docker-load
    # throughput; unset on Kubernetes entries, which reject the field).
    disk_tier: str | None = None
    # Per-entry Kubernetes ephemeral-storage request in GiB. Cloud VM
    # entries must keep this unset because they reject the field.
    ephemeral_storage: int | None = None
    # Exact provider instance shape selected by catalog feasibility. Keeping
    # it on the location lets cross-service paid-capacity admission distinguish
    # provider pools that expose the same accelerator count.
    instance_type: str | None = None

    def _accel_key(self, *, include_instance_type: bool = True) -> str:
        parts = []
        if self.accelerators:
            parts.append(','.join(
                f'{k}:{v}' for k, v in sorted(self.accelerators.items())))
        if self.image_id:
            parts.append(','.join(f'{k}={v}' for k, v in sorted(
                self.image_id.items(), key=lambda kv: str(kv[0]))))
        if self.container_image is not None:
            selector = (
                ('ref', self.container_image.ref),
                ('release', self.container_image.release),
                ('artifact_id', self.container_image.artifact_id),
                ('distribution', self.container_image.distribution),
            )
            parts.append(f'container_image={selector!r}')
        if self.disk_tier:
            parts.append(f'disk_tier={self.disk_tier}')
        if self.ephemeral_storage is not None:
            parts.append(f'ephemeral_storage={self.ephemeral_storage}')
        if include_instance_type and self.instance_type is not None:
            parts.append(f'instance_type={self.instance_type}')
        return '|'.join(parts)

    def sort_key(self) -> tuple[str, str, str, str, bool]:
        """Return a deterministic key for displaying placement state."""
        return (str(self.cloud), self.region, self.zone or
                '', self._accel_key(), self.use_spot)

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
        image_id, container_image = _location_image_fields_from_resources(
            resources)
        disk_tier = (resources.disk_tier.value
                     if resources.disk_tier is not None else None)
        return cls(resources.cloud,
                   resources.region,
                   resources.zone,
                   accelerators=resources.accelerators,
                   use_spot=resources.use_spot,
                   image_id=image_id,
                   container_image=container_image,
                   disk_tier=disk_tier,
                   ephemeral_storage=resources.ephemeral_storage,
                   instance_type=resources.instance_type)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
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
        # Per-location fields are ALWAYS included (None clears): the override
        # is applied to every any_of entry, so each selected location must
        # strip fields that only another backend supports.
        d['image_id'] = self.image_id
        d['container_image'] = (self.container_image.to_yaml_config()
                                if self.container_image is not None else None)
        d['disk_tier'] = self.disk_tier
        d['ephemeral_storage'] = self.ephemeral_storage
        d['instance_type'] = self.instance_type
        return d

    @classmethod
    def from_pickleable(
        cls,
        data: dict[str, Any] | None,
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
            container_image=(container_image_models.ContainerImage.from_config(
                data['container_image']) if data.get('container_image')
                             is not None else None),
            disk_tier=data.get('disk_tier'),
            ephemeral_storage=data.get('ephemeral_storage'),
            instance_type=data.get('instance_type'),
        )

    def to_pickleable(self) -> dict[str, Any]:
        return {
            'cloud': str(self.cloud),
            'region': self.region,
            'zone': self.zone,
            'accelerators': self.accelerators,
            'use_spot': self.use_spot,
            'image_id': self.image_id,
            'container_image': (self.container_image.to_yaml_config()
                                if self.container_image is not None else None),
            'disk_tier': self.disk_tier,
            'ephemeral_storage': self.ephemeral_storage,
            'instance_type': self.instance_type,
        }

    @classmethod
    def from_resources_override(
            cls, override: dict[str, Any] | None) -> Optional['Location']:
        """Reconstruct a location from an override that inlined to_dict().

        A placer-pinned launch stamps its location fields into the
        persisted resources_override (see to_dict). A re-driven launch
        (controller crash mid-PENDING) replays that override without
        re-entering the selection path, so the location must be
        recovered from the override itself -- otherwise the replica row
        is upserted with location=None and permanently drops out of
        zero-cost fill accounting. Returns None unless the override carries
        the placer's pin signature (cloud + region).
        """
        if not override:
            return None
        cloud_val = override.get('cloud')
        region = override.get('region')
        if cloud_val is None or region is None:
            return None
        cloud: sky_clouds.Cloud | None = (cloud_val if isinstance(
            cloud_val, sky_clouds.Cloud) else registry.CLOUD_REGISTRY.from_str(
                str(cloud_val)))
        if cloud is None:
            return None
        return cls(
            cloud=cloud,
            region=region,
            zone=override.get('zone'),
            accelerators=override.get('accelerators'),
            use_spot=override.get('use_spot', True),
            image_id=override.get('image_id'),
            container_image=(container_image_models.ContainerImage.from_config(
                override['container_image']) if override.get('container_image')
                             is not None else None),
            disk_tier=override.get('disk_tier'),
            ephemeral_storage=override.get('ephemeral_storage'),
            instance_type=override.get('instance_type'),
        )


def locations_match_placement(replica_location: Location,
                              zero_cost: Location) -> bool:
    """Relaxed zero-cost identity match for fill accounting.

    Deliberately NOT Location.__eq__: full equality includes image_id,
    disk_tier, and ephemeral_storage (via _accel_key), so pre-upgrade shape-less
    replica rows and replicas surviving an image-changing update
    would stop matching, undercounting zero_cost_count and stripping
    the fleet's scale-down protection. For "is this replica on the
    free tier" only the placement identity matters: cloud, region,
    zone (when both sides pin one), accelerator shape and spot-ness.
    Legacy shape-less rows (no accelerators/use_spot persisted) match
    by cloud+region alone.

    Lives here (not on the autoscaler) because both the autoscaler's fill
    overlay and the launch path's demand-placement gate need it, and
    replica_managers must not import autoscalers.
    """
    if str(replica_location.cloud).lower() != str(zero_cost.cloud).lower():
        return False
    if replica_location.region != zero_cost.region:
        return False
    if (replica_location.zone is not None and zero_cost.zone is not None and
            replica_location.zone != zero_cost.zone):
        return False
    if replica_location.accelerators:
        if (zero_cost.accelerators and
                replica_location.accelerators != zero_cost.accelerators):
            return False
        # Only shape-carrying rows enforce spot-ness: legacy rows
        # deserialize with the use_spot=True default, which must not
        # exclude them from a non-spot zero-cost pool.
        if replica_location.use_spot != zero_cost.use_spot:
            return False
    return True


@dataclasses.dataclass(frozen=True)
class CatalogLocationIndex:
    """Precomputed pure indexes for exact and legacy catalog matching."""

    exact: dict[Location, tuple[Location, ...]]
    shape: dict[tuple[str, str, str | None, bool, str], tuple[Location, ...]]
    coordinates: dict[tuple[str, str, str | None], tuple[Location, ...]]

    @classmethod
    def from_locations(
            cls,
            candidates: typing.Iterable[Location]) -> 'CatalogLocationIndex':
        exact: dict[Location, list[Location]] = collections.defaultdict(list)
        shape: dict[tuple[str, str, str | None, bool, str],
                    list[Location]] = collections.defaultdict(list)
        coordinates: dict[tuple[str, str, str | None],
                          list[Location]] = collections.defaultdict(list)
        # pylint: disable=protected-access
        for candidate in candidates:
            exact[candidate].append(candidate)
            cloud_name = str(candidate.cloud)
            shape[(cloud_name, candidate.region, candidate.zone,
                   candidate.use_spot,
                   candidate._accel_key(
                       include_instance_type=False))].append(candidate)
            coordinates[(cloud_name, candidate.region,
                         candidate.zone)].append(candidate)
        # pylint: enable=protected-access
        return cls(
            exact={
                key: tuple(value) for key, value in exact.items()
            },
            shape={
                key: tuple(value) for key, value in shape.items()
            },
            coordinates={
                key: tuple(value) for key, value in coordinates.items()
            },
        )

    def matches(self, location: Location) -> tuple[Location, ...]:
        """Return exact or legacy-compatible candidates in priority order."""
        exact = self.exact.get(location, ())
        if exact:
            return exact
        cloud_name = str(location.cloud)
        if location.instance_type is None:
            # pylint: disable=protected-access
            shape = self.shape.get(
                (cloud_name, location.region, location.zone, location.use_spot,
                 location._accel_key(include_instance_type=False)), ())
            # pylint: enable=protected-access
            if shape:
                return shape
        fully_shape_less = (location.accelerators is None and
                            location.image_id is None and
                            location.container_image is None and
                            location.disk_tier is None and
                            location.ephemeral_storage is None and
                            location.instance_type is None)
        if not fully_shape_less:
            return ()
        return self.coordinates.get(
            (cloud_name, location.region, location.zone), ())


def _catalog_location_matches(
    location: Location,
    candidates: typing.Iterable[Location] | CatalogLocationIndex,
) -> list[Location]:
    """Return exact or legacy-compatible catalog candidates in priority order.

    Exact identity wins.  An instance-type-less row next tries the historical
    shape identity, and a fully shape-less row finally falls back to exact
    cloud/region/zone coordinates.  The caller decides whether multiple
    legacy matches are admissible; dashboard pricing deliberately is strict,
    while operational placement retains its temporary cheapest-match mode.
    """
    if isinstance(candidates, CatalogLocationIndex):
        return list(candidates.matches(location))
    if isinstance(candidates, Mapping) and location in candidates:
        # Preserve SpotPlacer's common exact-key path as O(1). Returning the
        # supplied identity matches its historical resolve_location behavior.
        return [location]
    # Operational placement resolves against mutable status maps. Preserve its
    # one-pass lookup rather than allocating three full indexes per status
    # transition; bounded dashboard reads explicitly pass a request-local
    # prebuilt index above.
    candidates = list(candidates)
    exact = [candidate for candidate in candidates if candidate == location]
    if exact:
        return exact
    if location.instance_type is None:
        # pylint: disable=protected-access
        shape_matches = [
            candidate for candidate in candidates
            if candidate.cloud.is_same_cloud(location.cloud) and candidate.
            region == location.region and candidate.zone == location.zone and
            candidate.use_spot == location.use_spot and candidate._accel_key(
                include_instance_type=False) == location._accel_key(
                    include_instance_type=False)
        ]
        # pylint: enable=protected-access
        if shape_matches:
            return shape_matches
    fully_shape_less = (location.accelerators is None and
                        location.image_id is None and
                        location.container_image is None and
                        location.disk_tier is None and
                        location.ephemeral_storage is None and
                        location.instance_type is None)
    if not fully_shape_less:
        return []
    return [
        candidate for candidate in candidates
        if candidate.cloud.is_same_cloud(location.cloud) and
        candidate.region == location.region and candidate.zone == location.zone
    ]


def match_catalog_location_strict(
    location: Location,
    candidates: typing.Iterable[Location] | CatalogLocationIndex
) -> tuple[Location | None, bool]:
    """Resolve only exact or unambiguous legacy placement identity.

    Returns ``(location, False)`` for a unique match, ``(None, True)`` for an
    ambiguous legacy match, and ``(None, False)`` when no catalog entry is
    compatible.  This is pure: it performs no provider, task, or resource
    lookup.
    """
    matches = _catalog_location_matches(location, candidates)
    if len(matches) == 1:
        return matches[0], False
    return None, len(matches) > 1


class LocationStatus(enum.Enum):
    """Location Spot Status."""
    ACTIVE = 'ACTIVE'
    PREEMPTED = 'PREEMPTED'


@dataclasses.dataclass(frozen=True)
class PlacementCatalog:
    """Complete immutable placement candidates and their nominal costs."""

    entries: tuple[tuple[Location, float], ...]
    # The entry cost is per node.  New catalogs persist the task's immutable
    # node count; None is retained only for catalogs written before this
    # additive field existed.
    num_nodes: int | None = None

    @staticmethod
    def _serialize_location(location: Location) -> dict[str, Any]:
        """Return a JSON-safe location without lossy object-key coercion."""
        serialized = location.to_pickleable()
        image_id = serialized.get('image_id')
        if image_id is not None:
            # JSON object keys cannot preserve Python None. Region-independent
            # image IDs deliberately use None as their key, so encode the map
            # as records rather than letting json.dumps turn it into "null".
            serialized['image_id'] = [{
                'region': region,
                'image': image,
            } for region, image in sorted(image_id.items(),
                                          key=lambda item: str(item[0]))]
        return serialized

    @staticmethod
    def _deserialize_location(data: dict[str, Any]) -> Location:
        """Restore the catalog's JSON-safe location representation."""
        serialized = dict(data)
        image_id = serialized.get('image_id')
        if image_id is not None:
            if not isinstance(image_id, list):
                raise ValueError(
                    'Placement catalog image_id must be a list or null.')
            restored_image_id: dict[str | None, str] = {}
            for image_entry in image_id:
                if not isinstance(image_entry, dict):
                    raise ValueError(
                        'Placement catalog image_id entry must be a mapping.')
                if set(image_entry) != {'region', 'image'}:
                    raise ValueError(
                        'Placement catalog image_id entry must contain only '
                        'region and image.')
                region = image_entry['region']
                image = image_entry['image']
                if region is not None and not isinstance(region, str):
                    raise ValueError('Placement catalog image_id region must '
                                     'be a string or null.')
                if not isinstance(image, str):
                    raise ValueError(
                        'Placement catalog image_id image must be a string.')
                if region in restored_image_id:
                    raise ValueError(
                        'Placement catalog image_id regions must be unique.')
                restored_image_id[region] = image
            serialized['image_id'] = restored_image_id
        location = Location.from_pickleable(serialized)
        if location is None:
            raise ValueError('Placement catalog location cannot be null.')
        return location

    @classmethod
    def from_task(
        cls,
        task: 'task_lib.Task',
        *,
        expand_accelerator_counts: bool = False,
        workspace: str | None = None,
    ) -> 'PlacementCatalog':
        location_kwargs: dict[str, Any] = {}
        if expand_accelerator_counts:
            location_kwargs['expand_accelerator_counts'] = True
        if workspace is not None:
            location_kwargs['workspace'] = workspace
        locations = _get_possible_location_from_task(task, **location_kwargs)
        if len(locations) > _PLACEMENT_CATALOG_MAX_LOCATIONS:
            raise ValueError(
                'Spot placement catalog contains too many locations: '
                f'{len(locations)} > {_PLACEMENT_CATALOG_MAX_LOCATIONS}.')
        resources = list(task.resources)[0]
        entries = []
        for location in sorted(locations, key=lambda item: item.sort_key()):
            if str(location.cloud).lower() == 'kubernetes':
                cost = 0.0
            else:
                materialized = resources.copy(**location.to_dict())
                try:
                    cost = float(materialized.get_cost(seconds=3600))
                    if not math.isfinite(cost) or cost < 0:
                        raise ValueError(f'invalid hourly cost {cost!r}')
                except (TypeError, ValueError) as e:
                    # Catalog feasibility can identify an exact launchable
                    # provider shape whose requested purchase model has no
                    # price. Keep the location available but sort it last.
                    logger.warning('No usable price for placement catalog '
                                   f'location {location}: {e}')
                    cost = float('inf')
            entries.append((location, cost))
        num_nodes = task.num_nodes
        if (isinstance(num_nodes, bool) or not isinstance(num_nodes, int) or
                num_nodes < 1):
            raise ValueError('Placement catalog num_nodes must be a positive '
                             f'integer. Got: {num_nodes!r}')
        return cls(tuple(entries), num_nodes=num_nodes)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'PlacementCatalog':
        """Deserialize and strictly validate a persisted catalog."""
        if not isinstance(data, dict):
            raise ValueError('Placement catalog must be a mapping.')
        schema_version = data.get('schema_version')
        if (isinstance(schema_version, bool) or
                not isinstance(schema_version, int) or
                schema_version != _PLACEMENT_CATALOG_SCHEMA_VERSION):
            raise ValueError('Unsupported placement catalog schema version: '
                             f'{schema_version!r}.')
        num_nodes = data.get('num_nodes')
        if (num_nodes is not None and
            (isinstance(num_nodes, bool) or not isinstance(num_nodes, int) or
             num_nodes < 1)):
            raise ValueError('Placement catalog num_nodes must be a positive '
                             'integer or absent.')
        raw_entries = data.get('entries')
        if not isinstance(raw_entries, list):
            raise ValueError('Placement catalog entries must be a list.')
        if len(raw_entries) > _PLACEMENT_CATALOG_MAX_LOCATIONS:
            raise ValueError(
                'Persisted placement catalog contains too many locations: '
                f'{len(raw_entries)} > {_PLACEMENT_CATALOG_MAX_LOCATIONS}.')
        entries: list[tuple[Location, float]] = []
        seen: set[Location] = set()
        for raw_entry in raw_entries:
            if not isinstance(raw_entry, dict):
                raise ValueError('Placement catalog entry must be a mapping.')
            raw_location = raw_entry.get('location')
            if not isinstance(raw_location, dict):
                raise ValueError(
                    'Placement catalog location must be a mapping.')
            location = cls._deserialize_location(raw_location)
            if location in seen:
                raise ValueError(
                    f'Duplicate placement catalog location: {location}.')
            seen.add(location)
            raw_cost = raw_entry.get('hourly_cost')
            if raw_cost is None:
                cost = float('inf')
            elif (isinstance(raw_cost, bool) or
                  not isinstance(raw_cost, (int, float))):
                raise ValueError('Placement catalog hourly cost must be a '
                                 'non-negative finite number or null.')
            else:
                try:
                    cost = float(raw_cost)
                except OverflowError as exc:
                    raise ValueError(
                        'Placement catalog hourly cost must be a non-negative '
                        'finite number or null.') from exc
                if not math.isfinite(cost) or cost < 0:
                    raise ValueError('Placement catalog hourly cost must be a '
                                     'non-negative finite number or null.')
            entries.append((location, cost))
        if entries != sorted(entries, key=lambda item: item[0].sort_key()):
            raise ValueError(
                'Placement catalog entries must be deterministically sorted.')
        return cls(tuple(entries), num_nodes=num_nodes)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            'schema_version': _PLACEMENT_CATALOG_SCHEMA_VERSION,
            'entries': [{
                'location': self._serialize_location(location),
                'hourly_cost': cost if math.isfinite(cost) else None,
            } for location, cost in self.entries],
        }
        if self.num_nodes is not None:
            result['num_nodes'] = self.num_nodes
        return result

    def costs(self) -> dict[Location, float]:
        """Return a mutable runtime map with one value for every location."""
        return dict(self.entries)


def _shape_free_config(resources: 'resources_lib.Resources') -> dict[str, Any]:
    """Return the resource fields shared by every placement candidate."""
    # Accelerators and spot-ness are per-location attributes (a heterogeneous
    # any_of mixes e.g. cloud L4 spot with reserved-cluster A100 on-demand);
    # everything else must be uniform across entries.
    config = resources.copy(cloud=None, region=None, zone=None).to_yaml_config()
    for key in ('accelerators', 'use_spot', 'spot_recovery', 'image_id',
                'container_image', 'disk_tier', 'ephemeral_storage',
                'instance_type'):
        config.pop(key, None)
    return config


def _validate_placement_resource_configs(task: 'task_lib.Task') -> None:
    """Validate placement shapes without enumerating provider catalogs."""
    assert task.resources  # Guaranteed in task constructor.
    resources_list = list(task.resources)
    empty_location_resources_config = _shape_free_config(resources_list[0])
    for resources in resources_list:
        if _shape_free_config(resources) != empty_location_resources_config:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'Different resource configurations are not supported '
                    'for spot placement. All resources must have the same '
                    'configuration except for cloud/region/zone/'
                    'accelerators/use_spot/image_id/container_image/disk_tier/'
                    'ephemeral_storage/instance_type.')


def _expand_accelerator_counts_for_cloud(
    resources: 'resources_lib.Resources',
    cloud: sky_clouds.Cloud,
    counts_cache: dict[tuple[str, str], list[float]] | None = None,
) -> list['resources_lib.Resources']:
    """Expand one paid spot GPU shape using SkyPilot's static catalog.

    ``dynamic_fallback_per_gpu`` treats every whole GPU as an equivalent
    serving slot. The accelerator model remains user policy, while supported
    machine widths are provider catalog data owned by SkyPilot. Cluster-backed
    catalogs are deliberately excluded because listing them can query live
    control planes.
    """
    # TODO(fran): Represent fractional/MIG devices and their capacity weights
    # in the resource/catalog abstraction instead of assuming whole GPUs.
    accelerators = resources.accelerators
    if (not resources.use_spot or accelerators is None or
            len(accelerators) != 1 or resources.instance_type is not None):
        return [resources]
    cloud_name = str(cloud).lower()
    if cloud_name in _LIVE_ACCELERATOR_CATALOG_CLOUDS:
        return [resources]

    accelerator_name, configured_count = next(iter(accelerators.items()))
    if not float(configured_count).is_integer():
        return [resources]
    cache_key = (cloud_name, accelerator_name.lower())
    if counts_cache is None or cache_key not in counts_cache:
        counts_by_name = catalog.list_accelerator_counts(
            name_filter=f'^{re.escape(accelerator_name)}$',
            clouds=cloud_name,
        )
        catalog_counts = next(
            (counts for name, counts in counts_by_name.items()
             if name.lower() == accelerator_name.lower()), [])
        if counts_cache is not None:
            counts_cache[cache_key] = catalog_counts
    else:
        catalog_counts = counts_cache[cache_key]
    whole_counts = {
        int(count)
        for count in catalog_counts
        if count >= 1 and float(count).is_integer()
    }
    whole_counts.add(int(configured_count))
    return [
        resources.copy(accelerators={accelerator_name: count})
        for count in sorted(whole_counts)
    ]


def _get_possible_location_from_task(
    task: 'task_lib.Task',
    *,
    expand_accelerator_counts: bool = False,
    workspace: str | None = None,
) -> list[Location]:
    _validate_placement_resource_configs(task)
    resources_list = list(task.resources)
    if workspace is None:
        workspace = skypilot_config.get_active_workspace()
    allowed_cloud_names = {
        cloud_name.lower()
        for cloud_name in sky_check.get_workspace_allowed_clouds(
            workspace, capability=sky_cloud.CloudCapability.COMPUTE)
    }

    # Group entries by (accelerators, use_spot) shape: locations are
    # enumerated per shape so each candidate location knows exactly what
    # to launch there.
    possible_locations = set()
    accelerator_counts_cache: dict[tuple[str, str], list[float]] = {}
    for shape_entry in resources_list:
        location_requirements: dict[str, dict[str, set[str]]] = (
            collections.defaultdict(lambda: collections.defaultdict(set)))
        r = shape_entry
        if r.cloud is not None:
            cloud_str = str(r.cloud)
            if cloud_str.lower() not in allowed_cloud_names:
                logger.info(f'Skipping {cloud_str} spot-placement candidates: '
                            f'the cloud is not allowed in workspace '
                            f'{workspace!r}.')
                continue
            if r.region is None:
                _ = location_requirements[cloud_str]
            elif r.zone is None:
                _ = location_requirements[cloud_str][r.region]
            else:
                location_requirements[cloud_str][r.region].add(r.zone)

        clouds_list: list[sky_clouds.Cloud] = []
        for c in location_requirements.keys():
            cloud_obj = registry.CLOUD_REGISTRY.from_str(c)
            assert cloud_obj is not None
            clouds_list.append(cloud_obj)
        if not clouds_list:
            # No location requirement on this entry: all enabled clouds.
            with skypilot_config.local_active_workspace_ctx(workspace):
                clouds_list = sky_check.get_cached_enabled_clouds_or_refresh(
                    capability=sky_cloud.CloudCapability.COMPUTE,
                    raise_if_no_cloud_access=False)
            # The enabled-cloud cache and workspace policy are updated
            # separately. Intersect them here so a stale cache snapshot cannot
            # reintroduce a cloud that this workspace has disabled.
            clouds_list = [
                cloud for cloud in clouds_list
                if str(cloud).lower() in allowed_cloud_names
            ]
            for cloud in clouds_list:
                _ = location_requirements[str(cloud)]

        # The entry itself is the shape template: building the shape from
        # resources_list[0] and overriding only the keys this entry sets
        # leaks the template's per-location attributes into entries that
        # leave them unset (e.g. a cloud entry's disk_tier poisons a
        # Kubernetes entry, whose feasibility check then rejects
        # custom_disk_tier and silently drops the context).
        for cloud in clouds_list:
            # Kubernetes, SSH, and Slurm derive their allowed contexts from
            # the active workspace while resolving feasibility and regions.
            # Keep the whole provider enumeration under the durable service
            # workspace; wrapping only the enabled-cloud cache is insufficient
            # for explicitly named cloud entries.
            with skypilot_config.local_active_workspace_ctx(workspace):
                # Strip only location-specific attributes. Provider
                # feasibility still requires explicit instance types to be
                # bound to the cloud currently being enumerated.
                shape_resources = r.copy(cloud=cloud, region=None, zone=None)
                resource_shapes = [shape_resources]
                if expand_accelerator_counts:
                    resource_shapes = _expand_accelerator_counts_for_cloud(
                        shape_resources, cloud, accelerator_counts_cache)
                for candidate_shape in resource_shapes:
                    feasible_resources: resources_utils.FeasibleResources = (
                        cloud.get_feasible_launchable_resources(
                            candidate_shape, num_nodes=task.num_nodes))
                    for feasible in feasible_resources.resources_list:
                        # We set override_optimize_by_zone=True to force the
                        # provisioner to use zone-level provisioning. This is
                        # to get accurate location information.
                        launchables: list[resources_lib.Resources] = (
                            resources_utils.
                            make_launchables_for_valid_region_zones(
                                feasible, override_optimize_by_zone=True))
                        for launchable in launchables:
                            cloud_str = str(launchable.cloud)
                            region = launchable.region
                            zone = launchable.zone
                            assert region is not None, (
                                'Region must be specified')
                            if (cloud_str not in location_requirements and
                                    location_requirements):
                                continue
                            # .get() avoids creating extra entries in
                            # location_requirements that would then be treated
                            # as user requirements for following regions.
                            cloud_reqs = location_requirements.get(
                                cloud_str, {})
                            if region not in cloud_reqs and cloud_reqs:
                                continue
                            region_reqs = cloud_reqs.get(region, set())
                            if zone not in region_reqs and region_reqs:
                                continue
                            loc = Location.from_resources(launchable)
                            # Pin the shape from the catalog-expanded entry,
                            # not the launchable (make_launchables may resolve
                            # counts differently).
                            loc.accelerators = candidate_shape.accelerators
                            loc.use_spot = candidate_shape.use_spot
                            loc.image_id, loc.container_image = (
                                _location_image_fields_from_resources(
                                    candidate_shape))
                            loc.disk_tier = (candidate_shape.disk_tier.value
                                             if candidate_shape.disk_tier
                                             is not None else None)
                            loc.ephemeral_storage = (
                                candidate_shape.ephemeral_storage)
                            # Feasibility resolves the provider shape. Preserve
                            # it even though accelerator count is restored from
                            # the catalog-expanded entry above.
                            loc.instance_type = launchable.instance_type
                            possible_locations.add(loc)
    return list(possible_locations)


class SpotPlacer:
    """Spot Placement specification."""

    _expand_accelerator_counts = False
    _RETRY_STATE_VERSION = 1
    _BENCH_REASONS = frozenset({'capacity', 'quota', 'preempted'})

    def __init__(
        self,
        task: 'task_lib.Task',
        placement_catalog: PlacementCatalog | dict[str, Any] | None = None,
        workspace: str | None = None,
    ) -> None:
        if placement_catalog is None:
            catalog_value = PlacementCatalog.from_task(
                task,
                expand_accelerator_counts=self._expand_accelerator_counts,
                workspace=workspace)
        elif isinstance(placement_catalog, PlacementCatalog):
            catalog_value = placement_catalog
        else:
            catalog_value = PlacementCatalog.from_dict(placement_catalog)
        self.placement_catalog = catalog_value
        # Keep the durable service workspace so every launch-facing view can
        # re-evaluate current policy. Workspace policy may be narrowed while a
        # service is scaled to zero; constructor-only filtering would leave the
        # long-lived controller with stale candidates.
        self._workspace = workspace
        possible_locations = [location for location, _ in catalog_value.entries]
        self.location2status: dict[Location, LocationStatus] = {
            location: LocationStatus.ACTIVE for location in possible_locations
        }
        # When each PREEMPTED mark was set; drives the TTL retry.
        self.location2preempted_at: dict[Location, float] = {}
        self.location2preempted_reason: dict[Location, str] = {}
        # A separate durable timestamp reserves the one expired-bench probe
        # without rewriting the underlying provider observation. Generic
        # launch failures can release this reservation and leave the location
        # immediately eligible; typed failures replace the observation.
        self.location2retry_reserved_at: dict[Location, float] = {}
        # Last measured free slots per zero-cost location, as (slots, when).
        # Only pools the broker counts every round appear here; a paid spot
        # region is never measured.
        self.location2observed_free: dict[Location, tuple[int, float]] = {}
        self._retry_state_dirty = False
        # Complete by construction. Runtime paths must never resolve provider
        # feasibility or pricing because a location cost is missing.
        self.location2cost = catalog_value.costs()
        eligible_locations = self.known_locations()
        excluded_count = len(possible_locations) - len(eligible_locations)
        if excluded_count:
            logger.info(f'Excluded {excluded_count} placement candidate(s) '
                        f'not allowed in workspace {workspace!r}.')
        logger.info(
            f'{len(eligible_locations)} eligible location candidates loaded '
            'from the centralized placement catalog.')
        logger.debug(f'All eligible locations: {eligible_locations}')
        # Already checked there is only one resource in the task.
        self.resources = list(task.resources)[0]
        self.num_nodes = task.num_nodes

    def _ensure_retry_state_fields(self) -> None:
        """Initialize fields for lightweight or legacy reconstructed placers."""
        if not hasattr(self, 'location2preempted_reason'):
            self.location2preempted_reason = {}
        if not hasattr(self, 'location2retry_reserved_at'):
            self.location2retry_reserved_at = {}
        if not hasattr(self, 'location2observed_free'):
            self.location2observed_free = {}
        if not hasattr(self, '_retry_state_dirty'):
            self._retry_state_dirty = False

    def observe_zero_cost_capacity(self, free_by_location: dict[Location, int],
                                   observed_at: float) -> None:
        """Record a broker round's measured free slots per zero-cost location.

        A reserved Kubernetes pool is counted every round, so its bench is not
        carrying information the way a spot region's is: there is nothing left
        to discover by spending a probe. Recording the count lets
        `_effective_status` prefer the measurement over the probe clock.

        Only zero-cost locations are accepted. A paid region is never measured,
        so a caller naming one must not buy it a bench bypass.
        """
        self._ensure_retry_state_fields()
        if not math.isfinite(observed_at):
            return
        for location, free in free_by_location.items():
            resolved = self.resolve_location(location,
                                             allow_ambiguous_legacy_shape=True)
            if resolved is None or self.location2cost.get(resolved) != 0:
                continue
            previous = self.location2observed_free.get(resolved)
            # Rounds can complete out of order; a late arrival must not
            # overwrite a fresher count.
            if previous is not None and previous[1] >= observed_at:
                continue
            self.location2observed_free[resolved] = (max(0, int(free)),
                                                     float(observed_at))
            self._retry_state_dirty = True

    def _measured_available(self, location: Location) -> bool:
        """Whether a fresh count says this location can take a launch now.

        The count must also be NEWER than the bench it would override.
        Otherwise a pool that measures free but refuses launches (taints,
        affinity, admission webhooks) would spin: every failure re-benches,
        and a single stale reading would clear it again forever.
        """
        self._ensure_retry_state_fields()
        entry = self.location2observed_free.get(location)
        if entry is None:
            return False
        free, observed_at = entry
        if free <= 0:
            return False
        if time.time() - observed_at > _measured_capacity_ttl_seconds():
            return False
        benched_at = self.location2preempted_at.get(location)
        if benched_at is not None and benched_at >= observed_at:
            return False
        return True

    def __init_subclass__(cls, name: str, default: bool = False):
        SPOT_PLACERS[name] = cls
        if default:
            global DEFAULT_SPOT_PLACER
            assert DEFAULT_SPOT_PLACER is None, (
                'Only one policy can be default.')
            DEFAULT_SPOT_PLACER = name

    def select_next_location(
            self,
            *,
            skip_zero_cost_preference: bool = False,
            allowed_locations: set[Location] | None = None) -> Location | None:
        """Select next location to place spot instance.

        skip_zero_cost_preference disables the fill-the-free-tier-first
        rule in placers that have one; the placer stays service-agnostic
        and the decision to skip (the broker's demand-placement gate) is
        made by the caller in the launch path. ``allowed_locations`` keeps a
        card-targeted launch inside its exact accelerator subset.
        """
        raise NotImplementedError

    def ranked_active_locations(
            self,
            allowed_locations: set[Location] | None = None) -> list[Location]:
        """Return active candidates in the placer's actual cost order.

        This is an observation-only counterpart to repeated
        select_next_location calls over paid candidates. It deliberately
        delegates every rank to _min_cost_location so specialized placers,
        such as capacity-aware per-slot pricing, keep identical semantics.
        """
        remaining = [
            location for location in self.active_locations()
            if allowed_locations is None or location in allowed_locations
        ]
        ranked = []
        while remaining:
            selected = self._min_cost_location(remaining)
            ranked.append(selected)
            remaining.remove(selected)
        return ranked

    def resolve_location(
            self,
            location: Location,
            *,
            allow_ambiguous_legacy_shape: bool = False) -> Location | None:
        """Map a possibly-legacy location onto this placer's key set.

        A row written before ``instance_type`` was persisted may map to a
        current shape only when all older shape fields match and exactly one
        instance type is possible. Operational callers may opt into the
        cheapest matching current shape to preserve temporary rollout behavior,
        but claim attribution always uses the strict default. Rows predating
        all shape fields retain the coordinates-only fallback under the same
        rule.
        """
        resolved, ambiguous = match_catalog_location_strict(
            location, self.location2status)
        if resolved is not None:
            return resolved
        if allow_ambiguous_legacy_shape and ambiguous:
            matches = _catalog_location_matches(location, self.location2status)
            return self._min_cost_location(matches)
        return None

    def set_active(self,
                   location: Location,
                   *,
                   selected_at: float | None = None) -> None:
        self._ensure_retry_state_fields()
        resolved = self.resolve_location(location,
                                         allow_ambiguous_legacy_shape=True)
        if resolved is None:
            logger.warning(f'set_active: unknown location {location}; '
                           'ignoring (likely a pre-upgrade replica row).')
            return
        preempted_at = self.location2preempted_at.get(resolved)
        if (selected_at is not None and preempted_at is not None and
                preempted_at > selected_at):
            # A slower sibling selected before a newer capacity failure must
            # not clear that failure when it eventually succeeds. A TTL probe
            # is selected after the current bench timestamp, so its success
            # still reactivates the location immediately.
            return
        changed = (self.location2status[resolved] != LocationStatus.ACTIVE or
                   resolved in self.location2preempted_at or
                   resolved in self.location2preempted_reason or
                   resolved in self.location2retry_reserved_at)
        self.location2status[resolved] = LocationStatus.ACTIVE
        self.location2preempted_at.pop(resolved, None)
        self.location2preempted_reason.pop(resolved, None)
        self.location2retry_reserved_at.pop(resolved, None)
        self._retry_state_dirty |= changed

    def set_preemptive(self,
                       location: Location,
                       *,
                       reason: str = 'capacity',
                       observed_at: float | None = None) -> None:
        self._ensure_retry_state_fields()
        resolved = self.resolve_location(location,
                                         allow_ambiguous_legacy_shape=True)
        if resolved is None:
            logger.warning(f'set_preemptive: unknown location {location}; '
                           'ignoring (likely a pre-upgrade replica row).')
            return
        if reason not in self._BENCH_REASONS:
            raise ValueError(f'Unsupported placement bench reason: {reason!r}')
        if observed_at is None:
            observed_at = time.time()
        if not math.isfinite(observed_at):
            raise ValueError('Placement bench timestamp must be finite.')
        self.location2status[resolved] = LocationStatus.PREEMPTED
        # (Re)start the bench clock: a failed retry benches the location
        # for another full TTL window.
        self.location2preempted_at[resolved] = observed_at
        self.location2preempted_reason[resolved] = reason
        self.location2retry_reserved_at.pop(resolved, None)
        self._retry_state_dirty = True

    def set_quota_limited(self,
                          location: Location,
                          *,
                          observed_at: float | None = None) -> None:
        """Bench every same-region candidate covered by quota evidence."""
        resolved = self.resolve_location(location,
                                         allow_ambiguous_legacy_shape=True)
        if resolved is None:
            logger.warning(f'set_quota_limited: unknown location {location}; '
                           'ignoring (likely a pre-upgrade replica row).')
            return
        if observed_at is None:
            observed_at = time.time()
        for candidate in self.location2status:
            if (not candidate.cloud.is_same_cloud(resolved.cloud) or
                    candidate.region != resolved.region or
                    candidate.use_spot != resolved.use_spot or
                    candidate.accelerators != resolved.accelerators):
                continue
            self.set_preemptive(candidate,
                                reason='quota',
                                observed_at=observed_at)

    def clear_preemptive_locations(self) -> None:
        self._ensure_retry_state_fields()
        changed = bool(self.location2preempted_at or
                       self.location2preempted_reason or
                       self.location2retry_reserved_at)
        for location in self.location2status:
            changed |= self.location2status[location] != LocationStatus.ACTIVE
            self.location2status[location] = LocationStatus.ACTIVE
        self.location2preempted_at.clear()
        self.location2preempted_reason.clear()
        self.location2retry_reserved_at.clear()
        self._retry_state_dirty |= changed

    def inherit_preemption_state(self, old_placer: 'SpotPlacer') -> None:
        """Carry live benches for unchanged shapes into a rebuilt placer.

        Exact ``Location`` equality is intentional.  A service update may
        retain a cloud/region/zone while changing the accelerator, purchase
        model, image, disk tier, or ephemeral storage; a capacity failure for
        the old shape must not bench that new shape.
        """
        self._ensure_retry_state_fields()
        old_placer._ensure_retry_state_fields()  # pylint: disable=protected-access
        for location in self.location2status:
            if (old_placer.location2status.get(location)
                    != LocationStatus.PREEMPTED):
                continue
            self.location2status[location] = LocationStatus.PREEMPTED
            preempted_at = old_placer.location2preempted_at.get(location)
            if preempted_at is not None:
                self.location2preempted_at[location] = preempted_at
            reason = old_placer.location2preempted_reason.get(location)
            if reason is not None:
                self.location2preempted_reason[location] = reason
            retry_reserved_at = old_placer.location2retry_reserved_at.get(
                location)
            if retry_reserved_at is not None:
                self.location2retry_reserved_at[location] = retry_reserved_at
            # The free-slot count belongs to the pool, not to the service
            # version being replaced; dropping it would re-bench a measured
            # pool for a whole TTL on every service update.
            observed = old_placer.location2observed_free.get(location)
            if observed is not None:
                self.location2observed_free[location] = observed
            self._retry_state_dirty = True

    def dump_retry_state(self) -> dict[str, Any]:
        """Return bounded JSON-safe placement retry state."""
        self._ensure_retry_state_fields()
        benches = []
        for location in sorted(self.location2status,
                               key=lambda candidate: candidate.sort_key()):
            if self.location2status[location] != LocationStatus.PREEMPTED:
                continue
            observed_at = self.location2preempted_at.get(location)
            if observed_at is None or not math.isfinite(observed_at):
                continue
            bench = {
                'location': location.to_pickleable(),
                'reason': self.location2preempted_reason.get(
                    location, 'capacity'),
                'observed_at': observed_at,
            }
            retry_reserved_at = self.location2retry_reserved_at.get(location)
            if (retry_reserved_at is not None and
                    math.isfinite(retry_reserved_at)):
                bench['retry_reserved_at'] = retry_reserved_at
            measured = self.location2observed_free.get(location)
            if measured is not None and math.isfinite(measured[1]):
                bench['measured_free'] = int(measured[0])
                bench['measured_at'] = float(measured[1])
            benches.append(bench)
        return {'version': self._RETRY_STATE_VERSION, 'benches': benches}

    def load_retry_state(self, state: dict[str, Any] | None) -> None:
        """Restore exact durable benches without restarting their clocks."""
        self._ensure_retry_state_fields()
        if not isinstance(state, dict) or state.get(
                'version') != self._RETRY_STATE_VERSION:
            return
        raw_benches = state.get('benches')
        if not isinstance(raw_benches, list):
            return
        now = time.time()
        for raw in raw_benches[:len(self.location2status)]:
            if not isinstance(raw, dict):
                continue
            reason = raw.get('reason')
            observed_at = raw.get('observed_at')
            retry_reserved_at = raw.get('retry_reserved_at')
            if (reason not in self._BENCH_REASONS or
                    not isinstance(observed_at, (int, float)) or
                    isinstance(observed_at, bool) or
                    not math.isfinite(observed_at)):
                continue
            if (retry_reserved_at is not None and
                (not isinstance(retry_reserved_at, (int, float)) or
                 isinstance(retry_reserved_at, bool) or
                 not math.isfinite(retry_reserved_at))):
                continue
            location_state = raw.get('location')
            if not isinstance(location_state, dict):
                continue
            try:
                location = Location.from_pickleable(location_state)
            except (AssertionError, KeyError, TypeError, ValueError):
                continue
            if location is None:
                continue
            resolved = self.resolve_location(location)
            if resolved is None:
                continue
            restored_at = min(float(observed_at), now)
            self.location2status[resolved] = LocationStatus.PREEMPTED
            self.location2preempted_at[resolved] = restored_at
            self.location2preempted_reason[resolved] = reason
            if retry_reserved_at is not None:
                self.location2retry_reserved_at[resolved] = min(
                    float(retry_reserved_at), now)
            measured_free = raw.get('measured_free')
            measured_at = raw.get('measured_at')
            if (isinstance(measured_free, int) and
                    not isinstance(measured_free, bool) and
                    isinstance(measured_at, (int, float)) and
                    not isinstance(measured_at, bool) and
                    math.isfinite(measured_at)):
                self.location2observed_free[resolved] = (max(
                    0, measured_free), min(float(measured_at), now))
        self._retry_state_dirty = False

    @property
    def retry_state_dirty(self) -> bool:
        return getattr(self, '_retry_state_dirty', False)

    def mark_retry_state_persisted(self) -> None:
        self._retry_state_dirty = False

    def _min_cost_location(self, locations: list[Location]) -> Location:
        return min(
            locations,
            key=lambda location: self.location2cost.get(location, float('inf')))

    def _effective_status(self, location: Location) -> LocationStatus:
        """Status with TTL decay: an expired PREEMPTED mark counts ACTIVE.

        The stored status is left untouched — if the retry launch fails,
        set_preemptive refreshes the timestamp (benched for another TTL);
        if it succeeds, set_active clears the mark entirely.

        A zero-cost location whose free slots were counted more recently than
        its bench is ACTIVE on that count instead of on the probe clock: the
        bench was standing in for an observation that has since arrived.
        """
        self._ensure_retry_state_fields()
        status = self.location2status[location]
        if status == LocationStatus.PREEMPTED:
            if self._measured_available(location):
                return LocationStatus.ACTIVE
            retry_from = self.location2retry_reserved_at.get(
                location, self.location2preempted_at.get(location))
            if (retry_from is not None and
                    time.time() - retry_from >= _preemption_retry_seconds()):
                return LocationStatus.ACTIVE
        return status

    def _workspace_eligible_locations(self) -> set[Location]:
        """Return locations allowed by the workspace's current policy.

        This is deliberately config-only: selection is a hot controller path,
        so it must not probe credentials or provider control planes. It does
        re-read the in-process config on each launch-facing view because
        workspace cloud, capability, and context policy can change after a
        scale-to-zero service's placer was constructed.
        """
        workspace = getattr(self, '_workspace', None)
        if workspace is None:
            return set(self.location2status)
        allowed_cloud_names = {
            cloud_name.lower()
            for cloud_name in sky_check.get_workspace_allowed_clouds(
                workspace, capability=sky_cloud.CloudCapability.COMPUTE)
        }
        cloud_configs = {
            cloud_name: skypilot_config.get_workspace_cloud(
                cloud_name,
                workspace) for cloud_name in _LIVE_ACCELERATOR_CATALOG_CLOUDS
        }

        eligible = set()
        for location in self.location2status:
            cloud_name = str(location.cloud).lower()
            if cloud_name not in allowed_cloud_names:
                continue
            region = location.region
            cloud_config = cloud_configs.get(cloud_name)
            if cloud_config is None or region is None:
                eligible.add(location)
                continue
            if cloud_name == 'kubernetes':
                allowed_contexts = cloud_config.get('allowed_contexts', None)
                if (isinstance(allowed_contexts, list) and
                        region not in allowed_contexts):
                    continue
            elif cloud_name == 'ssh':
                allowed_node_pools = cloud_config.get('allowed_node_pools',
                                                      None)
                node_pool = region.removeprefix('ssh-')
                if (isinstance(allowed_node_pools, list) and
                        node_pool not in allowed_node_pools):
                    continue
            elif cloud_name == 'slurm':
                allowed_clusters = cloud_config.get('allowed_clusters', None)
                if (isinstance(allowed_clusters, list) and
                        region not in allowed_clusters):
                    continue
            eligible.add(location)
        return eligible

    def refresh_workspace_policy(self) -> None:
        """Reload centrally managed config before final launch admission."""
        if getattr(self, '_workspace', None) is not None:
            skypilot_config.safe_reload_config()

    def _location_with_status(self, status: LocationStatus) -> list[Location]:
        eligible_locations = self._workspace_eligible_locations()
        return [
            location for location in self.location2status
            if location in eligible_locations and
            self._effective_status(location) == status
        ]

    def _consume_retry_if_benched(self, location: Location) -> None:
        """Consume the TTL retry budget of a benched location on selection.

        An expired PREEMPTED mark makes the location selectable again, but
        the retry must be consumed the moment it is selected — not when the
        probe launch later fails. Otherwise a burst of scale-ups inside one
        window would all pile onto the benched location (it looks like the
        cheapest ACTIVE candidate) before the first failure re-benches it.
        Recording a separate reservation timestamp here caps it to one probe
        launch per TTL window regardless of batch size while preserving the
        underlying provider observation. A successful launch clears both via
        set_active; a generic failure releases only the reservation.
        """
        self._ensure_retry_state_fields()
        if self._measured_available(location):
            # Selection is riding a live count, not a probe. Charging it to the
            # probe budget would re-bench the location after one launch and cap
            # a measured pool's refill at one replica per TTL window.
            return
        if self.location2status.get(location) == LocationStatus.PREEMPTED:
            self.location2retry_reserved_at[location] = time.time()
            self._retry_state_dirty = True

    def reserve_retry(self, location: Location) -> None:
        """Consume an expired exact-location retry selected by another layer."""
        resolved = self.resolve_location(location,
                                         allow_ambiguous_legacy_shape=True)
        if resolved is not None:
            self._consume_retry_if_benched(resolved)

    def release_retry(self, location: Location) -> None:
        """Release an expired-bench probe after a non-availability failure."""
        self._ensure_retry_state_fields()
        resolved = self.resolve_location(location,
                                         allow_ambiguous_legacy_shape=True)
        if (resolved is not None and
                resolved in self.location2retry_reserved_at):
            self.location2retry_reserved_at.pop(resolved, None)
            self._retry_state_dirty = True

    def active_locations(self) -> list[Location]:
        return self._location_with_status(LocationStatus.ACTIVE)

    def known_locations(self) -> list[Location]:
        """Return every configured location, regardless of retry status.

        Card-level cold placement order must remain stable while a location is
        temporarily benched. Callers may inspect nominal catalog costs through
        cost_per_hour(), but launch selection must still use active_locations()
        or select_next_location().
        """
        eligible_locations = self._workspace_eligible_locations()
        return [
            location for location in self.location2status
            if location in eligible_locations
        ]

    def placement_snapshot(
        self,
        limit: int = _PLACEMENT_SNAPSHOT_MAX_LOCATIONS,
        paid_admission_by_location: dict[Location, dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        """Serialize already-resident retry state without provider calls."""
        self._ensure_retry_state_fields()
        if (not isinstance(limit, int) or isinstance(limit, bool) or
                limit < 1 or limit > _PLACEMENT_SNAPSHOT_MAX_LOCATIONS):
            raise ValueError(f'limit must be an integer from 1 to '
                             f'{_PLACEMENT_SNAPSHOT_MAX_LOCATIONS}.')
        now = time.time()
        retry_seconds = _preemption_retry_seconds()
        eligible_locations = self._workspace_eligible_locations()
        locations = sorted((location for location in self.location2status
                            if location in eligible_locations),
                           key=lambda location: location.sort_key())
        entries = []
        for location in locations[:limit]:
            stored_status = self.location2status[location]
            benched_at = self.location2preempted_at.get(location)
            retry_reserved_at = self.location2retry_reserved_at.get(location)
            next_probe_at = None
            effective_status = stored_status
            if (stored_status == LocationStatus.PREEMPTED and
                    benched_at is not None):
                retry_from = (benched_at if retry_reserved_at is None else
                              retry_reserved_at)
                next_probe_at = retry_from + retry_seconds
                if now >= next_probe_at:
                    effective_status = LocationStatus.ACTIVE
            cached_cost = self.location2cost.get(location)
            if cached_cost is not None and not math.isfinite(cached_cost):
                cached_cost = None
            entries.append({
                'cloud': str(location.cloud),
                'region': location.region,
                'zone': location.zone,
                'instance_type': location.instance_type,
                'accelerators': location.accelerators,
                'use_spot': location.use_spot,
                'stored_status': stored_status.value,
                'effective_status': effective_status.value,
                'bench_reason': self.location2preempted_reason.get(location),
                'probe_eligible': (stored_status == LocationStatus.PREEMPTED and
                                   effective_status == LocationStatus.ACTIVE),
                'benched_at': benched_at,
                'retry_reserved_at': retry_reserved_at,
                'next_probe_at': next_probe_at,
                'cached_hourly_cost': cached_cost,
                'paid_admission':
                    (None if paid_admission_by_location is None else
                     paid_admission_by_location.get(location)),
            })
        return {
            'available': True,
            'enabled': True,
            'retry_seconds': retry_seconds,
            'observed_at': now,
            'status_semantics':
                ('Controller eligibility only; ACTIVE does not guarantee live '
                 'provider capacity.'),
            'locations': entries,
            'truncated': len(locations) > limit,
        }

    def is_active_location(self, location: Location) -> bool:
        """Whether a known location is currently selectable."""
        resolved = self.resolve_location(location,
                                         allow_ambiguous_legacy_shape=True)
        return (resolved is not None and
                resolved in self._workspace_eligible_locations() and
                self._effective_status(resolved) == LocationStatus.ACTIVE)

    def is_launch_admissible(self, location: Location, *,
                             selected_at: float | None) -> bool:
        """Whether a queued placement is still valid for launch admission.

        Selecting an expired bench consumes its one retry by recording a
        separate reservation before the replica row is created. That specific
        row remains admissible even though the location is no longer
        effectively ACTIVE. A bench recorded after the row was selected is
        newer failure evidence and fences the queued launch.
        """
        resolved = self.resolve_location(location,
                                         allow_ambiguous_legacy_shape=True)
        if (resolved is None or
                resolved not in self._workspace_eligible_locations()):
            return False
        if self._effective_status(resolved) == LocationStatus.ACTIVE:
            return True
        if (self.location2status[resolved] != LocationStatus.PREEMPTED or
                selected_at is None):
            return False
        preempted_at = self.location2preempted_at.get(resolved)
        return preempted_at is not None and preempted_at <= selected_at

    def cost_per_hour(self, location: Location) -> float:
        """Return the centralized catalog cost without provider resolution."""
        resolved = self.resolve_location(location,
                                         allow_ambiguous_legacy_shape=True)
        if (resolved is None or
                resolved not in self._workspace_eligible_locations()):
            return float('inf')
        return self.location2cost.get(resolved, float('inf'))

    def preemptive_locations(self) -> list[Location]:
        return self._location_with_status(LocationStatus.PREEMPTED)

    def zero_cost_locations(self) -> list[Location]:
        """All cataloged zero-cost locations, regardless of bench status.

        Enumeration surface for the reserved-capacity fill poller: a
        benched (PREEMPTED) zero-cost location still defines capacity to
        watch -- it comes back via the TTL retry, and free slots observed
        on it should already be feeding the fill target.
        """
        eligible_locations = self._workspace_eligible_locations()
        return [
            location for location in self.location2status
            if location in eligible_locations and
            self.location2cost.get(location) == 0
        ]

    def select_next_zero_cost_location(
            self,
            allowed_locations: set[Location] | None = None) -> Location | None:
        """Select among zero-cost ACTIVE locations only; None when none is.

        The no-spill guarantee of reserved-capacity fill: a fill launch
        either lands on a zero-cost location or does not happen at all, so
        this deliberately does NOT fall back to paid locations (unlike
        select_next_location). ``allowed_locations`` can further restrict a
        measured batch to contexts whose free-slot budget remains. Uses
        effective status, so a benched
        zero-cost location whose TTL expired is selectable again -- and
        its retry budget is consumed on selection like any other probe.
        """
        candidates = [
            location for location in self.zero_cost_locations()
            if self._effective_status(location) == LocationStatus.ACTIVE and
            (allowed_locations is None or location in allowed_locations)
        ]
        if not candidates:
            return None
        res = self._min_cost_location(candidates)
        self._consume_retry_if_benched(res)
        return res

    @classmethod
    def validate_task(cls, spec: 'service_spec.SkyServiceSpec',
                      task: 'task_lib.Task') -> None:
        """Validate placer resource shape without provider enumeration."""
        if spec.spot_placer is not None:
            _validate_placement_resource_configs(task)

    @classmethod
    def build_catalog(
        cls,
        spec: 'service_spec.SkyServiceSpec',
        task: 'task_lib.Task',
        workspace: str | None = None,
    ) -> PlacementCatalog | None:
        """Build the one complete catalog for an immutable service version."""
        if spec.spot_placer is None:
            return None
        placer_cls = SPOT_PLACERS[spec.spot_placer]
        return PlacementCatalog.from_task(
            task,
            expand_accelerator_counts=placer_cls._expand_accelerator_counts,  # pylint: disable=protected-access
            workspace=workspace)

    @classmethod
    def from_task(
        cls,
        spec: 'service_spec.SkyServiceSpec',
        task: 'task_lib.Task',
        placement_catalog: PlacementCatalog | dict[str, Any] | None = None,
        workspace: str | None = None,
    ) -> Optional['SpotPlacer']:
        if spec.spot_placer is None:
            return None
        return SPOT_PLACERS[spec.spot_placer](
            task, placement_catalog=placement_catalog, workspace=workspace)


class DynamicFallbackSpotPlacer(SpotPlacer,
                                name=SPOT_HEDGE_PLACER,
                                default=True):
    """Dynamic Fallback Placer."""

    def __init__(
        self,
        task: 'task_lib.Task',
        placement_catalog: PlacementCatalog | dict[str, Any] | None = None,
        workspace: str | None = None,
    ) -> None:
        super().__init__(task,
                         placement_catalog=placement_catalog,
                         workspace=workspace)
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
                default_value=None,
                override_configs=self.resources.cluster_config_overrides)
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

    def select_next_location(
            self,
            *,
            skip_zero_cost_preference: bool = False,
            allowed_locations: set[Location] | None = None) -> Location | None:
        active_locations = [
            location for location in self.active_locations()
            if allowed_locations is None or location in allowed_locations
        ]
        if not active_locations:
            return None
        # Zero-cost tier first: locations that cost nothing (reserved /
        # already-paid capacity, e.g. a Kubernetes pool) are filled
        # COMPLETELY before any paid location is considered, regardless
        # of load. When such a location is full, its launches fail fast,
        # it gets benched, and the TTL retry re-probes it as capacity
        # frees — so load drifts back automatically.
        # skip_zero_cost_preference (the broker's demand-placement gate:
        # this service already holds its zero-cost grant) EXCLUDES the
        # free tier from the candidate set — merely demoting it to
        # normal competition is not enough because cost-first selection
        # always prefers a free location. Excluded only while a paid candidate
        # exists: a zero-cost-only set must still serve (the gate throttles
        # placement preference, never availability).
        zero_cost = [
            location for location in active_locations
            if self.location2cost.get(location) == 0
        ]
        if zero_cost and not skip_zero_cost_preference:
            active_locations = zero_cost
        elif zero_cost and skip_zero_cost_preference:
            paid = [
                location for location in active_locations
                if location not in zero_cost
            ]
            if paid:
                active_locations = paid
        # Keep filling the cheapest usable location. A failed launch benches
        # that exact location, so the next selection falls through to the
        # next-cheapest ACTIVE candidate instead of getting stuck retrying it.
        res = self._min_cost_location(active_locations)
        self._consume_retry_if_benched(res)
        logger.info(f'Active locations: {active_locations}\n'
                    f'Selected location: {res}\n')
        return res


class CapacityAwareDynamicFallbackSpotPlacer(DynamicFallbackSpotPlacer,
                                             name=CAPACITY_AWARE_SPOT_PLACER):
    """Dynamic fallback that discovers and prices whole-GPU spot shapes.

    Fill the cheapest active shape per GPU until a failed launch benches it,
    then fall through to the next-cheapest active candidate.
    """

    _expand_accelerator_counts = True

    @staticmethod
    def _accelerator_slots(location: Location) -> float:
        slots = sum((location.accelerators or {}).values())
        return float(slots) if slots > 0 else 1.0

    def _min_cost_location(self, locations: list[Location]) -> Location:
        # TODO(fran): Rank heterogeneous accelerators by measured workload
        # throughput per dollar once services can publish benchmark weights.
        return min(
            locations,
            key=lambda location: self.location2cost.get(location, float(
                'inf')) / self._accelerator_slots(location))
