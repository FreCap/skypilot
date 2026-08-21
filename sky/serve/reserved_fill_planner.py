"""Pure typed planning for protocol-v2 reserved-capacity fill.

This module deliberately owns no mutable capacity feed and performs no
provider or database I/O.  Callers publish immutable pool snapshots, ask the
planner for a :class:`FillPlan`, and mutate occupancy or fairness state only
after validating a :class:`FillCommitResult` from durable admission.
"""

from collections.abc import Mapping
import dataclasses
import enum
import hashlib
import json
import math
import re
from typing import Any

from sky.serve import reserved_capacity_broker
from sky.serve import spot_placer

_SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')
ALLOCATION_MAP_SCHEMA_VERSION = 5
_LOCATION_PICKLEABLE_FIELDS = frozenset({
    'cloud',
    'region',
    'zone',
    'accelerators',
    'use_spot',
    'image_id',
    'container_image',
    'disk_tier',
    'ephemeral_storage',
    'instance_type',
})
_POOL_FILL_SNAPSHOT_FIELDS = frozenset({
    'protocol_version',
    'pool_key',
    'physical_cluster_uid',
    'service_generation',
    'worker_projection_sha256_by_accelerator',
    'edge_cap',
    'broker_slot_width',
    'free_slots',
    'free_slots_by_accelerator',
    'grant',
    'grant_epoch',
    'observation_generation',
    'observation_sequence',
    'ordinary_zero_cost_admission_sequence',
    'valid_until',
    'zero_cost_location_keys',
})
_AUTHENTICATED_ALLOCATION_MAP_FIELDS = frozenset({
    'schema_version',
    'allocation_generation',
    'allocation_input_sha256',
    'allocation_claim_generation',
    'service_version',
    'ordinary_zero_cost_admission_sequence_high_water',
    'reconciliation_gate_generation',
    'reclaim_fleet_bundle_sha256',
    'reclaim_policy_revision',
    'reclaim_provider_inventory_sha256',
    'pool_snapshots',
})


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload,
                         sort_keys=True,
                         separators=(',', ':'),
                         allow_nan=False).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _intent_identity_payload(fields: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(fields)
    locations = payload.get('allowed_locations')
    if (not isinstance(locations, tuple) or
            any(not isinstance(location, LocationSnapshot)
                for location in locations)):
        raise ValueError('Intent identity requires immutable locations.')
    payload['allowed_locations'] = [
        location.to_pickleable() for location in locations
    ]
    capacity_unit = payload.get('capacity_unit')
    if isinstance(capacity_unit, FillCapacityUnit):
        payload['capacity_unit'] = capacity_unit.value
    return payload


def _require_int(value: Any, subject: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f'{subject} must be an integer >= {minimum}.')
    return value


def _require_nonempty_string(value: Any, subject: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f'{subject} must be a nonempty string.')
    return value


def _require_sha256(value: Any, subject: str) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f'{subject} must be a lowercase SHA-256 digest.')
    return value


def _require_canonical_positive_float(value: Any, subject: str) -> float:
    if type(value) is not float or not math.isfinite(value) or value <= 0:
        raise ValueError(f'{subject} must be a finite positive float.')
    return value


def _require_closed_fields(data: Mapping[str, Any], expected: frozenset[str],
                           subject: str) -> None:
    actual = set(data)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise ValueError(
            f'{subject} fields must be exact; missing={missing!r}, '
            f'unknown={unknown!r}.')


@dataclasses.dataclass(frozen=True)
class LocationSnapshot:
    """Deeply immutable form of ``Location.to_pickleable()``.

    ``spot_placer.Location`` predates the typed fill contract and is mutable.
    This value preserves its established pickleable shape without retaining a
    mutable ``Location`` or nested dictionary inside a plan.
    """

    cloud: str
    region: str
    zone: str | None
    accelerators: tuple[tuple[str, int], ...]
    use_spot: bool
    image_id: tuple[tuple[str | None, str], ...] | None = None
    container_image: str | tuple[tuple[str, str], ...] | None = None
    disk_tier: str | None = None
    ephemeral_storage: int | None = None
    instance_type: str | None = None

    def __post_init__(self) -> None:
        cloud = _require_nonempty_string(self.cloud, 'Location cloud')
        if cloud.casefold() != 'kubernetes':
            raise ValueError('Reserved fill locations must be Kubernetes '
                             'locations.')
        _require_nonempty_string(self.region, 'Location region')
        if self.zone is not None:
            _require_nonempty_string(self.zone, 'Location zone')
        if type(self.use_spot) is not bool:
            raise ValueError('Location use_spot must be a boolean.')
        if self.use_spot:
            raise ValueError('Reserved fill locations must be zero-cost '
                             'on-demand Kubernetes locations.')
        if (type(self.accelerators) is not tuple or
                len(self.accelerators) != 1):
            raise ValueError('A fill location must carry one exact accelerator '
                             'shape.')
        name, count = self.accelerators[0]
        _require_nonempty_string(name, 'Location accelerator')
        _require_int(count, 'Location accelerator count', minimum=1)

        if self.image_id is not None:
            if type(self.image_id) is not tuple:
                raise ValueError('Location image_id must be an immutable '
                                 'tuple when present.')
            seen_image_regions: set[str | None] = set()
            for region, image in self.image_id:
                if region is not None:
                    _require_nonempty_string(region, 'Image region')
                _require_nonempty_string(image, 'Image reference')
                if region in seen_image_regions:
                    raise ValueError('Location image_id contains a duplicate '
                                     'region.')
                seen_image_regions.add(region)
            canonical_image_id = tuple(
                sorted(self.image_id,
                       key=lambda item: '' if item[0] is None else item[0]))
            if self.image_id != canonical_image_id:
                raise ValueError('Location image_id must use canonical '
                                 'ordering.')

        if isinstance(self.container_image, tuple):
            seen_container_keys: set[str] = set()
            for key, value in self.container_image:
                _require_nonempty_string(key, 'Container-image key')
                _require_nonempty_string(value, 'Container-image value')
                if key in seen_container_keys:
                    raise ValueError('Location container_image contains a '
                                     'duplicate key.')
                seen_container_keys.add(key)
            if self.container_image != tuple(sorted(self.container_image)):
                raise ValueError('Location container_image must use canonical '
                                 'ordering.')
        elif self.container_image is not None:
            _require_nonempty_string(self.container_image,
                                     'Container-image reference')

        if self.disk_tier is not None:
            _require_nonempty_string(self.disk_tier, 'Location disk tier')
        if self.ephemeral_storage is not None:
            _require_int(self.ephemeral_storage,
                         'Location ephemeral storage',
                         minimum=0)
        if self.instance_type is not None:
            _require_nonempty_string(self.instance_type,
                                     'Location instance type')

    @classmethod
    def from_pickleable(cls, data: Mapping[str, Any]) -> 'LocationSnapshot':
        """Validate and freeze one established ``Location`` payload."""
        if type(data) is not dict:
            raise ValueError('Location payload must be an exact dictionary.')
        _require_closed_fields(data, _LOCATION_PICKLEABLE_FIELDS,
                               'Location payload')
        if type(data['accelerators']) is not dict:
            raise ValueError('Location accelerators must be an exact '
                             'dictionary.')
        if (data['image_id'] is not None and
                type(data['image_id']) is not dict):
            raise ValueError('Location image_id must be an exact dictionary '
                             'when present.')
        if (data['container_image'] is not None and
                type(data['container_image']) not in (str, dict)):
            raise ValueError('Location container_image must use an exact '
                             'pickleable type.')
        if type(data['use_spot']) is not bool:
            raise ValueError('Location use_spot must be a boolean.')
        try:
            location = spot_placer.Location.from_pickleable(dict(data))
        except (AssertionError, KeyError, TypeError, ValueError) as error:
            raise ValueError(
                'Malformed Location pickleable payload.') from error
        if location is None:
            raise ValueError('A fill location payload cannot be None.')
        canonical = location.to_pickleable()
        if data != canonical:
            raise ValueError('Location payload must use the canonical '
                             'pickleable representation.')

        raw_accelerators = canonical.get('accelerators')
        if not isinstance(raw_accelerators, dict):
            raise ValueError('A fill location must carry an accelerator map.')
        accelerators = tuple(raw_accelerators.items())

        raw_image_id = canonical.get('image_id')
        image_id: tuple[tuple[str | None, str], ...] | None = None
        if raw_image_id is not None:
            if not isinstance(raw_image_id, dict):
                raise ValueError('Location image_id must be a mapping when '
                                 'present.')
            image_id = tuple(
                sorted(raw_image_id.items(),
                       key=lambda item: '' if item[0] is None else item[0]))

        raw_container_image = canonical.get('container_image')
        container_image: str | tuple[tuple[str, str], ...] | None
        if isinstance(raw_container_image, dict):
            container_image = tuple(sorted(raw_container_image.items()))
        elif raw_container_image is None or isinstance(raw_container_image,
                                                       str):
            container_image = raw_container_image
        else:
            raise ValueError('Location container_image must use the Location '
                             'pickleable contract.')

        return cls(
            cloud=canonical['cloud'],
            region=canonical['region'],
            zone=canonical['zone'],
            accelerators=accelerators,
            use_spot=canonical['use_spot'],
            image_id=image_id,
            container_image=container_image,
            disk_tier=canonical.get('disk_tier'),
            ephemeral_storage=canonical.get('ephemeral_storage'),
            instance_type=canonical.get('instance_type'),
        )

    @property
    def accelerator(self) -> str:
        return self.accelerators[0][0]

    @property
    def accelerator_count(self) -> int:
        return self.accelerators[0][1]

    def to_pickleable(self) -> dict[str, Any]:
        """Return a fresh mutable payload accepted by ``Location``."""
        container_image: str | dict[str, str] | None
        if isinstance(self.container_image, tuple):
            container_image = dict(self.container_image)
        else:
            container_image = self.container_image
        return {
            'cloud': self.cloud,
            'region': self.region,
            'zone': self.zone,
            'accelerators': dict(self.accelerators),
            'use_spot': self.use_spot,
            'image_id': None if self.image_id is None else dict(self.image_id),
            'container_image': container_image,
            'disk_tier': self.disk_tier,
            'ephemeral_storage': self.ephemeral_storage,
            'instance_type': self.instance_type,
        }

    def to_location(self) -> spot_placer.Location:
        """Reconstruct a fresh mutable ``Location`` for legacy consumers."""
        location = spot_placer.Location.from_pickleable(self.to_pickleable())
        assert location is not None
        return location


@dataclasses.dataclass(frozen=True)
class PoolFillSnapshot:
    """Immutable launch authority for one protocol-v2 physical pool."""

    protocol_version: int
    pool_key: str
    physical_cluster_uid: str
    service_generation: int
    worker_projection_sha256_by_accelerator: tuple[tuple[str, str], ...]
    edge_cap: int
    free_slots: int
    free_slots_by_accelerator: tuple[tuple[str, int], ...] | None
    grant: int
    grant_epoch: int | None
    observation_generation: int
    observation_sequence: int
    ordinary_zero_cost_admission_sequence: int
    valid_until: float
    locations: tuple[LocationSnapshot, ...]
    broker_slot_width: int = 1

    def __post_init__(self) -> None:
        _require_int(self.protocol_version, 'Pool protocol version', minimum=1)
        if self.protocol_version != reserved_capacity_broker.PROTOCOL_V2:
            raise ValueError('Pool fill snapshots require protocol version 2.')
        pool_key = _require_nonempty_string(self.pool_key, 'Pool key')
        try:
            identity = reserved_capacity_broker.parse_pool_identity(pool_key)
        except (TypeError, ValueError) as error:
            raise ValueError(
                'Pool fill snapshot has a malformed pool key.') from error
        if identity.protocol_version != reserved_capacity_broker.PROTOCOL_V2:
            raise ValueError('Pool fill snapshots require a protocol-v2 pool '
                             'key.')
        physical_uid = _require_nonempty_string(self.physical_cluster_uid,
                                                'Physical cluster UID')
        if identity.physical_cluster_uid != physical_uid:
            raise ValueError('Pool key and physical cluster UID do not match.')
        canonical_key = reserved_capacity_broker.make_pool_key(
            '',
            identity.gpu_names,
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid=physical_uid)
        if canonical_key != pool_key:
            raise ValueError('Pool key must use the canonical protocol-v2 '
                             'encoding.')

        _require_int(self.service_generation, 'Service generation', minimum=1)
        projection_digests: dict[str, str] = {}
        if type(self.worker_projection_sha256_by_accelerator) is not tuple:
            raise ValueError('Worker projection digests must be an immutable '
                             'tuple.')
        for raw_card, raw_digest in self.worker_projection_sha256_by_accelerator:
            card = _require_nonempty_string(
                raw_card, 'Worker projection accelerator').casefold()
            _require_sha256(raw_digest, 'Worker projection hash')
            if card in projection_digests:
                raise ValueError('Worker projection digests contain a '
                                 'duplicate accelerator.')
            projection_digests[card] = raw_digest
        if set(projection_digests) != set(identity.gpu_names):
            raise ValueError('Worker projection digests must exactly cover '
                             'the pool accelerators.')
        object.__setattr__(self, 'worker_projection_sha256_by_accelerator',
                           tuple(sorted(projection_digests.items())))
        _require_int(self.edge_cap, 'Pool edge cap')
        _require_int(self.broker_slot_width, 'Broker slot width', minimum=1)
        _require_int(self.free_slots, 'Pool free slots')
        _require_int(self.grant, 'Pool grant')
        if self.grant > self.edge_cap:
            raise ValueError('Pool grant cannot exceed its edge cap.')
        if self.free_slots > self.grant:
            raise ValueError('Pool free slots cannot exceed its grant.')
        if self.grant_epoch is not None:
            _require_int(self.grant_epoch, 'Pool grant epoch', minimum=1)
        if (self.free_slots > 0 or self.grant > 0) and self.grant_epoch is None:
            raise ValueError('Live pool authority requires a positive grant '
                             'epoch.')
        _require_int(self.observation_generation,
                     'Observation generation',
                     minimum=1)
        _require_int(self.observation_sequence,
                     'Observation sequence',
                     minimum=0)
        _require_int(self.ordinary_zero_cost_admission_sequence,
                     'Ordinary zero-cost admission sequence',
                     minimum=0)
        if (self.ordinary_zero_cost_admission_sequence
                > self.observation_sequence):
            raise ValueError('Ordinary zero-cost admission sequence cannot '
                             'exceed the total observation sequence.')
        _require_canonical_positive_float(self.valid_until,
                                          'Observation valid_until')

        if type(self.locations) is not tuple or not self.locations:
            raise ValueError('Pool fill snapshots require an immutable, '
                             'nonempty location tuple.')
        if any(not isinstance(location, LocationSnapshot)
               for location in self.locations):
            raise ValueError('Pool locations must be LocationSnapshot values.')
        if len(set(self.locations)) != len(self.locations):
            raise ValueError('Pool locations must not contain duplicates.')

        cards: set[str] = set()
        display_shapes: dict[str, tuple[str, int]] = {}
        contexts: set[str] = set()
        widths: set[int] = set()
        for location in self.locations:
            if location.cloud.casefold() != 'kubernetes':
                raise ValueError('Protocol-v2 fill locations must be '
                                 'Kubernetes locations.')
            card = location.accelerator.casefold()
            prior_shape = display_shapes.setdefault(
                card, (location.accelerator, location.accelerator_count))
            if prior_shape != (location.accelerator,
                               location.accelerator_count):
                raise ValueError('One pool card must use one canonical exact '
                                 'display shape.')
            cards.add(card)
            contexts.add(location.region)
            widths.add(location.accelerator_count)
        if (cards != set(identity.gpu_names) or len(contexts) != 1 or
                len(widths) != 1):
            raise ValueError('Pool locations must exactly cover the pool cards '
                             'in one context and at one GPU width.')
        if (next(iter(widths)) != self.broker_slot_width and
            (self.free_slots != 0 or self.grant != 0)):
            raise ValueError('A mixed-width loser must carry zero launch and '
                             'shelter authority for this pool.')

        exact_slots = self.free_slots_by_accelerator
        if exact_slots is None:
            if self.free_slots > 0 and len(identity.gpu_names) != 1:
                raise ValueError('A composite pool with free capacity requires '
                                 'an exact-card feed.')
            return
        if type(exact_slots) is not tuple:
            raise ValueError('Exact-card feed must be an immutable tuple.')
        normalized: dict[str, int] = {}
        for raw_card, raw_count in exact_slots:
            card = _require_nonempty_string(
                raw_card, 'Exact-card accelerator').casefold()
            count = _require_int(raw_count, 'Exact-card free slots')
            if card in normalized:
                raise ValueError('Exact-card feed contains a duplicate card.')
            if card not in identity.gpu_names:
                raise ValueError('Exact-card feed contains a card outside its '
                                 'pool key.')
            normalized[card] = count
        if sum(normalized.values()) != self.free_slots:
            raise ValueError('Exact-card feed must sum to aggregate free '
                             'slots.')
        object.__setattr__(self, 'free_slots_by_accelerator',
                           tuple(sorted(normalized.items())))

    @classmethod
    def from_mapping(cls,
                     data: Mapping[str, Any],
                     *,
                     map_key: str | None = None) -> 'PoolFillSnapshot':
        """Strictly parse one durable allocation-map entry."""
        if type(data) is not dict:
            raise ValueError('Pool fill snapshot payload must be an exact '
                             'dictionary.')
        _require_closed_fields(data, _POOL_FILL_SNAPSHOT_FIELDS,
                               'Pool fill snapshot')
        pool_key = data['pool_key']
        if map_key is not None and pool_key != map_key:
            raise ValueError('Pool fill snapshot key does not match its map '
                             'key.')

        raw_exact_slots = data['free_slots_by_accelerator']
        exact_slots: tuple[tuple[str, int], ...] | None
        if raw_exact_slots is None:
            exact_slots = None
        elif type(raw_exact_slots) is dict:
            exact_slots = tuple(raw_exact_slots.items())
        else:
            raise ValueError('Exact-card feed must be a mapping when present.')

        raw_projection_digests = data['worker_projection_sha256_by_accelerator']
        if type(raw_projection_digests) is not dict:
            raise ValueError('Worker projection digests must be a mapping.')

        raw_locations = data['zero_cost_location_keys']
        if type(raw_locations) is not list:
            raise ValueError('Pool location keys must be an exact list.')
        locations = tuple(
            LocationSnapshot.from_pickleable(location)
            for location in raw_locations)
        return cls(
            protocol_version=data['protocol_version'],
            pool_key=pool_key,
            physical_cluster_uid=data['physical_cluster_uid'],
            service_generation=data['service_generation'],
            worker_projection_sha256_by_accelerator=tuple(
                raw_projection_digests.items()),
            edge_cap=data['edge_cap'],
            broker_slot_width=data['broker_slot_width'],
            free_slots=data['free_slots'],
            free_slots_by_accelerator=exact_slots,
            grant=data['grant'],
            grant_epoch=data['grant_epoch'],
            observation_generation=data['observation_generation'],
            observation_sequence=data['observation_sequence'],
            ordinary_zero_cost_admission_sequence=(
                data['ordinary_zero_cost_admission_sequence']),
            valid_until=data['valid_until'],
            locations=locations,
        )

    def canonical_payload(self) -> dict[str, Any]:
        """Return the exact durable value covered by allocation auth."""
        exact_slots = self.free_slots_by_accelerator
        return {
            'protocol_version': self.protocol_version,
            'pool_key': self.pool_key,
            'physical_cluster_uid': self.physical_cluster_uid,
            'service_generation': self.service_generation,
            'worker_projection_sha256_by_accelerator': dict(
                self.worker_projection_sha256_by_accelerator),
            'edge_cap': self.edge_cap,
            'broker_slot_width': self.broker_slot_width,
            'free_slots': self.free_slots,
            'free_slots_by_accelerator':
                (None if exact_slots is None else dict(exact_slots)),
            'grant': self.grant,
            'grant_epoch': self.grant_epoch,
            'observation_generation': self.observation_generation,
            'observation_sequence': self.observation_sequence,
            'ordinary_zero_cost_admission_sequence':
                (self.ordinary_zero_cost_admission_sequence),
            'valid_until': self.valid_until,
            'zero_cost_location_keys': [
                location.to_pickleable() for location in self.locations
            ],
        }


@dataclasses.dataclass(frozen=True)
class AuthenticatedAllocationMap:
    """One complete ordered allocation input with a self-verifying hash."""

    allocation_generation: int
    allocation_input_sha256: str
    allocation_claim_generation: int
    service_version: int
    ordinary_zero_cost_admission_sequence_high_water: int
    reconciliation_gate_generation: int
    reclaim_fleet_bundle_sha256: str
    reclaim_policy_revision: str
    reclaim_provider_inventory_sha256: str
    pool_snapshots: tuple[PoolFillSnapshot, ...]

    @staticmethod
    def _input_payload(
        allocation_generation: int,
        allocation_claim_generation: int,
        service_version: int,
        ordinary_zero_cost_admission_sequence_high_water: int,
        reconciliation_gate_generation: int,
        reclaim_fleet_bundle_sha256: str,
        reclaim_policy_revision: str,
        reclaim_provider_inventory_sha256: str,
        pool_snapshots: tuple[PoolFillSnapshot, ...],
    ) -> dict[str, Any]:
        return {
            'schema_version': ALLOCATION_MAP_SCHEMA_VERSION,
            'allocation_generation': allocation_generation,
            'allocation_claim_generation': allocation_claim_generation,
            'service_version': service_version,
            'ordinary_zero_cost_admission_sequence_high_water': ordinary_zero_cost_admission_sequence_high_water,
            'reconciliation_gate_generation': reconciliation_gate_generation,
            'reclaim_fleet_bundle_sha256': reclaim_fleet_bundle_sha256,
            'reclaim_policy_revision': reclaim_policy_revision,
            'reclaim_provider_inventory_sha256': reclaim_provider_inventory_sha256,
            'pool_snapshots': [
                snapshot.canonical_payload() for snapshot in pool_snapshots
            ],
        }

    @classmethod
    def create(
        cls,
        *,
        allocation_generation: int,
        allocation_claim_generation: int,
        service_version: int,
        ordinary_zero_cost_admission_sequence_high_water: int,
        reconciliation_gate_generation: int,
        reclaim_fleet_bundle_sha256: str,
        reclaim_policy_revision: str,
        reclaim_provider_inventory_sha256: str,
        pool_snapshots: tuple[PoolFillSnapshot, ...],
    ) -> 'AuthenticatedAllocationMap':
        """Create an allocation map and bind its complete canonical input."""
        _require_int(allocation_generation, 'Allocation generation', minimum=1)
        _require_int(allocation_claim_generation,
                     'Allocation claim generation',
                     minimum=1)
        _require_int(service_version, 'Service version', minimum=1)
        _require_int(ordinary_zero_cost_admission_sequence_high_water,
                     'Ordinary zero-cost admission sequence high-water',
                     minimum=0)
        _require_int(reconciliation_gate_generation,
                     'Reconciliation gate generation',
                     minimum=1)
        _require_sha256(reclaim_fleet_bundle_sha256,
                        'Reclaim fleet bundle hash')
        _require_nonempty_string(reclaim_policy_revision,
                                 'Reclaim policy revision')
        _require_sha256(reclaim_provider_inventory_sha256,
                        'Reclaim provider inventory hash')
        if type(pool_snapshots) is not tuple or any(
                not isinstance(snapshot, PoolFillSnapshot)
                for snapshot in pool_snapshots):
            raise ValueError('Pool snapshots must be an immutable tuple of '
                             'PoolFillSnapshot values.')
        input_hash = _canonical_sha256(
            cls._input_payload(
                allocation_generation, allocation_claim_generation,
                service_version,
                ordinary_zero_cost_admission_sequence_high_water,
                reconciliation_gate_generation, reclaim_fleet_bundle_sha256,
                reclaim_policy_revision, reclaim_provider_inventory_sha256,
                pool_snapshots))
        return cls(
            allocation_generation=allocation_generation,
            allocation_input_sha256=input_hash,
            allocation_claim_generation=allocation_claim_generation,
            service_version=service_version,
            ordinary_zero_cost_admission_sequence_high_water=(
                ordinary_zero_cost_admission_sequence_high_water),
            reconciliation_gate_generation=reconciliation_gate_generation,
            reclaim_fleet_bundle_sha256=reclaim_fleet_bundle_sha256,
            reclaim_policy_revision=reclaim_policy_revision,
            reclaim_provider_inventory_sha256=(
                reclaim_provider_inventory_sha256),
            pool_snapshots=pool_snapshots,
        )

    @classmethod
    def from_mapping(
        cls,
        data: Mapping[str, Any],
    ) -> 'AuthenticatedAllocationMap':
        """Strictly parse one complete durable allocation publication."""
        if type(data) is not dict:
            raise ValueError('Authenticated allocation map must be an exact '
                             'dictionary.')
        _require_closed_fields(data, _AUTHENTICATED_ALLOCATION_MAP_FIELDS,
                               'Authenticated allocation map')
        if data['schema_version'] != ALLOCATION_MAP_SCHEMA_VERSION:
            raise ValueError('Authenticated allocation map schema version is '
                             'unsupported.')
        raw_snapshots = data['pool_snapshots']
        if type(raw_snapshots) is not list:
            raise ValueError('Allocation pool snapshots must be an exact '
                             'list.')
        snapshots = tuple(
            PoolFillSnapshot.from_mapping(snapshot)
            for snapshot in raw_snapshots)
        return cls(
            allocation_generation=data['allocation_generation'],
            allocation_input_sha256=data['allocation_input_sha256'],
            allocation_claim_generation=data['allocation_claim_generation'],
            service_version=data['service_version'],
            ordinary_zero_cost_admission_sequence_high_water=(
                data['ordinary_zero_cost_admission_sequence_high_water']),
            reconciliation_gate_generation=(
                data['reconciliation_gate_generation']),
            reclaim_fleet_bundle_sha256=data['reclaim_fleet_bundle_sha256'],
            reclaim_policy_revision=data['reclaim_policy_revision'],
            reclaim_provider_inventory_sha256=(
                data['reclaim_provider_inventory_sha256']),
            pool_snapshots=snapshots,
        )

    def __post_init__(self) -> None:
        _require_int(self.allocation_generation,
                     'Allocation generation',
                     minimum=1)
        _require_sha256(self.allocation_input_sha256, 'Allocation input hash')
        _require_int(self.allocation_claim_generation,
                     'Allocation claim generation',
                     minimum=1)
        _require_int(self.service_version, 'Service version', minimum=1)
        _require_int(self.ordinary_zero_cost_admission_sequence_high_water,
                     'Ordinary zero-cost admission sequence high-water',
                     minimum=0)
        _require_int(self.reconciliation_gate_generation,
                     'Reconciliation gate generation',
                     minimum=1)
        _require_sha256(self.reclaim_fleet_bundle_sha256,
                        'Reclaim fleet bundle hash')
        _require_nonempty_string(self.reclaim_policy_revision,
                                 'Reclaim policy revision')
        _require_sha256(self.reclaim_provider_inventory_sha256,
                        'Reclaim provider inventory hash')
        if type(self.pool_snapshots) is not tuple:
            raise ValueError('Pool snapshots must be an immutable tuple.')
        if any(not isinstance(snapshot, PoolFillSnapshot)
               for snapshot in self.pool_snapshots):
            raise ValueError('Pool snapshots must be PoolFillSnapshot values.')

        pool_keys = [snapshot.pool_key for snapshot in self.pool_snapshots]
        if len(set(pool_keys)) != len(pool_keys):
            raise ValueError('Pool snapshots must not repeat a pool key.')
        generations = {
            snapshot.service_generation for snapshot in self.pool_snapshots
        }
        if len(generations) > 1:
            raise ValueError('Pool snapshots must share one service '
                             'generation.')
        cards_by_physical_uid: dict[str, set[str]] = {}
        for snapshot in self.pool_snapshots:
            if (snapshot.ordinary_zero_cost_admission_sequence
                    != self.ordinary_zero_cost_admission_sequence_high_water):
                raise ValueError(
                    'Every allocation observation must share the exact '
                    'ordinary zero-cost admission high-water.')
            identity = reserved_capacity_broker.parse_pool_identity(
                snapshot.pool_key)
            physical_cards = cards_by_physical_uid.setdefault(
                snapshot.physical_cluster_uid, set())
            overlap = physical_cards.intersection(identity.gpu_names)
            if overlap:
                raise ValueError('Pool snapshots overlap accelerator cards on '
                                 'one physical cluster.')
            physical_cards.update(identity.gpu_names)

        self.validate_authentication()

    def validate_authentication(self) -> None:
        """Recompute the allocation hash at every authority-use boundary."""
        expected_hash = _canonical_sha256(
            self._input_payload(
                self.allocation_generation, self.allocation_claim_generation,
                self.service_version,
                self.ordinary_zero_cost_admission_sequence_high_water,
                self.reconciliation_gate_generation,
                self.reclaim_fleet_bundle_sha256, self.reclaim_policy_revision,
                self.reclaim_provider_inventory_sha256, self.pool_snapshots))
        if self.allocation_input_sha256 != expected_hash:
            raise ValueError('Allocation input hash does not match the '
                             'complete canonical allocation map.')

    def to_mapping(self) -> dict[str, Any]:
        """Return the exact durable publication shape."""
        return {
            'schema_version': ALLOCATION_MAP_SCHEMA_VERSION,
            'allocation_generation': self.allocation_generation,
            'allocation_input_sha256': self.allocation_input_sha256,
            'allocation_claim_generation': self.allocation_claim_generation,
            'service_version': self.service_version,
            'ordinary_zero_cost_admission_sequence_high_water':
                (self.ordinary_zero_cost_admission_sequence_high_water),
            'reconciliation_gate_generation':
                self.reconciliation_gate_generation,
            'reclaim_fleet_bundle_sha256': self.reclaim_fleet_bundle_sha256,
            'reclaim_policy_revision': self.reclaim_policy_revision,
            'reclaim_provider_inventory_sha256':
                self.reclaim_provider_inventory_sha256,
            'pool_snapshots': [
                snapshot.canonical_payload() for snapshot in self.pool_snapshots
            ],
        }

    @property
    def identity(self) -> 'ReservedFillAllocationIdentity':
        """Return the closed identity required at destructive commit."""
        return ReservedFillAllocationIdentity(
            allocation_generation=self.allocation_generation,
            allocation_input_sha256=self.allocation_input_sha256,
            allocation_claim_generation=self.allocation_claim_generation,
            service_version=self.service_version,
            ordinary_zero_cost_admission_sequence_high_water=(
                self.ordinary_zero_cost_admission_sequence_high_water),
            reconciliation_gate_generation=(
                self.reconciliation_gate_generation),
            reclaim_fleet_bundle_sha256=self.reclaim_fleet_bundle_sha256,
            reclaim_policy_revision=self.reclaim_policy_revision,
            reclaim_provider_inventory_sha256=(
                self.reclaim_provider_inventory_sha256),
        )


class FillCapacityUnit(str, enum.Enum):
    """Unit used by service-global maximum and planned capacity."""

    PHYSICAL = 'physical'
    LOGICAL = 'logical'

    def intent_cost(self, accelerator_count: int) -> int:
        """Return service-global capacity consumed by one intent."""
        if self is FillCapacityUnit.PHYSICAL:
            return 1
        return accelerator_count


def exact_accelerator_shape(
    accelerators: Mapping[str, int | float] | None,) -> tuple[str, int]:
    """Return one canonical whole-GPU shape or reject it as ambiguous."""
    if not isinstance(accelerators, Mapping) or len(accelerators) != 1:
        raise ValueError('A materialized replica requires one exact '
                         'accelerator shape.')
    raw_card, raw_count = next(iter(accelerators.items()))
    if (not isinstance(raw_card, str) or not raw_card or
            isinstance(raw_count, bool) or
            not isinstance(raw_count, (int, float)) or raw_count < 1 or
            not float(raw_count).is_integer()):
        raise ValueError('A materialized replica accelerator shape is '
                         'malformed.')
    return raw_card.casefold(), int(raw_count)


@dataclasses.dataclass(frozen=True)
class ReservedFillAllocationIdentity:
    """The immutable allocation columns required at destructive commit."""

    allocation_generation: int
    allocation_input_sha256: str
    allocation_claim_generation: int
    service_version: int
    ordinary_zero_cost_admission_sequence_high_water: int
    reconciliation_gate_generation: int
    reclaim_fleet_bundle_sha256: str
    reclaim_policy_revision: str
    reclaim_provider_inventory_sha256: str

    def __post_init__(self) -> None:
        _require_int(self.allocation_generation,
                     'Allocation identity generation',
                     minimum=1)
        _require_sha256(self.allocation_input_sha256,
                        'Allocation identity input hash')
        _require_int(self.allocation_claim_generation,
                     'Allocation identity claim generation',
                     minimum=1)
        _require_int(self.service_version,
                     'Allocation identity service version',
                     minimum=1)
        _require_int(self.ordinary_zero_cost_admission_sequence_high_water,
                     'Allocation identity ordinary admission high-water')
        _require_int(self.reconciliation_gate_generation,
                     'Allocation identity gate generation',
                     minimum=1)
        _require_sha256(self.reclaim_fleet_bundle_sha256,
                        'Allocation identity reclaim fleet bundle hash')
        _require_nonempty_string(self.reclaim_policy_revision,
                                 'Allocation identity reclaim policy revision')
        _require_sha256(self.reclaim_provider_inventory_sha256,
                        'Allocation identity reclaim provider inventory hash')


@dataclasses.dataclass(frozen=True)
class MaterializedFillHolding:
    """Provider-free facts for one materialized reserved-fill backend."""

    replica_id: int
    service_version: int
    capacity: int
    pool_key: str | None
    physical_cluster_uid: str | None
    service_generation: int | None
    accelerator: str | None
    accelerator_count: int | None

    def __post_init__(self) -> None:
        _require_int(self.replica_id, 'Holding replica ID')
        _require_int(self.service_version, 'Holding service version', minimum=1)
        _require_int(self.capacity, 'Holding capacity', minimum=1)
        if self.pool_key is not None:
            _require_nonempty_string(self.pool_key, 'Holding pool key')
        if self.physical_cluster_uid is not None:
            _require_nonempty_string(self.physical_cluster_uid,
                                     'Holding physical cluster UID')
        if self.service_generation is not None:
            _require_int(self.service_generation,
                         'Holding service generation',
                         minimum=1)
        if self.accelerator is not None:
            object.__setattr__(
                self, 'accelerator',
                _require_nonempty_string(self.accelerator,
                                         'Holding accelerator').casefold())
        if self.accelerator_count is not None:
            _require_int(self.accelerator_count,
                         'Holding accelerator count',
                         minimum=1)


@dataclasses.dataclass(frozen=True)
class SequencedRetirementShelter:
    """Frozen PostgreSQL-derived reserved capacity protected from teardown.

    ``allocation_identity is None`` means the sequenced selector is active but
    its current map is unavailable.  That state deliberately shelters every
    already-materialized fill backend while authorizing neither a new fill nor
    a destructive commit.
    """

    service_version: int
    target_capacity: int
    target_capacity_by_accelerator: tuple[tuple[str, int], ...]
    accelerator_shapes: tuple[tuple[str, int], ...]
    allocation_identity: ReservedFillAllocationIdentity | None

    def __post_init__(self) -> None:
        _require_int(self.service_version, 'Shelter service version', minimum=1)
        _require_int(self.target_capacity, 'Shelter target capacity')
        if type(self.target_capacity_by_accelerator) is not tuple:
            raise ValueError('Shelter exact target must be an immutable tuple.')
        if type(self.accelerator_shapes) is not tuple:
            raise ValueError('Shelter shapes must be an immutable tuple.')
        target: dict[str, int] = {}
        shapes: dict[str, int] = {}
        for raw_card, value in self.target_capacity_by_accelerator:
            card = _require_nonempty_string(
                raw_card, 'Shelter target accelerator').casefold()
            if card in target:
                raise ValueError('Shelter target repeats an accelerator.')
            target[card] = _require_int(value, 'Shelter card target', minimum=1)
        for raw_card, value in self.accelerator_shapes:
            card = _require_nonempty_string(
                raw_card, 'Shelter shape accelerator').casefold()
            if card in shapes:
                raise ValueError('Shelter shapes repeat an accelerator.')
            shapes[card] = _require_int(value,
                                        'Shelter accelerator width',
                                        minimum=1)
        if (sum(target.values()) != self.target_capacity or
                set(target) - set(shapes)):
            raise ValueError('Shelter exact target does not match its capacity '
                             'and shapes.')
        object.__setattr__(self, 'target_capacity_by_accelerator',
                           tuple(target.items()))
        object.__setattr__(self, 'accelerator_shapes', tuple(shapes.items()))

    @property
    def authority_current(self) -> bool:
        return self.allocation_identity is not None


@dataclasses.dataclass(frozen=True)
class RetirementCapacityFloor:
    """Demand plus the non-overlapping part of a reserved-fill shelter."""

    capacity: int
    capacity_by_accelerator: tuple[tuple[str, int], ...] = ()
    accelerator_shapes: tuple[tuple[str, int], ...] = ()


def _holding_exact_shape(
    holding: MaterializedFillHolding,) -> tuple[str, int] | None:
    if (holding.accelerator is None or holding.accelerator_count is None or
            holding.accelerator_count <= 0):
        return None
    return holding.accelerator.casefold(), holding.accelerator_count


def derive_sequenced_retirement_shelter(  # pylint: disable=too-many-locals
    *,
    allocation: AuthenticatedAllocationMap | None,
    holdings: tuple[MaterializedFillHolding, ...],
    service_version: int,
    max_capacity: int,
    capacity_unit: FillCapacityUnit,
) -> SequencedRetirementShelter:
    """Derive teardown shelter without adapting into legacy mutable feeds."""
    _require_int(service_version, 'Shelter service version', minimum=1)
    _require_int(max_capacity, 'Shelter maximum capacity')
    if type(holdings) is not tuple or any(
            not isinstance(holding, MaterializedFillHolding)
            for holding in holdings):
        raise ValueError('Shelter holdings must be an immutable typed tuple.')
    if not isinstance(capacity_unit, FillCapacityUnit):
        raise ValueError('Shelter capacity unit must be FillCapacityUnit.')

    ordered_holdings = tuple(sorted(holdings, key=lambda item: item.replica_id))
    if allocation is None:
        # Unavailable is not an authenticated grant of zero.  Preserve every
        # materialized row, including an old-version/recovery row, until a
        # current map makes an explicit destructive decision possible.
        selected: list[MaterializedFillHolding] = []
        remaining_capacity = max_capacity
        for holding in ordered_holdings:
            if holding.capacity > remaining_capacity:
                continue
            selected.append(holding)
            remaining_capacity -= holding.capacity
        capacity = sum(holding.capacity for holding in selected)
        exact: dict[str, int] = {}
        unavailable_shapes: dict[str, int] = {}
        exact_complete = True
        for holding in selected:
            shape = _holding_exact_shape(holding)
            if shape is None:
                exact_complete = False
                break
            card, width = shape
            previous = unavailable_shapes.setdefault(card, width)
            if previous != width:
                exact_complete = False
                break
            exact[card] = exact.get(card, 0) + holding.capacity
        if not exact_complete:
            exact = {}
            unavailable_shapes = {}
        return SequencedRetirementShelter(
            service_version=service_version,
            target_capacity=capacity,
            target_capacity_by_accelerator=tuple(exact.items()),
            accelerator_shapes=tuple(unavailable_shapes.items()),
            allocation_identity=None,
        )

    allocation.validate_authentication()
    if allocation.service_version != service_version:
        raise ValueError('Shelter allocation service version is stale.')

    snapshots = {
        snapshot.pool_key: snapshot for snapshot in allocation.pool_snapshots
    }
    shapes: dict[str, int] = {}
    display_cards: dict[str, str] = {}
    card_order_by_pool: dict[str, tuple[str, ...]] = {}
    for snapshot in allocation.pool_snapshots:
        card_order: list[str] = []
        for location in snapshot.locations:
            card = location.accelerator.casefold()
            if card not in card_order:
                card_order.append(card)
            display_cards.setdefault(card, location.accelerator)
            width = location.accelerator_count
            prior_width = shapes.setdefault(card, width)
            if prior_width != width:
                raise ValueError('Shelter allocation has inconsistent service '
                                 'accelerator shapes.')
        card_order_by_pool[snapshot.pool_key] = tuple(card_order)

    holdings_by_pool_card: dict[tuple[str, str],
                                list[MaterializedFillHolding]] = {}
    for holding in ordered_holdings:
        if holding.service_version > service_version:
            raise ValueError('A materialized holding has a future service '
                             'version.')
        if holding.pool_key is None:
            raise ValueError('A materialized holding lacks exact pool '
                             'attribution.')
        matched_snapshot = snapshots.get(holding.pool_key)
        if matched_snapshot is None:
            # The current authenticated claim set removed this pool.  Its old
            # holding is no longer part of the current grant shelter.
            continue
        holding_card = holding.accelerator
        if (holding_card is None or holding_card
                not in card_order_by_pool[matched_snapshot.pool_key] or
                holding.accelerator_count != shapes.get(holding_card) or
                holding.physical_cluster_uid
                != matched_snapshot.physical_cluster_uid or
                holding.service_generation is None or holding.service_generation
                > matched_snapshot.service_generation):
            raise ValueError('A materialized holding does not match the '
                             'authenticated allocation topology.')
        holdings_by_pool_card.setdefault(
            (matched_snapshot.pool_key, holding_card), []).append(holding)

    physical_targets: list[tuple[str, int]] = []
    for snapshot in allocation.pool_snapshots:
        cards = card_order_by_pool[snapshot.pool_key]
        remaining_slots = min(snapshot.grant, snapshot.edge_cap)
        selected_by_card: dict[str, int] = {}
        for card in cards:
            candidates = holdings_by_pool_card.get((snapshot.pool_key, card),
                                                   [])
            admitted = min(remaining_slots, len(candidates))
            if admitted > 0:
                selected_by_card[card] = admitted
                remaining_slots -= admitted
        # FEED authorizes new work; it is not materialized capacity and cannot
        # become a teardown floor. Otherwise a paid replica at max capacity
        # could neither retire nor make headroom for its free replacement.
        # Unused grant entitlement likewise carries no retirement shelter.
        physical_targets.extend(selected_by_card.items())

    remaining_capacity = max_capacity
    target_by_card: dict[str, int] = {}
    for card, physical_target in physical_targets:
        unit_capacity = capacity_unit.intent_cost(shapes[card])
        admitted_slots = min(physical_target,
                             remaining_capacity // unit_capacity)
        if admitted_slots <= 0:
            continue
        admitted_capacity = admitted_slots * unit_capacity
        target_by_card[card] = (target_by_card.get(card, 0) + admitted_capacity)
        remaining_capacity -= admitted_capacity

    target_capacity = sum(target_by_card.values())
    return SequencedRetirementShelter(
        service_version=service_version,
        target_capacity=target_capacity,
        target_capacity_by_accelerator=tuple(target_by_card.items()),
        accelerator_shapes=tuple(
            (display_cards[card], width) for card, width in shapes.items()),
        allocation_identity=allocation.identity,
    )


def compose_retirement_capacity_floor(
    *,
    demand_capacity: int,
    demand_capacity_by_accelerator: tuple[tuple[str, int], ...],
    accelerator_shapes: tuple[tuple[str, int], ...],
    shelter: SequencedRetirementShelter,
    max_capacity: int,
) -> RetirementCapacityFloor | None:
    """Compose demand and fill per exact card, preserving demand first."""
    _require_int(demand_capacity, 'Demand retirement capacity')
    _require_int(max_capacity, 'Retirement maximum capacity')
    if (type(demand_capacity_by_accelerator) is not tuple or
            type(accelerator_shapes) is not tuple):
        raise ValueError('Demand retirement state must be immutable tuples.')
    demand_by_card = {
        str(card).casefold(): value
        for card, value in demand_capacity_by_accelerator
    }
    shapes = {str(card).casefold(): value for card, value in accelerator_shapes}
    shelter_by_card = dict(shelter.target_capacity_by_accelerator)
    shelter_shapes = dict(shelter.accelerator_shapes)
    exact = (sum(demand_by_card.values()) == demand_capacity and
             sum(shelter_by_card.values()) == shelter.target_capacity and
             set(demand_by_card) <= set(shapes) and
             set(shelter_by_card) <= set(shapes) and
             all(shapes[card] == width
                 for card, width in shelter_shapes.items()
                 if card in shapes))
    if not exact:
        return None
    if shelter.target_capacity == 0:
        return RetirementCapacityFloor(
            capacity=demand_capacity,
            capacity_by_accelerator=demand_capacity_by_accelerator,
            accelerator_shapes=accelerator_shapes,
        )

    target = dict(demand_by_card)
    remaining = max(0, max_capacity - demand_capacity)
    for card in shapes:
        additional = max(0, shelter_by_card.get(card, 0) - target.get(card, 0))
        admitted = min(remaining, additional)
        if admitted > 0:
            target[card] = target.get(card, 0) + admitted
            remaining -= admitted
    return RetirementCapacityFloor(
        capacity=sum(target.values()),
        capacity_by_accelerator=tuple((card, target.get(card, 0))
                                      for card in shapes
                                      if target.get(card, 0) > 0),
        accelerator_shapes=tuple(shapes.items()),
    )


@dataclasses.dataclass(frozen=True)
class CommittedFillDebit:
    """Physical slots already admitted from one allocation publication.

    This is replay protection for rows created by a previous reconcile pass.
    Carrying the complete allocation identity prevents a row from an older
    publication from reducing a newer feed that may describe different
    physical capacity.
    """

    allocation_generation: int
    allocation_input_sha256: str
    allocation_claim_generation: int
    pool_key: str
    accelerator: str
    replica_slots: int

    def __post_init__(self) -> None:
        _require_int(self.allocation_generation,
                     'Committed-fill allocation generation',
                     minimum=1)
        _require_sha256(self.allocation_input_sha256,
                        'Committed-fill allocation input hash')
        _require_int(self.allocation_claim_generation,
                     'Committed-fill allocation claim generation',
                     minimum=1)
        pool_key = _require_nonempty_string(self.pool_key,
                                            'Committed-fill debit pool key')
        accelerator = _require_nonempty_string(
            self.accelerator, 'Committed-fill debit accelerator').casefold()
        _require_int(self.replica_slots,
                     'Committed-fill debit replica slots',
                     minimum=1)
        try:
            identity = reserved_capacity_broker.parse_pool_identity(pool_key)
        except (TypeError, ValueError) as error:
            raise ValueError('Committed-fill debit has a malformed pool '
                             'key.') from error
        if identity.protocol_version != reserved_capacity_broker.PROTOCOL_V2:
            raise ValueError('Committed-fill debit requires a protocol-v2 '
                             'pool key.')
        if accelerator not in identity.gpu_names:
            raise ValueError('Committed-fill debit accelerator is outside '
                             'its pool.')
        object.__setattr__(self, 'accelerator', accelerator)


@dataclasses.dataclass(frozen=True)
class FillIntent:
    """One exactly shaped, fully fenced reserved-fill admission intent."""

    ordinal: int
    idempotency_key: str
    protocol_version: int
    policy_revision: int
    reconcile_generation: int
    allocation_generation: int
    allocation_input_sha256: str
    allocation_claim_generation: int
    reconciliation_gate_generation: int
    reclaim_fleet_bundle_sha256: str
    reclaim_policy_revision: str
    reclaim_provider_inventory_sha256: str
    service_incarnation: str
    service_version: int
    controller_owner: str
    service_generation: int
    pool_key: str
    pool_epoch: int
    physical_cluster_uid: str
    worker_projection_sha256: str
    observation_generation: int
    observation_sequence: int
    ordinary_zero_cost_admission_sequence: int
    valid_until: float
    accelerator: str
    accelerator_count: int
    capacity_unit: FillCapacityUnit
    allowed_locations: tuple[LocationSnapshot, ...]

    @classmethod
    def create(cls, **fields: Any) -> 'FillIntent':
        """Construct an intent with its deterministic idempotency key."""
        if 'idempotency_key' in fields:
            raise ValueError('FillIntent.create computes idempotency_key.')
        key = _canonical_sha256(_intent_identity_payload(fields))
        return cls(idempotency_key=key, **fields)

    def __post_init__(self) -> None:
        _require_int(self.ordinal, 'Intent ordinal')
        _require_sha256(self.idempotency_key, 'Intent idempotency key')
        _require_int(self.protocol_version,
                     'Intent protocol version',
                     minimum=1)
        if self.protocol_version != reserved_capacity_broker.PROTOCOL_V2:
            raise ValueError('Fill intents require protocol version 2.')
        _require_int(self.policy_revision, 'Policy revision', minimum=1)
        _require_int(self.reconcile_generation,
                     'Reconcile generation',
                     minimum=1)
        _require_int(self.allocation_generation,
                     'Allocation generation',
                     minimum=1)
        _require_sha256(self.allocation_input_sha256, 'Allocation input hash')
        _require_int(self.allocation_claim_generation,
                     'Allocation claim generation',
                     minimum=1)
        _require_int(self.reconciliation_gate_generation,
                     'Reconciliation gate generation',
                     minimum=1)
        _require_sha256(self.reclaim_fleet_bundle_sha256,
                        'Reclaim fleet bundle hash')
        _require_nonempty_string(self.reclaim_policy_revision,
                                 'Reclaim policy revision')
        _require_sha256(self.reclaim_provider_inventory_sha256,
                        'Reclaim provider inventory hash')
        _require_nonempty_string(self.service_incarnation,
                                 'Service incarnation')
        _require_int(self.service_version, 'Service version', minimum=1)
        _require_nonempty_string(self.controller_owner, 'Controller owner')
        _require_int(self.service_generation, 'Service generation', minimum=1)
        _require_int(self.pool_epoch, 'Pool epoch', minimum=1)
        physical_uid = _require_nonempty_string(self.physical_cluster_uid,
                                                'Physical cluster UID')
        _require_sha256(self.worker_projection_sha256, 'Worker projection hash')
        _require_int(self.observation_generation,
                     'Observation generation',
                     minimum=1)
        _require_int(self.observation_sequence,
                     'Observation sequence',
                     minimum=0)
        _require_int(self.ordinary_zero_cost_admission_sequence,
                     'Ordinary zero-cost admission sequence',
                     minimum=0)
        if (self.ordinary_zero_cost_admission_sequence
                > self.observation_sequence):
            raise ValueError('Intent ordinary zero-cost admission sequence '
                             'cannot exceed its total observation sequence.')
        _require_canonical_positive_float(self.valid_until,
                                          'Observation valid_until')
        accelerator = _require_nonempty_string(self.accelerator,
                                               'Intent accelerator')
        _require_int(self.accelerator_count,
                     'Intent accelerator count',
                     minimum=1)
        if not isinstance(self.capacity_unit, FillCapacityUnit):
            raise ValueError('Intent capacity unit must be FillCapacityUnit.')

        try:
            identity = reserved_capacity_broker.parse_pool_identity(
                self.pool_key)
        except (TypeError, ValueError) as error:
            raise ValueError('Fill intent has a malformed pool key.') from error
        if (identity.protocol_version != reserved_capacity_broker.PROTOCOL_V2 or
                identity.physical_cluster_uid != physical_uid):
            raise ValueError('Fill intent pool identity does not match its '
                             'protocol or physical UID fence.')
        if accelerator.casefold() not in identity.gpu_names:
            raise ValueError('Fill intent accelerator is outside its pool.')
        if (type(self.allowed_locations) is not tuple or
                not self.allowed_locations):
            raise ValueError('Fill intent requires an immutable, nonempty '
                             'allowed-location tuple.')
        if any(not isinstance(location, LocationSnapshot)
               for location in self.allowed_locations):
            raise ValueError('Fill intent locations must be LocationSnapshot '
                             'values.')
        if len(set(self.allowed_locations)) != len(self.allowed_locations):
            raise ValueError('Fill intent locations must not contain '
                             'duplicates.')
        contexts: set[str] = set()
        for location in self.allowed_locations:
            if (location.cloud.casefold() != 'kubernetes' or
                    location.accelerator != accelerator or
                    location.accelerator_count != self.accelerator_count):
                raise ValueError('Every allowed location must match the '
                                 'intent exact accelerator shape.')
            contexts.add(location.region)
        if len(contexts) != 1:
            raise ValueError('Fill intent locations must use one Kubernetes '
                             'context.')
        if self.idempotency_key != self._compute_idempotency_key():
            raise ValueError('Fill intent idempotency key does not match its '
                             'immutable authority payload.')

    def _compute_idempotency_key(self) -> str:
        fields = {
            'ordinal': self.ordinal,
            'protocol_version': self.protocol_version,
            'policy_revision': self.policy_revision,
            'reconcile_generation': self.reconcile_generation,
            'allocation_generation': self.allocation_generation,
            'allocation_input_sha256': self.allocation_input_sha256,
            'allocation_claim_generation': self.allocation_claim_generation,
            'reconciliation_gate_generation':
                self.reconciliation_gate_generation,
            'reclaim_fleet_bundle_sha256': self.reclaim_fleet_bundle_sha256,
            'reclaim_policy_revision': self.reclaim_policy_revision,
            'reclaim_provider_inventory_sha256':
                self.reclaim_provider_inventory_sha256,
            'service_incarnation': self.service_incarnation,
            'service_version': self.service_version,
            'controller_owner': self.controller_owner,
            'service_generation': self.service_generation,
            'pool_key': self.pool_key,
            'pool_epoch': self.pool_epoch,
            'physical_cluster_uid': self.physical_cluster_uid,
            'worker_projection_sha256': self.worker_projection_sha256,
            'observation_generation': self.observation_generation,
            'observation_sequence': self.observation_sequence,
            'ordinary_zero_cost_admission_sequence':
                (self.ordinary_zero_cost_admission_sequence),
            'valid_until': self.valid_until,
            'accelerator': self.accelerator,
            'accelerator_count': self.accelerator_count,
            'capacity_unit': self.capacity_unit,
            'allowed_locations': self.allowed_locations,
        }
        return _canonical_sha256(_intent_identity_payload(fields))

    def allowed_location_keys(self) -> tuple[dict[str, Any], ...]:
        """Return fresh legacy payloads; mutating them cannot alter the plan."""
        return tuple(
            location.to_pickleable() for location in self.allowed_locations)


@dataclasses.dataclass(frozen=True)
class FillPlan:
    """Immutable planner output; admission is not implied by construction."""

    policy_revision: int
    reconcile_generation: int
    allocation_generation: int
    allocation_input_sha256: str
    allocation_claim_generation: int
    reconciliation_gate_generation: int
    reclaim_fleet_bundle_sha256: str
    reclaim_policy_revision: str
    reclaim_provider_inventory_sha256: str
    capacity_unit: FillCapacityUnit
    intents: tuple[FillIntent, ...]

    def __post_init__(self) -> None:
        _require_int(self.policy_revision, 'Policy revision', minimum=1)
        _require_int(self.reconcile_generation,
                     'Reconcile generation',
                     minimum=1)
        _require_int(self.allocation_generation,
                     'Allocation generation',
                     minimum=1)
        _require_sha256(self.allocation_input_sha256, 'Allocation input hash')
        _require_int(self.allocation_claim_generation,
                     'Allocation claim generation',
                     minimum=1)
        _require_int(self.reconciliation_gate_generation,
                     'Reconciliation gate generation',
                     minimum=1)
        _require_sha256(self.reclaim_fleet_bundle_sha256,
                        'Reclaim fleet bundle hash')
        _require_nonempty_string(self.reclaim_policy_revision,
                                 'Reclaim policy revision')
        _require_sha256(self.reclaim_provider_inventory_sha256,
                        'Reclaim provider inventory hash')
        if not isinstance(self.capacity_unit, FillCapacityUnit):
            raise ValueError('Plan capacity unit must be FillCapacityUnit.')
        if type(self.intents) is not tuple:
            raise ValueError('FillPlan intents must be an immutable tuple.')
        if any(not isinstance(intent, FillIntent) for intent in self.intents):
            raise ValueError('FillPlan entries must be FillIntent values.')
        intent_keys = [intent.idempotency_key for intent in self.intents]
        if len(set(intent_keys)) != len(intent_keys):
            raise ValueError('FillPlan intent idempotency keys must be unique.')
        common_service_fence: tuple[str, int, str, int] | None = None
        for expected_ordinal, intent in enumerate(self.intents):
            if intent.ordinal != expected_ordinal:
                raise ValueError('FillPlan intent ordinals must be contiguous '
                                 'and match tuple order.')
            if (intent.policy_revision != self.policy_revision or
                    intent.reconcile_generation != self.reconcile_generation or
                    intent.allocation_generation != self.allocation_generation
                    or intent.allocation_input_sha256
                    != self.allocation_input_sha256 or
                    intent.allocation_claim_generation
                    != self.allocation_claim_generation or
                    intent.reconciliation_gate_generation
                    != self.reconciliation_gate_generation or
                    intent.reclaim_fleet_bundle_sha256
                    != self.reclaim_fleet_bundle_sha256 or
                    intent.reclaim_policy_revision
                    != self.reclaim_policy_revision or
                    intent.reclaim_provider_inventory_sha256
                    != self.reclaim_provider_inventory_sha256 or
                    intent.capacity_unit != self.capacity_unit):
                raise ValueError('FillPlan intent authority does not match '
                                 'the plan authority.')
            service_fence = (intent.service_incarnation, intent.service_version,
                             intent.controller_owner, intent.service_generation)
            if common_service_fence is None:
                common_service_fence = service_fence
            elif common_service_fence != service_fence:
                raise ValueError('FillPlan intents must share one service '
                                 'authority fence.')


class DeferredFillReason(enum.Enum):
    """Typed causes for an intent not admitted durably."""

    PROVIDER_QUEUE_BACKPRESSURE = 'provider_queue_backpressure'
    STALE_OBSERVATION = 'stale_observation'
    ADMISSION_SEQUENCE_CHANGED = 'admission_sequence_changed'
    SUPERSEDED_POLICY = 'superseded_policy'
    LOST_OWNER = 'lost_owner'
    CHANGED_EPOCH = 'changed_epoch'
    PHYSICAL_CLUSTER_UID_MISMATCH = 'physical_cluster_uid_mismatch'
    MAX_REPLICAS_EXHAUSTED = 'max_replicas_exhausted'


@dataclasses.dataclass(frozen=True)
class DeferredFillIntent:
    """One planned intent rejected or deferred by durable admission."""

    intent: FillIntent
    reason: DeferredFillReason
    detail: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.intent, FillIntent):
            raise ValueError('Deferred fill entries require a FillIntent.')
        if not isinstance(self.reason, DeferredFillReason):
            raise ValueError('Deferred fill entries require a typed reason.')
        if self.detail is not None:
            _require_nonempty_string(self.detail, 'Deferral detail')


@dataclasses.dataclass(frozen=True)
class AcceptedFillIntent:
    """Durable acceptance keyed to one exact planned intent.

    During the Serve052 transition, direct admission returns the replica ID
    while grant-before-row admission returns ``None`` until the pool executor
    materializes the row. Both are durable acceptance receipts and force a
    fresh capacity replan before paid residual is published.
    """

    intent_idempotency_key: str
    replica_id: int | None

    def __post_init__(self) -> None:
        _require_sha256(self.intent_idempotency_key,
                        'Accepted intent idempotency key')
        if self.replica_id is not None:
            _require_int(self.replica_id, 'Accepted replica ID', minimum=1)


@dataclasses.dataclass(frozen=True)
class FillCommitResult:
    """Typed bijective receipt for rows created from one fill plan."""

    accepted: tuple[AcceptedFillIntent, ...]
    deferred: tuple[DeferredFillIntent, ...]
    authority_current: bool

    def __post_init__(self) -> None:
        if type(self.accepted) is not tuple or any(
                not isinstance(item, AcceptedFillIntent)
                for item in self.accepted):
            raise ValueError('Accepted entries must be an immutable tuple of '
                             'AcceptedFillIntent values.')
        replica_ids = [
            item.replica_id
            for item in self.accepted
            if item.replica_id is not None
        ]
        if len(set(replica_ids)) != len(replica_ids):
            raise ValueError('Accepted replica IDs must be unique.')
        accepted_keys = [item.intent_idempotency_key for item in self.accepted]
        if len(set(accepted_keys)) != len(accepted_keys):
            raise ValueError('Accepted intent idempotency keys must be unique.')
        if type(self.deferred) is not tuple or any(
                not isinstance(item, DeferredFillIntent)
                for item in self.deferred):
            raise ValueError('Deferred entries must be an immutable tuple of '
                             'DeferredFillIntent values.')
        if type(self.authority_current) is not bool:
            raise ValueError('authority_current must be a boolean.')

    def validate_for_plan(self, plan: FillPlan) -> None:
        """Reject a partial, duplicated, or foreign admission receipt."""
        if not isinstance(plan, FillPlan):
            raise ValueError('Commit receipt validation requires a FillPlan.')
        plan_by_key = {
            intent.idempotency_key: intent for intent in plan.intents
        }
        accepted_keys = {item.intent_idempotency_key for item in self.accepted}
        foreign_accepted = accepted_keys - set(plan_by_key)
        if foreign_accepted:
            raise ValueError('Commit receipt accepts an intent from a '
                             'different plan.')
        deferred_keys: set[str] = set()
        for deferred in self.deferred:
            key = deferred.intent.idempotency_key
            if key in deferred_keys:
                raise ValueError('Commit receipt defers an intent more than '
                                 'once.')
            expected = plan_by_key.get(key)
            if expected is None or expected != deferred.intent:
                raise ValueError('Commit receipt contains an intent from a '
                                 'different plan.')
            deferred_keys.add(key)
        if accepted_keys.intersection(deferred_keys):
            raise ValueError('Commit receipt cannot both accept and defer the '
                             'same intent.')
        if accepted_keys.union(deferred_keys) != set(plan_by_key):
            raise ValueError('Commit receipt must account for every planned '
                             'intent exactly once by idempotency key.')

    def accepted_intents_for_plan(
            self, plan: FillPlan) -> tuple[tuple[FillIntent, int | None], ...]:
        """Return exact accepted intent receipts in plan order."""
        self.validate_for_plan(plan)
        replica_by_intent = {
            item.intent_idempotency_key: item.replica_id
            for item in self.accepted
        }
        return tuple((intent, replica_by_intent[intent.idempotency_key])
                     for intent in plan.intents
                     if intent.idempotency_key in replica_by_intent)

    def accepted_rotation_anchor(self, plan: FillPlan) -> str | None:
        """Return the first durably accepted pool, never a planned-only one."""
        accepted = self.accepted_intents_for_plan(plan)
        return accepted[0][0].pool_key if accepted else None


class ReservedFillPlanner:
    """Stateless protocol-v2 fill planner."""

    @staticmethod
    def plan(
        *,
        policy_revision: int,
        reconcile_generation: int,
        allocation_map: AuthenticatedAllocationMap,
        service_incarnation: str,
        service_version: int,
        controller_owner: str,
        max_replicas: int,
        planned_replicas: int,
        capacity_unit: FillCapacityUnit,
        committed_fill_debits: tuple[CommittedFillDebit, ...] = (),
        rotation_anchor: str | None = None,
    ) -> FillPlan:
        """Build a deterministic plan without spending feed or an anchor."""
        _require_int(policy_revision, 'Policy revision', minimum=1)
        _require_int(reconcile_generation, 'Reconcile generation', minimum=1)
        if not isinstance(allocation_map, AuthenticatedAllocationMap):
            raise ValueError('Planner requires an AuthenticatedAllocationMap.')
        allocation_map.validate_authentication()
        _require_nonempty_string(service_incarnation, 'Service incarnation')
        _require_int(service_version, 'Service version', minimum=1)
        if service_version != allocation_map.service_version:
            raise ValueError('Planner service version does not match the '
                             'authenticated allocation map.')
        _require_nonempty_string(controller_owner, 'Controller owner')
        _require_int(max_replicas, 'max_replicas')
        _require_int(planned_replicas, 'planned_replicas')
        if not isinstance(capacity_unit, FillCapacityUnit):
            raise ValueError('capacity_unit must be FillCapacityUnit.')
        if type(committed_fill_debits) is not tuple or any(
                not isinstance(debit, CommittedFillDebit)
                for debit in committed_fill_debits):
            raise ValueError('Committed-fill debits must be an immutable '
                             'tuple of CommittedFillDebit values.')
        if rotation_anchor is not None:
            _require_nonempty_string(rotation_anchor, 'Rotation anchor')

        pool_snapshots = allocation_map.pool_snapshots
        pool_keys = [snapshot.pool_key for snapshot in pool_snapshots]
        ordered_snapshots = list(pool_snapshots)
        if rotation_anchor in pool_keys:
            start = pool_keys.index(rotation_anchor) + 1
            ordered_snapshots = (ordered_snapshots[start:] +
                                 ordered_snapshots[:start])

        remaining_by_pool_card: dict[str, dict[str, int]] = {}
        card_order_by_pool: dict[str, tuple[str, ...]] = {}
        display_shape_by_pool_card: dict[str, dict[str, tuple[str, int]]] = {}
        locations_by_pool_card: dict[str, dict[str, tuple[LocationSnapshot,
                                                          ...]]] = {}
        remaining_by_pool: dict[str, int] = {}
        for snapshot in ordered_snapshots:
            card_order: list[str] = []
            display_shapes: dict[str, tuple[str, int]] = {}
            locations: dict[str, list[LocationSnapshot]] = {}
            for location in snapshot.locations:
                card = location.accelerator.casefold()
                if card not in locations:
                    card_order.append(card)
                    locations[card] = []
                    display_shapes[card] = (location.accelerator,
                                            location.accelerator_count)
                locations[card].append(location)
            if snapshot.free_slots_by_accelerator is None:
                exact_slots = {
                    card: snapshot.free_slots if len(card_order) == 1 else 0
                    for card in card_order
                }
            else:
                exact_slots = dict.fromkeys(card_order, 0)
                exact_slots.update(snapshot.free_slots_by_accelerator)
            remaining_by_pool_card[snapshot.pool_key] = exact_slots
            card_order_by_pool[snapshot.pool_key] = tuple(card_order)
            display_shape_by_pool_card[snapshot.pool_key] = display_shapes
            locations_by_pool_card[snapshot.pool_key] = {
                card: tuple(card_locations)
                for card, card_locations in locations.items()
            }
            remaining_by_pool[snapshot.pool_key] = min(snapshot.free_slots,
                                                       snapshot.grant,
                                                       snapshot.edge_cap)

        committed_debit_keys: set[tuple[str, str]] = set()
        for debit in committed_fill_debits:
            if (debit.allocation_generation
                    != allocation_map.allocation_generation or
                    debit.allocation_input_sha256
                    != allocation_map.allocation_input_sha256 or
                    debit.allocation_claim_generation
                    != allocation_map.allocation_claim_generation):
                raise ValueError('Committed-fill debit references a different '
                                 'authenticated allocation map.')
            debit_key = (debit.pool_key, debit.accelerator)
            if debit_key in committed_debit_keys:
                raise ValueError('Committed-fill debits must contain one '
                                 'entry per pool/card.')
            committed_debit_keys.add(debit_key)
            if debit.pool_key not in remaining_by_pool_card:
                raise ValueError('Committed-fill debit references a pool '
                                 'outside the authenticated allocation map.')
            exact_slots = remaining_by_pool_card[debit.pool_key]
            if debit.accelerator not in card_order_by_pool[debit.pool_key]:
                raise ValueError('Committed-fill debit references a card '
                                 'outside its authenticated pool locations.')
            exact_slots[debit.accelerator] = max(
                0, exact_slots[debit.accelerator] - debit.replica_slots)
            remaining_by_pool[debit.pool_key] = max(
                0, remaining_by_pool[debit.pool_key] - debit.replica_slots)

        hard_headroom = max(0, max_replicas - planned_replicas)
        intents: list[FillIntent] = []
        while hard_headroom > 0:
            made_progress = False
            for snapshot in ordered_snapshots:
                if hard_headroom <= 0:
                    break
                pool_key = snapshot.pool_key
                if remaining_by_pool[pool_key] <= 0:
                    continue
                selected_card: str | None = None
                for card in card_order_by_pool[pool_key]:
                    if remaining_by_pool_card[pool_key].get(card, 0) <= 0:
                        continue
                    _, accelerator_count = (
                        display_shape_by_pool_card[pool_key][card])
                    if capacity_unit.intent_cost(
                            accelerator_count) <= hard_headroom:
                        selected_card = card
                        break
                if selected_card is None:
                    continue
                display_card, accelerator_count = (
                    display_shape_by_pool_card[pool_key][selected_card])
                assert snapshot.grant_epoch is not None
                allowed_locations = locations_by_pool_card[pool_key][
                    selected_card]
                intents.append(
                    FillIntent.create(
                        ordinal=len(intents),
                        protocol_version=snapshot.protocol_version,
                        policy_revision=policy_revision,
                        reconcile_generation=reconcile_generation,
                        allocation_generation=(
                            allocation_map.allocation_generation),
                        allocation_input_sha256=(
                            allocation_map.allocation_input_sha256),
                        allocation_claim_generation=(
                            allocation_map.allocation_claim_generation),
                        reconciliation_gate_generation=(
                            allocation_map.reconciliation_gate_generation),
                        reclaim_fleet_bundle_sha256=(
                            allocation_map.reclaim_fleet_bundle_sha256),
                        reclaim_policy_revision=(
                            allocation_map.reclaim_policy_revision),
                        reclaim_provider_inventory_sha256=(
                            allocation_map.reclaim_provider_inventory_sha256),
                        service_incarnation=service_incarnation,
                        service_version=service_version,
                        controller_owner=controller_owner,
                        service_generation=snapshot.service_generation,
                        pool_key=pool_key,
                        pool_epoch=snapshot.grant_epoch,
                        physical_cluster_uid=snapshot.physical_cluster_uid,
                        worker_projection_sha256=dict(
                            snapshot.worker_projection_sha256_by_accelerator)
                        [selected_card],
                        observation_generation=snapshot.observation_generation,
                        observation_sequence=snapshot.observation_sequence,
                        ordinary_zero_cost_admission_sequence=(
                            snapshot.ordinary_zero_cost_admission_sequence),
                        valid_until=snapshot.valid_until,
                        accelerator=display_card,
                        accelerator_count=accelerator_count,
                        capacity_unit=capacity_unit,
                        allowed_locations=allowed_locations,
                    ))
                remaining_by_pool_card[pool_key][selected_card] -= 1
                remaining_by_pool[pool_key] -= 1
                hard_headroom -= capacity_unit.intent_cost(accelerator_count)
                made_progress = True
            if not made_progress:
                break

        return FillPlan(
            policy_revision=policy_revision,
            reconcile_generation=reconcile_generation,
            allocation_generation=allocation_map.allocation_generation,
            allocation_input_sha256=allocation_map.allocation_input_sha256,
            allocation_claim_generation=(
                allocation_map.allocation_claim_generation),
            reconciliation_gate_generation=(
                allocation_map.reconciliation_gate_generation),
            reclaim_fleet_bundle_sha256=(
                allocation_map.reclaim_fleet_bundle_sha256),
            reclaim_policy_revision=allocation_map.reclaim_policy_revision,
            reclaim_provider_inventory_sha256=(
                allocation_map.reclaim_provider_inventory_sha256),
            capacity_unit=capacity_unit,
            intents=tuple(intents),
        )
