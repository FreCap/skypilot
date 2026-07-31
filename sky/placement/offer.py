"""Generic, provider-independent V1 placement offer contracts."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import hashlib
import json
import re
import typing
import unicodedata

from sky.utils.json_types import freeze_json
from sky.utils.json_types import FrozenJSONDict
from sky.utils.json_types import JSONValue
from sky.utils.json_types import thaw_json

if typing.TYPE_CHECKING:
    from sky import resources as resources_lib

_SCHEMA_VERSION = 1
_PROVIDER_PAYLOAD_VERSION = 1
_MAX_PROVIDER_PAYLOAD_BYTES = 4_096
_MAX_ENVELOPE_BYTES = 16_384
_MAX_PAYLOAD_KEYS = 64
_MAX_PAYLOAD_ARRAY_ELEMENTS = 128
_MAX_PAYLOAD_CONTAINER_DEPTH = 4
_DIGEST_PATTERN = re.compile(r'^sha256:[0-9a-f]{64}$')
_PROVIDER_PATTERN = re.compile(r'^[a-z0-9](?:[a-z0-9._-]{0,61}[a-z0-9])?$')
_LOWER_ENUM_PATTERN = re.compile(r'^[a-z0-9_]{1,128}$')
_TIER_PATTERN = re.compile(r'^[a-z0-9_-]{1,64}$')
_TIMESTAMP_PATTERN = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:'
                                r'[0-9]{2}:[0-9]{2}Z$')
_DECIMAL_PATTERN = re.compile(
    r'^(?:0|[1-9][0-9]{0,37})(?:\.[0-9]{0,17}[1-9])?$')
_CAPTURE_ID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-'
                                 r'[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
_PAYLOAD_SECRET_SEGMENTS = frozenset({
    'secret',
    'password',
    'passwd',
    'token',
    'credential',
    'credentials',
    'kubeconfig',
    'authorization',
    'cookie',
})
_PAYLOAD_SECRET_PAIRS = frozenset({
    ('api', 'key'),
    ('access', 'key'),
    ('private', 'key'),
    ('client', 'secret'),
})
_PAYLOAD_SECRET_UNSPLIT = frozenset({
    'apikey',
    'accesskey',
    'privatekey',
    'clientsecret',
})


class OfferOperationV1(enum.Enum):
    PLAN_CREATE = 'plan_create'
    FRESH_CREATE = 'fresh_create'
    REUSE = 'reuse'
    RESTART = 'restart'


class OfferActuationKindV1(enum.Enum):
    DIRECT_POD = 'direct_pod'
    CONTROLLER = 'controller'
    HA_DEPLOYMENT = 'ha_deployment'
    UNKNOWN = 'unknown'


class ObservationFreshnessV1(enum.Enum):
    ALLOW_REQUEST_CACHE = 'allow_request_cache'
    REQUIRE_FRESH = 'require_fresh'


class OfferReasonCodeV1(enum.Enum):
    """Closed V1 source, orchestration, and revalidation dispositions."""

    NONE = 'none'
    NO_FEASIBLE_SHAPE = 'no_feasible_shape'
    UNSUPPORTED_OPERATION = 'unsupported_operation'
    UNSUPPORTED_ACTUATION_KIND = 'unsupported_actuation_kind'
    UNSUPPORTED_NODE_COUNT = 'unsupported_node_count'
    UNSUPPORTED_ACCELERATOR = 'unsupported_accelerator'
    UNSUPPORTED_RESOURCE_MODE = 'unsupported_resource_mode'
    UNSUPPORTED_NETWORK_TIER = 'unsupported_network_tier'
    VOLUME_OR_STORAGE_MOUNT = 'volume_or_storage_mount'
    KUEUE_ENABLED = 'kueue_enabled'
    RESERVATION_REQUESTED = 'reservation_requested'
    CUSTOM_PLACEMENT_CONFIG = 'custom_placement_config'
    UNRESOLVED_SCOPE = 'unresolved_scope'
    CONTEXT_UNREACHABLE = 'context_unreachable'
    SCOPE_CHANGED = 'scope_changed'
    CONFIGURATION_CHANGED = 'configuration_changed'
    SHAPE_NO_LONGER_SUPPORTED = 'shape_no_longer_supported'
    CAPACITY_UNAVAILABLE = 'capacity_unavailable'
    QUOTA_UNAVAILABLE = 'quota_unavailable'
    OFFER_IDENTITY_CHANGED = 'offer_identity_changed'
    OBSERVATION_LIMIT_EXCEEDED = 'observation_limit_exceeded'
    PROVIDER_OBJECT_CONFLICT = 'provider_object_conflict'
    SOURCE_ERROR = 'source_error'
    RETRY_AFTER_PROVIDER_ATTEMPT = 'retry_after_provider_attempt'


class OfferSetStatusV1(enum.Enum):
    OK = 'ok'
    NO_OFFERS = 'no_offers'
    NOT_REPRESENTABLE = 'not_representable'


class OfferRevalidationStatusV1(enum.Enum):
    VALID = 'valid'
    UNAVAILABLE = 'unavailable'
    NOT_REPRESENTABLE = 'not_representable'


class OfferPriceBasisV1(enum.Enum):
    NODE_HOUR = 'node_hour'


class OfferCurrencyV1(enum.Enum):
    USD = 'USD'


class OfferPurchaseModeV1(enum.Enum):
    ON_DEMAND = 'on_demand'


class OfferAvailabilityV1(enum.Enum):
    UNKNOWN = 'unknown'
    UNAVAILABLE = 'unavailable'


class OfferRevalidationPolicyV1(enum.Enum):
    BEFORE_MUTATION = 'before_mutation'


class OfferReservationEvidenceV1(enum.Enum):
    NOT_APPLICABLE = 'not_applicable'


class OfferQuotaEvidenceV1(enum.Enum):
    UNKNOWN = 'unknown'
    UNAVAILABLE = 'unavailable'


class OfferCapacityEvidenceV1(enum.Enum):
    SHAPE_FITS_EXISTING_NODE = 'shape_fits_existing_node'
    CONTEXT_UNREACHABLE = 'context_unreachable'
    SHAPE_NO_LONGER_SUPPORTED = 'shape_no_longer_supported'
    CAPACITY_UNAVAILABLE = 'capacity_unavailable'
    PROVIDER_OBJECT_CONFLICT = 'provider_object_conflict'


class ProviderPayloadNodeKindV1(enum.Enum):
    STRING = 'string'
    DIGEST = 'digest'
    INTEGER = 'integer'
    BOOLEAN = 'boolean'
    NULL = 'null'
    OBJECT = 'object'
    ARRAY = 'array'


class PlacementOfferActuationModeV1(enum.Enum):
    SHADOW = 'shadow'
    SHADOW_LEGACY_FALLBACK = 'shadow_legacy_fallback'
    AUTHORITATIVE = 'authoritative'
    LEGACY_FIRST_ATTEMPT = 'legacy_first_attempt'
    LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT = (
        'legacy_retry_after_provider_attempt')


_NOT_REPRESENTABLE_OFFER_SET_REASONS = frozenset({
    OfferReasonCodeV1.UNSUPPORTED_OPERATION,
    OfferReasonCodeV1.UNSUPPORTED_ACTUATION_KIND,
    OfferReasonCodeV1.UNSUPPORTED_NODE_COUNT,
    OfferReasonCodeV1.UNSUPPORTED_ACCELERATOR,
    OfferReasonCodeV1.UNSUPPORTED_RESOURCE_MODE,
    OfferReasonCodeV1.UNSUPPORTED_NETWORK_TIER,
    OfferReasonCodeV1.VOLUME_OR_STORAGE_MOUNT,
    OfferReasonCodeV1.KUEUE_ENABLED,
    OfferReasonCodeV1.RESERVATION_REQUESTED,
    OfferReasonCodeV1.CUSTOM_PLACEMENT_CONFIG,
    OfferReasonCodeV1.UNRESOLVED_SCOPE,
    OfferReasonCodeV1.OBSERVATION_LIMIT_EXCEEDED,
})
_NOT_REPRESENTABLE_REVALIDATION_REASONS = frozenset({
    OfferReasonCodeV1.SCOPE_CHANGED,
    OfferReasonCodeV1.CONFIGURATION_CHANGED,
    OfferReasonCodeV1.OFFER_IDENTITY_CHANGED,
})


@dataclasses.dataclass(frozen=True)
class OfferRequestV1:
    """Provider-independent input to a placement offer source."""

    resources: resources_lib.Resources
    num_nodes: int
    workspace: str | None
    has_volume_mounts: bool
    has_storage_mounts: bool
    operation: OfferOperationV1
    actuation_kind: OfferActuationKindV1

    def __post_init__(self) -> None:
        _require_int_range(self.num_nodes, 1, 10_000, 'num_nodes')
        if self.workspace is not None and type(self.workspace) is not str:
            raise ValueError('workspace must be a string or None.')
        _require_exact_bool(self.has_volume_mounts, 'has_volume_mounts')
        _require_exact_bool(self.has_storage_mounts, 'has_storage_mounts')
        _require_enum(self.operation, OfferOperationV1, 'operation')
        _require_enum(self.actuation_kind, OfferActuationKindV1,
                      'actuation_kind')


@typing.runtime_checkable
class ProviderObservationSnapshotV1(typing.Protocol):
    """Process-local immutable provider observation provenance."""

    @property
    def provider(self) -> str:
        ...

    @property
    def observed_at(self) -> datetime.datetime:
        ...

    @property
    def capture_id(self) -> str:
        ...


@typing.runtime_checkable
class ProviderActuationContextV1(typing.Protocol):
    """Request-local exact-target transport used for actuation."""

    @property
    def provider(self) -> str:
        ...

    @property
    def capture_id(self) -> str:
        ...

    def close(self) -> None:
        ...


@dataclasses.dataclass(frozen=True)
class ObservationCaptureV1:
    """A provider observation and its optional matching transport."""

    observation: ProviderObservationSnapshotV1
    actuation_context: ProviderActuationContextV1 | None

    def __post_init__(self) -> None:
        _validate_observation(self.observation)
        context = self.actuation_context
        if context is None:
            return
        _validate_actuation_context(context)
        if context.provider != self.observation.provider:
            raise ValueError('Observation and context providers differ.')
        if context.capture_id != self.observation.capture_id:
            raise ValueError('Observation and context captures differ.')


@typing.runtime_checkable
class OfferSourceV1(typing.Protocol):
    """Read-only provider placement offer source."""

    def capture_observation(
        self,
        request: OfferRequestV1,
        *,
        observed_at: datetime.datetime,
        freshness: ObservationFreshnessV1,
    ) -> ObservationCaptureV1:
        ...

    def list_offers(
        self,
        request: OfferRequestV1,
        *,
        observation: ProviderObservationSnapshotV1,
    ) -> OfferSetResultV1:
        ...

    def revalidate(
        self,
        offer: PlacementOfferV1,
        request: OfferRequestV1,
        *,
        observation: ProviderObservationSnapshotV1,
    ) -> OfferRevalidationResultV1:
        ...


@dataclasses.dataclass(frozen=True)
class OfferScopeV1:
    """Opaque provider scope identity."""

    kind: str
    id: str

    def __post_init__(self) -> None:
        _require_lower_enum(self.kind, 'scope.kind')
        _require_digest(self.id, 'scope.id')

    def to_json(self) -> dict[str, JSONValue]:
        return {'kind': self.kind, 'id': self.id}


@dataclasses.dataclass(frozen=True)
class OfferAcceleratorV1:
    """Canonical accelerator name and count."""

    name: str
    count: int

    def __post_init__(self) -> None:
        _require_bounded_text(self.name,
                              field='accelerator.name',
                              maximum_bytes=128)
        _require_int_range(self.count, 1, 2_147_483_647, 'accelerator.count')

    def to_json(self) -> dict[str, JSONValue]:
        return {'name': self.name, 'count': self.count}


@dataclasses.dataclass(frozen=True)
class OfferResourcesV1:
    """Canonical resources entering stable placement identity."""

    instance_type: str
    cpus: str
    memory_gib: str
    accelerators: tuple[OfferAcceleratorV1, ...]
    disk_tier: str | None
    network_tier: str | None
    placement_constraints_digest: str | None

    def __post_init__(self) -> None:
        _require_bounded_text(self.instance_type,
                              field='resources.instance_type',
                              maximum_bytes=256)
        _require_decimal(self.cpus, 'resources.cpus')
        _require_decimal(self.memory_gib, 'resources.memory_gib')
        if type(self.accelerators) is not tuple:
            raise ValueError('resources.accelerators must be an exact tuple.')
        if len(self.accelerators) > 8:
            raise ValueError('resources.accelerators exceeds 8 entries.')
        if any(
                type(item) is not OfferAcceleratorV1
                for item in self.accelerators):
            raise ValueError('resources.accelerators must contain exact '
                             'OfferAcceleratorV1 objects.')
        names = tuple(item.name for item in self.accelerators)
        if len(names) != len(set(names)):
            raise ValueError('resources.accelerator names must be unique.')
        if names != tuple(sorted(names)):
            raise ValueError(
                'resources.accelerators must already be sorted by name.')
        _require_optional_tier(self.disk_tier, 'resources.disk_tier')
        _require_optional_tier(self.network_tier, 'resources.network_tier')
        if self.placement_constraints_digest is not None:
            _require_digest(self.placement_constraints_digest,
                            'resources.placement_constraints_digest')

    def to_json(self) -> dict[str, JSONValue]:
        return {
            'instance_type': self.instance_type,
            'cpus': self.cpus,
            'memory_gib': self.memory_gib,
            'accelerators': [
                accelerator.to_json() for accelerator in self.accelerators
            ],
            'disk_tier': self.disk_tier,
            'network_tier': self.network_tier,
            'placement_constraints_digest': self.placement_constraints_digest,
        }


@dataclasses.dataclass(frozen=True)
class OfferPriceV1:
    """Canonical per-node offer price."""

    amount: str
    basis: OfferPriceBasisV1
    currency: OfferCurrencyV1

    def __post_init__(self) -> None:
        _require_decimal(self.amount, 'price.amount')
        _require_enum(self.basis, OfferPriceBasisV1, 'price.basis')
        _require_enum(self.currency, OfferCurrencyV1, 'price.currency')

    def to_json(self) -> dict[str, JSONValue]:
        return {
            'amount': self.amount,
            'basis': self.basis.value,
            'currency': self.currency.value,
        }


@dataclasses.dataclass(frozen=True)
class OfferEvidenceV1:
    """Closed reservation, quota, and capacity evidence row."""

    reservation: OfferReservationEvidenceV1
    quota: OfferQuotaEvidenceV1
    capacity: OfferCapacityEvidenceV1
    requested_nodes: int

    def __post_init__(self) -> None:
        _require_enum(self.reservation, OfferReservationEvidenceV1,
                      'evidence.reservation')
        _require_enum(self.quota, OfferQuotaEvidenceV1, 'evidence.quota')
        _require_enum(self.capacity, OfferCapacityEvidenceV1,
                      'evidence.capacity')
        _require_int_range(self.requested_nodes, 1, 10_000,
                           'evidence.requested_nodes')

    def to_json(self) -> dict[str, JSONValue]:
        return {
            'reservation': self.reservation.value,
            'quota': self.quota.value,
            'capacity': self.capacity.value,
            'requested_nodes': self.requested_nodes,
        }


@dataclasses.dataclass(frozen=True)
class ProviderPayloadSchemaNodeV1:
    """One immutable node in a provider payload allowlist."""

    kind: ProviderPayloadNodeKindV1
    fields: tuple[tuple[str, ProviderPayloadSchemaNodeV1], ...] = ()
    item: ProviderPayloadSchemaNodeV1 | None = None
    allowed_strings: tuple[str, ...] = ()
    allow_empty: bool = False

    def __post_init__(self) -> None:
        _require_enum(self.kind, ProviderPayloadNodeKindV1, 'schema.kind')
        if type(self.fields) is not tuple:
            raise ValueError('schema.fields must be an exact tuple.')
        if len(self.fields) > 32:
            raise ValueError('schema.fields exceeds 32 object keys.')
        validated_fields: list[tuple[str, ProviderPayloadSchemaNodeV1]] = []
        for field in self.fields:
            if type(field) is not tuple or len(field) != 2:
                raise ValueError(
                    'schema.fields entries must be exact two-tuples.')
            key, child = field
            _require_payload_key(key, 'schema.fields')
            if type(child) is not ProviderPayloadSchemaNodeV1:
                raise ValueError(
                    'schema.fields children must be exact schema nodes.')
            validated_fields.append((key, child))
        field_keys = tuple(key for key, _ in validated_fields)
        if len(field_keys) != len(set(field_keys)):
            raise ValueError('schema.fields keys must be unique.')
        if field_keys != tuple(sorted(field_keys)):
            raise ValueError('schema.fields keys must already be sorted.')
        if type(self.allowed_strings) is not tuple:
            raise ValueError('schema.allowed_strings must be an exact tuple.')
        for value in self.allowed_strings:
            _require_bounded_text(value,
                                  field='schema.allowed_strings',
                                  maximum_bytes=1024,
                                  allow_empty=self.allow_empty)
        if len(self.allowed_strings) != len(set(self.allowed_strings)):
            raise ValueError('schema.allowed_strings must be unique.')
        if self.allowed_strings != tuple(sorted(self.allowed_strings)):
            raise ValueError('schema.allowed_strings must already be sorted.')
        _require_exact_bool(self.allow_empty, 'schema.allow_empty')

        if self.kind is ProviderPayloadNodeKindV1.OBJECT:
            if self.item is not None:
                raise ValueError('OBJECT schema nodes cannot have an item.')
            if self.allowed_strings:
                raise ValueError(
                    'OBJECT schema nodes cannot have allowed strings.')
            if self.allow_empty:
                raise ValueError(
                    'OBJECT schema nodes cannot allow empty strings.')
            return
        if self.kind is ProviderPayloadNodeKindV1.ARRAY:
            if self.fields:
                raise ValueError('ARRAY schema nodes cannot have fields.')
            if type(self.item) is not ProviderPayloadSchemaNodeV1:
                raise ValueError(
                    'ARRAY schema nodes require one exact schema-node item.')
            if self.allowed_strings:
                raise ValueError(
                    'ARRAY schema nodes cannot have allowed strings.')
            if self.allow_empty:
                raise ValueError(
                    'ARRAY schema nodes cannot allow empty strings.')
            return

        if self.fields:
            raise ValueError('Scalar schema nodes cannot have fields.')
        if self.item is not None:
            raise ValueError('Scalar schema nodes cannot have an item.')
        if self.kind is not ProviderPayloadNodeKindV1.STRING:
            if self.allowed_strings:
                raise ValueError(
                    'Only STRING schema nodes can have allowed strings.')
            if self.allow_empty:
                raise ValueError(
                    'Only STRING schema nodes can allow empty strings.')


@dataclasses.dataclass(frozen=True)
class ProviderPayloadSchemaV1:
    """Provider-owned allowlists for identity and observation payloads."""

    provider: str
    identity: ProviderPayloadSchemaNodeV1
    observation: ProviderPayloadSchemaNodeV1

    def __post_init__(self) -> None:
        _require_provider(self.provider)
        if type(self.identity) is not ProviderPayloadSchemaNodeV1:
            raise ValueError('schema.identity must be an exact schema node.')
        if type(self.observation) is not ProviderPayloadSchemaNodeV1:
            raise ValueError('schema.observation must be an exact schema node.')
        if self.identity.kind is not ProviderPayloadNodeKindV1.OBJECT:
            raise ValueError('schema.identity root must be OBJECT.')
        if self.observation.kind is not ProviderPayloadNodeKindV1.OBJECT:
            raise ValueError('schema.observation root must be OBJECT.')


@dataclasses.dataclass(frozen=True, init=False)
class OfferProviderPayloadV1:
    """Factory-validated immutable provider payload."""

    version: int
    identity: FrozenJSONDict
    observation: FrozenJSONDict

    def __init__(self) -> None:
        raise TypeError('Use OfferProviderPayloadV1.create().')

    @classmethod
    def create(
        cls,
        *,
        identity: dict[str, JSONValue],
        observation: dict[str, JSONValue],
        payload_schema: ProviderPayloadSchemaV1,
    ) -> OfferProviderPayloadV1:
        _require_payload_schema(payload_schema)
        counters = _PayloadCounters()
        identity_copy = _validate_payload_value(
            identity,
            payload_schema.identity,
            path='provider_payload.identity',
            container_depth=0,
            counters=counters)
        observation_copy = _validate_payload_value(
            observation,
            payload_schema.observation,
            path='provider_payload.observation',
            container_depth=0,
            counters=counters)
        _validate_payload_counts(counters)
        frozen_identity = typing.cast(FrozenJSONDict,
                                      freeze_json(identity_copy))
        frozen_observation = typing.cast(FrozenJSONDict,
                                         freeze_json(observation_copy))
        instance = object.__new__(cls)
        object.__setattr__(instance, 'version', _PROVIDER_PAYLOAD_VERSION)
        object.__setattr__(instance, 'identity', frozen_identity)
        object.__setattr__(instance, 'observation', frozen_observation)
        if len(canonical_json_bytes_v1(
                instance.to_json())) > _MAX_PROVIDER_PAYLOAD_BYTES:
            raise ValueError('provider_payload exceeds 4 KiB.')
        return instance

    def to_json(self) -> dict[str, JSONValue]:
        return {
            'version': self.version,
            'identity': typing.cast(dict[str, JSONValue],
                                    thaw_json(self.identity)),
            'observation': typing.cast(dict[str, JSONValue],
                                       thaw_json(self.observation)),
        }


@dataclasses.dataclass(frozen=True, init=False)
class PlacementOfferV1:
    """Factory-validated immutable V1 placement decision."""

    schema_version: int
    operation: OfferOperationV1
    actuation_kind: OfferActuationKindV1
    offer_id: str
    observation_id: str
    provider: str
    scope: OfferScopeV1
    resources: OfferResourcesV1
    region: str
    candidate_zones: tuple[str, ...]
    batching_scope: str
    price: OfferPriceV1
    purchase_mode: OfferPurchaseModeV1
    availability: OfferAvailabilityV1
    observed_at: datetime.datetime
    ttl_seconds: int
    revalidation_policy: OfferRevalidationPolicyV1
    evidence: OfferEvidenceV1
    provider_payload: OfferProviderPayloadV1

    def __init__(self) -> None:
        raise TypeError(
            'Use PlacementOfferV1.create(), from_envelope(), or from_json().')

    @classmethod
    def create(
        cls,
        *,
        operation: OfferOperationV1,
        actuation_kind: OfferActuationKindV1,
        provider: str,
        scope: OfferScopeV1,
        resources: OfferResourcesV1,
        region: str,
        candidate_zones: tuple[str, ...],
        batching_scope: str,
        price: OfferPriceV1,
        purchase_mode: OfferPurchaseModeV1,
        availability: OfferAvailabilityV1,
        observed_at: datetime.datetime,
        ttl_seconds: int,
        revalidation_policy: OfferRevalidationPolicyV1,
        evidence: OfferEvidenceV1,
        provider_payload: OfferProviderPayloadV1,
        payload_schema: ProviderPayloadSchemaV1,
    ) -> PlacementOfferV1:
        _require_enum(operation, OfferOperationV1, 'operation')
        if operation not in (OfferOperationV1.PLAN_CREATE,
                             OfferOperationV1.FRESH_CREATE):
            raise ValueError(
                'Only PLAN_CREATE and FRESH_CREATE offers are constructible.')
        _require_enum(actuation_kind, OfferActuationKindV1, 'actuation_kind')
        _require_provider(provider)
        _require_payload_schema(payload_schema)
        if payload_schema.provider != provider:
            raise ValueError(
                'payload_schema.provider must equal the offer provider.')
        if type(scope) is not OfferScopeV1:
            raise ValueError('scope must be an exact OfferScopeV1.')
        scope.__post_init__()
        if type(resources) is not OfferResourcesV1:
            raise ValueError('resources must be exact OfferResourcesV1.')
        resources.__post_init__()
        _require_bounded_text(region, field='region', maximum_bytes=1_024)
        if type(candidate_zones) is not tuple:
            raise ValueError('candidate_zones must be an exact tuple.')
        if len(candidate_zones) > 32:
            raise ValueError('candidate_zones exceeds 32 entries.')
        for index, zone in enumerate(candidate_zones):
            _require_bounded_text(zone,
                                  field=f'candidate_zones[{index}]',
                                  maximum_bytes=1_024)
        if len(candidate_zones) != len(set(candidate_zones)):
            raise ValueError('candidate_zones entries must be unique.')
        _require_lower_enum(batching_scope, 'batching_scope')
        if type(price) is not OfferPriceV1:
            raise ValueError('price must be an exact OfferPriceV1.')
        price.__post_init__()
        _require_enum(purchase_mode, OfferPurchaseModeV1, 'purchase_mode')
        _require_enum(availability, OfferAvailabilityV1, 'availability')
        normalized_observed_at = _require_utc_datetime(observed_at,
                                                       'observed_at')
        _require_int_range(ttl_seconds, 1, 300, 'ttl_seconds')
        _require_enum(revalidation_policy, OfferRevalidationPolicyV1,
                      'revalidation_policy')
        if type(evidence) is not OfferEvidenceV1:
            raise ValueError('evidence must be an exact OfferEvidenceV1.')
        evidence.__post_init__()
        _validate_provider_payload_against_schema(provider_payload,
                                                  payload_schema)

        instance = object.__new__(cls)
        object.__setattr__(instance, 'schema_version', _SCHEMA_VERSION)
        object.__setattr__(instance, 'operation', operation)
        object.__setattr__(instance, 'actuation_kind', actuation_kind)
        object.__setattr__(instance, 'offer_id', '')
        object.__setattr__(instance, 'observation_id', '')
        object.__setattr__(instance, 'provider', provider)
        object.__setattr__(instance, 'scope', scope)
        object.__setattr__(instance, 'resources', resources)
        object.__setattr__(instance, 'region', region)
        object.__setattr__(instance, 'candidate_zones', candidate_zones)
        object.__setattr__(instance, 'batching_scope', batching_scope)
        object.__setattr__(instance, 'price', price)
        object.__setattr__(instance, 'purchase_mode', purchase_mode)
        object.__setattr__(instance, 'availability', availability)
        object.__setattr__(instance, 'observed_at', normalized_observed_at)
        object.__setattr__(instance, 'ttl_seconds', ttl_seconds)
        object.__setattr__(instance, 'revalidation_policy', revalidation_policy)
        object.__setattr__(instance, 'evidence', evidence)
        object.__setattr__(instance, 'provider_payload', provider_payload)
        offer_id = _compute_offer_id(instance)
        object.__setattr__(instance, 'offer_id', offer_id)
        object.__setattr__(instance, 'observation_id',
                           _compute_observation_id(instance))
        _validate_offer_envelope_size(instance)
        return instance

    @classmethod
    def from_envelope(
        cls,
        envelope: dict[str, JSONValue],
        *,
        payload_schema: ProviderPayloadSchemaV1,
    ) -> PlacementOfferV1:
        _require_payload_schema(payload_schema)
        if type(envelope) is not dict:
            raise ValueError('offer envelope must be an exact dictionary.')
        _require_json_builtins(envelope, 'offer envelope')
        outer = _require_object(envelope, _OFFER_ENVELOPE_KEYS,
                                'offer envelope')
        schema_version = outer['schema_version']
        if type(schema_version) is not int or schema_version != _SCHEMA_VERSION:
            raise ValueError('schema_version must be integer 1.')
        operation = _parse_enum(outer['operation'], OfferOperationV1,
                                'operation')
        if operation is not OfferOperationV1.FRESH_CREATE:
            raise ValueError(
                'Persisted V1 offer envelopes require FRESH_CREATE.')
        actuation_kind = _parse_enum(outer['actuation_kind'],
                                     OfferActuationKindV1, 'actuation_kind')
        provider = _require_provider(outer['provider'])
        if payload_schema.provider != provider:
            raise ValueError(
                'payload_schema.provider must equal the offer provider.')

        scope_json = _require_object(outer['scope'], _SCOPE_KEYS, 'scope')
        scope = OfferScopeV1(kind=_require_string(scope_json['kind'],
                                                  'scope.kind'),
                             id=_require_string(scope_json['id'], 'scope.id'))

        resources_json = _require_object(outer['resources'], _RESOURCE_KEYS,
                                         'resources')
        accelerator_values = resources_json['accelerators']
        if type(accelerator_values) is not list:
            raise ValueError('resources.accelerators must be a JSON array.')
        accelerators: list[OfferAcceleratorV1] = []
        for index, value in enumerate(accelerator_values):
            accelerator_json = _require_object(
                value, _ACCELERATOR_KEYS, f'resources.accelerators[{index}]')
            accelerators.append(
                OfferAcceleratorV1(
                    name=_require_string(
                        accelerator_json['name'],
                        f'resources.accelerators[{index}].name'),
                    count=_require_int(
                        accelerator_json['count'],
                        f'resources.accelerators[{index}].count')))
        resources = OfferResourcesV1(
            instance_type=_require_string(resources_json['instance_type'],
                                          'resources.instance_type'),
            cpus=_require_string(resources_json['cpus'], 'resources.cpus'),
            memory_gib=_require_string(resources_json['memory_gib'],
                                       'resources.memory_gib'),
            accelerators=tuple(accelerators),
            disk_tier=_require_optional_string(resources_json['disk_tier'],
                                               'resources.disk_tier'),
            network_tier=_require_optional_string(
                resources_json['network_tier'], 'resources.network_tier'),
            placement_constraints_digest=_require_optional_string(
                resources_json['placement_constraints_digest'],
                'resources.placement_constraints_digest'))

        zones_json = outer['candidate_zones']
        if type(zones_json) is not list:
            raise ValueError('candidate_zones must be a JSON array.')
        candidate_zones = tuple(
            _require_string(zone, f'candidate_zones[{index}]')
            for index, zone in enumerate(zones_json))

        price_json = _require_object(outer['price'], _PRICE_KEYS, 'price')
        price = OfferPriceV1(amount=_require_string(price_json['amount'],
                                                    'price.amount'),
                             basis=_parse_enum(price_json['basis'],
                                               OfferPriceBasisV1,
                                               'price.basis'),
                             currency=_parse_enum(price_json['currency'],
                                                  OfferCurrencyV1,
                                                  'price.currency'))
        evidence_json = _require_object(outer['evidence'], _EVIDENCE_KEYS,
                                        'evidence')
        evidence = OfferEvidenceV1(
            reservation=_parse_enum(evidence_json['reservation'],
                                    OfferReservationEvidenceV1,
                                    'evidence.reservation'),
            quota=_parse_enum(evidence_json['quota'], OfferQuotaEvidenceV1,
                              'evidence.quota'),
            capacity=_parse_enum(evidence_json['capacity'],
                                 OfferCapacityEvidenceV1, 'evidence.capacity'),
            requested_nodes=_require_int(evidence_json['requested_nodes'],
                                         'evidence.requested_nodes'))
        payload_json = _require_object(outer['provider_payload'],
                                       _PROVIDER_PAYLOAD_KEYS,
                                       'provider_payload')
        if (type(payload_json['version']) is not int or
                payload_json['version'] != _PROVIDER_PAYLOAD_VERSION):
            raise ValueError('provider_payload.version must be integer 1.')
        identity_json = _require_object_value(payload_json['identity'],
                                              'provider_payload.identity')
        observation_json = _require_object_value(
            payload_json['observation'], 'provider_payload.observation')
        provider_payload = OfferProviderPayloadV1.create(
            identity=identity_json,
            observation=observation_json,
            payload_schema=payload_schema)

        supplied_offer_id = _require_offer_id(outer['offer_id'], provider)
        supplied_observation_id = _require_digest(outer['observation_id'],
                                                  'observation_id')
        result = cls.create(
            operation=operation,
            actuation_kind=actuation_kind,
            provider=provider,
            scope=scope,
            resources=resources,
            region=_require_string(outer['region'], 'region'),
            candidate_zones=candidate_zones,
            batching_scope=_require_string(outer['batching_scope'],
                                           'batching_scope'),
            price=price,
            purchase_mode=_parse_enum(outer['purchase_mode'],
                                      OfferPurchaseModeV1, 'purchase_mode'),
            availability=_parse_enum(outer['availability'], OfferAvailabilityV1,
                                     'availability'),
            observed_at=_parse_timestamp(outer['observed_at']),
            ttl_seconds=_require_int(outer['ttl_seconds'], 'ttl_seconds'),
            revalidation_policy=_parse_enum(outer['revalidation_policy'],
                                            OfferRevalidationPolicyV1,
                                            'revalidation_policy'),
            evidence=evidence,
            provider_payload=provider_payload,
            payload_schema=payload_schema)
        if result.offer_id != supplied_offer_id:
            raise ValueError('offer_id does not match the stable offer fields.')
        if result.observation_id != supplied_observation_id:
            raise ValueError(
                'observation_id does not match the observation fields.')
        if len(canonical_json_bytes_v1(envelope)) > _MAX_ENVELOPE_BYTES:
            raise ValueError('offer envelope exceeds 16 KiB.')
        return result

    @classmethod
    def from_json(
        cls,
        serialized: str | bytes,
        *,
        payload_schema: ProviderPayloadSchemaV1,
    ) -> PlacementOfferV1:
        if type(serialized) is bytes:
            try:
                text = serialized.decode('utf-8', errors='strict')
            except UnicodeDecodeError as error:
                raise ValueError(
                    'serialized offer must be strict UTF-8.') from error
        elif type(serialized) is str:
            text = serialized
        else:
            raise ValueError(
                'serialized offer must be an exact str or bytes value.')
        try:
            parsed = json.loads(text,
                                object_pairs_hook=_reject_duplicate_keys,
                                parse_float=_reject_json_float,
                                parse_constant=_reject_json_constant)
        except (json.JSONDecodeError, UnicodeError) as error:
            raise ValueError(
                'serialized offer is not valid V1 JSON.') from error
        if type(parsed) is not dict:
            raise ValueError('serialized offer root must be an object.')
        return cls.from_envelope(typing.cast(dict[str, JSONValue], parsed),
                                 payload_schema=payload_schema)

    def to_envelope(self) -> dict[str, JSONValue]:
        if self.operation is not OfferOperationV1.FRESH_CREATE:
            raise ValueError('PLAN_CREATE offers cannot be enveloped.')
        envelope = _offer_envelope(self)
        if len(canonical_json_bytes_v1(envelope)) > _MAX_ENVELOPE_BYTES:
            raise ValueError('offer envelope exceeds 16 KiB.')
        return envelope

    def to_json(self) -> str:
        return canonical_json_bytes_v1(self.to_envelope()).decode('utf-8')

    def is_expired(self, now: datetime.datetime) -> bool:
        normalized_now = _require_utc_datetime(now,
                                               'now',
                                               require_whole_seconds=False)
        return (normalized_now - self.observed_at
                >= datetime.timedelta(seconds=self.ttl_seconds))


@dataclasses.dataclass(frozen=True)
class OfferSetResultV1:
    """Closed result of listing placement offers."""

    status: OfferSetStatusV1
    offers: tuple[PlacementOfferV1, ...]
    reason_code: OfferReasonCodeV1

    def __post_init__(self) -> None:
        _require_enum(self.status, OfferSetStatusV1, 'status')
        if type(self.offers) is not tuple:
            raise ValueError('offers must be an exact tuple.')
        if any(type(offer) is not PlacementOfferV1 for offer in self.offers):
            raise ValueError(
                'offers must contain exact PlacementOfferV1 objects.')
        _require_enum(self.reason_code, OfferReasonCodeV1, 'reason_code')
        if self.status is OfferSetStatusV1.OK:
            if not self.offers:
                raise ValueError('OK requires at least one offer.')
            if self.reason_code is not OfferReasonCodeV1.NONE:
                raise ValueError('OK requires reason_code NONE.')
            return
        if self.status is OfferSetStatusV1.NO_OFFERS:
            if self.offers:
                raise ValueError('NO_OFFERS requires an empty offer tuple.')
            if self.reason_code is not OfferReasonCodeV1.NO_FEASIBLE_SHAPE:
                raise ValueError(
                    'NO_OFFERS requires reason_code NO_FEASIBLE_SHAPE.')
            return
        if self.offers:
            raise ValueError('NOT_REPRESENTABLE requires an empty offer tuple.')
        if self.reason_code not in _NOT_REPRESENTABLE_OFFER_SET_REASONS:
            raise ValueError('Invalid NOT_REPRESENTABLE offer-set reason code.')


@dataclasses.dataclass(frozen=True, init=False)
class OfferRevalidationResultV1:
    """Factory-validated result of revalidating one stable offer."""

    status: OfferRevalidationStatusV1
    offer: PlacementOfferV1 | None
    reason_code: OfferReasonCodeV1

    def __init__(self) -> None:
        raise TypeError('Use an OfferRevalidationResultV1 factory method.')

    @classmethod
    def valid(
        cls,
        original: PlacementOfferV1,
        replacement: PlacementOfferV1,
    ) -> OfferRevalidationResultV1:
        _validate_revalidation_pair(original, replacement)
        _require_evidence_row(
            replacement,
            availability=OfferAvailabilityV1.UNKNOWN,
            quota=OfferQuotaEvidenceV1.UNKNOWN,
            capacity=OfferCapacityEvidenceV1.SHAPE_FITS_EXISTING_NODE)
        return cls._create(OfferRevalidationStatusV1.VALID, replacement,
                           OfferReasonCodeV1.NONE)

    @classmethod
    def unavailable(
        cls,
        original: PlacementOfferV1,
        replacement: PlacementOfferV1,
        reason_code: OfferReasonCodeV1,
    ) -> OfferRevalidationResultV1:
        _validate_revalidation_pair(original, replacement)
        _require_enum(reason_code, OfferReasonCodeV1, 'reason_code')
        row = _UNAVAILABLE_REVALIDATION_ROWS.get(reason_code)
        if row is None:
            raise ValueError('Invalid unavailable revalidation reason code.')
        quota, capacity = row
        _require_evidence_row(replacement,
                              availability=OfferAvailabilityV1.UNAVAILABLE,
                              quota=quota,
                              capacity=capacity)
        return cls._create(OfferRevalidationStatusV1.UNAVAILABLE, replacement,
                           reason_code)

    @classmethod
    def not_representable(
        cls,
        reason_code: OfferReasonCodeV1,
    ) -> OfferRevalidationResultV1:
        _require_enum(reason_code, OfferReasonCodeV1, 'reason_code')
        if reason_code not in _NOT_REPRESENTABLE_REVALIDATION_REASONS:
            raise ValueError(
                'Invalid NOT_REPRESENTABLE revalidation reason code.')
        return cls._create(OfferRevalidationStatusV1.NOT_REPRESENTABLE, None,
                           reason_code)

    @classmethod
    def _create(
        cls,
        status: OfferRevalidationStatusV1,
        offer: PlacementOfferV1 | None,
        reason_code: OfferReasonCodeV1,
    ) -> OfferRevalidationResultV1:
        instance = object.__new__(cls)
        object.__setattr__(instance, 'status', status)
        object.__setattr__(instance, 'offer', offer)
        object.__setattr__(instance, 'reason_code', reason_code)
        return instance


@dataclasses.dataclass(frozen=True)
class TaskPlacementDecisionV1:
    """Process-local optimizer comparison evidence for one task."""

    task_index: int
    resources_fingerprint: str
    operation: OfferOperationV1
    offer: PlacementOfferV1 | None
    selection_capture_id: str | None

    def __post_init__(self) -> None:
        if type(self.task_index) is not int or self.task_index < 0:
            raise ValueError('task_index must be a nonnegative integer.')
        _require_digest(self.resources_fingerprint, 'resources_fingerprint')
        _require_enum(self.operation, OfferOperationV1, 'operation')
        if self.operation is not OfferOperationV1.PLAN_CREATE:
            raise ValueError('placement decisions require PLAN_CREATE.')
        if self.selection_capture_id is not None:
            _require_capture_id(self.selection_capture_id,
                                'selection_capture_id')
        if self.offer is None:
            return
        if type(self.offer) is not PlacementOfferV1:
            raise ValueError('offer must be an exact PlacementOfferV1.')
        if self.offer.operation is not OfferOperationV1.PLAN_CREATE:
            raise ValueError('decision offers require PLAN_CREATE.')
        if self.offer.operation is not self.operation:
            raise ValueError('decision and offer operations must match.')
        if self.selection_capture_id is None:
            raise ValueError(
                'A decision with an offer requires a selection capture ID.')


@dataclasses.dataclass(frozen=True)
class OptimizationOfferPlanV1:
    """Ordered process-local optimizer comparison decisions."""

    decisions: tuple[TaskPlacementDecisionV1, ...]

    def __post_init__(self) -> None:
        if type(self.decisions) is not tuple:
            raise ValueError('decisions must be an exact tuple.')
        if any(
                type(decision) is not TaskPlacementDecisionV1
                for decision in self.decisions):
            raise ValueError(
                'decisions must contain exact TaskPlacementDecisionV1 '
                'objects.')
        indices = tuple(decision.task_index for decision in self.decisions)
        if len(indices) != len(set(indices)):
            raise ValueError('decision task indices must be unique.')


@dataclasses.dataclass(frozen=True)
class PlacementOfferHandoffV1:
    """Trusted same-process placement handoff to provider actuation."""

    mode: PlacementOfferActuationModeV1
    offer: PlacementOfferV1 | None
    actuation_context: ProviderActuationContextV1 | None
    provider_attempt_count: int
    reason_code: OfferReasonCodeV1

    def __post_init__(self) -> None:
        _require_enum(self.mode, PlacementOfferActuationModeV1, 'mode')
        _require_positive_int(self.provider_attempt_count,
                              'provider_attempt_count')
        _require_enum(self.reason_code, OfferReasonCodeV1, 'reason_code')
        if self.offer is not None:
            if type(self.offer) is not PlacementOfferV1:
                raise ValueError('offer must be an exact PlacementOfferV1.')
            if self.offer.operation is not OfferOperationV1.FRESH_CREATE:
                raise ValueError('Handoff offers require FRESH_CREATE.')

        if self.mode is PlacementOfferActuationModeV1.SHADOW:
            if self.offer is None or self.actuation_context is not None:
                raise ValueError(
                    'SHADOW requires an offer and no actuation context.')
            if self.reason_code is not OfferReasonCodeV1.NONE:
                raise ValueError('SHADOW requires reason_code NONE.')
            return
        if self.mode is PlacementOfferActuationModeV1.SHADOW_LEGACY_FALLBACK:
            if self.offer is not None or self.actuation_context is not None:
                raise ValueError(
                    'SHADOW_LEGACY_FALLBACK requires null offer and context.')
            if self.reason_code in (OfferReasonCodeV1.NONE,
                                    OfferReasonCodeV1.PROVIDER_OBJECT_CONFLICT):
                raise ValueError('Invalid SHADOW_LEGACY_FALLBACK reason code.')
            return
        if self.mode is PlacementOfferActuationModeV1.AUTHORITATIVE:
            if self.offer is None or self.actuation_context is None:
                raise ValueError('AUTHORITATIVE requires an offer and context.')
            if self.provider_attempt_count != 1:
                raise ValueError('AUTHORITATIVE requires attempt count 1.')
            if self.reason_code is not OfferReasonCodeV1.NONE:
                raise ValueError('AUTHORITATIVE requires reason_code NONE.')
            _validate_actuation_context(self.actuation_context)
            if self.offer.provider != self.actuation_context.provider:
                raise ValueError(
                    'AUTHORITATIVE offer and context providers differ.')
            return
        if self.mode is PlacementOfferActuationModeV1.LEGACY_FIRST_ATTEMPT:
            if self.offer is not None or self.actuation_context is not None:
                raise ValueError(
                    'LEGACY_FIRST_ATTEMPT requires null offer and context.')
            if self.provider_attempt_count != 1:
                raise ValueError(
                    'LEGACY_FIRST_ATTEMPT requires attempt count 1.')
            if self.reason_code not in _NOT_REPRESENTABLE_OFFER_SET_REASONS:
                raise ValueError('Invalid LEGACY_FIRST_ATTEMPT reason code.')
            return
        if self.offer is not None or self.actuation_context is not None:
            raise ValueError(
                'LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT requires null offer '
                'and context.')
        if self.provider_attempt_count < 2:
            raise ValueError(
                'LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT requires attempt '
                'count at least 2.')
        if (self.reason_code
                is not OfferReasonCodeV1.RETRY_AFTER_PROVIDER_ATTEMPT):
            raise ValueError(
                'LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT requires its exact '
                'reason code.')


def validate_authoritative_capture_v1(
    offer: PlacementOfferV1,
    capture: ObservationCaptureV1,
    *,
    freshness: ObservationFreshnessV1,
    selection_capture_id: str,
) -> ProviderActuationContextV1:
    """Validate and return the exact fresh provider actuation context."""
    if type(offer) is not PlacementOfferV1:
        raise ValueError('offer must be an exact PlacementOfferV1.')
    if offer.operation is not OfferOperationV1.FRESH_CREATE:
        raise ValueError('Authoritative capture requires FRESH_CREATE.')
    if type(capture) is not ObservationCaptureV1:
        raise ValueError('capture must be an exact ObservationCaptureV1.')
    _require_enum(freshness, ObservationFreshnessV1, 'freshness')
    if freshness is not ObservationFreshnessV1.REQUIRE_FRESH:
        raise ValueError('Authoritative capture requires REQUIRE_FRESH.')
    _require_capture_id(selection_capture_id, 'selection_capture_id')
    _validate_observation(capture.observation)
    context = capture.actuation_context
    if context is None:
        raise ValueError('Authoritative capture requires a context.')
    _validate_actuation_context(context)
    if context.provider != capture.observation.provider:
        raise ValueError('Observation and context providers differ.')
    if context.capture_id != capture.observation.capture_id:
        raise ValueError('Observation and context captures differ.')
    if context.capture_id == selection_capture_id:
        raise ValueError(
            'Fresh capture ID must differ from selection capture ID.')
    if offer.provider != capture.observation.provider:
        raise ValueError('Offer and capture providers differ.')
    if offer.observed_at != capture.observation.observed_at:
        raise ValueError('Offer and capture observation times differ.')
    return context


_OFFER_ENVELOPE_KEYS = frozenset({
    'schema_version',
    'operation',
    'actuation_kind',
    'offer_id',
    'observation_id',
    'provider',
    'scope',
    'resources',
    'region',
    'candidate_zones',
    'batching_scope',
    'price',
    'purchase_mode',
    'availability',
    'observed_at',
    'ttl_seconds',
    'revalidation_policy',
    'evidence',
    'provider_payload',
})
_SCOPE_KEYS = frozenset({'kind', 'id'})
_RESOURCE_KEYS = frozenset({
    'instance_type',
    'cpus',
    'memory_gib',
    'accelerators',
    'disk_tier',
    'network_tier',
    'placement_constraints_digest',
})
_ACCELERATOR_KEYS = frozenset({'name', 'count'})
_PRICE_KEYS = frozenset({'amount', 'basis', 'currency'})
_EVIDENCE_KEYS = frozenset({
    'reservation',
    'quota',
    'capacity',
    'requested_nodes',
})
_PROVIDER_PAYLOAD_KEYS = frozenset({
    'version',
    'identity',
    'observation',
})
_UNAVAILABLE_REVALIDATION_ROWS = {
    OfferReasonCodeV1.CONTEXT_UNREACHABLE:
        (OfferQuotaEvidenceV1.UNKNOWN,
         OfferCapacityEvidenceV1.CONTEXT_UNREACHABLE),
    OfferReasonCodeV1.SHAPE_NO_LONGER_SUPPORTED:
        (OfferQuotaEvidenceV1.UNKNOWN,
         OfferCapacityEvidenceV1.SHAPE_NO_LONGER_SUPPORTED),
    OfferReasonCodeV1.CAPACITY_UNAVAILABLE:
        (OfferQuotaEvidenceV1.UNKNOWN,
         OfferCapacityEvidenceV1.CAPACITY_UNAVAILABLE),
    OfferReasonCodeV1.QUOTA_UNAVAILABLE:
        (OfferQuotaEvidenceV1.UNAVAILABLE,
         OfferCapacityEvidenceV1.SHAPE_FITS_EXISTING_NODE),
    OfferReasonCodeV1.PROVIDER_OBJECT_CONFLICT:
        (OfferQuotaEvidenceV1.UNKNOWN,
         OfferCapacityEvidenceV1.PROVIDER_OBJECT_CONFLICT),
}

_EnumT = typing.TypeVar('_EnumT', bound=enum.Enum)


@dataclasses.dataclass
class _PayloadCounters:
    keys: int = 0
    array_elements: int = 0


def normalize_nfc_v1(value: str) -> str:
    """Return the fixed Unicode 3.2 NFC normalization used by V1."""
    if type(value) is not str:
        raise ValueError('NFC_V1 accepts only exact strings.')
    return unicodedata.ucd_3_2_0.normalize('NFC', value)


NFC_V1 = normalize_nfc_v1


def canonical_json_bytes_v1(value: JSONValue) -> bytes:
    """Serialize a previously validated V1 JSON value canonically."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
        allow_nan=False,
    ).encode('utf-8')


def _validate_observation(value: object) -> None:
    if not isinstance(value, ProviderObservationSnapshotV1):
        raise ValueError(
            'observation does not implement ProviderObservationSnapshotV1.')
    observation = value
    _require_provider(observation.provider)
    _require_capture_id(observation.capture_id, 'observation.capture_id')
    _require_utc_datetime(observation.observed_at, 'observation.observed_at')


def _validate_actuation_context(value: object) -> None:
    if not isinstance(value, ProviderActuationContextV1):
        raise ValueError('actuation_context does not implement '
                         'ProviderActuationContextV1.')
    context = value
    _require_provider(context.provider)
    _require_capture_id(context.capture_id, 'actuation_context.capture_id')
    if not callable(context.close):
        raise ValueError('actuation_context.close must be callable.')


def _require_payload_schema(value: object) -> ProviderPayloadSchemaV1:
    if type(value) is not ProviderPayloadSchemaV1:
        raise ValueError(
            'payload_schema must be an exact ProviderPayloadSchemaV1.')
    value.__post_init__()
    return value


def _validate_provider_payload_against_schema(
    value: object,
    payload_schema: ProviderPayloadSchemaV1,
) -> None:
    if type(value) is not OfferProviderPayloadV1:
        raise ValueError(
            'provider_payload must be an exact OfferProviderPayloadV1.')
    payload = value
    if type(payload.version) is not int or payload.version != 1:
        raise ValueError('provider_payload.version must be integer 1.')
    if type(payload.identity) is not FrozenJSONDict:
        raise ValueError(
            'provider_payload.identity must be an exact FrozenJSONDict.')
    if type(payload.observation) is not FrozenJSONDict:
        raise ValueError(
            'provider_payload.observation must be an exact FrozenJSONDict.')
    identity = thaw_json(payload.identity)
    observation = thaw_json(payload.observation)
    if type(identity) is not dict or type(observation) is not dict:
        raise ValueError('provider payload roots must be objects.')
    counters = _PayloadCounters()
    _validate_payload_value(identity,
                            payload_schema.identity,
                            path='provider_payload.identity',
                            container_depth=0,
                            counters=counters)
    _validate_payload_value(observation,
                            payload_schema.observation,
                            path='provider_payload.observation',
                            container_depth=0,
                            counters=counters)
    _validate_payload_counts(counters)
    if len(canonical_json_bytes_v1(
            payload.to_json())) > _MAX_PROVIDER_PAYLOAD_BYTES:
        raise ValueError('provider_payload exceeds 4 KiB.')


def _validate_payload_value(
    value: object,
    schema: ProviderPayloadSchemaNodeV1,
    *,
    path: str,
    container_depth: int,
    counters: _PayloadCounters,
) -> JSONValue:
    kind = schema.kind
    if kind is ProviderPayloadNodeKindV1.OBJECT:
        if type(value) is not dict:
            raise ValueError(f'{path} must be an exact object.')
        if container_depth > _MAX_PAYLOAD_CONTAINER_DEPTH:
            raise ValueError(f'{path} exceeds container depth 4.')
        object_value = typing.cast(dict[object, object], value)
        if len(object_value) > 32:
            raise ValueError(f'{path} exceeds 32 object keys.')
        for key in object_value:
            _require_payload_key(key, path)
        expected_keys = tuple(key for key, _ in schema.fields)
        actual_keys = set(typing.cast(dict[str, object], object_value))
        if actual_keys != set(expected_keys):
            unknown = sorted(actual_keys - set(expected_keys))
            missing = sorted(set(expected_keys) - actual_keys)
            raise ValueError(
                f'{path} keys do not match its schema; unknown={unknown}, '
                f'missing={missing}.')
        counters.keys += len(object_value)
        result: dict[str, JSONValue] = {}
        string_object = typing.cast(dict[str, object], object_value)
        for key, child_schema in schema.fields:
            result[key] = _validate_payload_value(
                string_object[key],
                child_schema,
                path=f'{path}.{key}',
                container_depth=container_depth + 1,
                counters=counters)
        return result

    if kind is ProviderPayloadNodeKindV1.ARRAY:
        if type(value) is not list:
            raise ValueError(f'{path} must be an exact array.')
        if container_depth > _MAX_PAYLOAD_CONTAINER_DEPTH:
            raise ValueError(f'{path} exceeds container depth 4.')
        array_value = typing.cast(list[object], value)
        if len(array_value) > 32:
            raise ValueError(f'{path} exceeds 32 array elements.')
        counters.array_elements += len(array_value)
        if schema.item is None:
            raise ValueError(f'{path} has an invalid ARRAY schema.')
        return [
            _validate_payload_value(child,
                                    schema.item,
                                    path=f'{path}[{index}]',
                                    container_depth=container_depth + 1,
                                    counters=counters)
            for index, child in enumerate(array_value)
        ]

    if kind is ProviderPayloadNodeKindV1.STRING:
        string_value = _require_bounded_text(value,
                                             field=path,
                                             maximum_bytes=1_024,
                                             allow_empty=schema.allow_empty)
        if (schema.allowed_strings and
                string_value not in schema.allowed_strings):
            raise ValueError(f'{path} is not in its string allowlist.')
        return string_value
    if kind is ProviderPayloadNodeKindV1.DIGEST:
        return _require_digest(value, path)
    if kind is ProviderPayloadNodeKindV1.INTEGER:
        integer = _require_int(value, path)
        if not -(2**63) <= integer <= 2**63 - 1:
            raise ValueError(f'{path} exceeds the signed 64-bit range.')
        return integer
    if kind is ProviderPayloadNodeKindV1.BOOLEAN:
        _require_exact_bool(value, path)
        return typing.cast(bool, value)
    if kind is ProviderPayloadNodeKindV1.NULL:
        if value is not None:
            raise ValueError(f'{path} must be null.')
        return None
    raise ValueError(f'{path} has an unknown payload schema kind.')


def _validate_payload_counts(counters: _PayloadCounters) -> None:
    if counters.keys > _MAX_PAYLOAD_KEYS:
        raise ValueError('provider_payload exceeds 64 combined keys.')
    if counters.array_elements > _MAX_PAYLOAD_ARRAY_ELEMENTS:
        raise ValueError(
            'provider_payload exceeds 128 combined array elements.')


def _require_payload_key(value: object, path: str) -> str:
    if type(value) is not str:
        raise ValueError(f'{path} object keys must be exact strings.')
    if (not 1 <= len(value) <= 64 or not value.isascii() or
            any(not 0x20 <= ord(character) <= 0x7e for character in value)):
        raise ValueError(
            f'{path} keys must be 1 to 64 printable ASCII characters.')
    lowered = value.lower()
    segments = tuple(
        segment for segment in re.split(r'[_-]+', lowered) if segment)
    if any(segment in _PAYLOAD_SECRET_SEGMENTS for segment in segments):
        raise ValueError(f'{path} contains a suspicious secret-like key.')
    if any(pair in _PAYLOAD_SECRET_PAIRS
           for pair in zip(segments, segments[1:])):
        raise ValueError(f'{path} contains a suspicious secret-like key.')
    if lowered in _PAYLOAD_SECRET_UNSPLIT:
        raise ValueError(f'{path} contains a suspicious secret-like key.')
    return value


def _compute_offer_id(offer: PlacementOfferV1) -> str:
    preimage: dict[str, JSONValue] = {
        'schema_version': offer.schema_version,
        'provider': offer.provider,
        'operation': offer.operation.value,
        'actuation_kind': offer.actuation_kind.value,
        'scope': offer.scope.to_json(),
        'region': offer.region,
        'candidate_zones': list(offer.candidate_zones),
        'batching_scope': offer.batching_scope,
        'resources': offer.resources.to_json(),
        'purchase_mode': offer.purchase_mode.value,
        'provider_payload': {
            'version': offer.provider_payload.version,
            'identity': typing.cast(dict[str, JSONValue],
                                    thaw_json(offer.provider_payload.identity)),
        },
    }
    return (f'{offer.provider}:sha256:'
            f'{hashlib.sha256(canonical_json_bytes_v1(preimage)).hexdigest()}')


def _compute_observation_id(offer: PlacementOfferV1) -> str:
    preimage: dict[str, JSONValue] = {
        'offer_id': offer.offer_id,
        'price': offer.price.to_json(),
        'availability': offer.availability.value,
        'observed_at': _format_timestamp(offer.observed_at),
        'ttl_seconds': offer.ttl_seconds,
        'revalidation_policy': offer.revalidation_policy.value,
        'evidence': offer.evidence.to_json(),
        'provider_payload': {
            'observation': typing.cast(
                dict[str, JSONValue],
                thaw_json(offer.provider_payload.observation)),
        },
    }
    return 'sha256:' + hashlib.sha256(
        canonical_json_bytes_v1(preimage)).hexdigest()


def _offer_envelope(offer: PlacementOfferV1) -> dict[str, JSONValue]:
    return {
        'schema_version': offer.schema_version,
        'operation': offer.operation.value,
        'actuation_kind': offer.actuation_kind.value,
        'offer_id': offer.offer_id,
        'observation_id': offer.observation_id,
        'provider': offer.provider,
        'scope': offer.scope.to_json(),
        'resources': offer.resources.to_json(),
        'region': offer.region,
        'candidate_zones': list(offer.candidate_zones),
        'batching_scope': offer.batching_scope,
        'price': offer.price.to_json(),
        'purchase_mode': offer.purchase_mode.value,
        'availability': offer.availability.value,
        'observed_at': _format_timestamp(offer.observed_at),
        'ttl_seconds': offer.ttl_seconds,
        'revalidation_policy': offer.revalidation_policy.value,
        'evidence': offer.evidence.to_json(),
        'provider_payload': offer.provider_payload.to_json(),
    }


def _validate_offer_envelope_size(offer: PlacementOfferV1) -> None:
    _require_offer_id(offer.offer_id, offer.provider)
    _require_digest(offer.observation_id, 'observation_id')
    if len(canonical_json_bytes_v1(
            _offer_envelope(offer))) > _MAX_ENVELOPE_BYTES:
        raise ValueError('offer envelope exceeds 16 KiB.')


def _validate_revalidation_pair(
    original: PlacementOfferV1,
    replacement: PlacementOfferV1,
) -> None:
    if type(original) is not PlacementOfferV1:
        raise ValueError('original must be an exact PlacementOfferV1.')
    if type(replacement) is not PlacementOfferV1:
        raise ValueError('replacement must be an exact PlacementOfferV1.')
    if original.offer_id != replacement.offer_id:
        raise ValueError('Revalidation must retain the original offer_id.')
    if (original.evidence.requested_nodes
            != replacement.evidence.requested_nodes):
        raise ValueError('Revalidation must retain the requested-node count.')
    if replacement.observed_at < original.observed_at:
        raise ValueError('Revalidation observed_at must be nondecreasing.')


def _require_evidence_row(
    offer: PlacementOfferV1,
    *,
    availability: OfferAvailabilityV1,
    quota: OfferQuotaEvidenceV1,
    capacity: OfferCapacityEvidenceV1,
) -> None:
    if offer.availability is not availability:
        raise ValueError('Replacement availability violates its matrix row.')
    if (offer.evidence.reservation
            is not OfferReservationEvidenceV1.NOT_APPLICABLE):
        raise ValueError('Replacement reservation violates its matrix row.')
    if offer.evidence.quota is not quota:
        raise ValueError('Replacement quota violates its matrix row.')
    if offer.evidence.capacity is not capacity:
        raise ValueError('Replacement capacity violates its matrix row.')


def _require_json_builtins(value: object, path: str) -> None:
    if type(value) is dict:
        object_value = typing.cast(dict[object, object], value)
        for key, child in object_value.items():
            if type(key) is not str:
                raise ValueError(f'{path} object keys must be exact strings.')
            _require_json_builtins(child, f'{path}.{key}')
        return
    if type(value) is list:
        for index, child in enumerate(typing.cast(list[object], value)):
            _require_json_builtins(child, f'{path}[{index}]')
        return
    if value is None or type(value) in (str, int, bool):
        return
    raise ValueError(f'{path} contains a non-JSON or forbidden float value.')


def _require_object(
    value: object,
    expected_keys: frozenset[str],
    path: str,
) -> dict[str, JSONValue]:
    if type(value) is not dict:
        raise ValueError(f'{path} must be an exact object.')
    object_value = typing.cast(dict[str, JSONValue], value)
    actual_keys = set(object_value)
    if actual_keys != expected_keys:
        unknown = sorted(actual_keys - expected_keys)
        missing = sorted(expected_keys - actual_keys)
        raise ValueError(f'{path} keys do not match V1; unknown={unknown}, '
                         f'missing={missing}.')
    return object_value


def _require_object_value(
    value: object,
    path: str,
) -> dict[str, JSONValue]:
    if type(value) is not dict:
        raise ValueError(f'{path} must be an exact object.')
    return typing.cast(dict[str, JSONValue], value)


def _require_enum(
    value: object,
    enum_type: type[_EnumT],
    field: str,
) -> _EnumT:
    if type(value) is not enum_type:
        raise ValueError(f'{field} must be exact {enum_type.__name__}.')
    return value


def _parse_enum(
    value: object,
    enum_type: type[_EnumT],
    field: str,
) -> _EnumT:
    if type(value) is not str:
        raise ValueError(f'{field} must be a string enum value.')
    try:
        return enum_type(value)
    except ValueError as error:
        raise ValueError(f'{field} has an invalid enum value.') from error


def _require_string(value: object, field: str) -> str:
    if type(value) is not str:
        raise ValueError(f'{field} must be an exact string.')
    return value


def _require_optional_string(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field)


def _require_int(value: object, field: str) -> int:
    if type(value) is not int:
        raise ValueError(f'{field} must be an exact integer.')
    return value


def _require_provider(value: object) -> str:
    if type(value) is not str or _PROVIDER_PATTERN.fullmatch(value) is None:
        raise ValueError('provider has an invalid V1 name.')
    return value


def _require_offer_id(value: object, provider: str) -> str:
    if type(value) is not str or not value.isascii() or len(value) > 256:
        raise ValueError('offer_id must be no more than 256 ASCII characters.')
    prefix = f'{provider}:sha256:'
    digest = value[len(prefix):] if value.startswith(prefix) else ''
    if (len(digest) != 64 or
            any(character not in '0123456789abcdef' for character in digest)):
        raise ValueError('offer_id does not match the V1 hash grammar.')
    return value


def _require_digest(value: object, field: str) -> str:
    if type(value) is not str or _DIGEST_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f'{field} must be sha256: plus 64 lowercase hex characters.')
    return value


def _require_capture_id(value: object, field: str) -> str:
    if type(value) is not str or _CAPTURE_ID_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f'{field} must be a canonical lowercase RFC 4122 UUIDv4.')
    return value


def _require_lower_enum(value: object, field: str) -> str:
    if type(value) is not str or _LOWER_ENUM_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f'{field} must contain 1 to 128 lowercase letters, digits, or _.')
    return value


def _require_optional_tier(value: object, field: str) -> None:
    if value is not None and (type(value) is not str or
                              _TIER_PATTERN.fullmatch(value) is None):
        raise ValueError(f'{field} has an invalid tier value.')


def _require_decimal(value: object, field: str) -> str:
    if type(value) is not str or _DECIMAL_PATTERN.fullmatch(value) is None:
        raise ValueError(f'{field} must be a canonical nonnegative decimal.')
    return value


def _require_bounded_text(
    value: object,
    *,
    field: str,
    maximum_bytes: int,
    allow_empty: bool = False,
) -> str:
    if type(value) is not str:
        raise ValueError(f'{field} must be an exact string.')
    if normalize_nfc_v1(value) != value:
        raise ValueError(f'{field} must already use fixed NFC_V1.')
    for character in value:
        codepoint = ord(character)
        if (0xd800 <= codepoint <= 0xdfff or 0x00 <= codepoint <= 0x1f or
                0x7f <= codepoint <= 0x9f):
            raise ValueError(
                f'{field} must not contain surrogates or C0/C1 controls.')
    encoded_length = len(value.encode('utf-8'))
    minimum = 0 if allow_empty else 1
    if not minimum <= encoded_length <= maximum_bytes:
        if allow_empty:
            raise ValueError(
                f'{field} must be 0 to {maximum_bytes} UTF-8 bytes.')
        raise ValueError(f'{field} must be 1 to {maximum_bytes} UTF-8 bytes.')
    return value


def _require_exact_bool(value: object, field: str) -> None:
    if type(value) is not bool:
        raise ValueError(f'{field} must be an exact bool.')


def _require_int_range(
    value: object,
    minimum: int,
    maximum: int,
    field: str,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(
            f'{field} must be an integer from {minimum} through {maximum}.')
    return value


def _require_positive_int(value: object, field: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f'{field} must be a positive integer.')
    return value


def _require_utc_datetime(
    value: object,
    field: str,
    *,
    require_whole_seconds: bool = True,
) -> datetime.datetime:
    if type(value) is not datetime.datetime or value.tzinfo is None:
        raise ValueError(f'{field} must be a timezone-aware UTC datetime.')
    if value.utcoffset() != datetime.timedelta(0):
        raise ValueError(f'{field} must be in UTC.')
    normalized = value.astimezone(datetime.timezone.utc)
    if require_whole_seconds and normalized.microsecond != 0:
        raise ValueError(f'{field} must have whole-second precision.')
    return normalized


def _parse_timestamp(value: object) -> datetime.datetime:
    text = _require_string(value, 'observed_at')
    if (len(text) != 20 or not text.isascii() or
            _TIMESTAMP_PATTERN.fullmatch(text) is None):
        raise ValueError('observed_at must use YYYY-MM-DDTHH:MM:SSZ.')
    try:
        parsed = datetime.datetime.strptime(
            text, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc)
    except ValueError as error:
        raise ValueError('observed_at must be a valid UTC datetime.') from error
    if _format_timestamp(parsed) != text:
        raise ValueError('observed_at is not canonical.')
    return parsed


def _format_timestamp(value: datetime.datetime) -> str:
    normalized = _require_utc_datetime(value, 'observed_at')
    return normalized.strftime('%Y-%m-%dT%H:%M:%SZ')


def _reject_duplicate_keys(
    pairs: list[tuple[str, JSONValue]],) -> dict[str, JSONValue]:
    result: dict[str, JSONValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Duplicate JSON object key: {key}.')
        result[key] = value
    return result


def _reject_json_float(value: str) -> typing.NoReturn:
    raise ValueError(f'JSON floats are forbidden: {value}.')


def _reject_json_constant(value: str) -> typing.NoReturn:
    raise ValueError(f'Invalid JSON constant: {value}.')
