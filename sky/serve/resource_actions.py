"""Pure versioned contracts for durable SkyServe resource actions.

This module owns the bounded SkyServe/provider values persisted by the shadow
journal and, later, the authoritative action adapter.  It deliberately has no
database, SDK, provisioner, or provider imports.  Canonical bytes, hashes, and
logical action IDs reuse the generic resource-action kernel contract.
"""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import enum
import json
import re
from typing import Any, ClassVar, TypeVar
import uuid

from sky.server.requests import resource_actions as kernel_actions

_MAX_OBJECT_BYTES = 65_536
_MAX_TEXT_BYTES = 1_024
_MAX_SHORT_TEXT_BYTES = 253
_MAX_LIST_ITEMS = 256
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_UTC_TIMESTAMP_RE = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:'
                               r'[0-9]{2}\.[0-9]{6}Z$')
_DECIMAL_INTEGER_RE = re.compile(r'^(0|[1-9][0-9]*)$')

JsonObject = dict[str, Any]
_EnumT = TypeVar('_EnumT', bound=enum.Enum)


class ResourceActionMode(str, enum.Enum):
    """Per-service mutation authority mode."""

    LEGACY = 'legacy'
    SHADOW = 'shadow'
    AUTHORITATIVE = 'authoritative'


class ProviderProfile(str, enum.Enum):
    """Versioned provider lifecycle profiles."""

    POD_CLUSTER_V1 = 'pod_cluster_v1'


class ProfileEligibility(str, enum.Enum):
    """Whether a frozen profile is admitted to an authority cohort."""

    ELIGIBLE = 'ELIGIBLE'
    UNSUPPORTED = 'UNSUPPORTED'


class ShadowParentPhase(str, enum.Enum):
    """Closed logical shadow-sample phases."""

    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETE = 'COMPLETE'
    ABANDONED_PRE_SUBMIT = 'ABANDONED_PRE_SUBMIT'
    AMBIGUOUS = 'AMBIGUOUS'


class ShadowAttemptPhase(str, enum.Enum):
    """Closed per-mutation shadow-attempt phases."""

    PRE_SUBMIT = 'PRE_SUBMIT'
    REQUEST_BOUND = 'REQUEST_BOUND'
    COMPLETE = 'COMPLETE'
    ABANDONED_PRE_SUBMIT = 'ABANDONED_PRE_SUBMIT'
    REQUEST_ASSOCIATION_UNKNOWN = 'REQUEST_ASSOCIATION_UNKNOWN'


class ShadowRequestRole(str, enum.Enum):
    """Relationship of a legacy mutation to its logical action."""

    PRIMARY_LAUNCH = 'PRIMARY_LAUNCH'
    PRIMARY_DOWN = 'PRIMARY_DOWN'
    LAUNCH_CLEANUP_DOWN = 'LAUNCH_CLEANUP_DOWN'


class PlannedExecutionKind(str, enum.Enum):
    """How a shadow child reaches the existing mutation path."""

    API_REQUEST = 'api_request'
    LEGACY_DIRECT_DOWN = 'legacy_direct_down'


class ShadowParityClass(str, enum.Enum):
    """Final or pending parity classification for a logical sample."""

    PENDING = 'PENDING'
    MATCH = 'MATCH'
    IDENTITY_MISMATCH = 'IDENTITY_MISMATCH'
    PLACEMENT_MISMATCH = 'PLACEMENT_MISMATCH'
    SUBMISSION_CERTAINTY_MISMATCH = 'SUBMISSION_CERTAINTY_MISMATCH'
    OPERATION_ID_MISMATCH = 'OPERATION_ID_MISMATCH'
    RETRY_MISMATCH = 'RETRY_MISMATCH'
    OBSERVATION_MISMATCH = 'OBSERVATION_MISMATCH'
    TERMINAL_MISMATCH = 'TERMINAL_MISMATCH'
    UNSUPPORTED_PROVIDER_PROFILE = 'UNSUPPORTED_PROVIDER_PROFILE'
    ABANDONED = 'ABANDONED'
    AMBIGUOUS = 'AMBIGUOUS'


class ShadowDivergenceClass(str, enum.Enum):
    """Child-level divergences which block shadow promotion."""

    IDENTITY_MISMATCH = 'IDENTITY_MISMATCH'
    PLACEMENT_MISMATCH = 'PLACEMENT_MISMATCH'
    SUBMISSION_CERTAINTY_MISMATCH = 'SUBMISSION_CERTAINTY_MISMATCH'
    OPERATION_ID_MISMATCH = 'OPERATION_ID_MISMATCH'
    RETRY_MISMATCH = 'RETRY_MISMATCH'
    OBSERVATION_MISMATCH = 'OBSERVATION_MISMATCH'
    TERMINAL_MISMATCH = 'TERMINAL_MISMATCH'
    UNSUPPORTED_PROVIDER_PROFILE = 'UNSUPPORTED_PROVIDER_PROFILE'

    @property
    def parity_class(self) -> ShadowParityClass:
        return ShadowParityClass(self.value)


class ProviderErrorCategory(str, enum.Enum):
    TRANSIENT = 'transient'
    CAPACITY = 'capacity'
    QUOTA = 'quota'
    RATE_LIMITED = 'rate_limited'
    INVALID_REQUEST = 'invalid_request'
    PERMISSION = 'permission'
    CONFLICT = 'conflict'
    UNKNOWN = 'unknown'


class ProviderSubmissionDisposition(str, enum.Enum):
    NOT_SUBMITTED = 'not_submitted'
    ACCEPTED = 'accepted'
    AMBIGUOUS = 'ambiguous'


class ProviderObservationState(str, enum.Enum):
    PRESENT = 'present'
    ABSENT = 'absent'
    CONFLICT = 'conflict'
    UNCERTAIN = 'uncertain'


class ProviderObservationCertainty(str, enum.Enum):
    AUTHORITATIVE = 'authoritative'
    EVENTUALLY_CONSISTENT = 'eventually_consistent'
    UNKNOWN = 'unknown'


class ServeActionDisposition(str, enum.Enum):
    SUCCEEDED = 'succeeded'
    RETRYABLE = 'retryable'
    UNCERTAIN = 'uncertain'
    TERMINAL_ERROR = 'terminal_error'
    CANCELLED = 'cancelled'


class ServeActionCertainty(str, enum.Enum):
    OBSERVED = 'observed'
    PROVIDER_ACKNOWLEDGED = 'provider_acknowledged'
    UNKNOWN = 'unknown'


class ServeRetryClass(str, enum.Enum):
    TRANSIENT = 'transient'
    CAPACITY = 'capacity'
    QUOTA = 'quota'
    RATE_LIMITED = 'rate_limited'
    OBSERVATION_REQUIRED = 'observation_required'


class ShadowRowDisposition(str, enum.Enum):
    RETAINED = 'retained'
    REMOVED = 'removed'


class ShadowCapacityOutcome(str, enum.Enum):
    SUCCESS = 'success'
    CAPACITY_FAILURE = 'capacity_failure'
    QUOTA_FAILURE = 'quota_failure'
    GENERIC_FAILURE = 'generic_failure'


class ShadowRetryDecision(str, enum.Enum):
    RETRY_SAME_PLAN = 'retry_same_plan'
    REPLAN_NEW_GENERATION = 'replan_new_generation'
    OBSERVE = 'observe'
    BLOCK = 'block'
    TERMINAL = 'terminal'


class ReplicaStatusValue(str, enum.Enum):
    """Stable serialized replica statuses accepted by shadow projections."""

    PENDING = 'PENDING'
    PROVISIONING = 'PROVISIONING'
    STARTING = 'STARTING'
    READY = 'READY'
    NOT_READY = 'NOT_READY'
    SHUTTING_DOWN = 'SHUTTING_DOWN'
    FAILED = 'FAILED'
    FAILED_INITIAL_DELAY = 'FAILED_INITIAL_DELAY'
    FAILED_PROBING = 'FAILED_PROBING'
    FAILED_PROVISION = 'FAILED_PROVISION'
    FAILED_CLEANUP = 'FAILED_CLEANUP'
    PREEMPTED = 'PREEMPTED'
    UNKNOWN = 'UNKNOWN'


def canonical_json_bytes(value: Any) -> bytes:
    """Return bytes from the generic resource-action canonical serializer."""

    return kernel_actions.canonical_json_bytes(value)


def canonical_sha256(value: Any) -> str:
    """Return a hash from the generic resource-action canonical serializer."""

    return kernel_actions.canonical_sha256(value)


def _closed_object(value: Any, *, name: str,
                   keys: frozenset[str]) -> JsonObject:
    if not isinstance(value, Mapping):
        raise TypeError(f'{name} must be an object.')
    if any(not isinstance(key, str) for key in value):
        raise TypeError(f'{name} keys must be text.')
    if set(value) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    encoded = canonical_json_bytes(value)
    if len(encoded) > _MAX_OBJECT_BYTES:
        raise ValueError(f'{name} exceeds {_MAX_OBJECT_BYTES} bytes.')
    normalized = json.loads(encoded.decode('utf-8'))
    if value != normalized:
        raise ValueError(f'{name} is not canonical.')
    return normalized


def _enum_value(enum_type: type[_EnumT], value: Any, *, name: str) -> _EnumT:
    if not isinstance(value, str):
        raise TypeError(f'{name} must be text.')
    try:
        parsed = enum_type(value)
    except ValueError as e:
        raise ValueError(f'{name} is unsupported.') from e
    if parsed.value != value:
        raise ValueError(f'{name} is not canonical.')
    return parsed


def _text(value: Any,
          *,
          name: str,
          maximum_bytes: int = _MAX_TEXT_BYTES) -> str:
    if not isinstance(value, str):
        raise TypeError(f'{name} must be text.')
    size = len(value.encode('utf-8'))
    if size == 0 or size > maximum_bytes:
        raise ValueError(f'{name} must be 1..{maximum_bytes} UTF-8 bytes.')
    if canonical_json_bytes(value) != canonical_json_bytes(
            value.encode('utf-8').decode('utf-8')):
        raise ValueError(f'{name} is not canonical.')
    # The surrounding closed-object comparison catches NFC differences.  This
    # standalone check keeps direct dataclass construction fail closed too.
    if json.loads(canonical_json_bytes(value).decode('utf-8')) != value:
        raise ValueError(f'{name} is not NFC-normalized.')
    return value


def _optional_text(value: Any,
                   *,
                   name: str,
                   maximum_bytes: int = _MAX_TEXT_BYTES) -> str | None:
    if value is None:
        return None
    return _text(value, name=name, maximum_bytes=maximum_bytes)


def _sha256(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be lowercase SHA-256 hex.')
    return value


def _uuid(value: Any, *, name: str) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    if not isinstance(value, str):
        raise TypeError(f'{name} must be canonical UUID text.')
    try:
        parsed = uuid.UUID(value)
    except ValueError as e:
        raise ValueError(f'{name} must be a UUID.') from e
    if str(parsed) != value:
        raise ValueError(f'{name} must be lowercase hyphenated UUID text.')
    return parsed


def _nonnegative_integer(value: Any, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f'{name} must be a nonnegative integer.')
    return value


def _positive_integer(value: Any, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f'{name} must be a positive integer.')
    return value


def _version_one(value: Any, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value != 1:
        raise ValueError(f'{name} must be integer 1.')
    return value


def _optional_nonnegative_integer(value: Any, *, name: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_integer(value, name=name)


def _boolean(value: Any, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f'{name} must be a Boolean.')
    return value


def _timestamp(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or _UTC_TIMESTAMP_RE.fullmatch(value) is None:
        raise ValueError(
            f'{name} must be UTC RFC 3339 with six fractional digits.')
    try:
        parsed = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError as e:
        raise ValueError(f'{name} is not a valid UTC timestamp.') from e
    if parsed.strftime('%Y-%m-%dT%H:%M:%S.%fZ') != value:
        raise ValueError(f'{name} is not canonical.')
    return value


class _CanonicalContract:
    """Common encoding helpers for immutable closed contracts."""

    def canonical_value(self) -> JsonObject:
        raise NotImplementedError

    @property
    def canonical_bytes(self) -> bytes:
        value = self.canonical_value()
        encoded = canonical_json_bytes(value)
        if len(encoded) > _MAX_OBJECT_BYTES:
            raise ValueError(
                f'{type(self).__name__} exceeds {_MAX_OBJECT_BYTES} bytes.')
        return encoded

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.canonical_value())


@dataclasses.dataclass(frozen=True)
class ProviderResourceIdentityV1(_CanonicalContract):
    """Provider-facing subset of one Serve replica action identity."""

    service_hash: str
    service_incarnation: uuid.UUID
    replica_id: int
    replica_incarnation: uuid.UUID
    desired_generation: int

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'service_hash', 'service_incarnation', 'replica_id',
        'replica_incarnation', 'desired_generation'
    })

    def __post_init__(self) -> None:
        service_hash = _text(self.service_hash,
                             name='resource_identity.service_hash')
        service_incarnation = _uuid(
            self.service_incarnation,
            name='resource_identity.service_incarnation')
        replica_incarnation = _uuid(
            self.replica_incarnation,
            name='resource_identity.replica_incarnation')
        if service_hash != str(service_incarnation):
            raise ValueError('service_hash must equal service_incarnation.')
        object.__setattr__(self, 'service_hash', service_hash)
        object.__setattr__(self, 'service_incarnation', service_incarnation)
        object.__setattr__(
            self, 'replica_id',
            _nonnegative_integer(self.replica_id,
                                 name='resource_identity.replica_id'))
        object.__setattr__(self, 'replica_incarnation', replica_incarnation)
        object.__setattr__(
            self, 'desired_generation',
            _positive_integer(self.desired_generation,
                              name='resource_identity.desired_generation'))

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderResourceIdentityV1':
        raw = _closed_object(value, name='resource_identity', keys=cls._KEYS)
        return cls(service_hash=raw['service_hash'],
                   service_incarnation=_uuid(
                       raw['service_incarnation'],
                       name='resource_identity.service_incarnation'),
                   replica_id=raw['replica_id'],
                   replica_incarnation=_uuid(
                       raw['replica_incarnation'],
                       name='resource_identity.replica_incarnation'),
                   desired_generation=raw['desired_generation'])

    def canonical_value(self) -> JsonObject:
        return {
            'service_hash': self.service_hash,
            'service_incarnation': str(self.service_incarnation),
            'replica_id': self.replica_id,
            'replica_incarnation': str(self.replica_incarnation),
            'desired_generation': self.desired_generation,
        }

    def action_identity(
        self, action_kind: kernel_actions.ActionKind
    ) -> kernel_actions.ResourceActionIdentity:
        return kernel_actions.ResourceActionIdentity(
            service_hash=self.service_hash,
            service_incarnation=self.service_incarnation,
            replica_id=self.replica_id,
            replica_incarnation=self.replica_incarnation,
            desired_generation=self.desired_generation,
            action_kind=action_kind)


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesLocatorV1(_CanonicalContract):
    """Identity-qualified Kubernetes locator details."""

    cluster_fingerprint_sha256: str
    namespace: str
    workload_kind: str
    workload_name: str
    cluster_record_uuid_label: str
    replica_incarnation_label: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'cluster_fingerprint_sha256', 'namespace', 'workload_kind',
        'workload_name', 'cluster_record_uuid_label',
        'replica_incarnation_label'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'cluster_fingerprint_sha256',
            _sha256(self.cluster_fingerprint_sha256,
                    name='kubernetes.cluster_fingerprint_sha256'))
        for field in ('namespace', 'workload_kind', 'workload_name'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'kubernetes.{field}',
                      maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        # Labels are canonical UUID strings, not arbitrary provider text.
        object.__setattr__(
            self, 'cluster_record_uuid_label',
            str(
                _uuid(self.cluster_record_uuid_label,
                      name='kubernetes.cluster_record_uuid_label')))
        object.__setattr__(
            self, 'replica_incarnation_label',
            str(
                _uuid(self.replica_incarnation_label,
                      name='kubernetes.replica_incarnation_label')))

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesLocatorV1':
        raw = _closed_object(value, name='kubernetes', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderLocatorV1(_CanonicalContract):
    """Frozen provider locator for one logical replica action."""

    version: int
    profile: ProviderProfile
    cloud: str
    region: str | None
    zone: str | None
    sky_cluster_name: str
    sky_cluster_record_uuid: uuid.UUID
    kubernetes: ProviderKubernetesLocatorV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'profile', 'cloud', 'region', 'zone', 'sky_cluster_name',
        'sky_cluster_record_uuid', 'kubernetes'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='provider locator version')
        profile = (self.profile if isinstance(self.profile, ProviderProfile)
                   else _enum_value(
                       ProviderProfile, self.profile, name='locator.profile'))
        object.__setattr__(self, 'profile', profile)
        object.__setattr__(self, 'cloud', _text(self.cloud,
                                                name='locator.cloud'))
        object.__setattr__(self, 'region',
                           _optional_text(self.region, name='locator.region'))
        object.__setattr__(self, 'zone',
                           _optional_text(self.zone, name='locator.zone'))
        object.__setattr__(
            self, 'sky_cluster_name',
            _text(self.sky_cluster_name,
                  name='locator.sky_cluster_name',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        cluster_uuid = _uuid(self.sky_cluster_record_uuid,
                             name='locator.sky_cluster_record_uuid')
        object.__setattr__(self, 'sky_cluster_record_uuid', cluster_uuid)
        if (self.kubernetes is not None and
                not isinstance(self.kubernetes, ProviderKubernetesLocatorV1)):
            raise TypeError('locator.kubernetes has an invalid type.')
        if (self.kubernetes is not None and
                self.kubernetes.cluster_record_uuid_label != str(cluster_uuid)):
            raise ValueError('Kubernetes cluster label does not match the '
                             'cluster-record UUID.')

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderLocatorV1':
        raw = _closed_object(value, name='provider locator', keys=cls._KEYS)
        kubernetes = (None if raw['kubernetes'] is None else
                      ProviderKubernetesLocatorV1.from_value(raw['kubernetes']))
        return cls(version=raw['version'],
                   profile=_enum_value(ProviderProfile,
                                       raw['profile'],
                                       name='locator.profile'),
                   cloud=raw['cloud'],
                   region=raw['region'],
                   zone=raw['zone'],
                   sky_cluster_name=raw['sky_cluster_name'],
                   sky_cluster_record_uuid=_uuid(
                       raw['sky_cluster_record_uuid'],
                       name='locator.sky_cluster_record_uuid'),
                   kubernetes=kubernetes)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'profile': self.profile.value,
            'cloud': self.cloud,
            'region': self.region,
            'zone': self.zone,
            'sky_cluster_name': self.sky_cluster_name,
            'sky_cluster_record_uuid': str(self.sky_cluster_record_uuid),
            'kubernetes': (None if self.kubernetes is None else
                           self.kubernetes.canonical_value()),
        }

    @property
    def is_authoritative_pod_locator(self) -> bool:
        return (self.profile is ProviderProfile.POD_CLUSTER_V1 and
                self.cloud == 'kubernetes' and self.kubernetes is not None and
                self.kubernetes.workload_kind == 'Pod')


@dataclasses.dataclass(frozen=True)
class ResolvedProviderTargetV1(_CanonicalContract):
    """Write-once provider-native target evidence."""

    version: int
    requested_target_sha256: str
    provider_resource_id: str | None
    workload_uid: str | None
    provider_operation_id: str | None
    resolved_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'requested_target_sha256', 'provider_resource_id',
        'workload_uid', 'provider_operation_id', 'resolved_at'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='resolved target version')
        object.__setattr__(
            self, 'requested_target_sha256',
            _sha256(self.requested_target_sha256,
                    name='resolved_target.requested_target_sha256'))
        for field in ('provider_resource_id', 'workload_uid',
                      'provider_operation_id'):
            object.__setattr__(
                self, field,
                _optional_text(getattr(self, field),
                               name=f'resolved_target.{field}'))
        object.__setattr__(
            self, 'resolved_at',
            _timestamp(self.resolved_at, name='resolved_target.resolved_at'))

    @classmethod
    def from_value(cls, value: Any) -> 'ResolvedProviderTargetV1':
        raw = _closed_object(value, name='resolved target', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)

    def validate_requested_target(self, target: ProviderLocatorV1) -> None:
        if self.requested_target_sha256 != target.sha256:
            raise ValueError('Resolved target does not match requested target.')


@dataclasses.dataclass(frozen=True)
class ProviderAcceleratorV1(_CanonicalContract):
    """Bounded accelerator selection."""

    name: str
    count: int

    _KEYS: ClassVar[frozenset[str]] = frozenset({'name', 'count'})

    def __post_init__(self) -> None:
        object.__setattr__(self, 'name',
                           _text(self.name, name='accelerator.name'))
        object.__setattr__(
            self, 'count',
            _positive_integer(self.count, name='accelerator.count'))

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderAcceleratorV1':
        raw = _closed_object(value, name='accelerator', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderLabelV1(_CanonicalContract):
    """One sorted, nonsecret provider label."""

    key: str
    value: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'key', 'value'})

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'key',
            _text(self.key,
                  name='label.key',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(
            self, 'value',
            _text(self.value,
                  name='label.value',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderLabelV1':
        raw = _closed_object(value, name='label', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderPodResourceSnapshotV1(_CanonicalContract):
    """Closed provider-effective resource snapshot for a direct Pod."""

    version: int
    cloud: str
    cluster_fingerprint_sha256: str
    namespace: str
    instance_type: str | None
    accelerator: ProviderAcceleratorV1 | None
    cpus: str | None
    memory: str | None
    image_id: str | None
    disk_size_gb: int
    disk_tier: str | None
    ports: tuple[str, ...]
    labels: tuple[ProviderLabelV1, ...]
    use_spot: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'cloud', 'cluster_fingerprint_sha256', 'namespace',
        'instance_type', 'accelerator', 'cpus', 'memory', 'image_id',
        'disk_size_gb', 'disk_tier', 'ports', 'labels', 'use_spot'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='resource snapshot version')
        if self.cloud != 'kubernetes':
            raise ValueError('resource snapshot cloud must be kubernetes.')
        object.__setattr__(
            self, 'cluster_fingerprint_sha256',
            _sha256(self.cluster_fingerprint_sha256,
                    name='resources.cluster_fingerprint_sha256'))
        object.__setattr__(
            self, 'namespace',
            _text(self.namespace,
                  name='resources.namespace',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        for field in ('instance_type', 'cpus', 'memory', 'image_id',
                      'disk_tier'):
            object.__setattr__(
                self, field,
                _optional_text(getattr(self, field), name=f'resources.{field}'))
        if self.accelerator is not None and not isinstance(
                self.accelerator, ProviderAcceleratorV1):
            raise TypeError('resources.accelerator has an invalid type.')
        object.__setattr__(
            self, 'disk_size_gb',
            _positive_integer(self.disk_size_gb, name='resources.disk_size_gb'))
        if (not isinstance(self.ports, tuple) or
                len(self.ports) > _MAX_LIST_ITEMS):
            raise ValueError('resources.ports must be a tuple of at most 256.')
        ports = tuple(
            _text(port,
                  name='resources.port',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES) for port in self.ports)
        if ports != tuple(sorted(set(ports))):
            raise ValueError('resources.ports must be sorted and unique.')
        object.__setattr__(self, 'ports', ports)
        if (not isinstance(self.labels, tuple) or
                len(self.labels) > _MAX_LIST_ITEMS or
                any(not isinstance(label, ProviderLabelV1)
                    for label in self.labels)):
            raise ValueError('resources.labels must be a tuple of at most 256 '
                             'typed labels.')
        label_keys = tuple(label.key for label in self.labels)
        if label_keys != tuple(sorted(set(label_keys))):
            raise ValueError('resources.labels must be sorted by unique key.')
        _boolean(self.use_spot, name='resources.use_spot')
        if self.use_spot:
            raise ValueError('pod_cluster_v1 requires use_spot=false.')
        # Force the complete object bound during direct construction too.
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderPodResourceSnapshotV1':
        raw = _closed_object(value, name='resource snapshot', keys=cls._KEYS)
        ports = raw['ports']
        labels = raw['labels']
        if not isinstance(ports, list):
            raise TypeError('resources.ports must be a list.')
        if not isinstance(labels, list):
            raise TypeError('resources.labels must be a list.')
        accelerator = (None if raw['accelerator'] is None else
                       ProviderAcceleratorV1.from_value(raw['accelerator']))
        return cls(version=raw['version'],
                   cloud=raw['cloud'],
                   cluster_fingerprint_sha256=raw['cluster_fingerprint_sha256'],
                   namespace=raw['namespace'],
                   instance_type=raw['instance_type'],
                   accelerator=accelerator,
                   cpus=raw['cpus'],
                   memory=raw['memory'],
                   image_id=raw['image_id'],
                   disk_size_gb=raw['disk_size_gb'],
                   disk_tier=raw['disk_tier'],
                   ports=tuple(ports),
                   labels=tuple(
                       ProviderLabelV1.from_value(label) for label in labels),
                   use_spot=raw['use_spot'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'cloud': 'kubernetes',
            'cluster_fingerprint_sha256': self.cluster_fingerprint_sha256,
            'namespace': self.namespace,
            'instance_type': self.instance_type,
            'accelerator': (None if self.accelerator is None else
                            self.accelerator.canonical_value()),
            'cpus': self.cpus,
            'memory': self.memory,
            'image_id': self.image_id,
            'disk_size_gb': self.disk_size_gb,
            'disk_tier': self.disk_tier,
            'ports': list(self.ports),
            'labels': [label.canonical_value() for label in self.labels],
            'use_spot': False,
        }


@dataclasses.dataclass(frozen=True)
class ProviderLaunchSourceV1(_CanonicalContract):
    """Content-addressed source of a prepared Serve launch."""

    store: str
    service_name: str
    service_incarnation: uuid.UUID
    service_version: int
    yaml_content_sha256: str
    workspace: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'store', 'service_name', 'service_incarnation', 'service_version',
        'yaml_content_sha256', 'workspace'
    })

    def __post_init__(self) -> None:
        if self.store != 'serve_version_specs':
            raise ValueError('launch source store must be serve_version_specs.')
        object.__setattr__(
            self, 'service_name',
            _text(self.service_name, name='launch.source.service_name'))
        object.__setattr__(
            self, 'service_incarnation',
            _uuid(self.service_incarnation,
                  name='launch.source.service_incarnation'))
        object.__setattr__(
            self, 'service_version',
            _positive_integer(self.service_version,
                              name='launch.source.service_version'))
        object.__setattr__(
            self, 'yaml_content_sha256',
            _sha256(self.yaml_content_sha256,
                    name='launch.source.yaml_content_sha256'))
        object.__setattr__(
            self, 'workspace',
            _text(self.workspace, name='launch.source.workspace'))

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderLaunchSourceV1':
        raw = _closed_object(value, name='launch source', keys=cls._KEYS)
        return cls(store=raw['store'],
                   service_name=raw['service_name'],
                   service_incarnation=_uuid(
                       raw['service_incarnation'],
                       name='launch.source.service_incarnation'),
                   service_version=raw['service_version'],
                   yaml_content_sha256=raw['yaml_content_sha256'],
                   workspace=raw['workspace'])

    def canonical_value(self) -> JsonObject:
        return {
            'store': self.store,
            'service_name': self.service_name,
            'service_incarnation': str(self.service_incarnation),
            'service_version': self.service_version,
            'yaml_content_sha256': self.yaml_content_sha256,
            'workspace': self.workspace,
        }


@dataclasses.dataclass(frozen=True)
class ProviderLaunchInvocationV1(_CanonicalContract):
    """Redacted provider-effective launch invocation."""

    source: ProviderLaunchSourceV1
    resources: ProviderPodResourceSnapshotV1
    replica_id_text: str
    security_group_scope: str
    admin_policy_input_sha256: str
    admin_policy_output_sha256: str
    retry_until_up: bool
    exact_resources_override: bool
    backend: str
    optimize_target: str
    dryrun: bool
    no_setup: bool
    clone_disk_from: None
    fast: bool
    file_mounts_blob_id: str | None
    tls_material_ref: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'source', 'resources', 'replica_env', 'security_group_scope',
        'admin_policy_input_sha256', 'admin_policy_output_sha256',
        'retry_until_up', 'exact_resources_override', 'backend',
        'optimize_target', 'dryrun', 'no_setup', 'clone_disk_from', 'fast',
        'file_mounts_blob_id', 'tls_material_ref'
    })
    _REPLICA_ENV_KEYS: ClassVar[frozenset[str]] = frozenset(
        {'SKYPILOT_SERVE_REPLICA_ID'})

    def __post_init__(self) -> None:
        if not isinstance(self.source, ProviderLaunchSourceV1):
            raise TypeError('launch.source has an invalid type.')
        if not isinstance(self.resources, ProviderPodResourceSnapshotV1):
            raise TypeError('launch.resources has an invalid type.')
        replica_id_text = _text(
            self.replica_id_text,
            name='launch.replica_env.SKYPILOT_SERVE_REPLICA_ID')
        if _DECIMAL_INTEGER_RE.fullmatch(replica_id_text) is None:
            raise ValueError('launch replica ID must be canonical decimal '
                             'integer text.')
        object.__setattr__(self, 'replica_id_text', replica_id_text)
        object.__setattr__(
            self, 'security_group_scope',
            _text(self.security_group_scope,
                  name='launch.security_group_scope'))
        for field in ('admin_policy_input_sha256',
                      'admin_policy_output_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field), name=f'launch.{field}'))
        _boolean(self.retry_until_up, name='launch.retry_until_up')
        _boolean(self.exact_resources_override,
                 name='launch.exact_resources_override')
        if self.backend != 'cloud_vm_ray':
            raise ValueError('launch backend must be cloud_vm_ray.')
        if self.optimize_target != 'cost':
            raise ValueError('launch optimize_target must be cost.')
        fixed = {
            'dryrun': self.dryrun,
            'no_setup': self.no_setup,
            'fast': self.fast,
        }
        for name, value in fixed.items():
            _boolean(value, name=f'launch.{name}')
            if value:
                raise ValueError(f'launch {name} must be false.')
        if self.clone_disk_from is not None:
            raise ValueError('launch clone_disk_from must be null.')
        object.__setattr__(
            self, 'file_mounts_blob_id',
            _optional_text(self.file_mounts_blob_id,
                           name='launch.file_mounts_blob_id'))
        object.__setattr__(
            self, 'tls_material_ref',
            _optional_text(self.tls_material_ref,
                           name='launch.tls_material_ref'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderLaunchInvocationV1':
        raw = _closed_object(value, name='launch invocation', keys=cls._KEYS)
        replica_env = _closed_object(raw['replica_env'],
                                     name='launch.replica_env',
                                     keys=cls._REPLICA_ENV_KEYS)
        return cls(source=ProviderLaunchSourceV1.from_value(raw['source']),
                   resources=ProviderPodResourceSnapshotV1.from_value(
                       raw['resources']),
                   replica_id_text=replica_env['SKYPILOT_SERVE_REPLICA_ID'],
                   security_group_scope=raw['security_group_scope'],
                   admin_policy_input_sha256=raw['admin_policy_input_sha256'],
                   admin_policy_output_sha256=raw['admin_policy_output_sha256'],
                   retry_until_up=raw['retry_until_up'],
                   exact_resources_override=raw['exact_resources_override'],
                   backend=raw['backend'],
                   optimize_target=raw['optimize_target'],
                   dryrun=raw['dryrun'],
                   no_setup=raw['no_setup'],
                   clone_disk_from=raw['clone_disk_from'],
                   fast=raw['fast'],
                   file_mounts_blob_id=raw['file_mounts_blob_id'],
                   tls_material_ref=raw['tls_material_ref'])

    def canonical_value(self) -> JsonObject:
        return {
            'source': self.source.canonical_value(),
            'resources': self.resources.canonical_value(),
            'replica_env': {
                'SKYPILOT_SERVE_REPLICA_ID': self.replica_id_text
            },
            'security_group_scope': self.security_group_scope,
            'admin_policy_input_sha256': self.admin_policy_input_sha256,
            'admin_policy_output_sha256': self.admin_policy_output_sha256,
            'retry_until_up': self.retry_until_up,
            'exact_resources_override': self.exact_resources_override,
            'backend': 'cloud_vm_ray',
            'optimize_target': 'cost',
            'dryrun': False,
            'no_setup': False,
            'clone_disk_from': None,
            'fast': False,
            'file_mounts_blob_id': self.file_mounts_blob_id,
            'tls_material_ref': self.tls_material_ref,
        }

    @property
    def first_authority_cohort_redacted(self) -> bool:
        return (self.file_mounts_blob_id is None and
                self.tls_material_ref is None)


@dataclasses.dataclass(frozen=True)
class ProviderDownInvocationV1(_CanonicalContract):
    """Identity-fenced provider-effective down invocation."""

    cluster_name: str
    expected_cluster_record_uuid: uuid.UUID
    workspace: str
    purge: bool
    graceful: bool
    graceful_timeout: None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'cluster_name', 'expected_cluster_record_uuid', 'workspace', 'purge',
        'graceful', 'graceful_timeout'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'cluster_name',
            _text(self.cluster_name,
                  name='down.cluster_name',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(
            self, 'expected_cluster_record_uuid',
            _uuid(self.expected_cluster_record_uuid,
                  name='down.expected_cluster_record_uuid'))
        object.__setattr__(self, 'workspace',
                           _text(self.workspace, name='down.workspace'))
        for name, value in (('purge', self.purge), ('graceful', self.graceful)):
            _boolean(value, name=f'down.{name}')
            if value:
                raise ValueError(f'down {name} must be false.')
        if self.graceful_timeout is not None:
            raise ValueError('down graceful_timeout must be null.')

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderDownInvocationV1':
        raw = _closed_object(value, name='down invocation', keys=cls._KEYS)
        return cls(cluster_name=raw['cluster_name'],
                   expected_cluster_record_uuid=_uuid(
                       raw['expected_cluster_record_uuid'],
                       name='down.expected_cluster_record_uuid'),
                   workspace=raw['workspace'],
                   purge=raw['purge'],
                   graceful=raw['graceful'],
                   graceful_timeout=raw['graceful_timeout'])

    def canonical_value(self) -> JsonObject:
        return {
            'cluster_name': self.cluster_name,
            'expected_cluster_record_uuid': str(
                self.expected_cluster_record_uuid),
            'workspace': self.workspace,
            'purge': False,
            'graceful': False,
            'graceful_timeout': None,
        }


@dataclasses.dataclass(frozen=True)
class ProviderLifecycleInvocationV1(_CanonicalContract):
    """Exact launch/down invocation union committed by an action."""

    version: int
    profile: ProviderProfile
    redaction_profile: str
    action_kind: kernel_actions.ActionKind
    resource_identity: ProviderResourceIdentityV1
    requested_target: ProviderLocatorV1
    launch: ProviderLaunchInvocationV1 | None
    down: ProviderDownInvocationV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'profile', 'redaction_profile', 'action_kind',
        'resource_identity', 'requested_target', 'launch', 'down'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='invocation version')
        profile = (self.profile if isinstance(
            self.profile, ProviderProfile) else _enum_value(
                ProviderProfile, self.profile, name='invocation.profile'))
        object.__setattr__(self, 'profile', profile)
        if self.redaction_profile != 'provider_lifecycle_redaction_v1':
            raise ValueError('invocation redaction profile is unsupported.')
        try:
            action_kind = (self.action_kind if isinstance(
                self.action_kind, kernel_actions.ActionKind) else
                           kernel_actions.ActionKind(self.action_kind))
        except (TypeError, ValueError) as e:
            raise ValueError('invocation action kind is unsupported.') from e
        object.__setattr__(self, 'action_kind', action_kind)
        if not isinstance(self.resource_identity, ProviderResourceIdentityV1):
            raise TypeError('invocation resource identity has an invalid type.')
        if not isinstance(self.requested_target, ProviderLocatorV1):
            raise TypeError('invocation requested target has an invalid type.')
        if self.requested_target.profile is not profile:
            raise ValueError('invocation and locator profiles do not match.')
        if (self.requested_target.kubernetes is not None and
                self.requested_target.kubernetes.replica_incarnation_label
                != str(self.resource_identity.replica_incarnation)):
            raise ValueError('Kubernetes replica label does not match the '
                             'resource identity.')
        if action_kind is kernel_actions.ActionKind.LAUNCH:
            if not isinstance(self.launch, ProviderLaunchInvocationV1) or (
                    self.down is not None):
                raise ValueError('launch invocation requires only launch.')
            launch = self.launch
            if (launch.source.service_incarnation
                    != self.resource_identity.service_incarnation):
                raise ValueError('launch source service incarnation does not '
                                 'match resource identity.')
            if launch.replica_id_text != str(self.resource_identity.replica_id):
                raise ValueError('launch replica environment does not match '
                                 'resource identity.')
            kube = self.requested_target.kubernetes
            if (kube is not None and
                (launch.resources.cluster_fingerprint_sha256
                 != kube.cluster_fingerprint_sha256 or
                 launch.resources.namespace != kube.namespace)):
                raise ValueError('launch resource snapshot does not match the '
                                 'requested Kubernetes target.')
        else:
            if not isinstance(self.down, ProviderDownInvocationV1) or (
                    self.launch is not None):
                raise ValueError('down invocation requires only down.')
            if (self.down.cluster_name != self.requested_target.sky_cluster_name
                    or self.down.expected_cluster_record_uuid
                    != self.requested_target.sky_cluster_record_uuid):
                raise ValueError('down invocation does not match the requested '
                                 'target.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderLifecycleInvocationV1':
        raw = _closed_object(value,
                             name='provider lifecycle invocation',
                             keys=cls._KEYS)
        action_kind = _enum_value(kernel_actions.ActionKind,
                                  raw['action_kind'],
                                  name='invocation.action_kind')
        launch = (None if raw['launch'] is None else
                  ProviderLaunchInvocationV1.from_value(raw['launch']))
        down = (None if raw['down'] is None else
                ProviderDownInvocationV1.from_value(raw['down']))
        return cls(version=raw['version'],
                   profile=_enum_value(ProviderProfile,
                                       raw['profile'],
                                       name='invocation.profile'),
                   redaction_profile=raw['redaction_profile'],
                   action_kind=action_kind,
                   resource_identity=ProviderResourceIdentityV1.from_value(
                       raw['resource_identity']),
                   requested_target=ProviderLocatorV1.from_value(
                       raw['requested_target']),
                   launch=launch,
                   down=down)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'profile': self.profile.value,
            'redaction_profile': self.redaction_profile,
            'action_kind': self.action_kind.value,
            'resource_identity': self.resource_identity.canonical_value(),
            'requested_target': self.requested_target.canonical_value(),
            'launch':
                (None if self.launch is None else self.launch.canonical_value()
                ),
            'down':
                (None if self.down is None else self.down.canonical_value()),
        }

    @property
    def action_id(self) -> uuid.UUID:
        return self.resource_identity.action_identity(
            self.action_kind).action_id


@dataclasses.dataclass(frozen=True)
class ProviderLifecyclePlanV1(_CanonicalContract):
    """Frozen provider plan and content commitments for one action."""

    version: int
    profile: ProviderProfile
    action_kind: kernel_actions.ActionKind
    resource_identity: ProviderResourceIdentityV1
    placement_decision_sha256: str
    resources_snapshot_sha256: str
    workspace_identity_sha256: str
    requested_target: ProviderLocatorV1
    prior_resolved_target: ResolvedProviderTargetV1 | None
    request_payload_sha256: str
    redaction_profile: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'profile', 'action_kind', 'resource_identity',
        'placement_decision_sha256', 'resources_snapshot_sha256',
        'workspace_identity_sha256', 'requested_target',
        'prior_resolved_target', 'request_payload_sha256', 'redaction_profile'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='provider plan version')
        profile = (self.profile if isinstance(self.profile, ProviderProfile)
                   else _enum_value(
                       ProviderProfile, self.profile, name='plan.profile'))
        object.__setattr__(self, 'profile', profile)
        try:
            action_kind = (self.action_kind if isinstance(
                self.action_kind, kernel_actions.ActionKind) else
                           kernel_actions.ActionKind(self.action_kind))
        except (TypeError, ValueError) as e:
            raise ValueError('provider plan action kind is unsupported.') from e
        object.__setattr__(self, 'action_kind', action_kind)
        if not isinstance(self.resource_identity, ProviderResourceIdentityV1):
            raise TypeError('plan resource identity has an invalid type.')
        for field in ('placement_decision_sha256', 'resources_snapshot_sha256',
                      'workspace_identity_sha256', 'request_payload_sha256'):
            object.__setattr__(
                self, field, _sha256(getattr(self, field),
                                     name=f'plan.{field}'))
        if not isinstance(self.requested_target, ProviderLocatorV1):
            raise TypeError('plan requested target has an invalid type.')
        if self.requested_target.profile is not profile:
            raise ValueError('plan and locator profiles do not match.')
        if (self.requested_target.kubernetes is not None and
                self.requested_target.kubernetes.replica_incarnation_label
                != str(self.resource_identity.replica_incarnation)):
            raise ValueError('Kubernetes replica label does not match plan '
                             'resource identity.')
        if self.prior_resolved_target is not None:
            if not isinstance(self.prior_resolved_target,
                              ResolvedProviderTargetV1):
                raise TypeError('plan prior resolved target has invalid type.')
            self.prior_resolved_target.validate_requested_target(
                self.requested_target)
        if (action_kind is kernel_actions.ActionKind.LAUNCH and
                self.prior_resolved_target is not None):
            raise ValueError('launch plan prior_resolved_target must be null.')
        if self.redaction_profile != 'provider_lifecycle_redaction_v1':
            raise ValueError('provider plan redaction profile is unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderLifecyclePlanV1':
        raw = _closed_object(value,
                             name='provider lifecycle plan',
                             keys=cls._KEYS)
        prior = (None if raw['prior_resolved_target'] is None else
                 ResolvedProviderTargetV1.from_value(
                     raw['prior_resolved_target']))
        return cls(version=raw['version'],
                   profile=_enum_value(ProviderProfile,
                                       raw['profile'],
                                       name='plan.profile'),
                   action_kind=_enum_value(kernel_actions.ActionKind,
                                           raw['action_kind'],
                                           name='plan.action_kind'),
                   resource_identity=ProviderResourceIdentityV1.from_value(
                       raw['resource_identity']),
                   placement_decision_sha256=raw['placement_decision_sha256'],
                   resources_snapshot_sha256=raw['resources_snapshot_sha256'],
                   workspace_identity_sha256=raw['workspace_identity_sha256'],
                   requested_target=ProviderLocatorV1.from_value(
                       raw['requested_target']),
                   prior_resolved_target=prior,
                   request_payload_sha256=raw['request_payload_sha256'],
                   redaction_profile=raw['redaction_profile'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'profile': self.profile.value,
            'action_kind': self.action_kind.value,
            'resource_identity': self.resource_identity.canonical_value(),
            'placement_decision_sha256': self.placement_decision_sha256,
            'resources_snapshot_sha256': self.resources_snapshot_sha256,
            'workspace_identity_sha256': self.workspace_identity_sha256,
            'requested_target': self.requested_target.canonical_value(),
            'prior_resolved_target':
                (None if self.prior_resolved_target is None else
                 self.prior_resolved_target.canonical_value()),
            'request_payload_sha256': self.request_payload_sha256,
            'redaction_profile': self.redaction_profile,
        }

    @property
    def action_id(self) -> uuid.UUID:
        return self.resource_identity.action_identity(
            self.action_kind).action_id

    def validate_invocation(self,
                            invocation: ProviderLifecycleInvocationV1) -> None:
        if not isinstance(invocation, ProviderLifecycleInvocationV1):
            raise TypeError('invocation has an invalid type.')
        if (self.profile is not invocation.profile or
                self.action_kind is not invocation.action_kind or
                self.resource_identity != invocation.resource_identity or
                self.requested_target != invocation.requested_target):
            raise ValueError('provider plan and invocation identities differ.')
        if self.request_payload_sha256 != invocation.sha256:
            raise ValueError('request payload hash does not match invocation.')
        if (invocation.launch is not None and self.resources_snapshot_sha256
                != invocation.launch.resources.sha256):
            raise ValueError('resource snapshot hash does not match launch.')


@dataclasses.dataclass(frozen=True)
class ProviderErrorV1(_CanonicalContract):
    """Bounded provider error classification without raw exception data."""

    category: ProviderErrorCategory
    provider_code: str | None
    retry_after_seconds: int | None
    normalized_message: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'category', 'provider_code', 'retry_after_seconds', 'normalized_message'
    })

    def __post_init__(self) -> None:
        category = (self.category
                    if isinstance(self.category, ProviderErrorCategory) else
                    _enum_value(ProviderErrorCategory,
                                self.category,
                                name='provider_error.category'))
        object.__setattr__(self, 'category', category)
        object.__setattr__(
            self, 'provider_code',
            _optional_text(self.provider_code,
                           name='provider_error.provider_code'))
        object.__setattr__(
            self, 'retry_after_seconds',
            _optional_nonnegative_integer(
                self.retry_after_seconds,
                name='provider_error.retry_after_seconds'))
        object.__setattr__(
            self, 'normalized_message',
            _optional_text(self.normalized_message,
                           name='provider_error.normalized_message'))

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderErrorV1':
        raw = _closed_object(value, name='provider error', keys=cls._KEYS)
        return cls(category=_enum_value(ProviderErrorCategory,
                                        raw['category'],
                                        name='provider_error.category'),
                   provider_code=raw['provider_code'],
                   retry_after_seconds=raw['retry_after_seconds'],
                   normalized_message=raw['normalized_message'])

    def canonical_value(self) -> JsonObject:
        return {
            'category': self.category.value,
            'provider_code': self.provider_code,
            'retry_after_seconds': self.retry_after_seconds,
            'normalized_message': self.normalized_message,
        }


@dataclasses.dataclass(frozen=True)
class ProviderSubmissionV1(_CanonicalContract):
    """Normalized evidence returned by one submission boundary."""

    disposition: ProviderSubmissionDisposition
    provider_operation_id: str | None
    normalized_response_sha256: str | None
    normalized_error: ProviderErrorV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'disposition', 'provider_operation_id', 'normalized_response_sha256',
        'normalized_error'
    })

    def __post_init__(self) -> None:
        disposition = (self.disposition if isinstance(
            self.disposition, ProviderSubmissionDisposition) else _enum_value(
                ProviderSubmissionDisposition,
                self.disposition,
                name='submission.disposition'))
        object.__setattr__(self, 'disposition', disposition)
        object.__setattr__(
            self, 'provider_operation_id',
            _optional_text(self.provider_operation_id,
                           name='submission.provider_operation_id'))
        if self.normalized_response_sha256 is not None:
            object.__setattr__(
                self, 'normalized_response_sha256',
                _sha256(self.normalized_response_sha256,
                        name='submission.normalized_response_sha256'))
        if self.normalized_error is not None and not isinstance(
                self.normalized_error, ProviderErrorV1):
            raise TypeError('submission normalized error has invalid type.')
        if disposition is ProviderSubmissionDisposition.ACCEPTED:
            if self.normalized_error is not None:
                raise ValueError('accepted submission cannot contain an error.')
        elif disposition is ProviderSubmissionDisposition.NOT_SUBMITTED:
            if (self.provider_operation_id is not None or
                    self.normalized_response_sha256 is not None):
                raise ValueError('not_submitted cannot contain provider '
                                 'submission evidence.')

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderSubmissionV1':
        raw = _closed_object(value, name='provider submission', keys=cls._KEYS)
        error = (None if raw['normalized_error'] is None else
                 ProviderErrorV1.from_value(raw['normalized_error']))
        return cls(disposition=_enum_value(ProviderSubmissionDisposition,
                                           raw['disposition'],
                                           name='submission.disposition'),
                   provider_operation_id=raw['provider_operation_id'],
                   normalized_response_sha256=raw['normalized_response_sha256'],
                   normalized_error=error)

    def canonical_value(self) -> JsonObject:
        return {
            'disposition': self.disposition.value,
            'provider_operation_id': self.provider_operation_id,
            'normalized_response_sha256': self.normalized_response_sha256,
            'normalized_error': (None if self.normalized_error is None else
                                 self.normalized_error.canonical_value()),
        }


@dataclasses.dataclass(frozen=True)
class ProviderLifecycleObservationV1(_CanonicalContract):
    """Identity-qualified read-only provider observation."""

    version: int
    target_sha256: str
    state: ProviderObservationState
    certainty: ProviderObservationCertainty
    observed_provider_operation_id: str | None
    observed_provider_resource_id: str | None
    observed_cluster_record_uuid: uuid.UUID | None
    observed_workload_uid: str | None
    observed_replica_incarnation_label: str | None
    resolved_target: ResolvedProviderTargetV1 | None
    ready: bool | None
    evidence_sha256: str
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'target_sha256', 'state', 'certainty',
        'observed_provider_operation_id', 'observed_provider_resource_id',
        'observed_cluster_record_uuid', 'observed_workload_uid',
        'observed_replica_incarnation_label', 'resolved_target', 'ready',
        'evidence_sha256', 'observed_at'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='observation version')
        object.__setattr__(
            self, 'target_sha256',
            _sha256(self.target_sha256, name='observation.target_sha256'))
        state = (self.state if isinstance(
            self.state, ProviderObservationState) else _enum_value(
                ProviderObservationState, self.state, name='observation.state'))
        certainty = (self.certainty
                     if isinstance(self.certainty, ProviderObservationCertainty)
                     else _enum_value(ProviderObservationCertainty,
                                      self.certainty,
                                      name='observation.certainty'))
        object.__setattr__(self, 'state', state)
        object.__setattr__(self, 'certainty', certainty)
        for field in ('observed_provider_operation_id',
                      'observed_provider_resource_id', 'observed_workload_uid'):
            object.__setattr__(
                self, field,
                _optional_text(getattr(self, field),
                               name=f'observation.{field}'))
        if self.observed_cluster_record_uuid is not None:
            object.__setattr__(
                self, 'observed_cluster_record_uuid',
                _uuid(self.observed_cluster_record_uuid,
                      name='observation.observed_cluster_record_uuid'))
        if self.observed_replica_incarnation_label is not None:
            object.__setattr__(
                self, 'observed_replica_incarnation_label',
                str(
                    _uuid(self.observed_replica_incarnation_label,
                          name=('observation.'
                                'observed_replica_incarnation_label'))))
        if self.resolved_target is not None and not isinstance(
                self.resolved_target, ResolvedProviderTargetV1):
            raise TypeError('observation resolved target has invalid type.')
        if self.ready is not None:
            _boolean(self.ready, name='observation.ready')
        if state is ProviderObservationState.ABSENT and self.ready is not None:
            raise ValueError('absent observation must have ready=null.')
        object.__setattr__(
            self, 'evidence_sha256',
            _sha256(self.evidence_sha256, name='observation.evidence_sha256'))
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at, name='observation.observed_at'))
        if (self.resolved_target is not None and
                self.resolved_target.requested_target_sha256
                != self.target_sha256):
            raise ValueError('observation resolved target hash differs from '
                             'its target hash.')
        if self.resolved_target is not None:
            mismatched_resolved_evidence = any(
                (expected is not None and actual != expected)
                for actual, expected in (
                    (self.observed_provider_operation_id,
                     self.resolved_target.provider_operation_id),
                    (self.observed_provider_resource_id,
                     self.resolved_target.provider_resource_id),
                    (self.observed_workload_uid,
                     self.resolved_target.workload_uid),
                ))
            if (mismatched_resolved_evidence and
                    state is not ProviderObservationState.CONFLICT):
                raise ValueError('observation resolved identity conflicts.')
        if (state is ProviderObservationState.PRESENT and
                certainty is ProviderObservationCertainty.AUTHORITATIVE):
            if (self.observed_cluster_record_uuid is None or
                    self.observed_replica_incarnation_label is None or
                    self.resolved_target is None or
                    self.resolved_target.workload_uid is None or
                    any(actual != expected for actual, expected in (
                        (self.observed_provider_operation_id,
                         self.resolved_target.provider_operation_id),
                        (self.observed_provider_resource_id,
                         self.resolved_target.provider_resource_id),
                        (self.observed_workload_uid,
                         self.resolved_target.workload_uid),
                    ))):
                raise ValueError('authoritative present observation requires '
                                 'complete resolved identity evidence.')

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderLifecycleObservationV1':
        raw = _closed_object(value,
                             name='provider lifecycle observation',
                             keys=cls._KEYS)
        resolved = (None if raw['resolved_target'] is None else
                    ResolvedProviderTargetV1.from_value(raw['resolved_target']))
        cluster_uuid = (None if raw['observed_cluster_record_uuid'] is None else
                        _uuid(raw['observed_cluster_record_uuid'],
                              name=('observation.'
                                    'observed_cluster_record_uuid')))
        return cls(
            version=raw['version'],
            target_sha256=raw['target_sha256'],
            state=_enum_value(ProviderObservationState,
                              raw['state'],
                              name='observation.state'),
            certainty=_enum_value(ProviderObservationCertainty,
                                  raw['certainty'],
                                  name='observation.certainty'),
            observed_provider_operation_id=raw[
                'observed_provider_operation_id'],
            observed_provider_resource_id=raw['observed_provider_resource_id'],
            observed_cluster_record_uuid=cluster_uuid,
            observed_workload_uid=raw['observed_workload_uid'],
            observed_replica_incarnation_label=raw[
                'observed_replica_incarnation_label'],
            resolved_target=resolved,
            ready=raw['ready'],
            evidence_sha256=raw['evidence_sha256'],
            observed_at=raw['observed_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'target_sha256': self.target_sha256,
            'state': self.state.value,
            'certainty': self.certainty.value,
            'observed_provider_operation_id':
                self.observed_provider_operation_id,
            'observed_provider_resource_id': self.observed_provider_resource_id,
            'observed_cluster_record_uuid':
                (None if self.observed_cluster_record_uuid is None else str(
                    self.observed_cluster_record_uuid)),
            'observed_workload_uid': self.observed_workload_uid,
            'observed_replica_incarnation_label':
                self.observed_replica_incarnation_label,
            'resolved_target': (None if self.resolved_target is None else
                                self.resolved_target.canonical_value()),
            'ready': self.ready,
            'evidence_sha256': self.evidence_sha256,
            'observed_at': self.observed_at,
        }

    def validate_target(self, target: ProviderLocatorV1) -> None:
        if self.target_sha256 != target.sha256:
            raise ValueError('observation does not match requested target.')
        if self.resolved_target is not None:
            self.resolved_target.validate_requested_target(target)
        if (self.state is ProviderObservationState.PRESENT and
                self.certainty is ProviderObservationCertainty.AUTHORITATIVE):
            kubernetes = target.kubernetes
            if (not target.is_authoritative_pod_locator or kubernetes is None or
                    self.observed_cluster_record_uuid
                    != target.sky_cluster_record_uuid or
                    self.observed_replica_incarnation_label
                    != kubernetes.replica_incarnation_label):
                raise ValueError('authoritative present observation does not '
                                 'match the frozen target identity.')


@dataclasses.dataclass(frozen=True)
class ServeReplicaActionOutcomeV1(_CanonicalContract):
    """Closed provider outcome consumed by the Serve reducer."""

    disposition: ServeActionDisposition
    certainty: ServeActionCertainty
    provider_operation_id: str | None
    provider_code: str | None
    retry_class: ServeRetryClass | None
    retry_after_seconds: int | None
    observation: ProviderLifecycleObservationV1 | None
    normalized_message: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'disposition', 'certainty', 'provider_operation_id', 'provider_code',
        'retry_class', 'retry_after_seconds', 'observation',
        'normalized_message'
    })

    def __post_init__(self) -> None:
        disposition = (self.disposition
                       if isinstance(self.disposition, ServeActionDisposition)
                       else _enum_value(ServeActionDisposition,
                                        self.disposition,
                                        name='outcome.disposition'))
        certainty = (self.certainty if isinstance(
            self.certainty, ServeActionCertainty) else _enum_value(
                ServeActionCertainty, self.certainty, name='outcome.certainty'))
        object.__setattr__(self, 'disposition', disposition)
        object.__setattr__(self, 'certainty', certainty)
        object.__setattr__(
            self, 'provider_operation_id',
            _optional_text(self.provider_operation_id,
                           name='outcome.provider_operation_id'))
        object.__setattr__(
            self, 'provider_code',
            _optional_text(self.provider_code, name='outcome.provider_code'))
        if self.retry_class is not None:
            retry_class = (self.retry_class if isinstance(
                self.retry_class, ServeRetryClass) else _enum_value(
                    ServeRetryClass,
                    self.retry_class,
                    name='outcome.retry_class'))
            object.__setattr__(self, 'retry_class', retry_class)
        object.__setattr__(
            self, 'retry_after_seconds',
            _optional_nonnegative_integer(self.retry_after_seconds,
                                          name='outcome.retry_after_seconds'))
        if self.observation is not None and not isinstance(
                self.observation, ProviderLifecycleObservationV1):
            raise TypeError('outcome observation has an invalid type.')
        object.__setattr__(
            self, 'normalized_message',
            _optional_text(self.normalized_message,
                           name='outcome.normalized_message'))
        if disposition in (ServeActionDisposition.RETRYABLE,
                           ServeActionDisposition.UNCERTAIN):
            if self.retry_class is None:
                raise ValueError('retryable/uncertain outcome requires a retry '
                                 'class.')
        elif (self.retry_class is not None or
              self.retry_after_seconds is not None):
            raise ValueError('terminal outcome cannot contain retry fields.')
        if (disposition is ServeActionDisposition.UNCERTAIN and
                self.retry_class is not ServeRetryClass.OBSERVATION_REQUIRED):
            raise ValueError('uncertain outcome requires observation_required.')
        if (self.observation is not None and
                self.provider_operation_id is not None and
                self.observation.observed_provider_operation_id is not None and
                self.provider_operation_id
                != self.observation.observed_provider_operation_id):
            raise ValueError('outcome operation IDs conflict.')

    @classmethod
    def from_value(cls, value: Any) -> 'ServeReplicaActionOutcomeV1':
        raw = _closed_object(value,
                             name='Serve replica action outcome',
                             keys=cls._KEYS)
        observation = (None if raw['observation'] is None else
                       ProviderLifecycleObservationV1.from_value(
                           raw['observation']))
        retry_class = (None if raw['retry_class'] is None else _enum_value(
            ServeRetryClass, raw['retry_class'], name='outcome.retry_class'))
        return cls(disposition=_enum_value(ServeActionDisposition,
                                           raw['disposition'],
                                           name='outcome.disposition'),
                   certainty=_enum_value(ServeActionCertainty,
                                         raw['certainty'],
                                         name='outcome.certainty'),
                   provider_operation_id=raw['provider_operation_id'],
                   provider_code=raw['provider_code'],
                   retry_class=retry_class,
                   retry_after_seconds=raw['retry_after_seconds'],
                   observation=observation,
                   normalized_message=raw['normalized_message'])

    def canonical_value(self) -> JsonObject:
        return {
            'disposition': self.disposition.value,
            'certainty': self.certainty.value,
            'provider_operation_id': self.provider_operation_id,
            'provider_code': self.provider_code,
            'retry_class':
                (None if self.retry_class is None else self.retry_class.value),
            'retry_after_seconds': self.retry_after_seconds,
            'observation': (None if self.observation is None else
                            self.observation.canonical_value()),
            'normalized_message': self.normalized_message,
        }


@dataclasses.dataclass(frozen=True)
class ServeShadowProjectionV1(_CanonicalContract):
    """Bounded legacy or proposed Serve row projection."""

    version: int
    action_kind: kernel_actions.ActionKind
    row_disposition: ShadowRowDisposition
    replica_status: ReplicaStatusValue | None
    capacity_outcome: ShadowCapacityOutcome | None
    action_disposition: ServeActionDisposition
    resolved_target: ResolvedProviderTargetV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'action_kind', 'row_disposition', 'replica_status',
        'capacity_outcome', 'action_disposition', 'resolved_target'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='shadow projection version')
        try:
            action_kind = (self.action_kind if isinstance(
                self.action_kind, kernel_actions.ActionKind) else
                           kernel_actions.ActionKind(self.action_kind))
        except (TypeError, ValueError) as e:
            raise ValueError(
                'shadow projection action kind is unsupported.') from e
        object.__setattr__(self, 'action_kind', action_kind)
        row_disposition = (self.row_disposition if isinstance(
            self.row_disposition, ShadowRowDisposition) else _enum_value(
                ShadowRowDisposition,
                self.row_disposition,
                name='projection.row_disposition'))
        object.__setattr__(self, 'row_disposition', row_disposition)
        if self.replica_status is not None:
            object.__setattr__(
                self, 'replica_status', self.replica_status if isinstance(
                    self.replica_status, ReplicaStatusValue) else _enum_value(
                        ReplicaStatusValue,
                        self.replica_status,
                        name='projection.replica_status'))
        if self.capacity_outcome is not None:
            object.__setattr__(
                self, 'capacity_outcome', self.capacity_outcome if isinstance(
                    self.capacity_outcome, ShadowCapacityOutcome) else
                _enum_value(ShadowCapacityOutcome,
                            self.capacity_outcome,
                            name='projection.capacity_outcome'))
        action_disposition = (self.action_disposition if isinstance(
            self.action_disposition, ServeActionDisposition) else _enum_value(
                ServeActionDisposition,
                self.action_disposition,
                name='projection.action_disposition'))
        object.__setattr__(self, 'action_disposition', action_disposition)
        if self.resolved_target is not None and not isinstance(
                self.resolved_target, ResolvedProviderTargetV1):
            raise TypeError('projection resolved target has invalid type.')
        if (row_disposition is ShadowRowDisposition.REMOVED and
                self.replica_status is not None):
            raise ValueError(
                'removed projection must have replica_status=null.')
        if (action_kind is kernel_actions.ActionKind.DOWN and
                self.capacity_outcome is not None):
            raise ValueError('down projection must have capacity_outcome=null.')

    @classmethod
    def from_value(cls, value: Any) -> 'ServeShadowProjectionV1':
        raw = _closed_object(value,
                             name='Serve shadow projection',
                             keys=cls._KEYS)
        resolved = (None if raw['resolved_target'] is None else
                    ResolvedProviderTargetV1.from_value(raw['resolved_target']))
        return cls(
            version=raw['version'],
            action_kind=_enum_value(kernel_actions.ActionKind,
                                    raw['action_kind'],
                                    name='projection.action_kind'),
            row_disposition=_enum_value(ShadowRowDisposition,
                                        raw['row_disposition'],
                                        name='projection.row_disposition'),
            replica_status=(None if raw['replica_status'] is None else
                            _enum_value(ReplicaStatusValue,
                                        raw['replica_status'],
                                        name='projection.replica_status')),
            capacity_outcome=(None if raw['capacity_outcome'] is None else
                              _enum_value(ShadowCapacityOutcome,
                                          raw['capacity_outcome'],
                                          name='projection.capacity_outcome')),
            action_disposition=_enum_value(
                ServeActionDisposition,
                raw['action_disposition'],
                name='projection.action_disposition'),
            resolved_target=resolved)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'action_kind': self.action_kind.value,
            'row_disposition': self.row_disposition.value,
            'replica_status': (None if self.replica_status is None else
                               self.replica_status.value),
            'capacity_outcome': (None if self.capacity_outcome is None else
                                 self.capacity_outcome.value),
            'action_disposition': self.action_disposition.value,
            'resolved_target': (None if self.resolved_target is None else
                                self.resolved_target.canonical_value()),
        }


@dataclasses.dataclass(frozen=True)
class ServeShadowRetryDecisionV1(_CanonicalContract):
    """Bounded retry interpretation for one legacy logical attempt."""

    version: int
    decision: ShadowRetryDecision
    retry_class: ServeRetryClass | None
    delay_seconds: int | None
    logical_attempt: int

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'decision', 'retry_class', 'delay_seconds', 'logical_attempt'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='shadow retry version')
        decision = (self.decision if isinstance(
            self.decision, ShadowRetryDecision) else _enum_value(
                ShadowRetryDecision, self.decision, name='retry.decision'))
        object.__setattr__(self, 'decision', decision)
        if self.retry_class is not None:
            object.__setattr__(
                self, 'retry_class', self.retry_class if
                isinstance(self.retry_class, ServeRetryClass) else _enum_value(
                    ServeRetryClass, self.retry_class,
                    name='retry.retry_class'))
        object.__setattr__(
            self, 'delay_seconds',
            _optional_nonnegative_integer(self.delay_seconds,
                                          name='retry.delay_seconds'))
        object.__setattr__(
            self, 'logical_attempt',
            _positive_integer(self.logical_attempt,
                              name='retry.logical_attempt'))
        if decision in (ShadowRetryDecision.RETRY_SAME_PLAN,
                        ShadowRetryDecision.REPLAN_NEW_GENERATION,
                        ShadowRetryDecision.OBSERVE):
            if self.retry_class is None or self.delay_seconds is None:
                raise ValueError('retry/observe decision requires class and '
                                 'delay.')
        elif self.retry_class is not None or self.delay_seconds is not None:
            raise ValueError('block/terminal decision cannot contain retry '
                             'fields.')
        if (decision is ShadowRetryDecision.OBSERVE and
                self.retry_class is not ServeRetryClass.OBSERVATION_REQUIRED):
            raise ValueError('observe decision requires observation_required.')

    @classmethod
    def from_value(cls, value: Any) -> 'ServeShadowRetryDecisionV1':
        raw = _closed_object(value,
                             name='Serve shadow retry decision',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            decision=_enum_value(ShadowRetryDecision,
                                 raw['decision'],
                                 name='retry.decision'),
            retry_class=(None if raw['retry_class'] is None else _enum_value(
                ServeRetryClass, raw['retry_class'], name='retry.retry_class')),
            delay_seconds=raw['delay_seconds'],
            logical_attempt=raw['logical_attempt'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'decision': self.decision.value,
            'retry_class':
                (None if self.retry_class is None else self.retry_class.value),
            'delay_seconds': self.delay_seconds,
            'logical_attempt': self.logical_attempt,
        }
