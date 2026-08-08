"""Closed Serve035 authority-plane values.

This module is deliberately independent of the Serve033/P2a codecs.  The
version-one registration contracts in :mod:`sky.serve.resource_actions` are
historical, byte-frozen values; Serve035 runtime membership is represented
only by the version-two values below.

The classes in this file are pure.  They perform no database, Kubernetes, or
provider I/O and accept no ambient configuration.  Every persisted value has
one bounded canonical JSON representation and recomputes its own digest.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import hashlib
import itertools
import json
import re
from typing import Any, ClassVar, TypeVar
import unicodedata
import uuid

from sky.serve import resource_actions

RESOURCE_ACTION_WORKER_REGISTRATION_LEASE_RENEW_SECONDS_V1 = 20
RESOURCE_ACTION_WORKER_REGISTRATION_LEASE_TTL_SECONDS_V1 = 60
RESOURCE_ACTION_WORKER_API_INSTANCE_LEASE_TTL_SECONDS_V1 = 20
RESOURCE_ACTION_WORKER_FENCE_MAX_REQUEST_CLAIMS_V1 = 64
RESOURCE_ACTION_WORKER_FENCE_MAX_REQUEST_CLAIMS_JSON_BYTES_V1 = 24_576
RESOURCE_ACTION_WORKER_FENCE_MAX_CANONICAL_BYTES_V1 = 30_720
RESOURCE_ACTION_WORKER_COLD_FENCES_MAX_CANONICAL_BYTES_V1 = 65_536
RESOURCE_ACTION_QUALIFICATION_POLICY_PATH_V1 = (
    '/etc/skypilot/resource-actions/qualification-policy.json')

_MAX_CANONICAL_BYTES = 65_536
_MAX_TEXT_BYTES = 1_024
_MAX_SHORT_TEXT_BYTES = 253
_MAX_POSTGRES_BIGINT = 2**63 - 1
_MAX_STRICT_JSON_DEPTH = 32
_MAX_STRICT_JSON_MEMBERS = 8_192
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_SHA256_DIGEST_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
_COMMIT_RE = re.compile(r'^[0-9a-f]{40}$')
_UTC_TIMESTAMP_RE = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:'
                               r'[0-9]{2}\.[0-9]{6}Z$')
_WORKER_REGISTRATION_MAX_AGE = datetime.timedelta(minutes=5)
_UTC = datetime.timezone.utc

JsonObject = dict[str, Any]
_EnumT = TypeVar('_EnumT', bound=enum.Enum)


def canonical_json_bytes(value: Any) -> bytes:
    """Return the resource-action kernel's canonical JSON encoding."""

    return resource_actions.canonical_json_bytes(value)


def canonical_sha256(value: Any) -> str:
    """Return the SHA-256 of the canonical JSON encoding."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _closed_object(value: Any, *, name: str,
                   keys: frozenset[str]) -> JsonObject:
    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    if any(type(key) is not str for key in value):
        raise TypeError(f'{name} keys must be text.')
    if set(value) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    encoded = canonical_json_bytes(value)
    if len(encoded) > _MAX_CANONICAL_BYTES:
        raise ValueError(f'{name} exceeds {_MAX_CANONICAL_BYTES} bytes.')
    normalized = json.loads(encoded.decode('utf-8'))
    if normalized != value:
        raise ValueError(f'{name} is not canonical.')
    return normalized


def _canonical_object(value: Any,
                      *,
                      name: str,
                      maximum_bytes: int = _MAX_CANONICAL_BYTES) -> JsonObject:
    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    encoded = canonical_json_bytes(value)
    if len(encoded) > maximum_bytes:
        raise ValueError(f'{name} exceeds {maximum_bytes} bytes.')
    normalized = json.loads(encoded.decode('utf-8'))
    if normalized != value:
        raise ValueError(f'{name} is not canonical.')
    return normalized


def _strict_canonical_json_object(value: Any, *, name: str) -> JsonObject:
    """Return a bounded immutable-source JSON tree with exact scalar types."""

    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    stack: list[tuple[Any, int]] = [(value, 0)]
    seen_containers: set[int] = set()
    aggregate_members = 0
    while stack:
        current, depth = stack.pop()
        if current is None or type(current) is bool:
            continue
        if type(current) is int:
            if not -(2**63) <= current <= _MAX_POSTGRES_BIGINT:
                raise ValueError(f'{name} integer is outside signed-int64.')
            continue
        if type(current) is str:
            if ('\x00' in current or
                    unicodedata.normalize('NFC', current) != current):
                raise ValueError(f'{name} text is not canonical NFC text.')
            continue
        if type(current) not in (dict, list):
            raise TypeError(f'{name} contains a non-JSON or subclass value.')
        if depth >= _MAX_STRICT_JSON_DEPTH:
            raise ValueError(f'{name} exceeds the JSON depth bound.')
        identity = id(current)
        if identity in seen_containers:
            raise ValueError(f'{name} contains a cycle or shared container.')
        seen_containers.add(identity)
        aggregate_members += len(current)
        if aggregate_members > _MAX_STRICT_JSON_MEMBERS:
            raise ValueError(f'{name} exceeds the JSON member bound.')
        if type(current) is dict:
            for key, item in current.items():
                if (type(key) is not str or '\x00' in key or
                        unicodedata.normalize('NFC', key) != key):
                    raise ValueError(f'{name} contains a noncanonical key.')
                stack.append((item, depth + 1))
        else:
            stack.extend((item, depth + 1) for item in current)
    return _canonical_object(value, name=name)


def _parse_strict_canonical_json_object(value: Any, *, name: str) -> JsonObject:
    """Parse exact compact canonical JSON text without duplicate keys."""

    if type(value) is not str:
        raise TypeError(f'{name} must be canonical JSON text.')

    def _closed_pairs(pairs: list[tuple[str, Any]]) -> JsonObject:
        result: JsonObject = {}
        for key, item in pairs:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = item
        return result

    def _forbid_noninteger_number(_: str) -> Any:
        raise ValueError('noninteger JSON number')

    try:
        raw = value.encode('utf-8')
        if not raw or len(raw) > _MAX_CANONICAL_BYTES:
            raise ValueError('canonical JSON byte size is outside bounds')
        parsed = json.loads(value,
                            object_pairs_hook=_closed_pairs,
                            parse_float=_forbid_noninteger_number,
                            parse_constant=_forbid_noninteger_number)
        normalized = _strict_canonical_json_object(parsed, name=name)
        if canonical_json_bytes(normalized) != raw:
            raise ValueError('JSON text is not the exact canonical encoding')
        return normalized
    except (UnicodeError, json.JSONDecodeError, RecursionError, TypeError,
            ValueError) as e:
        raise ValueError(f'{name} is not exact canonical JSON.') from e


def _normalized_pod_template_text_map(value: Any, *, name: str,
                                      require_nonempty: bool) -> JsonObject:
    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    if require_nonempty and not value:
        raise ValueError(f'{name} must not be empty.')
    result: JsonObject = {}
    for key in sorted(value):
        item = value[key]
        if type(key) is not str or type(item) is not str:
            raise TypeError(f'{name} keys and values must be exact text.')
        if (not key or '\x00' in key or '\x00' in item or
                unicodedata.normalize('NFC', key) != key or
                unicodedata.normalize('NFC', item) != item):
            raise ValueError(f'{name} contains noncanonical text.')
        result[key] = item
    return result


def _normalize_ordinary_role_pod_template(value: Any) -> JsonObject:
    """Normalize only the raw Deployment PodTemplate API projection."""

    template = _strict_canonical_json_object(
        value, name='qualified ordinary-role Pod template')
    if set(template) != {'metadata', 'spec'}:
        raise ValueError('qualified Pod template must contain exactly metadata '
                         'and spec.')
    metadata = template['metadata']
    if type(metadata) is not dict:
        raise TypeError('qualified Pod-template metadata must be an object.')
    allowed_metadata = {'annotations', 'creationTimestamp', 'labels'}
    if (not set(metadata).issubset(allowed_metadata) or
            'labels' not in metadata):
        raise ValueError('qualified Pod-template metadata has unknown or '
                         'missing fields.')
    if ('creationTimestamp' in metadata and
            metadata['creationTimestamp'] is not None):
        raise ValueError('qualified Pod-template creationTimestamp must be '
                         'absent or null.')
    labels = _normalized_pod_template_text_map(
        metadata['labels'],
        name='qualified Pod-template labels',
        require_nonempty=True)
    if 'pod-template-hash' in labels:
        raise ValueError('Deployment Pod template cannot contain the '
                         'controller-owned pod-template-hash label.')
    annotations_value = metadata.get('annotations')
    if annotations_value is None:
        annotations_value = {}
    annotations = _normalized_pod_template_text_map(
        annotations_value,
        name='qualified Pod-template annotations',
        require_nonempty=False)
    spec = template['spec']
    if type(spec) is not dict or not spec:
        raise TypeError(
            'qualified Pod-template spec must be a nonempty object.')
    containers = spec.get('containers')
    if type(containers) is not list or not containers:
        raise ValueError('qualified Pod-template spec must contain containers.')
    container_names: set[str] = set()
    for field in ('containers', 'initContainers', 'ephemeralContainers'):
        values = spec.get(field, [])
        if type(values) is not list:
            raise TypeError(f'qualified Pod-template {field} must be a list.')
        for container in values:
            if type(container) is not dict:
                raise TypeError('qualified Pod-template containers must be '
                                'objects.')
            name = container.get('name')
            if type(name) is not str or not name or name in container_names:
                raise ValueError('qualified Pod-template container names must '
                                 'be nonempty and globally unique.')
            container_names.add(name)
    normalized = {
        'metadata': {
            'annotations': annotations,
            'labels': labels,
        },
        'spec': spec,
    }
    return _strict_canonical_json_object(
        normalized, name='normalized qualified ordinary-role Pod template')


def _text(value: Any,
          *,
          name: str,
          maximum_bytes: int = _MAX_TEXT_BYTES) -> str:
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    if '\x00' in value:
        raise ValueError(f'{name} cannot contain U+0000.')
    size = len(value.encode('utf-8'))
    if size == 0 or size > maximum_bytes:
        raise ValueError(f'{name} must be 1..{maximum_bytes} UTF-8 bytes.')
    if json.loads(canonical_json_bytes(value).decode('utf-8')) != value:
        raise ValueError(f'{name} is not canonical text.')
    return value


def _sha256(value: Any, *, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be lowercase SHA-256 hex.')
    return value


def _sha256_digest(value: Any, *, name: str) -> str:
    if type(value) is not str or _SHA256_DIGEST_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be sha256:<64 lowercase hex>.')
    return value


def _commit(value: Any, *, name: str) -> str:
    if type(value) is not str or _COMMIT_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be a 40-character lowercase commit.')
    return value


def _positive_integer(value: Any, *, name: str) -> int:
    if type(value) is not int or value <= 0 or value > _MAX_POSTGRES_BIGINT:
        raise ValueError(f'{name} must be a positive signed-int64 integer.')
    return value


def _nonnegative_integer(value: Any, *, name: str) -> int:
    if type(value) is not int or value < 0 or value > _MAX_POSTGRES_BIGINT:
        raise ValueError(f'{name} must be a nonnegative signed-int64 integer.')
    return value


def _boolean(value: Any, *, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f'{name} must be a Boolean.')
    return value


def _uuid(value: Any, *, name: str) -> uuid.UUID:
    if type(value) is uuid.UUID:
        parsed = value
    elif type(value) is str:
        try:
            parsed = uuid.UUID(value)
        except ValueError as e:
            raise ValueError(f'{name} must be a UUID.') from e
        if str(parsed) != value:
            raise ValueError(f'{name} must be lowercase hyphenated UUID text.')
    else:
        raise TypeError(f'{name} must be a UUID or canonical UUID text.')
    return parsed


def _schema_uuid(value: Any, *, name: str) -> uuid.UUID:
    parsed = _uuid(value, name=name)
    if (parsed.variant != uuid.RFC_4122 or parsed.version is None or
            not 1 <= parsed.version <= 5):
        raise ValueError(f'{name} must be an RFC 4122 version 1..5 UUID.')
    return parsed


def _enum_value(enum_type: type[_EnumT], value: Any, *, name: str) -> _EnumT:
    if type(value) is enum_type:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    try:
        parsed = enum_type(value)
    except ValueError as e:
        raise ValueError(f'{name} is unsupported.') from e
    if parsed.value != value:
        raise ValueError(f'{name} is not canonical.')
    return parsed


def _timestamp(value: Any, *, name: str) -> str:
    if type(value) is not str or _UTC_TIMESTAMP_RE.fullmatch(value) is None:
        raise ValueError(
            f'{name} must be UTC RFC 3339 with six fractional digits.')
    try:
        parsed = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError as e:
        raise ValueError(f'{name} is not a valid UTC timestamp.') from e
    if parsed.strftime('%Y-%m-%dT%H:%M:%S.%fZ') != value:
        raise ValueError(f'{name} is not canonical.')
    return value


def datetime_to_timestamp(value: datetime.datetime, *, name: str) -> str:
    """Convert one timezone-aware database timestamp to canonical UTC text."""

    if (not isinstance(value, datetime.datetime) or value.tzinfo is None or
            value.utcoffset() is None):
        raise TypeError(f'{name} must be a timezone-aware datetime.')
    return value.astimezone(_UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def timestamp_to_datetime(value: str, *, name: str) -> datetime.datetime:
    """Parse canonical UTC text into a timezone-aware datetime."""

    _timestamp(value, name=name)
    return datetime.datetime.strptime(
        value, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=_UTC)


class CanonicalContract:
    """Common encoding helpers for closed immutable contracts."""

    def canonical_value(self) -> JsonObject:
        raise NotImplementedError

    @property
    def canonical_bytes(self) -> bytes:
        encoded = canonical_json_bytes(self.canonical_value())
        if len(encoded) > _MAX_CANONICAL_BYTES:
            raise ValueError(
                f'{type(self).__name__} exceeds {_MAX_CANONICAL_BYTES} bytes.')
        return encoded

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


@dataclasses.dataclass(frozen=True)
class HashedCanonicalObjectV1(CanonicalContract):
    """A bounded canonical object paired with its recomputed hash.

    This is used only where the design deliberately delegates an inventory's
    internal schema to its owning component.  It never turns an arbitrary
    caller object into trust: proof builders must still construct the object
    from locked server-side state.
    """

    value: JsonObject
    value_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'value', 'value_sha256'})

    def __post_init__(self) -> None:
        normalized = _strict_canonical_json_object(self.value,
                                                   name='hashed object')
        object.__setattr__(self, 'value', normalized)
        digest = _sha256(self.value_sha256, name='hashed object digest')
        if digest != canonical_sha256(normalized):
            raise ValueError('hashed object digest does not match its value.')
        object.__setattr__(self, 'value_sha256', digest)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> HashedCanonicalObjectV1:
        if type(value) is not dict:
            raise TypeError('hashed canonical object must be an object.')
        if (any(type(key) is not str for key in value) or
                set(value) != cls._KEYS):
            raise ValueError('hashed canonical object has unknown or missing '
                             'fields.')
        return cls(value=value['value'], value_sha256=value['value_sha256'])

    @classmethod
    def from_object(cls, value: JsonObject) -> HashedCanonicalObjectV1:
        normalized = _strict_canonical_json_object(value, name='hashed object')
        return cls(normalized, canonical_sha256(normalized))

    def canonical_value(self) -> JsonObject:
        normalized = _strict_canonical_json_object(self.value,
                                                   name='hashed object')
        if canonical_sha256(normalized) != self.value_sha256:
            raise ValueError('hashed object was mutated after validation.')
        return {'value': normalized, 'value_sha256': self.value_sha256}


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerCohortManifestV2(CanonicalContract):
    """Serve035 static authority-worker manifest.

    The nested renderer inputs remain the shipped V1 leaf contracts.  The new
    top-level version and claim contract prevent a V1/Recreate manifest from
    being reinterpreted as RollingUpdate membership.
    """

    version: int
    cohort_id: str
    namespace: str
    deployment_name: str
    service_account_name: str
    container_name: str
    image: resource_actions.ProviderOCIImageQualificationV1
    pod_template_contract: resource_actions.ProviderRepoArtifactRefV1
    pod_template_binding: resource_actions.ProviderAuthorityWorkerPodTemplateBindingV1
    artifact_inventory: resource_actions.ProviderRepoArtifactRefV1
    callable_inventory: resource_actions.ProviderRepoArtifactRefV1
    claim_contract: str
    handler_allowlist: tuple[str, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'cohort_id', 'namespace', 'deployment_name',
        'service_account_name', 'container_name', 'image',
        'pod_template_contract', 'pod_template_binding', 'artifact_inventory',
        'callable_inventory', 'claim_contract', 'handler_allowlist'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('cohort manifest version must be integer 2.')
        object.__setattr__(self, 'cohort_id',
                           _text(self.cohort_id, name='manifest.cohort_id'))
        for field in ('namespace', 'deployment_name', 'service_account_name'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'manifest.{field}',
                      maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        if self.container_name != 'skypilot-authority-worker':
            raise ValueError('manifest container_name is unsupported.')
        if type(self.image
               ) is not resource_actions.ProviderOCIImageQualificationV1:
            raise TypeError('manifest image has an invalid type.')
        for field in ('pod_template_contract', 'artifact_inventory',
                      'callable_inventory'):
            if type(getattr(
                    self,
                    field)) is not resource_actions.ProviderRepoArtifactRefV1:
                raise TypeError(f'manifest {field} has an invalid type.')
        if type(
                self.pod_template_binding
        ) is not resource_actions.ProviderAuthorityWorkerPodTemplateBindingV1:
            raise TypeError(
                'manifest Pod-template binding has an invalid type.')
        release = self.pod_template_binding.release_inputs
        if (release.cohort_id != self.cohort_id or
                release.namespace != self.namespace or
                release.deployment_name != self.deployment_name or
                release.service_account_name != self.service_account_name or
                release.container_name != self.container_name or
                release.image != self.image.requested_reference):
            raise ValueError('manifest does not match its renderer inputs.')
        if (self.pod_template_binding.projector_artifact_sha256
                != self.pod_template_contract.sha256):
            raise ValueError('manifest projector artifact does not match.')
        if self.claim_contract != 'frozen_action_cohort_join_v2':
            raise ValueError('manifest claim contract must be V2.')
        if (type(self.handler_allowlist) is not tuple or
                self.handler_allowlist != resource_actions.
                PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1):
            raise ValueError('manifest handler allowlist is not exact.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerCohortManifestV2:
        raw = _closed_object(value,
                             name='authority worker V2 manifest',
                             keys=cls._KEYS)
        handlers = raw['handler_allowlist']
        if type(handlers) is not list:
            raise TypeError('manifest handler_allowlist must be a list.')
        return cls(
            version=raw['version'],
            cohort_id=raw['cohort_id'],
            namespace=raw['namespace'],
            deployment_name=raw['deployment_name'],
            service_account_name=raw['service_account_name'],
            container_name=raw['container_name'],
            image=resource_actions.ProviderOCIImageQualificationV1.from_value(
                raw['image']),
            pod_template_contract=resource_actions.ProviderRepoArtifactRefV1.
            from_value(raw['pod_template_contract']),
            pod_template_binding=(
                resource_actions.ProviderAuthorityWorkerPodTemplateBindingV1.
                from_value(raw['pod_template_binding'])),
            artifact_inventory=resource_actions.ProviderRepoArtifactRefV1.
            from_value(raw['artifact_inventory']),
            callable_inventory=resource_actions.ProviderRepoArtifactRefV1.
            from_value(raw['callable_inventory']),
            claim_contract=raw['claim_contract'],
            handler_allowlist=tuple(handlers))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'cohort_id': self.cohort_id,
            'namespace': self.namespace,
            'deployment_name': self.deployment_name,
            'service_account_name': self.service_account_name,
            'container_name': 'skypilot-authority-worker',
            'image': self.image.canonical_value(),
            'pod_template_contract':
                self.pod_template_contract.canonical_value(),
            'pod_template_binding': self.pod_template_binding.canonical_value(),
            'artifact_inventory': self.artifact_inventory.canonical_value(),
            'callable_inventory': self.callable_inventory.canonical_value(),
            'claim_contract': 'frozen_action_cohort_join_v2',
            'handler_allowlist': list(self.handler_allowlist),
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerCohortV2(CanonicalContract):
    """Resolved immutable identity for a Serve035 worker cohort."""

    version: int
    manifest: ProviderAuthorityWorkerCohortManifestV2
    manifest_sha256: str
    deployment_uid: str
    service_account_uid: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'manifest', 'manifest_sha256', 'deployment_uid',
        'service_account_uid'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('worker cohort version must be integer 2.')
        if type(self.manifest) is not ProviderAuthorityWorkerCohortManifestV2:
            raise TypeError('worker cohort manifest has an invalid type.')
        digest = _sha256(self.manifest_sha256, name='cohort.manifest_sha256')
        if digest != self.manifest.sha256:
            raise ValueError('worker cohort manifest hash does not match.')
        object.__setattr__(self, 'manifest_sha256', digest)
        for field in ('deployment_uid', 'service_account_uid'):
            object.__setattr__(
                self, field, _text(getattr(self, field),
                                   name=f'cohort.{field}'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerCohortV2:
        raw = _closed_object(value,
                             name='authority worker V2 cohort',
                             keys=cls._KEYS)
        return cls(version=raw['version'],
                   manifest=ProviderAuthorityWorkerCohortManifestV2.from_value(
                       raw['manifest']),
                   manifest_sha256=raw['manifest_sha256'],
                   deployment_uid=raw['deployment_uid'],
                   service_account_uid=raw['service_account_uid'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'manifest': self.manifest.canonical_value(),
            'manifest_sha256': self.manifest_sha256,
            'deployment_uid': self.deployment_uid,
            'service_account_uid': self.service_account_uid,
        }

    @property
    def cohort_id(self) -> str:
        return self.manifest.cohort_id


def validate_locked_action_spec_cohort_v2(
    action_spec_or_reference: resource_actions.ServeReplicaActionSpecV2 |
    resource_actions.ProviderAuthorityWorkerCohortReferenceV1,
    cohort: ProviderAuthorityWorkerCohortV2,
) -> None:
    """Resolve one compact live reference against a parsed locked V2 row.

    The retained cohort object is the sole authority.  Its digest is
    recomputed from the typed canonical value here; callers cannot substitute
    a version, cohort ID, or digest scalar for the locked row.
    """

    if type(action_spec_or_reference
           ) is resource_actions.ServeReplicaActionSpecV2:
        reference = action_spec_or_reference.invocation.executor_cohort_reference
    elif type(action_spec_or_reference) is (
            resource_actions.ProviderAuthorityWorkerCohortReferenceV1):
        reference = action_spec_or_reference
    else:
        raise TypeError('action_spec_or_reference must be a live V2 action '
                        'spec or compact cohort reference.')
    if type(cohort) is not ProviderAuthorityWorkerCohortV2:
        raise TypeError('cohort must be a parsed locked V2 worker cohort.')
    if (reference.cohort_id != cohort.cohort_id or
            reference.cohort_identity_sha256 != cohort.sha256):
        raise ValueError('action spec cohort reference does not match the '
                         'parsed locked V2 worker cohort.')


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerIdentityV2(CanonicalContract):
    """One fresh self-read Pod/owner-chain identity for Serve035."""

    version: int
    namespace: str
    pod_name: str
    pod_uid: uuid.UUID
    pod_resource_version: str
    pod_service_account_name: str
    pod_controller_owner: resource_actions.ProviderKubernetesControllerOwnerV1
    replica_set_name: str
    replica_set_uid: str
    replica_set_resource_version: str
    replica_set_controller_owner: resource_actions.ProviderKubernetesControllerOwnerV1
    deployment_name: str
    deployment_uid: str
    deployment_generation: int
    deployment_observed_generation: int
    pod_template_contract_sha256: str
    image: resource_actions.ProviderAuthorityWorkerImageV1
    service_account_uid: str
    artifact_inventory_sha256: str
    callable_inventory_sha256: str
    handler_allowlist_sha256: str
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'namespace', 'pod_name', 'pod_uid', 'pod_resource_version',
        'pod_service_account_name', 'pod_controller_owner', 'replica_set_name',
        'replica_set_uid', 'replica_set_resource_version',
        'replica_set_controller_owner', 'deployment_name', 'deployment_uid',
        'deployment_generation', 'deployment_observed_generation',
        'pod_template_contract_sha256', 'image', 'service_account_uid',
        'artifact_inventory_sha256', 'callable_inventory_sha256',
        'handler_allowlist_sha256', 'observed_at'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('worker identity version must be integer 2.')
        for field in ('namespace', 'pod_name', 'pod_service_account_name',
                      'replica_set_name', 'deployment_name'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'worker.{field}',
                      maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(self, 'pod_uid',
                           _uuid(self.pod_uid, name='worker.pod_uid'))
        for field in ('pod_resource_version', 'replica_set_uid',
                      'replica_set_resource_version', 'deployment_uid',
                      'service_account_uid'):
            object.__setattr__(
                self, field, _text(getattr(self, field),
                                   name=f'worker.{field}'))
        if type(self.pod_controller_owner
               ) is not resource_actions.ProviderKubernetesControllerOwnerV1:
            raise TypeError('worker Pod owner has an invalid type.')
        if type(self.replica_set_controller_owner
               ) is not resource_actions.ProviderKubernetesControllerOwnerV1:
            raise TypeError('worker ReplicaSet owner has an invalid type.')
        if (self.pod_controller_owner.kind != 'ReplicaSet' or
                self.replica_set_controller_owner.kind != 'Deployment' or
                self.pod_controller_owner.name != self.replica_set_name or
                self.pod_controller_owner.uid != self.replica_set_uid or
                self.replica_set_controller_owner.name != self.deployment_name
                or
                self.replica_set_controller_owner.uid != self.deployment_uid):
            raise ValueError('worker controller-owner chain is inconsistent.')
        generation = _positive_integer(self.deployment_generation,
                                       name='worker.deployment_generation')
        observed = _positive_integer(
            self.deployment_observed_generation,
            name='worker.deployment_observed_generation')
        if generation != observed:
            raise ValueError('worker Deployment generation is not current.')
        object.__setattr__(self, 'deployment_generation', generation)
        object.__setattr__(self, 'deployment_observed_generation', observed)
        for field in ('pod_template_contract_sha256',
                      'artifact_inventory_sha256', 'callable_inventory_sha256',
                      'handler_allowlist_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field), name=f'worker.{field}'))
        if type(self.image
               ) is not resource_actions.ProviderAuthorityWorkerImageV1:
            raise TypeError('worker image has an invalid type.')
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at, name='worker.observed_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerIdentityV2:
        raw = _closed_object(value,
                             name='authority worker V2 identity',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            namespace=raw['namespace'],
            pod_name=raw['pod_name'],
            pod_uid=raw['pod_uid'],
            pod_resource_version=raw['pod_resource_version'],
            pod_service_account_name=raw['pod_service_account_name'],
            pod_controller_owner=(
                resource_actions.ProviderKubernetesControllerOwnerV1.from_value(
                    raw['pod_controller_owner'])),
            replica_set_name=raw['replica_set_name'],
            replica_set_uid=raw['replica_set_uid'],
            replica_set_resource_version=raw['replica_set_resource_version'],
            replica_set_controller_owner=(
                resource_actions.ProviderKubernetesControllerOwnerV1.from_value(
                    raw['replica_set_controller_owner'])),
            deployment_name=raw['deployment_name'],
            deployment_uid=raw['deployment_uid'],
            deployment_generation=raw['deployment_generation'],
            deployment_observed_generation=raw[
                'deployment_observed_generation'],
            pod_template_contract_sha256=raw['pod_template_contract_sha256'],
            image=resource_actions.ProviderAuthorityWorkerImageV1.from_value(
                raw['image']),
            service_account_uid=raw['service_account_uid'],
            artifact_inventory_sha256=raw['artifact_inventory_sha256'],
            callable_inventory_sha256=raw['callable_inventory_sha256'],
            handler_allowlist_sha256=raw['handler_allowlist_sha256'],
            observed_at=raw['observed_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'namespace': self.namespace,
            'pod_name': self.pod_name,
            'pod_uid': str(self.pod_uid),
            'pod_resource_version': self.pod_resource_version,
            'pod_service_account_name': self.pod_service_account_name,
            'pod_controller_owner': self.pod_controller_owner.canonical_value(),
            'replica_set_name': self.replica_set_name,
            'replica_set_uid': self.replica_set_uid,
            'replica_set_resource_version': self.replica_set_resource_version,
            'replica_set_controller_owner':
                self.replica_set_controller_owner.canonical_value(),
            'deployment_name': self.deployment_name,
            'deployment_uid': self.deployment_uid,
            'deployment_generation': self.deployment_generation,
            'deployment_observed_generation':
                self.deployment_observed_generation,
            'pod_template_contract_sha256': self.pod_template_contract_sha256,
            'image': self.image.canonical_value(),
            'service_account_uid': self.service_account_uid,
            'artifact_inventory_sha256': self.artifact_inventory_sha256,
            'callable_inventory_sha256': self.callable_inventory_sha256,
            'handler_allowlist_sha256': self.handler_allowlist_sha256,
            'observed_at': self.observed_at,
        }

    def validate_for_cohort(self,
                            cohort: ProviderAuthorityWorkerCohortV2) -> None:
        if type(cohort) is not ProviderAuthorityWorkerCohortV2:
            raise TypeError('worker cohort has an invalid type.')
        manifest = cohort.manifest
        if (self.namespace != manifest.namespace or
                self.pod_service_account_name != manifest.service_account_name
                or self.deployment_name != manifest.deployment_name or
                self.deployment_uid != cohort.deployment_uid or
                self.service_account_uid != cohort.service_account_uid or
                self.pod_template_contract_sha256
                != manifest.pod_template_contract.sha256 or
                self.artifact_inventory_sha256
                != manifest.artifact_inventory.sha256 or
                self.callable_inventory_sha256
                != manifest.callable_inventory.sha256 or
                self.handler_allowlist_sha256
                != resource_actions.canonical_sha256(
                    list(manifest.handler_allowlist)) or
                self.image.qualification.canonical_bytes
                != manifest.image.canonical_bytes):
            raise ValueError('worker identity does not match its cohort.')


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerRegistrationV2(CanonicalContract):
    """One current ready-Pod V2 registration."""

    version: int
    worker_instance_id: uuid.UUID
    worker: ProviderAuthorityWorkerIdentityV2
    pod_ready: bool
    registered_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'worker_instance_id', 'worker', 'pod_ready', 'registered_at'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('worker registration version must be integer 2.')
        instance_id = _uuid(self.worker_instance_id,
                            name='registration.worker_instance_id')
        object.__setattr__(self, 'worker_instance_id', instance_id)
        if type(self.worker) is not ProviderAuthorityWorkerIdentityV2:
            raise TypeError('registration worker has an invalid type.')
        if instance_id != self.worker.pod_uid:
            raise ValueError('worker instance ID must equal the Pod UID.')
        _boolean(self.pod_ready, name='registration.pod_ready')
        if not self.pod_ready:
            raise ValueError('worker registration requires a ready Pod.')
        registered = _timestamp(self.registered_at,
                                name='registration.registered_at')
        if timestamp_to_datetime(
                self.worker.observed_at,
                name='registration.worker.observed_at') > timestamp_to_datetime(
                    registered, name='registration.registered_at'):
            raise ValueError('registration predates its worker observation.')
        object.__setattr__(self, 'registered_at', registered)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerRegistrationV2:
        raw = _closed_object(value,
                             name='authority worker V2 registration',
                             keys=cls._KEYS)
        return cls(version=raw['version'],
                   worker_instance_id=raw['worker_instance_id'],
                   worker=ProviderAuthorityWorkerIdentityV2.from_value(
                       raw['worker']),
                   pod_ready=raw['pod_ready'],
                   registered_at=raw['registered_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'worker_instance_id': str(self.worker_instance_id),
            'worker': self.worker.canonical_value(),
            'pod_ready': True,
            'registered_at': self.registered_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerDeploymentSnapshotV2(CanonicalContract):
    """The sole set-level final Deployment snapshot."""

    version: int
    deployment_name: str
    deployment_uid: str
    deployment_resource_version: str
    deployment_generation: int
    deployment_observed_generation: int
    pod_template_contract_sha256: str
    deployment_strategy: str
    deployment_max_surge: int
    deployment_max_unavailable: int
    deployment_spec_replicas: int
    deployment_status_replicas: int
    deployment_updated_replicas: int
    deployment_ready_replicas: int
    deployment_available_replicas: int
    deployment_unavailable_replicas: int
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'deployment_name', 'deployment_uid',
        'deployment_resource_version', 'deployment_generation',
        'deployment_observed_generation', 'pod_template_contract_sha256',
        'deployment_strategy', 'deployment_max_surge',
        'deployment_max_unavailable', 'deployment_spec_replicas',
        'deployment_status_replicas', 'deployment_updated_replicas',
        'deployment_ready_replicas', 'deployment_available_replicas',
        'deployment_unavailable_replicas', 'observed_at'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('Deployment snapshot version must be integer 2.')
        object.__setattr__(
            self, 'deployment_name',
            _text(self.deployment_name,
                  name='snapshot.deployment_name',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        for field in ('deployment_uid', 'deployment_resource_version'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field), name=f'snapshot.{field}'))
        generation = _positive_integer(self.deployment_generation,
                                       name='snapshot.deployment_generation')
        observed = _positive_integer(
            self.deployment_observed_generation,
            name='snapshot.deployment_observed_generation')
        if generation != observed:
            raise ValueError('snapshot Deployment generation is not current.')
        object.__setattr__(self, 'deployment_generation', generation)
        object.__setattr__(self, 'deployment_observed_generation', observed)
        object.__setattr__(
            self, 'pod_template_contract_sha256',
            _sha256(self.pod_template_contract_sha256,
                    name='snapshot.pod_template_contract_sha256'))
        strategy = _text(self.deployment_strategy,
                         name='snapshot.deployment_strategy')
        if strategy != 'RollingUpdate':
            raise ValueError(
                'snapshot.deployment_strategy must equal RollingUpdate.')
        object.__setattr__(self, 'deployment_strategy', strategy)
        if _nonnegative_integer(self.deployment_max_surge,
                                name='snapshot.deployment_max_surge') != 0:
            raise ValueError('snapshot.deployment_max_surge must equal 0.')
        if _nonnegative_integer(
                self.deployment_max_unavailable,
                name='snapshot.deployment_max_unavailable') != 1:
            raise ValueError(
                'snapshot.deployment_max_unavailable must equal 1.')
        for field in ('deployment_spec_replicas', 'deployment_status_replicas',
                      'deployment_updated_replicas',
                      'deployment_ready_replicas',
                      'deployment_available_replicas'):
            if _positive_integer(getattr(self, field),
                                 name=f'snapshot.{field}') != 2:
                raise ValueError(f'snapshot.{field} must equal 2.')
        if _nonnegative_integer(
                self.deployment_unavailable_replicas,
                name='snapshot.deployment_unavailable_replicas') != 0:
            raise ValueError(
                'snapshot.deployment_unavailable_replicas must equal 0.')
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at, name='snapshot.observed_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderAuthorityWorkerDeploymentSnapshotV2:
        raw = _closed_object(value,
                             name='authority worker Deployment snapshot V2',
                             keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'deployment_name': self.deployment_name,
            'deployment_uid': self.deployment_uid,
            'deployment_resource_version': self.deployment_resource_version,
            'deployment_generation': self.deployment_generation,
            'deployment_observed_generation':
                self.deployment_observed_generation,
            'pod_template_contract_sha256': self.pod_template_contract_sha256,
            'deployment_strategy': 'RollingUpdate',
            'deployment_max_surge': 0,
            'deployment_max_unavailable': 1,
            'deployment_spec_replicas': 2,
            'deployment_status_replicas': 2,
            'deployment_updated_replicas': 2,
            'deployment_ready_replicas': 2,
            'deployment_available_replicas': 2,
            'deployment_unavailable_replicas': 0,
            'observed_at': self.observed_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerRegistrationSetV2(CanonicalContract):
    """Canonical one-or-two-member Serve035 registration set."""

    version: int
    cohort_identity_sha256: str
    revision: int
    deployment_snapshot: ProviderAuthorityWorkerDeploymentSnapshotV2 | None
    workers: tuple[ProviderAuthorityWorkerRegistrationV2, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'cohort_identity_sha256', 'revision', 'deployment_snapshot',
        'workers'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('registration set version must be integer 2.')
        object.__setattr__(
            self, 'cohort_identity_sha256',
            _sha256(self.cohort_identity_sha256,
                    name='registration_set.cohort_identity_sha256'))
        object.__setattr__(
            self, 'revision',
            _positive_integer(self.revision, name='registration_set.revision'))
        if (type(self.workers) is not tuple or
                not 1 <= len(self.workers) <= 2 or any(
                    type(worker) is not ProviderAuthorityWorkerRegistrationV2
                    for worker in self.workers)):
            raise ValueError('registration set workers must be one or two '
                             'typed registrations.')
        identities = tuple(worker.worker_instance_id for worker in self.workers)
        if identities != tuple(
                sorted(set(identities), key=lambda item: item.bytes)):
            raise ValueError('registration set workers must be sorted by '
                             'distinct UUID bytes.')
        common = {
            (worker.worker.namespace, worker.worker.deployment_name,
             worker.worker.deployment_uid, worker.worker.deployment_generation,
             worker.worker.deployment_observed_generation,
             worker.worker.pod_template_contract_sha256,
             worker.worker.pod_service_account_name,
             worker.worker.service_account_uid, worker.worker.image.sha256,
             worker.worker.artifact_inventory_sha256,
             worker.worker.callable_inventory_sha256,
             worker.worker.handler_allowlist_sha256) for worker in self.workers
        }
        if len(common) != 1:
            raise ValueError('registration workers disagree on immutable '
                             'cohort or Deployment fields.')
        snapshot = self.deployment_snapshot
        if snapshot is not None:
            if type(snapshot
                   ) is not ProviderAuthorityWorkerDeploymentSnapshotV2:
                raise TypeError('registration set snapshot has invalid type.')
            if len(self.workers) != 2:
                raise ValueError('a final snapshot requires two workers.')
            snapshot_observed_at = timestamp_to_datetime(
                snapshot.observed_at, name='snapshot.observed_at')
            for registration in self.workers:
                worker = registration.worker
                if (snapshot.deployment_name != worker.deployment_name or
                        snapshot.deployment_uid != worker.deployment_uid or
                        snapshot.deployment_generation
                        != worker.deployment_generation or
                        snapshot.deployment_observed_generation
                        != worker.deployment_observed_generation or
                        snapshot.pod_template_contract_sha256
                        != worker.pod_template_contract_sha256):
                    raise ValueError('snapshot does not match every worker.')
                if (snapshot_observed_at < timestamp_to_datetime(
                        registration.registered_at,
                        name='registration.registered_at') or
                        snapshot_observed_at < timestamp_to_datetime(
                            worker.observed_at, name='worker.observed_at')):
                    raise ValueError('final snapshot predates worker '
                                     'registration evidence.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerRegistrationSetV2:
        raw = _closed_object(value,
                             name='authority worker registration set V2',
                             keys=cls._KEYS)
        workers = raw['workers']
        if type(workers) is not list:
            raise TypeError('registration_set.workers must be a list.')
        snapshot = raw['deployment_snapshot']
        return cls(
            version=raw['version'],
            cohort_identity_sha256=raw['cohort_identity_sha256'],
            revision=raw['revision'],
            deployment_snapshot=(None if snapshot is None else
                                 ProviderAuthorityWorkerDeploymentSnapshotV2.
                                 from_value(snapshot)),
            workers=tuple(
                ProviderAuthorityWorkerRegistrationV2.from_value(worker)
                for worker in workers))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'cohort_identity_sha256': self.cohort_identity_sha256,
            'revision': self.revision,
            'deployment_snapshot':
                (None if self.deployment_snapshot is None else
                 self.deployment_snapshot.canonical_value()),
            'workers': [worker.canonical_value() for worker in self.workers],
        }

    def validate_registering(self) -> None:
        if self.deployment_snapshot is not None:
            raise ValueError('REGISTERING requires a null Deployment snapshot.')

    def validate_accepted(self) -> None:
        if len(self.workers) != 2 or self.deployment_snapshot is None:
            raise ValueError('accepted membership requires two workers and '
                             'one final Deployment snapshot.')

    def validate_for_cohort(self,
                            cohort: ProviderAuthorityWorkerCohortV2) -> None:
        if type(cohort) is not ProviderAuthorityWorkerCohortV2:
            raise TypeError('registration set cohort has invalid type.')
        if self.cohort_identity_sha256 != cohort.sha256:
            raise ValueError('registration set cohort hash does not match.')
        for registration in self.workers:
            registration.worker.validate_for_cohort(cohort)

    def validate_freshness(self, database_now: datetime.datetime) -> None:
        if (not isinstance(database_now, datetime.datetime) or
                database_now.tzinfo is None or
                database_now.utcoffset() is None):
            raise TypeError('database_now must be timezone aware.')
        normalized_now = database_now.astimezone(_UTC)
        oldest = normalized_now - _WORKER_REGISTRATION_MAX_AGE
        timestamps: list[tuple[str, str]] = []
        for registration in self.workers:
            timestamps.extend(
                (('registered_at', registration.registered_at),
                 ('worker.observed_at', registration.worker.observed_at)))
        if self.deployment_snapshot is not None:
            timestamps.append(
                ('snapshot.observed_at', self.deployment_snapshot.observed_at))
        for name, timestamp in timestamps:
            parsed = timestamp_to_datetime(timestamp, name=name)
            if parsed > normalized_now:
                raise ValueError(f'{name} is in the database future.')
            if parsed < oldest:
                raise ValueError(f'{name} is older than five minutes.')

    def registration_for(
        self, worker_instance_id: uuid.UUID
    ) -> ProviderAuthorityWorkerRegistrationV2 | None:
        identity = _uuid(worker_instance_id, name='worker_instance_id')
        return next((worker for worker in self.workers
                     if worker.worker_instance_id == identity), None)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerStableIdentityProjectionV1(CanonicalContract):
    """ResourceVersion-free stable identity used by lease successors."""

    version: int
    namespace: str
    pod_name: str
    pod_uid: uuid.UUID
    pod_service_account_name: str
    pod_controller_owner: resource_actions.ProviderKubernetesControllerOwnerV1
    replica_set_name: str
    replica_set_uid: str
    replica_set_controller_owner: resource_actions.ProviderKubernetesControllerOwnerV1
    deployment_name: str
    deployment_uid: str
    deployment_generation: int
    deployment_observed_generation: int
    pod_template_contract_sha256: str
    image: resource_actions.ProviderAuthorityWorkerImageV1
    service_account_uid: str
    artifact_inventory_sha256: str
    callable_inventory_sha256: str
    handler_allowlist_sha256: str

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'namespace': self.namespace,
            'pod_name': self.pod_name,
            'pod_uid': str(self.pod_uid),
            'pod_service_account_name': self.pod_service_account_name,
            'pod_controller_owner': self.pod_controller_owner.canonical_value(),
            'replica_set_name': self.replica_set_name,
            'replica_set_uid': self.replica_set_uid,
            'replica_set_controller_owner':
                self.replica_set_controller_owner.canonical_value(),
            'deployment_name': self.deployment_name,
            'deployment_uid': self.deployment_uid,
            'deployment_generation': self.deployment_generation,
            'deployment_observed_generation':
                self.deployment_observed_generation,
            'pod_template_contract_sha256': self.pod_template_contract_sha256,
            'image': self.image.canonical_value(),
            'service_account_uid': self.service_account_uid,
            'artifact_inventory_sha256': self.artifact_inventory_sha256,
            'callable_inventory_sha256': self.callable_inventory_sha256,
            'handler_allowlist_sha256': self.handler_allowlist_sha256,
        }


def project_stable_worker_identity_v1(
    identity: ProviderAuthorityWorkerIdentityV2,
) -> ProviderAuthorityWorkerStableIdentityProjectionV1:
    """Project the fields which may not drift across V2 lease renewal."""

    if type(identity) is not ProviderAuthorityWorkerIdentityV2:
        raise TypeError('stable projection requires a V2 worker identity.')
    return ProviderAuthorityWorkerStableIdentityProjectionV1(
        version=1,
        namespace=identity.namespace,
        pod_name=identity.pod_name,
        pod_uid=identity.pod_uid,
        pod_service_account_name=identity.pod_service_account_name,
        pod_controller_owner=identity.pod_controller_owner,
        replica_set_name=identity.replica_set_name,
        replica_set_uid=identity.replica_set_uid,
        replica_set_controller_owner=identity.replica_set_controller_owner,
        deployment_name=identity.deployment_name,
        deployment_uid=identity.deployment_uid,
        deployment_generation=identity.deployment_generation,
        deployment_observed_generation=identity.deployment_observed_generation,
        pod_template_contract_sha256=identity.pod_template_contract_sha256,
        image=identity.image,
        service_account_uid=identity.service_account_uid,
        artifact_inventory_sha256=identity.artifact_inventory_sha256,
        callable_inventory_sha256=identity.callable_inventory_sha256,
        handler_allowlist_sha256=identity.handler_allowlist_sha256)


class WorkerRegistrationLeaseState(str, enum.Enum):
    ACTIVE = 'ACTIVE'
    REVOKED = 'REVOKED'


class WorkerRegistrationLeaseOperation(str, enum.Enum):
    INSERT = 'INSERT'
    RENEW = 'RENEW'
    REVOKE = 'REVOKE'


class WorkerRegistrationLeaseRevocationReason(str, enum.Enum):
    STALE_HANDOFF = 'STALE_HANDOFF'
    CANDIDATE_ABANDONED = 'CANDIDATE_ABANDONED'
    COHORT_COLD_RECOVERY = 'COHORT_COLD_RECOVERY'
    COHORT_REMOVAL = 'COHORT_REMOVAL'


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerLeaseV1(CanonicalContract):
    """Strict typed projection of one Serve035 registration lease row."""

    version: int
    worker_instance_id: uuid.UUID
    generation: int
    state: WorkerRegistrationLeaseState
    renewal_registration: ProviderAuthorityWorkerRegistrationV2
    renewal_registration_sha256: str
    renewed_at: str
    expires_at: str
    revoked_at: str | None
    revocation_reason: WorkerRegistrationLeaseRevocationReason | None
    revocation_owner_id: uuid.UUID | None
    last_operation_id: uuid.UUID
    last_operation_kind: WorkerRegistrationLeaseOperation
    revision: int

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'worker_instance_id', 'generation', 'state',
        'renewal_registration', 'renewal_registration_sha256', 'renewed_at',
        'expires_at', 'revoked_at', 'revocation_reason', 'revocation_owner_id',
        'last_operation_id', 'last_operation_kind', 'revision'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('worker lease version must be integer 1.')
        identity = _uuid(self.worker_instance_id,
                         name='lease.worker_instance_id')
        object.__setattr__(self, 'worker_instance_id', identity)
        object.__setattr__(
            self, 'generation',
            _positive_integer(self.generation, name='lease.generation'))
        state = _enum_value(WorkerRegistrationLeaseState,
                            self.state,
                            name='lease.state')
        object.__setattr__(self, 'state', state)
        if type(self.renewal_registration
               ) is not ProviderAuthorityWorkerRegistrationV2:
            raise TypeError('lease registration has invalid type.')
        if identity != self.renewal_registration.worker_instance_id:
            raise ValueError('lease identity differs from its registration.')
        digest = _sha256(self.renewal_registration_sha256,
                         name='lease.renewal_registration_sha256')
        if digest != self.renewal_registration.sha256:
            raise ValueError('lease registration digest does not match.')
        object.__setattr__(self, 'renewal_registration_sha256', digest)
        renewed = _timestamp(self.renewed_at, name='lease.renewed_at')
        expires = _timestamp(self.expires_at, name='lease.expires_at')
        renewed_dt = timestamp_to_datetime(renewed, name='lease.renewed_at')
        expires_dt = timestamp_to_datetime(expires, name='lease.expires_at')
        if expires_dt != renewed_dt + datetime.timedelta(
                seconds=RESOURCE_ACTION_WORKER_REGISTRATION_LEASE_TTL_SECONDS_V1
        ):
            raise ValueError('lease expiry must be exactly 60 seconds.')
        if self.renewal_registration.registered_at != renewed:
            raise ValueError('lease renewal and registration times differ.')
        object.__setattr__(self, 'renewed_at', renewed)
        object.__setattr__(self, 'expires_at', expires)
        operation_id = _uuid(self.last_operation_id,
                             name='lease.last_operation_id')
        object.__setattr__(self, 'last_operation_id', operation_id)
        operation = _enum_value(WorkerRegistrationLeaseOperation,
                                self.last_operation_kind,
                                name='lease.last_operation_kind')
        object.__setattr__(self, 'last_operation_kind', operation)
        revision = _positive_integer(self.revision, name='lease.revision')
        object.__setattr__(self, 'revision', revision)
        reason = self.revocation_reason
        if reason is not None:
            reason = _enum_value(WorkerRegistrationLeaseRevocationReason,
                                 reason,
                                 name='lease.revocation_reason')
            object.__setattr__(self, 'revocation_reason', reason)
        owner = self.revocation_owner_id
        if owner is not None:
            owner = _uuid(owner, name='lease.revocation_owner_id')
            object.__setattr__(self, 'revocation_owner_id', owner)
        if state is WorkerRegistrationLeaseState.ACTIVE:
            if (revision != self.generation or self.revoked_at is not None or
                    reason is not None or owner is not None or
                (self.generation == 1 and
                 operation is not WorkerRegistrationLeaseOperation.INSERT) or
                (self.generation > 1 and
                 operation is not WorkerRegistrationLeaseOperation.RENEW)):
                raise ValueError('ACTIVE lease has an invalid closed shape.')
        else:
            revoked = _timestamp(self.revoked_at, name='lease.revoked_at')
            if (revision != self.generation + 1 or reason is None or
                    operation is not WorkerRegistrationLeaseOperation.REVOKE or
                    timestamp_to_datetime(
                        revoked, name='lease.revoked_at') < renewed_dt or
                (reason
                 is WorkerRegistrationLeaseRevocationReason.COHORT_REMOVAL and
                 owner is not None) or
                (reason
                 is not WorkerRegistrationLeaseRevocationReason.COHORT_REMOVAL
                 and owner is None)):
                raise ValueError('REVOKED lease has an invalid closed shape.')
            object.__setattr__(self, 'revoked_at', revoked)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityWorkerLeaseV1:
        raw = _closed_object(value,
                             name='authority worker lease V1',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            worker_instance_id=raw['worker_instance_id'],
            generation=raw['generation'],
            state=raw['state'],
            renewal_registration=ProviderAuthorityWorkerRegistrationV2.
            from_value(raw['renewal_registration']),
            renewal_registration_sha256=raw['renewal_registration_sha256'],
            renewed_at=raw['renewed_at'],
            expires_at=raw['expires_at'],
            revoked_at=raw['revoked_at'],
            revocation_reason=raw['revocation_reason'],
            revocation_owner_id=raw['revocation_owner_id'],
            last_operation_id=raw['last_operation_id'],
            last_operation_kind=raw['last_operation_kind'],
            revision=raw['revision'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'worker_instance_id': str(self.worker_instance_id),
            'generation': self.generation,
            'state': self.state.value,
            'renewal_registration': self.renewal_registration.canonical_value(),
            'renewal_registration_sha256': self.renewal_registration_sha256,
            'renewed_at': self.renewed_at,
            'expires_at': self.expires_at,
            'revoked_at': self.revoked_at,
            'revocation_reason': (None if self.revocation_reason is None else
                                  self.revocation_reason.value),
            'revocation_owner_id': (None if self.revocation_owner_id is None
                                    else str(self.revocation_owner_id)),
            'last_operation_id': str(self.last_operation_id),
            'last_operation_kind': self.last_operation_kind.value,
            'revision': self.revision,
        }

    def is_fresh(self, database_now: datetime.datetime) -> bool:
        if self.state is not WorkerRegistrationLeaseState.ACTIVE:
            return False
        return timestamp_to_datetime(
            self.expires_at,
            name='lease.expires_at') > database_now.astimezone(_UTC)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerAcceptedMembershipV2(CanonicalContract):
    """One accepted registration paired with a structurally active lease.

    Time freshness is deliberately not structural: the transactional proof
    builder must compare ``lease.expires_at`` with its PostgreSQL clock read.
    """

    version: int
    registration: ProviderAuthorityWorkerRegistrationV2
    registration_set_revision: int
    registration_set_sha256: str
    lease: ProviderAuthorityWorkerLeaseV1

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('accepted membership version must be integer 2.')
        if type(self.registration) is not ProviderAuthorityWorkerRegistrationV2:
            raise TypeError('accepted registration has invalid type.')
        object.__setattr__(
            self, 'registration_set_revision',
            _positive_integer(self.registration_set_revision,
                              name='membership.registration_set_revision'))
        object.__setattr__(
            self, 'registration_set_sha256',
            _sha256(self.registration_set_sha256,
                    name='membership.registration_set_sha256'))
        if type(self.lease) is not ProviderAuthorityWorkerLeaseV1:
            raise TypeError('accepted membership lease has invalid type.')
        if self.lease.state is not WorkerRegistrationLeaseState.ACTIVE:
            raise ValueError('accepted membership lease must be ACTIVE.')
        if (self.registration.worker_instance_id
                != self.lease.worker_instance_id or
                project_stable_worker_identity_v1(
                    self.registration.worker).canonical_bytes
                != project_stable_worker_identity_v1(
                    self.lease.renewal_registration.worker).canonical_bytes):
            raise ValueError('accepted registration and lease disagree.')

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'registration': self.registration.canonical_value(),
            'registration_set_revision': self.registration_set_revision,
            'registration_set_sha256': self.registration_set_sha256,
            'lease': self.lease.canonical_value(),
        }


class PodUidAbsenceDisposition(str, enum.Enum):
    NOT_FOUND = 'not_found'
    SAME_NAME_DIFFERENT_UID = 'same_name_different_uid'


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerPodUidAbsenceProofV1(CanonicalContract):
    """UID-qualified monotonic absence proof for one exact Pod."""

    version: int
    disposition: PodUidAbsenceDisposition
    namespace: str
    pod_name: str
    expected_absent_pod_uid: uuid.UUID
    current_pod_uid: uuid.UUID | None
    current_pod_resource_version: str | None
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'disposition', 'namespace', 'pod_name',
        'expected_absent_pod_uid', 'current_pod_uid',
        'current_pod_resource_version', 'observed_at'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('absence proof version must be integer 1.')
        disposition = _enum_value(PodUidAbsenceDisposition,
                                  self.disposition,
                                  name='absence.disposition')
        object.__setattr__(self, 'disposition', disposition)
        for field in ('namespace', 'pod_name'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'absence.{field}',
                      maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        expected = _uuid(self.expected_absent_pod_uid,
                         name='absence.expected_absent_pod_uid')
        object.__setattr__(self, 'expected_absent_pod_uid', expected)
        if disposition is PodUidAbsenceDisposition.NOT_FOUND:
            if (self.current_pod_uid is not None or
                    self.current_pod_resource_version is not None):
                raise ValueError('not-found proof has a current Pod.')
        else:
            current = _uuid(self.current_pod_uid,
                            name='absence.current_pod_uid')
            if current == expected:
                raise ValueError('different-UID proof retained the same UID.')
            object.__setattr__(self, 'current_pod_uid', current)
            object.__setattr__(
                self, 'current_pod_resource_version',
                _text(self.current_pod_resource_version,
                      name='absence.current_pod_resource_version'))
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at, name='absence.observed_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderAuthorityWorkerPodUidAbsenceProofV1:
        raw = _closed_object(value,
                             name='authority worker Pod UID absence proof',
                             keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'disposition': self.disposition.value,
            'namespace': self.namespace,
            'pod_name': self.pod_name,
            'expected_absent_pod_uid': str(self.expected_absent_pod_uid),
            'current_pod_uid': (None if self.current_pod_uid is None else str(
                self.current_pod_uid)),
            'current_pod_resource_version': self.current_pod_resource_version,
            'observed_at': self.observed_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerRequestClaimFenceV1(CanonicalContract):
    """One replayably queued request claim fenced during membership change."""

    request_id: uuid.UUID
    execution_generation: int
    claim_token_sha256: str
    prior_lease_expires_at: str
    fenced_delivery_state: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'request_id', 'execution_generation', 'claim_token_sha256',
        'prior_lease_expires_at', 'fenced_delivery_state'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'request_id',
            _uuid(self.request_id, name='claim_fence.request_id'))
        object.__setattr__(
            self, 'execution_generation',
            _positive_integer(self.execution_generation,
                              name='claim_fence.execution_generation'))
        object.__setattr__(
            self, 'claim_token_sha256',
            _sha256(self.claim_token_sha256,
                    name='claim_fence.claim_token_sha256'))
        object.__setattr__(
            self, 'prior_lease_expires_at',
            _timestamp(self.prior_lease_expires_at,
                       name='claim_fence.prior_lease_expires_at'))
        if self.fenced_delivery_state != 'queued':
            raise ValueError('claim fence delivery state must be queued.')

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderAuthorityWorkerRequestClaimFenceV1:
        return cls(**_closed_object(
            value, name='worker request claim fence', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return {
            'request_id': str(self.request_id),
            'execution_generation': self.execution_generation,
            'claim_token_sha256': self.claim_token_sha256,
            'prior_lease_expires_at': self.prior_lease_expires_at,
            'fenced_delivery_state': 'queued',
        }


def _validate_claim_fences(
    claims: tuple[ProviderAuthorityWorkerRequestClaimFenceV1, ...],
    *,
    name: str,
) -> tuple[ProviderAuthorityWorkerRequestClaimFenceV1, ...]:
    if type(claims) is not tuple or any(
            type(claim) is not ProviderAuthorityWorkerRequestClaimFenceV1
            for claim in claims):
        raise TypeError(f'{name} must be a tuple of request claim fences.')
    if len(claims) > RESOURCE_ACTION_WORKER_FENCE_MAX_REQUEST_CLAIMS_V1:
        raise ValueError(f'{name} has too many request claims.')
    identities = tuple(claim.request_id for claim in claims)
    if identities != tuple(sorted(set(identities),
                                  key=lambda item: item.bytes)):
        raise ValueError(f'{name} must be sorted by distinct request UUIDs.')
    encoded = canonical_json_bytes(
        [claim.canonical_value() for claim in claims])
    if len(encoded
          ) > RESOURCE_ACTION_WORKER_FENCE_MAX_REQUEST_CLAIMS_JSON_BYTES_V1:
        raise ValueError(f'{name} exceeds its canonical byte bound.')
    return claims


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerStaleAuthorityFenceV1(CanonicalContract):
    """Bounded stale-member execution fence retained by one handoff chain."""

    version: int
    origin_revoking_handoff_id: uuid.UUID
    stale_worker_instance_id: uuid.UUID
    stale_lease_generation: int
    prior_stale_lease_revision: int
    revoked_stale_lease_revision: int
    request_claims: tuple[ProviderAuthorityWorkerRequestClaimFenceV1, ...]
    fenced_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'origin_revoking_handoff_id', 'stale_worker_instance_id',
        'stale_lease_generation', 'prior_stale_lease_revision',
        'revoked_stale_lease_revision', 'request_claims', 'fenced_at'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('stale fence version must be integer 1.')
        for field in ('origin_revoking_handoff_id', 'stale_worker_instance_id'):
            object.__setattr__(
                self, field,
                _uuid(getattr(self, field), name=f'stale_fence.{field}'))
        object.__setattr__(
            self, 'stale_lease_generation',
            _positive_integer(self.stale_lease_generation,
                              name='stale_fence.stale_lease_generation'))
        prior = _positive_integer(self.prior_stale_lease_revision,
                                  name='stale_fence.prior_stale_lease_revision')
        terminal = _positive_integer(
            self.revoked_stale_lease_revision,
            name='stale_fence.revoked_stale_lease_revision')
        if terminal != prior + 1:
            raise ValueError('stale lease revision must advance exactly once.')
        object.__setattr__(self, 'prior_stale_lease_revision', prior)
        object.__setattr__(self, 'revoked_stale_lease_revision', terminal)
        _validate_claim_fences(self.request_claims,
                               name='stale_fence.request_claims')
        object.__setattr__(
            self, 'fenced_at',
            _timestamp(self.fenced_at, name='stale_fence.fenced_at'))
        if len(self.canonical_bytes
              ) > RESOURCE_ACTION_WORKER_FENCE_MAX_CANONICAL_BYTES_V1:
            raise ValueError('stale authority fence exceeds 30,720 bytes.')

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderAuthorityWorkerStaleAuthorityFenceV1:
        raw = _closed_object(value,
                             name='stale authority fence',
                             keys=cls._KEYS)
        claims = raw['request_claims']
        if type(claims) is not list:
            raise TypeError('stale_fence.request_claims must be a list.')
        return cls(
            version=raw['version'],
            origin_revoking_handoff_id=raw['origin_revoking_handoff_id'],
            stale_worker_instance_id=raw['stale_worker_instance_id'],
            stale_lease_generation=raw['stale_lease_generation'],
            prior_stale_lease_revision=raw['prior_stale_lease_revision'],
            revoked_stale_lease_revision=raw['revoked_stale_lease_revision'],
            request_claims=tuple(
                ProviderAuthorityWorkerRequestClaimFenceV1.from_value(claim)
                for claim in claims),
            fenced_at=raw['fenced_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'origin_revoking_handoff_id': str(self.origin_revoking_handoff_id),
            'stale_worker_instance_id': str(self.stale_worker_instance_id),
            'stale_lease_generation': self.stale_lease_generation,
            'prior_stale_lease_revision': self.prior_stale_lease_revision,
            'revoked_stale_lease_revision': self.revoked_stale_lease_revision,
            'request_claims': [
                claim.canonical_value() for claim in self.request_claims
            ],
            'fenced_at': self.fenced_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerColdRecoveryFenceV1(CanonicalContract):
    """Bounded old-member lease/claim fence retained by cold recovery."""

    version: int
    recovery_id: uuid.UUID
    worker_instance_id: uuid.UUID
    pod_uid: uuid.UUID
    prior_lease_state: WorkerRegistrationLeaseState
    lease_generation: int
    prior_lease_revision: int
    terminal_lease_revision: int
    preserved_revocation_reason: WorkerRegistrationLeaseRevocationReason | None
    preserved_revocation_owner_id: uuid.UUID | None
    request_claims: tuple[ProviderAuthorityWorkerRequestClaimFenceV1, ...]
    fenced_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'recovery_id', 'worker_instance_id', 'pod_uid',
        'prior_lease_state', 'lease_generation', 'prior_lease_revision',
        'terminal_lease_revision', 'preserved_revocation_reason',
        'preserved_revocation_owner_id', 'request_claims', 'fenced_at'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('cold recovery fence version must be integer 1.')
        for field in ('recovery_id', 'worker_instance_id', 'pod_uid'):
            object.__setattr__(
                self, field,
                _uuid(getattr(self, field), name=f'cold_fence.{field}'))
        if self.worker_instance_id != self.pod_uid:
            raise ValueError('cold fence worker instance must equal Pod UID.')
        state = _enum_value(WorkerRegistrationLeaseState,
                            self.prior_lease_state,
                            name='cold_fence.prior_lease_state')
        object.__setattr__(self, 'prior_lease_state', state)
        object.__setattr__(
            self, 'lease_generation',
            _positive_integer(self.lease_generation,
                              name='cold_fence.lease_generation'))
        prior = _positive_integer(self.prior_lease_revision,
                                  name='cold_fence.prior_lease_revision')
        terminal = _positive_integer(self.terminal_lease_revision,
                                     name='cold_fence.terminal_lease_revision')
        reason = self.preserved_revocation_reason
        owner = self.preserved_revocation_owner_id
        if state is WorkerRegistrationLeaseState.ACTIVE:
            if terminal != prior + 1 or reason is not None or owner is not None:
                raise ValueError(
                    'ACTIVE cold fence has invalid terminal shape.')
        else:
            if terminal != prior:
                raise ValueError('REVOKED cold fence cannot advance revision.')
            reason = _enum_value(WorkerRegistrationLeaseRevocationReason,
                                 reason,
                                 name='cold_fence.preserved_revocation_reason')
            if reason is not WorkerRegistrationLeaseRevocationReason.STALE_HANDOFF:
                raise ValueError('cold recovery preserves only stale handoff.')
            owner = _uuid(owner,
                          name='cold_fence.preserved_revocation_owner_id')
            object.__setattr__(self, 'preserved_revocation_reason', reason)
            object.__setattr__(self, 'preserved_revocation_owner_id', owner)
        object.__setattr__(self, 'prior_lease_revision', prior)
        object.__setattr__(self, 'terminal_lease_revision', terminal)
        _validate_claim_fences(self.request_claims,
                               name='cold_fence.request_claims')
        if (state is WorkerRegistrationLeaseState.REVOKED and
                self.request_claims):
            raise ValueError('preserved revoked fence must have no claims.')
        object.__setattr__(
            self, 'fenced_at',
            _timestamp(self.fenced_at, name='cold_fence.fenced_at'))
        if len(self.canonical_bytes
              ) > RESOURCE_ACTION_WORKER_FENCE_MAX_CANONICAL_BYTES_V1:
            raise ValueError('cold recovery fence exceeds 30,720 bytes.')

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderAuthorityWorkerColdRecoveryFenceV1:
        raw = _closed_object(value, name='cold recovery fence', keys=cls._KEYS)
        claims = raw['request_claims']
        if type(claims) is not list:
            raise TypeError('cold_fence.request_claims must be a list.')
        return cls(
            version=raw['version'],
            recovery_id=raw['recovery_id'],
            worker_instance_id=raw['worker_instance_id'],
            pod_uid=raw['pod_uid'],
            prior_lease_state=raw['prior_lease_state'],
            lease_generation=raw['lease_generation'],
            prior_lease_revision=raw['prior_lease_revision'],
            terminal_lease_revision=raw['terminal_lease_revision'],
            preserved_revocation_reason=raw['preserved_revocation_reason'],
            preserved_revocation_owner_id=raw['preserved_revocation_owner_id'],
            request_claims=tuple(
                ProviderAuthorityWorkerRequestClaimFenceV1.from_value(claim)
                for claim in claims),
            fenced_at=raw['fenced_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'recovery_id': str(self.recovery_id),
            'worker_instance_id': str(self.worker_instance_id),
            'pod_uid': str(self.pod_uid),
            'prior_lease_state': self.prior_lease_state.value,
            'lease_generation': self.lease_generation,
            'prior_lease_revision': self.prior_lease_revision,
            'terminal_lease_revision': self.terminal_lease_revision,
            'preserved_revocation_reason':
                (None if self.preserved_revocation_reason is None else
                 self.preserved_revocation_reason.value),
            'preserved_revocation_owner_id':
                (None if self.preserved_revocation_owner_id is None else str(
                    self.preserved_revocation_owner_id)),
            'request_claims': [
                claim.canonical_value() for claim in self.request_claims
            ],
            'fenced_at': self.fenced_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerCandidateZeroEffectProofV1(CanonicalContract):
    """Exact all-zero effect inventory for an absent handoff candidate."""

    version: int
    candidate_worker_instance_id: uuid.UUID
    candidate_pod_uid: uuid.UUID
    accepted_membership_count: int
    live_request_claim_count: int
    attempt_attestation_count: int
    provider_progress_count: int
    provider_operation_count: int
    provider_effect_count: int
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'candidate_worker_instance_id', 'candidate_pod_uid',
        'accepted_membership_count', 'live_request_claim_count',
        'attempt_attestation_count', 'provider_progress_count',
        'provider_operation_count', 'provider_effect_count', 'observed_at'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('zero-effect proof version must be integer 1.')
        instance = _uuid(self.candidate_worker_instance_id,
                         name='zero_effect.candidate_worker_instance_id')
        pod = _uuid(self.candidate_pod_uid,
                    name='zero_effect.candidate_pod_uid')
        if instance != pod:
            raise ValueError('zero-effect worker instance must equal Pod UID.')
        object.__setattr__(self, 'candidate_worker_instance_id', instance)
        object.__setattr__(self, 'candidate_pod_uid', pod)
        for field in ('accepted_membership_count', 'live_request_claim_count',
                      'attempt_attestation_count', 'provider_progress_count',
                      'provider_operation_count', 'provider_effect_count'):
            if _nonnegative_integer(getattr(self, field),
                                    name=f'zero_effect.{field}') != 0:
                raise ValueError(f'zero_effect.{field} must equal zero.')
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at, name='zero_effect.observed_at'))

    @classmethod
    def from_value(
            cls,
            value: Any) -> ProviderAuthorityWorkerCandidateZeroEffectProofV1:
        return cls(**_closed_object(
            value, name='candidate zero-effect proof', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'candidate_worker_instance_id': str(
                self.candidate_worker_instance_id),
            'candidate_pod_uid': str(self.candidate_pod_uid),
            'accepted_membership_count': 0,
            'live_request_claim_count': 0,
            'attempt_attestation_count': 0,
            'provider_progress_count': 0,
            'provider_operation_count': 0,
            'provider_effect_count': 0,
            'observed_at': self.observed_at,
        }


class ApprovedRole(str, enum.Enum):
    API = 'api'
    ORDINARY_EXECUTOR = 'ordinary-executor'
    CONTROLLER = 'controller'


@dataclasses.dataclass(frozen=True)
class ApprovedRoleImageV1(CanonicalContract):
    """One exact ordinary-role image approved by the trust policy."""

    role: ApprovedRole
    oci_manifest_digest: str
    source_commit: str
    artifact_inventory_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'role', 'oci_manifest_digest', 'source_commit',
        'artifact_inventory_sha256'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'role',
            _enum_value(ApprovedRole, self.role, name='approved_image.role'))
        object.__setattr__(
            self, 'oci_manifest_digest',
            _sha256_digest(self.oci_manifest_digest,
                           name='approved_image.oci_manifest_digest'))
        object.__setattr__(
            self, 'source_commit',
            _commit(self.source_commit, name='approved_image.source_commit'))
        object.__setattr__(
            self, 'artifact_inventory_sha256',
            _sha256(self.artifact_inventory_sha256,
                    name='approved_image.artifact_inventory_sha256'))

    @classmethod
    def from_value(cls, value: Any) -> ApprovedRoleImageV1:
        return cls(
            **_closed_object(value, name='approved role image', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return {
            'role': self.role.value,
            'oci_manifest_digest': self.oci_manifest_digest,
            'source_commit': self.source_commit,
            'artifact_inventory_sha256': self.artifact_inventory_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ApprovedAuthorityCohortArtifactV1(CanonicalContract):
    """One exact immutable V2 authority cohort approved by policy."""

    cohort_id: str
    oci_manifest_digest: str
    oci_config_digest: str
    manifest_sha256: str
    qualification_artifact_sha256: str
    pod_template_contract_sha256: str
    pod_template_binding_sha256: str
    artifact_inventory_sha256: str
    callable_inventory_sha256: str
    handler_allowlist_sha256: str
    claim_contract: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'cohort_id', 'oci_manifest_digest', 'oci_config_digest',
        'manifest_sha256', 'qualification_artifact_sha256',
        'pod_template_contract_sha256', 'pod_template_binding_sha256',
        'artifact_inventory_sha256', 'callable_inventory_sha256',
        'handler_allowlist_sha256', 'claim_contract'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'cohort_id',
            _text(self.cohort_id, name='approved_cohort.cohort_id'))
        for field in ('oci_manifest_digest', 'oci_config_digest'):
            object.__setattr__(
                self, field,
                _sha256_digest(getattr(self, field),
                               name=f'approved_cohort.{field}'))
        for field in ('manifest_sha256', 'qualification_artifact_sha256',
                      'pod_template_contract_sha256',
                      'pod_template_binding_sha256',
                      'artifact_inventory_sha256', 'callable_inventory_sha256',
                      'handler_allowlist_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field), name=f'approved_cohort.{field}'))
        if (type(self.claim_contract) is not str or
                self.claim_contract != 'frozen_action_cohort_join_v2'):
            raise ValueError('approved cohort claim contract must be V2.')

    @classmethod
    def from_value(cls, value: Any) -> ApprovedAuthorityCohortArtifactV1:
        return cls(**_closed_object(
            value, name='approved authority cohort', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return {
            'cohort_id': self.cohort_id,
            'oci_manifest_digest': self.oci_manifest_digest,
            'oci_config_digest': self.oci_config_digest,
            'manifest_sha256': self.manifest_sha256,
            'qualification_artifact_sha256': self.qualification_artifact_sha256,
            'pod_template_contract_sha256': self.pod_template_contract_sha256,
            'pod_template_binding_sha256': self.pod_template_binding_sha256,
            'artifact_inventory_sha256': self.artifact_inventory_sha256,
            'callable_inventory_sha256': self.callable_inventory_sha256,
            'handler_allowlist_sha256': self.handler_allowlist_sha256,
            'claim_contract': 'frozen_action_cohort_join_v2',
        }


@dataclasses.dataclass(frozen=True)
class ResourceActionQualificationPolicyV1(CanonicalContract):
    """Server-owned closed M4 qualification trust file."""

    version: int
    api_requests_head: str
    serve_head: str
    global_user_state_head: str
    candidate_minimum_seconds: int
    minimum_clean_launches: int
    minimum_clean_downs: int
    approved_role_images: tuple[ApprovedRoleImageV1, ...]
    approved_cohorts: tuple[ApprovedAuthorityCohortArtifactV1, ...]
    crash_canary_inventory_contract: str
    required_crash_canary_inventory_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'api_requests_head', 'serve_head', 'global_user_state_head',
        'candidate_minimum_seconds', 'minimum_clean_launches',
        'minimum_clean_downs', 'approved_role_images', 'approved_cohorts',
        'crash_canary_inventory_contract',
        'required_crash_canary_inventory_sha256'
    })

    def __post_init__(self) -> None:
        fixed = ((self.version, 1, 'version'),
                 (self.candidate_minimum_seconds, 86_400,
                  'candidate_minimum_seconds'), (self.minimum_clean_launches,
                                                 100, 'minimum_clean_launches'),
                 (self.minimum_clean_downs, 100, 'minimum_clean_downs'))
        for actual, expected, name in fixed:
            if type(actual) is not int or actual != expected:
                raise ValueError(f'qualification policy {name} must equal '
                                 f'{expected}.')
        if (type(self.api_requests_head) is not str or
                type(self.serve_head) is not str or
                type(self.global_user_state_head) is not str or
                self.api_requests_head != '007' or self.serve_head != '035' or
                self.global_user_state_head != '028'):
            raise ValueError('qualification policy schema heads are not exact.')
        if (type(self.approved_role_images) is not tuple or
                tuple(image.role for image in self.approved_role_images)
                != (ApprovedRole.API, ApprovedRole.ORDINARY_EXECUTOR,
                    ApprovedRole.CONTROLLER) or any(
                        type(image) is not ApprovedRoleImageV1
                        for image in self.approved_role_images)):
            raise ValueError('approved role images must contain the exact '
                             'three-role order.')
        if (type(self.approved_cohorts) is not tuple or
                not 1 <= len(self.approved_cohorts) <= 16 or any(
                    type(cohort) is not ApprovedAuthorityCohortArtifactV1
                    for cohort in self.approved_cohorts)):
            raise ValueError('approved cohorts must contain 1..16 entries.')
        cohort_ids = tuple(cohort.cohort_id for cohort in self.approved_cohorts)
        if cohort_ids != tuple(sorted(set(cohort_ids))):
            raise ValueError('approved cohorts must be sorted and distinct.')
        if (type(self.crash_canary_inventory_contract) is not str or
                self.crash_canary_inventory_contract
                != 'resource_action_crash_canary_inventory_v1'):
            raise ValueError('crash inventory contract is unsupported.')
        object.__setattr__(
            self, 'required_crash_canary_inventory_sha256',
            _sha256(self.required_crash_canary_inventory_sha256,
                    name='policy.required_crash_canary_inventory_sha256'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ResourceActionQualificationPolicyV1:
        raw = _closed_object(value,
                             name='resource-action qualification policy',
                             keys=cls._KEYS)
        role_images = raw['approved_role_images']
        cohorts = raw['approved_cohorts']
        if type(role_images) is not list or type(cohorts) is not list:
            raise TypeError('qualification policy inventories must be lists.')
        return cls(
            version=raw['version'],
            api_requests_head=raw['api_requests_head'],
            serve_head=raw['serve_head'],
            global_user_state_head=raw['global_user_state_head'],
            candidate_minimum_seconds=raw['candidate_minimum_seconds'],
            minimum_clean_launches=raw['minimum_clean_launches'],
            minimum_clean_downs=raw['minimum_clean_downs'],
            approved_role_images=tuple(
                ApprovedRoleImageV1.from_value(item) for item in role_images),
            approved_cohorts=tuple(
                ApprovedAuthorityCohortArtifactV1.from_value(item)
                for item in cohorts),
            crash_canary_inventory_contract=raw[
                'crash_canary_inventory_contract'],
            required_crash_canary_inventory_sha256=raw[
                'required_crash_canary_inventory_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'api_requests_head': '007',
            'serve_head': '035',
            'global_user_state_head': '028',
            'candidate_minimum_seconds': 86_400,
            'minimum_clean_launches': 100,
            'minimum_clean_downs': 100,
            'approved_role_images': [
                item.canonical_value() for item in self.approved_role_images
            ],
            'approved_cohorts': [
                item.canonical_value() for item in self.approved_cohorts
            ],
            'crash_canary_inventory_contract': 'resource_action_crash_canary_inventory_v1',
            'required_crash_canary_inventory_sha256':
                self.required_crash_canary_inventory_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ResourceActionQualificationPolicyRefV1(CanonicalContract):
    """Exact projected path, size, and digest for the trust file."""

    path: str
    byte_size: int
    policy_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'path', 'byte_size', 'sha256'})

    def __post_init__(self) -> None:
        if (type(self.path) is not str or
                self.path != RESOURCE_ACTION_QUALIFICATION_POLICY_PATH_V1):
            raise ValueError('qualification policy path is not exact.')
        size = _positive_integer(self.byte_size, name='policy_ref.byte_size')
        if size > _MAX_CANONICAL_BYTES:
            raise ValueError('qualification policy exceeds 65,536 bytes.')
        object.__setattr__(self, 'byte_size', size)
        object.__setattr__(
            self, 'policy_sha256',
            _sha256(self.policy_sha256, name='policy_ref.sha256'))

    @classmethod
    def from_value(cls, value: Any) -> ResourceActionQualificationPolicyRefV1:
        raw = _closed_object(value,
                             name='qualification policy reference',
                             keys=cls._KEYS)
        return cls(path=raw['path'],
                   byte_size=raw['byte_size'],
                   policy_sha256=raw['sha256'])

    @classmethod
    def for_policy(
        cls, policy: ResourceActionQualificationPolicyV1
    ) -> ResourceActionQualificationPolicyRefV1:
        if type(policy) is not ResourceActionQualificationPolicyV1:
            raise TypeError('policy reference requires a typed policy.')
        return cls(path=RESOURCE_ACTION_QUALIFICATION_POLICY_PATH_V1,
                   byte_size=len(policy.canonical_bytes),
                   policy_sha256=policy.sha256)

    @classmethod
    def for_policy_v2(
        cls, policy: ResourceActionQualificationPolicyV2
    ) -> ResourceActionQualificationPolicyRefV1:
        """Build the byte-identical fixed-path reference for a V2 policy."""

        if type(policy) is not ResourceActionQualificationPolicyV2:
            raise TypeError('V2 policy reference requires a typed policy.')
        return cls(path=RESOURCE_ACTION_QUALIFICATION_POLICY_PATH_V1,
                   byte_size=len(policy.canonical_bytes),
                   policy_sha256=policy.sha256)

    def canonical_value(self) -> JsonObject:
        return {
            'path': self.path,
            'byte_size': self.byte_size,
            'sha256': self.policy_sha256,
        }


@dataclasses.dataclass(frozen=True)
class QualifiedResourceActionRolePodTemplateV1(CanonicalContract):
    """Exact normalized raw Deployment.spec.template JSON projection."""

    version: int
    contract: str
    template_json: str

    _CONTRACT: ClassVar[str] = (
        'qualified_resource_action_role_pod_template_v1')
    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'contract', 'template_json'})

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError(
                'qualified Pod-template version must be integer 1.')
        if type(self.contract) is not str or self.contract != self._CONTRACT:
            raise ValueError('qualified Pod-template contract is unsupported.')
        template = _parse_strict_canonical_json_object(
            self.template_json, name='qualified Pod-template JSON')
        normalized = _normalize_ordinary_role_pod_template(template)
        normalized_json = canonical_json_bytes(normalized).decode('utf-8')
        if self.template_json != normalized_json:
            raise ValueError('qualified Pod-template JSON is not the exact '
                             'normalized projection.')
        _ = self.canonical_bytes

    @classmethod
    def from_deployment_template_value(
            cls, value: Any) -> QualifiedResourceActionRolePodTemplateV1:
        """Build from the raw JSON value at Deployment.spec.template."""

        normalized = _normalize_ordinary_role_pod_template(value)
        return cls(
            version=1,
            contract=cls._CONTRACT,
            template_json=canonical_json_bytes(normalized).decode('utf-8'))

    @classmethod
    def from_value(cls, value: Any) -> QualifiedResourceActionRolePodTemplateV1:
        return cls(**_closed_object(
            value, name='qualified role Pod template', keys=cls._KEYS))

    @property
    def template_value(self) -> JsonObject:
        """Return a fresh decoded copy of the normalized Pod template."""

        return _parse_strict_canonical_json_object(
            self.template_json, name='qualified Pod-template JSON')

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'contract': self._CONTRACT,
            'template_json': self.template_json,
        }


@dataclasses.dataclass(frozen=True)
class QualifiedResourceActionRoleDeploymentV1(CanonicalContract):
    """Stable qualified identity of one ordinary-role Deployment."""

    version: int
    role: ApprovedRole
    namespace: str
    deployment_name: str
    deployment_uid: str
    generation: int
    observed_generation: int
    desired_replicas: int
    updated_replicas: int
    ready_replicas: int
    available_replicas: int
    unavailable_replicas: int
    pod_template: QualifiedResourceActionRolePodTemplateV1
    pod_template_sha256: str
    oci_manifest_digest: str
    source_commit: str
    artifact_inventory_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'role', 'namespace', 'deployment_name', 'deployment_uid',
        'generation', 'observed_generation', 'desired_replicas',
        'updated_replicas', 'ready_replicas', 'available_replicas',
        'unavailable_replicas', 'pod_template', 'pod_template_sha256',
        'oci_manifest_digest', 'source_commit', 'artifact_inventory_sha256'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('qualified deployment version must be integer 1.')
        object.__setattr__(
            self, 'role',
            _enum_value(ApprovedRole,
                        self.role,
                        name='qualified_deployment.role'))
        for field in ('namespace', 'deployment_name', 'deployment_uid'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'qualified_deployment.{field}',
                      maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        generation = _positive_integer(self.generation,
                                       name='qualified_deployment.generation')
        observed_generation = _positive_integer(
            self.observed_generation,
            name='qualified_deployment.observed_generation')
        if observed_generation != generation:
            raise ValueError('qualified deployment generation is not fully '
                             'observed.')
        object.__setattr__(self, 'generation', generation)
        object.__setattr__(self, 'observed_generation', observed_generation)
        desired = _positive_integer(
            self.desired_replicas, name='qualified_deployment.desired_replicas')
        object.__setattr__(self, 'desired_replicas', desired)
        for field in ('updated_replicas', 'ready_replicas',
                      'available_replicas'):
            count = _positive_integer(getattr(self, field),
                                      name=f'qualified_deployment.{field}')
            if count != desired:
                raise ValueError('qualified deployment is not fully updated, '
                                 'ready, and available.')
            object.__setattr__(self, field, count)
        unavailable = _nonnegative_integer(
            self.unavailable_replicas,
            name='qualified_deployment.unavailable_replicas')
        if unavailable != 0:
            raise ValueError('qualified deployment has unavailable replicas.')
        object.__setattr__(self, 'unavailable_replicas', unavailable)
        if type(self.pod_template
               ) is not QualifiedResourceActionRolePodTemplateV1:
            raise TypeError('qualified deployment Pod template is not typed.')
        pod_template_sha256 = _sha256(
            self.pod_template_sha256,
            name='qualified_deployment.pod_template_sha256')
        if pod_template_sha256 != self.pod_template.sha256:
            raise ValueError(
                'qualified deployment Pod-template digest does not '
                'match its projection.')
        object.__setattr__(self, 'pod_template_sha256', pod_template_sha256)
        object.__setattr__(
            self, 'oci_manifest_digest',
            _sha256_digest(self.oci_manifest_digest,
                           name='qualified_deployment.oci_manifest_digest'))
        object.__setattr__(
            self, 'source_commit',
            _commit(self.source_commit,
                    name='qualified_deployment.source_commit'))
        object.__setattr__(
            self, 'artifact_inventory_sha256',
            _sha256(self.artifact_inventory_sha256,
                    name=('qualified_deployment.'
                          'artifact_inventory_sha256')))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> QualifiedResourceActionRoleDeploymentV1:
        raw = _closed_object(value,
                             name='qualified role deployment',
                             keys=cls._KEYS)
        raw['pod_template'] = QualifiedResourceActionRolePodTemplateV1.from_value(
            raw['pod_template'])
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'role': self.role.value,
            'namespace': self.namespace,
            'deployment_name': self.deployment_name,
            'deployment_uid': self.deployment_uid,
            'generation': self.generation,
            'observed_generation': self.observed_generation,
            'desired_replicas': self.desired_replicas,
            'updated_replicas': self.updated_replicas,
            'ready_replicas': self.ready_replicas,
            'available_replicas': self.available_replicas,
            'unavailable_replicas': 0,
            'pod_template': self.pod_template.canonical_value(),
            'pod_template_sha256': self.pod_template_sha256,
            'oci_manifest_digest': self.oci_manifest_digest,
            'source_commit': self.source_commit,
            'artifact_inventory_sha256': self.artifact_inventory_sha256,
        }

    def validate_approved_image(self, image: ApprovedRoleImageV1) -> None:
        """Require exact equality with one policy-approved role artifact."""

        if type(image) is not ApprovedRoleImageV1:
            raise TypeError('qualified deployment image is not typed.')
        if (self.role is not image.role or
                self.oci_manifest_digest != image.oci_manifest_digest or
                self.source_commit != image.source_commit or
                self.artifact_inventory_sha256
                != image.artifact_inventory_sha256):
            raise ValueError('qualified deployment artifact is not approved.')


@dataclasses.dataclass(frozen=True)
class ResourceActionDeploymentInventoryV1(CanonicalContract):
    """Exact stable inventory of the three qualified ordinary roles."""

    version: int
    contract: str
    deployments: tuple[QualifiedResourceActionRoleDeploymentV1, ...]

    _CONTRACT: ClassVar[str] = 'resource_action_deployment_inventory_v1'
    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'contract', 'deployments'})
    _ROLE_ORDER: ClassVar[tuple[ApprovedRole,
                                ...]] = (ApprovedRole.API,
                                         ApprovedRole.ORDINARY_EXECUTOR,
                                         ApprovedRole.CONTROLLER)

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('deployment inventory version must be integer 1.')
        if type(self.contract) is not str or self.contract != self._CONTRACT:
            raise ValueError('deployment inventory contract is unsupported.')
        if (type(self.deployments) is not tuple or any(
                type(item) is not QualifiedResourceActionRoleDeploymentV1
                for item in self.deployments) or
                tuple(item.role for item in self.deployments)
                != self._ROLE_ORDER):
            raise ValueError('deployment inventory must contain the exact '
                             'three-role order.')
        if len({item.namespace for item in self.deployments}) != 1:
            raise ValueError('qualified deployments must share one namespace.')
        if (len({item.deployment_name for item in self.deployments}) != 3 or
                len({item.deployment_uid for item in self.deployments}) != 3):
            raise ValueError('qualified deployment identities must be '
                             'distinct.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ResourceActionDeploymentInventoryV1:
        raw = _closed_object(value,
                             name='resource-action deployment inventory',
                             keys=cls._KEYS)
        if type(raw['deployments']) is not list:
            raise TypeError('deployment inventory deployments must be a list.')
        return cls(version=raw['version'],
                   contract=raw['contract'],
                   deployments=tuple(
                       QualifiedResourceActionRoleDeploymentV1.from_value(item)
                       for item in raw['deployments']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'contract': self._CONTRACT,
            'deployments': [
                item.canonical_value() for item in self.deployments
            ],
        }

    def validate_for_policy(
            self, policy: ResourceActionQualificationPolicyV1) -> None:
        """Require each role to equal the policy's approved artifact."""

        if type(policy) is not ResourceActionQualificationPolicyV1:
            raise TypeError('deployment inventory policy is not typed.')
        for deployment, image in zip(self.deployments,
                                     policy.approved_role_images):
            deployment.validate_approved_image(image)


class ResourceActionCrashCanaryBoundaryV1(str, enum.Enum):
    """Closed one-to-one identifiers for fault categories 1 through 20."""

    PREPARATION_IDENTITY_PUBLICATION = 'preparation_identity_publication'
    WEIGHTED_CAPACITY_ADMISSION = 'weighted_capacity_admission'
    ADMISSION_COMMIT_BEFORE_APPROVAL = 'admission_commit_before_approval'
    APPROVAL_BEFORE_PRE_SUBMIT = 'approval_before_pre_submit'
    DUAL_DISPATCHER_DUE_DISCOVERY = 'dual_dispatcher_due_discovery'
    REQUEST_COMMIT_BEFORE_MATERIALIZATION_ACK = (
        'request_commit_before_materialization_ack')
    CLAIM_BEFORE_INITIAL_PROGRESS = 'claim_before_initial_progress'
    PROVIDER_PROGRESS_CHECKPOINTS = 'provider_progress_checkpoints'
    SKYLET_JOB_OUTBOX_RUNTIME = 'skylet_job_outbox_runtime'
    PROVIDER_RESULT_BEFORE_TERMINALIZATION = (
        'provider_result_before_terminalization')
    TERMINALIZATION_BEFORE_SERVE_REDUCTION = (
        'terminalization_before_serve_reduction')
    RETRY_REDUCTION_BEFORE_DUE_OBSERVATION = (
        'retry_reduction_before_due_observation')
    RETRY_MATERIALIZATION_WORKER_HANDOFF = (
        'retry_materialization_worker_handoff')
    PARTIAL_LAUNCH_SUPERSESSION_CLEANUP = (
        'partial_launch_supersession_cleanup')
    ROLE_EVICTION_LEADERSHIP_CHANGE = 'role_eviction_leadership_change'
    COMPATIBLE_IMAGE_ROLLBACK_REUPGRADE = (
        'compatible_image_rollback_reupgrade')
    COHORT_SELECTION_RETIREMENT = 'cohort_selection_retirement'
    CRASH_CANARY_LIFECYCLE = 'crash_canary_lifecycle'
    POLICY_ROTATION_MIXED_ROLES = 'policy_rotation_mixed_roles'
    MIXED_PATH_LAST_CAPACITY_UNIT = 'mixed_path_last_capacity_unit'


@dataclasses.dataclass(frozen=True)
class ResourceActionCrashCanaryRequirementV1(CanonicalContract):
    """One ordered mandatory fault-category requirement."""

    sequence: int
    boundary_id: ResourceActionCrashCanaryBoundaryV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({'sequence', 'boundary_id'})

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'sequence',
            _positive_integer(self.sequence, name='crash_requirement.sequence'))
        object.__setattr__(
            self, 'boundary_id',
            _enum_value(ResourceActionCrashCanaryBoundaryV1,
                        self.boundary_id,
                        name='crash_requirement.boundary_id'))

    @classmethod
    def from_value(cls, value: Any) -> ResourceActionCrashCanaryRequirementV1:
        return cls(**_closed_object(
            value, name='crash-canary requirement', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return {
            'sequence': self.sequence,
            'boundary_id': self.boundary_id.value,
        }


@dataclasses.dataclass(frozen=True)
class ResourceActionRequiredCrashCanaryInventoryV1(CanonicalContract):
    """Checked-in exact required crash-boundary inventory."""

    version: int
    contract: str
    requirements: tuple[ResourceActionCrashCanaryRequirementV1, ...]

    _CONTRACT: ClassVar[str] = 'resource_action_crash_canary_inventory_v1'
    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'contract', 'requirements'})
    _BOUNDARIES: ClassVar[tuple[ResourceActionCrashCanaryBoundaryV1, ...]] = (
        tuple(ResourceActionCrashCanaryBoundaryV1))

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('crash inventory version must be integer 1.')
        if type(self.contract) is not str or self.contract != self._CONTRACT:
            raise ValueError('crash inventory contract is unsupported.')
        if (type(self.requirements) is not tuple or any(
                type(item) is not ResourceActionCrashCanaryRequirementV1
                for item in self.requirements)):
            raise TypeError('crash inventory requirements must be typed.')
        actual = tuple(
            (item.sequence, item.boundary_id) for item in self.requirements)
        expected = tuple(enumerate(self._BOUNDARIES, start=1))
        if actual != expected:
            raise ValueError('crash inventory must contain the exact ordered '
                             '20-boundary matrix.')
        _ = self.canonical_bytes

    @classmethod
    def required(cls) -> ResourceActionRequiredCrashCanaryInventoryV1:
        """Construct the one complete required version-one inventory."""

        return cls(
            version=1,
            contract=cls._CONTRACT,
            requirements=tuple(
                ResourceActionCrashCanaryRequirementV1(sequence=index,
                                                       boundary_id=boundary)
                for index, boundary in enumerate(cls._BOUNDARIES, start=1)))

    @classmethod
    def from_value(cls,
                   value: Any) -> ResourceActionRequiredCrashCanaryInventoryV1:
        raw = _closed_object(value,
                             name='required crash-canary inventory',
                             keys=cls._KEYS)
        if type(raw['requirements']) is not list:
            raise TypeError('crash inventory requirements must be a list.')
        return cls(version=raw['version'],
                   contract=raw['contract'],
                   requirements=tuple(
                       ResourceActionCrashCanaryRequirementV1.from_value(item)
                       for item in raw['requirements']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'contract': self._CONTRACT,
            'requirements': [
                item.canonical_value() for item in self.requirements
            ],
        }


@dataclasses.dataclass(frozen=True)
class AuthoritySchemaHeadsV1(CanonicalContract):
    """The only schema-head tuple that may mint Serve035 authority."""

    api_requests_head: str
    serve_head: str
    global_user_state_head: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'api_requests_head', 'serve_head', 'global_user_state_head'})

    def __post_init__(self) -> None:
        if (type(self.api_requests_head) is not str or
                type(self.serve_head) is not str or
                type(self.global_user_state_head) is not str or
            (self.api_requests_head, self.serve_head,
             self.global_user_state_head) != ('007', '035', '028')):
            raise ValueError('authority proof schema heads are not exact.')

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)

    @classmethod
    def from_value(cls, value: Any) -> AuthoritySchemaHeadsV1:
        return cls(**_closed_object(
            value, name='authority schema heads', keys=cls._KEYS))


@dataclasses.dataclass(frozen=True)
class ResourceActionCandidateBindingV1(CanonicalContract):
    """Complete immutable preimage whose hash is candidate qualification."""

    version: int
    qualification_policy_sha256: str
    schema_heads: AuthoritySchemaHeadsV1
    deployment_inventory: ResourceActionDeploymentInventoryV1
    deployment_inventory_sha256: str
    selected_cohort: ApprovedAuthorityCohortArtifactV1
    selected_cohort_sha256: str
    capacity_profile: resource_actions.ServeActionCapacityProfileV1
    capacity_profile_sha256: str
    elected_version_identity: resource_actions.ServeServiceVersionSpecIdentityV1
    elected_version_identity_sha256: str
    live_replica_identity_inventory: HashedCanonicalObjectV1
    required_crash_canary_inventory: ResourceActionRequiredCrashCanaryInventoryV1
    required_crash_canary_inventory_sha256: str
    _live_replica_identity_inventory_snapshot: bytes = dataclasses.field(
        init=False, repr=False, compare=False)

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'qualification_policy_sha256', 'schema_heads',
        'deployment_inventory', 'deployment_inventory_sha256',
        'selected_cohort', 'selected_cohort_sha256', 'capacity_profile',
        'capacity_profile_sha256', 'elected_version_identity',
        'elected_version_identity_sha256', 'live_replica_identity_inventory',
        'required_crash_canary_inventory',
        'required_crash_canary_inventory_sha256'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('candidate binding version must be integer 1.')
        object.__setattr__(
            self, 'qualification_policy_sha256',
            _sha256(self.qualification_policy_sha256,
                    name='candidate_binding.qualification_policy_sha256'))
        if type(self.schema_heads) is not AuthoritySchemaHeadsV1:
            raise TypeError('candidate binding schema heads are invalid.')
        if type(self.deployment_inventory
               ) is not ResourceActionDeploymentInventoryV1:
            raise TypeError('candidate binding deployment inventory is '
                            'invalid.')
        self._require_nested_hash('deployment_inventory')
        if type(self.selected_cohort) is not ApprovedAuthorityCohortArtifactV1:
            raise TypeError('candidate binding selected cohort is invalid.')
        self._require_nested_hash('selected_cohort')
        if type(self.capacity_profile
               ) is not resource_actions.ServeActionCapacityProfileV1:
            raise TypeError('candidate binding capacity profile is invalid.')
        expected_profile = (resource_actions.ServeActionCapacityProfileV1.
                            ordinary_ondemand_physical_width1())
        if self.capacity_profile.canonical_bytes != expected_profile.canonical_bytes:
            raise ValueError('candidate binding capacity profile is not the '
                             'closed M4 profile.')
        self._require_nested_hash('capacity_profile')
        if type(self.elected_version_identity
               ) is not resource_actions.ServeServiceVersionSpecIdentityV1:
            raise TypeError('candidate binding elected identity is invalid.')
        if (self.elected_version_identity.capacity_profile.canonical_bytes
                != self.capacity_profile.canonical_bytes or
                self.elected_version_identity.provider_profile
                != 'pod_cluster_v1'):
            raise ValueError('candidate binding elected identity is outside '
                             'the closed provider/capacity profile.')
        self._require_nested_hash('elected_version_identity')
        if type(self.live_replica_identity_inventory
               ) is not HashedCanonicalObjectV1:
            raise TypeError('candidate binding live-replica inventory is '
                            'invalid.')
        live_replica_identity_inventory = HashedCanonicalObjectV1.from_value(
            self.live_replica_identity_inventory.canonical_value())
        object.__setattr__(self, 'live_replica_identity_inventory',
                           live_replica_identity_inventory)
        object.__setattr__(self, '_live_replica_identity_inventory_snapshot',
                           live_replica_identity_inventory.canonical_bytes)
        if type(self.required_crash_canary_inventory
               ) is not ResourceActionRequiredCrashCanaryInventoryV1:
            raise TypeError('candidate binding crash inventory is invalid.')
        self._require_nested_hash('required_crash_canary_inventory')
        _ = self.canonical_bytes

    def _require_nested_hash(self, field: str) -> None:
        value = getattr(self, field)
        digest_field = f'{field}_sha256'
        digest = _sha256(getattr(self, digest_field),
                         name=f'candidate_binding.{digest_field}')
        if digest != value.sha256:
            raise ValueError(f'candidate binding {field} digest does not '
                             'match its value.')
        object.__setattr__(self, digest_field, digest)

    def validate_for_policy(
            self, policy: ResourceActionQualificationPolicyV1) -> None:
        """Require all policy-derived candidate inputs to match exactly."""

        if type(policy) is not ResourceActionQualificationPolicyV1:
            raise TypeError('candidate binding policy is not typed.')
        self._validated_live_replica_identity_inventory()
        if self.qualification_policy_sha256 != policy.sha256:
            raise ValueError('candidate binding policy digest does not '
                             'match.')
        expected_heads = AuthoritySchemaHeadsV1(
            api_requests_head=policy.api_requests_head,
            serve_head=policy.serve_head,
            global_user_state_head=policy.global_user_state_head)
        if self.schema_heads.canonical_bytes != expected_heads.canonical_bytes:
            raise ValueError('candidate binding schema heads do not match '
                             'policy.')
        self.deployment_inventory.validate_for_policy(policy)
        if not any(self.selected_cohort.canonical_bytes == item.canonical_bytes
                   for item in policy.approved_cohorts):
            raise ValueError('candidate binding selected cohort is not '
                             'approved.')
        if (policy.crash_canary_inventory_contract
                != self.required_crash_canary_inventory.contract or
                policy.required_crash_canary_inventory_sha256
                != self.required_crash_canary_inventory.sha256):
            raise ValueError('candidate binding crash inventory does not '
                             'match policy.')

    def _validated_live_replica_identity_inventory(
            self) -> HashedCanonicalObjectV1:
        current = HashedCanonicalObjectV1.from_value(
            self.live_replica_identity_inventory.canonical_value())
        if (current.canonical_bytes
                != self._live_replica_identity_inventory_snapshot):
            raise ValueError('candidate binding live-replica inventory differs '
                             'from its construction snapshot.')
        return current

    @classmethod
    def from_value(cls, value: Any) -> ResourceActionCandidateBindingV1:
        live_replica_identity_inventory = None
        if (type(value) is dict and all(type(key) is str for key in value) and
                set(value) == cls._KEYS):
            live_replica_identity_inventory = HashedCanonicalObjectV1.from_value(
                value['live_replica_identity_inventory'])
        raw = _closed_object(value,
                             name='resource-action candidate binding',
                             keys=cls._KEYS)
        assert live_replica_identity_inventory is not None
        return cls(
            version=raw['version'],
            qualification_policy_sha256=raw['qualification_policy_sha256'],
            schema_heads=AuthoritySchemaHeadsV1.from_value(raw['schema_heads']),
            deployment_inventory=ResourceActionDeploymentInventoryV1.from_value(
                raw['deployment_inventory']),
            deployment_inventory_sha256=raw['deployment_inventory_sha256'],
            selected_cohort=ApprovedAuthorityCohortArtifactV1.from_value(
                raw['selected_cohort']),
            selected_cohort_sha256=raw['selected_cohort_sha256'],
            capacity_profile=resource_actions.ServeActionCapacityProfileV1.
            from_value(raw['capacity_profile']),
            capacity_profile_sha256=raw['capacity_profile_sha256'],
            elected_version_identity=resource_actions.
            ServeServiceVersionSpecIdentityV1.from_value(
                raw['elected_version_identity']),
            elected_version_identity_sha256=raw[
                'elected_version_identity_sha256'],
            live_replica_identity_inventory=live_replica_identity_inventory,
            required_crash_canary_inventory=
            ResourceActionRequiredCrashCanaryInventoryV1.from_value(
                raw['required_crash_canary_inventory']),
            required_crash_canary_inventory_sha256=raw[
                'required_crash_canary_inventory_sha256'])

    def canonical_value(self) -> JsonObject:
        live_replica_identity_inventory = (
            self._validated_live_replica_identity_inventory())
        return {
            'version': 1,
            'qualification_policy_sha256': self.qualification_policy_sha256,
            'schema_heads': self.schema_heads.canonical_value(),
            'deployment_inventory': self.deployment_inventory.canonical_value(),
            'deployment_inventory_sha256': self.deployment_inventory_sha256,
            'selected_cohort': self.selected_cohort.canonical_value(),
            'selected_cohort_sha256': self.selected_cohort_sha256,
            'capacity_profile': self.capacity_profile.canonical_value(),
            'capacity_profile_sha256': self.capacity_profile_sha256,
            'elected_version_identity':
                self.elected_version_identity.canonical_value(),
            'elected_version_identity_sha256':
                self.elected_version_identity_sha256,
            'live_replica_identity_inventory':
                live_replica_identity_inventory.canonical_value(),
            'required_crash_canary_inventory':
                self.required_crash_canary_inventory.canonical_value(),
            'required_crash_canary_inventory_sha256':
                self.required_crash_canary_inventory_sha256,
        }


@dataclasses.dataclass(frozen=True)
class AuthorityServiceFenceV1(CanonicalContract):
    """One exact service incarnation, controller owner, and lifecycle fence."""

    service_name: str
    service_hash: str
    controller_owner_fence: str
    lifecycle_epoch: int

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'service_name', 'service_hash', 'controller_owner_fence',
        'lifecycle_epoch'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'service_name',
            _text(self.service_name,
                  name='service_fence.service_name',
                  maximum_bytes=256))
        service_uuid = _schema_uuid(self.service_hash,
                                    name='service_fence.service_hash')
        object.__setattr__(self, 'service_hash', str(service_uuid))
        object.__setattr__(
            self, 'controller_owner_fence',
            _text(self.controller_owner_fence,
                  name='service_fence.controller_owner_fence'))
        object.__setattr__(
            self, 'lifecycle_epoch',
            _positive_integer(self.lifecycle_epoch,
                              name='service_fence.lifecycle_epoch'))

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)

    @classmethod
    def from_value(cls, value: Any) -> AuthorityServiceFenceV1:
        return cls(**_closed_object(
            value, name='authority service fence', keys=cls._KEYS))


@dataclasses.dataclass(frozen=True)
class PrivateShadowActivationProofV1(CanonicalContract):
    """Server-minted proof consumed only by ``legacy -> shadow``."""

    version: int
    service_fence: AuthorityServiceFenceV1
    candidate_since_before: None
    selected_cohort_id: str
    approved_cohort: ApprovedAuthorityCohortArtifactV1
    registration_set: ProviderAuthorityWorkerRegistrationSetV2
    registration_set_sha256: str
    capacity_profile: HashedCanonicalObjectV1
    elected_version_identity: HashedCanonicalObjectV1
    schema_heads: AuthoritySchemaHeadsV1
    verified_at: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('shadow activation proof version must be 1.')
        if type(self.service_fence) is not AuthorityServiceFenceV1:
            raise TypeError('shadow activation service fence is invalid.')
        if self.candidate_since_before is not None:
            raise ValueError('shadow activation prior window must be null.')
        object.__setattr__(
            self, 'selected_cohort_id',
            _text(self.selected_cohort_id,
                  name='activation.selected_cohort_id'))
        if (type(self.approved_cohort) is not ApprovedAuthorityCohortArtifactV1
                or self.approved_cohort.cohort_id != self.selected_cohort_id):
            raise ValueError('activation approved cohort does not match.')
        if type(self.registration_set
               ) is not ProviderAuthorityWorkerRegistrationSetV2:
            raise TypeError('activation registration set is invalid.')
        self.registration_set.validate_accepted()
        digest = _sha256(self.registration_set_sha256,
                         name='activation.registration_set_sha256')
        if digest != self.registration_set.sha256:
            raise ValueError('activation registration-set hash differs.')
        object.__setattr__(self, 'registration_set_sha256', digest)
        for field in ('capacity_profile', 'elected_version_identity'):
            if type(getattr(self, field)) is not HashedCanonicalObjectV1:
                raise TypeError(f'activation {field} is invalid.')
        if type(self.schema_heads) is not AuthoritySchemaHeadsV1:
            raise TypeError('activation schema heads are invalid.')
        object.__setattr__(
            self, 'verified_at',
            _timestamp(self.verified_at, name='activation.verified_at'))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'service_fence': self.service_fence.canonical_value(),
            'candidate_since_before': None,
            'selected_cohort_id': self.selected_cohort_id,
            'approved_cohort': self.approved_cohort.canonical_value(),
            'registration_set': self.registration_set.canonical_value(),
            'registration_set_sha256': self.registration_set_sha256,
            'capacity_profile': self.capacity_profile.canonical_value(),
            'elected_version_identity':
                self.elected_version_identity.canonical_value(),
            'schema_heads': self.schema_heads.canonical_value(),
            'verified_at': self.verified_at,
        }


class PrivateDispatchKind(str, enum.Enum):
    SHADOW_CANDIDATE = 'shadow_candidate'
    AUTHORITATIVE_ACTION = 'authoritative_action'


@dataclasses.dataclass(frozen=True)
class PrivateRequestClaimProofV1(CanonicalContract):
    """One exact RUNNING request claim and unexpired queue lease."""

    request_id: uuid.UUID
    execution_generation: int
    claim_token_sha256: str
    queue_lease_expires_at: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'request_id',
            _uuid(self.request_id, name='dispatch_claim.request_id'))
        object.__setattr__(
            self, 'execution_generation',
            _positive_integer(self.execution_generation,
                              name='dispatch_claim.execution_generation'))
        object.__setattr__(
            self, 'claim_token_sha256',
            _sha256(self.claim_token_sha256,
                    name='dispatch_claim.claim_token_sha256'))
        object.__setattr__(
            self, 'queue_lease_expires_at',
            _timestamp(self.queue_lease_expires_at,
                       name='dispatch_claim.queue_lease_expires_at'))

    def canonical_value(self) -> JsonObject:
        return {
            'request_id': str(self.request_id),
            'execution_generation': self.execution_generation,
            'claim_token_sha256': self.claim_token_sha256,
            'queue_lease_expires_at': self.queue_lease_expires_at,
        }


@dataclasses.dataclass(frozen=True)
class ShadowCandidateDispatchBindingV1(CanonicalContract):
    """Shadow-candidate branch of private dispatch readiness."""

    candidate_epoch: uuid.UUID
    qualification_policy_sha256: str
    qualification_binding_sha256: str
    represented_attempt: HashedCanonicalObjectV1

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'candidate_epoch',
            _uuid(self.candidate_epoch, name='shadow_dispatch.candidate_epoch'))
        for field in ('qualification_policy_sha256',
                      'qualification_binding_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field), name=f'shadow_dispatch.{field}'))
        if type(self.represented_attempt) is not HashedCanonicalObjectV1:
            raise TypeError('shadow dispatch attempt is invalid.')

    def canonical_value(self) -> JsonObject:
        return {
            'candidate_epoch': str(self.candidate_epoch),
            'qualification_policy_sha256': self.qualification_policy_sha256,
            'qualification_binding_sha256': self.qualification_binding_sha256,
            'represented_attempt': self.represented_attempt.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class AuthoritativeDispatchBindingV1(CanonicalContract):
    """UUID policy and action-attempt branch of dispatch readiness."""

    policy_epoch: uuid.UUID
    policy_sha256: str
    authority_binding_sha256: str
    action_id: uuid.UUID
    attempt: int
    immutable_input_sha256: str
    progress_revision: int
    progress_sha256: str
    service_version_identity_sha256: str
    capacity_profile_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'policy_epoch',
            _uuid(self.policy_epoch, name='dispatch.policy_epoch'))
        object.__setattr__(self, 'action_id',
                           _uuid(self.action_id, name='dispatch.action_id'))
        object.__setattr__(
            self, 'attempt',
            _positive_integer(self.attempt, name='dispatch.attempt'))
        object.__setattr__(
            self, 'progress_revision',
            _positive_integer(self.progress_revision,
                              name='dispatch.progress_revision'))
        for field in ('policy_sha256', 'authority_binding_sha256',
                      'immutable_input_sha256', 'progress_sha256',
                      'service_version_identity_sha256',
                      'capacity_profile_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field), name=f'dispatch.{field}'))

    def canonical_value(self) -> JsonObject:
        value = dataclasses.asdict(self)
        value['policy_epoch'] = str(self.policy_epoch)
        value['action_id'] = str(self.action_id)
        return value


@dataclasses.dataclass(frozen=True)
class PrivateDispatchReadinessProofV1(CanonicalContract):
    """One claim-bound, non-bearer private dispatch proof."""

    version: int
    dispatch_kind: PrivateDispatchKind
    service_fence: AuthorityServiceFenceV1
    service_mode: resource_actions.ResourceActionMode
    candidate_since: str
    decision_id: uuid.UUID
    reference_revision: int
    claim: PrivateRequestClaimProofV1
    cohort_id: str
    accepted_membership: ProviderAuthorityWorkerAcceptedMembershipV2
    proof_inventory: HashedCanonicalObjectV1
    schema_heads: AuthoritySchemaHeadsV1
    binding: ShadowCandidateDispatchBindingV1 | AuthoritativeDispatchBindingV1
    verified_at: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('dispatch readiness version must be integer 1.')
        kind = _enum_value(PrivateDispatchKind,
                           self.dispatch_kind,
                           name='dispatch.kind')
        object.__setattr__(self, 'dispatch_kind', kind)
        if type(self.service_fence) is not AuthorityServiceFenceV1:
            raise TypeError('dispatch service fence is invalid.')
        if type(self.service_mode) is not resource_actions.ResourceActionMode:
            try:
                object.__setattr__(
                    self, 'service_mode',
                    resource_actions.ResourceActionMode(self.service_mode))
            except (TypeError, ValueError) as e:
                raise ValueError('dispatch service mode is invalid.') from e
        object.__setattr__(
            self, 'candidate_since',
            _timestamp(self.candidate_since, name='dispatch.candidate_since'))
        object.__setattr__(self, 'decision_id',
                           _uuid(self.decision_id, name='dispatch.decision_id'))
        object.__setattr__(
            self, 'reference_revision',
            _positive_integer(self.reference_revision,
                              name='dispatch.reference_revision'))
        if type(self.claim) is not PrivateRequestClaimProofV1:
            raise TypeError('dispatch claim is invalid.')
        object.__setattr__(self, 'cohort_id',
                           _text(self.cohort_id, name='dispatch.cohort_id'))
        if type(self.accepted_membership
               ) is not ProviderAuthorityWorkerAcceptedMembershipV2:
            raise TypeError('dispatch accepted membership is invalid.')
        if type(self.proof_inventory) is not HashedCanonicalObjectV1:
            raise TypeError('dispatch proof inventory is invalid.')
        if type(self.schema_heads) is not AuthoritySchemaHeadsV1:
            raise TypeError('dispatch schema heads are invalid.')
        if kind is PrivateDispatchKind.SHADOW_CANDIDATE:
            if (self.service_mode
                    is not resource_actions.ResourceActionMode.SHADOW or
                    type(self.binding) is not ShadowCandidateDispatchBindingV1):
                raise ValueError('shadow dispatch branch is inconsistent.')
        elif (self.service_mode
              is not resource_actions.ResourceActionMode.AUTHORITATIVE or
              type(self.binding) is not AuthoritativeDispatchBindingV1):
            raise ValueError('authoritative dispatch branch is inconsistent.')
        object.__setattr__(
            self, 'verified_at',
            _timestamp(self.verified_at, name='dispatch.verified_at'))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'dispatch_kind': self.dispatch_kind.value,
            'service_fence': self.service_fence.canonical_value(),
            'service_mode': self.service_mode.value,
            'candidate_since': self.candidate_since,
            'decision_id': str(self.decision_id),
            'reference_revision': self.reference_revision,
            'claim': self.claim.canonical_value(),
            'cohort_id': self.cohort_id,
            'accepted_membership': self.accepted_membership.canonical_value(),
            'proof_inventory': self.proof_inventory.canonical_value(),
            'schema_heads': self.schema_heads.canonical_value(),
            'binding': self.binding.canonical_value(),
            'verified_at': self.verified_at,
        }


@dataclasses.dataclass(frozen=True)
class AuthoritativePromotionProofV1(CanonicalContract):
    """Server-minted proof consumed only by ``shadow -> authoritative``."""

    version: int
    service_fence: AuthorityServiceFenceV1
    candidate_epoch: uuid.UUID
    candidate_since: str
    verified_at: str
    candidate_duration_seconds: int
    qualification_policy_sha256: str
    qualification_binding_sha256: str
    coverage_inventory_sha256: str
    clean_launches: int
    clean_downs: int
    blocker_count: int
    crash_canary_inventory: HashedCanonicalObjectV1
    referenced_cohort_inventory: HashedCanonicalObjectV1
    deployment_inventory: HashedCanonicalObjectV1
    elected_version_identity: HashedCanonicalObjectV1
    live_replica_identity_inventory: HashedCanonicalObjectV1
    schema_heads: AuthoritySchemaHeadsV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'service_fence', 'candidate_epoch', 'candidate_since',
        'verified_at', 'candidate_duration_seconds',
        'qualification_policy_sha256', 'qualification_binding_sha256',
        'coverage_inventory_sha256', 'clean_launches', 'clean_downs',
        'blocker_count', 'crash_canary_inventory',
        'referenced_cohort_inventory', 'deployment_inventory',
        'elected_version_identity', 'live_replica_identity_inventory',
        'schema_heads'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('promotion proof version must be integer 1.')
        if type(self.service_fence) is not AuthorityServiceFenceV1:
            raise TypeError('promotion service fence is invalid.')
        object.__setattr__(
            self, 'candidate_epoch',
            _uuid(self.candidate_epoch, name='promotion.candidate_epoch'))
        for field in ('candidate_since', 'verified_at'):
            object.__setattr__(
                self, field,
                _timestamp(getattr(self, field), name=f'promotion.{field}'))
        duration = _positive_integer(
            self.candidate_duration_seconds,
            name='promotion.candidate_duration_seconds')
        if duration < 86_400:
            raise ValueError('promotion window is shorter than 24 hours.')
        object.__setattr__(self, 'candidate_duration_seconds', duration)
        for field in ('qualification_policy_sha256',
                      'qualification_binding_sha256',
                      'coverage_inventory_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field), name=f'promotion.{field}'))
        for field in ('clean_launches', 'clean_downs'):
            count = _nonnegative_integer(getattr(self, field),
                                         name=f'promotion.{field}')
            if count < 100:
                raise ValueError(f'promotion {field} is below 100.')
            object.__setattr__(self, field, count)
        if _nonnegative_integer(self.blocker_count,
                                name='promotion.blocker_count') != 0:
            raise ValueError('promotion proof cannot contain blockers.')
        for field in ('crash_canary_inventory', 'referenced_cohort_inventory',
                      'deployment_inventory', 'elected_version_identity',
                      'live_replica_identity_inventory'):
            if type(getattr(self, field)) is not HashedCanonicalObjectV1:
                raise TypeError(f'promotion {field} is invalid.')
        if type(self.schema_heads) is not AuthoritySchemaHeadsV1:
            raise TypeError('promotion schema heads are invalid.')

    @property
    def completed_at(self) -> str:
        return self.verified_at

    @classmethod
    def from_value(cls, value: Any) -> AuthoritativePromotionProofV1:
        raw = _closed_object(value,
                             name='authoritative promotion proof',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            service_fence=AuthorityServiceFenceV1.from_value(
                raw['service_fence']),
            candidate_epoch=raw['candidate_epoch'],
            candidate_since=raw['candidate_since'],
            verified_at=raw['verified_at'],
            candidate_duration_seconds=raw['candidate_duration_seconds'],
            qualification_policy_sha256=raw['qualification_policy_sha256'],
            qualification_binding_sha256=raw['qualification_binding_sha256'],
            coverage_inventory_sha256=raw['coverage_inventory_sha256'],
            clean_launches=raw['clean_launches'],
            clean_downs=raw['clean_downs'],
            blocker_count=raw['blocker_count'],
            crash_canary_inventory=HashedCanonicalObjectV1.from_value(
                raw['crash_canary_inventory']),
            referenced_cohort_inventory=HashedCanonicalObjectV1.from_value(
                raw['referenced_cohort_inventory']),
            deployment_inventory=HashedCanonicalObjectV1.from_value(
                raw['deployment_inventory']),
            elected_version_identity=HashedCanonicalObjectV1.from_value(
                raw['elected_version_identity']),
            live_replica_identity_inventory=HashedCanonicalObjectV1.from_value(
                raw['live_replica_identity_inventory']),
            schema_heads=AuthoritySchemaHeadsV1.from_value(raw['schema_heads']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'service_fence': self.service_fence.canonical_value(),
            'candidate_epoch': str(self.candidate_epoch),
            'candidate_since': self.candidate_since,
            'verified_at': self.verified_at,
            'candidate_duration_seconds': self.candidate_duration_seconds,
            'qualification_policy_sha256': self.qualification_policy_sha256,
            'qualification_binding_sha256': self.qualification_binding_sha256,
            'coverage_inventory_sha256': self.coverage_inventory_sha256,
            'clean_launches': self.clean_launches,
            'clean_downs': self.clean_downs,
            'blocker_count': 0,
            'crash_canary_inventory':
                self.crash_canary_inventory.canonical_value(),
            'referenced_cohort_inventory':
                self.referenced_cohort_inventory.canonical_value(),
            'deployment_inventory': self.deployment_inventory.canonical_value(),
            'elected_version_identity':
                self.elected_version_identity.canonical_value(),
            'live_replica_identity_inventory':
                self.live_replica_identity_inventory.canonical_value(),
            'schema_heads': self.schema_heads.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class AuthorityNonterminalInventoryV1(CanonicalContract):
    """Canonical bounded work inventory retained by one policy epoch."""

    version: int
    leased_private_request_ids: tuple[uuid.UUID, ...]
    nonterminal_action_ids: tuple[uuid.UUID, ...]
    active_reference_ids: tuple[uuid.UUID, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'leased_private_request_ids', 'nonterminal_action_ids',
        'active_reference_ids'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('nonterminal inventory version must be integer 1.')
        for field in ('leased_private_request_ids', 'nonterminal_action_ids',
                      'active_reference_ids'):
            values = getattr(self, field)
            if type(values) is not tuple:
                raise TypeError(f'nonterminal inventory {field} must be tuple.')
            parsed = tuple(
                _uuid(value, name=f'nonterminal_inventory.{field}')
                for value in values)
            if parsed != tuple(sorted(set(parsed),
                                      key=lambda item: item.bytes)):
                raise ValueError(
                    f'nonterminal inventory {field} must be sorted '
                    'and distinct.')
            object.__setattr__(self, field, parsed)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> AuthorityNonterminalInventoryV1:
        raw = _closed_object(value,
                             name='authority nonterminal inventory',
                             keys=cls._KEYS)
        for field in ('leased_private_request_ids', 'nonterminal_action_ids',
                      'active_reference_ids'):
            if type(raw[field]) is not list:
                raise TypeError(f'nonterminal inventory {field} must be list.')
            raw[field] = tuple(raw[field])
        return cls(**raw)

    @property
    def is_empty(self) -> bool:
        return not (self.leased_private_request_ids or
                    self.nonterminal_action_ids or self.active_reference_ids)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'leased_private_request_ids': [
                str(value) for value in self.leased_private_request_ids
            ],
            'nonterminal_action_ids': [
                str(value) for value in self.nonterminal_action_ids
            ],
            'active_reference_ids': [
                str(value) for value in self.active_reference_ids
            ],
        }


@dataclasses.dataclass(frozen=True)
class ServeAuthorityPolicyRotationProofV1(CanonicalContract):
    """Closed compatible-image rotation proof with UUID lineage."""

    version: int
    service_fence: AuthorityServiceFenceV1
    predecessor_policy_epoch: uuid.UUID
    predecessor_policy_sha256: str
    schema_heads: AuthoritySchemaHeadsV1
    successor_policy: ResourceActionQualificationPolicyV1
    successor_policy_sha256: str
    successor_authority_binding_sha256: str
    staged_artifact_inventory: HashedCanonicalObjectV1
    rollback_artifact_inventory: HashedCanonicalObjectV1
    service_version_inventory: HashedCanonicalObjectV1
    cohort_inventory: HashedCanonicalObjectV1
    nonterminal_inventory: AuthorityNonterminalInventoryV1
    started_at: str
    completed_at: str
    reason: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'service_fence', 'predecessor_policy_epoch',
        'predecessor_policy_sha256', 'schema_heads', 'successor_policy',
        'successor_policy_sha256', 'successor_authority_binding_sha256',
        'staged_artifact_inventory', 'rollback_artifact_inventory',
        'service_version_inventory', 'cohort_inventory',
        'nonterminal_inventory', 'started_at', 'completed_at', 'reason'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('rotation proof version must be integer 1.')
        if type(self.service_fence) is not AuthorityServiceFenceV1:
            raise TypeError('rotation service fence is invalid.')
        object.__setattr__(
            self, 'predecessor_policy_epoch',
            _uuid(self.predecessor_policy_epoch,
                  name='rotation.predecessor_policy_epoch'))
        object.__setattr__(
            self, 'predecessor_policy_sha256',
            _sha256(self.predecessor_policy_sha256,
                    name='rotation.predecessor_policy_sha256'))
        if type(self.schema_heads) is not AuthoritySchemaHeadsV1:
            raise TypeError('rotation schema heads are invalid.')
        if type(self.successor_policy
               ) is not ResourceActionQualificationPolicyV1:
            raise TypeError('rotation successor policy is invalid.')
        digest = _sha256(self.successor_policy_sha256,
                         name='rotation.successor_policy_sha256')
        if digest != self.successor_policy.sha256:
            raise ValueError('rotation successor policy hash differs.')
        object.__setattr__(self, 'successor_policy_sha256', digest)
        object.__setattr__(
            self, 'successor_authority_binding_sha256',
            _sha256(self.successor_authority_binding_sha256,
                    name='rotation.successor_authority_binding_sha256'))
        for field in ('staged_artifact_inventory',
                      'rollback_artifact_inventory',
                      'service_version_inventory', 'cohort_inventory'):
            if type(getattr(self, field)) is not HashedCanonicalObjectV1:
                raise TypeError(f'rotation {field} is invalid.')
        if type(self.nonterminal_inventory
               ) is not AuthorityNonterminalInventoryV1:
            raise TypeError('rotation nonterminal inventory is invalid.')
        if not self.nonterminal_inventory.is_empty:
            raise ValueError(
                'rotation requires an empty nonterminal inventory.')
        for field in ('started_at', 'completed_at'):
            object.__setattr__(
                self, field,
                _timestamp(getattr(self, field), name=f'rotation.{field}'))
        if timestamp_to_datetime(
                self.completed_at,
                name='rotation.completed_at') < timestamp_to_datetime(
                    self.started_at, name='rotation.started_at'):
            raise ValueError('rotation completion predates its start.')
        if self.reason != 'COMPATIBLE_IMAGE_ROTATION':
            raise ValueError('rotation reason is unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ServeAuthorityPolicyRotationProofV1:
        raw = _closed_object(value,
                             name='authority policy rotation proof',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            service_fence=AuthorityServiceFenceV1.from_value(
                raw['service_fence']),
            predecessor_policy_epoch=raw['predecessor_policy_epoch'],
            predecessor_policy_sha256=raw['predecessor_policy_sha256'],
            schema_heads=AuthoritySchemaHeadsV1.from_value(raw['schema_heads']),
            successor_policy=ResourceActionQualificationPolicyV1.from_value(
                raw['successor_policy']),
            successor_policy_sha256=raw['successor_policy_sha256'],
            successor_authority_binding_sha256=raw[
                'successor_authority_binding_sha256'],
            staged_artifact_inventory=HashedCanonicalObjectV1.from_value(
                raw['staged_artifact_inventory']),
            rollback_artifact_inventory=HashedCanonicalObjectV1.from_value(
                raw['rollback_artifact_inventory']),
            service_version_inventory=HashedCanonicalObjectV1.from_value(
                raw['service_version_inventory']),
            cohort_inventory=HashedCanonicalObjectV1.from_value(
                raw['cohort_inventory']),
            nonterminal_inventory=AuthorityNonterminalInventoryV1.from_value(
                raw['nonterminal_inventory']),
            started_at=raw['started_at'],
            completed_at=raw['completed_at'],
            reason=raw['reason'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'service_fence': self.service_fence.canonical_value(),
            'predecessor_policy_epoch': str(self.predecessor_policy_epoch),
            'predecessor_policy_sha256': self.predecessor_policy_sha256,
            'schema_heads': self.schema_heads.canonical_value(),
            'successor_policy': self.successor_policy.canonical_value(),
            'successor_policy_sha256': self.successor_policy_sha256,
            'successor_authority_binding_sha256':
                self.successor_authority_binding_sha256,
            'staged_artifact_inventory':
                self.staged_artifact_inventory.canonical_value(),
            'rollback_artifact_inventory':
                self.rollback_artifact_inventory.canonical_value(),
            'service_version_inventory':
                self.service_version_inventory.canonical_value(),
            'cohort_inventory': self.cohort_inventory.canonical_value(),
            'nonterminal_inventory':
                self.nonterminal_inventory.canonical_value(),
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'reason': 'COMPATIBLE_IMAGE_ROTATION',
        }


@dataclasses.dataclass(frozen=True)
class AuthoritySchemaHeadsV2(CanonicalContract):
    """Exact API/Serve/state heads accepted by live V2 authority."""

    api_requests_head: str
    serve_head: str
    global_user_state_head: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'api_requests_head', 'serve_head', 'global_user_state_head'})

    def __post_init__(self) -> None:
        if (type(self.api_requests_head) is not str or
                type(self.serve_head) is not str or
                type(self.global_user_state_head) is not str or
                self.api_requests_head != '008' or
                self.serve_head not in ('039', '040') or
                self.global_user_state_head != '028'):
            raise ValueError('V2 authority proof schema heads are not exact.')

    @classmethod
    def from_value(cls, value: Any) -> AuthoritySchemaHeadsV2:
        return cls(**_closed_object(
            value, name='V2 authority schema heads', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ApprovedAuthorityDeploymentSetV1(CanonicalContract):
    """One complete, immutable ordinary-role/cohort deployment set."""

    version: int
    role_images: tuple[ApprovedRoleImageV1, ...]
    approved_cohorts: tuple[ApprovedAuthorityCohortArtifactV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'role_images', 'approved_cohorts'})
    _ROLE_ORDER: ClassVar[tuple[ApprovedRole,
                                ...]] = (ApprovedRole.API,
                                         ApprovedRole.ORDINARY_EXECUTOR,
                                         ApprovedRole.CONTROLLER)

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('deployment-set version must be integer 1.')
        if (type(self.role_images) is not tuple or any(
                type(item) is not ApprovedRoleImageV1
                for item in self.role_images) or
                tuple(item.role for item in self.role_images)
                != self._ROLE_ORDER):
            raise ValueError('deployment set must contain the exact '
                             'three-role order.')
        if (type(self.approved_cohorts) is not tuple or
                not 1 <= len(self.approved_cohorts) <= 16 or any(
                    type(item) is not ApprovedAuthorityCohortArtifactV1
                    for item in self.approved_cohorts)):
            raise ValueError('deployment set must contain 1..16 typed '
                             'cohorts.')
        cohort_ids = tuple(item.cohort_id for item in self.approved_cohorts)
        if cohort_ids != tuple(sorted(set(cohort_ids))):
            raise ValueError('deployment-set cohorts must be sorted and '
                             'distinct.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ApprovedAuthorityDeploymentSetV1:
        raw = _closed_object(value,
                             name='approved authority deployment set',
                             keys=cls._KEYS)
        if (type(raw['role_images']) is not list or
                type(raw['approved_cohorts']) is not list):
            raise TypeError('deployment-set inventories must be lists.')
        return cls(version=raw['version'],
                   role_images=tuple(
                       ApprovedRoleImageV1.from_value(item)
                       for item in raw['role_images']),
                   approved_cohorts=tuple(
                       ApprovedAuthorityCohortArtifactV1.from_value(item)
                       for item in raw['approved_cohorts']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'role_images': [
                item.canonical_value() for item in self.role_images
            ],
            'approved_cohorts': [
                item.canonical_value() for item in self.approved_cohorts
            ],
        }


@dataclasses.dataclass(frozen=True)
class ApprovedAuthorityDeploymentSetBindingV1(CanonicalContract):
    """One deployment set paired with its recomputed canonical digest."""

    deployment_set: ApprovedAuthorityDeploymentSetV1
    deployment_set_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'deployment_set', 'deployment_set_sha256'})

    def __post_init__(self) -> None:
        if type(self.deployment_set) is not ApprovedAuthorityDeploymentSetV1:
            raise TypeError('deployment-set binding value is invalid.')
        digest = _sha256(self.deployment_set_sha256,
                         name='deployment_set_binding.sha256')
        if digest != self.deployment_set.sha256:
            raise ValueError('deployment-set binding digest differs.')
        object.__setattr__(self, 'deployment_set_sha256', digest)

    @classmethod
    def for_deployment_set(
        cls, deployment_set: ApprovedAuthorityDeploymentSetV1
    ) -> ApprovedAuthorityDeploymentSetBindingV1:
        if type(deployment_set) is not ApprovedAuthorityDeploymentSetV1:
            raise TypeError('deployment-set binding requires a typed set.')
        return cls(deployment_set=deployment_set,
                   deployment_set_sha256=deployment_set.sha256)

    @classmethod
    def from_value(cls, value: Any) -> ApprovedAuthorityDeploymentSetBindingV1:
        raw = _closed_object(value,
                             name='approved deployment-set binding',
                             keys=cls._KEYS)
        return cls(deployment_set=ApprovedAuthorityDeploymentSetV1.from_value(
            raw['deployment_set']),
                   deployment_set_sha256=raw['deployment_set_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'deployment_set': self.deployment_set.canonical_value(),
            'deployment_set_sha256': self.deployment_set_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ApprovedAuthorityDeploymentSelectionV1(CanonicalContract):
    """Exact deployment-set choice for each independently rotating role."""

    api_deployment_set_sha256: str
    ordinary_executor_deployment_set_sha256: str
    controller_deployment_set_sha256: str
    authority_cohort_deployment_set_sha256: str

    _FIELDS: ClassVar[tuple[str,
                            ...]] = ('api_deployment_set_sha256',
                                     'ordinary_executor_deployment_set_sha256',
                                     'controller_deployment_set_sha256',
                                     'authority_cohort_deployment_set_sha256')
    _KEYS: ClassVar[frozenset[str]] = frozenset(_FIELDS)

    def __post_init__(self) -> None:
        for field in self._FIELDS:
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field), name=f'selection.{field}'))

    @property
    def sort_key(self) -> tuple[str, str, str, str]:
        return (self.api_deployment_set_sha256,
                self.ordinary_executor_deployment_set_sha256,
                self.controller_deployment_set_sha256,
                self.authority_cohort_deployment_set_sha256)

    @classmethod
    def from_value(cls, value: Any) -> ApprovedAuthorityDeploymentSelectionV1:
        return cls(**_closed_object(
            value, name='approved deployment selection', keys=cls._KEYS))

    def canonical_value(self) -> JsonObject:
        return {field: getattr(self, field) for field in self._FIELDS}

    def deployment_set_sha256_for_role(self, role: ApprovedRole) -> str:
        """Return the deployment set selected for one ordinary role."""

        parsed_role = _enum_value(ApprovedRole,
                                  role,
                                  name='selection lookup role')
        return {
            ApprovedRole.API: self.api_deployment_set_sha256,
            ApprovedRole.ORDINARY_EXECUTOR:
                self.ordinary_executor_deployment_set_sha256,
            ApprovedRole.CONTROLLER: self.controller_deployment_set_sha256,
        }[parsed_role]


@dataclasses.dataclass(frozen=True)
class ResourceActionDeploymentCompatibilityInventoryV1(CanonicalContract):
    """Complete finite Cartesian inventory for one or two deployment sets."""

    version: int
    selections: tuple[ApprovedAuthorityDeploymentSelectionV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({'version', 'selections'})

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('compatibility inventory version must be '
                             'integer 1.')
        if (type(self.selections) is not tuple or
                len(self.selections) not in (1, 16) or any(
                    type(item) is not ApprovedAuthorityDeploymentSelectionV1
                    for item in self.selections)):
            raise ValueError('compatibility inventory must contain exactly '
                             '1 or 16 typed selections.')
        keys = tuple(item.sort_key for item in self.selections)
        if keys != tuple(sorted(set(keys))):
            raise ValueError('compatibility selections must be '
                             'lexicographically sorted and distinct.')
        _ = self.canonical_bytes

    @classmethod
    def for_deployment_set_hashes(
        cls, deployment_set_hashes: tuple[str, ...]
    ) -> ResourceActionDeploymentCompatibilityInventoryV1:
        if type(deployment_set_hashes) is not tuple:
            raise TypeError('deployment-set hashes must be a tuple.')
        hashes = tuple(
            _sha256(value, name='compatibility deployment-set hash')
            for value in deployment_set_hashes)
        if (len(hashes) not in (1, 2) or hashes != tuple(sorted(set(hashes)))):
            raise ValueError('compatibility requires one or two sorted '
                             'distinct deployment-set hashes.')
        return cls(version=1,
                   selections=tuple(
                       ApprovedAuthorityDeploymentSelectionV1(*selection)
                       for selection in itertools.product(hashes, repeat=4)))

    def validate_deployment_set_hashes(
            self, deployment_set_hashes: tuple[str, ...]) -> None:
        expected = self.for_deployment_set_hashes(deployment_set_hashes)
        if self.canonical_bytes != expected.canonical_bytes:
            raise ValueError('compatibility inventory is not the complete '
                             'deployment-set Cartesian product.')

    @classmethod
    def from_value(
            cls,
            value: Any) -> ResourceActionDeploymentCompatibilityInventoryV1:
        raw = _closed_object(value,
                             name='deployment compatibility inventory',
                             keys=cls._KEYS)
        if type(raw['selections']) is not list:
            raise TypeError('compatibility selections must be a list.')
        return cls(version=raw['version'],
                   selections=tuple(
                       ApprovedAuthorityDeploymentSelectionV1.from_value(item)
                       for item in raw['selections']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'selections': [item.canonical_value() for item in self.selections],
        }


@dataclasses.dataclass(frozen=True)
class ResourceActionQualificationPolicyV2(CanonicalContract):
    """Closed Serve039/040 authority trust and compatibility policy."""

    version: int
    api_requests_head: str
    serve_head: str
    global_user_state_head: str
    candidate_minimum_seconds: int
    minimum_clean_launches: int
    minimum_clean_downs: int
    approved_deployment_sets: tuple[ApprovedAuthorityDeploymentSetBindingV1,
                                    ...]
    elected_deployment_set_sha256: str
    rollback_deployment_set_sha256: str
    deployment_compatibility_inventory: ResourceActionDeploymentCompatibilityInventoryV1
    deployment_compatibility_inventory_sha256: str
    crash_canary_inventory_contract: str
    required_crash_canary_inventory_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'api_requests_head', 'serve_head', 'global_user_state_head',
        'candidate_minimum_seconds', 'minimum_clean_launches',
        'minimum_clean_downs', 'approved_deployment_sets',
        'elected_deployment_set_sha256', 'rollback_deployment_set_sha256',
        'deployment_compatibility_inventory',
        'deployment_compatibility_inventory_sha256',
        'crash_canary_inventory_contract',
        'required_crash_canary_inventory_sha256'
    })

    def __post_init__(self) -> None:
        fixed = ((self.version, 2, 'version'),
                 (self.candidate_minimum_seconds, 86_400,
                  'candidate_minimum_seconds'), (self.minimum_clean_launches,
                                                 100, 'minimum_clean_launches'),
                 (self.minimum_clean_downs, 100, 'minimum_clean_downs'))
        for actual, expected, name in fixed:
            if type(actual) is not int or actual != expected:
                raise ValueError(f'V2 qualification policy {name} must equal '
                                 f'{expected}.')
        AuthoritySchemaHeadsV2(
            api_requests_head=self.api_requests_head,
            serve_head=self.serve_head,
            global_user_state_head=self.global_user_state_head)
        if (type(self.approved_deployment_sets) is not tuple or
                not 1 <= len(self.approved_deployment_sets) <= 2 or any(
                    type(item) is not ApprovedAuthorityDeploymentSetBindingV1
                    for item in self.approved_deployment_sets)):
            raise ValueError('V2 policy must contain one or two typed '
                             'deployment sets.')
        set_hashes = tuple(item.deployment_set_sha256
                           for item in self.approved_deployment_sets)
        if set_hashes != tuple(sorted(set(set_hashes))):
            raise ValueError('V2 policy deployment sets must be sorted and '
                             'distinct.')
        elected = _sha256(self.elected_deployment_set_sha256,
                          name='policy.elected_deployment_set_sha256')
        rollback = _sha256(self.rollback_deployment_set_sha256,
                           name='policy.rollback_deployment_set_sha256')
        object.__setattr__(self, 'elected_deployment_set_sha256', elected)
        object.__setattr__(self, 'rollback_deployment_set_sha256', rollback)
        if elected not in set_hashes or rollback not in set_hashes:
            raise ValueError('V2 policy elected/rollback set is not approved.')
        if ((len(set_hashes) == 1 and elected != rollback) or
            (len(set_hashes) == 2 and elected == rollback)):
            raise ValueError('V2 policy elected/rollback shape does not match '
                             'its deployment-set cardinality.')
        if type(self.deployment_compatibility_inventory
               ) is not ResourceActionDeploymentCompatibilityInventoryV1:
            raise TypeError('V2 policy compatibility inventory is invalid.')
        compatibility_digest = _sha256(
            self.deployment_compatibility_inventory_sha256,
            name='policy.deployment_compatibility_inventory_sha256')
        if compatibility_digest != self.deployment_compatibility_inventory.sha256:
            raise ValueError('V2 policy compatibility digest differs.')
        object.__setattr__(self, 'deployment_compatibility_inventory_sha256',
                           compatibility_digest)
        self.deployment_compatibility_inventory.validate_deployment_set_hashes(
            set_hashes)
        if (type(self.crash_canary_inventory_contract) is not str or
                self.crash_canary_inventory_contract
                != 'resource_action_crash_canary_inventory_v1'):
            raise ValueError('V2 policy crash inventory contract is '
                             'unsupported.')
        object.__setattr__(
            self, 'required_crash_canary_inventory_sha256',
            _sha256(self.required_crash_canary_inventory_sha256,
                    name='policy.required_crash_canary_inventory_sha256'))
        _ = self.canonical_bytes

    @property
    def schema_heads(self) -> AuthoritySchemaHeadsV2:
        return AuthoritySchemaHeadsV2(
            api_requests_head=self.api_requests_head,
            serve_head=self.serve_head,
            global_user_state_head=self.global_user_state_head)

    def deployment_set_by_hash(
            self,
            deployment_set_sha256: str) -> ApprovedAuthorityDeploymentSetV1:
        digest = _sha256(deployment_set_sha256,
                         name='policy deployment-set lookup digest')
        matches = tuple(item.deployment_set
                        for item in self.approved_deployment_sets
                        if item.deployment_set_sha256 == digest)
        if len(matches) != 1:
            raise ValueError('policy deployment-set digest is not unique.')
        return matches[0]

    def validate_deployment_selection(
            self, selection: ApprovedAuthorityDeploymentSelectionV1) -> None:
        """Require one exact member of the finite compatibility inventory."""

        if type(selection) is not ApprovedAuthorityDeploymentSelectionV1:
            raise TypeError('policy deployment selection is not typed.')
        if not any(
                selection.canonical_bytes == item.canonical_bytes
                for item in self.deployment_compatibility_inventory.selections):
            raise ValueError('deployment selection is not approved by policy.')

    def validate_deployment_inventory(
            self, selection: ApprovedAuthorityDeploymentSelectionV1,
            inventory: ResourceActionDeploymentInventoryV1) -> None:
        """Resolve every observed ordinary role through its selected set."""

        self.validate_deployment_selection(selection)
        if type(inventory) is not ResourceActionDeploymentInventoryV1:
            raise TypeError('policy deployment inventory is not typed.')
        for deployment in inventory.deployments:
            deployment_set = self.deployment_set_by_hash(
                selection.deployment_set_sha256_for_role(deployment.role))
            image_matches = tuple(image for image in deployment_set.role_images
                                  if image.role is deployment.role)
            if len(image_matches) != 1:
                raise ValueError('selected deployment set has a crossed role.')
            deployment.validate_approved_image(image_matches[0])

    def validate_selected_cohort(
            self, selection: ApprovedAuthorityDeploymentSelectionV1,
            cohort: ApprovedAuthorityCohortArtifactV1) -> None:
        """Resolve the authority cohort through its independently selected set."""

        self.validate_deployment_selection(selection)
        if type(cohort) is not ApprovedAuthorityCohortArtifactV1:
            raise TypeError('policy selected cohort is not typed.')
        deployment_set = self.deployment_set_by_hash(
            selection.authority_cohort_deployment_set_sha256)
        if not any(cohort.canonical_bytes == item.canonical_bytes
                   for item in deployment_set.approved_cohorts):
            raise ValueError('selected cohort is not approved by its '
                             'deployment set.')

    @classmethod
    def from_value(cls, value: Any) -> ResourceActionQualificationPolicyV2:
        raw = _closed_object(value,
                             name='V2 resource-action qualification policy',
                             keys=cls._KEYS)
        if type(raw['approved_deployment_sets']) is not list:
            raise TypeError('V2 policy deployment sets must be a list.')
        return cls(
            version=raw['version'],
            api_requests_head=raw['api_requests_head'],
            serve_head=raw['serve_head'],
            global_user_state_head=raw['global_user_state_head'],
            candidate_minimum_seconds=raw['candidate_minimum_seconds'],
            minimum_clean_launches=raw['minimum_clean_launches'],
            minimum_clean_downs=raw['minimum_clean_downs'],
            approved_deployment_sets=tuple(
                ApprovedAuthorityDeploymentSetBindingV1.from_value(item)
                for item in raw['approved_deployment_sets']),
            elected_deployment_set_sha256=raw['elected_deployment_set_sha256'],
            rollback_deployment_set_sha256=raw[
                'rollback_deployment_set_sha256'],
            deployment_compatibility_inventory=
            ResourceActionDeploymentCompatibilityInventoryV1.from_value(
                raw['deployment_compatibility_inventory']),
            deployment_compatibility_inventory_sha256=raw[
                'deployment_compatibility_inventory_sha256'],
            crash_canary_inventory_contract=raw[
                'crash_canary_inventory_contract'],
            required_crash_canary_inventory_sha256=raw[
                'required_crash_canary_inventory_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'api_requests_head': self.api_requests_head,
            'serve_head': self.serve_head,
            'global_user_state_head': self.global_user_state_head,
            'candidate_minimum_seconds': 86_400,
            'minimum_clean_launches': 100,
            'minimum_clean_downs': 100,
            'approved_deployment_sets': [
                item.canonical_value() for item in self.approved_deployment_sets
            ],
            'elected_deployment_set_sha256': self.elected_deployment_set_sha256,
            'rollback_deployment_set_sha256':
                self.rollback_deployment_set_sha256,
            'deployment_compatibility_inventory':
                self.deployment_compatibility_inventory.canonical_value(),
            'deployment_compatibility_inventory_sha256':
                self.deployment_compatibility_inventory_sha256,
            'crash_canary_inventory_contract': 'resource_action_crash_canary_inventory_v1',
            'required_crash_canary_inventory_sha256':
                self.required_crash_canary_inventory_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ResourceActionCandidateBindingV2(CanonicalContract):
    """Complete Serve036/037 qualification preimage for one service."""

    version: int
    qualification_policy_sha256: str
    schema_heads: AuthoritySchemaHeadsV2
    deployment_inventory: ResourceActionDeploymentInventoryV1
    deployment_inventory_sha256: str
    deployment_selection: ApprovedAuthorityDeploymentSelectionV1
    deployment_selection_sha256: str
    selected_cohort: ApprovedAuthorityCohortArtifactV1
    selected_cohort_sha256: str
    capacity_profile: resource_actions.ServeActionCapacityProfileV1
    capacity_profile_sha256: str
    elected_version_identity: resource_actions.ServeServiceVersionSpecIdentityV1
    elected_version_identity_sha256: str
    live_replica_identity_inventory: HashedCanonicalObjectV1
    required_crash_canary_inventory: ResourceActionRequiredCrashCanaryInventoryV1
    required_crash_canary_inventory_sha256: str
    _live_replica_identity_inventory_snapshot: bytes = dataclasses.field(
        init=False, repr=False, compare=False)

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'qualification_policy_sha256', 'schema_heads',
        'deployment_inventory', 'deployment_inventory_sha256',
        'deployment_selection', 'deployment_selection_sha256',
        'selected_cohort', 'selected_cohort_sha256', 'capacity_profile',
        'capacity_profile_sha256', 'elected_version_identity',
        'elected_version_identity_sha256', 'live_replica_identity_inventory',
        'required_crash_canary_inventory',
        'required_crash_canary_inventory_sha256'
    })

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError('candidate binding version must be integer 2.')
        object.__setattr__(
            self, 'qualification_policy_sha256',
            _sha256(self.qualification_policy_sha256,
                    name='candidate_binding.qualification_policy_sha256'))
        if type(self.schema_heads) is not AuthoritySchemaHeadsV2:
            raise TypeError('V2 candidate binding schema heads are invalid.')
        if type(self.deployment_inventory
               ) is not ResourceActionDeploymentInventoryV1:
            raise TypeError('V2 candidate deployment inventory is invalid.')
        self._require_nested_hash('deployment_inventory')
        if type(self.deployment_selection
               ) is not ApprovedAuthorityDeploymentSelectionV1:
            raise TypeError('V2 candidate deployment selection is invalid.')
        self._require_nested_hash('deployment_selection')
        if type(self.selected_cohort) is not ApprovedAuthorityCohortArtifactV1:
            raise TypeError('V2 candidate selected cohort is invalid.')
        self._require_nested_hash('selected_cohort')
        if type(self.capacity_profile
               ) is not resource_actions.ServeActionCapacityProfileV1:
            raise TypeError('V2 candidate capacity profile is invalid.')
        expected_profile = (resource_actions.ServeActionCapacityProfileV1.
                            ordinary_ondemand_physical_width1())
        if self.capacity_profile.canonical_bytes != expected_profile.canonical_bytes:
            raise ValueError('V2 candidate capacity profile is not the '
                             'closed M4 profile.')
        self._require_nested_hash('capacity_profile')
        if type(self.elected_version_identity
               ) is not resource_actions.ServeServiceVersionSpecIdentityV1:
            raise TypeError('V2 candidate elected identity is invalid.')
        if (self.elected_version_identity.capacity_profile.canonical_bytes
                != self.capacity_profile.canonical_bytes or
                self.elected_version_identity.provider_profile
                != 'pod_cluster_v1'):
            raise ValueError('V2 candidate elected identity is outside the '
                             'closed provider/capacity profile.')
        self._require_nested_hash('elected_version_identity')
        if type(self.live_replica_identity_inventory
               ) is not HashedCanonicalObjectV1:
            raise TypeError('V2 candidate live-replica inventory is invalid.')
        live_inventory = HashedCanonicalObjectV1.from_value(
            self.live_replica_identity_inventory.canonical_value())
        object.__setattr__(self, 'live_replica_identity_inventory',
                           live_inventory)
        object.__setattr__(self, '_live_replica_identity_inventory_snapshot',
                           live_inventory.canonical_bytes)
        if type(self.required_crash_canary_inventory
               ) is not ResourceActionRequiredCrashCanaryInventoryV1:
            raise TypeError('V2 candidate crash inventory is invalid.')
        self._require_nested_hash('required_crash_canary_inventory')
        _ = self.canonical_bytes

    def _require_nested_hash(self, field: str) -> None:
        value = getattr(self, field)
        digest_field = f'{field}_sha256'
        digest = _sha256(getattr(self, digest_field),
                         name=f'candidate_binding.{digest_field}')
        if digest != value.sha256:
            raise ValueError(f'V2 candidate {field} digest differs.')
        object.__setattr__(self, digest_field, digest)

    def _validated_live_replica_identity_inventory(
            self) -> HashedCanonicalObjectV1:
        current = HashedCanonicalObjectV1.from_value(
            self.live_replica_identity_inventory.canonical_value())
        if current.canonical_bytes != self._live_replica_identity_inventory_snapshot:
            raise ValueError('V2 candidate live-replica inventory was '
                             'mutated after construction.')
        return current

    def validate_for_policy(
            self, policy: ResourceActionQualificationPolicyV2) -> None:
        """Cross-check every policy-derived candidate input."""

        if type(policy) is not ResourceActionQualificationPolicyV2:
            raise TypeError('V2 candidate policy is not typed.')
        self._validated_live_replica_identity_inventory()
        if self.qualification_policy_sha256 != policy.sha256:
            raise ValueError('V2 candidate policy digest differs.')
        if self.schema_heads.canonical_bytes != policy.schema_heads.canonical_bytes:
            raise ValueError('V2 candidate schema heads differ from policy.')
        policy.validate_deployment_inventory(self.deployment_selection,
                                             self.deployment_inventory)
        policy.validate_selected_cohort(self.deployment_selection,
                                        self.selected_cohort)
        if (policy.crash_canary_inventory_contract
                != self.required_crash_canary_inventory.contract or
                policy.required_crash_canary_inventory_sha256
                != self.required_crash_canary_inventory.sha256):
            raise ValueError('V2 candidate crash inventory differs from '
                             'policy.')

    @classmethod
    def from_value(cls, value: Any) -> ResourceActionCandidateBindingV2:
        live_inventory = None
        if (type(value) is dict and all(type(key) is str for key in value) and
                set(value) == cls._KEYS):
            live_inventory = HashedCanonicalObjectV1.from_value(
                value['live_replica_identity_inventory'])
        raw = _closed_object(value,
                             name='V2 resource-action candidate binding',
                             keys=cls._KEYS)
        assert live_inventory is not None
        return cls(
            version=raw['version'],
            qualification_policy_sha256=raw['qualification_policy_sha256'],
            schema_heads=AuthoritySchemaHeadsV2.from_value(raw['schema_heads']),
            deployment_inventory=ResourceActionDeploymentInventoryV1.from_value(
                raw['deployment_inventory']),
            deployment_inventory_sha256=raw['deployment_inventory_sha256'],
            deployment_selection=ApprovedAuthorityDeploymentSelectionV1.
            from_value(raw['deployment_selection']),
            deployment_selection_sha256=raw['deployment_selection_sha256'],
            selected_cohort=ApprovedAuthorityCohortArtifactV1.from_value(
                raw['selected_cohort']),
            selected_cohort_sha256=raw['selected_cohort_sha256'],
            capacity_profile=resource_actions.ServeActionCapacityProfileV1.
            from_value(raw['capacity_profile']),
            capacity_profile_sha256=raw['capacity_profile_sha256'],
            elected_version_identity=resource_actions.
            ServeServiceVersionSpecIdentityV1.from_value(
                raw['elected_version_identity']),
            elected_version_identity_sha256=raw[
                'elected_version_identity_sha256'],
            live_replica_identity_inventory=live_inventory,
            required_crash_canary_inventory=
            ResourceActionRequiredCrashCanaryInventoryV1.from_value(
                raw['required_crash_canary_inventory']),
            required_crash_canary_inventory_sha256=raw[
                'required_crash_canary_inventory_sha256'])

    def canonical_value(self) -> JsonObject:
        live_inventory = self._validated_live_replica_identity_inventory()
        return {
            'version': 2,
            'qualification_policy_sha256': self.qualification_policy_sha256,
            'schema_heads': self.schema_heads.canonical_value(),
            'deployment_inventory': self.deployment_inventory.canonical_value(),
            'deployment_inventory_sha256': self.deployment_inventory_sha256,
            'deployment_selection': self.deployment_selection.canonical_value(),
            'deployment_selection_sha256': self.deployment_selection_sha256,
            'selected_cohort': self.selected_cohort.canonical_value(),
            'selected_cohort_sha256': self.selected_cohort_sha256,
            'capacity_profile': self.capacity_profile.canonical_value(),
            'capacity_profile_sha256': self.capacity_profile_sha256,
            'elected_version_identity':
                self.elected_version_identity.canonical_value(),
            'elected_version_identity_sha256':
                self.elected_version_identity_sha256,
            'live_replica_identity_inventory': live_inventory.canonical_value(),
            'required_crash_canary_inventory':
                self.required_crash_canary_inventory.canonical_value(),
            'required_crash_canary_inventory_sha256':
                self.required_crash_canary_inventory_sha256,
        }
