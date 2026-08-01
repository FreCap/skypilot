"""Pure versioned contracts for durable SkyServe resource actions.

This module owns the bounded SkyServe/provider values persisted by the shadow
journal and, later, the authoritative action adapter.  It deliberately has no
database, SDK, provisioner, or provider imports.  Canonical bytes, hashes, and
logical action IDs reuse the generic resource-action kernel contract.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
import dataclasses
import datetime
import enum
import json
import re
import types
from typing import Any, ClassVar, TypeVar
import uuid

from sky.container_images import models as container_image_models
from sky.server.requests import resource_actions as kernel_actions
from sky.utils import common_utils

_MAX_OBJECT_BYTES = 65_536
_MAX_TEXT_BYTES = 1_024
_MAX_SHORT_TEXT_BYTES = 253
_MAX_CA_CERT_DER_BASE64_BYTES = 16_384
_MAX_CA_CERT_DER_BYTES = 12_288
_MAX_SERVICE_NAME_BYTES = 256
_MAX_REQUEST_ID_BYTES = 128
_MAX_LIST_ITEMS = 256
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_SHA256_DIGEST_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
_DNS_LABEL_RE = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')
_MAX_POSTGRES_BIGINT = 2**63 - 1
_MAX_POSTGRES_INTEGER = 2**31 - 1
_UTC_TIMESTAMP_RE = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:'
                               r'[0-9]{2}\.[0-9]{6}Z$')
_DECIMAL_INTEGER_RE = re.compile(r'^(0|[1-9][0-9]*)$')
_DECIMAL_PORT_RE = re.compile(r'^[1-9][0-9]{0,4}$')
WORKER_REGISTRATION_MAX_AGE = datetime.timedelta(minutes=5)

PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1 = (
    'serve_shadow_candidate_launch',
    'serve_shadow_candidate_down',
    'serve_resource_action_launch',
    'serve_resource_action_down',
)

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


class NormalizationOutcome(str, enum.Enum):
    """Closed result persisted for every admitted normalization decision."""

    REPRESENTABLE = 'REPRESENTABLE'
    NOT_REPRESENTABLE = 'NOT_REPRESENTABLE'


class ProviderLaunchNotRepresentableReasonV1(str, enum.Enum):
    """Closed launch failures, declared in deterministic precedence order."""

    REQUEST_CONTRACT = 'request_contract'
    SECRET_OR_TLS_MATERIAL = 'secret_or_tls_material'
    SOURCE_MISMATCH = 'source_mismatch'
    POLICY_CONFIGURED_OR_MUTATED = 'policy_configured_or_mutated'
    MANAGED_SECRETS = 'managed_secrets'
    MULTI_TASK = 'multi_task'
    MULTI_NODE = 'multi_node'
    MULTI_RESOURCE = 'multi_resource'
    MOUNT_OR_STORAGE = 'mount_or_storage'
    NON_KUBERNETES = 'non_kubernetes'
    SPOT = 'spot'
    NON_DIRECT_POD_TOPOLOGY = 'non_direct_pod_topology'
    PORT_CONTRACT = 'port_contract'
    RESERVED_LABEL_COLLISION = 'reserved_label_collision'
    MUTABLE_IMAGE = 'mutable_image'
    CUSTOM_PROVIDER_IMPLEMENTATION = 'custom_provider_implementation'
    PREFLIGHT_UNAVAILABLE_OR_INVALID = 'preflight_unavailable_or_invalid'
    AUTHORITY_WORKER_ATTESTATION = 'authority_worker_attestation'
    AUTHORIZATION_OR_PRINCIPAL_DRIFT = 'authorization_or_principal_drift'
    PREREQUISITE_OR_NETWORK_DRIFT = 'prerequisite_or_network_drift'
    ADMITTED_OBJECT_CONTRACT = 'admitted_object_contract'
    RUNTIME_OR_JOB_CONTRACT = 'runtime_or_job_contract'
    UNREPRESENTED_EXECUTION_CONFIG = 'unrepresented_execution_config'
    UNREPRESENTED_RESOURCE = 'unrepresented_resource'
    UNFROZEN_PLACEMENT = 'unfrozen_placement'
    UNFROZEN_IDENTITY = 'unfrozen_identity'
    UNFROZEN_KUBERNETES_SCOPE = 'unfrozen_kubernetes_scope'
    TARGET_MISMATCH = 'target_mismatch'

    @property
    def precedence(self) -> int:
        """Return the zero-based precedence committed by this enum."""

        return tuple(type(self)).index(self)


class ProviderDownNotRepresentableReasonV1(str, enum.Enum):
    """Closed down failures, declared in deterministic precedence order."""

    REQUEST_CONTRACT = 'request_contract'
    PRIOR_LAUNCH_BASIS = 'prior_launch_basis'
    TARGET_MISMATCH = 'target_mismatch'
    PREFLIGHT_UNAVAILABLE_OR_INVALID = 'preflight_unavailable_or_invalid'
    AUTHORITY_WORKER_ATTESTATION = 'authority_worker_attestation'
    AUTHORIZATION_OR_PRINCIPAL_DRIFT = 'authorization_or_principal_drift'
    PREREQUISITE_OR_NETWORK_DRIFT = 'prerequisite_or_network_drift'
    POLICY_CONFIGURED_OR_MUTATED = 'policy_configured_or_mutated'
    UNREPRESENTED_EXECUTION_CONFIG = 'unrepresented_execution_config'
    UNFROZEN_KUBERNETES_SCOPE = 'unfrozen_kubernetes_scope'

    @property
    def precedence(self) -> int:
        """Return the zero-based precedence committed by this enum."""

        return tuple(type(self)).index(self)


PROVIDER_LAUNCH_NOT_REPRESENTABLE_REASON_PRECEDENCE = tuple(
    ProviderLaunchNotRepresentableReasonV1)
PROVIDER_DOWN_NOT_REPRESENTABLE_REASON_PRECEDENCE = tuple(
    ProviderDownNotRepresentableReasonV1)


class WorkerCohortLifecycleState(str, enum.Enum):
    """Closed lifecycle of one immutable authority-worker cohort."""

    REGISTERING = 'REGISTERING'
    ACCEPTING = 'ACCEPTING'
    DRAINING = 'DRAINING'
    REMOVAL_AUTHORIZED = 'REMOVAL_AUTHORIZED'
    RETIRED = 'RETIRED'


class WorkerCohortReferenceState(str, enum.Enum):
    """Closed retention-reference lifecycle for one decision."""

    PREPARING = 'PREPARING'
    SHADOW_ACTIVE = 'SHADOW_ACTIVE'
    ACTION_ACTIVE = 'ACTION_ACTIVE'
    RELEASED = 'RELEASED'


class CoverageAttemptTerminalStatus(str, enum.Enum):
    """Closed terminal status copied from a bound legacy request."""

    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


class CoverageAttemptRetryDisposition(str, enum.Enum):
    """Closed next-step decision for one completed coverage-only request."""

    RETRY_SAME_DECISION = 'RETRY_SAME_DECISION'
    TERMINAL = 'TERMINAL'
    REPLAN_NEW_GENERATION = 'REPLAN_NEW_GENERATION'
    BLOCK = 'BLOCK'


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


# Coverage-only attempts deliberately share the represented ledger's phase
# and role vocabulary.  Aliases prevent the two tables from drifting.
CoverageAttemptPhase = ShadowAttemptPhase
CoverageAttemptRequestRole = ShadowRequestRole


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
    if '\x00' in value:
        raise ValueError(f'{name} cannot contain U+0000.')
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


def _decimal_port_text(value: Any, *, name: str) -> str:
    """Validate canonical decimal text for a workload or management port."""

    port = _text(value, name=name, maximum_bytes=_MAX_SHORT_TEXT_BYTES)
    if (_DECIMAL_PORT_RE.fullmatch(port) is None or int(port) > 65_535):
        raise ValueError(
            f'{name} must be canonical decimal port text in 1..65535.')
    return port


def _sha256(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be lowercase SHA-256 hex.')
    return value


def _sha256_digest(value: Any, *, name: str) -> str:
    if (not isinstance(value, str) or
            _SHA256_DIGEST_RE.fullmatch(value) is None):
        raise ValueError(f'{name} must be sha256:<64 lowercase hex>.')
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


def _schema_uuid(value: Any, *, name: str) -> uuid.UUID:
    """Return the UUID subset accepted by Serve033 text CHECK constraints."""
    parsed = _uuid(value, name=name)
    if (parsed.variant != uuid.RFC_4122 or parsed.version is None or
            parsed.version < 1 or parsed.version > 5):
        raise ValueError(f'{name} must be an RFC 4122 version 1..5 UUID.')
    return parsed


def _dns_label(value: Any, *, name: str) -> str:
    normalized = _text(value, name=name, maximum_bytes=_MAX_TEXT_BYTES)
    if _DNS_LABEL_RE.fullmatch(normalized) is None:
        raise ValueError(f'{name} must be a DNS label.')
    return normalized


def _nonnegative_integer(value: Any,
                         *,
                         name: str,
                         maximum: int = _MAX_POSTGRES_BIGINT) -> int:
    if (not isinstance(value, int) or isinstance(value, bool) or value < 0 or
            value > maximum):
        raise ValueError(
            f'{name} must be a nonnegative integer no greater than {maximum}.')
    return value


def _positive_integer(value: Any,
                      *,
                      name: str,
                      maximum: int = _MAX_POSTGRES_BIGINT) -> int:
    if (not isinstance(value, int) or isinstance(value, bool) or value <= 0 or
            value > maximum):
        raise ValueError(
            f'{name} must be a positive integer no greater than {maximum}.')
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


def _action_kind(value: Any, *, name: str) -> kernel_actions.ActionKind:
    if not isinstance(value, (str, kernel_actions.ActionKind)):
        raise TypeError(f'{name} must be text.')
    try:
        return (value if isinstance(value, kernel_actions.ActionKind) else
                kernel_actions.ActionKind(value))
    except ValueError as e:
        raise ValueError(f'{name} is unsupported.') from e


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


def _canonical_der_base64(value: Any, *, name: str) -> str:
    """Validate one canonical RFC 4648 encoding of nonempty DER bytes."""

    encoded = _text(value,
                    name=name,
                    maximum_bytes=_MAX_CA_CERT_DER_BASE64_BYTES)
    if not encoded.isascii() or len(encoded) < 4:
        raise ValueError(f'{name} must be 4..'
                         f'{_MAX_CA_CERT_DER_BASE64_BYTES} ASCII bytes.')
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as e:
        raise ValueError(f'{name} must be canonical RFC 4648 base64.') from e
    if (not decoded or len(decoded) > _MAX_CA_CERT_DER_BYTES or
            base64.b64encode(decoded).decode('ascii') != encoded):
        raise ValueError(f'{name} must be canonical RFC 4648 base64 of '
                         '1..12288 DER bytes.')
    return encoded


@dataclasses.dataclass(frozen=True)
class _ProviderKubernetesServerOriginV1(_CanonicalContract):
    """Closed normalized server-origin component of a frozen transport."""

    scheme: str
    host: str
    port: int
    path: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'scheme', 'host', 'port', 'path'})

    def __post_init__(self) -> None:
        if self.scheme != 'https':
            raise ValueError('kubernetes server origin scheme must be https.')
        object.__setattr__(
            self, 'host',
            _text(self.host, name='kubernetes.transport.server_origin.host'))
        object.__setattr__(
            self, 'port',
            _positive_integer(self.port,
                              name='kubernetes.transport.server_origin.port'))
        object.__setattr__(
            self, 'path',
            _text(self.path, name='kubernetes.transport.server_origin.path'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> '_ProviderKubernetesServerOriginV1':
        raw = _closed_object(value,
                             name='kubernetes server origin',
                             keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'scheme': 'https',
            'host': self.host,
            'port': self.port,
            'path': self.path,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesTransportIdentityV1(_CanonicalContract):
    """Bounded nonsecret transport identity for one Kubernetes API server."""

    version: int
    server_origin: _ProviderKubernetesServerOriginV1
    tls_server_name: str | None
    ca_cert_der_base64: tuple[str, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'server_origin', 'tls_server_name', 'ca_cert_der_base64'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='kubernetes transport version')
        if not isinstance(self.server_origin,
                          _ProviderKubernetesServerOriginV1):
            raise TypeError('kubernetes transport server origin has an '
                            'invalid type.')
        object.__setattr__(
            self, 'tls_server_name',
            _optional_text(self.tls_server_name,
                           name='kubernetes.transport.tls_server_name'))
        if (not isinstance(self.ca_cert_der_base64, tuple) or
                not 1 <= len(self.ca_cert_der_base64) <= _MAX_LIST_ITEMS):
            raise ValueError('kubernetes transport CA certificates must be a '
                             'tuple of 1..256 values.')
        certificates = tuple(
            _canonical_der_base64(
                certificate,
                name=f'kubernetes.transport.ca_cert_der_base64[{index}]')
            for index, certificate in enumerate(self.ca_cert_der_base64))
        if certificates != tuple(sorted(set(certificates))):
            raise ValueError('kubernetes transport CA certificates must be '
                             'sorted and duplicate-free.')
        object.__setattr__(self, 'ca_cert_der_base64', certificates)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesTransportIdentityV1':
        raw = _closed_object(value,
                             name='kubernetes transport identity',
                             keys=cls._KEYS)
        certificates = raw['ca_cert_der_base64']
        if not isinstance(certificates, list):
            raise TypeError('kubernetes transport CA certificates must be a '
                            'list.')
        return cls(version=raw['version'],
                   server_origin=_ProviderKubernetesServerOriginV1.from_value(
                       raw['server_origin']),
                   tls_server_name=raw['tls_server_name'],
                   ca_cert_der_base64=tuple(certificates))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'server_origin': self.server_origin.canonical_value(),
            'tls_server_name': self.tls_server_name,
            'ca_cert_der_base64': list(self.ca_cert_der_base64),
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesScopeV1(_CanonicalContract):
    """Frozen bounded identity derived with one live Kubernetes client."""

    version: int
    context_name: str
    context_identity: tuple[str, ...]
    in_cluster: bool
    namespace: str
    transport: ProviderKubernetesTransportIdentityV1
    kube_system_namespace_uid: str
    target_namespace_uid: str
    api_server_git_version: str
    caller_service_account_namespace: str
    caller_service_account_name: str
    caller_service_account_uid: str
    workload_service_account_namespace: str
    workload_service_account_name: str
    workload_service_account_uid: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'context_name', 'context_identity', 'in_cluster',
        'namespace', 'transport', 'kube_system_namespace_uid',
        'target_namespace_uid', 'api_server_git_version',
        'caller_service_account_namespace', 'caller_service_account_name',
        'caller_service_account_uid', 'workload_service_account_namespace',
        'workload_service_account_name', 'workload_service_account_uid'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='kubernetes scope version')
        for field in ('context_name', 'kube_system_namespace_uid',
                      'target_namespace_uid', 'api_server_git_version',
                      'caller_service_account_name',
                      'caller_service_account_uid',
                      'workload_service_account_name',
                      'workload_service_account_uid'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field), name=f'kubernetes.scope.{field}'))
        for field in ('namespace', 'caller_service_account_namespace',
                      'workload_service_account_namespace'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'kubernetes.scope.{field}',
                      maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        if (not isinstance(self.context_identity, tuple) or
                not 1 <= len(self.context_identity) <= _MAX_LIST_ITEMS):
            raise ValueError('kubernetes scope context_identity must be a '
                             'tuple of 1..256 values.')
        identity = tuple(
            _text(item, name=f'kubernetes.scope.context_identity[{index}]')
            for index, item in enumerate(self.context_identity))
        object.__setattr__(self, 'context_identity', identity)
        _boolean(self.in_cluster, name='kubernetes.scope.in_cluster')
        if not isinstance(self.transport,
                          ProviderKubernetesTransportIdentityV1):
            raise TypeError('kubernetes scope transport has an invalid type.')
        if ((self.namespace == 'kube-system') != (
                self.target_namespace_uid == self.kube_system_namespace_uid)):
            raise ValueError('kubernetes target namespace name and namespace '
                             'UIDs contradict one another.')
        if self.workload_service_account_namespace != self.namespace:
            raise ValueError('kubernetes workload service-account namespace '
                             'must equal the target namespace.')
        caller_name = (self.caller_service_account_namespace,
                       self.caller_service_account_name)
        workload_name = (self.workload_service_account_namespace,
                         self.workload_service_account_name)
        if ((caller_name == workload_name)
                != (self.caller_service_account_uid ==
                    self.workload_service_account_uid)):
            raise ValueError('kubernetes service-account names and UIDs '
                             'contradict one another.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesScopeV1':
        raw = _closed_object(value,
                             name='kubernetes scope identity',
                             keys=cls._KEYS)
        context_identity = raw['context_identity']
        if not isinstance(context_identity, list):
            raise TypeError('kubernetes scope context_identity must be a list.')
        return cls(
            version=raw['version'],
            context_name=raw['context_name'],
            context_identity=tuple(context_identity),
            in_cluster=raw['in_cluster'],
            namespace=raw['namespace'],
            transport=ProviderKubernetesTransportIdentityV1.from_value(
                raw['transport']),
            kube_system_namespace_uid=raw['kube_system_namespace_uid'],
            target_namespace_uid=raw['target_namespace_uid'],
            api_server_git_version=raw['api_server_git_version'],
            caller_service_account_namespace=raw[
                'caller_service_account_namespace'],
            caller_service_account_name=raw['caller_service_account_name'],
            caller_service_account_uid=raw['caller_service_account_uid'],
            workload_service_account_namespace=raw[
                'workload_service_account_namespace'],
            workload_service_account_name=raw['workload_service_account_name'],
            workload_service_account_uid=raw['workload_service_account_uid'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'context_name': self.context_name,
            'context_identity': list(self.context_identity),
            'in_cluster': self.in_cluster,
            'namespace': self.namespace,
            'transport': self.transport.canonical_value(),
            'kube_system_namespace_uid': self.kube_system_namespace_uid,
            'target_namespace_uid': self.target_namespace_uid,
            'api_server_git_version': self.api_server_git_version,
            'caller_service_account_namespace':
                self.caller_service_account_namespace,
            'caller_service_account_name': self.caller_service_account_name,
            'caller_service_account_uid': self.caller_service_account_uid,
            'workload_service_account_namespace':
                self.workload_service_account_namespace,
            'workload_service_account_name': self.workload_service_account_name,
            'workload_service_account_uid': self.workload_service_account_uid,
        }


class ProviderKubernetesScopeReadDispositionV1(str, enum.Enum):
    """Closed outcomes of one Kubernetes scope identity read."""

    COMPLETE = 'complete'
    NOT_FOUND = 'not_found'
    FORBIDDEN = 'forbidden'
    TIMEOUT = 'timeout'
    TRANSPORT_ERROR = 'transport_error'
    MALFORMED = 'malformed'


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesScopeReadV1(_CanonicalContract):
    """Typed success or failure evidence from one Kubernetes scope read."""

    disposition: ProviderKubernetesScopeReadDispositionV1
    scope: ProviderKubernetesScopeV1 | None
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'disposition', 'scope', 'observed_at'})

    def __post_init__(self) -> None:
        disposition = _enum_value(ProviderKubernetesScopeReadDispositionV1,
                                  self.disposition,
                                  name='kubernetes scope read disposition')
        object.__setattr__(self, 'disposition', disposition)
        if self.scope is not None and not isinstance(self.scope,
                                                     ProviderKubernetesScopeV1):
            raise TypeError('kubernetes scope read scope has an invalid type.')
        if ((disposition is ProviderKubernetesScopeReadDispositionV1.COMPLETE)
                != (self.scope is not None)):
            raise ValueError('complete Kubernetes scope reads require a scope; '
                             'failed reads require null.')
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at,
                       name='kubernetes.scope_read.observed_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesScopeReadV1':
        raw = _closed_object(value,
                             name='kubernetes scope read',
                             keys=cls._KEYS)
        return cls(disposition=raw['disposition'],
                   scope=(None if raw['scope'] is None else
                          ProviderKubernetesScopeV1.from_value(raw['scope'])),
                   observed_at=raw['observed_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'disposition': self.disposition.value,
            'scope': None
                     if self.scope is None else self.scope.canonical_value(),
            'observed_at': self.observed_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderWorkloadNameBasisV1(_CanonicalContract):
    """Frozen inputs for deterministic provider workload names."""

    version: int
    display_name: str
    frozen_user_hash: str
    max_length: int
    cluster_name_hash_length: int

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'display_name', 'frozen_user_hash', 'max_length',
        'cluster_name_hash_length'
    })
    _MAX_LENGTH: ClassVar[int] = 42
    _CLUSTER_NAME_HASH_LENGTH: ClassVar[int] = 8
    # Reserve one display-name character and the two separating dashes when a
    # long display name needs its collision-resistant hash.
    _MAX_FROZEN_USER_HASH_LENGTH: ClassVar[int] = (_MAX_LENGTH -
                                                   _CLUSTER_NAME_HASH_LENGTH -
                                                   3)

    def __post_init__(self) -> None:
        _version_one(self.version, name='workload name basis version')
        object.__setattr__(
            self, 'display_name',
            _text(self.display_name,
                  name='workload_name_basis.display_name',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(
            self, 'frozen_user_hash',
            _text(self.frozen_user_hash,
                  name='workload_name_basis.frozen_user_hash',
                  maximum_bytes=self._MAX_FROZEN_USER_HASH_LENGTH))
        if (not isinstance(self.max_length, int) or
                isinstance(self.max_length, bool) or
                self.max_length != self._MAX_LENGTH):
            raise ValueError('workload name max_length must be integer 42.')
        if (not isinstance(self.cluster_name_hash_length, int) or
                isinstance(self.cluster_name_hash_length, bool) or
                self.cluster_name_hash_length
                != self._CLUSTER_NAME_HASH_LENGTH):
            raise ValueError('workload cluster_name_hash_length must be '
                             'integer 8.')
        provider_cluster_name = self.provider_cluster_name
        _dns_label(provider_cluster_name,
                   name='workload_name_basis.provider_cluster_name')
        _dns_label(self.workload_name, name='workload_name_basis.workload_name')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderWorkloadNameBasisV1':
        raw = _closed_object(value,
                             name='provider workload name basis',
                             keys=cls._KEYS)
        return cls(**raw)

    @property
    def provider_cluster_name(self) -> str:
        """Return the frozen historical SkyPilot cloud-cluster name."""

        return common_utils.make_cluster_name_on_cloud_for_user(
            self.display_name,
            max_length=self.max_length,
            cluster_name_hash_length=self.cluster_name_hash_length,
            user_hash=self.frozen_user_hash)

    @property
    def workload_name(self) -> str:
        """Return the direct-Pod topology's workload name."""

        return f'{self.provider_cluster_name}-head'

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'display_name': self.display_name,
            'frozen_user_hash': self.frozen_user_hash,
            'max_length': self._MAX_LENGTH,
            'cluster_name_hash_length': self._CLUSTER_NAME_HASH_LENGTH,
        }


@dataclasses.dataclass(frozen=True)
class ProviderRepoArtifactRefV1:
    """Content-addressed reference to one checked-in repository artifact."""

    repo_path: str
    byte_size: int
    sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'repo_path', 'byte_size', 'sha256'})

    def __post_init__(self) -> None:
        object.__setattr__(self, 'repo_path',
                           _text(self.repo_path, name='artifact.repo_path'))
        object.__setattr__(
            self, 'byte_size',
            _positive_integer(self.byte_size, name='artifact.byte_size'))
        object.__setattr__(self, 'sha256',
                           _sha256(self.sha256, name='artifact.sha256'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderRepoArtifactRefV1':
        raw = _closed_object(value, name='artifact reference', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)

    @property
    def canonical_bytes(self) -> bytes:
        encoded = canonical_json_bytes(self.canonical_value())
        if len(encoded) > _MAX_OBJECT_BYTES:
            raise ValueError('ProviderRepoArtifactRefV1 exceeds 65536 bytes.')
        return encoded


@dataclasses.dataclass(frozen=True)
class ProviderOCIImageQualificationV1(_CanonicalContract):
    """Digest-qualified immutable OCI image identity."""

    requested_reference: str
    oci_manifest_digest: str
    oci_config_digest: str
    qualification_artifact: ProviderRepoArtifactRefV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'requested_reference', 'oci_manifest_digest', 'oci_config_digest',
        'qualification_artifact'
    })

    def __post_init__(self) -> None:
        reference = _text(self.requested_reference,
                          name='image.requested_reference')
        try:
            canonical_reference = container_image_models.validate_oci_reference(
                reference, 'image.requested_reference')
            _, requested_manifest_digest = container_image_models.split_digest(
                canonical_reference)
        except (TypeError, ValueError) as e:
            raise ValueError('image.requested_reference is not a canonical '
                             'secret-free OCI reference.') from e
        if (canonical_reference != reference or
                requested_manifest_digest is None):
            raise ValueError('image.requested_reference must be digest-pinned.')
        object.__setattr__(self, 'requested_reference', reference)
        for field in ('oci_manifest_digest', 'oci_config_digest'):
            object.__setattr__(
                self, field,
                _sha256_digest(getattr(self, field), name=f'image.{field}'))
        if requested_manifest_digest != self.oci_manifest_digest:
            raise ValueError('image requested reference digest must equal the '
                             'qualified OCI manifest digest.')
        if not isinstance(self.qualification_artifact,
                          ProviderRepoArtifactRefV1):
            raise TypeError('image qualification artifact has an invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderOCIImageQualificationV1':
        raw = _closed_object(value,
                             name='OCI image qualification',
                             keys=cls._KEYS)
        return cls(requested_reference=raw['requested_reference'],
                   oci_manifest_digest=raw['oci_manifest_digest'],
                   oci_config_digest=raw['oci_config_digest'],
                   qualification_artifact=ProviderRepoArtifactRefV1.from_value(
                       raw['qualification_artifact']))

    def canonical_value(self) -> JsonObject:
        return {
            'requested_reference': self.requested_reference,
            'oci_manifest_digest': self.oci_manifest_digest,
            'oci_config_digest': self.oci_config_digest,
            'qualification_artifact':
                self.qualification_artifact.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderRuntimeImageIdentityV1(_CanonicalContract):
    """Runtime image evidence tied to one qualified OCI image."""

    raw_image_id: str
    runtime_image_id_scheme: str
    runtime_image_id_digest: str
    qualified_oci_manifest_digest: str
    qualified_oci_config_digest: str
    qualification_artifact_sha256: str
    runtime_id_contract: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'raw_image_id', 'runtime_image_id_scheme', 'runtime_image_id_digest',
        'qualified_oci_manifest_digest', 'qualified_oci_config_digest',
        'qualification_artifact_sha256', 'runtime_id_contract'
    })
    _SCHEMES: ClassVar[frozenset[str]] = frozenset(
        {'containerd', 'cri-o', 'docker-pullable'})

    def __post_init__(self) -> None:
        raw_image_id = _text(self.raw_image_id,
                             name='runtime_image.raw_image_id')
        object.__setattr__(self, 'raw_image_id', raw_image_id)
        scheme = _text(self.runtime_image_id_scheme,
                       name='runtime_image.runtime_image_id_scheme')
        if scheme not in self._SCHEMES:
            raise ValueError('runtime image ID scheme is unsupported.')
        object.__setattr__(self, 'runtime_image_id_scheme', scheme)
        for field in ('runtime_image_id_digest',
                      'qualified_oci_manifest_digest',
                      'qualified_oci_config_digest'):
            object.__setattr__(
                self, field,
                _sha256_digest(getattr(self, field),
                               name=f'runtime_image.{field}'))
        prefix = f'{scheme}://'
        if not raw_image_id.startswith(prefix):
            raise ValueError('raw runtime image ID does not match its declared '
                             'scheme.')
        raw_body = raw_image_id[len(prefix):]
        raw_digest: str | None
        if scheme in ('containerd', 'cri-o'):
            raw_digest = raw_body
        else:
            try:
                canonical_raw_reference = (
                    container_image_models.validate_oci_reference(
                        raw_body, 'runtime_image.raw_image_id'))
                _, raw_digest = container_image_models.split_digest(
                    canonical_raw_reference)
            except (TypeError, ValueError) as e:
                raise ValueError('raw runtime image ID is not a canonical '
                                 'docker-pullable reference.') from e
            if canonical_raw_reference != raw_body or raw_digest is None:
                raise ValueError('raw runtime image ID is not a canonical '
                                 'docker-pullable reference.')
        if _SHA256_DIGEST_RE.fullmatch(raw_digest) is None:
            raise ValueError('raw runtime image ID does not contain one '
                             'canonical SHA-256 digest under its scheme.')
        if raw_digest != self.runtime_image_id_digest:
            raise ValueError('raw runtime image ID digest does not match its '
                             'parsed runtime digest.')
        object.__setattr__(
            self, 'qualification_artifact_sha256',
            _sha256(self.qualification_artifact_sha256,
                    name='runtime_image.qualification_artifact_sha256'))
        if self.runtime_id_contract != 'qualified_oci_config_digest_v1':
            raise ValueError('runtime image ID contract is unsupported.')
        if self.runtime_image_id_digest != self.qualified_oci_config_digest:
            raise ValueError('runtime image ID must equal the qualified OCI '
                             'config digest.')

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderRuntimeImageIdentityV1':
        raw = _closed_object(value,
                             name='runtime image identity',
                             keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerImageV1(_CanonicalContract):
    """Qualified configured and observed image identity for one worker."""

    qualification: ProviderOCIImageQualificationV1
    runtime: ProviderRuntimeImageIdentityV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({'qualification', 'runtime'})

    def __post_init__(self) -> None:
        if not isinstance(self.qualification, ProviderOCIImageQualificationV1):
            raise TypeError('worker image qualification has an invalid type.')
        if not isinstance(self.runtime, ProviderRuntimeImageIdentityV1):
            raise TypeError('worker runtime image has an invalid type.')
        if (self.runtime.qualified_oci_manifest_digest
                != self.qualification.oci_manifest_digest or
                self.runtime.qualified_oci_config_digest
                != self.qualification.oci_config_digest or
                self.runtime.qualification_artifact_sha256
                != self.qualification.qualification_artifact.sha256):
            raise ValueError('worker runtime image differs from its qualified '
                             'OCI image.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderAuthorityWorkerImageV1':
        raw = _closed_object(value, name='worker image', keys=cls._KEYS)
        return cls(qualification=ProviderOCIImageQualificationV1.from_value(
            raw['qualification']),
                   runtime=ProviderRuntimeImageIdentityV1.from_value(
                       raw['runtime']))

    def canonical_value(self) -> JsonObject:
        return {
            'qualification': self.qualification.canonical_value(),
            'runtime': self.runtime.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerCohortManifestV1(_CanonicalContract):
    """Static release-rendered identity for an authority-worker cohort."""

    version: int
    cohort_id: str
    namespace: str
    deployment_name: str
    service_account_name: str
    container_name: str
    image: ProviderOCIImageQualificationV1
    pod_template_contract: ProviderRepoArtifactRefV1
    artifact_inventory: ProviderRepoArtifactRefV1
    callable_inventory: ProviderRepoArtifactRefV1
    claim_contract: str
    handler_allowlist: tuple[str, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'cohort_id', 'namespace', 'deployment_name',
        'service_account_name', 'container_name', 'image',
        'pod_template_contract', 'artifact_inventory', 'callable_inventory',
        'claim_contract', 'handler_allowlist'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='cohort manifest version')
        cohort_id = _dns_label(self.cohort_id, name='cohort_manifest.cohort_id')
        object.__setattr__(self, 'cohort_id', cohort_id)
        for field in ('namespace', 'deployment_name', 'service_account_name'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'cohort_manifest.{field}',
                      maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        if self.container_name != 'skypilot-authority-worker':
            raise ValueError('cohort manifest container name is unsupported.')
        if not isinstance(self.image, ProviderOCIImageQualificationV1):
            raise TypeError('cohort manifest image has an invalid type.')
        for field in ('pod_template_contract', 'artifact_inventory',
                      'callable_inventory'):
            if not isinstance(getattr(self, field), ProviderRepoArtifactRefV1):
                raise TypeError(f'cohort manifest {field} has an invalid type.')
        if self.claim_contract != 'frozen_action_cohort_join_v1':
            raise ValueError('cohort manifest claim contract is unsupported.')
        if self.handler_allowlist != (
                PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1):
            raise ValueError('cohort manifest handler allowlist must be the '
                             'ordered v1 allowlist.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> 'ProviderAuthorityWorkerCohortManifestV1':
        raw = _closed_object(value,
                             name='authority-worker cohort manifest',
                             keys=cls._KEYS)
        handlers = raw['handler_allowlist']
        if not isinstance(handlers, list):
            raise TypeError('cohort manifest handler_allowlist must be a list.')
        return cls(version=raw['version'],
                   cohort_id=raw['cohort_id'],
                   namespace=raw['namespace'],
                   deployment_name=raw['deployment_name'],
                   service_account_name=raw['service_account_name'],
                   container_name=raw['container_name'],
                   image=ProviderOCIImageQualificationV1.from_value(
                       raw['image']),
                   pod_template_contract=ProviderRepoArtifactRefV1.from_value(
                       raw['pod_template_contract']),
                   artifact_inventory=ProviderRepoArtifactRefV1.from_value(
                       raw['artifact_inventory']),
                   callable_inventory=ProviderRepoArtifactRefV1.from_value(
                       raw['callable_inventory']),
                   claim_contract=raw['claim_contract'],
                   handler_allowlist=tuple(handlers))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'cohort_id': self.cohort_id,
            'namespace': self.namespace,
            'deployment_name': self.deployment_name,
            'service_account_name': self.service_account_name,
            'container_name': 'skypilot-authority-worker',
            'image': self.image.canonical_value(),
            'pod_template_contract':
                self.pod_template_contract.canonical_value(),
            'artifact_inventory': self.artifact_inventory.canonical_value(),
            'callable_inventory': self.callable_inventory.canonical_value(),
            'claim_contract': 'frozen_action_cohort_join_v1',
            'handler_allowlist': list(self.handler_allowlist),
        }


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerCohortV1(_CanonicalContract):
    """Complete resolved identity retained for one immutable cohort."""

    version: int
    manifest: ProviderAuthorityWorkerCohortManifestV1
    manifest_sha256: str
    deployment_uid: str
    service_account_uid: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'manifest', 'manifest_sha256', 'deployment_uid',
        'service_account_uid'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='worker cohort version')
        if not isinstance(self.manifest,
                          ProviderAuthorityWorkerCohortManifestV1):
            raise TypeError('worker cohort manifest has an invalid type.')
        object.__setattr__(
            self, 'manifest_sha256',
            _sha256(self.manifest_sha256, name='cohort.manifest_sha256'))
        if self.manifest_sha256 != self.manifest.sha256:
            raise ValueError('worker cohort manifest hash does not match.')
        for field in ('deployment_uid', 'service_account_uid'):
            object.__setattr__(
                self, field, _text(getattr(self, field),
                                   name=f'cohort.{field}'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderAuthorityWorkerCohortV1':
        raw = _closed_object(value,
                             name='authority-worker cohort identity',
                             keys=cls._KEYS)
        return cls(version=raw['version'],
                   manifest=ProviderAuthorityWorkerCohortManifestV1.from_value(
                       raw['manifest']),
                   manifest_sha256=raw['manifest_sha256'],
                   deployment_uid=raw['deployment_uid'],
                   service_account_uid=raw['service_account_uid'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'manifest': self.manifest.canonical_value(),
            'manifest_sha256': self.manifest_sha256,
            'deployment_uid': self.deployment_uid,
            'service_account_uid': self.service_account_uid,
        }

    @property
    def cohort_id(self) -> str:
        return self.manifest.cohort_id


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesControllerOwnerV1(_CanonicalContract):
    """Closed Kubernetes controller-owner identity."""

    api_version: str
    kind: str
    name: str
    uid: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'api_version', 'kind', 'name', 'uid'})

    def __post_init__(self) -> None:
        if self.api_version != 'apps/v1':
            raise ValueError('controller owner api_version must be apps/v1.')
        if self.kind not in ('ReplicaSet', 'Deployment'):
            raise ValueError('controller owner kind is unsupported.')
        object.__setattr__(
            self, 'name',
            _text(self.name,
                  name='controller_owner.name',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(self, 'uid',
                           _text(self.uid, name='controller_owner.uid'))

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesControllerOwnerV1':
        raw = _closed_object(value, name='controller owner', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerIdentityV1(_CanonicalContract):
    """Bounded live Kubernetes identity attested by one authority worker."""

    namespace: str
    pod_name: str
    pod_uid: str
    pod_resource_version: str
    pod_service_account_name: str
    pod_controller_owner: ProviderKubernetesControllerOwnerV1
    replica_set_name: str
    replica_set_uid: str
    replica_set_resource_version: str
    replica_set_controller_owner: ProviderKubernetesControllerOwnerV1
    deployment_name: str
    deployment_uid: str
    deployment_resource_version: str
    deployment_generation: int
    deployment_observed_generation: int
    pod_template_contract_sha256: str
    image: ProviderAuthorityWorkerImageV1
    service_account_uid: str
    artifact_inventory_sha256: str
    callable_inventory_sha256: str
    handler_allowlist_sha256: str
    observed_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'namespace', 'pod_name', 'pod_uid', 'pod_resource_version',
        'pod_service_account_name', 'pod_controller_owner', 'replica_set_name',
        'replica_set_uid', 'replica_set_resource_version',
        'replica_set_controller_owner', 'deployment_name', 'deployment_uid',
        'deployment_resource_version', 'deployment_generation',
        'deployment_observed_generation', 'pod_template_contract_sha256',
        'image', 'service_account_uid', 'artifact_inventory_sha256',
        'callable_inventory_sha256', 'handler_allowlist_sha256', 'observed_at'
    })

    def __post_init__(self) -> None:
        for field in ('namespace', 'pod_name', 'pod_service_account_name',
                      'replica_set_name', 'deployment_name'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field),
                      name=f'worker.{field}',
                      maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        for field in ('pod_uid', 'pod_resource_version', 'replica_set_uid',
                      'replica_set_resource_version', 'deployment_uid',
                      'deployment_resource_version', 'service_account_uid'):
            object.__setattr__(
                self, field, _text(getattr(self, field),
                                   name=f'worker.{field}'))
        if not isinstance(self.pod_controller_owner,
                          ProviderKubernetesControllerOwnerV1):
            raise TypeError('worker Pod controller owner has an invalid type.')
        if not isinstance(self.replica_set_controller_owner,
                          ProviderKubernetesControllerOwnerV1):
            raise TypeError('worker ReplicaSet owner has an invalid type.')
        if self.pod_controller_owner.kind != 'ReplicaSet':
            raise ValueError('worker Pod owner must be a ReplicaSet.')
        if self.replica_set_controller_owner.kind != 'Deployment':
            raise ValueError('worker ReplicaSet owner must be a Deployment.')
        if (self.pod_controller_owner.name != self.replica_set_name or
                self.pod_controller_owner.uid != self.replica_set_uid or
                self.replica_set_controller_owner.name != self.deployment_name
                or
                self.replica_set_controller_owner.uid != self.deployment_uid):
            raise ValueError('worker controller-owner chain is inconsistent.')
        for field in ('deployment_generation',
                      'deployment_observed_generation'):
            object.__setattr__(
                self, field,
                _positive_integer(getattr(self, field), name=f'worker.{field}'))
        for field in ('pod_template_contract_sha256',
                      'artifact_inventory_sha256', 'callable_inventory_sha256',
                      'handler_allowlist_sha256'):
            object.__setattr__(
                self, field,
                _sha256(getattr(self, field), name=f'worker.{field}'))
        if not isinstance(self.image, ProviderAuthorityWorkerImageV1):
            raise TypeError('worker image has an invalid type.')
        object.__setattr__(
            self, 'observed_at',
            _timestamp(self.observed_at, name='worker.observed_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderAuthorityWorkerIdentityV1':
        raw = _closed_object(value,
                             name='authority-worker identity',
                             keys=cls._KEYS)
        return cls(
            namespace=raw['namespace'],
            pod_name=raw['pod_name'],
            pod_uid=raw['pod_uid'],
            pod_resource_version=raw['pod_resource_version'],
            pod_service_account_name=raw['pod_service_account_name'],
            pod_controller_owner=ProviderKubernetesControllerOwnerV1.from_value(
                raw['pod_controller_owner']),
            replica_set_name=raw['replica_set_name'],
            replica_set_uid=raw['replica_set_uid'],
            replica_set_resource_version=raw['replica_set_resource_version'],
            replica_set_controller_owner=(
                ProviderKubernetesControllerOwnerV1.from_value(
                    raw['replica_set_controller_owner'])),
            deployment_name=raw['deployment_name'],
            deployment_uid=raw['deployment_uid'],
            deployment_resource_version=raw['deployment_resource_version'],
            deployment_generation=raw['deployment_generation'],
            deployment_observed_generation=(
                raw['deployment_observed_generation']),
            pod_template_contract_sha256=raw['pod_template_contract_sha256'],
            image=ProviderAuthorityWorkerImageV1.from_value(raw['image']),
            service_account_uid=raw['service_account_uid'],
            artifact_inventory_sha256=raw['artifact_inventory_sha256'],
            callable_inventory_sha256=raw['callable_inventory_sha256'],
            handler_allowlist_sha256=raw['handler_allowlist_sha256'],
            observed_at=raw['observed_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'namespace': self.namespace,
            'pod_name': self.pod_name,
            'pod_uid': self.pod_uid,
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
            'deployment_resource_version': self.deployment_resource_version,
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
                            cohort: ProviderAuthorityWorkerCohortV1) -> None:
        """Require every immutable worker field to match ``cohort``."""

        if not isinstance(cohort, ProviderAuthorityWorkerCohortV1):
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
                self.handler_allowlist_sha256 != canonical_sha256(
                    list(manifest.handler_allowlist)) or
                self.image.qualification.canonical_bytes
                != manifest.image.canonical_bytes):
            raise ValueError('worker identity does not match its cohort.')


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerRegistrationV1(_CanonicalContract):
    """One current ready-worker registration attestation."""

    worker: ProviderAuthorityWorkerIdentityV1
    pod_ready: bool
    deployment_spec_replicas: int
    deployment_status_observed_generation: int
    deployment_ready_replicas: int
    deployment_available_replicas: int
    registered_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'worker', 'pod_ready', 'deployment_spec_replicas',
        'deployment_status_observed_generation', 'deployment_ready_replicas',
        'deployment_available_replicas', 'registered_at'
    })

    def __post_init__(self) -> None:
        if not isinstance(self.worker, ProviderAuthorityWorkerIdentityV1):
            raise TypeError('worker registration identity has an invalid type.')
        _boolean(self.pod_ready, name='registration.pod_ready')
        if not self.pod_ready:
            raise ValueError('worker registration requires a ready Pod.')
        for field in ('deployment_spec_replicas', 'deployment_ready_replicas',
                      'deployment_available_replicas'):
            if _positive_integer(getattr(self, field),
                                 name=f'registration.{field}') != 2:
                raise ValueError(f'registration.{field} must equal 2.')
        observed = _positive_integer(
            self.deployment_status_observed_generation,
            name='registration.deployment_status_observed_generation')
        if (observed != self.worker.deployment_observed_generation or
                observed != self.worker.deployment_generation):
            raise ValueError('registration Deployment generation is not '
                             'current and consistently observed.')
        object.__setattr__(self, 'deployment_status_observed_generation',
                           observed)
        object.__setattr__(
            self, 'registered_at',
            _timestamp(self.registered_at, name='registration.registered_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderAuthorityWorkerRegistrationV1':
        raw = _closed_object(value,
                             name='authority-worker registration',
                             keys=cls._KEYS)
        return cls(
            worker=ProviderAuthorityWorkerIdentityV1.from_value(raw['worker']),
            pod_ready=raw['pod_ready'],
            deployment_spec_replicas=raw['deployment_spec_replicas'],
            deployment_status_observed_generation=raw[
                'deployment_status_observed_generation'],
            deployment_ready_replicas=raw['deployment_ready_replicas'],
            deployment_available_replicas=raw['deployment_available_replicas'],
            registered_at=raw['registered_at'])

    def canonical_value(self) -> JsonObject:
        return {
            'worker': self.worker.canonical_value(),
            'pod_ready': True,
            'deployment_spec_replicas': 2,
            'deployment_status_observed_generation':
                self.deployment_status_observed_generation,
            'deployment_ready_replicas': 2,
            'deployment_available_replicas': 2,
            'registered_at': self.registered_at,
        }

    @property
    def pod_uid(self) -> str:
        return self.worker.pod_uid


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerRegistrationSetV1(_CanonicalContract):
    """Canonical one-or-two-worker registration set for a cohort."""

    version: int
    cohort_identity_sha256: str
    workers: tuple[ProviderAuthorityWorkerRegistrationV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'cohort_identity_sha256', 'workers'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='worker registration set version')
        object.__setattr__(
            self, 'cohort_identity_sha256',
            _sha256(self.cohort_identity_sha256,
                    name='registration_set.cohort_identity_sha256'))
        if (not isinstance(self.workers, tuple) or
                not 1 <= len(self.workers) <= 2 or
                any(not isinstance(worker,
                                   ProviderAuthorityWorkerRegistrationV1)
                    for worker in self.workers)):
            raise ValueError('registration workers must be a tuple of one or '
                             'two typed attestations.')
        pod_uids = tuple(worker.pod_uid for worker in self.workers)
        if pod_uids != tuple(sorted(set(pod_uids))):
            raise ValueError('registration workers must be sorted by distinct '
                             'Pod UID.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> 'ProviderAuthorityWorkerRegistrationSetV1':
        raw = _closed_object(value,
                             name='authority-worker registration set',
                             keys=cls._KEYS)
        workers = raw['workers']
        if not isinstance(workers, list):
            raise TypeError('registration_set.workers must be a list.')
        return cls(version=raw['version'],
                   cohort_identity_sha256=raw['cohort_identity_sha256'],
                   workers=tuple(
                       ProviderAuthorityWorkerRegistrationV1.from_value(worker)
                       for worker in workers))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'cohort_identity_sha256': self.cohort_identity_sha256,
            'workers': [worker.canonical_value() for worker in self.workers],
        }

    @property
    def registrations(
            self) -> tuple[ProviderAuthorityWorkerRegistrationV1, ...]:
        return self.workers

    @property
    def count(self) -> int:
        return len(self.workers)

    def validate_for_cohort(
            self,
            cohort: ProviderAuthorityWorkerCohortV1,
            *,
            require_two: bool = False,
            database_now: datetime.datetime | None = None) -> None:
        """Verify the set's hash and every worker against ``cohort``."""

        if not isinstance(cohort, ProviderAuthorityWorkerCohortV1):
            raise TypeError('worker cohort has an invalid type.')
        if self.cohort_identity_sha256 != cohort.sha256:
            raise ValueError('registration set cohort hash does not match.')
        if require_two and len(self.workers) != 2:
            raise ValueError('accepting a cohort requires two workers.')
        for registration in self.workers:
            registration.worker.validate_for_cohort(cohort)
        deployment_versions = {
            (registration.worker.deployment_resource_version,
             registration.worker.deployment_generation,
             registration.worker.deployment_observed_generation,
             registration.deployment_status_observed_generation)
            for registration in self.workers
        }
        if len(deployment_versions) != 1:
            raise ValueError('registration workers observe different '
                             'Deployment versions.')
        if database_now is not None:
            self.validate_freshness(database_now)

    def validate_freshness(self, database_now: datetime.datetime) -> None:
        """Enforce the nonconfigurable five-minute DB-clock transition gate."""

        if (not isinstance(database_now, datetime.datetime) or
                database_now.tzinfo is None or
                database_now.utcoffset() is None):
            raise TypeError('database_now must be a timezone-aware datetime.')
        normalized_now = database_now.astimezone(datetime.timezone.utc)
        oldest = normalized_now - WORKER_REGISTRATION_MAX_AGE
        for registration in self.workers:
            for name, value in (('registered_at', registration.registered_at),
                                ('worker.observed_at',
                                 registration.worker.observed_at)):
                parsed = datetime.datetime.strptime(
                    value, '%Y-%m-%dT%H:%M:%S.%fZ').replace(
                        tzinfo=datetime.timezone.utc)
                if parsed > normalized_now:
                    raise ValueError(f'registration {name} is in the database '
                                     'future.')
                if parsed < oldest:
                    raise ValueError(f'registration {name} is older than five '
                                     'minutes.')


# Storage-facing aliases retain the provider contract's exact canonical shape.
WorkerCohortIdentityV1 = ProviderAuthorityWorkerCohortV1
WorkerCohortRegistrationSetV1 = ProviderAuthorityWorkerRegistrationSetV1


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
        service_incarnation = _schema_uuid(
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
class CoverageDecisionIdentityV1(_CanonicalContract):
    """Provider-independent identity of one immutable coverage decision."""

    version: int
    service_hash: str
    service_incarnation: uuid.UUID
    replica_id: int
    replica_incarnation: uuid.UUID
    desired_generation: int
    action_type: kernel_actions.ActionKind

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'service_hash', 'service_incarnation', 'replica_id',
        'replica_incarnation', 'desired_generation', 'action_type'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='coverage identity version')
        resource_identity = ProviderResourceIdentityV1(
            service_hash=self.service_hash,
            service_incarnation=self.service_incarnation,
            replica_id=self.replica_id,
            replica_incarnation=self.replica_incarnation,
            desired_generation=self.desired_generation)
        object.__setattr__(self, 'service_hash', resource_identity.service_hash)
        object.__setattr__(self, 'service_incarnation',
                           resource_identity.service_incarnation)
        object.__setattr__(self, 'replica_id', resource_identity.replica_id)
        object.__setattr__(self, 'replica_incarnation',
                           resource_identity.replica_incarnation)
        object.__setattr__(self, 'desired_generation',
                           resource_identity.desired_generation)
        object.__setattr__(
            self, 'action_type',
            _action_kind(self.action_type, name='coverage.action_type'))

    @classmethod
    def from_value(cls, value: Any) -> 'CoverageDecisionIdentityV1':
        raw = _closed_object(value,
                             name='coverage decision identity',
                             keys=cls._KEYS)
        return cls(version=raw['version'],
                   service_hash=raw['service_hash'],
                   service_incarnation=raw['service_incarnation'],
                   replica_id=raw['replica_id'],
                   replica_incarnation=raw['replica_incarnation'],
                   desired_generation=raw['desired_generation'],
                   action_type=raw['action_type'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'service_hash': self.service_hash,
            'service_incarnation': str(self.service_incarnation),
            'replica_id': self.replica_id,
            'replica_incarnation': str(self.replica_incarnation),
            'desired_generation': self.desired_generation,
            'action_type': self.action_type.value,
        }

    @property
    def resource_identity(self) -> ProviderResourceIdentityV1:
        return ProviderResourceIdentityV1(
            service_hash=self.service_hash,
            service_incarnation=self.service_incarnation,
            replica_id=self.replica_id,
            replica_incarnation=self.replica_incarnation,
            desired_generation=self.desired_generation)

    @property
    def kernel_identity(self) -> kernel_actions.ResourceActionIdentity:
        return self.resource_identity.action_identity(self.action_type)

    @property
    def decision_id(self) -> uuid.UUID:
        """Return the exact resource-action UUIDv5; no second namespace."""

        return self.kernel_identity.action_id


@dataclasses.dataclass(frozen=True)
class CoverageDecisionV1(_CanonicalContract):
    """Immutable canonical value of one Serve033 coverage row."""

    decision_id: uuid.UUID
    service_name: str
    service_hash: str
    service_incarnation: uuid.UUID
    replica_id: int
    replica_incarnation: uuid.UUID
    desired_generation: int
    action_type: kernel_actions.ActionKind
    normalizer_contract_version: int
    normalization_outcome: NormalizationOutcome
    not_representable_reason: (ProviderLaunchNotRepresentableReasonV1 |
                               ProviderDownNotRepresentableReasonV1 | None)
    worker_cohort_ref_id: uuid.UUID | None
    admitted_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'decision_id', 'service_name', 'service_hash', 'service_incarnation',
        'replica_id', 'replica_incarnation', 'desired_generation',
        'action_type', 'normalizer_contract_version', 'normalization_outcome',
        'not_representable_reason', 'worker_cohort_ref_id', 'admitted_at'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'service_name',
            _text(self.service_name,
                  name='coverage.service_name',
                  maximum_bytes=_MAX_SERVICE_NAME_BYTES))
        identity = CoverageDecisionIdentityV1(
            version=1,
            service_hash=self.service_hash,
            service_incarnation=self.service_incarnation,
            replica_id=self.replica_id,
            replica_incarnation=self.replica_incarnation,
            desired_generation=self.desired_generation,
            action_type=self.action_type)
        for field in ('service_hash', 'service_incarnation', 'replica_id',
                      'replica_incarnation', 'desired_generation',
                      'action_type'):
            object.__setattr__(self, field, getattr(identity, field))
        decision_id = _uuid(self.decision_id, name='coverage.decision_id')
        if decision_id != identity.decision_id:
            raise ValueError('coverage decision ID does not match UUIDv5 '
                             'identity.')
        object.__setattr__(self, 'decision_id', decision_id)
        _version_one(self.normalizer_contract_version,
                     name='normalizer contract version')
        outcome = (self.normalization_outcome if isinstance(
            self.normalization_outcome, NormalizationOutcome) else _enum_value(
                NormalizationOutcome,
                self.normalization_outcome,
                name='coverage.normalization_outcome'))
        object.__setattr__(self, 'normalization_outcome', outcome)
        reason = self.not_representable_reason
        if outcome is NormalizationOutcome.REPRESENTABLE:
            if reason is not None:
                raise ValueError('representable coverage requires null reason.')
        else:
            if reason is None:
                raise ValueError('not-representable coverage requires a '
                                 'closed reason.')
            expected_type: type[ProviderLaunchNotRepresentableReasonV1] | type[
                ProviderDownNotRepresentableReasonV1]
            expected_type = (ProviderLaunchNotRepresentableReasonV1
                             if identity.action_type
                             is kernel_actions.ActionKind.LAUNCH else
                             ProviderDownNotRepresentableReasonV1)
            if isinstance(reason, (ProviderLaunchNotRepresentableReasonV1,
                                   ProviderDownNotRepresentableReasonV1)):
                if not isinstance(reason, expected_type):
                    raise ValueError('coverage reason has the wrong action '
                                     'kind.')
            else:
                reason = _enum_value(expected_type,
                                     reason,
                                     name='coverage.not_representable_reason')
            object.__setattr__(self, 'not_representable_reason', reason)
        if self.worker_cohort_ref_id is not None:
            cohort_ref_id = _uuid(self.worker_cohort_ref_id,
                                  name='coverage.worker_cohort_ref_id')
            if cohort_ref_id != decision_id:
                raise ValueError('coverage cohort reference must use the '
                                 'decision ID.')
            object.__setattr__(self, 'worker_cohort_ref_id', cohort_ref_id)
        object.__setattr__(
            self, 'admitted_at',
            _timestamp(self.admitted_at, name='coverage.admitted_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'CoverageDecisionV1':
        raw = _closed_object(value, name='coverage decision', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        reason = self.not_representable_reason
        return {
            'decision_id': str(self.decision_id),
            'service_name': self.service_name,
            'service_hash': self.service_hash,
            'service_incarnation': str(self.service_incarnation),
            'replica_id': self.replica_id,
            'replica_incarnation': str(self.replica_incarnation),
            'desired_generation': self.desired_generation,
            'action_type': self.action_type.value,
            'normalizer_contract_version': 1,
            'normalization_outcome': self.normalization_outcome.value,
            'not_representable_reason':
                (None if reason is None else reason.value),
            'worker_cohort_ref_id': (None if self.worker_cohort_ref_id is None
                                     else str(self.worker_cohort_ref_id)),
            'admitted_at': self.admitted_at,
        }

    @property
    def identity(self) -> CoverageDecisionIdentityV1:
        return CoverageDecisionIdentityV1(
            version=1,
            service_hash=self.service_hash,
            service_incarnation=self.service_incarnation,
            replica_id=self.replica_id,
            replica_incarnation=self.replica_incarnation,
            desired_generation=self.desired_generation,
            action_type=self.action_type)


@dataclasses.dataclass(frozen=True)
class WorkerCohortReferenceInputV1(_CanonicalContract):
    """Bounded immutable identity presented when preparing a reference."""

    version: int
    decision_id: uuid.UUID
    cohort_id: str
    service_hash: str
    replica_incarnation: uuid.UUID
    desired_generation: int
    action_type: kernel_actions.ActionKind
    controller_owner_fence: str
    lifecycle_epoch: int

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'decision_id', 'cohort_id', 'service_hash',
        'replica_incarnation', 'desired_generation', 'action_type',
        'controller_owner_fence', 'lifecycle_epoch'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='worker cohort reference version')
        object.__setattr__(
            self, 'decision_id',
            _uuid(self.decision_id, name='cohort_reference.decision_id'))
        object.__setattr__(
            self, 'cohort_id',
            _dns_label(self.cohort_id, name='cohort_reference.cohort_id'))
        service_hash = _schema_uuid(self.service_hash,
                                    name='cohort_reference.service_hash')
        object.__setattr__(self, 'service_hash', str(service_hash))
        object.__setattr__(
            self, 'replica_incarnation',
            _uuid(self.replica_incarnation,
                  name='cohort_reference.replica_incarnation'))
        object.__setattr__(
            self, 'desired_generation',
            _positive_integer(self.desired_generation,
                              name='cohort_reference.desired_generation'))
        object.__setattr__(
            self, 'action_type',
            _action_kind(self.action_type, name='cohort_reference.action_type'))
        object.__setattr__(
            self, 'controller_owner_fence',
            _text(self.controller_owner_fence,
                  name='cohort_reference.controller_owner_fence'))
        object.__setattr__(
            self, 'lifecycle_epoch',
            _positive_integer(self.lifecycle_epoch,
                              name='cohort_reference.lifecycle_epoch'))

    @classmethod
    def from_value(cls, value: Any) -> 'WorkerCohortReferenceInputV1':
        raw = _closed_object(value,
                             name='worker cohort reference input',
                             keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'decision_id': str(self.decision_id),
            'cohort_id': self.cohort_id,
            'service_hash': self.service_hash,
            'replica_incarnation': str(self.replica_incarnation),
            'desired_generation': self.desired_generation,
            'action_type': self.action_type.value,
            'controller_owner_fence': self.controller_owner_fence,
            'lifecycle_epoch': self.lifecycle_epoch,
        }

    def validate_coverage(self, coverage: CoverageDecisionV1) -> None:
        if not isinstance(coverage, CoverageDecisionV1):
            raise TypeError('coverage decision has an invalid type.')
        if (self.decision_id != coverage.decision_id or
                coverage.worker_cohort_ref_id != self.decision_id or
                self.service_hash != coverage.service_hash or
                self.replica_incarnation != coverage.replica_incarnation or
                self.desired_generation != coverage.desired_generation or
                self.action_type is not coverage.action_type):
            raise ValueError('cohort reference does not match coverage.')


@dataclasses.dataclass(frozen=True)
class CoverageAttemptV1(_CanonicalContract):
    """Canonical coverage-only one-use legacy submission fence."""

    decision_id: uuid.UUID
    request_sequence: int
    logical_attempt: int
    request_role: ShadowRequestRole
    phase: ShadowAttemptPhase
    legacy_request_id: str | None
    terminal_request_status: CoverageAttemptTerminalStatus | None
    retry_disposition: CoverageAttemptRetryDisposition | None
    admitted_at: str
    request_bound_at: str | None
    completed_at: str | None
    updated_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'decision_id', 'request_sequence', 'logical_attempt', 'request_role',
        'phase', 'legacy_request_id', 'terminal_request_status',
        'retry_disposition', 'admitted_at', 'request_bound_at', 'completed_at',
        'updated_at'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'decision_id',
            _uuid(self.decision_id, name='coverage_attempt.decision_id'))
        for field in ('request_sequence', 'logical_attempt'):
            object.__setattr__(
                self, field,
                _positive_integer(getattr(self, field),
                                  name=f'coverage_attempt.{field}',
                                  maximum=_MAX_POSTGRES_INTEGER))
        role = (self.request_role
                if isinstance(self.request_role, ShadowRequestRole) else
                _enum_value(ShadowRequestRole,
                            self.request_role,
                            name='coverage_attempt.request_role'))
        phase = (self.phase if isinstance(
            self.phase, ShadowAttemptPhase) else _enum_value(
                ShadowAttemptPhase, self.phase, name='coverage_attempt.phase'))
        object.__setattr__(self, 'request_role', role)
        object.__setattr__(self, 'phase', phase)
        object.__setattr__(
            self, 'legacy_request_id',
            _optional_text(self.legacy_request_id,
                           name='coverage_attempt.legacy_request_id',
                           maximum_bytes=_MAX_REQUEST_ID_BYTES))
        terminal = self.terminal_request_status
        if terminal is not None:
            terminal = (terminal if isinstance(
                terminal, CoverageAttemptTerminalStatus) else _enum_value(
                    CoverageAttemptTerminalStatus,
                    terminal,
                    name='coverage_attempt.terminal_request_status'))
            object.__setattr__(self, 'terminal_request_status', terminal)
        retry = self.retry_disposition
        if retry is not None:
            retry = (retry
                     if isinstance(retry, CoverageAttemptRetryDisposition) else
                     _enum_value(CoverageAttemptRetryDisposition,
                                 retry,
                                 name='coverage_attempt.retry_disposition'))
            object.__setattr__(self, 'retry_disposition', retry)
        admitted_at = _timestamp(self.admitted_at,
                                 name='coverage_attempt.admitted_at')
        request_bound_at = (None
                            if self.request_bound_at is None else _timestamp(
                                self.request_bound_at,
                                name='coverage_attempt.request_bound_at'))
        completed_at = (None if self.completed_at is None else _timestamp(
            self.completed_at, name='coverage_attempt.completed_at'))
        updated_at = _timestamp(self.updated_at,
                                name='coverage_attempt.updated_at')
        object.__setattr__(self, 'admitted_at', admitted_at)
        object.__setattr__(self, 'request_bound_at', request_bound_at)
        object.__setattr__(self, 'completed_at', completed_at)
        object.__setattr__(self, 'updated_at', updated_at)
        if (updated_at < admitted_at or
            (request_bound_at is not None and request_bound_at < admitted_at) or
            (completed_at is not None and completed_at < admitted_at)):
            raise ValueError('coverage attempt timestamps are out of order.')
        if phase is ShadowAttemptPhase.PRE_SUBMIT:
            if any(value is not None
                   for value in (self.legacy_request_id, terminal, retry,
                                 request_bound_at, completed_at)):
                raise ValueError('pre-submit coverage attempt has later '
                                 'evidence.')
        elif phase is ShadowAttemptPhase.REQUEST_BOUND:
            if (self.legacy_request_id is None or request_bound_at is None or
                    any(value is not None
                        for value in (terminal, retry, completed_at))):
                raise ValueError('request-bound coverage attempt has invalid '
                                 'evidence.')
        elif phase is ShadowAttemptPhase.COMPLETE:
            if (self.legacy_request_id is None or request_bound_at is None or
                    completed_at is None or terminal is None or retry is None):
                raise ValueError('complete coverage attempt lacks terminal '
                                 'evidence.')
        elif phase is ShadowAttemptPhase.ABANDONED_PRE_SUBMIT:
            if (completed_at is None or
                    any(value is not None
                        for value in (self.legacy_request_id, terminal, retry,
                                      request_bound_at))):
                raise ValueError('abandoned coverage attempt has mutation '
                                 'evidence.')
        elif (completed_at is None or
              any(value is not None
                  for value in (self.legacy_request_id, terminal, retry,
                                request_bound_at))):
            raise ValueError('unknown request association has invalid shape.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'CoverageAttemptV1':
        raw = _closed_object(value, name='coverage attempt', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'decision_id': str(self.decision_id),
            'request_sequence': self.request_sequence,
            'logical_attempt': self.logical_attempt,
            'request_role': self.request_role.value,
            'phase': self.phase.value,
            'legacy_request_id': self.legacy_request_id,
            'terminal_request_status':
                (None if self.terminal_request_status is None else
                 self.terminal_request_status.value),
            'retry_disposition': (None if self.retry_disposition is None else
                                  self.retry_disposition.value),
            'admitted_at': self.admitted_at,
            'request_bound_at': self.request_bound_at,
            'completed_at': self.completed_at,
            'updated_at': self.updated_at,
        }


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


def _provider_label_tuple(value: Any, *,
                          name: str) -> tuple[ProviderLabelV1, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f'{name} must be a tuple.')
    if (len(value) > _MAX_LIST_ITEMS or
            any(not isinstance(label, ProviderLabelV1) for label in value)):
        raise ValueError(f'{name} must contain at most 256 typed labels.')
    label_keys = tuple(label.key for label in value)
    if label_keys != tuple(sorted(set(label_keys))):
        raise ValueError(f'{name} must be sorted by unique key.')
    return value


class ProviderPodTopologyMutableObjectKindV1(str, enum.Enum):
    """Closed Kubernetes kinds mutated by the direct-Pod topology."""

    SERVICE = 'Service'
    POD = 'Pod'


class ProviderObjectRoleV1(str, enum.Enum):
    """Closed role order for the direct-Pod topology."""

    HEAD_SSH_SERVICE = 'head_ssh_service'
    HEAD_SERVICE = 'head_service'
    HEAD_POD = 'head_pod'


@dataclasses.dataclass(frozen=True)
class ProviderPodTopologyMutableObjectV1(_CanonicalContract):
    """One role-specific mutable object in a direct-Pod topology."""

    kind: ProviderPodTopologyMutableObjectKindV1
    role: ProviderObjectRoleV1
    name: str
    labels: tuple[ProviderLabelV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'kind', 'role', 'name', 'labels'})
    _ROLE_KINDS: ClassVar[dict[
        ProviderObjectRoleV1, ProviderPodTopologyMutableObjectKindV1]] = {
            ProviderObjectRoleV1.HEAD_SSH_SERVICE:
                ProviderPodTopologyMutableObjectKindV1.SERVICE,
            ProviderObjectRoleV1.HEAD_SERVICE:
                ProviderPodTopologyMutableObjectKindV1.SERVICE,
            ProviderObjectRoleV1.HEAD_POD:
                ProviderPodTopologyMutableObjectKindV1.POD,
        }

    def __post_init__(self) -> None:
        kind = _enum_value(ProviderPodTopologyMutableObjectKindV1,
                           self.kind,
                           name='topology mutable-object kind')
        role = _enum_value(ProviderObjectRoleV1,
                           self.role,
                           name='topology mutable-object role')
        object.__setattr__(self, 'kind', kind)
        object.__setattr__(self, 'role', role)
        if kind is not self._ROLE_KINDS[role]:
            raise ValueError('topology mutable-object role and kind mismatch.')
        object.__setattr__(
            self, 'name', _text(self.name, name='topology.mutable_object.name'))
        object.__setattr__(
            self, 'labels',
            _provider_label_tuple(self.labels,
                                  name='topology mutable-object labels'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderPodTopologyMutableObjectV1':
        raw = _closed_object(value,
                             name='topology mutable object',
                             keys=cls._KEYS)
        labels = raw['labels']
        if not isinstance(labels, list):
            raise TypeError('topology mutable-object labels must be a list.')
        return cls(kind=raw['kind'],
                   role=raw['role'],
                   name=raw['name'],
                   labels=tuple(
                       ProviderLabelV1.from_value(label) for label in labels))

    def canonical_value(self) -> JsonObject:
        return {
            'kind': self.kind.value,
            'role': self.role.value,
            'name': self.name,
            'labels': [label.canonical_value() for label in self.labels],
        }


@dataclasses.dataclass(frozen=True)
class ProviderPodTopologyV1(_CanonicalContract):
    """Exact one-Pod/two-Service provider topology."""

    version: int
    kind: str
    node_count: int
    application_port: str
    resources_ports: tuple[str, ...]
    mutable_objects: tuple[ProviderPodTopologyMutableObjectV1, ...]
    shared_prerequisites: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'kind', 'node_count', 'application_port', 'resources_ports',
        'mutable_objects', 'shared_prerequisites'
    })
    _EXPECTED_ROLES: ClassVar[tuple[ProviderObjectRoleV1, ...]] = (
        ProviderObjectRoleV1.HEAD_SSH_SERVICE,
        ProviderObjectRoleV1.HEAD_SERVICE,
        ProviderObjectRoleV1.HEAD_POD,
    )
    _DISPLAY_LABEL: ClassVar[str] = 'skypilot-cluster-name'
    _CLUSTER_UUID_LABEL: ClassVar[str] = 'skypilot.co/cluster-record-uuid'
    _REPLICA_UUID_LABEL: ClassVar[str] = 'skypilot.co/serve-replica-incarnation'

    def __post_init__(self) -> None:
        _version_one(self.version, name='pod topology version')
        if self.kind != 'single_direct_pod_two_services':
            raise ValueError('pod topology kind is unsupported.')
        if (not isinstance(self.node_count, int) or
                isinstance(self.node_count, bool) or self.node_count != 1):
            raise ValueError('pod topology node_count must be integer 1.')
        object.__setattr__(
            self, 'application_port',
            _decimal_port_text(self.application_port,
                               name='topology.application_port'))
        if not isinstance(self.resources_ports, tuple):
            raise TypeError('topology resources_ports must be a tuple.')
        resources_ports = tuple(
            _decimal_port_text(port, name='topology.resources_ports')
            for port in self.resources_ports)
        if resources_ports != (self.application_port,):
            raise ValueError('topology resources_ports must contain exactly '
                             'the application port.')
        object.__setattr__(self, 'resources_ports', resources_ports)
        if (not isinstance(self.mutable_objects, tuple) or
                len(self.mutable_objects) != len(self._EXPECTED_ROLES) or
                any(not isinstance(item, ProviderPodTopologyMutableObjectV1)
                    for item in self.mutable_objects)):
            raise ValueError('pod topology mutable_objects must be the exact '
                             'three typed role entries.')
        roles = tuple(item.role for item in self.mutable_objects)
        if roles != self._EXPECTED_ROLES:
            raise ValueError('pod topology mutable_objects have invalid order.')
        if self.shared_prerequisites != 'preexisting_read_only':
            raise ValueError('pod topology shared prerequisites are '
                             'unsupported.')
        ssh_service, head_service, head_pod = self.mutable_objects
        workload_name = head_service.name
        if (head_pod.name != workload_name or
                ssh_service.name != f'{workload_name}-ssh' or
                not workload_name.endswith('-head') or
                len(workload_name) == len('-head')):
            raise ValueError('pod topology mutable-object names are '
                             'inconsistent.')
        complete_label_maps = tuple(
            item.labels for item in self.mutable_objects)
        if len(set(complete_label_maps)) != len(complete_label_maps):
            raise ValueError('pod topology complete role label maps must be '
                             'pairwise distinct.')
        provider_cluster_name = workload_name[:-len('-head')]
        required_label_keys = (self._DISPLAY_LABEL, self._CLUSTER_UUID_LABEL,
                               self._REPLICA_UUID_LABEL)
        shared_values: dict[str, str] | None = None
        for item in self.mutable_objects:
            labels = {label.key: label.value for label in item.labels}
            if any(key not in labels for key in required_label_keys):
                raise ValueError('every topology mutable object requires the '
                                 'three provider identity labels.')
            identity_values = {key: labels[key] for key in required_label_keys}
            if shared_values is None:
                shared_values = identity_values
            elif identity_values != shared_values:
                raise ValueError('topology identity label values must match '
                                 'across all mutable objects.')
        assert shared_values is not None
        if shared_values[self._DISPLAY_LABEL] != provider_cluster_name:
            raise ValueError('topology display-cluster label does not match '
                             'the workload name.')
        for key in (self._CLUSTER_UUID_LABEL, self._REPLICA_UUID_LABEL):
            _uuid(shared_values[key], name=f'topology label {key}')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderPodTopologyV1':
        raw = _closed_object(value, name='pod topology', keys=cls._KEYS)
        resources_ports = raw['resources_ports']
        mutable_objects = raw['mutable_objects']
        if not isinstance(resources_ports, list):
            raise TypeError('topology resources_ports must be a list.')
        if not isinstance(mutable_objects, list):
            raise TypeError('topology mutable_objects must be a list.')
        return cls(version=raw['version'],
                   kind=raw['kind'],
                   node_count=raw['node_count'],
                   application_port=raw['application_port'],
                   resources_ports=tuple(resources_ports),
                   mutable_objects=tuple(
                       ProviderPodTopologyMutableObjectV1.from_value(item)
                       for item in mutable_objects),
                   shared_prerequisites=raw['shared_prerequisites'])

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'kind': 'single_direct_pod_two_services',
            'node_count': 1,
            'application_port': self.application_port,
            'resources_ports': list(self.resources_ports),
            'mutable_objects': [
                item.canonical_value() for item in self.mutable_objects
            ],
            'shared_prerequisites': 'preexisting_read_only',
        }


@dataclasses.dataclass(frozen=True)
class ProviderPodImageV1(_CanonicalContract):
    """Fixed explicit digest-qualified workload image contract."""

    source: str
    qualification: ProviderOCIImageQualificationV1
    auth_strategy: str
    implementation_contract: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'source', 'qualification', 'auth_strategy', 'implementation_contract'})

    def __post_init__(self) -> None:
        if self.source != 'explicit':
            raise ValueError('pod image source must be explicit.')
        if not isinstance(self.qualification, ProviderOCIImageQualificationV1):
            raise TypeError('pod image qualification has an invalid type.')
        if self.auth_strategy != 'anonymous':
            raise ValueError('pod image auth strategy must be anonymous.')
        if (self.implementation_contract
                != 'kubernetes_serve_prebooted_runtime_v1'):
            raise ValueError('pod image implementation contract is '
                             'unsupported.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderPodImageV1':
        raw = _closed_object(value, name='pod image', keys=cls._KEYS)
        return cls(source=raw['source'],
                   qualification=ProviderOCIImageQualificationV1.from_value(
                       raw['qualification']),
                   auth_strategy=raw['auth_strategy'],
                   implementation_contract=raw['implementation_contract'])

    def canonical_value(self) -> JsonObject:
        return {
            'source': 'explicit',
            'qualification': self.qualification.canonical_value(),
            'auth_strategy': 'anonymous',
            'implementation_contract': 'kubernetes_serve_prebooted_runtime_v1',
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesConfigProjectionV1(_CanonicalContract):
    """Closed provider-effective configuration reads for the v1 renderer."""

    version: int
    workspace: str
    context_mode: str
    target_namespace: str
    port_mode: str
    built_in_provider: bool
    custom_provider_implementation: None
    custom_provisioner: None
    custom_template: None
    custom_pod_config: None
    custom_metadata: tuple[Any, ...]
    global_labels: tuple[Any, ...]
    runtime_class_name: None
    priority_class_name: None
    queue: None
    kueue: bool
    dws: bool
    autoscaler: None
    detected_network_type: str
    persistent_volumes: tuple[Any, ...]
    object_stores: tuple[Any, ...]
    file_mounts: tuple[Any, ...]
    workdir: None
    fuse: bool
    docker_cache: bool
    auto_mounts: bool
    tls_material: None
    managed_secrets: tuple[Any, ...]
    task_secrets: tuple[Any, ...]
    service_account_bootstrap: bool
    rbac_bootstrap: bool
    config_access_inventory: ProviderRepoArtifactRefV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'workspace', 'context_mode', 'target_namespace', 'port_mode',
        'built_in_provider', 'custom_provider_implementation',
        'custom_provisioner', 'custom_template', 'custom_pod_config',
        'custom_metadata', 'global_labels', 'runtime_class_name',
        'priority_class_name', 'queue', 'kueue', 'dws', 'autoscaler',
        'detected_network_type', 'persistent_volumes', 'object_stores',
        'file_mounts', 'workdir', 'fuse', 'docker_cache', 'auto_mounts',
        'tls_material', 'managed_secrets', 'task_secrets',
        'service_account_bootstrap', 'rbac_bootstrap', 'config_access_inventory'
    })
    _NULL_FIELDS: ClassVar[tuple[str, ...]] = ('custom_provider_implementation',
                                               'custom_provisioner',
                                               'custom_template',
                                               'custom_pod_config',
                                               'runtime_class_name',
                                               'priority_class_name', 'queue',
                                               'autoscaler', 'workdir',
                                               'tls_material')
    _EMPTY_FIELDS: ClassVar[tuple[str,
                                  ...]] = ('custom_metadata', 'global_labels',
                                           'persistent_volumes',
                                           'object_stores', 'file_mounts',
                                           'managed_secrets', 'task_secrets')
    _FALSE_FIELDS: ClassVar[tuple[str, ...]] = ('kueue', 'dws', 'fuse',
                                                'docker_cache', 'auto_mounts',
                                                'service_account_bootstrap',
                                                'rbac_bootstrap')

    def __post_init__(self) -> None:
        _version_one(self.version, name='config projection version')
        object.__setattr__(
            self, 'workspace',
            _text(self.workspace, name='config_projection.workspace'))
        object.__setattr__(
            self, 'target_namespace',
            _text(self.target_namespace,
                  name='config_projection.target_namespace',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        if self.context_mode != 'in_cluster':
            raise ValueError('config projection context_mode must be '
                             'in_cluster.')
        if self.port_mode != 'podip':
            raise ValueError('config projection port_mode must be podip.')
        _boolean(self.built_in_provider,
                 name='config_projection.built_in_provider')
        if not self.built_in_provider:
            raise ValueError('config projection requires built_in_provider.')
        for field in self._NULL_FIELDS:
            if getattr(self, field) is not None:
                raise ValueError(f'config projection {field} must be null.')
        for field in self._EMPTY_FIELDS:
            value = getattr(self, field)
            if not isinstance(value, tuple):
                raise TypeError(f'config projection {field} must be a tuple.')
            if value:
                raise ValueError(f'config projection {field} must be empty.')
        for field in self._FALSE_FIELDS:
            value = getattr(self, field)
            _boolean(value, name=f'config_projection.{field}')
            if value:
                raise ValueError(f'config projection {field} must be false.')
        if self.detected_network_type != 'default':
            raise ValueError('config projection detected_network_type must be '
                             'default.')
        if not isinstance(self.config_access_inventory,
                          ProviderRepoArtifactRefV1):
            raise TypeError('config access inventory has an invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesConfigProjectionV1':
        raw = _closed_object(value, name='config projection', keys=cls._KEYS)
        normalized = dict(raw)
        for field in cls._EMPTY_FIELDS:
            items = raw[field]
            if not isinstance(items, list):
                raise TypeError(f'config projection {field} must be a list.')
            normalized[field] = tuple(items)
        normalized['config_access_inventory'] = (
            ProviderRepoArtifactRefV1.from_value(
                raw['config_access_inventory']))
        return cls(**normalized)

    def canonical_value(self) -> JsonObject:
        value: JsonObject = dataclasses.asdict(self)
        for field in self._EMPTY_FIELDS:
            value[field] = []
        return value


@dataclasses.dataclass(frozen=True)
class ProviderPolicyModeEvidenceV1(_CanonicalContract):
    """Typed proof that policy and managed-secret modes are absent."""

    admin_policy_entrypoint: None
    admin_policy_applied: bool
    managed_secrets_provider: None
    managed_secret_reference_count: int

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'admin_policy_entrypoint', 'admin_policy_applied',
        'managed_secrets_provider', 'managed_secret_reference_count'
    })

    def __post_init__(self) -> None:
        if self.admin_policy_entrypoint is not None:
            raise ValueError('admin policy entrypoint must be null.')
        _boolean(self.admin_policy_applied,
                 name='policy_modes.admin_policy_applied')
        if self.admin_policy_applied:
            raise ValueError('admin policy applied must be false.')
        if self.managed_secrets_provider is not None:
            raise ValueError('managed-secrets provider must be null.')
        _nonnegative_integer(self.managed_secret_reference_count,
                             name='policy_modes.managed_secret_reference_count')
        if self.managed_secret_reference_count != 0:
            raise ValueError('managed-secret reference count must be zero.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderPolicyModeEvidenceV1':
        raw = _closed_object(value, name='policy modes', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class ProviderAnnotationV1(_CanonicalContract):
    """One sorted, nonsecret provider annotation with generic text bounds."""

    key: str
    value: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'key', 'value'})

    def __post_init__(self) -> None:
        object.__setattr__(self, 'key', _text(self.key, name='annotation.key'))
        object.__setattr__(self, 'value',
                           _text(self.value, name='annotation.value'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderAnnotationV1':
        raw = _closed_object(value, name='annotation', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return dataclasses.asdict(self)


def _provider_annotation_tuple(value: Any, *,
                               name: str) -> tuple[ProviderAnnotationV1, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f'{name} must be a tuple.')
    if (len(value) > _MAX_LIST_ITEMS or
            any(not isinstance(annotation, ProviderAnnotationV1)
                for annotation in value)):
        raise ValueError(f'{name} must contain at most 256 typed annotations.')
    keys = tuple(annotation.key for annotation in value)
    if keys != tuple(sorted(set(keys))):
        raise ValueError(f'{name} must be sorted by unique key.')
    return value


def _sorted_text_tuple(value: Any,
                       *,
                       name: str,
                       minimum_items: int = 0,
                       maximum_bytes: int = _MAX_TEXT_BYTES) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f'{name} must be a tuple.')
    if not minimum_items <= len(value) <= _MAX_LIST_ITEMS:
        raise ValueError(f'{name} must contain {minimum_items}..256 values.')
    normalized = tuple(
        _text(item, name=f'{name}[{index}]', maximum_bytes=maximum_bytes)
        for index, item in enumerate(value))
    if normalized != tuple(sorted(set(normalized))):
        raise ValueError(f'{name} must be sorted and duplicate-free.')
    return normalized


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesServiceAccountProjectionV1(_CanonicalContract):
    """Bounded live projection of one Kubernetes ServiceAccount."""

    namespace: str
    name: str
    uid: str
    resource_version: str
    labels: tuple[ProviderLabelV1, ...]
    annotations: tuple[ProviderAnnotationV1, ...]
    automount_service_account_token: bool
    image_pull_secrets: tuple[str, ...]
    legacy_secret_refs: tuple[str, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'namespace', 'name', 'uid', 'resource_version', 'labels', 'annotations',
        'automount_service_account_token', 'image_pull_secrets',
        'legacy_secret_refs'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'namespace',
            _text(self.namespace,
                  name='service_account.namespace',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        for field in ('name', 'uid', 'resource_version'):
            object.__setattr__(
                self, field,
                _text(getattr(self, field), name=f'service_account.{field}'))
        object.__setattr__(
            self, 'labels',
            _provider_label_tuple(self.labels, name='service-account labels'))
        object.__setattr__(
            self, 'annotations',
            _provider_annotation_tuple(self.annotations,
                                       name='service-account annotations'))
        _boolean(self.automount_service_account_token,
                 name='service_account.automount_service_account_token')
        object.__setattr__(
            self, 'image_pull_secrets',
            _sorted_text_tuple(self.image_pull_secrets,
                               name='service-account image-pull secrets'))
        object.__setattr__(
            self, 'legacy_secret_refs',
            _sorted_text_tuple(self.legacy_secret_refs,
                               name='service-account legacy secret refs'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(
            cls, value: Any) -> 'ProviderKubernetesServiceAccountProjectionV1':
        raw = _closed_object(value,
                             name='service-account projection',
                             keys=cls._KEYS)
        for field in ('labels', 'annotations', 'image_pull_secrets',
                      'legacy_secret_refs'):
            if not isinstance(raw[field], list):
                raise TypeError(f'service-account {field} must be a list.')
        return cls(
            namespace=raw['namespace'],
            name=raw['name'],
            uid=raw['uid'],
            resource_version=raw['resource_version'],
            labels=tuple(
                ProviderLabelV1.from_value(item) for item in raw['labels']),
            annotations=tuple(
                ProviderAnnotationV1.from_value(item)
                for item in raw['annotations']),
            automount_service_account_token=raw[
                'automount_service_account_token'],
            image_pull_secrets=tuple(raw['image_pull_secrets']),
            legacy_secret_refs=tuple(raw['legacy_secret_refs']))

    def canonical_value(self) -> JsonObject:
        return {
            'namespace': self.namespace,
            'name': self.name,
            'uid': self.uid,
            'resource_version': self.resource_version,
            'labels': [label.canonical_value() for label in self.labels],
            'annotations': [
                annotation.canonical_value() for annotation in self.annotations
            ],
            'automount_service_account_token':
                self.automount_service_account_token,
            'image_pull_secrets': list(self.image_pull_secrets),
            'legacy_secret_refs': list(self.legacy_secret_refs),
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesSelfIdentityV1(_CanonicalContract):
    """Closed nonsecret SelfSubjectReview identity."""

    username: str
    uid: str
    groups: tuple[str, ...]
    extra_keys: tuple[Any, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'username', 'uid', 'groups', 'extra_keys'})
    _GROUP_PREFIX: ClassVar[tuple[str, str]] = ('system:authenticated',
                                                'system:serviceaccounts')

    def __post_init__(self) -> None:
        object.__setattr__(self, 'username',
                           _text(self.username, name='self_identity.username'))
        object.__setattr__(self, 'uid', _text(self.uid,
                                              name='self_identity.uid'))
        if not isinstance(self.groups, tuple):
            raise TypeError('self identity groups must be a tuple.')
        if len(self.groups) != 3 or self.groups[:2] != self._GROUP_PREFIX:
            raise ValueError('self identity groups must have the exact '
                             'authenticated service-account order.')
        namespace_group_prefix = 'system:serviceaccounts:'
        namespace_group = _text(self.groups[2], name='self_identity.groups[2]')
        if not namespace_group.startswith(namespace_group_prefix):
            raise ValueError('self identity namespace group is invalid.')
        _text(namespace_group[len(namespace_group_prefix):],
              name='self_identity.caller_namespace',
              maximum_bytes=_MAX_SHORT_TEXT_BYTES)
        object.__setattr__(self, 'groups',
                           (*self._GROUP_PREFIX, namespace_group))
        if not isinstance(self.extra_keys, tuple):
            raise TypeError('self identity extra_keys must be a tuple.')
        if self.extra_keys:
            raise ValueError('self identity extra_keys must be empty.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesSelfIdentityV1':
        raw = _closed_object(value, name='self identity', keys=cls._KEYS)
        groups = raw['groups']
        extra_keys = raw['extra_keys']
        if not isinstance(groups, list):
            raise TypeError('self identity groups must be a list.')
        if not isinstance(extra_keys, list):
            raise TypeError('self identity extra_keys must be a list.')
        return cls(username=raw['username'],
                   uid=raw['uid'],
                   groups=tuple(groups),
                   extra_keys=tuple(extra_keys))

    def canonical_value(self) -> JsonObject:
        return {
            'username': self.username,
            'uid': self.uid,
            'groups': list(self.groups),
            'extra_keys': [],
        }


class ProviderKubernetesApiGroupV1(str, enum.Enum):
    """Closed API groups used by the v1 provider session."""

    CORE = ''
    APPS = 'apps'
    NETWORKING = 'networking.k8s.io'
    ADMISSION_REGISTRATION = 'admissionregistration.k8s.io'
    AUTHENTICATION = 'authentication.k8s.io'
    AUTHORIZATION = 'authorization.k8s.io'


class ProviderKubernetesResourceV1(str, enum.Enum):
    """Closed Kubernetes resources used by the v1 provider session."""

    NAMESPACES = 'namespaces'
    SERVICE_ACCOUNTS = 'serviceaccounts'
    PODS = 'pods'
    SERVICES = 'services'
    REPLICA_SETS = 'replicasets'
    DEPLOYMENTS = 'deployments'
    NETWORK_POLICIES = 'networkpolicies'
    VALIDATING_ADMISSION_POLICIES = 'validatingadmissionpolicies'
    VALIDATING_ADMISSION_POLICY_BINDINGS = ('validatingadmissionpolicybindings')
    SELF_SUBJECT_REVIEWS = 'selfsubjectreviews'
    SELF_SUBJECT_RULES_REVIEWS = 'selfsubjectrulesreviews'
    SELF_SUBJECT_ACCESS_REVIEWS = 'selfsubjectaccessreviews'


class ProviderKubernetesVerbV1(str, enum.Enum):
    """Closed Kubernetes verbs admitted to typed review evidence."""

    GET = 'get'
    CREATE = 'create'
    DELETE = 'delete'
    LIST = 'list'
    WATCH = 'watch'
    PATCH = 'patch'
    UPDATE = 'update'
    DELETE_COLLECTION = 'deletecollection'


PROVIDER_KUBERNETES_API_GROUP_RESOURCE_MAP_V1 = types.MappingProxyType({
    ProviderKubernetesApiGroupV1.CORE: frozenset({
        ProviderKubernetesResourceV1.NAMESPACES,
        ProviderKubernetesResourceV1.PODS,
        ProviderKubernetesResourceV1.SERVICE_ACCOUNTS,
        ProviderKubernetesResourceV1.SERVICES,
    }),
    ProviderKubernetesApiGroupV1.ADMISSION_REGISTRATION: frozenset({
        ProviderKubernetesResourceV1.VALIDATING_ADMISSION_POLICIES,
        ProviderKubernetesResourceV1.VALIDATING_ADMISSION_POLICY_BINDINGS,
    }),
    ProviderKubernetesApiGroupV1.APPS: frozenset({
        ProviderKubernetesResourceV1.DEPLOYMENTS,
        ProviderKubernetesResourceV1.REPLICA_SETS,
    }),
    ProviderKubernetesApiGroupV1.AUTHENTICATION: frozenset(
        {ProviderKubernetesResourceV1.SELF_SUBJECT_REVIEWS}),
    ProviderKubernetesApiGroupV1.AUTHORIZATION: frozenset({
        ProviderKubernetesResourceV1.SELF_SUBJECT_ACCESS_REVIEWS,
        ProviderKubernetesResourceV1.SELF_SUBJECT_RULES_REVIEWS,
    }),
    ProviderKubernetesApiGroupV1.NETWORKING: frozenset(
        {ProviderKubernetesResourceV1.NETWORK_POLICIES}),
})


def _sorted_enum_tuple(enum_type: type[_EnumT], value: Any, *, name: str,
                       minimum_items: int) -> tuple[_EnumT, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f'{name} must be a tuple.')
    if not minimum_items <= len(value) <= _MAX_LIST_ITEMS:
        raise ValueError(f'{name} must contain {minimum_items}..256 values.')
    parsed = tuple(
        _enum_value(enum_type, item, name=f'{name}[{index}]')
        for index, item in enumerate(value))
    serialized = tuple(item.value for item in parsed)
    if serialized != tuple(sorted(set(serialized))):
        raise ValueError(f'{name} must be sorted and duplicate-free.')
    return parsed


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesResourceRuleV1(_CanonicalContract):
    """One canonical resource rule from SelfSubjectRulesReview."""

    api_groups: tuple[ProviderKubernetesApiGroupV1, ...]
    resources: tuple[ProviderKubernetesResourceV1, ...]
    resource_names: tuple[str, ...]
    verbs: tuple[ProviderKubernetesVerbV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'api_groups', 'resources', 'resource_names', 'verbs'})

    def __post_init__(self) -> None:
        api_groups = _sorted_enum_tuple(ProviderKubernetesApiGroupV1,
                                        self.api_groups,
                                        name='resource_rule.api_groups',
                                        minimum_items=1)
        if len(api_groups) != 1:
            raise ValueError('resource rule requires exactly one API group.')
        resources = _sorted_enum_tuple(ProviderKubernetesResourceV1,
                                       self.resources,
                                       name='resource_rule.resources',
                                       minimum_items=1)
        if any(resource not in PROVIDER_KUBERNETES_API_GROUP_RESOURCE_MAP_V1[
                api_groups[0]] for resource in resources):
            raise ValueError('resource rule contains a resource outside its '
                             'API group.')
        resource_names = _sorted_text_tuple(self.resource_names,
                                            name='resource_rule.resource_names')
        verbs = _sorted_enum_tuple(ProviderKubernetesVerbV1,
                                   self.verbs,
                                   name='resource_rule.verbs',
                                   minimum_items=1)
        object.__setattr__(self, 'api_groups', api_groups)
        object.__setattr__(self, 'resources', resources)
        object.__setattr__(self, 'resource_names', resource_names)
        object.__setattr__(self, 'verbs', verbs)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesResourceRuleV1':
        raw = _closed_object(value, name='resource rule', keys=cls._KEYS)
        for field in cls._KEYS:
            if not isinstance(raw[field], list):
                raise TypeError(f'resource rule {field} must be a list.')
        return cls(api_groups=tuple(raw['api_groups']),
                   resources=tuple(raw['resources']),
                   resource_names=tuple(raw['resource_names']),
                   verbs=tuple(raw['verbs']))

    def canonical_value(self) -> JsonObject:
        return {
            'api_groups': [item.value for item in self.api_groups],
            'resources': [item.value for item in self.resources],
            'resource_names': list(self.resource_names),
            'verbs': [item.value for item in self.verbs],
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesNonResourceRuleV1(_CanonicalContract):
    """The sole nonresource rule allowed by the v1 provider session."""

    urls: tuple[str, ...]
    verbs: tuple[str, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({'urls', 'verbs'})

    def __post_init__(self) -> None:
        if not isinstance(self.urls, tuple) or not isinstance(
                self.verbs, tuple):
            raise TypeError('nonresource rule fields must be tuples.')
        if self.urls != ('/version',) or self.verbs != ('get',):
            raise ValueError('nonresource rule must be exactly GET /version.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesNonResourceRuleV1':
        raw = _closed_object(value, name='nonresource rule', keys=cls._KEYS)
        if not isinstance(raw['urls'], list) or not isinstance(
                raw['verbs'], list):
            raise TypeError('nonresource rule fields must be lists.')
        return cls(urls=tuple(raw['urls']), verbs=tuple(raw['verbs']))

    def canonical_value(self) -> JsonObject:
        return {'urls': ['/version'], 'verbs': ['get']}


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesRulesReviewV1(_CanonicalContract):
    """Complete canonical SelfSubjectRulesReview evidence."""

    namespace: str
    incomplete: bool
    evaluation_error: bool
    resource_rules: tuple[ProviderKubernetesResourceRuleV1, ...]
    non_resource_rules: tuple[ProviderKubernetesNonResourceRuleV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'namespace', 'incomplete', 'evaluation_error', 'resource_rules',
        'non_resource_rules'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'namespace',
            _text(self.namespace,
                  name='rules_review.namespace',
                  maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        for field in ('incomplete', 'evaluation_error'):
            value = getattr(self, field)
            _boolean(value, name=f'rules_review.{field}')
            if value:
                raise ValueError(f'rules review {field} must be false.')
        if (not isinstance(self.resource_rules, tuple) or
                not 1 <= len(self.resource_rules) <= _MAX_LIST_ITEMS or
                any(not isinstance(rule, ProviderKubernetesResourceRuleV1)
                    for rule in self.resource_rules)):
            raise ValueError('rules review resource_rules must contain 1..256 '
                             'typed rules.')
        rule_bytes = tuple(rule.canonical_bytes for rule in self.resource_rules)
        if rule_bytes != tuple(sorted(set(rule_bytes))):
            raise ValueError('rules review resource_rules must be sorted and '
                             'duplicate-free by canonical bytes.')
        if (not isinstance(self.non_resource_rules, tuple) or
                len(self.non_resource_rules) != 1 or
                not isinstance(self.non_resource_rules[0],
                               ProviderKubernetesNonResourceRuleV1)):
            raise ValueError('rules review requires exactly one typed '
                             'nonresource rule.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesRulesReviewV1':
        raw = _closed_object(value, name='rules review', keys=cls._KEYS)
        if not isinstance(raw['resource_rules'], list):
            raise TypeError('rules review resource_rules must be a list.')
        if not isinstance(raw['non_resource_rules'], list):
            raise TypeError('rules review non_resource_rules must be a list.')
        return cls(namespace=raw['namespace'],
                   incomplete=raw['incomplete'],
                   evaluation_error=raw['evaluation_error'],
                   resource_rules=tuple(
                       ProviderKubernetesResourceRuleV1.from_value(rule)
                       for rule in raw['resource_rules']),
                   non_resource_rules=tuple(
                       ProviderKubernetesNonResourceRuleV1.from_value(rule)
                       for rule in raw['non_resource_rules']))

    def canonical_value(self) -> JsonObject:
        return {
            'namespace': self.namespace,
            'incomplete': False,
            'evaluation_error': False,
            'resource_rules': [
                rule.canonical_value() for rule in self.resource_rules
            ],
            'non_resource_rules': [
                rule.canonical_value() for rule in self.non_resource_rules
            ],
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesResourceAccessV1(_CanonicalContract):
    """One typed Kubernetes resource access-review input."""

    api_group: ProviderKubernetesApiGroupV1
    resource: ProviderKubernetesResourceV1
    subresource: None
    verb: ProviderKubernetesVerbV1
    namespace: str | None
    name: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'api_group', 'resource', 'subresource', 'verb', 'namespace', 'name'})

    def __post_init__(self) -> None:
        api_group = _enum_value(ProviderKubernetesApiGroupV1,
                                self.api_group,
                                name='resource_access.api_group')
        resource = _enum_value(ProviderKubernetesResourceV1,
                               self.resource,
                               name='resource_access.resource')
        if resource not in PROVIDER_KUBERNETES_API_GROUP_RESOURCE_MAP_V1[
                api_group]:
            raise ValueError('resource access contains a resource outside its '
                             'API group.')
        if self.subresource is not None:
            raise ValueError('resource access subresource must be null.')
        verb = _enum_value(ProviderKubernetesVerbV1,
                           self.verb,
                           name='resource_access.verb')
        object.__setattr__(
            self, 'namespace',
            _optional_text(self.namespace,
                           name='resource_access.namespace',
                           maximum_bytes=_MAX_SHORT_TEXT_BYTES))
        object.__setattr__(
            self, 'name', _optional_text(self.name,
                                         name='resource_access.name'))
        object.__setattr__(self, 'api_group', api_group)
        object.__setattr__(self, 'resource', resource)
        object.__setattr__(self, 'verb', verb)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesResourceAccessV1':
        raw = _closed_object(value, name='resource access', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {
            'api_group': self.api_group.value,
            'resource': self.resource.value,
            'subresource': None,
            'verb': self.verb.value,
            'namespace': self.namespace,
            'name': self.name,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesNonResourceAccessV1(_CanonicalContract):
    """The sole nonresource access-review input in v1."""

    verb: str
    path: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({'verb', 'path'})

    def __post_init__(self) -> None:
        if self.verb != 'get' or self.path != '/version':
            raise ValueError('nonresource access must be exactly GET /version.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesNonResourceAccessV1':
        raw = _closed_object(value, name='nonresource access', keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> JsonObject:
        return {'verb': 'get', 'path': '/version'}


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesAccessDecisionV1(_CanonicalContract):
    """One ordered expected and observed authorization decision."""

    check_sequence: int
    resource: ProviderKubernetesResourceAccessV1 | None
    non_resource: ProviderKubernetesNonResourceAccessV1 | None
    expected_allowed: bool
    observed_allowed: bool
    observed_denied: bool
    evaluation_error: bool

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'check_sequence', 'resource', 'non_resource', 'expected_allowed',
        'observed_allowed', 'observed_denied', 'evaluation_error'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'check_sequence',
            _nonnegative_integer(self.check_sequence,
                                 name='access_decision.check_sequence'))
        if ((self.resource is None) == (self.non_resource is None) or
            (self.resource is not None and
             not isinstance(self.resource, ProviderKubernetesResourceAccessV1))
                or (self.non_resource is not None and not isinstance(
                    self.non_resource, ProviderKubernetesNonResourceAccessV1))):
            raise ValueError('access decision requires exactly one typed '
                             'resource discriminator.')
        for field in ('expected_allowed', 'observed_allowed', 'observed_denied',
                      'evaluation_error'):
            _boolean(getattr(self, field), name=f'access_decision.{field}')
        if self.evaluation_error:
            raise ValueError('access decision evaluation_error must be false.')
        if self.observed_allowed != self.expected_allowed:
            raise ValueError('access decision observed result differs from '
                             'its expectation.')
        if self.observed_allowed and self.observed_denied:
            raise ValueError('an allowed access decision cannot also be '
                             'observed denied.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesAccessDecisionV1':
        raw = _closed_object(value, name='access decision', keys=cls._KEYS)
        return cls(
            check_sequence=raw['check_sequence'],
            resource=(None if raw['resource'] is None else
                      ProviderKubernetesResourceAccessV1.from_value(
                          raw['resource'])),
            non_resource=(None if raw['non_resource'] is None else
                          ProviderKubernetesNonResourceAccessV1.from_value(
                              raw['non_resource'])),
            expected_allowed=raw['expected_allowed'],
            observed_allowed=raw['observed_allowed'],
            observed_denied=raw['observed_denied'],
            evaluation_error=raw['evaluation_error'])

    def canonical_value(self) -> JsonObject:
        return {
            'check_sequence': self.check_sequence,
            'resource': (None if self.resource is None else
                         self.resource.canonical_value()),
            'non_resource': (None if self.non_resource is None else
                             self.non_resource.canonical_value()),
            'expected_allowed': self.expected_allowed,
            'observed_allowed': self.observed_allowed,
            'observed_denied': self.observed_denied,
            'evaluation_error': False,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesAuthorizationEvidenceV1(_CanonicalContract):
    """Complete content-addressed Kubernetes authorization evidence."""

    identity: ProviderKubernetesSelfIdentityV1
    rules: ProviderKubernetesRulesReviewV1
    rules_sha256: str
    access_matrix_contract: ProviderRepoArtifactRefV1
    access_decisions: tuple[ProviderKubernetesAccessDecisionV1, ...]
    access_decisions_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'identity', 'rules', 'rules_sha256', 'access_matrix_contract',
        'access_decisions', 'access_decisions_sha256'
    })

    def __post_init__(self) -> None:
        if not isinstance(self.identity, ProviderKubernetesSelfIdentityV1):
            raise TypeError('authorization identity has an invalid type.')
        if not isinstance(self.rules, ProviderKubernetesRulesReviewV1):
            raise TypeError('authorization rules have an invalid type.')
        object.__setattr__(
            self, 'rules_sha256',
            _sha256(self.rules_sha256, name='authorization.rules_sha256'))
        if self.rules_sha256 != self.rules.sha256:
            raise ValueError('authorization rules hash does not match its '
                             'embedded preimage.')
        if not isinstance(self.access_matrix_contract,
                          ProviderRepoArtifactRefV1):
            raise TypeError('authorization access-matrix contract has an '
                            'invalid type.')
        if (not isinstance(self.access_decisions, tuple) or
                not 1 <= len(self.access_decisions) <= _MAX_LIST_ITEMS or
                any(not isinstance(decision, ProviderKubernetesAccessDecisionV1)
                    for decision in self.access_decisions)):
            raise ValueError('authorization access_decisions must contain '
                             '1..256 typed decisions.')
        sequences = tuple(
            decision.check_sequence for decision in self.access_decisions)
        if sequences != tuple(range(len(self.access_decisions))):
            raise ValueError('authorization access_decisions must be a '
                             'contiguous zero-based sequence.')
        object.__setattr__(
            self, 'access_decisions_sha256',
            _sha256(self.access_decisions_sha256,
                    name='authorization.access_decisions_sha256'))
        decisions_value = [
            decision.canonical_value() for decision in self.access_decisions
        ]
        if self.access_decisions_sha256 != canonical_sha256(decisions_value):
            raise ValueError('authorization access-decisions hash does not '
                             'match its embedded preimage.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> 'ProviderKubernetesAuthorizationEvidenceV1':
        raw = _closed_object(value,
                             name='authorization evidence',
                             keys=cls._KEYS)
        decisions = raw['access_decisions']
        if not isinstance(decisions, list):
            raise TypeError('authorization access_decisions must be a list.')
        return cls(identity=ProviderKubernetesSelfIdentityV1.from_value(
            raw['identity']),
                   rules=ProviderKubernetesRulesReviewV1.from_value(
                       raw['rules']),
                   rules_sha256=raw['rules_sha256'],
                   access_matrix_contract=ProviderRepoArtifactRefV1.from_value(
                       raw['access_matrix_contract']),
                   access_decisions=tuple(
                       ProviderKubernetesAccessDecisionV1.from_value(decision)
                       for decision in decisions),
                   access_decisions_sha256=raw['access_decisions_sha256'])

    def canonical_value(self) -> JsonObject:
        return {
            'identity': self.identity.canonical_value(),
            'rules': self.rules.canonical_value(),
            'rules_sha256': self.rules_sha256,
            'access_matrix_contract':
                self.access_matrix_contract.canonical_value(),
            'access_decisions': [
                decision.canonical_value() for decision in self.access_decisions
            ],
            'access_decisions_sha256': self.access_decisions_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesPrincipalsV1(_CanonicalContract):
    """Caller/workload principals and the caller's exact authorization."""

    caller: ProviderKubernetesServiceAccountProjectionV1
    workload: ProviderKubernetesServiceAccountProjectionV1
    caller_authorization: ProviderKubernetesAuthorizationEvidenceV1

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'caller', 'workload', 'caller_authorization'})

    def __post_init__(self) -> None:
        if not isinstance(self.caller,
                          ProviderKubernetesServiceAccountProjectionV1):
            raise TypeError('caller service account has an invalid type.')
        if not isinstance(self.workload,
                          ProviderKubernetesServiceAccountProjectionV1):
            raise TypeError('workload service account has an invalid type.')
        if not isinstance(self.caller_authorization,
                          ProviderKubernetesAuthorizationEvidenceV1):
            raise TypeError('caller authorization has an invalid type.')
        if not self.caller.automount_service_account_token:
            raise ValueError('caller ServiceAccount token automount must be '
                             'true.')
        if self.workload.automount_service_account_token:
            raise ValueError('workload ServiceAccount token automount must be '
                             'false.')
        if (self.workload.image_pull_secrets or
                self.workload.legacy_secret_refs):
            raise ValueError('workload ServiceAccount secret references must '
                             'be empty.')
        caller_name = (self.caller.namespace, self.caller.name)
        workload_name = (self.workload.namespace, self.workload.name)
        if ((caller_name == workload_name)
                != (self.caller.uid == self.workload.uid)):
            raise ValueError('caller/workload ServiceAccount names and UIDs '
                             'contradict one another.')
        authorization = self.caller_authorization
        expected_username = (
            f'system:serviceaccount:{self.caller.namespace}:{self.caller.name}')
        expected_groups = ('system:authenticated', 'system:serviceaccounts',
                           f'system:serviceaccounts:{self.caller.namespace}')
        if (authorization.identity.username != expected_username or
                authorization.identity.uid != self.caller.uid or
                authorization.identity.groups != expected_groups):
            raise ValueError('caller SelfSubjectReview identity does not match '
                             'the caller ServiceAccount.')
        if authorization.rules.namespace != self.workload.namespace:
            raise ValueError('rules-review namespace must equal the workload '
                             'ServiceAccount namespace.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderKubernetesPrincipalsV1':
        raw = _closed_object(value,
                             name='Kubernetes principals',
                             keys=cls._KEYS)
        return cls(
            caller=ProviderKubernetesServiceAccountProjectionV1.from_value(
                raw['caller']),
            workload=ProviderKubernetesServiceAccountProjectionV1.from_value(
                raw['workload']),
            caller_authorization=(
                ProviderKubernetesAuthorizationEvidenceV1.from_value(
                    raw['caller_authorization'])))

    def canonical_value(self) -> JsonObject:
        return {
            'caller': self.caller.canonical_value(),
            'workload': self.workload.canonical_value(),
            'caller_authorization': self.caller_authorization.canonical_value(),
        }


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

    def require_launch(self) -> ProviderLaunchInvocationV1:
        """Return the launch member or reject a wrong-kind access."""

        if (self.action_kind is not kernel_actions.ActionKind.LAUNCH or
                self.launch is None):
            raise ValueError('provider lifecycle invocation is not launch.')
        return self.launch

    def require_down(self) -> ProviderDownInvocationV1:
        """Return the down member or reject a wrong-kind access."""

        if (self.action_kind is not kernel_actions.ActionKind.DOWN or
                self.down is None):
            raise ValueError('provider lifecycle invocation is not down.')
        return self.down

    def as_launch(self) -> 'ProviderLaunchLifecycleInvocationV1':
        """Return a statically refined launch view with identical bytes."""

        self.require_launch()
        return ProviderLaunchLifecycleInvocationV1.from_value(
            self.canonical_value())

    def as_down(self) -> 'ProviderDownLifecycleInvocationV1':
        """Return a statically refined down view with identical bytes."""

        self.require_down()
        return ProviderDownLifecycleInvocationV1.from_value(
            self.canonical_value())

    def refined(
        self
    ) -> (ProviderLaunchLifecycleInvocationV1 |
          ProviderDownLifecycleInvocationV1):
        """Refine the closed discriminator without changing serialization."""

        if self.action_kind is kernel_actions.ActionKind.LAUNCH:
            return self.as_launch()
        return self.as_down()


@dataclasses.dataclass(frozen=True)
class ProviderLaunchLifecycleInvocationV1(ProviderLifecycleInvocationV1):
    """Kind-refined launch invocation; canonical shape equals the base union."""

    action_kind: kernel_actions.ActionKind
    launch: ProviderLaunchInvocationV1
    down: None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.action_kind is not kernel_actions.ActionKind.LAUNCH:
            raise ValueError('refined launch invocation requires launch kind.')

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderLaunchLifecycleInvocationV1':
        base = ProviderLifecycleInvocationV1.from_value(value)
        base.require_launch()
        return cls(version=base.version,
                   profile=base.profile,
                   redaction_profile=base.redaction_profile,
                   action_kind=base.action_kind,
                   resource_identity=base.resource_identity,
                   requested_target=base.requested_target,
                   launch=base.require_launch(),
                   down=None)


@dataclasses.dataclass(frozen=True)
class ProviderDownLifecycleInvocationV1(ProviderLifecycleInvocationV1):
    """Kind-refined down invocation; canonical shape equals the base union."""

    action_kind: kernel_actions.ActionKind
    launch: None
    down: ProviderDownInvocationV1

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.action_kind is not kernel_actions.ActionKind.DOWN:
            raise ValueError('refined down invocation requires down kind.')

    @classmethod
    def from_value(cls, value: Any) -> 'ProviderDownLifecycleInvocationV1':
        base = ProviderLifecycleInvocationV1.from_value(value)
        base.require_down()
        return cls(version=base.version,
                   profile=base.profile,
                   redaction_profile=base.redaction_profile,
                   action_kind=base.action_kind,
                   resource_identity=base.resource_identity,
                   requested_target=base.requested_target,
                   launch=None,
                   down=base.require_down())


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
class ServeReplicaActionSpecV1(_CanonicalContract):
    """Closed immutable Serve wrapper around one provider action plan."""

    version: int
    provider_plan: ProviderLifecyclePlanV1
    invocation: ProviderLifecycleInvocationV1

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'provider_plan', 'invocation'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='Serve replica action spec version')
        if not isinstance(self.provider_plan, ProviderLifecyclePlanV1):
            raise TypeError('provider_plan has an invalid type.')
        if not isinstance(self.invocation, ProviderLifecycleInvocationV1):
            raise TypeError('invocation has an invalid type.')
        if self.provider_plan.action_id != self.invocation.action_id:
            raise ValueError('provider plan and invocation action IDs differ.')
        self.provider_plan.validate_invocation(self.invocation)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> 'ServeReplicaActionSpecV1':
        raw = _closed_object(value,
                             name='Serve replica action spec',
                             keys=cls._KEYS)
        return cls(version=raw['version'],
                   provider_plan=ProviderLifecyclePlanV1.from_value(
                       raw['provider_plan']),
                   invocation=ProviderLifecycleInvocationV1.from_value(
                       raw['invocation']))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 1,
            'provider_plan': self.provider_plan.canonical_value(),
            'invocation': self.invocation.canonical_value(),
        }

    @property
    def action_id(self) -> uuid.UUID:
        return self.provider_plan.action_id

    def validate_parent_provider_plan(
            self, provider_plan: ProviderLifecyclePlanV1) -> None:
        """Require a shadow parent's indexed plan to be the wrapper member."""

        if not isinstance(provider_plan, ProviderLifecyclePlanV1):
            raise TypeError('parent provider_plan has an invalid type.')
        if provider_plan.canonical_bytes != self.provider_plan.canonical_bytes:
            raise ValueError('parent provider plan is not byte-equal to the '
                             'immutable spec provider plan.')

    def launch_cleanup_down_invocation(
            self) -> ProviderDownLifecycleInvocationV1:
        """Derive the sole non-primary invocation allowed by this wrapper."""

        if self.invocation.action_kind is not kernel_actions.ActionKind.LAUNCH:
            raise ValueError('cleanup down requires a launch action spec.')
        launch = self.invocation.launch
        if launch is None:
            raise ValueError('cleanup down requires a launch invocation.')
        target = self.invocation.requested_target
        return ProviderDownLifecycleInvocationV1(
            version=1,
            profile=self.invocation.profile,
            redaction_profile=self.invocation.redaction_profile,
            action_kind=kernel_actions.ActionKind.DOWN,
            resource_identity=self.invocation.resource_identity,
            requested_target=target,
            launch=None,
            down=ProviderDownInvocationV1(
                cluster_name=target.sky_cluster_name,
                expected_cluster_record_uuid=target.sky_cluster_record_uuid,
                workspace=launch.source.workspace,
                purge=False,
                graceful=False,
                graceful_timeout=None))

    def validate_shadow_child_invocation(
            self, role: ShadowRequestRole,
            invocation: ProviderLifecycleInvocationV1) -> None:
        """Require the exact primary invocation or derived cleanup exception."""

        if not isinstance(role, ShadowRequestRole):
            raise TypeError('shadow request role has an invalid type.')
        if not isinstance(invocation, ProviderLifecycleInvocationV1):
            raise TypeError('child invocation has an invalid type.')
        expected: ProviderLifecycleInvocationV1
        if role is ShadowRequestRole.LAUNCH_CLEANUP_DOWN:
            expected = self.launch_cleanup_down_invocation()
        else:
            expected_role = (ShadowRequestRole.PRIMARY_LAUNCH
                             if self.invocation.action_kind
                             is kernel_actions.ActionKind.LAUNCH else
                             ShadowRequestRole.PRIMARY_DOWN)
            if role is not expected_role:
                raise ValueError('primary child role does not match the action '
                                 'spec action kind.')
            expected = self.invocation
        if invocation.canonical_bytes != expected.canonical_bytes:
            raise ValueError('child invocation is not byte-equal to the '
                             'immutable action spec invocation.')


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

    def validate_for_invocation(
            self, invocation: ProviderLifecycleInvocationV1) -> None:
        """Require action-specific evidence before accepting this outcome.

        Provider submission acknowledgement is not proof that a resource
        reached its requested lifecycle state.  A successful launch therefore
        requires an authoritative, identity-matched, ready observation; a
        successful down requires authoritative absence.  Non-success outcomes
        retain their closed retry/error semantics, but any attached observation
        must still belong to the invocation's frozen target.
        """

        if not isinstance(invocation, ProviderLifecycleInvocationV1):
            raise TypeError('invocation has an invalid type.')
        if self.observation is not None:
            self.observation.validate_target(invocation.requested_target)
        if self.disposition is not ServeActionDisposition.SUCCEEDED:
            return
        if self.certainty is not ServeActionCertainty.OBSERVED:
            raise ValueError('succeeded outcome requires observed certainty; '
                             'provider acknowledgement is not success proof.')
        observation = self.observation
        if observation is None:
            raise ValueError('succeeded outcome requires an observation.')
        if invocation.action_kind is kernel_actions.ActionKind.LAUNCH:
            if observation.state is not ProviderObservationState.PRESENT:
                raise ValueError('succeeded launch requires a PRESENT '
                                 'observation.')
            if (observation.certainty
                    is not ProviderObservationCertainty.AUTHORITATIVE):
                raise ValueError('succeeded launch requires an authoritative '
                                 'observation.')
            if observation.ready is not True:
                raise ValueError('succeeded launch requires ready=True.')
            if observation.resolved_target is None:
                raise ValueError('succeeded launch requires a resolved target.')
            return
        if observation.state is not ProviderObservationState.ABSENT:
            raise ValueError('succeeded down requires an ABSENT observation.')
        if (observation.certainty
                is not ProviderObservationCertainty.AUTHORITATIVE):
            raise ValueError('succeeded down requires an authoritative '
                             'observation.')


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
